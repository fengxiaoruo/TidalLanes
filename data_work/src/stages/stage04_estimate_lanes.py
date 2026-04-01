"""
Stage 04: Estimate Lanes

Purpose:
- Infer lane counts from raw road classes
- Aggregate lane estimates to centerline direction level
- Produce lane consistency diagnostics across opposite directions
"""

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd


LANE_MAP = {
    2: 6.0,  # expressway
    3: 4.0,  # arterial
    4: 2.0,  # secondary arterial
}


def parse_args():
    parser = argparse.ArgumentParser(description="Stage 04: Estimate lanes")
    parser.add_argument("--config", default=None, help="Optional config file path.")
    parser.add_argument("--version-id", required=True, help="Version identifier for outputs.")
    parser.add_argument("--output-dir", default="outputs", help="Base output directory for versioned results.")
    return parser.parse_args()


def save_config_snapshot(version_root: Path, config_path: str | None):
    payload = {
        "stage": "stage04_estimate_lanes",
        "config_path": config_path,
        "lane_map": LANE_MAP,
        "note": (
            "Lane assumptions from roadtype hierarchy: "
            "2=expressway -> 6 lanes, "
            "3=arterial -> 4 lanes, "
            "4=secondary arterial -> 2 lanes."
        ),
        "lane_sensitivity_options": {
            "2": [6.0, 8.0],
            "3": [4.0, 6.0],
            "4": [2.0, 4.0],
        },
    }
    (version_root / "config_snapshot.stage04.json").write_text(
        json.dumps(payload, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )


def load_inputs(version_root: Path):
    data_dir = version_root / "data"
    raw_segments = pd.read_parquet(data_dir / "raw_segment_master.parquet")
    match_master = pd.read_parquet(data_dir / "raw_to_centerline_match_master.parquet")
    centerline_dir = pd.read_parquet(data_dir / "centerline_dir_master.parquet")
    return raw_segments, match_master, centerline_dir


def build_lane_master(raw_segments: pd.DataFrame, match_master: pd.DataFrame, centerline_dir: pd.DataFrame):
    seg = raw_segments.copy()
    road_class_source = "road_class" if "road_class" in seg.columns else "roadtype"
    seg["road_class_num"] = pd.to_numeric(seg.get(road_class_source), errors="coerce")
    seg["lane_est_segment"] = pd.to_numeric(seg.get("road_class_lane_mean"), errors="coerce")
    seg["lane_est_segment"] = seg["lane_est_segment"].fillna(seg["road_class_num"].map(LANE_MAP)).astype(float)
    seg["length_m"] = pd.to_numeric(seg["length_m"], errors="coerce")

    mm = match_master[
        [
            "split_id",
            "raw_edge_id",
            "matched_final",
            "skel_dir_final",
            "cline_id_final",
            "dir_final",
            "match_method_final",
            "s_from",
            "s_to",
        ]
    ].copy()
    mm = mm.rename(
        columns={
            "matched_final": "matched",
            "skel_dir_final": "skel_dir",
            "cline_id_final": "cline_id",
            "dir_final": "dir",
            "match_method_final": "match_method",
        }
    )

    seg = seg.drop(columns=["matched", "skel_dir", "cline_id", "dir", "match_method", "s_from", "s_to"], errors="ignore")
    df = seg.merge(mm, on=["split_id", "raw_edge_id"], how="left")
    df = df[pd.to_numeric(df["matched"], errors="coerce") == 1].copy()
    df["matched_length_m"] = pd.to_numeric(df["s_to"], errors="coerce") - pd.to_numeric(df["s_from"], errors="coerce")
    df["matched_length_m"] = np.where(df["matched_length_m"] > 0, df["matched_length_m"], df["length_m"])
    df["matched_length_m"] = pd.to_numeric(df["matched_length_m"], errors="coerce")

    use = df[df["lane_est_segment"].notna() & (df["matched_length_m"] > 0)].copy()
    use["lane_w_len"] = use["lane_est_segment"] * use["matched_length_m"]
    use["is_proj_match"] = use["match_method"].eq("projection_fallback").astype(int)
    use["matched_length_proj_m"] = np.where(use["is_proj_match"] == 1, use["matched_length_m"], 0.0)
    use["matched_length_old_m"] = np.where(use["is_proj_match"] == 0, use["matched_length_m"], 0.0)
    use["lane_w_len_proj"] = np.where(use["is_proj_match"] == 1, use["lane_w_len"], 0.0)
    use["lane_w_len_old"] = np.where(use["is_proj_match"] == 0, use["lane_w_len"], 0.0)

    agg = (
        use.groupby(["skel_dir", "cline_id", "dir"], as_index=False)
        .agg(
            n_matched_segments=("split_id", "count"),
            matched_length_m=("matched_length_m", "sum"),
            n_proj_segments=("is_proj_match", "sum"),
            matched_length_proj_m=("matched_length_proj_m", "sum"),
            matched_length_old_m=("matched_length_old_m", "sum"),
            lane_w_sum=("lane_w_len", "sum"),
            lane_w_sum_proj=("lane_w_len_proj", "sum"),
            lane_w_sum_old=("lane_w_len_old", "sum"),
            lane_est_raw_weighted=("lane_est_segment", "mean"),
        )
    )
    agg["lane_est_length_weighted"] = np.where(
        agg["matched_length_m"] > 0, agg["lane_w_sum"] / agg["matched_length_m"], np.nan
    )
    agg["lane_est_old_weighted"] = np.where(
        agg["matched_length_old_m"] > 0, agg["lane_w_sum_old"] / agg["matched_length_old_m"], np.nan
    )
    agg["lane_est_proj_weighted"] = np.where(
        agg["matched_length_proj_m"] > 0, agg["lane_w_sum_proj"] / agg["matched_length_proj_m"], np.nan
    )
    agg["lane_est_overlap_weighted"] = agg["lane_est_length_weighted"]
    agg["lane_est_quality_flag"] = np.where(agg["n_matched_segments"] > 0, "ok", "missing")
    agg["proj_match_share_len"] = np.where(
        agg["matched_length_m"] > 0, agg["matched_length_proj_m"] / agg["matched_length_m"], np.nan
    )

    lane_master = centerline_dir[["skel_dir", "cline_id", "dir"]].merge(
        agg[
            [
                "skel_dir",
                "cline_id",
                "dir",
                "lane_est_raw_weighted",
                "lane_est_length_weighted",
                "lane_est_old_weighted",
                "lane_est_proj_weighted",
                "lane_est_overlap_weighted",
                "n_matched_segments",
                "n_proj_segments",
                "matched_length_m",
                "matched_length_old_m",
                "matched_length_proj_m",
                "proj_match_share_len",
                "lane_est_quality_flag",
            ]
        ],
        on=["skel_dir", "cline_id", "dir"],
        how="left",
    )

    pair = lane_master[["cline_id", "dir", "lane_est_length_weighted"]].copy()
    pair = pair.pivot_table(index="cline_id", columns="dir", values="lane_est_length_weighted", aggfunc="first").reset_index()
    if "AB" not in pair.columns:
        pair["AB"] = np.nan
    if "BA" not in pair.columns:
        pair["BA"] = np.nan
    pair["opposite_dir_lane_diff"] = pair["AB"] - pair["BA"]
    pair["opposite_dir_lane_ratio"] = np.where(
        (pair[["AB", "BA"]].min(axis=1) > 0) & pair[["AB", "BA"]].notna().all(axis=1),
        pair[["AB", "BA"]].max(axis=1) / pair[["AB", "BA"]].min(axis=1),
        np.nan,
    )

    lane_master = lane_master.merge(pair[["cline_id", "opposite_dir_lane_diff", "opposite_dir_lane_ratio"]], on="cline_id", how="left")
    lane_master["lane_symmetry_flag"] = np.where(
        lane_master["opposite_dir_lane_ratio"].notna() & (lane_master["opposite_dir_lane_ratio"] <= 1.5),
        1,
        0,
    )
    return lane_master


def save_metrics(lane_master: pd.DataFrame, metrics_dir: Path):
    metrics_dir.mkdir(parents=True, exist_ok=True)
    valid = lane_master[pd.to_numeric(lane_master["lane_est_length_weighted"], errors="coerce").notna()].copy()
    summary = pd.DataFrame(
        [
            {
                "rows": len(lane_master),
                "rows_with_lane_est": len(valid),
                "mean_lane_est": float(pd.to_numeric(valid["lane_est_length_weighted"], errors="coerce").mean()) if len(valid) else np.nan,
                "median_lane_est": float(pd.to_numeric(valid["lane_est_length_weighted"], errors="coerce").median()) if len(valid) else np.nan,
                "mean_opposite_ratio": float(pd.to_numeric(valid["opposite_dir_lane_ratio"], errors="coerce").mean()) if len(valid) else np.nan,
                "share_lane_symmetry_flag": float(pd.to_numeric(lane_master["lane_symmetry_flag"], errors="coerce").mean()),
                "rows_with_projection_support": int((pd.to_numeric(lane_master["n_proj_segments"], errors="coerce").fillna(0) > 0).sum()),
                "mean_projection_share_len": float(pd.to_numeric(valid["proj_match_share_len"], errors="coerce").mean()) if len(valid) else np.nan,
            }
        ]
    )
    summary.to_csv(metrics_dir / "stage04_lane_summary.csv", index=False)


def run(config_path: str | None, version_id: str, output_dir: str):
    version_root = Path(output_dir) / version_id
    data_dir = version_root / "data"
    metrics_dir = version_root / "metrics"
    data_dir.mkdir(parents=True, exist_ok=True)
    metrics_dir.mkdir(parents=True, exist_ok=True)

    save_config_snapshot(version_root, config_path)
    raw_segments, match_master, centerline_dir = load_inputs(version_root)
    lane_master = build_lane_master(raw_segments, match_master, centerline_dir)
    out = data_dir / "centerline_lane_master.parquet"
    lane_master.to_parquet(out, index=False)
    save_metrics(lane_master, metrics_dir)
    print(f"[stage04] saved centerline_lane_master: {out}")


def main():
    args = parse_args()
    run(args.config, args.version_id, args.output_dir)


if __name__ == "__main__":
    main()
