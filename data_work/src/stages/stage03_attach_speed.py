"""
Stage 03: Attach Speed

Purpose:
- Attach and aggregate speed observations to directed centerlines
- Produce centerline-by-time speed records
- Preserve filtering flags and observation counts

Planned inputs:
- outputs/{version_id}/data/raw_to_centerline_match_master.parquet
- outputs/{version_id}/data/centerline_dir_master.parquet
- raw_data/speed_Beijing_all_wgs84.csv

Planned outputs:
- outputs/{version_id}/data/centerline_speed_master.parquet

Current source notebook:
- code/02_Centerline_Match_Speed.ipynb
"""

import argparse
import json
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
RAW_SPEED_PATH = ROOT / "raw_data" / "speed_Beijing_all_wgs84.csv"


def parse_args():
    parser = argparse.ArgumentParser(description="Stage 03: Attach speed to centerline")
    parser.add_argument("--config", default=None, help="Optional config file path.")
    parser.add_argument("--version-id", required=True, help="Version identifier for outputs.")
    parser.add_argument(
        "--output-dir",
        default="outputs",
        help="Base output directory for versioned results.",
    )
    return parser.parse_args()


def save_config_snapshot(version_root: Path, config_path: str | None):
    payload = {
        "stage": "stage03_attach_speed",
        "config_path": config_path,
        "raw_speed_path": str(RAW_SPEED_PATH),
    }
    (version_root / "config_snapshot.stage03.json").write_text(
        json.dumps(payload, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )


def load_stage_inputs(version_root: Path):
    data_dir = version_root / "data"
    centerline_dir_path = data_dir / "centerline_dir_master.parquet"
    raw_segment_path = data_dir / "raw_segment_master.parquet"
    match_path = data_dir / "raw_to_centerline_match_master.parquet"

    for path in [centerline_dir_path, raw_segment_path, match_path]:
        if not path.exists():
            raise FileNotFoundError(f"Required upstream file missing: {path}")

    cl_dir = gpd.read_parquet(centerline_dir_path)
    raw_segments = gpd.read_parquet(raw_segment_path)
    match_master = pd.read_parquet(match_path)
    return cl_dir, raw_segments, match_master


def load_speed_data():
    df = pd.read_csv(RAW_SPEED_PATH)

    if {"day", "hour"}.issubset(df.columns):
        df["day"] = pd.to_numeric(df["day"], errors="coerce")
        df["hour"] = pd.to_numeric(df["hour"], errors="coerce")
        df["hour_int"] = df["hour"].round().astype("Int64")
        df["dt"] = pd.to_datetime(
            df["hour_int"].astype(str),
            format="%Y%m%d%H%M",
            errors="coerce",
        )
    elif "dt" in df.columns:
        df["dt"] = pd.to_datetime(df["dt"], errors="coerce")
    else:
        raise ValueError("Speed file must contain either ('day','hour') or 'dt'.")

    df["hour_of_day"] = df["dt"].dt.hour
    df["is_weekday"] = df["dt"].dt.weekday < 5
    df["weekday_label"] = np.where(df["is_weekday"], "weekday", "weekend")
    df["is_am_peak"] = (df["hour_of_day"] >= 7) & (df["hour_of_day"] < 9)
    df["is_pm_peak"] = (df["hour_of_day"] >= 17) & (df["hour_of_day"] < 19)
    df["is_freeflow_2205"] = (df["hour_of_day"] >= 22) | (df["hour_of_day"] <= 5)
    df["is_freeflow_0005"] = df["hour_of_day"] <= 5
    return df


def build_centerline_speed_master(speed_df, cl_dir, raw_segments, match_master):
    seg = raw_segments[["split_id", "raw_edge_id", "roadseg_id", "length_m"]].copy()
    seg = seg.rename(columns={"length_m": "seg_len_m"})

    mm = match_master[
        [
            "split_id",
            "raw_edge_id",
            "matched_final",
            "skel_dir_final",
            "cline_id_final",
            "dir_final",
        ]
    ].copy()
    mm = mm.rename(
        columns={
            "matched_final": "matched",
            "skel_dir_final": "skel_dir",
            "cline_id_final": "cline_id",
            "dir_final": "dir",
        }
    )

    seg_match = seg.merge(mm, on=["split_id", "raw_edge_id"], how="left")

    speed_local = speed_df.copy()
    speed_local["roadseg_id"] = speed_local["roadseg_id"].astype("string")
    speed_local["speed"] = pd.to_numeric(speed_local["speed"], errors="coerce")

    seg_match["roadseg_id"] = seg_match["roadseg_id"].astype("string")
    seg_match["seg_len_m"] = pd.to_numeric(seg_match["seg_len_m"], errors="coerce")

    cl_obs = speed_local.merge(
        seg_match[["roadseg_id", "matched", "seg_len_m", "skel_dir", "cline_id", "dir"]],
        on="roadseg_id",
        how="left",
    )

    cl_obs["matched"] = pd.to_numeric(cl_obs["matched"], errors="coerce")
    cl_obs["seg_len_m"] = pd.to_numeric(cl_obs["seg_len_m"], errors="coerce")
    cl_obs["speed"] = pd.to_numeric(cl_obs["speed"], errors="coerce")

    cl_obs = cl_obs[
        (cl_obs["matched"] == 1)
        & (cl_obs["seg_len_m"] > 0)
        & (cl_obs["speed"] > 0)
        & cl_obs["dt"].notna()
    ].copy()
    cl_obs["travel_time_hours"] = cl_obs["seg_len_m"] / cl_obs["speed"]

    cl_dir_local = cl_dir.copy()
    cl_dir_local["cl_len_m"] = cl_dir_local.geometry.length.astype(float)

    cl_speed = (
        cl_obs.groupby(["skel_dir", "cline_id", "dir", "weekday_label", "hour_of_day"], as_index=False)
        .agg(
            total_dist=("seg_len_m", "sum"),
            total_time=("travel_time_hours", "sum"),
            n_obs=("roadseg_id", "size"),
        )
    )
    cl_speed["cl_speed_kmh"] = np.where(
        cl_speed["total_time"] > 0,
        cl_speed["total_dist"] / cl_speed["total_time"],
        np.nan,
    )
    cl_speed = cl_speed.rename(
        columns={
            "total_dist": "total_dist_m",
            "total_time": "total_time_h",
        }
    )

    cl_speed["is_weekday"] = cl_speed["weekday_label"] == "weekday"
    cl_speed["is_am_peak"] = (cl_speed["hour_of_day"] >= 7) & (cl_speed["hour_of_day"] < 9)
    cl_speed["is_pm_peak"] = (cl_speed["hour_of_day"] >= 17) & (cl_speed["hour_of_day"] < 19)
    cl_speed["is_freeflow_2205"] = (cl_speed["hour_of_day"] >= 22) | (cl_speed["hour_of_day"] <= 5)
    cl_speed["is_freeflow_0005"] = cl_speed["hour_of_day"] <= 5
    cl_speed["period"] = np.select(
        [cl_speed["is_am_peak"], cl_speed["is_pm_peak"], cl_speed["is_freeflow_2205"], cl_speed["is_freeflow_0005"]],
        ["AM", "PM", "FF_2205", "FF_0005"],
        default="OTHER",
    )

    cl_speed = cl_speed.merge(
        cl_dir_local[["skel_dir", "cline_id", "dir", "cl_len_m"]],
        on=["skel_dir", "cline_id", "dir"],
        how="left",
    )

    cl_speed["has_valid_speed"] = pd.to_numeric(cl_speed["cl_speed_kmh"], errors="coerce").gt(0)
    cl_speed["sample_keep_baseline"] = cl_speed["has_valid_speed"]
    cl_speed["sample_keep_relaxed"] = cl_speed["has_valid_speed"]
    cl_speed["source_version"] = "stage03_baseline"
    return cl_speed, cl_obs


def save_metrics(cl_speed: pd.DataFrame, cl_obs: pd.DataFrame, metrics_dir: Path):
    metrics_dir.mkdir(parents=True, exist_ok=True)

    summary = pd.DataFrame(
        [
            {
                "matched_speed_observations": int(len(cl_obs)),
                "centerline_time_rows": int(len(cl_speed)),
                "unique_centerlines": int(cl_speed[["skel_dir", "cline_id", "dir"]].drop_duplicates().shape[0]),
                "weekday_rows": int(cl_speed["is_weekday"].sum()),
                "weekend_rows": int((~cl_speed["is_weekday"]).sum()),
                "am_peak_rows": int(cl_speed["is_am_peak"].sum()),
                "pm_peak_rows": int(cl_speed["is_pm_peak"].sum()),
                "mean_speed_kmh": float(pd.to_numeric(cl_speed["cl_speed_kmh"], errors="coerce").mean()),
                "median_speed_kmh": float(pd.to_numeric(cl_speed["cl_speed_kmh"], errors="coerce").median()),
            }
        ]
    )
    summary.to_csv(metrics_dir / "stage03_speed_summary.csv", index=False)

    by_period = (
        cl_speed.groupby("period", as_index=False)
        .agg(
            n=("skel_dir", "count"),
            mean_speed_kmh=("cl_speed_kmh", "mean"),
            median_speed_kmh=("cl_speed_kmh", "median"),
        )
    )
    by_period.to_csv(metrics_dir / "stage03_speed_by_period.csv", index=False)


def run(config_path: str | None, version_id: str, output_dir: str):
    version_root = Path(output_dir) / version_id
    data_dir = version_root / "data"
    metrics_dir = version_root / "metrics"
    figures_dir = version_root / "figures"

    data_dir.mkdir(parents=True, exist_ok=True)
    metrics_dir.mkdir(parents=True, exist_ok=True)
    figures_dir.mkdir(parents=True, exist_ok=True)

    print(f"[stage03] version_id={version_id}")
    print(f"[stage03] config={config_path}")
    print(f"[stage03] output_root={version_root}")
    print(f"[stage03] data_dir={data_dir}")
    print(f"[stage03] metrics_dir={metrics_dir}")
    print(f"[stage03] figures_dir={figures_dir}")

    save_config_snapshot(version_root, config_path)

    cl_dir, raw_segments, match_master = load_stage_inputs(version_root)
    speed_df = load_speed_data()
    cl_speed, cl_obs = build_centerline_speed_master(speed_df, cl_dir, raw_segments, match_master)

    out_path = data_dir / "centerline_speed_master.parquet"
    cl_speed.to_parquet(out_path, index=False)
    save_metrics(cl_speed, cl_obs, metrics_dir)

    print(f"[stage03] loaded speed observations: {len(speed_df):,}")
    print(f"[stage03] matched speed observations: {len(cl_obs):,}")
    print(f"[stage03] centerline-time rows: {len(cl_speed):,}")
    print(f"[stage03] saved centerline_speed_master: {out_path}")


def main():
    args = parse_args()
    run(args.config, args.version_id, args.output_dir)


if __name__ == "__main__":
    main()
