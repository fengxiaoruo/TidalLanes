"""
Rerun only the projection-fallback step for stage02.

This is a debug helper for tuning projection matching on the baseline
unmatched subset without recomputing the baseline first-pass match.
"""

import argparse
import shutil
import sys
from pathlib import Path

import geopandas as gpd
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


from src.stages.stage02_match_raw_to_centerline import (
    align_match_ids_to_stage01,
    build_match_master,
    filter_by_flag,
    prepare_directed_centerline_for_matching,
    projection_fallback_segments,
    save_config_snapshot,
    save_metrics,
)


def parse_args():
    parser = argparse.ArgumentParser(description="Stage02 projection-only debug rerun")
    parser.add_argument("--source-version", required=True, help="Version to read baseline stage02 outputs from.")
    parser.add_argument("--target-version", required=True, help="Version to write the projection-debug outputs to.")
    parser.add_argument("--output-dir", default="outputs", help="Base output directory.")
    parser.add_argument(
        "--copy-stage01",
        action="store_true",
        help="Also copy stage01 centerline outputs and metrics into the target version.",
    )
    return parser.parse_args()


def ensure_dirs(version_root: Path):
    for name in ["data", "metrics", "figures"]:
        (version_root / name).mkdir(parents=True, exist_ok=True)


def maybe_copy(src: Path, dst: Path):
    if src.exists():
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)


def copy_stage01_artifacts(source_root: Path, target_root: Path):
    for rel in [
        Path("data/centerline_master.parquet"),
        Path("data/centerline_dir_master.parquet"),
        Path("metrics/stage01_centerline_summary.csv"),
        Path("metrics/stage01_direction_source_summary.csv"),
    ]:
        maybe_copy(source_root / rel, target_root / rel)


def run(source_version: str, target_version: str, output_dir: str, copy_stage01: bool):
    root = Path(output_dir)
    source_root = root / source_version
    target_root = root / target_version
    ensure_dirs(target_root)

    if copy_stage01:
        copy_stage01_artifacts(source_root, target_root)

    raw_segment_master = gpd.read_parquet(source_root / "data" / "raw_segment_master.parquet")
    match_source = pd.read_parquet(source_root / "data" / "raw_to_centerline_match_master.parquet")
    centerline_dir = gpd.read_parquet(source_root / "data" / "centerline_dir_master.parquet")
    cl_dir_match = prepare_directed_centerline_for_matching(filter_by_flag(centerline_dir, "keep_baseline"))

    baseline_df = match_source[
        [
            "split_id",
            "raw_edge_id",
            "matched_old",
            "skel_dir_old",
            "cline_id_old",
            "dir_old",
            "score_old",
            "angle_diff_old",
            "dist_mean_old",
            "candidate_count_old",
            "s_from_old",
            "s_to_old",
        ]
    ].copy()

    unmatched_input = raw_segment_master.loc[raw_segment_master["keep_baseline"].fillna(False)].copy()
    unmatched_input = unmatched_input.merge(
        baseline_df[["split_id", "matched_old"]],
        on="split_id",
        how="left",
    )
    unmatched_input["matched_old"] = pd.to_numeric(unmatched_input["matched_old"], errors="coerce").fillna(0).astype(int)
    unmatched_input = unmatched_input.loc[unmatched_input["matched_old"] == 0].drop(columns=["matched_old"])

    projection_df = projection_fallback_segments(unmatched_input, cl_dir_match) if len(unmatched_input) else pd.DataFrame()
    raw2split = raw_segment_master[["raw_edge_id", "split_id", "raw_seg_idx"]].copy()
    match_master = build_match_master(raw2split, baseline_df, projection_df, raw_segment_master)
    match_master = align_match_ids_to_stage01(match_master, centerline_dir)

    split2cl = match_master[
        [
            "split_id",
            "raw_edge_id",
            "matched_final",
            "skel_dir_final",
            "cline_id_final",
            "dir_final",
            "dist_mean_final",
            "angle_diff_final",
            "s_from",
            "s_to",
        ]
    ].rename(
        columns={
            "matched_final": "matched",
            "skel_dir_final": "skel_dir",
            "cline_id_final": "cline_id",
            "dir_final": "dir",
            "dist_mean_final": "dist_mean",
            "angle_diff_final": "angle_diff",
        }
    )

    save_config_snapshot(target_root, None)
    raw2split.to_parquet(target_root / "data" / "xwalk_raw_to_split.parquet", index=False)
    split2cl.to_parquet(target_root / "data" / "xwalk_split_to_centerline.parquet", index=False)
    raw_segment_master.merge(split2cl, on=["split_id", "raw_edge_id"], how="left").to_parquet(
        target_root / "data" / "raw_split_centerline.parquet",
        index=False,
    )
    raw_segment_master.to_parquet(target_root / "data" / "raw_segment_master.parquet", index=False)
    match_master.to_parquet(target_root / "data" / "raw_to_centerline_match_master.parquet", index=False)
    save_metrics(raw_segment_master, match_master, target_root / "metrics")

    projection_added = int(((match_master["matched_old"] == 0) & (match_master["matched_proj"] == 1)).sum())
    baseline_unmatched = int((match_master["matched_old"] == 0).sum())
    final_unmatched = int((match_master["matched_final"] == 0).sum())
    print(f"[stage02_projection_debug] source_version={source_version}")
    print(f"[stage02_projection_debug] target_version={target_version}")
    print(f"[stage02_projection_debug] baseline_unmatched={baseline_unmatched}")
    print(f"[stage02_projection_debug] projection_added_matches={projection_added}")
    print(f"[stage02_projection_debug] final_unmatched={final_unmatched}")


def main():
    args = parse_args()
    run(args.source_version, args.target_version, args.output_dir, args.copy_stage01)


if __name__ == "__main__":
    main()
