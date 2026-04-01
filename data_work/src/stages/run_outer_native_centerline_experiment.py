import argparse
import json
import sys
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import LineString, MultiLineString, Point
from shapely.ops import linemerge


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import src.stages.stage02_match_raw_to_centerline as s2
from src.stages.stage01_build_centerline import (
    MIN_CL_LEN,
    TIANANMEN_LONLAT,
    TARGET_EPSG,
    build_centerline_from_raw,
    build_directed_centerline,
    finalize_centerline,
    geom_bearing_full,
    load_raw_roads,
    save_metrics as save_stage01_metrics,
)


def parse_args():
    parser = argparse.ArgumentParser(description="Run outer native-centerline experiments")
    parser.add_argument("--version-id", required=True)
    parser.add_argument("--output-dir", default="outputs")
    parser.add_argument("--native-min-length", type=float, default=3000.0)
    parser.add_argument("--native-min-center-dist", type=float, default=10000.0)
    parser.add_argument("--native-opposite-tol", type=float, default=20.0)
    parser.add_argument("--split-sample-step", type=float, default=200.0)
    parser.add_argument("--split-search-dist", type=float, default=80.0)
    parser.add_argument("--cut-buf", type=float, default=40.0)
    parser.add_argument("--snap-tol", type=float, default=30.0)
    parser.add_argument("--min-seg-gap", type=float, default=5.0)
    parser.add_argument("--proj-search-dist", type=float, default=120.0)
    return parser.parse_args()


def ang_diff_abs(a, b):
    return abs((a - b + 180) % 360 - 180)


def to_single_linestring(geom):
    if geom is None or geom.is_empty:
        return None
    if geom.geom_type == "LineString":
        return geom
    if geom.geom_type == "MultiLineString":
        merged = linemerge(geom)
        if merged.is_empty:
            return None
        if merged.geom_type == "LineString":
            return merged
    return None


def canonical_geom_key(geom):
    if geom is None or geom.is_empty:
        return None
    try:
        return geom.normalize().wkb_hex
    except Exception:
        return geom.wkb_hex


def center_point_3857():
    return gpd.GeoSeries([Point(TIANANMEN_LONLAT)], crs="EPSG:4326").to_crs(TARGET_EPSG).iloc[0]


def identify_outer_native_pairs(raw: gpd.GeoDataFrame, min_length: float, min_center_dist: float, opposite_tol: float):
    use = raw.copy()
    use["geometry"] = use.geometry.apply(to_single_linestring)
    use = use.loc[use["geometry"].notna()].copy()
    use["length_m"] = use.geometry.length.astype(float)
    use = use.loc[use["length_m"] >= min_length].copy()
    ctr = center_point_3857()
    use["dist_center_m"] = use.geometry.apply(lambda g: g.interpolate(0.5, normalized=True).distance(ctr))
    use = use.loc[use["dist_center_m"] >= min_center_dist].copy()
    use["geom_bear"] = use.geometry.apply(geom_bearing_full).astype(float)
    use["geom_key"] = use.geometry.apply(canonical_geom_key)
    use = use.loc[use["geom_key"].notna()].copy()

    pair_rows = []
    map_rows = []
    reps = []
    pair_id = 0
    for _, grp in use.groupby("geom_key", dropna=True):
        if len(grp) != 2:
            continue
        a = grp.iloc[0]
        b = grp.iloc[1]
        geom_gap = ang_diff_abs(float(a["geom_bear"]), float(b["geom_bear"]))
        if abs(geom_gap - 180.0) > opposite_tol:
            continue
        pair_id += 1
        len_m = float(max(a["length_m"], b["length_m"]))
        rep_geom = a["geometry"]
        pair_rows.extend(
            [
                {
                    "pair_id": pair_id,
                    "raw_edge_id": int(a["raw_edge_id"]),
                    "roadname": a.get("roadname"),
                    "roadtype": a.get("roadtype"),
                    "len_m": float(a["length_m"]),
                    "dist_ctr": float(a["dist_center_m"]),
                    "geom_gap": float(geom_gap),
                    "geometry": a["geometry"],
                },
                {
                    "pair_id": pair_id,
                    "raw_edge_id": int(b["raw_edge_id"]),
                    "roadname": b.get("roadname"),
                    "roadtype": b.get("roadtype"),
                    "len_m": float(b["length_m"]),
                    "dist_ctr": float(b["dist_center_m"]),
                    "geom_gap": float(geom_gap),
                    "geometry": b["geometry"],
                },
            ]
        )
        reps.append(
            {
                "pair_id": pair_id,
                "raw_a": int(a["raw_edge_id"]),
                "raw_b": int(b["raw_edge_id"]),
                "roadname": a.get("roadname") if pd.notna(a.get("roadname")) else b.get("roadname"),
                "roadtype": a.get("roadtype"),
                "length_m": len_m,
                "dist_center_m": float(max(a["dist_center_m"], b["dist_center_m"])),
                "geometry": rep_geom,
            }
        )
        map_rows.extend(
            [
                {"pair_id": pair_id, "raw_edge_id": int(a["raw_edge_id"]), "dir": "AB"},
                {"pair_id": pair_id, "raw_edge_id": int(b["raw_edge_id"]), "dir": "BA"},
            ]
        )

    pair_gdf = gpd.GeoDataFrame(pair_rows, geometry="geometry", crs=raw.crs)
    rep_gdf = gpd.GeoDataFrame(reps, geometry="geometry", crs=raw.crs)
    map_df = pd.DataFrame(map_rows)
    return pair_gdf, rep_gdf, map_df


def append_native_centerlines(centerline: gpd.GeoDataFrame, rep_gdf: gpd.GeoDataFrame):
    if rep_gdf.empty:
        return centerline.copy(), pd.DataFrame(columns=["pair_id", "raw_edge_id", "cline_id", "dir"])

    base = centerline.copy()
    next_id = int(base["cline_id"].max()) + 1 if len(base) else 0
    reps = rep_gdf.copy().reset_index(drop=True)
    reps["cline_id"] = np.arange(next_id, next_id + len(reps), dtype=int)
    reps["is_linestring"] = True
    reps["length_m"] = reps.geometry.length.astype(float)
    reps["is_short_centerline"] = reps["length_m"] < MIN_CL_LEN
    reps["is_valid_geometry"] = reps.geometry.notna() & (~reps.geometry.is_empty)
    reps["keep_baseline"] = reps["is_valid_geometry"] & (~reps["is_short_centerline"])
    reps["keep_relaxed"] = reps["is_valid_geometry"]
    reps["keep_qsm"] = reps["keep_baseline"]
    reps["source_version"] = "stage01_native_outer_pair"
    reps = reps[base.columns]
    out = pd.concat([base, reps], ignore_index=True)
    return out, rep_gdf.merge(reps[["cline_id"]], left_index=True, right_index=True)[["pair_id", "cline_id"]]


def build_prelinked_segments(raw: gpd.GeoDataFrame, map_df: pd.DataFrame, pair_to_cline: pd.DataFrame, split_offset: int):
    if map_df.empty:
        return gpd.GeoDataFrame(columns=list(raw.columns) + ["raw_seg_idx", "split_id", "raw_geometry", "need_split"], geometry="geometry", crs=raw.crs), pd.DataFrame()

    direct = raw.merge(map_df, on="raw_edge_id", how="inner").merge(pair_to_cline, on="pair_id", how="left")
    direct = direct.copy()
    direct["raw_seg_idx"] = 0
    direct["need_split"] = False
    direct["raw_geometry"] = direct.geometry
    direct["split_id"] = np.arange(split_offset, split_offset + len(direct), dtype=int)
    return direct, direct[["split_id", "raw_edge_id", "cline_id", "dir"]].copy()


def prelinked_baseline_df(prelinked_segments: gpd.GeoDataFrame, cl_dir_match: gpd.GeoDataFrame):
    if prelinked_segments.empty:
        return pd.DataFrame(
            columns=[
                "split_id",
                "raw_edge_id",
                "matched_old",
                "skel_dir_old",
                "cline_id_old",
                "dir_old",
                "score_old",
                "dist_mean_old",
                "angle_diff_old",
                "candidate_count_old",
                "s_from_old",
                "s_to_old",
            ]
        )
    ref = cl_dir_match[["skel_dir", "cline_id", "dir"]].copy()
    direct = prelinked_segments.merge(ref, on=["cline_id", "dir"], how="left")
    rows = []
    for row in direct.itertuples(index=False):
        rows.append(
            {
                "split_id": int(row.split_id),
                "raw_edge_id": int(row.raw_edge_id),
                "matched_old": 1,
                "skel_dir_old": int(row.skel_dir),
                "cline_id_old": int(row.cline_id),
                "dir_old": row.dir,
                "score_old": 999.0,
                "dist_mean_old": 0.0,
                "angle_diff_old": 0.0,
                "candidate_count_old": 1,
                "s_from_old": 0.0,
                "s_to_old": float(row.geometry.length),
            }
        )
    return pd.DataFrame(rows)


def save_native_review(version_root: Path, pair_gdf: gpd.GeoDataFrame, rep_gdf: gpd.GeoDataFrame):
    out_dir = version_root / "gis_exports" / "native_outer_pairs_review"
    out_dir.mkdir(parents=True, exist_ok=True)
    if not pair_gdf.empty:
        pair_out = pair_gdf.rename(columns={"raw_edge_id": "raw_id", "roadname": "road_nm", "roadtype": "rtype"}).to_crs(4326)
        pair_out.to_file(out_dir / "raw_outer_native_pairs.shp", driver="ESRI Shapefile", encoding="UTF-8")
    if not rep_gdf.empty:
        rep_out = rep_gdf.rename(columns={"roadname": "road_nm", "roadtype": "rtype", "dist_center_m": "dist_ctr"}).to_crs(4326)
        rep_out.to_file(out_dir / "centerline_seed_outer_pairs.shp", driver="ESRI Shapefile", encoding="UTF-8")


def save_experiment_config(version_root: Path, args):
    payload = {
        "stage": "outer_native_centerline_experiment",
        "native_min_length": args.native_min_length,
        "native_min_center_dist": args.native_min_center_dist,
        "native_opposite_tol": args.native_opposite_tol,
        "split_sample_step": args.split_sample_step,
        "split_search_dist": args.split_search_dist,
        "cut_buf": args.cut_buf,
        "snap_tol": args.snap_tol,
        "min_seg_gap": args.min_seg_gap,
        "proj_search_dist": args.proj_search_dist,
    }
    (version_root / "config_snapshot.experiment.json").write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def run(args):
    version_root = Path(args.output_dir) / args.version_id
    data_dir = version_root / "data"
    metrics_dir = version_root / "metrics"
    figures_dir = version_root / "figures"
    data_dir.mkdir(parents=True, exist_ok=True)
    metrics_dir.mkdir(parents=True, exist_ok=True)
    figures_dir.mkdir(parents=True, exist_ok=True)
    save_experiment_config(version_root, args)

    s2.CUT_BUF = float(args.cut_buf)
    s2.SNAP_TOL = float(args.snap_tol)
    s2.MIN_SEG_GAP = float(args.min_seg_gap)
    s2.SPLIT_SAMPLE_STEP = float(args.split_sample_step)
    s2.SPLIT_SEARCH_DIST = float(args.split_search_dist)
    s2.PROJ_SEARCH_DIST = float(args.proj_search_dist)

    raw = load_raw_roads()
    raw_valid = raw.loc[raw["is_valid_geometry"] & raw["is_linestring"]].copy()
    pair_gdf, rep_gdf, map_df = identify_outer_native_pairs(
        raw_valid,
        min_length=float(args.native_min_length),
        min_center_dist=float(args.native_min_center_dist),
        opposite_tol=float(args.native_opposite_tol),
    )
    save_native_review(version_root, pair_gdf, rep_gdf)

    excluded_raw_ids = set(map_df["raw_edge_id"].tolist())
    raw_for_raster = raw_valid.loc[~raw_valid["raw_edge_id"].isin(excluded_raw_ids)].copy()

    centerline = build_centerline_from_raw(raw_for_raster)
    centerline = finalize_centerline(centerline)
    centerline, pair_to_cline = append_native_centerlines(centerline, rep_gdf)
    cl_dir = build_directed_centerline(centerline)

    centerline.to_parquet(data_dir / "centerline_master.parquet", index=False)
    cl_dir.to_parquet(data_dir / "centerline_dir_master.parquet", index=False)
    save_stage01_metrics(raw, centerline, cl_dir, metrics_dir)

    raw_for_stage02 = raw_valid.loc[~raw_valid["raw_edge_id"].isin(excluded_raw_ids)].copy()
    centerline_keep = s2.filter_by_flag(centerline, "keep_baseline")
    cl_dir_match = s2.prepare_directed_centerline_for_matching(s2.filter_by_flag(cl_dir, "keep_baseline"))
    raw_for_stage02 = s2.infer_need_split(raw_for_stage02, cl_dir_match)
    normal_segments = s2.split_raw_segments(raw_for_stage02, centerline_keep)
    prelinked_segments, prelinked_map = build_prelinked_segments(
        raw_valid.loc[raw_valid["raw_edge_id"].isin(excluded_raw_ids)].copy(),
        map_df,
        pair_to_cline,
        split_offset=int(normal_segments["split_id"].max()) + 1 if len(normal_segments) else 0,
    )
    raw_segments = pd.concat([normal_segments, prelinked_segments], ignore_index=True)
    raw_segments = gpd.GeoDataFrame(raw_segments, geometry="geometry", crs=raw_valid.crs)
    raw_segment_master = s2.build_raw_segment_master(raw_segments)
    match_input = raw_segment_master.loc[raw_segment_master["keep_baseline"].fillna(False)].copy()

    normal_match_input = match_input.loc[~match_input["raw_edge_id"].isin(excluded_raw_ids)].copy()
    baseline_df = s2.baseline_match_segments(normal_match_input, cl_dir_match)
    baseline_df = pd.concat([baseline_df, prelinked_baseline_df(prelinked_segments, cl_dir_match)], ignore_index=True)

    unmatched_input = match_input.merge(baseline_df[["split_id", "matched_old"]], on="split_id", how="left")
    unmatched_input["matched_old"] = pd.to_numeric(unmatched_input["matched_old"], errors="coerce").fillna(0).astype(int)
    unmatched_input = unmatched_input.loc[(unmatched_input["matched_old"] == 0) & (~unmatched_input["raw_edge_id"].isin(excluded_raw_ids))].drop(columns=["matched_old"])
    projection_df = s2.projection_fallback_segments(unmatched_input, cl_dir_match) if len(unmatched_input) else pd.DataFrame(columns=["split_id", "raw_edge_id"])

    raw2split = raw_segment_master[["raw_edge_id", "split_id", "raw_seg_idx"]].copy()
    match_master = s2.build_match_master(raw2split, baseline_df, projection_df, raw_segment_master)

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

    raw2split.to_parquet(data_dir / "xwalk_raw_to_split.parquet", index=False)
    split2cl.to_parquet(data_dir / "xwalk_split_to_centerline.parquet", index=False)
    raw_segment_master.merge(split2cl, on=["split_id", "raw_edge_id"], how="left").to_parquet(data_dir / "raw_split_centerline.parquet", index=False)
    raw_segment_master.to_parquet(data_dir / "raw_segment_master.parquet", index=False)
    match_master.to_parquet(data_dir / "raw_to_centerline_match_master.parquet", index=False)
    if not map_df.empty:
        map_df.merge(pair_to_cline, on="pair_id", how="left").to_parquet(data_dir / "prelinked_raw_centerline.parquet", index=False)
    s2.save_metrics(raw_segment_master, match_master, metrics_dir)

    native_summary = pd.DataFrame(
        [
            {
                "native_pairs": int(len(rep_gdf)),
                "native_raw_edges": int(len(map_df)),
                "native_centerline_length_m": float(rep_gdf.geometry.length.sum()) if len(rep_gdf) else 0.0,
                "native_min_length": float(args.native_min_length),
                "native_min_center_dist": float(args.native_min_center_dist),
            }
        ]
    )
    native_summary.to_csv(metrics_dir / "stage01_native_outer_summary.csv", index=False)

    print(f"[experiment] version={args.version_id}")
    print(f"[experiment] native_pairs={len(rep_gdf):,}")
    print(f"[experiment] native_raw_edges={len(map_df):,}")
    print(f"[experiment] split_segments={len(raw_segment_master):,}")
    print(f"[experiment] final_match_rate={match_master['matched_final'].mean():.2%}")


def main():
    args = parse_args()
    run(args)


if __name__ == "__main__":
    main()
