"""
Stage 02: Match Raw to Centerline

Purpose:
- Split raw roads into matchable segments
- Run baseline raw-to-centerline matching
- Add fallback projection-based matching for unmatched segments
- Preserve old, fallback, and final chosen matches side by side

Planned inputs:
- outputs/{version_id}/data/centerline_master.parquet
- outputs/{version_id}/data/centerline_dir_master.parquet
- raw_data/gis/roads_baidu/beijing_roads.shp

Planned outputs:
- outputs/{version_id}/data/raw_segment_master.parquet
- outputs/{version_id}/data/raw_to_centerline_match_master.parquet

Current source notebook:
- code/01_Match_Final.ipynb
"""

import argparse
import json
import sys
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import LineString, MultiPoint, Point, Polygon
from shapely.ops import split, substring, unary_union
from tqdm import tqdm

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.stages.stage01_build_centerline import geom_bearing_full, load_raw_roads

TARGET_EPSG = 3857
MAX_DIST = 60.0
CUT_BUF = 40.0
SNAP_TOL = 30.0
MIN_SEG_GAP = 5.0
SPLIT_SAMPLE_STEP = 200.0
SPLIT_SEARCH_DIST = 80.0
NODE_SNAP_TOL = 35.0
NODE_BUFFER = 40.0
MERGE_CUT_DIST = 30.0
DISABLE_CUT_MERGE = False
SAMPLE_STEP = 30.0
W_DIST = 1.0
W_ANG = 0.1
DIST_CAP = 180.0
MIN_SEG_LEN_BASELINE = 50.0
PROJ_SEARCH_DIST = 120.0
PROJ_BUF = 18.0
PROJ_DIST_PENALTY = 0.15
PROJ_CLOSE_DIST = 60.0
PROJ_MIN_CLOSE_SHARE = 0.30
PROJ_MAX_AREA_PER_LENGTH = 180.0
LONG_UNMATCHED_THRESHOLD_M = 1000.0
LONG_PROJ_CLOSE_DIST = 120.0
LONG_PROJ_MIN_CLOSE_SHARE = 0.15
LONG_PROJ_MAX_AREA_PER_LENGTH = 400.0
ROADTYPE_LANE_MAP = {
    2: 6.0,  # expressway
    3: 4.0,  # arterial
    4: 2.0,  # secondary arterial
}


def parse_args():
    parser = argparse.ArgumentParser(description="Stage 02: Match raw roads to centerline")
    parser.add_argument("--config", default=None, help="Optional config file path.")
    parser.add_argument("--version-id", required=True, help="Version identifier for outputs.")
    parser.add_argument(
        "--output-dir",
        default="outputs",
        help="Base output directory for versioned results.",
    )
    parser.add_argument(
        "--manual-overrides",
        default=None,
        help="Optional CSV with manual split/raw to centerline overrides.",
    )
    return parser.parse_args()


def save_config_snapshot(version_root: Path, config_path: str | None, manual_overrides_path: str | None):
    payload = {
        "stage": "stage02_match_raw_to_centerline",
        "config_path": config_path,
        "target_epsg": TARGET_EPSG,
        "execution_mode": "raw_only",
        "min_seg_len_baseline": MIN_SEG_LEN_BASELINE,
        "cut_buf": CUT_BUF,
        "snap_tol": SNAP_TOL,
        "min_seg_gap": MIN_SEG_GAP,
        "split_sample_step": SPLIT_SAMPLE_STEP,
        "split_search_dist": SPLIT_SEARCH_DIST,
        "node_snap_tol": NODE_SNAP_TOL,
        "node_buffer": NODE_BUFFER,
        "merge_cut_dist": MERGE_CUT_DIST,
        "disable_cut_merge": DISABLE_CUT_MERGE,
        "manual_overrides_path": manual_overrides_path,
        "baseline_matching_mode": "notebook_consistent_distance_cap_only",
        "dist_cap": DIST_CAP,
        "proj_search_dist": PROJ_SEARCH_DIST,
        "projection_mode": "integrated_projection_area",
        "proj_buffer": PROJ_BUF,
        "proj_close_dist": PROJ_CLOSE_DIST,
        "proj_min_close_share": PROJ_MIN_CLOSE_SHARE,
        "proj_max_area_per_length": PROJ_MAX_AREA_PER_LENGTH,
        "long_unmatched_threshold_m": LONG_UNMATCHED_THRESHOLD_M,
        "long_proj_close_dist": LONG_PROJ_CLOSE_DIST,
        "long_proj_min_close_share": LONG_PROJ_MIN_CLOSE_SHARE,
        "long_proj_max_area_per_length": LONG_PROJ_MAX_AREA_PER_LENGTH,
        "roadtype_lane_map": ROADTYPE_LANE_MAP,
        "roadtype_lane_sensitivity": {
            "2": [6.0, 8.0],
            "3": [4.0, 6.0],
            "4": [2.0, 4.0],
        },
    }
    (version_root / "config_snapshot.stage02.json").write_text(
        json.dumps(payload, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )


def resolve_manual_override_split_ids(match_master: pd.DataFrame, overrides: pd.DataFrame) -> pd.DataFrame:
    use = overrides.copy()
    if "split_id" in use.columns:
        use["split_id"] = pd.to_numeric(use["split_id"], errors="coerce").astype("Int64")
    else:
        use["split_id"] = pd.Series(pd.NA, index=use.index, dtype="Int64")

    if "raw_edge_id" in use.columns:
        use["raw_edge_id"] = pd.to_numeric(use["raw_edge_id"], errors="coerce").astype("Int64")
        raw_only = use["split_id"].isna() & use["raw_edge_id"].notna()
        if raw_only.any():
            single_split = (
                match_master.groupby("raw_edge_id")["split_id"]
                .agg(["count", "first"])
                .reset_index()
                .rename(columns={"first": "_single_split_id"})
            )
            single_split = single_split.loc[single_split["count"] == 1, ["raw_edge_id", "_single_split_id"]]
            use = use.merge(single_split, on="raw_edge_id", how="left")
            use.loc[raw_only, "split_id"] = use.loc[raw_only, "_single_split_id"].astype("Int64")
            use = use.drop(columns=["_single_split_id"], errors="ignore")
    return use


def apply_manual_overrides(
    match_master: pd.DataFrame,
    centerline_dir: gpd.GeoDataFrame,
    manual_overrides_path: str | None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not manual_overrides_path:
        return match_master, pd.DataFrame()

    path = Path(manual_overrides_path)
    if not path.exists():
        print(f"[stage02] manual overrides file not found, skipping: {path}")
        return match_master, pd.DataFrame()

    overrides = pd.read_csv(path)
    if overrides.empty:
        print(f"[stage02] manual overrides file is empty, skipping: {path}")
        return match_master, overrides

    required = {"cline_id", "dir"}
    if not required.issubset(overrides.columns):
        raise ValueError(f"Manual overrides must include columns: {sorted(required)}")
    if "split_id" not in overrides.columns and "raw_edge_id" not in overrides.columns:
        raise ValueError("Manual overrides must include split_id or raw_edge_id.")

    use = resolve_manual_override_split_ids(match_master, overrides)
    use = use.loc[use["split_id"].notna()].copy()
    if use.empty:
        print(f"[stage02] manual overrides resolved to zero split_ids: {path}")
        return match_master, overrides

    use["cline_id"] = pd.to_numeric(use["cline_id"], errors="coerce").astype("Int64")
    use["dir"] = use["dir"].astype("string").str.upper()
    use["s_from"] = pd.to_numeric(use.get("s_from"), errors="coerce")
    use["s_to"] = pd.to_numeric(use.get("s_to"), errors="coerce")
    use["note"] = use.get("note", pd.Series("", index=use.index)).astype("string")

    cl_ref = centerline_dir[["cline_id", "dir", "skel_dir"]].copy()
    cl_ref["cline_id"] = pd.to_numeric(cl_ref["cline_id"], errors="coerce").astype("Int64")
    cl_ref["dir"] = cl_ref["dir"].astype("string").str.upper()
    cl_ref["skel_dir"] = pd.to_numeric(cl_ref["skel_dir"], errors="coerce").astype("Int64")
    use = use.merge(cl_ref, on=["cline_id", "dir"], how="left")
    unresolved = int(use["skel_dir"].isna().sum())
    if unresolved:
        print(f"[stage02] manual overrides with unresolved cline_id/dir skipped: {unresolved}")
    use = use.loc[use["skel_dir"].notna()].copy()
    if use.empty:
        return match_master, overrides

    manual = use[["split_id", "cline_id", "dir", "skel_dir", "s_from", "s_to", "note"]].drop_duplicates(
        subset=["split_id"],
        keep="last",
    )

    out = match_master.copy()
    merged = out.merge(manual, on="split_id", how="left", suffixes=("", "_manual"))
    mask = merged["skel_dir_manual"].notna()
    if mask.any():
        merged.loc[mask, "match_method_final"] = "manual_override"
        merged.loc[mask, "matched_final"] = 1
        merged.loc[mask, "skel_dir_final"] = merged.loc[mask, "skel_dir_manual"].astype("Int64")
        merged.loc[mask, "cline_id_final"] = merged.loc[mask, "cline_id_manual"].astype("Int64")
        merged.loc[mask, "dir_final"] = merged.loc[mask, "dir_manual"].astype("string")
        merged.loc[mask, "score_final"] = 0.0
        merged.loc[mask, "dist_mean_final"] = 0.0
        merged.loc[mask, "angle_diff_final"] = np.nan
        merged.loc[mask, "review_flag"] = 0
        merged.loc[mask, "s_from"] = merged.loc[mask, "s_from_manual"].combine_first(merged.loc[mask, "s_from"])
        merged.loc[mask, "s_to"] = merged.loc[mask, "s_to_manual"].combine_first(merged.loc[mask, "s_to"])
        print(f"[stage02] applied manual overrides: {int(mask.sum())}")

    merged = merged.drop(
        columns=[
            "cline_id_manual",
            "dir_manual",
            "skel_dir_manual",
            "s_from_manual",
            "s_to_manual",
            "note",
        ],
        errors="ignore",
    )
    return merged, manual


def export_manual_override_review(
    raw_segment_master: gpd.GeoDataFrame,
    match_master: pd.DataFrame,
    metrics_dir: Path,
):
    review = raw_segment_master.merge(
        match_master[["split_id", "matched_final"]],
        on="split_id",
        how="left",
    )
    review = review.loc[review["keep_baseline"].fillna(False) & (review["matched_final"] == 0)].copy()
    if review.empty:
        return
    review["segment_length_m"] = review.geometry.length
    cols = [
        "split_id",
        "raw_edge_id",
        "raw_seg_idx",
        "roadseg_id",
        "roadname",
        "roadtype",
        "need_split",
        "is_split",
        "segment_length_m",
    ]
    cols = [c for c in cols if c in review.columns]
    review[cols].sort_values("segment_length_m", ascending=False).to_csv(
        metrics_dir / "manual_override_review_candidates.csv",
        index=False,
    )


def ensure_3857(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if gdf.crs is None:
        raise ValueError("GeoDataFrame CRS is missing.")
    if gdf.crs.to_epsg() != TARGET_EPSG:
        return gdf.to_crs(epsg=TARGET_EPSG)
    return gdf


def line_endpoints(ls):
    coords = list(ls.coords)
    return Point(coords[0]), Point(coords[-1])


def angle_diff(a, b):
    if pd.isna(a) or pd.isna(b):
        return np.nan
    return abs((a - b + 180) % 360 - 180)


def extract_centerline_nodes(centerline: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    node_rows = []
    for row in centerline.itertuples(index=False):
        ls = row.geometry
        if ls.is_empty or ls.geom_type != "LineString":
            continue
        coords = list(ls.coords)
        if len(coords) < 2:
            continue
        node_rows.append({"node_key": coords[0], "geometry": Point(coords[0])})
        node_rows.append({"node_key": coords[-1], "geometry": Point(coords[-1])})

    if not node_rows:
        return gpd.GeoDataFrame(columns=["node_key", "degree", "is_major_node", "geometry"], geometry="geometry", crs=centerline.crs)

    nodes = gpd.GeoDataFrame(node_rows, geometry="geometry", crs=centerline.crs)
    degree = nodes.groupby("node_key").size().rename("degree").reset_index()
    nodes = nodes.drop_duplicates(subset=["node_key"]).merge(degree, on="node_key", how="left")
    nodes["is_major_node"] = nodes["degree"].fillna(0).astype(int) != 2
    return nodes


def midpoint_cut_positions_from_node_buffer(ls: LineString, node_pts, node_buffer: float):
    if ls.is_empty or ls.length <= 0:
        return []

    s_vals = []
    for pt in node_pts:
        inter = ls.intersection(pt.buffer(node_buffer))
        if inter.is_empty:
            continue
        if inter.geom_type == "Point":
            s_vals.append(float(ls.project(inter)))
            continue
        if inter.geom_type == "MultiPoint":
            for g in inter.geoms:
                s_vals.append(float(ls.project(g)))
            continue
        if inter.geom_type == "LineString":
            mid = inter.interpolate(0.5, normalized=True)
            s_vals.append(float(ls.project(mid)))
            continue
        for g in getattr(inter, "geoms", []):
            if g.geom_type == "Point":
                s_vals.append(float(ls.project(g)))
            elif g.geom_type == "LineString" and g.length > 0:
                mid = g.interpolate(0.5, normalized=True)
                s_vals.append(float(ls.project(mid)))
    return s_vals


def merge_cut_positions(s_vals, merge_dist: float, line_length: float):
    if not s_vals:
        return []
    use = sorted(float(s) for s in s_vals if 1.0 < float(s) < line_length - 1.0)
    if not use:
        return []
    if DISABLE_CUT_MERGE:
        out = []
        for s in use:
            if not out or abs(s - out[-1]) > 1e-6:
                out.append(s)
        return out

    groups = [[use[0]]]
    for s in use[1:]:
        if s - groups[-1][-1] <= merge_dist:
            groups[-1].append(s)
        else:
            groups.append([s])
    return [float(np.mean(g)) for g in groups]


def sampled_nearest_cline_ids(ls: LineString, cl_gdf: gpd.GeoDataFrame, cl_sindex, step: float, search_dist: float):
    if ls.is_empty or ls.length <= 0:
        return []

    L = float(ls.length)
    s_vals = list(np.arange(0.0, L, step))
    if (not s_vals) or (s_vals[-1] < L):
        s_vals.append(L)

    out = []
    for s in s_vals:
        pt = ls.interpolate(float(s))
        cand_idx = list(cl_sindex.query(pt.buffer(search_dist)))
        if not cand_idx:
            out.append((float(s), None))
            continue
        cand = cl_gdf.iloc[cand_idx]
        d = cand.distance(pt)
        j = int(np.argmin(d.values))
        dmin = float(d.iloc[j])
        if dmin > search_dist:
            out.append((float(s), None))
            continue
        out.append((float(s), int(cand.iloc[j]["cline_id"])))
    return out


def sample_change_positions(ls: LineString, cl_gdf: gpd.GeoDataFrame, cl_sindex, step: float, search_dist: float):
    sampled = sampled_nearest_cline_ids(ls, cl_gdf, cl_sindex, step=step, search_dist=search_dist)
    if not sampled:
        return []

    valid = [(s, cid) for s, cid in sampled if cid is not None]
    if len(valid) < 2:
        return []

    change_s = []
    prev_s, prev_cid = valid[0]
    for s, cid in valid[1:]:
        if cid != prev_cid:
            change_s.append(0.5 * (prev_s + s))
        prev_s, prev_cid = s, cid
    return change_s


def sample_dist_mean(seg: LineString, ls: LineString) -> float:
    L = seg.length
    ds = list(np.arange(0, L, SAMPLE_STEP))
    if (not ds) or (ds[-1] < L):
        ds.append(L)
    dists = [ls.distance(seg.interpolate(d)) for d in ds] if L > 0 else [ls.distance(seg)]
    return float(np.mean(dists)) if len(dists) else float(ls.distance(seg))


def project_span(seg: LineString, ls: LineString):
    pS, pT = line_endpoints(seg)
    s0 = float(ls.project(pS))
    s1 = float(ls.project(pT))
    s_from, s_to = (s0, s1) if s0 <= s1 else (s1, s0)
    return s_from, s_to


def load_stage01_outputs(version_root: Path):
    data_dir = version_root / "data"
    centerline_path = data_dir / "centerline_master.parquet"
    centerline_dir_path = data_dir / "centerline_dir_master.parquet"

    if not centerline_path.exists():
        raise FileNotFoundError(f"Stage01 output missing: {centerline_path}")
    if not centerline_dir_path.exists():
        raise FileNotFoundError(f"Stage01 output missing: {centerline_dir_path}")

    centerline = gpd.read_parquet(centerline_path)
    centerline_dir = gpd.read_parquet(centerline_dir_path)
    centerline = ensure_3857(centerline)
    centerline_dir = ensure_3857(centerline_dir)
    return centerline, centerline_dir


def prepare_directed_centerline_for_matching(centerline_dir: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    cl_dir = centerline_dir.copy()
    cl_dir["buf"] = cl_dir.geometry.buffer(MAX_DIST)
    return cl_dir


def infer_need_split(raw: gpd.GeoDataFrame, cl_dir: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    cl_sindex = cl_dir.sindex
    node_gdf = extract_centerline_nodes(cl_dir)
    node_major = node_gdf.loc[node_gdf["is_major_node"]].copy()
    node_sindex = node_major.sindex if len(node_major) else None

    def nearest_cline_id(pt):
        cand_idx = list(cl_sindex.query(pt.buffer(MAX_DIST)))
        if not cand_idx:
            return None
        cand = cl_dir.iloc[cand_idx]
        d = cand.distance(pt)
        j = int(np.argmin(d.values))
        return int(cand.iloc[j]["cline_id"])

    need_split = []
    for row in tqdm(raw.itertuples(index=False), total=len(raw), desc="infer_need_split"):
        ls = row.geometry
        if ls.is_empty or ls.geom_type != "LineString":
            need_split.append(False)
            continue
        p0, p1 = line_endpoints(ls)
        c0 = nearest_cline_id(p0)
        c1 = nearest_cline_id(p1)
        endpoint_change = c0 is not None and c1 is not None and c0 != c1
        sample_pairs = sampled_nearest_cline_ids(ls, cl_dir, cl_sindex, step=SPLIT_SAMPLE_STEP, search_dist=SPLIT_SEARCH_DIST)
        sampled_ids = [cid for _, cid in sample_pairs if cid is not None]
        sampled_change = len(set(sampled_ids)) >= 2
        major_node_hit = False
        if node_sindex is not None:
            cand_idx = list(node_sindex.query(ls.buffer(SPLIT_SEARCH_DIST)))
            if cand_idx:
                sel = node_major.iloc[cand_idx]
                s_vals = midpoint_cut_positions_from_node_buffer(ls, sel.geometry.values, NODE_BUFFER)
                major_node_hit = len(merge_cut_positions(s_vals, MERGE_CUT_DIST, float(ls.length))) > 0
        need_split.append(endpoint_change or sampled_change or major_node_hit)

    out = raw.copy()
    out["need_split"] = need_split
    return out


def split_raw_segments(raw: gpd.GeoDataFrame, centerline: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    cl_endpts = []
    for ls in centerline.geometry.values:
        if ls.is_empty or ls.geom_type != "LineString":
            continue
        cs = list(ls.coords)
        cl_endpts.extend([Point(cs[0]), Point(cs[-1])])

    cl_endpts_gdf = gpd.GeoDataFrame(geometry=cl_endpts, crs=centerline.crs)
    cl_endpts_sindex = cl_endpts_gdf.sindex
    cl_union = unary_union(centerline.geometry)
    cl_sindex = centerline.sindex
    node_gdf = extract_centerline_nodes(centerline)
    node_major = node_gdf.loc[node_gdf["is_major_node"]].copy()
    node_sindex = node_major.sindex if len(node_major) else None

    def cut_one(row):
        ls = row.geometry
        if (not row.need_split) or ls.is_empty or ls.length <= 0:
            return [ls]

        buf = ls.buffer(CUT_BUF)
        cand_idx = list(cl_endpts_sindex.query(buf))

        pts = []
        if cand_idx:
            sel = cl_endpts_gdf.iloc[cand_idx]
            for p in sel.geometry.values:
                if p.distance(ls) <= SNAP_TOL:
                    pts.append(p)

        inter = ls.intersection(cl_union)
        if not inter.is_empty:
            if inter.geom_type == "Point":
                if inter.distance(ls) <= SNAP_TOL:
                    pts.append(inter)
            elif inter.geom_type == "LineString":
                coords = list(inter.coords)
                if len(coords) >= 2:
                    pts.extend([Point(coords[0]), Point(coords[-1])])
            else:
                for g in getattr(inter, "geoms", []):
                    if g.geom_type == "Point" and g.distance(ls) <= SNAP_TOL:
                        pts.append(g)
                    elif g.geom_type == "LineString":
                        coords = list(g.coords)
                        if len(coords) >= 2:
                            pts.extend([Point(coords[0]), Point(coords[-1])])

        # Project major centerline nodes onto raw so splitting follows the network graph.
        if node_sindex is not None:
            cand_idx = list(node_sindex.query(ls.buffer(SPLIT_SEARCH_DIST)))
            if cand_idx:
                sel = node_major.iloc[cand_idx]
                for s in midpoint_cut_positions_from_node_buffer(ls, sel.geometry.values, NODE_BUFFER):
                    if 1.0 < s < ls.length - 1.0:
                        pts.append(ls.interpolate(s))

        # For long curved roads, split where the locally nearest centerline changes.
        for s in sample_change_positions(
            ls,
            centerline,
            cl_sindex,
            step=SPLIT_SAMPLE_STEP,
            search_dist=SPLIT_SEARCH_DIST,
        ):
            if 1.0 < s < ls.length - 1.0:
                pts.append(ls.interpolate(float(s)))

        if not pts:
            return [ls]

        L = ls.length
        s_vals = [ls.project(p) for p in pts if 1.0 < ls.project(p) < L - 1.0]
        s_clean = merge_cut_positions(s_vals, max(MIN_SEG_GAP, MERGE_CUT_DIST), L)
        if not s_clean:
            return [ls]

        cut_pts = MultiPoint([ls.interpolate(s) for s in s_clean])
        try:
            parts = split(ls, cut_pts)
            return [g for g in parts.geoms if g.length > 0]
        except Exception:
            return [ls]

    split_rows = []
    for row in tqdm(raw.itertuples(index=False), total=len(raw), desc="split_raw_segments"):
        segs = cut_one(row)
        for k, seg in enumerate(segs):
            split_rows.append({"raw_edge_id": int(row.raw_edge_id), "raw_seg_idx": int(k), "geometry": seg})

    split_all = gpd.GeoDataFrame(split_rows, geometry="geometry", crs=raw.crs).reset_index(drop=True)
    split_all["split_id"] = split_all.index.astype(int)

    raw_cols = [c for c in raw.columns if c != "geometry"]
    split_with_raw = split_all.merge(
        raw[raw_cols + ["geometry"]].rename(columns={"geometry": "_raw_geom"}),
        on="raw_edge_id",
        how="left",
    ).rename(columns={"_raw_geom": "raw_geometry"})
    return split_with_raw


def baseline_match_segments(split_with_raw: gpd.GeoDataFrame, cl_dir: gpd.GeoDataFrame) -> pd.DataFrame:
    cl_sindex = cl_dir.sindex

    def best_dir_for_segment(seg: LineString, seg_dir_deg: float):
        if seg.is_empty or seg.geom_type != "LineString" or seg.length <= 0:
            return None

        pS, pT = line_endpoints(seg)
        bearing_seg = geom_bearing_full(seg) if pd.isna(seg_dir_deg) else float(seg_dir_deg)

        def score_one(ls, bear):
            d_mean = sample_dist_mean(seg, ls)
            a = angle_diff(bearing_seg, bear)
            if not np.isfinite(a):
                a = 180.0
            return W_DIST * d_mean + W_ANG * a, float(a), float(d_mean)

        radii = [MAX_DIST, MAX_DIST * 3.0, MAX_DIST * 10.0]
        cand_idx = []
        for rr in radii:
            idx = list(cl_sindex.query(seg.buffer(rr)))
            if idx:
                cand_idx = idx
                break
        if not cand_idx:
            return None

        cand = cl_dir.iloc[cand_idx].copy()
        sel = cand[cand["buf"].intersects(seg)]
        cand = sel if not sel.empty else cand
        candidate_count = int(len(cand))
        if candidate_count == 0:
            return None

        best = None
        for cr in cand.itertuples(index=False):
            s, a, d = score_one(cr.geometry, cr.bear)
            if (best is None) or (s < best[0]):
                s_from, s_to = project_span(seg, cr.geometry)
                best = (s, a, d, int(cr.skel_dir), int(cr.cline_id), cr.dir, s_from, s_to)

        if best is None:
            return None

        s, a, d, skel_dir, cline_id, direc, s_from, s_to = best
        # Match notebook behavior: baseline match only hard-rejects segments
        # whose mean perpendicular distance exceeds DIST_CAP.
        if (d is None) or (not np.isfinite(d)) or (float(d) > float(DIST_CAP)):
            return None

        return {
            "matched_old": 1,
            "skel_dir_old": skel_dir,
            "cline_id_old": cline_id,
            "dir_old": direc,
            "score_old": float(s),
            "angle_diff_old": float(a),
            "dist_mean_old": float(d),
            "candidate_count_old": candidate_count,
            "s_from_old": float(s_from),
            "s_to_old": float(s_to),
        }

    match_rows = []
    dir_col = "dir_deg" if "dir_deg" in split_with_raw.columns else "dir_deg_final"
    for row in tqdm(split_with_raw.itertuples(index=False), total=len(split_with_raw), desc="baseline_match"):
        seg = row.geometry
        res = best_dir_for_segment(seg, getattr(row, dir_col, np.nan))
        rec = {"split_id": int(row.split_id), "raw_edge_id": int(row.raw_edge_id)}
        if res is None:
            rec.update(
                {
                    "matched_old": 0,
                    "skel_dir_old": None,
                    "cline_id_old": None,
                    "dir_old": None,
                    "score_old": None,
                    "angle_diff_old": None,
                    "dist_mean_old": None,
                    "candidate_count_old": 0,
                    "s_from_old": None,
                    "s_to_old": None,
                }
            )
        else:
            rec.update(res)
        match_rows.append(rec)

    return pd.DataFrame(match_rows)


def projection_fallback_segments(split_with_raw: gpd.GeoDataFrame, cl_dir: gpd.GeoDataFrame) -> pd.DataFrame:
    cl_sindex = cl_dir.sindex

    def projection_area_metrics(seg: LineString, ls: LineString, close_dist: float):
        L = float(seg.length)
        if L <= 0:
            return {
                "proj_area": np.nan,
                "proj_area_per_length": np.nan,
                "proj_mean_dist": np.nan,
                "proj_max_dist": np.nan,
                "proj_p90_dist": np.nan,
                "proj_close_share": np.nan,
            }

        ds = list(np.arange(0, L, SAMPLE_STEP))
        if (not ds) or (ds[-1] < L):
            ds.append(L)

        seg_pts = [seg.interpolate(d) for d in ds]
        proj_pts = [ls.interpolate(float(ls.project(pt))) for pt in seg_pts]
        dists = np.asarray([float(a.distance(b)) for a, b in zip(seg_pts, proj_pts)], dtype=float)

        quad_area = 0.0
        for p0, p1, q0, q1 in zip(seg_pts[:-1], seg_pts[1:], proj_pts[:-1], proj_pts[1:]):
            poly = Polygon(
                [
                    (p0.x, p0.y),
                    (p1.x, p1.y),
                    (q1.x, q1.y),
                    (q0.x, q0.y),
                ]
            )
            if poly.is_empty:
                continue
            quad_area += abs(float(poly.area))

        return {
            "proj_area": float(quad_area),
            "proj_area_per_length": float(quad_area / L) if L > 0 else np.nan,
            "proj_mean_dist": float(dists.mean()) if len(dists) else np.nan,
            "proj_max_dist": float(dists.max()) if len(dists) else np.nan,
            "proj_p90_dist": float(np.quantile(dists, 0.90)) if len(dists) else np.nan,
            "proj_close_share": float((dists <= close_dist).mean()) if len(dists) else np.nan,
        }

    def score_projection(seg: LineString, seg_dir_deg: float, cand_row):
        seg_bear = geom_bearing_full(seg) if pd.isna(seg_dir_deg) else float(seg_dir_deg)
        a = angle_diff(seg_bear, cand_row.bear)
        seg_len = float(seg.length)
        is_long_unmatched = seg_len > LONG_UNMATCHED_THRESHOLD_M
        close_dist = LONG_PROJ_CLOSE_DIST if is_long_unmatched else PROJ_CLOSE_DIST
        min_close_share = LONG_PROJ_MIN_CLOSE_SHARE if is_long_unmatched else PROJ_MIN_CLOSE_SHARE
        max_area_per_length = LONG_PROJ_MAX_AREA_PER_LENGTH if is_long_unmatched else PROJ_MAX_AREA_PER_LENGTH

        s_from, s_to = project_span(seg, cand_row.geometry)
        span = max(0.0, float(s_to - s_from))
        if span <= 0:
            return None

        proj_line = substring(cand_row.geometry, s_from, s_to)
        if proj_line.is_empty or proj_line.length <= 0:
            return None

        area_metrics = projection_area_metrics(seg, cand_row.geometry, close_dist)
        d_mean = float(area_metrics["proj_mean_dist"])
        area_per_length = float(area_metrics["proj_area_per_length"])
        close_share = float(area_metrics["proj_close_share"])
        p90_dist = float(area_metrics["proj_p90_dist"])
        if not np.isfinite(d_mean) or d_mean > DIST_CAP:
            return None
        if (not np.isfinite(area_per_length)) or area_per_length > max_area_per_length:
            return None
        if (not np.isfinite(close_share)) or close_share < min_close_share:
            return None

        score = float(
            close_share
            - PROJ_DIST_PENALTY * min(area_per_length, PROJ_SEARCH_DIST) / PROJ_SEARCH_DIST
        )
        return {
            "score_proj": score,
            "proj_overlap_area": np.nan,
            "proj_overlap_share": np.nan,
            "proj_area": float(area_metrics["proj_area"]),
            "proj_area_per_length": area_per_length,
            "proj_length_ratio": np.nan,
            "proj_close_share": close_share,
            "dist_mean_proj": float(d_mean),
            "dist_max_proj": float(area_metrics["proj_max_dist"]),
            "dist_p90_proj": p90_dist,
            "projection_rule_proj": "long_segment_relaxed" if is_long_unmatched else "default",
            "angle_diff_proj": float(a) if np.isfinite(a) else np.nan,
            "skel_dir_proj": int(cand_row.skel_dir),
            "cline_id_proj": int(cand_row.cline_id),
            "dir_proj": cand_row.dir,
            "s_from_proj": float(s_from),
            "s_to_proj": float(s_to),
        }

    rows = []
    dir_col = "dir_deg" if "dir_deg" in split_with_raw.columns else "dir_deg_final"
    for row in tqdm(split_with_raw.itertuples(index=False), total=len(split_with_raw), desc="projection_fallback"):
        seg = row.geometry
        rec = {"split_id": int(row.split_id), "raw_edge_id": int(row.raw_edge_id)}
        if seg.is_empty or seg.geom_type != "LineString" or seg.length <= 0:
            rec.update(
                {
                    "matched_proj": 0,
                    "skel_dir_proj": None,
                    "cline_id_proj": None,
                    "dir_proj": None,
                    "score_proj": None,
                    "proj_overlap_area": None,
                    "proj_overlap_share": None,
                    "proj_area": None,
                    "proj_area_per_length": None,
                    "proj_length_ratio": None,
                    "proj_close_share": None,
                    "candidate_count_proj": 0,
                    "dist_mean_proj": None,
                    "dist_max_proj": None,
                    "dist_p90_proj": None,
                    "projection_rule_proj": None,
                    "angle_diff_proj": None,
                    "s_from_proj": None,
                    "s_to_proj": None,
                }
            )
            rows.append(rec)
            continue

        cand_idx = list(cl_sindex.query(seg.buffer(PROJ_SEARCH_DIST)))
        cand = cl_dir.iloc[cand_idx].copy() if cand_idx else cl_dir.iloc[[]].copy()
        if not cand.empty:
            cand = cand[cand.distance(seg) <= PROJ_SEARCH_DIST].copy()
        best = None
        candidate_count = int(len(cand))
        for cand_row in cand.itertuples(index=False):
            scored = score_projection(seg, getattr(row, dir_col, np.nan), cand_row)
            if scored is None:
                continue
            if (best is None) or (scored["score_proj"] > best["score_proj"]):
                best = scored

        if best is None:
            rec.update(
                {
                    "matched_proj": 0,
                    "skel_dir_proj": None,
                    "cline_id_proj": None,
                    "dir_proj": None,
                    "score_proj": None,
                    "proj_overlap_area": None,
                    "proj_overlap_share": None,
                    "proj_area": None,
                    "proj_area_per_length": None,
                    "proj_length_ratio": None,
                    "proj_close_share": None,
                    "candidate_count_proj": candidate_count,
                    "dist_mean_proj": None,
                    "dist_max_proj": None,
                    "dist_p90_proj": None,
                    "projection_rule_proj": None,
                    "angle_diff_proj": None,
                    "s_from_proj": None,
                    "s_to_proj": None,
                }
            )
        else:
            best["matched_proj"] = 1
            best["candidate_count_proj"] = candidate_count
            rec.update(best)
        rows.append(rec)

    return pd.DataFrame(rows)


def build_raw_segment_master(raw_segments: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    gdf = raw_segments.copy()
    gdf["length_m"] = gdf.geometry.length.astype(float)
    gdf["is_valid_geometry"] = gdf.geometry.notna() & (~gdf.geometry.is_empty)
    gdf["is_linestring"] = gdf.geometry.geom_type.isin(["LineString", "MultiLineString"])
    gdf["is_split"] = gdf.groupby("raw_edge_id")["split_id"].transform("count") > 1
    gdf["is_short_segment"] = gdf["length_m"] < MIN_SEG_LEN_BASELINE
    gdf["is_dead_end"] = pd.NA
    gdf["road_class"] = gdf["roadtype"] if "roadtype" in gdf.columns else pd.NA
    gdf["road_class_lane_mean"] = pd.to_numeric(gdf["road_class"], errors="coerce").map(ROADTYPE_LANE_MAP)
    if "dir_deg" in gdf.columns and "dir_deg_final" not in gdf.columns:
        gdf = gdf.rename(columns={"dir_deg": "dir_deg_final"})
    if "dir_deg_sem" not in gdf.columns:
        gdf["dir_deg_sem"] = pd.NA
    if "bear_geom" not in gdf.columns:
        gdf["bear_geom"] = pd.NA
    if "dir_deg_final" not in gdf.columns:
        gdf["dir_deg_final"] = pd.NA
    if "dir_source" not in gdf.columns:
        gdf["dir_source"] = pd.NA
    if "need_split" not in gdf.columns:
        gdf["need_split"] = pd.NA
    gdf["keep_baseline"] = gdf["is_valid_geometry"] & gdf["is_linestring"] & (~gdf["is_short_segment"])
    gdf["keep_relaxed"] = gdf["is_valid_geometry"] & gdf["is_linestring"]
    gdf["keep_qsm"] = gdf["keep_baseline"]
    gdf["source_version"] = "stage02_raw_build"
    return gdf


def build_match_master(
    raw2split: pd.DataFrame,
    baseline_df: pd.DataFrame,
    projection_df: pd.DataFrame,
    raw_segments: gpd.GeoDataFrame,
) -> pd.DataFrame:
    base = raw2split.merge(baseline_df, on=["split_id", "raw_edge_id"], how="left")
    base = base.merge(projection_df, on=["split_id", "raw_edge_id"], how="left")
    base = base.merge(
        raw_segments[["split_id", "keep_baseline", "keep_relaxed", "keep_qsm"]],
        on="split_id",
        how="left",
    )

    if "roadseg_id" in raw_segments.columns:
        base = base.merge(raw_segments[["split_id", "roadseg_id"]], on="split_id", how="left")
    else:
        base["roadseg_id"] = pd.NA

    base["matched_old"] = pd.to_numeric(base["matched_old"], errors="coerce").fillna(0).astype(int)
    base["matched_proj"] = pd.to_numeric(base["matched_proj"], errors="coerce").fillna(0).astype(int)

    use_old = base["matched_old"] == 1
    use_proj = (base["matched_old"] == 0) & (base["matched_proj"] == 1)
    final_method = np.select(
        [use_old, use_proj],
        ["baseline_old", "projection_fallback"],
        default="unmatched",
    )
    matched_final = np.where(use_old | use_proj, 1, 0).astype(int)
    skel_dir_final = np.where(use_old, base["skel_dir_old"], np.where(use_proj, base["skel_dir_proj"], pd.NA))
    cline_id_final = np.where(use_old, base["cline_id_old"], np.where(use_proj, base["cline_id_proj"], pd.NA))
    dir_final = np.where(use_old, base["dir_old"], np.where(use_proj, base["dir_proj"], pd.NA))
    score_final = np.where(use_old, base["score_old"], np.where(use_proj, base["score_proj"], np.nan))
    dist_mean_final = np.where(use_old, base["dist_mean_old"], np.where(use_proj, base["dist_mean_proj"], np.nan))
    angle_diff_final = np.where(use_old, base["angle_diff_old"], np.where(use_proj, base["angle_diff_proj"], np.nan))
    s_from_final = np.where(use_old, base["s_from_old"], np.where(use_proj, base["s_from_proj"], np.nan))
    s_to_final = np.where(use_old, base["s_to_old"], np.where(use_proj, base["s_to_proj"], np.nan))

    out = pd.DataFrame(
        {
            "split_id": base["split_id"],
            "raw_edge_id": base["raw_edge_id"],
            "roadseg_id": base["roadseg_id"],
            "keep_baseline": base["keep_baseline"],
            "keep_relaxed": base["keep_relaxed"],
            "keep_qsm": base["keep_qsm"],
            "matched_old": base["matched_old"],
            "skel_dir_old": base["skel_dir_old"],
            "cline_id_old": base["cline_id_old"],
            "dir_old": base["dir_old"],
            "score_old": pd.to_numeric(base["score_old"], errors="coerce"),
            "dist_mean_old": pd.to_numeric(base["dist_mean_old"], errors="coerce"),
            "angle_diff_old": pd.to_numeric(base["angle_diff_old"], errors="coerce"),
            "candidate_count_old": pd.to_numeric(base["candidate_count_old"], errors="coerce"),
            "s_from_old": pd.to_numeric(base["s_from_old"], errors="coerce"),
            "s_to_old": pd.to_numeric(base["s_to_old"], errors="coerce"),
            "matched_proj": base["matched_proj"],
            "skel_dir_proj": base["skel_dir_proj"],
            "cline_id_proj": base["cline_id_proj"],
            "dir_proj": base["dir_proj"],
            "score_proj": pd.to_numeric(base["score_proj"], errors="coerce"),
            "proj_overlap_area": pd.to_numeric(base["proj_overlap_area"], errors="coerce"),
            "proj_overlap_share": pd.to_numeric(base["proj_overlap_share"], errors="coerce"),
            "proj_area": pd.to_numeric(base["proj_area"], errors="coerce"),
            "proj_area_per_length": pd.to_numeric(base["proj_area_per_length"], errors="coerce"),
            "proj_length_ratio": pd.to_numeric(base["proj_length_ratio"], errors="coerce"),
            "proj_close_share": pd.to_numeric(base["proj_close_share"], errors="coerce"),
            "candidate_count_proj": pd.to_numeric(base["candidate_count_proj"], errors="coerce"),
            "dist_mean_proj": pd.to_numeric(base["dist_mean_proj"], errors="coerce"),
            "dist_max_proj": pd.to_numeric(base["dist_max_proj"], errors="coerce"),
            "dist_p90_proj": pd.to_numeric(base["dist_p90_proj"], errors="coerce"),
            "projection_rule_proj": base["projection_rule_proj"],
            "angle_diff_proj": pd.to_numeric(base["angle_diff_proj"], errors="coerce"),
            "s_from_proj": pd.to_numeric(base["s_from_proj"], errors="coerce"),
            "s_to_proj": pd.to_numeric(base["s_to_proj"], errors="coerce"),
            "match_method_final": final_method,
            "matched_final": matched_final,
            "skel_dir_final": skel_dir_final,
            "cline_id_final": cline_id_final,
            "dir_final": dir_final,
            "score_final": pd.to_numeric(score_final, errors="coerce"),
            "dist_mean_final": pd.to_numeric(dist_mean_final, errors="coerce"),
            "angle_diff_final": pd.to_numeric(angle_diff_final, errors="coerce"),
            "review_flag": 0,
            "match_conflict_flag": 0,
            "s_from": pd.to_numeric(s_from_final, errors="coerce"),
            "s_to": pd.to_numeric(s_to_final, errors="coerce"),
            "source_version": "stage02_raw_build",
        }
    )

    out["match_conflict_flag"] = np.where(
        (out["matched_old"] == 1)
        & (out["matched_proj"] == 1)
        & (
            out["skel_dir_old"].astype("string").fillna("<NA>")
            != out["skel_dir_proj"].astype("string").fillna("<NA>")
        ),
        1,
        0,
    )
    out["review_flag"] = np.where(
        (out["match_method_final"] == "projection_fallback")
        & (
            out["proj_area_per_length"].isna()
            | (out["proj_area_per_length"] > 120.0)
            | out["proj_close_share"].isna()
            | (out["proj_close_share"] < 0.5)
            | (pd.to_numeric(out["candidate_count_proj"], errors="coerce").fillna(0) >= 10)
        ),
        1,
        0,
    )
    out.loc[out["matched_final"] == 0, ["skel_dir_final", "cline_id_final", "dir_final"]] = pd.NA
    return out


def align_match_ids_to_stage01(match_master: pd.DataFrame, centerline_dir: gpd.GeoDataFrame) -> pd.DataFrame:
    use = match_master.copy()
    dir_ref = centerline_dir[["skel_dir", "cline_id", "dir"]].copy()
    for col in ["skel_dir"]:
        use[col + "_old"] = pd.to_numeric(use.get(col + "_old"), errors="coerce").astype("Int64")
        use[col + "_proj"] = pd.to_numeric(use.get(col + "_proj"), errors="coerce").astype("Int64")
        use[col + "_final"] = pd.to_numeric(use.get(col + "_final"), errors="coerce").astype("Int64")
    for col in ["cline_id_old", "cline_id_proj", "cline_id_final"]:
        use[col] = pd.to_numeric(use.get(col), errors="coerce").astype("Int64")
    dir_ref["skel_dir"] = pd.to_numeric(dir_ref["skel_dir"], errors="coerce").astype("Int64")
    dir_ref["cline_id"] = pd.to_numeric(dir_ref["cline_id"], errors="coerce").astype("Int64")
    dir_ref = dir_ref.rename(
        columns={
            "skel_dir": "skel_dir_stage01",
            "cline_id": "cline_id_stage01",
            "dir": "dir_stage01",
        }
    )

    old_map = dir_ref.rename(
        columns={
            "skel_dir_stage01": "skel_dir_old",
            "cline_id_stage01": "cline_id_old_ref",
            "dir_stage01": "dir_old_ref",
        }
    )
    use = use.merge(old_map, on="skel_dir_old", how="left")

    final_map = dir_ref.rename(
        columns={
            "skel_dir_stage01": "skel_dir_final",
            "cline_id_stage01": "cline_id_final_ref",
            "dir_stage01": "dir_final_ref",
        }
    )
    use = use.merge(final_map, on="skel_dir_final", how="left")

    use["stage01_old_key_found"] = use["cline_id_old_ref"].notna().astype(int)
    use["stage01_final_key_found"] = use["cline_id_final_ref"].notna().astype(int)

    use["cline_id_old"] = use["cline_id_old_ref"].combine_first(use["cline_id_old"])
    use["dir_old"] = use["dir_old_ref"].combine_first(use["dir_old"])
    use["cline_id_final"] = use["cline_id_final_ref"].combine_first(use["cline_id_final"])
    use["dir_final"] = use["dir_final_ref"].combine_first(use["dir_final"])

    use = use.drop(
        columns=[
            "cline_id_old_ref",
            "dir_old_ref",
            "cline_id_final_ref",
            "dir_final_ref",
        ]
    )
    return use


def filter_by_flag(df, flag_col: str):
    if flag_col in df.columns:
        return df[df[flag_col].fillna(False)].copy()
    return df.copy()


def save_metrics(raw_segment_master: gpd.GeoDataFrame, match_master: pd.DataFrame, metrics_dir: Path):
    metrics_dir.mkdir(parents=True, exist_ok=True)

    baseline_mask = match_master["keep_baseline"].fillna(False) if "keep_baseline" in match_master.columns else pd.Series(True, index=match_master.index)
    relaxed_mask = match_master["keep_relaxed"].fillna(False) if "keep_relaxed" in match_master.columns else pd.Series(True, index=match_master.index)

    old_rate = float(match_master.loc[baseline_mask, "matched_old"].mean()) if baseline_mask.any() else np.nan
    proj_rate = float(match_master.loc[baseline_mask, "matched_proj"].mean()) if baseline_mask.any() else np.nan
    match_rate = float(match_master.loc[baseline_mask, "matched_final"].mean()) if baseline_mask.any() else np.nan
    raw_edge_rate = (
        float(match_master.loc[baseline_mask].groupby("raw_edge_id")["matched_final"].max().mean())
        if baseline_mask.any()
        else np.nan
    )
    relaxed_match_rate = float(match_master.loc[relaxed_mask, "matched_final"].mean()) if relaxed_mask.any() else np.nan

    summary = pd.DataFrame(
        [
            {
                "split_segments": int(len(raw_segment_master)),
                "raw_edges": int(raw_segment_master["raw_edge_id"].nunique()),
                "split_segments_keep_baseline": int(raw_segment_master["keep_baseline"].fillna(False).sum()),
                "split_segments_keep_relaxed": int(raw_segment_master["keep_relaxed"].fillna(False).sum()),
                "split_match_rate_old": old_rate,
                "split_match_rate_proj_only": proj_rate,
                "split_match_rate": match_rate,
                "split_match_rate_relaxed": relaxed_match_rate,
                "raw_edge_match_rate": raw_edge_rate,
                "projection_uplift_pp": (match_rate - old_rate) * 100.0 if np.isfinite(match_rate) and np.isfinite(old_rate) else np.nan,
                "projection_added_matches": int(((match_master["matched_old"] == 0) & (match_master["matched_proj"] == 1) & baseline_mask).sum()),
                "matched_split_segments": int(match_master.loc[baseline_mask, "matched_final"].sum()),
                "unmatched_split_segments": int(((match_master["matched_final"] == 0) & baseline_mask).sum()),
                "projection_review_flagged": int(((match_master["review_flag"] == 1) & baseline_mask).sum()),
            }
        ]
    )
    summary.to_csv(metrics_dir / "stage02_match_summary.csv", index=False)

    by_dir = (
        match_master.loc[baseline_mask].groupby(["dir_final", "match_method_final"], dropna=False)
        .agg(
            n=("split_id", "count"),
            matched=("matched_final", "sum"),
        )
        .reset_index()
    )
    by_dir["match_rate"] = np.where(by_dir["n"] > 0, by_dir["matched"] / by_dir["n"], np.nan)
    by_dir.to_csv(metrics_dir / "stage02_match_by_dir.csv", index=False)

    if "stage01_final_key_found" in match_master.columns:
        key_summary = pd.DataFrame(
            [
                {
                    "stage01_old_key_found_share": float(match_master["stage01_old_key_found"].mean()),
                    "stage01_final_key_found_share": float(match_master["stage01_final_key_found"].mean()),
                }
            ]
        )
        key_summary.to_csv(metrics_dir / "stage02_stage01_alignment_summary.csv", index=False)


def run(config_path: str | None, version_id: str, output_dir: str, manual_overrides_path: str | None = None):
    version_root = Path(output_dir) / version_id
    data_dir = version_root / "data"
    metrics_dir = version_root / "metrics"
    figures_dir = version_root / "figures"

    data_dir.mkdir(parents=True, exist_ok=True)
    metrics_dir.mkdir(parents=True, exist_ok=True)
    figures_dir.mkdir(parents=True, exist_ok=True)

    print(f"[stage02] version_id={version_id}")
    print(f"[stage02] config={config_path}")
    print(f"[stage02] output_root={version_root}")
    print(f"[stage02] data_dir={data_dir}")
    print(f"[stage02] metrics_dir={metrics_dir}")
    print(f"[stage02] figures_dir={figures_dir}")

    save_config_snapshot(version_root, config_path, manual_overrides_path)
    centerline, centerline_dir = load_stage01_outputs(version_root)
    print(f"[stage02] loaded stage01 centerline rows: {len(centerline):,}")
    print(f"[stage02] loaded stage01 directed rows: {len(centerline_dir):,}")

    raw = load_raw_roads()
    raw_match = raw.loc[raw["is_valid_geometry"] & raw["is_linestring"]].copy()
    centerline_keep = filter_by_flag(centerline, "keep_baseline")
    cl_dir_match = prepare_directed_centerline_for_matching(filter_by_flag(centerline_dir, "keep_baseline"))
    raw_match = infer_need_split(raw_match, cl_dir_match)
    raw_segments = split_raw_segments(raw_match, centerline_keep)
    raw_segment_master = build_raw_segment_master(raw_segments)
    match_input = raw_segment_master.loc[raw_segment_master["keep_baseline"].fillna(False)].copy()
    baseline_df = baseline_match_segments(match_input, cl_dir_match)
    unmatched_input = match_input.merge(
        baseline_df[["split_id", "matched_old"]],
        on="split_id",
        how="left",
    )
    unmatched_input["matched_old"] = pd.to_numeric(unmatched_input["matched_old"], errors="coerce").fillna(0).astype(int)
    unmatched_input = unmatched_input.loc[unmatched_input["matched_old"] == 0].drop(columns=["matched_old"])
    projection_df = projection_fallback_segments(unmatched_input, cl_dir_match) if len(unmatched_input) else pd.DataFrame(
        columns=[
            "split_id",
            "raw_edge_id",
            "matched_proj",
            "skel_dir_proj",
            "cline_id_proj",
            "dir_proj",
            "score_proj",
            "proj_overlap_area",
            "proj_overlap_share",
            "proj_area",
            "proj_area_per_length",
            "proj_length_ratio",
            "proj_close_share",
            "candidate_count_proj",
            "dist_mean_proj",
            "dist_max_proj",
            "dist_p90_proj",
            "projection_rule_proj",
            "angle_diff_proj",
            "s_from_proj",
            "s_to_proj",
        ]
    )
    raw2split = raw_segments[["raw_edge_id", "split_id", "raw_seg_idx"]].copy()
    match_master = build_match_master(raw2split, baseline_df, projection_df, raw_segment_master)
    match_master = align_match_ids_to_stage01(match_master, centerline_dir)
    match_master, manual_applied = apply_manual_overrides(match_master, centerline_dir, manual_overrides_path)
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

    raw_segment_path = data_dir / "raw_segment_master.parquet"
    match_master_path = data_dir / "raw_to_centerline_match_master.parquet"
    raw2split_path = data_dir / "xwalk_raw_to_split.parquet"
    split2cl_path = data_dir / "xwalk_split_to_centerline.parquet"
    splitgeo_path = data_dir / "raw_split_centerline.parquet"
    raw2split.to_parquet(raw2split_path, index=False)
    split2cl.to_parquet(split2cl_path, index=False)
    raw_segment_master.merge(split2cl, on=["split_id", "raw_edge_id"], how="left").to_parquet(splitgeo_path, index=False)
    raw_segment_master.to_parquet(raw_segment_path, index=False)
    match_master.to_parquet(match_master_path, index=False)
    save_metrics(raw_segment_master, match_master, metrics_dir)
    export_manual_override_review(raw_segment_master, match_master, metrics_dir)
    if len(manual_applied):
        manual_applied.to_csv(metrics_dir / "manual_overrides_applied.csv", index=False)

    print(f"[stage02] loaded split segments: {len(raw_segment_master):,}")
    print(f"[stage02] loaded match rows: {len(match_master):,}")
    print(f"[stage02] matched_final rate: {match_master['matched_final'].mean():.2%}")
    print(f"[stage02] saved xwalk_raw_to_split: {raw2split_path}")
    print(f"[stage02] saved xwalk_split_to_centerline: {split2cl_path}")
    print(f"[stage02] saved raw_split_centerline: {splitgeo_path}")
    print(f"[stage02] saved raw_segment_master: {raw_segment_path}")
    print(f"[stage02] saved raw_to_centerline_match_master: {match_master_path}")
    print("[stage02] stage02 raw-only migration complete")


def main():
    args = parse_args()
    run(args.config, args.version_id, args.output_dir, args.manual_overrides)


if __name__ == "__main__":
    main()
