"""
Stage 01: Build Centerline

Purpose:
- Build the undirected centerline network from raw roads
- Build the directed centerline network
- Preserve structural flags instead of dropping observations where possible

Planned inputs:
- raw_data/gis/roads_baidu/beijing_roads.shp

Planned outputs:
- outputs/{version_id}/data/centerline_master.parquet
- outputs/{version_id}/data/centerline_dir_master.parquet

Current source notebook:
- code/01_Match_Final.ipynb
"""

import argparse
import json
import re
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import LineString, MultiLineString, Point
from shapely.ops import linemerge, snap, unary_union


ROOT = Path(__file__).resolve().parents[2]
RAW_PATH = ROOT / "raw_data" / "gis" / "roads_baidu" / "beijing_roads.shp"
MANUAL_GROUPS_PATH = ROOT / "config" / "manual_centerline_groups.json"
TARGET_EPSG = 3857
BUF_WIDTH = 50.0
RES = 5.0
SKELETON_MAX_RASTER_MPIX = 600  # auto-tile if full raster would exceed this many megapixels
MIN_CL_LEN = 50.0
TIANANMEN_LONLAT = (116.397389, 39.908722)
MANUAL_MIDLINE_STEP = 50.0
MANUAL_SIMPLIFY_TOL = 10.0
MANUAL_CONNECT_SNAP_M = 35.0
MANUAL_PAIR_MAX_DIST_M = 120.0
DEFAULT_STAGE01_OPTIONS = {
    "manual_exclude_from_skeleton_input": True,
    "manual_replace_default_centerline": False,
    "manual_replace_buffer_m": 80.0,
    "manual_replace_overlap_share": 0.6,
}
DEFAULT_STAGE01_RUNTIME = {
    "raw_path": str(RAW_PATH),
    "manual_groups_path": str(MANUAL_GROUPS_PATH),
    "center_lonlat": [TIANANMEN_LONLAT[0], TIANANMEN_LONLAT[1]],
}

DIR_SINGLE_DEG = {
    "北": 0,
    "东北": 45,
    "东": 90,
    "东南": 135,
    "南": 180,
    "西南": 225,
    "西": 270,
    "西北": 315,
}
PAT_LAST_AFTER_COMMA = re.compile(r".*[,，]\s*([^,，]+)\s*$")
PAT_X_TO_Y = re.compile(r"(?:由)?\s*([东南西北]{1,2})\s*向\s*([东南西北]{1,2})")


def parse_args():
    parser = argparse.ArgumentParser(description="Stage 01: Build centerline")
    parser.add_argument("--config", default=None, help="Optional config file path.")
    parser.add_argument("--version-id", required=True, help="Version identifier for outputs.")
    parser.add_argument(
        "--output-dir",
        default="outputs",
        help="Base output directory for versioned results.",
    )
    return parser.parse_args()


def load_stage_config(config_path: str | None) -> dict:
    opts = DEFAULT_STAGE01_OPTIONS.copy()
    if not config_path:
        return opts
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Stage01 config not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    stage_payload = payload.get("stage01", payload)
    for key in opts:
        if key in stage_payload:
            opts[key] = stage_payload[key]
    opts["manual_exclude_from_skeleton_input"] = bool(opts["manual_exclude_from_skeleton_input"])
    opts["manual_replace_default_centerline"] = bool(opts["manual_replace_default_centerline"])
    opts["manual_replace_buffer_m"] = float(opts["manual_replace_buffer_m"])
    opts["manual_replace_overlap_share"] = float(opts["manual_replace_overlap_share"])
    return opts


def resolve_stage01_runtime(config_path: str | None) -> dict:
    runtime = DEFAULT_STAGE01_RUNTIME.copy()
    if not config_path:
        return runtime
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Stage01 config not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    stage_payload = payload.get("stage01", payload)
    for key in runtime:
        if key in stage_payload:
            runtime[key] = stage_payload[key]
    center_lonlat = stage_payload.get("center_lonlat", runtime["center_lonlat"])
    if isinstance(center_lonlat, (list, tuple)) and len(center_lonlat) == 2:
        runtime["center_lonlat"] = [float(center_lonlat[0]), float(center_lonlat[1])]
    return runtime


def configure_stage01_runtime(config_path: str | None) -> dict:
    global RAW_PATH, MANUAL_GROUPS_PATH, TIANANMEN_LONLAT
    runtime = resolve_stage01_runtime(config_path)
    RAW_PATH = Path(runtime["raw_path"])
    MANUAL_GROUPS_PATH = Path(runtime["manual_groups_path"])
    TIANANMEN_LONLAT = (float(runtime["center_lonlat"][0]), float(runtime["center_lonlat"][1]))
    return runtime


def parse_dir(text):
    if not isinstance(text, str):
        return np.nan
    m_last = PAT_LAST_AFTER_COMMA.match(text)
    phrase = m_last.group(1).strip() if m_last else text.strip()
    m_xy = PAT_X_TO_Y.search(phrase)
    if m_xy:
        return float(DIR_SINGLE_DEG.get(m_xy.group(2), np.nan))
    for key, value in DIR_SINGLE_DEG.items():
        if key in phrase:
            return float(value)
    return np.nan


def geom_bearing_full(geom):
    line = geom.geoms[0] if geom.geom_type == "MultiLineString" else geom
    (x0, y0) = line.coords[0]
    (x1, y1) = line.coords[-1]
    return float((np.degrees(np.arctan2((x1 - x0), (y1 - y0))) + 360) % 360)


def bearing_pt_to_pt(p_from, p_to):
    dx = p_to.x - p_from.x
    dy = p_to.y - p_from.y
    return float((np.degrees(np.arctan2(dx, dy)) + 360) % 360)


def ang_diff_abs(a, b):
    return abs((a - b + 180) % 360 - 180)


def orient_line_outward(ls: LineString, center_pt: Point):
    if ls.is_empty or ls.geom_type != "LineString" or ls.length <= 0:
        return ls
    mid = ls.interpolate(0.5, normalized=True)
    b_out = bearing_pt_to_pt(center_pt, mid)
    coords = list(ls.coords)
    b_ln = bearing_pt_to_pt(Point(coords[0]), Point(coords[-1]))
    return ls if ang_diff_abs(b_ln, b_out) <= 90 else LineString(coords[::-1])


def load_raw_roads():
    raw = gpd.read_file(RAW_PATH)
    if raw.crs is None or raw.crs.to_epsg() != TARGET_EPSG:
        raw = raw.to_crs(TARGET_EPSG)

    raw = raw.reset_index(drop=True)
    raw["raw_edge_id"] = raw.index.astype(int)
    raw["is_valid_geometry"] = raw.geometry.notna() & (~raw.geometry.is_empty)
    raw["is_linestring"] = raw.geometry.geom_type.isin(["LineString", "MultiLineString"])
    raw["dir_deg_sem"] = raw["semantic"].apply(parse_dir).astype("Float64") if "semantic" in raw.columns else np.nan
    raw["bear_geom"] = raw.geometry.apply(geom_bearing_full).astype(float)
    raw["dir_deg"] = raw["dir_deg_sem"].fillna(raw["bear_geom"])
    raw["dir_deg_final"] = raw["dir_deg"]
    raw["dir_source"] = np.where(raw["dir_deg_sem"].notna(), "semantic", "geometry")
    return raw


def load_manual_centerline_groups(path: Path = MANUAL_GROUPS_PATH):
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    groups = []
    for item in payload:
        min_raw = item.get("min_raw_length_m", None)
        snap = item.get("main_component_snap_m", 12.0)
        groups.append(
            {
                "group_id": str(item["group_id"]),
                "group_name": str(item.get("group_name", item["group_id"])).strip(),
                "roadnames": [str(x) for x in item.get("roadnames", []) if str(x).strip()],
                "roadseg_ids": [str(x) for x in item.get("roadseg_ids", []) if str(x).strip()],
                "min_raw_length_m": float(min_raw) if min_raw is not None else None,
                "keep_main_component": bool(item.get("keep_main_component", False)),
                "main_component_snap_m": float(snap) if snap is not None else 12.0,
            }
        )
    return groups


def select_manual_group_raw(raw_gdf: gpd.GeoDataFrame, groups):
    if not groups:
        empty_raw = raw_gdf.iloc[[]].copy()
        empty_report = pd.DataFrame(
            columns=[
                "group_id",
                "group_name",
                "n_selected_raw",
                "n_assigned_unique",
                "n_overlap_dropped",
                "roadnames",
                "roadseg_ids",
            ]
        )
        return empty_raw, raw_gdf.copy(), empty_report

    picked_frames = []
    report_rows = []
    used_raw_ids = set()

    for group in groups:
        mask = pd.Series(False, index=raw_gdf.index)
        if group["roadnames"]:
            mask = mask | raw_gdf["roadname"].astype("string").isin(group["roadnames"]).fillna(False)
        if group["roadseg_ids"]:
            mask = mask | raw_gdf["roadseg_id"].astype("string").isin(group["roadseg_ids"]).fillna(False)
        n0 = int(mask.sum())

        if n0 == 0:
            report_rows.append(
                {
                    "group_id": group["group_id"],
                    "group_name": group["group_name"],
                    "n_selected_raw": 0,
                    "n_assigned_unique": 0,
                    "n_overlap_dropped": 0,
                    "roadnames": "|".join(group["roadnames"]),
                    "roadseg_ids": "|".join(group["roadseg_ids"]),
                }
            )
            continue

        sel = raw_gdf.loc[mask].copy()

        if sel.empty:
            report_rows.append(
                {
                    "group_id": group["group_id"],
                    "group_name": group["group_name"],
                    "n_selected_raw": n0,
                    "n_assigned_unique": 0,
                    "n_overlap_dropped": 0,
                    "roadnames": "|".join(group["roadnames"]),
                    "roadseg_ids": "|".join(group["roadseg_ids"]),
                }
            )
            continue

        assigned = sel.loc[~sel["raw_edge_id"].isin(used_raw_ids)].copy()
        assigned["manual_group_id"] = group["group_id"]
        assigned["manual_group_name"] = group["group_name"]
        if not assigned.empty:
            picked_frames.append(assigned)
            used_raw_ids.update(assigned["raw_edge_id"].astype(int).tolist())
        report_rows.append(
            {
                "group_id": group["group_id"],
                "group_name": group["group_name"],
                "n_selected_raw": n0,
                "n_assigned_unique": int(len(assigned)),
                "n_overlap_dropped": int(len(sel) - len(assigned)),
                "roadnames": "|".join(group["roadnames"]),
                "roadseg_ids": "|".join(group["roadseg_ids"]),
            }
        )

    if not picked_frames:
        empty_raw = raw_gdf.iloc[[]].copy()
        return empty_raw, raw_gdf.copy(), pd.DataFrame(report_rows)

    manual_raw = pd.concat(picked_frames, ignore_index=True)
    manual_raw = manual_raw.sort_values(["manual_group_id", "raw_edge_id"]).drop_duplicates(subset=["raw_edge_id"], keep="first").reset_index(drop=True)
    default_raw = raw_gdf.loc[~raw_gdf["raw_edge_id"].isin(list(used_raw_ids))].copy()
    return manual_raw, default_raw, pd.DataFrame(report_rows)


def extract_lines(geom):
    if geom is None or geom.is_empty:
        return []
    if geom.geom_type == "LineString":
        return [geom]
    if geom.geom_type == "MultiLineString":
        return [g for g in geom.geoms if not g.is_empty and g.length > 0]
    return []


def _uf_find(parent: list, i: int) -> int:
    while parent[i] != i:
        parent[i] = parent[parent[i]]
        i = parent[i]
    return i


def _uf_union(parent: list, i: int, j: int) -> None:
    ri, rj = _uf_find(parent, i), _uf_find(parent, j)
    if ri != rj:
        parent[ri] = rj


def _row_endpoints(geom) -> list[Point]:
    pts = []
    for ln in extract_lines(geom):
        c = list(ln.coords)
        if len(c) >= 2:
            pts.append(Point(c[0]))
            pts.append(Point(c[-1]))
    return pts


def filter_to_largest_line_component(gdf: gpd.GeoDataFrame, snap_m: float) -> gpd.GeoDataFrame:
    """Keep rows in the spatial component with largest total line length.

    Rows link if geometries intersect/touch, or any endpoint pair is within snap_m (meters in projected CRS).
    """
    if len(gdf) <= 1:
        return gdf

    n = len(gdf)
    parent = list(range(n))
    geoms = gdf.geometry.values

    for i in range(n):
        for j in range(i + 1, n):
            gi, gj = geoms[i], geoms[j]
            if gi.intersects(gj) or gi.touches(gj) or gi.distance(gj) <= 1e-6:
                _uf_union(parent, i, j)
                continue
            ei, ej = _row_endpoints(gi), _row_endpoints(gj)
            if not ei or not ej:
                continue
            linked = False
            for a in ei:
                for b in ej:
                    if a.distance(b) <= snap_m:
                        linked = True
                        break
                if linked:
                    break
            if linked:
                _uf_union(parent, i, j)

    comp_len: dict[int, float] = {}
    for i in range(n):
        r = _uf_find(parent, i)
        comp_len[r] = comp_len.get(r, 0.0) + float(geoms[i].length)

    best_root = max(comp_len, key=comp_len.get)
    keep_ix = [i for i in range(n) if _uf_find(parent, i) == best_root]
    return gdf.iloc[keep_ix].copy()


def compute_axis_vector(group_gdf: gpd.GeoDataFrame):
    pts = []
    for geom in group_gdf.geometry.values:
        for line in extract_lines(geom):
            pts.extend(list(line.coords))
    arr = np.asarray(pts, dtype=float)
    if len(arr) < 2:
        return np.array([1.0, 0.0]), arr.mean(axis=0) if len(arr) else np.array([0.0, 0.0])
    center = arr.mean(axis=0)
    centered = arr - center
    cov = np.cov(centered.T)
    vals, vecs = np.linalg.eigh(cov)
    axis = vecs[:, int(np.argmax(vals))]
    axis = axis / np.linalg.norm(axis)
    return axis, center


def raw_direction_sign(geom, axis_vec):
    line = geom.geoms[0] if geom.geom_type == "MultiLineString" else geom
    coords = list(line.coords)
    if len(coords) < 2:
        return 1
    start = np.asarray(coords[0], dtype=float)
    end = np.asarray(coords[-1], dtype=float)
    delta = end - start
    return 1 if float(np.dot(delta, axis_vec)) >= 0 else -1


def orient_line_with_axis(line: LineString, axis_vec):
    coords = list(line.coords)
    if len(coords) < 2:
        return line
    start = np.asarray(coords[0], dtype=float)
    end = np.asarray(coords[-1], dtype=float)
    return line if float(np.dot(end - start, axis_vec)) >= 0 else LineString(coords[::-1])


def merge_directional_lines(geoms, axis_vec):
    lines = []
    for geom in geoms:
        for line in extract_lines(geom):
            if line.length > 0:
                lines.append(orient_line_with_axis(line, axis_vec))
    if not lines:
        return []
    endpoint_pts = []
    for line in lines:
        coords = list(line.coords)
        if len(coords) >= 2:
            endpoint_pts.append(Point(coords[0]))
            endpoint_pts.append(Point(coords[-1]))
    endpoint_union = unary_union(endpoint_pts) if endpoint_pts else None
    snapped_lines = [snap(line, endpoint_union, MANUAL_CONNECT_SNAP_M) for line in lines] if endpoint_union else lines
    unioned = unary_union(snapped_lines)
    merged = linemerge(unioned) if unioned.geom_type != "LineString" else unioned
    out = [orient_line_with_axis(line, axis_vec) for line in extract_lines(merged)]
    return sorted(out, key=lambda g: g.centroid.x * axis_vec[0] + g.centroid.y * axis_vec[1])


def line_axis_overlap(line_a: LineString, line_b: LineString, axis_vec):
    def project_range(line):
        arr = np.asarray(list(line.coords), dtype=float)
        vals = arr[:, 0] * axis_vec[0] + arr[:, 1] * axis_vec[1]
        return float(vals.min()), float(vals.max())

    a0, a1 = project_range(line_a)
    b0, b1 = project_range(line_b)
    return max(0.0, min(a1, b1) - max(a0, b0))


def choose_group_centerline_geometry(line_a: LineString | None, line_b: LineString | None):
    candidates = [line for line in [line_a, line_b] if line is not None and (not line.is_empty) and line.length > 0]
    if not candidates:
        return None
    chosen = max(candidates, key=lambda line: float(line.length))
    return chosen.simplify(MANUAL_SIMPLIFY_TOL, preserve_topology=False) if chosen.length > 0 else chosen


def pair_directional_lines(pos_lines, neg_lines, axis_vec):
    if not pos_lines and not neg_lines:
        return []

    pairs = []
    used_neg = set()
    for pos_line in pos_lines:
        best = None
        for neg_idx, neg_line in enumerate(neg_lines):
            if neg_idx in used_neg:
                continue
            overlap = line_axis_overlap(pos_line, neg_line, axis_vec)
            min_len = min(float(pos_line.length), float(neg_line.length))
            if min_len <= 0:
                continue
            overlap_share = overlap / min_len
            dist = float(pos_line.distance(neg_line))
            if overlap_share < 0.15 and dist > MANUAL_PAIR_MAX_DIST_M:
                continue
            score = (overlap_share, -dist, min_len)
            if best is None or score > best[0]:
                best = (score, neg_idx)
        if best is None:
            pairs.append((pos_line, None))
            continue
        neg_idx = best[1]
        used_neg.add(neg_idx)
        pairs.append((pos_line, neg_lines[neg_idx]))

    for neg_idx, neg_line in enumerate(neg_lines):
        if neg_idx not in used_neg:
            pairs.append((None, neg_line))
    return pairs


def build_manual_centerlines(manual_raw: gpd.GeoDataFrame):
    if manual_raw.empty:
        empty_cl = gpd.GeoDataFrame(
            columns=["manual_group_id", "manual_group_name", "manual_component_id", "pre_cline_key", "build_source", "geometry"],
            geometry="geometry",
            crs=manual_raw.crs,
        )
        empty_xwalk = pd.DataFrame(columns=["raw_edge_id", "manual_group_id", "manual_group_name", "pre_cline_key"])
        return empty_cl, empty_xwalk

    manual_centerline_rows = []
    raw_xwalk_rows = []

    for group_id, grp in manual_raw.groupby("manual_group_id", dropna=False):
        grp = grp.copy().reset_index(drop=True)
        group_name = str(grp["manual_group_name"].dropna().iloc[0]) if "manual_group_name" in grp.columns and grp["manual_group_name"].notna().any() else str(group_id)
        axis_vec, _ = compute_axis_vector(grp)
        grp["dir_sign"] = grp.geometry.apply(lambda g: raw_direction_sign(g, axis_vec))

        pos_lines = merge_directional_lines(grp.loc[grp["dir_sign"] >= 0, "geometry"].tolist(), axis_vec)
        neg_lines = merge_directional_lines(grp.loc[grp["dir_sign"] < 0, "geometry"].tolist(), -axis_vec)

        built_lines = []
        for pos_line, neg_line in pair_directional_lines(pos_lines, neg_lines, axis_vec):
            chosen = choose_group_centerline_geometry(pos_line, neg_line)
            if chosen is not None and (not chosen.is_empty) and chosen.length > 0:
                built_lines.append(chosen)

        for comp_idx, line in enumerate(built_lines):
            pre_key = f"{group_id}__{comp_idx}"
            manual_centerline_rows.append(
                {
                    "manual_group_id": group_id,
                    "manual_group_name": group_name,
                    "manual_component_id": int(comp_idx),
                    "pre_cline_key": pre_key,
                    "build_source": "manual_group",
                    "geometry": line,
                }
            )

        if not built_lines:
            continue

        candidate_df = gpd.GeoDataFrame(manual_centerline_rows[-len(built_lines):], geometry="geometry", crs=manual_raw.crs)
        for row in grp.itertuples(index=False):
            dists = candidate_df.distance(row.geometry)
            best_idx = int(np.argmin(dists.values))
            raw_xwalk_rows.append(
                {
                    "raw_edge_id": int(row.raw_edge_id),
                    "manual_group_id": group_id,
                    "manual_group_name": group_name,
                    "pre_cline_key": str(candidate_df.iloc[best_idx]["pre_cline_key"]),
                }
            )

    manual_centerline = gpd.GeoDataFrame(manual_centerline_rows, geometry="geometry", crs=manual_raw.crs)
    manual_centerline = manual_centerline.loc[manual_centerline.geometry.notna() & (~manual_centerline.geometry.is_empty)].reset_index(drop=True)
    raw_xwalk = pd.DataFrame(raw_xwalk_rows).drop_duplicates(subset=["raw_edge_id"], keep="first")
    return manual_centerline, raw_xwalk


def _skeleton_single_block(raw_gdf):
    """Compute skeleton in one pass. Works when total raster fits in memory."""
    import rasterio
    from rasterio import features
    from skimage.morphology import skeletonize

    edges = raw_gdf.copy()
    edges["geometry"] = edges.geometry.buffer(BUF_WIDTH)
    road_surface = unary_union(edges.geometry)
    polys = [road_surface] if road_surface.geom_type == "Polygon" else list(road_surface.geoms)
    surf = gpd.GeoDataFrame({"rid": np.arange(len(polys))}, geometry=polys, crs=raw_gdf.crs)

    minx, miny, maxx, maxy = surf.total_bounds
    width  = int(np.ceil((maxx - minx) / RES))
    height = int(np.ceil((maxy - miny) / RES))
    transform = rasterio.transform.from_origin(minx, maxy, RES, RES)
    print(f"[stage01] rasterizing: width={width:,} height={height:,} res={RES}")
    burned = features.rasterize(
        shapes=((g, 1) for g in surf.geometry),
        out_shape=(height, width),
        transform=transform,
        fill=0, all_touched=True, dtype=np.uint8,
    )
    print("[stage01] skeletonizing")
    skel_bool = skeletonize(burned.astype(bool))
    yy, xx = np.nonzero(skel_bool)
    pixel_set = set(zip(xx.tolist(), yy.tolist()))
    segments = []
    rh = 0.5
    for x, y in pixel_set:
        for dx, dy in [(1, 0), (0, 1), (1, 1), (-1, 1)]:
            t = (x + dx, y + dy)
            if t in pixel_set:
                segments.append(LineString([
                    (minx + (x + rh) * RES, maxy - (y + rh) * RES),
                    (minx + (t[0] + rh) * RES, maxy - (t[1] + rh) * RES),
                ]))
    return segments, minx, miny, maxx, maxy


def _skeleton_tiled(raw_gdf, n_tiles, overlap_px=60):
    """Compute skeleton in NxN tiles to bound peak memory. Used when single-block would OOM."""
    import rasterio
    from rasterio import features
    from skimage.morphology import skeletonize
    from shapely.geometry import box as shapely_box

    minx_g, miny_g, maxx_g, maxy_g = raw_gdf.total_bounds
    tw = (maxx_g - minx_g) / n_tiles
    th = (maxy_g - miny_g) / n_tiles
    ov = overlap_px * RES
    all_segs = []
    n_done = 0
    for ti in range(n_tiles):
        for tj in range(n_tiles):
            tx0 = max(minx_g + ti * tw - ov, minx_g);  tx1 = min(minx_g + (ti+1)*tw + ov, maxx_g)
            ty0 = max(miny_g + tj * th - ov, miny_g);  ty1 = min(miny_g + (tj+1)*th + ov, maxy_g)
            tile_box = shapely_box(tx0, ty0, tx1, ty1)
            sel = raw_gdf.cx[tx0 - BUF_WIDTH - 10 : tx1 + BUF_WIDTH + 10,
                              ty0 - BUF_WIDTH - 10 : ty1 + BUF_WIDTH + 10]
            if len(sel) == 0:
                n_done += 1; continue
            buf = sel.copy()
            buf["geometry"] = buf.geometry.buffer(BUF_WIDTH).intersection(tile_box)
            buf = buf[~buf.geometry.is_empty]
            if len(buf) == 0:
                n_done += 1; continue
            sg = unary_union(buf.geometry)
            polys = [sg] if sg.geom_type == "Polygon" else list(sg.geoms)
            w = int(np.ceil((tx1 - tx0) / RES));  h = int(np.ceil((ty1 - ty0) / RES))
            burned = features.rasterize(
                shapes=((g, 1) for g in polys),
                out_shape=(h, w),
                transform=rasterio.transform.from_origin(tx0, ty1, RES, RES),
                fill=0, all_touched=True, dtype=np.uint8,
            )
            skel = skeletonize(burned.astype(bool))
            # inner zone: strip overlap border to avoid duplicate segments at seams
            ix0 = tx0 + (ov if ti > 0 else 0);       ix1 = tx1 - (ov if ti < n_tiles-1 else 0)
            iy0 = ty0 + (ov if tj > 0 else 0);       iy1 = ty1 - (ov if tj < n_tiles-1 else 0)
            yy, xx = np.nonzero(skel)
            ps = set(zip(xx.tolist(), yy.tolist()))
            rh = 0.5
            for x, y in ps:
                wx = tx0 + (x + rh) * RES;  wy = ty1 - (y + rh) * RES
                if not (ix0 <= wx <= ix1 and iy0 <= wy <= iy1):
                    continue
                for dx, dy in [(1, 0), (0, 1), (1, 1), (-1, 1)]:
                    t2 = (x + dx, y + dy)
                    if t2 in ps:
                        all_segs.append(LineString([
                            (wx, wy),
                            (tx0 + (t2[0]+rh)*RES, ty1 - (t2[1]+rh)*RES),
                        ]))
            n_done += 1
            if n_done % n_tiles == 0:
                print(f"[stage01]   tile {n_done:02d}/{n_tiles*n_tiles}  segs: {len(all_segs):,}")
    return all_segs, minx_g, miny_g, maxx_g, maxy_g


def build_centerline_from_raw(raw_gdf):
    """
    Build skeleton centerline from raw roads.
    Automatically selects single-block or tiled mode based on estimated raster size:
    - single-block: preferred, no tile seam artifacts, requires enough RAM
    - tiled: fallback when full raster would exceed SKELETON_MAX_RASTER_MPIX
    The RES parameter controls resolution quality; 5m is recommended for production.
    """
    minx_g, miny_g, maxx_g, maxy_g = raw_gdf.total_bounds
    full_mpix = (int(np.ceil((maxx_g - minx_g) / RES)) *
                 int(np.ceil((maxy_g - miny_g) / RES))) / 1e6
    print(f"[stage01] skeleton: RES={RES}m  full-city raster={full_mpix:.0f}M pixels  "
          f"(limit={SKELETON_MAX_RASTER_MPIX}M)")

    if full_mpix <= SKELETON_MAX_RASTER_MPIX:
        print("[stage01] mode=single-block")
        segments, minx, miny, maxx, maxy = _skeleton_single_block(raw_gdf)
    else:
        # Choose n_tiles so each tile ≈ SKELETON_MAX_RASTER_MPIX/2 pixels
        n_tiles = int(np.ceil(np.sqrt(full_mpix / (SKELETON_MAX_RASTER_MPIX / 2))))
        n_tiles = max(n_tiles, 2)
        print(f"[stage01] mode=tiled  n_tiles={n_tiles}x{n_tiles}")
        segments, minx, miny, maxx, maxy = _skeleton_tiled(raw_gdf, n_tiles=n_tiles)

    print(f"[stage01] merging skeleton segments: {len(segments):,} candidate segments")
    merged = linemerge(MultiLineString(segments))
    lines = [merged] if merged.geom_type == "LineString" else list(merged.geoms)
    centerline = (gpd.GeoDataFrame(geometry=lines, crs=raw_gdf.crs)
                  .explode(index_parts=False)
                  .reset_index(drop=True))
    centerline["cline_id"] = centerline.index.astype(int)
    print(f"[stage01] raw centerline fragments before flags: {len(centerline):,}")
    return centerline


def remove_default_centerline_overlapped_by_manual(
    default_centerline: gpd.GeoDataFrame,
    manual_centerline: gpd.GeoDataFrame,
    buffer_m: float,
    overlap_share: float,
):
    if default_centerline.empty or manual_centerline.empty:
        return default_centerline.copy(), pd.DataFrame(columns=["pre_cline_key", "length_m", "overlap_share", "distance_m"])

    manual_union = unary_union(manual_centerline.geometry.values)
    corridor = manual_union.buffer(buffer_m)
    keep_rows = []
    drop_rows = []
    for row in default_centerline.itertuples(index=False):
        line = row.geometry
        if line.is_empty or line.length <= 0:
            keep_rows.append(row._asdict())
            continue
        overlap_len = float(line.intersection(corridor).length)
        share = overlap_len / float(line.length) if line.length > 0 else 0.0
        dist = float(line.distance(manual_union))
        payload = {
            "pre_cline_key": getattr(row, "pre_cline_key", None),
            "length_m": float(line.length),
            "overlap_share": share,
            "distance_m": dist,
        }
        if share >= overlap_share:
            drop_rows.append(payload)
        else:
            keep_rows.append(row._asdict())

    kept = gpd.GeoDataFrame(keep_rows, geometry="geometry", crs=default_centerline.crs)
    dropped = pd.DataFrame(drop_rows)
    return kept, dropped


def finalize_centerline(centerline):
    centerline = centerline.copy()
    centerline = centerline.reset_index(drop=True)
    centerline["is_linestring"] = centerline.geometry.type == "LineString"
    centerline["length_m"] = centerline.geometry.length.astype(float)
    centerline["cline_id"] = np.arange(len(centerline), dtype=int)
    centerline["is_short_centerline"] = centerline["length_m"] < MIN_CL_LEN
    centerline["is_valid_geometry"] = centerline.geometry.notna() & (~centerline.geometry.is_empty)
    centerline["keep_baseline"] = centerline["is_valid_geometry"] & centerline["is_linestring"] & (~centerline["is_short_centerline"])
    centerline["keep_relaxed"] = centerline["is_valid_geometry"] & centerline["is_linestring"]
    centerline["keep_qsm"] = centerline["keep_baseline"]
    if "build_source" not in centerline.columns:
        centerline["build_source"] = "skeleton_default"
    centerline["source_version"] = "stage01_raw_build"
    return centerline


def build_directed_centerline(centerline):
    center_pt = gpd.GeoSeries([Point(TIANANMEN_LONLAT)], crs="EPSG:4326").to_crs(centerline.crs).iloc[0]
    rows = []
    for row in centerline.itertuples(index=False):
        ls = row.geometry
        if ls.is_empty or ls.geom_type != "LineString":
            continue
        ls_out = orient_line_outward(ls, center_pt)
        rows.append(
            {
                "cline_id": row.cline_id,
                "dir": "AB",
                "geometry": ls_out,
                "length_m": row.length_m,
                "source_version": row.source_version,
                "build_source": getattr(row, "build_source", pd.NA),
                "manual_group_id": getattr(row, "manual_group_id", pd.NA),
                "keep_baseline": row.keep_baseline,
                "keep_relaxed": row.keep_relaxed,
                "keep_qsm": row.keep_qsm,
                "is_valid_geometry": row.is_valid_geometry,
                "is_short_centerline": row.is_short_centerline,
            }
        )
        rows.append(
            {
                "cline_id": row.cline_id,
                "dir": "BA",
                "geometry": LineString(list(ls_out.coords)[::-1]),
                "length_m": row.length_m,
                "source_version": row.source_version,
                "build_source": getattr(row, "build_source", pd.NA),
                "manual_group_id": getattr(row, "manual_group_id", pd.NA),
                "keep_baseline": row.keep_baseline,
                "keep_relaxed": row.keep_relaxed,
                "keep_qsm": row.keep_qsm,
                "is_valid_geometry": row.is_valid_geometry,
                "is_short_centerline": row.is_short_centerline,
            }
        )
    cl_dir = gpd.GeoDataFrame(rows, geometry="geometry", crs=centerline.crs).reset_index(drop=True)
    cl_dir["skel_dir"] = np.arange(len(cl_dir), dtype=int)
    cl_dir["bear"] = cl_dir.geometry.apply(geom_bearing_full)
    cl_dir["direction_role"] = np.where(cl_dir["dir"] == "AB", "canonical", "reverse")
    return cl_dir

def save_config_snapshot(version_root: Path, config_path: str | None, stage_options: dict):
    payload = {
        "stage": "stage01_build_centerline",
        "config_path": config_path,
        "raw_path": str(RAW_PATH),
        "center_lonlat": [float(TIANANMEN_LONLAT[0]), float(TIANANMEN_LONLAT[1])],
        "target_epsg": TARGET_EPSG,
        "buf_width": BUF_WIDTH,
        "res": RES,
        "min_cl_len": MIN_CL_LEN,
        "manual_groups_path": str(MANUAL_GROUPS_PATH),
        "manual_midline_step": MANUAL_MIDLINE_STEP,
        "manual_simplify_tol": MANUAL_SIMPLIFY_TOL,
        "manual_connect_snap_m": MANUAL_CONNECT_SNAP_M,
        "manual_pair_max_dist_m": MANUAL_PAIR_MAX_DIST_M,
        "stage01_options": stage_options,
        "execution_mode": "raw_only",
    }
    (version_root / "config_snapshot.stage01.json").write_text(
        json.dumps(payload, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )


def save_metrics(
    raw,
    centerline,
    cl_dir,
    metrics_dir: Path,
    manual_group_report: pd.DataFrame | None = None,
    manual_replace_drop_report: pd.DataFrame | None = None,
):
    metrics_dir.mkdir(parents=True, exist_ok=True)

    dir_source_summary = raw["dir_source"].value_counts().rename_axis("source").reset_index(name="n_segments")
    dir_source_summary["share"] = dir_source_summary["n_segments"] / dir_source_summary["n_segments"].sum()
    dir_source_summary.to_csv(metrics_dir / "stage01_direction_source_summary.csv", index=False)

    summary = pd.DataFrame(
        [
            {
                "raw_segments": int(len(raw)),
                "centerlines": int(len(centerline)),
                "centerlines_keep_baseline": int(centerline["keep_baseline"].sum()),
                "centerlines_keep_relaxed": int(centerline["keep_relaxed"].sum()),
                "directed_centerlines": int(len(cl_dir)),
                "directed_keep_baseline": int(cl_dir["keep_baseline"].sum()),
                "directed_keep_relaxed": int(cl_dir["keep_relaxed"].sum()),
                "total_centerline_length_m": float(centerline["length_m"].sum()),
                "baseline_centerline_length_m": float(centerline.loc[centerline["keep_baseline"], "length_m"].sum()),
                "relaxed_centerline_length_m": float(centerline.loc[centerline["keep_relaxed"], "length_m"].sum()),
            }
        ]
    )
    summary.to_csv(metrics_dir / "stage01_centerline_summary.csv", index=False)
    if manual_group_report is not None and not manual_group_report.empty:
        manual_group_report.to_csv(metrics_dir / "stage01_manual_group_report.csv", index=False)
    if manual_replace_drop_report is not None and not manual_replace_drop_report.empty:
        manual_replace_drop_report.to_csv(metrics_dir / "stage01_manual_replace_dropped_default_centerline.csv", index=False)
def run(config_path: str | None, version_id: str, output_dir: str):
    version_root = Path(output_dir) / version_id
    data_dir = version_root / "data"
    metrics_dir = version_root / "metrics"
    figures_dir = version_root / "figures"

    data_dir.mkdir(parents=True, exist_ok=True)
    metrics_dir.mkdir(parents=True, exist_ok=True)
    figures_dir.mkdir(parents=True, exist_ok=True)

    runtime = configure_stage01_runtime(config_path)
    print(f"[stage01] version_id={version_id}")
    print(f"[stage01] config={config_path}")
    print(f"[stage01] output_root={version_root}")
    print(f"[stage01] data_dir={data_dir}")
    print(f"[stage01] metrics_dir={metrics_dir}")
    print(f"[stage01] figures_dir={figures_dir}")
    print(f"[stage01] raw_path={RAW_PATH}")

    stage_options = load_stage_config(config_path)
    raw = load_raw_roads()
    save_config_snapshot(version_root, config_path, stage_options)

    print(f"[stage01] runtime={runtime}")
    print(f"[stage01] loaded raw roads: {len(raw):,}")
    print(f"[stage01] valid raw line geometries: {int((raw['is_valid_geometry'] & raw['is_linestring']).sum()):,}")
    raw_valid = raw.loc[raw["is_valid_geometry"] & raw["is_linestring"]].copy()
    manual_groups = load_manual_centerline_groups()
    manual_raw, default_raw, manual_group_report = select_manual_group_raw(raw_valid, manual_groups)
    if not manual_group_report.empty:
        print(f"[stage01] manual centerline groups loaded: {len(manual_group_report):,}")
        print(f"[stage01] raw rows diverted to manual groups: {int(len(manual_raw)):,}")

    skeleton_input = default_raw.copy() if stage_options["manual_exclude_from_skeleton_input"] else raw_valid.copy()
    print(f"[stage01] skeleton input rows: {len(skeleton_input):,}")
    default_centerline = build_centerline_from_raw(skeleton_input) if len(skeleton_input) else gpd.GeoDataFrame(geometry=[], crs=raw_valid.crs)
    if len(default_centerline):
        default_centerline["build_source"] = "skeleton_default"
        default_centerline["pre_cline_key"] = [f"skeleton__{i}" for i in range(len(default_centerline))]

    manual_centerline, manual_raw_xwalk = build_manual_centerlines(manual_raw)
    if len(manual_centerline):
        print(f"[stage01] manual centerline fragments built: {len(manual_centerline):,}")

    replace_drop_report = pd.DataFrame()
    if stage_options["manual_replace_default_centerline"] and len(default_centerline) and len(manual_centerline):
        default_centerline, replace_drop_report = remove_default_centerline_overlapped_by_manual(
            default_centerline,
            manual_centerline,
            buffer_m=stage_options["manual_replace_buffer_m"],
            overlap_share=stage_options["manual_replace_overlap_share"],
        )
        print(f"[stage01] default centerline dropped by manual replacement: {len(replace_drop_report):,}")

    if len(default_centerline) and len(manual_centerline):
        centerline = pd.concat([default_centerline, manual_centerline], ignore_index=True)
        centerline = gpd.GeoDataFrame(centerline, geometry="geometry", crs=raw_valid.crs)
    elif len(default_centerline):
        centerline = default_centerline.copy()
    else:
        centerline = manual_centerline.copy()
    centerline = finalize_centerline(centerline)
    cl_dir = build_directed_centerline(centerline)

    if len(manual_raw_xwalk):
        manual_raw_xwalk = manual_raw_xwalk.merge(centerline[["pre_cline_key", "cline_id"]], on="pre_cline_key", how="left")
        manual_raw_xwalk["source_version"] = "stage01_manual_group"

    centerline_path = data_dir / "centerline_master.parquet"
    cl_dir_path = data_dir / "centerline_dir_master.parquet"
    manual_raw_xwalk_path = data_dir / "manual_raw_to_centerline.parquet"
    centerline.to_parquet(centerline_path, index=False)
    cl_dir.to_parquet(cl_dir_path, index=False)
    if len(manual_raw_xwalk):
        manual_raw_xwalk.to_parquet(manual_raw_xwalk_path, index=False)
    save_metrics(raw, centerline, cl_dir, metrics_dir, manual_group_report, replace_drop_report)

    print(f"[stage01] saved centerline: {centerline_path}")
    print(f"[stage01] saved directed centerline: {cl_dir_path}")
    if len(manual_raw_xwalk):
        print(f"[stage01] saved manual raw_to_centerline xwalk: {manual_raw_xwalk_path}")
    print("[stage01] stage01 minimum migration complete")


def main():
    args = parse_args()
    run(args.config, args.version_id, args.output_dir)


if __name__ == "__main__":
    main()
