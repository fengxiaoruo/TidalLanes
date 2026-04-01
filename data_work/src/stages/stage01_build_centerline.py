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
from shapely.ops import linemerge, unary_union


ROOT = Path(__file__).resolve().parents[2]
RAW_PATH = ROOT / "raw_data" / "gis" / "roads_baidu" / "beijing_roads.shp"
TARGET_EPSG = 3857
BUF_WIDTH = 50.0
RES = 5.0
MIN_CL_LEN = 50.0
TIANANMEN_LONLAT = (116.397389, 39.908722)

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


def build_centerline_from_raw(raw_gdf):
    print("[stage01] buffering raw roads for road surface")
    edges_for_buf = raw_gdf.copy()
    edges_for_buf["geometry"] = edges_for_buf.geometry.buffer(BUF_WIDTH)
    road_surface_local = unary_union(edges_for_buf.geometry)

    polys = [road_surface_local] if road_surface_local.geom_type == "Polygon" else list(road_surface_local.geoms)
    surf = gpd.GeoDataFrame({"rid": np.arange(len(polys))}, geometry=polys, crs=raw_gdf.crs)

    import rasterio
    from rasterio import features
    from skimage.morphology import skeletonize

    minx, miny, maxx, maxy = surf.total_bounds
    width = int(np.ceil((maxx - minx) / RES))
    height = int(np.ceil((maxy - miny) / RES))
    transform = rasterio.transform.from_origin(minx, maxy, RES, RES)

    print(f"[stage01] rasterizing surface to grid: width={width:,} height={height:,} res={RES}")
    burned = features.rasterize(
        shapes=((g, 1) for g in surf.geometry),
        out_shape=(height, width),
        transform=transform,
        fill=0,
        all_touched=True,
        dtype=np.uint8,
    )

    print("[stage01] skeletonizing raster surface")
    skel_bool = skeletonize(burned.astype(bool))
    yy, xx = np.nonzero(skel_bool)
    pixel_set = set(zip(xx, yy))

    nbrs4 = [(1, 0), (0, 1), (1, 1), (-1, 1)]
    segments = []
    res_half = 0.5

    for x, y in pixel_set:
        for dx, dy in nbrs4:
            t = (x + dx, y + dy)
            if t in pixel_set:
                x1 = minx + (x + res_half) * RES
                y1 = maxy - (y + res_half) * RES
                x2 = minx + (t[0] + res_half) * RES
                y2 = maxy - (t[1] + res_half) * RES
                segments.append(LineString([(x1, y1), (x2, y2)]))

    print(f"[stage01] merging skeleton segments: {len(segments):,} candidate segments")
    merged = linemerge(MultiLineString(segments))
    lines = [merged] if merged.geom_type == "LineString" else list(merged.geoms)
    centerline = gpd.GeoDataFrame(geometry=lines, crs=raw_gdf.crs).explode(index_parts=False).reset_index(drop=True)
    centerline["cline_id"] = centerline.index.astype(int)
    print(f"[stage01] raw centerline fragments before flags: {len(centerline):,}")
    return centerline


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

def save_config_snapshot(version_root: Path, config_path: str | None):
    payload = {
        "stage": "stage01_build_centerline",
        "config_path": config_path,
        "raw_path": str(RAW_PATH),
        "target_epsg": TARGET_EPSG,
        "buf_width": BUF_WIDTH,
        "res": RES,
        "min_cl_len": MIN_CL_LEN,
        "execution_mode": "raw_only",
    }
    (version_root / "config_snapshot.stage01.json").write_text(
        json.dumps(payload, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )


def save_metrics(raw, centerline, cl_dir, metrics_dir: Path):
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


def run(config_path: str | None, version_id: str, output_dir: str):
    version_root = Path(output_dir) / version_id
    data_dir = version_root / "data"
    metrics_dir = version_root / "metrics"
    figures_dir = version_root / "figures"

    data_dir.mkdir(parents=True, exist_ok=True)
    metrics_dir.mkdir(parents=True, exist_ok=True)
    figures_dir.mkdir(parents=True, exist_ok=True)

    print(f"[stage01] version_id={version_id}")
    print(f"[stage01] config={config_path}")
    print(f"[stage01] output_root={version_root}")
    print(f"[stage01] data_dir={data_dir}")
    print(f"[stage01] metrics_dir={metrics_dir}")
    print(f"[stage01] figures_dir={figures_dir}")
    print(f"[stage01] raw_path={RAW_PATH}")

    raw = load_raw_roads()
    save_config_snapshot(version_root, config_path)

    print(f"[stage01] loaded raw roads: {len(raw):,}")
    print(f"[stage01] valid raw line geometries: {int((raw['is_valid_geometry'] & raw['is_linestring']).sum()):,}")
    centerline = build_centerline_from_raw(raw.loc[raw["is_valid_geometry"] & raw["is_linestring"]].copy())
    centerline = finalize_centerline(centerline)
    cl_dir = build_directed_centerline(centerline)

    centerline_path = data_dir / "centerline_master.parquet"
    cl_dir_path = data_dir / "centerline_dir_master.parquet"
    centerline.to_parquet(centerline_path, index=False)
    cl_dir.to_parquet(cl_dir_path, index=False)
    save_metrics(raw, centerline, cl_dir, metrics_dir)

    print(f"[stage01] saved centerline: {centerline_path}")
    print(f"[stage01] saved directed centerline: {cl_dir_path}")
    print("[stage01] stage01 minimum migration complete")


def main():
    args = parse_args()
    run(args.config, args.version_id, args.output_dir)


if __name__ == "__main__":
    main()
