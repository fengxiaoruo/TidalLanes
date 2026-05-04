import argparse
import sys
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import LineString

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def parse_args():
    parser = argparse.ArgumentParser(description="Export manual centerline review shapefiles.")
    parser.add_argument("--version-id", required=True, help="Version identifier under outputs/.")
    parser.add_argument("--output-dir", default="outputs", help="Base output directory.")
    return parser.parse_args()


def safe_name(value, max_len=80):
    if pd.isna(value):
        return ""
    text = str(value)
    return text[:max_len]


def build_link_geometry(raw_geom, cl_geom):
    if raw_geom is None or cl_geom is None or raw_geom.is_empty or cl_geom.is_empty:
        return None
    p0 = raw_geom.interpolate(0.5, normalized=True)
    p1 = cl_geom.interpolate(0.5, normalized=True)
    return LineString([(p0.x, p0.y), (p1.x, p1.y)])


def shorten_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    rename_map = {
        "manual_group_id": "grp_id",
        "manual_group_name": "grp_name",
        "raw_edge_id": "raw_id",
        "roadseg_id": "roadseg_id",
        "roadname": "roadname",
        "manual_cline_id": "m_cline_id",
        "cline_id": "cline_id",
        "pre_cline_key": "pre_key",
        "match_method_final": "match_mth",
        "matched_final": "matched",
        "build_source": "build_src",
        "manual_component_id": "comp_id",
        "raw_count": "raw_cnt",
        "raw_len_km": "raw_km",
        "raw_to_cl_dist_m": "link_dist_m",
    }
    cols = {}
    used = set()
    for col in gdf.columns:
        if col == "geometry":
            continue
        new = rename_map.get(col, col[:10])
        base = new[:10]
        candidate = base
        idx = 1
        while candidate in used:
            suffix = str(idx)
            candidate = f"{base[:10-len(suffix)]}{suffix}"
            idx += 1
        used.add(candidate)
        cols[col] = candidate
    return gdf.rename(columns=cols)


def drop_extra_geometry_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    active_geom = gdf.geometry.name
    drop_cols = []
    for col in gdf.columns:
        if col == active_geom:
            continue
        if hasattr(gdf[col], "geom_type"):
            drop_cols.append(col)
    if drop_cols:
        gdf = gdf.drop(columns=drop_cols)
    return gdf


def main():
    args = parse_args()
    version_root = Path(args.output_dir) / args.version_id
    data_dir = version_root / "data"
    gis_dir = version_root / "gis_review"
    gis_dir.mkdir(parents=True, exist_ok=True)

    centerline = gpd.read_parquet(data_dir / "centerline_master.parquet")
    raw_segments = gpd.read_parquet(data_dir / "raw_segment_master.parquet")
    match_master = pd.read_parquet(data_dir / "raw_to_centerline_match_master.parquet")
    manual_xwalk = pd.read_parquet(data_dir / "manual_raw_to_centerline.parquet")

    manual_centerline = centerline.loc[centerline["build_source"].astype("string") == "manual_group"].copy()
    manual_centerline["manual_group_id"] = manual_centerline["manual_group_id"].astype("string")
    manual_centerline["manual_group_name"] = manual_centerline.get("manual_group_name", pd.Series("", index=manual_centerline.index)).map(safe_name)

    manual_raw = raw_segments.loc[raw_segments["manual_exact_match"].fillna(False)].copy()
    manual_raw = manual_raw.merge(
        match_master[["split_id", "match_method_final", "matched_final", "cline_id_final"]],
        on="split_id",
        how="left",
    )
    if "manual_group_name" not in manual_raw.columns:
        manual_raw = manual_raw.merge(
            manual_xwalk[["raw_edge_id", "manual_group_name"]].drop_duplicates(subset=["raw_edge_id"]),
            on="raw_edge_id",
            how="left",
        )
    manual_raw["manual_group_id"] = manual_raw["manual_group_id"].astype("string")
    manual_raw["manual_group_name"] = manual_raw["manual_group_name"].astype("string").map(safe_name)

    cl_counts = (
        manual_xwalk.groupby("cline_id")
        .agg(raw_count=("raw_edge_id", "nunique"))
        .reset_index()
    )
    raw_len = (
        manual_raw.groupby("manual_cline_id")
        .agg(raw_len_km=("length_m", lambda s: float(pd.to_numeric(s, errors="coerce").sum()) / 1000.0))
        .reset_index()
        .rename(columns={"manual_cline_id": "cline_id"})
    )
    manual_centerline = manual_centerline.merge(cl_counts, on="cline_id", how="left")
    manual_centerline = manual_centerline.merge(raw_len, on="cline_id", how="left")
    manual_centerline["raw_count"] = manual_centerline["raw_count"].fillna(0).astype(int)

    cl_geom = manual_centerline[["cline_id", "geometry"]].rename(columns={"geometry": "cline_geometry"})
    manual_links = manual_raw.merge(cl_geom, left_on="manual_cline_id", right_on="cline_id", how="left")
    manual_links["raw_to_cl_dist_m"] = manual_links.apply(
        lambda r: float(r.geometry.distance(r.cline_geometry)) if r.cline_geometry is not None else np.nan,
        axis=1,
    )
    manual_links["geometry"] = manual_links.apply(
        lambda r: build_link_geometry(r.geometry, r.cline_geometry),
        axis=1,
    )
    manual_links = gpd.GeoDataFrame(manual_links.drop(columns=["cline_geometry"]), geometry="geometry", crs=manual_raw.crs)
    manual_links = manual_links.loc[manual_links.geometry.notna() & (~manual_links.geometry.is_empty)].copy()

    for name, gdf in [
        ("manual_raw_selected", manual_raw),
        ("manual_centerline", manual_centerline),
        ("manual_raw_to_cline_links", manual_links),
    ]:
        out = drop_extra_geometry_columns(gdf.copy())
        out = shorten_columns(out)
        out.to_file(gis_dir / f"{name}.shp")
        print(f"[manual-gis] saved {gis_dir / f'{name}.shp'}")


if __name__ == "__main__":
    main()
