"""
Export long raw-road pairs that can plausibly bypass raster centerline extraction.

The target pattern is intentionally conservative:
- long raw LineStrings
- exactly overlapping geometry after orientation normalization
- exactly two records share the same normalized geometry
- the two records point in opposite directions

Outputs are saved under:
outputs/{version_id}/gis_exports/direct_raw_centerline_candidates/
"""

import argparse
import sys
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import LineString, MultiLineString
from shapely.ops import linemerge


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.stages.stage01_build_centerline import geom_bearing_full, load_raw_roads


def parse_args():
    parser = argparse.ArgumentParser(description="Export direct-raw-centerline candidates")
    parser.add_argument("--version-id", required=True, help="Version identifier for outputs.")
    parser.add_argument(
        "--output-dir",
        default="outputs",
        help="Base output directory for versioned results.",
    )
    parser.add_argument(
        "--min-length",
        type=float,
        default=1000.0,
        help="Minimum raw segment length in meters.",
    )
    parser.add_argument(
        "--opposite-tol",
        type=float,
        default=20.0,
        help="Tolerance from 180 degrees when testing opposite directions.",
    )
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


def build_candidate_rows(raw: gpd.GeoDataFrame, opposite_tol: float) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    rows = []
    rep_rows = []
    pair_id = 0

    grouped = raw.groupby("geom_key", dropna=True)
    for geom_key, grp in grouped:
        if len(grp) != 2:
            continue

        a = grp.iloc[0]
        b = grp.iloc[1]
        dir_a = float(a["dir_deg_final"])
        dir_b = float(b["dir_deg_final"])
        geom_bear_a = float(a["geom_bear"])
        geom_bear_b = float(b["geom_bear"])
        dir_gap = ang_diff_abs(dir_a, dir_b)
        geom_gap = ang_diff_abs(geom_bear_a, geom_bear_b)
        opposite_by_dir = abs(dir_gap - 180.0) <= opposite_tol
        opposite_by_geom = abs(geom_gap - 180.0) <= opposite_tol

        if not (opposite_by_dir or opposite_by_geom):
            continue

        pair_id += 1
        pair_len = float(max(a["length_m"], b["length_m"]))
        pair_name = a["roadname"] if pd.notna(a["roadname"]) and str(a["roadname"]).strip() else b["roadname"]

        for side, row in [("A", a), ("B", b)]:
            rows.append(
                {
                    "pair_id": pair_id,
                    "pair_side": side,
                    "raw_edge_id": int(row["raw_edge_id"]),
                    "roadname": row.get("roadname"),
                    "roadtype": row.get("roadtype"),
                    "semantic": row.get("semantic"),
                    "length_m": float(row["length_m"]),
                    "dir_deg": float(row["dir_deg_final"]),
                    "geom_bear": float(row["geom_bear"]),
                    "dir_gap": float(dir_gap),
                    "geom_gap": float(geom_gap),
                    "opp_dir": int(opposite_by_dir),
                    "opp_geom": int(opposite_by_geom),
                    "geometry": row["geometry"],
                }
            )

        rep_geom = a["geometry"]
        rep_rows.append(
            {
                "pair_id": pair_id,
                "raw_a": int(a["raw_edge_id"]),
                "raw_b": int(b["raw_edge_id"]),
                "roadname": pair_name,
                "roadtype": a.get("roadtype"),
                "length_m": pair_len,
                "dir_gap": float(dir_gap),
                "geom_gap": float(geom_gap),
                "opp_dir": int(opposite_by_dir),
                "opp_geom": int(opposite_by_geom),
                "geometry": rep_geom,
            }
        )

    pair_gdf = gpd.GeoDataFrame(rows, geometry="geometry", crs=raw.crs)
    rep_gdf = gpd.GeoDataFrame(rep_rows, geometry="geometry", crs=raw.crs)
    return pair_gdf, rep_gdf


def main():
    args = parse_args()
    version_root = Path(args.output_dir) / args.version_id
    out_dir = version_root / "gis_exports" / "direct_raw_centerline_candidates"
    out_dir.mkdir(parents=True, exist_ok=True)

    raw = load_raw_roads().copy()
    raw["geometry"] = raw.geometry.apply(to_single_linestring)
    raw = raw.loc[raw["geometry"].notna()].copy()
    raw["length_m"] = raw.geometry.length.astype(float)
    raw["geom_bear"] = raw.geometry.apply(geom_bearing_full).astype(float)
    raw = raw.loc[raw["length_m"] >= float(args.min_length)].copy()
    raw["geom_key"] = raw.geometry.apply(canonical_geom_key)
    raw = raw.loc[raw["geom_key"].notna()].copy()

    pair_gdf, rep_gdf = build_candidate_rows(raw, opposite_tol=float(args.opposite_tol))

    pair_out = pair_gdf.rename(
        columns={
            "raw_edge_id": "raw_id",
            "roadname": "road_nm",
            "roadtype": "rtype",
            "semantic": "semantic",
            "length_m": "len_m",
            "geom_bear": "gbear",
            "pair_side": "side",
            "opp_dir": "opp_dir",
            "opp_geom": "opp_geom",
        }
    )
    rep_out = rep_gdf.rename(
        columns={
            "roadname": "road_nm",
            "roadtype": "rtype",
            "length_m": "len_m",
            "raw_a": "raw_a",
            "raw_b": "raw_b",
            "geom_gap": "geom_gap",
        }
    )

    pair_path = out_dir / "raw_exact_overlap_pairs_review.shp"
    rep_path = out_dir / "centerline_seed_candidates_review.shp"
    csv_path = out_dir / "direct_raw_centerline_candidates_summary.csv"

    pair_out.to_crs(4326).to_file(pair_path, driver="ESRI Shapefile", encoding="UTF-8")
    rep_out.to_crs(4326).to_file(rep_path, driver="ESRI Shapefile", encoding="UTF-8")
    rep_out.drop(columns="geometry").to_csv(csv_path, index=False, encoding="utf-8-sig")

    print(f"[direct_raw_centerline_candidates] pair features: {len(pair_out):,}")
    print(f"[direct_raw_centerline_candidates] candidate pairs: {len(rep_out):,}")
    print(f"[direct_raw_centerline_candidates] saved: {pair_path}")
    print(f"[direct_raw_centerline_candidates] saved: {rep_path}")
    print(f"[direct_raw_centerline_candidates] saved: {csv_path}")


if __name__ == "__main__":
    main()
