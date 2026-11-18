"""
Export Baidu crawl route geometries alongside baseline centerlines for manual QA.

Reads `responses.jsonl` from a crawl session (requires `steps_info=1` responses),
builds route LineStrings, joins centerline geometry, and writes GeoParquet + GeoJSON.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.centerline_baidu_validation.baidu_client import (
    extract_route,
    extract_route_path,
    merge_path_coords,
    parse_baidu_path_text,
)
from src.centerline_baidu_validation.build_crawl_manifest import load_baseline_centerlines
from src.national_tidal_commuting.common import ensure_version_dirs, get_version_root, load_config, resolve_path

QC_LO, QC_HI = 0.6, 1.8


def parse_args():
    p = argparse.ArgumentParser(description="Export API route geometries for manual QA.")
    p.add_argument("--config", required=True)
    p.add_argument("--session-dir", required=True, help="crawl_sessions/<run_id>/ under version root or absolute path")
    p.add_argument("--version-id", default=None)
    p.add_argument("--output-dir", default=None)
    p.add_argument("--limit", type=int, default=None)
    return p.parse_args()


def linestring_from_lonlat(coords: list[tuple[float, float]]) -> LineString | None:
    if len(coords) < 2:
        return None
    return LineString(coords)


def load_session_rows(session_dir: Path, *, limit: int | None) -> pd.DataFrame:
    jsonl_path = session_dir / "responses.jsonl"
    if not jsonl_path.exists():
        raise FileNotFoundError(f"Missing {jsonl_path}")
    rows = []
    with jsonl_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            rec = json.loads(line)
            req = rec.get("request") or {}
            resp = rec.get("response") or {}
            route = extract_route(resp)
            path = extract_route_path(resp)
            result = resp.get("result", {}) if isinstance(resp.get("result"), dict) else {}
            routes = result.get("routes", []) if isinstance(result.get("routes", []), list) else []
            route0 = routes[0] if routes else {}
            step_paths = []
            if isinstance(route0, dict) and isinstance(route0.get("steps"), list):
                for step in route0["steps"]:
                    if isinstance(step, dict):
                        step_paths.append(parse_baidu_path_text(str(step.get("path") or "")))
            coords = merge_path_coords(step_paths)
            rows.append(
                {
                    "request_id": rec.get("request_id"),
                    "panel": rec.get("panel"),
                    "tactics": rec.get("tactics"),
                    "query_time_local": rec.get("query_time_local"),
                    "coord_type": req.get("coord_type"),
                    "ret_coordtype": req.get("ret_coordtype"),
                    "steps_info": req.get("steps_info"),
                    "api_status": route["api_status"],
                    "duration_sec": route["duration_sec"],
                    "distance_m": route["distance_m"],
                    "n_path_points": path["n_path_points"],
                    "has_route_path": path["has_route_path"],
                    "route_coords_lonlat": coords,
                }
            )
            if limit is not None and len(rows) >= int(limit):
                break
    return pd.DataFrame(rows)


def main():
    args = parse_args()
    config = load_config(args.config)
    version_root = get_version_root(config, args.version_id, args.output_dir)
    dirs = ensure_version_dirs(version_root)

    session_dir = Path(args.session_dir)
    if not session_dir.is_absolute():
        cwd_path = Path.cwd() / session_dir
        session_dir = cwd_path if cwd_path.exists() else version_root / session_dir
    if not session_dir.exists():
        raise FileNotFoundError(session_dir)

    df = load_session_rows(session_dir, limit=args.limit)
    manifest = pd.read_parquet(version_root / "data" / "crawl_manifest.parquet")
    manifest = manifest.drop_duplicates(subset=["request_id"])
    meta = manifest.merge(df, on="request_id", how="right", suffixes=("", "_resp"))

    baseline_root = resolve_path(config.get("output_dir", "outputs")) / config["baseline_network_version_id"]
    cl = load_baseline_centerlines(baseline_root)
    cl = cl[["skel_dir", "cline_id", "dir", "geometry"]].rename(columns={"geometry": "centerline_geometry"})

    meta = meta.merge(cl, on=["skel_dir", "cline_id", "dir"], how="left")
    meta["length_m"] = pd.to_numeric(meta["length_m"], errors="coerce")
    meta["distance_m"] = pd.to_numeric(meta["distance_m"], errors="coerce")
    meta["distance_over_length"] = meta["distance_m"] / meta["length_m"]
    meta["route_bind_ok"] = meta["distance_over_length"].between(QC_LO, QC_HI)

    route_geoms = [linestring_from_lonlat(list(c) if c else []) for c in meta["route_coords_lonlat"]]
    meta["route_geometry"] = route_geoms
    meta = meta.drop(columns=["route_coords_lonlat"])

    route_crs_note = str(meta["ret_coordtype"].dropna().iloc[0]) if meta["ret_coordtype"].notna().any() else str(
        meta["coord_type"].dropna().iloc[0] if meta["coord_type"].notna().any() else "EPSG:4326"
    )
    if route_crs_note in {"wgs84", "gcj02", "bd09ll"}:
        route_crs = "EPSG:4326"  # geographic; note datum in sidecar
    else:
        route_crs = "EPSG:4326"

    out_stem = session_dir.name
    out_dir = dirs["data"]
    out_dir.mkdir(parents=True, exist_ok=True)

    centerline_gdf = gpd.GeoDataFrame(
        meta,
        geometry="centerline_geometry",
        crs="EPSG:4326",
    )
    route_gdf = gpd.GeoDataFrame(
        meta,
        geometry="route_geometry",
        crs=route_crs,
    )

    centerline_path = out_dir / f"route_qa_centerlines_{out_stem}.parquet"
    route_path = out_dir / f"route_qa_api_routes_{out_stem}.parquet"
    paired_path = out_dir / f"route_qa_paired_{out_stem}.parquet"

    keep_cols = [
        "request_id",
        "skel_dir",
        "cline_id",
        "dir",
        "panel",
        "length_m",
        "distance_m",
        "distance_over_length",
        "route_bind_ok",
        "duration_sec",
        "n_waypoints",
        "n_path_points",
        "has_route_path",
        "has_inferred_speed",
        "api_status",
        "query_time_local",
    ]
    centerline_gdf[keep_cols + ["centerline_geometry"]].to_parquet(centerline_path, index=False)
    route_gdf[keep_cols + ["route_geometry"]].to_parquet(route_path, index=False)

    paired = meta[keep_cols].copy()
    paired["centerline_wkt"] = centerline_gdf["centerline_geometry"].to_wkt()
    paired["route_wkt"] = route_gdf["route_geometry"].to_wkt()
    paired.to_parquet(paired_path, index=False)

    geojson_path = out_dir / f"route_qa_paired_{out_stem}.geojson"
    centerline_geojson = out_dir / f"route_qa_centerlines_{out_stem}.geojson"
    overlay_path = out_dir / f"route_qa_overlay_{out_stem}.geojson"
    mask = meta["has_route_path"].fillna(False)
    export_gdf = gpd.GeoDataFrame(
        meta.loc[mask, keep_cols].copy(),
        geometry=gpd.GeoSeries(meta.loc[mask, "route_geometry"], crs=route_crs),
        crs=route_crs,
    )
    centerline_export = gpd.GeoDataFrame(
        meta.loc[mask, keep_cols].copy(),
        geometry=gpd.GeoSeries(meta.loc[mask, "centerline_geometry"], crs="EPSG:4326"),
        crs="EPSG:4326",
    )
    if len(export_gdf) <= 5000:
        export_gdf.to_file(geojson_path, driver="GeoJSON")
        centerline_export.to_file(centerline_geojson, driver="GeoJSON")
        overlay_rows = []
        for _, row in meta.loc[mask].iterrows():
            rid = row["request_id"]
            cl = row["centerline_geometry"]
            rt = row["route_geometry"]
            if cl is not None and not cl.is_empty:
                overlay_rows.append(
                    {"request_id": rid, "role": "centerline", "geometry": cl, **{c: row[c] for c in keep_cols if c != "request_id"}}
                )
            if rt is not None and not rt.is_empty:
                overlay_rows.append(
                    {"request_id": rid, "role": "api_route", "geometry": rt, **{c: row[c] for c in keep_cols if c != "request_id"}}
                )
        if overlay_rows:
            gpd.GeoDataFrame(overlay_rows, crs="EPSG:4326").to_file(overlay_path, driver="GeoJSON")
    else:
        geojson_path = None
        centerline_geojson = None
        overlay_path = None

    sidecar = {
        "session_dir": str(session_dir),
        "route_geometry_crs": route_crs,
        "route_geometry_datum_note": (
            "API path coords follow request coord_type/ret_coordtype. "
            "Centerlines are EPSG:4326 WGS84 from baseline network."
        ),
        "n_rows": int(len(meta)),
        "n_with_route_path": int(meta["has_route_path"].fillna(False).sum()),
        "centerline_parquet": str(centerline_path),
        "route_parquet": str(route_path),
        "paired_parquet": str(paired_path),
        "geojson": str(geojson_path) if geojson_path else None,
        "centerline_geojson": str(centerline_geojson) if centerline_geojson else None,
        "overlay_geojson": str(overlay_path) if overlay_path else None,
    }
    (out_dir / f"route_qa_{out_stem}.json").write_text(json.dumps(sidecar, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[export_route_geometries] rows={len(meta)} with_route_path={sidecar['n_with_route_path']}")
    print(f"[export_route_geometries] wrote {route_path}")
    print(f"[export_route_geometries] wrote {paired_path}")
    if geojson_path:
        print(f"[export_route_geometries] wrote {geojson_path}")
    if centerline_geojson:
        print(f"[export_route_geometries] wrote {centerline_geojson}")
    if overlay_path:
        print(f"[export_route_geometries] wrote {overlay_path}")


if __name__ == "__main__":
    main()
