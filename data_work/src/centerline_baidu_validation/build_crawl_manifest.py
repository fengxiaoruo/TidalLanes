"""
Build crawl manifest for baseline centerline Baidu direction validation.

Each baseline-kept directed centerline (AB or BA) yields one crawl row. Both
directions of an undirected corridor are covered by separate AB/BA records, not
by forward/reverse duplicates on the same record. Intermediate points along the
centerline geometry are written as ordered waypoints so the API route follows the
corridor rather than a free shortest path between endpoints.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import LineString

ROOT = Path(__file__).resolve().parents[2]

import sys

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.national_tidal_commuting.common import format_latlon, load_config, resolve_path


def parse_args():
    parser = argparse.ArgumentParser(description="Build Baidu crawl manifest for baseline centerlines.")
    parser.add_argument("--config", required=True, help="JSON config path.")
    parser.add_argument("--version-id", default=None, help="Optional validation version override.")
    parser.add_argument("--output-dir", default=None, help="Optional output dir override.")
    parser.add_argument("--limit", type=int, default=None, help="Optional smoke-test limit on centerline count.")
    return parser.parse_args()


def normalize_bearing_deg(value) -> float | None:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    deg = float(value) % 360.0
    return deg


def line_to_latlon_points(line: LineString) -> list[tuple[float, float]]:
    return [(float(y), float(x)) for x, y in line.coords]


def choose_internal_fractions(length_m: float, *, max_waypoints: int, min_spacing_m: float) -> list[float]:
    if max_waypoints <= 0 or length_m <= min_spacing_m:
        return []
    n_by_length = int(length_m // min_spacing_m)
    n_internal = min(max_waypoints, max(n_by_length - 1, 0))
    if n_internal <= 0:
        return []
    return [float(x) for x in np.linspace(0.0, 1.0, n_internal + 2)[1:-1]]


def build_route_points(
    line_wgs84: LineString,
    *,
    length_m: float,
    max_waypoints: int,
    min_waypoint_spacing_m: float,
) -> tuple[tuple[float, float], tuple[float, float], list[tuple[float, float]]]:
    if line_wgs84.is_empty or length_m <= 0:
        raise ValueError("Empty centerline geometry.")
    latlon = line_to_latlon_points(line_wgs84)
    origin = latlon[0]
    destination = latlon[-1]
    fractions = choose_internal_fractions(
        float(length_m),
        max_waypoints=max_waypoints,
        min_spacing_m=min_waypoint_spacing_m,
    )
    waypoints = []
    for frac in fractions:
        pt = line_wgs84.interpolate(frac, normalized=True)
        waypoints.append((float(pt.y), float(pt.x)))
    return origin, destination, waypoints


def load_coverage_flags(baseline_root: Path) -> pd.DataFrame:
    keys = ["skel_dir", "cline_id", "dir"]
    split = pd.read_parquet(baseline_root / "data" / "raw_split_centerline.parquet")
    split["matched"] = pd.to_numeric(split["matched"], errors="coerce")
    matched_keys = split.loc[split["matched"] == 1, keys].drop_duplicates()
    matched_keys["has_raw_match"] = True

    spd = pd.read_parquet(baseline_root / "data" / "centerline_speed_master.parquet")
    spd["cl_speed_kmh"] = pd.to_numeric(spd["cl_speed_kmh"], errors="coerce")
    inferred_keys = spd.loc[spd["has_valid_speed"] & spd["cl_speed_kmh"].gt(0), keys].drop_duplicates()
    inferred_keys["has_inferred_speed"] = True

    out = matched_keys.merge(inferred_keys, on=keys, how="outer")
    out["has_raw_match"] = out["has_raw_match"].fillna(False)
    out["has_inferred_speed"] = out["has_inferred_speed"].fillna(False)
    return out


def gps_direction_for_route(bearing_deg: float | None) -> int | None:
    if bearing_deg is None:
        return None
    return int(round(float(bearing_deg) % 360.0)) % 360


def load_baseline_centerlines(baseline_root: Path) -> gpd.GeoDataFrame:
    path = baseline_root / "data" / "centerline_dir_master.parquet"
    cl = gpd.read_parquet(path)
    if cl.crs is None:
        raise ValueError(f"{path} has no CRS.")
    cl = cl[cl["keep_baseline"].fillna(False)].copy()
    cl = cl[cl.geometry.notna() & (~cl.geometry.is_empty)].copy()
    if cl.empty:
        raise ValueError("No baseline-kept directed centerlines found.")
    return cl.to_crs("EPSG:4326")


def build_manifest_rows(cl: gpd.GeoDataFrame, coverage: pd.DataFrame, config: dict) -> pd.DataFrame:
    crawl_cfg = config["crawl"]
    max_waypoints = int(crawl_cfg.get("max_waypoints", 10))
    min_spacing_m = float(crawl_cfg.get("min_waypoint_spacing_m", 120.0))
    rows: list[dict] = []

    for row in cl.itertuples(index=False):
        geom = row.geometry
        geoms = [geom] if isinstance(geom, LineString) else [g for g in getattr(geom, "geoms", []) if isinstance(g, LineString)]
        if not geoms:
            continue
        line = max(geoms, key=lambda g: g.length)
        length_m = float(getattr(row, "length_m", line.length))
        origin, destination, waypoints = build_route_points(
            line,
            length_m=length_m,
            max_waypoints=max_waypoints,
            min_waypoint_spacing_m=min_spacing_m,
        )
        bear = normalize_bearing_deg(getattr(row, "bear", None))
        rows.append(
            {
                "skel_dir": int(row.skel_dir),
                "cline_id": int(row.cline_id),
                "dir": str(row.dir),
                "length_m": length_m,
                "n_waypoints": len(waypoints),
                "waypoints_latlon": "|".join(format_latlon(lat, lon) for lat, lon in waypoints),
                "origin_lat": origin[0],
                "origin_lon": origin[1],
                "destination_lat": destination[0],
                "destination_lon": destination[1],
                "origin_text": format_latlon(origin[0], origin[1]),
                "destination_text": format_latlon(destination[0], destination[1]),
                "gps_direction": gps_direction_for_route(bear),
            }
        )

    out = pd.DataFrame(rows)
    out = out.merge(coverage, on=["skel_dir", "cline_id", "dir"], how="left")
    out["has_raw_match"] = out["has_raw_match"].fillna(False)
    out["has_inferred_speed"] = out["has_inferred_speed"].fillna(False)
    out["request_id"] = (
        out["skel_dir"].astype(str) + "_" + out["cline_id"].astype(str) + "_" + out["dir"].astype(str)
    )
    return out


def save_manifest(version_root: Path, manifest: pd.DataFrame, config: dict) -> None:
    data_dir = version_root / "data"
    metrics_dir = version_root / "metrics"
    data_dir.mkdir(parents=True, exist_ok=True)
    metrics_dir.mkdir(parents=True, exist_ok=True)

    manifest_path = data_dir / "crawl_manifest.parquet"
    manifest.to_parquet(manifest_path, index=False)
    manifest.to_csv(data_dir / "crawl_manifest.csv", index=False)

    summary = pd.DataFrame(
        [
            {
                "n_manifest_rows": int(len(manifest)),
                "n_unique_centerlines": int(manifest[["skel_dir", "cline_id", "dir"]].drop_duplicates().shape[0]),
                "n_with_raw_match": int(manifest["has_raw_match"].astype(bool).sum()),
                "n_with_inferred_speed": int(manifest["has_inferred_speed"].astype(bool).sum()),
                "n_crawl_only_no_inferred_speed": int((~manifest["has_inferred_speed"].astype(bool)).sum()),
                "median_length_m": float(pd.to_numeric(manifest["length_m"], errors="coerce").median()),
                "median_n_waypoints": float(pd.to_numeric(manifest["n_waypoints"], errors="coerce").median()),
                "share_with_waypoints": float((manifest["n_waypoints"] > 0).mean()),
            }
        ]
    )
    summary.to_csv(metrics_dir / "crawl_manifest_summary.csv", index=False)

    snapshot = {
        "stage": "build_crawl_manifest",
        "config_path": config.get("_config_path"),
        "baseline_network_version_id": config["baseline_network_version_id"],
        "crawl_api": config["crawl"]["api"],
        "manifest_path": str(manifest_path),
        "summary_path": str(metrics_dir / "crawl_manifest_summary.csv"),
    }
    (version_root / "config_snapshot.build_crawl_manifest.json").write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def run(config_path: str, *, version_id: str | None = None, output_dir: str | None = None, limit: int | None = None):
    config = load_config(config_path)
    actual_version = version_id or config["version_id"]
    baseline_version = config["baseline_network_version_id"]
    base_out = resolve_path(output_dir) if output_dir else resolve_path(config.get("output_dir", "outputs"))
    version_root = base_out / actual_version
    baseline_root = base_out / baseline_version
    if not (baseline_root / "data" / "centerline_dir_master.parquet").exists():
        raise FileNotFoundError(
            f"Missing baseline network at {baseline_root / 'data' / 'centerline_dir_master.parquet'}. "
            f"Check baseline_network_version_id in config."
        )

    cl = load_baseline_centerlines(baseline_root)
    if limit is not None:
        cl = cl.head(int(limit)).copy()

    coverage = load_coverage_flags(baseline_root)
    manifest = build_manifest_rows(cl, coverage, config)
    save_manifest(version_root, manifest, config)
    print(f"[build_crawl_manifest] saved {len(manifest):,} rows to {version_root / 'data' / 'crawl_manifest.parquet'}")


def main():
    args = parse_args()
    run(args.config, version_id=args.version_id, output_dir=args.output_dir, limit=args.limit)


if __name__ == "__main__":
    main()
