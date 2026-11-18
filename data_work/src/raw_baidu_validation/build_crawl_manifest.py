"""
Build crawl manifest for baseline raw road segments (Baidu direction validation).

Each keep_baseline raw segment yields one crawl row with ordered waypoints along the
segment geometry so the API route follows the underlying road rather than a shortcut.
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

from src.centerline_baidu_validation.build_crawl_manifest import (
    build_route_points,
    gps_direction_for_route,
    normalize_bearing_deg,
)
from src.national_tidal_commuting.common import format_latlon, load_config, resolve_path


def parse_args():
    parser = argparse.ArgumentParser(description="Build Baidu crawl manifest for baseline raw segments.")
    parser.add_argument("--config", required=True, help="JSON config path.")
    parser.add_argument("--version-id", default=None, help="Optional validation version override.")
    parser.add_argument("--output-dir", default=None, help="Optional output dir override.")
    parser.add_argument("--limit", type=int, default=None, help="Optional smoke-test limit on segment count.")
    return parser.parse_args()


def resolve_raw_speed_path(config: dict, baseline_root: Path) -> Path:
    comparison = config.get("comparison", {})
    if comparison.get("raw_speed_path"):
        return resolve_path(comparison["raw_speed_path"])
    snap = baseline_root / "config_snapshot.stage03.json"
    if snap.exists():
        payload = json.loads(snap.read_text(encoding="utf-8"))
        if payload.get("raw_speed_path"):
            return resolve_path(payload["raw_speed_path"])
    return ROOT / "raw_data" / "speed_Beijing_all_wgs84.csv"


def load_direct_speed_flags(speed_path: Path, roadseg_ids: pd.Series) -> pd.DataFrame:
    if not speed_path.exists():
        raise FileNotFoundError(f"Missing raw speed file: {speed_path}")

    usecols = ["roadseg_id", "speed"]
    df = pd.read_csv(speed_path, usecols=usecols)
    df["roadseg_id"] = df["roadseg_id"].astype("string")
    df["speed"] = pd.to_numeric(df["speed"], errors="coerce")
    df = df[df["roadseg_id"].notna() & df["speed"].gt(0)].copy()

    obs = df.groupby("roadseg_id", as_index=False).agg(n_speed_obs=("speed", "size"))
    obs["has_direct_speed"] = True

    target = pd.DataFrame({"roadseg_id": roadseg_ids.astype("string").drop_duplicates()})
    out = target.merge(obs, on="roadseg_id", how="left")
    out["has_direct_speed"] = out["has_direct_speed"].fillna(False)
    out["n_speed_obs"] = pd.to_numeric(out["n_speed_obs"], errors="coerce").fillna(0).astype(int)
    return out


def load_centerline_crosswalk(baseline_root: Path) -> pd.DataFrame:
    match = pd.read_parquet(baseline_root / "data" / "raw_to_centerline_match_master.parquet")
    cols = ["split_id", "raw_edge_id", "matched_final", "skel_dir_final", "cline_id_final", "dir_final"]
    out = match[cols].copy()
    out = out.rename(
        columns={
            "matched_final": "matched_centerline",
            "skel_dir_final": "skel_dir",
            "cline_id_final": "cline_id",
            "dir_final": "dir",
        }
    )
    out["matched_centerline"] = pd.to_numeric(out["matched_centerline"], errors="coerce").fillna(0).astype(int).eq(1)
    return out


def load_baseline_raw_segments(baseline_root: Path, baseline_flag: str) -> gpd.GeoDataFrame:
    path = baseline_root / "data" / "raw_segment_master.parquet"
    raw = gpd.read_parquet(path)
    if raw.crs is None:
        raise ValueError(f"{path} has no CRS.")
    if baseline_flag not in raw.columns:
        raise ValueError(f"Missing baseline flag column: {baseline_flag}")
    raw = raw[raw[baseline_flag].fillna(False)].copy()
    raw = raw[raw.geometry.notna() & (~raw.geometry.is_empty)].copy()
    if raw.empty:
        raise ValueError(f"No baseline raw segments found for {baseline_flag=}.")
    return raw.to_crs("EPSG:4326")


def build_manifest_rows(raw: gpd.GeoDataFrame, speed_flags: pd.DataFrame, crosswalk: pd.DataFrame, config: dict) -> pd.DataFrame:
    crawl_cfg = config["crawl"]
    max_waypoints = int(crawl_cfg.get("max_waypoints", 10))
    min_spacing_m = float(crawl_cfg.get("min_waypoint_spacing_m", 120.0))
    rows: list[dict] = []

    for row in raw.itertuples(index=False):
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
        bear = normalize_bearing_deg(getattr(row, "dir_deg_final", None))
        if bear is None:
            bear = normalize_bearing_deg(getattr(row, "bear_geom", None))
        rows.append(
            {
                "split_id": int(row.split_id),
                "raw_edge_id": int(row.raw_edge_id),
                "roadseg_id": str(row.roadseg_id),
                "roadname": str(getattr(row, "roadname", "") or ""),
                "road_class": int(row.road_class) if pd.notna(getattr(row, "road_class", np.nan)) else "",
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
    out = out.merge(speed_flags, on="roadseg_id", how="left")
    out["has_direct_speed"] = out["has_direct_speed"].fillna(False)
    out["n_speed_obs"] = pd.to_numeric(out["n_speed_obs"], errors="coerce").fillna(0).astype(int)
    out = out.merge(crosswalk, on=["split_id", "raw_edge_id"], how="left")
    out["matched_centerline"] = out["matched_centerline"].fillna(False)
    out["request_id"] = out["split_id"].astype(str) + "_" + out["raw_edge_id"].astype(str)
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
                "n_unique_segments": int(manifest[["split_id", "raw_edge_id"]].drop_duplicates().shape[0]),
                "n_with_direct_speed": int(manifest["has_direct_speed"].astype(bool).sum()),
                "n_matched_centerline": int(manifest["matched_centerline"].astype(bool).sum()),
                "median_length_m": float(pd.to_numeric(manifest["length_m"], errors="coerce").median()),
                "median_n_waypoints": float(pd.to_numeric(manifest["n_waypoints"], errors="coerce").median()),
                "share_with_waypoints": float((manifest["n_waypoints"] > 0).mean()),
            }
        ]
    )
    summary.to_csv(metrics_dir / "crawl_manifest_summary.csv", index=False)

    snapshot = {
        "stage": "build_crawl_manifest",
        "network_type": "raw_segment",
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
    baseline_flag = str(config.get("comparison", {}).get("baseline_flag", "keep_baseline"))
    base_out = resolve_path(output_dir) if output_dir else resolve_path(config.get("output_dir", "outputs"))
    version_root = base_out / actual_version
    baseline_root = base_out / baseline_version
    raw_path = baseline_root / "data" / "raw_segment_master.parquet"
    if not raw_path.exists():
        raise FileNotFoundError(f"Missing raw segment master at {raw_path}.")

    raw = load_baseline_raw_segments(baseline_root, baseline_flag)
    if limit is not None:
        raw = raw.head(int(limit)).copy()

    speed_path = resolve_raw_speed_path(config, baseline_root)
    speed_flags = load_direct_speed_flags(speed_path, raw["roadseg_id"])
    crosswalk = load_centerline_crosswalk(baseline_root)
    manifest = build_manifest_rows(raw, speed_flags, crosswalk, config)
    save_manifest(version_root, manifest, config)
    print(f"[raw build_crawl_manifest] saved {len(manifest):,} rows to {version_root / 'data' / 'crawl_manifest.parquet'}")


def main():
    args = parse_args()
    run(args.config, version_id=args.version_id, output_dir=args.output_dir, limit=args.limit)


if __name__ == "__main__":
    main()
