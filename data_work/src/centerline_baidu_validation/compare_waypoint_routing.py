"""
Smoke / diagnostic: compare Baidu routing with vs without ordered waypoints.

For each centerline, query twice (same origin/destination/tactics) and report
distance/duration differences relative to centerline geometric length.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.centerline_baidu_validation.baidu_client import extract_route, fetch_direction
from src.national_tidal_commuting.common import ensure_version_dirs, get_version_root, load_baidu_ak, load_config

SH_TZ = ZoneInfo("Asia/Shanghai")


def parse_args():
    parser = argparse.ArgumentParser(description="Compare Baidu routing with/without waypoints.")
    parser.add_argument("--config", required=True)
    parser.add_argument("--version-id", default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--panel", default="FREE", choices=["FREE", "AM", "PM"])
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--require-waypoints", action="store_true")
    parser.add_argument("--steps-info", action="store_true", help="Request step/path details from API.")
    parser.add_argument("--sleep-sec", type=float, default=0.3)
    parser.add_argument("--ak-source", default="auto", choices=["auto", "local", "env"])
    return parser.parse_args()


def load_manifest(version_root: Path, *, limit: int, require_waypoints: bool) -> pd.DataFrame:
    df = pd.read_parquet(version_root / "data" / "crawl_manifest.parquet")
    if require_waypoints:
        df = df[pd.to_numeric(df["n_waypoints"], errors="coerce").fillna(0).gt(0)].copy()
    return df.head(int(limit)).copy()


def query_variant(row: dict, *, ak: str, crawl_cfg: dict, tactics: int, use_waypoints: bool, steps_info: int) -> dict:
    waypoints_text = str(row.get("waypoints_latlon") or "").strip() if use_waypoints else ""
    gps_direction = row.get("gps_direction")
    gps_direction = int(gps_direction) if pd.notna(gps_direction) else None
    payload, request_log = fetch_direction(
        api_url=str(crawl_cfg.get("api_url", "https://api.map.baidu.com/directionlite/v1/driving")),
        ak=ak,
        origin_text=str(row["origin_text"]),
        destination_text=str(row["destination_text"]),
        waypoints_text=waypoints_text,
        tactics=tactics,
        coord_type=str(crawl_cfg.get("coord_type", "wgs84")),
        ret_coordtype=str(crawl_cfg.get("ret_coordtype")) if crawl_cfg.get("ret_coordtype") else None,
        gps_direction=gps_direction,
        steps_info=steps_info,
    )
    out = extract_route(payload)
    out["request"] = request_log
    out["response"] = payload
    return out


def main():
    args = parse_args()
    config = load_config(args.config)
    version_root = get_version_root(config, args.version_id, args.output_dir)
    dirs = ensure_version_dirs(version_root)
    crawl_cfg = config.get("crawl", {})
    ak = load_baidu_ak(ak_source=args.ak_source).token
    tactics = int(crawl_cfg.get("tactics_by_panel", {}).get(args.panel, 0))
    steps_info = 1 if args.steps_info else 0

    manifest = load_manifest(version_root, limit=args.limit, require_waypoints=args.require_waypoints)
    rows = []
    run_stamp = datetime.now(SH_TZ).strftime("%Y%m%dT%H%M%S")
    out_dir = dirs["metrics"] / f"waypoint_ab_{args.panel}_{run_stamp}"
    out_dir.mkdir(parents=True, exist_ok=True)

    for i, row in enumerate(manifest.to_dict("records"), start=1):
        length_m = float(row["length_m"])
        for label, use_wp in [("with_waypoints", True), ("without_waypoints", False)]:
            result = query_variant(
                row,
                ak=ak,
                crawl_cfg=crawl_cfg,
                tactics=tactics,
                use_waypoints=use_wp,
                steps_info=steps_info,
            )
            rows.append(
                {
                    "request_id": row["request_id"],
                    "variant": label,
                    "length_m": length_m,
                    "n_waypoints": int(row["n_waypoints"]) if use_wp else 0,
                    "api_status": result["api_status"],
                    "api_message": result["api_message"],
                    "duration_sec": result["duration_sec"],
                    "distance_m": result["distance_m"],
                    "distance_over_length": result["distance_m"] / length_m if length_m > 0 else float("nan"),
                    "duration_min": result["duration_sec"] / 60.0 if pd.notna(result["duration_sec"]) else float("nan"),
                    "n_steps": result["n_steps"],
                    "steps_distance_m": result["steps_distance_m"],
                }
            )
            time.sleep(args.sleep_sec)
        wp = rows[-2]
        no = rows[-1]
        print(
            f"[{i}/{len(manifest)}] {row['request_id']} "
            f"len={length_m:.0f}m | "
            f"wp_dist={wp['distance_m']} ({wp['api_status']}) | "
            f"od_dist={no['distance_m']} ({no['api_status']}) | "
            f"ratio_wp={wp['distance_over_length']:.3f} ratio_od={no['distance_over_length']:.3f}"
        )

    out = pd.DataFrame(rows)
    wide = out.pivot_table(
        index=["request_id", "length_m"],
        columns="variant",
        values=["distance_m", "duration_sec", "distance_over_length", "api_status"],
        aggfunc="first",
    )
    wide.columns = [f"{a}_{b}" for a, b in wide.columns]
    wide = wide.reset_index()
    if "distance_m_with_waypoints" in wide.columns and "distance_m_without_waypoints" in wide.columns:
        wide["distance_delta_m"] = wide["distance_m_with_waypoints"] - wide["distance_m_without_waypoints"]
        wide["distance_ratio_wp_over_od"] = wide["distance_m_with_waypoints"] / wide["distance_m_without_waypoints"]
    if "duration_sec_with_waypoints" in wide.columns and "duration_sec_without_waypoints" in wide.columns:
        wide["duration_delta_sec"] = wide["duration_sec_with_waypoints"] - wide["duration_sec_without_waypoints"]

    csv_path = out_dir / "waypoint_ab_compare.csv"
    wide_path = out_dir / "waypoint_ab_wide.csv"
    out.to_csv(csv_path, index=False)
    wide.to_csv(wide_path, index=False)
    (out_dir / "waypoint_ab_long.json").write_text(out.to_json(orient="records", force_ascii=False, indent=2), encoding="utf-8")
    print(f"[compare_waypoint_routing] wrote {csv_path}")
    print(f"[compare_waypoint_routing] wrote {wide_path}")


if __name__ == "__main__":
    main()
