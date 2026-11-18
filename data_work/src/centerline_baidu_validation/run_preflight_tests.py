"""
Pre-crawl validation battery for centerline Baidu direction queries.

Runs stratified smoke samples across:
- coord_type variants (wgs84 / gcj02)
- with vs without ordered waypoints
- optional gps_direction on/off

Writes CSV tables and a short markdown report under metrics/preflight_<stamp>/.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.centerline_baidu_validation.baidu_client import extract_route, fetch_direction
from src.centerline_baidu_validation.manifest_sampling import pick_stratified_sample
from src.national_tidal_commuting.common import ensure_version_dirs, get_version_root, load_baidu_ak, load_config

SH_TZ = ZoneInfo("Asia/Shanghai")
COORD_TYPES = ["wgs84", "gcj02"]


def parse_args():
    parser = argparse.ArgumentParser(description="Run centerline Baidu preflight tests.")
    parser.add_argument("--config", required=True)
    parser.add_argument("--version-id", default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--panel", default="FREE", choices=["FREE", "AM", "PM"])
    parser.add_argument("--sample-size", type=int, default=12, help="Number of centerlines in stratified sample.")
    parser.add_argument("--sleep-sec", type=float, default=0.25)
    parser.add_argument("--ak-source", default="auto", choices=["auto", "local", "env"])
    return parser.parse_args()


def haversine_m(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    r = 6371000.0
    p = math.pi / 180.0
    dlat = (lat2 - lat1) * p
    dlon = (lon2 - lon1) * p
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1 * p) * math.cos(lat2 * p) * math.sin(dlon / 2) ** 2
    return 2 * r * math.asin(min(1.0, math.sqrt(a)))


def query_once(row: dict, *, ak: str, crawl_cfg: dict, tactics: int, coord_type: str, use_waypoints: bool, use_gps: bool) -> dict:
    waypoints_text = str(row.get("waypoints_latlon") or "").strip() if use_waypoints else ""
    gps_direction = row.get("gps_direction")
    gps_direction = int(gps_direction) if use_gps and pd.notna(gps_direction) else None
    payload, _ = fetch_direction(
        api_url=str(crawl_cfg.get("api_url", "https://api.map.baidu.com/directionlite/v1/driving")),
        ak=ak,
        origin_text=str(row["origin_text"]),
        destination_text=str(row["destination_text"]),
        waypoints_text=waypoints_text,
        tactics=tactics,
        coord_type=coord_type,
        ret_coordtype=str(crawl_cfg.get("ret_coordtype")) if crawl_cfg.get("ret_coordtype") else None,
        gps_direction=gps_direction,
        steps_info=0,
    )
    route = extract_route(payload)
    length_m = float(row["length_m"])
    od_m = haversine_m(float(row["origin_lon"]), float(row["origin_lat"]), float(row["destination_lon"]), float(row["destination_lat"]))
    dist = route["distance_m"]
    dur = route["duration_sec"]
    return {
        **route,
        "coord_type": coord_type,
        "use_waypoints": use_waypoints,
        "use_gps_direction": use_gps,
        "length_m": length_m,
        "od_haversine_m": od_m,
        "distance_over_length": dist / length_m if length_m > 0 and np.isfinite(dist) else np.nan,
        "distance_over_od": dist / od_m if od_m > 0 and np.isfinite(dist) else np.nan,
        "duration_min": dur / 60.0 if np.isfinite(dur) else np.nan,
        "implied_speed_kmh": (dist / 1000.0) / (dur / 3600.0) if np.isfinite(dist) and np.isfinite(dur) and dur > 0 else np.nan,
    }


def score_coord_type(df: pd.DataFrame) -> pd.DataFrame:
    ok = df[(df["api_status"] == 0) & df["use_waypoints"] & df["use_gps_direction"]].copy()
    rows = []
    for coord_type, sub in ok.groupby("coord_type"):
        ratios = sub["distance_over_length"].replace([np.inf, -np.inf], np.nan).dropna()
        rows.append(
            {
                "coord_type": coord_type,
                "n_ok": int(len(ratios)),
                "median_distance_over_length": float(ratios.median()) if len(ratios) else np.nan,
                "p90_distance_over_length": float(ratios.quantile(0.9)) if len(ratios) else np.nan,
                "share_ratio_0p8_1p5": float(((ratios >= 0.8) & (ratios <= 1.5)).mean()) if len(ratios) else np.nan,
                "median_implied_speed_kmh": float(sub["implied_speed_kmh"].median()),
            }
        )
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    out = out.sort_values(["share_ratio_0p8_1p5", "median_distance_over_length"], ascending=[False, True])
    return out


def write_report(path: Path, *, panel: str, sample: pd.DataFrame, detail: pd.DataFrame, coord_score: pd.DataFrame, wp_compare: pd.DataFrame, recommendation: str):
    lines = [
        "# Centerline Baidu preflight report",
        "",
        f"- Generated: {datetime.now(SH_TZ).isoformat(timespec='seconds')}",
        f"- Panel: {panel}",
        f"- Sample size: {len(sample)}",
        "",
        "## Recommendation",
        "",
        recommendation,
        "",
        "## Coord type score (with waypoints + gps_direction)",
        "",
    ]
    if len(coord_score):
        lines.append(coord_score.to_csv(index=False))
    else:
        lines.append("No successful coord-type rows.")
    lines.extend(["", "## Waypoint A/B (best coord_type, median over sample)", ""])
    if len(wp_compare):
        lines.append(wp_compare.to_csv(index=False))
    else:
        lines.append("No waypoint comparison available.")
    lines.extend(["", "## Sample rows", ""])
    lines.append(sample[["request_id", "length_m", "n_waypoints", "has_inferred_speed", "has_raw_match"]].to_csv(index=False))
    lines.extend(["", "## Notes", ""])
    lines.append("- Target: `distance_m / length_m` in `[0.8, 1.5]` for corridor-following routes.")
    lines.append("- Compare `with_waypoints` vs `without_waypoints` under the chosen coord_type.")
    lines.append("- Full detail in `preflight_detail.csv`.")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main():
    args = parse_args()
    config = load_config(args.config)
    version_root = get_version_root(config, args.version_id, args.output_dir)
    dirs = ensure_version_dirs(version_root)
    crawl_cfg = config.get("crawl", {})
    ak = load_baidu_ak(ak_source=args.ak_source).token
    tactics = int(crawl_cfg.get("tactics_by_panel", {}).get(args.panel, 0))

    manifest = pd.read_parquet(version_root / "data" / "crawl_manifest.parquet")
    sample = pick_stratified_sample(manifest, args.sample_size)
    stamp = datetime.now(SH_TZ).strftime("%Y%m%dT%H%M%S")
    out_dir = dirs["metrics"] / f"preflight_{args.panel}_{stamp}"
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = []
    total = len(sample) * len(COORD_TYPES) * 4  # wp x gps combos
    step = 0
    for row in sample.to_dict("records"):
        for coord_type in COORD_TYPES:
            for use_waypoints in [True, False]:
                for use_gps in [True, False]:
                    step += 1
                    result = query_once(
                        row,
                        ak=ak,
                        crawl_cfg=crawl_cfg,
                        tactics=tactics,
                        coord_type=coord_type,
                        use_waypoints=use_waypoints,
                        use_gps=use_gps,
                    )
                    rows.append({"request_id": row["request_id"], **result})
                    print(
                        f"[{step}/{total}] {row['request_id']} {coord_type} "
                        f"wp={use_waypoints} gps={use_gps} status={result['api_status']} "
                        f"dist={result['distance_m']} ratio={result['distance_over_length']:.3f}"
                        if np.isfinite(result.get("distance_over_length", np.nan))
                        else f"[{step}/{total}] {row['request_id']} {coord_type} wp={use_waypoints} gps={use_gps} status={result['api_status']}"
                    )
                    time.sleep(args.sleep_sec)

    detail = pd.DataFrame(rows)
    detail.to_csv(out_dir / "preflight_detail.csv", index=False)
    sample.to_csv(out_dir / "preflight_sample.csv", index=False)

    coord_score = score_coord_type(detail)
    coord_score.to_csv(out_dir / "preflight_coord_score.csv", index=False)
    best_coord = str(coord_score.iloc[0]["coord_type"]) if len(coord_score) else str(crawl_cfg.get("coord_type", "wgs84"))

    best = detail[(detail["coord_type"] == best_coord) & (detail["api_status"] == 0)].copy()
    wp_rows = []
    for request_id, sub in best.groupby("request_id"):
        sub = sub.set_index(["use_waypoints", "use_gps_direction"])
        try:
            wp = sub.loc[(True, True)]
            od = sub.loc[(False, True)]
            wp_rows.append(
                {
                    "request_id": request_id,
                    "length_m": wp["length_m"],
                    "ratio_with_waypoints": wp["distance_over_length"],
                    "ratio_without_waypoints": od["distance_over_length"],
                    "distance_delta_m": wp["distance_m"] - od["distance_m"],
                    "duration_delta_min": wp["duration_min"] - od["duration_min"],
                }
            )
        except KeyError:
            continue
    wp_compare = pd.DataFrame(wp_rows)
    if len(wp_compare):
        wp_summary = pd.DataFrame(
            [
                {
                    "coord_type": best_coord,
                    "n": len(wp_compare),
                    "median_ratio_with_waypoints": float(wp_compare["ratio_with_waypoints"].median()),
                    "median_ratio_without_waypoints": float(wp_compare["ratio_without_waypoints"].median()),
                    "median_distance_delta_m": float(wp_compare["distance_delta_m"].median()),
                    "share_wp_closer_to_length": float(((wp_compare["ratio_with_waypoints"] >= 0.8) & (wp_compare["ratio_with_waypoints"] <= 1.5)).mean()),
                    "share_od_closer_to_length": float(((wp_compare["ratio_without_waypoints"] >= 0.8) & (wp_compare["ratio_without_waypoints"] <= 1.5)).mean()),
                }
            ]
        )
        wp_summary.to_csv(out_dir / "preflight_waypoint_summary.csv", index=False)
        wp_compare.to_csv(out_dir / "preflight_waypoint_compare.csv", index=False)
    else:
        wp_summary = pd.DataFrame()
        wp_compare = pd.DataFrame()

    if len(coord_score):
        best_row = coord_score.iloc[0]
        recommendation = (
            f"Recommend `coord_type={best_coord}` for formal crawl. "
            f"Median distance/length={best_row['median_distance_over_length']:.3f}, "
            f"share in [0.8,1.5]={best_row['share_ratio_0p8_1p5']:.2%}."
        )
        if len(wp_summary):
            recommendation += (
                f" Under {best_coord}, waypoint routing median ratio={wp_summary.iloc[0]['median_ratio_with_waypoints']:.3f} "
                f"vs OD-only={wp_summary.iloc[0]['median_ratio_without_waypoints']:.3f}."
            )
    else:
        recommendation = "No successful API rows; check AK, quota, or coordinate inputs."

    write_report(
        out_dir / "PREFLIGHT_REPORT.md",
        panel=args.panel,
        sample=sample,
        detail=detail,
        coord_score=coord_score,
        wp_compare=wp_summary if len(wp_summary) else wp_compare,
        recommendation=recommendation,
    )
    (out_dir / "preflight_summary.json").write_text(
        json.dumps(
            {
                "panel": args.panel,
                "recommended_coord_type": best_coord,
                "recommendation": recommendation,
                "out_dir": str(out_dir),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"[run_preflight_tests] wrote {out_dir / 'PREFLIGHT_REPORT.md'}")
    print(f"[run_preflight_tests] {recommendation}")


if __name__ == "__main__":
    main()
