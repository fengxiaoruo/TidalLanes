"""
Query Baidu directionlite/v1/driving for baseline centerline validation manifest rows.

Each request sends origin, destination, and ordered waypoints so routing follows the
centerline corridor rather than a free shortest path between endpoints.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from zoneinfo import ZoneInfo

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.centerline_baidu_validation.baidu_client import extract_route, fetch_direction
from src.centerline_baidu_validation.manifest_sampling import filter_manifest, pick_stratified_sample
from src.national_tidal_commuting.baidu_rate_limit import wait_for_ak_slot
from src.national_tidal_commuting.common import (
    resolve_path,
    baidu_driving_directionlite_panel,
    ensure_version_dirs,
    get_version_root,
    load_baidu_ak,
    load_config,
    validate_query_window_local_clock,
)

SH_TZ = ZoneInfo("Asia/Shanghai")


def parse_args():
    parser = argparse.ArgumentParser(description="Query Baidu directionlite for centerline manifest rows.")
    parser.add_argument("--config", required=True, help="JSON config path.")
    parser.add_argument("--version-id", default=None, help="Optional validation version override.")
    parser.add_argument("--output-dir", default=None, help="Optional output dir override.")
    parser.add_argument("--panel", required=True, choices=["FREE", "AM", "PM"], help="Query panel.")
    parser.add_argument("--limit", type=int, default=None, help="Optional smoke-test row limit.")
    parser.add_argument(
        "--require-waypoints",
        action="store_true",
        help="Only query rows with n_waypoints > 0 (useful for smoke tests).",
    )
    parser.add_argument(
        "--only-inferred-speed",
        action="store_true",
        help="Only query rows with has_inferred_speed=True.",
    )
    parser.add_argument(
        "--stratified-sample",
        action="store_true",
        help="Stratified sample by length (300–5000 m) instead of manifest head.",
    )
    parser.add_argument("--sample-size", type=int, default=150, help="Rows when --stratified-sample.")
    parser.add_argument("--sample-seed", type=int, default=42)
    parser.add_argument(
        "--sample-manifest",
        default=None,
        help="Fixed sample CSV (e.g. prior crawl session sample_manifest.csv). Overrides stratified/head.",
    )
    parser.add_argument("--label", default="", help="Optional run label suffix.")
    parser.add_argument("--ak-source", default="auto", choices=["auto", "local", "env"])
    parser.add_argument("--ak-env-var", default="BAIDU_MAP_AUTH_TOKEN")
    parser.add_argument("--strict-clock-window", action="store_true")
    parser.add_argument("--allow-off-window", action="store_true")
    parser.add_argument("--retry-count", type=int, default=None)
    parser.add_argument("--sleep-sec", type=float, default=None)
    parser.add_argument(
        "--steps-info",
        action="store_true",
        help="Request step path geometry from API (required for route line export).",
    )
    parser.add_argument(
        "--no-steps-info",
        action="store_true",
        help="Disable steps_info even if enabled in config.",
    )
    return parser.parse_args()


def write_jsonl(path: Path, row: dict) -> None:
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def load_manifest(
    version_root: Path,
    *,
    limit: int | None,
    require_waypoints: bool,
    only_inferred_speed: bool,
    stratified_sample: bool,
    sample_size: int,
    sample_seed: int,
    sample_manifest: str | None,
) -> pd.DataFrame:
    if sample_manifest:
        path = Path(sample_manifest)
        if not path.is_absolute():
            cwd_path = Path.cwd() / path
            path = cwd_path if cwd_path.exists() else version_root / path
        if not path.exists():
            raise FileNotFoundError(f"Missing sample manifest: {path}")
        df = pd.read_csv(path)
        if limit is not None:
            df = df.head(int(limit)).copy()
        if df.empty:
            raise RuntimeError(f"No rows in sample manifest: {path}")
        return df.reset_index(drop=True)

    manifest_path = version_root / "data" / "crawl_manifest.parquet"
    if not manifest_path.exists():
        raise FileNotFoundError(f"Missing manifest: {manifest_path}. Run build_crawl_manifest.py first.")
    df = pd.read_parquet(manifest_path)
    df = filter_manifest(
        df,
        only_inferred_speed=only_inferred_speed,
        require_waypoints=require_waypoints,
    )
    if stratified_sample:
        n = int(sample_size if limit is None else min(sample_size, limit))
        df = pick_stratified_sample(df, n, seed=sample_seed)
    elif limit is not None:
        df = df.head(int(limit)).copy()
    if df.empty:
        raise RuntimeError("No manifest rows selected for query.")
    return df


def resolve_rate_limit(config: dict) -> tuple[bool, float, Path | None]:
    cfg = config.get("ak_rate_limit", {})
    enabled = bool(cfg.get("enabled", False))
    interval = float(cfg.get("min_interval_sec", 0.35))
    lock_path = cfg.get("lock_path")
    return enabled, interval, resolve_path(lock_path) if lock_path else None


def main():
    args = parse_args()
    config = load_config(args.config)
    version_root = get_version_root(config, args.version_id, args.output_dir)
    dirs = ensure_version_dirs(version_root)
    crawl_cfg = config.get("crawl", {})
    rate_limit_enabled, rate_limit_interval, rate_limit_lock = resolve_rate_limit(config)

    ak_selection = load_baidu_ak(env_var=args.ak_env_var, ak_source=args.ak_source)
    ak = ak_selection.token
    print(f"[query_baidu_direction] AK source: {ak_selection.source}")
    if ak_selection.note:
        print(f"[query_baidu_direction] {ak_selection.note}")
    if rate_limit_enabled:
        print(
            f"[query_baidu_direction] AK rate limit ON: min_interval={rate_limit_interval}s "
            f"lock={rate_limit_lock or 'default'}"
        )

    panel = baidu_driving_directionlite_panel(args.panel)
    validate_query_window_local_clock(
        args.panel,
        strict=bool(args.strict_clock_window),
        allow_off_window=bool(args.allow_off_window),
        strict_live=False,
    )

    manifest = load_manifest(
        version_root,
        limit=args.limit,
        require_waypoints=args.require_waypoints,
        only_inferred_speed=args.only_inferred_speed,
        stratified_sample=args.stratified_sample,
        sample_size=args.sample_size,
        sample_seed=args.sample_seed,
        sample_manifest=args.sample_manifest,
    )
    retry_count = args.retry_count if args.retry_count is not None else int(crawl_cfg.get("retry_count", 2))
    sleep_sec = args.sleep_sec if args.sleep_sec is not None else float(crawl_cfg.get("sleep_sec", 0.25))
    api_url = str(crawl_cfg.get("api_url", "https://api.map.baidu.com/directionlite/v1/driving"))
    coord_type = str(crawl_cfg.get("coord_type", "wgs84"))
    ret_coordtype = crawl_cfg.get("ret_coordtype")
    tactics = int(crawl_cfg.get("tactics_by_panel", {}).get(args.panel, panel["tactics"]))
    if args.no_steps_info:
        steps_info = 0
    elif args.steps_info:
        steps_info = 1
    else:
        steps_info = int(crawl_cfg.get("steps_info", 1))

    run_stamp = datetime.now(SH_TZ).strftime("%Y%m%dT%H%M%S")
    label_suffix = f"_{args.label}" if args.label else ""
    session_dir = dirs["api"] / "crawl_sessions" / f"{args.panel}_{run_stamp}{label_suffix}"
    session_dir.mkdir(parents=True, exist_ok=True)
    if args.stratified_sample:
        sample_path = session_dir / "sample_manifest.csv"
        manifest.to_csv(sample_path, index=False)
        print(f"[query_baidu_direction] stratified sample n={len(manifest)} -> {sample_path}")
    raw_jsonl = session_dir / "responses.jsonl"
    flat_csv = session_dir / "results_flat.csv"

    fieldnames = [
        "request_id",
        "skel_dir",
        "cline_id",
        "dir",
        "length_m",
        "n_waypoints",
        "waypoints_latlon",
        "panel",
        "tactics",
        "query_time_local",
        "duration_sec",
        "distance_m",
        "api_status",
        "api_message",
        "origin_text",
        "destination_text",
        "gps_direction",
        "has_inferred_speed",
        "n_path_points",
        "has_route_path",
        "route_path_wkt",
    ]
    successes = 0
    with flat_csv.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for seq, row in enumerate(manifest.to_dict("records"), start=1):
            origin_text = str(row["origin_text"])
            destination_text = str(row["destination_text"])
            waypoints_text = str(row.get("waypoints_latlon") or "").strip()
            gps_direction = row.get("gps_direction")
            gps_direction = int(gps_direction) if pd.notna(gps_direction) else None

            attempt = 0
            payload = None
            request_log = None
            while attempt <= retry_count:
                try:
                    if rate_limit_enabled:
                        wait_for_ak_slot(rate_limit_interval, lock_path=rate_limit_lock)
                    payload, request_log = fetch_direction(
                        api_url=api_url,
                        ak=ak,
                        origin_text=origin_text,
                        destination_text=destination_text,
                        waypoints_text=waypoints_text,
                        tactics=tactics,
                        coord_type=coord_type,
                        ret_coordtype=str(ret_coordtype) if ret_coordtype else None,
                        gps_direction=gps_direction,
                        steps_info=steps_info,
                    )
                    break
                except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
                    attempt += 1
                    if attempt > retry_count:
                        payload = {"status": -1, "message": f"{type(exc).__name__}: {exc}", "result": {}}
                        request_log = {
                            "origin": origin_text,
                            "destination": destination_text,
                            "waypoints": waypoints_text,
                            "waypoints_count": 0 if not waypoints_text else len(waypoints_text.split("|")),
                            "tactics": tactics,
                            "coord_type": coord_type,
                            "ret_coordtype": ret_coordtype,
                            "gps_direction": gps_direction,
                            "steps_info": steps_info,
                        }
                        break
                    time.sleep(1.0)

            query_time = datetime.now(SH_TZ).isoformat(timespec="seconds")
            write_jsonl(
                raw_jsonl,
                {
                    "seq": seq,
                    "request_id": row["request_id"],
                    "panel": args.panel,
                    "tactics": tactics,
                    "query_time_local": query_time,
                    "request": request_log,
                    "response": payload,
                },
            )

            status = int(payload.get("status", -1))
            message = str(payload.get("message", ""))
            route = extract_route(payload)
            if status == 0:
                successes += 1

            writer.writerow(
                {
                    "request_id": row["request_id"],
                    "skel_dir": row["skel_dir"],
                    "cline_id": row["cline_id"],
                    "dir": row["dir"],
                    "length_m": row["length_m"],
                    "n_waypoints": row["n_waypoints"],
                    "waypoints_latlon": waypoints_text,
                    "panel": args.panel,
                    "tactics": tactics,
                    "query_time_local": query_time,
                    "duration_sec": route["duration_sec"] if pd.notna(route["duration_sec"]) else "",
                    "distance_m": route["distance_m"] if pd.notna(route["distance_m"]) else "",
                    "api_status": status,
                    "api_message": message,
                    "origin_text": origin_text,
                    "destination_text": destination_text,
                    "gps_direction": gps_direction if gps_direction is not None else "",
                    "has_inferred_speed": row.get("has_inferred_speed", ""),
                    "n_path_points": route.get("n_path_points", 0),
                    "has_route_path": route.get("has_route_path", False),
                    "route_path_wkt": route.get("route_path_wkt", ""),
                }
            )
            fh.flush()
            print(
                f"[{seq}/{len(manifest)}] {row['request_id']} "
                f"wp={request_log['waypoints_count']} status={status} msg={message[:80]}"
            )
            time.sleep(sleep_sec)

    manifest_info = {
        "panel": args.panel,
        "tactics": tactics,
        "n_requested": int(len(manifest)),
        "n_success": int(successes),
        "require_waypoints": bool(args.require_waypoints),
        "only_inferred_speed": bool(args.only_inferred_speed),
        "stratified_sample": bool(args.stratified_sample),
        "sample_manifest": str(args.sample_manifest) if args.sample_manifest else None,
        "sample_size": int(args.sample_size) if args.stratified_sample else None,
        "sample_seed": int(args.sample_seed) if args.stratified_sample else None,
        "steps_info": int(steps_info),
        "ak_rate_limit_enabled": rate_limit_enabled,
        "ak_rate_limit_interval_sec": rate_limit_interval if rate_limit_enabled else None,
        "session_dir": str(session_dir),
        "ak_source": ak_selection.source,
    }
    (session_dir / "run_manifest.json").write_text(json.dumps(manifest_info, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[query_baidu_direction] wrote {flat_csv}")
    print(f"[query_baidu_direction] wrote {raw_jsonl}")
    print(f"[query_baidu_direction] success {successes}/{len(manifest)}")


if __name__ == "__main__":
    main()
