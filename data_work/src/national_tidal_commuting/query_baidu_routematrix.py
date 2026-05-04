from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen
from zoneinfo import ZoneInfo

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.national_tidal_commuting.common import (
    baidu_driving_directionlite_panel,
    ensure_version_dirs,
    format_latlon,
    get_version_root,
    load_config,
    load_baidu_ak,
    validate_query_window_local_clock,
)


API_URL = "https://api.map.baidu.com/directionlite/v1/driving"
SH_TZ = ZoneInfo("Asia/Shanghai")


def parse_args():
    parser = argparse.ArgumentParser(description="Query Baidu directionlite/v1/driving for top commuting pairs.")
    parser.add_argument("--config", required=True, help="JSON config path.")
    parser.add_argument("--version-id", default=None, help="Optional version override.")
    parser.add_argument("--output-dir", default=None, help="Optional output dir override.")
    parser.add_argument(
        "--query-window",
        required=True,
        choices=["AM", "PM", "FREE", "NIGHT"],
        help="AM/PM: tactics 2, run near 08:00/18:00 (见 --strict-clock-window). FREE: tactics=0 轻量常规路线基线. NIGHT: legacy 22:30 夜间路况.",
    )
    parser.add_argument("--query-date", default=None, help="YYYY-MM-DD, used for logging only.")
    parser.add_argument(
        "--ak-source",
        default="auto",
        choices=["auto", "local", "env"],
        help="AK 来源：auto 默认优先 ak_local.py，再回退环境变量；也可强制 local 或 env。",
    )
    parser.add_argument("--ak-env-var", default="BAIDU_MAP_AUTH_TOKEN", help="Environment variable name used when --ak-source env/auto falls back to env.")
    parser.add_argument(
        "--strict-clock-window",
        action="store_true",
        help="可选：北京时间需落在较宽的 AM(05:00–10:30)/PM(16:00–21:30)/晚间建议窗 内，否则退出。默认不检查，长任务可任意时刻开跑。",
    )
    parser.add_argument(
        "--allow-off-window",
        action="store_true",
        help="与 --strict-clock-window 联用时显式允许任意时刻（仍会关闭严格窗）。单独使用可兼容旧脚本。",
    )
    parser.add_argument(
        "--strict-live-window",
        action="store_true",
        help="更严格的真实时段口径：只建议对单个 AM / PM / NIGHT panel 单独运行，并要求北京时间落在对应窄时窗内。",
    )
    parser.add_argument("--retry-count", type=int, default=2, help="Retries for transient request failures.")
    parser.add_argument("--sleep-sec", type=float, default=0.2, help="Sleep between successful requests.")
    return parser.parse_args()


def chunked(seq, size):
    for start in range(0, len(seq), size):
        yield seq[start : start + size]


def build_request_groups(df: pd.DataFrame, direction: str):
    if direction == "home_to_work":
        work = df.rename(columns={"home_lon": "origin_lon", "home_lat": "origin_lat", "work_lon": "dest_lon", "work_lat": "dest_lat"}).copy()
        work["origin_type"] = "home"
        work["dest_type"] = "work"
    else:
        work = df.rename(columns={"work_lon": "origin_lon", "work_lat": "origin_lat", "home_lon": "dest_lon", "home_lat": "dest_lat"}).copy()
        work["origin_type"] = "work"
        work["dest_type"] = "home"
    return work


def fetch_batch(ak: str, origin_text: str, destination_texts: list[str], *, tactics: int) -> dict:
    if len(destination_texts) != 1:
        raise ValueError("directionlite/v1/driving only supports one destination per request in this workflow.")
    params = {
        "origin": origin_text,
        "destination": destination_texts[0],
        "coord_type": "wgs84",
        "tactics": str(tactics),
        "ak": ak,
    }
    with urlopen(f"{API_URL}?{urlencode(params)}", timeout=30) as response:
        payload = response.read().decode("utf-8")
    return json.loads(payload)


def write_jsonl(path: Path, row: dict):
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def main():
    args = parse_args()
    config = load_config(args.config)
    version_root = get_version_root(config, args.version_id, args.output_dir)
    dirs = ensure_version_dirs(version_root)
    top_pairs_path = dirs["data"] / "top_commuting_pairs.csv"
    if not top_pairs_path.exists():
        raise FileNotFoundError(f"Missing pair sample file: {top_pairs_path}. Run build_top_pairs.py first.")
    ak_selection = load_baidu_ak(env_var=args.ak_env_var, ak_source=args.ak_source)
    ak = ak_selection.token
    print(f"[query_baidu] AK source: {ak_selection.source}")
    if ak_selection.note:
        print(f"[query_baidu] {ak_selection.note}")
    panel = baidu_driving_directionlite_panel(args.query_window)
    validate_query_window_local_clock(
        args.query_window,
        strict=bool(args.strict_clock_window),
        allow_off_window=bool(args.allow_off_window),
        strict_live=bool(args.strict_live_window),
    )

    df = pd.read_csv(top_pairs_path)
    if df.empty:
        raise RuntimeError(f"No pairs found in {top_pairs_path}")
    if len(df) > 5000:
        print(f"[query_baidu] info: {len(df)} pair rows; directionlite/v1 will send one request per direction-row, so confirm quota and run time.")
    query_date = args.query_date or datetime.now(SH_TZ).strftime("%Y-%m-%d")
    run_stamp = datetime.now(SH_TZ).strftime("%Y%m%dT%H%M%S")
    raw_jsonl = dirs["api"] / f"baidu_matrix_raw_{args.query_window}_{run_stamp}.jsonl"
    flat_csv = dirs["api"] / f"baidu_matrix_results_{args.query_window}_{run_stamp}.csv"

    fieldnames = [
        "city_id",
        "city_label",
        "pair_id",
        "origin_type",
        "dest_type",
        "query_window",
        "tactics",
        "notional_departure_hhmm",
        "panel_metric_note_zh",
        "query_date",
        "query_time_local",
        "request_batch_id",
        "duration_sec",
        "distance_m",
        "api_status",
        "api_message",
        "origin_lon",
        "origin_lat",
        "dest_lon",
        "dest_lat",
    ]
    with flat_csv.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        batch_seq = 0
        for direction in ["home_to_work", "work_to_home"]:
            work = build_request_groups(df, direction)
            for row in work.to_dict("records"):
                batch_seq += 1
                request_batch_id = f"{args.query_window}_{run_stamp}_{batch_seq:05d}"
                origin_text = format_latlon(row["origin_lat"], row["origin_lon"])
                destination_texts = [format_latlon(row["dest_lat"], row["dest_lon"])]
                attempt = 0
                payload = None
                while attempt <= args.retry_count:
                    try:
                        payload = fetch_batch(ak, origin_text, destination_texts, tactics=panel["tactics"])
                        break
                    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
                        attempt += 1
                        if attempt > args.retry_count:
                            payload = {"status": -1, "message": f"{type(exc).__name__}: {exc}", "result": {}}
                            break
                        time.sleep(1.0)

                request_time = datetime.now(SH_TZ).isoformat(timespec="seconds")
                write_jsonl(
                    raw_jsonl,
                    {
                        "request_batch_id": request_batch_id,
                        "query_window": args.query_window,
                        "tactics": panel["tactics"],
                        "notional_departure_hhmm": panel["notional_departure_hhmm"],
                        "query_date": query_date,
                        "query_time_local": request_time,
                        "origin": origin_text,
                        "destination_count": 1,
                        "direction": direction,
                        "response": payload,
                    },
                )
                status = int(payload.get("status", -1))
                message = str(payload.get("message", ""))
                result = payload.get("result", {}) if isinstance(payload.get("result", {}), dict) else {}
                routes = result.get("routes", []) if isinstance(result.get("routes", []), list) else []
                route0 = routes[0] if routes else {}
                writer.writerow(
                    {
                        "city_id": row["city_id"],
                        "city_label": row["city_label"],
                        "pair_id": row["pair_id"],
                        "origin_type": row["origin_type"],
                        "dest_type": row["dest_type"],
                        "query_window": args.query_window,
                        "tactics": panel["tactics"],
                        "notional_departure_hhmm": panel["notional_departure_hhmm"],
                        "panel_metric_note_zh": panel["metric_note_zh"],
                        "query_date": query_date,
                        "query_time_local": request_time,
                        "request_batch_id": request_batch_id,
                        "duration_sec": route0.get("duration", 0) if isinstance(route0, dict) else 0,
                        "distance_m": route0.get("distance", 0) if isinstance(route0, dict) else 0,
                        "api_status": status,
                        "api_message": message,
                        "origin_lon": row["origin_lon"],
                        "origin_lat": row["origin_lat"],
                        "dest_lon": row["dest_lon"],
                        "dest_lat": row["dest_lat"],
                    }
                )
                fh.flush()
                time.sleep(args.sleep_sec)
    print(f"[query_baidu_routematrix] wrote flat results to {flat_csv}")
    print(f"[query_baidu_routematrix] wrote raw responses to {raw_jsonl}")


if __name__ == "__main__":
    main()
