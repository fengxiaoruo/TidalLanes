"""
一键跑满「全国 top-50 城 × 每城 50 对」的百度轻量驾车路线规划三档（默认 FREE/AM/PM），
每条 OD 完成后立即落盘，并在终端显示进度。

AK：默认优先从同包 `ak_local.py` 读 BAIDU_MAP_AUTH_TOKEN；若本地文件不存在，再回退到环境变量。
如需强制来源，可传 `--ak-source local|env|auto`。

输出目录（在 version 的 api 下）：
  crawl_sessions/<crawl_run_id>/
    session_matrix_flat.csv   — 逐批追加的展平表（含 crawl_run_id / crawl_label 等，便于后处理区分轮次与时段）
    session_raw.jsonl         — 每批完整 API 回包
    progress.jsonl            — 每批一条进度/心跳
    run_manifest.json         — 起止与统计

Usage:
  cd data_work
  python -m src.national_tidal_commuting.run_full_crawl
  python -m src.national_tidal_commuting.run_full_crawl --config config/national_tidal_commuting_top50cities_2021_v1.json --label my_run_v1
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

from src.national_tidal_commuting.common import (
    baidu_driving_directionlite_panel,
    ensure_version_dirs,
    format_latlon,
    get_version_root,
    load_config,
    load_baidu_ak,
    validate_query_window_local_clock,
)
from src.national_tidal_commuting.query_baidu_routematrix import (
    SH_TZ,
    build_request_groups,
    fetch_batch,
)


def _build_task_list(df: pd.DataFrame) -> list[tuple[str, dict[str, Any]]]:
    out: list[tuple[str, dict[str, Any]]] = []
    for direction in ["home_to_work", "work_to_home"]:
        work = build_request_groups(df, direction)
        for row in work.to_dict("records"):
            out.append((direction, row))
    return out


def _append_progress(path: Path, rec: dict[str, Any]) -> None:
    rec = {**rec, "logged_at_local": datetime.now(SH_TZ).isoformat(timespec="seconds")}
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
        fh.flush()
        try:
            os.fsync(fh.fileno())
        except OSError:
            pass


@dataclass
class CrawlStats:
    n_batches: int = 0
    n_rows: int = 0
    n_api_error_batches: int = 0


def run() -> None:
    parser = argparse.ArgumentParser(description="Full Baidu directionlite/v1/driving crawl (FREE+AM+PM) with per-request persistence.")
    parser.add_argument("--config", default="config/national_tidal_commuting_top50cities_2021_v1.json")
    parser.add_argument("--version-id", default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument(
        "--label",
        default="",
        help="自定义标签，写入 CSV/ manifest，后处理时区分跑数目的（如 morning_2026Q2）。",
    )
    parser.add_argument(
        "--panels",
        default="FREE,AM,PM",
        help="逗号分隔，如 FREE,AM,PM 或只跑 FREE",
    )
    parser.add_argument("--query-date", default=None, help="仅写入 CSV 的 query_date 列，默认今天（上海）。")
    parser.add_argument(
        "--ak-source",
        default="auto",
        choices=["auto", "local", "env"],
        help="AK 来源：auto 默认优先 ak_local.py，再回退环境变量；也可强制 local 或 env。",
    )
    parser.add_argument("--sleep-sec", type=float, default=0.2)
    parser.add_argument("--retry-count", type=int, default=2)
    parser.add_argument("--strict-clock-window", action="store_true")
    parser.add_argument("--allow-off-window", action="store_true")
    parser.add_argument(
        "--strict-live-window",
        action="store_true",
        help="更严格的真实时段口径：一次只跑一个 AM / PM / NIGHT panel，并要求北京时间落在对应窄时窗内。",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    version_root = get_version_root(config, args.version_id, args.output_dir)
    dirs = ensure_version_dirs(version_root)
    top_pairs_path = dirs["data"] / "top_commuting_pairs.csv"
    if not top_pairs_path.exists():
        raise FileNotFoundError(f"缺少 {top_pairs_path}，请先 build_top_pairs。")

    ak_selection = load_baidu_ak(ak_source=args.ak_source)
    ak = ak_selection.token
    print(f"[run_full_crawl] AK source: {ak_selection.source}")
    if ak_selection.note:
        print(f"[run_full_crawl] {ak_selection.note}")
    df = pd.read_csv(top_pairs_path)
    if df.empty:
        raise RuntimeError("top_commuting_pairs 为空。")
    n_pairs = len(df)
    if n_pairs > 5000:
        print(f"[run_full_crawl] 提示: {n_pairs} 对，请确认百度配额与耗时。")

    query_date = args.query_date or datetime.now(SH_TZ).strftime("%Y-%m-%d")
    started = datetime.now(SH_TZ)
    run_id = started.strftime("%Y%m%dT%H%M%S")
    crawl_label = (args.label or "").strip() or run_id
    started_iso = started.isoformat(timespec="seconds")

    session_dir = dirs["api"] / "crawl_sessions" / run_id
    session_dir.mkdir(parents=True, exist_ok=True)
    flat_path = session_dir / "session_matrix_flat.csv"
    raw_path = session_dir / "session_raw.jsonl"
    prog_path = session_dir / "progress.jsonl"
    manifest_path = session_dir / "run_manifest.json"

    panels = [p.strip().upper() for p in args.panels.split(",") if p.strip()]
    if args.strict_live_window:
        if len(panels) != 1:
            raise RuntimeError("启用 --strict-live-window 时，一次只能跑一个 panel。请分别运行 AM、PM、NIGHT。")
        if panels[0] == "FREE":
            raise RuntimeError("启用 --strict-live-window 时不接受 FREE。请改用 NIGHT 作为低拥堵时段并单独运行。")

    fieldnames = [
        "crawl_run_id",
        "crawl_label",
        "crawl_session_start_local",
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

    all_tasks: list[tuple[str, list[tuple[str, dict[str, Any]]]]] = []
    total_batches = 0
    for w in panels:
        validate_query_window_local_clock(
            w,
            strict=bool(args.strict_clock_window),
            allow_off_window=bool(args.allow_off_window),
            strict_live=bool(args.strict_live_window),
        )
        tw = _build_task_list(df)
        all_tasks.append((w, tw))
        total_batches += len(tw)

    manifest = {
        "crawl_run_id": run_id,
        "crawl_label": crawl_label,
        "crawl_session_start_local": started_iso,
        "timezone": "Asia/Shanghai",
        "config_path": config.get("_config_path"),
        "version_root": str(version_root.resolve()),
        "top_commuting_pairs": str(top_pairs_path.resolve()),
        "n_pair_rows": n_pairs,
        "panels": panels,
        "expected_total_batches": total_batches,
        "expected_api_rows": n_pairs * 2 * len(panels),
        "output_session_dir": str(session_dir.resolve()),
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[run_full_crawl] crawl_run_id={run_id} label={crawl_label!r} 会写 {len(panels) * 2 * n_pairs} 条方向记录（{len(panels)} 档 × 2 向 × {n_pairs} 对）。")
    print(f"[run_full_crawl] 输出: {session_dir}")
    _append_progress(
        prog_path,
        {
            "event": "crawl_start",
            "crawl_run_id": run_id,
            "crawl_label": crawl_label,
            "panels": panels,
            "total_batches": total_batches,
        },
    )

    batch_global = 0
    stat = CrawlStats()
    header_written = False

    def _open_csv_append():
        return flat_path.open("a", newline="", encoding="utf-8")

    for widx, (query_window, tasks) in enumerate(all_tasks, start=1):
        panel = baidu_driving_directionlite_panel(query_window)
        print(f"\n[run_full_crawl] === 档 {widx}/{len(panels)}: {query_window} tactics={panel['tactics']} ===\n", flush=True)
        for tidx, (direction, row) in enumerate(tasks, start=1):
            batch_global += 1
            request_batch_id = f"{query_window}_{run_id}_{batch_global:05d}"
            origin_text = format_latlon(row["origin_lat"], row["origin_lon"])
            destination_texts = [format_latlon(row["dest_lat"], row["dest_lon"])]

            attempt = 0
            payload: dict = {}
            while attempt <= args.retry_count:
                try:
                    payload = fetch_batch(ak, origin_text, destination_texts, tactics=panel["tactics"])
                    break
                except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
                    attempt += 1
                    if attempt > args.retry_count:
                        payload = {"status": -1, "message": f"{type(exc).__name__}: {exc}", "result": []}
                        break
                    time.sleep(1.0)

            request_time = datetime.now(SH_TZ).isoformat(timespec="seconds")
            raw_row = {
                "crawl_run_id": run_id,
                "crawl_label": crawl_label,
                "request_batch_id": request_batch_id,
                "query_window": query_window,
                "tactics": panel["tactics"],
                "notional_departure_hhmm": panel["notional_departure_hhmm"],
                "query_date": query_date,
                "query_time_local": request_time,
                "origin": origin_text,
                "destination_count": 1,
                "direction": direction,
                "response": payload,
            }
            with raw_path.open("a", encoding="utf-8") as rfh:
                rfh.write(json.dumps(raw_row, ensure_ascii=False) + "\n")
                rfh.flush()
                try:
                    os.fsync(rfh.fileno())
                except OSError:
                    pass

            status = int(payload.get("status", -1))
            if status != 0:
                stat.n_api_error_batches += 1
            message = str(payload.get("message", ""))
            result = payload.get("result", {}) if isinstance(payload.get("result", {}), dict) else {}
            routes = result.get("routes", []) if isinstance(result.get("routes", []), list) else []
            route0 = routes[0] if routes else {}
            n_written = 0
            with _open_csv_append() as fh:
                wcsv = csv.DictWriter(fh, fieldnames=fieldnames)
                if not header_written:
                    wcsv.writeheader()
                    header_written = True
                wcsv.writerow(
                    {
                        "crawl_run_id": run_id,
                        "crawl_label": crawl_label,
                        "crawl_session_start_local": started_iso,
                        "city_id": row["city_id"],
                        "city_label": row["city_label"],
                        "pair_id": row["pair_id"],
                        "origin_type": row["origin_type"],
                        "dest_type": row["dest_type"],
                        "query_window": query_window,
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
                n_written += 1
                fh.flush()
                try:
                    os.fsync(fh.fileno())
                except OSError:
                    pass

            stat.n_batches += 1
            stat.n_rows += n_written
            pct = 100.0 * batch_global / total_batches if total_batches else 100.0
            line = (
                f"[{query_window}] 批 {tidx}/{len(tasks)} | 总进度 {batch_global}/{total_batches} ({pct:.1f}%) | 本批行 {n_written} | api status {status}"
            )
            print(line, flush=True)
            _append_progress(
                prog_path,
                {
                    "event": "batch_done",
                    "crawl_run_id": run_id,
                    "crawl_label": crawl_label,
                    "query_window": query_window,
                    "direction": direction,
                    "batch_in_window": tidx,
                    "batches_in_window": len(tasks),
                    "batch_global": batch_global,
                    "batches_total": total_batches,
                    "percent": round(pct, 2),
                    "n_rows_in_batch": n_written,
                    "api_status": status,
                },
            )
            time.sleep(args.sleep_sec)

    manifest["finished_at_local"] = datetime.now(SH_TZ).isoformat(timespec="seconds")
    manifest["actual_total_batches"] = stat.n_batches
    manifest["actual_total_rows"] = stat.n_rows
    manifest["n_batches_with_api_status_nonzero"] = stat.n_api_error_batches
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    _append_progress(prog_path, {"event": "crawl_end", "crawl_run_id": run_id, **{k: manifest[k] for k in ("actual_total_rows",) if k in manifest}})

    print(f"\n[run_full_crawl] 完成。展平: {flat_path}  原始: {raw_path}  进度: {prog_path}  清单: {manifest_path}")


if __name__ == "__main__":
    run()
