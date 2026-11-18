"""
Compare how closely Baidu route geometries follow baseline centerlines,
for OD-only vs waypoint-constrained routing (and optional cached crawl sessions).
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

import geopandas as gpd
import numpy as np
import pandas as pd
import pyproj
from shapely.geometry import LineString
from shapely.ops import transform

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.centerline_baidu_validation.baidu_client import (
    extract_route,
    extract_route_path,
    fetch_direction,
    merge_path_coords,
    parse_baidu_path_text,
)
from src.centerline_baidu_validation.build_crawl_manifest import load_baseline_centerlines
from src.national_tidal_commuting.common import ensure_version_dirs, get_version_root, load_baidu_ak, load_config, resolve_path

SH_TZ = ZoneInfo("Asia/Shanghai")
QC_LO, QC_HI = 0.6, 1.8

_A = 6378245.0
_EE = 0.00669342162296594323


def gcj02_to_wgs84(lon, lat):
    if not (72.004 <= lon <= 137.8347 and 0.8293 <= lat <= 55.8271):
        return lon, lat
    dlat = _transform_lat(lon - 105, lat - 35)
    dlon = _transform_lon(lon - 105, lat - 35)
    radlat = lat / 180 * math.pi
    magic = 1 - _EE * math.sin(radlat) ** 2
    sqrtmagic = math.sqrt(magic)
    dlat = (dlat * 180) / ((_A * (1 - _EE)) / (magic * sqrtmagic) * math.pi)
    dlon = (dlon * 180) / (_A / sqrtmagic * math.cos(radlat) * math.pi)
    return lon - dlon, lat - dlat


def _transform_lat(lon, lat):
    ret = -100 + 2 * lon + 3 * lat + 0.2 * lat * lat + 0.1 * lon * lat + 0.2 * math.sqrt(abs(lon))
    ret += (20 * math.sin(6 * lon * math.pi) + 20 * math.sin(2 * lon * math.pi)) * 2 / 3
    ret += (20 * math.sin(lat * math.pi) + 40 * math.sin(lat / 3 * math.pi)) * 2 / 3
    ret += (160 * math.sin(lat / 12 * math.pi) + 320 * math.sin(lat * math.pi / 30)) * 2 / 3
    return ret


def _transform_lon(lon, lat):
    ret = 300 + lon + 2 * lat + 0.1 * lon * lon + 0.1 * lon * lat + 0.1 * math.sqrt(abs(lon))
    ret += (20 * math.sin(6 * lon * math.pi) + 20 * math.sin(2 * lon * math.pi)) * 2 / 3
    ret += (20 * math.sin(lon * math.pi) + 40 * math.sin(lon / 3 * math.pi)) * 2 / 3
    ret += (150 * math.sin(lon / 12 * math.pi) + 300 * math.sin(lon / math.pi / 30)) * 2 / 3
    return ret


def line_gcj_to_wgs(line: LineString) -> LineString:
    if line is None or line.is_empty:
        return line
    return LineString([gcj02_to_wgs84(x, y) for x, y in line.coords])


def parse_args():
    p = argparse.ArgumentParser(description="Spatial fit: Baidu route vs baseline centerline.")
    p.add_argument("--config", required=True)
    p.add_argument("--sample-manifest", required=True)
    p.add_argument("--panel", default="FREE", choices=["FREE", "AM", "PM"])
    p.add_argument(
        "--waypoint-session",
        default=None,
        help="Existing crawl session with steps_info routes (e.g. qc_sample150_route_geom).",
    )
    p.add_argument("--query-od", action="store_true", help="Live-query OD-only routes with steps_info=1.")
    p.add_argument("--sleep-sec", type=float, default=0.25)
    p.add_argument("--buffer-m", type=float, default=50.0)
    p.add_argument("--limit", type=int, default=None)
    return p.parse_args()


def route_from_response(resp: dict) -> LineString | None:
    result = resp.get("result", {}) if isinstance(resp.get("result"), dict) else {}
    routes = result.get("routes", []) if isinstance(result.get("routes", []), list) else []
    route0 = routes[0] if routes else {}
    step_paths = []
    if isinstance(route0, dict) and isinstance(route0.get("steps"), list):
        for step in route0["steps"]:
            if isinstance(step, dict):
                step_paths.append(parse_baidu_path_text(str(step.get("path") or "")))
    coords = merge_path_coords(step_paths)
    if len(coords) < 2:
        return None
    return LineString(coords)


def spatial_metrics(centerline_wgs: LineString, route_gcj: LineString, *, buffer_m: float, to_utm) -> dict:
    route_wgs = line_gcj_to_wgs(route_gcj)
    clu = to_utm(centerline_wgs)
    rtu = to_utm(route_wgs)
    if clu.length <= 0 or rtu.length <= 0:
        return {
            "route_length_m": float(rtu.length),
            "length_ratio_route_over_cl": np.nan,
            "mean_dist_cl_to_route_m": np.nan,
            "mean_dist_route_to_cl_m": np.nan,
            "hausdorff_m": np.nan,
            "overlap_cl_in_route_buffer": np.nan,
            "overlap_route_in_cl_buffer": np.nan,
            "sym_overlap": np.nan,
        }
    cl_pts = [clu.interpolate(i / 20, normalized=True) for i in range(21)]
    rt_pts = [rtu.interpolate(i / 20, normalized=True) for i in range(21)]
    mean_cl_to_rt = float(np.mean([rtu.distance(p) for p in cl_pts]))
    mean_rt_to_cl = float(np.mean([clu.distance(p) for p in rt_pts]))
    buf = float(buffer_m)
    overlap_cl = clu.intersection(rtu.buffer(buf)).length / clu.length
    overlap_rt = rtu.intersection(clu.buffer(buf)).length / rtu.length
    return {
        "route_length_m": float(rtu.length),
        "length_ratio_route_over_cl": float(rtu.length / clu.length),
        "mean_dist_cl_to_route_m": mean_cl_to_rt,
        "mean_dist_route_to_cl_m": mean_rt_to_cl,
        "hausdorff_m": float(clu.hausdorff_distance(rtu)),
        "overlap_cl_in_route_buffer": float(overlap_cl),
        "overlap_route_in_cl_buffer": float(overlap_rt),
        "sym_overlap": float(min(overlap_cl, overlap_rt)),
    }


def load_session_routes(session_dir: Path) -> dict[str, dict]:
    out: dict[str, dict] = {}
    jsonl = session_dir / "responses.jsonl"
    with jsonl.open(encoding="utf-8") as fh:
        for line in fh:
            rec = json.loads(line)
            rid = rec.get("request_id")
            resp = rec.get("response") or {}
            route = extract_route(resp)
            path = extract_route_path(resp)
            geom = route_from_response(resp)
            out[str(rid)] = {
                "response": resp,
                "api_status": route["api_status"],
                "distance_m": route["distance_m"],
                "duration_sec": route["duration_sec"],
                "has_route_path": path["has_route_path"],
                "route_gcj": geom,
            }
    return out


def query_od_route(row: dict, *, ak: str, crawl_cfg: dict, tactics: int) -> dict:
    payload, _ = fetch_direction(
        api_url=str(crawl_cfg.get("api_url", "https://api.map.baidu.com/directionlite/v1/driving")),
        ak=ak,
        origin_text=str(row["origin_text"]),
        destination_text=str(row["destination_text"]),
        waypoints_text="",
        tactics=tactics,
        coord_type=str(crawl_cfg.get("coord_type", "wgs84")),
        ret_coordtype=str(crawl_cfg.get("ret_coordtype")) if crawl_cfg.get("ret_coordtype") else None,
        gps_direction=int(row["gps_direction"]) if pd.notna(row.get("gps_direction")) else None,
        steps_info=1,
    )
    route = extract_route(payload)
    geom = route_from_response(payload)
    return {
        "response": payload,
        "api_status": route["api_status"],
        "distance_m": route["distance_m"],
        "duration_sec": route["duration_sec"],
        "has_route_path": geom is not None,
        "route_gcj": geom,
    }


def main():
    args = parse_args()
    config = load_config(args.config)
    version_root = get_version_root(config)
    dirs = ensure_version_dirs(version_root)
    crawl_cfg = config.get("crawl", {})
    tactics = int(crawl_cfg.get("tactics_by_panel", {}).get(args.panel, 0))

    sample_path = Path(args.sample_manifest)
    if not sample_path.is_absolute():
        sample_path = version_root / sample_path
    sample = pd.read_csv(sample_path)
    if args.limit:
        sample = sample.head(int(args.limit)).copy()

    baseline_root = resolve_path(config.get("output_dir", "outputs")) / config["baseline_network_version_id"]
    cl = load_baseline_centerlines(baseline_root).set_index(["skel_dir", "cline_id", "dir"])
    proj = pyproj.Transformer.from_crs("EPSG:4326", "EPSG:32650", always_xy=True)
    to_utm = lambda g: transform(proj.transform, g)

    wp_routes: dict[str, dict] = {}
    if args.waypoint_session:
        session_dir = Path(args.waypoint_session)
        if not session_dir.is_absolute():
            session_dir = version_root / "api" / "crawl_sessions" / session_dir
        wp_routes = load_session_routes(session_dir)

    ak = load_baidu_ak().token if args.query_od else None
    od_cache_path = dirs["metrics"] / "od_route_geom_cache.jsonl"
    od_cache: dict[str, dict] = {}
    if od_cache_path.exists():
        with od_cache_path.open(encoding="utf-8") as fh:
            for line in fh:
                rec = json.loads(line)
                od_cache[str(rec["request_id"])] = rec

    rows = []
    n = len(sample)
    for i, row in enumerate(sample.to_dict("records"), 1):
        rid = str(row["request_id"])
        key = (int(row["skel_dir"]), int(row["cline_id"]), str(row["dir"]))
        if key not in cl.index:
            continue
        centerline = cl.loc[key].geometry
        if not isinstance(centerline, LineString):
            parts = [g for g in getattr(centerline, "geoms", []) if isinstance(g, LineString)]
            centerline = max(parts, key=lambda g: g.length) if parts else centerline
        length_m = float(row["length_m"])

        variants: list[tuple[str, dict | None]] = []
        if rid in wp_routes:
            variants.append(("wp_120m", wp_routes[rid]))
        if args.query_od:
            if rid in od_cache and od_cache[rid].get("route_coords_lonlat"):
                coords = od_cache[rid]["route_coords_lonlat"]
                geom = LineString(coords) if len(coords) >= 2 else None
                variants.append(
                    (
                        "od_only",
                        {
                            "api_status": od_cache[rid].get("api_status", 0),
                            "distance_m": od_cache[rid].get("distance_m"),
                            "duration_sec": od_cache[rid].get("duration_sec"),
                            "route_gcj": geom,
                            "has_route_path": geom is not None,
                        },
                    )
                )
            else:
                od = query_od_route(row, ak=ak, crawl_cfg=crawl_cfg, tactics=tactics)
                coords = list(od["route_gcj"].coords) if od["route_gcj"] is not None else []
                with od_cache_path.open("a", encoding="utf-8") as fh:
                    fh.write(
                        json.dumps(
                            {
                                "request_id": rid,
                                "api_status": od["api_status"],
                                "distance_m": od["distance_m"],
                                "duration_sec": od["duration_sec"],
                                "route_coords_lonlat": coords,
                            },
                            ensure_ascii=False,
                        )
                        + "\n"
                    )
                variants.append(("od_only", od))
                print(f"[{i}/{n}] queried od_only {rid} dist={od['distance_m']}")
                time.sleep(args.sleep_sec)

        for mode, payload in variants:
            if payload is None or payload.get("route_gcj") is None:
                continue
            sm = spatial_metrics(centerline, payload["route_gcj"], buffer_m=args.buffer_m, to_utm=to_utm)
            dist = payload.get("distance_m")
            ratio = float(dist) / length_m if dist and length_m > 0 else np.nan
            rows.append(
                {
                    "request_id": rid,
                    "mode": mode,
                    "length_m": length_m,
                    "api_status": payload.get("api_status"),
                    "distance_m": dist,
                    "duration_sec": payload.get("duration_sec"),
                    "distance_over_length": ratio,
                    "route_bind_ok": bool(dist and QC_LO <= ratio <= QC_HI),
                    **sm,
                }
            )

    df = pd.DataFrame(rows)
    stamp = datetime.now(SH_TZ).strftime("%Y%m%dT%H%M%S")
    out_dir = dirs["metrics"] / f"route_spatial_fit_{stamp}"
    out_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_dir / "route_spatial_fit_detail.csv", index=False)

    summary_rows = []
    for mode, g in df.groupby("mode"):
        ok = g["api_status"].eq(0)
        sub = g[ok]
        summary_rows.append(
            {
                "mode": mode,
                "n": int(len(sub)),
                "median_sym_overlap": float(sub["sym_overlap"].median()),
                "median_overlap_cl_in_route_buffer": float(sub["overlap_cl_in_route_buffer"].median()),
                "median_mean_dist_cl_to_route_m": float(sub["mean_dist_cl_to_route_m"].median()),
                "median_hausdorff_m": float(sub["hausdorff_m"].median()),
                "median_length_ratio": float(sub["length_ratio_route_over_cl"].median()),
                "median_distance_over_length": float(sub["distance_over_length"].median()),
                "share_route_bind_ok": float(sub["route_bind_ok"].mean()),
                "share_sym_overlap_ge_0p8": float((sub["sym_overlap"] >= 0.8).mean()),
                "share_mean_dist_le_20m": float((sub["mean_dist_cl_to_route_m"] <= 20).mean()),
            }
        )
    summary = pd.DataFrame(summary_rows).sort_values("mode")
    summary.to_csv(out_dir / "route_spatial_fit_summary.csv", index=False)

    wide = df.pivot_table(index="request_id", columns="mode", values="sym_overlap", aggfunc="first")
    winners = []
    if "od_only" in wide.columns and "wp_120m" in wide.columns:
        both = wide.dropna(subset=["od_only", "wp_120m"])
        od_wins = (both["od_only"] > both["wp_120m"]).sum()
        wp_wins = (both["wp_120m"] > both["od_only"]).sum()
        tie = len(both) - od_wins - wp_wins
        winners = [("sym_overlap od wins", int(od_wins)), ("sym_overlap wp wins", int(wp_wins)), ("tie", int(tie))]

    lines = [
        "# Route spatial fit vs baseline centerline",
        "",
        f"- Generated: {datetime.now(SH_TZ).isoformat(timespec='seconds')}",
        f"- Sample n={n}, buffer={args.buffer_m}m, GCJ→WGS correction on API geometry",
        "",
        "## Summary",
        "",
        summary.to_csv(index=False),
        "",
    ]
    if winners:
        lines.extend(["## Head-to-head (sym_overlap)", ""])
        for label, cnt in winners:
            lines.append(f"- {label}: {cnt} / {len(both)}")
        lines.append("")

    metric_cols = [
        "sym_overlap",
        "mean_dist_cl_to_route_m",
        "hausdorff_m",
        "length_ratio_route_over_cl",
        "distance_over_length",
    ]
    if "od_only" in df["mode"].values and "wp_120m" in df["mode"].values:
        lines.append("## Per-request winners")
        lines.append("")
        for metric in metric_cols:
            w = (
                df.pivot_table(index="request_id", columns="mode", values=metric, aggfunc="first")
                .dropna(subset=["od_only", "wp_120m"])
            )
            if metric in {"mean_dist_cl_to_route_m", "hausdorff_m", "distance_over_length"}:
                od_better = (w["od_only"] < w["wp_120m"]).sum()
                wp_better = (w["wp_120m"] < w["od_only"]).sum()
            else:
                od_better = (w["od_only"] > w["wp_120m"]).sum()
                wp_better = (w["wp_120m"] > w["od_only"]).sum()
            lines.append(f"- `{metric}`: od better {od_better}, wp better {wp_better}, n={len(w)}")

    (out_dir / "ROUTE_SPATIAL_FIT_REPORT.md").write_text("\n".join(lines), encoding="utf-8")
    print(f"[compare_route_spatial_fit] wrote {out_dir / 'ROUTE_SPATIAL_FIT_REPORT.md'}")
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
