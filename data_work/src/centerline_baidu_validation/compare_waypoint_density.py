"""
A/B test on a fixed sample: waypoint spacing (120m vs 60m) vs OD-only.

Reuses prior 120m results if available in qc_sample_detail.csv to save API calls.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import LineString

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.centerline_baidu_validation.baidu_client import extract_route, fetch_direction
from src.centerline_baidu_validation.build_crawl_manifest import build_route_points, load_baseline_centerlines
from src.national_tidal_commuting.common import ensure_version_dirs, get_version_root, load_baidu_ak, load_config, resolve_path

SH_TZ = ZoneInfo("Asia/Shanghai")
QC_LO, QC_HI = 0.6, 1.8


def parse_args():
    p = argparse.ArgumentParser(description="Compare waypoint spacing vs OD-only on fixed sample.")
    p.add_argument("--config", required=True)
    p.add_argument("--sample-manifest", required=True, help="CSV with fixed request_id sample.")
    p.add_argument("--prior-detail-csv", default=None, help="Optional prior qc_sample_detail with 120m wp results.")
    p.add_argument("--spacing-a", type=float, default=120.0)
    p.add_argument("--spacing-b", type=float, default=60.0)
    p.add_argument("--max-waypoints", type=int, default=10)
    p.add_argument("--panel", default="FREE", choices=["FREE", "AM", "PM"])
    p.add_argument("--sleep-sec", type=float, default=0.25)
    p.add_argument("--skip-od", action="store_true")
    p.add_argument("--skip-spacing-b", action="store_true")
    return p.parse_args()


def qc_share(ratio: pd.Series) -> float:
    r = pd.to_numeric(ratio, errors="coerce")
    return float(r.between(QC_LO, QC_HI).mean()) if len(r) else float("nan")


def main():
    args = parse_args()
    config = load_config(args.config)
    version_root = get_version_root(config)
    dirs = ensure_version_dirs(version_root)
    crawl = config["crawl"]
    ak = load_baidu_ak().token
    tactics = int(crawl.get("tactics_by_panel", {}).get(args.panel, 0))
    api_url = str(crawl.get("api_url", "https://api.map.baidu.com/directionlite/v1/driving"))
    coord_type = str(crawl.get("coord_type", "wgs84"))
    ret_coordtype = crawl.get("ret_coordtype")

    sample = pd.read_csv(args.sample_manifest)
    keys = ["skel_dir", "cline_id", "dir"]
    baseline_root = resolve_path(config.get("output_dir", "outputs")) / config["baseline_network_version_id"]
    cl = load_baseline_centerlines(baseline_root)
    cl_key = cl.set_index(keys)

    rows = []
    prior = None
    if args.prior_detail_csv:
        prior_path = Path(args.prior_detail_csv)
        if not prior_path.is_absolute():
            prior_path = version_root / prior_path
        if prior_path.exists():
            prior = pd.read_csv(prior_path)
            prior["distance_over_length"] = prior["distance_m"] / prior["length_m"]

    stamp = datetime.now(SH_TZ).strftime("%Y%m%dT%H%M%S")
    out_dir = dirs["metrics"] / f"waypoint_density_ab_{stamp}"
    out_dir.mkdir(parents=True, exist_ok=True)

    n = len(sample)
    for i, row in enumerate(sample.to_dict("records"), 1):
        key = (int(row["skel_dir"]), int(row["cline_id"]), str(row["dir"]))
        if key not in cl_key.index:
            print(f"[{i}/{n}] skip missing geometry {row['request_id']}")
            continue
        geom_row = cl_key.loc[key]
        if isinstance(geom_row, pd.DataFrame):
            geom_row = geom_row.iloc[0]
        line = geom_row.geometry
        if not isinstance(line, LineString):
            parts = [g for g in getattr(line, "geoms", []) if isinstance(g, LineString)]
            line = max(parts, key=lambda g: g.length) if parts else line
        length_m = float(row["length_m"])
        gps = int(row["gps_direction"]) if pd.notna(row.get("gps_direction")) else None
        origin_text = str(row["origin_text"])
        destination_text = str(row["destination_text"])

        def query_mode(waypoints_text: str, mode: str) -> dict:
            payload, _ = fetch_direction(
                api_url=api_url,
                ak=ak,
                origin_text=origin_text,
                destination_text=destination_text,
                waypoints_text=waypoints_text,
                tactics=tactics,
                coord_type=coord_type,
                ret_coordtype=str(ret_coordtype) if ret_coordtype else None,
                gps_direction=gps,
            )
            out = extract_route(payload)
            dist = out.get("distance_m")
            ratio = dist / length_m if dist and length_m > 0 else np.nan
            return {
                "request_id": row["request_id"],
                "length_m": length_m,
                "mode": mode,
                "n_waypoints": 0 if not waypoints_text else len(waypoints_text.split("|")),
                "api_status": out["api_status"],
                "distance_m": dist,
                "duration_sec": out.get("duration_sec"),
                "distance_over_length": ratio,
                "route_bind_ok": bool(dist and QC_LO <= ratio <= QC_HI),
            }

        # 120m from prior crawl or recompute text from sample
        if prior is not None and row["request_id"] in set(prior["request_id"]):
            prow = prior[prior["request_id"] == row["request_id"]].iloc[0]
            rec120 = {
                "request_id": row["request_id"],
                "length_m": length_m,
                "mode": f"wp_{int(args.spacing_a)}m",
                "n_waypoints": int(row.get("n_waypoints", 0)),
                "api_status": int(prow["api_status"]),
                "distance_m": float(prow["distance_m"]),
                "duration_sec": float(prow["duration_sec"]) if pd.notna(prow.get("duration_sec")) else None,
                "distance_over_length": float(prow["distance_over_length"]),
                "route_bind_ok": bool(QC_LO <= float(prow["distance_over_length"]) <= QC_HI),
            }
            rows.append(rec120)
        else:
            wp120 = str(row.get("waypoints_latlon") or "").strip()
            rec = query_mode(wp120, f"wp_{int(args.spacing_a)}m")
            rows.append(rec)
            print(f"[{i}/{n}] {row['request_id']} wp120 ratio={rec['distance_over_length']:.3f}")
            time.sleep(args.sleep_sec)

        if not args.skip_spacing_b:
            _, _, wps60 = build_route_points(
                line,
                length_m=length_m,
                max_waypoints=args.max_waypoints,
                min_waypoint_spacing_m=float(args.spacing_b),
            )
            wp60_text = "|".join(f"{lat:.6f},{lon:.6f}" for lat, lon in wps60)
            rec60 = query_mode(wp60_text, f"wp_{int(args.spacing_b)}m")
            rows.append(rec60)
            print(f"[{i}/{n}] {row['request_id']} wp60 n={rec60['n_waypoints']} ratio={rec60['distance_over_length']:.3f}")
            time.sleep(args.sleep_sec)

        if not args.skip_od:
            rec_od = query_mode("", "od_only")
            rows.append(rec_od)
            print(f"[{i}/{n}] {row['request_id']} od ratio={rec_od['distance_over_length']:.3f}")
            time.sleep(args.sleep_sec)

    df = pd.DataFrame(rows)
    df.to_csv(out_dir / "waypoint_density_detail.csv", index=False)

    summary_rows = []
    for mode, g in df.groupby("mode"):
        ok = g["api_status"].eq(0)
        summary_rows.append(
            {
                "mode": mode,
                "n": int(len(g)),
                "api_ok_rate": float(ok.mean()),
                "median_ratio": float(g.loc[ok, "distance_over_length"].median()),
                "share_qc_loose": qc_share(g.loc[ok, "distance_over_length"]),
                "median_n_waypoints": float(g["n_waypoints"].median()),
            }
        )
    summary = pd.DataFrame(summary_rows).sort_values("mode")
    summary.to_csv(out_dir / "waypoint_density_summary.csv", index=False)

    # head-to-head: which mode wins per request_id
    wide = df.pivot_table(
        index="request_id",
        columns="mode",
        values="distance_over_length",
        aggfunc="first",
    )
    wide.to_csv(out_dir / "waypoint_density_wide.csv")

    lines = [
        "# Waypoint density A/B (fixed sample n={})".format(n),
        "",
        f"- Generated: {datetime.now(SH_TZ).isoformat(timespec='seconds')}",
        f"- QC loose: [{QC_LO}, {QC_HI}]",
        "",
        "## Summary",
        "",
        summary.to_csv(index=False),
        "",
    ]

    if f"wp_{int(args.spacing_a)}m" in wide.columns and f"wp_{int(args.spacing_b)}m" in wide.columns:
        a_col = f"wp_{int(args.spacing_a)}m"
        b_col = f"wp_{int(args.spacing_b)}m"
        closer_a = (wide[a_col] - 1.0).abs() < (wide[b_col] - 1.0).abs()
        lines.append(f"- {b_col} closer to 1 than {a_col}: {closer_a.mean():.1%} of rows")

    if "od_only" in wide.columns and f"wp_{int(args.spacing_a)}m" in wide.columns:
        a_col = f"wp_{int(args.spacing_a)}m"
        od_closer_len = (wide["od_only"] - 1.0).abs() < (wide[a_col] - 1.0).abs()
        od_qc = wide["od_only"].between(QC_LO, QC_HI)
        wp_qc = wide[a_col].between(QC_LO, QC_HI)
        lines.extend(
            [
                "",
                "## OD-only vs waypoints (120m)",
                "",
                f"- OD distance closer to length_m: {od_closer_len.mean():.1%}",
                f"- OD loose QC pass: {od_qc.mean():.1%}",
                f"- WP 120m loose QC pass: {wp_qc.mean():.1%}",
                f"- OD wins QC but WP fails: {(od_qc & ~wp_qc).sum()} rows",
                f"- WP wins QC but OD fails: {(wp_qc & ~od_qc).sum()} rows",
            ]
        )

    (out_dir / "WAYPOINT_DENSITY_REPORT.md").write_text("\n".join(lines), encoding="utf-8")
    print(f"[compare_waypoint_density] wrote {out_dir / 'WAYPOINT_DENSITY_REPORT.md'}")
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
