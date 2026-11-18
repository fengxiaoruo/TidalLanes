"""
Overlay two road-network versions around the city center with 5 km / 10 km reference rings.

Default layers:
  - baseline directed centerline (keep_baseline)
  - Baidu API route geometry from a crawl session (GCJ→WGS corrected)
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
from shapely import wkt
from shapely.geometry import LineString, Point

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.centerline_baidu_validation.build_crawl_manifest import load_baseline_centerlines
from src.baidu_validation_common.spatial import line_gcj_to_wgs
from src.diagnostics.mpl_setup import ensure_mpl_cache
from src.national_tidal_commuting.common import ensure_version_dirs, get_version_root, load_config, resolve_path

SH_TZ = ZoneInfo("Asia/Shanghai")
TIANANMEN_LONLAT = (116.397389, 39.908722)
TARGET_CRS = "EPSG:32650"
QC_LO, QC_HI = 0.6, 1.8

CENTERLINE_COLOR = "#2171b5"
BAIDU_COLOR = "#e6550d"
RING_COLORS = {5000: "#636363", 10000: "#969696"}


def parse_args():
    p = argparse.ArgumentParser(description="Overlay centerline vs Baidu routes around city center.")
    p.add_argument("--config", default="config/centerline_baidu_validation_beijing_v1.json")
    p.add_argument("--label", default="formal_week_20260527_d01", help="Crawl session label suffix.")
    p.add_argument("--panels", default="FREE,AM,PM", help="Comma-separated panels to include for Baidu routes.")
    p.add_argument("--center-lon", type=float, default=TIANANMEN_LONLAT[0])
    p.add_argument("--center-lat", type=float, default=TIANANMEN_LONLAT[1])
    p.add_argument("--radii-m", default="5000,10000", help="Reference ring radii in meters.")
    p.add_argument("--plot-radius-m", type=float, default=10000.0, help="Map extent radius.")
    p.add_argument("--bind-ok-only", action="store_true", help="Only plot Baidu routes passing bind QC.")
    p.add_argument("--tag", default=None, help="Optional output tag.")
    return p.parse_args()


def roi_circles(center_lon: float, center_lat: float, radii_m: list[float]) -> tuple[gpd.GeoDataFrame, gpd.GeoSeries]:
    center_wgs = gpd.GeoSeries([Point(center_lon, center_lat)], crs="EPSG:4326")
    center_utm = center_wgs.to_crs(TARGET_CRS).iloc[0]
    rings = []
    for r in radii_m:
        rings.append({"radius_m": r, "geometry": center_utm.buffer(float(r))})
    ring_gdf = gpd.GeoDataFrame(rings, crs=TARGET_CRS)
    return ring_gdf, center_utm


def clip_lines_to_roi(lines: gpd.GeoDataFrame, roi: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if lines.empty:
        return lines
    out = gpd.clip(lines, roi)
    out = out[~out.geometry.is_empty].copy()
    return out


def load_centerlines_in_roi(baseline_root: Path, roi_wgs: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    cl = load_baseline_centerlines(baseline_root)
    cl_utm = cl.to_crs(TARGET_CRS)
    roi_utm = roi_wgs.to_crs(TARGET_CRS)
    clipped = clip_lines_to_roi(cl_utm, roi_utm)
    return clipped


def find_sessions(version_root: Path, label: str, panels: list[str]) -> dict[str, Path]:
    out: dict[str, Path] = {}
    for panel in panels:
        matches = sorted((version_root / "api" / "crawl_sessions").glob(f"{panel}_*_{label}"))
        if not matches:
            raise FileNotFoundError(f"No session for panel={panel}, label={label}")
        out[panel] = matches[-1]
    return out


def load_baidu_routes_in_roi(
    version_root: Path,
    *,
    label: str,
    panels: list[str],
    roi_wgs: gpd.GeoDataFrame,
    bind_ok_only: bool,
) -> gpd.GeoDataFrame:
    sessions = find_sessions(version_root, label, panels)
    roi_utm = roi_wgs.to_crs(TARGET_CRS)
    rows: list[dict] = []
    for panel, session_dir in sessions.items():
        flat = pd.read_csv(session_dir / "results_flat.csv")
        flat = flat[flat["api_status"].eq(0) & flat.get("has_route_path", False).fillna(False)].copy()
        if bind_ok_only:
            ratio = flat["distance_m"] / flat["length_m"]
            flat = flat[ratio.between(QC_LO, QC_HI)].copy()
        for rec in flat.to_dict("records"):
            wkt_text = rec.get("route_path_wkt")
            if not isinstance(wkt_text, str) or not wkt_text.strip():
                continue
            try:
                route_gcj = wkt.loads(wkt_text)
            except Exception:
                continue
            if not isinstance(route_gcj, LineString) or route_gcj.is_empty:
                continue
            route_wgs = line_gcj_to_wgs(route_gcj)
            geom_utm = gpd.GeoSeries([route_wgs], crs="EPSG:4326").to_crs(TARGET_CRS).iloc[0]
            if not geom_utm.intersects(roi_utm.geometry.iloc[0]):
                continue
            rows.append({"panel": panel, "request_id": rec["request_id"], "geometry": geom_utm})
    if not rows:
        return gpd.GeoDataFrame(columns=["panel", "request_id", "geometry"], crs=TARGET_CRS)
    return gpd.GeoDataFrame(rows, crs=TARGET_CRS)


def plot_overlay(
    path: Path,
    *,
    centerline: gpd.GeoDataFrame,
    baidu: gpd.GeoDataFrame,
    rings: gpd.GeoDataFrame,
    center_utm: Point,
    plot_radius_m: float,
    center_lonlat: tuple[float, float],
    label: str,
    bind_ok_only: bool,
):
    cx, cy = center_utm.x, center_utm.y
    pad = plot_radius_m * 0.04
    xlim = (cx - plot_radius_m - pad, cx + plot_radius_m + pad)
    ylim = (cy - plot_radius_m - pad, cy + plot_radius_m + pad)

    fig, ax = plt.subplots(figsize=(9.5, 9.5), dpi=260)
    for _, ring in rings.sort_values("radius_m").iterrows():
        r = float(ring["radius_m"])
        gpd.GeoSeries([ring.geometry], crs=TARGET_CRS).boundary.plot(
            ax=ax,
            color=RING_COLORS.get(int(r), "#969696"),
            linewidth=1.4,
            linestyle="--",
            alpha=0.9,
            zorder=1,
        )
        ax.text(
            cx + r * 0.70,
            cy + r * 0.70,
            f"{int(r / 1000)} km",
            fontsize=9,
            color=RING_COLORS.get(int(r), "#969696"),
            ha="center",
            va="center",
            zorder=6,
        )

    if len(centerline):
        centerline.plot(ax=ax, color=CENTERLINE_COLOR, linewidth=0.55, alpha=0.95, zorder=3, label="Baseline centerline")
    if len(baidu):
        baidu.plot(ax=ax, color=BAIDU_COLOR, linewidth=0.45, alpha=0.55, zorder=2, label="Baidu API routes")

    ax.scatter([cx], [cy], s=36, c="#111827", marker="*", zorder=7, label="City center")
    ax.set_xlim(*xlim)
    ax.set_ylim(*ylim)
    ax.set_aspect("equal", adjustable="box")
    ax.set_axis_off()

    baidu_note = "bind_ok only" if bind_ok_only else "all api_ok routes"
    title = (
        f"Road network overlay — Tiananmen center ({center_lonlat[0]:.3f}E, {center_lonlat[1]:.3f}N)\n"
        f"Baidu crawl `{label}` ({baidu_note})"
    )
    ax.set_title(title, fontsize=12, pad=10)
    ax.legend(loc="upper right", frameon=True, fontsize=9)
    fig.text(
        0.5,
        0.02,
        f"Centerline n={len(centerline):,}; Baidu routes n={len(baidu):,}. "
        f"Dashed rings: 5 km / 10 km. Baidu geometry GCJ→WGS corrected.",
        ha="center",
        fontsize=9,
        color="#4b5563",
    )
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def main():
    args = parse_args()
    config = load_config(args.config)
    version_root = get_version_root(config)
    dirs = ensure_version_dirs(version_root)
    ensure_mpl_cache(str(version_root.parent))

    baseline_root = resolve_path(config.get("output_dir", "outputs")) / config["baseline_network_version_id"]
    radii_m = [float(x.strip()) for x in args.radii_m.split(",") if x.strip()]
    panels = [x.strip() for x in args.panels.split(",") if x.strip()]
    center_lonlat = (args.center_lon, args.center_lat)

    rings, center_utm = roi_circles(center_lonlat[0], center_lonlat[1], radii_m)
    plot_r = float(args.plot_radius_m)
    roi_wgs = gpd.GeoDataFrame(
        geometry=gpd.GeoSeries([Point(center_lonlat)], crs="EPSG:4326").to_crs(TARGET_CRS).buffer(plot_r),
        crs=TARGET_CRS,
    ).to_crs("EPSG:4326")

    centerline = load_centerlines_in_roi(baseline_root, roi_wgs)
    baidu = load_baidu_routes_in_roi(
        version_root,
        label=args.label,
        panels=panels,
        roi_wgs=roi_wgs,
        bind_ok_only=args.bind_ok_only,
    )

    stamp = datetime.now(SH_TZ).strftime("%Y%m%dT%H%M%S")
    tag = args.tag or f"network_overlay_{args.label}_{stamp}"
    fig_dir = dirs["figures"] / tag
    fig_dir.mkdir(parents=True, exist_ok=True)
    out_path = fig_dir / "fig_network_overlay_centerline_vs_baidu_5km_10km.png"

    plot_overlay(
        out_path,
        centerline=centerline,
        baidu=baidu,
        rings=rings,
        center_utm=center_utm,
        plot_radius_m=plot_r,
        center_lonlat=center_lonlat,
        label=args.label,
        bind_ok_only=args.bind_ok_only,
    )

    summary = pd.DataFrame(
        [
            {
                "center_lon": center_lonlat[0],
                "center_lat": center_lonlat[1],
                "plot_radius_m": plot_r,
                "n_centerline": len(centerline),
                "n_baidu_routes": len(baidu),
                "label": args.label,
                "panels": ",".join(panels),
                "bind_ok_only": args.bind_ok_only,
            }
        ]
    )
    summary.to_csv(fig_dir / "network_overlay_summary.csv", index=False)
    print(f"[plot_network_overlay_map] centerline={len(centerline):,} baidu={len(baidu):,}")
    print(f"[plot_network_overlay_map] wrote {out_path}")


if __name__ == "__main__":
    main()
