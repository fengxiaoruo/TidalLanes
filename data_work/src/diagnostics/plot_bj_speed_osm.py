"""Plot Beijing raw segment speeds on an OSM/CARTO basemap for PreSlides speed_eg.png."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import contextily as cx
import geopandas as gpd
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.collections import LineCollection
from matplotlib.patches import Patch
from shapely.geometry import Point, Polygon

ROOT = Path(__file__).resolve().parents[2]
REPO = ROOT.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.diagnostics.mpl_setup import ensure_mpl_cache

RAW_SPEED_PATH = ROOT / "raw_data" / "speed_Beijing_all_wgs84.csv"
DISTRICT_JSON = ROOT / "src" / "raw_connected_v3" / "Processed_Data" / "beijing_district_regions.json"
TIANANMEN = (116.397389, 39.908722)
WEB_MERCATOR = "EPSG:3857"
WGS84 = "EPSG:4326"
DEFAULT_HOUR = "202505190830"
DEFAULT_RADIUS_M = 28_000

SPEED_BINS = (
    (5.0, "#8B2500", "≤5 km/h  Severe congestion"),
    (20.0, "#E53935", "6–20 km/h  Congestion"),
    (30.0, "#FBC02D", "21–30 km/h  Slow"),
    (float("inf"), "#43A047", ">30 km/h  Smooth"),
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Plot Beijing speed map with OSM/CARTO basemap.")
    p.add_argument("--hour", default=DEFAULT_HOUR, help="Timestamp YYYYMMDDHHMM.")
    p.add_argument(
        "--scope",
        choices=("part", "full", "both"),
        default="both",
        help="part=28 km radius around Tiananmen; full=all Beijing segments; both=write both figures.",
    )
    p.add_argument("--radius-m", type=float, default=DEFAULT_RADIUS_M)
    p.add_argument("--snapshot-csv", type=Path, default=ROOT / "outputs" / "bj_am_0830.csv")
    p.add_argument(
        "--output-png",
        type=Path,
        default=REPO / "Documents" / "PreSlides" / "figs" / "speed_eg.png",
    )
    p.add_argument(
        "--output-png-full",
        type=Path,
        default=REPO / "Documents" / "PreSlides" / "figs" / "speed_eg_full.png",
    )
    p.add_argument(
        "--output-png-l3",
        type=Path,
        default=REPO / "Documents" / "L3_figs" / "speed_eg.png",
    )
    return p.parse_args()


def configure_mpl() -> None:
    ensure_mpl_cache(str(ROOT / ".mplcache"))
    mpl.rcParams.update(
        {
            "font.sans-serif": ["Arial", "Helvetica", "DejaVu Sans", "PingFang SC", "Noto Sans CJK SC"],
            "font.family": "sans-serif",
            "axes.unicode_minus": False,
            "figure.dpi": 180,
            "savefig.dpi": 600,
        }
    )


def speed_color(speed: float) -> str:
    for upper, color, _label in SPEED_BINS:
        if speed <= upper:
            return color
    return SPEED_BINS[-1][1]


def format_title(hour: str, scope: str) -> str:
    dt_label = f"{hour[0:4]}-{hour[4:6]}-{hour[6:8]} {hour[8:10]}:{hour[10:12]}"
    city_label = "Beijing (part)" if scope == "part" else "Beijing"
    return f"{city_label} Road Traffic Speed ({dt_label})"


def load_speed_snapshot(hour: str, radius_m: float | None) -> gpd.GeoDataFrame:
    rows: list[pd.DataFrame] = []
    for chunk in pd.read_csv(
        RAW_SPEED_PATH,
        chunksize=500_000,
        usecols=["hour", "speed", "roadseg_id", "geometry", "roadname"],
    ):
        chunk["hour_str"] = pd.to_numeric(chunk["hour"], errors="coerce").astype("Int64").astype(str)
        sub = chunk[chunk["hour_str"] == hour].copy()
        if len(sub):
            rows.append(sub)
    if not rows:
        raise RuntimeError(f"No speed rows found for hour={hour}")

    df = pd.concat(rows, ignore_index=True)
    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.GeoSeries.from_wkt(df["geometry"], crs=WGS84),
        crs=WGS84,
    )
    gdf["speed"] = pd.to_numeric(gdf["speed"], errors="coerce")
    gdf = gdf[gdf["speed"].gt(0)].copy()

    if radius_m is not None:
        center = gpd.GeoSeries([Point(TIANANMEN)], crs=WGS84).to_crs("EPSG:32650").iloc[0]
        mid = gdf.geometry.to_crs("EPSG:32650").interpolate(0.5, normalized=True)
        gdf = gdf.loc[mid.distance(center) <= radius_m].copy()
    return gdf


def load_districts() -> gpd.GeoDataFrame:
    payload = json.loads(DISTRICT_JSON.read_text(encoding="utf-8"))
    records = []
    for region in payload["regions"]:
        paths = region.get("boundary_paths") or []
        polys = []
        for path in paths:
            if len(path) < 3:
                continue
            polys.append(Polygon([(lon, lat) for lat, lon in path]))
        if not polys:
            bbox = region["bbox"]
            polys = [
                Polygon(
                    [
                        (bbox["min_lon"], bbox["min_lat"]),
                        (bbox["max_lon"], bbox["min_lat"]),
                        (bbox["max_lon"], bbox["max_lat"]),
                        (bbox["min_lon"], bbox["max_lat"]),
                    ]
                )
            ]
        geom = max(polys, key=lambda p: p.area)
        records.append({"district": region["district"], "geometry": geom})
    return gpd.GeoDataFrame(records, crs=WGS84)


def add_basemap(ax: plt.Axes, bounds_3857: tuple[float, float, float, float]) -> bool:
    xmin, ymin, xmax, ymax = bounds_3857
    ax.set_xlim(xmin, xmax)
    ax.set_ylim(ymin, ymax)
    try:
        cx.add_basemap(
            ax,
            source=cx.providers.CartoDB.Positron,
            crs=WEB_MERCATOR,
            zoom="auto",
            attribution=False,
        )
        return True
    except Exception:
        roads = gpd.read_file(ROOT / "raw_data" / "gis" / "roads_baidu" / "beijing_roads.shp").to_crs(WEB_MERCATOR)
        clip = roads.cx[xmin:xmax, ymin:ymax]
        clip.plot(ax=ax, color="#d8d8d8", linewidth=0.25, alpha=0.85, zorder=0)
        return False


def add_speed_legend(ax: plt.Axes) -> None:
    handles = [Patch(facecolor=color, edgecolor="none", label=label) for _upper, color, label in SPEED_BINS]
    ax.legend(
        handles=handles,
        loc="lower right",
        title="Speed",
        frameon=True,
        framealpha=0.95,
        facecolor="white",
        edgecolor="#333333",
        fontsize=8.5,
        title_fontsize=9.5,
        handlelength=1.4,
        handleheight=1.0,
        borderpad=0.6,
        labelspacing=0.45,
    )


def plot_speed_map(
    gdf: gpd.GeoDataFrame,
    districts: gpd.GeoDataFrame,
    hour: str,
    out_png: Path,
    *,
    scope: str,
) -> None:
    roads = gdf.to_crs(WEB_MERCATOR)
    dist = districts.to_crs(WEB_MERCATOR)

    if scope == "full":
        bounds = dist.total_bounds
        pad = 3500.0
    else:
        bounds = roads.geometry.total_bounds
        pad = 1200.0
    view = (bounds[0] - pad, bounds[1] - pad, bounds[2] + pad, bounds[3] + pad)
    width = view[2] - view[0]
    height = view[3] - view[1]
    fig_w = 8.8
    fig_h = max(fig_w * height / width, 7.0)

    fig, ax = plt.subplots(figsize=(fig_w, fig_h))
    fig.subplots_adjust(top=0.93, bottom=0.03, left=0.02, right=0.98)
    has_web = add_basemap(ax, view)

    dist_clip = dist.cx[view[0] : view[2], view[1] : view[3]]
    dist_clip.plot(
        ax=ax,
        facecolor="#ffffff" if has_web else "#f2f2f2",
        edgecolor="none",
        alpha=0.42 if has_web else 0.75,
        zorder=1,
    )
    dist_clip.boundary.plot(ax=ax, color="#bdbdbd", linewidth=0.45, alpha=0.9, zorder=2)

    seg_coords = []
    seg_colors = []
    line_width = 1.0 if scope == "part" else 0.78
    for geom, speed in zip(roads.geometry, roads["speed"]):
        xs, ys = geom.xy
        seg_coords.append(np.column_stack([xs, ys]))
        seg_colors.append(speed_color(float(speed)))
    ax.add_collection(
        LineCollection(seg_coords, colors=seg_colors, linewidths=line_width, alpha=0.95, zorder=4)
    )

    add_speed_legend(ax)
    fig.suptitle(format_title(hour, scope), fontsize=14, fontweight="bold", y=0.985)
    ax.set_axis_off()
    ax.set_xlim(view[0], view[2])
    ax.set_ylim(view[1], view[3])

    out_png.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_png, bbox_inches="tight", facecolor="white", pad_inches=0.08)
    fig.savefig(out_png.with_suffix(".pdf"), bbox_inches="tight", facecolor="white", pad_inches=0.08)
    plt.close(fig)


def write_snapshot_csv(gdf: gpd.GeoDataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    export = gdf.drop(columns="geometry").copy()
    export["geometry_wkt"] = gdf.geometry.to_wkt()
    export.to_csv(path, index=False)


def main() -> None:
    args = parse_args()
    configure_mpl()
    districts = load_districts()
    scopes = ("part", "full") if args.scope == "both" else (args.scope,)

    for scope in scopes:
        radius_m = args.radius_m if scope == "part" else None
        gdf = load_speed_snapshot(args.hour, radius_m)
        if scope == "part":
            write_snapshot_csv(gdf, args.snapshot_csv)
            plot_speed_map(gdf, districts, args.hour, args.output_png, scope=scope)
            if args.output_png_l3 != args.output_png:
                plot_speed_map(gdf, districts, args.hour, args.output_png_l3, scope=scope)
            print(f"Wrote {args.output_png}")
            print(f"Wrote {args.output_png_l3}")
            print(f"Wrote {args.snapshot_csv} ({len(gdf):,} segments)")
        else:
            plot_speed_map(gdf, districts, args.hour, args.output_png_full, scope=scope)
            write_snapshot_csv(gdf, ROOT / "outputs" / "bj_am_0830_full.csv")
            print(f"Wrote {args.output_png_full}")
            print(f"Wrote {ROOT / 'outputs' / 'bj_am_0830_full.csv'} ({len(gdf):,} segments)")


if __name__ == "__main__":
    main()
