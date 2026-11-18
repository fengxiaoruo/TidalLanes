from __future__ import annotations

import json
from pathlib import Path

import geopandas as gpd
import matplotlib as mpl
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.patheffects as pe
import numpy as np
import pandas as pd

from plot_caitian_osm_overlays import add_osm_background, clean_map_axes


ROOT = Path(__file__).resolve().parents[3]
CATALOG = ROOT / "data_work/outputs/shenzhen_caitian_baidu_analysis_v1/data/analysis_segment_catalog.geojson"
SPEED_PANEL = ROOT / "data_work/outputs/shenzhen_caitian_baidu_analysis_v1/data/selected_speed_panel_raw.csv"
GPS_MANIFEST = ROOT / "flowvedio/20260710彩田路/inbound_recording_manifest.csv"
OUTPUT = ROOT / "data_work/outputs/shenzhen_caitian_video_baidu_segment_v1"
PROJECTED_CRS = "EPSG:32650"

SEGMENTS = {
    "south_to_north": {
        "roadseg_id": "RS_NB1521432208_NB1521432620_1",
        "label_cn": "南向北（福田→龙华）",
        "color": "#4C78A8",
    },
    "north_to_south": {
        "roadseg_id": "RS_NB1521432253_NB1521439708_1",
        "label_cn": "北向南（龙华→福田）",
        "color": "#E15759",
    },
}


def configure_style() -> None:
    mpl.rcParams.update(
        {
            "font.family": "sans-serif",
            "font.sans-serif": ["PingFang SC", "Hiragino Sans GB", "Arial Unicode MS", "DejaVu Sans"],
            "font.size": 8.5,
            "axes.labelsize": 8.5,
            "axes.titlesize": 9,
            "xtick.labelsize": 7.5,
            "ytick.labelsize": 7.5,
            "axes.linewidth": 0.8,
            "xtick.major.width": 0.8,
            "ytick.major.width": 0.8,
            "xtick.direction": "out",
            "ytick.direction": "out",
            "svg.fonttype": "none",
            "pdf.fonttype": 42,
            "savefig.dpi": 600,
        }
    )


def save_all(fig: plt.Figure, stem: Path) -> None:
    for suffix in (".png", ".pdf", ".svg"):
        fig.savefig(stem.with_suffix(suffix), bbox_inches="tight", dpi=600 if suffix == ".png" else None)
    plt.close(fig)


def load_inputs() -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, pd.DataFrame]:
    roads = gpd.read_file(CATALOG)
    ids = {x["roadseg_id"] for x in SEGMENTS.values()}
    roads = roads[roads["roadseg_id"].isin(ids)].copy()
    if set(roads["roadseg_id"]) != ids:
        raise ValueError("Selected directed Baidu segments are missing from the retained catalog")

    gps = pd.read_csv(GPS_MANIFEST)
    gps = gps.dropna(subset=["longitude", "latitude"]).copy()
    gps_points = gpd.GeoDataFrame(
        gps,
        geometry=gpd.points_from_xy(gps["longitude"], gps["latitude"]),
        crs="EPSG:4326",
    )

    usecols = ["roadseg_id", "speed", "datetime", "date", "dow", "day_type", "minute_of_day"]
    panel = pd.read_csv(SPEED_PANEL, usecols=usecols)
    panel = panel[panel["roadseg_id"].isin(ids)].copy()
    panel["datetime"] = pd.to_datetime(panel["datetime"])
    panel["date"] = pd.to_datetime(panel["date"])
    id_to_direction = {v["roadseg_id"]: k for k, v in SEGMENTS.items()}
    panel["direction"] = panel["roadseg_id"].map(id_to_direction)
    panel["direction_label"] = panel["direction"].map({k: v["label_cn"] for k, v in SEGMENTS.items()})
    panel = panel.rename(columns={"speed": "speed_kmh"}).sort_values(["datetime", "direction"])
    return roads, gps_points, panel


def build_match_table(roads: gpd.GeoDataFrame, gps: gpd.GeoDataFrame, panel: pd.DataFrame) -> pd.DataFrame:
    roads_m = roads.to_crs(PROJECTED_CRS)
    gps_m = gps.to_crs(PROJECTED_CRS)
    gps_union = gps_m.geometry.union_all()
    rows = []
    for direction, spec in SEGMENTS.items():
        road = roads_m.loc[roads_m["roadseg_id"].eq(spec["roadseg_id"])].iloc[0]
        sub = panel[panel["roadseg_id"].eq(spec["roadseg_id"])]
        rows.append(
            {
                "direction": direction,
                "direction_label": spec["label_cn"],
                "roadseg_id": spec["roadseg_id"],
                "roadname": road["roadname"],
                "semantic": road["semantic"],
                "segment_length_m": float(road.geometry.length),
                "min_video_gps_distance_m": float(road.geometry.distance(gps_union)),
                "median_video_gps_distance_m": float(np.median(gps_m.geometry.distance(road.geometry))),
                "n_speed_observations": int(len(sub)),
                "first_timestamp": sub["datetime"].min(),
                "last_timestamp": sub["datetime"].max(),
                "missing_of_expected_10min": int(7 * 24 * 6 - len(sub)),
            }
        )
    return pd.DataFrame(rows)


def plot_match_map(roads: gpd.GeoDataFrame, gps: gpd.GeoDataFrame, match: pd.DataFrame) -> None:
    roads_m = roads.to_crs(PROJECTED_CRS)
    gps_m = gps.to_crs(PROJECTED_CRS)
    fig, ax = plt.subplots(figsize=(5.2, 4.2))
    for direction, spec in SEGMENTS.items():
        roads_m[roads_m["roadseg_id"].eq(spec["roadseg_id"])].plot(
            ax=ax, color=spec["color"], linewidth=3.2, label=spec["label_cn"], zorder=2
        )
    gps_m.plot(ax=ax, color="#111111", marker="o", markersize=18, alpha=0.75, label="视频 GPS", zorder=3)
    center = gps_m.geometry.union_all().centroid
    ax.annotate(
        "视频采集断面",
        xy=(center.x, center.y),
        xytext=(12, 14),
        textcoords="offset points",
        arrowprops={"arrowstyle": "-", "color": "#333333", "lw": 0.8},
        fontsize=8,
    )
    xmin, ymin, xmax, ymax = gps_m.total_bounds
    ax.set_xlim(xmin - 220, xmax + 220)
    ax.set_ylim(ymin - 260, ymax + 260)
    ax.set_aspect("equal")
    ax.set_axis_off()
    ax.legend(frameon=False, loc="upper left", handlelength=2.6)
    ax.set_title("视频断面与百度双向子路段的空间匹配", loc="left", fontweight="bold")
    note = "最近 GPS–路段距离：" + "；".join(
        f"{row.direction_label.split('（')[0]} {row.min_video_gps_distance_m:.1f} m" for row in match.itertuples()
    )
    fig.text(0.02, 0.015, note, fontsize=7.5, color="#444444")
    save_all(fig, OUTPUT / "figures/video_gps_baidu_segment_match")


def plot_osm_location_map(roads: gpd.GeoDataFrame, gps: gpd.GeoDataFrame) -> None:
    roads_3857 = roads.to_crs("EPSG:3857")
    gps_3857 = gps.to_crs("EPSG:3857")
    minx, miny, maxx, maxy = roads.total_bounds
    bounds = (minx - 0.0030, miny - 0.0015, maxx + 0.0030, maxy + 0.0015)
    fig, ax = plt.subplots(figsize=(5.8, 7.2))
    cache = ROOT / "data_work/outputs/shenzhen_caitian_baidu_analysis_v1/data/osm_tile_cache"
    add_osm_background(ax, bounds, cache, zoom=16)
    for direction, spec in SEGMENTS.items():
        roads_3857[roads_3857["roadseg_id"].eq(spec["roadseg_id"])].plot(
            ax=ax,
            color=spec["color"],
            linewidth=5.0,
            label=spec["label_cn"],
            path_effects=[pe.Stroke(linewidth=7.5, foreground="white"), pe.Normal()],
            zorder=5,
        )
    gps_3857.plot(
        ax=ax,
        color="#111111",
        edgecolor="white",
        linewidth=0.8,
        markersize=32,
        label="视频 GPS",
        zorder=7,
    )
    center = gps_3857.geometry.union_all().centroid
    ax.annotate(
        "视频采集位置",
        xy=(center.x, center.y),
        xytext=(24, 8),
        textcoords="offset points",
        fontsize=9,
        fontweight="bold",
        arrowprops={"arrowstyle": "->", "lw": 1.1, "color": "#222222"},
        path_effects=[pe.withStroke(linewidth=3, foreground="white")],
        zorder=9,
    )
    for label, xy in {
        "北环彩田立交": (114.0623, 22.5661),
        "红荔路": (114.0627, 22.5515),
    }.items():
        pt = gpd.GeoSeries(gpd.points_from_xy([xy[0]], [xy[1]]), crs="EPSG:4326").to_crs("EPSG:3857").iloc[0]
        ax.text(
            pt.x,
            pt.y,
            label,
            fontsize=8.5,
            ha="center",
            va="center",
            path_effects=[pe.withStroke(linewidth=3, foreground="white")],
            zorder=8,
        )
    ax.legend(frameon=True, facecolor="white", framealpha=0.9, edgecolor="#BBBBBB", loc="lower left")
    ax.set_title("彩田路视频采集路段位置", loc="left", fontweight="bold", pad=8)
    clean_map_axes(ax)
    save_all(fig, OUTPUT / "figures/video_segment_osm_location_map")


def plot_week(panel: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(10.2, 3.7))
    for direction, spec in SEGMENTS.items():
        sub = panel[panel["direction"].eq(direction)]
        ax.plot(sub["datetime"], sub["speed_kmh"], color=spec["color"], lw=0.9, label=spec["label_cn"])
    dates = pd.date_range(panel["date"].min(), panel["date"].max(), freq="D")
    for date in dates:
        if date.dayofweek < 5:
            ax.axvspan(date + pd.Timedelta(hours=16, minutes=30), date + pd.Timedelta(hours=21, minutes=30), color="#D9D9D9", alpha=0.38, lw=0)
        if date > dates.min():
            ax.axvline(date, color="#D0D0D0", lw=0.5, zorder=0)
    ax.set_ylabel("速度（km/h）")
    ax.set_xlabel("日期（2025年5月19–25日；灰色为工作日 16:30–21:30）")
    ax.xaxis.set_major_locator(mdates.DayLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d"))
    ax.set_ylim(bottom=0)
    ax.grid(axis="y", color="#E6E6E6", linewidth=0.6)
    ax.spines[["top", "right"]].set_visible(False)
    ax.legend(frameon=False, ncol=2, loc="upper right")
    ax.set_title("彩田路视频采集子路段：百度双向 10 分钟速度时间序列", loc="left", fontweight="bold")
    save_all(fig, OUTPUT / "figures/video_segment_week_speed_timeseries")


def plot_daily_profiles(panel: pd.DataFrame) -> None:
    dates = sorted(panel["date"].unique())
    fig, axes = plt.subplots(4, 2, figsize=(7.1, 8.2), sharex=True, sharey=True)
    axes = axes.ravel()
    for ax, date in zip(axes, dates):
        day = panel[panel["date"].eq(date)]
        for direction, spec in SEGMENTS.items():
            sub = day[day["direction"].eq(direction)]
            ax.plot(sub["minute_of_day"] / 60, sub["speed_kmh"], color=spec["color"], lw=0.95, label=spec["label_cn"])
        ts = pd.Timestamp(date)
        if ts.dayofweek < 5:
            ax.axvspan(16.5, 21.5, color="#D9D9D9", alpha=0.45, lw=0)
        ax.set_title(f"{ts:%m-%d}  {'工作日' if ts.dayofweek < 5 else '周末'}", loc="left")
        ax.grid(axis="y", color="#E8E8E8", linewidth=0.5)
        ax.spines[["top", "right"]].set_visible(False)
        ax.set_xlim(0, 24)
        ax.set_ylim(bottom=0)
    for ax in axes[len(dates):]:
        ax.axis("off")
    for ax in axes[-2:]:
        if ax.axison:
            ax.set_xlabel("时刻")
    for ax in axes[::2]:
        ax.set_ylabel("速度（km/h）")
    axes[0].legend(frameon=False, ncol=2, loc="upper left", bbox_to_anchor=(0, 1.33))
    fig.suptitle("彩田路视频采集子路段：逐日双向速度", x=0.07, ha="left", fontweight="bold", y=0.995)
    fig.tight_layout(rect=(0, 0, 1, 0.965), h_pad=1.1, w_pad=1.0)
    save_all(fig, OUTPUT / "figures/video_segment_daily_speed_profiles")


def main() -> None:
    configure_style()
    (OUTPUT / "data").mkdir(parents=True, exist_ok=True)
    (OUTPUT / "figures").mkdir(parents=True, exist_ok=True)
    (OUTPUT / "metrics").mkdir(parents=True, exist_ok=True)
    roads, gps, panel = load_inputs()
    match = build_match_table(roads, gps, panel)
    panel.to_csv(OUTPUT / "data/video_segment_bidirectional_speed_10min.csv", index=False)
    match.to_csv(OUTPUT / "metrics/video_segment_match_summary.csv", index=False)
    gps.drop(columns="geometry").to_csv(OUTPUT / "data/video_gps_points.csv", index=False)
    roads.to_file(OUTPUT / "data/video_segment_baidu_roads.geojson", driver="GeoJSON")
    plot_match_map(roads, gps, match)
    plot_osm_location_map(roads, gps)
    plot_week(panel)
    plot_daily_profiles(panel)
    metadata = {
        "speed_source": str(SPEED_PANEL.relative_to(ROOT)),
        "road_source": str(CATALOG.relative_to(ROOT)),
        "gps_source": str(GPS_MANIFEST.relative_to(ROOT)),
        "frequency": "10 minutes",
        "date_range": [str(panel["date"].min().date()), str(panel["date"].max().date())],
        "aggregation": "none; each line is the raw Baidu speed for one directed road segment",
        "selected_segments": SEGMENTS,
    }
    (OUTPUT / "metrics/run_metadata.json").write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
    print(match.to_string(index=False))


if __name__ == "__main__":
    main()
