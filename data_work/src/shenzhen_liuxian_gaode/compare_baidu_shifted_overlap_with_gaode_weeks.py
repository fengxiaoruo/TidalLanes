from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from shapely.affinity import translate

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT / "data_work"))

from src.shenzhen_liuxian_gaode.event_study_liuxian import (  # noqa: E402
    DEFAULT_INPUT_DIR,
    DEFAULT_OUTPUT_DIR,
    POLICY_DATE,
    configure_plot,
    ensure_output_dirs,
    load_panel,
)
from src.shenzhen_liuxian_gaode.match_baidu_underlying_segments import (  # noqa: E402
    DEFAULT_BAIDU_ROADS,
    DEFAULT_BAIDU_SPEED,
    bearing_degrees,
    chain_from_bearing,
    extract_speed_panel,
    load_gaode_segments,
)
from src.shenzhen_liuxian_gaode.plot_chain_window_speeds import (  # noqa: E402
    CHAIN_LABELS,
    assign_chain_direction,
)


SHIFT_DIAGNOSTIC = DEFAULT_OUTPUT_DIR / "metrics/baidu_gaode_coordinate_shift_diagnostic.csv"
PREEXTRACTED_BAIDU_PANEL = (
    DEFAULT_OUTPUT_DIR / "data/baidu_liuxian_underlying_speed_panel_20250519_20250525.csv"
)

SHIFTED_TARGET_BUFFER_M = 20.0
MAX_SHIFTED_TARGET_DISTANCE_M = 40.0
MIN_BAIDU_SHARE_IN_SHIFTED_BUFFER = 0.05

SOURCE_ORDER = ["baidu_2025", "gaode_pre", "gaode_post"]
SOURCE_TITLES = {
    "baidu_2025": "Baidu raw speed\n2025-05-19 to 2025-05-25",
    "gaode_pre": "Gaode purchased pre week\n2026-03-02 to 2026-03-08",
    "gaode_post": "Gaode purchased post week\n2026-04-13 to 2026-04-19",
}
CHAIN_ORDER = ["eastbound_chain", "westbound_chain"]
CHAIN_SHORT = {
    "eastbound_chain": "Eastbound chain\nSW -> NE",
    "westbound_chain": "Westbound chain\nNE -> SW",
}
WEEKDAY_LABELS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare shifted-overlap Baidu raw speeds with two purchased Gaode weeks."
    )
    parser.add_argument("--gaode-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--baidu-roads", type=Path, default=DEFAULT_BAIDU_ROADS)
    parser.add_argument("--baidu-speed", type=Path, default=DEFAULT_BAIDU_SPEED)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def shifted_gaode_segments(
    gaode_segments: gpd.GeoDataFrame,
    shift_table: pd.DataFrame,
    metric_crs,
) -> gpd.GeoDataFrame:
    gaode_m = gaode_segments.to_crs(metric_crs).copy()
    pieces = []
    for shift in shift_table.itertuples(index=False):
        sub = gaode_m[gaode_m["chain_direction"].eq(shift.chain_direction)].copy()
        sub["geometry_original_m"] = sub.geometry
        sub["geometry"] = sub.geometry.map(
            lambda geom: translate(
                geom,
                xoff=shift.translation_dx_m_east,
                yoff=shift.translation_dy_m_north,
            )
        )
        sub["translation_dx_m_east"] = shift.translation_dx_m_east
        sub["translation_dy_m_north"] = shift.translation_dy_m_north
        pieces.append(sub)
    return gpd.GeoDataFrame(pd.concat(pieces, ignore_index=True), geometry="geometry", crs=metric_crs)


def find_shifted_overlap_baidu_segments(
    gaode_shifted_m: gpd.GeoDataFrame,
    roads_m: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    roads = roads_m.copy()
    roads["baidu_bearing_deg"] = roads.geometry.to_crs("EPSG:4326").map(bearing_degrees)
    roads["chain_direction"] = roads["baidu_bearing_deg"].map(chain_from_bearing)
    roads["baidu_length_m"] = roads.geometry.length

    selected = []
    for chain in CHAIN_ORDER:
        target = gaode_shifted_m.loc[gaode_shifted_m["chain_direction"].eq(chain), "geometry"].union_all()
        target_buffer = target.buffer(SHIFTED_TARGET_BUFFER_M)
        sub = roads[(roads["roadname"].eq("留仙大道")) & (roads["chain_direction"].eq(chain))].copy()
        sub["dist_to_shifted_gaode_m"] = sub.geometry.distance(target)
        sub["share_in_shifted_gaode_20m_buffer"] = (
            sub.geometry.intersection(target_buffer).length / sub["baidu_length_m"]
        )
        sub = sub[
            (sub["dist_to_shifted_gaode_m"] <= MAX_SHIFTED_TARGET_DISTANCE_M)
            & (sub["share_in_shifted_gaode_20m_buffer"] >= MIN_BAIDU_SHARE_IN_SHIFTED_BUFFER)
        ].copy()
        selected.append(sub)

    out = gpd.GeoDataFrame(pd.concat(selected, ignore_index=True), geometry="geometry", crs=roads_m.crs)
    out = out.sort_values(["chain_direction", "dist_to_shifted_gaode_m", "roadseg_id"]).copy()
    return out


def load_or_extract_baidu_panel(output_dir: Path, speed_csv: Path, roadseg_ids: set[str]) -> pd.DataFrame:
    preextracted = output_dir / "data/baidu_liuxian_underlying_speed_panel_20250519_20250525.csv"
    if preextracted.exists():
        panel = pd.read_csv(preextracted)
        panel = panel[panel["roadseg_id"].astype(str).isin(roadseg_ids)].copy()
        panel["datetime"] = pd.to_datetime(panel["datetime"])
        panel["date"] = pd.to_datetime(panel["date"])
        return panel
    panel = extract_speed_panel(speed_csv, roadseg_ids)
    panel["date"] = pd.to_datetime(panel["date"])
    return panel


def add_week_index(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    date_order = (
        out[["source_period", "date"]]
        .drop_duplicates()
        .sort_values(["source_period", "date"])
        .copy()
    )
    date_order["day_index"] = date_order.groupby("source_period").cumcount()
    out = out.merge(date_order, on=["source_period", "date"], how="left")
    out["week_minute"] = out["day_index"] * 24 * 60 + out["minute_of_day"]
    out["dow_name"] = out["datetime"].dt.day_name()
    return out


def corridor_speed(group: pd.DataFrame) -> float:
    denom = (group["length_m"] / group["speed_kmh"]).sum()
    if denom <= 0:
        return np.nan
    return float(group["length_m"].sum() / denom)


def build_baidu_corridor_panel(
    baidu_speed: pd.DataFrame,
    selected_baidu_m: gpd.GeoDataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    attrs = selected_baidu_m.drop(columns="geometry").copy()
    attrs = attrs[
        [
            "roadseg_id",
            "roadname",
            "semantic",
            "location",
            "chain_direction",
            "baidu_length_m",
            "dist_to_shifted_gaode_m",
            "share_in_shifted_gaode_20m_buffer",
        ]
    ].copy()
    baidu_speed = baidu_speed.copy()
    baidu_speed["roadseg_id"] = baidu_speed["roadseg_id"].astype(str)
    attrs["roadseg_id"] = attrs["roadseg_id"].astype(str)
    panel = baidu_speed.merge(attrs, on=["roadseg_id", "roadname", "semantic", "location"], how="inner")
    panel["speed_kmh"] = pd.to_numeric(panel["speed"], errors="coerce")
    panel["length_m"] = panel["baidu_length_m"]
    panel = panel[(panel["speed_kmh"] > 0) & (panel["length_m"] > 0)].copy()
    panel["source_period"] = "baidu_2025"
    panel["date"] = pd.to_datetime(panel["date"])
    panel["datetime"] = pd.to_datetime(panel["datetime"])

    expected = panel.groupby("chain_direction")["roadseg_id"].nunique().to_dict()
    corridor = (
        panel.groupby(["source_period", "chain_direction", "date", "datetime", "minute_of_day"], as_index=False)
        .apply(
            lambda group: pd.Series(
                {
                    "speed_kmh": corridor_speed(group),
                    "mean_segment_speed_kmh": group["speed_kmh"].mean(),
                    "n_segments": group["roadseg_id"].nunique(),
                    "total_length_m": group["length_m"].sum(),
                }
            ),
            include_groups=False,
        )
        .reset_index(drop=True)
    )
    corridor = corridor[
        corridor.apply(lambda row: row["n_segments"] == expected[row["chain_direction"]], axis=1)
    ].copy()

    coverage = (
        panel.groupby(["chain_direction", "roadseg_id", "roadname", "semantic", "location"], as_index=False)
        .agg(
            n_obs=("speed_kmh", "size"),
            n_days=("date", "nunique"),
            mean_speed_kmh=("speed_kmh", "mean"),
            length_m=("length_m", "first"),
            dist_to_shifted_gaode_m=("dist_to_shifted_gaode_m", "first"),
            share_in_shifted_gaode_20m_buffer=("share_in_shifted_gaode_20m_buffer", "first"),
        )
    )
    return corridor, coverage


def build_gaode_corridor_panel(gaode_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    panel = load_panel(gaode_dir)
    panel, inventory = assign_chain_direction(panel)
    panel["source_period"] = np.where(panel["date"] < POLICY_DATE, "gaode_pre", "gaode_post")
    panel = panel.rename(columns={"segment_length_m": "length_m", "corridor_speed_kmh": "unused"})
    expected = panel.groupby("chain_direction")["segment_id"].nunique().to_dict()

    corridor = (
        panel.groupby(["source_period", "chain_direction", "date", "datetime", "minute_of_day"], as_index=False)
        .apply(
            lambda group: pd.Series(
                {
                    "speed_kmh": corridor_speed(group),
                    "mean_segment_speed_kmh": group["speed_kmh"].mean(),
                    "n_segments": group["segment_id"].nunique(),
                    "total_length_m": group["length_m"].sum(),
                }
            ),
            include_groups=False,
        )
        .reset_index(drop=True)
    )
    corridor = corridor[
        corridor.apply(lambda row: row["n_segments"] == expected[row["chain_direction"]], axis=1)
    ].copy()
    return corridor, inventory


def summarize_windows(combined: pd.DataFrame) -> pd.DataFrame:
    rows = []
    windows = {
        "all_day": (0, 24 * 60),
        "weekday_1500_1730": (15 * 60, 17 * 60 + 30),
        "weekday_1730_2000": (17 * 60 + 30, 20 * 60),
        "weekday_2000_2200": (20 * 60, 22 * 60),
        "night_0000_0500": (0, 5 * 60),
    }
    for name, (lo, hi) in windows.items():
        sub = combined[(combined["minute_of_day"] >= lo) & (combined["minute_of_day"] < hi)].copy()
        if name.startswith("weekday"):
            sub = sub[sub["datetime"].dt.dayofweek < 5].copy()
        grouped = (
            sub.groupby(["source_period", "chain_direction"], as_index=False)
            .agg(
                mean_speed_kmh=("speed_kmh", "mean"),
                median_speed_kmh=("speed_kmh", "median"),
                n_10min=("speed_kmh", "size"),
                n_days=("date", "nunique"),
            )
        )
        grouped["window"] = name
        rows.append(grouped)
    return pd.concat(rows, ignore_index=True).sort_values(["chain_direction", "window", "source_period"])


def add_rolling_speed(combined: pd.DataFrame) -> pd.DataFrame:
    out = combined.sort_values(["source_period", "chain_direction", "week_minute"]).copy()
    out["speed_1h_roll_kmh"] = (
        out.groupby(["source_period", "chain_direction"])["speed_kmh"]
        .transform(lambda s: s.rolling(6, min_periods=1, center=True).mean())
    )
    return out


def draw_pm_bands(ax) -> None:
    for day in range(5):
        ax.axvspan(
            day * 24 * 60 + 17 * 60 + 30,
            day * 24 * 60 + 20 * 60,
            color="#f2c14e",
            alpha=0.09,
            linewidth=0,
        )


def format_week_axis(ax) -> None:
    for day in range(1, 7):
        ax.axvline(day * 24 * 60, color="#d0d0d0", linewidth=0.6, alpha=0.8)
    ax.set_xlim(0, 7 * 24 * 60)
    ax.set_xticks([day * 24 * 60 + 12 * 60 for day in range(7)])
    ax.set_xticklabels(WEEKDAY_LABELS, fontsize=8)


def plot_weekly_timeseries(combined: pd.DataFrame, output_path: Path) -> None:
    colors = {"eastbound_chain": "#c44e52", "westbound_chain": "#4c78a8"}
    fig, axes = plt.subplots(2, 3, figsize=(14.2, 6.2), sharex=True, sharey=True)
    for row, chain in enumerate(CHAIN_ORDER):
        for col, source in enumerate(SOURCE_ORDER):
            ax = axes[row, col]
            sub = combined[
                (combined["source_period"].eq(source)) & (combined["chain_direction"].eq(chain))
            ].sort_values("week_minute")
            draw_pm_bands(ax)
            if not sub.empty:
                ax.plot(
                    sub["week_minute"],
                    sub["speed_kmh"],
                    color=colors[chain],
                    linewidth=0.55,
                    alpha=0.28,
                    label="10-min speed",
                )
                ax.plot(
                    sub["week_minute"],
                    sub["speed_1h_roll_kmh"],
                    color=colors[chain],
                    linewidth=1.6,
                    alpha=0.95,
                    label="1-hour rolling mean",
                )
            format_week_axis(ax)
            ax.grid(axis="y", color="#e0e0e0", linewidth=0.6, alpha=0.8)
            ax.spines["top"].set_visible(False)
            ax.spines["right"].set_visible(False)
            if row == 0:
                ax.set_title(SOURCE_TITLES[source], fontsize=10.5)
            if col == 0:
                ax.set_ylabel(f"{CHAIN_SHORT[chain]}\nSpeed (km/h)")
            if row == 1:
                ax.set_xlabel("Day within observed week")
            if row == 0 and col == 2:
                ax.legend(loc="upper right", frameon=False)

    fig.suptitle("Liuxian Avenue speed trend comparison on shifted Baidu-Gaode overlap", x=0.01, ha="left")
    fig.text(
        0.01,
        0.01,
        "Notes: Yellow bands mark weekday 17:30-20:00. Baidu segments are selected after applying "
        "the estimated Gaode-to-Baidu translation. Speeds are length-weighted harmonic corridor speeds "
        "within each source-period and direction.",
        fontsize=7.7,
        color="#444444",
    )
    fig.tight_layout(rect=(0, 0.06, 1, 0.94))
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)


def plot_weekday_diurnal(combined: pd.DataFrame, output_path: Path) -> None:
    colors = {"eastbound_chain": "#c44e52", "westbound_chain": "#4c78a8"}
    weekdays = combined[combined["datetime"].dt.dayofweek < 5].copy()
    diurnal = (
        weekdays.groupby(["source_period", "chain_direction", "minute_of_day"], as_index=False)
        .agg(mean_speed_kmh=("speed_kmh", "mean"))
    )
    fig, axes = plt.subplots(2, 3, figsize=(14.2, 5.8), sharex=True, sharey=True)
    for row, chain in enumerate(CHAIN_ORDER):
        for col, source in enumerate(SOURCE_ORDER):
            ax = axes[row, col]
            sub = diurnal[
                (diurnal["source_period"].eq(source)) & (diurnal["chain_direction"].eq(chain))
            ].sort_values("minute_of_day")
            ax.axvspan(15 * 60, 17 * 60 + 30, color="#4c78a8", alpha=0.08, linewidth=0)
            ax.axvspan(17 * 60 + 30, 20 * 60, color="#c44e52", alpha=0.08, linewidth=0)
            ax.plot(sub["minute_of_day"], sub["mean_speed_kmh"], color=colors[chain], linewidth=1.6)
            ax.set_xlim(0, 24 * 60)
            ax.set_xticks([0, 6 * 60, 12 * 60, 18 * 60, 24 * 60])
            ax.set_xticklabels(["00:00", "06:00", "12:00", "18:00", "24:00"], fontsize=8)
            ax.grid(axis="y", color="#e0e0e0", linewidth=0.6, alpha=0.8)
            ax.spines["top"].set_visible(False)
            ax.spines["right"].set_visible(False)
            if row == 0:
                ax.set_title(SOURCE_TITLES[source], fontsize=10.5)
            if col == 0:
                ax.set_ylabel(f"{CHAIN_SHORT[chain]}\nMean weekday speed (km/h)")
            if row == 1:
                ax.set_xlabel("Time of day")

    fig.suptitle("Weekday mean 10-minute diurnal speed profile on shifted overlap", x=0.01, ha="left")
    fig.text(
        0.01,
        0.01,
        "Blue shading marks 15:00-17:30; red shading marks 17:30-20:00.",
        fontsize=7.7,
        color="#444444",
    )
    fig.tight_layout(rect=(0, 0.055, 1, 0.93))
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)


def plot_weekly_timeseries_overlay_directions(combined: pd.DataFrame, output_path: Path) -> None:
    colors = {"eastbound_chain": "#c44e52", "westbound_chain": "#4c78a8"}
    labels = {
        "eastbound_chain": "Eastbound, SW -> NE",
        "westbound_chain": "Westbound, NE -> SW",
    }
    fig, axes = plt.subplots(1, 3, figsize=(14.2, 3.9), sharex=True, sharey=True)
    for col, source in enumerate(SOURCE_ORDER):
        ax = axes[col]
        draw_pm_bands(ax)
        for chain in CHAIN_ORDER:
            sub = combined[
                (combined["source_period"].eq(source)) & (combined["chain_direction"].eq(chain))
            ].sort_values("week_minute")
            if sub.empty:
                continue
            ax.plot(
                sub["week_minute"],
                sub["speed_kmh"],
                color=colors[chain],
                linewidth=0.45,
                alpha=0.16,
            )
            ax.plot(
                sub["week_minute"],
                sub["speed_1h_roll_kmh"],
                color=colors[chain],
                linewidth=1.65,
                alpha=0.95,
                label=labels[chain],
            )
        format_week_axis(ax)
        ax.set_title(SOURCE_TITLES[source], fontsize=10.5)
        ax.grid(axis="y", color="#e0e0e0", linewidth=0.6, alpha=0.8)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        if col == 0:
            ax.set_ylabel("Speed (km/h)")
        ax.set_xlabel("Day within observed week")
        if col == 2:
            ax.legend(loc="upper right", frameon=False)

    fig.suptitle(
        "Liuxian Avenue speed trend comparison with both directions overlaid",
        x=0.01,
        ha="left",
    )
    fig.text(
        0.01,
        0.01,
        "Notes: Faint lines are 10-minute speeds; solid lines are 1-hour rolling means. "
        "Yellow bands mark weekday 17:30-20:00.",
        fontsize=7.7,
        color="#444444",
    )
    fig.tight_layout(rect=(0, 0.08, 1, 0.9))
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)


def plot_weekday_diurnal_overlay_directions(combined: pd.DataFrame, output_path: Path) -> None:
    colors = {"eastbound_chain": "#c44e52", "westbound_chain": "#4c78a8"}
    labels = {
        "eastbound_chain": "Eastbound, SW -> NE",
        "westbound_chain": "Westbound, NE -> SW",
    }
    weekdays = combined[combined["datetime"].dt.dayofweek < 5].copy()
    diurnal = (
        weekdays.groupby(["source_period", "chain_direction", "minute_of_day"], as_index=False)
        .agg(mean_speed_kmh=("speed_kmh", "mean"))
    )
    fig, axes = plt.subplots(1, 3, figsize=(14.2, 3.8), sharex=True, sharey=True)
    for col, source in enumerate(SOURCE_ORDER):
        ax = axes[col]
        ax.axvspan(15 * 60, 17 * 60 + 30, color="#4c78a8", alpha=0.08, linewidth=0)
        ax.axvspan(17 * 60 + 30, 20 * 60, color="#c44e52", alpha=0.08, linewidth=0)
        for chain in CHAIN_ORDER:
            sub = diurnal[
                (diurnal["source_period"].eq(source)) & (diurnal["chain_direction"].eq(chain))
            ].sort_values("minute_of_day")
            ax.plot(
                sub["minute_of_day"],
                sub["mean_speed_kmh"],
                color=colors[chain],
                linewidth=1.8,
                label=labels[chain],
            )
        ax.set_xlim(0, 24 * 60)
        ax.set_xticks([0, 6 * 60, 12 * 60, 18 * 60, 24 * 60])
        ax.set_xticklabels(["00:00", "06:00", "12:00", "18:00", "24:00"], fontsize=8)
        ax.set_title(SOURCE_TITLES[source], fontsize=10.5)
        ax.grid(axis="y", color="#e0e0e0", linewidth=0.6, alpha=0.8)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        if col == 0:
            ax.set_ylabel("Mean weekday speed (km/h)")
        ax.set_xlabel("Time of day")
        if col == 2:
            ax.legend(loc="upper right", frameon=False)

    fig.suptitle(
        "Weekday diurnal profile with both directions overlaid on shifted overlap",
        x=0.01,
        ha="left",
    )
    fig.text(
        0.01,
        0.01,
        "Blue shading marks 15:00-17:30; red shading marks 17:30-20:00.",
        fontsize=7.7,
        color="#444444",
    )
    fig.tight_layout(rect=(0, 0.08, 1, 0.9))
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    args = parse_args()
    configure_plot()
    dirs = ensure_output_dirs(args.output_dir)

    roads = gpd.read_file(args.baidu_roads)
    metric_crs = roads.estimate_utm_crs()
    roads_m = roads.to_crs(metric_crs)
    shift_table = pd.read_csv(SHIFT_DIAGNOSTIC)

    gaode_segments = load_gaode_segments(args.gaode_dir)
    gaode_shifted_m = shifted_gaode_segments(gaode_segments, shift_table, metric_crs)
    selected_baidu_m = find_shifted_overlap_baidu_segments(gaode_shifted_m, roads_m)
    selected_ids = set(selected_baidu_m["roadseg_id"].astype(str))

    baidu_speed_raw = load_or_extract_baidu_panel(args.output_dir, args.baidu_speed, selected_ids)
    baidu_corridor, baidu_coverage = build_baidu_corridor_panel(baidu_speed_raw, selected_baidu_m)
    gaode_corridor, gaode_inventory = build_gaode_corridor_panel(args.gaode_dir)

    combined = pd.concat([baidu_corridor, gaode_corridor], ignore_index=True)
    combined = add_week_index(combined)
    combined = add_rolling_speed(combined)
    combined["source_title"] = combined["source_period"].map(SOURCE_TITLES)
    combined["chain_label"] = combined["chain_direction"].map(CHAIN_LABELS)

    selected_for_csv = selected_baidu_m.to_crs("EPSG:4326").drop(columns="geometry").copy()
    selected_for_csv["has_speed_in_baidu_week"] = selected_for_csv["roadseg_id"].astype(str).isin(
        set(baidu_coverage["roadseg_id"].astype(str))
    )
    selected_for_csv.to_csv(
        dirs["metrics"] / "baidu_shifted_overlap_segments.csv",
        index=False,
        encoding="utf-8-sig",
    )
    baidu_coverage.to_csv(
        dirs["metrics"] / "baidu_shifted_overlap_speed_segment_coverage.csv",
        index=False,
        encoding="utf-8-sig",
    )
    gaode_inventory.to_csv(
        dirs["metrics"] / "gaode_chain_segments_used_for_baidu_overlap_compare.csv",
        index=False,
        encoding="utf-8-sig",
    )
    combined.to_csv(
        dirs["data"] / "baidu_gaode_shifted_overlap_weekly_10min_speed_compare.csv",
        index=False,
        encoding="utf-8-sig",
    )
    summary = summarize_windows(combined)
    summary.to_csv(
        dirs["metrics"] / "baidu_gaode_shifted_overlap_speed_window_summary.csv",
        index=False,
        encoding="utf-8-sig",
    )

    plot_weekly_timeseries(
        combined,
        dirs["figures"] / "baidu_gaode_shifted_overlap_weekly_10min_speed_compare.png",
    )
    plot_weekday_diurnal(
        combined,
        dirs["figures"] / "baidu_gaode_shifted_overlap_weekday_diurnal_compare.png",
    )
    plot_weekly_timeseries_overlay_directions(
        combined,
        dirs["figures"] / "baidu_gaode_shifted_overlap_weekly_10min_overlay_directions.png",
    )
    plot_weekday_diurnal_overlay_directions(
        combined,
        dirs["figures"] / "baidu_gaode_shifted_overlap_weekday_diurnal_overlay_directions.png",
    )

    metadata = {
        "shift_diagnostic": str(SHIFT_DIAGNOSTIC),
        "baidu_roads": str(args.baidu_roads),
        "baidu_speed": str(args.baidu_speed),
        "shifted_target_buffer_m": SHIFTED_TARGET_BUFFER_M,
        "max_shifted_target_distance_m": MAX_SHIFTED_TARGET_DISTANCE_M,
        "min_baidu_share_in_shifted_buffer": MIN_BAIDU_SHARE_IN_SHIFTED_BUFFER,
        "n_selected_baidu_segments": int(len(selected_baidu_m)),
        "n_selected_baidu_segments_with_speed": int(selected_for_csv["has_speed_in_baidu_week"].sum()),
        "selected_baidu_speed_dates": sorted(
            int(day) for day in baidu_speed_raw["day"].dropna().astype(int).unique()
        )
        if not baidu_speed_raw.empty
        else [],
        "gaode_speed_dates": sorted(combined.loc[combined["source_period"].str.startswith("gaode"), "date"].dt.strftime("%Y%m%d").unique().tolist()),
    }
    with (dirs["metrics"] / "baidu_gaode_shifted_overlap_compare_metadata.json").open(
        "w", encoding="utf-8"
    ) as fh:
        json.dump(metadata, fh, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
