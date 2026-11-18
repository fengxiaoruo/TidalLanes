from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from event_study_liuxian import (
    DEFAULT_INPUT_DIR,
    DEFAULT_OUTPUT_DIR,
    POLICY_DATE,
    configure_plot,
    ensure_output_dirs,
    load_panel,
)


WINDOWS = {
    "pre_window_1500_1730": (15 * 60, 17 * 60 + 30),
    "active_window_1730_2000": (17 * 60 + 30, 20 * 60),
}

WINDOW_LABELS = {
    "pre_window_1500_1730": "15:00-17:30, no tidal lane",
    "active_window_1730_2000": "17:30-20:00, tidal lane window",
}

CHAIN_LABELS = {
    "eastbound_chain": "Eastbound chain (SW -> NE, toward Fulong Rd)",
    "westbound_chain": "Westbound chain (NE -> SW, toward Xueyuan Ave)",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot eastbound/westbound weekday speed by pre-window and active-window."
    )
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def parse_coord(coord: str) -> tuple[float, float]:
    lon, lat = coord.split(",")
    return float(lon), float(lat)


def bearing_degrees(start: str, end: str) -> float:
    lon1, lat1 = parse_coord(start)
    lon2, lat2 = parse_coord(end)
    dy = (lat2 - lat1) * 111_320
    dx = (lon2 - lon1) * 111_320 * math.cos(math.radians((lat1 + lat2) / 2))
    return (math.degrees(math.atan2(dx, dy)) + 360) % 360


def assign_chain_direction(panel: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    segment_cols = [
        "segment_id",
        "路段编号",
        "路段名称（唯一）",
        "路段方向",
        "路段长度",
        "路段起点坐标",
        "路段终点坐标",
    ]
    segment_inventory = panel[segment_cols].drop_duplicates().copy()
    segment_inventory["bearing_deg"] = segment_inventory.apply(
        lambda row: bearing_degrees(row["路段起点坐标"], row["路段终点坐标"]),
        axis=1,
    )
    segment_inventory["chain_direction"] = np.where(
        (segment_inventory["bearing_deg"] > 0) & (segment_inventory["bearing_deg"] < 180),
        "eastbound_chain",
        "westbound_chain",
    )
    segment_inventory["chain_label"] = segment_inventory["chain_direction"].map(CHAIN_LABELS)
    segment_inventory = segment_inventory.sort_values(["chain_direction", "bearing_deg", "路段编号"])

    mapping = segment_inventory.set_index("segment_id")["chain_direction"]
    out = panel.copy()
    out["chain_direction"] = out["segment_id"].map(mapping)
    out["chain_label"] = out["chain_direction"].map(CHAIN_LABELS)
    return out, segment_inventory


def build_chain_corridor(panel: pd.DataFrame) -> pd.DataFrame:
    expected_counts = panel.groupby("chain_direction")["segment_id"].nunique().to_dict()
    corridor = (
        panel.groupby(
            [
                "chain_direction",
                "chain_label",
                "period",
                "date",
                "dow",
                "datetime",
                "minute_of_day",
            ],
            as_index=False,
        )
        .agg(
            travel_time_min=("segment_travel_time_min", "sum"),
            total_length_m=("segment_length_m", "sum"),
            mean_segment_speed_kmh=("speed_kmh", "mean"),
            n_segments=("segment_id", "nunique"),
        )
        .sort_values(["chain_direction", "datetime"])
    )
    corridor = corridor[
        corridor.apply(
            lambda row: row["n_segments"] == expected_counts[row["chain_direction"]],
            axis=1,
        )
    ].copy()
    corridor["corridor_speed_kmh"] = (
        corridor["total_length_m"] / 1000.0 / (corridor["travel_time_min"] / 60.0)
    )
    corridor["period_short"] = np.where(corridor["date"] < POLICY_DATE, "pre", "post")
    corridor["event_day"] = (corridor["date"] - POLICY_DATE).dt.days
    corridor["date_label"] = corridor["date"].dt.strftime("%m-%d")
    corridor["dow_name"] = corridor["datetime"].dt.day_name()
    return corridor


def build_daily_window_summary(corridor: pd.DataFrame) -> pd.DataFrame:
    rows = []
    weekdays = corridor[corridor["dow"] < 5].copy()
    for window, (lo, hi) in WINDOWS.items():
        sub = weekdays[(weekdays["minute_of_day"] >= lo) & (weekdays["minute_of_day"] < hi)]
        grouped = (
            sub.groupby(
                [
                    "chain_direction",
                    "chain_label",
                    "period",
                    "period_short",
                    "date",
                    "date_label",
                    "dow",
                    "dow_name",
                    "event_day",
                ],
                as_index=False,
            )
            .agg(
                corridor_speed_kmh=("corridor_speed_kmh", "mean"),
                mean_segment_speed_kmh=("mean_segment_speed_kmh", "mean"),
                travel_time_min=("travel_time_min", "mean"),
                n_10min=("corridor_speed_kmh", "size"),
            )
        )
        grouped["window"] = window
        grouped["window_label"] = WINDOW_LABELS[window]
        rows.append(grouped)
    return pd.concat(rows, ignore_index=True)


def build_period_summary(daily: pd.DataFrame) -> pd.DataFrame:
    summary = (
        daily.groupby(["chain_direction", "chain_label", "window", "window_label", "period_short"], as_index=False)
        .agg(
            mean_daily_corridor_speed_kmh=("corridor_speed_kmh", "mean"),
            mean_daily_travel_time_min=("travel_time_min", "mean"),
            n_days=("date", "nunique"),
        )
    )
    wide = summary.pivot_table(
        index=["chain_direction", "chain_label", "window", "window_label"],
        columns="period_short",
        values=["mean_daily_corridor_speed_kmh", "mean_daily_travel_time_min"],
        aggfunc="first",
    )
    wide.columns = [f"{metric}_{period}" for metric, period in wide.columns]
    wide = wide.reset_index()
    wide["post_minus_pre_speed_kmh"] = (
        wide["mean_daily_corridor_speed_kmh_post"]
        - wide["mean_daily_corridor_speed_kmh_pre"]
    )
    wide["post_minus_pre_travel_time_min"] = (
        wide["mean_daily_travel_time_min_post"]
        - wide["mean_daily_travel_time_min_pre"]
    )
    return wide.sort_values(["chain_direction", "window"])


def plot_daily_windows(daily: pd.DataFrame, output_path: Path) -> None:
    colors = {
        "pre_window_1500_1730": "#4c78a8",
        "active_window_1730_2000": "#c44e52",
    }
    fig, axes = plt.subplots(2, 1, figsize=(10.6, 6.2), sharex=True)
    chain_order = ["eastbound_chain", "westbound_chain"]

    for ax, chain in zip(axes, chain_order):
        chain_data = daily[daily["chain_direction"] == chain]
        for window, label in WINDOW_LABELS.items():
            for period_short in ["pre", "post"]:
                part = chain_data[
                    (chain_data["window"] == window)
                    & (chain_data["period_short"] == period_short)
                ].sort_values("event_day")
                if part.empty:
                    continue
                ax.plot(
                    part["event_day"],
                    part["corridor_speed_kmh"],
                    marker="o",
                    linewidth=1.4,
                    markersize=4.0,
                    color=colors[window],
                    alpha=0.95,
                    label=label if period_short == "pre" else None,
                )
        ax.axhline(0, color="#888888", linewidth=0.6, alpha=0.4)
        ax.axvline(0, color="#333333", linewidth=0.9, linestyle="--")
        ax.set_title(CHAIN_LABELS[chain], loc="left", fontweight="bold")
        ax.set_ylabel("Corridor speed (km/h)")
        ax.grid(axis="y", color="#d9d9d9", linewidth=0.6, alpha=0.8)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    observed = daily[["event_day", "date_label", "dow_name"]].drop_duplicates().sort_values("event_day")
    tick_labels = [f"{row.date_label}\n{row.dow_name[:3]}" for row in observed.itertuples(index=False)]
    axes[-1].set_xticks(observed["event_day"])
    axes[-1].set_xticklabels(tick_labels, fontsize=8, rotation=35, ha="right")
    axes[-1].set_xlabel("Weekday date; dashed line marks 2026-03-23 policy start")
    axes[0].legend(loc="upper right", frameon=False)
    fig.suptitle("Liuxian Avenue daily weekday speed by chain direction and time window", y=0.98)
    fig.text(
        0.01,
        0.01,
        "Notes: Direction chains are inferred from connected segment coordinates. "
        "Speed is length-weighted harmonic corridor speed over balanced common segments. "
        "Post-only endpoint segments are excluded for pre/post comparability.",
        fontsize=7.5,
        color="#444444",
    )
    fig.tight_layout(rect=(0, 0.075, 1, 0.95))
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)


def plot_period_change(summary: pd.DataFrame, output_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(7.6, 4.4))
    plot_data = summary.copy()
    plot_data["x_label"] = plot_data["chain_direction"].map(
        {
            "eastbound_chain": "Eastbound",
            "westbound_chain": "Westbound",
        }
    )
    window_order = ["pre_window_1500_1730", "active_window_1730_2000"]
    x = np.arange(plot_data["x_label"].nunique())
    width = 0.34
    colors = {
        "pre_window_1500_1730": "#4c78a8",
        "active_window_1730_2000": "#c44e52",
    }
    for idx, window in enumerate(window_order):
        part = plot_data[plot_data["window"] == window].sort_values("chain_direction")
        ax.bar(
            x + (idx - 0.5) * width,
            part["post_minus_pre_speed_kmh"],
            width=width,
            color=colors[window],
            label=WINDOW_LABELS[window],
        )
    ax.axhline(0, color="#555555", linewidth=0.8)
    ax.set_xticks(x)
    ax.set_xticklabels(["Eastbound", "Westbound"])
    ax.set_ylabel("Post minus pre speed (km/h)")
    ax.set_title("Post-pre speed change by chain and time window", loc="left", fontweight="bold")
    ax.grid(axis="y", color="#d9d9d9", linewidth=0.6, alpha=0.8)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    args = parse_args()
    configure_plot()
    dirs = ensure_output_dirs(args.output_dir)

    panel = load_panel(args.input_dir)
    panel, segment_inventory = assign_chain_direction(panel)
    corridor = build_chain_corridor(panel)
    daily = build_daily_window_summary(corridor)
    summary = build_period_summary(daily)

    segment_inventory.to_csv(
        dirs["metrics"] / "chain_direction_segment_inventory_common.csv",
        index=False,
        encoding="utf-8-sig",
    )
    daily.to_csv(
        dirs["data"] / "chain_direction_daily_weekday_window_speeds.csv",
        index=False,
        encoding="utf-8-sig",
    )
    summary.to_csv(
        dirs["metrics"] / "chain_direction_weekday_window_speed_summary.csv",
        index=False,
        encoding="utf-8-sig",
    )
    with (dirs["metrics"] / "chain_direction_window_speed_metadata.json").open(
        "w", encoding="utf-8"
    ) as fh:
        json.dump(
            {
                "policy_date": POLICY_DATE.strftime("%Y-%m-%d"),
                "windows": WINDOW_LABELS,
                "chain_labels": CHAIN_LABELS,
                "sample": "weekdays only; balanced common segments present in pre and post",
                "speed_measure": "length-weighted harmonic corridor speed by chain and 10-minute timestamp",
                "n_common_segments_by_chain": corridor.groupby("chain_direction")["n_segments"].max().to_dict(),
            },
            fh,
            ensure_ascii=False,
            indent=2,
        )

    plot_daily_windows(
        daily,
        dirs["figures"] / "chain_direction_daily_weekday_window_speeds.png",
    )
    plot_period_change(
        summary,
        dirs["figures"] / "chain_direction_weekday_window_speed_changes.png",
    )


if __name__ == "__main__":
    main()
