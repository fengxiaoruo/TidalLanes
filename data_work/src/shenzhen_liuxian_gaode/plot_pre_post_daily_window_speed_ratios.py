from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from event_study_liuxian import DEFAULT_OUTPUT_DIR, configure_plot, ensure_output_dirs


DEFAULT_COMPARE_PANEL = (
    DEFAULT_OUTPUT_DIR / "data/baidu_gaode_shifted_overlap_weekly_10min_speed_compare.csv"
)

REFERENCE_WINDOW = (15 * 60, 17 * 60 + 30)
ACTIVE_WINDOW = (17 * 60 + 30, 20 * 60)
SOURCE_PERIODS = ["gaode_pre", "gaode_post"]
SOURCE_LABELS = {
    "gaode_pre": "Pre week",
    "gaode_post": "Post week",
}
CHAIN_LABELS = {
    "eastbound_chain": "SW -> NE",
    "westbound_chain": "NE -> SW",
}
CHAIN_ORDER = ["eastbound_chain", "westbound_chain"]
PERIOD_COLORS = {
    "gaode_pre": "#8da0cb",
    "gaode_post": "#d95f02",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare pre and post daily active/reference speed ratios."
    )
    parser.add_argument("--compare-panel", type=Path, default=DEFAULT_COMPARE_PANEL)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def filter_window(df: pd.DataFrame, window: tuple[int, int]) -> pd.DataFrame:
    lo, hi = window
    return df[(df["minute_of_day"] >= lo) & (df["minute_of_day"] < hi)].copy()


def build_ratios(compare_panel: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    panel = pd.read_csv(compare_panel, parse_dates=["date", "datetime"])
    panel = panel[
        panel["source_period"].isin(SOURCE_PERIODS) & (panel["datetime"].dt.dayofweek < 5)
    ].copy()
    rows = []
    for window_name, window in [
        ("reference_1500_1730", REFERENCE_WINDOW),
        ("active_1730_2000", ACTIVE_WINDOW),
    ]:
        sub = filter_window(panel, window)
        grouped = (
            sub.groupby(["source_period", "date", "dow_name", "chain_direction"], as_index=False)
            .agg(
                mean_speed_kmh=("speed_kmh", "mean"),
                n_10min=("speed_kmh", "size"),
            )
        )
        grouped["window"] = window_name
        rows.append(grouped)
    long = pd.concat(rows, ignore_index=True)
    daily = long.pivot_table(
        index=["source_period", "date", "dow_name", "chain_direction"],
        columns="window",
        values="mean_speed_kmh",
        aggfunc="first",
    ).reset_index()
    daily["active_over_reference"] = (
        daily["active_1730_2000"] / daily["reference_1500_1730"]
    )
    daily["active_speed_drop_pct"] = 100 * (1 - daily["active_over_reference"])
    daily["weekday_num"] = daily["date"].dt.dayofweek
    daily["weekday_label"] = daily["date"].dt.strftime("%a")
    daily["source_label"] = daily["source_period"].map(SOURCE_LABELS)
    daily["chain_label"] = daily["chain_direction"].map(CHAIN_LABELS)
    daily = daily.sort_values(["chain_direction", "weekday_num", "source_period"]).copy()

    summary = (
        daily.groupby(["source_period", "source_label", "chain_direction", "chain_label"], as_index=False)
        .agg(
            mean_active_over_reference=("active_over_reference", "mean"),
            median_active_over_reference=("active_over_reference", "median"),
            mean_speed_drop_pct=("active_speed_drop_pct", "mean"),
            n_weekdays=("date", "nunique"),
        )
        .sort_values(["chain_direction", "source_period"])
    )
    return daily, summary


def plot_ratios(daily: pd.DataFrame, summary: pd.DataFrame, output_path: Path) -> None:
    weekdays = ["Mon", "Tue", "Wed", "Thu", "Fri"]
    x = np.arange(len(weekdays))
    width = 0.34
    fig, axes = plt.subplots(1, 3, figsize=(13.0, 4.1), gridspec_kw={"width_ratios": [1, 1, 0.9]})

    for ax, chain in zip(axes[:2], CHAIN_ORDER):
        sub_chain = daily[daily["chain_direction"].eq(chain)]
        for offset, source in [(-width / 2, "gaode_pre"), (width / 2, "gaode_post")]:
            sub = sub_chain[sub_chain["source_period"].eq(source)].sort_values("weekday_num")
            ax.bar(
                x + offset,
                sub["active_over_reference"],
                width=width,
                color=PERIOD_COLORS[source],
                alpha=0.84,
                label=SOURCE_LABELS[source],
            )
        ax.axhline(1, color="#333333", linewidth=0.8, linestyle="--")
        ax.set_title(CHAIN_LABELS[chain], loc="left", fontweight="bold")
        ax.set_xticks(x)
        ax.set_xticklabels(weekdays)
        ax.set_ylim(0.4, 1.12)
        ax.grid(axis="y", color="#e0e0e0", linewidth=0.6)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
    axes[0].set_ylabel("17:30-20:00 speed / 15:00-17:30 speed")
    axes[1].legend(frameon=False, loc="lower left")

    ax = axes[2]
    y_positions = np.arange(len(CHAIN_ORDER))
    for source, marker in [("gaode_pre", "o"), ("gaode_post", "s")]:
        sub = summary[summary["source_period"].eq(source)].set_index("chain_direction").loc[CHAIN_ORDER]
        ax.scatter(
            sub["mean_active_over_reference"],
            y_positions,
            s=52,
            color=PERIOD_COLORS[source],
            marker=marker,
            label=SOURCE_LABELS[source],
            zorder=3,
        )
        for y, value in zip(y_positions, sub["mean_active_over_reference"]):
            ax.plot([1, value], [y, y], color=PERIOD_COLORS[source], linewidth=1.6, alpha=0.65)
    ax.axvline(1, color="#333333", linewidth=0.8, linestyle="--")
    ax.set_yticks(y_positions)
    ax.set_yticklabels([CHAIN_LABELS[c] for c in CHAIN_ORDER])
    ax.set_xlim(0.55, 1.05)
    ax.set_title("Week average", loc="left", fontweight="bold")
    ax.set_xlabel("Active / reference speed")
    ax.grid(axis="x", color="#e0e0e0", linewidth=0.6)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.legend(frameon=False, loc="lower left")

    fig.suptitle(
        "Pre and post daily speed ratios around the tidal-lane window",
        x=0.01,
        ha="left",
    )
    fig.text(
        0.01,
        0.01,
        "Notes: Values below 1 mean 17:30-20:00 is slower than 15:00-17:30. "
        "Speeds are length-weighted harmonic corridor speeds from the shifted-overlap comparison panel.",
        fontsize=7.5,
        color="#444444",
    )
    fig.tight_layout(rect=(0, 0.1, 1, 0.9))
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    args = parse_args()
    configure_plot()
    dirs = ensure_output_dirs(args.output_dir)

    daily, summary = build_ratios(args.compare_panel)
    daily.to_csv(
        dirs["data"] / "gaode_pre_post_daily_active_reference_speed_ratios.csv",
        index=False,
        encoding="utf-8-sig",
    )
    summary.to_csv(
        dirs["metrics"] / "gaode_pre_post_active_reference_speed_ratio_summary.csv",
        index=False,
        encoding="utf-8-sig",
    )
    plot_ratios(
        daily,
        summary,
        dirs["figures"] / "gaode_pre_post_daily_active_reference_speed_ratios.png",
    )
    with (dirs["metrics"] / "gaode_pre_post_daily_window_speed_ratios_metadata.json").open(
        "w", encoding="utf-8"
    ) as fh:
        json.dump(
            {
                "reference_window": "15:00-17:30",
                "active_window": "17:30-20:00",
                "input_panel": str(args.compare_panel),
                "figure": str(dirs["figures"] / "gaode_pre_post_daily_active_reference_speed_ratios.png"),
            },
            fh,
            ensure_ascii=False,
            indent=2,
        )


if __name__ == "__main__":
    main()
