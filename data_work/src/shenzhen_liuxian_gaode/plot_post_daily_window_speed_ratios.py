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
SOURCE_PERIOD = "gaode_post"
CHAIN_LABELS = {
    "eastbound_chain": "SW -> NE",
    "westbound_chain": "NE -> SW",
}
CHAIN_COLORS = {
    "eastbound_chain": "#c44e52",
    "westbound_chain": "#4c78a8",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot post-week daily active/reference speed ratios by direction."
    )
    parser.add_argument("--compare-panel", type=Path, default=DEFAULT_COMPARE_PANEL)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def filter_window(df: pd.DataFrame, window: tuple[int, int]) -> pd.DataFrame:
    lo, hi = window
    return df[(df["minute_of_day"] >= lo) & (df["minute_of_day"] < hi)].copy()


def build_daily_ratios(compare_panel: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    panel = pd.read_csv(compare_panel, parse_dates=["date", "datetime"])
    post = panel[
        panel["source_period"].eq(SOURCE_PERIOD) & (panel["datetime"].dt.dayofweek < 5)
    ].copy()
    if post.empty:
        raise ValueError("No post-week weekday observations found in the comparison panel.")

    rows = []
    for window_name, window in [
        ("reference_1500_1730", REFERENCE_WINDOW),
        ("active_1730_2000", ACTIVE_WINDOW),
    ]:
        sub = filter_window(post, window)
        grouped = (
            sub.groupby(["date", "dow_name", "chain_direction"], as_index=False)
            .agg(
                mean_speed_kmh=("speed_kmh", "mean"),
                median_speed_kmh=("speed_kmh", "median"),
                n_10min=("speed_kmh", "size"),
            )
        )
        grouped["window"] = window_name
        rows.append(grouped)
    long = pd.concat(rows, ignore_index=True)

    within = long.pivot_table(
        index=["date", "dow_name", "chain_direction"],
        columns="window",
        values="mean_speed_kmh",
        aggfunc="first",
    ).reset_index()
    within["active_over_reference"] = (
        within["active_1730_2000"] / within["reference_1500_1730"]
    )
    within["active_speed_drop_pct"] = 100 * (1 - within["active_over_reference"])
    within["chain_label"] = within["chain_direction"].map(CHAIN_LABELS)
    within = within.sort_values(["date", "chain_direction"]).copy()

    direction = long.pivot_table(
        index=["date", "dow_name", "window"],
        columns="chain_direction",
        values="mean_speed_kmh",
        aggfunc="first",
    ).reset_index()
    direction["swne_over_nesw_speed_ratio"] = (
        direction["eastbound_chain"] / direction["westbound_chain"]
    )
    direction["window_label"] = direction["window"].map(
        {
            "reference_1500_1730": "15:00-17:30",
            "active_1730_2000": "17:30-20:00",
        }
    )
    direction = direction.sort_values(["date", "window"]).copy()

    rr = within.pivot_table(
        index=["date", "dow_name"],
        columns="chain_direction",
        values="active_over_reference",
        aggfunc="first",
    ).reset_index()
    rr["ratio_of_ratios_swne_over_nesw"] = (
        rr["eastbound_chain"] / rr["westbound_chain"]
    )
    rr["relative_change_pct"] = 100 * (rr["ratio_of_ratios_swne_over_nesw"] - 1)
    rr = rr.sort_values("date").copy()
    return within, direction, rr


def plot_daily_ratios(
    within: pd.DataFrame,
    direction: pd.DataFrame,
    rr: pd.DataFrame,
    output_path: Path,
) -> None:
    dates = rr["date"].dt.strftime("%m-%d\n%a").tolist()
    x = np.arange(len(dates))
    width = 0.34

    fig, axes = plt.subplots(1, 3, figsize=(13.2, 4.1))

    ax = axes[0]
    for offset, chain in [(-width / 2, "eastbound_chain"), (width / 2, "westbound_chain")]:
        sub = within[within["chain_direction"].eq(chain)].sort_values("date")
        ax.bar(
            x + offset,
            sub["active_over_reference"],
            width=width,
            color=CHAIN_COLORS[chain],
            alpha=0.86,
            label=CHAIN_LABELS[chain],
        )
    ax.axhline(1, color="#333333", linewidth=0.8, linestyle="--")
    ax.set_title("a. Active / reference speed", loc="left", fontweight="bold")
    ax.set_ylabel("17:30-20:00 speed / 15:00-17:30 speed")
    ax.set_xticks(x)
    ax.set_xticklabels(dates, fontsize=8)
    ax.set_ylim(0.4, 1.05)
    ax.legend(frameon=False, loc="lower left")
    ax.grid(axis="y", color="#e0e0e0", linewidth=0.6)

    ax = axes[1]
    for window, color, marker in [
        ("reference_1500_1730", "#777777", "o"),
        ("active_1730_2000", "#111111", "s"),
    ]:
        sub = direction[direction["window"].eq(window)].sort_values("date")
        ax.plot(
            x,
            sub["swne_over_nesw_speed_ratio"],
            color=color,
            marker=marker,
            linewidth=1.6,
            markersize=4.5,
            label=sub["window_label"].iloc[0],
        )
    ax.axhline(1, color="#333333", linewidth=0.8, linestyle="--")
    ax.set_title("b. Directional speed ratio", loc="left", fontweight="bold")
    ax.set_ylabel("SW -> NE speed / NE -> SW speed")
    ax.set_xticks(x)
    ax.set_xticklabels(dates, fontsize=8)
    ax.set_ylim(0.9, max(1.95, direction["swne_over_nesw_speed_ratio"].max() + 0.08))
    ax.legend(frameon=False, loc="upper left")
    ax.grid(axis="y", color="#e0e0e0", linewidth=0.6)

    ax = axes[2]
    colors = np.where(rr["ratio_of_ratios_swne_over_nesw"] >= 1, "#238b45", "#cb4b4b")
    ax.bar(x, rr["ratio_of_ratios_swne_over_nesw"], color=colors, alpha=0.86)
    ax.axhline(1, color="#333333", linewidth=0.8, linestyle="--")
    ax.set_title("c. Ratio of ratios", loc="left", fontweight="bold")
    ax.set_ylabel("(SW->NE active/ref) / (NE->SW active/ref)")
    ax.set_xticks(x)
    ax.set_xticklabels(dates, fontsize=8)
    ax.set_ylim(0.75, max(1.8, rr["ratio_of_ratios_swne_over_nesw"].max() + 0.12))
    ax.grid(axis="y", color="#e0e0e0", linewidth=0.6)

    for ax in axes:
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    fig.suptitle(
        "Gaode post week daily speed ratios around the tidal-lane window",
        x=0.01,
        ha="left",
    )
    fig.text(
        0.01,
        0.01,
        "Notes: Speeds are length-weighted harmonic corridor speeds from the shifted-overlap comparison panel. "
        "Values below 1 in panel a mean the active window is slower than 15:00-17:30. "
        "Panel c above 1 means SW->NE slows less, relative to its own 15:00-17:30 speed, than NE->SW.",
        fontsize=7.5,
        color="#444444",
    )
    fig.tight_layout(rect=(0, 0.11, 1, 0.9))
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    args = parse_args()
    configure_plot()
    dirs = ensure_output_dirs(args.output_dir)

    within, direction, rr = build_daily_ratios(args.compare_panel)
    within.to_csv(
        dirs["data"] / "gaode_post_daily_active_reference_speed_ratios.csv",
        index=False,
        encoding="utf-8-sig",
    )
    direction.to_csv(
        dirs["data"] / "gaode_post_daily_direction_speed_ratios_by_window.csv",
        index=False,
        encoding="utf-8-sig",
    )
    rr.to_csv(
        dirs["metrics"] / "gaode_post_daily_ratio_of_ratios.csv",
        index=False,
        encoding="utf-8-sig",
    )
    plot_daily_ratios(
        within,
        direction,
        rr,
        dirs["figures"] / "gaode_post_daily_window_speed_ratios.png",
    )
    with (dirs["metrics"] / "gaode_post_daily_window_speed_ratios_metadata.json").open(
        "w", encoding="utf-8"
    ) as fh:
        json.dump(
            {
                "source_period": SOURCE_PERIOD,
                "reference_window": "15:00-17:30",
                "active_window": "17:30-20:00",
                "input_panel": str(args.compare_panel),
                "figure": str(dirs["figures"] / "gaode_post_daily_window_speed_ratios.png"),
            },
            fh,
            ensure_ascii=False,
            indent=2,
        )


if __name__ == "__main__":
    main()
