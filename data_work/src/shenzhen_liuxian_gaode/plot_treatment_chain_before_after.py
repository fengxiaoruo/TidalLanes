from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from event_study_liuxian import DEFAULT_OUTPUT_DIR, configure_plot, ensure_output_dirs


DEFAULT_DAILY_PANEL = (
    DEFAULT_OUTPUT_DIR / "data/chain_direction_daily_weekday_window_speeds.csv"
)

TREATMENT_CHAIN = "eastbound_chain"
TREATMENT_LABEL = "SW -> NE treatment chain"

WINDOW_ORDER = ["pre_window_1500_1730", "active_window_1730_2000"]
WINDOW_LABELS = {
    "pre_window_1500_1730": "15:00-17:30",
    "active_window_1730_2000": "17:30-20:00",
}
WINDOW_COLORS = {
    "pre_window_1500_1730": "#4c78a8",
    "active_window_1730_2000": "#c44e52",
}
PERIOD_LABELS = {"pre": "Pre week", "post": "Post week"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot treatment-chain-only pre/post weekday speed changes."
    )
    parser.add_argument("--daily-panel", type=Path, default=DEFAULT_DAILY_PANEL)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def load_treatment_daily(daily_panel: Path) -> pd.DataFrame:
    if not daily_panel.exists():
        raise FileNotFoundError(
            f"Missing daily panel: {daily_panel}. Run plot_chain_window_speeds.py first."
        )
    daily = pd.read_csv(daily_panel, parse_dates=["date"])
    required = {
        "chain_direction",
        "period_short",
        "date",
        "date_label",
        "dow_name",
        "corridor_speed_kmh",
        "travel_time_min",
        "window",
    }
    missing = sorted(required.difference(daily.columns))
    if missing:
        raise ValueError(f"Daily panel is missing required columns: {missing}")

    treatment = daily[
        daily["chain_direction"].eq(TREATMENT_CHAIN)
        & daily["window"].isin(WINDOW_ORDER)
        & daily["period_short"].isin(["pre", "post"])
    ].copy()
    if treatment.empty:
        raise ValueError(f"No rows found for treatment chain {TREATMENT_CHAIN}.")

    treatment["period_order"] = treatment["period_short"].map({"pre": 0, "post": 1})
    treatment = treatment.sort_values(["period_order", "date", "window"]).copy()
    date_order = (
        treatment[["period_short", "date", "date_label", "dow_name", "period_order"]]
        .drop_duplicates()
        .sort_values(["period_order", "date"])
        .reset_index(drop=True)
    )
    date_order["plot_pos"] = np.arange(len(date_order))
    treatment = treatment.merge(
        date_order[["period_short", "date", "plot_pos"]],
        on=["period_short", "date"],
        how="left",
    )
    treatment["period_label"] = treatment["period_short"].map(PERIOD_LABELS)
    treatment["window_label_short"] = treatment["window"].map(WINDOW_LABELS)
    return treatment


def build_period_summary(treatment: pd.DataFrame) -> pd.DataFrame:
    summary = (
        treatment.groupby(["window", "window_label_short", "period_short"], as_index=False)
        .agg(
            mean_daily_speed_kmh=("corridor_speed_kmh", "mean"),
            mean_daily_travel_time_min=("travel_time_min", "mean"),
            n_weekdays=("date", "nunique"),
        )
    )
    wide = summary.pivot_table(
        index=["window", "window_label_short"],
        columns="period_short",
        values=["mean_daily_speed_kmh", "mean_daily_travel_time_min", "n_weekdays"],
        aggfunc="first",
    )
    wide.columns = [f"{metric}_{period}" for metric, period in wide.columns]
    wide = wide.reset_index()
    wide["post_minus_pre_speed_kmh"] = (
        wide["mean_daily_speed_kmh_post"] - wide["mean_daily_speed_kmh_pre"]
    )
    wide["speed_change_pct"] = (
        wide["post_minus_pre_speed_kmh"] / wide["mean_daily_speed_kmh_pre"] * 100
    )
    wide["post_minus_pre_travel_time_min"] = (
        wide["mean_daily_travel_time_min_post"]
        - wide["mean_daily_travel_time_min_pre"]
    )
    wide["travel_time_change_pct"] = (
        wide["post_minus_pre_travel_time_min"]
        / wide["mean_daily_travel_time_min_pre"]
        * 100
    )
    wide["post_over_pre_speed"] = (
        wide["mean_daily_speed_kmh_post"] / wide["mean_daily_speed_kmh_pre"]
    )
    return wide.sort_values("window").copy()


def build_daily_active_reference_ratio(treatment: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    daily = treatment.pivot_table(
        index=["period_short", "period_label", "date", "date_label", "dow_name", "plot_pos"],
        columns="window",
        values=["corridor_speed_kmh", "travel_time_min"],
        aggfunc="first",
    )
    daily.columns = [f"{metric}_{window}" for metric, window in daily.columns]
    daily = daily.reset_index().sort_values(["plot_pos"])
    daily["active_over_reference_speed"] = (
        daily["corridor_speed_kmh_active_window_1730_2000"]
        / daily["corridor_speed_kmh_pre_window_1500_1730"]
    )
    daily["active_minus_reference_speed_kmh"] = (
        daily["corridor_speed_kmh_active_window_1730_2000"]
        - daily["corridor_speed_kmh_pre_window_1500_1730"]
    )
    ratio_summary = (
        daily.groupby(["period_short", "period_label"], as_index=False)
        .agg(
            mean_active_over_reference_speed=("active_over_reference_speed", "mean"),
            median_active_over_reference_speed=("active_over_reference_speed", "median"),
            mean_active_minus_reference_speed_kmh=(
                "active_minus_reference_speed_kmh",
                "mean",
            ),
            n_weekdays=("date", "nunique"),
        )
        .sort_values("period_short")
    )
    return daily, ratio_summary


def plot_before_after(
    treatment: pd.DataFrame,
    ratio_daily: pd.DataFrame,
    period_summary: pd.DataFrame,
    ratio_summary: pd.DataFrame,
    output_path: Path,
) -> None:
    fig, axes = plt.subplots(
        1,
        2,
        figsize=(11.0, 4.45),
        gridspec_kw={"width_ratios": [1.45, 1.0]},
    )

    ax = axes[0]
    for window in WINDOW_ORDER:
        for period in ["pre", "post"]:
            part = treatment[
                treatment["window"].eq(window) & treatment["period_short"].eq(period)
            ].sort_values("plot_pos")
            ax.plot(
                part["plot_pos"],
                part["corridor_speed_kmh"],
                marker="o",
                linewidth=1.55,
                markersize=4.2,
                color=WINDOW_COLORS[window],
                label=WINDOW_LABELS[window] if period == "pre" else None,
            )

    date_labels = (
        treatment[["plot_pos", "period_short", "date_label", "dow_name"]]
        .drop_duplicates()
        .sort_values("plot_pos")
    )
    ax.axvline(4.5, color="#555555", linewidth=0.9, linestyle="--")
    ax.set_xticks(date_labels["plot_pos"])
    ax.set_xticklabels(
        [
            f"{row.period_short.title()}\n{row.date_label}\n{row.dow_name[:3]}"
            for row in date_labels.itertuples(index=False)
        ],
        fontsize=7.5,
    )
    ax.set_ylabel("Corridor speed (km/h)")
    ax.set_title("a. Daily weekday speed", loc="left", fontweight="bold")
    ax.grid(axis="y", color="#e0e0e0", linewidth=0.65)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.legend(frameon=False, loc="upper right")

    ax = axes[1]
    bar_colors = ratio_daily["period_short"].map({"pre": "#8da0cb", "post": "#d95f02"})
    ax.bar(
        ratio_daily["plot_pos"],
        ratio_daily["active_over_reference_speed"],
        color=bar_colors,
        alpha=0.86,
        width=0.72,
    )
    ax.axhline(1, color="#333333", linewidth=0.8, linestyle="--")
    ax.axvline(4.5, color="#555555", linewidth=0.9, linestyle="--")
    ax.set_xticks(date_labels["plot_pos"])
    ax.set_xticklabels(
        [f"{row.date_label}\n{row.dow_name[:3]}" for row in date_labels.itertuples(index=False)],
        fontsize=7.5,
    )
    ax.set_ylim(0.35, 1.12)
    ax.set_ylabel("17:30-20:00 speed / 15:00-17:30 speed")
    ax.set_title("b. Active/reference ratio", loc="left", fontweight="bold")
    ax.grid(axis="y", color="#e0e0e0", linewidth=0.65)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    speed_summary = period_summary.set_index("window")
    active = speed_summary.loc["active_window_1730_2000"]
    reference = speed_summary.loc["pre_window_1500_1730"]
    ratio_lookup = ratio_summary.set_index("period_short")
    annotation = (
        "Window means, km/h\n"
        f"15:00-17:30: {reference['mean_daily_speed_kmh_pre']:.1f} -> "
        f"{reference['mean_daily_speed_kmh_post']:.1f} "
        f"({reference['speed_change_pct']:.1f}%)\n"
        f"17:30-20:00: {active['mean_daily_speed_kmh_pre']:.1f} -> "
        f"{active['mean_daily_speed_kmh_post']:.1f} "
        f"({active['speed_change_pct']:.1f}%)\n"
        "Active/reference ratio\n"
        f"Pre: {ratio_lookup.loc['pre', 'mean_active_over_reference_speed']:.3f}; "
        f"Post: {ratio_lookup.loc['post', 'mean_active_over_reference_speed']:.3f}"
    )
    axes[1].text(
        0.03,
        0.04,
        annotation,
        transform=axes[1].transAxes,
        ha="left",
        va="bottom",
        fontsize=7.7,
        color="#333333",
        bbox={"facecolor": "white", "edgecolor": "#cccccc", "linewidth": 0.5, "alpha": 0.92},
    )

    fig.suptitle(
        f"Treatment direction only: {TREATMENT_LABEL}, pre vs post",
        x=0.01,
        y=0.995,
        ha="left",
        fontsize=12,
        fontweight="bold",
    )
    fig.text(
        0.01,
        0.015,
        "Notes: Uses balanced common Gaode segments observed in both purchased weeks. "
        "Speed is length-weighted harmonic corridor speed by weekday and window. "
        "The dashed vertical line separates the observed pre and post weeks.",
        fontsize=7.4,
        color="#444444",
    )
    fig.tight_layout(rect=(0, 0.08, 1, 0.92))
    fig.savefig(output_path, bbox_inches="tight")
    fig.savefig(output_path.with_suffix(".pdf"), bbox_inches="tight")
    fig.savefig(output_path.with_suffix(".svg"), bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    args = parse_args()
    configure_plot()
    dirs = ensure_output_dirs(args.output_dir)

    treatment = load_treatment_daily(args.daily_panel)
    period_summary = build_period_summary(treatment)
    ratio_daily, ratio_summary = build_daily_active_reference_ratio(treatment)

    treatment.to_csv(
        dirs["data"] / "treatment_chain_before_after_weekday_windows.csv",
        index=False,
        encoding="utf-8-sig",
    )
    ratio_daily.to_csv(
        dirs["data"] / "treatment_chain_active_reference_daily_ratios.csv",
        index=False,
        encoding="utf-8-sig",
    )
    period_summary.to_csv(
        dirs["metrics"] / "treatment_chain_before_after_weekday_summary.csv",
        index=False,
        encoding="utf-8-sig",
    )
    ratio_summary.to_csv(
        dirs["metrics"] / "treatment_chain_active_reference_ratio_summary.csv",
        index=False,
        encoding="utf-8-sig",
    )
    figure_path = dirs["figures"] / "treatment_chain_before_after_weekday_windows.png"
    plot_before_after(treatment, ratio_daily, period_summary, ratio_summary, figure_path)

    metadata = {
        "treatment_chain": TREATMENT_CHAIN,
        "treatment_label": TREATMENT_LABEL,
        "sample": "balanced common Gaode segments observed in both purchased weeks",
        "input_daily_panel": str(args.daily_panel),
        "figure_png": str(figure_path),
        "figure_pdf": str(figure_path.with_suffix(".pdf")),
        "figure_svg": str(figure_path.with_suffix(".svg")),
        "outputs": {
            "daily_windows": str(
                dirs["data"] / "treatment_chain_before_after_weekday_windows.csv"
            ),
            "daily_ratios": str(
                dirs["data"] / "treatment_chain_active_reference_daily_ratios.csv"
            ),
            "period_summary": str(
                dirs["metrics"] / "treatment_chain_before_after_weekday_summary.csv"
            ),
            "ratio_summary": str(
                dirs["metrics"] / "treatment_chain_active_reference_ratio_summary.csv"
            ),
        },
    }
    with (dirs["metrics"] / "treatment_chain_before_after_metadata.json").open(
        "w", encoding="utf-8"
    ) as fh:
        json.dump(metadata, fh, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
