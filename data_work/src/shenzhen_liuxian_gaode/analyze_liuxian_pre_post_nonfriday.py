from __future__ import annotations

import argparse
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from analyze_liuxian_late_opening_shock import (  # noqa: E402
    CHAIN_LABELS,
    DEFAULT_INPUT_DIR,
    DEFAULT_OUTPUT_DIR,
    POLICY_DATE,
    attach_chain_direction,
    build_corridor,
    build_segment_inventory,
    configure_plot,
    ensure_output_dirs,
    filter_segment_scope,
    load_panel,
    save_figure,
)


WINDOWS = {
    "reference_1500_1730": (15 * 60, 17 * 60 + 30),
    "just_before_1700_1730": (17 * 60, 17 * 60 + 30),
    "active_1730_2000": (17 * 60 + 30, 20 * 60),
    "late_active_1820_2000": (18 * 60 + 20, 20 * 60),
}
WINDOW_LABELS = {
    "reference_1500_1730": "15:00-17:30 reference",
    "just_before_1700_1730": "17:00-17:30 just before",
    "active_1730_2000": "17:30-20:00 active",
    "late_active_1820_2000": "18:20-20:00 late active",
}
WEEK_LABELS = {
    "week_20260302_20260308": "Pre: 2026-03-02 to 03-05",
    "week_20260413_20260419": "Post 1: 2026-04-13 to 04-16",
    "week_20260420_20260426": "Post 2: 2026-04-20 to 04-23",
    "post_pooled": "Post pooled: two Mon-Thu weeks",
}
WEEK_COLORS = {
    "week_20260302_20260308": "#4c78a8",
    "week_20260413_20260419": "#54a24b",
    "week_20260420_20260426": "#c44e52",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze Liuxian pre/post patterns after excluding Fridays."
    )
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def build_nonfriday_panel(input_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    panel_raw, _ = load_panel(input_dir)
    inventory = build_segment_inventory(panel_raw)
    panel = attach_chain_direction(panel_raw, inventory)
    common = build_corridor(panel, "common_all_weeks")
    common = common[(common["day_type"] == "Weekday") & (common["dow"] != 4)].copy()
    common = common.sort_values(["purchase_week", "chain_direction", "datetime"])
    return common, inventory


def build_window_daily(corridor: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for window, (lo, hi) in WINDOWS.items():
        sub = corridor[(corridor["minute_of_day"] >= lo) & (corridor["minute_of_day"] < hi)]
        grouped = (
            sub.groupby(
                [
                    "purchase_week",
                    "date_str",
                    "dow",
                    "dow_name",
                    "chain_direction",
                    "chain_label",
                ],
                as_index=False,
            )
            .agg(
                corridor_speed_kmh=("corridor_speed_kmh", "mean"),
                travel_time_min=("travel_time_min", "mean"),
                n_10min=("corridor_speed_kmh", "size"),
            )
        )
        grouped["window"] = window
        grouped["window_label"] = WINDOW_LABELS[window]
        rows.append(grouped)
    return pd.concat(rows, ignore_index=True).sort_values(
        ["purchase_week", "date_str", "chain_direction", "window"]
    )


def build_window_summary(daily: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    summary = (
        daily.groupby(["purchase_week", "chain_direction", "chain_label", "window", "window_label"], as_index=False)
        .agg(
            mean_speed_kmh=("corridor_speed_kmh", "mean"),
            median_speed_kmh=("corridor_speed_kmh", "median"),
            mean_travel_time_min=("travel_time_min", "mean"),
            n_days=("date_str", "nunique"),
        )
        .sort_values(["purchase_week", "chain_direction", "window"])
    )

    wide = daily.pivot_table(
        index=["purchase_week", "date_str", "dow_name", "chain_direction", "chain_label"],
        columns="window",
        values="corridor_speed_kmh",
        aggfunc="first",
    ).reset_index()
    wide.columns.name = None
    wide["active_over_reference_1500"] = wide["active_1730_2000"] / wide["reference_1500_1730"]
    wide["active_minus_reference_1500_kmh"] = wide["active_1730_2000"] - wide["reference_1500_1730"]
    wide["active_over_just_before"] = wide["active_1730_2000"] / wide["just_before_1700_1730"]
    wide["active_minus_just_before_kmh"] = wide["active_1730_2000"] - wide["just_before_1700_1730"]

    ratio_summary = (
        wide.groupby(["purchase_week", "chain_direction", "chain_label"], as_index=False)
        .agg(
            reference_1500_1730_speed_kmh=("reference_1500_1730", "mean"),
            active_1730_2000_speed_kmh=("active_1730_2000", "mean"),
            active_over_reference_1500=("active_over_reference_1500", "mean"),
            active_minus_reference_1500_kmh=("active_minus_reference_1500_kmh", "mean"),
            just_before_1700_1730_speed_kmh=("just_before_1700_1730", "mean"),
            active_over_just_before=("active_over_just_before", "mean"),
            active_minus_just_before_kmh=("active_minus_just_before_kmh", "mean"),
            n_days=("date_str", "nunique"),
        )
        .sort_values(["purchase_week", "chain_direction"])
    )

    gap = wide.pivot_table(
        index=["purchase_week", "date_str", "dow_name"],
        columns="chain_direction",
        values=["reference_1500_1730", "just_before_1700_1730", "active_1730_2000"],
        aggfunc="first",
    )
    gap.columns = [f"{metric}_{chain}" for metric, chain in gap.columns]
    gap = gap.reset_index()
    gap["active_gap_kmh"] = gap["active_1730_2000_eastbound_chain"] - gap["active_1730_2000_westbound_chain"]
    gap["reference_gap_kmh"] = (
        gap["reference_1500_1730_eastbound_chain"] - gap["reference_1500_1730_westbound_chain"]
    )
    gap["just_before_gap_kmh"] = (
        gap["just_before_1700_1730_eastbound_chain"] - gap["just_before_1700_1730_westbound_chain"]
    )
    gap["active_minus_reference_gap_kmh"] = gap["active_gap_kmh"] - gap["reference_gap_kmh"]
    gap["active_minus_just_before_gap_kmh"] = gap["active_gap_kmh"] - gap["just_before_gap_kmh"]
    gap_summary = (
        gap.groupby("purchase_week", as_index=False)
        .agg(
            active_gap_kmh=("active_gap_kmh", "mean"),
            reference_gap_kmh=("reference_gap_kmh", "mean"),
            just_before_gap_kmh=("just_before_gap_kmh", "mean"),
            active_minus_reference_gap_kmh=("active_minus_reference_gap_kmh", "mean"),
            active_minus_just_before_gap_kmh=("active_minus_just_before_gap_kmh", "mean"),
            n_days=("date_str", "nunique"),
        )
        .sort_values("purchase_week")
    )
    pre_ref = gap_summary.loc[
        gap_summary["purchase_week"].eq("week_20260302_20260308"),
        "active_minus_reference_gap_kmh",
    ].iloc[0]
    pre_before = gap_summary.loc[
        gap_summary["purchase_week"].eq("week_20260302_20260308"),
        "active_minus_just_before_gap_kmh",
    ].iloc[0]
    gap_summary["post_minus_pre_active_reference_ddd_kmh"] = (
        gap_summary["active_minus_reference_gap_kmh"] - pre_ref
    )
    gap_summary["post_minus_pre_active_just_before_ddd_kmh"] = (
        gap_summary["active_minus_just_before_gap_kmh"] - pre_before
    )
    return summary, ratio_summary, gap_summary


def build_pooled_post_summary(ratio_summary: pd.DataFrame, gap_summary: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    post_ratio = (
        ratio_summary[ratio_summary["purchase_week"].ne("week_20260302_20260308")]
        .groupby(["chain_direction", "chain_label"], as_index=False)
        .agg(
            reference_1500_1730_speed_kmh=("reference_1500_1730_speed_kmh", "mean"),
            active_1730_2000_speed_kmh=("active_1730_2000_speed_kmh", "mean"),
            active_over_reference_1500=("active_over_reference_1500", "mean"),
            active_minus_reference_1500_kmh=("active_minus_reference_1500_kmh", "mean"),
            just_before_1700_1730_speed_kmh=("just_before_1700_1730_speed_kmh", "mean"),
            active_over_just_before=("active_over_just_before", "mean"),
            active_minus_just_before_kmh=("active_minus_just_before_kmh", "mean"),
            n_days=("n_days", "sum"),
        )
    )
    post_ratio["purchase_week"] = "post_pooled"
    post_ratio = pd.concat([ratio_summary, post_ratio[ratio_summary.columns]], ignore_index=True)

    post_gap = (
        gap_summary[gap_summary["purchase_week"].ne("week_20260302_20260308")]
        .drop(columns=["post_minus_pre_active_reference_ddd_kmh", "post_minus_pre_active_just_before_ddd_kmh"])
        .mean(numeric_only=True)
        .to_frame()
        .T
    )
    post_gap["purchase_week"] = "post_pooled"
    post_gap["n_days"] = 8
    pre_ref = gap_summary.loc[
        gap_summary["purchase_week"].eq("week_20260302_20260308"),
        "active_minus_reference_gap_kmh",
    ].iloc[0]
    pre_before = gap_summary.loc[
        gap_summary["purchase_week"].eq("week_20260302_20260308"),
        "active_minus_just_before_gap_kmh",
    ].iloc[0]
    post_gap["post_minus_pre_active_reference_ddd_kmh"] = (
        post_gap["active_minus_reference_gap_kmh"] - pre_ref
    )
    post_gap["post_minus_pre_active_just_before_ddd_kmh"] = (
        post_gap["active_minus_just_before_gap_kmh"] - pre_before
    )
    gap_with_pooled = pd.concat([gap_summary, post_gap[gap_summary.columns]], ignore_index=True)
    return post_ratio, gap_with_pooled


def build_directional_ratio_panel(corridor: pd.DataFrame) -> pd.DataFrame:
    wide = corridor.pivot_table(
        index=[
            "purchase_week",
            "date_str",
            "dow",
            "dow_name",
            "datetime",
            "minute_of_day",
            "time_label",
        ],
        columns="chain_direction",
        values="corridor_speed_kmh",
        aggfunc="first",
    ).reset_index()
    wide.columns.name = None
    wide = wide.dropna(subset=["eastbound_chain", "westbound_chain"]).copy()
    wide["policy_speed_kmh"] = wide["eastbound_chain"]
    wide["opposite_speed_kmh"] = wide["westbound_chain"]
    wide["policy_over_opposite_speed"] = wide["policy_speed_kmh"] / wide["opposite_speed_kmh"]
    wide["policy_minus_opposite_kmh"] = wide["policy_speed_kmh"] - wide["opposite_speed_kmh"]
    wide["log_policy_over_opposite_speed"] = np.log(wide["policy_over_opposite_speed"])
    return wide.sort_values(["purchase_week", "datetime"])


def build_directional_ratio_window_summary(ratio_panel: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows = []
    for window, (lo, hi) in WINDOWS.items():
        sub = ratio_panel[(ratio_panel["minute_of_day"] >= lo) & (ratio_panel["minute_of_day"] < hi)]
        grouped = (
            sub.groupby(["purchase_week", "date_str", "dow_name"], as_index=False)
            .agg(
                policy_over_opposite_speed=("policy_over_opposite_speed", "mean"),
                log_policy_over_opposite_speed=("log_policy_over_opposite_speed", "mean"),
                policy_minus_opposite_kmh=("policy_minus_opposite_kmh", "mean"),
                policy_speed_kmh=("policy_speed_kmh", "mean"),
                opposite_speed_kmh=("opposite_speed_kmh", "mean"),
                n_10min=("policy_over_opposite_speed", "size"),
            )
        )
        grouped["window"] = window
        grouped["window_label"] = WINDOW_LABELS[window]
        rows.append(grouped)
    daily = pd.concat(rows, ignore_index=True)
    summary = (
        daily.groupby(["purchase_week", "window", "window_label"], as_index=False)
        .agg(
            policy_over_opposite_speed=("policy_over_opposite_speed", "mean"),
            log_policy_over_opposite_speed=("log_policy_over_opposite_speed", "mean"),
            policy_minus_opposite_kmh=("policy_minus_opposite_kmh", "mean"),
            policy_speed_kmh=("policy_speed_kmh", "mean"),
            opposite_speed_kmh=("opposite_speed_kmh", "mean"),
            n_days=("date_str", "nunique"),
        )
        .sort_values(["purchase_week", "window"])
    )
    wide = daily.pivot_table(
        index=["purchase_week", "date_str", "dow_name"],
        columns="window",
        values=[
            "policy_over_opposite_speed",
            "log_policy_over_opposite_speed",
            "policy_minus_opposite_kmh",
        ],
        aggfunc="first",
    )
    wide.columns = [f"{metric}_{window}" for metric, window in wide.columns]
    wide = wide.reset_index()
    wide["ratio_of_ratios_active_over_reference"] = (
        wide["policy_over_opposite_speed_active_1730_2000"]
        / wide["policy_over_opposite_speed_reference_1500_1730"]
    )
    wide["log_ratio_active_minus_reference"] = (
        wide["log_policy_over_opposite_speed_active_1730_2000"]
        - wide["log_policy_over_opposite_speed_reference_1500_1730"]
    )
    wide["gap_active_minus_reference_kmh"] = (
        wide["policy_minus_opposite_kmh_active_1730_2000"]
        - wide["policy_minus_opposite_kmh_reference_1500_1730"]
    )
    ror_summary = (
        wide.groupby("purchase_week", as_index=False)
        .agg(
            reference_policy_over_opposite_speed=(
                "policy_over_opposite_speed_reference_1500_1730",
                "mean",
            ),
            active_policy_over_opposite_speed=(
                "policy_over_opposite_speed_active_1730_2000",
                "mean",
            ),
            ratio_of_ratios_active_over_reference=(
                "ratio_of_ratios_active_over_reference",
                "mean",
            ),
            log_ratio_active_minus_reference=("log_ratio_active_minus_reference", "mean"),
            gap_active_minus_reference_kmh=("gap_active_minus_reference_kmh", "mean"),
            n_days=("date_str", "nunique"),
        )
        .sort_values("purchase_week")
    )
    pre_ror = ror_summary.loc[
        ror_summary["purchase_week"].eq("week_20260302_20260308"),
        "ratio_of_ratios_active_over_reference",
    ].iloc[0]
    pre_log = ror_summary.loc[
        ror_summary["purchase_week"].eq("week_20260302_20260308"),
        "log_ratio_active_minus_reference",
    ].iloc[0]
    ror_summary["post_minus_pre_ratio_of_ratios"] = (
        ror_summary["ratio_of_ratios_active_over_reference"] - pre_ror
    )
    ror_summary["post_minus_pre_log_ratio_change"] = (
        ror_summary["log_ratio_active_minus_reference"] - pre_log
    )
    return summary, ror_summary


def plot_nonfriday_diurnal(corridor: pd.DataFrame, output_path: Path) -> None:
    data = corridor[
        (corridor["minute_of_day"] >= 14 * 60 + 30)
        & (corridor["minute_of_day"] <= 21 * 60 + 30)
    ].copy()
    profile = (
        data.groupby(["purchase_week", "chain_direction", "minute_of_day"], as_index=False)
        .agg(corridor_speed_kmh=("corridor_speed_kmh", "mean"))
    )
    profile["hour"] = profile["minute_of_day"] / 60
    fig, axes = plt.subplots(2, 1, figsize=(9.8, 6.0), sharex=True, sharey=True)
    for ax, chain in zip(axes, ["eastbound_chain", "westbound_chain"]):
        ax.axvspan(17.5, 20.0, color="#7cb7a8", alpha=0.14, linewidth=0)
        ax.axvline(17.5, color="#222222", linestyle="--", linewidth=0.9)
        for week, group in profile[profile["chain_direction"].eq(chain)].groupby("purchase_week"):
            group = group.sort_values("minute_of_day")
            ax.plot(
                group["hour"],
                group["corridor_speed_kmh"],
                color=WEEK_COLORS.get(week, "#666666"),
                marker="o",
                markersize=2.6,
                linewidth=1.35,
                label=WEEK_LABELS.get(week, week),
            )
        ax.set_title(CHAIN_LABELS[chain], loc="left", fontweight="bold")
        ax.set_ylabel("Corridor speed (km/h)")
        ax.grid(axis="y", color="#dddddd", linewidth=0.6)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
    axes[0].legend(frameon=False, loc="upper right")
    ticks = np.arange(14.5, 21.6, 0.5)
    axes[-1].set_xticks(ticks)
    axes[-1].set_xticklabels([f"{int(t):02d}:{int(round((t % 1) * 60)):02d}" for t in ticks], rotation=45)
    axes[-1].set_xlabel("Clock time")
    fig.suptitle(
        "Liuxian Avenue pre/post pattern after excluding Fridays",
        x=0.01,
        y=0.995,
        ha="left",
        fontsize=12,
        fontweight="bold",
    )
    fig.text(
        0.01,
        0.012,
        "Notes: Sample is Monday-Thursday only. Uses the 14 Gaode segments common to all three purchased weeks. "
        "Shaded area is the 17:30-20:00 tidal-lane window. Points are 10-minute means; no smoothing.",
        fontsize=7.2,
        color="#444444",
    )
    fig.tight_layout(rect=(0, 0.06, 1, 0.94))
    save_figure(fig, output_path)
    plt.close(fig)


def plot_directional_ratio_diurnal(ratio_panel: pd.DataFrame, output_path: Path) -> None:
    profile = (
        ratio_panel[
            (ratio_panel["minute_of_day"] >= 14 * 60 + 30)
            & (ratio_panel["minute_of_day"] <= 21 * 60 + 30)
        ]
        .groupby(["purchase_week", "minute_of_day"], as_index=False)
        .agg(policy_over_opposite_speed=("policy_over_opposite_speed", "mean"))
    )
    profile["hour"] = profile["minute_of_day"] / 60
    fig, ax = plt.subplots(figsize=(9.8, 4.9))
    ax.axvspan(17.5, 20.0, color="#7cb7a8", alpha=0.14, linewidth=0)
    ax.axvline(17.5, color="#222222", linestyle="--", linewidth=0.9)
    ax.axhline(1, color="#333333", linewidth=0.8)
    for week, group in profile.groupby("purchase_week"):
        group = group.sort_values("minute_of_day")
        ax.plot(
            group["hour"],
            group["policy_over_opposite_speed"],
            color=WEEK_COLORS.get(week, "#666666"),
            marker="o",
            markersize=2.9,
            linewidth=1.45,
            label=WEEK_LABELS.get(week, week),
        )
    ticks = np.arange(14.5, 21.6, 0.5)
    ax.set_xticks(ticks)
    ax.set_xticklabels([f"{int(t):02d}:{int(round((t % 1) * 60)):02d}" for t in ticks], rotation=45)
    ax.set_xlabel("Clock time")
    ax.set_ylabel("Policy-direction speed / opposite-direction speed")
    ax.set_title(
        "Directional speed ratio after excluding Fridays",
        loc="left",
        fontweight="bold",
    )
    ax.grid(axis="y", color="#dddddd", linewidth=0.6)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.legend(frameon=False, loc="upper right")
    fig.text(
        0.01,
        0.012,
        "Notes: Sample is Monday-Thursday only, using the 14 common Gaode segments. "
        "Ratio above 1 means the policy direction is faster than the opposite direction.",
        fontsize=7.3,
        color="#444444",
    )
    fig.tight_layout(rect=(0, 0.06, 1, 1))
    save_figure(fig, output_path)
    plt.close(fig)


def plot_directional_ratio_window_summary(ror_summary: pd.DataFrame, output_path: Path) -> None:
    data = ror_summary.copy()
    data["week_order"] = data["purchase_week"].map(
        {
            "week_20260302_20260308": 0,
            "week_20260413_20260419": 1,
            "week_20260420_20260426": 2,
        }
    )
    data = data.sort_values("week_order")
    x = np.arange(len(data))
    fig, axes = plt.subplots(1, 2, figsize=(11.2, 4.5))
    axes[0].bar(x - 0.18, data["reference_policy_over_opposite_speed"], width=0.36, color="#999999", label="15:00-17:30")
    axes[0].bar(x + 0.18, data["active_policy_over_opposite_speed"], width=0.36, color="#4c78a8", label="17:30-20:00")
    axes[0].axhline(1, color="#333333", linewidth=0.8)
    axes[0].set_ylabel("Policy speed / opposite speed")
    axes[0].set_title("A. Directional speed ratio by window", loc="left", fontweight="bold")
    axes[0].legend(frameon=False, fontsize=7.4)

    axes[1].bar(x, data["ratio_of_ratios_active_over_reference"], color="#c44e52", alpha=0.88)
    axes[1].axhline(1, color="#333333", linewidth=0.8)
    axes[1].set_ylabel("Active ratio / reference ratio")
    axes[1].set_title("B. Ratio-of-ratios", loc="left", fontweight="bold")
    for ax in axes:
        ax.set_xticks(x)
        ax.set_xticklabels(
            ["Pre\n03-02..03-05", "Post 1\n04-13..04-16", "Post 2\n04-20..04-23"],
            fontsize=8,
        )
        ax.grid(axis="y", color="#dddddd", linewidth=0.6)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
    fig.suptitle(
        "Does the tidal window increase directional asymmetry?",
        x=0.01,
        y=0.995,
        ha="left",
        fontsize=12,
        fontweight="bold",
    )
    fig.text(
        0.01,
        0.012,
        "Notes: Ratio-of-ratios above 1 means directional asymmetry is stronger in 17:30-20:00 than in 15:00-17:30.",
        fontsize=7.3,
        color="#444444",
    )
    fig.tight_layout(rect=(0, 0.06, 1, 0.93))
    save_figure(fig, output_path)
    plt.close(fig)


def plot_ratio_summary(ratio_summary: pd.DataFrame, output_path: Path) -> None:
    data = ratio_summary[ratio_summary["purchase_week"].isin(list(WEEK_LABELS))].copy()
    data["week_order"] = data["purchase_week"].map(
        {
            "week_20260302_20260308": 0,
            "week_20260413_20260419": 1,
            "week_20260420_20260426": 2,
            "post_pooled": 3,
        }
    )
    fig, axes = plt.subplots(1, 2, figsize=(11.4, 4.7), sharey=False)
    for ax, col, ylabel, title in [
        (
            axes[0],
            "active_over_reference_1500",
            "17:30-20:00 speed / 15:00-17:30 speed",
            "A. Active/reference ratio",
        ),
        (
            axes[1],
            "active_minus_reference_1500_kmh",
            "17:30-20:00 minus 15:00-17:30 speed (km/h)",
            "B. Active-reference speed gap",
        ),
    ]:
        x_base = np.arange(data["purchase_week"].nunique())
        width = 0.34
        for idx, chain in enumerate(["eastbound_chain", "westbound_chain"]):
            part = data[data["chain_direction"].eq(chain)].sort_values("week_order")
            ax.bar(
                x_base + (idx - 0.5) * width,
                part[col],
                width=width,
                color="#4c78a8" if chain == "eastbound_chain" else "#d55e00",
                alpha=0.88,
                label=CHAIN_LABELS[chain],
            )
        if col == "active_over_reference_1500":
            ax.axhline(1, color="#333333", linewidth=0.8)
        else:
            ax.axhline(0, color="#333333", linewidth=0.8)
        ax.set_xticks(x_base)
        ax.set_xticklabels(
            [
                "Pre\n03-02..03-05",
                "Post 1\n04-13..04-16",
                "Post 2\n04-20..04-23",
                "Post pooled",
            ],
            fontsize=8,
        )
        ax.set_title(title, loc="left", fontweight="bold")
        ax.set_ylabel(ylabel)
        ax.grid(axis="y", color="#dddddd", linewidth=0.6)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
    axes[0].legend(frameon=False, loc="lower left", fontsize=7)
    fig.suptitle(
        "Non-Friday active-window slowdown by direction",
        x=0.01,
        y=0.995,
        ha="left",
        fontsize=12,
        fontweight="bold",
    )
    fig.tight_layout(rect=(0, 0.03, 1, 0.93))
    save_figure(fig, output_path)
    plt.close(fig)


def plot_gap_summary(gap_summary: pd.DataFrame, output_path: Path) -> None:
    data = gap_summary.copy()
    data["week_order"] = data["purchase_week"].map(
        {
            "week_20260302_20260308": 0,
            "week_20260413_20260419": 1,
            "week_20260420_20260426": 2,
            "post_pooled": 3,
        }
    )
    data = data.sort_values("week_order")
    fig, axes = plt.subplots(1, 2, figsize=(11.4, 4.6))
    x = np.arange(len(data))
    axes[0].bar(x - 0.18, data["reference_gap_kmh"], width=0.36, color="#999999", label="15:00-17:30 gap")
    axes[0].bar(x + 0.18, data["active_gap_kmh"], width=0.36, color="#4c78a8", label="17:30-20:00 gap")
    axes[0].axhline(0, color="#333333", linewidth=0.8)
    axes[0].set_ylabel("Policy speed - opposite speed (km/h)")
    axes[0].set_title("A. Directional speed gap", loc="left", fontweight="bold")
    axes[0].legend(frameon=False, fontsize=7.4)

    axes[1].bar(x, data["post_minus_pre_active_reference_ddd_kmh"], color="#c44e52", alpha=0.88)
    axes[1].axhline(0, color="#333333", linewidth=0.8)
    axes[1].set_ylabel("Post minus pre DDD (km/h)")
    axes[1].set_title("B. DDD using 15:00-17:30 reference", loc="left", fontweight="bold")
    for ax in axes:
        ax.set_xticks(x)
        ax.set_xticklabels(
            [
                "Pre\n03-02..03-05",
                "Post 1\n04-13..04-16",
                "Post 2\n04-20..04-23",
                "Post pooled",
            ],
            fontsize=8,
        )
        ax.grid(axis="y", color="#dddddd", linewidth=0.6)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
    fig.suptitle(
        "Does the non-Friday pre/post pattern match the desired directional effect?",
        x=0.01,
        y=0.995,
        ha="left",
        fontsize=12,
        fontweight="bold",
    )
    fig.text(
        0.01,
        0.012,
        "Notes: Positive DDD would mean the policy direction improves relative to the opposite direction during the tidal window after opening. "
        "The pre bar is normalized to zero by construction.",
        fontsize=7.2,
        color="#444444",
    )
    fig.tight_layout(rect=(0, 0.065, 1, 0.93))
    save_figure(fig, output_path)
    plt.close(fig)


def main() -> None:
    args = parse_args()
    configure_plot()
    dirs = ensure_output_dirs(args.output_dir)

    corridor, inventory = build_nonfriday_panel(args.input_dir)
    daily = build_window_daily(corridor)
    window_summary, ratio_summary, gap_summary = build_window_summary(daily)
    ratio_with_pooled, gap_with_pooled = build_pooled_post_summary(ratio_summary, gap_summary)
    directional_ratio_panel = build_directional_ratio_panel(corridor)
    directional_ratio_summary, directional_ror_summary = build_directional_ratio_window_summary(
        directional_ratio_panel
    )

    corridor.to_csv(
        dirs["data"] / "liuxian_nonfriday_common14_corridor_10min_panel.csv",
        index=False,
        encoding="utf-8-sig",
    )
    daily.to_csv(
        dirs["data"] / "liuxian_nonfriday_common14_daily_window_speeds.csv",
        index=False,
        encoding="utf-8-sig",
    )
    window_summary.to_csv(
        dirs["metrics"] / "liuxian_nonfriday_common14_window_summary.csv",
        index=False,
        encoding="utf-8-sig",
    )
    ratio_with_pooled.to_csv(
        dirs["metrics"] / "liuxian_nonfriday_common14_active_reference_summary.csv",
        index=False,
        encoding="utf-8-sig",
    )
    gap_with_pooled.to_csv(
        dirs["metrics"] / "liuxian_nonfriday_common14_directional_gap_ddd.csv",
        index=False,
        encoding="utf-8-sig",
    )
    directional_ratio_panel.to_csv(
        dirs["data"] / "liuxian_nonfriday_common14_directional_ratio_10min_panel.csv",
        index=False,
        encoding="utf-8-sig",
    )
    directional_ratio_summary.to_csv(
        dirs["metrics"] / "liuxian_nonfriday_common14_directional_ratio_window_summary.csv",
        index=False,
        encoding="utf-8-sig",
    )
    directional_ror_summary.to_csv(
        dirs["metrics"] / "liuxian_nonfriday_common14_directional_ratio_of_ratios.csv",
        index=False,
        encoding="utf-8-sig",
    )

    plot_nonfriday_diurnal(
        corridor,
        dirs["figures"] / "liuxian_nonfriday_pre_post_diurnal_common14.png",
    )
    plot_ratio_summary(
        ratio_with_pooled,
        dirs["figures"] / "liuxian_nonfriday_active_reference_by_direction.png",
    )
    plot_gap_summary(
        gap_with_pooled,
        dirs["figures"] / "liuxian_nonfriday_directional_gap_ddd.png",
    )
    plot_directional_ratio_diurnal(
        directional_ratio_panel,
        dirs["figures"] / "liuxian_nonfriday_directional_speed_ratio_diurnal.png",
    )
    plot_directional_ratio_window_summary(
        directional_ror_summary,
        dirs["figures"] / "liuxian_nonfriday_directional_ratio_of_ratios.png",
    )

    print(f"Wrote non-Friday pre/post analysis to {args.output_dir}")
    print(ratio_with_pooled.to_string(index=False))
    print(gap_with_pooled.to_string(index=False))
    print(directional_ror_summary.to_string(index=False))


if __name__ == "__main__":
    main()
