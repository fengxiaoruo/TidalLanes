#!/usr/bin/env python3
"""Create coefficient summaries for the July 23/24 collaborator report."""

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib import font_manager

from analyze_outbound_rdit_specification_search_v1 import EVENTS as EVENT_META, event_sample
from analyze_outbound_rdit_per_lane_supplement_v1 import add_per_lane


ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "flowvedio/batch_outputs/20260723_24_state_dependent_recognition_v5/analysis_basic_v1"
OUT = BASE / "outbound_rdit_collaborator_report_v1"
OUT.mkdir(parents=True, exist_ok=True)

for candidate in ("PingFang SC", "Heiti SC", "Arial Unicode MS"):
    if any(font.name == candidate for font in font_manager.fontManager.ttflist):
        plt.rcParams["font.family"] = candidate
        break
plt.rcParams["axes.unicode_minus"] = False


EVENTS = {
    "20260723_opening": "7/23 开通",
    "20260723_recovery": "7/23 恢复",
    "20260724_opening": "7/24 开通",
    "20260724_recovery": "7/24 恢复",
}


def coefficient_plot(frame, outcomes, labels, output, xlabel, events=None, figsize=(8.0, 6.2)):
    selected = frame[frame["outcome"].isin(outcomes)].copy()
    if events is not None:
        selected = selected[selected["event"].isin(events)].copy()
    selected["event_label_cn"] = selected["event"].map(EVENTS)
    selected["outcome_label_cn"] = selected["outcome"].map(labels)
    selected["row"] = selected["event_label_cn"] + "  " + selected["outcome_label_cn"]
    event_order = list(EVENTS) if events is None else events
    order = [f"{EVENTS[event]}  {labels[outcome]}" for event in event_order for outcome in outcomes]
    selected["row"] = pd.Categorical(selected["row"], categories=order[::-1], ordered=True)
    selected = selected.sort_values("row")

    colors = ["#2864A6" if lo <= 0 <= hi else "#C4473D" for lo, hi in zip(selected.ci95_low, selected.ci95_high)]
    fig, ax = plt.subplots(figsize=figsize)
    ax.axvline(0, color="#555555", linewidth=0.9)
    for (_, item), color in zip(selected.iterrows(), colors):
        ax.errorbar(
            item["tau"], item["row"],
            xerr=[[item["tau"] - item["ci95_low"]], [item["ci95_high"] - item["tau"]]],
            fmt="o", color=color, ecolor=color, markersize=4.8,
            elinewidth=1.5, capsize=3, zorder=3,
        )
    ax.set_xlabel(xlabel)
    ax.set_ylabel("")
    ax.grid(axis="x", color="#D9D9D9", linewidth=0.6)
    ax.spines[["top", "right", "left"]].set_visible(False)
    ax.tick_params(axis="y", length=0)
    fig.tight_layout()
    for suffix in ("pdf", "png", "svg"):
        fig.savefig(output.with_suffix(f".{suffix}"), dpi=300, bbox_inches="tight")
    plt.close(fig)


def paired_event_plot(frame, outcomes, labels, output, events, xlabel):
    selected = frame[
        frame["outcome"].isin(outcomes) & frame["event"].isin(events)
    ].copy()
    fig, ax = plt.subplots(figsize=(4.4, 4.0))
    x = np.arange(len(outcomes))
    styles = {
        events[0]: ("开通", "#2864A6", -0.12, "o"),
        events[1]: ("恢复", "#D05A47", 0.12, "s"),
    }
    for event, (event_label, color, offset, marker) in styles.items():
        part = selected[selected.event.eq(event)].set_index("outcome").loc[outcomes]
        ax.errorbar(
            x + offset, part.tau,
            yerr=[part.tau - part.ci95_low, part.ci95_high - part.tau],
            fmt=marker, color=color, ecolor=color, markersize=5.2,
            elinewidth=1.45, capsize=3, label=event_label, zorder=3,
        )
    ax.axhline(0, color="#333333", linewidth=0.9)
    ax.set_xticks(x, [labels[o] for o in outcomes], rotation=22, ha="right")
    ax.set_ylabel(xlabel)
    ax.set_xlabel("")
    ax.grid(axis="y", color="#D9D9D9", linewidth=0.6)
    ax.spines[["top", "right", "bottom"]].set_visible(False)
    ax.tick_params(axis="x", length=0)
    handles, legend_labels = ax.get_legend_handles_labels()
    fig.legend(handles, legend_labels, frameon=False, ncol=2, loc="upper center",
               bbox_to_anchor=(0.55, 1.0))
    fig.tight_layout(rect=(0, 0, 1, 0.90))
    for suffix in ("pdf", "png", "svg"):
        fig.savefig(output.with_suffix(f".{suffix}"), dpi=300, bbox_inches="tight")
    plt.close(fig)


def plot_per_lane_with_operation(event, stable_panel, operation):
    sample = add_per_lane(event_sample(stable_panel, event, 60, 0), event)
    date = pd.Timestamp(event["cutoff"]).strftime("%Y-%m-%d")
    op = operation[
        operation["date"].astype(str).eq(date)
        & operation["event"].eq(event["event"].split("_")[1])
        & operation["point_eligible"]
    ].copy()
    cutoff = pd.Timestamp(event["cutoff"])
    op["bin_start"] = pd.to_datetime(op["bin_start"])
    op["running_min"] = (op["bin_start"] - cutoff).dt.total_seconds() / 60
    north_pre, south_pre = ((3, 4) if event["favored"] == "south_to_north" else (4, 3))
    op["north_per_lane"] = op["north_total"] / north_pre
    op["south_per_lane"] = op["south_total"] / south_pre
    op["bidirectional_per_lane"] = op["bidirectional_total"] / 7
    if event["favored"] == "south_to_north":
        op["favored_minus_other_per_lane"] = op["north_per_lane"] - op["south_per_lane"]
    else:
        op["favored_minus_other_per_lane"] = op["south_per_lane"] - op["north_per_lane"]
    op["log_favored_other_per_lane_ratio"] = np.log(
        (np.where(event["favored"] == "south_to_north", op["north_per_lane"], op["south_per_lane"]) + 0.5)
        / (np.where(event["favored"] == "south_to_north", op["south_per_lane"], op["north_per_lane"]) + 0.5)
    )

    transition = (pd.Timestamp(event["transition_start"]) - cutoff).total_seconds() / 60
    keys = ["north_per_lane", "south_per_lane", "bidirectional_per_lane",
            "favored_minus_other_per_lane", "log_favored_other_per_lane_ratio"]
    labels = ["北向每车道", "南向每车道", "双向每条道路车道",
              "获益减另一方向每车道", "log获益/另一方向每车道比"]
    fig, axes = plt.subplots(2, 3, figsize=(9.2, 5.5))
    for ax, outcome, label in zip(axes.flat, keys, labels):
        ax.axvspan(transition, 0, color="#D9D9D9", alpha=.65, lw=0)
        ax.axvline(0, color="#222222", ls="--", lw=.9)
        ax.scatter(sample.running_min, sample[outcome], s=17, color="#2864A6",
                   edgecolor="white", lw=.3, label="稳定状态", zorder=3)
        ax.scatter(op.running_min, op[outcome], s=28, marker="D", color="#7B3294",
                   edgecolor="white", lw=.4, label="操作期", zorder=4)
        for _, part in sample.dropna(subset=[outcome]).groupby("post"):
            if len(part) >= 3:
                coef = np.polyfit(part.running_min, part[outcome], 1)
                xx = np.linspace(part.running_min.min(), part.running_min.max(), 100)
                ax.plot(xx, np.polyval(coef, xx), color="#E45756", lw=1.35)
        ax.axhline(0, color="#888888", lw=.6)
        ax.set_title(label, fontsize=8)
        ax.set_xlabel("距稳定运行完成时刻（分钟）", fontsize=7)
        ax.tick_params(labelsize=7)
        ax.spines[["top", "right"]].set_visible(False)
    axes.flat[5].axis("off")
    handles, legend_labels = axes.flat[0].get_legend_handles_labels()
    axes.flat[5].legend(handles[:2], legend_labels[:2], frameon=False, loc="center", fontsize=8)
    fig.suptitle(f'{EVENTS[event["event"]]}：单位车道流量', fontsize=10)
    fig.tight_layout()
    output = OUT / f'{event["event"]}__per_lane_with_operation'
    for suffix in ("pdf", "png", "svg"):
        fig.savefig(output.with_suffix(f".{suffix}"), dpi=300, bbox_inches="tight")
    plt.close(fig)


main = pd.read_csv(BASE / "outbound_casewise_rdit_all_devices_v2/casewise_rdit_main_60min_v2.csv")
is_difference = main["outcome"].eq("favored_minus_unfavored")
is_recovery = main["event"].str.endswith("recovery")
flip = is_difference & is_recovery
old_low = main.loc[flip, "ci95_low"].copy()
main.loc[flip, "tau"] = -main.loc[flip, "tau"]
main.loc[flip, "ci95_low"] = -main.loc[flip, "ci95_high"]
main.loc[flip, "ci95_high"] = -old_low
main.loc[is_difference, "outcome"] = "north_minus_south"

main_outcomes = ["south_to_north", "north_to_south", "total_flow", "north_minus_south"]
main_labels = {
    "south_to_north": "北向",
    "north_to_south": "南向",
    "total_flow": "双向总量",
    "north_minus_south": "北向减南向",
}
for date, events in {
    "20260723": ["20260723_opening", "20260723_recovery"],
    "20260724": ["20260724_opening", "20260724_recovery"],
}.items():
    paired_event_plot(
        main, main_outcomes, main_labels,
        OUT / f"baseline_flow_coefficients_60min_{date}",
        events=events,
        xlabel="断点水平差（车辆/分钟）",
    )

per_lane = pd.read_csv(BASE / "outbound_rdit_per_lane_supplement_v1/per_lane_baseline_60min.csv")
per_lane_outcomes = ["favored_per_lane", "favored_minus_other_per_lane"]
per_lane_labels = {
    "favored_per_lane": "获益方向每车道",
    "favored_minus_other_per_lane": "获益减另一方向每车道",
}
for date, events in {
    "20260723": ["20260723_opening", "20260723_recovery"],
    "20260724": ["20260724_opening", "20260724_recovery"],
}.items():
    paired_event_plot(
        per_lane, per_lane_outcomes, per_lane_labels,
        OUT / f"baseline_per_lane_coefficients_60min_{date}",
        events=events,
        xlabel="断点水平差（车辆/分钟/车道）",
    )

stable_five = pd.read_csv(
    BASE / "outbound_rdit_specification_search_v1/outbound_region_flow_panel_5min.csv",
    parse_dates=["bin_start"],
)
operation_five = pd.read_csv(
    BASE / "outbound_rdit_operation_overlay_v1/operation_flow_points_5min.csv"
)
for event in EVENT_META:
    plot_per_lane_with_operation(event, stable_five, operation_five)
