#!/usr/bin/env python3
"""Create first-pass descriptive statistics and figures for the 2026-07-10 Caitian Road run."""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.lines import Line2D
from matplotlib.patches import Patch


SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR / "batch_outputs" / "20260710_caitian_identification_final"
OUT = ROOT / "analysis_v1"

DIRS = ["north_to_south", "south_to_north"]
DIR_LABELS = {
    "north_to_south": "North → South",
    "south_to_north": "South → North",
}
DIR_COLORS = {
    "north_to_south": "#2563eb",
    "south_to_north": "#d97706",
}
STATE_COLORS = {
    "normal": "#16a34a",
    "tidal_deployed": "#2563eb",
    "deployment_in_progress": "#f59e0b",
    "reversal_in_progress": "#dc2626",
    "camera_unstable": "#6b7280",
}


def read_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    events = pd.read_csv(ROOT / "events_absolute.csv", parse_dates=["recorded_at_local"])
    segments = pd.read_csv(ROOT / "summary_by_video.csv", parse_dates=["start_local", "end_local"])
    events["geo_direction"] = events["line_name"].str.extract(
        r"^(north_to_south|south_to_north)", expand=False
    )
    events["minute"] = events["recorded_at_local"].dt.floor("min")
    events["five_minute"] = events["recorded_at_local"].dt.floor("5min")
    segments["duration_min"] = segments["duration_s"] / 60.0
    segments["stable_sample"] = segments["counting_status"].eq("count")
    return events, segments


def build_segment_rates(events: pd.DataFrame, segments: pd.DataFrame) -> pd.DataFrame:
    rates = (
        events.groupby(
            ["segment_id", "group", "state", "counting_status", "geo_direction"],
            as_index=False,
        )
        .size()
        .rename(columns={"size": "count"})
    )
    keep = [
        "segment_id",
        "source_video",
        "start_local",
        "end_local",
        "duration_min",
        "stable_sample",
    ]
    rates = rates.merge(segments[keep], on="segment_id", how="left", validate="many_to_one")
    rates["rate_per_min"] = rates["count"] / rates["duration_min"]
    return rates.sort_values(["group", "start_local", "geo_direction"])


def build_aggregate_stats(segment_rates: pd.DataFrame) -> pd.DataFrame:
    stats = (
        segment_rates.groupby(
            ["group", "state", "counting_status", "geo_direction"], as_index=False
        )
        .agg(
            segments=("segment_id", "nunique"),
            exposure_min=("duration_min", "sum"),
            crossing_count=("count", "sum"),
            median_segment_rate=("rate_per_min", "median"),
            p25_segment_rate=("rate_per_min", lambda x: x.quantile(0.25)),
            p75_segment_rate=("rate_per_min", lambda x: x.quantile(0.75)),
        )
    )
    stats["rate_per_min"] = stats["crossing_count"] / stats["exposure_min"]
    return stats.sort_values(["group", "state", "geo_direction"])


def build_quality_stats(events: pd.DataFrame) -> pd.DataFrame:
    grouped = events.groupby(["segment_id", "line_name"])
    rows: list[dict[str, object]] = []
    for (segment_id, line_name), frame in grouped:
        direction_counts = frame["direction"].value_counts()
        dominant = direction_counts.index[0]
        total = int(direction_counts.sum())
        dominant_count = int(direction_counts.iloc[0])
        rows.append(
            {
                "segment_id": segment_id,
                "line_name": line_name,
                "geo_direction": frame["geo_direction"].iloc[0],
                "total_crossings": total,
                "dominant_raw_direction": dominant,
                "non_dominant_crossings": total - dominant_count,
                "non_dominant_share": (total - dominant_count) / total if total else np.nan,
                "median_confidence": frame["confidence"].median(),
            }
        )
    return pd.DataFrame(rows).sort_values("non_dominant_share", ascending=False)


def state_spans(ax: plt.Axes, segments: pd.DataFrame, group: str) -> None:
    for _, row in segments[segments["group"].eq(group)].sort_values("start_local").iterrows():
        color = STATE_COLORS.get(row["state"], "#9ca3af")
        ax.axvspan(row["start_local"], row["end_local"], color=color, alpha=0.07, lw=0, zorder=0)


def minute_grid(events: pd.DataFrame, segments: pd.DataFrame, group: str, direction: str) -> pd.Series:
    segs = segments[segments["group"].eq(group)]
    start = segs["start_local"].min().floor("min")
    end = segs["end_local"].max().ceil("min")
    index = pd.date_range(start, end, freq="min")
    values = pd.Series(np.nan, index=index, dtype=float)
    for _, row in segs.iterrows():
        covered = (index >= row["start_local"].floor("min")) & (index <= row["end_local"].floor("min"))
        values.loc[covered] = values.loc[covered].fillna(0)
    counts = (
        events[(events["group"].eq(group)) & (events["geo_direction"].eq(direction))]
        .groupby("minute")
        .size()
    )
    values.loc[counts.index.intersection(values.index)] = counts.loc[counts.index.intersection(values.index)]
    return values


def plot_minute_counts(events: pd.DataFrame, segments: pd.DataFrame) -> None:
    fig, axes = plt.subplots(2, 1, figsize=(15, 9), sharex=True, constrained_layout=True)
    for ax, group in zip(axes, ["inbound", "outbound"]):
        state_spans(ax, segments, group)
        for direction in DIRS:
            series = minute_grid(events, segments, group, direction)
            ax.plot(
                series.index,
                series.values,
                color=DIR_COLORS[direction],
                lw=1.0,
                alpha=0.85,
                label=DIR_LABELS[direction],
            )
        ax.set_ylabel("Crossings / minute")
        ax.set_title(f"Source stream: {group}", loc="left", fontweight="bold")
        ax.grid(axis="y", color="#d1d5db", lw=0.6, alpha=0.7)
        ax.spines[["top", "right"]].set_visible(False)
        ax.legend(loc="upper right", frameon=False, ncol=2)
    axes[-1].xaxis.set_major_locator(mdates.MinuteLocator(byminute=[0, 30]))
    axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    fig.suptitle("Raw line-assigned vehicle crossings by source stream", fontsize=15, fontweight="bold")
    fig.savefig(OUT / "01_minute_counts_by_stream.png", dpi=180, bbox_inches="tight")
    plt.close(fig)


def plot_rolling_and_share(events: pd.DataFrame, segments: pd.DataFrame) -> None:
    fig, axes = plt.subplots(2, 2, figsize=(15, 9), sharex="col", constrained_layout=True)
    for col, group in enumerate(["inbound", "outbound"]):
        series_map = {direction: minute_grid(events, segments, group, direction) for direction in DIRS}
        frame = pd.DataFrame(series_map)
        rolling = frame.rolling(10, min_periods=3).mean()
        state_spans(axes[0, col], segments, group)
        for direction in DIRS:
            axes[0, col].plot(
                rolling.index,
                rolling[direction],
                color=DIR_COLORS[direction],
                lw=2,
                label=DIR_LABELS[direction],
            )
        axes[0, col].set_title(f"{group}: 10-minute rolling mean", loc="left", fontweight="bold")
        axes[0, col].set_ylabel("Crossings / minute")
        axes[0, col].grid(axis="y", color="#d1d5db", lw=0.6, alpha=0.7)
        axes[0, col].spines[["top", "right"]].set_visible(False)
        axes[0, col].legend(loc="upper right", frameon=False, fontsize=9)

        total = rolling[DIRS].sum(axis=1)
        share = rolling["south_to_north"] / total * 100
        state_spans(axes[1, col], segments, group)
        axes[1, col].plot(share.index, share, color="#7c3aed", lw=2)
        axes[1, col].axhline(50, color="#6b7280", lw=0.8, ls="--")
        axes[1, col].set_title(f"{group}: South → North share", loc="left", fontweight="bold")
        axes[1, col].set_ylabel("Share (%)")
        axes[1, col].set_ylim(0, 100)
        axes[1, col].grid(axis="y", color="#d1d5db", lw=0.6, alpha=0.7)
        axes[1, col].spines[["top", "right"]].set_visible(False)
        axes[1, col].xaxis.set_major_locator(mdates.MinuteLocator(byminute=[0, 30]))
        axes[1, col].xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    fig.suptitle("Smoothed flow and directional composition", fontsize=15, fontweight="bold")
    fig.savefig(OUT / "02_rolling_counts_and_direction_share.png", dpi=180, bbox_inches="tight")
    plt.close(fig)


def plot_segment_rates(segment_rates: pd.DataFrame, segments: pd.DataFrame) -> None:
    fig, axes = plt.subplots(2, 1, figsize=(15, 8), sharex=True, constrained_layout=True)
    for ax, group in zip(axes, ["inbound", "outbound"]):
        state_spans(ax, segments, group)
        frame = segment_rates[segment_rates["group"].eq(group)]
        for direction in DIRS:
            sub = frame[frame["geo_direction"].eq(direction)].sort_values("start_local")
            ax.plot(
                sub["start_local"],
                sub["rate_per_min"],
                color=DIR_COLORS[direction],
                marker="o",
                ms=4,
                lw=1.2,
                label=DIR_LABELS[direction],
            )
        ax.set_ylabel("Rate / minute")
        ax.set_title(f"Source stream: {group}", loc="left", fontweight="bold")
        ax.grid(axis="y", color="#d1d5db", lw=0.6, alpha=0.7)
        ax.spines[["top", "right"]].set_visible(False)
        ax.legend(loc="upper right", frameon=False, ncol=2)
    axes[-1].xaxis.set_major_locator(mdates.MinuteLocator(byminute=[0, 30]))
    axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    state_handles = [Patch(facecolor=color, alpha=0.25, edgecolor="none", label=state) for state, color in STATE_COLORS.items()]
    fig.legend(handles=state_handles, loc="lower center", ncol=5, frameon=False, bbox_to_anchor=(0.5, -0.02))
    fig.suptitle("Segment-level rate by assigned geographic direction", fontsize=15, fontweight="bold")
    fig.savefig(OUT / "03_segment_rates_by_state.png", dpi=180, bbox_inches="tight")
    plt.close(fig)


def write_notes(events: pd.DataFrame, segments: pd.DataFrame, stats: pd.DataFrame) -> None:
    stable = stats[stats["counting_status"].eq("count")].copy()
    classes = events["class"].value_counts().rename_axis("class").reset_index(name="count")
    classes["share"] = classes["count"] / classes["count"].sum()
    classes.to_csv(OUT / "class_counts.csv", index=False)
    stable.to_csv(OUT / "stable_stats_by_stream_state_direction.csv", index=False)

    lines = [
        "# 2026-07-10 彩田路：首轮描述性统计",
        "",
        "## 数据口径",
        "",
        "- 使用 `events_absolute.csv` 和 `summary_by_video.csv`；共 39 个片段、53,352 条过线事件。",
        "- `group` 表示来源视频流（inbound / outbound），不是最终的地理方向；地理方向由 `line_name` 的前缀映射。",
        "- 图 01–03 使用计数线归属的原始事件，暂不自动删除反向 raw direction 事件；方向混杂风险另存于 `direction_mixing_by_segment_line.csv`。",
        "- 主比较样本优先使用 `counting_status == count`，即 `normal` 与 `tidal_deployed`；施工、恢复和机位异常单独保留。",
        "",
        "## 输出",
        "",
        "- `01_minute_counts_by_stream.png`：来源视频流分别按分钟的两方向流量。",
        "- `02_rolling_counts_and_direction_share.png`：10 分钟平滑流量与南向北占比。",
        "- `03_segment_rates_by_state.png`：片段级每分钟流量及车道状态。",
        "- `stable_stats_by_stream_state_direction.csv`：稳定样本统计。",
        "- `direction_mixing_by_segment_line.csv`：raw direction 混杂诊断。",
        "",
        "## 解释边界",
        "",
        "- 两个来源视频流的覆盖区间并不完全相同，且可能观察到同一道路的不同视角；不应直接把 inbound 与 outbound 两个流的计数相加作为道路总流量。",
        "- 潮汐车道前后是不同的车道配置和不同时间段，当前图表是描述性比较，不是因果效应估计。",
    ]
    (OUT / "analysis_notes.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    events, segments = read_data()
    segment_rates = build_segment_rates(events, segments)
    stats = build_aggregate_stats(segment_rates)
    quality = build_quality_stats(events)
    segment_rates.to_csv(OUT / "segment_direction_rates.csv", index=False)
    stats.to_csv(OUT / "aggregate_stats_by_stream_state_direction.csv", index=False)
    quality.to_csv(OUT / "direction_mixing_by_segment_line.csv", index=False)
    plot_minute_counts(events, segments)
    plot_rolling_and_share(events, segments)
    plot_segment_rates(segment_rates, segments)
    write_notes(events, segments, stats)
    print(json.dumps({
        "events": int(len(events)),
        "segments": int(len(segments)),
        "output": str(OUT),
        "figures": [
            "01_minute_counts_by_stream.png",
            "02_rolling_counts_and_direction_share.png",
            "03_segment_rates_by_state.png",
        ],
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
