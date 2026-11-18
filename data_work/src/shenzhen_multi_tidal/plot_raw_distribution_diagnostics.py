from __future__ import annotations

import argparse
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from run_multi_tidal_post_only_analysis import (  # noqa: E402
    DIRECTION_LABELS,
    DIRECTION_ORDER,
    POLICY,
    ROAD_CONFIGS,
    configure_plot,
    minute_label,
    save_figure,
    window_bounds,
)


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data_work/outputs/shenzhen_multi_tidal_post_only_v1"

PERIOD_ORDER = ["reference", "active", "post"]
PERIOD_LABELS = {
    "reference": "Reference",
    "active": "Active",
    "post": "Post",
}
PERIOD_COLORS = {
    "reference": "#7f7f7f",
    "active": "#2b6cb0",
    "post": "#f2a65a",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot raw speed distributions for refined Shenzhen tidal-lane treatment/control samples."
    )
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def ensure_raw_dirs(output_dir: Path) -> dict[str, Path]:
    dirs = {
        "raw": output_dir / "figures/raw_distribution",
        "time": output_dir / "figures/raw_time_variation",
        "metrics": output_dir / "metrics",
    }
    for path in dirs.values():
        path.mkdir(parents=True, exist_ok=True)
    return dirs


def iter_road_windows() -> list[tuple[str, dict[str, object], dict[str, object]]]:
    rows = []
    for number, cfg0 in ROAD_CONFIGS.items():
        if not cfg0["include_main"]:
            continue
        cfg = dict(cfg0)
        cfg["tidal_number"] = number
        for window in cfg["windows"]:
            rows.append((number, cfg, window))
    return rows


def add_window_periods(corridor: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _number, cfg, window in iter_road_windows():
        ref_start, start, end, post_end, plot_start, plot_end = window_bounds(window)
        sub = corridor[
            corridor["road_key"].eq(cfg["road_key"])
            & corridor["window_key"].eq(window["window_key"])
            & corridor["day_type"].eq("weekday")
            & (corridor["minute_of_day"] >= plot_start)
            & (corridor["minute_of_day"] < plot_end)
        ].copy()
        if sub.empty:
            continue
        sub["period"] = np.select(
            [
                (sub["minute_of_day"] >= ref_start) & (sub["minute_of_day"] < start),
                (sub["minute_of_day"] >= start) & (sub["minute_of_day"] < end),
                (sub["minute_of_day"] >= end) & (sub["minute_of_day"] < post_end),
            ],
            PERIOD_ORDER,
            default="outside",
        )
        sub = sub[sub["period"].isin(PERIOD_ORDER)].copy()
        sub["period_label"] = sub["period"].map(PERIOD_LABELS)
        sub["window_start_min"] = start
        sub["window_end_min"] = end
        rows.append(sub)
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def build_distribution_quantiles(windowed: pd.DataFrame) -> pd.DataFrame:
    if windowed.empty:
        return pd.DataFrame()
    group_cols = [
        "tidal_number",
        "road_key",
        "tidal_road_name",
        "window_key",
        "operation_window_label",
        "corridor_id",
        "corridor_label",
        "role",
        "direction_group",
        "period",
    ]
    summary = (
        windowed.groupby(group_cols, as_index=False)
        .agg(
            n_10min=("corridor_speed_kmh", "count"),
            mean_speed_kmh=("corridor_speed_kmh", "mean"),
            sd_speed_kmh=("corridor_speed_kmh", "std"),
            p10_speed_kmh=("corridor_speed_kmh", lambda x: x.quantile(0.10)),
            p25_speed_kmh=("corridor_speed_kmh", lambda x: x.quantile(0.25)),
            median_speed_kmh=("corridor_speed_kmh", "median"),
            p75_speed_kmh=("corridor_speed_kmh", lambda x: x.quantile(0.75)),
            p90_speed_kmh=("corridor_speed_kmh", lambda x: x.quantile(0.90)),
        )
    )
    summary["period_order"] = summary["period"].map({p: i for i, p in enumerate(PERIOD_ORDER)})
    return summary.sort_values(
        ["tidal_number", "window_key", "role", "corridor_label", "direction_group", "period_order"]
    ).drop(columns="period_order")


def build_start_breakpoint(windowed: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows = []
    for _number, cfg, window in iter_road_windows():
        start = int(window["start_min"])
        sub = windowed[
            windowed["road_key"].eq(cfg["road_key"])
            & windowed["window_key"].eq(window["window_key"])
            & windowed["role"].eq("treatment")
            & windowed["minute_of_day"].between(start - 30, start + 29)
        ].copy()
        if sub.empty:
            continue
        for keys, part in sub.groupby(["date", "direction_group"], dropna=False):
            before = part[(part["minute_of_day"] >= start - 30) & (part["minute_of_day"] < start)]
            after = part[(part["minute_of_day"] >= start) & (part["minute_of_day"] < start + 30)]
            if before.empty or after.empty:
                continue
            rows.append(
                {
                    "tidal_number": cfg["tidal_number"],
                    "road_key": cfg["road_key"],
                    "tidal_road_name": cfg["road_name_cn"],
                    "window_key": window["window_key"],
                    "operation_window_label": window["label"],
                    "date": keys[0],
                    "direction_group": keys[1],
                    "before_30min_speed_kmh": before["corridor_speed_kmh"].mean(),
                    "after_30min_speed_kmh": after["corridor_speed_kmh"].mean(),
                    "after_minus_before_kmh": (
                        after["corridor_speed_kmh"].mean() - before["corridor_speed_kmh"].mean()
                    ),
                    "n_before_10min": int(before["minute_of_day"].nunique()),
                    "n_after_10min": int(after["minute_of_day"].nunique()),
                }
            )
    daily = pd.DataFrame(rows)
    if daily.empty:
        return daily, pd.DataFrame()
    summary = (
        daily.groupby(
            [
                "tidal_number",
                "road_key",
                "tidal_road_name",
                "window_key",
                "operation_window_label",
                "direction_group",
            ],
            as_index=False,
        )
        .agg(
            mean_before_30min_speed_kmh=("before_30min_speed_kmh", "mean"),
            mean_after_30min_speed_kmh=("after_30min_speed_kmh", "mean"),
            mean_after_minus_before_kmh=("after_minus_before_kmh", "mean"),
            median_after_minus_before_kmh=("after_minus_before_kmh", "median"),
            sd_after_minus_before_kmh=("after_minus_before_kmh", "std"),
            n_weekdays=("date", "nunique"),
        )
    )
    return daily, summary


def ordered_corridors(sub: pd.DataFrame) -> list[str]:
    order = (
        sub[["corridor_label", "role"]]
        .drop_duplicates()
        .sort_values(["role", "corridor_label"])
    )
    treatment = order[order["role"].eq("treatment")]["corridor_label"].tolist()
    controls = order[order["role"].eq("control")]["corridor_label"].tolist()
    return treatment + controls


def plot_corridor_distribution_by_road(windowed: pd.DataFrame, output_dir: Path) -> None:
    for _number, cfg, window in iter_road_windows():
        sub = windowed[
            windowed["road_key"].eq(cfg["road_key"])
            & windowed["window_key"].eq(window["window_key"])
        ].copy()
        if sub.empty:
            continue
        corridor_labels = ordered_corridors(sub)
        positions = np.arange(len(corridor_labels), dtype=float)
        width = 0.22
        offsets = {"reference": -width, "active": 0.0, "post": width}
        fig, axes = plt.subplots(1, 2, figsize=(13.0, 5.0), sharey=True)
        for ax, direction in zip(axes, DIRECTION_ORDER):
            directional = sub[sub["direction_group"].eq(direction)]
            for period in PERIOD_ORDER:
                data = [
                    directional[
                        directional["corridor_label"].eq(label)
                        & directional["period"].eq(period)
                    ]["corridor_speed_kmh"].dropna()
                    for label in corridor_labels
                ]
                nonempty = [x.to_numpy() for x in data if not x.empty]
                pos = [positions[i] + offsets[period] for i, x in enumerate(data) if not x.empty]
                if not nonempty:
                    continue
                box = ax.boxplot(
                    nonempty,
                    positions=pos,
                    widths=0.16,
                    patch_artist=True,
                    showfliers=False,
                    medianprops={"color": "#111111", "linewidth": 0.8},
                    whiskerprops={"color": PERIOD_COLORS[period], "linewidth": 0.8},
                    capprops={"color": PERIOD_COLORS[period], "linewidth": 0.8},
                )
                for patch in box["boxes"]:
                    patch.set(facecolor=PERIOD_COLORS[period], alpha=0.50, edgecolor=PERIOD_COLORS[period])
                rng = np.random.default_rng(20260628)
                for i, series in enumerate(data):
                    if series.empty:
                        continue
                    values = series.to_numpy()
                    jitter = rng.normal(0, 0.012, len(values))
                    ax.scatter(
                        np.full(len(values), positions[i] + offsets[period]) + jitter,
                        values,
                        s=4,
                        color=PERIOD_COLORS[period],
                        alpha=0.28,
                        linewidths=0,
                    )
            ax.axhline(0, color="#222222", linewidth=0.6)
            ax.set_title(DIRECTION_LABELS[direction], loc="left", fontweight="bold")
            ax.set_xticks(positions)
            ax.set_xticklabels(corridor_labels, rotation=35, ha="right")
            ax.grid(axis="y", color="#e5e5e5", linewidth=0.5)
            for spine in ["top", "right"]:
                ax.spines[spine].set_visible(False)
        axes[0].set_ylabel("Raw 10-minute corridor speed (km/h)")
        handles = [
            plt.Line2D([0], [0], marker="s", color="none", markerfacecolor=PERIOD_COLORS[p],
                       markeredgecolor=PERIOD_COLORS[p], alpha=0.70, markersize=8, label=PERIOD_LABELS[p])
            for p in PERIOD_ORDER
        ]
        fig.legend(handles=handles, frameon=False, loc="center right", bbox_to_anchor=(0.99, 0.54))
        fig.suptitle(
            f"{cfg['road_name_cn']} {window['label']} raw speed distributions by corridor",
            x=0.01,
            ha="left",
        )
        fig.tight_layout(rect=(0, 0, 0.90, 0.92))
        save_figure(
            fig,
            output_dir / f"{cfg['road_key']}_{window['window_key']}_raw_corridor_distribution.png",
        )


def plot_treatment_daily_points(windowed: pd.DataFrame, output_dir: Path) -> None:
    for _number, cfg, window in iter_road_windows():
        _ref_start, start, end, _post_end, plot_start, plot_end = window_bounds(window)
        sub = windowed[
            windowed["road_key"].eq(cfg["road_key"])
            & windowed["window_key"].eq(window["window_key"])
            & windowed["role"].eq("treatment")
            & (windowed["minute_of_day"] >= plot_start)
            & (windowed["minute_of_day"] < plot_end)
        ].copy()
        if sub.empty:
            continue
        fig, axes = plt.subplots(1, 2, figsize=(12.2, 4.6), sharey=True)
        dates = sorted(sub["date"].unique())
        cmap = plt.get_cmap("tab10")
        date_colors = {date: cmap(i % 10) for i, date in enumerate(dates)}
        for ax, direction in zip(axes, DIRECTION_ORDER):
            directional = sub[sub["direction_group"].eq(direction)]
            for date, part in directional.groupby("date"):
                part = part.sort_values("minute_of_day")
                ax.plot(
                    part["minute_of_day"] / 60,
                    part["corridor_speed_kmh"],
                    marker="o",
                    markersize=2.6,
                    linewidth=0.9,
                    color=date_colors[date],
                    alpha=0.90,
                    label=date,
                )
            ax.axvline(start / 60, color="#111111", linewidth=0.8, linestyle="--")
            ax.axvline(end / 60, color="#111111", linewidth=0.8, linestyle=":")
            ax.axvspan(start / 60, end / 60, color="#eeeeee", alpha=0.58)
            ax.set_xlim(plot_start / 60, plot_end / 60)
            ax.set_title(DIRECTION_LABELS[direction], loc="left", fontweight="bold")
            ax.set_xlabel("Hour")
            ax.grid(axis="y", color="#e5e5e5", linewidth=0.5)
            for spine in ["top", "right"]:
                ax.spines[spine].set_visible(False)
        axes[0].set_ylabel("Treatment raw 10-minute corridor speed (km/h)")
        handles, labels = axes[1].get_legend_handles_labels()
        if handles:
            fig.legend(handles, labels, frameon=False, loc="center right", bbox_to_anchor=(0.99, 0.5))
        fig.suptitle(
            f"{cfg['road_name_cn']} {window['label']} treatment daily raw points",
            x=0.01,
            ha="left",
        )
        fig.tight_layout(rect=(0, 0, 0.88, 0.92))
        save_figure(
            fig,
            output_dir / f"{cfg['road_key']}_{window['window_key']}_treatment_daily_raw_points.png",
        )


def plot_breakpoint_summary(summary: pd.DataFrame, output_path: Path) -> None:
    if summary.empty:
        return
    order = []
    for _number, cfg, window in iter_road_windows():
        order.append((cfg["road_name_cn"], window["label"]))
    labels = [f"{road} {window}" for road, window in order]
    positions = {(road, window): i for i, (road, window) in enumerate(order)}
    fig, ax = plt.subplots(figsize=(10.8, 4.5))
    colors = {DIRECTION_ORDER[0]: "#2b6cb0", DIRECTION_ORDER[1]: "#c44e52"}
    offsets = {DIRECTION_ORDER[0]: -0.18, DIRECTION_ORDER[1]: 0.18}
    for direction in DIRECTION_ORDER:
        part = summary[summary["direction_group"].eq(direction)].copy()
        if part.empty:
            continue
        x = [
            positions[(row.tidal_road_name, row.operation_window_label)] + offsets[direction]
            for row in part.itertuples()
        ]
        ax.bar(
            x,
            part["mean_after_minus_before_kmh"],
            width=0.34,
            color=colors[direction],
            alpha=0.88,
            label=DIRECTION_LABELS[direction],
        )
    ax.axhline(0, color="#222222", linewidth=0.8)
    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels, rotation=28, ha="right")
    ax.set_ylabel("Mean 30-min after-start minus before-start speed (km/h)")
    ax.set_title("Raw start-time discontinuity check, treatment only", loc="left")
    ax.grid(axis="y", color="#e5e5e5", linewidth=0.5)
    ax.legend(frameon=False, loc="best")
    for spine in ["top", "right"]:
        ax.spines[spine].set_visible(False)
    fig.tight_layout()
    save_figure(fig, output_path)


def build_segment_distribution(segment_panel: pd.DataFrame) -> pd.DataFrame:
    if segment_panel.empty:
        return pd.DataFrame()
    group_cols = [
        "tidal_number",
        "road_key",
        "tidal_road_name",
        "corridor_id",
        "corridor_label",
        "role",
        "roadseg_id",
    ]
    summary = (
        segment_panel[segment_panel["day_type"].eq("weekday")]
        .groupby(group_cols, as_index=False)
        .agg(
            n_obs=("speed_kmh", "count"),
            mean_speed_kmh=("speed_kmh", "mean"),
            p10_speed_kmh=("speed_kmh", lambda x: x.quantile(0.10)),
            median_speed_kmh=("speed_kmh", "median"),
            p90_speed_kmh=("speed_kmh", lambda x: x.quantile(0.90)),
        )
    )
    return summary.sort_values(["tidal_number", "role", "corridor_label", "roadseg_id"])


def build_raw_time_change_panel(windowed: pd.DataFrame) -> pd.DataFrame:
    if windowed.empty:
        return pd.DataFrame()
    index_cols = [
        "road_key",
        "window_key",
        "corridor_id",
        "corridor_label",
        "role",
        "direction_group",
        "date",
    ]
    reference = (
        windowed[windowed["period"].eq("reference")]
        .groupby(index_cols, as_index=False)
        .agg(reference_speed_kmh=("corridor_speed_kmh", "mean"))
    )
    panel = windowed.merge(reference, on=index_cols, how="left")
    panel["speed_change_from_reference_kmh"] = (
        panel["corridor_speed_kmh"] - panel["reference_speed_kmh"]
    )
    panel["event_time_min"] = panel["minute_of_day"] - panel["window_start_min"]
    panel["hour_of_day"] = panel["minute_of_day"] / 60
    return panel


def plot_raw_speed_over_time(time_panel: pd.DataFrame, output_dir: Path) -> None:
    for _number, cfg, window in iter_road_windows():
        _ref_start, start, end, _post_end, plot_start, plot_end = window_bounds(window)
        sub = time_panel[
            time_panel["road_key"].eq(cfg["road_key"])
            & time_panel["window_key"].eq(window["window_key"])
            & (time_panel["minute_of_day"] >= plot_start)
            & (time_panel["minute_of_day"] < plot_end)
        ].copy()
        if sub.empty:
            continue
        fig, axes = plt.subplots(1, 2, figsize=(12.8, 4.8), sharey=True)
        dates = sorted(sub[sub["role"].eq("treatment")]["date"].unique())
        cmap = plt.get_cmap("tab10")
        date_colors = {date: cmap(i % 10) for i, date in enumerate(dates)}
        for ax, direction in zip(axes, DIRECTION_ORDER):
            directional = sub[sub["direction_group"].eq(direction)].copy()
            controls = directional[directional["role"].eq("control")]
            if not controls.empty:
                ax.scatter(
                    controls["hour_of_day"],
                    controls["corridor_speed_kmh"],
                    s=7,
                    color="#9a9a9a",
                    alpha=0.24,
                    linewidths=0,
                    label="controls, all 10-min points",
                )
                control_mean = (
                    controls.groupby("minute_of_day", as_index=False)
                    .agg(corridor_speed_kmh=("corridor_speed_kmh", "mean"))
                )
                ax.plot(
                    control_mean["minute_of_day"] / 60,
                    control_mean["corridor_speed_kmh"],
                    color="#555555",
                    linewidth=1.2,
                    alpha=0.80,
                    label="controls mean",
                )
            treatment = directional[directional["role"].eq("treatment")]
            for date, part in treatment.groupby("date"):
                part = part.sort_values("minute_of_day")
                ax.plot(
                    part["hour_of_day"],
                    part["corridor_speed_kmh"],
                    marker="o",
                    markersize=2.8,
                    linewidth=1.0,
                    color=date_colors.get(date, "#08519c"),
                    alpha=0.92,
                    label=f"treatment {date}",
                )
            ax.axvline(start / 60, color="#111111", linewidth=0.8, linestyle="--")
            ax.axvline(end / 60, color="#111111", linewidth=0.8, linestyle=":")
            ax.axvspan(start / 60, end / 60, color="#eeeeee", alpha=0.58)
            ax.set_xlim(plot_start / 60, plot_end / 60)
            ax.set_title(DIRECTION_LABELS[direction], loc="left", fontweight="bold")
            ax.set_xlabel("Hour")
            ax.grid(axis="y", color="#e5e5e5", linewidth=0.5)
            for spine in ["top", "right"]:
                ax.spines[spine].set_visible(False)
        axes[0].set_ylabel("Raw 10-minute corridor speed (km/h)")
        handles, labels = axes[1].get_legend_handles_labels()
        if handles:
            # Keep only one control legend entry and all treatment dates.
            dedup = dict(zip(labels, handles))
            fig.legend(
                dedup.values(),
                dedup.keys(),
                frameon=False,
                loc="center right",
                bbox_to_anchor=(1.0, 0.5),
                fontsize=7,
            )
        fig.suptitle(
            f"{cfg['road_name_cn']} {window['label']} raw speed over time",
            x=0.01,
            ha="left",
        )
        fig.tight_layout(rect=(0, 0, 0.86, 0.92))
        save_figure(
            fig,
            output_dir / f"{cfg['road_key']}_{window['window_key']}_raw_speed_over_time.png",
        )


def plot_raw_change_over_time(time_panel: pd.DataFrame, output_dir: Path) -> None:
    for _number, cfg, window in iter_road_windows():
        _ref_start, start, end, _post_end, plot_start, plot_end = window_bounds(window)
        sub = time_panel[
            time_panel["road_key"].eq(cfg["road_key"])
            & time_panel["window_key"].eq(window["window_key"])
            & (time_panel["minute_of_day"] >= plot_start)
            & (time_panel["minute_of_day"] < plot_end)
        ].copy()
        if sub.empty:
            continue
        fig, axes = plt.subplots(1, 2, figsize=(12.8, 4.8), sharey=True)
        dates = sorted(sub[sub["role"].eq("treatment")]["date"].unique())
        cmap = plt.get_cmap("tab10")
        date_colors = {date: cmap(i % 10) for i, date in enumerate(dates)}
        for ax, direction in zip(axes, DIRECTION_ORDER):
            directional = sub[sub["direction_group"].eq(direction)].copy()
            controls = directional[directional["role"].eq("control")]
            if not controls.empty:
                ax.scatter(
                    controls["event_time_min"],
                    controls["speed_change_from_reference_kmh"],
                    s=7,
                    color="#9a9a9a",
                    alpha=0.24,
                    linewidths=0,
                    label="controls, all 10-min points",
                )
                control_mean = (
                    controls.groupby("event_time_min", as_index=False)
                    .agg(speed_change_from_reference_kmh=("speed_change_from_reference_kmh", "mean"))
                )
                ax.plot(
                    control_mean["event_time_min"],
                    control_mean["speed_change_from_reference_kmh"],
                    color="#555555",
                    linewidth=1.2,
                    alpha=0.80,
                    label="controls mean",
                )
            treatment = directional[directional["role"].eq("treatment")]
            for date, part in treatment.groupby("date"):
                part = part.sort_values("event_time_min")
                ax.plot(
                    part["event_time_min"],
                    part["speed_change_from_reference_kmh"],
                    marker="o",
                    markersize=2.8,
                    linewidth=1.0,
                    color=date_colors.get(date, "#08519c"),
                    alpha=0.92,
                    label=f"treatment {date}",
                )
            ax.axhline(0, color="#222222", linewidth=0.8)
            ax.axvline(0, color="#111111", linewidth=0.8, linestyle="--")
            ax.axvline(end - start, color="#111111", linewidth=0.8, linestyle=":")
            ax.axvspan(0, end - start, color="#eeeeee", alpha=0.58)
            ax.set_xlim(plot_start - start, plot_end - start)
            ax.set_title(DIRECTION_LABELS[direction], loc="left", fontweight="bold")
            ax.set_xlabel(f"Minutes relative to {minute_label(start)}")
            ax.grid(axis="y", color="#e5e5e5", linewidth=0.5)
            for spine in ["top", "right"]:
                ax.spines[spine].set_visible(False)
        axes[0].set_ylabel("Speed change from reference window (km/h)")
        handles, labels = axes[1].get_legend_handles_labels()
        if handles:
            dedup = dict(zip(labels, handles))
            fig.legend(
                dedup.values(),
                dedup.keys(),
                frameon=False,
                loc="center right",
                bbox_to_anchor=(1.0, 0.5),
                fontsize=7,
            )
        fig.suptitle(
            f"{cfg['road_name_cn']} {window['label']} raw speed change over time",
            x=0.01,
            ha="left",
        )
        fig.tight_layout(rect=(0, 0, 0.86, 0.92))
        save_figure(
            fig,
            output_dir / f"{cfg['road_key']}_{window['window_key']}_raw_change_over_time.png",
        )


def plot_segment_speed_histograms(segment_panel: pd.DataFrame, output_path: Path) -> None:
    weekday = segment_panel[segment_panel["day_type"].eq("weekday")].copy()
    if weekday.empty:
        return
    road_order = [cfg["road_name_cn"] for _number, cfg, _window in iter_road_windows()]
    road_order = list(dict.fromkeys(road_order))
    fig, axes = plt.subplots(len(road_order), 1, figsize=(9.6, 2.2 * len(road_order)), sharex=True)
    if len(road_order) == 1:
        axes = [axes]
    bins = np.arange(0, 91, 3)
    for ax, road_name in zip(axes, road_order):
        sub = weekday[weekday["tidal_road_name"].eq(road_name)]
        for role, color in [("treatment", "#08519c"), ("control", "#7f7f7f")]:
            values = sub[sub["role"].eq(role)]["speed_kmh"].dropna()
            if values.empty:
                continue
            ax.hist(
                values,
                bins=bins,
                density=True,
                histtype="step",
                linewidth=1.6 if role == "treatment" else 1.1,
                color=color,
                alpha=0.95,
                label=role,
            )
        ax.set_title(road_name, loc="left", fontweight="bold")
        ax.grid(axis="y", color="#e5e5e5", linewidth=0.5)
        ax.legend(frameon=False, loc="upper right")
        for spine in ["top", "right"]:
            ax.spines[spine].set_visible(False)
    axes[-1].set_xlabel("Raw segment speed (km/h)")
    fig.supylabel("Density")
    fig.suptitle("Weekday raw Baidu segment-speed distributions, selected treatment/control segments", x=0.01, ha="left")
    fig.tight_layout(rect=(0, 0, 1, 0.96))
    save_figure(fig, output_path)


def main() -> None:
    args = parse_args()
    configure_plot()
    dirs = ensure_raw_dirs(args.output_dir)
    corridor = pd.read_csv(args.output_dir / "data/corridor_10min_speed.csv")
    segment_panel = pd.read_csv(args.output_dir / "data/selected_segment_speed_panel.csv")
    windowed = add_window_periods(corridor)

    quantiles = build_distribution_quantiles(windowed)
    quantiles.to_csv(
        dirs["metrics"] / "raw_corridor_speed_distribution_quantiles.csv",
        index=False,
        encoding="utf-8-sig",
    )
    breakpoint_daily, breakpoint_summary = build_start_breakpoint(windowed)
    breakpoint_daily.to_csv(
        dirs["metrics"] / "raw_start_breakpoint_30min_daily.csv",
        index=False,
        encoding="utf-8-sig",
    )
    breakpoint_summary.to_csv(
        dirs["metrics"] / "raw_start_breakpoint_30min_summary.csv",
        index=False,
        encoding="utf-8-sig",
    )
    segment_distribution = build_segment_distribution(segment_panel)
    segment_distribution.to_csv(
        dirs["metrics"] / "raw_segment_speed_distribution_by_segment.csv",
        index=False,
        encoding="utf-8-sig",
    )
    time_change_panel = build_raw_time_change_panel(windowed)
    time_change_panel.to_csv(
        dirs["metrics"] / "raw_time_variation_change_panel.csv",
        index=False,
        encoding="utf-8-sig",
    )

    plot_corridor_distribution_by_road(windowed, dirs["raw"])
    plot_treatment_daily_points(windowed, dirs["raw"])
    plot_breakpoint_summary(
        breakpoint_summary,
        dirs["raw"] / "raw_start_breakpoint_30min_summary.png",
    )
    plot_segment_speed_histograms(
        segment_panel,
        dirs["raw"] / "raw_segment_speed_histograms_by_road.png",
    )
    plot_raw_speed_over_time(time_change_panel, dirs["time"])
    plot_raw_change_over_time(time_change_panel, dirs["time"])


if __name__ == "__main__":
    main()
