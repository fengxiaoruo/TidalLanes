from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data_work/outputs/shenzhen_caitian_baidu_analysis_v1"

POLICY_DIRECTION = "policy_direction_futian_to_longhua"
OPPOSITE_DIRECTION = "opposite_direction_longhua_to_futian"
DIRECTION_ORDER = [POLICY_DIRECTION, OPPOSITE_DIRECTION]
DIRECTION_LABELS = {
    POLICY_DIRECTION: "Futian -> Longhua",
    OPPOSITE_DIRECTION: "Longhua -> Futian",
}
CONTROL_ORDER = [
    "control_huanggang",
    "control_jintian",
    "control_yitian",
    "control_xiangmihu",
]
CORRIDOR_ORDER = ["caitian"] + CONTROL_ORDER
EVENT_START_MIN = 16 * 60 + 30
LOCAL_PRE = (16 * 60, EVENT_START_MIN)
LOCAL_POST_30 = (EVENT_START_MIN, 17 * 60)
LOCAL_POST_60 = (EVENT_START_MIN, 17 * 60 + 30)
ACTIVE_WINDOW = (EVENT_START_MIN, 21 * 60 + 30)
PLOT_WINDOW = (14 * 60 + 30, 23 * 60 + 30)
POST_PLOT_WINDOW = (21 * 60 + 30, 23 * 60 + 30)
WEEKDAY_NAMES = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Post-only 16:30 breakpoint and rollout diagnostics for Caitian Road."
    )
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def configure_plot() -> None:
    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "axes.titlesize": 11,
            "axes.labelsize": 9,
            "xtick.labelsize": 8,
            "ytick.labelsize": 8,
            "legend.fontsize": 8,
            "figure.dpi": 180,
            "savefig.dpi": 300,
        }
    )


def save_figure(fig: plt.Figure, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, bbox_inches="tight")
    fig.savefig(output_path.with_suffix(".pdf"), bbox_inches="tight")
    fig.savefig(output_path.with_suffix(".svg"), bbox_inches="tight")
    plt.close(fig)


def minute_label(minute: int) -> str:
    hour = minute // 60
    minute_rem = minute % 60
    return f"{hour:02d}:{minute_rem:02d}"


def date_label(date_value: object) -> str:
    date = pd.to_datetime(date_value)
    return f"{date.date()} ({WEEKDAY_NAMES[date.weekday()]})"


def build_local_jump_daily(corridor: pd.DataFrame) -> pd.DataFrame:
    weekday = corridor[corridor["day_type"].eq("weekday")].copy()
    rows = []
    for (corridor_id, corridor_label, direction, date), group in weekday.groupby(
        ["corridor_id", "corridor_label", "direction_group", "date"]
    ):
        pre = group[
            (group["minute_of_day"] >= LOCAL_PRE[0])
            & (group["minute_of_day"] < LOCAL_PRE[1])
        ]["corridor_speed_kmh"].mean()
        post30 = group[
            (group["minute_of_day"] >= LOCAL_POST_30[0])
            & (group["minute_of_day"] < LOCAL_POST_30[1])
        ]["corridor_speed_kmh"].mean()
        post60 = group[
            (group["minute_of_day"] >= LOCAL_POST_60[0])
            & (group["minute_of_day"] < LOCAL_POST_60[1])
        ]["corridor_speed_kmh"].mean()
        speed_1620 = group[group["minute_of_day"].eq(16 * 60 + 20)]["corridor_speed_kmh"].mean()
        speed_1630 = group[group["minute_of_day"].eq(EVENT_START_MIN)]["corridor_speed_kmh"].mean()
        rows.append(
            {
                "corridor_id": corridor_id,
                "corridor_label": corridor_label,
                "direction_group": direction,
                "direction_label": DIRECTION_LABELS.get(direction, direction),
                "date": date,
                "pre_1600_1630_speed": pre,
                "post_1630_1700_speed": post30,
                "post_1630_1730_speed": post60,
                "speed_1620": speed_1620,
                "speed_1630": speed_1630,
                "jump_10min_kmh": speed_1630 - speed_1620,
                "jump_30min_kmh": post30 - pre,
                "jump_60min_kmh": post60 - pre,
                "log_jump_30min": np.log(post30) - np.log(pre),
                "log_jump_60min": np.log(post60) - np.log(pre),
            }
        )
    return pd.DataFrame(rows)


def build_local_jump_summary(jump_daily: pd.DataFrame) -> pd.DataFrame:
    return (
        jump_daily.groupby(["corridor_id", "corridor_label", "direction_group", "direction_label"], as_index=False)
        .agg(
            n_weekdays=("date", "nunique"),
            pre_1600_1630_speed=("pre_1600_1630_speed", "mean"),
            post_1630_1700_speed=("post_1630_1700_speed", "mean"),
            post_1630_1730_speed=("post_1630_1730_speed", "mean"),
            jump_10min_kmh=("jump_10min_kmh", "mean"),
            jump_30min_kmh=("jump_30min_kmh", "mean"),
            jump_60min_kmh=("jump_60min_kmh", "mean"),
            log_jump_30min=("log_jump_30min", "mean"),
            log_jump_60min=("log_jump_60min", "mean"),
        )
    )


def build_weekday_date_window_summary(corridor: pd.DataFrame) -> pd.DataFrame:
    rows = []
    windows = {
        "reference_1500_1630": ("15:00-16:30", 15 * 60, EVENT_START_MIN),
        "early_active_1630_1730": ("16:30-17:30", EVENT_START_MIN, 17 * 60 + 30),
        "active_1630_2130": ("16:30-21:30", EVENT_START_MIN, ACTIVE_WINDOW[1]),
        "post_2130_2330": ("21:30-23:30", POST_PLOT_WINDOW[0], POST_PLOT_WINDOW[1]),
    }
    weekday = corridor[corridor["day_type"].eq("weekday")].copy()
    for window, (label, start_min, end_min) in windows.items():
        sub = weekday[
            (weekday["minute_of_day"] >= start_min)
            & (weekday["minute_of_day"] < end_min)
        ].copy()
        summary = (
            sub.groupby(
                ["corridor_id", "corridor_label", "direction_group", "date", "dow"],
                as_index=False,
            )
            .agg(
                speed_kmh=("corridor_speed_kmh", "mean"),
                travel_time_min=("travel_time_min", "mean"),
                n_10min=("minute_of_day", "nunique"),
            )
        )
        summary["window"] = window
        summary["window_label"] = label
        rows.append(summary)
    out = pd.concat(rows, ignore_index=True)
    out["date_label"] = out["date"].map(date_label)
    return out


def build_local_jump_dd(jump_daily: pd.DataFrame) -> pd.DataFrame:
    rows = []
    caitian = jump_daily[jump_daily["corridor_id"].eq("caitian")].copy()
    for control_id in CONTROL_ORDER:
        control = jump_daily[jump_daily["corridor_id"].eq(control_id)].copy()
        if control.empty:
            continue
        control_label = control["corridor_label"].iloc[0]
        for direction in DIRECTION_ORDER:
            cai = caitian[caitian["direction_group"].eq(direction)].set_index("date")
            con = control[control["direction_group"].eq(direction)].set_index("date")
            merged = cai[
                ["jump_10min_kmh", "jump_30min_kmh", "jump_60min_kmh", "log_jump_30min", "log_jump_60min"]
            ].join(
                con[
                    ["jump_10min_kmh", "jump_30min_kmh", "jump_60min_kmh", "log_jump_30min", "log_jump_60min"]
                ],
                how="inner",
                lsuffix="_caitian",
                rsuffix="_control",
            )
            for metric in ["jump_10min_kmh", "jump_30min_kmh", "jump_60min_kmh", "log_jump_30min", "log_jump_60min"]:
                merged[f"dd_{metric}"] = (
                    merged[f"{metric}_caitian"] - merged[f"{metric}_control"]
                )
            for metric in ["jump_10min_kmh", "jump_30min_kmh", "jump_60min_kmh", "log_jump_30min", "log_jump_60min"]:
                values = merged[f"dd_{metric}"]
                rows.append(
                    {
                        "control_id": control_id,
                        "control_label": control_label,
                        "direction_group": direction,
                        "direction_label": DIRECTION_LABELS[direction],
                        "metric": metric,
                        "n_weekdays": int(values.notna().sum()),
                        "mean_dd": float(values.mean()),
                        "se_dd": float(values.std(ddof=1) / np.sqrt(values.notna().sum())),
                        "min_daily_dd": float(values.min()),
                        "max_daily_dd": float(values.max()),
                        "all_weekdays_positive": bool((values > 0).all()),
                        "all_weekdays_negative": bool((values < 0).all()),
                    }
                )
    out = pd.DataFrame(rows)
    out["ci95_low"] = out["mean_dd"] - 1.96 * out["se_dd"]
    out["ci95_high"] = out["mean_dd"] + 1.96 * out["se_dd"]
    return out


def build_segment_event_deltas(segment_panel: pd.DataFrame, core: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    attrs = core[
        [
            "roadseg_id",
            "direction_group",
            "core_sequence",
            "centroid_lat",
        ]
    ].copy()
    panel = segment_panel.merge(attrs, on=["roadseg_id", "direction_group"], how="inner")
    panel = panel[
        panel["day_type"].eq("weekday")
        & (panel["minute_of_day"] >= PLOT_WINDOW[0])
        & (panel["minute_of_day"] <= PLOT_WINDOW[1])
    ].copy()
    base = (
        panel[
            (panel["minute_of_day"] >= LOCAL_PRE[0])
            & (panel["minute_of_day"] < LOCAL_PRE[1])
        ]
        .groupby("roadseg_id", as_index=False)
        .agg(pre_speed_kmh=("speed_kmh", "mean"))
    )
    event = (
        panel.groupby(
            [
                "roadseg_id",
                "direction_group",
                "core_sequence",
                "centroid_lat",
                "semantic",
                "length_m",
                "minute_of_day",
            ],
            as_index=False,
        )
        .agg(speed_kmh=("speed_kmh", "mean"))
        .merge(base, on="roadseg_id", how="left")
    )
    event["event_min"] = event["minute_of_day"] - EVENT_START_MIN
    event["delta_speed_kmh"] = event["speed_kmh"] - event["pre_speed_kmh"]
    event["north_to_south_rank"] = (
        event.groupby("direction_group")["centroid_lat"]
        .rank(method="dense", ascending=False)
        .astype(int)
    )
    rollout_rows = []
    rollout_window = event[(event["event_min"] >= 0) & (event["event_min"] <= 90)].copy()
    for (direction, roadseg_id), group in rollout_window.groupby(["direction_group", "roadseg_id"]):
        first_gt5 = group[group["delta_speed_kmh"] > 5]["event_min"].min()
        first_lt_minus5 = group[group["delta_speed_kmh"] < -5]["event_min"].min()
        row = group.iloc[0]
        rollout_rows.append(
            {
                "roadseg_id": roadseg_id,
                "direction_group": direction,
                "direction_label": DIRECTION_LABELS.get(direction, direction),
                "core_sequence": int(row["core_sequence"]),
                "north_to_south_rank": int(row["north_to_south_rank"]),
                "centroid_lat": float(row["centroid_lat"]),
                "semantic": row["semantic"],
                "pre_speed_kmh": float(row["pre_speed_kmh"]),
                "max_delta_0_90_kmh": float(group["delta_speed_kmh"].max()),
                "min_delta_0_90_kmh": float(group["delta_speed_kmh"].min()),
                "first_event_min_gt5": float(first_gt5) if pd.notna(first_gt5) else np.nan,
                "first_event_min_lt_minus5": float(first_lt_minus5) if pd.notna(first_lt_minus5) else np.nan,
            }
        )
    return event, pd.DataFrame(rollout_rows)


def plot_corridor_breakpoint(corridor: pd.DataFrame, output_path: Path) -> None:
    plot_data = corridor[
        corridor["day_type"].eq("weekday")
        & corridor["corridor_id"].isin(CORRIDOR_ORDER)
        & (corridor["minute_of_day"] >= PLOT_WINDOW[0])
        & (corridor["minute_of_day"] <= PLOT_WINDOW[1])
    ].copy()
    mean_data = (
        plot_data.groupby(["corridor_id", "corridor_label", "direction_group", "minute_of_day"], as_index=False)
        .agg(speed_kmh=("corridor_speed_kmh", "mean"))
    )
    fig, axes = plt.subplots(1, 2, figsize=(12.4, 4.4), sharey=True)
    colors = {
        "caitian": "#2b6cb0",
        "control_huanggang": "#8c8c8c",
        "control_jintian": "#b0b0b0",
        "control_yitian": "#d0d0d0",
        "control_xiangmihu": "#f2a65a",
    }
    for ax, direction in zip(axes, DIRECTION_ORDER):
        sub = mean_data[mean_data["direction_group"].eq(direction)]
        for corridor_id, part in sub.groupby("corridor_id"):
            label = part["corridor_label"].iloc[0]
            ax.plot(
                part["minute_of_day"] / 60,
                part["speed_kmh"],
                color=colors.get(corridor_id, "#999999"),
                linewidth=2.0 if corridor_id == "caitian" else 1.2,
                marker="o",
                markersize=3.2 if corridor_id == "caitian" else 2.5,
                alpha=0.96 if corridor_id == "caitian" else 0.82,
                label=label,
            )
        ax.axvline(EVENT_START_MIN / 60.0, color="#111111", linewidth=0.9, linestyle="--")
        ax.axvspan(
            ACTIVE_WINDOW[0] / 60.0,
            ACTIVE_WINDOW[1] / 60.0,
            color="#eeeeee",
            alpha=0.55,
        )
        ax.set_title(DIRECTION_LABELS[direction], loc="left", fontweight="bold")
        ax.set_xlabel("Hour")
        ax.set_xlim(PLOT_WINDOW[0] / 60.0, PLOT_WINDOW[1] / 60.0)
        ax.grid(axis="y", color="#e5e5e5", linewidth=0.5)
        for spine in ["top", "right"]:
            ax.spines[spine].set_visible(False)
    axes[0].set_ylabel("Mean speed, km/h")
    axes[1].legend(frameon=False, loc="best")
    fig.suptitle("Time series around Caitian 16:30-21:30 window, weekdays", x=0.01, ha="left")
    fig.tight_layout(rect=(0, 0, 1, 0.92))
    save_figure(fig, output_path)


def plot_corridor_breakpoint_by_date(corridor: pd.DataFrame, output_path: Path) -> None:
    plot_data = corridor[
        corridor["day_type"].eq("weekday")
        & corridor["corridor_id"].isin(CORRIDOR_ORDER)
        & (corridor["minute_of_day"] >= PLOT_WINDOW[0])
        & (corridor["minute_of_day"] <= PLOT_WINDOW[1])
    ].copy()
    plot_data["date_ts"] = pd.to_datetime(plot_data["date"])
    dates = sorted(plot_data["date_ts"].unique())
    mean_data = (
        plot_data.groupby(
            ["date_ts", "corridor_id", "corridor_label", "direction_group", "minute_of_day"],
            as_index=False,
        )
        .agg(speed_kmh=("corridor_speed_kmh", "mean"))
    )
    fig, axes = plt.subplots(len(dates), 2, figsize=(12.8, 2.55 * len(dates)), sharex=True, sharey=True)
    colors = {
        "caitian": "#2b6cb0",
        "control_huanggang": "#8c8c8c",
        "control_jintian": "#b0b0b0",
        "control_yitian": "#d0d0d0",
        "control_xiangmihu": "#f2a65a",
    }
    for row, date_ts in enumerate(dates):
        for col, direction in enumerate(DIRECTION_ORDER):
            ax = axes[row, col]
            sub = mean_data[
                mean_data["date_ts"].eq(date_ts)
                & mean_data["direction_group"].eq(direction)
            ].copy()
            for corridor_id, part in sub.groupby("corridor_id"):
                label = part["corridor_label"].iloc[0]
                ax.plot(
                    part["minute_of_day"] / 60,
                    part["speed_kmh"],
                    color=colors.get(corridor_id, "#999999"),
                    linewidth=1.65 if corridor_id == "caitian" else 1.05,
                    marker="o",
                    markersize=2.4 if corridor_id == "caitian" else 1.9,
                    alpha=0.96 if corridor_id == "caitian" else 0.78,
                    label=label,
                )
            ax.axvline(EVENT_START_MIN / 60.0, color="#111111", linewidth=0.8, linestyle="--")
            ax.axvline(ACTIVE_WINDOW[1] / 60.0, color="#111111", linewidth=0.8, linestyle=":")
            ax.axvspan(
                ACTIVE_WINDOW[0] / 60.0,
                ACTIVE_WINDOW[1] / 60.0,
                color="#eeeeee",
                alpha=0.55,
            )
            ax.set_xlim(PLOT_WINDOW[0] / 60.0, PLOT_WINDOW[1] / 60.0)
            if row == 0:
                ax.set_title(DIRECTION_LABELS[direction], loc="left", fontweight="bold")
            if col == 0:
                ax.set_ylabel(f"{date_label(date_ts)}\nkm/h")
            ax.grid(axis="y", color="#e5e5e5", linewidth=0.5)
            for spine in ["top", "right"]:
                ax.spines[spine].set_visible(False)
    for ax in axes[-1, :]:
        ax.set_xlabel("Hour")
    handles, labels = axes[0, 1].get_legend_handles_labels()
    if handles:
        fig.legend(handles, labels, frameon=False, loc="center right", bbox_to_anchor=(0.995, 0.5))
    fig.suptitle("Caitian 16:30-21:30 window by weekday date, 10-minute points", x=0.01, ha="left")
    fig.tight_layout(rect=(0, 0, 0.9, 0.97))
    save_figure(fig, output_path)


def plot_local_jump_dd(local_dd: pd.DataFrame, output_path: Path) -> None:
    plot_data = local_dd[local_dd["metric"].eq("jump_30min_kmh")].copy()
    fig, axes = plt.subplots(1, 2, figsize=(10.0, 4.0), sharey=True)
    x = np.arange(len(CONTROL_ORDER))
    for ax, direction in zip(axes, DIRECTION_ORDER):
        sub = plot_data[plot_data["direction_group"].eq(direction)].set_index("control_id").reindex(CONTROL_ORDER)
        ax.bar(
            x,
            sub["mean_dd"],
            color="#2b6cb0" if direction == POLICY_DIRECTION else "#c44e52",
            alpha=0.85,
        )
        ax.errorbar(
            x,
            sub["mean_dd"],
            yerr=1.96 * sub["se_dd"],
            fmt="none",
            ecolor="#333333",
            elinewidth=0.8,
            capsize=2,
        )
        ax.axhline(0, color="#111111", linewidth=0.8)
        ax.set_xticks(x)
        ax.set_xticklabels(sub["control_label"], rotation=25, ha="right")
        ax.set_title(DIRECTION_LABELS[direction], loc="left", fontweight="bold")
        ax.grid(axis="y", color="#e5e5e5", linewidth=0.5)
        for spine in ["top", "right"]:
            ax.spines[spine].set_visible(False)
    axes[0].set_ylabel("Caitian-control DD in 16:30 jump, km/h")
    fig.suptitle("16:30 local jump: 16:30-17:00 minus 16:00-16:30", x=0.01, ha="left")
    fig.tight_layout(rect=(0, 0, 1, 0.92))
    save_figure(fig, output_path)


def plot_segment_heatmap(event: pd.DataFrame, output_path: Path) -> None:
    plot_event = event[
        (event["event_min"] >= PLOT_WINDOW[0] - EVENT_START_MIN)
        & (event["event_min"] < PLOT_WINDOW[1] - EVENT_START_MIN)
    ].copy()
    fig, axes = plt.subplots(
        1,
        3,
        figsize=(12.8, 5.0),
        sharex=False,
        gridspec_kw={"width_ratios": [1, 1, 0.04], "wspace": 0.18},
    )
    plot_axes = axes[:2]
    cax = axes[2]
    vmax = np.nanpercentile(np.abs(plot_event["delta_speed_kmh"]), 95)
    vmax = max(vmax, 8)
    for ax, direction in zip(plot_axes, DIRECTION_ORDER):
        sub = plot_event[plot_event["direction_group"].eq(direction)].copy()
        pivot = sub.pivot_table(
            index="north_to_south_rank",
            columns="event_min",
            values="delta_speed_kmh",
            aggfunc="mean",
        ).sort_index()
        image = ax.imshow(
            pivot.to_numpy(),
            aspect="auto",
            cmap="RdBu",
            vmin=-vmax,
            vmax=vmax,
            interpolation="nearest",
        )
        ax.set_title(DIRECTION_LABELS[direction], loc="left", fontweight="bold")
        ax.set_yticks(np.arange(len(pivot.index)))
        ax.set_yticklabels([f"N{int(idx)}" for idx in pivot.index])
        tick_locs = [
            idx for idx, col in enumerate(pivot.columns) if int(col) % 60 == 0 or int(col) == -30
        ]
        ax.set_xticks(tick_locs)
        ax.set_xticklabels(
            [f"{int(pivot.columns[idx]):+d}" for idx in tick_locs],
            rotation=45,
            ha="right",
        )
        zero_pos = list(pivot.columns).index(0) if 0 in pivot.columns else None
        if zero_pos is not None:
            ax.axvline(zero_pos - 0.5, color="#111111", linewidth=0.8, linestyle="--")
        active_end_pos = list(pivot.columns).index(300) if 300 in pivot.columns else None
        if active_end_pos is not None:
            ax.axvline(active_end_pos + 0.5, color="#111111", linewidth=0.8, linestyle=":")
        ax.set_xlabel("Minutes relative to 16:30")
        ax.set_ylabel("Segment order, north to south")
    cbar = fig.colorbar(image, cax=cax)
    cbar.set_label("Speed minus 16:00-16:30 baseline, km/h")
    fig.suptitle("Caitian core segment event-time heatmap, weekdays", x=0.01, ha="left")
    fig.subplots_adjust(top=0.84, bottom=0.22, left=0.06, right=0.94)
    save_figure(fig, output_path)


def main() -> None:
    args = parse_args()
    configure_plot()
    data_dir = args.output_dir / "data"
    metrics_dir = args.output_dir / "metrics"
    figures_dir = args.output_dir / "figures"

    corridor = pd.read_csv(data_dir / "corridor_10min_speed.csv")
    segment_panel = pd.read_csv(data_dir / "selected_segment_speed_panel.csv")
    core = pd.read_csv(args.output_dir / "metrics/treatment_core_segments.csv")

    jump_daily = build_local_jump_daily(corridor)
    jump_summary = build_local_jump_summary(jump_daily)
    local_dd = build_local_jump_dd(jump_daily)
    weekday_date_summary = build_weekday_date_window_summary(corridor)
    segment_event, rollout = build_segment_event_deltas(segment_panel, core)

    jump_daily.to_csv(data_dir / "post_only_1630_local_jump_daily.csv", index=False)
    weekday_date_summary.to_csv(
        data_dir / "post_only_1630_weekday_date_window_summary.csv",
        index=False,
    )
    segment_event.to_csv(data_dir / "post_only_1630_segment_event_deltas.csv", index=False)
    jump_summary.to_csv(metrics_dir / "post_only_1630_local_jump_summary.csv", index=False)
    local_dd.to_csv(metrics_dir / "post_only_1630_local_jump_dd.csv", index=False)
    rollout.to_csv(metrics_dir / "post_only_1630_segment_rollout_summary.csv", index=False)
    spec = {
        "question": "Whether a visible speed breakpoint appears around 16:30 in the post-only week.",
        "local_pre_window": "16:00-16:30",
        "local_post_30_window": "16:30-17:00",
        "local_post_60_window": "16:30-17:30",
        "plot_window": "14:30-23:30",
        "active_window": "16:30-21:30",
        "time_trend_control": (
            "Caitian jump minus each control corridor's jump in the same direction and weekday."
        ),
        "segment_rollout": (
            "Caitian core segment speeds are ordered north to south by centroid latitude. "
            "Each cell subtracts that segment's 16:00-16:30 baseline speed."
        ),
        "interpretation": (
            "A sharp jump at 16:30 in the policy direction is supportive of an operational breakpoint. "
            "A spatially staggered heatmap would support a rollout process, but 10-minute Baidu data "
            "cannot measure sub-10-minute setup timing."
        ),
    }
    with (metrics_dir / "post_only_1630_breakpoint_spec.json").open("w", encoding="utf-8") as fh:
        json.dump(spec, fh, ensure_ascii=False, indent=2)

    plot_corridor_breakpoint(
        corridor,
        figures_dir / "post_only_1630_corridor_timeseries.png",
    )
    plot_corridor_breakpoint_by_date(
        corridor,
        figures_dir / "post_only_1630_corridor_timeseries_by_weekday_date.png",
    )
    plot_local_jump_dd(
        local_dd,
        figures_dir / "post_only_1630_local_jump_dd.png",
    )
    plot_segment_heatmap(
        segment_event,
        figures_dir / "post_only_1630_segment_rollout_heatmap.png",
    )


if __name__ == "__main__":
    main()
