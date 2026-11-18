#!/usr/bin/env python3
"""Summarize and plot the 2026-07-09 outbound speed-density pilot."""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR / "batch_outputs" / "20260709_speed_density_pilot"
OUT = ROOT / "analysis_v1"
FLOW_PANEL = SCRIPT_DIR / "batch_outputs" / "20260709_identification_first_hour_overnight_mps" / "panel_by_line.csv"

# Provisional line-marking calibration.  All distance-free outputs remain valid
# if this value is revised later.
DISTANCE_REF_M = 36.0
DISTANCE_LOW_M = 30.0
DISTANCE_HIGH_M = 42.0

PHASE_DIRS = {
    "pre": ROOT / "pre_000_165",
    "post": ROOT / "post_immediate_210_510",
}
PHASE_START = {"pre": 0.0, "post": 210.0}
PHASE_LABEL = {"pre": "Pre 0:00–2:45", "post": "Post 3:30–8:30"}
DIRECTION_LABEL = {"south_to_north": "South → North", "north_to_south": "North → South"}
DIRECTION_COLOR = {"south_to_north": "#4C78A8", "north_to_south": "#B279A2"}

STYLE_ROOT = Path.home() / ".codex" / "skills" / "top-journal-scientific-figures" / "scripts"
sys.path.insert(0, str(STYLE_ROOT))
from journal_style import add_panel_label, apply_axis_style, save_publication_figure, setup_publication_style  # noqa: E402


def load_inputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    speed_frames = []
    density_frames = []
    crossing_frames = []
    for phase, directory in PHASE_DIRS.items():
        speed = pd.read_csv(directory / "speed_gate_matches.csv")
        density = pd.read_csv(directory / "density_by_frame.csv")
        crossings = pd.read_csv(directory / "gate_crossings.csv")
        speed["phase"] = phase
        density["phase"] = phase
        crossings["phase"] = phase
        speed_frames.append(speed)
        density_frames.append(density)
        crossing_frames.append(crossings)
    return (
        pd.concat(speed_frames, ignore_index=True),
        pd.concat(density_frames, ignore_index=True),
        pd.concat(crossing_frames, ignore_index=True),
    )


def mark_speed_qa(speed: pd.DataFrame) -> pd.DataFrame:
    speed = speed.copy()
    speed["qa_keep"] = False
    speed["qa_reason"] = ""
    for _, index in speed.groupby(["phase", "direction"]).groups.items():
        values = speed.loc[index, "travel_time_s"].astype(float)
        median = float(values.median())
        mad = float((values - median).abs().median())
        threshold = max(0.5, 4.0 * 1.4826 * mad)
        keep = (values - median).abs() <= threshold
        speed.loc[index, "qa_keep"] = keep.to_numpy()
        speed.loc[index, "qa_reason"] = np.where(keep, "keep", "group_median_4mad_outlier")
    speed["speed_kmh_ref"] = 3.6 * DISTANCE_REF_M / speed["travel_time_s"]
    speed["speed_kmh_low_distance"] = 3.6 * DISTANCE_LOW_M / speed["travel_time_s"]
    speed["speed_kmh_high_distance"] = 3.6 * DISTANCE_HIGH_M / speed["travel_time_s"]
    return speed


def make_density_bins(density: pd.DataFrame) -> pd.DataFrame:
    density = density.copy()
    density["seconds_from_phase_start"] = density.apply(
        lambda row: float(row["timestamp_s"]) - PHASE_START[str(row["phase"])], axis=1
    )
    density["bin_10s"] = np.floor(density["seconds_from_phase_start"] / 10.0).astype(int)
    binned = (
        density.groupby(["phase", "direction", "lanes", "bin_10s"], as_index=False)
        .agg(
            timestamp_s_mean=("timestamp_s", "mean"),
            visible_vehicle_count=("visible_vehicle_count", "mean"),
            vehicles_per_lane_in_roi=("vehicles_per_lane_in_roi", "mean"),
            sampled_frames=("frame_index", "size"),
        )
    )
    binned["density_veh_per_lane_km_ref"] = binned["vehicles_per_lane_in_roi"] / (DISTANCE_REF_M / 1000.0)
    return binned


def load_flow() -> pd.DataFrame:
    panel = pd.read_csv(FLOW_PANEL)
    panel = panel[panel["source_video"].str.endswith("出城方向1621开始.MOV")].copy()
    panel["phase"] = np.select(
        [
            panel["segment_id"].str.contains("000000_000245"),
            panel["segment_id"].str.contains("000330_000830"),
        ],
        ["pre", "post"],
        default="",
    )
    panel = panel[panel["phase"].ne("")].copy()
    return panel[
        [
            "phase",
            "direction",
            "duration_s",
            "lanes",
            "count",
            "vehicles_per_hour",
            "vehicles_per_lane_hour",
        ]
    ].reset_index(drop=True)


def build_summary(
    speed: pd.DataFrame,
    density: pd.DataFrame,
    crossings: pd.DataFrame,
    flow: pd.DataFrame,
) -> pd.DataFrame:
    rows = []
    speed_qa = speed[speed["qa_keep"]].copy()
    for phase in ("pre", "post"):
        for direction in ("south_to_north", "north_to_south"):
            raw = speed[(speed["phase"].eq(phase)) & (speed["direction"].eq(direction))]
            clean = speed_qa[(speed_qa["phase"].eq(phase)) & (speed_qa["direction"].eq(direction))]
            den = density[(density["phase"].eq(phase)) & (density["direction"].eq(direction))]
            cross = crossings[(crossings["phase"].eq(phase)) & (crossings["direction"].eq(direction))]
            gate_counts = cross.groupby("gate").size()
            flow_row = flow[(flow["phase"].eq(phase)) & (flow["direction"].eq(direction))].iloc[0]

            mean_visible = float(den["visible_vehicle_count"].mean())
            mean_per_lane = float(den["vehicles_per_lane_in_roi"].mean())
            travel_mean = float(clean["travel_time_s"].mean())
            travel_median = float(clean["travel_time_s"].median())
            q_pred = mean_per_lane / travel_mean * 3600.0
            limiting_gate = min(int(gate_counts.get("far", 0)), int(gate_counts.get("near", 0)))
            rows.append(
                {
                    "phase": phase,
                    "phase_label": PHASE_LABEL[phase],
                    "direction": direction,
                    "direction_label": DIRECTION_LABEL[direction],
                    "lanes": int(flow_row["lanes"]),
                    "speed_matches_raw": len(raw),
                    "speed_matches_qa": len(clean),
                    "far_gate_crossings": int(gate_counts.get("far", 0)),
                    "near_gate_crossings": int(gate_counts.get("near", 0)),
                    "match_rate_limiting_gate": len(raw) / limiting_gate if limiting_gate else np.nan,
                    "travel_time_mean_s": travel_mean,
                    "travel_time_median_s": travel_median,
                    "median_speed_kmh_ref_36m": 3.6 * DISTANCE_REF_M / travel_median,
                    "median_speed_kmh_low_30m": 3.6 * DISTANCE_LOW_M / travel_median,
                    "median_speed_kmh_high_42m": 3.6 * DISTANCE_HIGH_M / travel_median,
                    "mean_visible_vehicles": mean_visible,
                    "mean_visible_vehicles_per_lane": mean_per_lane,
                    "density_veh_per_lane_km_ref_36m": mean_per_lane / (DISTANCE_REF_M / 1000.0),
                    "density_veh_per_lane_km_low_at_42m": mean_per_lane / (DISTANCE_HIGH_M / 1000.0),
                    "density_veh_per_lane_km_high_at_30m": mean_per_lane / (DISTANCE_LOW_M / 1000.0),
                    "flow_vehicles_per_hour": float(flow_row["vehicles_per_hour"]),
                    "flow_vehicles_per_lane_hour": float(flow_row["vehicles_per_lane_hour"]),
                    "qkv_predicted_vehicles_per_lane_hour": q_pred,
                    "qkv_gap_percent": 100.0 * (q_pred / float(flow_row["vehicles_per_lane_hour"]) - 1.0),
                }
            )
    return pd.DataFrame(rows)


def categorical_positions(summary: pd.DataFrame) -> tuple[list[tuple[str, str]], dict[tuple[str, str], float]]:
    order = [
        ("south_to_north", "pre"),
        ("south_to_north", "post"),
        ("north_to_south", "pre"),
        ("north_to_south", "post"),
    ]
    positions = {key: value for key, value in zip(order, [0.0, 1.0, 3.0, 4.0])}
    return order, positions


def plot_overview(speed: pd.DataFrame, density_bins: pd.DataFrame, summary: pd.DataFrame) -> None:
    setup_publication_style()
    rng = np.random.default_rng(20260709)
    order, positions = categorical_positions(summary)
    labels = ["S→N\nPre", "S→N\nPost", "N→S\nPre", "N→S\nPost"]
    xs = [positions[key] for key in order]

    fig, axes = plt.subplots(2, 2, figsize=(7.1, 5.4))
    fig.subplots_adjust(wspace=0.34, hspace=0.42)

    ax = axes[0, 0]
    clean = speed[speed["qa_keep"]]
    box_values = []
    for direction, phase in order:
        values = clean[(clean["direction"].eq(direction)) & (clean["phase"].eq(phase))]["travel_time_s"].to_numpy()
        box_values.append(values)
        jitter = rng.uniform(-0.16, 0.16, size=len(values))
        ax.scatter(
            positions[(direction, phase)] + jitter,
            values,
            s=8,
            alpha=0.35,
            color=DIRECTION_COLOR[direction],
            edgecolors="none",
            rasterized=True,
        )
    ax.boxplot(
        box_values,
        positions=xs,
        widths=0.52,
        patch_artist=True,
        showfliers=False,
        medianprops={"color": "black", "linewidth": 1.1},
        whiskerprops={"color": "black", "linewidth": 0.8},
        capprops={"color": "black", "linewidth": 0.8},
        boxprops={"facecolor": "white", "edgecolor": "black", "linewidth": 0.8},
    )
    ax.set_yscale("log")
    ax.set_ylabel("Gate travel time (s, log scale)")
    ax.set_xticks(xs, labels)
    ax.set_title("Tracked travel time between confirmed gates")
    apply_axis_style(ax)
    add_panel_label(ax, "a")

    ax = axes[0, 1]
    for direction, phase in order:
        values = density_bins[
            (density_bins["direction"].eq(direction)) & (density_bins["phase"].eq(phase))
        ]["vehicles_per_lane_in_roi"].to_numpy()
        jitter = rng.uniform(-0.12, 0.12, size=len(values))
        x = positions[(direction, phase)]
        ax.scatter(x + jitter, values, s=13, alpha=0.55, color=DIRECTION_COLOR[direction], edgecolors="none")
        ax.plot([x - 0.22, x + 0.22], [np.mean(values), np.mean(values)], color="black", linewidth=1.2)
    ax.set_ylabel("Visible vehicles per lane in ROI")
    ax.set_xticks(xs, labels)
    ax.set_title("Ten-second density-proxy blocks")
    apply_axis_style(ax)
    add_panel_label(ax, "b")

    ax = axes[1, 0]
    for direction, phase in order:
        row = summary[(summary["direction"].eq(direction)) & (summary["phase"].eq(phase))].iloc[0]
        x = positions[(direction, phase)]
        ax.scatter(x, row["flow_vehicles_per_lane_hour"], s=36, color=DIRECTION_COLOR[direction], edgecolors="black", linewidths=0.5)
    ax.set_ylabel("Flow (vehicles per lane-hour)")
    ax.set_xticks(xs, labels)
    ax.set_title("Existing line-count throughput")
    apply_axis_style(ax)
    add_panel_label(ax, "c")

    ax = axes[1, 1]
    max_value = 1.1 * max(
        summary["flow_vehicles_per_lane_hour"].max(), summary["qkv_predicted_vehicles_per_lane_hour"].max()
    )
    ax.plot([0, max_value], [0, max_value], color="#777777", linestyle="--", linewidth=0.9)
    phase_marker = {"pre": "o", "post": "s"}
    for row in summary.itertuples(index=False):
        ax.scatter(
            row.flow_vehicles_per_lane_hour,
            row.qkv_predicted_vehicles_per_lane_hour,
            s=38,
            marker=phase_marker[row.phase],
            color=DIRECTION_COLOR[row.direction],
            edgecolors="black",
            linewidths=0.5,
        )
        ax.annotate(
            f"{DIRECTION_LABEL[row.direction].replace('South → North', 'S→N').replace('North → South', 'N→S')} {row.phase}",
            (row.flow_vehicles_per_lane_hour, row.qkv_predicted_vehicles_per_lane_hour),
            xytext=(4, 4),
            textcoords="offset points",
            fontsize=6.8,
        )
    ax.set_xlim(0, max_value)
    ax.set_ylim(0, max_value)
    ax.set_xlabel("Observed flow (vehicles per lane-hour)")
    ax.set_ylabel("Density × space-mean speed")
    ax.set_title("Internal q–k–v consistency check")
    ax.set_aspect("equal", adjustable="box")
    apply_axis_style(ax)
    add_panel_label(ax, "d")

    fig.suptitle("2026-07-09 Caitian Road outbound-camera pilot", fontsize=10.5, y=0.995)
    save_publication_figure(fig, OUT / "speed_density_overview", formats=("svg", "pdf", "png"), dpi=600)
    plt.close(fig)


def plot_calibration_sensitivity(summary: pd.DataFrame) -> None:
    setup_publication_style()
    order, positions = categorical_positions(summary)
    labels = ["S→N\nPre", "S→N\nPost", "N→S\nPre", "N→S\nPost"]
    xs = [positions[key] for key in order]
    fig, axes = plt.subplots(1, 2, figsize=(7.1, 2.9))
    fig.subplots_adjust(wspace=0.32, bottom=0.23)

    ax = axes[0]
    for direction, phase in order:
        row = summary[(summary["direction"].eq(direction)) & (summary["phase"].eq(phase))].iloc[0]
        center = row["median_speed_kmh_ref_36m"]
        low = row["median_speed_kmh_low_30m"]
        high = row["median_speed_kmh_high_42m"]
        x = positions[(direction, phase)]
        ax.errorbar(
            x,
            center,
            yerr=[[center - low], [high - center]],
            fmt="o",
            markersize=5,
            color=DIRECTION_COLOR[direction],
            markeredgecolor="black",
            markeredgewidth=0.5,
            ecolor="black",
            elinewidth=0.8,
            capsize=2.5,
        )
    ax.set_ylabel("Median speed (km/h)")
    ax.set_xticks(xs, labels)
    ax.set_title("Provisional absolute speed")
    apply_axis_style(ax)
    add_panel_label(ax, "a")

    ax = axes[1]
    for direction, phase in order:
        row = summary[(summary["direction"].eq(direction)) & (summary["phase"].eq(phase))].iloc[0]
        center = row["density_veh_per_lane_km_ref_36m"]
        low = row["density_veh_per_lane_km_low_at_42m"]
        high = row["density_veh_per_lane_km_high_at_30m"]
        x = positions[(direction, phase)]
        ax.errorbar(
            x,
            center,
            yerr=[[center - low], [high - center]],
            fmt="o",
            markersize=5,
            color=DIRECTION_COLOR[direction],
            markeredgecolor="black",
            markeredgewidth=0.5,
            ecolor="black",
            elinewidth=0.8,
            capsize=2.5,
        )
    ax.set_ylabel("Density (vehicles per lane-km)")
    ax.set_xticks(xs, labels)
    ax.set_title("Provisional absolute density")
    apply_axis_style(ax)
    add_panel_label(ax, "b")

    fig.text(
        0.5,
        0.04,
        "Points use D = 36 m; ranges show D = 30–42 m calibration sensitivity, not sampling uncertainty.",
        ha="center",
        va="bottom",
        fontsize=7.5,
    )
    save_publication_figure(fig, OUT / "absolute_metric_calibration_sensitivity", formats=("svg", "pdf", "png"), dpi=600)
    plt.close(fig)


def write_notes(summary: pd.DataFrame) -> None:
    def row(phase: str, direction: str) -> pd.Series:
        return summary[(summary["phase"].eq(phase)) & (summary["direction"].eq(direction))].iloc[0]

    lines = [
        "# 2026-07-09 outbound speed-density pilot",
        "",
        "## Sample",
        "",
        "- Pre: 0:00–2:45, normal lane allocation.",
        "- Transition: 2:45–3:30, excluded.",
        "- Post-immediate: 3:30–8:30, stable tidal-lane operation.",
        "- Speed uses the bounding-box centre crossing the user-confirmed gates; density ROI assignment uses the bounding-box bottom centre.",
        "",
        "## Distance-free findings",
        "",
    ]
    for direction in ("south_to_north", "north_to_south"):
        pre = row("pre", direction)
        post = row("post", direction)
        speed_change = 100.0 * (pre["travel_time_median_s"] / post["travel_time_median_s"] - 1.0)
        density_change = 100.0 * (
            post["mean_visible_vehicles_per_lane"] / pre["mean_visible_vehicles_per_lane"] - 1.0
        )
        lines.append(
            f"- {DIRECTION_LABEL[direction]}: median gate time {pre['travel_time_median_s']:.2f}s → "
            f"{post['travel_time_median_s']:.2f}s; implied relative speed change {speed_change:+.1f}%. "
            f"Visible vehicles per lane {pre['mean_visible_vehicles_per_lane']:.2f} → "
            f"{post['mean_visible_vehicles_per_lane']:.2f} ({density_change:+.1f}%)."
        )
    lines.extend(
        [
            "",
            "## Provisional physical calibration",
            "",
            f"- Reference gate distance: {DISTANCE_REF_M:.0f}m; sensitivity interval: {DISTANCE_LOW_M:.0f}–{DISTANCE_HIGH_M:.0f}m.",
            "- Absolute speed and density depend linearly/inversely on this distance and remain provisional until one field or map distance is supplied.",
            "",
            "## Quality caveats",
            "",
            "- South-to-north is congested and heavily occluded. Its far-gate track matching is substantially lower than north-to-south, so the speed sample overrepresents continuously visible vehicles.",
            "- The pre window is only 165 seconds. These are measurement and implementation-process results, not a causal RDD.",
            "- The q–k–v comparison is an internal measurement check; it does not eliminate common detection error across flow, speed, and density.",
        ]
    )
    (OUT / "analysis_notes.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    speed, density, crossings = load_inputs()
    speed = mark_speed_qa(speed)
    density_bins = make_density_bins(density)
    flow = load_flow()
    summary = build_summary(speed, density, crossings, flow)

    speed.to_csv(OUT / "speed_gate_matches_qa.csv", index=False)
    density_bins.to_csv(OUT / "density_10s.csv", index=False)
    flow.to_csv(OUT / "flow_same_windows.csv", index=False)
    summary.to_csv(OUT / "phase_direction_summary.csv", index=False)
    summary[
        [
            "phase",
            "direction",
            "median_speed_kmh_low_30m",
            "median_speed_kmh_ref_36m",
            "median_speed_kmh_high_42m",
            "density_veh_per_lane_km_low_at_42m",
            "density_veh_per_lane_km_ref_36m",
            "density_veh_per_lane_km_high_at_30m",
        ]
    ].to_csv(OUT / "distance_calibration_sensitivity.csv", index=False)

    plot_overview(speed, density_bins, summary)
    plot_calibration_sensitivity(summary)
    write_notes(summary)
    print(summary.round(3).to_string(index=False))
    print(f"output={OUT}")


if __name__ == "__main__":
    main()
