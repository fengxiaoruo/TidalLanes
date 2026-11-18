#!/usr/bin/env python3
"""Compare pre, immediate-post, and stable-post video measurement windows."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd


SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR / "batch_outputs" / "20260709_speed_density_pilot"
OUT = ROOT / "analysis_three_windows_v1"
FLOW_PANEL = (
    SCRIPT_DIR
    / "batch_outputs"
    / "20260709_identification_first_hour_overnight_mps"
    / "panel_by_line.csv"
)

WINDOWS = {
    "pre": {
        "directory": ROOT / "pre_000_165",
        "start_s": 0.0,
        "end_s": 165.0,
        "label": "Pre 0:00-2:45",
        "flow_segments": ["001_000000_000245_outbound_pre"],
    },
    "post_immediate": {
        "directory": ROOT / "post_immediate_210_510",
        "start_s": 210.0,
        "end_s": 510.0,
        "label": "Immediate post 3:30-8:30",
        "flow_segments": ["002_000330_000830_outbound_post"],
    },
    "post_stable": {
        "directory": ROOT / "post_stable_2010_2910",
        "start_s": 2010.0,
        "end_s": 2910.0,
        "label": "Later post 33:30-48:30",
        "flow_segments": [
            "008_003330_003830_outbound_post",
            "009_003830_004330_outbound_post",
            "010_004330_004830_outbound_post",
        ],
    },
}

WINDOW_ORDER = list(WINDOWS)
DIRECTION_ORDER = ["south_to_north", "north_to_south"]
DIRECTION_LABEL = {
    "south_to_north": "South to North",
    "north_to_south": "North to South",
}


def load_measurements() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    speed_frames = []
    density_frames = []
    crossing_frames = []
    for window, config in WINDOWS.items():
        directory = Path(config["directory"])
        required = [
            directory / "speed_gate_matches.csv",
            directory / "density_by_frame.csv",
            directory / "gate_crossings.csv",
            directory / "measurement_summary.json",
        ]
        missing = [str(path) for path in required if not path.exists()]
        if missing:
            raise FileNotFoundError(f"Missing inputs for {window}: {missing}")

        summary = json.loads((directory / "measurement_summary.json").read_text(encoding="utf-8"))
        if abs(float(summary["start_s"]) - float(config["start_s"])) > 0.1:
            raise ValueError(f"Unexpected start time for {window}")
        if abs(float(summary["end_s"]) - float(config["end_s"])) > 0.1:
            raise ValueError(f"Unexpected end time for {window}")

        speed = pd.read_csv(directory / "speed_gate_matches.csv")
        density = pd.read_csv(directory / "density_by_frame.csv")
        crossings = pd.read_csv(directory / "gate_crossings.csv")
        for frame in (speed, density, crossings):
            frame["window"] = window
            frame["window_label"] = str(config["label"])
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
    for _, index in speed.groupby(["window", "direction"]).groups.items():
        values = speed.loc[index, "travel_time_s"].astype(float)
        median = float(values.median())
        mad = float((values - median).abs().median())
        threshold = max(0.5, 4.0 * 1.4826 * mad)
        keep = (values - median).abs() <= threshold
        speed.loc[index, "qa_keep"] = keep.to_numpy()
        speed.loc[index, "qa_reason"] = np.where(
            keep,
            "keep",
            "window_direction_median_4mad_outlier",
        )
    return speed


def load_flow() -> pd.DataFrame:
    panel = pd.read_csv(FLOW_PANEL)
    panel = panel[panel["source_video"].str.endswith("出城方向1621开始.MOV")].copy()
    rows = []
    for window, config in WINDOWS.items():
        selected = panel[panel["segment_id"].isin(config["flow_segments"])].copy()
        expected = len(config["flow_segments"]) * len(DIRECTION_ORDER)
        if len(selected) != expected:
            raise ValueError(f"Flow segment match failed for {window}: {len(selected)} rows, expected {expected}")
        for direction in DIRECTION_ORDER:
            group = selected[selected["direction"].eq(direction)]
            lane_values = group["lanes"].dropna().astype(int).unique()
            if len(lane_values) != 1:
                raise ValueError(f"Lane count is not constant for {window}, {direction}")
            duration_s = float(group["duration_s"].sum())
            count = int(group["count"].sum())
            lanes = int(lane_values[0])
            rows.append(
                {
                    "window": window,
                    "window_label": config["label"],
                    "direction": direction,
                    "duration_s": duration_s,
                    "lanes": lanes,
                    "count": count,
                    "vehicles_per_hour": count / duration_s * 3600.0,
                    "vehicles_per_lane_hour": count / duration_s * 3600.0 / lanes,
                }
            )
    return pd.DataFrame(rows)


def build_summary(
    speed: pd.DataFrame,
    density: pd.DataFrame,
    crossings: pd.DataFrame,
    flow: pd.DataFrame,
) -> pd.DataFrame:
    rows = []
    for window in WINDOW_ORDER:
        for direction in DIRECTION_ORDER:
            raw = speed[(speed["window"].eq(window)) & (speed["direction"].eq(direction))]
            clean = raw[raw["qa_keep"]]
            den = density[(density["window"].eq(window)) & (density["direction"].eq(direction))]
            cross = crossings[
                (crossings["window"].eq(window)) & (crossings["direction"].eq(direction))
            ]
            flow_row = flow[(flow["window"].eq(window)) & (flow["direction"].eq(direction))].iloc[0]
            if clean.empty or den.empty:
                raise ValueError(f"Empty retained sample for {window}, {direction}")
            gate_counts = cross.groupby("gate").size()
            limiting_gate = min(int(gate_counts.get("far", 0)), int(gate_counts.get("near", 0)))
            travel_time_mean = float(clean["travel_time_s"].mean())
            mean_visible = float(den["visible_vehicle_count"].mean())
            mean_visible_per_lane = float(den["vehicles_per_lane_in_roi"].mean())
            qkv_predicted = mean_visible_per_lane / travel_time_mean * 3600.0
            rows.append(
                {
                    "window": window,
                    "window_label": WINDOWS[window]["label"],
                    "direction": direction,
                    "direction_label": DIRECTION_LABEL[direction],
                    "duration_s": float(flow_row["duration_s"]),
                    "lanes": int(flow_row["lanes"]),
                    "flow_count": int(flow_row["count"]),
                    "flow_vehicles_per_hour": float(flow_row["vehicles_per_hour"]),
                    "flow_vehicles_per_lane_hour": float(flow_row["vehicles_per_lane_hour"]),
                    "speed_matches_raw": len(raw),
                    "speed_matches_qa": len(clean),
                    "far_gate_crossings": int(gate_counts.get("far", 0)),
                    "near_gate_crossings": int(gate_counts.get("near", 0)),
                    "match_rate_limiting_gate": len(raw) / limiting_gate if limiting_gate else np.nan,
                    "travel_time_mean_s": travel_time_mean,
                    "travel_time_median_s": float(clean["travel_time_s"].median()),
                    "travel_time_p25_s": float(clean["travel_time_s"].quantile(0.25)),
                    "travel_time_p75_s": float(clean["travel_time_s"].quantile(0.75)),
                    "sampled_frames": int(len(den)),
                    "mean_visible_vehicles": mean_visible,
                    "mean_visible_vehicles_per_lane": mean_visible_per_lane,
                    "qkv_predicted_vehicles_per_lane_hour": qkv_predicted,
                    "qkv_gap_percent": 100.0
                    * (qkv_predicted / float(flow_row["vehicles_per_lane_hour"]) - 1.0),
                }
            )
    summary = pd.DataFrame(rows)
    for direction in DIRECTION_ORDER:
        pre = summary[(summary["window"].eq("pre")) & (summary["direction"].eq(direction))].iloc[0]
        mask = summary["direction"].eq(direction)
        summary.loc[mask, "flow_change_vs_pre_pct"] = (
            summary.loc[mask, "flow_vehicles_per_hour"] / pre["flow_vehicles_per_hour"] - 1.0
        ) * 100.0
        summary.loc[mask, "flow_per_lane_change_vs_pre_pct"] = (
            summary.loc[mask, "flow_vehicles_per_lane_hour"] / pre["flow_vehicles_per_lane_hour"] - 1.0
        ) * 100.0
        summary.loc[mask, "travel_time_change_vs_pre_pct"] = (
            summary.loc[mask, "travel_time_median_s"] / pre["travel_time_median_s"] - 1.0
        ) * 100.0
        summary.loc[mask, "relative_speed_change_vs_pre_pct"] = (
            pre["travel_time_median_s"] / summary.loc[mask, "travel_time_median_s"] - 1.0
        ) * 100.0
        summary.loc[mask, "visible_vehicles_change_vs_pre_pct"] = (
            summary.loc[mask, "mean_visible_vehicles"] / pre["mean_visible_vehicles"] - 1.0
        ) * 100.0
        summary.loc[mask, "visible_per_lane_change_vs_pre_pct"] = (
            summary.loc[mask, "mean_visible_vehicles_per_lane"]
            / pre["mean_visible_vehicles_per_lane"]
            - 1.0
        ) * 100.0
        immediate = summary[
            (summary["window"].eq("post_immediate")) & (summary["direction"].eq(direction))
        ].iloc[0]
        summary.loc[mask, "flow_change_vs_immediate_pct"] = (
            summary.loc[mask, "flow_vehicles_per_hour"] / immediate["flow_vehicles_per_hour"] - 1.0
        ) * 100.0
        summary.loc[mask, "flow_per_lane_change_vs_immediate_pct"] = (
            summary.loc[mask, "flow_vehicles_per_lane_hour"]
            / immediate["flow_vehicles_per_lane_hour"]
            - 1.0
        ) * 100.0
        summary.loc[mask, "travel_time_change_vs_immediate_pct"] = (
            summary.loc[mask, "travel_time_median_s"] / immediate["travel_time_median_s"] - 1.0
        ) * 100.0
        summary.loc[mask, "relative_speed_change_vs_immediate_pct"] = (
            immediate["travel_time_median_s"] / summary.loc[mask, "travel_time_median_s"] - 1.0
        ) * 100.0
        summary.loc[mask, "visible_vehicles_change_vs_immediate_pct"] = (
            summary.loc[mask, "mean_visible_vehicles"] / immediate["mean_visible_vehicles"] - 1.0
        ) * 100.0
        summary.loc[mask, "visible_per_lane_change_vs_immediate_pct"] = (
            summary.loc[mask, "mean_visible_vehicles_per_lane"]
            / immediate["mean_visible_vehicles_per_lane"]
            - 1.0
        ) * 100.0
    return summary


def make_density_bins(density: pd.DataFrame) -> pd.DataFrame:
    density = density.copy()
    starts = {window: float(config["start_s"]) for window, config in WINDOWS.items()}
    density["seconds_from_window_start"] = density.apply(
        lambda row: float(row["timestamp_s"]) - starts[str(row["window"])],
        axis=1,
    )
    density["bin_10s"] = np.floor(density["seconds_from_window_start"] / 10.0).astype(int)
    return (
        density.groupby(["window", "window_label", "direction", "lanes", "bin_10s"], as_index=False)
        .agg(
            timestamp_s_mean=("timestamp_s", "mean"),
            visible_vehicle_count=("visible_vehicle_count", "mean"),
            vehicles_per_lane_in_roi=("vehicles_per_lane_in_roi", "mean"),
            sampled_frames=("frame_index", "size"),
        )
    )


def make_later_window_5min_qa(speed: pd.DataFrame, density: pd.DataFrame) -> pd.DataFrame:
    panel = pd.read_csv(FLOW_PANEL)
    panel = panel[panel["source_video"].str.endswith("出城方向1621开始.MOV")].copy()
    blocks = [
        ("008_003330_003830_outbound_post", 2010.0, 2310.0, "33:30-38:30"),
        ("009_003830_004330_outbound_post", 2310.0, 2610.0, "38:30-43:30"),
        ("010_004330_004830_outbound_post", 2610.0, 2910.0, "43:30-48:30"),
    ]
    retained = speed[(speed["window"].eq("post_stable")) & (speed["qa_keep"])].copy()
    retained["midpoint_s"] = (retained["first_timestamp_s"] + retained["second_timestamp_s"]) / 2.0
    later_density = density[density["window"].eq("post_stable")].copy()
    rows = []
    for segment_id, start_s, end_s, label in blocks:
        for direction in DIRECTION_ORDER:
            tracks = retained[
                (retained["direction"].eq(direction))
                & (retained["midpoint_s"].ge(start_s))
                & (retained["midpoint_s"].lt(end_s))
            ]
            den = later_density[
                (later_density["direction"].eq(direction))
                & (later_density["timestamp_s"].ge(start_s))
                & (later_density["timestamp_s"].lt(end_s))
            ]
            flow_row = panel[
                (panel["segment_id"].eq(segment_id)) & (panel["direction"].eq(direction))
            ].iloc[0]
            rows.append(
                {
                    "block": label,
                    "start_s": start_s,
                    "end_s": end_s,
                    "direction": direction,
                    "lanes": int(flow_row["lanes"]),
                    "flow_count": int(flow_row["count"]),
                    "flow_vehicles_per_hour": float(flow_row["vehicles_per_hour"]),
                    "flow_vehicles_per_lane_hour": float(flow_row["vehicles_per_lane_hour"]),
                    "speed_matches_qa": len(tracks),
                    "travel_time_median_s": float(tracks["travel_time_s"].median()),
                    "sampled_frames": len(den),
                    "mean_visible_vehicles": float(den["visible_vehicle_count"].mean()),
                    "mean_visible_vehicles_per_lane": float(den["vehicles_per_lane_in_roi"].mean()),
                }
            )
    return pd.DataFrame(rows)


def write_notes(summary: pd.DataFrame) -> None:
    def row(window: str, direction: str) -> pd.Series:
        return summary[(summary["window"].eq(window)) & (summary["direction"].eq(direction))].iloc[0]

    lines = [
        "# 2026-07-09 three-window traffic measurement comparison",
        "",
        "## Sample",
        "",
        "- Pre: 0:00-2:45, with the normal lane allocation.",
        "- Deployment: 2:45-3:30, excluded from every statistic.",
        "- Immediate post: 3:30-8:30, the first five minutes of stable lane operation.",
        "- Later post: 33:30-48:30, beginning 30 minutes after operation started.",
        "- Identical post-treatment gates and road ROIs are imposed in both post windows.",
        "",
        "## Directional results",
        "",
    ]
    for direction in DIRECTION_ORDER:
        pre = row("pre", direction)
        immediate = row("post_immediate", direction)
        stable = row("post_stable", direction)
        lines.extend(
            [
                f"### {DIRECTION_LABEL[direction]}",
                "",
                (
                    f"- Median gate travel time: {pre['travel_time_median_s']:.2f}s pre, "
                    f"{immediate['travel_time_median_s']:.2f}s immediate post, and "
                    f"{stable['travel_time_median_s']:.2f}s later post."
                ),
                (
                    f"- Visible vehicle stock: {pre['mean_visible_vehicles']:.2f} pre, "
                    f"{immediate['mean_visible_vehicles']:.2f} immediate post, and "
                    f"{stable['mean_visible_vehicles']:.2f} later post."
                ),
                (
                    f"- Directional flow: {pre['flow_vehicles_per_hour']:.0f}, "
                    f"{immediate['flow_vehicles_per_hour']:.0f}, and "
                    f"{stable['flow_vehicles_per_hour']:.0f} vehicles per hour."
                ),
                (
                    f"- Flow per lane: {pre['flow_vehicles_per_lane_hour']:.0f}, "
                    f"{immediate['flow_vehicles_per_lane_hour']:.0f}, and "
                    f"{stable['flow_vehicles_per_lane_hour']:.0f} vehicles per lane-hour."
                ),
                "",
            ]
        )
    lines.extend(
        [
            "## Interpretation limits",
            "",
            "- The later-post window estimates an operating-state contrast, not a local RDiT coefficient.",
            "- Five-minute diagnostics indicate stable directional throughput within the later window, but south-to-north travel time and visible stock rise in its final five minutes. The window is later, but it is not strictly stationary in every traffic-state outcome.",
            "- The pre window remains limited to 165 seconds. Its dual-gate speed sample is selective because dense traffic creates occlusion and interrupted tracks.",
            "- Gate travel time is distance-free. Absolute speed remains conditional on the provisional gate-distance calibration.",
            "- Flow, travel time, and visible stock share detector and tracking error; their joint movement is an internal consistency check rather than an independent validation sample.",
        ]
    )
    (OUT / "analysis_notes.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_sample_lineage() -> None:
    lines = [
        "# Sample lineage",
        "",
        "1. Raw video: `flowvedio/20260709彩田路/出城方向1621开始.MOV`.",
        "2. Lane-state timing: 0:00-2:45 pre, 2:45-3:30 deployment, and 3:30 onward post.",
        "3. Immediate-post extraction: 3:30-8:30 from the retained pilot output.",
        "4. Later-post extraction: 33:30-48:30, after a fixed 30-minute washout from the start of operation.",
        "5. Speed sample: tracks crossing both confirmed gates in the expected image direction; outliers are flagged within window and direction by a four-MAD rule.",
        "6. Visible stock: bottom-centre detections inside the confirmed direction-specific ROI in every processed frame.",
        "7. Flow: retained fixed-line counts aggregated over the identical three video windows.",
        "",
        "No deployment frame enters the comparison. Missing time is not recoded as zero traffic.",
    ]
    (OUT / "sample-lineage.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_qa_summary(
    speed: pd.DataFrame,
    density: pd.DataFrame,
    crossings: pd.DataFrame,
    summary: pd.DataFrame,
    later_5min: pd.DataFrame,
) -> None:
    measurement_summaries = {
        window: json.loads(
            (Path(config["directory"]) / "measurement_summary.json").read_text(encoding="utf-8")
        )
        for window, config in WINDOWS.items()
    }
    later_flow_cv = {}
    for direction in DIRECTION_ORDER:
        values = later_5min[later_5min["direction"].eq(direction)]["flow_vehicles_per_hour"]
        later_flow_cv[direction] = float(values.std(ddof=0) / values.mean())
    qa = {
        "post_geometry_identical": (
            measurement_summaries["post_immediate"]["geometry"]
            == measurement_summaries["post_stable"]["geometry"]
        ),
        "processed_frames": {
            window: int(data["processed_frames"]) for window, data in measurement_summaries.items()
        },
        "duplicate_speed_track_direction_rows": int(
            speed.duplicated(["window", "track_id", "direction"]).sum()
        ),
        "duplicate_gate_crossing_rows": int(
            crossings.duplicated(["window", "track_id", "direction", "gate"]).sum()
        ),
        "density_rows_per_window_direction": {
            f"{window}|{direction}": int(len(group))
            for (window, direction), group in density.groupby(["window", "direction"])
        },
        "max_absolute_qkv_gap_percent": float(summary["qkv_gap_percent"].abs().max()),
        "later_window_5min_flow_cv": later_flow_cv,
        "later_window_south_to_north_5min_travel_time_medians_s": later_5min[
            later_5min["direction"].eq("south_to_north")
        ]["travel_time_median_s"].tolist(),
        "later_window_south_to_north_5min_visible_stock_means": later_5min[
            later_5min["direction"].eq("south_to_north")
        ]["mean_visible_vehicles"].tolist(),
    }
    (OUT / "qa_summary.json").write_text(
        json.dumps(qa, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    speed, density, crossings = load_measurements()
    speed = mark_speed_qa(speed)
    flow = load_flow()
    summary = build_summary(speed, density, crossings, flow)
    density_bins = make_density_bins(density)
    later_5min = make_later_window_5min_qa(speed, density)

    speed.to_csv(OUT / "speed_gate_matches_qa.csv", index=False)
    density_bins.to_csv(OUT / "density_10s.csv", index=False)
    flow.to_csv(OUT / "flow_three_windows.csv", index=False)
    summary.to_csv(OUT / "window_direction_summary.csv", index=False)
    compact_columns = [
        "window",
        "window_label",
        "duration_s",
        "lanes",
        "flow_count",
        "flow_vehicles_per_hour",
        "flow_vehicles_per_lane_hour",
        "speed_matches_qa",
        "match_rate_limiting_gate",
        "travel_time_median_s",
        "travel_time_p25_s",
        "travel_time_p75_s",
        "sampled_frames",
        "mean_visible_vehicles",
        "mean_visible_vehicles_per_lane",
        "qkv_gap_percent",
        "flow_change_vs_pre_pct",
        "flow_per_lane_change_vs_pre_pct",
        "travel_time_change_vs_pre_pct",
        "relative_speed_change_vs_pre_pct",
        "visible_vehicles_change_vs_pre_pct",
        "visible_per_lane_change_vs_pre_pct",
        "flow_change_vs_immediate_pct",
        "travel_time_change_vs_immediate_pct",
        "visible_vehicles_change_vs_immediate_pct",
    ]
    summary[summary["direction"].eq("south_to_north")][compact_columns].to_csv(
        OUT / "south_to_north_comparison.csv",
        index=False,
    )
    summary[summary["direction"].eq("north_to_south")][compact_columns].to_csv(
        OUT / "north_to_south_validation.csv",
        index=False,
    )
    later_5min.to_csv(OUT / "later_window_5min_qa.csv", index=False)
    write_notes(summary)
    write_sample_lineage()
    write_qa_summary(speed, density, crossings, summary, later_5min)

    print(summary.round(3).to_string(index=False))
    print(f"output={OUT}")


if __name__ == "__main__":
    main()
