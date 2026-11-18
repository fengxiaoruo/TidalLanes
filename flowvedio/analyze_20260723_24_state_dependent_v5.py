#!/usr/bin/env python3
"""Create basic count statistics for the completed v5 stable-period recognition."""

from __future__ import annotations

import csv
import json
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parent
SOURCE = ROOT / "batch_outputs/20260723_24_state_dependent_recognition_v5"
CONFIG = ROOT / "batch_outputs/20260723_24_state_dependent_manual_review_v5/state_dependent_config_v5.json"
OUTPUT = SOURCE / "analysis_basic_v1"

TRANSITIONS = [
    ("2026-07-23", "进城", "opening", "2026-07-23 16:07:17", "2026-07-23 16:26:17"),
    ("2026-07-23", "出城", "opening", "2026-07-23 16:11:09", "2026-07-23 16:26:34"),
    ("2026-07-23", "进城", "recovery", "2026-07-23 21:31:48.375", "2026-07-23 21:48:54.375"),
    ("2026-07-23", "出城", "recovery", "2026-07-23 21:31:35", "2026-07-23 21:46:35"),
    ("2026-07-24", "进城", "opening", "2026-07-24 16:16:47", "2026-07-24 16:30:07"),
    ("2026-07-24", "出城", "opening", "2026-07-24 16:07:43.048", "2026-07-24 16:29:01"),
    ("2026-07-24", "进城", "recovery", "2026-07-24 21:34:33", "2026-07-24 21:43:43"),
    ("2026-07-24", "出城", "recovery", "2026-07-24 21:34:16", "2026-07-24 21:42:41"),
]


def load_inputs() -> tuple[pd.DataFrame, dict[str, dict[str, object]]]:
    plan = pd.read_csv(SOURCE / "run_plan.csv")
    config = json.loads(CONFIG.read_text(encoding="utf-8"))
    states = {str(row["阶段编号"]): row for row in config["states"]}
    return plan, states


def absolute_start(state: dict[str, object]) -> pd.Timestamp:
    return pd.Timestamp(f'{state["日期"]} {state["实际开始"]}')


def build_event_and_segment_tables() -> tuple[pd.DataFrame, pd.DataFrame]:
    plan, states = load_inputs()
    event_parts: list[pd.DataFrame] = []
    segment_rows: list[dict[str, object]] = []
    for row in plan.itertuples(index=False):
        state = states[str(row.stage_id)]
        directory = Path(row.measurement_dir)
        summary = json.loads((directory / "measurement_summary.json").read_text(encoding="utf-8"))
        crossings = pd.read_csv(directory / "gate_crossings.csv")
        dy_signs = summary["geometry"]["dy_signs"]
        crossings["entry_gate"] = crossings["direction"].map(
            {direction: "far" if int(sign) > 0 else "near" for direction, sign in dy_signs.items()}
        )
        entry = crossings[crossings["gate"].eq(crossings["entry_gate"])].copy()
        start = absolute_start(state)
        entry["recorded_at"] = start + pd.to_timedelta(entry["timestamp_s"] - float(row.start_s), unit="s")
        entry["stage_id"] = row.stage_id
        entry["date"] = str(state["日期"])
        entry["collection_direction"] = str(state["采集方向"])
        entry["state"] = str(state["阶段状态"])
        entry["tidal_state"] = str(state["潮汐可变区状态"])
        entry["source_file"] = row.source_file
        event_parts.append(entry)

        duration_min = (float(row.end_s) - float(row.start_s)) / 60.0
        matches = pd.read_csv(directory / "speed_gate_matches.csv")
        for direction in ("south_to_north", "north_to_south"):
            entry_count = int(entry["direction"].eq(direction).sum())
            matched = int(matches["direction"].eq(direction).sum())
            segment_rows.append(
                {
                    "stage_id": row.stage_id,
                    "date": str(state["日期"]),
                    "collection_direction": str(state["采集方向"]),
                    "source_file": row.source_file,
                    "state": str(state["阶段状态"]),
                    "tidal_state": str(state["潮汐可变区状态"]),
                    "direction": direction,
                    "start_local": str(state["实际开始"]),
                    "end_local": str(state["实际结束"]),
                    "duration_min": duration_min,
                    "entry_crossings": entry_count,
                    "flow_veh_per_min": entry_count / duration_min,
                    "two_gate_matched": matched,
                    "two_gate_match_share": matched / entry_count if entry_count else float("nan"),
                }
            )
    return pd.concat(event_parts, ignore_index=True), pd.DataFrame(segment_rows)


def weighted_summary(segments: pd.DataFrame, groups: list[str]) -> pd.DataFrame:
    result = (
        segments.groupby(groups, dropna=False)
        .agg(
            segments=("stage_id", "nunique"),
            covered_minutes=("duration_min", "sum"),
            entry_crossings=("entry_crossings", "sum"),
            two_gate_matched=("two_gate_matched", "sum"),
        )
        .reset_index()
    )
    result["flow_veh_per_min"] = result["entry_crossings"] / result["covered_minutes"]
    result["two_gate_match_share"] = result["two_gate_matched"] / result["entry_crossings"]
    return result


def overlap_minutes(segments: pd.DataFrame, date: str, camera: str, start: pd.Timestamp, end: pd.Timestamp) -> float:
    unique = segments[(segments["date"].eq(date)) & (segments["collection_direction"].eq(camera))].drop_duplicates("stage_id")
    total = 0.0
    for row in unique.itertuples(index=False):
        seg_start = pd.Timestamp(f"{date} {row.start_local}")
        seg_end = pd.Timestamp(f"{date} {row.end_local}")
        total += max(0.0, (min(end, seg_end) - max(start, seg_start)).total_seconds())
    return total / 60.0


def transition_windows(events: pd.DataFrame, segments: pd.DataFrame, minutes: int = 30) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    delta = pd.Timedelta(minutes=minutes)
    for date, camera, event, pre_end_text, post_start_text in TRANSITIONS:
        pre_end, post_start = pd.Timestamp(pre_end_text), pd.Timestamp(post_start_text)
        windows = [("pre", pre_end - delta, pre_end), ("post", post_start, post_start + delta)]
        for period, start, end in windows:
            tidal_state = "向南开放" if (event == "opening") == (period == "pre") else "向北开放"
            covered = overlap_minutes(segments, date, camera, start, end)
            sample = events[
                events["date"].eq(date)
                & events["collection_direction"].eq(camera)
                & events["recorded_at"].ge(start)
                & events["recorded_at"].lt(end)
            ]
            for direction in ("south_to_north", "north_to_south"):
                count = int(sample["direction"].eq(direction).sum())
                rows.append(
                    {
                        "date": date,
                        "collection_direction": camera,
                        "event": event,
                        "period": period,
                        "tidal_state": tidal_state,
                        "direction": direction,
                        "window_start": start,
                        "window_end": end,
                        "covered_minutes": covered,
                        "entry_crossings": count,
                        "flow_veh_per_min": count / covered if covered else float("nan"),
                    }
                )
    result = pd.DataFrame(rows)
    return result


def main() -> None:
    OUTPUT.mkdir(parents=True, exist_ok=True)
    events, segments = build_event_and_segment_tables()
    segments.to_csv(OUTPUT / "segment_direction_summary.csv", index=False)
    state = weighted_summary(
        segments,
        ["date", "collection_direction", "tidal_state", "direction"],
    )
    state.to_csv(OUTPUT / "state_direction_summary.csv", index=False)
    overall = weighted_summary(segments, ["tidal_state", "direction"])
    overall.to_csv(OUTPUT / "overall_state_direction_summary.csv", index=False)

    events["bin_start"] = events["recorded_at"].dt.floor("5min")
    panel = (
        events.groupby(
            ["date", "collection_direction", "tidal_state", "direction", "bin_start"],
            dropna=False,
        )
        .size()
        .rename("entry_crossings")
        .reset_index()
    )
    panel["flow_veh_per_min_observed"] = panel["entry_crossings"] / 5.0
    panel.to_csv(OUTPUT / "five_minute_event_panel_observed_bins.csv", index=False)

    pivot = overall.pivot(index="direction", columns="tidal_state", values="flow_veh_per_min").reset_index()
    if {"向北开放", "向南开放"}.issubset(pivot.columns):
        pivot["north_open_minus_south_open"] = pivot["向北开放"] - pivot["向南开放"]
        pivot["north_open_pct_change"] = pivot["north_open_minus_south_open"] / pivot["向南开放"] * 100
    pivot.to_csv(OUTPUT / "raw_state_comparison.csv", index=False)

    windows = transition_windows(events, segments)
    windows.to_csv(OUTPUT / "transition_30min_windows.csv", index=False)
    comparison = windows.pivot(
        index=["date", "collection_direction", "event", "direction"],
        columns="tidal_state",
        values="flow_veh_per_min",
    ).reset_index()
    comparison["north_open_minus_south_open"] = comparison["向北开放"] - comparison["向南开放"]
    comparison["north_open_pct_change"] = comparison["north_open_minus_south_open"] / comparison["向南开放"] * 100
    comparison.to_csv(OUTPUT / "transition_30min_comparison.csv", index=False)
    transition_summary = (
        comparison.groupby("direction")["north_open_pct_change"]
        .agg(events="size", mean_pct_change="mean", median_pct_change="median", min_pct_change="min", max_pct_change="max")
        .reset_index()
    )
    transition_summary.to_csv(OUTPUT / "transition_30min_summary.csv", index=False)

    qa = {
        "stable_segments": int(segments["stage_id"].nunique()),
        "covered_hours": float(segments.drop_duplicates("stage_id")["duration_min"].sum() / 60),
        "entry_crossings": int(len(events)),
        "two_gate_matched": int(segments["two_gate_matched"].sum()),
        "speed_calculated": False,
        "note": "Collection directions are separate camera streams and are not added together as a road total.",
    }
    (OUTPUT / "qa_summary.json").write_text(json.dumps(qa, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(qa, ensure_ascii=False, indent=2))
    print("\nOVERALL\n" + overall.to_string(index=False))
    print("\nRAW COMPARISON\n" + pivot.to_string(index=False))
    print("\nTRANSITION 30-MIN SUMMARY\n" + transition_summary.to_string(index=False))


if __name__ == "__main__":
    main()
