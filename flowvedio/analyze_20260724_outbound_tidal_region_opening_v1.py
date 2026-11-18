#!/usr/bin/env python3
"""Descriptive three-region comparison around the 24 July outbound opening."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from analyze_20260723_24_state_dependent_v5 import CONFIG, SOURCE


OUTPUT = SOURCE / "analysis_basic_v1/outbound_tidal_region_opening_20260724_v1"
SEGMENTS = [
    {
        "state": "pre_south_open",
        "mark_id": "M16",
        "directory": "C12_IMG_0020_M16_southopen_0p000_5141p048",
        "start_s": 0.0,
        "end_s": 5141.048,
    },
    {
        "state": "post_north_open",
        "mark_id": "M17",
        "directory": "C14_IMG_3028_M17_northopen_135p000_7508p673",
        "start_s": 135.0,
        "end_s": 7508.673,
    },
]


def region_bounds(mark: dict[str, object]) -> list[tuple[str, float, float]]:
    return [
        ("fixed_north", float(mark["x1_left"]), float(mark["x1_boundary1"])),
        ("tidal_variable", float(mark["x1_boundary1"]), float(mark["x1_boundary2"])),
        ("fixed_south", float(mark["x1_boundary2"]), float(mark["x1_right"])),
    ]


def summarize_window(
    crossings: pd.DataFrame,
    mark: dict[str, object],
    state: str,
    window: str,
    start_s: float,
    end_s: float,
) -> list[dict[str, object]]:
    minutes = (end_s - start_s) / 60.0
    data = crossings[
        crossings["gate"].eq("near")
        & crossings["timestamp_s"].ge(start_s)
        & crossings["timestamp_s"].lt(end_s)
    ]
    rows: list[dict[str, object]] = []
    for region, left, right in region_bounds(mark):
        region_data = data[data["crossing_x"].ge(left) & data["crossing_x"].lt(right)]
        for direction in ("south_to_north", "north_to_south"):
            count = int(region_data["direction"].eq(direction).sum())
            rows.append(
                {
                    "state": state,
                    "window": window,
                    "mark_id": mark["标记编号"],
                    "region": region,
                    "direction": direction,
                    "window_start_s": start_s,
                    "window_end_s": end_s,
                    "covered_minutes": minutes,
                    "vehicles": count,
                    "vehicles_per_min": count / minutes,
                }
            )
    return rows


def main() -> None:
    OUTPUT.mkdir(parents=True, exist_ok=True)
    config = json.loads(CONFIG.read_text(encoding="utf-8"))
    marks = {row["标记编号"]: row for row in config["marks"]}
    rows: list[dict[str, object]] = []
    for spec in SEGMENTS:
        mark = marks[spec["mark_id"]]
        crossings = pd.read_csv(SOURCE / "measurements" / spec["directory"] / "gate_crossings.csv")
        rows.extend(
            summarize_window(
                crossings, mark, spec["state"], "all_stable",
                float(spec["start_s"]), float(spec["end_s"]),
            )
        )
        if spec["state"].startswith("pre"):
            start_30, end_30 = float(spec["end_s"]) - 1800.0, float(spec["end_s"])
        else:
            start_30, end_30 = float(spec["start_s"]), float(spec["start_s"]) + 1800.0
        rows.extend(summarize_window(crossings, mark, spec["state"], "adjacent_30min", start_30, end_30))

    result = pd.DataFrame(rows)
    result.to_csv(OUTPUT / "tidal_region_opening_summary.csv", index=False)
    active = result[result["vehicles"].gt(0)].copy()
    active.to_csv(OUTPUT / "tidal_region_opening_nonzero.csv", index=False)

    direction_total = (
        result.groupby(["state", "window", "direction"], as_index=False)
        .agg(covered_minutes=("covered_minutes", "first"), vehicles=("vehicles", "sum"))
    )
    direction_total["vehicles_per_min"] = direction_total["vehicles"] / direction_total["covered_minutes"]
    direction_total.to_csv(OUTPUT / "direction_total_summary.csv", index=False)

    comparison = active.pivot_table(
        index=["window", "region", "direction"], columns="state", values="vehicles_per_min", aggfunc="first"
    ).reset_index()
    comparison["post_minus_pre"] = comparison.get("post_north_open") - comparison.get("pre_south_open")
    comparison.to_csv(OUTPUT / "region_pre_post_comparison.csv", index=False)
    print("NONZERO REGION FLOWS\n" + active.to_string(index=False))
    print("\nDIRECTION TOTALS\n" + direction_total.to_string(index=False))


if __name__ == "__main__":
    main()
