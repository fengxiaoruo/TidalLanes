#!/usr/bin/env python3
"""Split the valid 23 July outbound opening event into three physical regions."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from analyze_20260723_24_state_dependent_v5 import CONFIG, SOURCE


OUTPUT = SOURCE / "analysis_basic_v1/outbound_tidal_region_opening_v1"
SEGMENTS = [
    ("pre_south_open", "P08_IMG_0017_M08_southopen_0p000_755p000"),
    ("post_north_open", "P10_IMG_0017_M08_northopen_1680p000_4208p000"),
]


def main() -> None:
    OUTPUT.mkdir(parents=True, exist_ok=True)
    config = json.loads(CONFIG.read_text(encoding="utf-8"))
    mark = next(row for row in config["marks"] if row["标记编号"] == "M08")
    regions = [
        ("fixed_north", float(mark["x1_left"]), float(mark["x1_boundary1"])),
        ("tidal_variable", float(mark["x1_boundary1"]), float(mark["x1_boundary2"])),
        ("fixed_south", float(mark["x1_boundary2"]), float(mark["x1_right"])),
    ]
    rows: list[dict[str, object]] = []
    for state, name in SEGMENTS:
        directory = SOURCE / "measurements" / name
        summary = json.loads((directory / "measurement_summary.json").read_text(encoding="utf-8"))
        minutes = float(summary["duration_s"]) / 60.0
        crossings = pd.read_csv(directory / "gate_crossings.csv")
        crossings = crossings[crossings["gate"].eq("near")]
        for region, left, right in regions:
            sample = crossings[crossings["crossing_x"].ge(left) & crossings["crossing_x"].lt(right)]
            for direction in ("south_to_north", "north_to_south"):
                count = int(sample["direction"].eq(direction).sum())
                rows.append(
                    {
                        "state": state,
                        "region": region,
                        "direction": direction,
                        "covered_minutes": minutes,
                        "vehicles": count,
                        "vehicles_per_min": count / minutes,
                    }
                )
    result = pd.DataFrame(rows)
    result.to_csv(OUTPUT / "tidal_region_opening_summary.csv", index=False)
    print(result[result["vehicles"].gt(0)].to_string(index=False))


if __name__ == "__main__":
    main()
