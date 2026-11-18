#!/usr/bin/env python3
"""Measure directional flow and two-gate travel time during lane deployment."""

from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
EMPIRICAL = ROOT / "batch_outputs" / "20260723_caitian_empirical_v1"
OUT = EMPIRICAL / "transition_measurements"

SEGMENTS = [
    {
        "source_file": "IMG_3024.MOV",
        "camera_view": "inbound",
        "geometry_id": "inbound_3024_transition_fullroad",
        "segment_start_s": 480.0,
        "segment_end_s": 1620.0,
        "segment_start_local": "2026-07-23 16:07:17",
    },
    {
        "source_file": "IMG_0017.MOV",
        "camera_view": "outbound",
        "geometry_id": "outbound_0017_transition_fullroad",
        "segment_start_s": 755.0,
        "segment_end_s": 1680.0,
        "segment_start_local": "2026-07-23 16:11:09",
    },
]


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, object]] = []
    for segment in SEGMENTS:
        start = float(segment["segment_start_s"])
        end = float(segment["segment_end_s"])
        geometry_id = str(segment["geometry_id"])
        source_file = str(segment["source_file"])
        item = OUT / f"{Path(source_file).stem}_{geometry_id}_{start:.0f}_{end:.0f}"
        command = [
            sys.executable,
            str(ROOT / "measure_speed_density_yolo.py"),
            str(ROOT / "20260723彩田路" / source_file),
            "--outdir",
            str(item),
            "--geometry-json",
            str(EMPIRICAL / "geometry" / f"{geometry_id}.json"),
            "--geometry-id",
            geometry_id,
            "--lanes-json",
            json.dumps({"south_to_north": 1, "north_to_south": 1}),
            "--start-s",
            str(start),
            "--end-s",
            str(end),
            "--model",
            str(ROOT / "models" / "yolo11s.pt"),
            "--device",
            "mps",
            "--imgsz",
            "960",
            "--conf",
            "0.16",
            "--stride",
            "4",
        ]
        summary = item / "measurement_summary.json"
        if not summary.exists():
            subprocess.run(command, cwd=ROOT, check=True)
        rows.append(
            {
                **segment,
                "state": "deployment_in_progress",
                "measurement_dir": str(item),
                "status": "done",
                "command": " ".join(command),
            }
        )
    with (EMPIRICAL / "transition_run_plan.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    main()
