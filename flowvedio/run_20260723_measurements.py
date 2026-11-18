#!/usr/bin/env python3
"""Run confirmed two-gate traffic measurements for 2026-07-23 Caitian Road."""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
INPUT_DIR = ROOT / "20260723彩田路"
REVIEW_DIR = ROOT / "batch_outputs" / "20260723_caitian_point_review"
DEFAULT_OUTPUT = ROOT / "batch_outputs" / "20260723_caitian_empirical_v1"
STATE_PLAN = REVIEW_DIR / "lane_state_plan_20260723_v1.csv"
POINT_PLAN = REVIEW_DIR / "point_confirmation_20260723_v1.csv"


def parse_line(value: str) -> tuple[tuple[int, int], tuple[int, int]]:
    left, right = value.split("-")
    parse = lambda text: tuple(int(item) for item in text.strip("()").split(","))
    return parse(left), parse(right)  # type: ignore[return-value]


def load_geometry() -> dict[str, dict[str, object]]:
    grouped: dict[str, dict[str, object]] = {}
    with POINT_PLAN.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            geometry_id = row["geometry_id"]
            direction = row["direction"]
            gate_1 = parse_line(row["speed_gate_1"])
            gate_2 = parse_line(row["speed_gate_2"])
            grouped.setdefault(geometry_id, {"speed_gates": {}, "rois": {}})
            speed_gates = grouped[geometry_id]["speed_gates"]
            rois = grouped[geometry_id]["rois"]
            assert isinstance(speed_gates, dict) and isinstance(rois, dict)
            speed_gates[direction] = {
                "far": [*gate_1[0], *gate_1[1]],
                "near": [*gate_2[0], *gate_2[1]],
            }
            rois[direction] = [list(gate_1[0]), list(gate_1[1]), list(gate_2[1]), list(gate_2[0])]
    return grouped


def load_stable_segments() -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    with STATE_PLAN.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["counting_status"] == "count":
                rows.append(row)
    return rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--model", type=Path, default=ROOT / "models" / "yolo11s.pt")
    parser.add_argument("--device", default="mps")
    parser.add_argument("--imgsz", type=int, default=960)
    parser.add_argument("--conf", type=float, default=0.16)
    parser.add_argument("--stride", type=int, default=4)
    parser.add_argument("--smoke", action="store_true", help="Run at most 60 seconds per stable segment.")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    output = args.output.expanduser().resolve()
    geometry_dir = output / "geometry"
    measurement_dir = output / ("smoke_measurements" if args.smoke else "measurements")
    geometry_dir.mkdir(parents=True, exist_ok=True)
    measurement_dir.mkdir(parents=True, exist_ok=True)

    geometries = load_geometry()
    for geometry_id, payload in geometries.items():
        (geometry_dir / f"{geometry_id}.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

    run_rows: list[dict[str, object]] = []
    for row in load_stable_segments():
        source = INPUT_DIR / row["source_file"]
        geometry_id = row["geometry_id"]
        start_s = float(row["segment_start_s"])
        planned_end_s = float(row["segment_end_s"])
        end_s = min(planned_end_s, start_s + 60.0) if args.smoke else planned_end_s
        item_id = f"{source.stem}_{geometry_id}_{start_s:.3f}_{end_s:.3f}".replace(".", "p")
        item_dir = measurement_dir / item_id
        summary_path = item_dir / "measurement_summary.json"
        lanes = {
            "south_to_north": int(row["south_to_north_lanes"]),
            "north_to_south": int(row["north_to_south_lanes"]),
        }
        command = [
            sys.executable,
            str(ROOT / "measure_speed_density_yolo.py"),
            str(source),
            "--outdir",
            str(item_dir),
            "--geometry-json",
            str(geometry_dir / f"{geometry_id}.json"),
            "--geometry-id",
            geometry_id,
            "--lanes-json",
            json.dumps(lanes, separators=(",", ":")),
            "--start-s",
            str(start_s),
            "--end-s",
            str(end_s),
            "--model",
            str(args.model),
            "--device",
            args.device,
            "--imgsz",
            str(args.imgsz),
            "--conf",
            str(args.conf),
            "--stride",
            str(args.stride),
        ]
        status = "skipped_existing"
        if args.force or not summary_path.exists():
            print(f"run {item_id}", flush=True)
            subprocess.run(command, cwd=ROOT, check=True)
            status = "done"
        run_rows.append(
            {
                "source_file": source.name,
                "camera_view": row["camera_view"],
                "state": row["state"],
                "geometry_id": geometry_id,
                "segment_start_s": start_s,
                "segment_end_s": end_s,
                "planned_segment_end_s": planned_end_s,
                "segment_start_local": row["segment_start_local"],
                "lanes_json": json.dumps(lanes, sort_keys=True),
                "status": status,
                "measurement_dir": str(item_dir),
                "command": " ".join(command),
            }
        )

    fields = list(run_rows[0])
    with (output / ("smoke_run_plan.csv" if args.smoke else "run_plan.csv")).open(
        "w", encoding="utf-8", newline=""
    ) as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(run_rows)
    print(f"output={output}")


if __name__ == "__main__":
    main()
