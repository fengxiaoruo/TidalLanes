#!/usr/bin/env python3
"""Recognize all recorded outbound operation-period video for July 23 and 24.

The output is additive. Stable-period recognition and all prior results remain
unchanged. Full-road gates are used because lane boundaries move during the
operation. Direction is determined by crossing direction, not lane position.
"""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
VIDEO_DIR = ROOT / "2026072324彩田路"
CONFIG = ROOT / "batch_outputs/20260723_24_state_dependent_manual_review_v5/state_dependent_config_v5.json"
OUT = ROOT / "batch_outputs/20260723_24_state_dependent_recognition_v5/operation_period_recognition_v1"
EXISTING_23_OPENING = (
    ROOT
    / "batch_outputs/20260723_caitian_empirical_v1/transition_measurements"
    / "IMG_0017_outbound_0017_transition_fullroad_755_1680"
)

TARGET_STAGES = ["P09", "C06", "C13", "C21"]


def seconds(value: str) -> float:
    h, m, s = [float(item) for item in value.split(":")]
    return h * 3600 + m * 60 + s


def clamp(value: object, maximum: int) -> int:
    return max(0, min(int(float(value)), maximum - 1))


def fullroad_geometry(mark: dict[str, object]) -> dict[str, object]:
    width, height = int(mark["图片宽度"]), int(mark["图片高度"])
    near = [
        clamp(mark["x1_left"], width),
        clamp(mark["y1"], height),
        clamp(mark["x1_right"], width),
        clamp(mark["y1"], height),
    ]
    far = [
        clamp(mark["x2_left"], width),
        clamp(mark["y2"], height),
        clamp(mark["x2_right"], width),
        clamp(mark["y2"], height),
    ]
    polygon = [[far[0], far[1]], [far[2], far[3]], [near[2], near[3]], [near[0], near[1]]]
    return {
        "speed_gates": {
            "south_to_north": {"near": near, "far": far},
            "north_to_south": {"near": near, "far": far},
        },
        "rois": {"south_to_north": polygon, "north_to_south": polygon},
        "dy_signs": {"south_to_north": 1, "north_to_south": -1},
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--device", default="mps")
    parser.add_argument("--model", type=Path, default=ROOT / "models/yolo11s.pt")
    args = parser.parse_args()

    config = json.loads(CONFIG.read_text(encoding="utf-8"))
    states = {str(row["阶段编号"]): row for row in config["states"]}
    marks = {str(row["标记编号"]): row for row in config["marks"]}
    geometry_dir = OUT / "geometry"
    measurement_dir = OUT / "measurements"
    geometry_dir.mkdir(parents=True, exist_ok=True)
    measurement_dir.mkdir(parents=True, exist_ok=True)

    rows = []
    for stage_id in TARGET_STAGES:
        state = states[stage_id]
        start_s, end_s = seconds(state["片内开始"]), seconds(state["片内结束"])
        if stage_id == "P09":
            item_dir = EXISTING_23_OPENING
            status = "reused_existing"
            geometry_path = ROOT / "batch_outputs/20260723_caitian_empirical_v1/geometry/outbound_0017_transition_fullroad.json"
        else:
            mark = marks[str(state["坐标编号"])]
            geometry = fullroad_geometry(mark)
            geometry_path = geometry_dir / f'{state["坐标编号"]}_fullroad.json'
            geometry_path.write_text(json.dumps(geometry, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            item_dir = measurement_dir / f'{stage_id}_{Path(state["视频文件"]).stem}_{state["坐标编号"]}_fullroad'
            summary = item_dir / "measurement_summary.json"
            command = [
                sys.executable,
                str(ROOT / "measure_speed_density_yolo.py"),
                str(VIDEO_DIR / state["视频文件"]),
                "--outdir", str(item_dir),
                "--geometry-json", str(geometry_path),
                "--geometry-id", f'{state["坐标编号"]}_operation_fullroad',
                "--lanes-json", '{"south_to_north":1,"north_to_south":1}',
                "--start-s", str(start_s),
                "--end-s", str(end_s),
                "--model", str(args.model),
                "--device", args.device,
                "--imgsz", "960",
                "--conf", "0.16",
                "--stride", "4",
                "--no-speed-proxy",
            ]
            status = "skipped_existing"
            if args.force or not summary.exists():
                print(f"run {stage_id}: {state['实际开始']}--{state['实际结束']}", flush=True)
                subprocess.run(command, cwd=ROOT, check=True)
                status = "done"
        rows.append({
            "stage_id": stage_id,
            "date": state["日期"],
            "event": "opening" if stage_id in {"P09", "C13"} else "recovery",
            "source_file": state["视频文件"],
            "start_s": start_s,
            "end_s": end_s,
            "start_local": state["实际开始"],
            "end_local": state["实际结束"],
            "duration_min": (end_s - start_s) / 60,
            "geometry_path": str(geometry_path),
            "measurement_dir": str(item_dir),
            "status": status,
        })

    with (OUT / "run_plan.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(OUT / "run_plan.csv", flush=True)


if __name__ == "__main__":
    main()
