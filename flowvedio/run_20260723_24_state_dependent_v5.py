#!/usr/bin/env python3
"""Run v5 state-dependent Caitian Road vehicle recognition.

The input JSON is a machine-readable extraction of the user-completed v5
workbook. Stable periods are assigned to the two geographic directions. The
tidal-variable region is merged into the direction in which it is open.
Implementation/transition periods remain excluded from the stable run.
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
DEFAULT_CONFIG = ROOT / "batch_outputs/20260723_24_state_dependent_manual_review_v5/state_dependent_config_v5.json"
DEFAULT_OUTPUT = ROOT / "batch_outputs/20260723_24_state_dependent_recognition_v5"


def seconds(value: str) -> float:
    parts = [float(item) for item in value.split(":")]
    return parts[0] * 3600 + parts[1] * 60 + parts[2]


def clamp(value: object, maximum: int) -> int:
    return max(0, min(int(float(value)), maximum - 1))


def direction_key(label: str) -> str:
    if "向北" in label:
        return "south_to_north"
    if "向南" in label:
        return "north_to_south"
    raise ValueError(f"Unrecognized direction label: {label}")


def geometry_for(mark: dict[str, object], tidal_state: str) -> tuple[dict[str, object], dict[str, int]]:
    width, height = int(mark["图片宽度"]), int(mark["图片高度"])
    y1 = clamp(mark["y1"], height)
    y2 = clamp(mark["y2"], height)
    if mark["kind"] == "ordinary":
        regions = [
            ("left", "split", direction_key(str(mark["左段车流方向"]))),
            ("split", "right", direction_key(str(mark["右段车流方向"]))),
        ]
    else:
        middle = direction_key(tidal_state)
        regions = [
            ("left", "boundary1", direction_key(str(mark["左侧物理区域"]))),
            ("boundary1", "boundary2", middle),
            ("boundary2", "right", direction_key(str(mark["右侧物理区域"]))),
        ]

    spans: dict[str, list[tuple[int, int, int, int]]] = {"south_to_north": [], "north_to_south": []}
    for left, right, direction in regions:
        spans[direction].append(
            (
                clamp(mark[f"x1_{left}"], width),
                clamp(mark[f"x1_{right}"], width),
                clamp(mark[f"x2_{left}"], width),
                clamp(mark[f"x2_{right}"], width),
            )
        )

    speed_gates: dict[str, object] = {}
    rois: dict[str, object] = {}
    direction_centers: dict[str, float] = {}
    for direction, pieces in spans.items():
        x1_left = min(piece[0] for piece in pieces)
        x1_right = max(piece[1] for piece in pieces)
        x2_left = min(piece[2] for piece in pieces)
        x2_right = max(piece[3] for piece in pieces)
        speed_gates[direction] = {
            "near": [x1_left, y1, x1_right, y1],
            "far": [x2_left, y2, x2_right, y2],
        }
        rois[direction] = [[x1_left, y1], [x1_right, y1], [x2_right, y2], [x2_left, y2]]
        direction_centers[direction] = (x1_left + x1_right + x2_left + x2_right) / 4.0

    road_center = sum(direction_centers.values()) / len(direction_centers)
    dy_signs = {
        direction: 1 if center < road_center else -1
        for direction, center in direction_centers.items()
    }

    north_open = "向北" in tidal_state
    lanes = {
        "south_to_north": 4 if north_open else 3,
        "north_to_south": 3 if north_open else 4,
    }
    return {"speed_gates": speed_gates, "rois": rois, "dy_signs": dy_signs}, lanes


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--model", type=Path, default=ROOT / "models/yolo11s.pt")
    parser.add_argument("--device", default="mps")
    parser.add_argument("--imgsz", type=int, default=960)
    parser.add_argument("--conf", type=float, default=0.16)
    parser.add_argument("--stride", type=int, default=4)
    parser.add_argument("--smoke-seconds", type=float, default=0.0)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--max-items", type=int, default=0)
    args = parser.parse_args()

    config = json.loads(args.config.resolve().read_text(encoding="utf-8"))
    marks = {row["标记编号"]: row for row in config["marks"]}
    output = args.output.resolve()
    geometry_dir = output / "geometry"
    measurement_dir = output / ("smoke_measurements" if args.smoke_seconds else "measurements")
    log_dir = output / "logs"
    for folder in (geometry_dir, measurement_dir, log_dir):
        folder.mkdir(parents=True, exist_ok=True)

    run_rows: list[dict[str, object]] = []
    allowed = {"纳入稳定窗口", "纳入但窗口很短"}
    for state in config["states"]:
        if args.max_items and len(run_rows) >= args.max_items:
            break
        if state["计数处理"] not in allowed or not state["坐标编号"]:
            continue
        mark = marks[state["坐标编号"]]
        tidal_state = str(state["潮汐可变区状态"])
        if "向北" not in tidal_state and "向南" not in tidal_state:
            continue
        geometry, lanes = geometry_for(mark, tidal_state)
        geometry_id = f'{state["坐标编号"]}_{"northopen" if "向北" in tidal_state else "southopen"}'
        geometry_path = geometry_dir / f"{geometry_id}.json"
        geometry_path.write_text(json.dumps(geometry, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        start_s = seconds(str(state["片内开始"]))
        planned_end_s = seconds(str(state["片内结束"]))
        end_s = min(planned_end_s, start_s + args.smoke_seconds) if args.smoke_seconds else planned_end_s
        source = VIDEO_DIR / str(state["视频文件"])
        item_id = f'{state["阶段编号"]}_{source.stem}_{geometry_id}_{start_s:.3f}_{end_s:.3f}'.replace(".", "p")
        item_dir = measurement_dir / item_id
        summary_path = item_dir / "measurement_summary.json"
        command = [
            sys.executable, str(ROOT / "measure_speed_density_yolo.py"), str(source),
            "--outdir", str(item_dir), "--geometry-json", str(geometry_path),
            "--geometry-id", geometry_id, "--lanes-json", json.dumps(lanes, separators=(",", ":")),
            "--start-s", str(start_s), "--end-s", str(end_s), "--model", str(args.model),
            "--device", args.device, "--imgsz", str(args.imgsz), "--conf", str(args.conf),
            "--stride", str(args.stride), "--no-speed-proxy",
        ]
        status = "skipped_existing"
        if args.force or not summary_path.exists():
            print(f"run {item_id}", flush=True)
            subprocess.run(command, cwd=ROOT, check=True)
            status = "done"
        run_rows.append({
            "stage_id": state["阶段编号"], "source_file": source.name, "state": state["阶段状态"],
            "tidal_state": tidal_state, "geometry_id": geometry_id, "start_s": start_s,
            "end_s": end_s, "planned_end_s": planned_end_s, "status": status,
            "measurement_dir": str(item_dir),
        })

    plan_path = output / ("smoke_run_plan.csv" if args.smoke_seconds else "run_plan.csv")
    with plan_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(run_rows[0]))
        writer.writeheader()
        writer.writerows(run_rows)
    print(f"output={output}", flush=True)


if __name__ == "__main__":
    main()
