#!/usr/bin/env python3
"""Create full-resolution line-review frames for the 2026-07-09 outbound video."""

from __future__ import annotations

import argparse
from pathlib import Path

import cv2
import numpy as np


ROOT = Path(__file__).resolve().parent
DEFAULT_VIDEO = ROOT / "20260709彩田路" / "出城方向1621开始.MOV"
DEFAULT_OUT = ROOT / "batch_outputs" / "20260709_speed_density_pilot" / "line_review"


# User-confirmed transverse gates.  Each tuple is (x1, y1, x2, y2).
# The pre/post geometries differ because the tidal-lane boundary moves.
GEOMETRY = {
    "pre": {
        "speed_gates": {
            "south_to_north": {
                "near": (300, 800, 950, 800),
                "far": (700, 500, 980, 500),
            },
            "north_to_south": {
                "far": (1030, 500, 1300, 500),
                "near": (1100, 800, 1910, 800),
            },
        },
        "flow_lines": {
            "south_to_north": (500, 600, 1000, 600),
            "north_to_south": (1050, 600, 1500, 600),
        },
        "rois": {
            "south_to_north": np.array([(700, 500), (980, 500), (950, 800), (300, 800)], dtype=np.int32),
            "north_to_south": np.array([(1030, 500), (1300, 500), (1910, 800), (1100, 800)], dtype=np.int32),
        },
    },
    "post": {
        "speed_gates": {
            "south_to_north": {
                "near": (300, 800, 1250, 800),
                "far": (700, 500, 1100, 500),
            },
            "north_to_south": {
                "far": (1100, 500, 1300, 500),
                "near": (1250, 800, 1900, 800),
            },
        },
        "flow_lines": {
            "south_to_north": (500, 600, 1150, 600),
            "north_to_south": (1150, 600, 1500, 600),
        },
        "rois": {
            "south_to_north": np.array([(700, 500), (1100, 500), (1250, 800), (300, 800)], dtype=np.int32),
            "north_to_south": np.array([(1100, 500), (1300, 500), (1900, 800), (1250, 800)], dtype=np.int32),
        },
    },
}

COLORS = {
    "south_to_north": (0, 215, 255),  # yellow in BGR
    "north_to_south": (255, 155, 40),  # blue in BGR
}


def read_frame(video: Path, timestamp_s: float) -> np.ndarray:
    cap = cv2.VideoCapture(str(video))
    if not cap.isOpened():
        raise RuntimeError(f"Could not open video: {video}")
    fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
    cap.set(cv2.CAP_PROP_POS_FRAMES, int(round(timestamp_s * fps)))
    ok, frame = cap.read()
    cap.release()
    if not ok:
        raise RuntimeError(f"Could not read frame at {timestamp_s:.1f}s: {video}")
    return frame


def draw_grid(frame: np.ndarray, step: int = 100) -> None:
    height, width = frame.shape[:2]
    for x in range(0, width, step):
        cv2.line(frame, (x, 0), (x, height), (205, 205, 205), 1)
        cv2.putText(frame, str(x), (x + 4, 24), cv2.FONT_HERSHEY_SIMPLEX, 0.62, (0, 255, 255), 2, cv2.LINE_AA)
    for y in range(0, height, step):
        cv2.line(frame, (0, y), (width, y), (205, 205, 205), 1)
        label_y = min(height - 8, y + 22)
        cv2.putText(frame, str(y), (7, label_y), cv2.FONT_HERSHEY_SIMPLEX, 0.62, (0, 255, 255), 2, cv2.LINE_AA)


def draw_text_box(frame: np.ndarray, text: str, xy: tuple[int, int], color: tuple[int, int, int]) -> None:
    font = cv2.FONT_HERSHEY_SIMPLEX
    scale = 0.62
    thickness = 2
    x, y = xy
    (width, height), baseline = cv2.getTextSize(text, font, scale, thickness)
    cv2.rectangle(frame, (x - 3, y - height - 7), (x + width + 5, y + baseline + 4), (0, 0, 0), -1)
    cv2.putText(frame, text, (x, y), font, scale, color, thickness, cv2.LINE_AA)


def phase_for_timestamp(timestamp_s: float) -> str:
    if timestamp_s < 165.0:
        return "pre"
    if timestamp_s >= 210.0:
        return "post"
    raise ValueError("The 02:45-03:30 deployment window has no stable speed/density geometry.")


def draw_common_title(frame: np.ndarray, timestamp_s: float, phase: str, suffix: str) -> None:
    minute = int(timestamp_s // 60)
    second = int(round(timestamp_s - minute * 60))
    draw_text_box(frame, f"0709 outbound 1621 | {minute:02d}:{second:02d} | {phase} | {suffix}", (24, 1060), (255, 255, 255))


def draw_speed_lines(frame: np.ndarray, timestamp_s: float, phase: str) -> None:
    draw_grid(frame)
    geometry = GEOMETRY[phase]
    for direction, gates in geometry["speed_gates"].items():
        color = COLORS[direction]
        for gate_name, (x1, y1, x2, y2) in gates.items():
            cv2.line(frame, (x1, y1), (x2, y2), color, 6, cv2.LINE_AA)
            cv2.circle(frame, (x1, y1), 8, color, -1, cv2.LINE_AA)
            cv2.circle(frame, (x2, y2), 8, color, -1, cv2.LINE_AA)
            label = f"{direction} {gate_name}: ({x1},{y1})-({x2},{y2})"
            draw_text_box(frame, label, (x1 + 10, max(45, y1 - 12)), color)

    for direction, (x1, y1, x2, y2) in geometry["flow_lines"].items():
        color = COLORS[direction]
        for x_start in range(x1, x2, 32):
            x_end = min(x_start + 16, x2)
            cv2.line(frame, (x_start, y1), (x_end, y2), (245, 245, 245), 3, cv2.LINE_AA)
        draw_text_box(frame, f"existing flow line: {direction}", (x1 + 10, y1 + 32), color)

    draw_text_box(frame, "YELLOW = south_to_north | BLUE = north_to_south | WHITE DASH = existing flow line", (24, 58), (255, 255, 255))
    draw_common_title(frame, timestamp_s, phase, "confirmed speed gates")


def draw_density_rois(frame: np.ndarray, timestamp_s: float, phase: str) -> None:
    draw_grid(frame)
    geometry = GEOMETRY[phase]
    overlay = frame.copy()
    for direction, polygon in geometry["rois"].items():
        cv2.fillPoly(overlay, [polygon], COLORS[direction])
    cv2.addWeighted(overlay, 0.22, frame, 0.78, 0, frame)

    for direction, polygon in geometry["rois"].items():
        color = COLORS[direction]
        cv2.polylines(frame, [polygon], True, color, 6, cv2.LINE_AA)
        coords = " ".join(f"({x},{y})" for x, y in polygon.tolist())
        anchor_x, anchor_y = polygon[0]
        draw_text_box(frame, f"{direction} ROI: {coords}", (int(anchor_x) + 8, int(anchor_y) - 12), color)

    draw_text_box(frame, "YELLOW ROI = south_to_north | BLUE ROI = north_to_south", (24, 58), (255, 255, 255))
    draw_common_title(frame, timestamp_s, phase, "confirmed density ROIs")


def write_calibration_crop(frame: np.ndarray, output_path: Path, timestamp_s: float) -> None:
    """Write a 2x crop of the clearer north-to-south lane markings."""
    x_min, y_min, x_max, y_max = 950, 350, 1920, 1000
    crop = frame[y_min:y_max, x_min:x_max].copy()
    for x in range(1000, 1921, 25):
        local_x = x - x_min
        cv2.line(crop, (local_x, 0), (local_x, crop.shape[0]), (190, 190, 190), 1)
        if x % 100 == 0:
            cv2.putText(crop, str(x), (local_x + 3, 22), cv2.FONT_HERSHEY_SIMPLEX, 0.48, (0, 255, 255), 1, cv2.LINE_AA)
    for y in range(350, 1001, 25):
        local_y = y - y_min
        cv2.line(crop, (0, local_y), (crop.shape[1], local_y), (190, 190, 190), 1)
        if y % 100 == 0:
            cv2.putText(crop, str(y), (5, local_y + 18), cv2.FONT_HERSHEY_SIMPLEX, 0.48, (0, 255, 255), 1, cv2.LINE_AA)
    enlarged = cv2.resize(crop, None, fx=2.0, fy=2.0, interpolation=cv2.INTER_CUBIC)
    draw_text_box(enlarged, f"lane-marking calibration crop | t={timestamp_s:.0f}s | coordinates are original pixels", (20, 52), (255, 255, 255))
    cv2.imwrite(str(output_path), enlarged, [cv2.IMWRITE_JPEG_QUALITY, 97])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--video", type=Path, default=DEFAULT_VIDEO)
    parser.add_argument("--outdir", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--times", type=float, nargs="+", default=[120.0, 240.0])
    args = parser.parse_args()

    video = args.video.expanduser().resolve()
    outdir = args.outdir.expanduser().resolve()
    outdir.mkdir(parents=True, exist_ok=True)

    for timestamp_s in args.times:
        raw = read_frame(video, timestamp_s)
        stamp = f"{int(round(timestamp_s)):04d}s"
        phase = phase_for_timestamp(timestamp_s)

        speed_frame = raw.copy()
        draw_speed_lines(speed_frame, timestamp_s, phase)
        speed_path = outdir / f"outbound_1621_{stamp}_{phase}_speed_lines_v2.jpg"
        cv2.imwrite(str(speed_path), speed_frame, [cv2.IMWRITE_JPEG_QUALITY, 96])

        density_frame = raw.copy()
        draw_density_rois(density_frame, timestamp_s, phase)
        density_path = outdir / f"outbound_1621_{stamp}_{phase}_density_rois_v2.jpg"
        cv2.imwrite(str(density_path), density_frame, [cv2.IMWRITE_JPEG_QUALITY, 96])

        calibration_path = outdir / f"outbound_1621_{stamp}_{phase}_calibration_crop.jpg"
        write_calibration_crop(raw, calibration_path, timestamp_s)

        print(speed_path)
        print(density_path)
        print(calibration_path)


if __name__ == "__main__":
    main()
