#!/usr/bin/env python3
"""Create gridded snapshots for manual transition-time and flow-line confirmation."""

from __future__ import annotations

from pathlib import Path

import cv2
import numpy as np


ROOT = Path(__file__).resolve().parent
VIDEO_DIR = ROOT / "2026072324彩田路"
OUT_DIR = ROOT / "batch_outputs" / "20260724_caitian_manual_locator_v1"

REVIEW_FRAMES = [
    ("inbound", "opening", "pre", "IMG_0006.MOV", 4633.0, "2026-07-24 16:00:00"),
    ("inbound", "opening", "candidate_transition", "IMG_0006.MOV", 5833.0, "2026-07-24 16:20:00"),
    ("inbound", "opening", "candidate_post", "IMG_0006.MOV", 6313.0, "2026-07-24 16:28:00"),
    ("inbound", "opening", "post_check", "IMG_0022.mov", 242.0, "2026-07-24 16:35:00"),
    ("outbound", "opening", "last_pre_check", "IMG_0020.mov", 4678.0, "2026-07-24 16:00:00"),
    ("outbound", "opening", "first_post_check", "IMG_3028.mov", 14.0, "2026-07-24 16:27:00"),
    ("inbound", "recovery", "last_tidal_check", "IMG_0007.MOV", 2917.0, "2026-07-24 18:20:00"),
    ("inbound", "recovery", "first_recovered_check", "IMG_0008.MOV", 77.0, "2026-07-24 20:05:00"),
    ("outbound", "recovery", "last_tidal_check", "IMG_3028.mov", 6794.0, "2026-07-24 18:20:00"),
    ("outbound", "recovery", "first_recovered_check", "IMG_3029.mov", 129.0, "2026-07-24 20:05:00"),
]


def read_frame(video: Path, offset_s: float) -> np.ndarray:
    cap = cv2.VideoCapture(str(video))
    if not cap.isOpened():
        raise RuntimeError(f"Could not open {video}")
    cap.set(cv2.CAP_PROP_POS_MSEC, offset_s * 1000.0)
    ok, frame = cap.read()
    cap.release()
    if not ok:
        raise RuntimeError(f"Could not read {video} at {offset_s:.3f}s")
    return frame


def add_grid(frame: np.ndarray, step: int = 100) -> None:
    height, width = frame.shape[:2]
    for x in range(0, width, step):
        cv2.line(frame, (x, 0), (x, height), (205, 205, 205), 1)
        cv2.putText(frame, str(x), (x + 4, 80), cv2.FONT_HERSHEY_SIMPLEX, 0.55, (0, 255, 255), 2, cv2.LINE_AA)
    for y in range(100, height, step):
        cv2.line(frame, (0, y), (width, y), (205, 205, 205), 1)
        cv2.putText(frame, str(y), (8, y + 20), cv2.FONT_HERSHEY_SIMPLEX, 0.55, (0, 255, 255), 2, cv2.LINE_AA)


def add_label(frame: np.ndarray, text: str) -> None:
    cv2.rectangle(frame, (0, 0), (frame.shape[1], 57), (0, 0, 0), -1)
    cv2.putText(frame, text, (16, 39), cv2.FONT_HERSHEY_SIMPLEX, 0.78, (255, 255, 255), 2, cv2.LINE_AA)


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    previews = []
    for index, (direction, event, role, filename, offset_s, wall_time) in enumerate(REVIEW_FRAMES, start=1):
        review_id = f"R{index:02d}"
        frame = read_frame(VIDEO_DIR / filename, offset_s)
        label = f"{review_id} | {direction} | {event} | {role} | {wall_time} | {filename} | t={offset_s:.1f}s"

        raw = frame.copy()
        add_label(raw, label)
        raw_path = OUT_DIR / f"{review_id}_{direction}_{event}_{role}_raw.jpg"
        cv2.imwrite(str(raw_path), raw, [cv2.IMWRITE_JPEG_QUALITY, 94])

        gridded = frame.copy()
        add_grid(gridded)
        add_label(gridded, label)
        grid_path = OUT_DIR / f"{review_id}_{direction}_{event}_{role}_grid.jpg"
        cv2.imwrite(str(grid_path), gridded, [cv2.IMWRITE_JPEG_QUALITY, 94])

        previews.append(cv2.resize(gridded, (640, 360), interpolation=cv2.INTER_AREA))
        print(grid_path)

    blank = np.full_like(previews[0], 245)
    while len(previews) % 2:
        previews.append(blank.copy())
    sheet = np.vstack([np.hstack(previews[i : i + 2]) for i in range(0, len(previews), 2)])
    cv2.imwrite(str(OUT_DIR / "manual_locator_contact_sheet.jpg"), sheet, [cv2.IMWRITE_JPEG_QUALITY, 94])
    print(OUT_DIR / "manual_locator_contact_sheet.jpg")


if __name__ == "__main__":
    main()
