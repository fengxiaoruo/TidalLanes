#!/usr/bin/env python3
"""Create gridded frames at user-confirmed transition and camera-change times."""

from __future__ import annotations

from pathlib import Path

import cv2
import numpy as np

from create_20260724_manual_locator import add_grid, add_label, read_frame


ROOT = Path(__file__).resolve().parent
VIDEO_DIR = ROOT / "2026072324彩田路"
OUT_DIR = ROOT / "batch_outputs" / "20260723_24_confirmed_manual_locator_v2"

REVIEW_FRAMES = [
    ("C01", "2026-07-23", "inbound", "recovery", "pre", "IMG_3026.MOV", 4200, "2026-07-23 21:26:48"),
    ("C02", "2026-07-23", "inbound", "recovery", "start_no_flow", "IMG_3026.MOV", 4500, "2026-07-23 21:31:48"),
    ("C03", "2026-07-23", "inbound", "recovery", "operation", "IMG_3026.MOV", 5406, "2026-07-23 21:46:54"),
    ("C04", "2026-07-23", "inbound", "recovery", "complete", "IMG_3026.MOV", 5526, "2026-07-23 21:48:54"),
    ("C05", "2026-07-23", "outbound", "recovery", "pre", "IMG_0019.MOV", 4500, "2026-07-23 21:26:35"),
    ("C06", "2026-07-23", "outbound", "recovery", "start", "IMG_0019.MOV", 4800, "2026-07-23 21:31:35"),
    ("C07", "2026-07-23", "outbound", "recovery", "complete", "IMG_0019.MOV", 5700, "2026-07-23 21:46:35"),
    ("C08", "2026-07-24", "inbound", "opening", "pre", "IMG_0006.MOV", 5400, "2026-07-24 16:12:47"),
    ("C09", "2026-07-24", "inbound", "opening", "start", "IMG_0006.MOV", 5640, "2026-07-24 16:16:47"),
    ("C10", "2026-07-24", "inbound", "opening", "complete", "IMG_0006.MOV", 6440, "2026-07-24 16:30:07"),
    ("C11", "2026-07-24", "inbound", "opening", "post_new_file", "IMG_0022.mov", 5, "2026-07-24 16:31:03"),
    ("C12", "2026-07-24", "outbound", "opening", "last_pre", "IMG_0020.mov", 4800, "2026-07-24 16:02:02"),
    ("C13", "2026-07-24", "outbound", "opening", "transition_tail", "IMG_3028.mov", 30, "2026-07-24 16:27:16"),
    ("C14", "2026-07-24", "outbound", "opening", "complete", "IMG_3028.mov", 135, "2026-07-24 16:29:01"),
    ("C15", "2026-07-24", "inbound", "recovery", "camera_a_stable", "IMG_0008.MOV", 300, "2026-07-24 20:08:43"),
    ("C16", "2026-07-24", "inbound", "recovery", "before_camera_shake", "IMG_0008.MOV", 4140, "2026-07-24 21:12:43"),
    ("C17", "2026-07-24", "inbound", "recovery", "camera_b_stable", "IMG_0008.MOV", 4200, "2026-07-24 21:13:43"),
    ("C18", "2026-07-24", "inbound", "recovery", "start", "IMG_0008.MOV", 5450, "2026-07-24 21:34:33"),
    ("C19", "2026-07-24", "inbound", "recovery", "complete", "IMG_0008.MOV", 6000, "2026-07-24 21:43:43"),
    ("C20", "2026-07-24", "outbound", "recovery", "stable", "IMG_3029.mov", 300, "2026-07-24 20:07:51"),
    ("C21", "2026-07-24", "outbound", "recovery", "start", "IMG_3029.mov", 5485, "2026-07-24 21:34:16"),
    ("C22", "2026-07-24", "outbound", "recovery", "complete", "IMG_3029.mov", 5990, "2026-07-24 21:42:41"),
]


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    previews = []
    for review_id, date, direction, event, role, filename, offset_s, wall_time in REVIEW_FRAMES:
        frame = read_frame(VIDEO_DIR / filename, float(offset_s))
        text = f"{review_id} | {date} | {direction} | {event} | {role} | {wall_time} | {filename} | t={offset_s}s"

        raw = frame.copy()
        add_label(raw, text)
        cv2.imwrite(str(OUT_DIR / f"{review_id}_{direction}_{event}_{role}_raw.jpg"), raw, [cv2.IMWRITE_JPEG_QUALITY, 94])

        gridded = frame.copy()
        add_grid(gridded)
        add_label(gridded, text)
        grid_path = OUT_DIR / f"{review_id}_{direction}_{event}_{role}_grid.jpg"
        cv2.imwrite(str(grid_path), gridded, [cv2.IMWRITE_JPEG_QUALITY, 94])
        previews.append(cv2.resize(gridded, (640, 360), interpolation=cv2.INTER_AREA))
        print(grid_path)

    blank = np.full_like(previews[0], 245)
    while len(previews) % 2:
        previews.append(blank.copy())
    sheet = np.vstack([np.hstack(previews[i : i + 2]) for i in range(0, len(previews), 2)])
    cv2.imwrite(str(OUT_DIR / "confirmed_transition_contact_sheet.jpg"), sheet, [cv2.IMWRITE_JPEG_QUALITY, 94])
    print(OUT_DIR / "confirmed_transition_contact_sheet.jpg")


if __name__ == "__main__":
    main()
