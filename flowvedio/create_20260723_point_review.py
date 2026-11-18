#!/usr/bin/env python3
"""Export grid-overlay frames for 2026-07-23 Caitian Road point review."""

from __future__ import annotations

from pathlib import Path

import cv2
import numpy as np


ROOT = Path(__file__).resolve().parent
VIDEO_DIR = ROOT / "20260723彩田路"
OUT_DIR = ROOT / "batch_outputs" / "20260723_caitian_point_review"

REVIEW_FRAMES = [
    ("IMG_0003.MOV", 60.0, "inbound_base"),
    ("IMG_0004.MOV", 60.0, "inbound_new_file"),
    ("IMG_0016.MOV", 60.0, "outbound_base"),
    ("IMG_3023.MOV", 60.0, "outbound_new_file"),
    ("IMG_3024.MOV", 300.0, "inbound_pre_deployment"),
    ("IMG_3024.MOV", 1800.0, "inbound_post_deployment"),
    ("IMG_0017.MOV", 300.0, "outbound_pre_deployment"),
    ("IMG_0017.MOV", 1800.0, "outbound_post_deployment"),
    ("IMG_0017.MOV", 4320.0, "outbound_after_camera_adjustment"),
]

CONFIRMED_GATES = {
    ("IMG_0003.MOV", 60): (((700, 550), (900, 550), (1150, 550)), ((150, 800), (900, 800), (1500, 800))),
    ("IMG_0004.MOV", 60): (((700, 550), (900, 550), (1150, 550)), ((150, 800), (900, 800), (1500, 800))),
    ("IMG_0016.MOV", 60): (((700, 600), (900, 600), (1200, 600)), ((300, 900), (900, 900), (1700, 900))),
    ("IMG_3023.MOV", 60): (((800, 600), (1000, 600), (1250, 600)), ((400, 900), (1000, 900), (1800, 900))),
    ("IMG_3024.MOV", 300): (((680, 600), (940, 600), (1200, 600)), ((200, 800), (900, 800), (1500, 800))),
    ("IMG_3024.MOV", 1800): (((680, 600), (840, 600), (1200, 600)), ((200, 800), (700, 800), (1500, 800))),
    ("IMG_0017.MOV", 300): (((750, 600), (1080, 600), (1450, 600)), ((500, 800), (1100, 800), (1900, 800))),
    ("IMG_0017.MOV", 1800): (((750, 600), (1180, 600), (1450, 600)), ((500, 800), (1300, 800), (1900, 800))),
    ("IMG_0017.MOV", 4320): (((800, 600), (1170, 600), (1400, 600)), ((550, 800), (1300, 800), (1800, 800))),
}

COLORS = {
    "south_to_north": (0, 215, 255),
    "north_to_south": (255, 155, 40),
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
        raise RuntimeError(f"Could not read frame at {timestamp_s:.3f}s: {video}")
    return frame


def text_box(frame: np.ndarray, text: str, xy: tuple[int, int]) -> None:
    font = cv2.FONT_HERSHEY_SIMPLEX
    scale = 0.62
    thickness = 2
    x, y = xy
    (width, height), baseline = cv2.getTextSize(text, font, scale, thickness)
    cv2.rectangle(
        frame,
        (x - 4, y - height - 7),
        (x + width + 5, y + baseline + 4),
        (0, 0, 0),
        -1,
    )
    cv2.putText(
        frame,
        text,
        (x, y),
        font,
        scale,
        (255, 255, 255),
        thickness,
        cv2.LINE_AA,
    )


def add_grid(frame: np.ndarray, step: int = 100) -> None:
    height, width = frame.shape[:2]
    for x in range(0, width, step):
        cv2.line(frame, (x, 0), (x, height), (205, 205, 205), 1)
        cv2.putText(
            frame,
            str(x),
            (x + 4, 24),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.58,
            (0, 255, 255),
            2,
            cv2.LINE_AA,
        )
    for y in range(0, height, step):
        cv2.line(frame, (0, y), (width, y), (205, 205, 205), 1)
        cv2.putText(
            frame,
            str(y),
            (7, min(height - 8, y + 22)),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.58,
            (0, 255, 255),
            2,
            cv2.LINE_AA,
        )


def add_confirmed_gates(
    frame: np.ndarray,
    gate_1: tuple[tuple[int, int], tuple[int, int], tuple[int, int]],
    gate_2: tuple[tuple[int, int], tuple[int, int], tuple[int, int]],
) -> None:
    for gate_number, (left, split, right) in enumerate((gate_1, gate_2), start=1):
        cv2.line(frame, left, split, COLORS["south_to_north"], 6, cv2.LINE_AA)
        cv2.line(frame, split, right, COLORS["north_to_south"], 6, cv2.LINE_AA)
        for point in (left, split, right):
            cv2.circle(frame, point, 8, (255, 255, 255), -1, cv2.LINE_AA)
            cv2.circle(frame, point, 8, (0, 0, 0), 2, cv2.LINE_AA)
        text_box(
            frame,
            f"Gate {gate_number}: {left} | split {split} | {right}",
            (max(24, left[0]), max(90, left[1] - 14)),
        )


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    previews = []
    for filename, timestamp_s, stage in REVIEW_FRAMES:
        frame = read_frame(VIDEO_DIR / filename, timestamp_s)
        raw_path = OUT_DIR / f"{filename.removesuffix('.MOV')}_{int(timestamp_s):05d}s_{stage}_raw.jpg"
        cv2.imwrite(str(raw_path), frame, [cv2.IMWRITE_JPEG_QUALITY, 96])

        gridded = frame.copy()
        add_grid(gridded)
        minute = int(timestamp_s // 60)
        second = int(timestamp_s % 60)
        text_box(
            gridded,
            f"2026-07-23 | {filename} | t={minute:02d}:{second:02d} | {stage}",
            (24, 1058),
        )
        text_box(
            gridded,
            "Please confirm two transverse speed gates for EACH direction and lane/flow points.",
            (24, 58),
        )
        grid_path = OUT_DIR / f"{filename.removesuffix('.MOV')}_{int(timestamp_s):05d}s_{stage}_grid.jpg"
        cv2.imwrite(str(grid_path), gridded, [cv2.IMWRITE_JPEG_QUALITY, 96])

        confirmed = gridded.copy()
        add_confirmed_gates(confirmed, *CONFIRMED_GATES[(filename, int(timestamp_s))])
        text_box(
            confirmed,
            "YELLOW = south_to_north | BLUE = north_to_south | physical gate distance pending",
            (24, 92),
        )
        confirmed_path = OUT_DIR / f"{filename.removesuffix('.MOV')}_{int(timestamp_s):05d}s_{stage}_confirmed.jpg"
        cv2.imwrite(str(confirmed_path), confirmed, [cv2.IMWRITE_JPEG_QUALITY, 96])

        preview = cv2.resize(confirmed, (640, 360), interpolation=cv2.INTER_AREA)
        previews.append(preview)
        print(confirmed_path)

    blank = np.zeros_like(previews[0])
    while len(previews) % 3:
        previews.append(blank.copy())
    rows = [np.hstack(previews[i : i + 3]) for i in range(0, len(previews), 3)]
    contact_sheet = np.vstack(rows)
    contact_path = OUT_DIR / "confirmed_gates_contact_sheet.jpg"
    cv2.imwrite(str(contact_path), contact_sheet, [cv2.IMWRITE_JPEG_QUALITY, 94])
    print(contact_path)


if __name__ == "__main__":
    main()
