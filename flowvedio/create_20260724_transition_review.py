#!/usr/bin/env python3
"""Export wall-clock review frames for locating 2026-07-24 tidal-lane transitions."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import cv2
import numpy as np


ROOT = Path(__file__).resolve().parent
VIDEO_DIR = ROOT / "2026072324彩田路"
OUT_DIR = ROOT / "batch_outputs" / "20260724_caitian_transition_review_v1"

VIDEO_STARTS = {
    "IMG_0006.MOV": datetime.fromisoformat("2026-07-24 14:42:47"),
    "IMG_0022.mov": datetime.fromisoformat("2026-07-24 16:30:58"),
    "IMG_0007.MOV": datetime.fromisoformat("2026-07-24 17:31:23"),
    "IMG_0008.MOV": datetime.fromisoformat("2026-07-24 20:03:43"),
    "IMG_0020.mov": datetime.fromisoformat("2026-07-24 14:42:02"),
    "IMG_3028.mov": datetime.fromisoformat("2026-07-24 16:26:46"),
    "IMG_3029.mov": datetime.fromisoformat("2026-07-24 20:02:51"),
}

REVIEW_WINDOWS = {
    "inbound_opening_1min": [
        ("IMG_0006.MOV", datetime.fromisoformat("2026-07-24 15:55:00"), datetime.fromisoformat("2026-07-24 16:30:00"), 60),
        ("IMG_0022.mov", datetime.fromisoformat("2026-07-24 16:31:00"), datetime.fromisoformat("2026-07-24 16:40:00"), 60),
    ],
    "outbound_opening_bracket_1min": [
        ("IMG_0020.mov", datetime.fromisoformat("2026-07-24 15:50:00"), datetime.fromisoformat("2026-07-24 16:07:00"), 60),
        ("IMG_3028.mov", datetime.fromisoformat("2026-07-24 16:27:00"), datetime.fromisoformat("2026-07-24 16:45:00"), 60),
    ],
    "inbound_recovery_bracket_2min": [
        ("IMG_0007.MOV", datetime.fromisoformat("2026-07-24 18:10:00"), datetime.fromisoformat("2026-07-24 18:30:00"), 120),
        ("IMG_0008.MOV", datetime.fromisoformat("2026-07-24 20:04:00"), datetime.fromisoformat("2026-07-24 20:24:00"), 120),
    ],
    "outbound_recovery_bracket_2min": [
        ("IMG_3028.mov", datetime.fromisoformat("2026-07-24 18:10:00"), datetime.fromisoformat("2026-07-24 18:30:00"), 120),
        ("IMG_3029.mov", datetime.fromisoformat("2026-07-24 20:04:00"), datetime.fromisoformat("2026-07-24 20:24:00"), 120),
    ],
}


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


def label(frame: np.ndarray, filename: str, wall_time: datetime, offset_s: float) -> np.ndarray:
    result = frame.copy()
    text = f"{wall_time:%Y-%m-%d %H:%M:%S} | {filename} | t={offset_s:.1f}s"
    cv2.rectangle(result, (0, 0), (result.shape[1], 55), (0, 0, 0), -1)
    cv2.putText(result, text, (18, 38), cv2.FONT_HERSHEY_SIMPLEX, 0.82, (255, 255, 255), 2, cv2.LINE_AA)
    return result


def iter_times(start: datetime, end: datetime, step_s: int):
    current = start
    while current <= end:
        yield current
        current += timedelta(seconds=step_s)


def build_contact_sheet(frames: list[np.ndarray], cols: int = 4) -> np.ndarray:
    thumbs = [cv2.resize(frame, (480, 270), interpolation=cv2.INTER_AREA) for frame in frames]
    blank = np.full_like(thumbs[0], 245)
    while len(thumbs) % cols:
        thumbs.append(blank.copy())
    return np.vstack([np.hstack(thumbs[i : i + cols]) for i in range(0, len(thumbs), cols)])


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for window_id, pieces in REVIEW_WINDOWS.items():
        window_dir = OUT_DIR / window_id
        window_dir.mkdir(parents=True, exist_ok=True)
        review_frames = []
        for filename, wall_start, wall_end, step_s in pieces:
            video_start = VIDEO_STARTS[filename]
            for wall_time in iter_times(wall_start, wall_end, step_s):
                offset_s = (wall_time - video_start).total_seconds()
                frame = label(read_frame(VIDEO_DIR / filename, offset_s), filename, wall_time, offset_s)
                output = window_dir / f"{wall_time:%H%M%S}_{Path(filename).stem}.jpg"
                cv2.imwrite(str(output), frame, [cv2.IMWRITE_JPEG_QUALITY, 92])
                review_frames.append(frame)
        sheet = build_contact_sheet(review_frames)
        cv2.imwrite(str(OUT_DIR / f"{window_id}_contact_sheet.jpg"), sheet, [cv2.IMWRITE_JPEG_QUALITY, 92])
        print(OUT_DIR / f"{window_id}_contact_sheet.jpg")


if __name__ == "__main__":
    main()
