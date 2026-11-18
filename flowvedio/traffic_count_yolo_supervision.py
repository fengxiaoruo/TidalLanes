#!/usr/bin/env python3
"""Count vehicles using YOLO detections plus supervision ByteTrack."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict, deque
from pathlib import Path

import cv2
import numpy as np
import supervision as sv
from ultralytics import YOLO


VEHICLE_CLASSES = {
    2: "car",
    3: "motorcycle",
    5: "bus",
    7: "truck",
}


def parse_line(value: str) -> tuple[tuple[int, int], tuple[int, int]]:
    parts = [int(v.strip()) for v in value.split(",")]
    if len(parts) != 4:
        raise argparse.ArgumentTypeError("line must be x1,y1,x2,y2")
    return (parts[0], parts[1]), (parts[2], parts[3])


def side_of_line(point: tuple[float, float], line: tuple[tuple[int, int], tuple[int, int]]) -> float:
    (x1, y1), (x2, y2) = line
    px, py = point
    return (x2 - x1) * (py - y1) - (y2 - y1) * (px - x1)


def direction_from_sides(previous: float, current: float) -> str | None:
    if previous < 0 <= current:
        return "neg_to_pos"
    if previous > 0 >= current:
        return "pos_to_neg"
    return None


def segments_intersect(
    a: tuple[float, float],
    b: tuple[float, float],
    c: tuple[float, float],
    d: tuple[float, float],
) -> bool:
    def orient(p, q, r) -> float:
        return (q[0] - p[0]) * (r[1] - p[1]) - (q[1] - p[1]) * (r[0] - p[0])

    def on_segment(p, q, r) -> bool:
        return (
            min(p[0], r[0]) <= q[0] <= max(p[0], r[0])
            and min(p[1], r[1]) <= q[1] <= max(p[1], r[1])
        )

    o1 = orient(a, b, c)
    o2 = orient(a, b, d)
    o3 = orient(c, d, a)
    o4 = orient(c, d, b)
    if o1 * o2 < 0 and o3 * o4 < 0:
        return True
    eps = 1e-9
    return (
        (abs(o1) <= eps and on_segment(a, c, b))
        or (abs(o2) <= eps and on_segment(a, d, b))
        or (abs(o3) <= eps and on_segment(c, a, d))
        or (abs(o4) <= eps and on_segment(c, b, d))
    )


def draw_label(frame, text: str, origin: tuple[int, int], color: tuple[int, int, int]) -> None:
    font = cv2.FONT_HERSHEY_SIMPLEX
    scale = 0.6
    thickness = 2
    (w, h), baseline = cv2.getTextSize(text, font, scale, thickness)
    x, y = origin
    cv2.rectangle(frame, (x, y - h - 8), (x + w + 8, y + baseline + 4), (0, 0, 0), -1)
    cv2.putText(frame, text, (x + 4, y - 4), font, scale, color, thickness, cv2.LINE_AA)


def main() -> None:
    default_model = Path(__file__).resolve().with_name("models") / "yolo11m.pt"
    parser = argparse.ArgumentParser()
    parser.add_argument("video", type=Path)
    parser.add_argument("--outdir", type=Path)
    parser.add_argument("--model", default=str(default_model))
    parser.add_argument("--imgsz", type=int, default=1600)
    parser.add_argument("--conf", type=float, default=0.03)
    parser.add_argument("--stride", type=int, default=2)
    parser.add_argument("--line", type=parse_line, required=True)
    parser.add_argument("--device", default=None)
    parser.add_argument("--track-activation-threshold", type=float, default=0.03)
    parser.add_argument("--minimum-matching-threshold", type=float, default=0.75)
    parser.add_argument("--minimum-consecutive-frames", type=int, default=1)
    parser.add_argument("--lost-track-buffer", type=int, default=45)
    args = parser.parse_args()

    video = args.video.expanduser().resolve()
    outdir = (args.outdir or video.parent / "traffic_count_outputs").resolve()
    outdir.mkdir(parents=True, exist_ok=True)

    cap = cv2.VideoCapture(str(video))
    if not cap.isOpened():
        raise SystemExit(f"Could not open video: {video}")
    fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    frames_total = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    processed_fps = fps / max(args.stride, 1)

    stem = video.stem
    events_path = outdir / f"{stem}_vehicle_crossings.csv"
    minute_path = outdir / f"{stem}_vehicle_counts_by_minute.csv"
    summary_path = outdir / f"{stem}_traffic_summary.json"
    video_path = outdir / f"{stem}_annotated_count.mp4"

    writer = cv2.VideoWriter(
        str(video_path),
        cv2.VideoWriter_fourcc(*"mp4v"),
        processed_fps,
        (width, height),
    )
    if not writer.isOpened():
        raise SystemExit(f"Could not create annotated video: {video_path}")

    model = YOLO(args.model)
    tracker = sv.ByteTrack(
        track_activation_threshold=args.track_activation_threshold,
        lost_track_buffer=args.lost_track_buffer,
        minimum_matching_threshold=args.minimum_matching_threshold,
        frame_rate=processed_fps,
        minimum_consecutive_frames=args.minimum_consecutive_frames,
    )

    previous_side: dict[int, float] = {}
    previous_center: dict[int, tuple[float, float]] = {}
    counted: set[tuple[int, str]] = set()
    track_history: dict[int, deque[tuple[int, int]]] = defaultdict(lambda: deque(maxlen=24))
    counts = Counter()
    counts_by_direction = Counter()
    counts_by_class = Counter()
    events: list[dict[str, object]] = []

    frame_index = 0
    processed = 0
    while True:
        ok, frame = cap.read()
        if not ok:
            break
        if frame_index % args.stride != 0:
            frame_index += 1
            continue

        result = model.predict(
            frame,
            classes=list(VEHICLE_CLASSES.keys()),
            conf=args.conf,
            imgsz=args.imgsz,
            device=args.device,
            verbose=False,
        )[0]
        detections = sv.Detections.from_ultralytics(result)
        detections = tracker.update_with_detections(detections)

        for xyxy, confidence, class_id, tracker_id in zip(
            detections.xyxy,
            detections.confidence,
            detections.class_id,
            detections.tracker_id,
        ):
            if tracker_id is None or class_id not in VEHICLE_CLASSES:
                continue
            x1, y1, x2, y2 = [float(v) for v in xyxy]
            cx, cy = (x1 + x2) / 2, (y1 + y2) / 2
            current_side = side_of_line((cx, cy), args.line)
            previous = previous_side.get(int(tracker_id))
            prev_center = previous_center.get(int(tracker_id))
            if previous is not None and prev_center is not None:
                direction = direction_from_sides(previous, current_side)
                crossed_segment = segments_intersect(prev_center, (cx, cy), *args.line)
                if direction and crossed_segment and (int(tracker_id), direction) not in counted:
                    counted.add((int(tracker_id), direction))
                    vehicle_class = VEHICLE_CLASSES[int(class_id)]
                    timestamp_s = frame_index / fps
                    events.append(
                        {
                            "timestamp_s": round(timestamp_s, 3),
                            "frame_index": frame_index,
                            "track_id": int(tracker_id),
                            "class": vehicle_class,
                            "direction": direction,
                            "confidence": round(float(confidence), 4),
                            "center_x": round(cx, 1),
                            "center_y": round(cy, 1),
                        }
                    )
                    counts["total"] += 1
                    counts_by_direction[direction] += 1
                    counts_by_class[vehicle_class] += 1
            previous_side[int(tracker_id)] = current_side
            previous_center[int(tracker_id)] = (cx, cy)

            box_color = (40, 220, 80)
            cv2.rectangle(frame, (int(x1), int(y1)), (int(x2), int(y2)), box_color, 1)
            draw_label(
                frame,
                f"#{int(tracker_id)} {VEHICLE_CLASSES[int(class_id)]} {float(confidence):.2f}",
                (int(x1), max(22, int(y1) - 3)),
                box_color,
            )
            history = track_history[int(tracker_id)]
            history.append((int(cx), int(cy)))
            if len(history) > 1:
                pts = np.array(history, dtype=np.int32).reshape((-1, 1, 2))
                cv2.polylines(frame, [pts], False, box_color, 1)

        cv2.line(frame, args.line[0], args.line[1], (0, 255, 255), 4)
        draw_label(frame, f"total {counts['total']}", (24, 42), (255, 255, 255))
        draw_label(frame, f"neg_to_pos {counts_by_direction['neg_to_pos']}", (24, 82), (255, 255, 255))
        draw_label(frame, f"pos_to_neg {counts_by_direction['pos_to_neg']}", (24, 122), (255, 255, 255))
        writer.write(frame)

        processed += 1
        if processed % 100 == 0:
            print(f"processed {processed} frames; count={counts['total']}", flush=True)
        frame_index += 1

    cap.release()
    writer.release()

    with events_path.open("w", newline="") as f:
        fieldnames = [
            "timestamp_s",
            "frame_index",
            "track_id",
            "class",
            "direction",
            "confidence",
            "center_x",
            "center_y",
        ]
        out = csv.DictWriter(f, fieldnames=fieldnames)
        out.writeheader()
        out.writerows(events)

    minute_counts = Counter()
    for event in events:
        minute = int(float(event["timestamp_s"]) // 60)
        minute_counts[(minute, event["direction"], event["class"])] += 1
    with minute_path.open("w", newline="") as f:
        fieldnames = ["minute", "start_s", "end_s", "direction", "class", "count"]
        out = csv.DictWriter(f, fieldnames=fieldnames)
        out.writeheader()
        for (minute, direction, vehicle_class), count in sorted(minute_counts.items()):
            out.writerow(
                {
                    "minute": minute,
                    "start_s": minute * 60,
                    "end_s": (minute + 1) * 60,
                    "direction": direction,
                    "class": vehicle_class,
                    "count": count,
                }
            )

    summary = {
        "video": str(video),
        "model": args.model,
        "frame_width": width,
        "frame_height": height,
        "fps": fps,
        "frame_count": frames_total,
        "duration_s": frames_total / fps if fps else None,
        "processed_stride": args.stride,
        "processed_frames": processed,
        "line_xyxy": [*args.line[0], *args.line[1]],
        "total_count": counts["total"],
        "counts_by_direction": dict(counts_by_direction),
        "counts_by_class": dict(counts_by_class),
        "events_csv": str(events_path),
        "counts_by_minute_csv": str(minute_path),
        "annotated_video": str(video_path),
    }
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2))
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
