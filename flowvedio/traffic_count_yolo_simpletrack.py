#!/usr/bin/env python3
"""Count vehicles with YOLO detections and a simple greedy tracker."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict, deque
from pathlib import Path

import cv2
import numpy as np
from ultralytics import YOLO


VEHICLE_CLASSES = {2: "car", 3: "motorcycle", 5: "bus", 7: "truck"}


def parse_line(value: str) -> tuple[tuple[int, int], tuple[int, int]]:
    parts = [int(v.strip()) for v in value.split(",")]
    if len(parts) != 4:
        raise argparse.ArgumentTypeError("line must be x1,y1,x2,y2")
    return (parts[0], parts[1]), (parts[2], parts[3])


def center(box: np.ndarray) -> tuple[float, float]:
    x1, y1, x2, y2 = box
    return float((x1 + x2) / 2), float((y1 + y2) / 2)


def iou(a: np.ndarray, b: np.ndarray) -> float:
    ax1, ay1, ax2, ay2 = a
    bx1, by1, bx2, by2 = b
    ix1, iy1 = max(ax1, bx1), max(ay1, by1)
    ix2, iy2 = min(ax2, bx2), min(ay2, by2)
    iw, ih = max(0.0, ix2 - ix1), max(0.0, iy2 - iy1)
    inter = iw * ih
    area_a = max(0.0, ax2 - ax1) * max(0.0, ay2 - ay1)
    area_b = max(0.0, bx2 - bx1) * max(0.0, by2 - by1)
    denom = area_a + area_b - inter
    return inter / denom if denom else 0.0


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


def segments_intersect(a, b, c, d) -> bool:
    def orient(p, q, r) -> float:
        return (q[0] - p[0]) * (r[1] - p[1]) - (q[1] - p[1]) * (r[0] - p[0])

    def on_segment(p, q, r) -> bool:
        return min(p[0], r[0]) <= q[0] <= max(p[0], r[0]) and min(p[1], r[1]) <= q[1] <= max(p[1], r[1])

    o1, o2 = orient(a, b, c), orient(a, b, d)
    o3, o4 = orient(c, d, a), orient(c, d, b)
    if o1 * o2 < 0 and o3 * o4 < 0:
        return True
    eps = 1e-9
    return (
        (abs(o1) <= eps and on_segment(a, c, b))
        or (abs(o2) <= eps and on_segment(a, d, b))
        or (abs(o3) <= eps and on_segment(c, a, d))
        or (abs(o4) <= eps and on_segment(c, b, d))
    )


class SimpleTracker:
    def __init__(self, max_distance: float, min_iou: float, max_missed: int) -> None:
        self.max_distance = max_distance
        self.min_iou = min_iou
        self.max_missed = max_missed
        self.next_id = 1
        self.tracks: dict[int, dict[str, object]] = {}

    def update(self, detections: list[dict[str, object]]) -> list[dict[str, object]]:
        unmatched = set(range(len(detections)))
        updated: list[dict[str, object]] = []

        for track_id, track in list(self.tracks.items()):
            best_idx = None
            best_score = float("inf")
            old_box = track["box"]
            old_center = track["center"]
            for idx in list(unmatched):
                det = detections[idx]
                det_center = det["center"]
                dist = float(np.hypot(det_center[0] - old_center[0], det_center[1] - old_center[1]))
                overlap = iou(old_box, det["box"])
                if overlap >= self.min_iou or dist <= self.max_distance:
                    score = dist - overlap * 80
                    if score < best_score:
                        best_score = score
                        best_idx = idx
            if best_idx is None:
                track["missed"] = int(track["missed"]) + 1
                if int(track["missed"]) > self.max_missed:
                    del self.tracks[track_id]
                continue
            unmatched.remove(best_idx)
            det = detections[best_idx]
            det["track_id"] = track_id
            self.tracks[track_id] = {
                "box": det["box"],
                "center": det["center"],
                "class_id": det["class_id"],
                "missed": 0,
            }
            updated.append(det)

        for idx in sorted(unmatched):
            det = detections[idx]
            track_id = self.next_id
            self.next_id += 1
            det["track_id"] = track_id
            self.tracks[track_id] = {
                "box": det["box"],
                "center": det["center"],
                "class_id": det["class_id"],
                "missed": 0,
            }
            updated.append(det)
        return updated


def draw_label(frame, text: str, origin: tuple[int, int], color: tuple[int, int, int]) -> None:
    font = cv2.FONT_HERSHEY_SIMPLEX
    scale = 0.55
    thickness = 1
    (w, h), baseline = cv2.getTextSize(text, font, scale, thickness)
    x, y = origin
    cv2.rectangle(frame, (x, y - h - 6), (x + w + 6, y + baseline + 3), (0, 0, 0), -1)
    cv2.putText(frame, text, (x + 3, y - 3), font, scale, color, thickness, cv2.LINE_AA)


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
    parser.add_argument("--max-distance", type=float, default=42)
    parser.add_argument("--min-iou", type=float, default=0.02)
    parser.add_argument("--max-missed", type=int, default=8)
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
    writer = cv2.VideoWriter(str(video_path), cv2.VideoWriter_fourcc(*"mp4v"), processed_fps, (width, height))

    model = YOLO(args.model)
    tracker = SimpleTracker(args.max_distance, args.min_iou, args.max_missed)
    previous_side: dict[int, float] = {}
    previous_center: dict[int, tuple[float, float]] = {}
    counted: set[int] = set()
    track_history: dict[int, deque[tuple[int, int]]] = defaultdict(lambda: deque(maxlen=18))
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
        detections = []
        if result.boxes is not None:
            for box, conf, cls in zip(result.boxes.xyxy.cpu().numpy(), result.boxes.conf.cpu().numpy(), result.boxes.cls.int().cpu().numpy()):
                detections.append({"box": box.astype(float), "confidence": float(conf), "class_id": int(cls), "center": center(box)})
        detections.sort(key=lambda d: d["confidence"], reverse=True)
        tracks = tracker.update(detections)

        for det in tracks:
            track_id = int(det["track_id"])
            class_id = int(det["class_id"])
            vehicle_class = VEHICLE_CLASSES[class_id]
            x1, y1, x2, y2 = [float(v) for v in det["box"]]
            cx, cy = det["center"]
            current_side = side_of_line((cx, cy), args.line)
            previous = previous_side.get(track_id)
            prev_center = previous_center.get(track_id)
            if previous is not None and prev_center is not None:
                direction = direction_from_sides(previous, current_side)
                crossed_segment = segments_intersect(prev_center, (cx, cy), *args.line)
                if direction and crossed_segment and track_id not in counted:
                    counted.add(track_id)
                    timestamp_s = frame_index / fps
                    events.append({
                        "timestamp_s": round(timestamp_s, 3),
                        "frame_index": frame_index,
                        "track_id": track_id,
                        "class": vehicle_class,
                        "direction": direction,
                        "confidence": round(float(det["confidence"]), 4),
                        "center_x": round(cx, 1),
                        "center_y": round(cy, 1),
                    })
                    counts["total"] += 1
                    counts_by_direction[direction] += 1
                    counts_by_class[vehicle_class] += 1
            previous_side[track_id] = current_side
            previous_center[track_id] = (cx, cy)

            color = (40, 220, 80)
            cv2.rectangle(frame, (int(x1), int(y1)), (int(x2), int(y2)), color, 1)
            draw_label(frame, f"#{track_id} {vehicle_class} {float(det['confidence']):.2f}", (int(x1), max(20, int(y1) - 2)), color)
            hist = track_history[track_id]
            hist.append((int(cx), int(cy)))
            if len(hist) > 1:
                pts = np.array(hist, dtype=np.int32).reshape((-1, 1, 2))
                cv2.polylines(frame, [pts], False, color, 1)

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

    fieldnames = ["timestamp_s", "frame_index", "track_id", "class", "direction", "confidence", "center_x", "center_y"]
    with events_path.open("w", newline="") as f:
        out = csv.DictWriter(f, fieldnames=fieldnames)
        out.writeheader()
        out.writerows(events)

    minute_counts = Counter((int(float(e["timestamp_s"]) // 60), e["direction"], e["class"]) for e in events)
    with minute_path.open("w", newline="") as f:
        out = csv.DictWriter(f, fieldnames=["minute", "start_s", "end_s", "direction", "class", "count"])
        out.writeheader()
        for (minute, direction, vehicle_class), count in sorted(minute_counts.items()):
            out.writerow({"minute": minute, "start_s": minute * 60, "end_s": (minute + 1) * 60, "direction": direction, "class": vehicle_class, "count": count})

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
        "tracker_note": "simple greedy IoU/center tracker; use for visual QA, not final audited counts",
    }
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2))
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
