#!/usr/bin/env python3
"""Count vehicles crossing a line in a traffic video with YOLO tracking."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict, deque
from pathlib import Path

import cv2
import numpy as np
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


def parse_named_line(value: str) -> tuple[str, tuple[tuple[int, int], tuple[int, int]]]:
    if ":" not in value:
        raise argparse.ArgumentTypeError("named line must be name:x1,y1,x2,y2")
    name, line_text = value.split(":", 1)
    name = name.strip()
    if not name:
        raise argparse.ArgumentTypeError("line name cannot be empty")
    return name, parse_line(line_text)


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
    if abs(o1) <= eps and on_segment(a, c, b):
        return True
    if abs(o2) <= eps and on_segment(a, d, b):
        return True
    if abs(o3) <= eps and on_segment(c, a, d):
        return True
    if abs(o4) <= eps and on_segment(c, b, d):
        return True
    return False


def draw_label(frame, text: str, origin: tuple[int, int], color: tuple[int, int, int]) -> None:
    font = cv2.FONT_HERSHEY_SIMPLEX
    scale = 0.62
    thickness = 2
    (w, h), baseline = cv2.getTextSize(text, font, scale, thickness)
    x, y = origin
    cv2.rectangle(frame, (x, y - h - 8), (x + w + 8, y + baseline + 4), (0, 0, 0), -1)
    cv2.putText(frame, text, (x + 4, y - 4), font, scale, color, thickness, cv2.LINE_AA)


def main() -> None:
    parser = argparse.ArgumentParser()
    default_model = Path(__file__).resolve().with_name("models") / "yolo11s.pt"
    parser.add_argument("video", type=Path)
    parser.add_argument("--outdir", type=Path)
    parser.add_argument("--model", default=str(default_model))
    parser.add_argument("--imgsz", type=int, default=1280)
    parser.add_argument("--conf", type=float, default=0.18)
    parser.add_argument("--stride", type=int, default=2)
    parser.add_argument("--line", type=parse_line, default=parse_line("720,700,920,380"))
    parser.add_argument("--line-name", default="line", help="Name used when a single --line is supplied.")
    parser.add_argument("--lines", action="append", type=parse_named_line, help="Count multiple lines in one pass: name:x1,y1,x2,y2. Can be repeated.")
    parser.add_argument("--tracker", default="bytetrack.yaml")
    parser.add_argument("--device", default=None)
    parser.add_argument("--start-s", type=float, default=0.0, help="Start time in seconds.")
    parser.add_argument("--end-s", type=float, default=None, help="End time in seconds.")
    parser.add_argument("--no-video", action="store_true", help="Skip annotated mp4 output for faster batch runs.")
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
    start_frame = max(0, int(round(args.start_s * fps)))
    end_frame = frames_total if args.end_s is None else min(frames_total, int(round(args.end_s * fps)))
    if end_frame <= start_frame:
        raise SystemExit(f"Empty time window: start_s={args.start_s}, end_s={args.end_s}")
    cap.set(cv2.CAP_PROP_POS_FRAMES, start_frame)
    line_items = args.lines if args.lines else [(args.line_name, args.line)]
    line_lookup = {name: line for name, line in line_items}

    stem = video.stem
    events_path = outdir / f"{stem}_vehicle_crossings.csv"
    summary_path = outdir / f"{stem}_traffic_summary.json"
    minute_path = outdir / f"{stem}_vehicle_counts_by_minute.csv"
    video_path = outdir / f"{stem}_annotated_count.mp4"

    writer = None
    if not args.no_video:
        writer = cv2.VideoWriter(
            str(video_path),
            cv2.VideoWriter_fourcc(*"mp4v"),
            processed_fps,
            (width, height),
        )
        if not writer.isOpened():
            raise SystemExit(f"Could not create annotated video: {video_path}")

    model = YOLO(args.model)
    previous_side: dict[int, float] = {}
    previous_side_by_line: dict[tuple[int, str], float] = {}
    previous_center: dict[int, tuple[float, float]] = {}
    counted: set[tuple[int, str]] = set()
    track_history: dict[int, deque[tuple[int, int]]] = defaultdict(lambda: deque(maxlen=24))
    track_class_scores: dict[int, Counter[str]] = defaultdict(Counter)
    events: list[dict[str, object]] = []
    counts = Counter()
    counts_by_direction = Counter()
    counts_by_class = Counter()
    counts_by_line = Counter()
    counts_by_line_direction = Counter()
    counts_by_line_class = Counter()
    counts_by_direction_class = Counter()
    counts_by_line_direction_class = Counter()

    frame_index = start_frame
    processed = 0
    line_color = (0, 255, 255)
    line_colors = [(0, 255, 255), (255, 170, 0), (0, 180, 255), (255, 0, 180)]
    box_color = (40, 220, 80)

    while True:
        if frame_index >= end_frame:
            break
        ok, frame = cap.read()
        if not ok:
            break
        if frame_index % args.stride != 0:
            frame_index += 1
            continue

        result = model.track(
            frame,
            persist=True,
            tracker=args.tracker,
            classes=list(VEHICLE_CLASSES.keys()),
            conf=args.conf,
            imgsz=args.imgsz,
            device=args.device,
            verbose=False,
        )[0]

        if result.boxes is not None and result.boxes.id is not None:
            boxes = result.boxes.xyxy.cpu().numpy()
            track_ids = result.boxes.id.int().cpu().tolist()
            class_ids = result.boxes.cls.int().cpu().tolist()
            confs = result.boxes.conf.cpu().tolist()

            for box, track_id, class_id, conf in zip(boxes, track_ids, class_ids, confs):
                if class_id not in VEHICLE_CLASSES:
                    continue
                x1, y1, x2, y2 = box
                cx = float((x1 + x2) / 2)
                cy = float((y1 + y2) / 2)
                current_class = VEHICLE_CLASSES[class_id]
                track_class_scores[track_id][current_class] += float(conf)
                vehicle_class = track_class_scores[track_id].most_common(1)[0][0]
                prev_center = previous_center.get(track_id)
                for line_name, line in line_items:
                    current_side = side_of_line((cx, cy), line)
                    previous = previous_side_by_line.get((track_id, line_name))
                    if previous is not None and prev_center is not None:
                        direction = direction_from_sides(previous, current_side)
                        crossed_segment = segments_intersect(prev_center, (cx, cy), *line)
                        if direction and crossed_segment and (track_id, line_name) not in counted:
                            counted.add((track_id, line_name))
                            timestamp_s = frame_index / fps
                            event = {
                                "timestamp_s": round(timestamp_s, 3),
                                "frame_index": frame_index,
                                "track_id": track_id,
                                "line_name": line_name,
                                "class": vehicle_class,
                                "current_frame_class": current_class,
                                "direction": direction,
                                "confidence": round(float(conf), 4),
                                "center_x": round(cx, 1),
                                "center_y": round(cy, 1),
                            }
                            events.append(event)
                            counts["total"] += 1
                            counts_by_direction[direction] += 1
                            counts_by_class[vehicle_class] += 1
                            counts_by_line[line_name] += 1
                            counts_by_line_direction[(line_name, direction)] += 1
                            counts_by_line_class[(line_name, vehicle_class)] += 1
                            counts_by_direction_class[(direction, vehicle_class)] += 1
                            counts_by_line_direction_class[(line_name, direction, vehicle_class)] += 1
                    previous_side_by_line[(track_id, line_name)] = current_side
                previous_side[track_id] = side_of_line((cx, cy), args.line)
                previous_center[track_id] = (cx, cy)

                if writer is not None:
                    p1 = (int(x1), int(y1))
                    p2 = (int(x2), int(y2))
                    cv2.rectangle(frame, p1, p2, box_color, 2)
                    label = f"#{track_id} {current_class} {conf:.2f}"
                    draw_label(frame, label, (int(x1), max(24, int(y1) - 4)), box_color)
                    history = track_history[track_id]
                    history.append((int(cx), int(cy)))
                    if len(history) > 1:
                        pts = np.array(history, dtype=np.int32).reshape((-1, 1, 2))
                        cv2.polylines(frame, [pts], False, box_color, 1)

        if writer is not None:
            for i, (line_name, line) in enumerate(line_items):
                color = line_colors[i % len(line_colors)]
                cv2.line(frame, line[0], line[1], color, 4)
                draw_label(frame, f"{line_name} {counts_by_line[line_name]}", (line[0][0], max(24, line[0][1] - 10)), color)
            draw_label(frame, f"total {counts['total']}", (24, 42), (255, 255, 255))
            draw_label(frame, f"neg_to_pos {counts_by_direction['neg_to_pos']}", (24, 82), (255, 255, 255))
            draw_label(frame, f"pos_to_neg {counts_by_direction['pos_to_neg']}", (24, 122), (255, 255, 255))
            writer.write(frame)

        processed += 1
        if processed % 100 == 0:
            print(f"processed {processed} frames; count={counts['total']}", flush=True)
        frame_index += 1

    cap.release()
    if writer is not None:
        writer.release()

    with events_path.open("w", newline="") as f:
        fieldnames = [
            "timestamp_s",
            "frame_index",
            "track_id",
            "line_name",
            "class",
            "current_frame_class",
            "direction",
            "confidence",
            "center_x",
            "center_y",
        ]
        writer_csv = csv.DictWriter(f, fieldnames=fieldnames)
        writer_csv.writeheader()
        writer_csv.writerows(events)

    minute_rows = []
    minute_counts = Counter()
    for event in events:
        minute = int(float(event["timestamp_s"]) // 60)
        key = (minute, event["line_name"], event["direction"], event["class"])
        minute_counts[key] += 1
    for (minute, line_name, direction, vehicle_class), count in sorted(minute_counts.items()):
        minute_rows.append(
            {
                "minute": minute,
                "start_s": minute * 60,
                "end_s": (minute + 1) * 60,
                "line_name": line_name,
                "direction": direction,
                "class": vehicle_class,
                "count": count,
            }
        )
    with minute_path.open("w", newline="") as f:
        fieldnames = ["minute", "start_s", "end_s", "line_name", "direction", "class", "count"]
        writer_csv = csv.DictWriter(f, fieldnames=fieldnames)
        writer_csv.writeheader()
        writer_csv.writerows(minute_rows)

    duration_s = (end_frame - start_frame) / fps if fps else None
    lines_xyxy = {name: [*line[0], *line[1]] for name, line in line_lookup.items()}
    summary = {
        "video": str(video),
        "model": args.model,
        "frame_width": width,
        "frame_height": height,
        "fps": fps,
        "frame_count": frames_total,
        "duration_s": duration_s,
        "start_s": start_frame / fps if fps else args.start_s,
        "end_s": end_frame / fps if fps else args.end_s,
        "processed_stride": args.stride,
        "processed_frames": processed,
        "line_xyxy": [*line_items[0][1][0], *line_items[0][1][1]],
        "lines_xyxy": lines_xyxy,
        "total_count": counts["total"],
        "counts_by_direction": dict(counts_by_direction),
        "counts_by_class": dict(counts_by_class),
        "counts_by_line": dict(counts_by_line),
        "counts_by_line_direction": {
            f"{line_name}|{direction}": count
            for (line_name, direction), count in counts_by_line_direction.items()
        },
        "counts_by_line_class": {
            f"{line_name}|{vehicle_class}": count
            for (line_name, vehicle_class), count in counts_by_line_class.items()
        },
        "counts_by_direction_class": {
            f"{direction}|{vehicle_class}": count
            for (direction, vehicle_class), count in counts_by_direction_class.items()
        },
        "counts_by_line_direction_class": {
            f"{line_name}|{direction}|{vehicle_class}": count
            for (line_name, direction, vehicle_class), count in counts_by_line_direction_class.items()
        },
        "events_csv": str(events_path),
        "counts_by_minute_csv": str(minute_path),
        "annotated_video": str(video_path) if writer is not None else "",
    }
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2))
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
