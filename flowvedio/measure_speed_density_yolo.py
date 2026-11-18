#!/usr/bin/env python3
"""Extract vehicle trajectories, gate travel times, and ROI density from video.

The two speed gates and the direction-specific density ROIs come from
``create_20260709_speed_density_review.py``.  Vehicle box centres are used for
the user-confirmed speed gates, matching the existing counting convention;
bottom-centres are used for direction-specific road ROI assignment.  Absolute
speed and density are only written when a real-world gate distance is supplied;
otherwise the raw gate travel time and visible vehicles per lane remain
available without a distance assumption.
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from pathlib import Path

import cv2
import numpy as np
from ultralytics import YOLO

from create_20260709_speed_density_review import COLORS, GEOMETRY


VEHICLE_CLASSES = {
    2: "car",
    3: "motorcycle",
    5: "bus",
    7: "truck",
}

LANES_BY_PHASE = {
    "pre": {"south_to_north": 3, "north_to_south": 4},
    "post": {"south_to_north": 4, "north_to_south": 3},
}

EXPECTED_GATE_ORDER = {
    # In this outbound camera view, south-to-north traffic approaches the
    # camera (image y increases), while north-to-south traffic recedes.
    "south_to_north": ("far", "near"),
    "north_to_south": ("near", "far"),
}


def crossing_time(
    previous: tuple[float, float, float],
    current: tuple[float, float, float],
    line: tuple[int, int, int, int],
    expected_dy_sign: int,
) -> tuple[float, float] | None:
    """Return interpolated (time, x) when a track crosses a horizontal line."""

    x0, y0, t0 = previous
    x1, y1, t1 = current
    lx0, ly0, lx1, ly1 = line
    if ly0 != ly1 or y1 == y0:
        return None
    dy = y1 - y0
    if expected_dy_sign < 0 and dy >= 0:
        return None
    if expected_dy_sign > 0 and dy <= 0:
        return None
    if not (min(y0, y1) <= ly0 <= max(y0, y1)):
        return None
    fraction = (ly0 - y0) / dy
    if not 0.0 <= fraction <= 1.0:
        return None
    x_cross = x0 + fraction * (x1 - x0)
    if not min(lx0, lx1) <= x_cross <= max(lx0, lx1):
        return None
    t_cross = t0 + fraction * (t1 - t0)
    return t_cross, x_cross


def in_polygon(point: tuple[float, float], polygon: np.ndarray) -> bool:
    return cv2.pointPolygonTest(polygon, point, False) >= 0


def draw_qa_frame(
    frame: np.ndarray,
    geometry: dict[str, object],
    detections: list[dict[str, object]],
    timestamp_s: float,
    phase: str,
) -> np.ndarray:
    annotated = frame.copy()
    overlay = annotated.copy()
    rois = geometry["rois"]
    speed_gates = geometry["speed_gates"]
    assert isinstance(rois, dict)
    assert isinstance(speed_gates, dict)
    for direction, polygon in rois.items():
        cv2.fillPoly(overlay, [polygon], COLORS[direction])
    cv2.addWeighted(overlay, 0.14, annotated, 0.86, 0, annotated)
    for direction, polygon in rois.items():
        cv2.polylines(annotated, [polygon], True, COLORS[direction], 4, cv2.LINE_AA)
        for gate_name, (x0, y0, x1, y1) in speed_gates[direction].items():
            cv2.line(annotated, (x0, y0), (x1, y1), COLORS[direction], 4, cv2.LINE_AA)
            cv2.putText(
                annotated,
                f"{direction} {gate_name}",
                (x0 + 6, max(28, y0 - 8)),
                cv2.FONT_HERSHEY_SIMPLEX,
                0.6,
                COLORS[direction],
                2,
                cv2.LINE_AA,
            )
    for detection in detections:
        x0, y0, x1, y1 = [int(round(float(detection[key]))) for key in ("bbox_x1", "bbox_y1", "bbox_x2", "bbox_y2")]
        anchor_x = int(round(float(detection["anchor_x"])))
        anchor_y = int(round(float(detection["anchor_y"])))
        speed_anchor_x = int(round(float(detection["speed_anchor_x"])))
        speed_anchor_y = int(round(float(detection["speed_anchor_y"])))
        direction = str(detection["roi_direction"] or "")
        color = COLORS.get(direction, (70, 230, 70))
        cv2.rectangle(annotated, (x0, y0), (x1, y1), color, 2)
        cv2.circle(annotated, (anchor_x, anchor_y), 5, color, -1, cv2.LINE_AA)
        cv2.drawMarker(
            annotated,
            (speed_anchor_x, speed_anchor_y),
            (255, 255, 255),
            cv2.MARKER_CROSS,
            12,
            2,
            cv2.LINE_AA,
        )
        cv2.putText(
            annotated,
            f"#{detection['track_id']}",
            (x0, max(22, y0 - 4)),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.52,
            color,
            2,
            cv2.LINE_AA,
        )
    cv2.rectangle(annotated, (12, 12), (570, 54), (0, 0, 0), -1)
    cv2.putText(
        annotated,
        f"0709 outbound | {phase} | t={timestamp_s:.1f}s | speed=center, density=bottom",
        (22, 41),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.72,
        (255, 255, 255),
        2,
        cv2.LINE_AA,
    )
    return annotated


def main() -> None:
    root = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser()
    parser.add_argument("video", type=Path)
    parser.add_argument("--outdir", type=Path, required=True)
    parser.add_argument("--phase", choices=sorted(GEOMETRY))
    parser.add_argument("--geometry-json", type=Path, help="External geometry JSON for a confirmed camera/state.")
    parser.add_argument("--geometry-id", help="Label written to outputs when --geometry-json is supplied.")
    parser.add_argument("--lanes-json", help='Direction-to-lane mapping, for example {"south_to_north":3,"north_to_south":4}.')
    parser.add_argument("--start-s", type=float, required=True)
    parser.add_argument("--end-s", type=float, required=True)
    parser.add_argument("--distance-m", type=float, default=None)
    parser.add_argument("--model", default=str(root / "models" / "yolo11s.pt"))
    parser.add_argument("--imgsz", type=int, default=960)
    parser.add_argument("--conf", type=float, default=0.16)
    parser.add_argument("--stride", type=int, default=4)
    parser.add_argument("--tracker", default="bytetrack.yaml")
    parser.add_argument("--device", default="mps")
    parser.add_argument("--qa-every-s", type=float, default=60.0)
    parser.add_argument("--speed-anchor", choices=["center", "bottom"], default="center")
    parser.add_argument(
        "--no-speed-proxy",
        action="store_true",
        help="Keep all speed fields blank; retain only raw two-gate travel times.",
    )
    args = parser.parse_args()

    if args.geometry_json is None and args.phase is None:
        raise SystemExit("Supply either --phase or --geometry-json")
    if args.geometry_json is not None and not args.geometry_id:
        raise SystemExit("--geometry-id is required with --geometry-json")
    if args.end_s <= args.start_s:
        raise SystemExit("end-s must be greater than start-s")
    if args.distance_m is not None and args.distance_m <= 0:
        raise SystemExit("distance-m must be positive")

    video = args.video.expanduser().resolve()
    outdir = args.outdir.expanduser().resolve()
    outdir.mkdir(parents=True, exist_ok=True)
    qa_dir = outdir / "qa_frames"
    qa_dir.mkdir(parents=True, exist_ok=True)

    cap = cv2.VideoCapture(str(video))
    if not cap.isOpened():
        raise SystemExit(f"Could not open video: {video}")
    fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
    frames_total = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    start_frame = max(0, int(round(args.start_s * fps)))
    end_frame = min(frames_total, int(round(args.end_s * fps)))
    cap.set(cv2.CAP_PROP_POS_FRAMES, start_frame)

    phase_label = args.geometry_id or args.phase
    if args.geometry_json is not None:
        payload = json.loads(args.geometry_json.expanduser().resolve().read_text())
        geometry = {
            "speed_gates": {
                direction: {
                    gate: tuple(int(value) for value in xyxy)
                    for gate, xyxy in gates.items()
                }
                for direction, gates in payload["speed_gates"].items()
            },
            "rois": {
                direction: np.asarray(points, dtype=np.int32)
                for direction, points in payload["rois"].items()
            },
        }
        dy_signs = {
            direction: int(value)
            for direction, value in payload.get("dy_signs", {}).items()
        }
    else:
        geometry = GEOMETRY[args.phase]
        dy_signs = {"south_to_north": 1, "north_to_south": -1}
    speed_gates = geometry["speed_gates"]
    rois = geometry["rois"]
    lanes = json.loads(args.lanes_json) if args.lanes_json else LANES_BY_PHASE[args.phase]
    model = YOLO(args.model)

    trajectory_path = outdir / "trajectories.csv"
    density_path = outdir / "density_by_frame.csv"
    speed_path = outdir / "speed_gate_matches.csv"
    crossing_path = outdir / "gate_crossings.csv"
    summary_path = outdir / "measurement_summary.json"

    trajectory_fields = [
        "phase",
        "timestamp_s",
        "frame_index",
        "track_id",
        "class",
        "current_frame_class",
        "confidence",
        "bbox_x1",
        "bbox_y1",
        "bbox_x2",
        "bbox_y2",
        "center_x",
        "center_y",
        "anchor_x",
        "anchor_y",
        "speed_anchor_x",
        "speed_anchor_y",
        "roi_direction",
    ]
    density_fields = [
        "phase",
        "timestamp_s",
        "frame_index",
        "direction",
        "lanes",
        "visible_vehicle_count",
        "vehicles_per_lane_in_roi",
        "density_veh_per_km",
        "density_veh_per_lane_km",
    ]

    previous_anchor: dict[int, tuple[float, float, float]] = {}
    class_scores: dict[int, Counter[str]] = defaultdict(Counter)
    crossings: dict[tuple[int, str, str], dict[str, object]] = {}
    frame_density_rows: list[dict[str, object]] = []
    processed_frames = 0
    detection_rows = 0
    frame_index = start_frame
    next_qa_s = args.start_s

    with trajectory_path.open("w", newline="") as trajectory_file:
        trajectory_writer = csv.DictWriter(trajectory_file, fieldnames=trajectory_fields)
        trajectory_writer.writeheader()

        while frame_index < end_frame:
            ok, frame = cap.read()
            if not ok:
                break
            if frame_index % args.stride != 0:
                frame_index += 1
                continue
            timestamp_s = frame_index / fps
            result = model.track(
                frame,
                persist=True,
                tracker=args.tracker,
                classes=list(VEHICLE_CLASSES),
                conf=args.conf,
                imgsz=args.imgsz,
                device=args.device,
                verbose=False,
            )[0]

            detections: list[dict[str, object]] = []
            direction_counts = Counter()
            if result.boxes is not None and result.boxes.id is not None:
                boxes = result.boxes.xyxy.cpu().numpy()
                track_ids = result.boxes.id.int().cpu().tolist()
                class_ids = result.boxes.cls.int().cpu().tolist()
                confidences = result.boxes.conf.cpu().tolist()
                for box, track_id, class_id, confidence in zip(boxes, track_ids, class_ids, confidences):
                    if class_id not in VEHICLE_CLASSES:
                        continue
                    x0, y0, x1, y1 = [float(value) for value in box]
                    center_x = (x0 + x1) / 2.0
                    center_y = (y0 + y1) / 2.0
                    anchor_x = (x0 + x1) / 2.0
                    anchor_y = y1
                    if args.speed_anchor == "center":
                        speed_anchor_x, speed_anchor_y = center_x, center_y
                    else:
                        speed_anchor_x, speed_anchor_y = anchor_x, anchor_y
                    current_class = VEHICLE_CLASSES[class_id]
                    class_scores[track_id][current_class] += float(confidence)
                    stable_class = class_scores[track_id].most_common(1)[0][0]

                    roi_direction = ""
                    for direction, polygon in rois.items():
                        if in_polygon((anchor_x, anchor_y), polygon):
                            roi_direction = direction
                            direction_counts[direction] += 1
                            break

                    row = {
                        "phase": phase_label,
                        "timestamp_s": round(timestamp_s, 4),
                        "frame_index": frame_index,
                        "track_id": track_id,
                        "class": stable_class,
                        "current_frame_class": current_class,
                        "confidence": round(float(confidence), 4),
                        "bbox_x1": round(x0, 2),
                        "bbox_y1": round(y0, 2),
                        "bbox_x2": round(x1, 2),
                        "bbox_y2": round(y1, 2),
                        "center_x": round(center_x, 2),
                        "center_y": round(center_y, 2),
                        "anchor_x": round(anchor_x, 2),
                        "anchor_y": round(anchor_y, 2),
                        "speed_anchor_x": round(speed_anchor_x, 2),
                        "speed_anchor_y": round(speed_anchor_y, 2),
                        "roi_direction": roi_direction,
                    }
                    trajectory_writer.writerow(row)
                    detections.append(row)
                    detection_rows += 1

                    current_anchor = (speed_anchor_x, speed_anchor_y, timestamp_s)
                    previous = previous_anchor.get(track_id)
                    if previous is not None:
                        for direction, gates in speed_gates.items():
                            dy_sign = dy_signs.get(direction, 1 if direction == "south_to_north" else -1)
                            for gate_name, line in gates.items():
                                key = (track_id, direction, gate_name)
                                if key in crossings:
                                    continue
                                crossed = crossing_time(previous, current_anchor, line, dy_sign)
                                if crossed is None:
                                    continue
                                event_time, event_x = crossed
                                crossings[key] = {
                                    "phase": phase_label,
                                    "track_id": track_id,
                                    "direction": direction,
                                    "gate": gate_name,
                                    "timestamp_s": round(event_time, 4),
                                    "crossing_x": round(event_x, 2),
                                    "class": stable_class,
                                    "frame_index_after_crossing": frame_index,
                                }
                    previous_anchor[track_id] = current_anchor

            for direction in ("south_to_north", "north_to_south"):
                count = int(direction_counts[direction])
                lane_count = int(lanes[direction])
                density_total = ""
                density_per_lane = ""
                if args.distance_m is not None:
                    segment_km = args.distance_m / 1000.0
                    density_total = round(count / segment_km, 4)
                    density_per_lane = round(count / segment_km / lane_count, 4)
                frame_density_rows.append(
                    {
                        "phase": phase_label,
                        "timestamp_s": round(timestamp_s, 4),
                        "frame_index": frame_index,
                        "direction": direction,
                        "lanes": lane_count,
                        "visible_vehicle_count": count,
                        "vehicles_per_lane_in_roi": round(count / lane_count, 4),
                        "density_veh_per_km": density_total,
                        "density_veh_per_lane_km": density_per_lane,
                    }
                )

            if args.qa_every_s > 0 and timestamp_s + 1e-9 >= next_qa_s:
                qa_frame = draw_qa_frame(frame, geometry, detections, timestamp_s, str(phase_label))
                qa_path = qa_dir / f"{phase_label}_{int(round(timestamp_s)):05d}s.jpg"
                cv2.imwrite(str(qa_path), qa_frame, [cv2.IMWRITE_JPEG_QUALITY, 94])
                next_qa_s += args.qa_every_s

            processed_frames += 1
            if processed_frames % 250 == 0:
                print(
                    f"phase={phase_label} processed={processed_frames} "
                    f"t={timestamp_s:.1f}s detections={detection_rows} crossings={len(crossings)}",
                    flush=True,
                )
            frame_index += 1

    cap.release()

    with density_path.open("w", newline="") as density_file:
        writer = csv.DictWriter(density_file, fieldnames=density_fields)
        writer.writeheader()
        writer.writerows(frame_density_rows)

    crossing_rows = sorted(crossings.values(), key=lambda row: (float(row["timestamp_s"]), int(row["track_id"])))
    crossing_fields = [
        "phase",
        "track_id",
        "direction",
        "gate",
        "timestamp_s",
        "crossing_x",
        "class",
        "frame_index_after_crossing",
    ]
    with crossing_path.open("w", newline="") as crossing_file:
        writer = csv.DictWriter(crossing_file, fieldnames=crossing_fields)
        writer.writeheader()
        writer.writerows(crossing_rows)

    speed_rows: list[dict[str, object]] = []
    track_ids_with_crossings = sorted({int(row["track_id"]) for row in crossing_rows})
    for track_id in track_ids_with_crossings:
        for direction in speed_gates:
            dy_sign = dy_signs.get(direction, 1 if direction == "south_to_north" else -1)
            first_gate, second_gate = ("far", "near") if dy_sign > 0 else ("near", "far")
            first = crossings.get((track_id, direction, first_gate))
            second = crossings.get((track_id, direction, second_gate))
            if first is None or second is None:
                continue
            travel_time_s = float(second["timestamp_s"]) - float(first["timestamp_s"])
            if not 0.5 <= travel_time_s <= 180.0:
                continue
            speed_per_100m: float | str = ""
            if not args.no_speed_proxy:
                speed_per_100m = round(360.0 / travel_time_s, 4)
            speed_kmh = ""
            if args.distance_m is not None:
                speed_kmh = round(3.6 * args.distance_m / travel_time_s, 4)
            speed_rows.append(
                {
                    "phase": phase_label,
                    "track_id": track_id,
                    "direction": direction,
                    "class": second["class"],
                    "first_gate": first_gate,
                    "first_timestamp_s": first["timestamp_s"],
                    "second_gate": second_gate,
                    "second_timestamp_s": second["timestamp_s"],
                    "travel_time_s": round(travel_time_s, 4),
                    "speed_kmh_per_100m_assumed": speed_per_100m,
                    "distance_m": args.distance_m if args.distance_m is not None else "",
                    "speed_kmh": speed_kmh,
                }
            )
    speed_fields = [
        "phase",
        "track_id",
        "direction",
        "class",
        "first_gate",
        "first_timestamp_s",
        "second_gate",
        "second_timestamp_s",
        "travel_time_s",
        "speed_kmh_per_100m_assumed",
        "distance_m",
        "speed_kmh",
    ]
    with speed_path.open("w", newline="") as speed_file:
        writer = csv.DictWriter(speed_file, fieldnames=speed_fields)
        writer.writeheader()
        writer.writerows(speed_rows)

    summary = {
        "video": str(video),
        "phase": phase_label,
        "start_s": args.start_s,
        "end_s": args.end_s,
        "duration_s": args.end_s - args.start_s,
        "fps": fps,
        "frame_width": width,
        "frame_height": height,
        "stride": args.stride,
        "processed_frames": processed_frames,
        "detection_rows": detection_rows,
        "gate_crossings": len(crossing_rows),
        "matched_speed_tracks": len(speed_rows),
        "distance_m": args.distance_m,
        "speed_anchor": f"bounding_box_{args.speed_anchor}",
        "density_anchor": "bounding_box_bottom_center",
        "geometry": {
            "speed_gates": speed_gates,
            "rois": {key: value.tolist() for key, value in rois.items()},
            "lanes": lanes,
            "dy_signs": dy_signs,
        },
        "outputs": {
            "trajectories": str(trajectory_path),
            "density_by_frame": str(density_path),
            "gate_crossings": str(crossing_path),
            "speed_gate_matches": str(speed_path),
            "qa_frames": str(qa_dir),
        },
    }
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
