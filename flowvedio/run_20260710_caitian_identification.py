#!/usr/bin/env python3
"""Run the confirmed 2026-07-10 Caitian Road video count plan.

The script reads the two time manifests and the segment-level state plan,
assigns the confirmed line plan, writes full-resolution line previews, and
optionally runs the existing YOLO + ByteTrack counter. Anomaly and transition
segments are kept separate and passed to the counter with their source-video
time windows. Relative event times are converted into Asia/Shanghai wall-clock
timestamps so the two camera directions can be aligned on overlapping periods.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import subprocess
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import cv2


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT = SCRIPT_DIR / "20260710彩田路"
DEFAULT_OUTPUT = SCRIPT_DIR / "batch_outputs" / "20260710_caitian_identification_final"
DEFAULT_LINE_PLAN = SCRIPT_DIR / "line_plan_20260710_confirmed.csv"
DEFAULT_SEGMENT_LINE_PLAN = SCRIPT_DIR / "segment_line_plan_20260710_final.csv"
DEFAULT_STATE_PLAN = SCRIPT_DIR / "batch_outputs" / "20260710_line_review" / "lane_state_plan_20260710_v3.csv"
DEFAULT_MODEL = SCRIPT_DIR / "models" / "yolo11s.pt"
INBOUND_NIGHT_START = "19:00"
VIDEO_EXTENSIONS = {".mov", ".mp4", ".m4v", ".avi"}


@dataclass(frozen=True)
class LineSpec:
    name: str
    xyxy: tuple[int, int, int, int]
    geo_direction: str
    notes: str


@dataclass(frozen=True)
class Clip:
    path: Path
    group: str
    mode: str
    start_local: datetime
    end_local: datetime
    duration_s: float
    lines: tuple[LineSpec, ...]
    source_start_local: datetime
    source_end_local: datetime
    source_duration_s: float
    segment_id: str
    state: str
    segment_start_s: float
    segment_end_s: float
    counting_status: str
    notes: str


def safe_stem(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in value).strip("_") or "video"


def parse_local(value: str) -> datetime:
    value = value.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            pass
    raise ValueError(f"Unsupported local datetime: {value}")


def parse_hhmm(value: str) -> tuple[int, int]:
    match = re.fullmatch(r"(\d{1,2}):(\d{2})", value.strip())
    if not match:
        raise ValueError(f"Expected HH:MM, got {value}")
    return int(match.group(1)), int(match.group(2))


def read_line_plan(path: Path) -> dict[tuple[str, str], tuple[LineSpec, ...]]:
    grouped: dict[tuple[str, str], list[LineSpec]] = {}
    with path.open(newline="") as handle:
        for row in csv.DictReader(handle):
            key = (row["group"].strip(), row["mode"].strip())
            grouped.setdefault(key, []).append(
                LineSpec(
                    name=row["line_name"].strip(),
                    xyxy=tuple(int(row[key].strip()) for key in ("x1", "y1", "x2", "y2")),
                    geo_direction=row.get("geo_direction", "").strip(),
                    notes=row.get("notes", "").strip(),
                )
            )
    return {key: tuple(value) for key, value in grouped.items()}


def parse_point(value: str) -> tuple[int, int]:
    parts = [int(v.strip()) for v in value.split(",")]
    if len(parts) != 2:
        raise ValueError(f"Expected x,y point, got {value}")
    return parts[0], parts[1]


def read_segment_line_plan(path: Path) -> dict[tuple[str, str, float, float], tuple[LineSpec, ...]]:
    grouped: dict[tuple[str, str, float, float], tuple[LineSpec, ...]] = {}
    with path.open(newline="") as handle:
        for row in csv.DictReader(handle):
            source_file = row["source_file"].strip()
            state = row["state"].strip()
            start_s = round(float(row["segment_start_s"]), 3)
            end_s = round(float(row["segment_end_s"]), 3)
            true_direction = row["true_direction"].strip()
            left_endpoint = parse_point(row["left_endpoint"])
            split_point = parse_point(row["split_point"])
            right_endpoint = parse_point(row["right_endpoint"])
            left_geo = "south_to_north" if true_direction == "outbound" else "north_to_south"
            right_geo = "north_to_south" if true_direction == "outbound" else "south_to_north"
            notes = row.get("notes", "").strip()
            grouped[(source_file, state, start_s, end_s)] = (
                LineSpec(f"{left_geo}_left", (*left_endpoint, *split_point), left_geo, notes),
                LineSpec(f"{right_geo}_right", (*split_point, *right_endpoint), right_geo, notes),
            )
    return grouped


def read_state_plan(path: Path) -> dict[str, tuple[dict[str, str], ...]]:
    grouped: dict[str, list[dict[str, str]]] = {}
    with path.open(newline="") as handle:
        for row in csv.DictReader(handle):
            grouped.setdefault(row["source_file"].strip(), []).append(row)
    for rows in grouped.values():
        rows.sort(key=lambda row: float(row["segment_start_s"]))
    return {key: tuple(value) for key, value in grouped.items()}


def read_manifest(
    path: Path,
    group: str,
    input_dir: Path,
    line_plan: dict[tuple[str, str], tuple[LineSpec, ...]],
    segment_line_plan: dict[tuple[str, str, float, float], tuple[LineSpec, ...]],
    state_plan: dict[str, tuple[dict[str, str], ...]],
    night_start: tuple[int, int],
) -> list[Clip]:
    clips: list[Clip] = []
    with path.open(newline="") as handle:
        for row in csv.DictReader(handle):
            source = input_dir / row["file"]
            if not source.exists():
                raise FileNotFoundError(f"Manifest file is missing: {source}")
            if group == "outbound":
                start_local = parse_local(row["recording_start_local"])
            else:
                start_local = parse_local(row["internal_creation_time_local"])
            end_local = parse_local(row["recording_end_local"])
            duration_s = float(row["duration_s"])
            segments = state_plan.get(row["file"].strip())
            if not segments:
                segments = (
                    {
                        "state": "normal",
                        "segment_start_s": "0",
                        "segment_end_s": str(duration_s),
                        "counting_status": "count",
                        "notes": "No state-plan row; treated as one full clip.",
                    },
                )
            for segment_index, segment in enumerate(segments, start=1):
                segment_group = segment.get("true_direction", group).strip() or group
                mode = "all" if segment_group == "outbound" else ("night" if (start_local.hour, start_local.minute) >= night_start else "day")
                segment_start_s = float(segment["segment_start_s"])
                segment_end_s = float(segment["segment_end_s"])
                if segment_end_s <= segment_start_s:
                    raise ValueError(f"Empty state-plan segment for {source}: {segment}")
                lines = segment_line_plan.get(
                    (
                        row["file"].strip(),
                        segment.get("state", "").strip(),
                        round(segment_start_s, 3),
                        round(segment_end_s, 3),
                    )
                )
                if lines is None:
                    lines = line_plan[(segment_group, mode)]
                segment_start_local = start_local + timedelta(seconds=segment_start_s)
                segment_end_local = start_local + timedelta(seconds=segment_end_s)
                segment_id = f"{safe_stem(source.stem)}_seg{segment_index:02d}_{segment_start_s:.3f}_{segment_end_s:.3f}".replace(".", "p")
                clips.append(
                    Clip(
                        path=source,
                        group=segment_group,
                        mode=mode,
                        start_local=segment_start_local,
                        end_local=segment_end_local,
                        duration_s=segment_end_s - segment_start_s,
                        lines=lines,
                        source_start_local=start_local,
                        source_end_local=end_local,
                        source_duration_s=duration_s,
                        segment_id=segment_id,
                        state=segment.get("state", "normal").strip(),
                        segment_start_s=segment_start_s,
                        segment_end_s=segment_end_s,
                        counting_status=segment.get("counting_status", "count").strip(),
                        notes=segment.get("notes", "").strip(),
                    )
                )
    return sorted(clips, key=lambda clip: clip.start_local)


def load_clips(
    input_dir: Path,
    line_plan: dict[tuple[str, str], tuple[LineSpec, ...]],
    segment_line_plan: dict[tuple[str, str, float, float], tuple[LineSpec, ...]],
    state_plan: dict[str, tuple[dict[str, str], ...]],
    night_start: tuple[int, int],
) -> list[Clip]:
    outbound_manifest = input_dir / "recording_manifest.csv"
    inbound_manifest = input_dir / "inbound_recording_manifest.csv"
    missing = [str(path) for path in (outbound_manifest, inbound_manifest) if not path.exists()]
    if missing:
        raise FileNotFoundError("Missing time manifest(s): " + ", ".join(missing))
    clips = read_manifest(outbound_manifest, "outbound", input_dir, line_plan, segment_line_plan, state_plan, night_start)
    clips.extend(read_manifest(inbound_manifest, "inbound", input_dir, line_plan, segment_line_plan, state_plan, night_start))
    return sorted(clips, key=lambda clip: clip.start_local)


def line_json(lines: tuple[LineSpec, ...]) -> str:
    return json.dumps(
        [
            {"name": line.name, "xyxy": list(line.xyxy), "geo_direction": line.geo_direction}
            for line in lines
        ],
        ensure_ascii=False,
        sort_keys=True,
    )


def read_midframe(video: Path, start_s: float = 0.0, end_s: float | None = None):
    cap = cv2.VideoCapture(str(video))
    if not cap.isOpened():
        raise RuntimeError(f"Could not open video: {video}")
    fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
    frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    start_frame = max(0, int(round(start_s * fps)))
    end_frame = frames if end_s is None else min(frames, int(round(end_s * fps)))
    cap.set(cv2.CAP_PROP_POS_FRAMES, max(start_frame, (start_frame + end_frame) // 2))
    ok, frame = cap.read()
    cap.release()
    if not ok:
        raise RuntimeError(f"Could not read midpoint frame: {video}")
    return frame


def write_preview(clip: Clip, output_path: Path) -> None:
    frame = read_midframe(clip.path, clip.segment_start_s, clip.segment_end_s)
    height, width = frame.shape[:2]
    for x in range(0, width, 100):
        cv2.line(frame, (x, 0), (x, height), (120, 120, 120), 1, cv2.LINE_AA)
        if x < width - 20:
            cv2.putText(frame, str(x), (x + 4, 24), cv2.FONT_HERSHEY_SIMPLEX, 0.62, (0, 255, 255), 2, cv2.LINE_AA)
    for y in range(0, height, 100):
        cv2.line(frame, (0, y), (width, y), (120, 120, 120), 1, cv2.LINE_AA)
        cv2.putText(frame, str(y), (5, min(height - 8, y + 24)), cv2.FONT_HERSHEY_SIMPLEX, 0.62, (0, 255, 255), 2, cv2.LINE_AA)
    cv2.rectangle(frame, (0, 34), (width, 92), (0, 0, 0), -1)
    cv2.putText(
        frame,
        f"{clip.path.name} | {clip.group}_{clip.mode} | {clip.state} | {clip.start_local:%H:%M:%S}",
        (12, 74),
        cv2.FONT_HERSHEY_SIMPLEX,
        1.05,
        (255, 255, 255),
        2,
        cv2.LINE_AA,
    )
    colors = [(0, 255, 255), (255, 170, 0), (0, 180, 255), (255, 0, 180)]
    for idx, line in enumerate(clip.lines):
        x1, y1, x2, y2 = line.xyxy
        color = colors[idx % len(colors)]
        cv2.line(frame, (x1, y1), (x2, y2), color, 6, cv2.LINE_AA)
        label = f"{line.name}: ({x1},{y1})-({x2},{y2})"
        box_x1 = max(4, x1 + 4)
        box_y1 = max(96, y1 - 44)
        box_x2 = min(width - 4, box_x1 + 420)
        cv2.rectangle(frame, (box_x1, box_y1), (box_x2, box_y1 + 38), (0, 0, 0), -1)
        cv2.putText(frame, label, (box_x1 + 8, box_y1 + 27), cv2.FONT_HERSHEY_SIMPLEX, 0.72, color, 2, cv2.LINE_AA)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    cv2.imwrite(str(output_path), frame, [cv2.IMWRITE_JPEG_QUALITY, 92])


def write_run_plan(clips: list[Clip], output_root: Path) -> None:
    fields = [
        "group",
        "mode",
        "source_video",
        "segment_id",
        "state",
        "counting_status",
        "notes",
        "source_start_local",
        "source_end_local",
        "segment_start_s",
        "segment_end_s",
        "start_local",
        "end_local",
        "duration_s",
        "lines",
        "output_dir",
    ]
    with (output_root / "run_plan.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for clip in clips:
            item_dir = output_root / clip.group / clip.mode / clip.segment_id
            writer.writerow(
                {
                    "group": clip.group,
                    "mode": clip.mode,
                    "source_video": str(clip.path),
                    "segment_id": clip.segment_id,
                    "state": clip.state,
                    "counting_status": clip.counting_status,
                    "notes": clip.notes,
                    "source_start_local": clip.source_start_local.isoformat(sep=" ", timespec="seconds"),
                    "source_end_local": clip.source_end_local.isoformat(sep=" ", timespec="milliseconds"),
                    "segment_start_s": round(clip.segment_start_s, 3),
                    "segment_end_s": round(clip.segment_end_s, 3),
                    "start_local": clip.start_local.isoformat(sep=" ", timespec="seconds"),
                    "end_local": clip.end_local.isoformat(sep=" ", timespec="milliseconds"),
                    "duration_s": round(clip.duration_s, 3),
                    "lines": line_json(clip.lines),
                    "output_dir": str(item_dir),
                }
            )


def run_counter(clip: Clip, item_dir: Path, args: argparse.Namespace) -> tuple[str, str]:
    stem = clip.path.stem
    summary_path = item_dir / f"{stem}_traffic_summary.json"
    if summary_path.exists() and not args.force:
        return "skipped_existing", ""
    command = [
        sys.executable,
        str(SCRIPT_DIR / "traffic_count_yolo.py"),
        str(clip.path),
        "--outdir",
        str(item_dir),
        "--model",
        str(args.model),
        "--imgsz",
        str(args.imgsz),
        "--conf",
        str(args.conf),
        "--stride",
        str(args.stride),
        "--tracker",
        args.tracker,
        "--start-s",
        str(clip.segment_start_s),
        "--end-s",
        str(clip.segment_end_s),
    ]
    for line in clip.lines:
        command.extend(["--lines", f"{line.name}:{','.join(str(v) for v in line.xyxy)}"])
    if args.device:
        command.extend(["--device", args.device])
    if not args.save_video:
        command.append("--no-video")
    subprocess.run(command, cwd=str(SCRIPT_DIR), check=True)
    return "done", " ".join(command)


def load_summary(clip: Clip, item_dir: Path) -> dict[str, object]:
    path = item_dir / f"{clip.path.stem}_traffic_summary.json"
    return json.loads(path.read_text()) if path.exists() else {}


def write_absolute_events(results: list[dict[str, object]], output_root: Path) -> None:
    events_fields = [
        "recorded_at_local",
        "source_video",
        "segment_id",
        "group",
        "mode",
        "state",
        "counting_status",
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
    event_rows: list[dict[str, object]] = []
    minute_counts: Counter[tuple[str, str, str, str, str, str, str]] = Counter()
    for result in results:
        if result.get("status") not in {"done", "skipped_existing"}:
            continue
        clip: Clip = result["clip"]  # type: ignore[assignment]
        item_dir = Path(str(result["output_dir"]))
        events_path = item_dir / f"{clip.path.stem}_vehicle_crossings.csv"
        if not events_path.exists():
            continue
        with events_path.open(newline="") as handle:
            for row in csv.DictReader(handle):
                timestamp_s = float(row.get("timestamp_s") or 0)
                recorded_at = clip.source_start_local + timedelta(seconds=timestamp_s)
                event = {
                    "recorded_at_local": recorded_at.isoformat(sep=" ", timespec="milliseconds"),
                    "source_video": clip.path.name,
                    "segment_id": clip.segment_id,
                    "group": clip.group,
                    "mode": clip.mode,
                    "state": clip.state,
                    "counting_status": clip.counting_status,
                    **{
                        key: row.get(key, "")
                        for key in events_fields
                        if key not in {"recorded_at_local", "source_video", "segment_id", "group", "mode", "state", "counting_status"}
                    },
                }
                event_rows.append(event)
                minute = recorded_at.replace(second=0, microsecond=0).isoformat(sep=" ", timespec="minutes")
                minute_counts[
                    (
                        minute,
                        clip.group,
                        clip.mode,
                        clip.state,
                        clip.counting_status,
                        row.get("line_name", ""),
                        row.get("direction", ""),
                    )
                ] += 1
    event_rows.sort(key=lambda row: (row["recorded_at_local"], row["source_video"], row["line_name"]))
    with (output_root / "events_absolute.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=events_fields)
        writer.writeheader()
        writer.writerows(event_rows)
    with (output_root / "counts_by_wall_minute.csv").open("w", newline="") as handle:
        fields = ["wall_minute_local", "group", "mode", "state", "counting_status", "line_name", "direction", "count"]
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for key, count in sorted(minute_counts.items()):
            writer.writerow(dict(zip(fields, key + (count,))))


def write_summary(results: list[dict[str, object]], output_root: Path) -> None:
    fields = [
        "group",
        "mode",
        "source_video",
        "segment_id",
        "state",
        "counting_status",
        "notes",
        "source_start_local",
        "source_end_local",
        "segment_start_s",
        "segment_end_s",
        "start_local",
        "end_local",
        "duration_s",
        "status",
        "total_count",
        "counts_by_line",
        "counts_by_direction",
        "line_xyxy",
        "output_dir",
        "error",
    ]
    with (output_root / "summary_by_video.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for result in results:
            clip: Clip = result["clip"]  # type: ignore[assignment]
            summary = result.get("summary") or {}
            writer.writerow(
                {
                    "group": clip.group,
                    "mode": clip.mode,
                    "source_video": clip.path.name,
                    "segment_id": clip.segment_id,
                    "state": clip.state,
                    "counting_status": clip.counting_status,
                    "notes": clip.notes,
                    "source_start_local": clip.source_start_local.isoformat(sep=" ", timespec="seconds"),
                    "source_end_local": clip.source_end_local.isoformat(sep=" ", timespec="milliseconds"),
                    "segment_start_s": round(clip.segment_start_s, 3),
                    "segment_end_s": round(clip.segment_end_s, 3),
                    "start_local": clip.start_local.isoformat(sep=" ", timespec="seconds"),
                    "end_local": clip.end_local.isoformat(sep=" ", timespec="milliseconds"),
                    "duration_s": round(clip.duration_s, 3),
                    "status": result.get("status", ""),
                    "total_count": summary.get("total_count", ""),
                    "counts_by_line": json.dumps(summary.get("counts_by_line", {}), ensure_ascii=False, sort_keys=True),
                    "counts_by_direction": json.dumps(summary.get("counts_by_direction", {}), ensure_ascii=False, sort_keys=True),
                    "line_xyxy": json.dumps(summary.get("lines_xyxy", {}), ensure_ascii=False, sort_keys=True),
                    "output_dir": result.get("output_dir", ""),
                    "error": result.get("error", ""),
                }
            )


def main() -> None:
    parser = argparse.ArgumentParser(description="Process 2026-07-10 Caitian Road videos with confirmed lines.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--line-plan", type=Path, default=DEFAULT_LINE_PLAN)
    parser.add_argument("--segment-line-plan", type=Path, default=DEFAULT_SEGMENT_LINE_PLAN, help="Final per-segment three-point line plan CSV.")
    parser.add_argument("--state-plan", type=Path, default=DEFAULT_STATE_PLAN, help="Segment-level state/anomaly plan CSV.")
    parser.add_argument("--model", type=Path, default=DEFAULT_MODEL)
    parser.add_argument("--night-start", default=INBOUND_NIGHT_START, help="Inbound day/night split, HH:MM local time.")
    parser.add_argument("--device", default=None, help="Example: mps or cpu.")
    parser.add_argument("--imgsz", type=int, default=1280)
    parser.add_argument("--conf", type=float, default=0.16)
    parser.add_argument("--stride", type=int, default=2)
    parser.add_argument("--tracker", default="bytetrack.yaml")
    parser.add_argument("--save-video", action="store_true", help="Save annotated mp4 outputs; off by default.")
    parser.add_argument("--dry-run", action="store_true", help="Write plan and full-size line previews without running YOLO.")
    parser.add_argument("--force", action="store_true", help="Re-run clips with existing summaries.")
    parser.add_argument("--continue-on-error", action="store_true", help="Record a failed clip and continue.")
    args = parser.parse_args()

    args.input = args.input.expanduser().resolve()
    args.output = args.output.expanduser().resolve()
    args.line_plan = args.line_plan.expanduser().resolve()
    args.segment_line_plan = args.segment_line_plan.expanduser().resolve()
    args.state_plan = args.state_plan.expanduser().resolve()
    args.model = args.model.expanduser().resolve()
    args.output.mkdir(parents=True, exist_ok=True)
    line_plan = read_line_plan(args.line_plan)
    segment_line_plan = read_segment_line_plan(args.segment_line_plan)
    state_plan = read_state_plan(args.state_plan)
    clips = load_clips(args.input, line_plan, segment_line_plan, state_plan, parse_hhmm(args.night_start))
    write_run_plan(clips, args.output)

    results: list[dict[str, object]] = []
    for clip in clips:
        item_dir = args.output / clip.group / clip.mode / clip.segment_id
        preview_path = item_dir / "line_preview_fullsize.jpg"
        write_preview(clip, preview_path)
        result: dict[str, object] = {"clip": clip, "output_dir": str(item_dir), "status": "dry_run" if args.dry_run else "pending"}
        if not args.dry_run:
            try:
                status, command = run_counter(clip, item_dir, args)
                result["status"] = status
                result["command"] = command
                result["summary"] = load_summary(clip, item_dir)
            except subprocess.CalledProcessError as exc:
                result["status"] = "error"
                result["error"] = f"counter exited with code {exc.returncode}"
                if not args.continue_on_error:
                    results.append(result)
                    write_summary(results, args.output)
                    raise
        results.append(result)
        print(f"{result['status']}: {clip.group}/{clip.mode}/{clip.path.name}", flush=True)

    write_summary(results, args.output)
    if not args.dry_run:
        write_absolute_events(results, args.output)
    print(f"Wrote plan/output summary under {args.output}")


if __name__ == "__main__":
    main()
