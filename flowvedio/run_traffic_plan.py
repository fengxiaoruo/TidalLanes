#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
from pathlib import Path

from split_traffic_videos import find_video, format_stamp, parse_time, safe_stem, scan_videos


def read_line_plan(path: Path) -> dict[str, list[dict[str, object]]]:
    with path.open(newline="") as f:
        rows = list(csv.DictReader(f))
    by_label: dict[str, list[dict[str, object]]] = {}
    for row in rows:
        label = (row.get("label") or "").strip()
        line_name = (row.get("line_name") or "").strip()
        line = (row.get("line") or "").strip()
        if not label or not line_name or not line:
            raise ValueError(f"Invalid line-plan row: {row}")
        lanes_text = (row.get("lanes") or "").strip()
        lanes = int(lanes_text) if lanes_text else None
        item: dict[str, object] = dict(row)
        item["line_name"] = line_name
        item["line"] = line
        item["lanes"] = lanes
        by_label.setdefault(label, []).append(item)
    return by_label


def read_segment_plan(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def summary_path_for(output_dir: Path, video: Path) -> Path:
    return output_dir / f"{video.stem}_traffic_summary.json"


def compact_json(value: object) -> str:
    return json.dumps(value or {}, ensure_ascii=False, sort_keys=True)


def write_summary(rows: list[dict[str, object]], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / "summary_plan.csv"
    fields = [
        "source_video",
        "segment_id",
        "label",
        "start_s",
        "end_s",
        "duration_s",
        "status",
        "total_count",
        "count_per_hour",
        "counts_by_line",
        "counts_by_line_class",
        "counts_by_line_direction_class",
        "counts_by_class",
        "events_csv",
        "counts_by_minute_csv",
        "annotated_video",
        "output_dir",
    ]
    with csv_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            duration = float(row.get("duration_s") or 0)
            total = int(row.get("total_count") or 0)
            writer.writerow(
                {
                    "source_video": row.get("source_video", ""),
                    "segment_id": row.get("segment_id", ""),
                    "label": row.get("label", ""),
                    "start_s": row.get("start_s", ""),
                    "end_s": row.get("end_s", ""),
                    "duration_s": round(duration, 3) if duration else "",
                    "status": row.get("status", ""),
                    "total_count": total if row.get("status") == "done" else "",
                    "count_per_hour": round(total / duration * 3600, 1) if duration and row.get("status") == "done" else "",
                    "counts_by_line": compact_json(row.get("counts_by_line")),
                    "counts_by_line_class": compact_json(row.get("counts_by_line_class")),
                    "counts_by_line_direction_class": compact_json(row.get("counts_by_line_direction_class")),
                    "counts_by_class": compact_json(row.get("counts_by_class")),
                    "events_csv": row.get("events_csv", ""),
                    "counts_by_minute_csv": row.get("counts_by_minute_csv", ""),
                    "annotated_video": row.get("annotated_video", ""),
                    "output_dir": row.get("output_dir", ""),
                }
            )
    (output_dir / "summary_plan.json").write_text(json.dumps(rows, ensure_ascii=False, indent=2))


def count_line_class(summary: dict[str, object], line_name: str, vehicle_class: str) -> int:
    data = summary.get("counts_by_line_class") or {}
    if not isinstance(data, dict):
        return 0
    return int(data.get(f"{line_name}|{vehicle_class}") or 0)


def write_line_panel(rows: list[dict[str, object]], output_dir: Path, line_plan: dict[str, list[dict[str, object]]]) -> None:
    panel_path = output_dir / "panel_by_line.csv"
    fields = [
        "source_video",
        "segment_id",
        "label",
        "regime",
        "analysis_sample",
        "start_s",
        "end_s",
        "duration_s",
        "line_name",
        "direction",
        "lanes",
        "treated_delta_lanes",
        "count",
        "vehicles_per_hour",
        "vehicles_per_lane_hour",
        "car_count",
        "bus_count",
        "truck_count",
        "motorcycle_count",
        "output_dir",
    ]
    with panel_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            if row.get("status") != "done":
                continue
            label = str(row.get("label") or "")
            duration = float(row.get("duration_s") or 0)
            counts_by_line = row.get("counts_by_line") or {}
            if not isinstance(counts_by_line, dict):
                counts_by_line = {}
            for line_info in line_plan.get(label, []):
                line_name = str(line_info.get("line_name") or "")
                lanes = line_info.get("lanes")
                lanes_float = float(lanes) if lanes else 0.0
                count = int(counts_by_line.get(line_name) or 0)
                vph = count / duration * 3600 if duration else 0.0
                writer.writerow(
                    {
                        "source_video": row.get("source_video", ""),
                        "segment_id": row.get("segment_id", ""),
                        "label": label,
                        "regime": row.get("plan_regime", label),
                        "analysis_sample": row.get("plan_analysis_sample", ""),
                        "start_s": row.get("start_s", ""),
                        "end_s": row.get("end_s", ""),
                        "duration_s": round(duration, 3) if duration else "",
                        "line_name": line_name,
                        "direction": line_info.get("direction") or line_name,
                        "lanes": lanes or "",
                        "treated_delta_lanes": line_info.get("treated_delta_lanes", ""),
                        "count": count,
                        "vehicles_per_hour": round(vph, 2) if duration else "",
                        "vehicles_per_lane_hour": round(vph / lanes_float, 2) if duration and lanes_float else "",
                        "car_count": count_line_class(row, line_name, "car"),
                        "bus_count": count_line_class(row, line_name, "bus"),
                        "truck_count": count_line_class(row, line_name, "truck"),
                        "motorcycle_count": count_line_class(row, line_name, "motorcycle"),
                        "output_dir": row.get("output_dir", ""),
                    }
                )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run multi-line traffic counting from segment and line plans.")
    parser.add_argument("--input", type=Path, required=True, help="Folder containing source videos.")
    parser.add_argument("--segments", type=Path, required=True, help="CSV with video,start,end,label,keep.")
    parser.add_argument("--lines", type=Path, required=True, help="CSV with label,line_name,line,lanes.")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--model", default=str(Path(__file__).resolve().with_name("models") / "yolo11s.pt"))
    parser.add_argument("--imgsz", type=int, default=1280)
    parser.add_argument("--conf", type=float, default=0.16)
    parser.add_argument("--stride", type=int, default=2)
    parser.add_argument("--tracker", default="bytetrack.yaml")
    parser.add_argument("--device", default=None)
    parser.add_argument("--video-contains", action="append", help="Only run rows whose video name contains this text. Can be repeated.")
    parser.add_argument("--limit", type=int, help="Run only the first N planned rows after filtering.")
    parser.add_argument("--save-video", action="store_true", help="Write annotated mp4 for each segment.")
    parser.add_argument("--force", action="store_true", help="Re-run segments even if their summary already exists.")
    args = parser.parse_args()

    videos = scan_videos(args.input)
    if not videos:
        raise SystemExit(f"No videos found under {args.input}")
    line_plan = read_line_plan(args.lines)
    segment_rows = read_segment_plan(args.segments)

    rows: list[dict[str, object]] = []
    selected_rows: list[dict[str, str]] = []
    filters = args.video_contains or []
    for row in segment_rows:
        keep = str(row.get("keep", "1")).strip().lower()
        if keep in {"0", "false", "no", "n", "skip"}:
            continue
        video_name = row.get("video") or row.get("source_video") or ""
        if filters and not any(token in video_name for token in filters):
            continue
        selected_rows.append(row)
    if args.limit:
        selected_rows = selected_rows[: args.limit]

    for idx, row in enumerate(selected_rows, start=1):
        source = find_video(row.get("video") or row.get("source_video") or "", videos)
        start_s = parse_time(row.get("start") or row.get("start_s") or "")
        end_s = parse_time(row.get("end") or row.get("end_s") or "")
        label = safe_stem(row.get("label") or f"segment_{idx:03d}")
        if label not in line_plan:
            raise ValueError(f"No lines configured for label: {label}")
        segment_id = f"{idx:03d}_{format_stamp(start_s)}_{format_stamp(end_s)}_{label}"
        item_dir = args.output / safe_stem(source.stem) / segment_id
        item_dir.mkdir(parents=True, exist_ok=True)
        summary_path = summary_path_for(item_dir, source)
        if summary_path.exists() and not args.force:
            summary = json.loads(summary_path.read_text())
            summary.update(
                {
                    "source_video": str(source),
                    "segment_id": segment_id,
                    "label": label,
                    "status": "done",
                    "output_dir": str(item_dir),
                    **{f"plan_{key}": value for key, value in row.items() if key not in {"video", "source_video", "start", "start_s", "end", "end_s", "label", "keep"}},
                }
            )
            rows.append(summary)
            write_summary(rows, args.output)
            write_line_panel(rows, args.output, line_plan)
            print(f"skip existing {segment_id}", flush=True)
            continue

        cmd = [
            sys.executable,
            str(Path(__file__).resolve().with_name("traffic_count_yolo.py")),
            str(source),
            "--outdir",
            str(item_dir),
            "--model",
            args.model,
            "--imgsz",
            str(args.imgsz),
            "--conf",
            str(args.conf),
            "--stride",
            str(args.stride),
            "--tracker",
            args.tracker,
            "--start-s",
            str(start_s),
            "--end-s",
            str(end_s),
        ]
        for line_info in line_plan[label]:
            cmd.extend(["--lines", f"{line_info['line_name']}:{line_info['line']}"])
        if args.device:
            cmd.extend(["--device", args.device])
        if not args.save_video:
            cmd.append("--no-video")

        print(f"run {segment_id}: {source.name} {start_s:.1f}-{end_s:.1f}s label={label}", flush=True)
        subprocess.run(cmd, check=True)
        summary = json.loads(summary_path.read_text())
        summary.update(
            {
                "source_video": str(source),
                "segment_id": segment_id,
                "label": label,
                "status": "done",
                "output_dir": str(item_dir),
                **{f"plan_{key}": value for key, value in row.items() if key not in {"video", "source_video", "start", "start_s", "end", "end_s", "label", "keep"}},
            }
        )
        rows.append(summary)
        write_summary(rows, args.output)
        write_line_panel(rows, args.output, line_plan)
    write_summary(rows, args.output)
    write_line_panel(rows, args.output, line_plan)
    print(f"Wrote summary: {args.output / 'summary_plan.csv'}")


if __name__ == "__main__":
    main()
