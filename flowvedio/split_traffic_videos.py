#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import math
from dataclasses import dataclass
from pathlib import Path

import cv2


VIDEO_EXTENSIONS = {".mov", ".mp4", ".m4v", ".avi"}


@dataclass(frozen=True)
class VideoInfo:
    fps: float
    frames: int
    width: int
    height: int
    duration_s: float


@dataclass(frozen=True)
class Segment:
    source: Path
    start_s: float
    end_s: float
    label: str

    @property
    def duration_s(self) -> float:
        return self.end_s - self.start_s


def safe_stem(path_or_label: str) -> str:
    return "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in path_or_label).strip("_") or "segment"


def parse_time(value: str) -> float:
    text = str(value).strip()
    if not text:
        raise ValueError("empty time value")
    if ":" not in text:
        return float(text)
    parts = [float(part) for part in text.split(":")]
    if len(parts) == 2:
        minutes, seconds = parts
        return minutes * 60 + seconds
    if len(parts) == 3:
        hours, minutes, seconds = parts
        return hours * 3600 + minutes * 60 + seconds
    raise ValueError(f"invalid time value: {value}")


def format_stamp(seconds: float) -> str:
    total = int(round(seconds))
    hours = total // 3600
    minutes = (total % 3600) // 60
    secs = total % 60
    return f"{hours:02d}{minutes:02d}{secs:02d}"


def scan_videos(input_path: Path) -> list[Path]:
    input_path = input_path.expanduser()
    if input_path.is_file():
        return [input_path]
    return sorted(
        path
        for path in input_path.rglob("*")
        if path.is_file() and path.suffix.lower() in VIDEO_EXTENSIONS
    )


def probe_video(video: Path) -> VideoInfo:
    cap = cv2.VideoCapture(str(video))
    if not cap.isOpened():
        raise RuntimeError(f"Could not open video: {video}")
    fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
    frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    cap.release()
    return VideoInfo(
        fps=fps,
        frames=frames,
        width=width,
        height=height,
        duration_s=frames / fps if fps else 0,
    )


def build_fixed_segments(video: Path, duration_s: float, chunk_seconds: float, min_seconds: float, label: str) -> list[Segment]:
    if chunk_seconds <= 0:
        raise ValueError("chunk_seconds must be positive")
    segments: list[Segment] = []
    start = 0.0
    while start < duration_s:
        end = min(duration_s, start + chunk_seconds)
        tail = duration_s - end
        if segments and 0 < tail < min_seconds:
            end = duration_s
        if end - start >= min_seconds or not segments:
            segments.append(Segment(video, start, end, label))
        start = end
    return segments


def find_video(token: str, videos: list[Path]) -> Path:
    wanted = str(token).strip()
    matches = [
        video
        for video in videos
        if wanted in {str(video), video.name, video.stem}
        or video.name == Path(wanted).name
        or video.stem == Path(wanted).stem
    ]
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise ValueError(f"Plan references unknown video: {token}")
    raise ValueError(f"Plan reference is ambiguous: {token}")


def read_plan(plan_path: Path, videos: list[Path]) -> list[Segment]:
    with plan_path.open(newline="") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        return []
    segments: list[Segment] = []
    for idx, row in enumerate(rows, start=1):
        keep = str(row.get("keep", "1")).strip().lower()
        if keep in {"0", "false", "no", "n", "skip"}:
            continue
        video_token = row.get("video") or row.get("source_video")
        if video_token:
            source = find_video(video_token, videos)
        elif len(videos) == 1:
            source = videos[0]
        else:
            raise ValueError("Plan rows need a video column when input contains multiple videos.")
        start = parse_time(row.get("start") or row.get("start_s") or "")
        end = parse_time(row.get("end") or row.get("end_s") or "")
        if end <= start:
            raise ValueError(f"Plan row {idx} has end <= start.")
        label = safe_stem(row.get("label") or f"segment_{idx:03d}")
        segments.append(Segment(source, start, end, label))
    return segments


def output_path_for(segment: Segment, source_index: int, output_dir: Path, extension: str) -> Path:
    source_stem = safe_stem(segment.source.stem)
    label = safe_stem(segment.label)
    name = f"{source_index:03d}_{format_stamp(segment.start_s)}_{format_stamp(segment.end_s)}_{label}{extension}"
    return output_dir / source_stem / name


def write_segment(segment: Segment, output_path: Path, info: VideoInfo, codec: str, dry_run: bool) -> int:
    if dry_run:
        return 0
    output_path.parent.mkdir(parents=True, exist_ok=True)
    cap = cv2.VideoCapture(str(segment.source))
    if not cap.isOpened():
        raise RuntimeError(f"Could not open video: {segment.source}")
    start_frame = max(0, int(math.floor(segment.start_s * info.fps)))
    end_frame = min(info.frames, int(math.ceil(segment.end_s * info.fps)))
    cap.set(cv2.CAP_PROP_POS_FRAMES, start_frame)
    writer = cv2.VideoWriter(
        str(output_path),
        cv2.VideoWriter_fourcc(*codec),
        info.fps,
        (info.width, info.height),
    )
    if not writer.isOpened():
        cap.release()
        raise RuntimeError(f"Could not create output video: {output_path}")
    written = 0
    frame_index = start_frame
    while frame_index < end_frame:
        ok, frame = cap.read()
        if not ok:
            break
        writer.write(frame)
        written += 1
        frame_index += 1
        if written % 900 == 0:
            print(f"  wrote {written} frames -> {output_path.name}", flush=True)
    cap.release()
    writer.release()
    return written


def write_manifest(rows: list[dict[str, object]], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest = output_dir / "segments_manifest.csv"
    fieldnames = [
        "source_video",
        "segment_video",
        "label",
        "start_s",
        "end_s",
        "duration_s",
        "fps",
        "width",
        "height",
        "frames_written",
        "status",
    ]
    with manifest.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Split field traffic videos into analysis clips.")
    parser.add_argument("--input", type=Path, default=Path("flowvedio/input_videos"), help="Video file or folder.")
    parser.add_argument("--output", type=Path, default=Path("flowvedio/split_outputs"), help="Output folder.")
    parser.add_argument("--chunk-seconds", type=float, default=300, help="Fixed split length when --plan is omitted.")
    parser.add_argument("--min-seconds", type=float, default=30, help="Minimum trailing segment length.")
    parser.add_argument("--label", default="fixed", help="Label for fixed-length segments.")
    parser.add_argument("--plan", type=Path, help="CSV with columns video,start,end,label,keep.")
    parser.add_argument("--codec", default="mp4v", help="OpenCV fourcc codec, default mp4v.")
    parser.add_argument("--extension", default=".mp4", help="Output extension, default .mp4.")
    parser.add_argument("--limit", type=int, help="Process only the first N input videos.")
    parser.add_argument("--dry-run", action="store_true", help="Write only the manifest, not video files.")
    args = parser.parse_args()

    videos = scan_videos(args.input)
    if args.limit:
        videos = videos[: args.limit]
    if not videos:
        raise SystemExit(f"No videos found under {args.input}")

    info_by_video = {video: probe_video(video) for video in videos}
    if args.plan:
        segments = read_plan(args.plan, videos)
    else:
        segments = []
        for video in videos:
            info = info_by_video[video]
            segments.extend(build_fixed_segments(video, info.duration_s, args.chunk_seconds, args.min_seconds, args.label))

    rows: list[dict[str, object]] = []
    source_counter: dict[Path, int] = {}
    for segment in segments:
        info = info_by_video[segment.source]
        start = max(0.0, segment.start_s)
        end = min(info.duration_s, segment.end_s)
        if end <= start:
            print(f"Skipping empty segment: {segment.source} {segment.start_s}-{segment.end_s}", flush=True)
            continue
        clipped = Segment(segment.source, start, end, segment.label)
        source_counter[clipped.source] = source_counter.get(clipped.source, 0) + 1
        output_path = output_path_for(clipped, source_counter[clipped.source], args.output, args.extension)
        print(f"{'DRY ' if args.dry_run else ''}split {clipped.source.name}: {clipped.start_s:.1f}-{clipped.end_s:.1f}s -> {output_path}", flush=True)
        frames_written = write_segment(clipped, output_path, info, args.codec, args.dry_run)
        rows.append(
            {
                "source_video": str(clipped.source),
                "segment_video": str(output_path),
                "label": clipped.label,
                "start_s": round(clipped.start_s, 3),
                "end_s": round(clipped.end_s, 3),
                "duration_s": round(clipped.duration_s, 3),
                "fps": round(info.fps, 6),
                "width": info.width,
                "height": info.height,
                "frames_written": frames_written,
                "status": "dry_run" if args.dry_run else "done",
            }
        )
    write_manifest(rows, args.output)
    print(f"Wrote manifest: {args.output / 'segments_manifest.csv'}")


if __name__ == "__main__":
    main()
