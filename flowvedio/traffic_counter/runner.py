from __future__ import annotations

import csv
import json
import subprocess
import sys
from dataclasses import asdict
from pathlib import Path

import cv2

from .config import Profile, load_profiles, resolve_model


VIDEO_EXTENSIONS = {".mov", ".mp4", ".m4v", ".avi"}
IGNORED_DIR_NAMES = {"traffic_count_outputs", "batch_outputs", "work", "__pycache__", "models"}
PACKAGE_ROOT = Path(__file__).resolve().parents[1]


def safe_stem(path: Path) -> str:
    return "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in path.stem)


def scan_videos(input_dir: Path) -> list[Path]:
    videos = []
    for path in sorted(input_dir.rglob("*")):
        if path.is_file() and path.suffix.lower() in VIDEO_EXTENSIONS:
            if any(part in IGNORED_DIR_NAMES for part in path.parts):
                continue
            videos.append(path)
    return videos


def probe_video(video: Path) -> dict[str, float | int]:
    cap = cv2.VideoCapture(str(video))
    if not cap.isOpened():
        raise RuntimeError(f"Could not open video: {video}")
    fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
    frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    cap.release()
    return {
        "fps": fps,
        "frames": frames,
        "width": width,
        "height": height,
        "duration_s": frames / fps if fps else 0,
    }


def choose_profile(video_info: dict[str, float | int], requested: str, profiles: dict[str, Profile], video: Path | None = None) -> Profile:
    if requested != "auto":
        if requested not in profiles:
            raise KeyError(f"Unknown profile: {requested}")
        return profiles[requested]
    width = int(video_info["width"])
    height = int(video_info["height"])
    if height > width:
        return profiles["vertical_rotated_dense"]
    name = video.stem.lower() if video else ""
    if any(token in name for token in ("出城", "outbound")):
        return profiles.get("horizontal_outbound_right", profiles["horizontal_mainline"])
    if any(token in name for token in ("进城", "入城", "inbound")):
        return profiles.get("horizontal_inbound_left", profiles["horizontal_mainline"])
    return profiles["horizontal_mainline"]


def rotate_video(src: Path, dst: Path, rotate: str, force: bool = False) -> Path:
    if rotate == "none":
        return src
    if dst.exists() and not force:
        return dst
    dst.parent.mkdir(parents=True, exist_ok=True)
    cap = cv2.VideoCapture(str(src))
    if not cap.isOpened():
        raise RuntimeError(f"Could not open video: {src}")
    fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    if rotate in {"cw90", "ccw90"}:
        out_size = (height, width)
    elif rotate == "180":
        out_size = (width, height)
    else:
        raise ValueError(f"Unknown rotate mode: {rotate}")
    writer = cv2.VideoWriter(str(dst), cv2.VideoWriter_fourcc(*"mp4v"), fps, out_size)
    if not writer.isOpened():
        cap.release()
        raise RuntimeError(f"Could not create rotated video: {dst}")
    while True:
        ok, frame = cap.read()
        if not ok:
            break
        if rotate == "cw90":
            frame = cv2.rotate(frame, cv2.ROTATE_90_CLOCKWISE)
        elif rotate == "ccw90":
            frame = cv2.rotate(frame, cv2.ROTATE_90_COUNTERCLOCKWISE)
        elif rotate == "180":
            frame = cv2.rotate(frame, cv2.ROTATE_180)
        writer.write(frame)
    cap.release()
    writer.release()
    return dst


def line_from_ratio(width: int, height: int, ratio: tuple[float, float, float, float]) -> tuple[int, int, int, int]:
    x1, y1, x2, y2 = ratio
    return (
        int(round(x1 * width)),
        int(round(y1 * height)),
        int(round(x2 * width)),
        int(round(y2 * height)),
    )


def read_preview_frame(video: Path):
    cap = cv2.VideoCapture(str(video))
    if not cap.isOpened():
        raise RuntimeError(f"Could not open video: {video}")
    frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    cap.set(cv2.CAP_PROP_POS_FRAMES, max(0, frames // 2))
    ok, frame = cap.read()
    cap.release()
    if not ok:
        raise RuntimeError(f"Could not read preview frame: {video}")
    return frame


def write_coordinate_preview(video: Path, output_path: Path, step: int = 100) -> None:
    frame = read_preview_frame(video)
    height, width = frame.shape[:2]
    for x in range(0, width, step):
        cv2.line(frame, (x, 0), (x, height), (180, 180, 180), 1)
        cv2.putText(frame, str(x), (x + 4, 22), cv2.FONT_HERSHEY_SIMPLEX, 0.55, (0, 255, 255), 2, cv2.LINE_AA)
    for y in range(0, height, step):
        cv2.line(frame, (0, y), (width, y), (180, 180, 180), 1)
        cv2.putText(frame, str(y), (6, y + 20 if y + 20 < height else height - 8), cv2.FONT_HERSHEY_SIMPLEX, 0.55, (0, 255, 255), 2, cv2.LINE_AA)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    cv2.imwrite(str(output_path), frame)


def write_line_preview(video: Path, line: tuple[int, int, int, int], output_path: Path) -> None:
    frame = read_preview_frame(video)
    x1, y1, x2, y2 = line
    cv2.line(frame, (x1, y1), (x2, y2), (0, 255, 255), 4)
    cv2.putText(frame, "count line", (x1, max(24, y1 - 10)), cv2.FONT_HERSHEY_SIMPLEX, 0.8, (0, 255, 255), 2, cv2.LINE_AA)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    cv2.imwrite(str(output_path), frame)


def command_for(profile: Profile, video: Path, output_dir: Path, line: tuple[int, int, int, int], device: str | None) -> list[str]:
    model = resolve_model(profile.model)
    line_arg = ",".join(str(v) for v in line)
    if profile.method == "bytetrack":
        script = PACKAGE_ROOT / "traffic_count_yolo.py"
        cmd = [
            sys.executable,
            str(script),
            str(video),
            "--outdir",
            str(output_dir),
            "--model",
            model,
            "--imgsz",
            str(profile.imgsz),
            "--conf",
            str(profile.conf),
            "--stride",
            str(profile.stride),
            "--line",
            line_arg,
        ]
        if profile.tracker:
            cmd.extend(["--tracker", profile.tracker])
    elif profile.method == "simpletrack":
        script = PACKAGE_ROOT / "traffic_count_yolo_simpletrack.py"
        cmd = [
            sys.executable,
            str(script),
            str(video),
            "--outdir",
            str(output_dir),
            "--model",
            model,
            "--imgsz",
            str(profile.imgsz),
            "--conf",
            str(profile.conf),
            "--stride",
            str(profile.stride),
            "--line",
            line_arg,
            "--max-distance",
            str(profile.max_distance),
            "--min-iou",
            str(profile.min_iou),
            "--max-missed",
            str(profile.max_missed),
        ]
    else:
        raise ValueError(f"Unknown method: {profile.method}")
    if device:
        cmd.extend(["--device", device])
    return cmd


def load_summary(output_dir: Path, counted_video: Path) -> dict[str, object]:
    path = output_dir / f"{counted_video.stem}_traffic_summary.json"
    return json.loads(path.read_text()) if path.exists() else {}


def run_one(
    video: Path,
    output_root: Path,
    profile: Profile,
    device: str | None,
    line_override: tuple[int, int, int, int] | None,
    line_ratio_override: tuple[float, float, float, float] | None,
    dry_run: bool,
    force: bool,
) -> dict[str, object]:
    original_info = probe_video(video)
    item_dir = output_root / safe_stem(video)
    work_dir = item_dir / "work"
    count_input = video
    if profile.rotate != "none":
        count_input = rotate_video(video, work_dir / f"{safe_stem(video)}_{profile.rotate}.mp4", profile.rotate, force=force)
    count_info = probe_video(count_input)
    if line_override:
        line = line_override
        line_source = "manual_pixels"
    elif line_ratio_override:
        line = line_from_ratio(int(count_info["width"]), int(count_info["height"]), line_ratio_override)
        line_source = "manual_ratio"
    else:
        line = line_from_ratio(int(count_info["width"]), int(count_info["height"]), profile.line_ratio)
        line_source = "profile"
    write_coordinate_preview(count_input, item_dir / "coordinate_preview.jpg")
    write_line_preview(count_input, line, item_dir / "line_preview.jpg")

    run_meta: dict[str, object] = {
        "source_video": str(video),
        "count_video": str(count_input),
        "profile": profile.name,
        "profile_note": profile.note,
        "original_info": original_info,
        "count_info": count_info,
        "line_xyxy": list(line),
        "line_source": line_source,
        "output_dir": str(item_dir),
        "line_preview": str(item_dir / "line_preview.jpg"),
        "coordinate_preview": str(item_dir / "coordinate_preview.jpg"),
    }
    (item_dir / "run_meta.json").write_text(json.dumps(run_meta, ensure_ascii=False, indent=2))
    if dry_run:
        run_meta["status"] = "dry_run"
        return run_meta

    cmd = command_for(profile, count_input, item_dir, line, device)
    subprocess.run(cmd, cwd=str(PACKAGE_ROOT.parents[0]), check=True)
    summary = load_summary(item_dir, count_input)
    run_meta.update(summary)
    run_meta["status"] = "done"
    return run_meta


def write_batch_summary(rows: list[dict[str, object]], output_root: Path) -> None:
    csv_path = output_root / "summary_all_videos.csv"
    fieldnames = [
        "source_video",
        "profile",
        "status",
        "total_count",
        "duration_s",
        "count_per_hour",
        "counts_by_direction",
        "counts_by_class",
        "counts_by_direction_class",
        "line_source",
        "line_xyxy",
        "events_csv",
        "annotated_video",
        "line_preview",
        "coordinate_preview",
        "output_dir",
    ]
    with csv_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            duration = float(row.get("duration_s") or (row.get("count_info") or {}).get("duration_s") or 0)
            total = int(row.get("total_count") or 0)
            writer.writerow(
                {
                    "source_video": row.get("source_video", ""),
                    "profile": row.get("profile", ""),
                    "status": row.get("status", ""),
                    "total_count": total if row.get("status") == "done" else "",
                    "duration_s": round(duration, 3) if duration else "",
                    "count_per_hour": round(total / duration * 3600, 1) if duration and row.get("status") == "done" else "",
                    "counts_by_direction": json.dumps(row.get("counts_by_direction", {}), ensure_ascii=False),
                    "counts_by_class": json.dumps(row.get("counts_by_class", {}), ensure_ascii=False),
                    "counts_by_direction_class": json.dumps(row.get("counts_by_direction_class", {}), ensure_ascii=False),
                    "line_source": row.get("line_source", ""),
                    "line_xyxy": json.dumps(row.get("line_xyxy", []), ensure_ascii=False),
                    "events_csv": row.get("events_csv", ""),
                    "annotated_video": row.get("annotated_video", ""),
                    "line_preview": row.get("line_preview", ""),
                    "coordinate_preview": row.get("coordinate_preview", ""),
                    "output_dir": row.get("output_dir", ""),
                }
            )
    (output_root / "summary_all_videos.json").write_text(json.dumps(rows, ensure_ascii=False, indent=2))


def run_batch(
    input_dir: Path,
    output_dir: Path,
    profile_name: str,
    config_path: Path | None,
    device: str | None,
    line_override: tuple[int, int, int, int] | None,
    line_ratio_override: tuple[float, float, float, float] | None,
    dry_run: bool,
    force: bool,
    limit: int | None,
) -> list[dict[str, object]]:
    profiles = load_profiles(config_path)
    videos = scan_videos(input_dir)
    if limit:
        videos = videos[:limit]
    output_dir.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, object]] = []
    for video in videos:
        info = probe_video(video)
        profile = choose_profile(info, profile_name, profiles, video)
        rows.append(
            run_one(
                video,
                output_dir,
                profile,
                device=device,
                line_override=line_override,
                line_ratio_override=line_ratio_override,
                dry_run=dry_run,
                force=force,
            )
        )
    write_batch_summary(rows, output_dir)
    return rows
