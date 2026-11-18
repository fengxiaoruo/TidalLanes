#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from traffic_counter.runner import run_batch


def parse_line(value: str) -> tuple[int, int, int, int]:
    parts = [int(v.strip()) for v in value.split(",")]
    if len(parts) != 4:
        raise argparse.ArgumentTypeError("line must be x1,y1,x2,y2")
    return parts[0], parts[1], parts[2], parts[3]


def parse_line_ratio(value: str) -> tuple[float, float, float, float]:
    parts = [float(v.strip()) for v in value.split(",")]
    if len(parts) != 4:
        raise argparse.ArgumentTypeError("line-ratio must be x1_ratio,y1_ratio,x2_ratio,y2_ratio")
    if any(v < 0 or v > 1 for v in parts):
        raise argparse.ArgumentTypeError("line-ratio values must be between 0 and 1")
    return parts[0], parts[1], parts[2], parts[3]


def main() -> None:
    parser = argparse.ArgumentParser(description="Batch vehicle counting for TidalLanes field videos.")
    parser.add_argument("--input", type=Path, default=Path("flowvedio/input_videos"))
    parser.add_argument("--output", type=Path, default=Path("flowvedio/batch_outputs"))
    parser.add_argument("--profile", default="auto", help="auto, horizontal_mainline, horizontal_outbound_right, horizontal_inbound_left, or vertical_rotated_dense")
    parser.add_argument("--config", type=Path, help="Optional YAML config overriding or adding profiles.")
    parser.add_argument("--device", default=None, help="Use mps on Apple Silicon, cpu otherwise; omit for auto.")
    parser.add_argument("--line", type=parse_line, help="Override the profile count line with absolute pixels: x1,y1,x2,y2.")
    parser.add_argument("--line-ratio", type=parse_line_ratio, help="Override the profile count line with ratios: x1,y1,x2,y2 in [0,1].")
    parser.add_argument("--dry-run", action="store_true", help="Only probe videos and write line previews.")
    parser.add_argument("--force", action="store_true", help="Recreate intermediate rotated videos.")
    parser.add_argument("--limit", type=int, help="Process only the first N videos.")
    args = parser.parse_args()
    if args.line and args.line_ratio:
        parser.error("Use either --line or --line-ratio, not both.")

    rows = run_batch(
        input_dir=args.input,
        output_dir=args.output,
        profile_name=args.profile,
        config_path=args.config,
        device=args.device,
        line_override=args.line,
        line_ratio_override=args.line_ratio,
        dry_run=args.dry_run,
        force=args.force,
        limit=args.limit,
    )
    print(f"Processed {len(rows)} video(s).")
    print(f"Summary: {args.output / 'summary_all_videos.csv'}")


if __name__ == "__main__":
    main()
