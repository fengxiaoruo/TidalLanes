"""
Compatibility wrapper for figure generation.

Prefer using:
- src/stages/stage10_generate_figures.py
- src/stages/run_full_pipeline.py
"""

import argparse
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def parse_args():
    parser = argparse.ArgumentParser(description="Generate all diagnostics figures")
    parser.add_argument("--version-id", required=True, help="Version identifier under outputs/.")
    parser.add_argument("--output-dir", default="outputs", help="Base output directory.")
    return parser.parse_args()


def main():
    from src.stages.stage10_generate_figures import run as run_stage10

    args = parse_args()
    run_stage10(None, args.version_id, args.output_dir, "all", "all")
    print(f"[generate_all_figures] completed for version={args.version_id}")


if __name__ == "__main__":
    main()
