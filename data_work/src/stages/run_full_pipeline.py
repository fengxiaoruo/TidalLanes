"""
Run the full stage-based pipeline for a versioned run.
"""

import argparse
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


STAGE_ORDER = [
    "stage01",
    "stage02",
    "stage03",
    "stage03b",
    "stage04",
    "stage05",
    "stage06",
    "stage07",
    "stage08",
    "stage09",
    "stage10",
]


def parse_args():
    parser = argparse.ArgumentParser(description="Run the full stage-based pipeline")
    parser.add_argument("--config", default=None, help="Optional config file path.")
    parser.add_argument("--version-id", required=True, help="Version identifier for outputs.")
    parser.add_argument("--output-dir", default="outputs", help="Base output directory for versioned results.")
    parser.add_argument(
        "--from-stage",
        default="stage01",
        choices=STAGE_ORDER,
        help="First stage to run.",
    )
    parser.add_argument(
        "--to-stage",
        default="stage10",
        choices=STAGE_ORDER,
        help="Last stage to run.",
    )
    parser.add_argument(
        "--grid-type",
        default="all",
        choices=["all", "square", "hex", "voronoi"],
        help="Grid system for stages that support it.",
    )
    parser.add_argument(
        "--skip-figures",
        action="store_true",
        help="Skip stage10 figure generation even if the stage range includes it.",
    )
    return parser.parse_args()


def selected_stages(from_stage: str, to_stage: str) -> list[str]:
    start = STAGE_ORDER.index(from_stage)
    end = STAGE_ORDER.index(to_stage)
    if start > end:
        raise ValueError("--from-stage must not come after --to-stage")
    return STAGE_ORDER[start : end + 1]


def run_stage(stage_name: str, config_path: str | None, version_id: str, output_dir: str, grid_type: str):
    if stage_name == "stage01":
        from src.stages.stage01_build_centerline import run as run_stage01

        run_stage01(config_path, version_id, output_dir)
    elif stage_name == "stage02":
        from src.stages.stage02_match_raw_to_centerline import run as run_stage02

        run_stage02(config_path, version_id, output_dir)
    elif stage_name == "stage03":
        from src.stages.stage03_attach_speed import run as run_stage03

        run_stage03(config_path, version_id, output_dir)
    elif stage_name == "stage03b":
        from src.stages.stage03b_centerline_asymmetry import run as run_stage03b

        run_stage03b(config_path, version_id, output_dir)
    elif stage_name == "stage04":
        from src.stages.stage04_estimate_lanes import run as run_stage04

        run_stage04(config_path, version_id, output_dir)
    elif stage_name == "stage05":
        from src.stages.stage05_build_grids import run as run_stage05

        run_stage05(config_path, version_id, output_dir)
    elif stage_name == "stage06":
        from src.stages.stage06_build_grid_links import run as run_stage06

        run_stage06(config_path, version_id, output_dir, grid_type)
    elif stage_name == "stage07":
        from src.stages.stage07_build_od_and_population import run as run_stage07

        run_stage07(config_path, version_id, output_dir, grid_type)
    elif stage_name == "stage08":
        from src.stages.stage08_build_qsm_inputs import run as run_stage08

        run_stage08(config_path, version_id, output_dir, grid_type)
    elif stage_name == "stage09":
        from src.stages.stage09_compare_versions import run as run_stage09

        run_stage09(output_dir)
    elif stage_name == "stage10":
        from src.stages.stage10_generate_figures import run as run_stage10

        run_stage10(config_path, version_id, output_dir, "all", grid_type)
    else:
        raise ValueError(f"Unsupported stage: {stage_name}")


def main():
    args = parse_args()
    stages = selected_stages(args.from_stage, args.to_stage)
    if args.skip_figures and "stage10" in stages:
        stages = [stage for stage in stages if stage != "stage10"]

    for stage_name in stages:
        print(f"[run_full_pipeline] running {stage_name}")
        run_stage(stage_name, args.config, args.version_id, args.output_dir, args.grid_type)

    print(
        "[run_full_pipeline] completed "
        f"version={args.version_id} stages={','.join(stages)}"
    )


if __name__ == "__main__":
    main()
