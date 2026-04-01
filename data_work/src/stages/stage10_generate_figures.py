"""
Stage 10: Generate Figures

Purpose:
- Generate restored diagnostics figures for a completed versioned run
- Provide a formal plotting stage under the stage-based framework
"""

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def parse_args():
    parser = argparse.ArgumentParser(description="Stage 10: Generate figures")
    parser.add_argument("--config", default=None, help="Optional config file path.")
    parser.add_argument("--version-id", required=True, help="Version identifier for outputs.")
    parser.add_argument("--output-dir", default="outputs", help="Base output directory for versioned results.")
    parser.add_argument(
        "--figure-group",
        default="all",
        choices=["all", "match", "speed", "asymmetry", "grid"],
        help="Subset of figures to generate.",
    )
    parser.add_argument(
        "--grid-type",
        default="all",
        choices=["all", "square", "hex", "voronoi"],
        help="Grid system to process when figure-group includes grid figures.",
    )
    return parser.parse_args()


def save_config_snapshot(version_root: Path, config_path: str | None, figure_group: str, grid_type: str):
    payload = {
        "stage": "stage10_generate_figures",
        "config_path": config_path,
        "figure_group": figure_group,
        "grid_type": grid_type,
    }
    (version_root / "config_snapshot.stage10.json").write_text(
        json.dumps(payload, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )


def run(config_path: str | None, version_id: str, output_dir: str, figure_group: str, grid_type: str):
    from src.diagnostics.mpl_setup import ensure_mpl_cache

    version_root = Path(output_dir) / version_id
    version_root.mkdir(parents=True, exist_ok=True)
    ensure_mpl_cache(output_dir)
    from src.diagnostics.plot_asymmetry_diagnostics import run as run_asymmetry
    from src.diagnostics.plot_grid_diagnostics import run as run_grid
    from src.diagnostics.plot_match_diagnostics import run as run_match
    from src.diagnostics.plot_speed_diagnostics import run as run_speed

    save_config_snapshot(version_root, config_path, figure_group, grid_type)

    if figure_group in {"all", "match"}:
        run_match(version_id, output_dir)
    if figure_group in {"all", "speed"}:
        run_speed(version_id, output_dir)
    if figure_group in {"all", "asymmetry"}:
        run_asymmetry(version_id, output_dir)
    if figure_group in {"all", "grid"}:
        run_grid(version_id, output_dir, grid_type)

    print(
        "[stage10] generated figures "
        f"for version={version_id} figure_group={figure_group} grid_type={grid_type}"
    )


def main():
    args = parse_args()
    run(args.config, args.version_id, args.output_dir, args.figure_group, args.grid_type)


if __name__ == "__main__":
    main()
