"""
Step 1 for the standalone raw-topology prototype:
- load raw roads
- project to EPSG:3857
- remove exact duplicates
- save a clean raw baseline
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from src.raw_topology.utils import deduplicate_exact_geometries, ensure_output_dirs, load_raw_roads


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT = ROOT / "raw_data" / "gis" / "roads_baidu" / "beijing_roads.shp"
DEFAULT_OUTPUT = ROOT / "outputs" / "raw_topology_mvp"


def parse_args():
    parser = argparse.ArgumentParser(description="Clean raw roads for the standalone topology prototype")
    parser.add_argument("--input", default=str(DEFAULT_INPUT), help="Input raw roads shapefile")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT), help="Prototype output directory")
    return parser.parse_args()


def main():
    args = parse_args()
    output_root = Path(args.output_root)
    paths = ensure_output_dirs(output_root)
    raw = load_raw_roads(Path(args.input))
    clean, removed_exact_dups = deduplicate_exact_geometries(raw)
    clean.to_parquet(paths.data_dir / "raw_roads_clean.parquet", index=False)

    summary = {
        "input_path": str(Path(args.input)),
        "rows_input": int(len(raw)),
        "rows_clean": int(len(clean)),
        "removed_exact_duplicates": int(removed_exact_dups),
        "length_total_km": float(clean.length_m.sum() / 1000.0),
        "length_median_m": float(clean.length_m.median()),
    }
    (paths.metrics_dir / "clean_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()

