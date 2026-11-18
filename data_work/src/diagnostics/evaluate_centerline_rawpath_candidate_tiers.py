"""Evaluate candidate tiers without promoting them to the centerline baseline."""

from __future__ import annotations

import argparse
from pathlib import Path

import geopandas as gpd
import pandas as pd

from bridge_centerline_endpoints_experiment import (
    connectivity_stats,
    directed_pairs_from_centerlines,
    rook_pairs,
    summarize_rook_coverage,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", default="data_work/outputs/manual_centerline_rules_v11")
    parser.add_argument(
        "--candidate-root",
        default="data_work/outputs/centerline_rawpath_topology_candidates_v1",
    )
    parser.add_argument("--component-snap-m", type=float, default=35.0)
    args = parser.parse_args()

    source = Path(args.source_root)
    root = Path(args.candidate_root)
    output = root / "metrics" / "candidate_tier_network_impact.csv"
    if output.exists():
        raise SystemExit(f"Refusing to overwrite existing evaluation: {output}")

    centerline = gpd.read_parquet(source / "data" / "centerline_master.parquet")
    baseline = centerline.loc[centerline.keep_baseline].copy().reset_index(drop=True)
    grid = gpd.read_parquet(source / "data" / "grid_square_master.parquet")
    candidate = gpd.read_file(root / "data" / "rawpath_topology_candidates.geojson")
    if grid.crs != baseline.crs:
        grid = grid.to_crs(baseline.crs)
    if candidate.crs != baseline.crs:
        candidate = candidate.to_crs(baseline.crs)

    rook = rook_pairs(grid)
    rows = []
    scenarios = [
        ("baseline", []),
        ("high_only", ["high"]),
        ("high_plus_medium", ["high", "medium"]),
        ("all_candidates", ["high", "medium", "low"]),
    ]
    for name, tiers in scenarios:
        add = candidate.loc[candidate.confidence.isin(tiers)].copy()
        add["length_m"] = add.geometry.length
        network = pd.concat(
            [baseline[["geometry", "length_m"]], add[["geometry", "length_m"]]],
            ignore_index=True,
        )
        network = gpd.GeoDataFrame(network, geometry="geometry", crs=baseline.crs)
        conn = connectivity_stats(network, args.component_snap_m)
        directed = directed_pairs_from_centerlines(network, grid)
        coverage = summarize_rook_coverage(rook, directed)
        rows.append(
            {
                "scenario": name,
                "candidate_count": int(len(add)),
                "weak_components": conn["n_weak_comp"],
                "lcc_centerline_share": conn["lcc_share"],
                "lcc_length_share": conn["lcc_len_share"],
                "rook_any_direction_share": coverage["any_dir_share"],
                "directed_grid_pairs": coverage["directed_pairs"],
            }
        )
    pd.DataFrame(rows).to_csv(output, index=False)
    print(pd.DataFrame(rows).to_string(index=False))


if __name__ == "__main__":
    main()
