"""Evaluate OD shortest-time effects of provisional cross-grid edge tiers."""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.sparse import csr_matrix
from scipy.sparse.csgraph import dijkstra


def distances_for_od(edges, od, n_nodes):
    rows = pd.to_numeric(edges.i, errors="coerce").astype(int).to_numpy()
    cols = pd.to_numeric(edges.j, errors="coerce").astype(int).to_numpy()
    vals = pd.to_numeric(edges.t_min, errors="coerce").to_numpy(float)
    matrix = csr_matrix((vals, (rows, cols)), shape=(n_nodes, n_nodes))
    origins = np.sort(od.origin_node.unique().astype(int))
    dist = dijkstra(matrix, directed=True, indices=origins, return_predecessors=False)
    origin_pos = {node: pos for pos, node in enumerate(origins)}
    return np.asarray([
        dist[origin_pos[int(o)], int(d)] for o, d in zip(od.origin_node, od.destination_node)
    ])


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--source-root", default="data_work/outputs/manual_centerline_rules_v11")
    p.add_argument(
        "--tier-root",
        default="data_work/outputs/centerline_crossgrid_robustness_tiers_v1",
    )
    args = p.parse_args()
    source, root = Path(args.source_root), Path(args.tier_root)
    output = root / "metrics" / "od_shortest_time_impact.csv"
    detail_output = root / "data" / "od_shortest_time_impact.parquet"
    if output.exists() or detail_output.exists():
        raise SystemExit("Refusing to overwrite existing OD-time evaluation.")

    od = pd.read_parquet(source / "data" / "qsm_input_od_square.parquet")
    od = od.loc[od.commuters_road.gt(0)].copy().reset_index(drop=True)
    node_map = pd.read_csv(source / "data" / "t_nodes_square.csv")
    n_nodes = int(max(node_map.node_i.max(), od.origin_node.max(), od.destination_node.max())) + 1
    files = {
        "baseline": source / "data" / "t_edges_square_AM.csv",
        "tier_A_speed_missing_matched": root / "data" / "t_edges_square_AM_tier_A_speed_missing_matched.csv",
        "tier_B_plus_geometry_matched": root / "data" / "t_edges_square_AM_tier_B_plus_geometry_matched.csv",
        "tier_C_all_full_speed": root / "data" / "t_edges_square_AM_tier_C_all_full_speed.csv",
    }
    detail = od[["origin_grid", "destination_grid", "origin_node", "destination_node", "commuters_road"]].copy()
    for name, path in files.items():
        edges = pd.read_csv(path)
        detail[f"time_{name}_min"] = distances_for_od(edges, od, n_nodes)

    baseline = detail.time_baseline_min.to_numpy()
    weight = detail.commuters_road.to_numpy(float)
    rows = []
    for name in list(files)[1:]:
        current = detail[f"time_{name}_min"].to_numpy()
        common = np.isfinite(baseline) & np.isfinite(current)
        w = weight[common]
        delta = current[common] - baseline[common]
        reduced = delta < -1e-9
        rows.append({
            "scenario": name,
            "common_road_commuters": float(w.sum()),
            "weighted_baseline_time_min": float(np.average(baseline[common], weights=w)),
            "weighted_scenario_time_min": float(np.average(current[common], weights=w)),
            "weighted_time_change_pct": float(np.average(delta, weights=w) / np.average(baseline[common], weights=w) * 100),
            "road_commuter_share_time_reduced": float(w[reduced].sum() / w.sum()),
            "person_minutes_reduced": float(-(delta * w).sum()),
            "od_pairs_time_reduced": int(reduced.sum()),
            "median_reduction_affected_min": float(np.median(-delta[reduced])) if reduced.any() else 0.0,
            "p95_reduction_affected_min": float(np.quantile(-delta[reduced], 0.95)) if reduced.any() else 0.0,
        })
    pd.DataFrame(rows).to_csv(output, index=False)
    detail.to_parquet(detail_output, index=False)
    print(pd.DataFrame(rows).to_string(index=False))


if __name__ == "__main__":
    main()
