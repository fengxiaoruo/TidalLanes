"""
Step 2 for the standalone raw-topology prototype:
- snap nearby endpoints
- node the linear network at exact intersections
- assign source lineage
- build node/edge tables
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import geopandas as gpd

from src.raw_topology.utils import (
    assign_lineage,
    build_nodes_and_edges,
    cluster_endpoints,
    ensure_output_dirs,
    node_network,
    snap_line_endpoints,
)

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT = ROOT / "outputs" / "raw_topology_mvp"


def parse_args():
    parser = argparse.ArgumentParser(description="Build a standalone raw-road topology graph")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT), help="Prototype output directory")
    parser.add_argument("--snap-tol", type=float, default=15.0, help="Endpoint snap tolerance in meters")
    parser.add_argument("--node-tol", type=float, default=0.5, help="Final node clustering tolerance in meters")
    parser.add_argument("--min-edge-len", type=float, default=3.0, help="Drop noded edges shorter than this threshold")
    return parser.parse_args()


def main():
    args = parse_args()
    output_root = Path(args.output_root)
    paths = ensure_output_dirs(output_root)
    clean = gpd.read_parquet(paths.data_dir / "raw_roads_clean.parquet")
    endpoint_gdf, centers = cluster_endpoints(clean, args.snap_tol)
    snapped = snap_line_endpoints(clean, endpoint_gdf)
    snapped = snapped.loc[snapped.length_m >= args.min_edge_len].copy()
    noded_edges = node_network(snapped)
    noded_edges = noded_edges.loc[noded_edges.geometry.length >= args.min_edge_len].copy()
    topo_edges = assign_lineage(noded_edges, snapped)
    nodes, topo_edges = build_nodes_and_edges(topo_edges, node_tol_m=args.node_tol)

    endpoint_gdf.to_parquet(paths.data_dir / "raw_endpoint_clusters.parquet", index=False)
    snapped.to_parquet(paths.data_dir / "raw_roads_snapped.parquet", index=False)
    topo_edges.to_parquet(paths.data_dir / "raw_topology_edges.parquet", index=False)
    nodes.to_parquet(paths.data_dir / "raw_topology_nodes.parquet", index=False)

    summary = {
        "snap_tol_m": args.snap_tol,
        "node_tol_m": args.node_tol,
        "min_edge_len_m": args.min_edge_len,
        "raw_rows_clean": int(len(clean)),
        "endpoint_cluster_count": int(len(centers)),
        "rows_snapped": int(len(snapped)),
        "rows_noded_edges": int(len(topo_edges)),
        "rows_nodes": int(len(nodes)),
        "topology_length_total_km": float(topo_edges.geometry.length.sum() / 1000.0),
        "lineage_overlap_positive_share": float((topo_edges["source_overlap_m"] > 0).mean()),
    }
    (paths.metrics_dir / "build_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
