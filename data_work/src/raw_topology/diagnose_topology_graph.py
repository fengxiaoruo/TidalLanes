"""
Step 3 for the standalone raw-topology prototype:
- compute graph connectivity diagnostics
- compare edge/node degree patterns
"""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path

import geopandas as gpd
import pandas as pd

from src.raw_topology.utils import ensure_output_dirs


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT = ROOT / "outputs" / "raw_topology_mvp"


def parse_args():
    parser = argparse.ArgumentParser(description="Diagnose a standalone raw-road topology graph")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT), help="Prototype output directory")
    return parser.parse_args()


def component_labels(edges: pd.DataFrame) -> dict[int, int]:
    adj = defaultdict(set)
    for row in edges.itertuples(index=False):
        adj[int(row.u)].add(int(row.v))
        adj[int(row.v)].add(int(row.u))
    labels = {}
    comp_id = 0
    for node in adj:
        if node in labels:
            continue
        stack = [node]
        labels[node] = comp_id
        while stack:
            cur = stack.pop()
            for nbr in adj[cur]:
                if nbr not in labels:
                    labels[nbr] = comp_id
                    stack.append(nbr)
        comp_id += 1
    return labels


def main():
    args = parse_args()
    output_root = Path(args.output_root)
    paths = ensure_output_dirs(output_root)
    edges = gpd.read_parquet(paths.data_dir / "raw_topology_edges.parquet")
    nodes = gpd.read_parquet(paths.data_dir / "raw_topology_nodes.parquet")

    labels = component_labels(edges[["u", "v"]])
    node_comp = pd.DataFrame({"node_id": list(labels.keys()), "component_id": list(labels.values())})
    component_sizes = node_comp["component_id"].value_counts().sort_values(ascending=False).rename_axis("component_id").reset_index(name="node_count")
    nodes = nodes.merge(node_comp, left_on="node_id", right_on="node_id", how="left")
    edges["component_id"] = edges["u"].map(labels)
    edges.to_parquet(paths.data_dir / "raw_topology_edges.parquet", index=False)
    nodes.to_parquet(paths.data_dir / "raw_topology_nodes.parquet", index=False)

    deg = Counter()
    for row in edges.itertuples(index=False):
        deg[int(row.u)] += 1
        deg[int(row.v)] += 1
    degree_df = pd.DataFrame({"node_id": list(deg.keys()), "degree": list(deg.values())})
    degree_hist = degree_df["degree"].value_counts().sort_index().rename_axis("degree").reset_index(name="node_count")
    degree_hist.to_csv(paths.metrics_dir / "degree_histogram.csv", index=False)
    component_sizes.to_csv(paths.metrics_dir / "component_sizes.csv", index=False)

    giant = int(component_sizes.iloc[0]["node_count"]) if not component_sizes.empty else 0
    summary = {
        "nodes_total": int(len(nodes)),
        "edges_total": int(len(edges)),
        "components_total": int(len(component_sizes)),
        "largest_component_nodes": giant,
        "largest_component_share_nodes": float(giant / len(nodes)) if len(nodes) else 0.0,
        "self_loop_edges": int((edges["u"] == edges["v"]).sum()),
        "median_degree": float(degree_df["degree"].median()) if not degree_df.empty else 0.0,
        "dead_end_nodes": int((degree_df["degree"] == 1).sum()),
        "dead_end_share_nodes": float((degree_df["degree"] == 1).mean()) if not degree_df.empty else 0.0,
    }
    (paths.metrics_dir / "graph_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()

