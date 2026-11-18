"""Largest connected component helpers for grid travel-cost bundles."""
from __future__ import annotations

import pandas as pd


def largest_component_grids(edges: pd.DataFrame, grid_a: str, grid_b: str) -> set[str]:
    """Return grid_ids in the largest undirected component."""
    nodes = pd.Index(pd.unique(pd.concat([edges[grid_a], edges[grid_b]], ignore_index=True))).astype(str)
    parent = {node: node for node in nodes}
    rank = {node: 0 for node in nodes}

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: str, b: str) -> None:
        ra, rb = find(a), find(b)
        if ra == rb:
            return
        if rank[ra] < rank[rb]:
            parent[ra] = rb
        elif rank[ra] > rank[rb]:
            parent[rb] = ra
        else:
            parent[rb] = ra
            rank[ra] += 1

    for row in edges.itertuples(index=False):
        union(str(getattr(row, grid_a)), str(getattr(row, grid_b)))

    comp = pd.DataFrame({"grid_id": nodes})
    comp["root"] = comp["grid_id"].map(find)
    sizes = comp.groupby("root")["grid_id"].transform("size")
    max_size = int(sizes.max())
    return set(comp.loc[sizes == max_size, "grid_id"].astype(str))
