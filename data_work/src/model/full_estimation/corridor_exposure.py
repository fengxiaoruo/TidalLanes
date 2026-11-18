"""Shortest-path corridor exposure for tidal-lane beneficiary counts."""
from __future__ import annotations

import heapq

import numpy as np

from src.model.full_estimation.build_primitives import Primitives

EPS = 1e-12


def _build_adjacency(prim: Primitives, tau_min: np.ndarray) -> list[list[tuple[int, float, int]]]:
    n = prim.n_nodes
    adj: list[list[tuple[int, float, int]]] = [[] for _ in range(n)]
    for e, (i, j) in enumerate(zip(prim.edge_i, prim.edge_j)):
        w = float(tau_min[e])
        if w > 0 and np.isfinite(w):
            adj[int(i)].append((int(j), w, int(e)))
    return adj


def _dijkstra_tree(adj: list[list[tuple[int, float, int]]], src: int) -> tuple[np.ndarray, np.ndarray]:
    n = len(adj)
    dist = np.full(n, np.inf)
    pred_edge = np.full(n, -1, dtype=int)
    pred_node = np.full(n, -1, dtype=int)
    dist[src] = 0.0
    heap: list[tuple[float, int]] = [(0.0, src)]
    while heap:
        d, u = heapq.heappop(heap)
        if d > dist[u]:
            continue
        for v, w, eidx in adj[u]:
            nd = d + w
            if nd < dist[v]:
                dist[v] = nd
                pred_edge[v] = eidx
                pred_node[v] = u
                heapq.heappush(heap, (nd, v))
    return pred_node, pred_edge


def path_edge_indices(
    src: int,
    dst: int,
    pred_node: np.ndarray,
    pred_edge: np.ndarray,
) -> set[int]:
    if src == dst:
        return set()
    if pred_edge[dst] < 0:
        return set()
    edges: set[int] = set()
    cur = dst
    while cur != src:
        e = int(pred_edge[cur])
        if e < 0:
            return set()
        edges.add(e)
        cur = int(pred_node[cur])
        if cur < 0:
            return set()
    return edges


def od_path_exposure(
    prim: Primitives,
    tau_min: np.ndarray,
    treated_edge_idx: np.ndarray,
    *,
    min_od_flow: float = 0.0,
) -> dict:
    """Share of observed OD commuter mass on shortest paths using treated edges."""
    treated = set(int(x) for x in np.asarray(treated_edge_idx, dtype=int))
    adj = _build_adjacency(prim, tau_min)
    L = prim.L_ij_obs
    n = prim.n_nodes

    trees: dict[int, tuple[np.ndarray, np.ndarray]] = {}
    exposed_mass = 0.0
    total_mass = 0.0
    n_pairs = 0
    n_exposed_pairs = 0

    for i in range(n):
        row_sum = float(L[i, :].sum())
        if row_sum <= min_od_flow:
            continue
        if i not in trees:
            trees[i] = _dijkstra_tree(adj, i)
        pred_node, pred_edge = trees[i]
        for j in range(n):
            flow = float(L[i, j])
            if flow <= min_od_flow:
                continue
            total_mass += flow
            n_pairs += 1
            if path_edge_indices(i, j, pred_node, pred_edge) & treated:
                exposed_mass += flow
                n_exposed_pairs += 1

    share = exposed_mass / max(total_mass, EPS)
    return {
        "od_total_mass": total_mass,
        "od_exposed_mass": exposed_mass,
        "od_exposed_share": share,
        "od_exposed_share_pct": 100.0 * share,
        "od_n_pairs_positive": n_pairs,
        "od_n_pairs_exposed": n_exposed_pairs,
        "od_n_pairs_exposed_pct": 100.0 * n_exposed_pairs / max(n_pairs, 1),
    }


def slow_direction_od_mass(
    prim: Primitives,
    slow_i: np.ndarray,
    slow_j: np.ndarray,
) -> dict:
    """Direct slow-leg grid OD (not full path exposure)."""
    L = prim.L_ij_obs
    flows = np.array([float(L[int(i), int(j)]) for i, j in zip(slow_i, slow_j)])
    bilateral = np.array([
        float(L[int(i), int(j)] + L[int(j), int(i)])
        for i, j in zip(slow_i, slow_j)
    ])
    return {
        "slow_dir_od_sum": float(flows.sum()),
        "slow_dir_od_share_pct": 100.0 * float(flows.sum()) / max(prim.Lbar, EPS),
        "bilateral_od_sum": float(bilateral.sum()),
        "bilateral_od_share_pct": 100.0 * float(bilateral.sum()) / max(prim.Lbar, EPS),
        "n_pairs_slow_od_positive": int((flows > 0).sum()),
        "n_pairs": len(flows),
    }
