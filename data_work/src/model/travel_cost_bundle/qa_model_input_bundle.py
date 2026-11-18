"""QA checks for locked model-input bundles.

The checks focus on the handoff points that can silently break model results:
grid ids, fresh node indices, OD links, geometry coverage, and LCC scope.
"""
from __future__ import annotations

import argparse
import json
from collections import deque
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


REPO = Path(__file__).resolve().parents[3]
GRID_MASTER = REPO / "outputs" / "manual_centerline_rules_v11" / "data" / "grid_square_master.parquet"
UTM_EPSG = 32650


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="QA a QSM model-input bundle.")
    p.add_argument("--version-id", required=True)
    p.add_argument("--grid-type", default="square")
    p.add_argument("--period", default="AM")
    p.add_argument("--detour-gate", type=float, default=None)
    p.add_argument("--output-dir", default=None)
    p.add_argument("--outputs-root", default=str(REPO / "outputs"))
    return p.parse_args()


def _weak_components(n_nodes: int, edge_i: np.ndarray, edge_j: np.ndarray) -> list[list[int]]:
    adj: list[list[int]] = [[] for _ in range(n_nodes)]
    for i, j in zip(edge_i, edge_j):
        if 0 <= i < n_nodes and 0 <= j < n_nodes:
            adj[int(i)].append(int(j))
            adj[int(j)].append(int(i))

    seen = np.zeros(n_nodes, dtype=bool)
    comps: list[list[int]] = []
    for start in range(n_nodes):
        if seen[start]:
            continue
        seen[start] = True
        q: deque[int] = deque([start])
        comp: list[int] = []
        while q:
            node = q.popleft()
            comp.append(node)
            for nxt in adj[node]:
                if not seen[nxt]:
                    seen[nxt] = True
                    q.append(nxt)
        comps.append(comp)
    comps.sort(key=len, reverse=True)
    return comps


def _finite_positive(series: pd.Series) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    return np.isfinite(values) & (values > 0)


def _load_geometry(grid_type: str) -> tuple[pd.DataFrame, dict[str, tuple[float, float]], dict[str, Any]]:
    if grid_type != "square":
        return pd.DataFrame(), {}, {"geometry_loaded": False, "reason": "only square grid master is configured"}

    import geopandas as gpd

    grid = gpd.read_parquet(GRID_MASTER)
    geom_info: dict[str, Any] = {
        "geometry_loaded": True,
        "grid_master_path": str(GRID_MASTER),
        "grid_master_crs": str(grid.crs),
        "grid_master_n": int(len(grid)),
        "grid_id_unique": bool(grid["grid_id"].astype(str).is_unique),
    }
    grid_4326 = grid.to_crs(4326)
    bounds = grid_4326.total_bounds
    geom_info["bounds_lonlat"] = [float(x) for x in bounds]
    grid_utm = grid.to_crs(UTM_EPSG)
    cent = grid_utm.geometry.centroid
    xy = {
        str(gid): (float(x), float(y))
        for gid, x, y in zip(grid_utm["grid_id"], cent.x.values, cent.y.values)
    }
    return pd.DataFrame(grid.drop(columns="geometry")), xy, geom_info


def _detour_stats(edges: pd.DataFrame, xy: dict[str, tuple[float, float]], gate: float | None) -> dict[str, Any]:
    if not xy or "route_length_m" not in edges.columns:
        return {"detour_check_available": False}

    route = pd.to_numeric(edges["route_length_m"], errors="coerce").to_numpy(dtype=float)
    dists = np.full(len(edges), np.nan, dtype=float)
    for pos, (o, d) in enumerate(zip(edges["grid_o"].astype(str), edges["grid_d"].astype(str))):
        if o in xy and d in xy:
            (x1, y1), (x2, y2) = xy[o], xy[d]
            dists[pos] = np.hypot(x1 - x2, y1 - y2)

    with np.errstate(divide="ignore", invalid="ignore"):
        detour = route / dists
    valid = np.isfinite(detour)
    out: dict[str, Any] = {
        "detour_check_available": True,
        "detour_valid_edges": int(valid.sum()),
        "detour_missing_edges": int((~valid).sum()),
    }
    if valid.any():
        vals = detour[valid]
        out.update(
            {
                "detour_min": float(np.min(vals)),
                "detour_p50": float(np.quantile(vals, 0.50)),
                "detour_p95": float(np.quantile(vals, 0.95)),
                "detour_p99": float(np.quantile(vals, 0.99)),
                "detour_max": float(np.max(vals)),
            }
        )
        if gate is not None:
            out["detour_gate"] = float(gate)
            out["edges_above_gate"] = int((vals > gate + 1e-9).sum())
    return out


def run_qa(args: argparse.Namespace) -> tuple[dict[str, Any], pd.DataFrame]:
    root = Path(args.outputs_root) / args.version_id
    data = root / "data"
    nodes = pd.read_parquet(data / f"qsm_input_nodes_{args.grid_type}.parquet")
    edges = pd.read_parquet(data / f"qsm_input_edges_{args.grid_type}.parquet")
    od = pd.read_parquet(data / f"qsm_input_od_{args.grid_type}.parquet")

    for frame, cols in [
        (nodes, ["grid_id", "node_i"]),
        (edges, ["grid_o", "grid_d", "i", "j", "tau_obs_min", "tau_ff_min"]),
        (od, ["home_grid", "work_grid", "home_i", "work_i", "commuters_road"]),
    ]:
        for col in cols:
            if col not in frame.columns:
                raise ValueError(f"Missing required column: {col}")

    nodes = nodes.copy()
    edges = edges.copy()
    od = od.copy()
    nodes["grid_id"] = nodes["grid_id"].astype(str)
    edges["grid_o"] = edges["grid_o"].astype(str)
    edges["grid_d"] = edges["grid_d"].astype(str)
    od["home_grid"] = od["home_grid"].astype(str)
    od["work_grid"] = od["work_grid"].astype(str)

    n_nodes = len(nodes)
    node_i = pd.to_numeric(nodes["node_i"], errors="coerce").astype("Int64")
    expected = np.arange(n_nodes, dtype=int)
    node_index_contiguous = (
        node_i.notna().all()
        and np.array_equal(np.sort(node_i.astype(int).to_numpy()), expected)
    )
    grid_to_idx = dict(zip(nodes["grid_id"], node_i.astype(int)))

    edge_i = pd.to_numeric(edges["i"], errors="coerce").astype("Int64")
    edge_j = pd.to_numeric(edges["j"], errors="coerce").astype("Int64")
    edge_i_expected = edges["grid_o"].map(grid_to_idx)
    edge_j_expected = edges["grid_d"].map(grid_to_idx)
    edge_index_ok = edge_i.equals(edge_i_expected.astype("Int64")) and edge_j.equals(edge_j_expected.astype("Int64"))

    od_home_i = pd.to_numeric(od["home_i"], errors="coerce").astype("Int64")
    od_work_i = pd.to_numeric(od["work_i"], errors="coerce").astype("Int64")
    od_home_expected = od["home_grid"].map(grid_to_idx)
    od_work_expected = od["work_grid"].map(grid_to_idx)
    od_index_ok = od_home_i.equals(od_home_expected.astype("Int64")) and od_work_i.equals(od_work_expected.astype("Int64"))
    origin_ok = True
    dest_ok = True
    if "origin_node" in od.columns:
        origin_ok = pd.to_numeric(od["origin_node"], errors="coerce").astype("Int64").equals(od_home_expected.astype("Int64"))
    if "destination_node" in od.columns:
        dest_ok = pd.to_numeric(od["destination_node"], errors="coerce").astype("Int64").equals(od_work_expected.astype("Int64"))

    comps = _weak_components(n_nodes, edge_i.astype(int).to_numpy(), edge_j.astype(int).to_numpy())
    comp_sizes = [len(c) for c in comps]
    edge_degree = np.zeros(n_nodes, dtype=int)
    np.add.at(edge_degree, edge_i.astype(int).to_numpy(), 1)
    np.add.at(edge_degree, edge_j.astype(int).to_numpy(), 1)

    grid_master, xy, geom_info = _load_geometry(args.grid_type)
    if len(grid_master):
        master_ids = set(grid_master["grid_id"].astype(str))
        geom_missing_nodes = sorted(set(nodes["grid_id"]) - master_ids)
    else:
        geom_missing_nodes = []

    summary: dict[str, Any] = {
        "version_id": args.version_id,
        "grid_type": args.grid_type,
        "period": args.period,
        "bundle_root": str(root),
        "n_nodes": int(n_nodes),
        "n_edges": int(len(edges)),
        "n_od_pairs": int(len(od)),
        "node_grid_id_unique": bool(nodes["grid_id"].is_unique),
        "node_i_contiguous_0_based": bool(node_index_contiguous),
        "edge_grid_ids_all_in_nodes": bool(edges["grid_o"].isin(grid_to_idx).all() and edges["grid_d"].isin(grid_to_idx).all()),
        "edge_i_j_match_grid_ids": bool(edge_index_ok),
        "edge_duplicate_grid_pairs": int(edges.duplicated(["grid_o", "grid_d"]).sum()),
        "edge_tau_obs_positive": bool(_finite_positive(edges["tau_obs_min"]).all()),
        "edge_tau_ff_positive": bool(_finite_positive(edges["tau_ff_min"]).all()),
        "edge_tau_obs_lt_tau_ff_count": int(
            (pd.to_numeric(edges["tau_obs_min"], errors="coerce") < pd.to_numeric(edges["tau_ff_min"], errors="coerce")).sum()
        ),
        "od_grid_ids_all_in_nodes": bool(od["home_grid"].isin(grid_to_idx).all() and od["work_grid"].isin(grid_to_idx).all()),
        "od_home_work_i_match_grid_ids": bool(od_index_ok),
        "od_origin_destination_match_home_work": bool(origin_ok and dest_ok),
        "od_duplicate_grid_pairs": int(od.duplicated(["home_grid", "work_grid"]).sum()),
        "od_commuters_road_sum": float(pd.to_numeric(od["commuters_road"], errors="coerce").fillna(0).sum()),
        "weak_component_count": int(len(comps)),
        "weak_component_sizes_top10": [int(x) for x in comp_sizes[:10]],
        "is_single_lcc_scope": bool(len(comps) == 1 and comp_sizes[0] == n_nodes),
        "isolated_nodes_in_edge_graph": int((edge_degree == 0).sum()),
        "geometry_missing_node_count": int(len(geom_missing_nodes)),
        "geometry_missing_node_examples": geom_missing_nodes[:10],
    }
    summary.update(geom_info)
    summary.update(_detour_stats(edges, xy, args.detour_gate))
    lanes = pd.to_numeric(edges.get("lanes_directional"), errors="coerce")
    lane_valid = np.isfinite(lanes) & (lanes > 0)
    summary.update(
        {
            "edge_lanes_positive": bool(lane_valid.all()),
            "edge_lanes_missing_count": int((~lane_valid).sum()),
            "edge_lanes_unique_count": int(lanes[lane_valid].round(6).nunique()),
            "edge_lanes_mean": float(lanes[lane_valid].mean()) if lane_valid.any() else np.nan,
            "edge_lanes_p10": float(lanes[lane_valid].quantile(0.10)) if lane_valid.any() else np.nan,
            "edge_lanes_p50": float(lanes[lane_valid].quantile(0.50)) if lane_valid.any() else np.nan,
            "edge_lanes_p90": float(lanes[lane_valid].quantile(0.90)) if lane_valid.any() else np.nan,
        }
    )
    if "lanes_directional_source" in edges.columns:
        summary["edge_lane_source_counts"] = {
            str(k): int(v) for k, v in edges["lanes_directional_source"].value_counts(dropna=False).items()
        }
    if "lane_path_all_missing_flag" in edges.columns:
        all_missing = pd.to_numeric(edges["lane_path_all_missing_flag"], errors="coerce").fillna(0)
        summary["edge_lane_path_all_missing_count"] = int((all_missing == 1).sum())
        summary["edge_lane_path_all_missing_share"] = float((all_missing == 1).mean()) if len(all_missing) else np.nan

    checks = [
        ("node_grid_id_unique", summary["node_grid_id_unique"]),
        ("node_i_contiguous_0_based", summary["node_i_contiguous_0_based"]),
        ("edge_grid_ids_all_in_nodes", summary["edge_grid_ids_all_in_nodes"]),
        ("edge_i_j_match_grid_ids", summary["edge_i_j_match_grid_ids"]),
        ("edge_no_duplicate_grid_pairs", summary["edge_duplicate_grid_pairs"] == 0),
        ("edge_tau_obs_positive", summary["edge_tau_obs_positive"]),
        ("edge_tau_ff_positive", summary["edge_tau_ff_positive"]),
        ("od_grid_ids_all_in_nodes", summary["od_grid_ids_all_in_nodes"]),
        ("od_home_work_i_match_grid_ids", summary["od_home_work_i_match_grid_ids"]),
        ("od_origin_destination_match_home_work", summary["od_origin_destination_match_home_work"]),
        ("od_no_duplicate_grid_pairs", summary["od_duplicate_grid_pairs"] == 0),
        ("single_lcc_scope", summary["is_single_lcc_scope"]),
        ("no_isolated_nodes", summary["isolated_nodes_in_edge_graph"] == 0),
        ("geometry_covers_nodes", summary["geometry_missing_node_count"] == 0),
        ("edge_lanes_positive", summary["edge_lanes_positive"]),
    ]
    if args.detour_gate is not None and summary.get("detour_check_available"):
        checks.append(("detour_gate_respected", summary.get("edges_above_gate", 0) == 0))

    checks_df = pd.DataFrame(
        [{"check": name, "passed": bool(passed)} for name, passed in checks]
    )
    summary["all_required_checks_passed"] = bool(checks_df["passed"].all())
    summary["failed_checks"] = checks_df.loc[~checks_df["passed"], "check"].tolist()
    return summary, checks_df


def main() -> None:
    args = _parse_args()
    summary, checks = run_qa(args)
    out_dir = Path(args.output_dir) if args.output_dir else Path(args.outputs_root) / args.version_id / "qa"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "model_input_qa.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    checks.to_csv(out_dir / "model_input_qa_checks.csv", index=False)
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
