from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import shortest_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Decompose edge-projection adjacent-pair travel-time tails into connector and road-path components."
    )
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--grid-type", default="square", choices=["square", "hex", "voronoi"])
    parser.add_argument("--period", default="AM", choices=["AM", "PM", "FF"])
    parser.add_argument("--top-n", type=int, default=200)
    parser.add_argument("--rank-by", default="raw_connected_travel_time_min", choices=["raw_connected_travel_time_min", "diff_min", "ratio_raw_connected_over_old"])
    parser.add_argument("--out-dir", required=True)
    return parser.parse_args()


def first_existing(paths: list[Path]) -> Path:
    for path in paths:
        if path.exists():
            return path
    raise FileNotFoundError("None of these files exists: " + ", ".join(str(p) for p in paths))


def load_inputs(output_root: Path, grid_type: str, period: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    data_dir = output_root / "data"
    pair_path = first_existing(
        [
            data_dir / f"raw_connected_vs_old_adjacent_adjacent_{grid_type}_{period}.csv",
            data_dir / f"kunpeng_vs_old_adjacent_adjacent_{grid_type}_{period}.csv",
        ]
    )
    connector_path = first_existing(
        [
            data_dir / f"raw_connected_grid_connectors_adjacent_{grid_type}.csv",
            data_dir / f"kunpeng_grid_connectors_adjacent_{grid_type}.csv",
        ]
    )
    edge_path = first_existing(
        [
            data_dir / f"raw_connected_road_edges_{period}.csv",
            data_dir / f"kunpeng_road_edges_{period}.csv",
        ]
    )

    pairs = pd.read_csv(pair_path)
    pairs["home_grid"] = pairs["home_grid"].astype(str)
    pairs["work_grid"] = pairs["work_grid"].astype(str)
    if "kunpeng_travel_time_min" in pairs.columns:
        pairs = pairs.rename(columns={"kunpeng_travel_time_min": "raw_connected_travel_time_min"})
    if "ratio_kunpeng_over_old" in pairs.columns:
        pairs = pairs.rename(columns={"ratio_kunpeng_over_old": "ratio_raw_connected_over_old"})

    connectors = pd.read_csv(connector_path)
    connectors["grid_id"] = connectors["grid_id"].astype(str)
    for col in ["from_node_id", "to_node_id", "connector_rank"]:
        connectors[col] = pd.to_numeric(connectors[col], errors="coerce").astype("Int64")
    for col in ["connector_dist_m", "connector_time_min", "access_fraction"]:
        connectors[col] = pd.to_numeric(connectors[col], errors="coerce")

    road_edges = pd.read_csv(edge_path)
    for col in ["from_node_id", "to_node_id"]:
        road_edges[col] = pd.to_numeric(road_edges[col], errors="coerce").astype("Int64")
    for col in ["travel_time_min", "length_m"]:
        road_edges[col] = pd.to_numeric(road_edges[col], errors="coerce")
    road_edges = road_edges.dropna(subset=["from_node_id", "to_node_id", "travel_time_min"]).copy()
    road_edges["from_node_id"] = road_edges["from_node_id"].astype(int)
    road_edges["to_node_id"] = road_edges["to_node_id"].astype(int)
    return pairs, connectors, road_edges


def build_road_graph(road_edges: pd.DataFrame):
    road_node_ids = sorted(pd.unique(pd.concat([road_edges["from_node_id"], road_edges["to_node_id"]], ignore_index=True)).tolist())
    road_node_map = {int(node_id): idx for idx, node_id in enumerate(road_node_ids)}
    rows = road_edges["from_node_id"].map(road_node_map).to_numpy(dtype=int)
    cols = road_edges["to_node_id"].map(road_node_map).to_numpy(dtype=int)
    weights = road_edges["travel_time_min"].to_numpy(dtype=float)
    graph = coo_matrix((weights, (rows, cols)), shape=(len(road_node_map), len(road_node_map))).tocsr()
    edge_lookup = road_edges.set_index(["from_node_id", "to_node_id"])
    return graph, road_node_map, edge_lookup


def get_edge(edge_lookup: pd.DataFrame, from_node_id: int, to_node_id: int) -> pd.Series | None:
    try:
        row = edge_lookup.loc[(from_node_id, to_node_id)]
    except KeyError:
        return None
    if isinstance(row, pd.DataFrame):
        return row.iloc[0]
    return row


def decompose_pair(
    home_grid: str,
    work_grid: str,
    connectors_by_grid: dict[str, pd.DataFrame],
    graph,
    road_node_map: dict[int, int],
    edge_lookup: pd.DataFrame,
) -> dict | None:
    home_conn = connectors_by_grid.get(home_grid)
    work_conn = connectors_by_grid.get(work_grid)
    if home_conn is None or work_conn is None or home_conn.empty or work_conn.empty:
        return None

    best = None
    for src in home_conn.itertuples(index=False):
        if pd.isna(src.from_node_id) or pd.isna(src.to_node_id):
            continue
        start_idx = road_node_map.get(int(src.to_node_id))
        if start_idx is None:
            continue
        src_host = get_edge(edge_lookup, int(src.from_node_id), int(src.to_node_id))
        if src_host is None:
            continue
        dist = shortest_path(graph, directed=True, indices=[start_idx], unweighted=False, method="D")
        dist = np.asarray(dist, dtype=float)[0]
        for dst in work_conn.itertuples(index=False):
            if pd.isna(dst.from_node_id) or pd.isna(dst.to_node_id):
                continue
            end_idx = road_node_map.get(int(dst.from_node_id))
            if end_idx is None or not np.isfinite(dist[end_idx]):
                continue
            dst_host = get_edge(edge_lookup, int(dst.from_node_id), int(dst.to_node_id))
            if dst_host is None:
                continue
            outbound_host_time = float(src_host.travel_time_min) * (1.0 - float(src.access_fraction))
            inbound_host_time = float(dst_host.travel_time_min) * float(dst.access_fraction)
            connector_time = float(src.connector_time_min) + float(dst.connector_time_min)
            total = connector_time + outbound_host_time + float(dist[end_idx]) + inbound_host_time
            if not np.isfinite(total):
                continue
            if best is None or total < best["decomposed_total_min"]:
                best = {
                    "decomposed_total_min": total,
                    "connector_time_min": connector_time,
                    "endpoint_host_time_min": outbound_host_time + inbound_host_time,
                    "middle_road_time_min": float(dist[end_idx]),
                    "road_time_min": outbound_host_time + float(dist[end_idx]) + inbound_host_time,
                    "home_connector_rank": int(src.connector_rank),
                    "work_connector_rank": int(dst.connector_rank),
                    "home_connector_dist_m": float(src.connector_dist_m),
                    "work_connector_dist_m": float(dst.connector_dist_m),
                    "home_access_fraction": float(src.access_fraction),
                    "work_access_fraction": float(dst.access_fraction),
                    "home_from_node_id": int(src.from_node_id),
                    "home_to_node_id": int(src.to_node_id),
                    "work_from_node_id": int(dst.from_node_id),
                    "work_to_node_id": int(dst.to_node_id),
                }
    return best


def summarize_quantiles(series: pd.Series, prefix: str) -> dict[str, float]:
    x = pd.to_numeric(series, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    if x.empty:
        return {}
    probs = [0.5, 0.75, 0.9, 0.95, 0.99]
    return {f"{prefix}_p{int(p * 100):02d}": float(x.quantile(p)) for p in probs} | {f"{prefix}_mean": float(x.mean())}


def main() -> None:
    args = parse_args()
    output_root = Path(args.output_root)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    pairs, connectors, road_edges = load_inputs(output_root, args.grid_type, args.period)
    graph, road_node_map, edge_lookup = build_road_graph(road_edges)
    connectors_by_grid = {grid_id: grp.copy() for grid_id, grp in connectors.groupby("grid_id")}

    pairs["raw_connected_travel_time_min"] = pd.to_numeric(pairs["raw_connected_travel_time_min"], errors="coerce")
    pairs["old_travel_time_min"] = pd.to_numeric(pairs["old_travel_time_min"], errors="coerce")
    pairs["diff_min"] = pd.to_numeric(pairs["diff_min"], errors="coerce")
    pairs["ratio_raw_connected_over_old"] = pd.to_numeric(pairs["ratio_raw_connected_over_old"], errors="coerce")
    connected = pairs[pairs["raw_connected_travel_time_min"].gt(0)].copy()
    selected = connected.sort_values(args.rank_by, ascending=False).head(args.top_n).reset_index(drop=True)

    rows = []
    for pair in selected.itertuples(index=False):
        decomp = decompose_pair(
            home_grid=str(pair.home_grid),
            work_grid=str(pair.work_grid),
            connectors_by_grid=connectors_by_grid,
            graph=graph,
            road_node_map=road_node_map,
            edge_lookup=edge_lookup,
        )
        if decomp is None:
            continue
        base = pair._asdict()
        base.update(decomp)
        base["connector_share_of_total"] = base["connector_time_min"] / base["decomposed_total_min"]
        base["road_share_of_total"] = base["road_time_min"] / base["decomposed_total_min"]
        base["endpoint_host_share_of_road"] = base["endpoint_host_time_min"] / base["road_time_min"] if base["road_time_min"] > 0 else np.nan
        base["decomposition_error_min"] = base["decomposed_total_min"] - base["raw_connected_travel_time_min"]
        rows.append(base)

    tail = pd.DataFrame(rows)
    tail_path = out_dir / f"edgeproj_detour_tail_top{args.top_n}_{args.grid_type}_{args.period}.csv"
    tail.to_csv(tail_path, index=False)

    summary_rows = [
        {
            "grid_type": args.grid_type,
            "period": args.period,
            "connected_pairs": int(len(connected)),
            "selected_pairs": int(len(tail)),
            "rank_by": args.rank_by,
            **summarize_quantiles(connected["raw_connected_travel_time_min"], "all_connected_raw_tt_min"),
            **summarize_quantiles(connected["diff_min"], "all_connected_diff_min"),
            **summarize_quantiles(tail["raw_connected_travel_time_min"], "tail_raw_tt_min"),
            **summarize_quantiles(tail["connector_time_min"], "tail_connector_time_min"),
            **summarize_quantiles(tail["road_time_min"], "tail_road_time_min"),
            **summarize_quantiles(tail["middle_road_time_min"], "tail_middle_road_time_min"),
            **summarize_quantiles(tail["connector_share_of_total"], "tail_connector_share"),
            "tail_mean_decomposition_abs_error_min": float(tail["decomposition_error_min"].abs().mean()) if len(tail) else np.nan,
            "tail_share_connector_gt_25pct": float(tail["connector_share_of_total"].gt(0.25).mean()) if len(tail) else np.nan,
            "tail_share_road_gt_75pct": float(tail["road_share_of_total"].gt(0.75).mean()) if len(tail) else np.nan,
            "tail_share_endpoint_host_gt_25pct_of_road": float(tail["endpoint_host_share_of_road"].gt(0.25).mean()) if len(tail) else np.nan,
        }
    ]
    summary_path = out_dir / f"edgeproj_detour_tail_summary_{args.grid_type}_{args.period}.csv"
    pd.DataFrame(summary_rows).to_csv(summary_path, index=False)
    print(f"wrote {tail_path}")
    print(f"wrote {summary_path}")


if __name__ == "__main__":
    main()
