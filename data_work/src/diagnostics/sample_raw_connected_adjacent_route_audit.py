from __future__ import annotations

import argparse
import json
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import shortest_path
from shapely.geometry import LineString


def parse_args():
    parser = argparse.ArgumentParser(
        description="Sample adjacent grid pairs and plot local raw_connected_v1 shortest paths."
    )
    parser.add_argument("--output-root", required=True, help="Output root from build_raw_connected_v1_grid_travel_time.py.")
    parser.add_argument("--grid-type", default="square", choices=["square", "hex", "voronoi"])
    parser.add_argument("--period", default="AM", choices=["AM", "PM", "FF"])
    parser.add_argument("--n-samples", type=int, default=20)
    parser.add_argument("--seed", type=int, default=20260421)
    parser.add_argument("--buffer-m", type=float, default=2500.0, help="Map buffer around the sampled grid pair and route.")
    parser.add_argument("--out-dir", default=None, help="Defaults to <output-root>/route_audit_<grid>_<period>.")
    parser.add_argument(
        "--pair-list-csv",
        default=None,
        help="Optional CSV with columns home_grid and work_grid to force the sampled pairs.",
    )
    return parser.parse_args()


def load_run_config(output_root: Path) -> dict:
    return json.loads((output_root / "run_config.json").read_text(encoding="utf-8"))


def load_grid(config: dict, grid_type: str) -> gpd.GeoDataFrame:
    baseline_root = Path(config["baseline_root"])
    grid = gpd.read_parquet(baseline_root / "data" / f"grid_{grid_type}_master.parquet")[["grid_id", "geometry"]].copy()
    grid["grid_id"] = grid["grid_id"].astype("string")
    return grid.to_crs(3857).reset_index(drop=True)


def find_connector_path(output_root: Path, grid_type: str) -> Path:
    patterns = [
        output_root / "data" / f"raw_connected_grid_connectors_all_{grid_type}.csv",
        output_root / "data" / f"raw_connected_grid_connectors_adjacent_{grid_type}.csv",
        output_root / "data" / f"raw_connected_grid_connectors_{grid_type}.csv",
        output_root / "data" / f"kunpeng_grid_connectors_all_{grid_type}.csv",
        output_root / "data" / f"kunpeng_grid_connectors_adjacent_{grid_type}.csv",
        output_root / "data" / f"kunpeng_grid_connectors_{grid_type}.csv",
    ]
    for path in patterns:
        if path.exists():
            return path
    raise FileNotFoundError(f"Cannot find connector file for grid_type={grid_type}")


def load_inputs(output_root: Path, grid_type: str, period: str):
    pair_path = output_root / "data" / f"raw_connected_vs_old_adjacent_adjacent_{grid_type}_{period}.csv"
    if not pair_path.exists():
        pair_path = output_root / "data" / f"kunpeng_vs_old_adjacent_adjacent_{grid_type}_{period}.csv"
    pair_df = pd.read_csv(pair_path)
    pair_df["home_grid"] = pair_df["home_grid"].astype("string")
    pair_df["work_grid"] = pair_df["work_grid"].astype("string")
    if "kunpeng_travel_time_min" in pair_df.columns and "raw_connected_travel_time_min" not in pair_df.columns:
        pair_df = pair_df.rename(columns={"kunpeng_travel_time_min": "raw_connected_travel_time_min"})
    pair_df["raw_connected_travel_time_min"] = pd.to_numeric(pair_df["raw_connected_travel_time_min"], errors="coerce")

    connectors = pd.read_csv(find_connector_path(output_root, grid_type))
    connectors["grid_id"] = connectors["grid_id"].astype("string")
    connectors["node_id"] = pd.to_numeric(connectors["node_id"], errors="coerce").astype("Int64")

    node_path = output_root / "data" / "raw_connected_road_nodes_usable.csv"
    if not node_path.exists():
        node_path = output_root / "data" / "kunpeng_road_nodes_usable.csv"
    nodes = pd.read_csv(node_path)
    nodes["node_id"] = pd.to_numeric(nodes["node_id"], errors="coerce").astype("Int64")
    nodes = nodes[nodes["node_id"].notna()].copy()
    nodes["node_id"] = nodes["node_id"].astype(int)

    edge_path = output_root / "data" / f"raw_connected_road_edges_{period}.csv"
    if not edge_path.exists():
        edge_path = output_root / "data" / f"kunpeng_road_edges_{period}.csv"
    road_edges = pd.read_csv(edge_path)
    road_edges["from_node_id"] = pd.to_numeric(road_edges["from_node_id"], errors="coerce").astype("Int64")
    road_edges["to_node_id"] = pd.to_numeric(road_edges["to_node_id"], errors="coerce").astype("Int64")
    road_edges = road_edges[road_edges["from_node_id"].notna() & road_edges["to_node_id"].notna()].copy()
    road_edges["from_node_id"] = road_edges["from_node_id"].astype(int)
    road_edges["to_node_id"] = road_edges["to_node_id"].astype(int)
    road_edges["travel_time_min"] = pd.to_numeric(road_edges["travel_time_min"], errors="coerce")
    geom = gpd.GeoSeries.from_wkt(road_edges["geometry"], crs="EPSG:4326", on_invalid="ignore")
    valid = geom.notna() & (~geom.is_empty)
    road_edges = road_edges.loc[valid].copy()
    road_edges = gpd.GeoDataFrame(road_edges, geometry=geom.loc[valid], crs="EPSG:4326").to_crs(3857)
    road_edges["length_m"] = road_edges.geometry.length.astype(float)
    return pair_df, connectors, nodes, road_edges


def build_pairwise_graph(nodes: pd.DataFrame, road_edges: gpd.GeoDataFrame, home_conn: pd.DataFrame, work_conn: pd.DataFrame):
    road_node_ids = sorted(nodes["node_id"].astype(int).unique().tolist())
    road_map = {nid: i for i, nid in enumerate(road_node_ids)}

    ei = road_edges["from_node_id"].map(road_map).to_numpy(dtype=int)
    ej = road_edges["to_node_id"].map(road_map).to_numpy(dtype=int)
    ew = road_edges["travel_time_min"].to_numpy(dtype=float)

    use_home = home_conn[home_conn["node_id"].notna()].copy()
    use_work = work_conn[work_conn["node_id"].notna()].copy()
    use_home["node_id"] = use_home["node_id"].astype(int)
    use_work["node_id"] = use_work["node_id"].astype(int)

    home_idx = len(road_map)
    work_idx = len(road_map) + 1
    ci_h = np.full(len(use_home), home_idx, dtype=int)
    cj_h = use_home["node_id"].map(road_map).to_numpy(dtype=int)
    cw_h = pd.to_numeric(use_home["connector_time_min"], errors="coerce").to_numpy(dtype=float)

    ci_w = use_work["node_id"].map(road_map).to_numpy(dtype=int)
    cj_w = np.full(len(use_work), work_idx, dtype=int)
    cw_w = pd.to_numeric(use_work["connector_time_min"], errors="coerce").to_numpy(dtype=float)

    n_total = len(road_map) + 2
    rows = np.concatenate([ei, ci_h, ci_w])
    cols = np.concatenate([ej, cj_h, cj_w])
    weights = np.concatenate([ew, cw_h, cw_w])
    graph = coo_matrix((weights, (rows, cols)), shape=(n_total, n_total)).tocsr()

    idx_to_label: dict[int, tuple[str, str | int]] = {}
    for nid, idx in road_map.items():
        idx_to_label[idx] = ("road", int(nid))
    idx_to_label[home_idx] = ("home", "HOME")
    idx_to_label[work_idx] = ("work", "WORK")

    edge_lookup = road_edges.set_index(["from_node_id", "to_node_id"])
    return graph, road_map, idx_to_label, edge_lookup, use_home.reset_index(drop=True), use_work.reset_index(drop=True), home_idx, work_idx


def reconstruct_node_path(predecessors: np.ndarray, start_idx: int, end_idx: int) -> list[int]:
    path = [int(end_idx)]
    cur = int(end_idx)
    while cur != int(start_idx):
        prev = int(predecessors[cur])
        if prev < 0:
            return []
        path.append(prev)
        cur = prev
    path.reverse()
    return path


def decode_path(
    path_idx: list[int],
    idx_to_label: dict[int, tuple[str, str | int]],
    edge_lookup,
    home_conn: pd.DataFrame,
    work_conn: pd.DataFrame,
    nodes_xy: pd.DataFrame,
):
    road_rows = []
    connector_rows = []
    node_xy = nodes_xy.set_index("node_id")[["x_m", "y_m"]]
    for a, b in zip(path_idx[:-1], path_idx[1:]):
        ta, ida = idx_to_label[a]
        tb, idb = idx_to_label[b]
        if ta == "road" and tb == "road":
            row = edge_lookup.loc[(int(ida), int(idb))]
            if isinstance(row, pd.DataFrame):
                row = row.iloc[0]
            road_rows.append(row)
        elif ta == "home" and tb == "road":
            row = home_conn[home_conn["node_id"].astype(int).eq(int(idb))].sort_values(
                ["connector_time_min", "connector_rank"]
            )
            if row.empty:
                continue
            row = row.iloc[0]
            connector_rows.append(
                {
                    "grid_id": str(row["grid_id"]),
                    "node_id": int(idb),
                    "connector_rank": int(row["connector_rank"]),
                    "connector_dist_m": float(row["connector_dist_m"]),
                    "connector_time_min": float(row["connector_time_min"]),
                    "geometry": LineString([(float(row["cx"]), float(row["cy"])), (float(node_xy.loc[int(idb), "x_m"]), float(node_xy.loc[int(idb), "y_m"]))]),
                }
            )
        elif ta == "road" and tb == "work":
            row = work_conn[work_conn["node_id"].astype(int).eq(int(ida))].sort_values(
                ["connector_time_min", "connector_rank"]
            )
            if row.empty:
                continue
            row = row.iloc[0]
            connector_rows.append(
                {
                    "grid_id": str(row["grid_id"]),
                    "node_id": int(ida),
                    "connector_rank": int(row["connector_rank"]),
                    "connector_dist_m": float(row["connector_dist_m"]),
                    "connector_time_min": float(row["connector_time_min"]),
                    "geometry": LineString([(float(node_xy.loc[int(ida), "x_m"]), float(node_xy.loc[int(ida), "y_m"])), (float(row["cx"]), float(row["cy"]))]),
                }
            )
    road_gdf = gpd.GeoDataFrame(pd.DataFrame(road_rows), geometry="geometry", crs=3857) if road_rows else gpd.GeoDataFrame(columns=["geometry"], geometry="geometry", crs=3857)
    conn_gdf = gpd.GeoDataFrame(pd.DataFrame(connector_rows), geometry="geometry", crs=3857) if connector_rows else gpd.GeoDataFrame(columns=["geometry"], geometry="geometry", crs=3857)
    return road_gdf, conn_gdf


def build_local_window(grid_pair: gpd.GeoDataFrame, route_edges: gpd.GeoDataFrame, connectors: gpd.GeoDataFrame, buffer_m: float):
    pieces = [geom for geom in grid_pair.geometry]
    if not route_edges.empty:
        pieces.extend(route_edges.geometry.tolist())
    if not connectors.empty:
        pieces.extend(connectors.geometry.tolist())
    union = gpd.GeoSeries(pieces, crs=3857).union_all()
    return union.buffer(buffer_m)


def sample_pairs(pair_df: pd.DataFrame, n_samples: int, seed: int) -> pd.DataFrame:
    use = pair_df[np.isfinite(pair_df["raw_connected_travel_time_min"]) & pair_df["raw_connected_travel_time_min"].gt(0)].copy()
    if len(use) < n_samples:
        n_samples = len(use)
    return use.sample(n=n_samples, random_state=seed).sort_values(["home_grid", "work_grid"]).reset_index(drop=True)


def load_or_sample_pairs(pair_df: pd.DataFrame, n_samples: int, seed: int, pair_list_csv: str | None) -> pd.DataFrame:
    if pair_list_csv:
        forced = pd.read_csv(pair_list_csv)
        forced["home_grid"] = forced["home_grid"].astype("string")
        forced["work_grid"] = forced["work_grid"].astype("string")
        keep_cols = ["home_grid", "work_grid"]
        merged = forced[keep_cols].merge(pair_df, on=keep_cols, how="left")
        return merged.sort_values(["home_grid", "work_grid"]).reset_index(drop=True)
    return sample_pairs(pair_df, n_samples=n_samples, seed=seed)


def plot_case(path: Path, case_row: pd.Series, grid_pair: gpd.GeoDataFrame, local_roads: gpd.GeoDataFrame, route_edges: gpd.GeoDataFrame, route_connectors: gpd.GeoDataFrame):
    fig, ax = plt.subplots(figsize=(9, 9), dpi=220)
    if not local_roads.empty:
        local_roads.plot(ax=ax, color="#bdbdbd", linewidth=0.35, alpha=0.85, rasterized=True)
    grid_pair.boundary.plot(ax=ax, color="#444444", linewidth=1.1, alpha=0.95)
    grid_pair.plot(ax=ax, color=["#fdd0a2", "#9ecae1"], edgecolor="#444444", linewidth=0.9, alpha=0.20)
    if not route_edges.empty:
        route_edges.plot(ax=ax, color="#08519c", linewidth=2.2, alpha=0.95, rasterized=True)
    if not route_connectors.empty:
        route_connectors.plot(ax=ax, color="#e6550d", linewidth=2.0, alpha=0.95, linestyle="--", rasterized=True)

    cent = grid_pair.geometry.centroid
    for gid, x, y in zip(grid_pair["grid_id"], cent.x, cent.y):
        ax.text(x, y, str(gid), fontsize=9, ha="center", va="center", color="#222222")

    title = (
        f"{case_row['home_grid']} -> {case_row['work_grid']}\n"
        f"raw_connected_tt={case_row['raw_connected_travel_time_min']:.2f} min, "
        f"old_tt={case_row['old_travel_time_min']:.2f} min"
    )
    ax.set_title(title)
    ax.set_axis_off()
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def main():
    args = parse_args()
    output_root = Path(args.output_root)
    out_dir = Path(args.out_dir) if args.out_dir else output_root / f"route_audit_{args.grid_type}_{args.period}"
    out_dir.mkdir(parents=True, exist_ok=True)

    config = load_run_config(output_root)
    grid = load_grid(config, args.grid_type)
    pair_df, connectors, nodes, road_edges = load_inputs(output_root, args.grid_type, args.period)
    sampled = load_or_sample_pairs(pair_df, args.n_samples, args.seed, args.pair_list_csv)
    nodes_xy = nodes[["node_id", "x_m", "y_m"]].copy()
    summary_rows = []

    for case_idx, case_row in sampled.iterrows():
        home_grid = str(case_row["home_grid"])
        work_grid = str(case_row["work_grid"])
        home_conn = connectors[connectors["grid_id"].eq(home_grid)].copy()
        work_conn = connectors[connectors["grid_id"].eq(work_grid)].copy()
        if home_conn.empty or work_conn.empty:
            continue
        graph, road_map, idx_to_label, edge_lookup, use_home, use_work, start_idx, end_idx = build_pairwise_graph(
            nodes, road_edges, home_conn, work_conn
        )

        _, predecessors = shortest_path(
            graph,
            directed=True,
            indices=[start_idx],
            return_predecessors=True,
            method="D",
        )
        pred = predecessors[0]
        node_path = reconstruct_node_path(pred, start_idx, end_idx)
        if not node_path:
            continue

        route_edges, route_connectors = decode_path(
            node_path, idx_to_label, edge_lookup, use_home, use_work, nodes_xy
        )
        grid_pair = grid[grid["grid_id"].isin([home_grid, work_grid])].copy()
        window = build_local_window(grid_pair, route_edges, route_connectors, args.buffer_m)
        local_roads = road_edges[road_edges.geometry.intersects(window)].copy()
        grid_local = grid_pair.copy()

        fig_name = f"{case_idx + 1:02d}_{home_grid}_to_{work_grid}.png"
        plot_case(out_dir / fig_name, case_row, grid_local, local_roads, route_edges, route_connectors)

        route_road_time = float(route_edges["travel_time_min"].sum()) if "travel_time_min" in route_edges.columns and len(route_edges) else 0.0
        route_conn_time = float(route_connectors["connector_time_min"].sum()) if "connector_time_min" in route_connectors.columns and len(route_connectors) else 0.0
        summary_rows.append(
            {
                "figure_file": fig_name,
                "home_grid": home_grid,
                "work_grid": work_grid,
                "raw_connected_travel_time_min": float(case_row["raw_connected_travel_time_min"]),
                "old_travel_time_min": float(case_row["old_travel_time_min"]) if np.isfinite(case_row["old_travel_time_min"]) else np.nan,
                "shared_boundary_m": float(case_row["shared_boundary_m"]) if np.isfinite(case_row["shared_boundary_m"]) else np.nan,
                "n_route_road_edges": int(len(route_edges)),
                "n_route_connectors": int(len(route_connectors)),
                "route_road_time_min": route_road_time,
                "route_connector_time_min": route_conn_time,
                "home_connector_rank_used": int(route_connectors.iloc[0]["connector_rank"]) if len(route_connectors) else np.nan,
                "work_connector_rank_used": int(route_connectors.iloc[-1]["connector_rank"]) if len(route_connectors) else np.nan,
            }
        )

    summary = pd.DataFrame(summary_rows)
    summary.to_csv(out_dir / "sampled_adjacent_route_summary.csv", index=False)
    sampled.to_csv(out_dir / "sampled_adjacent_pairs.csv", index=False)
    (out_dir / "run_note.txt").write_text(
        (
            f"grid_type={args.grid_type}\n"
            f"period={args.period}\n"
            f"n_samples={args.n_samples}\n"
            f"seed={args.seed}\n"
            f"buffer_m={args.buffer_m}\n"
            f"pair_list_csv={args.pair_list_csv or ''}\n"
            "sampling_frame=adjacent pairs with finite raw_connected_travel_time_min\n"
            "routing_logic=pair_specific_virtual_home_work_nodes\n"
        ),
        encoding="utf-8",
    )
    print(f"[sample-raw-connected-adjacent-route-audit] wrote outputs to {out_dir}")


if __name__ == "__main__":
    main()
