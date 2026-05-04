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
from shapely.geometry import LineString, Point


def parse_args():
    parser = argparse.ArgumentParser(description="Sample adjacent grid pairs and plot local routes for edge-projection connector runs.")
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--grid-type", default="square", choices=["square", "hex", "voronoi"])
    parser.add_argument("--period", default="AM", choices=["AM", "PM", "FF"])
    parser.add_argument("--n-samples", type=int, default=20)
    parser.add_argument("--seed", type=int, default=20260421)
    parser.add_argument("--buffer-m", type=float, default=2500.0)
    parser.add_argument("--out-dir", default=None)
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


def load_inputs(output_root: Path, grid_type: str, period: str):
    pair_path = output_root / "data" / f"raw_connected_vs_old_adjacent_adjacent_{grid_type}_{period}.csv"
    if not pair_path.exists():
        pair_path = output_root / "data" / f"kunpeng_vs_old_adjacent_adjacent_{grid_type}_{period}.csv"
    pair_df = pd.read_csv(pair_path)
    pair_df["home_grid"] = pair_df["home_grid"].astype("string")
    pair_df["work_grid"] = pair_df["work_grid"].astype("string")
    if "kunpeng_travel_time_min" in pair_df.columns and "raw_connected_travel_time_min" not in pair_df.columns:
        pair_df = pair_df.rename(columns={"kunpeng_travel_time_min": "raw_connected_travel_time_min"})
    conn_path = output_root / "data" / f"raw_connected_grid_connectors_adjacent_{grid_type}.csv"
    if not conn_path.exists():
        conn_path = output_root / "data" / f"raw_connected_grid_connectors_all_{grid_type}.csv"
    if not conn_path.exists():
        conn_path = output_root / "data" / f"kunpeng_grid_connectors_adjacent_{grid_type}.csv"
    if not conn_path.exists():
        conn_path = output_root / "data" / f"kunpeng_grid_connectors_all_{grid_type}.csv"
    connectors = pd.read_csv(conn_path)
    connectors["grid_id"] = connectors["grid_id"].astype("string")
    edge_path = output_root / "data" / f"raw_connected_road_edges_{period}.csv"
    if not edge_path.exists():
        edge_path = output_root / "data" / f"kunpeng_road_edges_{period}.csv"
    road_edges = pd.read_csv(edge_path)
    geom = gpd.GeoSeries.from_wkt(road_edges["geometry"], crs="EPSG:3857", on_invalid="ignore")
    valid = geom.notna() & (~geom.is_empty)
    road_edges = gpd.GeoDataFrame(road_edges.loc[valid].copy(), geometry=geom.loc[valid], crs="EPSG:3857")
    road_edges["travel_time_min"] = pd.to_numeric(road_edges["travel_time_min"], errors="coerce")
    road_edges["length_m"] = pd.to_numeric(road_edges["length_m"], errors="coerce")
    connectors["from_node_id"] = pd.to_numeric(connectors["from_node_id"], errors="coerce").astype("Int64")
    connectors["to_node_id"] = pd.to_numeric(connectors["to_node_id"], errors="coerce").astype("Int64")
    connectors["access_fraction"] = pd.to_numeric(connectors["access_fraction"], errors="coerce")
    connectors["connector_time_min"] = pd.to_numeric(connectors["connector_time_min"], errors="coerce")
    return pair_df, connectors, road_edges


def build_road_graph(road_edges: gpd.GeoDataFrame):
    road_node_ids = sorted(
        pd.unique(pd.concat([road_edges["from_node_id"], road_edges["to_node_id"]], ignore_index=True)).tolist()
    )
    road_node_map = {int(nid): idx for idx, nid in enumerate(road_node_ids)}
    rows = road_edges["from_node_id"].map(road_node_map).to_numpy(dtype=int)
    cols = road_edges["to_node_id"].map(road_node_map).to_numpy(dtype=int)
    weights = road_edges["travel_time_min"].to_numpy(dtype=float)
    graph = coo_matrix((weights, (rows, cols)), shape=(len(road_node_map), len(road_node_map))).tocsr()
    edge_lookup = road_edges.set_index(["from_node_id", "to_node_id"])
    return graph, road_node_map, edge_lookup


def sample_pairs(pair_df: pd.DataFrame, n_samples: int, seed: int) -> pd.DataFrame:
    use = pair_df[pd.to_numeric(pair_df["raw_connected_travel_time_min"], errors="coerce").gt(0)].copy()
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


def reconstruct_path(pred_row: np.ndarray, start_idx: int, end_idx: int) -> list[int]:
    path = [int(end_idx)]
    cur = int(end_idx)
    while cur != int(start_idx):
        prev = int(pred_row[cur])
        if prev < 0:
            return []
        path.append(prev)
        cur = prev
    path.reverse()
    return path


def extract_linestring_segment(line, frac_start: float, frac_end: float) -> LineString:
    fa = min(max(float(frac_start), 0.0), 1.0)
    fb = min(max(float(frac_end), 0.0), 1.0)
    if fb < fa:
        fa, fb = fb, fa
    start_d = line.length * fa
    end_d = line.length * fb
    coords = [line.interpolate(start_d).coords[0]]
    for c in line.coords:
        d = line.project(Point(c))
        if start_d < d < end_d:
            coords.append(c)
    coords.append(line.interpolate(end_d).coords[0])
    if len(coords) < 2:
        coords = [line.interpolate(start_d).coords[0], line.interpolate(end_d).coords[0]]
    return LineString(coords)


def plot_case(path, case_row, grid_pair, local_roads, route_edges, route_connectors):
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
    pair_df, connectors, road_edges = load_inputs(output_root, args.grid_type, args.period)
    road_graph, road_node_map, edge_lookup = build_road_graph(road_edges)
    idx_to_road_node = {idx: node_id for node_id, idx in road_node_map.items()}
    sampled = load_or_sample_pairs(pair_df, args.n_samples, args.seed, args.pair_list_csv)
    summary_rows = []

    for case_idx, case_row in sampled.iterrows():
        home_grid = str(case_row["home_grid"])
        work_grid = str(case_row["work_grid"])
        home_conn = connectors[connectors["grid_id"].eq(home_grid)].copy()
        work_conn = connectors[connectors["grid_id"].eq(work_grid)].copy()
        best = None
        best_path = None
        best_home_geom = None
        best_work_geom = None
        for src_row in home_conn.itertuples(index=False):
            if pd.isna(src_row.to_node_id):
                continue
            home_node_idx = road_node_map.get(int(src_row.to_node_id))
            if home_node_idx is None:
                continue
            host_src = edge_lookup.loc[(int(src_row.from_node_id), int(src_row.to_node_id))]
            if isinstance(host_src, pd.DataFrame):
                host_src = host_src.iloc[0]
            dist, pred = shortest_path(
                road_graph,
                directed=True,
                indices=[home_node_idx],
                return_predecessors=True,
                unweighted=False,
                method="D",
            )
            dist = np.asarray(dist)[0]
            pred = np.asarray(pred)[0]
            for dst_row in work_conn.itertuples(index=False):
                if pd.isna(dst_row.from_node_id):
                    continue
                work_node_idx = road_node_map.get(int(dst_row.from_node_id))
                if work_node_idx is None:
                    continue
                host_dst = edge_lookup.loc[(int(dst_row.from_node_id), int(dst_row.to_node_id))]
                if isinstance(host_dst, pd.DataFrame):
                    host_dst = host_dst.iloc[0]
                outbound = float(src_row.connector_time_min) + float(host_src.travel_time_min) * (1.0 - float(src_row.access_fraction))
                inbound = float(host_dst.travel_time_min) * float(dst_row.access_fraction) + float(dst_row.connector_time_min)
                total = outbound + float(dist[work_node_idx]) + inbound
                if not np.isfinite(total):
                    continue
                if best is None or total < best["total"]:
                    path_nodes = reconstruct_path(pred, int(home_node_idx), int(work_node_idx))
                    if not path_nodes:
                        continue
                    best_home_geom = extract_linestring_segment(host_src.geometry, float(src_row.access_fraction), 1.0)
                    best_work_geom = extract_linestring_segment(host_dst.geometry, 0.0, float(dst_row.access_fraction))
                    best = {
                        "total": total,
                        "src": src_row,
                        "dst": dst_row,
                        "outbound": outbound,
                        "inbound": inbound,
                        "src_host_time": float(host_src.travel_time_min),
                        "src_host_length": float(host_src.length_m),
                        "dst_host_time": float(host_dst.travel_time_min),
                        "dst_host_length": float(host_dst.length_m),
                    }
                    best_path = path_nodes
        if best is None or not best_path:
            continue

        route_edge_rows = []
        for a, b in zip(best_path[:-1], best_path[1:]):
            from_node_id = idx_to_road_node[a]
            to_node_id = idx_to_road_node[b]
            row = edge_lookup.loc[(from_node_id, to_node_id)]
            if isinstance(row, pd.DataFrame):
                row = row.iloc[0]
            route_edge_rows.append(row)
        if best_home_geom is not None:
            route_edge_rows.insert(
                0,
                {
                    "from_node_id": int(best["src"].from_node_id),
                    "to_node_id": int(best["src"].to_node_id),
                    "travel_time_min": float(best["src_host_time"]) * (1.0 - float(best["src"].access_fraction)),
                    "length_m": float(best["src_host_length"]) * (1.0 - float(best["src"].access_fraction)),
                    "geometry": best_home_geom,
                },
            )
        if best_work_geom is not None:
            route_edge_rows.append(
                {
                    "from_node_id": int(best["dst"].from_node_id),
                    "to_node_id": int(best["dst"].to_node_id),
                    "travel_time_min": float(best["dst_host_time"]) * float(best["dst"].access_fraction),
                    "length_m": float(best["dst_host_length"]) * float(best["dst"].access_fraction),
                    "geometry": best_work_geom,
                }
            )
        route_edges = gpd.GeoDataFrame(pd.DataFrame(route_edge_rows), geometry="geometry", crs=3857) if route_edge_rows else gpd.GeoDataFrame(columns=["geometry"], geometry="geometry", crs=3857)
        route_connectors = gpd.GeoDataFrame(
            [
                {"geometry": LineString([(best["src"].cx, best["src"].cy), (best["src"].access_x_m, best["src"].access_y_m)]), "connector_time_min": best["src"].connector_time_min, "connector_rank": best["src"].connector_rank},
                {"geometry": LineString([(best["dst"].cx, best["dst"].cy), (best["dst"].access_x_m, best["dst"].access_y_m)]), "connector_time_min": best["dst"].connector_time_min, "connector_rank": best["dst"].connector_rank},
            ],
            geometry="geometry",
            crs=3857,
        )
        grid_pair = grid[grid["grid_id"].isin([home_grid, work_grid])].copy()
        pieces = list(grid_pair.geometry) + list(route_edges.geometry) + list(route_connectors.geometry)
        window = gpd.GeoSeries(pieces, crs=3857).union_all().buffer(args.buffer_m)
        local_roads = road_edges[road_edges.geometry.intersects(window)].copy()

        fig_name = f"{case_idx + 1:02d}_{home_grid}_to_{work_grid}.png"
        plot_case(out_dir / fig_name, case_row, grid_pair, local_roads, route_edges, route_connectors)
        summary_rows.append(
            {
                "figure_file": fig_name,
                "home_grid": home_grid,
                "work_grid": work_grid,
                "raw_connected_travel_time_min": float(case_row["raw_connected_travel_time_min"]),
                "old_travel_time_min": float(case_row["old_travel_time_min"]) if np.isfinite(case_row["old_travel_time_min"]) else np.nan,
                "shared_boundary_m": float(case_row["shared_boundary_m"]) if np.isfinite(case_row["shared_boundary_m"]) else np.nan,
                "n_route_road_edges": int(len(route_edges)),
                "n_route_connectors": 2,
                "route_road_time_min": float(route_edges["travel_time_min"].sum()) if len(route_edges) else 0.0,
                "route_connector_time_min": float(route_connectors["connector_time_min"].sum()),
                "home_connector_rank_used": int(best["src"].connector_rank),
                "work_connector_rank_used": int(best["dst"].connector_rank),
            }
        )

    pd.DataFrame(summary_rows).to_csv(out_dir / "sampled_adjacent_route_summary.csv", index=False)
    sampled.to_csv(out_dir / "sampled_adjacent_pairs.csv", index=False)
    (out_dir / "run_note.txt").write_text(
        (
            f"grid_type={args.grid_type}\n"
            f"period={args.period}\n"
            f"n_samples={args.n_samples}\n"
            f"seed={args.seed}\n"
            f"pair_list_csv={args.pair_list_csv or ''}\n"
            "connector_geometry_rule=edge_projection_access_point\n"
            "routing_logic=pair_specific_endpoint_projection\n"
        ),
        encoding="utf-8",
    )
    print(f"[sample-raw-connected-edgeproj-adjacent-route-audit] wrote outputs to {out_dir}")


if __name__ == "__main__":
    main()
