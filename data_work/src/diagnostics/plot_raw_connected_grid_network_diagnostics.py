from __future__ import annotations

import argparse
import json
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from shapely.geometry import LineString


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RAW_CONNECTED_DIR = ROOT / "src" / "raw_connected_v1" / "Processed_Data"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Plot source-vs-used raw_connected_v1 road-network coverage for raw-grid travel-cost runs."
    )
    parser.add_argument("--output-root", required=True, help="Output root from build_raw_connected_v1_grid_travel_time.py.")
    parser.add_argument("--grid-type", default="square", choices=["square", "hex", "voronoi"])
    parser.add_argument("--period", default="AM", choices=["AM", "PM", "FF"])
    parser.add_argument("--raw-network-dir", default=str(DEFAULT_RAW_CONNECTED_DIR))
    parser.add_argument("--out-dir", default=None, help="Defaults to <output-root>/diagnostics_network.")
    return parser.parse_args()


def load_run_config(output_root: Path) -> dict:
    return json.loads((output_root / "run_config.json").read_text(encoding="utf-8"))


def load_source_edges(raw_network_dir: Path) -> gpd.GeoDataFrame:
    edges = pd.read_excel(raw_network_dir / "directed_edges.xlsx")
    edges["edge_id"] = edges["edge_id"].astype("string")
    geom = gpd.GeoSeries.from_wkt(edges["geometry"], crs="EPSG:4326", on_invalid="ignore")
    valid = geom.notna() & (~geom.is_empty)
    gdf = gpd.GeoDataFrame(edges.loc[valid].copy(), geometry=geom.loc[valid], crs="EPSG:4326").to_crs(3857)
    gdf["length_m"] = gdf.geometry.length.astype(float)
    return gdf.reset_index(drop=True)


def load_used_edges(output_root: Path, period: str) -> gpd.GeoDataFrame:
    path = output_root / "data" / f"raw_connected_road_edges_{period}.csv"
    if not path.exists():
        path = output_root / "data" / f"kunpeng_road_edges_{period}.csv"
    edges = pd.read_csv(path)
    geom = gpd.GeoSeries.from_wkt(edges["geometry"], crs="EPSG:4326", on_invalid="ignore")
    valid = geom.notna() & (~geom.is_empty)
    gdf = gpd.GeoDataFrame(edges.loc[valid].copy(), geometry=geom.loc[valid], crs="EPSG:4326").to_crs(3857)
    gdf["length_m"] = gdf.geometry.length.astype(float)
    return gdf.reset_index(drop=True)


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
    raise FileNotFoundError(f"Cannot find connector file for grid_type={grid_type} under {output_root / 'data'}")


def load_connectors(output_root: Path, grid_type: str) -> pd.DataFrame:
    conn = pd.read_csv(find_connector_path(output_root, grid_type))
    for col in ["grid_id", "node_id"]:
        if col in conn.columns:
            conn[col] = conn[col].astype("string")
    return conn


def load_grid(config: dict, grid_type: str) -> gpd.GeoDataFrame:
    baseline_root = Path(config["baseline_root"])
    grid = gpd.read_parquet(baseline_root / "data" / f"grid_{grid_type}_master.parquet")[["grid_id", "geometry"]].copy()
    grid["grid_id"] = grid["grid_id"].astype("string")
    return grid.to_crs(3857).reset_index(drop=True)


def summarize_network(source_edges: gpd.GeoDataFrame, used_edges: gpd.GeoDataFrame, used_nodes: pd.DataFrame, grid: gpd.GeoDataFrame, connectors: pd.DataFrame, grid_type: str) -> pd.DataFrame:
    connected_grid_ids = connectors["grid_id"].dropna().astype("string").unique().tolist()
    conn_dist = pd.to_numeric(connectors["connector_dist_m"], errors="coerce")
    out = pd.DataFrame(
        [
            {
                "grid_type": grid_type,
                "source_directed_edges": int(len(source_edges)),
                "used_directed_edges": int(len(used_edges)),
                "used_edge_share_count": float(len(used_edges) / len(source_edges)) if len(source_edges) else np.nan,
                "source_total_length_km": float(source_edges["length_m"].sum() / 1000.0),
                "used_total_length_km": float(used_edges["length_m"].sum() / 1000.0),
                "used_length_share": float(used_edges["length_m"].sum() / source_edges["length_m"].sum()) if source_edges["length_m"].sum() > 0 else np.nan,
                "usable_road_nodes": int(len(used_nodes)),
                "source_unique_nodes_in_edges": int(len(pd.unique(pd.concat([source_edges["from_node_id"], source_edges["to_node_id"]], ignore_index=True)))),
                "used_unique_nodes_in_edges": int(len(pd.unique(pd.concat([used_edges["from_node_id"], used_edges["to_node_id"]], ignore_index=True)))),
                "total_grids": int(len(grid)),
                "connected_grids": int(len(connected_grid_ids)),
                "connected_grid_share": float(len(connected_grid_ids) / len(grid)) if len(grid) else np.nan,
                "connector_rows": int(len(connectors)),
                "mean_connector_dist_m": float(conn_dist.mean()) if conn_dist.notna().any() else np.nan,
                "p90_connector_dist_m": float(conn_dist.quantile(0.90)) if conn_dist.notna().any() else np.nan,
            }
        ]
    )
    return out


def plot_source_vs_used(path: Path, source_edges: gpd.GeoDataFrame, used_edges: gpd.GeoDataFrame, title: str) -> None:
    fig, ax = plt.subplots(figsize=(11, 11), dpi=220)
    source_edges.plot(ax=ax, color="#c7c7c7", linewidth=0.22, alpha=0.65, rasterized=True)
    used_edges.plot(ax=ax, color="#2171b5", linewidth=0.35, alpha=0.90, rasterized=True)
    ax.set_title(title)
    ax.set_axis_off()
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def plot_used_with_connectors(path: Path, grid: gpd.GeoDataFrame, used_edges: gpd.GeoDataFrame, connectors: pd.DataFrame, title: str) -> None:
    grid = grid.copy()
    conn = connectors.copy()
    conn["grid_id"] = conn["grid_id"].astype("string")
    rank1 = conn[pd.to_numeric(conn["connector_rank"], errors="coerce").fillna(99).astype(int) == 1].copy()
    connected_grid = grid[grid["grid_id"].isin(rank1["grid_id"])].copy()
    if "cx" not in rank1.columns or "cy" not in rank1.columns:
        cent = connected_grid.geometry.centroid
        connected_grid["cx"] = cent.x
        connected_grid["cy"] = cent.y
        rank1 = rank1.merge(connected_grid[["grid_id", "cx", "cy"]], on="grid_id", how="left")

    cent = connected_grid.geometry.centroid
    connected_grid["cx"] = cent.x
    connected_grid["cy"] = cent.y

    connector_lines = []
    for row in rank1.itertuples(index=False):
        if not (np.isfinite(row.cx) and np.isfinite(row.cy) and np.isfinite(row.node_x_m) and np.isfinite(row.node_y_m)):
            continue
        connector_lines.append(
            {
                "geometry": LineString([(row.cx, row.cy), (row.node_x_m, row.node_y_m)])
            }
        )

    fig, ax = plt.subplots(figsize=(11, 11), dpi=220)
    grid.boundary.plot(ax=ax, color="#dddddd", linewidth=0.18, alpha=0.55, rasterized=True)
    used_edges.plot(ax=ax, color="#636363", linewidth=0.25, alpha=0.65, rasterized=True)
    if connector_lines:
        gpd.GeoDataFrame(connector_lines, crs=3857).plot(ax=ax, color="#e6550d", linewidth=0.28, alpha=0.55, rasterized=True)
    if not connected_grid.empty:
        connected_grid.plot(ax=ax, color="#fdd0a2", edgecolor="none", alpha=0.35, rasterized=True)
        ax.scatter(connected_grid["cx"], connected_grid["cy"], s=4, color="#cc4c02", alpha=0.65, linewidths=0)
    ax.set_title(title)
    ax.set_axis_off()
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def attach_node_xy(connectors: pd.DataFrame, used_nodes: pd.DataFrame) -> pd.DataFrame:
    node_xy = used_nodes[["node_id", "x_m", "y_m"]].copy()
    node_xy["node_id"] = node_xy["node_id"].astype("string")
    out = connectors.copy()
    out["node_id"] = out["node_id"].astype("string")
    out = out.merge(node_xy.rename(columns={"x_m": "node_x_m", "y_m": "node_y_m"}), on="node_id", how="left")
    return out


def main():
    args = parse_args()
    output_root = Path(args.output_root)
    out_dir = Path(args.out_dir) if args.out_dir else output_root / "diagnostics_network"
    out_dir.mkdir(parents=True, exist_ok=True)

    config = load_run_config(output_root)
    source_edges = load_source_edges(Path(args.raw_network_dir))
    used_edges = load_used_edges(output_root, args.period)
    node_path = output_root / "data" / "raw_connected_road_nodes_usable.csv"
    if not node_path.exists():
        node_path = output_root / "data" / "kunpeng_road_nodes_usable.csv"
    used_nodes = pd.read_csv(node_path)
    connectors = attach_node_xy(load_connectors(output_root, args.grid_type), used_nodes)
    grid = load_grid(config, args.grid_type)

    summary = summarize_network(source_edges, used_edges, used_nodes, grid, connectors, args.grid_type)
    summary.to_csv(out_dir / f"network_coverage_summary_{args.grid_type}_{args.period}.csv", index=False)

    plot_source_vs_used(
        out_dir / f"map_source_vs_used_{args.period}.png",
        source_edges,
        used_edges,
        f"raw_connected_v1 source vs used directed road edges ({args.period})",
    )
    plot_used_with_connectors(
        out_dir / f"map_used_with_connectors_{args.grid_type}_{args.period}.png",
        grid,
        used_edges,
        connectors,
        f"Used raw_connected_v1 road graph with grid connectors: {args.grid_type} ({args.period})",
    )
    print(f"[raw-connected-network-diagnostics] wrote outputs to {out_dir}")


if __name__ == "__main__":
    main()
