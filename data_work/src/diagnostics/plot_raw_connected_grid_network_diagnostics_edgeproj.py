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
    parser = argparse.ArgumentParser(description="Plot source-vs-used network coverage for edge-projection connector runs.")
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--grid-type", default="square", choices=["square", "hex", "voronoi"])
    parser.add_argument("--period", default="AM", choices=["AM", "PM", "FF"])
    parser.add_argument("--raw-network-dir", default=str(DEFAULT_RAW_CONNECTED_DIR))
    parser.add_argument("--out-dir", default=None)
    return parser.parse_args()


def load_run_config(output_root: Path) -> dict:
    return json.loads((output_root / "run_config.json").read_text(encoding="utf-8"))


def load_source_edges(raw_network_dir: Path) -> gpd.GeoDataFrame:
    edges = pd.read_excel(raw_network_dir / "directed_edges.xlsx")
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
    geom = gpd.GeoSeries.from_wkt(edges["geometry"], crs="EPSG:3857", on_invalid="ignore")
    valid = geom.notna() & (~geom.is_empty)
    gdf = gpd.GeoDataFrame(edges.loc[valid].copy(), geometry=geom.loc[valid], crs="EPSG:3857")
    gdf["length_m"] = gdf.geometry.length.astype(float)
    return gdf.reset_index(drop=True)


def load_grid(config: dict, grid_type: str) -> gpd.GeoDataFrame:
    baseline_root = Path(config["baseline_root"])
    grid = gpd.read_parquet(baseline_root / "data" / f"grid_{grid_type}_master.parquet")[["grid_id", "geometry"]].copy()
    grid["grid_id"] = grid["grid_id"].astype("string")
    return grid.to_crs(3857).reset_index(drop=True)


def load_connectors(output_root: Path, grid_type: str) -> pd.DataFrame:
    path = output_root / "data" / f"raw_connected_grid_connectors_all_{grid_type}.csv"
    if not path.exists():
        path = output_root / "data" / f"raw_connected_grid_connectors_adjacent_{grid_type}.csv"
    if not path.exists():
        path = output_root / "data" / f"kunpeng_grid_connectors_all_{grid_type}.csv"
    if not path.exists():
        path = output_root / "data" / f"kunpeng_grid_connectors_adjacent_{grid_type}.csv"
    conn = pd.read_csv(path)
    conn["grid_id"] = conn["grid_id"].astype("string")
    return conn


def summarize_network(source_edges, used_edges, grid, connectors, grid_type):
    conn_dist = pd.to_numeric(connectors["connector_dist_m"], errors="coerce")
    connected = connectors["grid_id"].dropna().astype("string").nunique()
    return pd.DataFrame(
        [
            {
                "grid_type": grid_type,
                "source_directed_edges": int(len(source_edges)),
                "used_directed_edges": int(len(used_edges)),
                "used_edge_share_count": float(len(used_edges) / len(source_edges)) if len(source_edges) else np.nan,
                "source_total_length_km": float(source_edges["length_m"].sum() / 1000.0),
                "used_total_length_km": float(used_edges["length_m"].sum() / 1000.0),
                "used_length_share": float(used_edges["length_m"].sum() / source_edges["length_m"].sum()) if source_edges["length_m"].sum() > 0 else np.nan,
                "total_grids": int(len(grid)),
                "connected_grids": int(connected),
                "connected_grid_share": float(connected / len(grid)) if len(grid) else np.nan,
                "connector_rows": int(len(connectors)),
                "mean_connector_dist_m": float(conn_dist.mean()) if conn_dist.notna().any() else np.nan,
                "p90_connector_dist_m": float(conn_dist.quantile(0.90)) if conn_dist.notna().any() else np.nan,
            }
        ]
    )


def plot_source_vs_used(path, source_edges, used_edges, title):
    fig, ax = plt.subplots(figsize=(11, 11), dpi=220)
    source_edges.plot(ax=ax, color="#c7c7c7", linewidth=0.22, alpha=0.65, rasterized=True)
    used_edges.plot(ax=ax, color="#2171b5", linewidth=0.35, alpha=0.90, rasterized=True)
    ax.set_title(title)
    ax.set_axis_off()
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def plot_used_with_connectors(path, grid, used_edges, connectors, title):
    rank1 = connectors[pd.to_numeric(connectors["connector_rank"], errors="coerce").fillna(99).astype(int) == 1].copy()
    connector_lines = [
        {"geometry": LineString([(row.cx, row.cy), (row.access_x_m, row.access_y_m)])}
        for row in rank1.itertuples(index=False)
        if np.isfinite(row.cx) and np.isfinite(row.cy) and np.isfinite(row.access_x_m) and np.isfinite(row.access_y_m)
    ]
    connected_grid = grid[grid["grid_id"].isin(rank1["grid_id"])].copy()
    fig, ax = plt.subplots(figsize=(11, 11), dpi=220)
    grid.boundary.plot(ax=ax, color="#dddddd", linewidth=0.18, alpha=0.55, rasterized=True)
    used_edges.plot(ax=ax, color="#636363", linewidth=0.25, alpha=0.65, rasterized=True)
    if connector_lines:
        gpd.GeoDataFrame(connector_lines, crs=3857).plot(ax=ax, color="#e6550d", linewidth=0.28, alpha=0.55, rasterized=True)
    if not connected_grid.empty:
        connected_grid.plot(ax=ax, color="#fdd0a2", edgecolor="none", alpha=0.35, rasterized=True)
    ax.set_title(title)
    ax.set_axis_off()
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def main():
    args = parse_args()
    output_root = Path(args.output_root)
    out_dir = Path(args.out_dir) if args.out_dir else output_root / "diagnostics_network"
    out_dir.mkdir(parents=True, exist_ok=True)
    config = load_run_config(output_root)
    source_edges = load_source_edges(Path(args.raw_network_dir))
    used_edges = load_used_edges(output_root, args.period)
    connectors = load_connectors(output_root, args.grid_type)
    grid = load_grid(config, args.grid_type)
    summary = summarize_network(source_edges, used_edges, grid, connectors, args.grid_type)
    summary.to_csv(out_dir / f"network_coverage_summary_{args.grid_type}_{args.period}.csv", index=False)
    plot_source_vs_used(out_dir / f"map_source_vs_used_{args.period}.png", source_edges, used_edges, f"raw_connected_v1 source vs used directed road edges ({args.period})")
    plot_used_with_connectors(out_dir / f"map_used_with_connectors_{args.grid_type}_{args.period}.png", grid, used_edges, connectors, f"Used raw_connected_v1 road graph with edge-projection connectors: {args.grid_type} ({args.period})")
    print(f"[raw-connected-edgeproj-network-diagnostics] wrote outputs to {out_dir}")


if __name__ == "__main__":
    main()
