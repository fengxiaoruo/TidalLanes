"""
Step 5 for the standalone raw-topology prototype:
- export GIS layers for manual review
- generate overview and suspicious-node atlas figures
"""

from __future__ import annotations

import argparse
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt

from src.raw_topology.utils import ensure_output_dirs


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT = ROOT / "outputs" / "raw_topology_mvp"


def parse_args():
    parser = argparse.ArgumentParser(description="Export GIS and figures for topology review")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT), help="Prototype output directory")
    parser.add_argument("--atlas-count", type=int, default=12, help="Number of suspicious nodes to include in the atlas")
    parser.add_argument("--context-radius", type=float, default=600.0, help="Atlas panel radius in meters")
    return parser.parse_args()


def export_gpkg(paths: Path):
    data_dir = paths / "data"
    gpkg = data_dir / "raw_topology_review.gpkg"
    suspicious_all = gpd.read_parquet(data_dir / "raw_topology_suspicious_nodes.parquet")
    suspicious_high = gpd.read_parquet(data_dir / "raw_topology_suspicious_nodes_high.parquet")
    suspicious_review = gpd.read_parquet(data_dir / "raw_topology_suspicious_nodes_review.parquet")
    layers = {
        "edges_base": gpd.read_parquet(data_dir / "raw_topology_edges.parquet"),
        "edges_refined": gpd.read_parquet(data_dir / "raw_topology_edges_refined.parquet"),
        "nodes_base": gpd.read_parquet(data_dir / "raw_topology_nodes.parquet"),
        "nodes_refined": gpd.read_parquet(data_dir / "raw_topology_nodes_refined.parquet"),
        "suspicious_nodes": suspicious_all,
        "suspicious_high": suspicious_high,
        "suspicious_review": suspicious_review,
        "suspicious_edge_context": gpd.read_parquet(data_dir / "raw_topology_suspicious_edge_context.parquet"),
    }
    if gpkg.exists():
        gpkg.unlink()
    for layer_name, gdf in layers.items():
        gdf.to_file(gpkg, layer=layer_name, driver="GPKG")
    return gpkg


def plot_overview(output_root: Path):
    data_dir = output_root / "data"
    fig_dir = output_root / "figures"
    fig_dir.mkdir(parents=True, exist_ok=True)
    edges_base = gpd.read_parquet(data_dir / "raw_topology_edges.parquet")
    edges_refined = gpd.read_parquet(data_dir / "raw_topology_edges_refined.parquet")
    suspicious_high = gpd.read_parquet(data_dir / "raw_topology_suspicious_nodes_high.parquet")
    suspicious_review = gpd.read_parquet(data_dir / "raw_topology_suspicious_nodes_review.parquet")

    fig, ax = plt.subplots(figsize=(10, 10))
    edges_base.plot(ax=ax, color="#c7ced6", linewidth=0.15, alpha=0.6)
    edges_refined.plot(ax=ax, color="#234d70", linewidth=0.15, alpha=0.45)
    if not suspicious_review.empty:
        suspicious_review.plot(ax=ax, color="#f4a259", markersize=7, alpha=0.75)
    if not suspicious_high.empty:
        suspicious_high.plot(ax=ax, color="#d1495b", markersize=8, alpha=0.9)
    ax.set_title("Raw topology conservative repair overview\nred=auto break, orange=review only")
    ax.set_axis_off()
    fig.tight_layout()
    path = fig_dir / "raw_topology_overview.png"
    fig.savefig(path, dpi=220, bbox_inches="tight")
    plt.close(fig)
    return path


def plot_atlas(output_root: Path, atlas_count: int, context_radius: float):
    data_dir = output_root / "data"
    fig_dir = output_root / "figures"
    fig_dir.mkdir(parents=True, exist_ok=True)
    edges_base = gpd.read_parquet(data_dir / "raw_topology_edges.parquet")
    suspicious = gpd.read_parquet(data_dir / "raw_topology_suspicious_nodes.parquet")
    if suspicious.empty:
        return None

    suspicious = suspicious.sort_values(["confidence", "component_id", "roadtype_a", "roadtype_b"], ascending=[True, True, True, True]).head(atlas_count).copy()
    n = len(suspicious)
    ncols = 3
    nrows = (n + ncols - 1) // ncols
    fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(14, 4.8 * nrows))
    axes = axes.flatten()

    for ax in axes[n:]:
        ax.axis("off")

    for ax, row in zip(axes, suspicious.itertuples(index=False)):
        x = float(row.geometry.x)
        y = float(row.geometry.y)
        bbox = (x - context_radius, y - context_radius, x + context_radius, y + context_radius)
        local = edges_base.cx[bbox[0]:bbox[2], bbox[1]:bbox[3]]
        local.plot(ax=ax, color="#bbc4ce", linewidth=0.8)
        local_ctx = local.loc[(local["u"] == row.node_id) | (local["v"] == row.node_id)].copy()
        if not local_ctx.empty:
            local_ctx.plot(ax=ax, color="#234d70", linewidth=1.4)
        gpd.GeoSeries([row.geometry], crs=suspicious.crs).plot(ax=ax, color="#d1495b", markersize=20)
        ax.set_xlim(bbox[0], bbox[2])
        ax.set_ylim(bbox[1], bbox[3])
        ax.set_axis_off()
        ax.set_title(
            f"{row.confidence} | node {row.node_id} | comp {row.component_id}\n"
            f"{row.roadtype_a}-{row.roadtype_b} | "
            f"{row.straight_angle_a:.0f}/{row.straight_angle_b:.0f}",
            fontsize=9,
        )

    fig.tight_layout()
    path = fig_dir / "raw_topology_suspicious_atlas.png"
    fig.savefig(path, dpi=220, bbox_inches="tight")
    plt.close(fig)
    return path


def main():
    args = parse_args()
    output_root = Path(args.output_root)
    paths = ensure_output_dirs(output_root)
    gpkg = export_gpkg(output_root)
    overview = plot_overview(output_root)
    atlas = plot_atlas(output_root, args.atlas_count, args.context_radius)
    print(f"GPKG: {gpkg}")
    print(f"Overview: {overview}")
    print(f"Atlas: {atlas}")


if __name__ == "__main__":
    main()
