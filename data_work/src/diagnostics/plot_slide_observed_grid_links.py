from __future__ import annotations

import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.collections import LineCollection


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DATA_WORK = PROJECT_ROOT / "data_work"
sys.path.insert(0, str(DATA_WORK))

from src.model.travel_cost_bundle.plot_descriptive_audit import (  # noqa: E402
    _centroids,
    _grid_gdf,
    _load,
    _prep_edges,
)


VERSION_ROOT = DATA_WORK / "outputs" / "model_input_edgeproj_v2_lcc_gate5_v1"
OUTPUT_STEM = PROJECT_ROOT / "Documents" / "PreSlides" / "figs" / "app_edgeproj_adjacent_m_obs_slide"


def main() -> None:
    _, edges, _ = _load(VERSION_ROOT, "square")
    centroids = _centroids()
    edge_data = _prep_edges(edges, centroids, gate=None).dropna(subset=["tau_ep"]).copy()
    grid = _grid_gdf()

    lcc_ids = set(edge_data["grid_o"]) | set(edge_data["grid_d"])
    lcc_grid = grid.loc[grid["grid_id"].isin(lcc_ids)].copy()
    xmin, ymin, xmax, ymax = lcc_grid.total_bounds
    pad_x = 0.025 * (xmax - xmin)
    pad_y = 0.025 * (ymax - ymin)

    segments = []
    values = []
    for row in edge_data.itertuples(index=False):
        if row.grid_o not in centroids or row.grid_d not in centroids:
            continue
        segments.append([centroids[row.grid_o], centroids[row.grid_d]])
        values.append(float(row.tau_ep))

    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "font.size": 10,
            "axes.linewidth": 0.8,
            "figure.facecolor": "white",
            "axes.facecolor": "white",
            "pdf.fonttype": 42,
            "ps.fonttype": 42,
        }
    )
    fig, ax = plt.subplots(figsize=(8.2, 5.0), dpi=300)
    lcc_grid.plot(ax=ax, color="#f4f4f4", edgecolor="white", linewidth=0.18, zorder=0)

    vmax = float(edge_data["tau_ep"].quantile(0.95))
    norm = plt.Normalize(vmin=0.0, vmax=max(vmax, 1.0))
    collection = LineCollection(
        segments,
        cmap=plt.cm.magma_r,
        norm=norm,
        linewidths=1.05,
        capstyle="round",
        zorder=2,
    )
    collection.set_array(np.asarray(values, dtype=float))
    ax.add_collection(collection)

    ax.set_xlim(xmin - pad_x, xmax + pad_x)
    ax.set_ylim(ymin - pad_y, ymax + pad_y)
    ax.set_aspect("equal")
    ax.set_axis_off()
    ax.text(
        0.015,
        0.985,
        f"{len(segments):,} directed grid-links",
        transform=ax.transAxes,
        ha="left",
        va="top",
        fontsize=10,
        color="#30343b",
        bbox={"boxstyle": "round,pad=0.22", "fc": "white", "ec": "#d7d9dc", "lw": 0.6, "alpha": 0.94},
    )

    colorbar = fig.colorbar(
        collection,
        ax=ax,
        orientation="horizontal",
        fraction=0.055,
        pad=0.025,
        aspect=35,
    )
    colorbar.set_label(r"Physical link time $m_{kl}^{obs}$ (minutes)", labelpad=3)
    colorbar.outline.set_linewidth(0.7)
    colorbar.ax.tick_params(length=2.5, width=0.7, labelsize=9)

    OUTPUT_STEM.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUTPUT_STEM.with_suffix(".pdf"), bbox_inches="tight", pad_inches=0.02)
    fig.savefig(OUTPUT_STEM.with_suffix(".png"), dpi=600, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig)


if __name__ == "__main__":
    main()
