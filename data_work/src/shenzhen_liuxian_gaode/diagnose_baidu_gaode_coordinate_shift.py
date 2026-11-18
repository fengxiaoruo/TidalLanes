from __future__ import annotations

import sys
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from shapely.affinity import translate

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT / "data_work"))

from src.shenzhen_liuxian_gaode.match_baidu_underlying_segments import (  # noqa: E402
    DEFAULT_BAIDU_ROADS,
    DEFAULT_GAODE_DIR,
    DEFAULT_OUTPUT_DIR,
    configure_plot,
    load_gaode_segments,
)


def sample_points(line, n: int = 200):
    if line.is_empty:
        return []
    return [line.interpolate(i / (n - 1), normalized=True) for i in range(n)]


def estimate_translation_m(gaode_line_m, baidu_line_m) -> dict[str, float]:
    vectors = []
    before = []
    for point in sample_points(gaode_line_m):
        nearest = baidu_line_m.interpolate(baidu_line_m.project(point))
        vectors.append((nearest.x - point.x, nearest.y - point.y))
        before.append(point.distance(nearest))

    arr = np.asarray(vectors)
    dx = float(np.median(arr[:, 0]))
    dy = float(np.median(arr[:, 1]))
    shifted = translate(gaode_line_m, xoff=dx, yoff=dy)
    after = [point.distance(baidu_line_m) for point in sample_points(shifted)]
    return {
        "translation_dx_m_east": dx,
        "translation_dy_m_north": dy,
        "translation_magnitude_m": float(np.hypot(dx, dy)),
        "median_distance_before_m": float(np.median(before)),
        "median_distance_after_translation_m": float(np.median(after)),
        "mean_distance_before_m": float(np.mean(before)),
        "mean_distance_after_translation_m": float(np.mean(after)),
    }


def main() -> None:
    configure_plot()
    output_dir = DEFAULT_OUTPUT_DIR
    metrics_dir = output_dir / "metrics"
    figures_dir = output_dir / "figures"
    metrics_dir.mkdir(parents=True, exist_ok=True)
    figures_dir.mkdir(parents=True, exist_ok=True)

    gaode = load_gaode_segments(DEFAULT_GAODE_DIR)
    candidates = pd.read_csv(metrics_dir / "baidu_liuxian_underlying_candidates.csv")
    main_ids = set(candidates.loc[candidates["match_class"].eq("main_liuxian"), "roadseg_id"].astype(str))
    roads = gpd.read_file(DEFAULT_BAIDU_ROADS)
    baidu = roads[roads["roadseg_id"].astype(str).isin(main_ids)].copy()

    metric_crs = roads.estimate_utm_crs()
    gaode_m = gaode.to_crs(metric_crs)
    baidu_m = baidu.to_crs(metric_crs)

    rows = []
    shifted_parts = []
    for chain in ["eastbound_chain", "westbound_chain"]:
        g_line = gaode_m.loc[gaode_m["chain_direction"].eq(chain), "geometry"].union_all()
        b_line = baidu_m.loc[baidu_m["roadseg_id"].isin(
            set(candidates.loc[
                candidates["match_class"].eq("main_liuxian")
                & candidates["chain_direction"].eq(chain),
                "roadseg_id",
            ].astype(str))
        ), "geometry"].union_all()
        stats = estimate_translation_m(g_line, b_line)
        stats["chain_direction"] = chain
        rows.append(stats)
        for geom in gaode_m.loc[gaode_m["chain_direction"].eq(chain), "geometry"]:
            shifted_parts.append(
                {
                    "chain_direction": chain,
                    "geometry": translate(
                        geom,
                        xoff=stats["translation_dx_m_east"],
                        yoff=stats["translation_dy_m_north"],
                    ),
                }
            )

    diagnostics = pd.DataFrame(rows)
    ordered = [
        "chain_direction",
        "translation_dx_m_east",
        "translation_dy_m_north",
        "translation_magnitude_m",
        "median_distance_before_m",
        "median_distance_after_translation_m",
        "mean_distance_before_m",
        "mean_distance_after_translation_m",
    ]
    diagnostics[ordered].to_csv(
        metrics_dir / "baidu_gaode_coordinate_shift_diagnostic.csv",
        index=False,
        encoding="utf-8-sig",
    )

    shifted_gdf = gpd.GeoDataFrame(shifted_parts, geometry="geometry", crs=metric_crs)
    fig, axes = plt.subplots(1, 2, figsize=(11.0, 4.8), sharex=True, sharey=True)
    colors = {"eastbound_chain": "#c44e52", "westbound_chain": "#4c78a8"}
    for ax, shifted in [(axes[0], False), (axes[1], True)]:
        baidu_m.plot(ax=ax, color="#222222", linewidth=2.2, alpha=0.75, label="Baidu matched segments")
        for chain, color in colors.items():
            gaode_m.loc[gaode_m["chain_direction"].eq(chain)].plot(
                ax=ax,
                color=color,
                linewidth=1.2,
                linestyle="--",
                alpha=0.85,
                label=f"Gaode original {chain.replace('_chain', '')}",
            )
            if shifted:
                shifted_gdf.loc[shifted_gdf["chain_direction"].eq(chain)].plot(
                    ax=ax,
                    color=color,
                    linewidth=2.0,
                    alpha=0.95,
                    label=f"Gaode shifted {chain.replace('_chain', '')}",
                )
        ax.set_axis_off()
        ax.set_title("Original overlay" if not shifted else "After chain-specific translation", loc="left")
    axes[1].legend(loc="lower right", fontsize=7, frameon=True)
    fig.suptitle("Liuxian Avenue Baidu-Gaode coordinate shift diagnostic", x=0.02, y=0.98, ha="left")
    fig.tight_layout(rect=(0, 0, 1, 0.91))
    fig.savefig(figures_dir / "baidu_liuxian_coordinate_shift_diagnostic.png", bbox_inches="tight")
    plt.close(fig)


if __name__ == "__main__":
    main()
