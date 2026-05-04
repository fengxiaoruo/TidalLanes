"""
Summarize Spec C spatial responses by distance to the model center and
draw treated grid links for the manuscript.

Outputs:
  Replication_apply/results/tidal_lane/specC_spatial_response_by_distance.csv
  Replication_apply/results/tidal_lane/specC_node_population_changes.csv
  Replication_apply/results/tidal_lane/specC_spatial_response_summary.csv
  Documents/L3_figs/fig4_treated_grid_links.{pdf,png}
  Documents/L3_figs/fig5_resident_change_by_distance.{pdf,png}
  Documents/L3_figs/fig6_worker_change_by_distance.{pdf,png}
"""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.collections import LineCollection
import numpy as np
import pandas as pd
import shapely
from statsmodels.nonparametric.smoothers_lowess import lowess


PROJECT_ROOT = Path(__file__).resolve().parents[2]
REPL_ROOT = PROJECT_ROOT / "Replication_apply"
RESULTS_DIR = REPL_ROOT / "results" / "tidal_lane"
FIG_DIR = PROJECT_ROOT / "Documents" / "L3_figs"
GRID_PATH = PROJECT_ROOT / "data_work" / "outputs" / "manual_centerline_rules_v11" / "data" / "grid_square_master.parquet"
NODE_PATH = REPL_ROOT / "aa_input_square_v2" / "node_lr_lf_seattle.csv"
CHI_PATH = RESULTS_DIR / "chi_lr_lf_specC.csv"
SELECTED_PATH = RESULTS_DIR / "selected_pairs_specC.csv"


def load_grid() -> gpd.GeoDataFrame:
    grid = pd.read_parquet(GRID_PATH)
    geom = shapely.from_wkb(grid["geometry"].to_numpy())
    return gpd.GeoDataFrame(grid.drop(columns=["geometry"]), geometry=geom, crs="EPSG:4326")


def load_nodes(grid: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    node = pd.read_csv(
        NODE_PATH,
        header=None,
        names=["node_aa", "l_R", "l_F", "x_placeholder", "y_placeholder"],
    )
    chi = pd.read_csv(CHI_PATH)
    out = chi.merge(node[["node_aa", "l_R", "l_F"]], on="node_aa", how="left")
    out = out.merge(grid[["grid_id", "geometry"]], on="grid_id", how="left")
    out = gpd.GeoDataFrame(out, geometry="geometry", crs=grid.crs)
    out["l_R_new"] = out["l_R"] * out["l_r_hat"]
    out["l_F_new"] = out["l_F"] * out["l_f_hat"]
    out["lr_pct"] = (out["l_r_hat"] - 1.0) * 100.0
    out["lf_pct"] = (out["l_f_hat"] - 1.0) * 100.0
    return out


def add_distance_to_center(nodes: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    proj = nodes.to_crs("EPSG:32650")
    active_union = proj.geometry.union_all()
    center = active_union.centroid
    proj["dist_center_km"] = proj.geometry.representative_point().distance(center) / 1000.0
    out = pd.DataFrame(proj.drop(columns="geometry"))
    out["delta_residents"] = out["l_R_new"] - out["l_R"]
    out["delta_workers"] = out["l_F_new"] - out["l_F"]
    return out


def weighted_mean(x: pd.Series, w: pd.Series) -> float:
    return float((x * w).sum() / w.sum())


def summarize_distance(nodes: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    out_rows = []
    for group_name, base_col, new_col, pct_col in [
        ("Residents", "l_R", "l_R_new", "lr_pct"),
        ("Workers", "l_F", "l_F_new", "lf_pct"),
    ]:
        before = weighted_mean(nodes["dist_center_km"], nodes[base_col])
        after = weighted_mean(nodes["dist_center_km"], nodes[new_col])
        out_rows.append(
            {
                "group": group_name,
                "baseline_mean_dist_km": before,
                "counterfactual_mean_dist_km": after,
                "change_km": after - before,
                "change_m": (after - before) * 1000.0,
                "weighted_mean_hat": weighted_mean(nodes[pct_col], nodes[base_col]),
                "weighted_p10_hat_pct": nodes[pct_col].quantile(0.10),
                "weighted_p90_hat_pct": nodes[pct_col].quantile(0.90),
            }
        )
    summary = pd.DataFrame(out_rows)

    bins = pd.qcut(nodes["dist_center_km"], 3, labels=["inner", "middle", "outer"])
    nodes = nodes.copy()
    nodes["distance_bin"] = bins
    bin_rows = []
    for group_name, base_col, pct_col in [
        ("Residents", "l_R", "lr_pct"),
        ("Workers", "l_F", "lf_pct"),
    ]:
        for label, sub in nodes.groupby("distance_bin", observed=True):
            bin_rows.append(
                {
                    "group": group_name,
                    "distance_bin": str(label),
                    "mean_dist_km": weighted_mean(sub["dist_center_km"], sub[base_col]),
                    "weighted_mean_change_pct": weighted_mean(sub[pct_col], sub[base_col]),
                    "node_count": int(len(sub)),
                }
            )
    by_bin = pd.DataFrame(bin_rows)
    return summary, by_bin


def plot_treated_grid_links(grid: gpd.GeoDataFrame) -> None:
    selected = pd.read_csv(SELECTED_PATH)
    grid_proj = grid.to_crs("EPSG:32650")
    point = grid_proj.copy()
    point["pt"] = point.geometry.representative_point()
    xy = {r.grid_id: (r.pt.x, r.pt.y) for r in point[["grid_id", "pt"]].itertuples(index=False)}

    segs = []
    colors = []
    widths = []
    for row in selected.itertuples(index=False):
        go = str(row.grid_o)
        gd = str(row.grid_d)
        if go not in xy or gd not in xy:
            continue
        if row.slow_dir == "ij":
            start, end = go, gd
        else:
            start, end = gd, go
        segs.append([xy[start], xy[end]])
        colors.append(float(row.asym_ratio))
        widths.append(1.0 + 2.6 * (float(row.asym_ratio) / selected["asym_ratio"].max()))

    fig, ax = plt.subplots(figsize=(6.8, 7.2), constrained_layout=True)
    grid_proj.boundary.plot(ax=ax, color="#e8e8e8", linewidth=0.12, zorder=0)
    lc = LineCollection(segs, array=pd.Series(colors).to_numpy(), cmap="magma_r", linewidths=widths, alpha=0.92, zorder=3)
    ax.add_collection(lc)
    cbar = fig.colorbar(lc, ax=ax, shrink=0.72)
    cbar.set_label("Observed AM asymmetry ratio")
    ax.set_axis_off()
    ax.autoscale()
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    fig.savefig(FIG_DIR / "fig4_treated_grid_links.pdf")
    fig.savefig(FIG_DIR / "fig4_treated_grid_links.png", dpi=240)
    plt.close(fig)


def plot_population_change_by_distance(nodes: pd.DataFrame) -> None:
    specs = [
        ("delta_residents", "Residents", "#2166ac", "fig5_resident_change_by_distance"),
        ("delta_workers", "Workers", "#b2182b", "fig6_worker_change_by_distance"),
    ]
    for col, title, color, stem in specs:
        y = nodes[col]
        fig, ax = plt.subplots(figsize=(7.0, 4.8), constrained_layout=True)
        ax.scatter(
            nodes["dist_center_km"],
            y,
            s=12,
            color=color,
            alpha=0.40,
            linewidth=0,
        )
        fit_df = nodes[["dist_center_km", col]].dropna().sort_values("dist_center_km")
        fit = lowess(fit_df[col], fit_df["dist_center_km"], frac=0.25, it=1, return_sorted=True)
        ax.plot(fit[:, 0], fit[:, 1], color="#1f1f1f", linewidth=1.8, label="LOWESS fit")
        ax.axhline(0, color="#4a4a4a", linewidth=0.8)
        ax.set_xlabel("Distance to model centre (km)")
        ax.set_ylabel("People change")
        ax.set_title(title)
        ax.grid(True, color="#eeeeee", linewidth=0.5)
        ax.legend(frameon=False, loc="upper right")
        y_abs = float(y.abs().quantile(0.995))
        if y_abs > 0:
            ax.set_ylim(-y_abs * 1.08, y_abs * 1.08)
        fig.savefig(FIG_DIR / f"{stem}.pdf")
        fig.savefig(FIG_DIR / f"{stem}.png", dpi=240)
        plt.close(fig)


def main() -> None:
    grid = load_grid()
    nodes = add_distance_to_center(load_nodes(grid))
    summary, by_bin = summarize_distance(nodes)
    summary.to_csv(RESULTS_DIR / "specC_spatial_response_summary.csv", index=False)
    by_bin.to_csv(RESULTS_DIR / "specC_spatial_response_by_distance.csv", index=False)
    nodes.to_csv(RESULTS_DIR / "specC_node_population_changes.csv", index=False)
    plot_treated_grid_links(grid)
    plot_population_change_by_distance(nodes)

    print(summary.to_string(index=False, float_format=lambda x: f"{x:.4f}"))
    print()
    print(by_bin.to_string(index=False, float_format=lambda x: f"{x:.4f}"))
    print(f"Wrote {FIG_DIR / 'fig4_treated_grid_links.pdf'}")
    print(f"Wrote {FIG_DIR / 'fig5_resident_change_by_distance.pdf'}")
    print(f"Wrote {FIG_DIR / 'fig6_worker_change_by_distance.pdf'}")


if __name__ == "__main__":
    main()
