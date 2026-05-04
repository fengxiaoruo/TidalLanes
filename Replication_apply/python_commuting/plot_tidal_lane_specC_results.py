"""
Plot the current Spec C tidal-lane counterfactual results.

This is a figure analogue to the AA highway-segment welfare maps, but for the
current Beijing square-grid Spec C experiment: a top-100 treated-pair lane
reallocation rather than a per-link 1% marginal elasticity exercise.

Inputs:
  Replication_apply/results/tidal_lane/selected_pairs_specC.csv
  Replication_apply/results/tidal_lane/chi_lr_lf_specC.csv
  data_work/outputs/manual_centerline_rules_v11/data/grid_square_master.parquet
  Replication_apply/prepare_from_data_work_square_v2/node_index_mapping_square.csv

Outputs:
  Replication_apply/results/tidal_lane/figures/specC_*.png/.pdf
  Replication_apply/results/tidal_lane/specC_plot_summary.csv
"""

from __future__ import annotations

from pathlib import Path
import re

import geopandas as gpd
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.collections import LineCollection
from matplotlib.colors import TwoSlopeNorm
import numpy as np
import pandas as pd
import shapely


SCRIPT_DIR = Path(__file__).resolve().parent
REPL_ROOT = SCRIPT_DIR.parent
ROOT = REPL_ROOT.parent

RESULTS_DIR = REPL_ROOT / "results" / "tidal_lane"
FIG_DIR = RESULTS_DIR / "figures"
GRID_PATH = ROOT / "data_work/outputs/manual_centerline_rules_v11/data/grid_square_master.parquet"
MAPPING_PATH = REPL_ROOT / "prepare_from_data_work_square_v2/node_index_mapping_square.csv"
ADJ_PATH = REPL_ROOT / "aa_input_square_v2/sparse_adjmat_seattle.csv"


def _grid_sort_key(grid_id: str) -> tuple[int, int]:
    match = re.match(r"sq_(\d+)_(\d+)$", str(grid_id))
    if not match:
        return (10**9, 10**9)
    return (int(match.group(1)), int(match.group(2)))


def load_grid() -> gpd.GeoDataFrame:
    grid = pd.read_parquet(GRID_PATH)
    geom = shapely.from_wkb(grid["geometry"].to_numpy())
    gdf = gpd.GeoDataFrame(grid.drop(columns=["geometry"]), geometry=geom, crs="EPSG:4326")
    gdf = gdf.sort_values("grid_id", key=lambda s: s.map(_grid_sort_key)).reset_index(drop=True)
    return gdf


def load_results(grid: gpd.GeoDataFrame) -> tuple[gpd.GeoDataFrame, pd.DataFrame, pd.DataFrame]:
    chi = pd.read_csv(RESULTS_DIR / "chi_lr_lf_specC.csv")
    selected = pd.read_csv(RESULTS_DIR / "selected_pairs_specC.csv")
    mapping = pd.read_csv(MAPPING_PATH)

    nodes = grid.merge(mapping[["grid_id", "node_i_aa_1based"]], on="grid_id", how="inner")
    nodes = nodes.merge(chi, left_on="node_i_aa_1based", right_on="node_aa", how="inner")
    nodes["lr_pct"] = (nodes["l_r_hat"] - 1.0) * 100.0
    nodes["lf_pct"] = (nodes["l_f_hat"] - 1.0) * 100.0

    centroids = grid.copy()
    centroids["centroid"] = centroids.geometry.representative_point()
    xy = {
        row.grid_id: (row.centroid.x, row.centroid.y)
        for row in centroids[["grid_id", "centroid"]].itertuples(index=False)
    }

    links = build_directional_shock_table(selected, xy)
    return nodes, selected, links


def build_directional_shock_table(selected: pd.DataFrame, xy: dict[str, tuple[float, float]]) -> pd.DataFrame:
    rows = []
    for _, row in selected.iterrows():
        go = str(row["grid_o"])
        gd = str(row["grid_d"])
        if go not in xy or gd not in xy:
            continue
        if row["slow_dir"] == "ij":
            slow_o, slow_d = go, gd
            fast_o, fast_d = gd, go
        else:
            slow_o, slow_d = gd, go
            fast_o, fast_d = go, gd

        rows.append(
            {
                "rank": int(row["rank"]),
                "grid_o": slow_o,
                "grid_d": slow_d,
                "role": "slow direction: cost falls",
                "tbar": float(row["tbar_slow"]),
                "shock_pct": (float(row["tbar_slow"]) - 1.0) * 100.0,
                "asym_ratio": float(row["asym_ratio"]),
                "tau_obs": float(row["tau_slow_obs"]),
                "tau_new": float(row["tau_slow_new"]),
                "x0": xy[slow_o][0],
                "y0": xy[slow_o][1],
                "x1": xy[slow_d][0],
                "y1": xy[slow_d][1],
            }
        )
        rows.append(
            {
                "rank": int(row["rank"]),
                "grid_o": fast_o,
                "grid_d": fast_d,
                "role": "fast direction: cost rises",
                "tbar": float(row["tbar_fast"]),
                "shock_pct": (float(row["tbar_fast"]) - 1.0) * 100.0,
                "asym_ratio": float(row["asym_ratio"]),
                "tau_obs": float(row["tau_fast_obs"]),
                "tau_new": float(row["tau_fast_new"]),
                "x0": xy[fast_o][0],
                "y0": xy[fast_o][1],
                "x1": xy[fast_d][0],
                "y1": xy[fast_d][1],
            }
        )
    return pd.DataFrame(rows)


def add_network_background(ax, grid: gpd.GeoDataFrame) -> None:
    adj = pd.read_csv(ADJ_PATH, header=None, names=["i", "j", "xi", "length", "tau"])
    mapping = pd.read_csv(MAPPING_PATH)
    aa_to_grid = dict(zip(mapping["node_i_aa_1based"].astype(int), mapping["grid_id"]))
    centroids = grid[["grid_id", "geometry"]].copy()
    centroids["centroid"] = centroids.geometry.representative_point()
    xy = {
        row.grid_id: (row.centroid.x, row.centroid.y)
        for row in centroids[["grid_id", "centroid"]].itertuples(index=False)
    }
    segs = []
    for row in adj.itertuples(index=False):
        go = aa_to_grid.get(int(row.i))
        gd = aa_to_grid.get(int(row.j))
        if go in xy and gd in xy:
            segs.append([xy[go], xy[gd]])
    ax.add_collection(LineCollection(segs, colors="#d0d0d0", linewidths=0.35, alpha=0.55, zorder=1))


def plot_treated_link_map(grid: gpd.GeoDataFrame, links: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(8.5, 8), constrained_layout=True)
    grid.boundary.plot(ax=ax, color="#eeeeee", linewidth=0.15, zorder=0)
    add_network_background(ax, grid)

    for role, color in [
        ("slow direction: cost falls", "#2166ac"),
        ("fast direction: cost rises", "#b2182b"),
    ]:
        sub = links[links["role"] == role]
        segs = [[(r.x0, r.y0), (r.x1, r.y1)] for r in sub.itertuples(index=False)]
        widths = 0.8 + 2.8 * (sub["shock_pct"].abs() / sub["shock_pct"].abs().max())
        ax.add_collection(LineCollection(segs, colors=color, linewidths=widths, alpha=0.82, zorder=3, label=role))

    ax.set_title("Spec C treated links: directional travel-cost shock", fontsize=13)
    ax.text(
        0.01,
        0.01,
        "Blue: cost reduction in slow direction; red: cost increase in fast direction",
        transform=ax.transAxes,
        fontsize=8.5,
        ha="left",
        va="bottom",
        bbox={"facecolor": "white", "edgecolor": "none", "alpha": 0.78, "pad": 3},
    )
    ax.set_axis_off()
    ax.autoscale()
    fig.savefig(FIG_DIR / "specC_treated_link_shock_map.png", dpi=240)
    fig.savefig(FIG_DIR / "specC_treated_link_shock_map.pdf")
    plt.close(fig)


def plot_population_map(nodes: gpd.GeoDataFrame) -> None:
    lr_abs = float(np.nanpercentile(np.abs(nodes["lr_pct"]), 99))
    lf_abs = float(np.nanpercentile(np.abs(nodes["lf_pct"]), 99))
    lr_norm = TwoSlopeNorm(vcenter=0, vmin=-lr_abs, vmax=lr_abs)
    lf_norm = TwoSlopeNorm(vcenter=0, vmin=-lf_abs, vmax=lf_abs)

    fig, axes = plt.subplots(1, 2, figsize=(12.5, 6), constrained_layout=True)
    nodes.plot(column="lr_pct", cmap="RdBu_r", norm=lr_norm, linewidth=0, ax=axes[0], legend=True)
    nodes.boundary.plot(ax=axes[0], color="#ffffff", linewidth=0.04, alpha=0.5)
    axes[0].set_title("Residents: percent change")
    axes[0].set_axis_off()

    nodes.plot(column="lf_pct", cmap="RdBu_r", norm=lf_norm, linewidth=0, ax=axes[1], legend=True)
    nodes.boundary.plot(ax=axes[1], color="#ffffff", linewidth=0.04, alpha=0.5)
    axes[1].set_title("Jobs: percent change")
    axes[1].set_axis_off()

    fig.suptitle("Spec C equilibrium population response", fontsize=13)
    fig.savefig(FIG_DIR / "specC_population_response_map.png", dpi=240)
    fig.savefig(FIG_DIR / "specC_population_response_map.pdf")
    plt.close(fig)


def plot_shock_scatter(selected: pd.DataFrame) -> None:
    df = selected.copy()
    df["slow_cost_reduction_pct"] = (1.0 - df["tbar_slow"]) * 100.0
    df["fast_cost_increase_pct"] = (df["tbar_fast"] - 1.0) * 100.0
    df["slow_tau_reduction_min"] = df["tau_slow_obs"] - df["tau_slow_new"]
    df["fast_tau_increase_min"] = df["tau_fast_new"] - df["tau_fast_obs"]

    fig, axes = plt.subplots(1, 2, figsize=(12, 5), constrained_layout=True)
    sc = axes[0].scatter(
        df["asym_ratio"],
        df["slow_cost_reduction_pct"],
        c=df["fast_cost_increase_pct"],
        s=20 + 2.0 * np.sqrt(df["tau_slow_obs"].clip(lower=0)),
        cmap="YlOrRd",
        alpha=0.86,
        edgecolor="#333333",
        linewidth=0.25,
    )
    axes[0].set_xlabel("Baseline directional travel-time asymmetry")
    axes[0].set_ylabel("Cost reduction in slow direction (%)")
    axes[0].set_title("Targeted reduction vs baseline asymmetry")
    cbar = fig.colorbar(sc, ax=axes[0])
    cbar.set_label("Cost increase in fast direction (%)")

    axes[1].scatter(df["tau_slow_obs"], df["tau_slow_new"], color="#2166ac", alpha=0.75, s=24, label="slow direction")
    axes[1].scatter(df["tau_fast_obs"], df["tau_fast_new"], color="#b2182b", alpha=0.70, s=24, label="fast direction")
    lo = min(df[["tau_slow_obs", "tau_slow_new", "tau_fast_obs", "tau_fast_new"]].min())
    hi = max(df[["tau_slow_obs", "tau_slow_new", "tau_fast_obs", "tau_fast_new"]].max())
    axes[1].plot([lo, hi], [lo, hi], color="#444444", linewidth=1, linestyle="--")
    axes[1].set_xlabel("Observed travel time (min)")
    axes[1].set_ylabel("Counterfactual travel time (min)")
    axes[1].set_title("BPR-implied travel-time changes")
    axes[1].legend(frameon=False)

    fig.suptitle("Spec C treated-pair shock diagnostics", fontsize=13)
    fig.savefig(FIG_DIR / "specC_shock_scatter.png", dpi=240)
    fig.savefig(FIG_DIR / "specC_shock_scatter.pdf")
    plt.close(fig)


def plot_combined_analogue(grid: gpd.GeoDataFrame, nodes: gpd.GeoDataFrame, selected: pd.DataFrame, links: pd.DataFrame) -> None:
    fig = plt.figure(figsize=(13.5, 10), constrained_layout=True)
    gs = fig.add_gridspec(2, 2)
    ax_map = fig.add_subplot(gs[0, 0])
    ax_pop = fig.add_subplot(gs[0, 1])
    ax_scatter = fig.add_subplot(gs[1, 0])
    ax_tau = fig.add_subplot(gs[1, 1])

    grid.boundary.plot(ax=ax_map, color="#eeeeee", linewidth=0.12, zorder=0)
    add_network_background(ax_map, grid)
    for role, color in [
        ("slow direction: cost falls", "#2166ac"),
        ("fast direction: cost rises", "#b2182b"),
    ]:
        sub = links[links["role"] == role]
        segs = [[(r.x0, r.y0), (r.x1, r.y1)] for r in sub.itertuples(index=False)]
        widths = 0.7 + 2.2 * (sub["shock_pct"].abs() / sub["shock_pct"].abs().max())
        ax_map.add_collection(LineCollection(segs, colors=color, linewidths=widths, alpha=0.82, zorder=3))
    ax_map.set_title("(A) Treated directional links")
    ax_map.set_axis_off()
    ax_map.autoscale()

    vmax = float(np.nanpercentile(np.abs(nodes["lr_pct"]), 99))
    nodes.plot(column="lr_pct", cmap="RdBu_r", norm=TwoSlopeNorm(vcenter=0, vmin=-vmax, vmax=vmax), linewidth=0, ax=ax_pop)
    nodes.boundary.plot(ax=ax_pop, color="#ffffff", linewidth=0.035, alpha=0.45)
    ax_pop.set_title("(B) Resident response (%)")
    ax_pop.set_axis_off()

    df = selected.copy()
    df["slow_cost_reduction_pct"] = (1.0 - df["tbar_slow"]) * 100.0
    df["fast_cost_increase_pct"] = (df["tbar_fast"] - 1.0) * 100.0
    ax_scatter.scatter(
        df["asym_ratio"],
        df["slow_cost_reduction_pct"],
        c=df["fast_cost_increase_pct"],
        cmap="YlOrRd",
        s=34,
        alpha=0.86,
        edgecolor="#333333",
        linewidth=0.25,
    )
    ax_scatter.set_title("(C) Shock intensity by asymmetry")
    ax_scatter.set_xlabel("Baseline asymmetry")
    ax_scatter.set_ylabel("Slow-direction cost reduction (%)")

    ax_tau.scatter(df["tau_slow_obs"], df["tau_slow_new"], color="#2166ac", alpha=0.75, s=24, label="slow")
    ax_tau.scatter(df["tau_fast_obs"], df["tau_fast_new"], color="#b2182b", alpha=0.70, s=24, label="fast")
    lo = min(df[["tau_slow_obs", "tau_slow_new", "tau_fast_obs", "tau_fast_new"]].min())
    hi = max(df[["tau_slow_obs", "tau_slow_new", "tau_fast_obs", "tau_fast_new"]].max())
    ax_tau.plot([lo, hi], [lo, hi], color="#444444", linewidth=1, linestyle="--")
    ax_tau.set_title("(D) Travel time before and after")
    ax_tau.set_xlabel("Observed time (min)")
    ax_tau.set_ylabel("Counterfactual time (min)")
    ax_tau.legend(frameon=False)

    chi = float(nodes["chi_hat"].iloc[0])
    fig.suptitle(
        f"Spec C tidal-lane counterfactual: top-100 pair reallocation, chi_hat={chi:.10f}",
        fontsize=14,
    )
    fig.savefig(FIG_DIR / "specC_results_figure5_6_analogue.png", dpi=240)
    fig.savefig(FIG_DIR / "specC_results_figure5_6_analogue.pdf")
    plt.close(fig)


def write_summary(nodes: gpd.GeoDataFrame, selected: pd.DataFrame, links: pd.DataFrame) -> None:
    chi = float(nodes["chi_hat"].iloc[0])
    summary = pd.DataFrame(
        [
            {
                "chi_hat": chi,
                "welfare_gain_ppm": (1.0 - chi) * 1e6,
                "treated_pairs": len(selected),
                "directed_treated_links": len(links),
                "asym_ratio_min": selected["asym_ratio"].min(),
                "asym_ratio_median": selected["asym_ratio"].median(),
                "asym_ratio_max": selected["asym_ratio"].max(),
                "slow_cost_reduction_pct_mean": ((1.0 - selected["tbar_slow"]) * 100.0).mean(),
                "fast_cost_increase_pct_mean": ((selected["tbar_fast"] - 1.0) * 100.0).mean(),
                "resident_pct_p1": nodes["lr_pct"].quantile(0.01),
                "resident_pct_p99": nodes["lr_pct"].quantile(0.99),
                "jobs_pct_p1": nodes["lf_pct"].quantile(0.01),
                "jobs_pct_p99": nodes["lf_pct"].quantile(0.99),
            }
        ]
    )
    summary.to_csv(RESULTS_DIR / "specC_plot_summary.csv", index=False)


def main() -> None:
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    grid = load_grid()
    nodes, selected, links = load_results(grid)
    plot_treated_link_map(grid, links)
    plot_population_map(nodes)
    plot_shock_scatter(selected)
    plot_combined_analogue(grid, nodes, selected, links)
    write_summary(nodes, selected, links)
    print(f"saved figures to {FIG_DIR}")
    print(f"saved summary to {RESULTS_DIR / 'specC_plot_summary.csv'}")


if __name__ == "__main__":
    main()
