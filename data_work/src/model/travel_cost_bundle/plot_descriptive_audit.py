"""
Reproducible descriptive-audit figures for a locked travel-cost bundle.

Regenerates the fig01..fig07 set under
``outputs/<version_id>/figures/descriptive_audit/`` directly from the bundle
parquets plus the grid geometry, so the maps stay in sync with the data.

An optional ``--detour-gate G`` drops adjacent edges whose routed length
exceeds ``G`` times the straight-line centroid distance; this is used to
illustrate the detour-mitigation experiment (figures get a ``_gateG`` suffix).

Usage (from data_work/):
    python -m src.model.travel_cost_bundle.plot_descriptive_audit \\
        --version-id model_input_edgeproj_v2_lcc_v1
    python -m src.model.travel_cost_bundle.plot_descriptive_audit \\
        --version-id model_input_edgeproj_v2_lcc_v1 --detour-gate 5
"""
from __future__ import annotations

import argparse
from pathlib import Path

import geopandas as gpd
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
GRID_MASTER = REPO / "outputs" / "manual_centerline_rules_v11" / "data" / "grid_square_master.parquet"
UTM = 32650  # Beijing (~116.4E) -> UTM zone 50N


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Descriptive-audit figures for a locked bundle")
    p.add_argument("--version-id", required=True)
    p.add_argument("--grid-type", default="square")
    p.add_argument("--detour-gate", type=float, default=None,
                   help="Drop adjacent edges with route/straightline > G (optional).")
    p.add_argument("--output-dir", default=str(REPO / "outputs"))
    return p.parse_args()


def _load(version_root: Path, grid_type: str):
    data = version_root / "data"
    nodes = pd.read_parquet(data / f"qsm_input_nodes_{grid_type}.parquet")
    edges = pd.read_parquet(data / f"qsm_input_edges_{grid_type}.parquet")
    od = pd.read_parquet(data / f"qsm_input_od_{grid_type}.parquet")
    return nodes, edges, od


def _centroids() -> dict[str, tuple[float, float]]:
    g = gpd.read_parquet(GRID_MASTER).to_crs(UTM)
    c = g.geometry.centroid
    return {str(gid): (x, y) for gid, x, y in zip(g["grid_id"], c.x.values, c.y.values)}


def _prep_edges(edges: pd.DataFrame, cents: dict, gate: float | None):
    e = edges.copy()
    e["grid_o"] = e["grid_o"].astype(str)
    e["grid_d"] = e["grid_d"].astype(str)
    e["tau_ep"] = pd.to_numeric(e["tau_obs_min"], errors="coerce")
    e["tau_cl"] = pd.to_numeric(e.get("tau_obs_min_old"), errors="coerce")

    def dgeo(o, d):
        if o in cents and d in cents:
            (x1, y1), (x2, y2) = cents[o], cents[d]
            return float(np.hypot(x1 - x2, y1 - y2))
        return np.nan

    e["d_geo"] = [dgeo(o, d) for o, d in zip(e["grid_o"], e["grid_d"])]
    e["detour"] = e["route_length_m"] / e["d_geo"]
    if gate is not None:
        e = e[e["detour"] <= gate].copy()
    return e


def _grid_gdf() -> gpd.GeoDataFrame:
    g = gpd.read_parquet(GRID_MASTER).to_crs(UTM)
    g["grid_id"] = g["grid_id"].astype(str)
    return g


def _save(fig, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    args = _parse_args()
    root = Path(args.output_dir) / args.version_id
    suffix = "" if args.detour_gate is None else f"_gate{args.detour_gate:g}"
    figdir = root / "figures" / "descriptive_audit"

    nodes, edges, od = _load(root, args.grid_type)
    cents = _centroids()
    e = _prep_edges(edges, cents, args.detour_gate)
    grid = _grid_gdf()

    lcc_grids = set(e["grid_o"]) | set(e["grid_d"])
    grid["in_lcc"] = grid["grid_id"].isin(lcc_grids).astype(int)

    # fig01: LCC scope map
    fig, ax = plt.subplots(figsize=(7, 7))
    grid.plot(ax=ax, color="#e8e8e8", edgecolor="white", linewidth=0.2)
    grid[grid.in_lcc == 1].plot(ax=ax, color="#2c7fb8", edgecolor="white", linewidth=0.2)
    ax.set_title(f"LCC scope ({len(lcc_grids)} grids){suffix}")
    ax.set_axis_off()
    _save(fig, figdir / f"fig01_lcc_scope_map{suffix}.png")

    # fig02: tau_obs distribution
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.hist(e["tau_ep"].dropna(), bins=60, color="#2c7fb8")
    ax.set_xlabel("edge tau_obs (min)")
    ax.set_ylabel("count")
    ax.set_title(f"Edge travel-time distribution{suffix}")
    _save(fig, figdir / f"fig02_tau_obs_distribution{suffix}.png")

    # fig03: directional asymmetry histogram
    rev = e.rename(columns={"grid_o": "grid_d", "grid_d": "grid_o"})
    both = e.merge(rev, on=["grid_o", "grid_d"], suffixes=("_f", "_r"))
    ratio = (both["tau_ep_f"] / both["tau_ep_r"]).replace([np.inf, -np.inf], np.nan).dropna()
    sym = np.minimum(ratio, 1.0 / ratio)
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.hist(sym, bins=40, color="#d95f0e")
    ax.axvline(0.5, color="k", ls="--", lw=1)
    ax.set_xlabel("min(r, 1/r) of directional tau ratio")
    ax.set_ylabel("count")
    ax.set_title(f"Directional asymmetry (median={sym.median():.3f}){suffix}")
    _save(fig, figdir / f"fig03_asymmetry_histogram{suffix}.png")

    # fig04: edgeproj vs centerline scatter
    fig, ax = plt.subplots(figsize=(5.5, 5.5))
    sub = e.dropna(subset=["tau_ep", "tau_cl"])
    ax.scatter(sub["tau_cl"], sub["tau_ep"], s=5, alpha=0.3, color="#2c7fb8")
    lim = max(sub["tau_cl"].quantile(0.99), sub["tau_ep"].quantile(0.99))
    ax.plot([0, lim], [0, lim], "k--", lw=1)
    ax.set_xlim(0, lim)
    ax.set_ylim(0, lim)
    ax.set_xlabel("centerline tau (min)")
    ax.set_ylabel("edgeproj tau (min)")
    ax.set_title(f"Edgeproj vs centerline{suffix}")
    _save(fig, figdir / f"fig04_edgeproj_vs_centerline_scatter{suffix}.png")

    # fig05: grid median tau choropleth
    gm = pd.concat([
        e[["grid_o", "tau_ep"]].rename(columns={"grid_o": "grid_id"}),
        e[["grid_d", "tau_ep"]].rename(columns={"grid_d": "grid_id"}),
    ])
    gmed = gm.groupby("grid_id")["tau_ep"].median()
    gp = grid.merge(gmed.rename("med_tau"), on="grid_id", how="left")
    fig, ax = plt.subplots(figsize=(7, 7))
    gp.plot(ax=ax, color="#f0f0f0", edgecolor="white", linewidth=0.2)
    gp.dropna(subset=["med_tau"]).plot(
        ax=ax, column="med_tau", cmap="viridis", legend=True,
        vmin=0, vmax=float(gmed.quantile(0.95)), edgecolor="white", linewidth=0.2,
    )
    ax.set_title(f"Grid median edge tau (min){suffix}")
    ax.set_axis_off()
    _save(fig, figdir / f"fig05_grid_median_tau_map{suffix}.png")

    # fig06: adjacent-edge tau map (line segments)
    fig, ax = plt.subplots(figsize=(8, 8))
    grid.plot(ax=ax, color="#f5f5f5", edgecolor="white", linewidth=0.15)
    seg = e.dropna(subset=["tau_ep"]).copy()
    vmax = float(seg["tau_ep"].quantile(0.95))
    cmap = plt.cm.magma_r
    norm = plt.Normalize(vmin=0, vmax=max(vmax, 1.0))
    for _, r in seg.iterrows():
        if r["grid_o"] in cents and r["grid_d"] in cents:
            (x1, y1), (x2, y2) = cents[r["grid_o"]], cents[r["grid_d"]]
            ax.plot([x1, x2], [y1, y2], color=cmap(norm(r["tau_ep"])), lw=0.8)
    sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])
    fig.colorbar(sm, ax=ax, fraction=0.04, label=r"physical link time $m_{kl}^{obs}$ (minutes)")
    ax.set_title(f"Observed time on directed adjacent-grid links ({len(seg)} links){suffix}")
    ax.set_axis_off()
    _save(fig, figdir / f"fig06_adjacent_edge_tau_map{suffix}.png")

    # fig07: ratio over centerline map (line segments)
    fig, ax = plt.subplots(figsize=(8, 8))
    grid.plot(ax=ax, color="#f5f5f5", edgecolor="white", linewidth=0.15)
    seg = e.dropna(subset=["tau_ep", "tau_cl"]).copy()
    seg["ratio"] = seg["tau_ep"] / seg["tau_cl"]
    cmap = plt.cm.RdYlBu_r
    norm = plt.Normalize(vmin=0.5, vmax=5.0)
    for _, r in seg.iterrows():
        if r["grid_o"] in cents and r["grid_d"] in cents:
            (x1, y1), (x2, y2) = cents[r["grid_o"]], cents[r["grid_d"]]
            ax.plot([x1, x2], [y1, y2], color=cmap(norm(r["ratio"])), lw=0.8)
    sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])
    fig.colorbar(sm, ax=ax, fraction=0.04, label="edgeproj / centerline tau")
    ax.set_title(f"Edge detour ratio over centerline{suffix}")
    ax.set_axis_off()
    _save(fig, figdir / f"fig07_ratio_over_centerline_map{suffix}.png")

    print(f"[descriptive_audit] wrote {figdir} (edges={len(e)}, grids={len(lcc_grids)}, suffix='{suffix or 'none'}')")


if __name__ == "__main__":
    main()
