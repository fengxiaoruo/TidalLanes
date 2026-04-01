"""
Plot grid-level diagnostics for a versioned run.
"""

import argparse
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
from matplotlib.collections import LineCollection
import numpy as np
import pandas as pd


TARGET_CRS = "EPSG:3857"
GRID_SUFFIX = {"square": "square3km", "hex": "hex3km", "voronoi": "voronoi3km"}


def parse_args():
    parser = argparse.ArgumentParser(description="Plot grid diagnostics")
    parser.add_argument("--version-id", required=True, help="Version identifier under outputs/.")
    parser.add_argument("--output-dir", default="outputs", help="Base output directory.")
    parser.add_argument("--grid-type", default="all", choices=["all", "square", "hex", "voronoi"])
    return parser.parse_args()


def clip_norm(vals, q=(0.02, 0.98)):
    vals = np.asarray(vals, float)
    vals = vals[np.isfinite(vals)]
    if vals.size == 0:
        return plt.Normalize(0, 1)
    vmin, vmax = np.quantile(vals, q[0]), np.quantile(vals, q[1])
    if not np.isfinite(vmin) or not np.isfinite(vmax) or vmax <= vmin:
        vmin, vmax = float(np.nanmin(vals)), float(np.nanmax(vals) + 1e-6)
    return plt.Normalize(vmin=vmin, vmax=vmax)


def sym_edges(edges: pd.DataFrame, time_col: str):
    e = edges.copy()
    e[time_col] = pd.to_numeric(e[time_col], errors="coerce")
    e = e[np.isfinite(e[time_col]) & (e[time_col] > 0)].copy()
    e["a"] = e[["grid_o", "grid_d"]].min(axis=1)
    e["b"] = e[["grid_o", "grid_d"]].max(axis=1)
    e_dir = e.groupby(["a", "b", "grid_o", "grid_d"], as_index=False)[time_col].min()
    return e_dir.groupby(["a", "b"], as_index=False)[time_col].mean().rename(columns={time_col: "t_sym"})


def sym_speed(cl_df: gpd.GeoDataFrame):
    c = cl_df.copy()
    c["cline_id"] = c["cline_id"].astype("string")
    c["cl_speed_kmh"] = pd.to_numeric(c["cl_speed_kmh"], errors="coerce")
    g = c.groupby("cline_id", as_index=False)["cl_speed_kmh"].mean().rename(columns={"cl_speed_kmh": "cl_speed_sym_kmh"})
    return c.merge(g, on="cline_id", how="left")


def lines_and_vals(gdf, val_col):
    segs, vals = [], []
    for geom, val in zip(gdf.geometry.values, gdf[val_col].values):
        if geom is None or geom.is_empty:
            continue
        geoms = [geom] if geom.geom_type == "LineString" else list(getattr(geom, "geoms", []))
        for line in geoms:
            segs.append(np.asarray(line.coords))
            vals.append(val)
    return segs, vals


def build_pair_table(links: pd.DataFrame, speed_col: str):
    d = links[["period", "grid_o", "grid_d", speed_col]].copy()
    d[speed_col] = pd.to_numeric(d[speed_col], errors="coerce")
    d = d[np.isfinite(d[speed_col]) & (d[speed_col] > 0)].copy()
    d["a"] = d[["grid_o", "grid_d"]].min(axis=1)
    d["b"] = d[["grid_o", "grid_d"]].max(axis=1)
    d["dir"] = np.where(d["grid_o"] == d["a"], "ab", "ba")
    piv = d.pivot_table(index=["period", "a", "b"], columns="dir", values=speed_col, aggfunc="mean").reset_index()
    piv.columns.name = None
    if "ab" not in piv.columns:
        piv["ab"] = np.nan
    if "ba" not in piv.columns:
        piv["ba"] = np.nan
    piv["ratio"] = np.exp(-np.abs(np.log(piv["ab"]) - np.log(piv["ba"])))
    piv["intensity"] = 1.0 - piv["ratio"]
    return piv


def plot_edge_map(path: Path, centroids, edges: pd.DataFrame, time_col: str, title: str):
    e = edges.copy()
    e[time_col] = pd.to_numeric(e[time_col], errors="coerce")
    e = e[np.isfinite(e[time_col]) & (e[time_col] > 0)].copy()
    segs, vals = [], []
    for row in e.itertuples(index=False):
        if row.grid_o not in centroids.index or row.grid_d not in centroids.index:
            continue
        p, q = centroids.loc[row.grid_o], centroids.loc[row.grid_d]
        segs.append(np.asarray([(p.x, p.y), (q.x, q.y)]))
        vals.append(getattr(row, time_col))
    fig, ax = plt.subplots(figsize=(8, 8), dpi=220)
    if segs:
        lc = LineCollection(segs, cmap="RdBu_r", norm=clip_norm(vals), linewidths=0.8, alpha=0.9)
        lc.set_array(np.asarray(vals))
        ax.add_collection(lc)
        fig.colorbar(lc, ax=ax, fraction=0.03, pad=0.01, label="Travel time (min)")
    ax.scatter([p.x for p in centroids.values], [p.y for p in centroids.values], s=1, color="#444444", alpha=0.3)
    ax.autoscale()
    ax.set_axis_off()
    ax.set_title(title)
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def plot_ratio_hist(path: Path, vals: pd.Series, title: str):
    fig, ax = plt.subplots(figsize=(6, 4), dpi=200)
    v = vals.dropna()
    ax.hist(v, bins=50, color="#2171b5", alpha=0.75, edgecolor="white")
    ax.axvline(0.9, color="#fd8d3c", linestyle="--", linewidth=1.5)
    ax.axvline(0.8, color="#d7301f", linestyle="--", linewidth=1.5)
    ax.set_title(title)
    ax.set_xlabel("Directional speed ratio")
    ax.set_ylabel("Count")
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def plot_ratio_scatter(path: Path, am: pd.DataFrame, pm: pd.DataFrame, suffix: str):
    both = am[["a", "b", "ratio"]].rename(columns={"ratio": "ratio_am"}).merge(
        pm[["a", "b", "ratio"]].rename(columns={"ratio": "ratio_pm"}),
        on=["a", "b"],
        how="inner",
    )
    fig, ax = plt.subplots(figsize=(5, 5), dpi=200)
    ax.scatter(both["ratio_am"], both["ratio_pm"], s=8, alpha=0.35, color="#2b8cbe")
    ax.plot([0, 1], [0, 1], color="red", linestyle="--", linewidth=1.2)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.set_xlabel("AM ratio")
    ax.set_ylabel("PM ratio")
    ax.set_title(f"AM vs PM asymmetry ratio ({suffix})")
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def plot_tidal_intensity(path: Path, centroids, pair_df: pd.DataFrame, title: str):
    segs, vals = [], []
    for row in pair_df.itertuples(index=False):
        if row.a not in centroids.index or row.b not in centroids.index or not np.isfinite(row.intensity):
            continue
        p, q = centroids.loc[row.a], centroids.loc[row.b]
        segs.append(np.asarray([(p.x, p.y), (q.x, q.y)]))
        vals.append(row.intensity)
    fig, ax = plt.subplots(figsize=(8, 8), dpi=220)
    if segs:
        lc = LineCollection(segs, cmap="YlOrRd", norm=clip_norm(vals, q=(0.05, 0.98)), linewidths=1.0, alpha=0.9)
        lc.set_array(np.asarray(vals))
        ax.add_collection(lc)
        fig.colorbar(lc, ax=ax, fraction=0.03, pad=0.01, label="Asymmetry intensity")
    ax.autoscale()
    ax.set_axis_off()
    ax.set_title(title)
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def plot_two_panel(path: Path, grid: gpd.GeoDataFrame, cl_am: gpd.GeoDataFrame, centroids, edges_am: pd.DataFrame, pop: pd.DataFrame, title: str):
    cl_am = sym_speed(cl_am)
    cl_valid = cl_am[np.isfinite(cl_am["cl_speed_sym_kmh"]) & (cl_am["cl_speed_sym_kmh"] > 0)].copy()
    cl_segs, cl_vals = lines_and_vals(cl_valid, "cl_speed_sym_kmh")
    e_sym = sym_edges(edges_am, "t_min")
    edge_segs, edge_vals = [], []
    for row in e_sym.itertuples(index=False):
        if row.a not in centroids.index or row.b not in centroids.index:
            continue
        p, q = centroids.loc[row.a], centroids.loc[row.b]
        edge_segs.append(np.asarray([(p.x, p.y), (q.x, q.y)]))
        edge_vals.append(row.t_sym)

    pop = pop.copy()
    pop["residents"] = pd.to_numeric(pop["residents"], errors="coerce").fillna(0)
    pop = pop[pop["grid_id"].isin(centroids.index)].copy()
    sizes = np.sqrt(np.clip(pop["residents"].values, 0, None))
    if sizes.size:
        sizes = 10 + 200 * (sizes - sizes.min()) / (sizes.max() - sizes.min() + 1e-9)

    fig, axes = plt.subplots(1, 2, figsize=(14, 7), dpi=220)
    grid_boundary = grid.boundary
    grid_boundary.plot(ax=axes[0], color="#d8d8d8", linewidth=0.25, alpha=0.8, zorder=0)
    if cl_segs:
        lc1 = LineCollection(cl_segs, cmap="Reds_r", norm=clip_norm(cl_vals), linewidths=0.35, alpha=0.9)
        lc1.set_array(np.asarray(cl_vals))
        axes[0].add_collection(lc1)
        fig.colorbar(lc1, ax=axes[0], fraction=0.03, pad=0.01, label="Speed (km/h)")
    axes[0].autoscale()
    axes[0].set_axis_off()
    axes[0].set_title("AM centerline speed")

    if len(pop):
        pts = np.array([(centroids.loc[g].x, centroids.loc[g].y) for g in pop["grid_id"].values])
        axes[1].scatter(
            pts[:, 0],
            pts[:, 1],
            s=sizes,
            facecolors="#f7e7a1",
            edgecolors="none",
            linewidths=0.0,
            alpha=0.5,
            zorder=0,
        )
    grid_boundary.plot(ax=axes[1], color="#b6b6b6", linewidth=0.4, alpha=0.95, zorder=1)
    if edge_segs:
        lc2 = LineCollection(edge_segs, cmap="RdBu_r", norm=clip_norm(edge_vals), linewidths=0.9, alpha=0.9, zorder=2)
        lc2.set_array(np.asarray(edge_vals))
        axes[1].add_collection(lc2)
        fig.colorbar(lc2, ax=axes[1], fraction=0.03, pad=0.01, label="Travel time (min)")
    axes[1].autoscale()
    axes[1].set_axis_off()
    axes[1].set_title("AM grid network and residents")
    fig.suptitle(title)
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def run_for_grid(version_root: Path, grid_type: str):
    data_dir = version_root / "data"
    figures_dir = version_root / "figures"
    figures_dir.mkdir(parents=True, exist_ok=True)
    suffix = GRID_SUFFIX[grid_type]

    grid = gpd.read_parquet(data_dir / f"grid_{grid_type}_master.parquet").to_crs(TARGET_CRS).copy()
    grid["grid_id"] = grid["grid_id"].astype("string")
    centroids = grid.set_index("grid_id").geometry.centroid

    cl_peak = gpd.read_parquet(data_dir / f"cl_speed_peak_geo_{grid_type}.parquet").to_crs(TARGET_CRS)
    links = pd.read_csv(data_dir / f"grid_links_{grid_type}_agg.csv")
    for col in ["grid_o", "grid_d"]:
        links[col] = links[col].astype("string")
    edges_am = pd.read_csv(data_dir / f"t_edges_{grid_type}_AM.csv")
    edges_pm = pd.read_csv(data_dir / f"t_edges_{grid_type}_PM.csv")
    for df in [edges_am, edges_pm]:
        for col in ["grid_o", "grid_d"]:
            df[col] = df[col].astype("string")
    pop = pd.read_csv(data_dir / f"grid_population_summary_{grid_type}.csv")
    pop["grid_id"] = pop["grid_id"].astype("string")
    mode = pd.read_csv(data_dir / f"commute_mode_share_summary_{grid_type}.csv")

    plot_edge_map(figures_dir / f"grid_links_{suffix}_AM.png", centroids, edges_am, "t_min", f"{suffix} AM grid links")
    plot_edge_map(figures_dir / f"grid_links_{suffix}_PM.png", centroids, edges_pm, "t_min", f"{suffix} PM grid links")

    speed_col = "v_harm_kmh" if "v_harm_kmh" in links.columns else "v_len_w_kmh"
    pair = build_pair_table(links, speed_col)
    am = pair[pair["period"] == "AM"].copy()
    pm = pair[pair["period"] == "PM"].copy()
    plot_ratio_hist(figures_dir / f"asym_hist_AM_{suffix}.png", am["ratio"], f"AM asymmetry ratio ({suffix})")
    plot_ratio_hist(figures_dir / f"asym_hist_PM_{suffix}.png", pm["ratio"], f"PM asymmetry ratio ({suffix})")
    plot_ratio_scatter(figures_dir / f"asym_scatter_AM_vs_PM_{suffix}.png", am, pm, suffix)
    plot_tidal_intensity(figures_dir / f"tidal_link_intensity_{suffix}_AM.png", centroids, am, f"Tidal link intensity AM ({suffix})")

    cl_am = cl_peak[cl_peak["period"] == "AM"].copy()
    plot_two_panel(figures_dir / f"aa_style_{suffix}_AM_2panel.png", grid, cl_am, centroids, edges_am, pop, f"{suffix} AM two-panel view")

    fig, ax = plt.subplots(figsize=(6, 4), dpi=180)
    ax.bar(mode["mode"], mode["share_in_identified_pct"], color="#4c78a8")
    ax.set_ylabel("Share within identified (%)")
    ax.set_title(f"Mode share ({suffix})")
    ax.tick_params(axis="x", rotation=30)
    fig.tight_layout()
    fig.savefig(figures_dir / f"commute_mode_share_bar_{suffix}.png", bbox_inches="tight")
    plt.close(fig)


def run(version_id: str, output_dir: str, grid_type: str):
    version_root = Path(output_dir) / version_id
    grid_types = ["square", "hex", "voronoi"] if grid_type == "all" else [grid_type]
    for gt in grid_types:
        run_for_grid(version_root, gt)
    print(f"[plot_grid_diagnostics] saved figures for {','.join(grid_types)}")


def main():
    args = parse_args()
    run(args.version_id, args.output_dir, args.grid_type)


if __name__ == "__main__":
    main()
