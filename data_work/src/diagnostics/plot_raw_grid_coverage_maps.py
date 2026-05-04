from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
from matplotlib.collections import LineCollection
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.diagnostics.plot_grid_diagnostics import clip_norm

TARGET_CRS = "EPSG:3857"
BOUNDARY_PATH = ROOT / "raw_data" / "gis" / "map" / "北京市边界.shp"
GRID_TYPES = ("square", "hex", "voronoi")
PERIODS = ("AM", "PM")


def parse_args():
    parser = argparse.ArgumentParser(description="Plot raw-grid coverage maps with population overlays.")
    parser.add_argument("--output-root", required=True, help="Raw-grid output root, e.g. edgeproj_v2.")
    parser.add_argument(
        "--grid-type",
        default="all",
        choices=["all", "square", "hex", "voronoi"],
        help="Grid system to process.",
    )
    parser.add_argument(
        "--period",
        default="all",
        choices=["all", "AM", "PM"],
        help="Period to process.",
    )
    return parser.parse_args()


def normalize_grid_id(series: pd.Series) -> pd.Series:
    return series.astype("string").str.strip()


def load_run_config(output_root: Path) -> dict:
    return json.loads((output_root / "run_config.json").read_text(encoding="utf-8"))


def load_boundary(grid: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if BOUNDARY_PATH.exists():
        boundary = gpd.read_file(BOUNDARY_PATH)
        if boundary.crs is None:
            boundary = boundary.set_crs("EPSG:4326")
        return boundary.to_crs(TARGET_CRS)
    geom = gpd.GeoSeries([grid.geometry.union_all()], crs=grid.crs)
    return gpd.GeoDataFrame(geometry=geom, crs=grid.crs)


def load_grid_and_population(baseline_root: Path, grid_type: str) -> tuple[gpd.GeoDataFrame, pd.DataFrame, pd.DataFrame]:
    data_dir = baseline_root / "data"
    grid = gpd.read_parquet(data_dir / f"grid_{grid_type}_master.parquet")[["grid_id", "geometry"]].copy()
    if grid.crs is None:
        raise ValueError(f"grid_{grid_type}_master.parquet has no CRS.")
    grid = grid.to_crs(TARGET_CRS)
    grid["grid_id"] = normalize_grid_id(grid["grid_id"])
    grid["centroid"] = grid.geometry.centroid

    pop = pd.read_csv(data_dir / f"grid_population_summary_{grid_type}.csv")
    pop["grid_id"] = normalize_grid_id(pop["grid_id"])
    for col in ["residents", "jobs", "job_resident_ratio"]:
        if col in pop.columns:
            pop[col] = pd.to_numeric(pop[col], errors="coerce").fillna(0.0)

    od = pd.read_csv(data_dir / f"OD_{grid_type}.csv")
    od["home_grid"] = normalize_grid_id(od["home_grid"])
    od["work_grid"] = normalize_grid_id(od["work_grid"])
    for col in ["pop", "type_walk", "type_bike", "type_sub", "type_bus", "type_car"]:
        if col in od.columns:
            od[col] = pd.to_numeric(od[col], errors="coerce").fillna(0.0)
    return grid, pop, od


def load_raw_pairs(output_root: Path, grid_type: str, period: str) -> pd.DataFrame:
    path = output_root / "data" / f"raw_connected_vs_old_adjacent_adjacent_{grid_type}_{period}.csv"
    if not path.exists():
        path = output_root / "data" / f"kunpeng_vs_old_adjacent_adjacent_{grid_type}_{period}.csv"
    pairs = pd.read_csv(path)
    pairs["home_grid"] = normalize_grid_id(pairs["home_grid"])
    pairs["work_grid"] = normalize_grid_id(pairs["work_grid"])
    pairs["raw_connected_travel_time_min"] = pd.to_numeric(pairs["raw_connected_travel_time_min"], errors="coerce")
    pairs = pairs[np.isfinite(pairs["raw_connected_travel_time_min"]) & (pairs["raw_connected_travel_time_min"] > 0)].copy()
    return pairs


def build_undirected_edges(pairs: pd.DataFrame) -> pd.DataFrame:
    edges = pairs[["home_grid", "work_grid", "raw_connected_travel_time_min"]].copy()
    edges["a"] = edges[["home_grid", "work_grid"]].min(axis=1)
    edges["b"] = edges[["home_grid", "work_grid"]].max(axis=1)
    dir_min = (
        edges.groupby(["a", "b", "home_grid", "work_grid"], as_index=False)["raw_connected_travel_time_min"]
        .min()
        .rename(columns={"raw_connected_travel_time_min": "t_dir_min"})
    )
    undir = (
        dir_min.groupby(["a", "b"], as_index=False)["t_dir_min"]
        .mean()
        .rename(columns={"t_dir_min": "t_sym"})
    )
    return undir


def paired_ratio(a: pd.Series, b: pd.Series) -> pd.Series:
    aa = pd.to_numeric(a, errors="coerce").astype(float)
    bb = pd.to_numeric(b, errors="coerce").astype(float)
    out = np.minimum(aa, bb) / np.maximum(aa, bb)
    out[(~np.isfinite(aa)) | (~np.isfinite(bb)) | (aa <= 0) | (bb <= 0)] = np.nan
    return pd.Series(out)


def build_tidal_asymmetry_pairs(pairs: pd.DataFrame, period: str) -> pd.DataFrame:
    directed = pairs[["home_grid", "work_grid", "raw_connected_travel_time_min"]].copy()
    directed["home_grid"] = normalize_grid_id(directed["home_grid"])
    directed["work_grid"] = normalize_grid_id(directed["work_grid"])
    directed["raw_connected_travel_time_min"] = pd.to_numeric(directed["raw_connected_travel_time_min"], errors="coerce")
    directed = directed[
        np.isfinite(directed["raw_connected_travel_time_min"]) & (directed["raw_connected_travel_time_min"] > 0)
    ].copy()
    directed["a"] = directed[["home_grid", "work_grid"]].min(axis=1)
    directed["b"] = directed[["home_grid", "work_grid"]].max(axis=1)
    directed["dir"] = np.where(directed["home_grid"] == directed["a"], "ab", "ba")

    pair_df = directed.pivot_table(
        index=["a", "b"],
        columns="dir",
        values="raw_connected_travel_time_min",
        aggfunc="min",
    ).reset_index()
    pair_df.columns.name = None
    if "ab" not in pair_df.columns:
        pair_df["ab"] = np.nan
    if "ba" not in pair_df.columns:
        pair_df["ba"] = np.nan
    pair_df["ratio"] = paired_ratio(pair_df["ab"], pair_df["ba"])
    pair_df["intensity"] = 1.0 - pair_df["ratio"]
    pair_df["period"] = period
    pair_df["faster_dir"] = np.where(
        ~np.isfinite(pair_df["ratio"]),
        pd.NA,
        np.where(pair_df["ab"] <= pair_df["ba"], "ab", "ba"),
    )
    pair_df["slower_dir"] = np.where(
        ~np.isfinite(pair_df["ratio"]),
        pd.NA,
        np.where(pair_df["ab"] > pair_df["ba"], "ab", "ba"),
    )
    pair_df["time_gap_min"] = np.where(
        np.isfinite(pair_df["ab"]) & np.isfinite(pair_df["ba"]),
        np.abs(pair_df["ab"] - pair_df["ba"]),
        np.nan,
    )
    return pair_df


def summarise_tidal_asymmetry(pair_df: pd.DataFrame, grid_type: str, period: str) -> pd.DataFrame:
    ratio = pd.to_numeric(pair_df["ratio"], errors="coerce")
    intensity = pd.to_numeric(pair_df["intensity"], errors="coerce")
    valid = pair_df[np.isfinite(ratio)].copy()
    summary = {
        "grid_type": grid_type,
        "period": period,
        "undirected_pairs_total": int(len(pair_df)),
        "undirected_pairs_both_dir": int(len(valid)),
        "mean_directional_ratio": float(ratio.mean()) if len(valid) else np.nan,
        "median_directional_ratio": float(ratio.median()) if len(valid) else np.nan,
        "p10_directional_ratio": float(ratio.quantile(0.10)) if len(valid) else np.nan,
        "p90_directional_ratio": float(ratio.quantile(0.90)) if len(valid) else np.nan,
        "mean_asymmetry_intensity": float(intensity.mean()) if len(valid) else np.nan,
        "median_asymmetry_intensity": float(intensity.median()) if len(valid) else np.nan,
        "share_asym_lt_0_9": float((ratio < 0.9).mean()) if len(valid) else np.nan,
        "share_asym_lt_0_8": float((ratio < 0.8).mean()) if len(valid) else np.nan,
        "share_asym_lt_0_5": float((ratio < 0.5).mean()) if len(valid) else np.nan,
        "mean_time_gap_min": float(pd.to_numeric(valid["time_gap_min"], errors="coerce").mean()) if len(valid) else np.nan,
        "median_time_gap_min": float(pd.to_numeric(valid["time_gap_min"], errors="coerce").median()) if len(valid) else np.nan,
    }
    return pd.DataFrame([summary])


def compute_components(nodes: list[str], edges: pd.DataFrame) -> pd.DataFrame:
    parent = {node: node for node in nodes}
    rank = {node: 0 for node in nodes}

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: str, b: str):
        ra, rb = find(a), find(b)
        if ra == rb:
            return
        if rank[ra] < rank[rb]:
            parent[ra] = rb
        elif rank[ra] > rank[rb]:
            parent[rb] = ra
        else:
            parent[rb] = ra
            rank[ra] += 1

    for row in edges.itertuples(index=False):
        union(row.a, row.b)

    comp = pd.DataFrame({"grid_id": pd.Series(nodes, dtype="string")})
    comp["component_root"] = comp["grid_id"].map(find).astype("string")
    comp["component_size"] = comp.groupby("component_root")["grid_id"].transform("size").astype(int)
    rank_map = (
        comp[["component_root", "component_size"]]
        .drop_duplicates()
        .sort_values(["component_size", "component_root"], ascending=[False, True])
        .reset_index(drop=True)
        .reset_index()
        .rename(columns={"index": "component_rank"})
    )
    rank_map["component_rank"] = rank_map["component_rank"] + 1
    comp["component_rank"] = comp["component_root"].map(dict(zip(rank_map["component_root"], rank_map["component_rank"]))).astype(int)
    return comp


def build_grid_coverage_table(
    grid: gpd.GeoDataFrame,
    pop: pd.DataFrame,
    period: str,
    undir_edges: pd.DataFrame,
) -> tuple[gpd.GeoDataFrame, pd.DataFrame]:
    covered_nodes = pd.Index(pd.unique(pd.concat([undir_edges["a"], undir_edges["b"]], ignore_index=True))).astype("string")
    comp = compute_components(covered_nodes.tolist(), undir_edges) if len(covered_nodes) else pd.DataFrame(
        columns=["grid_id", "component_root", "component_size", "component_rank"]
    )

    grid_cov = grid.merge(pop, on="grid_id", how="left")
    grid_cov["residents"] = pd.to_numeric(grid_cov["residents"], errors="coerce").fillna(0.0)
    grid_cov["jobs"] = pd.to_numeric(grid_cov["jobs"], errors="coerce").fillna(0.0)
    grid_cov["job_resident_ratio"] = pd.to_numeric(grid_cov["job_resident_ratio"], errors="coerce")
    grid_cov["period"] = period
    grid_cov["covered_flag"] = grid_cov["grid_id"].isin(covered_nodes).astype(int)
    if len(comp):
        grid_cov = grid_cov.merge(comp, on="grid_id", how="left")
    else:
        grid_cov["component_root"] = pd.NA
        grid_cov["component_size"] = pd.NA
        grid_cov["component_rank"] = pd.NA
    grid_cov["largest_component_flag"] = np.where(grid_cov["component_rank"].fillna(999999).astype(int) == 1, 1, 0)
    grid_cov["centroid_x_m"] = grid_cov["centroid"].x
    grid_cov["centroid_y_m"] = grid_cov["centroid"].y
    return grid_cov, undir_edges


def summarise_coverage(grid_cov: gpd.GeoDataFrame, od: pd.DataFrame, grid_type: str, period: str) -> pd.DataFrame:
    covered = set(grid_cov.loc[grid_cov["covered_flag"] == 1, "grid_id"].astype("string"))
    largest = set(grid_cov.loc[grid_cov["largest_component_flag"] == 1, "grid_id"].astype("string"))
    total_residents = float(grid_cov["residents"].sum())
    total_jobs = float(grid_cov["jobs"].sum())
    total_pop = float(pd.to_numeric(od["pop"], errors="coerce").fillna(0.0).sum())

    od_home_covered = od["home_grid"].isin(covered)
    od_work_covered = od["work_grid"].isin(covered)
    od_both_largest = od["home_grid"].isin(largest) & od["work_grid"].isin(largest)
    od_home_largest = od["home_grid"].isin(largest)
    od_work_largest = od["work_grid"].isin(largest)
    od_pop = pd.to_numeric(od["pop"], errors="coerce").fillna(0.0)

    summary = {
        "grid_type": grid_type,
        "period": period,
        "total_grids": int(len(grid_cov)),
        "covered_grids": int(grid_cov["covered_flag"].sum()),
        "covered_grid_share": float(grid_cov["covered_flag"].mean()) if len(grid_cov) else np.nan,
        "largest_component_grids": int(grid_cov["largest_component_flag"].sum()),
        "largest_component_grid_share": float(grid_cov["largest_component_flag"].mean()) if len(grid_cov) else np.nan,
        "total_residents": total_residents,
        "covered_residents": float(grid_cov.loc[grid_cov["covered_flag"] == 1, "residents"].sum()),
        "covered_resident_share": float(grid_cov.loc[grid_cov["covered_flag"] == 1, "residents"].sum() / total_residents) if total_residents > 0 else np.nan,
        "largest_component_residents": float(grid_cov.loc[grid_cov["largest_component_flag"] == 1, "residents"].sum()),
        "largest_component_resident_share": float(grid_cov.loc[grid_cov["largest_component_flag"] == 1, "residents"].sum() / total_residents) if total_residents > 0 else np.nan,
        "total_jobs": total_jobs,
        "covered_jobs": float(grid_cov.loc[grid_cov["covered_flag"] == 1, "jobs"].sum()),
        "covered_job_share": float(grid_cov.loc[grid_cov["covered_flag"] == 1, "jobs"].sum() / total_jobs) if total_jobs > 0 else np.nan,
        "largest_component_jobs": float(grid_cov.loc[grid_cov["largest_component_flag"] == 1, "jobs"].sum()),
        "largest_component_job_share": float(grid_cov.loc[grid_cov["largest_component_flag"] == 1, "jobs"].sum() / total_jobs) if total_jobs > 0 else np.nan,
        "od_total_population": total_pop,
        "od_home_in_covered_pop": float(od_pop[od_home_covered].sum()),
        "od_home_in_covered_pop_share": float(od_pop[od_home_covered].sum() / total_pop) if total_pop > 0 else np.nan,
        "od_work_in_covered_pop": float(od_pop[od_work_covered].sum()),
        "od_work_in_covered_pop_share": float(od_pop[od_work_covered].sum() / total_pop) if total_pop > 0 else np.nan,
        "od_home_in_largest_pop": float(od_pop[od_home_largest].sum()),
        "od_home_in_largest_pop_share": float(od_pop[od_home_largest].sum() / total_pop) if total_pop > 0 else np.nan,
        "od_work_in_largest_pop": float(od_pop[od_work_largest].sum()),
        "od_work_in_largest_pop_share": float(od_pop[od_work_largest].sum() / total_pop) if total_pop > 0 else np.nan,
        "od_both_in_largest_pop": float(od_pop[od_both_largest].sum()),
        "od_both_in_largest_pop_share": float(od_pop[od_both_largest].sum() / total_pop) if total_pop > 0 else np.nan,
    }
    return pd.DataFrame([summary])


def build_component_outline(grid_cov: gpd.GeoDataFrame):
    largest = grid_cov.loc[grid_cov["largest_component_flag"] == 1, ["geometry"]].copy()
    if largest.empty:
        return None
    union_geom = largest.geometry.union_all()
    if union_geom is None or union_geom.is_empty:
        return None
    return gpd.GeoSeries([union_geom.boundary], crs=grid_cov.crs)


def build_edge_segments(undir_edges: pd.DataFrame, centroids: pd.Series) -> tuple[list[np.ndarray], np.ndarray]:
    segs, vals = [], []
    for row in undir_edges.itertuples(index=False):
        if row.a not in centroids.index or row.b not in centroids.index:
            continue
        p = centroids.loc[row.a]
        q = centroids.loc[row.b]
        segs.append(np.asarray([(p.x, p.y), (q.x, q.y)]))
        vals.append(row.t_sym)
    return segs, np.asarray(vals, dtype=float)


def build_pair_segments(pair_df: pd.DataFrame, centroids: pd.Series, value_col: str) -> tuple[list[np.ndarray], np.ndarray]:
    segs, vals = [], []
    for row in pair_df.itertuples(index=False):
        if row.a not in centroids.index or row.b not in centroids.index:
            continue
        val = getattr(row, value_col)
        if not np.isfinite(val):
            continue
        p = centroids.loc[row.a]
        q = centroids.loc[row.b]
        segs.append(np.asarray([(p.x, p.y), (q.x, q.y)]))
        vals.append(val)
    return segs, np.asarray(vals, dtype=float)


def resident_sizes(values: np.ndarray) -> np.ndarray:
    vals = np.sqrt(np.clip(values.astype(float), 0, None))
    if vals.size == 0:
        return vals
    return 10.0 + 200.0 * (vals - vals.min()) / (vals.max() - vals.min() + 1e-9)


def plot_coverage_map(
    path: Path,
    boundary: gpd.GeoDataFrame,
    grid_cov: gpd.GeoDataFrame,
    undir_edges: pd.DataFrame,
    summary_row: pd.Series,
    title: str,
):
    centroids = grid_cov.set_index("grid_id")["centroid"]
    edge_segs, edge_vals = build_edge_segments(undir_edges, centroids)
    outline = build_component_outline(grid_cov)

    pop = grid_cov.loc[grid_cov["residents"] > 0, ["grid_id", "residents"]].copy()
    sizes = resident_sizes(pop["residents"].to_numpy(dtype=float))
    fig, ax = plt.subplots(figsize=(11, 11), dpi=240)

    boundary.boundary.plot(ax=ax, color="#6b6b6b", linewidth=0.9, alpha=0.95, zorder=0, rasterized=True)
    if len(pop):
        pts = np.array([(centroids.loc[g].x, centroids.loc[g].y) for g in pop["grid_id"].values])
        ax.scatter(
            pts[:, 0],
            pts[:, 1],
            s=sizes,
            facecolors="#f7e7a1",
            edgecolors="none",
            linewidths=0.0,
            alpha=0.46,
            zorder=1,
            rasterized=True,
        )
    grid_cov.boundary.plot(ax=ax, color="#b6b6b6", linewidth=0.28, alpha=0.92, zorder=2, rasterized=True)
    if edge_segs:
        lc = LineCollection(
            edge_segs,
            cmap="RdBu_r",
            norm=clip_norm(edge_vals),
            linewidths=0.92,
            alpha=0.9,
            zorder=3,
        )
        lc.set_array(edge_vals)
        ax.add_collection(lc)
        fig.colorbar(lc, ax=ax, fraction=0.03, pad=0.01, label="Travel time (min)")
    if outline is not None:
        outline.plot(ax=ax, color="#cb181d", linewidth=2.2, alpha=0.98, zorder=4)

    ax.set_title(
        title
        + "\n"
        + (
            f"Covered grids {int(summary_row['covered_grids'])}/{int(summary_row['total_grids'])}"
            f" ({summary_row['covered_grid_share']:.1%}); "
            f"covered residents {summary_row['covered_resident_share']:.1%}"
        )
        + "\n"
        + (
            f"Maximumconnected covered grids {int(summary_row['maximumconnected_covered_grids'])}/{int(summary_row['total_grids'])}"
            f" ({summary_row['maximumconnected_covered_grid_share']:.1%}); "
            f"Maximumconnected covered residents {summary_row['maximumconnected_covered_resident_share']:.1%}"
        )
    )
    ax.set_axis_off()
    ax.set_aspect("equal")
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def plot_tidal_asymmetry_map(
    path: Path,
    boundary: gpd.GeoDataFrame,
    grid_cov: gpd.GeoDataFrame,
    pair_df: pd.DataFrame,
    summary_row: pd.Series,
    title: str,
):
    centroids = grid_cov.set_index("grid_id")["centroid"]
    segs, vals = build_pair_segments(pair_df, centroids, "intensity")

    fig, ax = plt.subplots(figsize=(11, 11), dpi=240)
    boundary.boundary.plot(ax=ax, color="#6b6b6b", linewidth=0.9, alpha=0.95, zorder=0, rasterized=True)
    grid_cov.boundary.plot(ax=ax, color="#d0d0d0", linewidth=0.22, alpha=0.88, zorder=1, rasterized=True)
    if segs:
        lc = LineCollection(
            segs,
            cmap="YlOrRd",
            norm=clip_norm(vals, q=(0.05, 0.98)),
            linewidths=1.0,
            alpha=0.92,
            zorder=2,
        )
        lc.set_array(vals)
        ax.add_collection(lc)
        fig.colorbar(lc, ax=ax, fraction=0.03, pad=0.01, label="Asymmetry intensity")
    ax.set_title(
        title
        + "\n"
        + (
            f"Both-dir pairs {int(summary_row['undirected_pairs_both_dir'])}; "
            f"median ratio {summary_row['median_directional_ratio']:.3f}; "
            f"share ratio<0.8 {summary_row['share_asym_lt_0_8']:.1%}"
        )
    )
    ax.set_axis_off()
    ax.set_aspect("equal")
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def run_for_grid_period(
    output_root: Path,
    baseline_root: Path,
    boundary: gpd.GeoDataFrame,
    grid_type: str,
    period: str,
    data_out_dir: Path,
    metrics_out_dir: Path,
    figures_out_dir: Path,
    asym_data_out_dir: Path,
    asym_metrics_out_dir: Path,
    asym_figures_out_dir: Path,
) -> pd.DataFrame:
    grid, pop, od = load_grid_and_population(baseline_root, grid_type)
    pairs = load_raw_pairs(output_root, grid_type, period)
    undir_edges = build_undirected_edges(pairs)
    grid_cov, undir_edges = build_grid_coverage_table(grid, pop, period, undir_edges)
    summary = summarise_coverage(grid_cov, od, grid_type, period)
    summary["maximumconnected_covered_grids"] = summary["largest_component_grids"]
    summary["maximumconnected_covered_grid_share"] = summary["largest_component_grid_share"]
    summary["maximumconnected_covered_residents"] = summary["largest_component_residents"]
    summary["maximumconnected_covered_resident_share"] = summary["largest_component_resident_share"]

    base_pop = pop.copy()
    base_pop["grid_type"] = grid_type
    base_pop["population_source_root"] = str(baseline_root)
    base_pop.to_csv(data_out_dir / f"grid_population_summary_{grid_type}.csv", index=False)

    grid_cov.drop(columns=["geometry", "centroid"]).to_csv(
        data_out_dir / f"grid_population_coverage_{grid_type}_{period}.csv",
        index=False,
    )
    undir_edges.to_csv(data_out_dir / f"raw_grid_connected_edges_{grid_type}_{period}.csv", index=False)
    summary.to_csv(metrics_out_dir / f"raw_grid_coverage_summary_{grid_type}_{period}.csv", index=False)

    plot_coverage_map(
        figures_out_dir / f"raw_grid_coverage_{grid_type}_{period}.png",
        boundary=boundary,
        grid_cov=grid_cov,
        undir_edges=undir_edges,
        summary_row=summary.iloc[0],
        title=f"Raw-grid coverage and residents ({grid_type}, {period})",
    )

    pair_df = build_tidal_asymmetry_pairs(pairs, period)
    asym_summary = summarise_tidal_asymmetry(pair_df, grid_type, period)
    pair_df.to_csv(asym_data_out_dir / f"raw_grid_tidal_asymmetry_pairs_{grid_type}_{period}.csv", index=False)
    asym_summary.to_csv(asym_metrics_out_dir / f"raw_grid_tidal_asymmetry_summary_{grid_type}_{period}.csv", index=False)
    plot_tidal_asymmetry_map(
        asym_figures_out_dir / f"raw_grid_tidal_asymmetry_{grid_type}_{period}.png",
        boundary=boundary,
        grid_cov=grid_cov,
        pair_df=pair_df,
        summary_row=asym_summary.iloc[0],
        title=f"Raw-grid tidal asymmetry ({grid_type}, {period})",
    )
    return summary


def run(output_root: Path, grid_type: str, period: str):
    config = load_run_config(output_root)
    baseline_root = Path(config["baseline_root"])
    grid_types = GRID_TYPES if grid_type == "all" else (grid_type,)
    periods = PERIODS if period == "all" else (period,)

    data_out_dir = output_root / "data" / "grid_coverage_maps"
    metrics_out_dir = output_root / "metrics" / "grid_coverage_maps"
    figures_out_dir = output_root / "figures" / "grid_coverage_maps"
    asym_data_out_dir = output_root / "data" / "grid_tidal_asymmetry"
    asym_metrics_out_dir = output_root / "metrics" / "grid_tidal_asymmetry"
    asym_figures_out_dir = output_root / "figures" / "grid_tidal_asymmetry"
    data_out_dir.mkdir(parents=True, exist_ok=True)
    metrics_out_dir.mkdir(parents=True, exist_ok=True)
    figures_out_dir.mkdir(parents=True, exist_ok=True)
    asym_data_out_dir.mkdir(parents=True, exist_ok=True)
    asym_metrics_out_dir.mkdir(parents=True, exist_ok=True)
    asym_figures_out_dir.mkdir(parents=True, exist_ok=True)

    sample_grid = gpd.read_parquet(baseline_root / "data" / "grid_square_master.parquet")[["geometry"]]
    if sample_grid.crs is None:
        sample_grid = sample_grid.set_crs("EPSG:4326")
    boundary = load_boundary(sample_grid.to_crs(TARGET_CRS))

    all_summaries = []
    for gt in grid_types:
        for per in periods:
            summary = run_for_grid_period(
                output_root=output_root,
                baseline_root=baseline_root,
                boundary=boundary,
                grid_type=gt,
                period=per,
                data_out_dir=data_out_dir,
                metrics_out_dir=metrics_out_dir,
                figures_out_dir=figures_out_dir,
                asym_data_out_dir=asym_data_out_dir,
                asym_metrics_out_dir=asym_metrics_out_dir,
                asym_figures_out_dir=asym_figures_out_dir,
            )
            all_summaries.append(summary)

    if all_summaries:
        pd.concat(all_summaries, ignore_index=True).to_csv(metrics_out_dir / "raw_grid_coverage_summary_all.csv", index=False)
        asym_all = []
        for gt in grid_types:
            for per in periods:
                path = asym_metrics_out_dir / f"raw_grid_tidal_asymmetry_summary_{gt}_{per}.csv"
                asym_all.append(pd.read_csv(path))
        pd.concat(asym_all, ignore_index=True).to_csv(
            asym_metrics_out_dir / "raw_grid_tidal_asymmetry_summary_all.csv",
            index=False,
        )

    note = {
        "output_root": str(output_root),
        "baseline_root": str(baseline_root),
        "population_layer": "residents",
        "coverage_definition": "grid endpoints appearing in finite raw_connected adjacent travel-cost pairs",
        "largest_component_definition": "largest weakly connected component on the undirected graph induced by finite raw_connected adjacent pairs",
        "boundary_source": str(BOUNDARY_PATH) if BOUNDARY_PATH.exists() else "grid union fallback",
    }
    (metrics_out_dir / "README_raw_grid_coverage_maps.json").write_text(
        json.dumps(note, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )


def main():
    args = parse_args()
    run(Path(args.output_root), args.grid_type, args.period)


if __name__ == "__main__":
    main()
