"""Generate PreSlides pattern-section figures and national jobs-housing stats.

Outputs go to Documents/PreSlides/figs/ (slide-ready PNG) and
data_work/outputs/preslide_pattern_figures_v1/ (metrics + source copies).
"""

from __future__ import annotations

import argparse
import io
import json
import math
import sys
import time
import urllib.request
from pathlib import Path

import geopandas as gpd
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.collections import LineCollection
from matplotlib.colors import TwoSlopeNorm
from matplotlib.gridspec import GridSpec
from matplotlib.patches import Patch, Rectangle
from PIL import Image
from shapely.geometry import Point

ROOT = Path(__file__).resolve().parents[2]
REPO = ROOT.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.diagnostics.mpl_setup import ensure_mpl_cache

GATE5_VERSION = "model_input_edgeproj_v2_lcc_gate5_v1"
STAGE2_EQ = "stage2_endogenous_edgeproj_v2_lcc_gate5_bpr_pos05_v1/baseline_endogenous/equilibrium_arrays.npz"
STAGE07_VERSION = "manual_centerline_rules_v11"
TARGET_CRS = "EPSG:32650"
WEB_MERCATOR_CRS = "EPSG:3857"
TIANANMEN_LONLAT = (116.397389, 39.908722)
CENTERLINE_MAP_RADIUS_M = 20_000
OSM_TILE_URL = "https://tile.openstreetmap.org/{z}/{x}/{y}.png"
OSM_CACHE_DIR = ROOT / "outputs" / ".osm_tile_cache"
SLIDE_FIG_DIR = REPO / "Documents" / "PreSlides" / "figs"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate PreSlides pattern figures.")
    p.add_argument("--output-name", default="preslide_pattern_figures_v1")
    return p.parse_args()


def configure_matplotlib() -> None:
    ensure_mpl_cache(str(ROOT / ".mplcache"))
    mpl.rcParams.update(
        {
            "font.sans-serif": ["Arial", "Helvetica", "DejaVu Sans", "PingFang SC", "Noto Sans CJK SC"],
            "font.family": "sans-serif",
            "axes.unicode_minus": False,
            "figure.dpi": 180,
            "savefig.dpi": 600,
            "axes.linewidth": 0.8,
            "axes.labelsize": 9,
            "axes.titlesize": 10,
            "xtick.labelsize": 8,
            "ytick.labelsize": 8,
            "legend.fontsize": 7.5,
        }
    )


def save_slide_fig(fig: mpl.figure.Figure, stem: Path) -> None:
    stem.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(stem.with_suffix(".png"), bbox_inches="tight", facecolor="white")
    fig.savefig(stem.with_suffix(".pdf"), bbox_inches="tight", facecolor="white")
    plt.close(fig)


def load_gate5_edges_with_equilibrium() -> pd.DataFrame:
    data_dir = ROOT / "outputs" / GATE5_VERSION / "data"
    edges = pd.read_parquet(data_dir / "qsm_input_edges_square.parquet").copy()
    eq = np.load(ROOT / "outputs" / STAGE2_EQ)
    edges["Xi_kl"] = eq["Xi_kl"]
    edges["tau_obs_min"] = pd.to_numeric(edges["tau_obs_min"], errors="coerce")
    edges["tau_ff_min"] = pd.to_numeric(edges["tau_ff_min"], errors="coerce")
    edges["lanes"] = pd.to_numeric(edges["lanes_directional"], errors="coerce")
    edges["speed_kmh"] = 60.0 * edges["route_length_m"] / 1000.0 / edges["tau_obs_min"].clip(lower=1e-6)
    edges["xi_per_lane"] = edges["Xi_kl"] / edges["lanes"].clip(lower=1e-6)
    return edges


def load_grid_coords() -> pd.DataFrame:
    grid = gpd.read_parquet(ROOT / "outputs" / STAGE07_VERSION / "data" / "grid_square_master.parquet")
    grid = grid.to_crs(TARGET_CRS)
    grid["x_m"] = grid.geometry.centroid.x
    grid["y_m"] = grid.geometry.centroid.y
    return grid[["grid_id", "x_m", "y_m"]].astype({"grid_id": str})


def build_reciprocal_edge_table(edges: pd.DataFrame, coords: pd.DataFrame) -> pd.DataFrame:
    use = edges.copy()
    use["grid_o"] = use["grid_o"].astype(str)
    use["grid_d"] = use["grid_d"].astype(str)
    use["pair_key"] = use.apply(lambda r: "|".join(sorted([r["grid_o"], r["grid_d"]])), axis=1)
    fwd = use.rename(
        columns={
            "grid_o": "a",
            "grid_d": "b",
            "tau_obs_min": "tau_ab",
            "tau_ff_min": "tau_ff_ab",
            "speed_kmh": "speed_ab",
            "Xi_kl": "xi_ab",
            "xi_per_lane": "xi_lane_ab",
            "lanes": "lanes_ab",
        }
    )
    rev = use.rename(
        columns={
            "grid_o": "b",
            "grid_d": "a",
            "tau_obs_min": "tau_ba",
            "tau_ff_min": "tau_ff_ba",
            "speed_kmh": "speed_ba",
            "Xi_kl": "xi_ba",
            "xi_per_lane": "xi_lane_ba",
            "lanes": "lanes_ba",
        }
    )
    pair = fwd.merge(
        rev[
            [
                "pair_key",
                "a",
                "b",
                "tau_ba",
                "tau_ff_ba",
                "speed_ba",
                "xi_ba",
                "xi_lane_ba",
                "lanes_ba",
            ]
        ],
        on=["pair_key", "a", "b"],
        how="inner",
    )
    pair = pair.merge(coords.rename(columns={"grid_id": "a", "x_m": "ax", "y_m": "ay"}), on="a", how="left")
    pair = pair.merge(coords.rename(columns={"grid_id": "b", "x_m": "bx", "y_m": "by"}), on="b", how="left")
    pair["time_ratio"] = pair[["tau_ab", "tau_ba"]].min(axis=1) / pair[["tau_ab", "tau_ba"]].max(axis=1)
    pair["speed_ratio"] = pair[["speed_ab", "speed_ba"]].min(axis=1) / pair[["speed_ab", "speed_ba"]].max(axis=1)
    pair["log_xi_lane_ratio"] = np.log(pair["xi_lane_ab"].clip(lower=1e-9) / pair["xi_lane_ba"].clip(lower=1e-9))
    pair["slow_dir"] = np.where(pair["tau_ab"] >= pair["tau_ba"], "a_to_b", "b_to_a")
    return pair


def plot_motivation_am_asymmetry(pair: pd.DataFrame, out_stem: Path) -> dict:
    fig = plt.figure(figsize=(10.5, 4.2))
    gs = GridSpec(1, 2, width_ratios=[1.45, 0.85], wspace=0.08)
    ax_map = fig.add_subplot(gs[0, 0])
    ax_hist = fig.add_subplot(gs[0, 1])

    boundary = gpd.read_file(ROOT / "raw_data" / "gis" / "map" / "北京市边界.shp").to_crs(TARGET_CRS)
    boundary.boundary.plot(ax=ax_map, color="#444444", linewidth=0.7, zorder=1)

    segs = np.stack([pair[["ax", "ay"]].to_numpy(), pair[["bx", "by"]].to_numpy()], axis=1)
    vals = pair["log_xi_lane_ratio"].to_numpy(float)
    vmax = np.nanquantile(np.abs(vals), 0.98)
    norm = TwoSlopeNorm(vmin=-vmax, vcenter=0.0, vmax=vmax)
    lc = LineCollection(segs, cmap="RdBu_r", norm=norm, linewidths=0.55, alpha=0.82, zorder=2)
    lc.set_array(vals)
    ax_map.add_collection(lc)
    ax_map.autoscale()
    ax_map.set_aspect("equal")
    ax_map.set_axis_off()
    ax_map.set_title("AM traffic-per-lane imbalance on the estimation network (gate5)")

    sm = mpl.cm.ScalarMappable(cmap="RdBu_r", norm=norm)
    sm.set_array([])
    cbar = fig.colorbar(sm, ax=ax_map, fraction=0.035, pad=0.01)
    cbar.set_label(r"$\log(\Xi_{AB}/n_{AB})-\log(\Xi_{BA}/n_{BA})$", fontsize=8)
    cbar.ax.tick_params(labelsize=7)

    ratio = pair["time_ratio"].dropna()
    ax_hist.hist(ratio, bins=np.linspace(0.5, 1.0, 26), color="#4C78A8", alpha=0.82, edgecolor="white", linewidth=0.4)
    med = float(ratio.median())
    share90 = float((ratio <= 0.90).mean())
    ax_hist.axvline(med, color="#222222", lw=1.0, ls="--")
    ax_hist.text(
        0.04,
        0.95,
        f"median ratio = {med:.3f}\nshare $\\leq$0.90 = {100*share90:.1f}%",
        transform=ax_hist.transAxes,
        va="top",
        ha="left",
        fontsize=8,
    )
    ax_hist.set_xlabel(r"$\min(\tau_{AB},\tau_{BA})/\max(\tau_{AB},\tau_{BA})$")
    ax_hist.set_ylabel("Reciprocal grid links")
    ax_hist.set_title("Two-way travel-time asymmetry")
    ax_hist.spines[["top", "right"]].set_visible(False)

    fig.suptitle("Directional congestion on the model network (AM, Stage-2 baseline $\\Xi$)", y=1.02, fontsize=11)
    save_slide_fig(fig, out_stem)
    return {
        "n_reciprocal_links": int(len(pair)),
        "median_time_ratio": med,
        "share_time_ratio_le_0_90": share90,
        "median_abs_log_xi_lane_ratio": float(np.nanmedian(np.abs(vals))),
    }


def plot_study_network_overlay(out_stem: Path) -> None:
    grid = gpd.read_parquet(ROOT / "outputs" / STAGE07_VERSION / "data" / "grid_square_master.parquet").to_crs(TARGET_CRS)
    pop = pd.read_csv(ROOT / "outputs" / STAGE07_VERSION / "data" / "grid_population_summary_square.csv")
    pop["grid_id"] = pop["grid_id"].astype(str)
    for col in ["residents", "jobs"]:
        pop[col] = pd.to_numeric(pop[col], errors="coerce").fillna(0.0)
    denom = pop["residents"] + pop["jobs"]
    pop["job_resident_balance"] = np.where(denom > 0, (pop["jobs"] - pop["residents"]) / denom, np.nan)
    gdf = grid.merge(pop, on="grid_id", how="left")
    nodes = pd.read_parquet(ROOT / "outputs" / GATE5_VERSION / "data" / "qsm_input_nodes_square.parquet")
    keep = set(nodes.loc[nodes["in_largest_component"].eq(1), "grid_id"].astype(str))
    gdf = gdf.loc[gdf["grid_id"].isin(keep)].copy()

    boundary = gpd.read_file(ROOT / "raw_data" / "gis" / "map" / "北京市边界.shp").to_crs(TARGET_CRS)
    roads = gpd.read_file(ROOT / "raw_data" / "gis" / "roads_baidu" / "beijing_roads.shp").to_crs(TARGET_CRS)
    roads["roadtype_num"] = pd.to_numeric(roads["roadtype"], errors="coerce")
    roads = roads.loc[roads["roadtype_num"].le(3)].copy()

    fig, ax = plt.subplots(figsize=(10.5, 8.8))
    norm = TwoSlopeNorm(vmin=-1, vcenter=0, vmax=1)
    gdf.plot(
        ax=ax,
        column="job_resident_balance",
        cmap="RdBu_r",
        norm=norm,
        linewidth=0.04,
        edgecolor="#ffffff",
        alpha=0.48,
        missing_kwds={"color": "#f5f5f5", "alpha": 0.15},
        zorder=1,
    )
    roads.plot(ax=ax, color="#5f6368", linewidth=0.48, alpha=0.95, zorder=2)
    boundary.boundary.plot(ax=ax, color="#111111", linewidth=0.9, zorder=3)
    ax.set_axis_off()
    ax.set_aspect("equal")
    ax.set_title("Study area: square grid and major roads")

    sm = mpl.cm.ScalarMappable(cmap="RdBu_r", norm=norm)
    sm.set_array([])
    cbar = fig.colorbar(sm, ax=ax, fraction=0.028, pad=0.01)
    cbar.set_label("(jobs $-$ residents) / (jobs $+$ residents)")
    handles = [
        Patch(facecolor=plt.get_cmap("RdBu_r")(0.92), edgecolor="none", label="Job-heavy grid"),
        Patch(facecolor=plt.get_cmap("RdBu_r")(0.08), edgecolor="none", label="Resident-heavy grid"),
        mpl.lines.Line2D([0], [0], color="#5f6368", lw=1.2, alpha=0.95, label="Major roads"),
    ]
    ax.legend(handles=handles, loc="lower left", frameon=False, fontsize=7.5)
    save_slide_fig(fig, out_stem)


def weighted_mean(values: pd.Series, weights: pd.Series) -> float:
    v = pd.to_numeric(values, errors="coerce").astype(float)
    w = pd.to_numeric(weights, errors="coerce").fillna(0.0).astype(float)
    m = v.notna() & np.isfinite(v) & w.gt(0)
    if not m.any():
        return float("nan")
    return float(np.average(v.loc[m], weights=w.loc[m]))


def weighted_share(condition: pd.Series, weights: pd.Series) -> float:
    return weighted_mean(condition.fillna(False).astype(float), weights)


def weighted_median(values: pd.Series, weights: pd.Series) -> float:
    v = pd.to_numeric(values, errors="coerce").astype(float)
    w = pd.to_numeric(weights, errors="coerce").fillna(0.0).astype(float)
    m = v.notna() & np.isfinite(v) & w.gt(0)
    if not m.any():
        return float("nan")
    vv = v.loc[m].to_numpy()
    ww = w.loc[m].to_numpy()
    order = np.argsort(vv)
    vv = vv[order]
    ww = ww[order]
    csum = np.cumsum(ww)
    return float(vv[np.searchsorted(csum, 0.5 * csum[-1], side="left")])


def build_national_jobs_housing_metrics(top_n_cities: int = 50) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    flows = pd.read_stata(ROOT / "raw_data" / "commuting_town_2021.dta")
    centers = pd.read_parquet(ROOT / "raw_data" / "commuting_town_2021_centers.parquet")
    centers = centers.rename(columns={"towncode": "town_code"})
    centers["town_code"] = centers["town_code"].astype(str)
    lon = pd.to_numeric(centers["centroid_lon"], errors="coerce")
    lat = pd.to_numeric(centers["centroid_lat"], errors="coerce")
    miss = lon.isna() | lat.isna()
    lon = lon.fillna(pd.to_numeric(centers.loc[miss, "office_lon"], errors="coerce"))
    lat = lat.fillna(pd.to_numeric(centers.loc[miss, "office_lat"], errors="coerce"))
    centers["lon"] = lon
    centers["lat"] = lat
    centers = centers[["town_code", "lon", "lat"]].dropna()

    intra = flows.loc[
        flows["origin_prov_name"].eq(flows["dest_prov_name"])
        & flows["origin_city_name"].eq(flows["dest_city_name"])
        & ~flows["origin_town_code"].astype(str).eq(flows["dest_town_code"].astype(str))
    ].copy()
    intra["origin_town_code"] = intra["origin_town_code"].astype(str)
    intra["dest_town_code"] = intra["dest_town_code"].astype(str)
    intra["city_key"] = intra["origin_prov_name"].str.strip() + "||" + intra["origin_city_name"].str.strip()

    city_rank = (
        intra.groupby("city_key", as_index=False)
        .agg(total_commuters=("commuters_2021", "sum"), n_town_flows=("commuters_2021", "count"))
        .sort_values("total_commuters", ascending=False)
        .head(top_n_cities)
    )
    top_keys = set(city_rank["city_key"])
    use = intra.loc[intra["city_key"].isin(top_keys)].copy()
    use = use.merge(
        centers.rename(columns={"town_code": "origin_town_code", "lon": "origin_lon", "lat": "origin_lat"}),
        on="origin_town_code",
        how="inner",
    )
    use = use.merge(
        centers.rename(columns={"town_code": "dest_town_code", "lon": "dest_lon", "lat": "dest_lat"}),
        on="dest_town_code",
        how="inner",
    )

    def haversine_km(lon1, lat1, lon2, lat2):
        lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
        return 6371.0 * 2 * np.arcsin(np.sqrt(a))

    use["commute_distance_km"] = haversine_km(use["origin_lon"], use["origin_lat"], use["dest_lon"], use["dest_lat"])

    rows = []
    for city_key, g in use.groupby("city_key"):
        w = g["commuters_2021"]
        total = float(w.sum())
        origin_mass = g.groupby("origin_town_code", as_index=False)["commuters_2021"].sum()
        dest_mass = g.groupby("dest_town_code", as_index=False)["commuters_2021"].sum()
        origin_mass = origin_mass.merge(
            centers.rename(columns={"town_code": "origin_town_code", "lon": "origin_lon", "lat": "origin_lat"}),
            on="origin_town_code",
        )
        dest_mass = dest_mass.merge(
            centers.rename(columns={"town_code": "dest_town_code", "lon": "dest_lon", "lat": "dest_lat"}),
            on="dest_town_code",
        )
        origin_hhi = float(((origin_mass["commuters_2021"] / origin_mass["commuters_2021"].sum()) ** 2).sum())
        dest_hhi = float(((dest_mass["commuters_2021"] / dest_mass["commuters_2021"].sum()) ** 2).sum())
        res_lon = weighted_mean(origin_mass["origin_lon"], origin_mass["commuters_2021"])
        res_lat = weighted_mean(origin_mass["origin_lat"], origin_mass["commuters_2021"])
        work_lon = weighted_mean(dest_mass["dest_lon"], dest_mass["commuters_2021"])
        work_lat = weighted_mean(dest_mass["dest_lat"], dest_mass["commuters_2021"])
        centroid_separation_km = haversine_km(res_lon, res_lat, work_lon, work_lat)
        rows.append(
            {
                "city_key": city_key,
                "total_commuters": total,
                "centroid_separation_km": centroid_separation_km,
                "residence_centroid_lon": res_lon,
                "residence_centroid_lat": res_lat,
                "workplace_centroid_lon": work_lon,
                "workplace_centroid_lat": work_lat,
                "origin_hhi": origin_hhi,
                "dest_hhi": dest_hhi,
                "hhi_ratio": dest_hhi / origin_hhi if origin_hhi > 0 else np.nan,
                "jobs_more_concentrated": bool(dest_hhi > origin_hhi),
                "mean_commute_distance_km": weighted_mean(g["commute_distance_km"], w),
                "n_origin_towns": int(origin_mass.shape[0]),
                "n_dest_towns": int(dest_mass.shape[0]),
            }
        )
    city_metrics = pd.DataFrame(rows).merge(
        city_rank[["city_key", "total_commuters"]].rename(columns={"total_commuters": "rank_total_commuters"}),
        on="city_key",
    )
    city_metrics["city_label"] = city_metrics["city_key"].str.split("||", regex=False).str[-1]
    city_metrics = merge_top50_am_asymmetry(city_metrics)
    city_metrics = city_metrics.sort_values("total_commuters", ascending=False).reset_index(drop=True)

    pooled = {
        "top_n_cities": top_n_cities,
        "pooled_total_commuters": float(city_metrics["total_commuters"].sum()),
        "pooled_centroid_separation_km": weighted_mean(
            city_metrics["centroid_separation_km"], city_metrics["total_commuters"]
        ),
        "median_centroid_separation_km": float(city_metrics["centroid_separation_km"].median()),
        "share_cities_jobs_more_concentrated": float(city_metrics["jobs_more_concentrated"].mean()),
        "pooled_hhi_ratio": weighted_mean(city_metrics["hhi_ratio"], city_metrics["total_commuters"]),
        "corr_centroid_sep_vs_am_ratio": float(
            city_metrics[["centroid_separation_km", "am_symmetry_ratio"]].dropna().corr().iloc[0, 1]
        )
        if city_metrics["am_symmetry_ratio"].notna().sum() >= 5
        else float("nan"),
    }
    return city_metrics, use, pooled


def merge_top50_am_asymmetry(city_metrics: pd.DataFrame) -> pd.DataFrame:
    root = ROOT / "outputs/national_tidal_commuting_top50cities_2021_v1/metrics"
    asym_path = root / "top_commuting_pairs_city_metrics.csv"
    summary_path = root / "top_commuting_pairs_city_summary.csv"
    if not asym_path.exists() or not summary_path.exists():
        city_metrics["am_symmetry_ratio"] = np.nan
        return city_metrics
    asym = pd.read_csv(asym_path)[["city_id", "weighted_am_asymmetry_median"]].rename(
        columns={"weighted_am_asymmetry_median": "am_symmetry_ratio"}
    )
    keys = pd.read_csv(summary_path)[["city_key", "city_id"]]
    asym = keys.merge(asym, on="city_id", how="left")
    return city_metrics.merge(asym[["city_key", "am_symmetry_ratio"]], on="city_key", how="left")


def plot_national_jobs_housing(city_metrics: pd.DataFrame, pooled: dict, out_stem: Path) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(10.5, 4.3))

    ax = axes[0]
    top = city_metrics.nlargest(20, "centroid_separation_km").sort_values("centroid_separation_km", ascending=True)
    ax.barh(top["city_label"], top["centroid_separation_km"], color="#4C78A8", alpha=0.85)
    ax.axvline(pooled["pooled_centroid_separation_km"], color="#B22222", lw=1.1, ls="--")
    ax.set_xlabel("Residence vs.\ workplace centroid distance (km)")
    ax.set_title("Top-20 cities by centroid separation")
    ax.text(
        pooled["pooled_centroid_separation_km"] + 0.15,
        19.2,
        f"pooled = {pooled['pooled_centroid_separation_km']:.1f} km",
        color="#B22222",
        fontsize=7.5,
        va="center",
    )

    ax = axes[1]
    plot_df = city_metrics.dropna(subset=["centroid_separation_km", "am_symmetry_ratio"]).copy()
    sizes = 18 + 120 * np.sqrt(plot_df["total_commuters"] / plot_df["total_commuters"].max())
    ax.scatter(
        plot_df["centroid_separation_km"],
        plot_df["am_symmetry_ratio"],
        s=sizes,
        c="#E15759",
        alpha=0.72,
        edgecolors="white",
        linewidths=0.3,
    )
    ax.set_xlabel("Residence vs.\ workplace centroid distance (km)")
    ax.set_ylabel(r"AM symmetry ratio $\min/\max$ (crawl sample)")
    ax.set_title("Separation vs.\ directional asymmetry")
    ax.grid(alpha=0.2, linewidth=0.5)

    fig.suptitle(
        "National township OD implies spatial mismatch between residence and workplace activity",
        y=1.02,
        fontsize=10.5,
    )
    save_slide_fig(fig, out_stem)


def analyze_beijing_cluster_coverage(out_dir: Path) -> dict:
    metrics_dir = ROOT / "outputs" / "beijing_commuting_direction_evidence_v1" / "metrics"
    cluster_flow = pd.read_csv(metrics_dir / "cluster_to_cluster_flow.csv")
    centers = pd.read_csv(metrics_dir / "residence_job_cluster_centers.csv")
    summary = pd.read_csv(metrics_dir / "commuting_direction_summary.csv")
    odm = pd.read_parquet(ROOT / "outputs" / "beijing_commuting_direction_evidence_v1" / "data" / "od_direction_metrics_square.parquet")

    primary = "car_bus_commuters"
    total = float(odm[primary].sum())
    top14 = cluster_flow.head(14)
    top6x6 = cluster_flow.head(36)

    res_centers = centers.loc[centers["cluster_type"].eq("residents")].copy()
    job_centers = centers.loc[centers["cluster_type"].eq("jobs")].copy()
    res_cov = float(res_centers["cluster_weight"].sum()) / float(odm.groupby("home_grid")[primary].sum().sum())
    job_cov = float(job_centers["cluster_weight"].sum()) / float(odm.groupby("work_grid")[primary].sum().sum())

    row = summary.loc[summary["weight_col"].eq(primary)].iloc[0]
    angle_row = {
        "resultant_length": float(row["angle_resultant_length"]),
        "dominant_angle_deg": float(row["dominant_angle_deg"]),
        "share_work_closer_to_work_job_cluster": float(row["share_work_closer_to_work_job_cluster"]),
    }

    out = {
        "primary_weight": primary,
        "total_car_bus_commuters": total,
        "cluster_selection": {
            "method": "Weighted KMeans on grid centroids, K=6, random_state=20260629",
            "R_labels": "Rank residence clusters R1..R6 by total residents in cluster",
            "J_labels": "Rank job clusters J1..J6 by total jobs in cluster",
            "residence_cluster_coverage_of_home_mass": res_cov,
            "job_cluster_coverage_of_work_mass": job_cov,
        },
        "panel_d_cluster_flow_coverage": {
            "top14_flow_share_of_total": float(top14["flow_weight"].sum() / total),
            "top14_n_pairs": int(len(top14)),
            "all_36_pairs_flow_share_of_total": float(top6x6["flow_weight"].sum() / total),
            "largest_single_pair_share": float(cluster_flow.iloc[0]["flow_share"]),
        },
        "panel_b_direction_diagnostics": angle_row,
        "cluster_weights": {
            "residence": res_centers[["cluster", "cluster_weight"]].to_dict(orient="records"),
            "jobs": job_centers[["cluster", "cluster_weight"]].to_dict(orient="records"),
        },
        "top_cluster_pairs": top14[["res_cluster", "job_cluster", "flow_weight", "flow_share"]].to_dict(orient="records"),
    }
    (out_dir / "beijing_cluster_coverage_audit.json").write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    return out


def plot_improved_direction_panels(out_stem: Path) -> None:
    """Panel-style figure: stronger direction rose + cluster flow with coverage labels."""
    metrics_dir = ROOT / "outputs" / "beijing_commuting_direction_evidence_v1" / "metrics"
    odm = pd.read_parquet(ROOT / "outputs" / "beijing_commuting_direction_evidence_v1" / "data" / "od_direction_metrics_square.parquet")
    cluster_flow = pd.read_csv(metrics_dir / "cluster_to_cluster_flow.csv")
    centers = pd.read_csv(metrics_dir / "residence_job_cluster_centers.csv")
    boundary = gpd.read_file(ROOT / "raw_data" / "gis" / "map" / "北京市边界.shp").to_crs(TARGET_CRS)

    primary = "car_bus_commuters"
    sample = odm.loc[(odm[primary] > 0) & (odm["centroid_distance_km"] >= 1.0)].copy()
    toward = sample.loc[sample["delta_dist_to_work_job_cluster_km"] < 0].copy()

    fig = plt.figure(figsize=(10.5, 4.5))
    gs = GridSpec(1, 2, width_ratios=[0.95, 1.05], wspace=0.18)
    ax_rose = fig.add_subplot(gs[0, 0], projection="polar")
    ax_cluster = fig.add_subplot(gs[0, 1])

    bins = np.linspace(-np.pi, np.pi, 17)
    hist_all, edges = np.histogram(sample["angle_rad"], bins=bins, weights=sample[primary])
    hist_in, _ = np.histogram(toward["angle_rad"], bins=bins, weights=toward[primary])
    share_all = hist_all / hist_all.sum()
    share_in = hist_in / hist_in.sum() if hist_in.sum() > 0 else hist_in
    centers_a = edges[:-1] + np.diff(edges) / 2
    ax_rose.bar(centers_a, share_all, width=np.diff(edges) * 0.92, color="#cccccc", alpha=0.8, label="All AM flows")
    ax_rose.bar(centers_a, share_in, width=np.diff(edges) * 0.92, color="#E15759", alpha=0.85, label="Inward to job cluster")
    ax_rose.set_theta_zero_location("E")
    ax_rose.set_theta_direction(1)
    ax_rose.set_xticks([0, np.pi / 2, np.pi, 3 * np.pi / 2])
    ax_rose.set_xticklabels(["E", "N", "W", "S"])
    ax_rose.set_yticklabels([])
    ax_rose.set_title("Direction angle (inward flows highlighted)", pad=14)
    ax_rose.legend(loc="upper left", bbox_to_anchor=(0.55, 1.18), fontsize=7, frameon=False)

    boundary.boundary.plot(ax=ax_cluster, color="#333333", linewidth=0.5)
    lookup = centers.set_index("cluster")
    top = cluster_flow.head(14)
    total = float(odm[primary].sum())
    cov = float(top["flow_weight"].sum() / total)
    max_flow = float(top["flow_weight"].max())
    for row in top.itertuples(index=False):
        rx, ry = lookup.loc[row.res_cluster, ["x_m", "y_m"]]
        jx, jy = lookup.loc[row.job_cluster, ["x_m", "y_m"]]
        lw = 0.4 + 2.8 * np.sqrt(row.flow_weight / max_flow)
        ax_cluster.annotate(
            "",
            xy=(jx, ry),
            xytext=(rx, ry),
            arrowprops={"arrowstyle": "-|>", "color": "#222222", "lw": lw, "alpha": 0.38, "mutation_scale": 7},
        )
    res = centers.loc[centers["cluster_type"].eq("residents")]
    job = centers.loc[centers["cluster_type"].eq("jobs")]
    ax_cluster.scatter(res["x_m"], res["y_m"], s=120 + 180 * np.sqrt(res["cluster_weight"] / res["cluster_weight"].max()), c="#4C78A8", edgecolors="white", linewidths=0.5)
    ax_cluster.scatter(job["x_m"], job["y_m"], s=120 + 180 * np.sqrt(job["cluster_weight"] / job["cluster_weight"].max()), c="#E15759", marker="s", edgecolors="white", linewidths=0.5)
    for row in centers.itertuples(index=False):
        ax_cluster.text(row.x_m, row.y_m, row.cluster, ha="center", va="center", fontsize=6.5, color="white", fontweight="bold")
    ax_cluster.set_axis_off()
    ax_cluster.set_title(f"Top 14 residence$\\to$job cluster pairs ({100*cov:.1f}% of car+bus flow)")
    save_slide_fig(fig, out_stem)


def lonlat_to_tile(lon: float, lat: float, zoom: int) -> tuple[int, int]:
    lat = min(max(lat, -85.05112878), 85.05112878)
    n = 2**zoom
    x = int((lon + 180.0) / 360.0 * n)
    lat_rad = math.radians(lat)
    y = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
    return max(0, min(n - 1, x)), max(0, min(n - 1, y))


def lonlat_to_mercator(lon: float, lat: float) -> tuple[float, float]:
    radius = 6378137.0
    lat = min(max(lat, -85.05112878), 85.05112878)
    x = radius * math.radians(lon)
    y = radius * math.log(math.tan(math.pi / 4.0 + math.radians(lat) / 2.0))
    return x, y


def tile_bounds_mercator(x: int, y: int, zoom: int) -> tuple[float, float, float, float]:
    n = 2**zoom

    def tile_to_lonlat(tx: int, ty: int) -> tuple[float, float]:
        lon = tx / n * 360.0 - 180.0
        lat_rad = math.atan(math.sinh(math.pi * (1.0 - 2.0 * ty / n)))
        return lon, math.degrees(lat_rad)

    west, north = tile_to_lonlat(x, y)
    east, south = tile_to_lonlat(x + 1, y + 1)
    minx, maxy = lonlat_to_mercator(west, north)
    maxx, miny = lonlat_to_mercator(east, south)
    return minx, miny, maxx, maxy


def fetch_osm_tile(z: int, x: int, y: int) -> Image.Image:
    OSM_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = OSM_CACHE_DIR / f"{z}_{x}_{y}.png"
    if cache_path.exists():
        return Image.open(cache_path).convert("RGB")
    request = urllib.request.Request(
        OSM_TILE_URL.format(z=z, x=x, y=y),
        headers={"User-Agent": "TidalLanes-preslide/1.0 (local research map QA)"},
    )
    with urllib.request.urlopen(request, timeout=12) as response:
        tile = Image.open(io.BytesIO(response.read())).convert("RGB")
    tile.save(cache_path)
    time.sleep(0.05)
    return tile


def add_osm_basemap(ax: plt.Axes, bounds_3857: tuple[float, float, float, float], zoom: int = 13) -> bool:
    xmin, ymin, xmax, ymax = bounds_3857
    lon_min, lat_min = lonlat_to_mercator_to_lonlat(xmin, ymin)
    lon_max, lat_max = lonlat_to_mercator_to_lonlat(xmax, ymax)
    x0, y0 = lonlat_to_tile(lon_min, lat_max, zoom)
    x1, y1 = lonlat_to_tile(lon_max, lat_min, zoom)
    xs = range(min(x0, x1), max(x0, x1) + 1)
    ys = range(min(y0, y1), max(y0, y1) + 1)
    if len(xs) * len(ys) > 49 and zoom > 11:
        return add_osm_basemap(ax, bounds_3857, zoom - 1)

    tile_size = 256
    mosaic = Image.new("RGB", (len(xs) * tile_size, len(ys) * tile_size), "#f4f4f4")
    try:
        for ix, x in enumerate(xs):
            for iy, y in enumerate(ys):
                mosaic.paste(fetch_osm_tile(zoom, x, y), (ix * tile_size, iy * tile_size))
    except Exception:
        return False
    west, _south, _east, north = tile_bounds_mercator(min(xs), min(ys), zoom)
    _west, south, east, _north = tile_bounds_mercator(max(xs), max(ys), zoom)
    ax.imshow(np.asarray(mosaic), extent=(west, east, south, north), origin="upper", alpha=0.90, zorder=0)
    return True


def lonlat_to_mercator_to_lonlat(x: float, y: float) -> tuple[float, float]:
    radius = 6378137.0
    lon = math.degrees(x / radius)
    lat = math.degrees(2.0 * math.atan(math.exp(y / radius)) - math.pi / 2.0)
    return lon, lat


def within_center_radius(gdf: gpd.GeoDataFrame, center_lonlat: tuple[float, float], radius_m: float) -> gpd.GeoDataFrame:
    center_pt = gpd.GeoSeries([Point(center_lonlat)], crs="EPSG:4326").to_crs(gdf.crs).iloc[0]
    midpts = gdf.geometry.interpolate(0.5, normalized=True)
    return gdf.loc[midpts.distance(center_pt) <= radius_m].copy()


def load_centerline_am_assets() -> tuple[gpd.GeoDataFrame, pd.DataFrame]:
    version_root = ROOT / "outputs" / STAGE07_VERSION / "data"
    cl_dir = gpd.read_parquet(version_root / "centerline_dir_master.parquet")
    asym = pd.read_parquet(version_root / "centerline_asymmetry_table.parquet")
    am = asym[
        (asym["peak"] == "AM")
        & (~asym["miss_AB"])
        & (~asym["miss_BA"])
        & asym["ratio"].notna()
    ].copy()
    cl_map = within_center_radius(cl_dir, TIANANMEN_LONLAT, CENTERLINE_MAP_RADIUS_M)
    return cl_map, am


def centerline_am_summary(am: pd.DataFrame) -> dict[str, float | int]:
    ratio = am["ratio"].astype(float)
    n_both = int(len(am))
    return {
        "n_two_way": n_both,
        "median_ratio": float(ratio.median()),
        "share_le_0_90": float((ratio <= 0.90).mean()),
    }


def plot_preslide_centerline_am_map(cl_dir: gpd.GeoDataFrame, am: pd.DataFrame, out_stem: Path) -> None:
    from src.diagnostics.preslide_asymmetry_style import (
        FIG_MAP,
        FONT,
        SAR_CMAP,
        SAR_TITLE,
        SAR_VMAX,
        SAR_VMIN,
        configure_preslide_asymmetry_mpl,
    )

    configure_preslide_asymmetry_mpl()
    fig, ax = plt.subplots(figsize=FIG_MAP)
    cl_map = cl_dir.to_crs(WEB_MERCATOR_CRS)
    am_ids = set(am["cline_id"].astype(int))
    lines = cl_map[cl_map["cline_id"].isin(am_ids)].merge(
        am[["cline_id", "ratio"]].drop_duplicates("cline_id"),
        on="cline_id",
        how="inner",
    )
    bounds = lines.geometry.bounds
    minx, miny = bounds["minx"].min(), bounds["miny"].min()
    maxx, maxy = bounds["maxx"].max(), bounds["maxy"].max()
    pad = 900.0
    view_bounds = (minx - pad, miny - pad, maxx + pad, maxy + pad)
    ax.set_xlim(view_bounds[0], view_bounds[2])
    ax.set_ylim(view_bounds[1], view_bounds[3])

    bg = cl_map.cx[view_bounds[0] : view_bounds[2], view_bounds[1] : view_bounds[3]]
    has_osm = add_osm_basemap(ax, view_bounds, zoom=13)
    if has_osm:
        ax.add_patch(
            Rectangle(
                (view_bounds[0], view_bounds[1]),
                view_bounds[2] - view_bounds[0],
                view_bounds[3] - view_bounds[1],
                facecolor="white",
                edgecolor="none",
                alpha=0.22,
                zorder=1,
            )
        )
    bg.plot(ax=ax, color="#bdbdbd", linewidth=0.28, alpha=0.35, zorder=2)

    ratio_by_cline = lines.drop_duplicates("cline_id").set_index("cline_id")["ratio"].astype(float)
    cmap = plt.get_cmap(SAR_CMAP)
    norm = mpl.colors.Normalize(vmin=SAR_VMIN, vmax=SAR_VMAX)
    seg_coords = []
    seg_colors = []
    line_width = 1.35
    for row in lines.itertuples(index=False):
        xs, ys = row.geometry.xy
        seg_coords.append(np.column_stack([xs, ys]))
        seg_colors.append(cmap(norm(float(ratio_by_cline.loc[row.cline_id]))))
    lc = LineCollection(
        seg_coords,
        colors=seg_colors,
        linewidths=line_width,
        alpha=0.92,
        zorder=4,
    )
    ax.add_collection(lc)

    sm = mpl.cm.ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])
    cbar = fig.colorbar(sm, ax=ax, fraction=0.035, pad=0.01)
    cbar.set_label("Speed asymmetry ratio", fontsize=FONT["cbar_label"])
    cbar.ax.tick_params(labelsize=FONT["cbar_tick"])
    ax.set_title(f"{SAR_TITLE} (map, ~{CENTERLINE_MAP_RADIUS_M // 1000} km)", fontsize=FONT["title"])
    ax.set_axis_off()
    save_slide_fig(fig, out_stem)


def plot_preslide_centerline_am_hist(am: pd.DataFrame, out_stem: Path, summary: dict[str, float | int]) -> None:
    from src.diagnostics.preslide_asymmetry_style import (
        FIG_HIST,
        SAR_COLOR_SINGLE,
        SAR_TITLE,
        add_sar_stats_box,
        configure_preslide_asymmetry_mpl,
        finalize_sar_figure,
        plot_sar_bar_histogram,
        plot_sar_median_line,
        style_sar_axes,
    )

    configure_preslide_asymmetry_mpl()
    fig, ax = plt.subplots(figsize=FIG_HIST)
    ratio = am["ratio"].astype(float).to_numpy()
    plot_sar_bar_histogram(ax, ratio, color=SAR_COLOR_SINGLE)
    plot_sar_median_line(ax, summary["median_ratio"])
    add_sar_stats_box(
        ax,
        f"AM two-way centerlines: {summary['n_two_way']:,}\n"
        f"median = {summary['median_ratio']:.3f}\n"
        f"share $\\leq 0.90$ = {100 * summary['share_le_0_90']:.1f}%",
    )
    ax.set_title(SAR_TITLE)
    style_sar_axes(ax, ylabel="Centerline segments")
    finalize_sar_figure(fig)
    save_slide_fig(fig, out_stem)


def main() -> None:
    args = parse_args()
    configure_matplotlib()
    out_root = ROOT / "outputs" / args.output_name
    fig_dir = out_root / "figures"
    metrics_dir = out_root / "metrics"
    fig_dir.mkdir(parents=True, exist_ok=True)
    metrics_dir.mkdir(parents=True, exist_ok=True)
    SLIDE_FIG_DIR.mkdir(parents=True, exist_ok=True)

    edges = load_gate5_edges_with_equilibrium()
    coords = load_grid_coords()
    pair = build_reciprocal_edge_table(edges, coords)
    pair.to_csv(metrics_dir / "gate5_reciprocal_link_asymmetry.csv", index=False)

    mot_stats = plot_motivation_am_asymmetry(pair, fig_dir / "slide_motivation_am_asymmetry")
    plot_study_network_overlay(fig_dir / "slide_data_study_network")

    city_metrics, _, pooled = build_national_jobs_housing_metrics(top_n_cities=50)
    city_metrics.to_csv(metrics_dir / "national_jobs_housing_city_metrics_top50.csv", index=False)
    plot_national_jobs_housing(city_metrics, pooled, fig_dir / "slide_national_jobs_housing")

    cluster_audit = analyze_beijing_cluster_coverage(metrics_dir)
    plot_improved_direction_panels(fig_dir / "slide_beijing_direction_clusters_improved")

    cl_dir, am_centerline = load_centerline_am_assets()
    centerline_summary = centerline_am_summary(am_centerline)
    plot_preslide_centerline_am_map(cl_dir, am_centerline, fig_dir / "slide_bj_centerline_map")
    plot_preslide_centerline_am_hist(am_centerline, fig_dir / "slide_bj_centerline_hist", centerline_summary)

    # Copy slide-ready PNGs
    copies = {
        "slide_motivation_am_asymmetry.png": "motivation_am_asymmetry.png",
        "slide_data_study_network.png": "slide_data_study_network.png",
        "slide_national_jobs_housing.png": "pat_national_jobs_housing.png",
        "slide_beijing_direction_clusters_improved.png": "pat_beijing_direction_clusters.png",
        "slide_bj_centerline_map.png": "pat_bj_centerline_map.png",
        "slide_bj_centerline_hist.png": "pat_bj_centerline_hist.png",
    }
    for src, dst in copies.items():
        src_path = fig_dir / src
        if src_path.exists():
            (SLIDE_FIG_DIR / dst).write_bytes(src_path.read_bytes())

    manifest = {
        "motivation_am_asymmetry": mot_stats,
        "national_jobs_housing_pooled": pooled,
        "beijing_cluster_coverage_audit": cluster_audit,
        "centerline_am_summary": centerline_summary,
        "slide_fig_dir": str(SLIDE_FIG_DIR),
        "outputs": str(out_root),
    }
    (metrics_dir / "preslide_figure_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"[generate_preslide_pattern_figures] wrote figures to {fig_dir}")
    print(f"[generate_preslide_pattern_figures] copied slide PNGs to {SLIDE_FIG_DIR}")


if __name__ == "__main__":
    main()
