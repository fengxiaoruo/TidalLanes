"""First-pass diagnostics for the 2026-07-10 coauthor revision requests."""
from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import matplotlib.pyplot as plt
from matplotlib import font_manager
import numpy as np
import pandas as pd

DATA_WORK = Path(__file__).resolve().parents[3]
if str(DATA_WORK) not in sys.path:
    sys.path.insert(0, str(DATA_WORK))

from src.model.directional_cost.policy_counterfactuals import outer_ring_mask  # noqa: E402
from src.model.full_estimation import build_primitives as bp  # noqa: E402
from src.model.full_estimation import parameters as pm  # noqa: E402
from src.model.full_estimation import stage2_counterfactuals as s2  # noqa: E402
from src.model.full_estimation.corridor_exposure import od_path_exposure  # noqa: E402
from src.model.full_estimation.counterfactual_levels import (  # noqa: E402
    apply_bidirectional_lane_delta,
)
from src.model.full_estimation.equilibrium_solve import (  # noqa: E402
    MAINLINE_ALPHA,
    MAINLINE_BETA,
    MAINLINE_CONGESTION_FORM,
    MAINLINE_LAMBDA_PHYS,
    MAINLINE_PAIR_SELECTION,
    solve_equilibrium,
)
from src.model.full_estimation.phase_metrics_common import (  # noqa: E402
    data_work_root,
    grid_xy,
    node_distance_to_hub,
)
from src.model.full_estimation.run_alpha_robustness_quick_gate5 import (  # noqa: E402
    solver_overrides_for_params,
)

LOG = logging.getLogger("coauthor_revision_attempts")
EPS = 1e-12
GRID_KM = 3.0

DEFAULT_OUTPUT = "coauthor_revision_attempts_20260710_v1"
DEFAULT_MODEL_INPUT = "model_input_edgeproj_v2_lcc_gate5_v1"
DEFAULT_STAGE2 = "stage2_endogenous_edgeproj_v2_lcc_gate5_bpr_pos05_v1"
DEFAULT_SYMMETRIC = "symmetric_input_reestimated_gate5_bpr_pos05_v1"
DEFAULT_TIDAL_DIAG = "tidal_lane_diagnostics_edgeproj_v2_lcc_gate5_bpr_pos05_v1"
DEFAULT_PHASE_B = "phase_b_sorting_patterns_gate5_bpr_pos05_v1"
DEFAULT_NATIONAL_GROUP = "national_center_periphery_group_diagnostics_v1"


def _setup_style() -> None:
    font_candidates = [
        "Arial Unicode MS",
        "PingFang SC",
        "Songti SC",
        "Heiti SC",
        "Noto Sans CJK SC",
        "DejaVu Sans",
    ]
    available = {f.name for f in font_manager.fontManager.ttflist}
    font_family = next((f for f in font_candidates if f in available), "DejaVu Sans")
    plt.rcParams.update({
        "figure.dpi": 200,
        "savefig.dpi": 300,
        "font.family": font_family,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.linewidth": 0.8,
        "xtick.direction": "out",
        "ytick.direction": "out",
        "legend.frameon": False,
        "axes.grid": False,
    })


def _savefig(fig: plt.Figure, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    for ext in (".png", ".pdf", ".svg"):
        fig.savefig(path.with_suffix(ext), bbox_inches="tight")
    plt.close(fig)


def _write_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, default=float))


def _weighted_regression(x: np.ndarray, y: np.ndarray, w: np.ndarray | None = None) -> dict:
    x = np.asarray(x, dtype=float)
    y = np.asarray(y, dtype=float)
    mask = np.isfinite(x) & np.isfinite(y)
    if w is None:
        w = np.ones_like(y)
    else:
        w = np.asarray(w, dtype=float)
        mask &= np.isfinite(w) & (w > 0)
    x = x[mask]
    y = y[mask]
    w = w[mask]
    if len(x) < 3:
        return {"n": int(len(x)), "slope_per_10km": np.nan, "intercept": np.nan, "r2": np.nan}
    X = np.column_stack([np.ones_like(x), x])
    sw = np.sqrt(w / np.mean(w))
    beta = np.linalg.lstsq(X * sw[:, None], y * sw, rcond=None)[0]
    yhat = X @ beta
    ybar = np.average(y, weights=w)
    ssr = float(np.sum(w * (y - yhat) ** 2))
    sst = float(np.sum(w * (y - ybar) ** 2))
    return {
        "n": int(len(x)),
        "slope_per_10km": float(beta[1] * 10.0),
        "intercept": float(beta[0]),
        "r2": float(1.0 - ssr / max(sst, EPS)),
    }


def _weighted_summary(values: np.ndarray, weights: np.ndarray | None = None) -> dict:
    v = np.asarray(values, dtype=float)
    mask = np.isfinite(v)
    if weights is None:
        w = np.ones_like(v)
    else:
        w = np.asarray(weights, dtype=float)
        mask &= np.isfinite(w) & (w > 0)
    v = v[mask]
    w = w[mask]
    if len(v) == 0:
        return {
            "n": 0, "mean": np.nan, "median": np.nan, "p10": np.nan,
            "p90": np.nan, "l2": np.nan, "variance": np.nan,
        }
    order = np.argsort(v)
    vs = v[order]
    ws = w[order]
    cdf = np.cumsum(ws) / np.sum(ws)
    def q(p: float) -> float:
        idx = int(np.searchsorted(cdf, p, side="left"))
        idx = min(max(idx, 0), len(vs) - 1)
        return float(vs[idx])
    mean = float(np.average(v, weights=w))
    return {
        "n": int(len(v)),
        "mean": mean,
        "median": q(0.5),
        "p10": q(0.1),
        "p90": q(0.9),
        "l2": float(np.sqrt(np.average(v ** 2, weights=w))),
        "variance": float(np.average((v - mean) ** 2, weights=w)),
    }


def run_national_attempt(data_work: Path, out: Path, group_dir: str) -> None:
    task = out / "01_national_large_city_jobs_housing"
    metrics = task / "metrics"
    figures = task / "figures"
    metrics.mkdir(parents=True, exist_ok=True)
    figures.mkdir(parents=True, exist_ok=True)

    src = data_work / "outputs" / group_dir / "metrics"
    group = pd.read_csv(src / "threshold_group_summary.csv")
    city = pd.read_csv(src / "threshold_city_flows_wide.csv")

    group["center_bound_imbalance_pp"] = 100.0 * group["P_to_C_minus_C_to_P_share"]
    city["center_bound_imbalance_pp"] = 100.0 * city["P_to_C_minus_C_to_P_share"]
    group.to_csv(metrics / "large_city_group_threshold_summary.csv", index=False)

    sens = []
    for name, g in group.groupby("city_group"):
        vals = g["center_bound_imbalance_pp"].to_numpy()
        sens.append({
            "city_group": name,
            "n_thresholds": int(len(g)),
            "min_imbalance_pp": float(np.nanmin(vals)),
            "max_imbalance_pp": float(np.nanmax(vals)),
            "range_imbalance_pp": float(np.nanmax(vals) - np.nanmin(vals)),
            "mean_imbalance_pp": float(np.nanmean(vals)),
            "all_positive": bool(np.all(vals > 0)),
        })
    pd.DataFrame(sens).to_csv(metrics / "spatial_unit_threshold_sensitivity.csv", index=False)

    city30 = city[np.isclose(city["core_workplace_share"], 0.30)].copy()
    city30 = city30.sort_values("rank_by_inter_town_flow")
    city30.to_csv(metrics / "city_ranked_30pct_summary.csv", index=False)
    city30.head(40).to_csv(metrics / "top40_city_30pct_summary.csv", index=False)

    fig, ax = plt.subplots(figsize=(6.6, 4.2))
    color = {
        "all_cities": "#737373",
        "top100_flow": "#4C78A8",
        "top50_flow": "#59A14F",
        "top20_flow": "#F28E2B",
        "first_tier_manual": "#B07AA1",
    }
    order = ["all_cities", "top100_flow", "top50_flow", "top20_flow", "first_tier_manual"]
    for name in order:
        g = group[group["city_group"] == name].sort_values("core_workplace_share")
        if g.empty:
            continue
        ax.plot(
            100.0 * g["core_workplace_share"], g["center_bound_imbalance_pp"],
            marker="o", lw=1.6, ms=4, label=name.replace("_", " "),
            color=color.get(name),
        )
    ax.axhline(0, color="black", lw=0.8)
    ax.set_xlabel("Employment-core threshold (% of workplace commuters)")
    ax.set_ylabel("Center-bound minus reverse share (pp)")
    ax.set_title("Jobs-housing directionality is clearer in large cities", loc="left")
    ax.legend(ncol=2, fontsize=8)
    _savefig(fig, figures / "figure_large_city_threshold_sensitivity")

    top20 = city30.head(20).iloc[::-1].copy()
    fig, ax = plt.subplots(figsize=(6.8, 5.4))
    ax.barh(top20["city_name"], top20["center_bound_imbalance_pp"], color="#4C78A8")
    ax.axvline(0, color="black", lw=0.8)
    ax.set_xlabel("Center-bound minus reverse share (pp)")
    ax.set_ylabel("")
    ax.set_title("Top-flow cities at the 30% employment-core definition", loc="left")
    _savefig(fig, figures / "figure_top20_city_center_bound_imbalance_30pct")

    summary = {
        "source_output": group_dir,
        "n_city_threshold_rows": int(len(city)),
        "n_group_threshold_rows": int(len(group)),
        "top20_30pct_mean_imbalance_pp": float(city30.head(20)["center_bound_imbalance_pp"].mean()),
        "top50_30pct_mean_imbalance_pp": float(
            group[(group["city_group"] == "top50_flow") & np.isclose(group["core_workplace_share"], 0.30)]
            ["center_bound_imbalance_pp"].iloc[0]
        ),
        "first_tier_30pct_imbalance_pp": float(
            group[(group["city_group"] == "first_tier_manual") & np.isclose(group["core_workplace_share"], 0.30)]
            ["center_bound_imbalance_pp"].iloc[0]
        ),
    }
    _write_json(metrics / "summary.json", summary)


def _ring_band(dist_km: pd.Series) -> pd.Series:
    bins = [-np.inf, 10.0, 18.0, 28.0, 38.0, np.inf]
    labels = ["0_10km_core", "10_18km_inner", "18_28km_middle", "28_38km_six_ring_proxy", "38km_plus_outer"]
    return pd.cut(dist_km, bins=bins, labels=labels)


def run_symmetric_attempt(data_work: Path, out: Path, model_input: str, symmetric_dir: str) -> None:
    task = out / "02_symmetric_fundamental_distribution"
    metrics = task / "metrics"
    figures = task / "figures"
    metrics.mkdir(parents=True, exist_ok=True)
    figures.mkdir(parents=True, exist_ok=True)

    prim = bp.load_primitives(data_work / "outputs" / model_input, period="AM")
    params = replace(pm.Parameters(), alpha=MAINLINE_ALPHA, beta=MAINLINE_BETA)
    x, y = grid_xy(prim)
    dist = node_distance_to_hub(prim)
    dist_km = dist * GRID_KM

    root = data_work / "outputs" / symmetric_dir / "baseline_distribution"
    directed = pd.read_csv(root / "directed_main" / "nodes.csv").add_suffix("_directed")
    symmetric = pd.read_csv(root / "symmetric_reestimated" / "nodes.csv").add_suffix("_symmetric")
    df = directed.merge(
        symmetric,
        left_on="node_idx_directed",
        right_on="node_idx_symmetric",
        how="inner",
        validate="one_to_one",
    )
    df["node_idx"] = df["node_idx_directed"].astype(int)
    df["grid_id"] = df["grid_id_directed"]
    df["sq_x"] = x[df["node_idx"].to_numpy()]
    df["sq_y"] = y[df["node_idx"].to_numpy()]
    df["dist_to_center_grid"] = dist[df["node_idx"].to_numpy()]
    df["dist_to_center_km"] = dist_km[df["node_idx"].to_numpy()]
    df["distance_band"] = _ring_band(df["dist_to_center_km"]).astype(str)

    theta = params.theta
    alpha = params.alpha
    beta = params.beta
    lbar = prim.Lbar
    for world in ("directed", "symmetric"):
        lr = np.clip(df[f"l_R_model_{world}"].to_numpy() * lbar, EPS, None)
        lf = np.clip(df[f"l_F_model_{world}"].to_numpy() * lbar, EPS, None)
        df[f"log_ubar_{world}"] = np.log(np.clip(df[f"ubar_theta_{world}"], EPS, None)) / theta
        df[f"log_abar_{world}"] = np.log(np.clip(df[f"abar_theta_{world}"], EPS, None)) / theta
        df[f"log_u_effective_{world}"] = df[f"log_ubar_{world}"] + beta * np.log(lr)
        df[f"log_wage_{world}"] = df[f"log_abar_{world}"] + alpha * np.log(lf)
        df[f"log_Pi_inv_theta_{world}"] = -theta * np.log(np.clip(df[f"Pi_i_{world}"], EPS, None))
        df[f"log_P_inv_theta_{world}"] = -theta * np.log(np.clip(df[f"P_j_{world}"], EPS, None))

    metrics_cols = ["log_ubar", "log_abar", "log_u_effective", "log_wage", "log_Pi_inv_theta", "log_P_inv_theta"]
    for col in metrics_cols:
        df[f"delta_{col}"] = df[f"{col}_symmetric"] - df[f"{col}_directed"]

    df.to_csv(metrics / "node_symmetric_minus_directed_fundamentals.csv", index=False)

    weights = {
        "log_ubar": np.clip(prim.L_R_obs, EPS, None),
        "log_u_effective": np.clip(prim.L_R_obs, EPS, None),
        "log_abar": np.clip(prim.L_F_obs, EPS, None),
        "log_wage": np.clip(prim.L_F_obs, EPS, None),
        "log_Pi_inv_theta": np.clip(prim.L_R_obs, EPS, None),
        "log_P_inv_theta": np.clip(prim.L_F_obs, EPS, None),
    }
    summary_rows = []
    gradient_rows = []
    for col in metrics_cols:
        summ = _weighted_summary(df[f"delta_{col}"].to_numpy(), weights[col])
        summ.update({"metric": col, "weight": "residence" if "u" in col or "Pi" in col else "workplace"})
        summary_rows.append(summ)
        grad = _weighted_regression(df["dist_to_center_km"].to_numpy(), df[f"delta_{col}"].to_numpy(), weights[col])
        grad.update({"metric": col, "weight": summ["weight"]})
        gradient_rows.append(grad)
    pd.DataFrame(summary_rows).to_csv(metrics / "fundamental_delta_distribution_summary.csv", index=False)
    pd.DataFrame(gradient_rows).to_csv(metrics / "fundamental_distance_gradients.csv", index=False)

    band_rows = []
    for band, g in df.groupby("distance_band", observed=True):
        for col in metrics_cols:
            summ = _weighted_summary(g[f"delta_{col}"].to_numpy(), weights[col][g.index.to_numpy()])
            summ.update({"distance_band": band, "metric": col})
            band_rows.append(summ)
    pd.DataFrame(band_rows).to_csv(metrics / "fundamental_delta_by_distance_band.csv", index=False)

    profile = []
    df["dist_bin"] = pd.qcut(df["dist_to_center_km"], 12, duplicates="drop")
    for b, g in df.groupby("dist_bin", observed=True):
        row = {
            "dist_mid_km": float(g["dist_to_center_km"].mean()),
            "n_nodes": int(len(g)),
        }
        for col in ("log_ubar", "log_abar", "log_wage"):
            w = weights[col][g.index.to_numpy()]
            row[f"mean_delta_{col}"] = float(np.average(g[f"delta_{col}"], weights=w))
        profile.append(row)
    prof = pd.DataFrame(profile)
    prof.to_csv(metrics / "fundamental_radial_profile.csv", index=False)

    fig, axes = plt.subplots(1, 3, figsize=(11.0, 3.2), sharex=True)
    panel = [
        ("mean_delta_log_ubar", "Residence fundamental"),
        ("mean_delta_log_abar", "Workplace fundamental"),
        ("mean_delta_log_wage", "Wage component"),
    ]
    for ax, (col, title) in zip(axes, panel):
        ax.plot(prof["dist_mid_km"], prof[col], marker="o", lw=1.5, ms=3, color="#4C78A8")
        ax.axhline(0, color="black", lw=0.8)
        ax.set_title(title, loc="left", fontsize=10)
        ax.set_xlabel("Distance to employment center (km)")
    axes[0].set_ylabel("Symmetric minus directed log value")
    fig.tight_layout()
    _savefig(fig, figures / "figure_symmetric_fundamental_radial_profiles")

    fig, axes = plt.subplots(1, 3, figsize=(10.5, 3.2))
    for ax, col, title in zip(
        axes,
        ("delta_log_ubar", "delta_log_abar", "delta_log_wage"),
        ("Residence fundamental", "Workplace fundamental", "Wage component"),
    ):
        ax.hist(df[col], bins=50, color="#4C78A8", alpha=0.85)
        ax.axvline(0, color="black", lw=0.8)
        ax.set_title(title, loc="left", fontsize=10)
        ax.set_xlabel("Symmetric minus directed")
    axes[0].set_ylabel("Nodes")
    fig.tight_layout()
    _savefig(fig, figures / "figure_symmetric_fundamental_delta_distributions")

    for col, title, fname in [
        ("delta_log_ubar", "Residence fundamental", "map_delta_log_ubar"),
        ("delta_log_abar", "Workplace fundamental", "map_delta_log_abar"),
        ("delta_log_wage", "Wage component", "map_delta_log_wage"),
    ]:
        vmax = max(float(df[col].abs().quantile(0.98)), 1e-6)
        fig, ax = plt.subplots(figsize=(5.3, 4.9))
        sc = ax.scatter(df["sq_x"], df["sq_y"], c=df[col].clip(-vmax, vmax), s=10, cmap="RdBu_r", linewidths=0)
        fig.colorbar(sc, ax=ax, label="Symmetric minus directed")
        ax.set_aspect("equal")
        ax.set_xlabel("grid x")
        ax.set_ylabel("grid y")
        ax.set_title(title, loc="left")
        _savefig(fig, figures / fname)


def run_tidal_exposure_attempt(data_work: Path, out: Path, model_input: str, tidal_dir: str) -> None:
    task = out / "03_tidal_time_savings_exposure"
    metrics = task / "metrics"
    figures = task / "figures"
    metrics.mkdir(parents=True, exist_ok=True)
    figures.mkdir(parents=True, exist_ok=True)

    prim = bp.load_primitives(data_work / "outputs" / model_input, period="AM")
    detail = pd.read_csv(data_work / "outputs" / tidal_dir / "tidal_treated_edge_detail.csv")
    detail = detail[detail["spec"].eq("top100_d1.0")].copy()
    detail["gross_saving_person_min_base"] = np.maximum(-detail["delta_tau_min"], 0.0) * detail["Xi_base"]
    detail["gross_loss_person_min_base"] = np.maximum(detail["delta_tau_min"], 0.0) * detail["Xi_base"]
    detail["net_saving_person_min_base"] = -detail["delta_tau_min"] * detail["Xi_base"]
    detail["edge_passages_base"] = detail["Xi_base"]
    detail["edge_passages_scenario"] = detail["Xi_scenario"]
    detail.to_csv(metrics / "tidal_top100_segment_person_time_detail.csv", index=False)

    rows = []
    for horizon, g in detail.groupby("horizon"):
        rows.append({
            "spec": "top100_d1.0",
            "horizon": horizon,
            "n_edge_rows": int(len(g)),
            "n_unique_edges": int(g["edge_idx"].nunique()),
            "edge_passages_base": float(g["edge_passages_base"].sum()),
            "slow_edge_passages_base": float(g.loc[g["role"].eq("slow"), "edge_passages_base"].sum()),
            "fast_edge_passages_base": float(g.loc[g["role"].eq("fast"), "edge_passages_base"].sum()),
            "gross_saving_person_min_base": float(g["gross_saving_person_min_base"].sum()),
            "gross_loss_person_min_base": float(g["gross_loss_person_min_base"].sum()),
            "net_saving_person_min_base": float(g["net_saving_person_min_base"].sum()),
            "gross_saving_person_hours_base": float(g["gross_saving_person_min_base"].sum() / 60.0),
            "net_saving_person_hours_base": float(g["net_saving_person_min_base"].sum() / 60.0),
            "mean_delta_tau_sec_weighted_by_Xi": float(np.average(g["delta_tau_sec"], weights=np.clip(g["Xi_base"], EPS, None))),
        })
    pd.DataFrame(rows).to_csv(metrics / "tidal_top100_aggregate_exposure_summary.csv", index=False)

    top = detail.sort_values("gross_saving_person_min_base", ascending=False).head(30)
    top.to_csv(metrics / "tidal_top30_segment_savings.csv", index=False)

    selected = pd.read_csv(data_work / "outputs" / tidal_dir / "top100_d1.0" / "selected_pairs.csv")
    treated_edges = np.unique(np.concatenate([
        selected["slow_edge_idx"].to_numpy(dtype=int),
        selected["fast_edge_idx"].to_numpy(dtype=int),
    ]))
    base_tau = np.load(data_work / "outputs" / tidal_dir / "baseline" / "equilibrium_arrays.npz")["tau_min_kl"]
    exposure = od_path_exposure(prim, base_tau, treated_edges)
    _write_json(metrics / "tidal_top100_shortest_path_od_exposure.json", exposure)

    plot_df = pd.DataFrame(rows)
    plot_df["horizon"] = pd.Categorical(plot_df["horizon"], ["short_run", "long_run"], ordered=True)
    plot_df = plot_df.sort_values("horizon")
    plot_df["horizon_label"] = plot_df["horizon"].astype(str).str.replace("_", " ", regex=False).str.title()
    fig, axes = plt.subplots(1, 2, figsize=(8.0, 3.4))
    axes[0].bar(plot_df["horizon_label"], plot_df["gross_saving_person_min_base"], color="#4C78A8", label="Gross savings")
    axes[0].bar(plot_df["horizon_label"], -plot_df["gross_loss_person_min_base"], color="#E15759", label="Losses")
    axes[0].axhline(0, color="black", lw=0.8)
    axes[0].set_ylabel("Person-minutes per day on treated links")
    axes[0].set_title("Segment-person-time accounting", loc="left", fontsize=10)
    axes[0].legend(fontsize=8)
    slow = plot_df["slow_edge_passages_base"].to_numpy()
    fast = plot_df["fast_edge_passages_base"].to_numpy()
    axes[1].bar(plot_df["horizon_label"], slow, color="#59A14F", label="Slow direction")
    axes[1].bar(plot_df["horizon_label"], fast, bottom=slow, color="#F28E2B", label="Fast direction")
    axes[1].set_ylabel("Model link passages")
    axes[1].set_title("Affected segment passages", loc="left", fontsize=10)
    axes[1].legend(fontsize=8, loc="upper center", bbox_to_anchor=(0.5, 1.18), ncol=2)
    fig.tight_layout()
    _savefig(fig, figures / "figure_tidal_segment_person_time_summary")


def run_sorting_attempt(data_work: Path, out: Path, model_input: str, stage2_dir: str, phase_b_dir: str) -> None:
    task = out / "04_sorting_fundamentals_wage_trends"
    metrics = task / "metrics"
    figures = task / "figures"
    metrics.mkdir(parents=True, exist_ok=True)
    figures.mkdir(parents=True, exist_ok=True)

    prim = bp.load_primitives(data_work / "outputs" / model_input, period="AM")
    params = replace(pm.Parameters(), alpha=MAINLINE_ALPHA, beta=MAINLINE_BETA)
    theta = params.theta
    alpha = params.alpha
    beta = params.beta
    fund = np.load(data_work / "outputs" / stage2_dir / "fundamentals" / "fundamentals_arrays.npz")
    node = pd.read_csv(data_work / "outputs" / phase_b_dir / "node_sorting_changes.csv")

    node["log_ubar"] = np.log(np.clip(fund["ubar_theta"], EPS, None)) / theta
    node["log_abar"] = np.log(np.clip(fund["abar_theta"], EPS, None)) / theta
    node["log_u_effective_base"] = node["log_ubar"] + beta * np.log(np.clip(node["l_R_level_base"] * prim.Lbar, EPS, None))
    node["log_u_effective_scenario"] = node["log_ubar"] + beta * np.log(np.clip(node["l_R_level_scenario"] * prim.Lbar, EPS, None))
    node["log_wage_base"] = node["log_abar"] + alpha * np.log(np.clip(node["l_F_level_base"] * prim.Lbar, EPS, None))
    node["log_wage_scenario"] = node["log_abar"] + alpha * np.log(np.clip(node["l_F_level_scenario"] * prim.Lbar, EPS, None))
    node["delta_log_u_effective"] = node["log_u_effective_scenario"] - node["log_u_effective_base"]
    node["delta_log_wage"] = node["log_wage_scenario"] - node["log_wage_base"]
    node["delta_log_ubar_fixed"] = 0.0
    node["delta_log_abar_fixed"] = 0.0
    node["dist_to_hub_km"] = node["dist_to_hub"] * GRID_KM
    node.to_csv(metrics / "node_sorting_fundamental_wage_changes.csv", index=False)

    summary = []
    for col, weight_col in [
        ("log_l_R_change", "L_R_obs"),
        ("log_l_F_change", "L_F_obs"),
        ("delta_log_u_effective", "L_R_obs"),
        ("delta_log_wage", "L_F_obs"),
        ("log_Pi_change", "L_R_obs"),
        ("log_P_change", "L_F_obs"),
    ]:
        summ = _weighted_summary(node[col].to_numpy(), np.clip(node[weight_col].to_numpy(), EPS, None))
        summ.update({"metric": col, "weight": weight_col})
        summary.append(summ)
    pd.DataFrame(summary).to_csv(metrics / "sorting_fundamental_wage_summary.csv", index=False)

    pairs = [
        ("log_l_R_change", "delta_log_u_effective", "L_R_obs", "unit"),
        ("log_l_F_change", "delta_log_wage", "L_F_obs", "unit"),
        ("dist_to_hub_km", "delta_log_u_effective", "L_R_obs", "per_10km"),
        ("dist_to_hub_km", "delta_log_wage", "L_F_obs", "per_10km"),
    ]
    corr_rows = []
    for xcol, ycol, wcol, slope_scale in pairs:
        x = node[xcol].to_numpy()
        y = node[ycol].to_numpy()
        w = np.clip(node[wcol].to_numpy(), EPS, None)
        reg = _weighted_regression(x, y, w)
        raw_slope = reg["slope_per_10km"] / 10.0 if np.isfinite(reg["slope_per_10km"]) else np.nan
        reg.update({
            "x": xcol,
            "y": ycol,
            "weight": wcol,
            "slope": raw_slope,
            "slope_unit": "per x unit" if slope_scale == "unit" else "per km",
            "slope_per_10km": reg["slope_per_10km"] if slope_scale == "per_10km" else np.nan,
        })
        corr_rows.append(reg)
    pd.DataFrame(corr_rows).to_csv(metrics / "sorting_fundamental_wage_trend_regressions.csv", index=False)

    node["dist_bin"] = pd.qcut(node["dist_to_hub"], 10, duplicates="drop")
    prof_rows = []
    for b, g in node.groupby("dist_bin", observed=True):
        prof_rows.append({
            "dist_mid_grid": float(g["dist_to_hub"].mean()),
            "dist_mid_km": float(g["dist_to_hub"].mean() * GRID_KM),
            "mean_log_l_R_change": float(np.average(g["log_l_R_change"], weights=np.clip(g["L_R_obs"], EPS, None))),
            "mean_log_l_F_change": float(np.average(g["log_l_F_change"], weights=np.clip(g["L_F_obs"], EPS, None))),
            "mean_delta_log_u_effective": float(np.average(g["delta_log_u_effective"], weights=np.clip(g["L_R_obs"], EPS, None))),
            "mean_delta_log_wage": float(np.average(g["delta_log_wage"], weights=np.clip(g["L_F_obs"], EPS, None))),
            "n_nodes": int(len(g)),
        })
    prof = pd.DataFrame(prof_rows)
    prof.to_csv(metrics / "sorting_radial_profile_with_wage_amenity.csv", index=False)

    fig, axes = plt.subplots(1, 2, figsize=(8.4, 3.4))
    axes[0].scatter(node["log_l_R_change"], node["delta_log_u_effective"], s=8, alpha=0.55, color="#4C78A8", linewidths=0)
    axes[0].axhline(0, color="black", lw=0.8)
    axes[0].axvline(0, color="black", lw=0.8)
    axes[0].set_xlabel("Delta log residence share")
    axes[0].set_ylabel("Delta log effective amenity")
    axes[0].set_title("Residential crowding channel", loc="left", fontsize=10)
    axes[1].scatter(node["log_l_F_change"], node["delta_log_wage"], s=8, alpha=0.55, color="#59A14F", linewidths=0)
    axes[1].axhline(0, color="black", lw=0.8)
    axes[1].axvline(0, color="black", lw=0.8)
    axes[1].set_xlabel("Delta log workplace share")
    axes[1].set_ylabel("Delta log wage component")
    axes[1].set_title("Agglomeration wage channel", loc="left", fontsize=10)
    fig.tight_layout()
    _savefig(fig, figures / "figure_sorting_fundamental_wage_scatter")

    fig, ax = plt.subplots(figsize=(6.2, 3.8))
    ax.plot(prof["dist_mid_km"], prof["mean_log_l_R_change"], "o-", color="#4C78A8", label="Residence share")
    ax.plot(prof["dist_mid_km"], prof["mean_delta_log_u_effective"], "o--", color="#4C78A8", label="Effective amenity")
    ax.plot(prof["dist_mid_km"], prof["mean_log_l_F_change"], "s-", color="#59A14F", label="Workplace share")
    ax.plot(prof["dist_mid_km"], prof["mean_delta_log_wage"], "s--", color="#59A14F", label="Wage component")
    ax.axhline(0, color="black", lw=0.8)
    ax.set_xlabel("Distance to employment center (km)")
    ax.set_ylabel("Mean log change")
    ax.set_title("Sorting and endogenous fundamentals over distance", loc="left")
    ax.legend(ncol=2, fontsize=8)
    fig.tight_layout()
    _savefig(fig, figures / "figure_sorting_radial_fundamental_wage_profile")


def _scaled_primitives(prim: bp.Primitives, scale: float) -> bp.Primitives:
    return replace(
        prim,
        L_R_obs=prim.L_R_obs * scale,
        L_F_obs=prim.L_F_obs * scale,
        L_ij_obs=prim.L_ij_obs * scale,
        Lbar=prim.Lbar * scale,
        meta={**prim.meta, "population_scale_smoke": scale},
    )


def _result_row(world: str, policy: str, horizon: str, base, cf, prim: bp.Primitives, n_pairs: int, converged_note: str) -> dict:
    return {
        "world": world,
        "policy": policy,
        "horizon": horizon,
        "Wbar_base": float(base.Wbar),
        "Wbar_cf": float(cf.Wbar),
        "Wbar_pct_change": float(100.0 * (cf.Wbar / base.Wbar - 1.0)),
        "mean_tau_base_min": float(np.mean(base.tau_min_kl)),
        "mean_tau_cf_min": float(np.mean(cf.tau_min_kl)),
        "mean_tau_pct_change": float(100.0 * (np.mean(cf.tau_min_kl) / np.mean(base.tau_min_kl) - 1.0)),
        "mean_Xi_base": float(np.mean(base.Xi_kl)),
        "mean_Xi_cf": float(np.mean(cf.Xi_kl)),
        "residence_dissimilarity_pct": float(100.0 * 0.5 * np.sum(np.abs(cf.l_R - base.l_R))),
        "workplace_dissimilarity_pct": float(100.0 * 0.5 * np.sum(np.abs(cf.l_F - base.l_F))),
        "n_pairs": int(n_pairs),
        "Lbar": float(prim.Lbar),
        "base_converged": bool(base.converged),
        "cf_converged": bool(cf.converged),
        "base_iter": int(base.n_outer),
        "cf_iter": int(cf.n_outer),
        "converged_note": converged_note,
    }


def run_growth_smoke_attempt(data_work: Path, out: Path, model_input: str, stage2_dir: str) -> None:
    task = out / "05_population_growth_urban_expansion_smoke"
    metrics = task / "metrics"
    figures = task / "figures"
    metrics.mkdir(parents=True, exist_ok=True)
    figures.mkdir(parents=True, exist_ok=True)

    prim0 = bp.load_primitives(data_work / "outputs" / model_input, period="AM")
    params = replace(pm.Parameters(), alpha=MAINLINE_ALPHA, beta=MAINLINE_BETA)
    fund_arrays = np.load(data_work / "outputs" / stage2_dir / "fundamentals" / "fundamentals_arrays.npz")
    fund_meta = json.loads((data_work / "outputs" / stage2_dir / "fundamentals" / "fundamentals_meta.json").read_text())
    ubar0 = fund_arrays["ubar_theta"].copy()
    abar0 = fund_arrays["abar_theta"].copy()

    base_stage = np.load(data_work / "outputs" / stage2_dir / "baseline_endogenous" / "equilibrium_arrays.npz")
    baseline_xi = base_stage["Xi_kl"].copy()
    solver_base = {
        "kappa": float(fund_meta.get("kappa", 1.0)),
        "chi": float(fund_meta.get("chi", 1.0)),
        "congestion_form": MAINLINE_CONGESTION_FORM,
        "congestion_lambda": MAINLINE_LAMBDA_PHYS,
        "xi_obs_kl": baseline_xi,
        "n_obs_kl": np.asarray(prim0.n_kl, dtype=float),
        "tau_obs_anchor": np.asarray(prim0.tau_obs_min, dtype=float),
        "max_outer": 90,
        "tol": 1e-4,
    }
    solver_base.update({k: v for k, v in solver_overrides_for_params(params).items() if v is not None})

    rows = []
    selection_rows = []
    pop_scales = [1.0, 1.15, 1.30]
    expansion_specs = [
        ("compact_observed_city", 1.0),
        ("outer_residential_amenity_10pct", 1.10),
    ]
    mainline_pairs = None

    for pop_scale in pop_scales:
        prim = _scaled_primitives(prim0, pop_scale)
        outer = outer_ring_mask(prim)
        for expansion_name, outer_amenity_mult in expansion_specs:
            ubar = ubar0.copy()
            if outer_amenity_mult != 1.0:
                ubar[outer] *= outer_amenity_mult ** params.theta
            world = f"pop{pop_scale:.2f}_{expansion_name}"
            LOG.info("Solving smoke world %s", world)
            base = solve_equilibrium(
                prim, ubar, abar0, params,
                tau_min_init=prim.tau_obs_min.copy(),
                endogenous_congestion=True,
                label=f"{world}_baseline",
                **solver_base,
            )
            fund_smoke = SimpleNamespace(
                ubar_theta=ubar,
                abar_theta=abar0,
                kappa=solver_base["kappa"],
                chi=solver_base["chi"],
            )

            tidal_st, sel = s2.run_tidal_endogenous(
                prim, fund_smoke,
                params, base, top_k=100, delta=1.0, horizon="short_run",
                pair_selection=MAINLINE_PAIR_SELECTION,
                **solver_base,
            )
            tidal_lt, _ = s2.run_tidal_endogenous(
                prim, fund_smoke,
                params, base, top_k=100, delta=1.0, horizon="long_run",
                pair_selection=MAINLINE_PAIR_SELECTION,
                **solver_base,
            )

            sel_edges = set(map(tuple, sel.treated_pairs.tolist()))
            if mainline_pairs is None and np.isclose(pop_scale, 1.0) and expansion_name == "compact_observed_city":
                mainline_pairs = sel_edges
            overlap = len(sel_edges & (mainline_pairs or set())) / max(len(sel_edges), 1)
            selection_rows.append({
                "world": world,
                "population_scale": pop_scale,
                "urban_expansion": expansion_name,
                "n_tidal_pairs": int(sel.n_treated),
                "overlap_with_pop1_compact_share": float(overlap),
            })

            rows.append(_result_row(world, "tidal_reallocation_top100", "short_run", base, tidal_st, prim, sel.n_treated, "smoke"))
            rows.append(_result_row(world, "tidal_reallocation_top100", "long_run", base, tidal_lt, prim, sel.n_treated, "smoke"))

            n_cf = apply_bidirectional_lane_delta(prim, sel, delta=1.0)
            road_st = solve_equilibrium(
                prim, ubar, abar0, params,
                n_kl_override=n_cf,
                tau_min_init=prim.tau_obs_min.copy(),
                endogenous_congestion=True,
                label=f"{world}_road_expansion_short_run",
                l_R_fixed=base.l_R,
                l_F_fixed=base.l_F,
                **solver_base,
            )
            road_lt = solve_equilibrium(
                prim, ubar, abar0, params,
                n_kl_override=n_cf,
                tau_min_init=prim.tau_obs_min.copy(),
                endogenous_congestion=True,
                label=f"{world}_road_expansion_long_run",
                **solver_base,
            )
            rows.append(_result_row(world, "same_corridor_bidirectional_expansion", "short_run", base, road_st, prim, sel.n_treated, "smoke"))
            rows.append(_result_row(world, "same_corridor_bidirectional_expansion", "long_run", base, road_lt, prim, sel.n_treated, "smoke"))

    res = pd.DataFrame(rows)
    res["population_scale"] = res["world"].str.extract(r"pop([0-9.]+)_").astype(float)
    res["urban_expansion"] = np.where(
        res["world"].str.contains("outer_residential_amenity"), "outer_residential_amenity_10pct", "compact_observed_city"
    )
    res.to_csv(metrics / "growth_expansion_policy_smoke_results.csv", index=False)
    pd.DataFrame(selection_rows).to_csv(metrics / "growth_expansion_tidal_selection_overlap.csv", index=False)

    fig, axes = plt.subplots(1, 2, figsize=(9.0, 3.6), sharey=True)
    for ax, expansion in zip(axes, ["compact_observed_city", "outer_residential_amenity_10pct"]):
        g = res[(res["urban_expansion"] == expansion) & (res["horizon"] == "long_run")]
        for policy, color in [
            ("tidal_reallocation_top100", "#4C78A8"),
            ("same_corridor_bidirectional_expansion", "#F28E2B"),
        ]:
            p = g[g["policy"] == policy].sort_values("population_scale")
            ax.plot(p["population_scale"], p["Wbar_pct_change"], marker="o", lw=1.6, color=color, label=policy.replace("_", " "))
        ax.axhline(0, color="black", lw=0.8)
        ax.set_xlabel("Population scale")
        ax.set_title(expansion.replace("_", " "), loc="left", fontsize=10)
    axes[0].set_ylabel("Long-run Wbar change (%)")
    axes[1].legend(fontsize=8)
    fig.tight_layout()
    _savefig(fig, figures / "figure_growth_expansion_long_run_policy_response")


def write_readme(out: Path) -> None:
    text = """# Coauthor Revision Attempts, 2026-07-10

This output package records first-pass diagnostics for the five coauthor requests.

## Contents

1. `01_national_large_city_jobs_housing`: large-city center-bound commuting diagnostics and threshold sensitivity.
2. `02_symmetric_fundamental_distribution`: full re-estimation comparison, symmetric minus directed fundamentals over space.
3. `03_tidal_time_savings_exposure`: segment-person-time accounting for the top-100 tidal-lane experiment.
4. `04_sorting_fundamentals_wage_trends`: sorting changes linked to effective amenity and wage components.
5. `05_population_growth_urban_expansion_smoke`: smoke counterfactuals for population growth and outer residential expansion.

The fifth item is a smoke exercise. It keeps baseline fundamentals fixed, scales commuting mass, uses the original congestion anchor, and compares tidal reallocation with same-corridor bidirectional expansion. Congestion pricing is not included in this smoke package because its toll and rebate accounting needs a separate closed welfare block.
"""
    (out / "README.md").write_text(text)


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo-root", default=str(DATA_WORK))
    p.add_argument("--output-name", default=DEFAULT_OUTPUT)
    p.add_argument("--model-input", default=DEFAULT_MODEL_INPUT)
    p.add_argument("--stage2-dir", default=DEFAULT_STAGE2)
    p.add_argument("--symmetric-dir", default=DEFAULT_SYMMETRIC)
    p.add_argument("--tidal-dir", default=DEFAULT_TIDAL_DIAG)
    p.add_argument("--phase-b-dir", default=DEFAULT_PHASE_B)
    p.add_argument("--national-group-dir", default=DEFAULT_NATIONAL_GROUP)
    p.add_argument("--skip-growth-smoke", action="store_true")
    args = p.parse_args(argv)

    _setup_style()
    data_work = data_work_root(args.repo_root)
    out = data_work / "outputs" / args.output_name
    out.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s | %(message)s")

    LOG.info("Running national diagnostic")
    run_national_attempt(data_work, out, args.national_group_dir)
    LOG.info("Running symmetric fundamental diagnostic")
    run_symmetric_attempt(data_work, out, args.model_input, args.symmetric_dir)
    LOG.info("Running tidal exposure diagnostic")
    run_tidal_exposure_attempt(data_work, out, args.model_input, args.tidal_dir)
    LOG.info("Running sorting/fundamental diagnostic")
    run_sorting_attempt(data_work, out, args.model_input, args.stage2_dir, args.phase_b_dir)
    if not args.skip_growth_smoke:
        LOG.info("Running growth/expansion smoke")
        run_growth_smoke_attempt(data_work, out, args.model_input, args.stage2_dir)
    write_readme(out)
    LOG.info("Wrote %s", out)


if __name__ == "__main__":
    main()
