"""OD-track calibration: PO gravity theta and SMM aggregate moments."""
from __future__ import annotations

from dataclasses import replace
import json
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.sparse import csr_matrix
from scipy.sparse.csgraph import dijkstra

from src.model.full_estimation.build_primitives import Primitives
from src.model.full_estimation import invert_fundamentals as inv
from src.model.full_estimation.parameters import Parameters
from src.model.directional_cost import fixed_tau_model as ft
from src.model.spatial_equilibrium import estimate_theta_two_way_fe


EPS = 1e-12

# External placeholders (Gu et al. 2026 Shenzhen Wall draft; not Beijing estimates).
BORROW_GU_EPSILON = 5.09
BORROW_GU_KAPPA = 0.0256

TAU_SPECS = ("leontief", "dijkstra", "dijkstra_diag_zero")
OD_WEIGHT_MODES = ("aa_leontief", "iceberg_minutes", "gu_exponential")
INTRA_SPECS_COMPARISON = ("baseline", "exclude_diag_plus_proxy_calibrated")
THETA_SOURCES = (
    "placeholder_6_83",
    "borrow_gu_epsilon_5_09",
    "po_gravity",
    "smm_local",
)


def bilateral_minutes_from_leontief(tau_inv_theta: np.ndarray, theta: float) -> np.ndarray:
    """Convert theta-free Leontief index ``T`` to bilateral minutes ``tau_ij``."""
    return np.clip(tau_inv_theta, EPS, None) ** (-1.0 / theta)


def _od_arrays_for_gravity(
    prim: Primitives,
    tau_min: np.ndarray,
    off_diagonal_only: bool,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Flatten positive OD cells with finite bilateral travel times."""
    origins, dests, flows, tau = [], [], [], []
    for i in range(prim.n_nodes):
        for j in range(prim.n_nodes):
            if prim.L_ij_obs[i, j] <= EPS:
                continue
            if off_diagonal_only and i == j:
                continue
            t_ij = float(tau_min[i, j])
            if not np.isfinite(t_ij) or t_ij <= EPS:
                continue
            origins.append(i)
            dests.append(j)
            flows.append(float(prim.L_ij_obs[i, j]))
            tau.append(t_ij)
    return (
        np.asarray(origins, dtype=int),
        np.asarray(dests, dtype=int),
        np.asarray(flows, dtype=float),
        np.asarray(tau, dtype=float),
    )


def estimate_theta_po_gravity(
    prim: Primitives,
    tau_min: np.ndarray,
    *,
    off_diagonal_only: bool = True,
    flow_weighted: bool = False,
    min_theta: float = 0.1,
) -> dict:
    """Two-way FE gravity: log L_ij = mu_i + nu_j - theta log tau_ij(minutes)."""
    o, d, pop, tau = _od_arrays_for_gravity(prim, tau_min, off_diagonal_only)
    if pop.size == 0:
        return {"theta_hat": float("nan"), "n_obs": 0}
    if not flow_weighted:
        fit = estimate_theta_two_way_fe(o, d, pop, tau)
        theta_hat = float(max(min_theta, fit["theta_hat"]))
        return {**fit, "theta_hat": theta_hat, "off_diagonal_only": off_diagonal_only, "flow_weighted": False}

    y = np.log(pop)
    x = np.log(tau)
    x_t, y_t = x.copy(), y.copy()
    for _ in range(200):
        x_prev, y_prev = x_t.copy(), y_t.copy()
        x_t -= pd.Series(x_t).groupby(o).transform("mean").to_numpy()
        x_t -= pd.Series(x_t).groupby(d).transform("mean").to_numpy()
        y_t -= pd.Series(y_t).groupby(o).transform("mean").to_numpy()
        y_t -= pd.Series(y_t).groupby(d).transform("mean").to_numpy()
        if max(float(np.max(np.abs(x_t - x_prev))), float(np.max(np.abs(y_t - y_prev)))) < 1e-10:
            break
    w = np.sqrt(pop)
    denom = float(np.dot(w * x_t, w * x_t))
    beta_hat = float(np.dot(w * x_t, w * y_t) / denom) if denom > 0 else np.nan
    theta_hat = float(max(min_theta, -beta_hat)) if np.isfinite(beta_hat) else float("nan")
    return {
        "theta_hat": theta_hat,
        "beta_hat": beta_hat,
        "n_obs": int(pop.size),
        "off_diagonal_only": off_diagonal_only,
        "flow_weighted": True,
    }


def estimate_theta_po_gravity_leontief(
    prim: Primitives,
    tau_inv_theta: np.ndarray,
    *,
    off_diagonal_only: bool = True,
    flow_weighted: bool = False,
    theta_ref: float = 6.83,
    n_iter: int = 1,
    min_theta: float = 0.1,
) -> dict:
    """PO gravity when bilateral minutes come from Leontief ``T``."""
    theta = float(theta_ref)
    last: dict = {}
    for it in range(max(1, n_iter)):
        tau_min = bilateral_minutes_from_leontief(tau_inv_theta, theta)
        last = estimate_theta_po_gravity(
            prim, tau_min, off_diagonal_only=off_diagonal_only,
            flow_weighted=flow_weighted, min_theta=min_theta,
        )
        last["iteration"] = it
        last["theta_ref"] = theta_ref
        if not np.isfinite(last.get("theta_hat", np.nan)):
            break
        theta = float(last["theta_hat"])
    return last


def estimate_theta_log_leontief_coef(
    prim: Primitives,
    tau_inv_theta: np.ndarray,
    off_diagonal_only: bool = True,
) -> dict:
    """TWFE coefficient on log T_ij; structural mapping expects coef ~ 1."""
    o, d, pop, tau = _od_arrays_for_gravity(prim, tau_inv_theta, off_diagonal_only)
    if pop.size == 0:
        return {"log_T_coef": float("nan"), "n_obs": 0}
    y = np.log(pop)
    x = np.log(tau)
    x_t, y_t = x.copy(), y.copy()
    for _ in range(200):
        x_prev, y_prev = x_t.copy(), y_t.copy()
        x_t -= pd.Series(x_t).groupby(o).transform("mean").to_numpy()
        x_t -= pd.Series(x_t).groupby(d).transform("mean").to_numpy()
        y_t -= pd.Series(y_t).groupby(o).transform("mean").to_numpy()
        y_t -= pd.Series(y_t).groupby(d).transform("mean").to_numpy()
        if max(float(np.max(np.abs(x_t - x_prev))), float(np.max(np.abs(y_t - y_prev)))) < 1e-10:
            break
    denom = float(np.dot(x_t, x_t))
    coef = float(np.dot(x_t, y_t) / denom) if denom > 0 else float("nan")
    return {
        "log_T_coef": coef,
        "n_obs": int(pop.size),
        "off_diagonal_only": off_diagonal_only,
        "note": "Under L_ij ∝ T_ij holding FEs, coefficient should be near 1.",
    }


def observed_od_aggregate_moments(
    prim: Primitives,
    tau_inv_theta: np.ndarray,
    theta_ref: float = 6.83,
) -> dict:
    """Observed OD aggregate moments used in SMM.

    ``theta_ref`` fixes the map ``T -> tau_minutes`` for the observed mean commute.
    It does **not** vary with the structural theta being searched in SMM.
    """
    L = prim.L_ij_obs
    positive = L > EPS
    on_diag = np.eye(prim.n_nodes, dtype=bool)
    tau_min = bilateral_minutes_from_leontief(tau_inv_theta, theta_ref)
    valid = positive & np.isfinite(tau_min) & (tau_min > EPS)
    weights = L[valid] / L[valid].sum()
    mean_commute_min = float(np.sum(weights * tau_min[valid]))
    intra_share = float(L[on_diag].sum() / L[positive].sum())
    p = L[positive] / L[positive].sum()
    od_entropy = float(-np.sum(p * np.log(np.clip(p, EPS, None))))
    return {
        "mean_commute_min": mean_commute_min,
        "intrazonal_share": intra_share,
        "od_entropy": od_entropy,
        "theta_ref_for_observed_mean_commute": float(theta_ref),
    }


def predicted_od_aggregate_moments(
    prim: Primitives,
    result: ft.StaticEquilibriumResult,
    od_shares: np.ndarray,
    params: Parameters,
) -> dict:
    """Model-implied aggregate moments from predicted OD shares."""
    L_pred = od_shares * prim.Lbar
    positive = L_pred > EPS
    on_diag = np.eye(prim.n_nodes, dtype=bool)
    mean_commute_min = ft.weighted_bilateral_cost(result, od_shares, params.theta)
    intra_share = float(L_pred[on_diag].sum() / max(L_pred[positive].sum(), EPS))
    p = L_pred[positive] / max(L_pred[positive].sum(), EPS)
    od_entropy = float(-np.sum(p * np.log(np.clip(p, EPS, None))))
    return {
        "mean_commute_min": mean_commute_min,
        "intrazonal_share": intra_share,
        "od_entropy": od_entropy,
    }


def _relative_sse(obs: dict, pred: dict, keys: tuple[str, ...]) -> float:
    err = 0.0
    for key in keys:
        o = float(obs[key])
        p = float(pred[key])
        if abs(o) <= EPS:
            continue
        err += ((p - o) / o) ** 2
    return float(err)


def evaluate_theta_case(
    prim: Primitives,
    params: Parameters,
    kappa: float,
    tau_inv_theta: np.ndarray,
    obs_moments: dict,
    *,
    intra_spec: str = "exclude_diag_plus_proxy_calibrated",
    inv_max_iter: int = 1800,
    inv_tol: float = 1.5e-4,
    eq_max_iter: int = 1200,
    eq_tol: float = 1e-9,
) -> dict:
    """Re-invert at ``params.theta``, predict OD, and report fit + aggregate moments."""
    l_R, l_F, _ = ft.observed_shares(prim)
    fund = inv.invert_fundamentals(
        prim, params, kappa=kappa, max_iter=inv_max_iter, tol=inv_tol,
    )
    eq = ft.solve_static_equilibrium(
        prim, prim.tau_obs_min, fund.ubar_theta, fund.abar_theta, fund.chi, params,
        kappa=kappa, initial_l_R=l_R, initial_l_F=l_F,
        max_iter=eq_max_iter, tol=eq_tol,
    )
    intra_kw: dict = {"intra_spec": intra_spec}
    if intra_spec == "exclude_diag_plus_proxy_calibrated":
        intra_kw["intra_cost_scale"] = ft.calibrate_intrazonal_cost_scale(
            prim, eq, fund.abar_theta, params,
        )
    od = ft.predicted_od_shares(eq, fund.abar_theta, params, **intra_kw)
    od_fit_summary = ft.od_fit_to_observed_summary(prim, od)
    pred_mom = predicted_od_aggregate_moments(prim, eq, od, params)
    smm_keys = ("mean_commute_min", "intrazonal_share", "od_entropy")
    return {
        "theta": float(params.theta),
        "intra_spec": intra_spec,
        "intra_cost_scale": float(intra_kw.get("intra_cost_scale", 1.0)),
        "global_l1_pct": od_fit_summary["global_l1_pct"],
        "off_diagonal_l1_pct": od_fit_summary["off_diagonal_l1_pct"],
        "intrazonal_share_pred_pct": od_fit_summary["intrazonal_share_pred_pct"],
        "weighted_log_corr": od_fit_summary["weighted_log_corr"],
        "observed_moments": obs_moments,
        "predicted_moments": pred_mom,
        "smm_relative_sse": _relative_sse(obs_moments, pred_mom, smm_keys),
        "fundamentals_converged": bool(fund.converged),
        "equilibrium_converged": bool(eq.converged),
    }


def smm_theta_grid(
    prim: Primitives,
    base_params: Parameters,
    kappa: float,
    tau_inv_theta: np.ndarray,
    theta_grid: list[float],
    obs_moments: dict,
    *,
    intra_spec: str = "exclude_diag_plus_proxy_calibrated",
) -> list[dict]:
    """Evaluate SMM loss over a theta grid (marginals still pinned by re-inversion)."""
    rows = []
    for theta in theta_grid:
        params = replace(base_params, theta=float(theta))
        rows.append(
            evaluate_theta_case(
                prim, params, kappa, tau_inv_theta, obs_moments,
                intra_spec=intra_spec,
            )
        )
    return rows


def build_sp_dijkstra_minutes(
    prim: Primitives,
    *,
    diag_value: float | None = 0.0,
    unreachable_fill: float = 1e4,
) -> np.ndarray:
    """Directed shortest-path travel times on the observed edge network."""
    n = prim.n_nodes
    graph = csr_matrix(
        (np.clip(prim.tau_obs_min, EPS, None), (prim.edge_i, prim.edge_j)),
        shape=(n, n),
    )
    dist = np.asarray(dijkstra(graph, directed=True, indices=np.arange(n)), dtype=float)
    dist[~np.isfinite(dist)] = unreachable_fill
    dist = np.clip(dist, EPS, None)
    if diag_value is not None:
        np.fill_diagonal(dist, float(diag_value))
    return dist


def resolve_tau_minutes(
    prim: Primitives,
    tau_inv_theta: np.ndarray,
    tau_spec: str,
    theta: float,
) -> np.ndarray:
    """Bilateral minutes matrix for gravity / iceberg / Gu exponential weights."""
    if tau_spec == "leontief":
        return bilateral_minutes_from_leontief(tau_inv_theta, theta)
    if tau_spec == "dijkstra":
        return build_sp_dijkstra_minutes(prim, diag_value=EPS)
    if tau_spec == "dijkstra_diag_zero":
        return build_sp_dijkstra_minutes(prim, diag_value=0.0)
    raise ValueError(f"unknown tau_spec: {tau_spec}")


def build_bilateral_accessibility(
    prim: Primitives,
    tau_inv_theta: np.ndarray,
    *,
    tau_spec: str,
    od_weight_mode: str,
    theta: float,
    kappa_gu: float = BORROW_GU_KAPPA,
) -> tuple[np.ndarray, np.ndarray]:
    """Return (accessibility matrix, bilateral minutes used for PO gravity)."""
    tau_min = resolve_tau_minutes(prim, tau_inv_theta, tau_spec, theta)
    if od_weight_mode == "aa_leontief":
        accessibility = np.asarray(tau_inv_theta, dtype=float)
    elif od_weight_mode == "iceberg_minutes":
        accessibility = np.clip(tau_min, EPS, None) ** (-theta)
    elif od_weight_mode == "gu_exponential":
        accessibility = np.exp(-kappa_gu * tau_min)
    else:
        raise ValueError(f"unknown od_weight_mode: {od_weight_mode}")
    return accessibility, tau_min


def composite_fitness(
    od_fit: dict,
    *,
    w_off_diag: float = 0.40,
    w_global: float = 0.35,
    w_intra: float = 0.15,
    w_logcorr: float = 0.10,
) -> float:
    """Lower is better. Primary target: off-diagonal L1, then global L1."""
    intra_err = abs(
        od_fit["intrazonal_share_pred_pct"] - od_fit["intrazonal_share_obs_pct"]
    )
    logcorr_penalty = 100.0 * (1.0 - float(od_fit["weighted_log_corr"]))
    return float(
        w_off_diag * od_fit["off_diagonal_l1_pct"]
        + w_global * od_fit["global_l1_pct"]
        + w_intra * intra_err
        + w_logcorr * logcorr_penalty
    )


def _invert_and_equilibrium(
    prim: Primitives,
    params: Parameters,
    kappa: float,
    *,
    inv_max_iter: int = 1800,
    inv_tol: float = 1.5e-4,
    eq_max_iter: int = 1200,
    eq_tol: float = 1e-9,
) -> tuple[inv.InversionResult, ft.StaticEquilibriumResult]:
    l_R, l_F, _ = ft.observed_shares(prim)
    fund = inv.invert_fundamentals(
        prim, params, kappa=kappa, max_iter=inv_max_iter, tol=inv_tol,
    )
    eq = ft.solve_static_equilibrium(
        prim, prim.tau_obs_min, fund.ubar_theta, fund.abar_theta, fund.chi, params,
        kappa=kappa, initial_l_R=l_R, initial_l_F=l_F,
        max_iter=eq_max_iter, tol=eq_tol,
    )
    return fund, eq


def evaluate_od_method_case(
    prim: Primitives,
    params: Parameters,
    kappa: float,
    tau_inv_theta: np.ndarray,
    *,
    tau_spec: str,
    od_weight_mode: str,
    intra_spec: str,
    obs_moments: dict | None = None,
) -> dict:
    """One OD-layer configuration after re-inversion at ``params.theta``."""
    fund, eq = _invert_and_equilibrium(prim, params, kappa)
    accessibility, tau_min = build_bilateral_accessibility(
        prim, tau_inv_theta, tau_spec=tau_spec, od_weight_mode=od_weight_mode, theta=params.theta,
    )
    intra_kw: dict = {"intra_spec": intra_spec, "accessibility": accessibility}
    if intra_spec == "exclude_diag_plus_proxy_calibrated":
        intra_kw["intra_cost_scale"] = ft.calibrate_intrazonal_cost_scale(
            prim, eq, fund.abar_theta, params, accessibility=accessibility,
        )
    od = ft.predicted_od_shares(eq, fund.abar_theta, params, **intra_kw)
    od_fit = ft.od_fit_to_observed_summary(prim, od)
    row: dict = {
        "theta": float(params.theta),
        "tau_spec": tau_spec,
        "od_weight_mode": od_weight_mode,
        "intra_spec": intra_spec,
        "intra_cost_scale": float(intra_kw.get("intra_cost_scale", 1.0)),
        "global_l1_pct": od_fit["global_l1_pct"],
        "off_diagonal_l1_pct": od_fit["off_diagonal_l1_pct"],
        "intrazonal_share_obs_pct": od_fit["intrazonal_share_obs_pct"],
        "intrazonal_share_pred_pct": od_fit["intrazonal_share_pred_pct"],
        "weighted_log_corr": od_fit["weighted_log_corr"],
        "origin_conditional_l1_median_pct": od_fit["origin_conditional_l1_median_pct"],
        "off_diagonal_top_1pct_cells_l1_pct": od_fit["off_diagonal_top_1pct_cells_l1_pct"],
        "fundamentals_converged": bool(fund.converged),
        "equilibrium_converged": bool(eq.converged),
    }
    row["composite_fitness"] = composite_fitness(od_fit)
    if obs_moments is not None:
        pred_mom = predicted_od_aggregate_moments(prim, eq, od, params)
        smm_keys = ("mean_commute_min", "intrazonal_share", "od_entropy")
        row["smm_relative_sse"] = _relative_sse(obs_moments, pred_mom, smm_keys)
        row["predicted_mean_commute_min"] = pred_mom["mean_commute_min"]
    return row


def resolve_theta_for_source(
    prim: Primitives,
    tau_inv_theta: np.ndarray,
    *,
    theta_source: str,
    base_params: Parameters,
    tau_spec: str,
    smm_theta_grid: list[float] | None = None,
    obs_moments: dict | None = None,
    kappa: float,
    od_weight_mode: str = "aa_leontief",
    intra_spec: str = "exclude_diag_plus_proxy_calibrated",
) -> tuple[float, dict]:
    """Map a theta_source label to a numeric theta and estimation metadata."""
    meta: dict = {"theta_source": theta_source, "tau_spec": tau_spec}
    if theta_source == "placeholder_6_83":
        return float(base_params.theta), meta
    if theta_source == "borrow_gu_epsilon_5_09":
        return float(BORROW_GU_EPSILON), {**meta, "note": "borrowed from Gu et al. Shenzhen draft"}
    if theta_source == "po_gravity":
        tau_min = resolve_tau_minutes(prim, tau_inv_theta, tau_spec, base_params.theta)
        if tau_spec == "leontief":
            est = estimate_theta_po_gravity_leontief(
                prim, tau_inv_theta, off_diagonal_only=True, flow_weighted=False,
                theta_ref=base_params.theta, n_iter=3,
            )
        else:
            est = estimate_theta_po_gravity(
                prim, tau_min, off_diagonal_only=True, flow_weighted=False,
            )
        return float(est["theta_hat"]), {**meta, "po_gravity": est}
    if theta_source == "smm_local":
        if obs_moments is None:
            obs_moments = observed_od_aggregate_moments(
                prim, tau_inv_theta, theta_ref=base_params.theta,
            )
        grid = smm_theta_grid or [2.0, 4.0, 6.0, 8.0]
        rows = []
        for theta in grid:
            params = replace(base_params, theta=float(theta))
            case = evaluate_od_method_case(
                prim, params, kappa, tau_inv_theta,
                tau_spec=tau_spec, od_weight_mode=od_weight_mode, intra_spec=intra_spec,
                obs_moments=obs_moments,
            )
            rows.append(case)
        best = min(rows, key=lambda r: r["smm_relative_sse"])
        return float(best["theta"]), {**meta, "smm_local_grid": rows, "smm_local_best": best}
    raise ValueError(f"unknown theta_source: {theta_source}")


def run_method_comparison(
    prim: Primitives,
    kappa: float,
    base_params: Parameters | None = None,
    *,
    tau_specs: tuple[str, ...] = TAU_SPECS,
    od_weight_modes: tuple[str, ...] = OD_WEIGHT_MODES,
    intra_specs: tuple[str, ...] = INTRA_SPECS_COMPARISON,
    theta_sources: tuple[str, ...] = THETA_SOURCES,
    smm_local_grid: list[float] | None = None,
) -> dict:
    """Grid over OD-layer knobs; rank by composite fitness (lower better)."""
    base_params = base_params or Parameters()
    tau_inv_theta = ft.build_tau_inv_theta(prim, prim.tau_obs_min, kappa=kappa)
    obs_moments = observed_od_aggregate_moments(
        prim, tau_inv_theta, theta_ref=base_params.theta,
    )

    po_by_tau: dict[str, dict] = {}
    for tau_spec in tau_specs:
        if tau_spec == "leontief":
            po_by_tau[tau_spec] = estimate_theta_po_gravity_leontief(
                prim, tau_inv_theta, off_diagonal_only=True, flow_weighted=False,
                theta_ref=base_params.theta, n_iter=3,
            )
        else:
            tau_min = resolve_tau_minutes(prim, tau_inv_theta, tau_spec, base_params.theta)
            po_by_tau[tau_spec] = estimate_theta_po_gravity(
                prim, tau_min, off_diagonal_only=True, flow_weighted=False,
            )

    cases: list[dict] = []
    theta_cache: dict[tuple[str, str], tuple[float, dict]] = {}

    for tau_spec in tau_specs:
        for od_weight_mode in od_weight_modes:
            if od_weight_mode == "aa_leontief" and tau_spec != "leontief":
                continue
            for intra_spec in intra_specs:
                for theta_source in theta_sources:
                    if theta_source == "smm_local" and intra_spec == "baseline":
                        continue
                    key = (theta_source, tau_spec)
                    if key not in theta_cache:
                        theta_val, theta_meta = resolve_theta_for_source(
                            prim, tau_inv_theta,
                            theta_source=theta_source,
                            base_params=base_params,
                            tau_spec=tau_spec,
                            smm_theta_grid=smm_local_grid,
                            obs_moments=obs_moments,
                            kappa=kappa,
                            od_weight_mode=od_weight_mode,
                            intra_spec=intra_spec,
                        )
                        theta_cache[key] = (theta_val, theta_meta)
                    else:
                        theta_val, theta_meta = theta_cache[key]

                    params = replace(base_params, theta=float(theta_val))
                    case = evaluate_od_method_case(
                        prim, params, kappa, tau_inv_theta,
                        tau_spec=tau_spec,
                        od_weight_mode=od_weight_mode,
                        intra_spec=intra_spec,
                        obs_moments=obs_moments,
                    )
                    case["theta_source"] = theta_source
                    case["theta_resolution"] = theta_meta
                    case["method_id"] = (
                        f"{tau_spec}|{od_weight_mode}|{intra_spec}|{theta_source}|theta={theta_val:.3f}"
                    )
                    cases.append(case)

    ranked = sorted(cases, key=lambda r: r["composite_fitness"])
    baseline_cases = [
        c for c in cases
        if c["tau_spec"] == "leontief"
        and c["od_weight_mode"] == "aa_leontief"
        and c["intra_spec"] == "baseline"
        and c["theta_source"] == "placeholder_6_83"
    ]
    baseline = baseline_cases[0] if baseline_cases else None

    return {
        "track": "od_fit_method_comparison",
        "placeholder_theta": float(base_params.theta),
        "observed_aggregate_moments": obs_moments,
        "po_gravity_by_tau_spec": po_by_tau,
        "n_cases": len(cases),
        "fitness_weights": {
            "off_diagonal_l1": 0.40,
            "global_l1": 0.35,
            "intrazonal_share_abs_error": 0.15,
            "log_corr_penalty": 0.10,
        },
        "baseline_reference": baseline,
        "ranked_by_composite_fitness": ranked,
        "top_10": ranked[:10],
        "method_axes": {
            "tau_spec": list(tau_specs),
            "od_weight_mode": list(od_weight_modes),
            "intra_spec": list(intra_specs),
            "theta_source": list(theta_sources),
        },
        "interpretation": {
            "tau_spec": (
                "leontief: grid Leontief T=(I-A)^-1 (Stage 1 default); "
                "dijkstra: directed SP minutes on edges; "
                "dijkstra_diag_zero: SP with zero diagonal (Qilin-style)."
            ),
            "od_weight_mode": (
                "aa_leontief: bilateral weight ∝ T_ij (AA structural); "
                "iceberg_minutes: ∝ tau_ij^{-theta}; "
                "gu_exponential: ∝ exp(-kappa_gu * tau_ij) with borrowed kappa."
            ),
            "intra_spec": (
                "baseline: model T_ii (over-predicts intra); "
                "exclude_diag_plus_proxy_calibrated: proxy intra + scale to match observed intra share."
            ),
            "theta_source": (
                "placeholder_6_83: fixed external placeholder; "
                "borrow_gu_epsilon_5_09: literature epsilon; "
                "po_gravity: TWFE on observed OD vs log tau; "
                "smm_local: mini-grid minimizing aggregate moment SSE (calibrated intra only)."
            ),
            "composite_fitness": (
                "Weighted sum prioritising off-diagonal L1 (40%), global L1 (35%), "
                "intra-share error (15%), log-corr penalty (10%). Lower is better."
            ),
        },
    }


def run_theta_and_smm_calibration(
    prim: Primitives,
    kappa: float,
    base_params: Parameters | None = None,
    *,
    theta_grid: list[float] | None = None,
    intra_spec: str = "exclude_diag_plus_proxy_calibrated",
) -> dict:
    """Full P2 report: PO gravity theta estimates + SMM theta grid + comparison."""
    base_params = base_params or Parameters()
    tau_inv_theta = ft.build_tau_inv_theta(prim, prim.tau_obs_min, kappa=kappa)

    po_estimates = {
        "off_diag_unweighted_at_placeholder": estimate_theta_po_gravity_leontief(
            prim, tau_inv_theta,
            off_diagonal_only=True, flow_weighted=False,
            theta_ref=base_params.theta, n_iter=1,
        ),
        "off_diag_flow_weighted_at_placeholder": estimate_theta_po_gravity_leontief(
            prim, tau_inv_theta,
            off_diagonal_only=True, flow_weighted=True,
            theta_ref=base_params.theta, n_iter=1,
        ),
        "off_diag_unweighted_iter3": estimate_theta_po_gravity_leontief(
            prim, tau_inv_theta,
            off_diagonal_only=True, flow_weighted=False,
            theta_ref=base_params.theta, n_iter=3,
        ),
        "log_T_coef_off_diag": estimate_theta_log_leontief_coef(
            prim, tau_inv_theta, off_diagonal_only=True,
        ),
    }

    obs_moments = observed_od_aggregate_moments(
        prim, tau_inv_theta, theta_ref=base_params.theta,
    )

    if theta_grid is None:
        theta_grid = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 6.83, 8.0, 10.0, 12.0, 15.0]

    baseline_theta = float(base_params.theta)
    grid = smm_theta_grid(
        prim, base_params, kappa, tau_inv_theta, theta_grid,
        obs_moments, intra_spec=intra_spec,
    )
    smm_best = min(grid, key=lambda r: r["smm_relative_sse"])
    baseline_case = next(r for r in grid if abs(r["theta"] - baseline_theta) < 1e-9)

    po_primary = po_estimates["off_diag_unweighted_at_placeholder"]["theta_hat"]
    po_case = evaluate_theta_case(
        prim, replace(base_params, theta=float(po_primary)), kappa, tau_inv_theta,
        obs_moments, intra_spec=intra_spec,
    )

    return {
        "track": "od_fit_improvement_p2_theta_and_smm",
        "placeholder_theta": baseline_theta,
        "po_gravity_estimates": po_estimates,
        "po_primary_theta_hat": float(po_primary),
        "observed_aggregate_moments": obs_moments,
        "smm_theta_grid": grid,
        "smm_best_theta": float(smm_best["theta"]),
        "smm_best_relative_sse": float(smm_best["smm_relative_sse"]),
        "comparison": {
            "baseline_placeholder": {
                "theta": baseline_theta,
                "global_l1_pct": baseline_case["global_l1_pct"],
                "off_diagonal_l1_pct": baseline_case["off_diagonal_l1_pct"],
                "smm_relative_sse": baseline_case["smm_relative_sse"],
                "predicted_mean_commute_min": baseline_case["predicted_moments"]["mean_commute_min"],
            },
            "po_theta_hat": {
                "theta": float(po_primary),
                "global_l1_pct": po_case["global_l1_pct"],
                "off_diagonal_l1_pct": po_case["off_diagonal_l1_pct"],
                "smm_relative_sse": po_case["smm_relative_sse"],
                "predicted_mean_commute_min": po_case["predicted_moments"]["mean_commute_min"],
            },
            "smm_best": {
                "theta": float(smm_best["theta"]),
                "global_l1_pct": smm_best["global_l1_pct"],
                "off_diagonal_l1_pct": smm_best["off_diagonal_l1_pct"],
                "smm_relative_sse": smm_best["smm_relative_sse"],
                "predicted_mean_commute_min": smm_best["predicted_moments"]["mean_commute_min"],
            },
        },
        "intra_spec_used": intra_spec,
        "note": (
            "PO gravity and SMM operate on the OD-improvement layer only. "
            "Re-inversion still uses residence/workplace marginals; theta changes fundamentals, not Stage 1 comparison logic."
        ),
    }


def write_json(path: Path, obj: dict) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, default=float))
