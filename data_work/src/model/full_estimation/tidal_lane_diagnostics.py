"""Tidal-lane diagnostics: congestion cost, first-order vs GE, ST vs LT."""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from src.model.directional_cost import fixed_tau_model as ft
from src.model.full_estimation.build_primitives import Primitives
from src.model.full_estimation.counterfactual_levels import TidalLaneSelection
from src.model.full_estimation.equilibrium_solve import EquilibriumResult
from src.model.full_estimation.parameters import Parameters

EPS = 1e-12


def _to_static(eq: EquilibriumResult) -> ft.StaticEquilibriumResult:
    return ft.StaticEquilibriumResult(
        label=eq.label,
        tau_min_kl=eq.tau_min_kl,
        tau_ij_inv_theta=eq.tau_ij_inv_theta,
        l_R=eq.l_R,
        l_F=eq.l_F,
        Pi_i=eq.Pi_i,
        P_j=eq.P_j,
        Wbar=eq.Wbar,
        n_iter=eq.n_outer,
        converged=eq.converged,
        max_log_change=eq.max_log_change,
        kappa=eq.kappa,
    )


def bilateral_composite_cost_matrix(eq: EquilibriumResult, theta: float) -> np.ndarray:
    """Return the all-route bilateral iceberg-cost index ``tau_ij``.

    This object enters location and route choice.  It is not measured in
    minutes and must not be multiplied by 60 to construct seconds.
    """
    return np.clip(eq.tau_ij_inv_theta, EPS, None) ** (-1.0 / theta)


def equilibrium_od_shares(
    eq: EquilibriumResult,
    prim: Primitives,
    theta: float,
) -> np.ndarray:
    """Return equilibrium OD shares from the gravity and market-access system."""
    R = np.asarray(eq.tau_ij_inv_theta, dtype=float)
    L_R = np.asarray(eq.l_R, dtype=float) * prim.Lbar
    L_F = np.asarray(eq.l_F, dtype=float) * prim.Lbar
    origin = L_R * np.clip(eq.Pi_i, EPS, None) ** theta
    destination = L_F * np.clip(eq.P_j, EPS, None) ** theta
    shares = R * origin[:, None] * destination[None, :] / prim.Lbar
    total = float(shares.sum())
    if not np.isfinite(total) or total <= 0:
        raise ValueError("equilibrium OD shares have non-positive mass")
    return shares / total


def expected_additive_route_minutes(
    eq: EquilibriumResult,
    prim: Primitives,
) -> np.ndarray:
    r"""Return expected physical route minutes conditional on each OD pair.

    Let ``R=(I-A)^{-1}``, where the implemented directed-edge weight is
    ``a_e=1/(kappa*m_e)`` and ``m_e`` is physical link time in minutes.
    For route ``r``, physical time is additive, ``M_r=sum_{e in r} m_e``.
    The route-choice weights remain multiplicative.  Summing the resulting
    conditional expectation over all routes gives

        M_ij = [R B R]_ij / R_ij,

    where ``B_kl=sum_{e:k->l} a_e*m_e``.  The formula counts repeated edge
    passages when routes contain cycles and handles parallel directed edges.
    """
    R = np.asarray(eq.tau_ij_inv_theta, dtype=float)
    eta = float(getattr(eq, "route_eta", 1.0))
    if eta != 1.0:  # tau^-theta = R^eta; the route resolvent is the eta-th root
        R = np.clip(R, EPS, None) ** (1.0 / eta)
    m = np.clip(np.asarray(eq.m_min_kl, dtype=float), EPS, None)
    kappa = float(eq.kappa)
    B = np.zeros_like(R)
    edge_reward_weight = (1.0 / (kappa * m)) * m
    np.add.at(B, (prim.edge_i, prim.edge_j), edge_reward_weight)
    numerator = R @ B @ R
    return np.divide(
        numerator,
        R,
        out=np.zeros_like(numerator),
        where=R > EPS,
    )


def od_cost_decomposition(
    baseline: EquilibriumResult,
    short_run: EquilibriumResult,
    long_run: EquilibriumResult,
    prim: Primitives,
    params: Parameters,
    abar_theta: np.ndarray,
) -> dict:
    """Decompose expected physical commuting-time changes.

    ``M`` is expected additive route time, constructed from physical link
    minutes.  The bilateral composite iceberg-cost index ``tau_ij`` is retained
    separately and never interpreted as minutes.

    The legacy decomposition names are preserved for downstream scripts, but
    their values now refer to physical time rather than ``tau_ij``.
    """
    od0 = equilibrium_od_shares(baseline, prim, params.theta)
    od_st = equilibrium_od_shares(short_run, prim, params.theta)
    od_lt = equilibrium_od_shares(long_run, prim, params.theta)
    M0 = expected_additive_route_minutes(baseline, prim)
    Mst = expected_additive_route_minutes(short_run, prim)
    Mlt = expected_additive_route_minutes(long_run, prim)

    od_route_avg0 = float(np.sum(od0 * M0))
    od_route_avg_st = float(np.sum(od_st * Mst))
    od_route_avg_lt = float(np.sum(od_lt * Mlt))

    composite0 = bilateral_composite_cost_matrix(baseline, params.theta)
    composite_st = bilateral_composite_cost_matrix(short_run, params.theta)
    composite_lt = bilateral_composite_cost_matrix(long_run, params.theta)

    def link_average(eq: EquilibriumResult) -> float:
        # The solver's Xi omits the route-diffusion normalization when kappa != 1.
        return float(
            np.dot(eq.Xi_kl, eq.m_min_kl) /
            (prim.Lbar * float(eq.kappa))
        )

    avg0 = link_average(baseline)
    avg_st = link_average(short_run)
    avg_lt = link_average(long_run)
    total_st = avg_st - avg0
    total_lt = avg_lt - avg0

    # Exact physical-time decomposition in link-passage space.  The short-run
    # term includes the complete ST equilibrium change.  The remaining two
    # terms split the ST-to-LT adjustment into a link-time change at ST
    # passages and a passage-reallocation term evaluated at LT minutes.
    st_passages = np.asarray(short_run.Xi_kl, dtype=float) / float(short_run.kappa)
    lt_passages = np.asarray(long_run.Xi_kl, dtype=float) / float(long_run.kappa)
    direct_st = total_st
    ge_wedge = float(
        np.dot(long_run.m_min_kl - short_run.m_min_kl, st_passages) /
        prim.Lbar
    )
    sorting = float(
        np.dot(long_run.m_min_kl, lt_passages - st_passages) /
        prim.Lbar
    )

    return {
        "avg_physical_time_base_min": avg0,
        "avg_physical_time_st_min": avg_st,
        "avg_physical_time_lt_min": avg_lt,
        "od_route_matrix_avg_time_base_min": od_route_avg0,
        "od_route_matrix_avg_time_st_min": od_route_avg_st,
        "od_route_matrix_avg_time_lt_min": od_route_avg_lt,
        "avg_composite_cost_base_index": float(np.sum(od0 * composite0)),
        "avg_composite_cost_st_index": float(np.sum(od_st * composite_st)),
        "avg_composite_cost_lt_index": float(np.sum(od_lt * composite_lt)),
        "direct_st_cost_change_min": direct_st,
        "direct_st_cost_change_sec": 60.0 * direct_st,
        "ge_cost_wedge_min": ge_wedge,
        "ge_cost_wedge_sec": 60.0 * ge_wedge,
        "sorting_wedge_min": sorting,
        "sorting_wedge_sec": 60.0 * sorting,
        "total_lt_cost_change_min": total_lt,
        "total_lt_cost_change_sec": 60.0 * total_lt,
        "total_st_cost_change_min": total_st,
        "total_st_cost_change_sec": 60.0 * total_st,
        "physical_time_saved_st_sec_per_commuter": -60.0 * total_st,
        "physical_time_saved_lt_sec_per_commuter": -60.0 * total_lt,
        "physical_time_saved_st_person_hours": -total_st * prim.Lbar / 60.0,
        "physical_time_saved_lt_person_hours": -total_lt * prim.Lbar / 60.0,
        "od_route_matrix_minus_link_time_base_sec": 60.0 * (od_route_avg0 - avg0),
        "od_route_matrix_minus_link_time_st_sec": 60.0 * (od_route_avg_st - avg_st),
        "od_route_matrix_minus_link_time_lt_sec": 60.0 * (od_route_avg_lt - avg_lt),
        "od_dissimilarity_st_vs_base_pct": 100.0 * 0.5 * float(
            np.sum(np.abs(od_st - od0))
        ),
        "od_dissimilarity_lt_vs_base_pct": 100.0 * 0.5 * float(
            np.sum(np.abs(od_lt - od0))
        ),
    }


def edge_congestion_summary(
    eq: EquilibriumResult,
    prim: Primitives,
    baseline: EquilibriumResult | None = None,
    treated_edge_idx: np.ndarray | None = None,
) -> dict:
    """Edge-level congestion cost objects (minutes and markups)."""
    tau = eq.tau_min_kl
    tau_ff = prim.tau_ff_min
    tau_obs = prim.tau_obs_min
    xi = np.clip(eq.Xi_kl, EPS, None)
    w = xi / xi.sum()
    markup_ff = tau / np.clip(tau_ff, EPS, None) - 1.0
    markup_obs = tau / np.clip(tau_obs, EPS, None) - 1.0
    cong_min = tau - tau_ff

    def _mask_stats(mask: np.ndarray, prefix: str) -> dict:
        if not mask.any():
            return {}
        wm = w[mask] / w[mask].sum()
        return {
            f"{prefix}_mean_tau_min": float(tau[mask].mean()),
            f"{prefix}_mean_congestion_min": float(cong_min[mask].mean()),
            f"{prefix}_xi_weighted_congestion_min": float(np.sum(wm * cong_min[mask])),
            f"{prefix}_mean_markup_ff": float(np.sum(wm * markup_ff[mask])),
            f"{prefix}_mean_markup_obs": float(np.sum(wm * markup_obs[mask])),
            f"{prefix}_total_xi_weighted_travel_min": float(np.sum(wm * tau[mask])),
        }

    out = {
        "network_xi_weighted_congestion_min": float(np.sum(w * cong_min)),
        "network_xi_weighted_travel_min": float(np.sum(w * tau)),
        "network_mean_markup_ff": float(np.sum(w * markup_ff)),
        "network_mean_markup_obs": float(np.sum(w * markup_obs)),
        "network_total_xi": float(xi.sum()),
    }
    out.update(_mask_stats(np.ones_like(tau, dtype=bool), "network"))

    if treated_edge_idx is not None and len(treated_edge_idx):
        mask = np.zeros_like(tau, dtype=bool)
        mask[np.asarray(treated_edge_idx, dtype=int)] = True
        out.update(_mask_stats(mask, "treated"))

    if baseline is not None:
        dtau = tau - baseline.tau_min_kl
        dxi = eq.Xi_kl - baseline.Xi_kl
        out["network_mean_delta_tau_min"] = float(dtau.mean())
        out["network_xi_weighted_delta_tau_min"] = float(np.sum(w * dtau))
        out["network_mean_delta_xi_pct"] = 100.0 * float(
            np.mean(eq.Xi_kl / np.clip(baseline.Xi_kl, EPS, None) - 1.0)
        )
        if treated_edge_idx is not None and len(treated_edge_idx):
            mask = np.zeros_like(tau, dtype=bool)
            mask[np.asarray(treated_edge_idx, dtype=int)] = True
            wm = w[mask] / w[mask].sum()
            out["treated_mean_delta_tau_min"] = float(dtau[mask].mean())
            out["treated_xi_weighted_delta_tau_min"] = float(np.sum(wm * dtau[mask]))
            out["treated_mean_delta_xi_pct"] = 100.0 * float(
                np.mean(eq.Xi_kl[mask] / np.clip(baseline.Xi_kl[mask], EPS, None) - 1.0)
            )

    return out


def treated_edge_frame(
    prim: Primitives,
    baseline: EquilibriumResult,
    scenario: EquilibriumResult,
    sel: TidalLaneSelection,
) -> pd.DataFrame:
    rows = []
    for k, (slow, fast) in enumerate(sel.treated_pairs):
        for role, idx in (("slow", slow), ("fast", fast)):
            rows.append({
                "pair_rank": k + 1,
                "role": role,
                "edge_idx": int(idx),
                "node_i": int(prim.edge_i[idx]),
                "node_j": int(prim.edge_j[idx]),
                "tau_base_min": float(baseline.tau_min_kl[idx]),
                "tau_scenario_min": float(scenario.tau_min_kl[idx]),
                "delta_tau_min": float(scenario.tau_min_kl[idx] - baseline.tau_min_kl[idx]),
                "delta_tau_sec": 60.0 * float(scenario.tau_min_kl[idx] - baseline.tau_min_kl[idx]),
                "delta_tau_pct": 100.0 * float(
                    scenario.tau_min_kl[idx] / np.clip(baseline.tau_min_kl[idx], EPS, None) - 1.0
                ),
                "tau_ff_min": float(prim.tau_ff_min[idx]),
                "congestion_markup_ff_base": float(
                    baseline.tau_min_kl[idx] / np.clip(prim.tau_ff_min[idx], EPS, None) - 1.0
                ),
                "congestion_markup_ff_scenario": float(
                    scenario.tau_min_kl[idx] / np.clip(prim.tau_ff_min[idx], EPS, None) - 1.0
                ),
                "Xi_base": float(baseline.Xi_kl[idx]),
                "Xi_scenario": float(scenario.Xi_kl[idx]),
                "delta_Xi_pct": 100.0 * float(
                    scenario.Xi_kl[idx] / np.clip(baseline.Xi_kl[idx], EPS, None) - 1.0
                ),
                "n_lanes_base": float(prim.n_kl[idx]),
            })
    return pd.DataFrame(rows)


def summarise_tidal_pair(
    *,
    spec_label: str,
    top_k: int,
    delta_lanes: float,
    baseline: EquilibriumResult,
    short_run: EquilibriumResult,
    long_run: EquilibriumResult,
    prim: Primitives,
    params: Parameters,
    abar_theta: np.ndarray,
    sel: TidalLaneSelection,
) -> tuple[dict, dict, pd.DataFrame]:
    """One row for ST/LT comparison table + decomposition + treated-edge detail."""
    treated_idx = np.unique(np.concatenate([sel.treated_pairs.ravel()]))
    decomp = od_cost_decomposition(baseline, short_run, long_run, prim, params, abar_theta)

    row = {
        "spec": spec_label,
        "top_k": top_k,
        "delta_lanes": delta_lanes,
        "n_pairs": sel.n_treated,
        "Wbar_base": baseline.Wbar,
        "Wbar_st": short_run.Wbar,
        "Wbar_lt": long_run.Wbar,
        "Wbar_pct_change_st": 100.0 * (short_run.Wbar / baseline.Wbar - 1.0),
        "Wbar_pct_change_lt": 100.0 * (long_run.Wbar / baseline.Wbar - 1.0),
        "sorting_welfare_wedge_pct": 100.0 * (
            (long_run.Wbar / baseline.Wbar - 1.0)
            - (short_run.Wbar / baseline.Wbar - 1.0)
        ),
        **decomp,
    }
    for horizon, eq in (("st", short_run), ("lt", long_run)):
        edge = edge_congestion_summary(
            eq, prim, baseline=baseline, treated_edge_idx=treated_idx,
        )
        for k, v in edge.items():
            row[f"{horizon}_{k}"] = v

    detail = treated_edge_frame(prim, baseline, long_run, sel)
    detail["spec"] = spec_label
    detail["horizon"] = "long_run"
    detail_st = treated_edge_frame(prim, baseline, short_run, sel)
    detail_st["spec"] = spec_label
    detail_st["horizon"] = "short_run"
    edges = pd.concat([detail_st, detail], ignore_index=True)
    return row, decomp, edges
