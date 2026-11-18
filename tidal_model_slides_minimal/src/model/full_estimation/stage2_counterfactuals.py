"""Stage 2 counterfactuals: exogenous-tau replica + endogenous congestion."""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from src.model.directional_cost import fixed_tau_model as ft
from src.model.directional_cost.policy_counterfactuals import (
    PolicySpec,
    apply_policy_abar,
    apply_policy_tau,
    apply_policy_ubar,
    default_policies,
    employment_hub_mask,
    outer_ring_mask,
)
from src.model.directional_cost.tidal_lane_counterfactuals import (
    TidalLaneSpec,
    apply_tidal_lane_shock,
    default_experiment_grid,
    select_pairs,
)
from src.model.full_estimation.build_primitives import Primitives
from src.model.full_estimation.counterfactual_levels import (
    TidalLaneSelection,
    run_cf1_symmetric,
    run_cf2_tidal,
    run_cf3_uniform,
    select_tidal_lane_pairs,
    select_tidal_lane_pairs_by_rank,
    summarise_diff,
)
from src.model.full_estimation.equilibrium_solve import (
    BEIJING_LAMBDA_PHYS,
    MAINLINE_CONGESTION_FORM,
    MAINLINE_DAMPING,
    MAINLINE_LAMBDA_PHYS,
    MAINLINE_PAIR_SELECTION,
    _build_A_matrix,
    _compute_tau_inv_theta,
    _congestion_tau_update,
    _resolve_lanes,
    compute_observation_anchors,
)
from src.model.full_estimation.equilibrium_solve import EquilibriumResult, solve_equilibrium
from src.model.full_estimation.invert_fundamentals import InversionResult
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


def summarise_vs_baseline(
    baseline: EquilibriumResult,
    scenario: EquilibriumResult,
    prim: Primitives,
    params: Parameters,
    abar_theta: np.ndarray,
    abar_theta_scenario: np.ndarray | None = None,
) -> dict:
    """Stage-1-compatible comparison metrics on level-form equilibria."""
    abar_scen = abar_theta if abar_theta_scenario is None else abar_theta_scenario
    row = ft.comparison_summary(
        _to_static(baseline),
        _to_static(scenario),
        prim,
        params,
        abar_theta=abar_theta,
        abar_theta_scenario=abar_scen,
    )
    row.update({
        "label_baseline": baseline.label,
        "label_scenario": scenario.label,
        "endogenous_congestion": scenario.endogenous_congestion,
        "mean_Xi_pct_change": 100.0 * float(np.mean(scenario.Xi_kl / np.clip(baseline.Xi_kl, EPS, None) - 1.0)),
        "p95_abs_log_Pi_change": float(np.quantile(
            np.abs(np.log(np.clip(scenario.Pi_i, EPS, None) / np.clip(baseline.Pi_i, EPS, None))),
            0.95,
        )),
    })
    return row


def build_congestion_solver_kwargs(
    prim: Primitives,
    fund: InversionResult,
    params: Parameters,
    *,
    congestion_form: str = MAINLINE_CONGESTION_FORM,
    congestion_lambda: float = MAINLINE_LAMBDA_PHYS,
    damping: float | None = None,
    **extra,
) -> dict:
    """Solver kwargs for mainline anchored congestion (semi-lock: level at tau_obs)."""
    damp = MAINLINE_DAMPING if congestion_form == "anchored" else 0.5
    if damping is not None:
        damp = damping
    kw = _solver_kwargs(
        fund,
        max_outer=extra.pop("max_outer", 80),
        tol=extra.pop("tol", 1e-5),
        damping=damp,
        congestion_form=congestion_form,
        congestion_lambda=congestion_lambda if congestion_form != "legacy" else None,
        **extra,
    )
    if congestion_form == "anchored":
        anchors = compute_observation_anchors(
            prim, fund.ubar_theta, fund.abar_theta, params,
            chi=fund.chi, kappa=fund.kappa,
        )
        kw.update(
            xi_obs_kl=anchors["xi_obs_kl"],
            n_obs_kl=anchors["n_obs_kl"],
            tau_obs_anchor=anchors["tau_obs_anchor"],
        )
    return kw


def _solver_kwargs(fund: InversionResult, **extra) -> dict:
    base = dict(
        kappa=fund.kappa,
        chi=fund.chi,
        max_outer=extra.pop("max_outer", 80),
        tol=extra.pop("tol", 1e-5),
        damping=extra.pop("damping", 0.5),
    )
    base.update(extra)
    return base


def solve_baseline_endogenous(
    prim: Primitives,
    fund: InversionResult,
    params: Parameters,
    **kwargs,
) -> EquilibriumResult:
    """Observed-anchored baseline with endogenous (E5)-(E6) feedback."""
    kw = _solver_kwargs(fund, **kwargs)
    return solve_equilibrium(
        prim, fund.ubar_theta, fund.abar_theta, params,
        tau_min_init=prim.tau_obs_min.copy(),
        endogenous_congestion=True,
        label="baseline_endogenous",
        **kw,
    )


def solve_baseline_exogenous_tau(
    prim: Primitives,
    fund: InversionResult,
    params: Parameters,
    **kwargs,
) -> EquilibriumResult:
    """Fixed observed τ — parity with Stage 1 static solver on level-form stack."""
    kw = _solver_kwargs(fund, **kwargs)
    return solve_equilibrium(
        prim, fund.ubar_theta, fund.abar_theta, params,
        tau_min_init=prim.tau_obs_min.copy(),
        endogenous_congestion=False,
        label="baseline_exogenous_tau",
        **kw,
    )


def run_policy_exogenous_tau(
    prim: Primitives,
    fund: InversionResult,
    params: Parameters,
    baseline: EquilibriumResult,
    spec: PolicySpec,
    **kwargs,
) -> EquilibriumResult:
    """Mirror Stage 1 policy CF: fixed τ shock, fundamentals fixed."""
    kw = _solver_kwargs(fund, **kwargs)
    tau_cf = apply_policy_tau(prim.tau_obs_min, prim, spec)
    ubar_cf = apply_policy_ubar(fund.ubar_theta, prim, spec)
    abar_cf = apply_policy_abar(fund.abar_theta, prim, spec)
    return solve_equilibrium(
        prim, ubar_cf, abar_cf, params,
        tau_min_init=tau_cf,
        endogenous_congestion=False,
        label=f"{spec.name}_exogenous_tau",
        **kw,
    )


def _apply_tau_ff_policy(tau_ff: np.ndarray, prim: Primitives, spec: PolicySpec) -> np.ndarray:
    """Apply the same multipliers as Stage 1 τ shocks to free-flow times."""
    out = np.asarray(tau_ff, dtype=float).copy() * spec.tau_scale
    if spec.tau_scale_on_hub_edges is not None:
        hub = employment_hub_mask(prim, spec.hub_quantile)
        touch = hub[prim.edge_i] | hub[prim.edge_j]
        out[touch] *= spec.tau_scale_on_hub_edges
    if spec.tau_scale_on_outer_edges is not None:
        outer = outer_ring_mask(prim, spec.outer_ring_dist_quantile)
        touch = outer[prim.edge_i] | outer[prim.edge_j]
        out[touch] *= spec.tau_scale_on_outer_edges
    return out


def run_policy_endogenous(
    prim: Primitives,
    fund: InversionResult,
    params: Parameters,
    spec: PolicySpec,
    **kwargs,
) -> EquilibriumResult:
    """Policy with congestion feedback.

    Transport shocks scale free-flow times; amenity/productivity shocks
    scale inverted fundamentals. τ re-equilibrates from observed anchor.
    """
    kw = _solver_kwargs(fund, **kwargs)
    tau_ff = _apply_tau_ff_policy(prim.tau_ff_min, prim, spec)
    ubar_cf = apply_policy_ubar(fund.ubar_theta, prim, spec)
    abar_cf = apply_policy_abar(fund.abar_theta, prim, spec)
    return solve_equilibrium(
        prim, ubar_cf, abar_cf, params,
        tau_ff_override=tau_ff,
        tau_min_init=prim.tau_obs_min.copy(),
        endogenous_congestion=True,
        label=f"{spec.name}_endogenous",
        **kw,
    )


def run_tidal_endogenous(
    prim: Primitives,
    fund: InversionResult,
    params: Parameters,
    baseline: EquilibriumResult,
    top_k: int = 50,
    delta: float = 1.0,
    horizon: str = "long_run",
    pair_selection: str = MAINLINE_PAIR_SELECTION,
    **kwargs,
) -> tuple[EquilibriumResult, TidalLaneSelection]:
    kw = _solver_kwargs(fund, **kwargs)
    sel = select_tidal_lane_pairs_by_rank(
        prim, top_k=top_k, delta=delta, rank=pair_selection,
        xi_kl=baseline.Xi_kl if pair_selection == "score_bpr_st" else None,
    )
    n_cf = prim.n_kl.copy()
    for slow, fast in sel.treated_pairs:
        n_cf[slow] = n_cf[slow] + delta
        n_cf[fast] = n_cf[fast] - delta
    pin_kw: dict = {}
    if horizon == "short_run":
        pin_kw = dict(l_R_fixed=baseline.l_R, l_F_fixed=baseline.l_F)
    elif horizon != "long_run":
        raise ValueError(f"unknown tidal horizon: {horizon}")
    eq = solve_equilibrium(
        prim, fund.ubar_theta, fund.abar_theta, params,
        n_kl_override=n_cf,
        tau_min_init=prim.tau_obs_min.copy(),
        endogenous_congestion=True,
        label=f"tidal_top{top_k}_d{delta}_{horizon}",
        **pin_kw,
        **kw,
    )
    return eq, sel


def run_tidal_exogenous_bpr_grid(
    prim: Primitives,
    fund: InversionResult,
    params: Parameters,
    baseline: EquilibriumResult,
    specs: list[TidalLaneSpec] | None = None,
    **kwargs,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Stage 1 tidal replica: BPR at fixed ξ, no network congestion loop."""
    kw = _solver_kwargs(fund, **kwargs)
    specs = specs or default_experiment_grid()
    rows: list[dict] = []
    edge_parts: list[pd.DataFrame] = []
    for spec in specs:
        selected = select_pairs(prim, spec)
        tau_cf, edge_detail = apply_tidal_lane_shock(prim, prim.tau_obs_min, spec, selected=selected)
        for horizon, endog, pin in (
            ("exogenous_tau", False, False),
            ("short_run_fixed_shares", False, True),
        ):
            pin_kw: dict = {}
            if pin:
                pin_kw = dict(l_R_fixed=baseline.l_R, l_F_fixed=baseline.l_F)
            eq_cf = solve_equilibrium(
                prim, fund.ubar_theta, fund.abar_theta, params,
                tau_min_init=tau_cf,
                endogenous_congestion=endog,
                label=f"{spec.name}_{horizon}",
                **pin_kw,
                **kw,
            )
            row = summarise_vs_baseline(baseline, eq_cf, prim, params, fund.abar_theta)
            row.update({
                "policy_family": "tidal_lane_bpr_fixed_xi",
                "spec_name": spec.name,
                "horizon": horizon,
                "top_n": spec.top_n,
                "delta_lanes": spec.delta_lanes,
                "n_pairs": int(len(selected)),
            })
            rows.append(row)
        if not edge_detail.empty:
            edge_parts.append(edge_detail)
    return pd.DataFrame(rows), pd.concat(edge_parts, ignore_index=True) if edge_parts else pd.DataFrame()


def run_all_policy_counterfactuals(
    prim: Primitives,
    fund: InversionResult,
    params: Parameters,
    baseline_exog: EquilibriumResult,
    baseline_endog: EquilibriumResult | None = None,
    policies: list[PolicySpec] | None = None,
    **kwargs,
) -> pd.DataFrame:
    policies = policies or default_policies()
    baseline_endog = baseline_endog or baseline_exog
    rows: list[dict] = []
    for spec in policies:
        eq_exog = run_policy_exogenous_tau(prim, fund, params, baseline_exog, spec, **kwargs)
        row_exog = summarise_vs_baseline(baseline_exog, eq_exog, prim, params, fund.abar_theta)
        row_exog.update({"policy": spec.name, "congestion_mode": "exogenous_tau"})
        rows.append(row_exog)

        eq_endog = run_policy_endogenous(prim, fund, params, spec, **kwargs)
        row_endog = summarise_vs_baseline(
            baseline_endog, eq_endog, prim, params, fund.abar_theta,
        )
        row_endog.update({"policy": spec.name, "congestion_mode": "endogenous"})
        rows.append(row_endog)
    return pd.DataFrame(rows)


def run_structural_counterfactuals(
    prim: Primitives,
    fund: InversionResult,
    params: Parameters,
    baseline: EquilibriumResult,
    cf2_top_k: int = 50,
    cf3_factors: list[float] | None = None,
    sym_method: str = "geometric",
    pair_selection: str = MAINLINE_PAIR_SELECTION,
    **kwargs,
) -> pd.DataFrame:
    """CF1 symmetric, CF2 tidal lanes, CF3 uniform expansion — all endogenous."""
    kw = _solver_kwargs(fund, **kwargs)
    rows: list[dict] = []

    eq_cf1 = run_cf1_symmetric(
        prim, params, fund.ubar_theta, fund.abar_theta,
        sym_method=sym_method, **kw,
    )
    row = summarise_vs_baseline(baseline, eq_cf1, prim, params, fund.abar_theta)
    row.update({
        "cf": f"CF1_symmetric_{sym_method}",
        "congestion_mode": "endogenous",
    })
    rows.append(row)

    if cf2_top_k <= 0:
        return pd.DataFrame(rows)

    eq_cf2, sel = run_cf2_tidal(
        prim, params, fund.ubar_theta, fund.abar_theta,
        top_k=cf2_top_k, pair_selection=pair_selection,
        xi_kl=baseline.Xi_kl if pair_selection == "score_bpr_st" else None,
        **kw,
    )
    row = summarise_vs_baseline(baseline, eq_cf2, prim, params, fund.abar_theta)
    row.update({
        "cf": f"CF2_tidal_top{cf2_top_k}",
        "congestion_mode": "endogenous",
        "n_pairs": sel.n_treated,
        "pair_selection": pair_selection,
    })
    rows.append(row)

    factors = [1.10, 1.25] if cf3_factors is None else cf3_factors
    for fac in factors:
        eq_cf3 = run_cf3_uniform(
            prim, params, fund.ubar_theta, fund.abar_theta, factor=fac, **kw,
        )
        row = summarise_vs_baseline(baseline, eq_cf3, prim, params, fund.abar_theta)
        row.update({"cf": f"CF3_uniform_{fac:.2f}", "congestion_mode": "endogenous"})
        rows.append(row)

    return pd.DataFrame(rows)


def run_structural_exogenous(
    prim: Primitives,
    fund: InversionResult,
    params: Parameters,
    baseline: EquilibriumResult,
    *,
    cf2_top_k: int = 50,
    cf3_factors: list[float] | None = None,
    sym_method: str = "geometric",
    pair_selection: str = MAINLINE_PAIR_SELECTION,
    **kwargs,
) -> pd.DataFrame:
    """CF1/CF2/CF3 with fixed τ (no congestion feedback loop).

    These are *measurement* counterfactuals: a direct shock to the travel-cost
    field τ, with congestion frozen. They are NOT the fixed-feedback twin of the
    endogenous capacity policies:
      * CF1 exog fixes the symmetrised τ table. The endogenous CF1 diagnostic
        starts from the same table, then lets anchored congestion update edge
        times as traffic re-equilibrates.
      * CF2 exog applies the anchored capacity-equivalent τ at baseline Ξ.
      * CF3 exog is a uniform *speed* shock τ→τ/factor (a measurement question),
        deliberately distinct from the endogenous CF3 which is a *capacity*
        shock n→factor·n. The deck labels these two arms accordingly.
    """
    from src.model.full_estimation.counterfactual_levels import build_symmetric_tau_min

    kw = _solver_kwargs(fund, **kwargs)
    rows: list[dict] = []

    tau_sym = build_symmetric_tau_min(prim, method=sym_method)
    eq_cf1 = solve_equilibrium(
        prim, fund.ubar_theta, fund.abar_theta, params,
        tau_min_init=tau_sym,
        endogenous_congestion=False,
        label=f"CF1_symmetric_{sym_method}_exog",
        **kw,
    )
    row = summarise_vs_baseline(baseline, eq_cf1, prim, params, fund.abar_theta)
    row.update({"cf": f"CF1_symmetric_{sym_method}", "congestion_mode": "exogenous_tau"})
    rows.append(row)

    if cf2_top_k <= 0:
        return pd.DataFrame(rows)

    sel = select_tidal_lane_pairs_by_rank(
        prim, top_k=cf2_top_k, delta=1.0, rank=pair_selection,
        xi_kl=baseline.Xi_kl if pair_selection == "score_bpr_st" else None,
    )
    tau_cf2 = prim.tau_obs_min.copy()
    delta1 = params.lambda_ * params.theta
    for slow, fast in sel.treated_pairs:
        for idx, d_n in ((slow, 1.0), (fast, -1.0)):
            n_old = float(prim.n_kl[idx])
            n_new = max(n_old + d_n, 0.25)
            xi = float(baseline.Xi_kl[idx])
            tau_ff = float(prim.tau_ff_min[idx])
            tau_cf2[idx] = max(
                tau_ff * (xi / max(n_new, EPS)) ** delta1,
                tau_ff,
            )
    eq_cf2 = solve_equilibrium(
        prim, fund.ubar_theta, fund.abar_theta, params,
        tau_min_init=tau_cf2,
        endogenous_congestion=False,
        label=f"CF2_tidal_top{cf2_top_k}_exog",
        **kw,
    )
    row = summarise_vs_baseline(baseline, eq_cf2, prim, params, fund.abar_theta)
    row.update({
        "cf": f"CF2_tidal_top{cf2_top_k}",
        "congestion_mode": "exogenous_tau",
        "n_pairs": sel.n_treated,
        "pair_selection": pair_selection,
    })
    rows.append(row)

    factors = [1.10] if cf3_factors is None else cf3_factors
    for fac in factors:
        tau_cf3 = prim.tau_obs_min / float(fac)
        eq_cf3 = solve_equilibrium(
            prim, fund.ubar_theta, fund.abar_theta, params,
            tau_min_init=tau_cf3,
            endogenous_congestion=False,
            label=f"CF3_uniform_{fac:.2f}_exog",
            **kw,
        )
        row = summarise_vs_baseline(baseline, eq_cf3, prim, params, fund.abar_theta)
        row.update({"cf": f"CF3_uniform_{fac:.2f}", "congestion_mode": "exogenous_tau"})
        rows.append(row)

    return pd.DataFrame(rows)


def _lane_km_added(prim: Primitives, n_base: np.ndarray, n_cf: np.ndarray) -> float:
    return float(np.sum((n_cf - n_base) * prim.ell_kl_km))


def run_road_expansion_same_corridor(
    prim: Primitives,
    fund: InversionResult,
    params: Parameters,
    baseline: EquilibriumResult,
    *,
    top_k: int = 100,
    delta: float = 1.0,
    pair_selection: str = "score_bpr_st",
    horizon: str = "long_run",
    **kwargs,
) -> tuple[EquilibriumResult, TidalLaneSelection]:
    """P2: same treated pairs as tidal, +delta lanes on both directions."""
    from src.model.full_estimation.counterfactual_levels import apply_bidirectional_lane_delta

    kw = _solver_kwargs(fund, **kwargs)
    sel = select_tidal_lane_pairs_by_rank(
        prim, top_k=top_k, delta=delta, rank=pair_selection,
        xi_kl=baseline.Xi_kl if pair_selection == "score_bpr_st" else None,
    )
    n_cf = apply_bidirectional_lane_delta(prim, sel, delta=delta)
    pin_kw: dict = {}
    if horizon == "short_run":
        pin_kw = dict(l_R_fixed=baseline.l_R, l_F_fixed=baseline.l_F)
    eq = solve_equilibrium(
        prim, fund.ubar_theta, fund.abar_theta, params,
        n_kl_override=n_cf,
        tau_min_init=prim.tau_obs_min.copy(),
        endogenous_congestion=True,
        label=f"expansion_same_corridor_top{top_k}_{horizon}",
        **pin_kw,
        **kw,
    )
    return eq, sel


def run_road_expansion_qdelay_topk(
    prim: Primitives,
    fund: InversionResult,
    params: Parameters,
    baseline: EquilibriumResult,
    *,
    top_k: int = 100,
    delta: float = 1.0,
    horizon: str = "long_run",
    **kwargs,
) -> tuple[EquilibriumResult, TidalLaneSelection]:
    """P3: top-K pairs by q·delay, bidirectional +delta."""
    from src.model.full_estimation.counterfactual_levels import (
        apply_bidirectional_lane_delta,
        select_tidal_lane_pairs_by_qdelay,
    )

    kw = _solver_kwargs(fund, **kwargs)
    sel = select_tidal_lane_pairs_by_qdelay(
        prim, top_k=top_k, delta=delta, xi_kl=baseline.Xi_kl,
    )
    n_cf = apply_bidirectional_lane_delta(prim, sel, delta=delta)
    pin_kw: dict = {}
    if horizon == "short_run":
        pin_kw = dict(l_R_fixed=baseline.l_R, l_F_fixed=baseline.l_F)
    eq = solve_equilibrium(
        prim, fund.ubar_theta, fund.abar_theta, params,
        n_kl_override=n_cf,
        tau_min_init=prim.tau_obs_min.copy(),
        endogenous_congestion=True,
        label=f"expansion_qdelay_top{top_k}_{horizon}",
        **pin_kw,
        **kw,
    )
    return eq, sel


def realized_welfare_excl_toll(
    prim: Primitives,
    fund: InversionResult,
    params: Parameters,
    scenario: EquilibriumResult,
    *,
    congestion_lambda: float,
    xi_obs_kl: np.ndarray,
    n_obs_kl: np.ndarray,
    tau_obs_anchor: np.ndarray,
    n_kl: np.ndarray | None = None,
    congestion_form: str = MAINLINE_CONGESTION_FORM,
) -> float:
    """Gauge-free welfare at a *priced* equilibrium using REALIZED travel time.

    A monetary toll added to ``tau_ff`` is a transfer that is rebated lump-sum,
    not a real resource cost. The priced equilibrium (``scenario``) already gives
    the behavioural allocation (``l_R, l_F, Ξ``) under the perceived cost. Here we
    re-evaluate the anchored congestion edge times at that equilibrium ``Ξ`` with
    the ORIGINAL free-flow floor ``prim.tau_ff_min`` (toll excluded), rebuild the
    OD cost matrix, and recompute ``W̄``. For toll-free policies this reproduces
    ``scenario.Wbar`` exactly, so it is a safe drop-in headline for all policies.
    """
    n_used = prim.n_kl if n_kl is None else n_kl
    tau_real = _congestion_tau_update(
        form=congestion_form,
        lam=congestion_lambda,
        tau_ff=prim.tau_ff_min,
        tau_obs=tau_obs_anchor,
        Xi_kl=scenario.Xi_kl,
        n_kl=_resolve_lanes(n_used),
        ell_km=prim.ell_kl_km,
        xi_obs_kl=xi_obs_kl,
        n_obs_kl=n_obs_kl,
    )
    A = _build_A_matrix(prim.edge_i, prim.edge_j, tau_real, prim.n_nodes, kappa=scenario.kappa)
    tau_inv = _compute_tau_inv_theta(A)
    th = params.theta
    L_R = np.clip(scenario.l_R * prim.Lbar, EPS, None)
    L_F = np.clip(scenario.l_F * prim.Lbar, EPS, None)
    u_theta = fund.ubar_theta * (L_R ** (params.beta * th))
    w_theta = fund.abar_theta * (L_F ** (params.alpha * th))
    Wbar_theta = float(np.sum(tau_inv * u_theta[:, None] * w_theta[None, :]))
    return Wbar_theta ** (1.0 / th) if Wbar_theta > 0 else float("nan")


def _pigouvian_toll_per_edge(
    prim: Primitives,
    baseline: EquilibriumResult,
    params: Parameters,
    *,
    congestion_lambda: float = MAINLINE_LAMBDA_PHYS,
) -> np.ndarray:
    """Marginal external cost toll_e ≈ λ_phys · τ_e at baseline Ξ (anchored slope)."""
    return congestion_lambda * baseline.tau_min_kl


def run_congestion_pricing_first_best(
    prim: Primitives,
    fund: InversionResult,
    params: Parameters,
    baseline: EquilibriumResult,
    *,
    horizon: str = "long_run",
    congestion_lambda: float = MAINLINE_LAMBDA_PHYS,
    **kwargs,
) -> tuple[EquilibriumResult, dict]:
    """P6 diagnostic: add a baseline congestion toll to perceived edge cost."""
    kw = _solver_kwargs(fund, **kwargs)
    toll = _pigouvian_toll_per_edge(
        prim, baseline, params, congestion_lambda=congestion_lambda,
    )
    tau_ff_cf = prim.tau_ff_min + toll
    pin_kw: dict = {}
    if horizon == "short_run":
        pin_kw = dict(l_R_fixed=baseline.l_R, l_F_fixed=baseline.l_F)
    eq = solve_equilibrium(
        prim, fund.ubar_theta, fund.abar_theta, params,
        tau_ff_override=tau_ff_cf,
        tau_min_init=prim.tau_obs_min.copy(),
        endogenous_congestion=True,
        label=f"pricing_first_best_{horizon}",
        **pin_kw,
        **kw,
    )
    # Revenue is collected on the PRICED-equilibrium flows, not baseline flows:
    # the toll depresses traffic, so baseline Xi overstates the transfer
    # (bug fix: was baseline.Xi_kl).
    revenue = float(np.sum(eq.Xi_kl * toll))
    meta = {
        "toll_revenue_per_day": revenue,
        "lump_sum_rebate_per_worker": revenue / max(prim.Lbar, EPS),
        "policy_capex_lane_km": 0.0,
    }
    return eq, meta


def run_cordon_pricing(
    prim: Primitives,
    fund: InversionResult,
    params: Parameters,
    baseline: EquilibriumResult,
    *,
    hub_quantile: float = 0.90,
    toll_per_min: float = 2.0,
    horizon: str = "long_run",
    **kwargs,
) -> tuple[EquilibriumResult, dict]:
    """P7: flat cordon toll on edges touching employment hub nodes."""
    kw = _solver_kwargs(fund, **kwargs)
    hub = employment_hub_mask(prim, hub_quantile)
    touch = hub[prim.edge_i] | hub[prim.edge_j]
    tau_ff_cf = prim.tau_ff_min.copy()
    tau_ff_cf[touch] = tau_ff_cf[touch] + toll_per_min
    pin_kw: dict = {}
    if horizon == "short_run":
        pin_kw = dict(l_R_fixed=baseline.l_R, l_F_fixed=baseline.l_F)
    eq = solve_equilibrium(
        prim, fund.ubar_theta, fund.abar_theta, params,
        tau_ff_override=tau_ff_cf,
        tau_min_init=prim.tau_obs_min.copy(),
        endogenous_congestion=True,
        label=f"pricing_cordon_q{hub_quantile}_{horizon}",
        **pin_kw,
        **kw,
    )
    n_edges = int(np.sum(touch))
    revenue = float(np.sum(baseline.Xi_kl[touch] * toll_per_min))
    meta = {
        "n_treated_edges": n_edges,
        "toll_revenue_per_day": revenue,
        "lump_sum_rebate_per_worker": revenue / max(prim.Lbar, EPS),
        "policy_capex_lane_km": 0.0,
    }
    return eq, meta
