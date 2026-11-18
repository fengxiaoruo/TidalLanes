"""Clean directed-vs-symmetric diagnostic for QSM place and cost shocks.

This runner avoids tidal lanes and lane reallocation. It asks whether a
researcher who starts from reciprocal-symmetric commuting costs obtains
different counterfactual predictions for standard QSM exercises:

* local productivity and amenity shocks at selected places;
* bilateral commuting-cost reductions in selected OD markets.

Both directed and symmetric worlds are fully re-estimated from their own input
cost surfaces before counterfactuals are solved. Treatment sets are common
physical nodes or OD pairs, selected from the directed-world diagnostics.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

DATA_WORK = Path(__file__).resolve().parents[3]
if str(DATA_WORK) not in sys.path:
    sys.path.insert(0, str(DATA_WORK))

from src.model.directional_cost import fixed_tau_model as ft  # noqa: E402
from src.model.directional_cost.policy_counterfactuals import (  # noqa: E402
    _grid_xy,
    employment_hub_mask,
    outer_ring_mask,
)
from src.model.full_estimation import stage2_counterfactuals as s2  # noqa: E402
from src.model.full_estimation.equilibrium_solve import (  # noqa: E402
    EquilibriumResult,
    _market_access,
    _solve_lR_lF_given_tau,
    solve_equilibrium,
)
from src.model.full_estimation.phase_metrics_common import data_work_root  # noqa: E402
from src.model.full_estimation.run_symmetric_input_reestimated_suite_gate5 import (  # noqa: E402
    DEFAULT_DIRECTED_VERSION,
    DEFAULT_SYMMETRIC_VERSION,
    WorldState,
    _build_world,
    _load_params,
    baseline_distribution_row,
    build_symmetric_cost_bundle,
    compare_baselines,
    write_baseline_distribution,
)

LOG = logging.getLogger("symmetric_input_place_cost_suite")
EPS = 1e-12

DEFAULT_OUTPUT = "symmetric_input_place_cost_suite_gate5_bpr_pos05_v1"


@dataclass(frozen=True)
class PlaceTreatment:
    scenario: str
    family: str
    mask: np.ndarray
    shock_kind: str
    multiplier: float
    selection_rule: str
    solver_scope: str


@dataclass(frozen=True)
class OdTreatment:
    scenario: str
    family: str
    mask: np.ndarray
    cost_scale: float
    selection_rule: str


def _tau_ij_min(eq: EquilibriumResult, theta: float) -> np.ndarray:
    return np.clip(eq.tau_ij_inv_theta, EPS, None) ** (-1.0 / theta)


def _bilateral_asymmetry_gap(eq: EquilibriumResult, theta: float) -> np.ndarray:
    tau = _tau_ij_min(eq, theta)
    rev = tau.T
    gap = np.abs(np.log(np.clip(tau, EPS, None) / np.clip(rev, EPS, None)))
    np.fill_diagonal(gap, 0.0)
    return gap


def _top_positive_mask(score: np.ndarray, valid: np.ndarray, *, top_share: float) -> np.ndarray:
    score = np.asarray(score, dtype=float)
    valid = np.asarray(valid, dtype=bool) & np.isfinite(score)
    idx = np.flatnonzero(valid)
    out = np.zeros(score.shape, dtype=bool)
    if idx.size == 0:
        return out
    k = max(1, int(np.ceil(top_share * idx.size)))
    flat_score = score.ravel()
    order = idx[np.argsort(flat_score[idx])[::-1]]
    out.ravel()[order[:k]] = True
    return out


def _outer_residence_top_mask(world: WorldState, *, top_share: float) -> np.ndarray:
    x, y = _grid_xy(world.prim)
    w = np.clip(world.prim.L_F_obs, EPS, None)
    cx = float(np.average(x, weights=w))
    cy = float(np.average(y, weights=w))
    dist = np.hypot(x - cx, y - cy)
    score = dist * np.clip(world.prim.L_R_obs, 0.0, None)
    return _top_positive_mask(score, world.prim.L_R_obs > 0, top_share=top_share)


def _node_asymmetry_exposures(world: WorldState) -> tuple[np.ndarray, np.ndarray]:
    """Observed-flow weighted outbound and inbound reciprocal cost asymmetry."""
    flow = np.asarray(world.prim.L_ij_obs, dtype=float)
    gap = _bilateral_asymmetry_gap(world.baseline, world.params.theta)
    outbound_num = np.sum(flow * gap, axis=1)
    inbound_num = np.sum(flow * gap, axis=0)
    outbound = outbound_num / np.clip(flow.sum(axis=1), EPS, None)
    inbound = inbound_num / np.clip(flow.sum(axis=0), EPS, None)
    outbound = np.where(flow.sum(axis=1) > EPS, outbound, 0.0)
    inbound = np.where(flow.sum(axis=0) > EPS, inbound, 0.0)
    return outbound, inbound


def build_place_treatments(
    directed: WorldState,
    *,
    top_node_share: float,
    multiplier: float,
) -> list[PlaceTreatment]:
    outbound_asym, inbound_asym = _node_asymmetry_exposures(directed)
    prim = directed.prim

    hub_mask = _top_positive_mask(prim.L_F_obs, prim.L_F_obs > 0, top_share=top_node_share)
    asym_job_score = prim.L_F_obs * inbound_asym
    high_asym_jobs = _top_positive_mask(asym_job_score, prim.L_F_obs > 0, top_share=top_node_share)

    outer_res = _outer_residence_top_mask(directed, top_share=top_node_share)
    asym_res_score = prim.L_R_obs * outbound_asym
    high_asym_res = _top_positive_mask(asym_res_score, prim.L_R_obs > 0, top_share=top_node_share)

    base_specs = [
        ("hub_productivity_x1p10", "productivity", hub_mask, "top workplace nodes by observed jobs"),
        (
            "high_asym_job_productivity_x1p10",
            "productivity",
            high_asym_jobs,
            "top workplace nodes by inbound observed-flow asymmetry exposure",
        ),
        (
            "outer_residence_amenity_x1p10",
            "amenity",
            outer_res,
            "top residence nodes by distance from the employment-weighted centroid",
        ),
        (
            "high_asym_residence_amenity_x1p10",
            "amenity",
            high_asym_res,
            "top residence nodes by outbound observed-flow asymmetry exposure",
        ),
    ]

    out: list[PlaceTreatment] = []
    for scenario, kind, mask, rule in base_specs:
        for solver_scope in ("fixed_costs", "endogenous_congestion"):
            out.append(
                PlaceTreatment(
                    scenario=f"{scenario}_{solver_scope}",
                    family="place_based_fundamental",
                    mask=mask.copy(),
                    shock_kind=kind,
                    multiplier=multiplier,
                    selection_rule=rule,
                    solver_scope=solver_scope,
                )
            )
    return out


def build_od_treatments(
    directed: WorldState,
    *,
    top_od_share: float,
    cost_scale: float,
) -> list[OdTreatment]:
    prim = directed.prim
    flow = np.asarray(prim.L_ij_obs, dtype=float)
    n = prim.n_nodes
    off_diag = ~np.eye(n, dtype=bool)
    positive_flow = (flow > EPS) & off_diag
    gap = _bilateral_asymmetry_gap(directed.baseline, directed.params.theta)

    uniform = off_diag.copy()
    top_flow = _top_positive_mask(flow, positive_flow, top_share=top_od_share)
    top_asym_flow = _top_positive_mask(flow * gap, positive_flow, top_share=top_od_share)

    hub_dest = employment_hub_mask(prim, quantile=0.90)
    outer_orig = outer_ring_mask(prim, quantile=0.75)
    outer_to_hub = outer_orig[:, None] & hub_dest[None, :] & off_diag

    return [
        OdTreatment(
            scenario="uniform_bilateral_cost_x0p90",
            family="bilateral_travel_cost",
            mask=uniform,
            cost_scale=cost_scale,
            selection_rule="all off-diagonal OD markets",
        ),
        OdTreatment(
            scenario="top_flow_bilateral_cost_x0p90",
            family="bilateral_travel_cost",
            mask=top_flow,
            cost_scale=cost_scale,
            selection_rule="top observed-flow OD markets",
        ),
        OdTreatment(
            scenario="high_asym_flow_bilateral_cost_x0p90",
            family="bilateral_travel_cost",
            mask=top_asym_flow,
            cost_scale=cost_scale,
            selection_rule="top observed-flow times reciprocal-asymmetry OD markets",
        ),
        OdTreatment(
            scenario="outer_to_hub_bilateral_cost_x0p90",
            family="bilateral_travel_cost",
            mask=outer_to_hub,
            cost_scale=cost_scale,
            selection_rule="outer-ring residence origins to top job-center destinations",
        ),
    ]


def _solve_fixed_tau_inv(
    world: WorldState,
    *,
    tau_inv_theta: np.ndarray,
    ubar_theta: np.ndarray,
    abar_theta: np.ndarray,
    label: str,
) -> ft.StaticEquilibriumResult:
    l_R, l_F, it = _solve_lR_lF_given_tau(
        tau_inv_theta,
        ubar_theta,
        abar_theta,
        world.params,
        chi=world.fund.chi,
        max_iter=800,
        tol=1e-9,
    )
    Pi, Pj = _market_access(tau_inv_theta, l_R, l_F, world.prim.Lbar, world.params.theta)
    Wbar = ft.welfare(
        ubar_theta,
        abar_theta,
        tau_inv_theta,
        l_R,
        l_F,
        world.prim.Lbar,
        world.params,
    )
    return ft.StaticEquilibriumResult(
        label=label,
        tau_min_kl=world.baseline.tau_min_kl.copy(),
        tau_ij_inv_theta=np.asarray(tau_inv_theta, dtype=float),
        l_R=l_R,
        l_F=l_F,
        Pi_i=Pi,
        P_j=Pj,
        Wbar=float(Wbar),
        n_iter=int(it),
        converged=True,
        max_log_change=0.0,
        kappa=float(world.baseline.kappa),
    )


def _augment_effect_row(
    row: dict,
    world: WorldState,
    *,
    scenario: str,
    family: str,
    solver_scope: str,
    selection_rule: str,
    shock_kind: str,
    treatment_flow_share_pct: float | None = None,
    treatment_node_share_pct: float | None = None,
    treatment_LR_share_pct: float | None = None,
    treatment_LF_share_pct: float | None = None,
    cost_scale: float | None = None,
    multiplier: float | None = None,
) -> dict:
    base_cost = float(row.get("base_od_weighted_bilateral_cost_min", np.nan))
    scen_cost = float(row.get("scenario_od_weighted_bilateral_cost_min", np.nan))
    row.update({
        "world": world.name,
        "version_id": world.version_id,
        "scenario": scenario,
        "family": family,
        "solver_scope": solver_scope,
        "selection_rule": selection_rule,
        "shock_kind": shock_kind,
        "treatment_flow_share_pct": treatment_flow_share_pct,
        "treatment_node_share_pct": treatment_node_share_pct,
        "treatment_LR_share_pct": treatment_LR_share_pct,
        "treatment_LF_share_pct": treatment_LF_share_pct,
        "cost_scale": cost_scale,
        "fundamental_multiplier": multiplier,
        "od_cost_change_min_per_commuter": scen_cost - base_cost,
        "od_cost_change_sec_per_commuter": 60.0 * (scen_cost - base_cost),
        "od_cost_change_person_min_day": (scen_cost - base_cost) * world.prim.Lbar,
    })
    return row


def _add_od_cost_decomposition(
    row: dict,
    base: ft.StaticEquilibriumResult,
    scenario: ft.StaticEquilibriumResult,
    world: WorldState,
    *,
    abar_theta_base: np.ndarray,
    abar_theta_scenario: np.ndarray,
) -> dict:
    """Decompose average OD-cost changes into direct and sorting terms."""
    od_base = ft.predicted_od_shares(base, abar_theta_base, world.params)
    od_scen = ft.predicted_od_shares(scenario, abar_theta_scenario, world.params)
    tau_base = np.clip(base.tau_ij_inv_theta, EPS, None) ** (-1.0 / world.params.theta)
    tau_scen = np.clip(scenario.tau_ij_inv_theta, EPS, None) ** (-1.0 / world.params.theta)
    direct = float(np.sum(od_base * (tau_scen - tau_base)) / np.clip(od_base.sum(), EPS, None))
    sorting = float(np.sum((od_scen - od_base) * tau_scen) / np.clip(od_base.sum(), EPS, None))
    total = float(np.sum(od_scen * tau_scen) - np.sum(od_base * tau_base))
    row.update({
        "od_cost_direct_min_baseline_od": direct,
        "od_cost_direct_sec_baseline_od": 60.0 * direct,
        "od_cost_sorting_min": sorting,
        "od_cost_sorting_sec": 60.0 * sorting,
        "od_cost_decomp_total_sec": 60.0 * total,
        "od_cost_decomp_error_sec": 60.0 * (total - direct - sorting),
    })
    return row


def _node_treatment_summary(world: WorldState, spec: PlaceTreatment) -> dict:
    mask = np.asarray(spec.mask, dtype=bool)
    outbound_asym, inbound_asym = _node_asymmetry_exposures(world)
    asym = inbound_asym if spec.shock_kind == "productivity" else outbound_asym
    weights = world.prim.L_F_obs if spec.shock_kind == "productivity" else world.prim.L_R_obs
    return {
        "scenario": spec.scenario,
        "family": spec.family,
        "shock_kind": spec.shock_kind,
        "selection_rule": spec.selection_rule,
        "solver_scope": spec.solver_scope,
        "n_treated_nodes": int(mask.sum()),
        "treated_node_share_pct": 100.0 * float(mask.mean()),
        "treated_LR_share_pct": 100.0 * float(world.prim.L_R_obs[mask].sum() / np.clip(world.prim.L_R_obs.sum(), EPS, None)),
        "treated_LF_share_pct": 100.0 * float(world.prim.L_F_obs[mask].sum() / np.clip(world.prim.L_F_obs.sum(), EPS, None)),
        "treated_weighted_mean_abs_log_asym": float(
            np.average(asym[mask], weights=np.clip(weights[mask], EPS, None))
        ) if mask.any() else np.nan,
    }


def _od_treatment_summary(world: WorldState, spec: OdTreatment) -> dict:
    mask = np.asarray(spec.mask, dtype=bool)
    flow = np.asarray(world.prim.L_ij_obs, dtype=float)
    gap = _bilateral_asymmetry_gap(world.baseline, world.params.theta)
    positive = flow > EPS
    selected_flow = float(flow[mask].sum())
    return {
        "scenario": spec.scenario,
        "family": spec.family,
        "selection_rule": spec.selection_rule,
        "n_treated_od_cells": int(mask.sum()),
        "n_treated_positive_flow_od_cells": int((mask & positive).sum()),
        "treated_flow_share_pct": 100.0 * selected_flow / np.clip(float(flow.sum()), EPS, None),
        "treated_weighted_mean_abs_log_asym": float(
            np.average(gap[mask & positive], weights=np.clip(flow[mask & positive], EPS, None))
        ) if (mask & positive).any() else np.nan,
        "cost_scale": float(spec.cost_scale),
    }


def run_place_treatment(world: WorldState, spec: PlaceTreatment) -> dict:
    ubar = world.fund.ubar_theta.copy()
    abar = world.fund.abar_theta.copy()
    if spec.shock_kind == "productivity":
        abar[spec.mask] *= spec.multiplier
    elif spec.shock_kind == "amenity":
        ubar[spec.mask] *= spec.multiplier
    else:
        raise ValueError(f"unknown shock_kind: {spec.shock_kind}")

    summ = _node_treatment_summary(world, spec)
    if spec.solver_scope == "endogenous_congestion":
        eq = solve_equilibrium(
            world.prim,
            ubar,
            abar,
            world.params,
            tau_min_init=world.prim.tau_obs_min.copy(),
            endogenous_congestion=True,
            label=spec.scenario,
            **world.skw,
        )
        row = s2.summarise_vs_baseline(
            world.baseline,
            eq,
            world.prim,
            world.params,
            world.fund.abar_theta,
            abar_theta_scenario=abar,
        )
        row = _add_od_cost_decomposition(
            row,
            s2._to_static(world.baseline),
            s2._to_static(eq),
            world,
            abar_theta_base=world.fund.abar_theta,
            abar_theta_scenario=abar,
        )
    elif spec.solver_scope == "fixed_costs":
        base = s2._to_static(world.baseline)
        eq = _solve_fixed_tau_inv(
            world,
            tau_inv_theta=world.baseline.tau_ij_inv_theta,
            ubar_theta=ubar,
            abar_theta=abar,
            label=spec.scenario,
        )
        row = ft.comparison_summary(
            base,
            eq,
            world.prim,
            world.params,
            abar_theta=world.fund.abar_theta,
            abar_theta_scenario=abar,
        )
        row = _add_od_cost_decomposition(
            row,
            base,
            eq,
            world,
            abar_theta_base=world.fund.abar_theta,
            abar_theta_scenario=abar,
        )
    else:
        raise ValueError(f"unknown solver_scope: {spec.solver_scope}")

    return _augment_effect_row(
        row,
        world,
        scenario=spec.scenario,
        family=spec.family,
        solver_scope=spec.solver_scope,
        selection_rule=spec.selection_rule,
        shock_kind=spec.shock_kind,
        treatment_node_share_pct=summ["treated_node_share_pct"],
        treatment_LR_share_pct=summ["treated_LR_share_pct"],
        treatment_LF_share_pct=summ["treated_LF_share_pct"],
        multiplier=spec.multiplier,
    )


def run_od_treatment(world: WorldState, spec: OdTreatment) -> dict:
    tau_inv = np.asarray(world.baseline.tau_ij_inv_theta, dtype=float).copy()
    tau_inv[spec.mask] *= float(spec.cost_scale) ** (-world.params.theta)
    base = s2._to_static(world.baseline)
    eq = _solve_fixed_tau_inv(
        world,
        tau_inv_theta=tau_inv,
        ubar_theta=world.fund.ubar_theta,
        abar_theta=world.fund.abar_theta,
        label=spec.scenario,
    )
    row = ft.comparison_summary(
        base,
        eq,
        world.prim,
        world.params,
        abar_theta=world.fund.abar_theta,
        abar_theta_scenario=world.fund.abar_theta,
    )
    row = _add_od_cost_decomposition(
        row,
        base,
        eq,
        world,
        abar_theta_base=world.fund.abar_theta,
        abar_theta_scenario=world.fund.abar_theta,
    )
    summ = _od_treatment_summary(world, spec)
    return _augment_effect_row(
        row,
        world,
        scenario=spec.scenario,
        family=spec.family,
        solver_scope="fixed_bilateral_costs",
        selection_rule=spec.selection_rule,
        shock_kind="bilateral_tau",
        treatment_flow_share_pct=summ["treated_flow_share_pct"],
        cost_scale=spec.cost_scale,
    )


def over_under_effects(effect_df: pd.DataFrame) -> pd.DataFrame:
    metrics = [
        "Wbar_pct_change",
        "od_weighted_bilateral_cost_pct_change",
        "od_cost_change_sec_per_commuter",
        "od_cost_change_person_min_day",
        "od_cost_direct_sec_baseline_od",
        "od_cost_sorting_sec",
        "od_cost_decomp_total_sec",
        "residence_dissimilarity_pct",
        "workplace_dissimilarity_pct",
        "od_dissimilarity_pct",
        "p95_abs_log_Pi_change",
        "mean_Xi_pct_change",
        "mean_edge_tau_pct_change",
    ]
    id_cols = [
        "scenario", "family", "solver_scope", "selection_rule", "shock_kind",
        "treatment_flow_share_pct", "treatment_node_share_pct",
        "treatment_LR_share_pct", "treatment_LF_share_pct",
        "cost_scale", "fundamental_multiplier",
    ]
    rows: list[dict] = []
    for scenario, grp in effect_df.groupby("scenario", sort=False):
        if {"directed_main", "symmetric_reestimated"} - set(grp["world"]):
            continue
        drow = grp[grp["world"] == "directed_main"].iloc[0]
        srow = grp[grp["world"] == "symmetric_reestimated"].iloc[0]
        row = {
            "scenario": scenario,
            "bias_definition": "symmetric_reestimated_effect_minus_directed_main_effect",
        }
        for c in id_cols:
            if c in drow:
                row[c] = drow[c]
        for m in metrics:
            if m in drow and m in srow and pd.notna(drow[m]) and pd.notna(srow[m]):
                row[f"directed_{m}"] = float(drow[m])
                row[f"symmetric_{m}"] = float(srow[m])
                row[f"bias_{m}"] = float(srow[m]) - float(drow[m])
                row[f"abs_bias_{m}"] = abs(row[f"bias_{m}"])
        rows.append(row)
    return pd.DataFrame(rows)


def discussion_digest(effect_df: pd.DataFrame, bias_df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "scenario", "family", "solver_scope",
        "directed_Wbar_pct_change", "symmetric_Wbar_pct_change", "bias_Wbar_pct_change",
        "directed_od_cost_change_sec_per_commuter",
        "symmetric_od_cost_change_sec_per_commuter",
        "bias_od_cost_change_sec_per_commuter",
        "directed_od_cost_direct_sec_baseline_od",
        "symmetric_od_cost_direct_sec_baseline_od",
        "bias_od_cost_direct_sec_baseline_od",
        "directed_od_cost_sorting_sec",
        "symmetric_od_cost_sorting_sec",
        "bias_od_cost_sorting_sec",
        "directed_workplace_dissimilarity_pct",
        "symmetric_workplace_dissimilarity_pct",
        "bias_workplace_dissimilarity_pct",
        "directed_od_dissimilarity_pct",
        "symmetric_od_dissimilarity_pct",
        "bias_od_dissimilarity_pct",
    ]
    present = [c for c in cols if c in bias_df.columns]
    sort_cols = [c for c in ("family", "solver_scope", "abs_bias_od_dissimilarity_pct") if c in bias_df.columns]
    ordered = bias_df.sort_values(
        sort_cols,
        ascending=[True, True, False][: len(sort_cols)],
        kind="stable",
    ) if sort_cols else bias_df
    out = ordered[present].copy()
    return out


def run_suite(
    data_work: Path,
    *,
    directed_version_id: str,
    symmetric_version_id: str,
    output_name: str,
    sym_method: str,
    top_node_share: float,
    top_od_share: float,
    place_multiplier: float,
    cost_scale: float,
    force_bundle: bool,
) -> None:
    out = data_work / "outputs" / output_name
    out.mkdir(parents=True, exist_ok=True)

    build_symmetric_cost_bundle(
        data_work,
        directed_version_id=directed_version_id,
        symmetric_version_id=symmetric_version_id,
        sym_method=sym_method,
        force=force_bundle,
    )

    params = _load_params()
    params.write(out / "parameters.json")

    directed = _build_world(data_work, name="directed_main", version_id=directed_version_id, params=params)
    symmetric = _build_world(data_work, name="symmetric_reestimated", version_id=symmetric_version_id, params=params)
    worlds = [directed, symmetric]

    pd.DataFrame([baseline_distribution_row(w) for w in worlds]).to_csv(
        out / "baseline_summary_by_world.csv", index=False,
    )
    (out / "baseline_comparison.json").write_text(
        json.dumps(compare_baselines(directed, symmetric), indent=2, default=float),
    )
    for w in worlds:
        write_baseline_distribution(w, out)

    place_specs = build_place_treatments(
        directed, top_node_share=top_node_share, multiplier=place_multiplier,
    )
    od_specs = build_od_treatments(
        directed, top_od_share=top_od_share, cost_scale=cost_scale,
    )

    treatment_rows = []
    treatment_rows.extend(_node_treatment_summary(directed, s) for s in place_specs)
    treatment_rows.extend(_od_treatment_summary(directed, s) for s in od_specs)
    pd.DataFrame(treatment_rows).to_csv(out / "treatment_sets.csv", index=False)

    effect_rows: list[dict] = []
    for world in worlds:
        LOG.info("Running place treatments in %s", world.name)
        for spec in place_specs:
            effect_rows.append(run_place_treatment(world, spec))
        LOG.info("Running bilateral travel-cost treatments in %s", world.name)
        for spec in od_specs:
            effect_rows.append(run_od_treatment(world, spec))

    effect_df = pd.DataFrame(effect_rows)
    effect_df.to_csv(out / "place_cost_effects_by_world.csv", index=False)

    bias_df = over_under_effects(effect_df)
    bias_df.to_csv(out / "over_under_place_cost.csv", index=False)
    discussion_digest(effect_df, bias_df).to_csv(out / "place_cost_discussion_digest.csv", index=False)

    meta = {
        "output_name": output_name,
        "directed_version_id": directed_version_id,
        "symmetric_version_id": symmetric_version_id,
        "sym_method": sym_method,
        "top_node_share": top_node_share,
        "top_od_share": top_od_share,
        "place_multiplier": place_multiplier,
        "cost_scale": cost_scale,
        "bias_sign": "symmetric_reestimated_effect_minus_directed_main_effect",
        "design": {
            "full_re_estimation": True,
            "common_physical_treatments": True,
            "no_tidal_lanes": True,
            "no_lane_reallocation": True,
            "place_shocks": "productivity and amenity fundamentals on common node sets",
            "travel_cost_shocks": "direct bilateral tau_ij reductions on common OD sets",
        },
    }
    (out / "suite_meta.json").write_text(json.dumps(meta, indent=2, default=float))
    LOG.info("Wrote suite to %s", out)


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo-root", default=str(DATA_WORK))
    p.add_argument("--directed-version-id", default=DEFAULT_DIRECTED_VERSION)
    p.add_argument("--symmetric-version-id", default=DEFAULT_SYMMETRIC_VERSION)
    p.add_argument("--output-name", default=DEFAULT_OUTPUT)
    p.add_argument("--sym-method", choices=("geometric", "arithmetic"), default="geometric")
    p.add_argument("--top-node-share", type=float, default=0.10)
    p.add_argument("--top-od-share", type=float, default=0.01)
    p.add_argument("--place-multiplier", type=float, default=1.10)
    p.add_argument("--cost-scale", type=float, default=0.90)
    p.add_argument("--force-bundle", action="store_true")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s | %(message)s")
    data_work = data_work_root(args.repo_root)
    run_suite(
        data_work,
        directed_version_id=args.directed_version_id,
        symmetric_version_id=args.symmetric_version_id,
        output_name=args.output_name,
        sym_method=args.sym_method,
        top_node_share=args.top_node_share,
        top_od_share=args.top_od_share,
        place_multiplier=args.place_multiplier,
        cost_scale=args.cost_scale,
        force_bundle=args.force_bundle,
    )

    out = data_work / "outputs" / args.output_name
    print("\n=== baseline_summary_by_world ===")
    print(pd.read_csv(out / "baseline_summary_by_world.csv").to_string(index=False))
    print("\n=== discussion digest ===")
    print(pd.read_csv(out / "place_cost_discussion_digest.csv").to_string(index=False))


if __name__ == "__main__":
    main()
