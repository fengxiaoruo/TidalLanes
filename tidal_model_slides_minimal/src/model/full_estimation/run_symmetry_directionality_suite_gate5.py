"""Symmetry-to-directionality diagnostics for gate5 bpr+pos05 mainline.

This runner keeps primitives, parameters, fundamentals, welfare formula, and
solver tolerances fixed. It varies only the travel-cost surface:

  directed  = observed directed AM edge costs;
  symmetric = reciprocal-pair symmetric observed and free-flow edge costs.

For each cost surface it runs:
  - exogenous fixed-cost equilibrium;
  - endogenous anchored-congestion equilibrium;
  - place shocks under both modes;
  - transport policies under endogenous mode.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass, replace
from pathlib import Path

import numpy as np
import pandas as pd

DATA_WORK = Path(__file__).resolve().parents[3]
if str(DATA_WORK) not in sys.path:
    sys.path.insert(0, str(DATA_WORK))

from src.model.directional_cost.policy_counterfactuals import (  # noqa: E402
    PolicySpec,
    apply_policy_abar,
    apply_policy_ubar,
    employment_hub_mask,
    outer_ring_mask,
)
from src.model.full_estimation import build_primitives as bp  # noqa: E402
from src.model.full_estimation import invert_fundamentals as inv  # noqa: E402
from src.model.full_estimation import parameters as pm  # noqa: E402
from src.model.full_estimation import stage2_counterfactuals as s2  # noqa: E402
from src.model.full_estimation.counterfactual_levels import (  # noqa: E402
    apply_bidirectional_lane_delta,
    select_tidal_lane_pairs_by_qdelay,
    select_tidal_lane_pairs_by_rank,
    uniform_expansion_factor_for_lane_budget,
)
from src.model.full_estimation.equilibrium_solve import (  # noqa: E402
    MAINLINE_ALPHA,
    MAINLINE_BETA,
    MAINLINE_LAMBDA_PHYS,
    MAINLINE_PAIR_SELECTION,
    _resolve_lanes,
    solve_equilibrium,
)
from src.model.full_estimation.phase_metrics_common import data_work_root, grid_xy  # noqa: E402
from src.model.full_estimation.run_alpha_robustness_quick_gate5 import (  # noqa: E402
    solver_overrides_for_params,
)


LOG = logging.getLogger("symmetry_directionality_suite")
EPS = 1e-12


@dataclass
class World:
    name: str
    mode: str
    tau_obs_anchor: np.ndarray
    tau_ff: np.ndarray
    kwargs: dict
    baseline: object


def symmetric_edge_values(prim: bp.Primitives, values: np.ndarray, method: str = "geometric") -> np.ndarray:
    """Symmetrize reciprocal directed edge values while preserving one-way edges."""
    out = np.asarray(values, dtype=float).copy()
    pair = {(int(i), int(j)): k for k, (i, j) in enumerate(zip(prim.edge_i, prim.edge_j))}
    seen: set[tuple[int, int]] = set()
    for k, (i, j) in enumerate(zip(prim.edge_i, prim.edge_j)):
        rev = pair.get((int(j), int(i)))
        if rev is None:
            continue
        key = tuple(sorted((int(k), int(rev))))
        if key in seen:
            continue
        seen.add(key)
        a = float(values[k])
        b = float(values[rev])
        if method == "geometric":
            v = float(np.sqrt(max(a, EPS) * max(b, EPS)))
        elif method == "arithmetic":
            v = 0.5 * (a + b)
        else:
            raise ValueError(f"unknown symmetric method: {method}")
        out[k] = v
        out[rev] = v
    return out


def centroid_gap(prim: bp.Primitives, l_R: np.ndarray, l_F: np.ndarray) -> float:
    x, y = grid_xy(prim)
    wr = np.clip(l_R, EPS, None)
    wf = np.clip(l_F, EPS, None)
    rx, ry = float(np.average(x, weights=wr)), float(np.average(y, weights=wr))
    fx, fy = float(np.average(x, weights=wf)), float(np.average(y, weights=wf))
    return float(np.hypot(rx - fx, ry - fy))


def sorting_stats(prim: bp.Primitives, base, scen, *, sel=None) -> dict:
    lr0 = np.clip(base.l_R, EPS, None)
    lf0 = np.clip(base.l_F, EPS, None)
    lr1 = np.clip(scen.l_R, EPS, None)
    lf1 = np.clip(scen.l_F, EPS, None)
    dlr = np.log(lr1) - np.log(lr0)
    dlf = np.log(lf1) - np.log(lf0)
    hub = employment_hub_mask(prim)
    outer = outer_ring_mask(prim)
    out = {
        "centroid_gap_base": centroid_gap(prim, base.l_R, base.l_F),
        "centroid_gap_scenario": centroid_gap(prim, scen.l_R, scen.l_F),
        "centroid_gap_change": centroid_gap(prim, scen.l_R, scen.l_F) - centroid_gap(prim, base.l_R, base.l_F),
        "mean_log_lR_change_hub": float(np.mean(dlr[hub])) if hub.any() else np.nan,
        "mean_log_lF_change_hub": float(np.mean(dlf[hub])) if hub.any() else np.nan,
        "mean_log_lR_change_outer": float(np.mean(dlr[outer])) if outer.any() else np.nan,
        "mean_log_lF_change_outer": float(np.mean(dlf[outer])) if outer.any() else np.nan,
        "max_abs_log_lR_change": float(np.max(np.abs(dlr))),
        "max_abs_log_lF_change": float(np.max(np.abs(dlf))),
    }
    if sel is not None and len(sel.treated_pairs):
        slow_nodes: set[int] = set()
        for slow, _fast in sel.treated_pairs:
            slow_nodes.update([int(prim.edge_i[slow]), int(prim.edge_j[slow])])
        idx = np.asarray(sorted(slow_nodes), dtype=int)
        out.update({
            "n_slow_endpoint_nodes": int(len(idx)),
            "mean_log_lR_change_slow_endpoints": float(np.mean(dlr[idx])) if len(idx) else np.nan,
            "mean_log_lF_change_slow_endpoints": float(np.mean(dlf[idx])) if len(idx) else np.nan,
            "sum_delta_lR_slow_endpoints": float(np.sum(scen.l_R[idx] - base.l_R[idx])) if len(idx) else np.nan,
            "sum_delta_lF_slow_endpoints": float(np.sum(scen.l_F[idx] - base.l_F[idx])) if len(idx) else np.nan,
        })
    return out


def compare_row(prim, params, base, scen, *, world: World, scenario: str, family: str, abar_base, abar_scen=None, sel=None) -> dict:
    row = s2.summarise_vs_baseline(
        base, scen, prim, params, abar_base,
        abar_theta_scenario=abar_scen if abar_scen is not None else abar_base,
    )
    row.update({
        "world": world.name,
        "mode": world.mode,
        "scenario": scenario,
        "family": family,
    })
    row.update(sorting_stats(prim, base, scen, sel=sel))
    return row


def make_worlds(prim, fund, params, *, sym_method: str, congestion_lambda: float, solver_kw: dict) -> dict[tuple[str, str], World]:
    tau_obs_dir = prim.tau_obs_min.copy()
    tau_ff_dir = prim.tau_ff_min.copy()
    tau_obs_sym = symmetric_edge_values(prim, prim.tau_obs_min, method=sym_method)
    tau_ff_sym = symmetric_edge_values(prim, prim.tau_ff_min, method=sym_method)

    def base_kwargs(tau_obs_anchor, tau_ff):
        anchor = solve_equilibrium(
            prim, fund.ubar_theta, fund.abar_theta, params,
            tau_ff_override=tau_ff,
            tau_min_init=tau_obs_anchor,
            endogenous_congestion=False,
            chi=fund.chi,
            kappa=fund.kappa,
            label="world_anchor",
        )
        kw = dict(
            chi=fund.chi,
            kappa=fund.kappa,
            max_outer=solver_kw["max_outer"],
            tol=solver_kw["tol"],
            damping=solver_kw.get("damping", 0.25),
            congestion_form="anchored",
            congestion_lambda=congestion_lambda,
            xi_obs_kl=anchor.Xi_kl,
            n_obs_kl=_resolve_lanes(prim.n_kl),
            tau_obs_anchor=tau_obs_anchor,
        )
        return kw

    worlds: dict[tuple[str, str], World] = {}
    for name, tau_obs, tau_ff in (
        ("directed", tau_obs_dir, tau_ff_dir),
        ("symmetric", tau_obs_sym, tau_ff_sym),
    ):
        kw = base_kwargs(tau_obs, tau_ff)
        exog = solve_equilibrium(
            prim, fund.ubar_theta, fund.abar_theta, params,
            tau_ff_override=tau_ff,
            tau_min_init=tau_obs,
            endogenous_congestion=False,
            label=f"{name}_exog_baseline",
            **kw,
        )
        endog = solve_equilibrium(
            prim, fund.ubar_theta, fund.abar_theta, params,
            tau_ff_override=tau_ff,
            tau_min_init=tau_obs,
            endogenous_congestion=True,
            label=f"{name}_endog_baseline",
            **kw,
        )
        worlds[(name, "exogenous_tau")] = World(name, "exogenous_tau", tau_obs, tau_ff, kw, exog)
        worlds[(name, "endogenous")] = World(name, "endogenous", tau_obs, tau_ff, kw, endog)
    return worlds


def solve_in_world(prim, fund, params, world: World, *, ubar, abar, n_kl=None, tau_ff=None, endogenous=None, label: str):
    mode_endog = world.mode == "endogenous" if endogenous is None else bool(endogenous)
    return solve_equilibrium(
        prim, ubar, abar, params,
        n_kl_override=n_kl,
        tau_ff_override=world.tau_ff if tau_ff is None else tau_ff,
        tau_min_init=world.tau_obs_anchor,
        endogenous_congestion=mode_endog,
        label=label,
        **world.kwargs,
    )


def run_suite(data_work: Path, *, version_id: str, output_name: str, sym_method: str, top_k: int) -> pd.DataFrame:
    out = data_work / "outputs" / output_name
    out.mkdir(parents=True, exist_ok=True)
    prim = bp.load_primitives(data_work / "outputs" / version_id, period="AM")
    params = replace(pm.Parameters(), alpha=MAINLINE_ALPHA, beta=MAINLINE_BETA)
    fund = inv.invert_fundamentals(prim, params, tol=1.5e-4, max_iter=1800, kappa=1.0)
    solver_kw = solver_overrides_for_params(params)
    worlds = make_worlds(
        prim, fund, params,
        sym_method=sym_method,
        congestion_lambda=MAINLINE_LAMBDA_PHYS,
        solver_kw=solver_kw,
    )

    rows: list[dict] = []

    # Symmetry level comparison: symmetric baseline relative to directed baseline within mode.
    for mode in ("exogenous_tau", "endogenous"):
        directed = worlds[("directed", mode)]
        symmetric = worlds[("symmetric", mode)]
        rows.append(compare_row(
            prim, params, directed.baseline, symmetric.baseline,
            world=symmetric, scenario="symmetric_cost_surface", family="cost_surface",
            abar_base=fund.abar_theta,
        ))

    place_specs = [
        PolicySpec(name="hub_productivity_10pct", abar_mult_on_hubs=1.10),
        PolicySpec(name="outer_ring_amenity_10pct", ubar_mult_on_outer_ring=1.10),
    ]
    for world in worlds.values():
        for spec in place_specs:
            ubar = apply_policy_ubar(fund.ubar_theta, prim, spec)
            abar = apply_policy_abar(fund.abar_theta, prim, spec)
            eq = solve_in_world(
                prim, fund, params, world,
                ubar=ubar, abar=abar,
                label=f"{world.name}_{world.mode}_{spec.name}",
            )
            rows.append(compare_row(
                prim, params, world.baseline, eq,
                world=world, scenario=spec.name, family="place_shock",
                abar_base=fund.abar_theta, abar_scen=abar,
            ))

    # Transport policies: only meaningful in endogenous mode because lanes enter congestion technology.
    for world in (worlds[("directed", "endogenous")], worlds[("symmetric", "endogenous")]):
        tidal_sel = select_tidal_lane_pairs_by_rank(
            prim, top_k=top_k, delta=1.0, rank=MAINLINE_PAIR_SELECTION, xi_kl=world.baseline.Xi_kl,
        )
        n_tidal = prim.n_kl.copy()
        for slow, fast in tidal_sel.treated_pairs:
            n_tidal[slow] += 1.0
            n_tidal[fast] -= 1.0
        eq = solve_in_world(
            prim, fund, params, world,
            ubar=fund.ubar_theta, abar=fund.abar_theta,
            n_kl=n_tidal, label=f"{world.name}_tidal_top{top_k}",
        )
        rows.append(compare_row(
            prim, params, world.baseline, eq,
            world=world, scenario=f"tidal_top{top_k}", family="transport_policy",
            abar_base=fund.abar_theta, sel=tidal_sel,
        ))

        n_same = apply_bidirectional_lane_delta(prim, tidal_sel, delta=1.0)
        p2_lane_km = float(np.sum((n_same - prim.n_kl) * prim.ell_kl_km))
        eq = solve_in_world(
            prim, fund, params, world,
            ubar=fund.ubar_theta, abar=fund.abar_theta,
            n_kl=n_same, label=f"{world.name}_same_corridor_expansion_top{top_k}",
        )
        row = compare_row(
            prim, params, world.baseline, eq,
            world=world, scenario=f"same_corridor_expansion_top{top_k}", family="transport_policy",
            abar_base=fund.abar_theta, sel=tidal_sel,
        )
        row["lane_km_added"] = p2_lane_km
        rows.append(row)

        qdelay_sel = select_tidal_lane_pairs_by_qdelay(prim, top_k=top_k, delta=1.0, xi_kl=world.baseline.Xi_kl)
        n_qdelay = apply_bidirectional_lane_delta(prim, qdelay_sel, delta=1.0)
        eq = solve_in_world(
            prim, fund, params, world,
            ubar=fund.ubar_theta, abar=fund.abar_theta,
            n_kl=n_qdelay, label=f"{world.name}_qdelay_expansion_top{top_k}",
        )
        row = compare_row(
            prim, params, world.baseline, eq,
            world=world, scenario=f"qdelay_expansion_top{top_k}", family="transport_policy",
            abar_base=fund.abar_theta, sel=qdelay_sel,
        )
        row["lane_km_added"] = float(np.sum((n_qdelay - prim.n_kl) * prim.ell_kl_km))
        row["overlap_with_tidal_pairs"] = int(
            len({tuple(x) for x in qdelay_sel.treated_pairs.tolist()} & {tuple(x) for x in tidal_sel.treated_pairs.tolist()})
        )
        rows.append(row)

        factor = uniform_expansion_factor_for_lane_budget(prim, p2_lane_km)
        eq = solve_in_world(
            prim, fund, params, world,
            ubar=fund.ubar_theta, abar=fund.abar_theta,
            n_kl=prim.n_kl * factor, label=f"{world.name}_uniform_expansion_matched",
        )
        row = compare_row(
            prim, params, world.baseline, eq,
            world=world, scenario="uniform_expansion_matched", family="transport_policy",
            abar_base=fund.abar_theta,
        )
        row["lane_km_added"] = p2_lane_km
        row["expansion_factor"] = factor
        rows.append(row)

    df = pd.DataFrame(rows)
    df.to_csv(out / "summary_by_scenario.csv", index=False)
    meta = {
        "version_id": version_id,
        "output_name": output_name,
        "sym_method": sym_method,
        "top_k": top_k,
        "alpha": params.alpha,
        "beta": params.beta,
        "theta": params.theta,
        "congestion_form": "anchored",
        "congestion_lambda": MAINLINE_LAMBDA_PHYS,
        "audit": {
            "same_primitives": True,
            "same_fundamentals": True,
            "same_welfare_formula": "primitive Frechet denominator",
            "exogenous_vs_endogenous_difference": "endogenous_congestion flag and anchored tau update only",
            "symmetric_world_difference": "reciprocal-pair symmetric tau_obs_anchor and tau_ff",
        },
    }
    (out / "suite_meta.json").write_text(json.dumps(meta, indent=2, default=float))
    return df


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo-root", default=str(DATA_WORK))
    p.add_argument("--version-id", default="model_input_edgeproj_v2_lcc_gate5_v1")
    p.add_argument("--output-name", default="symmetry_directionality_suite_gate5_bpr_pos05_v1")
    p.add_argument("--sym-method", choices=("geometric", "arithmetic"), default="geometric")
    p.add_argument("--top-k", type=int, default=100)
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s | %(message)s")
    data_work = data_work_root(args.repo_root)
    df = run_suite(
        data_work,
        version_id=args.version_id,
        output_name=args.output_name,
        sym_method=args.sym_method,
        top_k=args.top_k,
    )
    out = data_work / "outputs" / args.output_name
    LOG.info("Wrote %d rows to %s", len(df), out)
    cols = ["family", "world", "mode", "scenario", "Wbar_pct_change", "od_weighted_bilateral_cost_pct_change", "residence_dissimilarity_pct", "workplace_dissimilarity_pct"]
    print(df[cols].to_string(index=False))


if __name__ == "__main__":
    main()
