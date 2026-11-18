"""Policy grid for directed vs symmetric-input full re-estimation worlds.

This runner extends ``run_symmetric_input_reestimated_suite_gate5.py`` from a
small scenario set to a policy grid. It keeps the comparison target fixed:

* directed-main input bundle, with fundamentals re-inverted;
* symmetric-input bundle, with reciprocal observed and free-flow costs
  symmetrized before fundamentals are re-inverted;
* identical parameters, welfare formula, solver, network topology, lanes, OD
  flows, and marginals.

The output separates two objects:

* own-world selection: each world selects its own treated corridors, measuring
  policy-design error from using a symmetric model;
* directed-fixed selection: both worlds evaluate the directed-main treated
  corridors, measuring policy-evaluation error for the same physical policy.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
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
)
from src.model.full_estimation import stage2_counterfactuals as s2  # noqa: E402
from src.model.full_estimation.counterfactual_levels import (  # noqa: E402
    TidalLaneSelection,
    apply_bidirectional_lane_delta,
    select_tidal_lane_pairs_by_qdelay,
    select_tidal_lane_pairs_by_rank,
    uniform_expansion_factor_for_lane_budget,
)
from src.model.full_estimation.equilibrium_solve import (  # noqa: E402
    MAINLINE_PAIR_SELECTION,
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
)

LOG = logging.getLogger("symmetric_input_policy_grid")
EPS = 1e-12

DEFAULT_OUTPUT = "symmetric_input_policy_grid_gate5_bpr_pos05_v1"


def _parse_csv_numbers(value: str, typ=float) -> list:
    return [typ(x.strip()) for x in value.split(",") if x.strip()]


def _tidal_n_cf(world: WorldState, sel: TidalLaneSelection, *, delta: float) -> np.ndarray:
    n_cf = world.prim.n_kl.copy()
    for slow, fast in sel.treated_pairs:
        n_cf[int(slow)] = n_cf[int(slow)] + delta
        n_cf[int(fast)] = n_cf[int(fast)] - delta
    return n_cf


def _lane_km_stats(world: WorldState, n_cf: np.ndarray) -> dict:
    delta_n = n_cf - world.prim.n_kl
    lane_km_net = float(np.sum(delta_n * world.prim.ell_kl_km))
    lane_km_gross = float(np.sum(np.abs(delta_n) * world.prim.ell_kl_km))
    return {
        "lane_km_added_net": lane_km_net,
        "lane_km_changed_gross": lane_km_gross,
    }


def _solve_capacity_policy(
    world: WorldState,
    n_cf: np.ndarray,
    *,
    label: str,
    horizon: str,
):
    pin_kw = {}
    if horizon == "short_run":
        pin_kw = dict(l_R_fixed=world.baseline.l_R, l_F_fixed=world.baseline.l_F)
    elif horizon != "long_run":
        raise ValueError(f"unknown horizon: {horizon}")
    return solve_equilibrium(
        world.prim,
        world.fund.ubar_theta,
        world.fund.abar_theta,
        world.params,
        n_kl_override=n_cf,
        tau_min_init=world.prim.tau_obs_min.copy(),
        endogenous_congestion=True,
        label=label,
        **pin_kw,
        **world.skw,
    )


def _row(
    world: WorldState,
    *,
    scenario: str,
    family: str,
    policy_kind: str,
    horizon: str,
    selection_scope: str,
    eq,
    extra: dict | None = None,
    abar_scen: np.ndarray | None = None,
) -> dict:
    row = s2.summarise_vs_baseline(
        world.baseline,
        eq,
        world.prim,
        world.params,
        world.fund.abar_theta,
        abar_theta_scenario=world.fund.abar_theta if abar_scen is None else abar_scen,
    )
    base_cost = float(row["base_od_weighted_bilateral_cost_min"])
    scen_cost = float(row["scenario_od_weighted_bilateral_cost_min"])
    row.update({
        "world": world.name,
        "version_id": world.version_id,
        "scenario": scenario,
        "family": family,
        "policy_kind": policy_kind,
        "horizon": horizon,
        "selection_scope": selection_scope,
        "od_cost_change_min_per_commuter": scen_cost - base_cost,
        "od_cost_change_sec_per_commuter": 60.0 * (scen_cost - base_cost),
        "od_cost_change_person_min_day": (scen_cost - base_cost) * world.prim.Lbar,
    })
    if extra:
        row.update(extra)
    return row


def _selection_key(sel: TidalLaneSelection) -> set[tuple[int, int]]:
    return {tuple(sorted((int(s), int(f)))) for s, f in sel.treated_pairs}


def _selection_orientation(sel: TidalLaneSelection) -> dict[tuple[int, int], int]:
    return {tuple(sorted((int(s), int(f)))): int(s) for s, f in sel.treated_pairs}


def _selection_overlap_row(
    *,
    policy_kind: str,
    top_k: int,
    delta: float,
    directed_sel: TidalLaneSelection,
    symmetric_sel: TidalLaneSelection,
) -> dict:
    dset = _selection_key(directed_sel)
    sset = _selection_key(symmetric_sel)
    inter = dset & sset
    union = dset | sset
    dori = _selection_orientation(directed_sel)
    sori = _selection_orientation(symmetric_sel)
    if inter:
        orientation_agree = sum(1 for k in inter if dori.get(k) == sori.get(k)) / len(inter)
    else:
        orientation_agree = np.nan
    return {
        "policy_kind": policy_kind,
        "top_k": top_k,
        "delta_lanes": delta,
        "directed_n": directed_sel.n_treated,
        "symmetric_n": symmetric_sel.n_treated,
        "overlap_n": len(inter),
        "union_n": len(union),
        "jaccard": len(inter) / max(len(union), 1),
        "overlap_share_of_directed": len(inter) / max(len(dset), 1),
        "overlap_share_of_symmetric": len(inter) / max(len(sset), 1),
        "orientation_agreement_share": orientation_agree,
    }


def _own_selection(world: WorldState, policy_kind: str, *, top_k: int, delta: float) -> TidalLaneSelection:
    if policy_kind in {"tidal", "same_corridor_expansion"}:
        return select_tidal_lane_pairs_by_rank(
            world.prim,
            top_k=top_k,
            delta=delta,
            rank=MAINLINE_PAIR_SELECTION,
            xi_kl=world.baseline.Xi_kl if MAINLINE_PAIR_SELECTION == "score_bpr_st" else None,
        )
    if policy_kind == "qdelay_expansion":
        return select_tidal_lane_pairs_by_qdelay(
            world.prim,
            top_k=top_k,
            delta=delta,
            xi_kl=world.baseline.Xi_kl,
        )
    raise ValueError(f"unknown policy_kind: {policy_kind}")


def _run_selected_capacity_policy(
    world: WorldState,
    *,
    policy_kind: str,
    sel: TidalLaneSelection,
    top_k: int,
    delta: float,
    horizon: str,
    selection_scope: str,
) -> dict:
    if policy_kind == "tidal":
        n_cf = _tidal_n_cf(world, sel, delta=delta)
        scenario = f"tidal_top{top_k}_d{delta:g}"
    elif policy_kind == "same_corridor_expansion":
        n_cf = apply_bidirectional_lane_delta(world.prim, sel, delta=delta)
        scenario = f"same_corridor_expansion_top{top_k}_d{delta:g}"
    elif policy_kind == "qdelay_expansion":
        n_cf = apply_bidirectional_lane_delta(world.prim, sel, delta=delta)
        scenario = f"qdelay_expansion_top{top_k}_d{delta:g}"
    else:
        raise ValueError(f"unknown policy_kind: {policy_kind}")

    eq = _solve_capacity_policy(
        world,
        n_cf,
        label=f"{scenario}_{selection_scope}_{horizon}",
        horizon=horizon,
    )
    extra = {
        "top_k": top_k,
        "delta_lanes": delta,
        "n_treated_pairs": int(sel.n_treated),
        **_lane_km_stats(world, n_cf),
    }
    return _row(
        world,
        scenario=scenario,
        family="capacity_transport",
        policy_kind=policy_kind,
        horizon=horizon,
        selection_scope=selection_scope,
        eq=eq,
        extra=extra,
    )


def _run_uniform_capacity(world: WorldState, *, factor: float, horizon: str) -> dict:
    n_cf = world.prim.n_kl * float(factor)
    eq = _solve_capacity_policy(
        world,
        n_cf,
        label=f"uniform_capacity_x{factor:g}_{horizon}",
        horizon=horizon,
    )
    return _row(
        world,
        scenario=f"uniform_capacity_x{factor:g}",
        family="capacity_transport",
        policy_kind="uniform_capacity",
        horizon=horizon,
        selection_scope="not_applicable",
        eq=eq,
        extra={
            "uniform_capacity_factor": factor,
            **_lane_km_stats(world, n_cf),
        },
    )


def _run_matched_uniform_capacity(
    world: WorldState,
    *,
    target_lane_km: float,
    top_k: int,
    delta: float,
    horizon: str,
    selection_scope: str,
) -> dict:
    factor = uniform_expansion_factor_for_lane_budget(world.prim, target_lane_km)
    n_cf = world.prim.n_kl * factor
    eq = _solve_capacity_policy(
        world,
        n_cf,
        label=f"uniform_capacity_matched_top{top_k}_d{delta:g}_{selection_scope}_{horizon}",
        horizon=horizon,
    )
    return _row(
        world,
        scenario=f"uniform_capacity_matched_top{top_k}_d{delta:g}",
        family="capacity_transport",
        policy_kind="uniform_capacity_matched",
        horizon=horizon,
        selection_scope=selection_scope,
        eq=eq,
        extra={
            "top_k": top_k,
            "delta_lanes": delta,
            "uniform_capacity_factor": factor,
            "target_lane_km_added": target_lane_km,
            **_lane_km_stats(world, n_cf),
        },
    )


def _run_place_benchmark(world: WorldState, *, kind: str, pct: float) -> dict:
    if kind == "hub_productivity":
        spec = PolicySpec(name=f"hub_productivity_{pct:g}pct", abar_mult_on_hubs=1.0 + pct / 100.0)
    elif kind == "outer_ring_amenity":
        spec = PolicySpec(name=f"outer_ring_amenity_{pct:g}pct", ubar_mult_on_outer_ring=1.0 + pct / 100.0)
    else:
        raise ValueError(f"unknown place benchmark: {kind}")
    abar = apply_policy_abar(world.fund.abar_theta, world.prim, spec)
    _ = apply_policy_ubar(world.fund.ubar_theta, world.prim, spec)
    eq = s2.run_policy_endogenous(world.prim, world.fund, world.params, spec, **world.skw)
    return _row(
        world,
        scenario=spec.name,
        family="place_shock_benchmark",
        policy_kind=kind,
        horizon="long_run",
        selection_scope="not_applicable",
        eq=eq,
        abar_scen=abar,
        extra={"shock_pct": pct},
    )


def over_under(policy_df: pd.DataFrame) -> pd.DataFrame:
    metrics = [
        "Wbar_pct_change",
        "od_weighted_bilateral_cost_pct_change",
        "od_cost_change_sec_per_commuter",
        "od_cost_change_person_min_day",
        "residence_dissimilarity_pct",
        "workplace_dissimilarity_pct",
        "od_dissimilarity_pct",
        "mean_Xi_pct_change",
        "mean_edge_tau_pct_change",
    ]
    keys = ["scenario", "family", "policy_kind", "horizon", "selection_scope"]
    rows: list[dict] = []
    for key, grp in policy_df.groupby(keys, sort=False, dropna=False):
        if grp["world"].nunique() < 2:
            continue
        drow = grp[grp["world"] == "directed_main"].iloc[0]
        srow = grp[grp["world"] == "symmetric_reestimated"].iloc[0]
        row = dict(zip(keys, key))
        row["bias_definition"] = "symmetric_reestimated_effect_minus_directed_main_effect"
        for passthrough in [
            "top_k", "delta_lanes", "uniform_capacity_factor", "lane_km_added_net",
            "lane_km_changed_gross", "target_lane_km_added", "n_treated_pairs",
        ]:
            if passthrough in drow.index:
                row[f"directed_{passthrough}"] = drow.get(passthrough)
            if passthrough in srow.index:
                row[f"symmetric_{passthrough}"] = srow.get(passthrough)
        for metric in metrics:
            if metric in drow.index and metric in srow.index:
                row[f"directed_{metric}"] = float(drow[metric])
                row[f"symmetric_{metric}"] = float(srow[metric])
                row[f"bias_{metric}"] = float(srow[metric]) - float(drow[metric])
                row[f"abs_bias_{metric}"] = abs(row[f"bias_{metric}"])
        rows.append(row)
    return pd.DataFrame(rows)


def summarize_bias(bias_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []
    for (family, policy_kind, selection_scope), grp in bias_df.groupby(
        ["family", "policy_kind", "selection_scope"], sort=False, dropna=False,
    ):
        row = {
            "family": family,
            "policy_kind": policy_kind,
            "selection_scope": selection_scope,
            "n_scenarios": len(grp),
        }
        for metric in [
            "Wbar_pct_change",
            "od_cost_change_sec_per_commuter",
            "od_cost_change_person_min_day",
            "workplace_dissimilarity_pct",
            "od_dissimilarity_pct",
            "mean_Xi_pct_change",
        ]:
            col = f"abs_bias_{metric}"
            raw = f"bias_{metric}"
            if col in grp:
                row[f"median_abs_bias_{metric}"] = float(grp[col].median())
                row[f"max_abs_bias_{metric}"] = float(grp[col].max())
                idx = grp[col].idxmax()
                row[f"max_abs_bias_{metric}_scenario"] = str(grp.loc[idx, "scenario"])
                row[f"signed_bias_at_max_{metric}"] = float(grp.loc[idx, raw])
        rows.append(row)
    return pd.DataFrame(rows)


def run_policy_grid(
    data_work: Path,
    *,
    directed_version_id: str,
    symmetric_version_id: str,
    output_name: str,
    sym_method: str,
    top_k_list: list[int],
    delta_list: list[float],
    uniform_factors: list[float],
    place_pcts: list[float],
    horizon: str,
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

    pd.DataFrame([
        baseline_distribution_row(directed),
        baseline_distribution_row(symmetric),
    ]).to_csv(out / "baseline_summary_by_world.csv", index=False)

    rows: list[dict] = []
    overlap_rows: list[dict] = []

    LOG.info("Running place-shock benchmarks")
    for pct in place_pcts:
        for kind in ("hub_productivity", "outer_ring_amenity"):
            rows.append(_run_place_benchmark(directed, kind=kind, pct=pct))
            rows.append(_run_place_benchmark(symmetric, kind=kind, pct=pct))

    LOG.info("Running uniform capacity factors")
    for factor in uniform_factors:
        rows.append(_run_uniform_capacity(directed, factor=factor, horizon=horizon))
        rows.append(_run_uniform_capacity(symmetric, factor=factor, horizon=horizon))

    policy_kinds = ["tidal", "same_corridor_expansion", "qdelay_expansion"]
    directed_selections: dict[tuple[str, int, float], TidalLaneSelection] = {}
    symmetric_selections: dict[tuple[str, int, float], TidalLaneSelection] = {}

    LOG.info("Building and evaluating top-K selected capacity policies")
    for delta in delta_list:
        for top_k in top_k_list:
            for policy_kind in policy_kinds:
                key = (policy_kind, top_k, delta)
                dsel = _own_selection(directed, policy_kind, top_k=top_k, delta=delta)
                ssel = _own_selection(symmetric, policy_kind, top_k=top_k, delta=delta)
                directed_selections[key] = dsel
                symmetric_selections[key] = ssel
                overlap_rows.append(_selection_overlap_row(
                    policy_kind=policy_kind,
                    top_k=top_k,
                    delta=delta,
                    directed_sel=dsel,
                    symmetric_sel=ssel,
                ))

                rows.append(_run_selected_capacity_policy(
                    directed,
                    policy_kind=policy_kind,
                    sel=dsel,
                    top_k=top_k,
                    delta=delta,
                    horizon=horizon,
                    selection_scope="own_world",
                ))
                rows.append(_run_selected_capacity_policy(
                    symmetric,
                    policy_kind=policy_kind,
                    sel=ssel,
                    top_k=top_k,
                    delta=delta,
                    horizon=horizon,
                    selection_scope="own_world",
                ))

                rows.append(_run_selected_capacity_policy(
                    directed,
                    policy_kind=policy_kind,
                    sel=dsel,
                    top_k=top_k,
                    delta=delta,
                    horizon=horizon,
                    selection_scope="directed_fixed",
                ))
                rows.append(_run_selected_capacity_policy(
                    symmetric,
                    policy_kind=policy_kind,
                    sel=dsel,
                    top_k=top_k,
                    delta=delta,
                    horizon=horizon,
                    selection_scope="directed_fixed",
                ))

            # Budget-matched uniform expansion uses the same-corridor lane-km as the
            # policy budget, once for each selection scope.
            d_same = directed_selections[("same_corridor_expansion", top_k, delta)]
            s_same = symmetric_selections[("same_corridor_expansion", top_k, delta)]
            d_budget = _lane_km_stats(
                directed, apply_bidirectional_lane_delta(directed.prim, d_same, delta=delta)
            )["lane_km_added_net"]
            s_budget = _lane_km_stats(
                symmetric, apply_bidirectional_lane_delta(symmetric.prim, s_same, delta=delta)
            )["lane_km_added_net"]
            rows.append(_run_matched_uniform_capacity(
                directed,
                target_lane_km=d_budget,
                top_k=top_k,
                delta=delta,
                horizon=horizon,
                selection_scope="own_world",
            ))
            rows.append(_run_matched_uniform_capacity(
                symmetric,
                target_lane_km=s_budget,
                top_k=top_k,
                delta=delta,
                horizon=horizon,
                selection_scope="own_world",
            ))
            rows.append(_run_matched_uniform_capacity(
                directed,
                target_lane_km=d_budget,
                top_k=top_k,
                delta=delta,
                horizon=horizon,
                selection_scope="directed_fixed",
            ))
            rows.append(_run_matched_uniform_capacity(
                symmetric,
                target_lane_km=d_budget,
                top_k=top_k,
                delta=delta,
                horizon=horizon,
                selection_scope="directed_fixed",
            ))

    policy_df = pd.DataFrame(rows)
    policy_df.to_csv(out / "policy_grid_effects_by_world.csv", index=False)

    overlap_df = pd.DataFrame(overlap_rows)
    overlap_df.to_csv(out / "selection_overlap.csv", index=False)

    bias_df = over_under(policy_df)
    bias_df.to_csv(out / "over_under_policy_grid.csv", index=False)

    summary_df = summarize_bias(bias_df)
    summary_df.to_csv(out / "bias_summary_by_policy_family.csv", index=False)

    meta = {
        "directed_version_id": directed_version_id,
        "symmetric_version_id": symmetric_version_id,
        "output_name": output_name,
        "sym_method": sym_method,
        "top_k_list": top_k_list,
        "delta_list": delta_list,
        "uniform_factors": uniform_factors,
        "place_pcts": place_pcts,
        "horizon": horizon,
        "bias_definition": "symmetric_reestimated_effect_minus_directed_main_effect",
        "selection_scopes": {
            "own_world": "each world selects treated corridors using its own baseline",
            "directed_fixed": "both worlds evaluate directed-main treated corridors",
            "not_applicable": "policy has no treated-edge selection",
        },
    }
    (out / "suite_meta.json").write_text(json.dumps(meta, indent=2, default=float))
    LOG.info("Wrote policy grid to %s", out)


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo-root", default=str(DATA_WORK))
    p.add_argument("--directed-version-id", default=DEFAULT_DIRECTED_VERSION)
    p.add_argument("--symmetric-version-id", default=DEFAULT_SYMMETRIC_VERSION)
    p.add_argument("--output-name", default=DEFAULT_OUTPUT)
    p.add_argument("--sym-method", choices=("geometric", "arithmetic"), default="geometric")
    p.add_argument("--top-k-list", default="25,50,100,200")
    p.add_argument("--delta-list", default="1.0")
    p.add_argument("--uniform-factors", default="1.01,1.025,1.05,1.10")
    p.add_argument("--place-pcts", default="10")
    p.add_argument("--horizon", choices=("short_run", "long_run"), default="long_run")
    p.add_argument("--force-bundle", action="store_true")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s | %(message)s")
    data_work = data_work_root(args.repo_root)
    run_policy_grid(
        data_work,
        directed_version_id=args.directed_version_id,
        symmetric_version_id=args.symmetric_version_id,
        output_name=args.output_name,
        sym_method=args.sym_method,
        top_k_list=_parse_csv_numbers(args.top_k_list, int),
        delta_list=_parse_csv_numbers(args.delta_list, float),
        uniform_factors=_parse_csv_numbers(args.uniform_factors, float),
        place_pcts=_parse_csv_numbers(args.place_pcts, float),
        horizon=args.horizon,
        force_bundle=args.force_bundle,
    )

    out = data_work / "outputs" / args.output_name
    print("\n=== bias_summary_by_policy_family ===")
    print(pd.read_csv(out / "bias_summary_by_policy_family.csv").to_string(index=False))
    print("\n=== selection_overlap ===")
    print(pd.read_csv(out / "selection_overlap.csv").to_string(index=False))


if __name__ == "__main__":
    main()
