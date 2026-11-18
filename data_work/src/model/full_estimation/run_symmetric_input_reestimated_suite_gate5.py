"""Symmetric-input full re-estimation suite for gate5 bpr+pos05 mainline.

Unlike ``run_symmetry_directionality_suite_gate5.py`` (fixed fundamentals,
cost-surface swap only), this runner:

  1. Builds a symmetric-cost QSM input bundle by replacing reciprocal-pair
     directed AM observed times and night free-flow times with geometric means.
  2. Re-inverts fundamentals and re-solves the endogenous model from primitives
     for both directed-main and symmetric-reestimated worlds.
  3. Compares baseline distributions and common counterfactual effects.
  4. Reports over/under estimates as symmetric-reestimated effect minus
     directed-main effect (Wbar, OD cost, sorting, congestion feedback).
"""
from __future__ import annotations

import argparse
import json
import logging
import shutil
import sys
from dataclasses import dataclass, replace
from pathlib import Path

import numpy as np
import pandas as pd

DATA_WORK = Path(__file__).resolve().parents[3]
if str(DATA_WORK) not in sys.path:
    sys.path.insert(0, str(DATA_WORK))

from src.model.directional_cost import fixed_tau_model as ft  # noqa: E402
from src.model.directional_cost.policy_counterfactuals import (  # noqa: E402
    PolicySpec,
    apply_policy_abar,
    apply_policy_ubar,
)
from src.model.full_estimation import build_primitives as bp  # noqa: E402
from src.model.full_estimation import invert_fundamentals as inv  # noqa: E402
from src.model.full_estimation import parameters as pm  # noqa: E402
from src.model.full_estimation import stage2_counterfactuals as s2  # noqa: E402
from src.model.full_estimation.equilibrium_solve import resolve_kappa  # noqa: E402
from src.model.full_estimation.counterfactual_levels import (  # noqa: E402
    apply_bidirectional_lane_delta,
    run_cf3_uniform,
    uniform_expansion_factor_for_lane_budget,
)
from src.model.full_estimation.equilibrium_solve import (  # noqa: E402
    MAINLINE_ALPHA,
    MAINLINE_BETA,
    MAINLINE_CONGESTION_FORM,
    MAINLINE_LAMBDA_PHYS,
    MAINLINE_PAIR_SELECTION,
    EquilibriumResult,
)
from src.model.full_estimation.phase_metrics_common import data_work_root  # noqa: E402
from src.model.full_estimation.run_alpha_robustness_quick_gate5 import (  # noqa: E402
    solver_overrides_for_params,
)

LOG = logging.getLogger("symmetric_input_reestimated")
EPS = 1e-12

DEFAULT_DIRECTED_VERSION = "model_input_edgeproj_v2_lcc_gate5_v1"
DEFAULT_SYMMETRIC_VERSION = "model_input_edgeproj_v2_lcc_gate5_symmetric_geometric_v1"
DEFAULT_OUTPUT = "symmetric_input_reestimated_gate5_bpr_pos05_v1"


@dataclass
class WorldState:
    name: str
    version_id: str
    prim: bp.Primitives
    fund: inv.InversionResult
    params: pm.Parameters
    skw: dict
    baseline: EquilibriumResult


def _symmetric_pair_value(a: float, b: float, method: str) -> float:
    if method == "geometric":
        return float(np.sqrt(max(a, EPS) * max(b, EPS)))
    if method == "arithmetic":
        return float(0.5 * (a + b))
    raise ValueError(f"unknown sym_method: {method}")


def symmetrize_edges_dataframe(edges: pd.DataFrame, *, method: str = "geometric") -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return symmetrized edges and a reciprocal-pair audit table."""
    out = edges.copy()
    pair_lookup: dict[tuple[str, str], int] = {}
    for idx, row in out.iterrows():
        pair_lookup[(str(row["grid_o"]), str(row["grid_d"]))] = int(idx)

    rows: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for idx, row in out.iterrows():
        go, gd = str(row["grid_o"]), str(row["grid_d"])
        key = tuple(sorted((go, gd)))
        if key in seen:
            continue
        rev_idx = pair_lookup.get((gd, go))
        if rev_idx is None:
            rows.append({
                "grid_o": go, "grid_d": gd, "has_reverse": False,
                "tau_obs_dir": float(row["tau_obs_min"]),
                "tau_obs_rev": np.nan,
                "tau_obs_sym": float(row["tau_obs_min"]),
                "tau_ff_dir": float(row["tau_ff_min"]),
                "tau_ff_rev": np.nan,
                "tau_ff_sym": float(row["tau_ff_min"]),
            })
            continue
        seen.add(key)
        rev = out.loc[rev_idx]
        tau_obs_sym = _symmetric_pair_value(float(row["tau_obs_min"]), float(rev["tau_obs_min"]), method)
        tau_ff_sym = _symmetric_pair_value(float(row["tau_ff_min"]), float(rev["tau_ff_min"]), method)
        for edge_idx in (idx, rev_idx):
            out.at[edge_idx, "tau_obs_min"] = tau_obs_sym
            out.at[edge_idx, "tau_ff_min"] = tau_ff_sym
            if "t_min" in out.columns:
                out.at[edge_idx, "t_min"] = tau_obs_sym
            if "t_ff_min" in out.columns:
                out.at[edge_idx, "t_ff_min"] = tau_ff_sym
        rows.append({
            "grid_o": go, "grid_d": gd, "has_reverse": True,
            "tau_obs_dir": float(row["tau_obs_min"]),
            "tau_obs_rev": float(rev["tau_obs_min"]),
            "tau_obs_sym": tau_obs_sym,
            "tau_ff_dir": float(row["tau_ff_min"]),
            "tau_ff_rev": float(rev["tau_ff_min"]),
            "tau_ff_sym": tau_ff_sym,
            "tau_obs_ratio_before": float(row["tau_obs_min"] / max(float(rev["tau_obs_min"]), EPS)),
        })

    audit = pd.DataFrame(rows)
    return out, audit


def build_symmetric_cost_bundle(
    data_work: Path,
    *,
    directed_version_id: str,
    symmetric_version_id: str,
    sym_method: str = "geometric",
    grid_type: str = "square",
    force: bool = False,
) -> Path:
    """Copy directed bundle and symmetrize reciprocal travel-time columns."""
    src = data_work / "outputs" / directed_version_id
    dst = data_work / "outputs" / symmetric_version_id
    if dst.exists() and not force:
        LOG.info("Symmetric bundle already exists: %s", dst)
        return dst

    if dst.exists() and force:
        shutil.rmtree(dst)
    dst.mkdir(parents=True, exist_ok=True)
    (dst / "data").mkdir(parents=True, exist_ok=True)

    for fname in (f"qsm_input_nodes_{grid_type}.parquet", f"qsm_input_od_{grid_type}.parquet"):
        shutil.copy2(src / "data" / fname, dst / "data" / fname)
    for fname in ("data_quality_square_qsm.csv",):
        p = src / "data" / fname
        if p.exists():
            shutil.copy2(p, dst / "data" / fname)

    edges = pd.read_parquet(src / "data" / f"qsm_input_edges_{grid_type}.parquet")
    edges_sym, audit = symmetrize_edges_dataframe(edges, method=sym_method)
    edges_sym["symmetric_input_method"] = sym_method
    edges_sym["symmetric_input_source_version"] = directed_version_id
    edges_sym.to_parquet(dst / "data" / f"qsm_input_edges_{grid_type}.parquet", index=False)
    (dst / "metrics").mkdir(parents=True, exist_ok=True)
    audit.to_csv(dst / "metrics" / "symmetric_pair_audit.csv", index=False)

    meta = {
        "bundle_type": "symmetric_cost_input",
        "sym_method": sym_method,
        "directed_source_version": directed_version_id,
        "symmetric_version_id": symmetric_version_id,
        "unchanged_objects": [
            "network topology", "lanes", "OD flows", "node marginals",
            "parameters", "solver tolerances", "welfare formula",
        ],
        "symmetrized_columns": ["tau_obs_min", "tau_ff_min", "t_min", "t_ff_min"],
        "n_edges": int(len(edges_sym)),
        "n_reciprocal_pairs": int(audit["has_reverse"].sum()),
        "n_one_way_edges": int((~audit["has_reverse"]).sum()),
        "share_tau_obs_changed": float(
            np.mean(np.abs(edges_sym["tau_obs_min"].to_numpy() - edges["tau_obs_min"].to_numpy()) > 1e-9)
        ),
    }
    (dst / "symmetric_input_bundle_summary.json").write_text(json.dumps(meta, indent=2, default=float))

    snap = src / "config_snapshot.travel_cost_bundle.json"
    if snap.exists():
        snap_obj = json.loads(snap.read_text())
        snap_obj["symmetric_input_overlay"] = meta
        (dst / "config_snapshot.travel_cost_bundle.json").write_text(json.dumps(snap_obj, indent=2, default=float))

    LOG.info("Built symmetric input bundle -> %s", dst)
    return dst


# congestion technology; main() may override (defaults = mainline anchored, lambda 1.1)
# kappa: rule applied to the directed bundle; the same value is used in every world
KAPPA = {"rule": "fixed1", "value": None, "info": None}
CONGESTION = {"congestion_form": MAINLINE_CONGESTION_FORM, "congestion_lambda": MAINLINE_LAMBDA_PHYS, "xi_floor_rel": 0.0}


def _load_params() -> pm.Parameters:
    return replace(pm.Parameters(), alpha=MAINLINE_ALPHA, beta=MAINLINE_BETA)


def _build_world(data_work: Path, *, name: str, version_id: str, params: pm.Parameters) -> WorldState:
    prim = bp.load_primitives(data_work / "outputs" / version_id, period="AM")
    if KAPPA["value"] is None:
        KAPPA["value"], KAPPA["info"] = resolve_kappa(prim, KAPPA["rule"])
        KAPPA["info"]["set_from_version"] = version_id
    fund = inv.invert_fundamentals(prim, params, tol=1.5e-4, max_iter=1800, kappa=KAPPA["value"])
    skw = s2.build_congestion_solver_kwargs(
        prim, fund, params,
        **CONGESTION,
        **{k: v for k, v in solver_overrides_for_params(params).items() if v is not None},
    )
    baseline = s2.solve_baseline_endogenous(prim, fund, params, **skw)
    return WorldState(name, version_id, prim, fund, params, skw, baseline)


def _predicted_od_shares(eq: EquilibriumResult, abar_theta: np.ndarray, params: pm.Parameters) -> np.ndarray:
    static = s2._to_static(eq)
    return ft.predicted_od_shares(static, abar_theta, params)


def baseline_distribution_row(world: WorldState) -> dict:
    prim, fund, eq, params = world.prim, world.fund, world.baseline, world.params
    od = _predicted_od_shares(eq, fund.abar_theta, params)
    valid = (prim.L_R_obs > 0) | (prim.L_F_obs > 0)
    return {
        "world": world.name,
        "version_id": world.version_id,
        "Wbar": eq.Wbar,
        "chi": fund.chi,
        "kappa": fund.kappa,
        "mean_ubar_theta": float(np.mean(fund.ubar_theta[valid])),
        "mean_abar_theta": float(np.mean(fund.abar_theta[valid])),
        "gmean_ubar_theta": float(np.exp(np.mean(np.log(np.clip(fund.ubar_theta[valid], EPS, None))))),
        "gmean_abar_theta": float(np.exp(np.mean(np.log(np.clip(fund.abar_theta[valid], EPS, None))))),
        "residence_dissimilarity_vs_obs_pct": 100.0 * 0.5 * float(np.sum(np.abs(eq.l_R - prim.L_R_obs / prim.Lbar))),
        "workplace_dissimilarity_vs_obs_pct": 100.0 * 0.5 * float(np.sum(np.abs(eq.l_F - prim.L_F_obs / prim.Lbar))),
        "od_dissimilarity_vs_obs_pct": 100.0 * 0.5 * float(np.sum(np.abs(od - prim.L_ij_obs / prim.Lbar))),
        "od_weighted_bilateral_cost_min": ft.weighted_bilateral_cost(s2._to_static(eq), od, params.theta),
        "mean_edge_tau_min": float(np.mean(eq.tau_min_kl)),
        "p95_edge_tau_min": float(np.quantile(eq.tau_min_kl, 0.95)),
        "mean_Xi": float(np.mean(eq.Xi_kl)),
        "p95_Xi": float(np.quantile(eq.Xi_kl, 0.95)),
        "sum_Xi": float(np.sum(eq.Xi_kl)),
        "baseline_converged": bool(eq.converged),
        "baseline_n_outer": int(eq.n_outer),
        "inversion_converged": bool(fund.converged),
        "inversion_n_iter": int(fund.n_iter),
    }


def write_baseline_distribution(world: WorldState, out_dir: Path) -> None:
    prim, fund, eq = world.prim, world.fund, world.baseline
    od = _predicted_od_shares(eq, fund.abar_theta, world.params)
    wdir = out_dir / "baseline_distribution" / world.name
    wdir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame({
        "node_idx": np.arange(prim.n_nodes),
        "grid_id": prim.node_grid_ids,
        "ubar_theta": fund.ubar_theta,
        "abar_theta": fund.abar_theta,
        "l_R_model": eq.l_R,
        "l_F_model": eq.l_F,
        "L_R_obs_share": prim.L_R_obs / prim.Lbar,
        "L_F_obs_share": prim.L_F_obs / prim.Lbar,
        "Pi_i": eq.Pi_i,
        "P_j": eq.P_j,
    }).to_csv(wdir / "nodes.csv", index=False)

    pd.DataFrame({
        "edge_i": prim.edge_i,
        "edge_j": prim.edge_j,
        "tau_obs_input": prim.tau_obs_min,
        "tau_ff_input": prim.tau_ff_min,
        "tau_min_equilibrium": eq.tau_min_kl,
        "Xi_kl": eq.Xi_kl,
        "n_kl": prim.n_kl,
        "ell_kl_km": prim.ell_kl_km,
    }).to_csv(wdir / "edges.csv", index=False)

    np.savez_compressed(
        wdir / "od_shares.npz",
        od_shares=od,
        L_ij_obs_share=prim.L_ij_obs / prim.Lbar,
    )
    fund.write(wdir / "fundamentals")
    eq.write(wdir / "equilibrium")


def _cf_row(world: WorldState, scenario: str, family: str, eq: EquilibriumResult, abar_scen=None, extra=None) -> dict:
    row = s2.summarise_vs_baseline(
        world.baseline, eq, world.prim, world.params, world.fund.abar_theta,
        abar_theta_scenario=world.fund.abar_theta if abar_scen is None else abar_scen,
    )
    row.update({
        "world": world.name,
        "scenario": scenario,
        "family": family,
    })
    if extra:
        row.update(extra)
    return row


def run_counterfactuals(world: WorldState, *, top_k: int, include_tidal: bool) -> list[dict]:
    prim, fund, params, skw, baseline = (
        world.prim, world.fund, world.params, world.skw, world.baseline,
    )
    rows: list[dict] = []

    place_specs = [
        PolicySpec(name="hub_productivity_10pct", abar_mult_on_hubs=1.10),
        PolicySpec(name="outer_ring_amenity_10pct", ubar_mult_on_outer_ring=1.10),
    ]
    for spec in place_specs:
        ubar = apply_policy_ubar(fund.ubar_theta, prim, spec)
        abar = apply_policy_abar(fund.abar_theta, prim, spec)
        eq = s2.run_policy_endogenous(prim, fund, params, spec, **skw)
        rows.append(_cf_row(world, spec.name, "place_shock", eq, abar_scen=abar))

    eq_uniform = run_cf3_uniform(
        prim, params, fund.ubar_theta, fund.abar_theta, factor=1.10, **skw,
    )
    rows.append(_cf_row(world, "uniform_speed_1.10", "capacity_transport", eq_uniform))

    eq_same, sel_same = s2.run_road_expansion_same_corridor(
        prim, fund, params, baseline,
        top_k=top_k, pair_selection=MAINLINE_PAIR_SELECTION, horizon="long_run", **skw,
    )
    n_same = apply_bidirectional_lane_delta(prim, sel_same, delta=1.0)
    lane_km_same = float(np.sum((n_same - prim.n_kl) * prim.ell_kl_km))
    rows.append(_cf_row(
        world, f"same_corridor_expansion_top{top_k}", "capacity_transport", eq_same,
        extra={"lane_km_added": lane_km_same},
    ))

    eq_qdelay, sel_qdelay = s2.run_road_expansion_qdelay_topk(
        prim, fund, params, baseline, top_k=top_k, horizon="long_run", **skw,
    )
    n_qdelay = apply_bidirectional_lane_delta(prim, sel_qdelay, delta=1.0)
    lane_km_qdelay = float(np.sum((n_qdelay - prim.n_kl) * prim.ell_kl_km))
    rows.append(_cf_row(
        world, f"qdelay_expansion_top{top_k}", "capacity_transport", eq_qdelay,
        extra={"lane_km_added": lane_km_qdelay},
    ))

    factor = uniform_expansion_factor_for_lane_budget(prim, lane_km_same)
    eq_matched = run_cf3_uniform(
        prim, params, fund.ubar_theta, fund.abar_theta, factor=factor, **skw,
    )
    rows.append(_cf_row(
        world, "uniform_expansion_matched", "capacity_transport", eq_matched,
        extra={"lane_km_added": lane_km_same, "expansion_factor": factor},
    ))

    if include_tidal:
        eq_tidal, sel_tidal = s2.run_tidal_endogenous(
            prim, fund, params, baseline,
            top_k=top_k, delta=1.0, horizon="long_run",
            pair_selection=MAINLINE_PAIR_SELECTION, **skw,
        )
        rows.append(_cf_row(
            world, f"tidal_top{top_k}", "capacity_transport_optional", eq_tidal,
            extra={"n_treated_pairs": int(len(sel_tidal.treated_pairs))},
        ))

    return rows


def compare_baselines(directed: WorldState, symmetric: WorldState) -> dict:
    valid = (directed.prim.L_R_obs > 0) | (directed.prim.L_F_obs > 0)
    out = ft.fundamentals_comparison_summary(directed.fund, symmetric.fund, valid)
    eq_cmp = ft.comparison_summary(
        s2._to_static(directed.baseline),
        s2._to_static(symmetric.baseline),
        directed.prim,
        directed.params,
        abar_theta=directed.fund.abar_theta,
        abar_theta_scenario=symmetric.fund.abar_theta,
    )
    out.update({
        "comparison": "symmetric_reestimated_vs_directed_main",
        "directed_version_id": directed.version_id,
        "symmetric_version_id": symmetric.version_id,
        **{f"equilibrium_{k}": v for k, v in eq_cmp.items()},
    })
    return out


def over_under_estimates(cf_df: pd.DataFrame) -> pd.DataFrame:
    """symmetric-reestimated effect minus directed-main effect."""
    metrics = [
        "Wbar_pct_change",
        "od_weighted_bilateral_cost_pct_change",
        "residence_dissimilarity_pct",
        "workplace_dissimilarity_pct",
        "od_dissimilarity_pct",
        "mean_Xi_pct_change",
        "mean_edge_tau_pct_change",
    ]
    rows: list[dict] = []
    for (scenario, family), grp in cf_df.groupby(["scenario", "family"], sort=False):
        if grp["world"].nunique() < 2:
            continue
        drow = grp[grp["world"] == "directed_main"].iloc[0]
        srow = grp[grp["world"] == "symmetric_reestimated"].iloc[0]
        row = {
            "scenario": scenario,
            "family": family,
            "bias_definition": "symmetric_reestimated_effect_minus_directed_main_effect",
        }
        for m in metrics:
            if m in drow and m in srow and pd.notna(drow[m]) and pd.notna(srow[m]):
                row[f"directed_{m}"] = float(drow[m])
                row[f"symmetric_{m}"] = float(srow[m])
                row[f"bias_{m}"] = float(srow[m]) - float(drow[m])
        rows.append(row)
    return pd.DataFrame(rows)


def run_suite(
    data_work: Path,
    *,
    directed_version_id: str,
    symmetric_version_id: str,
    output_name: str,
    sym_method: str,
    top_k: int,
    include_tidal: bool,
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

    directed = _build_world(
        data_work, name="directed_main", version_id=directed_version_id, params=params,
    )
    symmetric = _build_world(
        data_work, name="symmetric_reestimated", version_id=symmetric_version_id, params=params,
    )

    baseline_rows = [
        baseline_distribution_row(directed),
        baseline_distribution_row(symmetric),
    ]
    pd.DataFrame(baseline_rows).to_csv(out / "baseline_summary_by_world.csv", index=False)
    baseline_cmp = compare_baselines(directed, symmetric)
    (out / "baseline_comparison.json").write_text(json.dumps(baseline_cmp, indent=2, default=float))

    write_baseline_distribution(directed, out)
    write_baseline_distribution(symmetric, out)

    cf_rows = run_counterfactuals(directed, top_k=top_k, include_tidal=include_tidal)
    cf_rows.extend(run_counterfactuals(symmetric, top_k=top_k, include_tidal=include_tidal))
    cf_df = pd.DataFrame(cf_rows)
    cf_df.to_csv(out / "counterfactual_effects_by_world.csv", index=False)

    bias_df = over_under_estimates(cf_df)
    bias_df.to_csv(out / "over_under_estimates.csv", index=False)

    meta = {
        "directed_version_id": directed_version_id,
        "symmetric_version_id": symmetric_version_id,
        "output_name": output_name,
        "sym_method": sym_method,
        "top_k": top_k,
        "include_tidal": include_tidal,
        "alpha": params.alpha,
        "beta": params.beta,
        "theta": params.theta,
        **CONGESTION,
        "kappa": KAPPA["info"],
        "audit": {
            "not_fixed_fundamentals_diagnostic": True,
            "full_re_estimation_from_primitives_and_inversion": True,
            "bias_sign": "symmetric_reestimated_effect_minus_directed_main_effect",
            "unchanged_across_worlds": [
                "network", "lanes", "OD/marginals", "parameters", "solver", "welfare formula",
            ],
            "changed_across_worlds": [
                "reciprocal-pair symmetric tau_obs_min and tau_ff_min in input bundle",
                "re-inverted ubar_theta and abar_theta",
                "re-solved baseline and counterfactual equilibria",
            ],
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
    p.add_argument("--top-k", type=int, default=100)
    p.add_argument("--include-tidal", action="store_true", default=True)
    p.add_argument("--no-tidal", action="store_false", dest="include_tidal")
    p.add_argument("--force-bundle", action="store_true", help="Rebuild symmetric input bundle even if it exists.")
    p.add_argument("--congestion-form", choices=("anchored", "ce_floor", "bpr"), default=MAINLINE_CONGESTION_FORM)
    p.add_argument("--congestion-lambda", type=float, default=MAINLINE_LAMBDA_PHYS)
    p.add_argument("--xi-floor-rel", type=float, default=0.0)
    p.add_argument("--kappa-rule", default="fixed1",
                   help="fixed1: kappa=1 (pre-2026-11-09 runs); spectral: smallest kappa>=1 with rho(A)<=0.95 at min(tau_ff,tau_obs); or a number")
    args = p.parse_args(argv)
    KAPPA["rule"] = args.kappa_rule
    CONGESTION.update(congestion_form=args.congestion_form, congestion_lambda=args.congestion_lambda,
                      xi_floor_rel=args.xi_floor_rel)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s | %(message)s")
    data_work = data_work_root(args.repo_root)
    run_suite(
        data_work,
        directed_version_id=args.directed_version_id,
        symmetric_version_id=args.symmetric_version_id,
        output_name=args.output_name,
        sym_method=args.sym_method,
        top_k=args.top_k,
        include_tidal=args.include_tidal,
        force_bundle=args.force_bundle,
    )

    out = data_work / "outputs" / args.output_name
    print("\n=== baseline_summary_by_world ===")
    print(pd.read_csv(out / "baseline_summary_by_world.csv").to_string(index=False))
    print("\n=== over_under_estimates (head) ===")
    bias = pd.read_csv(out / "over_under_estimates.csv")
    show = [c for c in bias.columns if c.startswith("bias_") or c in ("scenario", "family")]
    print(bias[show].to_string(index=False))


if __name__ == "__main__":
    main()
