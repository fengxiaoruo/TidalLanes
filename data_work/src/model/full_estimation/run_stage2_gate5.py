"""Stage 2 gate5 driver: anchored endogenous congestion + full CF suite.

Mainline (2026-06-29):
  - travel cost: model_input_edgeproj_v2_lcc_gate5_v1 (raw_connected_v1 edgeproj)
  - congestion_form=anchored, lambda_phys=1.1 (documented calibration)
  - damping=0.25 for anchored outer loop
  - legacy diagnostic lambda: --congestion-form legacy --lambda-mode diagnostic
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

from src.model.full_estimation import build_primitives as bp  # noqa: E402
from src.model.full_estimation import parameters as pm  # noqa: E402
from src.model.full_estimation import invert_fundamentals as inv  # noqa: E402
from src.model.full_estimation import lambda_diagnostic as lam  # noqa: E402
from src.model.full_estimation import stage2_counterfactuals as s2  # noqa: E402
from src.model.full_estimation.equilibrium_solve import resolve_kappa  # noqa: E402
from src.model.full_estimation.equilibrium_solve import (  # noqa: E402
    BEIJING_LAMBDA_PHYS,
    MAINLINE_ALPHA,
    MAINLINE_BETA,
    MAINLINE_CONGESTION_FORM,
    MAINLINE_LAMBDA_PHYS,
    MAINLINE_PAIR_SELECTION,
)
from src.model.full_estimation.run_alpha_robustness_quick_gate5 import (  # noqa: E402
    solver_overrides_for_params,
)
from src.model.directional_cost.tidal_lane_counterfactuals import (  # noqa: E402
    TidalLaneSpec,
    default_experiment_grid,
)


LOG = logging.getLogger("stage2_gate5")


def _data_work_root(arg: str) -> Path:
    p = Path(arg).resolve()
    if (p / "outputs").is_dir() and (p / "src" / "model").is_dir():
        return p
    if (p / "data_work" / "outputs").is_dir():
        return p / "data_work"
    return DATA_WORK


def _resolve_params(args, lambda_hat_probe: float | None = None) -> pm.Parameters:
    from dataclasses import replace

    base = replace(
        pm.Parameters(),
        alpha=float(args.alpha),
        beta=float(args.beta),
        alpha_source=(
            "Literature mid-range gross production spillover (mainline pos05)"
            if args.alpha > 0
            else "AA(2022) Seattle net workplace externality"
        ),
        beta_source="AA(2022) Seattle beta held",
    )
    if args.lambda_mode == "placeholder":
        return base
    if args.lambda_mode == "diagnostic":
        if lambda_hat_probe is None:
            raise ValueError("diagnostic lambda requires a probe estimate")
        lam_val = max(float(lambda_hat_probe), 1e-4)
        return base.with_lambda(lam_val, source=f"cross-section diagnostic hat={lam_val:.5f}")
    if args.lambda_mode == "custom":
        return base.with_lambda(float(args.lambda_value), source="CLI --lambda-value")
    raise ValueError(f"unknown lambda_mode: {args.lambda_mode}")


def run_pipeline(
    data_work: Path,
    version_id: str,
    out: Path,
    params: pm.Parameters,
    *,
    quick: bool,
    skip_tidal_grid: bool,
    congestion_form: str,
    congestion_lambda: float,
    pair_selection: str,
    skw_extra: dict,
) -> dict:
    out.mkdir(parents=True, exist_ok=True)
    (out / "counterfactuals").mkdir(parents=True, exist_ok=True)

    prim = bp.load_primitives(data_work / "outputs" / version_id, period="AM")
    params.write(out / "parameters.json")
    bp.write_primitives_summary(prim, out / "primitives")

    LOG.info("Inverting fundamentals")
    kappa, kinfo = resolve_kappa(prim, skw_extra.pop("kappa_rule", "fixed1"))
    (out / "kappa.json").write_text(json.dumps(kinfo, indent=2))
    fund = inv.invert_fundamentals(
        prim, params, tol=1.5e-4, max_iter=1800, kappa=kappa,
    )
    fund.write(out / "fundamentals")

    skw = s2.build_congestion_solver_kwargs(
        prim, fund, params,
        congestion_form=congestion_form,
        congestion_lambda=congestion_lambda,
        **skw_extra,
    )

    mainline_meta = {
        "congestion_form": congestion_form,
        "congestion_lambda": congestion_lambda,
        "xi_floor_rel": skw_extra.get("xi_floor_rel", 0.0),
        "pair_selection": pair_selection,
        "travel_cost": "raw_connected_v1 edgeproj_v2 gate5",
    }
    (out / "mainline_config.json").write_text(json.dumps(mainline_meta, indent=2))

    LOG.info("Baseline — exogenous tau")
    baseline_exog = s2.solve_baseline_exogenous_tau(prim, fund, params, **skw)
    baseline_exog.write(out / "baseline_exogenous_tau")

    LOG.info("Baseline — endogenous (%s, lambda=%.3f)", congestion_form, congestion_lambda)
    baseline_endog = s2.solve_baseline_endogenous(prim, fund, params, **skw)
    baseline_endog.write(out / "baseline_endogenous")

    lam_diag = lam.estimate_lambda_cross_section(
        prim.tau_obs_min, prim.tau_ff_min, prim.n_kl, baseline_endog.Xi_kl,
    )
    (out / "lambda_diagnostic.json").write_text(json.dumps(lam_diag.to_dict(), indent=2))

    meta = {
        "version_id": version_id,
        "lambda_used": params.lambda_,
        "lambda_source": params.lambda_source,
        **mainline_meta,
        "baseline_exogenous": {
            "Wbar": baseline_exog.Wbar,
            "converged": baseline_exog.converged,
        },
        "baseline_endogenous": {
            "Wbar": baseline_endog.Wbar,
            "converged": baseline_endog.converged,
            "n_outer": baseline_endog.n_outer,
            "max_log_change": baseline_endog.max_log_change,
            "mean_tau_vs_obs_pct": 100.0 * float(
                baseline_endog.tau_min_kl.mean() / prim.tau_obs_min.mean() - 1.0
            ),
            "median_tau_ratio_vs_obs": float(
                np.median(baseline_endog.tau_min_kl / prim.tau_obs_min)
            ),
        },
        "lambda_diagnostic_hat_at_this_lambda": lam_diag.lambda_hat,
    }
    (out / "baseline_summary.json").write_text(json.dumps(meta, indent=2, default=float))

    LOG.info("Policy CFs — exogenous + endogenous")
    policy_df = s2.run_all_policy_counterfactuals(
        prim, fund, params, baseline_exog, baseline_endog=baseline_endog, **skw,
    )
    policy_df.to_csv(out / "counterfactuals" / "policy_cf_all_modes.csv", index=False)

    cf_top_k = 25 if quick else 50
    cf3 = [1.10] if quick else [1.10, 1.25, 1.50]
    LOG.info("Structural CFs (geometric symmetric CF1)")
    structural_df = s2.run_structural_counterfactuals(
        prim, fund, params, baseline_endog,
        cf2_top_k=cf_top_k, cf3_factors=cf3, sym_method="geometric",
        pair_selection=pair_selection, **skw,
    )
    if not quick:
        arith = s2.run_structural_counterfactuals(
            prim, fund, params, baseline_endog,
            cf2_top_k=0, cf3_factors=[], sym_method="arithmetic",
            pair_selection=pair_selection, **skw,
        )
        arith = arith[arith["cf"].str.startswith("CF1")]
        structural_df = pd.concat([structural_df, arith], ignore_index=True)
    structural_df.to_csv(out / "counterfactuals" / "structural_cf_endogenous.csv", index=False)

    LOG.info("Structural CFs — exogenous tau")
    exog_structural_df = s2.run_structural_exogenous(
        prim, fund, params, baseline_exog,
        cf2_top_k=cf_top_k, cf3_factors=cf3 if not quick else [1.10],
        sym_method="geometric", pair_selection=pair_selection, **skw,
    )
    exog_structural_df.to_csv(
        out / "counterfactuals" / "structural_cf_exogenous.csv", index=False,
    )

    tidal_specs = [(cf_top_k, 1.0)]
    if not quick:
        tidal_specs.extend([(50, 0.5), (100, 1.0)])
    tidal_rows = []
    for top_k, delta in tidal_specs:
        for horizon in ("short_run", "long_run"):
            LOG.info(
                "Tidal endogenous top=%s delta=%s horizon=%s selection=%s",
                top_k, delta, horizon, pair_selection,
            )
            eq_t, sel = s2.run_tidal_endogenous(
                prim, fund, params, baseline_endog,
                top_k=top_k, delta=delta, horizon=horizon,
                pair_selection=pair_selection, **skw,
            )
            sub = out / "counterfactuals" / f"tidal_endog_top{top_k}_d{delta}_{horizon}"
            eq_t.write(sub)
            if horizon == "long_run":
                sel.to_frame(prim).to_csv(sub / "selected_pairs.csv", index=False)
            row = s2.summarise_vs_baseline(
                baseline_endog, eq_t, prim, params, fund.abar_theta,
            )
            row.update({
                "cf": "tidal_lane_endogenous",
                "top_k": top_k,
                "delta_lanes": delta,
                "horizon": horizon,
                "n_pairs": sel.n_treated,
                "pair_selection": pair_selection,
                "congestion_form": congestion_form,
                "congestion_lambda": congestion_lambda,
            })
            tidal_rows.append(row)
    pd.DataFrame(tidal_rows).to_csv(
        out / "counterfactuals" / "tidal_lane_endogenous_summary.csv", index=False,
    )

    if not skip_tidal_grid:
        LOG.info("Tidal exogenous BPR grid (+ short-run fixed shares)")
        t_specs = default_experiment_grid()
        if quick:
            t_specs = [TidalLaneSpec(name="bpr_top25_d1", top_n=25, delta_lanes=1.0)]
        tidal_exog_df, tidal_edges = s2.run_tidal_exogenous_bpr_grid(
            prim, fund, params, baseline_exog, specs=t_specs, **skw,
        )
        tidal_exog_df.to_csv(
            out / "counterfactuals" / "tidal_lane_exogenous_bpr_grid.csv", index=False,
        )
        if not tidal_edges.empty:
            tidal_edges.to_csv(
                out / "counterfactuals" / "tidal_lane_exogenous_edge_detail.csv", index=False,
            )

    meta["lambda_diagnostic_hat"] = lam_diag.lambda_hat
    return meta


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo-root", default=str(DATA_WORK), help="data_work root")
    p.add_argument("--version-id", default="model_input_edgeproj_v2_lcc_gate5_v1")
    p.add_argument(
        "--output-name",
        default="stage2_endogenous_edgeproj_v2_lcc_gate5_bpr_pos05_v1",
    )
    p.add_argument("--alpha", type=float, default=MAINLINE_ALPHA)
    p.add_argument("--beta", type=float, default=MAINLINE_BETA)
    p.add_argument("--quick", action="store_true")
    p.add_argument("--skip-tidal-grid", action="store_true")
    p.add_argument("--max-outer", type=int, default=80)
    p.add_argument("--tol", type=float, default=1e-5)
    p.add_argument(
        "--congestion-form",
        choices=("anchored", "ce_floor", "bpr", "legacy", "density"),
        default=MAINLINE_CONGESTION_FORM,
    )
    p.add_argument(
        "--congestion-lambda",
        type=float,
        default=MAINLINE_LAMBDA_PHYS,
        help="lambda_phys for anchored/density (mainline default 1.1)",
    )
    p.add_argument("--xi-floor-rel", type=float, default=0.0,
                   help="anchor flow floor as a multiple of the median observed anchor flow (anchored forms; 0 = off)")
    p.add_argument(
        "--pair-selection",
        choices=("asym", "score_bpr_st", "score_st", "score_lt"),
        default=MAINLINE_PAIR_SELECTION,
        help="tidal pair rank: pure asymmetry or corridor score",
    )
    p.add_argument(
        "--lambda-mode",
        choices=("placeholder", "diagnostic", "custom"),
        default="placeholder",
        help="legacy params.json lambda only; mainline E6 uses --congestion-lambda",
    )
    p.add_argument("--lambda-value", type=float, default=None)
    p.add_argument(
        "--legacy-diagnostic-two-phase",
        action="store_true",
        help="Robustness: probe diagnostic lambda then re-run with legacy form",
    )
    p.add_argument("--kappa-rule", default="fixed1",
                   help="fixed1: kappa=1 (pre-2026-11-09 runs); spectral: smallest kappa>=1 with rho(A)<=0.95 at min(tau_ff,tau_obs); or a number")
    args = p.parse_args(argv)

    data_work = _data_work_root(args.repo_root)
    out = data_work / "outputs" / args.output_name
    out.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s | %(message)s",
        handlers=[logging.FileHandler(out / "run.log"), logging.StreamHandler()],
    )
    skw_extra = dict(max_outer=args.max_outer, tol=args.tol)
    if args.xi_floor_rel > 0:
        skw_extra["xi_floor_rel"] = args.xi_floor_rel
    skw_extra["kappa_rule"] = args.kappa_rule

    if args.legacy_diagnostic_two_phase:
        args.congestion_form = "legacy"
        args.lambda_mode = "diagnostic"
        LOG.info("Legacy two-phase diagnostic run")
        probe_out = out / "_lambda_probe"
        probe_meta = run_pipeline(
            data_work, args.version_id, probe_out,
            pm.Parameters(), quick=True, skip_tidal_grid=True,
            congestion_form="legacy", congestion_lambda=pm.AA_SEATTLE_DELTA1 / pm.AA_SEATTLE_THETA,
            pair_selection=args.pair_selection, skw_extra=skw_extra,
        )
        lambda_hat = float(probe_meta["lambda_diagnostic_hat"])
        params = _resolve_params(args, lambda_hat_probe=lambda_hat)
        meta = run_pipeline(
            data_work, args.version_id, out, params,
            quick=args.quick, skip_tidal_grid=args.skip_tidal_grid,
            congestion_form="legacy",
            congestion_lambda=float(params.lambda_),
            pair_selection=args.pair_selection, skw_extra=skw_extra,
        )
    else:
        params = _resolve_params(args)
        skw_extra.update({k: v for k, v in solver_overrides_for_params(params).items() if v is not None})
        meta = run_pipeline(
            data_work, args.version_id, out, params,
            quick=args.quick, skip_tidal_grid=args.skip_tidal_grid,
            congestion_form=args.congestion_form,
            congestion_lambda=args.congestion_lambda,
            pair_selection=args.pair_selection, skw_extra=skw_extra,
        )

    LOG.info(
        "Done — %s | form=%s lambda=%.3f | endogenous Wbar=%.4f | median tau/obs=%.3f",
        out, meta["congestion_form"], meta["congestion_lambda"],
        meta["baseline_endogenous"]["Wbar"],
        meta["baseline_endogenous"].get("median_tau_ratio_vs_obs", float("nan")),
    )


if __name__ == "__main__":
    main()
