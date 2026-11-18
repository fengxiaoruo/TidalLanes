"""Policy counterfactual comparison suite (P1–P8) for gate5 mainline."""
from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import replace
from pathlib import Path

import pandas as pd

DATA_WORK = Path(__file__).resolve().parents[3]
if str(DATA_WORK) not in sys.path:
    sys.path.insert(0, str(DATA_WORK))

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
)
from src.model.full_estimation.phase_metrics_common import data_work_root  # noqa: E402
from src.model.full_estimation.run_alpha_robustness_quick_gate5 import (  # noqa: E402
    solver_overrides_for_params,
)

LOG = logging.getLogger("policy_comparison")
DEFAULT_STAGE2 = "stage2_endogenous_edgeproj_v2_lcc_gate5_bpr_pos05_v1"
DEFAULT_OUT = "policy_comparison_gate5_bpr_pos05_v1"


def _load_params(stage2_dir: Path) -> pm.Parameters:
    p = pm.Parameters()
    path = stage2_dir / "parameters.json"
    if path.exists():
        meta = json.loads(path.read_text())
        p = replace(
            p,
            alpha=float(meta.get("alpha", MAINLINE_ALPHA)),
            beta=float(meta.get("beta", MAINLINE_BETA)),
            theta=float(meta.get("theta", p.theta)),
        )
    else:
        p = replace(p, alpha=MAINLINE_ALPHA, beta=MAINLINE_BETA)
    return p


def _skw(prim, fund, params, *, congestion_lambda: float, congestion_form: str = MAINLINE_CONGESTION_FORM,
         xi_floor_rel: float = 0.0) -> dict:
    kw = s2.build_congestion_solver_kwargs(
        prim, fund, params,
        congestion_form=congestion_form,
        congestion_lambda=congestion_lambda,
        xi_floor_rel=xi_floor_rel,
        **{k: v for k, v in solver_overrides_for_params(params).items() if v is not None},
    )
    return kw


def run_policies(
    data_work: Path,
    stage2_dir: Path,
    out: Path,
    *,
    version_id: str,
    pair_selection: str,
    congestion_lambda: float,
    horizons: list[str],
    congestion_form: str = MAINLINE_CONGESTION_FORM,
    xi_floor_rel: float = 0.0,
    kappa_rule: str = "fixed1",
    pricing_impl: str = "auto",
    pricing_only: bool = False,
) -> pd.DataFrame:
    out.mkdir(parents=True, exist_ok=True)
    prim = bp.load_primitives(data_work / "outputs" / version_id, period="AM")
    params = _load_params(stage2_dir)
    kappa, kinfo = resolve_kappa(prim, kappa_rule)
    (out / "kappa.json").write_text(json.dumps(kinfo, indent=2))
    fund = inv.invert_fundamentals(prim, params, tol=1.5e-4, max_iter=1800, kappa=kappa)
    kw = _skw(prim, fund, params, congestion_lambda=congestion_lambda,
              congestion_form=congestion_form, xi_floor_rel=xi_floor_rel)
    # P6/P7 add the toll to tau_ff, which acts as a toll only under the anchored form
    # max(tau_ff, .). Under ce_floor / bpr the floor is min(tau_ff, tau_obs) (the toll is
    # capped by tau_obs) and bpr also rescales its markup, and the P6 toll lambda*tau is the
    # anchored marginal external cost. Pricing is therefore skipped for the other forms.
    # 2026-11-10: pricing_impl 'additive' adds the toll to the perceived cost
    # (s2.run_pricing_additive) and works for every anchored form; 'auto' keeps the
    # legacy tau_ff shift for 'anchored' (reproduces earlier runs) and uses additive otherwise.
    if pricing_impl == "auto":
        pricing_impl = "legacy" if congestion_form == "anchored" else "additive"
    run_pricing = pricing_impl == "additive" or congestion_form == "anchored"
    baseline = s2.solve_baseline_endogenous(prim, fund, params, **kw)

    rows: list[dict] = []
    p2_lane_km = 0.0

    for horizon in horizons:
        if not pricing_only:
          # P1 tidal
          eq, sel = s2.run_tidal_endogenous(
              prim, fund, params, baseline,
              top_k=100, delta=1.0, horizon=horizon,
              pair_selection=pair_selection, **kw,
          )
          row = s2.summarise_vs_baseline(baseline, eq, prim, params, fund.abar_theta)
          row.update({
              "policy_id": "P1_tidal", "horizon": horizon,
              "lane_km_added": 0.0, "pair_selection": pair_selection,
              "congestion_lambda": congestion_lambda,
          })
          rows.append(row)

          # P2 same-corridor expansion
          eq2, sel2 = s2.run_road_expansion_same_corridor(
              prim, fund, params, baseline,
              top_k=100, pair_selection=pair_selection, horizon=horizon, **kw,
          )
          n_cf = apply_bidirectional_lane_delta(prim, sel2, delta=1.0)
          p2_lane_km = float(sum(
              (n_cf[e] - prim.n_kl[e]) * prim.ell_kl_km[e] for e in range(len(prim.n_kl))
          ))
          row = s2.summarise_vs_baseline(baseline, eq2, prim, params, fund.abar_theta)
          row.update({
              "policy_id": "P2_expansion_same_corridor", "horizon": horizon,
              "lane_km_added": p2_lane_km, "congestion_lambda": congestion_lambda,
          })
          rows.append(row)

          # P3 qdelay expansion — compute its OWN added lane-km.
          # (bug fix: previously reused p2_lane_km; P3 selects a different edge set
          #  via q*delay, so its lane-km budget differs from the same-corridor P2.)
          eq3, sel3 = s2.run_road_expansion_qdelay_topk(
              prim, fund, params, baseline, top_k=100, horizon=horizon, **kw,
          )
          n_cf3 = apply_bidirectional_lane_delta(prim, sel3, delta=1.0)
          p3_lane_km = float(sum(
              (n_cf3[e] - prim.n_kl[e]) * prim.ell_kl_km[e] for e in range(len(prim.n_kl))
          ))
          row = s2.summarise_vs_baseline(baseline, eq3, prim, params, fund.abar_theta)
          row.update({
              "policy_id": "P3_expansion_qdelay", "horizon": horizon,
              "lane_km_added": p3_lane_km, "congestion_lambda": congestion_lambda,
          })
          rows.append(row)

          # P4 uniform expansion matched to P2 lane-km.
          # Pin l_R/l_F for short_run so P4/P5 align with the P1-P3 horizon convention
          # (bug fix: uniform CF3 was previously unpinned, i.e. always long-run).
          pin_kw = (
              dict(l_R_fixed=baseline.l_R, l_F_fixed=baseline.l_F)
              if horizon == "short_run" else {}
          )
          fac = uniform_expansion_factor_for_lane_budget(prim, p2_lane_km)
          eq4 = run_cf3_uniform(
              prim, params, fund.ubar_theta, fund.abar_theta, factor=fac,
              **pin_kw, **kw,
          )
          row = s2.summarise_vs_baseline(baseline, eq4, prim, params, fund.abar_theta)
          row.update({
              "policy_id": "P4_expansion_uniform_matched", "horizon": horizon,
              "lane_km_added": p2_lane_km, "expansion_factor": fac,
              "congestion_lambda": congestion_lambda,
          })
          rows.append(row)

          # P5 CF3 uniform speed ceiling (same horizon pin as P4)
          eq5 = run_cf3_uniform(
              prim, params, fund.ubar_theta, fund.abar_theta, factor=1.10,
              **pin_kw, **kw,
          )
          row = s2.summarise_vs_baseline(baseline, eq5, prim, params, fund.abar_theta)
          row.update({
              "policy_id": "P5_uniform_speed_1.10", "horizon": horizon,
              "lane_km_added": float("nan"), "congestion_lambda": congestion_lambda,
          })
          rows.append(row)

        if not run_pricing:
            continue
        kw_policy = {k: v for k, v in kw.items() if k != "congestion_lambda"}
        if pricing_impl == "additive":
            eq6, meta6 = s2.run_pricing_additive(prim, fund, params, baseline, policy="first_best",
                                                 horizon=horizon, **kw)
            eq7, meta7 = s2.run_pricing_additive(prim, fund, params, baseline, policy="cordon",
                                                 horizon=horizon, **kw)
            eq6b, meta6b = s2.run_pricing_additive(prim, fund, params, baseline, policy="baseline_pigou",
                                                   horizon=horizon, **kw)
        else:
            eq6, meta6 = s2.run_congestion_pricing_first_best(
                prim, fund, params, baseline, horizon=horizon,
                congestion_lambda=congestion_lambda, **kw_policy,
            )
            eq7, meta7 = s2.run_cordon_pricing(
                prim, fund, params, baseline, horizon=horizon, **kw,
            )
        pricing_rows = [("P6_first_best_pricing", eq6, meta6), ("P7_cordon_pricing", eq7, meta7)]
        if pricing_impl == "additive":
            pricing_rows.append(("P6b_baseline_pigou_fixed_toll", eq6b, meta6b))
        for pid, eqp, metap in pricing_rows:
            row = s2.summarise_vs_baseline(baseline, eqp, prim, params, fund.abar_theta)
            w_real = s2.realized_welfare_excl_toll(
                prim, fund, params, eqp, congestion_lambda=congestion_lambda,
                xi_obs_kl=kw["xi_obs_kl"], n_obs_kl=kw["n_obs_kl"],
                tau_obs_anchor=kw["tau_obs_anchor"], congestion_form=congestion_form,
                xi_floor=kw.get("xi_floor", 0.0),
            )
            row.update({
                "policy_id": pid, "horizon": horizon, "converged": bool(eqp.converged),
                "n_outer": int(eqp.n_outer), "max_log_change": float(eqp.max_log_change),
                "Wbar_pct_change_realized": 100.0 * (w_real / baseline.Wbar - 1.0),
                **metap,
            })
            rows.append(row)

    df = pd.DataFrame(rows)
    # Headline welfare uses REALIZED travel cost (toll is a lump-sum-rebated
    # transfer, not a resource cost). For toll-free policies realized == the raw
    # model Wbar; only P6/P7 set a distinct realized value above.
    if "Wbar_pct_change_realized" not in df.columns:
        df["Wbar_pct_change_realized"] = float("nan")
    df["Wbar_pct_change_perceived"] = df["Wbar_pct_change"]
    df["Wbar_pct_change_realized"] = df["Wbar_pct_change_realized"].fillna(df["Wbar_pct_change"])
    df.to_csv(out / "results_by_policy.csv", index=False)

    # Cost-effectiveness is a LONG-RUN object: select LT rows explicitly rather
    # than averaging across horizons (bug fix: prior code took a horizon mean but
    # labelled the column _lt).
    if "lane_km_added" not in df.columns:
        df["lane_km_added"] = float("nan")
    lt = df[df["horizon"] == "long_run"]
    if lt.empty:
        lt = df
    ce = lt.groupby("policy_id").agg(
        Wbar_pct_change_lt=("Wbar_pct_change_realized", "mean"),
        Wbar_pct_change_lt_perceived=("Wbar_pct_change_perceived", "mean"),
        lane_km_added=("lane_km_added", "first"),
    ).reset_index()
    ce["Wbar_per_lane_km"] = ce["Wbar_pct_change_lt"] / ce["lane_km_added"].replace(0, float("nan"))
    ce.to_csv(out / "cost_effectiveness.csv", index=False)

    manifest = {
        "version_id": version_id,
        "stage2_dir": str(stage2_dir),
        "pair_selection": pair_selection,
        "alpha": params.alpha,
        "beta": params.beta,
        "congestion_lambda": congestion_lambda,
        "congestion_form": congestion_form,
        "xi_floor_rel": xi_floor_rel,
        "kappa": fund.kappa,
        "kappa_rule": kappa_rule,
        "policies": ([] if pricing_only else ["P1", "P2", "P3", "P4", "P5"]) + (["P6", "P7"] if run_pricing else []),
        "pricing_impl": pricing_impl if run_pricing else None,
        "pricing_skipped_reason": None if run_pricing else
            "legacy P6/P7 implement the toll through tau_ff, valid only for the anchored form",
    }
    (out / "policies_manifest.json").write_text(json.dumps(manifest, indent=2, default=float))
    return df


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo-root", default=str(DATA_WORK))
    p.add_argument("--version-id", default="model_input_edgeproj_v2_lcc_gate5_v1")
    p.add_argument("--stage2-dir", default=DEFAULT_STAGE2)
    p.add_argument("--output-name", default=DEFAULT_OUT)
    p.add_argument("--pair-selection", default=MAINLINE_PAIR_SELECTION)
    p.add_argument("--congestion-lambda", type=float, default=MAINLINE_LAMBDA_PHYS)
    p.add_argument("--horizons", default="short_run,long_run")
    p.add_argument("--congestion-form", choices=("anchored", "ce_floor", "bpr"), default=MAINLINE_CONGESTION_FORM)
    p.add_argument("--xi-floor-rel", type=float, default=0.0)
    p.add_argument("--kappa-rule", default="fixed1",
                   help="fixed1: kappa=1 (pre-2026-11-09 runs); spectral: smallest kappa>=1 with rho(A)<=0.95 at min(tau_ff,tau_obs); or a number")
    p.add_argument("--pricing-impl", choices=("auto", "legacy", "additive"), default="auto",
                   help="auto: legacy tau_ff shift for anchored, additive perceived-cost toll otherwise")
    p.add_argument("--pricing-only", action="store_true", help="run P6/P7 only")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s | %(message)s")
    data_work = data_work_root(args.repo_root)
    stage2_dir = Path(args.stage2_dir)
    if not stage2_dir.is_absolute():
        stage2_dir = data_work / "outputs" / stage2_dir
    out = data_work / "outputs" / args.output_name
    horizons = [h.strip() for h in args.horizons.split(",") if h.strip()]

    df = run_policies(
        data_work, stage2_dir, out,
        version_id=args.version_id,
        pair_selection=args.pair_selection,
        congestion_lambda=args.congestion_lambda,
        horizons=horizons,
        congestion_form=args.congestion_form,
        xi_floor_rel=args.xi_floor_rel,
        kappa_rule=args.kappa_rule,
        pricing_impl=args.pricing_impl,
        pricing_only=args.pricing_only,
    )
    LOG.info("Wrote %d policy rows to %s", len(df), out)
    print(df[["policy_id", "horizon", "Wbar_pct_change_realized", "Wbar_pct_change_perceived"]].to_string(index=False))


if __name__ == "__main__":
    main()
