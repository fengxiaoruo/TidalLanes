"""Run tidal-lane ST/LT diagnostics with congestion-cost decomposition (gate5)."""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import pandas as pd
import numpy as np

DATA_WORK = Path(__file__).resolve().parents[3]
if str(DATA_WORK) not in sys.path:
    sys.path.insert(0, str(DATA_WORK))

from src.model.full_estimation import build_primitives as bp  # noqa: E402
from src.model.full_estimation import invert_fundamentals as inv  # noqa: E402
from src.model.full_estimation import parameters as pm  # noqa: E402
from src.model.full_estimation import stage2_counterfactuals as s2  # noqa: E402
from src.model.full_estimation.equilibrium_solve import resolve_kappa  # noqa: E402
from src.model.full_estimation.equilibrium_solve import (  # noqa: E402
    ANCHORED_FORMS,
    BEIJING_LAMBDA_PHYS,
    MAINLINE_ALPHA,
    MAINLINE_BETA,
    MAINLINE_LAMBDA_PHYS,
    MAINLINE_PAIR_SELECTION,
    compute_observation_anchors,
)
from src.model.full_estimation.run_alpha_robustness_quick_gate5 import (  # noqa: E402
    solver_overrides_for_params,
)
from src.model.full_estimation.tidal_lane_diagnostics import summarise_tidal_pair  # noqa: E402


LOG = logging.getLogger("tidal_diagnostics")


def _data_work_root(arg: str) -> Path:
    p = Path(arg).resolve()
    if (p / "outputs").is_dir() and (p / "src" / "model").is_dir():
        return p
    if (p / "data_work" / "outputs").is_dir():
        return p / "data_work"
    return DATA_WORK


def _skw(fund: inv.InversionResult, *, congestion_form: str, congestion_lambda: float | None = None, **extra) -> dict:
    damp = 0.25 if congestion_form in ANCHORED_FORMS else 0.5
    lam = congestion_lambda
    if lam is None:
        lam = BEIJING_LAMBDA_PHYS if congestion_form != "legacy" else None
    base = dict(
        kappa=fund.kappa,
        chi=fund.chi,
        max_outer=extra.pop("max_outer", 120),
        tol=extra.pop("tol", 1e-5),
        damping=extra.pop("damping", damp),
        congestion_form=congestion_form,
        congestion_lambda=lam if congestion_form != "legacy" else None,
    )
    base.update(extra)
    return base


def run(
    data_work: Path,
    version_id: str,
    out: Path,
    *,
    congestion_form: str = "anchored",
    congestion_lambda: float | None = None,
    pair_selection: str = MAINLINE_PAIR_SELECTION,
    specs: list[tuple[int, float]] | None = None,
    params: pm.Parameters | None = None,
    xi_floor_rel: float = 0.0,
    kappa_rule: str = "fixed1",
    **skw,
) -> pd.DataFrame:
    out.mkdir(parents=True, exist_ok=True)
    prim = bp.load_primitives(data_work / "outputs" / version_id, period="AM")
    params = params or pm.Parameters()
    params.write(out / "parameters.json")
    kappa, kinfo = resolve_kappa(prim, kappa_rule)
    (out / "kappa.json").write_text(json.dumps(kinfo, indent=2))
    fund = inv.invert_fundamentals(prim, params, tol=1.5e-4, max_iter=1800, kappa=kappa)

    anchors = None
    xi_floor = 0.0
    if congestion_form in ANCHORED_FORMS:
        anchors = compute_observation_anchors(
            prim, fund.ubar_theta, fund.abar_theta, params,
            chi=fund.chi, kappa=fund.kappa,
        )
        xi_med = float(np.median(anchors["xi_obs_kl"]))
        xi_floor = float(xi_floor_rel) * xi_med
        (out / "observation_anchors.json").write_text(
            json.dumps({"xi_obs_median": xi_med, "xi_floor_rel": float(xi_floor_rel), "xi_floor": xi_floor,
                        "edges_below_floor_share": float(np.mean(anchors["xi_obs_kl"] < xi_floor))}, indent=2)
        )

    kw_base = _skw(fund, congestion_form=congestion_form, congestion_lambda=congestion_lambda, **skw)
    if anchors is not None:
        kw_base.update(
            xi_obs_kl=anchors["xi_obs_kl"],
            n_obs_kl=anchors["n_obs_kl"],
            tau_obs_anchor=anchors["tau_obs_anchor"],
            xi_floor=xi_floor,
        )

    baseline = s2.solve_baseline_endogenous(prim, fund, params, **kw_base)
    baseline.write(out / "baseline")

    specs = specs or [(25, 1.0), (50, 1.0), (50, 0.5), (100, 1.0)]
    rows: list[dict] = []
    edge_parts: list[pd.DataFrame] = []

    for top_k, delta in specs:
        label = f"top{top_k}_d{delta}"
        LOG.info("Tidal %s — ST + LT", label)
        eq_st, sel = s2.run_tidal_endogenous(
            prim, fund, params, baseline,
            top_k=top_k, delta=delta, horizon="short_run",
            pair_selection=pair_selection, **kw_base,
        )
        eq_lt, _ = s2.run_tidal_endogenous(
            prim, fund, params, baseline,
            top_k=top_k, delta=delta, horizon="long_run",
            pair_selection=pair_selection, **kw_base,
        )
        eq_st.write(out / label / "short_run")
        eq_lt.write(out / label / "long_run")
        sel.to_frame(prim).to_csv(out / label / "selected_pairs.csv", index=False)

        row, _, edges = summarise_tidal_pair(
            spec_label=label,
            top_k=top_k,
            delta_lanes=delta,
            baseline=baseline,
            short_run=eq_st,
            long_run=eq_lt,
            prim=prim,
            params=params,
            abar_theta=fund.abar_theta,
            sel=sel,
        )
        row["congestion_form"] = congestion_form
        lam_used = congestion_lambda if congestion_lambda is not None else (
            BEIJING_LAMBDA_PHYS if congestion_form != "legacy" else params.lambda_
        )
        row["lambda"] = lam_used
        row["pair_selection"] = pair_selection
        rows.append(row)
        edge_parts.append(edges)

    df = pd.DataFrame(rows)
    df.to_csv(out / "tidal_st_lt_congestion_summary.csv", index=False)
    if edge_parts:
        pd.concat(edge_parts, ignore_index=True).to_csv(
            out / "tidal_treated_edge_detail.csv", index=False,
        )

    # Compact ST vs LT table for slides
    compact_cols = [
        "spec", "top_k", "n_pairs",
        "Wbar_pct_change_st", "Wbar_pct_change_lt",
        "avg_physical_time_base_min", "avg_physical_time_st_min", "avg_physical_time_lt_min",
        "physical_time_saved_st_sec_per_commuter", "physical_time_saved_lt_sec_per_commuter",
        "physical_time_saved_st_person_hours", "physical_time_saved_lt_person_hours",
        "direct_st_cost_change_sec", "ge_cost_wedge_sec", "sorting_wedge_sec",
        "total_st_cost_change_sec", "total_lt_cost_change_sec",
        "st_treated_mean_delta_tau_sec" if "st_treated_mean_delta_tau_min" in df.columns else None,
        "lt_treated_mean_delta_tau_min",
        "st_treated_xi_weighted_delta_tau_min", "lt_treated_xi_weighted_delta_tau_min",
    ]
    if "st_treated_mean_delta_tau_min" in df.columns:
        df["st_treated_mean_delta_tau_sec"] = 60.0 * df["st_treated_mean_delta_tau_min"]
        df["lt_treated_mean_delta_tau_sec"] = 60.0 * df["lt_treated_mean_delta_tau_min"]
    compact_cols = [c for c in compact_cols if c and c in df.columns]
    df[compact_cols].to_csv(out / "tidal_st_lt_compact.csv", index=False)
    return df


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo-root", default=str(DATA_WORK), help="data_work root")
    p.add_argument("--version-id", default="model_input_edgeproj_v2_lcc_gate5_v1")
    p.add_argument(
        "--output-name",
        default="tidal_lane_diagnostics_edgeproj_v2_lcc_gate5_bpr_pos05_v1",
    )
    p.add_argument("--alpha", type=float, default=MAINLINE_ALPHA)
    p.add_argument("--beta", type=float, default=MAINLINE_BETA)
    p.add_argument(
        "--congestion-form",
        choices=("anchored", "legacy", "density", "ce_floor", "bpr"),
        default="anchored",
    )
    p.add_argument(
        "--congestion-lambda",
        type=float,
        default=MAINLINE_LAMBDA_PHYS,
        help="lambda_phys for anchored/density (mainline default 1.1)",
    )
    p.add_argument(
        "--p1-calib-json",
        default="",
        help="Optional: load lambda_phys_robustness_lower for robustness run only",
    )
    p.add_argument(
        "--pair-selection",
        choices=("asym", "score_bpr_st", "score_st", "score_lt"),
        default=MAINLINE_PAIR_SELECTION,
    )
    p.add_argument("--max-outer", type=int, default=500)
    p.add_argument(
        "--xi-floor-rel",
        type=float,
        default=0.0,
        help="anchored forms: congestion ignores flow changes below this multiple of the "
             "median observation-anchor flow (0 = off, the mainline)",
    )
    p.add_argument(
        "--only-top100",
        action="store_true",
        help="Mainline shortcut: run top100_d1.0 only (skip top25/50 robustness)",
    )
    p.add_argument("--kappa-rule", default="fixed1",
                   help="fixed1: kappa=1 (pre-2026-11-09 runs); spectral: smallest kappa>=1 with rho(A)<=0.95 at min(tau_ff,tau_obs); or a number")
    p.add_argument("--route-eta", type=float, default=1.0,
                   help="nested Frechet: tau^-theta = R^eta, eta = theta/rho_route (1 = AA mainline)")
    args = p.parse_args(argv)

    data_work = _data_work_root(args.repo_root)
    out = data_work / "outputs" / args.output_name
    out.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(out / "run.log"),
            logging.StreamHandler(),
        ],
    )

    congestion_lambda = args.congestion_lambda
    if args.p1_calib_json:
        calib_path = Path(args.p1_calib_json)
        if not calib_path.is_absolute():
            calib_path = data_work / "outputs" / calib_path
        if calib_path.exists():
            calib = json.loads(calib_path.read_text())
            key = "lambda_phys_robustness_lower"
            if key in calib:
                congestion_lambda = float(calib[key])
                LOG.info("Robustness lambda=%.4f from %s", congestion_lambda, calib_path)

    from dataclasses import replace

    params = replace(
        pm.Parameters(),
        alpha=float(args.alpha),
        beta=float(args.beta),
        alpha_source="Literature mid-range gross production spillover (mainline pos05)",
        beta_source="AA(2022) Seattle beta held",
        route_eta=float(args.route_eta),
        route_eta_source=("AA(2022): route and location dispersion equal (eta=1)" if args.route_eta == 1.0
                          else "calibrated to observed OD gravity semi-elasticity (2026-11-11)"),
    )
    skw = {k: v for k, v in solver_overrides_for_params(params).items() if v is not None}
    skw["max_outer"] = max(args.max_outer, skw.get("max_outer", args.max_outer))

    specs = [(100, 1.0)] if args.only_top100 else None
    df = run(
        data_work, args.version_id, out,
        congestion_form=args.congestion_form,
        congestion_lambda=congestion_lambda,
        pair_selection=args.pair_selection,
        params=params,
        specs=specs,
        xi_floor_rel=args.xi_floor_rel,
        kappa_rule=args.kappa_rule,
        **skw,
    )
    LOG.info("Wrote %s", out)
    print(df[[
        "spec", "Wbar_pct_change_st", "Wbar_pct_change_lt",
        "avg_physical_time_base_min",
        "physical_time_saved_st_sec_per_commuter",
        "physical_time_saved_lt_sec_per_commuter",
        "physical_time_saved_st_person_hours",
        "physical_time_saved_lt_person_hours",
    ]].to_string(index=False))


if __name__ == "__main__":
    main()
