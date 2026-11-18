"""Theta sensitivity on gate5 mainline: full re-invert + baseline + tidal ST/LT per theta.

Theta is not a table-only knob — each value requires:
  - tau_ij^{-theta} / link accessibility A_kl = t_kl^{-theta}
  - alpha*theta and beta*theta in agglomeration feedback
  - Wbar = W_theta^{1/theta} welfare transform

Default grid: {3, 5, 6.83, 8, 10} with tidal top100 delta=1.0 (headline mainline spec).
Outputs go to a separate version line (does not overwrite stage2 / tidal mainline).
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import replace
from pathlib import Path

import numpy as np
import pandas as pd

DATA_WORK = Path(__file__).resolve().parents[3]
if str(DATA_WORK) not in sys.path:
    sys.path.insert(0, str(DATA_WORK))

from src.model.full_estimation import build_primitives as bp  # noqa: E402
from src.model.full_estimation import invert_fundamentals as inv  # noqa: E402
from src.model.full_estimation import parameters as pm  # noqa: E402
from src.model.full_estimation import stage2_counterfactuals as s2  # noqa: E402
from src.model.full_estimation.equilibrium_solve import (  # noqa: E402
    MAINLINE_ALPHA,
    MAINLINE_BETA,
    MAINLINE_CONGESTION_FORM,
    MAINLINE_LAMBDA_PHYS,
    MAINLINE_PAIR_SELECTION,
)
from src.model.full_estimation.phase_metrics_common import (  # noqa: E402
    DEFAULT_VERSION,
    data_work_root,
    write_json,
)
from src.model.full_estimation.run_alpha_robustness_quick_gate5 import (  # noqa: E402
    _sorting_stats,
    solver_overrides_for_params,
)
from src.model.full_estimation.tidal_lane_diagnostics import summarise_tidal_pair  # noqa: E402

LOG = logging.getLogger("theta_sensitivity")

DEFAULT_THETA_GRID = (3.0, 5.0, pm.AA_SEATTLE_THETA, 8.0, 10.0)
EPS = 1e-12


def _params_for_theta(theta: float) -> pm.Parameters:
    return replace(
        pm.Parameters(),
        theta=float(theta),
        alpha=float(MAINLINE_ALPHA),
        beta=float(MAINLINE_BETA),
        lambda_=pm.AA_SEATTLE_DELTA1 / float(theta),
        theta_source=f"theta sensitivity grid (baseline AA={pm.AA_SEATTLE_THETA})",
        alpha_source="Literature mid-range gross production spillover (mainline pos05)",
        beta_source="AA(2022) Seattle beta held",
        lambda_source=f"LEGACY exog-only: delta1/theta with delta1={pm.AA_SEATTLE_DELTA1}",
    )


def run_theta_spec(
    prim: bp.Primitives,
    theta: float,
    *,
    top_k: int,
    delta_lanes: float,
    out_dir: Path,
    pair_selection: str,
    max_outer: int,
) -> dict:
    params = _params_for_theta(theta)
    spec_dir = out_dir / f"theta_{theta:g}".replace(".", "p")
    spec_dir.mkdir(parents=True, exist_ok=True)
    params.write(spec_dir / "parameters.json")

    LOG.info("theta=%.4f — invert fundamentals", theta)
    fund = inv.invert_fundamentals(prim, params, tol=1.5e-4, max_iter=1800, kappa=1.0)
    fund.write(spec_dir / "fundamentals")

    solver_kw = solver_overrides_for_params(params, max_outer=max_outer)
    skw = s2.build_congestion_solver_kwargs(
        prim,
        fund,
        params,
        congestion_form=MAINLINE_CONGESTION_FORM,
        congestion_lambda=MAINLINE_LAMBDA_PHYS,
        **solver_kw,
    )

    LOG.info("theta=%.4f — baseline endogenous", theta)
    baseline = s2.solve_baseline_endogenous(prim, fund, params, **skw)
    baseline.write(spec_dir / "baseline_endogenous")

    label = f"top{top_k}_d{delta_lanes}"
    LOG.info("theta=%.4f — tidal %s ST/LT", theta, label)
    eq_st, sel = s2.run_tidal_endogenous(
        prim,
        fund,
        params,
        baseline,
        top_k=top_k,
        delta=delta_lanes,
        horizon="short_run",
        pair_selection=pair_selection,
        **skw,
    )
    eq_lt, _ = s2.run_tidal_endogenous(
        prim,
        fund,
        params,
        baseline,
        top_k=top_k,
        delta=delta_lanes,
        horizon="long_run",
        pair_selection=pair_selection,
        **skw,
    )
    eq_st.write(spec_dir / label / "short_run")
    eq_lt.write(spec_dir / label / "long_run")
    sel.to_frame(prim).to_csv(spec_dir / label / "selected_pairs.csv", index=False)

    row, decomp, edges = summarise_tidal_pair(
        spec_label=label,
        top_k=top_k,
        delta_lanes=delta_lanes,
        baseline=baseline,
        short_run=eq_st,
        long_run=eq_lt,
        prim=prim,
        params=params,
        abar_theta=fund.abar_theta,
        sel=sel,
    )
    sort_lt = _sorting_stats(baseline, eq_lt)
    edges.to_csv(spec_dir / label / "treated_edge_detail.csv", index=False)

    meta = {
        "theta": float(theta),
        "fund_converged": fund.converged,
        "fund_n_iter": fund.n_iter,
        "fund_max_log_change": fund.max_log_change,
        "fund_resid_R_max_abs": float(np.max(np.abs(fund.resid_R))),
        "fund_resid_F_max_abs": float(np.max(np.abs(fund.resid_F))),
        "baseline_converged": baseline.converged,
        "baseline_n_outer": baseline.n_outer,
        "baseline_max_log_change": baseline.max_log_change,
        "baseline_median_tau_ratio_vs_obs": float(
            np.median(baseline.tau_min_kl / np.clip(prim.tau_obs_min, EPS, None))
        ),
        "tidal_st_converged": eq_st.converged,
        "tidal_lt_converged": eq_lt.converged,
        "tidal_st_n_outer": eq_st.n_outer,
        "tidal_lt_n_outer": eq_lt.n_outer,
        "one_minus_alpha_theta": 1.0 - params.alpha * params.theta,
        "one_minus_beta_theta": 1.0 - params.beta * params.theta,
    }
    write_json(spec_dir / "run_meta.json", meta)

    return {
        "theta": float(theta),
        "top_k": top_k,
        "delta_lanes": delta_lanes,
        "pair_selection": pair_selection,
        "baseline_Wbar": baseline.Wbar,
        "Wbar_pct_change_st": row["Wbar_pct_change_st"],
        "Wbar_pct_change_lt": row["Wbar_pct_change_lt"],
        "lt_route_cost_sec": decomp["total_lt_cost_change_sec"],
        "st_route_cost_sec": decomp["total_st_cost_change_sec"],
        "direct_st_cost_change_sec": decomp["direct_st_cost_change_sec"],
        "sorting_wedge_sec": decomp["sorting_wedge_sec"],
        "person_min_day_lt_total": decomp["total_lt_cost_change_sec"] / 60.0 * prim.Lbar,
        "residence_dissimilarity_pct": sort_lt["residence_dissimilarity_pct"],
        "workplace_dissimilarity_pct": sort_lt["workplace_dissimilarity_pct"],
        "max_abs_log_residence_change": sort_lt["max_abs_log_l_R_change"],
        "max_abs_log_workplace_change": sort_lt["max_abs_log_l_F_change"],
        "p95_abs_log_residence_change": sort_lt["p95_abs_log_l_R_change"],
        "p95_abs_log_workplace_change": sort_lt["p95_abs_log_l_F_change"],
        "od_dissimilarity_lt_pct": decomp["od_dissimilarity_lt_vs_base_pct"],
        **{k: meta[k] for k in meta if k.endswith("_converged") or k.endswith("_max_log_change")},
        "fund_converged": meta["fund_converged"],
        "baseline_n_outer": meta["baseline_n_outer"],
        "tidal_lt_n_outer": meta["tidal_lt_n_outer"],
    }


def _appendix_table(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out = out.sort_values("theta")
    out["theta_label"] = out["theta"].map(lambda x: f"{x:g}")
    return out[
        [
            "theta_label",
            "Wbar_pct_change_st",
            "Wbar_pct_change_lt",
            "lt_route_cost_sec",
            "person_min_day_lt_total",
            "residence_dissimilarity_pct",
            "workplace_dissimilarity_pct",
            "max_abs_log_residence_change",
            "max_abs_log_workplace_change",
            "baseline_converged",
            "tidal_st_converged",
            "tidal_lt_converged",
            "fund_converged",
        ]
    ]


def _write_appendix_markdown(df: pd.DataFrame, path: Path, *, version_id: str, top_k: int) -> None:
    appendix = _appendix_table(df)
    lines = [
        "# Theta sensitivity — gate5 mainline (robustness appendix draft)",
        "",
        f"- Input bundle: `{version_id}`",
        f"- Congestion: `{MAINLINE_CONGESTION_FORM}`, lambda_phys={MAINLINE_LAMBDA_PHYS}",
        f"- Alpha/beta: {MAINLINE_ALPHA} / {MAINLINE_BETA}",
        f"- Tidal: top{top_k}, delta=1.0, pair_selection=`{MAINLINE_PAIR_SELECTION}`",
        "- Each theta: re-invert fundamentals, re-solve baseline, tidal ST/LT.",
        "",
        "| theta | ST Wbar | LT Wbar | LT route cost (s) | person-min/day | resident dissim (%) | job dissim (%) |",
        "|------:|--------:|--------:|------------------:|---------------:|--------------------:|---------------:|",
    ]
    for _, r in appendix.iterrows():
        lines.append(
            f"| {r['theta_label']} | {r['Wbar_pct_change_st']:+.3f}% | "
            f"{r['Wbar_pct_change_lt']:+.3f}% | {r['lt_route_cost_sec']:+.3f} | "
            f"{r['person_min_day_lt_total']:,.0f} | {r['residence_dissimilarity_pct']:.3f} | "
            f"{r['workplace_dissimilarity_pct']:.3f} |"
        )
    lines.extend(
        [
            "",
            "Convergence flags in `theta_sensitivity_comparison.csv`.",
            "",
            "Slide note: theta enters commuting-cost sensitivity, tau/access objects, "
            "alpha/beta feedback, and the Wbar transform — not a post-hoc rescaling.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo-root", default=str(DATA_WORK))
    p.add_argument("--version-id", default=DEFAULT_VERSION)
    p.add_argument("--output-name", default="theta_sensitivity_gate5_bpr_pos05_v1")
    p.add_argument(
        "--theta-grid",
        default=",".join(str(x) for x in DEFAULT_THETA_GRID),
        help="Comma-separated theta values",
    )
    p.add_argument("--top-k", type=int, default=100)
    p.add_argument("--delta-lanes", type=float, default=1.0)
    p.add_argument("--pair-selection", default=MAINLINE_PAIR_SELECTION)
    p.add_argument("--max-outer", type=int, default=500)
    args = p.parse_args(argv)

    data_work = data_work_root(args.repo_root)
    out = data_work / "outputs" / args.output_name
    out.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s | %(message)s",
        handlers=[logging.FileHandler(out / "run.log"), logging.StreamHandler()],
    )

    theta_grid = [float(x.strip()) for x in args.theta_grid.split(",") if x.strip()]
    prim = bp.load_primitives(data_work / "outputs" / args.version_id, period="AM")
    bp.write_primitives_summary(prim, out / "primitives")

    package_meta = {
        "version_id": args.version_id,
        "theta_grid": theta_grid,
        "top_k": args.top_k,
        "delta_lanes": args.delta_lanes,
        "pair_selection": args.pair_selection,
        "congestion_form": MAINLINE_CONGESTION_FORM,
        "congestion_lambda": MAINLINE_LAMBDA_PHYS,
        "alpha": MAINLINE_ALPHA,
        "beta": MAINLINE_BETA,
        "Lbar": prim.Lbar,
        "note": "Separate robustness line; does not overwrite stage2/tidal mainline outputs.",
    }
    write_json(out / "package_meta.json", package_meta)

    rows = []
    for theta in theta_grid:
        rows.append(
            run_theta_spec(
                prim,
                theta,
                top_k=args.top_k,
                delta_lanes=args.delta_lanes,
                out_dir=out,
                pair_selection=args.pair_selection,
                max_outer=args.max_outer,
            )
        )

    df = pd.DataFrame(rows).sort_values("theta")
    df.to_csv(out / "theta_sensitivity_comparison.csv", index=False)
    appendix = _appendix_table(df)
    appendix.to_csv(out / "theta_sensitivity_appendix_table.csv", index=False)
    _write_appendix_markdown(
        df,
        out / "THETA_SENSITIVITY_APPENDIX.md",
        version_id=args.version_id,
        top_k=args.top_k,
    )

    failed = df[
        ~df["fund_converged"]
        | ~df["baseline_converged"]
        | ~df["tidal_st_converged"]
        | ~df["tidal_lt_converged"]
    ]
    if not failed.empty:
        LOG.warning(
            "Non-converged theta values:\n%s",
            failed[["theta", "fund_converged", "baseline_converged", "tidal_st_converged", "tidal_lt_converged"]].to_string(
                index=False
            ),
        )

    LOG.info("Done %s", out)
    print(
        appendix[
            [
                "theta_label",
                "Wbar_pct_change_st",
                "Wbar_pct_change_lt",
                "lt_route_cost_sec",
                "residence_dissimilarity_pct",
                "workplace_dissimilarity_pct",
            ]
        ].to_string(index=False)
    )


if __name__ == "__main__":
    main()
