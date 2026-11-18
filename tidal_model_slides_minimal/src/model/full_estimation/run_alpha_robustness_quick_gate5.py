"""Quick alpha robustness: re-invert + tidal top-K ST/LT vs Seattle (negative) alpha."""
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

from src.model.full_estimation import build_primitives as bp
from src.model.full_estimation import invert_fundamentals as inv
from src.model.full_estimation import parameters as pm
from src.model.full_estimation import stage2_counterfactuals as s2
from src.model.full_estimation.equilibrium_solve import MAINLINE_PAIR_SELECTION, solve_equilibrium
from src.model.full_estimation.phase_metrics_common import (
    DEFAULT_TOP_K,
    DEFAULT_VERSION,
    data_work_root,
    tidal_selection,
    write_json,
)
from src.model.full_estimation.tidal_lane_diagnostics import od_cost_decomposition

LOG = logging.getLogger("alpha_robustness")

SPECS = [
    ("seattle_aa_baseline", -0.12, -0.10, "AA(2022) Seattle replication"),
    ("aa_main_positive", +0.10, -0.30, "AA(2022) main paper central values"),
    ("v3_combes_positive", +0.06, -0.30, "TidalLanes v3 table (Combes-Gobillon alpha)"),
    ("alpha06_seattle_beta", +0.06, -0.10, "Positive alpha only; Seattle beta held"),
]

EPS = 1e-12

# Positive alpha raises share-update sensitivity (1 - alpha*theta smaller).
# Mainline damping=0.25 / tol=1e-5 can stall the outer loop; use conservative updates.
POSITIVE_ALPHA_SOLVER = dict(max_outer=500, damping=0.10, tol=1e-4)
DEFAULT_SOLVER = dict(max_outer=120, tol=1e-5)


def solver_overrides_for_params(params: pm.Parameters, *, max_outer: int | None = None) -> dict:
    """Pick outer-loop kwargs that converge for the given (alpha, beta)."""
    if params.alpha > 0:
        kw = dict(POSITIVE_ALPHA_SOLVER)
    else:
        kw = dict(DEFAULT_SOLVER)
    if max_outer is not None:
        # Never downgrade the positive-alpha budget; CLI default (120) is too small for α>0.
        kw["max_outer"] = max(max_outer, kw["max_outer"])
    return kw


def _sorting_stats(base, lt) -> dict:
    dl_r = np.log(np.clip(lt.l_R, EPS, None)) - np.log(np.clip(base.l_R, EPS, None))
    dl_f = np.log(np.clip(lt.l_F, EPS, None)) - np.log(np.clip(base.l_F, EPS, None))
    d_pi = np.log(np.clip(lt.Pi_i, EPS, None)) - np.log(np.clip(base.Pi_i, EPS, None))
    return {
        "max_abs_log_l_R_change": float(np.max(np.abs(dl_r))),
        "max_abs_log_l_F_change": float(np.max(np.abs(dl_f))),
        "mean_abs_log_l_R_change": float(np.mean(np.abs(dl_r))),
        "mean_abs_log_l_F_change": float(np.mean(np.abs(dl_f))),
        "p95_abs_log_l_R_change": float(np.quantile(np.abs(dl_r), 0.95)),
        "p95_abs_log_l_F_change": float(np.quantile(np.abs(dl_f), 0.95)),
        "max_abs_log_Pi_change": float(np.max(np.abs(d_pi))),
        "residence_dissimilarity_pct": 100.0 * 0.5 * float(np.sum(np.abs(lt.l_R - base.l_R))),
        "workplace_dissimilarity_pct": 100.0 * 0.5 * float(np.sum(np.abs(lt.l_F - base.l_F))),
    }


def _corridor_pct(base, lt, sel) -> dict:
    slow = sel.treated_pairs[:, 0]
    pct = []
    for s in slow:
        pct.append(100.0 * (1.0 - lt.tau_min_kl[s] / max(base.tau_min_kl[s], 1e-12)))
    pct = np.asarray(pct)
    return {
        "corridor_slow_median_pct_save_lt": float(np.median(pct)),
        "corridor_slow_mean_pct_save_lt": float(np.mean(pct)),
    }


def run_spec(
    prim: bp.Primitives,
    params: pm.Parameters,
    *,
    label: str,
    top_k: int,
    max_outer: int,
    out_dir: Path,
    pair_selection: str = MAINLINE_PAIR_SELECTION,
) -> dict:
    solver_kw = solver_overrides_for_params(params, max_outer=max_outer)
    LOG.info(
        "Spec %s  alpha=%.2f beta=%.2f  solver=%s",
        label, params.alpha, params.beta, solver_kw,
    )
    fund = inv.invert_fundamentals(prim, params, tol=1.5e-4, max_iter=1800, kappa=1.0)
    skw = s2.build_congestion_solver_kwargs(prim, fund, params, **solver_kw)
    base = s2.solve_baseline_endogenous(prim, fund, params, **skw)

    sel = tidal_selection(
        prim, top_k=top_k, delta=1.0,
        pair_selection=pair_selection, baseline=base,
    )
    n_cf = prim.n_kl.copy()
    for slow, fast in sel.treated_pairs:
        n_cf[slow] += 1.0
        n_cf[fast] -= 1.0

    eq_st = solve_equilibrium(
        prim, fund.ubar_theta, fund.abar_theta, params,
        n_kl_override=n_cf,
        tau_min_init=prim.tau_obs_min.copy(),
        l_R_fixed=base.l_R, l_F_fixed=base.l_F,
        endogenous_congestion=True,
        label=f"{label}_tidal_st",
        **skw,
    )
    eq_lt = solve_equilibrium(
        prim, fund.ubar_theta, fund.abar_theta, params,
        n_kl_override=n_cf,
        tau_min_init=prim.tau_obs_min.copy(),
        endogenous_congestion=True,
        label=f"{label}_tidal_lt",
        **skw,
    )

    decomp = od_cost_decomposition(base, eq_st, eq_lt, prim, params, fund.abar_theta)
    sort = _sorting_stats(base, eq_lt)
    corr = _corridor_pct(base, eq_lt, sel)

    spec_dir = out_dir / label
    spec_dir.mkdir(parents=True, exist_ok=True)
    base.write(spec_dir / "baseline")
    eq_lt.write(spec_dir / "tidal_lt")
    write_json(spec_dir / "fund_inversion_meta.json", {
        "label": label,
        "alpha": params.alpha,
        "beta": params.beta,
        "chi": fund.chi,
        "kappa": fund.kappa,
        "n_iter": fund.n_iter,
        "fund_converged": fund.converged,
        "resid_R_max_abs": float(np.max(np.abs(fund.resid_R))),
        "resid_F_max_abs": float(np.max(np.abs(fund.resid_F))),
        "solver": solver_kw,
    })
    write_json(spec_dir / "convergence_meta.json", {
        "baseline": {
            "converged": base.converged,
            "n_outer": base.n_outer,
            "max_log_change": base.max_log_change,
        },
        "tidal_st": {
            "converged": eq_st.converged,
            "n_outer": eq_st.n_outer,
            "max_log_change": eq_st.max_log_change,
        },
        "tidal_lt": {
            "converged": eq_lt.converged,
            "n_outer": eq_lt.n_outer,
            "max_log_change": eq_lt.max_log_change,
        },
    })

    row = {
        "spec_label": label,
        "alpha": params.alpha,
        "beta": params.beta,
        "solver_max_outer": solver_kw["max_outer"],
        "solver_damping": skw.get("damping"),
        "solver_tol": solver_kw["tol"],
        "baseline_converged": base.converged,
        "tidal_st_converged": eq_st.converged,
        "tidal_lt_converged": eq_lt.converged,
        "baseline_max_log_change": base.max_log_change,
        "tidal_lt_max_log_change": eq_lt.max_log_change,
        "top_k": top_k,
        "n_pairs": sel.n_treated,
        "baseline_Wbar": base.Wbar,
        "baseline_median_tau_ratio_vs_obs": float(np.median(
            base.tau_min_kl / np.clip(prim.tau_obs_min, 1e-12, None)
        )),
        "Wbar_pct_change_st": 100.0 * (eq_st.Wbar / base.Wbar - 1.0),
        "Wbar_pct_change_lt": 100.0 * (eq_lt.Wbar / base.Wbar - 1.0),
        **corr,
        **decomp,
        **sort,
        "person_min_day_lt_total": decomp["total_lt_cost_change_sec"] / 60.0 * prim.Lbar,
        "person_min_day_sorting_wedge": decomp["sorting_wedge_sec"] / 60.0 * prim.Lbar,
        "one_minus_alpha_theta": 1.0 - params.alpha * params.theta,
        "one_minus_beta_theta": 1.0 - params.beta * params.theta,
        "pair_selection": pair_selection,
    }
    return row


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo-root", default=str(DATA_WORK))
    p.add_argument("--version-id", default=DEFAULT_VERSION)
    p.add_argument("--output-name", default="alpha_robustness_quick_gate5_v1")
    p.add_argument("--top-k", type=int, default=DEFAULT_TOP_K)
    p.add_argument("--only-positive", action="store_true", help="Skip Seattle baseline, run positive specs only")
    p.add_argument("--max-outer", type=int, default=120)
    p.add_argument("--pair-selection", default=MAINLINE_PAIR_SELECTION)
    args = p.parse_args(argv)

    data_work = data_work_root(args.repo_root)
    out = data_work / "outputs" / args.output_name
    out.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s | %(message)s",
        handlers=[logging.FileHandler(out / "run.log"), logging.StreamHandler()],
    )

    prim = bp.load_primitives(data_work / "outputs" / args.version_id, period="AM")
    base_params = pm.Parameters()

    specs = SPECS[1:] if args.only_positive else SPECS
    rows = []
    for label, alpha, beta, src in specs:
        params = replace(
            base_params,
            alpha=alpha,
            beta=beta,
            alpha_source=src,
            beta_source=src,
        )
        rows.append(run_spec(
            prim, params, label=label, top_k=args.top_k,
            max_outer=args.max_outer, out_dir=out,
            pair_selection=args.pair_selection,
        ))

    df = pd.DataFrame(rows)
    df.to_csv(out / "alpha_robustness_comparison.csv", index=False)

    failed = df[
        ~df["baseline_converged"] | ~df["tidal_st_converged"] | ~df["tidal_lt_converged"]
    ]
    if not failed.empty:
        LOG.warning("Non-converged specs:\n%s", failed[
            ["spec_label", "baseline_converged", "tidal_st_converged", "tidal_lt_converged",
             "baseline_max_log_change", "tidal_lt_max_log_change"]
        ].to_string(index=False))

    if not args.only_positive and len(df) >= 2:
        base_row = df[df.spec_label == "seattle_aa_baseline"].iloc[0]
        pos_row = df[df.spec_label == "aa_main_positive"].iloc[0]
        diag = {
            "comparison": "aa_main_positive vs seattle_aa_baseline",
            "top_k": args.top_k,
            "corridor_pct_ratio": float(pos_row["corridor_slow_median_pct_save_lt"] / base_row["corridor_slow_median_pct_save_lt"]),
            "max_log_lR_ratio": float(pos_row["max_abs_log_l_R_change"] / base_row["max_abs_log_l_R_change"]),
            "sorting_wedge_sec_ratio": float(pos_row["sorting_wedge_sec"] / base_row["sorting_wedge_sec"]),
            "Wbar_lt_pct_diff_pp": float(pos_row["Wbar_pct_change_lt"] - base_row["Wbar_pct_change_lt"]),
            "person_min_lt_ratio": float(pos_row["person_min_day_lt_total"] / base_row["person_min_day_lt_total"]),
        }
        write_json(out / "diagnosis.json", diag)

    LOG.info("Done %s", out)
    cols = [
        "spec_label", "alpha", "beta",
        "baseline_converged", "tidal_lt_converged",
        "corridor_slow_median_pct_save_lt",
        "max_abs_log_l_R_change", "sorting_wedge_sec", "Wbar_pct_change_lt",
        "person_min_day_lt_total", "residence_dissimilarity_pct",
    ]
    print(df[cols].to_string(index=False))


if __name__ == "__main__":
    main()
