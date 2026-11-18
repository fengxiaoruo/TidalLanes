"""
Numerical uniqueness check for the anchored equilibrium (progress note 2026-11-09, section 6.4)
==============================================================================================
AA (2022) Proposition 1 covers a pure power congestion function only; the mainline uses
max(f, tau_obs dev^beta) (ce_floor) and the comparison form is BPR, so uniqueness is checked
numerically, in the spirit of Almagro et al. (2026, appendix F.5).

1. Multi-start. For the baseline, tidal top-100 short run and tidal top-100 long run, the
   outer fixed point is solved from several starting times: tau_obs (the production start),
   the floor min(tau_ff, tau_obs), 2 tau_obs, and tau_obs * exp(0.3 e) for random e. Inner
   share solves start from uniform shares (production) or, with --random-inner, from random
   shares in every outer iteration. Each solution is compared with the production solution
   (max |d log tau|, max |d log l_R|, |d log l_F|, relative d Wbar).
2. Inner uniqueness. At the production equilibrium times, the share system is solved from
   random initial shares and compared with the uniform start.
3. Monotonicity of the outer map. G(x) = log tau_new(x) for x = log tau (one undamped outer
   step: shares re-solved, market access, traffic, congestion update). A unique fixed point
   of G follows if G(x) - x is strictly monotone decreasing, i.e.
       r = (G(x1) - G(x2)) . (x1 - x2) / |x1 - x2|^2 < 1
   for all x1 != x2. r is evaluated on random pairs around the baseline equilibrium at
   perturbation scales 0.02, 0.1 and 0.3 (log points). Passing every pair is evidence, not
   a proof (Almagro et al. use the same logic for their Assumption 1).
Setup (inversion, anchors, xi floor, kappa rule, solver kwargs) is the same as
run_tidal_lane_diagnostics_gate5.py. Inputs read only.
Output: data_work/outputs/<output-name>/ (refuses to overwrite)
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

DATA_WORK = Path(__file__).resolve().parents[3]
if str(DATA_WORK) not in sys.path:
    sys.path.insert(0, str(DATA_WORK))

from src.model.full_estimation import build_primitives as bp  # noqa: E402
from src.model.full_estimation import equilibrium_solve as es  # noqa: E402
from src.model.full_estimation import invert_fundamentals as inv  # noqa: E402
from src.model.full_estimation import parameters as pm  # noqa: E402
from src.model.full_estimation import stage2_counterfactuals as s2  # noqa: E402
from src.model.full_estimation.counterfactual_levels import select_tidal_lane_pairs_by_rank  # noqa: E402
from src.model.full_estimation.equilibrium_solve import (  # noqa: E402
    ANCHORED_FORMS,
    MAINLINE_PAIR_SELECTION,
    compute_observation_anchors,
    resolve_kappa,
)
from src.model.full_estimation.run_tidal_lane_diagnostics_gate5 import _skw  # noqa: E402

_ORIG_INNER = es._solve_lR_lF_given_tau
_INNER_RNG: dict = {"rng": None}


def _inner_random(tau_inv_theta, ubar_theta, abar_theta, params, chi=1.0, max_iter=400, tol=1e-8):
    """Same iteration as es._solve_lR_lF_given_tau, started from random shares."""
    rng = _INNER_RNG["rng"]
    theta, alpha, beta = params.theta, params.alpha, params.beta
    N = tau_inv_theta.shape[0]
    l_R = rng.dirichlet(np.ones(N)); l_F = rng.dirichlet(np.ones(N))
    EPS = es.EPS
    for it in range(max_iter):
        rhs_R = chi * ubar_theta * (tau_inv_theta @ (abar_theta * (l_F ** (alpha * theta))))
        l_R_new = np.clip(rhs_R, EPS, None) ** (1.0 / (1.0 - beta * theta)); l_R_new /= l_R_new.sum()
        rhs_F = chi * abar_theta * (tau_inv_theta.T @ (ubar_theta * (l_R_new ** (beta * theta))))
        l_F_new = np.clip(rhs_F, EPS, None) ** (1.0 / (1.0 - alpha * theta)); l_F_new /= l_F_new.sum()
        d = max(float(np.max(np.abs(np.log(np.clip(l_R_new, EPS, None)) - np.log(np.clip(l_R, EPS, None))))),
                float(np.max(np.abs(np.log(np.clip(l_F_new, EPS, None)) - np.log(np.clip(l_F, EPS, None))))))
        l_R, l_F = l_R_new, l_F_new
        if d < tol:
            return l_R, l_F, it + 1
    return l_R, l_F, max_iter


def compare(ref, eq) -> dict:
    lg = lambda a: np.log(np.clip(a, es.EPS, None))  # noqa: E731
    return {"converged": bool(eq.converged), "n_outer": int(eq.n_outer),
            "max_dlog_tau": float(np.max(np.abs(lg(eq.tau_min_kl) - lg(ref.tau_min_kl)))),
            "max_dlog_lR": float(np.max(np.abs(lg(eq.l_R) - lg(ref.l_R)))),
            "max_dlog_lF": float(np.max(np.abs(lg(eq.l_F) - lg(ref.l_F)))),
            "rel_dWbar": float(eq.Wbar / ref.Wbar - 1.0), "Wbar": float(eq.Wbar)}


def outer_map(prim, fund, params, kw, n_kl, x):
    """One undamped outer step G(x) = log tau_new at x = log tau (long-run: shares re-solved)."""
    tau = np.exp(x)
    A = es._build_A_matrix(prim.edge_i, prim.edge_j, tau, prim.n_nodes, kappa=kw["kappa"])
    tit = es._compute_tau_inv_theta(A)
    l_R, l_F, _ = es._solve_lR_lF_given_tau(tit, fund.ubar_theta, fund.abar_theta, params, chi=kw["chi"])
    Pi, Pj = es._market_access(tit, l_R, l_F, prim.Lbar, params.theta)
    Xi = (tau ** (-1.0)) * (Pj[prim.edge_i] ** (-params.theta)) * (Pi[prim.edge_j] ** (-params.theta))
    new = es._congestion_tau_update(
        form=kw["congestion_form"], lam=kw["congestion_lambda"], tau_ff=prim.tau_ff_min,
        tau_obs=kw["tau_obs_anchor"], Xi_kl=Xi, n_kl=es._resolve_lanes(n_kl), ell_km=es._edge_lengths_km(prim),
        xi_obs_kl=kw["xi_obs_kl"], n_obs_kl=kw["n_obs_kl"], xi_floor=kw["xi_floor"])
    return np.log(np.clip(new, es.EPS, None))


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--version-id", default="model_input_osm1b_L300_gate5_od2025_v2")
    ap.add_argument("--congestion-form", choices=list(ANCHORED_FORMS), default="ce_floor")
    ap.add_argument("--congestion-lambda", type=float, default=0.25)
    ap.add_argument("--xi-floor-rel", type=float, default=0.01)
    ap.add_argument("--kappa-rule", default="spectral")
    ap.add_argument("--n-random-starts", type=int, default=4)
    ap.add_argument("--n-pairs", type=int, default=60, help="random pairs per perturbation scale")
    ap.add_argument("--max-outer", type=int, default=500)
    ap.add_argument("--seed", type=int, default=20261110)
    ap.add_argument("--output-name", required=True)
    a = ap.parse_args(argv)
    out = DATA_WORK / "outputs" / a.output_name
    if out.exists():
        raise SystemExit(f"{out} exists; choose a new --output-name")
    out.mkdir(parents=True)
    rng = np.random.default_rng(a.seed)

    prim = bp.load_primitives(DATA_WORK / "outputs" / a.version_id, period="AM")
    params = pm.Parameters()
    kappa, kinfo = resolve_kappa(prim, a.kappa_rule)
    fund = inv.invert_fundamentals(prim, params, tol=1.5e-4, max_iter=1800, kappa=kappa)
    anchors = compute_observation_anchors(prim, fund.ubar_theta, fund.abar_theta, params, chi=fund.chi, kappa=fund.kappa)
    xi_floor = a.xi_floor_rel * float(np.median(anchors["xi_obs_kl"]))
    kw = _skw(fund, congestion_form=a.congestion_form, congestion_lambda=a.congestion_lambda, max_outer=a.max_outer)
    kw.update(xi_obs_kl=anchors["xi_obs_kl"], n_obs_kl=anchors["n_obs_kl"],
              tau_obs_anchor=anchors["tau_obs_anchor"], xi_floor=xi_floor)

    base = s2.solve_baseline_endogenous(prim, fund, params, **kw)
    sel = select_tidal_lane_pairs_by_rank(prim, top_k=100, delta=1.0, rank=MAINLINE_PAIR_SELECTION, xi_kl=base.Xi_kl)
    n_cf = prim.n_kl.copy()
    for slow, fast in sel.treated_pairs:
        n_cf[slow] += 1.0
        n_cf[fast] -= 1.0
    tau_obs = prim.tau_obs_min.copy()
    floor = np.minimum(prim.tau_ff_min, tau_obs)
    starts = {"tau_obs": tau_obs, "floor": floor, "2x_tau_obs": 2.0 * tau_obs}
    for k in range(a.n_random_starts):
        starts[f"random{k}"] = tau_obs * np.exp(0.3 * rng.standard_normal(len(tau_obs)))
    scen = {
        "baseline": dict(n_kl_override=None, pin=None),
        "tidal_top100_short_run": dict(n_kl_override=n_cf, pin=(base.l_R, base.l_F)),
        "tidal_top100_long_run": dict(n_kl_override=n_cf, pin=None),
    }
    rows = []
    for sname, s in scen.items():
        extra = {} if s["pin"] is None else dict(l_R_fixed=s["pin"][0], l_F_fixed=s["pin"][1])
        solve = lambda t0, lab: es.solve_equilibrium(  # noqa: E731
            prim, fund.ubar_theta, fund.abar_theta, params, n_kl_override=s["n_kl_override"],
            tau_min_init=t0, endogenous_congestion=True, label=lab, **extra, **kw)
        ref = solve(tau_obs.copy(), "ref")
        for stn, t0 in starts.items():
            for inner in (["uniform", "random"] if stn in ("tau_obs", "random0") and s["pin"] is None else ["uniform"]):
                if inner == "random":
                    _INNER_RNG["rng"] = np.random.default_rng(rng.integers(1e9))
                    es._solve_lR_lF_given_tau = _inner_random
                try:
                    eq = solve(t0.copy(), f"{sname}_{stn}_{inner}")
                finally:
                    es._solve_lR_lF_given_tau = _ORIG_INNER
                rows.append({"scenario": sname, "start": stn, "inner_init": inner,
                             "ref_converged": bool(ref.converged), "ref_Wbar": float(ref.Wbar), **compare(ref, eq)})
                print(json.dumps(rows[-1]), flush=True)
    starts_df = pd.DataFrame(rows)
    starts_df.to_csv(out / "multistart.csv", index=False)

    # inner uniqueness at the baseline equilibrium times
    A = es._build_A_matrix(prim.edge_i, prim.edge_j, base.tau_min_kl, prim.n_nodes, kappa=kw["kappa"])
    tit = es._compute_tau_inv_theta(A)
    lR0, lF0, _ = _ORIG_INNER(tit, fund.ubar_theta, fund.abar_theta, params, chi=kw["chi"])
    inner_rows = []
    for k in range(10):
        _INNER_RNG["rng"] = np.random.default_rng(rng.integers(1e9))
        lR, lF, it = _inner_random(tit, fund.ubar_theta, fund.abar_theta, params, chi=kw["chi"])
        inner_rows.append({"draw": k, "iters": it, "max_dlog_lR": float(np.max(np.abs(np.log(lR / lR0)))),
                           "max_dlog_lF": float(np.max(np.abs(np.log(lF / lF0))))})
    pd.DataFrame(inner_rows).to_csv(out / "inner_shares_random_starts.csv", index=False)

    # monotonicity of the outer map (long run, baseline lanes and tidal lanes)
    mono = []
    x0 = np.log(base.tau_min_kl)
    for lanes_name, lanes in (("baseline", prim.n_kl), ("tidal_top100", n_cf)):
        for scale in (0.02, 0.1, 0.3):
            for k in range(a.n_pairs):
                x1 = x0 + scale * rng.standard_normal(len(x0))
                x2 = x0 + scale * rng.standard_normal(len(x0))
                g1, g2 = outer_map(prim, fund, params, kw, lanes, x1), outer_map(prim, fund, params, kw, lanes, x2)
                dx = x1 - x2
                mono.append({"lanes": lanes_name, "scale": scale, "pair": k,
                             "r": float((g1 - g2) @ dx / (dx @ dx)),
                             "lipschitz_ratio": float(np.linalg.norm(g1 - g2) / np.linalg.norm(dx))})
            m = pd.DataFrame([r for r in mono if r["lanes"] == lanes_name and r["scale"] == scale])
            print(lanes_name, scale, "r max", round(m.r.max(), 4), "lip max", round(m.lipschitz_ratio.max(), 4), flush=True)
    mono_df = pd.DataFrame(mono)
    mono_df.to_csv(out / "outer_map_monotonicity.csv", index=False)

    summ = {
        "version_id": a.version_id, "congestion_form": a.congestion_form, "congestion_lambda": a.congestion_lambda,
        "xi_floor_rel": a.xi_floor_rel, "kappa": kinfo, "baseline_converged": bool(base.converged),
        "multistart_all_converged": bool(starts_df.converged.all()),
        "multistart_max_dlog_tau": float(starts_df.max_dlog_tau.max()),
        "multistart_max_dlog_lR": float(starts_df.max_dlog_lR.max()),
        "multistart_max_abs_rel_dWbar": float(starts_df.rel_dWbar.abs().max()),
        "inner_max_dlog_lR": float(pd.DataFrame(inner_rows).max_dlog_lR.max()),
        "monotonicity": mono_df.groupby(["lanes", "scale"]).agg(r_max=("r", "max"), r_median=("r", "median"),
                                                               lip_max=("lipschitz_ratio", "max"),
                                                               share_r_lt_1=("r", lambda v: float((v < 1).mean())))
                               .reset_index().to_dict(orient="records"),
    }
    (out / "summary.json").write_text(json.dumps(summ, indent=2, default=float))
    print(json.dumps(summ, indent=2, default=float))


if __name__ == "__main__":
    main()
