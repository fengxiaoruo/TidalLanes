"""Multi-start and tight-tolerance check of the long-run policy equilibria (2026-11-14).

Question: at beta_c = 0.35 is P7 (cordon toll, LT W-bar 0.424%) really above P3 (0.419%)
and P2 (0.414%), or are they tied within solver error? The tidal case was checked in
run_beta_c_rerun_checks.py; this script does the same for P1, P2, P3, P6, P6b, P7.

Setup is exactly run_policy_comparison_gate5.run_policies (same bundle, stage-2 params,
spectral kappa, ce_floor, floor 1%, score_bpr_st, solver kwargs). Each policy is solved
through its unchanged stage2_counterfactuals function; only the call to
``solve_equilibrium`` is intercepted to swap the starting link times and the tolerance:

  start S  -> tau_min_init = S + toll0  (toll0 = initial toll for P6/P6b/P7, 0 otherwise;
                                        'production' keeps the function's own init)
  P6 only  -> extra starts with the successive-average toll started at 0 and 2x baseline MEC
  tolerance 'production' = solver kwargs (1e-4, 500 outer); 'tight' = 1e-7 (max_outer cap)

Welfare: W-bar % change (P1-P3) and the route-choice realized welfare (P6/P6b/P7), the
same columns used in the policy comparison. Long run only (the pricing welfare formula
holds only in the long run). Existing modules are imported, never modified.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

DATA_WORK = Path(__file__).resolve().parents[3]
if str(DATA_WORK) not in sys.path:
    sys.path.insert(0, str(DATA_WORK))

from src.model.full_estimation import build_primitives as bp  # noqa: E402
from src.model.full_estimation import equilibrium_solve as es  # noqa: E402
from src.model.full_estimation import invert_fundamentals as inv  # noqa: E402
from src.model.full_estimation import run_policy_comparison_gate5 as rpc  # noqa: E402
from src.model.full_estimation import stage2_counterfactuals as s2  # noqa: E402
from src.model.full_estimation.equilibrium_solve import resolve_kappa, spectral_radius_A  # noqa: E402

OUT = DATA_WORK / "outputs"
_ORIG_SOLVE = s2.solve_equilibrium
_CTRL: dict = {}


def _patched_solve(prim, ubar, abar, params, **k):
    c = _CTRL
    if c:
        init = np.asarray(k.get("tau_min_init"), dtype=float)
        toll0 = 0.0
        if k.get("toll_kl") is not None:
            toll0 = np.asarray(k["toll_kl"], dtype=float)
            if k.get("toll_pigou") and c.get("toll_scale") is not None:
                k["toll_kl"] = c["toll_scale"] * toll0
        if c.get("start") is not None:
            k["tau_min_init"] = c["start"] + toll0
        else:
            k["tau_min_init"] = init
        k["tol"] = c["tol"]
        k["max_outer"] = c["max_outer"]
        c["used_init"] = np.asarray(k["tau_min_init"]).copy()
    return _ORIG_SOLVE(prim, ubar, abar, params, **k)


def main(argv=None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--version-id", default="model_input_osm1b_L300_gate5_od2025_v2_alldaytt0709")
    ap.add_argument("--stage2-dir", default="stage2_endogenous_osm1b_ce_floor_b025_fl001_ks_od2025_v1")
    ap.add_argument("--congestion-lambda", type=float, required=True)
    ap.add_argument("--xi-floor-rel", type=float, default=0.01)
    ap.add_argument("--kappa-rule", default="spectral")
    ap.add_argument("--pair-selection", default="score_bpr_st")
    ap.add_argument("--policies", default="P7,P3,P2,P1,P6b,P6")
    ap.add_argument("--n-random", type=int, default=2)
    ap.add_argument("--tight-tol", type=float, default=1e-7)
    ap.add_argument("--tight-max-outer", type=int, default=4000)
    ap.add_argument("--tight-max-outer-p6", type=int, default=20000)
    ap.add_argument("--seed", type=int, default=20261114)
    ap.add_argument("--output-name", required=True)
    a = ap.parse_args(argv)

    out = OUT / a.output_name
    if out.exists():
        raise SystemExit(f"{out} exists; refusing to overwrite")
    out.mkdir(parents=True)
    (out / "command.txt").write_text(" ".join([sys.executable] + sys.argv) + "\n")

    prim = bp.load_primitives(OUT / a.version_id, period="AM")
    params = rpc._load_params(OUT / a.stage2_dir)
    kappa, kinfo = resolve_kappa(prim, a.kappa_rule)
    fund = inv.invert_fundamentals(prim, params, tol=1.5e-4, max_iter=1800, kappa=kappa)
    kw = rpc._skw(prim, fund, params, congestion_lambda=a.congestion_lambda,
                  congestion_form="ce_floor", xi_floor_rel=a.xi_floor_rel)
    baseline = s2.solve_baseline_endogenous(prim, fund, params, **kw)
    (out / "setup.json").write_text(json.dumps({
        "congestion_lambda_used": kw["congestion_lambda"], "congestion_form": kw["congestion_form"],
        "xi_floor": kw.get("xi_floor"), "kappa": kinfo, "theta": params.theta, "alpha": params.alpha,
        "beta_amenity": params.beta, "route_eta": params.route_eta,
        "production_tol": kw["tol"], "production_max_outer": kw["max_outer"], "damping": kw["damping"],
        "baseline_converged": bool(baseline.converged), "baseline_Wbar": float(baseline.Wbar),
        "tight_tol": a.tight_tol, "tight_max_outer": a.tight_max_outer,
        "tight_max_outer_p6": a.tight_max_outer_p6,
    }, indent=2, default=float))
    print(f"lambda={kw['congestion_lambda']} tol={kw['tol']} max_outer={kw['max_outer']} "
          f"damping={kw['damping']} base conv={baseline.converged}", flush=True)

    tau_obs = prim.tau_obs_min.copy()
    rng = np.random.default_rng(a.seed)
    starts: dict[str, np.ndarray | None] = {
        "production": None,
        "tau_ff": prim.tau_ff_min.copy(),
        "floor_min(ff,obs)": np.minimum(prim.tau_ff_min, tau_obs),
        "2x_tau_obs": 2.0 * tau_obs,
    }
    for r in range(a.n_random):
        starts[f"random{r}_tau_obs*exp(0.3e)"] = tau_obs * np.exp(0.3 * rng.standard_normal(len(tau_obs)))
    tight_starts = ["production", "floor_min(ff,obs)", "2x_tau_obs", "random0_tau_obs*exp(0.3e)"]

    def run_policy(pid):
        if pid == "P1":
            eq, _ = s2.run_tidal_endogenous(prim, fund, params, baseline, top_k=100, delta=1.0,
                                            horizon="long_run", pair_selection=a.pair_selection, **kw)
            return eq, None
        if pid == "P2":
            eq, _ = s2.run_road_expansion_same_corridor(prim, fund, params, baseline, top_k=100,
                                                        pair_selection=a.pair_selection,
                                                        horizon="long_run", **kw)
            return eq, None
        if pid == "P3":
            eq, _ = s2.run_road_expansion_qdelay_topk(prim, fund, params, baseline, top_k=100,
                                                      horizon="long_run", **kw)
            return eq, None
        pol = {"P6": "first_best", "P6b": "baseline_pigou", "P7": "cordon"}[pid]
        return s2.run_pricing_additive(prim, fund, params, baseline, policy=pol, horizon="long_run", **kw)

    s2.solve_equilibrium = _patched_solve
    rows = []
    try:
        for pid in [p.strip() for p in a.policies.split(",") if p.strip()]:
            plan = []
            for tol_name in ("production", "tight"):
                for sname in starts:
                    if tol_name == "tight" and sname not in tight_starts:
                        continue
                    plan.append((tol_name, sname, None))
                if pid == "P6":
                    for ts in (0.0, 2.0):
                        plan.append((tol_name, "production", ts))
            ref = {}
            for tol_name, sname, tscale in plan:
                tol = kw["tol"] if tol_name == "production" else a.tight_tol
                mo = kw["max_outer"] if tol_name == "production" else (
                    a.tight_max_outer_p6 if pid == "P6" else a.tight_max_outer)
                _CTRL.clear()
                _CTRL.update(start=starts[sname], tol=tol, max_outer=mo, toll_scale=tscale)
                t0 = time.time()
                eq, meta = run_policy(pid)
                _CTRL.clear()
                w = (meta["Wbar_pct_change_realized_route_choice"] if meta is not None
                     else 100.0 * (eq.Wbar / baseline.Wbar - 1.0))
                key = tol_name
                if key not in ref:
                    ref[key] = (eq, w)
                req, rw = ref[key]
                row = {
                    "policy": pid, "tolerance": tol_name, "tol": tol, "max_outer": mo,
                    "start": sname if tscale is None else f"production_toll_start_x{tscale:g}",
                    "converged": bool(eq.converged), "n_outer": int(eq.n_outer),
                    "final_max_log_change": float(eq.max_log_change),
                    "Wbar_lt_pct": float(w),
                    "abs_diff_Wbar_pct_vs_ref_pp": float(abs(w - rw)),
                    "max_abs_dlog_tau_vs_ref": float(np.max(np.abs(np.log(eq.tau_min_kl)
                                                                  - np.log(req.tau_min_kl)))),
                    "rho_A_solver_cost": spectral_radius_A(prim.edge_i, prim.edge_j, eq.tau_min_kl,
                                                           prim.n_nodes, kappa=float(eq.kappa)),
                    "runtime_sec": time.time() - t0,
                }
                rows.append(row)
                pd.DataFrame(rows).to_csv(out / "multistart_long_run.csv", index=False)
                print(json.dumps(row), flush=True)
    finally:
        s2.solve_equilibrium = _ORIG_SOLVE

    df = pd.DataFrame(rows)
    summ = []
    for (pid, tl), g in df.groupby(["policy", "tolerance"], sort=False):
        summ.append({"policy": pid, "tolerance": tl, "n_starts": len(g),
                     "all_converged": bool(g.converged.all()),
                     "max_n_outer": int(g.n_outer.max()),
                     "max_final_max_log_change": float(g.final_max_log_change.max()),
                     "Wbar_lt_min": float(g.Wbar_lt_pct.min()), "Wbar_lt_max": float(g.Wbar_lt_pct.max()),
                     "Wbar_lt_spread_pp": float(g.Wbar_lt_pct.max() - g.Wbar_lt_pct.min()),
                     "Wbar_lt_production_start": float(g.Wbar_lt_pct.iloc[0])})
    pd.DataFrame(summ).to_csv(out / "spread_by_policy.csv", index=False)
    print(pd.DataFrame(summ).to_string(index=False), flush=True)


if __name__ == "__main__":
    main()
