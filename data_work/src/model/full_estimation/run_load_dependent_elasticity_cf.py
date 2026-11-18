"""Load-dependent congestion elasticity in the tidal counterfactual (trial, 2026-11-15).

Step 3-4 of the load-dependent elasticity trial. Needs the estimate/ output of
run_load_dependent_elasticity_estimate.py in the same output directory.

Setup is the mainline exactly (run_beta_c_rerun_checks._setup: 1B bundle alldaytt0709, ce_floor,
xi floor 1%, spectral kappa, theta 6.83, alpha 0.05, beta -0.10, damping 0.1, tol 1e-4, 500 outer,
score_bpr_st top100). No existing file is edited: equilibrium_solve._congestion_tau_update is
replaced at call time (only when a variant is active and form == 'ce_floor') by

  dev   = (max(Xi, xf)/max(Xi_obs, xf)) * n_obs/n       (unchanged)
  floor = min(tau_ff, tau_obs)                            (unchanged)
  ce      tau = max(floor, tau_obs * dev^beta_e)                    edge-specific constant elasticity
  curved  tau = max(floor, tau_obs * exp(G_e(ln dev)))
          G_e(s) = int_0^s clip(beta_e + c u, lo, hi) du             local elasticity beta_e at the anchor,
                                                                     moving with slope c along ln dev
  bpr     tau = floor * (1 + m_e dev^b_e), m_e = tau_obs/floor - 1   on edges with m_e >= m_min;
          b_e = beta_e (1 + m_e) / m_e (BPR local elasticity at the anchor = beta_e); other edges: ce
At dev = 1 every form returns tau_obs, so the baseline is the observation anchor.

beta_e = clip(e(load_e) + shift, lo, hi): e() a schedule from estimate/, load_e the edge's 07-09
position; 'treated' scope applies it to the 200 tidal edges only (others 0.35), 'all' to all edges.
'raw' uses shift 0; 'meanpres' picks shift so the simple mean of beta_e over the scope is 0.35.
Unmatched edges (no gate5 panel unit) take the median load of matched edges with the same role.

Output: outputs/<output-name>/cf/ (refuses to overwrite).
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

from src.model.full_estimation import equilibrium_solve as es  # noqa: E402
from src.model.full_estimation import stage2_counterfactuals as s2  # noqa: E402
from src.model.full_estimation.equilibrium_solve import MAINLINE_PAIR_SELECTION, spectral_radius_A  # noqa: E402
from src.model.full_estimation.run_beta_c_rerun_checks import _setup  # noqa: E402
from src.model.full_estimation.tidal_lane_diagnostics import summarise_tidal_pair  # noqa: E402

OUT = DATA_WORK / "outputs"
EPS = es.EPS
BENCH = 0.35
T0 = time.time()
_ORIG = es._congestion_tau_update
CTRL: dict = {}


def log(*a):
    print(f"[{time.time() - T0:6.0f}s]", *a, flush=True)


def G_curved(s, b1, c, lo, hi):
    """int_0^s clip(b1 + c u, lo, hi) du, elementwise (b1 within [lo, hi])."""
    s = np.asarray(s, float)
    if c == 0:
        return b1 * s
    ulo, uhi = (lo - b1) / c, (hi - b1) / c        # c > 0: ulo <= 0 <= uhi
    if c < 0:
        ulo, uhi = (hi - b1) / c, (lo - b1) / c
    g = lambda u: b1 * u + 0.5 * c * u * u  # noqa: E731
    elo = b1 + c * ulo
    ehi = b1 + c * uhi
    return np.where(s > uhi, g(uhi) + ehi * (s - uhi), np.where(s < ulo, g(ulo) + elo * (s - ulo), g(s)))


def patched_update(*, form, lam, tau_ff, tau_obs, Xi_kl, n_kl, ell_km, xi_obs_kl, n_obs_kl, xi_floor=0.0):
    if not CTRL or form != "ce_floor":
        return _ORIG(form=form, lam=lam, tau_ff=tau_ff, tau_obs=tau_obs, Xi_kl=Xi_kl, n_kl=n_kl, ell_km=ell_km,
                     xi_obs_kl=xi_obs_kl, n_obs_kl=n_obs_kl, xi_floor=xi_floor)
    if xi_floor > 0:
        flow = np.maximum(Xi_kl, xi_floor) / np.maximum(xi_obs_kl, xi_floor)
    else:
        flow = np.clip(Xi_kl / np.clip(xi_obs_kl, EPS, None), EPS, None)
    dev = flow * np.clip(n_obs_kl / np.clip(n_kl, EPS, None), EPS, None)
    floor = np.minimum(tau_ff, tau_obs)
    kind, beta = CTRL["kind"], CTRL["beta"]
    if kind == "ce":
        return np.maximum(floor, tau_obs * dev ** beta)
    if kind == "curved":
        return np.maximum(floor, tau_obs * np.exp(G_curved(np.log(dev), beta, CTRL["c"], CTRL["lo"], CTRL["hi"])))
    if kind == "bpr":
        ce = np.maximum(floor, tau_obs * dev ** beta)
        markup = np.maximum(tau_obs / floor - 1.0, 0.0)
        bp_ = floor * (1.0 + markup * dev ** CTRL["b_bpr"])
        return np.where(CTRL["bpr_mask"], bp_, ce)
    raise ValueError(kind)


# --------------------------------------------------------------- schedules
def load_schedules(est: Path) -> dict:
    j = json.loads((est / "estimate_summary.json").read_text())
    C = pd.read_csv(est / "2_C_iv_by_congestion_ratio_tercile.csv")
    cC = C[C.selection == "hours 07-09"].sort_values("cr_tercile").beta.to_numpy()
    q = j["Q_quadratic"]
    sp = j["S_spline"]
    cuts = np.array(j["cr_tercile_cuts"])
    return {
        "Q": dict(measure="s_q", f=lambda s: q["b1"] + 2 * q["b2"] * s, c=2 * q["b2"]),
        "S": dict(measure="s_q", f=lambda s: sp["b"][0] + sum(b * (s > k) for b, k in zip(sp["b"][1:], sp["knots_xc"])), c=0.0),
        "C": dict(measure="cr", f=lambda s: cC[np.digitize(s, cuts)], c=0.0),
        "Cb": dict(measure="ln_obs_over_ff", f=lambda s: cC[np.digitize(s, cuts)], c=0.0),
        "_json": j,
    }


def edge_loads(est: Path, measure: str) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    e = pd.read_csv(est / "model_edge_load_positions.csv").sort_values("edge_idx")
    v = e[measure].to_numpy(float)
    imputed = ~np.isfinite(v)
    for role in e.role.unique():
        m = (e.role == role).to_numpy()
        med = np.nanmedian(v[m & ~imputed])
        v = np.where(m & imputed, med, v)
    return v, e.role.to_numpy(), imputed


def make_beta(sched, loads, roles, scope, norm, lo, hi):
    e_raw = np.asarray(sched["f"](loads), float)
    scope_m = np.ones(len(loads), bool) if scope == "all" else (roles != "other")

    def build(shift):
        b = np.full(len(loads), BENCH)
        b[scope_m] = np.clip(e_raw[scope_m] + shift, lo, hi)
        return b
    shift = 0.0
    if norm == "meanpres":
        a_, b_ = -1.0, 1.0
        for _ in range(80):
            mid = 0.5 * (a_ + b_)
            if build(mid)[scope_m].mean() < BENCH:
                a_ = mid
            else:
                b_ = mid
        shift = 0.5 * (a_ + b_)
    return build(shift), shift


# --------------------------------------------------------------- one scenario
def group_decomp(prim, base, st, k, roles):
    L = prim.Lbar
    out = {}
    for g in ("heavy_gain_lane", "light_lose_lane", "other"):
        m = roles == g
        tot = np.sum(st.Xi_kl[m] * st.tau_min_kl[m] - base.Xi_kl[m] * base.tau_min_kl[m]) / (L * k)
        fo = np.sum(base.Xi_kl[m] * (st.tau_min_kl[m] - base.tau_min_kl[m])) / (L * k)
        out[f"direct_st_sec_{g}"] = 60 * tot
        out[f"direct_st_sec_{g}_time_change_at_base_flow"] = 60 * fo
        out[f"st_xi_weighted_dlog_tau_{g}"] = float(np.sum(base.Xi_kl[m] * np.log(st.tau_min_kl[m] / base.tau_min_kl[m]))
                                                     / np.sum(base.Xi_kl[m]))
        out[f"st_mean_delta_tau_sec_{g}"] = float(60 * np.mean(st.tau_min_kl[m] - base.tau_min_kl[m]))
    return out


def floor_stats(prim, kw, eq, tag):
    floor = np.minimum(prim.tau_ff_min, kw["tau_obs_anchor"])
    return {f"{tag}_share_tau_at_floor": float(np.mean(eq.tau_min_kl <= floor * (1 + 1e-6))),
            f"{tag}_share_flow_below_xi_floor": float(np.mean(eq.Xi_kl < kw["xi_floor"]))}


def run_scenario(name, spec, S, out, do_p2=True):
    prim, params, fund, kw0 = S["prim"], S["params"], S["fund"], S["kw"]
    kw = dict(kw0)
    if spec.get("native_form"):
        kw.update(congestion_form=spec["native_form"], congestion_lambda=spec["native_lambda"])
    CTRL.clear()
    if spec.get("kind"):
        CTRL.update(spec)
    k = float(fund.kappa)
    t = time.time()
    try:
        base = s2.solve_baseline_endogenous(prim, fund, params, **kw)
        st, sel = s2.run_tidal_endogenous(prim, fund, params, base, top_k=100, delta=1.0, horizon="short_run",
                                          pair_selection=MAINLINE_PAIR_SELECTION, **kw)
        lt, _ = s2.run_tidal_endogenous(prim, fund, params, base, top_k=100, delta=1.0, horizon="long_run",
                                        pair_selection=MAINLINE_PAIR_SELECTION, **kw)
        p2 = {}
        if do_p2:
            for h in ("short_run", "long_run"):
                eq2, _ = s2.run_road_expansion_same_corridor(prim, fund, params, base, top_k=100,
                                                             pair_selection=MAINLINE_PAIR_SELECTION, horizon=h, **kw)
                p2[h] = eq2
    finally:
        CTRL.clear()
    row, _, edges = summarise_tidal_pair(spec_label=name, top_k=100, delta_lanes=1.0, baseline=base, short_run=st,
                                         long_run=lt, prim=prim, params=params, abar_theta=fund.abar_theta, sel=sel)
    sel_pairs = {(int(a), int(b)) for a, b in sel.treated_pairs}
    r = {"variant": name, "description": spec.get("desc", ""),
         "selection_equals_mainline": sel_pairs == S["sel_main"],
         "Wbar_pct_st": row["Wbar_pct_change_st"], "Wbar_pct_lt": row["Wbar_pct_change_lt"],
         "time_saved_sec_st": row["physical_time_saved_st_sec_per_commuter"],
         "time_saved_sec_lt": row["physical_time_saved_lt_sec_per_commuter"],
         "direct_st_sec": row["direct_st_cost_change_sec"], "ge_wedge_sec": row["ge_cost_wedge_sec"],
         "sorting_wedge_sec": row["sorting_wedge_sec"]}
    r.update(group_decomp(prim, base, st, k, S["roles"]))
    b = spec.get("beta")
    if b is not None and np.ndim(b):
        for g in ("heavy_gain_lane", "light_lose_lane", "other"):
            m = S["roles"] == g
            r[f"beta_e_mean_{g}"] = float(np.mean(b[m]))
        r["beta_e_mean_all"] = float(np.mean(b))
    r["shift"] = spec.get("shift", np.nan)
    for tag, eq in (("base", base), ("st", st), ("lt", lt)):
        r[f"{tag}_converged"] = bool(eq.converged)
        r[f"{tag}_n_outer"] = int(eq.n_outer)
        r[f"{tag}_max_log_change"] = float(eq.max_log_change)
        r[f"{tag}_rho_A"] = spectral_radius_A(prim.edge_i, prim.edge_j, eq.tau_min_kl, prim.n_nodes, kappa=k)
        r.update(floor_stats(prim, kw, eq, tag))
    r["base_max_abs_dlog_tau_vs_tau_obs"] = float(np.max(np.abs(np.log(base.tau_min_kl) - np.log(kw["tau_obs_anchor"]))))
    if p2:
        for h, tg in (("short_run", "st"), ("long_run", "lt")):
            r[f"P2_Wbar_pct_{tg}"] = 100.0 * (p2[h].Wbar / base.Wbar - 1.0)
            r[f"P2_converged_{tg}"] = bool(p2[h].converged)
        r["P1_over_P2_st"] = r["Wbar_pct_st"] / r["P2_Wbar_pct_st"]
        r["P1_over_P2_lt"] = r["Wbar_pct_lt"] / r["P2_Wbar_pct_lt"]
    r["runtime_sec"] = time.time() - t
    edges.insert(0, "variant", name)
    if b is not None and np.ndim(b):
        edges["beta_e"] = b[edges.edge_idx.to_numpy()]
    edges.to_csv(out / "edges" / f"{name}.csv", index=False)
    return r, (base, st, lt)


def probe(name, spec, S, base, n_cf_starts=("tau_obs", "2x_tau_obs", "tau_ff")):
    """Two/three-start uniqueness probe at tight tolerance for the tidal ST and LT."""
    prim, params, fund, kw0 = S["prim"], S["params"], S["fund"], S["kw"]
    sel = S["sel_obj"]
    n_cf = prim.n_kl.copy()
    for slow, fast in sel.treated_pairs:
        n_cf[slow] += 1.0
        n_cf[fast] -= 1.0
    starts = {"tau_obs": prim.tau_obs_min.copy(), "2x_tau_obs": 2 * prim.tau_obs_min, "tau_ff": prim.tau_ff_min.copy()}
    rows = []
    CTRL.clear(); CTRL.update(spec)
    try:
        for h in ("short_run", "long_run"):
            pin = dict(l_R_fixed=base.l_R, l_F_fixed=base.l_F) if h == "short_run" else {}
            kwt = dict(kw0, tol=1e-7, max_outer=4000)
            ref = None
            for sn in n_cf_starts:
                eq = es.solve_equilibrium(prim, fund.ubar_theta, fund.abar_theta, params, n_kl_override=n_cf,
                                          tau_min_init=starts[sn].copy(), endogenous_congestion=True,
                                          label=f"probe_{h}_{sn}", **pin, **kwt)
                ref = ref or eq
                w, wr = 100 * (eq.Wbar / base.Wbar - 1), 100 * (ref.Wbar / base.Wbar - 1)
                rows.append({"variant": name, "horizon": h, "start": sn, "tol": 1e-7, "converged": bool(eq.converged),
                             "n_outer": int(eq.n_outer), "Wbar_pct": w, "abs_diff_Wbar_pp_vs_first": abs(w - wr),
                             "max_abs_dlog_tau_vs_first": float(np.max(np.abs(np.log(eq.tau_min_kl / ref.tau_min_kl)))),
                             "max_abs_diff_tau_min_vs_first": float(np.max(np.abs(eq.tau_min_kl - ref.tau_min_kl)))})
                log(json.dumps(rows[-1]))
    finally:
        CTRL.clear()
    return rows


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-name", default="load_dependent_elasticity_trial_v1")
    ap.add_argument("--lo", type=float, default=0.10)
    ap.add_argument("--hi", type=float, default=0.50)
    ap.add_argument("--bpr-m-min", type=float, default=0.05)
    ap.add_argument("--bpr-b-cap", type=float, default=10.0)
    ap.add_argument("--only", default="", help="comma list of variant names (default all)")
    ap.add_argument("--no-probe", action="store_true")
    ap.add_argument("--set", default="main", choices=("main", "decomp"),
                    help="main: validation + schedule variants (written to cf/); "
                         "decomp: heavy-only / light-only variants (written to cf_decomp/)")
    a = ap.parse_args(argv)
    root = OUT / a.output_name
    est = root / "estimate"
    out = root / ("cf" if a.set == "main" else "cf_decomp")
    if not est.exists():
        raise SystemExit(f"{est} missing; run run_load_dependent_elasticity_estimate.py first")
    if out.exists():
        raise SystemExit(f"{out} exists; refusing to overwrite")
    (out / "edges").mkdir(parents=True)
    (out / "command.txt").write_text(" ".join([sys.executable] + sys.argv) + "\n")
    es._congestion_tau_update = patched_update

    prim, params, fund, kw, kinfo, anchors, xi_floor = _setup(BENCH)
    sel_main = pd.read_csv(OUT / "tidal_lane_diagnostics_osm1b_ce_floor_b035_fl001_ks_od2025_alldaytt0709_v1"
                           / "top100_d1.0" / "selected_pairs.csv")
    sch = load_schedules(est)
    S = {"prim": prim, "params": params, "fund": fund, "kw": kw,
         "sel_main": {(int(x), int(y)) for x, y in zip(sel_main.slow_edge_idx, sel_main.fast_edge_idx)}}
    lo, hi = a.lo, a.hi
    M = prim.n_edges
    loads, roles, imputed = {}, None, {}
    for meas in ("s_q", "cr", "ln_obs_over_ff"):
        loads[meas], roles, imputed[meas] = edge_loads(est, meas)
    S["roles"] = roles
    tau_obs = kw["tau_obs_anchor"]
    floor = np.minimum(prim.tau_ff_min, tau_obs)
    m_obs = np.maximum(tau_obs / floor - 1.0, 0.0)
    c_Q = sch["Q"]["c"]

    V: dict[str, dict] = {}
    V["bench_native_b035"] = dict(native_form="ce_floor", native_lambda=BENCH, desc="mainline ce_floor 0.35, no patch")
    V["val_flat_vector_ce"] = dict(kind="ce", beta=np.full(M, BENCH), desc="validation: ce with a flat 0.35 vector")
    V["val_flat_curved_c0"] = dict(kind="curved", beta=np.full(M, BENCH), c=0.0, lo=lo, hi=hi,
                                   desc="validation: curved form, b1=0.35, c=0")
    V["val_bpr_native_b165"] = dict(native_form="bpr", native_lambda=1.65, desc="existing BPR form b=1.65 (reference)")
    V["val_bpr_wrapped_b165"] = dict(kind="bpr", beta=np.full(M, BENCH), b_bpr=np.full(M, 1.65),
                                     bpr_mask=np.ones(M, bool), desc="validation: wrapped BPR with b=1.65 everywhere")
    V["curv_only_flat035_cQ"] = dict(kind="curved", beta=np.full(M, BENCH), c=c_Q, lo=lo, hi=hi,
                                     desc="flat 0.35 at the anchor, curvature c from Q")

    def add(name, sk, scope, norm, kind, lo_=lo, hi_=hi, c=None, desc=""):
        sched = sch[sk]
        b, shift = make_beta(sched, loads[sched["measure"]], roles, scope, norm, lo_, hi_)
        spec = dict(kind=kind, beta=b, shift=shift, lo=lo_, hi=hi_,
                    desc=f"schedule {sk} on {sched['measure']}, scope {scope}, {norm}, bounds [{lo_},{hi_}] {desc}")
        if kind == "curved":
            spec["c"] = c_Q if c is None else c
            spec["desc"] += f", c={spec['c']:.3f}"
        if kind == "bpr":
            mask = m_obs >= a.bpr_m_min
            bb = np.where(mask, b * (1 + m_obs) / np.clip(m_obs, EPS, None), 1.0)
            spec["b_bpr"] = np.minimum(bb, a.bpr_b_cap)
            spec["bpr_mask"] = mask
            spec["desc"] += f", BPR b_e=beta_e(1+m)/m on m>={a.bpr_m_min} (cap {a.bpr_b_cap}), ce elsewhere"
        V[name] = spec

    for scope in ("treated", "all"):
        for norm in ("raw", "meanpres"):
            add(f"Q_{scope}_{norm}_ce", "Q", scope, norm, "ce")
            add(f"Q_{scope}_{norm}_curved", "Q", scope, norm, "curved")
    add("Q_treated_meanpres_bpr", "Q", "treated", "meanpres", "bpr")
    add("Q_all_meanpres_bpr", "Q", "all", "meanpres", "bpr")
    # sensitivity: schedule / load measure / bounds / curvature
    add("S_treated_meanpres_ce", "S", "treated", "meanpres", "ce")
    add("S_all_meanpres_ce", "S", "all", "meanpres", "ce")
    add("C_treated_meanpres_ce", "C", "treated", "meanpres", "ce")
    add("C_treated_meanpres_ce_wide", "C", "treated", "meanpres", "ce", lo_=0.03, hi_=0.80)
    add("C_all_meanpres_ce", "C", "all", "meanpres", "ce")
    add("C_treated_raw_ce", "C", "treated", "raw", "ce")
    add("Cb_treated_meanpres_ce", "Cb", "treated", "meanpres", "ce", desc="(bundle tau_obs/tau_ff as load)")
    add("Q_treated_meanpres_ce_b2045", "Q", "treated", "meanpres", "ce", lo_=0.20, hi_=0.45)
    add("Q_treated_meanpres_curved_c2x", "Q", "treated", "meanpres", "curved", c=2 * c_Q)

    if a.set == "decomp":
        bq, _ = make_beta(sch["Q"], loads["s_q"], roles, "treated", "meanpres", lo, hi)
        hv, lt_ = roles == "heavy_gain_lane", roles == "light_lose_lane"
        V = {"bench_native_b035": V["bench_native_b035"]}

        def only(mask, vals, desc):
            b = np.full(M, BENCH)
            b[mask] = vals[mask] if np.ndim(vals) else vals
            return dict(kind="ce", beta=b, desc=desc)
        V["Qmp_heavy_only"] = only(hv, bq, "Q_treated_meanpres values on heavy edges only; light and others 0.35")
        V["Qmp_light_only"] = only(lt_, bq, "Q_treated_meanpres values on light edges only; heavy and others 0.35")
        V["heavy_flat_mean_Qmp"] = only(hv, float(bq[hv].mean()), "heavy edges flat at their Q_treated_meanpres mean")
        V["light_flat_010"] = only(lt_, 0.10, "light edges 0.10, rest 0.35")
        V["light_flat_050"] = only(lt_, 0.50, "light edges 0.50, rest 0.35")
        V["heavy_flat_030"] = only(hv, 0.30, "heavy edges 0.30, rest 0.35")
        V["heavy_flat_040"] = only(hv, 0.40, "heavy edges 0.40, rest 0.35")
        V["heavy_flat_045"] = only(hv, 0.45, "heavy edges 0.45, rest 0.35")
    names = [n for n in V if not a.only or n in a.only.split(",")]
    meta = {"kappa": kinfo, "xi_floor": xi_floor, "bounds": [lo, hi], "c_Q": c_Q,
            "imputed_treated_edges": {m: int(np.sum(imputed[m] & (roles != "other"))) for m in imputed},
            "imputed_all_edges": {m: int(np.sum(imputed[m])) for m in imputed},
            "variants": {n: V[n].get("desc", "") for n in names}}
    (out / "setup.json").write_text(json.dumps(meta, indent=2, default=float))
    from src.model.full_estimation.counterfactual_levels import select_tidal_lane_pairs_by_rank
    base0 = s2.solve_baseline_endogenous(prim, fund, params, **kw)
    S["sel_obj"] = select_tidal_lane_pairs_by_rank(prim, top_k=100, delta=1.0, rank=MAINLINE_PAIR_SELECTION,
                                                   xi_kl=base0.Xi_kl)
    rows, probes = [], []
    for n in names:
        log("variant", n)
        r, (base, st, lt) = run_scenario(n, V[n], S, out)
        rows.append(r)
        log({k: r[k] for k in ("Wbar_pct_st", "Wbar_pct_lt", "time_saved_sec_st", "time_saved_sec_lt",
                               "direct_st_sec_heavy_gain_lane", "direct_st_sec_light_lose_lane", "st_converged", "lt_converged")})
        pd.DataFrame(rows).to_csv(out / "results_by_variant.csv", index=False)
        if not a.no_probe and n in ("Q_treated_meanpres_curved", "Q_all_meanpres_curved", "Q_all_raw_curved",
                                     "Q_treated_meanpres_bpr", "curv_only_flat035_cQ"):
            probes += probe(n, V[n], S, base)
            pd.DataFrame(probes).to_csv(out / "probe_multistart_tight.csv", index=False)
    log("done")


if __name__ == "__main__":
    main()
