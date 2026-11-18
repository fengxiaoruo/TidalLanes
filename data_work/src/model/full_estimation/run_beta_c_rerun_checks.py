"""beta_c rerun (2026-11-13): checks and summary for the ce_floor elasticity rerun.

Mainline spec (1B, all-day OD + 07-09 times, ce_floor, floor 1% of median anchor flow,
spectral kappa, theta 6.83, route_eta 1, score_bpr_st top100). The tidal and policy runs
themselves are made with the unchanged scripts ``run_tidal_lane_diagnostics_gate5.py`` and
``run_policy_comparison_alldaytt0709.py``; this script only reads their outputs and re-solves
equilibria for checks. Existing modules are imported, never modified.

  checks  --beta B --tidal-dir DIR --out-dir OUT
      Rebuilds the tidal setup exactly as run_tidal_lane_diagnostics_gate5.run (same params,
      solver kwargs, anchors, floor, kappa) and writes OUT/checks_bXXX/:
        convergence.csv     baseline / ST / LT: converged, n_outer, final max log change,
                            spectral radius rho(A); plus whether the re-solved equilibria match
                            the saved ones in DIR
        multistart.csv      tidal top100 ST and LT solved from several starting link times,
                            at production tolerance and at a tight tolerance (1e-7); max diff in
                            edge times (minutes and log) and in W-bar % change vs the reference
        floor_share.csv     share of edges on the time floor and below the flow floor
        selection.csv       overlap of the selected top100 pairs/edges with the b025 run
        outer_map.csv       long-run outer-map monotonicity probe r (r < 1 on every pair is
                            evidence for a unique fixed point; same test as
                            run_equilibrium_uniqueness_check.py)
  summary --out-dir OUT
      Collects tidal, policy and check results into OUT/summary.csv and companion tables.
"""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import replace
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
from src.model.full_estimation.equilibrium_solve import (  # noqa: E402
    MAINLINE_ALPHA,
    MAINLINE_BETA,
    MAINLINE_PAIR_SELECTION,
    compute_observation_anchors,
    resolve_kappa,
    spectral_radius_A,
)
from src.model.full_estimation.run_alpha_robustness_quick_gate5 import solver_overrides_for_params  # noqa: E402
from src.model.full_estimation.run_equilibrium_uniqueness_check import outer_map  # noqa: E402
from src.model.full_estimation.run_tidal_lane_diagnostics_gate5 import _skw  # noqa: E402

OUT = DATA_WORK / "outputs"
VERSION_ID = "model_input_osm1b_L300_gate5_od2025_v2_alldaytt0709"
B025_TIDAL = "tidal_lane_diagnostics_osm1b_ce_floor_b025_fl001_ks_od2025_alldaytt0709_v3"
TIDAL_DIRS = {
    0.25: B025_TIDAL,
    0.29: "tidal_lane_diagnostics_osm1b_ce_floor_b029_fl001_ks_od2025_alldaytt0709_v1",
    0.35: "tidal_lane_diagnostics_osm1b_ce_floor_b035_fl001_ks_od2025_alldaytt0709_v1",
    0.44: "tidal_lane_diagnostics_osm1b_ce_floor_b044_fl001_ks_od2025_alldaytt0709_v1",
}
POLICY_DIRS = {
    0.25: "policy_comparison_osm1b_ce_floor_b025_fl001_ks_od2025_alldaytt0709_v1",
    0.35: "policy_comparison_osm1b_ce_floor_b035_fl001_ks_od2025_alldaytt0709_v1",
}
EPS = es.EPS


def _tag(beta: float) -> str:
    return f"b{int(round(beta * 100)):03d}"


def _setup(beta: float):
    """Same objects as run_tidal_lane_diagnostics_gate5.main/run with the mainline flags."""
    prim = bp.load_primitives(OUT / VERSION_ID, period="AM")
    params = replace(pm.Parameters(), alpha=MAINLINE_ALPHA, beta=MAINLINE_BETA, route_eta=1.0)
    skw = {k: v for k, v in solver_overrides_for_params(params).items() if v is not None}
    skw["max_outer"] = max(500, skw.get("max_outer", 500))
    kappa, kinfo = resolve_kappa(prim, "spectral")
    fund = inv.invert_fundamentals(prim, params, tol=1.5e-4, max_iter=1800, kappa=kappa)
    anchors = compute_observation_anchors(prim, fund.ubar_theta, fund.abar_theta, params,
                                          chi=fund.chi, kappa=fund.kappa)
    xi_floor = 0.01 * float(np.median(anchors["xi_obs_kl"]))
    kw = _skw(fund, congestion_form="ce_floor", congestion_lambda=beta, **skw)
    kw.update(xi_obs_kl=anchors["xi_obs_kl"], n_obs_kl=anchors["n_obs_kl"],
              tau_obs_anchor=anchors["tau_obs_anchor"], xi_floor=xi_floor)
    return prim, params, fund, kw, kinfo, anchors, xi_floor


def _floor_stats(prim, kw, eq, n_kl) -> dict:
    tau_obs = kw["tau_obs_anchor"]
    floor = np.minimum(prim.tau_ff_min, tau_obs)
    xf = kw["xi_floor"]
    flow = np.maximum(eq.Xi_kl, xf) / np.maximum(kw["xi_obs_kl"], xf)
    dev = flow * kw["n_obs_kl"] / np.clip(es._resolve_lanes(n_kl), EPS, None)
    cand = tau_obs * dev ** kw["congestion_lambda"]
    return {
        "share_time_floor_strict": float(np.mean(cand < floor * (1 - 1e-9))),
        "share_time_floor_incl_ties": float(np.mean(cand <= floor * (1 + 1e-9))),
        "share_tau_at_floor": float(np.mean(eq.tau_min_kl <= floor * (1 + 1e-6))),
        "share_flow_below_xi_floor": float(np.mean(eq.Xi_kl < xf)),
        "share_edges_tau_obs_le_tau_ff": float(np.mean(tau_obs <= prim.tau_ff_min)),
    }


def _load_saved(d: Path):
    z = np.load(d / "equilibrium_arrays.npz")
    meta = json.loads((d / "equilibrium_meta.json").read_text())
    return z["tau_min_kl"], meta


def checks(argv: list[str]) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--beta", type=float, required=True)
    ap.add_argument("--tidal-dir", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--n-random", type=int, default=2)
    ap.add_argument("--n-pairs", type=int, default=20)
    ap.add_argument("--seed", type=int, default=20261113)
    a = ap.parse_args(argv)
    beta = a.beta
    out = OUT / a.out_dir / f"checks_{_tag(beta)}"
    if out.exists():
        raise SystemExit(f"{out} exists; refusing to overwrite")
    out.mkdir(parents=True)
    (out / "command.txt").write_text(" ".join([sys.executable] + sys.argv) + "\n")
    rng = np.random.default_rng(a.seed)
    tdir = OUT / a.tidal_dir

    prim, params, fund, kw, kinfo, anchors, xi_floor = _setup(beta)
    base = s2.solve_baseline_endogenous(prim, fund, params, **kw)
    st, sel = s2.run_tidal_endogenous(prim, fund, params, base, top_k=100, delta=1.0, horizon="short_run",
                                      pair_selection=MAINLINE_PAIR_SELECTION, **kw)
    lt, _ = s2.run_tidal_endogenous(prim, fund, params, base, top_k=100, delta=1.0, horizon="long_run",
                                    pair_selection=MAINLINE_PAIR_SELECTION, **kw)
    n_cf = prim.n_kl.copy()
    for slow, fast in sel.treated_pairs:
        n_cf[slow] += 1.0
        n_cf[fast] -= 1.0
    k = float(fund.kappa)

    # 1. convergence + spectral radius (+ match with saved run)
    conv = []
    for name, eq, sub, lanes in (("baseline", base, "baseline", prim.n_kl),
                                 ("tidal_short_run", st, "top100_d1.0/short_run", n_cf),
                                 ("tidal_long_run", lt, "top100_d1.0/long_run", n_cf)):
        saved_tau, meta = _load_saved(tdir / sub)
        conv.append({
            "beta_c": beta, "equilibrium": name, "converged": bool(eq.converged), "n_outer": int(eq.n_outer),
            "final_max_log_change": float(eq.max_log_change), "tol": kw["tol"], "max_outer": kw["max_outer"],
            "damping": kw["damping"], "kappa": k,
            "rho_A": spectral_radius_A(prim.edge_i, prim.edge_j, eq.tau_min_kl, prim.n_nodes, kappa=k),
            "Wbar": float(eq.Wbar),
            "saved_run_converged": bool(meta["converged"]), "saved_run_n_outer": int(meta["n_outer"]),
            "max_abs_dlog_tau_vs_saved_run": float(np.max(np.abs(np.log(eq.tau_min_kl) - np.log(saved_tau)))),
            **_floor_stats(prim, kw, eq, lanes),
        })
    conv_df = pd.DataFrame(conv)
    conv_df.to_csv(out / "convergence.csv", index=False)
    conv_df[["beta_c", "equilibrium"] + [c for c in conv_df.columns if c.startswith("share_")]].to_csv(
        out / "floor_share.csv", index=False)
    print(conv_df.to_string(index=False), flush=True)

    # 2. multistart on the tidal counterfactual
    tau_obs = prim.tau_obs_min.copy()
    floor = np.minimum(prim.tau_ff_min, tau_obs)
    starts = {"tau_obs(production)": tau_obs, "baseline_eq": base.tau_min_kl.copy(), "floor_min(ff,obs)": floor,
              "free_flow_tau_ff": prim.tau_ff_min.copy(), "2x_tau_obs": 2.0 * tau_obs}
    for r in range(a.n_random):
        starts[f"random{r}_tau_obs*exp(0.3e)"] = tau_obs * np.exp(0.3 * rng.standard_normal(len(tau_obs)))
    tight_starts = ["tau_obs(production)", "floor_min(ff,obs)", "2x_tau_obs", "random0_tau_obs*exp(0.3e)"]
    rows = []
    for horizon in ("short_run", "long_run"):
        pin = dict(l_R_fixed=base.l_R, l_F_fixed=base.l_F) if horizon == "short_run" else {}
        for tol_name, tol, mo in (("production", kw["tol"], kw["max_outer"]), ("tight", 1e-7, 4000)):
            kwt = dict(kw, tol=tol, max_outer=mo)
            ref = None
            for sname, t0 in starts.items():
                if tol_name == "tight" and sname not in tight_starts:
                    continue
                eq = es.solve_equilibrium(prim, fund.ubar_theta, fund.abar_theta, params, n_kl_override=n_cf,
                                          tau_min_init=t0.copy(), endogenous_congestion=True,
                                          label=f"{horizon}_{sname}_{tol_name}", **pin, **kwt)
                if ref is None:
                    ref = eq
                w = 100.0 * (eq.Wbar / base.Wbar - 1.0)
                wref = 100.0 * (ref.Wbar / base.Wbar - 1.0)
                rows.append({
                    "beta_c": beta, "horizon": horizon, "tolerance": tol_name, "tol": tol, "start": sname,
                    "converged": bool(eq.converged), "n_outer": int(eq.n_outer),
                    "final_max_log_change": float(eq.max_log_change),
                    "Wbar_pct_change": w,
                    "abs_diff_Wbar_pct_vs_ref_pp": abs(w - wref),
                    "max_abs_diff_tau_min_vs_ref": float(np.max(np.abs(eq.tau_min_kl - ref.tau_min_kl))),
                    "max_abs_dlog_tau_vs_ref": float(np.max(np.abs(np.log(eq.tau_min_kl) - np.log(ref.tau_min_kl)))),
                    "max_abs_dlog_lR_vs_ref": float(np.max(np.abs(np.log(np.clip(eq.l_R, EPS, None))
                                                                  - np.log(np.clip(ref.l_R, EPS, None))))),
                    "rho_A": spectral_radius_A(prim.edge_i, prim.edge_j, eq.tau_min_kl, prim.n_nodes, kappa=k),
                })
                print(json.dumps(rows[-1]), flush=True)
    ms = pd.DataFrame(rows)
    ms.to_csv(out / "multistart.csv", index=False)

    # 3. selection overlap with b025
    s0 = pd.read_csv(OUT / B025_TIDAL / "top100_d1.0" / "selected_pairs.csv")
    s1 = pd.read_csv(tdir / "top100_d1.0" / "selected_pairs.csv")
    e0 = set(s0.slow_edge_idx) | set(s0.fast_edge_idx)
    e1 = set(s1.slow_edge_idx) | set(s1.fast_edge_idx)
    p0 = set(zip(s0.slow_edge_idx, s0.fast_edge_idx))
    p1 = set(zip(s1.slow_edge_idx, s1.fast_edge_idx))
    pd.DataFrame([{"beta_c": beta, "n_edges": len(e1), "edge_overlap_with_b025": len(e0 & e1),
                   "n_pairs": len(p1), "pair_overlap_with_b025_same_direction": len(p0 & p1),
                   "selected_in_rerun_equals_saved": bool({(int(s_), int(f_)) for s_, f_ in sel.treated_pairs}
                                                          == {(int(s_), int(f_)) for s_, f_ in p1})}]).to_csv(
        out / "selection.csv", index=False)

    # 4. outer-map monotonicity (long run)
    mono = []
    x0 = np.log(base.tau_min_kl)
    for lanes_name, lanes in (("baseline", prim.n_kl), ("tidal_top100", n_cf)):
        for scale in (0.02, 0.1, 0.3):
            for p in range(a.n_pairs):
                x1 = x0 + scale * rng.standard_normal(len(x0))
                x2 = x0 + scale * rng.standard_normal(len(x0))
                g1, g2 = outer_map(prim, fund, params, kw, lanes, x1), outer_map(prim, fund, params, kw, lanes, x2)
                dx = x1 - x2
                mono.append({"beta_c": beta, "lanes": lanes_name, "scale": scale, "pair": p,
                             "r": float((g1 - g2) @ dx / (dx @ dx)),
                             "lipschitz_ratio": float(np.linalg.norm(g1 - g2) / np.linalg.norm(dx))})
    pd.DataFrame(mono).to_csv(out / "outer_map.csv", index=False)
    (out / "setup.json").write_text(json.dumps({
        "beta_c": beta, "version_id": VERSION_ID, "tidal_dir": a.tidal_dir, "kappa": kinfo,
        "xi_floor": xi_floor, "solver_kwargs": {k_: v for k_, v in kw.items() if np.isscalar(v)},
        "alpha": params.alpha, "beta_amenity": params.beta, "theta": params.theta, "route_eta": params.route_eta,
    }, indent=2, default=float))
    print("done", out, flush=True)


def summary(argv: list[str]) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", required=True)
    a = ap.parse_args(argv)
    out = OUT / a.out_dir
    betas = sorted(TIDAL_DIRS)
    col = {b: f"beta_c={b:.2f}" for b in betas}
    tid = {b: pd.read_csv(OUT / d / "tidal_st_lt_compact.csv").iloc[0] for b, d in TIDAL_DIRS.items()}
    pol = {b: pd.read_csv(OUT / d / "summary_direct_vs_longrun.csv").set_index("policy_id")
           for b, d in POLICY_DIRS.items() if (OUT / d / "summary_direct_vs_longrun.csv").exists()}

    def chk(b, name):
        p = out / f"checks_{_tag(b)}" / name
        return pd.read_csv(p) if p.exists() else None

    rows = []

    def add(section, item, vals):
        rows.append({"section": section, "item": item, **{col[b]: vals.get(b, np.nan) for b in betas}})

    add("tidal_top100", "Wbar_pct_short_run", {b: t.Wbar_pct_change_st for b, t in tid.items()})
    add("tidal_top100", "Wbar_pct_long_run", {b: t.Wbar_pct_change_lt for b, t in tid.items()})
    add("tidal_top100", "long_over_short_welfare", {b: t.Wbar_pct_change_lt / t.Wbar_pct_change_st for b, t in tid.items()})
    add("tidal_top100", "time_saved_sec_per_person_short_run",
        {b: t.physical_time_saved_st_sec_per_commuter for b, t in tid.items()})
    add("tidal_top100", "time_saved_sec_per_person_long_run",
        {b: t.physical_time_saved_lt_sec_per_commuter for b, t in tid.items()})
    add("tidal_top100", "decomp_direct_st_sec", {b: t.direct_st_cost_change_sec for b, t in tid.items()})
    add("tidal_top100", "decomp_ge_wedge_sec", {b: t.ge_cost_wedge_sec for b, t in tid.items()})
    add("tidal_top100", "decomp_sorting_wedge_sec", {b: t.sorting_wedge_sec for b, t in tid.items()})
    add("tidal_top100", "decomp_total_lt_sec", {b: t.total_lt_cost_change_sec for b, t in tid.items()})
    add("tidal_vs_expansion", "P1_over_P2_long_run_welfare",
        {b: p.loc["P1_tidal", "Wbar_pct_lt"] / p.loc["P2_expansion_same_corridor", "Wbar_pct_lt"] for b, p in pol.items()})
    add("tidal_vs_expansion", "P1_over_P2_short_run_welfare",
        {b: p.loc["P1_tidal", "Wbar_pct_st"] / p.loc["P2_expansion_same_corridor", "Wbar_pct_st"] for b, p in pol.items()})
    for pid in ["P1_tidal", "P2_expansion_same_corridor", "P3_expansion_qdelay", "P4_expansion_uniform_matched",
                "P5_uniform_speed_1.10", "P6_first_best_pricing", "P7_cordon_pricing", "P6b_baseline_pigou_fixed_toll"]:
        add("policy_long_run_Wbar_pct", pid, {b: p.loc[pid, "Wbar_pct_lt"] for b, p in pol.items() if pid in p.index})
        add("policy_short_run_Wbar_pct", pid, {b: p.loc[pid, "Wbar_pct_st"] for b, p in pol.items() if pid in p.index})
    main7 = ["P1_tidal", "P2_expansion_same_corridor", "P3_expansion_qdelay", "P4_expansion_uniform_matched",
             "P5_uniform_speed_1.10", "P6_first_best_pricing", "P7_cordon_pricing"]
    for b, p in pol.items():
        order = p.loc[main7, "Wbar_pct_lt"].sort_values(ascending=False)
        for rank, pid in enumerate(order.index, 1):
            rows.append({"section": "policy_rank_long_run", "item": pid,
                         **{col[x]: (rank if x == b else np.nan) for x in betas}})
    rank_rows = [r for r in rows if r["section"] == "policy_rank_long_run"]
    rows = [r for r in rows if r["section"] != "policy_rank_long_run"]
    for pid in main7:
        merged = {}
        for r in rank_rows:
            if r["item"] == pid:
                merged.update({k: v for k, v in r.items() if k.startswith("beta_c") and pd.notna(v)})
        rows.append({"section": "policy_rank_long_run", "item": pid, **{col[b]: merged.get(col[b], np.nan) for b in betas}})
    for b, p in pol.items():
        for c in ["direct_st_sec", "ge_wedge_sec", "sorting_wedge_sec", "total_lt_sec",
                  "phys_time_saved_st_sec", "phys_time_saved_lt_sec", "rho_max"]:
            rows.append({"section": "P1_decomposition_policy_run", "item": c,
                         **{col[x]: (p.loc["P1_tidal", c] if x == b else np.nan) for x in betas}})
    # merge duplicate decomposition rows
    df = pd.DataFrame(rows)
    df = df.groupby(["section", "item"], sort=False).first().reset_index()

    conv = {b: chk(b, "convergence.csv") for b in betas}
    ms = {b: chk(b, "multistart.csv") for b in betas}
    sel = {b: chk(b, "selection.csv") for b in betas}
    mono = {b: chk(b, "outer_map.csv") for b in betas}
    extra = []

    def addc(item, f):
        vals = {}
        for b in betas:
            try:
                vals[b] = f(b)
            except Exception:  # check not run for this beta
                vals[b] = np.nan
        extra.append({"section": "checks", "item": item, **{col[b]: vals[b] for b in betas}})

    for eqn in ("baseline", "tidal_short_run", "tidal_long_run"):
        g = lambda b, c, e=eqn: conv[b].set_index("equilibrium").loc[e, c]  # noqa: E731
        addc(f"{eqn}_converged", lambda b, g=g: bool(g(b, "converged")))
        addc(f"{eqn}_n_outer", lambda b, g=g: int(g(b, "n_outer")))
        addc(f"{eqn}_final_max_log_change", lambda b, g=g: g(b, "final_max_log_change"))
        addc(f"{eqn}_rho_A", lambda b, g=g: g(b, "rho_A"))
        addc(f"{eqn}_share_time_floor_binding", lambda b, g=g: g(b, "share_time_floor_strict"))
        addc(f"{eqn}_share_at_or_on_time_floor", lambda b, g=g: g(b, "share_time_floor_incl_ties"))
        addc(f"{eqn}_share_flow_below_xi_floor", lambda b, g=g: g(b, "share_flow_below_xi_floor"))
    for hz in ("short_run", "long_run"):
        for tl in ("production", "tight"):
            sub = lambda b, h=hz, t=tl: ms[b][(ms[b].horizon == h) & (ms[b].tolerance == t)]  # noqa: E731
            addc(f"multistart_{hz}_{tl}_n_starts", lambda b, s=sub: len(s(b)))
            addc(f"multistart_{hz}_{tl}_all_converged", lambda b, s=sub: bool(s(b).converged.all()))
            addc(f"multistart_{hz}_{tl}_max_abs_diff_tau_min", lambda b, s=sub: s(b).max_abs_diff_tau_min_vs_ref.max())
            addc(f"multistart_{hz}_{tl}_max_abs_dlog_tau", lambda b, s=sub: s(b).max_abs_dlog_tau_vs_ref.max())
            addc(f"multistart_{hz}_{tl}_max_abs_diff_Wbar_pp", lambda b, s=sub: s(b).abs_diff_Wbar_pct_vs_ref_pp.max())
            addc(f"multistart_{hz}_{tl}_max_n_outer", lambda b, s=sub: int(s(b).n_outer.max()))
    addc("selection_edge_overlap_with_b025_of_200", lambda b: int(sel[b].edge_overlap_with_b025.iloc[0]))
    addc("selection_pair_overlap_with_b025_of_100", lambda b: int(sel[b].pair_overlap_with_b025_same_direction.iloc[0]))
    for ln in ("baseline", "tidal_top100"):
        addc(f"outer_map_r_max_{ln}", lambda b, ln=ln: mono[b][mono[b].lanes == ln].r.max())
        addc(f"outer_map_share_r_lt_1_{ln}", lambda b, ln=ln: float((mono[b][mono[b].lanes == ln].r < 1).mean()))
        addc(f"outer_map_lipschitz_max_{ln}", lambda b, ln=ln: mono[b][mono[b].lanes == ln].lipschitz_ratio.max())
    df = pd.concat([df, pd.DataFrame(extra)], ignore_index=True)
    df.to_csv(out / "summary.csv", index=False)

    if pol:
        pr = pd.concat({f"{b:.2f}": p.loc[main7, ["Wbar_pct_st", "Wbar_pct_lt", "phys_time_saved_st_sec",
                                                  "phys_time_saved_lt_sec", "converged_st", "converged_lt",
                                                  "rho_max"]] for b, p in pol.items()}, names=["beta_c"])
        pr.reset_index().to_csv(out / "policy_long_run_by_beta.csv", index=False)
    with pd.option_context("display.width", 250, "display.max_columns", 20, "display.max_rows", 300):
        print(df.to_string(index=False))


if __name__ == "__main__":
    mode, rest = sys.argv[1], sys.argv[2:]
    {"checks": checks, "summary": summary}[mode](rest)
