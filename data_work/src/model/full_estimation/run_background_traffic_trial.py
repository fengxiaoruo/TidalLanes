"""Background (non-commuting) traffic in the congestion function: tidal-lane trial (2026-11-15).

Mainline setup exactly (run_beta_c_rerun_checks._setup: 1B bundle alldaytt0709, ce_floor beta 0.35,
xi floor 1% of median Xi_obs, spectral kappa, theta 6.83, alpha 0.05, beta -0.10, damping 0.1,
tol 1e-4, 500 outer, score_bpr_st top100). No existing file is edited: at call time
equilibrium_solve._congestion_tau_update is replaced (only while a background spec is active and
form == 'ce_floor') by

    dev_e = ((max(Xi_e, xf) + B_e) / (max(Xi_obs_e, xf) + B_e_obs)) * n_obs_e / n_e
    tau_e = max(min(tau_ff_e, tau_obs_e), tau_obs_e * dev_e^beta)

B = 0 is the mainline form; at the observation (Xi = Xi_obs, n = n_obs, B = B_obs) dev = 1, so the
baseline anchor holds for every B.

(A) fixed background: B_e = B_e_obs fixed vector (commuter-equivalent units, i.e. Xi units).
(B) rerouting background: a fixed OD matrix L^B routed with the model's own route kernel at the
    current link times, B_e(tau) = k * (1/tau_e) * [R' (L^B / R) R']_e, R = (I - A)^-1,
    A = 1/(kappa tau) (the formula of equilibrium_solve._route_traffic, same theta, same kappa,
    no location choice). R is captured by wrapping equilibrium_solve._resolvent_and_tau.

Background shapes (edge-level, before scaling):
  obs  observed 07-09 weekday non-commuting car flow on gate5 edges (RN all-purpose length flow
       all_am minus strict home-work HW; analysis/edge_commuter_share_weekday.csv), matched to 1B
       edges by grid pair. Unmatched 1B edges: c * routed non-HW OD flow, with c fitted so the two
       sum to the same total on matched edges.
  od   non-HW all-purpose endpoint OD (platform 0751-0759, dep 06-10, /19 days) routed at tau_obs.
  res  residual: max(k_all * all_am - Xi_obs, 0) (total = observed where possible); same imputation.
Scales (multiple m = sum B_obs / sum Xi_obs over all edges):
  rn     k = sum Xi_obs / sum HW over matched edges (model commuters = observed HW commuters)
  hv     k = sum Xi_obs / sum HW over matched edges in the top quintile of observed commuter share
  s356 / s456  m = (1-s)/s with platform car trip shares 35.6% (strict HW) / 45.6% (commute chain)
  odk    k = sum Xi_obs / sum routed HW endpoint OD (same platform data, same band)
  x0.5, x1, x2, x3  fixed multiples

Output: outputs/<output-name>/ (refuses to overwrite an existing step directory).
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import replace as dc_replace
from pathlib import Path

import numpy as np
import pandas as pd

DATA_WORK = Path(__file__).resolve().parents[3]
if str(DATA_WORK) not in sys.path:
    sys.path.insert(0, str(DATA_WORK))

from src.model.full_estimation import equilibrium_solve as es  # noqa: E402
from src.model.full_estimation import stage2_counterfactuals as s2  # noqa: E402
from src.model.full_estimation.counterfactual_levels import (  # noqa: E402
    apply_bidirectional_lane_delta, select_tidal_lane_pairs_by_rank)
from src.model.full_estimation.equilibrium_solve import MAINLINE_PAIR_SELECTION, spectral_radius_A  # noqa: E402
from src.model.full_estimation.run_beta_c_rerun_checks import _setup  # noqa: E402
from src.model.full_estimation.tidal_lane_diagnostics import od_cost_decomposition  # noqa: E402
from src.validation.check_proportional_background import endpoint_od  # noqa: E402

OUT = DATA_WORK / "outputs"
MAIN_DIR = OUT / "tidal_lane_diagnostics_osm1b_ce_floor_b035_fl001_ks_od2025_alldaytt0709_v1"
SHARE_FILE = OUT / "all_traffic_speed_panel_rn202505_gate5_v1/analysis/edge_commuter_share_weekday.csv"
EPS = es.EPS
BETA = 0.35
T0 = time.time()
_ORIG_UPDATE = es._congestion_tau_update
_ORIG_RESOLVENT = es._resolvent_and_tau
CTRL: dict = {}
STASH: dict = {}
NONHW = ["WH", "home_other", "work_other", "other"]


def log(*a):
    print(f"[{time.time() - T0:6.0f}s]", *a, flush=True)


# ------------------------------------------------------------------ runtime patches
def patched_resolvent(A, eta=1.0):
    R, tinv = _ORIG_RESOLVENT(A, eta)
    if CTRL.get("reroute"):
        STASH["R"] = R
    return R, tinv


def route_xi(L: np.ndarray, R: np.ndarray, tau: np.ndarray, ei, ej) -> np.ndarray:
    """Edge flow of OD matrix L in Xi units (kappa x passages) = (1/tau_e) [R' (L/R) R']_e."""
    G = np.divide(L, R, out=np.zeros_like(L), where=R > 0)
    Y = R.T @ G @ R.T
    return Y[ei, ej] / np.clip(tau, EPS, None)


def current_background(Xi_kl) -> np.ndarray:
    if not CTRL.get("reroute"):
        return CTRL["B"]
    R = STASH["R"]
    tau = STASH["tau_cur"]   # link times of the A that produced R (one edge per node pair, checked at build)
    return CTRL["k"] * route_xi(CTRL["L"], R, tau, CTRL["ei"], CTRL["ej"])


def patched_update(*, form, lam, tau_ff, tau_obs, Xi_kl, n_kl, ell_km, xi_obs_kl, n_obs_kl, xi_floor=0.0):
    if not CTRL or form != "ce_floor":
        return _ORIG_UPDATE(form=form, lam=lam, tau_ff=tau_ff, tau_obs=tau_obs, Xi_kl=Xi_kl, n_kl=n_kl,
                            ell_km=ell_km, xi_obs_kl=xi_obs_kl, n_obs_kl=n_obs_kl, xi_floor=xi_floor)
    B = current_background(Xi_kl)
    num = np.maximum(Xi_kl, xi_floor) + B
    den = np.maximum(xi_obs_kl, xi_floor) + CTRL["B_obs"]
    if xi_floor <= 0:
        num, den = np.clip(Xi_kl + B, EPS, None), np.clip(xi_obs_kl + CTRL["B_obs"], EPS, None)
    dev = (num / den) * np.clip(n_obs_kl / np.clip(n_kl, EPS, None), EPS, None)
    floor = np.minimum(tau_ff, tau_obs)
    return np.maximum(floor, tau_obs * dev ** lam)


# ------------------------------------------------------------------ background construction
def build_background(prim, kw, base0, out: Path) -> dict:
    """Edge table with observed flows, routed OD flows, and every B_obs vector."""
    ei, ej = prim.edge_i, prim.edge_j
    if pd.Series(list(zip(ei, ej))).duplicated().any():
        raise SystemExit("parallel edges present; rerouting wrapper assumes one edge per node pair")
    if np.any(ei == ej):
        raise SystemExit("self-loop edges present")
    k = float(kw["kappa"])
    tau_obs = kw["tau_obs_anchor"]
    xi_obs = kw["xi_obs_kl"]
    if np.max(np.abs(base0.Xi_kl / xi_obs - 1)) > 1e-8:
        raise SystemExit("baseline Xi differs from Xi_obs")
    g = prim.node_grid_ids
    e = pd.DataFrame({"edge_idx": np.arange(prim.n_edges), "grid_from": g[ei], "grid_to": g[ej],
                      "tau_obs": tau_obs, "tau_ff": prim.tau_ff_min, "n_lanes": prim.n_kl, "km": prim.ell_kl_km,
                      "Xi_obs": xi_obs})
    sh = pd.read_csv(SHARE_FILE)
    e = e.merge(sh[["grid_from", "grid_to", "HW", "all_am"]], on=["grid_from", "grid_to"], how="left")
    e["matched"] = e.all_am.notna()
    # routed OD flows at tau_obs (Xi units)
    A = es._build_A_matrix(ei, ej, tau_obs, prim.n_nodes, kappa=k)
    R, _ = _ORIG_RESOLVENT(A, 1.0)
    idx = {gg: i for i, gg in enumerate(g)}
    L_hw, cov_hw = endpoint_od("dep_0610", ["HW"], idx, prim.n_nodes)
    L_all, cov_all = endpoint_od("dep_0610", ["HW"] + NONHW, idx, prim.n_nodes)
    L_non, _ = endpoint_od("dep_0610", NONHW, idx, prim.n_nodes)
    for L in (L_hw, L_all, L_non):
        np.fill_diagonal(L, 0.0)
    L_c = prim.L_ij_obs.astype(float).copy()
    np.fill_diagonal(L_c, 0.0)
    e["rt_hw"] = route_xi(L_hw, R, tau_obs, ei, ej)
    e["rt_nonhw"] = route_xi(L_non, R, tau_obs, ei, ej)
    e["rt_commute_od"] = route_xi(L_c, R, tau_obs, ei, ej)
    m = e.matched.to_numpy()
    S_xi = float(xi_obs.sum())

    def impute(v_matched_vals):
        """Fill unmatched edges with c * routed non-HW flow, c fitted on matched edges."""
        v = np.where(m, v_matched_vals, np.nan)
        c = np.nansum(v[m]) / e.rt_nonhw[m].sum()
        return np.where(m, v, c * e.rt_nonhw.to_numpy()), c

    nonhw_obs = np.clip((e.all_am - e.HW).to_numpy(), 0.0, None)
    shape_obs, c_obs = impute(nonhw_obs)                       # in RN units
    k_rn = float(xi_obs[m].sum() / e.HW[m].sum())
    sc = (e.HW / e.all_am).to_numpy()
    okq = m & (e.all_am.to_numpy() >= 100)
    q80 = np.nanquantile(sc[okq], 0.8)
    top = okq & (sc >= q80)
    k_hv = float(xi_obs[top].sum() / e.HW[top].sum())
    k_od = float(S_xi / e.rt_hw.sum())
    cal = {"kappa": k, "sum_Xi_obs": S_xi, "n_edges": int(len(e)), "n_matched": int(m.sum()),
           "treated_unmatched": None, "endpoint_coverage_hw": cov_hw, "endpoint_coverage_all": cov_all,
           "obs_imputation_c_rn_units_per_routed_xi": float(c_obs),
           "k_rn": k_rn, "k_hv": k_hv, "hv_commuter_share_cut_q80": float(q80), "hv_n_edges": int(top.sum()),
           "k_od": k_od, "matched_HW_over_all": float(e.HW[m].sum() / e.all_am[m].sum())}
    Bs: dict[str, np.ndarray] = {}
    Bs["obs_rn"] = k_rn * shape_obs
    Bs["obs_hv"] = k_hv * shape_obs
    mult_obs = shape_obs.sum() / S_xi
    for name, mm in (("obs_s356", (1 - 0.356) / 0.356), ("obs_s456", (1 - 0.456) / 0.456),
                     ("obs_x0.5", 0.5), ("obs_x1", 1.0), ("obs_x2", 2.0), ("obs_x3", 3.0)):
        Bs[name] = shape_obs * (mm / mult_obs)
    Bs["od_odk"] = k_od * e.rt_nonhw.to_numpy()
    # od shape at the main multiple, for a shape comparison at equal scale
    Bs["od_at_obs_rn"] = e.rt_nonhw.to_numpy() * (Bs["obs_rn"].sum() / e.rt_nonhw.sum())
    # residual: total = k_all * all_am where above Xi_obs; k_all set so the multiple equals obs_rn
    target = Bs["obs_rn"].sum()
    allv = e.all_am.to_numpy()

    def res_vec(kk):
        v = np.where(m, np.clip(kk * np.nan_to_num(allv) - xi_obs, 0.0, None), np.nan)
        cc = np.nansum(v[m]) / e.rt_nonhw[m].sum()
        return np.where(m, v, cc * e.rt_nonhw.to_numpy())
    lo, hi = 0.0, 50.0
    for _ in range(100):
        mid = 0.5 * (lo + hi)
        if res_vec(mid).sum() < target:
            lo = mid
        else:
            hi = mid
    Bs["res_at_obs_rn"] = res_vec(0.5 * (lo + hi))
    cal["k_res"] = 0.5 * (lo + hi)
    for n, v in Bs.items():
        e[f"B_{n}"] = v
        cal[f"multiple_{n}"] = float(v.sum() / S_xi)
    sel = pd.read_csv(MAIN_DIR / "top100_d1.0" / "selected_pairs.csv")
    e["role"] = "other"
    e.loc[e.edge_idx.isin(sel.slow_edge_idx), "role"] = "heavy"
    e.loc[e.edge_idx.isin(sel.fast_edge_idx), "role"] = "light"
    cal["treated_unmatched"] = int(((e.role != "other") & ~e.matched).sum())
    e.to_parquet(out / "edge_background.parquet", index=False)
    (out / "calibration.json").write_text(json.dumps(cal, indent=2))
    return {"edges": e, "B": Bs, "cal": cal, "L_non": L_non, "R_obs": R}


def direction_table(e: pd.DataFrame, sel_pairs: np.ndarray, Bs: dict) -> pd.DataFrame:
    h, l = sel_pairs[:, 0], sel_pairs[:, 1]
    both = (e.matched.to_numpy()[h]) & (e.matched.to_numpy()[l])
    rows = []

    def add(label, v, note=""):
        v = np.asarray(v, float)
        r = v[l] / np.where(v[h] > 0, v[h], np.nan)
        rows.append({"flow": label, "median_light_over_heavy_100": float(np.nanmedian(r)),
                     "sum_light_over_sum_heavy_100": float(v[l].sum() / v[h].sum()),
                     "median_light_over_heavy_68matched": float(np.nanmedian(r[both])),
                     "sum_ratio_68matched": float(v[l][both].sum() / v[h][both].sum()), "note": note})
    add("model commuting Xi_obs (B=0)", e.Xi_obs, "benchmark")
    add("observed all-purpose car flow 07-09 (RN all_am)", e.all_am, "observed, 68 matched pairs only")
    add("observed strict home-work flow (RN HW)", e.HW, "observed commuters, 68 matched pairs only")
    add("observed commuting OD routed on model kernel", e.rt_commute_od, "prim.L_ij_obs routed at tau_obs")
    add("routed HW endpoint OD", e.rt_hw, "")
    add("routed non-HW endpoint OD", e.rt_nonhw, "")
    for n, v in Bs.items():
        add(f"total Xi_obs + B [{n}]", e.Xi_obs.to_numpy() + v, f"multiple {v.sum() / e.Xi_obs.sum():.2f}")
    return pd.DataFrame(rows)


# ------------------------------------------------------------------ accounting
def accounting(prim, kappa, base, eq, B0, B1, groups: dict) -> dict:
    """Vehicle-time changes, seconds per commuter (divide by Lbar*kappa as the mainline does)."""
    s = 60.0 / (prim.Lbar * kappa)
    m0, m1 = base.tau_min_kl, eq.tau_min_kl
    out = {}
    tot0 = s * float(np.sum((base.Xi_kl + B0) * m0))
    out["base_all_vehicle_time_sec_per_commuter"] = tot0
    out["base_commuter_time_sec_per_commuter"] = s * float(np.sum(base.Xi_kl * m0))
    for gname, gm in list(groups.items()) + [("all", np.ones(len(m0), bool))]:
        c = s * float(np.sum(eq.Xi_kl[gm] * m1[gm] - base.Xi_kl[gm] * m0[gm]))
        b = s * float(np.sum(B1[gm] * m1[gm] - B0[gm] * m0[gm]))
        out[f"dT_commuter_{gname}"] = c
        out[f"dT_background_{gname}"] = b
        out[f"dT_allveh_{gname}"] = c + b
    for who in ("commuter", "background", "allveh"):
        gain = -out[f"dT_{who}_heavy"]
        loss = out[f"dT_{who}_light"]
        out[f"light_loss_over_heavy_gain_{who}"] = loss / gain if gain != 0 else np.nan
    out["allveh_time_change_pct_of_base"] = 100.0 * out["dT_allveh_all"] / tot0
    return out


# ------------------------------------------------------------------ scenarios
def solve(S, n_cf, horizon, base, init=None, tol=None):
    prim, params, fund, kw = S["prim"], S["params"], S["fund"], dict(S["kw"])
    if tol is not None:
        kw.update(tol=tol, max_outer=4000)
    pin = dict(l_R_fixed=base.l_R, l_F_fixed=base.l_F) if horizon == "short_run" else {}
    return es.solve_equilibrium(prim, fund.ubar_theta, fund.abar_theta, params, n_kl_override=n_cf,
                                tau_min_init=(prim.tau_obs_min.copy() if init is None else init.copy()),
                                endogenous_congestion=True, label=f"bg_{horizon}", **pin, **kw)


def eq_background(S, eq, spec) -> np.ndarray:
    if not spec.get("reroute"):
        return spec["B"]
    prim, k = S["prim"], float(S["kw"]["kappa"])
    A = es._build_A_matrix(prim.edge_i, prim.edge_j, eq.tau_min_kl, prim.n_nodes, kappa=k)
    R, _ = _ORIG_RESOLVENT(A, 1.0)
    return spec["k"] * route_xi(spec["L"], R, eq.tau_min_kl, prim.edge_i, prim.edge_j)


def activate(spec, S):
    CTRL.clear()
    STASH.clear()
    if spec is None:
        return
    CTRL.update(spec)
    if spec.get("reroute"):
        CTRL.update(ei=S["prim"].edge_i, ej=S["prim"].edge_j)


def run_spec(name, spec, S, out, sel_variants=("same", "reselect"), do_p2=True):
    """spec None = native mainline (no patch). Returns rows, one per selection variant."""
    prim, params, fund, kw = S["prim"], S["params"], S["fund"], S["kw"]
    kappa = float(kw["kappa"])
    rows = []
    t = time.time()
    activate(spec, S)
    try:
        base = s2.solve_baseline_endogenous(prim, fund, params, **kw)
        B0 = np.zeros(prim.n_edges) if spec is None else spec["B_obs"]
        for sv in sel_variants:
            if sv == "same":
                sel = S["sel_main"]
            else:
                if spec is None:
                    continue
                sel = select_tidal_lane_pairs_by_rank(prim, top_k=100, delta=1.0, rank=MAINLINE_PAIR_SELECTION,
                                                      xi_kl=base.Xi_kl + B0)
            pairs = np.asarray(sel.treated_pairs, int)
            n_p1 = prim.n_kl.copy()
            n_p1[pairs[:, 0]] += 1.0
            n_p1[pairs[:, 1]] -= 1.0
            n_p2 = apply_bidirectional_lane_delta(prim, sel, delta=1.0)
            groups = {"heavy": np.isin(np.arange(prim.n_edges), pairs[:, 0]),
                      "light": np.isin(np.arange(prim.n_edges), pairs[:, 1])}
            groups["other"] = ~(groups["heavy"] | groups["light"])
            res = {}
            for pol, ncf in (("P1", n_p1), ("P2", n_p2)):
                if pol == "P2" and not do_p2:
                    continue
                for h in ("short_run", "long_run"):
                    res[(pol, h)] = solve(S, ncf, h, base)
            main_set = {(int(a), int(b)) for a, b in S["sel_main"].treated_pairs}
            r = {"spec": name, "selection": sv, "desc": "" if spec is None else spec.get("desc", ""),
                 "multiple": 0.0 if spec is None else float(B0.sum() / base.Xi_kl.sum()),
                 "reroute": bool(spec and spec.get("reroute")),
                 "n_pairs_same_as_mainline": len({(int(a), int(b)) for a, b in pairs} & main_set),
                 "base_converged": bool(base.converged), "base_n_outer": int(base.n_outer),
                 "base_max_abs_dlog_tau_vs_tau_obs": float(np.max(np.abs(np.log(base.tau_min_kl / kw["tau_obs_anchor"])))),
                 "base_Wbar": base.Wbar}
            h_, l_ = pairs[:, 0], pairs[:, 1]
            tot0 = base.Xi_kl + B0
            r["dir_median_light_over_heavy_total_base"] = float(np.median(tot0[l_] / tot0[h_]))
            r["dir_sum_light_over_heavy_total_base"] = float(tot0[l_].sum() / tot0[h_].sum())
            r["dir_median_light_over_heavy_commuter_base"] = float(np.median(base.Xi_kl[l_] / base.Xi_kl[h_]))
            for (pol, h), eq in res.items():
                tg = f"{pol}_{'st' if h == 'short_run' else 'lt'}"
                B1 = eq_background(S, eq, spec) if spec is not None else np.zeros(prim.n_edges)
                r[f"{tg}_Wbar_pct"] = 100.0 * (eq.Wbar / base.Wbar - 1.0)
                acc = accounting(prim, kappa, base, eq, B0, B1, groups)
                r.update({f"{tg}_{k}": v for k, v in acc.items() if not k.startswith("base_")})
                r["base_all_vehicle_time_sec_per_commuter"] = acc["base_all_vehicle_time_sec_per_commuter"]
                r["base_commuter_time_sec_per_commuter"] = acc["base_commuter_time_sec_per_commuter"]
                r[f"{tg}_commuter_time_saved_sec"] = -acc["dT_commuter_all"]
                r[f"{tg}_allveh_time_saved_sec"] = -acc["dT_allveh_all"]
                r[f"{tg}_background_time_saved_sec"] = -acc["dT_background_all"]
                for gname, gm in groups.items():
                    d = eq.tau_min_kl[gm] / base.tau_min_kl[gm] - 1.0
                    w = tot0[gm]
                    r[f"{tg}_median_pct_dtau_{gname}"] = 100 * float(np.median(d))
                    r[f"{tg}_totalflow_wtd_pct_dtau_{gname}"] = 100 * float(np.sum(w * d) / np.sum(w))
                tot1 = eq.Xi_kl + B1
                r[f"{tg}_dir_median_light_over_heavy_total"] = float(np.median(tot1[l_] / tot1[h_]))
                r[f"{tg}_converged"] = bool(eq.converged)
                r[f"{tg}_n_outer"] = int(eq.n_outer)
                r[f"{tg}_max_log_change"] = float(eq.max_log_change)
                r[f"{tg}_rho_A"] = spectral_radius_A(prim.edge_i, prim.edge_j, eq.tau_min_kl, prim.n_nodes, kappa=kappa)
                floor = np.minimum(prim.tau_ff_min, kw["tau_obs_anchor"])
                r[f"{tg}_share_tau_at_floor"] = float(np.mean(eq.tau_min_kl <= floor * (1 + 1e-6)))
                r[f"{tg}_share_flow_below_xi_floor"] = float(np.mean(eq.Xi_kl < kw["xi_floor"]))
                if pol == "P1" and sv == "same" and spec is None:
                    dec = od_cost_decomposition(base, res[("P1", "short_run")], res[("P1", "long_run")], prim,
                                                params, fund.abar_theta)
                    r["check_mainline_physical_time_saved_st"] = dec["physical_time_saved_st_sec_per_commuter"]
                    r["check_mainline_physical_time_saved_lt"] = dec["physical_time_saved_lt_sec_per_commuter"]
                if pol == "P1":
                    ed = pd.DataFrame({"edge_idx": np.r_[h_, l_], "role": ["heavy"] * len(h_) + ["light"] * len(l_),
                                       "Xi_base": base.Xi_kl[np.r_[h_, l_]], "B_base": B0[np.r_[h_, l_]],
                                       "Xi_cf": eq.Xi_kl[np.r_[h_, l_]], "B_cf": B1[np.r_[h_, l_]],
                                       "tau_base": base.tau_min_kl[np.r_[h_, l_]], "tau_cf": eq.tau_min_kl[np.r_[h_, l_]]})
                    ed.to_csv(out / "edges" / f"{name}__{sv}__{tg}.csv", index=False)
            for h in ("st", "lt"):
                if f"P2_{h}_Wbar_pct" in r:
                    r[f"P1_over_P2_welfare_{h}"] = r[f"P1_{h}_Wbar_pct"] / r[f"P2_{h}_Wbar_pct"]
                    r[f"P1_over_P2_allveh_time_{h}"] = r[f"P1_{h}_allveh_time_saved_sec"] / r[f"P2_{h}_allveh_time_saved_sec"]
                    r[f"P1_over_P2_commuter_time_{h}"] = r[f"P1_{h}_commuter_time_saved_sec"] / r[f"P2_{h}_commuter_time_saved_sec"]
            r["runtime_sec"] = time.time() - t
            rows.append(r)
            if sv == "reselect":
                pd.DataFrame(pairs, columns=["slow_edge_idx", "fast_edge_idx"]).to_csv(
                    out / "edges" / f"{name}__reselected_pairs.csv", index=False)
    finally:
        activate(None, S)
    return rows


def probe(name, spec, S, out_rows):
    prim = S["prim"]
    activate(spec, S)
    try:
        base = s2.solve_baseline_endogenous(prim, S["fund"], S["params"], **S["kw"])
        pairs = np.asarray(S["sel_main"].treated_pairs, int)
        n_cf = prim.n_kl.copy()
        n_cf[pairs[:, 0]] += 1.0
        n_cf[pairs[:, 1]] -= 1.0
        starts = {"tau_obs": prim.tau_obs_min, "2x_tau_obs": 2 * prim.tau_obs_min, "tau_ff": prim.tau_ff_min,
                  "0.7x_tau_obs": 0.7 * prim.tau_obs_min}
        for h in ("short_run", "long_run"):
            ref = None
            for sn, init in starts.items():
                eq = solve(S, n_cf, h, base, init=init, tol=1e-7)
                ref = ref or eq
                w, wr = 100 * (eq.Wbar / base.Wbar - 1), 100 * (ref.Wbar / base.Wbar - 1)
                out_rows.append({"spec": name, "horizon": h, "start": sn, "tol": 1e-7, "converged": bool(eq.converged),
                                 "n_outer": int(eq.n_outer), "Wbar_pct": w, "abs_diff_Wbar_pp_vs_first": abs(w - wr),
                                 "max_abs_dlog_tau_vs_first": float(np.max(np.abs(np.log(eq.tau_min_kl / ref.tau_min_kl))))})
                log(json.dumps(out_rows[-1]))
    finally:
        activate(None, S)


# The rerouting spec needs tau at the A used for the current R. Wrap _build_A_matrix to remember it.
_ORIG_BUILD_A = es._build_A_matrix


def patched_build_A(edge_i, edge_j, tau_min_kl, N, kappa=1.0):
    if CTRL.get("reroute"):
        STASH["tau_cur"] = np.asarray(tau_min_kl, float).copy()
    return _ORIG_BUILD_A(edge_i, edge_j, tau_min_kl, N, kappa=kappa)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-name", default="background_traffic_trial_v1")
    ap.add_argument("--set", default="all", choices=("all", "smoke"))
    ap.add_argument("--no-probe", action="store_true")
    a = ap.parse_args(argv)
    root = OUT / a.output_name
    sub = root / ("cf" if a.set == "all" else "cf_smoke")
    if sub.exists():
        raise SystemExit(f"{sub} exists; refusing to overwrite")
    (sub / "edges").mkdir(parents=True)
    (sub / "command.txt").write_text(" ".join([sys.executable] + sys.argv) + "\n")
    es._congestion_tau_update = patched_update
    es._resolvent_and_tau = patched_resolvent
    es._build_A_matrix = patched_build_A

    prim, params, fund, kw, kinfo, anchors, xi_floor = _setup(BETA)
    base0 = s2.solve_baseline_endogenous(prim, fund, params, **kw)
    sel_main = select_tidal_lane_pairs_by_rank(prim, top_k=100, delta=1.0, rank=MAINLINE_PAIR_SELECTION,
                                               xi_kl=base0.Xi_kl)
    ref = pd.read_csv(MAIN_DIR / "top100_d1.0" / "selected_pairs.csv")
    same = {(int(x), int(y)) for x, y in zip(ref.slow_edge_idx, ref.fast_edge_idx)} == \
        {(int(x), int(y)) for x, y in sel_main.treated_pairs}
    log("selection equals saved mainline:", same)
    if not same:
        raise SystemExit("mainline selection not reproduced")
    S = {"prim": prim, "params": params, "fund": fund, "kw": kw, "sel_main": sel_main}
    bg = build_background(prim, kw, base0, sub)
    log(json.dumps(bg["cal"]))
    direction_table(bg["edges"], np.asarray(sel_main.treated_pairs, int), bg["B"]).to_csv(
        sub / "table_direction_ratio_baseline.csv", index=False)
    (sub / "setup.json").write_text(json.dumps({"kappa": kinfo, "xi_floor": xi_floor, "beta": BETA}, indent=2, default=float))

    V: dict[str, dict | None] = {"B0_native": None,
                                 "B0_patched": dict(B=np.zeros(prim.n_edges), B_obs=np.zeros(prim.n_edges),
                                                    desc="validation: patched form with B = 0")}
    descs = {"obs_rn": "MAIN: observed non-commuting flow, scale model commuters = observed HW (RN)",
             "obs_hv": "observed shape, scale matched on top-quintile commuter-share edges",
             "obs_s356": "observed shape, commuter share 35.6% (platform strict HW trips)",
             "obs_s456": "observed shape, commuter share 45.6% (platform commute chains)",
             "obs_x0.5": "observed shape, total B = 0.5 x commuting", "obs_x1": "observed shape, 1x",
             "obs_x2": "observed shape, 2x", "obs_x3": "observed shape, 3x",
             "od_odk": "routed non-HW OD, scale model commuters = routed HW endpoint OD",
             "od_at_obs_rn": "routed non-HW OD shape at the obs_rn multiple",
             "res_at_obs_rn": "residual shape max(k*all - Xi_obs, 0) at the obs_rn multiple"}
    for n, d in descs.items():
        V[n] = dict(B=bg["B"][n], B_obs=bg["B"][n], desc=d)
    if a.set == "smoke":
        V = {k_: V[k_] for k_ in ("B0_native", "B0_patched", "obs_rn")}
    # (B) rerouting background OD
    L_non = bg["L_non"]
    k_od = bg["cal"]["k_od"]
    mult_main = bg["B"]["obs_rn"].sum() / bg["B"]["od_odk"].sum()
    for n, kk, d in (("odR_odk", k_od, "REROUTING non-HW OD, scale as od_odk"),
                     ("odR_at_obs_rn", k_od * mult_main, "REROUTING non-HW OD at the obs_rn multiple")):
        if a.set == "smoke" and n != "odR_odk":
            continue
        B_obs = kk * bg["edges"].rt_nonhw.to_numpy()
        V[n] = dict(reroute=True, L=L_non, k=kk, B_obs=B_obs, B=B_obs, desc=d)

    rows, probes = [], []
    for n, spec in V.items():
        log("spec", n)
        rr = run_spec(n, spec, S, sub)
        rows += rr
        for r in rr:
            log(n, r["selection"], {k_: round(r[k_], 4) for k_ in
                                    ("multiple", "P1_st_Wbar_pct", "P1_lt_Wbar_pct", "P1_st_allveh_time_saved_sec",
                                     "P1_st_commuter_time_saved_sec", "P1_st_light_loss_over_heavy_gain_allveh")
                                    if k_ in r})
        pd.DataFrame(rows).to_csv(sub / "results.csv", index=False)
    if not a.no_probe:
        for n in ("obs_rn", "odR_odk"):
            if n in V:
                probe(n, V[n], S, probes)
                pd.DataFrame(probes).to_csv(sub / "probe_multistart_tight.csv", index=False)
    log("done")


if __name__ == "__main__":
    main()
