"""Trial (2026-09-28): gravity estimate of theta*kappa and an exponential route kernel.

This script does NOT touch the mainline. It answers two questions:

1. What is the semi-elasticity of observed road-commuting flows to travel
   minutes in Beijing?  PPML with origin and destination fixed effects,
       E[L_ij] = exp(FE_i + FE_j - nu * SP_ij),
   where SP_ij is the directed shortest-path time (minutes) on the gate5
   grid network at observed AM link minutes.  The same regression on the
   mainline model's own OD prediction gives the elasticity the current
   kernel A_kl = 1/m_kl imposes.  A log-log variant and the model-consistent
   check  L_ij ~ FE + b * ln R_ij  (b should be 1) are also reported.

2. What changes if the route kernel is  a_kl = exp(-rho*kappa*m_kl)  with the
   route-dispersion parameter rho separated from the residence-workplace
   Frechet shape theta, and kappa = nu/theta from the PPML above?  Then
       R = (I - A)^{-1},   tau_ij^{-theta} = R_ij^{theta/rho},
       Xi_kl = a_kl [R' G R']_{kl},  G_ij = L_ij / R_ij,
   with the same inversion, anchored congestion, TCS selection and ST/LT
   tidal experiment as the mainline.  rho is set so the largest row sum of
   A equals a target (default 0.8).  kernel="power" (a_kl = 1/m_kl, rho=theta)
   reproduces the mainline solver and is run first as a unit check.

Outputs go to data_work/outputs/route_kernel_exp_trial_20260928_v1/.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass, replace
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import linalg as sla
from scipy.sparse import csr_matrix
from scipy.sparse.csgraph import dijkstra

DATA_WORK = Path(__file__).resolve().parents[3]
if str(DATA_WORK) not in sys.path:
    sys.path.insert(0, str(DATA_WORK))

from src.model.full_estimation import build_primitives as bp  # noqa: E402
from src.model.full_estimation import parameters as pm  # noqa: E402
from src.model.full_estimation.build_primitives import Primitives  # noqa: E402
from src.model.full_estimation.counterfactual_levels import (  # noqa: E402
    TidalLaneSelection,
    select_tidal_lane_pairs_by_rank,
)
from src.model.full_estimation.equilibrium_solve import (  # noqa: E402
    _resolve_lanes,
    _solve_lR_lF_given_tau,
)

LOG = logging.getLogger("route_kernel_trial")
EPS = 1e-12


# --------------------------------------------------------------------------- #
# 1. Gravity / PPML
# --------------------------------------------------------------------------- #
def shortest_path_minutes(prim: Primitives, m: np.ndarray) -> np.ndarray:
    N = prim.n_nodes
    df = pd.DataFrame({"i": prim.edge_i, "j": prim.edge_j, "m": m}).groupby(["i", "j"], as_index=False)["m"].min()
    g = csr_matrix((df["m"].to_numpy(), (df["i"].to_numpy(), df["j"].to_numpy())), shape=(N, N))
    return dijkstra(g, directed=True)


def ppml_two_way_fe(y: np.ndarray, x: np.ndarray, mask: np.ndarray, max_iter: int = 500, tol: float = 1e-9) -> dict:
    """PPML  E[y_ij] = exp(a_i + b_j + beta * x_ij)  on cells where mask is True.

    Alternating closed-form updates of the fixed effects (Poisson first-order
    conditions) with a one-dimensional Newton step on beta.  Returns beta, a
    sandwich-free (Poisson ML) standard error, and pseudo-R2 statistics.
    """
    N = y.shape[0]
    y = np.where(mask, y, 0.0)
    x = np.where(mask, x, 0.0)
    a = np.zeros(N); b = np.zeros(N); beta = 0.0
    row_y = y.sum(1); col_y = y.sum(0)
    keep_r = row_y > 0; keep_c = col_y > 0
    for it in range(max_iter):
        mu = np.exp(a[:, None] + b[None, :] + beta * x) * mask
        # a_i update
        s = mu.sum(1); a_new = a.copy()
        a_new[keep_r] = a[keep_r] + np.log(row_y[keep_r] / np.clip(s[keep_r], EPS, None))
        a_new[~keep_r] = -50.0
        mu = np.exp(a_new[:, None] + b[None, :] + beta * x) * mask
        s = mu.sum(0); b_new = b.copy()
        b_new[keep_c] = b[keep_c] + np.log(col_y[keep_c] / np.clip(s[keep_c], EPS, None))
        b_new[~keep_c] = -50.0
        mu = np.exp(a_new[:, None] + b_new[None, :] + beta * x) * mask
        g = float(np.sum((y - mu) * x)); h = float(np.sum(mu * x * x))
        beta_new = beta + g / max(h, EPS)
        delta = max(abs(beta_new - beta), float(np.max(np.abs(a_new[keep_r] - a[keep_r]))), float(np.max(np.abs(b_new[keep_c] - b[keep_c]))))
        a, b, beta = a_new, b_new, beta_new
        if delta < tol:
            break
    mu = np.exp(a[:, None] + b[None, :] + beta * x) * mask
    # Poisson-ML s.e. on beta after partialling out the FEs (Frisch-Waugh on the weighted design):
    w = mu
    xw = x - (w * x).sum(1, keepdims=True) / np.clip(w.sum(1, keepdims=True), EPS, None)
    xw = xw - (w * xw).sum(0, keepdims=True) / np.clip(w.sum(0, keepdims=True), EPS, None)
    info = float(np.sum(w * xw * xw))
    se_ml = 1.0 / np.sqrt(max(info, EPS))
    # heteroskedasticity-robust (HC) version
    score = (y - mu) * xw
    se_hc = float(np.sqrt(np.sum(score * score))) / max(info, EPS)
    dev_null = None
    return {
        "beta": float(beta), "se_ml": float(se_ml), "se_hc": float(se_hc), "n_iter": it + 1,
        "n_cells": int(mask.sum()), "n_positive": int((y[mask] > 0).sum()),
        "sum_y": float(y.sum()), "corr_log_pos": float(np.corrcoef(np.log(y[mask & (y > 0)]), np.log(np.clip(mu[mask & (y > 0)], EPS, None)))[0, 1]),
    }


def gravity_block(prim: Primitives, sp: np.ndarray, L_obs: np.ndarray, L_model: np.ndarray, R_main: np.ndarray, out: Path) -> dict:
    N = prim.n_nodes
    offdiag = ~np.eye(N, dtype=bool)
    reach = np.isfinite(sp) & (sp > 0) & offdiag
    res = {}
    for label, L in (("observed", L_obs), ("mainline_model", L_model)):
        r = ppml_two_way_fe(L, -sp, reach)               # beta = -nu  -> flow falls with minutes
        res[f"{label}_semi_elasticity_nu_per_min"] = r["beta"]; res[f"{label}_nu_se_hc"] = r["se_hc"]
        res[f"{label}_nu_ncells"] = r["n_cells"]; res[f"{label}_nu_npos"] = r["n_positive"]
        r2 = ppml_two_way_fe(L, -np.log(np.where(reach, sp, 1.0)), reach)   # log-log: L ~ SP^{-b}
        res[f"{label}_loglog_elasticity_b"] = r2["beta"]; res[f"{label}_loglog_se_hc"] = r2["se_hc"]
        # 60-minute window (drop very long pairs that are mostly zeros)
        m60 = reach & (sp <= 60.0)
        r3 = ppml_two_way_fe(L, -sp, m60)
        res[f"{label}_semi_elasticity_nu_le60"] = r3["beta"]; res[f"{label}_nu_le60_se_hc"] = r3["se_hc"]; res[f"{label}_nu_le60_ncells"] = r3["n_cells"]
    # Model-consistent check: L_obs ~ FE + b * ln R_ij ; the mainline imposes b = 1.
    lnR = np.log(np.clip(R_main, 1e-300, None))
    r4 = ppml_two_way_fe(L_obs, lnR, reach)
    res["observed_on_lnR_slope_b"] = r4["beta"]; res["observed_on_lnR_se_hc"] = r4["se_hc"]
    r5 = ppml_two_way_fe(L_model, lnR, reach)
    res["mainline_model_on_lnR_slope_b"] = r5["beta"]
    # descriptive: observed vs model share by SP band
    bands = [0, 10, 20, 30, 45, 60, 90, 1e9]
    rows = []
    for lo, hi in zip(bands[:-1], bands[1:]):
        mk = reach & (sp > lo) & (sp <= hi)
        rows.append({"sp_min_lo": lo, "sp_min_hi": hi, "obs_share": float(L_obs[mk].sum() / L_obs.sum()), "model_share": float(L_model[mk].sum() / L_model.sum()), "n_cells": int(mk.sum())})
    rows.append({"sp_min_lo": "intrazonal", "sp_min_hi": "", "obs_share": float(np.trace(L_obs) / L_obs.sum()), "model_share": float(np.trace(L_model) / L_model.sum()), "n_cells": N})
    pd.DataFrame(rows).to_csv(out / "od_share_by_sp_band.csv", index=False)
    res["observed_intrazonal_share"] = float(np.trace(L_obs) / L_obs.sum())
    res["mainline_model_intrazonal_share"] = float(np.trace(L_model) / L_model.sum())
    res["observed_mean_sp_min_offdiag"] = float(np.average(sp[reach], weights=L_obs[reach]))
    res["mainline_model_mean_sp_min_offdiag"] = float(np.average(sp[reach], weights=L_model[reach]))
    return res


# --------------------------------------------------------------------------- #
# 2. General-kernel solver
# --------------------------------------------------------------------------- #
@dataclass
class KernelSpec:
    kernel: str          # "power" (a=1/m, rho=theta) | "exp" (a=exp(-rho*kappa*m))
    theta: float
    rho: float
    kappa: float         # per-minute utility cost of travel time (exp kernel only)

    def a(self, m: np.ndarray) -> np.ndarray:
        if self.kernel == "power":
            return 1.0 / np.clip(m, EPS, None)
        return np.exp(-self.rho * self.kappa * np.clip(m, 0.0, None))


def build_A(prim: Primitives, a: np.ndarray) -> np.ndarray:
    A = np.zeros((prim.n_nodes, prim.n_nodes))
    np.add.at(A, (prim.edge_i, prim.edge_j), a)
    return A


def resolvent(A: np.ndarray) -> np.ndarray:
    N = A.shape[0]
    return np.clip(sla.solve(np.eye(N) - A, np.eye(N), check_finite=False), 1e-300, None)


def tau_inv_theta_from_R(R: np.ndarray, ks: KernelSpec) -> np.ndarray:
    return R ** (ks.theta / ks.rho)


def pick_rho_for_rowsum(prim: Primitives, m: np.ndarray, kappa: float, target: float) -> float:
    """Smallest rho with max row-sum of exp(-rho*kappa*m) <= target (bisection)."""
    def maxrow(rho):
        raw = np.zeros(prim.n_nodes); np.add.at(raw, prim.edge_i, np.exp(-rho * kappa * m)); return raw.max()
    lo, hi = 1e-3, 1e4
    for _ in range(200):
        mid = np.sqrt(lo * hi)
        if maxrow(mid) > target: lo = mid
        else: hi = mid
    return hi


def invert(prim: Primitives, params: pm.Parameters, T: np.ndarray, max_iter=1800, tol=1.5e-4, floor_share=1e-6):
    theta, alpha, beta = params.theta, params.alpha, params.beta
    N = prim.n_nodes
    L_R = np.maximum(prim.L_R_obs, floor_share); L_F = np.maximum(prim.L_F_obs, floor_share)
    l_R = L_R / L_R.sum(); l_F = L_F / L_F.sum()
    valid = (prim.L_R_obs > 0) & (prim.L_F_obs > 0)
    v = np.ones(N); w = np.ones(N)
    pR, pF, qR, qF = 1 - beta * theta, 1 - alpha * theta, beta * theta, alpha * theta
    def gnorm(x):
        x = np.clip(x, 1e-30, None); g = np.exp(np.mean(np.log(x[valid]))); return x / g
    for it in range(max_iter):
        a_ = T @ (w * l_F ** qF); v_new = gnorm(l_R ** pR / np.clip(a_, EPS, None))
        b_ = T.T @ (v_new * l_R ** qR); w_new = gnorm(l_F ** pF / np.clip(b_, EPS, None))
        a_chk = T @ (w_new * l_F ** qF); b_chk = T.T @ (v_new * l_R ** qR)
        chiR = np.log(np.clip((l_R ** pR / np.clip(v_new * a_chk, EPS, None))[valid], EPS, None))
        chiF = np.log(np.clip((l_F ** pF / np.clip(w_new * b_chk, EPS, None))[valid], EPS, None))
        d = max(np.std(chiR), np.std(chiF)); v, w = v_new, w_new
        if d < tol: break
    a_ = T @ (w * l_F ** qF); b_ = T.T @ (v * l_R ** qR)
    chi = float(np.exp(0.5 * (np.mean(np.log(np.clip((l_R ** pR / np.clip(v * a_, EPS, None))[valid], EPS, None))) + np.mean(np.log(np.clip((l_F ** pF / np.clip(w * b_, EPS, None))[valid], EPS, None))))))
    return v, w, chi, it + 1, d


@dataclass
class Eq:
    label: str; m: np.ndarray; Xi: np.ndarray; T: np.ndarray; R: np.ndarray; L: np.ndarray
    l_R: np.ndarray; l_F: np.ndarray; Wbar: float; n_outer: int; converged: bool; max_log_change: float
    @property
    def Mbar(self): return float(np.dot(self.Xi, self.m) / self.L.sum())
    @property
    def intrazonal(self): return float(np.trace(self.L) / self.L.sum())


def solve(prim, ks: KernelSpec, params, v, w, chi, *, m_init, n_kl, endogenous, lam=1.1, damping=0.25, max_outer=500, tol=1e-4,
          xi_obs=None, n_obs=None, m_obs=None, m_ff=None, l_R_fixed=None, l_F_fixed=None, label="eq") -> Eq:
    theta = params.theta
    m = m_init.copy(); n_kl = _resolve_lanes(n_kl)
    pin = l_R_fixed is not None
    converged = False; last = np.inf; it = 0
    def inner(m):
        A = build_A(prim, ks.a(m)); R = resolvent(A); T = tau_inv_theta_from_R(R, ks)
        if pin: l_R, l_F = l_R_fixed, l_F_fixed
        else: l_R, l_F, _ = _solve_lR_lF_given_tau(T, v, w, params, chi=chi)
        L_R = l_R * prim.Lbar; L_F = l_F * prim.Lbar
        u = v * np.clip(L_R, EPS, None) ** (params.beta * theta); ww = w * np.clip(L_F, EPS, None) ** (params.alpha * theta)
        K = T * u[:, None] * ww[None, :]; Wtheta = K.sum()
        L = prim.Lbar * K / Wtheta
        if pin:  # gravity re-balancing to the pinned marginals (same as mainline short run)
            L = ipf(L, L_R, L_F)
        G = L / np.clip(R, 1e-300, None)
        Xi = ks.a(m) * (R.T @ G @ R.T)[prim.edge_i, prim.edge_j]
        return R, T, l_R, l_F, L, Xi, Wtheta ** (1 / theta)
    for it in range(1, max_outer + 1):
        R, T, l_R, l_F, L, Xi, Wbar = inner(m)
        if not endogenous: converged = True; last = 0.0; break
        dev = np.clip(Xi / np.clip(xi_obs, EPS, None), EPS, None) * np.clip(n_obs / n_kl, EPS, None)
        m_new = np.maximum(m_ff, m_obs * dev ** lam)
        lm = np.exp(damping * np.log(m_new) + (1 - damping) * np.log(m)); last = float(np.max(np.abs(np.log(lm) - np.log(m)))); m = lm
        if last < tol: converged = True; break
    R, T, l_R, l_F, L, Xi, Wbar = inner(m)
    return Eq(label, m, Xi, T, R, L, l_R, l_F, float(Wbar), it, converged, last)


def ipf(L, rowt, colt, iters=200):
    L = L.copy()
    for _ in range(iters):
        L *= (rowt / np.clip(L.sum(1), EPS, None))[:, None]
        L *= (colt / np.clip(L.sum(0), EPS, None))[None, :]
    return L


def run_kernel(prim, params, ks: KernelSpec, out: Path, top_k=100) -> dict:
    out.mkdir(parents=True, exist_ok=True)
    m_obs = prim.tau_obs_min.copy(); m_ff = prim.tau_ff_min.copy(); n0 = _resolve_lanes(prim.n_kl)
    A0 = build_A(prim, ks.a(m_obs)); rho_A = float(np.max(np.abs(np.linalg.eigvals(A0)))); rowsum = float(A0.sum(1).max())
    R0 = resolvent(A0); T0 = tau_inv_theta_from_R(R0, ks)
    v, w, chi, n_it, resid = invert(prim, params, T0)
    anchor = solve(prim, ks, params, v, w, chi, m_init=m_obs, n_kl=n0, endogenous=False, label="anchor")
    kw = dict(xi_obs=anchor.Xi, n_obs=n0, m_obs=m_obs, m_ff=m_ff)
    base = solve(prim, ks, params, v, w, chi, m_init=m_obs, n_kl=n0, endogenous=True, label="baseline", damping=0.10, **kw)
    sel = select_tidal_lane_pairs_by_rank(prim, top_k=top_k, delta=1.0, rank="score_bpr_st", xi_kl=base.Xi)
    n_cf = prim.n_kl.copy()
    for s, f in sel.treated_pairs: n_cf[s] += 1.0; n_cf[f] -= 1.0
    st = solve(prim, ks, params, v, w, chi, m_init=m_obs, n_kl=n_cf, endogenous=True, damping=0.10, l_R_fixed=base.l_R, l_F_fixed=base.l_F, label="tidal_ST", **kw)
    lt = solve(prim, ks, params, v, w, chi, m_init=m_obs, n_kl=n_cf, endogenous=True, damping=0.10, label="tidal_LT", **kw)
    treated = np.unique(sel.treated_pairs.ravel())
    def pct(a, b): return 100.0 * (a / b - 1.0)
    res = {
        "kernel": ks.kernel, "theta": ks.theta, "rho": ks.rho, "kappa_per_min": ks.kappa, "rho_kappa": ks.rho * ks.kappa,
        "spectral_radius_A_obs": rho_A, "max_rowsum_A_obs": rowsum,
        "inversion_iters": n_it, "inversion_resid": resid, "chi": chi,
        "baseline_converged": base.converged, "baseline_n_outer": base.n_outer, "baseline_median_m_over_obs": float(np.median(base.m / m_obs)),
        "baseline_Wbar": base.Wbar, "baseline_Mbar_min": base.Mbar, "baseline_intrazonal_share": base.intrazonal,
        "baseline_intrazonal_share_note": "Frechet flows L_ij = Lbar*K_ij/sum K",
        "ST_converged": st.converged, "LT_converged": lt.converged,
        "Wbar_pct_ST": pct(st.Wbar, base.Wbar), "Wbar_pct_LT": pct(lt.Wbar, base.Wbar),
        "Mbar_ST_min": st.Mbar, "Mbar_LT_min": lt.Mbar,
        "time_saved_ST_sec_per_commuter": 60 * (base.Mbar - st.Mbar), "time_saved_LT_sec_per_commuter": 60 * (base.Mbar - lt.Mbar),
        "time_saved_ST_person_hours": (base.Mbar - st.Mbar) * prim.Lbar / 60, "time_saved_LT_person_hours": (base.Mbar - lt.Mbar) * prim.Lbar / 60,
        "treated_slow_median_pct_time_change_ST": float(np.median(100 * (st.m[sel.treated_pairs[:, 0]] / base.m[sel.treated_pairs[:, 0]] - 1))),
        "treated_fast_median_pct_time_change_ST": float(np.median(100 * (st.m[sel.treated_pairs[:, 1]] / base.m[sel.treated_pairs[:, 1]] - 1))),
        "res_dissim_LT_pct": 100 * 0.5 * float(np.abs(lt.l_R - base.l_R).sum()), "wp_dissim_LT_pct": 100 * 0.5 * float(np.abs(lt.l_F - base.l_F).sum()),
        "n_treated_pairs": int(sel.n_treated),
    }
    sel.to_frame(prim).to_csv(out / "selected_pairs.csv", index=False)
    np.savez_compressed(out / "baseline_arrays.npz", m=base.m, Xi=base.Xi, l_R=base.l_R, l_F=base.l_F, R=base.R, L=base.L)
    (out / "summary.json").write_text(json.dumps(res, indent=2, default=float))
    return res, base, sel


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--version-id", default="model_input_edgeproj_v2_lcc_gate5_v1")
    p.add_argument("--output-name", default="route_kernel_exp_trial_20260928_v1")
    p.add_argument("--rowsum-target", type=float, default=0.8)
    p.add_argument("--nu-override", type=float, default=None, help="use this theta*kappa instead of the PPML estimate")
    p.add_argument("--skip-exp", action="store_true")
    args = p.parse_args(argv)
    out = DATA_WORK / "outputs" / args.output_name
    if out.exists() and any(out.iterdir()):
        raise SystemExit(f"Refusing to overwrite {out}")
    out.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s | %(message)s", handlers=[logging.FileHandler(out / "run.log"), logging.StreamHandler()])

    prim = bp.load_primitives(DATA_WORK / "outputs" / args.version_id, period="AM")
    params = replace(pm.Parameters(), alpha=0.05, beta=-0.10)
    theta = params.theta

    # ---- power kernel = mainline reproduction --------------------------------
    LOG.info("Power kernel (mainline reproduction)")
    ks_pow = KernelSpec("power", theta, theta, np.nan)
    res_pow, base_pow, _ = run_kernel(prim, params, ks_pow, out / "kernel_power")
    LOG.info("power: Wbar=%.4f intrazonal=%.3f Mbar=%.3f ST=%.4f%% LT=%.4f%%", res_pow["baseline_Wbar"], res_pow["baseline_intrazonal_share"], res_pow["baseline_Mbar_min"], res_pow["Wbar_pct_ST"], res_pow["Wbar_pct_LT"])

    # ---- gravity / PPML -------------------------------------------------------
    LOG.info("Gravity PPML")
    sp = shortest_path_minutes(prim, prim.tau_obs_min)
    grav = gravity_block(prim, sp, prim.L_ij_obs, base_pow.L, base_pow.R, out)
    (out / "gravity_ppml.json").write_text(json.dumps(grav, indent=2, default=float))
    LOG.info("nu_obs=%.4f/min (le60: %.4f) | nu_model=%.4f | loglog b_obs=%.3f b_model=%.3f | slope on lnR: obs %.3f model %.3f", grav["observed_semi_elasticity_nu_per_min"], grav["observed_semi_elasticity_nu_le60"], grav["mainline_model_semi_elasticity_nu_per_min"], grav["observed_loglog_elasticity_b"], grav["mainline_model_loglog_elasticity_b"], grav["observed_on_lnR_slope_b"], grav["mainline_model_on_lnR_slope_b"])

    rows = [res_pow]
    if not args.skip_exp:
        nu = args.nu_override if args.nu_override is not None else grav["observed_semi_elasticity_nu_per_min"]
        kappa = nu / theta
        rho = pick_rho_for_rowsum(prim, prim.tau_obs_min, kappa, args.rowsum_target)
        for tag, rho_used in (("exp_rowsum_target", rho), ("exp_rho2x", 2 * rho)):
            ks = KernelSpec("exp", theta, rho_used, kappa)
            LOG.info("Exp kernel %s: nu=%.4f kappa=%.5f rho=%.2f rho*kappa=%.4f", tag, nu, kappa, rho_used, rho_used * kappa)
            try:
                res, base_e, _ = run_kernel(prim, params, ks, out / f"kernel_{tag}")
                g2 = ppml_two_way_fe(base_e.L, -sp, np.isfinite(sp) & (sp > 0) & ~np.eye(prim.n_nodes, dtype=bool))
                res["model_implied_nu_per_min"] = g2["beta"]; res["tag"] = tag
                LOG.info("%s: Wbar=%.4f intrazonal=%.3f Mbar=%.3f ST=%.4f%% LT=%.4f%% implied nu=%.4f", tag, res["baseline_Wbar"], res["baseline_intrazonal_share"], res["baseline_Mbar_min"], res["Wbar_pct_ST"], res["Wbar_pct_LT"], res["model_implied_nu_per_min"])
                rows.append(res)
            except Exception as ex:  # keep going, record failure
                LOG.exception("exp kernel %s failed: %s", tag, ex)
                rows.append({"kernel": "exp", "tag": tag, "rho": rho_used, "kappa_per_min": kappa, "error": str(ex)})
    pd.DataFrame(rows).to_csv(out / "kernel_comparison.csv", index=False)
    LOG.info("Done -> %s", out)


if __name__ == "__main__":
    main()
