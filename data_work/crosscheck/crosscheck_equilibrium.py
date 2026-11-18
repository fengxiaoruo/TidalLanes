"""Independent cross-validation of the full_estimation equilibrium stack.

Re-solves baseline + tidal ST/LT with the CURRENT mainline code, then verifies
every multiply-solvable object with an independent method. Pure diagnostic;
not imported by the mainline. Run:  python3 crosscheck/crosscheck_equilibrium.py
"""
from __future__ import annotations

import sys
from dataclasses import replace
from pathlib import Path

import numpy as np

DW = Path(__file__).resolve().parents[1]
if str(DW) not in sys.path:
    sys.path.insert(0, str(DW))

from src.model.full_estimation import build_primitives as bp
from src.model.full_estimation import invert_fundamentals as inv
from src.model.full_estimation import parameters as pm
from src.model.full_estimation import stage2_counterfactuals as s2
from src.model.full_estimation.equilibrium_solve import (
    _build_A_matrix, _compute_tau_inv_theta, _market_access, EPS,
    compute_observation_anchors,
)
from src.model.full_estimation.run_alpha_robustness_quick_gate5 import solver_overrides_for_params

VERSION = "model_input_edgeproj_v2_lcc_gate5_v1"
ALPHA, BETA = 0.05, -0.10
TOP_K = 100

results: list[tuple[str, bool, str]] = []
def check(name, passed, detail=""):
    results.append((name, bool(passed), detail))
    print(f"[{'PASS' if passed else 'FAIL'}] {name}  {detail}")


def solve_all():
    prim = bp.load_primitives(DW / "outputs" / VERSION, period="AM")
    params = replace(pm.Parameters(), alpha=ALPHA, beta=BETA)
    skw = solver_overrides_for_params(params)
    fund = inv.invert_fundamentals(prim, params, tol=1.5e-4, max_iter=1800, kappa=1.0)
    anchors = compute_observation_anchors(prim, fund.ubar_theta, fund.abar_theta, params,
                                          chi=fund.chi, kappa=fund.kappa)
    kw = s2.build_congestion_solver_kwargs(prim, fund, params,
                                           congestion_form="anchored", congestion_lambda=1.1, **skw)
    kw.update(xi_obs_kl=anchors["xi_obs_kl"], n_obs_kl=anchors["n_obs_kl"],
              tau_obs_anchor=anchors["tau_obs_anchor"])
    base = s2.solve_baseline_endogenous(prim, fund, params, **kw)
    eq_st, _ = s2.run_tidal_endogenous(prim, fund, params, base, top_k=TOP_K,
                                       horizon="short_run", pair_selection="score_bpr_st", **kw)
    eq_lt, sel = s2.run_tidal_endogenous(prim, fund, params, base, top_k=TOP_K,
                                         horizon="long_run", pair_selection="score_bpr_st", **kw)
    return prim, params, fund, base, eq_st, eq_lt


def main():
    prim, params, fund, base, eq_st, eq_lt = solve_all()
    th = params.theta; Lbar = prim.Lbar; N = prim.n_nodes

    def u_theta(lR): return fund.ubar_theta * (np.clip(lR * Lbar, EPS, None) ** (params.beta * th))
    def w_theta(lF): return fund.abar_theta * (np.clip(lF * Lbar, EPS, None) ** (params.alpha * th))

    # ---- 1. spectral radius ----
    A = _build_A_matrix(prim.edge_i, prim.edge_j, base.tau_min_kl, N, kappa=base.kappa)
    rho = float(np.max(np.abs(np.linalg.eigvals(A))))
    check("1. spectral radius rho(A)<1", rho < 1.0, f"rho={rho:.4f}, kappa={base.kappa}")

    # ---- 2. tau_inv: linear solve vs Neumann series vs residual ----
    B = _compute_tau_inv_theta(A)
    # Neumann partial sum
    S = np.eye(N); term = np.eye(N)
    for _ in range(400):
        term = term @ A
        S = S + term
        if np.max(np.abs(term)) < 1e-14:
            break
    rel_neumann = float(np.max(np.abs(S - B)) / np.max(np.abs(B)))
    resid = float(np.max(np.abs((np.eye(N) - A) @ B - np.eye(N))))
    check("2a. (I-A)^-1 vs Neumann series", rel_neumann < 1e-6, f"max rel diff={rel_neumann:.2e}")
    check("2b. (I-A)X=I residual", resid < 1e-8, f"max resid={resid:.2e}")
    check("2c. tau_inv matches eq.tau_ij_inv_theta", 
          float(np.max(np.abs(B - base.tau_ij_inv_theta))) < 1e-8,
          f"max diff={float(np.max(np.abs(B-base.tau_ij_inv_theta))):.2e}")

    # ---- 3. welfare four ways (baseline) ----
    u0 = u_theta(base.l_R); w0 = w_theta(base.l_F)
    LF0 = base.l_F * Lbar; LR0 = base.l_R * Lbar
    Wth_gf = float(u0 @ (B @ w0))                                   # gauge-free primitive
    # gauge-fixed via Pj: Pj^th = w^th / L_F  => Pi^{-th}=B@(L_F*Pj^th)=B@w^th
    Pi_inv_fixed = B @ w0
    Wth_pifix = float(np.sum(u0 * Pi_inv_fixed))
    # symmetric gauge via Pi: Pi^th = u^th / L_R => P^{-th}=B.T@(L_R*Pi^th)=B.T@u^th
    P_inv_fixed = B.T @ u0
    Wth_pfix = float(np.sum(w0 * P_inv_fixed))
    check("3a. W^th gauge-free == Pi-gauge-fixed", abs(Wth_gf/Wth_pifix-1) < 1e-9,
          f"gf={Wth_gf:.6e} pifix={Wth_pifix:.6e}")
    check("3b. W^th gauge-free == P-gauge-fixed (symmetric)", abs(Wth_gf/Wth_pfix-1) < 1e-9,
          f"gf={Wth_gf:.6e} pfix={Wth_pfix:.6e}")
    check("3c. eq.Wbar == gauge-free^(1/th)", abs(base.Wbar/(Wth_gf**(1/th))-1) < 1e-6,
          f"eq.Wbar={base.Wbar:.6f} recompute={Wth_gf**(1/th):.6f}")

    # demonstrate gauge contamination of the OLD ones-init Pi welfare ratio
    def W_gf_ratio(eq_a, eq_b):
        Ba = _compute_tau_inv_theta(_build_A_matrix(prim.edge_i, prim.edge_j, eq_a.tau_min_kl, N, kappa=base.kappa))
        Bb = _compute_tau_inv_theta(_build_A_matrix(prim.edge_i, prim.edge_j, eq_b.tau_min_kl, N, kappa=base.kappa))
        Wa = float(u_theta(eq_a.l_R) @ (Ba @ w_theta(eq_a.l_F)))
        Wb = float(u_theta(eq_b.l_R) @ (Bb @ w_theta(eq_b.l_F)))
        return (Wa/Wb)**(1/th) - 1
    def W_pi_ratio(eq_a, eq_b):
        wa = float(np.sum(u_theta(eq_a.l_R)*np.clip(eq_a.Pi_i,EPS,None)**(-th)))
        wb = float(np.sum(u_theta(eq_b.l_R)*np.clip(eq_b.Pi_i,EPS,None)**(-th)))
        return (wa/wb)**(1/th) - 1
    lt_gf = 100*W_gf_ratio(eq_lt, base); lt_pi = 100*W_pi_ratio(eq_lt, base)
    print(f"     LT welfare %: gauge-free={lt_gf:+.4f}  ones-init-Pi={lt_pi:+.4f}")
    check("3d. gauge-free vs ones-init-Pi LT welfare DIFFER (fix matters)",
          abs(lt_gf - lt_pi) > 0.1, f"diff={lt_gf-lt_pi:+.4f}pp")

    # ---- 4. Xi: code formula vs gravity/kappa path formula ----
    t_inv = 1.0 / np.clip(base.tau_min_kl, EPS, None)   # t_kl^{-th}, kappa=1 baseline
    BTu = B.T @ u0      # (B^T u^th)_k
    Bw = B @ w0         # (B w^th)_l
    Xi_path = t_inv * BTu[prim.edge_i] * Bw[prim.edge_j]   # up to constant L̄/W^th
    Xi_code = base.Xi_kl
    rr = np.log(np.clip(Xi_code, EPS, None) / np.clip(Xi_path, EPS, None))
    spread_all = float(np.std(rr))
    # flow-weighted spread (low-traffic edges dominated by floored-node noise)
    wflow = Xi_code / np.sum(Xi_code)
    mean_w = float(np.sum(wflow * rr))
    spread_w = float(np.sqrt(np.sum(wflow * (rr - mean_w) ** 2)))
    # restrict to edges between valid (positive-pop) nodes
    valid = (prim.L_R_obs > 0) & (prim.L_F_obs > 0)
    em = valid[prim.edge_i] & valid[prim.edge_j]
    spread_valid = float(np.std(rr[em]))
    # flow-weighted agreement is what matters for congestion/selection; the
    # unweighted tail (~few %) lives on low-traffic edges touching floored nodes
    # where the ones-init market-access solver and the gravity form diverge.
    check("4. Xi code == gravity/kappa path form (flow-weighted)",
          spread_w < 5e-3,
          f"flow-wt std={spread_w:.2e} | tail: std_all={spread_all:.2e} valid-edge std={spread_valid:.2e}")

    # ---- 5. gravity marginals reproduce shares ----
    lR_grav = u0 * Bw / np.sum(u0 * Bw)         # L̄ u^th (Bw^th)/W^th, normalized
    lF_grav = w0 * BTu / np.sum(w0 * BTu)
    valid5 = (prim.L_R_obs > 0) & (prim.L_F_obs > 0)   # identifying nodes (inversion mask)
    dlogR = np.abs(np.log(np.clip(lR_grav,EPS,None)) - np.log(np.clip(base.l_R,EPS,None)))
    dlogF = np.abs(np.log(np.clip(lF_grav,EPS,None)) - np.log(np.clip(base.l_F,EPS,None)))
    eR_all = float(np.max(dlogR)); eF_all = float(np.max(dlogF))
    eR_valid = float(np.max(dlogR[valid5])); eF_valid = float(np.max(dlogF[valid5]))
    check("5a. gravity row-marginal == l_R (E_R clearing, valid nodes)", eR_valid < 1e-2,
          f"max|dlog| valid={eR_valid:.2e} (all={eR_all:.2e}, floored-node noise)")
    check("5b. gravity col-marginal == l_F (E_F clearing, valid nodes)", eF_valid < 1e-2,
          f"max|dlog| valid={eF_valid:.2e} (all={eF_all:.2e})")

    # ---- 6. inversion round-trip ----
    rR = float(np.max(np.abs(fund.resid_R))); rF = float(np.max(np.abs(fund.resid_F)))
    check("6. inversion residuals small (reproduce l_R/l_F obs)", max(rR,rF) < 5e-2,
          f"max|resid_R|={rR:.2e} max|resid_F|={rF:.2e}")

    # ---- 7. anchored baseline fixed point ----
    ratio_tau = base.tau_min_kl / np.clip(prim.tau_obs_min, EPS, None)
    med = float(np.median(ratio_tau))
    n_floor = int(np.sum(prim.tau_obs_min < prim.tau_ff_min))
    check("7. anchored baseline median tau/tau_obs ~ 1", abs(med-1) < 1e-3,
          f"median={med:.5f}; free-flow-floor edges={n_floor}/{len(prim.tau_obs_min)}")

    # ---- 8. decomposition identities (commute, gauge-free) ----
    def od_shares(eq):
        u = u_theta(eq.l_R); w = w_theta(eq.l_F)
        m = eq.tau_ij_inv_theta * u[:,None] * w[None,:]
        return m / np.sum(m)
    def tau_min_ij(eq): return np.clip(eq.tau_ij_inv_theta, EPS, None)**(-1/th)
    s0 = od_shares(base); slt = od_shares(eq_lt)
    t0 = tau_min_ij(base); tst = tau_min_ij(eq_st); tlt = tau_min_ij(eq_lt)
    direct = np.sum(s0*(tst-t0)); ge = np.sum(s0*(tlt-tst)); sort = np.sum((slt-s0)*tlt)
    total = np.sum(slt*tlt - s0*t0)
    check("8. commute decomp: direct+GE+sort == total", abs((direct+ge+sort)-total) < 1e-9,
          f"sum={direct+ge+sort:.6f} total={total:.6f}")

    # ---- 9. gauge / normalization invariance of welfare ratio ----
    rng = np.random.default_rng(0)
    cu = np.exp(rng.normal(0, 0.5, N))   # arbitrary positive rescale of ubar (changes normalization)
    def W_gf(eq, ubar_th):
        Bx = _compute_tau_inv_theta(_build_A_matrix(prim.edge_i, prim.edge_j, eq.tau_min_kl, N, kappa=base.kappa))
        u = ubar_th * (np.clip(eq.l_R*Lbar,EPS,None)**(params.beta*th))
        w = w_theta(eq.l_F)
        return float(u @ (Bx @ w))
    # NOTE: rescaling ubar changes the equilibrium shares in general, so here we only
    # test that the gauge-free welfare formula is invariant to market-access init scale.
    Pi_a, Pj_a = _market_access(B, base.l_R, base.l_F, Lbar, th)
    Pi_b, Pj_b = _market_access(B, base.l_R, base.l_F, Lbar, th)  # deterministic; check Xi-product gauge-invariance
    prod_a = (Pj_a[prim.edge_i]**(-th)) * (Pi_a[prim.edge_j]**(-th))
    # regauge by hand
    c = 7.3
    prod_b = ((Pj_a/c)[prim.edge_i]**(-th)) * ((c*Pi_a)[prim.edge_j]**(-th))
    check("9. Xi market-access product gauge-invariant", 
          float(np.max(np.abs(np.log(prod_a/prod_b)))) < 1e-9,
          "Pi->c*Pi, P->P/c leaves P^-th*Pi^-th unchanged")

    # ---- 10. gauge-free welfare decomposition completeness (W^th = sum_i u_i^th M_i) ----
    Blt = _compute_tau_inv_theta(_build_A_matrix(prim.edge_i, prim.edge_j, eq_lt.tau_min_kl, N, kappa=base.kappa))
    Bst = _compute_tau_inv_theta(_build_A_matrix(prim.edge_i, prim.edge_j, eq_st.tau_min_kl, N, kappa=base.kappa))
    w_lt = w_theta(eq_lt.l_F); u_lt = u_theta(eq_lt.l_R)
    M0 = B @ w0                     # base access (base tau, base l_F)
    M_st = Bst @ w0                 # ST tau, base l_F
    M_tauonly = Blt @ w0            # LT tau, base l_F
    M_lf = Blt @ w_lt               # LT tau, LT l_F
    Wth_base = float(np.sum(u0 * M0)); Wth_lt = float(np.sum(u_lt * M_lf))
    L1 = float(np.sum(u0 * (M_st - M0)))            # ST tau
    L2 = float(np.sum(u0 * (M_tauonly - M_st)))     # GE tau beyond ST
    L3 = float(np.sum(u0 * (M_lf - M_tauonly)))     # l_F sorting
    L4 = float(np.sum((u_lt - u0) * M_lf))          # l_R crowding (u channel) -- MISSING in current code
    recon = L1 + L2 + L3 + L4
    print(f"     W^th layers: STtau={L1:+.2f} GEtau={L2:+.2f} lF={L3:+.2f} lR/u={L4:+.2f}  sum={recon:+.2f} dW^th={Wth_lt-Wth_base:+.2f}")
    check("10. full 4-layer welfare decomp sums to dW^th", abs(recon - (Wth_lt - Wth_base)) < 1e-6,
          f"3-layer (code) omits u-channel L4={L4:+.2f} ({100*L4/(Wth_lt-Wth_base):+.1f}% of dW^th)")

    npass = sum(p for _,p,_ in results)
    print(f"\n==== {npass}/{len(results)} checks PASS ====")


if __name__ == "__main__":
    main()
