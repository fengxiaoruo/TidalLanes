"""Theta sensitivity + first-order vs full-solve decomposition of tidal-lane gains (OSM 1B).

Mainline (2026-11-11): bundle model_input_osm1b_L300_gate5_od2025_v2_alldaytt0709,
ce_floor congestion (physical-time elasticity 0.25, anchor flow floor 1% of median),
kappa by the spectral rule (rho <= 0.95), alpha=0.05, beta=-0.10, top100 tidal pairs
held FIXED at the mainline selection (not re-selected per theta).

Per spec (theta, route_eta, pair list):
  1. re-invert fundamentals at theta (baseline reproduces observed tau and marginals);
  2. baseline, tidal short run (shares pinned) and long run (re-sorting);
  3. existing physical-time decomposition (tidal_lane_diagnostics.od_cost_decomposition);
  4. first-order welfare test at the baseline equilibrium:

     fixed-flow cost change   m1_e = C(Xi0_e, n1_e),  dln m_e = ln m1_e - ln C(Xi0_e, n0_e)
                              (C = ce_floor congestion map, baseline flows Xi0, new lanes n1)
     edge welfare share       s_e = dln W^theta / dln A_e at baseline
                                  = eta * A_e [R' G R']_e / W^theta,  G = R^(eta-1) o (u w'),
                              W^theta = sum_ij R_ij^eta u_i w_j, A_e = 1/(kappa m_e)
                              (eta = 1: s_e = expected passages of e per commuter)
     first-order prediction   dWbar/Wbar ~= -(1/theta) * sum_e s_e dln m_e
                              (fixed flows, fixed routes, fixed locations)
     fixed-flow exact (PE)    Wbar at A(m1) with baseline u, w (routes re-chosen,
                              no congestion feedback, no sorting)

Nothing in existing modules is modified; outputs go to a new directory.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
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
from src.model.full_estimation.counterfactual_levels import (  # noqa: E402
    TidalLaneSelection,
    select_tidal_lane_pairs_by_rank,
)
from src.model.full_estimation.equilibrium_solve import (  # noqa: E402
    MAINLINE_ALPHA,
    MAINLINE_BETA,
    _build_A_matrix,
    _congestion_tau_update,
    _resolve_lanes,
    _resolvent_and_tau,
    compute_observation_anchors,
    resolve_kappa,
    solve_equilibrium,
)
from src.model.full_estimation.run_alpha_robustness_quick_gate5 import (  # noqa: E402
    solver_overrides_for_params,
)
from src.model.full_estimation.tidal_lane_diagnostics import (  # noqa: E402
    equilibrium_od_shares,
    expected_additive_route_minutes,
    summarise_tidal_pair,
)

LOG = logging.getLogger("theta_fo_so")
EPS = 1e-12


def _params(theta: float, eta: float) -> pm.Parameters:
    return replace(
        pm.Parameters(),
        theta=float(theta),
        alpha=float(MAINLINE_ALPHA),
        beta=float(MAINLINE_BETA),
        route_eta=float(eta),
        theta_source=f"theta grid (mainline AA {pm.AA_SEATTLE_THETA})",
        alpha_source="Literature mid-range gross production spillover (mainline pos05)",
        beta_source="AA(2022) Seattle beta held",
        route_eta_source=("AA(2022): route and location dispersion equal (eta=1)" if eta == 1.0
                          else "calibrated to observed OD gravity semi-elasticity (2026-11-11)"),
    )


def _load_selection(path: Path) -> TidalLaneSelection:
    df = pd.read_csv(path)
    pairs = df[["slow_edge_idx", "fast_edge_idx"]].to_numpy(dtype=int)
    return TidalLaneSelection(treated_pairs=pairs, asym_ratio=df["asym_ratio"].to_numpy(),
                              n_treated=len(pairs), n_skipped=0)


def _u_w_theta(eq, fund, params, Lbar):
    u = fund.ubar_theta * np.clip(eq.l_R * Lbar, EPS, None) ** (params.beta * params.theta)
    w = fund.abar_theta * np.clip(eq.l_F * Lbar, EPS, None) ** (params.alpha * params.theta)
    return u, w


def _w_theta_at(prim, m, kappa, eta, u, w) -> float:
    A = _build_A_matrix(prim.edge_i, prim.edge_j, m, prim.n_nodes, kappa=kappa)
    _, tinv = _resolvent_and_tau(A, eta)
    return float(np.sum(tinv * u[:, None] * w[None, :]))


def first_order_block(prim, params, fund, base, kw, n_cf, eqs) -> dict:
    theta, eta, kappa = params.theta, float(params.route_eta), float(base.kappa)
    cmap = dict(form=kw["congestion_form"], lam=kw["congestion_lambda"], tau_ff=prim.tau_ff_min,
                tau_obs=kw["tau_obs_anchor"], ell_km=np.ones_like(prim.tau_ff_min),
                xi_obs_kl=kw["xi_obs_kl"], n_obs_kl=kw["n_obs_kl"], xi_floor=kw.get("xi_floor", 0.0))
    m0 = base.m_min_kl
    m0_map = _congestion_tau_update(Xi_kl=base.Xi_kl, n_kl=_resolve_lanes(prim.n_kl), **cmap)
    m1 = _congestion_tau_update(Xi_kl=base.Xi_kl, n_kl=_resolve_lanes(n_cf), **cmap)
    dlnm = np.log(m1) - np.log(m0_map)

    u, w = _u_w_theta(base, fund, params, prim.Lbar)
    A = _build_A_matrix(prim.edge_i, prim.edge_j, m0, prim.n_nodes, kappa=kappa)
    R, tinv = _resolvent_and_tau(A, eta)
    W0 = float(np.sum(tinv * u[:, None] * w[None, :]))
    G = (tinv / R) * u[:, None] * w[None, :]          # R^(eta-1) u w'
    Y = R.T @ G @ R.T
    s = eta * A[prim.edge_i, prim.edge_j] * Y[prim.edge_i, prim.edge_j] / W0
    # NB: parallel directed edges share A_kl; the share above is per (k,l) cell, so
    # split it by each edge's own 1/m weight.
    a_e = 1.0 / np.clip(kappa * m0, EPS, None)
    cell = A[prim.edge_i, prim.edge_j]
    s = s * a_e / np.clip(cell, EPS, None)

    fo_log = -(1.0 / theta) * float(np.sum(s * dlnm))
    W1 = _w_theta_at(prim, m1, kappa, eta, u, w)
    passages = base.Xi_kl / (kappa * prim.Lbar)
    # DFM-style congestion-externality term, evaluated at baseline shares:
    #   -sum_e s_e * dln t_e^{cong},  dln t = dln m / theta,
    #   formula version: dln m^{cong} = lam * dln Xi on active edges (above floor, flow > xi_floor)
    lam = float(kw["congestion_lambda"])
    xf = float(kw.get("xi_floor", 0.0))
    floor = np.minimum(prim.tau_ff_min, kw["tau_obs_anchor"])
    ext = {}
    for hz, eq in eqs.items():
        dlnxi = np.log(np.clip(np.maximum(eq.Xi_kl, xf), EPS, None)) - \
            np.log(np.clip(np.maximum(base.Xi_kl, xf), EPS, None))
        active = (eq.m_min_kl > floor * (1 + 1e-9)) & (m1 > floor * (1 + 1e-9))
        ext[f"ext_formula_{hz}_pct"] = -100.0 * (lam / theta) * float(np.sum(s * dlnxi * active))
        ext[f"ext_actual_{hz}_pct"] = -100.0 / theta * float(
            np.sum(s * (np.log(eq.m_min_kl) - np.log(m1))))
        ext[f"fo_plus_ext_actual_{hz}_pct"] = 100.0 * fo_log + ext[f"ext_actual_{hz}_pct"]
    return {
        **ext,
        "fixed_flow_max_abs_dlnm_untreated": float(np.max(np.abs(np.delete(dlnm, np.flatnonzero(n_cf != prim.n_kl))))) if np.any(n_cf == prim.n_kl) else 0.0,
        "baseline_map_fixed_point_max_abs_log": float(np.max(np.abs(np.log(m0_map) - np.log(m0)))),
        "sum_edge_welfare_share": float(s.sum()),
        "sum_passages_per_commuter": float(passages.sum()),
        "W_theta_check_rel": float(W0 / base.Wbar ** theta - 1.0),
        "fo_Wbar_pct": 100.0 * fo_log,
        "fo_Wbar_pct_exp": 100.0 * (np.exp(fo_log) - 1.0),
        "pe_fixed_flow_Wbar_pct": 100.0 * ((W1 / W0) ** (1.0 / theta) - 1.0),
        "fo_time_saved_sec": -60.0 * float(np.sum(passages * (m1 - m0_map))),
        "fo_sum_s_dlnm": float(np.sum(s * dlnm)),
    }


def run_spec(prim, out_root: Path, *, tag: str, theta: float, eta: float, pairs_csv: Path,
             mainline_pairs: set, congestion_lambda: float, xi_floor_rel: float,
             kappa_rule: str) -> dict:
    t0 = time.time()
    params = _params(theta, eta).with_congestion("ce_floor", congestion_lambda)
    d = out_root / tag
    d.mkdir(parents=True, exist_ok=True)
    params.write(d / "parameters.json")
    kappa, kinfo = resolve_kappa(prim, kappa_rule)
    (d / "kappa.json").write_text(json.dumps(kinfo, indent=2))
    LOG.info("[%s] theta=%.3f eta=%.3f kappa=%.4f — invert", tag, theta, eta, kappa)
    fund = inv.invert_fundamentals(prim, params, tol=1.5e-4, max_iter=1800, kappa=kappa)
    fund.write(d / "fundamentals")

    anchors = compute_observation_anchors(prim, fund.ubar_theta, fund.abar_theta, params,
                                          chi=fund.chi, kappa=fund.kappa)
    xi_floor = float(xi_floor_rel) * float(np.median(anchors["xi_obs_kl"]))
    sk = {k: v for k, v in solver_overrides_for_params(params).items() if v is not None}
    kw = dict(kappa=fund.kappa, chi=fund.chi, max_outer=max(500, sk.get("max_outer", 500)),
              tol=sk.get("tol", 1e-5), damping=sk.get("damping", 0.5),
              congestion_form="ce_floor", congestion_lambda=float(congestion_lambda),
              xi_obs_kl=anchors["xi_obs_kl"], n_obs_kl=anchors["n_obs_kl"],
              tau_obs_anchor=anchors["tau_obs_anchor"], xi_floor=xi_floor)

    base = s2.solve_baseline_endogenous(prim, fund, params, **kw)
    base.write(d / "baseline")

    sel = _load_selection(pairs_csv)
    own = select_tidal_lane_pairs_by_rank(prim, top_k=100, delta=1.0, rank="score_bpr_st",
                                          xi_kl=base.Xi_kl)
    own_set = {tuple(map(int, p)) for p in own.treated_pairs}
    n_cf = prim.n_kl.copy()
    for slow, fast in sel.treated_pairs:
        n_cf[slow] += 1.0
        n_cf[fast] -= 1.0

    eqs = {}
    for hz in ("short_run", "long_run"):
        pin = dict(l_R_fixed=base.l_R, l_F_fixed=base.l_F) if hz == "short_run" else {}
        eqs[hz] = solve_equilibrium(prim, fund.ubar_theta, fund.abar_theta, params,
                                    n_kl_override=n_cf, tau_min_init=prim.tau_obs_min.copy(),
                                    endogenous_congestion=True, label=f"tidal_top100_{hz}",
                                    **pin, **kw)
        eqs[hz].write(d / "top100_d1.0" / hz)
    sel.to_frame(prim).to_csv(d / "top100_d1.0" / "selected_pairs.csv", index=False)
    st, lt = eqs["short_run"], eqs["long_run"]

    row, _, edges = summarise_tidal_pair(spec_label=tag, top_k=100, delta_lanes=1.0,
                                         baseline=base, short_run=st, long_run=lt, prim=prim,
                                         params=params, abar_theta=fund.abar_theta, sel=sel)
    edges.to_csv(d / "tidal_treated_edge_detail.csv", index=False)

    # observed-OD weighting of expected route minutes
    Lobs = np.asarray(prim.L_ij_obs, dtype=float)
    wobs = Lobs / Lobs.sum()
    Loff = Lobs * (1.0 - np.eye(Lobs.shape[0]))
    wobs_off = Loff / Loff.sum()
    M = {k: expected_additive_route_minutes(e, prim) for k, e in
         (("base", base), ("st", st), ("lt", lt))}
    od0 = equilibrium_od_shares(base, prim, params.theta)
    fo = first_order_block(prim, params, fund, base, kw, n_cf, {"st": st, "lt": lt})

    pst, plt_ = row["Wbar_pct_change_st"], row["Wbar_pct_change_lt"]
    sav_st = row["physical_time_saved_st_sec_per_commuter"]
    out = {
        "tag": tag, "theta": theta, "route_eta": eta, "kappa": float(fund.kappa),
        "pairs_source": str(pairs_csv.parent.parent.name),
        "own_selection_overlap_with_mainline_top100": len(own_set & mainline_pairs),
        "fund_converged": bool(fund.converged), "fund_n_iter": int(fund.n_iter),
        "fund_max_abs_resid_R": float(np.max(np.abs(fund.resid_R))),
        "fund_max_abs_resid_F": float(np.max(np.abs(fund.resid_F))),
        "baseline_converged": bool(base.converged),
        "baseline_max_abs_log_tau_vs_obs": float(np.max(np.abs(np.log(base.m_min_kl / prim.tau_obs_min)))),
        "baseline_max_abs_lR_minus_obs": float(np.max(np.abs(base.l_R - prim.L_R_obs / prim.L_R_obs.sum()))),
        "st_converged": bool(st.converged), "st_n_outer": int(st.n_outer),
        "lt_converged": bool(lt.converged), "lt_n_outer": int(lt.n_outer),
        "Wbar_pct_st": pst, "Wbar_pct_lt": plt_,
        "lt_over_st_welfare": plt_ / pst if pst else np.nan,
        "time_saved_model_od_st_sec": sav_st,
        "time_saved_model_od_lt_sec": row["physical_time_saved_lt_sec_per_commuter"],
        "time_saved_obs_od_st_sec": 60.0 * float(np.sum(wobs * (M["base"] - M["st"]))),
        "time_saved_obs_od_lt_sec": 60.0 * float(np.sum(wobs * (M["base"] - M["lt"]))),
        "time_saved_obs_od_offdiag_st_sec": 60.0 * float(np.sum(wobs_off * (M["base"] - M["st"]))),
        "time_saved_obs_od_offdiag_lt_sec": 60.0 * float(np.sum(wobs_off * (M["base"] - M["lt"]))),
        "base_avg_commute_min_obs_od_offdiag": float(np.sum(wobs_off * M["base"])),
        "Wbar_pct_per_min_saved_st": pst / (sav_st / 60.0) if sav_st else np.nan,
        "base_same_cell_share_model_od": float(np.trace(od0)),
        "base_avg_commute_min_model_od": row["avg_physical_time_base_min"],
        "base_avg_commute_min_obs_od": float(np.sum(wobs * M["base"])),
        "decomp_direct_st_sec": row["direct_st_cost_change_sec"],
        "decomp_ge_wedge_sec": row["ge_cost_wedge_sec"],
        "decomp_sorting_wedge_sec": row["sorting_wedge_sec"],
        "decomp_total_lt_sec": row["total_lt_cost_change_sec"],
        "sorting_welfare_wedge_pct": row["sorting_welfare_wedge_pct"],
        **fo,
    }
    out["fo_over_st"] = out["fo_Wbar_pct"] / pst if pst else np.nan
    out["fo_over_lt"] = out["fo_Wbar_pct"] / plt_ if plt_ else np.nan
    out["pe_over_st"] = out["pe_fixed_flow_Wbar_pct"] / pst if pst else np.nan
    # Tsivanidis-style split of (full ST - first order):
    #   shock size  = PE(fixed flows, exact) - FO      (gap with congestion feedback off)
    #   externality = ST - PE                          (gap caused by congestion feedback)
    #   sorting     = LT - ST
    out["gap_st_minus_fo_pct"] = pst - out["fo_Wbar_pct"]
    out["gap_shock_size_pct"] = out["pe_fixed_flow_Wbar_pct"] - out["fo_Wbar_pct"]
    out["gap_externality_pct"] = pst - out["pe_fixed_flow_Wbar_pct"]
    out["gap_sorting_pct"] = plt_ - pst
    out["fo_time_over_st_time"] = out["fo_time_saved_sec"] / sav_st if sav_st else np.nan
    out["runtime_sec"] = time.time() - t0
    (d / "spec_summary.json").write_text(json.dumps(out, indent=2, default=float))
    LOG.info("[%s] done in %.0fs: W ST/LT %.4f/%.4f  FO %.4f  PE %.4f  conv %s/%s/%s/%s",
             tag, out["runtime_sec"], pst, plt_, out["fo_Wbar_pct"], out["pe_fixed_flow_Wbar_pct"],
             fund.converged, base.converged, st.converged, lt.converged)
    return out


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--version-id", default="model_input_osm1b_L300_gate5_od2025_v2_alldaytt0709")
    p.add_argument("--output-name", default="theta_first_second_order_osm1b_alldaytt0709_v1")
    p.add_argument("--mainline-dir",
                   default="tidal_lane_diagnostics_osm1b_ce_floor_b025_fl001_ks_od2025_alldaytt0709_v3")
    p.add_argument("--eta-dir",
                   default="tidal_lane_diagnostics_osm1b_ce_floor_b025_fl001_ks_od2025_alldaytt0709_eta0p21_v1")
    p.add_argument("--theta-grid", default="6.83,4,10,3.4,5,8")
    p.add_argument("--eta-specs", default="0.21:own,0.21:mainline",
                   help="eta:pairs at theta=6.83; pairs=own (eta run's selection) or mainline")
    p.add_argument("--congestion-lambda", type=float, default=0.25)
    p.add_argument("--xi-floor-rel", type=float, default=0.01)
    p.add_argument("--kappa-rule", default="spectral")
    args = p.parse_args(argv)

    out = DATA_WORK / "outputs" / args.output_name
    out.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s | %(message)s",
                        handlers=[logging.FileHandler(out / "run.log"), logging.StreamHandler()])
    LOG.info("argv: %s", " ".join(sys.argv))

    prim = bp.load_primitives(DATA_WORK / "outputs" / args.version_id, period="AM")
    main_csv = DATA_WORK / "outputs" / args.mainline_dir / "top100_d1.0" / "selected_pairs.csv"
    eta_csv = DATA_WORK / "outputs" / args.eta_dir / "top100_d1.0" / "selected_pairs.csv"
    mainline_pairs = {tuple(map(int, r)) for r in
                      pd.read_csv(main_csv)[["slow_edge_idx", "fast_edge_idx"]].to_numpy()}

    specs = [(f"theta_{float(t):g}".replace(".", "p"), float(t), 1.0, main_csv)
             for t in args.theta_grid.split(",") if t.strip()]
    for e in [x for x in args.eta_specs.split(",") if x.strip()]:
        ev, src = e.split(":")
        specs.append((f"theta_6p83_eta{float(ev):g}_{src}pairs".replace(".", "p"),
                      pm.AA_SEATTLE_THETA, float(ev), eta_csv if src == "own" else main_csv))

    summary_path = out / "summary.csv"
    rows = pd.read_csv(summary_path).to_dict("records") if summary_path.exists() else []
    done = {r["tag"] for r in rows}
    for tag, theta, eta, csv in specs:
        if tag in done:
            LOG.info("[%s] already in summary.csv, skipped", tag)
            continue
        try:
            r = run_spec(prim, out, tag=tag, theta=theta, eta=eta, pairs_csv=csv,
                         mainline_pairs=mainline_pairs, congestion_lambda=args.congestion_lambda,
                         xi_floor_rel=args.xi_floor_rel, kappa_rule=args.kappa_rule)
        except Exception as exc:  # record and move on; no tolerance changes
            LOG.exception("[%s] failed: %s", tag, exc)
            r = {"tag": tag, "theta": theta, "route_eta": eta, "error": repr(exc)}
        rows.append(r)
        pd.DataFrame(rows).to_csv(summary_path, index=False)
    LOG.info("Wrote %s", summary_path)


if __name__ == "__main__":
    main()
