"""Compare tidal-lane edge-selection rules on LT welfare (gate5, bpr+pos05).

Selections:
  - score_bpr_st : ST congestion relief  Xi_s*(t_s-t_s^ff) - Xi_f*(t_f-t_f^ff)   [current main]
  - score_sort   : (b) induced job-residence-distance proxy. High score = improving the
                   slow direction SHORTENS the dominant flows. Uses baseline B=(I-A)^{-1},
                   model OD flows M, and node coords. Per-edge flow-weighted mean OD distance:
                       meandist[k,l] = (B^T (M*d / B) B^T)_{kl} / (B^T (M / B) B^T)_{kl}
                   pair score = meandist[fast] - meandist[slow]   (slow shorter than fast = good).
  - score_hybrid : (c) z(bpr_st) - gamma * z(meandist[slow])  (keep ST gain, penalize long
                   induced flows on the improved/slow direction).

For each rule: pick top-100 eligible reciprocal pairs, solve ST (pinned shares) + LT
(full sorting) under anchored congestion, and report the ST/GE/sorting commute
decomposition and Wbar ST/LT.
"""
from __future__ import annotations

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
    solve_equilibrium,
    compute_observation_anchors,
)
from src.model.full_estimation.counterfactual_levels import (  # noqa: E402
    _iter_eligible_reciprocal_pairs,
    bpr_st_pair_score,
)
from src.model.full_estimation.tidal_lane_diagnostics import od_cost_decomposition, _to_static  # noqa: E402
from src.model.full_estimation.run_alpha_robustness_quick_gate5 import solver_overrides_for_params  # noqa: E402
from src.model.directional_cost import fixed_tau_model as ft  # noqa: E402

EPS = 1e-12
VERSION = "model_input_edgeproj_v2_lcc_gate5_v1"
TOP_K = 100
GAMMA = 1.0
OUT = DATA_WORK / "outputs" / "tidal_lt_selection_compare_v1"


def node_coords(prim) -> tuple[np.ndarray, np.ndarray]:
    xs = np.zeros(prim.n_nodes)
    ys = np.zeros(prim.n_nodes)
    for idx, gid in enumerate(prim.node_grid_ids):
        p = str(gid).split("_")
        if len(p) >= 3 and p[0] == "sq":
            xs[idx] = float(p[1]); ys[idx] = float(p[2])
    return xs, ys


def edge_mean_commute_distance(prim, B, M, xs, ys) -> np.ndarray:
    """Flow-weighted mean OD distance of trips traversing each directed edge."""
    Bc = np.clip(np.asarray(B, dtype=float), EPS, None)
    D = np.sqrt((xs[:, None] - xs[None, :]) ** 2 + (ys[:, None] - ys[None, :]) ** 2)
    X = M / Bc
    Hh = Bc.T @ X @ Bc.T
    Gg = Bc.T @ (X * D) @ Bc.T
    ei, ej = prim.edge_i, prim.edge_j
    return Gg[ei, ej] / np.clip(Hh[ei, ej], EPS, None)


def zscore(a: np.ndarray) -> np.ndarray:
    a = np.asarray(a, dtype=float)
    s = a.std()
    return (a - a.mean()) / s if s > 0 else a - a.mean()


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    prim = bp.load_primitives(DATA_WORK / "outputs" / VERSION, period="AM")
    params = replace(pm.Parameters(), alpha=0.05, beta=-0.10)
    skw = {k: v for k, v in solver_overrides_for_params(params).items() if v is not None}

    print("inverting fundamentals ...", flush=True)
    fund = inv.invert_fundamentals(prim, params, tol=1.5e-4, max_iter=1800, kappa=1.0)
    anchors = compute_observation_anchors(
        prim, fund.ubar_theta, fund.abar_theta, params, chi=fund.chi, kappa=fund.kappa
    )
    kw = s2.build_congestion_solver_kwargs(
        prim, fund, params, congestion_form="anchored", congestion_lambda=1.1, **skw
    )
    kw.update(
        xi_obs_kl=anchors["xi_obs_kl"],
        n_obs_kl=anchors["n_obs_kl"],
        tau_obs_anchor=anchors["tau_obs_anchor"],
    )
    print("solving baseline ...", flush=True)
    base = s2.solve_baseline_endogenous(prim, fund, params, **kw)

    # --- per-edge mean commute distance from baseline objects ---
    xs, ys = node_coords(prim)
    static_base = _to_static(base)
    od0 = ft.predicted_od_shares(static_base, fund.abar_theta, params)
    M = od0 * prim.Lbar
    B = base.tau_ij_inv_theta
    mean_dist_edge = edge_mean_commute_distance(prim, B, M, xs, ys)

    rows = _iter_eligible_reciprocal_pairs(prim, min_length_m=200.0, delta=1.0)
    slow_idx = np.array([r[0] for r in rows], dtype=int)
    fast_idx = np.array([r[1] for r in rows], dtype=int)
    bpr = np.array([bpr_st_pair_score(prim, s, f, base.Xi_kl) for s, f, _ in rows])
    md_slow = mean_dist_edge[slow_idx]
    md_fast = mean_dist_edge[fast_idx]
    score_sort = md_fast - md_slow                      # (b)
    score_hybrid = zscore(bpr) - GAMMA * zscore(md_slow)  # (c)

    specs = {
        "score_bpr_st": bpr,
        "score_sort": score_sort,
        "score_hybrid": score_hybrid,
    }

    def pick(scores: np.ndarray) -> np.ndarray:
        order = np.argsort(-scores)[:TOP_K]
        return np.stack([slow_idx[order], fast_idx[order]], axis=1)

    def solve_tidal(pairs: np.ndarray, horizon: str):
        n_cf = prim.n_kl.copy()
        for s_, f_ in pairs:
            n_cf[s_] += 1.0
            n_cf[f_] -= 1.0
        pin = dict(l_R_fixed=base.l_R, l_F_fixed=base.l_F) if horizon == "short_run" else {}
        return solve_equilibrium(
            prim, fund.ubar_theta, fund.abar_theta, params,
            n_kl_override=n_cf, tau_min_init=prim.tau_obs_min.copy(),
            endogenous_congestion=True, label=f"tidal_{horizon}", **pin, **kw,
        )

    bpr_pairs_set = {tuple(p) for p in pick(specs["score_bpr_st"])}
    only = sys.argv[1] if len(sys.argv) > 1 else None
    to_run = [only] if only else list(specs.keys())
    csv_path = OUT / "lt_selection_compare.csv"

    for name in to_run:
        scores = specs[name]
        print(f"solving selection {name} ...", flush=True)
        pairs = pick(scores)
        eq_st = solve_tidal(pairs, "short_run")
        eq_lt = solve_tidal(pairs, "long_run")
        dec = od_cost_decomposition(base, eq_st, eq_lt, prim, params, fund.abar_theta)
        overlap = len({tuple(p) for p in pairs} & bpr_pairs_set)
        row = {
            "selection": name,
            "n_pairs": len(pairs),
            "overlap_with_bpr": overlap,
            "ST_direct_sec": dec["direct_st_cost_change_sec"],
            "GE_wedge_sec": dec["ge_cost_wedge_sec"],
            "sorting_wedge_sec": dec["sorting_wedge_sec"],
            "LT_total_sec": dec["total_lt_cost_change_sec"],
            "person_min_day": dec["total_lt_cost_change_sec"] / 60.0 * prim.Lbar,
            "Wbar_ST_pct": 100.0 * (eq_st.Wbar / base.Wbar - 1.0),
            "Wbar_LT_pct": 100.0 * (eq_lt.Wbar / base.Wbar - 1.0),
            "st_conv": bool(eq_st.converged),
            "lt_conv": bool(eq_lt.converged),
            "mean_dist_slow_avg": float(md_slow[np.argsort(-scores)[:TOP_K]].mean()),
        }
        if csv_path.exists():
            df = pd.read_csv(csv_path)
            df = df[df["selection"] != name]
            df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
        else:
            df = pd.DataFrame([row])
        df.to_csv(csv_path, index=False)
        print(pd.DataFrame([row]).to_string(index=False), flush=True)

    print("\nDONE -> " + str(csv_path), flush=True)


if __name__ == "__main__":
    main()
