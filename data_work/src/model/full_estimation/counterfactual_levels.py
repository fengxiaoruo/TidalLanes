"""
Counterfactual runners — level-form, no exact-hat
=================================================

Three scenarios from v4.tex §6.8:

    CF1  Symmetric vs. directional commuting costs
        Replace t_kl^obs with the corridor mean
            (t_kl^obs + t_lk^obs) / 2,  for each (k,l) with reverse edge.
        Lane capacities and fundamentals are held fixed.
        Implementation: build a symmetrised tau_obs_min table, feed
        it into the equilibrium solver as a *starting state* with
        damping = 1 (no congestion update) — this isolates the
        bilateral-cost effect.  CF1 reports the same equilibrium re-
        solved with a different observed t_kl; we still let the
        congestion technology update so the GE adjustment is captured.

    CF2  Tidal-lane reallocation
        On a treated set E* of directed edges, +Delta on the slow
        direction and -Delta on the fast direction, preserving
        corridor capacity n_kl + n_lk.
        Treated set: top-K bidirectional pairs by asymmetry
        ratio = max(tau_obs_min)/min(tau_obs_min); fast direction
        must retain n_lk - Delta >= 1 lane.

    CF3  Uniform lane expansion
        n_kl_cf = factor * n_kl, factor in {1.10, 1.25, 1.50}.

For each scenario, we re-solve the level-form equilibrium
(solve_equilibrium) and compare:
    Wbar_cf vs Wbar_baseline
    l_R_cf, l_F_cf vs baseline
    tau_min_cf vs tau_min_obs
    Xi_cf vs Xi_baseline

We do NOT touch (ubar_theta, abar_theta) — by construction those are
inverted at baseline and are policy-invariant fundamentals.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict, field
import json
from pathlib import Path

import numpy as np
import pandas as pd

from .parameters import Parameters
from .build_primitives import Primitives
from .equilibrium_solve import EquilibriumResult, solve_equilibrium


# --------------------------------------------------------------------------- #
# CF1 — symmetric cost
# --------------------------------------------------------------------------- #
def build_symmetric_tau_min(prim: Primitives) -> np.ndarray:
    """Return tau_obs_min where each bidirectional pair is averaged.

    For directed edge (i,j) with reverse (j,i) present, replace both
    by their arithmetic mean. Unpaired edges are kept unchanged.
    """
    pair = {}
    for idx, (i, j) in enumerate(zip(prim.edge_i, prim.edge_j)):
        pair[(int(i), int(j))] = idx
    tau = prim.tau_obs_min.copy()
    for idx, (i, j) in enumerate(zip(prim.edge_i, prim.edge_j)):
        rev = pair.get((int(j), int(i)))
        if rev is None or rev == idx:
            continue
        avg = 0.5 * (prim.tau_obs_min[idx] + prim.tau_obs_min[rev])
        tau[idx] = avg
    return tau


def run_cf1_symmetric(
    prim: Primitives, params: Parameters,
    ubar_theta: np.ndarray, abar_theta: np.ndarray,
    **solver_kwargs,
) -> EquilibriumResult:
    """CF1: re-solve equilibrium starting from corridor-averaged tau_obs.

    We seed `tau_min_init = symmetric tau_obs`; the outer loop then
    re-runs the congestion technology to find the GE-consistent t_kl.
    """
    tau_sym_init = build_symmetric_tau_min(prim)
    return solve_equilibrium(
        prim, ubar_theta, abar_theta, params,
        n_kl_override=None,
        tau_min_init=tau_sym_init,
        **solver_kwargs,
    )


# --------------------------------------------------------------------------- #
# CF2 — tidal-lane reallocation
# --------------------------------------------------------------------------- #
@dataclass
class TidalLaneSelection:
    treated_pairs: np.ndarray   # shape (K, 2) of (slow_idx, fast_idx) edge indices
    asym_ratio: np.ndarray      # shape (K,) max/min of tau_obs
    n_treated: int
    n_skipped: int
    skip_reason: list = field(default_factory=list)

    def to_frame(self, prim: Primitives) -> pd.DataFrame:
        rows = []
        for k, (s, f) in enumerate(self.treated_pairs):
            rows.append({
                "slow_edge_idx": int(s), "fast_edge_idx": int(f),
                "slow_i": int(prim.edge_i[s]), "slow_j": int(prim.edge_j[s]),
                "fast_i": int(prim.edge_i[f]), "fast_j": int(prim.edge_j[f]),
                "tau_slow_min": float(prim.tau_obs_min[s]),
                "tau_fast_min": float(prim.tau_obs_min[f]),
                "n_slow_pre": float(prim.n_kl[s]),
                "n_fast_pre": float(prim.n_kl[f]),
                "asym_ratio": float(self.asym_ratio[k]),
            })
        return pd.DataFrame(rows)


def select_tidal_lane_pairs(
    prim: Primitives, top_k: int = 100,
    min_length_m: float = 200.0, delta: float = 1.0,
) -> TidalLaneSelection:
    """Pick top-K bidirectional pairs by tau-asymmetry, eligibility-filtered."""
    pair = {}
    for idx, (i, j) in enumerate(zip(prim.edge_i, prim.edge_j)):
        pair[(int(i), int(j))] = idx

    rows = []
    seen = set()
    for idx, (i, j) in enumerate(zip(prim.edge_i, prim.edge_j)):
        rev = pair.get((int(j), int(i)))
        if rev is None: continue
        key = tuple(sorted((idx, rev)))
        if key in seen: continue
        seen.add(key)
        a, b = key
        tau_a, tau_b = prim.tau_obs_min[a], prim.tau_obs_min[b]
        if tau_a >= tau_b:
            slow, fast = a, b
        else:
            slow, fast = b, a
        # length filter — both directions must be at least min_length_m
        L_a_m = prim.ell_kl_km[a] * 1000.0
        L_b_m = prim.ell_kl_km[b] * 1000.0
        if min(L_a_m, L_b_m) < min_length_m: continue
        asym = float(prim.tau_obs_min[slow] / max(prim.tau_obs_min[fast], 1e-9))
        rows.append((slow, fast, asym))
    rows.sort(key=lambda r: -r[2])

    treated, ratios, skipped, skip_reason = [], [], 0, []
    for slow, fast, asym in rows[:top_k]:
        if prim.n_kl[fast] - delta < 1.0:
            skipped += 1
            skip_reason.append(f"fast lane <2: idx={fast}")
            continue
        treated.append((slow, fast)); ratios.append(asym)

    return TidalLaneSelection(
        treated_pairs=np.asarray(treated, dtype=int).reshape(-1, 2),
        asym_ratio=np.asarray(ratios, dtype=float),
        n_treated=len(treated), n_skipped=skipped, skip_reason=skip_reason,
    )


def run_cf2_tidal(
    prim: Primitives, params: Parameters,
    ubar_theta: np.ndarray, abar_theta: np.ndarray,
    top_k: int = 100, delta: float = 1.0, min_length_m: float = 200.0,
    **solver_kwargs,
) -> tuple[EquilibriumResult, TidalLaneSelection]:
    """Apply +delta lanes to slow direction, -delta to fast direction."""
    sel = select_tidal_lane_pairs(prim, top_k=top_k, delta=delta, min_length_m=min_length_m)
    n_cf = prim.n_kl.copy()
    for (slow, fast) in sel.treated_pairs:
        n_cf[slow] = n_cf[slow] + delta
        n_cf[fast] = n_cf[fast] - delta
    eq = solve_equilibrium(prim, ubar_theta, abar_theta, params,
                            n_kl_override=n_cf, **solver_kwargs)
    return eq, sel


# --------------------------------------------------------------------------- #
# CF3 — uniform lane expansion
# --------------------------------------------------------------------------- #
def run_cf3_uniform(
    prim: Primitives, params: Parameters,
    ubar_theta: np.ndarray, abar_theta: np.ndarray,
    factor: float = 1.10,
    **solver_kwargs,
) -> EquilibriumResult:
    """n_kl_cf = factor * n_kl uniformly across edges."""
    n_cf = prim.n_kl * float(factor)
    return solve_equilibrium(prim, ubar_theta, abar_theta, params,
                              n_kl_override=n_cf, **solver_kwargs)


# --------------------------------------------------------------------------- #
# Comparison helpers
# --------------------------------------------------------------------------- #
def summarise_diff(baseline: EquilibriumResult, cf: EquilibriumResult,
                    prim: Primitives) -> pd.DataFrame:
    """Return a short comparison frame (one row, key metrics)."""
    return pd.DataFrame([{
        "Wbar_baseline": baseline.Wbar,
        "Wbar_cf": cf.Wbar,
        "Wbar_pct_change": (cf.Wbar / baseline.Wbar - 1) if baseline.Wbar > 0 else np.nan,
        "tau_min_mean_baseline": float(np.mean(baseline.tau_min_kl)),
        "tau_min_mean_cf": float(np.mean(cf.tau_min_kl)),
        "tau_min_pct_change": float(np.mean(cf.tau_min_kl) / np.mean(baseline.tau_min_kl) - 1),
        "l_R_max_abs_log_change": float(np.max(np.abs(np.log(np.clip(cf.l_R, 1e-12, None))
                                                        - np.log(np.clip(baseline.l_R, 1e-12, None))))),
        "l_F_max_abs_log_change": float(np.max(np.abs(np.log(np.clip(cf.l_F, 1e-12, None))
                                                        - np.log(np.clip(baseline.l_F, 1e-12, None))))),
        "converged_baseline": baseline.converged,
        "converged_cf": cf.converged,
    }])
