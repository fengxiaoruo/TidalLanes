"""
Counterfactual runners — level-form, no exact-hat
=================================================

Three scenarios from v4.tex §6.8:

    CF1  Symmetric vs. directional commuting costs
        Replace t_kl^obs with the corridor mean of the two directions
            (geometric: sqrt(t_kl^obs * t_lk^obs); arithmetic: the mean),
        for each (k,l) with a reverse edge. Lane capacities and
        fundamentals are held fixed.
        Implementation: build a symmetrised tau_obs_min table. The EXOGENOUS
        variant fixes that table as the travel-cost field, isolating the
        bilateral-cost measurement effect. The ENDOGENOUS diagnostic uses the
        same table as the initial condition, then lets the anchored congestion
        technology update edge times as traffic re-equilibrates.

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
import logging
from pathlib import Path

import numpy as np
import pandas as pd

from .parameters import Parameters
from .build_primitives import Primitives
from .equilibrium_solve import EquilibriumResult, MAINLINE_PAIR_SELECTION, solve_equilibrium


# --------------------------------------------------------------------------- #
# CF1 — symmetric cost
# --------------------------------------------------------------------------- #
def build_symmetric_tau_min(prim: Primitives, method: str = "geometric") -> np.ndarray:
    """Symmetrise reciprocal directed edges (link-level, not OD).

    ``geometric`` matches Stage~1 main specification; ``arithmetic`` is legacy.
    """
    pair = {}
    for idx, (i, j) in enumerate(zip(prim.edge_i, prim.edge_j)):
        pair[(int(i), int(j))] = idx
    tau = prim.tau_obs_min.copy()
    visited: set[tuple[int, int]] = set()
    for idx, (i, j) in enumerate(zip(prim.edge_i, prim.edge_j)):
        rev = pair.get((int(j), int(i)))
        if rev is None:
            continue
        key = tuple(sorted((idx, rev)))
        if key in visited:
            continue
        visited.add(key)
        t_ij = float(prim.tau_obs_min[idx])
        t_ji = float(prim.tau_obs_min[rev])
        if method == "geometric":
            sym = float(np.sqrt(t_ij * t_ji))
        elif method == "arithmetic":
            sym = 0.5 * (t_ij + t_ji)
        else:
            raise ValueError(f"unknown symmetrisation method: {method}")
        tau[idx] = sym
        tau[rev] = sym
    return tau


def run_cf1_symmetric(
    prim: Primitives, params: Parameters,
    ubar_theta: np.ndarray, abar_theta: np.ndarray,
    sym_method: str = "geometric",
    **solver_kwargs,
) -> EquilibriumResult:
    """CF1: re-solve equilibrium starting from symmetrised tau_obs."""
    tau_sym_init = build_symmetric_tau_min(prim, method=sym_method)
    _d = np.log(np.clip(tau_sym_init, 1e-12, None)) - np.log(np.clip(prim.tau_obs_min, 1e-12, None))
    logging.getLogger("counterfactual").info(
        "CF1 %s symmetrisation shock: median |dln tau|=%.4f, max=%.4f over %d edges",
        sym_method, float(np.median(np.abs(_d))), float(np.max(np.abs(_d))), _d.size,
    )
    return solve_equilibrium(
        prim, ubar_theta, abar_theta, params,
        n_kl_override=None,
        tau_min_init=tau_sym_init,
        label=f"CF1_symmetric_{sym_method}",
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


def _iter_eligible_reciprocal_pairs(
    prim: Primitives,
    *,
    min_length_m: float = 200.0,
    delta: float = 1.0,
    min_asym_ratio: float = 0.0,
) -> list[tuple[int, int, float]]:
    """Yield (slow_idx, fast_idx, asym_ratio) for eligible reciprocal pairs."""
    pair: dict[tuple[int, int], int] = {}
    for idx, (i, j) in enumerate(zip(prim.edge_i, prim.edge_j)):
        pair[(int(i), int(j))] = int(idx)

    rows: list[tuple[int, int, float]] = []
    seen: set[tuple[int, int]] = set()
    for idx, (i, j) in enumerate(zip(prim.edge_i, prim.edge_j)):
        rev = pair.get((int(j), int(i)))
        if rev is None:
            continue
        key = tuple(sorted((idx, rev)))
        if key in seen:
            continue
        seen.add(key)
        a, b = key
        tau_a, tau_b = float(prim.tau_obs_min[a]), float(prim.tau_obs_min[b])
        if tau_a >= tau_b:
            slow, fast = a, b
        else:
            slow, fast = b, a
        len_m = min(prim.ell_kl_km[slow], prim.ell_kl_km[fast]) * 1000.0
        if len_m < min_length_m:
            continue
        asym = float(prim.tau_obs_min[slow] / max(prim.tau_obs_min[fast], 1e-9))
        if asym < min_asym_ratio:
            continue
        if prim.n_kl[fast] - delta < 1.0:
            continue
        rows.append((slow, fast, asym))
    return rows


def bpr_st_pair_score(
    prim: Primitives,
    slow: int,
    fast: int,
    xi_kl: np.ndarray,
) -> float:
    """ST first-order score: q_s*(t_s - t_ff) - q_f*(t_f - t_ff) with q = Xi."""
    delay_s = max(float(prim.tau_obs_min[slow] - prim.tau_ff_min[slow]), 0.0)
    delay_f = max(float(prim.tau_obs_min[fast] - prim.tau_ff_min[fast]), 0.0)
    return float(xi_kl[slow]) * delay_s - float(xi_kl[fast]) * delay_f


def select_tidal_lane_pairs_by_rank(
    prim: Primitives,
    top_k: int = 100,
    min_length_m: float = 200.0,
    delta: float = 1.0,
    *,
    rank: str = MAINLINE_PAIR_SELECTION,
    xi_kl: np.ndarray | None = None,
) -> TidalLaneSelection:
    """Select tidal pairs by BPR-ST score (mainline) or legacy ranks (asym/ST/LT)."""
    if rank == "asym":
        return select_tidal_lane_pairs(prim, top_k=top_k, min_length_m=min_length_m, delta=delta)

    if rank == "score_bpr_st":
        if xi_kl is None:
            raise ValueError("score_bpr_st requires baseline equilibrium Xi_kl")
        rows = _iter_eligible_reciprocal_pairs(
            prim, min_length_m=min_length_m, delta=delta,
        )
        scored = [
            (s, f, asym, bpr_st_pair_score(prim, s, f, xi_kl))
            for s, f, asym in rows
        ]
        scored.sort(key=lambda r: -r[3])
        treated = [(s, f) for s, f, _, _ in scored[:top_k]]
        ratios = [a for _, _, a, _ in scored[:top_k]]
        return TidalLaneSelection(
            treated_pairs=np.asarray(treated, dtype=int).reshape(-1, 2),
            asym_ratio=np.asarray(ratios, dtype=float),
            n_treated=len(treated),
            n_skipped=max(0, len(rows) - len(treated)),
            skip_reason=[],
        )

    from src.model.full_estimation.score_tidal_corridor_candidates import build_candidate_table

    score_col = {"score_st": "score_st", "score_lt": "score_lt"}.get(rank)
    if score_col is None:
        raise ValueError(
            f"unknown rank: {rank!r}; use asym, score_bpr_st, score_st, or score_lt"
        )

    df = build_candidate_table(prim, min_length_m=min_length_m, delta_lanes=delta)
    if df.empty:
        return TidalLaneSelection(
            treated_pairs=np.zeros((0, 2), dtype=int),
            asym_ratio=np.zeros(0, dtype=float),
            n_treated=0, n_skipped=0, skip_reason=["no candidates"],
        )

    df = df.sort_values(score_col, ascending=False)
    treated, ratios, skipped, skip_reason = [], [], 0, []
    for _, row in df.iterrows():
        slow, fast = int(row["slow_edge_idx"]), int(row["fast_edge_idx"])
        if prim.n_kl[fast] - delta < 1.0:
            skipped += 1
            skip_reason.append(f"fast lane <2: idx={fast}")
            continue
        treated.append((slow, fast))
        ratios.append(float(row["asym_ratio"]))
        if len(treated) >= top_k:
            break

    return TidalLaneSelection(
        treated_pairs=np.asarray(treated, dtype=int).reshape(-1, 2),
        asym_ratio=np.asarray(ratios, dtype=float),
        n_treated=len(treated), n_skipped=skipped, skip_reason=skip_reason,
    )


def run_cf2_tidal(
    prim: Primitives, params: Parameters,
    ubar_theta: np.ndarray, abar_theta: np.ndarray,
    top_k: int = 100, delta: float = 1.0, min_length_m: float = 200.0,
    pair_selection: str = MAINLINE_PAIR_SELECTION,
    xi_kl: np.ndarray | None = None,
    **solver_kwargs,
) -> tuple[EquilibriumResult, TidalLaneSelection]:
    """Apply +delta lanes to slow direction, -delta to fast direction."""
    sel = select_tidal_lane_pairs_by_rank(
        prim, top_k=top_k, delta=delta, min_length_m=min_length_m,
        rank=pair_selection,
        xi_kl=xi_kl if pair_selection == "score_bpr_st" else None,
    )
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


def qdelay_pair_score(
    prim: Primitives,
    slow: int,
    fast: int,
    xi_kl: np.ndarray,
) -> float:
    """Congestion delay exposure: q_s·delay_s + q_f·delay_f on reciprocal pair."""
    delay_s = max(float(prim.tau_obs_min[slow] - prim.tau_ff_min[slow]), 0.0)
    delay_f = max(float(prim.tau_obs_min[fast] - prim.tau_ff_min[fast]), 0.0)
    return float(xi_kl[slow]) * delay_s + float(xi_kl[fast]) * delay_f


def select_tidal_lane_pairs_by_qdelay(
    prim: Primitives,
    top_k: int = 100,
    min_length_m: float = 200.0,
    delta: float = 1.0,
    xi_kl: np.ndarray | None = None,
) -> TidalLaneSelection:
    """Top-K reciprocal pairs by q·delay congestion exposure."""
    if xi_kl is None:
        raise ValueError("qdelay selection requires baseline Xi_kl")
    rows = _iter_eligible_reciprocal_pairs(
        prim, min_length_m=min_length_m, delta=delta,
    )
    scored = [
        (s, f, asym, qdelay_pair_score(prim, s, f, xi_kl))
        for s, f, asym in rows
    ]
    scored.sort(key=lambda r: -r[3])
    treated = [(s, f) for s, f, _, _ in scored[:top_k]]
    ratios = [a for _, _, a, _ in scored[:top_k]]
    return TidalLaneSelection(
        treated_pairs=np.asarray(treated, dtype=int).reshape(-1, 2),
        asym_ratio=np.asarray(ratios, dtype=float),
        n_treated=len(treated),
        n_skipped=max(0, len(rows) - len(treated)),
        skip_reason=[],
    )


def apply_bidirectional_lane_delta(
    prim: Primitives,
    sel: TidalLaneSelection,
    delta: float = 1.0,
) -> np.ndarray:
    """Add delta lanes to both directions of each treated pair."""
    n_cf = prim.n_kl.copy()
    for slow, fast in sel.treated_pairs:
        n_cf[slow] = n_cf[slow] + delta
        n_cf[fast] = n_cf[fast] + delta
    return n_cf


def uniform_expansion_factor_for_lane_budget(
    prim: Primitives,
    target_lane_km: float,
) -> float:
    """Scale all n_kl so total lane-km matches target (vs baseline)."""
    base_km = float(np.sum(prim.n_kl * prim.ell_kl_km))
    if base_km <= 0:
        return 1.0
    return 1.0 + target_lane_km / base_km


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
