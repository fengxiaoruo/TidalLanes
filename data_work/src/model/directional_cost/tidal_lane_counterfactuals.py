"""Tidal-lane counterfactuals on the Stage-1 fixed directed-cost model.

Policy logic (Spec-C style, no congestion propagation):
  - Select reciprocal edge pairs with peak-period travel-time asymmetry.
  - Move ``delta_lanes`` from the fast direction to the slow direction.
  - Update directed edge times via BPR at *observed* congestion (fixed xi):
        tau_new = tau_ff * (xi / lanes_new)^delta1,  tau_new >= tau_ff
  - Short-run: fix residence and workplace shares at baseline; recompute access,
    welfare, and implied OD flows through the new Leontief aggregator.
  - Long-run: re-solve the static spatial equilibrium with sorting.

Congestion does not propagate to untreated links.
"""
from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass, asdict
from pathlib import Path

import numpy as np
import pandas as pd

from src.model.full_estimation.build_primitives import Primitives
from src.model.full_estimation.parameters import Parameters, AA_SEATTLE_DELTA1
from src.model.directional_cost import fixed_tau_model as ft

EPS = 1e-12
DEFAULT_LANES_FALLBACK = 2.0


@dataclass(frozen=True)
class TidalLaneSpec:
    """One tidal-lane experiment."""

    name: str
    top_n: int = 50
    delta_lanes: float = 1.0
    min_asym_ratio: float = 1.10
    min_len_m: float = 200.0
    min_fast_lanes: float = 2.0
    method: str = "bpr"  # bpr | direct_tau
    slow_tau_scale: float = 0.90
    fast_tau_scale: float = 1.10
    delta1: float = AA_SEATTLE_DELTA1


def _edge_groups(prim: Primitives) -> dict[tuple[int, int], list[int]]:
    groups: dict[tuple[int, int], list[int]] = defaultdict(list)
    for idx in range(prim.n_edges):
        groups[(int(prim.edge_i[idx]), int(prim.edge_j[idx]))].append(idx)
    return groups


def _lane_count(prim: Primitives, edge_idx: int) -> float:
    n = float(prim.n_kl[edge_idx])
    if np.isfinite(n) and n > 0:
        return n
    return DEFAULT_LANES_FALLBACK


def _observed_xi(lanes: float, tau_obs: float, tau_ff: float, delta1: float) -> float:
    ratio = max(tau_obs, tau_ff) / max(tau_ff, EPS)
    return lanes * ratio ** (1.0 / delta1)


def build_reciprocal_pair_table(
    prim: Primitives,
    min_len_m: float = 200.0,
    min_asym_ratio: float = 1.0,
) -> pd.DataFrame:
    """Candidate reciprocal pairs ranked by peak travel-time asymmetry."""
    groups = _edge_groups(prim)
    tau = prim.tau_obs_min
    tau_ff = prim.tau_ff_min
    ell_m = prim.ell_kl_km * 1000.0
    rows: list[dict] = []
    visited: set[tuple[int, int]] = set()

    for (i, j), idx_ij in groups.items():
        if (i, j) in visited:
            continue
        idx_ji = groups.get((j, i), [])
        visited.add((i, j))
        visited.add((j, i))
        if not idx_ji:
            continue

        e_ij = int(idx_ij[0])
        e_ji = int(idx_ji[0])
        len_ij = float(np.mean(ell_m[idx_ij]))
        len_ji = float(np.mean(ell_m[idx_ji]))
        if min(len_ij, len_ji) < min_len_m:
            continue

        tau_ij = float(np.mean(tau[idx_ij]))
        tau_ji = float(np.mean(tau[idx_ji]))
        tau_ff_ij = float(np.mean(tau_ff[idx_ij]))
        tau_ff_ji = float(np.mean(tau_ff[idx_ji]))
        ratio = max(tau_ij, tau_ji) / min(tau_ij, tau_ji)
        if ratio < min_asym_ratio:
            continue

        lanes_ij = float(np.mean([_lane_count(prim, k) for k in idx_ij]))
        lanes_ji = float(np.mean([_lane_count(prim, k) for k in idx_ji]))
        rows.append({
            "node_i": i,
            "node_j": j,
            "grid_i": str(prim.node_grid_ids[i]),
            "grid_j": str(prim.node_grid_ids[j]),
            "edge_idx_ij": e_ij,
            "edge_idx_ji": e_ji,
            "len_ij_m": len_ij,
            "len_ji_m": len_ji,
            "tau_ij": tau_ij,
            "tau_ji": tau_ji,
            "tau_ff_ij": tau_ff_ij,
            "tau_ff_ji": tau_ff_ji,
            "lanes_ij": lanes_ij,
            "lanes_ji": lanes_ji,
            "asym_ratio": ratio,
        })

    return pd.DataFrame(rows).sort_values("asym_ratio", ascending=False).reset_index(drop=True)


def select_pairs(prim: Primitives, spec: TidalLaneSpec) -> pd.DataFrame:
    """Top-N eligible pairs under the spec filters."""
    pool = build_reciprocal_pair_table(
        prim, min_len_m=spec.min_len_m, min_asym_ratio=spec.min_asym_ratio,
    )
    if pool.empty:
        return pool

    picked: list[dict] = []
    for _, row in pool.iterrows():
        if len(picked) >= spec.top_n:
            break
        if row["tau_ij"] >= row["tau_ji"]:
            slow, fast = "ij", "ji"
            lanes_slow, lanes_fast = row["lanes_ij"], row["lanes_ji"]
            tau_ff_slow, tau_ff_fast = row["tau_ff_ij"], row["tau_ff_ji"]
            tau_slow, tau_fast = row["tau_ij"], row["tau_ji"]
            edge_slow, edge_fast = row["edge_idx_ij"], row["edge_idx_ji"]
        else:
            slow, fast = "ji", "ij"
            lanes_slow, lanes_fast = row["lanes_ji"], row["lanes_ij"]
            tau_ff_slow, tau_ff_fast = row["tau_ff_ji"], row["tau_ff_ij"]
            tau_slow, tau_fast = row["tau_ji"], row["tau_ij"]
            edge_slow, edge_fast = row["edge_idx_ji"], row["edge_idx_ij"]

        if lanes_fast < spec.min_fast_lanes:
            continue
        if lanes_fast - spec.delta_lanes < 1.0:
            continue

        rec = row.to_dict()
        rec.update({
            "slow_dir": slow,
            "fast_dir": fast,
            "lanes_slow": lanes_slow,
            "lanes_fast": lanes_fast,
            "tau_slow_obs": tau_slow,
            "tau_fast_obs": tau_fast,
            "edge_idx_slow": int(edge_slow),
            "edge_idx_fast": int(edge_fast),
        })
        picked.append(rec)

    out = pd.DataFrame(picked)
    if not out.empty:
        out["rank"] = np.arange(1, len(out) + 1)
    return out


def _bpr_tau_new(
    tau_ff: float,
    tau_obs: float,
    lanes_obs: float,
    lanes_new: float,
    delta1: float,
) -> float:
    xi = _observed_xi(lanes_obs, tau_obs, tau_ff, delta1)
    tau_new = tau_ff * (xi / max(lanes_new, EPS)) ** delta1
    return max(tau_new, tau_ff)


def apply_tidal_lane_shock(
    prim: Primitives,
    tau_base: np.ndarray,
    spec: TidalLaneSpec,
    selected: pd.DataFrame | None = None,
) -> tuple[np.ndarray, pd.DataFrame]:
    """Return edge-level tau after tidal-lane reallocation."""
    tau_out = np.asarray(tau_base, dtype=float).copy()
    selected = selected if selected is not None else select_pairs(prim, spec)
    if selected.empty:
        return tau_out, selected

    detail_rows: list[dict] = []
    for _, row in selected.iterrows():
        e_slow = int(row["edge_idx_slow"])
        e_fast = int(row["edge_idx_fast"])
        tau_slow_obs = float(row["tau_slow_obs"])
        tau_fast_obs = float(row["tau_fast_obs"])
        lanes_slow = float(row["lanes_slow"])
        lanes_fast = float(row["lanes_fast"])

        if spec.method == "bpr":
            tau_ff_slow = float(prim.tau_ff_min[e_slow])
            tau_ff_fast = float(prim.tau_ff_min[e_fast])
            lanes_slow_new = lanes_slow + spec.delta_lanes
            lanes_fast_new = lanes_fast - spec.delta_lanes
            tau_slow_new = _bpr_tau_new(
                tau_ff_slow, tau_slow_obs, lanes_slow, lanes_slow_new, spec.delta1,
            )
            tau_fast_new = _bpr_tau_new(
                tau_ff_fast, tau_fast_obs, lanes_fast, lanes_fast_new, spec.delta1,
            )
        elif spec.method == "direct_tau":
            lanes_slow_new = lanes_slow + spec.delta_lanes
            lanes_fast_new = lanes_fast - spec.delta_lanes
            tau_slow_new = tau_slow_obs * spec.slow_tau_scale
            tau_fast_new = tau_fast_obs * spec.fast_tau_scale
        else:
            raise ValueError(f"unknown tidal lane method: {spec.method}")

        tau_out[e_slow] = tau_slow_new
        tau_out[e_fast] = tau_fast_new
        detail_rows.append({
            **{k: row[k] for k in row.index if k != "rank"},
            "rank": int(row["rank"]),
            "spec_name": spec.name,
            "method": spec.method,
            "delta_lanes": spec.delta_lanes,
            "lanes_slow_new": lanes_slow_new,
            "lanes_fast_new": lanes_fast_new,
            "tau_slow_new": tau_slow_new,
            "tau_fast_new": tau_fast_new,
            "tau_slow_pct_change": 100.0 * (tau_slow_new / tau_slow_obs - 1.0),
            "tau_fast_pct_change": 100.0 * (tau_fast_new / tau_fast_obs - 1.0),
        })

    return tau_out, pd.DataFrame(detail_rows)


def solve_short_run(
    prim: Primitives,
    tau_min_kl: np.ndarray,
    ubar_theta: np.ndarray,
    abar_theta: np.ndarray,
    params: Parameters,
    kappa: float,
    l_R_fixed: np.ndarray,
    l_F_fixed: np.ndarray,
    label: str = "short_run",
) -> ft.StaticEquilibriumResult:
    """Fixed shares; recompute market access and welfare only."""
    l_R = np.clip(np.asarray(l_R_fixed, dtype=float), EPS, None)
    l_F = np.clip(np.asarray(l_F_fixed, dtype=float), EPS, None)
    l_R = l_R / l_R.sum()
    l_F = l_F / l_F.sum()
    tau_inv = ft.build_tau_inv_theta(prim, tau_min_kl, kappa=kappa)
    Pi, Pj = ft.market_access(tau_inv, l_R, l_F, prim.Lbar, params.theta)
    Wbar = ft.welfare(ubar_theta, abar_theta, tau_inv, l_R, l_F, prim.Lbar, params)
    return ft.StaticEquilibriumResult(
        label=label,
        tau_min_kl=np.asarray(tau_min_kl, dtype=float),
        tau_ij_inv_theta=tau_inv,
        l_R=l_R,
        l_F=l_F,
        Pi_i=Pi,
        P_j=Pj,
        Wbar=float(Wbar),
        n_iter=0,
        converged=True,
        max_log_change=0.0,
        kappa=float(kappa),
    )


def _market_access_metrics(base: ft.StaticEquilibriumResult,
                           scenario: ft.StaticEquilibriumResult) -> dict:
    log_pi = np.log(np.clip(scenario.Pi_i, EPS, None)) - np.log(np.clip(base.Pi_i, EPS, None))
    log_p = np.log(np.clip(scenario.P_j, EPS, None)) - np.log(np.clip(base.P_j, EPS, None))
    return {
        "mean_abs_log_Pi_change": float(np.mean(np.abs(log_pi))),
        "p95_abs_log_Pi_change": float(np.quantile(np.abs(log_pi), 0.95)),
        "mean_abs_log_P_change": float(np.mean(np.abs(log_p))),
        "p95_abs_log_P_change": float(np.quantile(np.abs(log_p), 0.95)),
    }


def evaluate_spec(
    prim: Primitives,
    params: Parameters,
    kappa: float,
    tau_base: np.ndarray,
    fund,
    eq_base: ft.StaticEquilibriumResult,
    spec: TidalLaneSpec,
    eq_kw: dict,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Run short- and long-run tidal-lane CF for one spec."""
    selected = select_pairs(prim, spec)
    tau_cf, edge_detail = apply_tidal_lane_shock(prim, tau_base, spec, selected=selected)

    summary_rows: list[dict] = []
    for horizon, runner in (
        ("short_run", _eval_short),
        ("long_run", _eval_long),
    ):
        eq_cf = runner(
            prim, params, kappa, tau_cf, fund, eq_base, spec, eq_kw,
        )
        row = ft.comparison_summary(
            eq_base, eq_cf, prim, params, abar_theta=fund.abar_theta,
        )
        row.update(_market_access_metrics(eq_base, eq_cf))
        row.update({
            "horizon": horizon,
            "spec_name": spec.name,
            "method": spec.method,
            "top_n": spec.top_n,
            "delta_lanes": spec.delta_lanes,
            "min_asym_ratio": spec.min_asym_ratio,
            "n_pairs_applied": int(len(selected)),
            "mean_tau_slow_pct_change": float(edge_detail["tau_slow_pct_change"].mean())
            if not edge_detail.empty else float("nan"),
            "mean_tau_fast_pct_change": float(edge_detail["tau_fast_pct_change"].mean())
            if not edge_detail.empty else float("nan"),
        })
        summary_rows.append(row)

    return pd.DataFrame(summary_rows), edge_detail


def _eval_short(prim, params, kappa, tau_cf, fund, eq_base, spec, eq_kw):
    return solve_short_run(
        prim, tau_cf, fund.ubar_theta, fund.abar_theta, params, kappa,
        l_R_fixed=eq_base.l_R,
        l_F_fixed=eq_base.l_F,
        label=f"{spec.name}_short_run",
    )


def _eval_long(prim, params, kappa, tau_cf, fund, eq_base, spec, eq_kw):
    return ft.solve_static_equilibrium(
        prim, tau_cf, fund.ubar_theta, fund.abar_theta, fund.chi, params,
        kappa=kappa, label=f"{spec.name}_long_run", **eq_kw,
    )


def default_experiment_grid() -> list[TidalLaneSpec]:
    """Small grid: sample size × lane scale × selection threshold."""
    specs: list[TidalLaneSpec] = []
    for top_n in (10, 25, 50, 100):
        for delta_lanes in (0.5, 1.0):
            specs.append(TidalLaneSpec(
                name=f"bpr_top{top_n}_d{str(delta_lanes).replace('.', 'p')}_r1p10",
                top_n=top_n,
                delta_lanes=delta_lanes,
                min_asym_ratio=1.10,
                method="bpr",
            ))
    for min_ratio in (1.25, 1.50, 2.00):
        specs.append(TidalLaneSpec(
            name=f"bpr_top50_d1_r{str(min_ratio).replace('.', 'p')}",
            top_n=50,
            delta_lanes=1.0,
            min_asym_ratio=min_ratio,
            method="bpr",
        ))
    for slow_scale, fast_scale in ((0.90, 1.10), (0.85, 1.15), (0.80, 1.20)):
        specs.append(TidalLaneSpec(
            name=f"direct_top50_s{str(slow_scale).replace('.', '')}_f{str(fast_scale).replace('.', '')}",
            top_n=50,
            delta_lanes=1.0,
            min_asym_ratio=1.10,
            method="direct_tau",
            slow_tau_scale=slow_scale,
            fast_tau_scale=fast_scale,
        ))
    return specs


def run_grid(
    prim: Primitives,
    params: Parameters,
    kappa: float,
    tau_base: np.ndarray,
    fund,
    eq_base: ft.StaticEquilibriumResult,
    eq_kw: dict,
    specs: list[TidalLaneSpec] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    specs = specs or default_experiment_grid()
    summary_parts: list[pd.DataFrame] = []
    edge_parts: list[pd.DataFrame] = []
    for spec in specs:
        summary, edges = evaluate_spec(
            prim, params, kappa, tau_base, fund, eq_base, spec, eq_kw,
        )
        summary_parts.append(summary)
        if not edges.empty:
            edge_parts.append(edges)
    summary_df = pd.concat(summary_parts, ignore_index=True)
    edge_df = pd.concat(edge_parts, ignore_index=True) if edge_parts else pd.DataFrame()
    return summary_df, edge_df


def write_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, default=float))
