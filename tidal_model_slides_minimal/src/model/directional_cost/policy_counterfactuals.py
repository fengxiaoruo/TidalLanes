"""Fixed-cost policy counterfactuals (non-directional), run in each calibration world."""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from src.model.full_estimation.build_primitives import Primitives
from src.model.full_estimation.parameters import Parameters
from src.model.directional_cost import fixed_tau_model as ft


EPS = 1e-12


@dataclass
class PolicySpec:
    name: str
    tau_scale: float = 1.0
    tau_scale_on_hub_edges: float | None = None
    tau_scale_on_outer_edges: float | None = None
    hub_quantile: float = 0.75
    outer_ring_dist_quantile: float = 0.75
    abar_mult_on_hubs: float = 1.0
    ubar_mult_on_outer_ring: float = 1.0


def _grid_xy(prim: Primitives) -> tuple[np.ndarray, np.ndarray]:
    xs, ys = [], []
    for gid in prim.node_grid_ids:
        parts = str(gid).split("_")
        xs.append(int(parts[1]) if len(parts) >= 3 else 0)
        ys.append(int(parts[2]) if len(parts) >= 3 else 0)
    return np.asarray(xs, dtype=float), np.asarray(ys, dtype=float)


def employment_hub_mask(prim: Primitives, quantile: float = 0.75) -> np.ndarray:
    pos = prim.L_F_obs > 0
    thr = float(np.quantile(prim.L_F_obs[pos], quantile))
    return prim.L_F_obs >= thr


def outer_ring_mask(prim: Primitives, quantile: float = 0.75) -> np.ndarray:
    """Fringe cells: far from employment-weighted grid centroid."""
    x, y = _grid_xy(prim)
    w = np.clip(prim.L_F_obs, EPS, None)
    cx = float(np.average(x, weights=w))
    cy = float(np.average(y, weights=w))
    dist = np.hypot(x - cx, y - cy)
    valid = prim.L_R_obs > 0
    thr = float(np.quantile(dist[valid], quantile))
    return dist >= thr


def apply_policy_tau(tau: np.ndarray, prim: Primitives, spec: PolicySpec) -> np.ndarray:
    out = np.asarray(tau, dtype=float) * spec.tau_scale
    if spec.tau_scale_on_hub_edges is not None:
        hub = employment_hub_mask(prim, spec.hub_quantile)
        touch = hub[prim.edge_i] | hub[prim.edge_j]
        out = out.copy()
        out[touch] *= spec.tau_scale_on_hub_edges
    if spec.tau_scale_on_outer_edges is not None:
        outer = outer_ring_mask(prim, spec.outer_ring_dist_quantile)
        touch = outer[prim.edge_i] | outer[prim.edge_j]
        out = out.copy()
        out[touch] *= spec.tau_scale_on_outer_edges
    return out


def apply_policy_abar(abar_theta: np.ndarray, prim: Primitives, spec: PolicySpec) -> np.ndarray:
    out = np.asarray(abar_theta, dtype=float).copy()
    if spec.abar_mult_on_hubs != 1.0:
        out[employment_hub_mask(prim, spec.hub_quantile)] *= spec.abar_mult_on_hubs
    return out


def apply_policy_ubar(ubar_theta: np.ndarray, prim: Primitives, spec: PolicySpec) -> np.ndarray:
    out = np.asarray(ubar_theta, dtype=float).copy()
    if spec.ubar_mult_on_outer_ring != 1.0:
        out[outer_ring_mask(prim, spec.outer_ring_dist_quantile)] *= spec.ubar_mult_on_outer_ring
    return out


def run_policy_in_world(
    prim: Primitives,
    params: Parameters,
    kappa: float,
    tau_base: np.ndarray,
    fund,
    eq_base: ft.StaticEquilibriumResult,
    spec: PolicySpec,
    eq_kw: dict,
    world_label: str,
) -> tuple[ft.StaticEquilibriumResult, dict]:
    tau_cf = apply_policy_tau(tau_base, prim, spec)
    abar_cf = apply_policy_abar(fund.abar_theta, prim, spec)
    ubar_cf = apply_policy_ubar(fund.ubar_theta, prim, spec)
    eq_cf = ft.solve_static_equilibrium(
        prim, tau_cf, ubar_cf, abar_cf, fund.chi, params,
        kappa=kappa, label=f"{world_label}_{spec.name}", **eq_kw,
    )
    row = ft.comparison_summary(eq_base, eq_cf, prim, params, abar_theta=fund.abar_theta)
    row.update({
        "world": world_label,
        "policy": spec.name,
        "Wbar_level_cf": eq_cf.Wbar,
        "Wbar_level_base": eq_base.Wbar,
        "share_nodes_outer_ring_amenity": float(
            outer_ring_mask(prim, spec.outer_ring_dist_quantile).mean()
        ) if spec.ubar_mult_on_outer_ring != 1.0 else 0.0,
    })
    row.update(_market_access_shift(eq_base, eq_cf))
    return eq_cf, row


def _market_access_shift(base: ft.StaticEquilibriumResult, cf: ft.StaticEquilibriumResult) -> dict:
    log_pi = np.log(np.clip(cf.Pi_i, EPS, None)) - np.log(np.clip(base.Pi_i, EPS, None))
    log_p = np.log(np.clip(cf.P_j, EPS, None)) - np.log(np.clip(base.P_j, EPS, None))
    return {
        "mean_abs_log_Pi_change": float(np.mean(np.abs(log_pi))),
        "p95_abs_log_Pi_change": float(np.quantile(np.abs(log_pi), 0.95)),
        "mean_abs_log_P_change": float(np.mean(np.abs(log_p))),
        "p95_abs_log_P_change": float(np.quantile(np.abs(log_p), 0.95)),
    }


def default_policies() -> list[PolicySpec]:
    return [
        PolicySpec(name="uniform_speed_10pct", tau_scale=0.90),
        PolicySpec(name="hub_access_15pct", tau_scale=1.0, tau_scale_on_hub_edges=0.85),
        PolicySpec(name="hub_productivity_10pct", abar_mult_on_hubs=1.10),
        PolicySpec(name="outer_ring_amenity_10pct", ubar_mult_on_outer_ring=1.10),
        PolicySpec(name="fringe_transport_20pct", tau_scale=1.0, tau_scale_on_outer_edges=0.80),
    ]


def run_all(
    prim: Primitives,
    params: Parameters,
    kappa: float,
    worlds: dict,
    eq_kw: dict,
    policies: list[PolicySpec] | None = None,
) -> pd.DataFrame:
    policies = policies or default_policies()
    rows = []
    for world_label, bundle in worlds.items():
        fund = bundle["fund"]
        eq_base = bundle["eq"]
        tau_base = bundle["tau"]
        for spec in policies:
            _, row = run_policy_in_world(
                prim, params, kappa, tau_base, fund, eq_base, spec, eq_kw, world_label,
            )
            rows.append(row)
    return pd.DataFrame(rows)


def node_maps_frame(
    prim: Primitives,
    params: Parameters,
    fund_dir,
    fund_sym,
    eq_dir: ft.StaticEquilibriumResult,
    eq_sym: ft.StaticEquilibriumResult,
) -> pd.DataFrame:
    theta = params.theta
    alpha = params.alpha
    pi_inv_dir = np.clip(eq_dir.Pi_i, EPS, None) ** (-theta)
    pi_inv_sym = np.clip(eq_sym.Pi_i, EPS, None) ** (-theta)
    p_inv_dir = np.clip(eq_dir.P_j, EPS, None) ** (-theta)
    p_inv_sym = np.clip(eq_sym.P_j, EPS, None) ** (-theta)
    wage_dir = np.clip(fund_dir.abar_theta, EPS, None) ** (1.0 / theta) * (
        np.clip(eq_dir.l_F, EPS, None) ** alpha
    )
    wage_sym = np.clip(fund_sym.abar_theta, EPS, None) ** (1.0 / theta) * (
        np.clip(eq_sym.l_F, EPS, None) ** alpha
    )
    x, y = _grid_xy(prim)
    return pd.DataFrame({
        "node_idx": np.arange(prim.n_nodes),
        "grid_id": prim.node_grid_ids,
        "sq_x": x,
        "sq_y": y,
        "L_R_obs": prim.L_R_obs,
        "L_F_obs": prim.L_F_obs,
        "is_outer_ring": outer_ring_mask(prim).astype(int),
        "log_pi_inv_dir": np.log(pi_inv_dir),
        "log_pi_inv_sym": np.log(pi_inv_sym),
        "log_p_inv_dir": np.log(p_inv_dir),
        "log_p_inv_sym": np.log(p_inv_sym),
        "log_wage_index_dir": np.log(wage_dir),
        "log_wage_index_sym": np.log(wage_sym),
        "log_pi_inv_sym_minus_dir": np.log(pi_inv_sym) - np.log(pi_inv_dir),
        "log_p_inv_sym_minus_dir": np.log(p_inv_sym) - np.log(p_inv_dir),
        "log_wage_sym_minus_dir": np.log(wage_sym) - np.log(wage_dir),
    })


def write_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, default=float))
