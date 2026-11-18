"""Shared helpers for Phase A/B/C gate5 result packages."""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from src.model.directional_cost.policy_counterfactuals import employment_hub_mask, outer_ring_mask
from src.model.full_estimation import build_primitives as bp
from src.model.full_estimation import invert_fundamentals as inv
from src.model.full_estimation import parameters as pm
from src.model.full_estimation import stage2_counterfactuals as s2
from src.model.full_estimation.counterfactual_levels import select_tidal_lane_pairs_by_rank
from src.model.full_estimation.equilibrium_solve import (
    EquilibriumResult,
    MAINLINE_CONGESTION_FORM,
    MAINLINE_LAMBDA_PHYS,
    MAINLINE_PAIR_SELECTION,
    compute_observation_anchors,
)

EPS = 1e-12
DEFAULT_VERSION = "model_input_edgeproj_v2_lcc_gate5_v1"
DEFAULT_TOP_K = 100


def data_work_root(arg: str | Path) -> Path:
    p = Path(arg).resolve()
    if (p / "outputs").is_dir() and (p / "src" / "model").is_dir():
        return p
    if (p / "data_work" / "outputs").is_dir():
        return p / "data_work"
    return Path(__file__).resolve().parents[3]


def solver_kwargs(
    fund: inv.InversionResult,
    prim: bp.Primitives,
    params: pm.Parameters,
    *,
    congestion_form: str = MAINLINE_CONGESTION_FORM,
    congestion_lambda: float = MAINLINE_LAMBDA_PHYS,
    max_outer: int = 120,
    tol: float = 1e-5,
) -> dict:
    return s2.build_congestion_solver_kwargs(
        prim, fund, params,
        congestion_form=congestion_form,
        congestion_lambda=congestion_lambda,
        max_outer=max_outer,
        tol=tol,
    )


def load_fundamentals(data_work: Path, version_id: str = DEFAULT_VERSION):
    prim = bp.load_primitives(data_work / "outputs" / version_id, period="AM")
    params = pm.Parameters()
    fund = inv.invert_fundamentals(prim, params, tol=1.5e-4, max_iter=1800, kappa=1.0)
    return prim, params, fund


def load_equilibrium_npz(path: Path) -> dict[str, np.ndarray]:
    return dict(np.load(path))


def grid_xy(prim: bp.Primitives) -> tuple[np.ndarray, np.ndarray]:
    xs, ys = [], []
    for gid in prim.node_grid_ids:
        parts = str(gid).split("_")
        xs.append(int(parts[1]) if len(parts) >= 3 else 0)
        ys.append(int(parts[2]) if len(parts) >= 3 else 0)
    return np.asarray(xs, dtype=float), np.asarray(ys, dtype=float)


def employment_centroid(prim: bp.Primitives) -> tuple[float, float]:
    x, y = grid_xy(prim)
    w = np.clip(prim.L_F_obs, EPS, None)
    return float(np.average(x, weights=w)), float(np.average(y, weights=w))


def node_distance_to_hub(prim: bp.Primitives) -> np.ndarray:
    x, y = grid_xy(prim)
    cx, cy = employment_centroid(prim)
    return np.hypot(x - cx, y - cy)


def treated_edge_indices(sel_path: Path) -> np.ndarray:
    sel = pd.read_csv(sel_path)
    return np.unique(np.concatenate([sel["slow_edge_idx"].values, sel["fast_edge_idx"].values]))


def tidal_selection(
    prim: bp.Primitives,
    top_k: int = DEFAULT_TOP_K,
    delta: float = 1.0,
    pair_selection: str = MAINLINE_PAIR_SELECTION,
    xi_kl: np.ndarray | None = None,
    *,
    baseline: EquilibriumResult | None = None,
):
    """Select tidal pairs; score_bpr_st requires baseline Ξ (via xi_kl or baseline eq)."""
    if pair_selection == "score_bpr_st" and xi_kl is None:
        if baseline is None:
            raise ValueError("score_bpr_st requires xi_kl or baseline equilibrium with Xi_kl")
        xi_kl = baseline.Xi_kl
    return select_tidal_lane_pairs_by_rank(
        prim, top_k=top_k, delta=delta, rank=pair_selection, xi_kl=xi_kl,
    )


def write_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, default=float))


def node_change_frame(
    prim: bp.Primitives,
    base: dict[str, np.ndarray] | EquilibriumResult,
    scen: dict[str, np.ndarray] | EquilibriumResult,
    *,
    label: str,
) -> pd.DataFrame:
    def _get(obj, key):
        return getattr(obj, key) if isinstance(obj, EquilibriumResult) else obj[key]

    l_r0 = np.clip(_get(base, "l_R"), EPS, None)
    l_f0 = np.clip(_get(base, "l_F"), EPS, None)
    pi0 = np.clip(_get(base, "Pi_i"), EPS, None)
    pj0 = np.clip(_get(base, "P_j"), EPS, None)
    l_r1 = np.clip(_get(scen, "l_R"), EPS, None)
    l_f1 = np.clip(_get(scen, "l_F"), EPS, None)
    pi1 = np.clip(_get(scen, "Pi_i"), EPS, None)
    pj1 = np.clip(_get(scen, "P_j"), EPS, None)

    x, y = grid_xy(prim)
    dist_hub = node_distance_to_hub(prim)
    hub = employment_hub_mask(prim)
    outer = outer_ring_mask(prim)

    return pd.DataFrame({
        "node_idx": np.arange(prim.n_nodes),
        "grid_id": prim.node_grid_ids,
        "sq_x": x,
        "sq_y": y,
        "dist_to_hub": dist_hub,
        "is_hub": hub.astype(int),
        "is_outer_ring": outer.astype(int),
        "L_R_obs": prim.L_R_obs,
        "L_F_obs": prim.L_F_obs,
        "log_l_R_change": np.log(l_r1) - np.log(l_r0),
        "log_l_F_change": np.log(l_f1) - np.log(l_f0),
        "log_Pi_change": np.log(pi1) - np.log(pi0),
        "log_P_change": np.log(pj1) - np.log(pj0),
        "l_R_level_base": l_r0,
        "l_R_level_scenario": l_r1,
        "l_F_level_base": l_f0,
        "l_F_level_scenario": l_f1,
        "scenario": label,
    })
