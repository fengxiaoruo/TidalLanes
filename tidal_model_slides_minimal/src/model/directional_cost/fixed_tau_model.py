"""Static directed-cost equilibrium tools.

The model keeps directed travel time fixed. This isolates the spatial
equilibrium consequences of replacing a directed cost matrix with a symmetric
approximation before adding any lane-capacity or traffic-count technology.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
import json
from pathlib import Path
from collections import defaultdict

import numpy as np
import pandas as pd
from scipy import linalg as sla

from src.model.full_estimation.build_primitives import Primitives
from src.model.full_estimation.parameters import Parameters


EPS = 1e-12


@dataclass
class StaticEquilibriumResult:
    """Equilibrium with fixed directed edge costs."""

    label: str
    tau_min_kl: np.ndarray
    tau_ij_inv_theta: np.ndarray
    l_R: np.ndarray
    l_F: np.ndarray
    Pi_i: np.ndarray
    P_j: np.ndarray
    Wbar: float
    n_iter: int
    converged: bool
    max_log_change: float
    kappa: float

    def write(self, out_dir: Path) -> None:
        out_dir = Path(out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        np.savez_compressed(
            out_dir / "static_equilibrium_arrays.npz",
            tau_min_kl=self.tau_min_kl,
            l_R=self.l_R,
            l_F=self.l_F,
            Pi_i=self.Pi_i,
            P_j=self.P_j,
        )
        meta = {
            k: v for k, v in asdict(self).items()
            if not isinstance(v, np.ndarray)
        }
        (out_dir / "static_equilibrium_meta.json").write_text(
            json.dumps(meta, indent=2, default=float)
        )


def observed_shares(prim: Primitives, floor_share: float = 1e-6) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Return floored residence and workplace shares plus valid-node mask."""
    L_R = np.maximum(prim.L_R_obs, floor_share)
    L_F = np.maximum(prim.L_F_obs, floor_share)
    l_R = L_R / L_R.sum()
    l_F = L_F / L_F.sum()
    valid = (prim.L_R_obs > 0) & (prim.L_F_obs > 0)
    return l_R, l_F, valid


def pick_kappa(
    edge_i: np.ndarray,
    tau_min_kl: np.ndarray,
    n_nodes: int,
    target_rowsum: float = 0.9,
) -> float:
    """Choose a uniform scaling so row sums of A are bounded."""
    raw = np.zeros(n_nodes)
    np.add.at(raw, edge_i, 1.0 / np.clip(tau_min_kl, EPS, None))
    max_rowsum = float(raw.max())
    return max(1.0, max_rowsum / target_rowsum)


def build_tau_inv_theta(
    prim: Primitives,
    tau_min_kl: np.ndarray,
    kappa: float,
) -> np.ndarray:
    """Return ``(I-A)^(-1)`` with ``A_kl = 1/(kappa*tau_kl)``."""
    A = np.zeros((prim.n_nodes, prim.n_nodes), dtype=float)
    inv = 1.0 / np.clip(kappa * tau_min_kl, EPS, None)
    np.add.at(A, (prim.edge_i, prim.edge_j), inv)
    try:
        out = sla.solve(np.eye(prim.n_nodes) - A, np.eye(prim.n_nodes), check_finite=False)
    except sla.LinAlgError as exc:
        raise RuntimeError("static directed-cost system violates the spectral-radius condition") from exc
    return np.clip(out, EPS, None)


def _parse_square_grid_coords(grid_id: str) -> tuple[int, int] | None:
    parts = str(grid_id).split("_")
    if len(parts) >= 3 and parts[0] == "sq":
        return int(parts[1]), int(parts[2])
    return None


def _spatial_neighbor_indices(prim: Primitives) -> list[set[int]]:
    """Rook-adjacent square-grid cells plus the cell itself."""
    coord_to_idx: dict[tuple[int, int], int] = {}
    for idx, gid in enumerate(prim.node_grid_ids):
        coords = _parse_square_grid_coords(gid)
        if coords is not None:
            coord_to_idx[coords] = idx

    neighbors: list[set[int]] = []
    for idx, gid in enumerate(prim.node_grid_ids):
        coords = _parse_square_grid_coords(gid)
        if coords is None:
            neighbors.append({idx})
            continue
        x, y = coords
        nb = {idx}
        for dx, dy in ((1, 0), (-1, 0), (0, 1), (0, -1)):
            nb_idx = coord_to_idx.get((x + dx, y + dy))
            if nb_idx is not None:
                nb.add(nb_idx)
        neighbors.append(nb)
    return neighbors


def _build_spatial_mean_cost(prim: Primitives) -> tuple[np.ndarray, pd.DataFrame]:
    """Average travel times over rook-neighbourhood endpoint pairs."""
    tau_obs = np.asarray(prim.tau_obs_min, dtype=float)
    spatial_nb = _spatial_neighbor_indices(prim)
    pair_lookup: dict[tuple[int, int], list[float]] = defaultdict(list)
    for idx, (i, j) in enumerate(zip(prim.edge_i, prim.edge_j)):
        pair_lookup[(int(i), int(j))].append(float(tau_obs[idx]))

    tau_sym = np.zeros_like(tau_obs)
    edge_signed = np.zeros(prim.n_edges)
    for idx, (i, j) in enumerate(zip(prim.edge_i, prim.edge_j)):
        vals: list[float] = []
        for ii in spatial_nb[int(i)]:
            for jj in spatial_nb[int(j)]:
                vals.extend(pair_lookup.get((ii, jj), []))
        tau_sym[idx] = float(np.mean(vals)) if vals else float(tau_obs[idx])
        edge_signed[idx] = float(np.log(max(tau_sym[idx], EPS) / max(tau_obs[idx], EPS)))

    edge_df = pd.DataFrame({
        "edge_idx": np.arange(prim.n_edges),
        "edge_i": prim.edge_i,
        "edge_j": prim.edge_j,
        "pair_id": [f"{i}_{j}" for i, j in zip(prim.edge_i, prim.edge_j)],
        "has_reverse": False,
        "tau_obs_min": tau_obs,
        "tau_sym_min": tau_sym,
        "signed_log_asym_component": edge_signed,
        "pair_abs_log_tau_ratio": np.abs(edge_signed),
        "pair_tau_ratio": np.exp(np.abs(edge_signed)),
        "log_tau_sym_over_obs": edge_signed,
    })
    pair_df = pd.DataFrame(columns=[
        "pair_id", "node_a", "node_b", "tau_ab_mean", "tau_ba_mean",
        "tau_sym", "abs_log_tau_ratio", "tau_ratio", "n_edges_ab", "n_edges_ba",
    ])
    pair_df.attrs["edge_df"] = edge_df
    return tau_sym, pair_df


def build_symmetric_cost(
    prim: Primitives,
    method: str = "geometric",
) -> tuple[np.ndarray, pd.DataFrame]:
    """Symmetrize directed edge costs.

    Methods
    -------
    geometric / arithmetic :
        Reciprocal-pair average of travel times on ``(i,j)`` and ``(j,i)``.
    speed_arithmetic :
        Reciprocal-pair average of directed speeds, then convert back to
        travel time using each direction's own route length.
    spatial_mean :
        For each directed edge, average observed travel times over all
        directed edges whose endpoints lie in the rook neighbourhood of the
        origin and destination cells.
    """
    if method not in {"geometric", "arithmetic", "speed_arithmetic", "spatial_mean"}:
        raise ValueError(
            "method must be one of geometric, arithmetic, speed_arithmetic, spatial_mean"
        )

    if method == "spatial_mean":
        return _build_spatial_mean_cost(prim)

    groups: dict[tuple[int, int], list[int]] = defaultdict(list)
    for idx, (i, j) in enumerate(zip(prim.edge_i, prim.edge_j)):
        groups[(int(i), int(j))].append(idx)

    tau_obs = np.asarray(prim.tau_obs_min, dtype=float)
    ell_km = np.asarray(prim.ell_kl_km, dtype=float)
    tau_sym = tau_obs.copy()
    edge_pair_abs_log_ratio = np.zeros(prim.n_edges)
    edge_signed_log_component = np.zeros(prim.n_edges)
    edge_has_reverse = np.zeros(prim.n_edges, dtype=bool)
    edge_pair_id = np.array([""] * prim.n_edges, dtype=object)
    rows = []
    visited: set[tuple[int, int]] = set()

    for (i, j), idx_ij in groups.items():
        if (i, j) in visited:
            continue
        idx_ji = groups.get((j, i), [])
        visited.add((i, j))
        visited.add((j, i))
        pair_id = f"{min(i, j)}_{max(i, j)}"
        if not idx_ji:
            for idx in idx_ij:
                edge_pair_id[idx] = pair_id
            continue

        tau_ij = float(np.mean(tau_obs[idx_ij]))
        tau_ji = float(np.mean(tau_obs[idx_ji]))
        ell_ij = float(np.mean(ell_km[idx_ij]))
        ell_ji = float(np.mean(ell_km[idx_ji]))
        if method == "geometric":
            tau_pair = float(np.sqrt(tau_ij * tau_ji))
        elif method == "arithmetic":
            tau_pair = float(0.5 * (tau_ij + tau_ji))
        else:
            v_ij = ell_ij / max(tau_ij, EPS)
            v_ji = ell_ji / max(tau_ji, EPS)
            v_sym = 0.5 * (v_ij + v_ji)
            tau_pair_ij = ell_ij / max(v_sym, EPS)
            tau_pair_ji = ell_ji / max(v_sym, EPS)
            tau_pair = float(0.5 * (tau_pair_ij + tau_pair_ji))
        abs_log_ratio = float(abs(np.log(tau_ij / tau_ji)))
        signed_ij = float(np.log(tau_ij / tau_pair))
        signed_ji = float(np.log(tau_ji / tau_pair))

        for idx in idx_ij:
            if method == "speed_arithmetic":
                v_ij = ell_km[idx] / max(tau_obs[idx], EPS)
                v_ji = ell_ji / max(tau_ji, EPS)
                v_sym = 0.5 * (v_ij + v_ji)
                tau_sym[idx] = ell_km[idx] / max(v_sym, EPS)
            else:
                tau_sym[idx] = tau_pair
            edge_pair_abs_log_ratio[idx] = abs_log_ratio
            edge_signed_log_component[idx] = signed_ij
            edge_has_reverse[idx] = True
            edge_pair_id[idx] = pair_id
        for idx in idx_ji:
            if method == "speed_arithmetic":
                v_ij = ell_ij / max(tau_ij, EPS)
                v_ji = ell_km[idx] / max(tau_obs[idx], EPS)
                v_sym = 0.5 * (v_ij + v_ji)
                tau_sym[idx] = ell_km[idx] / max(v_sym, EPS)
            else:
                tau_sym[idx] = tau_pair
            edge_pair_abs_log_ratio[idx] = abs_log_ratio
            edge_signed_log_component[idx] = signed_ji
            edge_has_reverse[idx] = True
            edge_pair_id[idx] = pair_id

        rows.append({
            "pair_id": pair_id,
            "node_a": min(i, j),
            "node_b": max(i, j),
            "tau_ab_mean": tau_ij if i < j else tau_ji,
            "tau_ba_mean": tau_ji if i < j else tau_ij,
            "tau_sym": tau_pair,
            "abs_log_tau_ratio": abs_log_ratio,
            "tau_ratio": float(np.exp(abs_log_ratio)),
            "n_edges_ab": len(idx_ij) if i < j else len(idx_ji),
            "n_edges_ba": len(idx_ji) if i < j else len(idx_ij),
        })

    pair_df = pd.DataFrame(rows).sort_values("abs_log_tau_ratio", ascending=False)
    edge_df = pd.DataFrame({
        "edge_idx": np.arange(prim.n_edges),
        "edge_i": prim.edge_i,
        "edge_j": prim.edge_j,
        "pair_id": edge_pair_id,
        "has_reverse": edge_has_reverse,
        "tau_obs_min": tau_obs,
        "tau_sym_min": tau_sym,
        "signed_log_asym_component": edge_signed_log_component,
        "pair_abs_log_tau_ratio": edge_pair_abs_log_ratio,
        "pair_tau_ratio": np.exp(edge_pair_abs_log_ratio),
        "log_tau_sym_over_obs": np.log(np.clip(tau_sym, EPS, None) / np.clip(tau_obs, EPS, None)),
    })
    pair_df.attrs["edge_df"] = edge_df
    return tau_sym, pair_df


def tau_from_asymmetry_intensity(tau_sym: np.ndarray, tau_obs: np.ndarray, intensity: float) -> np.ndarray:
    """Interpolate from symmetric costs (0) to observed directed costs (1) in log space."""
    return np.exp(np.log(np.clip(tau_sym, EPS, None)) +
                  intensity * (np.log(np.clip(tau_obs, EPS, None)) - np.log(np.clip(tau_sym, EPS, None))))


def tau_from_asymmetry_threshold(
    tau_sym: np.ndarray,
    tau_obs: np.ndarray,
    pair_abs_log_ratio: np.ndarray,
    threshold_ratio: float,
) -> np.ndarray:
    """Keep directed asymmetry only for reciprocal pairs above a ratio threshold."""
    threshold_log = float(np.log(threshold_ratio))
    keep = pair_abs_log_ratio >= threshold_log
    return np.where(keep, tau_obs, tau_sym)


def solve_static_equilibrium(
    prim: Primitives,
    tau_min_kl: np.ndarray,
    ubar_theta: np.ndarray,
    abar_theta: np.ndarray,
    chi: float,
    params: Parameters,
    kappa: float,
    label: str = "scenario",
    max_iter: int = 1000,
    tol: float = 1e-9,
    initial_l_R: np.ndarray | None = None,
    initial_l_F: np.ndarray | None = None,
) -> StaticEquilibriumResult:
    """Solve residence and workplace shares for a fixed directed cost surface."""
    theta = params.theta
    alpha = params.alpha
    beta = params.beta
    tau_inv = build_tau_inv_theta(prim, tau_min_kl, kappa=kappa)
    N = prim.n_nodes
    if initial_l_R is None:
        l_R = np.full(N, 1.0 / N)
    else:
        l_R = np.clip(np.asarray(initial_l_R, dtype=float), EPS, None)
        l_R = l_R / l_R.sum()
    if initial_l_F is None:
        l_F = np.full(N, 1.0 / N)
    else:
        l_F = np.clip(np.asarray(initial_l_F, dtype=float), EPS, None)
        l_F = l_F / l_F.sum()

    converged = False
    last_delta = np.inf
    for it in range(1, max_iter + 1):
        rhs_R = chi * ubar_theta * (
            tau_inv @ (abar_theta * (l_F ** (alpha * theta)))
        )
        l_R_new = np.clip(rhs_R, EPS, None) ** (1.0 / (1.0 - beta * theta))
        l_R_new = l_R_new / l_R_new.sum()

        rhs_F = chi * abar_theta * (
            tau_inv.T @ (ubar_theta * (l_R_new ** (beta * theta)))
        )
        l_F_new = np.clip(rhs_F, EPS, None) ** (1.0 / (1.0 - alpha * theta))
        l_F_new = l_F_new / l_F_new.sum()

        last_delta = max(
            float(np.max(np.abs(np.log(np.clip(l_R_new, EPS, None)) - np.log(np.clip(l_R, EPS, None))))),
            float(np.max(np.abs(np.log(np.clip(l_F_new, EPS, None)) - np.log(np.clip(l_F, EPS, None))))),
        )
        l_R, l_F = l_R_new, l_F_new
        if last_delta < tol:
            converged = True
            break

    Pi, Pj = market_access(tau_inv, l_R, l_F, prim.Lbar, theta)
    Wbar = welfare(ubar_theta, abar_theta, tau_inv, l_R, l_F, prim.Lbar, params)
    return StaticEquilibriumResult(
        label=label,
        tau_min_kl=np.asarray(tau_min_kl, dtype=float),
        tau_ij_inv_theta=tau_inv,
        l_R=l_R,
        l_F=l_F,
        Pi_i=Pi,
        P_j=Pj,
        Wbar=float(Wbar),
        n_iter=it,
        converged=converged,
        max_log_change=float(last_delta),
        kappa=float(kappa),
    )


def market_access(
    tau_inv_theta: np.ndarray,
    l_R: np.ndarray,
    l_F: np.ndarray,
    Lbar: float,
    theta: float,
    max_iter: int = 500,
    tol: float = 1e-10,
) -> tuple[np.ndarray, np.ndarray]:
    """Solve resident and firm market-access indices."""
    L_R = l_R * Lbar
    L_F = l_F * Lbar
    N = len(l_R)
    Pi = np.ones(N)
    Pj = np.ones(N)
    for _ in range(max_iter):
        Pi_inv_theta = tau_inv_theta @ (L_F * (Pj ** theta))
        Pi_new = np.clip(Pi_inv_theta, EPS, None) ** (-1.0 / theta)
        Pj_inv_theta = tau_inv_theta.T @ (L_R * (Pi_new ** theta))
        Pj_new = np.clip(Pj_inv_theta, EPS, None) ** (-1.0 / theta)
        delta = max(
            float(np.max(np.abs(np.log(np.clip(Pi_new, EPS, None) / np.clip(Pi, EPS, None))))),
            float(np.max(np.abs(np.log(np.clip(Pj_new, EPS, None) / np.clip(Pj, EPS, None))))),
        )
        Pi, Pj = Pi_new, Pj_new
        if delta < tol:
            break
    return Pi, Pj


def welfare(
    ubar_theta: np.ndarray,
    abar_theta: np.ndarray,
    tau_inv_theta: np.ndarray,
    l_R: np.ndarray,
    l_F: np.ndarray,
    Lbar: float,
    params: Parameters,
) -> float:
    """Gauge-free aggregate welfare from the primitive u-w Frechet denominator."""
    theta = params.theta
    L_R = np.clip(l_R * Lbar, EPS, None)
    L_F = np.clip(l_F * Lbar, EPS, None)
    u_theta = ubar_theta * (L_R ** (params.beta * theta))
    w_theta = abar_theta * (L_F ** (params.alpha * theta))
    Wbar_theta = float(np.sum(np.asarray(tau_inv_theta, dtype=float) * u_theta[:, None] * w_theta[None, :]))
    return Wbar_theta ** (1.0 / theta) if Wbar_theta > 0 else np.nan


def intrazonal_cost_minutes(tau_min_kl: np.ndarray) -> float:
    """Proxy same-cell commute time: half the median positive directed edge time."""
    positive = np.asarray(tau_min_kl, dtype=float)
    positive = positive[positive > EPS]
    return 0.5 * float(np.median(positive)) if positive.size else 1.0


def _workplace_choice_weights(
    result: StaticEquilibriumResult,
    abar_theta: np.ndarray,
    params: Parameters,
    intra_spec: str,
    intra_cost_scale: float = 1.0,
    accessibility: np.ndarray | None = None,
) -> np.ndarray:
    """Unnormalised workplace-choice weights for each origin-destination pair."""
    theta = params.theta
    n = len(result.l_R)
    dest_factor = abar_theta * (result.l_F ** (params.alpha * theta))
    if accessibility is not None:
        weights = np.asarray(accessibility, dtype=float) * dest_factor[None, :]
    else:
        weights = result.tau_ij_inv_theta * dest_factor[None, :]

    if intra_spec == "baseline":
        return weights

    if intra_spec == "exclude_diag":
        out = weights.copy()
        np.fill_diagonal(out, 0.0)
        return out

    if intra_spec == "intrazonal_proxy":
        out = weights.copy()
        intra_inv_theta = intrazonal_cost_minutes(result.tau_min_kl) ** (-theta)
        np.fill_diagonal(out, intra_inv_theta * dest_factor)
        return out

    if intra_spec in {"exclude_diag_plus_proxy", "exclude_diag_plus_proxy_calibrated"}:
        out = weights.copy()
        np.fill_diagonal(out, 0.0)
        intra_inv_theta = (intrazonal_cost_minutes(result.tau_min_kl) * intra_cost_scale) ** (-theta)
        out[np.arange(n), np.arange(n)] = intra_inv_theta * dest_factor
        return out

    raise ValueError(f"unknown intra_spec: {intra_spec}")


def calibrate_intrazonal_cost_scale(
    prim: Primitives,
    result: StaticEquilibriumResult,
    abar_theta: np.ndarray,
    params: Parameters,
    target_intra_share: float | None = None,
    accessibility: np.ndarray | None = None,
) -> float:
    """Choose intra-cost scale so aggregate predicted intra share matches target."""
    if target_intra_share is None:
        positive = prim.L_ij_obs > EPS
        on_diag = np.eye(prim.n_nodes, dtype=bool)
        target_intra_share = float(
            prim.L_ij_obs[on_diag].sum() / prim.L_ij_obs[positive].sum()
        )

    def predicted_intra_share(scale: float) -> float:
        od = predicted_od_shares(
            result, abar_theta, params,
            intra_spec="exclude_diag_plus_proxy",
            intra_cost_scale=scale,
            accessibility=accessibility,
        )
        on_diag = np.eye(prim.n_nodes, dtype=bool)
        positive = od > EPS
        if not positive.any():
            return 0.0
        return float(od[on_diag].sum() / od[positive].sum())

    lo, hi = 1e-4, 10.0
    for _ in range(80):
        mid = np.sqrt(lo * hi)
        if predicted_intra_share(mid) >= target_intra_share:
            lo = mid
        else:
            hi = mid
    return float(np.sqrt(lo * hi))


def predicted_od_shares(
    result: StaticEquilibriumResult,
    abar_theta: np.ndarray,
    params: Parameters,
    intra_spec: str = "baseline",
    intra_cost_scale: float = 1.0,
    accessibility: np.ndarray | None = None,
) -> np.ndarray:
    """Allocate workplace choices conditional on residence shares.

    ``intra_spec`` (new OD-track specs; default ``baseline`` preserves Stage 1):
    - ``baseline``: use full ``T`` including Leontief diagonal.
    - ``exclude_diag``: zero ``i->i`` weight; interzonal shares renormalize.
    - ``intrazonal_proxy``: replace ``T_ii`` with proxy intra cost in weights.
    - ``exclude_diag_plus_proxy``: separate intra term with proxy cost (spatial_equilibrium convention).
    - ``exclude_diag_plus_proxy_calibrated``: same, with ``intra_cost_scale`` calibrated to observed aggregate intra share.
    """
    weights = _workplace_choice_weights(
        result, abar_theta, params, intra_spec, intra_cost_scale=intra_cost_scale,
        accessibility=accessibility,
    )
    row_sum = np.clip(weights.sum(axis=1), EPS, None)
    prob = weights / row_sum[:, None]
    return result.l_R[:, None] * prob


def compare_od_intra_specs(
    prim: Primitives,
    result: StaticEquilibriumResult,
    abar_theta: np.ndarray,
    params: Parameters,
) -> dict:
    """Compare baseline and P1 intra-zonal OD prediction specs."""
    specs: list[tuple[str, dict]] = [
        ("baseline", {"intra_spec": "baseline"}),
        ("exclude_diag", {"intra_spec": "exclude_diag"}),
        ("intrazonal_proxy", {"intra_spec": "intrazonal_proxy"}),
        ("exclude_diag_plus_proxy", {"intra_spec": "exclude_diag_plus_proxy"}),
    ]
    calibrated_scale = calibrate_intrazonal_cost_scale(prim, result, abar_theta, params)
    specs.append((
        "exclude_diag_plus_proxy_calibrated",
        {
            "intra_spec": "exclude_diag_plus_proxy",
            "intra_cost_scale": calibrated_scale,
        },
    ))

    baseline_fit: dict | None = None
    rows: list[dict] = []
    for label, kw in specs:
        od = predicted_od_shares(result, abar_theta, params, **kw)
        fit = od_fit_to_observed_summary(prim, od, top_k_errors=5)
        fit_row = {
            "spec": label,
            "intra_spec": kw["intra_spec"],
            "intra_cost_scale": float(kw.get("intra_cost_scale", 1.0)),
            "intrazonal_cost_minutes_base": intrazonal_cost_minutes(result.tau_min_kl),
            "global_l1_pct": fit["global_l1_pct"],
            "off_diagonal_l1_pct": fit["off_diagonal_l1_pct"],
            "intrazonal_l1_pct": fit["intrazonal_l1_pct"],
            "intrazonal_share_pred_pct": fit["intrazonal_share_pred_pct"],
            "weighted_log_corr": fit["weighted_log_corr"],
        }
        if label == "baseline":
            baseline_fit = fit_row
        elif baseline_fit is not None:
            fit_row["global_l1_pct_change_vs_baseline"] = (
                fit_row["global_l1_pct"] - baseline_fit["global_l1_pct"]
            )
            fit_row["off_diagonal_l1_pct_change_vs_baseline"] = (
                fit_row["off_diagonal_l1_pct"] - baseline_fit["off_diagonal_l1_pct"]
            )
            fit_row["intrazonal_share_pred_pct_change_vs_baseline"] = (
                fit_row["intrazonal_share_pred_pct"] - baseline_fit["intrazonal_share_pred_pct"]
            )
        rows.append(fit_row)

    return {
        "track": "od_fit_improvement_p1_intra_zonal",
        "note": (
            "New specs affect implied OD only; Stage 1 equilibrium and re-invert fundamentals unchanged."
        ),
        "observed_intrazonal_share_pct": 100.0 * float(
            prim.L_ij_obs[np.eye(prim.n_nodes, dtype=bool)].sum()
            / prim.L_ij_obs[prim.L_ij_obs > EPS].sum()
        ),
        "specs": rows,
        "recommended_next": "P2 theta / SMM moments; off-diagonal L1 remains ~50%+ under structural P1",
    }


def _od_l1_pct(pred: np.ndarray, obs: np.ndarray, mask: np.ndarray) -> float:
    """Half L1 distance between pred and obs, as percent of obs mass in mask."""
    m = mask & (obs > EPS)
    if not m.any():
        return float("nan")
    return 100.0 * 0.5 * float(np.abs(pred[m] - obs[m]).sum() / obs[m].sum())


def _weighted_log_corr(pred: np.ndarray, obs: np.ndarray, mask: np.ndarray,
                       weighted: bool = True) -> float:
    """Pearson correlation of log flows on masked positive cells."""
    m = mask & (obs > EPS) & (pred > EPS)
    if int(m.sum()) < 2:
        return float("nan")
    lp = np.log(pred[m])
    lo = np.log(obs[m])
    if weighted:
        w = obs[m]
        w = w / w.sum()
        lp_mean = float(np.sum(w * lp))
        lo_mean = float(np.sum(w * lo))
        cov = float(np.sum(w * (lp - lp_mean) * (lo - lo_mean)))
        var_p = float(np.sum(w * (lp - lp_mean) ** 2))
        var_o = float(np.sum(w * (lo - lo_mean) ** 2))
    else:
        lp_mean = float(lp.mean())
        lo_mean = float(lo.mean())
        cov = float(np.mean((lp - lp_mean) * (lo - lo_mean)))
        var_p = float(np.var(lp))
        var_o = float(np.var(lo))
    denom = np.sqrt(var_p * var_o)
    return float(cov / denom) if denom > EPS else float("nan")


def od_fit_to_observed_summary(
    prim: Primitives,
    od_pred: np.ndarray,
    od_pred_sym: np.ndarray | None = None,
    top_k_errors: int = 15,
) -> dict:
    """Compare model-implied OD to observed bilateral flows.

    ``od_pred`` and optional ``od_pred_sym`` are population shares (sum to 1 over
    the full matrix). Observed flows come from ``prim.L_ij_obs``.
    """
    n = prim.n_nodes
    L_obs = np.asarray(prim.L_ij_obs, dtype=float)
    L_pred = np.asarray(od_pred, dtype=float) * prim.Lbar
    positive = L_obs > EPS
    off_diag = ~np.eye(n, dtype=bool)
    on_diag = np.eye(n, dtype=bool)
    pos_mass = float(L_obs[positive].sum())

    origin_l1: list[float] = []
    for i in range(n):
        row_obs = L_obs[i]
        tot = float(row_obs.sum())
        if tot <= EPS:
            continue
        origin_l1.append(0.5 * float(np.abs(L_pred[i] - row_obs).sum() / tot))

    origin_arr = np.asarray(origin_l1, dtype=float) if origin_l1 else np.array([], dtype=float)

    # Top flows by observed mass (top 1% of positive cells).
    pos_idx = np.flatnonzero(positive)
    n_top = max(1, int(np.ceil(0.01 * pos_idx.size)))
    top_flow_idx = pos_idx[np.argsort(-L_obs.ravel()[pos_idx])[:n_top]]
    top_flow_mask = np.zeros_like(positive, dtype=bool)
    top_flow_mask.ravel()[top_flow_idx] = True

    pos_off_idx = np.flatnonzero(positive & off_diag)
    n_top_off = max(1, int(np.ceil(0.01 * pos_off_idx.size)))
    top_off_idx = pos_off_idx[np.argsort(-L_obs.ravel()[pos_off_idx])[:n_top_off]]
    top_off_mask = np.zeros_like(positive, dtype=bool)
    top_off_mask.ravel()[top_off_idx] = True

    abs_err = np.zeros_like(L_obs)
    abs_err[positive] = np.abs(L_pred[positive] - L_obs[positive])
    k = min(top_k_errors, int(positive.sum()))
    flat = abs_err.ravel()
    idx = np.argpartition(flat, -k)[-k:]
    idx = idx[np.argsort(-flat[idx])]
    origins, dests = np.unravel_index(idx, L_obs.shape)
    top_errors = [
        {
            "origin_idx": int(origins[t]),
            "destination_idx": int(dests[t]),
            "origin_grid_id": str(prim.node_grid_ids[origins[t]]),
            "destination_grid_id": str(prim.node_grid_ids[dests[t]]),
            "L_obs": float(L_obs[origins[t], dests[t]]),
            "L_pred": float(L_pred[origins[t], dests[t]]),
            "abs_error": float(abs_err[origins[t], dests[t]]),
            "is_intrazonal": bool(origins[t] == dests[t]),
        }
        for t in range(k)
    ]

    out: dict = {
        "n_nodes": n,
        "n_positive_od_cells": int(positive.sum()),
        "total_obs_commuters": pos_mass,
        "global_l1_pct": _od_l1_pct(L_pred, L_obs, positive),
        "off_diagonal_l1_pct": _od_l1_pct(L_pred, L_obs, positive & off_diag),
        "intrazonal_l1_pct": _od_l1_pct(L_pred, L_obs, positive & on_diag),
        "intrazonal_share_obs_pct": 100.0 * float(L_obs[on_diag].sum() / pos_mass) if pos_mass > 0 else float("nan"),
        "intrazonal_share_pred_pct": 100.0 * float(L_pred[on_diag].sum() / max(L_pred[positive].sum(), EPS)),
        "origin_conditional_l1_median_pct": 100.0 * float(np.median(origin_arr)) if origin_arr.size else float("nan"),
        "origin_conditional_l1_p90_pct": 100.0 * float(np.quantile(origin_arr, 0.90)) if origin_arr.size else float("nan"),
        "n_origins_with_obs_outflow": int(origin_arr.size),
        "weighted_log_corr": _weighted_log_corr(L_pred, L_obs, positive, weighted=True),
        "unweighted_log_corr": _weighted_log_corr(L_pred, L_obs, positive, weighted=False),
        "top_1pct_cells_l1_pct": _od_l1_pct(L_pred, L_obs, top_flow_mask),
        "top_1pct_cells_n": int(n_top),
        "off_diagonal_top_1pct_cells_l1_pct": _od_l1_pct(L_pred, L_obs, top_off_mask),
        "off_diagonal_top_1pct_cells_n": int(n_top_off),
        "top_1pct_cells_weighted_log_corr": _weighted_log_corr(L_pred, L_obs, top_flow_mask, weighted=True),
        "top_abs_errors": top_errors,
    }

    if od_pred_sym is not None:
        L_pred_sym = np.asarray(od_pred_sym, dtype=float) * prim.Lbar
        share_dir = np.asarray(od_pred, dtype=float)
        share_sym = np.asarray(od_pred_sym, dtype=float)
        out["directed_vs_symmetric_od_l1_pct"] = 100.0 * 0.5 * float(
            np.abs(share_sym - share_dir).sum()
        )
        out["symmetric_vs_observed_global_l1_pct"] = _od_l1_pct(L_pred_sym, L_obs, positive)
        out["symmetric_vs_observed_off_diagonal_l1_pct"] = _od_l1_pct(
            L_pred_sym, L_obs, positive & off_diag,
        )

    return out


def weighted_bilateral_cost(
    result: StaticEquilibriumResult,
    od_shares: np.ndarray,
    theta: float,
) -> float:
    """OD-weighted average bilateral route-cost index in minutes."""
    tau_ij_min = np.clip(result.tau_ij_inv_theta, EPS, None) ** (-1.0 / theta)
    return float(np.sum(od_shares * tau_ij_min) / np.clip(od_shares.sum(), EPS, None))


def fundamentals_comparison_summary(
    fund_base,
    fund_scenario,
    valid: np.ndarray,
) -> dict:
    """Compare inverted fundamentals across two cost surfaces."""
    valid = np.asarray(valid, dtype=bool)
    log_u = np.log(np.clip(fund_scenario.ubar_theta[valid], EPS, None)) - np.log(
        np.clip(fund_base.ubar_theta[valid], EPS, None)
    )
    log_a = np.log(np.clip(fund_scenario.abar_theta[valid], EPS, None)) - np.log(
        np.clip(fund_base.abar_theta[valid], EPS, None)
    )
    u_base = fund_base.ubar_theta[valid]
    u_scen = fund_scenario.ubar_theta[valid]
    a_base = fund_base.abar_theta[valid]
    a_scen = fund_scenario.abar_theta[valid]
    return {
        "residence_fundamental_l1_pct_valid": 100.0 * 0.5 * float(np.sum(np.abs(u_scen - u_base))),
        "workplace_fundamental_l1_pct_valid": 100.0 * 0.5 * float(np.sum(np.abs(a_scen - a_base))),
        "mean_abs_log_residence_fundamental_change_valid": float(np.mean(np.abs(log_u))),
        "mean_abs_log_workplace_fundamental_change_valid": float(np.mean(np.abs(log_a))),
        "p95_abs_log_residence_fundamental_change_valid": float(np.quantile(np.abs(log_u), 0.95)),
        "p95_abs_log_workplace_fundamental_change_valid": float(np.quantile(np.abs(log_a), 0.95)),
        "chi_base": float(fund_base.chi),
        "chi_scenario": float(fund_scenario.chi),
        "chi_pct_change": 100.0 * (fund_scenario.chi / fund_base.chi - 1.0),
    }


def comparison_summary(
    base: StaticEquilibriumResult,
    scenario: StaticEquilibriumResult,
    prim: Primitives,
    params: Parameters,
    abar_theta: np.ndarray | None = None,
    abar_theta_scenario: np.ndarray | None = None,
) -> dict:
    """Summarize a scenario relative to a base equilibrium."""
    valid_R = (base.l_R > EPS) & (scenario.l_R > EPS)
    valid_F = (base.l_F > EPS) & (scenario.l_F > EPS)
    out = {
        "base_label": base.label,
        "scenario_label": scenario.label,
        "Wbar_base": base.Wbar,
        "Wbar_scenario": scenario.Wbar,
        "Wbar_pct_change": 100.0 * (scenario.Wbar / base.Wbar - 1.0),
        "residence_dissimilarity_pct": 100.0 * 0.5 * float(np.sum(np.abs(scenario.l_R - base.l_R))),
        "workplace_dissimilarity_pct": 100.0 * 0.5 * float(np.sum(np.abs(scenario.l_F - base.l_F))),
        "max_abs_log_residence_change": float(np.max(np.abs(np.log(scenario.l_R[valid_R]) - np.log(base.l_R[valid_R])))),
        "p95_abs_log_residence_change": float(np.quantile(np.abs(np.log(scenario.l_R[valid_R]) - np.log(base.l_R[valid_R])), 0.95)),
        "max_abs_log_workplace_change": float(np.max(np.abs(np.log(scenario.l_F[valid_F]) - np.log(base.l_F[valid_F])))),
        "p95_abs_log_workplace_change": float(np.quantile(np.abs(np.log(scenario.l_F[valid_F]) - np.log(base.l_F[valid_F])), 0.95)),
        "mean_edge_tau_pct_change": 100.0 * float(np.mean(scenario.tau_min_kl / np.clip(base.tau_min_kl, EPS, None) - 1.0)),
        "p95_abs_edge_log_tau_change": float(np.quantile(np.abs(np.log(scenario.tau_min_kl / np.clip(base.tau_min_kl, EPS, None))), 0.95)),
        "base_converged": bool(base.converged),
        "scenario_converged": bool(scenario.converged),
        "base_iter": int(base.n_iter),
        "scenario_iter": int(scenario.n_iter),
        "kappa": float(scenario.kappa),
    }
    if abar_theta is not None:
        abar_scen = abar_theta if abar_theta_scenario is None else abar_theta_scenario
        od_base = predicted_od_shares(base, abar_theta, params)
        od_scen = predicted_od_shares(scenario, abar_scen, params)
        out["od_dissimilarity_pct"] = 100.0 * 0.5 * float(np.sum(np.abs(od_scen - od_base)))
        out["base_od_weighted_bilateral_cost_min"] = weighted_bilateral_cost(base, od_base, params.theta)
        out["scenario_od_weighted_bilateral_cost_min"] = weighted_bilateral_cost(scenario, od_scen, params.theta)
        out["od_weighted_bilateral_cost_pct_change"] = (
            100.0 * (out["scenario_od_weighted_bilateral_cost_min"] /
                     out["base_od_weighted_bilateral_cost_min"] - 1.0)
        )
    return out


def node_comparison_frame(
    prim: Primitives,
    base: StaticEquilibriumResult,
    scenario: StaticEquilibriumResult,
) -> pd.DataFrame:
    """Node-level allocation and market-access comparison."""
    return pd.DataFrame({
        "node_idx": np.arange(prim.n_nodes),
        "grid_id": prim.node_grid_ids,
        "L_R_obs": prim.L_R_obs,
        "L_F_obs": prim.L_F_obs,
        "l_R_base": base.l_R,
        "l_R_scenario": scenario.l_R,
        "l_F_base": base.l_F,
        "l_F_scenario": scenario.l_F,
        "log_l_R_scenario_over_base": np.log(np.clip(scenario.l_R, EPS, None) / np.clip(base.l_R, EPS, None)),
        "log_l_F_scenario_over_base": np.log(np.clip(scenario.l_F, EPS, None) / np.clip(base.l_F, EPS, None)),
        "Pi_base": base.Pi_i,
        "Pi_scenario": scenario.Pi_i,
        "P_base": base.P_j,
        "P_scenario": scenario.P_j,
        "log_Pi_inv_theta_scenario_over_base": np.log(
            np.clip(scenario.Pi_i, EPS, None) ** (-1.0) /
            np.clip(base.Pi_i, EPS, None) ** (-1.0)
        ),
        "log_P_inv_theta_scenario_over_base": np.log(
            np.clip(scenario.P_j, EPS, None) ** (-1.0) /
            np.clip(base.P_j, EPS, None) ** (-1.0)
        ),
    })


def top_od_changes_frame(
    prim: Primitives,
    base: StaticEquilibriumResult,
    scenario: StaticEquilibriumResult,
    abar_theta: np.ndarray,
    params: Parameters,
    top_k: int = 500,
) -> pd.DataFrame:
    """Top predicted OD-flow changes by absolute commuter count."""
    od_base = predicted_od_shares(base, abar_theta, params) * prim.Lbar
    od_scen = predicted_od_shares(scenario, abar_theta, params) * prim.Lbar
    delta = od_scen - od_base
    flat = np.abs(delta).ravel()
    k = min(top_k, flat.size)
    idx = np.argpartition(flat, -k)[-k:]
    idx = idx[np.argsort(flat[idx])[::-1]]
    origins, dests = np.unravel_index(idx, delta.shape)
    return pd.DataFrame({
        "origin_idx": origins,
        "destination_idx": dests,
        "origin_grid_id": prim.node_grid_ids[origins],
        "destination_grid_id": prim.node_grid_ids[dests],
        "flow_base": od_base[origins, dests],
        "flow_scenario": od_scen[origins, dests],
        "flow_change": delta[origins, dests],
        "abs_flow_change": np.abs(delta[origins, dests]),
        "log_flow_scenario_over_base": np.log(
            np.clip(od_scen[origins, dests], EPS, None) /
            np.clip(od_base[origins, dests], EPS, None)
        ),
    })


def write_json(path: Path, obj: dict | list) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(obj, indent=2, default=float))

