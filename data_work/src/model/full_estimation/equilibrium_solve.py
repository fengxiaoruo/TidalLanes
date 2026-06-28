"""
Step 5 — Level-form equilibrium solver
======================================

Solve Definition 4.1 of TidalLanes_Model_v4.tex in **levels** (not
exact-hat). Given fundamentals (ubar_i, Abar_j), network primitives
(ell_kl, v0_kl, n_kl), and parameters (theta, alpha, beta, lambda), find

    (t_kl, tau_ij, Xi_kl, l_R, l_F, Pi_i, P_j, Wbar)

that jointly satisfy

    (E1)  tau_ij^{-theta}  = [(I - A)^{-1}]_{ij},   A_{kl} = t_{kl}^{-theta}
    (E_R) (l_i^R)^{1-beta*theta}
           = chi * sum_j tau_ij^{-theta} * ubar_i^theta * Abar_j^theta
                       * (l_j^F)^{alpha*theta}
    (E_F) (l_j^F)^{1-alpha*theta}
           = chi * sum_i tau_ij^{-theta} * ubar_i^theta * Abar_j^theta
                       * (l_i^R)^{beta*theta}
    (E4)  Pi_i^{-theta}  = sum_j tau_ij^{-theta} L_j^F P_j^theta
          P_j^{-theta}   = sum_i tau_ij^{-theta} L_i^R Pi_i^theta
    (E5)  Xi_{kl}        = t_{kl}^{-theta} * P_k^{-theta} * Pi_l^{-theta}
    (E6)  t_{kl}         = (tau_ff_kl)^{1/theta} * (Xi_{kl}/n_{kl})^{lambda/theta}

Iceberg form (v4 §3.3):
    t_kl       = (tau_kl_min)^{1/theta}
    A_kl       = t_kl^{-theta}     = 1 / tau_kl_min
    tau_kl_min  = tau_ff_kl * (Xi_kl / n_kl)^{lambda}       (level update)

Outer loop iterates t_kl until convergence; inside each outer iteration
an inner loop solves (l_R, l_F) given current tau_ij.

We avoid exact-hat algebra — the solver re-runs from level fundamentals
for each (baseline / CF1 / CF2 / CF3) scenario.

Numerical notes:
    * (I - A)^{-1} is computed by scipy.linalg.solve on a dense N x N
      system. For N ~ 2700 this takes a few seconds per outer iter.
    * We damp t_kl updates in log-space with `damping ∈ (0,1]`.
    * Convergence: max |log t_kl_new - log t_kl_old| < tol.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
import json
from pathlib import Path

import numpy as np
from scipy import linalg as sla

from .parameters import Parameters
from .build_primitives import Primitives


EPS = 1e-12


@dataclass
class EquilibriumResult:
    tau_min_kl: np.ndarray          # observed-form directed time, minutes (M,)
    t_kl_iceberg: np.ndarray        # iceberg cost (M,)
    Xi_kl: np.ndarray               # directed link traffic (M,)
    tau_ij_inv_theta: np.ndarray    # tau_ij^{-theta} (N,N)
    l_R: np.ndarray                 # residence share l_i^R (N,)
    l_F: np.ndarray                 # workplace share l_j^F (N,)
    Pi_i: np.ndarray                # market access Pi (N,)
    P_j: np.ndarray                 # market access P (N,)
    Wbar: float                     # aggregate welfare scale
    n_outer: int
    converged: bool
    max_log_change: float

    def write(self, out_dir: Path) -> None:
        out_dir = Path(out_dir); out_dir.mkdir(parents=True, exist_ok=True)
        np.savez_compressed(
            out_dir / "equilibrium_arrays.npz",
            tau_min_kl=self.tau_min_kl, t_kl_iceberg=self.t_kl_iceberg,
            Xi_kl=self.Xi_kl, l_R=self.l_R, l_F=self.l_F,
            Pi_i=self.Pi_i, P_j=self.P_j,
        )
        meta = {k: v for k, v in asdict(self).items()
                if not isinstance(v, np.ndarray)}
        (out_dir / "equilibrium_meta.json").write_text(json.dumps(meta, indent=2, default=float))


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _build_A_matrix(edge_i, edge_j, tau_min_kl, N, kappa: float = 1.0):
    """A_kl = (kappa * t_kl)^{-theta} = 1 / (kappa * tau_min_kl).

    `kappa` is a uniform iceberg-cost rescaling used to enforce the
    spectral-radius condition (v4 §3.2 (E7)). Counterfactual ratios are
    invariant to `kappa`; absolute levels of `tau_ij` and `Wbar` are not.
    """
    A = np.zeros((N, N), dtype=float)
    inv = 1.0 / np.clip(kappa * tau_min_kl, EPS, None)
    np.add.at(A, (edge_i, edge_j), inv)
    return A


def _pick_kappa(edge_i, edge_j, tau_min_kl, N, target_rowsum: float = 0.9) -> float:
    """Choose the smallest kappa so the raw row-sums of A are ≤ target.

    Row sum is sum_l 1/(kappa*tau_min_kl) for edges leaving k.
    Choosing kappa = max(1, max_rowsum_raw / target) bounds the row sum
    (a sufficient condition for spectral radius < 1).
    """
    raw = np.zeros(N)
    np.add.at(raw, edge_i, 1.0 / np.clip(tau_min_kl, EPS, None))
    max_rowsum = float(raw.max())
    if max_rowsum <= target_rowsum:
        return 1.0
    return max_rowsum / target_rowsum


def _compute_tau_inv_theta(A: np.ndarray) -> np.ndarray:
    """Solve (I - A) X = I  ==>  X = (I - A)^{-1}, returns tau^{-theta}."""
    N = A.shape[0]
    M = np.eye(N) - A
    try:
        X = sla.solve(M, np.eye(N), check_finite=False)
    except sla.LinAlgError as exc:
        raise RuntimeError(
            "(I - A) is singular — spectral-radius condition fails."
        ) from exc
    return np.clip(X, EPS, None)


def _solve_lR_lF_given_tau(
    tau_inv_theta: np.ndarray,
    ubar_theta: np.ndarray,
    abar_theta: np.ndarray,
    params: Parameters,
    chi: float = 1.0,
    max_iter: int = 400,
    tol: float = 1e-8,
) -> tuple[np.ndarray, np.ndarray, int]:
    """Solve (E_R)-(E_F) for share allocations (l_R, l_F).

    Alternating fixed-point in log space with population-sum
    renormalisation each step.  Returns shares summing to 1.
    """
    theta = params.theta; alpha = params.alpha; beta = params.beta
    N = tau_inv_theta.shape[0]
    l_R = np.full(N, 1.0 / N); l_F = np.full(N, 1.0 / N)

    for it in range(max_iter):
        # rhs_R[i] = chi * ubar^theta_i * sum_j tau_ij^{-theta} abar^theta_j (l_F)^{alpha*theta}
        rhs_R = chi * ubar_theta * (tau_inv_theta @ (abar_theta * (l_F ** (alpha * theta))))
        l_R_new = np.clip(rhs_R, EPS, None) ** (1.0 / (1.0 - beta * theta))
        l_R_new = l_R_new / l_R_new.sum()

        rhs_F = chi * abar_theta * (tau_inv_theta.T @ (ubar_theta * (l_R_new ** (beta * theta))))
        l_F_new = np.clip(rhs_F, EPS, None) ** (1.0 / (1.0 - alpha * theta))
        l_F_new = l_F_new / l_F_new.sum()

        delta = max(
            float(np.max(np.abs(np.log(np.clip(l_R_new, EPS, None)) - np.log(np.clip(l_R, EPS, None))))),
            float(np.max(np.abs(np.log(np.clip(l_F_new, EPS, None)) - np.log(np.clip(l_F, EPS, None))))),
        )
        l_R, l_F = l_R_new, l_F_new
        if delta < tol:
            return l_R, l_F, it + 1
    return l_R, l_F, max_iter


def _market_access(tau_inv_theta: np.ndarray, l_R: np.ndarray, l_F: np.ndarray,
                   Lbar: float, theta: float,
                   max_iter: int = 400, tol: float = 1e-9) -> tuple[np.ndarray, np.ndarray]:
    """Solve (E4) for (Pi_i, P_j) by alternating updates.

    Pi_i^{-theta} = sum_j tau_ij^{-theta} L_j^F P_j^{theta}
    P_j^{-theta}  = sum_i tau_ij^{-theta} L_i^R Pi_i^{theta}
    """
    L_R = l_R * Lbar; L_F = l_F * Lbar
    N = len(l_R)
    Pi = np.ones(N); Pj = np.ones(N)
    for _ in range(max_iter):
        Pi_inv_theta = tau_inv_theta @ (L_F * (Pj ** theta))
        Pi_new = np.clip(Pi_inv_theta, EPS, None) ** (-1.0 / theta)
        Pj_inv_theta = tau_inv_theta.T @ (L_R * (Pi_new ** theta))
        Pj_new = np.clip(Pj_inv_theta, EPS, None) ** (-1.0 / theta)
        delta = max(
            float(np.max(np.abs(np.log(Pi_new / np.clip(Pi, EPS, None))))),
            float(np.max(np.abs(np.log(Pj_new / np.clip(Pj, EPS, None))))),
        )
        Pi, Pj = Pi_new, Pj_new
        if delta < tol:
            break
    return Pi, Pj


# --------------------------------------------------------------------------- #
# Main outer loop
# --------------------------------------------------------------------------- #
def solve_equilibrium(
    prim: Primitives,
    ubar_theta: np.ndarray,
    abar_theta: np.ndarray,
    params: Parameters,
    n_kl_override: np.ndarray | None = None,
    tau_min_init: np.ndarray | None = None,
    damping: float = 0.5,
    max_outer: int = 60,
    tol: float = 1e-5,
    kappa: float | None = None,
    kappa_target_rowsum: float = 0.9,
) -> EquilibriumResult:
    """Outer fixed point in t_kl.

    Parameters
    ----------
    prim : Primitives from Step 1.
    ubar_theta, abar_theta : inverted fundamentals (Step 4).
    params : externally calibrated (theta, alpha, beta, lambda).
    n_kl_override : lane counterfactual (CF2/CF3); if None, uses prim.n_kl.
    tau_min_init : starting t_kl (in minutes); if None, uses tau_ff.
    damping : log-space update damping for tau_min_kl.
    """
    theta, lam = params.theta, params.lambda_
    N = prim.n_nodes
    n_kl = prim.n_kl if n_kl_override is None else np.asarray(n_kl_override, float)
    tau_ff = prim.tau_ff_min
    tau_min = tau_ff.copy() if tau_min_init is None else np.asarray(tau_min_init, float).copy()

    # spectral-radius safety rescaling (see _pick_kappa)
    if kappa is None:
        kappa = _pick_kappa(prim.edge_i, prim.edge_j, tau_min, N,
                              target_rowsum=kappa_target_rowsum)

    converged = False; last_delta = np.inf; it = 0
    for it in range(1, max_outer + 1):
        # (E1) — bilateral cost index from current t_kl
        A = _build_A_matrix(prim.edge_i, prim.edge_j, tau_min, N, kappa=kappa)
        tau_inv_theta = _compute_tau_inv_theta(A)

        # (E_R)/(E_F) — share allocations given fundamentals
        l_R, l_F, _ = _solve_lR_lF_given_tau(tau_inv_theta, ubar_theta, abar_theta, params)

        # (E4) — market access
        Pi, Pj = _market_access(tau_inv_theta, l_R, l_F, prim.Lbar, theta)

        # (E5) — directed link traffic
        t_kl_iceberg = tau_min ** (1.0 / theta)
        Xi_kl = (t_kl_iceberg ** (-theta)) * (Pj[prim.edge_i] ** (-theta)) * (Pi[prim.edge_j] ** (-theta))

        # (E6) — congestion update on tau_min (= ell/v_kl)
        # tau_min_new = tau_ff * (Xi_kl / n_kl)^{lambda}
        ratio = np.clip(Xi_kl / np.clip(n_kl, EPS, None), EPS, None)
        tau_min_new = tau_ff * ratio ** lam
        # damped log-update for stability
        log_old = np.log(np.clip(tau_min, EPS, None))
        log_new = np.log(np.clip(tau_min_new, EPS, None))
        tau_min_damped = np.exp(damping * log_new + (1 - damping) * log_old)

        last_delta = float(np.max(np.abs(np.log(tau_min_damped) - log_old)))
        tau_min = tau_min_damped
        if last_delta < tol:
            converged = True
            break

    # final pass with the converged tau to lock the recorded outputs
    A = _build_A_matrix(prim.edge_i, prim.edge_j, tau_min, N, kappa=kappa)
    tau_inv_theta = _compute_tau_inv_theta(A)
    l_R, l_F, _ = _solve_lR_lF_given_tau(tau_inv_theta, ubar_theta, abar_theta, params)
    Pi, Pj = _market_access(tau_inv_theta, l_R, l_F, prim.Lbar, theta)
    t_kl_iceberg = tau_min ** (1.0 / theta)
    Xi_kl = (t_kl_iceberg ** (-theta)) * (Pj[prim.edge_i] ** (-theta)) * (Pi[prim.edge_j] ** (-theta))

    # Welfare scale Wbar via eq:welfare_MA — Wbar^theta = sum_i u_i^theta Pi_i^{-theta}
    # u_i = ubar_i * (L_i^R)^{beta} = ubar_i * (l_R * Lbar)^{beta}; u_i^theta = ubar^theta * (l_R*Lbar)^{beta*theta}
    L_R = l_R * prim.Lbar
    u_theta = ubar_theta * (np.clip(L_R, EPS, None) ** (params.beta * theta))
    Pi_inv_theta = np.clip(Pi, EPS, None) ** (-theta)
    Wbar_theta = float(np.sum(u_theta * Pi_inv_theta))
    Wbar = Wbar_theta ** (1.0 / theta) if Wbar_theta > 0 else np.nan

    return EquilibriumResult(
        tau_min_kl=tau_min, t_kl_iceberg=t_kl_iceberg, Xi_kl=Xi_kl,
        tau_ij_inv_theta=tau_inv_theta, l_R=l_R, l_F=l_F,
        Pi_i=Pi, P_j=Pj, Wbar=float(Wbar),
        n_outer=it, converged=converged, max_log_change=last_delta,
    )
