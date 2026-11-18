"""
Step 4 — Invert baseline fundamentals (ubar_i^theta, Abar_j^theta)
==================================================================

v4.tex §6.4, eqs (invert_R)-(invert_F):

    (l_i^{R,obs})^{1-beta*theta}
       = chi * sum_j (tau_ij^{obs})^{-theta} ubar_i^theta abar_j^theta
                    (l_j^{F,obs})^{alpha*theta}
    (l_j^{F,obs})^{1-alpha*theta}
       = chi * sum_i (tau_ij^{obs})^{-theta} ubar_i^theta abar_j^theta
                    (l_i^{R,obs})^{beta*theta}

with the assumption (ass:obs_eq) that the observed allocation is an
equilibrium under primitives (ell_kl, v0_kl, n_kl) and parameters
(theta, alpha, beta, lambda).

Observed quantities
    l_R_obs = L_R_obs / Lbar,  l_F_obs = L_F_obs / Lbar
    A_kl_obs = (t_kl_obs)^{-theta} = 1 / tau_obs_min_kl    (§3.3)
    tau_ij_obs^{-theta} = [(I - A_obs)^{-1}]_{ij}

Algorithm — alternating updates with geometric-mean normalisation:
    v_i = ubar_i^theta,  w_j = abar_j^theta,
    define
        a_i = sum_j tau_ij^{-theta} (l_F_obs)^{alpha*theta} w_j
        b_j = sum_i tau_ij^{-theta} (l_R_obs)^{beta*theta}  v_i
    update v_i  = (l_R_obs)^{1-beta*theta}  / (chi * a_i)
           w_j  = (l_F_obs)^{1-alpha*theta} / (chi * b_j)
    Pin chi by enforcing geometric means prod(v) = prod(w) = 1 (v4 §3.4
    normalisation `prod_i ubar_i = prod_j Abar_j = 1`).  Numerically:
        re-scale v -> v / gmean(v),  w -> w / gmean(w),  let chi absorb
        any uniform shift so the two equations stay consistent.

Output: (ubar_theta, abar_theta, chi, residuals).
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
class InversionResult:
    ubar_theta: np.ndarray
    abar_theta: np.ndarray
    chi: float
    kappa: float                  # uniform iceberg-cost rescaling used
    tau_obs_inv_theta: np.ndarray
    n_iter: int
    converged: bool
    max_log_change: float
    resid_R: np.ndarray           # implied l_R_pred - l_R_obs in log
    resid_F: np.ndarray

    def write(self, out_dir: Path) -> None:
        out_dir = Path(out_dir); out_dir.mkdir(parents=True, exist_ok=True)
        np.savez_compressed(
            out_dir / "fundamentals_arrays.npz",
            ubar_theta=self.ubar_theta, abar_theta=self.abar_theta,
            resid_R=self.resid_R, resid_F=self.resid_F,
        )
        meta = {k: v for k, v in asdict(self).items()
                if not isinstance(v, np.ndarray)}
        (out_dir / "fundamentals_meta.json").write_text(json.dumps(meta, indent=2, default=float))


def _gmean_normalize(x: np.ndarray, mask: np.ndarray | None = None,
                       floor: float = 1e-30) -> tuple[np.ndarray, float]:
    """Return (x / gmean(x[mask]), gmean(x[mask])).

    `mask` restricts the gmean to identifying nodes — floored
    no-population nodes are excluded so they cannot drag the geometric
    mean by orders of magnitude. After division, gmean(x[mask]) == 1.
    """
    x = np.clip(np.asarray(x, dtype=float), floor, None)
    sel = x if mask is None else x[mask]
    g = float(np.exp(np.mean(np.log(sel))))
    if not np.isfinite(g) or g <= 0:
        return x, 1.0
    return x / g, g


def invert_fundamentals(
    prim: Primitives,
    params: Parameters,
    max_iter: int = 800,
    tol: float = 1e-8,
    floor_share: float = 1e-6,
    kappa: float | None = None,
    kappa_target_rowsum: float = 0.9,
    tau_min_kl: np.ndarray | None = None,
) -> InversionResult:
    """Recover (ubar_i^theta, Abar_j^theta) from the observed allocation.

    Parameters
    ----------
    floor_share :
        Many grid nodes have L_R_obs == 0 or L_F_obs == 0 — they appear
        in the network but no residents/jobs.  log(0) breaks the
        alternating update, so we floor the observed shares at
        `floor_share` BEFORE renormalising.  This mirrors the fix in
        `Replication_apply/python_commuting/compare_asym_vs_sym.py`
        (PLAN.md §5).
    """
    theta = params.theta; alpha = params.alpha; beta = params.beta
    N = prim.n_nodes

    # observed shares with a positive floor so log() stays finite
    L_R = np.maximum(prim.L_R_obs, floor_share)
    L_F = np.maximum(prim.L_F_obs, floor_share)
    l_R = L_R / L_R.sum()
    l_F = L_F / L_F.sum()

    # Nodes with strictly positive observed marginals on BOTH sides
    # carry identifying information for the inversion. Floored nodes
    # contribute degenerate (v,w) values and are masked out from the
    # convergence and chi-recovery calculations.
    valid = (prim.L_R_obs > 0) & (prim.L_F_obs > 0)

    # observed tau matrix from the iceberg convention. We apply the
    # same uniform `kappa` rescaling as the equilibrium solver so the
    # spectral-radius condition holds (v4 §3.2 (E7)).
    tau_edge = np.asarray(
        prim.tau_obs_min if tau_min_kl is None else tau_min_kl,
        dtype=float,
    )
    if kappa is None:
        raw = np.zeros(N)
        np.add.at(raw, prim.edge_i, 1.0 / np.clip(tau_edge, EPS, None))
        max_rs = float(raw.max())
        kappa = max(1.0, max_rs / kappa_target_rowsum)
    inv = 1.0 / np.clip(kappa * tau_edge, EPS, None)
    A = np.zeros((N, N), dtype=float)
    np.add.at(A, (prim.edge_i, prim.edge_j), inv)
    tau_inv_theta = sla.solve(np.eye(N) - A, np.eye(N), check_finite=False)
    eta = float(getattr(params, "route_eta", 1.0))
    if eta == 1.0:
        tau_inv_theta = np.clip(tau_inv_theta, EPS, None)
    else:  # nested Frechet: tau^-theta = R^eta (see equilibrium_solve._resolvent_and_tau)
        tau_inv_theta = np.clip(np.clip(tau_inv_theta, 1e-300, None) ** eta, EPS, None)

    v = np.ones(N); w = np.ones(N)
    pR = 1.0 - beta * theta
    pF = 1.0 - alpha * theta
    qR = beta * theta
    qF = alpha * theta

    # Alternating loop holds chi=1 internally; chi is recovered at the
    # end from the implied per-equation constant. (Letting chi drift
    # inside the loop introduces a chi <-> (v,w) feedback that destabi-
    # lises the geometric-mean normalisation.)
    last_delta = np.inf
    converged = False
    for it in range(1, max_iter + 1):
        # a_i = sum_j tau_ij^{-theta} (l_F)^{alpha*theta} w_j
        a = tau_inv_theta @ (w * (l_F ** qF))
        v_new = (l_R ** pR) / np.clip(a, EPS, None)
        v_new, _ = _gmean_normalize(v_new, mask=valid)

        # b_j = sum_i tau_ij^{-theta} (l_R)^{beta*theta} v_i
        b = tau_inv_theta.T @ (v_new * (l_R ** qR))
        w_new = (l_F ** pF) / np.clip(b, EPS, None)
        w_new, _ = _gmean_normalize(w_new, mask=valid)

        # Convergence metric — std(log chi_R), std(log chi_F) over valid
        # nodes only. A correct inversion has chi constant across i.
        a_chk = tau_inv_theta @ (w_new * (l_F ** qF))
        b_chk = tau_inv_theta.T @ (v_new * (l_R ** qR))
        chi_R = (l_R ** pR) / np.clip(v_new * a_chk, EPS, None)
        chi_F = (l_F ** pF) / np.clip(w_new * b_chk, EPS, None)
        log_chiR = np.log(np.clip(chi_R[valid], EPS, None))
        log_chiF = np.log(np.clip(chi_F[valid], EPS, None))
        last_delta = float(max(np.std(log_chiR), np.std(log_chiF)))
        v, w = v_new, w_new
        if last_delta < tol:
            converged = True
            break

    # Recover chi as the geometric mean over VALID nodes (gmean(v)=
    # gmean(w)=1 was applied over all nodes; the valid-only mean
    # removes floored-node noise).
    a = tau_inv_theta @ (w * (l_F ** qF))
    b = tau_inv_theta.T @ (v * (l_R ** qR))
    chi_R = (l_R ** pR) / np.clip(v * a, EPS, None)
    chi_F = (l_F ** pF) / np.clip(w * b, EPS, None)
    chi = float(np.exp(0.5 * (np.mean(np.log(np.clip(chi_R[valid], EPS, None))) +
                                np.mean(np.log(np.clip(chi_F[valid], EPS, None))))))

    # residuals: how well the inverted (v,w,chi) reproduce (l_R, l_F)
    l_R_pred = (chi * v * a) ** (1.0 / pR)
    l_F_pred = (chi * w * b) ** (1.0 / pF)
    l_R_pred /= l_R_pred.sum(); l_F_pred /= l_F_pred.sum()
    resid_R = np.zeros(N); resid_F = np.zeros(N)
    resid_R[valid] = np.log(np.clip(l_R_pred[valid], EPS, None)) - np.log(np.clip(l_R[valid], EPS, None))
    resid_F[valid] = np.log(np.clip(l_F_pred[valid], EPS, None)) - np.log(np.clip(l_F[valid], EPS, None))

    return InversionResult(
        ubar_theta=v, abar_theta=w, chi=float(chi),
        kappa=float(kappa),
        tau_obs_inv_theta=tau_inv_theta,
        n_iter=it, converged=converged,
        max_log_change=float(last_delta),
        resid_R=resid_R, resid_F=resid_F,
    )
