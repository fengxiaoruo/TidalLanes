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

# Congestion update forms for (E6):
#   legacy   — tau_ff * (Xi/n)^lambda          (Stage-2 default)
#   density  — tau_ff * (Xi/(n*ell))^lambda_phys  (Barwick density units)
#   anchored — tau_obs * ((Xi/Xi_obs)*(n_obs/n))^lambda_phys  (exact-hat)
CongestionForm = str  # "legacy" | "density" | "anchored"
BEIJING_LAMBDA_PHYS = 1.1

# Gate5 mainline (2026-06-29): semi-lock level at tau_obs; slope from documented lambda.
MAINLINE_CONGESTION_FORM = "anchored"
MAINLINE_LAMBDA_PHYS = BEIJING_LAMBDA_PHYS
MAINLINE_DAMPING = 0.25
MAINLINE_PAIR_SELECTION = "score_bpr_st"  # "asym" | "score_bpr_st" | "score_st" | "score_lt"
MAINLINE_ALPHA = 0.05
MAINLINE_BETA = -0.10


@dataclass
class EquilibriumResult:
    label: str
    tau_min_kl: np.ndarray          # legacy field name for physical m_kl, minutes (M,)
    t_kl_iceberg: np.ndarray        # iceberg cost (M,)
    Xi_kl: np.ndarray               # directed link traffic (M,)
    tau_ij_inv_theta: np.ndarray    # tau_ij^{-theta} (N,N)
    l_R: np.ndarray                 # residence share l_i^R (N,)
    l_F: np.ndarray                 # workplace share l_j^F (N,)
    Pi_i: np.ndarray                # market access Pi (N,)
    P_j: np.ndarray                 # market access P (N,)
    Wbar: float                     # aggregate welfare scale, computed from primitive u-w sum
    n_outer: int
    converged: bool
    max_log_change: float
    Wbar_pi_raw: float = np.nan     # gauge-dependent legacy Pi-based scale, for diagnostics only
    endogenous_congestion: bool = True
    kappa: float = 1.0
    congestion_form: str = "legacy"
    congestion_lambda: float = np.nan

    @property
    def m_min_kl(self) -> np.ndarray:
        """Physical directed-link travel time in minutes.

        ``tau_min_kl`` is retained as a compatibility name for existing
        runners and frozen outputs.  It stores ``m_kl``, not the bilateral
        all-route cost index ``tau_ij``.
        """
        return self.tau_min_kl

    def write(self, out_dir: Path) -> None:
        out_dir = Path(out_dir); out_dir.mkdir(parents=True, exist_ok=True)
        np.savez_compressed(
            out_dir / "equilibrium_arrays.npz",
            tau_min_kl=self.tau_min_kl, m_min_kl=self.m_min_kl,
            t_kl_iceberg=self.t_kl_iceberg,
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
    """A_kl = 1/(kappa*m_kl) = (kappa^(1/theta)*t_kl)^(-theta).

    Here ``m_kl=t_kl^theta`` is physical time in minutes.  The argument name
    ``tau_min_kl`` is retained only for compatibility with existing runners.

    `kappa` is a uniform iceberg-cost rescaling used to enforce the
    spectral-radius condition (v4 §3.2 (E7)). Absolute levels of `tau_ij`
    and `Wbar` are not invariant to `kappa`. Counterfactual *ratios* are
    only APPROXIMATELY kappa-invariant: kappa down-weights longer routes in
    (I-A)^{-1} = sum_K kappa^{-K} D^K, so it is a genuine route-diffusion
    parameter, not a pure gauge. Re-inverting fundamentals at each kappa
    keeps the baseline matched to data, but a small residual kappa-dependence
    in the CF response remains (e.g. tidal LT welfare +0.32% at kappa=1 vs
    +0.27% at kappa=3, 2026-06-30 audit). The runs use the calibrated minimal
    kappa (smallest value giving row-sum <= target). See
    Documents/L2_notes/estimation_audit_welfare_gauge_20260630.md.
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
def _resolve_lanes(n_kl: np.ndarray) -> np.ndarray:
    out = np.asarray(n_kl, dtype=float).copy()
    fallback = float(np.nanmedian(out[np.isfinite(out) & (out > 0)])) if np.any(np.isfinite(out)) else 2.0
    out = np.where(np.isfinite(out) & (out > 0), out, fallback)
    return np.clip(out, 1.0, None)


def _edge_lengths_km(prim: Primitives) -> np.ndarray:
    ell = np.asarray(prim.ell_kl_km, dtype=float)
    med = float(np.nanmedian(ell[np.isfinite(ell) & (ell > 0)])) if np.any(np.isfinite(ell) & (ell > 0)) else 1.0
    return np.clip(np.where(np.isfinite(ell) & (ell > 0), ell, med), EPS, None)


def _congestion_tau_update(
    *,
    form: str,
    lam: float,
    tau_ff: np.ndarray,
    tau_obs: np.ndarray,
    Xi_kl: np.ndarray,
    n_kl: np.ndarray,
    ell_km: np.ndarray,
    xi_obs_kl: np.ndarray | None,
    n_obs_kl: np.ndarray | None,
) -> np.ndarray:
    """Apply (E6) congestion mapping for one outer iteration."""
    if form == "legacy":
        ratio = np.clip(Xi_kl / np.clip(n_kl, EPS, None), EPS, None)
        return np.maximum(tau_ff, tau_ff * ratio ** lam)

    if form == "density":
        density = Xi_kl / np.clip(n_kl * ell_km, EPS, None)
        return np.maximum(tau_ff, tau_ff * np.clip(density, EPS, None) ** lam)

    if form == "anchored":
        if xi_obs_kl is None or n_obs_kl is None:
            raise ValueError("anchored congestion requires xi_obs_kl and n_obs_kl")
        dev = (
            np.clip(Xi_kl / np.clip(xi_obs_kl, EPS, None), EPS, None)
            * np.clip(n_obs_kl / np.clip(n_kl, EPS, None), EPS, None)
        )
        return np.maximum(tau_ff, tau_obs * dev ** lam)

    raise ValueError(f"unknown congestion form: {form}")


def compute_observation_anchors(
    prim: Primitives,
    ubar_theta: np.ndarray,
    abar_theta: np.ndarray,
    params: Parameters,
    *,
    chi: float = 1.0,
    kappa: float | None = None,
) -> dict[str, np.ndarray]:
    """Model-implied Xi and shares at the observed travel-time allocation."""
    eq = solve_equilibrium(
        prim, ubar_theta, abar_theta, params,
        tau_min_init=prim.tau_obs_min.copy(),
        endogenous_congestion=False,
        chi=chi,
        kappa=kappa,
        label="observation_anchor",
    )
    return {
        "xi_obs_kl": eq.Xi_kl.copy(),
        "n_obs_kl": _resolve_lanes(prim.n_kl),
        "tau_obs_anchor": prim.tau_obs_min.copy(),
        "l_R_obs_model": eq.l_R.copy(),
        "l_F_obs_model": eq.l_F.copy(),
    }


def solve_equilibrium(
    prim: Primitives,
    ubar_theta: np.ndarray,
    abar_theta: np.ndarray,
    params: Parameters,
    n_kl_override: np.ndarray | None = None,
    tau_ff_override: np.ndarray | None = None,
    tau_min_init: np.ndarray | None = None,
    chi: float = 1.0,
    damping: float = 0.5,
    max_outer: int = 60,
    tol: float = 1e-5,
    kappa: float | None = None,
    kappa_target_rowsum: float = 0.9,
    endogenous_congestion: bool = True,
    label: str = "equilibrium",
    l_R_fixed: np.ndarray | None = None,
    l_F_fixed: np.ndarray | None = None,
    congestion_form: str = "legacy",
    congestion_lambda: float | None = None,
    xi_obs_kl: np.ndarray | None = None,
    n_obs_kl: np.ndarray | None = None,
    tau_obs_anchor: np.ndarray | None = None,
) -> EquilibriumResult:
    """Outer fixed point in tau_min (= directed travel time, minutes).

    Parameters
    ----------
    endogenous_congestion :
        If False, hold ``tau_min`` at ``tau_min_init`` (or ``tau_ff``) and
        solve shares/market access only — Stage-1-style fixed-cost CFs on
        the level-form solver.
    l_R_fixed, l_F_fixed :
        If both provided, short-run mode: pin shares at these values while
        still allowing ``tau_min`` to update when ``endogenous_congestion``.
    tau_ff_override :
        Optional counterfactual free-flow times (minutes) for (E6).
    chi :
        Scale from fundamentals inversion; must match Step 4 output.
    congestion_form :
        ``legacy`` | ``density`` | ``anchored`` — see module docstring.
    congestion_lambda :
        Exponent on the congestion argument. Defaults to ``params.lambda_``.
        For Beijing Barwick calibration use ``1.1`` with ``density`` or
        ``anchored`` forms.
    xi_obs_kl, n_obs_kl, tau_obs_anchor :
        Required for ``anchored``; from :func:`compute_observation_anchors`.
    """
    theta = params.theta
    lam = params.lambda_ if congestion_lambda is None else float(congestion_lambda)
    ell_km = _edge_lengths_km(prim)
    tau_obs = np.asarray(
        prim.tau_obs_min if tau_obs_anchor is None else tau_obs_anchor,
        dtype=float,
    ).copy()
    if congestion_form == "anchored" and (xi_obs_kl is None or n_obs_kl is None):
        raise ValueError("anchored congestion requires xi_obs_kl and n_obs_kl")
    N = prim.n_nodes
    n_kl = _resolve_lanes(prim.n_kl if n_kl_override is None else n_kl_override)
    tau_ff = np.asarray(
        prim.tau_ff_min if tau_ff_override is None else tau_ff_override,
        dtype=float,
    ).copy()
    if tau_min_init is None:
        tau_min = tau_ff.copy()
    else:
        tau_min = np.asarray(tau_min_init, dtype=float).copy()

    if kappa is None:
        kappa = _pick_kappa(prim.edge_i, prim.edge_j, tau_min, N,
                            target_rowsum=kappa_target_rowsum)

    pin_shares = l_R_fixed is not None and l_F_fixed is not None
    if pin_shares:
        l_R_pin = np.clip(np.asarray(l_R_fixed, dtype=float), EPS, None)
        l_F_pin = np.clip(np.asarray(l_F_fixed, dtype=float), EPS, None)
        l_R_pin = l_R_pin / l_R_pin.sum()
        l_F_pin = l_F_pin / l_F_pin.sum()

    converged = False
    last_delta = np.inf
    it = 0
    for it in range(1, max_outer + 1):
        A = _build_A_matrix(prim.edge_i, prim.edge_j, tau_min, N, kappa=kappa)
        tau_inv_theta = _compute_tau_inv_theta(A)

        if pin_shares:
            l_R, l_F = l_R_pin, l_F_pin
        else:
            l_R, l_F, _ = _solve_lR_lF_given_tau(
                tau_inv_theta, ubar_theta, abar_theta, params, chi=chi,
            )
        Pi, Pj = _market_access(tau_inv_theta, l_R, l_F, prim.Lbar, theta)

        t_kl_iceberg = tau_min ** (1.0 / theta)
        Xi_kl = (
            (t_kl_iceberg ** (-theta))
            * (Pj[prim.edge_i] ** (-theta))
            * (Pi[prim.edge_j] ** (-theta))
        )

        if not endogenous_congestion:
            converged = True
            last_delta = 0.0
            break

        tau_min_new = _congestion_tau_update(
            form=congestion_form,
            lam=lam,
            tau_ff=tau_ff,
            tau_obs=tau_obs,
            Xi_kl=Xi_kl,
            n_kl=n_kl,
            ell_km=ell_km,
            xi_obs_kl=xi_obs_kl,
            n_obs_kl=n_obs_kl,
        )
        log_old = np.log(np.clip(tau_min, EPS, None))
        log_new = np.log(np.clip(tau_min_new, EPS, None))
        tau_min_damped = np.exp(damping * log_new + (1.0 - damping) * log_old)

        last_delta = float(np.max(np.abs(np.log(tau_min_damped) - log_old)))
        tau_min = tau_min_damped
        if last_delta < tol:
            converged = True
            break

    A = _build_A_matrix(prim.edge_i, prim.edge_j, tau_min, N, kappa=kappa)
    tau_inv_theta = _compute_tau_inv_theta(A)
    if pin_shares:
        l_R, l_F = l_R_pin, l_F_pin
    else:
        l_R, l_F, _ = _solve_lR_lF_given_tau(
            tau_inv_theta, ubar_theta, abar_theta, params, chi=chi,
        )
    Pi, Pj = _market_access(tau_inv_theta, l_R, l_F, prim.Lbar, theta)
    t_kl_iceberg = tau_min ** (1.0 / theta)
    Xi_kl = (
        (t_kl_iceberg ** (-theta))
        * (Pj[prim.edge_i] ** (-theta))
        * (Pi[prim.edge_j] ** (-theta))
    )

    L_R = l_R * prim.Lbar
    L_F = l_F * prim.Lbar
    u_theta = ubar_theta * (np.clip(L_R, EPS, None) ** (params.beta * theta))
    w_theta = abar_theta * (np.clip(L_F, EPS, None) ** (params.alpha * theta))
    Wbar_theta = float(np.sum(tau_inv_theta * u_theta[:, None] * w_theta[None, :]))
    Wbar = Wbar_theta ** (1.0 / theta) if Wbar_theta > 0 else np.nan

    Pi_inv_theta = np.clip(Pi, EPS, None) ** (-theta)
    Wbar_pi_theta = float(np.sum(u_theta * Pi_inv_theta))
    Wbar_pi_raw = Wbar_pi_theta ** (1.0 / theta) if Wbar_pi_theta > 0 else np.nan

    return EquilibriumResult(
        label=label,
        tau_min_kl=tau_min,
        t_kl_iceberg=t_kl_iceberg,
        Xi_kl=Xi_kl,
        tau_ij_inv_theta=tau_inv_theta,
        l_R=l_R,
        l_F=l_F,
        Pi_i=Pi,
        P_j=Pj,
        Wbar=float(Wbar),
        Wbar_pi_raw=float(Wbar_pi_raw),
        n_outer=it,
        converged=converged,
        max_log_change=last_delta,
        endogenous_congestion=endogenous_congestion,
        kappa=float(kappa),
        congestion_form=congestion_form,
        congestion_lambda=float(lam),
    )
