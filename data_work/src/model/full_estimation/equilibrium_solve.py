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
#   ce_floor — constant elasticity with a free-flow floor, anchored at the observation:
#              max(f, tau_obs * dev^lambda), f = min(tau_ff, tau_obs)
#              (the floor model estimated in run_congestion_iv_followup.py)
#   bpr      — anchored BPR: f * (1 + m_obs * dev^b), f = min(tau_ff, tau_obs),
#              m_obs = tau_obs / f - 1; b is passed as congestion_lambda
#   dev = (Xi/Xi_obs)*(n_obs/n). With xi_floor > 0 (any anchored form),
#   dev = (max(Xi, xi_floor)/max(Xi_obs, xi_floor))*(n_obs/n): the observation stays a fixed
#   point, but flow changes below the floor do not move travel time (guards edges whose
#   model flow at the observation is ~0). xi_floor = 0 reproduces the forms above exactly.
#   Using f = min(tau_ff, tau_obs) keeps the observed allocation a fixed point on edges
#   whose observed time is below the measured free-flow time.
CongestionForm = str  # "legacy" | "density" | "anchored" | "ce_floor" | "bpr"
ANCHORED_FORMS = ("anchored", "ce_floor", "bpr")
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
    route_eta: float = 1.0          # tau^-theta = R^route_eta (1 = AA mainline)

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


KAPPA_RHO_TARGET = 0.95


def spectral_radius_A(edge_i, edge_j, tau_min_kl, N, kappa: float = 1.0) -> float:
    """Spectral radius of A = 1/(kappa*tau); (I-A)^{-1} is the route sum only if < 1."""
    A = _build_A_matrix(edge_i, edge_j, tau_min_kl, N, kappa=kappa)
    return float(np.max(np.abs(np.linalg.eigvals(A))))


def resolve_kappa(prim, rule: str = "fixed1", target: float = KAPPA_RHO_TARGET) -> tuple[float, dict]:
    """kappa for a network under a named rule.

    fixed1   : kappa = 1 (all runs before 2026-11-09).
    <number> : that fixed kappa (sensitivity).
    spectral : smallest kappa >= 1 with rho(A) <= target at tau = min(tau_ff, tau_obs),
               the lowest time the ce_floor / bpr forms allow, so every counterfactual
               under those forms keeps rho < 1. rho scales as 1/kappa, so
               kappa = max(1, rho_1 / target). Networks with rho_1 <= target keep kappa = 1.
    """
    floor = np.minimum(prim.tau_ff_min, prim.tau_obs_min)
    rho_obs = spectral_radius_A(prim.edge_i, prim.edge_j, prim.tau_obs_min, prim.n_nodes)
    rho_floor = spectral_radius_A(prim.edge_i, prim.edge_j, floor, prim.n_nodes)
    if rule == "fixed1":
        kappa = 1.0
    elif rule == "spectral":
        kappa = max(1.0, rho_floor / target)
    else:
        try:
            kappa = float(rule)          # a fixed value, e.g. "1.5" (sensitivity runs)
        except ValueError:
            raise ValueError(f"unknown kappa rule {rule!r}") from None
    info = {"kappa_rule": rule, "kappa": kappa, "rho_target": target,
            "rho_at_tau_obs_kappa1": rho_obs, "rho_at_floor_kappa1": rho_floor,
            "rho_at_tau_obs": rho_obs / kappa, "rho_at_floor": rho_floor / kappa}
    return kappa, info


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


def _compute_tau_inv_theta(A: np.ndarray, eta: float = 1.0) -> np.ndarray:
    """Solve (I - A) X = I  ==>  X = (I - A)^{-1}, returns tau^{-theta} = X^eta."""
    return _resolvent_and_tau(A, eta)[1]


def _resolvent_and_tau(A: np.ndarray, eta: float = 1.0) -> tuple[np.ndarray, np.ndarray]:
    """Route resolvent R = (I - A)^{-1} and tau^{-theta} = R^eta (nested Frechet,
    eta = theta / rho_route; eta = 1 is the AA mainline, returned bit-identical)."""
    N = A.shape[0]
    M = np.eye(N) - A
    try:
        X = sla.solve(M, np.eye(N), check_finite=False)
    except sla.LinAlgError as exc:
        raise RuntimeError(
            "(I - A) is singular — spectral-radius condition fails."
        ) from exc
    if eta == 1.0:
        X = np.clip(X, EPS, None)
        return X, X
    R = np.clip(X, 1e-300, None)
    return R, np.clip(R ** eta, EPS, None)


def _route_traffic(prim, tau_min, R, tau_inv_theta, l_R, l_F, Pi, Pj, theta) -> np.ndarray:
    """Directed link traffic in the mainline units (kappa x passages).

    Passages on k->l summed over OD flows L_ij: a_kl [R' G R']_kl, G_ij = L_ij / R_ij,
    with L_ij = tau_ij^-theta (L_R Pi^theta)_i (L_F P^theta)_j. For eta = 1, G is
    rank one and this equals the mainline m^-1 P_k^-theta Pi_l^-theta exactly.
    """
    L_R = l_R * prim.Lbar; L_F = l_F * prim.Lbar
    o = L_R * Pi ** theta
    d = L_F * Pj ** theta
    G = (tau_inv_theta / R) * o[:, None] * d[None, :]
    Y = R.T @ G @ R.T
    return (1.0 / tau_min) * Y[prim.edge_i, prim.edge_j]


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
    xi_floor: float = 0.0,
) -> np.ndarray:
    """Apply (E6) congestion mapping for one outer iteration."""
    if form == "legacy":
        ratio = np.clip(Xi_kl / np.clip(n_kl, EPS, None), EPS, None)
        return np.maximum(tau_ff, tau_ff * ratio ** lam)

    if form == "density":
        density = Xi_kl / np.clip(n_kl * ell_km, EPS, None)
        return np.maximum(tau_ff, tau_ff * np.clip(density, EPS, None) ** lam)

    if form in ANCHORED_FORMS:
        if xi_obs_kl is None or n_obs_kl is None:
            raise ValueError("anchored congestion requires xi_obs_kl and n_obs_kl")
        if xi_floor > 0:
            flow = np.maximum(Xi_kl, xi_floor) / np.maximum(xi_obs_kl, xi_floor)
        else:
            flow = np.clip(Xi_kl / np.clip(xi_obs_kl, EPS, None), EPS, None)
        dev = flow * np.clip(n_obs_kl / np.clip(n_kl, EPS, None), EPS, None)
        if form == "anchored":
            return np.maximum(tau_ff, tau_obs * dev ** lam)
        floor = np.minimum(tau_ff, tau_obs)
        if form == "ce_floor":
            return np.maximum(floor, tau_obs * dev ** lam)
        markup = np.maximum(tau_obs / floor - 1.0, 0.0)
        return floor * (1.0 + markup * dev ** lam)

    raise ValueError(f"unknown congestion form: {form}")


@dataclass
class RoadLayer:
    """Road-level congestion (2026-11-10). Grid-edge flows Xi are loaded onto road units
    (OSM arcs) through the edge paths; congestion and lanes act on road units.

    D : sparse (M edges x U units), 1 if the edge path uses the unit
    H : sparse (M x U), the unit's share of the edge path time (rows sum to 1)
    n_obs, n : lanes per unit at the anchor and in the scenario
    x_obs : unit flow at the observation anchor, D.T @ xi_obs_kl
    x_floor : flow floor on units (same role as xi_floor on edges)

    Edge time, with dev_u = (max(X_u, f) / max(X_obs_u, f)) * n_obs_u / n_u and
    X = D.T @ Xi:
        ce_floor  tau_e = max(floor_e, tau_obs_e * sum_u H_eu dev_u^lam)
        bpr       tau_e = floor_e * (1 + markup_e * sum_u H_eu dev_u^lam)
        anchored  tau_e = max(tau_ff_e, tau_obs_e * sum_u H_eu dev_u^lam)
    With D = H = I and n = n_kl this is the edge-level mapping exactly.
    """
    D: object
    H: object
    n_obs: np.ndarray
    n: np.ndarray
    x_obs: np.ndarray
    x_floor: float = 0.0

    def with_lanes(self, n_new: np.ndarray) -> "RoadLayer":
        return RoadLayer(self.D, self.H, self.n_obs, np.asarray(n_new, dtype=float), self.x_obs, self.x_floor)

    def unit_flow(self, Xi_kl: np.ndarray) -> np.ndarray:
        return np.asarray(self.D.T @ Xi_kl).ravel()


def _road_tau_update(*, form: str, lam: float, tau_ff: np.ndarray, tau_obs: np.ndarray,
                     Xi_kl: np.ndarray, road: RoadLayer) -> np.ndarray:
    X = road.unit_flow(Xi_kl)
    f = road.x_floor
    if f > 0:
        flow = np.maximum(X, f) / np.maximum(road.x_obs, f)
    else:
        flow = np.clip(X / np.clip(road.x_obs, EPS, None), EPS, None)
    dev = flow * np.clip(road.n_obs / np.clip(road.n, EPS, None), EPS, None)
    agg = np.asarray(road.H @ (dev ** lam)).ravel()
    if form == "anchored":
        return np.maximum(tau_ff, tau_obs * agg)
    floor = np.minimum(tau_ff, tau_obs)
    if form == "ce_floor":
        return np.maximum(floor, tau_obs * agg)
    if form == "bpr":
        markup = np.maximum(tau_obs / floor - 1.0, 0.0)
        return floor * (1.0 + markup * agg)
    raise ValueError(f"road layer: unsupported congestion form {form}")


def marginal_external_cost(*, form: str, lam: float, tau_phys: np.ndarray, tau_ff: np.ndarray,
                           tau_obs: np.ndarray) -> np.ndarray:
    """Xi * d tau / d Xi (minutes) at physical times ``tau_phys`` for the anchored forms.

    anchored : lam * tau where tau > tau_ff (power branch), else 0
    ce_floor : lam * tau where tau > min(tau_ff, tau_obs), else 0
    bpr      : lam * (tau - f), f = min(tau_ff, tau_obs)   (lam is the BPR exponent b)
    The flow floor ``xi_floor`` is ignored here (it only binds on near-zero-flow edges).
    """
    tau_phys = np.asarray(tau_phys, dtype=float)
    if form == "anchored":
        return np.where(tau_phys > tau_ff * (1 + 1e-9), lam * tau_phys, 0.0)
    floor = np.minimum(tau_ff, tau_obs)
    if form == "ce_floor":
        return np.where(tau_phys > floor * (1 + 1e-9), lam * tau_phys, 0.0)
    if form == "bpr":
        return lam * np.maximum(tau_phys - floor, 0.0)
    raise ValueError(f"marginal_external_cost: unsupported form {form}")


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
    xi_floor: float = 0.0,
    toll_kl: np.ndarray | None = None,
    toll_pigou: bool = False,
    road: RoadLayer | None = None,
) -> EquilibriumResult:
    """Outer fixed point in tau_min (= directed travel time, minutes).

    Pricing (2026-11-10): with ``toll_kl`` (minutes, fixed) or ``toll_pigou`` (toll =
    current marginal external cost, see :func:`marginal_external_cost`), ``tau_min`` is the
    PERCEIVED cost tau_phys(Xi) + toll; physical time is re-derived from Xi by the caller
    (``stage2_counterfactuals.realized_welfare_excl_toll``). ``tau_min_init`` should then
    include the toll.

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
    eta = float(getattr(params, "route_eta", 1.0))
    lam = params.lambda_ if congestion_lambda is None else float(congestion_lambda)
    ell_km = _edge_lengths_km(prim)
    tau_obs = np.asarray(
        prim.tau_obs_min if tau_obs_anchor is None else tau_obs_anchor,
        dtype=float,
    ).copy()
    if congestion_form in ANCHORED_FORMS and (xi_obs_kl is None or n_obs_kl is None):
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

    if road is not None and (toll_kl is not None or toll_pigou):
        raise NotImplementedError("pricing with the road layer is not implemented")
    pin_shares = l_R_fixed is not None and l_F_fixed is not None
    if pin_shares:
        l_R_pin = np.clip(np.asarray(l_R_fixed, dtype=float), EPS, None)
        l_F_pin = np.clip(np.asarray(l_F_fixed, dtype=float), EPS, None)
        l_R_pin = l_R_pin / l_R_pin.sum()
        l_F_pin = l_F_pin / l_F_pin.sum()

    converged = False
    last_delta = np.inf
    it = 0
    toll_state = None if toll_kl is None else np.asarray(toll_kl, dtype=float)
    for it in range(1, max_outer + 1):
        A = _build_A_matrix(prim.edge_i, prim.edge_j, tau_min, N, kappa=kappa)
        R_route, tau_inv_theta = _resolvent_and_tau(A, eta)

        if pin_shares:
            l_R, l_F = l_R_pin, l_F_pin
        else:
            l_R, l_F, _ = _solve_lR_lF_given_tau(
                tau_inv_theta, ubar_theta, abar_theta, params, chi=chi,
            )
        Pi, Pj = _market_access(tau_inv_theta, l_R, l_F, prim.Lbar, theta)

        t_kl_iceberg = tau_min ** (1.0 / theta)
        if eta == 1.0:
            Xi_kl = (
                (t_kl_iceberg ** (-theta))
                * (Pj[prim.edge_i] ** (-theta))
                * (Pi[prim.edge_j] ** (-theta))
            )
        else:
            Xi_kl = _route_traffic(prim, tau_min, R_route, tau_inv_theta, l_R, l_F, Pi, Pj, theta)

        if not endogenous_congestion:
            converged = True
            last_delta = 0.0
            break

        if road is not None:
            tau_min_new = _road_tau_update(form=congestion_form, lam=lam, tau_ff=tau_ff,
                                           tau_obs=tau_obs, Xi_kl=Xi_kl, road=road)
        else:
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
                xi_floor=xi_floor,
            )
        if toll_pigou:
            # method of successive averages: the MEC jumps at the floor kink (0 below,
            # lam*tau above), so a plain update cycles; averaging with step 1/it converges
            # to a toll in the convex hull (a subgradient selection at kinked edges).
            mec = marginal_external_cost(
                form=congestion_form, lam=lam, tau_phys=tau_min_new, tau_ff=tau_ff, tau_obs=tau_obs)
            toll_state = mec if toll_state is None else toll_state + (mec - toll_state) / (it + 1)
            tau_min_new = tau_min_new + toll_state
        elif toll_kl is not None:
            tau_min_new = tau_min_new + toll_kl
        log_old = np.log(np.clip(tau_min, EPS, None))
        log_new = np.log(np.clip(tau_min_new, EPS, None))
        tau_min_damped = np.exp(damping * log_new + (1.0 - damping) * log_old)

        last_delta = float(np.max(np.abs(np.log(tau_min_damped) - log_old)))
        tau_min = tau_min_damped
        if last_delta < tol:
            converged = True
            break

    A = _build_A_matrix(prim.edge_i, prim.edge_j, tau_min, N, kappa=kappa)
    R_route, tau_inv_theta = _resolvent_and_tau(A, eta)
    if pin_shares:
        l_R, l_F = l_R_pin, l_F_pin
    else:
        l_R, l_F, _ = _solve_lR_lF_given_tau(
            tau_inv_theta, ubar_theta, abar_theta, params, chi=chi,
        )
    Pi, Pj = _market_access(tau_inv_theta, l_R, l_F, prim.Lbar, theta)
    t_kl_iceberg = tau_min ** (1.0 / theta)
    if eta == 1.0:
        Xi_kl = (
            (t_kl_iceberg ** (-theta))
            * (Pj[prim.edge_i] ** (-theta))
            * (Pi[prim.edge_j] ** (-theta))
        )
    else:
        Xi_kl = _route_traffic(prim, tau_min, R_route, tau_inv_theta, l_R, l_F, Pi, Pj, theta)

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
        route_eta=eta,
    )
