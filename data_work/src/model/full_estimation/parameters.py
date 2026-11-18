"""
Step 2 — External calibration of (theta, alpha, beta, lambda)
=============================================================

v4.tex §6.2:

    theta  = 6.83    Frechet shape (commuting elasticity)
    alpha  = -0.12   workplace externality elasticity
    beta   = -0.10   residential externality elasticity

Two distinct congestion elasticities live here — do not conflate them:

  * ``lambda_phys`` = 1.1  — the MAINLINE physical congestion elasticity
    (edge travel time w.r.t. traffic-per-lane). Sourced from Beijing
    speed-density evidence (elasticity ≈ 1.1). This is what the endogenous
    equilibrium and every headline/deck result use, passed to the solver as
    ``congestion_lambda`` (see equilibrium_solve.MAINLINE_LAMBDA_PHYS).

  * ``lambda_`` = delta1 / theta = 0.488 / 6.83 ≈ 0.07144 — a LEGACY
    AA(2022) Seattle placeholder (delta1 = 0.488). It is used ONLY inside the
    exogenous-τ diagnostics (``delta1 = params.lambda_ * theta``), never in the
    mainline. Kept for backward compatibility per the 2026-05-02 meeting; the
    mainline no longer depends on it. Do NOT overwrite it with lambda_phys or
    the exog δ1 shock will change.

We expose a small frozen dataclass so the rest of the pipeline reads
parameters from a single object and never from string literals.
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict, replace
import json
from pathlib import Path


# AA Seattle Table values — see also Replication_apply/PLAN.md §4.
AA_SEATTLE_THETA = 6.83
AA_SEATTLE_DELTA1 = 0.488
AA_SEATTLE_ALPHA = -0.12
AA_SEATTLE_BETA = -0.10

# Mainline physical congestion elasticity (Beijing speed-density evidence ≈ 1.1).
# Canonical source of truth is equilibrium_solve.MAINLINE_LAMBDA_PHYS; mirrored
# here so downstream code can read it off the Parameters object.
LAMBDA_PHYS_BEIJING = 1.1


@dataclass(frozen=True)
class Parameters:
    theta: float = AA_SEATTLE_THETA
    alpha: float = AA_SEATTLE_ALPHA
    beta: float = AA_SEATTLE_BETA
    # Mainline physical congestion elasticity (edge time vs traffic-per-lane).
    # This is the elasticity used by every headline/deck result.
    lambda_phys: float = LAMBDA_PHYS_BEIJING  # = 1.1, Beijing speed-density
    # Legacy AA-Seattle placeholder, used ONLY in exogenous-τ diagnostics
    # (delta1 = lambda_ * theta = 0.488). NOT used in the mainline. Do not
    # overwrite with lambda_phys — see module docstring.
    lambda_: float = AA_SEATTLE_DELTA1 / AA_SEATTLE_THETA  # ≈ 0.07144
    delta1_source: float = AA_SEATTLE_DELTA1
    # Location-choice exponent on the route composite (2026-11-11, nested Frechet):
    # tau_ij^-theta = R_ij^route_eta, R = (I-A)^-1, route_eta = theta / rho_route.
    # 1.0 = AA (2022): route and location choice share theta (mainline).
    route_eta: float = 1.0
    route_eta_source: str = "AA(2022): route and location dispersion equal (eta=1)"

    theta_source: str = "AA(2022) Seattle, via Ahlfeldt et al. (2015)"
    alpha_source: str = "AA(2022) Seattle calibration"
    beta_source: str = "AA(2022) Seattle calibration"
    # Label only: no solver reads params.lambda_phys. The elasticity actually used is
    # congestion_lambda, passed by each run script. Scripts that know it call
    # with_congestion() before writing parameters.json (2026-11-18).
    lambda_phys_source: str = (
        "Placeholder: old gate5 mainline default (anchored, 1.1). Not read by the solver; "
        "the elasticity used is congestion_lambda passed by the run script (see mainline_config.json "
        "or the run command)"
    )
    lambda_source: str = "LEGACY exog-only: delta1/theta with delta1=0.488 (AA 2022 Seattle)"
    notes: tuple = field(
        default_factory=lambda: (
            "lambda_phys is a label; the solver uses congestion_lambda from the run script. "
            "Current mainline (2026-11): ce_floor, congestion_lambda=0.35.",
            "lambda_=0.07144 is a legacy AA-Seattle placeholder used only in exog diagnostics.",
            "TODO: re-align exog δ1 with lambda_phys once directed-traffic data support a panel/IV estimate.",
        )
    )

    def to_dict(self) -> dict:
        d = asdict(self)
        d["lambda"] = d.pop("lambda_")
        return d

    def with_lambda(self, lambda_: float, source: str | None = None) -> "Parameters":
        kw = {"lambda_": float(lambda_)}
        if source is not None:
            kw["lambda_source"] = source
        return replace(self, **kw)

    def with_congestion(self, congestion_form: str, congestion_lambda: float | None,
                        source: str = "run script --congestion-lambda") -> "Parameters":
        """Record the congestion elasticity actually passed to the solver."""
        if congestion_lambda is None:
            return self
        return replace(
            self,
            lambda_phys=float(congestion_lambda),
            lambda_phys_source=f"{source}: congestion_form={congestion_form}, congestion_lambda={float(congestion_lambda):g}",
        )

    def write(self, out_path: Path) -> None:
        Path(out_path).parent.mkdir(parents=True, exist_ok=True)
        Path(out_path).write_text(json.dumps(self.to_dict(), indent=2, default=str))


default_parameters = Parameters
