"""
Step 2 — External calibration of (theta, alpha, beta, lambda)
=============================================================

v4.tex §6.2:

    theta  = 6.83    Frechet shape (commuting elasticity)
    alpha  = -0.12   workplace externality elasticity
    beta   = -0.10   residential externality elasticity

v4.tex §6.3 (current data cannot identify lambda structurally):

    lambda = delta1 / theta = 0.488 / 6.83 ≈ 0.07144

with delta1 = 0.488 imported from AA(2022) Seattle. The 2026-05-02
meeting decision is to keep this placeholder until Professor You Wei's
related parameters are chosen.

We expose a small frozen dataclass so the rest of the pipeline reads
parameters from a single object and never from string literals.
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
import json
from pathlib import Path


# AA Seattle Table values — see also Replication_apply/PLAN.md §4.
AA_SEATTLE_THETA = 6.83
AA_SEATTLE_DELTA1 = 0.488
AA_SEATTLE_ALPHA = -0.12
AA_SEATTLE_BETA = -0.10


@dataclass(frozen=True)
class Parameters:
    theta: float = AA_SEATTLE_THETA
    alpha: float = AA_SEATTLE_ALPHA
    beta: float = AA_SEATTLE_BETA
    # lambda is reserved in Python; we use lambda_ throughout.
    lambda_: float = AA_SEATTLE_DELTA1 / AA_SEATTLE_THETA  # ≈ 0.07144
    delta1_source: float = AA_SEATTLE_DELTA1

    theta_source: str = "AA(2022) Seattle, via Ahlfeldt et al. (2015)"
    alpha_source: str = "AA(2022) Seattle calibration"
    beta_source: str = "AA(2022) Seattle calibration"
    lambda_source: str = "delta1/theta with delta1=0.488 (AA 2022 Seattle)"
    notes: tuple = field(
        default_factory=lambda: (
            "v1 placeholder per 2026-05-02 meeting — no structural estimation yet.",
            "TODO: re-align with Professor You Wei's parameter choices.",
            "TODO: replace lambda with a panel/IV estimate once directed-traffic data are available.",
        )
    )

    def to_dict(self) -> dict:
        d = asdict(self)
        d["lambda"] = d.pop("lambda_")
        return d

    def write(self, out_path: Path) -> None:
        Path(out_path).parent.mkdir(parents=True, exist_ok=True)
        Path(out_path).write_text(json.dumps(self.to_dict(), indent=2, default=str))


default_parameters = Parameters
