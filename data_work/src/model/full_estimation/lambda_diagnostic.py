"""
Step 3 — Congestion elasticity λ (diagnostic only)
==================================================

Target equation (v4.tex eq. 6.3, repeated as eq. lambda_regression):

    ln(v_obs / v0)_kl  =  -lambda * ln( Xi_hat_kl / n_kl )  +  eta_kl  +  eps

Equivalently, in iceberg-time form:

    ln(tau_obs / tau_ff)_kl  =  + lambda * ln( Xi_hat_kl / n_kl ) + eps

v4.tex §6.3 already flags two limitations of the current data:
    (i)  directed link traffic Xi_kl is not observed,
    (ii) we have one peak-period cross-section, no panel / IV.

This module therefore reports a **diagnostic** regression only. The
output is NOT used to overwrite the placeholder λ defined in
parameters.py — it is a calibration-check number, not the structural
estimate, just as v4.tex §6.3 specifies for the
``estimate_lambda_cross_section`` routine.

Pipeline:
    Xi_hat_kl  := model-assigned baseline traffic; passed in from
                  the equilibrium solver at the observed allocation.
                  (Without an Xi_hat we cannot run this regression.)
    Residualise log(tau_obs/tau_ff) and log(Xi_hat/n_kl) by lane bins
    (n_kl rounded to integer) to remove cross-lane heterogeneity that
    is captured by edge fixed effects in the panel specification.
    OLS slope on the residualised regressor is the reported lambda_hat.

Outputs: a small dict with lambda_hat, std error, R^2, n_obs.
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass
class LambdaDiagnostic:
    lambda_hat: float
    se: float
    r2: float
    n_obs: int
    method: str
    notes: str

    def to_dict(self) -> dict:
        return {
            "lambda_hat": self.lambda_hat, "se": self.se,
            "r2": self.r2, "n_obs": self.n_obs,
            "method": self.method, "notes": self.notes,
        }


def estimate_lambda_cross_section(
    tau_obs: np.ndarray,
    tau_ff: np.ndarray,
    n_kl: np.ndarray,
    xi_hat: np.ndarray,
) -> LambdaDiagnostic:
    """Cross-section diagnostic for the congestion elasticity.

    Regression after lane-bin demeaning:
        y = log(tau_obs/tau_ff)        # rises with congestion
        x = log(xi_hat / n_kl)         # traffic intensity
        slope b   ->  lambda_hat = b
    """
    y = np.log(np.clip(tau_obs / tau_ff, 1e-12, None))
    x = np.log(np.clip(xi_hat / np.clip(n_kl, 1e-9, None), 1e-12, None))

    lane_bin = np.round(n_kl).astype(int)
    df = pd.DataFrame({"y": y, "x": x, "bin": lane_bin})
    # demean within lane bin (proxy for edge fixed effects)
    df["y_r"] = df["y"] - df.groupby("bin")["y"].transform("mean")
    df["x_r"] = df["x"] - df.groupby("bin")["x"].transform("mean")
    yr = df["y_r"].to_numpy(); xr = df["x_r"].to_numpy()

    valid = np.isfinite(yr) & np.isfinite(xr)
    yr = yr[valid]; xr = xr[valid]
    n = int(len(yr))
    xtx = float((xr * xr).sum())
    if n < 5 or xtx <= 0:
        return LambdaDiagnostic(np.nan, np.nan, np.nan, n,
                                "ols_lane_demeaned", "insufficient variation")

    b = float((xr * yr).sum() / xtx)
    yhat = b * xr
    resid = yr - yhat
    sse = float((resid * resid).sum()); sst = float((yr * yr).sum())
    r2 = 1.0 - sse / sst if sst > 0 else np.nan
    se = float(np.sqrt(sse / max(n - 1, 1) / xtx))

    return LambdaDiagnostic(
        lambda_hat=b, se=se, r2=r2, n_obs=n,
        method="OLS, lane-bin demeaned, model-implied Xi_hat",
        notes="Diagnostic only — see v4.tex §6.3; not used to override parameters.lambda_.",
    )
