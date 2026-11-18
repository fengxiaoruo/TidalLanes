"""
full_estimation
===============

Level-form (non-exact-hat) implementation of the TidalLanes structural
model described in `Documents/L1_manuscripts/TidalLanes_Model_v4.tex`
Section 6 (Estimation and Algorithms).

Five pipeline steps:
    Step 1  build_primitives        — load directed grid + OD primitives
    Step 2  parameters              — externally calibrated (theta, alpha,
                                       beta, lambda)
    Step 3  lambda_diagnostic       — diagnostic speed-flow regression
    Step 4  invert_fundamentals     — recover (ubar_i, Abar_j) from data
    Step 5  equilibrium_solve       — solve the level-form fixed point
            counterfactual_levels   — CF1 / CF2 / CF3 scenario runners

Design choices for v1 (set during 2026-05-02 meeting):
    * No parameter estimation: (theta, alpha, beta, lambda) sourced from
      AA (2022) Seattle.
    * No exact-hat: counterfactuals re-solve in levels each time.
    * Grid travel-cost convention is not locked; the loader exposes a
      `--version-id` switch so the choice remains open.

See ESTIMATION_NOTES.md for the per-step derivation and code thinking.
"""

__all__ = [
    "build_primitives",
    "parameters",
    "lambda_diagnostic",
    "invert_fundamentals",
    "equilibrium_solve",
    "counterfactual_levels",
]
