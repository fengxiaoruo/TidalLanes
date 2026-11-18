# Tidal Lanes Model Slides Minimal Package, R Replica

This folder is an independent R implementation of the structural model checks in
`tidal_model_slides_minimal/`. It reads the same package-local parquet inputs and
compares outputs against the Python reference tables.

## Current Commands

```bash
make smoke-model
make compare-smoke
make stage2-quick
make stage2-full
make policy-full
make tidal-diagnostics
make alpha-beta-sensitivity
make symmetry-suite
make compare-reference
make validate-full
```

## Current Coverage

- Baseline primitives, fundamentals inversion, market access, welfare, and
  anchored endogenous congestion are implemented in R.
- Smoke baseline matches the Python mini output within floating-point tolerance.
- Full Stage2 structural counterfactuals, policy all-modes rows, and fixed-Xi
  tidal BPR grids reproduce the Python reference tables.
- Tidal ST/LT diagnostics reproduce the Python reference tables.
- Alpha-beta sensitivity reproduces the converged Python reference rows.
- Symmetry-directionality diagnostics reproduce all 18 Python reference rows.
- Long-run P1 to P7 policy comparison and cost-effectiveness reproduce the
  Python reference rows.

## Validation Status

The consolidated comparison output is in
`outputs/validation_r_report/reference_comparison.csv`.

### 2026-09-26 update: physical-time decomposition

`R/model/diagnostics.R` now mirrors the Python `tidal_lane_diagnostics.py`
after its 2026-07-16 correction. The all-route cost index `tau_ij` is no longer
converted to seconds. Experienced commuting time is the link-passage average
`sum(Xi_kl * m_kl) / (Lbar * kappa)`, the OD-conditional expected route minutes
are `[R B R]_ij / R_ij`, and the short-run, congestion, and sorting terms are
defined in link-passage space. The compact table written by
`run_tidal_lane_diagnostics_gate5.R` carries the new `avg_physical_time_*` and
`physical_time_saved_*` columns. The reference tables for tidal diagnostics and
the alpha-beta grid were replaced by the physical-time runs from the main
repository (see `../tidal_model_slides_minimal/docs/FILE_MAP.csv`).

Rerun of `make tidal-diagnostics` against the new reference: both tidal tables
pass at tolerance `1e-6` (largest difference `1.4e-9`). The top-100 row
reproduces 11.298 baseline minutes, 1,030.77 short-run person-hours saved, and
352.25 long-run person-hours added. The alpha-beta grid was rerun with the
updated code on the same day: all eight converged cells pass at `1e-6`
(largest difference `5.3e-9`, in `person_min_day_lt_total`). Ten of eleven
table families now pass; the one failure is the pre-existing P6 short-run row
described below.

### Earlier status (2026-07)

Ten of eleven table families passed at tolerance `1e-6`. The single residual is
`policy_results_by_policy`, key `P6_first_best_pricing|short_run`. This
high-toll short-run pricing diagnostic differs in the pinned-share congestion
fixed point: the largest reported difference is `929.936` in
`toll_revenue_per_day`, with realized welfare differing by `0.0189` percentage
points. The long-run P6 row and `policy_cost_effectiveness.csv` reproduce the
Python reference. Note that the P6 and P7 rows add the toll to the free-flow
floor of the anchored congestion update, so they are not a valid pricing
counterfactual in either language; see
`Documents/L2_notes/model_audit_and_referee_review_20260926.md`.
