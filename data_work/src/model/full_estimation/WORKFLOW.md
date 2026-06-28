# Workflow — inputs / algorithm / outputs

This file defines the data contract between steps. For derivations and
algorithm reasoning see `ESTIMATION_NOTES.md`.

Source data root: `data_work/outputs/<version_id>/data/`. Default
`version_id = manual_centerline_rules_v11`, `grid_type = square`,
`period = AM`.

## Step 1 — `build_primitives.load_primitives`

Inputs

- `qsm_input_nodes_<grid>.parquet` columns used: `grid_id`, `node_i`,
  `residents`, `jobs`, `keep_for_model`.
- `qsm_input_edges_<grid>.parquet` columns used: `i`, `j`, `period`,
  `tau_obs_min`, `tau_ff_min`, `total_link_len_m`, `lanes_directional`,
  `lane_est_length_weighted` (lane fallback).
- `qsm_input_od_<grid>.parquet` columns used: `origin_node`,
  `destination_node`, `commuters_road` (default OD-pop column).

Output object `Primitives` (in-memory) + CSV/JSON dump under
`<out>/primitives/`:

| symbol             | shape | unit          | source                |
|--------------------|-------|---------------|-----------------------|
| `node_grid_ids`    | (N,)  | str           | nodes.grid_id         |
| `edge_i, edge_j`   | (M,)  | dense indices | nodes.node_i remap    |
| `ell_kl_km`        | (M,)  | km            | total_link_len_m/1000 |
| `tau_ff_min`       | (M,)  | minutes       | edges.tau_ff_min      |
| `tau_obs_min`      | (M,)  | minutes       | edges.tau_obs_min     |
| `v0_kl, v_obs_kl`  | (M,)  | km/min        | ell/tau               |
| `n_kl`             | (M,)  | lanes         | lanes_directional     |
| `L_R_obs`          | (N,)  | persons       | sum_j L_ij_obs        |
| `L_F_obs`          | (N,)  | persons       | sum_i L_ij_obs        |
| `L_ij_obs`         | (N,N) | persons       | dense OD matrix       |
| `Lbar`             | scalar| persons       | sum_i L_R_obs         |

Sanity rules: drop edges with non-positive or non-finite `tau_obs_min`
or `tau_ff_min`; clip lane fallback to `nanmean(n_kl)` when both
`lanes_directional` and `lane_est_length_weighted` are missing.

## Step 2 — `parameters.Parameters`

Hard-coded defaults; written as JSON to `<out>/parameters.json`.

| symbol | value         | source                                       |
|--------|---------------|----------------------------------------------|
| theta  | 6.83          | AA(2022) Seattle, via Ahlfeldt et al. (2015) |
| alpha  | -0.12         | AA(2022) Seattle calibration                 |
| beta   | -0.10         | AA(2022) Seattle calibration                 |
| lambda | 0.488 / 6.83 ≈ 0.07144 | δ1/θ with AA Seattle δ1             |

TODOs are recorded in `parameters.notes`.

## Step 3 — `lambda_diagnostic.estimate_lambda_cross_section`

Runs **after** the baseline equilibrium, because we need a baseline
`Xi_hat`. Output `<out>/lambda_diagnostic.json` has:

| key          | meaning                                                  |
|--------------|----------------------------------------------------------|
| `lambda_hat` | OLS slope after lane-bin demeaning (diagnostic)          |
| `se`, `r2`   | standard error, R²                                       |
| `n_obs`      | number of valid edges                                    |
| `method`     | "OLS, lane-bin demeaned, model-implied Xi_hat"           |
| `notes`      | "Diagnostic only — does not override parameters.lambda_" |

## Step 4 — `invert_fundamentals.invert_fundamentals`

Inputs: `Primitives`, `Parameters`. Algorithm: alternating updates of
`(v_i = ubar_i^θ, w_j = Abar_j^θ)` with geometric-mean normalisation;
the bilateral cost matrix `tau_ij_obs^{-θ}` is computed once via
`(I − A_obs)^{-1}`.

Output `<out>/fundamentals/`:

| file                       | content                                  |
|----------------------------|------------------------------------------|
| `fundamentals_arrays.npz`  | `ubar_theta`, `abar_theta`, `resid_R`, `resid_F` |
| `fundamentals_meta.json`   | `chi`, iteration count, convergence flag |

Residuals are log-share gaps `log(l_pred / l_obs)`; they should be
near zero if Assumption ass:obs_eq holds.

## Step 5 — `equilibrium_solve.solve_equilibrium`

Inputs: `Primitives`, `Parameters`, `ubar_theta`, `abar_theta`, and
optional `n_kl_override` (counterfactual lane vector). Output the
`EquilibriumResult` arrays:

| symbol         | shape | unit       |
|----------------|-------|------------|
| `tau_min_kl`   | (M,)  | minutes    |
| `t_kl_iceberg` | (M,)  | iceberg    |
| `Xi_kl`        | (M,)  | traffic    |
| `tau_ij_inv_theta` | (N,N) | dimensionless |
| `l_R`, `l_F`   | (N,)  | shares     |
| `Pi_i`, `P_j`  | (N,)  | market access |
| `Wbar`         | scalar| welfare    |

Saved to `<out>/baseline_check/` (baseline) and
`<out>/counterfactuals/CF{1,2,3}_.../` (scenarios).

## Counterfactuals

`<out>/counterfactuals/CF*/summary.csv` columns:

`Wbar_baseline`, `Wbar_cf`, `Wbar_pct_change`,
`tau_min_mean_baseline`, `tau_min_mean_cf`, `tau_min_pct_change`,
`l_R_max_abs_log_change`, `l_F_max_abs_log_change`,
`converged_baseline`, `converged_cf`.

CF2 additionally writes `selected_pairs.csv` with the treated edge
indices and pre-shock lane counts.

## Relationship to `Replication_apply/`

This package is **independent** of the exact-hat replication code in
`Replication_apply/python_commuting/`. The two paths share no Python
imports. The exact-hat path stays the authoritative AA(2022)
implementation; this scaffold is the level-form sibling that lets us
run the full estimation (Steps 1–4 + CFs) without invoking exact-hat.
