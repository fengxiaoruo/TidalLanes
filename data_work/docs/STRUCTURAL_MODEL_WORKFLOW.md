# Structural Model Workflow

This note documents the standalone structural-model prototype added on top of the existing pipeline outputs.

## Scope

The prototype uses the exported QSM-style inputs in `data_work/outputs/<version_id>/data/` and implements:

- shortest-path commuting costs on the directed grid graph
- a gravity-style calibration diagnostic for `theta`
- a weak cross-sectional proxy calibration for `lambda`
- inversion of baseline fundamentals `ubar_theta` and `abar_theta`
- fixed-point solution of the commuting equilibrium
- a tidal-lane counterfactual based on directional lane reallocation
- a counterfactual suite covering symmetric-cost and tidal-lane scenarios

## Important approximation

This implementation is intentionally pragmatic and does **not** yet solve the exact route aggregator in the TeX notes:

- bilateral commuting costs are approximated with shortest-path travel time
- route assignment is all-or-nothing on shortest paths
- congestion updates are anchored to observed edge times:

`t_cf = t_obs * [ (phi_cf / n_cf) / (phi_obs / n_obs) ] ^ lambda`

This avoids needing a separately identified free-flow travel-time vector before the first working version.

## Main entry point

```bash
python data_work/src/model/run_spatial_equilibrium.py \
  --version-id raw_rebuild_validation \
  --output-dir data_work/outputs \
  --grid-type square \
  --top-n 5
```

```bash
python data_work/src/model/run_counterfactual_suite.py \
  --version-id raw_rebuild_validation \
  --output-dir data_work/outputs \
  --grid-type square \
  --lambda-list 0.15 \
  --topn-list 5,10 \
  --max-iter 60
```

Outputs are written to:

- `data_work/outputs/<version_id>/model_square_baseline/`

Main files:

- `calibration_summary.json`
- `fundamentals_inverted.csv`
- `equilibrium_summary.csv`
- `edge_counterfactual_results.csv`
- `node_counterfactual_results.csv`
- `counterfactual_suite_summary.csv`

## Parameters

Defaults currently follow the draft:

- `theta = 6.83`
- `alpha = -0.12`
- `beta = -0.10`
- `lambda_congestion = 0.15`

`theta` and `lambda` are also accompanied by diagnostic calibration outputs, but the current `lambda` fit should be treated as a weak proxy rather than a publishable estimate.

## Next upgrades

- replace shortest-path costs with the route aggregator in the draft
- replace all-or-nothing assignment with path-share assignment
- estimate `lambda` with a panel or IV design
- reconcile the gravity-implied `theta` estimate with the externally calibrated benchmark
