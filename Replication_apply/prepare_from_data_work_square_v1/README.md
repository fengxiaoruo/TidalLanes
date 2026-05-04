# Prepare Inputs From `data_work` (Square v1)

This folder converts the current square-grid outputs under:

- `data_work/outputs/manual_centerline_rules_v11/data/`

into the minimal AA Seattle commuting input format expected by the original Matlab scripts:

- `counterfactuals/seattle/node_lr_lf_seattle.csv`
- `counterfactuals/seattle/sparse_adjmat_seattle.csv`
- `counterfactuals/seattle/sparse_commute_seattle.csv`

The canonical generated bundle is now stored in:

- `Replication_apply/aa_input_square_v1/`

The same three files are then synced into:

- `Replication_apply/counterfactuals/seattle/`

so the working Matlab and Python copies can keep the original AA-style relative paths.

## Current design

- Node source:
  - `qsm_input_nodes_square.parquet`
- OD source:
  - `qsm_input_od_square.parquet`
- Edge source:
  - `qsm_input_edges_square.parquet`

## Important note

The current `traffic` column in `sparse_adjmat_seattle.csv` is a first-pass traffic proxy recovered from:

- observed travel time
- free-flow travel time
- capacity proxy

using a simple monotone congestion transform. This is only meant to get the original AA-style replication workflow running quickly. It should be revisited once the preferred speed-flow specification is fixed.
