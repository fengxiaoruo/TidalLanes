# AA Input (Square v1)

This folder is the canonical AA-format input bundle for the first-pass square-grid application.

It contains the three CSVs expected by the Seattle commuting replication code:

- `node_lr_lf_seattle.csv`
- `sparse_adjmat_seattle.csv`
- `sparse_commute_seattle.csv`

These files are generated from:

- `data_work/outputs/manual_centerline_rules_v11/data/qsm_input_nodes_square.parquet`
- `data_work/outputs/manual_centerline_rules_v11/data/qsm_input_edges_square.parquet`
- `data_work/outputs/manual_centerline_rules_v11/data/qsm_input_od_square.parquet`

## How it is used

- This folder is the canonical source bundle for the current quick-run application.
- The builder script also syncs the same three files into:
  - `Replication_apply/counterfactuals/seattle/`

That sync keeps the working Matlab and Python code pointed at the original AA-style relative paths.

## Important current limitation

The third column of `sparse_adjmat_seattle.csv` is a first-pass traffic proxy recovered from:

- observed travel time
- free-flow travel time
- directional capacity proxy

It is only meant to preserve the AA package input shape so the validation workflow can run quickly. It should be replaced later with the preferred speed-flow-based traffic recovery rule.
