# Replication Apply

This folder is a self-contained workspace for applying the AA Seattle commuting replication package structure to this project's data.

## Purpose

The goal of this workspace is to keep three things side by side:

1. A preserved snapshot of the original Matlab commuting replication code.
2. A working Matlab mirror for application to this project's data.
3. The Python commuting port, copied as a reference and debugging companion.
4. A dedicated input-preparation layer that reshapes existing `data_work` outputs into AA-style input files.

## Structure

- `original_matlab_code_snapshot/`
  - unchanged copy of the original Matlab code brought over from the AA replication package
- `analysis/`
  - working Matlab analysis code for this application folder
- `counterfactuals/seattle/`
  - working Matlab counterfactual code
  - synced AA-style input CSVs used by the working code
- `aa_input_square_v1/`
  - canonical AA-format square-grid input bundle generated from `data_work`
- `data/seattle/derived/`
  - run outputs such as `predicted_lij.csv`
- `python_commuting/`
  - copied Python commuting port
- `prepare_from_data_work_square_v1/`
  - scripts and notes for converting the current square-grid `data_work` outputs into AA-style inputs

## Current scope

This first setup focuses on the commuting validation and baseline-input side:

- `node_lr_lf_seattle.csv`
- `sparse_adjmat_seattle.csv`
- `sparse_commute_seattle.csv`

The current goal is to get the commuting validation side running first. The full counterfactual adaptation and policy mapping can be handled later.

## Current conventions

- The canonical current input bundle lives in `aa_input_square_v1/`.
- The builder syncs those same files into `counterfactuals/seattle/` so the working Matlab and Python scripts can use the AA-style relative paths.
- The original Matlab code snapshot is preserved separately so any application-side edits remain transparent and minimal.
