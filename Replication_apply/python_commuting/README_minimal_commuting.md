## Minimal Seattle Commuting Replica

This folder is a minimal Python port of the Seattle commuting-mode core from `AA_ReplicationFinal`.

### Scope

The current minimal scope keeps only the pieces needed to:

1. Load the Seattle commuting inputs already prepared for Matlab.
2. Reproduce `analysis/seattle/predict_commuting.m`.
3. Reproduce the static commuting equilibrium routines used by Figure 1 and as the base for counterfactual work.

### Reference Matlab files

- `analysis/seattle/predict_commuting.m`
- `analysis/figure1/fn_AA_calc_eqm_commuting_static.m`
- `analysis/figure1/fn_AA_eqm_lr_lf.m`
- `counterfactuals/seattle/fn_AA_calc_eqm_commuting_counterfactual.m`
- `counterfactuals/seattle/fn_AA_eqm_lr_lf_counterfactual.m`

### Minimal input files

- `counterfactuals/seattle/sparse_adjmat_seattle.csv`
- `counterfactuals/seattle/node_lr_lf_seattle.csv`
- `counterfactuals/seattle/sparse_commute_seattle.csv`

### Current Python files

- `io.py`: reads the Seattle commuting inputs into dense NumPy arrays.
- `predict_commuting.py`: Python port of `predict_commuting.m`.
- `static_eqm.py`: Python port of the static commuting equilibrium helpers.
- `counterfactual.py`: Python port of the Seattle commuting exact-hat counterfactual solver.
- `run_single_edge_counterfactual.py`: single-edge validation entrypoint against the archived Matlab outputs.
- `run_edge_sanity_check.py`: small multi-edge accuracy check.
- `run_all_counterfactual_edges.py`: sequential whole-network driver analogous to `asym_counterfactual_berlin.m`.

### Notes

- The Python translation tries to stay close to Matlab variable names and algebra.
- The first priority is numerical faithfulness, not refactoring elegance.
- Counterfactual wrappers are not yet added; they will build directly on `static_eqm.py`.
