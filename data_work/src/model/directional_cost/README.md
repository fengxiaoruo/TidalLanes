# Directional Cost Model

Fixed-cost Stage-1 exercise: compare a **directed calibration world** and a **symmetric
calibration world**. Each world inverts its own fundamentals under its cost surface, then
solves static equilibrium. No propagation-scale parameter; $\kappa=1$ on the gate5 LCC
network when raw row sums are already below the spectral bound.

Default command (gate5 bundle):

```bash
python3 data_work/src/model/directional_cost/run_directional_cost.py \
  --version-id model_input_edgeproj_v2_lcc_gate5_v1 \
  --output-name directional_cost_edgeproj_v2_lcc_gate5_v1 \
  --sym-method geometric
```

Plots:

```bash
python3 data_work/src/model/directional_cost/plot_results.py \
  --output-root data_work/outputs/directional_cost_edgeproj_v2_lcc_gate5_v1
python3 data_work/src/model/directional_cost/plot_stage1_maps.py \
  --output-root data_work/outputs/directional_cost_edgeproj_v2_lcc_gate5_v1
```

Non-directional policy counterfactuals (same shock, directed vs symmetric world):

```bash
python3 data_work/src/model/directional_cost/run_policy_counterfactuals.py
```

Tidal-lane prototype (BPR fixed-ξ, short-run vs long-run, no spillovers):

```bash
python3 data_work/src/model/directional_cost/run_tidal_lane_counterfactuals.py
python3 data_work/src/model/directional_cost/run_tidal_lane_counterfactuals.py --quick
python3 data_work/src/model/directional_cost/plot_tidal_lane_results.py --repo-root .
```

Write-up: `Documents/L1_manuscripts/FixedDirectedCost_Stage1_gate5.tex`.

Complete workflow log: `Documents/L2_notes/stage1_fixed_directed_cost_complete_handoff_20260628.md`.

