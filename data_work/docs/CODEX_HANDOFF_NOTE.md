# Codex Handoff Note

This document is the fastest way for a new Codex session on another machine to understand the project, the current working state, and the main unresolved issues.

## Project identity

- Project root: `/Users/fxr/Desktop/TidalLanes`
- Main current working pipeline: `data_work/src/stages/`
- Main current baseline output: `data_work/outputs/raw_rebuild_validation`
- Best current matching-only experiment: `data_work/outputs/match_projection_complete_v7`

The project studies directional road congestion and tidal-lane policies, currently centered on Beijing. The pipeline constructs a centerline-based directed road network, attaches speeds, builds grid-level directed links and commuting OD objects, and exports model-ready inputs.

## What is already stable

Use these as the current trusted baselines.

- Matching / ETL baseline:
  - `data_work/outputs/raw_rebuild_validation`
- Best current stage02-side matching alternative:
  - `data_work/outputs/match_projection_complete_v7`
- Paper-style descriptive outputs:
  - `Documents/todo_outputs`
- Economics draft generated separately from the old TeX:
  - `Documents/TidalLanes_EconDraft_0318.tex`
  - `Documents/TidalLanes_EconDraft_0318.pdf`

## Important project structure

- `data_work/src/stages/`
  Stage-based production pipeline.
- `data_work/src/diagnostics/`
  Plotting scripts and paper-asset generation.
- `data_work/src/model/`
  New standalone structural-model prototype added recently.
- `data_work/raw_data/`
  Raw Beijing inputs.
- `data_work/outputs/`
  Versioned outputs.
- `data_work/docs/`
  Current operational documentation.
- `00archive/`
  Historical notebooks, figures, and legacy material.

## Pipeline stages

The current stage order is:

1. `stage01_build_centerline.py`
2. `stage02_match_raw_to_centerline.py`
3. `stage03_attach_speed.py`
4. `stage03b_centerline_asymmetry.py`
5. `stage04_estimate_lanes.py`
6. `stage05_build_grids.py`
7. `stage06_build_grid_links.py`
8. `stage07_build_od_and_population.py`
9. `stage08_build_qsm_inputs.py`
10. `stage09_compare_versions.py`
11. `stage10_generate_figures.py`

See also:

- `data_work/src/stages/README.md`
- `data_work/docs/REPRODUCIBILITY_SNAPSHOT.md`
- `data_work/docs/QUICK_RERUN_CHEATSHEET.md`

## Matching work: current state

The stage02 matching problem has been worked on extensively.

Current conclusion:

- The most useful version is `match_projection_complete_v7`.
- The big improvement came from bypassing some outer-ring raw segments that already had a native centerline-like structure.
- Several more aggressive split experiments were tested and discarded because they over-split or did not materially improve matching.

Current unresolved issue:

- In the urban core, some raw segments still should be split by major network nodes but are not.
- A manual override workflow exists for these cases:
  - `data_work/docs/MANUAL_MATCH_OVERRIDE_WORKFLOW.md`
- The user may later provide manual raw-to-centerline pairings.

Important files from that thread:

- `data_work/outputs/match_projection_complete_v7/gis_exports/manual_override_review_v7/raw_split_unmatched_v7.shp`
- `data_work/outputs/match_projection_complete_v7/metrics/manual_override_template.csv`

## Paper assets already generated

Standalone figure/table generation was implemented without changing the production pipeline.

Generated outputs:

- `Documents/todo_outputs/figs/figure1_spatial_distribution_residents_jobs.png`
- `Documents/todo_outputs/figs/figure2_average_commuting_speed_by_hour.png`
- `Documents/todo_outputs/figs/figure3_tidal_commuting_by_hour.png`
- `Documents/todo_outputs/figs/figure4_spatial_distribution_tidal_commuting.png`
- `Documents/todo_outputs/tables/table1_summary_statistics.csv`
- `Documents/todo_outputs/tables/table1_summary_statistics.tex`

Relevant scripts:

- `data_work/src/diagnostics/generate_todo_paper_assets.py`
- `data_work/src/diagnostics/generate_reasonable_travel_costs.py`

Travel-cost update:

- Grid-to-grid AM travel cost was updated from a rough connectivity object to shortest-path travel time on the directed grid graph.
- Output:
  - `Documents/todo_outputs/data/od_square_am_shortest_path_costs.csv`
  - `Documents/todo_outputs/data/od_square_am_shortest_path_summary.csv`

## Structural model work added recently

A new standalone prototype was added under `data_work/src/model/`.

Main files:

- `data_work/src/model/spatial_equilibrium.py`
- `data_work/src/model/run_spatial_equilibrium.py`
- `data_work/src/model/run_counterfactual_suite.py`
- `data_work/docs/STRUCTURAL_MODEL_WORKFLOW.md`

Current functionality:

- load QSM-style node / edge / OD inputs
- compute directed shortest-path commuting costs
- gravity-style diagnostic estimation for `theta`
- weak proxy calibration hook for `lambda`
- invert baseline fundamentals `ubar_theta` and `abar_theta`
- solve a prototype general equilibrium with congestion feedback
- run counterfactuals for:
  - symmetric vs asymmetric commuting costs
  - top-N congestion-based lane reallocations
  - top-N tidal-asymmetry-based lane reallocations

Current output directories:

- `data_work/outputs/raw_rebuild_validation/model_square_baseline`
- `data_work/outputs/raw_rebuild_validation/model_square_suite`

Important result files:

- `data_work/outputs/raw_rebuild_validation/model_square_baseline/equilibrium_summary.csv`
- `data_work/outputs/raw_rebuild_validation/model_square_baseline/calibration_summary.json`
- `data_work/outputs/raw_rebuild_validation/model_square_suite/counterfactual_suite_summary.csv`
- `data_work/outputs/raw_rebuild_validation/model_square_suite/figures/counterfactual_sensitivity_summary.png`
- `data_work/outputs/raw_rebuild_validation/model_square_suite/figures/counterfactual_policy_welfare_by_rule.png`

## Structural model caveats

This part is **working code but not final paper-ready code**.

Current approximations:

- commuting costs use shortest-path travel time, not the full route aggregator from the TeX notes
- route assignment is all-or-nothing on shortest paths
- congestion updates are anchored to observed edge times:
  - `t_cf = t_obs * [ (phi_cf / n_cf) / (phi_obs / n_obs) ] ^ lambda`

Current unresolved identification issues:

- the gravity-implied `theta` estimate is far from the externally calibrated benchmark
- `lambda` is only weakly proxied, not formally identified
- the equilibrium outer loop can run stably but often does not fully converge under short iteration caps

Interpretation:

- The current model outputs are useful for workflow development and directional comparisons.
- They should not yet be treated as final publishable welfare estimates.

## Most important unresolved problems

1. Matching / split logic in the urban core still misses some node-based splits.
2. Manual override integration for unresolved unmatched segments is prepared but not yet used.
3. Structural model still needs:
   - better route aggregator
   - smoother route shares
   - stronger `lambda` identification
   - cleaner treatment-edge definition for tidal-lane policies
4. Counterfactual welfare numbers are still prototype-level.

## How to restart work quickly

If the next Codex session needs to work immediately, these are the first files to read:

1. `data_work/docs/CODEX_HANDOFF_NOTE.md`
2. `data_work/docs/REPRODUCIBILITY_SNAPSHOT.md`
3. `data_work/docs/QUICK_RERUN_CHEATSHEET.md`
4. `data_work/docs/STRUCTURAL_MODEL_WORKFLOW.md`
5. `data_work/src/stages/README.md`

## Most useful commands

Regenerate paper assets:

```bash
python data_work/src/diagnostics/generate_todo_paper_assets.py \
  --version-id raw_rebuild_validation \
  --output-dir data_work/outputs \
  --paper-dir Documents/todo_outputs
```

Regenerate shortest-path travel-cost outputs:

```bash
python data_work/src/diagnostics/generate_reasonable_travel_costs.py \
  --version-id raw_rebuild_validation \
  --output-dir data_work/outputs \
  --paper-dir Documents/todo_outputs
```

Run the structural baseline:

```bash
python data_work/src/model/run_spatial_equilibrium.py \
  --version-id raw_rebuild_validation \
  --output-dir data_work/outputs \
  --grid-type square \
  --top-n 5
```

Run the structural sensitivity suite:

```bash
python data_work/src/model/run_counterfactual_suite.py \
  --version-id raw_rebuild_validation \
  --output-dir data_work/outputs \
  --grid-type square \
  --lambda-list 0.05,0.15,0.30 \
  --topn-list 5,10 \
  --max-iter 15
```

Plot structural counterfactual summaries:

```bash
python data_work/src/diagnostics/plot_model_counterfactuals.py \
  --version-id raw_rebuild_validation \
  --output-dir data_work/outputs \
  --model-subdir model_square_suite
```

## Environment notes

- The codebase is Python-based and relies mainly on:
  - `pandas`
  - `geopandas`
  - `numpy`
  - `scipy`
  - `shapely`
  - `matplotlib`
  - `rasterio`
  - `scikit-image`
  - `h3`
- There is no fully maintained environment file yet.
- A new machine will likely need manual package installation based on imports.

## Recommended instruction to a new Codex session

When starting on a new machine, tell Codex:

- read `data_work/docs/CODEX_HANDOFF_NOTE.md` first
- treat `raw_rebuild_validation` as the main baseline
- treat `match_projection_complete_v7` as the best current matching experiment
- do not overwrite the stage pipeline casually
- structural-model outputs are prototype-level, not final paper estimates
