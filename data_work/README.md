# TidalLanes Data Work

This directory contains the current script-based working pipeline for the TidalLanes project.

The current validated run is:

- [`outputs/raw_rebuild_validation`](/Users/fxr/Desktop/TidalLanes/data_work/outputs/raw_rebuild_validation)

This version was rerun from `raw_data` under the new stage-based framework and is the current baseline output to keep.

## Current Structure

- [`raw_data/`](/Users/fxr/Desktop/TidalLanes/data_work/raw_data)
  - source datasets used by the pipeline
- [`src/stages/`](/Users/fxr/Desktop/TidalLanes/data_work/src/stages)
  - production pipeline stages
- [`src/common/`](/Users/fxr/Desktop/TidalLanes/data_work/src/common)
  - shared utilities
- [`src/diagnostics/`](/Users/fxr/Desktop/TidalLanes/data_work/src/diagnostics)
  - diagnostics modules
- [`analysis/`](/Users/fxr/Desktop/TidalLanes/data_work/analysis)
  - baseline diagnostics scripts and outputs
- [`configs/`](/Users/fxr/Desktop/TidalLanes/data_work/configs)
  - future run configuration files
- [`docs/`](/Users/fxr/Desktop/TidalLanes/data_work/docs)
  - project snapshots and refactor documentation
- [`notebooks/`](/Users/fxr/Desktop/TidalLanes/data_work/notebooks)
  - reserved for future diagnostic or presentation notebooks
- [`outputs/`](/Users/fxr/Desktop/TidalLanes/data_work/outputs)
  - versioned pipeline outputs

## Active Pipeline

The current main pipeline stages are:

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

These live in [`src/stages/`](/Users/fxr/Desktop/TidalLanes/data_work/src/stages).

Unified execution entry point:

- [`src/stages/run_full_pipeline.py`](/Users/fxr/Desktop/TidalLanes/data_work/src/stages/run_full_pipeline.py)
  - runs the stage chain for a `version_id`
  - supports partial runs with `--from-stage` / `--to-stage`
  - can include or skip figure generation

## Current Baseline Status

The current `raw_rebuild_validation` run has been completed end-to-end.

Key status:

- centerline outputs match the prior working baseline
- raw-to-centerline match rates match the prior working baseline
- speed aggregation matches the prior working baseline
- centerline asymmetry matches the prior working baseline
- square and hex grid results are aligned with the prior working baseline up to minor ID-cleaning differences
- voronoi results now use normalized `grid_id` handling; differences from older notebook outputs reflect previous ID inconsistency in the notebook-era outputs

## Important Notes

- Notebook-based production logic is no longer stored in this directory.
- `outputs/` is versioned by run. Each top-level folder under `outputs/` should represent one retained pipeline run.
- `config_snapshot.stage0x.json` files record the effective parameters and inputs used by each stage for a given run.

## Recommended Entry Points

To understand the current working baseline:

1. Read [`docs/REPRODUCIBILITY_SNAPSHOT.md`](/Users/fxr/Desktop/TidalLanes/data_work/docs/REPRODUCIBILITY_SNAPSHOT.md)
2. Read [`docs/DATASET_SCHEMA_SNAPSHOT.md`](/Users/fxr/Desktop/TidalLanes/data_work/docs/DATASET_SCHEMA_SNAPSHOT.md)
3. Read [`docs/REFACTOR_BLUEPRINT.md`](/Users/fxr/Desktop/TidalLanes/data_work/docs/REFACTOR_BLUEPRINT.md)
4. Inspect [`src/stages/`](/Users/fxr/Desktop/TidalLanes/data_work/src/stages)
5. Inspect [`outputs/raw_rebuild_validation`](/Users/fxr/Desktop/TidalLanes/data_work/outputs/raw_rebuild_validation)

Common commands:

```bash
python src/stages/run_full_pipeline.py --version-id raw_rebuild_validation
python src/stages/stage10_generate_figures.py --version-id raw_rebuild_validation
python src/stages/run_full_pipeline.py --version-id raw_rebuild_validation --from-stage stage06 --to-stage stage10
python src/stages/run_full_pipeline.py --version-id raw_rebuild_validation --skip-figures
python src/stages/stage10_generate_figures.py --version-id raw_rebuild_validation --figure-group match
python src/stages/stage10_generate_figures.py --version-id raw_rebuild_validation --figure-group grid --grid-type voronoi
```

To understand project conventions:

1. Read [`AGENTS.md`](/Users/fxr/Desktop/TidalLanes/data_work/AGENTS.md)

## Current Output Policy

Keep only validated run directories under [`outputs/`](/Users/fxr/Desktop/TidalLanes/data_work/outputs).

At present, the retained validated run is:

- [`outputs/raw_rebuild_validation`](/Users/fxr/Desktop/TidalLanes/data_work/outputs/raw_rebuild_validation)
