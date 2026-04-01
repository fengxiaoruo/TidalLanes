# Pipeline Stages

This directory will hold the production stage scripts that replace notebook-based execution.

Planned stages:

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
12. `run_full_pipeline.py`

Migration status:

- `stage01_build_centerline.py`: raw-build path migrated
  - builds centerline and directed centerline directly from `raw_data`
  - versioned outputs are now the formal upstream for downstream stages
- `stage02_match_raw_to_centerline.py`: raw-build path migrated
  - consumes only stage01 outputs plus raw roads
  - reruns split and baseline matching from scratch
  - writes notebook-style crosswalk tables into the versioned output
- `stage03_attach_speed.py`: baseline logic migrated
  - uses new-framework stage01/stage02 outputs
  - writes `centerline_speed_master.parquet`
- `stage03b_centerline_asymmetry.py`: baseline terminal branch migrated
  - uses new-framework `centerline_speed_master.parquet` and `centerline_dir_master.parquet`
  - writes centerline asymmetry and tidal-candidate tables
- `stage04_estimate_lanes.py`: baseline logic migrated
  - writes `centerline_lane_master.parquet`
- `stage05_build_grids.py`: baseline logic migrated
  - writes square / hex / voronoi grid master tables
- `stage06_build_grid_links.py`: data-generation logic migrated
  - builds grid links and graph edge tables from new-framework outputs
- `stage07_build_od_and_population.py`: data-generation logic migrated
  - builds commute matching, OD, reachability, and population summaries
- `stage08_build_qsm_inputs.py`: baseline export logic migrated
  - writes QSM-ready nodes / edges / OD tables
- `stage09_compare_versions.py`: baseline comparison logic migrated
  - writes per-version summary metrics and cross-version comparison outputs
- `stage10_generate_figures.py`: figure-generation logic migrated
  - calls restored diagnostics plotting scripts under a formal stage entry point
- `run_full_pipeline.py`: unified execution wrapper
  - runs all or part of the stage chain for a `version_id`
