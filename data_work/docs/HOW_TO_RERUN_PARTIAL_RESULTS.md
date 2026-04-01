# How To Rerun Partial Results

This note is for practical manual work.

Goal:
- if you change a few hard-coded parameters in one stage
- and only want to regenerate the affected outputs / figures
- what command should you run

## Important First

Most pipeline parameters are still hard-coded inside the stage scripts under:

- `data_work/src/stages/`
- `data_work/src/diagnostics/`

The `configs/` directory is not the main control path yet.

That means the normal workflow is:

1. Edit parameters in a stage script.
2. Rerun that stage and all downstream stages that depend on it.
3. Optionally rerun only the figure stage if only plotting changed.

## Working Directory

All commands below assume you run them from the repo root:

```bash
/Users/fxr/Desktop/TidalLanes
```

Use the current validated version as the baseline:

```bash
match_projection_complete_v1
```

If you do not want to overwrite that version, create a new `version_id` first, for example:

```bash
match_projection_complete_v2
```

## Main Entry Points

Full / partial pipeline:

```bash
python data_work/src/stages/run_full_pipeline.py --version-id raw_rebuild_validation --output-dir data_work/outputs
python data_work/src/stages/run_full_pipeline.py --version-id raw_rebuild_validation --output-dir data_work/outputs --from-stage stage06 --to-stage stage10
```

Single stage:

```bash
python data_work/src/stages/stage02_match_raw_to_centerline.py --version-id raw_rebuild_validation --output-dir data_work/outputs
```

Figures only:

```bash
python data_work/src/stages/stage10_generate_figures.py --version-id raw_rebuild_validation --output-dir data_work/outputs
python data_work/src/stages/stage10_generate_figures.py --version-id raw_rebuild_validation --output-dir data_work/outputs --figure-group match
python data_work/src/stages/stage10_generate_figures.py --version-id raw_rebuild_validation --output-dir data_work/outputs --figure-group grid --grid-type voronoi
```

## Stage Dependency Map

Dependency chain:

`stage01 -> stage02 -> stage03 -> stage03b -> stage04 -> stage05 -> stage06 -> stage07 -> stage08 -> stage09 -> stage10`

Practical meaning:

- if you edit `stage01`, rerun `stage01` through `stage10`
- if you edit `stage02`, rerun `stage02` through `stage10`
- if you edit `stage03`, rerun `stage03` through `stage10`
- if you edit `stage03b`, rerun `stage03b` through `stage10`
- if you edit `stage04`, rerun `stage04` through `stage10`
- if you edit `stage05`, rerun `stage05` through `stage10`
- if you edit `stage06`, rerun `stage06` through `stage10`
- if you edit `stage07`, rerun `stage07` through `stage10`
- if you edit `stage08`, rerun `stage08` through `stage10`
- if you edit `stage09`, rerun `stage09` and optionally `stage10`
- if you edit only plotting code in `src/diagnostics/` or `stage10_generate_figures.py`, rerun only `stage10`

## What Each Stage Controls

### Stage 01: centerline construction

File:
- `data_work/src/stages/stage01_build_centerline.py`

Typical parameters:
- `BUF_WIDTH`
- `RES`
- `MIN_CL_LEN`

Main outputs:
- `centerline_master.parquet`
- `centerline_dir_master.parquet`

If you change this, run:

```bash
python data_work/src/stages/run_full_pipeline.py --version-id raw_rebuild_validation --output-dir data_work/outputs --from-stage stage01 --to-stage stage10
```

### Stage 02: raw-to-centerline matching

File:
- `data_work/src/stages/stage02_match_raw_to_centerline.py`

Typical parameters:
- `MAX_DIST`
- `BASELINE_DIST_CAP`
- `BASELINE_ANGLE_CAP`
- `PROJ_SEARCH_DIST`
- `PROJ_BUF`
- `PROJ_MAX_ANGLE`
- `PROJ_MIN_SHARE`
- `DIST_CAP`
- `MIN_SEG_LEN_BASELINE`
- `ROADTYPE_LANE_MAP`

Main outputs:
- `raw_segment_master.parquet`
- `raw_to_centerline_match_master.parquet`
- `raw_split_centerline.parquet`
- stage02 match metrics

If you change matching rules, run:

```bash
python data_work/src/stages/run_full_pipeline.py --version-id raw_rebuild_validation --output-dir data_work/outputs --from-stage stage02 --to-stage stage10
```

If you only changed match plotting, run:

```bash
python data_work/src/stages/stage10_generate_figures.py --version-id raw_rebuild_validation --output-dir data_work/outputs --figure-group match
```

### Stage 03: speed attachment

File:
- `data_work/src/stages/stage03_attach_speed.py`

Typical parameters:
- time parsing logic
- weekday / AM / PM definitions
- speed aggregation logic

Main outputs:
- `centerline_speed_master.parquet`

If you change this, run:

```bash
python data_work/src/stages/run_full_pipeline.py --version-id raw_rebuild_validation --output-dir data_work/outputs --from-stage stage03 --to-stage stage10
```

If you only changed speed figures, run:

```bash
python data_work/src/stages/stage10_generate_figures.py --version-id raw_rebuild_validation --output-dir data_work/outputs --figure-group speed
```

### Stage 03b: asymmetry / tidal candidate logic

File:
- `data_work/src/stages/stage03b_centerline_asymmetry.py`

Typical parameters:
- `RADIUS_M`
- `MIN_LEN_M`
- `COUNT_MISSING_AS_ASYM`
- asymmetry thresholds in `add_asym_measures`

Main outputs:
- `centerline_asymmetry_table.parquet`
- `tidal_lane_candidates.csv`
- asymmetry summary metrics

If you change this, run:

```bash
python data_work/src/stages/run_full_pipeline.py --version-id raw_rebuild_validation --output-dir data_work/outputs --from-stage stage03b --to-stage stage10
```

If you only changed asymmetry figures, run:

```bash
python data_work/src/stages/stage10_generate_figures.py --version-id raw_rebuild_validation --output-dir data_work/outputs --figure-group asymmetry
```

### Stage 04: lane estimation

File:
- `data_work/src/stages/stage04_estimate_lanes.py`

Typical parameters:
- `LANE_MAP`
- lane weighting logic
- opposite-direction comparison logic

Main outputs:
- `centerline_lane_master.parquet`

If you change this, run:

```bash
python data_work/src/stages/run_full_pipeline.py --version-id raw_rebuild_validation --output-dir data_work/outputs --from-stage stage04 --to-stage stage10
```

### Stage 05: grid construction

File:
- `data_work/src/stages/stage05_build_grids.py`

Typical parameters:
- square cell size
- H3 resolution
- Voronoi seed / spacing / filtering logic

Main outputs:
- `grid_square_master.parquet`
- `grid_hex_master.parquet`
- `grid_voronoi_master.parquet`

If you change grid geometry, run:

```bash
python data_work/src/stages/run_full_pipeline.py --version-id raw_rebuild_validation --output-dir data_work/outputs --from-stage stage05 --to-stage stage10
```

If you want only one grid type downstream:

```bash
python data_work/src/stages/run_full_pipeline.py --version-id raw_rebuild_validation --output-dir data_work/outputs --from-stage stage05 --to-stage stage10 --grid-type voronoi
```

### Stage 06: grid-link network

File:
- `data_work/src/stages/stage06_build_grid_links.py`

Typical parameters:
- link sequence construction
- travel-time aggregation
- edge travel-time definition

Main outputs:
- `grid_links_*_long.csv`
- `grid_links_*_agg.csv`
- `t_nodes_*.csv`
- `t_edges_*_AM.csv`
- `t_edges_*_PM.csv`

If you change this, run:

```bash
python data_work/src/stages/run_full_pipeline.py --version-id raw_rebuild_validation --output-dir data_work/outputs --from-stage stage06 --to-stage stage10
```

Only one grid type:

```bash
python data_work/src/stages/run_full_pipeline.py --version-id raw_rebuild_validation --output-dir data_work/outputs --from-stage stage06 --to-stage stage10 --grid-type square
```

If you only changed grid figures, run:

```bash
python data_work/src/stages/stage10_generate_figures.py --version-id raw_rebuild_validation --output-dir data_work/outputs --figure-group grid --grid-type square
```

### Stage 07: OD / population / reachability

File:
- `data_work/src/stages/stage07_build_od_and_population.py`

Typical parameters:
- point-to-grid matching
- OD aggregation
- reachability definition

Main outputs:
- `commute_*_matched.csv`
- `OD_*.csv`
- `OD_*_reachable_AM.csv`
- `grid_residents_*.csv`
- `grid_jobs_*.csv`
- `grid_population_summary_*.csv`

If you change this, run:

```bash
python data_work/src/stages/run_full_pipeline.py --version-id raw_rebuild_validation --output-dir data_work/outputs --from-stage stage07 --to-stage stage10
```

### Stage 08: QSM export

File:
- `data_work/src/stages/stage08_build_qsm_inputs.py`

Typical parameters:
- which edge time field to export
- sample definition for OD export

Main outputs:
- `qsm_input_nodes_*.parquet`
- `qsm_input_edges_*.parquet`
- `qsm_input_od_*.parquet`
- `qsm_input_parameters_*.json`

If you change this, run:

```bash
python data_work/src/stages/run_full_pipeline.py --version-id raw_rebuild_validation --output-dir data_work/outputs --from-stage stage08 --to-stage stage10
```

### Stage 09: version comparison

File:
- `data_work/src/stages/stage09_compare_versions.py`

Main outputs:
- `outputs/comparison/across_versions.csv`
- `outputs/comparison/comparison_report.md`

If you only want to refresh comparison tables:

```bash
python data_work/src/stages/stage09_compare_versions.py --output-dir data_work/outputs
```

### Stage 10: figures

Files:
- `data_work/src/stages/stage10_generate_figures.py`
- `data_work/src/diagnostics/*.py`

Figure groups:
- `match`
- `speed`
- `asymmetry`
- `grid`
- `all`

Common commands:

```bash
python data_work/src/stages/stage10_generate_figures.py --version-id raw_rebuild_validation --output-dir data_work/outputs --figure-group match
python data_work/src/stages/stage10_generate_figures.py --version-id raw_rebuild_validation --output-dir data_work/outputs --figure-group speed
python data_work/src/stages/stage10_generate_figures.py --version-id raw_rebuild_validation --output-dir data_work/outputs --figure-group asymmetry
python data_work/src/stages/stage10_generate_figures.py --version-id raw_rebuild_validation --output-dir data_work/outputs --figure-group grid --grid-type hex
python data_work/src/stages/stage10_generate_figures.py --version-id raw_rebuild_validation --output-dir data_work/outputs
```

## Fast Scenario Guide

### I changed only the raw-centerline matching thresholds

Edit:
- `stage02_match_raw_to_centerline.py`

Run:

```bash
python data_work/src/stages/run_full_pipeline.py --version-id raw_rebuild_validation --output-dir data_work/outputs --from-stage stage02 --to-stage stage10
```

### I changed only the match figures

Edit:
- `src/diagnostics/plot_match_diagnostics.py`

Run:

```bash
python data_work/src/stages/stage10_generate_figures.py --version-id raw_rebuild_validation --output-dir data_work/outputs --figure-group match
```

### I changed only AM/PM asymmetry thresholds

Edit:
- `stage03b_centerline_asymmetry.py`

Run:

```bash
python data_work/src/stages/run_full_pipeline.py --version-id raw_rebuild_validation --output-dir data_work/outputs --from-stage stage03b --to-stage stage10
```

### I changed only grid construction

Edit:
- `stage05_build_grids.py`

Run:

```bash
python data_work/src/stages/run_full_pipeline.py --version-id raw_rebuild_validation --output-dir data_work/outputs --from-stage stage05 --to-stage stage10
```

### I changed only one grid type downstream

Run:

```bash
python data_work/src/stages/run_full_pipeline.py --version-id raw_rebuild_validation --output-dir data_work/outputs --from-stage stage06 --to-stage stage10 --grid-type voronoi
```

### I changed only QSM export format

Edit:
- `stage08_build_qsm_inputs.py`

Run:

```bash
python data_work/src/stages/run_full_pipeline.py --version-id raw_rebuild_validation --output-dir data_work/outputs --from-stage stage08 --to-stage stage10
```

## Recommended Safe Workflow

If you are testing parameter changes, do this instead of overwriting the baseline run:

```bash
python data_work/src/stages/run_full_pipeline.py --version-id raw_rebuild_validation_test1 --output-dir data_work/outputs --from-stage stage02 --to-stage stage10
python data_work/src/stages/stage09_compare_versions.py --output-dir data_work/outputs
```

This makes it easier to compare the new version with the baseline.

## Where To Check Results

For one version:

- `data_work/outputs/{version_id}/data/`
- `data_work/outputs/{version_id}/metrics/`
- `data_work/outputs/{version_id}/figures/`

For cross-version summary:

- `data_work/outputs/comparison/across_versions.csv`
- `data_work/outputs/comparison/comparison_report.md`

## Current Limitation

The pipeline still depends on hard-coded parameters in Python files.

So the current best way to work is:

- modify the stage script directly
- rerun from that stage downstream
- keep a new `version_id` for each experiment

