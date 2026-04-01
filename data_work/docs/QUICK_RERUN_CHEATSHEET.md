# Quick Rerun Cheatsheet

This is the short version.

Use this when you already know what you changed and only want the command.

## Default Assumptions

Run from repo root:

```bash
/Users/fxr/Desktop/TidalLanes
```

Default version:

```bash
raw_rebuild_validation
```

Safer test version example:

```bash
raw_rebuild_validation_test1
```

## Most Useful Commands

Full pipeline:

```bash
python data_work/src/stages/run_full_pipeline.py --version-id raw_rebuild_validation --output-dir data_work/outputs
```

From one stage to the end:

```bash
python data_work/src/stages/run_full_pipeline.py --version-id raw_rebuild_validation --output-dir data_work/outputs --from-stage stage02 --to-stage stage10
```

Only figures:

```bash
python data_work/src/stages/stage10_generate_figures.py --version-id raw_rebuild_validation --output-dir data_work/outputs
```

## If You Changed...

### 1. Matching thresholds / matching logic

Edit:
- `data_work/src/stages/stage02_match_raw_to_centerline.py`

Run:

```bash
python data_work/src/stages/run_full_pipeline.py --version-id raw_rebuild_validation --output-dir data_work/outputs --from-stage stage02 --to-stage stage10
```

Typical parameters:
- `MAX_DIST`
- `BASELINE_DIST_CAP`
- `BASELINE_ANGLE_CAP`
- `PROJ_SEARCH_DIST`
- `PROJ_BUF`
- `PROJ_MAX_ANGLE`
- `PROJ_MIN_SHARE`
- `DIST_CAP`

### 2. Only match figures

Edit:
- `data_work/src/diagnostics/plot_match_diagnostics.py`

Run:

```bash
python data_work/src/stages/stage10_generate_figures.py --version-id raw_rebuild_validation --output-dir data_work/outputs --figure-group match
```

### 3. Speed aggregation / AM-PM definition

Edit:
- `data_work/src/stages/stage03_attach_speed.py`

Run:

```bash
python data_work/src/stages/run_full_pipeline.py --version-id raw_rebuild_validation --output-dir data_work/outputs --from-stage stage03 --to-stage stage10
```

### 4. Only speed figures

Run:

```bash
python data_work/src/stages/stage10_generate_figures.py --version-id raw_rebuild_validation --output-dir data_work/outputs --figure-group speed
```

### 5. Asymmetry thresholds / tidal candidate logic

Edit:
- `data_work/src/stages/stage03b_centerline_asymmetry.py`

Run:

```bash
python data_work/src/stages/run_full_pipeline.py --version-id raw_rebuild_validation --output-dir data_work/outputs --from-stage stage03b --to-stage stage10
```

### 6. Only asymmetry figures

Run:

```bash
python data_work/src/stages/stage10_generate_figures.py --version-id raw_rebuild_validation --output-dir data_work/outputs --figure-group asymmetry
```

### 7. Lane assumptions

Edit:
- `data_work/src/stages/stage04_estimate_lanes.py`

Run:

```bash
python data_work/src/stages/run_full_pipeline.py --version-id raw_rebuild_validation --output-dir data_work/outputs --from-stage stage04 --to-stage stage10
```

### 8. Grid construction

Edit:
- `data_work/src/stages/stage05_build_grids.py`

Run:

```bash
python data_work/src/stages/run_full_pipeline.py --version-id raw_rebuild_validation --output-dir data_work/outputs --from-stage stage05 --to-stage stage10
```

Only one grid type downstream:

```bash
python data_work/src/stages/run_full_pipeline.py --version-id raw_rebuild_validation --output-dir data_work/outputs --from-stage stage05 --to-stage stage10 --grid-type voronoi
```

### 9. Grid link / travel time / network edge logic

Edit:
- `data_work/src/stages/stage06_build_grid_links.py`

Run:

```bash
python data_work/src/stages/run_full_pipeline.py --version-id raw_rebuild_validation --output-dir data_work/outputs --from-stage stage06 --to-stage stage10
```

Only one grid type:

```bash
python data_work/src/stages/run_full_pipeline.py --version-id raw_rebuild_validation --output-dir data_work/outputs --from-stage stage06 --to-stage stage10 --grid-type square
```

### 10. OD / residents / jobs / reachability

Edit:
- `data_work/src/stages/stage07_build_od_and_population.py`

Run:

```bash
python data_work/src/stages/run_full_pipeline.py --version-id raw_rebuild_validation --output-dir data_work/outputs --from-stage stage07 --to-stage stage10
```

### 11. QSM export only

Edit:
- `data_work/src/stages/stage08_build_qsm_inputs.py`

Run:

```bash
python data_work/src/stages/run_full_pipeline.py --version-id raw_rebuild_validation --output-dir data_work/outputs --from-stage stage08 --to-stage stage10
```

### 12. Grid figures only

Run all grid figures:

```bash
python data_work/src/stages/stage10_generate_figures.py --version-id raw_rebuild_validation --output-dir data_work/outputs --figure-group grid
```

Run one grid type only:

```bash
python data_work/src/stages/stage10_generate_figures.py --version-id raw_rebuild_validation --output-dir data_work/outputs --figure-group grid --grid-type hex
```

### 13. Version comparison only

Run:

```bash
python data_work/src/stages/stage09_compare_versions.py --output-dir data_work/outputs
```

## Safe Testing Pattern

Do not overwrite baseline if you are experimenting.

Example:

```bash
python data_work/src/stages/run_full_pipeline.py --version-id raw_rebuild_validation_test1 --output-dir data_work/outputs --from-stage stage02 --to-stage stage10
python data_work/src/stages/stage09_compare_versions.py --output-dir data_work/outputs
```

## Output Locations

One version:

- `data_work/outputs/{version_id}/data`
- `data_work/outputs/{version_id}/metrics`
- `data_work/outputs/{version_id}/figures`

Cross-version comparison:

- `data_work/outputs/comparison/across_versions.csv`
- `data_work/outputs/comparison/comparison_report.md`

