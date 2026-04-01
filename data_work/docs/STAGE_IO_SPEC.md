# Stage I/O Specification

This document defines the initial input/output contract for the first three core stages in the refactor.

Scope:

- `stage01_build_centerline.py`
- `stage02_match_raw_to_centerline.py`
- `stage03_attach_speed.py`

Purpose:

- Freeze the upstream interfaces before migrating logic from notebooks
- Make later method changes easier to isolate
- Support baseline and upgraded versions without breaking downstream stages

## General Design Rules

1. Stage outputs should be stable, explicit, and versioned.
2. Stages should not depend on in-memory notebook state.
3. Rows should be preserved wherever possible; restrictions should be encoded as flags.
4. If a method is upgraded, baseline and upgraded result fields should coexist in the same master table when practical.
5. Geometry outputs should use explicit CRS and not rely on implicit assumptions.

## Stage 01: Build Centerline

File:

- `src/stages/stage01_build_centerline.py`

## Purpose

- Build the undirected centerline network from raw roads
- Build the directed centerline network
- Add structural flags without hard deletion where possible

## Required Inputs

### Raw geometry input

- `raw_data/gis/roads_baidu/beijing_roads.shp`

Expected required fields from the raw roads file:

- road geometry
- any available road identifier such as `roadseg_id`
- any existing semantic direction fields if present
- any road-class variables needed later for lane estimation

## Planned Outputs

### A. `centerline_master.parquet`

Recommended fields:

- `cline_id`
- `geometry`
- `length_m`
- `source_version`
- `is_short_centerline`
- `is_dead_end`
- `is_isolated`
- `is_valid_geometry`
- `keep_baseline`
- `keep_relaxed`

Primary key:

- `cline_id`

CRS:

- `EPSG:3857`

### B. `centerline_dir_master.parquet`

Recommended fields:

- `skel_dir`
- `cline_id`
- `dir`
- `geometry`
- `length_m`
- `bear`
- `direction_role`
- `source_version`
- `is_valid_geometry`
- `keep_baseline`
- `keep_relaxed`

Primary key:

- `skel_dir`

Natural key:

- (`cline_id`, `dir`)

CRS:

- `EPSG:3857`

## Migration Notes

- Current notebook behavior prunes short centerlines before export.
- Refactor target should preserve these records where feasible and replace hard deletion with `is_short_centerline` and `keep_*` flags.
- If exact baseline replication requires a strict filtered output, that should be generated as a derived view rather than replacing the master table.

## Stage 02: Match Raw to Centerline

File:

- `src/stages/stage02_match_raw_to_centerline.py`

## Purpose

- Split raw roads into matchable pieces
- Run baseline matching
- Run fallback projection-based matching on unmatched cases
- Record candidate quality and final chosen match without discarding baseline results

## Required Inputs

### Upstream stage outputs

- `centerline_master.parquet`
- `centerline_dir_master.parquet`

### Raw geometry input

- `raw_data/gis/roads_baidu/beijing_roads.shp`

## Planned Outputs

### A. `raw_segment_master.parquet`

Recommended fields:

- `raw_edge_id`
- `split_id`
- `raw_seg_idx`
- `roadseg_id`
- `geometry`
- `length_m`
- `road_class`
- `road_class_lane_mean`
- `dir_deg_sem`
- `dir_deg_geom`
- `dir_deg_final`
- `dir_source`
- `need_split`
- `is_split`
- `is_short_segment`
- `is_dead_end`
- `is_valid_geometry`
- `keep_baseline`
- `keep_relaxed`

Primary key:

- `split_id`

CRS:

- `EPSG:3857`

### B. `raw_to_centerline_match_master.parquet`

Recommended fields:

- `split_id`
- `raw_edge_id`
- `roadseg_id`

Baseline method fields:

- `matched_old`
- `skel_dir_old`
- `cline_id_old`
- `dir_old`
- `score_old`
- `dist_mean_old`
- `angle_diff_old`
- `candidate_count_old`

Projection fallback fields:

- `matched_proj`
- `skel_dir_proj`
- `cline_id_proj`
- `dir_proj`
- `score_proj`
- `proj_overlap_area`
- `proj_overlap_share`
- `candidate_count_proj`

Final chosen match fields:

- `match_method_final`
- `matched_final`
- `skel_dir_final`
- `cline_id_final`
- `dir_final`
- `score_final`
- `review_flag`
- `match_conflict_flag`
- `s_from`
- `s_to`

Primary key:

- `split_id`

## Migration Notes

- Current notebook writes:
  - `xwalk_raw_to_split.parquet`
  - `xwalk_split_to_centerline.parquet`
  - `raw_split_centerline.parquet`
- In the refactor these should become normalized master outputs rather than three loosely related exports.
- If needed for backward compatibility, the old-format exports can be produced as compatibility views from the master tables.

## Stage 03: Attach Speed

File:

- `src/stages/stage03_attach_speed.py`

## Purpose

- Attach speed observations to matched centerline directions
- Aggregate to centerline-by-time panel data
- Preserve observation coverage and filtering metadata

## Required Inputs

### Upstream stage outputs

- `raw_segment_master.parquet`
- `raw_to_centerline_match_master.parquet`
- `centerline_dir_master.parquet`

### Raw speed input

- `raw_data/speed_Beijing_all_wgs84.csv`

## Planned Outputs

### A. `centerline_speed_master.parquet`

Recommended fields:

- `skel_dir`
- `cline_id`
- `dir`
- `weekday_label`
- `hour_of_day`
- `period`
- `cl_speed_kmh`
- `cl_len_m`
- `n_obs`
- `is_weekday`
- `is_am_peak`
- `is_pm_peak`
- `has_valid_speed`
- `sample_keep_baseline`
- `sample_keep_relaxed`

Primary key:

- (`skel_dir`, `weekday_label`, `hour_of_day`)

Natural key:

- (`skel_dir`, `cline_id`, `dir`, `weekday_label`, `hour_of_day`)

### Optional derivative outputs

For backward compatibility, the stage may also export:

- a compatibility version of `cl_speed_by_time_for_asym.parquet`
- descriptive speed summary tables

These should be treated as derived outputs, not as the canonical master table.

## Migration Notes

- Current notebook uses `raw_split_centerline.parquet` and matched `roadseg_id` linkage.
- The refactor should make this dependency explicit through `raw_segment_master` and `raw_to_centerline_match_master`.
- If later sample restrictions differ by analysis branch, those restrictions should be encoded through flags rather than generating incompatible upstream tables.

## Dependency Summary

The intended upstream chain after migration is:

1. `stage01_build_centerline.py`
2. `stage02_match_raw_to_centerline.py`
3. `stage03_attach_speed.py`

This is the critical upstream backbone for:

- matching-method comparison
- lane estimation
- centerline asymmetry analysis
- grid-link construction
- QSM input generation

## Immediate Next Implementation Goal

Before migrating code, confirm whether the project wants:

1. exact backward-compatible baseline exports first
2. or direct migration to master-table outputs with compatibility adapters

Recommended approach:

- keep the master-table design as the canonical target
- add compatibility exports only where downstream notebooks still require them
