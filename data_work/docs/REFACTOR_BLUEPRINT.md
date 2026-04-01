# Refactor Blueprint

This document defines the target refactor structure for the project.

## Goals

- Make the pipeline easier to modify and compare across versions
- Separate production logic from exploratory notebooks
- Preserve all observations and encode sample restrictions as flags instead of dropping rows
- Support baseline vs upgraded matching logic, alternative travel-time definitions, and QSM-ready outputs

## Proposed Directory Structure

```text
data_work/
├── configs/
├── src/
│   ├── common/
│   ├── stages/
│   └── diagnostics/
├── notebooks/
├── outputs/
├── analysis/
├── docs/
├── raw_data/
├── interim_data/
└── archive/
```

## Stage Responsibilities

### Stage 01: Build Centerline

- Build undirected centerline
- Build directed centerline
- Add structural flags rather than dropping records

### Stage 02: Match Raw to Centerline

- Split raw segments
- Run baseline matching
- Run projection-based fallback matching for unmatched cases
- Preserve old, fallback, and final chosen match fields

### Stage 03: Attach Speed

- Aggregate speed observations to centerline-by-time records
- Preserve filtering flags and observation counts

### Stage 04: Estimate Lanes

- Map raw road class to lane-count proxy
- Aggregate lane counts to centerline direction level
- Compute two-direction consistency diagnostics

### Stage 05: Build Grids

- Build square, hex, and Voronoi grids
- Export geometry only

### Stage 06: Build Grid Links

- Map centerlines to grid sequences
- Build grid-link parts and aggregated grid-link network tables
- Preserve multiple travel-time definitions in parallel

### Stage 07: Build OD and Population

- Match commute points to grids
- Aggregate OD
- Compute residents/jobs
- Build reachability summaries

### Stage 08: Build QSM Inputs

- Export QSM-ready nodes, edges, OD, and parameter inputs

### Stage 09: Compare Versions

- Compare diagnostics across baseline and upgraded runs

## Recommended Master Tables

### `raw_segment_master`

Key fields:

- `raw_edge_id`
- `split_id`
- `roadseg_id`
- `geometry`
- `length_m`
- `road_class`
- `road_class_lane_mean`
- `is_short_segment`
- `is_dead_end`
- `keep_baseline`
- `keep_relaxed`

### `centerline_master`

Key fields:

- `cline_id`
- `geometry`
- `length_m`
- `is_short_centerline`
- `is_dead_end`
- `keep_baseline`
- `keep_relaxed`

### `centerline_dir_master`

Key fields:

- `skel_dir`
- `cline_id`
- `dir`
- `geometry`
- `length_m`
- `bear`
- `keep_baseline`
- `keep_relaxed`

### `raw_to_centerline_match_master`

Key fields:

- `split_id`
- `raw_edge_id`
- `matched_old`
- `score_old`
- `dist_mean_old`
- `angle_diff_old`
- `matched_proj`
- `score_proj`
- `proj_overlap_area`
- `proj_overlap_share`
- `match_method_final`
- `matched_final`
- `skel_dir_final`
- `cline_id_final`
- `dir_final`
- `review_flag`
- `s_from`
- `s_to`

### `centerline_speed_master`

Key fields:

- `skel_dir`
- `cline_id`
- `dir`
- `weekday_label`
- `hour_of_day`
- `period`
- `cl_speed_kmh`
- `cl_len_m`
- `n_obs`
- `sample_keep_baseline`
- `sample_keep_relaxed`

### `centerline_lane_master`

Key fields:

- `skel_dir`
- `cline_id`
- `dir`
- `lane_est_raw_weighted`
- `lane_est_length_weighted`
- `lane_est_overlap_weighted`
- `n_matched_segments`
- `matched_length_m`
- `opposite_dir_lane_diff`
- `opposite_dir_lane_ratio`
- `lane_symmetry_flag`

### `grid_master`

Key fields:

- `grid_type`
- `grid_id`
- `geometry`
- `area_km2`
- `is_boundary_cell`

### `grid_link_parts_master`

Key fields:

- `grid_type`
- `period`
- `skel_dir`
- `grid_o`
- `grid_d`
- `part_len_m`
- `part_tt_min`
- `part_speed_kmh`
- `lane_est`

### `grid_links_master`

Key fields:

- `grid_type`
- `period`
- `grid_o`
- `grid_d`
- `n_parts`
- `total_len_km`
- `tt_sum_min`
- `tt_weighted_mean_min`
- `tt_len_weighted_mean_min`
- `speed_harm_kmh`
- `speed_len_weighted_kmh`
- `lane_mean`
- `definition_version`

### `od_master`

Key fields:

- `grid_type`
- `home_grid`
- `work_grid`
- `pop`
- `type_walk`
- `type_bike`
- `type_sub`
- `type_bus`
- `type_car`
- `identified`
- `reachable_am`
- `reachable_pm`

### `grid_population_master`

Key fields:

- `grid_type`
- `grid_id`
- `residents`
- `jobs`
- `job_resident_ratio`

## Version Comparison Mechanism

### Output Layout

Each version writes to:

```text
outputs/{version_id}/
├── data/
├── metrics/
├── figures/
└── config_snapshot.yaml
```

### Required Version Metadata

Each run should record:

- `version_id`
- `match_method`
- `sample_definition`
- `travel_time_definition`
- `grid_type`

### Required Summary Metrics

Every version should export a standardized summary file including:

- Split match rate
- Raw-edge match rate
- Matched centerline length share
- Lane symmetry diagnostics
- Speed summary
- Grid connectivity
- Travel-time summary
- QSM coverage counts

### Cross-Version Comparison

Write a shared comparison table such as:

- `comparison/across_versions.csv`

## Notebook Strategy

### Keep as notebooks

- Match diagnostics
- Speed diagnostics
- Grid diagnostics
- Version comparison
- QSM results exploration

### Convert to scripts

- `code/01_Match_Final.ipynb`
- `code/02_Centerline_Match_Speed.ipynb`
- `code/03_GridConstruct.ipynb`
- `code/04_GirdlevelData.ipynb`
- `code/02_2_DefineAsymmetricCenterliens.ipynb` if kept in production flow

### Leave as exploratory / archive

- `code/00_1_Data_description_0911.ipynb`
- `code/00_2_graph_asymetric_flows_commutingflow.ipynb`
- `code/archive/*`

## Refactor Principles

1. Do not drop rows in intermediate tables; add flags instead.
2. Preserve baseline and upgraded methods side by side.
3. Keep each stage focused on one responsibility.
4. Persist version metadata with all final outputs.
5. Separate data outputs, diagnostics, and figures.

## Recommended Implementation Order

1. Establish the new directory structure
2. Freeze current snapshots and schemas
3. Convert deletion logic to flag logic
4. Split notebook production logic into stage scripts
5. Add versioned output directories
6. Upgrade matching logic
7. Add lane estimation
8. Fix grid-link travel-time aggregation
9. Build QSM-ready exports
