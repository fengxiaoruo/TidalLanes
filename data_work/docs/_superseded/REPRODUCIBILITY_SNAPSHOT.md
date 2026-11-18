# Reproducibility Snapshot

This document records the current working version of the project before refactoring.

## Minimal Reproducible Pipelines

### A. Grid-level final results

Required notebook chain:

1. `code/01_Match_Final.ipynb`
2. `code/02_Centerline_Match_Speed.ipynb`
3. `code/03_GridConstruct.ipynb`
4. `code/04_GirdlevelData.ipynb`

This is the main analysis chain for grid-level outputs.

`code/04_GirdlevelData.ipynb` is a merge point. It depends on:

- Centerline geometry from `01_Match_Final.ipynb`
- Centerline speed from `02_Centerline_Match_Speed.ipynb`
- Grid geometry from `03_GridConstruct.ipynb`
- Raw commute OD data

### B. Centerline-level final results

Required notebook chain:

1. `code/01_Match_Final.ipynb`
2. `code/02_Centerline_Match_Speed.ipynb`
3. `code/02_2_DefineAsymmetricCenterliens.ipynb`

This is a parallel terminal branch for centerline-level asymmetry results.

## Required Raw Inputs

- `raw_data/gis/roads_baidu/beijing_roads.shp`
- `raw_data/speed_Beijing_all_wgs84.csv`
- `raw_data/gis/map/北京市边界.shp`
- `raw_data/commute_202211.csv`

## Required Intermediate Inputs

Produced by `01_Match_Final.ipynb`:

- `interim_data/gis/step5_centerline_edges.parquet`
- `interim_data/gis/step5_centerline_edges_dir.parquet`
- `interim_data/gis/xwalk_raw_to_split.parquet`
- `interim_data/gis/xwalk_split_to_centerline.parquet`
- `interim_data/gis/raw_split_centerline.parquet`

Produced by `02_Centerline_Match_Speed.ipynb`:

- `interim_data/asym/cl_speed_by_time_for_asym.parquet`
- `interim_data/asym/cl_dir_with_length.parquet`

Produced by `03_GridConstruct.ipynb`:

- `interim_data/gis/grid_square_3km.parquet`
- `interim_data/gis/grid_hex_3km.parquet`
- `interim_data/gis/grid_voronoi_3km.parquet`

Note:

`code/04_GirdlevelData.ipynb` currently reads grid parquet paths from `raw_data/gis/map/grid_*_3km.parquet`, but the logical upstream dependency is still the grid-construction stage.

## Final Outputs

### Centerline-level

Produced by `code/02_2_DefineAsymmetricCenterliens.ipynb`:

- `interim_data/asym/asym_table_25km.parquet`
- `interim_data/asym/asym_table_25km.csv`
- `interim_data/asym/asym_summary_25km.csv`
- `interim_data/asym/asym_am_pm_comparison.csv`
- `interim_data/asym/tidal_lane_candidates.csv`
- GIS shapefiles and asymmetry figures

### Grid-level

Produced by `code/04_GirdlevelData.ipynb`:

- `interim_data/gis/cl_speed_peak_geo.parquet`
- `interim_data/gis/grid_links_*_long.csv`
- `interim_data/gis/grid_links_*_agg.csv`
- `interim_data/gis/grid_*_within_stats.csv`
- `interim_data/gis/t_nodes_*.csv`
- `interim_data/gis/t_edges_*_AM.csv`
- `interim_data/gis/t_edges_*_PM.csv`
- `interim_data/gis/commute_*_matched.csv`
- `interim_data/gis/commute_mode_share_summary.csv`
- `interim_data/gis/tidal_asymmetry_summary_*.csv`
- `interim_data/gis/OD_*.csv`
- `interim_data/gis/OD_*_reachable_AM.csv`
- `interim_data/gis/network_*_AM_summary.csv`
- `interim_data/gis/grid_residents_*.csv`
- `interim_data/gis/grid_jobs_*.csv`
- `interim_data/gis/grid_population_summary_*.csv`
- `interim_data/figs/grid_links_*_AM.png`
- `interim_data/figs/grid_links_*_PM.png`
- `interim_data/figs/asym_*`
- `interim_data/figs/tidal_link_intensity_*`
- `interim_data/figs/aa_style_*`

## Workflow Summary

### 1. `01_Match_Final.ipynb`

- Builds centerline network from raw roads
- Constructs directed centerline geometry
- Matches split raw segments to directed centerlines
- Produces the crosswalk tables used downstream

### 2. `02_Centerline_Match_Speed.ipynb`

- Reads speed observations and directed centerlines
- Aggregates speed to centerline-by-time records
- Produces centerline speed panel inputs for asymmetry and grid aggregation

### 3. `03_GridConstruct.ipynb`

- Builds square, hex, and Voronoi grid systems
- Exports grid geometries for downstream grid-level analysis

### 4A. `02_2_DefineAsymmetricCenterliens.ipynb`

- Computes AM/PM directional asymmetry on centerlines
- Produces centerline-level asymmetry outputs

### 4B. `04_GirdlevelData.ipynb`

- Merges centerline geometry, centerline speed, grid geometry, and commute data
- Builds grid-to-grid links
- Produces OD, connectivity, residents/jobs, and grid-level asymmetry outputs

## Dependency Summary

Grid-level main chain:

`01_Match_Final.ipynb -> 02_Centerline_Match_Speed.ipynb -> 03_GridConstruct.ipynb -> 04_GirdlevelData.ipynb`

More precisely:

- `04_GirdlevelData.ipynb` depends on `01`, `02`, and `03`
- `02_2_DefineAsymmetricCenterliens.ipynb` is a separate terminal branch from `01 -> 02`
