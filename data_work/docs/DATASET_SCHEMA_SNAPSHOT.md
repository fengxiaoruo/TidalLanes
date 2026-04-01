# Dataset Schema Snapshot

This document summarizes the intermediate datasets used in the current working pipeline, based on notebook code and existing markdown summaries only.

Scope:
- Core pipeline intermediate datasets only
- Columns
- Units
- Spatial reference
- Primary keys

Notes:
- This is a schema snapshot inferred from code, not a row-level validation of saved files.
- Some tables preserve all columns from upstream raw data. In those cases, only the columns explicitly used or created in code are listed with certainty.
- CRS is only relevant for datasets with geometry.

## 1. `interim_data/gis/step5_centerline_edges.parquet`

Source: `code/01_Match_Final.ipynb`

| Field | Description | Units |
|---|---|---|
| `cline_id` | Undirected centerline ID | integer |
| `geometry` | Centerline geometry | line geometry |

- Spatial reference: `EPSG:3857`
- Primary key: `cline_id`

Notes:
- The notebook computes `len_m` temporarily for pruning but drops it before saving.

## 2. `interim_data/gis/step5_centerline_edges_dir.parquet`

Source: `code/01_Match_Final.ipynb`

| Field | Description | Units |
|---|---|---|
| `cline_id` | Undirected centerline ID | integer |
| `dir` | Direction label | `AB` / `BA` |
| `geometry` | Directed centerline geometry | line geometry |
| `skel_dir` | Directed centerline ID | integer |
| `bear` | Bearing of line geometry | degrees |

- Spatial reference: `EPSG:3857`
- Primary key: `skel_dir`
- Natural key: (`cline_id`, `dir`)

## 3. `interim_data/gis/xwalk_raw_to_split.parquet`

Source: `code/01_Match_Final.ipynb`

| Field | Description | Units |
|---|---|---|
| `raw_edge_id` | Raw road segment ID | integer |
| `split_id` | Split-segment ID | integer |
| `raw_seg_idx` | Within-road split sequence | integer |

- Spatial reference: `N/A`
- Primary key: `split_id`

## 4. `interim_data/gis/xwalk_split_to_centerline.parquet`

Source: `code/01_Match_Final.ipynb`

| Field | Description | Units |
|---|---|---|
| `split_id` | Split-segment ID | integer |
| `raw_edge_id` | Raw road segment ID | integer |
| `matched` | Match indicator | 0/1 |
| `skel_dir` | Directed centerline ID | integer |
| `cline_id` | Undirected centerline ID | integer |
| `dir` | Matched direction | `AB` / `BA` |
| `angle_diff` | Angular difference between segment and candidate centerline | degrees |
| `dist_mean` | Mean perpendicular distance to matched centerline | meters |
| `s_from` | Start projection along matched centerline | meters |
| `s_to` | End projection along matched centerline | meters |

- Spatial reference: `N/A`
- Primary key: `split_id`

## 5. `interim_data/gis/raw_split_centerline.parquet`

Source: `code/01_Match_Final.ipynb`

Guaranteed columns from code:

| Field | Description | Units |
|---|---|---|
| `raw_edge_id` | Raw road segment ID | integer |
| `raw_seg_idx` | Within-road split sequence | integer |
| `split_id` | Split-segment ID | integer |
| `geometry` | Split segment geometry | line geometry |
| `raw_geometry` | Original raw road geometry | line geometry |
| `matched` | Match indicator | 0/1 |
| `skel_dir` | Directed centerline ID | integer |
| `cline_id` | Undirected centerline ID | integer |
| `dir` | Matched direction | `AB` / `BA` |
| `angle_diff` | Angular mismatch | degrees |
| `dist_mean` | Mean perpendicular distance | meters |
| `s_from` | Start projection on centerline | meters |
| `s_to` | End projection on centerline | meters |

Likely preserved upstream/raw columns:
- Raw-road attributes retained from the source roads file
- Direction helper columns created earlier in the notebook, such as `dir_deg`, `dir_deg_sem`, `bear_geom`, `dir_source`, `need_split`
- The downstream speed-matching notebook also expects `roadseg_id` to exist

- Spatial reference: `EPSG:3857`
- Primary key: `split_id`

## 6. `interim_data/asym/cl_speed_by_time_for_asym.parquet`

Source: `code/02_Centerline_Match_Speed.ipynb`

| Field | Description | Units |
|---|---|---|
| `skel_dir` | Directed centerline ID | integer |
| `cline_id` | Undirected centerline ID | integer |
| `dir` | Direction | `AB` / `BA` |
| `weekday_label` | Weekday grouping | string |
| `hour_of_day` | Hour of day | 0-23 |
| `cl_speed_time` | Time-weighted centerline speed | km/h |
| `is_weekday` | Weekday flag | boolean |
| `cl_len_m` | Centerline length | meters |
| `is_am_peak` | AM peak flag | boolean |
| `is_pm_peak` | PM peak flag | boolean |

- Spatial reference: `N/A`
- Primary key: (`skel_dir`, `weekday_label`, `hour_of_day`)
- Natural key: (`skel_dir`, `cline_id`, `dir`, `weekday_label`, `hour_of_day`)

## 7. `interim_data/asym/cl_dir_with_length.parquet`

Source: `code/02_Centerline_Match_Speed.ipynb`

| Field | Description | Units |
|---|---|---|
| `cline_id` | Undirected centerline ID | integer |
| `dir` | Direction | `AB` / `BA` |
| `geometry` | Directed centerline geometry | line geometry |
| `skel_dir` | Directed centerline ID | integer |
| `bear` | Bearing | degrees |
| `cl_len_m` | Centerline length | meters |

- Spatial reference: `EPSG:3857`
- Primary key: `skel_dir`
- Natural key: (`cline_id`, `dir`)

## 8. `interim_data/gis/grid_square_3km.parquet`
## 9. `interim_data/gis/grid_hex_3km.parquet`
## 10. `interim_data/gis/grid_voronoi_3km.parquet`

Source: `code/03_GridConstruct.ipynb`

Common schema:

| Field | Description | Units |
|---|---|---|
| `grid_id` | Grid cell ID | string |
| `area_km2` | Cell area | square kilometers |
| `geometry` | Grid polygon geometry | polygon geometry |

- Spatial reference: `EPSG:4326`
- Primary key: `grid_id`

Notes:
- `grid_id` format depends on grid type.
- `code/04_GirdlevelData.ipynb` reprojects these grids to `EPSG:3857` before spatial operations.

## 11. `interim_data/gis/cl_speed_peak_geo.parquet`

Source: `code/04_GirdlevelData.ipynb`

| Field | Description | Units |
|---|---|---|
| `cline_id` | Undirected centerline ID | integer |
| `dir` | Direction | `AB` / `BA` |
| `geometry` | Directed centerline geometry | line geometry |
| `skel_dir` | Directed centerline ID | integer |
| `bear` | Bearing | degrees |
| `period` | Peak period | `AM` / `PM` |
| `cl_speed_kmh` | Peak average speed | km/h |

- Spatial reference: `EPSG:3857`
- Primary key: (`skel_dir`, `period`)
- Natural key: (`cline_id`, `dir`, `period`)

## 12. `interim_data/gis/grid_links_{suffix}_long.csv`

Source: `code/04_GirdlevelData.ipynb`

| Field | Description | Units |
|---|---|---|
| `period` | Peak period | `AM` / `PM` |
| `skel_dir` | Directed centerline ID | integer |
| `cline_id` | Undirected centerline ID | integer |
| `dir` | Direction | `AB` / `BA` |
| `grid_o` | Origin grid ID | string |
| `grid_d` | Destination grid ID | string |
| `len_m` | Link segment length inside origin grid | meters |
| `tt_s` | Travel time for segment | seconds |
| `v_kmh` | Speed used for segment | km/h |

- Spatial reference: `N/A`
- Primary key: no strict single-column key; operationally a many-row detail table

## 13. `interim_data/gis/grid_links_{suffix}_agg.csv`

Source: `code/04_GirdlevelData.ipynb`

| Field | Description | Units |
|---|---|---|
| `period` | Peak period | `AM` / `PM` |
| `grid_o` | Origin grid ID | string |
| `grid_d` | Destination grid ID | string |
| `n_parts` | Number of centerline parts aggregated | count |
| `total_len_km` | Total length | km |
| `total_tt_min` | Total travel time | minutes |
| `v_len_w_kmh` | Length-weighted mean speed | km/h |
| `v_harm_kmh` | Harmonic mean speed | km/h |

- Spatial reference: `N/A`
- Primary key: (`period`, `grid_o`, `grid_d`)

## 14. `interim_data/gis/grid_{suffix}_within_stats.csv`

Source: `code/04_GirdlevelData.ipynb`

| Field | Description | Units |
|---|---|---|
| `grid` | Grid system label | string |
| `segment_total` | Total directed centerline segments | count |
| `segment_within` | Segments fully inside one grid cell | count |
| `segment_within_share` | Share of segments fully inside one grid cell | proportion |
| `len_total_km` | Total centerline length | km |
| `len_within_km` | Length fully inside one grid cell | km |
| `len_within_share` | Share of length fully inside one grid cell | proportion |

- Spatial reference: `N/A`
- Primary key: `grid`

## 15. `interim_data/gis/t_nodes_{suffix}.csv`

Source: `code/04_GirdlevelData.ipynb`

| Field | Description | Units |
|---|---|---|
| `grid_id` | Grid cell ID | string |
| `node_i` | Integer node index | integer |

- Spatial reference: `N/A`
- Primary key: `grid_id`
- Alternate key: `node_i`

## 16. `interim_data/gis/t_edges_{suffix}_AM.csv`
## 17. `interim_data/gis/t_edges_{suffix}_PM.csv`

Source: `code/04_GirdlevelData.ipynb`

| Field | Description | Units |
|---|---|---|
| `grid_o` | Origin grid ID | string |
| `grid_d` | Destination grid ID | string |
| `i` | Origin node index | integer |
| `j` | Destination node index | integer |
| `t_min` | Travel time | minutes |

- Spatial reference: `N/A`
- Primary key: (`grid_o`, `grid_d`)

## 18. `interim_data/gis/commute_{suffix}_matched.csv`

Source: `code/04_GirdlevelData.ipynb`

Guaranteed downstream fields:

| Field | Description | Units |
|---|---|---|
| `home_grid` | Matched home grid ID | string |
| `work_grid` | Matched work grid ID | string |
| `pop` | Population / weight | persons or weighted count |
| `type_walk` | Walk trips/persons | count |
| `type_bike` | Bike trips/persons | count |
| `type_sub` | Subway trips/persons | count |
| `type_bus` | Bus trips/persons | count |
| `type_car` | Car trips/persons | count |

Also preserved:
- Original columns from `raw_data/commute_202211.csv`
- Coordinate columns such as `home_x`, `home_y`, `work_x`, `work_y`

- Spatial reference: `N/A`
- Primary key: not explicitly defined in code

## 19. `interim_data/gis/commute_mode_share_summary.csv`

Source: `code/04_GirdlevelData.ipynb`

Current working fields:

| Field | Description | Units |
|---|---|---|
| `mode` | Travel mode label | string |
| `count` | Total count for mode | persons or weighted count |
| `share_in_identified_pct` | Share among identified trips | percent |
| `share_in_pop_pct` | Share relative to total population | percent |

Possible additional columns in one notebook variant:
- `share_in_identified`
- `share_in_pop`

- Spatial reference: `N/A`
- Primary key: `mode`

## 20. `interim_data/gis/tidal_asymmetry_summary_{suffix}_ratio.csv`

Source: `code/04_GirdlevelData.ipynb`

| Field | Description | Units |
|---|---|---|
| `period` | Summary row identifier | `AM`, `PM`, `AM_vs_PM` |
| `pairs_with_both_dirs` | Number of undirected pairs with both directions observed | count |
| `median_ratio` | Median speed ratio | ratio |
| `p10_ratio` | 10th percentile speed ratio | ratio |
| `share_ratio_lt_0.9` | Share with ratio below 0.9 | proportion |
| `share_ratio_lt_0.8` | Share with ratio below 0.8 | proportion |
| `pairs_with_both_periods` | Number of pairs observed in both AM and PM | count |
| `reversal_share` | Share of pairs with AM/PM reversal | proportion |

- Spatial reference: `N/A`
- Primary key: `period`

Notes:
- Not every row uses every column. `AM_vs_PM` uses the cross-period fields.

## 21. `interim_data/gis/OD_{suffix}.csv`

Source: `code/04_GirdlevelData.ipynb`

| Field | Description | Units |
|---|---|---|
| `home_grid` | Origin grid ID | string |
| `work_grid` | Destination grid ID | string |
| `pop` | Total population | persons or weighted count |
| `type_walk` | Walk count | count |
| `type_bike` | Bike count | count |
| `type_sub` | Subway count | count |
| `type_bus` | Bus count | count |
| `type_car` | Car count | count |
| `identified` | Sum of identified mode counts | count |
| `identified_share_of_pop` | Identified share of total population | proportion |

- Spatial reference: `N/A`
- Primary key: (`home_grid`, `work_grid`)

## 22. `interim_data/gis/grid_nodes_{suffix}.csv`

Source: `code/04_GirdlevelData.ipynb`

| Field | Description | Units |
|---|---|---|
| `grid_id` | Grid cell ID | string |
| `node_i` | Integer node index | integer |
| `component` | Weakly connected component ID | integer |

- Spatial reference: `N/A`
- Primary key: `grid_id`
- Alternate key: `node_i`

## 23. `interim_data/gis/OD_{suffix}_reachable_AM.csv`

Source: `code/04_GirdlevelData.ipynb`

Fields inherited from `OD_{suffix}.csv`, plus:

| Field | Description | Units |
|---|---|---|
| `home_i` | Origin node index | integer |
| `work_i` | Destination node index | integer |

- Spatial reference: `N/A`
- Primary key: (`home_grid`, `work_grid`)

Notes:
- `comp_home` and `comp_work` are temporary internal fields and are dropped before export.

## 24. `interim_data/gis/network_{suffix}_AM_summary.csv`

Source: `code/04_GirdlevelData.ipynb`

| Field | Description | Units |
|---|---|---|
| `nodes` | Number of nodes in graph | count |
| `edges` | Number of usable edges | count |
| `OD_pairs_all` | Total OD pairs | count |
| `OD_pairs_reachable` | Reachable OD pairs | count |
| `reachable_share_pairs` | Reachable OD share | proportion |
| `pop_all` | Total population in OD table | persons or weighted count |
| `pop_reachable` | Population in reachable OD pairs | persons or weighted count |
| `reachable_share_pop` | Reachable population share | proportion |

- Spatial reference: `N/A`
- Primary key: single-row summary, no practical key

## 25. `interim_data/gis/grid_residents_{suffix}.csv`

Source: `code/04_GirdlevelData.ipynb`

| Field | Description | Units |
|---|---|---|
| `grid_id` | Grid cell ID | string |
| `residents` | Residential population | persons or weighted count |

- Spatial reference: `N/A`
- Primary key: `grid_id`

## 26. `interim_data/gis/grid_jobs_{suffix}.csv`

Source: `code/04_GirdlevelData.ipynb`

| Field | Description | Units |
|---|---|---|
| `grid_id` | Grid cell ID | string |
| `jobs` | Workplace population | persons or weighted count |

- Spatial reference: `N/A`
- Primary key: `grid_id`

## 27. `interim_data/gis/grid_population_summary_{suffix}.csv`

Source: `code/04_GirdlevelData.ipynb`

| Field | Description | Units |
|---|---|---|
| `grid_id` | Grid cell ID | string |
| `residents` | Residential population | persons or weighted count |
| `jobs` | Workplace population | persons or weighted count |
| `job_resident_ratio` | Jobs-to-residents ratio | ratio |

- Spatial reference: `N/A`
- Primary key: `grid_id`

## Key Join Fields

The most important join keys in the current pipeline are:

- `raw_edge_id`
- `split_id`
- `cline_id`
- `skel_dir`
- `dir`
- `grid_id`
- `home_grid`
- `work_grid`
- `node_i`

Critical relationships:

- `step5_centerline_edges_dir.parquet`: `skel_dir` is unique
- `cl_speed_by_time_for_asym.parquet`: keyed by centerline-direction-time
- `grid_links_{suffix}_agg.csv`: keyed by (`period`, `grid_o`, `grid_d`)
- `OD_{suffix}.csv`: keyed by (`home_grid`, `work_grid`)
