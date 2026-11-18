# Shenzhen Grid-Level Spatial Asymmetry Evidence

This note records the spatial version of the Shenzhen fishnet1000 directional-asymmetry evidence. The new basemap makes two extensions possible: the grid-pair travel-time matrix can be mapped, and observed grid500 commuting flows can be assigned to fishnet1000 cells through point-in-polygon matching.

## 1. Code And Outputs

Code:

- `data_work/src/shenzhen_gridlevel/generate_gridlevel_spatial_asymmetry.py`

Output root:

- `data_work/outputs/shenzhen_gridlevel_spatial_asymmetry_v1/`

Main raw inputs:

- Travel-time matrix: `data_work/raw_data/shenzhen_gridlevel/fishnet1000_pairtime_hat.csv`
- Fishnet1000 basemap: `data_work/raw_data/shenzhen_gridlevel/shp/shenzhen_fishnet1000_info_combined.shp`
- Observed commuting flow: `data_work/raw_data/resid_work_flow_grid500.dta`

The script does not modify raw inputs.

## 2. Basemap Audit

The retained basemap is `shenzhen_fishnet1000_info_combined.shp`. It has 2,181 polygon records and a one-to-one `TARGET_FID` field from 0 to 2180. The script renames this field to `fid1000`.

The OD travel-time matrix contains 1,711 active fishnet1000 IDs. Every active OD-matrix ID is present in the basemap. The basemap has 470 additional IDs that are not present in the OD matrix, mostly cells without travel-time-matrix support.

Useful basemap fields include:

- `fid1000`: fishnet1000 polygon ID, from `TARGET_FID`.
- `QH_NAME` and `QH_CODE`: street-level administrative labels.
- `Econzone`: economic-zone indicator from the supplied GIS layer.
- `lon` and `lat`: centroid-style coordinate fields supplied in the shapefile.
- `geometry`: fishnet1000 polygon geometry.

No supplied fishnet1000 shapefile contains observed commuter counts or OD-flow fields. `shenzhen_fishnet1000_clip.shp` contains `FS2020` and `FS2024`, but the ArcGIS metadata lineage links those fields to `_tmp_floorspace.gdb\\ZS_2020` and `_tmp_floorspace.gdb\\ZS_2024`. They are retained as floor-space fields, not commuter-flow fields.

## 3. Flow Crosswalk

The observed flow file is at the grid500 level. It includes residence and workplace coordinates:

- `resi_lon_wgs`, `resi_lat_wgs`
- `work_lon_wgs`, `work_lat_wgs`

The script builds a grid500-to-fishnet1000 crosswalk by assigning each unique grid500 residence or workplace coordinate to the fishnet1000 polygon that contains it. Point coverage is complete among nonmissing coordinates:

- Residence grid500 points: 5,637 matched out of 5,637.
- Workplace grid500 points: 5,811 matched out of 5,811.

The retained crosswalk is:

- `data_work/outputs/shenzhen_gridlevel_spatial_asymmetry_v1/data/grid500_to_fishnet1000_crosswalk.csv`

The final OD exposure sample keeps inter-fishnet1000 commuting flows whose residence and workplace cells both appear in the travel-time matrix. This final sample contains 421,190 aggregated fishnet1000 OD pairs and 4,405,107 commuters, equal to 85.4 percent of total observed flow in `resid_work_flow_grid500.dta`.

## 3.1 Stage07 Commute CSV Check

The lower-level source file `data_work/raw_data/commute_shenzhen_202411_for_stage07.csv` was audited as an alternative way to link observed commuting flow to fishnet1000. This file has 3,970,569 rows, total `pop` of 5,155,936, and total `type_car` of 1,494,854. These totals match `resid_work_flow_grid500.dta`.

The file contains `home_plot_id`, `work_plot_id`, home and work coordinates, and `pop`. It does not contain a fishnet1000 ID. All unique `home_plot_id` and `work_plot_id` values are present in `data_work/raw_data/commuting_grid_fishnet500.dta`, so the CSV is consistent with the grid500 source layer.

Direct point-in-polygon assignment from the Stage07 CSV to fishnet1000 gives the following coverage:

- 96.9 percent of total `pop` has both home and work points inside the fishnet1000 basemap.
- 87.8 percent has both assigned fishnet1000 IDs inside the 1,711 active IDs of the pair-time matrix.
- 5.6 percent is internal to the same active fishnet1000 cell.
- 82.2 percent remains as inter-fishnet1000 pairtime exposure.

The retained grid500-aggregate path has slightly higher final exposure coverage, 85.4 percent. The 80 percent range is therefore not caused by failure to read the Stage07 CSV. It comes mainly from the pair-time matrix covering 1,711 of 2,181 basemap cells and from excluding same-cell commutes from directional inter-cell asymmetry.

Audit outputs:

- `data_work/outputs/shenzhen_gridlevel_stage07_commute_link_audit_v1/metrics/stage07_commute_grid1000_link_audit.json`
- `data_work/outputs/shenzhen_gridlevel_stage07_commute_link_audit_v1/metrics/stage07_direct_to_fishnet1000_coverage.csv`
- `data_work/outputs/shenzhen_gridlevel_stage07_commute_link_audit_v1/metrics/stage07_vs_retained_grid500_coverage_comparison.csv`

## 4. Spatial Measures

The script keeps the same core pair-level measures as the unweighted evidence:

```text
symmetry_ratio_time = min(car_time_ij, car_time_ji) / max(car_time_ij, car_time_ji)
slow_over_fast_time = max(car_time_ij, car_time_ji) / min(car_time_ij, car_time_ji)
```

It also introduces a directed exposure measure for actual commuting direction:

```text
directed_log_ratio = log(car_time_residence_to_work / car_time_work_to_residence)
```

Positive values mean the observed commute direction is slower than the reverse direction for the same OD pair. Negative values mean the observed commute direction is faster than the reverse direction.

## 5. Main Results

Unweighted potential grid pairs:

- Median symmetry ratio: 0.879.
- Share with symmetry ratio at or below 0.90: 57.5 percent.
- Share with symmetry ratio at or below 0.83: 35.1 percent.
- Share with symmetry ratio at or below 0.77: 21.7 percent.

Observed-flow weighted commuter exposure:

- Weighted median symmetry ratio: 0.819.
- Weighted share at or below 0.90: 71.6 percent.
- Weighted share at or below 0.83: 53.0 percent.
- Weighted share at or below 0.77: 38.6 percent.
- Weighted mean signed log ratio: 0.0088.
- 38.0 percent of observed commuters travel in a direction at least 10 percent slower than the reverse direction.
- 36.2 percent travel in a direction at least 10 percent faster than the reverse direction.

Car-asset weighted exposure is very close to the observed-flow result:

- Weighted median symmetry ratio: 0.821.
- Weighted share at or below 0.90: 71.2 percent.
- Weighted share at or below 0.83: 52.5 percent.
- Weighted share at or below 0.77: 38.0 percent.

The flow-weighted distribution is more asymmetric than the unweighted all-pair distribution. Actual commuters are disproportionately located on OD pairs with stronger directional cost differences.

## 6. Retained Figures

Coverage and comparison figures:

- `data_work/outputs/shenzhen_gridlevel_spatial_asymmetry_v1/figures/figure_grid1000_pairtime_flow_coverage.png`
- `data_work/outputs/shenzhen_gridlevel_spatial_asymmetry_v1/figures/figure_grid1000_flow_weighted_threshold_comparison.png`

Unweighted spatial evidence:

- `data_work/outputs/shenzhen_gridlevel_spatial_asymmetry_v1/figures/figure_grid1000_unweighted_share_symmetry_le_0_90.png`
- `data_work/outputs/shenzhen_gridlevel_spatial_asymmetry_v1/figures/figure_grid1000_unweighted_outbound_disadvantage.png`

Flow-weighted spatial evidence:

- `data_work/outputs/shenzhen_gridlevel_spatial_asymmetry_v1/figures/figure_grid1000_flow_origin_disadvantage.png`
- `data_work/outputs/shenzhen_gridlevel_spatial_asymmetry_v1/figures/figure_grid1000_flow_workplace_inbound_disadvantage.png`
- `data_work/outputs/shenzhen_gridlevel_spatial_asymmetry_v1/figures/figure_grid1000_origin_flow_volume.png`
- `data_work/outputs/shenzhen_gridlevel_spatial_asymmetry_v1/figures/figure_grid1000_workplace_flow_volume.png`

## 7. Retained Tables

- `data_work/outputs/shenzhen_gridlevel_spatial_asymmetry_v1/metrics/grid1000_basemap_audit.json`
- `data_work/outputs/shenzhen_gridlevel_spatial_asymmetry_v1/metrics/flow_crosswalk_coverage.json`
- `data_work/outputs/shenzhen_gridlevel_spatial_asymmetry_v1/metrics/flow_weighted_asymmetry_summary.csv`
- `data_work/outputs/shenzhen_gridlevel_spatial_asymmetry_v1/metrics/grid1000_cell_metric_summary.csv`
- `data_work/outputs/shenzhen_gridlevel_spatial_asymmetry_v1/metrics/flow_weighted_asymmetry_by_origin_street.csv`
- `data_work/outputs/shenzhen_gridlevel_spatial_asymmetry_v1/metrics/flow_weighted_asymmetry_by_workplace_street.csv`
- `data_work/outputs/shenzhen_gridlevel_spatial_asymmetry_v1/metrics/flow_weighted_asymmetry_by_origin_econzone.csv`
- `data_work/outputs/shenzhen_gridlevel_spatial_asymmetry_v1/metrics/flow_weighted_asymmetry_by_workplace_econzone.csv`
- `data_work/outputs/shenzhen_gridlevel_spatial_asymmetry_v1/metrics/flow_weighted_asymmetry_spatial_groups_all.csv`
- `data_work/outputs/shenzhen_gridlevel_spatial_asymmetry_v1/data/flow_grid1000_od_asymmetry.csv`
- `data_work/outputs/shenzhen_gridlevel_spatial_asymmetry_v1/data/grid1000_cell_metrics.csv`
- `data_work/outputs/shenzhen_gridlevel_spatial_asymmetry_v1/data/grid1000_spatial_metrics.gpkg`

## 8. Spatial Grouping Results

The street-level grouping uses `QH_NAME` and `QH_CODE` from the fishnet1000 polygon basemap. Both origin-street and workplace-street tables contain 57 street groups, and each table sums exactly to the final exposure flow of 4,405,107 commuters.

The largest origin-street groups by observed flow are:

- 西乡街道办事处: 302,026 commuters, weighted median symmetry ratio 0.815, 73.3 percent below 0.90.
- 公明办事处: 290,391 commuters, weighted median symmetry ratio 0.819, 73.0 percent below 0.90.
- 沙井街道办事处: 249,019 commuters, weighted median symmetry ratio 0.835, 68.3 percent below 0.90.
- 观澜街道办事处: 204,269 commuters, weighted median symmetry ratio 0.834, 67.7 percent below 0.90.
- 布吉街道办事处: 189,088 commuters, weighted median symmetry ratio 0.809, 75.4 percent below 0.90.

The Econzone split delivers a compact spatial heterogeneity check:

- Origin Econzone 0: 3,438,957 commuters, weighted median symmetry ratio 0.823, 70.8 percent below 0.90.
- Origin Econzone 1: 966,150 commuters, weighted median symmetry ratio 0.801, 74.2 percent below 0.90.
- Workplace Econzone 0: 3,025,298 commuters, weighted median symmetry ratio 0.824, 70.6 percent below 0.90.
- Workplace Econzone 1: 1,379,809 commuters, weighted median symmetry ratio 0.805, 73.7 percent below 0.90.

Econzone 1 commuters are more exposed to directional asymmetry under both residence and workplace grouping. This is a descriptive spatial pattern, not a causal estimate.

## 9. Interpretation For The Paper

The grid-level evidence now speaks to two claims. First, directional travel-time asymmetry is visible inside a single city at the OD-grid level. Second, this is not only a property of hypothetical OD pairs: observed commuter flows are more exposed to directional asymmetry than the unweighted all-pair distribution implies.

This matters for symmetric commuting-cost models because flow-weighted exposure is the object that enters sorting, workplace choice, and residence-market access. A symmetric model can absorb average bilateral cost levels, but it cannot represent which side of an OD pair bears the higher commute cost. That missing direction is especially relevant when the research question concerns residential sorting, workplace access, and place-based transport policy.

## 10. Remaining Limits

The flow assignment is spatial, not an official provider crosswalk. It relies on grid500 coordinate fields in `resid_work_flow_grid500.dta` and fishnet1000 polygon containment. This is appropriate for measurement diagnostics and paper evidence, but an official ID crosswalk would still be preferable for final production.

The travel-time matrix is static. The new maps document spatial asymmetry and flow exposure, not AM versus PM dynamics or policy timing. Segment-level speed panels remain necessary for peak-period and tidal-lane policy tests.
