# Shenzhen Grid-Level Flow-Weighted Asymmetry: Result And Paper Positioning

## Retained Version

Use the grid500 aggregate path as the retained version.

- Main script: `data_work/src/shenzhen_gridlevel/generate_gridlevel_spatial_asymmetry.py`
- Output root: `data_work/outputs/shenzhen_gridlevel_spatial_asymmetry_v1/`
- Main documentation: `data_work/docs/SHENZHEN_GRIDLEVEL_SPATIAL_ASYMMETRY_20260629.md`
- Main flow input: `data_work/raw_data/resid_work_flow_grid500.dta`
- Travel-time input: `data_work/raw_data/shenzhen_gridlevel/fishnet1000_pairtime_hat.csv`
- Basemap input: `data_work/raw_data/shenzhen_gridlevel/shp/shenzhen_fishnet1000_info_combined.shp`

The retained path assigns grid500 residence and workplace points to fishnet1000 polygons, aggregates observed OD flow to fishnet1000 OD pairs, then merges those pairs to the fishnet1000 bidirectional travel-time matrix. This path has higher final exposure coverage than directly assigning every row of `commute_shenzhen_202411_for_stage07.csv` to fishnet1000.

## Why This Version Is Retained

`commute_shenzhen_202411_for_stage07.csv` is the lower-level source behind the Shenzhen commuting data. It has 3,970,569 rows, total `pop` of 5,155,936, and total `type_car` of about 1,494,854. These totals match the aggregate flow file.

The Stage07 CSV does not contain fishnet1000 IDs. It has `home_plot_id`, `work_plot_id`, and coordinates. All unique `home_plot_id` and `work_plot_id` values are present in `commuting_grid_fishnet500.dta`, so the file is internally consistent with the grid500 source layer.

Directly assigning Stage07 CSV coordinates to fishnet1000 leaves 4,236,377 commuters in the final inter-cell pairtime exposure sample, equal to 82.2 percent of total flow. The retained grid500 aggregate path leaves 4,405,107 commuters, equal to 85.4 percent of total flow. The lower 80 percent range is not a merge failure. It comes from two sample restrictions: the pair-time matrix covers 1,711 active fishnet1000 cells out of 2,181 basemap cells, and same-fishnet1000 internal commutes are excluded from directional inter-cell asymmetry.

Stage07 direct assignment should be kept as QA. The main result should use the grid500 aggregate path.

## Sample And Measurement

The final retained exposure sample has:

- 421,190 fishnet1000 OD pairs.
- 4,405,107 commuters.
- 1,283,836 car-asset-weighted commuters.
- 85.4 percent of total observed flow.
- Complete point-in-polygon matching for the grid500 points used in the retained path: 5,637 of 5,637 residence points and 5,811 of 5,811 workplace points.

The main asymmetry variable is:

```text
symmetry_ratio_time = min(car_time_ij, car_time_ji) / max(car_time_ij, car_time_ji)
```

A lower value means stronger directional asymmetry. A value of 0.80 means the faster direction takes 80 percent of the slower direction's travel time, equivalent to a slow-over-fast ratio of 1.25.

The directed commuter-exposure variable is:

```text
directed_log_ratio = log(car_time_residence_to_work / car_time_work_to_residence)
```

Positive values mean the actual commuting direction is slower than the reverse direction for the same OD pair.

## Main Results

Unweighted potential grid pairs:

- Median symmetry ratio: 0.879.
- Share with symmetry ratio at or below 0.90: 57.5 percent.
- Share with symmetry ratio at or below 0.83: 35.1 percent.
- Share with symmetry ratio at or below 0.77: 21.7 percent.

Observed-flow weighted commuter exposure:

- Weighted median symmetry ratio: 0.819.
- Weighted median slow-over-fast ratio: 1.221.
- Weighted share at or below 0.90: 71.6 percent.
- Weighted share at or below 0.83: 53.0 percent.
- Weighted share at or below 0.77: 38.6 percent.
- 38.0 percent of observed commuters travel in a direction at least 10 percent slower than the reverse direction.
- 36.2 percent travel in a direction at least 10 percent faster than the reverse direction.

Car-asset weighted exposure is close to the observed-flow result:

- Weighted median symmetry ratio: 0.821.
- Weighted share at or below 0.90: 71.2 percent.
- Weighted share at or below 0.83: 52.5 percent.
- Weighted share at or below 0.77: 38.0 percent.

The main empirical takeaway is that actual commuter flows are more exposed to directional travel-time asymmetry than the unweighted all-pair distribution implies. Directional congestion is not only a property of hypothetical OD pairs.

## Spatial Heterogeneity

Street-level grouping:

- Origin-street and workplace-street summaries each retain 57 street groups.
- Each street table sums to 4,405,107 commuters.
- The largest origin-street groups by flow include 西乡街道办事处, 公明办事处, 沙井街道办事处, 观澜街道办事处, and 布吉街道办事处.

Econzone split:

- Origin Econzone 0: 3,438,957 commuters, median symmetry ratio 0.823, 70.8 percent below 0.90.
- Origin Econzone 1: 966,150 commuters, median symmetry ratio 0.801, 74.2 percent below 0.90.
- Workplace Econzone 0: 3,025,298 commuters, median symmetry ratio 0.824, 70.6 percent below 0.90.
- Workplace Econzone 1: 1,379,809 commuters, median symmetry ratio 0.805, 73.7 percent below 0.90.

Econzone 1 commuters have higher exposure to directional asymmetry under both residence and workplace grouping. Treat this as descriptive spatial heterogeneity, not a causal estimate.

## Paper Position

This evidence belongs in the paper as a descriptive measurement fact and a bridge across spatial scales. It should not be presented as a policy effect or as a structural welfare result.

Recommended placement:

1. Introduction: one sentence in the motivation paragraph. The sentence can say that within-city Shenzhen OD flows are heavily exposed to directional travel-time asymmetry, with 71.6 percent of observed commuter flow on pairs whose symmetry ratio is at or below 0.90.
2. Data and Measurement: describe the fishnet1000 travel-time matrix, the fishnet1000 basemap, the grid500-to-fishnet1000 point-in-polygon assignment, and the 85.4 percent final exposure coverage.
3. Descriptive Facts or Motivation section: include one main figure comparing unweighted potential pairs, observed-flow weighted exposure, and car-asset weighted exposure.
4. Model Motivation section: use the result to motivate why a symmetric commuting-cost matrix can miss the directional incidence of commuting frictions. This is especially relevant for sorting, workplace access, residence-market access, and place-based transport policy.
5. Appendix: put the coverage map, origin/workplace disadvantage maps, street/Econzone tables, and Stage07 direct-assignment QA.

The natural paper sequence is:

```text
Township-level national OD evidence -> Shenzhen grid-level flow-weighted OD evidence -> segment/corridor evidence -> model or policy counterfactuals
```

The Shenzhen grid-level result is the middle layer. It is more spatially granular than township OD evidence and more commuter-relevant than segment-only evidence because it attaches directionality to actual OD flows inside one city.

## Slide Placement

Recommended slide sequence:

1. Slide: Directional congestion is not a corridor anomaly.
   - Use the multi-level bridge figure from `shenzhen_gridlevel_asymmetry_evidence_v1`.
   - Takeaway: directionality appears at township, grid, adjacent-grid, and segment layers.

2. Slide: Actual commuters are more exposed than potential pairs.
   - Use `figure_grid1000_flow_weighted_threshold_comparison.png`.
   - Takeaway: unweighted potential pairs understate flow-weighted exposure.

3. Slide: The evidence is spatially grounded.
   - Use `figure_grid1000_pairtime_flow_coverage.png`.
   - Takeaway: the OD-flow exposure is not a black-box merge; the assigned cells cover most observed commuter flow.

4. Backup slide: Spatial heterogeneity.
   - Use origin/workplace disadvantage maps and Econzone split.
   - Takeaway: exposure varies across residence and workplace locations.

5. Backup slide: Sample QA.
   - Use `stage07_vs_retained_grid500_coverage_comparison.csv`.
   - Takeaway: Stage07 direct assignment confirms the result is not a merge artifact, but the retained grid500 aggregate path has higher final coverage.

## What Not To Claim

- Do not claim this identifies the causal effect of a tidal lane.
- Do not claim this captures AM versus PM dynamics. The fishnet1000 travel-time matrix is static.
- Do not claim the flow-weighted result is a welfare result. It is a measurement fact that motivates the directional-cost model.
- Do not claim uncovered flow is missing because of a bad merge. The main losses come from pairtime active-cell coverage and same-cell commuting.

## Figure And Table Paths

Main figure:

- `data_work/outputs/shenzhen_gridlevel_spatial_asymmetry_v1/figures/figure_grid1000_flow_weighted_threshold_comparison.png`

Recommended appendix figures:

- `data_work/outputs/shenzhen_gridlevel_spatial_asymmetry_v1/figures/figure_grid1000_pairtime_flow_coverage.png`
- `data_work/outputs/shenzhen_gridlevel_spatial_asymmetry_v1/figures/figure_grid1000_flow_origin_disadvantage.png`
- `data_work/outputs/shenzhen_gridlevel_spatial_asymmetry_v1/figures/figure_grid1000_flow_workplace_inbound_disadvantage.png`
- `data_work/outputs/shenzhen_gridlevel_spatial_asymmetry_v1/figures/figure_grid1000_origin_flow_volume.png`
- `data_work/outputs/shenzhen_gridlevel_spatial_asymmetry_v1/figures/figure_grid1000_workplace_flow_volume.png`

Main tables:

- `data_work/outputs/shenzhen_gridlevel_spatial_asymmetry_v1/metrics/flow_weighted_asymmetry_summary.csv`
- `data_work/outputs/shenzhen_gridlevel_spatial_asymmetry_v1/metrics/flow_crosswalk_coverage.json`
- `data_work/outputs/shenzhen_gridlevel_stage07_commute_link_audit_v1/metrics/stage07_vs_retained_grid500_coverage_comparison.csv`

Appendix tables:

- `data_work/outputs/shenzhen_gridlevel_spatial_asymmetry_v1/metrics/flow_weighted_asymmetry_by_origin_street.csv`
- `data_work/outputs/shenzhen_gridlevel_spatial_asymmetry_v1/metrics/flow_weighted_asymmetry_by_workplace_street.csv`
- `data_work/outputs/shenzhen_gridlevel_spatial_asymmetry_v1/metrics/flow_weighted_asymmetry_by_origin_econzone.csv`
- `data_work/outputs/shenzhen_gridlevel_spatial_asymmetry_v1/metrics/flow_weighted_asymmetry_by_workplace_econzone.csv`

