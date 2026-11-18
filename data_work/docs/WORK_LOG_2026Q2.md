# Work Log, April to June 2026

Archived from `WORK_LOG.md` on 2026-11-18, newest entries first. Numbers here may be superseded; check the "Current mainline" table in `DOCUMENT_INDEX.md` before quoting them. Newer entries are in [`WORK_LOG.md`](WORK_LOG.md).

## 2026-06-29: Shenzhen fishnet1000 spatial asymmetry and flow exposure retained

- Added `data_work/src/shenzhen_gridlevel/generate_gridlevel_spatial_asymmetry.py`.
- Added `data_work/src/shenzhen_gridlevel/audit_stage07_commute_grid1000_link.py`.
- Added documentation: `data_work/docs/SHENZHEN_GRIDLEVEL_SPATIAL_ASYMMETRY_20260629.md`.
- Added paper/slides positioning note: `Documents/L2_notes/shenzhen_gridlevel_flow_weighted_asymmetry_positioning_20260630.md`.
- Output root: `data_work/outputs/shenzhen_gridlevel_spatial_asymmetry_v1/`.
- Stage07 commute-link audit output root: `data_work/outputs/shenzhen_gridlevel_stage07_commute_link_audit_v1/`.
- Main raw inputs:
  - `data_work/raw_data/shenzhen_gridlevel/fishnet1000_pairtime_hat.csv`;
  - `data_work/raw_data/shenzhen_gridlevel/shp/shenzhen_fishnet1000_info_combined.shp`;
  - `data_work/raw_data/resid_work_flow_grid500.dta`.
- Basemap audit:
  - retained polygon basemap has 2,181 fishnet1000 cells;
  - `TARGET_FID` is a one-to-one ID from 0 to 2180 and is renamed to `fid1000`;
  - all 1,711 active OD-matrix IDs are present in the basemap;
  - the basemap has 470 cells not present in the OD matrix.
- Flow crosswalk:
  - grid500 residence and workplace points are assigned to fishnet1000 polygons by point-in-polygon matching;
  - 5,637 of 5,637 residence points and 5,811 of 5,811 workplace points are matched;
  - the final inter-fishnet1000 exposure sample has 421,190 aggregated OD pairs and 4,405,107 commuters, equal to 85.4% of total observed flow in `resid_work_flow_grid500.dta`.
- Stage07 raw commute CSV audit:
  - `commute_shenzhen_202411_for_stage07.csv` has 3,970,569 rows, total `pop` 5,155,936, and total `type_car` about 1,494,854;
  - all unique `home_plot_id` and `work_plot_id` values appear in `commuting_grid_fishnet500.dta`;
  - direct point-in-polygon assignment from the Stage07 CSV to fishnet1000 leaves 82.2% of total `pop` in the final inter-fishnet1000 pairtime exposure sample;
  - the 80% range is mainly due to the pair-time matrix covering only 1,711 of 2,181 basemap cells and the exclusion of same-fishnet1000 internal commutes, not a failure of the Stage07 CSV link.
- Flow-weighted asymmetry result:
  - unweighted potential grid pairs have median symmetry ratio 0.879 and 57.5% below the 0.90 threshold;
  - observed-flow weighted exposure has median symmetry ratio 0.819 and 71.6% below the 0.90 threshold;
  - car-asset weighted exposure has median symmetry ratio 0.821 and 71.2% below the 0.90 threshold.
- Directed exposure result:
  - observed-flow weighted mean signed log ratio is 0.0088;
  - 38.0% of observed commuters travel in a direction at least 10% slower than the reverse direction;
  - 36.2% travel in a direction at least 10% faster than the reverse direction.
- Spatial grouping result:
  - origin-street and workplace-street summaries each retain 57 street groups and sum to 4,405,107 commuters;
  - origin Econzone 1 has weighted median symmetry ratio 0.801, compared with 0.823 for origin Econzone 0;
  - workplace Econzone 1 has weighted median symmetry ratio 0.805, compared with 0.824 for workplace Econzone 0.
- Retained figures:
  - `figures/figure_grid1000_pairtime_flow_coverage.png`;
  - `figures/figure_grid1000_flow_weighted_threshold_comparison.png`;
  - `figures/figure_grid1000_unweighted_share_symmetry_le_0_90.png`;
  - `figures/figure_grid1000_unweighted_outbound_disadvantage.png`;
  - `figures/figure_grid1000_flow_origin_disadvantage.png`;
  - `figures/figure_grid1000_flow_workplace_inbound_disadvantage.png`;
  - `figures/figure_grid1000_origin_flow_volume.png`;
  - `figures/figure_grid1000_workplace_flow_volume.png`.
- Retained caveat: the grid500-to-fishnet1000 assignment is spatial and uses coordinate containment. It is suitable for diagnostics and paper evidence, but an official provider crosswalk remains preferable for final production.

## 2026-06-29: Beijing OD directional commuting motivation evidence

- Added `data_work/src/motivation/generate_beijing_commuting_direction_evidence.py`.
- Output root: `data_work/outputs/beijing_commuting_direction_evidence_v1/`.
- Inputs are the full stage07 Beijing square-grid OD and population assets from `manual_centerline_rules_v11`, not the model LCC or gate5 sample.
- Retained metrics: flow-weighted mean and median commuting distance, home-to-work direction angle, distance change toward the city job centroid, distance change toward destination job clusters, residence-cluster to job-cluster flow shares, and origin-level inwardness.
- Main road-user weight is car plus bus commuters. The retained summary also reports all commuters, car-only commuters, and mode-identified commuters.
- Main retained pattern: car plus bus commuters have mean grid-centroid distance 10.34 km and point-level mean distance 13.01 km; 64.8% of weighted OD demand ends closer to its destination job cluster than it starts, with a mean distance reduction of 4.68 km. The analogous all-commuter share is 64.2%.
- Retained figures:
  - `figures/figure_beijing_commuting_direction_evidence.{png,pdf,svg}`;
  - `figures/figure_beijing_origin_inwardness_map.{png,pdf,svg}`;
  - `figures/figure_beijing_cluster_flow_matrix.{png,pdf,svg}`;
  - `figures/figure_beijing_story_sleep_work_balance.{png,pdf,svg}`;
  - `figures/figure_beijing_story_top_corridors.{png,pdf,svg}`;
  - `figures/figure_beijing_story_inbound_sector.{png,pdf,svg}`.
- Updated story-style diagnostics after visual review against external reference maps:
  - `figure_beijing_story_sleep_work_balance` now retains only resident and job density panels. The job-weighted center marker is not plotted because it was visually confounded with a data cell.
  - `figure_beijing_story_top_corridors` now plots the dominant grid OD network. `dominant_od_network_top_flows.csv` keeps actual grid OD pairs with centroid distance at least 8 km, retains the stronger AM home-to-work direction within each unordered pair, and draws the top 250 long pairs. Drawn flows carry 4.6k car plus bus commuters, equal to 2.2% of all car plus bus demand and 4.9% of demand at least 8 km; the weighted mean stronger-direction share is 88.0%.
  - `dominant_od_network_summary.csv` reports flow-weighted mean distance 10.34 km, median distance 6.89 km, commuting share at least 30 km of 6.0%, commuting share at least 50 km of 1.3%, and top-20 grid-pair concentration of 0.33% of all car plus bus demand.
  - `story_top_cluster_corridors.csv` is retained as a legacy diagnostic. It restricts the OD network to flows at least 8 km that move closer to destination job clusters, then reports the top 12 cluster corridors. The retained top 12 carry 54.6k car plus bus commuters.
  - `inbound_sector_summary.csv` defines AM inbound demand as home outside 15 km from the job-weighted center and work inside 10 km. This sample has 20.7k inbound car plus bus commuters, versus 7.1k reverse-AM commuters under the mirror definition. The overall inbound-to-reverse ratio is 2.9; the peak sector has 6.0 times the reverse-AM flow and 2.1 times the mean sector inflow.
- Interpretation: the OD structure supplies demand-side motivation for AM inbound and PM outbound directional pressure. The same calculations can be applied to township-level national commuting flows after replacing grid centroids with township centroids and estimating city-specific residence and employment clusters.

## 2026-06-29 Liuxian Gaode late-opening shock analysis

- Added the newly purchased Gaode file `data_work/raw_data/Shenzhen_Gaode/深圳市路段评价20260629111534.csv` to the Liuxian audit.
- The new file covers `2026-04-20` to `2026-04-26`, 16 Gaode segments, 16,128 ten-minute observations, and is complete by the `7 days x 16 segments x 144 ten-minute bins` grid.
- Added a reproducible shock-analysis script:
  - `data_work/src/shenzhen_liuxian_gaode/analyze_liuxian_late_opening_shock.py`.
- Output root:
  - `data_work/outputs/shenzhen_liuxian_gaode_late_opening_shock_v1/`.
- Retained outputs include:
  - file and segment audit tables;
  - 10-minute corridor-speed panel and directional-asymmetry panel;
  - window-level shock DID table;
  - segment-level shock event-study coefficients;
  - segment-level abnormal-speed panel for `2026-04-24`.
- Retained figures are exported as PNG/PDF/SVG:
  - `liuxian_three_week_weekday_diurnal_common14`;
  - `liuxian_shock_week_daily_timeseries_post16`;
  - `liuxian_shock_day_asymmetry_timeseries_post16`;
  - `liuxian_late_opening_window_did`;
  - `liuxian_late_opening_event_study_did`;
  - `liuxian_shock_segment_abnormal_heatmap`.
- Main empirical result: the `2026-04-24` data do not show the clean sign expected from a fully missing `17:30-18:15` opening. The policy direction is extremely slow in the `17:00-17:30` reference window, but improves sharply immediately after `17:30`.
- Against all normal post-policy weekdays from `2026-04-13` to `2026-04-23`, the `17:30-18:10` policy-minus-opposite gap on the shock day is 2.73 km/h higher than normal same-window gaps. Net of the `17:00-17:30` reference window, the DDD is +16.11 km/h.
- The segment-level event study gives positive policy-direction abnormal-speed estimates after `17:30` and remains positive after the closest grid point to `18:15`; the mean coefficients are about -2.92 km/h from `16:30` to `17:20`, +25.90 km/h from `17:30` to `18:10`, and +22.92 km/h from `18:20` to `19:50`.
- Interpretation retained: this is supportive evidence for a capacity/operation effect beginning around the planned `17:30` start or layout process, but it does not support the stricter story that the policy direction stayed untreated until `18:15`. The `18:15` information may correspond to completion of setup, full opening, or complaint timing rather than the first detectable capacity effect.
- Added a two-post-Friday comparison inside the same script, comparing the normal post Friday `2026-04-17` with the late-opening Friday `2026-04-24`.
- New retained outputs:
  - `data/liuxian_two_post_fridays_segment_window_comparison.csv`;
  - `metrics/liuxian_two_post_fridays_window_summary.csv`;
  - `figures/liuxian_two_post_fridays_corridor_timeseries.{png,pdf,svg}`;
  - `figures/liuxian_two_post_fridays_segment_change_1730_1810.{png,pdf,svg}`.
- In the `17:30-18:10` delayed-opening window, the segment-level mean policy-direction speed is 24.77 km/h on `2026-04-17` and 26.90 km/h on `2026-04-24`; 6 of 8 policy-direction segments are faster on the shock Friday. The opposite direction falls from 16.96 km/h to 12.68 km/h; 5 of 8 opposite-direction segments are slower on the shock Friday.
- This Friday-to-Friday comparison strengthens the interpretation that the `2026-04-24` pattern is not a clean missing-operation shock. During the alleged delayed window, it looks more like policy-direction protection plus opposite-direction loss than policy-direction deterioration.
- Added controlled two-Friday DDD specifications for `17:00-18:10`, comparing `17:30-18:10` against `17:00-17:30`:
  - `metrics/liuxian_two_post_fridays_controlled_ddd_specs.csv`;
  - `figures/liuxian_two_post_fridays_controlled_ddd_specs.{png,pdf,svg}`.
- The controlled segment-level DDD estimate is +19.39 km/h in all retained specifications: raw DDD; segment/date/window FE; segment/date/exact-10-minute FE; additional policy-direction-by-minute and shock-day-by-minute controls; and additional segment-specific linear time trends. Segment-clustered diagnostic intervals remain positive, but there are only 16 segment clusters.
- Interpretation retained: adding within-road controls does not turn the `2026-04-24` delayed-window pattern negative. The evidence remains more consistent with early/partial operation or direction-specific protection beginning at `17:30` than with policy-direction deterioration from a missing operation.

## 2026-06-29 Liuxian non-Friday pre/post diagnostics

- Added a non-Friday pre/post diagnostic script:
  - `data_work/src/shenzhen_liuxian_gaode/analyze_liuxian_pre_post_nonfriday.py`.
- The main sample excludes Fridays and weekends, leaving Monday-Thursday observations only. Because the two additional post-only Gaode segments have no pre observations, the pre/post comparison uses the 14 Gaode segments common to all three purchased weeks.
- Retained outputs under `data_work/outputs/shenzhen_liuxian_gaode_late_opening_shock_v1/`:
  - `data/liuxian_nonfriday_common14_corridor_10min_panel.csv`;
  - `data/liuxian_nonfriday_common14_daily_window_speeds.csv`;
  - `metrics/liuxian_nonfriday_common14_window_summary.csv`;
  - `metrics/liuxian_nonfriday_common14_active_reference_summary.csv`;
  - `metrics/liuxian_nonfriday_common14_directional_gap_ddd.csv`;
  - `figures/liuxian_nonfriday_pre_post_diurnal_common14.{png,pdf,svg}`;
  - `figures/liuxian_nonfriday_active_reference_by_direction.{png,pdf,svg}`;
  - `figures/liuxian_nonfriday_directional_gap_ddd.{png,pdf,svg}`.
- Main result: after excluding Fridays, the average pre/post pattern does not show the desired policy-direction improvement. Relative to `15:00-17:30`, the `17:30-20:00` active-window speed ratio declines from 0.835 pre to 0.692 in post week 1 and 0.626 in post week 2 for the policy direction. The opposite direction declines from 0.919 to 0.741 and 0.720.
- Directional-gap DDD using `15:00-17:30` as reference is slightly negative: -1.14 km/h for post week 1, -1.12 km/h for post week 2, and -1.13 km/h pooled across the two post weeks. Positive values would support the desired pattern.
- Interpretation retained: the non-Friday three-week average points away from a clean pre/post policy effect on Liuxian. The stronger evidence in this dataset remains the `2026-04-24` within-day operation/partial-operation pattern, not the average post-vs-pre comparison.
- Added directional speed-ratio diagnostics to the same non-Friday script:
  - `data/liuxian_nonfriday_common14_directional_ratio_10min_panel.csv`;
  - `metrics/liuxian_nonfriday_common14_directional_ratio_window_summary.csv`;
  - `metrics/liuxian_nonfriday_common14_directional_ratio_of_ratios.csv`;
  - `figures/liuxian_nonfriday_directional_speed_ratio_diurnal.{png,pdf,svg}`;
  - `figures/liuxian_nonfriday_directional_ratio_of_ratios.{png,pdf,svg}`.
- Directional-ratio result: the policy direction is generally faster than the opposite direction, but the `17:30-20:00 / 15:00-17:30` ratio-of-ratios is below 1 in all non-Friday weeks: 0.909 in pre, 0.949 in post week 1, and 0.887 in post week 2. Post week 1 is slightly closer to the desired sign, but post week 2 reverses it.
- Including Fridays mechanically makes the post ratio-of-ratios look more supportive because the Friday patterns are special: 1.128 in post week 1 and 1.069 in post week 2, compared with 0.905 in pre. This reinforces the interpretation that the supportive evidence is concentrated in Friday/shock-like days rather than the average Monday-Thursday pre/post pattern.

## 2026-06-29 edgeproj lane lookup fix

- Fixed the lane-count bug in `data_work/src/raw_connected_v1/grid_travel_time/build_raw_connected_v1_grid_travel_time_edgeproj.py`.
- The prior `edgeproj_v2` lane lookup was keyed by numeric `raw_edge_id`, while routed raw-connected edges use `edge_id` values equal to `roadseg_id` strings. This key mismatch caused nearly all path lanes to fall back to the global mean `3.19622`.
- The corrected lookup links raw-connected edges to `raw_segment_master.parquet` by `roadseg_id`. Raw edges inherit `road_class_lane_mean`, with the retained road-class mapping `2 -> 6`, `3 -> 4`, and `4 -> 2`. `raw_edge_id` is kept only as a compatibility alias.
- Edges not linked to a raw segment retain missing lane values. Path-level lane aggregation skips missing lane segments; an explicit `2.0` fallback is used only when a routed path has no usable lane support.
- Added path-lane QA fields to the edgeproj CSV:
  - `lane_path_support_len_m`
  - `lane_path_total_road_len_m`
  - `lane_path_support_share`
  - `lane_path_all_missing_flag`
  - `lane_resolved_source`
- Synchronized the same key and missing-value rule in `build_raw_connected_v1_grid_travel_time_edgeproj_traffic.py`.
- Updated `data_work/src/model/travel_cost_bundle/build_locked_bundle.py` so edgeproj bundles use `lane_est_path_raw_edgeproj_len_weighted`, then `lane_est_path_raw_edgeproj_bottleneck`, then the explicit edgeproj fallback `lane_resolved_for_bpr`. The old bundle lane field is only a legacy fallback for older source CSVs.
- Added lane diagnostics to `data_work/src/model/travel_cost_bundle/qa_model_input_bundle.py`.

Retained rebuilds and QA:

- Rebuilt square AM and square PM edgeproj outputs under `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/`. AM all-grid outputs were refreshed under the final lane rule. PM hex/voronoi remain a low-priority cleanup item because the main model uses square AM.
- Road-edge traceability check after the fix:
  - raw_connected_v1 AM usable road edges: `23,729`
  - road edges matched to `raw_segment_master.roadseg_id`: `23,729`
  - road-edge lane hit share: `100%`
  - road-edge lane distribution: `11,719` two-lane edges, `10,012` four-lane edges, `1,998` six-lane edges
- Split traceability check:
  - `raw_segment_master` rows with `is_split == True`: `5`, all have `roadseg_id`, `road_class`, and `road_class_lane_mean`
  - rows with `need_split == True`: `10,160`, all have `roadseg_id`, `road_class`, and `road_class_lane_mean`
  - split rows with missing lane or raw link: `0`
- Informational check on later raw-connected versions:
  - raw_connected_v2 has `427` directed edges not matched to raw `roadseg_id`; examples are `MANUAL_E...` manual connections.
  - raw_connected_v3 has `2,565` directed edges not matched to raw `roadseg_id`; examples are also `MANUAL_E...` manual connections.
  - This is consistent with the intended rule that pure manual additions are missing for lane aggregation, while raw-traceable split pieces retain the original raw road class.
- Square AM source CSV after the fix:
  - rows: `4,632`
  - finite routed travel times: `3,666`
  - path length-weighted lane rows: `3,664`
  - length-weighted lane unique values after rounding to 6 decimals: `1,625`
  - lane range: `2.0` to `6.0`
  - `lane_resolved_source`: `3,664` path length-weighted, `2` all-missing fallback, `966` missing route
- Rebuilt `model_input_edgeproj_v2_lcc_gate5_v1`.
  - nodes: `1,104`
  - edges: `2,642`
  - `lanes_directional` unique values after rounding to 6 decimals: `1,103`
  - mean `lanes_directional`: `3.1995`
  - lane source counts: `2,641` path length-weighted, `1` explicit edgeproj fallback
  - edge path all-missing lane share: `0.038%`
  - bundle QA passed all required checks.
  - grid-edge lane distribution is no longer degenerate: p10 `2.0`, median `3.366`, p90 `4.0`, p99 `6.0`.
  - rounded grid-edge lane counts cluster at raw-class values and mixtures: `644` edges round to `2.0`, `849` round to `4.0`, and `40` round to `6.0`, with intermediate values generated by length-weighted paths that combine road classes.
  - exactly one retained gate5 grid edge uses the explicit all-missing fallback, `sq_9_48 -> sq_8_48`. Its connectors link to raw-lane road edges, but the decoded route has zero lane-support road length, so this is a path-support artifact rather than an unlinked manual-road lane miss.
- Rebuilt `model_input_edgeproj_v2_lcc_v1` for no-gate robustness.
  - nodes: `1,153`
  - edges: `3,198`
  - `lanes_directional` unique values after rounding to 6 decimals: `1,556`
  - mean `lanes_directional`: `3.2142`
  - lane source counts: `3,197` path length-weighted, `1` explicit edgeproj fallback
  - bundle QA passed all required checks.
- Gate5 source-lock QA passed: bundle `tau_obs_min` and `tau_ff_min` match the edgeproj square AM source CSV exactly, with max absolute difference `0.0` minutes.

Downstream status:

- Existing `directional_cost_edgeproj_v2_lcc_gate5_v1/` and `full_estimation_edgeproj_v2_lcc_gate5_v1/` outputs were produced before this lane fix. They should be rerun before model interpretation or counterfactual comparison.

## 2026-06-28: Remove propagation-scale (`phi`) from Stage 1 directional-cost model

- Dropped `propagation_scale` from `fixed_tau_model.py`; route aggregator is now standard `A_kl = 1/(kappa*tau_kl)`, `T = (I-A)^{-1}`.
- Removed `--propagation-grid`, `network_propagation` simulation, and `simulation_network_propagation.png` from `run_directional_cost.py` / `plot_results.py`.
- `kappa` is now picked only for the baseline route matrix (no inflation to cover `phi=1.25`). On gate5 LCC, `common_kappa = 1.0` because max raw row sum is already below 0.9.
- Updated `FixedDirectedCost_Stage1_gate5.tex` and `DIRECTIONAL_COST_MODEL_20260628.md` accordingly.
- Smoke rerun: `directional_cost_edgeproj_v2_lcc_gate5_v1_reinvert_geometric_v2/` completes with `kappa=1`.

## 2026-06-28: raw_connected_v3 replicated through the v1 edgeproj+LCC+gate5 pipeline; v1 vs v3 compared

- **Goal:** rebuild the connected travel-cost matrix from the updated `raw_connected_v3` network using the exact v1 logic, with added version control + schema validation, then compare to v1.
- **Code (version control, no algorithm change):**
  - `normalize_raw_connected_v2.py`: added `--version-label` (derived filename `raw_connected_<label>_edges_without_speed.csv`) and a fail-fast `validate_input_schema()` (asserts required columns in directed_edges/nodes/speed sources).
  - `build_locked_bundle.py`: added `--source-label`, recorded in summary + config_snapshot (distinguishes v1/v3 provenance).
  - `compare_raw_connected_v1_v2.py`: added `--v1-label/--v2-label`, made Replication/SpecC optional (`--skip-repl` + existence guards), added LCC-bundle comparison (`--lcc-v1-root/--lcc-v2-root`).
- **v3 pipeline run (AM):** normalize (14,796 nodes / 26,864 edges, schema OK) -> edgeproj (square/hex/voronoi, pair-specific) -> coverage/asymmetry diagnostics -> LCC bundles (no-gate + gate5).
- **Retained outputs:**
  - `outputs/raw_connected_v3_grid_travel_time_edgeproj_v2/` (raw edgeproj + coverage/asymmetry)
  - `outputs/model_input_raw_connected_v3_edgeproj_v2_lcc_v1/` (no gate), `outputs/model_input_raw_connected_v3_edgeproj_v2_lcc_gate5_v1/` (gate5)
  - `outputs/raw_connected_v3_grid_travel_time_edgeproj_v2/diagnostics_compare_v1_v3/` (+ `COMPARISON_v1_vs_v3.md`)
- **Key result:** v3 is a pure coverage gain at unchanged cost level. On square AM, finite routed share 79.1% -> 83.7%; overlap pairs (3,659) have median time ratio v3/v1 = 1.0 and median diff 0.0 min (v3 adds 218 pairs v1 could not route, v1 only 7). gate5 LCC: nodes 1104 -> 1133, edges 2642 -> 2901, OD road commuter share 92.6% -> 95.4%, median edge tau 8.71 -> 8.68, median directional ratio 0.811 -> 0.811 (asymmetry signal unchanged).
- **Pending:** v3 PM raw costs; downstream structural reruns (directional_cost / full_estimation / Replication) on v3 if v3 is adopted as the network source.

## 2026-06-28: Gate5 model mainline rerun and input QA

- Added reusable bundle QA script:
  - `data_work/src/model/travel_cost_bundle/qa_model_input_bundle.py`
- Ran QA on `model_input_edgeproj_v2_lcc_gate5_v1`; retained outputs:
  - `data_work/outputs/model_input_edgeproj_v2_lcc_gate5_v1/qa/model_input_qa.json`
  - `data_work/outputs/model_input_edgeproj_v2_lcc_gate5_v1/qa/model_input_qa_checks.csv`
- QA result: all required checks passed. The model scope is a single LCC with 1104 nodes and 2642 directed edges; no isolated edge-graph nodes; edge `i/j`, OD `home_i/work_i`, and `origin_node/destination_node` all resolve correctly to `grid_id`; geometry covers all model nodes; max retained detour is 4.9976 with zero edges above gate 5.
- Re-ran fixed-cost directional model on gate5:
  - `data_work/outputs/directional_cost_edgeproj_v2_lcc_gate5_v1/`
  - Geometric symmetric-cost operator: symmetric relative to directed gives `Wbar_pct_change = -0.726%`, residence dissimilarity 2.75%, workplace dissimilarity 2.33%, OD dissimilarity 5.21%.
  - Asymmetry diagnostics: 1193 reciprocal pairs; median tau ratio 1.233; 8.89% of reciprocal pairs have tau ratio >= 2.
  - Directed fit to observed marginals is tight: residence L1 0.00035%, workplace L1 0.00036%.
- Ran arithmetic symmetric-operator robustness:
  - `data_work/outputs/directional_cost_edgeproj_v2_lcc_gate5_v1_sym_arithmetic/`
  - `Wbar_pct_change = -0.503%`, residence dissimilarity 2.09%, workplace dissimilarity 1.79%, OD dissimilarity 4.09%.
- Re-ran full-estimation scaffold baseline on gate5, with counterfactuals skipped:
  - `data_work/outputs/full_estimation_edgeproj_v2_lcc_gate5_v1/`
  - Baseline equilibrium converged (`Wbar = 1.8181`, 16 outer iterations). Fundamentals inversion hit default 800 iterations but remaining `max_log_change = 3.37e-07`; lambda diagnostic is weak and diagnostic-only (`lambda_hat = 0.0179`, `R2 = 0.0313`).
- Added gate5-specific AA / Spec C scripts and outputs:
  - `Replication_apply/prepare_from_data_work_square_edgeproj_v2_lcc_gate5/build_inputs_from_data_work_square_edgeproj_v2_lcc_gate5.py`
  - `Replication_apply/python_commuting/tidal_lane_specC_edgeproj_v2_lcc_gate5.py`
  - `Replication_apply/aa_input_square_edgeproj_v2_lcc_gate5/`
  - `Replication_apply/results/tidal_lane_edgeproj_v2_lcc_gate5/`
- Spec C now selects treated pairs from the gated LCC bundle rather than the ungated raw edgeproj CSV. Result: 100 treated pairs, asymmetry ratio range 2.04--6.45, slow-direction tau -12.1%, fast-direction tau +21.0%, `chi_hat = 0.9996625271`, `(chi_hat - 1) * 1e6 = -337.47` (fixed-flow BPR approximation only).
- Retained summary:
  - `data_work/outputs/model_input_edgeproj_v2_lcc_gate5_v1/MODEL_RERUN_SUMMARY.md`

## 2026-06-28: Travel-cost FINAL lock to edgeproj_v2_lcc_gate5 (index bug fixed + detour gate)

- **Final v1 travel cost = `model_input_edgeproj_v2_lcc_gate5_v1`** (1104 nodes, 2642 edges). All downstream inputs build from this version; decision memo `Documents/L2_notes/travel_cost_lock_in_20260628.md` rewritten. Supersedes the no-gate `model_input_edgeproj_v2_lcc_v1` selection from the entry below.
- **Fix 1 (index bug):** `_restrict_lcc` had filtered edges by `edges.i in nodes.node_i`, but these are unrelated index spaces (edges.i/j = stage06 t-edge order 12..3356; node_i = 0..2735). It dropped ~half the LCC edges and mislinked the rest. Now joins on `grid_id` only and rebuilds one fresh contiguous node index shared by nodes/edges/OD. Edge count 1462 -> 3198, isolated nodes 588 -> 0, `edges.i` resolves to correct grid 100%.
- **Fix 2 (detour gate):** added `--detour-gate G` to `build_locked_bundle.py`; drops adjacent edges with `route_length_m / straight-line centroid distance > G` (computed in UTM 50N for Beijing) before LCC. G=5 chosen.
- Gate G=5 vs no-gate: drops ~17% edges but only ~4% grids / ~3% road commuters; edge tau p99 100->30 min, max 254->52 min, extreme asymmetry share (ratio<0.5) 15.6%->8.9%. Coverage 92.6% city road commuters, 91.8% residents.
- New reusable figure script `data_work/src/model/travel_cost_bundle/plot_descriptive_audit.py` (supports `--detour-gate`); side-by-side `compare_base_vs_gate5_fig06.png` and `COMPARISON_vs_no_gate.md` under the gate5 output dir.
- Geometry: city is Beijing (~116.4E); maps/distances use UTM 50N (EPSG:32650). Earlier 51N draft rotated the grid ~4.3 deg (meridian convergence), changed gated edge set by 1 edge.
- **Pending:** directional_cost / full_estimation / Replication / Spec C must be re-run on `model_input_edgeproj_v2_lcc_gate5_v1`; the prior reruns (in entry below) were on the pre-fix 1462-edge bundle and are invalid.

## 2026-06-28: Travel-cost matrix locked to edgeproj_v2_lcc; model reruns updated (SUPERSEDED by entry above)

- Added reusable bundle builder under `data_work/src/model/travel_cost_bundle/`:
  - `build_locked_bundle.py` — build QSM parquet bundles with explicit cost source + LCC scope
  - `compare_candidates.py` — compare `centerline_lcc` vs `edgeproj_v2_lcc` and write decision memo
  - `lcc.py` — largest-connected-component helper
- Compared candidates (square AM):
  - `model_input_centerline_lcc_v1`: 1774 nodes, 3201 edges, 96.8% residents, 99.8% road commuters, median dir. ratio 0.914
  - **`model_input_edgeproj_v2_lcc_v1` (LOCKED)**: 1153 nodes, 1462 edges, 93.7% residents, 95.8% road commuters, median dir. ratio 0.782
- **Locked decision:** `model_input_edgeproj_v2_lcc_v1` — raw_connected v1 edgeproj_v2 pair-specific routing on the largest connected component. Rationale: real road routing + asymmetry aligned with descriptive evidence (0.793); centerline heuristic rejected as primary. `centerline_lcc` retained for robustness only. `raw_connected_v2` still rejected (spatial clip).
- Decision artifacts:
  - `data_work/outputs/travel_cost_decision_v1/` (`candidate_comparison.csv`, `travel_cost_decision.json`, `model_rerun_summary.json`)
  - `Documents/L2_notes/travel_cost_lock_in_20260628.md`
- Re-ran models on locked bundle:
  - `directional_cost_edgeproj_v2_lcc_v1`: Wbar symmetric-vs-directed **−0.013%**; 21.1% reciprocal pairs with tau ratio ≥ 2.0
  - `directional_cost_centerline_lcc_v1`: robustness baseline (+0.113% Wbar change)
  - `full_estimation_edgeproj_v2_lcc_v1`: baseline converges (Wbar≈3.17); counterfactuals skipped pending observed-anchored congestion fix
  - Replication: `aa_input_square_edgeproj_v2_lcc/`, Spec C → `results/tidal_lane_edgeproj_v2_lcc/` (91 pairs, chi_hat−1 ≈ −2.7 ppm)
- New Replication prepare script: `Replication_apply/prepare_from_data_work_square_edgeproj_v2_lcc/build_inputs_from_data_work_square_edgeproj_v2_lcc.py`
- Superseded as primary defaults: `manual_centerline_rules_v11` QSM (centerline stage06), `directional_cost_square_v1`, `full_estimation_square_v1`, `aa_input_square_v2`

## 2026-06-28: Shenzhen multi-tidal pooled PSM-DID v1

- Added `data_work/src/shenzhen_multi_tidal/run_multi_tidal_pooled_psm_did.py`.
- Output root: `data_work/outputs/shenzhen_multi_tidal_pooled_psm_did_v1/`.
- Stacks 5 main cohorts (文锦 AM/PM, 彩田, 红岭, 新洲) with Mahalanobis PSM matching (≤3 far-similar controls per cohort × direction), pooled local-jump DID, event-study, asymmetry DDD, and placebos (12:00 + 留仙 pre-shock).
- Main ±30 min pooled policy jump: +0.35 km/h (SE 1.09); pooled asymmetry DDD: +0.105 (SE 0.041).

## 2026-06-28: Shenzhen multi-tidal Baidu refinement v1

- Added `data_work/src/shenzhen_multi_tidal/run_multi_tidal_refinement_analysis.py` to implement handoff Steps 1–5 on top of `shenzhen_multi_tidal_post_only_v1`.
- Output root: `data_work/outputs/shenzhen_multi_tidal_refinement_v1/`.
- Handoff doc updated: `data_work/docs/SHENZHEN_MULTI_TIDAL_BAIDU_HANDOFF_20260628.md`.
- Step 1 control tiers: corridor-level distance bins at 0–1 km, 1–3 km, and ≥3 km; primary tier uses far controls when available, otherwise medium, otherwise near. Current primary tiers are medium for 彩田路 and 留仙大道 and near for the other retained urban roads.
- Step 2 short-window local events: ±30, ±60, and ±90 minute local-jump and 10-minute event-study bins retained for all three tiers.
- Step 3 corridor packages: `core_only`, `core_plus_1km`, and `core_plus_3km` with endpoint-graph extension and active-window speed / log travel time summaries.
- Step 4 control quality screening: inactive-hour diurnal correlation, reference speed gap, and 12:00 placebo local jump.
- Step 5 Liuxian placebo: primary-tier local jumps are imprecise and do not support a policy-direction-specific pre-shock breakpoint.
- Retained figures under `figures/summary/`:
  - `short_window_local_jump_primary_tier.png`;
  - `control_quality_inactive_corr_heatmap.png`;
  - `corridor_package_active_window_comparison.png`;
  - `liuxian_pre_shock_placebo.png`.
- Refinement pattern retained: 彩田路 opposite-direction short-window losses remain larger than policy-direction losses under medium controls; 红岭路 AM keeps positive policy-direction local-jump point estimates under near controls; 留仙大道 remains a pre-shock placebo.

## 2026-06-28: Shenzhen multi-tidal control/spec exploration v1

- Added `data_work/src/shenzhen_multi_tidal/run_multi_tidal_control_exploration.py`.
- Output root: `data_work/outputs/shenzhen_multi_tidal_exploration_v1/`.
- Scanned 6 control portfolios × 3 corridor packages × 5 specification families across the five retained urban tidal roads.
- Expanded control search to 8 km; similarity features and portfolios in `metrics/control_similarity_features.csv` and `metrics/control_portfolios.csv`.
- Best far-similar controls retained: 红岭路 AM 文锦中路 (2.7 km, corr 0.79); 彩田路 PM 福龙路 (3.7 km, corr 0.95); 留仙大道 PM 龙观大道 (5.2 km, corr 0.82).
- Short-window local-jump specs with standard errors rarely support policy-direction speed gains; 文锦北路 PM near-control jump is the only marginally significant positive case.
- Corridor active-reference DDD without regression standard errors produces false positives, including +2.15 km/h on Liuxian pre-shock with far-similar controls.
- Most promising pattern remains 红岭路 AM active-reference speed DDD (+6.9 to +9.1 km/h) and 彩田路 PM opposite-direction short-window losses / positive asymmetry under far controls.

## 2026-06-28: Shenzhen grid-level asymmetry evidence retained

- Added `data_work/src/shenzhen_gridlevel/generate_gridlevel_asymmetry_evidence.py`.
- Added documentation: `data_work/docs/SHENZHEN_GRIDLEVEL_ASYMMETRY_EVIDENCE_20260628.md`.
- Output root: `data_work/outputs/shenzhen_gridlevel_asymmetry_evidence_v1/`.
- Main input: `data_work/raw_data/shenzhen_gridlevel/fishnet1000_pairtime_hat.csv`.
- Main travel-time field: `car_time`, following provider guidance.
- Unit of analysis: unordered reciprocal fishnet1000 grid pair with both directed `car_time` values at least 1 minute.
- Retained sample:
  - 1,462,815 valid reciprocal grid pairs;
  - median `slow_over_fast_time` is 1.137;
  - median `symmetry_ratio_time` is 0.879;
  - median directional time gap is 6.62 minutes.
- Threshold evidence:
  - 57.5% of grid pairs have `symmetry_ratio_time <= 0.90`;
  - 35.1% have `symmetry_ratio_time <= 0.83`;
  - 21.7% have `symmetry_ratio_time <= 0.77`;
  - 0.91% have `symmetry_ratio_time <= 0.50`.
- Distance gradient retained:
  - 0 to 5 km pairs have median symmetry ratio 0.833 and 68.1% below 0.90;
  - 80 km and above pairs have median symmetry ratio 0.920 and 39.2% below 0.90.
- Layer-bridge evidence retained:
  - township OD, national top-50 AM sample: median ratio 0.903 and 52.2% below 0.90;
  - Shenzhen grid OD: median ratio 0.879 and 57.5% below 0.90;
  - Beijing adjacent-grid AM: median ratio 0.728 and 68.8% below 0.90;
  - Beijing centerline segment AM: median ratio 0.884 and 54.7% below 0.90.
- Retained figures:
  - `figures/figure_gridlevel_symmetry_ratio_hist.png`;
  - `figures/figure_gridlevel_threshold_curve.png`;
  - `figures/figure_gridlevel_asymmetry_by_distance_bin.png`;
  - `figures/figure_gridlevel_reciprocal_time_hexbin.png`;
  - `figures/figure_multilevel_asymmetry_bridge.png`;
  - `figures/figure_multilevel_asymmetry_threshold_shares.png`.
- Retained caveat: this is unweighted all-pair evidence. The raw file still lacks grid geometry and an ID crosswalk, so the current version cannot map the pattern or attach observed commuter flows.
- Next empirical action: find fishnet1000 geometry or crosswalk, then recompute the same grid-level evidence with OD commuter-flow weights.

## 2026-06-28: Shenzhen fishnet1000 grid-level pair-time audit

- Added `data_work/src/shenzhen_gridlevel/analyze_gridlevel_pairtime.py`.
- Added documentation: `data_work/docs/SHENZHEN_GRIDLEVEL_PAIRTIME_AUDIT_20260628.md`.
- Output root: `data_work/outputs/shenzhen_gridlevel_pairtime_v1/`.
- Input audited: `data_work/raw_data/shenzhen_gridlevel/fishnet1000_pairtime_hat.csv`.
- Data structure retained:
  - 2,927,521 rows and 8 columns;
  - 1,711 unique `fid_home` and 1,711 unique `fid_work`;
  - the file is a complete `1711 x 1711` grid-pair travel-time matrix;
  - no missing values and no duplicate `fid_home, fid_work` pairs.
- Field interpretation:
  - `car_time` / `car_dist` are baseline OD car travel time and distance, and provider guidance is to use `car_time` as the main cost measure;
  - `car_time_detour` / `car_dist_detour` are detour-adjusted OD time and distance, retained only as robustness / diagnostics unless a provider codebook says otherwise;
  - `time_diff` and `dist_diff` match the computed differences up to float precision.
- Directionality diagnostics:
  - all OD rows have reverse pairs;
  - among reciprocal pairs with both `car_time` values at least 1 minute, median time ratio is 1.137, p90 is 1.453, p95 is 1.596;
  - 60.9% of reciprocal pairs have `car_time` ratio at least 1.10, 27.8% at least 1.25, 7.90% at least 1.50, and 0.91% at least 2.00.
- Compatibility diagnostics:
  - direct numeric-id merging with `resid_work_flow_grid500.dta` is not valid: only 4.48% of valid grid500 flow has both numeric IDs in the new fid set;
  - the new data lacks geometry/coordinates/person attributes and should not be treated as a replacement for the existing grid500 flow or Baidu road-segment speed panel;
  - a fishnet1000 geometry/crosswalk is needed before flow-weighted OD tests can be run.
- Retained figures:
  - `figures/travel_time_distribution.png`;
  - `figures/directionality_ratio_distribution.png`;
  - `figures/detour_time_penalty_distribution.png`.

## 2026-06-28: Fixed-cost directional congestion model retained

- Added a simplified fixed-cost directional model under `data_work/src/model/directional_cost/`.
- Added the model documentation `data_work/docs/DIRECTIONAL_COST_MODEL_20260628.md`.
- Retained output root: `data_work/outputs/directional_cost_square_v1/`.
- Inputs:
  - `version_id=manual_centerline_rules_v11`;
  - `grid_type=square`;
  - `period=AM`;
  - `od_pop_col=commuters_road`.
- Model scope:
  - observed directed travel time is treated as the cost primitive;
  - no lane reallocation, endogenous congestion update, `lambda` calibration, or exact-hat counterfactual is used;
  - reciprocal directed edges are symmetrized with the geometric mean for the main comparison.
- Retained diagnostics:
  - 2736 nodes, 4087 directed edges, 2004 reciprocal edge pairs, and 95145.589 road commuters;
  - median reciprocal-pair travel-time ratio is 1.079, p90 is 1.634, p95 is 2.169;
  - 44.1% of reciprocal pairs have at least a 10% directional travel-time gap, 12.8% have at least a 50% gap, and 6.0% have at least a 100% gap.
- Main retained result:
  - symmetric-cost welfare is 0.1507% higher than the directed observed-cost baseline, equivalently restoring full observed directionality from the symmetric world lowers `Wbar` by about 0.1504%;
  - symmetric-cost approximation changes residence shares by 1.0107% dissimilarity, workplace shares by 0.8221%, and predicted OD flows by 1.9726%.
- Retained simulations:
  - `simulations/asymmetry_intensity.csv`: log-space interpolation from symmetric costs to observed directed costs;
  - `simulations/asymmetry_threshold.csv`: only reciprocal pairs above a travel-time-ratio threshold remain directed;
  - `simulations/network_propagation.csv`: route-propagation scale `phi` varies from 0.50 to 1.25.
- Retained figures:
  - `figures/asymmetry_ratio_histogram.png`;
  - `figures/simulation_asymmetry_intensity.png`;
  - `figures/simulation_asymmetry_threshold.png`;
  - `figures/simulation_network_propagation.png`.
- Verification:
  - `python3 -m py_compile data_work/src/model/directional_cost/*.py` passed;
  - `run_directional_cost.py` completed with finite outputs;
  - retained CSV/JSON/NPZ outputs were checked for non-finite numeric values.
- Next model action: add parameter sensitivity for `theta`, `alpha`, and `beta`, plus symmetric-operator robustness using arithmetic mean and alternative pair-cost rules.

## 2026-06-28: Shenzhen Caitian Road Baidu post-only analysis

- Added `data_work/src/shenzhen_caitian_baidu/run_caitian_post_only_analysis.py`.
- Added `data_work/src/shenzhen_caitian_baidu/plot_caitian_osm_overlays.py`.
- Added `data_work/src/shenzhen_caitian_baidu/analyze_post_only_asymmetry_evidence.py`.
- Added `data_work/src/shenzhen_caitian_baidu/analyze_post_only_direction_decomposition.py`.
- Added `data_work/src/shenzhen_caitian_baidu/analyze_post_only_1630_breakpoint.py`.
- Recorded Shenzhen tidal-lane operation windows in `data_work/config/shenzhen_tidal_lane_operation_windows_v1.json`: 彩田路 `16:30-21:30`, 新洲路 `17:30-20:00`, 红岭路 `07:30-09:30`, 文锦北路 `07:00-09:30` and `18:00-20:00`, 留仙大道 `17:30-20:00`, and 惠深沿海高速 on weekends and legal holidays with flexible timing. These are treated as operation windows, not construction shocks.
- Output root: `data_work/outputs/shenzhen_caitian_baidu_analysis_v1/`.
- Inputs are the retained raw files `data_work/raw_data/gis/深圳潮汐车道shp/2_单线.shp`, `data_work/raw_data/gis/深圳潮汐车道shp/2_双线.shp`, `data_work/raw_data/gis/roads_baidu/shenzhen_roads.shp`, and `data_work/raw_data/speed_shenzhen_all_wgs84.csv`.
- The treatment chain is defined from the tidal-lane shapefile and Baidu road geometry using `EPSG:32650` and a 60 m main buffer, with 30 m and 100 m sensitivity tables. The retained core chain has 13 Baidu segments: 6 in the 福田 to 龙华 policy direction and 7 in the 龙华 to 福田 opposite direction, each about 5.01 km and complete for the 2025-05-19 to 2025-05-25 10-minute speed panel.
- The retained control corridors are 皇岗路 as the main control, with 金田路, 益田路, and 香蜜湖路 as robustness controls. The analysis also retains 31 directed upstream and downstream network segments within 3 km of the treatment endpoints.
- Retained figures include the treatment-control-network map, weekday and weekend 10-minute diurnal speed profiles, weekday diagnostic-window speeds, directional speed ratios, upstream and downstream active/reference ratios, post-only event-study figures, and 16:30 local-breakpoint figures. Each retained figure is exported as PNG, PDF, and SVG. Time-series figures use 10-minute points plus connecting lines; no smoothing is applied.
- Retained event-study files include `metrics/event_study_specifications.json`, treatment-only speed, DDD speed against 皇岗路, 金田路, 益田路, and 香蜜湖路, treatment-only log travel time, and corridor-level harmonic speed against 皇岗路. The event-time zero is now `16:30`, and the omitted bin is `16:00-16:30`. These specifications are post-only within-day event-time associations and are not pre/post causal estimates of the opening.
- Main descriptive ratio retained for interpretation: in the policy direction, 彩田路 has a weekday `16:30-21:30 / 15:00-16:30` speed ratio of 0.800; 皇岗路 has 0.623 in the analogous direction. In the opposite direction, 彩田路 has 0.728 and 皇岗路 has 0.912.
- Retained OSM-overlaid figures:
  - `figures/osm_caitian_treatment_controls_network.{png,pdf,svg}`;
  - `figures/osm_weekday_active_speed_segments.{png,pdf,svg}`;
  - `figures/osm_weekday_active_reference_ratio_segments.{png,pdf,svg}`;
  - `figures/osm_network_active_reference_ratio.{png,pdf,svg}`.
- The OSM overlays confirm that the retained treatment chain aligns with the 彩田路 and 新彩隧道 corridor, while 皇岗路、金田路、益田路 are nearby north-south parallel corridors and 香蜜湖路 is a more distant western robustness corridor.
- Retained post-only asymmetry evidence:
  - `figures/post_only_asymmetry_window_comparison.{png,pdf,svg}`;
  - `figures/post_only_asymmetry_ddd_support.{png,pdf,svg}`;
  - `figures/post_only_asymmetry_event_ddd.{png,pdf,svg}`;
  - `figures/post_only_asymmetry_daily_active_ddd.{png,pdf,svg}`;
  - `metrics/post_only_asymmetry_ddd_summary.csv`;
  - `metrics/post_only_asymmetry_event_ddd.csv`;
  - `metrics/post_only_asymmetry_evidence_spec.json`.
- Supportive post-only asymmetry pattern under the corrected window: 彩田路's weekday policy-direction/opposite-direction speed ratio rises from 0.789 in `15:00-16:30` to 0.982 in `16:30-21:30`. The active-window asymmetry DDD is positive against all four controls on all five weekdays, with mean log-point estimates of 0.604 against 皇岗路, 0.293 against 金田路, 0.354 against 益田路, and 1.159 against 香蜜湖路. This remains a post-only within-day association, not a pre/post opening effect.
- Retained direction-decomposition outputs:
  - `figures/post_only_direction_active_reference_ratios.{png,pdf,svg}`;
  - `figures/post_only_direction_dd_by_control.{png,pdf,svg}`;
  - `figures/post_only_direction_daily_dd.{png,pdf,svg}`;
  - `metrics/post_only_direction_dd_summary.csv`;
  - `metrics/post_only_direction_decomposition_spec.json`.
- Direction-decomposition result under the corrected window: in the policy direction, 彩田路's weekday `16:30-21:30 / 15:00-16:30` speed ratio is 0.800. Relative to control time trends in the same direction, the log-speed DD is 0.282 against 皇岗路, -0.180 against 金田路, -0.055 against 益田路, and 0.503 against 香蜜湖路. In the opposite direction, the log-speed DD is negative against all four controls: -0.230, -0.384, -0.283, and -0.462. This supports stronger directional asymmetry during the operation window, but the policy-direction speed gain is not robust across all controls when evaluated separately from the opposite-direction loss.
- Retained 16:30 local-breakpoint diagnostics:
  - `figures/post_only_1630_corridor_timeseries.{png,pdf,svg}`;
  - `figures/post_only_1630_local_jump_dd.{png,pdf,svg}`;
  - `figures/post_only_1630_segment_rollout_heatmap.{png,pdf,svg}`;
  - `metrics/post_only_1630_local_jump_summary.csv`;
  - `metrics/post_only_1630_local_jump_dd.csv`;
  - `metrics/post_only_1630_segment_rollout_summary.csv`;
  - `metrics/post_only_1630_breakpoint_spec.json`.
- Local-breakpoint result under the corrected window: the policy direction does not exhibit an immediate positive jump at 16:30. Its speed falls by 3.37 km/h from 16:20 to 16:30 and by 7.84 km/h when comparing `16:30-17:00` against `16:00-16:30`. Net of controls, the 30-minute local jump is positive only against 皇岗路 and negative against 金田路、益田路、香蜜湖路.
- Segment-level heatmaps order treatment-core segments north to south by centroid latitude. The policy-direction gains appear later, around 60 to 70 minutes after 16:30, in the middle and southern segments rather than as a clean north-to-south rollout. In the opposite direction, several core segments turn sharply negative within 20 to 30 minutes after 16:30. This remains a post-only within-day association, not a pre/post opening effect.
- The 16:30 time-series diagnostic now plots `14:30-23:30`, with the full `16:30-21:30` operation window shaded. The plot uses 10-minute point markers plus connecting lines, with no smoothing.
- Added weekday-date split for the expanded 16:30 diagnostic:
  - `figures/post_only_1630_corridor_timeseries_by_weekday_date.{png,pdf,svg}`;
  - `data/post_only_1630_weekday_date_window_summary.csv`.
- Weekday-date pattern retained: the opposite-direction drop during the operation window is present on all five weekdays, but the timing differs by date. Tuesday and Friday have the lowest 彩田路 active-window speeds in both directions. Friday is especially irregular because the post-21:30 policy-direction speed remains low rather than rebounding like Monday through Thursday. 香蜜湖路 is included in the expanded time-series diagnostics and is visually important because its policy-direction speed drops sharply during the evening peak.
- Added direction-specific event-study DID for the corrected 彩田路 window:
  - `data_work/src/shenzhen_caitian_baidu/analyze_caitian_1630_event_study_did.py`;
  - `figures/caitian_1630_event_study_did_pooled_controls.{png,pdf,svg}`;
  - `figures/caitian_1630_event_study_did_by_control.{png,pdf,svg}`;
  - `data/caitian_1630_event_study_did_sample.csv`;
  - `metrics/caitian_1630_event_study_did_coefficients.csv`;
  - `metrics/caitian_1630_event_study_did_diagnostics.json`;
  - `metrics/caitian_1630_event_study_did_spec.json`.
- Event-study DID specification retained: segment speed is regressed on road-segment fixed effects, date fixed effects, minute-of-day fixed effects, and Caitian-by-event-bin interactions, separately by direction and control set. The sample is weekdays `14:30-23:30`; event time zero is `16:30`; the omitted bin is `16:00-16:30`; standard errors are clustered by road segment.
- Event-study DID result retained: against pooled controls, the policy direction is negative in the first two post-16:30 bins, then mostly near zero through the operation window, with a positive late post-window bin at +390 minutes. The opposite direction is sharply negative from +30 through +240 minutes and returns toward zero by the end of the operation window. This pattern supports a displacement cost in the opposite direction more robustly than a same-window policy-direction speed gain.

## 2026-06-28: Model-line audit and learning guide

- Added `data_work/docs/MODEL_AUDIT_LEARNING_GUIDE_20260628.md`.
- Audited the latest `full_estimation` line, the older `spatial_equilibrium.py` prototype, the AA exact-hat replication attempts, and retained model outputs.
- Current main model line: `data_work/src/model/full_estimation/`, with existing output root `data_work/outputs/full_estimation_square_v1/`.
- Key diagnostics retained:
  - `CF1_symmetric` is currently not a valid directional-cost counterfactual because the symmetric travel-time surface is passed only as an initial `tau_min`, not as a persistent primitive.
  - The latest baseline has 46.5% of edges below free-flow time and 70.9% below observed time, so the congestion update needs free-flow and baseline anchoring before welfare numbers are interpreted.
  - The current diagnostic estimate `lambda_hat=0.01214` differs sharply from the placeholder structural value `lambda=0.07145`.
  - `full_estimation_square_v1` is not fully reproducible from the current code defaults: output metadata records `fundamentals_meta.n_iter=1512`, while the current `invert_fundamentals.py` default `max_iter` is 800, and `run.log` stops after Step 2.
- Next recommended model action: rerun `full_estimation` with `python3` into a dated output directory, then fix CF1 and the congestion/free-flow block before treating counterfactual welfare numbers as retained results.

## 2026-06-28 Shenzhen multi-tidal-lane Baidu post-only diagnostics

- Added a reproducible multi-road Shenzhen Baidu diagnostic module:
  - `data_work/src/shenzhen_multi_tidal/run_multi_tidal_post_only_analysis.py`
- Retained output root:
  - `data_work/outputs/shenzhen_multi_tidal_post_only_v1/`
- The tidal-lane shapefile number mapping is:
  - `1`: 文锦北路;
  - `2`: 彩田路;
  - `3`: 惠深沿海高速 / 盐坝高速-type highway, excluded from the main urban-road analysis;
  - `4`: 新洲路;
  - `5`: 红岭路;
  - `6`: 留仙大道.
- Locked operation windows and policy-like directions:
  - 彩田路: `16:30--21:30`, 福田至龙华出关方向, coded northbound-like;
  - 新洲路: `17:30--20:00`, 莲花路至梅华路之间, 北行直达梅丽路;
  - 红岭路: `07:30--09:30`, 泥岗路至笋岗路之间, 南行;
  - 文锦北路: `07:00--09:30` and `18:00--20:00`, PM is outbound / 出关, AM is coded as the reverse direction;
  - 留仙大道: `17:30--20:00`, west-to-east / southwest-to-northeast.
- Treatment matching uses the corresponding `*_单线.shp` tidal-lane file and the Baidu Shenzhen road network. The full buffer-intersection match is retained, but the main analysis uses `core_treatment` segments only: road-name matches, or segments that align with the tidal-lane axis, lie close to the single-line shapefile, and have sufficient overlap. Auxiliary intersection segments are retained for map QA but excluded from the corridor-speed and event-study samples.
- Retained QA tables:
  - `metrics/tidal_number_mapping.csv`;
  - `metrics/match_sensitivity_by_road.csv`;
  - `metrics/matched_treatment_segments.csv`;
  - `metrics/control_candidates_by_tidal_lane.csv`;
  - `metrics/segment_speed_coverage.csv`.
- Retained maps and figures:
  - OSM-overlaid treatment/control maps under `figures/maps/`;
  - weekday 10-minute point-and-line speed profiles for each retained road and operation window under `figures/`;
  - pooled-control post-only event-study DID figures under `figures/`.
- The event-study specification is:
  - `speed_{i d m} = alpha_i + delta_d + lambda_m + sum_{k != -30} beta_k TreatmentCorridor_i * 1[event_bin_m = k] + epsilon_{i d m}`.
  - Models are estimated separately by tidal lane, operation window, direction, and control set. Standard errors are clustered by road segment. Fixed effects are road segment, date, and minute of day.
- Estimation uses 10-minute speed observations from the Baidu post week, `2025-05-19` to `2025-05-25`. This is a post-only within-day treatment-control comparison, not a pre/post opening estimate.
- Selected control candidates retained for screening:
  - 文锦北路: 中翠路, 洪湖西路, 金运路, 无名路, 洪湖路;
  - 彩田路: 海田路, 皇岗路, 金田路, 凯丰路, 无名路;
  - 新洲路: 益田路, 景田路, 香梅路, 梅林路, 梅村路;
  - 红岭路: 红岭中路, 红岗路, 玉平大道, 泥岗红岭立交桥, 梨园路;
  - 留仙大道: 学苑大道, 玉龙路, 无名路, 民丰路, 南坪快速.
- Main descriptive pattern retained from pooled-control event studies:
  - 彩田路: policy-like direction is near zero during `16:30--21:30`, while the opposite direction is negative in most active-window bins. This is consistent with a directional-capacity story in which the policy-like direction is protected relative to the reverse direction, rather than one in which the policy-like direction becomes visibly faster.
  - 红岭路: the `07:30--09:30` southbound policy-like direction has positive active-window point estimates, while the opposite direction is negative. This is the clearest retained road-window pattern matching the desired sign structure.
  - 文锦北路, 新洲路, and 留仙大道 do not deliver the desired sign structure in the current within-day setup. Their policy-like directions are often slower relative to controls during the active window.

## 2026-06-28 correction to multi-tidal-lane windows and treatment-only summaries

- Corrected the 文锦北路 PM window from `17:30--20:30` to the official `18:00--20:00` window in:
  - `data_work/config/shenzhen_tidal_lane_operation_windows_v1.json`;
  - `data_work/src/shenzhen_multi_tidal/run_multi_tidal_post_only_analysis.py`.
- Re-ran `shenzhen_multi_tidal_post_only_v1` and removed stale `wenjinbei_pm_1730_2030_*` figure files from the output directory.
- Added retained treatment-only and length-summary outputs:
  - `data_work/outputs/shenzhen_multi_tidal_post_only_v1/metrics/treatment_only_window_trends.csv`;
  - `data_work/outputs/shenzhen_multi_tidal_post_only_v1/data/treatment_only_window_trends_daily.csv`;
  - `data_work/outputs/shenzhen_multi_tidal_post_only_v1/metrics/treatment_control_corridor_length_summary.csv`.
- Retained direction/time confirmations from Shenzhen public-security and traffic-police notices:
  - 新洲路 PM: 莲花路至梅丽路, `17:30--20:00`, lane-added direction south-to-north;
  - 文锦北路 AM: `07:00--09:30`, lane-added direction north-to-south; 文锦北路 PM: `18:00--20:00`, lane-added direction south-to-north;
  - 留仙大道: 学苑大道至福龙路, `17:30--20:00`, lane-added direction west-to-east.
- The Baidu speed panel used here is `2025-05-19` to `2025-05-25`. The 留仙大道 public notice starts from `2026-03-23`, so the retained Baidu-week Liuxian within-day diagnostics should not be described as a post-opening analysis.

## 2026-06-28 Tianyu route sheet audit and treatment-length reweighting

- Parsed Tianyu's workbook `data_work/raw_data/深圳市潮汐车道2.xlsx` into retained route-definition tables:
  - `data_work/outputs/shenzhen_multi_tidal_post_only_v1/metrics/tianyu_excel_route_definitions.csv`;
  - `data_work/outputs/shenzhen_multi_tidal_post_only_v1/metrics/route_source_conflicts.csv`.
- Recorded source conflicts between Tianyu's sheet and the official/project operation-window file:
  - 文锦北路 PM: Tianyu lists `17:30--20:30`, while the official/project window is `18:00--20:00`;
  - 彩田路: Tianyu lists `17:30--20:30`, while the current project window is `16:30--21:30`;
  - 红岭路: Tianyu lists `07:30--09:00`, while the official/project window is `07:30--09:30`.
- Tianyu's 文锦北路 endpoint line is 1.88 km, close to the workbook's 2 km segment length. Tianyu's 彩田路 endpoint coordinates imply 41.43 km, inconsistent with the 4 km length field; the Caitian endpoint coordinates are therefore flagged as unreliable in `route_source_conflicts.csv`.
- Revised the Shenzhen multi-road Baidu analysis to avoid weighting treatment corridors by the full Baidu roadseg length. Treatment segment travel time now uses `analysis_length_m`, defined as the overlap length between the matched roadseg and the tidal-lane shapefile buffer. Control segments still use the full selected Baidu roadseg length.
- Retained the new length outputs:
  - `data_work/outputs/shenzhen_multi_tidal_post_only_v1/metrics/treatment_control_corridor_length_summary.csv`;
  - `data_work/outputs/shenzhen_multi_tidal_post_only_v1/metrics/treatment_control_direction_length_summary.csv`.
- The treatment effective lengths are now much closer to the implementation corridors:
  - 文锦北路: full matched roadseg length 12.91 km, overlap analysis length 3.51 km; direction-specific analysis lengths are 1.70 km and 1.81 km;
  - 彩田路: full matched roadseg length 11.34 km, overlap analysis length 7.21 km; direction-specific analysis lengths are 3.42 km and 3.79 km;
  - 新洲路: full matched roadseg length 4.05 km, overlap analysis length 2.62 km; direction-specific analysis lengths are 1.26 km and 1.36 km;
  - 红岭路: full matched roadseg length 3.23 km, overlap analysis length 2.20 km; direction-specific analysis lengths are 1.10 km and 1.10 km;
  - 留仙大道: full matched roadseg length 10.35 km, overlap analysis length 2.95 km; direction-specific analysis lengths are 1.39 km and 1.56 km.

## 2026-06-28 focused control reselection and map QA

- Revised potential-control selection in `data_work/src/shenzhen_multi_tidal/run_multi_tidal_post_only_analysis.py` to use the focused treatment overlap length as the comparison target.
- Excluded unnamed and road-engineering candidates from the potential-control catalog. The exclusion rule removes empty road names, `无名路`, and names containing `无名`, `高速`, `快速`, `隧道`, `立交`, or `匝道`.
- Added two more cleanliness screens: candidate road names sharing the same tidal-road family token are removed, and candidates must be at least 120 m from the focused treatment geometry. This moves same-corridor extensions and direct connector segments out of the potential-control set.
- Retained final potential-control candidates in `data_work/outputs/shenzhen_multi_tidal_post_only_v1/metrics/control_candidates_by_tidal_lane.csv`:
  - 文锦北路: 金运路, 翠竹路, 吉华路, 红岗路, 西环路;
  - 彩田路: 金田路, 皇岗路, 益田路, 福田路, 华富路;
  - 新洲路: 景田路, 益田路, 香梅路, 梅村路, 梅秀路;
  - 红岭路: 八卦路, 梨园路, 宝安北路, 玉平大道, 上步中路;
  - 留仙大道: 学苑大道, 玉龙路, 民丰路, 民宝路, 民康路.
- Added two retained map families for each retained urban tidal road:
  - OSM basemap with a white transparency mask: `data_work/outputs/shenzhen_multi_tidal_post_only_v1/figures/maps_osm_masked/`;
  - Baidu road-network-only reference maps: `data_work/outputs/shenzhen_multi_tidal_post_only_v1/figures/maps_plain/`.
- QA notes: the final control table has no road names matching the excluded-name keywords and no same-family tidal-road extensions. The corridor length table remains two-directional at the corridor level, while `treatment_control_direction_length_summary.csv` gives the one-direction lengths.

## 2026-06-28 refined-control rerun results

- Re-ran `data_work/src/shenzhen_multi_tidal/run_multi_tidal_post_only_analysis.py` after the focused treatment and refined potential-control definitions were locked.
- Rebuilt the retained figure set:
  - six weekday 10-minute speed profiles;
  - six pooled-control event-study DID figures;
  - five OSM masked treatment-control maps;
  - five plain Baidu-road-network treatment-control maps.
- Added retained summary figures under `data_work/outputs/shenzhen_multi_tidal_post_only_v1/figures/summary/`:
  - `active_reference_treatment_control_change_summary`;
  - `directional_asymmetry_treatment_control_summary`;
  - `event_study_active_window_summary`.
- Refined-control conclusion:
  - 彩田路 is the strongest post-only asymmetry case. During `16:30--21:30`, the policy-like direction is roughly flat relative to controls in the active/reference comparison, while the opposite direction is much slower relative to controls. The pooled-control event study gives an active-window mean of -0.90 km/h for the policy-like direction and -9.99 km/h for the opposite direction.
  - 红岭路 is the clearest case where the policy-like direction is faster relative to controls. During `07:30--09:30`, the active/reference comparison is +4.02 km/h for the policy-like direction and -1.03 km/h for the opposite direction. The pooled-control event study gives active-window means of +2.05 km/h and +0.44 km/h, respectively.
  - 文锦北路, 新洲路, and 留仙大道 do not provide the desired sign pattern under this post-only design. Their policy-like directions are slower or near zero relative to controls during the active window.
- These results remain post-only within-day evidence. They should be interpreted as directional-capacity patterns conditional on the refined control set, not as opening-before/after causal estimates.

## 2026-06-28 raw-distribution diagnostics for refined Shenzhen controls

- Added a raw-distribution diagnostic script:
  - `data_work/src/shenzhen_multi_tidal/plot_raw_distribution_diagnostics.py`.
- The script reads retained outputs from `data_work/outputs/shenzhen_multi_tidal_post_only_v1/` and does not re-read or modify raw data.
- Retained raw-distribution figures under `data_work/outputs/shenzhen_multi_tidal_post_only_v1/figures/raw_distribution/`:
  - treatment/control raw 10-minute corridor-speed boxplots for each road-window;
  - treatment-only daily raw point time series for each road-window;
  - a treatment-only 30-minute start-time discontinuity summary;
  - weekday raw Baidu segment-speed histograms by tidal road.
- Retained raw-distribution tables:
  - `metrics/raw_corridor_speed_distribution_quantiles.csv`;
  - `metrics/raw_start_breakpoint_30min_daily.csv`;
  - `metrics/raw_start_breakpoint_30min_summary.csv`;
  - `metrics/raw_segment_speed_distribution_by_segment.csv`.
- Liuxian is treated as a pre-shock placebo in this Baidu week. The raw 17:30 start-time check has no policy-specific one-sided break: the policy-like direction falls by 5.3 km/h on average in the 30 minutes after 17:30, and the opposite direction falls by 2.7 km/h, with high weekday variation. The daily raw point plot indicates a broad evening-peak pattern rather than a clean tidal-lane discontinuity.
- The raw segment-speed histograms reinforce the control-sensitivity concern. Treatment and selected controls have different baseline speed distributions for several corridors, especially 彩田路 and 留仙大道, so DID estimates remain sensitive to the potential-control set.

## 2026-06-28 raw time-variation diagnostics

- Corrected the raw-data diagnostic focus from cross-sectional speed distributions to speed variation over time.
- Added time-axis figures under `data_work/outputs/shenzhen_multi_tidal_post_only_v1/figures/raw_time_variation/`:
  - `*_raw_speed_over_time` plots absolute 10-minute corridor speeds over clock time;
  - `*_raw_change_over_time` plots 10-minute corridor-speed changes relative to each corridor-date-direction reference-window mean.
- In these figures, treatment observations are plotted as weekday-specific raw 10-minute point-lines. Control observations are plotted as gray 10-minute point clouds, with a same-minute control mean line for visual reference only. No time smoothing is applied.
- Added the point-level retained table:
  - `data_work/outputs/shenzhen_multi_tidal_post_only_v1/metrics/raw_time_variation_change_panel.csv`.
- Liuxian remains consistent with a pre-shock placebo interpretation in the time-axis diagnostic. Both directions have broad evening-peak variation around 17:30, rather than a clean policy-direction discontinuity.

## 2026-06-28 Shenzhen multi-tidal Baidu handoff package

- Re-ran the final refined-control Shenzhen multi-tidal pipeline:
  - `python3 data_work/src/shenzhen_multi_tidal/run_multi_tidal_post_only_analysis.py`;
  - `python3 data_work/src/shenzhen_multi_tidal/plot_raw_distribution_diagnostics.py`.
- Refreshed the six `weekday_timeseries` figures using final treatment/control definitions:
  - 文锦北路 AM;
  - 文锦北路 PM;
  - 彩田路 PM;
  - 新洲路 PM;
  - 红岭路 AM;
  - 留仙大道 PM.
- Retained figures were verified in PNG/PDF/SVG formats. The main time-series outputs are under `data_work/outputs/shenzhen_multi_tidal_post_only_v1/figures/`.
- Added the handoff document for the next agent:
  - `data_work/docs/SHENZHEN_MULTI_TIDAL_BAIDU_HANDOFF_20260628.md`.
- The handoff document records the current data inputs, scripts, final treatment/control definitions, retained output paths, completed attempts, current results, identification limits, and a concrete next-step plan.

## 2026-06-28 Shenzhen tidal-lane operation shock notes

- Added a retained shock memo:
  - `data_work/docs/SHENZHEN_TIDAL_OPERATION_SHOCKS_20260628.md`.
- Recorded user-provided operation shocks:
  - 文锦北路 did not operate for roughly one month after the 2026 Spring Festival, reportedly because of metro construction;
  - 留仙大道 opened 45 minutes late on `2026-04-24`.
- The memo records data requirements for exploiting these shocks: high-frequency directed segment speeds, exact planned and actual operation logs, construction and lane-closure boundaries, weather and incident data, and complaint or public-feedback records.
- Identification note retained: 留仙大道 2026-04-24 is the cleaner candidate for a short-window operation-timing event. 文锦北路停实施 has useful on/off variation but is confounded by metro construction unless施工边界 and占道 information are collected.

## 2026-06-28 Liuxian Gaode purchase audit and Wenjinbei construction check

- Audited local Gaode raw files under `data_work/raw_data/Shenzhen_Gaode/`.
- Current purchased Liuxian Avenue Gaode weeks are:
  - `2026-03-02` to `2026-03-08`, 14 segments, 14,112 ten-minute observations, complete by segment-date-time grid;
  - `2026-04-13` to `2026-04-19`, 16 segments, 16,128 ten-minute observations, complete by segment-date-time grid.
- These purchased weeks do not cover the Liuxian Avenue `2026-04-24` 45-minute late-opening shock. A minimum follow-up purchase should cover `2026-04-20` to `2026-04-26`; a cleaner comparison window is `2026-04-13` to `2026-04-30`.
- Updated `data_work/docs/SHENZHEN_TIDAL_OPERATION_SHOCKS_20260628.md` with public Wenjinbei construction evidence:
  - Shenzhen traffic-police tidal-lane notice confirms Wenjinbei AM southbound and PM northbound operation, with PM official window `18:00-20:00`;
  - official February 2026 traffic-management data list `新建深圳至深汕合作区铁路工程` at `文锦北路与翠茵路交叉口`;
  - local reporting on 草埔立交改造 indicates temporary signalized at-grade traffic organization around 文锦北路/龙岗大道, suggesting the construction shock may affect both directions through intersection control and merging, not only one side of the road.
- Remaining data gap: public sources do not yet identify exact lane-side occupation, daily traffic-organization plans, the full permit period from the original permit, or whether the 2026 Spring Festival non-operation affected AM and PM equally. These must come from operation logs, construction traffic-organization drawings, or additional official permits/notices before a clean Wenjinbei shock design.

## 2026-06-27: Shenzhen Caitian Road Baidu segment candidates generated

- Added `data_work/src/shenzhen_caitian_baidu/find_caitian_baidu_segments.py`.
- The script matches Baidu road-network segments for 彩田路 from 新彩隧道 to 红荔路, with the 福田 to 龙华 direction marked as the policy direction.
- Retained outputs:
  - `data_work/outputs/shenzhen_caitian_baidu_candidates_v1/metrics/caitian_baidu_core_chain_segments.csv`
  - `data_work/outputs/shenzhen_caitian_baidu_candidates_v1/metrics/caitian_baidu_corridor_segments.csv`
  - `data_work/outputs/shenzhen_caitian_baidu_candidates_v1/metrics/caitian_baidu_text_candidates.csv`
  - `data_work/outputs/shenzhen_caitian_baidu_candidates_v1/metrics/caitian_baidu_candidate_metadata.json`
  - `data_work/outputs/shenzhen_caitian_baidu_candidates_v1/data/caitian_baidu_corridor_segments.geojson`
  - `data_work/outputs/shenzhen_caitian_baidu_candidates_v1/figures/caitian_baidu_corridor_segments_map.png`
  - `data_work/outputs/shenzhen_caitian_baidu_candidates_v1/figures/caitian_baidu_corridor_segments_map.pdf`
  - `data_work/outputs/shenzhen_caitian_baidu_candidates_v1/figures/caitian_baidu_corridor_segments_map.svg`
- Core continuous-chain candidates:
  - 福田 to 龙华: 6 Baidu segments, total length 5.013 km, all with speed observations.
  - 龙华 to 福田: 7 Baidu segments, total length 5.014 km, all with speed observations.
- The user-supplied first coordinate `(114.408944115753, 22.7662587886022)` is not used for selection because it is 41.4 km from the supplied 红荔路-side coordinate and about 41.2 km from the selected 彩田路 corridor. The retained selection is based on Baidu road names and geometries.

## 2026-06-27: Shenzhen Liuxian treatment-only diurnal figures generated

- Added a reproducible plotting script: `data_work/src/shenzhen_liuxian_gaode/plot_liuxian_diurnal.py`.
- Output root: `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/`.
- Inputs: the two raw Gaode CSV files under `data_work/raw_data/Shenzhen_Gaode/`.
- Sample retained for the main plots: 14 Liuxian Avenue segments common to the pre week and post week.
- Retained figures:
  - `figures/speed_diurnal_10min_by_direction_daytype_common_segments.png`;
  - `figures/speed_hourly_by_direction_daytype_common_segments.png`;
  - `figures/delay_diurnal_10min_by_direction_daytype_common_segments.png`;
  - `figures/delay_hourly_by_direction_daytype_common_segments.png`.
- Retained data and metrics:
  - `data/diurnal_10min_by_direction_daytype_common_segments.csv`;
  - `data/hourly_by_direction_daytype_common_segments.csv`;
  - `metrics/core_weekday_1730_2000_by_direction_common_segments.csv`;
  - `metrics/segment_inventory_used.csv`;
  - `metrics/run_metadata.json`.
- Descriptive pattern to carry forward: in the treatment-only data, the post week is slower than the pre week in the weekday 17:30-20:00 window for all four observed directions. This pattern requires a control corridor, weather checks, or both before it is used as reduced-form evidence.
- Added a treatment-direction travel-time diagnostic: `data_work/src/shenzhen_liuxian_gaode/analyze_treatment_westeast_travel_time.py`.
- Treatment-direction travel time is defined as the sum of `segment length / speed` across the three common west-to-east Gaode segments, total length 507.3 m.
- Retained treatment-direction figures:
  - `figures/treatment_westeast_travel_time_10min_weekday_weekend.png`;
  - `figures/treatment_westeast_travel_time_hourly_weekday_weekend.png`;
  - `figures/treatment_westeast_ratio_to_night_10min_weekday_weekend.png`;
  - `figures/treatment_westeast_ratio_to_afternoon15_10min_weekday_weekend.png`;
  - `figures/treatment_westeast_daily_pm_core_travel_time.png`;
  - `figures/treatment_westeast_peak_reference_ratio_bars.png`.
- Retained treatment-direction data and metrics:
  - `data/treatment_westeast_corridor_10min.csv`;
  - `data/treatment_westeast_diurnal_10min.csv`;
  - `data/treatment_westeast_diurnal_hourly.csv`;
  - `data/treatment_westeast_peak_reference_ratios_daily.csv`;
  - `metrics/treatment_westeast_peak_reference_ratio_summary.csv`;
  - `metrics/treatment_westeast_segment_set.csv`;
  - `metrics/treatment_westeast_travel_time_metadata.json`.
- The treatment-direction diagnostic keeps the same qualitative pattern after same-day normalization. For weekday 17:30-20:00, mean corridor travel time rises from 0.935 minutes in the pre week to 1.196 minutes in the post week. Relative to same-day night travel time, the ratio rises from 1.690 to 2.100; relative to same-day 15:00-16:00 travel time, the ratio rises from 1.400 to 1.716. This treatment-only pattern is not a causal estimate and should be interpreted only with a control corridor or weather checks.
- Added extra robustness diagnostics: `data_work/src/shenzhen_liuxian_gaode/robustness_liuxian_patterns.py`.
- Retained robustness figures:
  - `figures/robustness_westeast_pm_subwindow_travel_time.png`;
  - `figures/robustness_direction_ratio_to_afternoon15.png`;
  - `figures/robustness_direction_ratio_to_night.png`;
  - `figures/robustness_westeast_segment_ratio_to_afternoon15.png`;
  - `figures/robustness_westeast_segment_ratio_to_night.png`.
- Retained robustness metrics:
  - `metrics/robustness_westeast_sensitivity_summary.csv`;
  - `metrics/robustness_westeast_pm_subwindow_summary.csv`;
  - `metrics/robustness_direction_peak_reference_ratio_summary.csv`;
  - `metrics/robustness_segment_peak_reference_ratio_summary.csv`.
- The extra diagnostics imply that the west-to-east worsening is not a single-Friday artifact: excluding Friday still gives a post/pre PM-core travel-time ratio of 1.270, and the PM-core to 15:00 ratio rises from 1.419 to 1.695.
- The west-to-east worsening is concentrated after 17:00, with the largest half-hour gap at 18:00-18:30. The segment-level increase is strongest on `44196 / 留仙大道-179`.
- Same-corridor placebo directions also become more congested in the post week. Relative to the 15:00 reference, the PM-core ratio rises by 0.315 for west-to-east, 0.418 for east-to-west, 0.991 for north-to-south, and 0.448 for south-to-north. This points toward an area or week-level shock rather than a treatment-direction-specific gain.
- Public-source shock scan retained:
  - the official 2026-02 traffic-management monthly data reported Metro Line 27 occupancy on `学苑大道（塘源路至留仙大道）` and `留仙大道（学苑大道至长岭陂地铁站）`;
  - the official 2026-03 monthly data reported Metro Line 27 occupancy on `学苑大道（塘开路至南科二路）`;
  - the official 2026-04 monthly data did not list Liuxian Avenue among the highlighted occupancy locations, but reported 616 road-occupation permits and 448 illegal road-occupation enforcement actions citywide;
  - Shenzhen weather reporting on 2026-04-15 and 2026-04-17 indicated thunder showers, morning/evening fog, and local heavier rain around 2026-04-17 to 2026-04-18.
- Added a two-panel event-study script: `data_work/src/shenzhen_liuxian_gaode/event_study_liuxian.py`.
- Retained event-study outputs:
  - `figures/event_study_treatment_westeast_pm.png`;
  - `metrics/event_study_pm10min_coefficients.csv`;
  - `metrics/event_study_pm_over_15_daily_coefficients.csv`;
  - `metrics/event_study_metadata.json`;
  - `data/event_study_pm10min_sample.csv`;
  - `data/event_study_pm_over_15_daily_sample.csv`.
- Event-study specification: treated segments are the three west-to-east common Liuxian segments; controls are the other common Liuxian segments. The omitted event day is 2026-03-06, the last observed pre-policy weekday. Panel A uses segment and exact 10-minute timestamp fixed effects in the 17:30-20:00 window. Panel B uses daily `log(PM core) - log(15:00-16:00)` with segment and date fixed effects.
- The DID event-study coefficients imply that the treatment direction is not worse than same-corridor controls after common timestamp shocks are absorbed. Post-policy coefficients are imprecise, but they turn negative after 2026-04-15 in both the raw PM window and the PM-to-15:00 normalized specification. This pattern supports the interpretation that the treatment-only post-week worsening is partly driven by common or control-direction shocks.
- Added a speed-level event-study script: `data_work/src/shenzhen_liuxian_gaode/event_study_liuxian_speed.py`.
- Retained speed event-study outputs:
  - `figures/event_study_speed_westeast_pm.png`;
  - `metrics/event_study_speed_pm_coefficients.csv`;
  - `metrics/event_study_speed_metadata.json`;
  - `data/event_study_speed_pm_sample.csv`.
- Speed event-study specification: the outcome is segment speed in km/h. The sample is restricted to weekdays, 17:30-20:00. Treated segments are west-to-east common segments; control segments are the remaining common Liuxian segments. The model absorbs segment fixed effects and exact 10-minute timestamp fixed effects. Standard errors are clustered by segment. The omitted event day is 2026-03-06.
- Speed coefficients are centered near zero in the pre week. Post-policy coefficients are -1.84 km/h on 2026-04-13, -1.01 km/h on 2026-04-14, +3.15 km/h on 2026-04-15, +3.89 km/h on 2026-04-16, and +1.97 km/h on 2026-04-17. All post-policy confidence intervals include zero.
- Extended the speed event-study script to include weekend placebo days while retaining 2026-03-06 as the omitted day.
- Retained weekend-extended speed outputs:
  - `figures/event_study_speed_westeast_pm_with_weekend.png`;
  - `metrics/event_study_speed_pm_with_weekend_coefficients.csv`;
  - `metrics/event_study_speed_with_weekend_metadata.json`;
  - `data/event_study_speed_pm_with_weekend_sample.csv`.
- The weekend-extended speed event study has 2,940 observations, 14 segment clusters, and 210 exact timestamp fixed effects. Pre-weekend coefficients are -1.51 km/h on Saturday and -1.12 km/h on Sunday. Post-weekend coefficients are -1.30 km/h on Saturday and -0.08 km/h on Sunday. These placebo coefficients are close to zero and imprecise.
- Added a connected-chain window-speed script: `data_work/src/shenzhen_liuxian_gaode/plot_chain_window_speeds.py`.
- Retained connected-chain window-speed outputs:
  - `figures/chain_direction_daily_weekday_window_speeds.png`;
  - `figures/chain_direction_weekday_window_speed_changes.png`;
  - `data/chain_direction_daily_weekday_window_speeds.csv`;
  - `metrics/chain_direction_weekday_window_speed_summary.csv`;
  - `metrics/chain_direction_segment_inventory_common.csv`;
  - `metrics/chain_direction_window_speed_metadata.json`.
- Direction chains are inferred from connected segment coordinates rather than the Gaode raw direction label. Eastbound is the southwest-to-northeast chain toward Fulong Road; westbound is the northeast-to-southwest chain toward Xueyuan Avenue. The retained sample is balanced common segments only, excluding post-only endpoint segments.
- For 15:00-17:30 on weekdays, eastbound speed falls from 40.26 km/h to 38.25 km/h, and westbound speed falls from 29.98 km/h to 28.29 km/h. For 17:30-20:00 on weekdays, eastbound speed falls from 34.26 km/h to 27.21 km/h, and westbound speed falls from 27.94 km/h to 19.98 km/h. The post-pre slowdown is concentrated in the tidal-lane window for both directions, with a larger absolute decline on westbound.
- Added an operating-window DDD script: `data_work/src/shenzhen_liuxian_gaode/ddd_operating_window_liuxian.py`.
- Retained DDD outputs:
  - `figures/ddd_operating_window_event_study.png`;
  - `metrics/ddd_operating_window_coefficients.csv`;
  - `metrics/ddd_operating_window_metadata.json`;
  - `data/ddd_operating_window_weekday_all_day_sample.csv`;
  - `data/ddd_operating_window_weekday_shoulder_1500_2200_sample.csv`.
- DDD specification: treated segments are the west-to-east common segments; the active window is weekday 17:30-20:00. The main absorbed fixed effect is segment-by-10-minute-of-day, which compares each segment to its own stable diurnal profile. Three second fixed-effect choices were retained: date FE, date-by-active-window FE, and exact timestamp FE.
- The all-day date-FE DDD point estimate is +0.165 log points, or +18.0%, with a segment-clustered standard error of 0.111. The shoulder-window date-FE estimate is +0.071 log points, or +7.3%, with a standard error of 0.121. After absorbing date-by-active-window shocks, the estimate is -0.105 log points, or -10.0%, with a standard error of 0.172. None of these estimates is precise in the 14-segment sample.
- Interpretation retained for future purchase decisions: exploiting non-operating hours can shift the point estimate toward a positive active-window effect, but inference remains weak because the effective treatment variation is three west-to-east segments across five post weekdays, and the result is sensitive to whether post-week evening-peak shocks are absorbed as common shocks.

## 2026-06-27: Liuxian Avenue Gaode-to-Baidu raw road-segment match

- Added and ran `data_work/src/shenzhen_liuxian_gaode/match_baidu_underlying_segments.py`.
- The script treats the purchased Gaode coordinates as GCJ-02, converts them to WGS84, and matches the resulting target corridor to `data_work/raw_data/gis/roads_baidu/shenzhen_roads.shp` with a 120 m search buffer.
- Main matched Baidu `roadseg_id` records:
  - eastbound chain, `西南向东北`: `RS_NB1559531813_NB1520996562_1`, `RS_NB1520996562_NB1520991989_1`, `RS_NB1520991989_NB1520996131_1`.
  - westbound chain, `东北向西南`: `RS_NB1521004399_NB1520991671_1`, `RS_NB1520991671_NB1521007357_1`, `RS_NB1521007357_NB1530662314_1`.
- Five of the six main Liuxian Baidu segments have speed observations in `data_work/raw_data/speed_shenzhen_all_wgs84.csv` for 2025-05-19 through 2025-05-25. `RS_NB1520996562_NB1520991989_1` appears in the road network but not in that speed panel.
- Outputs:
  - `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/metrics/baidu_liuxian_underlying_candidates.csv`
  - `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/data/baidu_liuxian_underlying_speed_panel_20250519_20250525.csv`
  - `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/metrics/baidu_liuxian_underlying_speed_by_segment.csv`
  - `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/metrics/baidu_liuxian_underlying_speed_window_summary.csv`
  - `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/figures/baidu_liuxian_underlying_match_map.png`
- Added coordinate-shift diagnostic script:
  - `data_work/src/shenzhen_liuxian_gaode/diagnose_baidu_gaode_coordinate_shift.py`
- The parallel Baidu-Gaode overlay is consistent with a nearly common translation, not a wrong road match. Applying the median Gaode-to-Baidu translation by direction moves the Gaode line by roughly 73--75 m west and 73 m north. Median distance from the Gaode target line to the matched Baidu line falls from about 105 m to 7 m in both directions.
- Retained diagnostic outputs:
  - `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/metrics/baidu_gaode_coordinate_shift_diagnostic.csv`
  - `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/figures/baidu_liuxian_coordinate_shift_diagnostic.png`
- Added shifted-overlap speed comparison script:
  - `data_work/src/shenzhen_liuxian_gaode/compare_baidu_shifted_overlap_with_gaode_weeks.py`
- The comparison applies the estimated Gaode-to-Baidu translation, selects same-direction Baidu `留仙大道` segments that overlap the shifted Gaode target line, and compares the Baidu 2025-05-19 to 2025-05-25 10-minute speed panel with the purchased Gaode pre and post weeks. Speeds are aggregated as length-weighted harmonic corridor speeds within source-period, direction, and 10-minute timestamp.
- The shifted-overlap Baidu selection retains six `留仙大道` road segments, three per direction. Five have speed observations in the 2025 Baidu week; `RS_NB1520996562_NB1520991989_1` is present in the Baidu road network but not in that speed panel.
- Retained comparison outputs:
  - `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/figures/baidu_gaode_shifted_overlap_weekly_10min_speed_compare.png`
  - `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/figures/baidu_gaode_shifted_overlap_weekday_diurnal_compare.png`
  - `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/data/baidu_gaode_shifted_overlap_weekly_10min_speed_compare.csv`
  - `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/metrics/baidu_shifted_overlap_segments.csv`
  - `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/metrics/baidu_gaode_shifted_overlap_speed_window_summary.csv`
- Descriptive pattern retained: the Baidu 2025 week has a clear within-day speed-flow profile on the matched corridor, but the westbound chain is substantially faster than the Gaode weeks in level. Within the purchased Gaode data, the post week has deeper weekday 17:30-20:00 speed troughs in both chains, especially the westbound chain.
- Added key-window average-speed map script:
  - `data_work/src/shenzhen_liuxian_gaode/plot_shifted_overlap_speed_maps.py`
- The map uses the same shifted-overlap geometry convention: Gaode segments are shifted by the estimated Gaode-to-Baidu translation; Baidu selected roadseg geometries are clipped to the shifted target overlap for display, with raw roadseg speeds inherited from the Baidu speed panel.
- Retained mapped windows:
  - night baseline: `00:00-05:00`;
  - weekday reference window: `15:00-17:30`;
  - weekday tidal-lane window: `17:30-20:00`.
- Retained map outputs:
  - `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/figures/shifted_overlap_key_window_speed_maps.png`
  - `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/figures/shifted_overlap_key_window_speed_maps_zoomed.png`
  - `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/data/shifted_overlap_segment_window_speeds_for_maps.csv`
  - `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/metrics/shifted_overlap_segment_window_speed_map_source_table.csv`
  - `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/metrics/shifted_overlap_key_window_speed_maps_metadata.json`
- The map now applies an 8 m outward display offset by direction and uses narrower road-color strokes. This is a visualization-only offset; the underlying speed calculation, Baidu overlap selection, and shifted Gaode geometry remain unchanged.
- The map script now uses all observed Gaode segments within each source period instead of the balanced common-segment sample. Gaode pre map cells retain 14 observed segments per window, while Gaode post map cells retain 16 observed segments per window, including the two post-only endpoint segments `11160` and `12214`.
- Added post-week daily active-window ratio diagnostic:
  - `data_work/src/shenzhen_liuxian_gaode/plot_post_daily_window_speed_ratios.py`
- The diagnostic uses the shifted-overlap corridor-speed panel and restricts to the purchased Gaode post week. For each workday and each direction, it computes the ratio of mean speed in `17:30-20:00` to mean speed in `15:00-17:30`. It also computes the directional speed ratio `SW -> NE / NE -> SW` before and during the tidal-lane window, plus the ratio-of-ratios `(SW -> NE active/reference) / (NE -> SW active/reference)`.
- Retained outputs:
  - `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/figures/gaode_post_daily_window_speed_ratios.png`
  - `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/data/gaode_post_daily_active_reference_speed_ratios.csv`
  - `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/data/gaode_post_daily_direction_speed_ratios_by_window.csv`
  - `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/metrics/gaode_post_daily_ratio_of_ratios.csv`
- Descriptive pattern retained: on 2026-04-13, 2026-04-14, and 2026-04-16, the `SW -> NE` direction slows more relative to its own 15:00-17:30 speed than `NE -> SW`. On 2026-04-15 and 2026-04-17, the relative slowdown is larger for `NE -> SW`, with the Friday ratio-of-ratios driven by a very low `NE -> SW` active-window speed.
- Added pre-post active/reference ratio comparison:
  - `data_work/src/shenzhen_liuxian_gaode/plot_pre_post_daily_window_speed_ratios.py`
- Retained outputs:
  - `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/figures/gaode_pre_post_daily_active_reference_speed_ratios.png`
  - `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/data/gaode_pre_post_daily_active_reference_speed_ratios.csv`
  - `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/metrics/gaode_pre_post_active_reference_speed_ratio_summary.csv`
- Descriptive pattern retained: the pre week also has lower `17:30-20:00` speed than `15:00-17:30` for `SW -> NE` on every weekday, with mean active/reference speed ratio 0.855. For `NE -> SW`, the pre-week mean ratio is 0.946 and the ratio exceeds 1 on Thursday and Friday. In the post week, both directions have lower active-window speed on every weekday, with mean ratios 0.719 for `SW -> NE` and 0.690 for `NE -> SW`.
- Added a full-post-sample diagnostic without the common-segment restriction:
  - `data_work/src/shenzhen_liuxian_gaode/analyze_post_full_segments.py`
- Retained full-post outputs:
  - `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/figures/gaode_post_full_diurnal_by_daytype_chain.png`
  - `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/figures/gaode_post_full_daily_window_ratios.png`
  - `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/figures/gaode_post_full_segment_active_reference.png`
  - `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/data/gaode_post_full_10min_corridor_speed.csv`
  - `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/data/gaode_post_full_daily_window_summary.csv`
  - `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/data/gaode_post_full_daily_active_reference_ratios.csv`
  - `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/data/gaode_post_full_daily_direction_speed_ratios_by_window.csv`
  - `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/metrics/gaode_post_full_window_summary.csv`
  - `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/metrics/gaode_post_full_segment_active_reference_ratios.csv`
  - `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/metrics/gaode_post_full_ratio_of_ratios.csv`
  - `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/metrics/gaode_post_full_analysis_metadata.json`
- The full post sample contains 8 segments in each direction and 1,008 ten-minute timestamps per direction. Relative to the common-segment post sample, adding the post-only endpoints reduces `SW -> NE` speed by 2.62 km/h in `15:00-17:30` and by 1.27 km/h in `17:30-20:00`; it raises `NE -> SW` speed by 0.41 km/h and 0.36 km/h in the same windows.
- In the full post sample, mean weekday `17:30-20:00` speed is 25.94 km/h for `SW -> NE` and 20.35 km/h for `NE -> SW`. The active/reference speed ratios are 0.735 and 0.693, respectively. The `SW -> NE` post-only endpoint `11160 / 留仙大道-44` accounts for 25.1% of active-window travel time in that direction; the `NE -> SW` post-only endpoint `12214 / 留仙大道-50` accounts for 3.9%.
- The full-post diurnal figure `gaode_post_full_diurnal_by_daytype_chain.png` now uses weekday observations only and overlays both chain directions in one panel for direct visual comparison.
- Added compressed-axis event-study variants:
  - `data_work/src/shenzhen_liuxian_gaode/event_study_compressed_controls.py`
- Retained compressed event-study outputs:
  - `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/figures/event_study_compressed_control_methods.png`
  - `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/metrics/event_study_compressed_control_coefficients.csv`
  - `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/metrics/event_study_compressed_control_metadata.json`
  - `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/metrics/event_study_compressed_date_index.csv`
  - `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/data/event_study_compressed_control_samples.csv`
- The compressed x-axis places the observed pre weekdays and post weekdays next to each other, with 2026-03-06 as the omitted pre day. The retained figure now contains two specifications: `SW -> NE` common segments against `NE -> SW` common segments; and a chain-level active/reference DDD using 15:00-17:30 as non-active control variation.
- Both retained specifications use common segments only, because post-only endpoints lack within-segment pre-policy observations.
- Across the retained compressed specifications, post coefficients remain imprecise. The chain-level common-segment DID gives post coefficients of -0.03, 0.10, 6.72, 5.21, and 5.60 km/h from 2026-04-13 to 2026-04-17, all with confidence intervals crossing zero.
- Added a treatment-chain-only pre/post diagnostic:
  - `data_work/src/shenzhen_liuxian_gaode/plot_treatment_chain_before_after.py`
- Retained outputs:
  - `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/figures/treatment_chain_before_after_weekday_windows.png`
  - `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/figures/treatment_chain_before_after_weekday_windows.pdf`
  - `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/figures/treatment_chain_before_after_weekday_windows.svg`
  - `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/data/treatment_chain_before_after_weekday_windows.csv`
  - `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/data/treatment_chain_active_reference_daily_ratios.csv`
  - `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/metrics/treatment_chain_before_after_weekday_summary.csv`
  - `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/metrics/treatment_chain_active_reference_ratio_summary.csv`
  - `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/metrics/treatment_chain_before_after_metadata.json`
- The balanced common-segment treatment chain (`SW -> NE`) slows from 40.26 to 38.25 km/h in 15:00--17:30 and from 34.26 to 27.21 km/h in 17:30--20:00. The active/reference speed ratio falls from 0.855 in the pre week to 0.719 in the post week.

## 2026-06-26: Shenzhen Liuxian Avenue DID purchase design recorded

- Added `Documents/L2_notes/shenzhen_liuxian_did_purchase_design_20260626.md`.
- Locked reduced-form data-purchase design:
  - treatment: 留仙大道（学苑大道-福龙路），西往东，工作日晚高峰 17:30-20:00；
  - main control: 龙珠大道（原茶光路段，沙河西路-同乐路；可按研究需要裁剪到沙河西路-珠光北路），西往东；
  - auxiliary spillover check: 龙珠大道另一段（例如龙珠立交-龙井路），西往东，不作为主 DID 对照。
- Naming correction:
  - 茶光路 and 龙珠大道 should not be treated as two separate candidate roads for this design;
  - the relevant control is the former 茶光路 segment under the current 龙珠大道 name.
- Official treatment timing retained from 深圳公安/交警 sources:
  - formal implementation: 2026-03-23 to 2027-03-22;
  - prior trial run exists, so the week immediately before 2026-03-23 should be treated as a transition window if purchased.
- Shenzhen traffic police news details were added to the design note:
  - policy starts at the 2026-03-23 evening peak on 留仙大道（学苑大道-福龙路）;
  - the operated lane is the east-to-west inner lane, switched to west-to-east one-way traffic during the evening peak;
  - 17:30-20:00 is the stated workday operating window, but activation can occur earlier when west-to-east queues arrive early;
  - the lane serves traffic toward Shenzhen North Station and not vehicles turning from 留仙大道 into 福龙路;
  - the reported trial-run effects are 10-15 minutes saved for Shenzhen North Station-bound traffic and a 50% decline in west-to-east queue length.
- Recommended purchase window:
  - clean pre: 2026-03-02 to 2026-03-13 if available;
  - transition: 2026-03-16 to 2026-03-22;
  - post: 2026-03-23 to 2026-04-10, with robustness excluding the first formal week.
- Initial purchase recorded:
  - 留仙大道（学苑大道-福龙路）only;
  - bidirectional road-segment speed data for the roughly 2 km treated corridor;
  - 10-minute frequency;
  - pre week: 2026-03-02 to 2026-03-08;
  - post week: 2026-04-13 to 2026-04-19.
- Event-study feasibility judgment:
  - the initial two-week treatment-only purchase supports event-time descriptive plots and before-after reduced-form evidence;
  - it should not be described as a full event study because the data do not contain multiple pre-event windows or continuous observations around the 2026-03-23 implementation date.
- Earlier project `.md` files did not contain this specific Liuxian/control-segment design; this note is now the canonical record for the Shenzhen reduced-form data purchase.

## 2026-06-07: `raw_connected_v2` normalized to the v1 source-input interface

- Normalized the user-provided `raw_connected_v2` processed network in `data_work/src/raw_connected_v2/Processed_Data/`.
- Added reusable code:
  - `data_work/src/raw_connected_v2/normalize_raw_connected_v2.py`
- The script made a one-time backup of the pre-normalized inputs under:
  - `data_work/src/raw_connected_v2/Processed_Data/_source_before_normalize/`
- The v2 source folder now has v1-compatible top-level files:
  - `nodes.xlsx`
  - `directed_edges.xlsx`
  - `road_list.xlsx`
  - `segment_endpoints_nodes.xlsx`
- `directed_edges.xlsx` now has complete v1-compatible geometry and endpoint columns:
  - 9,552 edges received raw `RS_*` WKT geometry from `data_work/raw_data/speed_Beijing_all_wgs84.csv`.
  - 336 `MANUAL_*` edges received node-to-node `LINESTRING` geometry.
  - No edge has missing geometry after normalization.
- Added v1-compatible speed cache files under `data_work/src/raw_connected_v2/Processed_Data/speed_cache/`:
  - `raw_connected_speed_am_by_roadseg.csv`
  - `raw_connected_speed_pm_by_roadseg.csv`
  - `raw_connected_speed_ff_by_roadseg.csv`
  - `raw_connected_speed_all_by_roadseg.csv`
  - `raw_connected_v2_edges_without_speed.csv`
- Loader validation:
  - `nodes.xlsx`: 5,894 rows.
  - `directed_edges.xlsx`: 9,888 rows.
  - AM loader-cleaned edges: 9,352.
  - Manual edges retained after cleaning: 336.
  - Edges without v2 speed records: 80.
- Smoke run completed successfully:
  - command root: `data_work/`
  - output: `outputs/raw_connected_v2_grid_travel_time_edgeproj_v1_smoke/`
  - command:
    `python -m src.raw_connected_v1.grid_travel_time.build_raw_connected_v1_grid_travel_time_edgeproj --raw-network-dir src/raw_connected_v2/Processed_Data --speed-cache-dir src/raw_connected_v2/Processed_Data/speed_cache --output-root outputs/raw_connected_v2_grid_travel_time_edgeproj_v1_smoke --grid-type square --period AM --pair-scope adjacent --comparison-level adjacent`
- Reuse note: future full v2 grid-travel reruns can keep the existing raw-connected scripts and change only `--raw-network-dir`, `--speed-cache-dir`, and `--output-root`.

## 2026-06-07: full `raw_connected_v2` edgeproj rerun, diagnostics, and v1-v2 comparison

- Generated the full v2 edge-projection raw-grid travel-cost version:
  - `data_work/outputs/raw_connected_v2_grid_travel_time_edgeproj_v1/`
- The rerun used the existing edge-projection builder with only the raw network, speed cache, and output root changed:
  - `--raw-network-dir src/raw_connected_v2/Processed_Data`
  - `--speed-cache-dir src/raw_connected_v2/Processed_Data/speed_cache`
  - `--baseline-version-root outputs/manual_centerline_rules_v11`
  - `--pair-specific`
- Completed both periods and all retained grid systems:
  - `AM`, `PM`
  - `square`, `hex`, `voronoi`
- Generated the same diagnostic layers as the retained v1 edgeproj branch:
  - coverage maps and tables under `figures/grid_coverage_maps/`, `data/grid_coverage_maps/`, and `metrics/grid_coverage_maps/`
  - tidal-asymmetry maps and tables under `figures/grid_tidal_asymmetry/`, `data/grid_tidal_asymmetry/`, and `metrics/grid_tidal_asymmetry/`
- Added reusable comparison script:
  - `data_work/src/diagnostics/compare_raw_connected_v1_v2.py`
- Comparison outputs:
  - `data_work/outputs/raw_connected_v2_grid_travel_time_edgeproj_v1/diagnostics_compare_v1_v2/travel_cost_summary_by_version.csv`
  - `data_work/outputs/raw_connected_v2_grid_travel_time_edgeproj_v1/diagnostics_compare_v1_v2/travel_cost_v1_v2_overlap.csv`
  - `data_work/outputs/raw_connected_v2_grid_travel_time_edgeproj_v1/diagnostics_compare_v1_v2/grid_coverage_v1_v2_compare.csv`
  - `data_work/outputs/raw_connected_v2_grid_travel_time_edgeproj_v1/diagnostics_compare_v1_v2/grid_tidal_asymmetry_v1_v2_compare.csv`
  - `data_work/outputs/raw_connected_v2_grid_travel_time_edgeproj_v1/diagnostics_compare_v1_v2/replication_square_input_v1_v2_compare.csv`
  - `data_work/outputs/raw_connected_v2_grid_travel_time_edgeproj_v1/diagnostics_compare_v1_v2/tidal_lane_specC_v1_v2_compare.csv`
- Main travel-cost comparison:
  - square finite adjacent rows: v1 `3,666/4,632` in AM and `3,668/4,632` in PM; v2 `609/4,632` in both AM and PM.
  - hex finite adjacent rows: v1 `5,368/6,422` in AM and `5,367/6,422` in PM; v2 `1,032/6,422` in AM and `1,028/6,422` in PM.
  - voronoi finite adjacent rows: v1 `3,450/4,019` in AM and `3,447/4,019` in PM; v2 `999/4,019` in both AM and PM.
  - On overlapping finite links, median `v2/v1` travel-time ratio is `1.0` for all grid-periods; v2 is mostly a narrower connected subset rather than a different travel-time value on shared links.
- Main coverage comparison:
  - square covered grid share falls from `42.5%` to `6.2%`; covered resident share falls from `95.5%` to `47.0%`.
  - hex covered grid share falls from `41.1%` to `6.3%`; covered resident share falls from `95.6%` to `47.3%`.
  - voronoi covered grid share falls from `87.9%` to `24.2%`; covered resident share falls from `97.5%` to `48.3%`.
- Main tidal-asymmetry comparison:
  - both-direction undirected pairs fall sharply: square AM `1,763 -> 285`, hex AM `2,574 -> 489`, voronoi AM `1,658 -> 468`.
  - median directional ratios are lower in v2 for most grid-periods, implying stronger asymmetry inside the narrower connected subset.
- Generated a v2 square AA / Replication input bundle without overwriting the retained v1 bundle:
  - code: `Replication_apply/prepare_from_data_work_square_v2_raw_connected_v2/build_inputs_from_data_work_square_v2_raw_connected_v2.py`
  - inputs: `Replication_apply/aa_input_square_v2_raw_connected_v2/`
  - synced counterfactual folder: `Replication_apply/counterfactuals/seattle_raw_connected_v2/`
  - summary: `Replication_apply/prepare_from_data_work_square_v2_raw_connected_v2/build_summary_square.json`
- Replication square input comparison:
  - node rows: `2,723 -> 2,718`
  - edge rows: `3,666 -> 609`
  - OD rows unchanged: `377,708`
  - traffic mean rises from `5.875` to `8.184`
- Added and ran a v2 Spec C wrapper without overwriting existing results:
  - code: `Replication_apply/python_commuting/tidal_lane_specC_raw_connected_v2.py`
  - output: `Replication_apply/results/tidal_lane_raw_connected_v2/`
- Spec C comparison:
  - treated pairs remain `100`.
  - top-100 asymmetry ratio range changes from `3.24--25.17` in v1 to `1.66--7.37` in v2.
  - welfare index remains very close: `(chi_hat - 1) * 1e6 = -133.09` in v1 and `-135.04` in v2.
  - resident and worker response dispersion is much smaller in v2, consistent with the narrower active edge subset.

### Route-audit figures for manual path inspection

- Generated v2 square/AM route-audit figures with the existing edge-projection audit script:
  - shared v1 audit pairs:
    `data_work/outputs/raw_connected_v2_grid_travel_time_edgeproj_v1/route_audit_square_AM_shared20/`
  - random v2 finite pairs:
    `data_work/outputs/raw_connected_v2_grid_travel_time_edgeproj_v1/route_audit_square_AM_random20/`
- Each audit directory contains:
  - route PNGs
  - `sampled_adjacent_pairs.csv`
  - `sampled_adjacent_route_summary.csv`
  - `run_note.txt`
- The shared-pair audit produced only 3 route figures because most of the old v1 shared audit pairs are not finite in v2. This is consistent with the v2 coverage contraction documented above.
- The random v2 audit produced 20 route figures. Several sampled adjacent pairs have much longer raw-connected times than the old grid baseline and should be reviewed visually before treating v2 as a final travel-cost source:
  - `sq_22_42 -> sq_22_43`: `57.94` min versus old `5.38` min.
  - `sq_23_31 -> sq_23_32`: `41.00` min versus old `6.52` min.
  - `sq_31_31 -> sq_31_32`: `70.93` min versus old `6.49` min.
  - `sq_19_39 -> sq_19_38` in the shared audit: `41.51` min versus old `4.53` min.

## 2026-05-27: raw segment × Baidu API validation pipeline (parallel to centerline)

- **Purpose**: compare Baidu direction API travel cost on **raw road segments** (with waypoints + saved route geometry) against **direct speed observations** from `speed_Beijing_all_wgs84.csv`; separate from centerline validation for next-day comparison.
- **Landing location**:
  - code: `data_work/src/raw_baidu_validation/`
  - workflow doc: `data_work/docs/RAW_BAIDU_VALIDATION.md`
  - config: `data_work/config/raw_baidu_validation_beijing_v1.json`
  - output root: `data_work/outputs/raw_baidu_validation_beijing_v1/`
  - schedule script: `data_work/scripts/run_raw_validation_week.sh`
- **Manifest (built)**:
  - **23,799** baseline raw segments; **23,583** with direct speed obs; **75%** with waypoints
  - `build_crawl_manifest.py` → `data/crawl_manifest.parquet`
- **AK coordination**:
  - shared file lock: `outputs/.baidu_ak_global.lock` via `src/national_tidal_commuting/baidu_rate_limit.py`
  - enabled in both centerline and raw configs (`min_interval_sec=0.35`)
- **Formal crawl schedule**: same as centerline (FREE 02:30, AM 07:30, PM 17:30); d01 `steps_info=1`, d02+ off
- **Next**: start `run_raw_validation_week.sh` from 2026-05-28; build comparison table after first full day

## 2026-05-25: centerline × Baidu API validation — preflight complete (Beijing baseline)

- **Purpose**: validate inferred centerline travel time (`length / aggregated speed`) against Baidu driving API travel cost on the same baseline corridors.
- **Landing location**:
  - code: `data_work/src/centerline_baidu_validation/`
  - workflow doc: `data_work/docs/CENTERLINE_BAIDU_VALIDATION.md`
  - config: `data_work/config/centerline_baidu_validation_beijing_v1.json`
  - output root: `data_work/outputs/centerline_baidu_validation_beijing_v1/`
- **Manifest (locked)**:
  - **11,890** baseline directed rows (5,945 undirected × AB/BA); **8,342** with inferred speed
  - `build_crawl_manifest.py` → `data/crawl_manifest.parquet`
- **Preflight tests run (2026-05-25)**:
  - manifest static check ✓
  - `run_preflight_tests.py` (coord wgs84 vs gcj02 × waypoints × gps, n=10 stratified) ✓
  - `query_baidu_direction.py` smoke: FREE 5/5, AM 3/3, PM 3/3 ✓
  - `compare_waypoint_routing.py` head-10 (script OK; head rows bind poorly) ✓
  - random n=20 from `has_inferred_speed` (300–5000 m): wp median dist/len=2.76, OD=0.76 ✓
  - **Report**: `outputs/.../metrics/PREFLIGHT_SUMMARY.md`
- **Locked from preflight**:
  - `coord_type=wgs84` (gcj02 worse when coords are WGS84)
  - keep **waypoints + gps_direction** for formal crawl
  - post-crawl QC: `route_bind_ok = distance/length ∈ [0.6, 1.8]` before travel-time comparison
  - do **not** use manifest head rows for smoke quality judgment
- **Go for formal crawl**: yes, with QC filter on comparison table
- **Next**:
  - optional 100–200 stratified FREE crawl to estimate QC pass rate
  - `flatten_api_responses.py`, `build_comparison_table.py` (with `route_bind_ok`), figures, weekly scheduler

## 2026-05-14: full-estimation level-form scaffold (v1)

- Added the level-form (non-exact-hat) Estimation framework requested by the 2026-05-02 meeting under `data_work/src/model/full_estimation/`:
  - `build_primitives.py`, `parameters.py`, `lambda_diagnostic.py`, `invert_fundamentals.py`, `equilibrium_solve.py`, `counterfactual_levels.py`, `run_full_estimation.py`.
  - Documentation: `README.md`, `WORKFLOW.md`, `ESTIMATION_NOTES.md` (per-step derivation, formulas, solver choices, parameter sources, TODOs).
- Locked decisions for v1:
  - parameters use AA(2022) Seattle values verbatim, `theta=6.83, alpha=-0.12, beta=-0.10, lambda=delta1/theta=0.488/6.83`; will be revisited against Professor You Wei's work before any estimation is run.
  - no exact-hat; every counterfactual re-solves the equilibrium in levels.
  - grid travel-cost convention stays open — loader takes `--version-id`.
- Required spectral-radius safety: `A_kl = 1/(kappa * tau_obs_min)` with `kappa` chosen so max row-sum ≤ 0.9; the same `kappa` flows through inversion and equilibrium solve. CF ratios are invariant to `kappa`; absolute `Wbar` is not (documented in `ESTIMATION_NOTES.md`).
- Smoke test on `manual_centerline_rules_v11` square AM:
  - N=2736 nodes, M=4087 edges, Lbar≈95k.
  - Inversion residuals on valid nodes (1718): RMS≈4e-4 on R, ~0 on F.
  - Equilibrium outer iteration ≈10s; full convergence at default `max_outer=60` is meant to run on the user Mac, not sandbox.
- Outputs target: `data_work/outputs/full_estimation_square_v1/` (created on first CLI run).
- Counterfactuals CF1 (symmetric), CF2 (tidal-lane top-K), CF3 (uniform lane sweep over 1.10/1.25/1.50) are all wired into the CLI driver.

## 2026-05-13: national top-100 city list switched to 2025 Baidu commute-peak congestion ranking (xlsx)

- Added config `data_work/config/national_tidal_commuting_baidu2025_top100_congestion_v1.json`.
- City-selection source: `data_work/raw_data/baidu_2025_commuting_peak_congestion_ranking.xlsx`, sheet `百城通勤拥堵榜2025`, columns `城市` and `通勤高峰拥堵指数` (read directly as `source_congestion_index` after join to `commuting_town_2021.dta`).
- `city_aliases` retained for `大理` → `大理白族自治州` (same as 2024 config pattern).
- Rebuilt sample with:
  - `python src/national_tidal_commuting/build_top_pairs.py --config config/national_tidal_commuting_baidu2025_top100_congestion_v1.json`
  - output root `data_work/outputs/national_tidal_commuting_baidu2025_top100_congestion_v1/`
  - `7461` OD pairs in `data/top_commuting_pairs.csv` (same pair-count rule as prior top-100 congestion builds)
  - `metrics/source_congestion_city_ranking.csv` and `metrics/top_commuting_pairs_city_summary.csv` include `source_congestion_index` for scatter plots after crawl.
- Sanya remains in the 100-city source table but still exports zero non-same-town pairs under current town-coordinate rules.
- Updated `data_work/docs/NATIONAL_TIDAL_COMMUTING_WORKFLOW.md` and `nextstep.md` to point crawl work at the 2025 config by default.

## 2026-05-02: collaborator meeting notes recorded and live plan reset

- Added meeting notes at `Documents/L2_notes/meeting_notes_20260502.md`.
- Synced documents discussed in the meeting:
  - `Documents/L1_manuscripts/TidalLanes_0429.tex`
  - `Documents/L1_manuscripts/MODEL_replication_tidallanes.tex`
  - `Documents/L1_manuscripts/TidalLanes_Model_v4.tex`
- Retained decisions and open points:
  - missing or one-direction-only figure segments should be plotted in light gray;
  - grid-level travel-cost construction remains unsettled;
  - the national evidence should add a city-level scatter with congestion index on the x-axis and asymmetry on the y-axis, with additional crawl data if useful;
  - parameter choices should be cross-checked against Professor You Wei's related work.
- Updated `nextstep.md` so the live plan now prioritizes the full-estimation framework, pattern evidence, national scatter figure, Shenzhen reduced-form feasibility, and later parameter/data-detail refinement.

## 2026-05-02: national crawl city list switched to top-100 congestion-source cities

- Added a congestion-source city-selection branch to `data_work/src/national_tidal_commuting/town_2021_top_pairs.py`.
- New config:
  - `data_work/config/national_tidal_commuting_top100_congestion_2023_v1.json`
- City-selection source:
  - `data_work/raw_data/20190909-15百城拥堵数据-20230616.xlsx`
- Ranking rule:
  - compute city-level `source_congestion_index = mean(畅通速度 / 平均速度)` over all rows in the Excel file;
  - sort cities by this index in descending order;
  - match source city names such as `北京` to DTA city names such as `北京市`;
  - keep the previous within-city pair rule unchanged: top 50 unordered township pairs plus up to 30 supplemental long-distance pairs.
- Validation build completed with:
  - output root `data_work/outputs/national_tidal_commuting_top100_congestion_2023_v1/`
  - `100` selected source cities
  - `7461` selected OD pairs in `data/top_commuting_pairs.csv`
  - `99` cities with at least one selected pair
  - `source_congestion_city_ranking.csv` written under `metrics/`
- Caveat:
  - the Excel file name includes `20230616`, but the table's `日期` field runs from `20190909` to `20190915`;
  - Sanya is in the selected source city list, but current town-center coverage leaves no exportable non-same-town pair after the existing cleaning rules.
- Updated `data_work/docs/NATIONAL_TIDAL_COMMUTING_WORKFLOW.md` with the new config, outputs, and date caveat.

## 2026-05-02: official Baidu 2024 100-city congestion base downloaded and extracted

- Downloaded the official Baidu Map report PDF:
  - `data_work/raw_data/baidu_2024_china_city_traffic_report.pdf`
- Added extraction script:
  - `data_work/src/national_tidal_commuting/extract_baidu_2024_city_base.py`
- Extracted and retained a local 100-city base:
  - `data_work/raw_data/baidu_2024_city_congestion_base.csv`
  - `data_work/raw_data/baidu_2024_city_congestion_base.xlsx`
  - `data_work/raw_data/baidu_2024_city_congestion_base_metadata.json`
- The city base includes:
  - 2024 commute-peak congestion rank and index from attachment 1;
  - 2024 weekend congestion rank and index from attachment 2;
  - 2024 average commute duration and distance from attachment 3.
- Added config:
  - `data_work/config/national_tidal_commuting_baidu2024_top100_congestion_v1.json`
- Updated `town_2021_top_pairs.py` so the city-selection branch can read either an Excel speed table or a CSV with an already-defined congestion-index column.
- Validation build completed:
  - output root `data_work/outputs/national_tidal_commuting_baidu2024_top100_congestion_v1/`
  - `100` selected source cities
  - `7461` selected OD pairs
  - `99` cities with at least one selected pair
  - Sanya remains zero-pair under current town-coordinate coverage and existing cleaning rules.
- This 2024 official Baidu city base should supersede the older local Excel file for the next national crawl unless a deliberate historical comparison is needed.

## 2026-04-30: `MODEL_replication_tidallanes.tex` Sections 1--2 tightened

- Revised Sections 1--2 of `Documents/L1_manuscripts/MODEL_replication_tidallanes.tex` to reduce code-path detail and foreground the model contract.
- Retained the core data objects: AA sparse inputs, Beijing node/OD/edge counts, directed raw-path travel times, free-flow times, lane proxy, BPR traffic-state input, and solver outputs.
- Reframed the solver description around the minimal baseline and counterfactual fixed-point logic rather than named implementation files.
- Recompiled `Documents/L1_manuscripts/MODEL_replication_tidallanes.pdf` with `pdflatex`.

## 2026-04-30: `MODEL_replication_tidallanes.tex` Section 4 simplified

- Removed the old Section 4.1, 4.2, and 4.4 subsections from `Documents/L1_manuscripts/MODEL_replication_tidallanes.tex`.
- Condensed the former Section 4.3 into a short refinement section that states the current exact-hat scope, the value of richer Beijing OD and travel-time data, and the main path toward fundamentals recovery or Wardrop-consistent traffic assignment.
- Recompiled `Documents/L1_manuscripts/MODEL_replication_tidallanes.pdf` with `pdflatex`.

## 2026-04-30: `MODEL_replication_tidallanes.tex` Section 3 simplified

- Rewrote Section 3 of `Documents/L1_manuscripts/MODEL_replication_tidallanes.tex` around four points: what Spec C changes, how it is applied, what remains fixed, and what the directional / welfare / location results are.
- Removed the separate treated-link observed-time figure and population-response figure from the appendix.
- Initially retained only the cost-shock distribution figure (`fig1_tbar_shock`); this was superseded by the later figure-selection update below.
- Recompiled `Documents/L1_manuscripts/MODEL_replication_tidallanes.pdf` with `pdflatex`; the document is now 8 pages.

## 2026-04-30: `MODEL_replication_tidallanes.tex` spatial-response interpretation added

- Added `Replication_apply/python_commuting/analyze_specC_spatial_response.py`.
- The script computes Spec C resident and workplace responses by distance from the active square-grid system's geometric centre and exports:
  - `Replication_apply/results/tidal_lane/specC_spatial_response_summary.csv`
  - `Replication_apply/results/tidal_lane/specC_spatial_response_by_distance.csv`
  - `Replication_apply/results/tidal_lane/specC_node_population_changes.csv`
- Generated a treated grid-link map:
  - `Documents/L3_figs/fig4_treated_grid_links.{pdf,png}`
- Generated node-level people-change scatter plots:
  - `Documents/L3_figs/fig5_resident_change_by_distance.{pdf,png}`
  - `Documents/L3_figs/fig6_worker_change_by_distance.{pdf,png}`
- The distance scatter plots now include LOWESS nonparametric fitted curves, and the manuscript defines the model centre as the centroid of the union of all active square-grid polygons.
- Updated Section 3 of `Documents/L1_manuscripts/MODEL_replication_tidallanes.tex` to replace the old node-extrema population table with a distance-to-centre interpretation and table.
- Updated the appendix to include the treated asymmetry scatter, the treated grid-link map, and separate resident/worker distance scatter plots.
- Recompiled `Documents/L1_manuscripts/MODEL_replication_tidallanes.pdf` with `pdflatex`; the document is now 11 pages.

## 2026-04-29: future Baidu crawl variants should retain `tactics=0`, `tactics=2`, and `tactics=3`

- Retained decision for the next national commuting crawl code revision: support collecting `directionlite/v1/driving` outputs for the same OD pairs and real clock windows under all three route strategies.
- Interpretation to preserve in documentation and output metadata:
  - `tactics=0`: regular/default fast route strategy; useful as a common-strategy baseline.
  - `tactics=2`: congestion-avoidance strategy; closest to the intended object of a real-time congestion-adjusted recommended fastest route.
  - `tactics=3`: shortest-distance strategy; useful as a route-choice robustness check.
- The main paper specification can later choose between `tactics=0` and `tactics=2`; the other strategy should be retained as a robustness version, with `tactics=3` used to diagnose whether route selection rather than directional congestion drives the asymmetry.
- Also recorded in `data_work/docs/NATIONAL_TIDAL_COMMUTING_WORKFLOW.md` under the Baidu crawl section so this decision is visible when modifying crawl code.

## 2026-04-29: `MODEL_replication_tidallanes.tex` replication-apply audit

### What was checked

- Compared `Documents/L2_notes/MODEL_replication_tidallanes_PLAN.md` with `Documents/L1_manuscripts/MODEL_replication_tidallanes.tex`.
- Verified current `Replication_apply` scripts and outputs:
  - `Replication_apply/python_commuting/tidal_lane_specC.py`
  - `Replication_apply/prepare_from_data_work_square_v2/build_inputs_from_data_work_square_v2.py`
  - `Replication_apply/results/tidal_lane/selected_pairs_specC.csv`
  - `Replication_apply/results/tidal_lane/chi_lr_lf_specC.csv`
- Verified the Beijing square-grid source inputs under:
  - `data_work/outputs/manual_centerline_rules_v11/data/`

### Locked findings

- Current Spec C uses `Replication_apply/aa_input_square_v2/`, synced into `Replication_apply/counterfactuals/seattle/`.
- The current Beijing source version remains `manual_centerline_rules_v11`, square grid, AM period, `reachable_AM` OD sample.
- The current AA-style input sizes are:
  - nodes: `2729`
  - sparse adjacency / traffic rows: `4087`
  - OD rows: `377708`
- `aa_input_square_v2` differs materially from `aa_input_square_v1` in the `sparse_adjmat_seattle.csv` traffic input; v2 uses lane-priority plus BPR inversion rather than the first-pass capacity proxy.
- `MODEL_replication_tidallanes.tex` should not describe `sparse_adjmat_seattle.csv` as an iceberg-cost or `tau_ij` file. In the current Python port, the third column is read as `Xi_ij`, the edge-level baseline traffic input.
- Traceability note: the interim `99` treated-pair count in this audit was later corrected by the grid-ID remapping fix documented below.

### Audit note

- Detailed note saved at:
  - `Documents/L2_notes/MODEL_replication_tidallanes_audit_20260429.md`

## 2026-04-29: `Replication_apply` lane-source consistency fixed for Spec C

### Code updates

- `Replication_apply/prepare_from_data_work_square_v2/build_inputs_from_data_work_square_v2.py`
  - removed the fallback from raw-edge-projection path lane estimates to centerline `lanes_directional`;
  - current lane priority is:
    1. `lane_est_path_raw_edgeproj_len_weighted`
    2. `lane_est_path_raw_edgeproj_bottleneck`
    3. numeric fallback `2.0`
- `Replication_apply/python_commuting/tidal_lane_specC.py`
  - now merges the same raw-edge-projection path lane estimates before building the treated-pair BPR shock;
  - exports `lane_source_ij`, `lane_source_ji`, `lane_source_slow`, and `lane_source_fast` in `selected_pairs_specC.csv`;
  - no treated direction uses centerline `lanes_directional`.

### Rebuilt inputs and results

- Rebuilt `Replication_apply/aa_input_square_v2/` and synced it to `Replication_apply/counterfactuals/seattle/`.
- New baseline lane-source counts for the 4,087 exported directed edges:
  - raw-edge-projection length-weighted lane: `3304`
  - raw-edge-projection bottleneck lane: `0`
  - numeric fallback `2.0`: `783`
- Reran `tidal_lane_specC.py`.
- Traceability note: this intermediate run still showed `99` treated pairs before the endpoint remapping fix documented below.

### Updated Spec C result

- `t_bar_hat` slow range: `[0.8205, 0.8756]`
- `t_bar_hat` fast range: `[1.2009, 1.5058]`
- Average slow-direction travel-time change: `-12.8%`
- Average fast-direction travel-time change: `+21.7%`
- `chi_hat = 0.9994614034`
- `(chi_hat - 1) * 1e6 = -538.5966`
- Resident max gain/loss:
  - `sq_26_50`: `+50.6%`
  - `sq_14_10`: `-20.3%`
- Worker max gain/loss:
  - `sq_36_65`: `+43.5%`
  - `sq_53_46`: `-23.4%`

### Manuscript and figures

- Updated `Documents/L1_manuscripts/MODEL_replication_tidallanes.tex`.
- Regenerated:
  - `Documents/L3_figs/fig1_tbar_shock.{pdf,png}`
  - `Documents/L3_figs/fig2_population_changes.{pdf,png}`
  - `Documents/L3_figs/fig3_tau_obs.{pdf,png}`
- Recompiled `MODEL_replication_tidallanes.pdf` successfully.

## 2026-04-29: AA node mapping corrected to use grid IDs rather than stale edge `i/j`

### Issue

- `qsm_input_edges_square.parquet` carries `i/j` labels from the stage06 grid-edge export.
- `qsm_input_nodes_square.parquet` carries `node_i` labels from the stage07 OD-and-population node map.
- These are different numbering systems. For example:
  - edge row `sq_7_47 -> sq_8_47` had `i=3247`, `j=3295`;
  - the node table maps `sq_7_47 -> 1697`, `sq_8_47 -> 1725`.
- Therefore the previous `Replication_apply` builder was incorrectly relying on stale edge-level `i/j` labels instead of the stable `grid_o/grid_d` identifiers.

### Fix

- `Replication_apply/prepare_from_data_work_square_v2/build_inputs_from_data_work_square_v2.py`
  - now builds active nodes from grid IDs;
  - maps edge endpoints by `grid_o/grid_d -> node_i_aa_1based`;
  - keeps the raw-edge-projection lane hierarchy introduced above.
- `Replication_apply/python_commuting/tidal_lane_specC.py`
  - now maps treated-pair shocks by `grid_o/grid_d -> node_i_aa_1based`;
  - no longer uses raw `i/j` labels for AA matrix placement.

### Rebuilt results

- Rebuilt `aa_input_square_v2`:
  - nodes: `2736`
  - edges: `4632`
  - OD rows: `377708`
- Re-ran Spec C:
  - treated pairs: `100`
  - skipped pairs: `0`
  - asymmetry ratio range: `2.08--14.76`
  - `chi_hat = 0.9990852297`
  - `(chi_hat - 1) * 1e6 = -914.7703`
- Updated `MODEL_replication_tidallanes.tex`, regenerated the three Spec C figures, and recompiled the PDF.

## 2026-04-29: `edgeproj_v2` travel costs wired into `Replication_apply`

### Purpose

- Use the retained `raw_connected_v1_grid_travel_time_edgeproj_v2` path definition as the model edge-cost source.
- Keep observed time, free-flow time, lane proxy, and BPR traffic inversion on the same raw-edge-projection path.
- Stop mixing stage06 centerline-grid `tau_obs_min` / `tau_ff_min` with raw-edge-projection lane estimates in the AA input builder.

### Code updates

- `data_work/src/raw_connected_v1/grid_travel_time/build_raw_connected_v1_grid_travel_time_edgeproj.py`
  - now attaches raw-road free-flow speed from the 22:00--05:00 speed table;
  - computes path-level `raw_connected_tau_ff_min` along the same realized pair-specific path used for `raw_connected_travel_time_min`;
  - exports model-facing aliases `tau_obs_min` and `tau_ff_min`;
  - exports `lane_resolved_for_bpr` and `xi_bpr_edgeproj`;
  - records the observed-time, free-flow-time, and BPR definitions in `run_config.json`.
- `Replication_apply/prepare_from_data_work_square_v2/build_inputs_from_data_work_square_v2.py`
  - now reads square AM edge costs from `raw_connected_v1_grid_travel_time_edgeproj_v2`;
  - maps `home_grid/work_grid` to AA node IDs through the existing grid mapping;
  - computes `Xi_ij = lanes * max(tau_obs/tau_ff, 1)^(1/delta1)` using raw-edge-projection path costs.
- `Replication_apply/python_commuting/tidal_lane_specC.py`
  - now builds the treated-pair asymmetry table from the same raw-edge-projection square AM file;
  - uses raw-path `tau_obs_min`, raw-path `tau_ff_min`, and raw-path lane estimates for the Spec C BPR shock.

### Refreshed outputs

- Rebuilt in place for `square`, `hex`, and `voronoi`, both `AM` and `PM`:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/data/raw_connected_vs_old_adjacent_adjacent_{grid}_{period}.csv`
- Rebuilt AA inputs:
  - `Replication_apply/aa_input_square_v2/node_lr_lf_seattle.csv`
  - `Replication_apply/aa_input_square_v2/sparse_adjmat_seattle.csv`
  - `Replication_apply/aa_input_square_v2/sparse_commute_seattle.csv`
  - synced copies under `Replication_apply/counterfactuals/seattle/`
- Rebuilt metadata:
  - `Replication_apply/prepare_from_data_work_square_v2/build_summary_square.json`
  - `Replication_apply/prepare_from_data_work_square_v2/node_index_mapping_square.csv`
- Reran:
  - `Replication_apply/python_commuting/tidal_lane_specC.py`

### Retained diagnostics

- `raw_connected_vs_old_adjacent_adjacent_square_AM.csv`:
  - rows: `4,632`
  - finite raw-edge-projection observed/free-flow paths: `3,666`
  - mean `tau_obs_min`: `14.91`
  - mean `tau_ff_min`: `11.55`
  - finite `xi_bpr_edgeproj`: `3,666`
- Rebuilt AA input:
  - nodes: `2,723`
  - edge rows: `3,666`
  - OD rows: `377,708`
  - lane sources among exported edges:
    - raw-edge-projection length-weighted lane: `3,664`
    - raw-edge-projection bottleneck lane: `0`
    - numeric fallback `2.0`: `2`
- Spec C after the raw-edge-projection switch:
  - candidate pairs after the length filter: `1,763`
  - treated pairs: `100`
  - skipped pairs: `0`
  - asymmetry ratio range: `3.24--25.17`
  - `chi_hat = 0.9998669097`
  - `(chi_hat - 1) * 1e6 = -133.0903`

### Interpretation

- `manual_centerline_rules_v11` remains the source for grid IDs, node population, and reachable OD.
- Model edge costs now come from `raw_connected_v1_grid_travel_time_edgeproj_v2`, not from stage06 centerline-grid travel costs.
- The current free-flow cost is not `old_travel_time_min`; it is the raw-edge-projection path time recomputed with free-flow speeds on the same realized path.
- `old_travel_time_min` remains only a comparison field against the prior stage06 adjacent-grid benchmark.
- The manuscript and figures were not refreshed in this pass, so any paper-facing Spec C numbers should be updated before circulation.

## 2026-04-29: Spec C Figure 5/6 analogue plots generated

### Purpose

- Create current-result figures analogous to the original paper's link-level welfare maps and congestion comparison figures, using the already retained Spec C top-100 tidal-lane counterfactual.
- Scope note: this is not a strict per-link 1% marginal welfare elasticity run. It visualizes the current Spec C treated-link shock, equilibrium population response, and shock diagnostics.

### Code and inputs

- Added:
  - `Replication_apply/python_commuting/plot_tidal_lane_specC_results.py`
- Read:
  - `Replication_apply/results/tidal_lane/selected_pairs_specC.csv`
  - `Replication_apply/results/tidal_lane/chi_lr_lf_specC.csv`
  - `Replication_apply/prepare_from_data_work_square_v2/node_index_mapping_square.csv`
  - `data_work/outputs/manual_centerline_rules_v11/data/grid_square_master.parquet`

### Outputs

- Figures:
  - `Replication_apply/results/tidal_lane/figures/specC_treated_link_shock_map.{png,pdf}`
  - `Replication_apply/results/tidal_lane/figures/specC_population_response_map.{png,pdf}`
  - `Replication_apply/results/tidal_lane/figures/specC_shock_scatter.{png,pdf}`
  - `Replication_apply/results/tidal_lane/figures/specC_results_figure5_6_analogue.{png,pdf}`
- Summary:
  - `Replication_apply/results/tidal_lane/specC_plot_summary.csv`

### Retained numbers

- `chi_hat = 0.9998669097`
- Welfare gain: `133.0903` ppm
- Treated pairs: `100`
- Directed treated links: `200`
- Asymmetry ratio range: `3.24--25.17`; median `4.60`
- Mean slow-direction cost reduction: `10.59%`
- Mean fast-direction cost increase: `20.69%`

## 2026-04-29: `MODEL_replication_tidallanes.tex` narrowed to Spec C only

- Removed the section comparing multiple counterfactual specifications from `Documents/L1_manuscripts/MODEL_replication_tidallanes.tex`.
- Removed the explanatory text about earlier alternative specifications and the separate symmetric-versus-asymmetric counterfactual discussion.
- Updated the future-work roadmap so it no longer lists full-symmetrisation as a planned counterfactual.
- Recompiled `Documents/L1_manuscripts/MODEL_replication_tidallanes.pdf` with `pdflatex`.

## 2026-04-28: top-20 congestion-city note generated for AM, PM, and the later overnight baseline

- Added `src/national_tidal_commuting/generate_top20_congestion_note.py`.
- This note fixes the analysis sample to:
  - the top 20 cities ranked by weighted mean AM excess gap relative to the later overnight baseline
  - the top 50 commuting pairs within each selected city
- Retained time windows:
  - `AM`: session `20260427T070801`, `07:08:01--08:14:32`
  - `PM`: session `20260427T171554`, `17:15:54--18:26:28`
  - later overnight baseline: session `20260427T003434`, `00:34:35--01:48:51`
  - for that later overnight session, two failed directional rows are backfilled from the earlier complete night run `20260426T222257`
- New retained outputs:
  - metrics: `data_work/outputs/national_tidal_commuting_top50cities_2021_v1/metrics/top20_congestion_top50_note/`
  - figures: `data_work/outputs/national_tidal_commuting_top50cities_2021_v1/figures/top20_congestion_top50_note/`
  - TeX note: `Documents/L4_pipeline_outputs/national_tidal_commuting_notes/national_tidal_commuting_top20_congestion_note_20260428.tex`
  - compiled PDF: `Documents/L4_pipeline_outputs/national_tidal_commuting_notes/national_tidal_commuting_top20_congestion_note_20260428.pdf`

## 2026-04-28: `centerline_grid_v11` square baseline run completed for model/data alignment line

### Purpose

- Execute the pending full baseline run on the current retained `centerline_grid_v11` input line and record convergence / data-quality diagnostics before deciding whether to continue on centerline inputs or switch effort to `raw-grid`.

### Run command

```bash
python3 data_work/src/model/run_spatial_equilibrium.py \
  --version-id manual_centerline_rules_v11 \
  --grid-type square \
  --model-output-subdir model_square_baseline_centerline_v11
```

### Retained outputs

- `data_work/outputs/manual_centerline_rules_v11/model_square_baseline_centerline_v11/equilibrium_summary.csv`
- `data_work/outputs/manual_centerline_rules_v11/model_square_baseline_centerline_v11/calibration_summary.json`
- `data_work/outputs/manual_centerline_rules_v11/model_square_baseline_centerline_v11/edge_counterfactual_results.csv`

### Key diagnostics

- Baseline converged in `36` iterations (`solver_max_log_change = 9.31e-07`).
- `edge_tau_ff_imputed_share = 0.0`.
- `edge_missing_lane_share = 0.0`.
- Edge result export now contains explicit minutes and iceberg fields in parallel:
  - `tau_obs_min`, `tau_ff_min`
  - `t_obs_iceberg`, `t_ff_iceberg`
  - `t_baseline_eq_min`, `t_baseline_eq_iceberg`

### Calibration diagnostics retained

- `theta_diagnostic.theta_hat = 0.7363` (`n_obs = 3049`), while `theta_used = 6.83`.
- `lambda_diagnostic.lambda_hat = 0.01` (`n_edges = 4087`), while `lambda_used = 0.15`.
- These diagnostics remain calibration references only; the run still uses external defaults.

## 2026-04-28: path-based directional lane estimate added to `raw_edgeproj_v2` grid-cost outputs

### Purpose

- Add a second lane-estimation version that follows the realized pair-specific shortest path under `raw_connected_v1_grid_travel_time_edgeproj_v2`, and keep it separate from the existing centerline-derived grid-grid lane aggregation.

### Code update

- `data_work/src/raw_connected_v1/grid_travel_time/build_raw_connected_v1_grid_travel_time_edgeproj.py`
  - pair-specific route tracing now also exports:
    - `lane_est_path_raw_edgeproj_len_weighted`
    - `lane_est_path_raw_edgeproj_bottleneck`
  - values are computed along each directed shortest path (`home_grid -> work_grid`) using path segments on the raw road graph.
  - run metadata now records:
    - `path_lane_source`
    - `path_lane_fallback`

### Outputs refreshed in-place (no new version directory)

- `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/data/raw_connected_vs_old_adjacent_adjacent_{grid}_{period}.csv`
- refreshed for all three grid systems and both periods:
  - `square`, `hex`, `voronoi`
  - `AM`, `PM`

### Current retained caveat

- The current raw road graph does not carry direct lane fields and lacks a fully explicit key bridge to the stage lane table IDs.
- Under this constraint, path-lane values currently rely heavily on fallback lane proxy assignment; this produces valid path-based columns but weak cross-edge lane heterogeneity.

## 2026-04-28: AA-style path traffic proxy run saved as separate `raw_edgeproj_v2_traffic` output

### Purpose

- Implement a separate, non-overwriting version of the `raw_edgeproj_v2` grid-cost outputs that adds an AA-style path traffic proxy for model input construction.
- Keep the original `raw_connected_v1_grid_travel_time_edgeproj_v2` outputs untouched.

### Code and output separation

- New script:
  - `data_work/src/raw_connected_v1/grid_travel_time/build_raw_connected_v1_grid_travel_time_edgeproj_traffic.py`
- Separate output root:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2_traffic/`

### Added fields

- `path_total_road_length_m`
- `path_total_lanemiles_m_lane`
- `path_avg_aadt_proxy_len_weighted`

The last field is the AA-style path traffic proxy:

```text
path_avg_aadt_proxy_len_weighted = sum(aadt_proxy_edge * road_length_edge) / sum(road_length_edge)
```

where `aadt_proxy_edge` is obtained from a BPR-style inversion using observed travel time, free-flow travel time, lane proxy, and the default parameters recorded in `run_config.json`.

### Run completed

- Refreshed in the separate output root for:
  - `square`, `hex`, `voronoi`
  - `AM`, `PM`
- Retained files:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2_traffic/data/raw_connected_vs_old_adjacent_adjacent_square_AM.csv`
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2_traffic/data/raw_connected_vs_old_adjacent_adjacent_square_PM.csv`
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2_traffic/data/raw_connected_vs_old_adjacent_adjacent_hex_AM.csv`
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2_traffic/data/raw_connected_vs_old_adjacent_adjacent_hex_PM.csv`
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2_traffic/data/raw_connected_vs_old_adjacent_adjacent_voronoi_AM.csv`
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2_traffic/data/raw_connected_vs_old_adjacent_adjacent_voronoi_PM.csv`

### Quick validation

- Non-missing traffic proxy coverage:
  - square: `79.1%` in AM, `79.1%` in PM
  - hex: `83.6%` in AM, `83.6%` in PM
  - voronoi: `85.8%` in AM, `85.8%` in PM
- Mean `path_avg_aadt_proxy_len_weighted`:
  - square: `44,956` AM, `48,988` PM
  - hex: `45,478` AM, `49,982` PM
  - voronoi: `48,175` AM, `53,186` PM

## 2026-04-27: direction-agnostic AM asymmetry assets added for national top-pair evidence

- Added `data_work/src/national_tidal_commuting/generate_bidirectional_asymmetry_assets.py`.
- This asset builder intentionally does **not** rely on a pre-labeled `home/work` commuting direction when measuring within-pair asymmetry.
- For each pair and time window, it now constructs:
  - `slow_sec = max(t1, t2)`
  - `fast_sec = min(t1, t2)`
  - `asym_ratio = slow_sec / fast_sec`
  - `symmetry_ratio = fast_sec / slow_sec`
  - `abs_log_ratio = |log(asym_ratio)|`
- `NIGHT` remains the low-congestion comparison baseline; the new outputs therefore focus on:
  - same-time bidirectional asymmetry in `AM`
  - how much that asymmetry exceeds the nighttime baseline
- The new analysis keeps the three sampling scopes separate:
  - `top50`
  - `longdist30`
  - `union80`
- It also exports `longdist30_only` as a technical supplement so overlap between `top50` and the long-distance pool remains transparent.
- Current overlap structure in `national_tidal_commuting_top50cities_2021_v1`:
  - `top50 = 2500`
  - `longdist30 = 1500`
  - overlap `= 264`
  - `longdist30_only = 1236`
- Retained outputs:
  - `data_work/outputs/national_tidal_commuting_top50cities_2021_v1/metrics/am_bidirectional_asymmetry_national_summary.csv`
  - `data_work/outputs/national_tidal_commuting_top50cities_2021_v1/metrics/am_bidirectional_asymmetry_city_summary.csv`
  - `data_work/outputs/national_tidal_commuting_top50cities_2021_v1/metrics/am_bidirectional_asymmetry_pair_subset.csv`
  - `data_work/outputs/national_tidal_commuting_top50cities_2021_v1/figures/bidirectional_asymmetry_assets/`

## 2026-04-27: AM-to-NIGHT aligned asymmetry comparison added

- The bidirectional asymmetry asset builder now also computes a night-adjusted comparison that keeps the `AM` direction ordering fixed.
- For each pair:
  - if `AM` has `ab <= ba`, then compare `ab/ba` in `AM` against `ab/ba` in `NIGHT`
  - if `AM` has `ba < ab`, then compare `ba/ab` in `AM` against `ba/ab` in `NIGHT`
- Exported fields now include:
  - `night_ratio_aligned_to_am`
  - `am_over_night_aligned_ratio`
  - `night_over_am_aligned_ratio`
  - `aligned_ratio_gap`
  - indicators for whether `AM` is more asymmetric than `NIGHT`
- Interpretation:
  - `am_over_night_aligned_ratio < 1` means `AM` is more asymmetric than the same pair's nighttime directional baseline
  - `night_over_am_aligned_ratio > 1` is the inverse presentation of the same idea
- This allows the paper/slides version to emphasize excess morning asymmetry relative to nighttime rather than raw same-time asymmetry alone.

## 2026-04-27: `AM` analysis baseline switched from the late session to the complete 07:08-08:14 session

- The later `AM` crawl session `20260427T085411` was dropped after direct comparison showed materially weaker asymmetry than the earlier complete session.
- Retained `AM` session:
  - `20260427T070801`
  - time span `2026-04-27 07:08:01` to `08:14:32` (Asia/Shanghai)
  - `7472 / 7472` successful directional requests
- Deleted session:
  - `20260427T085411`
  - time span `2026-04-27 08:54:11` to `10:00:51`
  - `7470 / 7472` successful directional requests
- Comparison on the full `3736`-pair sample showed the earlier session is clearly stronger:
  - raw `AM min/max` median moved from about `0.927` to `0.905`
  - share with `AM min/max <= 0.90` rose from about `41.6%` to `51.6%`
  - median aligned `AM/NIGHT` ratio moved from about `0.954` to `0.919`
  - share with `AM` more asymmetric than `NIGHT` rose from about `69.8%` to `76.8%`
- After dropping the late session, `summarize_results.py`, `generate_am_asymmetry_assets.py`, `generate_bidirectional_asymmetry_assets.py`, and the ad hoc raw-histogram assets were all regenerated so current outputs now consistently use the earlier `AM` round.

## 2026-04-26: national tidal commuting AK source logic hardened for local smoke tests

- Unified Baidu AK loading into `src/national_tidal_commuting/common.py` so `run_full_crawl.py` and `query_baidu_routematrix.py` now share the same behavior.
- Added `--ak-source {auto,local,env}` to both entry points.
- New default is `auto`: prefer local `src/national_tidal_commuting/ak_local.py`, then fall back to environment variable `BAIDU_MAP_AUTH_TOKEN`.
- When both sources exist, the script now prints which source it chose and warns if the environment variable looks suspicious (for example an unrelated `sk-...` token).
- Updated `scripts/run_baidu_travel_panels.sh` so it no longer hard-fails when the environment variable is absent; it now passes through the same AK-selection logic as the Python entry points.
- Updated `docs/NATIONAL_TIDAL_COMMUTING_WORKFLOW.md` to document the new default and the explicit override `--ak-source env`.

## 2026-04-26: national tidal commuting request layer switched to Baidu `directionlite/v1/driving`

- Replaced the live-query layer from `routematrix/v2/driving` to single-OD `directionlite/v1/driving` so the current local AK can run successfully.
- `query_baidu_routematrix.py` and `run_full_crawl.py` now send one request per OD-direction row instead of batching multiple destinations into one matrix call.
- The output CSV / JSONL / progress file structure was intentionally kept stable so downstream inspection remains easy.
- Current panel mapping under `directionlite/v1/driving`:
  - `FREE -> tactics=0`
  - `AM/PM -> tactics=2`
  - `NIGHT -> tactics=0`
- Important retained caveat: under `directionlite/v1/driving`, `FREE` is only a lightweight baseline and is not the same estimand as the old `routematrix/v2` no-traffic baseline.
- Live smoke test completed successfully on `top2cities` with status `0` throughout:
  - `data_work/outputs/national_tidal_commuting_top2cities_2021_test/api/crawl_sessions/20260426T204833/`

## 2026-04-26: stricter live-time option added for AM / PM / NIGHT runs

- Added `--strict-live-window` to `run_full_crawl.py` and `query_baidu_routematrix.py`.
- In this mode, the workflow is intentionally stricter:
  - only one panel per run
  - `FREE` is disallowed
  - the local Shanghai clock must fall inside a narrower live window
- Current strict windows:
  - `AM`: `07:00–09:00`
  - `PM`: `17:00–19:30`
  - `NIGHT`: `22:00–次日01:00`
- This mode is meant for users who prefer to treat the third run as a real low-congestion nighttime observation rather than a same-time synthetic baseline.
- `summarize_results.py` was also updated to prefer `NIGHT` over legacy `FREE` when constructing the low-congestion comparison baseline.

## 2026-04-26: top-pair construction tightened and expanded for town-level commuting

- Town-level pair construction now drops same-township flows before unordered pair ranking.
- It also drops residual zero-distance pairs caused by different town codes sharing the same representative point.
- For each city, the retained list is now:
  - base `top_n_pairs = 50` by commuter volume
  - plus a supplemental long-distance set of up to `30` pairs ranked by commuter volume among pairs with `euclid_dist_km >= city_radius_est_km / 3`
  - final export is the union, so each city has `<= 80` pairs
- City radius is estimated from the convex-hull area of unique township representative points and converted by `r = sqrt(area / pi)`.
- Rebuilt `national_tidal_commuting_top50cities_2021_v1` outputs now contain `3736` pairs in total.
- After this rebuild:
  - exact same-point pairs in the exported sample fell from `1670` to `0`
  - median pair distance increased to about `9.14 km`
  - `49` of `50` cities now export more than `50` pairs, and `16` cities reach the full `80`

## 2026-04-26: AA commuting replication adaptation note added for speed-input version

- Added `Documents/L2_notes/AA_commuting_replication_adaptation_notes_20260426.md`.
- The note records how to adapt the AA Seattle commuting replication package to this project's data conditions while keeping the original traffic / cost / exact-hat structure as intact as possible.
- Locked high-level implementation principle:
  - input layer uses directed speed, free-flow speed, lane count, and a calibrated speed-flow mapping to recover edge traffic state;
  - model core continues to use the AA commuting inversion, baseline equilibrium, and exact-hat logic;
  - output layer maps counterfactual edge states back to time and speed.
- Updated `nextstep.md` so future model `tex` revisions and estimation-code preparation explicitly refer back to this note before changing the solver or data interface.

## 2026-04-26: national top-OD tidal-commuting trial toolkit created

### Scope

- Added a standalone toolkit for national motivation evidence based on high-flow commuting pairs rather than random within-city OD sampling.
- The toolkit is intentionally separate from the retained Beijing stage pipeline and is designed to scale to a 20-city trial once additional commuting files are available.

### New code and config

- New scripts under `data_work/src/national_tidal_commuting/`:
  - `build_top_pairs.py`
  - `query_baidu_routematrix.py`
  - `summarize_results.py`
- New sample config:
  - `data_work/config/national_tidal_commuting_planb_trial_v1.json`
- New implementation memo:
  - `Documents/L2_notes/national_tidal_commuting_planB_20260426.md`

### Locked implementation choices (updated)

- Main sample design follows plan B: top **undirected** pairs inside each city, not random spatial sampling.
- Pair selection: aggregate directed → collapse to unordered; rank by \((\text{OD}+\text{DO})/2 = (pop\_{ab}+pop\_{ba})/2\); take top `n` per city. **No** city-core filter, no distance band, no near-duplicate drop (those rules were removed from code).
- Canonical `home`/`work` in each row: the direction with the larger of the two directed volumes (`pop_ab >= pop_ba` after fold).

### API design

- The main live-query script uses Baidu `routematrix/v2/driving`.
- Under ordinary permissions, the implementation does **not** fake future departure times.
- Instead, the workflow is split into three separate live runs:
  - `AM`
  - `PM`
  - `NIGHT`
- Each run logs:
  - `query_window`
  - `query_date`
  - `query_time_local`
  - `request_batch_id`
  - raw JSON responses
  - flat per-pair result rows

### Current data coverage

- The repo currently contains directly reusable commuting inputs for:
  - Beijing: `data_work/raw_data/commute_202211.csv`
  - Shenzhen: `data_work/raw_data/commute_shenzhen_202411_for_stage07.csv`
- The sample config is therefore a two-city trial config.
- No 20-city raw commuting bundle is currently stored in this repo, so scaling beyond Beijing and Shenzhen only requires config expansion after new files are added.

## 2026-04-26: national 2021 town matrix + 2019 county map (50 cities × 50 undirected pairs)

- Primary inputs: `commuting_town_2021.dta` and `县（等积投影）.shp` (2019 审图号 bundle); `source: "town_2021"` in config.
- **50 cities** by intra-metro `commuters_2021` sum; for each, **up to 50** undirected town pairs ranked by **(OD+DO)/2**; no `compute_city_core` / no distance or dedup filters in code (avoids the former `invalid_city_core` empty-city case for small geographies like Zhongshan).
- Output target size **2500** rows when every city has at least 50 unique undirected pairs; otherwise `sample_shortfall_flag` on the row block for that city.
- Implementation memo: `Documents/L2_notes/national_tidal_commuting_planB_20260426.md`.
- `resolve_path` supports `data_work/...` from repo-relative paths; run `build_top_pairs` from the `data_work/` directory.

## 2026-04-26: `Replication_apply` workspace created for direct AA-format application to current square-grid data

### What was added

- New self-contained workspace:
  - `Replication_apply/`
- Preserved original Matlab snapshot:
  - `Replication_apply/original_matlab_code_snapshot/`
- Working application copies:
  - `Replication_apply/analysis/`
  - `Replication_apply/counterfactuals/seattle/`
  - `Replication_apply/python_commuting/`
- Dedicated canonical AA-format input bundle:
  - `Replication_apply/aa_input_square_v1/`
- Input-preparation layer:
  - `Replication_apply/prepare_from_data_work_square_v1/`

### Input bundle generated

- `Replication_apply/aa_input_square_v1/node_lr_lf_seattle.csv`
- `Replication_apply/aa_input_square_v1/sparse_adjmat_seattle.csv`
- `Replication_apply/aa_input_square_v1/sparse_commute_seattle.csv`

The builder also syncs the same three files into:

- `Replication_apply/counterfactuals/seattle/`

so the working Matlab and Python copies can keep AA-style relative paths.

### Current source mapping

- Nodes:
  - `data_work/outputs/manual_centerline_rules_v11/data/qsm_input_nodes_square.parquet`
- Edges:
  - `data_work/outputs/manual_centerline_rules_v11/data/qsm_input_edges_square.parquet`
- OD:
  - `data_work/outputs/manual_centerline_rules_v11/data/qsm_input_od_square.parquet`

### Working first-pass rules

- Node IDs are remapped from local 0-based indices to consecutive AA-style 1-based indices.
- The current square input keeps only nodes active in at least one of:
  - positive `residents` or `jobs`
  - positive `reachable_AM` `commuters_road`
  - AM edge incidence
- The current `traffic` column in `sparse_adjmat_seattle.csv` is a first-pass monotone proxy based on:
  - `tau_obs_min`
  - `tau_ff_min`
  - `capacity_proxy`
- Node longitude and latitude columns are still placeholder zeros in this quick-run version.

### Code adjustments kept minimal

- Original Matlab copies are preserved unchanged under `original_matlab_code_snapshot/`.
- Working Matlab copies under `Replication_apply/` were only adjusted to infer `N` from `node_lr_lf_seattle.csv` rather than hard-coding `217`.
- No other commuting solver logic was intentionally changed at this stage.

### Current runnable status

- Python commuting prediction now runs inside `Replication_apply/` and writes:
  - `Replication_apply/data/seattle/derived/predicted_lij_python.csv`
- Current run size:
  - `n_nodes = 2729`
  - `n_edges = 4087`
  - `n_od_rows = 377708`

### Immediate next use

- Once Matlab licensing is restored, use `Replication_apply/analysis/seattle/predict_commuting.m` as the first Matlab-side validation entrypoint.
- Compare that Matlab output against `Replication_apply/data/seattle/derived/predicted_lij_python.csv` before moving on to counterfactual application.

## 2026-04-24: grid-level raw-grid tidal asymmetry maps and summary tables generated

### Purpose

- Extend the latest `raw_edgeproj_v2` diagnostics from coverage-only maps to directional asymmetry diagnostics at the adjacent grid-pair level.
- Keep the asymmetry outputs strictly tied to the refreshed `manual_centerline_rules_v11`-aligned raw-grid run.

### Code update

- `data_work/src/diagnostics/plot_raw_grid_coverage_maps.py` was extended so the same post-processing pass now also:
  - reconstructs undirected adjacent grid pairs with both directions observed
  - computes `ab` and `ba` raw travel times
  - computes directional ratio `min(ab, ba) / max(ab, ba)`
  - computes asymmetry intensity `= 1 - ratio`
  - records faster / slower direction and absolute time gap

### Outputs written

- Figures:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/figures/grid_tidal_asymmetry/raw_grid_tidal_asymmetry_square_AM.png`
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/figures/grid_tidal_asymmetry/raw_grid_tidal_asymmetry_square_PM.png`
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/figures/grid_tidal_asymmetry/raw_grid_tidal_asymmetry_hex_AM.png`
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/figures/grid_tidal_asymmetry/raw_grid_tidal_asymmetry_hex_PM.png`
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/figures/grid_tidal_asymmetry/raw_grid_tidal_asymmetry_voronoi_AM.png`
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/figures/grid_tidal_asymmetry/raw_grid_tidal_asymmetry_voronoi_PM.png`
- Pair-level tables:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/data/grid_tidal_asymmetry/raw_grid_tidal_asymmetry_pairs_{grid}_{period}.csv`
- Summary tables:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/metrics/grid_tidal_asymmetry/raw_grid_tidal_asymmetry_summary_all.csv`
  - plus one grid-period summary per file under the same directory

### Summary metrics retained

- Square:
  - `AM` median directional ratio `= 0.793`
  - `PM` median directional ratio `= 0.798`
  - share with ratio `< 0.8`:
    - `AM = 47.3%`
    - `PM = 46.9%`
- Hex:
  - `AM` median directional ratio `= 0.795`
  - `PM` median directional ratio `= 0.798`
  - share with ratio `< 0.8`:
    - `AM = 47.0%`
    - `PM = 46.8%`
- Voronoi:
  - `AM` median directional ratio `= 0.793`
  - `PM` median directional ratio `= 0.810`
  - share with ratio `< 0.8`:
    - `AM = 47.4%`
    - `PM = 44.5%`

### Interpretation

- Under the latest retained raw-grid version, adjacent grid-pair asymmetry remains materially weaker than the centerline benchmark, but it is not negligible.
- The raw-grid directional ratio is centered around roughly `0.79-0.81` across all three grid systems.
- Voronoi again performs best on coverage, while its PM asymmetry distribution is slightly less extreme than AM.

## 2026-04-24: first-pass `model_v1` field alignment completed on `centerline_grid_v11`

### Code updates

- `data_work/src/stages/stage08_build_qsm_inputs.py`
  - now keeps `tau_obs_min` / `tau_ff_min` explicitly alongside legacy `t_min` / `t_ff_min`
  - writes empty placeholder columns for `t_obs_iceberg` / `t_ff_iceberg` so the downstream model layer can fill them deterministically
  - emits an explicit warning when `tau_ff_min` has to be imputed from observed minutes, instead of doing it silently
- `data_work/src/model/spatial_equilibrium.py`
  - `ModelInputs` now separates minute objects from iceberg objects:
    - `edge_tau_obs_min`
    - `edge_tau_ff_min`
    - `edge_t_obs_iceberg`
    - `edge_t_ff_iceberg`
  - loader now reads `tau_obs_min` / `tau_ff_min` first, carries `tau_ff_imputed_flag`, and constructs iceberg costs with explicit `delta0`
  - equilibrium summaries now report:
    - `solver_max_log_change`
    - `edge_tau_ff_imputed_share`
    - `edge_missing_lane_share`
  - congestion update no longer allows equilibrium travel time to fall below free-flow time
- `data_work/src/model/run_spatial_equilibrium.py`
  - adds `--iceberg-delta0`
  - output edge results now save both minute and iceberg costs plus imputation / lane-quality flags
  - calibration output now distinguishes:
    - `theta_diagnostic`
    - `lambda_diagnostic`
    - `theta_used`
    - `lambda_used`
- `data_work/src/model/run_counterfactual_suite.py`
  - updated to use the new minute/iceberg field split

### Verification

- Loader smoke test on `manual_centerline_rules_v11` square inputs completed successfully:
  - `n_nodes = 2736`
  - `n_edges = 4087`
  - `edge_tau_ff_imputed_share = 0.0`
- Smoke runs completed successfully:
  - `data_work/outputs/manual_centerline_rules_v11/model_square_baseline_smoke/`
  - `data_work/outputs/manual_centerline_rules_v11/model_square_baseline_smoke2/`
  - `data_work/outputs/manual_centerline_rules_v11/model_square_suite_smoke/`

### Retained interpretation

- The `centerline_grid_v11` line is now past the pure field-alignment stage: loader and runner can already read the new QSM minute fields and construct iceberg costs consistently.
- The next model task is no longer “edit the loader first”; it is to run and inspect a fuller baseline for convergence and data-quality behavior before deciding whether to keep refining the centerline-grid prototype or switch effort to a `raw_edgeproj_v2` input bundle.

## 2026-04-24: voronoi raw-grid comparison outputs refreshed to the current retained baseline

### Issue fixed

- The voronoi row used in the raw-grid comparison table of `Documents/L1_manuscripts/TidalLanes_0421.tex` was still tied to an older denominator (`1225` total grids) from the earlier comparison summary chain.
- The current retained baseline uses `1213` voronoi grids, so the comparison outputs needed to be realigned before treating the table as current.

### Code update

- `data_work/src/diagnostics/compare_grid_cost_versions.py`
  - now reads grid totals directly from the current baseline `grid_*_master.parquet` when available
  - no longer relies mechanically on stale `total_grids` values preserved inside older connector-summary metrics

### Refreshed outputs

- `data_work/outputs/comparison_grid_methods/three_version_adjacent_summary_AM.csv`
- `data_work/outputs/comparison_grid_methods/three_version_adjacent_summary_PM.csv`
- `data_work/outputs/comparison_grid_methods/three_version_connector_summary.csv`
- `data_work/outputs/comparison_grid_methods/three_version_full_comparison_AM.csv`
- `data_work/outputs/comparison_grid_methods/three_version_full_comparison_PM.csv`

### Retained voronoi values now in use

- Connector share:
  - `raw_node = 985 / 1213 = 0.812`
  - `raw_edgeproj_v2 = 1097 / 1213 = 0.904`
- Mean connector distance:
  - `raw_node = 676 m`
  - `raw_edgeproj_v2 = 414 m`
- AM adjacent travel time medians:
  - `centerline = 5.86 min`
  - `raw_node = 8.77 min`
  - `raw_edgeproj_v2 = 10.24 min`
- AM median directional ratio:
  - `centerline = 0.904`
  - `raw_node = 0.882`
  - `raw_edgeproj_v2 = 0.793`

### Manuscript sync

- `Documents/L1_manuscripts/TidalLanes_0421.tex` voronoi row in Table `rawgrid-method-comparison` was updated to the refreshed values.
- The accompanying sentence was tightened so it no longer describes the old voronoi comparison as if it matched the square/hex pattern exactly.

## 2026-04-23: `model_v1` implementation checklist added

### Purpose

- Converted the temporary `model_v1` review and data-gap diagnosis into an execution checklist.
- The checklist is intentionally process-only and can be deleted once the model/data alignment work is complete.

### New note

- `Documents/L2_notes/model_v1_implementation_checklist_temp_20260423.md`

### Key execution choices recorded

- Build model input bundles before interpreting estimation:
  - `centerline_grid_v11` for the current locked centerline version
  - `raw_edgeproj_v2` for the current raw-grid forward branch
- Separate observed minutes from AA model costs:
  - `tau_*_min` for physical travel time
  - `t_*_iceberg` for model transport costs
- Treat current `theta` and `lambda` regressions as diagnostic calibration until data and identification are strengthened.
- Add data-quality gates for free-flow time, lane/capacity coverage, OD graph support, and convergence before using welfare or counterfactual results.

## 2026-04-23: first `raw_edgeproj_v2` detour-tail decomposition completed

### Purpose

- Decompose the current edge-projection adjacent-pair long tail into connector time, endpoint host-edge time, and middle road-path time.
- Keep the diagnostic separate from model inputs because the raw-grid connector tail is not yet ready to serve as the welfare baseline.

### Script added

- `data_work/src/diagnostics/summarize_edgeproj_detour_tail.py`

### Outputs

- `data_work/outputs/comparison_grid_methods/edgeproj_v2_detour_tail_square_AM/`
- `data_work/outputs/comparison_grid_methods/edgeproj_v2_detour_tail_square_PM/`
- `data_work/outputs/comparison_grid_methods/edgeproj_v2_detour_tail_hex_AM/`
- `data_work/outputs/comparison_grid_methods/edgeproj_v2_detour_tail_voronoi_AM/`
- Combined summary:
  - `data_work/outputs/comparison_grid_methods/edgeproj_v2_detour_tail_summary_selected.csv`

### Main diagnostic result

- Across the four checked slices, the top-200 adjacent-pair tail is dominated by road-path time rather than connector time.
- Median connector share in the selected tail:
  - square AM: `2.85%`
  - square PM: `2.79%`
  - hex AM: `2.40%`
  - voronoi AM: `1.57%`
- Median tail road time:
  - square AM: `55.75 min`
  - square PM: `56.66 min`
  - hex AM: `62.58 min`
  - voronoi AM: `55.41 min`
- Share of selected tail pairs with road time above `75%` of total:
  - square AM: `100.0%`
  - square PM: `100.0%`
  - hex AM: `99.5%`
  - voronoi AM: `100.0%`

### Interpretation

- The first-order problem in the extreme `raw_edgeproj_v2` tail is not that centroid-to-road connectors are mechanically long.
- The more likely source is road-graph detour after access, caused by bottom-layer topology gaps, one-way directionality, or access edges that attach to locally plausible but globally poorly connected road segments.
- The next diagnostic should map the top-tail pairs and inspect whether they cluster around a small number of disconnected corridors or access-edge choices.

## 2026-04-23: stage02 matching score distance-dominance diagnostic completed

### Purpose

- Check whether the retained stage02 candidate-selection rule is still dominated by mean geometric distance after adding the angular component.
- Use the current `manual_centerline_rules_v11` outputs, excluding the `963` manual exact-match raw edges from the dominance test.

### Outputs

- `data_work/outputs/manual_centerline_rules_v11/metrics/stage02_distance_vs_angle_dominance_summary.csv`
- `data_work/outputs/manual_centerline_rules_v11/metrics/stage02_candidate_score_vs_distance_diagnostic.csv`
- `data_work/outputs/manual_centerline_rules_v11/metrics/stage02_candidate_score_components_long.csv`

### Main diagnostic result

- Among `22,713` non-manual baseline matches, the distance component exceeds the angular component in `95.84%` of final selected matches.
- The median distance-component share of the selected score is `94.78%`; the mean share is `88.40%`.
- Reconstructing the full candidate set gives `302,128` candidate rows and `136,680` candidates within the distance cap.
- The candidate chosen by the full score,
  - `score = d_mean + 0.1 * angle_diff`,
  is the same as the candidate chosen by distance alone in `99.45%` of non-manual baseline matches.
- Only `0.55%` of non-manual baseline matches select a higher-distance candidate because the angular gain offsets the distance penalty.

### Interpretation

- The retained stage02 matching rule is still distance-dominated.
- The angular term mainly acts as a tie-breaker or stabilizer when nearby centerline candidates have similar mean perpendicular distance.
- This supports the current Section 3.6 description: direction affects candidate selection, but final validity is governed by the `d_mean <= 180 m` distance cap.

## 2026-04-23: appendix map comparing undirected centerline and baseline filter added

### Purpose

- Add a paper-facing figure that lets the appendix reader see, at full-network scale, what the baseline filter removes from the undirected centerline system.
- Keep the figure-generation logic reusable rather than producing a one-off manual export.

### Code and output location rule

- Reusable figure-generation code for this class of appendix maps lives under:
  - `data_work/src/diagnostics/`
- Paper-facing final figures for one retained run live under:
  - `data_work/outputs/{version_id}/figures/paper_appendix/`
- This run follows that rule:
  - code added to `data_work/src/diagnostics/plot_match_diagnostics.py`
  - figure written to `data_work/outputs/manual_centerline_rules_v11/figures/paper_appendix/fig_undirected_vs_baseline_centerline_maps.png`

### Figure content

- Left panel: full undirected centerline network.
- Right panel: baseline-kept undirected centerline network (`keep_baseline = 1`).
- The figure is referenced in `Documents/L1_manuscripts/TidalLanes_0421.tex` Section 3.7.
- A local zoomed companion figure using the same layout was also exported to:
  - `data_work/outputs/manual_centerline_rules_v11/figures/paper_appendix/fig_undirected_vs_baseline_centerline_maps_tiananmen_15km.png`
- The zoomed version is clipped to the `15 km` Tiananmen-radius window already used elsewhere in the appendix diagnostics.

### Reusable convention

- If a new figure is primarily a paper or appendix asset, place the code in `data_work/src/diagnostics/` and write the final exported figure to `outputs/{version_id}/figures/paper_appendix/`.
- If a figure is mainly for GIS inspection or temporary QA, keep it in a diagnostic or review subdirectory instead of mixing it into `paper_appendix/`.

## 2026-04-23: lane-ratio histogram added for the lane-count appendix subsection

### Purpose

- Add a paper-facing diagnostic for the opposite-direction lane ratio so the appendix can directly assess whether the lane proxy is concentrated near one.

### Code and output location

- Reusable figure-generation code was added to:
  - `data_work/src/diagnostics/plot_lane_diagnostics.py`
- The retained paper-facing figure was written to:
  - `data_work/outputs/manual_centerline_rules_v11/figures/paper_appendix/fig_lane_ratio_hist_baseline_v11.png`

### Distribution summary

- Non-missing opposite-direction lane ratios: `7,394`
- Mean ratio: `1.036`
- Median ratio: `1.000`
- Share exactly equal to `1.0`: `83.6%`
- Share at or below `1.1`: `92.7%`
- Share at or below `1.5`: `97.9%`

### Interpretation

- The lane-ratio mass is tightly concentrated at one, with only a modest right tail.
- This supports the appendix interpretation that the lane proxy is usually balanced across directions on corridors where both-direction lane estimates are observed.
- The paper-facing histogram was later truncated at ratio `= 1.5` so the main mass near one is visually legible; this truncation still retains `97.9%` of observations.

## 2026-04-23: `TidalLanes_0421.tex` appendix verification and scope cleanup completed

### Purpose

- Continue paper-facing verification and revision of the dated appendix draft `Documents/L1_manuscripts/TidalLanes_0421.tex`.
- Check that Section 4 and Section 5 tables and figures point to the current retained `manual_centerline_rules_v11` outputs.
- Remove model-facing material from this appendix-facing draft so the file stays focused on the retained descriptive and construction diagnostics.

### Main edits

- Re-verified the Section 4 speed tables against:
  - `data_work/outputs/manual_centerline_rules_v11/metrics/stage03_speed_summary.csv`
  - `data_work/outputs/manual_centerline_rules_v11/data/centerline_speed_master.parquet`
- Re-verified the Section 5 asymmetry and tidal-candidate counts against:
  - `data_work/outputs/manual_centerline_rules_v11/metrics/stage03b_asymmetry_summary.csv`
  - `data_work/outputs/manual_centerline_rules_v11/data/centerline_asymmetry_summary.csv`
  - `data_work/outputs/manual_centerline_rules_v11/data/centerline_asymmetry_am_pm_comparison.csv`
  - `data_work/outputs/manual_centerline_rules_v11/data/centerline_tidal_lane_candidates.csv`
- Unified table-note layout so paper tables now use left-aligned notes rather than inheriting `\centering`.
- Removed the former Section 8 and Section 9 material from this draft:
  - `Model`
  - `Model Estimation`

### Compile status

- `Documents/L1_manuscripts/TidalLanes_0421.tex` compiles successfully after the cleanup.
- Remaining warnings are float-placement adjustments and one float-heavy page, not undefined references or control-sequence errors.

## 2026-04-23: Section 6 grid-system appendix updated to baseline-centered `v11` logic

### Purpose

- Bring the grid-construction appendix into line with the retained `manual_centerline_rules_v11` baseline centerline workflow.
- Remove the remaining mismatch in which the text still described Voronoi and grid-comparison objects from the older `raw_rebuild_validation` line.

### Code change

- `data_work/src/stages/stage05_build_grids.py` now filters `centerline_dir_master.parquet` to `keep_baseline = True` before constructing square, hex, and Voronoi grids.
- This makes the Stage 05 grid outputs explicitly baseline-centered and consistent with the rest of the retained appendix pipeline.

### New retained Stage 05 outputs

- `data_work/outputs/manual_centerline_rules_v11/data/grid_square_master.parquet`
- `data_work/outputs/manual_centerline_rules_v11/data/grid_hex_master.parquet`
- `data_work/outputs/manual_centerline_rules_v11/data/grid_voronoi_master.parquet`
- `data_work/outputs/manual_centerline_rules_v11/metrics/stage05_grid_comparison_stats.csv`

### Updated baseline-centered grid statistics

- Square:
  - `3,357` cells
  - `7,560 / 11,890 = 63.6%` baseline directed centerline segments fully within one cell
  - `4,214.52 / 19,391.30 km = 21.7%` of baseline directed centerline length fully within one cell
- Hex:
  - `4,604` cells
  - `6,924 / 11,890 = 58.2%`
  - `3,486.66 / 19,391.30 km = 18.0%`
- Voronoi:
  - `1,213` cells
  - `7,096 / 11,890 = 59.7%`
  - `3,058.18 / 19,391.30 km = 15.8%`

### Interpretation

- Square and hex geometry did not change; they remain exogenous tessellations over the study area.
- Their appendix statistics changed because the Stage 05 counting base now uses the retained baseline directed centerline backbone rather than the older unrestricted centerline line.
- Voronoi did change geometrically, because its seed construction is road-network-induced. After switching to the retained baseline centerline backbone, the Voronoi system now contains `1,213` cells rather than the older notebook-line count.

### Figure updates

- Added a new paper-facing comparison figure:
  - `data_work/outputs/manual_centerline_rules_v11/figures/paper_appendix/fig_grid_comparison_v11_baseline.png`
- This figure replaces the old archive-sourced grid-comparison image in `TidalLanes_0421.tex`.
- The Voronoi boundary color was later revised to blue in the retained paper-facing figure.

### Prose alignment in `TidalLanes_0421.tex`

- Section 6 introduction now distinguishes:
  - square and hex as exogenous tessellations
  - Voronoi as the only network-induced partition
- Section 6.1, 6.2, and 6.3 were rewritten to keep this distinction explicit and internally consistent.

## 2026-04-23: route distance and average speed added to pair-specific shortest-path outputs

### Purpose

- Extend the grid-to-grid pair-specific shortest-path computation to record, for each finite-cost adjacent pair, the total route distance (`route_length_m`) and the implied average travel speed (`route_avg_speed_kmh`) in addition to the existing travel time.
- These fields support downstream model inputs where physical distance and speed are needed separately from travel time.

### Code changes

- `data_work/src/raw_connected_v1/grid_travel_time/build_raw_connected_v1_grid_travel_time_edgeproj.py`:
  - `compute_grid_costs_adjacent_pairwise` now requests `return_predecessors=True` from `shortest_path`.
  - Builds a `(from_node, to_node) -> length_m` lookup covering road edges plus connector edges (home: `connector_dist_m + outbound_edge_length`; work: `inbound_edge_length + connector_dist_m`).
  - Traces the shortest-path predecessor chain from WORK to HOME and sums lengths.
  - Returns `route_length_m` and `route_avg_speed_kmh = (length_m / 1000) / (time_min / 60)` per pair.
- `data_work/src/raw_connected_v1/grid_travel_time/build_raw_connected_v1_grid_travel_time.py`:
  - Same predecessor-tracing logic added to `compute_raw_connected_grid_costs_adjacent_pairwise`.
  - Home connector length = `connector_dist_m`; work connector length = `connector_dist_m`.

### Outputs regenerated (AM and PM, all grid types)

- `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/data/raw_connected_vs_old_adjacent_adjacent_{grid}_{period}.csv`
- `data_work/outputs/raw_connected_v1_grid_travel_time_relaxed_v4/data/raw_connected_vs_old_adjacent_adjacent_{grid}_{period}.csv`

### Key descriptive results (square AM)

- `edgeproj_v2`: mean route length `7,717 m`, mean avg speed `35.4 km/h`, mean time `14.81 min`
- `relaxed_v4`: mean route length `6,738 m`, mean avg speed `35.0 km/h`, mean time `12.93 min`
- NaN count matches pairs with no finite travel time (connector unavailable); no speed outliers above 200 km/h or below 1 km/h.

## 2026-04-23: `manual_centerline_rules_v11` square QSM input handoff rebuilt for `model_v1`

### Purpose

- Start the `model/data` alignment line from the current locked centerline version rather than the older `raw_rebuild_validation` default.
- Close the first pipeline-to-model handoff gap by exporting square-grid QSM inputs with explicit time and lane-quality metadata.

### Code changes

- `data_work/src/stages/stage08_build_qsm_inputs.py` was extended so Stage 08 now:
  - exports `tau_obs_min` and `tau_ff_min` alongside the legacy `t_min` / `t_ff_min` names
  - records `tau_ff_missing_original`, `tau_ff_imputed_flag`, `tau_ff_source`, and `tau_consistency_flag`
  - aggregates lane support from `grid_links_square_long.csv` plus `centerline_lane_master.parquet`
  - writes edge-level `lane_quality_flag`, `lanes_directional`, and `capacity_proxy`
  - writes a grid-specific `data_quality_*_qsm.csv`
- Model entry defaults were updated so:
  - `data_work/src/model/run_spatial_equilibrium.py`
  - `data_work/src/model/run_counterfactual_suite.py`
  now default to `manual_centerline_rules_v11` instead of `raw_rebuild_validation`

### Pipeline run completed

- Re-ran:

```bash
python3.11 data_work/src/stages/run_full_pipeline.py \
  --config data_work/config/manual_centerline_plan1.json \
  --version-id manual_centerline_rules_v11 \
  --output-dir data_work/outputs \
  --from-stage stage06 --to-stage stage08 \
  --grid-type square
```

- This regenerated:
  - `grid_links_square_long.csv`
  - `grid_nodes_square.csv`
  - `OD_square_reachable_AM.csv`
  - `qsm_input_nodes_square.parquet`
  - `qsm_input_edges_square.parquet`
  - `qsm_input_od_square.parquet`
  - `qsm_input_parameters_square.json`
  - `data_quality_square_qsm.csv`

### Main retained diagnostics

- `qsm_input_edges_square.parquet` now contains the intended handoff fields:
  - `tau_obs_min`
  - `tau_ff_min`
  - `tau_ff_imputed_flag`
  - `tau_ff_source`
  - `lane_quality_flag`
  - `lanes_directional`
  - `capacity_proxy`
- `data_quality_square_qsm.csv` reports:
  - `n_nodes = 2,736`
  - `n_edges = 4,632`
  - `n_od_pairs = 377,708`
  - `od_total_commuters = 7,598,920`
  - `reachable_od_pair_share = 0.8530`
  - `reachable_od_commuters_share = 0.9543`
  - `edge_tau_ff_imputed_share = 0.0`
  - `edge_missing_lane_share_count = 0.0`
  - `edge_tau_obs_lt_tau_ff_share = 0.1431`

### Smoke test

- A one-iteration runner smoke test now completes successfully on the new default centerline input:
  - `data_work/outputs/manual_centerline_rules_v11/model_square_smoke_qsm_v11`

### Interpretation

- The project now has a usable square-grid `centerline_grid_v11` model handoff on the current locked version.
- The next bottleneck moves downstream from Stage 08 export to the model loader / solver:
  - separate minutes from iceberg objects inside `spatial_equilibrium.py`
  - stop relying on silently overloaded time-field names
  - then extend the same input-bundle logic to other grid systems and the `raw_edgeproj_v2` branch

## 2026-04-23: raw-grid coverage maps and population overlays generated for `raw_edgeproj_v2`

### Purpose

- Produce map-ready diagnostics for the current raw-grid forward branch using the grid system actually attached to that run.
- Quantify how much of the grid system and OD-implied population are covered by finite raw-grid adjacent travel-cost links.

### Latest alignment correction

- This diagnostic block was later refreshed so the retained `raw_edgeproj_v2` output root now points to:
  - `data_work/outputs/manual_centerline_rules_v11`
- The refresh was necessary because:
  - `square` and `hex` grid IDs are unchanged between the older baseline and `manual_centerline_rules_v11`
  - but `voronoi` changes materially under `manual_centerline_rules_v11` (`1,225 -> 1,213` cells), so old population and raw-grid coverage outputs were not version-consistent
- After the refresh:
  - `manual_centerline_rules_v11` now has complete `stage06-08` outputs for `square`, `hex`, and `voronoi`
  - `raw_connected_v1_grid_travel_time_edgeproj_v2` was rerun for both `AM` and `PM` against the `manual_centerline_rules_v11` baseline
  - the coverage figures and summaries were regenerated on top of the refreshed run config

### Code added

- New reusable diagnostic script:
  - `data_work/src/diagnostics/plot_raw_grid_coverage_maps.py`

### Outputs written

- Figures:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/figures/grid_coverage_maps/raw_grid_coverage_square_AM.png`
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/figures/grid_coverage_maps/raw_grid_coverage_square_PM.png`
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/figures/grid_coverage_maps/raw_grid_coverage_hex_AM.png`
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/figures/grid_coverage_maps/raw_grid_coverage_hex_PM.png`
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/figures/grid_coverage_maps/raw_grid_coverage_voronoi_AM.png`
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/figures/grid_coverage_maps/raw_grid_coverage_voronoi_PM.png`
- Summary tables:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/metrics/grid_coverage_maps/raw_grid_coverage_summary_all.csv`
  - one grid-period summary per file under the same directory
- Derived population / coverage tables:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/data/grid_coverage_maps/grid_population_summary_{grid}.csv`
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/data/grid_coverage_maps/grid_population_coverage_{grid}_{period}.csv`
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/data/grid_coverage_maps/raw_grid_connected_edges_{grid}_{period}.csv`
- Metadata note:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/metrics/grid_coverage_maps/README_raw_grid_coverage_maps.json`

### Figure design retained

- The maps intentionally follow the right-hand style of the earlier `aa_style` grid figure:
  - Beijing boundary
  - grid boundary
  - residential population circles at grid centroids
  - finite raw-grid adjacent links colored by travel time
- New layer added for this task:
  - the largest weakly connected component boundary is drawn as a red thick outline

### Coverage definition used

- A grid is counted as covered in a given period if it appears as an endpoint of at least one finite row in:
  - `raw_connected_vs_old_adjacent_adjacent_{grid}_{period}.csv`
- The red outline uses the largest weakly connected component of the undirected graph induced by those finite adjacent pairs.

### Main summary numbers after refresh to `manual_centerline_rules_v11`

- Square:
  - covered grids `= 1,427 / 3,357 = 42.5%`
  - largest-component grids `= 1,153 / 3,357 = 34.3%`
  - covered residents share `= 95.5%`
  - covered jobs share `= 97.2%`
  - OD population with both endpoints in the largest component `= 91.3%`
- Hex:
  - covered grids `= 1,893 / 4,604 = 41.1%` in `AM`
  - covered grids `= 1,893 / 4,604 = 41.1%` and largest-component grids `= 1,571 / 4,604 = 34.1%` in `PM`
  - covered residents share `= 95.6%`
  - covered jobs share `= 96.5%`
  - OD population with both endpoints in the largest component `≈ 90.6%`
- Voronoi:
  - covered grids `= 1,066 / 1,213 = 87.9%`
  - largest-component grids `= 996 / 1,213 = 82.1%`
  - covered residents share `= 97.5%`
  - covered jobs share `= 97.9%`
  - OD population with both endpoints in the largest component `= 94.2%`

### Interpretation

- The raw-grid branch still covers a minority of square / hex cells by count, but those connected cells contain most observed residents and jobs.
- After refreshing to `manual_centerline_rules_v11`, `voronoi` remains much stronger on both grid-count coverage and population coverage, and is now version-consistent with the latest retained grid construction.
- The largest-component red outline is meaningfully smaller than the full set of covered cells for square / hex, confirming that the current raw-grid issue is not only sparse coverage but also fragmentation into multiple connected pieces.

### Later figure-label extension

- The six `raw_grid_coverage_{grid}_{period}.png` figures were later refreshed so the title block now reports both:
  - `Covered grids` / `covered residents`
  - `Maximumconnected covered grids` / `Maximumconnected covered residents`
- The same exact metrics were also written into:
  - `metrics/grid_coverage_maps/raw_grid_coverage_summary_all.csv`
  using explicit alias columns:
  - `maximumconnected_covered_grids`
  - `maximumconnected_covered_grid_share`
  - `maximumconnected_covered_residents`
  - `maximumconnected_covered_resident_share`

## 2026-04-22: raw-grid method comparison reread sharpened the immediate next step

### What was checked

- Re-read the current comparison outputs under `data_work/outputs/comparison_grid_methods/`
- Rechecked connector summaries for:
  - `raw_connected_v1_grid_travel_time_edgeproj_v2`
  - `raw_connected_v1_grid_travel_time_relaxed_v4`

### Retained comparison takeaways

- `raw_edgeproj_v2` remains the preferred raw-grid direction if the project wants broader feasible coverage:
  - square coverage `47.4%` vs `36.8%` for `raw_node_v4`
  - hex coverage `49.4%` vs `38.4%`
  - voronoi coverage `87.5%` vs `80.4%`
- `raw_edgeproj_v2` also materially shortens connector access distance relative to `raw_node_v4`:
  - square mean connector distance `831 m` vs `1,094 m`
  - hex `850 m` vs `1,105 m`
  - voronoi `432 m` vs `676 m`
- But the extra coverage is not a free gain:
  - adjacent travel time is still much higher than centerline and is also higher than `raw_node_v4`
  - median directional ratio moves toward centerline relative to `raw_node_v4`, but remains far from the centerline benchmark
  - correlation with centerline travel times remains very low for both raw-grid methods

### Interpretation locked for planning

- The decision is no longer simply “choose `raw_edgeproj_v2` or `raw_node_v4`”.
- The immediate bottleneck is to explain the quality of the extra `raw_edgeproj_v2` connections:
  - which newly connected adjacent pairs are reasonable improvements
  - which ones are driven by long detours from topology gaps or poor access-point placement
- Therefore the project should not yet move straight into downstream `lane length / lane count` construction.
- The next operational step is a targeted audit of:
  - long-detour adjacent pairs under `raw_edgeproj_v2`
  - still-unconnected square / hex grids
  - whether the dominant problem is network topology, connector reach, or edge-projection access-point choice

### Rationale

- The asymmetry histogram carries the quantitative distribution more clearly than the two dense peak-specific maps.
- The tidal-candidate map remains useful because the final candidate set is sparse and directly inspectable.
- The lane-coverage map remains useful because it supports the main measurement claim that missing lane assignments are concentrated in a short peripheral tail rather than in the arterial backbone.

### Current interpretation for next-stage handoff

- The retained `v11` asymmetry signal is present but selective:
  - `405` AM `unsym1` centerlines
  - `361` PM `unsym1` centerlines
  - `638` unique centerlines flagged in at least one peak
  - `14` strict tidal candidates after requiring both strong asymmetry and reversal across peaks
- The current paper-facing evidence is sufficient for a targeted corridor review and a go/no-go decision on using the `v11` outputs in the downstream grid cost / model workflow.

## 2026-04-22: pair-specific shortest path implemented for both node-based and edgeproj; three-version comparison generated

### What changed

- **`compute_raw_connected_grid_costs` in node-based script was using bidirectional connector edges**:
  - `rows = [road_edges, grid→road, road→grid]` — all grid centroids were mutual waypoints
  - This allowed shortest paths to route through other grid centroids as intermediate nodes
- **`compute_grid_costs_pairwise` in edgeproj script built a combined graph with all access nodes**:
  - Other grids' access points (road split nodes) were present in every pair's routing graph

### New functions added

- `compute_raw_connected_grid_costs_adjacent_pairwise` in `build_raw_connected_v1_grid_travel_time.py`:
  - Builds pure road CSR graph once
  - For each adjacent pair: adds HOME virtual node (one-directional: home→road) and WORK virtual node (one-directional: road→work)
  - No other grid's connector edges enter the graph
- `compute_grid_costs_adjacent_pairwise` in `build_raw_connected_v1_grid_travel_time_edgeproj.py`:
  - For each access point on directed edge `(u→v)` at fraction `f`:
    - HOME → downstream road node `v` with cost `connector_time + edge_time*(1-f)`
    - Upstream road node `u_w` → WORK with cost `edge_time_w*f_w + connector_time_w`
  - Pure road graph only; no split-edge graph needed for pair-specific runs
  - Activated via `--pair-specific` flag

### Bug fix: period suffix hardcoded as 'AM' in node-based `run_for_grid`

- Fixed `suffix = f"..._{'AM'}"` → `suffix = f"..._{period}"`
- Added `period` as explicit parameter to `run_for_grid`

### New output directories

- `data_work/outputs/raw_connected_v1_grid_travel_time_relaxed_v4` — node-based with pair-specific routing
- `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2` — edge-projection with pair-specific routing

### Three-version comparison: key metrics (AM, adjacent pairs)

| version | grid | connected% | mean connector (m) | mean adj time (min) | median dir ratio | share asym<0.5 | corr vs centerline |
|---|---|---|---|---|---|---|---|
| centerline | square | — | — | 3.44 | 0.427 | 57.7% | — |
| raw_node | square | 36.8% | 1094 | 12.93 | 0.938 | 12.6% | 0.099 |
| raw_edgeproj | square | 47.4% | 831 | 14.81 | 0.793 | 14.9% | 0.088 |
| centerline | hex | — | — | 2.67 | 0.433 | 56.5% | — |
| raw_node | hex | 38.4% | 1105 | 11.50 | 0.937 | 12.0% | 0.073 |
| raw_edgeproj | hex | 49.4% | 850 | 13.49 | 0.796 | 15.0% | 0.093 |
| centerline | voronoi | — | — | 3.57 | 0.655 | 35.8% | — |
| raw_node | voronoi | 80.4% | 676 | 13.98 | 0.882 | 16.7% | 0.136 |
| raw_edgeproj | voronoi | 87.5% | 432 | 16.39 | 0.802 | 18.5% | 0.104 |

Full comparison outputs: `data_work/outputs/comparison_grid_methods/`

### Key interpretation

- Pair-specific routing for `raw_node` v4 gives essentially the same travel times as `relaxed_v3` (12.93 vs ~12.35 min for square adjacent) — the bidirectional connector issue in v3 was not causing material shortcuts
- `raw_edgeproj` v2 (pair-specific) also closely matches `edgeproj_v1` (14.81 vs 14.17 min) — access-node graph contamination was minor
- `raw_edgeproj` recovers ~10 percentage points of grid coverage vs `raw_node` (47% vs 37% for square), with ~20% shorter connectors
- Both raw-grid methods show much weaker directional asymmetry than centerline (median ratio ~0.94 vs ~0.43)
- Square/hex coverage remains a bottleneck (37–49%); voronoi coverage is stronger (80–88%)
- Correlation with centerline is still low (~0.07–0.14 for adjacent pairs), confirming the raw-grid results still reflect local connector geometry more than true road asymmetry

## 2026-04-22: pair-specific route-audit scripts aligned to latest methods; shared random 20-pair audit generated

### Audit-script correction

- `data_work/src/diagnostics/sample_raw_connected_adjacent_route_audit.py` was updated so the route audit now reconstructs the current pair-specific node-based graph instead of the older all-grids augmented graph.
- `data_work/src/diagnostics/sample_raw_connected_adjacent_route_audit_edgeproj.py` was updated so the route audit now reconstructs the current pair-specific edge-projection logic directly from the retained road graph and projected access geometry.
- Both audit scripts now accept a shared `home_grid` / `work_grid` pair list so the same random adjacent pairs can be compared across versions.

### Additional check on the pair-specific fix

- Aggregate means were close before and after the pair-specific correction, but the local tail is not negligible.
- For square AM adjacent pairs:
  - `raw_node`: `5.0%` of comparable pairs move by more than `1` minute and `3.6%` move by more than `5` minutes from `relaxed_v3` to `relaxed_v4`
  - `raw_edgeproj`: `30.8%` of comparable pairs move by more than `1` minute and `4.0%` move by more than `5` minutes from `edgeproj_v1` to `edgeproj_v2`
- Interpretation:
  - the earlier mixed-graph bug was not dominating the aggregate means
  - but it did matter for a nontrivial subset of local adjacent-pair routes, especially in the edge-projection branch

### Shared random square-grid audit (`20` pairs, AM)

- Shared sample file:
  - `data_work/outputs/comparison_grid_methods/shared_random_pairs_square_AM_20260422.csv`
- Node-based audit:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_relaxed_v4/route_audit_square_AM_shared20`
- Edge-projection audit:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/route_audit_square_AM_shared20`
- Merged comparison table:
  - `data_work/outputs/comparison_grid_methods/shared20_square_AM_route_audit_comparison.csv`

### Shared-random audit takeaways

- `raw_node_v4`:
  - zero-road-edge sampled cases `= 4 / 20`
  - mean connector time `= 3.57 min`
  - mean road-path time `= 5.04 min`
  - mean total adjacent time `= 8.57 min`
- `raw_edgeproj_v2`:
  - zero-road-edge sampled cases `= 0 / 20`
  - mean connector time `= 1.97 min`
  - mean road-path time `= 11.47 min`
  - mean total adjacent time `= 14.74 min`
- Interpretation:
  - edge projection materially reduces connector-only local routes and shortens centroid-to-road access
  - the cost is a longer road-path component, with a long-detour tail that now becomes the main diagnostic target

### Comparison figures generated

- Shared-20 total-time comparison:
  - `data_work/outputs/comparison_grid_methods/fig_shared20_total_time_compare_square_AM.png`
- Shared-20 paired examples for paper:
  - `data_work/outputs/comparison_grid_methods/fig_pair_examples_square_AM.png`
- All side-by-side shared-20 pair figures:
  - `data_work/outputs/comparison_grid_methods/shared20_square_AM_side_by_side/`

### Documentation refresh

- `Documents/L1_manuscripts/TidalLanes_0421.tex` was updated with:
  - the current raw-grid method split
  - the pair-specific-fix interpretation
  - the three-version aggregate comparison
  - the shared-random 20-pair local route audit
  - the current preference for `raw_edgeproj_v2` as the forward branch, with the long-detour tail left as the next diagnostic task

## 2026-04-22: lightweight project-management and agent-onboarding files tightened

### Goal

- Keep the project restart / handoff workflow light rather than introducing a heavy operating-log system.
- Make it cheaper for any newly attached agent to align on current status, current bottleneck, and next action.

### Changes made

- `nextstep.md` was lightly extended with:
  - a 3-line project summary
  - a short `当前开放问题` section
- `AGENTS.md` now points new sessions to a root-level quickstart file for low-token alignment.
- New file added:
  - `agent_quickstart.md`

### Retained workflow choice

- Do **not** add a heavy parallel project log.
- Keep the existing split:
  - `nextstep.md` for current status and next action
  - `data_work/docs/WORK_LOG.md` for completed work and retained decisions
- Use `agent_quickstart.md` as the minimal handoff / onboarding entrypoint for new agents.

## 2026-04-22: raw-grid comparison figure and table layout tightened in `TidalLanes_0421.tex`

### User-facing adjustments

- The local route-comparison figure in the raw-grid section was expanded beyond the earlier two-example layout.
- The method-comparison table in the same section was reformatted to avoid an overly wide layout.

### What changed

- Generated a new three-case comparison figure:
  - `data_work/outputs/comparison_grid_methods/fig_pair_examples_square_AM_3cases.png`
- Replaced the earlier two-case writeup with a three-case discussion covering:
  - a connector-only node-based case
  - a case where edge projection is faster
  - a remaining long-detour edge-projection case
- Reformatted `Table 11` in `Documents/L1_manuscripts/TidalLanes_0421.tex` into a compact three-row summary by grid type with abbreviated column headers.

### Compile status

- `Documents/L1_manuscripts/TidalLanes_0421.tex` recompiles successfully after the figure/table adjustment.
- The earlier raw-grid-specific float-too-large / table-width issues were removed; remaining warnings are legacy layout warnings elsewhere in the document.

## 2026-04-21: `TidalLanes_0320.tex` refreshed to the retained `v11` pipeline

### Section 3 updates

- Replaced the old four-network comparison with a five-network comparison:
  - raw
  - raw\_split
  - undirected centerline (all)
  - directed centerline (all)
  - directed centerline (baseline)
- Added the baseline-rule explanation directly in the appendix text and notes:
  - valid `LineString`
  - centerline length at least `50 m`
- Updated the matching discussion to use the new four-status directional coverage figures for baseline centerlines and raw edges.
- Added a lane-count subsection after the match assessment, including baseline lane coverage and the lane-coverage map.

### Section 4 updates

- Replaced the older speed-diagnostics references with current `manual_centerline_rules_v11` outputs.
- Added a stage03 summary table using the retained run:
  - matched raw speed observations
  - directed centerline-hour rows
  - unique directed centerlines
  - overall, AM, PM, and late-night means
- Added current directed-centerline speed distribution and diurnal figures generated from the retained run.

### Section 5 updates

- Updated asymmetry counts, overlap counts, and tidal-candidate counts to the retained `v11` outputs.
- Repointed the asymmetry histogram, tidal-candidate map, and AM/PM asymmetry maps to the current output directory.

### Compile status

- `Documents/TidalLanes_0320.tex` compiles successfully after the refresh.
- Remaining warnings are overfull boxes and float-placement adjustments outside the newly updated matching and speed sections; there are no undefined references after the rerun.

## 2026-04-21: final match review and dated TeX draft refresh

### Final match review assets

- Rebuilt `figures/final_match_review` so the centerline coverage map now uses four directional-status classes:
  - both `AB` and `BA` matched
  - `AB` only
  - `BA` only
  - neither matched
- Added both baseline and all-centerline versions, plus a raw-edge version mapped to the same corridor-level directional coverage logic.
- Exported accompanying GIS layers and a length-share table for the four status classes.

### New figure assets

- `data_work/outputs/manual_centerline_rules_v11/figures/final_match_review/map_final_match_centerline_coverage.png`
- `data_work/outputs/manual_centerline_rules_v11/figures/final_match_review/map_final_match_centerline_coverage_all.png`
- `data_work/outputs/manual_centerline_rules_v11/figures/final_match_review/map_final_match_raw_edges_4status.png`
- `data_work/outputs/manual_centerline_rules_v11/figures/paper_appendix/fig_length_cdf_5networks_v11.png`
- `data_work/outputs/manual_centerline_rules_v11/figures/paper_appendix/fig_centerline_speed_distribution_v11.png`
- `data_work/outputs/manual_centerline_rules_v11/figures/paper_appendix/fig_centerline_speed_diurnal_v11.png`

### Dated draft snapshot

- The updated appendix draft was preserved as a dated copy:
  - `Documents/TidalLanes_0421.tex`
- The front matter remains unchanged relative to the refreshed `Documents/TidalLanes_0320.tex`; the dated copy exists to preserve the 2026-04-21 snapshot of the appendix rewrite.

## 2026-04-21: appendix figure retention tightened for `TidalLanes_0421.tex`

### Appendix retention decision

- Retained in the current appendix-facing draft:
  - final match coverage map
  - lane coverage map
  - speed distribution figure
  - diurnal speed profile
  - asymmetry histogram
  - tidal candidate map
- Not retained in the current appendix layout:
  - separate AM asymmetry map

## 2026-04-21: raw-grid travel-cost conservative rerun started

### Current task framing

- `stage06` centerline-grid travel cost is retained as a comparison benchmark, not the preferred forward path for raw-grid cost construction.
- The current work focus is the raw_connected_v1 raw-grid route:
  - keep shortest-path routing
  - avoid relying on extra edge deletion that could damage connectivity
  - prioritize conservative grid-to-road attachment and explicit diagnostics

### Specific implementation goal

- Build a more conservative raw_connected_v1 raw-grid shortest-path version under the current topology state.
- Compare that raw-grid version against the retained centerline-grid version on key descriptive travel-cost and directional-pattern statistics.
- Keep in mind that one downstream network version currently mixes a centerline-based network object with raw speed inputs; this is acceptable for now but should be tracked as a possible source of smoothed local asymmetry or topology-speed mismatch.

## 2026-04-21: conservative raw_connected_v1 raw-grid travel-cost `v1` generated and benchmarked

### Files added / refreshed

- Conservative raw-grid output root:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_conservative_v1`
- Main raw-grid build script updated:
  - `data_work/src/raw_connected_v1/grid_travel_time/build_raw_connected_v1_grid_travel_time.py`
- New comparison diagnostic:
  - `data_work/src/diagnostics/compare_grid_cost_versions.py`

### Conservative raw-grid design used in this run

- Kept directed shortest-path routing on the raw_connected_v1 road graph.
- Did not rely on dropping tiny positive-time edges to clean the graph.
- Tightened grid-to-road attachment instead:
  - `connector_count = 1`
  - `connector_max_dist_m = 1500`
  - `connector_min_node_degree = 2`
  - no low-degree fallback
- Reused cached road-segment speed tables and compared against `data_work/outputs/raw_rebuild_validation`.

### Connector coverage diagnostics

- Square:
  - total grids `= 3,357`
  - grids with connectors `= 1,158`
  - grids without connectors `= 2,199`
  - mean connector distance `= 706.5 m`
  - p90 connector distance `= 1,290.4 m`
- Hex:
  - total grids `= 4,604`
  - grids with connectors `= 1,614`
  - grids without connectors `= 2,990`
  - mean connector distance `= 706.6 m`
  - p90 connector distance `= 1,289.2 m`
- Voronoi:
  - total grids `= 1,225`
  - grids with connectors `= 985`
  - grids without connectors `= 240`
  - mean connector distance `= 444.4 m`
  - p90 connector distance `= 982.6 m`

### OD-level comparison against centerline-grid baseline

- Diagnostic file:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_conservative_v1/diagnostics_compare/od_compare_summary_AM.csv`
- Weighted mean travel time differences:
  - square: `18.44` min (`stage06`) vs `60.45` min (`raw_connected_v1_raw_grid`)
  - hex: `12.02` min (`stage06`) vs `59.78` min (`raw_connected_v1_raw_grid`)
  - voronoi: `16.33` min (`stage06`) vs `59.60` min (`raw_connected_v1_raw_grid`)
- Interpretation:
  - this conservative raw-grid version is far slower than the retained centerline-grid benchmark
  - the result is too conservative to replace the current benchmark directly
  - the main bottleneck is now more likely connector design and incomplete bottom-layer connectivity than shortest-path logic itself

### Adjacent-pair comparison against centerline-grid baseline

- Diagnostic file:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_conservative_v1/diagnostics_compare/adjacent_compare_summary_AM.csv`
- Mean adjacent travel time:
  - square: `3.76` min (`stage06`) vs `17.51` min (`raw_connected_v1_raw_grid`)
  - hex: `2.74` min (`stage06`) vs `16.34` min (`raw_connected_v1_raw_grid`)
  - voronoi: `3.37` min (`stage06`) vs `17.81` min (`raw_connected_v1_raw_grid`)
- Adjacent-pair correlation remains low:
  - square `= 0.097`
  - hex `= 0.080`
  - voronoi `= 0.114`
- Interpretation:
  - beyond a level shift, the local ranking pattern is also unstable
  - this points to grid-entry / connector design and topology support as first-order issues

### Directional asymmetry comparison

- Diagnostic file:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_conservative_v1/diagnostics_compare/adjacent_directional_asymmetry_summary_AM.csv`
- Stage06 adjacent-pair directional time ratios are more asymmetric than the current conservative raw-grid version:
  - square median ratio:
    - `stage06 = 0.427`
    - `raw_connected_v1_raw_grid = 0.907`
  - hex median ratio:
    - `stage06 = 0.433`
    - `raw_connected_v1_raw_grid = 0.907`
  - voronoi median ratio:
    - `stage06 = 0.655`
    - `raw_connected_v1_raw_grid = 0.855`
- Share of strongly asymmetric adjacent pairs (`ratio < 0.5`):
  - square:
    - `stage06 = 57.7%`
    - `raw_connected_v1_raw_grid = 14.5%`
  - hex:
    - `stage06 = 56.5%`
    - `raw_connected_v1_raw_grid = 14.1%`
  - voronoi:
    - `stage06 = 35.8%`
    - `raw_connected_v1_raw_grid = 19.6%`

### Interpretation of the smoothing concern

- The current diagnostics do not support the simple claim that the centerline-grid version is the one mechanically smoothing local directional detail relative to the current raw-grid conservative version.
- In the present comparison, the conservative raw-grid version is actually more directionally symmetric at the adjacent-grid level.
- This should not be over-interpreted as evidence that raw-grid intrinsically smooths asymmetry less or more.
- A more plausible reading is:
  - the current raw-grid conservative version is dominated by sparse and restrictive grid connectors plus incomplete bottom-layer topology
  - those constraints suppress local directional variation before it can appear in grid-to-grid costs
- The hybrid-network concern should therefore remain active, but it cannot yet be cleanly diagnosed without a less distorted raw-grid connector design.

## 2026-04-21: raw_connected_v1 raw-grid relaxed rerun `v2` completed

### Goal of this rerun

- The conservative rerun was too restrictive for the user’s stated objective.
- The target here was to move back toward the earlier “normal working” version:
  - keep basic shortest-path safeguards
  - avoid obvious pathologies such as self-loops and zero-time links
  - restore more practical connector reach so that basic grid connectivity is not sacrificed

### Parameters restored toward the earlier working version

- Script updated:
  - `data_work/src/raw_connected_v1/grid_travel_time/build_raw_connected_v1_grid_travel_time.py`
- New default settings used for `relaxed_v2`:
  - `connector_count = 3`
  - `connector_max_dist_m = 2500`
  - `centroid_search_radii_m = 300,600,900,1500,2500`
  - `connector_min_node_degree = 1`
  - `allow_low_degree_fallback = 0`
  - `min_edge_time_min = 0.02`
- Interpretation of the parameter choice:
  - `connector_count = 3` restores multiple candidate attachments per grid, which helps basic reachability and reduces overdependence on one local node
  - `connector_max_dist_m = 2500` returns to the old search envelope instead of the conservative `1500 m` cap, which had clearly cut off too many square and hex grids
  - `connector_min_node_degree = 1` removes the extra conservative penalty against low-degree nodes; dead-end entry is still possible, but only on a positive-time directed graph
  - `min_edge_time_min = 0.02` keeps a light floor on extremely tiny edges without aggressively deleting positive travel-time links
  - Basic safeguards still remain:
    - no self-loop directed edges
    - no nonpositive travel-time edges
    - duplicate `from_node -> to_node` links still collapse to the fastest remaining edge

### Output root

- Relaxed raw-grid output:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_relaxed_v2`
- Network diagnostics added:
  - `data_work/src/diagnostics/plot_raw_connected_grid_network_diagnostics.py`
- Diagnostic outputs:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_relaxed_v2/diagnostics_compare/`
  - `data_work/outputs/raw_connected_v1_grid_travel_time_relaxed_v2/diagnostics_network/`

### Network coverage: source raw_connected_v1 graph vs graph actually used in travel-cost computation

- The relaxed rerun does **not** materially trim the raw_connected_v1 road graph itself.
- Diagnostic summaries:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_relaxed_v2/diagnostics_network/network_coverage_summary_square_AM.csv`
  - `data_work/outputs/raw_connected_v1_grid_travel_time_relaxed_v2/diagnostics_network/network_coverage_summary_hex_AM.csv`
  - `data_work/outputs/raw_connected_v1_grid_travel_time_relaxed_v2/diagnostics_network/network_coverage_summary_voronoi_AM.csv`
- Common road-network retention statistics across grid systems:
  - source directed edges `= 24,757`
  - used directed edges `= 23,983`
  - edge-count retention `= 96.87%`
  - source total length `= 18,507.3 km`
  - used total length `= 18,414.3 km`
  - length retention `= 99.50%`
  - source unique nodes in edges `= 14,746`
  - used unique nodes in edges `= 14,744`
- Interpretation:
  - the graph used for shortest path is visually and quantitatively very close to the source raw_connected_v1 directed graph
  - the main distortion is **not** that the main road graph was heavily deleted
  - the remaining issue is mostly how grids are attached into that graph

### Grid connector coverage under `relaxed_v2`

- Square:
  - total grids `= 3,357`
  - connected grids `= 1,238`
  - connected share `= 36.9%`
  - mean connector distance `= 1,093.9 m`
  - p90 connector distance `= 2,055.6 m`
- Hex:
  - total grids `= 4,604`
  - connected grids `= 1,768`
  - connected share `= 38.4%`
  - mean connector distance `= 1,103.5 m`
  - p90 connector distance `= 2,077.7 m`
- Voronoi:
  - total grids `= 1,225`
  - connected grids `= 986`
  - connected share `= 80.5%`
  - mean connector distance `= 675.2 m`
  - p90 connector distance `= 1,499.6 m`

### Comparison with the conservative rerun

- Relative to `conservative_v1`, the relaxed rerun restores some connector reach:
  - square connected share:
    - `34.5% -> 36.9%`
  - hex connected share:
    - `35.1% -> 38.4%`
  - voronoi connected share:
    - `80.4% -> 80.5%`
- Connector distances rise back toward the earlier working version:
  - this is expected, because restored connectivity is partly coming from allowing farther centroid-to-road attachment again
- Interpretation:
  - the relaxed version is directionally better than the conservative one for basic coverage
  - but square / hex coverage is still materially below what would be comfortable for a final production version

### OD-level comparison against the stage06 benchmark

- Diagnostic file:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_relaxed_v2/diagnostics_compare/od_compare_summary_AM.csv`
- Weighted mean travel time:
  - square: `18.63` min (`stage06`) vs `49.12` min (`relaxed_v2`)
  - hex: `12.40` min (`stage06`) vs `47.71` min (`relaxed_v2`)
  - voronoi: `16.65` min (`stage06`) vs `48.63` min (`relaxed_v2`)
- Median ratio (`raw_connected_v1 / stage06`):
  - square `= 2.240`
  - hex `= 2.966`
  - voronoi `= 2.537`
- OD correlation improves relative to the conservative version:
  - square `= 0.876`
  - hex `= 0.802`
  - voronoi `= 0.894`
- Interpretation:
  - the relaxed version is still systematically slower than `stage06`
  - but it is noticeably closer to a usable benchmark than the conservative version
  - global ranking alignment is now fairly strong again at the OD level

### Adjacent-pair comparison

- Diagnostic file:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_relaxed_v2/diagnostics_compare/adjacent_compare_summary_AM.csv`
- Mean adjacent travel time:
  - square: `3.60` min (`stage06`) vs `11.97` min (`relaxed_v2`)
  - hex: `2.70` min (`stage06`) vs `10.44` min (`relaxed_v2`)
  - voronoi: `3.39` min (`stage06`) vs `12.99` min (`relaxed_v2`)
- Median adjacent ratio (`raw_connected_v1 / stage06`):
  - square `= 3.062`
  - hex `= 3.628`
  - voronoi `= 3.169`
- Adjacent-pair correlation remains low:
  - square `= 0.097`
  - hex `= 0.110`
  - voronoi `= 0.161`
- Interpretation:
  - the local edge-level pattern is still not stable enough
  - this again points back to connector design and bottom-layer topology rather than to the core shortest-path routine

### Directional asymmetry pattern

- Diagnostic file:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_relaxed_v2/diagnostics_compare/adjacent_directional_asymmetry_summary_AM.csv`
- Adjacent undirected-pair median time ratio:
  - square:
    - `stage06 = 0.427`
    - `relaxed_v2 = 0.938`
  - hex:
    - `stage06 = 0.433`
    - `relaxed_v2 = 0.936`
  - voronoi:
    - `stage06 = 0.655`
    - `relaxed_v2 = 0.883`
- Share of strongly asymmetric adjacent pairs (`ratio < 0.5`):
  - square:
    - `stage06 = 57.7%`
    - `relaxed_v2 = 11.7%`
  - hex:
    - `stage06 = 56.5%`
    - `relaxed_v2 = 10.9%`
  - voronoi:
    - `stage06 = 35.8%`
    - `relaxed_v2 = 15.1%`
- Interpretation:
  - the relaxed raw-grid version is still much more symmetric than `stage06` at the adjacent-pair level
  - this does **not** yet imply that the centerline-based graph is the only source of smoothing
  - a more plausible reading is still that current raw-grid local asymmetry is being damped by connector geometry and incomplete underlying topology

### Figures produced for visual inspection

- Shared source-vs-used graph map:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_relaxed_v2/diagnostics_network/map_source_vs_used_AM.png`
- Used graph plus connector maps:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_relaxed_v2/diagnostics_network/map_used_with_connectors_square_AM.png`
  - `data_work/outputs/raw_connected_v1_grid_travel_time_relaxed_v2/diagnostics_network/map_used_with_connectors_hex_AM.png`
  - `data_work/outputs/raw_connected_v1_grid_travel_time_relaxed_v2/diagnostics_network/map_used_with_connectors_voronoi_AM.png`
- Intended use:
  - confirm that the road graph entering shortest-path computation is visually close to the source raw_connected_v1 graph
  - inspect whether the practical loss is coming from sparse grid attachment rather than graph deletion

### Current take-away

- For the user’s current objective, `relaxed_v2` is a better working version than the conservative rerun.
- It restores the old parameter logic closely enough to serve as the current reference raw-grid build.
- However, it is still not ready to replace the retained centerline-grid benchmark as the sole production travel-cost input.
- The next bottleneck remains:
  - better centroid-to-road / grid-to-road attachment design
  - continued manual cleanup of the bottom-layer raw_connected_v1 topology

## 2026-04-21: square-grid adjacent route audit figures generated for `relaxed_v2`

### Purpose

- Produce a first manual QA set for the latest raw-grid connector and routing strategy.
- The user requested random adjacent-grid local maps that show:
  - the surrounding road network
  - the currently selected shortest path under the active raw-grid strategy

### Files added

- New diagnostic script:
  - `data_work/src/diagnostics/sample_raw_connected_adjacent_route_audit.py`
- Output directory:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_relaxed_v2/route_audit_square_AM`

### Current implementation choice

- This first audit pass uses:
  - `grid_type = square`
  - `period = AM`
  - `n_samples = 20`
  - `seed = 20260421`
- Sampling frame:
  - adjacent square-grid pairs with finite `raw_connected_travel_time_min`
- Each figure includes:
  - the two sampled grids
  - all used raw_connected_v1 road edges in a local buffered window
  - the chosen shortest-path road segments
  - the specific connector segments used at the origin and destination side

### Output inventory

- `20` route-audit figures:
  - `01_sq_11_20_to_sq_10_20.png`
  - `02_sq_11_51_to_sq_12_51.png`
  - `03_sq_14_28_to_sq_15_28.png`
  - `04_sq_16_51_to_sq_16_50.png`
  - `05_sq_21_31_to_sq_21_30.png`
  - `06_sq_23_43_to_sq_24_43.png`
  - `07_sq_24_34_to_sq_23_34.png`
  - `08_sq_25_40_to_sq_25_41.png`
  - `09_sq_26_31_to_sq_26_30.png`
  - `10_sq_28_41_to_sq_28_42.png`
  - `11_sq_28_45_to_sq_29_45.png`
  - `12_sq_29_35_to_sq_29_34.png`
  - `13_sq_30_27_to_sq_29_27.png`
  - `14_sq_31_29_to_sq_32_29.png`
  - `15_sq_35_29_to_sq_34_29.png`
  - `16_sq_35_55_to_sq_34_55.png`
  - `17_sq_38_48_to_sq_37_48.png`
  - `18_sq_51_27_to_sq_51_28.png`
  - `19_sq_52_18_to_sq_52_19.png`
  - `20_sq_7_31_to_sq_6_31.png`
- Summary tables:
  - `sampled_adjacent_pairs.csv`
  - `sampled_adjacent_route_summary.csv`
  - `run_note.txt`

### Quick descriptive takeaways from the 20 sampled square pairs

- Mean route time decomposition:
  - mean road-path time `= 9.02 min`
  - mean connector time `= 3.75 min`
- Connector rank used on the sampled routes:
  - home side:
    - rank `1`: `11`
    - rank `2`: `3`
    - rank `3`: `6`
  - work side:
    - rank `1`: `8`
    - rank `2`: `9`
    - rank `3`: `3`
- `3` of the `20` sampled adjacent pairs use zero road edges and are connected entirely through connector-to-connector linkage:
  - `15_sq_35_29_to_sq_34_29.png`
  - `18_sq_51_27_to_sq_51_28.png`
  - `20_sq_7_31_to_sq_6_31.png`

### Interpretation

- This first audit set is useful precisely because it should reveal whether some adjacent-grid paths are being driven too much by connector geometry instead of actual road traversal.
- The presence of zero-road-edge adjacent routes is not automatically wrong, but these cases should be among the first manual inspections.
- Frequent use of rank-3 connectors in the sampled routes also suggests that connector choice remains a first-order determinant of local travel cost in the current raw-grid build.

## 2026-04-21: relaxed raw-grid `v3` rerun completed under strict period-specific speed use

### What changed relative to `relaxed_v2`

- `data_work/src/raw_connected_v1/grid_travel_time/build_raw_connected_v1_grid_travel_time.py` was updated so that:
  - `AM` runs use only `speed_am_kmh`
  - `PM` runs use only `speed_pm_kmh`
  - missing period-specific speeds are no longer filled from `FF` or `overall`
  - connector node local speeds are also derived only from retained period-specific road edges
- The `v3` output root is:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_relaxed_v3`

### Period-specific speed missingness

- Directed edges with missing AM speed:
  - total `= 24,763`
  - missing `= 270`
  - share `= 1.0903%`
- Directed edges with missing PM speed:
  - total `= 24,763`
  - missing `= 271`
  - share `= 1.0944%`
- Interpretation:
  - period-specific speed missingness is small but nonzero
  - under `v3`, these edges are no longer retained through cross-period fallback

### Cleaned `v3` configuration and speed-source metadata

- The output config and metrics were cleaned so they no longer imply cross-period speed fallback.
- `run_config.json` now explicitly records:
  - `strict_period_speed_only = true`
  - `connector_speed_rule = node_local_period_median_only`
  - `speed_stats.period_speed_source = speed_am_kmh` for the current AM build
- `FF` was also added as a valid explicit speed source option for future uncongested runs.

### `v3` versus `v2`: key quantitative differences

- Comparison file:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_relaxed_v3/diagnostics_compare_v2_vs_v3/v2_vs_v3_key_metrics.csv`
- Wide comparison file:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_relaxed_v3/diagnostics_compare_v2_vs_v3/v2_vs_v3_key_metrics_wide.csv`
- Main pattern:
  - `v3` is slightly tighter and slightly slower than `v2`, but not dramatically different
- Coverage:
  - square connected grids: `1238 -> 1235`
  - hex connected grids: `1768 -> 1766`
  - voronoi connected grids: `986 -> 984`
- Road graph retention:
  - used directed-edge share: `96.87% -> 95.85%`
  - used length share: `99.50% -> 98.94%`
- Weighted mean travel time:
  - square: `49.12 -> 50.47 min`
  - hex: `47.71 -> 48.37 min`
  - voronoi: `48.63 -> 49.90 min`
- OD correlation:
  - square: `0.876 -> 0.870`
  - hex: `0.802 -> 0.800`
  - voronoi: `0.894 -> 0.884`
- Interpretation:
  - removing cross-period fallback does not cause a collapse in the graph
  - but it does make the retained raw-grid network slightly sparser and slightly slower
  - the core bottleneck still appears to be connector design and graph construction, not speed interpolation alone

### `v3` square-grid route audit generated

- Output directory:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_relaxed_v3/route_audit_square_AM`
- This updated the same square-grid adjacent route audit under the stricter period-only speed rule.
- Compared with the earlier `v2` audit:
  - zero-road-edge sampled cases dropped from `3 / 20` to `1 / 20`
  - mean connector time dropped from `3.75` to `3.31 min`
  - mean road time dropped from `9.02` to `7.62 min`
- Interpretation:
  - under the sampled square-grid cases, the stricter speed rule does not worsen the local route pictures
  - if anything, the sampled routes appear slightly less dominated by pure connector-only movement

## 2026-04-21: next connector experiment chosen

### Decision

- Keep `relaxed_v3` as the current baseline raw-grid version.
- Start a **parallel** experiment rather than editing the baseline in place.

### New experiment goal

- Replace node-based connector attachment with a road-edge projection attachment:
  - instead of snapping a grid centroid to a nearby road node
  - project it to a nearby reasonable road edge
  - treat that projected access point as the road entry point
- Preserve separate code, separate output directories, and separate diagnostics so the baseline remains reproducible.

### Why this branch is worth trying

- The current node-based connector can overreact to:
  - dead-end nodes
  - awkward local direction structure
  - centroid sensitivity
- A projected access point on a nearby road segment may better represent:
  - where a grid would realistically enter the road system
  - a shorter and more natural centroid-to-road connection

### Required comparison outputs for the parallel branch

- Save all branch outputs separately from `relaxed_v3`
- Generate route-audit figures analogous to the baseline audit
- Generate coverage and travel-cost summaries analogous to the baseline diagnostics
- Produce a direct baseline-versus-branch comparison of connector behavior and route plausibility

## 2026-04-21: edge-projection connector parallel branch implemented and benchmarked

### Goal

- Keep `relaxed_v3` unchanged as the reproducible baseline.
- Build a parallel branch where grid connectors attach to projected access points on nearby reasonable road edges rather than to nearby road nodes.

### New files added

- Parallel build script:
  - `data_work/src/raw_connected_v1/grid_travel_time/build_raw_connected_v1_grid_travel_time_edgeproj.py`
- Parallel network diagnostics:
  - `data_work/src/diagnostics/plot_raw_connected_grid_network_diagnostics_edgeproj.py`
- Parallel route audit:
  - `data_work/src/diagnostics/sample_raw_connected_adjacent_route_audit_edgeproj.py`

### Output root

- Parallel branch output:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v1`

### What the parallel method changes

- Connector target is no longer a nearby node.
- For each grid:
  - search nearby road edges
  - project the grid centroid onto a nearby candidate edge
  - treat the projected point as an access point
  - split the host directed edge at projected access points
  - compute grid-to-grid costs through these split access nodes
- This branch still preserves:
  - strict period-only speed use
  - separate output directories and diagnostics
  - the same comparison framework against the retained centerline-grid baseline

### Main quantitative comparison against `relaxed_v3`

- Comparison files:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v1/diagnostics_compare_baseline_v3_vs_edgeproj/baseline_v3_vs_edgeproj_key_metrics.csv`
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v1/diagnostics_compare_baseline_v3_vs_edgeproj/baseline_v3_vs_edgeproj_key_metrics_wide.csv`

### Coverage gains

- Square connected grids:
  - `1,235 -> 1,591`
  - connected share `36.8% -> 47.4%`
- Hex connected grids:
  - `1,766 -> 2,273`
  - connected share `38.4% -> 49.4%`
- Voronoi connected grids:
  - `984 -> 1,072`
  - connected share `80.3% -> 87.5%`
- Mean connector distance falls substantially:
  - square: `1,094 m -> 831 m`
  - hex: `1,104 m -> 850 m`
  - voronoi: `674 m -> 433 m`
- Interpretation:
  - edge projection clearly improves connector coverage and shortens centroid-to-road attachment distance
  - this confirms that node-based snapping was one important source of under-connection

### Travel-cost tradeoff

- Weighted mean OD travel time becomes higher under edge projection:
  - square: `50.47 -> 60.07 min`
  - hex: `48.37 -> 59.86 min`
  - voronoi: `49.90 -> 57.60 min`
- OD correlation against the retained centerline-grid benchmark falls:
  - square: `0.870 -> 0.804`
  - hex: `0.800 -> 0.737`
  - voronoi: `0.884 -> 0.809`
- Adjacent-pair mean travel time also rises:
  - square: `12.35 -> 14.17 min`
  - hex: `10.47 -> 13.52 min`
  - voronoi: `13.24 -> 16.81 min`
- Interpretation:
  - the parallel branch gains connectivity, but it currently does so at the cost of longer implied travel times and weaker agreement with the centerline-grid benchmark
  - this likely reflects that the new access geometry is more permissive, but the resulting split-edge graph still needs further discipline before it becomes a clear improvement overall

### Directional pattern

- Adjacent-pair median time ratio moves slightly downward under edge projection:
  - square: `0.938 -> 0.890`
  - hex: `0.937 -> 0.886`
  - voronoi: `0.879 -> 0.839`
- Share of strongly asymmetric adjacent pairs (`ratio < 0.5`) rises:
  - square: `12.2% -> 14.4%`
  - hex: `10.8% -> 14.7%`
  - voronoi: `16.4% -> 19.8%`
- Interpretation:
  - edge projection does recover a bit more local directional variation than the node-based baseline
  - but it is still much more symmetric than the retained centerline-grid benchmark

### Edge-projection route audit

- Output directory:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v1/route_audit_square_AM`
- `20` square-grid adjacent route-audit figures were generated under the same random seed.
- Summary file:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v1/route_audit_square_AM/sampled_adjacent_route_summary.csv`
- Quick takeaways:
  - zero-road-edge sampled cases: `0 / 20`
  - mean connector time `= 1.84 min`
  - mean road time `= 9.41 min`
- Compared with baseline `relaxed_v3`:
  - baseline had `1 / 20` zero-road-edge cases
  - baseline mean connector time `= 3.31 min`
  - baseline mean road time `= 7.62 min`
- Interpretation:
  - the edge-projection branch does exactly what it was meant to do geometrically:
    - connectors become shorter
    - sampled routes rely less on pure connector-only movement
  - but the road-path component in the sampled routes becomes longer on average
  - this is consistent with the aggregate statistics showing better attachment but slower implied end-to-end movement

### Diagnostics generated

- Comparison summaries:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v1/diagnostics_compare/`
- Network diagnostics:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v1/diagnostics_network/`
- Baseline-versus-branch comparison:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v1/diagnostics_compare_baseline_v3_vs_edgeproj/`

### Current take-away

- The edge-projection connector branch is promising as a geometry fix:
  - higher grid coverage
  - shorter connector distances
  - fewer obviously connector-only local routes in the square-grid audit
- But in its current form it is **not yet** a clean replacement for the baseline:
  - travel times are higher
  - benchmark alignment is weaker
  - the split-edge access construction likely needs another round of discipline
- The most natural next question is whether edge projection should be combined with:
  - pair-specific shortest-path construction
  - tighter candidate-edge filters
  - or both

## 2026-04-20: raw-centerline baseline locked at `manual_centerline_rules_v11`

### Matching status

- Retained final version: `data_work/outputs/manual_centerline_rules_v11`
- Retained strategy: `plan1 / v5a`
- Discarded strategy: `fullskeleton_v1`
- `manual_centerline_groups.json` expanded through the latest retained manual additions

### Locked construction logic

- Manual raw edges are excluded from the default skeleton input.
- Manual centerlines are built separately from grouped raw corridors.
- Manual corridors generate exact `raw_edge_id -> cline_id` crosswalks.
- `stage02` uses exact matching for manual raw edges and baseline distance-cap matching for the rest.
- Current stage01 execution path is `5m` skeleton with tiling.

### Locked parameters

- Stage01 config: `data_work/config/manual_centerline_plan1.json`
- `manual_exclude_from_skeleton_input = true`
- `manual_replace_default_centerline = false`
- `BUF_WIDTH = 50.0`
- `RES = 5.0`
- `MIN_CL_LEN = 50.0`
- `SKELETON_MAX_RASTER_MPIX = 600`
- `manual_midline_step = 50.0`
- `manual_simplify_tol = 10.0`
- `manual_connect_snap_m = 35.0`
- `manual_pair_max_dist_m = 120.0`
- `min_seg_len_baseline = 50.0`
- `cut_buf = 40.0`
- `snap_tol = 30.0`
- `min_seg_gap = 5.0`
- `split_search_dist = 80.0`
- `node_snap_tol = 35.0`
- `node_buffer = 40.0`
- `merge_cut_dist = 30.0`
- `min_split_seg_len = 120.0`
- `dist_cap = 180.0`

### Key stage01-02 results

- `manual group = 129`
- `manual exact raw edges = 963`
- `manual centerline fragments = 310`
- `split_match_rate = 0.994243`
- `raw_edge_match_rate = 0.994242`
- `matched_split_segments = 23,662`
- `unmatched_split_segments = 137`
- `matched_raw_edges = 23,672`
- `unmatched_raw_edges = 1,091`
- unmatched raw length share `= 1.27%`
- unmatched raw-edge share `= 4.41%`

### Interpretation of remaining unmatched segments

- The remaining unmatched share is small in total length and concentrated in a narrow long-corridor tail.
- Short unmatched fragments are numerous but account for little total length.
- Further manual work should focus on a short list of long and geometrically complex corridors rather than network-wide cleanup.

### Output retention

- Retained:
  - `data_work/outputs/manual_centerline_rules_v11`
  - `data_work/outputs/comparison/`
- Non-final raw-centerline intermediate versions were removed from `data_work/outputs/`

## 2026-04-20: paper and documentation refresh completed

### Documentation updates

- `data_work/README.md` updated to reflect the retained raw-centerline workflow
- `data_work/docs/CODEX_HANDOFF_NOTE.md` updated to point new sessions to `manual_centerline_rules_v11`
- `data_work/outputs/comparison/raw_centerline_match_version_summary.md` updated to summarize retained versions and decisions

### TeX and figure updates

- `Documents/TidalLanes_0320.tex` Section 3 updated around the retained raw-centerline workflow
- Image path issues were fixed and the file compiles successfully
- Section 3 tables were restored to mostly default layout without unnecessary `resizebox`
- Matching diagnostics now emphasize high first-step coverage and the concentration of residual unmatched corridors

### Current appendix-facing summary stats already in use

- Baseline-kept network summary:
  - raw: `23,795`
  - raw_split: `23,799`
  - undirected_centerline: `5,945`
  - directed_centerline: `11,890`
- Manual exact-match corridors:
  - `963` raw edges
  - `3,846.0 km`
  - count share `3.9%`
- Directed centerline coverage:
  - all directions covered share `0.7059`
- Undirected coverage:
  - both directions covered share `0.6219`
  - one direction covered share `0.1680`
  - none covered share `0.2101`

## 2026-04-20: stage03-04 completed on top of `manual_centerline_rules_v11`

### Run command

```bash
python3 data_work/src/stages/run_full_pipeline.py \
  --config data_work/config/manual_centerline_plan1.json \
  --version-id manual_centerline_rules_v11 \
  --output-dir data_work/outputs \
  --from-stage stage03 --to-stage stage04
```

### Main outputs

- `data_work/outputs/manual_centerline_rules_v11/data/centerline_speed_master.parquet`
- `data_work/outputs/manual_centerline_rules_v11/data/centerline_asymmetry_table.parquet`
- `data_work/outputs/manual_centerline_rules_v11/data/centerline_asymmetry_summary.csv`
- `data_work/outputs/manual_centerline_rules_v11/data/centerline_asymmetry_am_pm_comparison.csv`
- `data_work/outputs/manual_centerline_rules_v11/data/centerline_tidal_lane_candidates.csv`
- `data_work/outputs/manual_centerline_rules_v11/data/centerline_lane_master.parquet`
- `data_work/outputs/manual_centerline_rules_v11/metrics/stage03_speed_summary.csv`
- `data_work/outputs/manual_centerline_rules_v11/metrics/stage03b_asymmetry_summary.csv`
- `data_work/outputs/manual_centerline_rules_v11/metrics/stage04_lane_summary.csv`
- `data_work/outputs/manual_centerline_rules_v11/figures/hist_speed_ratio_am_pm.png`

### Stage03 speed attachment

- loaded speed observations: `24,336,848`
- matched speed observations: `23,286,622`
- centerline-time rows: `394,468`
- directed centerlines with speed: `8,349`
- mean speed: `36.30 km/h`
- median speed: `33.33 km/h`
- period means:
  - `AM = 35.19 km/h`
  - `PM = 33.33 km/h`
  - `FF_2205 = 38.46 km/h`

### Stage03b asymmetry

- asymmetry table rows: `9,315`
- centerlines with both directions observed:
  - `AM = 3,670`
  - `PM = 3,669`
- `unsym1` share among both-direction centerlines:
  - `AM = 11.04%`
  - `PM = 9.84%`
- unique centerlines flagged as `unsym1` in at least one peak: `638`
- overlap:
  - `AM only = 277`
  - `PM only = 233`
  - `Both AM and PM = 128`
- reversed faster direction count: `1,032`
- strict tidal-lane candidates: `14`

### Stage04 lane attachment

- total directed rows in `centerline_lane_master`: `45,752`
- rows with valid lane estimate: `8,400`
- mean lane estimate: `3.13`
- median lane estimate: `3.32`
- mean opposite-direction lane ratio: `1.036`
- valid rows with symmetric lane flag: `86.17%`
- projection-supported lane rows: `0`

### Working interpretation

- The speed attachment step ran successfully on top of the retained `v11` match output.
- PM peak speeds are lower than AM peak speeds in the aggregate, which is directionally plausible for the Beijing commuting sample.
- Directional asymmetry is present but not pervasive: around one-tenth of pairable centerlines meet the baseline `unsym1` threshold in each peak.
- The strict tidal-candidate screen is highly selective, leaving only `14` centerlines after requiring both strong asymmetry and reversal between AM and PM.
- The next task is therefore interpretive rather than mechanical: check whether these candidate and asymmetry patterns line up with the corridors we expect, then decide whether any additional manual corridor work is still worth doing.
