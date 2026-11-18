# Outputs registry

Generated 2026-11-18 by `data_work/src/diagnostics/build_outputs_registry.py`; rerun it after adding outputs. 475 folders in `data_work/outputs/`.

Classification is by folder name and contents, so treat it as a first sort. The current mainline is defined by `MAINLINE_MANIFEST.md` and the "Current mainline" table in `DOCUMENT_INDEX.md`, not by this file. "Cited by" marks folders named in old-mainline documents or in the deck, paper, and replication packages; keep those even when the category says history. A folder missing here is newer than the last run.

| Category | Folders | Meaning |
|---|---|---|
| Current mainline | 25 | Listed in `MAINLINE_MANIFEST.md`. Back up; do not move or delete. |
| Current-spec experiments | 152 | Runs on the 1B / 1A networks, 2025 platform data, or the November checks. Context for the mainline; most are robustness or trials. |
| 2025 OD on the gate5 network | 70 | Transition runs (Sept to early Nov 2026) that put 2025 OD on the old gate5 network. Superseded by 1B. |
| Old gate5 mainline and its experiments | 163 | 2022 OD, anchored lambda = 1.1, BPR trials, Stage 1 and Stage 2. Keep the ones marked as cited; the rest are history. |
| Pipeline baselines and Baidu validation | 23 | Stage pipeline, centreline match, Baidu route validation. |
| Shenzhen, Caitian, national evidence | 21 | Reduced-form evidence lines, currently paused. |
| Smoke tests and named failures | 14 | Safe to delete after a check. |
| Failed runs (only run.log) | 7 | Recorded as void in the work log. Safe to delete. |

## Current mainline (25)

| Folder | README | Cited by |
|---|---|---|
| `background_traffic_trial_v1` | yes | deck / paper / replication |
| `beta_c_b035_downstream_rerun_v1` | yes |  |
| `beta_c_rerun_b035_summary_v1` | yes | deck / paper / replication |
| `congestion_floor_sensitivity_v1` | yes | deck / paper / replication |
| `flow_density_per_lane_v1` | yes | deck / paper / replication |
| `grid_pair_baidu_validation_v3` |  |  |
| `link_time_gamma_eta_scan_osm1b_alldaytt0709_v1` |  |  |
| `load_dependent_elasticity_trial_v1` | yes | deck / paper / replication |
| `model_input_edgeproj_v2_lcc_gate5_od2025_v2_odallday` |  |  |
| `model_input_osm1b_L300_gate5_od2025_v2_alldaytt0709` |  |  |
| `osm_grid_travel_time_v2` |  |  |
| `policy_comparison_osm1b_ce_floor_b025_fl001_ks_od2025_alldaytt0709_v1` |  |  |
| `policy_comparison_osm1b_ce_floor_b035_fl001_ks_od2025_alldaytt0709_v1` |  | deck / paper / replication |
| `policy_multistart_osm1b_ce_floor_b035_fl001_ks_od2025_alldaytt0709_v1` |  | deck / paper / replication |
| `pricing_additive_osm1b_ce_floor_b025_fl001_ks_od2025_alldaytt0709_v1` |  |  |
| `selection_split_day_check_osm1b_ce_floor_b035_fl001_ks_od2025_v1` |  |  |
| `stage2_endogenous_osm1b_ce_floor_b025_fl001_ks_od2025_v1` |  |  |
| `symmetric_input_reestimated_osm1b_ce_floor_b035_fl001_ks_arithmetic_od2025_v1` |  |  |
| `symmetric_input_reestimated_osm1b_ce_floor_b035_fl001_ks_geometric_od2025_v1` |  |  |
| `theta_first_second_order_osm1b_b035_alldaytt0709_v1` |  | deck / paper / replication |
| `tidal_lane_diagnostics_osm1b_ce_floor_b025_fl001_ks_od2025_alldaytt0709_eta0p21_v1` |  |  |
| `tidal_lane_diagnostics_osm1b_ce_floor_b025_fl001_ks_od2025_alldaytt0709_v3` |  |  |
| `tidal_lane_diagnostics_osm1b_ce_floor_b029_fl001_ks_od2025_alldaytt0709_v1` |  |  |
| `tidal_lane_diagnostics_osm1b_ce_floor_b035_fl001_ks_od2025_alldaytt0709_v1` |  |  |
| `tidal_lane_diagnostics_osm1b_ce_floor_b044_fl001_ks_od2025_alldaytt0709_v1` |  |  |

## Current-spec experiments (152)

| Folder | README | Cited by |
|---|---|---|
| `eq_uniqueness_osm1a_ce_floor_b025_fl001_ks_v1` |  |  |
| `eq_uniqueness_osm1b_bpr_b165_fl001_ks_v1` |  |  |
| `eq_uniqueness_osm1b_ce_floor_b025_fl001_ks_v1` |  |  |
| `flow_density_per_lane_v1_sens_keepfill` |  |  |
| `grid_chain_vs_road_od_osm1b_v1` |  |  |
| `grid_topology_vs_road_od_osm1b_hex_v1` |  |  |
| `grid_topology_vs_road_od_osm1b_sq4_v1` |  |  |
| `grid_topology_vs_road_od_osm1b_sq4_v2` |  |  |
| `grid_topology_vs_road_od_osm1b_sq8_v1` |  |  |
| `link_time_construction_example_v1` |  |  |
| `link_time_gamma_eta_scan_osm1b_alldaytt0709_fine_v1` |  |  |
| `link_time_gamma_scan_osm1b_alldaytt0709_rho0p999_v1` |  |  |
| `link_time_gamma_scan_osm1b_alldaytt0709_rho0p99_v1` |  |  |
| `link_time_gamma_scan_osm1b_alldaytt0709_v1` |  |  |
| `model_input_osm1a_gate5_od2025_v1` |  |  |
| `model_input_osm1a_gate5_od2025_v2` |  |  |
| `model_input_osm1a_gate5_od2025_v2_symmetric_arithmetic_v1` |  |  |
| `model_input_osm1a_gate5_od2025_v2_symmetric_geometric_v1` |  |  |
| `model_input_osm1b_L300_gate5_am_even_days_od2025_v1` |  |  |
| `model_input_osm1b_L300_gate5_am_odd_days_od2025_v1` |  |  |
| `model_input_osm1b_L300_gate5_od2025_v1` |  |  |
| `model_input_osm1b_L300_gate5_od2025_v2` |  |  |
| `model_input_osm1b_L300_gate5_od2025_v2_alldaytt0610` |  |  |
| `model_input_osm1b_L300_gate5_od2025_v2_alldaytt0709_accminor20` |  |  |
| `model_input_osm1b_L300_gate5_od2025_v2_chain0610mxtt0610` |  |  |
| `model_input_osm1b_L300_gate5_od2025_v2_chain0610tt0610` |  |  |
| `model_input_osm1b_L300_gate5_od2025_v2_chain0709tt0709` |  |  |
| `model_input_osm1b_L300_gate5_od2025_v2_ctl0709` |  |  |
| `model_input_osm1b_L300_gate5_od2025_v2_dep0610tt0610` |  |  |
| `model_input_osm1b_L300_gate5_od2025_v2_dep0610tt0709` |  |  |
| `model_input_osm1b_L300_gate5_od2025_v2_onroad0610tt0610` |  |  |
| `model_input_osm1b_L300_gate5_od2025_v2_symmetric_arithmetic_v1` |  |  |
| `model_input_osm1b_L300_gate5_od2025_v2_symmetric_geometric_v1` |  |  |
| `model_input_osm1b_L300_gate5_od2025_v2_win0709tt0610` |  |  |
| `model_input_osm1b_L300_nb8_gate5_od2025_v2` |  |  |
| `network_components_compare_v1` |  |  |
| `network_form_matrix_v1` |  |  |
| `network_form_matrix_v2` |  |  |
| `network_grid_reachability_v1` |  |  |
| `network_mainline_compare_v1` |  |  |
| `network_mainline_compare_v2` |  |  |
| `network_osm_gap_bridging_v1` |  |  |
| `network_osm_observed_variants_v1` |  |  |
| `network_versions_slide_v1` |  |  |
| `osm_directed_speed_v1` |  |  |
| `osm_grid_rob_1a_absolute_v1` |  |  |
| `osm_grid_rob_1a_classonly_v1` |  |  |
| `osm_grid_rob_1a_fallback_class_v1` |  |  |
| `osm_grid_rob_1a_hops1_v1` |  |  |
| `osm_grid_rob_1a_hops5_v1` |  |  |
| `osm_grid_rob_1b_L600_v1` |  |  |
| `osm_grid_robustness_compare_v1` |  |  |
| `osm_grid_travel_time_v1` |  |  |
| `osm_grid_travel_time_v2_acc_minor_v20` |  |  |
| `osm_grid_travel_time_v2_am0610cw` |  |  |
| `osm_grid_travel_time_v2_am_even_days` |  |  |
| `osm_grid_travel_time_v2_am_odd_days` |  |  |
| `osm_grid_travel_time_v2_nb8` |  |  |
| `osm_grid_travel_time_v2_recheck0709` |  |  |
| `policy_comparison_osm1a_ce_floor_b025_fl001_od2025_v1` |  |  |
| `policy_comparison_osm1b_bpr_b165_fl001_ks_od2025_v1` |  |  |
| `policy_comparison_osm1b_ce_floor_b025_fl001_ks_od2025_v1` |  |  |
| `policy_comparison_osm1b_ce_floor_b025_fl001_od2025_v1` |  |  |
| `pricing_additive_osm1b_bpr_b165_fl001_ks_od2025_v2` |  |  |
| `pricing_additive_osm1b_bpr_b165_fl001_ks_od2025_v3` |  |  |
| `pricing_additive_osm1b_ce_floor_b025_fl001_ks_od2025_v2` |  |  |
| `pricing_additive_osm1b_ce_floor_b025_fl001_ks_od2025_v3` |  |  |
| `proportional_background_check_osm1b_dep0610_v1` |  |  |
| `purpose_share_r_202505_v1` |  |  |
| `rn_osm_flow_v1` |  |  |
| `road_layer_model_input_osm1b_L300_gate5_od2025_v2_v1` |  |  |
| `road_layer_model_input_osm1b_L300_nb8_gate5_od2025_v2_v1` |  |  |
| `segment_flow_10min_rn202505_v1` |  |  |
| `segment_speed_flow_rn202505_v1` |  |  |
| `selection_split_day_check_osm1b_bpr_b165_fl001_ks_od2025_v1` |  |  |
| `selection_split_day_check_osm1b_ce_floor_b025_fl001_ks_od2025_v1` |  |  |
| `stage2_endogenous_osm1a_ce_floor_b025_fl001_od2025_v1` |  |  |
| `stage2_endogenous_osm1b_bpr_b165_fl001_ks_od2025_v1` |  |  |
| `stage2_endogenous_osm1b_ce_floor_b025_fl001_od2025_v1` |  |  |
| `symmetric_input_reestimated_osm1a_ce_floor_b025_fl001_arithmetic_od2025_v1` |  |  |
| `symmetric_input_reestimated_osm1a_ce_floor_b025_fl001_geometric_od2025_v1` |  |  |
| `symmetric_input_reestimated_osm1b_bpr_b165_fl001_ks_arithmetic_od2025_v1` |  |  |
| `symmetric_input_reestimated_osm1b_bpr_b165_fl001_ks_geometric_od2025_v1` |  |  |
| `symmetric_input_reestimated_osm1b_ce_floor_b025_fl001_arithmetic_od2025_v1` |  |  |
| `symmetric_input_reestimated_osm1b_ce_floor_b025_fl001_geometric_od2025_v1` |  |  |
| `symmetric_input_reestimated_osm1b_ce_floor_b025_fl001_ks_arithmetic_od2025_v1` |  |  |
| `symmetric_input_reestimated_osm1b_ce_floor_b025_fl001_ks_geometric_od2025_v1` |  |  |
| `theta_first_second_order_osm1b_alldaytt0709_v1` |  |  |
| `tidal_lane_diagnostics_osm1a_bpr_b165_fl001_s4_od2025_v1` |  |  |
| `tidal_lane_diagnostics_osm1a_bpr_b425_fl001_s4_od2025_v1` |  |  |
| `tidal_lane_diagnostics_osm1a_bpr_pos05_physical_time_lam020_od2025_v2` |  |  |
| `tidal_lane_diagnostics_osm1a_ce_floor_b025_fl001_k150_od2025_v1` |  |  |
| `tidal_lane_diagnostics_osm1a_ce_floor_b025_fl001_s4_od2025_v1` |  |  |
| `tidal_lane_diagnostics_osm1b_anchored_lam11_fl001_ks_od2025_v1` |  |  |
| `tidal_lane_diagnostics_osm1b_bpr_b165_fl001_ks_od2025_v1` |  |  |
| `tidal_lane_diagnostics_osm1b_bpr_b165_fl001_s4_od2025_v1` |  |  |
| `tidal_lane_diagnostics_osm1b_bpr_b425_fl001_s4_od2025_v1` |  |  |
| `tidal_lane_diagnostics_osm1b_bpr_pos05_physical_time_lam020_od2025_v2` |  |  |
| `tidal_lane_diagnostics_osm1b_ce_floor_b008_fl001_ks_od2025_v1` |  |  |
| `tidal_lane_diagnostics_osm1b_ce_floor_b016_fl001_ks_od2025_v1` |  |  |
| `tidal_lane_diagnostics_osm1b_ce_floor_b025_fl001_ks_od2025_alldaytt0610_v2` |  |  |
| `tidal_lane_diagnostics_osm1b_ce_floor_b025_fl001_ks_od2025_alldaytt0709_accminor20_v1` |  |  |
| `tidal_lane_diagnostics_osm1b_ce_floor_b025_fl001_ks_od2025_alldaytt0709_eta0p23_v1` |  |  |
| `tidal_lane_diagnostics_osm1b_ce_floor_b025_fl001_ks_od2025_alldaytt0709_eta1check_v1` |  |  |
| `tidal_lane_diagnostics_osm1b_ce_floor_b025_fl001_ks_od2025_chain0610mxtt0610_v2` |  |  |
| `tidal_lane_diagnostics_osm1b_ce_floor_b025_fl001_ks_od2025_chain0610tt0610_v2` |  |  |
| `tidal_lane_diagnostics_osm1b_ce_floor_b025_fl001_ks_od2025_chain0709tt0709_v2` |  |  |
| `tidal_lane_diagnostics_osm1b_ce_floor_b025_fl001_ks_od2025_ctl0709_v2` |  |  |
| `tidal_lane_diagnostics_osm1b_ce_floor_b025_fl001_ks_od2025_dep0610tt0610_v2` |  |  |
| `tidal_lane_diagnostics_osm1b_ce_floor_b025_fl001_ks_od2025_dep0610tt0709_v2` |  |  |
| `tidal_lane_diagnostics_osm1b_ce_floor_b025_fl001_ks_od2025_onroad0610tt0610_v2` |  |  |
| `tidal_lane_diagnostics_osm1b_ce_floor_b025_fl001_ks_od2025_v1` |  |  |
| `tidal_lane_diagnostics_osm1b_ce_floor_b025_fl001_ks_od2025_win0709tt0610_v2` |  |  |
| `tidal_lane_diagnostics_osm1b_ce_floor_b025_fl001_od2025_v1` |  |  |
| `tidal_lane_diagnostics_osm1b_ce_floor_b025_fl001_s4_od2025_v1` |  |  |
| `tidal_lane_diagnostics_osm1b_ce_floor_b025_fl0_ks_od2025_v1` |  |  |
| `tidal_lane_diagnostics_osm1b_ce_floor_b025_fl0_od2025_v1` |  |  |
| `tidal_lane_diagnostics_osm1b_ce_floor_b042_fl001_ks_od2025_v1` |  |  |
| `tidal_lane_diagnostics_osm1b_nb8_bpr_b165_fl001_ks_od2025_v1` |  |  |
| `tidal_lane_diagnostics_osm1b_nb8_ce_floor_b025_fl001_ks_od2025_v1` |  |  |
| `tidal_mainline_bpr_pos05_od2025_v2` |  |  |
| `tidal_road_level_decomp_osm1b_ce_floor_b025_fl001_ks_v1` |  |  |
| `tidal_road_level_decomp_osm1b_ce_floor_b025_fl001_ks_v2` |  |  |
| `tidal_road_level_decomp_osm1b_nb8_ce_floor_b025_fl001_ks_v2` |  |  |
| `tidal_road_level_identity_check_osm1b_ce_floor_v1` |  |  |
| `tidal_road_level_identity_check_osm1b_ce_floor_v2` |  |  |
| `tidal_road_level_osm1b_bpr_b165_fl001_ks_v1` |  |  |
| `tidal_road_level_osm1b_bpr_b165_fl001_ks_v2` |  |  |
| `tidal_road_level_osm1b_bpr_b165_fl001_ks_v3` |  |  |
| `tidal_road_level_osm1b_bpr_b165_fl001_ks_v4_opp0` |  |  |
| `tidal_road_level_osm1b_bpr_b165_fl001_ks_v5_opp0` |  |  |
| `tidal_road_level_osm1b_bpr_b219_fl001_ks_v5_opp0` |  |  |
| `tidal_road_level_osm1b_ce_floor_b025_fl001_ks_v1` |  |  |
| `tidal_road_level_osm1b_ce_floor_b025_fl001_ks_v2` |  |  |
| `tidal_road_level_osm1b_ce_floor_b025_fl001_ks_v3` |  |  |
| `tidal_road_level_osm1b_ce_floor_b025_fl001_ks_v4_opp0` |  |  |
| `tidal_road_level_osm1b_ce_floor_b025_fl001_ks_v5_opp0` |  |  |
| `tidal_road_level_osm1b_ce_floor_b042_fl001_ks_v5_opp0` |  |  |
| `tidal_road_level_osm1b_nb8_bpr_b165_fl001_ks_v1` |  |  |
| `tidal_road_level_osm1b_nb8_bpr_b165_fl001_ks_v2` |  |  |
| `tidal_road_level_osm1b_nb8_bpr_b165_fl001_ks_v3` |  |  |
| `tidal_road_level_osm1b_nb8_bpr_b165_fl001_ks_v4_opp0` |  |  |
| `tidal_road_level_osm1b_nb8_bpr_b165_fl001_ks_v5_opp0` |  |  |
| `tidal_road_level_osm1b_nb8_bpr_b219_fl001_ks_v5_opp0` |  |  |
| `tidal_road_level_osm1b_nb8_ce_floor_b025_fl001_ks_v1` |  |  |
| `tidal_road_level_osm1b_nb8_ce_floor_b025_fl001_ks_v2` |  |  |
| `tidal_road_level_osm1b_nb8_ce_floor_b025_fl001_ks_v3` |  |  |
| `tidal_road_level_osm1b_nb8_ce_floor_b025_fl001_ks_v4_opp0` |  |  |
| `tidal_road_level_osm1b_nb8_ce_floor_b025_fl001_ks_v5_opp0` |  |  |
| `tidal_road_level_osm1b_nb8_ce_floor_b042_fl001_ks_v5_opp0` |  |  |
| `unicom_2025_input_feasibility_v1` |  |  |
| `unicom_model_flow_validation_v1` |  |  |

## 2025 OD on the gate5 network (70)

| Folder | README | Cited by |
|---|---|---|
| `all_traffic_speed_panel_rn202505_gate5_v1` |  |  |
| `lambda_identification_rn202505_gate5_v1` |  |  |
| `lambda_identification_rn202505_gate5_v2` |  |  |
| `model_input_edgeproj_v2_lcc_gate5_am_even_days_od2025_v1` |  |  |
| `model_input_edgeproj_v2_lcc_gate5_am_odd_days_od2025_v1` |  |  |
| `model_input_edgeproj_v2_lcc_gate5_ff_0005_all_harmonic_od2025_v1` |  |  |
| `model_input_edgeproj_v2_lcc_gate5_ff_0104_all_harmonic_od2025_v1` |  |  |
| `model_input_edgeproj_v2_lcc_gate5_ff_0205_all_harmonic_od2025_v1` |  |  |
| `model_input_edgeproj_v2_lcc_gate5_ff_0205_all_p90_od2025_v1` |  |  |
| `model_input_edgeproj_v2_lcc_gate5_ff_2205_all_p90_od2025_v1` |  |  |
| `model_input_edgeproj_v2_lcc_gate5_ff_2205_weekday_harmonic_od2025_v1` |  |  |
| `model_input_edgeproj_v2_lcc_gate5_ff_2205_weekend_harmonic_od2025_v1` |  |  |
| `model_input_edgeproj_v2_lcc_gate5_od2025_v1` |  |  |
| `model_input_edgeproj_v2_lcc_gate5_od2025_v2` |  |  |
| `model_input_edgeproj_v2_lcc_gate5_od2025_v2_odchain0610` |  |  |
| `model_input_edgeproj_v2_lcc_gate5_od2025_v2_odchain0610mx` |  |  |
| `model_input_edgeproj_v2_lcc_gate5_od2025_v2_odchain0709` |  |  |
| `model_input_edgeproj_v2_lcc_gate5_od2025_v2_oddep0610` |  |  |
| `model_input_edgeproj_v2_lcc_gate5_od2025_v2_oddep0709` |  |  |
| `model_input_edgeproj_v2_lcc_gate5_od2025_v2_odonroad0610` |  |  |
| `model_input_edgeproj_v2_lcc_gate5_od2025_v2_recheck_win0709` |  |  |
| `model_input_edgeproj_v2_lcc_gate5_symmetric_arithmetic_od2025_v2` |  |  |
| `model_input_edgeproj_v2_lcc_gate5_symmetric_geometric_od2025_v1` |  |  |
| `model_input_edgeproj_v2_lcc_gate5_symmetric_geometric_od2025_v2` |  |  |
| `model_input_raw_connected_v3_edgeproj_v2_lcc_gate5_lanefix_od2025_v1` |  |  |
| `observed_traffic_rn202505_gate5_od2025_v1` |  |  |
| `observed_traffic_rn202505_gate5_v1` |  |  |
| `od2025_vs_od2022_gate5_v1` |  |  |
| `policy_comparison_gate5_bpr_b165_fl001_od2025_v1` |  |  |
| `policy_comparison_gate5_ce_floor_b025_fl001_od2025_v1` |  |  |
| `pricing_additive_gate5_anchored_lam11_od2025_v2` |  |  |
| `pricing_additive_gate5_anchored_lam11_od2025_v3` |  |  |
| `pricing_additive_gate5_ce_floor_b025_fl001_od2025_v2` |  |  |
| `pricing_additive_gate5_ce_floor_b025_fl001_od2025_v3` |  |  |
| `selection_split_day_check_gate5_bpr_b165_fl001_od2025_v1` |  |  |
| `selection_split_day_check_gate5_ce_floor_b025_fl001_od2025_v1` |  |  |
| `selection_split_day_check_gate5_od2025_v2` |  |  |
| `stage2_endogenous_edgeproj_v2_lcc_gate5_bpr_b165_fl001_od2025_v1` |  |  |
| `stage2_endogenous_edgeproj_v2_lcc_gate5_ce_floor_b025_fl001_od2025_v1` |  |  |
| `stage2_gate5_freeflow_baseline_2205_all_harmonic_quick_od2025_v2` |  |  |
| `stage2_gate5_freeflow_ff_0005_all_harmonic_quick_od2025_v2` |  |  |
| `stage2_gate5_freeflow_ff_0104_all_harmonic_quick_od2025_v2` |  |  |
| `stage2_gate5_freeflow_ff_0205_all_harmonic_quick_od2025_v2` |  |  |
| `stage2_gate5_freeflow_ff_0205_all_p90_quick_od2025_v2` |  |  |
| `stage2_gate5_freeflow_ff_2205_all_p90_quick_od2025_v2` |  |  |
| `stage2_gate5_freeflow_ff_2205_weekday_harmonic_quick_od2025_v2` |  |  |
| `stage2_gate5_freeflow_ff_2205_weekend_harmonic_quick_od2025_v2` |  |  |
| `symmetric_input_reestimated_gate5_bpr_b165_fl001_arithmetic_od2025_v1` |  |  |
| `symmetric_input_reestimated_gate5_bpr_b165_fl001_od2025_v1` |  |  |
| `symmetric_input_reestimated_gate5_ce_floor_b025_fl001_arithmetic_od2025_v1` |  |  |
| `symmetric_input_reestimated_gate5_ce_floor_b025_fl001_od2025_v1` |  |  |
| `tidal_lane_diagnostics_clv11routed_bpr_b165_fl001_ks_od2025_v1` |  |  |
| `tidal_lane_diagnostics_clv11routed_bpr_b165_fl001_s4_od2025_v1` |  |  |
| `tidal_lane_diagnostics_clv11routed_bpr_b425_fl001_s4_od2025_v1` |  |  |
| `tidal_lane_diagnostics_clv11routed_ce_floor_b025_fl001_ks_od2025_v1` |  |  |
| `tidal_lane_diagnostics_clv11routed_ce_floor_b025_fl001_s4_od2025_v1` |  |  |
| `tidal_lane_diagnostics_clv3routed_bpr_b165_fl001_ks_od2025_v1` |  |  |
| `tidal_lane_diagnostics_clv3routed_bpr_b165_fl001_s4_od2025_v1` |  |  |
| `tidal_lane_diagnostics_clv3routed_bpr_b425_fl001_s4_od2025_v1` |  |  |
| `tidal_lane_diagnostics_clv3routed_ce_floor_b025_fl001_ks_od2025_v1` |  |  |
| `tidal_lane_diagnostics_clv3routed_ce_floor_b025_fl001_s4_od2025_v1` |  |  |
| `tidal_lane_diagnostics_gate5_bpr_b165_fl001_s4_od2025_v1` |  |  |
| `tidal_lane_diagnostics_gate5_bpr_b425_fl001_s4_od2025_v1` |  |  |
| `tidal_lane_diagnostics_gate5_ce_floor_b025_fl001_k150_od2025_v1` |  |  |
| `tidal_lane_diagnostics_gate5_ce_floor_b025_fl001_od2025_v1` |  |  |
| `tidal_lane_diagnostics_gate5_ce_floor_b025_fl001_s4_od2025_v1` |  |  |
| `tidal_lane_diagnostics_gate5_ce_floor_b025_fl0_od2025_v1` |  |  |
| `tidal_lane_diagnostics_v3gate5_bpr_b165_fl001_s4_od2025_v1` |  |  |
| `tidal_lane_diagnostics_v3gate5_bpr_b425_fl001_s4_od2025_v1` |  |  |
| `tidal_lane_diagnostics_v3gate5_ce_floor_b025_fl001_s4_od2025_v1` |  |  |

## Old gate5 mainline and its experiments (163)

| Folder | README | Cited by |
|---|---|---|
| `alpha_beta_lt_decomposition_gate5_lanefix_v1` |  |  |
| `alpha_beta_sensitivity_bpr_pos05_explore_v1` |  | old-mainline doc |
| `alpha_beta_sensitivity_bpr_pos05_physical_time_v1` |  | old-mainline doc, deck / paper / replication |
| `alpha_beta_sensitivity_bpr_pos05_v1` |  | old-mainline doc, deck / paper / replication |
| `alpha_robustness_quick_gate5_v1` |  | deck / paper / replication |
| `am_day_split_speed_caches_v1` | yes |  |
| `anchored_model_audit_gate5_v1` |  |  |
| `beijing_commuting_direction_evidence_v1` |  | deck / paper / replication |
| `coauthor_revision_attempts_20260710_v1` | yes |  |
| `comparison` |  | old-mainline doc, deck / paper / replication |
| `comparison_grid_methods` |  | deck / paper / replication |
| `congestion_almagro_spec_v1` |  |  |
| `congestion_bpr_iv_v1` |  |  |
| `congestion_falling_tail_v1` |  |  |
| `congestion_far_od_iv_v1` |  |  |
| `congestion_forms_compare_v1` |  |  |
| `congestion_forms_slide_v1` |  |  |
| `congestion_iv_followup_v1` |  |  |
| `congestion_lambda11_comparison_edgeproj_v2_lcc_gate5_v1` |  |  |
| `congestion_reduced_form_gate5_v1` |  |  |
| `congestion_shape_slide_v1` |  |  |
| `congestion_tail_followup_v1` |  |  |
| `congestion_technology_explanation_20261103` |  |  |
| `directional_cost_edgeproj_v2_lcc_gate5_v1` |  | deck / paper / replication |
| `directional_cost_edgeproj_v2_lcc_gate5_v1_reinvert_geometric` |  |  |
| `directional_cost_edgeproj_v2_lcc_gate5_v1_reinvert_geometric_v2` |  |  |
| `directional_cost_edgeproj_v2_lcc_gate5_v1_reinvert_spatial_mean` |  |  |
| `directional_cost_edgeproj_v2_lcc_gate5_v1_reinvert_speed_arithmetic` |  |  |
| `directional_cost_edgeproj_v2_lcc_gate5_v1_sym_arithmetic` |  |  |
| `directional_cost_edgeproj_v2_lcc_v1` |  | old-mainline doc |
| `directional_cost_inversion_explainer_gate5_v1` |  |  |
| `directional_cost_mechanics_gate5_v1` |  |  |
| `directional_cost_square_v1` | yes |  |
| `edgeproj_pipeline_qa_gate5_v1` |  |  |
| `eq_uniqueness_gate5_ce_floor_b025_fl001_ks_v1` |  |  |
| `estimation_flat_check_v1` |  |  |
| `exog_endog_comparison_gate5_bpr_pos05_od2025_v2` |  |  |
| `exog_endog_comparison_gate5_bpr_pos05_v1` |  | old-mainline doc |
| `freeflow_robustness_gate5_ff0205_v1` | yes |  |
| `freeflow_robustness_gate5_quick_v1` | yes |  |
| `freeflow_speed_definitions_v1` | yes |  |
| `freeflow_speed_definitions_v2` | yes |  |
| `full_estimation_edgeproj_v2_lcc_gate5_v1` |  |  |
| `full_estimation_edgeproj_v2_lcc_v1` |  | old-mainline doc |
| `full_estimation_square_v1` |  | deck / paper / replication |
| `fundamental_maps_gate5_bpr_pos05_od2025_v2` |  |  |
| `fundamental_maps_gate5_bpr_pos05_v1` |  | old-mainline doc |
| `gate5_lanefix_rerun_comparison_v1` |  |  |
| `lambda_strict_match_v1` |  |  |
| `link_welfare_elasticity_v1` |  |  |
| `model_input_edgeproj_v2_lcc_gate5_am_even_days_v1` |  |  |
| `model_input_edgeproj_v2_lcc_gate5_am_odd_days_v1` |  |  |
| `model_input_edgeproj_v2_lcc_gate5_car_ratio_v1` |  |  |
| `model_input_edgeproj_v2_lcc_gate5_ff_0005_all_harmonic_v1` |  |  |
| `model_input_edgeproj_v2_lcc_gate5_ff_0104_all_harmonic_v1` |  |  |
| `model_input_edgeproj_v2_lcc_gate5_ff_0205_all_harmonic_v1` |  |  |
| `model_input_edgeproj_v2_lcc_gate5_ff_0205_all_p90_v1` |  |  |
| `model_input_edgeproj_v2_lcc_gate5_ff_2205_all_p90_v1` |  |  |
| `model_input_edgeproj_v2_lcc_gate5_ff_2205_weekday_harmonic_v1` |  |  |
| `model_input_edgeproj_v2_lcc_gate5_ff_2205_weekend_harmonic_v1` |  |  |
| `model_input_edgeproj_v2_lcc_gate5_symmetric_arithmetic_v1` |  |  |
| `model_input_edgeproj_v2_lcc_gate5_symmetric_geometric_v1` |  |  |
| `model_input_edgeproj_v2_lcc_gate5_v1` |  | old-mainline doc, deck / paper / replication |
| `model_input_edgeproj_v2_lcc_v1` |  | old-mainline doc, deck / paper / replication |
| `model_input_raw_connected_v3_edgeproj_v2_lcc_gate5_lanefix_v1` |  |  |
| `model_input_raw_connected_v3_edgeproj_v2_lcc_gate5_v1` |  |  |
| `model_input_raw_connected_v3_edgeproj_v2_lcc_v1` |  |  |
| `od_validation_aa_style_gate5_bpr_pos05_v1` | yes | deck / paper / replication |
| `phase_a_st_metrics_gate5_bpr_pos05_od2025_v2` |  |  |
| `phase_a_st_metrics_gate5_bpr_pos05_v1` |  | old-mainline doc |
| `phase_a_st_metrics_gate5_lanefix_neg_v1` |  |  |
| `phase_a_st_metrics_gate5_lanefix_pos05_v1` |  |  |
| `phase_a_st_metrics_gate5_v1` |  |  |
| `phase_b_sorting_patterns_gate5_bpr_pos05_od2025_v2` |  |  |
| `phase_b_sorting_patterns_gate5_bpr_pos05_v1` |  | old-mainline doc |
| `phase_b_sorting_patterns_gate5_v1` |  |  |
| `phase_c_composite_cf_gate5_v1` |  |  |
| `policy_comparison_gate5_bpr_pos05_auditfix_v1` |  | old-mainline doc, deck / paper / replication |
| `policy_comparison_gate5_bpr_pos05_od2025_v2` |  |  |
| `policy_comparison_gate5_bpr_pos05_v1` |  | old-mainline doc, deck / paper / replication |
| `preslide_pattern_figures_v1` |  |  |
| `raw_connected_v1_edgeproj_v2_square_AM_am_even_days_v1` |  |  |
| `raw_connected_v1_edgeproj_v2_square_AM_am_odd_days_v1` |  |  |
| `raw_connected_v1_edgeproj_v2_square_AM_ff_0005_all_harmonic_v1` |  |  |
| `raw_connected_v1_edgeproj_v2_square_AM_ff_0104_all_harmonic_v1` |  |  |
| `raw_connected_v1_edgeproj_v2_square_AM_ff_0205_all_harmonic_v1` |  |  |
| `raw_connected_v1_edgeproj_v2_square_AM_ff_0205_all_p90_v1` |  |  |
| `raw_connected_v1_edgeproj_v2_square_AM_ff_2205_all_p90_v1` |  |  |
| `raw_connected_v1_edgeproj_v2_square_AM_ff_2205_weekday_harmonic_v1` |  |  |
| `raw_connected_v1_edgeproj_v2_square_AM_ff_2205_weekend_harmonic_v1` |  |  |
| `raw_connected_v1_grid_travel_time_adjacent_v1` |  |  |
| `raw_connected_v1_grid_travel_time_conservative_v1` |  |  |
| `raw_connected_v1_grid_travel_time_edgeproj_v1` |  |  |
| `raw_connected_v1_grid_travel_time_edgeproj_v2` |  | deck / paper / replication |
| `raw_connected_v1_grid_travel_time_edgeproj_v2_traffic` |  |  |
| `raw_connected_v1_grid_travel_time_relaxed_v2` |  |  |
| `raw_connected_v1_grid_travel_time_relaxed_v3` |  |  |
| `raw_connected_v1_grid_travel_time_relaxed_v4` |  |  |
| `raw_connected_v1_grid_travel_time_v1` |  |  |
| `raw_connected_v2_grid_travel_time_edgeproj_v1` |  | deck / paper / replication |
| `raw_connected_v3_grid_travel_time_edgeproj_v2` |  |  |
| `raw_connected_v3_grid_travel_time_edgeproj_v2_lanefix` |  |  |
| `route_kernel_exp_trial_20260928_nu00122_v2` | yes |  |
| `route_kernel_exp_trial_20260928_nu00420_v2` | yes |  |
| `route_location_split_trial_v1` |  |  |
| `selection_split_day_check_gate5_v1` | yes |  |
| `stage2_endogenous_edgeproj_v2_lcc_gate5_anchored_lanefix_neg_v1` |  |  |
| `stage2_endogenous_edgeproj_v2_lcc_gate5_anchored_lanefix_pos05_v1` |  |  |
| `stage2_endogenous_edgeproj_v2_lcc_gate5_anchored_v1` |  | old-mainline doc, deck / paper / replication |
| `stage2_endogenous_edgeproj_v2_lcc_gate5_bpr_pos05_od2025_v1` |  |  |
| `stage2_endogenous_edgeproj_v2_lcc_gate5_bpr_pos05_od2025_v2` |  |  |
| `stage2_endogenous_edgeproj_v2_lcc_gate5_bpr_pos05_v1` |  | old-mainline doc, deck / paper / replication |
| `stage2_endogenous_edgeproj_v2_lcc_gate5_car_ratio_bpr_pos05_v1` |  |  |
| `stage2_endogenous_edgeproj_v2_lcc_gate5_v1` |  | old-mainline doc, deck / paper / replication |
| `stage2_gate5_freeflow_baseline_2205_all_harmonic_quick_v1` |  |  |
| `stage2_gate5_freeflow_ff_0005_all_harmonic_quick_v1` |  |  |
| `stage2_gate5_freeflow_ff_0104_all_harmonic_quick_v1` |  |  |
| `stage2_gate5_freeflow_ff_0205_all_harmonic_quick_v1` |  |  |
| `stage2_gate5_freeflow_ff_0205_all_p90_quick_v1` |  |  |
| `stage2_gate5_freeflow_ff_2205_all_p90_quick_v1` |  |  |
| `stage2_gate5_freeflow_ff_2205_weekday_harmonic_quick_v1` |  |  |
| `stage2_gate5_freeflow_ff_2205_weekend_harmonic_quick_v1` |  |  |
| `symmetric_input_mean_method_comparison_20260716_v1` |  |  |
| `symmetric_input_metro_candidates_20260716_v1` | yes |  |
| `symmetric_input_place_cost_suite_gate5_bpr_pos05_v1` |  |  |
| `symmetric_input_policy_grid_gate5_bpr_pos05_v1` |  |  |
| `symmetric_input_reestimated_gate5_bpr_pos05_arithmetic_od2025_v2` |  |  |
| `symmetric_input_reestimated_gate5_bpr_pos05_arithmetic_v1` |  |  |
| `symmetric_input_reestimated_gate5_bpr_pos05_od2025_v1` |  |  |
| `symmetric_input_reestimated_gate5_bpr_pos05_od2025_v2` |  |  |
| `symmetric_input_reestimated_gate5_bpr_pos05_v1` |  |  |
| `symmetry_directionality_suite_gate5_bpr_pos05_check` |  |  |
| `symmetry_directionality_suite_gate5_bpr_pos05_od2025_v2` |  |  |
| `symmetry_directionality_suite_gate5_bpr_pos05_v1` |  | deck / paper / replication |
| `theta_sensitivity_gate5_bpr_pos05_v1` |  |  |
| `tidal_bpr_st_vs_asym_gate5_lanefix_alpha_pos05_v1` |  |  |
| `tidal_bpr_st_vs_asym_gate5_lanefix_v1` |  |  |
| `tidal_corridor_candidates_gate5_v1` |  |  |
| `tidal_lane_diagnostics_edgeproj_v2_lcc_gate5_bpr_pos05_v1` |  | old-mainline doc, deck / paper / replication |
| `tidal_lane_diagnostics_edgeproj_v2_lcc_gate5_lanefix_neg_v1` |  |  |
| `tidal_lane_diagnostics_edgeproj_v2_lcc_gate5_lanefix_pos05_v1` |  |  |
| `tidal_lane_diagnostics_edgeproj_v2_lcc_gate5_p1_calib_v1` |  | old-mainline doc |
| `tidal_lane_diagnostics_edgeproj_v2_lcc_gate5_v1` |  | old-mainline doc |
| `tidal_lane_diagnostics_gate5_bpr_pos05_physical_time_lam020_od2025_v2` |  |  |
| `tidal_lane_diagnostics_gate5_bpr_pos05_physical_time_od2025_v1` |  |  |
| `tidal_lane_diagnostics_gate5_bpr_pos05_physical_time_od2025_v2` |  |  |
| `tidal_lane_diagnostics_gate5_bpr_pos05_physical_time_v1` |  |  |
| `tidal_lane_diagnostics_gate5_bpr_pos05_physical_time_v2` |  | old-mainline doc, deck / paper / replication |
| `tidal_lane_diagnostics_osm1a_gate5_bpr_pos05_physical_time_od2025_v2` |  |  |
| `tidal_lane_diagnostics_osm1b_L300_gate5_bpr_pos05_physical_time_od2025_v1` |  |  |
| `tidal_lane_diagnostics_osm1b_L300_gate5_bpr_pos05_physical_time_od2025_v2` |  |  |
| `tidal_lt_selection_compare_v1` |  |  |
| `tidal_mainline_bpr_pos05_v1` |  | old-mainline doc |
| `tidal_road_level_cross_sel4_in_nb8_bpr_v5` |  |  |
| `tidal_road_level_cross_sel4_in_nb8_ce_floor_v5` |  |  |
| `tidal_road_level_cross_sel8_in_nb4_bpr_v5` |  |  |
| `tidal_road_level_cross_sel8_in_nb4_ce_floor_v5` |  |  |
| `travel_cost_decision_v1` |  |  |
| `welfare_corridor_coupling_gate5_v1` |  |  |
| `welfare_sorting_diagnostics_gate5_bpr_pos05_od2025_v1` |  |  |
| `welfare_sorting_diagnostics_gate5_bpr_pos05_od2025_v2` |  |  |
| `welfare_sorting_diagnostics_gate5_bpr_pos05_v1` |  | old-mainline doc |
| `welfare_sorting_diagnostics_gate5_v1` |  | old-mainline doc |

## Pipeline baselines and Baidu validation (23)

| Folder | README | Cited by |
|---|---|---|
| `centerline_baidu_validation_beijing_v1` |  |  |
| `centerline_crossgrid_gap_candidates_v1` | yes |  |
| `centerline_crossgrid_robustness_tiers_v1` | yes |  |
| `centerline_endpoint_bridge35_v1` | yes |  |
| `centerline_rawpath_topology_candidates_v1` | yes |  |
| `centerline_v3_compare_v1` |  |  |
| `centerline_v3_compare_v2` |  |  |
| `centerline_v3_input_v1` |  |  |
| `centerline_v3_rules_v11_v1` |  |  |
| `directional_cost_centerline_lcc_v1` |  |  |
| `grid_pair_baidu_validation_v1` |  |  |
| `grid_pair_baidu_validation_v2` |  |  |
| `manual_centerline_rules_v11` |  | deck / paper / replication |
| `model_input_centerline_lcc_v1` |  |  |
| `model_input_centerline_v11_routed_gate5_od2025_v1` |  |  |
| `model_input_centerline_v3_routed_gate5_od2025_v1` |  |  |
| `raw_baidu_validation_beijing_v1` |  |  |
| `raw_rebuild_validation` |  |  |
| `raw_rebuild_validation_gridcost_harmonic_v1` |  |  |
| `raw_topology_mvp` |  |  |
| `rn_baidu_match_citywide_v1` |  |  |
| `rn_baidu_match_layers_v1` |  |  |
| `rn_baidu_match_network_v1` |  |  |

## Shenzhen, Caitian, national evidence (21)

| Folder | README | Cited by |
|---|---|---|
| `national_center_periphery_commuting_v1` |  |  |
| `national_center_periphery_group_diagnostics_v1` |  |  |
| `national_jobs_housing_asymmetry_slides_v1` |  |  |
| `national_tidal_commuting_baidu2024_top100_congestion_v1` |  |  |
| `national_tidal_commuting_baidu2025_top100_congestion_v1` |  | deck / paper / replication |
| `national_tidal_commuting_planb_trial_v1` |  |  |
| `national_tidal_commuting_top100_congestion_2023_v1` |  |  |
| `national_tidal_commuting_top50cities_2021_v1` |  | deck / paper / replication |
| `shenzhen_caitian_baidu_analysis_v1` |  |  |
| `shenzhen_caitian_baidu_candidates_v1` |  |  |
| `shenzhen_caitian_video_baidu_segment_v1` |  |  |
| `shenzhen_gridlevel_asymmetry_evidence_v1` | yes |  |
| `shenzhen_gridlevel_pairtime_v1` | yes |  |
| `shenzhen_gridlevel_spatial_asymmetry_v1` | yes | deck / paper / replication |
| `shenzhen_gridlevel_stage07_commute_link_audit_v1` |  |  |
| `shenzhen_liuxian_gaode_initial_v1` |  |  |
| `shenzhen_liuxian_gaode_late_opening_shock_v1` |  |  |
| `shenzhen_multi_tidal_exploration_v1` |  |  |
| `shenzhen_multi_tidal_pooled_psm_did_v1` | yes |  |
| `shenzhen_multi_tidal_post_only_v1` |  |  |
| `shenzhen_multi_tidal_refinement_v1` |  |  |

## Smoke tests and named failures (14)

| Folder | README | Cited by |
|---|---|---|
| `centerline_crossgrid_robustness_tiers_v1_failed_20260804_01` |  |  |
| `centerline_rawpath_topology_candidates_v1_failed_20260804_01` |  |  |
| `congestion_almagro_spec_test_noboot` |  |  |
| `national_center_periphery_commuting_smoke_v1` |  |  |
| `national_center_periphery_group_diagnostics_smoke_v1` |  |  |
| `national_tidal_commuting_baidu2025_top1_city_smoke_v1` |  |  |
| `national_tidal_commuting_baidu2025_top1city_smoke_v1` |  |  |
| `national_tidal_commuting_top2cities_2021_test` |  |  |
| `raw_connected_v1_grid_travel_time_edgeproj_v1_smoke` |  |  |
| `raw_connected_v2_grid_travel_time_edgeproj_v1_smoke` |  |  |
| `route_kernel_exp_trial_20260928_v1_failed_20260928_01` |  |  |
| `shenzhen_smallgrid_v1_run1` |  |  |
| `shenzhen_test_line_v1_run1` |  |  |
| `symmetric_input_policy_grid_gate5_bpr_pos05_smoke_v1` |  |  |

## Failed runs (only run.log) (7)

| Folder | README | Cited by |
|---|---|---|
| `tidal_lane_diagnostics_osm1b_ce_floor_b025_fl001_ks_od2025_alldaytt0610_v1` |  |  |
| `tidal_lane_diagnostics_osm1b_ce_floor_b025_fl001_ks_od2025_alldaytt0709_v2` |  |  |
| `tidal_lane_diagnostics_osm1b_ce_floor_b025_fl001_ks_od2025_ctl0709_v1` |  |  |
| `tidal_lane_diagnostics_osm1b_ce_floor_b025_fl001_ks_od2025_dep0610tt0610_v1` |  |  |
| `tidal_lane_diagnostics_osm1b_ce_floor_b025_fl001_ks_od2025_dep0610tt0709_v1` |  |  |
| `tidal_lane_diagnostics_osm1b_ce_floor_b025_fl001_ks_od2025_onroad0610tt0610_v1` |  |  |
| `tidal_lane_diagnostics_osm1b_ce_floor_b025_fl001_ks_od2025_win0709tt0610_v1` |  |  |
