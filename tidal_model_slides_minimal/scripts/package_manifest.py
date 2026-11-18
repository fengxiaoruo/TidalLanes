"""Single source of truth for the minimal package contents."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FileSpec:
    package_path: str
    main_path: str
    kind: str
    origin: str


MINIMAL_FULL_ESTIMATION = [
    "__init__.py",
    "build_primitives.py",
    "parameters.py",
    "invert_fundamentals.py",
    "equilibrium_solve.py",
    "lambda_diagnostic.py",
    "stage2_counterfactuals.py",
    "counterfactual_levels.py",
    "phase_metrics_common.py",
    "tidal_lane_diagnostics.py",
    "run_stage2_gate5.py",
    "run_tidal_lane_diagnostics_gate5.py",
    "run_policy_comparison_gate5.py",
    "run_alpha_beta_sensitivity_bpr_gate5.py",
    "run_symmetry_directionality_suite_gate5.py",
    "run_alpha_robustness_quick_gate5.py",
]

MINIMAL_DIRECTIONAL_COST = [
    "__init__.py",
    "fixed_tau_model.py",
    "policy_counterfactuals.py",
    "tidal_lane_counterfactuals.py",
]

FIG_SPECS = [
    # Assets referenced by Documents/PreSlides/Tidal0715.tex (package deck since 2026-09-28).
    FileSpec("figs/speed_eg.png", "Documents/PreSlides/figs/speed_eg.png", "figure", "Baidu 2025 one-week speed example figure"),
    FileSpec("figs/cityBeijing_c.jpg", "Documents/PreSlides/figs/cityBeijing_c.jpg", "figure", "Opening photo, Beijing"),
    FileSpec("figs/cityWuhan_c.jpg", "Documents/PreSlides/figs/cityWuhan_c.jpg", "figure", "Opening photo, Wuhan"),
    FileSpec("figs/cityShenzhen2_c.jpg", "Documents/PreSlides/figs/cityShenzhen2_c.jpg", "figure", "Opening photo, Shenzhen"),
    FileSpec("figs/cityKualaLumpur_c.jpg", "Documents/PreSlides/figs/cityKualaLumpur_c.jpg", "figure", "Opening photo, Kuala Lumpur"),
    FileSpec("figs/pat_national_ratio_hist.png", "Documents/PreSlides/figs/pat_national_ratio_hist.png", "figure", "National top-100 city speed asymmetry summary"),
    FileSpec("figs/pat_congestion_am_scatter.png", "Documents/PreSlides/figs/pat_congestion_am_scatter.png", "figure", "National top-100 city congestion and asymmetry summary"),
    FileSpec("figs/pat_bj_centerline_map.png", "Documents/PreSlides/figs/pat_bj_centerline_map.png", "figure", "Beijing centerline directional speed validation"),
    FileSpec("figs/pat_bj_centerline_hist.png", "Documents/PreSlides/figs/pat_bj_centerline_hist.png", "figure", "Beijing centerline directional speed validation"),
    FileSpec("figs/pat_jobs_housing_top20_gap_lollipop.pdf", "Documents/PreSlides/figs/pat_jobs_housing_top20_gap_lollipop.pdf", "figure", "Top-20 city center-bound minus reverse commuting share"),
    FileSpec("figs/pat_jobs_housing_balance.png", "Documents/PreSlides/figs/pat_jobs_housing_balance.png", "figure", "Beijing jobs-residents balance map"),
    FileSpec("figs/link_time_construction_actual.pdf", "Documents/PreSlides/figs/link_time_construction_actual.pdf", "figure", "Adjacent-grid physical-minute construction example"),
    FileSpec("figs/app_edgeproj_adjacent_m_obs_slide.pdf", "Documents/PreSlides/figs/app_edgeproj_adjacent_m_obs_slide.pdf", "figure", "Observed directed grid-link minutes map"),
    FileSpec("figs/route_examples/route_example_01.png", "Documents/PreSlides/figs/route_examples/route_example_01.png", "figure", "Selected shortest directed road path example"),
    FileSpec("figs/fund_ubar.png", "Documents/PreSlides/figs/fund_ubar.png", "figure", "Inverted residential fundamental map"),
    FileSpec("figs/fund_abar.png", "Documents/PreSlides/figs/fund_abar.png", "figure", "Inverted workplace/productivity fundamental map"),
    FileSpec("figs/symmetry_fundamental_relative_change.pdf", "Documents/PreSlides/figs/symmetry_fundamental_relative_change.pdf", "figure", "Symmetric-minus-directed fundamental radial profile"),
    FileSpec("figs/map_treated_overlay.png", "Documents/PreSlides/figs/map_treated_overlay.png", "figure", "Top-100 tidal candidate overlay"),
    FileSpec("figs/sort_lR.png", "Documents/PreSlides/figs/sort_lR.png", "figure", "Long-run residential sorting map"),
    FileSpec("figs/sort_lF.png", "Documents/PreSlides/figs/sort_lF.png", "figure", "Long-run workplace sorting map"),
    FileSpec("figs/app_duncan_cross_city.png", "Documents/PreSlides/figs/app_duncan_cross_city.png", "figure", "Beijing jobs-housing Duncan benchmark appendix figure"),
    FileSpec("figs/map_link_welfare_elast.png", "Documents/PreSlides/figs/map_link_welfare_elast.png", "figure", "Per-link welfare elasticity map"),
    FileSpec("figs/caitian_0709_pre_tracking.jpg", "Documents/PreSlides/figs/caitian_0709_pre_tracking.jpg", "figure", "Caitian video pre-opening tracking frame"),
    FileSpec("figs/caitian_0709_late_post_tracking.jpg", "Documents/PreSlides/figs/caitian_0709_late_post_tracking.jpg", "figure", "Caitian video post-opening tracking frame"),
    FileSpec("figs/figure_od_validation_structural_central99.pdf", "data_work/outputs/od_validation_aa_style_gate5_bpr_pos05_v1/figures/figure_od_validation_structural_central99.pdf", "figure", "AA-style bilateral OD validation scatter (path rewritten to figs/ on sync)"),
    FileSpec("figs/TidalVedio/pic.jpg", "Documents/PreSlides/figs/TidalVedio/pic.jpg", "figure", "Embedded tidal-lane field video preview image"),
    FileSpec("tidal_advisor_revision_round2_20260703.pdf", "Documents/PreSlides/tidal_advisor_revision_round2_20260703.pdf", "figure", "Advisor-revision deck; page 23 is included as the tidal-lane schematic"),
    FileSpec("A6_XiaoruoFeng_DirectionalCongestion_video.mov", "Documents/PreSlides/A6_XiaoruoFeng_DirectionalCongestion_video.mov", "video", "Run-linked field video on the motivation slide"),
]

OUTPUT_SPECS = [
    FileSpec("outputs/model_input_edgeproj_v2_lcc_gate5_v1/data/qsm_input_nodes_square.parquet", "data_work/outputs/model_input_edgeproj_v2_lcc_gate5_v1/data/qsm_input_nodes_square.parquet", "aggregate data", "Baidu 2022 commuting flow marginals plus grid nodes, aggregated for model input"),
    FileSpec("outputs/model_input_edgeproj_v2_lcc_gate5_v1/data/qsm_input_edges_square.parquet", "data_work/outputs/model_input_edgeproj_v2_lcc_gate5_v1/data/qsm_input_edges_square.parquet", "aggregate data", "Baidu 2025 AM and night speed data aggregated to directed grid links"),
    FileSpec("outputs/model_input_edgeproj_v2_lcc_gate5_v1/data/qsm_input_od_square.parquet", "data_work/outputs/model_input_edgeproj_v2_lcc_gate5_v1/data/qsm_input_od_square.parquet", "aggregate data", "Baidu 2022 commuting flow data aggregated to model OD grid pairs"),
    FileSpec("outputs/model_input_edgeproj_v2_lcc_gate5_v1/data/data_quality_square_qsm.csv", "data_work/outputs/model_input_edgeproj_v2_lcc_gate5_v1/data/data_quality_square_qsm.csv", "aggregate data QA", "Model input data-quality summary"),
    FileSpec("outputs/model_input_edgeproj_v2_lcc_gate5_v1/config_snapshot.travel_cost_bundle.json", "data_work/outputs/model_input_edgeproj_v2_lcc_gate5_v1/config_snapshot.travel_cost_bundle.json", "config", "Travel-cost bundle config snapshot"),
    FileSpec("outputs/model_input_edgeproj_v2_lcc_gate5_v1/travel_cost_bundle_summary.json", "data_work/outputs/model_input_edgeproj_v2_lcc_gate5_v1/metrics/travel_cost_bundle_summary.json", "metadata", "Travel-cost bundle summary"),
    FileSpec("outputs/model_input_edgeproj_v2_lcc_gate5_v1/model_input_qa.json", "data_work/outputs/model_input_edgeproj_v2_lcc_gate5_v1/qa/model_input_qa.json", "metadata", "Model input QA summary"),
    # 2026-09-26: reference tables moved to the physical-time runs. The June
    # directories report the bilateral iceberg index tau_ij as if it were
    # minutes; the physical_time runs use additive link minutes (Xi.m/Lbar).
    FileSpec("outputs/reference_results/tidal_st_lt_compact.csv", "data_work/outputs/tidal_lane_diagnostics_gate5_bpr_pos05_physical_time_v2/tidal_st_lt_compact.csv", "reference result", "Main tidal ST/LT decomposition (physical minutes)"),
    FileSpec("outputs/reference_results/tidal_st_lt_congestion_summary.csv", "data_work/outputs/tidal_lane_diagnostics_gate5_bpr_pos05_physical_time_v2/tidal_st_lt_congestion_summary.csv", "reference result", "Main tidal congestion summary (physical minutes)"),
    FileSpec("outputs/reference_results/alpha_beta_sensitivity_grid.csv", "data_work/outputs/alpha_beta_sensitivity_bpr_pos05_physical_time_v1/sensitivity_grid.csv", "reference result", "Alpha-beta sensitivity grid (physical minutes)"),
    FileSpec("outputs/reference_results/alpha_beta_sensitivity_grid_converged.csv", "data_work/outputs/alpha_beta_sensitivity_bpr_pos05_physical_time_v1/sensitivity_grid_converged.csv", "reference result", "Converged alpha-beta sensitivity grid (physical minutes)"),
    FileSpec("outputs/reference_results/structural_cf_exogenous.csv", "data_work/outputs/stage2_endogenous_edgeproj_v2_lcc_gate5_bpr_pos05_v1/counterfactuals/structural_cf_exogenous.csv", "reference result", "Exogenous structural counterfactuals"),
    FileSpec("outputs/reference_results/structural_cf_endogenous.csv", "data_work/outputs/stage2_endogenous_edgeproj_v2_lcc_gate5_bpr_pos05_v1/counterfactuals/structural_cf_endogenous.csv", "reference result", "Endogenous structural counterfactuals"),
    FileSpec("outputs/reference_results/tidal_lane_exogenous_bpr_grid.csv", "data_work/outputs/stage2_endogenous_edgeproj_v2_lcc_gate5_bpr_pos05_v1/counterfactuals/tidal_lane_exogenous_bpr_grid.csv", "reference result", "Fixed-Xi tidal BPR grid"),
    FileSpec("outputs/reference_results/policy_cf_all_modes.csv", "data_work/outputs/stage2_endogenous_edgeproj_v2_lcc_gate5_bpr_pos05_v1/counterfactuals/policy_cf_all_modes.csv", "reference result", "Structural policy counterfactuals"),
    FileSpec("outputs/reference_results/policy_results_by_policy.csv", "data_work/outputs/policy_comparison_gate5_bpr_pos05_auditfix_v1/results_by_policy.csv", "reference result", "Policy comparison table"),
    FileSpec("outputs/reference_results/policy_cost_effectiveness.csv", "data_work/outputs/policy_comparison_gate5_bpr_pos05_auditfix_v1/cost_effectiveness.csv", "reference result", "Policy cost-effectiveness and lane-km table"),
    FileSpec("outputs/reference_results/policy_policies_manifest.json", "data_work/outputs/policy_comparison_gate5_bpr_pos05_auditfix_v1/policies_manifest.json", "reference result", "Policy comparison manifest"),
    FileSpec("outputs/reference_results/symmetry_directionality_summary.csv", "data_work/outputs/symmetry_directionality_suite_gate5_bpr_pos05_v1/summary_by_scenario.csv", "reference result", "Symmetry-to-directionality diagnostic suite"),
    FileSpec("outputs/reference_results/symmetry_directionality_meta.json", "data_work/outputs/symmetry_directionality_suite_gate5_bpr_pos05_v1/suite_meta.json", "reference result", "Symmetry-to-directionality suite manifest"),
    FileSpec("outputs/reference_results/beijing_grid_population_clusters_square.csv", "data_work/outputs/beijing_commuting_direction_evidence_v1/data/grid_population_clusters_square.csv", "reference result", "Beijing grid jobs-residents aggregate used for jobs-housing separation metrics"),
    FileSpec("outputs/reference_results/beijing_duncan_cross_city.csv", "data_work/outputs/beijing_commuting_direction_evidence_v1/metrics/duncan_cross_city.csv", "reference result", "Top-100 city township-level jobs-housing Duncan benchmark"),
    FileSpec("outputs/reference_results/beijing_inbound_sector_summary.csv", "data_work/outputs/beijing_commuting_direction_evidence_v1/metrics/inbound_sector_summary.csv", "reference result", "Beijing radial inbound-reverse AM sector summary"),
    FileSpec("outputs/reference_results/beijing_jobs_housing_mismatch_metrics.json", "data_work/outputs/beijing_commuting_direction_evidence_v1/metrics/jobs_housing_mismatch_metrics.json", "reference result", "Beijing jobs-housing separation and radial direction metrics"),
    FileSpec("outputs/reference_results/national_pair_summary_baidu2025.csv", "data_work/outputs/national_tidal_commuting_baidu2025_top100_congestion_v1/metrics/triple_crawl_20260514_15/pair_summary.csv", "reference result", "National top-100 city pair-level AM/PM directional asymmetry summary"),
    FileSpec("outputs/reference_results/national_city_summary_baidu2025.csv", "data_work/outputs/national_tidal_commuting_baidu2025_top100_congestion_v1/metrics/triple_crawl_20260514_15/city_summary.csv", "reference result", "National top-100 city weighted directional asymmetry summary"),
    FileSpec("outputs/reference_results/national_congestion_asymmetry_correlation_baidu2025.csv", "data_work/outputs/national_tidal_commuting_baidu2025_top100_congestion_v1/metrics/triple_crawl_20260514_15/congestion_asymmetry_correlation.csv", "reference result", "National Baidu commute-peak congestion index and AM asymmetry correlation"),
    FileSpec("outputs/reference_results/shenzhen_flow_weighted_asymmetry_summary.csv", "data_work/outputs/shenzhen_gridlevel_spatial_asymmetry_v1/metrics/flow_weighted_asymmetry_summary.csv", "reference result", "Shenzhen grid-level flow-weighted directional asymmetry summary"),
    FileSpec("outputs/reference_results/beijing_centerline_asymmetry_table.csv", "data_work/outputs/manual_centerline_rules_v11/data/centerline_asymmetry_table.csv", "reference result", "Beijing AM road-centerline directional speed asymmetry table"),
]

SLIDE_SPECS = [
    # 2026-09-28: the package deck is the current seminar deck Tidal0715 (compiled as Tidal0718.pdf).
    FileSpec("tidal.tex", "Documents/PreSlides/Tidal0715.tex", "slide source", "Current seminar deck source (Tidal0715), copied as tidal.tex"),
    FileSpec("references.bib", "Documents/PreSlides/references.bib", "bibliography", "BibTeX database for the slide citations"),
    FileSpec("tidal.pdf", "Documents/PreSlides/Tidal0718.pdf", "compiled slide deck", "Compiled current seminar deck (Tidal0718.pdf), copied as tidal.pdf"),
    FileSpec(
        "tidal_speaker_script.md",
        "Documents/PreSlides/tidal_speaker_script_Tidal0715_20260718.md",
        "speaker script",
        "Slide-by-slide speaker script aligned with the Tidal0715 deck",
    ),
]

LOCAL_SPECS = [
    FileSpec("MAIN_REPO", "", "package workflow", "Relative path from this package to the main TidalLanes repo"),
    FileSpec(".gitignore", "", "package workflow", "Package-local workflow file"),
    FileSpec("Makefile", "", "package workflow", "Package-local workflow file"),
    FileSpec("README.md", "", "package workflow", "Package-local documentation"),
    FileSpec("requirements.txt", "", "package workflow", "Package-local dependency note"),
    FileSpec("scripts/check_package.py", "", "package workflow", "Package-local validation script"),
    FileSpec("scripts/package_manifest.py", "", "package workflow", "Package manifest and sync specs"),
    FileSpec("scripts/slide_metric_checks.py", "", "package workflow", "Package-local validation script"),
    FileSpec("scripts/smoke_model.py", "", "package workflow", "Package-local validation script"),
    FileSpec("scripts/repo_paths.py", "", "package workflow", "Main-repo relative path resolver"),
    FileSpec("scripts/sync_from_main.py", "", "package workflow", "Package-local sync script"),
    FileSpec("scripts/push_to_main.py", "", "package workflow", "Package-local push-back script"),
    FileSpec("scripts/validate_full_model.py", "", "package workflow", "Package-local full-validation script"),
    FileSpec("docs/MANIFEST.md", "", "package workflow", "Minimal package scope note"),
    FileSpec("docs/SOURCE_NOTES.md", "", "package workflow", "Package-local provenance note"),
    FileSpec("docs/SYNC_WORKFLOW.md", "", "package workflow", "Package-local sync note"),
    FileSpec("docs/VALIDATION_20260702.md", "", "package workflow", "Package-local validation record"),
    FileSpec("docs/VALIDATION_20260926.md", "", "package workflow", "Package-local validation record (physical-time reference update)"),
    FileSpec("src/model/full_estimation/ESTIMATION_NOTES.md", "", "package workflow", "Package-local estimation notes with 2026-09-26 status update"),
    FileSpec("src/__init__.py", "", "package workflow", "Package-local Python package marker"),
]


def code_specs(main) -> list[FileSpec]:
    from pathlib import Path

    main = Path(main)
    specs: list[FileSpec] = []
    for name in MINIMAL_FULL_ESTIMATION:
        rel = f"data_work/src/model/full_estimation/{name}"
        specs.append(FileSpec(f"src/model/full_estimation/{name}", rel, "model code", "Minimal full-estimation module"))
    for name in MINIMAL_DIRECTIONAL_COST:
        rel = f"data_work/src/model/directional_cost/{name}"
        specs.append(FileSpec(f"src/model/directional_cost/{name}", rel, "model code", "Minimal directional-cost module"))
    specs.append(FileSpec("src/model/__init__.py", "data_work/src/model/__init__.py", "model code", "Python package marker"))
    return specs


def all_specs(main) -> list[FileSpec]:
    return SLIDE_SPECS + FIG_SPECS + OUTPUT_SPECS + code_specs(main) + LOCAL_SPECS


def managed_package_paths(main) -> set[str]:
    return {spec.package_path for spec in all_specs(main) if spec.package_path}
