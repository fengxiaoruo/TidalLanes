# Document Index

Single index for every prose document in the project. `data_work/docs/` holds pipeline and evidence documentation; `Documents/L2_notes/` holds research memos and handoffs; `Documents/L1_manuscripts/` and `Documents/PreSlides/` hold writing output. This file is the only index; the per-directory READMEs point here rather than maintaining separate lists.

Status labels used below:

- **authoritative** means the document defines a current decision or specification. Where two documents disagree, this one wins.
- **active** means the content still holds and is in use, without being the locked reference for anything.
- **historical** means the work is finished and the record is kept for provenance. Numbers inside may have been superseded; check against the authoritative document before quoting them.

---

## Start here

| File | Purpose |
|---|---|
| `agent_quickstart.md` | Minimum-token onboarding path for a new session |
| `nextstep.md` | Live plan: current status, next actions, open questions |
| `data_work/docs/WORK_LOG.md` | Completed work, locked parameters, retained decisions, from 2026-10 onward, newest first. Earlier entries are in `WORK_LOG_2026Q3.md` (July to September) and `WORK_LOG_2026Q2.md` (April to June) |
| `AGENTS.md` | Cross-task rules, repository layout, writing-rule mandate |
| `writing.md` | Style rules governing all English prose in the project |

---

## Current mainline (single authority)

This table is the one place that states the current model specification, the command that produces it, and what the numbers may be quoted for. Other documents link here instead of restating it. When the mainline changes, update this table first, then `nextstep.md` and [`data_work/docs/MAINLINE_MANIFEST.md`](data_work/docs/MAINLINE_MANIFEST.md).

Status as of 2026-11-18.

| Item | Current value |
|---|---|
| Commuting flows | 2025-05 platform home-to-work OD, all day |
| Travel times | Link times 07:00 to 09:00 |
| Network and grid | OSM scheme 1B (`osm1b_L300`); 3 km square grid, 4-neighbour; policies defined on grid edges |
| Congestion function | `ce_floor` (constant elasticity with a free-flow floor), β_c = 0.35, floor 1% of flow. Estimates without the floor give 0.29 to 0.44. Changed from 0.25 on 2026-11-13 |
| Other parameters | κ by the spectral-radius rule (1.483 on 1B). θ = 6.83 and β = −0.10 are the Allen-Arkolakis (2022) Seattle values; α = 0.05 is a literature mid-range. None is estimated on Beijing data; the runs read them from the parameter folder below |
| Input bundle | `data_work/outputs/model_input_osm1b_L300_gate5_od2025_v2_alldaytt0709` |
| Parameter folder | `data_work/outputs/stage2_endogenous_osm1b_ce_floor_b025_fl001_ks_od2025_v1`. The runs take only α, β, θ from its `parameters.json`; inversion and equilibria are redone on the input bundle above. Its own run used `model_input_osm1b_L300_gate5_od2025_v2` (07:00 to 09:00 departure OD). The `travel_cost` field of `mainline_config.json` in this and every stage 2 folder made before 2026-11-18 is a fixed label ("raw_connected_v1 edgeproj_v2 gate5") and does not describe the input; use `version_id` in `baseline_summary.json` instead. Likewise `lambda_phys = 1.1` in any `parameters.json` written before 2026-11-18 is a placeholder label that no solver reads; the elasticity used is the `--congestion-lambda` of the run command |
| Headline result | Tidal lanes on the top 100 corridors: commuter welfare +0.627% short run, +0.397% long run; 29.9 seconds saved per commuter in the short run, 15.4 seconds added in the long run |
| Policy comparison | Long-run welfare of tidal lanes is 0.96 of same-corridor expansion. Ranking P6 > P7 ≈ P3 ≈ P2 > P1 > P5 > P4; P7 leads the expansions by only 0.004 to 0.010 points |
| What the numbers measure | Morning-peak commuter welfare, an upper bound on gains to all road users. In the model, the light direction of a tidal corridor carries almost no traffic. With fixed background traffic added, the all-vehicle short-run time saving of tidal lanes is 0.49 of same-corridor expansion (0.25 to 0.86 across versions). Quote "96% of expansion" only together with "commuter welfare" |
| Result folders | `tidal_lane_diagnostics_osm1b_ce_floor_b035_fl001_ks_od2025_alldaytt0709_v1` and `policy_comparison_osm1b_ce_floor_b035_fl001_ks_od2025_alldaytt0709_v1`; the full list to back up is in `MAINLINE_MANIFEST.md` |
| Run commands | `data_work/outputs/beta_c_rerun_b035_summary_v1/README.md`, section 文件与命令 |
| Code entry points | `data_work/src/model/full_estimation/run_tidal_lane_diagnostics_gate5.py` and `run_policy_comparison_alldaytt0709.py`. Despite `gate5` in the name, both run 1B when given the flags in the commands. Script defaults are still the old mainline (anchored, λ = 1.1), so every flag must be passed |
| Open choices | Background traffic, OD distance decay η, the headline metric, and the role of BPR; see `nextstep.md` |

### Materials and the results they use

| Material | Results it uses | Use |
|---|---|---|
| `Documents/PreSlides/brad_202611/` | Current mainline, β_c = 0.35 | November 2026 discussion with Brad Ross |
| `Documents/L2_notes/results_update_and_specifications_20261110.md` | 1B with β_c = 0.25 and 07:00 to 09:00 OD | Reasons for each specification choice; its numbers are superseded |
| `Documents/PreSlides/Tidal0715.tex` (`Tidal0718.pdf`) | Old gate5 mainline: 2022 OD, anchored λ = 1.1, +0.87% / +0.32% | July seminar deck; not current numbers |
| `Documents/L1_manuscripts/TidalLanes_0429.tex` | Old mainline, April | Stalled paper draft |
| `tidal_model_slides_minimal/`, `tidal_model_slides_minimal_r/` | Old gate5 inputs, mapped to `Tidal0715` | Coauthor replication package for the July deck |

---

## Where the current numbers live

Results have been revised several times, and older memos still contain the superseded values. The rule is that a memo dated before a revision keeps its original numbers; it is not retroactively corrected. Consult this table before quoting any figure.

The mainline changed between 2026-10-29 and 2026-11-11. The current specification uses the 2025 platform OD, the OSM 1B network, and a constant-elasticity congestion function with a free-flow floor. The first block below is the current mainline. The second block describes the earlier gate5 mainline (2022 OD, anchored form, lambda of 1.1), which the paper and the deck still quote.

| Question (current mainline) | Authoritative source |
|---|---|
| Specification, headline results, run commands, and what the numbers measure | The "Current mainline" table above. `Documents/L2_notes/results_update_and_specifications_20261110.md` gives the reasons for each choice but still quotes β_c = 0.25 |
| Network choice and congestion function put to coauthors | `Documents/L2_notes/coauthor_decisions_network_congestion_20261106.md` |
| Congestion elasticity: estimates, range, and why 1.1 was dropped | `Documents/L2_notes/results_update_and_specifications_20261110.md`, Section 4 |
| Discussion deck and page-by-page script for the November 2026 meeting with Brad Ross | `Documents/PreSlides/brad_202611/` (`slides_brad_202611.pdf`, `speaker_notes.md`); background and literature in `Documents/L2_notes/brad_meeting_prep_20261113.md` |
| Model flows by direction on tidal corridors (light direction nearly empty), and the load-dependent elasticity trial | `data_work/outputs/load_dependent_elasticity_trial_v1/README.md`; `data_work/docs/WORK_LOG.md` (2026-11-15) |
| Background traffic in the congestion function: commuter welfare against all-vehicle time, and tidal lanes against expansion | `data_work/outputs/background_traffic_trial_v1/README.md`; `data_work/docs/WORK_LOG.md` (2026-11-15) |
| Flow-density curve by road class and what the data cannot identify | `data_work/outputs/flow_density_per_lane_v1/README.md` |
| Main elasticity changed from 0.25 to 0.35 on 2026-11-13: evidence and rerun | `data_work/outputs/congestion_floor_sensitivity_v1/README.md` and `data_work/outputs/beta_c_rerun_b035_summary_v1/README.md`; decision recorded in `data_work/docs/WORK_LOG.md` (2026-11-13). Memos dated before this still quote 0.25 and its results. |
| BPR as a comparison form and the derivations it still needs | `Documents/L2_notes/bpr_congestion_version_20261108.md` |
| Grid topology and road-level policy definition | `Documents/L2_notes/grid_topology_and_road_level_policy_20261110.md` |
| Which data are observed and which are imputed | `Documents/L2_notes/observed_vs_imputed_data_use_20261106.md` |
| Platform extraction status | `Platform/runs/202505_v01/run-log.md` |

| Question (earlier gate5 mainline, historical) | Authoritative source |
|---|---|
| Which grid travel-cost bundle is the model input | `Documents/L2_notes/travel_cost_lock_in_20260628.md` |
| Stage 2 gate5 parameters and run commands | `data_work/docs/STAGE2_MAINLINE_GATE5.md` |
| Congestion functional form and the value of lambda | `Documents/L2_notes/congestion_lambda11_density_vs_anchored_20260629.md` |
| Welfare, commuting-time, person-hour, and corridor headline results | `Documents/L2_notes/model_and_results_summary_20260928.md` (re-issued 2026-09-28 in physical-time units; the 2026-06-30 version is archived in `_superseded/` and its commuting-time columns are wrong) |
| Model audit, open specification problems, referee view | `Documents/L2_notes/model_audit_and_referee_review_20260926.md` |
| The welfare gauge correction and its consequences | `Documents/L2_notes/estimation_audit_welfare_gauge_20260630.md` |
| Deck contents, page count, and framing of the July deck | `Documents/README.md` and the Manuscripts and slides section below |
| Parameter provenance against the literature | `data_work/docs/CONGESTION_ELASTICITY_PARAMETER_MAPPING.md` |

Five revisions in particular invalidate numbers in earlier memos. Commuting-time and person-minute figures dated before 2026-07-16 were computed from the bilateral cost index `tau_ij` as if it were minutes; the physical-time correction of that date (link-passage average of `m_kl`) changed the top-100 long-run figure from `+95` to `+21,135` person-minutes per day, and every memo, deck, or table quoting seconds per commuter from before that date is superseded. The welfare gauge was corrected on 2026-07-01, which changed the sign of the long-run welfare change; anything quoting a negative long-run welfare figure predates it. Corridor selection moved from the `asym` score to `score_bpr_st` around 2026-06-30, so the Phase A through D memos select a different edge set than the mainline does. The `alpha` used in Stage 1 differs from the mainline value by design, so the two are not interchangeable. Lane lookup in the edge-projection path was fixed on 2026-06-29, and results produced before that date were rebuilt.

---

## Platform extraction

| File | Status | Content |
|---|---|---|
| `Platform/202505_v01/01_geo_inputs/UPLOAD.md` | active | 原网格和合成fixture上传/校验 |
| `Platform/202505_v01/02_internal_builds/BUILD_CONTRACTS.md` | active | 独立person/trip/N/Q/RN构造规则 |
| `Platform/202505_v01/03_qc/QC_CONTRACTS.md` | active | 平台质量与闭合检查 |
| `Platform/202505_v01/04_aggregate_exports/EXPORT_SCHEMA.md` | active | 匿名聚合字段与单位 |
| `Platform/202505_v01/05_capacity/BUDGET.md` | active | pilot和容量/分片规则 |
| `Platform/202505_v01/PLAN.md` | active | 分阶段提取与模型接入计划 |
| `Platform/202505_v01/RUN_QUEUE.md` | active | 依赖解释，机器状态在queue.json |
| `Platform/AGENTS.md` | active | 平台执行和来源保护规则 |
| `Platform/README.md` | active | 工作区入口与目录 |
| `Platform/current-spec.md` | active | 2025提取统计对象与精确口径 |
| `Platform/docs/source-reuse.md` | active | 来源项目只读复用边界 |
| `Platform/runs/202505_v01/run-log.md` | active | 本批唯一真实执行记录 |

## 新增研究设计

平台数据当前待讨论问题：[`platform_network_congestion_questions_20261103.md`](Documents/L2_notes/platform_network_congestion_questions_20261103.md)，涵盖网络覆盖与补速度、segment λ、拥堵函数图和统一 R 人群的 trip 通勤占比。

观测与插补数据的使用规则：[`observed_vs_imputed_data_use_20261106.md`](Documents/L2_notes/observed_vs_imputed_data_use_20261106.md)，估计和路段层面分析只用观测数据，插补只用于模型路网与格对用时，并与只用观测的版本对照。

待合作者决定的路网、拥堵函数与λ：[`coauthor_decisions_network_congestion_20261106.md`](Documents/L2_notes/coauthor_decisions_network_congestion_20261106.md)，建议换OSM三级及以上路网、用畅通下限加常数弹性（边层面约0.2–0.25），并决定λ口径。

| File | Status | Content |
|---|---|---|
| `Documents/L2_notes/results_update_and_specifications_20261110.md` | active | 主口径各项选择的理由（数字为β_c = 0.25，已被上方Current mainline表取代）；、主结果、从旧主线到新主线每一步的变化、弹性说明与待做清单 |
| `Documents/L2_notes/progress_note_20261101_20261109.md` | active | 11-01至11-09的进展备忘 |
| `Documents/L2_notes/grid_topology_and_road_level_policy_20261110.md` | active | 4邻接、8邻接与按道路定义政策的对照 |
| `Documents/L2_notes/bpr_congestion_version_20261108.md` | active | BPR对照形式与待做的模型推导 |
| `Documents/L2_notes/platform_data_report_20261104.md` | active | 平台数据汇总报告：OD、路网车流、通勤占比、λ识别 |
| `Documents/L2_notes/_superseded/nextstep_snapshot_20261113.md` | historical | 2026-11-13精简前的完整nextstep，含6–9月各线状态与政策套件计划 |
| `Documents/L2_notes/unicom_2025_commute_input_assessment_20261029.md` | active | 核实2025职住与分时OD已下载；NG103实际时窗、5公里/3公里及方式差异；模型升级建议 |
| `Documents/L2_notes/flow_validation_and_network_extensions_20261029.md` | active | Speed联通与模型flow验证设计；福利分解、随机成本、link互补性与韧性扩展 |
| `Documents/L2_notes/unicom_model_flow_validation_results_20261029.md` | active | 已执行的跨期有向网格flow验证，方向性差异、覆盖与敏感性 |
| `data_work/docs/unicom_flow_validation_20261029/analysis-plan.md` | active | 固定比较规则和执行范围 |
| `data_work/docs/unicom_flow_validation_20261029/variable-dictionary.md` | active | 模型与联通计数、缺失和方向定义 |
| `data_work/docs/unicom_flow_validation_20261029/sample-lineage.md` | active | 输入、合并关系与样本损失 |
| `data_work/docs/unicom_flow_validation_20261029/geospatial-data-notes.md` | active | 网格边界、OSM筛查与物理道路限制 |
| `data_work/docs/unicom_flow_validation_20261029/regression-spec-matrix.md` | active | 描述性指标和不确定性定义，无因果回归 |
| `data_work/docs/unicom_flow_validation_20261029/table-shells.md` | active | 已填充表与图的来源 |
| `data_work/docs/unicom_flow_validation_20261029/replication-checklist.md` | active | 重建顺序、缓存边界和核验 |

## Model and estimation

| File | Status | Content |
|---|---|---|
| `data_work/docs/MODEL_AUDIT_LEARNING_GUIDE_20260628.md` | active | Full-picture audit of the model line and a reading path through the code |
| `data_work/docs/STAGE2_MAINLINE_GATE5.md` | historical | Stage 2 specification, parameters, and commands of the old gate5 mainline; reproduces `Tidal0715` and the April paper |
| `data_work/docs/DIRECTIONAL_COST_MODEL_20260628.md` | active | Stage 1 fixed-cost directional model, a parallel line to Stage 2 |
| `data_work/docs/CONGESTION_ELASTICITY_PARAMETER_MAPPING.md` | active | Maps the congestion elasticity to external estimates |
| `Documents/L2_notes/TidalLanes_estimation_notes_20260928.tex` | active | Formal estimation companion for the gate5 mainline (re-issued 2026-09-28; the 2026-06-30 version in `_superseded/` states the pre-gauge-fix welfare and is wrong on the long-run sign) |
| `Documents/L2_notes/parameter_estimation_data_roadmap_20260702.md` | active | Which data and method would identify each parameter |
| `Documents/L2_notes/model_and_results_summary_20260928.md` | historical | Specification numbers and output paths of the old gate5 mainline (physical-time units) |
| `Documents/L2_notes/model_audit_and_referee_review_20260926.md` | active | Audit of code, documents, and replication packages; referee-style assessment |
| `Documents/L2_notes/welfare_gauge_diagnostic_20260701.tex` | active | Why the Fréchet denominator is the correct welfare gauge |
| `Documents/L2_notes/estimation_audit_welfare_gauge_20260630.md` | authoritative | Verification of the gauge correction |

### Congestion specification

| File | Status | Content |
|---|---|---|
| `Documents/L2_notes/congestion_lambda11_density_vs_anchored_20260629.md` | historical | Compares legacy, density, and anchored forms; locked the anchored form for the old mainline (replaced by `ce_floor` in November) |
| `Documents/L2_notes/congestion_magnitude_and_anchored_form_20260626.md` | active | Why the level form misstates magnitude |
| `Documents/L2_notes/bpr_congestion_form_20260626.md` | active | BPR versus power law, and the implied exponent |
| `Documents/L2_notes/lambda_stability_findings_20260619.md` | historical | Stability experiments on the earlier solver |
| `Documents/L2_notes/additive_delay_and_route_elasticity_note_20260717.md` | active | Unimplemented additive-delay route, kept as a robustness plan |

### Symmetry versus directionality

| File | Status | Content |
|---|---|---|
| `Documents/L2_notes/symmetry_to_directional_model_experiments_20260701.md` | active | Experiment suite behind the second contribution |
| `Documents/L2_notes/symmetric_vs_directional_measurement_20260626.md` | active | Measurement-level argument for retaining direction |
| `Documents/L2_notes/exog_vs_endog_framing_20260630.md` | active | Exogenous against endogenous counterfactual framing |

### Stage handoffs and result phases

| File | Status | Content |
|---|---|---|
| `Documents/L2_notes/stage1_fixed_directed_cost_complete_handoff_20260628.md` | active | Stage 1 complete handoff |
| `Documents/L2_notes/stage1_od_fit_diagnosis_and_handoff_20260628.md` | active | Commuting-flow fit diagnosis and its scope limits |
| `Documents/L2_notes/stage2_endogenous_congestion_handoff_20260628.md` | historical | Superseded by the mainline summary and the gate5 specification |
| `Documents/L2_notes/phase_a_st_metrics_handoff_20260629.md` | historical | Short-run metrics under the earlier selection rule |
| `Documents/L2_notes/phase_b_sorting_patterns_handoff_20260629.md` | historical | Long-run sorting patterns |
| `Documents/L2_notes/phase_c_composite_cf_handoff_20260629.md` | historical | Composite counterfactual |
| `Documents/L2_notes/phase_d_results_narrative_20260629.md` | historical | Writing structure remains usable; figures are stale |
| `Documents/L2_notes/tidal_lane_st_lt_congestion_diagnostics_20260629.md` | active | Definitions of the short-run and long-run decomposition |
| `Documents/L2_notes/MAINLINE_WORKPLAN_handoff_20260630.md` | active | Executable phase plan for the mainline |
| `Documents/L2_notes/PROJECT_OUTLINE_20260721.md` | active | Broadest current overview, written for coauthor discussion |

---

## Data pipeline

| File | Status | Content |
|---|---|---|
| `data_work/AGENTS.md` | authoritative | Pipeline working rules and standard entry commands |
| `data_work/docs/HOW_TO_RERUN_PARTIAL_RESULTS.md` | active | Full guide to partial reruns and the stage dependency chain |
| `data_work/docs/QUICK_RERUN_CHEATSHEET.md` | active | Command lookup, a subset of the guide above |
| `data_work/docs/NEW_MACHINE_SETUP_CHECKLIST.md` | active | Environment setup and verification on a new machine |
| `data_work/docs/BACKUP_WORKFLOW.md` | authoritative | What is backed up, where, and how to restore |
| `data_work/docs/MAINLINE_MANIFEST.md` | authoritative | Output folders of the current mainline that must be backed up, with the input chain in dependency order |
| `data_work/docs/OUTPUTS_REGISTRY.md` | active | Every folder in `data_work/outputs/` sorted into eight categories (mainline, current-spec experiments, old gate5 line, failed runs, and others), with README presence and which documents cite it. Regenerated by `data_work/src/diagnostics/build_outputs_registry.py` |
| `Documents/L2_notes/travel_cost_lock_in_20260628.md` | historical | Locked the grid travel-cost bundle of the old gate5 mainline |
| `Documents/L2_notes/data_pipeline_versions_and_conventions_20260628.md` | historical | Version comparison table and naming conventions |
| `Documents/L2_notes/gate5_lanefix_rerun_plan_20260629.md` | historical | Plan for the lane-fix rebuild |
| `Documents/L2_notes/gate5_lanefix_rerun_summary_20260629.md` | historical | Results of that rebuild |
| `Documents/L2_notes/p1_edgeproj_qa_tidal_corridor_handoff_20260629.md` | active | Edge-projection quality assurance and corridor scoring |

### Baidu and Gaode data collection

| File | Status | Content |
|---|---|---|
| `data_work/docs/BAIDU_ROUTE_API_NOTES.md` | active | API products, parameters, and rate limits |
| `data_work/docs/NATIONAL_TIDAL_COMMUTING_WORKFLOW.md` | active | National hundred-city sample construction and crawl |
| `data_work/docs/NATIONAL_CRAWL_SCHEDULING.md` | active | Scheduled launch for the national crawl |
| `data_work/docs/CENTERLINE_BAIDU_VALIDATION.md` | active | Beijing centreline corridor validation |
| `data_work/docs/RAW_BAIDU_VALIDATION.md` | active | Beijing raw-segment validation, parallel to the above |
| `Documents/L2_notes/national_tidal_commuting_planB_20260426.md` | historical | Earlier sampling plan, replaced by the 2025 congestion ranking |

---

## Empirical evidence

### Shenzhen

The three grid-level documents form a sequence: the audit establishes the data, the unweighted evidence establishes the pattern, and the spatial version adds flow weighting and maps. Slides draw on the last.

| File | Status | Content |
|---|---|---|
| `data_work/docs/SHENZHEN_GRIDLEVEL_PAIRTIME_AUDIT_20260628.md` | active | Audit of the grid-pair travel-time matrix |
| `data_work/docs/SHENZHEN_GRIDLEVEL_ASYMMETRY_EVIDENCE_20260628.md` | active | Unweighted all-pair asymmetry evidence |
| `data_work/docs/SHENZHEN_GRIDLEVEL_SPATIAL_ASYMMETRY_20260629.md` | active | Flow-weighted and spatialised version used in slides |
| `Documents/L2_notes/shenzhen_gridlevel_flow_weighted_asymmetry_positioning_20260630.md` | authoritative | How this evidence may and may not be interpreted |
| `data_work/docs/SHENZHEN_TIDAL_OPERATION_SHOCKS_20260628.md` | active | Calendar of tidal-lane operating changes |
| `data_work/docs/SHENZHEN_MULTI_TIDAL_BAIDU_HANDOFF_20260628.md` | active | Segment-level post-only analysis across corridors |
| `Documents/L2_notes/shenzhen_liuxian_did_purchase_design_20260626.md` | active | Liuxian Avenue difference-in-differences design |
| `Documents/L2_notes/tau_cost_response_corridor_validation_idea_20260727.md` | active | Proposed cost-response corridor validation, not yet implemented |

### Caitian Road video

| File | Status | Content |
|---|---|---|
| `data_work/docs/CAITIAN_VIDEO_IDENTIFICATION_PLAN_20260715.md` | authoritative | Complete identification design for the video experiment |
| `Documents/L2_notes/fieldvideo_collection_plan_20260709.md` | active | Collection windows, controls, and equipment plan |

### Theory and literature

| File | Status | Content |
|---|---|---|
| `Documents/L2_notes/QSM_travel_cost_literature_review.md` | active | Where symmetric and asymmetric travel cost sit in the literature |
| `Documents/L2_notes/AA_2022_transport_cost_memo.md` | active | Iceberg link-cost definition and its measurement |
| `Documents/L2_notes/AA_commuting_replication_adaptation_notes_20260426.md` | historical | Adaptation route for the replication package |
| `Documents/L2_notes/MODEL_replication_tidallanes_PLAN.md` | historical | Writing plan for the replication document |
| `Documents/L2_notes/MODEL_replication_tidallanes_audit_20260429.md` | historical | Input-definition audit of that document |
| `Documents/L2_notes/02_Centerline_Match_Speed_Documentation.md` | historical | Centreline speed matching, retained for robustness only |

---

## Meetings, audits, and review

Meeting notes are kept per date rather than merged, because later meetings add to earlier ones instead of replacing them.

| File | Status | Content |
|---|---|---|
| `Documents/L2_notes/meeting_notes_20260727.md` | active | Coauthor meeting of 2026-07-27 |
| `Documents/L2_notes/brad_meeting_prep_20261113.md` | active | 下一次与 Brad Ross 讨论的准备：四个议题、我方发现、问题清单与待核实条目 |
| `Documents/L2_notes/meeting_notes_20261007_brad.md` | active | Brad Ross 会议：比较更多 congestion technology 设定与结果 |
| `Documents/L2_notes/meeting_notes_20260710.md` | active | Revision priorities that drove the July work |
| `Documents/L2_notes/coauthor_revision_attempts_20260710.md` | active | First-pass numerical response to those priorities |
| `Documents/L2_notes/meeting_discussion_memo_20260709.tex` | active | Options tabled on 2026-07-09 |
| `Documents/L2_notes/meeting_notes_20260502.md` | historical | Early framing meeting |
| `Documents/L2_notes/discussant_response_20260701.md` | active | Point-by-point response to the discussant |
| `Documents/L2_notes/discussant_comment_to_change_map_20260701.md` | active | Maps each comment to the code or slide that changed |
| `Documents/L2_notes/referee_review_model_code_20260629.md` | historical | Adversarial self-review, mostly addressed |
| `Documents/L2_notes/code_logic_audit_20260630.md` | historical | Code-level defect list from that review |
| `Documents/L2_notes/SESSION_HANDOFF_20260628.md` | historical | Session handoff from the pipeline-blocker period |

---

## Manuscripts and slides

The paper and the deck have diverged: the deck was rewritten through July while `TidalLanes_0429.tex` has not been touched since April. Deciding whether to write the paper forward from the deck is an open item.

| File | Status | Content |
|---|---|---|
| `Documents/PreSlides/Tidal0715.tex` | active | Main seminar deck, 42 pages, compiled as `Tidal0718.pdf` |
| `Documents/PreSlides/tidal_speaker_script_Tidal0715_20260718.md` | active | Speaker script for that deck |
| `Documents/PreSlides/tidal.tex` | active | Reduced deck synchronised into the replication package |
| `Documents/PreSlides/TidalLane_Video_RDiT_2slides_20260715.tex` | active | Two-page video evidence deck |
| `Documents/L1_manuscripts/TidalLanes_Model_v4.tex` | active | Full model description, aligned with the deck (welfare paragraph revised 2026-09-28; prior text in `drafts/TidalLanes_Model_v4_pre20260928.tex`) |
| `Documents/L1_manuscripts/FixedDirectedCost_Stage1_gate5.tex` | active | Stage 1 technical document |
| `Documents/L1_manuscripts/EndogenousCongestion_Stage2_gate5.tex` | active | Stage 2 technical document |
| `Documents/L1_manuscripts/PhysicalTravelTimeMeasurement.tex` | active | Measurement mapping between physical minutes and link cost |
| `Documents/L1_manuscripts/drafts/Tidalanes_ExtendedAbstract_0711.tex` | active | Most recent extended abstract |
| `Documents/L1_manuscripts/TidalLanes_0429.tex` | historical | Full paper draft, stalled since April |
| `Documents/L1_manuscripts/MODEL_replication_tidallanes.tex` | historical | Replication document, synchronised in April |
| `Documents/L1_manuscripts/drafts/` | historical | Earlier iterations of the paper, model, and abstract lines |

Figures live in three places by purpose: `Documents/L3_figs/` for manuscript figures, `Documents/PreSlides/figs/` for slide-only figures, and `Documents/L4_pipeline_outputs/` for tables and exhibits promoted out of the pipeline. Only the speed example figure is duplicated across the first two.

---

## Archived on 2026-07-27

Moved out of the working directories, retained on disk.

| Location | Contents |
|---|---|
| `data_work/docs/_superseded/` | March refactor documents (`REFACTOR_BLUEPRINT`, `PIPELINE_MIGRATION_PLAN`, `STAGE_IO_SPEC`, `REPRODUCIBILITY_SNAPSHOT`, `DATASET_SCHEMA_SNAPSHOT`), the prototype model line (`CURRENT_MODEL_EQUATIONS`, `MODEL_REFRESH_2026-03-21`, `STRUCTURAL_MODEL_WORKFLOW`), plus `MANUAL_MATCH_OVERRIDE_WORKFLOW`, `CODEX_HANDOFF_NOTE`, and `TEMP_STAGE02_PROJECTION_DEBUG` |
| `Documents/L2_notes/_superseded/` | The two `model_v1_*_temp_*` checklists from April; since 2026-09-28 also `model_and_results_summary_20260630.md` and `TidalLanes_estimation_notes_20260630.tex/.pdf` |
| `Documents/PreSlides/_snapshots/` | Seventeen `Tidal0715_backup_before_*` sources, three milestone PDFs, the advisor-revision line, and earlier speaker scripts. `SNAPSHOT_MANIFEST.md` records what each snapshot preceded |

Thirteen further backup PDFs and 180 LaTeX build artefacts were discarded to the trash on the same date. The PDFs recompile from the retained sources.
