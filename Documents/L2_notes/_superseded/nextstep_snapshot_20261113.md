# Next Step

这份文件只保留当前任务状态、最近可执行动作和开放决策。已完成工作与锁定参数统一记录在 `data_work/docs/WORK_LOG.md`。

## 当前状态

- 研究主线不变：以方向性速度不对称为核心，连接 directed network 与 quantitative spatial model。
- 2026-09-28 完成：四份旧口径文档换版并归档（`model_and_results_summary_20260928.md`、`TidalLanes_estimation_notes_20260928.tex`、`Model_v4` 福利段、`ESTIMATION_NOTES.md`）；复现包 deck 改为 `Tidal0715`；02–05 点自由流变体（负延误边 8.29%→7.76%，p90 才降到 0.3%，主线未换）；选边分日检验（winner's curse 约 3%，结论不变）；路径核试算（观测通勤半弹性 0.012–0.042/min，主线 0.087；指数核 ν=0.012 时区内份额 6% 与观测吻合；幅度随核变化一个量级）。审计 memo `model_audit_and_referee_review_20260926.md` §6 记录 anchor/additive delay/区内修正讨论。
- 待决定：(1) 主线 `R` 的 1e-12 截断在 89% OD 格绑定（E9），去掉后 ST/LT 福利 +1.07%/+0.29%，需决定去掉还是给理由；(2) additive delay 采用与否及 λ 的口径（Barwick 1.1 是总速度弹性，延误弹性需重推）；(3) 区内通勤修正方案（`m_ii` 假设值 / 校准 / 换核）；(4) P6/P7 撤下方式；(5) 文章走向（D7）。
- 2026-08-26 已完成彩田路 0723/0724 出城摄像头独立 RDiT specification search v1 及单车道流量 supplement。总流量结果保存在 `flowvedio/batch_outputs/20260723_24_state_dependent_recognition_v5/analysis_basic_v1/outbound_rdit_specification_search_v1/`，单车道结果在相邻的 `outbound_rdit_per_lane_supplement_v1/`。0724 开通的 60 分钟规格明确保留 37 个 pre bins；缺失来自 16:07:43–16:26:46 无录像及被删除的操作尾段，不作设备限制或零流量处理。0724 开通与恢复的方向总流量重分配符号一致，但单车道流量没有相应的显著正向跳变；单车道结果只作容量利用率分解。
- 2026-08-04 已完成中心线拓扑第一轮自动诊断，输出为 `data_work/outputs/centerline_rawpath_topology_candidates_v1/`。在不改 `manual_centerline_rules_v11` 的前提下找到 19 个 raw-road-supported 候选（高 3、中 8、低 8）；高+中候选可将 weak components 从 145 降至 134，但 square-grid directed pairs 仍为 4,778，覆盖率不变。原因是 stage06 按单条中心线穿越网格的序列建边，不使用道路级 component 连通性。同网格内补口不进入现行 grid cost，因此暂不为降低 component 数而自动生成 v12。
- 2026-08-04 已完成中心线跨网格缺口诊断与三档 robustness tier，输出为 `data_work/outputs/centerline_crossgrid_gap_candidates_v1/` 和 `data_work/outputs/centerline_crossgrid_robustness_tiers_v1/`。41 条 raw-supported 缺边中，30 条为有中心线几何但缺 stage06 速度边，11 条同时缺中心线几何；40 条有完整 AM/PM/free-flow raw speed。Tier B 的 32 条完全速度支持且完全匹配候选可把有效网络从 2 个 weak components 合为 1 个，但 road-commuter-weighted shortest time 只下降 0.358%，受益 commuters 占 11.7%。Tier A/B 保留为 centerline robustness 候选，不替换 raw-road gate5 主线；Tier C 含未匹配 raw roads，仅作诊断。
- 2026-08-04 已完成 gate5 free-flow speed robustness，汇总为 `data_work/outputs/freeflow_robustness_gate5_quick_v1/`。五套替代口径均独立重跑 square-AM edge projection、gate5 LCC bundle QA 和 anchored Stage 2 quick top-25。窄窗口及 weekday/weekend 的边级中位 free-flow time 与主线相差不超过约 1.2%；90 分位口径低 8.7%。短期福利变化为 +0.439% 至 +0.469%（主线 +0.438%），长期为 +0.143% 至 +0.148%（主线 +0.142%），top-25 overlap 为 23--25/25。主线继续保留全周 `22:00--05:00` harmonic；90 分位作为强 robustness，不替换 active bundle。
- 2026-07-27 合作者会议修改意见已整理到 `Documents/L2_notes/meeting_notes_20260727.md`（对照 `07/21 Progress`）：Data 强调最快绕过网格 + 中心化路网；Measures 做成城市形态/路网/congestion 因子解释；模型图改市中心 base、只画受影响 grids；CF 补 symmetric 图与深圳 keep-fm/change-asy；incidence 按走/不走 tidal 与 never/always/switch 四组报 `t` 百分比与分组 `W`；congestion technology 式子不变、interpretation 改 relative time/speed；深圳实地评估双时间断点。
- 2026-07-10 合作者会议修改意见见 `Documents/L2_notes/meeting_notes_20260710.md`；其中 descriptive framing、symmetric-input 分布诊断、segment-person-time、sorting、城市扩张 / 人口增长等仍有效，与 07-27 意见叠加而非替代。
- 2026-07-10 上述五项已完成 first-pass 尝试：脚本 `data_work/src/model/full_estimation/run_coauthor_revision_attempts_20260710.py`，输出 `data_work/outputs/coauthor_revision_attempts_20260710_v1/`，交接 `Documents/L2_notes/coauthor_revision_attempts_20260710.md`。
- 2026-07-17 `Documents/PreSlides/Tidal0715.tex` / PDF 的模型段沿用 link-cost 主线：$t_{kl}$ 是模型中的 directed link cost，$\tau_{ij}$ 是 all-route bilateral cost index；physical minutes $m_{kl}$ 进入第 17 页测量映射、第 18 页 estimation fixed point 和 physical-time 结果，转换关系为 $t_{kl}=m_{kl}^{1/\theta}$。因 $\lambda=1.1$ 是 physical time-flow elasticity，式（10）的 link-cost congestion exponent 为 $\lambda/\theta$；第 18 页和 Appendix Estimation Algorithm 现按代码实际顺序更新 $m_{kl}$，指数为 $\lambda$，随后转换为 $t_{kl}$。Appendix 同时写明 $\Xi_{kl}^{0}$ 是 model-implied baseline load、$\widehat q=\widehat\Xi$ 是 flow bridge、主线 log damping 为 $\omega=0.25$。第 7 页新增 Data 快速带过页；第 16 页只列 raw data source / coverage 和外部校准参数；第 17 页用一个实际北京相邻网格展示 AM 最短路径、五个 raw-road pieces、两端 access time 及加总到 $m_{kl}^{obs}$ 的过程。正文为 26 页；附录加入 video pilot evidence 和 opening-time RDiT design 后为 16 页，总计 42 页。底层 physical-time 结果仍按全城 expected additive physical time 计算：baseline 11.30 分钟，短期 10.60、长期 11.54；第 23 页已将 AM peak 总通勤时间变化（ST $-1{,}031$ person-hours，LT $+352$ person-hours）和福利变化（ST $+0.87\%$，LT $+0.32\%$）合并为一张表。原 1,276/714 treated-link totals 仅保留为局部 engineering incidence，不再作为全城人均时间。完整重估的 arithmetic symmetric-input 结果已进入正文：固定 passages 为 $-5.0\%$，完整重估为 $-5.5\%$；第 20 页采用左表右图，fundamentals 用 symmetric-minus-directed 的相对变化 profile 展示。
- 2026-05-02 合作者会议已同步 `TidalLanes_0429.tex`、`MODEL_replication_tidallanes.tex` 和 `TidalLanes_Model_v4.tex`；会议 notes 见 `Documents/L2_notes/meeting_notes_20260502.md`。
- **grid-travel cost 已锁定（最终版）**：主模型用 `model_input_edgeproj_v2_lcc_gate5_v1`（raw_connected v1 edgeproj_v2 + 最大连通分量 + detour gate G=5）；决策见 `Documents/L2_notes/travel_cost_lock_in_20260628.md`，对照见 `…/model_input_edgeproj_v2_lcc_gate5_v1/COMPARISON_vs_no_gate.md`。no-gate `model_input_edgeproj_v2_lcc_v1` 与 `model_input_centerline_lcc_v1` 仅作 robustness。2026-06-29 已修正 edgeproj path lane lookup：grid-grid lane 现在按 raw `roadseg_id` 接到底层 `road_class_lane_mean`，未接到底层 raw 的手动补充边保持 missing 并在 path lane 加权时跳过；gate5 与 no-gate bundle 已重建并 QA 通过。
- `raw_connected_v2` 已整理为与 v1 平行的 source-input 接口，并已生成 full edge-projection 版本 `data_work/outputs/raw_connected_v2_grid_travel_time_edgeproj_v1/`（AM/PM × square/hex/voronoi），含 coverage / tidal-asymmetry 图与 v1-v2 对比表；Replication square 输入另存为 `Replication_apply/aa_input_square_v2_raw_connected_v2/`。
- 图中没有数据或只有单向数据的线段，需要用浅灰色表示缺失的 directional information。
- 模型线近期不做参数估计，先搭 full estimation 框架和 counterfactual 流程；暂不考虑 exact-hat。
- full estimation 框架 v1 已落在 `data_work/src/model/full_estimation/`；**当前默认 QSM 输入**为 lane-fixed `model_input_edgeproj_v2_lcc_gate5_v1`。2026-07-01 已修正 welfare gauge：主 `Wbar` 采用原始 Frechet denominator，主线 bpr+pos05 Stage2 / tidal / policy / alpha-beta 输出已重跑。
- 2026-07-01 discussant audit A1--A6 与 B1--B4 已处理，B5 暂不改；policy auditfix 输出为 `data_work/outputs/policy_comparison_gate5_bpr_pos05_auditfix_v1/`，当前 slides 与 `tidal_model_slides_minimal/` 已同步并验证。
- 2026-07-01 symmetry-to-directionality suite 已加入主 slides 和 `tidal_model_slides_minimal/`；新增结果支持第二个贡献点“quantify what symmetry misses”，包内独立复现 18 行结果且 compare-only 为零差异。
- 2026-07-02 最新 `Documents/PreSlides/tidal.tex` 与 `tidal_model_slides_minimal/` 已再次同步并验证；mini 包现在覆盖 23 个图、1 个本地视频链接、模型 reference tables，以及 national / Shenzhen / Beijing centerline / jobs-housing descriptive metric checks。
- 2026-07-10 彩田路双设备视频已完成逐视频方向、片段状态和最终计数线整理；状态表为 `flowvedio/batch_outputs/20260710_line_review/lane_state_plan_20260710_v3.csv`，片段线计划为 `flowvedio/segment_line_plan_20260710_final.csv`；计数脚本已按 39 个片段 dry-run 验证。
- 2026-07-14 彩田路测试数据已完成全日计数、质量检查、跨设备时间对齐诊断和初步潮汐车道证据图；阶段性展示包为 `flowvedio/batch_outputs/20260710_caitian_identification_final/stage_evidence_v1/`。当前可展示的是方向性重分配与测量设计诊断，不应解读为最终因果或容量估计。
- 2026-07-14 已将两个来源流按 5 分钟同向 rate 做非加总融合，并完成最简单的开通/恢复 level-shift 检验；输出为 `flowvedio/batch_outputs/20260710_caitian_identification_final/pooled_breakpoint_v1/`。开通方向重分配信号较稳定，但恢复后的共同覆盖不足，当前不把 p 值作为正式证据。
- 2026-07-14 已复核 0709 的 `出城方向1621开始.MOV`：它可用于展示真实施工过程、定义 transition exclusion 和验证计数流程，但 pre 仅 2:45，不能单独承担 RDD；分析输出为 `flowvedio/batch_outputs/20260709_transition_v1/`。
- 2026-07-15 已完成 0709 outbound 的速度与密度 pilot：开通前 0:00–2:45、施工 2:45–3:30 排除、开通后 3:30–8:30；速度采用双线轨迹，密度采用方向 ROI，输出为 `flowvedio/batch_outputs/20260709_speed_density_pilot/analysis_v1/`。当前 36m 距离仅为标线推算，正式物理量仍需一段现场或地图距离锁定。
- 2026-07-17 已补充 0709 outbound 的三窗口比较：较晚窗口为 33:30–48:30，输出在 `flowvedio/batch_outputs/20260709_speed_density_pilot/analysis_three_windows_v1/`。南向北较晚期总 flow 与即时 post 基本相同，但旅行时间和可见车辆存量明显下降；5 分钟分块显示末段拥堵回升，因此该窗口用于 later operating-state 描述，不标为完全平稳状态或因果估计。
- 2026-07-15 彩田路视频 identification 完整设计已整理到 `data_work/docs/CAITIAN_VIDEO_IDENTIFICATION_PLAN_20260715.md`，覆盖理想采集、流量/速度/排队测量、双机位融合、多日重复断点、控制道路和现有初步证据。
- 2026-07-01 symmetric-input full re-estimation 与 policy-grid diagnostic 已完成：主输出为 `data_work/outputs/symmetric_input_reestimated_gate5_bpr_pos05_v1/` 和 `data_work/outputs/symmetric_input_policy_grid_gate5_bpr_pos05_v1/`。
- 2026-07-16 已完成不含 tidal 的 symmetric-input 地铁候选线实验：370 条共同候选线连接两个 6 km 端点 catchment，并把两端之间双向 generalized bilateral commuting cost 降低 10%。输出为 `data_work/outputs/symmetric_input_metro_candidates_20260716_v1/`。总福利排序几乎不变（Spearman 0.9995，top-100 overlap 100%，directed-benchmark regret 0）；每公里福利排序结论相同。该实验不支持“symmetric 会明显误选地铁线”，但保留 fundamentals 径向梯度相反的空间归因结果。
- 2026-07-16 已完成 symmetric-input 的 arithmetic-mean 完整重估和 geometric 对照。平均操作发生在 reciprocal physical link minutes，而不是 iceberg cost。Arithmetic mean 保持全网非加权 link-time 平均不变，但 symmetric 世界的模型平均 physical commuting time 低 5.5%；固定 observed road-commuter OD 的 shortest-path robustness 低 0.8%。Aggregate welfare 差异在两种平均法下均小于 0.07%，且符号不一致。`What Symmetry Misses` 已改为一页，主讲 directional flow × time covariance 和 fundamentals 空间归因，不再主讲 aggregate welfare 或地铁选址差异。
- 参数选择需要参考尤炜老师相关工作后再锁定。
- 2026-06-28 模型线审计已整理到 `data_work/docs/MODEL_AUDIT_LEARNING_GUIDE_20260628.md`；当前 `full_estimation_square_v1` 不能作为最终福利结果解释，需先修 CF1、free-flow/congestion anchoring 和输出可复现性。
- 2026-06-28 已新增简化 fixed-cost directional congestion 模型：代码在 `data_work/src/model/directional_cost/`；既有 gate5 主结果 `data_work/outputs/directional_cost_edgeproj_v2_lcc_gate5_v1/` 与 arithmetic symmetric robustness 是 lane fix 前结果，需用 lane-fixed bundle 重跑；`directional_cost_square_v1` 基于 centerline，仅对照。
- 2026-06-28 已审计深圳 `fishnet1000_pairtime_hat.csv`：这是完整 `1711 x 1711` grid-pair travel-time matrix，说明文档在 `data_work/docs/SHENZHEN_GRIDLEVEL_PAIRTIME_AUDIT_20260628.md`，输出在 `data_work/outputs/shenzhen_gridlevel_pairtime_v1/`。提供方建议主用 `car_time`；`car_time_detour` 暂作 robustness / diagnostic。
- 2026-06-28 已生成深圳 grid-level 非对称性图表证据：代码在 `data_work/src/shenzhen_gridlevel/generate_gridlevel_asymmetry_evidence.py`，说明文档在 `data_work/docs/SHENZHEN_GRIDLEVEL_ASYMMETRY_EVIDENCE_20260628.md`，输出在 `data_work/outputs/shenzhen_gridlevel_asymmetry_evidence_v1/`。当前版本是 unweighted all-pair evidence，用作 township-level 与 segment-level 之间的 within-city 中间层证据。
- 2026-06-29 已用深圳 fishnet1000 shp 完成空间化与真实 OD flow 暴露版本：代码在 `data_work/src/shenzhen_gridlevel/generate_gridlevel_spatial_asymmetry.py`，说明文档在 `data_work/docs/SHENZHEN_GRIDLEVEL_SPATIAL_ASYMMETRY_20260629.md`，输出在 `data_work/outputs/shenzhen_gridlevel_spatial_asymmetry_v1/`。`TARGET_FID` 已接到 OD matrix，grid500 通勤流通过坐标 point-in-polygon 匹配到 fishnet1000；已生成 flow-weighted threshold shares、地图和街道/Econzone 空间分组。
- 2026-06-29 已审计 `commute_shenzhen_202411_for_stage07.csv` 直接接 fishnet1000 的可行性：所有 `home_plot_id/work_plot_id` 都能接到 `commuting_grid_fishnet500.dta`，但 CSV 本身不含 fishnet1000 fid；用坐标直接落到 fishnet1000 后 final inter-cell pairtime exposure 为 82.2%，低于当前 retained grid500 聚合路径的 85.4%。80% 多的覆盖率主要来自 pairtime active fid 只有 1711/2181 和同一 fishnet1000 内部通勤被排除。
- 2026-06-30 已记录深圳 grid-level flow-weighted asymmetry 的文章定位和 slides 使用顺序：`Documents/L2_notes/shenzhen_gridlevel_flow_weighted_asymmetry_positioning_20260630.md`。该部分作为 descriptive measurement fact 和跨尺度 bridge，不作为 causal policy effect 或 structural welfare result。
- national 爬取城市清单主用 **2025 百城通勤高峰拥堵指数**（`raw_data/baidu_2025_commuting_peak_congestion_ranking.xlsx` 中 `城市` + `通勤高峰拥堵指数`）：`data_work/config/national_tidal_commuting_baidu2025_top100_congestion_v1.json`；建样本输出在 `data_work/outputs/national_tidal_commuting_baidu2025_top100_congestion_v1/`（7461 对 OD，与 2024 版同结构）。历史 2024 官方表版本仍保留在 `national_tidal_commuting_baidu2024_top100_congestion_v1` 便于对照。
- Shenzhen reduced-form 初步设计已记录在 `Documents/L2_notes/shenzhen_liuxian_did_purchase_design_20260626.md`：treatment 为留仙大道（学苑大道-福龙路，西往东），主 control 为龙珠大道（原茶光路段，西往东）；若另取龙珠大道远端路段，只作同名道路溢出/共同冲击辅助验证。
- 留仙大道 treatment-only 初步时序图已生成：`data_work/outputs/shenzhen_liuxian_gaode_initial_v1/`。post week 在工作日 17:30-20:00 对四个方向均慢于 pre week，不能在缺少 control / 天气检查时解释为政策效果。
- 留仙大道高德目标段已匹配到百度原始路网底层：主匹配为 6 条留仙大道 `roadseg_id`，其中 5 条在 2025-05-19 至 2025-05-25 的百度速度面板中有观测；详见 `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/metrics/baidu_liuxian_underlying_candidates.csv`。
- 深圳多条潮汐车道百度 post-only refinement v1 已落地：`data_work/outputs/shenzhen_multi_tidal_refinement_v1/`；交接与下一步见 `data_work/docs/SHENZHEN_MULTI_TIDAL_BAIDU_HANDOFF_20260628.md`。

## 当前下一步（按优先级）

- **平台数据与拥堵函数：下一步（2026-11-05整理）**。已完成部分见WORK_LOG 2026-11-04/05（匹配检查、路段λ、最严格匹配λ、形状图、四种函数、尾部下降检验）。汇报slides `Documents/PreSlides/platform_20261104/`。
  - **A 主线：拥堵函数口径（最优先）**
    1. 形状图尾部（2026-11-05已检验，见WORK_LOG）：下降始终在每条边车流最高的1–2%小时，与用q90/q95/q99无关；堵的小时车流稳定在q95的约0.83倍，不低于畅通时的高位，看不到堵后通过量下降；z尾部下降来自混入的非高峰小时。0–24点时全日λ约减半、高峰不变。剩余：0–24点曲线尾部为何不降；结论是λ要按负荷分段，单一常数弹性在低负荷和顶端都不适用。
    2. **IV讨论（先于函数估计）**：
       - 是否需要IV：OLS的偏误来源有三个，各自方向不同。车流到顶后q被压住而t上升（同时决定，OLS偏低）；联通计数的测量误差（衰减，OLS偏低）；事故、天气、信号等同时影响t与q的冲击（date×hour FE只吸收全市共同部分）。现有IV>OLS（边S1 0.160 vs 0.076）与前两者一致，但不能区分是哪一个。
       - 方向与变化来源：z是分时OD按自由流最短路分到边上的车流，S1用日内变化，S2/S3只用日间变化。预期第一阶段和简化式都为正（边S1第一阶段0.27）。到顶段需求增加不再增加q，第一阶段应变平甚至变负；IV估的是未到顶段的平均效应，需报告分段第一阶段（按z分档的平均q）。
       - 是否合理：(a) z与q同源（都来自联通出行），测量误差可能相关，这时IV纠正不了测量误差；(b) S1下z的日内变化与公交、行人、信号配时等同步，排除限制可疑，S2/S3更可信但第一阶段弱（边V3 0.11）；(c) 自由流路径与出发时刻平移带来时间错位。
       - 可做的检验：分段第一阶段；只用起讫点都离该边较远的OD构造z（减少同源误差与本地冲击）；留一日同小时车流作第二个工具做过度识别检验；比较S1与S2。
       - **文献对照规格（2026-11-05记录，同一面板、同一样本，逐项只改一处）**：
         (a)(b) 已做（2026-11-05，`congestion_almagro_spec_v1`，WORK_LOG）：边NLS β 0.15（Almagro 0.18）；全市负荷NIV 0.34–0.39、路段0.59–0.73，只用时段变化，排除限制可疑，暂不作主估计。分配车流作回归元只用时段变化时接近实测（0.078 vs 0.105），只用日间变化时降到0.044。
         (c) Allen–Arkolakis 2022截面做法：跨边比较每公里用时与每车道车流，用模型推出的车流（z或引力式车流）作IV，对照其Seattle δ1=0.488；
         (d) 北京尾号限行作日间工具（Yang, Purevjav & Li 2020的思路）：限行只在五环内，可用"五环内×当天限行尾号对应车辆比例"构造边层面工具用于S2；需先查2025年5月的轮换表和各尾号保有比例，五个工作日只有五个取值，第一阶段可能弱。
       - **需讨论**：车辆数口径。他们用载客率和公交权重换算车辆数，我们是联通SIM计数。边固定效应下规模不影响弹性，前提是联通份额在一天内稳定（B1第3项检验）；不稳定时弹性有偏，且SIM数不等于车辆数（一车多人、出租车、货车）。
       - 外部验证：他们用出行调查核对出行距离、时长分布和各收入组覆盖率。北京只有2015年出行调查可用，与2025年相差十年，只能核对分布形状，需讨论是否值得做。
    3. 函数形式：四种候选中BPR对形状图拟合最好，固定λ最差。顺序：
       (a) 带下限的常数弹性已做（2026-11-05/06，WORK_LOG）：z作IV、加日期×小时FE时边0.21–0.25、路段0.38–0.42，对时段窗口稳健；分段第一阶段在高负荷段不变平。下限模型到此为止，不再算bootstrap标准误（2026-11-06决定）。拥堵函数估计主窗口统一为06–22点，0–24点只作稳健性（2026-11-06决定）。远端OD构造的z已做（2026-11-06，`congestion_far_od_iv_v1`）：排除离边6–9 km内起讫的出行后，06–22点边λ从0.13降到0.08、路段0.16降到0.11，λ按区间报告。

       (b) 再用需求（IV）而非实际车流估BPR的b与容量c，避免车流到顶处的选择问题；
       (c) 边层面曲线是多条道路加总后的关系，按宏观基本图（Geroliminis & Daganzo 2008）解读，路段层面才对应单条道路的速度–车流关系；顶端用时下降与Hall 2018的"通过量摩擦"对照说明；
       核对Brad congestion pricing文章设定（2026-10-07会议，`meeting_notes_20261007_brad.md`）。Barwick 1.1是速度–密度弹性，不能直接当λ。
    4. 与合作者定主线函数与参数后：按新口径重跑反演、Stage 2、选边、反事实（后缀 `_od2025_v2` 那一批）；查Spec C在新OD下福利放大约15倍的来源；决定论文与slides是否换用2025 OD数字。
  - **B 稳健性：车流变量与匹配（记录，待执行）**
    1. 联通车流变量三项检验：主路/辅路与高架/地面是否合并或分错侧（京藏、京承、机场高速等）；识别率是否随车速变化（夜间畅通快速路）；联通份额日内稳定性（分时段联通常驻人口/普查人口）。外部流量校验待找到北京观测数据。
    2. 匹配：最严格匹配下IV λ只降0.00–0.02，匹配误差不是λ偏低的原因，以下降为低优先级：按重叠长度分配联通弦后重建路段面板；问联通平台为何同一路段有重叠节点序列（24.9%路段匹配弦长/路段长>1.2）。
    3. 路段层面结果今后一律报按gate5边聚类的标准误（约为按路段聚类的两倍）。
    4. 分配车流对照观测车流（Toole et al. 2015、Wang et al. 2012的验证做法，我们有实测可以直接做）：按边比较z与联通路网车流的水平、日内形状和方向比（现有对数相关0.60）；看差距来自路径假设（单一最短路）、漏掉的出行（只路由了49%的mode1出行、格内出行、过境）还是时段错位；试多路径或按实测速度重新分配，看第一阶段是否提高。
  - **D 底网与格对用时（2026-11-06定，按顺序执行；诊断结果见WORK_LOG 11-06各节，使用规则见 `Documents/L2_notes/observed_vs_imputed_data_use_20261106.md`）**：底网改用OSM三级路及以上（去层号）；主版本方案1B（观测路段＋300 m以内短缺口），上限对照方案1A（相对速度借上下游3跳，后备等级×圈层），只用观测对照为v3。
    1–3. 已完成（2026-11-06，`osm_directed_speed_v1`、`osm_grid_travel_time_v2`，见WORK_LOG）：与gate5（tau_obs，模型实际用的用时）早高峰用时相关0.36–0.40、方向比0.29–0.33；各构造之间方向比相关都只有0.27–0.41（2026-11-06更正，此前误用了gate5里继承的中心线用时t_min）。
    4. 【采集中】格对百度路径查询外部基准：样本扩为`grid_pair_baidu_validation_v3`（696对无向、1392条有向，含v2全部600条），按日配额5000每天约4200次；2026-11-10（北京时间）起连跑5个工作日 02:30/07:30/17:30（第1天无FREE），预计11-16 17:30完成。每时段跑完自动检查写入 `logs/panel_checks.jsonl`，成功率<50%自动停。11-11电脑重启中断总控进程（第1–2天6个时段均1392/1392成功）；已用 `START_DAY=3` 在 `caffeinate -i` 下重启，第3–5天为11-12、11-13、11-16。完成后做格对用时与方向比对照（1B、1A、gate5）。
    5. 已完成（2026-11-06，`rn_osm_flow_v1`）：RN节点即OSM节点，98.5%车公里落位，格边车流与旧口径相关0.90。注意百度只覆盖高速长度的约25%（按车流），1B会缺高速。
    6. 稳健性已做（2026-11-06，WORK_LOG）：1A的(a)–(d)对格对用时几乎无影响（相关≥0.98）；1B取600 m时方向比与300 m相关只有0.74；v3中心线（`centerline_v3_rules_v11_v1`）与v11几乎相同（用时0.98、方向比0.94），覆盖格边的联通车流91%。格边车流按格对汇总，与路网无关，可直接用于任一构造。
    6b. 换底网重跑主线已做（2026-11-06，`network_mainline_compare_v2`，WORK_LOG）：2025 OD与主线参数不变，OSM 1A的top100福利约为gate5的2倍（ST +2.61% vs +1.11%），方向不变，选中走廊只有40/100相同；1B基准内生均衡不收敛（anchored式在近零车流锚点的边上爆掉，λ=0.2也一样），要修需改拥堵函数或给锚点设下限，并入A4决定。v3 gate5 bundle是车道修复前建的，若要对照需先重建。
    6c. 拥堵函数已定（2026-11-08）：主线用带下限的常数弹性（`ce_floor`，β=0.25），锚点车流下限取中位数1%。BPR（b=1.65，需求IV）保留为对照，**模型推导待做**（存在性/唯一性、折点、边际外部成本与定价公式，见 `Documents/L2_notes/bpr_congestion_version_20261108.md`）；推导前BPR结果不进正文。两种函数在gate5上的A4试跑见WORK_LOG。
    6d. 谱半径问题已修（2026-11-09，WORK_LOG）：κ按网络取满足ρ≤0.95的最小值（gate5、v3、1A仍为1；1B 1.48；中心线v3 2.17），1B与中心线路由用ce_floor重跑，全部收敛。1B潮汐top100 W̄ +0.47%/+0.33%（1A +0.53%/+0.35%，gate5 +0.27%/+0.19%）；对称输入在两个OSM路网上都低估潮汐长期效应约0.1个百分点。κ对福利影响小（κ=1.5时降2–8%）。底网定为1B主版本、1A上限对照（2026-11-09确认）。1B上BPR已补跑（潮汐+1.25%/+0.63%，约为常数弹性2倍）、分日选边检验已做（奇偶选择88/100相同，高估0.2–1.0%，gate5为7.3%），见WORK_LOG 11-09。；均衡唯一性数值检查已做（多起点与外层映射单调性，四组均无多重均衡迹象，WORK_LOG 11-10）；P6/P7改为把税加在感知成本上并收敛，但福利口径漏掉选路扭曲、高估收益，需先推导定价下的福利公式（WORK_LOG 11-10）
    6e. 格网拓扑与按道路定义政策（2026-11-10，见 `Documents/L2_notes/grid_topology_and_road_level_policy_20261110.md`）：主线不改，按道路定义只作稳健性；用路段层面弹性时长期福利与主线几乎相同（4邻接0.342 vs 0.333）。
    6f. 结果与口径汇总（2026-11-10，`Documents/L2_notes/results_update_and_specifications_20261110.md`）。待定：(1) 背景车流口径：11-11检验显示总量大致成比例、方向上不成比例（潮汐走廊快/慢车流比，通勤0.29、全部0.48），假设A高估潮汐收益；下一步在拥堵式中加入背景车流（全部目的OD 0751–0759减通勤OD），做固定与按模型改路两版重跑，需改求解器；(2) 主口径（11-11定）：全日OD＋07–09点用时（0.441/0.313，`tidal_lane_diagnostics_osm1b_ce_floor_b025_fl001_ks_od2025_alldaytt0709_v3`）；07–09出发OD、06–10作稳健性；1.2节以后的结果待在新主口径下重跑；通勤链按链计×1.10（非1.28），是否纳入待定；(3) 11-16外部基准后确认1B；(4) 主表报弹性区间0.08–0.42。
    7. 与合作者定底网、函数与参数后，进入A4重跑。决策备忘已写（`Documents/L2_notes/coauthor_decisions_network_congestion_20261106.md`）。
  - **D 待做的探索（2026-11-11讨论记录，先不做）**
    1. 背景车流（A3）：拥堵式总车流＝通勤＋背景（全部目的OD 0751–0759减通勤OD）；主口径＝背景OD固定、按模型路径核改路；对照＝背景车流固定在格边（下限）与现行同比例假设（上限）；另报背景车流车小时变化；长期中背景OD固定需写明。需改求解器。
    2. 新主口径（全日OD＋07–09用时）下重跑结果文档1.2节以后：政策比较、稳健性、对称、按道路定义等。
    3. 弹性对照：套用Almagro et al.设定（只用边固定效应，工具变量为全市分小时出行总量，不加日期×小时固定效应），与β_c=0.25比较，作弹性稳健性表一行。另两项既有检验：只用起讫点离该边较远的OD构造z；留一日同小时车流做过度识别。
    4. 选址与路网加载分开（O4）：选址用全日OD，路网加载用07–09在途OD。需改求解器。
    5. 分时段模型（远期）：参照Almagro et al.按小时对齐OD、用时、车流、福利。简版＝各高峰小时分别求静态均衡、共用全日OD的选址，潮汐车道只在开放时段生效；完整版＝加出发时刻选择（Almagro未做，以Kreindler 2024出发时刻弹性低为由；文献中尚无）。数据已有（平台slot10、百度10分钟速度）。
    6b. 格对外部基准后续（11-11初步对照，见WORK_LOG）：11-16后正式重做对照；统一起讫点补查（百度从模型接入节点查询，每时段1392次，配额允许时做，或先做子样本）；改进格心接入（11-11试验：直线接入段无效；加OSM支路15 km/h可对齐用时水平与改善方向比，但用时相关不升，见WORK_LOG；支路只用于两端接入已实现并重跑：潮汐0.433/0.302，与主口径相差<4%，作稳健性，主口径暂不换，见WORK_LOG）；多点验证（子样本每格3点或从模型接入节点查百度）待11-16后用剩余配额做；查用时水平偏低27%的来源（路口与信号延误）；1B潮汐变化幅度比百度小约15%（斜率0.46为噪音衰减，已更正）。
    7. 模型OD距离衰减过陡（11-11发现，见WORK_LOG）：基准人均用时13.5分钟按模型OD算，同格46%（观测17%），半弹性0.233/分钟（观测0.050，ARSW 0.07，Tsivanidis 0.039）；观测OD×同一用时为35–38分钟。补救：(a) 结果并列报"观测OD×模型用时"的人均用时与节省；(b) 校准链路成本的时间弹性（AA式t=m^{δ0}，δ0≠1/θ，AA有δ0=1.6/θ稳健性）或成本尺度，使模型OD半弹性、平均距离或同格份额对上观测，接本分支od_fit_estimation.py；(c) 给同格出行加格内用时；(d) 补救后重跑top100，看福利与选边是否变。
       **执行计划（11-11启动，按序）**：路段权重改为 A=(κm)^{-γ}，γ=θδ0（AA主结果γ=1；τ^{-θ}=R不变）。① γ扫描：每个γ按谱规则定κ（ρ≤0.95，ρ随κ^{-γ}变），把R双比例拟合到观测居住/就业边际（等价于基准反演），算模型OD半弹性、同格份额、平均最短路分钟，对观测0.050/17%/34.6分钟，选γ；② 求解器加γ参数（默认1，主线不变），同步Ξ、κ规则、诊断的κ^γ归一化与期望路径分钟，γ=1须逐位复现主线；③ 选定γ下查收敛（基准与top100 ST/LT）；④ 重跑top100，对比W̄、节省时间（模型OD与观测OD两种权重）、选边重合。注意09-28指数核试算（旧gate5，定性稳、幅度不稳）与R的1e-12截断问题。新输出目录，不覆盖。
       **11-11已完成①–④（WORK_LOG）**：γ单独调不行（需≈0.05）；改为嵌套Fréchet η（τ^{-θ}=R^η），η=0.21对上半弹性，η=1逐位复现主线。top100：W̄ 0.085/0.072%（主线0.441/0.313），省时ST/LT 32.7/14.6秒（主线21.6/−12.7），选边重合85/100。待决定：是否换主口径；福利每分钟价值的口径（η标定偏低、主线偏高，ν/θ基准居中）；同格份额8.7%偏低，配合(c)格内用时。
    6. 文献：时间窗口对照表（AA、Tsivanidis、Almagro、Akbar et al.、HRS、Bordeu）待写进结果文档A2节；Tsivanidis正式版、Couture-Duranton-Turner (2018)未核对。
  - **C 通勤占比**：已完成（2026-11-10，平台0113E，WORK_LOG）：早高峰小汽车出行中通勤链（含中途停留）占45.6%（直接家→单位35.6%），晚高峰返程38.7%。按链计放大约1.10倍（1.28为按出行段计，已更正，11-11）；背景车流见6f。

- **2025平台提取**：入口 `Platform/README.md`，状态以 `Platform/202505_v01/queue.json` 与run-log为准。人员、trip、N、分时OD与逐日参与均已在平台建成；导出方案已定（合并×30分钟、合并×整窗、逐日×整窗、参与、N）。OD出口46份（0400）与RN出口37份（0460，0519–0523五天、strict）均已下载并通过总量核对。本地RN→模型边映射已完成（`Platform/utils/map_rn_to_model_edges.py`，结果见run-log）；0519–0523与其余14天OD差异小于日间差异，建议暂不自建其余14天RN（待用户确认）。边负荷口径已定：长度分摊为主、跨格次数为稳健性（之后需再仔细核查）；缺边负荷只报告覆盖率（约65%落在模型边上）；其余14天RN暂不做；平台其他表不动。模型2025 OD版输入用 `model_input_edgeproj_v2_lcc_gate5_od2025_v2`（节点、边不依赖OD故不重建；QA通过；原bundle未改）。λ识别v2：D3/D4加IV后λ≈0.14–0.19（18公里聚类se≈0.02–0.03），远低于主线 `lambda_phys=1.1`（Barwick et al.）；速度数据与RN同为0519–0525。全目的车流与速度面板（`all_traffic_speed_panel_rn202505_gate5_v1`）：通勤占比约22–28%，高峰弹性对全部车流约0.19–0.25、对通勤车流约0.12；用分时OD自由流分配作IV后全部车流弹性约0.14–0.23（全时段）、0.31–0.33（高峰），约13%的边有超饱和迹象；路段层面（RN匹配百度路段）结果相同（高峰IV约0.34）；R口径早高峰HW占R内出行36%；汇总报告见 `Documents/L2_notes/platform_data_report_20261104.md`。2026-11-04 补充路段λ稳健性（`lambda_robustness_v1`，见WORK_LOG）：约20%的百度速度是恒值填充，剔除后λ升10–17%（高峰IV约0.40）；按典型负荷分档弹性0.09→0.36，支持换成随负荷上升的拥堵函数。2026-11-03已用od2025_v2按主线参数（λ_phys=1.1）重跑反演、Stage 2、选边、反事实、对称/分日/自由流变体与AA/Spec C（后缀 `_od2025_v2`，见WORK_LOG）：方向与排序不变，主线W̄ ST/LT +1.11%/+0.43%。下一步见上条A3。

0. **彩田路 0723/0724 后续分析**：独立 specification search、单车道补充和方法审计已完成，完整 TeX/PDF 备忘录见 `Documents/L2_notes/caitian_video_rdit_complete_audit_20260826.*`。下一步不再继续按显著性扩展单日规格；若要形成正式因果 RDiT，优先补采相同切换时刻的多个工作日或同步对照。按当前研究设定忽略拍摄设备变化；0724 开通仍须披露约 23 分钟稳定样本空档，三区域结果须依赖逐段 geometry 标定。
0a. **Centerline robustness follow-up**：保留 v11 与旧 bridge 实验不动。跨网格候选和 provisional tier 已完成；若后续正式跑 centerline robustness，优先采用 Tier A，并以 Tier B 检查 geometry-missing 增量。进入 stage08 前需为新增边明确 lane 口径并生成独立 bundle。`centerline_rawpath_topology_candidates_v1` 的 19 个同网格拓扑候选只作审查和制图，不自动并入生产网络。
1. **Slides / paper integration decision**：`Tidal0715` 的 25 分钟讲述顺序现为 42 页 deck；第 7 页新增 Data 快速带过页，其他页和 speaker script 未改。模型段沿用 $t_{kl}$ / $\tau_{ij}$ 主线，$m_{kl}$ 仅用于第 17 页测量映射和 additive physical-time accounting。附录只保留 routing、time accounting、estimation、parameters/sensitivity、policy/targeting、validation、video pilot evidence、opening-time RDiT design 和 theoretical scope；treated-link segment-person-time 仅作局部 incidence。`What Symmetry Misses` 使用 5.0% fixed-passage bias、5.5% full re-estimation bias 和 fundamentals 径向 profile。下一步筛选 sorting wage/amenity accounting，并决定 observed-OD shortest-path 的 0.8% robustness 是否需要恢复到附录。Aggregate welfare 和 metro siting null 继续作为 scope result，不增加第二张主 symmetry slide。
2. **Symmetric-input spatial refinement**：当前 symmetric diagnostics 用 employment-weighted grid center 和距离带；若要讲“六环附近”，下一步接真实 CBD / ring-road geometry，再重画北京 ring-band map 和 radial profile。
3. **Population-growth / urban-expansion formalization**：当前 smoke 固定 fundamentals、用原始 congestion anchor、只比较 tidal 与 same-corridor expansion；若纳入正文，需要补正式城市扩张定义、人口增长口径，以及 congestion pricing 的 toll/rebate 闭合福利账。
4. **Sorting page rewrite**：用新增表说明 `\bar u_i,\bar A_j` 在政策反事实中固定，变化来自 effective amenity `\beta \Delta\log L_i^R` 和 wage `\alpha \Delta\log L_j^F`；避免写成 fundamentals 本身变化。
5. **Free-flow speed robustness**：五套版本化 quick suite 已完成，福利符号和 top-25 排序稳定。若进入论文附录，下一步只需把 90 分位强 robustness 扩展到正式 top-100 主规格，并汇总主线与 p90 的 `m_obs/m_ff` 尾部、全城 person-minute 和政策结果；其他窄窗口版本保留在复现包，不必全部进入正文。
6. **Existing model robustness cleanup**：基于 welfare gauge 修正继续补 no-gate 与 centerline_lcc robustness；theta grid 已完成，涉及 raw `(Π,P)` 的旧图表只作为诊断，不再作为福利结论。
7. **Policy-bundle extension**：在 free-flow / BPR 口径稳定后，再做固定实施 `K` 条边的组合选边、greedy re-ranking 和 local-search，对 TLC-score、welfare-elasticity、speed-only 和 local-search bundles 统一重解 endogenous GE。
8. **Remaining evidence maintenance**：Shenzhen grid-level slides 主用 flow-weighted threshold comparison；相关图件中没有数据或只有单向数据的线段改浅灰；national 速度爬取继续用 `national_tidal_commuting_baidu2025_top100_congestion_v1`。
9. **Lower-priority empirical follow-ups**：北京 centerline × 百度 API 验证、深圳龙珠大道 / 彩田路 / 红岭路 pre-post 速度数据，按已有 handoff 文档继续推进。
10. **Directional cost-response / corridor validation（想法已记录）**：把潮汐冲击下的成本变化接到真实动线设计，必要时加总到走廊/区县，并用深圳已有潮汐车道做对照；不是识别 \(\tau_{ij}\) 水平。说明与可执行分包见 `Documents/L2_notes/tau_cost_response_corridor_validation_idea_20260727.md`。最小起步是深圳 Package A（已开通走廊的 cost-proxy 对照表）。

## 政策反事实对比套件（plan, 2026-06-29）

**目的**：把潮汐车道放进一组标准 urban-commuting 交通政策里做量级对比 + 成本归一，回答"潮汐相对其他工具便宜多少、效果小多少、性价比如何"。全部在 gate5 anchored Stage 2 内生解里跑，套用现有 `tidal_lane_diagnostics` 指标。

### 政策清单（同一诊断套件，同一 treated-edge 集合尽量对齐）
容量类（改边 lane k → BPR 重算 τ）：
1. **Tidal lane（主线，已有）**：top100，慢向 +1 / 快向 −1，净 0 lane-km。
2. **道路扩张-同走廊**：top100，**双向各 +1**（净 +lane-km）。与潮汐唯一差别 = "再分配 vs 净增"，最干净的 apples-to-apples。
3. **道路扩张-定向最堵**：按 q·delay（流量×超额时间）选最堵 N 条 +1 双向，lane-km 预算与 #2 对齐，体现"定向价值"。
4. **道路扩张-均匀**：全网 +x% 容量，lane-km 总量与 #2/#3 对齐，体现"定向 vs 撒网"。
   （CF3 uniform speed ×1.10 已有 → 保留为**无成本上界/ceiling**，明确标注为非政策参照。）

定价类（在通勤感知成本上加 toll wedge，重解，收入 lump-sum 返还）：
5. **First-best 拥堵定价**：toll_e = 边际外部拥堵成本 = q_e·∂τ_e/∂q_e（每条边）。≈0 基建成本，理论福利上界。**收入返还假设要写明**（lump-sum rebate = 纯效率）。
6. **Second-best / cordon 定价**：仅对 CBD cordon 边（或仅 top100 潮汐走廊）收费，贴近现实，对照 first-best 的效率损失。
7.（可选）**均匀 per-km toll**：体现"定价也要定向"。

土地利用对照（不同 margin，可选但有价值）：
8. **职住附近住房供给放松**（走 β / ARSW housing block）：降通勤**距离**而非速度，展示交通 vs 土地杠杆。

**稳健性**：每个政策都跑 λ=1.1 与 λ=0.57 两档（band）；定价的相对排序应 λ-robust。

### 统一指标（每个政策复用现有诊断）
走廊/treated 边 % 时间变化；全城 person-min/day 与人均秒（ST+LT）；ΔW̄（ST+LT）；ST/LT 三段分解（direct / GE / sorting wedge）；OD 加权通勤变化；节点 sorting 分布（复用 Phase B）。

### 成本归一（这是 punchline）
给各政策粗略单位成本：lane-km 建设 capex（年化）、tolling 系统（固定+O&M）、潮汐（标识/执法，近零 capex）、定价（≈0 capex+管理费）。算两个性价比：**ΔW̄ / lane-km**（物理）与 **ΔW̄ / ¥**（货币）。画 cost-effectiveness frontier（x=成本，y=ΔW̄ 或 person-min）。成本数字标为 order-of-magnitude / 注明来源。

### 预期理论排序（用于 sanity check）
First-best 定价：福利最大、≈0 成本 → 性价比碾压 → 叙事主对照。道路扩张：raw 效果 > 潮汐，但贵 + LT 诱增需求侵蚀（接 fundamental law）。潮汐：效果小、净零成本、分配定向、比定价更易落地。Uniform speedup：天花板，非真实政策。

### 实施步骤（pipeline）
1. 在 `src/model/full_estimation/stage2_counterfactuals.py` 加 `road_expansion` 与 `congestion_pricing` 两类：容量类 scale k 后重算 BPR；定价类把 toll_e 加进感知 τ、重解、福利里做收入返还。**前置**：先解决 nextstep#1 的 observed-anchored congestion / free-flow 拆分——扩张改 k 必须正确扰动 free-flow vs congestion，否则 anchored 下加道无效。
2. 构建 treated-edge 集：top100 潮汐（已有）、top-q·delay 最堵集、CBD cordon 集。扩 `score_tidal_corridor_candidates.py`，把上一轮讨论的 **q·delay welfare 选边分数**一并落地（顺带验证 score_st/score_lt 是否已是 q·delay 形式、是否用均衡流量）。
3. 各政策走 `run_stage2_gate5.py` anchored，λ∈{1.1, 0.57}。
4. 每个结果过 `tidal_lane_diagnostics.py` 出统一指标 + ST/LT 分解。
5. 汇成主对比表 + frontier 图，新输出目录 `data_work/outputs/policy_comparison_gate5_v1/`，handoff memo 落 `Documents/L2_notes/`。
6. **验证步骤**：检查福利排序 first-best 定价 ≥ 道路扩张 ≥ 潮汐（符号/序）；检查定价收入账闭合；检查 λ band 不翻转排序；同 lane-km 预算口径一致。

### 待定决策
- 成本数字来源（深圳/中国 lane-km 单价、ETC/拥堵收费系统造价）用哪套？
- 定价收入返还口径：lump-sum 平均返还 vs 按收入比例 vs 不返还（影响福利符号，必须固定一种主口径）。
- 是否纳入土地利用对照（#8），还是只做纯交通四+定价。

## 当前开放问题

- （grid-travel cost 已锁定为 edgeproj_v2_lcc；见 `travel_cost_lock_in_20260628.md`）
- 深圳 `fishnet1000` 已有 polygon shp 并已完成 point-in-polygon flow crosswalk；是否还需要向数据提供方索要官方 grid500-to-fishnet1000 ID crosswalk 作最终 production QA？
- cross-city subsection 是否加入；如果加入，需求侧用职住分离，供给侧用交通方式比例和路网结构解释城市差异。
- Shenzhen 留仙大道 DID 是否能买到足够干净的开通前后速度数据，并避开正式开通前试运行污染？
- 参数选择具体对齐尤炜老师哪一篇或哪一组估计？
- 三亚在 2025 榜单内仍存在，但当前乡镇坐标与清洗规则下仍为 0 个可导出 pair（与 2024 版现象一致）；是否保留为 0 覆盖城市，还是补坐标或换下一名城市？

## 先看哪里

- `Documents/L2_notes/parameter_estimation_data_roadmap_20260702.md`
- `Documents/L2_notes/travel_cost_lock_in_20260628.md`
- `data_work/outputs/travel_cost_decision_v1/`
- `data_work/docs/WORK_LOG.md`
- `data_work/docs/DIRECTIONAL_COST_MODEL_20260628.md`
- `data_work/docs/SHENZHEN_GRIDLEVEL_PAIRTIME_AUDIT_20260628.md`
- `data_work/docs/SHENZHEN_GRIDLEVEL_ASYMMETRY_EVIDENCE_20260628.md`
- `data_work/docs/SHENZHEN_GRIDLEVEL_SPATIAL_ASYMMETRY_20260629.md`
- `Documents/L2_notes/shenzhen_gridlevel_flow_weighted_asymmetry_positioning_20260630.md`
- `data_work/docs/MODEL_AUDIT_LEARNING_GUIDE_20260628.md`
- `data_work/docs/CENTERLINE_BAIDU_VALIDATION.md`
- `Documents/L2_notes/meeting_notes_20260502.md`
- `data_work/docs/SHENZHEN_MULTI_TIDAL_BAIDU_HANDOFF_20260628.md`
- `Documents/L2_notes/AA_commuting_replication_adaptation_notes_20260426.md`
- `Documents/L2_notes/_superseded/model_v1_review_and_data_gap_temp_20260422.md`（已归档，仅历史参考）
- `Documents/L2_notes/_superseded/model_v1_implementation_checklist_temp_20260423.md`（已归档，仅历史参考）
- `data_work/outputs/comparison_grid_methods/`
- `Replication_apply/PLAN.md`
