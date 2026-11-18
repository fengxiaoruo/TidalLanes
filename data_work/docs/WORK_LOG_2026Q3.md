# Work Log, July to September 2026

Archived from `WORK_LOG.md` on 2026-11-18, newest entries first. Numbers here may be superseded; check the "Current mainline" table in `DOCUMENT_INDEX.md` before quoting them. Newer entries are in [`WORK_LOG.md`](WORK_LOG.md).

## 2026-09-28：文档换版、复现包改用 Tidal0715、路径核试算

- 按用户决定把四份旧口径文档换成新版并归档旧版：`Documents/L2_notes/model_and_results_summary_20260928.md`（旧版在 `_superseded/model_and_results_summary_20260630.md`）；`Documents/L2_notes/TidalLanes_estimation_notes_20260928.tex/.pdf`（旧版在 `_superseded/…_20260630.*`）；`Documents/L1_manuscripts/TidalLanes_Model_v4.tex` 的 (welfare_MA) 段改为"在 (Π,P) 归一化条件下等价、实现用 Fréchet 分母、反事实按水平重解"（旧文在 `drafts/TidalLanes_Model_v4_pre20260928.tex`）；`data_work/src/model/full_estimation/ESTIMATION_NOTES.md` 与包内同步（旧版在 `data_work/docs/_superseded/ESTIMATION_NOTES_full_estimation_v1_20260514.md`）。`DOCUMENT_INDEX.md` 同步。
- 新版摘要的通勤时间列全部改为物理时间口径；P6/P7 行标记为实现有误并撤出主表；αβ 表改为 ST direct / LT total / person-min。历史 memo 中对旧文件名的引用按索引规则保留不改。
- `tidal_model_slides_minimal/` 的包内 deck 改为 `Documents/PreSlides/Tidal0715.tex`（包内仍名 `tidal.tex`；PDF 来自 `Tidal0718.pdf`；讲稿 `tidal_speaker_script_Tidal0715_20260718.md` 改名 `tidal_speaker_script.md`）。manifest 改为 27 张图加视频；`data_work/outputs/od_validation_…/figure_od_validation_structural_central99.pdf` 在同步时复制到 `figs/` 并改写路径。`make check` 通过（102 行 file map），`make slides` 编译 43 页无错误。旧的 reduced deck 不再进包。
- 新增 `data_work/src/model/full_estimation/run_route_kernel_exp_trial_20260928.py` 与输出 `data_work/outputs/route_kernel_exp_trial_20260928_nu00122_v2/`、`…_nu00420_v2/`（首次运行因弹性符号写反归档为 `…_v1_failed_20260928_01/`）。幂核单元检查复现主线（W̄ 3.2001、ST +0.871%、LT +0.324%、42.1 s / −14.4 s）。
- 观测通勤对网络最短路分钟的 PPML 半弹性为 0.0122/min（全样本）与 0.0420/min（≤60 分钟），log-log 弹性 1.36；主线模型 OD 的对应值 0.087/min 与 3.3；观测流量对 ln R 的斜率 0.165。观测通勤量加权最短路均值 121 分钟、34% 的通勤人数在 90 分钟以上，网格分钟数偏高。
- 发现主线对 `R=(I−A)^{-1}` 的 1e-12 截断在 88.8% 的 OD 格绑定（承载 30.7% 观测通勤人数）；去掉截断后主线 ST/LT 福利变为 +1.07%/+0.29%，ST/LT 时间变为 50.5 s / −18.7 s，区内占比 56.9%。
- 指数核 ν=0.0122：区内 6.3%、baseline 期望通勤 113 分钟、ST +0.53%、LT +0.33%、ST 省 112 s、LT −3 s；ν=0.042：区内 15%、ST +2.44%、LT +0.56%、ST 省 173 s、LT +108 s。定性结论稳定，幅度不稳定。全部为试算，未替换主线。
- 同日启动的 02:00–05:00 自由流变体与选边分日检验由后台任务执行，结果另记。

## 2026-09-28 Gate5 free-flow 02:00--04:59 window and top-100 selection split-day check

- Added `src/diagnostics/build_freeflow_speed_definition_caches_v2.py` (chunked read, `--definitions` option) and built two caches under `outputs/freeflow_speed_definitions_v2/`: `ff_0205_all_harmonic` and `ff_0205_all_p90` (stamps with `hour_of_day` in {2,3,4}, all seven days, 24,340 road segments, median 124 observations). Median segment speed ratios vs the retained 22:00--05:59 harmonic cache: 1.0138 and 1.0960. The v1 root and the retained cache were not modified.
- Reran the square-AM pair-specific edge projection, gate5 LCC bundle, QA, and Stage 2 quick for both. Every bundle has 1,104 nodes, 2,642 edges, and 310,525 OD pairs, passes QA, and keeps AM `tau_obs_min` identical to the mainline. Edge-level median free-flow time ratios: 0.9862 and 0.9074. Share of gate5 edges with `tau_obs_min < tau_ff_min`: 7.76 percent (205 edges) and 0.34 percent (9), against 8.29 percent (219) in the mainline. Median congestion ratio 1.172 and 1.279 vs 1.149.
- Stage 2 quick: baseline Wbar 3.200103 and 3.200318; top-25 tidal ST welfare change 0.441 and 0.470 percent (mainline 0.438), LT 0.143 and 0.148 percent (mainline 0.142); top-25 overlap with the mainline quick run 24 and 23. Summary and README in `outputs/freeflow_robustness_gate5_ff0205_v1/`. Retained decision unchanged: the 22:00--05:59 harmonic definition stays the mainline; the narrower window does not remove the roughly 8 percent of edges with negative measured congestion, only the 90th-percentile rule does.
- Winner's-curse check for the top-100 `score_bpr_st` selection: added `src/diagnostics/build_am_day_split_speed_caches.py` (exact replica of the retained AM rule, weekday 07:00--08:59 harmonic; pooled split reproduces the retained AM cache to 6.7e-16) and built `am_odd_days` (05-19, 05-21, 05-23) and `am_even_days` (05-20, 05-22) caches in `outputs/am_day_split_speed_caches_v1/`, then edgeproj, gate5 bundles, and QA for each (odd 1,104/2,644, even 1,104/2,646 edges, both 310,525 OD pairs, QA passed).
- Added `src/model/full_estimation/run_selection_split_day_check_gate5.py`. Output in `outputs/selection_split_day_check_gate5_v1/`. The mainline bundle's own selection reproduces the retained top-100 set and numbers. Matched on the same transferable pairs, selecting on one half-week and evaluating on the other gives ST direct effect 40.6 s vs 42.0 s in-sample (ratio 0.968), ST Wbar 0.835 vs 0.865 percent, LT Wbar 0.307 vs 0.319 percent, treated-edge Xi-weighted delta tau 3.0--3.2 percent smaller. Unrestricted gap 5.6--6.1 percent, about half of it from 3--6 non-transferable pairs. Top-100 overlap odd vs even 82 (Jaccard 0.69), each vs mainline 88--89. Conclusion: selection on noisy week speeds inflates the direct effect by about 3 percent, an upper bound for the full-week selection; no mainline conclusion changes.

## 2026-09-26：模型与估计审计、复现包同步到物理时间口径

- 审计结论与审稿人意见见 `Documents/L2_notes/model_audit_and_referee_review_20260926.md`。核心发现：复现包与 authoritative 摘要仍用 06-30 的 `tau_ij` 当分钟口径；`TidalLanes_estimation_notes.tex` 与 `TidalLanes_Model_v4.tex` (welfare_MA) 仍是 gauge 修正前的福利写法；P6/P7 把 toll 加在 anchored 更新的 free-flow floor 上，floor 在 100% 边绑定，−4.2% 不是定价效应；模型均衡 OD 的区内占比 52.7%（Fréchet 流量；doubly-constrained 口径 57.2%；观测 8.0%），源于 `A_kl=1/m_kl` 使 `R_ii/R_ij` 中位达 8.4。
- 主库新增 `data_work/outputs/tidal_lane_diagnostics_gate5_bpr_pos05_physical_time_v2/`（top25/top50/top50_d0.5/top100 四个 spec，物理时间口径；top100 与 07-16 的 `_physical_time_v1` 完全一致）和 `data_work/outputs/alpha_beta_sensitivity_bpr_pos05_physical_time_v1/`（9 格）。新口径下所有收敛格的长期通勤时间都上升（+9,837 至 +31,663 person-min/day），ST direct 稳定在 −42 s/人；福利列不变。旧摘要中"person-min 在 α≈0.04–0.05 过零"的结论不再成立。
- `tidal_model_slides_minimal/`：同步三个模型文件，参考表与 `FILE_MAP.csv` 改指新目录，`ESTIMATION_NOTES.md` 加状态更新；`make check`、`make smoke-model` 通过，包内重跑 tidal 与 αβ 网格对新参考表最大绝对差为 0。记录见 `docs/VALIDATION_20260926.md`。包内 `tidal.tex` 未改。
- `tidal_model_slides_minimal_r/`：`diagnostics.R` 改为物理时间分解，runner 的 compact 列同步；R 重跑 tidal 四 spec 与新参考表在 1e-6 下一致（最大差 1.4e-9）。 R 的 αβ 九格重跑后 8 个收敛格同样在 1e-6 下一致（最大差 5.3e-9）；11 个表族中 10 个通过，唯一失败仍是 07 月已知的 P6 short-run 行。
- 重算 E4/E5 的方法：读 `stage2_endogenous_edgeproj_v2_lcc_gate5_bpr_pos05_v1/baseline_endogenous/equilibrium_arrays.npz` 与 `primitives/*.csv`，用 `A=1/m` 重建 `R=(I-A)^{-1}`，OD 份额 `S = R ⊙ (L_R Π^θ)(L_F P^θ)^T` 归一后取对角；P6 检查 `tau_ff + 1.1·m_base > tau_obs` 的边占比。
- `DOCUMENT_INDEX.md` 的"Where the current numbers live"表已拆分福利与通勤时间两行。

## 2026-09-04：锁定合作者外发文档的写作原则

- 合作者反馈指出，外发材料存在明显的 AI 腔，结论不够直接，方法和术语过多。以后给合作者分享的文档默认先写结论，再用简单、直白的话说明核心证据、主要局限和下一步。
- `writing.md` 已将这一要求设为最高优先级，并删除了把常用词机械替换成生硬学术词的规则。技术术语只在能增加准确性时使用，首次出现时用普通语言解释。
- `AGENTS.md` 和 `agent_quickstart.md` 已同步这一要求，确保新接入的 agent 在最低阅读路径中也能看到。该原则不改变结果的准确性、识别边界或必要的技术审核，只改变对合作者的表达方式和信息顺序。

## 2026-08-28 — RDiT report figure-role clarification

- Revised `caitian_video_rdit_collaborator_report_20260827.pdf` so Figure 1 shows only vehicle detections and the near/far counting lines; it no longer overlays the fixed-north, tidal-variable, or fixed-south physical regions.
- Moved the standalone three-region road schematic to immediately after the first paragraph of Section 3, where regional flow construction is introduced.
- Replaced the generic vehicle-inclusion description with the four counted classes: car, motorcycle, bus, and truck.
- Reformatted Section 5 event-study figures into two panels per row (opening on the left, recovery on the right), reducing the report from 18 to 14 pages while retaining readable axes and legends.
- Renamed Section 5 to `Flow Scatterplots` and removed the unnecessary note that the RDiT treatment coefficient is sometimes denoted by $\tau$.
- Regenerated the annotated frames and recompiled the final 14-page PDF; the revised pages were visually checked.

## 2026-08-27：彩田路视频断面对应百度子路段及双向速度图

- 使用 2026-07-10 进城视频 QuickTime GPS（纬度约 22.5636--22.5645、经度约 114.0617--114.0634）匹配百度有向路网，锁定视频断面所在的红荔路至北环彩田立交桥出入口段。南向北 `RS_NB1521432208_NB1521432620_1` 与北向南 `RS_NB1521432253_NB1521439708_1` 距视频 GPS 最近点分别为 2.4 m 和 2.3 m。
- 新增可复现脚本 `data_work/src/shenzhen_caitian_baidu/plot_video_segment_speed_timeseries.py`，输出在 `data_work/outputs/shenzhen_caitian_video_baidu_segment_v1/`。两个方向在 2025-05-19 至 2025-05-25 均保留完整 1,008 个十分钟原始速度观测，不插值、不平滑、不再对整条约 5 km 彩田路走廊加总。
- 保留空间匹配图、一周连续时间序列、七日分面图及 PNG/PDF/SVG 版本；绘图数据、匹配摘要、路段 GeoJSON 和 GPS 点表同时落地。该输出用于把现场视频流量证据与同一百度子路段的历史速度形态连接，不构成同年同日的速度--流量联合估计。
- 补充审核视频 GPS 周边 250 m 的完整百度路网及原始速度文件。没有更短的彩田路主路段穿过摄像机断面；约 343 m 的下一段南向北路段和约 139 m 的下一段北向南路段分别位于摄像机北侧约 180 m 和 193 m，约 650 m 的近邻候选为北环大道辅路且方向不符，均不替换当前匹配。当前 1.6 km 段的工作日启用窗口均速仅由 31.35 降至 28.08 km/h；图中的显著向北低速主要由 2025-05-20 驱动，当日参考窗至启用窗由 26.46 降至 14.89 km/h，且北侧相邻南向北路段同日也普遍减速，支持走廊级拥堵而非单段误匹配解释。

## 2026-08-27 彩田路 RDiT 合作者工作报告

- 新增独立报告 `Documents/L2_notes/caitian_video_rdit_collaborator_report_20260827.tex` 及其11页PDF，不覆盖完整方法审计文档。
- 报告按原始视频类型与标记、处理流程、变量定义、四事件原始散点、RDiT specification、基准估计、带宽敏感性、单位车道结果和下一步设计组织。
- 操作期点明确按车辆实际穿越方向计入北向或南向，并继续排除在全部RDiT回归之外。7月24日16:07:43至16:26:46无录像及其后仅存2分15秒操作尾段被单独披露。
- 新增总流量和单位车道流量的基准系数图，输出保存在 `analysis_basic_v1/outbound_rdit_collaborator_report_v1/`；绘图脚本为 `flowvedio/plot_outbound_rdit_collaborator_coefficients_v1.py`。
- 报告中的目标系数记为 $\beta^{RD}$，同时说明它与常见符号 $\tau$ 表示同一断点水平差。单位车道流量仍定位为容量利用率分解，不解释为速度--流量弹性。
- 下一步设计锁定为寻找同钟点未切换工作日或同步控制道路、补采多个连续工作日，以及用具有已知物理间距的双gate构造统一速度或旅行时间。
- 外发版进一步删除“面向合作者”“阶段性工作报告”和摘要等内部措辞，标题改为“彩田路潮汐车道视频流量与时间断点分析”，正文从Introduction开始。
- 数据部分加入四张车辆检测与轨迹识别截图，覆盖7月23日和24日、白天和夜间、向北和向南开放状态。
- 原始流量图改为按7月23日和24日分别组织，北向、南向和方向差均使用整行宽度；基准系数图按日期拆分，方向差统一为北向减南向口径。
- 7月24日开通和恢复的带宽图分别放大；四次单位车道事件研究图逐事件拆分，单位车道系数图按日期拆分。最终PDF为18页A4，所有页面已渲染检查。
- 第二轮外发排版将标题改为 `RDiT Analysis of Caitian Tidal Lanes`，Introduction缩减为文档结构与内容概述，不在开篇重复结果摘要。
- 第5节前加入明确分页，使节标题、读图说明和首张7月23日图从同一页连续出现；该节六组日期内图缩至0.64倍正文宽度，保持按日期分组并减少空白。
- 图8和图9改为同一outcome上并列开通与恢复点估计及95%置信区间，蓝色圆点表示开通、红色方点表示恢复，方向反转可直接比较。
- 图12至图15新增操作期紫色菱形。由于操作时方向车道数正在变化，操作期点采用方向总流量除以操作前稳定状态车道数的描述性归一化；正文和图注明确该值不是操作期实际单位车道负载，且不进入RDiT拟合。
- 更新后PDF为17页A4，重新编译两次并检查全部页面，无未定义引用、控制序列或图表裁切。
- 图1新增measurement overlay。四张检测截图分别采用M06、M08、M16和M18既有geometry：红线标记near gate，白线标记远端辅助线，半透明区域标记固定北向、潮汐可变和固定南向物理区域。新增脚本为 `flowvedio/annotate_rdit_detection_examples_v1.py`，外发版PDF已重新编译并检查图1标签、线段和区域边界。
- 统一外发报告中的视频处理中间量表述：视频先转换为逐车轨迹；轨迹穿越近端计数线形成方向计数事件并构造流量；逐帧可见车辆数衡量画面覆盖路段内的车辆存量；同一车辆依次穿越两条计数线的时间差构造区间旅行时间。全文删除“横断面穿越事件”“帧级密度”“候选测速门匹配”和“跨gate匹配”等并行术语。
- 将“只有固定区域和潮汐区域变量需要逐视频geometry”改写为可直接解释的测量差异：方向总流量只需判断实际行驶方向；分区域流量还需判断车辆经过固定北向、潮汐可变或固定南向区域，因此不同机位下的画面边界必须逐段标定。
- 新增道路区域定义图，采用IMG_0017无检测框原始帧与M08边界，分别标注固定北向区域（始终向北）、潮汐可变区域（方向随状态切换）和固定南向区域（始终向南），并显示近端计数线与方向箭头。更新后外发PDF为18页A4。

## 2026-08-26 彩田路 RDiT 方法审计与完整结果备忘录

- 新增 `flowvedio/audit_outbound_rdit_methods_v1.py`，独立输出到 `analysis_basic_v1/outbound_rdit_method_audit_v1/`，未覆盖任何既有结果。审计补充完整局部线性系数、残差 ACF/Durbin-Watson/Ljung-Box 诊断、HAC lag 0/1/3/5/10/15 敏感性、事件窗口支持域和系统假断点。
- 0724 开通的 60 分钟规格虽有 37 个 pre bins，但最近稳定 pre 观测距稳定开通完成约 23.02 分钟。因此跳变是跨空档的趋势外推，不是断点邻域内的经典局部 RDiT 比较。按用户确认的研究设定，拍摄设备或视频文件变化不作为识别威胁；总方向流量继续使用统一 near gate，三区域 outcome 依赖逐段 geometry 标定。
- 在连续稳定录像中每 5 分钟设置假断点，并排除真实切换前后 30 分钟，共得到 59 个 cutoff、295 条 outcome 估计。HAC(5) 的名义 5% 拒绝率约为 16.9% 至 23.7%，说明普通时间波动频繁产生显著跳变。0724 的方向差在假断点分布中仍极端，但只能作诊断，不能替代同带宽随机化推断。
- 0724 开通和恢复的方向差分别为 +21.934 和 -14.764 辆/分钟，方向与车道重新分配一致；双向总流量证据较弱，单车道流量也未显示显著正向变化。保留结论是探索性 flow reduced form，不识别结构模型中的速度－流量弹性。
- 完整审计文档保存为 `Documents/L2_notes/caitian_video_rdit_complete_audit_20260826.tex` 和同名 PDF。文档记录数据支持域、变量定义、每类 specification、主结果、趋势变化、带宽与单车道图、假断点诊断、方法逐项对照和下一版采集设计。
- 按同一基准标准回查 0701、0709 和 0710 旧视频，并新增 `flowvedio/analyze_older_outbound_rdit_screen_v1.py`。独立输出保存在 `20260710_caitian_identification_final/older_outbound_rdit_same_standard_v1/`。0701 没有已核定的状态切换，0709 开通只有 2.75 分钟稳定 pre，均不能进入局部 RDiT。0710 开通的最近稳定 pre 距完成时刻 31.48 分钟，45 分钟方向差异常放大而 60 分钟降至 19.80（p=0.137），显示长距离趋势外推。0710 恢复两侧可估计，但方向差在所有可估计带宽均为正，与恢复应有的负号相反；定位为边际描述窗口，不作为正向识别证据。
- 更新 `caitian_video_rdit_complete_audit_20260826.tex/pdf` 的主结果图：北向总流量、南向总流量和北向减南向现在各自包含 0723 开通、0723 恢复、0724 开通和0724恢复四个面板。图形展示使用 5 分钟 bins，表中回归继续使用 1 分钟 bins；14 页 PDF 已重新编译并逐页检查新增图页。
- 新增 `flowvedio/run_outbound_operation_periods_20260723_24_v1.py` 和独立输出 `operation_period_recognition_v1/`。0723开通复用已完成的15分25秒全路识别；新跑0723恢复15分钟、0724开通尾段2分15秒和0724恢复8分25秒。操作期使用覆盖整条可见道路的横断面，方向按穿越方向判定；所有车辆类别保留，操作车辆未单独剔除。
- 新增 `flowvedio/plot_outbound_rdit_with_operation_points_v1.py` 和 `analysis_basic_v1/outbound_rdit_operation_overlay_v1/`。输出包含1分钟和5分钟操作期点数据、汇总表，以及16组PDF/PNG/SVG事件研究图。稳定期为蓝色圆点，操作期为紫色菱形；红色拟合线和全部回归仍只使用稳定期样本。完整审计PDF已改用带操作点的北向、南向和方向差四面板图，并补充带宽稳健性的偏误、精度、实际支持域和识别边界解释。

## 2026-08-25：彩田路 0723/0724 稳定状态全量识别与基础统计

- 完成 18 套 v5 手工 geometry 的数值与叠图检查；M17 超出 1280 像素画面右缘的 `x1_right=1300` 仅在运行配置中截到 1279，原工作簿保留不改。所有实际距离按缺失处理。
- 新增 `flowvedio/run_20260723_24_state_dependent_v5.py`，并完成 25 个稳定状态分段、21.356 小时视频的全量识别。输出包含 795,228 个处理帧、15,460,040 条检测轨迹记录、180,767 次双线合计穿越和 73,482 条完整双线轨迹；速度字段全部为空。
- 新增 `flowvedio/analyze_20260723_24_state_dependent_v5.py`。基础流量以每个方向的入口线穿越计数，避免两道线重复；共保留 85,655 次入口穿越。进城/出城来源机位分别汇总，不相加为全路总流量。
- 全稳定时段按覆盖分钟加权的描述结果：潮汐区向北开放相对向南开放，北向流量由 30.22 增至 40.07 辆/分钟（+32.6%），南向流量由 44.02 降至 22.40 辆/分钟（-49.1%）。八次同日同机位切换前后 30 分钟比较的中位变化分别为北向 +41.9%、南向 -21.0%；北向均值受 0724 出城低基数影响，不作为主描述量。
- 当前结果为描述统计，不是因果 RDiT。施工转换期尚未进入本版；下一步需单列普通车与操作车辆并完成分段/机位 QA。
- 新增同方向双机位同步 QA。第一版按行驶方向使用入口线时，南向同分钟相关为 0.908、五分钟为 0.924，但北向只有 0.424 和 0.442，且中位出城/进城比为 0.491。进一步拆分两道线后确认差异来自远线漏检：出城机位北向近线为 48.50 辆/分钟、远线仅 24.70；进城机位北向近线为 48.24、远线为 39.40。`flowvedio/analyze_20260723_24_camera_near_gate_v1.py` 统一使用清晰近线后，北向一分钟/五分钟相关升至 0.894/0.939，五分钟平均流量为 48.57/48.77；南向升至 0.980/0.995，平均为 31.57/30.89。两个方向最佳 lag 均为 0。主总流量应统一采用近线；远线保留用于双线轨迹和通行时间匹配，不作为跨机位总流量主计数线。
- 新增仅出城机位、统一近线流量的探索性 RDiT：`flowvedio/analyze_20260723_24_outbound_rdit_near_v1.py`。排除 0724 开通（录像缺口且 geometry 改变），保留 0723 开通、0723 恢复和 0724 恢复。三次事件的断点前后 30 分钟原始均值均呈预期方向重分配，但严格锁定同一 geometry 后，局部线性方向差在 30/45/60 分钟带宽下仅 0724 恢复稳定为正；0723 开通稳定为负，0723 恢复从负变为零。施工造成 8–15 分钟 donut hole，0723 开通同 geometry 的稳定 pre 只有 12 个完整分钟，因此回归依赖趋势外推。当前仅出城数据可作描述性事件证据，不能支持正式因果 RDiT。
- `flowvedio/analyze_20260723_outbound_tidal_region_opening_v1.py` 将 0723 出城开通的 M08 近线按固定北向、潮汐可变、固定南向三区域拆分。开通前潮汐区全部为南向，11.36 辆/分钟；开通后全部为北向，5.01 辆/分钟。固定北向由 43.31 增至 47.63，固定南向由 24.56 降至 19.22；含潮汐区后的北向总量为 52.64、南向为 19.22。该结果直接验证车道使用方向切换，但只有 12.6 分钟 pre，不能替代正式 RDiT。0724 出城开通前 M16 只有双方向 geometry，且与 post M17 不同；若需车道级前后描述，必须额外补标 M16 的潮汐边界。
- 用户补充 M16 开通前潮汐边界：`y1=400` 时使用原分界 700 与新增分界 800，`y2=300` 时使用原分界 680 与新增分界 750；三区域从左到右为固定北向、潮汐可变、固定南向。工作簿新增 `M16潮汐补充` 页，机器配置将 M16 更新为 tidal geometry，实际距离仍为空。
- `flowvedio/analyze_20260724_outbound_tidal_region_opening_v1.py` 使用 M16/M17 近线重算 0724 出城开通三区域。缺口前后各 30 分钟内，潮汐区从全部南向 16.30 辆/分钟切换为全部北向 5.33；固定北向从 40.47 增至 48.63，固定南向从 27.07 增至 28.60。含潮汐区的北向总量从 40.47 增至 53.97（+33.4%），南向从 43.37 降至 28.60（-34.1%），双向合计仅从 83.83 降至 82.57（-1.5%）。该结果支持方向重分配而非总通过量提高；19 分钟缺口和 geometry 变化仍排除连续 RDiT 解释。
- `flowvedio/plot_all_collected_direction_timeseries_v1.py` 将 0723/0724、进城/出城机位的全部完整五分钟样本分别绘成北向和南向四面板时间序列。连续录像段内连线，录像缺口保持空白，底色区分向北/向南开放；PNG、SVG、PDF 及绘图数据保存在 `analysis_basic_v1/all_collected_direction_timeseries_v1/`。
- `flowvedio/plot_outbound_combined_direction_timeseries_v1.py` 进一步只保留出城机位，将北向和南向总流量叠加在同图中，并按 0723/0724 分为上下两个共享尺度面板；输出与对应 CSV 保存在同一目录。
- `flowvedio/create_outbound_random_detection_audit_v1.py` 以固定种子 20260825 按日期和潮汐状态分层抽取 8 个出城机位时间点，直接叠加全量识别保存的车辆框、类别、置信度和轨迹 ID。全分辨率单帧、汇总页和抽样清单保存在 `analysis_basic_v1/outbound_random_detection_audit_v1/`，可复现同一抽查样本。
- 新增 `flowvedio/analyze_outbound_casewise_rdit_v1.py`，按四次切换分别估计出城机位的一分钟局部线性 RDiT，删除施工操作区间，主带宽为 60 分钟，HAC 最大滞后 5 分钟，并保留 30/45 分钟敏感性。逐事件图、完整回归表和研究设计说明保存在 `analysis_basic_v1/outbound_casewise_rdit_v1/`。
- 设计审计结论：0724 恢复的方向重分配系数在三种带宽下稳定为正，是当前最可用的探索性事件；0723 恢复不稳定；0723 开通只有 12 个同 geometry 的 pre 分钟；0724 开通前后更换视频和 geometry 且存在缺口，不作为因果 RDiT。当前材料不足以支持正式因果 RDD，后续需同机位、同 geometry 的多日同钟点窗口或同步对照。
- 按用户后续指定的“同为出城机位即可”口径，新增 `flowvedio/analyze_outbound_casewise_rdit_all_devices_v2.py`，不再以设备或 geometry 变化排除观测。0723 开通的60分钟 pre 样本由12增至41，净方向断点系数由 -48.16 变为 0.70 (SE 9.01, p=0.938)；45分钟结果同样接近零，30分钟仍因操作 donut 仅有13个 pre 分钟而不稳定。v2 输出在 `analysis_basic_v1/outbound_casewise_rdit_all_devices_v2/`。

This file records completed work, locked decisions, and retained outputs. `nextstep.md` is the live plan; this file is the running log.

## 2026-08-25 July 24 outbound directional-flow time series

- Added `flowvedio/plot_20260724_outbound_direction_timeseries_v1.py` and exported separate northbound and southbound total-flow figures (PNG, SVG, and PDF) to `flowvedio/batch_outputs/20260723_24_state_dependent_recognition_v5/analysis_basic_v1/outbound_tidal_region_opening_20260724_v1/`.
- The figures use complete five-minute bins on a common 0--75 vehicles/minute scale. They mark the exact last-30-minute pre-opening window, the recording gap, the 2-minute-15-second operation tail, and the first stable 30-minute post-opening window.
- Retained adjacent-window means are 40.5 to 54.0 vehicles/minute northbound and 43.4 to 28.6 vehicles/minute southbound. The plotted observations were also exported as `direction_total_flow_timeseries_plot_data_v1.csv`.

## 2026-08-25 彩田路 0723/0724 出城摄像头 RDiT specification search v1

- 新增脚本 `flowvedio/analyze_outbound_rdit_specification_search_v1.py`，独立输出到 `flowvedio/batch_outputs/20260723_24_state_dependent_recognition_v5/analysis_basic_v1/outbound_rdit_specification_search_v1/`，未覆盖旧 RDiT 结果。
- 共保留 37,560 条可估计结果。规格覆盖 1/2/5 分钟 bin、10/15/20/30/37/45/60 分钟带宽、0/2/5 分钟 stable donut、0/1/2 次局部多项式、共同/两侧趋势、uniform/triangular kernel 和 HAC/HC1 标准误。
- Outcome 包括两方向总流量、双向总流量、北/南固定区域、潮汐可变区域、方向差、固定区差、control-style 组合差、北向份额和 log 方向流量比。普通几何可以进入总流量 outcome，但区域 outcome 设为 missing 而不是补零。
- 0724 开通的 60 分钟窗口有 37 个 pre bins。缺口来自 16:07:43–16:26:46 无录像及之后被删除的操作尾段，不是设备限制。
- 与旧 v2 完全同口径的 1 分钟、60 分钟、局部线性两侧趋势、uniform-kernel HAC 结果已逐事件精确复核。0724 开通为北向 +14.588、南向 -7.346、北向减南向 +21.934；0724 恢复为北向 -11.221、南向 +3.543、北向减南向 -14.764。0723 开通的总流量断点接近零。
- 输出包含逐 outcome 事件研究图和关键 outcome 带宽图，共 56 组 PDF，并同步保留 PNG 与 SVG。大量规格带来多重检验风险，因此不按最小 p 值挑选结论；稳定性汇总限制为局部常数/线性且断点两侧至少 8 个 bins。
- 2026-08-26 新增独立单车道流量 supplement：脚本 `flowvedio/analyze_outbound_rdit_per_lane_supplement_v1.py`，输出为相邻目录 `outbound_rdit_per_lane_supplement_v1/`。共保留 34,920 条估计和四组事件研究图。0724 开通时北向每车道流量基准跳变为 +0.474（p=0.544），获益方向减另一方向每车道流量为 -0.086（p=0.927）；0724 恢复时获益方向每车道流量为 -0.916（p=0.059），相对差为 -1.218（p=0.146）。单车道结果不提供独立的正向政策证据，定位为容量利用率分解，不替代总流量 reduced form。

## 2026-08-04 Raw-road-supported centerline topology diagnostic

- Retained `data_work/outputs/manual_centerline_rules_v11/` as an immutable source. The earlier `centerline_endpoint_bridge35_v1` experiment was also left unchanged.
- Added `data_work/src/diagnostics/diagnose_centerline_rawpath_topology.py`. It reconstructs the 145 weak components under the retained 35 m endpoint snap, builds an undirected geometric support graph from 23,799 baseline raw segments, and searches for a raw-road-supported route from each non-LCC component to the LCC.
- Wrote the candidate-only package to `data_work/outputs/centerline_rawpath_topology_candidates_v1/`. The package contains 19 admissible candidates, classified mechanically as 3 high-, 8 medium-, and 8 low-confidence cases. No candidate was added to the production centerline, assigned a speed, or passed to the QSM pipeline.
- Added local review panels for all 19 candidates and a tier evaluation. Accepting the high-confidence tier mechanically would reduce weak components from 145 to 142. Accepting the high- and medium-confidence tiers would reduce them to 134. The all-candidate scenario would yield 126 components.
- None of the candidate tiers adds a square-grid directed adjacency pair; the retained share remains 36.87 percent (4,778 directed pairs). Inspection of `stage06_build_grid_links.py` explains this null result: grid edges are constructed from each speed-bearing centerline's grid-crossing sequence, without exploiting road-level component connectivity. Most endpoint repairs occur within a grid and therefore cannot alter the current centerline-grid travel-cost input.
- Retained decision: do not promote these candidates into a new centerline baseline solely to reduce the component count. Further automatic work should target raw-supported gaps that add a cross-grid edge or repair a paper-facing centerline map. Parallel carriageways, grade separation, and legal connectivity still require review before any geometric candidate is accepted.
- The first attempted run stopped during LineString merging. Its partial, non-result directory was preserved as `data_work/outputs/centerline_rawpath_topology_candidates_v1_failed_20260804_01/`; no source file was modified.

## 2026-08-04 Centerline cross-grid gap and robustness tiers

- Added `data_work/src/diagnostics/diagnose_centerline_crossgrid_gaps.py` and wrote the candidate-only output to `data_work/outputs/centerline_crossgrid_gap_candidates_v1/`. The retained `manual_centerline_rules_v11` source was not modified.
- Distinguished 4,778 geometric directed square-grid crossings from 4,632 speed-bearing AM stage06 edges. Raw roads support 4,610 directed rook crossings; 41 raw-supported pairs are absent from the effective AM edge set. Thirty pairs retain centerline geometry but lack an effective speed edge, while eleven also lack centerline geometry.
- Forty of the 41 candidates have complete AM, PM, and free-flow raw-speed support. Candidate ranking combines raw support length, speed coverage, centerline matching, road class, direct observed road commuting, and current-network detour. Local review panels were exported for the top 40 candidates.
- Added `data_work/src/diagnostics/build_centerline_crossgrid_robustness_tiers.py` and wrote provisional edge tables to `data_work/outputs/centerline_crossgrid_robustness_tiers_v1/`. Tier A contains 21 fully speed-supported, fully centerline-matched `speed_missing` pairs. Tier B adds eleven fully matched `geometry_missing` pairs. Tier C contains all 40 fully speed-supported pairs, including unmatched raw roads.
- Tier A raises the effective-network LCC from 1,774 to 1,780 nodes. Tier B merges the two weak components and raises the LCC to 1,782 nodes. Tier C adds one further node. The road-commuter same-component share moves from 99.829 percent to 99.841 percent in Tier B and 99.842 percent in Tier C.
- Added an all-OD shortest-time diagnostic. Relative to the retained centerline network, Tier A lowers the road-commuter-weighted shortest time by 0.336 percent and shortens travel for 11.1 percent of road commuters. Tier B yields 0.358 percent and 11.7 percent; Tier C yields 0.442 percent and 14.9 percent. Among affected OD pairs, the median reduction is 2.13 minutes.
- Retained decision: Tier A and Tier B are suitable candidates for a future centerline robustness bundle, not replacements for the raw-road gate5 mainline. Tier C is diagnostic because it admits unmatched raw roads. No active model configuration references these provisional tables.
- The first robustness-tier run inherited the geographic grid CRS and produced no support rows. Its partial directory was preserved as `data_work/outputs/centerline_crossgrid_robustness_tiers_v1_failed_20260804_01/`. The formal v1 converts the grid to the projected raw-road CRS before measuring crossings and centroid distance.

## 2026-08-04 Gate5 free-flow speed robustness

- Added `data_work/src/diagnostics/build_freeflow_speed_definition_caches.py` and retained five alternative road-segment free-flow caches under `data_work/outputs/freeflow_speed_definitions_v1/`. The active 22:00--05:00 all-day harmonic cache was not modified.
- The alternatives are 00:00--05:00 all-day harmonic, 01:00--04:00 all-day harmonic, 22:00--05:00 weekday harmonic, 22:00--05:00 weekend harmonic, and the all-day 22:00--05:00 90th percentile. Relative to the retained road-segment cache, their median speed ratios are 1.0068, 1.0105, 1.0003, 0.9993, and 1.0885.
- Reran square-AM pair-specific edge projection for all five definitions with the retained connector and routing settings. AM observed route time and route length are identical across definitions. The edge-level median free-flow time ratios are 0.9930, 0.9885, 0.9996, 1.0010, and 0.9128.
- Built five separate gate5 LCC model-input bundles. Every bundle contains 1,104 nodes, 2,642 edges, and 310,525 OD pairs, passes bundle QA, retains one weak component, respects the detour gate, and preserves the lane fields. No active configuration points to an alternative bundle.
- The retained harmonic definition has 8.29 percent of edges with AM time below the estimated free-flow time. The narrow harmonic and weekday/weekend definitions leave this share near 8.0--9.2 percent. The 90th-percentile definition reduces it to 0.26 percent and raises the median congestion ratio from 1.149 to 1.268.
- Reran the anchored Stage 2 quick top-25 tidal-lane suite for the retained definition and all five alternatives. Every baseline and counterfactual converged. Short-run welfare changes range from 0.439 to 0.469 percent, compared with 0.438 percent under the retained definition. Long-run welfare changes range from 0.143 to 0.148 percent, compared with 0.142 percent. Top-25 overlap with the retained selection is 23--25 pairs.
- Retained decision: the current 22:00--05:00 harmonic definition remains the mainline because the narrow-window and weekday/weekend alternatives deliver negligible changes. The 90th-percentile definition is retained as a strong robustness specification; it materially changes the congestion level but does not alter the welfare sign or policy-selection conclusion.
- Summary tables, edge-level comparison data, a figure, and replication notes are stored in `data_work/outputs/freeflow_robustness_gate5_quick_v1/`.

## 2026-07-27：记录 directional cost-response / corridor validation 想法

- 新增 `Documents/L2_notes/tau_cost_response_corridor_validation_idea_20260727.md`。
- 锁定表述：该思路是校验「潮汐/方向性容量冲击 → 成本变化」并接到真实动线设计，不是识别 \(\tau_{ij}\) 水平；全国可用 2024/2025 百度城样本，必要时走廊/区县加总；深圳已有潮汐车道作对照。
- 可执行分包：A 深圳已开通走廊 cost-proxy 对照（最小起步）；B 全国拥堵指数–不对称散点；C 北京模型 \(\Delta\tau\) 与走廊评分交叉核对。`nextstep.md` 低优先级项 #10 已挂接。

## 2026-07-27：彩田路 0723 晚间与 0724 实施窗口人工锁定

- 根据用户逐视频复核，锁定 0723 晚间恢复：进城 `IMG_3026.MOV` 片内 01:15:00–01:32:06，出城 `IMG_0019.MOV` 片内 01:20:00–01:35:00；`IMG_0018.MOV` 因仅 13.3 秒正式排除。
- 锁定 0724 进城开通为 `IMG_0006.MOV` 片内 01:34:00–01:47:20；`IMG_0022.mov` 从开头即为完成状态。出城开通开始仍处于缺失区间，`IMG_3028.mov` 片内 00:02:15 为首次调整后车流。
- 锁定 0724 晚间恢复：进城 `IMG_0008.MOV` 片内 01:30:50–01:40:00，出城 `IMG_3029.mov` 片内 01:31:25–01:39:50。
- `IMG_0008.MOV` 前 5 秒排除；片内 01:09:14–01:09:30 剧烈晃动排除，晃动前后使用不同 geometry。`IMG_3029.mov` 前 5 秒排除。
- 正式状态表为 `flowvedio/batch_outputs/20260724_caitian_transition_review_v1/lane_state_plan_20260724_v2.csv`；0723 晚间表为 `flowvedio/batch_outputs/20260723_caitian_point_review/lane_state_plan_20260723_evening_v1.csv`。22 张人工定点网格图和空白流量线表位于 `flowvedio/batch_outputs/20260723_24_confirmed_manual_locator_v2/`。

## 2026-07-27 Repository cleanup and backup workflow

- Removed empty or superseded top-level items: `Code/` (empty), `tmp/` (slide-rendering intermediates), `.codex_tmp_manual_flow/`, `compile.log`, `texput.log`, `tidal_model_slides_minimal.zip`, `data_work/configs/` (a March placeholder superseded by `data_work/config/`), and `data_work/data_work/` (a nested empty shell). Items were moved to `~/.Trash/TidalLanes_cleanup_20260727/` rather than deleted outright.
- Merged `Outputs/figs/` (eight PDFs) and `output/pdf/` (one PDF) into `Documents/L3_figs/` and removed both directories, eliminating a case-only collision that is unsafe on a case-insensitive filesystem. No filename conflicts existed.
- Deleted 80 `.DS_Store` files. The repository root now lists 16 visible entries, down from 27.
- Added a `Repository layout` section to `AGENTS.md` recording what each top-level directory holds and whether its contents are reproducible. Directory names are retained as-is because several hundred hard-coded paths in `nextstep.md`, this log, and `Documents/L2_notes/` depend on them.
- Added `data_work/docs/BACKUP_WORKFLOW.md` and `data_work/scripts/backup_to_drive.sh`. Backup coverage is restricted to the irreplaceable sets (`data_work/raw_data/`, `flowvedio/`, `fieldvisit/`, `Documents/PreSlides/`); `data_work/outputs/` is excluded as reproducible. The script defaults to a dry run and never deletes at the destination.
- Drive audit: the nine dated snapshots under `project备份/` total roughly 472 GB nominal. `Tidal0723` is an interrupted copy whose `Documents/`, `Replication_apply/`, and `data_work/src/` are empty or absent and which contains no video. Its populated directories were verified as strict subsets of the local tree, so no snapshot holds unique content. The Caitian Road video has never been backed up.
- The drive is exFAT with a one-megabyte allocation unit, which pads small files: a 4 MB output directory of 284 files occupies 339 MB there. Reformatting to APFS is deferred until the contents are consolidated.

## 2026-07-27 Caitian Road three-region manual re-marking package

- Replaced the prior two-direction split with three fixed physical road regions: fixed northbound, tidal-variable, and fixed southbound. Each camera geometry now uses two transverse gates, with two y coordinates and four x boundary coordinates per gate.
- Determined that the 2026-07-23 afternoon results must also be re-marked because the previous single split point cannot isolate the tidal-variable lane from the two fixed-direction regions.
- Initially created the manual package as `flowvedio/batch_outputs/20260723_24_three_region_manual_review_v4/`, then renamed the retained current package to `flowvedio/batch_outputs/20260723_24_state_dependent_manual_review_v5/`. It contains 18 full-resolution gridded reference images (`M01`–`M18`), a contact sheet, a UTF-8-BOM inventory, and a Chinese instruction file. The geometry list covers all retained videos and separately handles the `IMG_0017.MOV` camera adjustment and the `IMG_0008.MOV` camera shake.
- Created `three_region_manual_marking_form_v4.xlsx` with five sheets: start/checklist, three-region coordinate inputs, 36 implementation-state intervals, full video-to-geometry mapping, and coordinate instructions. Input validation checks coordinate completeness, image bounds, x ordering, and left/right physical-region labels.
- Retained implementation-period vehicle recognition as a separate sample. Ordinary and operation vehicles must be distinguished, and implementation observations must not be pooled into stable operating windows.
- Locked exclusions remain unchanged: `IMG_0018.MOV` is ignored; the 2026-07-24 outbound 16:07:43.048–16:26:46 interval remains missing; the specified `IMG_0008.MOV` and `IMG_3029.mov` camera-unstable seconds remain excluded.
- Corrected the output rule after user review. Ordinary periods without an open tidal lane are aggregated only into northbound and southbound flows. Separate tidal-lane flow is reported only from implementation start through tidal operation; after recovery to the ordinary configuration, flows are again aggregated by direction.
- Superseded the all-three-region v4 input workbook with `state_dependent_manual_marking_form_v5.xlsx`. The corrected workbook has five direction-only geometries using two y and six x inputs, thirteen implementation/tidal geometries using two y and eight x inputs, and a state-level output-mode column that identifies direction-only, three-region, and no-output intervals.
- Moved the superseded v2, v3, and v4 manual-input workbooks, their CSV/inspection sidecars, the temporary Excel lock file, and the current workbook's inspection sidecar to `~/.Trash/TidalLanes_manual_forms_cleanup_20260727/`. Raw videos, all reference grids, all `C01`–`C22` implementation snapshots, the contact sheet, the current v5 workbook, and historical analysis outputs were preserved.
- Repaired the `M01`–`M18` reference grids after the user reported obscured coordinate labels. The image dimensions and original pixel origin were preserved; high-contrast x labels were redrawn immediately below the title strip and y labels were redrawn along the left edge. The contact sheet was regenerated, and the pre-repair grids were backed up to `~/.Trash/TidalLanes_manual_grid_backup_before_label_fix_20260727/`.

## 2026-07-27 Documentation consolidation and index

- Cleared 180 LaTeX build artefacts from `Documents/` and `data_work/docs/`, and 13 redundant slide backup PDFs (258 MB). Both sets went to the trash rather than being deleted outright. The PDFs recompile from the retained sources.
- Archived eleven documents to `data_work/docs/_superseded/`: the March refactor set (`REFACTOR_BLUEPRINT`, `PIPELINE_MIGRATION_PLAN`, `STAGE_IO_SPEC`, `REPRODUCIBILITY_SNAPSHOT`, `DATASET_SCHEMA_SNAPSHOT`), the prototype model line (`CURRENT_MODEL_EQUATIONS`, `MODEL_REFRESH_2026-03-21`, `STRUCTURAL_MODEL_WORKFLOW`), plus `MANUAL_MATCH_OVERRIDE_WORKFLOW`, `CODEX_HANDOFF_NOTE`, and `TEMP_STAGE02_PROJECTION_DEBUG`. The two `model_v1_*_temp_*` checklists moved to `Documents/L2_notes/_superseded/`.
- Moved 27 manual slide snapshots and three milestone PDFs into `Documents/PreSlides/_snapshots/`, with `SNAPSHOT_MANIFEST.md` recording which edit each snapshot preceded. `Documents/PreSlides/` fell from 163 entries and 948 MB to 44 entries and 689 MB.
- Added `DOCUMENT_INDEX.md` at the repository root as the single catalogue of every prose document, its status (authoritative, active, or historical), and which document is authoritative for each contested number. `data_work/docs/README.md` and `Documents/README.md` were rewritten to point at it instead of maintaining parallel lists; the former had listed only ten March documents out of thirty-two.
- Repaired twelve cross-references that the archiving would otherwise have broken, in `nextstep.md`, `data_work/README.md`, `NEW_MACHINE_SETUP_CHECKLIST.md`, `MODEL_AUDIT_LEARNING_GUIDE_20260628.md`, both copies of `ESTIMATION_NOTES.md`, and two L2 handoffs. No dangling reference remains.
- Active technical memos were deliberately not merged. A survey found eleven decisions that reverse across documents, including the long-run welfare sign after the 2026-07-01 gauge correction, the switch from `asym` to `score_bpr_st` corridor selection, and the person-minute totals. Merging would require adjudicating each number, and an error there would propagate into the paper. The index records which source is authoritative instead, and the superseded values are flagged in place.
- `flowvedio/batch_outputs/` was added to the backup set. It is regenerable in principle but costs tens of GPU-hours of YOLO tracking, so 4 GB of storage is the cheaper side of that trade. The backup set is now 2,444 files and 265 GB.

## 2026-07-26：彩田路 0724 视频时间与状态窗口首轮整理

- 完成 `flowvedio/2026072324彩田路/` 的视频时间、方向和连续覆盖核对。`IMG_0022.mov` 填补进城方向 16:30–17:30；`IMG_0020.mov` 已完成导入并覆盖出城方向约 14:42–16:07。
- 出城方向 16:07:43–16:26:46 暂按缺失处理，不插值、不记为零流量。约 18:20–20:00 为计划停采，两个方向的恢复过程均不可直接定位。
- 新增可复现抽帧脚本 `flowvedio/create_20260724_transition_review.py`，输出在 `flowvedio/batch_outputs/20260724_caitian_transition_review_v1/`，包含分钟级、10 秒级开通审核图和恢复前后审核图。
- 新增 `lane_state_plan_20260724_v1.csv` 与 `transition_window_inventory_20260724_v1.csv`。进城开通保留为待车流进一步定位的候选窗口；出城开通和双向恢复均按区间删失处理。
- 锁定识别限制：不同手机、不同机位或不同 geometry 的 series 不直接拼成窄带 RDD / RDiT。当前先用同一机位内车流定位状态边界，并只对稳定窗口做描述性比较。

## 2026-07-24 Caitian Road full measurement and exploratory RDiT

- Processed all nine stable camera-state segments from the 2026-07-23 collection with YOLO and ByteTrack. The retained output contains 60,433 single-gate crossings and 26,186 matched two-gate trajectories.
- Corrected the geographic-direction mapping identified during cross-camera QA. The outbound view maps image-left to south-to-north; the inbound view maps image-left to north-to-south. Pixel coordinates and trajectory matches were retained, and the inbound tracking labels were swapped during geographic aggregation.
- Built 1-, 2-, and 5-minute panels for directional entry flow, median two-gate travel time, and visible vehicles per lane. Bins with less than 90 percent stable-video coverage are excluded from RDiT.
- Cross-camera agreement is substantial after the direction correction. Across 36 overlapping stable 5-minute bins, flow correlations are 0.634 for south-to-north and 0.985 for north-to-south; travel-time correlations are 0.857 and 0.916. The inbound camera records approximately 9.4 percent more entry crossings in median ratio, so views are treated as repeated measurements rather than summed.
- The fixed-window comparison averages camera-specific estimates over stable pre minutes [-45,-15] and post minutes [0,30]. South-to-north flow rises from 37.43 to 52.98 vehicles per minute and median travel time falls from 11.10 to 3.45 seconds. North-to-south flow falls from 40.49 to 22.62 vehicles per minute and median travel time falls from 3.36 to 2.33 seconds.
- Exploratory local-linear RDiT estimates were generated for multiple bin widths and bandwidths with HAC standard errors. The nearest stable pre observations are 19 to 24 minutes from deployment completion because the construction interval is excluded. Coefficients are bandwidth-sensitive and differ across views, so single-day p-values are not retained as formal causal evidence.
- Outputs, figures, variable definitions, sample lineage, specification matrix, and replication notes are retained under `flowvedio/batch_outputs/20260723_caitian_empirical_v1/analysis_v1/`.
- Locked the outbound camera as the primary empirical view because the inbound view is exposed to the south-to-north bridge bottleneck. In low inbound vehicle-stock bins, south-to-north flow correlation across cameras is 0.969 and the mean absolute gap is 2.47 vehicles per minute. In high-stock bins, correlation falls to 0.739 and the mean absolute gap rises to 20.26 vehicles per minute, consistent with spatial vehicle storage and release between measurement sections plus congestion-related tracking loss.
- Reprocessed both deployment intervals with full-road transverse gates, retaining 2,400 additional matched two-gate trajectories. The RDiT figures now display construction-period observations as purple diamonds inside shaded deployment windows. Stable pre and post observations continue to determine the local-linear fits because lane allocation is not binary during construction.
- Added a publication-format two-panel RDiT figure restricted to the primary outbound camera and the treated south-to-north direction. Panel A reports entry flow and Panel B reports median two-gate travel time; PNG, PDF, and editable SVG exports are retained as `analysis_v1/figures/08_rdit_added_direction_outbound.*`.

## 2026-07-23 Caitian Road video inventory and geometry review package

- Catalogued six videos in `flowvedio/20260723彩田路/` using the recorded creation timestamps and actual media durations. The retained inventory covers two camera views from approximately 14:41 to 18:17.
- Split `IMG_3024.MOV` and `IMG_0017.MOV` into normal, deployment, and tidal-lane operating states. The two views place completed reallocation at 16:26:17 and 16:26:34. The deployment intervals remain view-specific because their visible start times differ.
- Recorded the capacity reallocation as south-to-north 3 to 4 lanes and north-to-south 4 to 3 lanes. Deployment intervals are excluded from stable-state measurement.
- Created nine full-resolution raw frames and nine 100-pixel grid overlays in `flowvedio/batch_outputs/20260723_caitian_point_review/`. The geometry set separates each file transition, the pre/post lane configurations, and the `IMG_0017.MOV` camera adjustment at 1:10:08.
- Added a direction-specific confirmation table with two speed gates per direction, lane or flow point fields, and physical gate-distance fields. Travel speed will not be reported in km/h until the physical longitudinal distance between each gate pair is confirmed.
- Locked the nine user-confirmed gate geometries. Each transverse gate is split into south-to-north and north-to-south segments at the supplied center point. The retained table contains 18 direction-specific gate pairs, and the review package includes nine annotated full-resolution frames plus a contact sheet.
- Physical gate distances remain pending. The locked geometry supports vehicle passage-time measurement; conversion to km/h is deferred until the corresponding longitudinal road distances are supplied.

## 2026-07-17 0709 three-window speed-density comparison

- Added `flowvedio/scripts/run_20260709_speed_density_stable_window.sh` and `flowvedio/analyze_20260709_speed_density_three_windows.py`. The new extraction covers 33:30-48:30, beginning 30 minutes after lane operation started, and retains the confirmed post-treatment gates and direction-specific ROIs.
- The later window contains 13,492 processed frames. South-to-north retains 655 dual-gate tracks after QA, with median gate travel time 8.87 seconds and mean visible stock 10.57 vehicles. Its directional flow is 3,284 vehicles per hour, or 821 vehicles per lane-hour.
- Relative to the immediate 3:30-8:30 window, south-to-north total flow and flow per lane are nearly unchanged (+0.24%), median gate travel time is 45.6% lower, and visible stock is 25.6% lower. North-to-south changes are more limited: flow is 4.9% higher, median gate time is 1.5% higher, and visible stock is 5.2% higher.
- Post-window geometry is identical, duplicate speed and gate-crossing keys are zero, and the maximum absolute internal q-k-v gap across six window-direction cells is 21.3%. Five-minute diagnostics qualify the stationarity claim: south-to-north flow is stable, but median gate time rises from 7.56 to 12.74 seconds and visible stock rises from 8.40 to 14.03 vehicles between the first and final five-minute blocks.
- Retained comparison tables, QA, notes, and sample lineage are stored in `flowvedio/batch_outputs/20260709_speed_density_pilot/analysis_three_windows_v1/`. The 33:30-48:30 sample is classified as a later operating window, not a local RDiT estimate or a fully stationary traffic state.

## 2026-07-17 standalone video slide updated to pre versus late post

- Updated `Documents/PreSlides/TidalLane_Video_RDiT_2slides_20260715.tex` to compare the 0:00--2:45 pre window with the 33:30--48:30 later operating window. The images are labelled only Pre and Post; sample windows, processing details, and track counts are retained in the Data notes.
- The result page reports flow and fixed-gate travel time for both directions. South-to-north flow is 2,902 versus 3,284 vehicles per hour (+13.2%), and median travel time is 18.73 versus 8.87 seconds (-52.6%). North-to-south flow is 1,484 versus 1,712 vehicles per hour (+15.4%), and median travel time is 3.49 versus 3.90 seconds (+11.8%).
- The page-level takeaway is that flow rises in both directions, whereas travel time falls only in the capacity-gaining direction. Visible vehicle stock remains outside the headline comparison because treatment-state road ROIs differ across the lane reallocation. The one-day contrast remains descriptive, not an RDiT estimate.
- Enlarged and cropped the two tracking frames to emphasize the observed road area, and compacted the two-direction flow and travel-time table below them. Detailed windows, matching counts, and processing qualifications remain in the Data notes.
- Rebuilt the identification page as a single-column opening-time RDiT slide. It now retains one local-linear equation, the local treatment estimand, one fuzzy-timing sentence, two identifying conditions, and a concise repeated-date implementation statement.
- Compiled and visually verified the two-page PDF with no LaTeX layout warnings. The dated presentation copy is retained at `output/pdf/TidalLane_Video_RDiT_2slides_20260717_pre_vs_late_post.pdf`.

## 2026-07-17 Tidal0715 concise link-use definition and direct cost anchor

- Shortened the visible definition of $\kappa_{ij}^{kl}$ on Slide 13 to “expected use of directed link $(k,l)$ by an $i\to j$ commuter.” The equation and the fuller speaker explanation remain unchanged.
- Rewrote equation (15) entirely in link-cost notation:
  \[
  t_{kl}=\max\!\left\{t_{kl}^{ff},\;t_{kl}^{obs}
  \left[(\Xi_{kl}/\Xi_{kl}^{0})(n_{kl}^{0}/n_{kl})\right]^{\lambda/\theta}\right\}.
  \]
  This expression is algebraically equivalent to updating physical minutes with exponent $\lambda$ and applying $t=m^{1/\theta}$, but it keeps $m_{kl}$ off Slide 17.
- Synchronized the Slide 17 speaker script, compiled the 43-page PDF, and inspected Slides 13 and 17 at full size. The LaTeX log contains no overfull boxes or undefined references.

## 2026-07-17 Tidal0715 appendix audit and 25-minute script rebuild

- Backed up the active TeX, PDF, and speaker script before the appendix and script revision as `Tidal0715_backup_before_appendix_script_rebuild_20260717.*` and `tidal_speaker_script_Tidal0715_backup_before_appendix_script_rebuild_20260717.md`.
- Reduced the appendix from 18 to 14 pages. Removed the outdated fixed-fundamentals symmetry diagnostic, the unlinked model-implied asymmetry page, and a duplicate sensitivity-details page. Merged validation and field-visit material into one page.
- Rebuilt the estimation algorithm as a baseline-inversion and counterfactual-fixed-point comparison; consolidated parameter interpretation into one table; combined the alpha and beta sensitivity results; and simplified the policy definitions table.
- Replaced the unconditional full-system uniqueness language with a separation between the Allen--Arkolakis core conditions and the numerical solution of the complete anchored directed-congestion implementation.
- Added main-slide links from bilateral costs, equilibrium, and physical-time results to the relevant appendix pages. Corrected the treated-link offsetting-loss expression to match the sign of the reported incidence entries.
- Rewrote `Documents/PreSlides/tidal_speaker_script_Tidal0715_20260717.md` around a four-step model narrative: familiar QSM location core, OD-route choice, endogenous directed-link congestion, and the separation of physical minutes from iceberg and bilateral cost indices. The script now explains where the model differs from a standard OD-cost QSM, how raw segment speeds construct $m_{kl}$ and $t_{kl}$, why $\Xi_{kl}^{0}$ is model-implied rather than observed flow, and how the anchored counterfactual is solved.
- Compiled `Documents/PreSlides/Tidal0715.pdf` successfully at 39 pages. The final LaTeX log has no overfull or underfull boxes, undefined references, or undefined citations. All 14 appendix pages and the affected main model/result pages were rendered and visually checked.

## 2026-07-17 Tidal0715 travel-cost alignment and single-flow appendix layouts

- Backed up the active TeX, PDF, and speaker script before the revision as `Tidal0715_backup_before_layout_cost_audit_20260717.*` and `tidal_speaker_script_Tidal0715_backup_before_layout_cost_audit_20260717.md`.
- Simplified Slide 12 by removing the separate parameter-interpretation bullet. Slide 15 now cites only Ahlfeldt et al. (2015) for the main $\theta$ value, moves the external-calibration statement into a tiny note, and removes the baseline sample-size footer.
- Rebuilt Slide 16 around two visuals: the upper route-selection portion of the actual adjacent-grid example and the citywide map of observed directed grid-link minutes. The map label was regenerated as $m_{kl}^{obs}$ rather than the legacy `tau_obs_min` variable name.
- Audited the implemented travel-cost chain. Raw-road and connector minutes use $60\ell/v$; observed and free-flow speeds are evaluated on the same AM-minimizing arc sequence; `t_kl_iceberg` equals $m_{kl}^{1/\theta}$ numerically; the mainline uses $\zeta=1$, so $A_{kl}=t_{kl}^{-\theta}=1/m_{kl}$; and the anchored physical-time update maps algebraically into the displayed link-cost exponent $\lambda/\theta$. The maximum numerical discrepancy in the two $m$-to-$t$ identities was below $2\times10^{-16}$, and the code's closed-form $\Xi_{kl}$ identity matched stored link loads exactly.
- Recorded two implementation qualifications in the appendix. The input field `tau_obs_min` is a legacy code name for physical link minutes $m_{kl}^{obs}$, not bilateral $\tau_{ij}$. In addition, 219 of 2,642 links have $m_{kl}^{obs}<m_{kl}^{ff}$ because the AM and night-window averages cross; the anchored solver applies the free-flow lower bound on those links.
- Rebuilt Appendix Slides 26, 29, 30, 33, 38, and 39 as single-flow layouts without Beamer column environments. Routing settings, treated-link incidence, the estimation algorithm, sensitivity results, validation, and theoretical scope now run vertically with aligned tables, formulas, and images.
- Synchronized `tidal_speaker_script_Tidal0715_20260717.md`, compiled the 39-page PDF, and visually checked the revised main and appendix pages. The final LaTeX log contains no overfull or underfull boxes, undefined references, or undefined citations.

## 2026-07-17 Tidal0715 merged short-run and long-run result page

- Backed up the active TeX, PDF, and speaker script as `Tidal0715_backup_before_result_merge_20260717.*` and `tidal_speaker_script_Tidal0715_backup_before_result_merge_20260717.md`.
- Before the final two-outcome decomposition, backed up the intermediate version as `Tidal0715_backup_before_two_outcome_decomp_20260717.*` and `tidal_speaker_script_Tidal0715_backup_before_two_outcome_decomp_20260717.md`.
- Removed the standalone `Short Run vs. Long Run` frame and merged its adjustment logic into the physical-time and welfare result frame. The main deck now contains 24 pages, followed by a 14-page appendix.
- Replaced the intermediate two-row outcome table with one three-column table containing two decomposition panels. Panel A reports physical time in hours per AM peak: the short-run equilibrium contributes $-1{,}031$, long-run link-time feedback adds $650$, and link-passage reallocation adds $733$, yielding a cumulative long-run change of $+352$ hours.
- Panel B reports a sequential expected-utility decomposition in percentage points. The short-run transport-cost effect contributes $+0.8714$, long-run cost feedback contributes $-0.5470$, workplace sorting contributes $+0.0045$, and residential crowding contributes $-0.0046$, yielding a cumulative $+0.3243\%$ welfare change.
- The welfare components are obtained from the exact $\bar W^\theta$ layers in `welfare_sorting_diagnostics_gate5_bpr_pos05_v1/pos05/wbar_theta_decomposition.json`, converted into telescoping changes in $\bar W$ along the displayed sequence. The decomposition is order-dependent and is not interpreted as a unique Shapley allocation.
- Confirmed from `tidal_lane_diagnostics.py` that the decomposition is constructed from additive physical link minutes and equilibrium link passages. It does not convert the bilateral iceberg-cost index $\tau_{ij}$ into clock time.
- Synchronized the 25-minute timing plan, main-slide numbering, appendix numbering, and result-page narration in `tidal_speaker_script_Tidal0715_20260717.md`. The revised 38-page PDF compiled without overfull or underfull boxes, undefined references, or undefined citations, and the merged result page and its adjacent slides passed full-size visual inspection.
- Backed up the next result-page revision as `Tidal0715_backup_before_indented_LT_totals_20260717.*` and `tidal_speaker_script_Tidal0715_backup_before_indented_LT_totals_20260717.md`. All component rows are now indented beneath Panels A and B. Each panel includes an explicit LT total row: $+352$ physical commuting hours per AM peak and $+0.3243\%$ expected utility relative to baseline.
- The slide note and speaker script now state why workplace and residential sorting can be introduced separately. Conditional on final LT commuting costs, workplace shares enter the productivity term and residential shares enter the crowding term. This sequential split closes exactly for the displayed order but is not unique across alternative orderings. The 38-page PDF compiled cleanly, and Slide 21 passed full-size visual inspection after the added rows.
- Backed up the deck as `Tidal0715_backup_before_remove_cumulative_20260717.*`, removed the cumulative-change column from Slide 21, and retained a single change column for the channel contributions and explicit LT totals. The subtitle now identifies incremental components and LT totals. The 38-page PDF compiled cleanly, and the revised two-column table passed full-size visual inspection.

## 2026-07-17: Recorded additive-delay and route-elasticity extensions

- Added `Documents/L2_notes/additive_delay_and_route_elasticity_note_20260717.md` as a retained model memo.
- Recorded `anchored_additive_delay` as a priority parallel robustness specification, including the baseline-anchor caveat for links with observed time below the night-based free-flow measure.
- Recorded that the current mapping \(t=m^{1/\theta}\) makes the physical-time route kernel independent of \(\theta\), so the current model does not identify \(\theta\) from an ordinary OD travel-time PPML.
- Retained the proposed sequence: first compare current all-route loads with a concentrated route-assignment benchmark; introduce separate \((\rho,\kappa,\theta)\) parameters only if policy results are sensitive.
- No model code, data, parameters, slides, results, or `nextstep.md` were changed.

## 2026-07-17 Tidal0715 video evidence and opening-time RDiT appendix

- Added two appendix slides after `Validation and Extensions`: a video-based pre/post comparison and the proposed opening-time RDiT design.
- Rebuilt the first slide with the deck's existing flat layout, retaining the two tracking frames, directional flow and matched travel-time estimates, and an explicit one-day descriptive qualification.
- Shortened the second slide's identification discussion to a predetermined-cutoff condition and a continuity condition. The local linear specification, repeated-date implementation, and fuzzy RDiT fallback remain visible.
- Added named navigation from the Motivation video and the Validation appendix to the new evidence slide, plus return links and an evidence-to-design link.
- Synchronized `tidal_speaker_script_Tidal0715_20260717.md`, compiled the 41-page PDF, and inspected Slides 4 and 38--41 at full size. The LaTeX log contains no overfull or underfull boxes, undefined references, or undefined citations; all new named destinations resolve to their intended pages.

## 2026-07-17 Tidal0715 estimation and grid-link measurement graphics

- Renamed Slide 15 to `Estimation` and synchronized the slide title in the speaker script.
- Restored Slide 15 to the earlier object-to-source table requested by the user. The restored specification reports the 07:00--09:00 weekday AM window, the 22:00--06:00 all-day free-flow window, and the original parameter roles and sources.
- Rebuilt the two-figure layout on Slide 16 with equal-width columns. The selected-path panel now uses its vector PDF, and the network panel uses a slide-specific vector map focused on the 2,642 retained directed grid-links.
- Added `data_work/src/diagnostics/plot_slide_observed_grid_links.py` as the reproducible source for the network panel. It reads the locked `model_input_edgeproj_v2_lcc_gate5_v1` bundle and exports PDF and 600-dpi PNG files.
- Retained the implemented endpoint-access rule: connector distance is divided by the period-specific speed of the directed host road edge. The chosen origin and destination access terms enter the same observed-AM shortest-path problem as the partial host-edge and intermediate road-segment minutes.
- Compiled the 40-page PDF and inspected Slides 15 and 16 at full size. The LaTeX log contains no overfull or underfull boxes, undefined references, or undefined citations.

## 2026-07-17 AA-style commuting OD validation

- Added `data_work/src/model/full_estimation/run_od_validation_aa_style_gate5.py` and generated `data_work/outputs/od_validation_aa_style_gate5_bpr_pos05_v1/` from the locked gate5 mainline.
- Matched the Allen and Arkolakis commuting validation procedure by projecting observed and predicted log OD flows separately on origin and destination fixed effects. The bundled Seattle data reproduce a residual correlation of 0.433501, compared with the paper's reported 0.4335.
- The current endogenous structural OD prediction has a residual correlation of 0.412534 with observed Beijing road commuting across 83,030 positive observed pairs. The exogenous-baseline result is 0.412490.
- Applying equation (35) to baseline model-implied link traffic gives a residual correlation of 0.326901 across 72,787 pairwise observations. This is an internal consistency check because the Beijing link traffic object is model-implied rather than observed AADT.
- The speed-implied traffic proxy gives a correlation of 0.330520, compared with 0.330712 for the lanes-only placebo. The current congestion proxy therefore adds no detectable pair-level fit beyond the network and lane structure in this inversion.
- The residual-fit slopes are 0.082 for the structural prediction and 0.022 for equation (35). The positive correlations measure bilateral sorting after fixed effects, not one-for-one calibration of residual flow magnitudes.
- Numerical and replication checks passed, including OD-marginal closure, fixed-effect orthogonality, sample-universe consistency, and the AA reference reproduction. The full-range and central-99-percent figures were exported as SVG, PDF, and 600 dpi PNG files and visually inspected.

## 2026-07-17 Tidal0715 bilateral OD fit appendix slide

- Added Appendix Slide 31, `Bilateral OD Fit beyond Marginals`, to `Documents/PreSlides/Tidal0715.tex`. The page is linked from the baseline-inversion step on Slide 17 and returns to that page.
- The slide retains one structural OD scatter plot. It defines separate origin and destination fixed-effect projections for observed and predicted log commuting flows and reports a residual correlation of 0.413 across 83,030 pairwise observations.
- The interpretation distinguishes bilateral pattern fit from unit-slope calibration. The baseline inversion matches residence and workplace marginals rather than the bilateral OD matrix; the remaining residual association therefore evaluates pair-specific commuting variation beyond origin and destination scale.
- Updated `Documents/PreSlides/tidal_speaker_script_Tidal0715_20260717.md` with the appendix talk track and revised appendix numbering. The final deck contains 41 pages, compiles without overfull or underfull boxes, undefined references, or undefined citations, and Slides 17 and 31 passed full-size visual inspection.
- A follow-up layout revision removed the two-column structure and the numerical AA comparison. The plotting script now exports a standalone structural-OD panel, so the slide PDF no longer contains clipped text from the equation (35) panel. Slide 31 retains one calculation sentence, one interpretation sentence, and the full-sample correlation note.

## 2026-07-17 Tidal0715 estimation algorithm aligned with the physical-time solver

- Audited the main Estimation equation and Appendix Estimation Algorithm against `compute_observation_anchors`, `solve_equilibrium`, and `run_tidal_endogenous`.
- Replaced the algebraically equivalent direct update of link cost $t_{kl}$ with the implemented sequence: initialize physical minutes at $m^{obs}$, recompute model link traffic $\Xi$, update $m$ with exponent $\lambda=1.1$, apply log damping with $\omega=0.25$, and map converged minutes into $t=m^{1/\theta}$.
- Clarified that the inversion matches residence and workplace marginals rather than the full bilateral OD matrix, $\Xi^0$ is a model-implied baseline link load, and the congestion mapping imposes $\widehat q=\widehat\Xi$. The implied baseline $q^0$ is eliminated algebraically in the implemented anchored equation.
- Synchronized the corresponding speaker-script passages, compiled the 41-page PDF without overfull boxes or undefined references, and visually inspected the revised main and appendix estimation pages.

## 2026-07-16: Jobs-housing direction measure unified

- The main jobs-housing slide now uses the 30 percent employment-core measure for both the top-20 comparison and Beijing. The Beijing bullet reports only the 9.4-percentage-point peripheral-to-core gap; the Duncan index remains unchanged.
- Removed the distance-threshold radial appendix slide and its 74.5 percent inbound share and 2.9 inbound-to-reverse ratio. The current deck has 25 main slides and 18 appendix slides, for 43 pages in total.
- Synchronized the speaker-script appendix map and rendered the revised main slide plus the adjacent appendix pages. The PDF compiles without overfull boxes, underfull boxes, or undefined references.

## 2026-07-16: Short-run and long-run slide returned to the bilateral-cost decomposition

- Replaced the physical-link-time identity on Slide 21 with the earlier OD-share-weighted bilateral-cost decomposition. The retained object is $\bar\tau^h=\sum_{ij}s_{ij}^h\tau_{ij}^h$, where $\tau_{ij}$ is generated from route choices and directed link costs $t_{kl}$.
- Verified that the three terms add exactly to $\bar\tau^{LT}-\bar\tau^0$. Relabeled the final term from sorting to OD reallocation because $s_{ij}^{LT}-s_{ij}^0$ includes the complete change in OD weights, not only a distinct sorting margin.
- Kept additive physical minutes on Slide 22 as a separate reported outcome and synchronized the speaker script. The 44-page PDF compiles without layout or reference warnings, and Slides 21--22 were rendered and inspected at slide resolution.

## 2026-07-16: Model slides restored to link-cost notation

- Compared the current model sequence with `Documents/PreSlides/tidalraw.tex`. The earlier deck kept physical speed measures outside the model block, using a link-cost object inside route choice and equilibrium.
- Restored Slides 9--14 to a single directed link-cost object, $t_{kl}$, and retained $\tau_{ij}$ as the all-route bilateral cost index. Introducing the additional symbol $c_{kl}$ from `tidalraw` would have created an unnecessary third link-cost notation, so the current deck keeps $t_{kl}$ throughout the model block.
- Localized physical minutes to the measurement and physical-time accounting slides. Slide 16 records $m_{kl}^{q}=60\sum_{a\in p_{kl}}\ell_a/v_a^q$ and $t_{kl}^{q}=(m_{kl}^{q})^{1/\theta}$. Slides 13 and 17 express the link-cost congestion exponent as $\lambda/\theta$ because $\lambda=1.1$ is calibrated as the physical time-flow elasticity.
- Synchronized the 25-minute speaker script. The 44-page PDF compiles without overfull boxes, underfull boxes, undefined references, citations, or control sequences. Pages 9--17 and 19 were rendered and inspected at slide resolution.

## 2026-07-16: One-slide arithmetic-symmetry result integrated into `Tidal0715`

- Added `data_work/src/model/full_estimation/plot_symmetry_one_slide_20260716.py`. The retained figure and slide metrics are under `data_work/outputs/symmetric_input_mean_method_comparison_20260716_v1/`. The current slide figure is `Documents/PreSlides/figs/symmetry_fundamental_relative_change.{pdf,png,svg}`; the earlier exposure and paired-level variants remain as diagnostics.
- The physical-time table retains only the directional covariance result. The slower direction carries 56.3 percent of baseline model-implied passages, versus 43.7 percent in the faster direction. Arithmetic symmetrization lowers passage-weighted physical time by 5.0 percent at fixed baseline passages and by 5.5 percent after routes, OD flows, fundamentals, and equilibrium are re-estimated, while the unweighted mean link time is unchanged.
- The retained fundamental graphic plots the arithmetic symmetric-minus-directed log revision against distance to the employment-weighted center. Five points report weighted distance-band means; node-level resident- and job-weighted fits have gradients of +0.43 and -0.26 in units of 100 times the log difference per 10 km. Positive values mean that symmetry assigns a higher recovered fundamental. The figure therefore isolates spatial reattribution without letting the much larger cross-sectional level variation dominate the comparison.
- Rebuilt the single `What Symmetry Misses` frame in `Documents/PreSlides/Tidal0715.tex` with a left-table, right-figure layout. The two bullets state the physical-time result and the residential-fundamental conclusion. The latter is phrased as a spatial-attribution pattern, because residential and workplace fundamentals are jointly re-inverted. The opening contribution slide, conclusion, and `Documents/PreSlides/tidal_speaker_script_Tidal0715_20260716.md` avoid overclaiming a one-equation causal decomposition. The appendix fixed-fundamentals diagnostic remains separate.
- Recompiled the 44-page deck. The log contains no overfull boxes, underfull boxes, undefined references, or undefined control sequences. Rendered pages 5, 19, and 25 were inspected at slide resolution, with no clipping or unreadable labels.

## 2026-07-16: Data, routing, and result slides consolidated in `Tidal0715`

- Backed up the pre-edit deck as `Documents/PreSlides/Tidal0715_backup_before_data_route_layout_20260716.tex` and PDF.
- Merged the former Data and Parameters frames into one main slide. It now records the source and construction of each model input, the four calibrated parameter values, and the retained sample counts.
- Rebuilt the directed-link measurement slide around the complete mapping from segment speed to physical minutes and then to iceberg link cost: $m_{kl}^{q}=60\sum_{a\in p_{kl}}\ell_a/v_a^q$ and $t_{kl}^{q}=(m_{kl}^{q})^{1/\theta}$. The active night-speed window is 22:00 to 05:00.
- Added a clickable route-construction appendix with access radii, connector limits, AM-time Dijkstra weighting, detour gate $G=5$, lane construction, and three cropped route examples.
- Replaced the national jobs-housing strip with a city-by-city lollipop chart, aligned the three images on the directional-facts slide, restored the three-term short-run and long-run physical-time decomposition, and combined physical-time and welfare results into one panelled table.
- Replaced the Beijing radial statistic with the same 30 percent employment-core measure used in the cross-city comparison. Among 6,080,198 Beijing inter-town commuters, peripheral-to-core flow is 1,362,503 (22.41 percent) and reverse flow is 788,568 (12.97 percent), yielding a 9.44-percentage-point gap and a 1.73 ratio. The Duncan index remains unchanged. The distance-threshold radial slide and its 74.5 percent and 2.9 ratio were removed from the deck.
- Clarified $C_{ij,r}$ as route-specific iceberg travel cost and $\kappa_{ij}^{kl}$ as an OD-conditional expected link-passage count. On simple paths, the latter is a route-use probability.
- Updated the 25-minute script to 25 main slides and 18 appendix slides. The retained timing is 7:00 for the opening, 5:00 for directional facts, 4:30 for the model, 4:00 for data and estimation, and 4:30 for counterfactuals and results.

## 2026-07-16: Arithmetic symmetric-input robustness and exposure covariance retained

- Added `data_work/src/model/full_estimation/compare_symmetric_mean_methods_20260716.py`. Retained outputs are `data_work/outputs/model_input_edgeproj_v2_lcc_gate5_symmetric_arithmetic_v1/`, `data_work/outputs/symmetric_input_reestimated_gate5_bpr_pos05_arithmetic_v1/`, and `data_work/outputs/symmetric_input_mean_method_comparison_20260716_v1/`.
- Both symmetric constructions average reciprocal AM and free-flow physical link times in minutes before the iceberg transformation. The arithmetic version uses $(m_{kl}+m_{lk})/2$. The geometric version uses $(m_{kl}m_{lk})^{1/2}$.
- Arithmetic averaging preserves the unweighted mean input link time at 10.189 minutes. Even so, citywide expected physical commuting time falls from 11.298 minutes in the directed baseline to 10.677 minutes in the arithmetic symmetric world. This is a 37.2-second difference. The corresponding geometric result is 10.563 minutes, or 44.1 seconds below the directed baseline.
- In the arithmetic comparison, holding directed baseline link passages fixed accounts for 33.9 of the 37.2 seconds. Passage reallocation accounts for the remaining 3.4 seconds. Across 1,193 reciprocal pairs, 56.3 percent of model-implied passages use the slower direction. Total slow-direction passages are 1.29 times fast-direction passages. The top 100 pair contributions account for 93.5 percent of the signed fixed-passage difference.
- A separate observed-OD check holds road-commuter OD flows fixed and computes additive shortest-path minutes on the two input networks. On the common reachable sample, which covers 91.5 percent of observed road-commuter mass, arithmetic symmetrization lowers mean shortest-path time by 52.7 seconds, or 0.80 percent. This agrees in sign with the model-passage result. The level is a network-implied shortest-path statistic, not reported travel time.
- Aggregate welfare differences are small and method-dependent. The geometric comparison is +0.0348 percent. The arithmetic comparison is -0.0656 percent. Residence and workplace distributions differ by less than 0.05 percent dissimilarity because both worlds re-invert fundamentals to match the same marginals. Model-implied mean home-work distance and cross-district commuting also change little.
- Fundamental spatial attribution is robust to the averaging rule. Under arithmetic averaging, the residence and workplace weighted L2 differences are 0.0431 and 0.0396 log point. The projected-geometry gradients are +0.00434 and -0.00263 log point per 10 km. At least a 0.05-log-point difference covers 17.6 percent of residents and 14.5 percent of jobs.
- The arithmetic district pattern is economically concentrated. Chaoyang accounts for 18.4 percent of residents and 21.2 percent of jobs. Its symmetric-minus-directed residence fundamental is -0.034, while its workplace fundamental is +0.026. Fengtai has the same sign pattern. Outer northern districts such as Shunyi, Huairou, and Miyun show the reverse sign pattern. Mentougou and Shijingshan are large western outliers, so the result is not purely concentric.
- The retained slide interpretation is directional exposure plus spatial attribution. Aggregate welfare and metro-candidate rankings are scope results rather than headline evidence that symmetry matters.

## 2026-07-16: Physical-time notation and tidal minutes locked

- Superseded the July 15 notation restoration. The retained convention is now $m_{kl}$ for physical directed-link travel time in minutes, $t_{kl}=m_{kl}^{1/\theta}$ for link iceberg cost, and $\tau_{ij}$ for the all-route bilateral cost index. Physical route minutes add across links; iceberg route costs multiply across links.
- Added `Documents/L1_manuscripts/PhysicalTravelTimeMeasurement.tex` and PDF. The note derives speed-to-minutes measurement, route-level additive minutes, the all-route expectation, the link-passage identity, and the short-run and long-run decomposition. It also records the current resolvent-floor diagnostic.
- Integrated the notation and physical-time equations into `Documents/L1_manuscripts/TidalLanes_Model_v4.tex` and `Documents/PreSlides/Tidal0715.tex`. The main results slide now reports citywide expected physical time; the former treated-link totals are retained only in an appendix as a local engineering-incidence measure.
- Synchronized `Documents/PreSlides/tidal_speaker_script_Tidal0715_20260716.md` with the locked notation, the citywide physical-time results, and the expanded 18-slide appendix Q&A map.
- Corrected `data_work/src/model/full_estimation/tidal_lane_diagnostics.py` so the bilateral $\tau_{ij}$ index is never converted to seconds. The headline measure is $\bar M^h=\bar L^{-1}\sum_{kl}\Xi_{kl}^h m_{kl}^h$ for the retained $\kappa=1$ run. The code also constructs OD-conditional expected additive minutes as a diagnostic.
- Added the explicit `EquilibriumResult.m_min_kl` alias and now writes both `m_min_kl` and the legacy-compatible `tau_min_kl` array name. The solver continues to map physical minutes into `t_kl_iceberg` at every equilibrium iteration.
- Retained output: `data_work/outputs/tidal_lane_diagnostics_gate5_bpr_pos05_physical_time_v1/`. Baseline, short-run, and long-run average physical times are 11.298, 10.596, and 11.538 minutes. The short run saves 42.1 seconds per commuter and 1,031 person-hours per AM peak; the long run adds 14.4 seconds per commuter and 352 person-hours relative to baseline. Expected-utility welfare remains $+0.87\%$ and $+0.32\%$.
- Verified $t_{kl}^{\theta}=m_{kl}$ to machine precision in all three equilibria and verified the physical-time decomposition identity to $7.1\times10^{-14}$ seconds. Recompiled and visually inspected the 44-page deck and the four-page derivation note.

## 2026-07-16: Tidal time magnitudes switched to total AM person-hours in `Tidal0715`

- Replaced the main result slide's OD-weighted per-commuter route-cost decomposition with treated-link segment-person-time accounting for the top-100 tidal reallocation.
- The retained headline totals are 1,276 net person-hours per AM peak in the short run and 714 in the long run. Gross savings are 1,385 and 804 person-hours; offsetting losses are 109 and 90.
- Added the shortest-path exposure count of 49,663 road commuters (56.4% of road-mode commuter mass) and a visible accounting note. The note distinguishes unique OD exposure from model link-passage totals and states that the measure is not a citywide route-time total.
- Synchronized the `This Paper`, short-run versus long-run, main results, policy comparison, conclusion, sensitivity, and targeting slides. Updated `Documents/PreSlides/tidal_speaker_script_Tidal0715_20260716.md` with the same definitions and cautions.
- Recompiled `Documents/PreSlides/Tidal0715.pdf` and inspected every modified slide. The 42-page build has no overfull boxes, underfull boxes, or undefined references.

## 2026-07-16: `Tidal0715` 25-minute speaker script rebuilt

- Added `Documents/PreSlides/tidal_speaker_script_Tidal0715_20260716.md`, aligned page by page with the 26-slide main presentation in the current `Tidal0715` deck.
- Locked the 25-minute allocation at 7:00 for the opening and introduction, 5:00 for directional facts, 4:30 for the model, 3:45 for data and estimation, and 4:45 for counterfactuals and results.
- Rewrote the model talk track around route-inclusive gravity, expected directed link use, model-implied link load, anchored congestion, and the distinction between measured physical time and the bilateral commuting-cost index.
- Marked the 1.10 percent fixed-fundamentals symmetry result as a benchmark rather than the pending full re-estimation result. Added a slide-specific Q&A map for the 16 appendix frames.

## 2026-07-16: Jobs-housing evidence and results slides consolidated

- Rebuilt the main jobs-housing slide as a balanced two-column composition: national evidence on the left and the Beijing illustration on the right. Both columns now place text before the figure. The national plot retains the all-positive city pattern without repeating the 4.5 percentage-point average as a vertical reference line.
- Added `data_work/src/national_tidal_commuting/plot_jobs_housing_main_slide_strip.py` and retained both horizontal and column-optimized outputs as `Documents/PreSlides/figs/pat_jobs_housing_top20_gap_{strip,column}.{pdf,png,svg}`. The plots read the retained 30 percent employment-core outputs and do not alter source data.
- Removed the three national directional-commuting appendix frames. The appendix now begins with the fixed-fundamentals symmetry diagnostic.
- Rebalanced the short-run and long-run results slide. The route-time decomposition and expected-utility welfare tables share the upper row; exposure, corridor effects, general-equilibrium adjustment, and the mode-switching scope condition run across the full width below.
- Compiled and visually inspected the revised deck. The retained PDF has 26 main slides and 16 appendix slides.

## 2026-07-16 symmetric-input reduced-form metro candidate experiment

- Added `data_work/src/model/full_estimation/run_symmetric_input_metro_candidates_20260716.py` and generated `data_work/outputs/symmetric_input_metro_candidates_20260716_v1/`.
- The experiment excludes tidal lanes and road-lane reallocation. Each of 370 common candidate lines connects two 6 km endpoint catchments and lowers generalized bilateral commuting costs by 10% for OD markets linking the two catchments in both directions.
- Candidate screening retains radial, orbital, high-flow, and high-asymmetry endpoint pairs. The same physical candidates and OD treatment masks are evaluated in the fully re-estimated directed and symmetric calibration worlds.
- The recovered-fundamental gradients reproduce the retained spatial diagnostic: the symmetric-minus-directed residence fundamental rises by 0.00447 log point per 10 km, whereas the workplace fundamental falls by 0.00264 log point per 10 km. Distance accounts for 2.1% and 0.85% of the node-level variation, so the gradients are treated as broad spatial tendencies.
- Candidate welfare rankings are nearly invariant. Spearman rank correlation is 0.99949, top-10 overlap is 90%, top-25 overlap is 92%, top-50 overlap is 98%, and top-100 overlap is 100%. Both worlds select the same welfare-maximizing candidate, which implies zero regret in the directed benchmark.
- Line-length normalization does not alter the inference. Welfare-per-kilometre rank correlation is 0.99947, top-100 overlap is 99%, and the best candidate remains unchanged.
- The median absolute symmetric-minus-directed welfare difference is 0.000257 percentage point; the maximum is 0.0213 percentage point. Fundamental alignment has a descriptive Spearman correlation of -0.198 with the welfare difference, and the joint candidate-characteristic regression has R-squared 0.134.
- Retained interpretation: symmetric inputs change the spatial attribution of recovered residence and workplace fundamentals, but the current reduced-form metro experiment does not deliver an economically meaningful aggregate-welfare or line-siting contrast.
- Validation passed: 740 candidate-world rows contain no duplicate keys, all fixed-cost counterfactuals converged, the OD-cost decomposition closes within 1.2e-13 seconds, all ranks are complete, and SVG, PDF, and 600 dpi PNG figures were inspected.

## 2026-07-16 Tidal0715 layout and physical-time notation audit

- Backed up `Documents/PreSlides/Tidal0715.tex` and PDF as `Tidal0715_backup_before_layout_formula_review_20260716.*` before editing.
- Removed the numerical 0.8 interpretation from Slide 7 and increased spacing around the definition, figures, and bullets. Aligned the lower-left chart on Slide 8 with the bullet text.
- Audited equation (10) against the formal model and code. Because $\lambda$ is the elasticity of physical minutes with respect to traffic per lane and $t=m^{1/\theta}$, the link-cost exponent $\lambda/\theta$ is retained.
- Rewrote equation (15) to match the anchored solver: physical minutes update with exponent $\lambda$ around $(m^{obs},\Xi^0,n^0)$ and then map to link cost through $t=m^{1/\theta}$. $\Xi^0$ remains a baseline model-implied passage load, not observed vehicle flow.
- Rebuilt Slide 15 using the older vertical two-table layout. It now separates raw data sources and coverage from external parameter calibrations. Slide 16 contains only the pair-specific path, same-path speed application, additive physical-minute construction, and the mapping to directed link costs.
- Rebalanced Slide 19 to a 40/58 table-figure split and combined the physical-time and welfare results on Slide 22 into one table.
- Synchronized the affected sections of `tidal_speaker_script_Tidal0715_20260716.md`. Compiled `Tidal0715.pdf` successfully at 43 pages and visually inspected Slides 7, 8, 13, 15, 16, 17, 19, and 22. The LaTeX log contains no overfull boxes or undefined references.
- Follow-up wording and sourcing revision: Slide 7 now presents the speed asymmetry definition as a separate bullet. Slide 8 separates the top-20 city-by-city result (20/20 positive gaps) from broader flow-weighted sample evidence (4.5 pp for top 20, 3.2 pp for top 50, and 2.2 pp for top 100). Slide 15 adds literature sources for $\theta$ and $\lambda$ while leaving $\alpha$ and $\beta$ uncited because their external calibration source has not been locked. The revised PDF remains 43 pages and Slides 7, 8, and 15 passed full-size visual inspection.
- Appendix Slide 27 was rebalanced into an upper routing-rule/settings block and a lower two-example block. Route examples were reduced from three to two, retaining one connector-assisted path and one direct road path. The routing copy and table labels were shortened, the text size was increased, and the note and back link now clear the footer. The final LaTeX log has no overfull boxes or undefined references.
- Slide 16 now uses an actual Beijing adjacent-grid example rather than the citywide link-time map. The retained pair is `sq_22_41` to `sq_22_40`, a regular 3 km square-grid pair whose observed-AM shortest path contains five raw-road pieces. Raw-road time is 8.2815 minutes, endpoint access time is 0.7296 minute, and total physical time is 9.0112 minutes.
- Added `data_work/src/diagnostics/plot_link_time_construction_example.py`, `data_work/outputs/link_time_construction_example_v1/`, and the slide figure `Documents/PreSlides/figs/link_time_construction_actual.png`. The script reconstructs the pair-specific Dijkstra path, exports the five-segment and endpoint-access breakdowns, and asserts that their sum equals the production AM pair output to numerical precision.
- The unsuffixed `raw_connected_grid_connectors_adjacent_square.csv` is overwritten when another period is run, so its stored `connector_time_min` cannot be reused for an AM audit after a PM run. The retained diagnostic recomputes connector minutes as connector distance divided by the AM speed of the host road edge. Slide 16 equation (13) now states the complete code-consistent decomposition: raw-road segment minutes plus two endpoint access terms.

## 2026-07-15: Model notation restored in `Tidal0715`

- Backed up the intermediate model rewrite as `Documents/PreSlides/Tidal0715_backup_before_model_notation_restore_20260715.tex` and `.pdf`.
- Restored the model notation used in `Tidal0715_backup_before_25min_20260715`: $t_{kl}^{ff}$ is free-flow directed link time, $t_{kl}$ is realized directed link time, $C_{ij,r}$ is route cost, and $\tau_{ij}$ is bilateral commuting cost. The expected link-use term is $\kappa_{ij}^{kl}$, model-implied link load is $\Xi_{kl}$, and the congestion elasticity is $\lambda$.
- Removed the intermediate $m_{kl}$, $\lambda_{\mathrm{phys}}$, and AA-mapping notation from the main deck and appendix. The anchor retains $\Xi_{kl}^{0}$ as baseline model-implied load rather than observed vehicle flow.
- Restored a six-frame model sequence: environment, joint residence-workplace-route choice, route-inclusive bilateral gravity, endogenous amenities and productivity, the link-load congestion feedback, and equilibrium. Rewording is limited to clearer transitions and definitions. Speaker notes allocate about 4.5 minutes to the six frames.
- Synchronized the restored notation across model data, measurement, estimation, counterfactuals, and appendix slides. The main equilibrium frame now references the main route-cost and bilateral-cost equations directly; the matrix route aggregator remains in the appendix.
- Compiled and visually inspected the revised deck. The retained PDF has 26 main slides and 19 appendix slides, with no overfull boxes, underfull boxes, or undefined references.

## 2026-07-15: 25-minute conference deck restructured

- Backed up the prior `Tidal0715` source and PDF as `Documents/PreSlides/Tidal0715_backup_before_25min_20260715.tex` and `.pdf` before editing.
- Restructured `Documents/PreSlides/Tidal0715.tex` into 25 main slides plus 19 appendix slides. Slides through Literature remain unchanged. The main directional-facts section now combines national and within-city road-segment evidence, retains one jobs-housing slide, and removes the standalone Shenzhen statistics slide.
- Reorganized the model sequence around directed network costs, route-inclusive commuting gravity, endogenous segment congestion, measured model objects, estimation targets, and the invert-anchor-solve implementation. The source includes timed speaker notes for the model, data, estimation, policy, and result slides.
- The intermediate 25-slide rewrite used $m_{kl}$ for physical minutes and $t_{kl}=m_{kl}^{1/\theta}$ for link iceberg cost. This notation was superseded by the restoration recorded above. The anchored congestion equation continues to use baseline model-implied commuter load $\Xi_{kl}^{0}$ rather than observed vehicle flow.
- Retained `What Symmetry Misses` in the main deck as a full re-estimation design with a visible pending-results marker. The prior fixed-fundamentals diagnostic is retained in the appendix under its distinct estimand. The opening `This Paper` slide still carries the prior numerical result because the instruction locked all slides through Literature; it must be synchronized when the full re-estimation result is selected.
- Placed the short-run versus long-run slide immediately after the tidal-policy definition. Combined direct, general-equilibrium, sorting, and welfare results into the remaining 25-minute sequence; detailed targeting and supplementary diagnostics are retained in the appendix.
- Compiled the revised PDF and inspected all 44 rendered pages. The final build has no overfull boxes, underfull boxes, or undefined references.

## 2026-07-15 pre-presentation clarification of congestion anchoring

- Revised `Documents/PreSlides/tidal.tex` and the accompanying speaker script to define $\Xi_{kl}$ as model-implied commuter-passage load rather than observed vehicle flow.
- Replaced the observed-traffic notation in the anchored equation with baseline model load $\Xi_{kl}^{0}$. The counterfactual is now stated as conditional on a stable person-to-vehicle conversion and the calibrated congestion slope.
- Added the appendix slide `Commuter Load and Vehicle Flow`, which defines $Q_{kl}=(\rho_{kl}/H)\Xi_{kl}$ and states the condition under which $Q_{kl}/Q_{kl}^{0}=\Xi_{kl}/\Xi_{kl}^{0}$.
- Reframed the Caitian Road videos as evidence that can discipline a local lane-time response once throughput is paired with speed or density across repeated switch windows. The current one-day flow evidence remains descriptive and is not presented as an estimate of $\lambda$ or as a sharp RDD.
- Updated targeting language from observed flow to model-implied commuter load where the score uses $\Xi$. Added concise answers to anticipated questions about units, identification, field-video evidence, and the interpretation of the current counterfactual.
- Compiled `Documents/PreSlides/tidal.pdf` successfully (45 pages), verified all hyperlink targets, and visually inspected the revised model, anchoring, next-step, load-flow bridge, and field-visit slides. No undefined references or citations were found.

## 2026-07-15 standalone video-measurement and RDiT slides

- Created the two-slide standalone deck `Documents/PreSlides/TidalLane_Video_RDiT_2slides_20260715.pptx`; the existing main slides were not modified.
- Slide 1 uses actual 0709 pre-opening and post-opening tracking frames to define total directional flow $Q_{dt}$, fixed-gate travel time $T_{dt}$, and visible vehicle stock $K_{dt}$. It reports the retained pilot values and labels the south-to-north travel-time comparison as preliminary because matching selection remains under validation.
- Slide 2 defines the scheduled opening cutoff, the 3-to-4-lane treatment discontinuity, the day by one-minute-bin observation unit, the local-linear sharp RDiT equation, the immediate reduced-form parameter $\tau$, bandwidth sensitivity, date-clustered inference, and the scheduled-time instrument for delayed actual opening.
- The reverse direction is assigned to clock and detector validation rather than treated as a second policy observation. Per-lane density remains secondary because lane reallocation changes its denominator mechanically.
- The PPTX was rendered slide by slide and passed the presentation overflow test. Both final rendered slides were inspected at full resolution; no clipping or unintended overlap was found.
- Added the standalone Beamer source `Documents/PreSlides/TidalLane_Video_RDiT_2slides_20260715.tex` and compiled PDF. The video-measurement page retains the 0709 images, sample windows, and $Q/T/K$ definitions.
- Recast the identification page in a standard economics design. Scheduled time defines the RDiT assignment, video-coded operation defines treatment, and the slide reports the reduced form, first stage, fuzzy-RDiT Wald estimand, identifying assumptions, bandwidths, repeated-date inference, and the limited validation role of the reverse camera. The TeX build has no layout or LaTeX warnings, and both rendered pages passed visual inspection.

## 2026-07-15 Jobs-Housing major-city slide integration

- Added `data_work/src/national_tidal_commuting/plot_jobs_housing_asymmetry_slides.py` to merge the top-20 city jobs-housing directionality table with the 2025 top-100 city township-pair AM speed asymmetry summary.
- Generated `data_work/outputs/national_jobs_housing_asymmetry_slides_v1/` with the merged top-20 table, group summary, correlation summary, manifest, and three slide-ready figures. The same figures were copied to `Documents/PreSlides/figs/`.
- Retained headline statistics: all top-20 cities have $P\to C$ above $C\to P$; the flow-weighted top-20 shares are 19.80% and 15.30%, giving a 4.50 pp gap. The all-city gap is 0.87 pp.
- The city-level link to AM speed asymmetry is weak in the top-20 cross section: Pearson $r$ with AM SAR is 0.11 for the $P\to C-C\to P$ gap and 0.09 for the $P\to C$ share. The slides therefore treat the correlation figures as descriptive diagnostics.
- Updated `Documents/PreSlides/Tidal0715.tex` by adding three Jobs-Housing frames after the existing grid-level mismatch page: top-20 directional shares, SAR scatter diagnostics, and the two-point city dumbbell. Compiled `Documents/PreSlides/Tidal0715.pdf` successfully at 49 pages and visually inspected the new pages 12--14 via `pypdfium2`.

## 2026-07-10: Coauthor revision attempts completed

- Added `data_work/src/model/full_estimation/run_coauthor_revision_attempts_20260710.py`.
- Retained output package: `data_work/outputs/coauthor_revision_attempts_20260710_v1/`.
- Added handoff note: `Documents/L2_notes/coauthor_revision_attempts_20260710.md`.
- National jobs-housing evidence now has a major-city first pass. At the 30% employment-core definition, center-bound minus reverse commuting is 0.87 pp for all cities, 4.50 pp for the top 20 by inter-town flow, and 6.18 pp for Beijing, Shanghai, Guangzhou, and Shenzhen. The sign is positive across 20% to 50% core thresholds for every city group.
- Symmetric-input diagnostics now compare the fully re-estimated symmetric world with the directed world at the node level. The weighted L2 distances are 0.055 log point for residence fundamentals and 0.049 log point for workplace fundamentals. Distance gradients are +0.0045 and -0.0026 log point per 10 km, respectively. Current distance is to the employment-weighted grid center, so a true CBD or sixth-ring figure still needs ring geometry.
- Tidal segment-person-time accounting is now retained for top-100. Shortest-path OD exposure is 49,663 road commuters, equal to 56.36% of the road-mode sample. Gross treated-link savings are 83,117 person-min/day short-run and 48,252 person-min/day long-run; net of fast-direction losses, the corresponding totals are 76,560 and 42,845 person-min/day.
- Sorting diagnostics now add fixed `log_ubar`, fixed `log_abar`, effective residential amenity, and wage components. The effective channels recover the calibrated slopes, `beta=-0.10` and `alpha=+0.05`, so the slide should treat them as model-accounting channels rather than new fundamentals responses.
- Population-growth and urban-expansion smoke counterfactuals converged for every row. With fixed baseline fundamentals, original congestion anchors, population scales 1.00/1.15/1.30, and an outer-residential-amenity proxy, long-run welfare effects attenuate as population scale rises. Same-corridor bidirectional expansion remains above tidal reallocation in this smoke grid. Congestion pricing is not included in this smoke package because toll and rebate accounting still needs a closed welfare block.

## 2026-07-10: Coauthor revision priorities captured

- Added `Documents/L2_notes/meeting_notes_20260710.md` with the latest coauthor comments on national jobs-housing evidence, symmetric-input re-estimation diagnostics, aggregate tidal time savings, sorting-page fundamentals / wage trends, and population-growth / urban-expansion counterfactuals.
- Updated `nextstep.md` so the current priority list starts from these revision tasks while retaining lower-priority robustness and empirical follow-ups.

## 2026-07-10 Caitian Road tidal-lane video state plan

- Organized the two camera streams by source-video recording intervals and corrected the true direction of `2026-07-10 215640.mov` to outbound.
- Recorded per-video direction, three-point line plan, tidal-lane state, and directional lane counts in `flowvedio/batch_outputs/20260710_line_review/lane_state_plan_20260710_v1.csv`.
- Locked the lane-count convention for this review: south-to-north is 3 lanes without tidal deployment and 4 with deployment; north-to-south is 4 without deployment and 3 with deployment.
- Identified `IMG_2997.MOV` as the outbound deployment transition source and `2026-07-10 211730.mov` / `IMG_3018.MOV` as the two-direction return/reversal review sources. Exact within-clip transition seconds remain open pending frame-level confirmation.
- Added the human-readable summary at `flowvedio/batch_outputs/20260710_line_review/lane_state_summary_20260710_v1.md`; the full-day counter should wait until the two transition seconds and remaining line confirmations are finalized.

### 2026-07-12 state-plan correction

- Updated the plan to `lane_state_plan_20260710_v2.csv`: `IMG_2996` is still normal, `IMG_2997` and `IMG_2998` are deployment-in-progress clips, and both directions are treated as unable to use the tidal lane during setup.
- Split `IMG_3002.MOV` at the user-confirmed in-clip 2:00 mark (`2026-07-10 16:30:59`): the first segment is excluded transition time with 3/3 usable lanes, and the remainder is tidal operation with 4 south-to-north lanes and 3 north-to-south lanes.
- Added the corresponding human-readable v2 summary; the return/reversal clips remain transition segments pending exact within-clip timing.
- Further split `IMG_3018.MOV` at the user-confirmed in-clip 14:00 mark (`2026-07-10 21:33:17`): before the mark it is stable tidal operation (4/3 usable lanes); after the mark the tidal lane is closed during the adjustment (3/3 usable lanes, excluded). `IMG_3021.MOV` remains the stable post-adjustment normal segment.

### 2026-07-12 camera-stability and within-clip state corrections

- Added `lane_state_plan_20260710_v3.csv` with the user-confirmed within-clip splits: `160016` at 20:00/31:00 and `211730` at 16:00; `214611` is stable normal traffic after tidal-lane recovery.
- Inspected the opening of `2026-07-10 150310.mov` and isolated the first approximately 4:30 as `camera_unstable`; exported it as a separate derived video under `flowvedio/batch_outputs/20260710_line_review/150310_stability_check/` and marked it `separate_anomaly` rather than pooling it with the stable remainder.
- Updated `flowvedio/run_20260710_caitian_identification.py` to consume the v3 segment plan, pass source start/end windows to `traffic_count_yolo.py`, preserve state/anomaly labels in event outputs, and honor the corrected outbound direction of `2026-07-10 215640.mov`. Dry-run validation produced 39 segment rows without running full-day detection.
- Generated full-size before/transition/after line-review images for the same-video state changes in `IMG_3002`, `2026-07-10 160016.mov`, `IMG_3018`, and `2026-07-10 211730.mov` under `flowvedio/batch_outputs/20260710_line_review/state_transition_v1/`; all images use the current two-color lines and coordinate grid.
- Regenerated the 10-image state-transition review as `state_transition_v2`: updated images 1–3 and 6 using the latest user coordinates, retained images 4–5, and added a larger 2×2 page for images 7–10.
- Corrected image 1 in `state_transition_v2` to use the pre-operation midpoint `(800,800)`.
- Added the final per-segment line plan `flowvedio/segment_line_plan_20260710_final.csv` and updated the counting wrapper to use segment-specific two-direction lines; dry-run verified 39 segments and the special `IMG_3002` / `160016` line changes.

### 2026-07-14 completed-segment QA

- Audited the 26 of 39 segments completed so far by `run_20260710_caitian_identification.py`. Every completed segment has a matching summary and event CSV; event totals match `total_count`, no duplicate `(track_id, line_name)` events were found, processed-frame ratios are approximately 1, and the actual line coordinates match `segment_line_plan_20260710_final.csv`.
- Long stable segments have plausible traffic magnitudes (roughly 55–93 vehicles/min) and median detection confidence around 0.80–0.86. Very short clips remain unsuitable as standalone rate-comparison samples.
- Flagged directional-mixing risks for follow-up: `171617` (~19.6% on one line), `181104` (~21.2%), `190717` (~36.5%), `IMG_3003` (~6.4%), and `IMG_3012` (~9.1%). `IMG_2998` has ~47.6% mixing and lower confidence, but is a deployment transition segment and should remain excluded from stable operation statistics.
- Saved the detailed QA report and frame samples under `flowvedio/batch_outputs/20260710_caitian_identification_qa_completed_v1/`.

### 2026-07-14 full-day counting completed

- The full-day Caitian Road run completed all 39/39 planned segments with no non-`done` status. The final output contains 53,352 vehicle-crossing events and the root aggregates `summary_by_video.csv`, `events_absolute.csv`, and `counts_by_wall_minute.csv`.
- The QA conclusions above continue to apply: completion and file-level integrity pass, while the flagged direction-mixing segments and transition/short-clip exclusions require filtering or follow-up before final analysis.

### 2026-07-14 first-pass descriptive analysis

- Added `flowvedio/analyze_20260710_caitian_final.py` and generated `flowvedio/batch_outputs/20260710_caitian_identification_final/analysis_v1/` with three time-series figures, segment rates, stable-sample statistics, class counts, and direction-mixing diagnostics.
- The first-pass stable sample contains 51,120 of 53,352 events (95.8%). The charts keep the two source streams separate and use `line_name` for geographic direction; the two streams are not added together.
- Across 78 segment-line combinations, 8 have more than 10% non-dominant raw-direction events and 6 exceed 20%. Newly completed high-risk long clips include `IMG_3015`, `193845`, `203954`, and `211730`; they require line/track review before final flow comparisons.

### 2026-07-14 source-stream alignment and first-pass RDiT plan

- Added `flowvedio/validate_20260710_stream_alignment.py` and `stream_alignment_v1/`. Stable events were aggregated into 1-, 5-, and 10-minute bins without filling uncovered minutes with zeros; outbound-to-inbound lags from -15 to +30 minutes were tested.
- At 5-minute bins, the best lag is 0 minutes for both directions: Pearson r is 0.891 for north-to-south and 0.682 for south-to-north; the mean inbound/outbound rate ratios are 1.19 and 0.91. At +5 minutes, correlations fall to about 0.787 and 0.139. Filtering each segment-line to its dominant raw direction leaves the best lag at 0 minutes.
- The current evidence supports contemporaneous common flow variation but does not identify a stable positive travel-time lag. The two source streams should remain separate until camera ordering and spatial distance are independently verified.
- Added `stream_alignment_v1/rdd_first_pass_plan.md`: the feasible first analysis is exploratory RDiT / interrupted-time-series around the approximately 16:31 opening and 21:33 recovery, with 5-minute bins, bandwidth sensitivity, HAC errors, transition gaps excluded, and a within-stream direction-difference specification as the preferred initial contrast.

### 2026-07-14 exploratory RDiT run

- Added `flowvedio/run_20260710_rdit_exploratory.py` and `rdit_v1/`. Estimated 5-minute local-linear time-RD specifications for the opening and recovery cutoffs, separately by source stream and direction, plus the within-stream directional difference.
- The 30-minute directional-difference estimate for opening/inbound is -13.37 vehicles/min (HAC SE 3.53), but it uses only 4 pre and 5 post bins separated by a 20-minute no-stable-data gap and changes to 1.14 (SE 8.33) at 45 minutes. It is not a credible causal estimate.
- The opening/outbound and recovery/inbound models are not locally identified within 30 minutes because one side has too few stable bins. Recovery/outbound is +22.94 (SE 17.87) at 30 minutes and +10.70 (SE 5.13) at 45 minutes, again sensitive to bandwidth and the transition gap.
- The exploratory conclusion is that the current data do not yet support a clean RDD; use the estimates as diagnostics for the next data-collection/measurement step.

### 2026-07-14 descriptive tidal-lane evidence

- Added `flowvedio/analyze_20260710_tidal_lane_evidence.py` and `tidal_evidence_v1/` to normalize the stable `normal` and `tidal_deployed` rates by the locked 3/4 lane counts.
- The outbound stream shows a descriptive redistribution consistent with the lane change: north-to-south flow falls 34.6% while south-to-north flow rises 13.4%. The inbound stream does not reproduce the same pattern: both directions fall.
- Per-lane flow does not consistently rise: outbound falls 12.8% north-to-south and 15.0% south-to-north; inbound is roughly flat north-to-south (+0.4%) but falls 32.0% south-to-north. Total flow also falls in both streams, so these are not capacity estimates or causal effects.

### 2026-07-14 stage-oriented evidence package

- Added `flowvedio/make_20260710_stage_evidence.py` and generated `stage_evidence_v1/` with a presentation-oriented directional-flow/per-lane figure and a state-coverage/transition-gap timeline.
- The package frames the current result as preliminary directional-reallocation evidence plus a measurement/identification diagnostic: outbound balance shifts from -3.0% to +24.0%, while inbound shifts from -2.7% to +6.6%; lane-normalized flow is not uniformly higher.
- Added `stage_results_and_next_collection.md`, which lays out the next evidence chain: verify lane-state timing, estimate reduced-form flow/direction changes, add speed and queue measures for efficiency/capacity, then use repeated transitions and better synchronized coverage for time-RD/causal analysis.

### 2026-07-14 pooled two-camera breakpoint check

- Added `flowvedio/run_20260710_pooled_breakpoint.py` and generated `pooled_breakpoint_v1/`. Within each 5-minute bin, the two source-stream rates for the same geographic direction are averaged rather than added, avoiding double-counting the same vehicle; a strict both-streams-covered sample is reported separately.
- Defined directional balance as `B = (South-to-North - North-to-South) / total flow`; it measures directional composition, not total flow or per-lane capacity.
- The simple level-shift check shows an opening increase in pooled B of about 0.22–0.28 across 30–60 minute windows. The strict both-streams sample gives about +0.33–0.34 at 45–60 minutes, but the recovery side lacks enough post-cutoff bins with both streams observed to estimate the strict version.
- Because the one-day sample has transition/coverage gaps and source composition changes near the cutoffs, the breakpoint estimates and model p-values are diagnostic only; they are not presented as causal RDD evidence.

### 2026-07-14 0709 transition-video assessment

- Confirmed that `flowvedio/20260709彩田路/出城方向1621开始.MOV` has an already-counted implementation sequence: 0:00–2:45 normal, 2:45–3:30 deployment transition, and 3:30 onward stable tidal operation. The inbound 1623 clip and both 1727 clips are post-only for this purpose.
- Added `flowvedio/analyze_20260709_transition.py` and generated `flowvedio/batch_outputs/20260709_transition_v1/` from existing count outputs. In the immediate comparison, total flow is 73.1 → 81.8 vehicles/min and directional balance is 0.323 → 0.335; this is not a strong directional reallocation result.
- The 0709 clip is retained as implementation-process and transition-exclusion evidence, not as a standalone RDD: the pre period is only 2:45, the transition creates a gap, and there is no inbound pre period. The long post period is a descriptive trend reference only.

### 2026-07-15 0709 outbound speed-density pilot

- Added user-confirmed pre and post geometries to `flowvedio/create_20260709_speed_density_review.py`. The post geometry moves the directional boundary to x=1100 at y=500 and x=1250 at y=800, consistent with the active tidal lane.
- Added `flowvedio/measure_speed_density_yolo.py` and `flowvedio/scripts/run_20260709_speed_density_pilot.sh`. Speed is measured from persistent bounding-box-centre tracks crossing two gates; density is measured from bounding-box bottom centres inside direction-specific road polygons.
- Processed the complete 0:00–2:45 pre window and the stable 3:30–8:30 post-immediate window. The 2:45–3:30 deployment interval remains excluded. The extraction retained 72 raw dual-gate tracks before the transition and 204 after it.
- Relative outcomes move in the expected directional-capacity pattern. South-to-north median gate time falls from 18.73s to 16.32s, while visible vehicles per lane fall from 4.68 to 3.55. North-to-south median gate time rises from 3.49s to 3.85s, while visible vehicles per lane rise from 0.43 to 0.73.
- The internal q–k–v comparison differs from observed lane flow by 12%–21% across the four phase-direction cells. South-to-north far-gate matching remains weaker because dense traffic and occlusion delay stable track initiation; its speed estimates overrepresent continuously visible vehicles.
- Added `flowvedio/analyze_20260709_speed_density_pilot.py`. Retained outputs, vector figures, variable definitions, sample lineage, spatial measurement notes, and replication checks are in `flowvedio/batch_outputs/20260709_speed_density_pilot/analysis_v1/`.
- Absolute metrics adopt a provisional 36m gate distance with a 30–42m sensitivity interval. This calibration is based on visible lane-marking cycles and must be replaced by one field or map distance before the estimates are presented as locked km/h or vehicles per lane-kilometre.

### 2026-07-15 ideal video identification design

- Added `data_work/docs/CAITIAN_VIDEO_IDENTIFICATION_PLAN_20260715.md`, a complete Chinese research-design memo covering ideal camera placement, continuous recording windows, clock synchronization, field metadata, raw/event/bin data schemas, flow/speed/queue measurement, camera fusion, sample lineage, quality control, repeated time-break identification, direction differences, control-road extensions, inference, and presentation sequencing.
- The retained operational target is two synchronized traffic cameras plus an optional state camera, continuous coverage for at least 60 minutes on each side of opening and recovery, a 5-day variance pilot, and approximately 20 ordinary weekdays for the main repeated-event sample. The sample target is subject to a power simulation after the pilot.
- The memo integrates the current 0709 and 0710 evidence. It classifies the 0710 opening-direction signal as preliminary descriptive evidence, treats the 0709 clip as implementation-process validation, and explicitly rules out current causal, capacity, or formal significance claims.

## 2026-07-05: Dropbox replication package `tidal_model_replication/` (input -> output -> slides)

- New shareable package at repo root, 68 MB, all paths relative. Layout: `data/` (locked gate5 QSM bundle), `code/` (config + model_core + numbered steps 01-05 + run_all), `output/`, `reference_results/` (frozen `stage2_endogenous_edgeproj_v2_lcc_gate5_bpr_pos05_v1`), `slides/` (current tidal.tex/pdf/figs/speaker script/field video, synced from the validated mini package), `docs/` (REPLICATION_MAP.md, SYNC_WORKFLOW.md, FILE_MAP.csv), `scripts/` (sync_from_main / push_to_main via MAIN_REPO relative pointer, mini-style).
- Code is the replication-package reorganisation of the audited flat script; function bodies in `code/model_core.py` are verbatim from `full_estimation_flat/run_estimation_flat.py`. Step 05 compares a fresh run against the frozen reference: PASS, worst diff 7.4e-09 (relative; near-zero references compared absolutely), tidal pair selection identical.
- Per-equilibrium cache under `output/_cache/` makes interrupted runs resumable; `FLAT_TIME_BUDGET` slices long runs. Slide figures are frozen assets refreshed by `make sync-from-main`; the package recomputes the structural numbers only.

## 2026-07-05: single-file flat rewrite of the estimation stack, verified against the mainline

- Added `data_work/src/model/full_estimation_flat/run_estimation_flat.py`, a one-file rewrite of the full mainline pipeline (`run_stage2_gate5.py` and its counterfactual suite) intended for line-by-line human audit. Each block carries the corresponding `TidalLanes_Model_v4.tex` equation reference: Step 1 primitives (§6.1), calibration (§6.2), fundamentals inversion (§6.4, invert_R/invert_F), the (E1)-(E6) level-form solver with anchored congestion, the λ cross-section diagnostic (§6.3), and every counterfactual arm (policy 5×2, CF1/CF2/CF3 endogenous and exogenous, tidal endogenous specs, the fixed-Ξ BPR grid).
- The script hard-codes the solver configuration the mainline actually ran under the α>0 override (damping 0.10, max_outer 500, tol 1e-4), which the CLI defaults in `run_stage2_gate5.py` do not reveal.
- Verification: full re-run into `data_work/outputs/estimation_flat_check_v1/`, compared item by item against `stage2_endogenous_edgeproj_v2_lcc_gate5_bpr_pos05_v1`. All summary JSONs, five counterfactual CSVs, fundamentals arrays, and equilibrium arrays agree within floating-point noise (relative differences at most 1e-9, typically below 1e-12); tidal pair selection matches pair for pair. The two apparent outliers in the report are ratios of values whose reference magnitude is zero. Report: `estimation_flat_check_v1/COMPARISON_vs_mainline.md`.
- The flat script caches each solved equilibrium under `estimation_flat_check_v1/_cache/`; deleting that directory forces a clean recomputation. `FLAT_TIME_BUDGET` (seconds) splits a run into resumable slices.

## 2026-07-03: R replica of mini structural model completed

- Created independent R replica folder `tidal_model_slides_minimal_r/`.
- Copied the same package-local aggregate inputs, reference results, slide assets, and model-input QA files from `tidal_model_slides_minimal/`.
- Added R model core:
  - `R/model/core.R` for parquet input loading, primitives, fundamentals inversion, market access, welfare, and anchored endogenous congestion;
  - `R/model/counterfactuals.R` for symmetric costs, tidal-lane selection, fixed-tau BPR shocks, structural counterfactuals, policy shocks, and pricing diagnostics.
- Added runners and helpers:
  - `R/runners/run_smoke_model.R`;
  - `R/scripts/compare_smoke.R`;
  - `R/runners/run_stage2_gate5.R`;
  - `R/runners/run_policy_comparison_gate5.R`;
  - `R/model/diagnostics.R`;
  - `R/model/sensitivity.R`;
  - `R/model/symmetry_suite.R`;
  - `R/model/solver_presets.R`;
  - `R/runners/run_tidal_lane_diagnostics_gate5.R`;
  - `R/runners/run_alpha_beta_sensitivity_bpr_gate5.R`;
  - `R/runners/run_symmetry_directionality_suite_gate5.R`;
  - `R/scripts/compare_reference_results.R`;
  - `Makefile`, `README.md`, and `requirements.R`.
- Installed and used R package `nanoparquet` so the R implementation reads the original parquet inputs directly.
- Optimised the R inner loops by precomputing `t(tau_inv_theta)` once per fixed-point solve. This leaves the equations unchanged and reduces the market-access loop time materially.
- Smoke validation retained in `tidal_model_slides_minimal_r/outputs/smoke_model/`:
  - R baseline `Wbar = 3.2000917`;
  - median endogenous `tau/obs = 1.0001054`;
  - max absolute difference versus Python smoke reference `1.56e-12`.
- Stage2 quick retained in `tidal_model_slides_minimal_r/outputs/validation_r_stage2_quick/`:
  - baseline and overlapping fixed-tau structural rows match Python reference at floating-point tolerance;
  - corrected R solver overrides to match Python positive-alpha mainline: `max_outer >= 500`, `damping = 0.10`, `tol = 1e-4`;
  - corrected structural CF2/CF3 initialization to start from free-flow when the Python structural runner does so.
- Long-run P1 to P7 policy comparison retained in `tidal_model_slides_minimal_r/outputs/validation_r_policy_comparison_long/`:
  - `results_by_policy.csv` matches Python long-run reference rows with max absolute numeric difference `9.31e-10`;
  - `cost_effectiveness.csv` matches Python reference with max absolute numeric difference `8.53e-12`.
- Full validation retained:
  - full Stage2: `tidal_model_slides_minimal_r/outputs/validation_r_stage2_full/`;
  - policy comparison, short and long horizons: `tidal_model_slides_minimal_r/outputs/validation_r_policy_comparison/`;
  - tidal ST/LT diagnostics: `tidal_model_slides_minimal_r/outputs/validation_r_tidal_diagnostics/`;
  - alpha-beta sensitivity: `tidal_model_slides_minimal_r/outputs/validation_r_alpha_beta_sensitivity/`;
  - symmetry-directionality suite: `tidal_model_slides_minimal_r/outputs/validation_r_symmetry_directionality/`;
  - consolidated comparison: `tidal_model_slides_minimal_r/outputs/validation_r_report/`.
- Consolidated comparison results:
  - 10 of 11 table families pass at tolerance `1e-6`;
  - Stage2 structural, policy all-modes, tidal BPR grid, tidal ST/LT diagnostics, converged alpha-beta rows, symmetry-directionality rows, and policy cost-effectiveness all match the Python reference within floating-point tolerance;
  - the retained residual is `policy_results_by_policy`, key `P6_first_best_pricing|short_run`, with largest difference `929.936` in `toll_revenue_per_day` and realized welfare difference `0.0189` percentage points;
  - the long-run P6 row matches, so the residual is isolated to the high-toll pinned-share pricing diagnostic.

## 2026-07-03 advisor-revision slide version

- Created a non-overwriting advisor-revision deck:
  - source: `Documents/PreSlides/tidal_advisor_revision_20260703.tex`;
  - compiled PDF: `Documents/PreSlides/tidal_advisor_revision_20260703.pdf`.
- Added a new four-city directionality figure for the opening slide:
  - `Documents/PreSlides/figs/pat_directional_congestion_four_cities.png`;
  - `Documents/PreSlides/figs/pat_directional_congestion_four_cities.pdf`.
- Reorganized the main deck into five sections: Introduction, Model, Estimation, Counterfactuals, and Next Steps.
- Implemented the advisor's `1/3` tidal-lane screen as a policy threshold separate from the broader `0.90` prevalence measure:
  - national township pairs: 12 of 7,460;
  - Shenzhen observed grid OD pairs: 605 pairs, carrying about 34,200 commuters;
  - Beijing AM two-way centerlines: 179 of 3,670.
- Moved jobs-housing mismatch to the slide immediately before the model, removed the repeated implication bullet, and moved the tidal-lane schematic into the counterfactual section.
- Added the mode-switching limitation to the welfare slide and a visible placeholder for local road-construction cost per lane-km on the policy-comparison slide.
- Validation completed:
  - `latexmk -pdf -interaction=nonstopmode -halt-on-error tidal_advisor_revision_20260703.tex` compiled a 46-page PDF;
  - the final log has no undefined citations, undefined references, fatal errors, or overfull boxes;
  - rendered a 46-page contact sheet at `/tmp/tidal_advisor_revision_pages/contact_sheet.png` for visual QA.

## 2026-07-03 segment-level scope variant for preslides

- Created a second non-overwriting revision deck:
  - source: `Documents/PreSlides/tidal_advisor_revision_segmentonly_20260703.tex`;
  - compiled PDF: `Documents/PreSlides/tidal_advisor_revision_segmentonly_20260703.pdf`.
- Restricted the `1/3` benchmark to the road-segment slide. It is now framed as an approximate application-scope benchmark, not as the prevalence definition:
  - road-segment benchmark: `SAR <= 1/3`;
  - implied application scope: 179 AM two-way centerlines as candidate corridors.
- Kept the national trip-level, Shenzhen grid-level, and opening four-city pages on the broader `SAR <= 0.90` prevalence framing.
- Added a broad-only opening figure without the `1/3` reference:
  - `Documents/PreSlides/figs/pat_directional_congestion_four_cities_broad.png`;
  - `Documents/PreSlides/figs/pat_directional_congestion_four_cities_broad.pdf`.
- Validation completed:
  - `latexmk -pdf -interaction=nonstopmode -halt-on-error tidal_advisor_revision_segmentonly_20260703.tex` compiled a 46-page PDF;
  - the final log has no undefined citations, undefined references, fatal errors, overfull boxes, or underfull boxes;
  - rendered all 46 pages and inspected the key introduction, road-segment, and counterfactual slides.

## 2026-07-03 advisor round-2 preslides and speaker script

- Created a new non-overwriting round-2 deck:
  - source: `Documents/PreSlides/tidal_advisor_revision_round2_20260703.tex`;
  - compiled PDF: `Documents/PreSlides/tidal_advisor_revision_round2_20260703.pdf`.
- Created a new round-2 speaking script:
  - `Documents/PreSlides/tidal_speaker_script_round2_20260703.md`.
- Implemented the advisor's comments on the main deck:
  - made the motivation language more conservative and added transportation-directionality references;
  - kept only the national median on the `This Paper` slide and removed the 58% statistic;
  - moved the `SAR <= 1/3` application-scope benchmark to the segment-level framing, with 179 Beijing AM two-way centerlines, 4.9% of the sample, and about 213 km;
  - highlighted each literature-slide `This paper` sentence with the deck's blue `good` style;
  - tightened the road-segment slide text and image spacing;
  - loosened the jobs-housing slide layout;
  - removed the `future version will estimate it from ...` sentence from the estimation-parameter slide;
  - removed the unclear policy-feasibility sentence from the tidal-lane schematic slide and overlaid labels for `s`, `f`, `n_s'`, and `n_f'`;
  - removed the appendix directional-congestion-across-cities figure;
  - removed the RDD highlight in next steps.
- Clarified the model exposition:
  - the physical road setup now defines the two directed links, free-flow time `t^{ff}`, and directed lane capacity `n` without data-source language;
  - the bilateral commuting-cost slide now defines the route-inclusive bilateral cost and the market-access/balancing terms `Pi_i` and `P_j`;
  - the amenity/productivity slide now defines `chi` as the normalization constant;
  - the welfare slide now describes `Wbar` as the Frechet expected-utility/inclusive-value scale, equivalent to the utility level up to a constant.
- Added references to `Documents/PreSlides/references.bib`:
  - `arnott1993`, `couture2018`, `akbar2023`, `kreindler2024`, and `wolshon2006`.
- Added a construction-cost benchmark:
  - source: Shenzhen Transport Bureau 2020 major-project list, Nanguang expressway municipalization, CNY 6.0bn for about 31 km and 8--10 lanes;
  - retained slide benchmark: at least order CNY 20m per lane-km, implying CNY 23bn+ for 1,140 lane-km.
- Validation completed:
  - `latexmk -pdf -interaction=nonstopmode -halt-on-error tidal_advisor_revision_round2_20260703.tex` compiled a 45-page PDF;
  - the final log has no undefined citations, undefined references, fatal errors, overfull boxes, or underfull boxes;
  - custom `details/back` hyperlink targets in the main text and appendix all resolve to existing frame labels;
  - rendered and visually checked the revised tidal-lane schematic slide after the final label adjustment.

## 2026-07-02: Road-user OD car-ratio pilot (square gate5; mainline unchanged)

- Implemented hierarchical mode-share scaling in `data_work/src/stages/mode_share_scaling.py` and bundle applier `data_work/src/model/travel_cost_bundle/apply_car_ratio_od_scaling.py`.
- Method: `commuters_road = pop × car_share` with fallback OD-pair → origin-grid → work-grid → distance/core bin → citywide; grid ratios shrunk toward citywide by identified sample size (`prior_n=30`); each OD records `mode_share_source`.
- Pilot inputs/outputs (do not overwrite locked mainline):
  - `data_work/outputs/model_input_edgeproj_v2_lcc_gate5_car_ratio_v1/` (Lbar 1,648,386 vs direct 88,110; ×18.7);
  - `data_work/outputs/stage2_endogenous_edgeproj_v2_lcc_gate5_car_ratio_bpr_pos05_v1/`.
- Stage2 comparison vs `stage2_endogenous_edgeproj_v2_lcc_gate5_bpr_pos05_v1`: policy directions unchanged; Wbar pct changes for tidal / CF3 roughly 5–10% smaller; median τ/obs anchor still ≈1.000.
- **Locked decision (for now):** keep direct `type_car` mainline and slides; difference judged small enough not to switch yet.
- **Future:** replicate/overwrite full downstream suite with this method if adopted; add car+bus road-user variant; retain direct type-count as robustness.

## 2026-07-02: Theta sensitivity grid (gate5 mainline; separate robustness line)

- Added `data_work/src/model/full_estimation/run_theta_sensitivity_gate5.py`.
- Output: `data_work/outputs/theta_sensitivity_gate5_bpr_pos05_v1/` (does not overwrite stage2 / tidal mainline).
- Grid: theta ∈ {3, 5, 6.83, 8, 10}; each value re-inverts fundamentals, re-solves anchored baseline (λ_phys=1.1, α=0.05, β=−0.10), runs tidal top100 Δ=1 ST/LT with `score_bpr_st`.
- At θ=6.83 matches existing tidal mainline: ST Wbar +0.871%, LT +0.324%, LT route cost +0.065 s.
- Pattern: lower θ raises Wbar pct magnitudes; θ=3–5 show pathological negative LT route-cost decomposition (flagged in appendix table).
- Appendix draft: `THETA_SENSITIVITY_APPENDIX.md` + `theta_sensitivity_appendix_table.csv`.
- Slides: `appsens_theta` frame + speaker notes on low-$\theta$ route decomposition ($\dag$).
- Roadmap memo: `Documents/L2_notes/parameter_estimation_data_roadmap_20260702.md` (Beijing $\theta$ intuition; data/method priorities for $\theta$, $\lambda_{\phys}$, $\alpha$/$\beta$, OD, $\tau^{ff}$, reduced-form validation).

## 2026-07-02: National town-level center-periphery commuting matrix

- Added `data_work/src/national_tidal_commuting/generate_center_periphery_commuting.py`.
- Output root: `data_work/outputs/national_center_periphery_commuting_v1/`.
- Inputs:
  - `data_work/raw_data/commuting_town_2021.dta`;
  - `data_work/raw_data/commuting_town_2021_centers.parquet`.
- Raw data are read in place. The script does not move, delete, or rewrite files under `data_work/raw_data/`.
- Center definition:
  - within each prefecture-level city, townships and subdistricts are sorted by workplace-side commuter mass, equal to the column sum of the 2021 OD matrix;
  - center towns are retained until cumulative workplace-side commuter mass reaches 30% of the city total;
  - residence-side worker mass is the row sum of the same OD matrix;
  - row and column sums are commuting-data masses, not full census residential population or full employment stock.
- Scope:
  - pass 1 reads 3,737,891 positive OD rows and retains 2,292,383 intra-city rows;
  - 367 cities have all-intra city summaries;
  - 366 cities have inter-town summaries after excluding same-town OD.
- Retained outputs:
  - `data/town_center_classification.csv`;
  - `metrics/city_center_definition_summary.csv`;
  - `metrics/city_center_periphery_flows_long.csv`;
  - `metrics/city_center_periphery_flows_wide.csv`;
  - `metrics/national_center_periphery_2x2_summary.csv`;
  - `figures/figure_national_center_periphery_2x2.{png,pdf,svg}`;
  - `figures/figure_city_center_bound_imbalance.{png,pdf,svg}`.
- National all-intra 2 by 2 shares:
  - center to center, 20.4%;
  - center to periphery, 10.8%;
  - periphery to center, 11.3%;
  - periphery to periphery, 57.5%.
- National inter-town 2 by 2 shares:
  - center to center, 8.5%;
  - center to periphery, 19.7%;
  - periphery to center, 20.6%;
  - periphery to periphery, 51.2%.
- Inter-town periphery-to-center demand is only modestly higher than center-to-periphery demand in the national aggregate: ratio 1.044 and difference 0.87 percentage points. City-level heterogeneity is more informative for the tidal-demand motivation. Among high-flow cities, Beijing, Dalian, Zhengzhou, Hangzhou, Shenyang, Shanghai, Guangzhou, Ningbo, Suzhou, Qingdao, Changsha, Xian, Chengdu, Shenzhen, and Wuhan have positive center-bound imbalance in the top-40 city figure.
- Added threshold and city-group diagnostics:
  - script: `data_work/src/national_tidal_commuting/generate_center_periphery_group_diagnostics.py`;
  - output root: `data_work/outputs/national_center_periphery_group_diagnostics_v1/`;
  - thresholds: 20%, 25%, 30%, 40%, and 50% of each city's workplace-side commuter mass;
  - city groups: all cities, top 100, top 50, top 20 by inter-town commuter mass, and a manual first-tier group containing Beijing, Shanghai, Guangzhou, and Shenzhen.
- Retained diagnostic outputs:
  - `metrics/threshold_group_summary.csv`;
  - `metrics/threshold_city_flows_wide.csv`;
  - `metrics/threshold_city_flows_long.csv`;
  - `figures/figure_threshold_sensitivity_center_bound_imbalance.{png,pdf,svg}`;
  - `figures/figure_group_2x2_at_30pct.{png,pdf,svg}`.
- Threshold sensitivity result:
  - all-city inter-town center-bound imbalance is positive but modest across the five thresholds, ranging from 0.70 to 1.08 percentage points;
  - top-50 imbalance is stable around 3.00 to 3.35 percentage points;
  - top-20 imbalance is stable around 3.92 to 4.50 percentage points;
  - first-tier imbalance ranges from 6.18 to 7.31 percentage points.
- Baseline interpretation: 30% is retained as the main employment-core threshold because it captures a concentrated workplace core without turning half of the city into the center. The main evidence is not threshold-maximized: the large-city and first-tier patterns remain positive under 20%, 25%, 40%, and 50% definitions.

## 2026-07-02 latest preslides and minimal package validation

- Synchronized the latest `Documents/PreSlides/tidal.tex` into `tidal_model_slides_minimal/`.
- Updated the package sync list to include:
  - the new video preview and run-linked `.mov` asset under `figs/TidalVedio/`;
  - the field-visit appendix photos;
  - the jobs-housing appendix figures;
  - compact descriptive reference metrics for national, Shenzhen, Beijing centerline, and jobs-housing slide values.
- Added `scripts/slide_metric_checks.py` and wired it into `make check` and `validate_full_model.py --compare-only`.
- Package checks completed:
  - `make check` resolved 23 slide graphics, one run-linked video asset, aggregate model inputs, the 127-row `FILE_MAP.csv`, and descriptive slide metrics.
  - `make slides` compiled the package deck to 42 pages.
  - `make smoke-model` converged with `Wbar=3.200092` and `median_tau_obs_ratio=1.0001`.
  - `make validate-full` reran Stage 2, tidal diagnostics, policy comparison, alpha-beta sensitivity, and the symmetry-to-directionality suite inside the package.
  - `python scripts/validate_full_model.py --compare-only` matched every checked model table with maximum absolute difference `0`.
- Retained descriptive metric checks:
  - grid Duncan `0.260`, random-allocation floor `0.004`, radial inbound share `74.5%`;
  - national AM flow-weighted SAR `0.878`;
  - Shenzhen observed-flow SAR `0.819`;
  - Beijing AM centerline SAR `0.884`.
- Rendered a 42-page contact sheet at `tidal_model_slides_minimal/outputs/qa_render/contact_sheet.png`; no missing images, blank pages, or obvious slide-level overlap were detected.
- The LaTeX log retains one small overfull vbox warning, `1.392pt` on the radial-inbound appendix page. The compile has no missing figures, undefined references, fatal errors, or undefined citations.

## 2026-07-01: Symmetric-input place and bilateral-cost diagnostic

- Added `data_work/src/model/full_estimation/run_symmetric_input_place_cost_suite_gate5.py`.
- Output root: `data_work/outputs/symmetric_input_place_cost_suite_gate5_bpr_pos05_v1/`.
  - `place_cost_effects_by_world.csv`: directed and symmetric world effects.
  - `over_under_place_cost.csv`: symmetric-minus-directed bias rows.
  - `place_cost_discussion_digest.csv`: compact discussion table.
  - `treatment_sets.csv`: node and OD treatment-set coverage.
- Diagnostic design:
  - both worlds are fully re-estimated from their own input cost surfaces;
  - treatment sets are common physical nodes or OD pairs selected from directed-world diagnostics;
  - no tidal-lane margin, lane reallocation, or tidal candidate score enters the suite;
  - place shocks scale productivity or amenity fundamentals by `1.10`;
  - bilateral travel-cost shocks reduce selected OD costs by `10%`.
- Treatment-set coverage:
  - high-asymmetry workplace productivity targets cover `57.1%` of observed jobs and have weighted mean absolute log asymmetry `0.577`;
  - high-asymmetry residence amenity targets cover `46.1%` of observed residents and have weighted mean absolute log asymmetry `0.661`;
  - high-asymmetry OD cost targets cover `9.20%` of observed flow and have weighted mean absolute log asymmetry `2.237`;
  - top-flow OD cost targets cover `17.38%` of observed flow.
- Retained results:
  - place-based fundamental shocks have modest symmetric-minus-directed differences. Endogenous high-asymmetry job productivity has welfare bias `-0.0012` percentage points, OD-cost bias `-0.531` seconds per commuter, and OD-sorting bias `-0.013` percentage points.
  - bilateral travel-cost shocks produce clearer sorting wedges. Top-flow OD cost reduction has welfare bias `+0.0140` percentage points and OD-cost bias `+0.431` seconds per commuter.
  - high-asymmetry OD cost reduction has welfare bias `+0.0129` percentage points, OD-cost bias `+0.265` seconds per commuter, workplace-sorting bias `+0.075` percentage points, and OD-sorting bias `+0.089` percentage points.
  - uniform bilateral cost reduction has a direct baseline-OD cost effect near `-33` seconds per commuter, but sorting toward longer OD markets raises the post-sorting average cost; the decomposition fields record direct and sorting components separately.
- Validation completed:
  - `python -m py_compile` passed for the new runner;
  - all reported equilibria converged;
  - no-shock fixed-cost QA reproduces each endogenous baseline with Wbar difference `0` and residence/workplace L1 errors below `4e-09` percentage points;
  - the OD-cost decomposition closes to numerical zero, with maximum error below `7e-14` seconds.
- Current interpretation:
  - symmetric-input re-estimation does not create a strong place-based-policy welfare contrast in the current shock grid;
  - the more useful object is the bilateral-cost and sorting decomposition, especially for targeted OD cost reductions that interact with directional access.

## 2026-07-01: Welfare gauge fix and mainline rerun

- Fixed aggregate welfare in `data_work/src/model/full_estimation/equilibrium_solve.py` and `data_work/src/model/directional_cost/fixed_tau_model.py`.
- The reported `Wbar` now uses the primitive Frechet denominator, `sum_ij tau_ij^{-theta} u_i^theta w_j^theta`, rather than the raw solver-gauge `sum_i u_i^theta Pi_i^{-theta}`.
- Retained `Wbar_pi_raw` in full-estimation equilibrium metadata as a diagnostic-only legacy gauge-dependent scale.
- Added theory note: `Documents/L2_notes/welfare_gauge_diagnostic_20260701.tex`.
- Re-ran retained mainline outputs:
  - `data_work/outputs/tidal_lane_diagnostics_edgeproj_v2_lcc_gate5_bpr_pos05_v1/`;
  - `data_work/outputs/welfare_sorting_diagnostics_gate5_bpr_pos05_v1/`;
  - `data_work/outputs/alpha_robustness_quick_gate5_v1/`;
  - `data_work/outputs/alpha_beta_sensitivity_bpr_pos05_v1/`;
  - `data_work/outputs/policy_comparison_gate5_bpr_pos05_v1/`;
  - `data_work/outputs/phase_c_composite_cf_gate5_v1/`;
  - `data_work/outputs/tidal_lt_selection_compare_v1/`;
  - `data_work/outputs/stage2_endogenous_edgeproj_v2_lcc_gate5_bpr_pos05_v1/`.
- Main bpr+pos05 top100 result after the fix: short-run `Wbar +0.871%`, long-run `Wbar +0.324%`; OD-weighted commute-cost long-run total remains near zero at `+0.065` seconds per commuter.
- Updated `Documents/PreSlides/tidal.tex`, `Documents/L2_notes/model_and_results_summary_20260630.md`, `Documents/L2_notes/tidal_lane_st_lt_congestion_diagnostics_20260629.md`, and `Documents/L2_notes/phase_d_results_narrative_20260629.md`.
- Corrected Result 2 market-access map generation to use the gauge-free resident-access object `sum_j tau_ij^{-theta} w_j^theta`; raw `(Pi,P)` maps are no longer used for welfare interpretation.
- Updated the `This Paper` slide to three model contributions: national-to-segment directional congestion facts, model quantification of what symmetry misses, and tidal-lane policy comparison.
- Added symmetry experiment design note: `Documents/L2_notes/symmetry_to_directional_model_experiments_20260701.md`.

## 2026-07-01 preslides model deck and minimal share package

- Updated `Documents/PreSlides/tidal.tex` and rebuilt `Documents/PreSlides/tidal.pdf`.
- Moved the former alpha-beta Result 4 into the appendix as sensitivity analysis.
- Reframed the main exogenous-versus-endogenous comparison as Result 4. The main table now uses the source structural counterfactual files rather than the stale merged summary table.
- Reframed the infrastructure comparison as Result 5. The slide now benchmarks the tidal-lane LT welfare gain against targeted and uniform lane expansion in lane-km units, without treating it as a monetary cost calculation.
- Corrected the appendix exogenous/endogenous table to match:
  - `structural_cf_exogenous.csv`;
  - `structural_cf_endogenous.csv`;
  - `tidal_lane_exogenous_bpr_grid.csv`.
- Built the standalone share package at `tidal_model_slides_minimal/`.
  - The package compiles `tidal.tex` without absolute figure paths.
  - It includes all slide figures, aggregate model inputs, reference result tables, copied model code, `Makefile`, package checks, a smoke-model script, and sync documentation.
  - `docs/FILE_MAP.csv` records package paths, main-project paths, file type, and source notes.
- Validation completed:
  - main deck compiled to 40 pages with no missing figures or undefined references;
  - package deck compiled to 40 pages using only local figure paths;
  - `make check` passed in the package;
  - `make smoke-model` passed in the package with fixed aggregate inputs and anchored endogenous baseline convergence.
- Full package validation was completed after the initial smoke test. The package now includes `make validate-full`, which reruns Stage 2 full, tidal diagnostics, policy comparison, and alpha-beta sensitivity inside the package and compares reproduced outputs to packaged reference results. The reproduced tables matched exactly except for floating-point roundoff below `1.2e-13` in policy comparison tables.

## 2026-07-01 discussant audit fixes and refreshed model deck

- Addressed audit items A1--A6 and B1--B4; B5 was intentionally left unchanged.
- Code updates:
  - clarified CF1 as a fixed-cost measurement diagnostic versus an anchored endogenous re-solve initialized from the same symmetrised table;
  - retained CF3 exogenous as a travel-time measurement shock and CF3 endogenous as a capacity shock, with slides now labelled accordingly;
  - recomputed P3 qdelay lane-km from its own selected reciprocal pairs instead of reusing the P2 same-corridor budget;
  - pinned P4 and P5 short-run runs to baseline residence and workplace shares;
  - computed cost-effectiveness from long-run rows only;
  - fixed P6 toll revenue to use priced-equilibrium traffic and relabelled P6 as a pricing diagnostic in the deck;
  - documented `lambda_phys=1.1` as the mainline congestion elasticity and kept legacy `lambda_=0.07144` for exogenous diagnostics.
- New policy comparison output: `data_work/outputs/policy_comparison_gate5_bpr_pos05_auditfix_v1/`.
  - P2/P4 matched lane budget: `1091.495705` lane-km.
  - P3 qdelay lane budget: `1140.296788` lane-km.
  - Long-run welfare: P1 tidal `+0.324315%`, P2 same-corridor `+0.381536%`, P3 qdelay `+0.444483%`, P4 uniform matched `+0.099513%`.
- Slide updates:
  - Result 4 is now framed as measurement benchmarks versus capacity policies, not as same-shock attenuation.
  - Result 5 is the link-level welfare-targeting slide.
  - Result 6 reports the welfare-equivalent infrastructure benchmark with P2/P4 at `1,092` lane-km and P3 at `1,140` lane-km.
  - Appendix policy rows now label P6 as a pricing diagnostic and report P3 lane-km correctly.
- Standalone package updates:
  - synchronized `tidal_model_slides_minimal/tidal.tex`, model scripts, `map_link_welfare_elast.png`, reference policy tables, sync script, `FILE_MAP.csv`, and validation notes.
  - Package policy comparison rerun matched packaged reference tables exactly.
- Validation completed:
  - main and package PDFs compile to 41 pages;
  - `make check` passed inside `tidal_model_slides_minimal/`;
  - `python scripts/validate_full_model.py --compare-only` passed with max absolute difference `0` across all checked tables;
  - rendered main deck contact sheet for visual QA and found no missing figures or obvious overlap.

## 2026-07-01 symmetry-to-directionality suite and slide update

- Added and validated `data_work/src/model/full_estimation/run_symmetry_directionality_suite_gate5.py`.
- Output root: `data_work/outputs/symmetry_directionality_suite_gate5_bpr_pos05_v1/`.
  - `summary_by_scenario.csv` has 18 rows.
  - `suite_meta.json` records the run manifest.
- Diagnostic design:
  - directed and symmetric worlds use the same shares, fundamentals, parameters, and welfare formula;
  - the symmetric world replaces directed reciprocal costs with geometric symmetric costs;
  - the corrected endogenous symmetric run uses symmetric `tau_obs_anchor` and symmetric `tau_ff`.
- Main retained results:
  - cost-surface diagnostic, anchored GE: welfare `+1.103376%`, residence dissimilarity `3.363766%`, workplace dissimilarity `6.147019%`;
  - hub-productivity shock, anchored GE: directed OD-cost response `+1.933513%`, symmetric OD-cost response `+1.809505%`;
  - auxiliary transport rows are retained in the output but not interpreted as symmetric tidal-lane evidence, because the symmetric world removes the directionality motive.
- Retained caveat: policy rows reselect top-100 corridors inside each cost world, so they are a model-design diagnostic rather than a fixed-corridor treatment comparison.
- Slide updates:
  - revised `This Paper` in a vertical contribution layout and rewrote the second contribution as quantifying what symmetry misses;
  - added main-text slide `Model Diagnostic. What Symmetry Misses`;
  - added appendix slide `Appendix: Symmetry-to-Directionality Suite`.
- Standalone package updates:
  - synchronized `tidal_model_slides_minimal/` with the new script, reference results, `FILE_MAP.csv`, and validation notes;
  - `make validate-full` now includes the symmetry-to-directionality suite.
- Validation completed:
  - main and package PDFs compile to 43 pages;
  - package `make check` passed;
  - package symmetry suite reran independently and reproduced all 18 reference rows exactly;
  - `python scripts/validate_full_model.py --compare-only` passed with max absolute difference `0` across all checked tables.

## 2026-07-01 symmetric-input full re-estimation and policy-grid diagnostic

- Added `data_work/src/model/full_estimation/run_symmetric_input_reestimated_suite_gate5.py`.
- Built symmetric input bundle `data_work/outputs/model_input_edgeproj_v2_lcc_gate5_symmetric_geometric_v1/`.
  - Reciprocal AM observed times and night free-flow times are replaced by pairwise geometric means.
  - Network topology, lanes, OD flows, node marginals, parameters, solver settings, and the welfare formula are unchanged.
- Full re-estimation output: `data_work/outputs/symmetric_input_reestimated_gate5_bpr_pos05_v1/`.
  - Baseline `Wbar` differs by `+0.035%` after symmetric-input inversion, because the inversion absorbs much of the cost-surface difference into `ubar_theta` and `abar_theta`.
  - The retained interpretation is not a baseline-welfare claim. The useful object is whether policy counterfactuals differ after the symmetric model is re-estimated from its own inputs.
- Added `data_work/src/model/full_estimation/run_symmetric_input_policy_grid_gate5.py`.
- Policy-grid output: `data_work/outputs/symmetric_input_policy_grid_gate5_bpr_pos05_v1/`.
  - `policy_grid_effects_by_world.csv`: 76 world-policy rows.
  - `over_under_policy_grid.csv`: 38 symmetric-minus-directed bias rows.
  - `selection_overlap.csv`: 12 top-K selection-overlap rows.
  - `policy_grid_discussion_digest.csv`: compact rows for slide or memo discussion.
  - All 76 counterfactual equilibria converged.
- Diagnostic design:
  - `own_world`: each world selects treated corridors from its own baseline, measuring policy-design error.
  - `directed_fixed`: both worlds evaluate the directed-main treated corridors, measuring policy-evaluation error for the same physical policy.
- Retained results:
  - For own-world tidal and same-corridor policies, top-K selection overlap is limited. Overlap shares for tidal are `60.0%`, `66.0%`, `57.0%`, and `50.5%` at top 25, 50, 100, and 200.
  - Q-delay expansion is more stable. Overlap shares are `88.0%`, `80.0%`, `87.0%`, and `89.0%` at the same top-K values.
  - For the same physical directed corridors, prediction bias is modest. For top-100 tidal, the OD cost bias is `-0.145` sec./commuter and workplace-sorting bias is `+0.021` percentage points.
  - When each model selects its own top-100 tidal policy, the OD cost bias is `-0.619` sec./commuter and workplace-sorting bias is `-0.275` percentage points.
  - The largest own-world sorting bias in the default grid occurs for top-200 tidal, with workplace-sorting bias `-0.482` percentage points and OD-sorting bias `-0.481` percentage points.
- Current conclusion:
  - The symmetric-input re-estimation does not support a strong baseline-welfare contrast.
  - It supports a more specific counterfactual claim: symmetric inputs can leave baseline fit nearly unchanged while changing which corridors are selected and how much capacity policies move sorting and OD costs.
