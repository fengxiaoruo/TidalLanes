# Work Log

## 2026-09-28：文档换版、复现包改用 Tidal0715、路径核试算

- 按用户决定把四份旧口径文档换成新版并归档旧版：`Documents/L2_notes/model_and_results_summary_20260928.md`（旧版在 `_superseded/model_and_results_summary_20260630.md`）；`Documents/L2_notes/TidalLanes_estimation_notes_20260928.tex/.pdf`（旧版在 `_superseded/…_20260630.*`）；`Documents/L1_manuscripts/TidalLanes_Model_v4.tex` 的 (welfare_MA) 段改为"在 (Π,P) 归一化条件下等价、实现用 Fréchet 分母、反事实按水平重解"（旧文在 `drafts/TidalLanes_Model_v4_pre20260928.tex`）；`data_work/src/model/full_estimation/ESTIMATION_NOTES.md` 与包内同步（旧版在 `data_work/docs/_superseded/ESTIMATION_NOTES_full_estimation_v1_20260514.md`）。`DOCUMENT_INDEX.md` 同步。
- 新版摘要的通勤时间列全部改为物理时间口径；P6/P7 行标记为实现有误并撤出主表；αβ 表改为 ST direct / LT total / person-min。历史 memo 中对旧文件名的引用按索引规则保留不改。
- `tidal_model_slides_minimal/` 的包内 deck 改为 `Documents/PreSlides/Tidal0715.tex`（包内仍名 `tidal.tex`；PDF 来自 `Tidal0718.pdf`；讲稿 `tidal_speaker_script_Tidal0715_20260718.md` 改名 `tidal_speaker_script.md`）。manifest 改为 27 张图加视频；`data_work/outputs/od_validation_…/figure_od_validation_structural_central99.pdf` 在同步时复制到 `figs/` 并改写路径。`make check` 通过（102 行 file map），`make slides` 编译 43 页无错误。旧的 reduced deck 不再进包。
- 新增 `data_work/src/model/full_estimation/run_route_kernel_exp_trial_20260928.py` 与输出 `data_work/outputs/route_kernel_exp_trial_20260928_nu00122_v2/`、`…_nu00420_v2/`（首次运行因弹性符号写反归档为 `…_v1_failed_20260928_01/`）。幂核单元检查复现主线（W̄ 3.2001、ST +0.871%、LT +0.324%、42.1 s / −14.4 s）。
- 观测通勤对网络最短路分钟的 PPML 半弹性为 0.0122/min（全样本）与 0.0420/min（≤60 分钟），log-log 弹性 1.36；主线模型 OD 的对应值 0.087/min 与 3.3；观测流量对 ln R 的斜率 0.165。观测通勤量加权最短路均值 121 分钟、34% 的通勤人数在 90 分钟以上，网格分钟数偏高。
- 发现主线对 `R=(I−A)^{-1}` 的 1e-12 截断在 88.8% 的 OD 格绑定（承载 30.7% 观测通勤人数）；去掉截断后主线 ST/LT 福利变为 +1.07%/+0.29%，ST/LT 时间变为 50.5 s / −18.7 s，区内占比 56.9%。
- 指数核 ν=0.0122：区内 6.3%、baseline 期望通勤 113 分钟、ST +0.53%、LT +0.33%、ST 省 112 s、LT −3 s；ν=0.042：区内 15%、ST +2.44%、LT +0.56%、ST 省 173 s、LT +108 s。定性结论稳定，幅度不稳定。全部为试算，未替换主线。
- 同日启动的 02:00–05:00 自由流变体与选边分日检验由后台任务执行，结果另记。


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

## 2026-08-27：彩田路视频断面对应百度子路段及双向速度图

- 使用 2026-07-10 进城视频 QuickTime GPS（纬度约 22.5636--22.5645、经度约 114.0617--114.0634）匹配百度有向路网，锁定视频断面所在的红荔路至北环彩田立交桥出入口段。南向北 `RS_NB1521432208_NB1521432620_1` 与北向南 `RS_NB1521432253_NB1521439708_1` 距视频 GPS 最近点分别为 2.4 m 和 2.3 m。
- 新增可复现脚本 `data_work/src/shenzhen_caitian_baidu/plot_video_segment_speed_timeseries.py`，输出在 `data_work/outputs/shenzhen_caitian_video_baidu_segment_v1/`。两个方向在 2025-05-19 至 2025-05-25 均保留完整 1,008 个十分钟原始速度观测，不插值、不平滑、不再对整条约 5 km 彩田路走廊加总。
- 保留空间匹配图、一周连续时间序列、七日分面图及 PNG/PDF/SVG 版本；绘图数据、匹配摘要、路段 GeoJSON 和 GPS 点表同时落地。该输出用于把现场视频流量证据与同一百度子路段的历史速度形态连接，不构成同年同日的速度--流量联合估计。
- 补充审核视频 GPS 周边 250 m 的完整百度路网及原始速度文件。没有更短的彩田路主路段穿过摄像机断面；约 343 m 的下一段南向北路段和约 139 m 的下一段北向南路段分别位于摄像机北侧约 180 m 和 193 m，约 650 m 的近邻候选为北环大道辅路且方向不符，均不替换当前匹配。当前 1.6 km 段的工作日启用窗口均速仅由 31.35 降至 28.08 km/h；图中的显著向北低速主要由 2025-05-20 驱动，当日参考窗至启用窗由 26.46 降至 14.89 km/h，且北侧相邻南向北路段同日也普遍减速，支持走廊级拥堵而非单段误匹配解释。

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

## 2026-07-27：记录 directional cost-response / corridor validation 想法

- 新增 `Documents/L2_notes/tau_cost_response_corridor_validation_idea_20260727.md`。
- 锁定表述：该思路是校验「潮汐/方向性容量冲击 → 成本变化」并接到真实动线设计，不是识别 \(\tau_{ij}\) 水平；全国可用 2024/2025 百度城样本，必要时走廊/区县加总；深圳已有潮汐车道作对照。
- 可执行分包：A 深圳已开通走廊 cost-proxy 对照（最小起步）；B 全国拥堵指数–不对称散点；C 北京模型 \(\Delta\tau\) 与走廊评分交叉核对。`nextstep.md` 低优先级项 #10 已挂接。

## 2026-07-26：彩田路 0724 视频时间与状态窗口首轮整理

- 完成 `flowvedio/2026072324彩田路/` 的视频时间、方向和连续覆盖核对。`IMG_0022.mov` 填补进城方向 16:30–17:30；`IMG_0020.mov` 已完成导入并覆盖出城方向约 14:42–16:07。
- 出城方向 16:07:43–16:26:46 暂按缺失处理，不插值、不记为零流量。约 18:20–20:00 为计划停采，两个方向的恢复过程均不可直接定位。
- 新增可复现抽帧脚本 `flowvedio/create_20260724_transition_review.py`，输出在 `flowvedio/batch_outputs/20260724_caitian_transition_review_v1/`，包含分钟级、10 秒级开通审核图和恢复前后审核图。
- 新增 `lane_state_plan_20260724_v1.csv` 与 `transition_window_inventory_20260724_v1.csv`。进城开通保留为待车流进一步定位的候选窗口；出城开通和双向恢复均按区间删失处理。
- 锁定识别限制：不同手机、不同机位或不同 geometry 的 series 不直接拼成窄带 RDD / RDiT。当前先用同一机位内车流定位状态边界，并只对稳定窗口做描述性比较。

## 2026-07-27：彩田路 0723 晚间与 0724 实施窗口人工锁定

- 根据用户逐视频复核，锁定 0723 晚间恢复：进城 `IMG_3026.MOV` 片内 01:15:00–01:32:06，出城 `IMG_0019.MOV` 片内 01:20:00–01:35:00；`IMG_0018.MOV` 因仅 13.3 秒正式排除。
- 锁定 0724 进城开通为 `IMG_0006.MOV` 片内 01:34:00–01:47:20；`IMG_0022.mov` 从开头即为完成状态。出城开通开始仍处于缺失区间，`IMG_3028.mov` 片内 00:02:15 为首次调整后车流。
- 锁定 0724 晚间恢复：进城 `IMG_0008.MOV` 片内 01:30:50–01:40:00，出城 `IMG_3029.mov` 片内 01:31:25–01:39:50。
- `IMG_0008.MOV` 前 5 秒排除；片内 01:09:14–01:09:30 剧烈晃动排除，晃动前后使用不同 geometry。`IMG_3029.mov` 前 5 秒排除。
- 正式状态表为 `flowvedio/batch_outputs/20260724_caitian_transition_review_v1/lane_state_plan_20260724_v2.csv`；0723 晚间表为 `flowvedio/batch_outputs/20260723_caitian_point_review/lane_state_plan_20260723_evening_v1.csv`。22 张人工定点网格图和空白流量线表位于 `flowvedio/batch_outputs/20260723_24_confirmed_manual_locator_v2/`。

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

## 2026-04-29: future Baidu crawl variants should retain `tactics=0`, `tactics=2`, and `tactics=3`

- Retained decision for the next national commuting crawl code revision: support collecting `directionlite/v1/driving` outputs for the same OD pairs and real clock windows under all three route strategies.
- Interpretation to preserve in documentation and output metadata:
  - `tactics=0`: regular/default fast route strategy; useful as a common-strategy baseline.
  - `tactics=2`: congestion-avoidance strategy; closest to the intended object of a real-time congestion-adjusted recommended fastest route.
  - `tactics=3`: shortest-distance strategy; useful as a route-choice robustness check.
- The main paper specification can later choose between `tactics=0` and `tactics=2`; the other strategy should be retained as a robustness version, with `tactics=3` used to diagnose whether route selection rather than directional congestion drives the asymmetry.
- Also recorded in `data_work/docs/NATIONAL_TIDAL_COMMUTING_WORKFLOW.md` under the Baidu crawl section so this decision is visible when modifying crawl code.

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

## Workflow rule

- `nextstep.md` only keeps the current status, the immediate next action, and short plan bullets.
- Completed work, locked parameters, historical comparisons, and retained decisions are recorded here.
- When a task is finished, remove it from `nextstep.md` and append the result here instead of leaving stale history in the live plan.

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

## 2026-07-15 Jobs-Housing major-city slide integration

- Added `data_work/src/national_tidal_commuting/plot_jobs_housing_asymmetry_slides.py` to merge the top-20 city jobs-housing directionality table with the 2025 top-100 city township-pair AM speed asymmetry summary.
- Generated `data_work/outputs/national_jobs_housing_asymmetry_slides_v1/` with the merged top-20 table, group summary, correlation summary, manifest, and three slide-ready figures. The same figures were copied to `Documents/PreSlides/figs/`.
- Retained headline statistics: all top-20 cities have $P\to C$ above $C\to P$; the flow-weighted top-20 shares are 19.80% and 15.30%, giving a 4.50 pp gap. The all-city gap is 0.87 pp.
- The city-level link to AM speed asymmetry is weak in the top-20 cross section: Pearson $r$ with AM SAR is 0.11 for the $P\to C-C\to P$ gap and 0.09 for the $P\to C$ share. The slides therefore treat the correlation figures as descriptive diagnostics.
- Updated `Documents/PreSlides/Tidal0715.tex` by adding three Jobs-Housing frames after the existing grid-level mismatch page: top-20 directional shares, SAR scatter diagnostics, and the two-point city dumbbell. Compiled `Documents/PreSlides/Tidal0715.pdf` successfully at 49 pages and visually inspected the new pages 12--14 via `pypdfium2`.

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

## 2026-07-23 Caitian Road video inventory and geometry review package

- Catalogued six videos in `flowvedio/20260723彩田路/` using the recorded creation timestamps and actual media durations. The retained inventory covers two camera views from approximately 14:41 to 18:17.
- Split `IMG_3024.MOV` and `IMG_0017.MOV` into normal, deployment, and tidal-lane operating states. The two views place completed reallocation at 16:26:17 and 16:26:34. The deployment intervals remain view-specific because their visible start times differ.
- Recorded the capacity reallocation as south-to-north 3 to 4 lanes and north-to-south 4 to 3 lanes. Deployment intervals are excluded from stable-state measurement.
- Created nine full-resolution raw frames and nine 100-pixel grid overlays in `flowvedio/batch_outputs/20260723_caitian_point_review/`. The geometry set separates each file transition, the pre/post lane configurations, and the `IMG_0017.MOV` camera adjustment at 1:10:08.
- Added a direction-specific confirmation table with two speed gates per direction, lane or flow point fields, and physical gate-distance fields. Travel speed will not be reported in km/h until the physical longitudinal distance between each gate pair is confirmed.
- Locked the nine user-confirmed gate geometries. Each transverse gate is split into south-to-north and north-to-south segments at the supplied center point. The retained table contains 18 direction-specific gate pairs, and the review package includes nine annotated full-resolution frames plus a contact sheet.
- Physical gate distances remain pending. The locked geometry supports vehicle passage-time measurement; conversion to km/h is deferred until the corresponding longitudinal road distances are supplied.

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
## 2026-08-25 July 24 outbound directional-flow time series

- Added `flowvedio/plot_20260724_outbound_direction_timeseries_v1.py` and exported separate northbound and southbound total-flow figures (PNG, SVG, and PDF) to `flowvedio/batch_outputs/20260723_24_state_dependent_recognition_v5/analysis_basic_v1/outbound_tidal_region_opening_20260724_v1/`.
- The figures use complete five-minute bins on a common 0--75 vehicles/minute scale. They mark the exact last-30-minute pre-opening window, the recording gap, the 2-minute-15-second operation tail, and the first stable 30-minute post-opening window.
- Retained adjacent-window means are 40.5 to 54.0 vehicles/minute northbound and 43.4 to 28.6 vehicles/minute southbound. The plotted observations were also exported as `direction_total_flow_timeseries_plot_data_v1.csv`.

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

## 2026-08-25 彩田路 0723/0724 出城摄像头 RDiT specification search v1

- 新增脚本 `flowvedio/analyze_outbound_rdit_specification_search_v1.py`，独立输出到 `flowvedio/batch_outputs/20260723_24_state_dependent_recognition_v5/analysis_basic_v1/outbound_rdit_specification_search_v1/`，未覆盖旧 RDiT 结果。
- 共保留 37,560 条可估计结果。规格覆盖 1/2/5 分钟 bin、10/15/20/30/37/45/60 分钟带宽、0/2/5 分钟 stable donut、0/1/2 次局部多项式、共同/两侧趋势、uniform/triangular kernel 和 HAC/HC1 标准误。
- Outcome 包括两方向总流量、双向总流量、北/南固定区域、潮汐可变区域、方向差、固定区差、control-style 组合差、北向份额和 log 方向流量比。普通几何可以进入总流量 outcome，但区域 outcome 设为 missing 而不是补零。
- 0724 开通的 60 分钟窗口有 37 个 pre bins。缺口来自 16:07:43–16:26:46 无录像及之后被删除的操作尾段，不是设备限制。
- 与旧 v2 完全同口径的 1 分钟、60 分钟、局部线性两侧趋势、uniform-kernel HAC 结果已逐事件精确复核。0724 开通为北向 +14.588、南向 -7.346、北向减南向 +21.934；0724 恢复为北向 -11.221、南向 +3.543、北向减南向 -14.764。0723 开通的总流量断点接近零。
- 输出包含逐 outcome 事件研究图和关键 outcome 带宽图，共 56 组 PDF，并同步保留 PNG 与 SVG。大量规格带来多重检验风险，因此不按最小 p 值挑选结论；稳定性汇总限制为局部常数/线性且断点两侧至少 8 个 bins。
- 2026-08-26 新增独立单车道流量 supplement：脚本 `flowvedio/analyze_outbound_rdit_per_lane_supplement_v1.py`，输出为相邻目录 `outbound_rdit_per_lane_supplement_v1/`。共保留 34,920 条估计和四组事件研究图。0724 开通时北向每车道流量基准跳变为 +0.474（p=0.544），获益方向减另一方向每车道流量为 -0.086（p=0.927）；0724 恢复时获益方向每车道流量为 -0.916（p=0.059），相对差为 -1.218（p=0.146）。单车道结果不提供独立的正向政策证据，定位为容量利用率分解，不替代总流量 reduced form。

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
## 2026-08-28 — RDiT report figure-role clarification

- Revised `caitian_video_rdit_collaborator_report_20260827.pdf` so Figure 1 shows only vehicle detections and the near/far counting lines; it no longer overlays the fixed-north, tidal-variable, or fixed-south physical regions.
- Moved the standalone three-region road schematic to immediately after the first paragraph of Section 3, where regional flow construction is introduced.
- Replaced the generic vehicle-inclusion description with the four counted classes: car, motorcycle, bus, and truck.
- Reformatted Section 5 event-study figures into two panels per row (opening on the left, recovery on the right), reducing the report from 18 to 14 pages while retaining readable axes and legends.
- Renamed Section 5 to `Flow Scatterplots` and removed the unnecessary note that the RDiT treatment coefficient is sometimes denoted by $\tau$.
- Regenerated the annotated frames and recompiled the final 14-page PDF; the revised pages were visually checked.
## 2026-09-28 Gate5 free-flow 02:00--04:59 window and top-100 selection split-day check

- Added `src/diagnostics/build_freeflow_speed_definition_caches_v2.py` (chunked read, `--definitions` option) and built two caches under `outputs/freeflow_speed_definitions_v2/`: `ff_0205_all_harmonic` and `ff_0205_all_p90` (stamps with `hour_of_day` in {2,3,4}, all seven days, 24,340 road segments, median 124 observations). Median segment speed ratios vs the retained 22:00--05:59 harmonic cache: 1.0138 and 1.0960. The v1 root and the retained cache were not modified.
- Reran the square-AM pair-specific edge projection, gate5 LCC bundle, QA, and Stage 2 quick for both. Every bundle has 1,104 nodes, 2,642 edges, and 310,525 OD pairs, passes QA, and keeps AM `tau_obs_min` identical to the mainline. Edge-level median free-flow time ratios: 0.9862 and 0.9074. Share of gate5 edges with `tau_obs_min < tau_ff_min`: 7.76 percent (205 edges) and 0.34 percent (9), against 8.29 percent (219) in the mainline. Median congestion ratio 1.172 and 1.279 vs 1.149.
- Stage 2 quick: baseline Wbar 3.200103 and 3.200318; top-25 tidal ST welfare change 0.441 and 0.470 percent (mainline 0.438), LT 0.143 and 0.148 percent (mainline 0.142); top-25 overlap with the mainline quick run 24 and 23. Summary and README in `outputs/freeflow_robustness_gate5_ff0205_v1/`. Retained decision unchanged: the 22:00--05:59 harmonic definition stays the mainline; the narrower window does not remove the roughly 8 percent of edges with negative measured congestion, only the 90th-percentile rule does.
- Winner's-curse check for the top-100 `score_bpr_st` selection: added `src/diagnostics/build_am_day_split_speed_caches.py` (exact replica of the retained AM rule, weekday 07:00--08:59 harmonic; pooled split reproduces the retained AM cache to 6.7e-16) and built `am_odd_days` (05-19, 05-21, 05-23) and `am_even_days` (05-20, 05-22) caches in `outputs/am_day_split_speed_caches_v1/`, then edgeproj, gate5 bundles, and QA for each (odd 1,104/2,644, even 1,104/2,646 edges, both 310,525 OD pairs, QA passed).
- Added `src/model/full_estimation/run_selection_split_day_check_gate5.py`. Output in `outputs/selection_split_day_check_gate5_v1/`. The mainline bundle's own selection reproduces the retained top-100 set and numbers. Matched on the same transferable pairs, selecting on one half-week and evaluating on the other gives ST direct effect 40.6 s vs 42.0 s in-sample (ratio 0.968), ST Wbar 0.835 vs 0.865 percent, LT Wbar 0.307 vs 0.319 percent, treated-edge Xi-weighted delta tau 3.0--3.2 percent smaller. Unrestricted gap 5.6--6.1 percent, about half of it from 3--6 non-transferable pairs. Top-100 overlap odd vs even 82 (Jaccard 0.69), each vs mainline 88--89. Conclusion: selection on noisy week speeds inflates the direct effect by about 3 percent, an upper bound for the full-week selection; no mainline conclusion changes.

## 2026-10-29：联通flow验证与网络研究想法归档

- 已核对Speed正式389份道路压力CSV目录、RN几何摘要、有向路由图、Speed到OSM映射与速度面板在本机存在；本次未逐行审计或计算一致性。
- 新增 `Documents/L2_notes/flow_validation_and_network_extensions_20261029.md`，记录验证样本、单位、时钟、crosswalk、独立验证规则及后续福利/随机成本/link互补性实验；同步live plan与文档索引。
- 保留用户记忆的天气state文章线索，题名及“xxxx”项待核实；未新增未经验证引用，未改变主模型或运行平台任务。

## 2026-10-29：完成联通与模型flow的首轮网格连接验证

- 只读既有gate5基线2642条Ξ与Speed完整389份正式道路压力文件（13288340行）；未重估模型、未运行平台SQL或新增下载。
- 新增 `data_work/src/validation/validate_unicom_model_flow.py`、`audit_unicom_flow_validation.py`、`write_unicom_flow_report.py`。输出目录为 `data_work/outputs/unicom_model_flow_validation_v1/`；保存源文件哈希、模型导出、RN crosswalk、OSM有向路径、每日比较、指标与矢量图。
- 主样本14317个RN pair，采用500米端点距离、10米OSM吸附及有向路径网格序列筛查；平衡工作日2198条模型link，覆盖96.9%的模型passage。观测口径仅保留目标模型pair全部RN跨界经过的59.3%，与模型覆盖率不同。
- 主流量排序相关0.539（按网格对bootstrap95%区间0.501至0.577）；964对双向连接的方向比相关0.214，高流量方向一致59.1%。模型高/低方向比中位数3.08，联通1.40。各匹配规则流量排序相关0.445至0.607，结论为部分空间一致、较弱方向吻合。
- 11项核验通过，包括原始CSV独立复算700条、独立秩相关与全部主匹配保存路径的有向弧及长度复查。图已目视检查。
- 保留结果报告 `Documents/L2_notes/unicom_model_flow_validation_results_20261029.md`，7份口径/复现文档在 `data_work/docs/unicom_flow_validation_20261029/`。同步研究设计、live plan和索引。报告明确2022通勤与2025机动车样本差异及共享Speed/OSM环境；本轮是网格边界压力代理验证，未声称完成逐物理道路flow验证。

## 2026-10-29：核对2025通勤输入与分时升级可行性

- 只读检查SubwayMobility与ChinaUnicomProjects共享项目03、Speed现有OD结构；未向来源项目写文件、未执行平台任务或变更模型。
- 确认NG103D六份2025分时通勤OD已本地下载，86346行/42441789次出行，非空组与build QC全部匹配，键唯一且单元人数≥5。NG74B2025有19127个5公里职住OD；共享NG74A含2025年1公里居住/工作边际。
- 直接核查NG103C SQL：07-09/10-16/17-19标签实际对应07至10/10至17/17至20时半开窗口；已导CSV无hour/date，不能本地截成07至09或目标周。另有5公里与原3公里网格差异、mode1并非验证后纯汽车、职住≥1公里及R样本筛选差异。Speed七列OD缓存没有purpose，不能从中直接筛通勤。
- 保留 `Documents/L2_notes/unicom_2025_commute_input_assessment_20261029.md` 与 `data_work/src/validation/inspect_unicom_2025_inputs.py`。来源哈希、schema、逐组QC与时窗表在 `data_work/outputs/unicom_2025_input_feasibility_v1/`；已更新索引和nextstep。建议独立建立2025版本，先对齐输入再加AM/PM、30至60分钟分时层，不直接改贴原结果。

## 2026-10-29：建立TidalLanes独立Platform提取工作区

- 按用户指示建立 `Platform/`，参考SubwayMobility现行NG的审计/内部构造/QC/聚合导出/批次日志组织，未复制其legacy流水线或写入来源项目。
- 保存202505_v01配置、来源合同和来源SQL/说明哈希、19项依赖队列、9条独立单语句只读探针、2份不可直接执行的参数模板、分阶段构造与导出合同、pilot容量规则。本地完成，平台提交/上传均为0，状态not_run。
- 生成公开参考：3357原始裁剪网格、1104节点、2642连接、7569合成空间fixture及12时间fixture；原网格为EPSG:3857投影3000米格，现用44格有边界裁剪。裸floor在合成点上1363处不同于裁剪多边形，要求实际点落面。
- `prepare_references.py`、`check_workspace.py`通过；`test_probe_validation.py`5项合成本地测试通过，覆盖09点边界、缺fixture、多重空间匹配和裁剪外点。测试不是平台成功记录。validator已准备，待真实探针下载后核对。
- 入口 `Platform/README.md`；本批唯一执行记录 `Platform/runs/202505_v01/run-log.md`。原样聚合下载、提交记录和生成参考排除Git，不向ChinaUnicomProjects自动同步。同步根AGENTS布局、文档索引和nextstep。

## 2026-11-01：2025-05平台通勤OD导出完成（202505_v01，0400）

- 平台内建成人员月表（R 8,988,684人，联合职住4,503,095人，与NG13一致）、19个工作日trip表（612,208,951个trip键，与NG20一致）、月度职住OD（N）、分时出发OD与逐日参与表。
- 速度有效性定两档：strict剔除>200 km/h；relaxed保留时长<60秒的退化记录（0519试跑显示超速记录中位时长7秒、93%不足60秒，属于时长退化而非真实高速）。两档都导出。
- 导出方案按0500A容量测试选定：逐日×30分钟只能保留63%高峰出行，故改为19天合并×30分钟（保留85%）、合并×整个高峰（91%）、逐日×整个高峰（81%），另加逐日参与和月度N及行列边际。所有导出行≥5个不同SIM，<5的子计数置空；合并计数在平台上重新去重，不跨日相加。
- 46份分片全部下载，`check_exports.py` 核对通过：六张表行数、strict行数、strict出行数和N总人数4,200,152均与平台建表时的总数一致。下载件在 `Platform/runs/202505_v01/query-results/`（不入Git），过程记录见 `Platform/runs/202505_v01/run-log.md`。
- 经验：平台结果可能被截断（0006只返回24组中的10组），每次下载都必须核对行数；DaaS登录约1–2小时过期，提交链会在过期时自动停止，续跑前先查同名任务防止重复提交。

## 2026-11-02：2025-05平台路段压力导出完成（202505_v01，0140–0460）

- Speed项目已有的路段通行表只覆盖经过日0518–0525，按用户选择只用0519–0523五个工作日，未自建其余14天。
- 0140：联合职住、mode1、HW早高峰/WH晚高峰出行按uid+date+move_id=moi_id只读关联Speed通行表。strict关联率每天每方向82.3%–83.0%；未关联的出行都在Speed清洗表内但没有有效RN记录。首个通行与出发时段相差≤1个10分钟段的占92%，无8小时偏差。共3.87亿次有效路段通过。
- 0150容量测试：5人门槛下，合并×30分钟保留98.8%通过次数，合并×整窗99.9%，逐日×整窗99.4%，合并×10分钟97.4%；relaxed只比strict多0.35%通过次数。
- 导出（用户选择）：strict的合并×30分钟（238万行）、合并×整窗（60万行）、逐日×整窗（206万行），加网格OD关联覆盖表（两版，14.9万行）作为压力分母。37份全部下载，`check_rn_exports.py` 核对行数与通过次数总和均一致。下载件在 `Platform/runs/202505_v01/query-results/`（不入Git）。
- 经验：用哈希分组切片时，Hive的hash取模不一定均匀（30分钟16组最大一组20万行，平均15万），切片前先看汇总里的最大组行数；登录在下载时过期的任务应直接按taskid下载，不重新提交（`daas_task.py chain` 已支持）。

## 2026-11-02：RN边负荷口径与覆盖（锁定，待复核）

- 路段对映射到gate5模型边：`Platform/utils/map_rn_to_model_edges.py`，输出在 `Platform/runs/202505_v01/local_model/`（不入Git）。
- 主口径为长度分摊（每段位移按东西、南北分量计入中点所在半格对应的模型边，单位为“格”），稳健性口径为跨格次数。理由：前者覆盖全部2,642条边，早晚潮汐反转更清楚（−0.82对−0.61）；与拥堵指标的相关两者相当（0.34对0.33）。**用户要求之后再仔细核查此选择。**
- 缺边负荷只报告不重分配：早高峰长度分摊负荷65%落在模型边上，约23%落在两个相邻模型节点之间但模型缺边（其中约10%只缺这一方向），约12%在模型节点外。
- 其余14个工作日RN暂不自建；平台上除本项目已删的19张表外不动其他表。

## 2026-11-02：观测路段流量接入gate5模型（诊断）

- 新脚本 `data_work/src/model/full_estimation/run_observed_traffic_gate5.py`，输出 `data_work/outputs/observed_traffic_rn202505_gate5_v1/`。读入 `Platform/runs/202505_v01/local_model/` 的边负荷作为观测Ξ_kl（早高峰HW、strict、0519–0523合并后除以5），当场重算基线均衡得到模型分配的Ξ̂（不沿用6月lane修复前的数组）。缺边负荷只报告覆盖率。
- λ截面诊断（与 `lambda_diagnostic` 同一车道分组去均值OLS）：观测Ξ得0.026（se 0.003，长度分摊；跨格次数0.026），模型Ξ̂得0.013，现参数占位值0.071（AA Seattle δ1/θ）。R²约3%，仍是单期截面、无IV，只作诊断，未改 `parameters.lambda_`。
- 模型Ξ̂与观测Ξ的边级对数相关0.44；双向边对的方向不对称相关0.40。两种口径结果几乎相同。
- 观测Ξ只含样本内联合职住的小汽车通勤者，不是全部车流；绝对量不可直接比，结论只用对数斜率和相关。

## 2026-11-02：λ识别尝试（观测RN流量，gate5）

- 脚本 `data_work/src/model/full_estimation/run_lambda_identification_gate5.py`，输出 `data_work/outputs/lambda_identification_rn202505_gate5_v1/`（表 `lambda_identification_table.csv`）。方程 ln(τ/τ_ff) = λ ln(Ξ/n)。Ξ：早高峰用HW、晚高峰用WH的RN边负荷（strict，0519–0523，日均）。晚高峰时间取同一边集的 `raw_connected_vs_old_adjacent_adjacent_square_PM.csv`。
- 工具变量：把2025平台OD（整窗、strict、mode1，早HW/晚WH）按模型路径核在自由流时间下分配到路网，Ξ^IV_kl = A_kl·Σ_ij L_ij X_ik X_lj / X_ij。旧模型OD作对照工具。
- 结果（长度分摊口径；跨格次数口径低10%–15%，方向一致）：
  - D1 早高峰截面（车道分组去均值）：OLS 0.026，IV 0.030（F≈2,600）。
  - D2 同一走廊两方向相减：OLS 0.144，IV 0.219（F≈84）。
  - D3 同一有向边早减晚：OLS 0.110，IV 0.138（F≈101）。
  - D4 走廊×时段双重差分：OLS 0.131，IV 0.159（F≈231）。
- 解读：截面估计被道路本身差异压低（流量大的多是快速路）；去掉走廊或边的固定差异后λ约0.11–0.22，约为占位值0.071的1.5–3倍。IV高于OLS，与样本车流作为总车流的测量误差相符。仍是诊断，未改参数。
- 主要待查：样本只含通勤小汽车，早晚非通勤车流占比不同会影响D3；已有潮汐车道使高峰方向实际车道数变化，模型车道数固定，可能低估λ；标准误为HC1，未处理空间相关；速度数据年份与2025 RN是否一致[待确认]。

## 2026-11-02：模型换用2025平台OD；λ识别补充

- 新模型输入 `data_work/outputs/model_input_edgeproj_v2_lcc_gate5_od2025_v1/`（脚本 `data_work/src/model/full_estimation/build_od2025_gate5_bundle.py`）：节点、边不变，OD换成平台202505_v01导出。`commuters_road` = 早高峰HW小汽车（mode1，strict）出行19个工作日日均，另存 `car_am_people`、`hw_people_month`（月度职住全方式）。91%的出行落在模型节点内；50个节点居住为0，56个节点就业为0。原bundle只读，未改。之后所有新结果都写新目录，脚本遇已存在目录即停止。
- 换OD后，模型分配Ξ̂与观测RN流量的边级对数相关由0.44升至0.55（`observed_traffic_rn202505_gate5_od2025_v1`）；λ截面诊断（模型Ξ̂）0.013→0.014。
- 速度数据核对：`raw_data/speed_Beijing_all_wgs84.csv` 的日期为2025-05-19至05-25，与RN五天同一周。
- λ识别v2（`lambda_identification_rn202505_gate5_v2`）：加分样本工具变量（同五天OD、其余14天OD、19天合并、旧OD）和空间聚类标准误（9公里、18公里网格块）。长度分摊口径，IV结果：D3早减晚0.136–0.149（2025 OD三种）、0.194（旧OD）；D4双重差分0.153–0.159、0.193；18公里聚类se约0.022–0.030。用不同日期的OD作工具变量结果不变，说明工具变量与RN流量共用同一批出行样本不是结果来源。跨格次数口径低约10%。

## 2026-11-02：OD依赖步骤核查；λ比较基准更正

- 网络不随OD重建：gate5节点集是绕行门限（G=5）后边图的最大连通分量（`build_locked_bundle._restrict_lcc` 只用边），边的通行时间、车道数来自路网与速度数据，均不用OD。旧流程里OD只影响stage07节点表的并集和"reachable"筛选，进入LCC后不起作用。
- 依赖OD、需随新OD重做的步骤：节点居住/就业列（已做，见下）；基本面反演（ū、ā、χ）与基准Ξ；anchored拥堵式中的Ξ_obs；潮汐车道选边 `score_bpr_st`（用基准Ξ）；所有反事实与对称情景；`Replication_apply` 的AA输入与Spec C；按居住/就业加权的作图；其他从旧bundle复制OD的变体（奇偶日、自由流、对称输入、car_ratio）。
- 新bundle `model_input_edgeproj_v2_lcc_gate5_od2025_v2`：在v1基础上把节点 `residents`/`jobs` 换成2025 OD边际，旧值改名 `residents_2022`/`jobs_2022`。QA全部通过（1,104节点、2,642边、单一连通分量、绕行≤5）。v1保留不动。
- 更正：此前把λ估计与0.071比较不当。0.071是AA(2022) Seattle的δ1/θ，只用于外生τ诊断；主线用 `lambda_phys=1.1`（Barwick et al. 2024北京速度–密度弹性，作者含Shanjun Li）。本项目回归是log(τ_obs/τ_ff)（分钟）对log(Ξ/n)（流量/车道），填的是模型拥堵式τ=τ̄(Ξ/n)^λ里同一个指数（主线现放1.1）；冰山成本单位下为λ/θ≈0.021。但1.1是速度对密度的弹性，与模型以流量写的拥堵式不是同一对象；同口径的外部基准是AA Seattle的δ1=0.488（行驶时间对每车道流量）。λ≈0.14–0.19约为δ1的1/3。差距与口径（通勤样本vs全部车流、3公里格边）待查，暂不替换参数。

## 2026-11-03：全目的车流、通勤占比与速度–车流面板（gate5，诊断）

- 输出 `data_work/outputs/all_traffic_speed_panel_rn202505_gate5_v1/`（speed/、rn_all/、analysis/）；脚本 `build_speed_edge_10min_gate5.py`、`build_rn_all_edge_10min_gate5.py`、`run_all_traffic_speed_panel_gate5.py`（均在 `data_work/src/model/full_estimation/`，遇已存在目录即停）。
- 输入只读：Speed的RN压力三计数389份（0519–0525，全部clean mode1、全部目的、10分钟，Speed未设≥5门槛；本项目只写边级合计，边×10分钟<5的行不写出）；速度原始文件（实为10分钟粒度，`hour`列为yyyymmddHHMM）。速度路段按与RN相同的长度分摊规则归到模型边，得到走廊单位里程用时指数（min/km），2,581条边、覆盖率中位数100%。全目的长度分摊车流62.5%落在模型边上。
- 通勤占比（strict HW/全部mode1，工作日）：AM全市28%、PM 23%；边中位数22%/21%（p10–p90 12%–33%）；30分钟峰值08:00为31%。是下限（strict定义+小单元抑制）。
- 时钟：全市车流与用时峰值都在08:10、晚峰18:10/17:50，最佳滞后10分钟，RN时刻与速度时钟一致。
- 小时面板（06–22时，边FE+日×时FE，边聚类）：0.075（se 0.004）；留一日IV 0.061；含周末的逐日变化0.098；仅工作日逐日0.025；工作日高峰小时0.19（se 0.011）。分段：畅通（用时<1.2×边最低）0.02，拥堵（≥1.3×）0.19（se 0.08）。分位图单调上升且凸，未见流量随用时上升而下降的回弯；但56%的边在最慢小时的流量低于其最大流量的90%（中位0.86），超饱和迹象有限、需按边细看。
- D3（工作日早晚峰，用时指数）：全目的车流0.245（se 0.045，18公里聚类），通勤车流0.122（se 0.022，与此前路网时间D3 OLS 0.11一致），全目的车流以通勤车流作IV 0.242。
- 解读：弹性随拥堵状态上升，高峰约0.19–0.25（对全部车流）；对strict通勤车流的简约式约0.12。模型λ取哪一个取决于Ξ是否包含背景车流，待与合作者定。均为诊断，未改参数。

## 2026-11-03：分时OD工具变量与逐边超饱和检验（gate5，诊断）

- 脚本 `data_work/src/model/full_estimation/run_od_iv_hypercongestion_gate5.py`（需duckdb；DuckDB溢写目录设在本地临时目录，不写Speed目录），输出 `all_traffic_speed_panel_rn202505_gate5_v1/od_iv/`。
- 工具变量：Speed周OD缓存（geohash6×出发10分钟，全部mode1、全部目的，0519–0525，只读）按geohash中心归到3公里格，o≠d且两端在gate5节点内的出行（6,720万，占全部mode1的49%）按自由流最短路（tau_ff_min）全有全无分配，并按到达各边的自由流时间平移时段。预测车流只取决于谁何时去哪，不受该边实际拥堵和路径选择影响。
- 小时面板（06–22时，2,570条边，边聚类），OLS → IV：
  - 边FE+日×时FE：0.076 → 0.160（se 0.009，F 1,641）；
  - 边×时FE+日×时FE（逐日变化，含周末）：0.100 → 0.232（se 0.010，F 1,185）；
  - 同上仅工作日：0.026 → 0.133（se 0.012，F 462）；
  - 工作日高峰小时：0.193 → 0.328（se 0.024，F 907）。
  IV约为OLS的2倍（仅工作日约5倍），与车流测量误差及超饱和段的负向关系压低OLS一致。
- 逐边超饱和（工作日五天合并10分钟、30分钟平滑；规则：高峰窗内用时≥最大流量时用时×1.1、流量≤最大流量×0.9、用时≥该边最低×1.3，持续≥30分钟）：2,572条边中333条（13%）标记，其中223条在五天中≥3天单独也满足；标记边回弯段最低流量为最大流量的中位69%。标记边分布、车道数与全部边相近。
- 去掉标记边后IV：0.142 / 0.204 / 0.309（三种设定），只用标记边：0.297 / 0.432 / 0.479。超饱和边拉高了IV但不改变量级。
- 解读：对全部车流，用需求工具变量得到的弹性约0.14–0.23（全时段）、约0.31–0.33（高峰）。仍是诊断；用时为走廊指数而非路径时间；出发时刻本身会对预期拥堵作出反应，日×时FE只吸收全市共同部分。未改参数。

## 2026-11-03：2025平台OD与2022通勤OD对照（gate5）

- 脚本 `data_work/src/validation/compare_od2025_vs_od2022_gate5.py`，输出 `data_work/outputs/od2025_vs_od2022_gate5_v1/`；两个bundle只读。2022 = `commute_202211.csv`（月度职住，全方式人数pop，方式识别列type_car）；2025 = 平台202505_v01 strict（月度联合职住SIM、早高峰HW mode1人数与日均出行）。2025单元<5被抑制，边际为下限。
- gate5内总量：2022 pop 703万、car 8.8万（方式识别只覆盖约5%人口）；2025月度职住SIM 383万、早高峰HW mode1 181万人、日均62万次。
- 空间分布高度一致：居住/就业边际对数相关0.96/0.97，前50就业格重合49个，份额差异（半绝对差）居住9.6%、就业7.7%。跨格OD对数相关0.86（全方式），小汽车0.64。2025的跨格职住量99.8%落在2022也有的格对上。
- 主要差异：2025同格职住占36%（2022为10%；2025不设职住≥1公里），跨格平均距离12.1 km（2022为14.4 km）；2025早高峰小汽车通勤中位距离6.7 km（2022小汽车9.0 km）。各居住格的小汽车占比两年几乎不相关（0.11），但2022方式识别样本很小、口径不同。2022年11月北京处于疫情管控期，通勤结构可能受影响[待核实]。

## 2026-11-03：用2025 OD重跑OD依赖步骤（gate5，参数不变）

- 输入 `model_input_edgeproj_v2_lcc_gate5_od2025_v2`；参数全部沿用主线（anchored、`lambda_phys=1.1`、α=0.05、β=−0.10、θ=6.83、`score_bpr_st`），只换OD，以便单独看OD的影响。λ口径定下后再换。所有输出写新目录（后缀 `_od2025_v2`），旧目录未改。
- 变体bundle：用 `build_od2025_gate5_bundle.py --source-version` 从原变体建 `model_input_edgeproj_v2_lcc_gate5_{am_odd_days,am_even_days,ff_*}_od2025_v1`（9个；边与原变体逐字相同，节点与OD与od2025_v2逐字相同）。对称输入bundle由对称套件从od2025_v2生成（`…_symmetric_{geometric,arithmetic}_od2025_v2`）。`car_ratio` 不建：它是把2022全方式OD按小汽车比例缩放，2025 OD本身就是小汽车出行。
- 基本面反演与基准：反演821次迭代收敛；ū、ā与旧OD的对数相关0.70/0.85。内生基准65次外层迭代收敛，τ/τ_obs中位1.0001。基准Ξ与旧版对数相关0.87；Ξ_obs中位26.7（旧5.0，按样本量放大，anchored式只用Ξ/Ξ_obs）。
- 潮汐选边 `score_bpr_st` 与旧版重合：top25 23/25、top50 40/50、top100 80/100。
- 主线潮汐top100（`tidal_lane_diagnostics_gate5_bpr_pos05_physical_time_od2025_v2`）：W̄ ST +1.11%（旧+0.87%）、LT +0.43%（旧+0.32%）；基准平均物理通勤时间14.1分钟（旧11.3）；每人ST省56秒（旧42），LT多13秒（旧14）。方向与"ST省时、LT时间回升、福利仍正"一致。人·小时总量按样本通勤人数算，新OD样本为61.9万、旧为8.8万，总量不可跨OD比较，只比每人秒数和百分比。
- 政策比较（`policy_comparison_gate5_bpr_pos05_od2025_v2`）LT W̄：P3 qdelay +0.55% > P5 +0.52% > P2同走廊 +0.48% > P1潮汐 +0.43% > P4 +0.13%；P6/P7仍为负。排序与旧版相同。Stage 2全部CF收敛。
- 对称输入重估：几何W̄ +0.03%、算术 −0.08%（旧+0.03%/−0.07%），OD加权双边成本 −1.5%/−0.9%（旧−1.1%/−0.7%）。对称–方向性套件结论不变。
- 选边分日检验：winner's curse 3.3%（旧3.2%），奇偶日top100重合78。
- 自由流稳健性quick top25：七个替代口径ST +0.506%至+0.508%、LT +0.188%至+0.189%，top25全部25/25（旧23–25/25）。
- Phase A：`run_phase_a_st_metrics_gate5.py` 新增 `--calib-dir`（默认不变），本次传空，跳过λ=0.57行（旧行读的是旧OD目录）。
- AA/Spec C：AA输入构建脚本与Spec C gate5包装脚本新增 `--source-version`/`--suffix`（构建脚本另有 `--sample-definition`），默认值不变；新后缀遇已存在目录即停。发现旧 `aa_input_square_edgeproj_v2_lcc_gate5/`（6月28日）早于6月29日车道修复，车道仍是修复前的。因此另建 `_lanefix2022`（2022 OD+修复后车道）作对照。Spec C (χ̂−1)×10⁶：旧−337，lanefix2022 −334，od2025 −5,106；选中100对三版完全相同。od2025求解残差err_eqm1=1.3e-2（lanefix2022为3.9e-3）。幅度放大约15倍的来源未查，暂不解读。
- 经验：多个求解进程并行时需限制BLAS线程（`VECLIB_MAXIMUM_THREADS`），否则互相抢占，看似卡住；反演早期的overflow警告是瞬时的，结果有限且收敛。
- 被中途停止的首轮半成品目录（`_od2025_v1` 后缀的stage2、tidal、welfare、symmetric_geometric bundle与重估输出）未删除，不可使用。

## 2026-11-03：平台数据后续问题与口径要求已记录

- 用户要求记录网络覆盖、拥堵函数和通勤占比的后续讨论，详见 `Documents/L2_notes/platform_network_congestion_questions_20261103.md`。λ 后续推进到 segment 层面；通勤占比先统一 R 人群，再按 purpose-level trip 计算。
- 保留两条待比较路线：联通/RN网络如何补速度，百度网络如何处理缺link。函数图需区分 q/q95 与 q/c，参考 Brad 文章前先核对版本。此前探索性分档尚无正式可复现输出，未据此锁定结构函数或新λ。
- 本次仅完成问题记录与计划入口，未改数据、模型网络、参数或运行结果。

## 2026-11-03：拥堵函数解释图

- `data_work/outputs/congestion_technology_explanation_20261103/` 保存常弹性与BPR的时间、局部弹性、速度示意图（PNG/SVG/PDF）及复现脚本，已检查图面。参数仅作示意，未拟合北京数据；常弹性图的低流量时间低于自由流基准，展示纯幂函数缺少自由流下限的问题。
- 本次回答区分segment识别与网格模型聚合，网络覆盖诊断尚不构成重建决定；R人群purpose-level trip通勤占比仍待重算。

## 2026-11-04：R口径分目的出行占比（平台0112）

- 平台只读查询0112（见run-log），本地汇总 `data_work/src/validation/summarize_purpose_share_r.py`，输出 `data_work/outputs/purpose_share_r_202505_v1/`。口径：strict有效出行、19个工作日、按出发时间；purpose由出行两端停留点类型定义；R=19–59岁、核心用户、有家停留点（joint另有工作停留点）。
- 早高峰07–09出发的mode1出行：R内HW占35.6%，家–其他40.8%，工作–其他11.2%，其他11.9%；joint内HW占46.8%。晚高峰17–19：R内WH占25.4%，joint内35.0%。R占全部用户出行的64%；全部用户的早高峰HW占28.8%，与此前路段经过口径的28%一致。
- 带中途停留的通勤（家→停留→单位）被拆成家–其他和工作–其他，两段不可在此表中区分，因此35.6%是R内通勤出行占比的下限。

## 2026-11-04：拥堵函数形状图

- 脚本 `data_work/src/model/full_estimation/plot_congestion_technology_gate5.py`，输出 `all_traffic_speed_panel_rn202505_gate5_v1/technology/`。图1为示意（常弹性λ=0.2、1.1与BPR a=0.15、b=4，参数为假设值）；图2为gate5边数据：按q/q95（该边小时车流第95百分位，不是容量）分档的用时指数与局部弹性，边聚类bootstrap 200次。
- 局部弹性：q/q95<0.4约0，0.5–0.75为0.05–0.12，0.85–0.95约0.34，0.95–1.05为0.21，>1.1转负（−0.3）。用时水平在q/q95≈1.03处最高，再往上用时下降：最高车流的小时不是最慢的小时，与超饱和段（排队时通过量下降）一致。OLS、无IV。

## 2026-11-04：路段（segment）层面的匹配与λ估计

- 构建 `data_work/src/model/full_estimation/build_segment_speed_flow_panel.py`，输出 `data_work/outputs/segment_speed_flow_rn202505_v1/`。联通RN路段对（弦线≥20 m）按中点距离≤20 m、方向与百度路段局部切线相差≤30°匹配到百度速度路段（raw_segment_master，未拆分几何）；中位距离3 m、中位方向差<1°。路段车流=匹配路段对通过次数的弦长加权平均，按路段×小时写出，<5的行不写。
- 覆盖：百度路段长度的92.5%有匹配；联通RN车公里只有57%落在可匹配百度路段的路段对上（其余主要在百度未覆盖的低等级道路）。百度类别2为环路与快速路，3为主干道与省道，4为县道与次干道。
- 估计 `run_segment_lambda.py`（输出 `.../lambda/`），18,378个路段，206万路段×小时，路段聚类：
  - 路段FE+日×时FE：OLS 0.069，留一日IV 0.071，分时OD IV（所属gate5边的预测车流）0.176；
  - 路段×时FE+日×时FE：OLS 0.066，分时OD IV 0.244；
  - 工作日高峰小时：OLS 0.194，留一日IV 0.245，分时OD IV 0.344。
  - 分道路类别：主干道与次干道和总体相近（高峰IV约0.30）；环路与快速路OLS约0，分时OD IV 1.09（高峰1.56，F 143–243），网格层面的工具变量与快速路车流对应较差，此结果暂不采用。
- q/q95分档的局部弹性与网格边层面几乎相同：<0.5约0.02–0.04，0.75时0.19，0.85时0.41，过q95后转负。
- 结论：λ的量级不因从网格边换到路段而改变，网格边估计不是低估的主要来源。

## 2026-11-04 RN→Baidu segment matching diagnostics and two model-network versions

- Script: `data_work/src/diagnostics/plot_rn_baidu_match_network.py`; output `data_work/outputs/rn_baidu_match_network_v1/` (figures copied to `Documents/L3_figs/platform_20261104/fig4–fig7`).
- Prior work: Speed project has a frozen Baidu roadseg → OSM crosswalk (`V1-OSM/data/raw/matching/beijing/mapping_all.csv`) but never did RN → OSM → Speed alignment; the direct geometric RN → Baidu match in `build_segment_speed_flow_panel.py` is new.
- Match quality (weekday vehicle-km): matched 57% (inside gate5 area 60%; chords ≤ 300 m 70%), Baidu nearby but direction off 3%, no Baidu within 20 m 39%, chord < 20 m 1%. Matched distance median 3.5 m (p90 12 m), heading difference median 0.7° (p90 5.5°). Road-name agreement with nearest OSM edge: 78% of named matched vehicle-km.
- Unmatched flow: OSM class tertiary 24%, expressway 24% (mostly outer expressways 六环/京承/京藏 far from any Baidu line, plus parallel carriageways/auxiliary roads 20–50 m away), primary/secondary 16%, residential/service 7%, no OSM road within 20 m 29% (long chords, vkm-weighted median about 1 km, cutting across curves: a chord-geometry limit). Unmatched chords are longer than matched ones (vkm-weighted median 587 m vs 378 m).
- Grid level, weekday RN length-flow: 63% on gate5 edges, 13% between adjacent gate5 nodes with no edge, 9% reverse direction only, 14% outside gate5. With threshold = 5th percentile of gate5 edge flow: 717 directed candidate new edges (15% of flow between gate5 nodes), 133 gate5 edges below threshold. Median candidate new edge has only 29% of its RN flow on Baidu-matched pairs, so its travel time would mostly need imputation.
- Cross-check against the no-gate bundle `model_input_edgeproj_v2_lcc_v1`: 244 of the 717 candidate new edges (58% of their RN flow) exist there and were dropped by the detour gate (Baidu-routed length > 5 × centroid distance). RN shows direct roads on these pairs; the detour comes from missing Baidu links. The other 473 have no Baidu route at all.

## 2026-11-04 Segment-level λ robustness: stale Baidu pace, 5/22, load regimes

- Script: `data_work/src/model/full_estimation/run_segment_lambda_robustness.py`; output `data_work/outputs/segment_speed_flow_rn202505_v1/lambda_robustness_v1/`. Prompted by coauthor's Trips/Speed variation report (constant-speed segments, low-count noise).
- Stale pace: 19.7% of 06–22 h 10-min slots sit in exact-constant runs ≥ 60 min; 96.7% of them equal the segment's modal value (vs 2.7% of other slots), so they are fill values, not real stable free flow. Concentrated by segment (20.5% of segments >50% stale; expressway class ≈0.4%, classes 3/4 ≈20–22%); flat across hours and across q/q95 bins (15–23%), so not a low-load phenomenon.
- Dropping stale slots raises λ by about 10–17%: segment FE IV 0.176→0.206; segment×hour FE IV 0.244→0.276; weekday peak IV 0.344→0.395. Dropping stale segments gives the same. Dropping 5/22 lowers estimates by 0.01 or less.
- Elasticity by typical load of the segment-hour (weekday mean / own q95; S2 day-to-day, routed-OD IV, stale dropped): <0.5 0.09; 0.5–0.7 0.16; 0.7–0.9 0.29; ≥0.9 0.36 (se 0.07; 0.58 when also dropping 5/22). OLS is negative in the two high-load regimes, consistent with throughput falling when congested. Supports a convex, load-dependent congestion function; even the highest regime stays well below λ_phys = 1.1.

## 2026-11-04 Congestion-technology forms drawn and fitted to the gate5 edge curve

- Script: `data_work/src/model/full_estimation/plot_congestion_forms_compare.py`; output `data_work/outputs/congestion_forms_compare_v1/` (fig8 theory, fig9 fit, `fit_params.json`); figures copied to `Documents/L3_figs/platform_20261104/`.
- Forms: constant elasticity; free-flow floor + constant (Almagro et al.); BPR; linear speed-density on the uncongested branch (Kashner-Ross, t/t0 = 2/(1+sqrt(1-q/c))).
- WLS fit to the 10 binned within-edge index means with q/q95 <= 1.05 (weights = edge-hours). Weighted RMSE: BPR 0.0040 (b = 4.25, c ≈ 0.99 q95 at a = 0.15); floor+constant 0.0071 (β = 0.37, kink 0.70 q95); linear speed-density 0.0168 (c = 1.71 q95); constant 0.0287 (λ = 0.09).
- Lesson: a fit to bin means, not an estimate; formal comparison needs edge-hour data with IV.

## 2026-11-04 Match maps and parallel-road ambiguity check

- Scripts: `data_work/src/diagnostics/plot_rn_baidu_match_layers.py` (3 km Guomao window, Unicom / Baidu / status; 85.74% of window vehicle-km matched) and `plot_rn_baidu_match_citywide.py` (fig11a links by status, fig11b matched share by 3 km cell; ambiguity check). Outputs `data_work/outputs/rn_baidu_match_layers_v1/`, `rn_baidu_match_citywide_v1/`; figures in `Documents/L3_figs/platform_20261104/`.
- Model area: 58.01% of weekday vehicle-km matched; cell matched share p10/p50/p90 = 0 / 37.9% / 78.6% (1,679 cells with >= 2,000 sample vehicle-km).
- Ambiguity (matched vehicle-km, same rule re-run reproduces 100% of matches): one valid segment 81.11%; second valid segment 18.89%; second within 5 m of winner 4.20% (different name 2.50%, different Baidu class 2.12%); Baidu class 2 but nearest OSM road minor 0.35%.
- Slides `Documents/PreSlides/platform_20261104/` appendix updated with these pages.
- 2026-11-04 slide redraw of the gate5 shape curve: `data_work/src/model/full_estimation/plot_congestion_shape_slide.py` → `outputs/congestion_shape_slide_v1/fig12_congestion_shape_slide.png` (no new estimation; local elasticity drawn at midpoints of adjacent bins; edge peak 0.35, not 0.4).

## 2026-11-04：最严格匹配口径下重估λ（网格边与路段）

- 结论：最严格匹配下，IV λ在两个层面都只小幅下降（0.00–0.02），两层之间的IV差距缩小（S1 0.016→0.007，S2 0.012→−0.002，S3 0.016→0.012）。匹配误差不是λ偏低的原因。
- 脚本 `data_work/src/model/full_estimation/run_lambda_strict_match.py`，输出 `data_work/outputs/lambda_strict_match_v1/`（`strict_filter_steps.csv`、`lambda_strict_vs_baseline.csv`、`lambda_by_variant.csv`、`summary.json`）。06–22点，0519–0525。
- 第0步：新代码重建的路段小时车流、边10分钟车流与原面板一致（最大差1e-12），12个基准数完全复现（差0.0000）。
- 依次剔除（占原匹配工作日车公里；括号内为该规则单独作用）：平行路<5 m 4.20%（4.20%）；端点离路段>20 m 25.24%（27.95%）；同向平行重叠取远侧 1.22%（4.43%，2,137条路段）；嵌套重叠取长弦 1.69%（7.08%）；快速路误配 0.14%（0.35%）。最终剩44,366条弦、原匹配车公里的67.5%、16,882条路段；估计样本13,859条路段、2,479条边。
- 严格版 vs 基准：
  - 边：OLS 0.058 / 0.070 / 0.164（基准0.076 / 0.100 / 0.193），IV 0.156 / 0.227 / 0.317（0.160 / 0.232 / 0.328），F 1,467 / 1,041 / 820。
  - 路段：OLS 0.077 / 0.081 / 0.193（0.069 / 0.066 / 0.194），IV 0.163 / 0.225 / 0.329（0.176 / 0.244 / 0.344）。
- 路段IV的标准误按所属gate5边聚类后约为按路段聚类的两倍：S1 0.0054→0.0102，S2 0.0068→0.0121，S3 0.0145→0.0253；第一阶段F 1,710 / 1,435 / 906（按路段聚类时为7,848 / 5,923 / 3,701）。原JSON字段 `se_cluster_edge` 在路段层面实为按路段聚类，新输出分开命名。
- 影响最大的一步：边层面是让q和t来自同一批道路（原匹配、未剔除时OLS已从0.076降到0.058，IV只降0.004–0.012），之后五条剔除几乎不再改变边层面的结果；路段层面是端点>20 m（S1 IV 0.172→0.163，OLS 0.069→0.076）。
- 附加（严格版+剔除≥60分钟恒值时段）：路段IV 0.187 / 0.251 / 0.372，边IV 0.158 / 0.223 / 0.308。恒值填充对路段λ的影响大于匹配口径。
- 2026-11-04 slide figure of the four candidate forms (one column each, level and local elasticity): `data_work/src/model/full_estimation/plot_congestion_forms_slide.py` → `outputs/congestion_forms_slide_v1/fig13_congestion_forms_slide.png`; replaces fig8 in the slide appendix.
- 2026-11-04 slide redraw of the model-network comparison (fig6 c,d): `data_work/src/diagnostics/plot_network_versions_slide.py` → `outputs/network_versions_slide_v1/fig14_network_versions_slide.png` (no new computation).

## 2026-11-05 拥堵形状曲线超过q95后下降的来源检验

- 脚本 `data_work/src/model/full_estimation/run_congestion_falling_tail_check.py`，输出 `data_work/outputs/congestion_falling_tail_v1/`（`summary.json`、各层 A/B/C 表、`fig15_falling_tail_checks.png`，图已复制到 `Documents/L3_figs/platform_20261104/`）。样本同形状曲线：工作日0519–0523，06–22点，小时；边204,127边小时/2,572边，路段1,468,954路段小时/18,378段。
- 下降只发生在q/q95>1.1的两档，共2,382边小时，占1.17%。
- A 构成：这两档仍多为高峰小时（83–91%），但比峰值档更快：相对速度（v/边自身90分位速度）0.846→0.885→0.917，慢速小时（<0.6）占比12.3%→7.7%→4.2%。
- C 速度–车流：按速度分档，平均q/q95随速度下降而上升，0.52（≥v90）→0.72（0.8–0.9）→0.82–0.86（<0.7后持平）；最慢一档未见车流回落。最慢小时（<0.6，占边小时3.05%）的q/q95中位0.874，只有18.1%超过1。路段层面相同（平台约0.83，慢速小时4.88%，中位0.849，13.2%超过1）。
- 结论：q95附近车流到顶，同一车流下速度差别很大；堵的小时车流约为q95的85%，落在峰值左侧，q最高的小时是车流到顶但仍在流动的小时。小时数据里看不到平均意义上的通过量下降，更像“到顶后持平”。
- D 按超饱和标记分边：333条标记边超过q95后用时继续上升（1.274→1.323→1.426），下降全部来自2,239条未标记边（1.152→1.105→1.062）。
- B 按需求分档：同小时其他日平均车流分档在1.1以上几乎无样本（131和1小时），不能检验尾部；按routed-OD工具变量z分档，边层面同样在1.1以上下降（1.148→1.128→1.090），路段层面升到1.1–1.2档（1.212）后最后一档降（1.159）。下降不全是联通计数噪声；z尾部为何也降未解决。
- 未做：10分钟分辨率的速度–车流图；密度 k=q/v 的车流–密度图（有除法偏误，需谨慎）。

## 2026-11-05 尾部下降后续检验（nextstep A1）

- 脚本 `data_work/src/model/full_estimation/run_congestion_tail_followup.py`，输出 `data_work/outputs/congestion_tail_followup_v1/`（`summary.json`、1a–4 表、`fig16_tail_followup.png`，图已复制到 `Documents/L3_figs/platform_20261104/`）。
- 归一化：用q90、q95、q99，峰值档分别在1.1–1.2、1.0–1.1、1.0–1.1，峰值之后的小时占1.77%、1.17%、0.10%。下降不是q95造成的，始终发生在每条边车流最高的1–2%小时；应表述为"车流最高的约1%小时"，不是"超过q95"。
- 堵的小时与畅通小时对比：398条边有≥5个慢速小时（v/v90<0.6），慢速小时车流中位数 / 畅通小时（v/v90≥0.9）车流95分位：中位1.125（p10 0.866，p90 1.465），只有24.9%的边小于1。堵的小时车流不低于畅通时的高位，不支持"堵后通过量下降（capacity drop）"。
- 10分钟速度–车流：与小时数据相同，速度<0.6后车流平均稳定在q95的0.81–0.83倍；慢速时段占3.15%，车流中位0.843。
- routed-OD z尾部：第一阶段随z单调上升到1.1–1.2档（平均q/q95 0.922），最后一档略降（0.900）。z最高档的高峰小时占比从0.785降到0.636，相对速度回升到0.916：z尾部下降来自z最高档混入了非高峰小时。分小时：t在8点、18点最高，q在17点、7–8点，z在8点、17–18点，时间对得上。
- 0–24点：边V1 OLS/IV 0.076/0.160→0.022/0.085，V2 0.100/0.232→0.053/0.184；路段S1 0.069/0.176→0.020/0.094，S2 0.066/0.244→0.040/0.192（标准误按gate5边聚类）；高峰规格不变。加入夜间后全日规格的λ约减半，因为样本里平的低负荷段变多，常数弹性更不适用。0–24点形状曲线的尾部几乎不降（1.237→1.241→1.226），原因未查（可能是按边去均值后各档的边构成变了）。
- 第一次运行时右上图标题按假设写成"jammed hours carry less flow"，与结果相反，已改为中性标题并重跑；数字与第一次完全相同。

## 2026-11-05 与 Almagro et al.（NBER w32185，2026年7月修订版）的数据构建对照

- 文件 `Literatures/Literatures_speed_flow/Almagro_etal_2026nber_OptimalUrbanTransportationPolicy.pdf`。他们：手机GPS出行（放大到CMAP调查总量）减去公交卡、出租车、网约车行政数据得到私家车出行；77个社区之间的有向边；OD出行按Google Maps固定路径分配成边车流F；通行时间为2022年查询的Google Maps驾车时间；按一周中的小时平均；拥堵式 t = max{T0, A F^β}，T0取0–5点均值，IV为全市每小时出行数；私家车β 0.16–0.20（主规格IV 0.202）。
- 公交卡数据：现金支付约12%不可见，按日总客流统一放大；公交下车站为推断；Metra不在内（<1%）。私家车出行是差值，公交与出租车数据的误差会直接进入私家车出行。
- 他们的车流与通行时间都在社区对之间的边上，不是真实路段。
- 我们的两个优势（保留，用于写作）：(1) 车流是观测的（联通路网经过记录），不依赖路径假设；(2) 通行时间与车流在同一周并保留逐日变化，可在"边×小时"固定效应下用日间变化识别（S2）。
- 用我们的数据按他们的做法（ln t 对分配车流 ln z 直接回归）就是现有的简化式：边V1 0.044、V4高峰 0.084（`od_iv_hypercongestion_summary.json`），远低于IV 0.160/0.328；第一阶段0.27说明同一条边内z的变化约为观测车流的4倍，分配车流有较大误差，直接用作回归元会使弹性偏低。

## 2026-11-05 文献对照规格：Almagro式下限+常数弹性与分配车流作回归元（nextstep A2 (a)(b)、A3 (a)）

- 脚本 `data_work/src/model/full_estimation/run_congestion_almagro_spec.py`，输出 `data_work/outputs/congestion_almagro_spec_v1/summary.json`（50次按聚类bootstrap，只对0–24点）。`congestion_almagro_spec_test_noboot/` 是不带bootstrap的测试运行，点估计完全相同，可删。
- 样本：0519–0525一周，小时；只有一周，"一周中的小时"即日期×小时，不再平均。边2,532条（0–24点389,994边小时）；路段15,653段（2,584,441段小时，按2,312条gate5边聚类）。T0为每个单位0–5点平均用时（≥3个完整小时），固定不估。全市负荷 = 该日期×小时所有gate5边的routed-OD z之和，减去本单位自己的z。
- 估计式 ln t = max{ln T0, μ_e + β ln F}：μ_e逐单位二分求解，β由一个矩条件求根；每个规格在β网格0–0.8上只有一个根（路段06–22点NIV-z恰为0.500，已在0.49–0.51逐点核对，是真实的根）。

| 层面 | 时段 | NLS，实测q | NIV全市负荷，实测q | NLS，分配z | NIV，分配z |
|---|---|---|---|---|---|
| 边 | 0–24 | 0.151 (0.006) | 0.337 (0.017) | 0.134 (0.006) | 0.350 (0.021) |
| 边 | 6–22 | 0.149 | 0.389 | 0.104 | 0.344 |
| 路段 | 0–24 | 0.307 (0.014) | 0.727 (0.045) | 0.289 (0.015) | 0.594 (0.032) |
| 路段 | 6–22 | 0.249 | 0.713 | 0.206 | 0.500 |

  Almagro私家车：NLS 0.181，NIV 0.202。低于T0的小时占比：边0–24点21.7%、6–22点13.5%；路段25.1%、17.2%。

- 同一样本的线性对照（边，6–22点）：只放边FE时，ln t对ln q 0.105，对ln z 0.078，全市负荷作IV 0.200；加日期×小时FE时，对ln z（简化式）0.044，z作IV（V1）0.161。只放边FE时同一边内ln q与ln z相关0.61（0–24点0.88）。路段6–22点对应为0.135 / 0.106 / 0.207 / 0.058 / 0.176。
- 结论：
  1. 用他们的NLS，边层面β≈0.15，接近其0.18；下限模型扣掉平坦段，β比线性λ高，应与BPR高负荷段比较，不能直接与线性λ比。
  2. 分配车流直接作回归元，在只用日内时段变化时偏差不大（0.078 vs 0.105）；只用日间变化时z误差大（第一阶段0.27），只得0.044。Almagro做法只用时段变化，所以能用分配车流；我们的日间识别必须用实测车流。
  3. 全市负荷IV只利用时段变化，凡与时段同步、又影响用时的因素（高峰车辆组成、信号配时、行人、公交）都会进入，边0.34–0.39、路段0.59–0.73明显高于我们V1的0.16。Almagro用天气控制和按周重复来论证，我们只有一周，不能做同样的检验。暂不把NIV作为主估计。

## 2026-11-06 候选路网的强/弱连通分量对比

- 脚本 `data_work/src/diagnostics/network_components_compare.py`，输出 `data_work/outputs/network_components_compare_v1/`（`summary.json`、`components.csv`）。只读输入：Speed `手工路网_北京_Processed_Data`（手工修补前的百度原表）、本项目 `raw_connected_v1/v2/v3`、Speed `V1-OSM/data/intermediate/route_v3/beijing_arcs.parquet`。
- v3 来源：Speed `Road_processing/手工路网_北京_Processed_Data`，手工操作2026-04-10至05-29（加边1,612、切分539、改速75、删边40），有效手工边1,563条、1,662 km（中位220 m，p90 3.3 km），速度借上游百度路段。6-28复制到本项目并重建（边数不变，坐标范围恢复全市）。kunpeng确认（2026-11-06，经用户转述）：手工路网不能完整覆盖，但已对比过手工连接的系数；建议对需要完整路网的项目用连通分量的思路判断。
- 结果（按km计最大分量占比）：
  - 百度原表 = v1（两者完全相同，v1无手工边）：14,754节点、24,763边、17,322 km；强连通分量2,651个，最大占77.7%；弱连通分量137个，最大占91.5%。
  - v2（中心区裁剪）：强855个/最大84.9%；弱26个/96.6%。
  - v3：14,796节点、26,864边、19,641 km；强364个/最大96.3%；弱27个/99.2%。手工连接主要修复连通性。
  - OSM全部机动车道路（663,802条有向弧、116,524 km）：按原节点ID，强37,653个/最大24.9%，弱15,873个/27.1%。原因是节点ID带立交层号（`|L0`、`|L1`…），桥梁、隧道路段与地面路段在共用端点处不相连。忽略层号后：强2,739个/最大97.8%，弱1,341个/98.4%（再按约10 m取整几乎不变）。OSM的节点只在共用顶点处相连，上下穿越的道路一般不共用顶点，所以忽略层号大体正确，但需抽查。

## 2026-11-06 OSM立交层号检查与各路网的格对可达性（不以gate5为标准）

- 脚本 `data_work/src/diagnostics/network_grid_reachability.py`，输出 `data_work/outputs/network_grid_reachability_v1/`（`summary.json`、`pairs_sq3000_model_origin.parquet`、`osm_layer_merge_points.csv`、`osm_layer_merge_samples.csv`、`fig_osm_layer_merge_samples.png`）。百度与OSM坐标同为WGS84（百度节点到最近OSM节点中位6 m）。
- 第1步，OSM去层号：21,786个坐标有不同层号的弧相交。其中13,013个各层都在其OSM父边的端点（桥头接地面）；6,865个一端一内点；1,908个都在内点。按道路等级或路名，98.2%（21,388个）是同一条路在桥/隧段与引道相接；其余398个几乎都是只有两条弧（度为2，中位2），是同一条路的等级变化，不是上下穿越（穿越应为度4）。抽样12处地图一致。结论：去掉层号连接是安全的。
- 第2步，各路网取最大强连通分量，接入点为离格心最近的道路节点，绕行比 = 两格接入点间最短路长 / 两格心实际距离；网格为EPSG:3857方格，模型原点3 km、平移半格的3 km、6 km及其平移。注意这里的接入点规则比主线的edge-projection粗，绝对水平不能与gate5直接比，只比较路网之间。
- 模型原点3 km网格：

| 路网 | 有道路的格子 | 相邻格对绕行≤5（格对数） | 联通车流加权：绕行≤2 / ≤5 | 717条缺边候选：≤5（车流） | 247条单向缺边：≤5（车流） |
|---|---|---|---|---|---|
| v1（百度原表） | 1,018 | 79.7% | 30.9% / 65.8% | 27.1% | 39.7% |
| v3（手工连接） | 1,218 | 83.8% | 37.2% / 77.2% | 60.8% | 69.1% |
| OSM全部机动车道 | 3,016 | 88.7% | 80.5% / 98.4% | 97.6% | 98.7% |
| OSM三级路及以上 | 2,493 | 91.1% | 69.2% / 95.5% | 94.6% | 97.8% |

- 换网格（平移半格、6 km）排序不变：绕行比中位数 v1 1.85–2.10、v3 1.85–2.01、OSM全部 1.63–1.66、OSM三级及以上 1.60–1.62。
- 结论：格对缺失主要来自百度路网只含主要道路；v3手工连接修好了连通性但只能补回一部分；OSM三级路及以上已能覆盖95%以上的相邻格对车流，居住区等低等级道路不是必需的。OSM三级路及以上在百度有观测速度的长度比例：主干58%、干线43%、次干36%、三级10%、高速10%（高速偏低需核对）。

## 2026-11-06 OSM只保留百度观测路段（方案2）与借用上下游速度的覆盖（方案1诊断）

- 脚本 `data_work/src/diagnostics/network_osm_observed_variants.py`，输出 `data_work/outputs/network_osm_observed_variants_v1/summary.json`。观测标记取自Speed `route_v3/beijing_fold_costs_*`（五天相同），去层号。
- 方案2（OSM内只留有百度观测的弧，48,402条、11,366 km）：强连通分量26,060个，最大只占观测长度6.7%；弱连通分量4,248个，最大14.1%。加上≤50 m的三级及以上未观测短弧（路口碎段）后，最大强连通分量也只有12.6%。在模型原点3 km网格上只覆盖120个格子、1.3%的相邻格对联通车流。原因：百度→OSM对应表在OSM上落成不连续的片段（路口段、对向车道常未匹配），因此"OSM内只用观测路段"在拓扑上不可行；只用观测路段的稳妥版本应直接用百度自身的网络（v3）。
- 方案1诊断（三级及以上道路，38,740 km，观测占26.8%）：未观测长度中，21.7%与观测弧直接相连，38.5%在2跳内，50.2%在3跳内；按等级，可从相邻观测弧借速度的未观测长度占比：高速26.7%、干线26.1%、主干32.2%、次干19.2%、三级18.6%、各类匝道35–61%。route_v3弧没有路名（roadname为空），同名道路借用需改从 `beijing_osm_edges.parquet` 按source_edge_id取路名，本次未算。

## 2026-11-06 方案1B：OSM观测路段＋短缺口连接插补；v11手工走廊与v3手工连接的关系

- 脚本 `data_work/src/diagnostics/network_osm_gap_bridging.py`，输出 `data_work/outputs/network_osm_gap_bridging_v1/summary.json`。规则：保留有百度观测的OSM弧，三级及以上未观测弧仅当两端都在观测弧L米（沿三级及以上道路、不分方向）以内时加入，即只补约2L以内的缺口。
- 结果（最大强连通分量占比 / 3 km模型原点网格有道路格子 / 相邻格对联通车流绕行≤5、≤2 / 相对观测长度新增）：L=0 19.3%/219/7.9%、5.3%/+8%；L=100 m 56.8%/828/59.8%、34.7%/+19%；L=300 m 74.6%/1,018/80.6%、52.9%/+37%；L=600 m 81.9%/1,139/87.6%、62.7%/+67%；L=1000 m 84.4%/1,212/90.7%、66.4%/+99%；L=2000 m 87.9%/1,359/93.4%、68.5%/+142%。对照：v3 77.2%/37.2%，OSM三级及以上全插补95.5%/69.2%。L=300 m时插补长度约占网络27%，覆盖已超过v3。
- v11手工走廊（`config/manual_centerline_groups.json`，129组：22组按路名，多为G/S/X郊区公路，107组按roadseg_id）是中心线几何规则：指定哪些复杂/双幅道路手工建中心线，不改原始路网拓扑；v1中对应963条原始边。v3手工编辑是拓扑修补（加连接、切分）。两者不冲突：v3中同一批走廊有1,058条边（v1的963条中78条被切分或删除，road_id保留），v3的1,562条手工边中149条接到走廊节点上。在v3上重建中心线时，走廊按road_id/路名重新选取即可，需另定手工连接边进入骨架还是作为显式连接。
- 以往百度路径API爬取（未做北京格对）：中心线验证（11,890条有向中心线，FREE/AM/PM）、原始路段验证（23,799段）、全国乡镇OD通勤。代码在 `src/centerline_baidu_validation/`、`src/raw_baidu_validation/`、`src/national_tidal_commuting/`。
- 补充核对（2026-11-06）：中心线stage01的输入是 `raw_data/gis/roads_baidu/beijing_roads.shp`（24,763个roadseg_id），与v1的edge_id、road_id完全一致；v3的25,302条非手工边的road_id全部在该shp中，shp的roadseg_id也全部出现在v3中（切分后的碎段共用原road_id）。v11与v3基于同一套百度原始路网，v11走廊规则按roadseg_id/路名选取，可在v3上直接执行；需要改的只是stage01的输入（从shp改为读v3边表的几何，按road_id归并）以及v3手工连接边的处理规则和车道口径。

## 2026-11-06 OSM有向速度与方案1A/1B格对用时（nextstep D第1–3步）

- 第1步 `data_work/src/network/build_osm_directed_speed.py` → `data_work/outputs/osm_directed_speed_v1/`。OSM三级及以上有向弧（171,070条、38,740 km）按行驶方向匹配百度有向路段（弧中点20 m内、方向差≤30°，<10 m的弧用父边方向）：匹配62,657条弧、20,716个百度路段；有向观测长度占27.9%（Speed无向标记26.8%，其中3,281 km只有一个方向真有观测）；每个百度路段匹配到的OSM长度/百度长度中位1.01（四分位0.94–1.15），百度总长的92%匹配上。Speed的 `bj_base` 合并上下行，不用。各等级有向观测长度：主干60%、干线51%、次干34%、高速12%、三级11%。
- 第2–3步 `data_work/src/network/build_osm_grid_travel_time.py` → `data_work/outputs/osm_grid_travel_time_v2/`（`arc_speeds_1A.parquet`、各方案×网格的 `pairs_*.parquet`、`summary.json`）。时段为工作日早高峰7–9点、晚高峰17–19点、畅通2–5点。弧速度来源（按长度）：1A 观测27.6%、借上下游24.4%、等级×圈层48.1%；1B 观测70.6%、短缺口29.4%。格对用时取每格3×3格点最近道路节点之间所有路径的平均；观测占比取格心节点之间路径。`osm_grid_travel_time_v1/` 只有格心单节点用时，噪声大，已被v2取代，可删。
- 模型原点3 km网格：1B 3,800个有向相邻格对，早高峰用时中位8.1分钟，早高峰/畅通1.14，方向比（慢/快的倒数，min/max）中位0.90，用时中观测占比中位86%（四分位70%–99%）；1A 8,174个格对，7.6分钟，1.12，0.95，观测占比中位21%。平移半格和6 km网格结果相近。1A把方向不对称冲淡（插补的相对速度不分方向）。
- 与现行gate5对照（共有格对）：1B覆盖v1 gate5边的91%，1A 99%。用时水平相关低：早高峰对数相关0.43（观测占比≥0.8的格对0.49），畅通0.25。拥堵比（早高峰/畅通）相关0.68；方向对数比相关0.36，符号一致66%。v1的畅通用时与其自身路长几乎不相关（0.03），本版本为0.71，说明v1的用时水平主要由连接线（connector）决定。【2026-11-06更正：本条比较用的是gate5 bundle里继承的v11中心线用时t_min/t_ff_min，不是模型用的tau_obs_min/tau_ff_min；更正后的数字见下文"gate5用时列更正"一节，"与路长不相关"的结论作废。】结论：两种构造在拥堵程度上一致，在用时水平和方向比上差别大，需要外部基准（第4步百度路径查询）判断哪个更接近实际。

## 2026-11-06 格对百度路径查询外部基准：准备完成，AK被停用（nextstep D第4步）

- 抽样 `data_work/src/network/build_grid_pair_baidu_manifest.py` → `data_work/outputs/grid_pair_baidu_validation_v1/data/sample_manifest.csv`：在1B与gate5（v1）共有、且往返都在的1,319对3 km模型原点格对中，按"两方向观测占比都≥0.8与否"×"两版本分歧三分位"6层各抽50对，共300对、600条有向请求；起终点为格心（WGS84），不加途经点；附1B与v1的用时以备对比。查询沿用 `src.raw_baidu_validation.query_baidu_direction`（`--output-dir outputs --version-id grid_pair_baidu_validation_v1 --sample-manifest ...`）。
- 修复 `src/national_tidal_commuting/baidu_rate_limit.py`：锁文件里存的是 `time.monotonic()`，重启后单调时钟归零，旧时间戳（2026-06-01）大于当前值，使每次请求无限等待；改为时间戳大于当前值时视为过期。
- 5条试查全部返回 status 201"APP被用户自己禁用，请在控制台解禁"：当前AK对应的百度应用已停用，需在百度地图开放平台控制台解禁或换新AK后再跑。试查目录 `api/crawl_sessions/FREE_20261107T022026_smoke/` 无有效结果。

## 2026-11-06 联通RN车流上OSM路网（nextstep D第5步）

- 脚本 `data_work/src/network/build_rn_osm_flow.py` → `data_work/outputs/rn_osm_flow_v1/`（`osm_arc_rn_flow.parquet` 弧层面工作日/早高峰经过量，`rn_pair_osm_route.parquet`，`grid_edge_flow_sq3000_model_origin.parquet`，`summary.json`）。
- 联通RN节点就是OSM节点：265,493个节点吸附距离中位0.0 m、p90 0.01 m，99.3%在5 m内。611,743个有车流的节点对中，按OSM最短路（≤1.5×弦长+50 m）落位596,068对，覆盖工作日车公里98.5%、早高峰98.6%；路径/弦长中位1.00、p90 1.12。对照：此前弦→百度路段几何匹配只覆盖57%车公里。
- 车公里按OSM等级：主干21.4%、三级19.6%、次干19.4%、高速17.7%、干线14.8%，匝道约4%，居住区等低等级约3%。三级及以上路网承载约97%的联通车流。
- 3 km模型原点网格：跨格弧汇总的格边车流与弦长分配相关0.90（对数，双方为正），gate5边上0.91，98.5%的gate5边有OSM车流；中位比值0.87。
- 按早高峰车公里加权，1A弧速度来源：观测56%、借上下游25%、等级×圈层16%、非三级以上3%。分等级观测占比：干线85%、主干79%、次干64%、三级39%、高速仅25%（百度速度路网缺高速）。1B只保留观测路段及300 m内短缺口，高速大段会缺失，路由会被迫走地面主干路；比较1A与1B的格对用时时需留意这一点。

## 2026-11-06 拥堵函数IV后续：带下限模型加日期×小时FE、分段第一阶段（nextstep A2检验、A3(a)剩余项）

- 脚本 `data_work/src/model/full_estimation/run_congestion_iv_followup.py` → `data_work/outputs/congestion_iv_followup_v1/`（`summary.json`、各层各时段 `*_first_stage_by_z_decile.csv`）。样本同 `congestion_almagro_spec_v1`；只用观测数据。无bootstrap标准误。
- 下限＋常数弹性 ln t = max{ln T0, μ_u + δ_{日期×小时} + β ln q}，z（routed-OD）残差作工具（与线性V1/S1同识别）：边 06–22点 0.245、0–24点 0.207；路段 0.420、0.377。同规格NLS：边0.113/0.070，路段0.151/0.098。对照线性IV：边V1 0.161（06–22）/0.085（0–24），路段S1 0.176/0.094。下限模型把全天与日间的差距从约一半缩到约15%，说明线性λ随时段窗口变化主要来自夜间平坦段；扣掉平坦段后，边层面弹性约0.21–0.25，路段约0.38–0.42。仍远低于λ_phys=1.1。
- 分段第一阶段（V1/S1残差，按z/z95十分位）：06–22点边层面局部斜率在0.20–0.30之间，最高档（z/z95≈1.0）0.27；路段0.15–0.38，最高档0.31。没有出现"到顶后第一阶段变平"，工具变量在高负荷段仍有效。0–24点边层面中间档（z/z95 0.45–0.60）斜率只有0.03–0.13，夜间与日间混在一起时中段识别弱。

## 2026-11-06 远端OD构造的工具变量（nextstep A3(a)剩余项）

- 决定：拥堵函数估计主窗口统一为06–22点，0–24点只作稳健性；带下限模型只作文献对照，不再算bootstrap标准误。
- 脚本 `data_work/src/model/full_estimation/run_congestion_far_od_iv.py` → `data_work/outputs/congestion_far_od_iv_v1/`（`edge_far_od_z_hour.parquet`、`summary.json`）。同一OD缓存、3 km格、gate5网络、畅通最短路与时间平移；对每条边只保留起点格和终点格都离该边两端格超过D格（切比雪夫距离）的出行。D=-1为原z，逐格复现（最大差0）。保留的出行×边记录：D=0 77%，D=1 55%，D=2 41%。
- 线性IV，共同样本（四种z都>0的单位小时；边131,107/285,827，1,206条边；路段905,111/1,756,923，8,214段）。06–22点：

| z | 边V1 | 路段S1 | 边第一阶段系数 |
|---|---|---|---|
| 原z | 0.130 (0.011) | 0.162 (0.014) | 0.36 |
| 去掉本格 D=0 | 0.116 (0.012) | 0.142 (0.014) | 0.31 |
| 去掉3–6 km D=1 | 0.100 (0.013) | 0.128 (0.017) | 0.24 |
| 去掉6–9 km D=2 | 0.083 (0.013) | 0.114 (0.017) | 0.17 |

  0–24点同样单调下降：边0.073→0.035，路段0.104→0.057。第一阶段F（边聚类）均在430以上，不存在弱工具问题。
- 解读：排除的出行离边越远，λ越低，D=2时比原z低约35%（边）和30%（路段）。可能原因有两个，目前无法区分：(1) 近处出行与该边共享局部冲击，原z高估；(2) 本地出行和过境出行对该边的作用不同，不同z识别的是不同人群的局部效应。两者都意味着λ的可信范围应写成区间（06–22点边层面约0.08–0.16），而不是单一值；无论哪端都远低于1.1。
- 注意：共同样本只有全样本的约一半（外围边在远端z下常为0），共同样本上原z的边V1是0.130，低于全样本的0.161。

## 2026-11-06 OSM格对用时的插补稳健性：1A选项(a)–(d)与1B L=600 m（nextstep D第6步）

- `data_work/src/network/build_osm_grid_travel_time.py` 新增参数 `--hops`、`--borrow relative|absolute`、`--fallback class_zone|class`、`--l-gap`、`--schemes`、`--grids`；默认值与v2相同（1A速度来源份额与v2逐位一致）。各变体只跑模型原点3 km网格，输出 `data_work/outputs/osm_grid_rob_{1b_L600,1a_absolute,1a_hops1,1a_hops5,1a_fallback_class,1a_classonly}_v1/`。对比脚本 `data_work/src/diagnostics/compare_osm_grid_robustness.py` → `data_work/outputs/osm_grid_robustness_compare_v1/`（`robustness_vs_main.csv`）。
- 与主版本（v2的1A、1B L=300）在相同有向格对上比较（多接入点平均用时tm）：

| 变体 | 早高峰用时相关（对数） | 早高峰/畅通相关 | 方向比相关 | 方向符号一致 | 用时中位比 |
|---|---|---|---|---|---|
| 1A (a) 借绝对速度 | 0.997 | 0.92 | 0.99 | 0.87 | 1.01 |
| 1A (b) 1跳 | 0.999 | 0.98 | 0.99 | 0.96 | 1.00 |
| 1A (b) 5跳 | 1.000 | 0.99 | 1.00 | 0.97 | 1.00 |
| 1A (c) 后备不分圈层 | 0.984 | 0.995 | 0.99 | 0.91 | 1.12 |
| 1A (d) 只用等级中位数 | 0.997 | 0.94 | 0.99 | 0.92 | 1.00 |
| 1B L=600 m | 0.939 | 0.97 | 0.74 | 0.89 | 1.00 |

- 结论：1A的格对用时对插补规则不敏感，用时水平与方向比的相关都在0.98以上；不分圈层会使用时整体高约12%，借绝对速度或只用等级中位数会改变部分格对的拥堵程度（早高峰/畅通相关0.92–0.94）。1B对缺口长度更敏感：L从300 m放宽到600 m，最大强连通分量从75%升到84%，可路由格对从3,788增到4,022，方向比相关只有0.74。方向不对称的结论在1B下需同时报告L=300与600。
- 注：`--fallback class` 时速度来源标签仍写作"class_zone"，实际后备不分圈层。

## 2026-11-06 v3上重建中心线（v11走廊规则），并更正gate5用时列（nextstep D第6步）

- 输入：`data_work/src/network/build_centerline_v3_input.py` → `data_work/outputs/centerline_v3_input_v1/data/beijing_roads_v3.gpkg`。v3有向边26,864条，其中百度原有25,302条（roadseg_id即原id，拆分段沿用母id），手工连接边1,562条（约1,667 km）。手工连接边的roadtype取两端节点上百度边的众数（并列取较高等级），据此由stage04给车道；它们没有速度。Excel单元格上限32,767字符截断了4条长路的几何，改用原始shp几何。
- stage03原样读取10 GB速度CSV（每行带几何）两次被系统杀掉（exit 137）。改读同目录下只含 roadseg_id、speed、day、hour 四列的副本 `speed_Beijing_all_slim.csv`（行数相同，24,336,848行）；配置 `data_work/config/manual_centerline_plan1_v3.json`。
- 运行：`run_full_pipeline.py --config config/manual_centerline_plan1_v3.json --version-id centerline_v3_rules_v11_v1 --from-stage stage01 --to-stage stage08 --grid-type square --skip-figures`（stage03起断点续跑）。
- 结果对v11：建模用有向中心线11,890 → 13,842条，长度9,696 → 10,652 km（+10%）；原始段匹配率99.4% → 98.5%；速度观测匹配97.5%。3 km格边4,632 → 4,606条，强连通分量33 → 34个、最大分量覆盖98%的格；与v11共有格边上早高峰用时对数相关0.975，方向比相关0.943。规则相同时，换成v3底网对格边用时影响很小。
- 对照脚本 `data_work/src/diagnostics/compare_centerline_v3.py` → `data_work/outputs/centerline_v3_compare_v2/`（v1误用gate5的t_min，作废）。相邻格联通车流（OSM路由）落在网络格边上的份额：v11中心线91.2%，v3中心线91.3%，gate5 65.6%，OSM 1A 99.8%，1B 92.1%。
- **gate5用时列更正**：gate5 bundle以v11中心线bundle为底，edgeproj用时写在 `tau_obs_min`/`tau_ff_min`（模型读取这两列，见 `spatial_equilibrium.py`），`t_min`/`t_ff_min` 是继承的中心线用时。此前OSM与gate5的对照（`osm_grid_travel_time_v2` 的 vs_gate5 字段、`osm_grid_robustness_compare_v1` 的 corr_log_tm_AM_vs_gate5_v1、格对样本v1的分层）用的都是 `t_min`，实际是与v11中心线比较。已改 `build_osm_grid_travel_time.py` 的 compare() 改读tau列；格对样本重建为 `grid_pair_baidu_validation_v2`（按与gate5 tau的分歧分层，同时附中心线用时），`scripts/run_grid_pair_validation_week.sh` 改指v2；v1样本作废（未发起查询）。
- 更正后：gate5的畅通用时与路长相关0.84（不是0.03）；gate5早高峰/畅通中位1.149。各构造两两比较（共有有向格边，早高峰用时对数相关 / 方向比相关 / 方向符号一致）：gate5–OSM 1A 0.36 / 0.29 / 59%，gate5–1B 0.40 / 0.33 / 60%，gate5–v11中心线 0.51 / 0.41 / 61%，中心线–OSM 0.41–0.44 / 0.27–0.34 / 59–65%，1A–1B 0.59 / 0.41 / 71%，v11–v3中心线 0.98 / 0.94 / 96%。OSM用时约为gate5的0.76–0.83倍，中心线的1.3–1.4倍。
- 结论：格边层面的用时水平和方向不对称强烈依赖路网与接入点的构造，几种合理的构造之间方向比相关只有0.3–0.4；同一规则换底网（v1→v3）影响很小。外部基准是决定主版本的唯一依据。

## 2026-11-06 换底网重跑主线（OSM 1A、1B；2025 OD与主线参数不变）

- 目的：只换路网，看主线潮汐结果变多少。百度基准暂缓（AK未解禁）。
- bundle：`data_work/src/model/travel_cost_bundle/build_osm_model_bundle.py`。边=3 km格的有向相邻格对，用时取 `osm_grid_travel_time_v2` 的多接入点平均（tm_AM/tm_FF），绕行门槛5（中心点路径km/格心距离），格层面最大强连通分量。车道=中心点早高峰路径上按长度加权：匹配到百度路段的弧沿用gate5车道表（`road_class_lane_mean`，占OSM长度28%），其余取同一OSM等级已匹配弧的众数。节点与OD取自 `model_input_edgeproj_v2_lcc_gate5_od2025_v2`。
  - 主比较 `_v2`：只保留gate5的1,104个格。1A 1,059格、3,478边，保留98.6%的小汽车通勤；1B 924格、2,872边，保留97.1%。
  - `model_input_osm1a_gate5_od2025_v1` 与 `model_input_osm1b_L300_gate5_od2025_v1`：保留路网可达的所有格，gate5以外的格人口记为0（1A为2,183格）。只作对照。
  - 重新追踪的中心点路径用时与源文件逐条一致（最大差0）。
- 运行：`run_tidal_lane_diagnostics_gate5.py` 默认主线（anchored、λ_phys=1.1、α=0.05、β=−0.10、score_bpr_st）。输出 `tidal_lane_diagnostics_{osm1a_gate5,osm1b_L300_gate5}_bpr_pos05_physical_time_od2025_{v2,v1}`；另在λ_phys=0.2下跑了三套（`…_{gate5,osm1a,osm1b}_bpr_pos05_physical_time_lam020_od2025_v2`）。汇总 `data_work/src/diagnostics/compare_network_mainline.py` → `data_work/outputs/network_mainline_compare_v2/`（v1不含λ=0.2，内容相同，可删）。

| top100 | gate5 | OSM 1A | gate5 λ=0.2 | 1A λ=0.2 |
|---|---|---|---|---|
| W̄ ST / LT | +1.11% / +0.43% | +2.61% / +0.83% | +0.22% / +0.16% | +0.42% / +0.30% |
| 基准平均物理通勤时间 | 14.1 分钟 | 16.9 分钟 | 14.1 | 16.9 |
| 每人ST省时 / LT增加 | 56 / 13 秒 | 90 / 27 秒 | 12 / 7 秒 | 20 / 13 秒 |
| 选中走廊与gate5重合（top25/50/100） | | 10 / 16 / 40 | | 10 / 16 / 40 |

- 1A：福利符号与"ST省时、LT回升"的方向不变，幅度约为gate5的2倍（λ=1.1与0.2都一样）；选中走廊只有约四成与gate5相同。λ从1.1降到0.2，福利缩小约80%，两个路网缩小比例相近。
- 1B：基准内生均衡在500次外层迭代内不收敛（λ=1.1和0.2都是），人口塌到sq_34_52（观测居住份额0.02%），结果不可用。用时外生时能一步复现观测；α=0时同样塌缩，所以与集聚外部性无关。机制：
  1. 1B有4.1%的边观测用时低于畅通用时，anchored式取max(τ_ff, ·)，第一次迭代就离开观测点；
  2. sq_32–35_52–53一带几条边的模型车流锚点约1e-9，车流稍有转入，(Ξ/Ξ_obs)^λ就达1e7量级，用时爆掉；
  3. 叠加τ^−θ的1e-12截断（E9），人口塌缩。
  gate5也有接近零的锚点（1.6%的边低于中位数的千分之一），但没有触发。修复需要改模型（给锚点设下限、或换带下限的拥堵函数），属于A4的决定，本次未改。
- 经验：zsh下 `set -- $p` 不拆分变量，循环传多参数要用函数。

## 2026-11-08 两种拥堵函数 × 六个路网；中心线路由成本；1B修复

- 求解器（`equilibrium_solve.py`）新增两种锚定式拥堵函数，默认值与原anchored主线不变：
  - `ce_floor`：带畅通下限的常数弹性，max(f, τ_obs·dev^β)，即 `run_congestion_iv_followup.py` 估计的下限模型；
  - `bpr`：f·(1 + m_obs·dev^b)，m_obs = τ_obs/f − 1。
  - 两者都取f = min(τ_ff, τ_obs)，使观测状态严格是不动点（原anchored在τ_obs<τ_ff的边上会被抬到τ_ff，这是1B第一次迭代就离开观测点的原因）。
  - 新增 `xi_floor`：dev中的车流比改为max(Ξ, f)/max(Ξ_obs, f)。观测点仍是不动点，但锚点车流接近零的边不再被放大。tidal脚本参数为 `--xi-floor-rel`（取中位锚点车流的倍数），默认0。
- 1B修复检验（ce_floor β=0.25，top100）：
  - 基准都一步收敛；
  - 不设下限时，长期反事实不收敛，福利+1067%，不可用；
  - 下限取中位数的1%后，ST/LT都收敛（+0.505%/+0.345%）；
  - gate5加不加下限结果完全相同。
- 路网：
  - **v3 gate5 车道修复版**：`raw_connected_v3_grid_travel_time_edgeproj_v2_lanefix`（用时与6月v3逐对相同，车道改为按路径加权）→ `model_input_raw_connected_v3_edgeproj_v2_lcc_gate5_lanefix_v1` → `…_od2025_v1`（1,133格、2,901边；2025 OD按本路网的格取，覆盖93.8%）。
  - **中心线路由成本（新）**：`data_work/src/model/travel_cost_bundle/build_centerline_routed_bundle.py` → `model_input_centerline_{v11,v3}_routed_gate5_od2025_v1`。
    - 现行中心线成本只是格心距离÷速度，没有路由。中心线层本身不能直接路由：骨架端点之间留有5–7个投影单位的缝，丁字口没有节点，不同中心线只有6–7%的端点重合。
    - 做法：每个端点吸附到阈值内的其他中心线，在投影点切开并合并节点，只吸附端点，不在中段交叉处相连；300 m以内无速度的短中心线用相接中心线速度插补，更长的去掉。
    - 阈值10/15/25个投影单位时，强连通分量所占长度：v11为72%/84%/89%，v3为41%/79%/90%。主版本取25（约19 m）；插补长度约3%。
    - 路由与格对用时沿用OSM的做法（多接入点平均、绕行门槛5、只保留gate5的格）。v11版504格，留住81%的通勤；v3版432格，留住78%。中心线只覆盖主路，很多格没有接入点。
    - v3新增的手工连接边无速度且多长于300 m，被去掉，所以v3中心线路由与v11接近。
  - 底层工具：`build_osm_model_bundle.py` 拆出共用的 `assemble()`，重建1B与原bundle逐列一致。
- 运行：`run_tidal_lane_diagnostics_gate5.py`，主线α=0.05、β=−0.10、score_bpr_st，四个spec。ce_floor取β=0.25（边层面IV估计），bpr取b=4.25（11-04分档拟合，非IV估计），都加 `--xi-floor-rel 0.01`。输出 `tidal_lane_diagnostics_<net>_{ce_floor_b025,bpr_b425}_fl001_s4_od2025_v1`；汇总 `data_work/src/diagnostics/compare_network_form_matrix.py` → `data_work/outputs/network_form_matrix_v1/`。12个运行的基准、ST、LT全部收敛。

| top100，W̄ ST / LT | gate5 | v3 gate5 | OSM 1A | OSM 1B | 中心线v11路由 | 中心线v3路由 |
|---|---|---|---|---|---|---|
| 常数弹性（带下限）β=0.25 | +0.27 / +0.19 | +0.39 / +0.27 | +0.53 / +0.35 | +0.51 / +0.35 | +0.62 / +0.37 | +0.66 / +0.41 |
| BPR b=4.25 | +1.51 / +0.53 | +2.44 / +0.73 | +2.58 / +0.85 | +3.28 / +0.96 | +3.89 / +0.97 | +4.64 / +1.10 |
| 选中走廊与gate5重合 | | 59 | 40 | 41 | 41 | 40 |

- 结论：
  1. 两种函数、六个路网下，"ST省时、LT回升、福利为正"的方向都不变。
  2. 幅度主要由函数决定：BPR约为常数弹性的5–7倍（ST）。
  3. 同一函数下，gate5最小，其他路网约为它的1.4–2.5倍。
  4. 选中走廊与gate5只有约40–60/100相同，排序依赖路网。
  5. 中心线路由两版只覆盖约500格、八成通勤，不能和其他路网逐格比较。
- 已作废的中间输出：`tidal_lane_diagnostics_{gate5,osm1b}_ce_floor_b025_{fl0,fl001}_od2025_v1` 是只跑top100的修复检验，保留作记录；`network_mainline_compare_v1` 可删。

## 2026-11-08 BPR指数b的需求IV估计；用IV值重跑六个路网（nextstep A3(b)）

- 决定（用户确认）：锚点车流下限 `--xi-floor-rel 0.01`（中位锚点车流的1%）作为默认做法；其他取值暂不测。
- 脚本 `data_work/src/model/full_estimation/run_congestion_bpr_iv.py` → `data_work/outputs/congestion_bpr_iv_v1/summary.json`。模型 `bpr` 形式只有指数b是自由参数，a与容量c并入单位×时段截距：ln t = ln T0_u + ln(1 + exp(μ_u + δ_{日期×小时} + b ln q))。T0、样本、矩条件与工具（z残差）同11-06下限模型；无bootstrap。每个规格在b网格上只有一个根。

| | b，IV | b，NLS | 工作日7–9点局部弹性中位，IV |
|---|---|---|---|
| 边 06–22点（主值） | 1.65 | 0.92 | 0.20 |
| 边 0–24点 | 1.55 | 0.73 | 0.19 |
| 路段 06–22点 | 2.19 | 1.12 | 0.20 |
| 路段 0–24点 | 2.08 | 0.93 | 0.19 |

- IV值约为11-04分档拟合（4.25）的40%；高峰局部弹性b·m/(1+m)约0.20，与ce_floor的β=0.25接近。NLS约为IV的一半，与线性、下限模型的方向一致。
- 用b=1.65重跑六个路网（`tidal_lane_diagnostics_<net>_bpr_b165_fl001_s4_od2025_v1`，其他设定同上节），全部收敛。汇总 `compare_network_form_matrix.py` 加入 `bpr_iv` → `data_work/outputs/network_form_matrix_v2/`。

| top100，W̄ ST / LT | gate5 | v3 gate5 | OSM 1A | OSM 1B | 中心线v11路由 | 中心线v3路由 |
|---|---|---|---|---|---|---|
| 常数弹性（带下限）β=0.25 | +0.27 / +0.19 | +0.39 / +0.27 | +0.53 / +0.35 | +0.51 / +0.35 | +0.62 / +0.37 | +0.66 / +0.41 |
| BPR b=1.65（IV） | +0.68 / +0.37 | +1.03 / +0.51 | +1.14 / +0.57 | +1.37 / +0.65 | +1.59 / +0.67 | +1.81 / +0.76 |
| BPR b=4.25（拟合） | +1.51 / +0.53 | +2.44 / +0.73 | +2.58 / +0.85 | +3.28 / +0.96 | +3.89 / +0.97 | +4.64 / +1.10 |

- 结论：用IV的b后，BPR与常数弹性的差距从5–7倍缩到约2.5倍（ST）、约2倍（LT）。高峰局部弹性两者相近，剩下的差距来自BPR的弹性随负荷上升，而选中的走廊正是负荷最高的边。选边由观测数据打分，不随拥堵函数变化，所以各函数下与gate5的重合数相同。

## 2026-11-08/09 定主线拥堵函数；两种函数在gate5上试跑A4

- 决定（用户）：主线拥堵函数用 `ce_floor`（β=0.25，锚点车流下限1%）。BPR（b=1.65）保留为对照；它的模型推导（存在性/唯一性、折点、边际外部成本与定价）还没做，推导前不进正文。记录与待做清单：`Documents/L2_notes/bpr_congestion_version_20261108.md`。
- 代码：`stage2_counterfactuals.build_congestion_solver_kwargs` 新增 `xi_floor_rel`（默认0，结果不变）；`run_stage2_gate5.py`、`run_policy_comparison_gate5.py`、`run_symmetric_input_reestimated_suite_gate5.py`、`run_selection_split_day_check_gate5.py` 新增 `--congestion-form`（ce_floor/bpr）、`--congestion-lambda`、`--xi-floor-rel`，默认值都与原主线相同。政策比较在非anchored函数下跳过P6/P7：现有实现把税加在τ_ff上，新函数的下限是min(τ_ff, τ_obs)，税会被截掉，BPR还会改变加价项，manifest里写明原因。
- 运行（gate5、2025 OD，`model_input_edgeproj_v2_lcc_gate5_od2025_v2`，只换函数，与11-03 anchored批次对照）。后缀 `_{ce_floor_b025,bpr_b165}_fl001_od2025_v1`：Stage 2、政策比较、对称输入重估（几何、算术）、分日选边。潮汐见上节 `tidal_lane_diagnostics_gate5_*_fl001_s4_od2025_v1`。所有均衡收敛（Stage 2各8个、分日各31个、对称各4个基准）。

| W̄变化（%），短期 / 长期 | anchored λ=1.1（11-03） | ce_floor β=0.25 | BPR b=1.65 |
|---|---|---|---|
| P1 潮汐top100 | 1.11 / 0.43 | 0.27 / 0.19 | 0.68 / 0.37 |
| P2 同走廊扩建 | 1.13 / 0.48 | 0.30 / 0.22 | 0.70 / 0.39 |
| P3 按q·延误扩建 | 1.41 / 0.55 | 0.34 / 0.25 | 0.89 / 0.45 |
| P4 均匀扩建（同车道公里） | 0.28 / 0.13 | 0.08 / 0.06 | 0.13 / 0.08 |
| P5 车道数×1.10 | 1.14 / 0.52 | 0.33 / 0.24 | 0.54 / 0.31 |
| 对称输入偏差，潮汐LT（几何 / 算术） | −0.030 / 未跑（11-03算术版无潮汐项） | +0.003 / +0.004 | −0.127 / −0.117 |
| 选边分日：winner's curse | 7.3% | 7.3% | 6.7% |

- 结论：
  1. 长期P3 > P2 > P1 > P4在三种函数下都成立。P5（车道×1.10）的位置随函数变：anchored与ce_floor下高于P2，BPR下低于P1。ce_floor下各政策幅度约为anchored的1/4–1/2。
  2. 选中的走廊在三种函数下完全相同（gate5与奇偶日bundle各100/100）：基准车流等于观测车流，选边分数不随函数变。
  3. 对称输入重估的偏差：ce_floor下几乎为零（潮汐+0.003个百分点）；BPR下潮汐长期效应被对称输入低估约三分之一（0.366→0.239）。BPR的弹性随加价上升，方向不对称的用时差在BPR下影响更大。这一项要在BPR推导完成后再解读。
  4. winner's curse与11-03几乎相同。
- 注意：本次分日检验没有传 `--mainline-top100`，脚本默认读2022 OD的主线列表，所以 `overlap.json` 中odd/even与mainline的重合（74/75）是对旧列表算的。对2025 OD列表（`tidal_lane_diagnostics_gate5_bpr_pos05_physical_time_od2025_v2`）重合为86（odd），与11-03相同。其他数字不受影响。
- 未重跑：自由流稳健性quick套件、Phase A/B、福利与sorting诊断、AA/Spec C（与函数无关或由上述结果派生）。路网仍是gate5，底网决定后再按新路网跑。

## 2026-11-09 OSM上的A4试跑（ce_floor）；发现1B与中心线路由网络违反谱半径条件

- 运行（ce_floor β=0.25、下限1%，2025 OD）：`stage2_endogenous_{osm1b,osm1a}_ce_floor_b025_fl001_od2025_v1`、`policy_comparison_{osm1b,osm1a}_ce_floor_b025_fl001_od2025_v1`、`symmetric_input_reestimated_{osm1b,osm1a}_ce_floor_b025_fl001_{geometric,arithmetic}_od2025_v1`；新建对称输入包 `model_input_{osm1b_L300,osm1a}_gate5_od2025_v2_symmetric_{geometric,arithmetic}_v1`。OSM没有奇偶日bundle，分日选边未跑。
- OSM 1A（结果可用）：长期W̄ P3 0.41 > P2 0.40 > P5 0.38 > P1 0.35 > P4 0.08（%），短期P1 0.53；对称输入使潮汐长期效应从0.35降到0.25（偏差−0.10个百分点，几何与算术相同；gate5为+0.003）。
- OSM 1B的P4/P5给出+550%至+1032%，查明是模型条件不满足，不是政策效果：
  - 路径矩阵A_kl = 1/(κ·τ_kl)，(I−A)^{-1}只有在谱半径ρ(A) < 1时才等于路径级数之和。所有运行都在反演时固定κ=1（`invert_fundamentals(kappa=1.0)`），没有检查ρ。
  - ρ(A)在观测用时处：gate5 0.51、v3 gate5 0.54、OSM 1A 0.83、**OSM 1B 1.25、中心线v11路由 1.33、中心线v3路由 1.95**；在min(τ_ff, τ_obs)处分别为0.59、0.59、0.93、1.41、1.41、2.06。1B与中心线路由的格边更多、更短，节点出边的1/τ之和超过1。
  - ρ > 1时 (I−A)^{-1} 不是正矩阵，求解器把负值截到1e-12，结果没有意义。1B上政策不变、只把迭代起点换成τ_ff，短期W̄也会变+227%；gate5上两种起点结果相同。
- 影响：OSM 1B、中心线v11路由、中心线v3路由上的所有模型结果作废，包括 `network_form_matrix_v1/v2` 中这三列、11-06的1B anchored运行、11-08的"1B修复"检验，以及本节1B的Stage 2/政策/对称结果。1B此前的不收敛与人口塌缩，至少部分应来自这一条，而不只是近零锚点车流；锚点车流下限是否仍需要，要在修好κ后重新检验。gate5、v3 gate5、OSM 1A不受影响（ρ < 1）。
- 待定（需用户决定）：κ的取法。现有 `_pick_kappa`（行和 ≤ 0.9）正是为这个条件写的，但反演时没有启用。

## 2026-11-09 κ按谱半径条件取值；1B与中心线路由重跑（ce_floor）

- 决定（用户）：每个网络取满足条件的最小κ。规则（`equilibrium_solve.resolve_kappa`，`--kappa-rule spectral`）：κ = max(1, ρ₁/0.95)，ρ₁是κ=1、τ = min(τ_ff, τ_obs)时A的谱半径。ce_floor与bpr下用时不低于这个下限，所以所有反事实都满足ρ < 1。默认 `--kappa-rule fixed1`（κ=1）保持旧结果不变；也可传数值（敏感性）。四个运行脚本（tidal、Stage 2、政策比较、对称重估）都已接入，每次运行写 `kappa.json`；对称重估在两个世界中用有向bundle的κ。
- 各网络κ：gate5、v3 gate5、OSM 1A为1（ρ₁分别0.59、0.59、0.93，结果不变）；OSM 1B 1.483；中心线v11路由1.480；中心线v3路由2.168。
- 潮汐top100（ce_floor β=0.25），全部收敛：

| | κ | W̄ ST / LT（%） | 每人省时 ST / LT（秒） |
|---|---|---|---|
| gate5 | 1 | 0.267 / 0.188 | 15.3 / −8.2 |
| OSM 1A | 1 | 0.526 / 0.348 | 25.2 / −15.1 |
| OSM 1B | 1.483 | 0.467 / 0.333 | 23.5 / −12.4 |
| 中心线v11路由 | 1.480 | 0.555 / 0.373 | 20.6 / −17.2 |
| 中心线v3路由 | 2.168 | 0.543 / 0.387 | 19.6 / −13.9 |
| gate5，κ=1.5（敏感性） | 1.5 | 0.252 / 0.184 | 13.7 / −6.3 |
| OSM 1A，κ=1.5（敏感性） | 1.5 | 0.485 / 0.340 | 20.3 / −12.9 |

- κ从1升到1.5，潮汐福利只降2–8%，所以1B与1A、gate5之间的差别主要来自路网，不是κ。
- 1B不加锚点车流下限（`fl0`）与加1%下限的结果逐位相同：修好κ以后，下限在1B上不起作用。此前1B的塌缩来自谱半径条件，不是近零锚点车流。下限保留为默认（对结果无影响，作为保护）。
- 1B完整A4（`stage2_endogenous_osm1b_ce_floor_b025_fl001_ks_od2025_v1`、`policy_comparison_osm1b_…_ks_od2025_v1`、`symmetric_input_reestimated_osm1b_…_ks_{geometric,arithmetic}_od2025_v1`），全部收敛。长期W̄：P3 0.35 ≈ P2 0.35 > P1 0.33 > P5 0.30 > P4 0.08（%）。对称输入使潮汐长期效应从0.33降到0.24（偏差−0.09至−0.10个百分点），与1A（−0.10）一致，gate5约为0。
- 选中走廊：1B与1A重合72/100，与gate5 41/100；1A与gate5 40/100。1B在κ=1.48与κ=1下的选边重合94/100（选边用基准车流，基准车流随κ略变）。
- 未重跑：BPR在1B与中心线路由上的运行（`network_form_matrix_v2` 中这三列仍作废）；OSM的分日选边（没有奇偶日bundle）。

## 2026-11-09 格对百度基准：换AK、样本扩为v3、启动采集

- 新AK单条测试 status 0（服务端应用）。日配额5000，样本由v2的300对无向扩为v3的696对无向（1392条有向），v2全部保留，每层补到116对（`build_grid_pair_baidu_manifest.py --n-undirected 700 --keep-from grid_pair_baidu_validation_v2`）。三个时段每天约4200次。
- 修bug：`query_baidu_direction.py` 把样本里空的途经点读成字符串"nan"，百度返回status 2。v2若直接跑会全部失败。冒烟测试 3/3 成功。
- 新增 `src/raw_baidu_validation/check_grid_pair_panel.py`：每时段结束后记录状态分布、成功率、用时中位数，以及与1B、gate5用时的对数相关；成功率<50%时写停止文件。`run_grid_pair_validation_week.sh` 增加 `GP_VERSION`、`FIRST_DAY_SKIP_FREE`。
- 2026-11-10 04:52（北京时间）后台启动，pid 42893，`caffeinate -is` 防休眠。冒烟会话 `FREE_*_smoke`、`FREE_*_smoke2` 不进入分析。

## 2026-11-09 BPR（b=1.65）在1B与中心线路由上按谱半径κ重跑

- 脚本同ks批次，`--congestion-form bpr --congestion-lambda 1.65 --xi-floor-rel 0.01 --kappa-rule spectral`，后缀 `bpr_b165_fl001_ks`。全部收敛，ρ在下限处=0.95。替代 `network_form_matrix_v2` 里作废的三列。
- 潮汐top100 W̄ 短期/长期（%）：1B +1.25/+0.63（κ 1.48），中心线v11 +1.34/+0.66（κ 1.48），中心线v3 +1.36/+0.70（κ 2.17）。对照1A +1.14/+0.57，gate5 +0.68/+0.37。BPR约为常数弹性的1.9–2.7倍，与gate5、1A上的倍数相同。
- 1B完整A4（BPR，长期W̄ %）：P3 0.64 ≈ P2 0.64 > P1潮汐 0.63 > P5 0.42 > P4 0.11。排序与常数弹性相同。
- 对称输入偏差（潮汐长期）：BPR −0.28（0.63→0.34，几何；算术−0.28），常数弹性 −0.09（0.33→0.24）。同走廊扩容偏差也相近（BPR −0.27，常数弹性 −0.09）。BPR下"忽略方向"的低估约为一半效果，常数弹性下约为三成。
- 输出：`tidal_lane_diagnostics_{osm1b,clv11routed,clv3routed}_bpr_b165_fl001_ks_od2025_v1`，`stage2_endogenous_osm1b_bpr_b165_fl001_ks_od2025_v1`，`policy_comparison_osm1b_bpr_b165_fl001_ks_od2025_v1`，`symmetric_input_reestimated_osm1b_bpr_b165_fl001_ks_{geometric,arithmetic}_od2025_v1`。

## 2026-11-09 1B分日选边检验（winner's curse）

- 1B奇偶日bundle：`build_osm_grid_travel_time.py` 新增 `--am-days`（只拆早高峰日期，晚高峰与畅通沿用全周，与gate5拆分口径相同），`build_osm_model_bundle.py` 新增 `--tt-source`。奇数日=0519/0521/0523，偶数日=0520/0522。输出 `osm_grid_travel_time_v2_am_{odd,even}_days`（只1B、只模型格网）、`model_input_osm1b_L300_gate5_am_{odd,even}_days_od2025_v1`：924格、2,871边（全周2,872），畅通用时与全周逐条相同，早高峰用时奇偶对数相关0.989。
- `run_selection_split_day_check_gate5.py` 新增 `--kappa-rule`，κ在全周bundle上定（1.483），两半沿用。
- 结果（`selection_split_day_check_osm1b_{ce_floor_b025,bpr_b165}_fl001_ks_od2025_v1`，基准全部收敛）：奇偶两半各自选的top100有88条相同，与全周选择各94条相同；两种函数选择相同。直接短期用时节省，用另一半选的走廊相对用本半选的：常数弹性低0.2%，BPR低1.0%。gate5为7.3%。1B上选边噪声带来的高估可以忽略。

## 2026-11-10 均衡唯一性的数值检查（进展备忘第6.4节）

- 背景：AA（2022）Proposition 1只覆盖纯幂函数拥堵；主线的下限形式与BPR没有唯一性定理。Almagro et al.（2026，附录F.5）的唯一性是假设单调性加数值检查，本次照此做。
- 脚本 `data_work/src/model/full_estimation/run_equilibrium_uniqueness_check.py`，四组：1B常数弹性、1B BPR、gate5常数弹性、1A常数弹性（都用下限1%、`--kappa-rule spectral`）。输出 `data_work/outputs/eq_uniqueness_{osm1b_ce_floor_b025,osm1b_bpr_b165,gate5_ce_floor_b025,osm1a_ce_floor_b025}_fl001_ks_v1/`。
- 多起点：基准、潮汐top100短期、长期，各从τ_obs、下限min(τ_ff, τ_obs)、2τ_obs和4个随机起点（τ_obs·exp(0.3ε)）求解，部分起点在每次外层迭代中都用随机份额起步。全部收敛，与生产解相比最大差：用时对数5.5e-5、居住份额对数3.1e-5、福利相对差1.9e-6，都在收敛容差量级。份额子系统从10个随机起点求解，与均匀起点差≤2e-9。
- 外层映射单调性：G(x) = 一次不阻尼的外层更新后的log τ。在基准均衡附近按0.02/0.1/0.3对数点随机扰动，每个尺度60对，计算 r = (G(x1)−G(x2))·(x1−x2)/|x1−x2|²。四组全部 r < 1（实际都为负，最大−0.18，即用时高的状态下一步用时低），所以G(x)−x在检查范围内严格递减，与唯一不动点一致。常数弹性下G的扰动放大倍数最大0.31–0.47（检查范围内是压缩映射）；BPR最大1.68，不是压缩映射，但单调性条件仍满足。
- 结论：两种函数在四个设定下都没有发现多重均衡的迹象。这是数值证据，不是证明；检查范围是基准附近±0.3对数点，没有覆盖全部状态空间。

## 2026-11-10 定价政策P6/P7改为把税加在感知成本上；福利口径仍不可用

- 发现：旧实现把税加在τ_ff上。anchored式是max(τ_ff + 税, τ_obs·dev^λ)，λ=1.1时P6的税（1.1τ）几乎总大于拥堵用时，感知成本被固定在τ_ff + 1.1τ_基准，不再随车流变化。旧主线里P6为负，至少部分来自这一点。
- 新实现（`equilibrium_solve.solve_equilibrium` 新参数 `toll_kl`、`toll_pigou`，`marginal_external_cost`；`stage2_counterfactuals.run_pricing_additive`；`run_policy_comparison_gate5.py --pricing-impl additive --pricing-only`）：感知成本 = τ_phys(Ξ) + 税。三种税：
  - P6：边际外部成本Ξ·∂τ/∂Ξ（常数弹性 β·τ，下限上为0；BPR b·(τ−f)），每次迭代按当前车流更新。下限折点处边际外部成本跳变，直接更新会振荡，改用逐次平均（步长1/(t+1)）后收敛；
  - P6b：固定为基准状态的边际外部成本；
  - P7：就业中心（90分位）相连边上每次2分钟。
  `--pricing-impl auto` 对anchored仍用旧实现，旧结果可复现。`realized_welfare_excl_toll` 新增 `xi_floor`。
- 运行（只跑P6/P7）：`pricing_additive_{osm1b_ce_floor_b025_fl001_ks,osm1b_bpr_b165_fl001_ks,gate5_ce_floor_b025_fl001,gate5_anchored_lam11}_od2025_v2`。常数弹性与BPR全部收敛；anchored λ=1.1的P6在500次内不收敛。（同名 `_v1` 为第一次不收敛的运行，已删除。）
- 长期"实际福利"（%）：1B常数弹性 P6 +1.40、P6b +1.57、P7 +0.46；1B BPR P6 +2.93、P6b +5.56、P7 +0.70；gate5 anchored P6b +23.0。
- **这些数字不能用。** `realized_welfare_excl_toll` 按物理用时重建A矩阵和路径加总，相当于假设人们按物理用时重新选路，只保留了收税后的居住与就业分布，漏掉了税对路径选择的扭曲成本，所以高估了收益。证据：固定在基准的税（P6b）都高于按均衡更新的庇古税（P6），anchored下达到+23%。按感知成本算的福利（不返还税）都为负（−0.8%至−3.9%），真实值在两者之间。要得到可用的定价结果，需要推导Fréchet路径选择下"按感知成本选路、支付物理用时、税收返还"的福利公式。[TODO]
- 车流和用时的反应（收敛解本身）可以用；只有福利评价有问题。

## 2026-11-10 定价下的福利公式（按含税成本选路、只承担物理用时）

- 推导：Fréchet在（居住地、工作地、路径）上联合选择时，被选中选项的最大感知效用V*的分布与选哪一项无关。被选路径的实际效用 = V* × (含税成本/物理成本)。逐边比值ρ_kl = (m̃_kl/m_kl)^(1/θ)沿路径相乘，其按选择概率的加总是一个矩阵逆：
  W_real = W̃^(1−θ) · Σ_ij u_i^θ w_j^θ [(I − B)^(−1)]_ij，B_kl = Ã_kl·ρ_kl，Ã_kl = 1/(κ·m̃_kl)。
  不收税时B = A，W_real = W（gate5上数值核对，差4e-16）。B逐元素不超过物理用时的A，谱半径条件自动满足。含义是每人支付的税原样退还给本人，行为仍按含税成本。代码 `stage2_counterfactuals.realized_welfare_route_choice`，`run_pricing_additive` 输出 `Wbar_pct_change_realized_route_choice`。
- 只对长期成立：推导要求V*是所有（i, j, r）上的无约束最大值；短期固定居住与就业份额，这一条不成立，短期数字（全为−0.1%至−0.5%）不解读。
- 运行 `pricing_additive_{osm1b_ce_floor_b025_fl001_ks,osm1b_bpr_b165_fl001_ks,gate5_ce_floor_b025_fl001,gate5_anchored_lam11}_od2025_v3`（v2同设定、无新福利列）。长期福利（%，新公式）：

| | P6 庇古税（均衡） | P6b 税固定在基准 | P7 就业中心2分钟 | 对照：P1潮汐 / P2同走廊扩建 |
|---|---|---|---|---|
| 1B 常数弹性 | +0.73 | +0.72 | +0.30 | +0.33 / +0.35 |
| 1B BPR | +1.53 | +1.50 | +0.53 | +0.63 / +0.64 |
| gate5 常数弹性 | +0.61 | +0.56 | +0.31 | +0.19 / +0.22 |
| gate5 anchored λ=1.1 | +2.98（未收敛） | +0.11 | +0.86 | +0.43 / +0.48 |

- 新公式下庇古税都不低于固定税，与理论一致（旧的realized口径相反，且anchored下给出+23%）。1B上一阶最优定价的长期福利约为潮汐车道的2.2倍（常数弹性）和2.4倍（BPR），就业中心收费与潮汐车道相近。税收约占车流总用时的24%（常数弹性）、37%（BPR）。
- 仍是近似：边际外部成本按分钟相加，没有按迭代后的均衡重新求最优税；收税后只退还给本人，不是一次性平均返还。

## 2026-11-10 含中途停留的通勤占比（平台0113E，nextstep C）

- 平台查询0113E（R人群、19个工作日、strict有效出行，从move_month独立构造，见Platform run-log 0113–0113E；0113读SubwayMobility链表失败，表已不在；0113B/C被防火墙拒绝；0113D时段错8小时，作废）。一天内到家即结束一个tour；家出发的tour里第一次到单位之前的段记为去程链，最后一次离开单位之后、以到家结束的段记为返程链；链内在非家非单位点停留<10分钟算换乘，≥10分钟算中途停留。
- 结果（小汽车，mode1）：

| | 直接家→单位 / 单位→家 | 含停留的通勤链 | 通勤合计 | 单位之间（白天在外） | 非通勤 |
|---|---|---|---|---|---|
| 早高峰07–09点出发，去程 | 35.6% | 10.0%（其中停留≥10分钟9.6%） | **45.6%** | 2.2% | 51.6% |
| 晚高峰17–19点出发，返程 | 25.4% | 13.4%（13.1%） | **38.7%** | 5.2% | 55.1% |

  非小汽车：早高峰去程通勤53.5%（含停留16.0%），晚高峰返程46.7%。
- QC：按端点purpose算的早高峰份额与0112逐项一致（HW 35.64%）。
- 结论：早高峰小汽车出行中通勤链占约46%，此前的35.6%低估约10个百分点；含停留的部分几乎都是真实停留（如送人、购物），不是换乘。按出行段计，不是按车公里计。对模型的含义：若Ξ只代表通勤车流，早高峰车流中约一半是背景车流；把通勤者口径从直接HW扩到含停留的链，模型通勤车流约放大1.28倍（45.6/35.6）。
- 输出 `Platform/runs/202505_v01/query-results/TL25M05_V01_0113E_commute_chain_share_r.csv`。

## 2026-11-10 格网串联用时对照路网直接用时；4邻接、8邻接、六边形；Almagro修正

- 问题：模型只给相邻格算边用时（路网最短路，3×3接入点平均），远距离OD由格网串联（AA路径加总）。检验串联结果是否接近在同一1B路网上直接求的OD用时。两边用同一路网，只检验"格网化"本身。验证时两种口径各自对齐（用户要求逻辑一致）：接入点口径（边=接入点平均，直接用时=起终格接入点平均，即模型口径）和中心点口径（边与直接用时都是中心点到中心点）。
- 脚本：`data_work/src/network/check_grid_chain_vs_road_od.py`（首轮，4邻接）、`check_grid_topology_vs_road_od.py`（拓扑对照）。κ按主线规则（下限用时、ρ≤0.95，两时段共用；4邻接复现主线1.4827）。首轮输出 `grid_chain_vs_road_od_osm1b_v1` 的早高峰减自由流一项按时段各取κ，以 `grid_topology_vs_road_od_osm1b_sq4_v2` 为准。
- 8邻接：`build_osm_grid_travel_time.py` 新增 `--neighbours 8`（默认4不变）→ `osm_grid_travel_time_v2_nb8`（只1B、模型格网）→ bundle `model_input_osm1b_L300_nb8_gate5_od2025_v2`（938格、5,750边，其中对角2,853；4邻接边用时与车道逐条相同；保留通勤者98.0%，4邻接97.1%）。六边形：面积与3 km方格相同（3857坐标），格心落在模型方格内的六边形，6邻接，规则同bundle（接入点7个：中心加6个0.55R点），819格、3,778边；只做用时验证，没有六边形OD（需平台重新汇总）。
- 结果（早高峰，接入点口径／中心点口径）：

| | 4邻接 | 8邻接 | 六边形 |
|---|---|---|---|
| 串联/直接，通勤加权中位 | 1.50 / 1.34 | 1.22 / 1.09 | 1.30 / 1.19 |
| 0–5 km | 1.17 / 1.00 | 1.00 / 1.00 | 1.12 / 1.00 |
| 10–20 km | 1.73 / 1.51 | 1.40 / 1.25 | 1.51 / 1.38 |
| 去固定效应后相关（模型成本） | 0.98 / 0.98 | 0.98 / 0.98 | 0.98 / 0.98 |
| 单时段方向差相关（模型） | 0.45 / 0.43 | 0.64 / 0.63 | 0.43 / 0.38 |
| 早减自由流方向差相关（模型） | 0.69 / 0.64 | 0.73 / 0.71 | 0.69 / 0.69 |
| κ | 1.48 / 2.07 | 1.73 / 2.26 | 1.59 / 1.78 |

  接入点重复计入使比值高0.11–0.16（两口径之差）；其余来自格网几何，8邻接去掉一半以上（中心点口径从1.34降到1.09）。
- Almagro et al.（2026，NBER w32185修订版）做法，核对原文：芝加哥77个社区区为节点，有向边连相邻社区区，路径固定为谷歌地图给出的路径（重新选路对反事实平均用时影响<0.1%，脚注22）；OD车内用时 = ψ_mj × 路径上各边用时之和（式14）；附录F.2：边模型对短行程拟合好、高估长行程（长行程更多走高速），用泊松回归 E[T_google | d, T_edges] = T_edges·exp(α_j + β_j log d)，T_edges作offset，d为直线距离，按方式分别估，模拟时乘ψ = exp(α̂ + β̂ log d)。他们的模型是加法用时、路径固定，ψ可直接乘在OD上；AA式路径加总没有OD层面的位置，这里只用于验证。我们的拟合（通勤加权、40 km内）：4邻接接入点口径 α=−0.04、β=−0.22（ψ在3/10/30 km为0.75/0.58/0.45）；8邻接 α=0.15、β=−0.21（0.92/0.71/0.56）。修正后中位比值0.93–0.99。
- 8邻接上重跑潮汐top100（`tidal_lane_diagnostics_osm1b_nb8_{ce_floor_b025,bpr_b165}_fl001_ks_od2025_v1`，全部收敛）：常数弹性 W̄ ST/LT +0.25%/+0.18%（4邻接 +0.47/+0.33），BPR +0.74/+0.37（4邻接 +1.25/+0.63）；每人短期省时19.1秒（4邻接23.5）。选中边68%为对角边，慢方向基准车流中位2,117（4邻接3,390）。
- 原因之一：格边不再与道路一一对应。按中心点早高峰路径统计，每公里道路平均出现在1.88条格边的路径上（4邻接），8邻接为3.97；8邻接下一条格边路径的中位重合比例为100%。在一条格边上加车道，只改变分到这条格边的车流，同一路段在其他格边上的部分不变。所以两种拓扑的"top100条边"不是同一政策，福利不能直接比。
- 结论：用时验证支持8邻接（水平和方向都更好）；政策与拥堵若用8邻接，需改成按实际道路定义车道和车流（路段加车道→所有经过该路段的格边按份额变容量，拥堵按路段车流算），属模型改动，待用户决定。

## 2026-11-10 按道路定义潮汐车道与拥堵（4邻接、8邻接）

- 全部方案、做法与结果见 `Documents/L2_notes/grid_topology_and_road_level_policy_20261110.md`。
- 实现：`equilibrium_solve.RoadLayer` 与 `solve_equilibrium(..., road=...)`（不传时结果不变；定价与道路层同用时报错）；道路层 `src/model/road_level/build_road_incidence.py`（格边中心点早高峰路径上的OSM路段及其对向车道，路径长度的77%有对向车道）；运行 `src/model/road_level/run_tidal_road_level.py`。恒等检验（D = H = I）与主线逐位相同。
- 结果（常数弹性，W̄ 短期/长期 %）：处理300 km时4邻接0.163/0.119，8邻接0.168/0.120；BPR 0.259/0.150与0.283/0.152。按道路定义后拓扑依赖基本消失。
- 分解（4邻接，主线top100选边）：主线0.467/0.333；只改按道路算拥堵0.389/0.282；要求同一条路的对向车道减车道（真实潮汐）0.231/0.170。主线约一半来自格边定义（没有对向车道也加车道、快方向减的不是同一条路、重复计算）。
- 选线：v1按路段速度（偏向格心附近，弃用），v2按格边延误分到路段；v2仍不如"主线选边加真实配对"（300 km 0.163 vs 274 km 0.231），需改进。
- 作废输出：`tidal_road_level_identity_check_osm1b_ce_floor_v1`（用了默认α=−0.12），`grid_chain_vs_road_od_osm1b_v1` 的早减自由流项（两时段κ不同）。

## 2026-11-10 按道路定义的一阶条件选线；4、8邻接交叉检验；主线不改

- 决定（用户）：主线继续按格边定义潮汐车道与拥堵（与AA、Bordeu、Fajgelbaum-Schaal一致）；按道路定义的结果只作稳健性。
- 选线v3/v4（`run_tidal_road_level.py --score foc`）：走廊得分为固定基准流量下的一阶福利 Σ_e Ξ_e[ln τ_e(现) − ln τ_e(加该走廊)]，τ按道路公式精确计算，懒惰贪心逐个重算。v4另把可选走廊的对向车道门槛从80%降为0（`--opp-km-min 0`），只处理路径中有对向车道的部分。一阶指标与实际短期福利之比在同一设定内不变（4邻接常数弹性0.184–0.186）。
- 结果（300 km，km300pk，W̄短期/长期%）：v4 4邻接 常数弹性0.342/0.245、BPR 0.572/0.317；8邻接 0.294/0.215、0.525/0.290。v2同口径为0.163/0.119与0.168/0.120。主线（按格边top100）0.467/0.333、BPR 1.254/0.626。
- 更正前一节两条结论：按道路定义的福利约为主线的73%（常数弹性），不是一半；8邻接比4邻接低8–14%，拓扑依赖没有消失。前一节的"几乎相同"来自弱选线。
- 交叉检验（`--apply-treated`）：同样的实际道路，4邻接模型的福利总是更高（4邻接选出的路：0.342对0.226；8邻接选出的路：0.335对0.294）。经过这些慢方向路段的车流按时间加权占比，4邻接模型为29%，8邻接模型为20%。差异来自格网路径分配，不来自选线。
- 输出：`tidal_road_level_osm1b{,_nb8}_{ce_floor_b025,bpr_b165}_fl001_ks_{v3,v4_opp0,v5_opp0}`（v5=v4，多存 `treated_units.parquet`）；`tidal_road_level_cross_sel{4_in_nb8,8_in_nb4}_{ce_floor,bpr}_v5`。记录：`Documents/L2_notes/grid_topology_and_road_level_policy_20261110.md`。

## 2026-11-10 弹性敏感性（1B）、按道路定义用路段层面弹性；结果与口径汇总

- 弹性敏感性（`run_tidal_lane_diagnostics_gate5.py --only-top100 --kappa-rule spectral --xi-floor-rel 0.01`，1B，κ=1.48，全部收敛），潮汐top100 W̄短期/长期（%）：ce_floor β=0.08 0.144/0.129，0.16 0.294/0.234，0.25 0.467/0.333，0.42 0.808/0.478；anchored λ=1.1 2.370/0.818。短期近似与β成正比，长期反应较小（长期/短期从0.90降到0.35）。输出 `tidal_lane_diagnostics_osm1b_{ce_floor_b008,ce_floor_b016,ce_floor_b042,anchored_lam11}_fl001_ks_od2025_v1`。注意：这些目录的 `parameters.json` 里 `lambda_phys` 仍写1.1（默认值），实际弹性以 `--congestion-lambda` 为准，见 run.log 与目录名。
- 按道路定义改用路段层面弹性（道路单元=路段，β=0.42、BPR b=2.19；v4选线，km300pk）：4邻接 0.560/0.342、BPR 0.679/0.335；8邻接 0.489/0.305、0.614/0.306。长期与主线（0.333）几乎相同。输出 `tidal_road_level_osm1b{,_nb8}_{ce_floor_b042,bpr_b219}_fl001_ks_v5_opp0`。
- 结果与口径汇总：`Documents/L2_notes/results_update_and_specifications_20261110.md`（主结果、旧到新主线的逐步变化、口径分类与待做、弹性详细说明）。指出背景车流口径未定：弹性对全部车流估计，模型Ξ只含通勤者；若背景车流固定，有效弹性约为β的0.4–0.5倍。

## 2026-11-11 OD口径与时段版本；通勤链；全目的端点OD与背景车流检验

- 平台（`Platform/202505_v01/07_od_extensions/`，记录见 `Platform/runs/202505_v01/run-log.md`）：0701分时段HW/WH OD（全日、出发06–10/07–09、在途06–10/07–09，晚高峰对应）；0702通勤链OD（直接、只换乘、含活动停留；mode1/mixed/other）；0703全部用户、全部目的的小汽车出行端点OD（出发06–10/16–20，按stay_poi落格）。导出0711–0716、0741–0748、0751–0768全部通过，每格≥5人。
- 平台结果大小限制：约11.5万行×12列可过，约19万行失败（st=1004，提示"字段类型不支持二进制或字段名有重复"，实为结果过大）；按 `pmod(hash(grid_o),k)` 拆分即可。
- 时段：主口径改为出发06–10（晚16–20），07–09作稳健性（用户决定，不抹平高峰）。06–10含HW出发的79.7%（07–09为55.7%）。06–10的格边用时按通勤出发人数加权各小时（`build_osm_grid_travel_time.py --hour-weights commute`），中位比07–09低1.2%。
- 更正：含停留的通勤链按链计只放大约1.10倍（含mixed 1.17倍），不是1.28；1.28是按出行段计（每链约2.3段）。
- 潮汐top100（1B，ce_floor 0.25，κ=1.483，W̄短期/长期%）：主线0.467/0.333；O1出发06–10 OD+06–10用时0.484/0.344；在途06–10 0.486/0.346；全日OD 0.441/0.314；链06–10 0.481/0.341；链含mixed 0.521/0.367；链07–09 0.471/0.335。选边与主线重合182–200/200。输出 `tidal_lane_diagnostics_osm1b_ce_floor_b025_fl001_ks_od2025_<tag>_v2`（`_v1` 同名目录只有失败的run.log，作废）。
- 背景车流检验（`src/validation/check_proportional_background.py`，输出 `outputs/proportional_background_check_osm1b_dep0610_v1/`）：把模型通勤OD、端点HW OD、全部目的OD按模型路径核在基准用时下分到格边。总量上大致成比例（log车流相关0.965；按通勤车流加权，62%落在总量/通勤比偏离均值±25%以内）。方向上不成比例：总车流的方向不对称约为模型通勤车流的一半（平均|ln X_kl/X_lk| 0.45对0.83，斜率0.51）。top100走廊快/慢方向车流比，通勤0.29，全部车流0.48；通勤占比慢方向39%、快方向26%，全网32%。结论：假设A高估潮汐收益，快方向的背景车流被低估。

## 2026-11-11 主口径改为全日OD＋07–09点用时

- 决定（用户）：用时取07–09点（静态模型要求时段内路况稳定，潮汐车道只在高峰开放，06–10会抹平高峰）；OD用不限时段的全日HW，避免丢样本，出行时刻留给以后的动态模型。同一前一节06–10主口径的决定作废。
- 结果（1B，ce_floor 0.25，κ=1.483，top100）：W̄ 0.441/0.313%，每人短期省21.6秒、长期多12.7秒；选边与原主线（07–09出发OD）重合188/200。bundle `model_input_osm1b_L300_gate5_od2025_v2_alldaytt0709`，输出 `tidal_lane_diagnostics_osm1b_ce_floor_b025_fl001_ks_od2025_alldaytt0709_v3`（同名 `_v2` 只有失败的run.log：`--scheme` 应为 `osm1b`，作废）。

## 2026-11-11 格对百度外部基准：前两天数据的初步对照（中期结果，11-16数据齐后正式重做）

- 数据：`grid_pair_baidu_validation_v3`，1392条有向相邻格对，第1–2天AM×2、PM×2、FREE×1，全部成功。百度自身可靠：两天间用时对数相关0.97（中位差6%），方向比相关0.88，潮汐变化（AM方向比−PM方向比）相关0.53（两日平均后上限约0.8）。
- 按模型现行口径（1B格内3×3多点平均）：早高峰用时与百度相关0.30（gate5 0.50，中心线v11 0.45），模型/百度中位0.80；自由流时方向比几乎无关（1B约0）；拥堵倍数AM/FF相关0.54、潮汐变化相关0.50（斜率0.46），这两项1B优于gate5。
- 原因在起讫点，不在速度：1B格心路径长度与百度距离相关仅0.15。偏长（>1.5倍，223对）一半在换成1A后恢复，来自1B缺路；偏短（<0.67倍，350对）路径短于格心直线距离，来自接入节点偏离格心，百度在这些格对反而绕行（距离为直线1.88倍）。
- 统一起讫规则（模型从离百度查询点最近的节点出发，按两端接入距离筛选）：1B接入距离≤150 m的102条，长度比0.99，AM用时相关0.79、方向比0.66、潮汐变化0.74（斜率0.51）、拥堵倍数0.68；≤300 m时0.63/0.36/0.63/0.56，相关随接入距离单调上升。1A同条件下较差（≤150 m用时0.64、潮汐0.61）。结论：1B路网速度可信，偏差主要来自接入。
- 遗留问题：(1) 1B接入距离中位517 m，仅约7%格对两端≤150 m；(2) 起讫点对齐后模型用时仍为百度的0.73倍，可能是路段速度不含路口、信号和转向延误；(3) 潮汐变化幅度：1B平均|AM方向比−PM方向比|为0.129，百度0.151，1B略小约15%；百度对1B的回归斜率0.46主要反映1B一侧的噪音（衰减），不表示幅度只有一半（同日更正）。
- 补充gate5（edgeproj）晚高峰后的对照：gate5用时水平最好（AM 0.50、PM 0.51），潮汐变化相关0.47与1B（0.50）相近，但幅度0.208，比百度高约38%，斜率0.26，噪音更大；拥堵倍数相关0.38，低于1B的0.54。
- 临时脚本（scratchpad，11-16后整理进 `src/raw_baidu_validation/`）：gp_interim.py、gp_len.py、gp_same_od.py、gp_same_od_cmp.py。
- 临时脚本补充：gp_rank.py（含gate5晚高峰）。

## 2026-11-11 1B格心接入方式试验（前两天百度数据，从百度查询点出发；中期结果）

- 基准（最近节点，无接入段）：AM用时相关0.34，模型/百度中位0.69，方向比AM 0.16、FF 0.09，潮汐变化0.48，拥堵倍数0.46。
- 试验1，直线接入段（查询点连最近20个节点、2 km内，绕行系数1.3，接入速度10–30 km/h）：不改善。接入速度越低，用时相关越差（10 km/h时0.21）；方向比AM升到0.21；潮汐幅度被稀释（0.10，对百度0.15）。
- 试验2，把OSM支路（residential、unclassified、living_street，固定速度）加进1B：15 km/h时用时水平对齐（模型/百度0.98），方向比AM 0.26、FF 0.20，拥堵倍数0.52，潮汐变化0.51；但用时水平相关不升（0.31）。加service道路无额外改善。加支路后强连通分量只保留67.7%的路网公里，碎片多，需检查节点连接。
- 判断：剩下的水平差异主要是单个查询点的特殊情况（小区内部、快速路无出口等），格心单点作为验证目标噪音大；模型实际用格内3×3多点平均，单点验证低估其质量。
- 下一步候选：(1) 支路只用于两端接入（前后约1 km），中段仍走有观测的路，保留拥堵信号；(2) 验证改为多点：子样本每格3点或从模型接入节点查询百度。
- 临时脚本：gp_access.py、gp_minor.py、gp_access_cmp.py（scratchpad）。

## 2026-11-11 1B支路接入（只用于两端）：实现、验证与潮汐重跑

- 实现：`src/network/minor_access.py`（查询点先直线吸附到最近节点，再只走OSM支路residential/unclassified/living_street，到达主路网节点，限时6分钟，固定接入速度；够不到主路时直线×1.3接最近主路节点）；`build_osm_grid_travel_time.py --access minor --access-speed --access-max-min`（tm_*改为两格3×3格点之间的平均，默认 `--access nearest` 不变）。
- 百度单点验证（从查询点出发，20 km/h、6分钟）：用时水平对齐（模型/百度1.06，原0.69），方向比AM 0.22（原0.16），拥堵倍数0.53（原0.46），潮汐变化0.51（原0.48）；用时水平相关0.31（原0.34）。
- 格内3×3格点版（模型实际用的tm）对百度：用时相关0.20（原0.30），模型/百度1.30（原0.80），方向比AM 0.20（原0.11），拥堵倍数相关0.60（原0.55），潮汐变化相关0.51（原0.50）；但潮汐幅度0.089（原0.129，百度0.151）、拥堵倍数中位1.078（原1.140，百度1.210），接入段（固定速度、无拥堵）稀释了拥堵与方向差异。
- 潮汐top100（全日OD＋07–09用时，ce_floor 0.25）：W̄ 0.433/0.302%（主口径0.441/0.313），基准人均用时17.6分钟（主口径13.5），κ谱半径规则下为1.0（主口径1.48），选边重合184/200。输出 `osm_grid_travel_time_v2_acc_minor_v20`、`model_input_osm1b_L300_gate5_od2025_v2_alldaytt0709_accminor20`、`tidal_lane_diagnostics_osm1b_ce_floor_b025_fl001_ks_od2025_alldaytt0709_accminor20_v1`。
- 判断：接入方式对潮汐福利影响很小（−2%/−4%）；改进了用时水平和方向结构，但稀释拥堵幅度，且单点验证无法判断格内平均的好坏。主口径暂不换，作为稳健性；是否采用待多点验证。

## 2026-11-11 模型OD距离衰减过陡：基准人均用时偏低的来源与θ的文献做法

- 结论：主口径基准人均用时13.5分钟偏低，主因是模型均衡OD的通勤距离远短于观测，不是路网用时。观测OD（commuters_road）乘同一套均衡用时，人均为38.4分钟（期望路径）或34.6分钟（最短路）。
- 口径：`avg_physical_time_base_min`＝Ξ·m/(L̄κ)，即模型均衡OD份额×期望路径分钟（复算13.6分钟）。基准反演只匹配居住与就业边际，不匹配双边OD。
- 模型OD与观测OD（主口径bundle `model_input_osm1b_L300_gate5_od2025_v2_alldaytt0709`，一格约3 km）：同格份额46%对17%；平均格距0.93对2.54；3格以上份额5.8%对30%。同格出行的路径用时约0分钟。
- 引力半弹性（PPML，起终点固定效应，非同格对，最短路分钟）：观测OD 0.050/分钟，模型OD 0.233/分钟；对数弹性2.03对4.06。模型的距离衰减约陡4.7倍。来源是 A_kl=1/(κ m_kl)：路径成本是逐边相乘，每多走一格（约8分钟）权重乘约1/12。
- 文献核对（原文PDF）：ARSW (2015) 用 d_ij=exp(κτ_ij)，τ为分钟，ν=εκ≈0.07（Table III，0.0697–0.0771），ε=6.83。AA (2022) 取θ=6.83（来自ARSW），链路成本 t_kl=(时间)^{δ0}，主结果设δ0=1/θ，即每条链路时间弹性为−1，理由是引力文献距离弹性为−1；稳健性有θ=4和δ0=1.6/θ（复现代码 asym_counterfactual_berlin_t4.m、_d016.m）。Tsivanidis (2023/8 稿) 用 d_ij=exp(κt_ij)，κ=0.011/分钟（众数选择），引力PPML得 θκ=0.039/分钟、θ=3.40。Almagro et al. 没有居住和就业选址，无对应θ。
- 含义：本项目的θ=6.83与δ0=1/θ一并沿用AA；OD距离衰减实际由"每边时间弹性−1、逐边相乘"决定，而非θ本身。人均节省秒数与W̄以偏短的模型OD为权重，长距离通勤被低估。补救措施列入nextstep D7。

## 2026-11-11 OD距离衰减补救：δ0扫描无效，改为嵌套Fréchet η并重跑top100

- 结论：只改路段时间弹性δ0（γ=θδ0）对不上北京OD，要γ≈0.05才接近，此时路径选择几乎不看时间。把路径与选址的离散度分开（τ^{-θ}=R^η，η=θ/ρ_route，γ保持1）后，η=0.21可对上观测半弹性。重跑top100：每人省时增加，长期由负转正；W̄降到约1/5，原因是每分钟的效用价值按η缩小。
- γ扫描（`src/model/full_estimation/run_link_time_gamma_scan.py`，输出 `link_time_gamma_scan_osm1b_alldaytt0709_v1`，κ按谱规则随γ变）：半弹性 γ=1 0.228，0.5 0.118，0.2 0.076，0.1 0.064，0.05 0.058（观测0.050）。ρ目标放宽到0.99/0.999几乎不变（γ=1 0.224/0.223，`…_rho0p99_v1`、`…_rho0p999_v1`）。原因：Leontief核收敛要求每边权重远小于1，跳数衰减由此决定。
- η扫描（同脚本`--etas`，`link_time_gamma_eta_scan_osm1b_alldaytt0709_v1`、`…_fine_v1`）：η=0.21 半弹性0.051、平均最短路37.0分钟、格距2.87、同格8.7%；η=0.23 0.055/34.4/2.67/9.9%（观测0.050/34.6/2.54/16.9%）。同格份额偏低，可由格内用时（D7(c)）再调。
- 代码：`Parameters.route_eta`（默认1）；`equilibrium_solve._resolvent_and_tau`、`_route_traffic`（η≠1时Ξ=a_kl[R'GR']_kl，G=L/R）；`invert_fundamentals`、`stage2_counterfactuals.realized_welfare`同步；带路径扭曲的定价福利在η≠1时报NotImplemented；`tidal_lane_diagnostics.expected_additive_route_minutes`用R=τ^{-θ/η}；runner加`--route-eta`。η=1复现主线逐位一致（`…_alldaytt0709_eta1check_v1`，数组与汇总差0，选边相同）；η=1下新车流公式与旧式之差为市场准入收敛误差（400次1.2e-3，5000次3.6e-6）。
- top100（1B，ce_floor 0.25，κ=1.483，全部收敛）：

  | | 主线η=1 | η=0.21 | η=0.23 |
  |---|---|---|---|
  | W̄ ST/LT % | 0.441/0.313 | 0.085/0.072 | 0.096/0.081 |
  | 模型OD基准人均分钟 | 13.5 | 41.1 | 38.2 |
  | 模型OD权重省时 ST/LT 秒 | 21.6/−12.7 | 32.7/14.6 | 32.5/13.4 |
  | 观测OD权重省时 ST/LT 秒 | 28.3/18.3 | 31.2/27.8 | 31.2/27.5 |
  | 选边与主线重合 | | 85/100 | 85/100 |

  输出 `tidal_lane_diagnostics_osm1b_ce_floor_b025_fl001_ks_od2025_alldaytt0709_eta0p21_v1`、`…_eta0p23_v1`。
- 福利口径：每省一分钟对应的W̄变化，主线约1.22%，η=0.21约0.16%；以北京半弹性与θ换算（ν/θ=0.05/6.83）约0.73%。截面衰减含跳数衰减，边际时间敏感度只有约一半，所以η标定后每分钟价值偏低，主线偏高，两者夹住基准。分钟数结论比W̄稳。

## 2026-11-13 进度诊断；nextstep、quickstart、文档索引更新

- `nextstep.md` 从190行精简为当前状态、六步顺序、进行中任务和开放问题；精简前的完整版本存 `Documents/L2_notes/_superseded/nextstep_snapshot_20261113.md`（6–9月各线状态、6-29政策套件计划、旧开放问题都在其中）。
- `agent_quickstart.md` 的"当前工作焦点"由 raw_edgeproj_v2 路网构造改为新主口径与当前瓶颈。
- `DOCUMENT_INDEX.md`"Where the current numbers live"分为当前主线与旧gate5主线两张表，补入11月的五份备忘。
- 格对百度对照的临时脚本（`gp_*.py`，10个）从另一会话的 `/private/tmp` 复制到 `data_work/src/raw_baidu_validation/interim_20261111/`，11-16后整理。
- 百度格对采集：第1–4天12个时段全部1392/1392，剩11-16一天。
- 诊断要点：方向与政策排序稳，福利水平不稳（长期0.07%–0.63%）；OD距离衰减（η）应先于背景车流处理，背景车流方向检验需在η定后重做；底网不必等11-16才推进；Git自6-29后未提交。

## 2026-11-13 政策比较与定价在主口径（全日OD＋07–09用时）下重跑

- 结论：长期福利排序不变，P6 0.67 > P3 0.33 ≈ P2 0.33 > P1 0.31 ≈ P7 0.30 > P5 0.28 > P4 0.07（W̄，%），各政策幅度比旧口径低约5–10%。P1复现潮汐主口径0.441/0.313。全部均衡收敛，κ=1.483，谱半径最大0.949。
- 短期到长期，扩建类政策福利都缩到约0.71–0.76倍。人均用时从短期节省（P1为21.6秒）变为长期增加（12.7秒），主要来自sorting项（+28秒）。定价的长期省时最多（P6为231秒/人）。
- 运行：新包装脚本 `src/model/full_estimation/run_policy_comparison_alldaytt0709.py`，原样调用 `run_policies`，另存谱半径、物理用时和直接/GE/sorting分解。参数为 ce_floor β=0.25、下限1%、`--kappa-rule spectral`、score_bpr_st，P6/P7用additive实现和v3福利公式。
- 输出：`policy_comparison_osm1b_ce_floor_b025_fl001_ks_od2025_alldaytt0709_v1`（含P6/P7，与定价目录逐位相同）、`pricing_additive_osm1b_ce_floor_b025_fl001_ks_od2025_alldaytt0709_v1`，汇总表 `summary_direct_vs_longrun.csv`。

## 2026-11-13 θ敏感性与一阶/二阶分解（1B主口径）

- 结论：短期福利几乎都来自一阶项（FO/ST=0.98，各θ相同）；长期为一阶预测的0.72，差额来自sorting。θ只按1/θ缩放福利（δ0=1/θ使A与θ无关），省时分钟、各比例、模型OD和选边都不随θ变。
- 做法：`run_theta_first_second_order_osm1b.py`，θ∈{3.4,4,5,6.83,8,10}，每个θ重新反演；ce_floor 0.25；κ=1.483；固定主线top100（各θ下重选重合100/100）。一阶项ΔlnW̄₁=−Σs_e·dln m_e/θ，dln m为固定基准Ξ下改车道后的用时变化；η=1时s_e等于每人经过次数。一阶与完整解之差拆为冲击规模（固定车流、允许改路的精确解减一阶）、拥堵外部性（短期减前者）、sorting（长期减短期）。
- 结果：W̄短期/长期，θ=3.4为0.887/0.633，θ=6.83为0.441/0.313（复现），θ=10为0.301/0.211。θ=6.83下冲击规模+0.028、拥堵外部性−0.021、sorting −0.128（百分点）。η=0.21：FO/ST=0.90，FO/LT=1.07，长期/短期0.84。每省一分钟的W̄：θ=3.4为2.46%，6.83为1.22%，10为0.83%，η=0.21为0.16%。
- 输出：`theta_first_second_order_osm1b_alldaytt0709_v1`。

## 2026-11-13 模型代码只读审计（子agent执行，四处代码位置已人工核对）

- 结论：没有发现改变主结果符号或排序的错误；结果文档§1.1、§1.2、§4.6与WORK_LOG 11-11 η表的数字和输出文件一致。以下问题**尚未修改**。
- B1 `tidal_lane_diagnostics.py:89` 期望路径分钟对R≤1e-12的OD格返回0；η=0.21时 `od_route_matrix_avg_time_base_min` 报37.9，应为41.1。WORK_LOG中的41.1与观测OD权重数字另行计算，不受影响。
- B2 `equilibrium_solve.py:226`、`invert_fundamentals.py:149` η=1时R按1e-12截断；审计称1B主口径下81.4%的OD格绑定、带1.8%虚假流量，潮汐短期W̄被低估约2%（0.441→约0.449），基准人均用时13.47→约13.58分钟（比例未复跑）。
- B3 `stage2_counterfactuals.py:793` 收费收入用Ξ×toll未除κ，`toll_revenue_per_day` 与 `lump_sum_rebate_per_worker` 在1B上高估1.48倍；不进福利。
- B4 `run_policy_comparison_gate5.py` 定价的默认 `Wbar_pct_change_realized` 列仍是作废公式（P6 +1.40%），正确值在 `…_route_choice` 列。
- 政策比较、对称重估、分日选边、唯一性检查、按道路定义等脚本不读 `route_eta`，会静默按η=1运行。
- 可辩护但需说明：短期W̄在份额固定下仍用Fréchet福利公式；P6收费按1/(it+1)更新，收敛判据偏松，未报告 max|toll−MEC|/MEC。
- 口径：选边重合"188/200"为有向边（94/100走廊），"85/100"为走廊，今后统一写走廊数。

## 2026-11-13 Brad Ross讨论准备

- 备忘 `Documents/L2_notes/brad_meeting_prep_20261113.md`：四个议题（拥堵技术、一阶与二阶、政策直接与长期效应、动态与出发时刻），每个含我方发现、他的做法、问题与可行思路；末尾列出引用前需核实的条目。
- 已确认其文章：Ater, Ross, Shany, Vasserman, "Road Pricing under Heterogeneity"（NBER w35887，2026-10）；Kashner & Ross, "Measuring and Mitigating Traffic Externalities"。此前会议记录中的[TODO: 确认文章题名]可据此更新。
- 换算：速度对密度弹性ε对应用时对车流弹性ε/(1−ε)；β_c=0.25即ε=0.20，ε=1.1落在通过量下降的一支。

## 2026-11-13 拥堵估计与数据构造只读审计（子agent执行；T0定义、背景检验默认口径、主估计无恒值筛选三处已人工核对代码）

- 结论：β_c对固定的凌晨下限T0很敏感，这是目前最大的估计问题。以下均**尚未修改或重跑**。
- 下限敏感性：`run_congestion_almagro_spec.py` 中T0取00–05点平均配速、不估计（`NIGHT=(0,4)`）。审计的数值检查：ln T0加0.03时β_c为0.370，减0.03时为0.199（基准0.245）；处在下限上的观测贡献一半矩条件。该幅度大于现报区间的格边部分，且主估计无标准误。模型里的下限f=min(τ_ff,τ_obs)用02–05点路径用时，与估计的T0口径不同。（±3%的数值未复跑。）
- 恒值填充（约20%）只在线性IV稳健性里剔除过，带下限的主估计与模型的格对用时都没有剔除；主估计可能偏低，T0也可能等于填充值。
- 估计边与模型边不一致：β_c在gate5边估计，用于1B边；审计称1B的2,872条边中761条（26%）不在估计样本，用时口径也不同（走廊配速指数对最短路用时）。
- 测量误差：误差在max内，IV修正不了；z与q同源时模拟显示带下限估计偏低，独立时偏高。格边层面泊松噪声小（ln q标准差约0.07），所以"测量误差解释IV约为OLS两倍"的说法不成立；路段层面0.42可能偏高。
- 背景车流检验（`check_proportional_background.py`）默认用dep0610数据包与选边，不是现主口径，需在全日OD＋07–09用时下重跑。其"通勤"列用的是观测通勤OD，与全部目的OD可比；模型均衡车流Ξ的方向比尚未检验。
- 没有问题的项：矩条件用全样本，无按因变量选样；固定效应与文档一致；z不含该边实测速度或车流；无重复计数；路段方向一致99.9%；BPR局部弹性公式正确；OD同比例放大不改变结果的说法成立；只用工作日时β_c为0.234。
- 建议（未做）：ln T0加共同平移参数一起估或报告β随平移的曲线；先剔恒值再算T0；聚类bootstrap每次重算T0；在1B边上用1B分小时用时重估。

### 2026-11-13 拥堵弹性对畅通下限的敏感性（子agent执行，`congestion_floor_sensitivity_v1`）

- 脚本 `data_work/src/model/full_estimation/run_congestion_floor_sensitivity.py`，输出 `data_work/outputs/congestion_floor_sensitivity_v1/`（含中文README）。未改已有文件，未重跑模型反事实。
- 复现：基准0.245（边）、0.420（路段）逐位复现。基准出自 `congestion_iv_followup_v1` 的 `floor_NIV_z_dateXhourFE`（06–22点，F为观测车流，工具为ln z），不在 `congestion_almagro_spec_v1`。下限上的观测占28.8%，不是"约一半"。
- 下限平移：审计数字成立（ln T0 +0.03 → 0.370，−0.03 → 0.199）；+0.05为0.564，+0.08为1.243。路段更敏感（±0.03为0.282 / 0.733）。
- 替代下限（边）：02–05均值0.238，00–05中位数0.240，00–05第10百分位0.193，全天第10百分位0.206。剔恒值填充后基准升到0.296（由路段重建边配速，重建本身0.252；恒值占路段时段17.6%）。
- 联合估计下限：共同平移量δ为0.026（IV矩，bootstrap标准差0.005）和0.03（残差平方和）。数据把下限放在凌晨均值之上约3%，但β定不住：同一位置IV版0.347（bootstrap标准差0.061，区间0.26–0.48），残差平方和版0.199。每条边各自的下限识别不出。
- 不含下限的高负荷线性IV（边FE＋日期×小时FE，06–22点；边 / 路段）：全部小时0.161 / 0.176；z>p50 0.289 / 0.268；z>p70 0.346 / 0.326；z>p85 0.445 / 0.416；07–09点0.421 / 0.355；17–19点0.337 / 0.345；早晚高峰合并0.359 / 0.353。第一阶段F均在480以上。全市负荷工具在晚高峰为负，不可用。
- 标准误（边，100次聚类bootstrap，主会话用重抽文件复算一致）：基准0.019（区间0.213–0.287）。按日期重抽凌晨数据重算T0后几乎不变，T0的问题是定义偏差，不是抽样误差。
- 形状：分箱后是平滑变弯，无明显折点；最低负荷箱的配速中位数仍比凌晨均值高3.5%。
- 文献核对（本地PDF）：Almagro et al. 也把每条边凌晨0–5点平均用时直接作T0，不估计。
- 判断（未锁定，待用户决定）：0.245依赖"凌晨均值即白天畅通值"的假设，数据不支持。局部弹性随负荷门槛单调上升（0.29→0.35→0.44），曲线是凸的，没有单一弹性；高峰附近约0.35–0.40。0.35超过唯一性充分条件0.317。常数弹性可能高估潮汐政策中轻方向的损失（推断，未验证）。
- 未做：路段层面bootstrap；β_c改值后的反事实重跑；gate5边与1B边不一致的问题。

### 2026-11-13 β_c = 0.35 重跑（子agent执行，`beta_c_rerun_b035_summary_v1`）

- 口径除β_c外不变（OSM 1B、2025全日OD＋07–09用时、`ce_floor`、下限1%、κ谱半径规则1.483、θ=6.83）。上游不依赖β_c：κ、反演、基准均衡（W̄=3.0571）在四档下相同；top100选边四档完全一致（200/200）。
- 新输出：`tidal_lane_diagnostics_osm1b_ce_floor_b{029,035,044}_fl001_ks_od2025_alldaytt0709_v1`、`policy_comparison_osm1b_ce_floor_b035_fl001_ks_od2025_alldaytt0709_v1`、汇总 `beta_c_rerun_b035_summary_v1/`（summary.csv、README、各档checks）。新脚本 `run_beta_c_rerun_checks.py`。命令见汇总README。
- 潮汐top100，β_c = 0.25 / 0.29 / 0.35 / 0.44：短期福利 0.441 / 0.514 / 0.627 / 0.799%；长期 0.313 / 0.348 / 0.397 / 0.461%；长短期比 0.71 / 0.68 / 0.63 / 0.58；每人短期省时 21.6 / 25.0 / 29.9 / 37.1 秒；长期 −12.7 / −13.8 / −15.4 / −17.2 秒。
- P1分解（0.35，秒/人）：直接 −29.9，一般均衡 +11.2，选址 +34.1，长期合计 +15.4（0.25时为 −21.6 / +6.5 / +27.8 / +12.7）。
- 政策长期福利（0.35）：P6 1.096，P7 0.424，P3 0.419，P2 0.414，P1 0.397，P5 0.346，P4 0.085；P6b 1.044。潮汐为同走廊扩建的95.9%（0.25时95.7%）。与0.25相比唯一的排序变化是区域收费P7从第5升到第2；P7对P3只差0.005个百分点，与求解容差下的多起点差异同量级，应视为并列。
- 检查：所有均衡收敛；路由矩阵谱半径潮汐≤0.845、0.35政策最大0.949（P6b）；潮汐反事实7个起点求解，生产容差下福利差≤0.005个百分点，容差收紧到1e-7后为4e-6，0.35与0.44都未见第二个均衡（数值证据，非证明）。用时下限在反事实中约束2.2–2.4%（短期）、4.4–4.7%（长期）的边。
- 未做：0.29与0.44下的政策比较；定价情景的多起点与收紧容差检查。0.35来自不含下限的回归，模型仍带下限，两者口径不完全相同。
- 主会话核对：汇总表与政策表的数字与子agent报告一致；未独立复跑。β_c主值未改，待用户决定。

### 2026-11-13 锁定决定：主口径 β_c 由 0.25 改为 0.35

- 用户决定。依据：`congestion_floor_sensitivity_v1`（0.245依赖凌晨均值作下限；不含下限的高负荷局部估计0.29–0.44，高峰附近约0.35，边与路段一致）和 `beta_c_rerun_b035_summary_v1`（方向性结论不变，多起点求解未见第二个均衡）。
- 新主结果：潮汐top100福利 +0.627% / +0.397%，短期省29.9秒、长期多15.4秒；报告区间取0.29–0.44（长期0.35%–0.46%）。0.25的结果留作对照。
- 已更新：`nextstep.md`、`agent_quickstart.md`、`DOCUMENT_INDEX.md`。未改：代码默认值（β_c由命令行 `--congestion-lambda` 显式传入；`run_equilibrium_uniqueness_check.py` 与 `run_theta_first_second_order_osm1b.py` 的默认值仍是0.25，调用时需显式给0.35）、`results_update_and_specifications_20261110.md`（按惯例旧备忘不追改）、论文与slides。
- 仍是0.25下的结果，待重跑：θ敏感性与一阶 / 二阶分解、对称输入、分日选边、定价最优税、BPR对照、η=0.21标定；Brad备忘中的θ表和政策表。
- 已知局限：0.35超过唯一性充分条件0.317；估计不含下限而模型带下限；估计用gate5边、模型用1B边。

### 2026-11-14 β_c = 0.35 下游结果重跑（子agent执行，`beta_c_b035_downstream_rerun_v1`）

- 用户确认主值0.35，0.25作为保守下界保留。汇总 `data_work/outputs/beta_c_b035_downstream_rerun_v1/`（summary.csv为0.25与0.35并列，README含命令）。
- θ敏感性与一阶 / 二阶分解（`theta_first_second_order_osm1b_b035_alldaytt0709_v1`，全部收敛）：福利短期 / 长期 θ=3.4为1.263 / 0.805，6.83为0.627 / 0.397，10为0.428 / 0.266。一阶 / 短期0.965–0.969（0.25时0.98），长期 / 短期0.62–0.64（0.25时0.70–0.71），两者仍几乎不随θ变，福利仍按1/θ缩放。θ=6.83的差额：冲击规模+0.057，拥堵外部性−0.037，选址−0.230个百分点（0.25时+0.028 / −0.021 / −0.128）。一阶 / 长期1.51–1.56。
- η=0.21：福利0.124 / 0.099%（0.25时0.085 / 0.072%），一阶 / 短期0.87，长期 / 短期0.80，每人省时44.8 / 19.5秒。
- 对称输入（`symmetric_input_reestimated_osm1b_ce_floor_b035_fl001_ks_{geometric,arithmetic}_od2025_v1`）：潮汐长期福利有向0.423、对称0.306（几何），偏差−0.117个百分点（0.25时0.333 / 0.239 / −0.094）；对称输入仍只得到约72%的潮汐效果。注意：这一对比沿用旧数据包 `model_input_osm1b_L300_gate5_od2025_v2`（早高峰OD），当前主口径没有对称数据包，所以有向水平不等于主结果。
- 定价：0.35的政策比较已包含P6 / P6b / P7（加性实现），不需另跑。
- 未跑：分日选边福利列、定价情景多起点与收紧容差检查。子agent启动 `run_selection_split_day_check_gate5.py` 和新写的 `run_policy_multistart_b035.py` 时被权限检查拒绝，主会话未代为运行，留给用户批准或手动运行。分日选边旧命令的 `--mainline-top100` 参数是子agent推断的。
- 主会话核对：新旧两份θ汇总表逐列读过，与报告一致；未独立复跑。已把 `Documents/L2_notes/brad_meeting_prep_20261113.md` 的θ表、政策表和议题1的弹性说明更新为0.35，旧值在文中注明。

### 2026-11-15 β_c = 0.35 的两项补跑（主会话运行，用户批准）

- 分日选边 `selection_split_day_check_osm1b_ce_floor_b035_fl001_ks_od2025_v1`（早高峰OD数据包，与旧检查一致）。长期福利：偶数日自选0.421、用奇数日选边0.414（0.98倍）；奇数日自选0.419、用偶数日选边0.429（1.02倍）。短期直接省时互换 / 自选为0.98和1.01。重合度不变（奇偶88/100，各与全周94/100）。主线自选结果与保留选边文件一致，子agent推断的 `--mainline-top100` 参数正确。
- 政策均衡多起点与收紧容差检查 `policy_multistart_osm1b_ce_floor_b035_fl001_ks_od2025_alldaytt0709_v1`（脚本 `run_policy_multistart_b035.py`，运行前读过，只读输入、写新目录）。长期福利，容差1e-7、4个起点完全一致：P7 0.4244，P3 0.4200，P2 0.4144，P1 0.3973，P6b 1.0443。生产容差（1e-4）下6个起点的差：P7 0.0002，P3 / P2 0.0007，P1 0.0005，P6b 0.0004，P6 0.0028个百分点。
- 结论：P7 > P3 > P2 > P1 的次序不是求解误差，更正此前"应视为并列"的判断中关于数值误差的部分。P7对P3只高0.004个百分点，且定价福利用"按实际路径选择"的口径，与容量政策的W̄不是同一公式，经济上仍宜表述为相当。未见第二个均衡。
- P6：收紧容差下20000步未达1e-7（最终对数变化2–3e-6，税按逐次平均更新所致），6个起点（含税初值0和2倍）福利1.0962，差0.00004个百分点。
- 生产容差对报告值的影响：P1报告0.3970、收紧后0.3973；P3 0.4195对0.4200；P2 0.4139对0.4144。
- 已更新 `nextstep.md` 与Brad备忘政策表下的说明。

### 2026-11-15 弹性随负荷变化的试跑，以及模型方向车流的问题（子agent执行，`load_dependent_elasticity_trial_v1`）

- 脚本 `run_load_dependent_elasticity_{estimate,cf,summary}.py`（运行时替换 `_congestion_tau_update`，未改已有文件）。验证：弹性取平的0.35、曲率为0时逐位复现0.6266 / 0.3970；包装的BPR复现已有BPR结果；全部均衡收敛，三起点收紧容差下福利差4e-6个百分点。
- 位置：07–09点重方向边处在本边白天负荷分布的第96百分位，轻方向边第61百分位（中位数）；ln(τ_obs/τ_ff) 重0.605、轻0.312。
- 负荷相关弹性的估计（边，06–22点，IV）：二次形式 e(s)=0.188(0.011)+0.208(0.023)·s，重 / 轻位置0.29 / 0.21；三段线性0.025、0.305、0.274（中段以上不再变陡）；按拥堵比三分组的07–09点IV 0.029、0.130、0.719（分组用了配速，可能偏高）。水平互相矛盾（重方向0.27–0.72）。
- 潮汐福利（短期 / 长期，基准0.627 / 0.397）：只加曲率0.565 / 0.375；重0.39轻0.31（均值保持0.35，这是为隔离方向差而设的归一化，不是估计）0.721 / 0.438；二次形式原始水平0.534 / 0.355；三段线性0.626 / 0.397。只改轻方向弹性0.10–0.50：0.633–0.624。只改重方向0.30 / 0.40 / 0.45：短期0.532 / 0.722 / 0.819。长期P1 / P2在0.948–0.985之间。
- 结论：猜想"常数弹性高估轻方向损失"在位置上成立，但在模型里几乎不起作用，因为轻方向边几乎没有车流；结果取决于重方向弹性的水平，数据定不住。暂不改函数形式。
- **方向车流问题（主会话已用 `tidal_treated_edge_detail.csv` 复算）**：100条潮汐走廊上模型基准车流轻 / 重之比中位数0.0176、总量比0.0205，全部低于0.1；观测07–09点全部车辆在68条匹配走廊上为0.67（中位数）。潮汐使重方向边用时变化中位数 −9.6%（−85秒）、轻方向 +14.8%（+97秒）。车流固定的局部核算（子agent，表 `table_pe_observed_vs_model_flow_weights.csv`，主会话读表未复跑）：按模型车流加权轻方向损失 / 重方向收益为0.028，按观测车流为0.81；观测加权的净省时为模型加权的0.20倍；负荷相关弹性在观测加权下使净省时为常数弹性的2.0–2.5倍（三段线性1.07倍）。
- 含义：现有福利是早高峰通勤者的福利，是全部道路使用者收益的上界，可能很宽松；10-29之后关于背景车流的检验用的是观测OD而非模型均衡车流，且未限定在模型选出的走廊，所以没有暴露这一点。背景车流进入拥堵函数并报告全部车辆的用时变化，应成为求解器改动的第一优先。
- 未做：均衡模型内加入背景车流后的重算；定价情景；估计误差向福利的传导。200条潮汐边中45条不在估计面板，取同角色中位数。
- Brad讨论材料：`Documents/PreSlides/brad_202611/`（`slides_brad_202611.tex/.pdf` 16页正文加5页附录、`speaker_notes.md`、`make_figs.py`），图在 `Documents/L3_figs/brad_202611/`。

### 2026-11-15 北京路段的流量–密度曲线（子agent执行，`flow_density_per_lane_v1`）

- 脚本 `build_segment_flow_10min.py`（从389个原始RN文件重建10分钟路段车流，按小时汇总后与 `segment_flow_hour` 逐位一致，输出 `segment_flow_10min_rn202505_v1`）和 `run_flow_density_per_lane.py`。样本18,361个路段、1,440万个路段×10分钟，2025-05-19至05-25。
- 数据口径（查实）：车流是联通RN有效通过次数，按弦长加权平均，是手机样本的人次，不是车辆，未扩样；车道数由 `ROADTYPE_LANE_MAP` 按道路等级赋值（2→6，3→4，4→2），项目中没有观测车道数，也没有限速。因此形状、速度比、弹性可识别，每车道通行能力和临界密度的水平不可识别。
- 新发现的数据问题：百度有五个默认速度值（23.6、28.6、18.6、21.0、20.0 km/h），占全部时段20%；恒值规则滤掉大部分，剩约3.7%以短串形式存在。本次剔除，保留它们的敏感性运行结果变化小于1%。恒值占18.3%（06–22点20.8%）。
- 路段内形状（车流除以本路段均值，速度除以本路段v90，路段固定效应）：三类道路都有上升的自由流段。快速路在速度为v90的0.42处达到最大车流，最低速度处车流为最大值的0.88 [0.85, 0.90]，工作日早 / 晚高峰31.8% / 36.5%的时段在下降段。主干路和次干路在0.18处才到最大，最低速度处为0.96 / 0.98，高峰约2%的时段在下降段，形状是平台而非下降段。
- 机械相关：密度由车流构造，按密度分箱使通行能力高估50–70%；主结果按速度分箱，并用RN对随机分半的车流做了检验（两半相关0.79–0.82），形状保留、顶部更低更平。
- 车道缩放：无法检验。车道数与道路等级完全重合，同一等级内没有车道数变化；用赋值车道数时每车道曲线不重合（次干路每车道通行能力为主干路的1.60倍），但无法区分是缩放不成立、车道映射有误还是各等级采样比例不同。需要分路段车道数（如OSM `lanes`）和车辆计数。
- 与项目弹性的关系：路段内曲线隐含的用时对车流弹性在速度为v90的0.97处0.07–0.13，0.92处0.18–0.26，0.87处0.34–0.55。与0.16–0.44量级一致，但两者用的变异不同，曲线也解释不了弹性随负荷上升。
- 潮汐粗算（车流固定，主干路，重方向0.7 v90、轻方向0.95 v90）：重方向速度约 +28%，轻方向约 −13%，总用时约 −10%。最不对称的路段对里重方向已在排队，静态曲线给不出答案。
- 主会话核对：读了路段内曲线表和图，与报告一致；未复跑。Brad讨论slides第9页和讲稿已据此填写。

### 2026-11-15 背景车流进入拥堵函数的试算（子agent执行，`background_traffic_trial_v1`）

- 脚本 `run_background_traffic_trial.py`、`run_background_traffic_trial_summary.py`（运行时替换求解器函数，未改已有文件）。形式：τ_e = max{f_e, τ_obs·[((Ξ_e+B_e)/(Ξ_obs,e+B_e))·(n_obs/n)]^β}，β=0.35，B_e为固定背景车流。验证：B=0逐位复现0.6266 / 0.3970；每个B下基准都落在观测锚点上；B=0时全部车辆用时等于通勤者用时；全部均衡收敛，主规格与改路规格四起点收紧容差下福利差5.8e-6个百分点。
- 背景车流的来源与规模：主规格用观测07–09点工作日非通勤车流（联通全部目的车流减严格通勤），按格对匹配到1B边（2,872条中2,111条匹配，潮汐200条中44条未匹配，用路由的非通勤端点OD补），规模按"模型通勤者＝观测严格通勤者"定为通勤车流的2.47倍。其他规模：1.19、1.81、3.42倍和0.5–3倍网格；其他形状：路由的非通勤端点OD（2.16倍）、使总车流逐边等于观测的残差形状。
- 方向比（100条走廊，轻 / 重，中位数）：模型通勤0.018；观测全部车辆0.674、观测严格通勤0.525（68对）；观测通勤OD按模型核路由0.296；加背景后主规格0.294，残差形状0.517。即模型均衡的通勤车流本身过于单向，加背景只修正权重，不修正这一点。
- 通勤者（同一批走廊）：福利主规格 +0.638% / +0.472%，各规模短期0.629–0.641、长期0.418–0.489；短期省时约29.7秒不变，长期 −19.6秒；重方向边用时 −9.6%，轻方向 +16.5%。
- 全部车辆（短期，秒 / 通勤者，背景只按时间计）：主规格重方向收益58.4、轻方向损失24.2，抵消41%（B=0时2%，背景车辆自身86%）；全部车辆省33.3（通勤29.7＋背景3.6），占基准全部车辆用时1.3%。残差形状：背景车辆净损失15.1，全部车辆只省14.5，抵消67%。背景按模型改路（规模2.16）：全部车辆省39.2，抵消27%。长期：主规格全部车辆 −21.8（通勤 −19.6，背景 −2.2），主要仍是通勤者选址后通勤变长。
- 潮汐对同走廊扩建：通勤者福利短 / 长期0.96 / 0.95（B=0为0.98 / 0.96）；全部车辆短期省时之比0.49（B=0为1.06；0.5 / 1 / 2 / 3倍为0.86 / 0.72 / 0.55 / 0.45；残差形状0.25；改路0.78）。长期扩建使全部车辆省12.6秒，潮汐多21.8秒。
- 按总车流重新选走廊：主规格保留87 / 100（各规格77–97）；通勤者福利0.589 / 0.440，全部车辆省33.7秒，对扩建之比0.50。重选不能挽回全部车辆口径下的差距。
- 结论：通勤者福利站得住；"潮汐不增车道公里而达到扩建的96%"只对早高峰通勤者成立，按全部车辆的省时约为扩建的一半（0.25–0.86，取决于背景规模和方向）。主会话读了汇总表核对，与报告一致；未复跑。
- 局限：背景规模不确定（车流是抽样人次；严格通勤占比是下界，2.47倍可能偏高）；端点OD是06–10点出发而用时是07–09点；β未重估；定价情景未跑；模型通勤车流过于单向的问题未处理。
- Brad讨论slides新增第9页"Background Traffic in the Model"（现为17页正文加5页附录），讲稿同步。

## 2026-11-18 项目入口整理与 Git 快照

- 当前主线口径只在 `DOCUMENT_INDEX.md` 顶部 “Current mainline” 表里写一次（口径、输入包、参数目录、主结果、运行命令、代码入口、数字的适用范围），并列出各对外材料用的是哪一版结果。旧 gate5 主线的四份 authoritative 文档改标 historical。
- 新增 `data_work/docs/MAINLINE_MANIFEST.md`：主线输出目录清单（不可重建的百度格对采集、输入链、0.35 结果、试算、0.25 对照），备份以此为准。输入包构造命令的 `--tt-source` 取值待核实。
- 入口文件改为指向该表：`agent_quickstart.md`（不再要求通读 WORK_LOG，合并并删除 `agent_entry_prompt.txt`）、根 `AGENTS.md`、`data_work/AGENTS.md`（重写）、`data_work/README.md`（重写，旧 stage 链和 gate5 复现移到末节）、`data_work/outputs/README.md`、`full_estimation/README.md`（注明默认值是旧主线）、11-10 结果备忘加取代说明。
- `.gitignore` 增加媒体、parquet、`*.env`、`Processed_Data/`、平台培训资料、生成的大表等规则；未跟踪文件从约 5.8 GB 降到约 12 MB。`sync_github.sh` 改为先列出待提交文件并要求确认，默认不推送。删除 7 月 3 日遗留的空 `.git/index.lock`。
- Git 快照提交在新分支 `snapshot/20261118-mainline`，未推送。
