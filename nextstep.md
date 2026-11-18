# Next Step

只放当前状态、下一步和开放决策。当前口径、主结果、运行命令只在 `DOCUMENT_INDEX.md` 的 “Current mainline” 表里写；已完成工作写 `data_work/docs/WORK_LOG.md`（新条目在最上面）。11-18 改版前的内容在 Git 历史和 `Documents/L2_notes/_superseded/nextstep_snapshot_20261113.md`。

## 当前状态（2026-11-18）

- **主线**：1B、全日 2025 OD＋07–09 用时、`ce_floor` β_c = 0.35。潮汐 top100 通勤者福利 +0.627% / +0.397%（短期 / 长期），长期为同走廊扩建的 0.96。政策排序和“短期省时、长期福利为正”在各口径下都成立；不稳的是福利水平。
- **最大的已知漏洞**：模型里潮汐走廊轻方向几乎没有车流（轻 / 重车流比，模型 0.02，观测 0.67）。所以现有福利只是早高峰通勤者的福利，是全部道路使用者收益的上界。加固定背景车流后，通勤者福利不变（0.638% / 0.472%），但潮汐对扩建的全车辆短期省时比降到 0.49。模型通勤车流本身过于单向，这一点还没解决。见 WORK_LOG 11-15。
- **0.35 的局限**：超过唯一性充分条件 0.317，只有数值证据；估计不含下限而模型带下限；估计用 gate5 边，模型用 1B 边。
- **对外材料**：Brad 讨论材料已是 0.35；论文（4 月）、`Tidal0715`（7 月）、复现包仍是旧主线数字。
- **百度格对采集**：`grid_pair_baidu_validation_v3` 11-16 18:01 全部完成，5 天 × 3 时段。
- **Git**：11-18 快照在分支 `snapshot/20261118-mainline`，已推送，未合并到 `main`。

## 下一步（按依赖）

### A. 现在就能做（不等合作者）

1. **备份不可重建的输出**：先备份 `grid_pair_baidu_validation_v3`，再按 `data_work/docs/MAINLINE_MANIFEST.md` 备份主线输出。
2. **格对外部基准正式对照**（1B、1A、gate5），决定格心接入方式（最近节点或支路接入）。中期判断：1B 的偏差来自接入，路网速度可信。临时脚本在 `data_work/src/raw_baidu_validation/interim_20261111/`，要整理成正式脚本。产物：网络对照表和格心接入建议。
3. **修代码问题**（见下方清单 B1–B5）。B2 会使潮汐短期福利上升约 2%，修完后主结果需重跑一次。

### B. 等合作者定口径（现有试算可直接拿去讨论）

4. 一次定四件事：
   - **背景车流**：“背景 OD 固定、按模型改路”，还是固定在格边，或维持同比例假设。试算 `background_traffic_trial_v1`。
   - **OD 距离衰减**：是否用嵌套参数 η、是否加格内用时、每分钟福利价值用哪个口径（主线 1.22%、η 标定 0.16%、ν/θ 换算 0.73%）。
   - **正文主指标**：建议以省时分钟数、与扩建的比值、政策排序为主，福利按区间报告；“96%”只和“通勤者福利”一起写。
   - **BPR**：只作对照，还是补完模型推导后进正文（`bpr_congestion_version_20261108.md`）。

### C. 口径冻结之后

5. **改求解器**：背景车流按定下的方式进入，并同时报告全部车辆的用时变化；η ＋ 格内用时。先后与合作者定。
6. **整套重跑一次**：政策比较、对称输入、分日选边、弹性区间、定价（按均衡重求最优税）；补带下限模型和 BPR 的标准误；查 Spec C 在 2025 OD 下福利放大约 15 倍的来源。新结果先改 `DOCUMENT_INDEX.md` 的主线表和 `MAINLINE_MANIFEST.md`。
7. **写作**：先把 `Tidal0715` 换成新口径，再从 deck 往前写论文，替换 4 月的 `TidalLanes_0429.tex`；复现包随后同步。

## 工作线状态

| 工作线 | 状态 | 下一步或重启条件 | 入口文档 | 入口代码 | 当前输出 |
|---|---|---|---|---|---|
| 主线模型 | 进行中 | 上面 A3、B、C | `DOCUMENT_INDEX.md` 主线表 | `data_work/src/model/full_estimation/` | 见 `MAINLINE_MANIFEST.md` |
| 百度格对验证 | 进行中 | 上面 A2 | WORK_LOG 11-11 两条 | `src/raw_baidu_validation/interim_20261111/`、`scripts/run_grid_pair_validation_week.sh` | `grid_pair_baidu_validation_v3` |
| 拥堵弹性估计 | 有未处理问题 | 下方清单 E1–E3；是否改弹性随负荷变化的形式（试跑后暂不改） | WORK_LOG 11-13 审计条 | `run_congestion_almagro_spec.py`、`run_congestion_floor_sensitivity.py` | `congestion_floor_sensitivity_v1` |
| 平台提取 | 本批完成 | 口径定后按需追加导出；是否要更多周的速度与车流（现只有 0519–0525） | `Platform/README.md` | `Platform/utils/daas_task.py` | `Platform/runs/202505_v01/` |
| 彩田路视频 | 暂停（8-26 审计后） | 补采同一切换时刻的多个工作日，需用户安排；唯一可能形成因果证据的一条线 | `data_work/docs/CAITIAN_VIDEO_IDENTIFICATION_PLAN_20260715.md` | `flowvedio/` | `Documents/L2_notes/caitian_video_rdit_complete_audit_20260826.pdf` |
| 深圳 | 暂停，低优先级 | 留仙 DID 需购买数据 | `shenzhen_liuxian_did_purchase_design_20260626.md`、`SHENZHEN_MULTI_TIDAL_BAIDU_HANDOFF_20260628.md` | `data_work/src/shenzhen_*` | `shenzhen_multi_tidal_refinement_v1` |
| 全国爬取 | 暂停，低优先级 | 只在论文需要跨城事实时重启 | `data_work/docs/NATIONAL_TIDAL_COMMUTING_WORKFLOW.md` | `data_work/src/national_tidal_commuting/` | `national_tidal_commuting_baidu2025_top100_congestion_v1` |
| 中心线稳健性 | 暂停 | 正式跑时用 Tier A，Tier B 检查缺几何的增量 | `WORK_LOG_2026Q3.md`（08-04） | `data_work/src/diagnostics/build_centerline_crossgrid_robustness_tiers.py` | `centerline_crossgrid_robustness_tiers_v1` |
| 写作与对外材料 | 等口径冻结 | 上面 C7 | `DOCUMENT_INDEX.md` 材料表 | `Documents/PreSlides/`、`Documents/L1_manuscripts/` | `Tidal0715`、`TidalLanes_0429.tex`（均旧数字） |

## 代码与估计问题清单（审计发现，未改）

模型代码（WORK_LOG 11-13 代码审计）：

- [ ] B1 `tidal_lane_diagnostics.py:89` 期望路径分钟对 R ≤ 1e-12 的 OD 返回 0（η = 0.21 时报 37.9，应为 41.1）
- [ ] B2 `equilibrium_solve.py:226`、`invert_fundamentals.py:149` η = 1 时 R 按 1e-12 截断，潮汐短期福利低估约 2%
- [ ] B3 `stage2_counterfactuals.py:793` 收费收入未除 κ（1B 上高估 1.48 倍，不进福利）
- [ ] B4 定价默认列 `Wbar_pct_change_realized` 仍是作废公式，正确值在 `…_route_choice` 列
- [ ] B5 政策比较、对称重估、分日选边、唯一性检查、按道路定义等脚本不读 `route_eta`，会静默按 η = 1 运行

拥堵估计（WORK_LOG 11-13 估计审计）：

- [ ] E1 β_c 对凌晨下限 T0 很敏感（ln T0 ±0.03 时 0.20–0.37）：报告 β 随下限平移的曲线，或把平移参数一起估
- [ ] E2 恒值填充（约 20%）在主估计和模型格对用时里都没剔除；另有五个百度默认速度值占全部时段的 20%
- [ ] E3 β_c 在 gate5 边估计、用于 1B 边（26% 的 1B 边不在估计样本）：在 1B 边上用 1B 分小时用时重估

（审计里“背景车流检验要在主口径重跑”一项，已由 11-15 的两项试算取代。）

## 开放问题（不急于定）

- “对称会漏掉什么”依赖底网（1B 上长期福利 0.33% → 0.24%，gate5 上几乎无差别），正文主讲哪一层。
- 通勤者范围是否扩到含停留的通勤链（×1.10，福利变化 < 1%），与背景车流一起定。
- 唯一性的解析部分：AA 充分条件换算为 β_c ≤ 0.317，路段层面 0.42 不满足；带下限函数与 BPR 的推广待做。
- 公交专用道、尾号限行、信号优先三个政策未跑（Brad 讨论提到）。
- 旧的开放问题（深圳 crosswalk、cross-city 小节、留仙 DID 数据、三亚）见快照文件。
