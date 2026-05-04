# Next Step

这份文件只保留当前任务状态、最近可执行动作和开放决策。已完成工作与锁定参数统一记录在 `data_work/docs/WORK_LOG.md`。

## 当前状态

- 研究主线不变：以方向性速度不对称为核心，连接 directed network 与 quantitative spatial model。
- 2026-05-02 合作者会议已同步 `TidalLanes_0429.tex`、`MODEL_replication_tidallanes.tex` 和 `TidalLanes_Model_v4.tex`；会议 notes 见 `Documents/L2_notes/meeting_notes_20260502.md`。
- grid-travel cost 的具体选择仍未定，不能把当前 `raw_connected_v1_grid_travel_time_edgeproj_v2` 直接写成最终口径。
- 图中没有数据或只有单向数据的线段，需要用浅灰色表示缺失的 directional information。
- 模型线近期不做参数估计，先搭 full estimation 框架和 counterfactual 流程；暂不考虑 exact-hat。
- 参数选择需要参考尤炜老师相关工作后再锁定。
- national 爬取城市清单已切到百度官方 2024 百城通勤高峰拥堵指数版本：`data_work/config/national_tidal_commuting_baidu2024_top100_congestion_v1.json`；建样本输出在 `data_work/outputs/national_tidal_commuting_baidu2024_top100_congestion_v1/`。

## 当前下一步（按优先级）

1. 搭 full estimation 的代码/文档框架，先不估计参数，不做 exact-hat，把 counterfactual workflow 写清楚。
2. Finish Pattern：先完成 within-city pattern evidence，必要时加入 Shenzhen。
3. 更新相关图件规则：没有数据或只有单向数据的线段改为浅灰色，并同步到需要展示的 manuscript / memo 图。
4. national 版本按 `national_tidal_commuting_baidu2024_top100_congestion_v1` 跑速度爬取；完成后补城市层面散点图：x 轴为 `commute_peak_congestion_index`，y 轴为 asymmetry。
5. 检查深圳 case 是否能做 reduced-form，并列出可行的数据、变量和识别边界。
6. 细化参数和数据细节，尤其是参数来源、grid-travel cost 口径和 BPR/free-flow 定义。

## 当前开放问题

- grid-travel cost 最终采用哪一种构造方式？
- cross-city subsection 是否加入；如果加入，需求侧用职住分离，供给侧用交通方式比例和路网结构解释城市差异。
- Shenzhen reduced-form 是否有足够干净的数据与可信识别空间？
- 参数选择具体对齐尤炜老师哪一篇或哪一组估计？
- 百度 2024 base 中三亚在源表内，但当前建样本为 0 个可导出 pair；是否保留为 0 覆盖城市，还是在爬取清单中补入下一名/另行处理三亚坐标覆盖？

## 先看哪里

- `data_work/docs/WORK_LOG.md`
- `Documents/L2_notes/meeting_notes_20260502.md`
- `Documents/L2_notes/AA_commuting_replication_adaptation_notes_20260426.md`
- `Documents/L2_notes/model_v1_review_and_data_gap_temp_20260422.md`
- `Documents/L2_notes/model_v1_implementation_checklist_temp_20260423.md`
- `data_work/outputs/comparison_grid_methods/`
- `Replication_apply/PLAN.md`
