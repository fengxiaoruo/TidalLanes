# 深圳多条潮汐车道百度 post-only 分析交接

## 当前口径

这项工作目前使用深圳百度速度样本的一周数据，时间为 `2025-05-19` 至 `2025-05-25`，每 10 分钟一条速度记录。当前结果是 post-only 的日内比较，不是开通前后因果估计。

主输出版本：

- `data_work/outputs/shenzhen_multi_tidal_post_only_v1/`

主脚本：

- `data_work/src/shenzhen_multi_tidal/run_multi_tidal_post_only_analysis.py`
- `data_work/src/shenzhen_multi_tidal/plot_raw_distribution_diagnostics.py`

重跑命令：

```bash
python3 -m py_compile \
  data_work/src/shenzhen_multi_tidal/run_multi_tidal_post_only_analysis.py \
  data_work/src/shenzhen_multi_tidal/plot_raw_distribution_diagnostics.py

python3 data_work/src/shenzhen_multi_tidal/run_multi_tidal_post_only_analysis.py
python3 data_work/src/shenzhen_multi_tidal/plot_raw_distribution_diagnostics.py
```

## 原始数据和配置

主要输入：

- 百度速度数据：`data_work/raw_data/speed_shenzhen_all_wgs84.csv`
- 百度深圳路网：`data_work/raw_data/gis/roads_baidu/shenzhen_roads.shp`
- 潮汐车道 shp：`data_work/raw_data/gis/深圳潮汐车道shp/`
- Tianyu 路段说明表：`data_work/raw_data/深圳市潮汐车道2.xlsx`
- 当前锁定窗口配置：`data_work/config/shenzhen_tidal_lane_operation_windows_v1.json`

潮汐 shp 数字对应关系：

- `1`: 文锦北路
- `2`: 彩田路
- `3`: 惠深沿海高速，城市道路分析中排除
- `4`: 新洲路
- `5`: 红岭路
- `6`: 留仙大道

当前运行窗口和 policy-like 方向：

- 文锦北路 AM：`07:00-09:30`，policy-like 为北往南
- 文锦北路 PM：`18:00-20:00`，policy-like 为南往北，出关方向
- 彩田路：`16:30-21:30`，policy-like 为福田至龙华，北行方向
- 新洲路：`17:30-20:00`，policy-like 为北行，莲花路至梅丽路方向
- 红岭路：`07:30-09:30`，policy-like 为南行
- 留仙大道：`17:30-20:00`，policy-like 为西往东或西南往东北

留仙大道要作为 pre-shock placebo 看待。当前百度周是 `2025-05-19` 至 `2025-05-25`，而留仙大道公开通告开通时间在 `2026-03-23` 之后，所以不能解释为 post-opening 效果。

## treatment 和 control 构造

treatment 使用潮汐车道 `*_单线.shp` 和百度路网相交匹配。当前主分析只使用 `core_treatment`，并用 `analysis_length_m` 加权。`analysis_length_m` 是百度 roadseg 与潮汐车道 buffer 的 overlap 长度，不再用整条百度 roadseg 长度。

control 使用最后一版 refined rule：

- 排除所有与潮汐车道 exclusion buffer 相交的路段
- 排除无名路
- 排除名称含 `无名`、`高速`、`快速`、`隧道`、`立交`、`匝道` 的路段
- 排除同名走廊延伸
- 候选路段距 treatment 至少 120 米
- 候选路段方向轴与 treatment 相近
- 候选路段长度与 treatment focused analysis length 不要过于离谱

最终 control 表：

- `data_work/outputs/shenzhen_multi_tidal_post_only_v1/metrics/control_candidates_by_tidal_lane.csv`

最终 control 名单：

- 文锦北路：金运路、翠竹路、吉华路、红岗路、西环路
- 彩田路：金田路、皇岗路、益田路、福田路、华富路
- 新洲路：景田路、益田路、香梅路、梅村路、梅秀路
- 红岭路：八卦路、梨园路、宝安北路、玉平大道、上步中路
- 留仙大道：学苑大道、玉龙路、民丰路、民宝路、民康路

长度核查：

- `data_work/outputs/shenzhen_multi_tidal_post_only_v1/metrics/treatment_control_corridor_length_summary.csv`
- `data_work/outputs/shenzhen_multi_tidal_post_only_v1/metrics/treatment_control_direction_length_summary.csv`

注意：corridor-level `analysis_length_km` 是两个方向合计；direction table 给单向长度。

## 已经重画的 weekday time-series 图

这些图使用 final treatment/control，样式与 `hongling_am_0730_0930_weekday_timeseries.png` 一致。每张图同时有 PNG、PDF、SVG。

目录：

- `data_work/outputs/shenzhen_multi_tidal_post_only_v1/figures/`

文件：

- `wenjinbei_am_0700_0930_weekday_timeseries.png`
- `wenjinbei_pm_1800_2000_weekday_timeseries.png`
- `caitian_pm_1630_2130_weekday_timeseries.png`
- `xinzhou_pm_1730_2000_weekday_timeseries.png`
- `hongling_am_0730_0930_weekday_timeseries.png`
- `liuxian_pm_1730_2000_weekday_timeseries.png`

图的含义：

- 横轴是小时
- 纵轴是 length-weighted harmonic corridor speed
- treatment 线更粗，controls 更细
- 两个 panel 分别是 `policy_like` 和 `opposite_like`
- 阴影区域是潮汐车道实施窗口
- 竖虚线和点线分别是开始和结束时点

## 其他 retained outputs

地图：

- OSM 蒙版地图：`data_work/outputs/shenzhen_multi_tidal_post_only_v1/figures/maps_osm_masked/`
- 无 OSM 百度路网地图：`data_work/outputs/shenzhen_multi_tidal_post_only_v1/figures/maps_plain/`

summary 图：

- `data_work/outputs/shenzhen_multi_tidal_post_only_v1/figures/summary/active_reference_treatment_control_change_summary.png`
- `data_work/outputs/shenzhen_multi_tidal_post_only_v1/figures/summary/directional_asymmetry_treatment_control_summary.png`
- `data_work/outputs/shenzhen_multi_tidal_post_only_v1/figures/summary/event_study_active_window_summary.png`

raw time-variation 图：

- `data_work/outputs/shenzhen_multi_tidal_post_only_v1/figures/raw_time_variation/`

每个 road-window 有两张：

- `*_raw_speed_over_time.png`
- `*_raw_change_over_time.png`

raw time-variation 点级表：

- `data_work/outputs/shenzhen_multi_tidal_post_only_v1/metrics/raw_time_variation_change_panel.csv`

event-study DID：

- 图：`data_work/outputs/shenzhen_multi_tidal_post_only_v1/figures/*_event_study_did_pooled.png`
- 系数：`data_work/outputs/shenzhen_multi_tidal_post_only_v1/metrics/event_study_did_coefficients.csv`
- diagnostics：`data_work/outputs/shenzhen_multi_tidal_post_only_v1/metrics/event_study_did_diagnostics.json`

主要面板数据：

- selected segment speed panel：`data_work/outputs/shenzhen_multi_tidal_post_only_v1/data/selected_segment_speed_panel.csv`
- corridor 10-minute speed：`data_work/outputs/shenzhen_multi_tidal_post_only_v1/data/corridor_10min_speed.csv`
- window daily summary：`data_work/outputs/shenzhen_multi_tidal_post_only_v1/data/window_daily_summary.csv`
- event-study sample：`data_work/outputs/shenzhen_multi_tidal_post_only_v1/data/event_study_did_sample.csv`

## 做过的主要尝试

1. 留仙大道高德 pre/post 两周数据初步分析  
   先做方向、工作日/周末、小时图；再看 15:00-17:30、17:30-20:00、夜间 reference 等窗口。结论是留仙大道在高德样本中没有清晰支持政策方向改善，且后续发现百度周对于留仙大道是 pre-shock。

2. 留仙大道高德和百度底层路段匹配  
   对高德 treatment 做坐标 shift 和百度 overlap 匹配，画地图和两个方向时序图。该部分输出在 `data_work/outputs/shenzhen_liuxian_gaode_initial_v1/`。

3. 彩田路百度路网匹配  
   使用潮汐车道 shp 和百度深圳路网相交，初步找 treatment、controls、上下游网络。后来扩展到所有城市道路潮汐车道。

4. 多条潮汐车道统一 post-only 分析  
   建立 `shenzhen_multi_tidal_post_only_v1`，包括 treatment matching、control selection、weekday time-series、window metrics、directional asymmetry、event-study DID、地图。

5. Tianyu Excel 核查和官方口径冲突记录  
   解析 `深圳市潮汐车道2.xlsx`。记录冲突：文锦北路 PM Tianyu 是 `17:30-20:30`，官方/project 是 `18:00-20:00`；彩田路 Tianyu 是 `17:30-20:30`，当前 project 是 `16:30-21:30`；红岭路 Tianyu 是 `07:30-09:00`，当前 project 是 `07:30-09:30`。

6. treatment length reweighting  
   treatment 不再按整条百度 roadseg 长度加权，而按潮汐车道 buffer overlap 长度加权。这个修正后，彩田路、红岭路、新洲路、文锦北路、留仙大道的 effective length 更接近实际施行长度。

7. refined control reselection  
   去掉无名路、快速路、立交、匝道、同名延伸和过近候选。当前 control 是 final refined version。

8. raw time-variation diagnostics  
   用户明确要看“速度随时间变化”，不是横截面 distribution。当前主看 `figures/raw_time_variation/`，其中 treatment 是每个工作日的 10 分钟原始点线，controls 是 10 分钟点云加 same-minute mean。

## 当前主要结果

基于 refined controls 的 active/reference 比较，treatment 相对 controls 的变化为：

- 彩田路 `16:30-21:30`：policy-like -0.32 km/h，opposite -9.03 km/h
- 红岭路 `07:30-09:30`：policy-like +4.02 km/h，opposite -1.03 km/h
- 文锦北路 AM `07:00-09:30`：policy-like -22.02 km/h，opposite -8.51 km/h
- 文锦北路 PM `18:00-20:00`：policy-like -3.84 km/h，opposite +1.79 km/h
- 新洲路 `17:30-20:00`：policy-like -4.87 km/h，opposite +0.09 km/h
- 留仙大道 `17:30-20:00`：policy-like -4.67 km/h，opposite +0.63 km/h

基于 pooled-control event-study active-window 均值：

- 彩田路：policy-like -0.90 km/h，opposite -9.99 km/h
- 红岭路：policy-like +2.05 km/h，opposite +0.44 km/h
- 文锦北路 AM：policy-like -15.97 km/h，opposite -7.22 km/h
- 文锦北路 PM：policy-like -0.68 km/h，opposite +2.03 km/h
- 新洲路：policy-like -0.28 km/h，opposite +1.61 km/h
- 留仙大道：policy-like -5.02 km/h，opposite -0.17 km/h

解释：

- 彩田路最像“政策方向被保护住，反方向明显变慢”。它不一定是政策方向绝对变快，而是相对反方向的不对称性增强。
- 红岭路最接近“政策方向变快，反方向不明显变差”的目标符号。
- 文锦北路、新洲路、留仙大道在当前 post-only 设计下不支持理想符号结构。
- 留仙大道应该作为 pre-shock placebo 使用。它在 raw time-variation 图中没有政策方向独有的清晰突变。

## 当前识别限制

- 样本只有一周，真正独立的日内冲击很少。
- 当前是 post-only 设计，不能替代开通前后 DID。
- final controls 比之前干净，但仍可能受到局部路网传导影响。
- treatment 和 control 的 raw speed distribution 在一些路段上本来就不同，尤其彩田路和留仙大道，control selection 对结果很敏感。
- 长时间 active window 可能混入排队传导、晚高峰需求变化和 network spillover。

## 下一步具体操作方案

### Step 1: 把 controls 分层

不要把近邻路段都当 clean controls。建议构造：

- `core_treatment`
- `near_spillover`: treatment 周边 0-1 km
- `medium_spillover`: 1-3 km
- `far_control`: 3 km 以外或不直接相连的平行路

主结果用 `far_control`。近邻路段改为 spillover evidence。

### Step 2: 做 short-window local event

对每条路分别估计 `±30`、`±60`、`±90` 分钟窗口。目标是减少长窗口中的路网传导和晚高峰趋势污染。

建议规格：

```text
speed_{i d h} = alpha_i + alpha_{i,d} + tau_{d,h}
              + beta Treat_i * PolicyDirection_i * 1[h >= 0]
              + epsilon_{i d h}
```

其中 `h=0` 是各路段实施开始时点，例如彩田路 16:30、红岭路 7:30。

### Step 3: 做 corridor 打包

同时构造三种 treatment/control corridor：

- core only
- core + 1 km upstream/downstream
- core + 3 km upstream/downstream

结果变量建议主用 `log travel time`，辅用 length-weighted harmonic speed。

### Step 4: 做 control quality screening

用非潮汐时段筛选 controls。可用指标包括：

- 非 active hours 的 raw speed profile 相似度
- reference window 速度水平
- 全天 diurnal shape correlation
- active window 之外的 placebo event-study

### Step 5: 留仙大道作为 placebo

留仙大道当前百度周是 pre-shock。下一版应该明确把它放在 placebo section：理想结果是两个方向都没有政策方向独有断点。

### Step 6: 如果能买数据，优先补真正 pre/post

最有价值的是为彩田路和红岭路补开通前后多周数据。当前一周 post-only 只能支持 directional pattern，不能单独支撑 causal opening effect。

## Refinement v1 已落地（2026-06-28）

在 `post_only_v1` 基础上，已实现并跑通 Step 1–5 的 refinement 模块。

主脚本：

- `data_work/src/shenzhen_multi_tidal/run_multi_tidal_refinement_analysis.py`

输出根目录：

- `data_work/outputs/shenzhen_multi_tidal_refinement_v1/`

重跑命令：

```bash
python3 -m py_compile data_work/src/shenzhen_multi_tidal/run_multi_tidal_refinement_analysis.py
python3 data_work/src/shenzhen_multi_tidal/run_multi_tidal_refinement_analysis.py
```

### Step 1: control 分层

- 按 corridor 最近距离分层：`near_spillover` 0–1 km，`medium_spillover` 1–3 km，`far_control` ≥3 km。
- 表：`metrics/control_tier_summary.csv`，segment 级表 `metrics/control_tiers_by_segment.csv`。
- 主结果 tier 规则：优先 `far_control`；若无则 `medium_spillover`；再无则 `near_spillover`。
- 记录：`metrics/primary_control_tier_by_road.csv`。
- 当前 primary tier：彩田路、留仙大道 → `medium_spillover`；其余 → `near_spillover`（3 km 内候选为主，远控极少）。

### Step 2: short-window local event

- ±30 / ±60 / ±90 分钟窗口，估计 `Treat × PostStart` local jump 和 10 分钟 event-study bins。
- 三个 tier 均保留，便于 near vs medium 对比。
- 表：`metrics/short_window_local_jump_coefficients.csv`，`metrics/short_window_event_study_coefficients.csv`。
- 图：`figures/summary/short_window_local_jump_primary_tier.png`。

Primary-tier local jump（±60 min，窗口起点断点）要点：

- 彩田路：policy-like −1.3 km/h，opposite −9.5 km/h（medium controls）。
- 红岭路 AM：policy-like +2.8 km/h，opposite −0.1 km/h（near controls，不精确）。
- 留仙大道 placebo：policy-like −3.4 km/h，opposite −0.9 km/h（medium controls，置信区间均跨 0）。

### Step 3: corridor 打包

- 三种 package：`core_only`，`core_plus_1km`，`core_plus_3km`（endpoint graph + buffer 扩展）。
- 表：`data/corridor_package_segment_catalog.csv`，`metrics/corridor_package_window_summary.csv`。
- 图：`figures/summary/corridor_package_active_window_comparison.png`。
- 主 outcome：active-window harmonic speed 与 log travel time。

### Step 4: control quality screening

- 非 active 时段 diurnal correlation、reference 速度差、12:00 placebo local jump。
- 表：`metrics/control_quality_screening.csv`。
- 图：`figures/summary/control_quality_inactive_corr_heatmap.png`。
- 彩田路 medium controls 的 inactive correlation 约 0.58–0.76；红岭路 near controls 更低（0.21–0.47），玉平大道 reference 差异常大。

### Step 5: 留仙大道 placebo

- 表：`metrics/liuxian_placebo_summary.csv`。
- 图：`figures/summary/liuxian_pre_shock_placebo.png`。
- 结论：primary tier（medium）下，两个方向 local jump 都不显著，不支持 pre-shock 周存在政策方向独有断点；与 handoff 预期一致。

### 仍待 Step 6

- 彩田路、红岭路真正 pre/post 多周数据购买与 DID。

## Exploration v1 多方案扫描（2026-06-28）

在 `post_only_v1` 上新增 broad exploration 模块，系统尝试 control 距离/相似度、corridor 长度、短窗 shock、多种 specification。

主脚本：

- `data_work/src/shenzhen_multi_tidal/run_multi_tidal_control_exploration.py`

输出：

- `data_work/outputs/shenzhen_multi_tidal_exploration_v1/`

重跑：

```bash
python3 data_work/src/shenzhen_multi_tidal/run_multi_tidal_control_exploration.py
```

### 尝试维度

**Control portfolios（6 种）**

- `refined_v1`：原 v1 近邻 control（0.4–1.3 km）
- `far_1500m` / `far_2000m`：≥1.5 / 2.0 km
- `similar_top3` / `far_similar_top3`：按 inactive-hour 相关 + 距离 + 轴向相似度排序
- `similar_best_single`：每条路只保留 1 个最相似远控

扩展搜索半径 8 km；相似度特征见 `metrics/control_similarity_features.csv`。

**Corridor packages**

- `core_only` / `core_plus_1km` / `core_plus_3km`

**Specifications**

- `local_jump_policy/opposite`：±15/30/45/60 min 窗口起点断点（有 segment cluster SE）
- `active_ref_ddd_speed/logtt`：active 相对 reference 的 treat−control 变化（当前为 daily mean DDD，尚无 SE）
- `asymmetry_active_ref_ddd`：policy/opposite 比值变化的 treat−control

### 主要发现

**1. 远控 + 相似度排序确实能把 control 推远，且 pattern 更可比**

| 路段 | 原 v1 最近 control | 相似度最佳远控 | 距离 | inactive corr |
|------|-------------------|----------------|------|---------------|
| 红岭路 AM | 八卦路 ~459 m | 文锦中路 | 2.7 km | 0.79 |
| 彩田路 PM | 皇岗路 ~395 m | 福龙路 | 3.7 km | 0.95 |
| 留仙大道 PM | 学苑大道 ~185 m | 龙观大道 | 5.2 km | 0.82 |

**2. 短窗 local jump 比长窗 active-ref 更干净，但显著结果很少**

±30 min policy-direction local jump（`core_only`，有 SE）：

- 文锦北路 PM + refined near：+2.8 km/h（p≈0.05 边缘）
- 红岭路 AM + similar_best_single：+5.0 km/h（SE 很大，不显著）
- 彩田路 / 留仙大道：政策方向点估计为负，不支持“提速”

**3. active-ref DDD 容易给出“假阳性”，留仙 placebo 已报警**

留仙大道（pre-shock）在 `far_similar_top3 + core_plus_1km` 下 active-ref speed DDD 仍可达 **+2.15 km/h**。说明：

- 不能把 active-ref corridor DDD 直接当计量证据；
- 远控并不能自动解决混杂，还需 daily FE + 正式 SE，或改用 short-window regression。

**4. 相对更有叙事潜力的结果**

- **红岭路 AM**：`similar_best_single`（文锦中路，2.7 km）active-ref speed DDD **+9.1 km/h**；`refined_v1` **+6.9 km/h**。方向符合“早高峰南行提速”，但当前 DDD 无 SE，且单 control 风险高。
- **彩田路 PM**：远控下 **opposite-direction local jump 显著为负**（反方向变慢）比 policy-direction 正跳更稳健；asymmetry DDD 在 far control 下为正。
- **文锦北路 AM**：`core_plus_3km + far_1500m` active-ref DDD **+7.5 km/h**，但 AM 窗口 treat/control 基线速度差大（~25 km/h），解释风险高。
- **新洲路 PM**：仅 `refined_v1 + core_plus_1km` 给出小幅正 DDD（+1.7 km/h），远控下转负。

**5. 长度选择**

- `core_only` 最贴近真实施行段，符号最稳定；
- `core_plus_1km/3km` 有时放大 treat 变化（文锦北路 AM），有时翻转符号（彩田路、新洲路），适合作为 spillover/网络范围 robustness，不宜作主规格。

### 建议的主线规格（下一步应固化）

1. **主 control**：`far_similar_top3` 或 `similar_best_single`，要求 inactive corr ≥ 0.6 且距离 ≥ 1.5 km。
2. **主 shock 窗口**：±30 min（必要时 ±45 min robustness），policy start 为 event time zero。
3. **主 regression**：

```text
speed_{i d m} = α_i + δ_d + λ_m + β (Treat_i × PostStart_m) + ε_{i d m}
```

segment + date + minute FE，cluster segment；分别估计 policy / opposite。

4. **主 outcome**：segment speed 与 corridor log travel time 并行；report asymmetry（policy/opposite）。
5. **placebo**：留仙大道 + 非政策时段（12:00）+ pre-window bins。
6. **真正 causal**：仍需彩田路/红岭路开通前后多周数据；当前 post-only 只能支撑 directional pattern，不能单独写 opening effect。

### Step 7: 利用运行 shock 重新设计识别

新增 shock 备忘录：

- `data_work/docs/SHENZHEN_TIDAL_OPERATION_SHOCKS_20260628.md`

用户提供两条重要运行信息：

- 文锦北路在 2026 年春节后约一个月未实施，原因为地铁施工。
- 留仙大道在 2026-04-24 晚开通 45 分钟。

后续应优先验证留仙大道 2026-04-24 晚开 45 分钟事件，因为它更接近日内运行时点 shock。文锦北路停实施期也有价值，但与地铁施工绑定，需要施工边界和占道信息，否则会混合潮汐车道停实施和施工冲击。
