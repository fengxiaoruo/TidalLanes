# Formal week 数据验证报告

- 生成时间: 2026-06-03T13:36:22+08:00
- 当前 crawl 标签: `formal_week_20260527`
- 对照 crawl: `无`

## 0. 纳入 session

| Day | Panel | Session | n | api_ok |
|-----|-------|---------|---|--------|
| d01 | AM | `AM_20260527T073000_formal_week_20260527_d01` | 11,890 | 100.0% |
| d01 | FREE | `FREE_20260527T023000_formal_week_20260527_d01` | 11,890 | 100.0% |
| d01 | PM | `PM_20260527T173000_formal_week_20260527_d01` | 11,890 | 100.0% |
| d02 | AM | `AM_20260528T221816_formal_week_20260527_d02_e71582` | 11,890 | 100.0% |
| d02 | FREE | `FREE_20260528T111616_formal_week_20260527_d02_sched` | 11,890 | 100.0% |
| d02 | PM | `PM_20260528T173000_formal_week_20260527_d02_sched` | 11,890 | 100.0% |
| d03 | AM | `AM_20260529T073000_formal_week_20260527_d03_sched` | 11,890 | 100.0% |
| d03 | FREE | `FREE_20260529T023000_formal_week_20260527_d03_sched` | 11,890 | 100.0% |
| d03 | PM | `PM_20260529T173001_formal_week_20260527_d03_sched` | 11,890 | 100.0% |
| d04 | AM | `AM_20260530T073000_formal_week_20260527_d04_sched` | 11,890 | 100.0% |
| d04 | FREE | `FREE_20260530T023000_formal_week_20260527_d04_sched` | 11,890 | 100.0% |
| d04 | PM | `PM_20260530T173000_formal_week_20260527_d04_sched` | 11,890 | 100.0% |
| d05 | AM | `AM_20260531T073000_formal_week_20260527_d05_sched` | 11,890 | 100.0% |
| d05 | FREE | `FREE_20260531T023000_formal_week_20260527_d05_sched` | 11,890 | 100.0% |
| d05 | PM | `PM_20260531T140310_formal_week_20260527_d05_e003` | 11,890 | 100.0% |

## 1. 爬取完成度与 bind QC

| Panel | 行数 | API 成功率 | median ratio | bind_ok 占比 | bind∩inferred n | time ratio median | r(time) |
|-------|------|------------|--------------|--------------|-----------------|-------------------|---------|
| FREE | 59450 | 100.00% | 4.20 | 31.0% | 15260 | 1.46 | 0.92 |
| AM | 59450 | 100.00% | 4.23 | 31.0% | 15258 | 1.48 | 0.88 |
| PM | 59450 | 100.00% | 4.26 | 31.0% | 15233 | 1.53 | 0.89 |

## 1b. 按 crawl day 分解

| Day | Panel | bind_ok | time ratio med | r(time) |
|-----|-------|---------|----------------|---------|
| d01 | AM | 31.0% | 1.64 | 0.87 |
| d01 | FREE | 31.0% | 1.45 | 0.92 |
| d01 | PM | 31.0% | 1.60 | 0.87 |
| d02 | AM | 31.1% | 1.26 | 0.89 |
| d02 | FREE | 31.0% | 1.67 | 0.92 |
| d02 | PM | 31.0% | 1.59 | 0.89 |
| d03 | AM | 31.0% | 1.61 | 0.88 |
| d03 | FREE | 31.0% | 1.39 | 0.93 |
| d03 | PM | 31.0% | 1.58 | 0.88 |
| d04 | AM | 31.0% | 1.48 | 0.90 |
| d04 | FREE | 31.1% | 1.39 | 0.93 |
| d04 | PM | 31.0% | 1.48 | 0.89 |
| d05 | AM | 31.0% | 1.44 | 0.90 |
| d05 | FREE | 31.1% | 1.40 | 0.91 |
| d05 | PM | 31.0% | 1.44 | 0.91 |

## 1c. 跨日稳定性（bind_ok∩inferred）

| Panel | n (≥2 days) | median CV | share CV≤0.15 | median max/min |
|-------|-------------|-----------|---------------|----------------|
| AM | 3,054 | 0.125 | 59.3% | 1.42 |
| FREE | 3,063 | 0.113 | 65.7% | 1.36 |
| PM | 3,051 | 0.099 | 70.4% | 1.32 |

## 2. 与推断 travel time 对比

Panel 对齐: FREE→FREE_WORK, AM→AMP_WORK, PM→PMP_WORK（weekday 聚合推断）。

- **FREE**: bind_ok∩inferred 中 ratio∈[0.8,1.25] 占 **31.1%**；MAE **1.38 min**
- **AM**: bind_ok∩inferred 中 ratio∈[0.8,1.25] 占 **28.5%**；MAE **1.78 min**
- **PM**: bind_ok∩inferred 中 ratio∈[0.8,1.25] 占 **27.8%**；MAE **1.91 min**

## 4. 路线几何 vs centerline

- 空间抽样 n=2500（GCJ→WGS 校正后，50m buffer）
- spatial PASS（mean_dist≤80m & CL overlap≥50%）: **29.4%**
- bind_ok 子集 spatial PASS: **24.7%** (n=1250)
- CL overlap median: **0.05**
- sym_overlap median: **0.01**
- mean_dist median: **106.9 m**

## 5. 图表

- 指标 CSV: `/Users/fxr/Dropbox/Projects/TidalLanes/data_work/outputs/centerline_baidu_validation_beijing_v1/metrics/formal_week_validation_formal_week_20260527_20260603T133612`
- 图表目录: `/Users/fxr/Dropbox/Projects/TidalLanes/data_work/outputs/centerline_baidu_validation_beijing_v1/figures/formal_week_validation_formal_week_20260527_20260603T133612`

| 文件 | 说明 |
|------|------|
| fig_dist_ratio_by_panel.png | distance/length 分布 |
| fig_baidu_travel_time_hist_by_panel.png | Baidu 行程时间分布 |
| fig_baidu_vs_inferred_scatter.png | Baidu vs 推断 scatter |
| fig_bland_altman_by_panel.png | Bland–Altman |
| fig_time_ratio_hist.png | time ratio 分布 |
| fig_speed_vs_inferred_scatter.png | 速度 scatter |
| fig_cross_crawl_scatter.png | 与先前 crawl 对照 |
| fig_spatial_overlap_hist.png | 空间 overlap 分布 |
| fig_spatial_mean_dist_hist.png | CL→route 平均距离 |
| fig_spatial_scatter_overlap_vs_ratio.png | overlap vs bind ratio |
| fig_day_panel_bind_ok.png | 正式周 bind_ok 按日变化 |