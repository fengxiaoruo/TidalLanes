# Shenzhen Grid-Level Pair-Time Audit, 2026-06-28

用途：检查 `/data_work/raw_data/shenzhen_gridlevel/fishnet1000_pairtime_hat.csv` 的结构，并和当前项目里的深圳底层数据做口径对照。这个文件是 raw data，只读取，不修改。

## 1. 数据位置和输出

原始文件：

```text
data_work/raw_data/shenzhen_gridlevel/fishnet1000_pairtime_hat.csv
```

审计脚本：

```text
data_work/src/shenzhen_gridlevel/analyze_gridlevel_pairtime.py
```

输出目录：

```text
data_work/outputs/shenzhen_gridlevel_pairtime_v1/
```

主要输出：

- `metrics/structure_summary.json`
- `metrics/consistency_summary.json`
- `metrics/directionality_summary.json`
- `metrics/compatibility_with_existing_data.json`
- `metrics/time_distance_summary.csv`
- `data/variable_dictionary.csv`
- `data/top_reciprocal_asymmetry_pairs.csv`
- `figures/travel_time_distribution.png`
- `figures/directionality_ratio_distribution.png`
- `figures/detour_time_penalty_distribution.png`

## 2. 基本结构

`fishnet1000_pairtime_hat.csv` 是一个完整 grid-pair OD travel-time matrix。

| 指标 | 数值 |
|---|---:|
| file size | 171.4 MB |
| rows | 2,927,521 |
| columns | 8 |
| unique `fid_home` | 1,711 |
| unique `fid_work` | 1,711 |
| full Cartesian over present IDs | true |
| duplicate OD pairs | 0 |
| self pairs | 1,711 |
| missing values | 0 |

行数正好等于：

```text
1711 * 1711 = 2927521
```

所以这个文件不是实际发生的通勤 OD sample，而是所有 present fishnet1000 grid pairs 的 travel-time table。

## 3. 字段含义

| 变量 | 解释 | 备注 |
|---|---|---|
| `fid_home` | origin/home fishnet1000 grid id | 只有 numeric id，没有附带 geometry 或坐标 |
| `fid_work` | destination/work fishnet1000 grid id | 和 `fid_home` 组成完整 OD 矩阵 |
| `car_time` | baseline car travel time | 单位从 implied speed 判断为分钟；提供方建议作为主分析口径 |
| `car_dist` | baseline car travel distance | 单位从 implied speed 判断为公里 |
| `car_time_detour` | detour-adjusted car travel time | 总是弱大于 `car_time`；目前没有 codebook 说明其精确定义，建议只作 robustness / diagnostic |
| `car_dist_detour` | detour-adjusted car travel distance | 有一部分小于 `car_dist`，不是简单的 max-distance rule |
| `time_diff` | `car_time_detour - car_time` | 和计算值一致，误差在浮点精度内 |
| `dist_diff` | `car_dist_detour - car_dist` | 和计算值一致，可有很小负值 |

## 4. Travel-time 和 detour 诊断

非 self OD pairs 的核心分布：

| 变量 | mean | median | p75 | p90 | p95 | p99 |
|---|---:|---:|---:|---:|---:|---:|
| `car_time` | 57.19 | 54.25 | 73.07 | 91.72 | 102.90 | 122.10 |
| `car_time_detour` | 60.44 | 58.77 | 77.20 | 94.48 | 104.92 | 123.27 |
| `time_diff` | 3.25 | 0.00 | 1.87 | 13.92 | 19.83 | 30.37 |
| `car_dist` | 41.26 | 37.85 | 55.77 | 72.86 | 84.19 | 100.20 |
| `car_dist_detour` | 42.67 | 39.75 | 57.78 | 74.24 | 84.86 | 100.24 |

一致性检查：

- `time_diff` 与 `car_time_detour - car_time` 的最大绝对残差为 `9e-6`。
- `dist_diff` 与 `car_dist_detour - car_dist` 的最大绝对残差为 `4e-6`。
- `car_time_detour < car_time` 的行数为 0。
- `car_dist_detour < car_dist` 的行数为 285,304。
- 非 self OD 中，`time_diff > 0` 的比例为 26.77%。
- 非 self OD 中，`dist_diff > 0` 的比例为 41.14%，`dist_diff < 0` 的比例为 11.63%。

解释：`detour` 版本在时间上确实是更弱约束或更慢的 travel-time measure，但距离并不总是更长。这说明 detour route 的构造不是简单地给 baseline route 加长度，它可能涉及另一套 route selection 或约束规则。由于提供方明确建议使用 `car_time`，后续主分析应以 `car_time` 为准，`car_time_detour` 只作为 robustness / diagnostic。

## 5. Directionality 测试

所有 OD 行都有 reverse pair。即如果有 `home -> work`，也有 `work -> home`。

为了避免极小时间污染 ratio，方向性统计只使用双向 travel time 都大于等于 1 分钟的 reciprocal pairs。

### 5.1 Baseline car time asymmetry

| 指标 | 数值 |
|---|---:|
| valid reciprocal pairs | 1,462,815 |
| mean ratio | 1.201 |
| median ratio | 1.137 |
| p75 ratio | 1.271 |
| p90 ratio | 1.453 |
| p95 ratio | 1.596 |
| p99 ratio | 1.975 |
| share ratio >= 1.10 | 60.93% |
| share ratio >= 1.25 | 27.82% |
| share ratio >= 1.50 | 7.90% |
| share ratio >= 2.00 | 0.91% |

### 5.2 Detour car time asymmetry

| 指标 | 数值 |
|---|---:|
| valid reciprocal pairs | 1,462,821 |
| mean ratio | 1.181 |
| median ratio | 1.126 |
| p75 ratio | 1.244 |
| p90 ratio | 1.401 |
| p95 ratio | 1.526 |
| p99 ratio | 1.871 |
| share ratio >= 1.10 | 58.32% |
| share ratio >= 1.25 | 24.07% |
| share ratio >= 1.50 | 5.75% |
| share ratio >= 2.00 | 0.61% |

### 5.3 直觉

这个数据本身已经能支持一个很直接的 fact：

```text
grid-level OD travel times are directionally asymmetric for a large share of Shenzhen grid pairs.
```

和之前北京路段级别的 directionality 不一样，这里是不经过路段匹配的 grid-to-grid route travel-time asymmetry。因此它更适合做 model-side AB test，例如 symmetric vs directed OD cost matrix。

## 6. 和当前底层数据的差别

### 6.1 和 `resid_work_flow_grid500.dta` 的差别

当前底层通勤流数据：

```text
data_work/raw_data/resid_work_flow_grid500.dta
```

它是实际发生的 OD flow 和人群属性数据，不是完整 OD travel-time matrix。

| 维度 | fishnet1000 pair-time | existing grid500 flow |
|---|---|---|
| 空间单元 | `fid_home/fid_work`, 1711 ids | `resi_all_id/work_all_id`, 6094+ ids |
| 行 | all possible OD pairs | observed OD pairs |
| 行数 | 2,927,521 | 1,338,147 |
| flow/person attributes | no | yes |
| coordinates | no | yes, WGS residence/workplace lon-lat |
| county/econzone | no | yes |
| time/date | no | no route time, but has commute-flow vintage |
| direct merge by numeric id | no | baseline id system |

最重要的兼容性测试：

- `grid500` valid flow total: 5,143,621。
- 如果把新 `fid` 和旧 `all_id` 直接按数字硬 merge，matched flow 只有 230,339。
- matched share of valid flow = 4.48%。
- matched share of valid car-asset count = 4.36%。

这个比例太低，说明不能把 `fishnet1000 fid` 当成旧 `grid500 all_id` 使用。需要一个 fishnet1000 geometry 或 crosswalk，才能把 travel-time matrix 接到现有 OD flow 上。

### 6.2 和 `speed_shenzhen_all_wgs84.csv` 的差别

当前深圳底层速度面：

```text
data_work/raw_data/speed_shenzhen_all_wgs84.csv
```

它是路段级速度 panel，不是 grid OD matrix。

| 维度 | fishnet1000 pair-time | speed_shenzhen_all_wgs84 |
|---|---|---|
| 空间对象 | grid-pair OD | directed road segment |
| 时间 | static, no timestamp | 7 days, 1008 ten-minute timestamps |
| 行数 | 2,927,521 | 10,435,104 |
| 对象数 | 1711 grid ids | 10433 road segments |
| 变量 | travel time and distance | speed, day, hour, road geometry |
| direction | OD direction | road-segment semantic direction |
| 能否看高峰变化 | no | yes |
| 能否直接做 grid OD time | yes | no, unless route aggregation is built |

因此，两者回答的问题不同：

- `fishnet1000_pairtime_hat.csv` 适合回答 grid OD 成本矩阵的方向性。
- `speed_shenzhen_all_wgs84.csv` 适合回答路段速度的时段性、政策窗口、上下游传播和路段方向性。

## 7. 现阶段可以做的 test

### Test A：grid OD directionality prevalence

已经完成。核心结论是 detour travel time 的 median reciprocal ratio 为 1.126，58.32% 的 reciprocal OD pairs 超过 1.10。

### Test B：symmetric-cost AB test

可以直接用这个数据构造：

```text
A: tau_ij = car_time_ij
B: tau_ij = tau_ji = sqrt(tau_ij * tau_ji)
```

这不需要路段底层数据，也不需要 fundamentals。如果只是做 exact-hat-compatible 的 cost-matrix test，可以先用 uniform OD weights 或 distance-bin weights。若要做真实通勤权重，必须先有 fishnet1000 to grid500/portrait-grid crosswalk。`car_time_detour` 可以作为同一 AB test 的 robustness。

### Test C：detour sensitivity

比较两套 cost：

```text
baseline cost = car_time
detour cost = car_time_detour
```

可以测试 constrained route 或 detour-adjusted route 对 directionality 的影响。当前结果显示 detour 版本的 asymmetry 略低于 baseline car time，但仍然很强。

### Test D：distance vs time asymmetry

距离 asymmetry 明显小于时间 asymmetry：

- detour time median ratio = 1.126。
- detour distance median ratio = 1.037。

这说明方向性不只是两方向路线长度不同，也反映速度、转向、路网约束或 route impedance 的差异。

### Test E：和真实 OD flow 加权的 test

暂时不能直接做。当前缺少 fishnet1000 geometry/crosswalk。直接按 numeric id merge 会只覆盖 4.48% 的 valid flow，不可用。

## 8. 下一步

1. 找到或生成 `fishnet1000 fid -> geometry`。
   - 没有这个 crosswalk，新数据只能做 unweighted OD cost-matrix test。
   - 有了 geometry 后，可以和 500m commute flow、通勤画像、county/econzone 做 spatial join。

2. 做 Shenzhen grid-level symmetric-cost AB test。
   - 先用 unweighted all-pair matrix。
   - 再按 distance bins 或 plausible OD weights 做 robustness。

3. 如果拿到 crosswalk，把新 travel-time matrix 接到真实通勤流。
   - 计算 observed-flow weighted directionality。
   - 对比 car-owner-weighted directionality。
   - 进一步做 sorting / market-access AB test。

4. 和路段速度面做互补，而不是替代。
   - grid matrix 用于 OD-level cost asymmetry。
   - road speed panel 用于 peak-period dynamics、policy windows 和 upstream/downstream propagation。
