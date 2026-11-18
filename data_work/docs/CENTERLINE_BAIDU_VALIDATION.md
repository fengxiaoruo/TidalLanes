# Centerline × 百度 API 验证工作流

所有命令在 **`data_work/`** 目录下执行。

---

## 1. 目的

在 **baseline directed centerline** 体系下，用百度驾车路线 API 获取外部 travel cost，与项目内由 **centerline 加总 speed + length 反推的 travel time** 对照，检验 stage03 推断是否可靠。

这不是 national OD 爬取（见 `NATIONAL_TIDAL_COMMUTING_WORKFLOW.md`），而是 **北京 centerline 走廊级** 的 ground-truth 校验。

**核心问题：**

- 推断的 `travel_time_min = (cl_len_m / 1000) / cl_speed_kmh × 60` 是否与百度在同一路廊、同一时段下的 `duration` 一致？
- 双向（AB / BA）是否呈现与 `centerline asymmetry` 一致的方向差异？
- 连续爬取一周时，推断值与 API 值的偏差是否稳定，还是仅在某些 panel 上失效？

---

## 2. 代码与输出落点

| 内容 | 路径 |
|------|------|
| Python 包 | `data_work/src/centerline_baidu_validation/` |
| 配置示例 | `data_work/config/centerline_baidu_validation_beijing_v1.example.json` |
| 规范输出根 | `data_work/outputs/centerline_baidu_validation_beijing_v1/` |
| 上游 baseline 网络 | `data_work/outputs/manual_centerline_rules_v11/` |
| 推断对照表 | `.../data/centerline_speed_master.parquet` |
| AK | 复用 `src/national_tidal_commuting/ak_local.py` 或 `BAIDU_MAP_AUTH_TOKEN` |
| AK 限速（与 raw 并行） | config `ak_rate_limit` + `outputs/.baidu_ak_global.lock` |

**输出目录结构（与 national 爬取对齐）：**

```
outputs/centerline_baidu_validation_beijing_v1/
  data/
    crawl_manifest.parquet      # 待爬 request 清单（双向 + 途经点）
    crawl_manifest.csv
    api_responses_flat.parquet  # （待实现）展平后的 API 结果
    comparison_table.parquet    # （待实现）推断 vs API
  api/
    crawl_sessions/<run_id>/    # 原始 JSONL + manifest
  metrics/
    crawl_manifest_summary.csv
    validation_summary.csv      # （待实现）
  figures/
    fig_inferred_vs_baidu_*.png # （待实现）
  logs/
```

---

## 3. 百度 API：沿 centerline 固定途经点

### 3.1 推荐接口（与 national 一致）

**DirectionLite 驾车路线规划（v1）**

- 产品：**轻量级路线规划 / DirectionLite API**
- 端点：`GET https://api.map.baidu.com/directionlite/v1/driving`
- 文档：[驾车路线规划 | DirectionLite](https://lbs.baidu.com/faq/api?title=webapi/guide/webservice-lwrouteplanapi/dirve)

**AK 权限（控制台需开通）：**

| 项目 | 要求 |
|------|------|
| 服务 | **DirectionLite → 驾车路线规划**（不是 routematrix v2，也不是 Direction v2，除非后续长 centerline 试验） |
| AK 类型 | 服务端 AK（与 national 爬取相同） |
| 应用状态 | 需 **启用**（status 201 =「APP被用户自己禁用」） |
| 配额 | 全量约 1.2 万/ panel；一周三轮约 25 万次，需确认日配额与 QPS |
| IP 白名单 | 若控制台绑定了 IP，需包含运行机器 |

不需要单独申请「v2 驾车」权限即可跑当前 pipeline；若改用 `direction/v2/driving`（18 途经点），需额外开通 **路线规划服务** 对应权限。

**关键参数：**

| 参数 | 用途 |
|------|------|
| `origin` | 起点，`纬度,经度`（WGS84 时配 `coord_type=wgs84`） |
| `destination` | 终点，同上 |
| `waypoints` | **有序途经点**，`|` 分隔，最多 **10** 个，例如 `40.465,116.314\|40.232,116.352` |
| `tactics` | `0` 常规（FREE 基线）；`2` 躲避拥堵（AM/PM，与 national 一致） |
| `gps_direction` | 起点车头方向 `[0,359]`，用于判断正逆向车道；**反向爬取时需翻转 180°** |
| `coord_type` | 建议 `wgs84`（centerline 由 EPSG:3857 转 WGS84 后写入 manifest） |

**返回字段（对比用）：**

- `result.routes[0].duration`：线路耗时，**秒**
- `result.routes[0].distance`：线路距离，**米**

### 3.2 途经点策略（本项目）

1. 仅 **baseline-kept directed centerline**（`keep_baseline == True`），**全量爬取**（约 11,890 条），不限制为有推断 speed 的子集。
2. 每条 directed 记录一行 manifest（`AB` / `BA` 已是两个方向；**不再**对同一记录做 forward/reverse 重复）。
3. 在 centerline 上按弧长等距抽取中间点（默认最多 10 个，间距 ≥ 120 m），保证路径**沿走廊**而非起终点直线。
4. `gps_direction` 优先用 `centerline_dir_master.bear`。
5. manifest 附带 `has_raw_match`、`has_inferred_speed`：爬取全量，对比分析时再用后者筛选（约 8.3k / scenario）。

### 3.3 更长 centerline 的备选

DirectionLite 仅支持 **10 个途经点**。若后续发现长走廊绑路偏差大，可改用完整版：

- 文档：[驾车路线规划 | Direction API v2](https://lbs.baidu.com/faq/api?title=webapi/webservice-direction/dirve)
- 端点：`https://api.map.baidu.com/direction/v2/driving`
- 最多 **18** 个途经点
- 设置 `intelligent_plan=0`，**禁止** API 自动重排途经点顺序（保持沿 line 的固定顺序）

当前 v1 先用 DirectionLite，与 national 爬取栈一致。

---

## 4. 推断对照口径

**推断侧（已有数据）：**

- 表：`outputs/manual_centerline_rules_v11/data/centerline_speed_master.parquet`
- 按 scenario 聚合（与 `plot_speed_diagnostics.py` 一致）：
  - `cl_speed_kmh = Σ total_dist_m / Σ total_time_h`
  - `travel_time_inferred_min = (cl_len_m / 1000) / cl_speed_kmh × 60`
- Scenario 定义：
  - `FREE_*`：`is_freeflow_2205` × weekday/weekend
  - `AMP_*`：`is_am_peak`（7–9 时）× weekday/weekend
  - `PMP_*`：`is_pm_peak`（17–19 时）× weekday/weekend

**API 侧（待爬）：**

- `travel_time_baidu_min = duration_sec / 60`
- 按 `query_time_local` 归入上述 panel

**比较指标（待实现）：**

- 级别：centerline × direction × scenario
- `diff_min = baidu - inferred`
- `ratio = baidu / inferred`
- 分位数、按 `road_class` / 长度分桶的 MAE 与相关系数

---

## 5. 正式爬取节奏（定时 + 工作日重复）

**锁定口径：**

- 始终 **带途经点**（120 m 间距，DirectionLite v1）；不用 OD-only。
- **第 1 天（d01）**：FREE + AM + PM 全量，且 `steps_info=1`（保存 route path，供空间 QA）。
- **第 2 天起（d02+）**：同样三 panel 全量，但 **`steps_info=0`**（只存 duration/distance，省配额与 JSON 体积）。
- 与推断对比仍用 **`route_bind_ok = distance/length ∈ [0.6, 1.8]`** + `has_inferred_speed`。

**Schedule（北京时间，串行）：**

| Panel | 启动时刻 | tactics | 说明 |
|-------|---------|---------|------|
| FREE | 02:30 | 0 | 深夜常规路线 |
| AM | 07:30 | 2 | 早高峰 |
| PM | 17:30 | 2 | 晚高峰 |

- 每日每 panel **全量 11,890 directed**；session 标签：`formal_week_YYYYMMDD_d01` … `_d05`（或 7 天）。
- 实际覆盖时段以各 session `results_flat.csv` 中 `query_time_local` 的 min/max 为准。
- 配额粗算：`35,670 × 5 days ≈ 17.8 万`（d01 因 steps_info 响应更大，其余日较轻）。

**一键启动：**

```bash
cd data_work
cp scripts/centerline_validation_week.env.example scripts/centerline_validation_week.env
# 编辑 FORMAL_WEEK_LABEL、FORMAL_DAYS、三轮 START_*_HHMM
nohup ./scripts/run_centerline_validation_week.sh \
  >> outputs/centerline_baidu_validation_beijing_v1/logs/formal_week_nohup.out 2>&1 &
```

- 主日志：`outputs/centerline_baidu_validation_beijing_v1/logs/${FORMAL_WEEK_LABEL}_master.log`
- 优雅停止（当前 panel 跑完后）：`touch outputs/centerline_baidu_validation_beijing_v1/logs/${FORMAL_WEEK_LABEL}.stop`
- **勿再用** `run_centerline_validation_continuous.sh` 做正式爬取（无墙钟对齐）。

---

## 6. 当前进度与下一步

### 已完成（2026-05-25）

- [x] 模块落点：`src/centerline_baidu_validation/`
- [x] 工作流文档（本文件）
- [x] 配置示例 + 北京 v1 配置
- [x] `build_crawl_manifest.py`：全量 11,890 directed + 途经点 manifest
- [x] `query_baidu_direction.py`、`compare_waypoint_routing.py`、`run_preflight_tests.py`
- [x] **Preflight 测试**（AK、manifest、coord_type、三 panel smoke）→ 汇总见  
  `outputs/centerline_baidu_validation_beijing_v1/metrics/PREFLIGHT_SUMMARY.md`

### Preflight 结论（摘要）

| 项 | 结论 |
|----|------|
| AK / API | 可用，`status=0` |
| `coord_type` | **锁定 `wgs84`**（勿用 gcj02 传 WGS84 坐标） |
| 途经点绑路 | 分层样本约 **40%** `distance/length ≤ 2`；需事后 QC |
| 正式 crawl | **可启动**；对比分析时用 `route_bind_ok = distance/length ∈ [0.6, 1.8]` 筛选 |

Smoke 测试勿用 manifest **前 N 行**（前几行绑路极差）；用 `run_preflight_tests.py` 或随机/分层样本。

### 待实现（按顺序）

1. **`flatten_api_responses.py`**：JSONL → `api_responses_flat.parquet`。
2. **`build_comparison_table.py`**：合并推断 travel time 与 API duration，含 `route_bind_ok` QC 列。
3. **`plot_validation_diagnostics.py`**：fine-bin histogram、Bland–Altman、ratio 散点。
4. [x] **`scripts/run_centerline_validation_week.sh`**：正式周爬（定时串行 + d01 路径几何）。
5. （建议）正式全量前：对 `has_inferred_speed` 分层抽 100–200 条估计 QC 通过率。

---

## 7. 命令

### 7.1 复制配置

```bash
cd data_work
cp config/centerline_baidu_validation_beijing_v1.example.json \
   config/centerline_baidu_validation_beijing_v1.json
```

### 7.2 生成 manifest（smoke test）

```bash
python -m src.centerline_baidu_validation.build_crawl_manifest \
  --config config/centerline_baidu_validation_beijing_v1.example.json \
  --limit 20
```

### 7.3 生成全量 manifest

```bash
python -m src.centerline_baidu_validation.build_crawl_manifest \
  --config config/centerline_baidu_validation_beijing_v1.json
```

### 7.4 正式跑前 preflight 套件

```bash
# 1) coord_type × waypoints × gps 矩阵（推荐 sample-size 10）
python -m src.centerline_baidu_validation.run_preflight_tests \
  --config config/centerline_baidu_validation_beijing_v1.json \
  --panel FREE --sample-size 10 --sleep-sec 0.25

# 2) 正式爬取栈 smoke（三 panel）
python -m src.centerline_baidu_validation.query_baidu_direction \
  --config config/centerline_baidu_validation_beijing_v1.json \
  --panel FREE --limit 5 --require-waypoints

python -m src.centerline_baidu_validation.query_baidu_direction \
  --config config/centerline_baidu_validation_beijing_v1.json \
  --panel AM --limit 3 --require-waypoints

python -m src.centerline_baidu_validation.query_baidu_direction \
  --config config/centerline_baidu_validation_beijing_v1.json \
  --panel PM --limit 3 --require-waypoints
```

汇总报告：`outputs/centerline_baidu_validation_beijing_v1/metrics/PREFLIGHT_SUMMARY.md`

### 7.5 途经点 A/B 对照（smoke）

同一条 centerline 各请求两次：**带 waypoints** vs **仅起终点**，比较 `distance`/`duration` 与 `length_m` 的比值：

```bash
python -m src.centerline_baidu_validation.compare_waypoint_routing \
  --config config/centerline_baidu_validation_beijing_v1.json \
  --panel FREE --limit 5 --require-waypoints --steps-info
```

输出：`metrics/waypoint_ab_<panel>_<timestamp>/waypoint_ab_wide.csv`

读表：`distance_over_length_with_waypoints` 接近 1 表示路径长度与 centerline 几何长度较一致；若 `without_waypoints` 明显更短，说明起终点直线发生了抄近道。

> **注意**：`--limit N` 取 manifest 前 N 行，前几行绑路可能极差；preflight 判定应看 `run_preflight_tests` 的分层样本，而非 head 行。

### 7.6 QC 通过率估计（分层样本 crawl）

```bash
python -m src.centerline_baidu_validation.query_baidu_direction \
  --config config/centerline_baidu_validation_beijing_v1.json \
  --panel FREE \
  --only-inferred-speed --require-waypoints \
  --stratified-sample --sample-size 150 \
  --label qc_sample150

python -m src.centerline_baidu_validation.report_qc_sample \
  --config config/centerline_baidu_validation_beijing_v1.json \
  --session-dir api/crawl_sessions/<FREE_*_qc_sample150> \
  --label qc_sample150
```

2026-05-25 结果（n=150）：API 100%；loose QC **30.7%**；推算 inferred+wp 池 ~**2,339 / 7,627** 可用于对比。详见 `metrics/qc_sample_*/QC_SAMPLE_REPORT.md`。

### 7.7 记录 API 实际路线 geometry（手动校对）

爬取时默认 `steps_info=1`，百度返回每段 `steps[].path`（`lng,lat;...`），可拼成完整 LineString。

**爬取**（config 中 `"steps_info": 1` 已默认开启）：

```bash
python -m src.centerline_baidu_validation.query_baidu_direction \
  --config config/centerline_baidu_validation_beijing_v1.json \
  --panel FREE --limit 10
```

`results_flat.csv` 含 `route_path_wkt`、`n_path_points`；完整 JSON 在 `responses.jsonl`。

**导出 GeoParquet / GeoJSON（叠加 centerline）**：

```bash
python -m src.centerline_baidu_validation.export_route_geometries \
  --config config/centerline_baidu_validation_beijing_v1.json \
  --session-dir api/crawl_sessions/<run_id>
```

输出：`data/route_qa_api_routes_<run_id>.parquet`、`route_qa_paired_<run_id>.geojson`（≤5000 行时）。

**坐标注意**：centerline 为 WGS84；API path 坐标系随 `coord_type` / `ret_coordtype`（当前 config 为 `wgs84` 输入、`ret_coordtype=gcj02` 输出），QGIS 叠加时若偏移 ~100–500 m 需做 GCJ↔WGS 转换。

---

## 8. 与现有文档的关系

- National OD 爬取：`docs/NATIONAL_TIDAL_COMMUTING_WORKFLOW.md`
- 爬取定时：`docs/NATIONAL_CRAWL_SCHEDULING.md`
- Centerline speed 推断：`docs/STAGE_IO_SPEC.md`（`centerline_speed_master.parquet`）
- Raw 路网并行验证：`docs/RAW_BAIDU_VALIDATION.md`
- 推断 travel time 诊断图：`src/diagnostics/plot_speed_diagnostics.py`

---

## 9. 开放决策

- [ ] v1 是否只用 DirectionLite（10 途经点），还是长 centerline 子样本试跑 Direction v2？
- [ ] 一周爬取是 **全量 daily** 还是 **分层子样本 + 全量首末日**？
- [ ] 比较时 API `distance` 是否与 `cl_len_m` 先做绑路偏差过滤？
- [ ] 推断侧用 **scenario 聚合 speed** 还是 **逐小时 manifest 对齐**（更严但更窄）？
