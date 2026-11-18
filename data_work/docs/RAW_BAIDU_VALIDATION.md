# Raw 路网 × 百度 API 验证工作流

所有命令在 **`data_work/`** 目录下执行。

---

## 1. 目的

在 **baseline raw segment**（底层路网）体系下，用百度驾车路线 API 获取 travel cost，与项目内 **百度 speed CSV 直接观测** 反推的 travel time 对照。

与 centerline 验证（`CENTERLINE_BAIDU_VALIDATION.md`）平行运行，但：

| 维度 | Centerline 验证 | Raw 验证（本工作流） |
|------|----------------|---------------------|
| 几何单元 | `centerline_dir_master` (~11,890) | `raw_segment_master` (~23,799) |
| 对照速度 | `centerline_speed_master` 聚合推断 | `speed_Beijing_all_wgs84.csv` 路段观测 |
| 输出根 | `outputs/centerline_baidu_validation_beijing_v1/` | `outputs/raw_baidu_validation_beijing_v1/` |
| 对比问题 | stage03 centerline 推断是否可靠 | API 路由 time/speed 与直接观测差多大 |

**核心问题：**

- 沿 raw segment 加途经点请求 API，`duration` 与 `(length_m / direct_speed_kmh) × 60` 差多少？
- 与 centerline 验证相比，底层路网是否绑路更准、偏差更小？
- 保存的实际路径 geometry 与 raw 几何是否一致？

---

## 2. 代码与输出落点

| 内容 | 路径 |
|------|------|
| Python 包 | `data_work/src/raw_baidu_validation/` |
| 配置 | `data_work/config/raw_baidu_validation_beijing_v1.json` |
| 规范输出根 | `data_work/outputs/raw_baidu_validation_beijing_v1/` |
| 上游 baseline | `data_work/outputs/manual_centerline_rules_v11/` |
| 直接速度 CSV | `raw_data/speed_Beijing_all_wgs84.csv` |
| AK 限速（与 centerline 共用） | `outputs/.baidu_ak_global.lock` |

**输出目录结构：**

```
outputs/raw_baidu_validation_beijing_v1/
  data/
    crawl_manifest.parquet
    crawl_manifest.csv
  api/
    crawl_sessions/<panel>_<timestamp>_<label>/
      responses.jsonl
      results_flat.csv        # 含 route_path_wkt（d01）
      run_manifest.json
  metrics/
    crawl_manifest_summary.csv
  logs/
    formal_week_YYYYMMDD_master.log
```

---

## 3. 途经点与路径保存

与 centerline 验证相同策略：

1. 仅 `keep_baseline == True` 的 raw segment，**全量**进入 manifest。
2. 沿 segment 弧长等距抽取中间点（最多 10 个，间距 ≥ 120 m）。
3. `gps_direction` 来自 `dir_deg_final`（fallback `bear_geom`）。
4. manifest 附带 `has_direct_speed`（roadseg_id 在 speed CSV 有观测）、`matched_centerline`（与 centerline 交叉引用）。

**路径保存：**

- d01：`steps_info=1`，`results_flat.csv` 含 `route_path_wkt`、`n_path_points`
- d02+：`steps_info=0`，只存 duration/distance

---

## 4. AK 频率控制（与 centerline 并行）

两套爬取共用同一 AK 时，**必须**启用全局限速：

```json
"ak_rate_limit": {
  "enabled": true,
  "min_interval_sec": 0.35,
  "lock_path": "data_work/outputs/.baidu_ak_global.lock"
}
```

- centerline 与 raw 的 `query_baidu_direction` 在每次 API 请求前都会 `wait_for_ak_slot`
- 实现：`src/national_tidal_commuting/baidu_rate_limit.py`
- 建议两边 `sleep_sec` 均 ≥ `min_interval_sec`（默认 0.35s）
- 粗算：raw 全量 ~23,799 × 3 panel ≈ 7.1 万/天；与 centerline ~3.6 万/天并行时，合并峰值约 2.8 req/s

**centerline config 也需开启**（已在 `centerline_baidu_validation_beijing_v1.json` 同步配置）。

---

## 5. 正式爬取节奏

与 centerline 相同（北京时间，串行 FREE → AM → PM）：

| Panel | 启动时刻 | tactics |
|-------|---------|---------|
| FREE | 02:30 | 0 |
| AM | 07:30 | 2 |
| PM | 17:30 | 2 |

**一键启动（明天起）：**

```bash
cd data_work

# 1) 先生成全量 manifest（只需一次）
python -m src.raw_baidu_validation.build_crawl_manifest \
  --config config/raw_baidu_validation_beijing_v1.json

# 2) 复制 env 并启动正式周
cp scripts/raw_validation_week.env.example scripts/raw_validation_week.env
# 编辑 FORMAL_WEEK_LABEL=formal_week_20260528 等
nohup ./scripts/run_raw_validation_week.sh \
  >> outputs/raw_baidu_validation_beijing_v1/logs/formal_week_nohup.out 2>&1 &
```

- 主日志：`outputs/raw_baidu_validation_beijing_v1/logs/${FORMAL_WEEK_LABEL}_master.log`
- 停止：`touch outputs/raw_baidu_validation_beijing_v1/logs/${FORMAL_WEEK_LABEL}.stop`

---

## 6. 命令

### 6.1 生成 manifest

```bash
# smoke
python -m src.raw_baidu_validation.build_crawl_manifest \
  --config config/raw_baidu_validation_beijing_v1.example.json \
  --limit 20

# 全量
python -m src.raw_baidu_validation.build_crawl_manifest \
  --config config/raw_baidu_validation_beijing_v1.json
```

### 6.2 Preflight smoke

```bash
python -m src.raw_baidu_validation.query_baidu_direction \
  --config config/raw_baidu_validation_beijing_v1.json \
  --panel FREE --limit 5 --require-waypoints --steps-info --label smoke_wp

python -m src.raw_baidu_validation.query_baidu_direction \
  --config config/raw_baidu_validation_beijing_v1.json \
  --panel AM --limit 3 --require-waypoints --steps-info --label smoke_am
```

### 6.3 分层 QC 样本（可选）

```bash
python -m src.raw_baidu_validation.query_baidu_direction \
  --config config/raw_baidu_validation_beijing_v1.json \
  --panel FREE \
  --only-direct-speed --require-waypoints \
  --stratified-sample --sample-size 150 \
  --steps-info --label qc_sample150
```

---

## 7. 对照口径（待实现 comparison 脚本）

**直接观测侧：**

- 表：`raw_data/speed_Beijing_all_wgs84.csv`
- 按 scenario 聚合 roadseg_id speed（与 stage03 / centerline 相同 panel 定义）
- `travel_time_direct_min = (length_m / 1000) / direct_speed_kmh × 60`

**API 侧：**

- `travel_time_baidu_min = duration_sec / 60`
- 按 `query_time_local` 归入 FREE / AM / PM panel

**QC：**

- `route_bind_ok = distance_m / length_m ∈ [0.6, 1.8]`
- 对比子集：`route_bind_ok & has_direct_speed`

---

## 8. 与 centerline 验证的关系

- 并行爬取，共用 AK 限速锁
- 结果目录完全分离，便于一天后对比
- manifest 含 `skel_dir/cline_id/dir` 交叉引用，可事后做 segment-centerline 配对分析
- centerline 文档：`docs/CENTERLINE_BAIDU_VALIDATION.md`

---

## 9. 待实现

1. `build_comparison_table.py`：合并 direct speed 与 API duration
2. `plot_validation_diagnostics.py`：推断 vs API 诊断图
3. `export_route_geometries.py`：raw segment + API path 叠加导出
