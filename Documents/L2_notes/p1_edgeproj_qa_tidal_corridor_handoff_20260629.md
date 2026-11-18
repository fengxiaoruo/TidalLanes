# P1 结论记录：edgeproj QA、简约式校准、潮汐重跑、走廊选择框架

**日期：** 2026-06-29（**主线更新：** λ=1.1 anchored 为默认；见 `data_work/docs/STAGE2_MAINLINE_GATE5.md`）  
**Travel cost 口径：** `raw_connected_v1` → `edgeproj_v2` → `model_input_edgeproj_v2_lcc_gate5_v1`（**非 centerline**）  
**前置 P0 输出：** `data_work/outputs/congestion_reduced_form_gate5_v1/`

---

## 0. 主线决策（2026-06-29）


| 项目            | 主规格                                                    | 稳健性                                   |
| ------------- | ------------------------------------------------------ | ------------------------------------- |
| E6 形式         | **anchored**                                           | legacy / density                      |
| `lambda_phys` | **1.1**（文献 documented）                                 | **0.572**（P0 截面局部斜率）                  |
| 潮汐选边          | **asym**（默认）                                           | `score_st` / `score_lt`               |
| Stage 2 输出    | `stage2_endogenous_edgeproj_v2_lcc_gate5_anchored_v1/` | 旧 `..._gate5_v1/` = legacy diagnostic |


**理由：** λ=1.1 与 0.57 **方向一致、幅度约 2×**；保留 1.1 使 CF 效应更可见，且与半锁定文献（先锁水平、再估斜率）一致。

---

## 1. P1a — Pipeline τ QA（edgeproj 内部 + 百度分布对照）

**脚本：** `data_work/src/model/full_estimation/run_edgeproj_pipeline_qa_gate5.py`  
**输出：** `data_work/outputs/edgeproj_pipeline_qa_gate5_v1/summary.json`

### 结论


| 检查项                                         | 结果                                            |
| ------------------------------------------- | --------------------------------------------- |
| Bundle ↔ edgeproj 源 CSV                     | **完全一致**（2642 边，`tau_obs`/`tau_ff` 最大差 0 min） |
| Gate5 median `c = τ_obs/τ_ff`               | **1.149**（+14.9% vs free flow）                |
| 百度 API（bind_ok，AM/FREE 成对 skeleton）median c | **1.218**                                     |
| Centerline inferred（同批 skeleton）median c    | **1.080**                                     |


**数据直觉：**

1. **结构模型应继续用 gate5 edgeproj** — 内部无漂移，与源矩阵 1:1 锁定。
2. **百度 API 的 peak/free 比比 gate5 高约 5–7 pp** — 单位不同（centerline skeleton vs grid pair routing），只能做**分布级**对照，不能逐边 merge。
3. Gate5 的 c 介于 centerline inferred 与 Baidu API 之间 → edgeproj 路由给出的拥堵幅度**偏保守**，适合作为 structural baseline；API 可作为**上界 robustness**。

---

## 2. P1b — Step1 简约式 → anchored (E6) 校准参数

**校准文件：** `data_work/outputs/p1_congestion_calibration_gate5_v1.json`


| 参数                             | 值       | 用途                         |
| ------------------------------ | ------- | -------------------------- |
| `**lambda_phys_primary`**      | **1.1** | **主线** anchored E6 / 潮汐 CF |
| `lambda_phys_robustness_lower` | 0.572   | 附录稳健性（P0 median 局部弹性）      |
| OLS `coef_ln_inv_n`            | 0.923   | 截面容量信号（不写入 E6）             |


**主线逻辑（半锁定）：** baseline 水平贴 `τ_obs`；`λ=1.1` 仅缩放 CF 中 lane-load 的**相对**响应。P0 的 0.57 作下界 robustness，不作主规格。

---

## 3. P1c — 潮汐模型重跑


| 运行     | 输出目录                                                        | λ       | 角色                                                        |
| ------ | ----------------------------------------------------------- | ------- | --------------------------------------------------------- |
| **主线** | `tidal_lane_diagnostics_edgeproj_v2_lcc_gate5_v1/`          | **1.1** | 默认 `run_tidal_lane_diagnostics_gate5.py`                  |
| 稳健性    | `tidal_lane_diagnostics_edgeproj_v2_lcc_gate5_p1_calib_v1/` | 0.572   | `--p1-calib-json p1_congestion_calibration_gate5_v1.json` |


**主线命令：**

```bash
cd data_work
python3 src/model/full_estimation/run_tidal_lane_diagnostics_gate5.py
python3 src/model/full_estimation/run_stage2_gate5.py
```

对比时读两目录下 `tidal_st_lt_compact.csv`。

### P1 重跑 vs λ=1.1（anchored，同一 top-K 选边）


| spec        | W̄ ST (λ=1.1) | W̄ ST (λ=0.57) | 直接 ST 成本变化 (1.1) | (0.57)   |
| ----------- | ------------- | -------------- | ---------------- | -------- |
| top25_d1.0  | +0.041%       | +0.013%        | −0.121 s         | −0.064 s |
| top50_d1.0  | +0.131%       | +0.050%        | −0.418 s         | −0.252 s |
| top100_d1.0 | +0.285%       | +0.100%        | −0.715 s         | −0.420 s |


**直觉：** 数据校准 λ≈0.57 约为文献 1.1 的一半 → 同样挪车道时，**边级 τ 响应和 ST 通勤成本变化约减半**；W̄ ST 仍为正但幅度缩小。LT sorting wedge 同向缩小。

### 模型 top-100（纯 asym）vs 走廊评分 top-100（ST）

- 1011 条可行候选；ST/LT 各自 top-100 **重叠 71 条**（29% 排序差异来自 OD/车道维度）。
- 模型 top-100 在 ST 评分中 **median rank = 55**；64% 同时落在 ST top-100。
- **无** model top-100 边落在 ST rank > 200 —— 纯 asym 选边未选出“评分很差”的走廊，但会 **忽略** 一些 OD 需求更高、同样不对称的走廊。

---

## 4. 走廊候选评分框架（现实政策筛选，ST/LT 分开）

**脚本：** `data_work/src/model/full_estimation/score_tidal_corridor_candidates.py`  
**输出：** `data_work/outputs/tidal_corridor_candidates_gate5_v1/`

### 评分维度（已实现）


| 维度                        | 含义                     | ST 权重 | LT 权重 |
| ------------------------- | ---------------------- | ----- | ----- |
| 不对称性 `asym_ratio`         | τ_slow / τ_fast        | 0.45  | 0.30  |
| 慢向拥堵 `c_slow`             | τ_obs/τ_ff on slow leg | 0.25  | 0.10  |
| 车道余量 `lane_slack_fast`    | n_fast − Δ − 1         | 0.20  | 0.15  |
| OD 方向对齐 `od_slow/od_fast` | 慢向是否也是高需求向             | 0.10  | 0.20  |
| 走廊总需求 `od_total`          | 两向 OD 之和               | —     | 0.25  |


**硬约束（与 Spec C 一致）：** len ≥ 200 m；fast 向 ≥ 2 lanes；`n_fast − Δ ≥ 1`。

### 尚未入模的政策因素（文档化，待后续）

- 公交/专用道冲突、执法与时段 reversibility  
- 安全/事故史、既有潮汐试点状态  
- 与区级规划、财政、舆情相关的可行性

**ST vs LT 解读：**

- **短期（ST）：** 固定人口份额 → 优先 **高 asym + 慢向已拥堵 + 有足够车道可挪**；OD 对齐是加分项（谁受益）。  
- **长期（LT）：** 加入 **走廊总 OD 需求** → 高流量走廊的 sorting / GE 反馈更大，可能改变 top-K 排序。

当前模型 CF **默认仍用纯 asym**（`--pair-selection asym`）；`score_st` / `score_lt` 可通过 CLI 切换。评分表供现实走廊 shortlist **交叉核对**。

---

## 5. 文件索引


| 用途              | 路径                                                                  |
| --------------- | ------------------------------------------------------------------- |
| **主线说明**        | `data_work/docs/STAGE2_MAINLINE_GATE5.md`                           |
| Stage 2 主线输出    | `outputs/stage2_endogenous_edgeproj_v2_lcc_gate5_anchored_v1/`      |
| P0 reduced-form | `outputs/congestion_reduced_form_gate5_v1/`                         |
| P1 QA           | `outputs/edgeproj_pipeline_qa_gate5_v1/`                            |
| P1 校准 JSON      | `outputs/p1_congestion_calibration_gate5_v1.json`                   |
| 走廊候选            | `outputs/tidal_corridor_candidates_gate5_v1/`                       |
| 潮汐（主线 λ=1.1）    | `outputs/tidal_lane_diagnostics_edgeproj_v2_lcc_gate5_v1/`          |
| 潮汐（λ=0.57 稳健性）  | `outputs/tidal_lane_diagnostics_edgeproj_v2_lcc_gate5_p1_calib_v1/` |
| Stage 2 legacy  | `outputs/stage2_endogenous_edgeproj_v2_lcc_gate5_v1/`               |


---

*数值以各目录 `summary.json` / CSV 为准。*