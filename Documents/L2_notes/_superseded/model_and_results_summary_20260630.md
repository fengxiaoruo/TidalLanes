# 模型与结果系统诊断（gate5 主线 bpr+pos05）

**最后更新：** 2026-07-01  
**用途：** 主规格结果 snapshot + 全部分析包注册表（脚本 / 输入 / 输出路径 / 关键数字）  
**相关工作计划：** `Documents/L2_notes/MAINLINE_WORKPLAN_handoff_20260630.md`  
**代码审计：** `Documents/L2_notes/code_logic_audit_20260630.md`

---

## 0. 主规格（全文默认）


| 维度                    | 锁定值                                                      |
| --------------------- | -------------------------------------------------------- |
| **Travel cost input** | `data_work/outputs/model_input_edgeproj_v2_lcc_gate5_v1` |
| **节点 / period**       | N=1104（gate5 LCC），AM                                     |
| **选边**                | `score_bpr_st`（BPR 慢向 score，需 baseline Ξ）                |
| **参数**                | α=+0.05，β=−0.10                                          |
| **拥堵**                | `anchored`，λ_phys=**1.1**（主）；λ=0.57 仅 Phase A 稳健性行       |
| **潮汐**                | top **100** 对，Δn=**1**（slow +1 / fast −1）                |
| **求解器（正 α）**          | damping=0.10，max_outer=500，tol=1e-4                      |
| **Lbar**              | 88,110 person-equivalents/day                            |


**代码常量：** `equilibrium_solve.py` → `MAINLINE_PAIR_SELECTION`, `MAINLINE_ALPHA`, `MAINLINE_BETA`, `MAINLINE_LAMBDA_PHYS`

**命名约定（主规格输出）：**

```
outputs/stage2_endogenous_edgeproj_v2_lcc_gate5_bpr_pos05_v1/
outputs/tidal_lane_diagnostics_edgeproj_v2_lcc_gate5_bpr_pos05_v1/
outputs/welfare_sorting_diagnostics_gate5_bpr_pos05_v1/
outputs/alpha_beta_sensitivity_bpr_pos05_v1/
outputs/policy_comparison_gate5_bpr_pos05_v1/
```

> **勿与旧 asym / lanefix 结果混用**（见 §11）。

---

## 1. 执行摘要（论文 / slides 用）

### 1.1 潮汐主线（top100，λ=1.1，endogenous）


| 账本          | 指标                      | 数值           | 解读                     |
| ----------- | ----------------------- | ------------ | ---------------------- |
| **ST 通勤**   | direct                  | **−1.63 s**  | 慢向走廊立即受益               |
| **LT 通勤分解** | GE wedge                | +0.99 s      | 网络反馈部分抵消               |
|             | sorting wedge           | **+0.71 s**  | α−β 机制项                |
|             | **LT total**            | **+0.065 s** | ≈ 打平                   |
|             | **person-min/day**      | **+95**      | 城市尺度近零                 |
| **福利**      | W̄ ST                   | **+0.87%**   | 原始 Frechet denominator |
|             | **W̄ LT**               | **+0.32%**   | LT 福利仍为正               |
| **走廊**      | slow median % save (LT) | 13.6%        | treated 慢向             |
| **暴露**      | OD path exposed share   | 56.4%        | 通勤流经 treated 走廊        |


**机制 punchline：** ST 省时间；LT **通勤账本** near zero；LT **福利账本** 仍为正。旧 `Σ_i u_i^θΠ_i^{-θ}` 负号来自 raw `(Π,P)` gauge 漂移，不再作为主福利口径。

### 1.2 与旧 asym 选边对照（同 αβ pos05）


| 指标             | asym（旧） | **bpr+pos05（主）** |
| -------------- | ------- | ---------------- |
| ST direct      | −0.79 s | **−1.63 s**      |
| sorting wedge  | +0.86 s | +0.71 s          |
| LT total       | +0.60 s | **+0.07 s**      |
| person-min/day | +884    | **+95**          |
| W̄ LT          | 需按新口径重跑 | **+0.32%**       |


bpr 放大 ST shock、压低 LT person-min；主福利口径修正后，bpr+pos05 的 LT W̄ 为正。

### 1.3 αβ 敏感性（18 格，17 收敛）

- **W̄ LT > 0：** 在当前收敛网格内均为正。
- **W̄ LT 最好：** α=−0.12, β=−0.10 → **+0.370%**。
- **person-min LT > 0：** 仅 **α≥0.05 且 β=−0.10**（主轨 +95）；α=0.04 仍为 −669。
- **β=+0.10 @ α=0.05：** 未收敛（median τ/obs≈1.35）。

### 1.4 政策对比（P1–P7，ST+LT 平均见 cost_effectiveness）


| 政策                         | W̄ LT       | lane-km | 备注                  |
| -------------------------- | ----------- | ------- | ------------------- |
| P3 qdelay expansion        | **+0.444%** | 1091    | 按拥堵选边扩张             |
| P5 uniform ×1.10           | +0.395%     | —       | 无基建成本天花板            |
| P2 same-corridor expansion | +0.382%     | 1091    | 与潮汐同走廊加 lane        |
| P1 tidal                   | +0.324%     | **0**   | ST +0.87%，LT +0.32% |
| P4 uniform matched         | +0.100%     | 1091    | 与 P2 同 lane-km 预算   |
| P7 cordon                  | −0.218%     | 0       | 弱效应                 |
| P6 first-best pricing      | −4.226%     | 0       | 当前 toll 实现下成本很高     |


**叙事：** 扩张方案福利更高但需要 lane-km；**潮汐 = 零 lane-km、定向分配、易落地，且 W̄ LT 为正**。

### 1.5 Exog vs Endog（Layer A）


| CF                 | exog τ W̄    | endog W̄ |
| ------------------ | ------------ | -------- |
| CF1 symmetric      | **+1.10%**   | ≈ 0%     |
| CF3 uniform ×1.10  | +1.39%       | +0.40%   |
| CF2 tidal top50 LT | −1.47%（exog） | +0.23%   |


固定 τ 与 endogenous anchored 口径不能混读；主模型福利采用原始 Frechet denominator。

---

## 2. 潮汐 ST/LT 全分解（主规格）

**来源：** `outputs/tidal_lane_diagnostics_edgeproj_v2_lcc_gate5_bpr_pos05_v1/tidal_st_lt_compact.csv`（spec=`top100_d1.0`）


| 分量                  | avg commute (min) | avg commute (sec) |
| ------------------- | ----------------- | ----------------- |
| baseline avg cost   | 6.003             | —                 |
| ST avg cost         | 5.976             | —                 |
| LT avg cost         | 6.005             | —                 |
| **direct (ST)**     | −0.027            | **−1.634**        |
| GE wedge            | +0.017            | +0.992            |
| sorting wedge       | +0.012            | +0.707            |
| **total LT change** | +0.001            | **+0.065**        |



| 福利  | ΔW̄         |
| --- | ----------- |
| ST  | **+0.871%** |
| LT  | **+0.324%** |



| 走廊 / 流                     | 数值                                |
| -------------------------- | --------------------------------- |
| treated mean Δτ ST         | +0.30 s（慢向改善为负号 convention 见 CSV） |
| treated mean Δτ LT         | +0.36 min                         |
| ST Ξ-weighted Δτ (treated) | −3.20 min                         |
| LT Ξ-weighted Δτ (treated) | −1.88 min                         |


**Stage2 潮汐规格网格**（`counterfactuals/tidal_lane_endogenous_summary.csv`）：


| spec            | horizon | W̄ Δ%     | res diss % | workplace diss % |
| --------------- | ------- | --------- | ---------- | ---------------- |
| top50 Δ1.0      | ST      | +0.67     | ≈0         | ≈0               |
| top50 Δ1.0      | LT      | +0.23     | 0.68       | 1.37             |
| top50 Δ0.5      | ST      | +0.32     | ≈0         | ≈0               |
| top50 Δ0.5      | LT      | +0.12     | 0.35       | 0.71             |
| **top100 Δ1.0** | **ST**  | **+0.87** | ≈0         | ≈0               |
| **top100 Δ1.0** | **LT**  | **+0.32** | 0.86       | 1.79             |


---

## 3. 福利 gauge 修正与 resident access（pos05）

**来源：** `outputs/welfare_sorting_diagnostics_gate5_bpr_pos05_v1/`

### 3.1 主福利口径

福利现在按模型原始定义计算：

```text
Wbar^theta = sum_ij tau_ij^{-theta} u_i^theta w_j^theta
```

主规格 top100 的结果为：


| 项         | 值           |
| --------- | ----------- |
| W̄_base   | 3.2001      |
| W̄_ST     | 3.2280      |
| W̄_LT     | 3.2105      |
| **W̄ ST** | **+0.871%** |
| **W̄ LT** | **+0.324%** |


旧 `sum_i u_i^theta Pi_i^{-theta}` 口径保留为 raw-Pi gauge 诊断；它给出 LT `−0.849%`，但该负号不再作为福利结论。

### 3.2 Gauge-free resident access

Result 2 的 access 图改为：

```text
MA_i = sum_j tau_ij^{-theta} w_j^theta
```

输出文件：

- `pos05/primitive_welfare_summary.json`
- `pos05/resident_access_decomposition.json`
- `pos05/node_sorting_changes.csv`
- `Documents/PreSlides/figs/sort_Pi.png`

### 3.3 汇总 CSV

- `welfare_decomposition_summary.csv` — 单行 pos05 轨
- `pos05/node_sorting_changes.csv` — 1104 节点 Δlog l_R, l_F, Π
- `pos05/endpoint_group_summary.csv` — hub / outer ring 分组

---

## 4. α / β 敏感性（完整）

**脚本：** `data_work/src/model/full_estimation/run_alpha_beta_sensitivity_bpr_gate5.py`  
**核心函数：** `run_alpha_robustness_quick_gate5.run_spec()`  
**输出根：** `outputs/alpha_beta_sensitivity_bpr_pos05_v1/`

### 4.1 运行命令

```bash
# 基础 9 格（alpha 扫描 @ β=−0.10 + beta 扫描 @ α=0.05）
python3 src/model/full_estimation/run_alpha_beta_sensitivity_bpr_gate5.py --sweep both

# mini 补充 10 格（实际有效 9 格；α=0.045 因标签 +.2f 与 0.04 冲突未跑）
python3 src/model/full_estimation/run_alpha_beta_sensitivity_bpr_gate5.py --sweep mini --skip-existing
```

### 4.2 基础 9 格（按 W̄ LT 从高到低）


| spec_label           | α     | β     | sorting (s) | person-min/day | W̄ ST % | W̄ LT %    | 收敛  |
| -------------------- | ----- | ----- | ----------- | -------------- | ------- | ---------- | --- |
| α−0.12 β−0.10        | −0.12 | −0.10 | −3.63       | −6417          | +0.872  | **+0.370** | ✓   |
| α0 β−0.10            | 0     | −0.10 | −1.33       | −2960          | +0.872  | +0.345     | ✓   |
| α0.02 β−0.10         | 0.02  | −0.10 | −0.65       | −1942          | +0.871  | +0.338     | ✓   |
| α0.03 β−0.10         | 0.03  | −0.10 | −0.25       | −1343          | +0.871  | +0.334     | ✓   |
| **α0.05 β−0.10（主轨）** | 0.05  | −0.10 | +0.71       | **+95**        | +0.871  | **+0.324** | ✓   |
| α0.05 β0             | 0.05  | 0     | −1.57       | −3173          | +0.871  | +0.306     | ✓   |
| α0.10 β−0.10         | 0.10  | −0.10 | +4.88       | +6340          | +0.872  | +0.286     | ✓   |
| α0.05 β+0.05         | 0.05  | 0.05  | −4.64       | −7579          | +0.872  | +0.282     | ✓   |
| α0.05 β+0.10         | 0.05  | 0.10  | n/a         | n/a            | n/a     | n/a        | 否   |


**CSV 文件：**


| 文件                        | 行数  | 说明                                           |
| ------------------------- | --- | -------------------------------------------- |
| `sensitivity_grid.csv`    | 9   | 基础 both 扫描，已按 gauge-free welfare 重跑          |
| `mini_grid.csv`           | 9   | mini 补充，旧 raw-Pi 口径，待重跑后解释                   |
| `mini_grid_converged.csv` | 9   | mini 补充，旧 raw-Pi 口径，待重跑后解释                   |
| `alpha_{α}_beta_{β}/`     | 多目录 | 每格 baseline + tidal ST/LT + convergence_meta |


**规律：**

1. **W̄ ST** 在收敛格内恒 ≈ **+0.87%**（与 α,β 几乎无关）。
2. **W̄ LT** 在基础收敛格内均为正，随 α 增大略降。
3. **person-min 过零**（β=−0.10 下）约在 **α≈0.04–0.05**；与 W̄ LT 过零 **不同步**。
4. **β 偏离 −0.10**（尤其 β≥0）同时恶化 W̄ LT 与 person-min。

---

## 5. Phase A — ST 指标

**脚本：** `run_phase_a_st_metrics_gate5.py`  
**输出：** `outputs/phase_a_st_metrics_gate5_bpr_pos05_v1/`  
**完成时间：** 2026-06-30


| run_label         | spec   | corridor save LT % | OD exposed % | person-min/day | W̄ LT % | W̄ ST % |
| ----------------- | ------ | ------------------ | ------------ | -------------- | ------- | ------- |
| **lambda_1.1（主）** | top100 | **13.55**          | **56.36**    | **+95.0**      | +0.324  | +0.871  |
| lambda_0.57       | top50  | 10.62              | 43.33        | −801.8         | −0.038  | +0.050  |
| lambda_0.57       | top100 | 10.67              | 56.36        | −1167.9        | −0.078  | +0.100  |


**主行附加：**

- person-min ST direct：−2,400 person-min/day（= −1.63 s × Lbar）
- person-min sorting wedge：+1,039
- 人口 +25% / +50% 情景 LT person-min：+119 / +142

**文件：** `phase_a_st_metrics.csv`，`summary.json`

---

## 6. Phase B — LT sorting 空间格局

**脚本：** `run_phase_b_sorting_patterns_gate5.py`  
**输出：** `outputs/phase_b_sorting_patterns_gate5_bpr_pos05_v1/`  
**完成时间：** 2026-06-30


| 指标                         | 值        |
| -------------------------- | -------- |
| max                        | Δlog l_R |
| max                        | Δlog l_F |
| mean Δlog l_R（走廊邻近 134 节点） | +0.0168  |
| mean Δlog l_R（远离走廊）        | −0.0121  |


**空间图：** `map_log_lR_change.png`，`map_log_lF_change.png`，`map_log_Pi_change.png`  
**径向 profile：** `radial_profile_log_lR.csv`，`.png`  
**Top movers：** `top_movers.csv`（ residence gainer 例：sq_25_42 Δlog l_R=+0.141）

**文件清单：**


| 文件                         | 内容                        |
| -------------------------- | ------------------------- |
| `node_sorting_changes.csv` | 全节点 sorting               |
| `top_movers.csv`           | top20 gainer/loser × 3 指标 |
| `summary.json`             | 上表摘要                      |


---

## 7. Phase 4 — 政策对比 P1–P7

**脚本：** `run_policy_comparison_gate5.py`  
**输出：** `outputs/policy_comparison_gate5_bpr_pos05_v1/`  
**状态：** ✅ **已完成**（全部 policy × horizon 收敛）

```bash
python3 src/model/full_estimation/run_policy_comparison_gate5.py
```

### 7.1 完整结果（`results_by_policy.csv`）


| ID  | 政策                      | horizon | W̄ Δ%      | res diss % | wp diss % | lane-km | 其他                 |
| --- | ----------------------- | ------- | ---------- | ---------- | --------- | ------- | ------------------ |
| P1  | 潮汐 top100               | ST      | **+0.871** | ≈0         | ≈0        | 0       | 主结果                |
| P1  | 潮汐 top100               | LT      | **+0.324** | 0.86       | 1.79      | 0       |                    |
| P2  | 同走廊扩张                   | ST      | +0.907     | ≈0         | ≈0        | 1091.5  |                    |
| P2  | 同走廊扩张                   | LT      | +0.382     | 0.94       | 1.70      | 1091.5  |                    |
| P3  | qdelay top100 扩张        | ST      | +1.261     | ≈0         | ≈0        | 1091.5  | ST 最大              |
| P3  | qdelay top100 扩张        | LT      | +0.444     | 1.10       | 1.92      | 1091.5  |                    |
| P4  | uniform matched lane-km | ST/LT   | +0.100     | 0.11       | 0.22      | 1091.5  | factor=1.024       |
| P5  | uniform speed ×1.10     | ST/LT   | +0.395     | 0.45       | 0.90      | —       | 无成本 CF             |
| P6  | first-best pricing      | ST      | **−4.218** | ≈0         | ≈0        | 0       | toll 1.09M/day     |
| P6  | first-best pricing      | LT      | −4.226     | 5.87       | 10.33     | 0       | rebate 12.4/worker |
| P7  | cordon (q0.9)           | ST      | −0.235     | ≈0         | ≈0        | 0       | 434 edges          |
| P7  | cordon (q0.9)           | LT      | −0.218     | 0.52       | 1.01      | 0       |                    |


### 7.2 性价比（`cost_effectiveness.csv`，ST/LT 简单平均 W̄）


| policy_id                    | mean W̄ Δ% | lane-km | W̄ / lane-km |
| ---------------------------- | ---------- | ------- | ------------ |
| P3_expansion_qdelay          | +0.853     | 1091.5  | 7.81e-4      |
| P2_expansion_same_corridor   | +0.644     | 1091.5  | 5.90e-4      |
| P1_tidal                     | +0.598     | 0       | —            |
| P5_uniform_speed_1.10        | +0.395     | —       | —            |
| P4_expansion_uniform_matched | +0.100     | 1091.5  | 9.12e-5      |
| P7_cordon_pricing            | −0.227     | 0       | —            |
| P6_first_best_pricing        | −4.222     | 0       | —            |


**manifest：** `policies_manifest.json`  
**日志：** `run.log`

---

## 8. Exogenous τ 层（Layer A）

**脚本：** `run_exog_endog_comparison_gate5.py`  
**输出：** `outputs/exog_endog_comparison_gate5_bpr_pos05_v1/`  
**memo：** `Documents/L2_notes/exog_vs_endog_framing_20260630.md`（复制于 `exog_vs_endog_framing.md`）

### 8.1 结构 CF（endogenous vs exogenous_tau）


| CF                       | endog W̄ % | exog W̄ % | 备注            |
| ------------------------ | ---------- | --------- | ------------- |
| CF1 symmetric geometric  | ≈0         | **−2.77** | exog 夸大       |
| CF1 symmetric arithmetic | ≈0         | ≈0        |               |
| CF2 tidal top50 LT       | −0.18      | +5.12     | exog 符号可 flip |
| CF3 uniform ×1.10        | −0.12      | −0.40     |               |
| CF3 uniform ×1.25        | −0.43      | −1.38     |               |
| CF3 uniform ×1.50        | −1.14      | −3.84     |               |


### 8.2 潮汐 endogenous（stage2 摘要，score_bpr_st）

见 §2 Stage2 网格表（top50/100 × Δ × ST/LT）。

### 8.3 文件


| 文件                          | 说明                   |
| --------------------------- | -------------------- |
| `exog_endog_comparison.csv` | 全族 long table（56+ 行） |
| `comparison_meta.json`      | stage2 路径等           |
| `exog_vs_endog_framing.md`  | 叙事要点                 |


---

## 9. Stage2 baseline 与结构 CF

**脚本：** `run_stage2_gate5.py`  
**输出：** `outputs/stage2_endogenous_edgeproj_v2_lcc_gate5_bpr_pos05_v1/`

### 9.1 Baseline（`baseline_summary.json`）


| 项              | 值                                    |
| -------------- | ------------------------------------ |
| W̄_endogenous  | 1.8605                               |
| converged      | true（n_outer=65，max_log_change≈1e-4） |
| median τ/τ_obs | **1.0001**                           |
| mean τ vs obs  | +0.13%                               |
| pair_selection | score_bpr_st                         |
| congestion     | anchored, λ=1.1                      |


### 9.2 结构 CF endogenous（`counterfactuals/structural_cf_endogenous.csv`）


| CF                      | W̄ Δ% | OD cost Δ% |
| ----------------------- | ----- | ---------- |
| CF1 symmetric geometric | ≈0    | ≈0         |
| CF2 tidal top50 LT      | −0.18 | +0.08      |
| CF3 uniform ×1.10       | −0.12 | −0.90      |
| CF3 uniform ×1.25       | −0.43 | −1.89      |
| CF3 uniform ×1.50       | −1.14 | −2.87      |


### 9.3 Stage2 目录关键文件


| 路径                                                     | 说明                           |
| ------------------------------------------------------ | ---------------------------- |
| `baseline_endogenous/equilibrium_arrays.npz`           | 主 baseline                   |
| `baseline_exogenous_tau/`                              | exog τ baseline              |
| `fundamentals/`                                        | inversion 结果                 |
| `parameters.json`                                      | α,β,θ 等                      |
| `counterfactuals/tidal_lane_exogenous_bpr_grid.csv`    | exog BPR 网格                  |
| `counterfactuals/tidal_lane_exogenous_edge_detail.csv` | 边级 exog                      |
| `counterfactuals/policy_cf_all_modes.csv`              | place-based CF               |
| `counterfactuals/tidal_endog_top100_d1.0_long_run/`    | **主 LT 均衡** + selected_pairs |


---

## 10. Fundamental 地图

**脚本：** `run_fundamental_maps_gate5.py`  
**输出：** `outputs/fundamental_maps_gate5_bpr_pos05_v1/`


| 统计                    | 值        |
| --------------------- | -------- |
| corr(ū, L_R_obs)      | **0.30** |
| corr(ā, L_F_obs)      | **0.67** |
| top-5 w_contrib share | 4.2%     |


**文件：** `fundamental_maps_summary.json`，`fundamental_node_table.csv`，`*.png`

---

## 11. 主分析打包

**脚本：** `run_tidal_mainline_gate5.py`  
**输出：** `outputs/tidal_mainline_bpr_pos05_v1/`


| 文件                         | 说明                    |
| -------------------------- | --------------------- |
| `memo_tidal_mainline.md`   | sanity check 对照表      |
| `package_meta.json`        | 指向 tidal + welfare 目录 |
| `node_sorting_changes.csv` | 复制自 welfare pos05     |
| `maps/`                    | 图表                    |


---

## 12. 全部分析包注册表


| #   | 分析                 | Driver 脚本                                              | 主要输入                                   | 输出目录                                              | 状态  |
| --- | ------------------ | ------------------------------------------------------ | -------------------------------------- | ------------------------------------------------- | --- |
| 1   | Stage2 baseline+CF | `run_stage2_gate5.py`                                  | `model_input_edgeproj_v2_lcc_gate5_v1` | `stage2_..._bpr_pos05_v1/`                        | ✅   |
| 2   | 潮汐 ST/LT           | `run_tidal_lane_diagnostics_gate5.py`                  | stage2 baseline + primitives           | `tidal_lane_diagnostics_..._bpr_pos05_v1/`        | ✅   |
| 3   | W̄/Π 分解            | `run_welfare_sorting_diagnostics_gate5.py`             | stage2 + tidal                         | `welfare_sorting_diagnostics_gate5_bpr_pos05_v1/` | ✅   |
| 4   | 主分析包               | `run_tidal_mainline_gate5.py`                          | #2+#3                                  | `tidal_mainline_bpr_pos05_v1/`                    | ✅   |
| 5   | Exog/endog         | `run_exog_endog_comparison_gate5.py`                   | stage2 全套 CSV                          | `exog_endog_comparison_gate5_bpr_pos05_v1/`       | ✅   |
| 6   | Fundamental        | `run_fundamental_maps_gate5.py`                        | stage2 baseline                        | `fundamental_maps_gate5_bpr_pos05_v1/`            | ✅   |
| 7   | αβ 基础 9 格          | `run_alpha_beta_sensitivity_bpr_gate5.py --sweep both` | primitives                             | `alpha_beta_sensitivity_bpr_pos05_v1/`            | ✅   |
| 8   | αβ mini +9 格       | 同上 `--sweep mini`                                      | 同上                                     | 同上 `mini_grid.csv`                                | ✅   |
| 9   | Phase A            | `run_phase_a_st_metrics_gate5.py`                      | #2 + stage2 NPZ                        | `phase_a_st_metrics_gate5_bpr_pos05_v1/`          | ✅   |
| 10  | Phase B            | `run_phase_b_sorting_patterns_gate5.py`                | stage2 tidal LT NPZ                    | `phase_b_sorting_patterns_gate5_bpr_pos05_v1/`    | ✅   |
| 11  | 政策 P1–P7           | `run_policy_comparison_gate5.py`                       | stage2                                 | `policy_comparison_gate5_bpr_pos05_v1/`           | ✅   |


**共享库：** `stage2_counterfactuals.py`，`equilibrium_solve.py`，`phase_metrics_common.py`，`run_alpha_robustness_quick_gate5.run_spec()`

---

## 13. 历史 / 非主规格（勿入主表）


| 标签            | 输出目录                                                   | 说明                       |
| ------------- | ------------------------------------------------------ | ------------------------ |
| asym + pos05  | `welfare_sorting_diagnostics_gate5_v1/`                | 旧选边 `asym`               |
| lanefix batch | `gate5_lanefix_`*，`phase_a_st_metrics_gate5_lanefix_`* | 2026-06-29 前 lane lookup |
| λ=0.57        | Phase A `lambda_0.57` 行                                | 校准 band 稳健性              |
| 231 格 explore | `alpha_beta_sensitivity_bpr_pos05_explore_v1/`         | **已中止**（仅 2 格 partial）   |
| quick α 四格    | `run_alpha_robustness_quick_gate5.py` 独立输出             | 非 bpr 全网格                |


---

## 14. 机制结论与写作口径

1. **主规格叙事：** 潮汐 **ST 福利正、LT 通勤 near zero、LT 福利仍为正**；通勤时间收益被 GE / sorting 抵消，但原始福利不转负。
2. **选边：** 锁 `score_bpr_st`；W̄ LT **+0.324%** 是当前主线福利数字。
3. **αβ：** 基础收敛网格内 **W̄ LT 均为正**；person-min 与 W̄ LT **可分叉**（主轨 +95 person-min vs +0.324% W̄）。
4. **政策：** qdelay expansion > same-corridor expansion > tidal（LT W̄）；潮汐优势在 **零 lane-km + 定向**，非 raw welfare maximum。
5. **Exog 层：** 单独成章；CF1/CF2 exog 幅度与符号不可直接用于 endog 主结论。
6. **收敛：** β=+0.10 @ α=0.05 不收敛；正 α 需 500 outer iterations。
7. **已知 QA：** ~8.3% 边 τ_obs<τ_ff（anchored floor）；见 `code_logic_audit_20260630.md`。

---

## 15. 复现命令速查

```bash
cd data_work

# Stage2（主规格）
python3 src/model/full_estimation/run_stage2_gate5.py \
  --pair-selection score_bpr_st --alpha 0.05 --beta -0.10 \
  --output-name stage2_endogenous_edgeproj_v2_lcc_gate5_bpr_pos05_v1

python3 src/model/full_estimation/run_tidal_lane_diagnostics_gate5.py --only-top100 --pair-selection score_bpr_st
python3 src/model/full_estimation/run_welfare_sorting_diagnostics_gate5.py --tracks pos05 --pair-selection score_bpr_st

# 敏感性
python3 src/model/full_estimation/run_alpha_beta_sensitivity_bpr_gate5.py --sweep both
python3 src/model/full_estimation/run_alpha_beta_sensitivity_bpr_gate5.py --sweep mini --skip-existing

# Phase A/B + 政策 + 对比
python3 src/model/full_estimation/run_phase_a_st_metrics_gate5.py
python3 src/model/full_estimation/run_phase_b_sorting_patterns_gate5.py
python3 src/model/full_estimation/run_policy_comparison_gate5.py
python3 src/model/full_estimation/run_exog_endog_comparison_gate5.py
python3 src/model/full_estimation/run_fundamental_maps_gate5.py
```

---

## 16. 相关 memo 索引


| 文档            | 路径                                                         |
| ------------- | ---------------------------------------------------------- |
| 工作计划 handoff  | `Documents/L2_notes/MAINLINE_WORKPLAN_handoff_20260630.md` |
| 代码/逻辑审计       | `Documents/L2_notes/code_logic_audit_20260630.md`          |
| Exog/endog 叙事 | `Documents/L2_notes/exog_vs_endog_framing_20260630.md`     |
| 本文档           | `Documents/L2_notes/model_and_results_summary_20260630.md` |


