# Stage 1 OD 拟合诊断、结论与后续方向

**日期：** 2026-06-28  
**数据 bundle：** `data_work/outputs/model_input_edgeproj_v2_lcc_gate5_v1/`  
**主模型输出：** `data_work/outputs/directional_cost_edgeproj_v2_lcc_gate5_v1/`  
**代码分支（Stage 1 主线）：** `cursor/stage1-directional-cost-reinvert`  
**相关 tex：** `Documents/L1_manuscripts/FixedDirectedCost_Stage1_gate5.tex`  
**模型说明：** `data_work/docs/DIRECTIONAL_COST_MODEL_20260628.md`

---

## 1. 本阶段对 OD fit 的定位

Stage 1 的研究问题是：

> 在**同样拟合 observed 居住/工作边际**的前提下，directed calibration world 与 symmetric calibration world 在 within-run welfare、implied OD、inverted fundamentals 上差多少？

因此：

- **边际拟合好是设计目标**（反演矩 = `L_R_obs`, `L_F_obs`）。
- **全 OD 矩阵拟合差不是 Stage 1 的 failure mode**，不应把「复现 8 万条 OD 流」当作本阶段 success criterion。
- 但必须在文档中**如实记录** implied OD vs observed OD 的偏差结构，避免误读为「模型已复现通勤矩阵」。

---

## 2. 诊断方法

### 2.1 模型如何产生 OD

均衡后 gravity 分配（`fixed_tau_model.predicted_od_shares`）：

```text
L_ij = L_i^R * [ T_ij * Ā_j^θ * (l_j^F)^{αθ} ] / Σ_k [ T_ik * Ā_k^θ * (l_k^F)^{αθ} ]
```

- `T = (I-A)^{-1}`，`A_kl = 1/(κ τ_kl)`，κ=1（gate5 LCC）。
- 反演**不使用** `L_ij^obs`，只用边际。

### 2.2 报告指标（directed world，re-invert，geometric sym 未参与此项）

| 指标 | 数值 | 备注 |
|------|-----:|------|
| 正 OD 格子数 | 83,030 | N=1104 稠密矩阵中的正流 |
| **全局 L1**（pred vs obs，正格） | **~72%** | `0.5 Σ|L_pred - L_obs| / Σ L_obs` |
| **去对角 L1**（i≠j） | **~51%** | 区际流 |
| Origin 条件 L1 中位数 | ~66% | 给定 i 出发，目的地份额分配 |
| Origin 条件 L1 p90 | ~80% | |
| 加权 log 相关系数（按 obs 流量） | **0.60** | 大流略好 |
| 非加权 log 相关 | 0.27 | |
| **Directed vs Symmetric 预测 OD 的 L1** | **2.46%** | Stage 1 核心对比量 |
| **Directed vs obs** | ~72% | |
| **Symmetric vs obs** | ~72% | 对称化几乎不改善对数据的 OD fit |

复现诊断（一次性脚本，未写入 pipeline）：

```bash
cd data_work
python3 - <<'PY'
# 见 conversation 2026-06-28；或运行 plot_model_mechanics 生成 origin 级柱状图
PY
```

可视化（已有脚本）：

```bash
python3 data_work/src/model/directional_cost/plot_model_mechanics.py \
  --version-id model_input_edgeproj_v2_lcc_gate5_v1 \
  --output-root data_work/outputs/directional_cost_model_mechanics_gate5_v1
```

输出含 `workplace_choice_obs_vs_model.png`（top origins 的目的地份额 obs vs model）。

---

## 3. 主要发现

### 3.1 边际 vs OD：结构性分离

| 对象 | L1 vs obs |
|------|----------:|
| 居住份额 | ~0.0005% |
| 工作份额 | ~0.0004% |
| **全 OD 矩阵** | **~72%** |

反演只识别 `2N` 个 fundamentals 矩，不识别 `O(N²)` 个 bilateral 流。

### 3.2 最大误差源：区内通勤 `i → j`（i=j）

| | 观测 | 模型预测 |
|--|-----:|--------:|
| 区内流占总流量 | **8.0%** | **52.7%** |

机制：`T_ii = [(I-A)^{-1}]_{ii}`（网络回到自身的可达性）在数值上常远大于典型 `T_ij`，gravity 把过多 mass 分给「同格工作」。

典型例子（origin `sq_21_33`）：

- obs `L_ii ≈ 52`，pred `L_ii ≈ 488`（~9.5×）
- `T_ii / T_{i,j*} ≈ 141`

最大绝对误差 Top 15 几乎全是 **origin = destination** 的格子（`sq_25_33`, `sq_21_33` 等）。

### 3.3 区际流仍偏差大，但排序略优于水平

- 去对角后全局 L1 ~51%；origin 条件中位 ~49–66%。
- 流量前 1% OD：L1 ~44%，加权 log corr ~0.37。
- 大 origin（如 `sq_21_33`）目的地 log 相关 ~0.64–0.67，但条件 L1 仍 ~66–69%（排序有部分信息，份额水平仍差）。

### 3.4 对 Stage 1 核心问题的含义

- **Directed vs symmetric** 改变 implied OD **~2.5%**（本阶段 actionable bias）。
- **Model vs data** 的 ~72% gap **与是否对称化无关**（directed/symmetric 对 obs 的 OD fit 几乎一样）。
- Stage 1 论文叙事：report **calibration-world OD comparison**，不要 claim **structural replication of Beijing OD**。

---

## 4. 本阶段结论（写入 tex / memo 的要点）

1. Directional congestion 在 gate5 网络上广泛存在（72.4% 双向对 r≥1.10）。
2. Re-invert 设计下，directed / symmetric **均拟合同一 observed marginals**；差异在 implied OD（~2.5%）、W̄（~−1.1%）、fundamentals（p95 |log Δ| ~0.55）。
3. Implied OD **不**拟合 observed OD 矩阵（~72% L1）；主因是 **T_ii / 区内流** 与数据定义不匹配，其次为 gravity 结构过简、θ 外生、反演矩不足。
4. **Poor OD fit 不削弱 Stage 1 对「对称化误读 directionality」的论证**，但必须在 Data/Results 中明确 scope。

---

## 5. 建议方向（供「OD 改进」平行 track）

按优先级；**在另一窗口推进**，与 Stage 1 主线解耦。

### P0 — 诊断固化

- [ ] 在 `run_directional_cost.py` 写入 `diagnostics/od_fit_to_observed.json`（全局 / off-diag / intra / weighted corr / top errors）。
- [ ] 固定输出 `workplace_choice_obs_vs_model.png` 到 gate5 figures 目录。

### P1 — 区内流处理（预期收益最大）

- [ ] Workplace choice 分母剔除 `j=i`，或单独 intra-zonal 效用/成本项。
- [ ] 明确 OD 表中 `i=j` 的经济含义（路网同格 vs 驻留口径）。
- [ ] 报告改进前后：intra mass、global L1、off-diag L1。

### P2 — 参数与矩

- [ ] 用北京 OD 估计 θ（PO gravity on `T_ij` 或 SP distance）。
- [ ] SMM：反演时增加 OD 汇总矩（mean commute time、OD entropy、top-k destination share）。
- [ ] θ / α sensitivity grid。

### P3 — 结构与数据

- [ ] Origin–destination taste shifters（低秩或 bilateral FE；牺牲结构简洁性）。
- [ ] 路径成本：`T` 是否应对齐 Baidu/实际 peak 通勤路径而非 grid Leontief。
- [ ] 网格尺度 / 样本过滤（极短距、非道路模式）。

### 不要在本 track 里做

- 潮汐车道 counterfactual、内生拥堵、λ 结构估计（Stage 2）。
- 为 fit OD 而改 Stage 1 的 **directed vs symmetric re-invert** 比较逻辑。

---

## 6. 关键文件索引

```text
data_work/src/model/directional_cost/run_directional_cost.py   # Stage 1 主入口
data_work/src/model/directional_cost/fixed_tau_model.py        # T, equilibrium, predicted_od_shares
data_work/src/model/full_estimation/invert_fundamentals.py     # 反演（只用边际）
data_work/src/model/directional_cost/plot_model_mechanics.py # OD 诊断图
Documents/L1_manuscripts/FixedDirectedCost_Stage1_gate5.tex   # Stage 1 文稿
```

---

## 7. OD 方向 vs 有向边成本 QA

脚本：

```bash
python3 data_work/src/model/directional_cost/qa_od_direction_vs_cost.py
```

输出：`data_work/outputs/directional_cost_edgeproj_v2_lcc_gate5_v1/diagnostics/od_direction_cost_qa.json`

**结论（gate5，AM）：**

- OD `i→j` = `home_i → work_i`；边 `τ_kl` = AM 从 `grid_o` 到 `grid_d` 的时间；**方向一致**。
- 在 **SP 可达** 的 off-diagonal 正 OD 上，加权 log 相关：**SP(i,j) vs T_ij^{-1/θ} ≈ 0.94**（排序高度一致）。
- **家→单位** 与 **单位→家** 的 SP 时间不同：可双向可达样本中 **~53%** 满足 `SP(home,work) > SP(work,home)`，中位 log 差 ~0.05（早高峰 inbound 更慢符合预期）。
- **12.5%** off-diagonal 正 OD 在**有向 SP** 上不可达，但 Leontief `T_ij` 仍有限——这是 soft aggregator 与纯最短路的不同，不是方向反了。
- `i=j` 时 SP=0，而 soft cost ~1 min：区内 OD 与 link 语义不一致（见 §3.2）。

---

## 8. 与 Stage 1 主线的关系

| 窗口 | 任务 |
|------|------|
| **本窗口（Stage 1）** | directed vs symmetric、re-invert、tex、simulation、policy scope |
| **另一窗口（OD track）** | P0–P3：诊断固化 → intra-zonal → θ/SMM → 结构扩展 |

两 track 共用 gate5 bundle；OD track 的改动应在新分支（如 `cursor/stage1-od-fit-improvement`）上，合并前跑 regression：Stage 1 的 `summary_metrics.json` 核心数字（W̄、marginals、directed-vs-sym OD 2.46%）不应被意外改变，除非 intentionally 改模型 spec。
