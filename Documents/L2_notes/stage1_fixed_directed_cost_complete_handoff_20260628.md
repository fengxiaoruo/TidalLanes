# Stage 1 固定定向成本空间均衡：完整工作记录

**日期：** 2026-06-28（持续更新至潮汐车道原型）  
**主 tex：** [`Documents/L1_manuscripts/FixedDirectedCost_Stage1_gate5.tex`](../L1_manuscripts/FixedDirectedCost_Stage1_gate5.tex)  
**PDF：** `Documents/L1_manuscripts/FixedDirectedCost_Stage1_gate5.pdf`  
**模型设计 doc：** [`data_work/docs/DIRECTIONAL_COST_MODEL_20260628.md`](../../data_work/docs/DIRECTIONAL_COST_MODEL_20260628.md)  
**OD 诊断 memo：** [`stage1_od_fit_diagnosis_and_handoff_20260628.md`](stage1_od_fit_diagnosis_and_handoff_20260628.md)

---

## 0. 一句话定位

在 **固定 AM 峰值 directed travel time** 下，构造 **directed calibration world** 与 **symmetric calibration world**（各自 re-invert fundamentals、各自拟合同一组居住/就业边际），比较两世界在 **OD 结构、可达性地图、政策 CF、潮汐车道原型** 上的差异；**不做**内生拥堵传导与 $\lambda$ 结构估计。

---

## 1. 研究问题与成功标准

### 1.1 核心问题

> 若把有方向的峰值拥堵成本压成 link-level symmetric index，在 **同样拟合 observed 边际人口** 的前提下，哪些 **可观测结果**（通勤 OD、$\Pi_i^{-\theta}$、分区人口、政策响应）会被系统性误读？

### 1.2 本阶段的 success / non-success

| 是 success | 不是 failure |
|-----------|-------------|
| 两世界 L1 边际 ≈ 0 | 全 OD L1 ~72%（未 targeting OD） |
| directed vs sym **implied OD** 差 ~2.5% | intra-zonal 预测 53% vs obs 8% |
| 交通类 CF 跨 world 楔子 > 区位类 CF | 单点 $\bar W$ 差 ~1%（fundamentals 补偿） |
| 潮汐车道原型：短期/长期符号反转 | 潮汐 CF 量级小（仅 50–100 相邻对） |

---

## 2. 数据与输入 bundle

### 2.1 Bundle 标识

```text
data_work/outputs/model_input_edgeproj_v2_lcc_gate5_v1/
```

| 项目 | 值 |
|------|-----|
| 路由 | raw_connected_v1 → edgeproj_v2 |
| 样本 | LCC + detour gate G=5 |
| Grid | square, N=1104 nodes |
| Directed edges | 2,642 |
| Reciprocal pairs | 1,193 |
| 正 OD 格子 | 310,525（L̄=88,110.2 road commuters, 92.6% of city OD table） |
| Period | AM |
| OD column | commuters_road |

### 2.2 Primitives 字段（`build_primitives.py`）

- 边：`ell_kl`, `v0_kl`, `v_obs_kl`, `tau_ff_min`, `tau_obs_min`, `n_kl`（车道）
- 人口：`L_R_obs`, `L_F_obs`, `L_ij_obs`, `Lbar`
- Stage 1 **主均衡只用** `tau_obs_min`；`tau_ff`/`n_kl` 用于潮汐 BPR 原型

### 2.3 方向性统计

- 互惠对 median $r_{kl}=\max(\tau)/\min(\tau)$ ≈ **1.23**；p90 ≈ 1.94
- **72.4%** 互惠对 $r\ge 1.10$；**8.89%** 满足 $r\ge 2$

---

## 3. 模型方程（锁定规格）

### 3.1 固定成本与路径聚合

```text
τ_kl = τ^obs_kl     (directed world)
τ^sym_kl = sqrt(τ^obs_kl · τ^obs_lk)   互惠 link 几何平均 (symmetric world)

A_kl = 1/(κ τ_kl),   T = (I-A)^(-1),   τ_ij^(-θ) ≡ T_ij
κ = 1   (gate5 LCC 上 raw row sum < 0.9)
```

### 3.2 静态均衡（Allen–Arkolakis 型）

```text
u_i = ū_i (L_i^R)^β,   A_j = Ā_j (L_j^F)^α

l_i^R = [ χ ū_i^θ Σ_j T_ij Ā_j^θ (l_j^F)^(αθ) ]^(1/(1-βθ))
l_j^F = [ χ Ā_j^θ Σ_i T_ij ū_i^θ (l_i^R)^(βθ) ]^(1/(1-αθ))

W̄ = [ Σ_i ū_i^θ (L_i^R)^(βθ) Π_i^(-θ) ]^(1/θ)
```

### 3.3 反演（structural fixed point，非 gravity FE）

给定固定 $\tau$ 与 observed shares $(l_i^{R,obs}, l_j^{F,obs})$，交替更新 $(v_i=\bar u_i^\theta, w_j=\bar A_j^\theta)$ 直至 residence/workplace 方程给出的 $\chi$ 一致（std(log χ) < 1.5e-4）。

**主规格：** symmetric world **重新 invert**，两世界各自 rationalise 同一组边际。

**Legacy 对照：** 只在 directed 上 invert，symmetric 只改 $\tau$ → sorting/OD gap 更大（居住/就业/OD ≈ 2.9% / 2.5% / 5.6%）。

### 3.4 隐含 OD（不进入反演矩）

```text
L_ij = L_i^R · [ T_ij Ā_j^θ (l_j^F)^(αθ) ] / Σ_k [ T_ik Ā_k^θ (l_k^F)^(αθ) ]
```

### 3.5 对称化 convention（重要）

- 对称化在 **routing graph 的 reciprocal directed links** 上，**不是** OD 矩阵对称化
- AM OD：home_i → work_j；边成本：AM peak $k\to l$

---

## 4. 参数

| 参数 | 值 | 来源 |
|------|-----|------|
| θ | 6.83 | AA(2022) Seattle placeholder |
| α | −0.12 | 同上 |
| β | −0.10 | 同上 |
| λ | — | 省略（Stage 2） |
| δ₁ (BPR) | 0.488 | AA Seattle；仅潮汐原型 |

---

## 5. 代码结构与职责

根目录：`data_work/src/model/`

| 模块 | 文件 | 功能 |
|------|------|------|
| Primitives | `full_estimation/build_primitives.py` | parquet → Primitives |
| 反演 | `full_estimation/invert_fundamentals.py` | 固定点反演 ū, Ā, χ |
| 参数 | `full_estimation/parameters.py` | frozen dataclass |
| 核心 | `directional_cost/fixed_tau_model.py` | T, 均衡, OD, 比较, 对称化 |
| 主跑 | `directional_cost/run_directional_cost.py` | directed vs sym + ρ/c 网格 |
| 政策 CF | `directional_cost/policy_counterfactuals.py` | 5 类非定向 CF |
| 政策跑 | `directional_cost/run_policy_counterfactuals.py` | 两 world × 5 policies |
| 潮汐 | `directional_cost/tidal_lane_counterfactuals.py` | 选边, BPR, 短/长期 |
| 潮汐跑 | `directional_cost/run_tidal_lane_counterfactuals.py` | 26-spec 网格 |
| 图 | `plot_results.py`, `plot_stage1_maps.py`, `plot_tidal_lane_results.py` | figures |
| QA | `qa_od_direction_vs_cost.py` | AM 方向 vs τ |
| OD track | `od_fit_estimation.py`, `run_od_fit_calibration.py` | θ/SMM 平行线（非 Stage1 主结论） |

README 入口：`data_work/src/model/directional_cost/README.md`

---

## 6. 复现命令（从 repo 根目录 `TidalLanes/`）

```bash
# 1) 主结果：directed vs symmetric + asymmetry simulations
python3 data_work/src/model/directional_cost/run_directional_cost.py \
  --version-id model_input_edgeproj_v2_lcc_gate5_v1 \
  --output-name directional_cost_edgeproj_v2_lcc_gate5_v1 \
  --sym-method geometric

# 2) 图
python3 data_work/src/model/directional_cost/plot_results.py \
  --output-root data_work/outputs/directional_cost_edgeproj_v2_lcc_gate5_v1
python3 data_work/src/model/directional_cost/plot_stage1_maps.py \
  --output-root data_work/outputs/directional_cost_edgeproj_v2_lcc_gate5_v1

# 3) 非定向政策 CF（directed vs symmetric world）
python3 data_work/src/model/directional_cost/run_policy_counterfactuals.py

# 4) 潮汐车道原型（26 specs；--quick 仅 2 specs）
python3 data_work/src/model/directional_cost/run_tidal_lane_counterfactuals.py
python3 data_work/src/model/directional_cost/plot_tidal_lane_results.py --repo-root .

# 5) OD 方向 QA
python3 data_work/src/model/directional_cost/qa_od_direction_vs_cost.py \
  --version-id model_input_edgeproj_v2_lcc_gate5_v1 \
  --output-root data_work/outputs/directional_cost_edgeproj_v2_lcc_gate5_v1
```

**输出根目录：**

```text
data_work/outputs/directional_cost_edgeproj_v2_lcc_gate5_v1/
```

---

## 7. 输出文件清单

### 7.1 主 pipeline

| 路径 | 内容 |
|------|------|
| `summary_metrics.json` | directed vs sym  headline |
| `run_metadata.json` | 运行参数 |
| `parameters.json` | θ, α, β |
| `fundamentals/directed/`, `symmetric/` | 反演 ū, Ā, χ |
| `equilibrium/directed_observed/`, `symmetric_cost/` | l_R, l_F, Π, P, W̄ |
| `simulations/asymmetry_intensity.csv` | ρ 网格 |
| `simulations/asymmetry_threshold.csv` | c 网格 |
| `diagnostics/reciprocal_pair_asymmetry.csv` | 边对 asymmetry |
| `diagnostics/edge_cost_comparison.csv` | τ_obs vs τ_sym |
| `diagnostics/node_directed_vs_symmetric.csv` | 节点级对比 |
| `diagnostics/directed_fit_to_observed.json` | 边际/OD fit |
| `diagnostics/od_direction_cost_qa.json` | AM 方向 QA |
| `diagnostics/node_market_access_maps.csv` | Π, P, wage index |

### 7.2 政策 CF

| 路径 | 内容 |
|------|------|
| `counterfactuals/policy_cf_summary.csv` | 5 policies × 2 worlds |
| `counterfactuals/policy_cf_world_wedge.csv` | sym−dir 楔子 |

### 7.3 潮汐车道

| 路径 | 内容 |
|------|------|
| `counterfactuals/tidal_lane/candidate_pairs_ranked.csv` | 1193 候选互惠对 |
| `counterfactuals/tidal_lane/tidal_lane_cf_summary.csv` | 26 specs × 2 horizons |
| `counterfactuals/tidal_lane/tidal_lane_short_vs_long.csv` | pivot 表 |
| `counterfactuals/tidal_lane/tidal_lane_edge_detail.csv` | 边级 τ 变化 |
| `counterfactuals/tidal_lane/tidal_lane_run_meta.json` | 运行元数据 |

### 7.4 图（figures/）

- `asymmetry_ratio_histogram.png`
- `simulation_asymmetry_intensity.png`, `simulation_asymmetry_threshold.png`
- `map_residence_access.png`, `map_firm_access.png`, `map_wage_productivity.png`
- `policy_cf_by_world.png`, `policy_cf_welfare_wedge.png`
- `tidal_lane_short_vs_long.png`
- （可选）`workplace_choice_obs_vs_model.png`

---

## 8. 数值结果摘要

### 8.1 Baseline：directed vs symmetric（geometric, re-invert）

| 对象 | 值 |
|------|-----|
| W̄ change (sym vs dir) | **−1.14%** |
| 居住/就业 share dissimilarity | **<0.001%** |
| Implied OD dissimilarity | **2.46%** |
| p95 \|log(ū_sym/ū_dir)\| | **0.55** |
| p95 \|log(Π_i^{-θ}) diff\| | **~0.52** |
| 边际 L1 vs obs | **~0.00053%** |

### 8.2 OD vs 数据（directed world，诊断）

| 指标 | ~值 |
|------|-----|
| 全局 L1 vs obs | 72% |
| 去对角 L1 | 51% |
| Origin-conditional L1 median | 66% |
| 预测 intra-zonal share | 53% vs obs 8% |
| directed vs sym implied OD | 2.46%（核心对比量） |

### 8.3 θ 敏感度（re-invert）

OD / Π  gap 对 θ∈[3,12] 几乎不变（~2.46%）；W̄ gap 从 −2.6%（θ=3）到 +0.27%（θ=12）。

### 8.4 非定向政策 CF（highlights）

**Within directed world：**

| Policy | ΔW̄ | OD dissim | 备注 |
|--------|-----|-----------|------|
| Uniform +10% speed | +0.82% | 4.6% | |
| Hub access +15% | +0.17% | 6.6% | 最大跨 world 楔子 |
| Hub productivity +10% | −0.69% | 0.45% | 区位 shock |
| Outer amenity +10% | +0.35% | 0.25% | |
| Fringe transport +20% | +0.58% | 0.55% | |

**Cross-world wedge（sym−dir，hub access）：** W̄ 响应 **−0.14 pp**；p95 Π^{-θ} **+3.7%**；人口份额 **<0.02 pp**。

**规划 outcome 映射（directed world）：**

- Hub access：外围居住 **−0.30 pp**；hub-bound OD **+0.78 pp**
- Fringe transport：外围居住 **+0.32 pp**

### 8.5 Asymmetry simulations（re-invert at each surface）

- ρ: 0→1：W̄ **+1.15%**；居住 share 几乎不动
- Threshold c=2：相对 full sym，W̄ **+0.86%**（8.9% 边对保持 directed）

### 8.6 潮汐车道原型（directed world；无拥堵 spillover）

**选边：** top-K by asym_ratio；len>200m；快向 ≥2 lanes；BPR fixed ξ 或 direct τ scale。

**默认网格：** 26 specs（top_n×Δn×min_ratio + direct τ bundles）。

| Spec | Horizon | ΔW̄ | Res dissim | OD dissim |
|------|---------|-----|------------|-----------|
| BPR top-100, Δn=1 | Short | **+0.075%** | 0 | 0.99% |
| BPR top-100, Δn=1 | Long | **−0.140%** | 0.52% | 0.93% |
| BPR top-50, Δn=1 | Long | −0.070% | 0.35% | 0.60% |
| Direct −15%/+15%, top-50 | Short | +0.079% | 0 | 0.71% |
| Direct −15%/+15%, top-50 | Long | −0.085% | 0.43% | 0.73% |

**机制解读：**

- 短期固定 shares → 慢向 τ↓ → Π↑ → W̄ 略升
- 长期 sorting 调整 → W̄ 转负（GE 反馈）
- 量级远小于 hub-access CF（全网络交通 shock vs 50–100 相邻对）

**BPR 公式（固定 ξ，无传导）：**

```text
ξ_s = n_s (max(τ^obs_s, τ^ff_s)/τ^ff_s)^(1/δ₁)
τ^new_s = max( τ^ff_s, τ^ff_s (ξ_s/(n_s+Δn))^δ₁ )
τ^new_f = max( τ^ff_f, τ^ff_f (ξ_f/(n_f−Δn))^δ₁ )
```

---

## 9. QA 与已知问题

### 9.1 AM 方向 QA（`od_direction_cost_qa.json`）

- 可达 off-diagonal 对上，SP(home→work) vs SP(work→home)：**57.4%** 前者更慢
- SP vs soft cost T^{-1/θ}：weighted log corr **0.94**

### 9.2 已知局限

1. **OD  bilateral fit 差**：反演矩只有 2N 边际；T_ii 导致 intra 质量过大
2. **re-invert 压缩 baseline sorting gap**：fundamentals 吸收 symmetric bias
3. **潮汐 prototype 只动相邻 grid 边**：效应偏小；无 network congestion
4. **参数 placeholder**：θ, α, β 未结构估计
5. **W̄ 非 headline**：更多反映 fundamentals 补偿

### 9.3 平行 track（非 Stage 1 主结论）

- `cursor/stage1-od-fit-improvement`：intra-zonal spec、θ grid、SMM
- 详见 `stage1_od_fit_diagnosis_and_handoff_20260628.md`

---

## 10. 叙事链（tex 对应章节）

1. **测量问题**：symmetric link index 混向 peak/off-peak（§Question, §Asymmetry）
2. **两 world 自洽校准**：re-invert + 同边际（§Model, §Main comparison）
3. **Headline 不是 W̄**：是 OD、Π 地图、政策 outcome（§Outcomes map）
4. **政策分类**：交通 shock → 跨 world 楔子大；区位 shock → 几乎无楔子（§Policy CF）
5. **放大 gap 的旋钮**：fixed fundamentals、θ、ρ/c 网格（§Parameters）
6. **潮汐 prototype**：定向 capacity 的第一 pass；短期/长期符号反转（§Tidal lane）
7. **Stage 2**：内生拥堵 + spillover + corridor sample（§Stage 2）

---

## 11. Git 与分支（参考）

| 分支 | 内容 |
|------|------|
| `cursor/stage1-directional-cost-reinvert` | Stage 1 主线 + 初始 tex（commit 0617371 等） |
| `cursor/stage1-od-fit-improvement` | OD fit 改进平行线 |

（政策 CF、地图、潮汐、tex 扩充可能在上述分支的未提交改动中；以 working tree 为准。）

---

## 12. 后续工作（优先级）

1. **Stage 2**：BPR/congestion 传导；tidal shock 在 corridor 样本上放大
2. **Outcome 导出**：zone-level ΔL_i^R, ΔL_ij → 学区/公交 peak load 代理
3. **Symmetric world 下同一 tidal shock** 的 cross-world 楔子
4. **OD track**：intra → cost metric → θ → SMM（见 OD handoff memo）
5. **参数**：与 You Wei 组对齐 θ, α, β, λ

---

## 13. 相关文档索引

| 文档 | 用途 |
|------|------|
| `FixedDirectedCost_Stage1_gate5.tex` | 论文级完整 note（模型+结果+复现） |
| `DIRECTIONAL_COST_MODEL_20260628.md` | 模型设计 rationale |
| `stage1_od_fit_diagnosis_and_handoff_20260628.md` | OD fit 专项 |
| `data_work/docs/_superseded/CURRENT_MODEL_EQUATIONS.md` | 全文方程 snapshot |
| `Replication_apply/python_commuting/tidal_lane_specC*.py` | AA replication Spec C 参考 |

---

*本 memo 为 Stage 1 fixed directed-cost 模块的单一 handoff 入口；细节数值以 `summary_metrics.json` 与各 CSV 为准。*
