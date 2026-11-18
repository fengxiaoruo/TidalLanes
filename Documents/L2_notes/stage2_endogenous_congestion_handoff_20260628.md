# Stage 2：内生拥堵空间均衡 — 完整工作记录

**日期：** 2026-06-28（**主线更新 2026-06-29**）  
**输入 bundle：** `model_input_edgeproj_v2_lcc_gate5_v1`（raw_connected_v1 edgeproj，**非 centerline**）  
**主线输出：** `data_work/outputs/stage2_endogenous_edgeproj_v2_lcc_gate5_anchored_v1/`  
**主线规格：** `data_work/docs/STAGE2_MAINLINE_GATE5.md` — anchored E6, `λ_phys=1.1`, damping=0.25  
**旧 diagnostic 输出（保留）：** `data_work/outputs/stage2_endogenous_edgeproj_v2_lcc_gate5_v1/`  
**主 tex：** [`Documents/L1_manuscripts/EndogenousCongestion_Stage2_gate5.tex`](../L1_manuscripts/EndogenousCongestion_Stage2_gate5.tex)  
**Stage 1 前置：** [`stage1_fixed_directed_cost_complete_handoff_20260628.md`](stage1_fixed_directed_cost_complete_handoff_20260628.md)

---

## 0. 定位

在 Stage 1 **固定 τ** 的 directed calibration 上，激活 **v4 全一般均衡（GE）** 中的拥堵反馈：

- **(E5)** 链路流量 Ξ 由市场可达 (Π, P) 内生决定  
- **(E6)** 旅行时间 τ 由 free-flow 锚定 BPR 更新：τ = τ_ff (Ξ/n)^λ，λ = δ₁/θ  
- 反事实分 **两层**：先 **重做 Stage 1 全部 CF（exogenous τ）**，再 **同一 shock 下开启内生拥堵**，最后 **潮汐车道（车道重分配 + 拥堵传导）**

---

## 1. 模型方程（锁定）

### 1.1 路径聚合（与 Stage 1 相同）

```text
A_kl = 1/(κ τ_kl),   T = (I-A)^(-1),   τ_ij^(-θ) = T_ij,   κ=1 (gate5)
```

### 1.2 份额与市场可达

```text
l_i^R, l_j^F  由 (E_R)-(E_F) 交替固定点求解，χ 来自 Step 4 反演
Π_i^(-θ) = Σ_j T_ij L_j^F P_j^θ
P_j^(-θ)  = Σ_i T_ij L_i^R Π_i^θ
```

### 1.3 内生拥堵（Stage 2 主线 — anchored）

```text
t_kl = τ_kl^(1/θ)
Ξ_kl = t_kl^(-θ) P_k^(-θ) Π_l^(-θ)

τ_kl = max{ τ_obs_kl,  τ_obs_kl · ((Ξ_kl/Ξ_obs_kl)(n_obs/n_kl))^λ_phys },   λ_phys = 1.1
```

外层不动点：给定 τ → T → (l, Π, P) → Ξ → τ_new（log 阻尼 **0.25**），直至收敛。  
Baseline 在观测点复现 `τ_obs`（median ratio ≈ 1）。

**Legacy（稳健性）：** `τ = max{τ_ff, τ_ff (Ξ/n)^λ}`，`λ ≈ 0.071` — 见旧输出目录。

### 1.4 反演（Step 4，不变）

仍在 **观测 τ_obs** 上反演 (ū, Ā, χ)，矩 = 居住/就业边际。  
Baseline 均衡用 **τ_min_init = τ_obs** 锚定（`travel_cost_lock_in` 建议 Stage 2 observed-anchored）。

### 1.5 两种拥堵模式

| 模式 | 代码 flag | 含义 |
|------|-----------|------|
| `exogenous_tau` | `endogenous_congestion=False` | τ 固定在 policy 冲击后的值；**Stage 1 CF 在 level-form 求解器上的复现** |
| `endogenous` | `endogenous_congestion=True` | τ 随 Ξ 迭代；交通 shock 施加在 **τ_ff** 上 |

---

## 2. 代码结构

| 文件 | 作用 |
|------|------|
| `full_estimation/equilibrium_solve.py` | 外层拥堵不动点；支持 `chi`, `fix_tau`, `tau_ff_override` |
| `full_estimation/stage2_counterfactuals.py` | 政策 CF（双模式）、结构 CF、潮汐（内生/外生 BPR 网格） |
| `full_estimation/run_stage2_gate5.py` | **gate5 主入口** |
| `full_estimation/counterfactual_levels.py` | CF1 对称化 / CF2 潮汐 / CF3 扩车道（内生） |
| `full_estimation/lambda_diagnostic.py` | 截面 λ 诊断（不覆盖 placeholder） |
| `directional_cost/policy_counterfactuals.py` | PolicySpec 与 hub/outer 掩码（复用） |
| `directional_cost/tidal_lane_counterfactuals.py` | BPR 固定 ξ 潮汐网格（Stage 1 逻辑，Stage 2 复跑） |

---

## 3. 复现命令

```bash
# 从 repo 根目录（默认 --lambda-mode diagnostic：先 probe 再重跑）
python3 data_work/src/model/full_estimation/run_stage2_gate5.py

# 快速 smoke（25 对潮汐、单 CF3 factor、1 个 BPR spec）
python3 data_work/src/model/full_estimation/run_stage2_gate5.py --quick

# 跳过 Stage 1 风格 BPR 网格
python3 data_work/src/model/full_estimation/run_stage2_gate5.py --skip-tidal-grid

# λ 模式
python3 data_work/src/model/full_estimation/run_stage2_gate5.py --lambda-mode placeholder
python3 data_work/src/model/full_estimation/run_stage2_gate5.py --lambda-mode custom --lambda-value 0.05
```

---

## 4. 输出清单

| 路径 | 内容 |
|------|------|
| `baseline_exogenous_tau/` | Stage 1 等价 baseline（固定 τ_obs） |
| `baseline_endogenous/` | 内生拥堵 baseline |
| `baseline_summary.json` | 两 baseline 对比、λ 诊断 |
| `lambda_diagnostic.json` | 截面回归 λ̂ |
| `counterfactuals/policy_cf_all_modes.csv` | 5 政策 × 2 拥堵模式 |
| `counterfactuals/structural_cf_endogenous.csv` | CF1/CF2/CF3（内生） |
| `counterfactuals/tidal_lane_endogenous_summary.csv` | 潮汐 + 内生 |
| `counterfactuals/tidal_lane_exogenous_bpr_grid.csv` | Stage 1 BPR 网格复跑 |

---

## 5. 数值结果（diagnostic λ ≈ 0.0165，完整跑通 2026-06-29）

默认入口 `--lambda-mode diagnostic`：Phase 1 用 placeholder λ=0.071 probe → λ̂≈0.017；Phase 2 用 λ=λ̂ 重跑全部 CF。

### 5.1 Baseline

| 对象 | W̄ | τ mean vs obs | 收敛 |
|------|-----|---------------|------|
| Exogenous τ（固定 obs） | 1.861 | 0% | yes |
| Endogenous（λ=0.0165） | **1.782** | **−19.7%** | yes，18 outer iters |
| λ 诊断（lane-bin FE） | λ̂ ≈ **0.0163** | R²≈0.032 | 与所用 λ 自洽 |

**对比 placeholder λ=0.071（旧 quick 跑）：** endogenous W̄=1.817，τ underfill −13.9%。诊断 λ 进一步压低均衡 τ，hub-access 负福利更大。

### 5.2 政策 CF（相对各自 baseline）

| Policy | Exog ΔW̄ | Endog ΔW̄ | Exog OD% | Endog OD% | Endog ΔΞ mean |
|--------|----------|-----------|----------|-----------|---------------|
| Uniform +10% speed | **+0.81%** | +0.15% | 4.6 | 8.6 | +18.0% |
| Hub access +15% | +0.16% | **−1.00%** | 6.6 | 13.2 | +2.1% |
| Hub productivity +10% | −0.68% | −0.59% | 0.45 | 0.40 | −1.4% |
| Outer amenity +10% | +0.35% | +0.30% | 0.25 | 0.20 | +0.8% |
| Fringe transport +20% | +0.56% | +0.58% | 0.55 | 0.54 | +20.3% |

**传导机制（相对 Stage 1 exogenous）：**

1. **交通 shock + 内生拥堵**：福利增益被 **Ξ 上升 → τ 回弹** 吸收（uniform：0.81% → 0.15%）。  
2. **Hub 接入**：固定 τ 下小幅正福利；内生下 **hub 边 Ξ 加载 → τ 上升 → ΔW̄ 转负（−1.0%）**。  
3. **区位 shock**（ū/Ā）：两模式几乎相同（不经过 Ξ 通道）。

### 5.3 结构 CF（相对 baseline_endogenous，geometric CF1）

| CF | ΔW̄ | OD dissim |
|----|-----|-----------|
| CF1 symmetric **geometric** | ≈0 | ≈0 |
| CF1 symmetric **arithmetic** | ≈0 | ≈0 |
| CF2 tidal top-50 Δn=1 | −0.011% | 0.05% |
| CF3 uniform lanes ×1.10 | −0.015% | 0.12% |
| CF3 uniform lanes ×1.25 | −0.066% | 0.28% |
| CF3 uniform lanes ×1.50 | −0.22% | 0.50% |

### 5.4 潮汐车道（内生，short-run vs long-run）

| Spec | Horizon | ΔW̄ | OD dissim | Pairs |
|------|---------|-----|-----------|-------|
| top-50, Δn=1 | short-run（钉份额） | +0.005% | 0.05% | 50 |
| top-50, Δn=1 | long-run | −0.011% | 0.05% | 50 |
| top-50, Δn=0.5 | short-run | +0.003% | 0.03% | 50 |
| top-50, Δn=0.5 | long-run | −0.006% | 0.03% | 50 |
| top-100, Δn=1 | short-run | +0.010% | 0.07% | 100 |
| top-100, Δn=1 | long-run | **−0.017%** | 0.07% | 100 |

**外生 BPR 网格**（14 spec × 2 horizon = 28 行）：ΔW̄ ∈ [−0.04%, +0.01%]；short-run fixed shares 与 exogenous τ 行并列输出。

---

## 6. 与 Stage 1 的对照

| 维度 | Stage 1 | Stage 2 |
|------|---------|---------|
| τ | 外生 | 内生 (E6) |
| Ξ | 潮汐 BPR 中固定 | 每轮由 (E5) 更新 |
| 政策 CF baseline | directed re-invert world | exog / endog 双 baseline |
| 福利 headline | 次要 | 仍受 λ 校准影响 |
| 关键新增 object | — | **ΔΞ**, τ 回弹, exog vs endog wedge |

Stage 1 `policy_cf` 的 exogenous_tau 行与 Stage 2 `exogenous_tau` 列应数值接近（同一 level-form 求解器 + χ）。

---

## 7. 已知问题与下一步

1. **τ underfill（−19.7%）**：即使 diagnostic λ，均衡 τ 仍低于 τ_obs；需 structural λ 或 τ 重锚定。  
2. **Directed vs symmetric calibration world**：Stage 2 目前单 world 反演；可扩展双 world。  
3. **RuntimeWarning（matmul overflow）**：部分 CF 外层迭代出现数值警告，未阻断收敛；可后续加 clipping。  
4. **Tex/PDF**：`EndogenousCongestion_Stage2_gate5.tex` 已更新 diagnostic λ 数值；可重新编译 PDF。

**已完成（2026-06-29）：** diagnostic λ 两阶段 pipeline；CF1 geometric；完整 BPR 网格 + top-50/100 潮汐 short/long-run。

---

## 8. 估计议程（未实现）

- [x] 用 baseline Ξ 诊断校准 λ（`--lambda-mode diagnostic`，两阶段 probe + 重跑）  
- [ ] θ, α, β 与 You Wei 组对齐  
- [ ] 面板/IV λ（v4 §6.3 原计划）  
- [ ] 定向 link traffic 观测接入后替换 (E5) 校准

---

*完整数值以 `data_work/outputs/stage2_endogenous_edgeproj_v2_lcc_gate5_v1/` 下 CSV/JSON 为准。*
