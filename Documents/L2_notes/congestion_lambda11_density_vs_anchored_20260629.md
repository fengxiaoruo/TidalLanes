# λ=1.1 拥堵规格对比：长度单位化 vs observed-anchored

> **历史文档（2026-11-18 加注）。** 本文锁定的 anchored、λ = 1.1 是旧 gate5 主线的拥堵设定，11 月已改为 `ce_floor`。 当前主线（OSM 1B、全日 2025 OD＋07–09 用时、`ce_floor` β_c = 0.35）见 `DOCUMENT_INDEX.md` 的 “Current mainline” 表。下文未改。

**日期：** 2026-06-29  
**输入：** `model_input_edgeproj_v2_lcc_gate5_v1`  
**输出：** `data_work/outputs/congestion_lambda11_comparison_edgeproj_v2_lcc_gate5_v1/`  
**复现：**

```bash
python3 data_work/src/model/full_estimation/run_congestion_lambda11_comparison.py
```

---

## 1. 三种规格

| 代号 | 更新规则 | λ |
|------|----------|---|
| `legacy_aa071` | `τ ← max(τ_ff, τ_ff·(Ξ/n)^λ)` | 0.071（AA Seattle） |
| `density_lambda11` | `τ ← max(τ_ff, τ_ff·(Ξ/(n·ℓ))^λ)` | **1.1**（Barwick 物理弹性） |
| `anchored_lambda11` | `τ ← max(τ_ff, τ_obs·((Ξ/Ξ_obs)(n_obs/n))^λ)` | **1.1** |

- **长度单位化**：密度自变量 `Ξ/(n·ℓ_km)`，对齐文献 vehicles/lane-km。  
- **Anchored**：`Ξ_obs` 在固定 `τ=τ_obs` 下由模型分配得到；λ 只作用于相对观测状态的偏离。  
- Anchored 需 **damping=0.25**（λ=1.1 时 damping=0.5 外层振荡不收敛）。

代码：`equilibrium_solve.py` 新增 `congestion_form`；对比入口 `run_congestion_lambda11_comparison.py`。

---

## 2. Baseline 对比

| 规格 | W̄ | mean τ vs obs | median τ/τ_obs | 收敛 (outer) |
|------|-----|---------------|----------------|--------------|
| exogenous τ_obs（参照） | 1.861 | 0% | 1.000 | — |
| legacy (λ=0.071) | 1.817 | **−13.9%** | 0.927 | ✓ (17) |
| density (λ=1.1, ÷ℓ) | 1.892 | **+7.9%** | 0.933 | ✓ (23) |
| **anchored (λ=1.1)** | **1.861** | **+0.13%** | **1.000** | ✓ (36) |

**解读：**

1. **仅长度单位化不够**：density 形式 baseline 仍偏离观测 ~8%，median<1；uniform shock 下 **70%** 边快于 free-flow。  
2. **Anchored 贴住数据**：W̄ 与 exogenous baseline 一致，median τ/τ_obs ≈ 1.000。  
3. Legacy AA placeholder 仍严重 underfill（−14%）。

`median(Ξ_obs/(n·ℓ)) ≈ 0.35`（密度量级合理）；anchored 通过 `Ξ/Ξ_obs` 消掉任意尺度，而非靠 level form 硬凑。

---

## 3. 政策 / 潮汐 CF（ΔW̄ %，相对各自 baseline）

| 场景 | legacy | density λ=1.1 | anchored λ=1.1 |
|------|--------|---------------|----------------|
| Uniform speed +10% | +0.31% | +0.64% | **+0.02%** |
| Hub access +15% | −0.65% | +0.22% | **+0.01%** |
| Tidal top-50 long-run | −0.03% | −0.01% | **−0.06%** |

**传导差异：**

- **Legacy**：hub access 内生性转负（经典 Ξ 加载故事）。  
- **Density λ=1.1**：baseline 偏离观测，CF 符号与幅度 **不可信**（hub 变正、幅度偏大）。  
- **Anchored λ=1.1**：baseline 正确锚定后，CF 幅度 **大幅缩小**（±0.06% 量级）——政策效应不再被错误的 baseline τ 水平放大/翻转。

---

## 4. 结论

| 问题 | density（÷ℓ） | anchored |
|------|---------------|----------|
| baseline 复现 τ_obs | ✗ (+8%) | ✓ (+0.1%) |
| λ=1.1 文献语义 | 部分（量纲对，尺度仍错） | ✓（相对偏离） |
| CF 可解释性 | 低 | 高 |
| 数值 | κ=1 收敛 | κ=1，需 damp=0.25 |

**建议：** Stage 2 默认采用 **`congestion_form=anchored`，`congestion_lambda=1.1`**；density/legacy 作稳健性附录。

---

*CSV：`congestion_form_comparison.csv`*
