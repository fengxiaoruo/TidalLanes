# Phase D — 结果方向与写作结构（gate5，2026-06-29）

**前提：** anchored λ=1.1 主线；潮汐 **top100**；λ 借参数保留为 limitation。

---

## 一句话主张

潮汐车道在 **走廊尺度** 带来约 **14--16%** 的 peak 通行时间下降，约 **56%** 的模型 OD 路径暴露于 treated 走廊；全城市通勤时间几乎打平（top100 LT 约 **+95 person-min/day**），但原始 Frechet welfare 为正（top100 LT **+0.32%**），且 LT 存在可识别的职住重配 pattern。

---

## 建议章节结构

### 1. Introduction
- 问题：directional congestion + 潮汐 realloc
- 贡献：ST **% + 人流**、LT **spatial pattern**、组合 CF **政策背景**

### 2. Model & data（简短）
- QSM + Leontief + anchored E6；gate5 edgeproj travel cost
- λ_phys=1.1 documented；**不声称** λ 估计

### 3. ST — 潮汐对通行/通勤的缓解（主结果）
- **主表：** `phase_a_st_metrics.csv` → top100：median **15.9%**，路径暴露 **45%**，person-min **−1,816/day**
- **图：** treated 走廊 Δτ/τ 分布；可选 λ=0.57 附录列（**10.7%**）
- **叙事：** 人均秒数小 → 正常；**走廊 % + 受益 OD 份额** 才是政策 relevant magnitude
- **人口：** mechanical ×1.25 / ×1.5 列（附录）；全文不写 W̄ ST (+0.28%) 作 headline

### 4. LT — Sorting pattern（幅度小，pattern 为主）
- **图：** Phase B 三张 map + radial profile
- **表：** top movers（`top_movers.csv`）
- **机制段：** direct ST ↓ → GE / sorting wedge 部分对冲通勤时间 → W̄ LT 仍为正
- **诚实：** OD 拟合有限 → qualitative spatial story

### 5. Composite counterfactuals
- **表：** `composite_summary.csv`（C1–C4）
- **论点：** 走廊 % 稳定；C3/C4 下 person-min 与 |Δlog l| 略变 → 潮汐在 **职住分离 / hub 增长** 背景下仍有一阶意义
- **可选图：** C4 vs tidal_only 的 Δlog l_R map 对比

### 6. Conclusion & limitations
- λ imported；选边 asym vs Ξ（17/50→40/100 错配）；raw `(Π,P)` gauge 已修正；W̄ 非货币

---

## 输出索引

| Phase | 目录 / memo |
|-------|-------------|
| A | `outputs/phase_a_st_metrics_gate5_v1/` · `L2_notes/phase_a_st_metrics_handoff_20260629.md` |
| B | `outputs/phase_b_sorting_patterns_gate5_v1/` · `L2_notes/phase_b_sorting_patterns_handoff_20260629.md` |
| C | `outputs/phase_c_composite_cf_gate5_v1/` · `L2_notes/phase_c_composite_cf_handoff_20260629.md` |
| D | 本文 |

---

## 脚本一览

```bash
cd data_work
python3 src/model/full_estimation/run_phase_a_st_metrics_gate5.py
python3 src/model/full_estimation/run_phase_b_sorting_patterns_gate5.py --top-k 100
python3 src/model/full_estimation/run_composite_cf_gate5.py --top-k 100
```

新增代码：
- `corridor_exposure.py` — 路径暴露
- `phase_metrics_common.py` — 共享加载
- `run_phase_a_st_metrics_gate5.py`
- `run_phase_b_sorting_patterns_gate5.py`
- `run_composite_cf_gate5.py`
