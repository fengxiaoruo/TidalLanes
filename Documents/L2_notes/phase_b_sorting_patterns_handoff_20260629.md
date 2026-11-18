# Phase B — LT sorting 空间 pattern（gate5，top100）

**日期：** 2026-06-29  
**脚本：** `data_work/src/model/full_estimation/run_phase_b_sorting_patterns_gate5.py`  
**输出：** `data_work/outputs/phase_b_sorting_patterns_gate5_v1/`

---

## 规格

- 对比：**baseline_endogenous** vs **tidal_top100_d1.0_long_run**
- 节点：1104（gate5 LCC）

---

## 幅度（可接受地小）

| 指标 | 值 |
|------|-----|
| max \|Δlog l_R\| | **12.4%** |
| max \|Δlog l_F\| | **12.9%** |
| 走廊邻接节点 mean Δlog l_R | **+0.027%** |
| 非邻接节点 mean Δlog l_R | **−0.32%** |

aggregate W̄ LT 仅 −0.12%，但 **个别节点可达 ~10% 相对份额变化** → 适合讲 **pattern**，不适合讲 **大 aggregate 福利**。

---

## Pattern 解读（待结合地图）

1. **径向剖面**（`radial_profile_log_lR.png`）：Δlog l_R / Δlog l_F vs 到就业 centroid 距离 — 检验是否沿径向走廊再分布。
2. **Top movers**（`top_movers.csv`）：gainers/losers 各 20 节点（l_R、l_F、Π）。
3. **走廊邻接 vs 远端**：邻接 treated 边的 169 个节点 vs 其余 — 局部效应是否更可见。

---

## 产出文件

| 文件 | 用途 |
|------|------|
| `node_sorting_changes.csv` | 全节点 Δlog l_R, Δlog l_F, Δlog Π, Δlog P + hub/outer 标记 |
| `map_log_lR_change.png` | 居住份额变化空间图 |
| `map_log_lF_change.png` | 就业份额变化空间图 |
| `map_log_Pi_change.png` | 市场可达变化 |
| `radial_profile_log_lR.csv` / `.png` | 径向剖面 |
| `top_movers.csv` | 案例节点 |

---

## 命令

```bash
cd data_work
python3 src/model/full_estimation/run_phase_b_sorting_patterns_gate5.py --top-k 100
```
