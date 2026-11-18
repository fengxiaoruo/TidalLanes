# 潮汐车道 ST/LT 诊断：拥堵成本分解

**日期：** 2026-06-29；福利口径更新于 2026-07-01  
**规格：** anchored congestion，λ=1.1，damping=0.25  
**复现：**

```bash
python3 data_work/src/model/full_estimation/run_tidal_lane_diagnostics_gate5.py
```

**输出：** `data_work/outputs/tidal_lane_diagnostics_edgeproj_v2_lcc_gate5_v1/`

---

## 为什么要看这些指标

W̄ 已改为原始 Frechet denominator。通勤成本和福利是不同对象，更可见的通勤对象包括：

1. **一阶通勤成本（ST）**：固定 baseline OD 份额，只看 τ 网络变化 → `direct_st_cost_change_sec`
2. **长期总成本（LT）**：份额重排 + τ 全反馈 → `total_lt_cost_change_sec`
3. **排序楔子**：LT − ST 中来自 OD 重配的部分 → `sorting_wedge_sec`
4. **处理边 τ 变化**：slow 方向 −15% / fast 方向 +25% 量级（见 `tidal_treated_edge_detail.csv`）

---

## gate5 结果摘要（anchored λ=1.1）

### ST vs LT（平均通勤路线成本，秒/人）

| Spec | ΔW̄ ST | ΔW̄ LT | 一阶 ST Δcost | 排序楔子 | LT 总 Δcost |
|------|--------|--------|---------------|----------|-------------|
| top-25 Δn=1 | +0.44% | +0.14% | **−0.97 s** | +0.98 s | **+0.65 s** |
| top-50 Δn=1 | +0.67% | +0.23% | **−1.35 s** | +0.88 s | **+0.40 s** |
| top-50 Δn=0.5 | +0.32% | +0.12% | **−0.73 s** | +0.49 s | **+0.22 s** |
| top-100 Δn=1 | +0.87% | +0.32% | **−1.63 s** | +0.71 s | **+0.06 s** |

解读：

- **ST 一阶**：钉住份额时，通勤成本下降 **0.7–1.6 秒**。
- **LT 总效果**：排序后 net 接近零，top100 为 **+0.06 秒**；W̄ 仍为正。
- **ST vs LT**：长期排序抵消大部分通勤时间收益，但不再翻转原始福利符号。

### 处理走廊（long-run，边级）

| Spec | slow Δτ | fast Δτ |
|------|---------|---------|
| top-50 | **≈ −15.6%**（≈ −206 s 均值*） | **≈ +25.9%** |
| top-100 | ≈ −15.8% | ≈ +26.4% |

\* 秒数为边级原始分钟差×60；不对称走廊上 slow/fast 量级大，但全网 OD 加权后平均只有亚秒级。

---

## 文件说明

| 文件 | 内容 |
|------|------|
| `tidal_st_lt_compact.csv` | 幻灯片用窄表 |
| `tidal_st_lt_congestion_summary.csv` | 全部分解字段 |
| `tidal_treated_edge_detail.csv` | 每条 slow/fast 边的 τ、Ξ 变化（ST+LT） |

---

## 如何让结果“更明显”

1. **报告层**：主表并列 **Δcost（秒）+ W̄ + 处理边 Δτ%**。  
2. **规格层**：增大 `top_k` 或 `delta_lanes`（top-100 一阶 −1.63 s）。  
3. **分解层**：始终并列 **ST / LT**，说明排序楔子。  
4. **空间层**：从 `tidal_treated_edge_detail.csv` 做走廊地图（slow 绿/fast 红）。

---

*模块：`tidal_lane_diagnostics.py`，入口：`run_tidal_lane_diagnostics_gate5.py`*
