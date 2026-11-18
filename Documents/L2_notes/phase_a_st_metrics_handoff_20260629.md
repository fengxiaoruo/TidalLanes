# Phase A — ST 指标包（gate5，top100 主线）

**日期：** 2026-06-29  
**脚本：** `data_work/src/model/full_estimation/run_phase_a_st_metrics_gate5.py`  
**输出：** `data_work/outputs/phase_a_st_metrics_gate5_v1/`

---

## 主规格

| 项目 | 值 |
|------|-----|
| Bundle | `model_input_edgeproj_v2_lcc_gate5_v1` |
| 潮汐 | **top100_d1.0**，asym 选边 |
| E6 | anchored，λ_phys=**1.1** |
| L̄ | **88,110** |

---

## 主结果（λ=1.1，top100 LT）

| 指标 | 值 | 解读 |
|------|-----|------|
| 走廊慢向 median **% 节省** | **15.9%** | 主文 headline（对 λ 相对稳健） |
| 路径暴露 OD 人流占比 | **45.1%** | 最短路径经 treated 边的 observed OD 质量占比 |
| 慢向 direct grid-pair OD | **1.0%** L̄ | 仅端点对齐，非全路径 |
| ST 直接人均 | **−0.72 s** | 固定 sorting |
| LT 总人均 | **−1.24 s** | 含 sorting |
| person-min/day（全城市） | **−1,816** | ≈ −1.24 s × L̄ |
| person-min/day（+25% 人口机械缩放） | **−2,270** | 报告层，非重解均衡 |
| W̄ LT | **−0.12%** | 附录；不作主结论 |

**λ=0.57 稳健性（top100）：** 走廊 median **10.7%**；person-min/day **−1,168**。

---

## 与 top50 对比

| spec | 走廊 % (LT) | 路径暴露 % | person-min/day (LT) |
|------|-------------|------------|---------------------|
| top50 | 15.9% | 37.8% | −1,212 |
| **top100** | **15.9%** | **45.1%** | **−1,816** |

top100 在 **受益人流覆盖** 与 **aggregate person-min** 上约 **1.5×** top50；走廊内 **% 节省** 几乎不变（政策边际在 extensive margin）。

---

## 文件

| 文件 | 内容 |
|------|------|
| `phase_a_st_metrics.csv` | 全表（含 λ=0.57、top50/100） |
| `summary.json` | top100 主线摘要 |

---

## 命令

```bash
cd data_work
python3 src/model/full_estimation/run_phase_a_st_metrics_gate5.py
```
