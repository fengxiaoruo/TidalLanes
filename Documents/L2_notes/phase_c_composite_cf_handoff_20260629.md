# Phase C — 组合反事实（background → tidal top100）

**日期：** 2026-06-29  
**脚本：** `data_work/src/model/full_estimation/run_composite_cf_gate5.py`  
**输出：** `data_work/outputs/phase_c_composite_cf_gate5_v1/`

---

## 设计

顺序：**先背景 policy 内生均衡 → 再在该状态下施 tidal top100（δ=1）**。

| ID | 背景 shock | 政策含义 |
|----|-----------|----------|
| tidal_only | （无） | 基准对照 |
| C1 | fringe_transport −20% | 外环通行改善 |
| C2 | outer_ring_amenity +10% | 郊区居住 amenity ↑（郊区化） |
| C3 | hub_access −15% | 枢纽接入提速 |
| C4 | hub_productivity +10% | 枢纽生产率 ↑（就业极化） |

---

## 主结果（top100 LT，λ=1.1）

| composite | 背景 W̄ | 走廊 median % 节省 | 路径暴露 % | person-min/day (LT) | W̄ LT Δ |
|-----------|---------|-------------------|------------|---------------------|---------|
| tidal_only | 1.861 | **15.85%** | 45.1% | **−1,816** | −0.120% |
| C1 fringe | 1.861 | 15.85% | 45.1% | −1,788 | −0.124% |
| C2 outer amenity | 1.867 | 15.85% | 45.1% | −1,827 | −0.121% |
| C3 hub access | 1.861 | **15.95%** | 45.1% | −1,768 | −0.127% |
| C4 hub prod | 1.849 | 15.84% | 45.1% | **−1,843** | −0.118% |

### 解读

1. **走廊 % 节省** 在各背景下 **极稳定（~15.8–16.0%）** — 潮汐 technology 的局部效应与 citywide fundamentals 近似可加。
2. **person-min/day** 在 C4（就业 hub 强化）下 **略放大**（−1,843 vs −1,816），与更高 baseline 通勤成本（6.06 min avg）一致。
3. **C3 hub access** 背景略 **提高** 走廊 % 节省（15.95%）— 枢纽接入改善后潮汐边际略大。
4. **W̄ LT** 在所有情景下仍 **~−0.12%** — 再次支持主文 pivot 到 **% + person-min + pattern**，而非 aggregate W̄。
5. **max |Δlog l|** 在 C1/C4 略升（~12.5–14.2%）— 背景 shock 略放大 sorting 幅度，供 Phase B 式地图对照。

各 composite 子目录含 `background/`、`tidal_lt/` 均衡数组，可复跑 Phase B 地图。

---

## 命令

```bash
cd data_work
python3 src/model/full_estimation/run_composite_cf_gate5.py --top-k 100
# 快速 smoke：--quick（仅 tidal_only + C1）
```

---

## 后续（可选）

- 对 C2/C4 复跑 **Phase B 地图**，对比 sorting pattern 是否随背景改变方向。
- **L̄ +25% 重解** + tidal（C5）— 尚未实现。
