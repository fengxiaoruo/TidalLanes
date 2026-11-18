# Gate5 lane-fix 重跑结果汇总（2026-06-29）

**Input:** `model_input_edgeproj_v2_lcc_gate5_v1`（lane lookup 已修正；n_kl 1074 个唯一值，p50≈3.37）

**Batch 脚本:** `data_work/src/model/full_estimation/run_gate5_lanefix_rerun_batch.py`

---

## 参数双轨

| 轨道 | α | β | 求解器 | 用途 |
|------|---|---|--------|------|
| **neg** | −0.12 | −0.10 | damping=0.25, max_outer=120 | Seattle net；通勤叙事 |
| **pos05** | **+0.05** | −0.10 | damping=0.10, max_outer=500, tol=1e-4 | 文献中段正 α；传导温和 |

两轨均 **baseline + tidal top100 LT 收敛**；median τ/τ_obs ≈ **1.000**。

---

## 主结果（top100, λ=1.1, asym）

| 指标 | α=−0.12 (neg) | α=+0.05 (pos05) | lane fix 前 (neg, 旧) |
|------|---------------|-----------------|----------------------|
| 走廊慢向 median %Δτ (LT) | **15.15%** | **13.95%** | ~15.85% |
| 路径暴露 OD 份额 | **45.1%** | **45.1%** | ~45.1% |
| ST 直接 (s/人) | −0.79 | −0.79 | ~−0.72 |
| sorting wedge (s) | **−1.09** | **+0.86** | ~−0.95 |
| LT 总通勤 (s/人) | **−1.40** | **+0.60** | ~−1.24 |
| person-min/day (LT) | **−2,051** | **+884** | ~−1,816 |
| W̄ ST Δ | +0.32% | +0.32% | +0.28% |
| W̄ LT Δ | **−0.16%** | **−0.37%** | −0.12% |

**解读：**

1. **Lane fix 对走廊 % 略降 ~0.7–2pp**（车道异质性恢复后，潮汐 realloc 的局部 τ 响应略不同）；**路径暴露几乎不变**。
2. **neg 轨：person-min 叙事更强**（−2,051 vs 旧 −1,816）；sorting 仍帮省通勤。
3. **pos05 轨：文献正 α + 收敛**；sorting 仍反超 ST（+884 person-min）。本 memo 的 W̄ 符号来自 2026-07-01 前 raw `(Π,P)` gauge 口径；主线福利已改用原始 Frechet denominator，需引用新输出。
4. **α=0.05 是「可收敛 + 传导适中 + 文献可辩护」的折中**；若还要 LT person-min 接近打平，需再降到 **α≈0.02**（见此前扫描）。

---

## 输出目录

| 内容 | neg | pos05 |
|------|-----|-------|
| Stage2 全套 | `outputs/stage2_endogenous_edgeproj_v2_lcc_gate5_anchored_lanefix_neg_v1/` | `..._lanefix_pos05_v1/` |
| Tidal ST/LT | `outputs/tidal_lane_diagnostics_edgeproj_v2_lcc_gate5_lanefix_neg_v1/` | `..._lanefix_pos05_v1/` |
| Phase A | `outputs/phase_a_st_metrics_gate5_lanefix_neg_v1/` | `..._lanefix_pos05_v1/` |
| 对比表 | `outputs/gate5_lanefix_rerun_comparison_v1/alpha_tracks_comparison.csv` | 同左 |

Stage2 含：exog/endog baseline、policy CF、CF1–3、tidal top25/50/100 ST+LT、exog BPR grid。

---

## 尚未重跑（低优先级）

- Phase B sorting 地图
- Phase C composite CF (C1–C4)
- `directional_cost_edgeproj_v2_lcc_gate5_v1/`（fixed-τ 层）
- Replication_apply Spec C

---

## 建议主文口径

- **主规格：** 可报 **α=+0.05** 作文献对齐；**附录** 并列 **α=−0.12** 的 person-min / sorting 对照。
- **Headline：** 走廊 **~14–15%** + 路径暴露 **~45%**（lane-fix 后更新数字）。
- **W̄：** 两轨 LT 均附录级（≈0）；不要作 headline。
- **person-min：** 明确标注 α 符号决定 LT 符号；正 α 下 ST 仍省、LT  net 增。

---

## 复现

```bash
cd data_work
python3 src/model/full_estimation/run_gate5_lanefix_rerun_batch.py
python3 src/model/full_estimation/run_gate5_lanefix_rerun_batch.py --only neg --skip-stage2  # 仅补 tidal
```
