# Gate5 lane-fix 重跑计划（2026-06-29）

## 背景

- **Travel cost 已重建**：`model_input_edgeproj_v2_lcc_gate5_v1`（path-level lane 接 raw roadseg_id；gate5 2642 edges；lanes 不再坍缩为常数）。
- **旧 downstream 全部作废**：`stage2_*`、`tidal_lane_diagnostics_*`、`phase_*`、`directional_cost_*` 均为 lane fix 前结果。

## 参数策略（双轨）

| 轨道 | α | β | 求解器 | 角色 |
|------|---|---|--------|------|
| **neg** | −0.12 | −0.10 | damping=0.25, max_outer=120, tol=1e-5 | Seattle net；通勤 person-min 叙事 |
| **pos** | **+0.05** | −0.10 | damping=0.10, max_outer=500, tol=1e-4 | 文献中段正 α；传导较 +0.10 温和、可收敛 |

**为何 α_pos=0.05：** 介于 Ahlfeldt/Combes 量级（0.05–0.07）与 AA 主文（0.10）之间；旧 bundle 上 sorting 强于 α=0.02 但弱于 0.10；预期 W̄/person-min 可辨、数值稳定。

## 主线路径（不变）

- Input: `model_input_edgeproj_v2_lcc_gate5_v1`
- Congestion: **anchored E6**, λ_phys=**1.1**
- Tidal 选边: **asym**, top100 主线

## 执行矩阵

| 步骤 | 内容 | 输出 suffix |
|------|------|-------------|
| 1 | Stage2 全套件（exog/endog baseline、policy CF、CF1–3、tidal top25/50/100 ST+LT） | `stage2_*_lanefix_alpha_{neg\|pos05}_v1` |
| 2 | Tidal ST/LT 诊断 + OD 分解 | `tidal_lane_diagnostics_*_lanefix_alpha_{neg\|pos05}_v1` |
| 3 | Phase A 主指标（top100 走廊 %、路径暴露、person-min） | `phase_a_*_lanefix_alpha_{neg\|pos05}_v1` |
| 4 | 双轨对比汇总 CSV + memo | `outputs/gate5_lanefix_rerun_comparison_v1/` |

**Phase B/C（sorting 地图、composite CF）**：Stage2+tidal 完成后按需追加；不阻塞主对比表。

## 验收标准

- baseline endogenous **converged**；median τ/τ_obs ≈ 1.0
- tidal top100 LT **converged**
- 两轨 **corridor %Δτ** 可比（预期 ~15% 量级，lane fix 后数值待更新）
- 汇总表含：W̄ ST/LT、person-min、sorting wedge、corridor %、baseline 校准

## 命令入口

```bash
cd data_work
python3 src/model/full_estimation/run_gate5_lanefix_rerun_batch.py
python3 src/model/full_estimation/run_gate5_lanefix_rerun_batch.py --only neg   # 仅负 α
python3 src/model/full_estimation/run_gate5_lanefix_rerun_batch.py --only pos05
```
