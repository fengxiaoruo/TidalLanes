# Replication_apply — 工作计划与技术备忘

> 最后更新：2026-04-28
> 用途：新 session 快速接入该子目录的工作，无需重读大量历史对话。

---

## 1. 这个目录做什么

把 AA (2022) commuting counterfactual 框架应用于本项目（北京 tidal lanes）的 square grid 数据，估计每条有向边通行成本下降 1% 对城市福利与居住/就业人口分布的影响。

**目标**：对比两种 baseline traffic matrix 的影响差异：
- **ASYM**：用方向性真实流量 `xi_ij`（每个方向独立估计，非对称）
- **SYM**：对称化版本 `xi_sym = (xi_ij + xi_ij.T) / 2`

---

## 2. 数据来源与输入文件

### 核心 QSM 输入（来自 `data_work/outputs/manual_centerline_rules_v11/data/`）
| 文件 | 内容 |
|------|------|
| `qsm_input_nodes_square.parquet` | N=2729 个节点，residents, jobs, grid_id |
| `qsm_input_edges_square.parquet` | AM 期 4087 条有向边，tau_obs_min, tau_ff_min, lanes_directional 等 |
| `qsm_input_od_square.parquet` | OD commuter flows（reachable_AM 过滤） |

### 车道估计（来自 `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/data/`）
| 文件 | 关键列 |
|------|-------|
| `raw_connected_vs_old_adjacent_adjacent_square_AM.csv` | `home_grid`, `work_grid`, `lane_est_path_raw_edgeproj_len_weighted`, `lane_est_path_raw_edgeproj_bottleneck` |
| Join key | `home_grid = grid_o`, `work_grid = grid_d` in edges parquet |

---

## 3. 数据准备 Pipeline（v2 版本）

**入口脚本**：`prepare_from_data_work_square_v2/build_inputs_from_data_work_square_v2.py`

**输出**：`aa_input_square_v2/` 内三个 CSV（同时复制到 `counterfactuals/seattle/`）：
- `sparse_adjmat_seattle.csv` — `[i, j, traffic, length_m, tau_obs_min]`
- `node_lr_lf_seattle.csv` — `[idx, l_r_residents, l_f_jobs, 0, 0]`
- `sparse_commute_seattle.csv` — `[home_i, work_i, commuters_road]`

**Traffic 反推公式**（来自论文 congestion regression 逆推）：
```
log(tau_obs/tau_ff) = delta1 * log(AADT/lanes)
=> traffic = lanes * (tau_obs/tau_ff)^(1/delta1)
delta1 = 0.488 (Seattle 估计值，论文 Table)
```

**车道优先级**：`len_weighted` → `bottleneck` → `lanes_directional` → 2.0（fallback）

**非对称性来源**：每条 AM 有向边有独立的 tau_obs，方向不同时间不同，因此 traffic 天然非对称。验证：93.9% 的双向边 traffic 值不同。

**运行方式**：
```bash
cd /path/to/TidalLanes
python Replication_apply/prepare_from_data_work_square_v2/build_inputs_from_data_work_square_v2.py
```
注意：脚本中 ROOT 用 `/Users/fxr/Desktop/TidalLanes`（用户机器路径），sandbox 需手动映射。

---

## 4. 模型参数（AA 2022 Seattle 版本）

| 参数 | 值 | 含义 |
|------|-----|------|
| θ (theta) | 6.83 | commuting elasticity |
| δ₁ (delta1) | 0.488 | congestion regression coefficient |
| λ (lambd) | (1/θ)×δ₁ = 0.07144 | derived |
| α (alpha) | -0.12 | residential amenity elasticity |
| β (beta) | -0.1 | productivity elasticity |

**counterfactual shock**：`t_bar_hat[i-1, j-1] = 0.99`（目标边通行成本降低 1%）

---

## 5. 求解器设置与性能

**Python 实现**：`python_commuting/` 目录（从 AA MATLAB 移植）

| 参数 | 推荐值 | 说明 |
|------|--------|------|
| `tol` | 1e-4 | 对 1% shock 足够精度，比 1e-6 快 2.5x |
| `inner_update` | 0.3 | damping factor，比默认 0.1 快 1.8x，稳定 |
| `workers` | 4 | ProcessPoolExecutor 并行，sandbox 有 4 核 |

**典型耗时（sandbox，N=2729）**：
- 1 条边：~2.3s（tol=1e-4, inner_update=0.3）
- 全部 4087 条边 × 2 版本，4 workers：~80 分钟
- sandbox 单次 bash call 上限 ~40s → 需要约 120 次调用

**已知数值问题与修复**：
- 110 个节点 l_r=0，58 个节点 l_f=0 → `log(0)=-inf` → 全矩阵 NaN
- 修复：`l_r = max(l_r_raw, 1e-4)` 后归一化（在 `compare_asym_vs_sym.py` 中已实现）

---

## 6. 比较估计脚本

**主脚本**：`python_commuting/compare_asym_vs_sym.py`  
**批次脚本**：`python_commuting/compare_batch.py`（支持断点续跑）

**输出目录**：`results/asym_vs_sym/`

| 文件 | 内容 |
|------|------|
| `chi_lr_lf_asym.csv` | `[edge_i, edge_j, node_k, chi_hat, l_r_hat, l_f_hat]` ASYM 版 |
| `chi_lr_lf_sym.csv` | 同上，SYM 版 |
| `errors_asym.csv` | `[edge_i, edge_j, err_eqm1, err_eqm2]` |
| `errors_sym.csv` | 同上 |
| `compare_summary.csv` | 每条边的 chi_hat / mean_lr_hat / mean_lf_hat 对比 |
| `progress.json` | 断点记录（已完成的边列表） |

**批次运行**（推荐每次 30 条边）：
```bash
cd Replication_apply/python_commuting
python compare_batch.py --batch-size 30 --workers 4 --tol 1e-4 --inner-update 0.3
# 重复调用直到 progress.json 显示全部完成
```

**在用户 Mac 上全量运行**（推荐）：
```bash
cd /Users/fxr/Desktop/TidalLanes/Replication_apply/python_commuting
python compare_asym_vs_sym.py --workers 8 --tol 1e-4 --inner-update 0.3
# 预计 ~40 分钟（8核 Mac）
```

---

## 7. 分析脚本

**脚本**：`python_commuting/analyze_comparison.py`（待完成）

输入：`results/asym_vs_sym/compare_summary.csv`

目标分析：
1. `chi_hat` 分布：ASYM vs SYM，每条边的福利变化幅度
2. `d_chi_hat = chi_hat_asym - chi_hat_sym`：非对称化引起的福利差异分布
3. `l_r_hat / l_f_hat`：哪些节点的人口分布在两个版本下差异最大
4. 与边的 traffic 水平、非对称程度（|xi[i,j] - xi[j,i]|）的相关性

---

## 8. 快速接入检查清单

新 session 接入时，按顺序确认：
```
[ ] aa_input_square_v2/ 三个 CSV 存在且行数正确（4087 edges, 2729 nodes）
[ ] results/asym_vs_sym/progress.json 中已完成边的数量
[ ] python_commuting/compare_batch.py 存在
[ ] scipy 已安装（pip install scipy --break-system-packages）
[ ] pyarrow 已安装（pip install pyarrow --break-system-packages）
```

---

## 9. 文件树摘要

```
Replication_apply/
├── PLAN.md                          ← 本文件
├── aa_input_square_v1/              ← 旧版输入（linear approx traffic）
├── aa_input_square_v2/              ← 当前版本（论文公式 traffic）
├── counterfactuals/seattle/         ← 与 aa_input_square_v2 内容相同（脚本自动复制）
├── prepare_from_data_work_square_v1/
├── prepare_from_data_work_square_v2/
│   └── build_inputs_from_data_work_square_v2.py  ← 数据准备入口
├── python_commuting/
│   ├── counterfactual.py            ← AA exact-hat solver（inner_update 已参数化）
│   ├── data_io.py                   ← CSV 加载器
│   ├── static_eqm.py                ← 静态均衡求解（仅 Figure 1 用，本项目不需要）
│   ├── compare_asym_vs_sym.py       ← 完整对比脚本（一次性运行）
│   ├── compare_batch.py             ← 断点续跑批次脚本
│   └── analyze_comparison.py        ← 结果分析脚本
└── results/asym_vs_sym/
    ├── chi_lr_lf_asym.csv
    ├── chi_lr_lf_sym.csv
    ├── compare_summary.csv
    ├── progress.json
    └── run.log
```
