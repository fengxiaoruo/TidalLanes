# Model Audit Learning Guide, 2026-06-28

用途：把当前模型文档、历史尝试、代码入口和结果输出对齐。读完这份文档，应能回答四个问题：最新版本是什么，代码如何把参数和变量传导到结果，做过哪些尝试，下一步应该先修什么。

## 1. 一句话结论

当前最新主线不是旧的 AA exact-hat replication，也不是 3 月的 `spatial_equilibrium.py` prototype，而是 2026-05-14 以后落在 `data_work/src/model/full_estimation/` 的 level-form scaffold v1。它对应 `Documents/L1_manuscripts/TidalLanes_Model_v4.tex` 的完整模型想法，但当前仍是 scaffold：不估参数，不用 exact-hat，先把 baseline inversion、level equilibrium 和 CF1/CF2/CF3 counterfactual 流程跑通。

当前最新结果目录是：

- `data_work/outputs/full_estimation_square_v1/`

这批结果已经生成 baseline 和 CF1/CF2/CF3，但不能直接作为论文模型结果解释。最主要原因是：

- `CF1_symmetric` 在代码里只改变 solver 初始值，没有改变持久的成本或容量 primitive，结果几乎等于 baseline。
- baseline equilibrium 有 46.5% 的边低于 free-flow time，70.9% 的边低于 observed time，说明现有拥堵方程没有把 free-flow 当成下界，也没有把 baseline 锚定到观测速度面。
- 输出和当前代码存在版本漂移：当前 `invert_fundamentals.py` 默认 `max_iter=800`，但结果里的 `fundamentals_meta.json` 记录 `n_iter=1512`；`run.log` 只记录到 Step 2，不能完整复现这次输出。

## 2. 读者先看哪里

按这个顺序读，最省时间。

1. `nextstep.md`
   - 当前项目状态。它明确说模型线近期先搭 full estimation 框架和 counterfactual 流程，暂不做参数估计，也暂不考虑 exact-hat。
2. `Documents/L2_notes/meeting_notes_20260502.md`
   - 决定了三个约束：grid travel cost 仍开放，参数要参考尤炜老师相关工作，模型先建框架不先估计。
3. `data_work/src/model/full_estimation/README.md`
   - 最新代码包的入口说明。
4. `data_work/src/model/full_estimation/WORKFLOW.md`
   - 每一步输入输出的 schema。
5. `data_work/src/model/full_estimation/ESTIMATION_NOTES.md`
   - 公式、参数来源、solver 选择和 TODO。
6. `Documents/L1_manuscripts/TidalLanes_Model_v4.tex`
   - 最新完整模型文本。
7. `Documents/L1_manuscripts/TidalLanes_Model_minimal.md`
   - v4 的中文最简说明，适合快速理解直觉。
8. `Replication_apply/results/tidal_lane/summary_specC.txt`
   - 旧 exact-hat Spec C 的代表性结果。

## 3. 最新版本和旧版本怎么区分

### 3.1 最新主线：full_estimation v1

路径：

- 代码：`data_work/src/model/full_estimation/`
- 输出：`data_work/outputs/full_estimation_square_v1/`
- 文档：`README.md`、`WORKFLOW.md`、`ESTIMATION_NOTES.md`
- 模型文本：`Documents/L1_manuscripts/TidalLanes_Model_v4.tex`

默认输入：

- `version_id = manual_centerline_rules_v11`
- `grid_type = square`
- `period = AM`
- `od_pop_col = commuters_road`

当前输出摘要：

- nodes: 2736
- directed edges: 4087
- road commuters `Lbar`: 95145.589
- parameter placeholder: `theta=6.83`、`alpha=-0.12`、`beta=-0.10`、`lambda=0.488/6.83=0.071449`
- baseline `Wbar=4.531321`
- baseline solver converged in 16 outer iterations
- lambda diagnostic: `lambda_hat=0.01214`，`R2=0.02185`

运行命令应优先写成：

```bash
python3 data_work/src/model/full_estimation/run_full_estimation.py \
  --version-id manual_centerline_rules_v11 \
  --grid-type square \
  --period AM \
  --output-name full_estimation_square_v1_rerun
```

当前机器上 `python` 命令不可用，文档里的 `python ...` 会失败。若用 module 形式，需要先确认 `PYTHONPATH` 或包路径。`nextstep.md` 里的 `python -m src.model.full_estimation.run_full_estimation` 从项目根目录直接跑，很可能找不到 `src` 包。

### 3.2 旧 prototype：spatial_equilibrium.py

路径：

- `data_work/src/model/spatial_equilibrium.py`
- `data_work/src/model/run_spatial_equilibrium.py`
- `data_work/src/model/run_counterfactual_suite.py`

这条线从 3 月到 4 月使用较多。它有 soft route assignment、diagnostic `theta`、diagnostic `lambda`、fundamental inversion 和 congestion fixed point。后来被审查为 prototype，因为：

- 早期 `t_{kl}` 和分钟 travel time 混用。
- route block 是工程近似，不是干净的 AA route aggregator。
- `lambda` 来自截面 proxy，不是结构识别。
- 很多旧输出没有收敛。

代表性旧输出：

- `data_work/outputs/raw_rebuild_validation/model_square_baseline/`
  - baseline 不收敛，100 iterations。
  - `theta_hat≈0.658`，`lambda_hat=0.01`。
- `data_work/outputs/manual_centerline_rules_v11/model_square_baseline_centerline_v11/`
  - baseline 收敛。
  - top-5 tidal lane welfare 约 `+0.0935%`，但 weighted commute time 上升约 `0.0108` 分钟。

这些结果只能作为历史调试和直觉来源，不是当前主模型结果。

### 3.3 旧 exact-hat replication：Replication_apply

路径：

- `Replication_apply/python_commuting/`
- `Replication_apply/aa_input_square_v2/`
- `Replication_apply/results/tidal_lane/`
- `Replication_apply/results/tidal_lane_raw_connected_v2/`

这条线把 AA Seattle commuting package 改造成北京数据版本。它和最新 full_estimation 不共享 Python imports。

核心思想：

- 把北京数据整理成 AA 三个输入文件：`node_lr_lf_seattle.csv`、`sparse_adjmat_seattle.csv`、`sparse_commute_seattle.csv`。
- `sparse_adjmat_seattle.csv` 的第三列在当前北京版本里是 `Xi_ij`，不是 `tau_ij`。
- 用 BPR inversion 从 `tau_obs/tau_ff` 和 lanes 构造 baseline traffic input：

```text
Xi_ij = lanes_ij * (max(tau_obs_ij, tau_ff_ij) / tau_ff_ij)^(1/delta1)
```

代码中实际实现的是这个 free-flow-floored BPR inversion。

代表性尝试：

- Spec A: 对称化目标时间后反推出 traffic shock，福利变差。
- Spec B: 直接用 travel-time ratio 做 cost shock，福利改善。
- Spec C: 物理 lane reallocation，先固定流量用 BPR 算 `tau_new`，再把 `tau_new/tau_obs` 作为 exact-hat cost shock。

当前 Spec C v1 结果：

- `Replication_apply/results/tidal_lane/summary_specC.txt`
- treated pairs: 100
- asymmetry range: 3.24 到 25.17
- slow direction 平均 travel time 下降 10.6%
- fast direction 平均 travel time 上升 20.7%
- `chi_hat=0.9998669`
- welfare gain: 133.1 ppm

raw_connected_v2 sidecar：

- `Replication_apply/results/tidal_lane_raw_connected_v2/summary_specC.txt`
- treated pairs: 100
- asymmetry range: 1.66 到 7.37
- `chi_hat=0.9998650`
- 但 AA input 的 edge_rows 只有 609，覆盖不足，不能直接升为主结果。

## 4. 最新代码的逐步学习

### Step 1：`build_primitives.py`

入口函数：

- `load_primitives(version_root, grid_type, period, od_pop_col)`

它读取三张 QSM 表：

- `qsm_input_nodes_square.parquet`
- `qsm_input_edges_square.parquet`
- `qsm_input_od_square.parquet`

关键逻辑：

- 只保留 `keep_for_model == 1` 的 nodes。
- 把原始 `node_i` 重新映射成连续 `0..N-1`。
- edges 按 `period == AM` 过滤。
- 丢掉 `tau_obs_min <= 0` 或 `tau_ff_min <= 0` 的边。
- lanes 先用 `lanes_directional`，缺失时用 `lane_est_length_weighted`，再缺失就用均值。
- `L_ij_obs` 来自 OD 表的 `commuters_road`，不是 nodes 表的 `residents/jobs`。
- `L_R_obs` 和 `L_F_obs` 是 OD 行列和。

直觉：

这里不是估计，只是把数据变成 solver 能吃的矩阵。最重要的传导是：

```text
OD commuters_road -> L_ij_obs -> L_R_obs, L_F_obs, Lbar
edges tau_obs/tau_ff/lanes -> route matrix and congestion block
```

需要记住：full_estimation 的人口边际来自 road-mode OD，而不是总居民和总就业。这和旧 `spatial_equilibrium.py` 的一些输出口径不同。

### Step 2：`parameters.py`

当前参数全是外部 placeholder：

```text
theta = 6.83
alpha = -0.12
beta = -0.10
delta1 = 0.488
lambda = delta1 / theta = 0.071449
```

直觉：

- `theta` 控制 residence-workplace choice 和 route cost aggregator 的离散度。
- `alpha` 和 `beta` 通过 `l_F^(alpha*theta)`、`l_R^(beta*theta)` 进入固定点。
- `lambda` 控制 traffic per lane 对 travel time 的弹性。
- `delta1` 是 AA/Seattle BPR 口径里的时间拥堵弹性，full_estimation 把它除以 `theta` 变成 iceberg/level-form 里的 `lambda`。

问题：

- `TidalLanes_Model_v4.tex` 的文字有时写 `alpha>0`，但代码和 AA replication 用 `alpha=-0.12`。这不是小符号问题，因为它改变 `l_F^(alpha*theta)` 的方向。
- 参数选择尚未对齐尤炜老师相关工作。

### Step 3：`invert_fundamentals.py`

入口函数：

- `invert_fundamentals(prim, params)`

目标：

在观测 `tau_obs` 和观测 `l_R_obs/l_F_obs` 下，反推出：

```text
ubar_theta = ubar_i^theta
abar_theta = Abar_j^theta
chi
```

关键逻辑：

- 先把 `L_R_obs`、`L_F_obs` 用 `floor_share=1e-6` 做正数下界。
- 只把 `L_R_obs > 0` 且 `L_F_obs > 0` 的节点当作 valid nodes。
- 用 `A_kl = 1 / (kappa * tau_obs_min)` 构造 directed adjacency。
- 用 dense solve 算 `(I - A)^(-1)`。
- 交替更新 `ubar_theta` 和 `abar_theta`，每轮用 valid nodes 的 geometric mean normalize。
- `chi` 最后从两组方程 implied constant 的 geometric mean 取。

直觉：

反演基本面是在说：给定观测通勤成本和观测居住-就业分布，哪些 amenity/productivity 可以 rationalize 当前分布。反事实时这些基本面固定。

当前结果：

- `chi=2.2419e-11`
- `kappa=1.5350`
- output says `n_iter=1512`
- residuals 很小，`resid_R` 大多在 `1e-4` 量级，`resid_F` 接近机器精度。

风险：

- 当前代码默认 `max_iter=800`，但输出记录 `n_iter=1512`。这说明输出不是由当前代码默认设置直接生成，或者代码生成后又被改过。
- `ubar_theta` 和 `abar_theta` 分布极端。`abar_theta` max 约 `7.58e10`，mean 约 `1.33e8`，median 约 `0.00695`。这在数值上能跑，但需要用图或分位数诊断是否由很多零 OD 节点和 floor 造成。

### Step 4：`equilibrium_solve.py`

入口函数：

- `solve_equilibrium(prim, ubar_theta, abar_theta, params, ...)`

传导关系：

```text
tau_min_kl -> A_kl = 1 / (kappa * tau_min_kl)
A -> tau_ij_inv_theta = (I - A)^(-1)
tau_ij_inv_theta + fundamentals -> l_R, l_F
l_R, l_F + tau_ij_inv_theta -> Pi_i, P_j
Pi_i, P_j + tau_min_kl -> Xi_kl
Xi_kl / n_kl + tau_ff -> tau_min_new
tau_min_new -> repeat
```

核心更新式在代码里是：

```text
Xi_kl = tau_min_kl^(-1) * P_j(edge_i)^(-theta) * Pi_i(edge_j)^(-theta)
tau_min_new = tau_ff * (Xi_kl / n_kl)^lambda
```

这里 `t_kl_iceberg = tau_min^(1/theta)`，所以 `t_kl^(-theta)=1/tau_min`。

直觉：

- lanes 多，`Xi/n` 低，travel time 低。
- `lambda` 越大，lane 或 traffic 的影响越大。
- `tau_ff` 是基础速度面。
- `Pi/P` 把网络可达性和 residence-workplace distribution 传导到 edge traffic。

当前最大问题：

代码没有 free-flow floor。只要 `Xi/n < 1`，就会有：

```text
tau_min_new < tau_ff
```

这在最新 baseline 中已经发生：46.5% 的边低于 free-flow time。旧 `spatial_equilibrium.py` 后来曾用过 `max(1, density^lambda)`，但最新 full_estimation 没有。

### Step 5：`lambda_diagnostic.py`

入口函数：

- `estimate_lambda_cross_section(tau_obs, tau_ff, n_kl, xi_hat)`

回归：

```text
y = log(tau_obs / tau_ff)
x = log(Xi_hat / n_kl)
lane-bin demeaned OLS
```

当前结果：

- `lambda_hat=0.01214`
- `se=0.00127`
- `R2=0.02185`
- n_obs = 4087

直觉：

这个数字不能替换 structural `lambda`。原因是 `Xi_hat` 是模型自己解出来的，不是真实 directed traffic。它可以作为 sanity check，但不能作为识别结果。

### Step 6：`counterfactual_levels.py`

当前三个 scenario：

1. CF1 symmetric cost
   - 代码对 bidirectional pair 的 `tau_obs_min` 取平均。
   - 但只把它作为 `tau_min_init` 传给 `solve_equilibrium`。
   - solver 下一轮会重新用同样的 `tau_ff`、同样的 lanes、同样的 fundamentals 更新。
   - 所以如果固定点唯一，最终回到 baseline。

2. CF2 tidal lane
   - 选 top-K bidirectional pairs by `tau_slow/tau_fast`。
   - 长度双向都要超过 200 m。
   - fast direction 减 1 条 lane 后必须仍然至少有 1 条 lane。
   - slow direction `+1`，fast direction `-1`。

3. CF3 uniform lane expansion
   - 所有 `n_kl` 乘以 `1.10`、`1.25`、`1.50`。

当前结果：

| scenario | welfare change | mean tau change | max log l_R change | max log l_F change |
|---|---:|---:|---:|---:|
| CF1 symmetric | -0.000004% | -0.000005% | 0.000003 | 0.000002 |
| CF2 tidal | +0.00324% | +0.0290% | 0.0193 | 0.0185 |
| CF3 1.10 | +0.0567% | -0.5497% | 0.0226 | 0.0247 |
| CF3 1.25 | +0.1339% | -1.2813% | 0.0529 | 0.0579 |
| CF3 1.50 | +0.2463% | -2.3136% | 0.0961 | 0.1052 |

CF2 的 mean travel time 上升但 welfare 也上升，需要谨慎解释。可能是空间重配或 market access 权重在起作用，也可能是当前拥堵方程和 welfare scale 尚未完全锚定。

## 5. 当前结果的诊断

### 5.1 baseline 不是观测速度面的复现

对 `full_estimation_square_v1`：

- baseline mean `tau_min`: 4.4026 min
- observed mean `tau_obs`: 5.4175 min
- free-flow mean `tau_ff`: 4.3608 min
- baseline 相对 observed 平均低 13.46%
- baseline 低于 observed 的边占 70.93%
- baseline 低于 free-flow 的边占 46.51%
- observed 低于 free-flow 的边占 13.29%

这说明当前 baseline equilibrium 是一个模型内部 fixed point，不是对 observed AM speed surface 的复现。这个状态可以用于调试 solver，但不能直接作为政策反事实的可信基准。

### 5.2 CF1 当前基本无效

CF1 的所有边相对 baseline 的 `tau` 变化都小于 `1e-5`。这是代码机制导致的，不是经济结果。要让 CF1 成为“对称 vs 有向成本”的有效实验，必须把对称成本作为持久 primitive，而不是初始值。

### 5.3 `kappa` 解决数值收敛，但改变 levels

`kappa=1.5350` 把 `A_kl=1/tau_min` 缩小，确保 row sum 和 spectral radius 安全。它能让 `(I-A)^(-1)` 可解，但绝对 `Wbar` level 依赖这个缩放。文档已经说 counterfactual ratios 更安全。下一步所有表图都应报告 kappa，并避免解释 `Wbar` absolute level。

### 5.4 最新结果缺少完整复现日志

`data_work/outputs/full_estimation_square_v1/run.log` 只记录到 Step 2，但输出目录已有 fundamentals、baseline、lambda diagnostic 和 counterfactuals。再加上 `n_iter=1512` 和当前代码默认 `max_iter=800` 不一致，说明这批结果必须先 rerun 才能作为 retained result。

## 6. 历史尝试汇总

### 尝试 A：3 月 structural prototype

文档：

- `data_work/docs/_superseded/MODEL_REFRESH_2026-03-21.md`
- `data_work/docs/_superseded/CURRENT_MODEL_EQUATIONS.md`
- `data_work/docs/_superseded/STRUCTURAL_MODEL_WORKFLOW.md`

代码：

- `data_work/src/model/spatial_equilibrium.py`

核心：

- soft route assignment 或 shortest-path 近似。
- diagnostic `theta` 和 `lambda`。
- congestion primitive 从 observed-time anchored 逐步改到 free-flow anchored。

结果：

- square / hex / voronoi 都跑过。
- diagnostic `lambda_hat` 经常贴近 0.01 floor。
- 部分模型不收敛。

保留价值：

- 给出了 route approximation、free-flow data gap、lambda diagnostic 的早期经验。

不应保留为主结果：

- 旧 route block 不是最终理论对象。
- 不少输出未收敛。
- 版本和当前 grid-travel cost 主线不一致。

### 尝试 B：4 月 model_v1 审查和输入包修复

文档：

- `Documents/L2_notes/_superseded/model_v1_review_and_data_gap_temp_20260422.md`
- `Documents/L2_notes/_superseded/model_v1_implementation_checklist_temp_20260423.md`

核心诊断：

- `t_kl` 和 `tau_min` 命名混乱。
- free-flow 不能被静默回填为 observed。
- `lambda` 和 `theta` 只能称为 diagnostic。
- 模型应把 centerline-grid 和 raw-edgeproj 输入分开。

保留价值：

- 这些诊断仍然适用于最新 full_estimation，尤其是 free-flow 下界、lambda 识别和 input bundle lineage。

### 尝试 C：AA exact-hat replication

文档：

- `Documents/L2_notes/AA_commuting_replication_adaptation_notes_20260426.md`
- `Documents/L2_notes/MODEL_replication_tidallanes_audit_20260429.md`
- `Documents/L1_manuscripts/MODEL_replication_tidallanes.tex`

代码：

- `Replication_apply/python_commuting/counterfactual.py`
- `Replication_apply/python_commuting/tidal_lane_specC.py`

做过的实验：

- Spec A: 对称目标时间通过 BPR inversion 进入 `t_bar_hat`，福利变差。
- Spec B: 直接用 travel-time ratio，福利改善。
- Spec C: lane reallocation 加 BPR fixed-flow shock，福利改善。

主要结果：

- Spec C v1: `chi_hat=0.9998669`，welfare gain 133.1 ppm。
- Spec C raw_connected_v2: `chi_hat=0.9998650`，方向相近。

主要问题：

- fixed-flow BPR shock 不是 full GE traffic assignment。
- exact-hat 输入是 edge traffic matrix，不是完整 level-form network。
- raw_connected_v2 AA input 只有 609 条边，覆盖不足。

### 尝试 D：full_estimation level-form scaffold

这是当前最新线。它把 v4 的 level-form idea 落到代码里，但还没有完成可信 baseline 和 counterfactual 设计。

保留价值：

- 文件结构清楚。
- 输入 schema 清楚。
- fundamentals inversion residual 可以很小。
- CF3 单调性方向合理。

需要先修：

- CF1 shock 没有生效。
- congestion equation 未锚定 observed / free-flow。
- 输出需要用当前代码完整 rerun。

## 7. 主要错误和问题清单

### P0. CF1 symmetric 没有真正改变模型 primitive

文件：

- `data_work/src/model/full_estimation/counterfactual_levels.py`

代码把 `tau_sym_init` 作为 `tau_min_init` 传入。这个初始值在外层 fixed point 中会被同一套 `tau_ff`、`n_kl`、`Xi_kl` 更新掉。结果上，CF1 welfare change 只有 `-4.18e-8`。

修复方向：

- 方案 1：做一个 fixed-cost CF1，不跑 congestion update，直接用 symmetric `tau_min` 解 `l_R/l_F/Pi/P/Wbar`。
- 方案 2：把 symmetric cost 作为持久 `tau_ff_override` 或 `tau_obs_override`，并定义它和 congestion technology 的关系。
- 方案 3：若目标是 v4 里的 `t_kl^obs` 对称化，则需要显式传入 counterfactual `A_kl` 或 cost multiplier，而不是只传初始值。

### P0. 拥堵更新允许低于 free-flow

文件：

- `data_work/src/model/full_estimation/equilibrium_solve.py`

当前公式：

```text
tau_min_new = tau_ff * (Xi_kl / n_kl)^lambda
```

当 `Xi_kl/n_kl < 1`，它直接低于 free-flow。当前 baseline 有 46.5% 边低于 free-flow。

修复方向：

- 用 `tau_min_new = tau_ff * max(1, Xi_kl / capacity_scale / n_kl)^lambda`。
- 或采用 BPR 型 `tau = tau_ff * (1 + a * density^lambda)`。
- 或先为每条边校准 baseline anchor，使 observed tau 能被 `tau_ff`、`n`、baseline `Xi` 复现。

### P0. 输出和当前代码不可完全对应

证据：

- 输出 `fundamentals_meta.json` 记录 `n_iter=1512`。
- 当前 `invert_fundamentals.py` 默认 `max_iter=800`。
- `run.log` 只记录到 Step 2。

修复方向：

- 用当前代码重新跑一个带日期的新输出目录。
- 保存完整 command、git status、参数、solver settings。
- 不要覆盖 `full_estimation_square_v1`，先生成 `full_estimation_square_v1_rerun_20260628` 之类目录。

### P1. baseline equilibrium 没有锚定 observed allocation 和 observed travel time

inversion 用 observed `tau_obs` 反演 fundamentals，但 baseline check 从 `tau_ff` 出发按 congestion equation 重解。若 congestion block 不能复现 observed `tau_obs`，baseline 就不再是 Assumption `obs_eq` 的观测均衡。

修复方向：

- 输出 baseline-vs-observed diagnostics。
- 把 `tau_obs/tau_ff` 的 baseline gap 用于 calibrating congestion intercept 或 edge fixed effect。
- 在文档里明确 baseline check 是 “model-implied equilibrium under current congestion technology”，不是 observed speed replication。

### P1. `lambda` 仍是 weak diagnostic

当前 `lambda_hat=0.01214`，和 placeholder `0.07145` 差距较大，且 `R2=0.02185`。这不是估计失败，而是当前没有真实 directed traffic 数据。

修复方向：

- 在没有 traffic count / IV 前，保留 placeholder 加 sensitivity grid。
- 把 `lambda_diagnostic` 明确写成 calibration check。
- 如要估计，先设计 panel 或 IV，不能只用 model-implied `Xi` 做 structural claim。

### P1. 参数符号需要统一

v4 模型文字有 `alpha>0` 的 agglomeration 直觉，代码和 AA replication 用 `alpha=-0.12`、`beta=-0.10`。如果符号来自 AA transformed system，需要在模型文档和代码里统一命名。

修复方向：

- 明确 `alpha_code`、`beta_code` 是 AA replication sign convention，还是理论模型里的 externality elasticity。
- 所有公式、README 和参数 JSON 用同一套符号。

### P1. results summary 太薄

`summary.csv` 只有 welfare、mean tau、max share log change。缺少：

- baseline vs observed tau gap。
- share of tau below free-flow。
- treated vs untreated tau changes。
- OD weighted travel time。
- distributional percentiles。
- `Xi/n` distribution。

修复方向：

- 增加 `diagnostics/` 输出，至少保存 edge-level baseline/cf comparison CSV。

### P2. dense solve 和 saved arrays 的复现问题

`solve_equilibrium` 运行中有 `tau_ij_inv_theta`，但写出时没有保存这个大矩阵。考虑到 N=2736，矩阵很大，不一定要全量保存，但至少要保存：

- `kappa`
- row-sum diagnostics
- spectral radius proxy
- selected OD pair diagnostics
- whether sparse/Neumann alternative is used

### P2. lane policy feasibility 仍很粗

CF2 只要求 reverse edge 存在、长度大于 200 m、fast direction 至少剩 1 lane。它没有检查：

- 是否同一物理 corridor。
- 是否有实际可逆车道条件。
- 是否早晚高峰方向反转。
- 是否 lane estimate 来自 observed 还是 fallback。

这适合 scaffold，不适合最终政策口径。

## 8. 下一步修复顺序

建议按 P0 到 P2 处理。

### Step 1：先冻结并复跑当前 full_estimation

目标：确认当前代码和当前输出能一一对应。

动作：

```bash
python3 data_work/src/model/full_estimation/run_full_estimation.py \
  --version-id manual_centerline_rules_v11 \
  --grid-type square \
  --period AM \
  --output-name full_estimation_square_v1_rerun_20260628
```

同时保存：

- command。
- git status。
- full log。
- parameters JSON。
- diagnostics JSON。

完成判据：

- `run.log` 包含 Step 1 到 counterfactual done。
- `fundamentals_meta.n_iter` 不超过当前代码 max_iter，或者命令里显式传入并记录。
- 新结果和旧结果差异被解释。

### Step 2：修 CF1

目标：让 symmetric vs directional cost 成为真实反事实。

最低测试：

- `CF1_symmetric` 的 `tau_min_kl` 和 baseline 至少在 bidirectional treated edges 上有非零差异。
- 如果选择 fixed-cost CF1，则文档写清楚不启用 congestion update。
- 如果选择 full GE CF1，则文档写清楚对称化的是 `tau_ff`、`tau_obs`、还是 cost multiplier。

### Step 3：修 congestion block

目标：free-flow 是下界，baseline 能解释 observed congestion wedge。

最低测试：

- baseline `tau_min < tau_ff` share 应为 0，或所有例外有明确 flag。
- baseline `tau_min/tau_obs` 分布被输出。
- `Xi/n` 的单位和 scale 有解释。

### Step 4：增加 baseline 和 CF diagnostics

新增输出建议：

- `diagnostics/baseline_edge_fit.csv`
- `diagnostics/cf_edge_changes.csv`
- `diagnostics/share_changes.csv`
- `diagnostics/solver_health.json`

关键字段：

- `tau_obs_min`
- `tau_ff_min`
- `tau_base_min`
- `tau_cf_min`
- `tau_base_over_obs`
- `tau_base_below_ff`
- `Xi_base`
- `Xi_cf`
- `n_base`
- `n_cf`
- `treated`

### Step 5：决定 exact-hat 和 level-form 的分工

建议：

- `Replication_apply` 保留为 AA exact-hat replication 和 robustness。
- `full_estimation` 作为当前主线，但必须修好 baseline 和 CF definitions。
- 论文里不要混用 `chi_hat` 和 `Wbar_pct_change`，除非写清楚它们来自不同 solver。

### Step 6：参数和输入版本再推进

参数：

- 先做 `lambda` sensitivity。
- 再查尤炜老师相关参数。
- 没有 directed traffic 前，不把 `lambda_diagnostic` 写成估计。

输入：

- 继续比较 `manual_centerline_rules_v11`、`raw_edgeproj_v2`、`raw_connected_v2`。
- raw_connected_v2 覆盖不足前，不升为主模型输入。
- grid travel cost 口径没定前，不在论文中写死为最终 baseline。

## 9. 从直觉到代码的速查表

| 经济直觉 | 模型对象 | 最新代码位置 | 当前状态 |
|---|---|---|---|
方向性通勤成本 | `tau_obs_min`, `tau_ff_min`, directed edge `(i,j)` | `build_primitives.py` | 已读入，口径来自 QSM export |
route aggregator | `(I-A)^(-1)`，`A_kl=1/(kappa*tau_min)` | `invert_fundamentals.py`, `equilibrium_solve.py` | 已实现，需保存更多诊断 |
居住和就业基本面 | `ubar_theta`, `abar_theta` | `invert_fundamentals.py` | 可反演，但分布极端 |
拥堵弹性 | `lambda_` | `parameters.py`, `lambda_diagnostic.py` | placeholder 加 weak diagnostic |
lane capacity | `n_kl` | `build_primitives.py`, `counterfactual_levels.py` | CF2/CF3 的政策杠杆 |
link traffic | `Xi_kl` | `equilibrium_solve.py` | 模型内生，不是实测 |
welfare | `Wbar` | `equilibrium_solve.py` | level 受 kappa 影响，优先看 ratio |
exact-hat welfare | `chi_hat` | `Replication_apply/python_commuting/counterfactual.py` | 旧 replication 结果，非 full_estimation |
对称成本反事实 | CF1 | `counterfactual_levels.py` | 当前未真正生效 |
潮汐车道反事实 | CF2 | `counterfactual_levels.py` | 能跑，但 congestion block 待修 |
统一扩容 | CF3 | `counterfactual_levels.py` | 单调性方向合理 |

## 10. 当前可对外表达的审慎版本

可以说：

- 项目已经有一个 level-form full estimation scaffold。
- 当前 scaffold 能读取 `manual_centerline_rules_v11` 的 square AM QSM inputs，反演 fundamentals，并跑 baseline、CF1、CF2、CF3。
- 历史 exact-hat Spec C 显示，基于 fixed-flow BPR 的一车道潮汐式重分配在 AA hat solver 下给出很小的 welfare gain。
- 当前最主要的工程任务不是再换参数，而是修正 full_estimation 的反事实定义和 baseline congestion anchoring。

不应说：

- 当前 full_estimation 已经完成参数估计。
- 当前 CF1 识别了 directionality 的福利贡献。
- 当前 baseline 已经复现观测 AM travel time。
- 当前 `lambda_hat` 是结构估计。
- raw_connected_v2 已经可以替代主模型输入。
