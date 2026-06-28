# Directional Cost Model, 2026-06-28

用途：给当前文章 framing 提供一个简化、可复现、直接服务于 directional congestion 的模型版本。这个版本不模拟潮汐车道，也不内生更新拥堵。它把 observed directed travel time 当作模型 primitive，比较 directed cost 和 symmetric cost 两个世界，并做 sensitivity / simulation。

## 1. 为什么需要这个简化版本

文章现在更应该回答的问题是：

```text
如果研究者把有方向的拥堵成本压成无方向平均成本，会怎样误读 market access、通勤流、空间分布和福利？
```

原 `full_estimation` 框架把 lanes、traffic、congestion update 和 counterfactual 全部放在一起。这个方向适合做政策应用，但当前没有真实 directed traffic counts，`lambda` 也不是结构估计。若文章主角是 directional congestion，最稳的主模型应先隔离 directionality 本身。

因此本版本固定 directed travel cost：

```text
tau_kl = observed directed travel time
```

模型不使用：

- lane reallocation
- endogenous congestion update
- `lambda` calibration
- exact-hat counterfactual
- tidal-lane policy simulation

模型保留：

- directed network
- observed directed travel time
- OD road-commuter matrix
- residence and workplace marginal shares
- fundamentals inversion
- fixed-cost spatial equilibrium
- directed vs symmetric comparison
- directionality sensitivity simulations

## 2. 代码和输出位置

代码：

- `data_work/src/model/directional_cost/fixed_tau_model.py`
- `data_work/src/model/directional_cost/run_directional_cost.py`
- `data_work/src/model/directional_cost/plot_results.py`
- `data_work/src/model/directional_cost/README.md`

输出：

- `data_work/outputs/directional_cost_square_v1/`

主结果文件：

- `summary_metrics.json`
- `diagnostics/asymmetry_summary.json`
- `diagnostics/directed_fit_to_observed.json`
- `diagnostics/edge_cost_comparison.csv`
- `diagnostics/node_directed_vs_symmetric.csv`
- `diagnostics/top_od_flow_changes.csv`
- `simulations/asymmetry_intensity.csv`
- `simulations/asymmetry_threshold.csv`

图：

- `figures/asymmetry_ratio_histogram.png`
- `figures/simulation_asymmetry_intensity.png`
- `figures/simulation_asymmetry_threshold.png`

## 3. 数据输入

默认输入来自：

```text
data_work/outputs/manual_centerline_rules_v11/data/
```

读取文件：

- `qsm_input_nodes_square.parquet`
- `qsm_input_edges_square.parquet`
- `qsm_input_od_square.parquet`
- `qsm_input_parameters_square.json`

运行口径：

- `version_id = manual_centerline_rules_v11`
- `grid_type = square`
- `period = AM`
- `od_pop_col = commuters_road`
- symmetric method = geometric mean

样本规模：

- nodes: 2736
- directed edges: 4087
- reciprocal directed edges: 4008
- reciprocal pairs: 2004
- road commuters: 95145.589

方向性成本变量：

```text
tau_obs_kl = observed directed peak-period travel time, minutes
tau_ff_kl  = free-flow travel time, minutes
```

本模型只使用 `tau_obs_kl` 作为 fixed directed cost。`tau_ff_kl` 和 lanes 保留在 primitives 里，但不参与主 equilibrium。

## 4. 对称成本的构造

对每一对同时存在 `k -> l` 和 `l -> k` 的 reciprocal edge pair，主口径使用几何平均：

```text
tau_sym_kl = tau_sym_lk = sqrt(tau_obs_kl * tau_obs_lk)
```

几何平均保留 pair-level log midpoint。它把一个方向的慢和另一个方向的快对称化，但不改变这对边的 log center。

如果某条边没有反向边，本版本保留原始 `tau_obs_kl`。这样做避免把没有方向性观测的信息强行补齐。

方向性强度定义为：

```text
ratio_pair = max(tau_obs_kl, tau_obs_lk) / min(tau_obs_kl, tau_obs_lk)
```

当前数据中：

| 指标 | 数值 |
|---|---:|
| reciprocal pairs | 2004 |
| median ratio | 1.079 |
| p75 ratio | 1.219 |
| p90 ratio | 1.634 |
| p95 ratio | 2.169 |
| p99 ratio | 3.978 |
| share ratio >= 1.10 | 44.1% |
| share ratio >= 1.25 | 22.8% |
| share ratio >= 1.50 | 12.8% |
| share ratio >= 2.00 | 6.0% |

这说明 directionality 不是少数异常边的问题。即使用 reciprocal-pair 口径，至少 44% 的双向边对有 10% 以上方向性 travel-time gap。

## 5. 模型方程

### 5.1 Route aggregator

给定 directed edge cost `tau_kl`，构造 directed adjacency：

```text
A_kl = 1 / (kappa * tau_kl)
```

其中 `kappa` 是统一缩放，保证 spectral-radius 条件（若 raw row sum 已低于 0.9，则 `kappa=1`）。

双边 route-cost aggregator 为：

```text
T = (I - A)^{-1}
```

代码中保存的是 `tau_ij_inv_theta = T_ij`。这与 Allen--Arkolakis 主文写法一致，不再引入额外的 propagation scale。

### 5.2 Fundamentals inversion

使用 directed observed costs 和 observed road-commuter marginals 反演 fundamentals：

```text
(l_i^R)^(1 - beta*theta)
  = chi * ubar_i^theta * sum_j T_ij * Abar_j^theta * (l_j^F)^(alpha*theta)

(l_j^F)^(1 - alpha*theta)
  = chi * Abar_j^theta * sum_i T_ij * ubar_i^theta * (l_i^R)^(beta*theta)
```

反演对象：

- `ubar_i^theta`
- `Abar_j^theta`
- `chi`

使用参数：

- `theta = 6.83`
- `alpha = -0.12`
- `beta = -0.10`

这些仍是 AA / Seattle placeholder。这个简化版本不使用 `lambda`，因为 travel time 不由 traffic per lane 内生生成。

数值设置：

- common `kappa = 1.9187366948`
- inversion tolerance = `1.5e-4`
- inversion iterations = 1236
- inversion converged = true
- `chi = 2.42124e-11`

### 5.3 Static equilibrium under a fixed cost surface

给定同一组 fundamentals 和一个固定 cost surface，解：

```text
l_R, l_F, Pi, P, Wbar
```

Directed baseline 使用 `tau_obs_kl`。Symmetric scenario 使用 `tau_sym_kl`。两个世界的 fundamentals 固定，所以差异来自 directed cost 被对称化。

### 5.4 Welfare and allocation metrics

主 summary 报告：

- `Wbar_pct_change`
- residence dissimilarity
- workplace dissimilarity
- OD dissimilarity
- OD-weighted bilateral cost
- max and p95 log allocation changes

其中 dissimilarity 使用：

```text
0.5 * sum_i |share_i^scenario - share_i^base|
```

## 6. 主结果

主比较是：

```text
base = directed_observed
scenario = symmetric_cost
```

结果：

| 指标 | symmetric relative to directed |
|---|---:|
| welfare change | +0.1507% |
| residence dissimilarity | 1.0107% |
| workplace dissimilarity | 0.8221% |
| OD dissimilarity | 1.9726% |
| OD-weighted bilateral cost change | -0.1201% |
| p95 abs log residence change | 0.0711 |
| p95 abs log workplace change | 0.0587 |

等价地，如果从 symmetric 世界逐步恢复 observed directionality，full directed asymmetry 使 `Wbar` 下降约 0.1504%。这个数字不应解释成政策福利损失。它是“把 directionality 平均掉”造成的 model-implied welfare-level bias。

一个有用的直觉是：几何平均对称化会提高原本快方向的成本，同时降低原本慢方向的成本。虽然 edge-level mean travel time 在 symmetric scenario 中上升 1.44%，OD-weighted bilateral route cost 下降 0.12%。原因是 route choice、market access 和 spatial reallocation 会重新加权这些边。

Directed baseline 对 observed marginals 的 fit：

| 指标 | 数值 |
|---|---:|
| residence L1 distance vs floored observed | 0.0072% |
| workplace L1 distance vs floored observed | 0.0247% |

最大 log gap 出现在低质量或低 mass 节点上，因此 L1 fit 更能反映主样本的 aggregate fit。

## 7. Simulation 1：directionality intensity

这个 simulation 在 symmetric 和 observed directed cost 之间做 log-space 插值：

```text
tau_kl(rho) = tau_sym_kl * exp(rho * [log(tau_obs_kl) - log(tau_sym_kl)])
```

其中：

- `rho = 0` 是完全 symmetric。
- `rho = 1` 是 observed directed。

结果相对 `rho = 0`：

| rho | welfare change | residence dissimilarity | workplace dissimilarity |
|---:|---:|---:|---:|
| 0.00 | 0.0000% | 0.0000% | 0.0000% |
| 0.25 | -0.0628% | 0.2657% | 0.2252% |
| 0.50 | -0.1083% | 0.5201% | 0.4348% |
| 0.75 | -0.1373% | 0.7671% | 0.6327% |
| 1.00 | -0.1504% | 1.0107% | 0.8221% |

主要读法：allocation response 基本随 directionality intensity 单调扩大。福利变化在 `rho=0.5` 后仍增加，但边际增幅下降，说明最初一半的方向性已经解释了相当多的 welfare-level gap。

## 8. Simulation 2：directionality threshold

这个 simulation 只保留超过给定 reciprocal-pair ratio 的方向性，低于阈值的 pair 被对称化：

```text
if ratio_pair >= c:
    tau_kl = tau_obs_kl
else:
    tau_kl = tau_sym_kl
```

结果相对 fully symmetric：

| threshold c | reciprocal edges kept directed | welfare change | residence dissimilarity |
|---:|---:|---:|---:|
| 1.00 | 100.0% | -0.1504% | 1.0107% |
| 1.05 | 64.6% | -0.1491% | 1.0010% |
| 1.10 | 44.1% | -0.1376% | 1.0000% |
| 1.25 | 22.8% | -0.1157% | 0.9466% |
| 1.50 | 12.8% | -0.1146% | 0.7539% |
| 2.00 | 6.0% | -0.0803% | 0.5103% |

主要读法：directional congestion 的影响高度集中但不是只由极端 outliers 产生。仅保留 ratio >= 2.00 的 6.0% reciprocal edges，仍可产生约 0.08% welfare gap 和 0.51% residence dissimilarity；把 threshold 放宽到 1.50 后，福利 gap 已接近 full effect 的四分之三。

## 9. 当前结果如何用于文章 framing

这版模型支持三句话：

1. Directional congestion is widespread in the Beijing directed network.
2. Collapsing directed costs into symmetric pair averages changes model-implied market access and reallocates residence, workplace, and OD flows.
3. The bias is not driven only by a few extreme road pairs; threshold simulations imply that moderately asymmetric pairs also contribute to the aggregate effect.

这版模型不支持：

- 潮汐车道的政策福利估计。
- lane reallocation 的最优设计。
- `lambda` 或 BPR congestion elasticity 的结构估计。
- 真实 traffic count 的 link-flow validation。

## 10. 复现命令

主模型：

```bash
python3 data_work/src/model/directional_cost/run_directional_cost.py \
  --version-id manual_centerline_rules_v11 \
  --grid-type square \
  --period AM \
  --output-name directional_cost_square_v1 \
  --inversion-tol 1.5e-4
```

图：

```bash
python3 data_work/src/model/directional_cost/plot_results.py \
  --output-root data_work/outputs/directional_cost_square_v1
```

默认 simulation grids：

```text
asymmetry intensity rho: 0, 0.25, 0.50, 0.75, 1.00
threshold ratio c: 1.00, 1.05, 1.10, 1.25, 1.50, 2.00
```

## 11. 局限和下一步

1. 参数仍是 placeholder。
   - `theta=6.83`、`alpha=-0.12`、`beta=-0.10` 继承自现有 full-estimation scaffold。
   - 下一步需要做 `theta`、`alpha`、`beta` sensitivity。

2. 最大 log fit gap 来自低 mass 节点。
   - L1 fit 很小，但 node-level extremes 需要在图表中标记。
   - 若进入论文表，建议报告 weighted distribution，而不是只报 max。

3. symmetric operator 需要做 robustness。
   - 当前主口径是 geometric mean。
   - 可加 arithmetic mean、min-cost、max-cost 或 OD-weighted mean。

4. 下一版可以加入 parameter sensitivity。
   - `theta` grid: 4, 6.83, 9。
   - `alpha/beta` grid: current values, zero externality, stronger agglomeration。
   - 输出同一套 `summary_metrics` 和 simulation figures。

