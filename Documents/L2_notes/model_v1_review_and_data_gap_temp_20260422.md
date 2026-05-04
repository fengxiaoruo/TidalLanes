# model_v1 审查与数据缺口临时笔记

用途：这是一份过程性工作笔记，供当前模型整理与下一步数据准备使用。后续相关问题处理完成后可直接删除，不作为长期 handoff 文档保留。

## 1. 当前结论

- `model_v1` 现在更像一个按 AA 方向改写的 prototype，而不是一个已经可以称为“严格遵照 AA 并可直接估计”的稳定版本。
- 主要问题不只在 estimation，也在更前面的对象定义：
  - `t_{kl}` 的理论定义还没有和 AA 的 iceberg convention 完全对齐。
  - 当前 route aggregator 是一个实用型 soft assignment，但还不是干净的 AA-consistent route block。
  - `lambda` 的估计还只是 diagnostic proxy，不是可直接拿来做结构解释的识别结果。
  - 当前模型入口默认读取的版本和当前项目真正锁定/偏好的数据版本还没有对齐。

## 2. model_v1 审查

### 2.1 高优先级问题

#### A. `t_{kl}` 的定义与 AA 不一致

- 当前代码和公式说明把 `t_{kl}` 直接当成了分钟级 travel time 来用。
- 见：
  - [data_work/docs/CURRENT_MODEL_EQUATIONS.md](/Users/fxr/Desktop/TidalLanes/data_work/docs/CURRENT_MODEL_EQUATIONS.md)
  - [data_work/src/model/spatial_equilibrium.py](/Users/fxr/Desktop/TidalLanes/data_work/src/model/spatial_equilibrium.py:274)
- 但 [Documents/L2_notes/AA_2022_transport_cost_memo.md](/Users/fxr/Desktop/TidalLanes/Documents/L2_notes/AA_2022_transport_cost_memo.md:6) 已明确记录：
  - AA 的 `t_{kl}` 是 iceberg、无量纲、`t_{kl} >= 1`
  - preferred 设定是 `delta_0 = 1 / theta`
  - 模型真正使用的是 `t_{kl}^{-\theta}` 的 multiplicative route system

这意味着当前版本最多只能说“受 AA 启发”，不能稳妥地说“严格按 AA 实现”。

#### B. soft route assignment 目前还是近似实现

- 当前实现：
  - [compute_soft_shortest_path_assignment](/Users/fxr/Desktop/TidalLanes/data_work/src/model/spatial_equilibrium.py:274)
- 主要特征：
  - 直接用 `edge_cost^{-theta}` 做权重
  - 对边成本先做 `clip(edge_cost, 1.001, None)`
  - 对 `p_edge` 再做 `clip(0, 1)`

这说明现在的 route block 还没有完全在理论上闭合：

- 若 `p_edge` 需要显式裁剪到 `[0,1]`，说明它不是一个完全干净的 choice-probability system。
- 当前实现也没有检查每个节点向外的 probability mass 是否满足理论上的归一性条件。
- 如果未来要真正 claim “AA-style link sharing / route assignment”，这一块还需要重写得更结构化。

#### C. 拥堵方程允许 `t_{kl} < t_{kl}^{ff}`

- 当前实现：
  - [data_work/src/model/spatial_equilibrium.py](/Users/fxr/Desktop/TidalLanes/data_work/src/model/spatial_equilibrium.py:595)
  - `t_new = t_ff * (phi / n)^lambda`

问题：

- 当 `phi / n < 1` 时，`t_new < t_ff`
- 这意味着 free-flow time 不是下界
- 在经济解释和物理解释上都不自然

如果继续沿 AA 方向走，拥堵 block 应该至少满足：

- free-flow 是可解释的基准下界
- 拥堵项不会把 travel time 压到 free-flow 以下
- `lambda` 对应的是拥堵弹性，而不是一个在低密度区间也会机械压缩时间的幂函数

#### D. `theta` 和 `lambda` 现在不是结构估计

- `theta`：
  - [estimate_theta_two_way_fe](/Users/fxr/Desktop/TidalLanes/data_work/src/model/spatial_equilibrium.py:356)
  - 本质上是 `log M_ij` 对 `log tau_ij` 的 two-way FE gravity diagnostic
- `lambda`：
  - [estimate_lambda_cross_section](/Users/fxr/Desktop/TidalLanes/data_work/src/model/spatial_equilibrium.py:393)
  - 本质上是 lane-bin FE 的截面 proxy regression

现有结果已经在提示这块不稳：

- 当前 baseline calibration summary:
  - [data_work/outputs/raw_rebuild_validation/model_square_baseline/calibration_summary.json](/Users/fxr/Desktop/TidalLanes/data_work/outputs/raw_rebuild_validation/model_square_baseline/calibration_summary.json)
- 其中：
  - `theta_hat ≈ 0.658`
  - solver 用的是外生 `theta = 6.83`
  - `lambda_hat = 0.01`
  - `lambda_hat` 基本贴着下界

所以现在更合理的表述是：

- `theta_fit` 和 `lambda_fit` 只是内部 diagnostic
- 不能把它们当作“已经从当前数据里识别出来的结构参数”

#### E. 默认模型版本和当前项目主线未对齐

- 当前运行入口默认读：
  - [data_work/src/model/run_spatial_equilibrium.py](/Users/fxr/Desktop/TidalLanes/data_work/src/model/run_spatial_equilibrium.py:32)
  - `version-id = raw_rebuild_validation`
- 但当前项目真正锁定的中心线版本是：
  - `manual_centerline_rules_v11`
- 当前更偏好的 raw-grid 方向则是：
  - `raw_connected_v1_grid_travel_time_edgeproj_v2`

这意味着当前模型不是跑在“当前最想保留和解释的版本”上，而是跑在一套较早的 `raw_rebuild_validation` 输入上。

#### F. 当前 baseline run 自身未稳定收敛

- 结果见：
  - [data_work/outputs/raw_rebuild_validation/model_square_baseline/equilibrium_summary.csv](/Users/fxr/Desktop/TidalLanes/data_work/outputs/raw_rebuild_validation/model_square_baseline/equilibrium_summary.csv)
- baseline 行显示：
  - `solver_iterations = 100`
  - `converged = False`

在收敛性没有解决前，不建议过度解读 welfare 或 policy counterfactual。

### 2.2 中等优先级问题

#### G. `t_ff` 当前常常没有真正进入模型

- loader 逻辑：
  - [data_work/src/model/spatial_equilibrium.py](/Users/fxr/Desktop/TidalLanes/data_work/src/model/spatial_equilibrium.py:181)
  - 若 `t_ff_min` 缺失，就回填成 `t_min`
- 但当前默认版本 `raw_rebuild_validation` 的：
  - [qsm_input_edges_square.parquet](/Users/fxr/Desktop/TidalLanes/data_work/outputs/raw_rebuild_validation/data/qsm_input_edges_square.parquet)
  - 实际并没有 `t_ff_min`

因此当前很多地方实际上变成了：

- `t_ff = t_obs`
- `t_obs / t_ff = 1`

这会直接削弱甚至破坏 `lambda` 估计。

#### H. 模型现在吃的是 centerline grid graph，不是当前偏好的 raw-grid graph

- `load_model_inputs` 读取的是：
  - `qsm_input_nodes_*`
  - `qsm_input_od_*`
  - `qsm_input_edges_*`
  - `grid_links_*_long.csv`
- 这些输入来自 stage06 / stage08 的 centerline-grid pipeline，而不是当前 raw-grid pair-specific 分支。

所以如果后续你想让模型反映你现在最偏好的 travel-cost construction，模型图对象本身也需要更新，而不只是替换几个参数。

### 2.3 暂时可以保留的部分

- `invert_fundamentals` 的基本结构是清楚的，至少在 prototype 层面是自洽的：
  - [data_work/src/model/spatial_equilibrium.py](/Users/fxr/Desktop/TidalLanes/data_work/src/model/spatial_equilibrium.py:416)
- 内层 population-employment fixed point 也是可理解的：
  - [data_work/src/model/spatial_equilibrium.py](/Users/fxr/Desktop/TidalLanes/data_work/src/model/spatial_equilibrium.py:455)

问题主要不在这个 block，而在：

- cost object
- route assignment
- congestion estimation
- input version alignment

## 3. 现有数据清单与模型需求的 gap

### 3.1 你现在已经有的关键数据

目前项目里已经有、并且对模型确实有用的数据包括：

- grid-level residents / jobs
- grid-level OD commuting matrix
- centerline-level directional speed data
- centerline-level lane estimate
- centerline asymmetry table
- tidal-lane candidate table
- grid-link long table，用于把 centerline 对象聚合到 grid-edge 上

也就是说，如果只是做一个基于 centerline-grid graph 的 prototype model，原材料并不算缺。

### 3.2 当前最直接的缺口

#### Gap 1. 当前锁定版本没有完整的 stage08 / QSM 输入包

模型 loader 默认需要：

- `qsm_input_nodes_*`
- `qsm_input_od_*`
- `qsm_input_edges_*`
- `grid_links_*_long.csv`

见：

- [data_work/src/model/spatial_equilibrium.py](/Users/fxr/Desktop/TidalLanes/data_work/src/model/spatial_equilibrium.py:121)

但当前你真正锁定的版本 `manual_centerline_rules_v11` 下，我核对到：

- 有：
  - `centerline_lane_master.parquet`
  - `centerline_asymmetry_table.parquet`
  - `centerline_tidal_lane_candidates.csv`
- 没有：
  - `qsm_input_nodes_square.parquet`
  - `qsm_input_od_square.parquet`
  - `qsm_input_edges_square.parquet`
  - `grid_links_square_long.csv`

所以第一件要补的数据工作，不是再找新数据，而是先把当前锁定版本的 model input bundle 重新导出。

#### Gap 2. 当前模型和当前 raw-grid 主线没有接上

你现在关于 travel cost 的主线已经在比较：

- `centerline`
- `raw_node_v4`
- `raw_edgeproj_v2`

但模型现在仍然吃的是 centerline-grid graph 那套输入，而不是 raw-grid version。

所以你缺的是一套与当前主推进方向一致的 model graph objects：

- node definition
- edge list
- observed edge cost
- free-flow edge cost
- lane / capacity mapping
- asymmetry / policy targeting mapping

如果未来模型要建立在 `raw_edgeproj_v2` 上，这一整套都要重构，不是只换一个 `tau_ij` 文件。

#### Gap 3. 缺真实的 road traffic / flow 数据，无法严肃估 `lambda`

AA 式 `lambda` 估计真正需要的是：

- link-level observed traffic volume
- 最好是 direction-specific
- 最好是 peak-specific
- 能和 lane count 对齐

你现在的 `lambda` 估计用的是：

- commuting OD 经 soft assignment 后得到的 edge-flow proxy

这对于内部 sanity check 可以接受，但不足以支撑结构解释。

所以严格来说，你还缺：

- 真实 road-link traffic volume 数据
- 或至少一个更可信的 link traffic proxy

#### Gap 4. 缺 congestion estimation 的 exogenous shifter / IV

AA 不是用简单 OLS，而是有识别设计。

你现在没有一套可用于 traffic-per-lane 回归的工具变量或外生 shifter。

如果要往“更像 AA”走，需要准备至少一类 network-side exogenous shifter，例如：

- intersections / turns / topology complexity
- historical geometry
- road design class / planned capacity
- 其他不会被当期 congestion 反向决定的 network-side variable

#### Gap 5. 缺更可信的 directional lane / capacity policy dataset

当前 lane data 有，但 policy feasibility data 没有。

现在的 tidal-lane counterfactual 是：

- 给一条边 `+1 lane`
- 反方向 `-1 lane`

见：

- [data_work/src/model/spatial_equilibrium.py](/Users/fxr/Desktop/TidalLanes/data_work/src/model/spatial_equilibrium.py:691)

这需要未来补一张更真实的 policy-feasibility table，例如：

- 哪些 corridor 可做方向性 lane reallocation
- 最大可调整幅度
- 是否有 physical separator / median
- 是否本来就是可逆车道候选
- 实际 lane count 与可调 lane count 的区别

#### Gap 6. 缺 road-relevant mode mapping

现有 OD 里带有：

- `type_walk`
- `type_bike`
- `type_sub`
- `type_bus`
- `type_car`

但目前求解器最终用的是总 `pop`，没有明确区分哪些 commuters 进入 road congestion block。

这意味着你还缺一个明确的 mapping 规则：

- 是否只使用 car commuters
- bus 是否进入 road congestion
- 若进入，如何将乘客转为 traffic units / PCU

这个问题不先处理，`Xi_kl` 的经济含义就会含糊。

#### Gap 7. 缺真正进入模型的 free-flow edge costs

你在 stage08 config 里写了：

- `free_flow_definition = grid_centroid_distance_over_v_ff_harm_min`

见：

- [data_work/src/stages/stage08_build_qsm_inputs.py](/Users/fxr/Desktop/TidalLanes/data_work/src/stages/stage08_build_qsm_inputs.py:31)

但 stage08 实际导出的 `edges_qsm` 只复制了 `edges`，并没有强制校验 `t_ff_min` 是否在文件里：

- [data_work/src/stages/stage08_build_qsm_inputs.py](/Users/fxr/Desktop/TidalLanes/data_work/src/stages/stage08_build_qsm_inputs.py:91)

而当前默认版本里，`qsm_input_edges_square.parquet` 实际没有 `t_ff_min`。

所以 free-flow data gap 不是概念问题，而是当前 pipeline-to-model handoff 本身没有闭合。

## 4. 当前最需要准备的数据

如果目标是把 `model_v1` 变成一个可以认真推进的版本，建议按下面顺序准备：

### 第一组：先把当前锁定版本的 model inputs 补全

针对你想保留的版本，至少重建：

- `qsm_input_nodes_*`
- `qsm_input_od_*`
- `qsm_input_edges_*`
- `grid_links_*_long.csv`
- `qsm_input_parameters_*.json`

而且需要明确：

- `t_obs`
- `t_ff`
- lane/capacity
- sample definition

### 第二组：明确模型到底跑哪张图

在进入下一轮 estimation 前，必须先选：

- `centerline-grid graph`
- 或 `raw-grid graph`

否则模型输入、counterfactual targeting 和 travel-cost interpretation 永远会错位。

### 第三组：补真实 traffic / density 数据

目标：

- 让 `lambda` 不再依赖纯 model-implied proxy

理想数据：

- link-direction-period traffic counts
- lane counts
- geometry / class / endpoint identifiers

### 第四组：补 policy-feasibility lane dataset

目标：

- 让 tidal-lane counterfactual 从“机械加减 1 lane”变成“可解释政策集”

### 第五组：补 mode-to-road mapping

目标：

- 明确哪些 OD commuters 进入 road congestion block

## 5. 推荐的下一步顺序

### 路线 A：先修模型定义

先统一：

- `t_{kl}` 到底采用 AA iceberg 版本
- 还是明确改成 additive-time variant

如果不先做这个决定，后续 `theta/lambda/welfare` 的解释都会不稳。

### 路线 B：先修数据入口

如果你希望先让 model 跑在当前版本上，则下一步最务实的是：

1. 给 `manual_centerline_rules_v11` 重建 stage08/QSM inputs
2. 检查 `t_ff_min` 是否真的进入 `qsm_input_edges_*`
3. 再重新跑 baseline model
4. 只在收敛和输入闭合后再解释参数与 welfare

### 路线 C：如果决定模型最终要跑 raw-grid

那就不要继续在 current centerline-grid model 上做太多局部修补，而应该开始设计：

- raw-grid 的 model input schema
- raw-grid 的 lane / asymmetry / policy targeting 映射
- raw-grid 与 OD sample 的 join logic

## 6. 一句话总结

当前真正的 bottleneck 不是“还缺一点估计技巧”，而是：

- 模型定义还没有和 AA 版本彻底统一
- 当前模型输入版本还没有和你现在锁定/偏好的数据版本统一
- `lambda` 所需的 traffic data 仍缺

在这三件事处理前，不建议把当前 `model_v1` 的结果当成可稳定解释的结构结果。
