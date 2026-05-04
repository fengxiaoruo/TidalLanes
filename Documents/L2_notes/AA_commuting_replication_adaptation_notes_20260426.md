# AA commuting replication package 改造为本项目数据版本的操作笔记

## 目的

这份笔记记录如何把 `Literatures/AA_ReplicationFinal` 中 Seattle commuting 这条 replication package，改造成适配本项目数据特征的版本。这里的核心目标不是“照抄 AA 的全部输入形式”，而是在尽量保留其模型骨架、均衡求解顺序和 exact-hat 结构的前提下，把本项目可直接观测到的数据对象接到对应的位置。

当前判断是：应尽量保留 AA 的 **traffic / cost / hat algebra** 主体，不建议一开始就把内部求解器彻底改写成纯 speed-space。更稳妥的路线是：

1. 输入层：用本项目的 directed speed、free-flow speed、车道数、长度等边级变量恢复 network edge traffic state。
2. 模型核心层：仍然保留 AA commuting 的 inversion、基准均衡和 exact-hat counterfactual 框架。
3. 输出层：把 counterfactual 后的 edge state 再映射回 equilibrium time / speed，供实证描述和图表使用。

## 一、AA 原 replication package 的最小核心

如果只看 commuting mode，AA 原包最小上只依赖三类对象：

1. 节点端居住与就业质量
   - 对应 `l_R`, `l_F`
2. network edge 上的 observed traffic state
   - 在原包中体现在 `Xi_ij`
3. 一组外部给定或外部估计的参数
   - `theta`, `lambda`, `alpha`, `beta`

原始 Seattle 路线的代码顺序是：

1. `analysis/seattle/*.do`
   - 生成网格节点、相邻边、traffic、commuting OD
2. `analysis/seattle/predict_commuting.m`
   - 用 observed traffic 反推 predicted commuting OD，做模型 validation
3. `analysis/figure1/fn_AA_calc_eqm_commuting_static.m`
4. `analysis/figure1/fn_AA_eqm_lr_lf.m`
   - 解基准均衡
5. `counterfactuals/seattle/fn_AA_calc_eqm_commuting_counterfactual.m`
6. `counterfactuals/seattle/fn_AA_eqm_lr_lf_counterfactual.m`
   - 解 exact-hat counterfactual
7. `counterfactuals/seattle/asym_counterfactual_berlin.m`
   - 对每一条 directed edge 做 1% improvement

## 二、本项目数据条件下的替换原则

本项目当前更自然的直接观测对象不是 edge-level flow，而是：

- directed observed speed
- free-flow speed 或 free-flow time
- 车道数量
- 路段长度
- network directed topology
- O 与 D 端的人口/就业分布

因此需要先完成一个“速度输入层”的替换：

### 保留的部分

以下部分建议尽量保留 AA 原结构：

1. `Xi_ij + l_R + l_F -> predicted Lij`
2. `l_R`, `l_F`, `chi` 的基准均衡固定点求解
3. `t_bar_hat / chi_hat / l_Rhat / l_Fhat / Xi_ij_hat` 的 exact-hat 反事实系统
4. 边级 improvement 的批处理方式

### 替换的部分

以下部分需要改成适配本项目数据的版本：

1. `observed traffic Xi_ij` 的构造方式
   - 原包：更接近直接用 observed traffic
   - 本项目：先用 observed speed + free-flow speed + 车道数 + speed-flow 函数恢复 edge traffic state
2. `t_bar_hat` 的政策/工程含义
   - 原包：一条边 iceberg cost 下降 1%
   - 本项目：可由 capacity increase、lane reallocation、free-flow time reduction 等工程变化映射而来

## 三、本项目版本的推荐实现顺序

### Step 1：统一边级基础数据对象

每条 directed edge 至少要标准化为：

- `from_node`
- `to_node`
- `length_m`
- `lanes`
- `speed_obs_kmh`
- `speed_ff_kmh`

推荐同步构造：

- `time_obs_min`
- `time_ff_min`
- `capacity_proxy`

其中：

- `time_obs_min = length / speed_obs`
- `time_ff_min = length / speed_ff`
- `capacity_proxy` 至少先用 `lanes * capacity_per_lane` 作为一阶近似

### Step 2：给定 speed-flow 函数，并从速度恢复边流量状态

这一步是相对于 AA 原包最大的新增环节，也是后续识别最关键的地方。

需要外部给定：

- speed-flow 函数形式
- 参数值

可行写法包括：

1. `t / t_ff = g(q / cap)`
2. `v = h(q, cap, v_ff)`
3. BPR 类时间函数

只要函数单调并可逆，就可以从：

- `speed_obs`
- `speed_ff`
- `lanes`

反推出：

- `q_e`
- 或与 `Xi_ij` 同尺度的 edge traffic intensity

这一步完成后，建议先把恢复出的 `q_e` 明确写成项目版本的 `Xi_ij`，不要立刻改动后面的 AA 求解器结构。

### Step 3：节点端质量的构造

AA commuting 需要的不是实际轨迹，而是节点端的 residence / workplace mass：

- `l_R`
- `l_F`

本项目需要决定：

1. 节点层级是什么
   - grid
   - TAZ
   - network-attached demand node
2. `l_R` 与 `l_F` 的来源
   - 常住人口 / 夜间人口
   - 就业岗位 / 白天人口 / workplace mass proxy
3. 总量一致化是否需要模仿 AA 的顺序处理

若后续要保留 AA `predict_commuting` 的对照逻辑，节点顺序和索引映射要从一开始固定。

### Step 4：先跑 traffic -> predicted commuting 的 inversion

若项目里没有真实 commuting OD，可把这一步当作内部一致性检查，而不是外部 validation。

若有外部 commuting OD、手机定位 OD、抽样调查 OD 或任意可比 OD 数据，这一步应当优先做。

推荐流程：

1. 用速度恢复的 `Xi_ij`
2. 给定 `l_R`, `l_F`
3. 跑 AA commuting inversion
4. 得到 `predicted Lij`
5. 与任何可获得的真实 OD 做相关性、排序、空间分布对照

这一步若表现很差，说明问题多半在：

- speed-flow calibration
- network topology
- 节点 mass 口径

而不是后面的 exact-hat solver。

### Step 5：基准均衡的最小实现

在保留 AA 结构的前提下，基准均衡部分优先只做最小改动：

1. 保留 `fn_AA_calc_eqm_commuting_static` 的方程结构
2. 保留 `fn_AA_eqm_lr_lf` 的内外两层固定点求解
3. 保留 `chi` 作为系统尺度闭合项

如果后续发现需要把 edge-level capacity / free-flow condition 更显式地嵌入基准系统，再在此基础上做第二轮扩展，而不是一开始就重写整个 solver。

### Step 6：把反事实变量改写成更符合本项目工程含义的对象

本项目版本建议优先使用以下 hat 变量：

1. `cap_hat_e`
   - 车道增加、潮汐车道、capacity reallocation
2. `t0_hat_e`
   - free-flow time 的变化
3. `q_hat_e`
   - 反事实交通状态
4. `time_hat_e`
   - 实际通行时间变化
5. `speed_hat_e`
   - 实际速度变化

实现上建议顺序是：

`cap_hat / t0_hat -> q_hat -> time_hat -> speed_hat`

而不是一开始直接把 solver 写成“速度 hat”系统。这样更容易保持与 AA 的 exact-hat 结构一致。

### Step 7：把边改善批处理和本项目实验设计连接起来

AA 原包是“每条 directed edge 的 iceberg cost 下降 1%”。

本项目需要先明确哪类实验更自然：

1. 单边 lane increase
2. 单边潮汐车道重分配
3. 某一组方向边的 capacity shift
4. corridor-level policy shock

技术上仍然建议保留原包的批处理逻辑：

1. 枚举边或枚举 corridor
2. 每次只改一组边的基础供给条件
3. 跑 exact-hat
4. 保存 `chi_hat`, `l_Rhat`, `l_Fhat`, `Xi_ij_hat`
5. 再映射成速度、时间、福利和排序指标

## 四、与原 replication package 对齐时最容易出错的地方

### 1. 不要把“速度”直接当成内部唯一状态变量

更稳的做法是：

- 输入层用速度恢复流量
- 内部仍保留 traffic / cost / hat algebra
- 输出层再转回速度

### 2. 不要提前依赖真实 OD route/path

AA commuting 不需要真实车辆轨迹，也不需要每个 OD 的实际路径。需要的是：

- network edge structure
- edge traffic state
- 节点端 mass

### 3. 识别最关键的是 speed-flow mapping，而不是后半段 solver

真正最需要单独记录和反复检查的是：

1. speed-flow 函数形式
2. 参数来源
3. capacity 如何构造
4. 是否识别的是绝对 flow，还是相对 traffic intensity
5. 是否需要外部 aggregate anchor

### 4. 若以速度为输入，validation 顺序必须前置

推荐顺序始终是：

1. 先验证 `Xi_ij -> predicted Lij`
2. 再跑基准均衡
3. 再跑 exact-hat counterfactual

不要跳过 inversion/validation 直接做政策反事实。

## 五、建议在代码层面如何修改现有 python_commuting

当前 `python_commuting` 最小实现已经对齐了 AA 原 Seattle 版本，后续若切到速度输入，建议按下列顺序改：

### 第一组：新增输入层模块

新增一个 speed-to-flow 模块，负责：

1. 读入边级：
   - `length`
   - `lanes`
   - `speed_obs`
   - `speed_ff`
2. 读入外部 calibration 参数
3. 输出：
   - `Xi_ij`
   - `time_obs`
   - `time_ff`
   - `vc_ratio` 或其他中间诊断量

### 第二组：保留并复用现有模块

以下现有模块建议尽量直接复用：

1. `predict_commuting.py`
2. `static_eqm.py`
3. `counterfactual.py`

只有当新的 `Xi_ij` 定义或 `t_bar_hat` 定义发生根本变化时，再进入这些文件内部做第二轮改动。

### 第三组：新增输出映射层

在 exact-hat 之后新增一个模块，把：

- `Xi_ij_hat`
- `cap_hat`
- `t0_hat`

重新映射成：

- `time_hat`
- `speed_hat`
- 边级速度分布变化
- corridor-level 速度改善汇总

## 六、当前建议的工作顺序

1. 先固定节点层级与边层级数据结构
2. 先固定 speed-flow 函数与参数来源
3. 先实现速度到 `Xi_ij` 的恢复
4. 先跑 `Xi_ij -> predicted Lij` 的 inversion 检查
5. 再决定是否直接沿用当前 `python_commuting` 的 solver，或是否需要在基准均衡里进一步显式嵌入 `cap` 与 `t_ff`
6. 最后再考虑论文 `tex` 中如何描述模型与数据适配

## 七、在后续 tex 与估计代码准备时需要回看本笔记的地方

当后续开始：

1. 修改模型 `tex`
2. 整理 estimation code
3. 决定 empirical workflow

应优先回看本笔记中的三部分：

- “二、本项目数据条件下的替换原则”
- “三、本项目版本的推荐实现顺序”
- “四、与原 replication package 对齐时最容易出错的地方”

原因是：后续 tex 写作若先行，很容易把模型描述写成“直接观测 flow”版本；而后续代码若先行，也容易在没有固定 speed-flow 输入层定义的情况下过早改写 solver。两者都应以本笔记为准，先把数据入口与保留/替换边界说清楚。

## 八、若当前目标只是“先快速跑通一版”，最简化的数据处理清单

如果当前目标不是立刻改写 solver，而是**先完全应用 AA 原 Seattle commuting package 的代码和输入格式，尽快跑通一版**，最稳妥的做法是：

- 不先改 `predict_commuting.m`
- 不先改 `fn_AA_eqm_lr_lf_counterfactual.m`
- 只把本项目数据整理成 AA 原包那三个最小输入文件

也就是先把自己的数据“硬整理”成原包能直接读取的格式，再跑原始 Matlab 逻辑。

### 8.1 先固定应用哪条线

只套用 AA 的 **Seattle commuting** 线，最小只依赖：

1. `counterfactuals/seattle/sparse_adjmat_seattle.csv`
2. `counterfactuals/seattle/node_lr_lf_seattle.csv`
3. `counterfactuals/seattle/sparse_commute_seattle.csv`

如果当前还没有真实 commuting OD，第 3 个文件可以先缺席，只跑：

- `predict_commuting.m` 以外的部分
- 或先放一个占位测试版本，仅检查代码读写

### 8.2 第一步：先固定节点体系

先选定一套节点。当前最简化建议是直接用现有 grid 作为节点。

要求：

1. 每个节点有唯一 ID
2. ID 最好是连续编号：`1, 2, ..., N`
3. 每个节点至少有：
   - `node_id`
   - `residential_mass`
   - `workplace_mass`
   - `lon`
   - `lat`

最终把它写成：

`counterfactuals/seattle/node_lr_lf_seattle.csv`

格式严格仿原文件，无表头 5 列：

1. `h_index`
2. `h_residents`
3. `h_laborforce`
4. `h_lon`
5. `h_lat`

也就是说，当前最小版本里：

- `h_residents` 对应 `l_R`
- `h_laborforce` 对应 `l_F`

如果当前 workplace 端只有 proxy，也可以先用 proxy 跑通第一版。

### 8.3 第二步：先生成 directed edge 邻接关系

这里最重要的不是 OD pair，而是 **network 的 directed edge list**。

要做的事情是：

1. 决定哪些节点之间存在“直接边”
2. 给每条 directed edge 一个：
   - `from_id`
   - `to_id`
   - `traffic`
   - `length`
   - `travel_time`

当前最简化做法建议是：

- 只保留那些你已经有 `speed_obs / speed_ff / length / lanes` 的 directed edge
- 暂时不要追求更复杂的 corridor aggregation

### 8.4 第三步：用速度恢复 edge traffic，填进 `sparse_adjmat_seattle.csv`

这是与你的数据条件最相关、也是最适合先做输入层替换的一步。

对每条 edge，先准备：

1. `length`
2. `lanes`
3. `speed_obs`
4. `speed_ff`

然后计算：

1. `time_obs = length / speed_obs`
2. `time_ff = length / speed_ff`

再给定一个 speed-flow 函数和参数，反推出：

- `traffic_ij`

在“先快速跑通”的阶段，不要求这个 `traffic_ij` 已经是最完美的 flow measure，但要求：

1. 与拥堵程度单调相关
2. 在边之间可比较
3. 在方向上保留 asymmetry
4. 没有 NaN，且原则上为正

最终写成：

`counterfactuals/seattle/sparse_adjmat_seattle.csv`

格式严格仿原文件，无表头 5 列：

1. `h_index`
2. `w_index`
3. `traffic`
4. `total_length`
5. `georoute_time`

也就是：

1. 起点节点 ID
2. 终点节点 ID
3. 由速度恢复出的 edge traffic state
4. 边长度
5. 观测 travel time

注意：

- 原核心 Matlab 求解器主要用前 3 列
- 但建议 5 列都补齐，保持与原包兼容

### 8.5 第四步：若有真实 OD，再整理 `sparse_commute_seattle.csv`

如果当前有任何可比 commuting OD 数据源，可以整理成：

`counterfactuals/seattle/sparse_commute_seattle.csv`

格式无表头 3 列：

1. `h_index`
2. `w_index`
3. `grid_commuters`

也就是：

1. origin node ID
2. destination node ID
3. commuter flow

如果当前没有真实 OD：

- 可以先跳过这一步
- 或仅做一个占位版本测试 `predict_commuting.m` 是否能正常读取

但需要明确：没有真实 OD 时，这一步不能承担 validation 作用。

### 8.6 第五步：把 Seattle 原脚本里所有 `217` 改成你的 `N`

这是最容易遗漏、但最关键的兼容步骤。

AA Seattle 原包默认节点数是 `217`。如果你的节点数不是 217，必须先改所有 hard-coded 维度。

优先检查：

1. `analysis/seattle/predict_commuting.m`
2. `analysis/seattle/seattle_link_intensity.m`
3. `counterfactuals/seattle/asym_counterfactual_berlin.m`
4. `counterfactuals/seattle/asym_counterfactual_berlin_t4.m`
5. `counterfactuals/seattle/asym_counterfactual_berlin_ns.m`
6. `counterfactuals/seattle/asym_counterfactual_berlin_d016.m`
7. `counterfactuals/seattle/asym_nc_counterfactual_berlin.m`

所有 `sparse(..., 217, 217)` 或等价写法，都应先改成你的节点数 `N`。

最稳妥的最小改法是：

1. 先读取 `node_lr_lf_seattle.csv`
2. 设 `N = size(nodes,1);`
3. 后面所有矩阵维度都用 `N`

### 8.7 第六步：第一轮只跑两个东西

为了尽快定位问题，不要一开始就整网批处理。

第一轮建议只跑：

1. `predict_commuting.m`
   - 检查文件格式、维度、`Xi_ij + l_R + l_F` 是否能正常读入和反推出 `predicted_lij`
2. 单边 counterfactual
   - 只选一条边 `(i,j)`，令 `t_bar_hat_iter(i,j)=0.99`
   - 跑一次 `fn_AA_eqm_lr_lf_counterfactual(...)`

这比直接跑整网更容易判断：

- 是输入文件问题
- 是节点索引问题
- 还是 solver 本身出了问题

### 8.8 第七步：第一轮参数先完全沿用 AA 原值

为避免“输入问题”和“参数变化”缠在一起，第一轮建议完全沿用 AA Seattle 的原始参数：

1. `theta = 6.83`
2. `delta1 = 0.488`
3. `lambda = (1/theta) * delta1`
4. `alpha = -0.12`
5. `beta = -0.1`

等原包完全跑通以后，再考虑替换成你自己的：

- congestion calibration
- `theta`
- `alpha / beta`

### 8.9 第八步：第一轮最该检查的五件事

先不要急着解释结果经济含义，先检查这些基础问题：

1. `node_lr_lf_seattle.csv` 的节点编号是否连续
2. `sparse_adjmat_seattle.csv` 中的边是否真是 directed edge
3. `traffic` 是否全为正、没有 NaN
4. `speed_obs < speed_ff` 时，是否能合理映射成更高拥堵/更大 traffic state
5. 所有输入文件的节点数是否一致

### 8.10 当前“先跑通一版”的一句话版本

当前最小可执行顺序就是：

1. 选定 grid 节点并连续编号
2. 为每个节点准备 `l_R, l_F, lon, lat`
3. 从 directed `speed_obs / speed_ff / lanes / length` 恢复 edge traffic
4. 写成 `sparse_adjmat_seattle.csv`
5. 若有真实 OD，写成 `sparse_commute_seattle.csv`
6. 把 Seattle Matlab 脚本里的 `217` 改成你的 `N`
7. 先跑 `predict_commuting.m`
8. 再跑单边 counterfactual
9. 最后再考虑整网批处理

### 8.11 这一步完成后再做什么

只有当“原始 AA package + 你整理好的输入格式”已经跑通之后，才进入第二阶段：

1. 决定是否继续保留原 solver，不动内部结构
2. 决定是否把速度输入层正式模块化
3. 决定是否在 `tex` 中改写模型叙述为 speed-input 版本

换句话说，这一节对应的是：

**先按原 package 格式跑通，再考虑更优雅的模型/代码改造。**

---

## 9. 反事实框架的扩展用法（2026-04-28 补充）

### 9.1 t_bar_hat 是唯一的 policy 入口

`solve_counterfactual_lr_lf` 接受任意形状的 N×N `t_bar_hat` 矩阵。所有外生政策冲击都通过这个矩阵进入模型，求解器本身不需要改动。

- 单边微小冲击（现有代码）：`t_bar_hat[i,j] = 0.99`
- 潮汐车道（双向容量重分配）：`t_bar_hat[i,j] = 0.90, t_bar_hat[j,i] = 1.10`
- 全网络政策：对任意边集合赋值，求解器全部支持

输入文件（`sparse_adjmat_seattle.csv` 等）保存**基线均衡状态**，不随政策变化。反事实结果以 hat 值返回：`chi_hat`（福利乘数）、`l_r_hat`（居住人口变化比例向量）、`l_f_hat`（就业人口变化比例向量）、`xi_ij_hat`（traffic flow 变化比例矩阵）。

### 9.2 Exact-hat 的正确理解

- **不是局部近似**：exact-hat 对任意大小冲击精确成立，不是一阶线性化或差分，名字里"exact"是关键词
- **无法给出水平值**：只能给出比值（hat）。基线福利水平 chi_0 依赖无法识别的 T_bar、u_bar，在 hat 中消掉了
- **xi_ij 是基线充分统计量**：输入的 xi_ij 不是"固定 flow 不变"，而是告诉模型基线均衡结构；反事实 flow = xi_ij × xi_ij_hat 是内生的

### 9.3 用 exact-hat 实现两个世界的福利水平对比

**问题**：想比较"非对称 travel cost 世界"与"对称 travel cost 世界"的福利差异，即求 chi_sym / chi_asym。

**解法**：构造一个 t_bar_hat，使其恰好把非对称基线均衡推到对称均衡。

**推导**：在 exact-hat 框架中，xi_ij_hat 的解析表达式为（T_bar_hat=1, u_bar_hat=1）：

$$\hat{\xi}_{ij} = \hat{\chi}^{-\frac{1}{1+\theta\lambda}} \cdot \hat{t}_{ij}^{-\frac{\theta}{1+\theta\lambda}} \cdot \hat{l}_{r,j}^{\frac{1-\beta\theta}{1+\theta\lambda}} \cdot \hat{l}_{f,i}^{\frac{1-\alpha\theta}{1+\theta\lambda}}$$

令目标 $\hat{\xi}_{ij} = \xi^{sym}_{ij} / \xi^{asym}_{ij}$，反解 $\hat{t}_{ij}$：

$$\hat{t}_{ij} = \left(\frac{\xi^{sym}_{ij}}{\xi^{asym}_{ij}}\right)^{-\frac{1+\theta\lambda}{\theta}} \cdot \hat{\chi}^{-\frac{1}{\theta}} \cdot \hat{l}_{r,j}^{\frac{1-\beta\theta}{\theta}} \cdot \hat{l}_{f,i}^{\frac{1-\alpha\theta}{\theta}}$$

**方式 A：一阶近似（令 chi_hat≈1, l_r_hat≈1, l_f_hat≈1）**

$$\hat{t}_{ij}^{(0)} = \left(\frac{\xi^{sym}_{ij}}{\xi^{asym}_{ij}}\right)^{-\frac{1+\theta\lambda}{\theta}}$$

代入本项目参数（theta=6.83, lambda=0.0714），指数约为 0.218。即使 xi 比值偏离 1 有 20%，t_bar_hat 只偏离 1 约 4–5%，一阶近似误差很小。

代码实现：

```python
theta, lambd = 6.83, (1/6.83)*0.488
exponent = (1 + theta * lambd) / theta   # ≈ 0.218

xi_asym = data.xi_ij
xi_sym  = (xi_asym + xi_asym.T) / 2.0

ratio = np.where(xi_asym > 0, xi_sym / xi_asym, 1.0)
t_bar_hat = ratio ** (-exponent)
np.fill_diagonal(t_bar_hat, 1.0)

result = solve_counterfactual_lr_lf(t_bar_hat=t_bar_hat, ..., xi_ij=xi_asym, ...)
# result.chi_hat ≈ chi_sym / chi_asym
```

**方式 B：精确迭代（2–3 次收敛）**

```
1. t_bar_hat^(0) = 一阶近似
2. 跑 solve_counterfactual_lr_lf → 得 chi_hat^(0), l_r_hat^(0), l_f_hat^(0)
3. 代入完整公式更新 t_bar_hat^(1)
4. 重复直到 t_bar_hat 不再变化（通常 2–3 次）
```

l_r_hat、l_f_hat 量级约 1e-6，对 t_bar_hat 的修正极小，迭代收敛快。

**结果解读**：`result.chi_hat` 即为两个世界的精确福利比值 chi_sym / chi_asym。chi_hat < 1 表示对称世界福利成本更低（=福利更高）。

### 9.4 当前已完成的比较（2026-04-28）

已在 `Replication_apply/results/asym_vs_sym/` 中完成 272 条边的 asym vs sym 边际价值对比：
- `chi_hat` 差异的相对量级约为效应本身的 25%（有散度但均值接近零，无系统偏差）
- `l_r_hat` 差异的相对量级约为 47%，`l_f_hat` 约为 34%
- 非对称 baseline 对节点级人口分布估计影响更大，对总体福利排序影响较小
- 完整 4087 条边的批量运行脚本已就绪（`compare_batch.py`），推荐在本机跑

下一步：实现 9.3 节的 t_bar_hat 构造，得到两个世界之间的单一福利比值。

---

## 十、潮汐车道反事实：两种 t_bar_hat 构造方式的对比（2026-04-29）

### 10.1 政策设置

选取 AM 峰值期非对称性排名前 20 的双向路段（asym_ratio 4.1× ~ 14.8×），将双向出行时间对称化：tau_sym = (tau_ij + tau_ji) / 2。

### 10.2 方式 A：拥堵反推法（congestion inversion）

```
xi_sym_ij = lanes_ij × (tau_sym / tau_ff_ij)^(1/delta1)  # 同一车道数下，tau_sym 对应的均衡车流
xi_asym_ij = lanes_ij × (tau_obs_ij / tau_ff_ij)^(1/delta1)
t_bar_hat[i,j] = (xi_sym_ij / xi_asym_ij)^(-(1+theta*lambda)/theta)
```

**符号逻辑**：tau_sym < tau_obs（拥堵方向改善）→ xi_sym < xi_asym → t_bar_hat > 1（成本**上升**）。

**反直觉结论**：道路更快 → 模型推断均衡车流更少 → 为维持更少通勤者，需要更高 t_bar。

**反事实结果（top-20）**：
- chi_hat = 1.0000754  → 福利**恶化** +75.4 × 10⁻⁶

**识别含义**：这是一个"固定车道数、调整均衡通勤流"的反事实，而非物理产能重分配。在拥堵均衡中，拥堵路段吸引大量通勤者正是因为其 t_bar 本身很低（fundamentals 好）。把 tau 对称化（保持车道）意味着减少该方向通勤流，需要提高 t_bar，导致福利下降。

### 10.3 方式 B：直接时间比法（direct travel-time ratio）

```
t_bar_hat[i,j] = (tau_sym / tau_ij)^epsilon
```

假设 iceberg cost 正比于出行时间的 epsilon 次幂（t_bar ∝ tau^epsilon）。

**符号逻辑**：
- 拥堵方向：tau_sym < tau_ij → t_bar_hat < 1（成本**下降**）→ 更多通勤者选择该方向
- 畅通方向：tau_sym > tau_ji → t_bar_hat > 1（成本**上升**）→ 较少人走该方向
- 成本下降惠及人数多（拥堵方向），成本上升影响人数少（畅通方向）→ 净福利改善

**反事实结果（top-20，epsilon=0.1）**：
- chi_hat = 0.9999794  → 福利**改善** −20.6 × 10⁻⁶

**对 epsilon 的敏感性**（需本机运行）：
- epsilon=0.1（保守）：已确认收敛，福利改善 −20.6 × 10⁻⁶
- epsilon=0.5 / 1.0：产生更大冲击（t_bar_hat 最大值 2.81× / 14.76×），需在本机以 tol=1e-4 运行

**物理含义**：潮汐车道把产能从畅通方向调配给拥堵方向 → 拥堵方向路速提升（tau↓）→ t_bar↓ → 更多通勤者选择该方向 → 福利改善。这更贴近潮汐车道的物理机制。

### 10.4 两种方式的核心区别

| 维度 | 方式 A（拥堵反推） | 方式 B（直接时间比） |
|---|---|---|
| t_bar_hat 构造 | 通过均衡车流反推 | 直接用时间比 |
| 拥堵方向符号 | t_bar_hat > 1（成本升） | t_bar_hat < 1（成本降） |
| 畅通方向符号 | t_bar_hat < 1（成本降） | t_bar_hat > 1（成本升） |
| 福利结论 | 恶化 (+75.4 × 10⁻⁶) | 改善 (−20.6 × 10⁻⁶) |
| 人口扰动幅度 | l_r std ≈ 3%（大） | l_r std ≈ 0.6%（小） |
| 物理对应 | "固定车道，调均衡流" | "产能重分配，出行时间改变" |
| 对潮汐车道的适合度 | 不直接对应 | 更贴近实际政策 |

### 10.5 建议

- **主结果**：用方式 B（epsilon=0.1）作为潮汐车道主要估计
- **稳健性**：用方式 A 作为对比，说明 t_bar_hat 构造选择对符号的影响
- **epsilon 敏感性**：本机运行 `tidal_lane_alt_spec.py`，对 epsilon ∈ {0.1, 0.5, 1.0} 全部跑出来
- **理论基础**：在 tex 文档中明确 t_bar 与 tau 的映射假设，以及方式 A/B 各自对应的经济含义
- **代码位置**：
  - 方式 A：`Replication_apply/python_commuting/tidal_lane_counterfactual.py`
  - 方式 B：`Replication_apply/python_commuting/tidal_lane_alt_spec.py`
  - 结果输出：`Replication_apply/results/tidal_lane/`
