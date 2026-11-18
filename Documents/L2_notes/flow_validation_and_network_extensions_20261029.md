# 联通 flow 交叉验证与 link 层面研究扩展

日期：2026-10-29。状态：首轮有向网格连接验证已完成，空间流量排序部分一致、方向性吻合较弱；结果见 [验证报告](unicom_model_flow_validation_results_20261029.md)。本文保留执行设计与其余研究扩展。

优先开展 Speed 联通道路压力与模型 link passage 的独立验证。福利分解、随机 link cost 和 link 间互补性先作为理论与稳健性扩展。Bottlenecks 可先在通勤模型内研究；供应链韧性与可贸易部门需要新增货运和生产网络数据及模型。

## 1. 已核对的数据入口

Speed 根目录：`/Users/fxr/Library/CloudStorage/Dropbox/Projects/Projects_FengGu/Speed/Road_processing/`。

以下路径相对该目录，本次已确认存在；只核对文件位置，未逐行审计内容：

| 数据 | 路径 | 下一步用途 |
| --- | --- | --- |
| 联通正式道路压力 | `GPS data/Speed202505/03_rn_pair_10min_pressure/exports/formal_counts/`，389份CSV | 经过日×有向RN pair×10分钟 |
| RN pair几何摘要 | `GPS data/outputs/grid_od_and_roads_20250519_20250525/tables/road_pair_geometry.csv` | RN到OSM映射起点，端点连线不是实际路线 |
| 有向路由图 | `V1-OSM/data/intermediate/route_v3/beijing_arcs.parquet` | 方向、连通性与路径检查 |
| Speed到OSM对应表 | `V1-OSM/data/raw/matching/beijing/mapping_all.csv` | Speed原始segment到OSM |
| Speed速度面板 | `V1-OSM/data/processed/panels/bj_base.parquet` | 同期速度与覆盖审计 |

详细字典入口：`GPS data/docs/DATA_AND_NETWORK_GUIDE_202505.md` 与 `GPS data/Speed202505/README.md`。这些文档保留Windows执行路径，以上是本机对应位置。文档登记的完整比较周为2025-05-19至25日；整月OD完成不代表整月道路压力也已完成。

TidalLanes保持gate5主线与既有拥堵设定。模型结果入口是 `Documents/L2_notes/model_and_results_summary_20260928.md`，基线运行目录是 `data_work/outputs/stage2_endogenous_edgeproj_v2_lcc_gate5_bpr_pos05_v1/`。需要从实际输出或求解器中核对并导出基线有向 passage Ξ、edge ID、单位及对应AM窗口，不能把OD flow直接当成link flow。

## 2. Flow一致性验证：按顺序执行

### A. 冻结口径与映射

1. 记录两侧的日期、时区、AM窗口、城市边界、通勤/全部机动车样本、计数单位与权重。联通是手机信令后加工路径压力，mode=1是算法判定机动车，不能视为实测车辆流量；不使用weight/gw扩样道路流量。模型是通勤person-equivalent passage，两者首先比较相对分布。
2. 联通采用经过时钟；OD采用出发时钟。核对跨窗口行程、10分钟槽及工作日覆盖。先用完整周工作日相同AM窗口；若模型对应其他年份，只能标记为跨期空间验证。
3. 构造RN→OSM有向弧→Speed→模型link的crosswalk。RN ID不等于OSM ID；Speed新路网也不等于模型既有grid link。保留geometry、方向、长度、匹配等级、覆盖率和歧义标记。
4. 同一RN路径跨多段时，经过量可传递到各段用于段上passage；汇总串联段至走廊时不能简单加总并称为独立行程数。并行段和方向分开处理。没有可信细粒度映射时，先在共同有向走廊层面比较。
5. 联通空槽保留缺失，不自动补零。n_unique_trips/users跨槽不能直接加总为全窗口独立数；n_valid_transitions与模型重复经过口径更接近，但必须先核对定义与重复计数。每个指标单独报告。

交付：输入manifest、变量/单位表、crosswalk、覆盖审计。无匹配和低置信度样本单列；不能用模型流量决定是否保留观测。

### B. 比较与诊断

- 主样本为高置信度共同支持集，报告道路数、长度及观测压力覆盖。稳健性扩大匹配范围，并按道路等级、区位、日期和方向分组。
- 比较归一化flow share、Spearman排序、log1p散点、头部link重叠和双向流量比；Pearson和水平拟合仅作辅助。共同支持集内的share一致不能说明全路网覆盖一致。
- 每日分别计算，再汇总稳定性；两侧总量差异与空间分布差异分别报告。可按走廊重抽样给出不确定性。
- 如果需要比例缩放，仅在训练日估计一个全局scale，在保留日验证；不得逐link缩放后宣称验证成功。
- 分开列出外部一致性与校准拟合。若模型使用了同一联通OD或道路速度，披露共享输入，并增加未参与估计的日期、道路或计数来源检验。
- 预先固定窗口、主指标及异常诊断规则，不按相关系数高低反复挑样本。当前不设没有依据的“通过”相关系数门槛。

交付：`data_work/outputs/unicom_model_flow_validation_v1/`（已生成首轮边界代理结果）的审计表、散点、方向比图、差异地图和一页结论。若差异集中在模式、时钟、路网覆盖或重复passage，先解释测量；其余再检查route choice及拥堵设定。

## 3. Welfare decomposition：保留原想法并安排推导

用户记忆线索：一篇使用天气state的文章；福利分解近似为“link usage × 投资margin × xxxx”。[TODO：待补题名、作者或PDF；第三项及原文公式未核实，不补造引用。]

可以先检验通用链式法则。令I_e为link投资、k_e为容量、W为当前福利对象：

`d log W / d I_e = (d log W / d log k_e) × (d log k_e / d I_e)`。

若想把第一项进一步写成link usage乘技术弹性、福利权重或拥堵外部性，必须从当前模型推导；不能直接把usage当福利权重。区分固定流量的直接收益、路由与拥堵反馈、职住sorting及外部性。离散大投资应沿政策路径积分或直接重解，再报告局部分解余项。

潮汐车道是容量重分配：慢向增加与快向减少必须同时计入，可另加实施成本。单向扩容的边际收益不能直接作为潮汐收益。

执行顺序：先核实文章；推导固定职住与GE两套导数；对单link做小扰动有限差分；验证分解与实际福利变化、正反扰动及步长收敛。沿用已修正的Fréchet福利口径。物理时间采用ΣΞm，不能用tau代替分钟。既有P6/P7定价结果有实现问题，不能用作该分解的有效基准。

## 4. Stochastic link cost与拥堵函数

候选对象是 `m_e(q_e,k_e,s)`，state s可以是天气、施工、事故、道路管制或外生需求冲击。先区分供给状态改变free-flow/capacity，与需求状态改变OD；不要把流量本身定义为外生state。

先做固定职住的两状态实验，分别重解路由和拥堵，比较状态概率加权福利、尾部延误及政策排序。明确出行者是否在选择路线前观察state；事前与事后选择对应不同福利对象，不能用平均成本替代期望福利。

当前anchored主规格保留。新增函数先作为独立稳健性版本，并与2026-10-07 Brad会议的congestion technology任务合并推进。观察到的速度与flow同时由均衡决定；拟合速度流量相关性不能单独识别技术参数。天气或冲击也可能改变出行需求，识别需要单独论证。

## 5. Co-utilization、complements/substitutes与bottlenecks

共同使用可定义为同一OD/路径中两条link的联合经过概率或联合暴露。需有路径联合矩或模型route kernel；仅靠边际link流量无法恢复。先从模型构造，联通仅在合法且已有的聚合路径信息足够时外部验证，不新增微观导出。

分别回答三个问题：

- 描述性co-utilization：控制共同需求与距离后，哪些link经常出现在同一路径？同向串联与并行替代路线分开。
- 流量替代：降低link e成本或增容量后，f的flow如何变化？明确扰动对象并冻结或放开OD。
- 福利互补：容量k_e、k_f的联合投资收益是否超过分别投资收益之和？使用交叉导数，或 `W(e,f)-W(e)-W(f)+W(0)`，正值为所定义福利对象下的互补，负值为替代。联合使用高不保证福利互补。

最小执行：在既定候选走廊内选串联、并行和共享瓶颈的少量pair，固定总投资预算及容量扰动，比较单独/联合反事实，再扩展到已有policy-bundle选边计划。有限差分报告步长、收敛和归一化，避免把数值误差当交互效应。

Bottleneck排序同时报告基线使用、增容边际福利、移除/降容损失及绕行替代性；高flow或高betweenness不是充分证据。随机state下另看尾部损失与备用通道价值。

供应链韧性/tradable部门作为远期扩展：需要货运OD、行业投入产出或企业买卖联系、交付时间价值及库存/替代供应商等机制。通勤手机信令不能直接验证货物运输或供应链福利。先完成通勤网络内的bottleneck与可靠性分析，再决定是否另建生产贸易模块。

## 6. 当前行动与完成标准

| 顺序 | 下一行动 | 完成标准 |
| --- | --- | --- |
| P1（首轮已完成） | 基线Ξ导出与RN→OSM支持的有向网格边界crosswalk | 口径、覆盖和样本已记录；逐物理道路映射仍属后续 |
| P2（已完成） | 主样本、敏感性、方向与每日比较 | 图表、差异解释、共享输入披露与核验已保存 |
| P3 | 补齐文章并推导福利边际 | 原文公式、模型映射与有限差分对齐 |
| P4 | 两状态拥堵与小规模link pair实验 | 独立版本、完整福利账与收敛检查 |
| 远期 | 供应链/可贸易部门可行性 | 数据与模型闭合条件明确后再启动 |

资料定位、执行设计与首轮网格边界flow验证均已完成；论文核实及模型扩展仍待执行。原设计中的逐物理道路crosswalk未被本次网格边界代理替代为已完成项。
