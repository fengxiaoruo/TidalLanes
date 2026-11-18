# 合作者会议修改意见，2026-07-10

## 背景

本轮意见主要针对当前 slides / paper 内容的可展示性和下一轮模型诊断。核心不是立刻改参数，而是重新选择几个更有解释力的结果口径：全国职住分离证据是否聚焦大城市，symmetric-input 部分是否改用完整重估后的 fundamentals 分布诊断，潮汐政策结果是否改成人数加总口径，以及反事实是否加入城市扩张和人口增长。

## 需要转成任务的意见

### 1. 全国职住分离证据

- 如果全国口径下关键 measure 的量级不够，或图形不够清楚，可以把主图改为大城市样本。
- 在大城市样本里检查 spatial unit 是否发挥作用，而不是只报全国平均。
- 可优先比较不同城市规模组或 top-N 城市的职住分离、中心导向通勤和不对称暴露。

### 2. Symmetric-input 叙事

- "Why symmetric" 应该回到完整重解均衡、重新 recover fundamentals 的实验。
- 之前已经做过完整重估，结果的平均影响不大；因此表述不要只盯平均值。
- 直觉上更应影响 fundamentals 的分布形状，以及它们和城市空间结构的关系。
- 下一步尝试不用平均值，而是看关键分布和到市中心距离的关系：
  - 每个城市或每个空间单元计算 gradient；
  - 比较离差平方和、L2 距离、城市内方差等指标；
  - 用这些指标反映城市内部异质性是否被对称化掩盖。
- 北京可以做更具体的例子：
  - 看六环附近的居住 fundamental 如何变；
  - 看城中区域如何变；
  - 看整体分布如何变。

### 3. Tidal analysis 的时间节省口径

- 关于节省时间的结果，不只报人均秒数或百分比。
- 改成更直观的 aggregate exposure：
  - 多少人受影响；
  - 这些人在所有 treated segments 上一共节省多少时间；
  - 经过某个 segment 的人，在这个 segment 上总共节省多少时间。

### 4. Sorting 页与第 27 页

- 在 sorting 结果里补看 `u_i` 和 `A_j` 的变化。
- 同时看工资和 `u_i` 的变化趋势，避免 sorting 只讲人口重新分布。
- 第 27 页可考虑把 sorting decomposition 和 fundamentals / wage trends 放在同一个逻辑链条里。

### 5. 反事实扩展

- 反事实部分可以加入城市扩张和人口增长。
- 目标是看潮汐车道、道路扩张或定价等政策，在城市规模扩大或人口增长时的影响是否被放大、削弱或改变排序。

## 建议的近期执行顺序

1. 先改 evidence framing：全国职住分离改为大城市优先，并补 spatial-unit diagnostic。
2. 再改 symmetric-input 部分：基于完整重估结果画 fundamentals 分布、中心距离 gradient、L2 / 方差指标，北京单独做空间例子。
3. 同步调整 tidal analysis 的时间节省指标，改成 segment-person-time total 和 exposed commuters。
4. 更新 sorting 页：补 `u_i`、`A_j`、工资与 amenity / productivity trend。
5. 最后设计 population growth / urban expansion counterfactual，不急于并入主表，先做 smoke grid。

