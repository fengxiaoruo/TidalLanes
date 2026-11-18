# 合作者会议修改意见，2026-07-27

## 背景

本轮讨论围绕 `07/21 Progress` 大纲推进，整理下一轮修改方向。核心不是立刻改参数，而是同步四条主线的表述与可执行任务：Data（路网与 grid 构造）、Measures（职住分离与方向性事实的因子解释）、模型评价与反事实（制图、symmetric / sorting / targeting）、以及深圳实地断点证据。第二页补充 incidence 分解、四种人分组福利，以及 congestion technology 的 interpretation 调整。

手写原稿页码：notebook 22/26、23/26。

## 需要转成任务的意见

### 1. Data：路网、中心线、grid-construction、city-choice

- 变量推导仍服务后续 measures 与模型输入。
- 更新描述时锁定两条规则：
  1. 仍然以优先速度最快绕过网格为原则（check-policy）。
  2. 采用中心化后的路网来做（此处指 **centerline**，不是 raw_connected）。
- Grid-construction 与 city-choice 继续作为 data 段并列条目，不拆成旁线。

### 2. Measures：Facts、Directionality、联系广度

- 职住分离仍是主展示对象；全国口径接 Kongkai 数据，并保留 Beijing / Shenzhen 的城市例子。
- Segment 与城市中心两层都要能讲，不只报全国平均。
- 方向性事实做成因子解释，而不是单点描述。建议拆解为：
  - **城市形态**：人口分布、地理上的梯度；
  - **路网**：大马路、选择性的多样性；
  - **Congestion**：population density、地形 / DEM、crossing；
  - 速度侧可接 Gaode speed indicators（作辅助测度，不替代主线百度速度）。
- “联系广度”单独标出，作为 directionality 之外需要补强的测度维度。

### 3. 模型评价、分析、反事实

#### 3.1 制图

- Pre / Tidal 地图改成以市中心为 base。
- 不画线、不画点—线；只画受影响的 grids。

#### 3.2 基准设定

- 继续写清：基准、参数选择、anchor 的方法。

#### 3.3 Counterfactuals

- **Symmetric**：是否需要图（level / %）与统计量一并回报；不要只留文字结论。
- **具体例子**：以深圳为例；keep fundamentals，改变 asymmetry。
- **开放问题**：5% 改的是效率，还是人口权重？（笔记记为 `v* − A_i` 相关对照。）
- **Sorting direction** 与 **externality decomposition** 并列推进。
- 政策对象可写成排序后的概率 / 开放量口径（after sorting），而不是只报均值政策冲击。
- 从模型到实践的链条：how to choose → 效果优化 → 效果对比；同时看 congestion GE change 与 sorting effect。

### 4. 深圳实地数据

- 问句保留为下一步设计问题：能不能利用两个时间断点，做更严谨的识别 / 反事实分析。
- 与现有彩田路多日开通断点、RDiT 设计衔接，不另起无关数据线。

### 5. Direct effect 与 sorting 分组（第二页）

#### 5.1 Direct effect（按是否走 tidal）

- 走 tidal：better-off。
- 不走 tidal：worse-off。
- 另看“走 tidal 的前面”（上游 / 前方路段使用者）。
- 全部改成时间 `t`，看百分比，而不是只报抽象福利指数。

#### 5.2 Sorting / incumbents

- 以前不用这条、现在用的人。
- 以前就在这边的 incumbents。
- 分组分别比较，报告每组的 `W`。

#### 5.3 Policy 前后四种人

| 组别 | 含义 | 备注 |
|------|------|------|
| Never taker | 前后都不走 | — |
| Always taker | 前后都走 | 这里面有 externality |
| 前用后不用 | 政策前走、政策后不走 | — |
| 后用前不用 | 政策前不走、政策后走 | 应是一个福利对象 `W` |

### 6. 表述与 congestion technology 细节

- 图中角标 / 下标统一写成 `\text{...}` 格式。
- Congestion technology：**式子不变，interpretation 改成 relative time / speed**。
- 核心关系写成：

$$
\left(\frac{t_{\mathrm{obs}}}{t_{\mathrm{ff}}}\right)^{\delta}
=
\left(\frac{E_{lke}}{n_{ke}}\right)^{1/\theta}
$$

- 左侧定义为 congestion index。
- 在 \(h_{\max}\) 处应有 \(t_{\mathrm{obs}}/t_{\mathrm{ff}}>1\)。
- 等价改写：

$$
t_{\mathrm{obs}}
=
t_{\mathrm{ff}}
\cdot
\left(\frac{E_{lke}}{n_{ke}}\right)^{1/(\delta\theta)}
$$

- 开放核对：\(\delta\) 与 \(\theta\) 的标定关系（笔记旁注涉及 \(\delta=1/\theta\) 一类约束）；落地前对照当前 slides / code 中 \(\lambda/\theta\) 主线，避免两套解释并存。

## 建议的近期执行顺序

1. Data 描述同步：最快绕过网格原则 + **centerline** 路网；检查是否与现有 check-policy / gate 口径一致。
2. Measures 改因子解释框架：城市形态 / 路网 / congestion（+ Gaode 辅助），职住分离与联系广度并排。
3. 模型图与 CF 展示：市中心 base、只画受影响 grids；symmetric 补 level/% 图与统计量；深圳 keep-fm / change-asy 例子。
4. Incidence 重做：direct effect 改 `t` 百分比；never / always / switch-in / switch-out 四组分别报 `W`；always taker 单独标 externality。
5. Congestion technology 只改 interpretation（relative time/speed）与下标 `\text` 格式；公式结构不动，先核对 \(\delta,\theta\) 与现有 \(\lambda/\theta\) 是否同一套。
6. 深圳实地：评估双时间断点能否支撑更严谨识别，接到现有视频 / RDiT 设计文档。

## 开放决策（会议未拍板）

- Symmetric 的 5% 是改效率还是改人口权重。
- 政策展示用均值冲击，还是 sorting 后的概率 / 开放量。
- Congestion index 中 \(\delta\) 与 \(\theta\) 是否施加显式关系。
- 深圳双断点最终服务 reduced-form identification，还是模型外 validation。
