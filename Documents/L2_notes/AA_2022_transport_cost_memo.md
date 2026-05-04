# Allen & Arkolakis (2022) Transportation Cost $t_{kl}$：Framework & Measurement
**备忘录 | Memo | 2026年4月18日**

---

## 一、$t_{kl}$ 的定义：Iceberg Convention 中的无量纲成本

### 原始定义（§2, p.2915）
在 AA 的框架中，$N \times N$ 矩阵 $\mathbf{T} = [t_{kl} \ge 1]$ 代表运输网络，其中 $t_{kl}$ 是：

> **(ad valorem) cost incurred from moving directly from $k$ to $l$ along a link**

"transportation costs are treated as **ad valorem (iceberg)**"（脚注 3, p.2915）

**关键性质：** $t_{kl} \ge 1$ 是**无量纲的成本因子**
- 不是分钟、不是小时、不是公里
- 是"传递一单位效用/一单位货物时，因穿过链路 $(k,l)$ 而损耗的比例"
- Follow spatial literature's iceberg convention

### 路径成本的构造（Route Cost）

路线成本是沿途 $t_{kl}$ 的**连乘**（multiplicative）：
$$\prod_{l=1}^{K} t_{r_{l-1}, r_l}$$

"Because iceberg costs are multiplicative, the total costs incurred from moving from $i$ to $j$ along route $r$ of length $K$ is then $\prod_{l=1}^K t_{r_{l-1}, r_l}$"（p.2915）

### Urban 模型中 $t_{kl}$ 进入效用函数的方式（§2.2.1, p.2919）

未编号方程（在 eq. 12 之前）：
$$V_{ij,r}(\nu) = \left( u_i w_j \bigg/ \prod_{l=1}^K t_{r_{l-1}, r_l} \right) \times \varepsilon_{ij,r}(\nu)$$

**解释：** 
- Utility 由 $u_i w_j$ 的 iceberg-deflated 值决定
- $\prod t_{r_{l-1}, r_l}$ 出现在分母，作为纯无量纲缩水因子

---

## 二、从数据测量 $t_{kl}$：Structural Approach（§5.3, pp.2933–2934）

AA **不直接观测** $t_{kl}$，而是通过微观基础把 iceberg 成本与可观测的速度、距离、车道数连接。

### 第一步：结构假设——Iceberg 成本是 Travel Time 的幂函数（Eq. 40）

$$t_{kl} = \left( \text{distance}_{kl} \times \text{speed}_{kl}^{-1} \right)^{\delta_0}$$

其中：
- $\delta_0$ = "time elasticity of transportation cost"
- **Preferred specification:** $\delta_0 = 1/\theta$（p.2933）
  - 这个选择确保"distance elasticity = –1"，与 gravity 文献一致
  - 这是 unit-invariant 的关键选择（见下文第三部分）

### 第二步：Inverse Speed 是拥堵的幂函数（Vickrey 1967, Eq. 41）

$$\text{speed}_{kl}^{-1} = m_0 \times \left( \frac{\Xi_{kl}}{\text{lanes}_{kl}} \right)^{\delta_1} \times \varepsilon_{kl}$$

其中：
- $\Xi_{kl}$ = link traffic
  - **Urban 模型中用 commuters 数量衡量**（p.2924）
  - "in the urban model, traffic is measured in the quantity of commuters flowing over a link"
- $m_0$ = 无拥堵时的平均 flow rate
- $\text{lanes}_{kl}$ = 车道数
- $\varepsilon_{kl}$ = 随机扰动

### 第三步：Reduced Form（合并 Eq. 40 和 41, p.2933）

$$t_{kl} = \bar{t}_{kl} \times (\Xi_{kl})^\lambda$$

其中：
$$\bar{t}_{kl} \equiv \text{lanes}_{kl}^{-\delta_0 \delta_1} \times \left( \text{distance}_{kl} \times m_0 \times \varepsilon_{kl} \right)^{\delta_0}$$

$$\lambda \equiv \delta_0 \delta_1$$

**解释：** 
- $\bar{t}_{kl}$（基础部分）由距离、车道、自由流速完全决定
- $(\Xi_{kl})^\lambda$（拥堵部分）是内生交通量的函数

### 第四步：Estimating Equation（Eq. 43, p.2933）

$$\ln \text{speed}_{kl}^{-1} = \ln m_0 + \delta_1 \ln\left( \frac{\Xi_{kl}}{\text{lanes}_{kl}} \right) + \ln \varepsilon_{kl}$$

**回归策略：**
- 因变量：$\ln(\text{inverse speed})$
- 自变量：$\ln(\text{traffic-per-lane})$
- 固定效应：start-location and end-location
- **方法：Instrumental Variables**（OLS 因反向因果向下偏）

**工具变量：**
- Seattle 案例：turns & intersections 数（p.2940）
- US highway 案例：distance（Table 1, p.2940）

**估计结果：**
- US 州际公路：$\delta_1 \approx 0.739$
- Seattle：$\delta_1 \approx 0.488$

### 数据来源（§6.1.1, p.2935; §6.2 for Seattle）

| 变量 | 数据源 |
|------|--------|
| 速度 $\text{speed}_{kl}$ | HERE API via `georoute` Stata command (Weber & Péclat 2017) |
| 交通量 $\Xi_{kl}$ | HPMS AADT（Highway Performance Monitoring System 年日均交通量） |
| 车道数 $\text{lanes}_{kl}$ | HPMS |
| 距离 $\text{distance}_{kl}$ | 网络几何构造 |

---

## 三、单位问题的解决：四层 Isolation Strategy

这是与 "dimensional problem" 批评的核心回应。AA 通过**结构性设计**而非 ad hoc 假设彻底清理单位问题。

### 第 1 层：Iceberg Convention 本身

- $t_{kl} \ge 1$ 是 ad-valorem loss 因子
- **本质上是无量纲的**（dimensionless）
- 与"time in minutes"属于不同 domain
- AA 在 §2 和脚注 3 明确选择 iceberg 传统

→ **$t_{kl}$ 没有物理单位**

### 第 2 层：$\delta_0 = 1/\theta$ 的 Preferred Calibration

公式 $t_{kl} = (\text{time}_{kl})^{\delta_0}$ 中，time 有量纲。但：

> "In our preferred results below, we set $\delta_0 = 1/\theta$ to imply a 'distance elasticity' of negative one, which is consistent with a large gravity literature."（p.2933）

**关键好处：**
- 在 Fréchet route-choice 下，kernel 为 $t_{kl}^{-\theta}$
- 代入得：$t_{kl}^{-\theta} = (\text{time}_{kl})^{-\theta \delta_0} = (\text{time}_{kl})^{-1}$
- 而模型中**$t$ 只通过 $t^{-\theta}$ 出现**

**单位 invariance 的推论：**
- 改变时间单位（minute → hour）意为 $\text{time}_{kl} \to c \cdot \text{time}_{kl}$（$c$ 为常数）
- 则 $t_{kl}^{-\theta} = (c \cdot \text{time}_{kl})^{-1} = c^{-1} \cdot \text{time}_{kl}^{-1}$
- $c^{-1}$ 对所有 link 相同，被吸收进 welfare normalization $\chi$
- **均衡量不变：** $\{y_i, l_i, \pi_{ij}, \Xi_{kl}\}$ 完全不受影响

### 第 3 层：估计过程的 Log-Log 形式

Estimating equation (43) 是对数-对数回归：
$$\ln \text{speed}^{-1} \text{ vs. } \ln(\Xi/\text{lanes})$$

**关键性质：**
- 斜率 $\delta_1$ **完全不依赖于速度的单位**（mph vs. km/h）
- 单位改变只影响截距 $\ln m_0$
- 估计得到的结构参数 $\lambda = \delta_0 \delta_1$ 是 log-log 斜率的乘积
- **$\lambda$ 对单位 invariant**

### 第 4 层：Page 2925 的关键论述

> "retaining the same units for traffic and bilateral flows — along with the assumed log-linear congestion relationship in equation (25) — ensures that the transportation costs between origin and destination remain ad-valorem in the presence of traffic congestion, that is, **our framework follows the large literature focusing on so-called iceberg transportation costs**."（p.2925）

**含义：**
- 只要把 traffic $\Xi$ 和 bilateral flow 用同一单位表达
- 再把 congestion 技术写成 log-linear
- 就自动保证 $t_{kl}$ 保持 iceberg/dimensionless
- **单位问题被结构性消解**，而非 ad hoc 解释

---

## 四、对当前 Tex 中 Remark 1.2 的批评与建议

### 现有问题

你的 Remark 1.2（"Log-linear transport cost in time"）：

> "we adopt the convention $\delta_0 = 1$, so that $\theta$ itself is the relevant dispersion parameter and $t_{kl}$ is interpreted directly as equilibrium travel time in minutes."

**在 AA convention 下存在两个问题：**

#### 问题 1：$\delta_0 = 1$ vs. AA 的 Preferred $\delta_0 = 1/\theta$

- AA 选 $\delta_0 = 1/\theta$ 不是随意选择，而是为了：
  - 让 $t_{kl}^{-\theta} = \text{time}_{kl}^{-1}$（gravity literature 的 distance elasticity = –1）
  - 获得单位 invariance（见第三部分第 2 层）
  
- 你把 $\delta_0$ 设成 1 后：
  - $t_{kl}^{-\theta} = \text{time}_{kl}^{-\theta}$
  - 失去了单位 invariance 的巧妙设计
  - 需要更深的辩护

#### 问题 2："$t_{kl}$ is interpreted directly as equilibrium travel time in minutes" 与 Iceberg Convention 不兼容

- AA 的 iceberg 要求 $t_{kl} \ge 1$，无量纲
- "travel time in minutes" 会让 $t_{kl}$ 取值任意大（如 30 分钟）
- 这不再是 iceberg，而是有量纲的加性成本
- **"dimensional problem" 的批评在这一点是站得住脚的**

### 建议的修法方向

**方案 A：回到 AA 的 Iceberg Convention**
- 让 $t_{kl} = (\text{time}_{kl})^{1/\theta}$ 是 iceberg、无量纲
- 路线成本：$\tau_{ij} = \left( \sum_r \prod_l t_{r_{l-1}, r_l}^{-\theta} \right)^{-1/\theta}$ 也无量纲
- 在 Remark 中改述为："$t_{kl}^{-\theta} \propto 1/\text{travel time}_{kl}$"
- 这正是 AA p.2933 preferred specification 的语言

**方案 B：采用加性成本 Variant**
- 如 AA 的 Supplementary Appendix D.1
- 但需要声明这是对原始 iceberg 框架的替代
- 修正后续所有 aggregation：
  - 路线成本从 $\prod t_{kl}$ 变成 $\sum t_{kl}$（加性）
  - Katz-Bonacich 推导需要重新调整
  - 所有关于单位 invariance 的论述需要修改

### 已识别的其他符号错误

需要一并修正：
- **第 572 行：** $n_{kl}^{\lambda/(1+\theta\lambda)}$ 应为 $n_{kl}^{-\lambda/(1+\theta\lambda)}$（符号）
- **第 591 行：** $\partial \ln t_{kl} / \partial \ln n_{kl}$ 的符号问题

---

## 参考资源

**Primary Source:**
- Allen, T., & Arkolakis, C. (2022). "Trade and the Topography of the Spatial Economy." *Review of Economic Studies*, 89(5), 2911–2957.
  - §2 (p.2915)：$t_{kl}$ 的定义和 iceberg convention（脚注 3）
  - §2.2.1 (p.2919)：Utility 函数形式
  - §3.1–3.3 (pp.2922–2925)：Urban 模型的均衡条件和拥堵关系（Eq. 21–27）
  - §5.3 (pp.2933–2934)：Measurement framework（Eq. 40–43）
  - §6.1.1 (p.2935)；§6.2：Data sources
  - Table 1 (p.2940)：估计结果

**Supplementary Materials:**
- AA Supplementary Appendix D.1：Additive (非 Iceberg) 成本的 Variant

---

## 核心要点总结

| 维度 | 关键事实 |
|------|---------|
| **定义** | $t_{kl}$ 是 iceberg 成本，无量纲，$t_{kl} \ge 1$ |
| **路线成本** | 连乘：$\prod_{l=1}^K t_{r_{l-1}, r_l}$ |
| **测量** | 通过 inverse speed 对 traffic-per-lane 的 IV 回归 |
| **结构** | $t_{kl} = \bar{t}_{kl} \times (\Xi_{kl})^\lambda$，其中 $\lambda = \delta_0 \delta_1$ |
| **单位处理** | 通过 $\delta_0 = 1/\theta$ 和 log-log 估计保证 unit invariance |
| **与你的模型的冲突** | Remark 1.2 的 $\delta_0 = 1$ 和"travel time in minutes"与 AA convention 不兼容 |

---

**Documentation Date:** 2026年4月18日  
**Status:** Reference memo for AA (2022) framework cross-check
