# QSM 文献中 Travel Cost 对称性设定综述

> 本文档整理 urban economics / trade 领域的 Quantitative Spatial Models (QSM)（特别是 ARSW/AA 风格的文章）中，travel cost 的模型设定和估计数据，重点区分对称 vs. 非对称的处理方式，为"引入非对称速度"这一 contribution 做 positioning。
>
> 注：以下为 landscape-level 总结，具体到某篇文章的 appendix 细节建议核对原文。

---

## 一、Travel Cost 在 QSM 中的标准设定

自 Ahlfeldt, Redding, Sturm, Wolf (2015, ECMA, "ARSW") 以来，绝大多数 urban QSM 文献采用 iceberg-style 的指数形式设定双边通勤/贸易成本：

```
d_ij = exp(κ · t_ij)
```

其中 `t_ij` 是从 `i` 到 `j` 的通勤/运输时间，`κ` 是 semi-elasticity。在 trade 类 QSM（Allen–Arkolakis 系）中则常写成：

```
τ_ij = t_ij^δ
```

或类似的距离/时间幂函数形式。

**关键观察**：在绝大多数主流 QSM 文章中，模型设定上默认 `t_ij = t_ji`，即对称。这种对称性既来自数据约束，也来自模型存在性/唯一性的考虑（参见 Allen, Arkolakis, Li 2024 关于 spatial equilibrium 的 spectral conditions 的工作——非对称会让条件更严格）。

---

## 二、采用对称 Travel Cost 的代表性文章

这是绝大多数 ARSW-style 文章的做法：

| 文章 | 场景 | Travel time 数据来源 | 对称处理 |
|------|------|----------------------|----------|
| ARSW (2015, ECMA) | 柏林墙 | 柏林公共交通 + 步行网络的 GIS 最短路径 | 对称 |
| Monte, Redding, Rossi-Hansberg (2018, AER) | 美国 CZ | ACS 通勤流反推 commuting cost | 对称 |
| Heblich, Redding, Sturm (2020, QJE) | 维多利亚伦敦 | 历史铁路/步行网络 GIS | 对称 |
| Tsivanidis (2019, WP) | Bogotá TransMilenio | GTFS + Google Maps API | 对称 |
| Severen (2023, ReStat) | LA Metro Rail | Google Maps Directions API | 对称 |
| Allen & Arkolakis (2014, QJE) | Trade & topography | 地形（slope, water）计算 effective distance | 对称 |
| Owens, Rossi-Hansberg, Sarte (2020, AER) | Detroit 重建 | GIS 最短路径 | 对称 |
| Almagro & Domínguez-Iino (2024) | Amsterdam tourism | GTFS / OSM | 对称 |

**数据来源高度集中于：**

1. Google Maps / GTFS / OpenTripPlanner 给出的等时图；
2. 历史地图 + 时刻表（历史 QSM）；
3. 直接用调查通勤流反推；
4. OSM 最短路径。

即使底层 API（如 Google Maps）原则上能返回方向特异的 travel time，主流 QSM 文章也通常取一个方向或做双向平均后进入模型。

---

## 三、考虑非对称性的文献（相对少数）

真正在模型层面或测量层面强调方向性的文章不多，主要分两类：

### (a) 网络 / Route-choice 框架下原则上允许非对称

- **Allen & Arkolakis (2022, ECMA)** — "The Welfare Effects of Transportation Infrastructure Improvements in GE"。Route-choice 框架可以容纳 directional links，但实证输入的 link 成本仍主要是对称的。
- **Fajgelbaum & Schaal (2020, ECMA)** — "Optimal Transport Networks"。Network 上的 flow 是 directional 的，但 link 的物理 traversal cost 一般设为对称。
- **Brinkman & Lin (2024)** — 高速公路与城市结构。

### (b) 关注 congestion、显式测量方向性速度

- **Couture, Duranton, Turner (2018, ReStat) "Speed"** — 用 Google Maps API 测量美国大都市区的小时级、方向级速度，证实 peak-direction 的非对称性显著。主要是 measurement，非 full QSM。
- **Akbar, Couture, Duranton, Storeygard (2023, AER) "Mobility and Congestion in Urban India"** — 系统性地用 Google Maps 在不同时间段、不同方向 query 印度城市的 travel time，明确捕捉 directional asymmetry。
- **Kreindler (2024, ECMA) "Peak-Hour Road Congestion Pricing in Bangalore"** — 司机 GPS 数据，方向和时段特定的 travel time。
- **Akbar (2024, WP)** — 进一步把方向性速度数据接入 QSM。
- **Barwick, Donaldson, Li, Lin, Rao 等关于中国城市拥堵的研究** — 部分使用了百度/高德的方向性数据。

### 非对称 Travel Time 数据来源

1. **Google Maps Directions API**：分别 query A→B 和 B→A，并按 `departure_time` 取不同时段。目前最主流。
2. **Uber Movement / 网约车数据**：天然 directional + time-stamped。
3. **TomTom / INRIX / HERE** 商业实时路况数据。
4. **司机 GPS 轨迹**（Kreindler 类）。
5. **国内的高德 / 百度 API**。
6. **历史时刻表 / 单向道路网 GIS**（特定历史或城市场景）。

---

## 四、非对称性的现实来源

- 通勤潮汐（morning peak inbound vs. outbound）
- 单行道与 turn restrictions
- 地形（上下坡速度不同）
- 路网拓扑与 ramp 结构（特别是 freeway on/off-ramp）
- 公共交通班次方向差异
- 历史道路修建顺序造成的拓扑 asymmetry

---

## 五、对 Contribution 的 Positioning 建议

基于上面的 landscape：

> 主流 QSM（ARSW-style）出于 tractability 和 uniqueness 的考虑，几乎清一色采用对称 travel cost；少数关注 congestion 的文章（Couture-Duranton-Turner, Akbar et al., Kreindler 等）在 measurement 层面记录了方向性差异，但要么不在 full QSM 框架里，要么只把它当作 reduced-form 的 input。真正在 GE QSM 中系统性地把非对称速度作为模型与实证设定的核心维度，仍是空白或刚起步。

### 写 contribution 时可强调三点

1. **数据创新**：用什么数据让 directional asymmetry 可以被精确测度。
2. **方法贡献**：在保留 ARSW/MRR 的可解性条件下，如何 accommodate 非对称。可引 Allen-Arkolakis-Li (2024) 讨论 uniqueness 条件，说明自己的设定如何满足。
3. **经济意义**：因为通勤潮汐、单行道、地形、路网结构，非对称在现实中量级很大；忽略它会系统性偏误 welfare / policy counterfactual 的某个方向。最好给一个 quantitative magnitude。

---

## 六、待核对清单（To-Do）

- [ ] 核对每篇 paper 的 appendix 对 travel time 是否做了 symmetrization（平均、取 min、取 max 等）
- [ ] 核对 Akbar (2024) 最新版是否已经把 asymmetric speed 放进 structural model
- [ ] 检查 Allen & Arkolakis (2022) appendix 中 route-choice 是否 allow `c_ij ≠ c_ji`
- [ ] 核对国内文献（Barwick 等）的数据处理细节
- [ ] 补充历史 QSM 文章（如 Donaldson 2018 "Railroads of the Raj"）对 directional shipping cost 的处理

---

*本文档为 literature review 工作笔记，可随阅读进度迭代更新。*
