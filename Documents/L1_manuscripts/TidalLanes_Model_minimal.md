# TidalLanes 模型 · 最简化说明

基于 `TidalLanes_Model_v4.tex`。只保留必要公式、公式说明、equilibrium 定义、求解说明。

---

## 0. 设定与记号

- 城市分为 $N$ 个网格 $\mathcal N=\{1,\dots,N\}$，有向图 $(\mathcal N,\mathcal E)$。有向边 $(k,l)$ 与 $(l,k)$ 是不同对象，所有 primitive 都允许方向不对称。
- 每条有向边的 primitive：长度 $\ell_{kl}$、车道容量 $n_{kl}$、自由流速度 $v^0_{kl}$，自由流时间 $\bar t_{kl}\equiv\ell_{kl}/v^0_{kl}$。
- 总通勤人口 $\bar L$，分布在居住地 $L_i^R$ 与工作地 $L_j^F$。
- 参数：$\theta>0$（Fréchet 离散度）、$\alpha>0$（生产集聚弹性）、$\beta<0$（居住拥挤弹性）、$\lambda\ge0$（速度对单车道流量的拥堵弹性）。

---

## 1. 必要公式

**(1) 有向 iceberg 链路成本 / 拥堵技术**

$$
t_{kl}=\big(\ell_{kl}/v_{kl}\big)^{1/\theta},\qquad
v_{kl}=v^0_{kl}\big(n_{kl}/\Xi_{kl}\big)^{\lambda}
$$

链路实现速度 $v_{kl}$ 随单车道流量 $\Xi_{kl}/n_{kl}$ 上升而下降；$t_{kl}\ge1$ 是无量纲 iceberg 成本，乘法进入效用，核为 $t_{kl}^{-\theta}$。物理分钟只通过 $v^0_{kl},v_{kl}$ 进入。等价写法：$t_{kl}=\bar t_{kl}^{1/\theta}(\Xi_{kl}/n_{kl})^{\lambda/\theta}$。

**(2) 双边通勤成本指数（路径加总）**

$$
\tau_{ij}^{-\theta}=\sum_{r\in\mathcal R_{ij}}\prod_m t_{r_{m-1},r_m}^{-\theta}
=\big[(I-A)^{-1}\big]_{ij},\qquad A_{kl}=t_{kl}^{-\theta}\mathbf 1\{(k,l)\in\mathcal E\}
$$

对所有可行路径求和，得到 $i\to j$ 的有效成本 $\tau_{ij}$。因方向性，一般 $\tau_{ij}\ne\tau_{ji}$。级数收敛需谱半径条件 $\rho(A)<1$。

**(3) 基本面（内生 amenity / 生产率）与工资**

$$
u_i=\bar u_i (L_i^R)^{\beta},\qquad A_j=\bar A_j (L_j^F)^{\alpha},\qquad w_j=A_j
$$

$\bar u_i,\bar A_j$ 是 baseline 基本面；规模通过 $\beta<0$（拥挤）和 $\alpha>0$（集聚）反馈。完全竞争下工资等于生产率。

**(4) 福利标量与引力流**

$$
\bar W=\Big[\sum_{i,j}\tau_{ij}^{-\theta}u_i^{\theta}w_j^{\theta}\Big]^{1/\theta},\qquad
L_{ij}=\bar L\,\frac{\tau_{ij}^{-\theta}u_i^{\theta}w_j^{\theta}}{\bar W^{\theta}}
$$

$\bar W$ 是城市代表性居民的确定性福利指数；$L_{ij}$ 是引力型通勤流，$L_i^R=\sum_j L_{ij}$，$L_j^F=\sum_i L_{ij}$。

**(5) Market access 与链路流量**

$$
\Pi_i^{-\theta}=\sum_j\tau_{ij}^{-\theta}L_j^F P_j^{\theta},\qquad
P_j^{-\theta}=\sum_i\tau_{ij}^{-\theta}L_i^R \Pi_i^{\theta},\qquad
\Xi_{kl}=t_{kl}^{-\theta}P_k^{-\theta}\Pi_l^{-\theta}
$$

$\Pi_i,P_j$ 是居住地/工作地 market access；因方向性一般 $\Pi_i\ne P_i$。$\Xi_{kl}$ 是边 $(k,l)$ 的期望通行量，闭合了拥堵技术。

**(6) 人口份额不动点**（$l_i^R=L_i^R/\bar L,\ l_j^F=L_j^F/\bar L$）

$$
(l_i^R)^{1-\beta\theta}=\chi\sum_j\tau_{ij}^{-\theta}\bar u_i^{\theta}\bar A_j^{\theta}(l_j^F)^{\alpha\theta},\qquad
(l_j^F)^{1-\alpha\theta}=\chi\sum_i\tau_{ij}^{-\theta}\bar u_i^{\theta}\bar A_j^{\theta}(l_i^R)^{\beta\theta}
$$

标量 $\chi\equiv\bar L^{\theta(\alpha+\beta)}/\bar W^{\theta}$ 由归一化 $\prod_i\bar u_i=\prod_j\bar A_j=1$ 定钉。这是 (1)–(5) 代入后的等价紧凑系统。

---

## 2. Equilibrium 定义

给定 primitive $\{(\bar u_i,\bar A_j)\}$、$\{(\ell_{kl},v^0_{kl},n_{kl})\}$、参数 $(\theta,\alpha,\beta,\lambda)$、人口 $\bar L$，均衡是满足以下条件的内生量
$\mathcal Q=(\{L_i^R\},\{L_j^F\},\{L_{ij}\},\{t_{kl}\},\{\tau_{ij}\},\{\Xi_{kl}\},\{\Pi_i\},\{P_j\},\bar W)$：

1. **双边成本指数** — 公式 (2)。
2. **引力流** — 公式 (4)，$u_i,w_j$ 由 (3) 给定。
3. **市场出清** — $L_i^R=\sum_j L_{ij}$，$L_j^F=\sum_i L_{ij}$，$\sum_i L_i^R=\sum_j L_j^F=\bar L$。
4. **Market access** — 公式 (5) 的 $(\Pi_i,P_j)$。
5. **链路流量** — $\Xi_{kl}=t_{kl}^{-\theta}P_k^{-\theta}\Pi_l^{-\theta}$。
6. **拥堵技术** — $t_{kl}=\bar t_{kl}^{1/\theta}(\Xi_{kl}/n_{kl})^{\lambda/\theta}$。
7. **谱半径条件** — $\rho(A)<1$，保证 (2) 收敛。

等价地，份额 $(l^R,l^F)$ 满足不动点系统 (6) 联立拥堵技术。

**福利表征**（任一均衡成立）：

$$
\bar W^{\theta}=\sum_i u_i^{\theta}\Pi_i^{-\theta}=\sum_j w_j^{\theta}P_j^{-\theta}
$$

反事实福利变化由 exact-hat 给出，报告为时间等价变动 $\hat{\mathbb W}$。

---

## 3. 求解说明（五步流水线）

**Step 1 — 构造数据输入.** 有向 primitive $(\ell_{kl},v^0_{kl},v^{obs}_{kl},n_{kl})$ 与人口 $(L_{ij}^{obs},L_i^{R,obs},L_j^{F,obs},\bar L)$。

**Step 2 — 外部校准 $(\theta,\alpha,\beta)$.** 沿用 AA(2022) Seattle / Ahlfeldt(2015)：$\theta=6.83,\ \alpha=-0.12,\ \beta=-0.10$。

**Step 3 — 拥堵弹性 $\lambda$.** 目标方程
$$\ln v^{obs}_{kl}=\ln v^0_{kl}-\lambda\ln(\hat\Xi_{kl}/n_{kl})+\eta_{kl}+\varepsilon_{kl}$$
（面板版加链路 FE $\mu_e$ 与时间 FE $\rho_t$；理想用 IV 处理流量内生）。当前数据无实测有向流量，改用两个 reduced-form 替代：(a) 由 AA 的 $\delta_1=0.488$ 换算 $\lambda=\delta_1/\theta$（baseline）；(b) 截面诊断回归作弱校准检查。

**Step 4 — 反演基本面 $(\bar u_i,\bar A_j)$.** 假设观测配置即均衡，在数据上对 (6) 取
$$(l_i^{R,obs})^{1-\beta\theta}=\chi\sum_j(\tau_{ij}^{obs})^{-\theta}\bar u_i^{\theta}\bar A_j^{\theta}(l_j^{F,obs})^{\alpha\theta}$$
及其对称式，用交替更新 $\bar u_i^{\theta},\bar A_j^{\theta}$（对两个几何均值归一化）求解。输出 $(\bar u_i^\star,\bar A_j^\star)$，在反事实中固定。

**Step 5 — Exact-hat 反事实.** 输入成本比矩阵 $\hat{\bar t}_{ij}=\tau_{ij}^{new}/\tau_{ij}^{obs}$；AA exact-hat solver 以比值（而非水平）重算均衡，固定基本面，对 $(\hat l_R,\hat l_F)$ 和尺度项 $\hat\chi$ 迭代至收敛。报告 $\hat\chi$、$(\hat l_R,\hat l_F)$、成本与流量变化；此归一化下 $\hat\chi<1$ 对应福利改善。

**潮汐车道渠道.** 在目标集 $\mathcal E^\star$ 上 $n_{kl}^{cf}=n_{kl}+\Delta_{kl}$，$n_{lk}^{cf}=n_{lk}-\Delta_{kl}$，保持走廊总容量不变；$t_{kl}$ 降、$t_{lk}$ 升。无向模型下该政策失效。

**反事实情景.** CF1 对称 vs 有向成本；CF2 潮汐车道再分配；CF3 统一扩容 $\{1.10,1.25,1.50\}$ 作基准。

**$\theta$ 内部稳健性.** 对 $\theta\in\{3,\dots,12\}$ 重算 $\tau_{ij}(\theta)$ 跑双向 FE 引力回归 $\ln L_{ij}^{obs}=-\theta\ln\tau_{ij}(\theta)+\mu_i+\nu_j+\varepsilon_{ij}$，选回归斜率等于所设 $\theta$ 的点；外部校准仍为 baseline。
