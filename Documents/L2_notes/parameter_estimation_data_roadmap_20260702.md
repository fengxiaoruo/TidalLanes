# Parameter estimation — Beijing intuition and data roadmap

**Date:** 2026-07-02  
**Context:** Theta sensitivity grid completed (`theta_sensitivity_gate5_bpr_pos05_v1`); slides appendix `appsens_theta` updated. This note records (i) Beijing θ intuition and (ii) what data/methods are needed to move beyond calibration for other parameters flagged in `nextstep.md`.

---

## 1. Theta (Fréchet / gravity exponent)

### Current practice

- **Baseline:** θ = 6.83 from Allen and Arkolakis (2022) Seattle / Ahlfeldt et al. (2015).
- **Robustness:** `run_theta_sensitivity_gate5.py` on θ ∈ {3, 5, 6.83, 8, 10}; each θ re-inverts fundamentals and re-solves baseline + tidal top-100 ST/LT.
- **Outputs:** `data_work/outputs/theta_sensitivity_gate5_bpr_pos05_v1/`.

### Beijing intuition (honest, not a point estimate)

| Source | Implied θ | Credibility |
|--------|-----------|-------------|
| Early square prototype: grid search on `ln L_ij ~ −θ ln τ_ij` with τ depending on θ | **θ̂ ≈ 0.6–0.7** | **Low** — τ is an equilibrium object; diagnostic is not model-consistent |
| External AA Seattle calibration | **6.83** | **Medium** — cross-city prior, not Beijing-specific |
| Theta grid (6.83–10) | Policy **signs stable**; W̄ Δ% magnitudes fall with θ | **Medium** — supports using 6.83 as a defensible middle of the robust band |
| Beijing features (peak congestion, minute-level heterogeneity, car-heavy sample in model) | Speculative **7–9** more plausible than **≪ 6.83** | **Weak** — judgment only |

**Working conclusion:** Do **not** use prototype 0.6–0.7 as Beijing θ. Keep **6.83 as baseline + appendix grid as robustness** until a Beijing-specific estimate is done with model-consistent τ and bilateral OD.

### Low-θ route-cost cells († on slides)

- At θ = 3, 5: equilibrium **converges**; W̄ ST/LT Δ% and sorting are interpretable.
- **LT route (s)** and person-min/day from OD cost **decomposition** can be pathological (e.g. −702 s at θ = 3) while LT W̄ is only +0.75%.
- **Not** “commutes got 11 minutes faster”; the accounting split breaks at low θ.
- **Speaker note:** use W̄ Δ% and sorting columns for low θ; do not read route cells literally.

### What is needed for a serious Beijing θ estimate

**Data**

- Grid bilateral **OD flows** (road-user harmonized; car-only or car+bus robustness).
- **Model-consistent bilateral costs** τ_ij(θ) from the locked network (edgeproj gate5 or exogenous-shock variants).
- **Instruments / exclusion** where possible: historical network, ring/subway openings, Redding–Sturm-style IV.

**Methods**

- PPML gravity: `L_ij ~ FE_i + FE_j − θ log τ_ij` with τ recomputed per candidate θ; or
- Full grid as in `run_theta_sensitivity_gate5.py` (re-invert + GE per θ), selecting θ by fit + economic plausibility.
- Compare to external 6.83; do not replace baseline unless internal and external estimates align.

**Reference:** `Documents/L2_notes/TidalLanes_estimation_notes.tex` §Fréchet shape θ; `stage1_od_fit_diagnosis_and_handoff_20260628.md` (Beijing OD θ TODO).

---

## 2. λ_phys (congestion elasticity; mainline 1.1)

**2026-10-07 与 Brad 会后更新：** 后续重点比较更多 congestion technology 设定及其结果，可参考他的 congestion pricing 文章；先核对原文，再整理候选版本和比较方案。见 [会议记录](meeting_notes_20261007_brad.md)。本次未确定替换主线函数或 λ 的取值。

### Current practice

- **Mainline:** λ_phys = 1.1 (Beijing speed–density / Barwick-style calibration), anchored congestion form.
- **Robustness:** λ = 0.57 legacy band; cross-section `lambda_diagnostic` is **diagnostic only**, does not override mainline.
- **Gap:** No observed **directed link traffic** in baseline; model Ξ is calibration/diagnostic input.

### What is needed

**Data**

- **Directed link flows** (AADT, license-plate counts, probe volume) × **directed speeds** panel.
- Baidu **speeds** exist; **q/k or Ξ** on grid edges is the binding constraint.

**Methods**

- Panel: `log τ_kl = FE + λ_phys log(q_kl / n_kl) + …` or BPR linearization.
- **IV:** weather, driving restrictions, policy timing, upstream demand (Yang et al. 2020; Barwick et al. 2024 China settings).
- Reconcile with anchored mainline: does estimated λ cluster near 1.1? Does **free-flow definition** (nextstep #2) move tails and CF ranking?

**Reference:** `TidalLanes_estimation_notes.tex` §Congestion elasticity; `data_work/docs/CONGESTION_ELASTICITY_PARAMETER_MAPPING.md`.

---

### 2026-10-06 补充：Brad Ross 工作对拥堵参数估计的提示

**核心启发：** 拥堵弹性可以随负荷变化；深圳视频可用于估计方向性的流量与密度关系，并检验容量是否随车道数成比例变化。这是一条待落实的估计路线，不改变当前主线 λ_phys = 1.1。

**来源与状态：** 以下记录用户于 2026-10-06 提供的 Brad Ross 工作解读，论文简称为 *Road Pricing* 与 *Traffic Externalities*。本次仅整理留存，未独立核对原文。论文方法、图号、样本量及作者评价均按用户提供内容记录；读图数值与弹性换算另行标明。[TODO: 补全两篇论文的正式题名、作者、版本日期和原文链接，并逐项核对。]

#### Road Pricing：按道路类型估计流量与密度曲线

据所提供解读，两篇论文均未给出可直接替代本项目 λ 的单一弹性参数。*Road Pricing* 的设定为：

- 数据来自 Ayalon 高速的 24 个线圈检测器，覆盖 2020–2022 年工作日，每 5 分钟一条，约 120 万条观测。
- 按“限速 70 或 90 km/h × 普通路段或桥梁隧道”分为四类；每类用 20 阶 Bernstein 多项式估计每车道流量与密度曲线。
- 施加三个约束：曲线凹、零密度时流量为零、原点斜率等于限速。其他限速的曲线在 70 和 90 km/h 之间线性插值或向外外推。
- 按车道数缩放。若 ρ 是全断面每公里车辆数、L 是车道数，则总流量为：

  $$f_e(\rho;L)=L f^{\mathrm{lane}}(\rho/L).$$

**用户从 Figure 9 和 F2 粗读的量级：** 每车道通行能力约 1,600–1,700 辆/小时，临界密度约 30–40 辆/公里/车道；限速向 50–120 km/h 外推后，每车道通行能力约为 1,600–1,870 辆/小时。这些值是读图近似，不是已核实的论文参数表。

固定路段长度，以 q 表示流量、v 表示速度、T 表示通行时间，用户给出的两段读图换算可记为 Δln T/Δln q = −Δln v/Δln q：

| 每车道密度变化（辆/公里） | 每车道流量变化（辆/小时） | 速度变化（km/h） | 区间弹性粗估 |
|---|---|---|---|
| 10 → 20 | 800 → 1,350 | 80 → 67 | 约 0.3（按列示取整值计算约 0.34） |
| 20 → 30 | 1,350 → 1,650 | 67 → 55 | 约 1（按列示取整值计算约 0.98） |

这些是区间弹性近似，不能当作论文直接估计的常数 λ。在流量随密度上升的分支上，若 q=f(ρ)、v=q/ρ，则局部通行时间对流量的弹性为 f(ρ)/[ρf′(ρ)]−1；在正流量的光滑容量点，从该分支趋近且 f′(ρ)→0⁺ 时，弹性趋于无穷。拥堵后的下降分支不能直接沿用同一单值时间与流量关系。

**与本项目的联系：** 用户的解读是 λ=1.1 大致对应曲线中较接近饱和的区段；这一对应关系仍需原图和运行点支持。六月笔记 [BPR 拥堵形式](bpr_congestion_form_20260626.md) 中的 b≈1.1 建议也强调负荷不同、弹性不同。不过，BPR 的指数 b 与有限负荷下的局部弹性不同：若 T=T_ff[1+a x^b]，则弹性为 abx^b/(1+ax^b)。因此，b≈1.1 不能直接解释为该运行点的局部弹性等于 1.1，也不能复现容量点处的发散。还需区分时间对流量、时间对密度，以及模型流量 Ξ 的口径。

#### Traffic Externalities：月度平均弹性

据所提供解读，该文采用 Greenshields 形式的线性速度与密度关系 V=a+bD，斜率 b 随限速变化，并用工具变量估计。这里的 b 是速度与密度关系的斜率，与 BPR 指数 b 不是同一参数。

用户记录的正文汇总量是“路段通行时间对月度车流的弹性，中位数约 0.11%（图上单位是百分比）”；作者称其整体偏小，但大于 Yang et al.（2020）的北京估计。[TODO: 核对图轴、弹性定义、百分比换算及上述文献比较；核实前保留原记录，不把 0.11% 擅自改写为 0.11 或用于校准。] 月度流量混合各时段，不能直接与高峰 λ 比较。

#### 深圳视频：候选估计与车道缩放检验

1. 按 5 分钟提取各方向流量与匹配的区间通行时间，保留路段长度、有效车道数及稳定运行状态。先以 v=路段长度/通行时间构造速度；在流量与空间平均速度覆盖同一路段、同一时间窗且测量口径相容时，再由 ρ=q/v 构造密度，并用 q/L、ρ/L 得到每车道散点。
2. 将凹性、零密度零流量和原点斜率约束作为候选估计方法。原点斜率是否可直接设为深圳道路限速，需要结合当地自由流速度和信号控制核查；20 阶多项式是否适合现有样本，也需按支持域和拟合稳定性判断。
3. 用加车道前后稳定状态检验 f(ρ;L)=L f^lane(ρ/L)：在可比密度下，每车道曲线是否重合；若样本覆盖容量附近，再判断总容量是否按车道数同比增加。观察到的流量变化本身不等于容量变化，还受需求、信号、排队和车辆组成影响。
4. 将检验结果用于约束模型中 n_kl 的容量作用，报告不同负荷下的局部弹性，而非只追求一个常数 λ。该路线与既有 [彩田路视频识别方案](../../data_work/docs/CAITIAN_VIDEO_IDENTIFICATION_PLAN_20260715.md) 衔接，当前记录不代表估计已完成。

---

### 2026-10-06 补充：Speed 的“交通状态”工具变量与现有手机数据

**数据条件已更新：用户确认现在已有手机数据，可提供更准确的 OD 和出发时间。** 因此，出发时间分布不再记为“等待 GPS 数据到位”的缺口。下一步是核对数据字段、日期覆盖、交通方式和扩样权重，并构造按出发时段划分的 OD。用户尚未提供数据路径；目前不能据此认定已有完整车辆轨迹、真实路段流量或已知渗透率。

**来源与状态：** 本节留存用户提供的 Speed 研究设想，并补充估计口径上的区分；尚未实施或核对所援引论文。用户描述 Speed 已有北京约 640 万对居住地与工作地 OD、参考路速下的 Q_sp，以及道路 × 时钟和城市 × 日期 × 半小时固定效应；潮汐车道项目另有百度 2022 年网格 OD。这些属于用户提供的背景，本次未检查 Speed 文件。[TODO: 补充 Speed 项目与手机数据路径、变量字典和日期覆盖；核对文献中的交通状态工具变量、POI 时间指数及两期年份。]

#### 候选识别逻辑与构造步骤

固定 OD 需求与出发时间分布后，远处路网状态的变化可能改变路线选择，对替代路线不同的路段形成不同的预测流量冲击。导航普及引起绕行变化是用户提出的机制例子；北京短期状态变化是否外生，需要单独论证，不能由冻结 OD 自动保证。

1. **构造分时 OD 基准。** 用手机数据估计 OD × 出发时段的需求 D^0_ijh，跨日期固定这一基准，以便工具变量主要反映路线重分配。可按训练日期或留出日期估计基准，避免把当天需求冲击直接带入工具变量。当天实际 OD 可用于需求测量和控制，但不宜未经论证就作为外生预测流量的权重。人员出行转换成车辆需求还需交通方式、乘员数和抽样覆盖信息。
2. **定义交通状态。** 候选方案是将工作日 × 10 分钟的全城路速向量聚类为 S 类（例如 20 类），并记录状态频率。为路段 ℓ 定义状态时，仅使用一定距离以外的道路（例如 3 公里外），记为 s_{−N(ℓ)}(d,t)。S 和距离阈值均是待检验选择。逐路段排除邻域后，状态标签及状态路速模板都必须遵守同一排除规则；只在分类时剔除本路、随后又用本路当期速度分配路线，仍会产生机械关联。
3. **按状态分配路线。** 将现有单一参考速度下的 Q_sp 扩展为状态 × 出发时段的路线分配 C_{ℓsh}。使用固定 OD 和状态路速计算最短路径或路线份额；本地及排除邻域的路速采用预先固定或留出样本构造的参考值。分配须考虑出发到经过路段的时间差，不能将出发时段内所有车辆直接算成该时段每条路的通过流量。
4. **形成预测流量工具变量。** 将分配结果按路段实际经过时刻聚合，得到 Z_{ℓdt}=log Ĉ_{ℓdt}。不考虑跨时段传播的简化写法是 Ĉ_{ℓdt}=C_{ℓ,s_{−N(ℓ)}(d,t),h(t)}，正式实现须加入传播时间。候选识别来自同一路段、同一时钟在不同日期的远处状态变化，以及同一城市时段内各路段受到的不同影响。先检查在既有固定效应剔除后，Z 是否仍有足够变化；零预测流量的处理另行预设。

#### 区分简化式与流量 IV

若只有预测流量，用户提出的回归应记为简化式：

$$\log v_{\ell dt}=\alpha_{\ell\times\mathrm{clock}}+\delta_{\mathrm{city}\times\mathrm{date}\times\mathrm{half\mbox{-}hour}}+\gamma_{\mathrm{class}}Z_{\ell dt}+u_{\ell dt}.$$

γ 描述速度对预测路线重分配的反应。即便把预测流量乘上车辆数标定系数，也不会自动把 γ 变成实际流量的结构弹性。

若手机轨迹或其他数据可构造可信的实际路段流量 q，则以 Z 为工具，对 log q 做第一阶段，再估计速度方程：

$$\log q_{\ell dt}=\alpha^q_{\ell\times\mathrm{clock}}+\delta^q_{\mathrm{city}\times\mathrm{date}\times\mathrm{half\mbox{-}hour}}+\pi Z_{\ell dt}+X_{\ell dt}'\eta+r_{\ell dt},$$

$$\log v_{\ell dt}=\alpha^v_{\ell\times\mathrm{clock}}+\delta^v_{\mathrm{city}\times\mathrm{date}\times\mathrm{half\mbox{-}hour}}+\beta_{\mathrm{class}}\log q_{\ell dt}+X_{\ell dt}'\theta+\varepsilon_{\ell dt}.$$

第二式中的 log q 为内生变量，使用第一阶段工具化；按道路等级或方向估计异质性时，相应内生交互项也需要工具及足够样本支持。相同路段长度和流量口径下，局部通行时间弹性 λ=−β。BPR 指数 b 还需自由流时间、负荷和函数形式信息，不能直接设为 −β。

**尺度问题需细分：** 对数弹性不要求一定知道统一的车辆数乘数；恒定乘数只改变截距，稳定的路段乘数可被路段固定效应吸收。随日期、时段、交通方式或拥堵变化的手机渗透率会影响斜率识别。估计容量水平、流量与密度曲线及车辆总数仍需要尺度信息。深圳视频可支持当地真实计数校验，但不能未经论证就把深圳换算比例用于北京。

#### 用途与仍需检查的限制

- **需求与容量分解：** 在排除性成立且实际流量可识别时，可以估计路线重分配带来的流量变化对速度的影响。未被解释的部分仍混合遗漏需求、容量变化、测量误差和函数形式误差，不能全部称为“容量渠道”。完整分解需要容量或供给侧的额外信息。
- **共同时间覆盖：** 用户描述现有路速仅有 5 个工作日。手机数据即使覆盖更长，也不能自动增加速度方程的共同样本；先核对两者日期重叠。检查固定效应后的工具变量变异、状态样本量和实际流量第一阶段强度。没有实际 q 时，不能报告拥堵流量方程的第一阶段。大量路段也不能替代独立日期冲击的数量。
- **排除性：** 远处拥堵可能由共同就业中心需求、天气、事故或网络排队传播引起；即使不使用本路速度，这些因素仍可能直接影响本路速度。扩大排除距离、留出样本构造状态和控制手机测得需求可作为诊断，但不能单独证明排除性。
- **安慰剂与第二冲击：** 用户提出反方向路线预测流量作为安慰剂，可保留为候选，但双向需求和道路运行可能相关，应先说明何时预期效应为零。降雨会直接影响行车速度和有效容量，不能未经额外识别论证就作为流量的排除工具；用户提到的 Road Pricing 降雨做法需核对原文。
- **近期可执行起点：** 先形成手机数据字段与覆盖表、分时 OD 基准，以及其与 Speed 路速的共同日期表；随后做少量状态和道路样本的路线分配试验，检查排除本地速度后是否仍有预测流量变化。该记录保留研究路线，不表示已获得可信 IV 或替代主线 λ=1.1 的估计。

---

## 3. α, β (production agglomeration / residential crowding)

### Current practice

- **Mainline:** α = +0.05, β = −0.10 (pos05 track); not jointly estimated with θ.
- **Sensitivity:** `alpha_beta_sensitivity_bpr_pos05_v1` — W̄ LT and person-min move with α; extensive grid done.
- **Inversion:** (ū_i^θ, Ā_j^θ) recovered from marginals; **α, β not identified** from cross-section OD + marginals alone.

### What is needed

**Data**

- **Exogenous transport shocks** with spatial heterogeneity: tidal-lane DID, new ring/subway, Shenzhen multi-tidal pre/post, Liuxian / Caitian corridors.
- Optional extension: **wages, employment density, rents** if housing block added.
- Align priors with **Professor You Wei** related China spatial work (nextstep open item).

**Methods**

- Moment matching / GMM on **Δlog l_R, Δlog l_F** vs model-implied market access under exogenous τ shocks.
- Reduced-form: place exposure × policy → sorting, mapped to α, β.
- Continue sensitivity until shock-based identification is available.

**Reference:** `TidalLanes_estimation_notes.tex` §Sorting parameters; `MAINLINE_WORKPLAN_handoff_20260630.md`.

---

## 4. OD / road-user construction (nextstep #3)

Not a single scalar parameter, but affects L̄, inversion, and all CFs.

**Data**

- Mode-identified commute flows with better **identified vs pop coverage** by origin, workplace, distance, core/periphery.
- car+bus robustness vs car-only.

**Methods**

- Mode ratio × pop with hierarchical fallback (pilot: `model_input_edgeproj_v2_lcc_gate5_car_ratio_v1`).
- Grid-level coverage diagnostic before switching mainline.

**Decision (2026-07-02):** Pilot shows ~5–10% smaller W̄ Δ%; **mainline unchanged for now**.

---

## 5. Free-flow speed / BPR anchoring (nextstep #2)

**Data**

- Stricter Baidu free-flow: narrow late-night window, weekday/weekend split, upper-envelope or high-quantile speeds.

**Methods**

- Rebuild τ_ff → edgeproj bundle → Stage2; check `t_obs/t_ff` tails, anchored expansion CFs, policy ranking.

---

## 6. Reduced-form validation (supports λ and shock magnitudes)

**Data**

- Shenzhen **true pre/post multi-week** speeds (Caitian, Hongling; nextstep #11).
- Beijing centerline × Baidu API validation (nextstep #9).
- Liuxian / Longzhu control corridors for DID (nextstep #10).

**Methods**

- Short-window DID / event study on corridor speeds vs model **ST direct** and lane-shock calibration.
- Does not estimate θ directly; disciplines λ and shock interpretation.

---

## 7. Estimation priority (recommended order)

| Priority | Parameter / object | Main data gap | Method sketch |
|----------|-------------------|---------------|---------------|
| 1 | **λ_phys** | Directed link traffic (q) | Panel BPR + IV; compare to 1.1 |
| 2 | **θ** | Model-consistent τ + bilateral OD | PPML / θ grid with re-invert |
| 3 | **α, β** | Policy/shock panels | GMM or exposure × shock |
| 4 | **OD口径, τ_ff** | Mode coverage, ff definition | Input rebuild + full rerun |

---

## 8. Slides / appendix cross-links

- Theta grid: `appsens_theta` in `Documents/PreSlides/tidal.tex` and `tidal_model_slides_minimal/tidal.tex`.
- Speaker script: `Documents/PreSlides/tidal_speaker_script_20260702.md` Slide 40 († and −702 s note).
- Full theta table: `data_work/outputs/theta_sensitivity_gate5_bpr_pos05_v1/theta_sensitivity_appendix_table.csv`.

---

## 9. Related registry

- Live plan: `nextstep.md` items #1–3, #12.
- Estimation design: `Documents/L2_notes/TidalLanes_estimation_notes.tex`.
- Work log: `data_work/docs/WORK_LOG.md` entry 2026-07-02 parameter estimation roadmap.
