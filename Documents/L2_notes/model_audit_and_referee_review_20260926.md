# 模型与估计审计、复现包更新、审稿人意见（2026-09-26）

**范围：** `data_work/src/model/full_estimation/`、`Documents/L2_notes/` 中的模型与结果文档、`Documents/PreSlides/Tidal0715.tex`、`tidal_model_slides_minimal/` 与 `_r/` 两个复现包。
**方法：** 通读代码与文档；用 `stage2_endogenous_edgeproj_v2_lcc_gate5_bpr_pos05_v1/baseline_endogenous/equilibrium_arrays.npz` 直接重算关键统计量；用主库代码重跑潮汐诊断与 αβ 网格；在两个复现包内重跑并对表。

---

## 0. 一句话结论

主线求解器、反演、anchored 拥堵和 Fréchet 福利口径的代码是对的，07-01 的福利 gauge 修正和 07-16 的物理时间修正也是对的；问题在于三处**没有跟上这两次修正**（复现包、authoritative 结果摘要、estimation notes），一个**实现错误的定价反事实**（P6/P7），以及两个**尚未处理的结构性识别问题**（区内通勤占比 57% vs 观测 8%；`t=m^{1/θ}` 映射使 θ 与路径成本不可分、每条 link 的流量时间弹性被固定为 1）。

---

## 1. 已核实的错误与不一致（按严重性）

### E1. 复现包滞后于主库（已修复代码与参考表，deck 未改）

- 包内 `equilibrium_solve.py`、`tidal_lane_diagnostics.py`、`run_tidal_lane_diagnostics_gate5.py` 与主库不一致：包内仍是 06-30 版本，把 all-route 冰山指数 `tau_ij` 当分钟乘 60 得到"秒"。主库 07-16 已改为物理时间口径（`Mbar = Σ Ξ_kl m_kl / Lbar`，以及 `M_ij = [RBR]_ij / R_ij`）。
- 包内参考表 `tidal_st_lt_compact.csv`、`alpha_beta_sensitivity_grid*.csv` 与包内 `tidal.tex` 仍报 ST direct `−1.63 s`、LT total `+0.065 s`、`+95 person-min/day`。正确口径下（top-100）：baseline 11.30 min，ST 10.60 min（省 42.1 s/人，1,031 person-hours），LT 11.54 min（多 14.4 s/人，+352 person-hours）。福利数字（+0.87%、+0.32%）不受影响。
- R 复现包 `diagnostics.R` 同样是旧口径。
- 已做：见第 2 节。未做：包内 `tidal.tex`（reduced deck）与 `Documents/PreSlides/tidal.tex` 仍是 07-02 叙事，需要决定是继续维护 reduced deck 还是把 `Tidal0715` 作为包内 deck。

### E2. Authoritative 结果摘要的通勤时间数字全部是旧单位

`Documents/L2_notes/model_and_results_summary_20260630.md`（`DOCUMENT_INDEX.md` 标为 authoritative）§1.1、§2、§4、§5 的 "sec/commuter"、"person-min/day" 全部来自 `tau_ij` 指数。用新口径重跑 αβ 网格后：

| cell | 旧 LT total (s) | 新 LT total (s) | 旧 person-min/day | 新 person-min/day |
|---|---:|---:|---:|---:|
| α=−0.12, β=−0.10 | −4.37 | +6.70 | −6,417 | +9,837 |
| α=0.05, β=−0.10（主） | +0.07 | +14.39 | +95 | +21,135 |
| α=0.10, β=−0.10 | +4.32 | +21.15 | +6,340 | +31,055 |

结论变化：摘要 §1.3 的"person-min 过零点约在 α≈0.04–0.05"不成立。**所有收敛格的长期通勤时间都上升**，且 ST direct 稳定在 −42 s/人而非 −1.63 s/人。福利列不变。新表：`data_work/outputs/alpha_beta_sensitivity_bpr_pos05_physical_time_v1/`。

### E3. 三份"active"文档仍是 gauge 修正前的口径

- `Documents/L2_notes/TidalLanes_estimation_notes.tex`（index 标 active）：写 `W̄^θ = Σ_i u_i^θ Π_i^{-θ}`，报 "net W̄_LT ≈ −0.85%"、"sorting total ≈ −8.5 pp"、"**No converged specification yields W̄_LT>0**"。这与 07-01 之后的全部结果相反。
- `Documents/L1_manuscripts/TidalLanes_Model_v4.tex` 第 361–370 行：eq (welfare_MA) `W̄^θ = Σ_i u_i^θ Π_i^{-θ}` 被写成 "the welfare object the model identifies"，并说反事实用 exact-hat。代码用的是 eq (Wbar_def) 的 Fréchet 分母，且不用 exact-hat。两式在数学上只在 (Π,P) 取特定 gauge 时相等，代码的 `_market_access` 不施加该 gauge，所以应把 (Wbar_def) 写成主口径，(welfare_MA) 改成"在 gauge 条件下等价"。
- 包内 `src/model/full_estimation/ESTIMATION_NOTES.md`：`alpha=−0.12`、legacy 拥堵、Π 福利、`N≈2700`。已在文首加"状态更新"段落，正文其余未重写。

### E4. P6 "first-best pricing" 与 P7 cordon 实现错误（−4.2% 是 artifact）

`stage2_counterfactuals.run_congestion_pricing_first_best` 把 toll 加到 `tau_ff_override`。在 anchored 形式下 `m = max(m_ff + toll, m_obs·dev^λ)`，`m_ff` 只是下界。我用 baseline 核算：`toll = 1.1·m_base`，在 **100%** 的边上 `m_ff + toll > m_obs`，所以 floor 全部绑定，拥堵函数被整体关闭，感知成本相对观测的中位数为 ×1.97。因此 −4.2% 不是定价的福利效应，而是"全网成本近乎翻倍且无拥堵反馈"的结果。P7 同理（toll 加到 floor 上），且收入用 `baseline.Xi_kl` 而不是定价后的流量。

更根本的问题：在常弹性乘法拥堵技术 `m = m_obs·(Ξ/Ξ0)^λ` 下，边际外部成本 `MEC = λ·m` 对**每条边都成比例**，Pigou 税等价于全网统一加价（在本模型里等价于 κ 重标度），"first-best" 没有差异化内容。只有把拥堵写成 `m = m_ff + D0·(Ξ/Ξ0 · n0/n)^λ`（0717 备忘的 `anchored_additive_delay`）之后，`MEC = λ·D` 才在边之间有差异，定价反事实才有意义。建议在 additive-delay 实现之前把 P6/P7 从所有表中撤下。

### E5. 区内通勤：模型 57% vs 观测 8%（结构性，未处理）

从 baseline 重算：模型均衡 OD 中 `i=j` 的占比 **52.7%**（Fréchet 流量 `L_ij ∝ τ_ij^{-θ}u_i^θw_j^θ`；若用存储的 `(Π,P)` 做 doubly-constrained gravity 则为 57.2%），观测 `commuters_road` 中 **8.0%**。原因是 `A_kl = 1/m_kl`，所以 `R_ii = [(I−A)^{-1}]_ii ≈ 1.03` 而相邻格 `R_ij` 中位 0.124，`R_ii/R_ij` 中位 **8.4**：留在原格的"成本"是 1，去相邻格的成本指数是 8 倍以上。后果：

- 物理时间水平（11.3 min）中一半以上的权重来自区内"环路"步行时间（`M_ii` 约 0.5 min），不能与任何真实通勤时间比较；
- 走廊 exposure 与潮汐效应被区内质量稀释；
- 反演出的 `ū_i, Ā_j` 吸收了这个错配；
- 附录 `ρ=0.413` 是 origin/destination FE 之后的残差相关，对角线每个 origin 只有一格，这个统计量看不出该问题。Stage 1 文档 §3.2 已经记录 52.7%，但没有进入主线修正。

### E6. `t = m^{1/θ}` 映射：θ 与路径成本不可分，link 流量时间弹性被固定为 1

- `A_kl = t_kl^{-θ} = 1/m_kl`：给定物理分钟，路径核 `(I−A)^{-1}` 与 θ 无关（0717 备忘已指出）。θ 只通过 `αθ, βθ` 和福利尺度起作用。
- 每条 link 的流量对时间弹性恒为 −1，与 θ 无关；一条 K 段路径整体时间涨 10% 时流量降 `1−1.1^{−K}`（K=10 时 −61%）。ARSW 的 `exp(−θκ·min)`（θκ≈0.07/min）下同样情形约 −19%。**θ=6.83 是在另一种成本规范下估出的，不能连同规范一起移植。**
- 路径成本是 link 时间的乘积，所以依赖网格离散化（把一条 10 分钟边拆成两条 5 分钟边，成本指数从 10 变成 25）；κ 和时间单位有实质内容（08-30 审计已确认 κ=1→3 改变 LT 福利 +0.32%→+0.27%）。
- 早期 gravity 诊断 θ̂≈0.62–0.70（vs 6.83）正是数据拒绝这一规范的信号，不应只当作"诊断不 model-consistent"搁置。

### E7. 小项

- Deck 第 16 页写自由流窗口 `22:00--06:00`，bundle 与 robustness README 都是 `22:00--05:00`。
- "Decomposition of Equilibrium Welfare" 页写的是 `τ̄` 的三项分解，结果表 Panel A 用的是物理时间的 link-passage 分解，Panel B 用福利；公式与表不对应。
- 8.3%（219 条）`m_obs < m_ff` 的边：anchor `Ξ0` 在未 floor 的 `m_obs` 处计算，baseline 均衡却落在 `m_ff`，两者在这些边上不一致；对这些边任何减 lane 的冲击都会被 floor 吃掉。
- "短期（locations fixed）"只钉住边际 `l^R, l^F`，OD 矩阵按 gravity 重新平衡，即短期允许换工作或换住处的重新配对；更严格的短期应固定 `L_ij`。
- 选边 `score_bpr_st` 用观测 `m_obs − m_ff` 排序 top-100，存在 winner's curse：噪声大的边更容易入选，直接效应被高估。手头有 5 个工作日的百度速度面板，可以分半检验。
- Stage 1 文稿（θ=6.83 行）"symmetric 世界 W̄ 低 1.14%" 与 07-01 suite "symmetric 成本下 W̄ +1.10%" 是两个不同实验（重反演 vs 固定 fundamentals），量级相同、符号相反，同时出现在文档里容易被误读。
- `code_logic_audit_20260630.md` 的 P1（`max_outer` 被覆盖）已修；P4（exog 层 CF2 用 δ₁=0.488）、P5（`run_cf2_tidal` 硬编码 asym）仍在代码里。

### E9（2026-09-28 补充）. 主线求解器把 `R=(I−A)^{-1}` 在 1e-12 处截断，且该截断在 89% 的 OD 格上绑定

`equilibrium_solve._compute_tau_inv_theta` 与 `invert_fundamentals` 都对 `R` 做 `clip(1e-12)`。在 `A_kl=1/m_kl` 下，88.8% 的 OD 格 `R_ij<1e-12`（中位 1e-33），这些格承载 30.7% 的观测通勤人数。截断之后这些远距离 OD 在模型里成本相同，等价于对三成通勤者关闭了距离衰减。去掉截断后主线数字变为：baseline W̄ 3.2001→3.2569，区内占比 52.7%→56.9%，ST/LT 福利 +0.87%/+0.32% → +1.07%/+0.29%，ST 省时 42.1→50.5 s/人，LT +14.4→+18.7 s/人。`PhysicalTravelTimeMeasurement.tex` 提到过这个 floor，但没有量化。它必须被去掉或有理由；同时它也是 E5/E6 的另一面：在幂核下远距离对的 `R_ij` 小到需要截断，本身说明核太陡。

### D6 试算结果（2026-09-28，`data_work/outputs/route_kernel_exp_trial_20260928_nu00122_v2/README.md`）

- 观测通勤对网络最短路分钟的 PPML 半弹性：全样本 0.0122/min，≤60 min 样本 0.0420/min；log-log 弹性 1.36。主线模型自身 OD 的对应值为 0.087/min 与 3.3，即模型对时间的敏感度是数据的 2–7 倍；观测流量对 `ln R_ij` 的斜率 0.165（模型强加 1）。
- 观测通勤量加权的网络最短路均值 121 分钟、34% 的通勤人数在 90 分钟以上，远高于任何合理的北京通勤时间，说明网格网络的分钟数本身偏高（连接段、绕行、20 km/h 的 link 速度）；`nu` 应理解为对网络分钟的弹性。
- 指数核 `a=exp(−ρκm)`、`τ^{-θ}=R^{θ/ρ}`、`κ=ν/θ`、`ρ` 由谱半径定：ν=0.0122 时区内占比 6.3%（观测 8%）、baseline 期望通勤 113 分钟（与观测加权最短路 121 一致）、ST 福利 +0.53%、LT +0.33%、ST 省 112 s/人、LT −3 s/人（近零）。ν=0.042 时区内 15%、ST +2.44%、LT +0.56%、ST 省 173 s、LT +108 s。
- 结论：定性结果（ST 省时、LT 福利为正、LT 时间不低于 baseline）在所有核下成立；幅度随核与 ν 变化一个量级。只有 ν=0.0122 的指数核同时匹配区内份额与通勤时间分布。进入正文前需要 (a) 一个可信的 door-to-door 分钟测量来定 ν，(b) 处理 E9 的截断。

---

## 2. 本次已完成的更新

**Python 复现包 `tidal_model_slides_minimal/`**

- 同步主库的 `equilibrium_solve.py`、`tidal_lane_diagnostics.py`、`run_tidal_lane_diagnostics_gate5.py`（现在 20 个模型文件与主库逐文件一致）。
- 主库新增输出：`data_work/outputs/tidal_lane_diagnostics_gate5_bpr_pos05_physical_time_v2/`（四个 spec，物理时间口径）与 `data_work/outputs/alpha_beta_sensitivity_bpr_pos05_physical_time_v1/`（9 格）。top-100 数字与 07-16 的 `_physical_time_v1` 完全一致。
- `scripts/package_manifest.py` 与 `docs/FILE_MAP.csv` 的四条 reference 指向上述新目录；参考表已替换。
- `src/model/full_estimation/ESTIMATION_NOTES.md` 文首加 2026-09-26 状态更新（福利口径、参数、anchored、物理时间、N）。
- 验证：`make check` 通过；`make smoke-model` `Wbar=3.200092`、`median_tau_obs_ratio=1.0001`；包内重跑 tidal diagnostics 与新参考表最大绝对差 0；αβ 网格验证见 `docs/VALIDATION_20260926.md`。

**R 复现包 `tidal_model_slides_minimal_r/`**

- `R/model/diagnostics.R` 改为物理时间分解（`equilibrium_od_shares`、`expected_additive_route_minutes`、link-passage 平均），`run_tidal_lane_diagnostics_gate5.R` 的 compact 列同步；参考表替换。R 重跑与对表结果见 `README.md` 验证段。

**未改**：两个包内的 `tidal.tex` / `tidal.pdf`、speaker script；`model_and_results_summary_20260630.md`；`TidalLanes_estimation_notes.tex`；`TidalLanes_Model_v4.tex` 的 (welfare_MA) 段。这些是编辑决策，列在第 3 节。

---

## 3. 下一步：按 `nextstep.md` 的顺序，哪些能直接做

按"能否直接操作"分三档。

### 3A. 现在就能做（只改文档或几十行代码，不需要新数据）

1. **改口径文档**：`model_and_results_summary_20260630.md` §1.1/§1.3/§2/§4/§5 的通勤时间列换成 `physical_time_v2` 与 `alpha_beta_..._physical_time_v1` 的数字；删除"person-min 过零点"结论。`TidalLanes_estimation_notes.tex` §Sorting 与 §Equilibrium 的福利段改为 Fréchet 口径。`TidalLanes_Model_v4.tex` (welfare_MA) 改为"gauge 条件下等价"。`DOCUMENT_INDEX.md` 的"四次修订"加第五次（07-16 物理时间）。
2. **撤下 P6/P7**：在 `run_policy_comparison_gate5.py` 加开关默认跳过，deck 附录与 nextstep 的"政策反事实对比套件"里标注"待 additive-delay 实现后重做"。
3. **Deck 小修**：自由流窗口改 `22:00--05:00`；decomposition 页公式改成 link-passage 版本或加一句说明。
4. **`code_logic_audit` P4/P5**：`run_cf2_tidal` 接 `pair_selection`；exog 层 CF2 用 anchored 口径。
5. **复现包 deck 决策**：要么把 `Tidal0715.tex` 与其 figs 纳入 `package_manifest.py`，要么把 reduced deck 结果页改成物理时间数字。前者只改 manifest。

### 3B. 用现有数据即可做，半天到两天

6. **`anchored_additive_delay`**（0717 备忘 §1，nextstep #6/#7 的前置）：在 `_congestion_tau_update` 加一个 form，`m = m_ff + max(m_obs − m_ff, 0)·dev^λ`；219 条 crossing links 先用规则 2（保留 floor 并报告受影响质量）。跑 baseline、top-100 ST/LT、P1–P5，与乘法 anchor 并排。这一步同时使 P6 有意义。
7. **区内通勤修正**（E5）：最小改法是给 `i=j` 一个正的区内分钟 `m_ii`（例如格内平均出行距离除以格内平均速度，或直接校准使模型区内占比等于 8%），进入 `A` 的对角线或作为 OD 选择集中的单独成本项；重反演、重跑 top-100。报告 `Mbar`、exposure、fundamentals 相关系数的变化。
8. **选边 winner's curse 检验**：用 5 个工作日速度面板，奇数日选 top-100、偶数日评估，比较直接效应；或 AM 选边、PM 速度评估。
9. **θκ 的 gravity 估计**（E6 的数据侧）：用百度 OD 与百度路径 API 的 door-to-door 分钟，PPML `L_ij ~ FE_i + FE_j − θκ·min_ij`，得到北京的 `θκ`。这不改模型，但给出替换 `A_kl = 1/m_kl` 的目标弹性。
10. **αβ 敏感性重写**：sensitivity 页的 "sort (s)" 与 "person-min/day" 列用新口径；叙事改为"所有收敛格 LT 通勤时间上升、福利为正"。

### 3C. 需要新数据或较大改动

11. **route-cost 规范切换**（E6 的模型侧）：`A_kl = exp(−ρκ·m_kl)` 或 `(κ m_kl)^{-ρ}`，ρ 与 θ 分离（Bordeu 式），κ 由第 9 步的 θκ 校准；重做谱半径检查与反演。这是投稿前最值得做的一项结构改动。
12. **彩田路正式采集**（nextstep #0）：同机位、同 geometry、多工作日、断点两侧各 60 分钟；这是唯一能把 λ 或"lane 切换 → 流量/速度"接到北京模型之外证据的路径。
13. **深圳 Package A**（nextstep #10）：已开通走廊的 cost-proxy 对照表；可与第 12 步合并汇报。
14. **人口增长/城市扩张正式化**（nextstep #3）与 **政策组合选边**（#7）：在 6、7 完成之后再做，否则结果会被重算一遍。

---

## 4. 审稿人意见：模型创新、识别、文章走向

### 4.1 模型创新：现在的"新"在哪里，哪些站不住

**站得住的贡献。** 把有向 link 作为 QSM 的基本单元，用高频分段速度构造有向 `m_kl`，并让拥堵反馈按方向发生。这是对 AA (2022) 的自然扩展，AA 的路网本身就是有向的，但他们的 Seattle 数据没有方向不对称的信号。本文的增量是**测量**（方向不对称在全国和分段两个尺度都存在且大）和**政策对象**（潮汐车道是唯一只能在有向模型里定义的交通政策）。

**目前会被质疑的地方。**

1. 模型结构是 AA 原样，所有参数外借。审稿人会问："除了把速度按方向分开，模型里有什么是必须由有向结构才能得到的定量结果？"目前的回答（"symmetric 低估通勤时间 5%"）依赖 `Ξ` 的方向协方差，这是一个测量事实，不是均衡结果；aggregate 福利差 <0.07%、地铁选址不变，说明**在本模型内**方向性对配置结论影响很小。这对"对称假设遗漏什么"这一贡献点是不利的，需要正面回答而不是回避：要么找到方向性改变排序的政策问题（潮汐 vs 单向扩容的选边、通勤方向相反的两类人的分配效应），要么把第二个贡献降为测量贡献。
2. E5 和 E6 会被任何熟悉 AA 框架的审稿人抓住：57% 区内、θ 不可分、乘法路径成本依赖网格。这些不是"稳健性"，是模型是否拟合数据的问题。在解决之前，任何福利的第二位小数都没有意义。
3. 短期/长期分解是好的展示设备，但"短期"里 OD 重配对、"GE wedge"实为 sorting 引致的拥堵回弹，命名需要重写：Direct（固定 `L_ij`）/ Rerouting & congestion（固定 `l^R,l^F`）/ Sorting。

**值得推的模型增量（按性价比）。** (a) `anchored_additive_delay`：free-flow 与 delay 分离，λ 有清楚含义，定价反事实才成立；(b) 路径成本用 `exp(−ρκ·m)` 并把 ρ、θ、κ 分开，κ 由北京 OD 的 gravity 估出，这是让 θ=6.83 可辩护的唯一办法；(c) 一个显式的方向性外部性分解：潮汐让"顺向"人受益、"逆向"人受损，把 07-27 会议要求的 never/always/switch 四组福利做出来，这是有向模型独有的分配结果，也是最容易讲清楚的创新。

### 4.2 识别：四个参数各自缺什么

| 参数 | 现状 | 可行的识别路径 | 优先级 |
|---|---|---|---|
| λ（速度对流量弹性） | 外借 Barwick 1.1，且被套在"相对偏离"上；模型内 `Ξ` 是通勤子样本而非车流 | 彩田路视频（流量 + 通行时间）直接给局部速度–流量斜率；百度速度 × 视频计数的同时段面板；至少给 λ∈{0.57, 1.1} 之外一个北京自己的点 | 最高 |
| θκ（通勤对时间的半弹性） | θ=6.83 外借，κ 隐含为 1/min 并与 θ 纠缠 | 百度 OD × 百度路径分钟的 PPML；用 2022 OD 与 2025 速度的时间错配作为讨论点而非无视 | 高 |
| α, β | 外借，敏感性网格 | 只能借；但要说明结论对 α 的方向：LT 通勤时间在所有格上升、福利在所有格为正 | 中 |
| 选边 | 模型内 `Ξ`+观测 delay | 分半日期检验；与深圳已建潮汐走廊对照（Package A） | 中 |

对识别的整体态度建议：结构模型部分明确写成**calibrated quantitative exercise**，把因果识别放在两个 reduced-form 支柱上：(i) 彩田路多日 RDiT 给"lane 切换 → 方向流量/速度"的局部效应；(ii) 深圳留仙/多走廊 pre-post 给走廊尺度的速度响应。模型的角色是把局部效应放到 GE 里加总，而不是自己识别参数。目前 deck 的 Next Steps 已经这样说，正文应该同步。

### 4.3 文章 framing 与走向

**当前 framing 的问题。** 标题和前 5 页讲"方向性拥堵被忽视"，中间讲一个参数全借的 QSM，结尾讲潮汐车道 +0.32% 福利、零 lane-km。三段之间的逻辑链是"测量 → 模型 → 政策"，但审稿人会觉得每一段都不够硬：测量部分是描述统计，模型部分拟合差，政策部分效应小且参数外借。

**建议的走向（三选一，越靠前越保守）。**

A. **测量 + 政策评估论文（最稳）。** 主贡献是方向性拥堵的系统测量（国家、城市、分段三层）和潮汐车道的因果证据（深圳视频 RDiT + 多走廊 pre-post）。QSM 作为第三部分，用来回答"局部效应在 GE 里剩多少"，只报相对量（ST vs LT 的比例、方向性分配），不报福利的绝对百分比。适合 JUE / RSUE / JTEG。

B. **有向 QSM 方法论文。** 主贡献是把有向 link、方向性拥堵反馈、路径成本 ρ–θ–κ 分离做成一个可估计的框架，并证明对称化在**分配**上遗漏了什么（谁受益谁受损），而不是在 aggregate 上。需要先做完 3B/3C 的 6、7、9、11，并把 never/always/switch 分组做成主结果。适合 JUE 或更高，但工作量最大。

C. **潮汐车道的成本–效果论文。** 以"零 lane-km 的定向再分配 vs 扩容 vs 定价"为主线，模型只是加总工具。要求 P6/P7 重做（additive delay）、成本数字有来源、λ 有北京自己的估计。政策相关性最强，但最依赖参数。

我的建议是 **A 为主、吸收 B 的分配结果**：把"symmetric 遗漏什么"从 aggregate 福利改写成"方向性分配"（顺向/逆向、中心/外围、never/always/switch），把结构模型的绝对福利数字降级为附录，把彩田路和深圳的 reduced-form 提到正文第二部分。这样每一段都有自己的证据支撑，模型的已知缺陷（E5、E6）成为"limitations"而不是"main result 的基础"。

**具体到写作。** 摘要与引言的第一句应是测量事实（"在 100 个最拥堵城市中，通勤流量加权的方向速度比中位数为 0.878；北京 4.9% 的双向中心线一侧慢于另一侧三倍以上"），第二句是政策事实（彩田路 lane 切换后方向流量重分配的量级），第三句才是模型（"在一个有向 QSM 里，直接效应的三分之一到一半在 sorting 后保留"），不要以福利百分比开头。

---

## 5. 相关输出与文件

- 新输出：`data_work/outputs/tidal_lane_diagnostics_gate5_bpr_pos05_physical_time_v2/`、`data_work/outputs/alpha_beta_sensitivity_bpr_pos05_physical_time_v1/`
- 复现包记录：`tidal_model_slides_minimal/docs/VALIDATION_20260926.md`、`tidal_model_slides_minimal_r/README.md`
- 本次核算脚本未入库；重算 E4、E5 的命令见 `data_work/docs/WORK_LOG.md` 2026-09-26 条目。

---

## 6. 2026-09-28 讨论：anchor 方法的问题、additive delay、区内通勤修正

### 6.1 现在的 anchor 与 AA 原文的差别，以及问题在哪

AA (2022) 的做法：观测到每条 link 的车流 `Ξ_obs`（AADT）和速度，估计 `speed^{-1} = m0 (Ξ/lanes)^{δ1}`，然后把 `t̄_kl` 反推出来，反事实用同一条水平方程。本项目没有观测车流，只有速度；`Ξ` 是模型分配出来的通勤子样本 passage 数，水平没有意义。所以采用 anchored 形式：

`m = max{ m_ff, m_obs · [(Ξ/Ξ⁰)(n⁰/n)]^λ }`

它只用比率，回避了 `Ξ` 的水平问题，这是对的。但它有四个后果：

1. **λ 作用在总时间上，而不是延误上。** Barwick 的 1.1 是 `ln(speed)` 对 `ln(density)` 的弹性，在拥堵路段上估的。套到总时间上意味着 free-flow 部分也随流量按 1.1 次方缩放；一条 free-flow 20 分钟、观测 22 分钟的边，流量减半时模型给出 `22·0.5^1.1 = 10.3` 分钟，被 `max` 截到 20。截断在这类边上吃掉了全部效应，在拥堵边上又给出过大的效应。
2. **`m_obs < m_ff` 的 219 条边（8.3%）上 anchor 与 baseline 不一致。** `Ξ⁰` 在 `m_obs` 处算，baseline 均衡却落在 `m_ff`。
3. **同一个 λ 对所有边。** 主线里 MEC = λ·m 处处成比例，所以定价反事实退化为统一加价（E4）。
4. **`Ξ` 是通勤子样本，且是 all-route 期望 passage，不是车流。** 比率 `Ξ/Ξ⁰` 隐含"总车流的相对变化等于通勤子样本 passage 的相对变化"。在 E9 的截断和幂核下，`Ξ` 里有很大一部分来自远距离对的 walk（含环路），这个比率的可信度取决于路径核。

### 6.2 additive delay 是什么

把每条边的观测时间拆成两部分：技术部分 `m_ff`（夜间自由流）和延误 `D⁰ = max{m_obs − m_ff, 0}`。反事实只让延误随单车道流量变化：

`m^cf = m_ff + D⁰ · [(Ξ^cf/n^cf) / (Ξ⁰/n⁰)]^λ`，然后 `t = (m^cf)^{1/θ}`。

直觉：一条边从 20 分钟自由流堵到 22 分钟，多的 2 分钟才是拥堵。给它加一条车道，延误从 2 分钟按 `(2/3)^1.1` 降到 1.3 分钟，总时间 21.3；乘法 anchor 会给出 `22·(2/3)^1.1 = 14.1`，再被截到 20。两者对轻度拥堵的边差别很大，对重度拥堵的边差别小。

它的好处：λ 直接是"延误对单车道流量的弹性"，与 BPR 一致；free-flow 自然成为下界，不再需要 `max`；无冲击时精确回到 `m_obs`（在 `m_obs ≥ m_ff` 的边上）；MEC = λ·D 在边之间有差异，定价反事实才有内容。代价：`m_obs < m_ff` 的边延误为 0，永远不响应；所以自由流口径要更严格（这就是 02–05 点窗口的目的：让 `m_ff` 更低，减少交叉边），并把受影响的边数、通勤质量和它们在 treated 集里的比例报告出来。

它与 Barwick 的映射：Barwick 的 1.1 是总速度弹性，不是延误弹性。在延误形式下，同一份数据隐含的延误弹性为 `λ_D = 1.1 · m/D`，在中位 `m_obs/m_ff = 1.15` 的边上约为 8。所以 additive delay 不能直接套 1.1，要么重新从速度–密度数据推延误弹性，要么把 λ 当作待校准量，用彩田路的流量–通行时间断点来定。这是采用 additive delay 前必须决定的事。

### 6.3 区内通勤修正（D4）

问题来源：幂核下 `R_ii ≈ 1.03`，相邻格 `R_ij ≈ 0.12`，模型把 53% 的通勤放在区内（观测 8%）。三种做法：

(a) **给区内一个正的时间 `m_ii`。** 在 `A` 之外，把 `τ_ii^{-θ}` 从 `R_ii` 改成 `R_ii · m_ii^{-1}`（幂核）或乘 `exp(−θκ m_ii)`（指数核）。`m_ii` 取格内平均出行距离除以格内平均速度：3 km 网格内均匀分布的两点平均距离约 1.6 km，按 20 km/h 约 5 分钟。改一行代码，重反演即可。缺点：`m_ii` 是假设值，结果对它敏感。

(b) **校准 `m_ii` 使模型区内份额等于 8%。** 一个标量、一个矩，闭合。缺点：把一个拟合问题藏进一个参数。

(c) **换路径核。** 指数核 ν=0.0122 下区内份额自动为 6.3%，不需要额外参数，因为 `exp(−θκ·0) = 1` 与 `exp(−θκ·SP)` 的比值由估计出的 ν 决定，而不是被 `1/m` 的量纲锁死。这是 D6 试算里最有说服力的一点。

建议顺序：先做 (a) 作为主线的最小修补并报告敏感性，(c) 作为并行规范；(b) 只作对照。无论哪种，`M̄` 的水平在修正前后都要重报，因为现在的 11.3 分钟有一半以上来自区内。
