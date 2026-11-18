# Additive congestion delay 与 route-elasticity separation 备忘

- 日期：2026-07-17
- 状态：仅记录，尚未实施

本备忘记录两个后续模型扩展方向：（1）用 additive congestion delay 替代对总通行时间的乘法缩放；（2）把 route substitution、通勤时间效用成本和居住地及工作地替代弹性分开。当前 mainline、参数、代码、slides 和结果均未改变。

## 1. Additive congestion delay

### 1.1 当前 anchored specification

当前反事实 link cost 为

\[
t_{kl}^{cf}
=
\max\left\{
t_{kl}^{ff},
t_{kl}^{obs}
\left[
\frac{\Xi_{kl}^{cf}}{\Xi_{kl}^{0}}
\frac{n_{kl}^{0}}{n_{kl}^{cf}}
\right]^{\lambda/\theta}
\right\}.
\]

这一设定能够把模型锚定在基准路网和模型生成的 baseline load 上，但在 floor 不约束时，车道数或 traffic load 的变化会按比例缩放整个 observed travel time。这样一来，原本不应由拥堵改变的 free-flow component 也会随容量变化。

### 1.2 候选设定

先在 physical minutes 上定义 baseline congestion delay：

\[
D_{kl}^{0}
=
\max\{m_{kl}^{obs}-m_{kl}^{ff},0\}.
\]

再令反事实分钟数为

\[
m_{kl}^{cf}
=
m_{kl}^{ff}
+
D_{kl}^{0}
\left[
\frac{\Xi_{kl}^{cf}/n_{kl}^{cf}}
     {\Xi_{kl}^{0}/n_{kl}^{0}}
\right]^{\lambda},
\]

最后映射回模型使用的 link cost：

\[
t_{kl}^{cf}=(m_{kl}^{cf})^{1/\theta}.
\]

这一候选设定可命名为 `anchored_additive_delay`。它的经济解释是：free-flow minutes 是不受拥堵影响的技术部分，traffic per lane 只缩放基准 congestion delay。

### 1.3 主要优点

1. 车道变化只作用于 congestion delay，不直接改变 free-flow time。
2. free-flow time 自然成为下界，不再需要对反事实总时间单独使用 `max` kink。
3. 对于满足 \(m_{kl}^{obs}\geq m_{kl}^{ff}\) 的 links，no-shock baseline 精确恢复 observed directed time。
4. 反事实只使用 load-per-lane ratio，因此 \(\Xi^0\) 的绝对计量单位抵消。没有 observed link counts 时仍可运行，但 \(\Xi^0\) 仍是模型生成的 load，而不是观测车流。
5. \(\lambda\) 可以直接解释为 congestion delay 对 traffic per lane 的弹性。

### 1.4 必须保留的边界条件

“baseline 精确匹配 observed time”不能无条件表述。当前样本中有 219 条 links 满足 \(m_{kl}^{obs}<m_{kl}^{ff}\)，占 2,642 条 links 的约 8.3%。如果按上式把负 delay 截断为零，这些 links 在 no-shock baseline 下得到的是 \(m_{kl}^{ff}\)，而不是 \(m_{kl}^{obs}\)。

实施前需要明确选择一种处理规则：

1. 对 crossing links 令清理后的 free-flow time 为 \(\min\{m^{ff},m^{obs}\}\)。这能恢复精确 baseline anchor，但会修改 free-flow primitive。
2. 保留原始 night-based free-flow time，并接受 floor-adjusted baseline。此时必须报告受影响的 link 数量、traffic mass 和 welfare relevance。
3. 构造透明的 measurement-error correction 或平滑规则。该规则必须保持 delay 非负，并单独报告敏感性。

不建议直接允许负 congestion delay。还需要为 \(\Xi_{kl}^{0}\) 接近零、反事实车道数不可行以及 solver convergence 设置显式保护。

### 1.5 后续验证顺序

该设定适合作为与当前 mainline 并排运行的候选 specification，而不是在未比较结果前直接替换正文模型。后续实施时至少需要检查：

1. 在选定的 crossing-link 规则下，no-shock counterfactual 是否恢复目标 baseline。
2. 是否始终满足 \(m_{kl}^{cf}\geq m_{kl}^{ff}\)。
3. 反事实时间是否随 \(\Xi_{kl}^{cf}\) 单调增加，并随 \(n_{kl}^{cf}\) 单调下降。
4. 不同 free-flow definitions 和 \(\lambda\) 下的结果是否稳定。
5. 与当前 multiplicative anchor 比较 short-run 和 long-run welfare、physical travel time、TCS top-100、入选路段重叠率及政策排序。
6. equilibrium solver 的谱半径、收敛率和异常 link 分布是否发生实质变化。

### 1.6 Takeaway

`anchored_additive_delay` 是最值得优先实现的 congestion robustness。它在经济解释上比缩放总 observed time 更自然，并保留不依赖 observed link counts 的优点。其主要未决问题不是公式本身，而是如何处理 \(m^{obs}<m^{ff}\) 的 links。解决这一数据边界后，才可以把“精确 baseline anchor”作为完整优点。

## 2. \(\theta\) cancellation 与 route elasticity

### 2.1 当前 normalization 下的抵消

当前模型定义

\[
t_{kl}=m_{kl}^{1/\theta},
\qquad
A_{kl}=t_{kl}^{-\theta}.
\]

因此

\[
A_{kl}=\frac{1}{m_{kl}}.
\]

令

\[
B=(I-A)^{-1},
\qquad
\tau_{ij}^{-\theta}=B_{ij}.
\]

link \((k,l)\) 对 OD pair \((i,j)\) 的 all-route intensity 可以写为

\[
\kappa_{ij}^{kl}
=
\left(
\frac{\tau_{ij}}
     {\tau_{ik}t_{kl}\tau_{lj}}
\right)^{\theta}
=
\frac{B_{ik}B_{lj}}{m_{kl}B_{ij}}.
\]

给定 physical minutes，network matrix、OD route aggregator 和 link-level route intensity 都不随 \(\theta\) 独立变化。也就是说，在当前 normalization 下，\(\theta\) 不是对 physical travel time 的独立 route-substitution parameter。

### 2.2 \(\theta\) 仍然发挥作用的部分

上述抵消不意味着 \(\theta\) 在整个模型中无关。它仍通过以下渠道影响结果：

1. 居住地和工作地选择中的 \(\alpha\theta\) 与 \(\beta\theta\)。
2. fundamentals inversion。
3. population sorting 和长期均衡调整。
4. welfare 的尺度和映射。

因此，当前更准确的表述是：\(\theta\) 控制 residence-work alternatives 及相关福利映射，但 holding physical minutes fixed 时，它不独立控制 route assignment。

### 2.3 对参数识别的含义

由

\[
-\theta\log \tau_{ij}=\log B_{ij}
\]

可见，在当前 time-to-cost normalization 下，使用同一模型生成的 \(\tau_{ij}\) 做普通 OD travel-time PPML，不能从 physical travel time variation 中单独识别 \(\theta\)。改变候选 \(\theta\) 会同时改变 \(t=m^{1/\theta}\) 和外层幂次，二者在 route block 中机械抵消。

保留的识别结论是：在当前 mainline 中，\(\theta=6.83\) 应继续被视为外部校准参数。除非先改变 route-cost mapping，否则不应声称北京 OD PPML 在当前结构下 point-identifies \(\theta\)。

### 2.4 Olivia 的分离方式

Olivia 的当前版本把三个作用分开：

\[
d_{kl}=\exp(\kappa m_{kl}),
\]

\[
\tau_{ij}^{-\rho}
=
\left[(I-D^{-\rho})^{-1}\right]_{ij},
\]

\[
L_{ij}
\propto
\tau_{ij}^{-\theta}u_i^{\theta}w_j^{\theta}.
\]

其中：

1. \(\rho\) 控制 route substitution 或 route dispersion。
2. \(\theta\) 控制 residence-work substitution。
3. \(\kappa\) 把 physical minutes 转换为通勤效用成本。

在 route kernel 中，分钟的局部惩罚由 \(\rho\kappa\) 共同决定。因此，仅凭 aggregate OD flows 通常难以把 \(\rho\) 和 \(\kappa\) 完全分开。Olivia 的做法是估计或校准不同部分，而不是把所有作用都交给同一个 \(\theta\)。

如果采用 least-cost-time approximation，可以先估计

\[
E[L_{ij}]
=
\exp\{\delta_i+\gamma_j-\theta\kappa T_{ij}\}.
\]

给定外部 \(\theta=6.83\)，PPML 估计的是 \(\theta\kappa\)，再由此恢复 \(\kappa\)。这不是对 \(\rho\) 的识别。若使用完整 all-route aggregator，则估计式必须使用由候选 \((\rho,\kappa)\) 生成的 model-consistent \(\tau_{ij}\)，不能把普通 shortest-time PPML 当作完全等价的结构估计。

### 2.5 对当前项目的可行路径

第一步应是低成本的 route-assignment robustness：

1. 保留当前 AA all-route assignment。
2. 构造 deterministic 或 near-shortest-path assignment，作为更集中的 route allocation benchmark。
3. 比较两者生成的 \(\Xi^0\)、TCS scores、top-100 links、政策选择和福利结果。

这一比较首先是 route-load measurement robustness，不应直接表述为完整的结构模型替换。如果结果对 route dispersion 明显敏感，再进入第二步：正式引入独立的 \((\rho,\kappa)\)，重新计算 route costs、fundamentals inversion、均衡和福利。

正式扩展时不能只把当前 \(\theta\) 替换为 \(\rho=80\)。semi-log mapping 会改变 link cost 的单位、network matrix 和谱半径条件，因此必须同时引入 \(\kappa\)，并重新检查参数单位和 solver stability。Olivia 的 \(\kappa=0.01\) 可作为数量级 reference，不能直接作为北京基准值。北京版本应优先使用 OD commuting flows 和 travel times 估计 \(\theta\kappa\)，再在外部 \(\theta\) 下恢复 \(\kappa\)。

建议后续比较的核心输出包括：

1. link load 的 rank correlation、集中度和空间分布。
2. TCS top-100 overlap 与 selected-link characteristics。
3. short-run 和 long-run welfare、总 physical travel time 及政策排序。
4. 对 \(\rho\) grid 和 \(\kappa\) calibration 的敏感性。
5. network spectral radius、solver convergence 和数值稳定性。

### 2.6 Takeaway

当前 \(\theta\) 的主要问题是解释边界，而不是一个需要立即修改的数值错误。应先把它准确表述为 residence-work substitution 和 welfare parameter，并保留外部校准。最有信息量的下一步是 route-assignment robustness。只有当 \(\Xi^0\)、TCS selection 或福利结果对 route dispersion 敏感时，才有必要承担完整引入 \((\rho,\kappa)\) 所需的模型和估计成本。

## 3. 当前保留决定

1. 当前 mainline 继续使用 total-time multiplicative anchor，\(\theta=6.83\)，\(\lambda=1.1\)。
2. `anchored_additive_delay` 记录为优先级较高的并行 robustness specification，尚未实施。
3. 在解决 \(m^{obs}<m^{ff}\) 的 links 之前，不无条件声称 additive-delay specification 精确匹配全部 baseline observed times。
4. 当前模型不使用北京 OD PPML 重新识别 \(\theta\)。
5. 先做 route-assignment robustness；若主要政策结论敏感，再考虑完整的 \((\rho,\kappa,\theta)\) separation。
6. 本次记录不修改模型代码、数据、参数、slides、`nextstep.md` 或任何结果。

## 4. 相关材料

- 当前 anchored formula：`Documents/PreSlides/Tidal0715.tex`，约第 432 行。
- 当前参数和模型说明：`Documents/PreSlides/Tidal0715.tex`。
- 参数估计路线图：`Documents/L2_notes/parameter_estimation_data_roadmap_20260702.md`。
- Olivia Bordeu 当前论文：[Fragmented Cities](https://www.oliviabordeu.com/papers/fragmented_cities_obordeu.pdf)。
