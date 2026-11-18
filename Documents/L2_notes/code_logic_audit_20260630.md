# 代码 / 逻辑 / 诊断 校验报告（gate5 主链）

**日期：** 2026-06-30（平行窗口，独立于 MAINLINE_WORKPLAN 执行）
**对象：** `model_and_results_summary_20260630.md` 所依赖的 `data_work/src/model/full_estimation/` 代码与输出
**结论：** 核心算法逻辑成立，无致命 bug；summary 中 §3–§5 的数字与产出 CSV **逐项对得上**（非凭空）。发现 **1 个真实代码 bug**、**1 个写作口径风险**、若干一致性/口径项需在 Phase 0 重跑前处理。

---

## 0. 一句话

> 均衡求解、anchored 拥堵、inversion、ST/LT 与 W̄/Π 分解在算法上都正确（kappa 固定、分解是精确恒等式且有 check 项）。但 (a) 潮汐诊断脚本有一处 `max_outer` 覆盖 bug 会把正-α 的 500 预算降回 120；(b) 换成 `score_bpr_st` 后 **W̄ LT ≈ −0.85%**，约为 asym(−0.37%) 的两倍，summary/handoff 的「W̄ LT 小幅负 0.15–0.37%」口径在主线切换后不再成立。

---

## 1. 已验证正确（可放心）

1. **kappa 跨 baseline/anchor/CF 固定。** `_solver_kwargs` 与 `build_congestion_solver_kwargs` 始终传 `kappa=fund.kappa`，`compute_observation_anchors` 也用同一 kappa。→ 反事实比值对 kappa 不变这一性质**真的成立**（docstring 的承诺被代码兑现）。

2. **anchored baseline 是 τ_obs 处的不动点（在 τ_obs≥τ_ff 的边上）。** dev=(Ξ/Ξ_obs)(n_obs/n)，baseline 时三者相等→dev=1→τ=τ_obs。median τ/τ_obs≈1.000 由构造保证。✔（floor 例外见 §3-③）

3. **W̄_θ 分解与 Π 四层分解是精确可加恒等式。** `decompose_wbar_theta` 的 `check_sum_st_plus_sort`、`decompose_pi_inv_theta` 的 `check_sequential_sum_mean` 都在代码里显式核对。ST(①) + sorting(②③④) 拆解互洽：pos05 下 ②+③+④ = −1.29−1.51−0.42 = **−3.22**，正是 §5.2「ΔΠ^{−θ}, u 钉 baseline = −3.22」。内部完全自洽。✔

4. **ST/LT 通勤分解恒等式成立。** `od_cost_decomposition` 中 direct_st+ge_wedge+sorting ≡ total_lt（代数严格，与 OD 边际是否硬约束无关）。✔

5. **summary 数字 = 产出 CSV（抽样核对，全部吻合）：**
   - Seattle asym：direct −0.786 / sorting −1.085 / LT −1.397s / W̄_LT −0.156% → §3、§4.2 ✔（−1.397/60×Lbar≈−2049 ≈ −2051 person-min）
   - pos05 asym：direct −0.787 / sorting +0.862 / LT +0.602s / W̄_LT −0.365% → §3、§5.1 ✔（+884 person-min ✔）
   - pos05 **bpr**：direct −1.634 / sorting +0.707 / LT +0.065s（+95 person-min）→ §4.4 ✔
   - 打平点：a02_bneg asym total_lt=−0.011≈0；a01=−0.17、a03=+0.17 → 过零 ≈ α0.02 ✔
   - person-min = `total_lt_cost_change_sec/60 * Lbar`，隐含 Lbar≈88k，六行一致。✔

6. **稳定性：** 求解器指数 `1/(1−αθ)`、`1/(1−βθ)` 在 α∈[−0.12,0.10]、β∈[−0.10,0.05] 全为正（αθ<1）；交替映射收缩条件 |αβθ²|<1 也满足。α 网格不会发散。✔

---

## 2. 发现的问题（按重要性）

### 🔴 P1 — `run_tidal_lane_diagnostics_gate5.main()` 把正-α 的 max_outer=500 覆盖成 120（真实 bug）

`run_alpha_robustness_quick_gate5.solver_overrides_for_params` 专门为 α>0 返回 `max_outer=500, damping=0.10, tol=1e-4`，并带注释「Never downgrade the positive-alpha budget」。但主诊断脚本 line 238–239：

```python
skw = {k: v for k, v in solver_overrides_for_params(params).items() if v is not None}
skw["max_outer"] = args.max_outer        # ← 无条件覆盖成 CLI 默认 120
```

`solver_overrides_for_params(params)` 没传 `max_outer`，所以 max(120,500) 的保护逻辑被绕过，最终 `max_outer=120`。

- **当前影响有限：** 现有 lanefix_pos05 输出在 n_outer=**65** 就收敛（<120），所以没踩到。
- **风险：** Phase 0 切到 `score_bpr_st`（direct_st 从 −0.79→−1.63，shock 大一倍）或扫 β>0 时，120 可能不够，会**静默地停在欠收敛点**（脚本里 tol 还放宽到 1e-4）。
- **修法：** 改为 `solver_overrides_for_params(params, max_outer=args.max_outer)`（函数已内置 max(...) 不降级逻辑）。Phase 0 重跑顺手把 tol 收紧到 1e-5 对齐主规格。

> 注：`run_welfare_sorting_diagnostics_gate5._solve_track` 用的是干净的 `solver_overrides_for_params(params)`（500 生效），所以 W̄/Π 那套输出不受此 bug 影响；只有 tidal ST/LT 那套受影响。

### 🟠 P2 — 主线切到 `score_bpr_st` 后，W̄ LT ≈ −0.85%，不是「小幅 −0.37%」（写作口径风险）

`alpha_beta_by_selection.csv`：α=0.05,β=−0.10：

| 选边 | direct_st | sorting | LT total | **W̄_LT** |
|---|---|---|---|---|
| asym | −0.79s | +0.86s | +0.60s | **−0.365%** |
| score_bpr_st | −1.63s | +0.71s | +0.07s | **−0.849%** |

通勤侧（person-min +884→+95）summary §4.4 已写对；但**福利侧 W̄ LT 从 −0.37% 翻到 −0.85%，约 2.3×**。而 handoff §0.2 预期「W̄ LT 略不同，仍小幅负」、summary §5.1/§8.3「W̄ LT 0.15–0.37%，不宜过度 claim」——这些都是 **asym 口径**。主线一旦锁 bpr，§5.1 表、§8.3 的 band 都要更新，否则 slides/论文会低估自己模型给出的 LT 福利损失。这其实对叙事**有利**（bpr 把「LT 福利经 Π 受损」讲得更强），但必须改数字。

### 🟡 P3 — anchored floor：8.3% 的边 τ_obs < τ_ff（baseline 非严格复现观测）

`qsm_input_edges_square` 共 2642 边，**219 边（8.3%）τ_obs<τ_ff**（观测比自由流还快，median τ_ff/τ_obs=1.02，最坏 2.33×）。anchored 更新 `max(τ_ff, τ_obs·dev^λ)` 在这些边 baseline(dev=1) 会顶到 τ_ff>τ_obs：

- baseline 在这 8% 边上**settle 在 τ_ff 而非 τ_obs**；而锚点 Ξ_obs 是在**未 floor 的 τ_obs** 上算的 → 锚与 baseline 在这些边轻微不自洽。median τ/τ_obs≈1.000 仍成立（92% 边不受影响），summary 用「median」措辞已隐含规避。
- **很可能就是**所有 run 收敛残差恒等于 9.9969e-05、n_outer 恒为 65 的原因：拖慢收敛的是这批 floor 边的松弛（与处理哪 100 对无关），**不是政策本身**。也就是说政策相关部分实际比全局 1e-4 flag 更收敛——但建议在重跑时单独看 treated 边的逐迭代变化确认。
- 这些 floor 边对小 lane shock 可能**零响应**（仍被钉在 τ_ff）。slow 向通常是堵的（不在违规集），影响多为二阶；fast 向减 lane 时若落在 floor 边会低估其变化。建议在 QA 里报一行「treated 边里有几条命中 τ_ff floor」。

### 🟡 P4 — exog 层 CF2 用 legacy λ=δ₁=0.488，endog 用 anchored λ_phys=1.1（Phase 1 不是干净对照）

`run_structural_exogenous` 的 CF2（line 441）`delta1 = params.lambda_*theta ≈ 0.488`，即 legacy BPR；而 endog 主模型是 anchored λ=1.1。Phase 1「exog vs endog 同一 CF 排序是否翻转」若直接比这两者，差异里**混入了拥堵弹性口径差**（0.488 vs 1.1），不是纯「有无 feedback」。做 Phase 1 前应让 exog 层也用 anchored 口径（固定 τ、不跑外循环，但 τ shock 用 anchored 映射算一次），否则附录要显式标注口径不同。

### ⚪ P5 — `run_cf2_tidal` / `run_structural_counterfactuals` 硬编码 asym

`run_cf2_tidal`（counterfactual_levels.py:306）只调 `select_tidal_lane_pairs`（asym），无视 `MAINLINE_PAIR_SELECTION`。若 Phase 2「标准 CF 对比表」走 `run_structural_counterfactuals`，其 CF2 行会是 **asym 而非 bpr**，与主线不一致。建议让它走 `select_tidal_lane_pairs_by_rank(rank=pair_selection)` 或加 `pair_selection` 形参。（主 ST/LT 包走的是 `run_tidal_endogenous`，那条路是对的。）

### ⚪ P6 — 福利里 β-amenity 用 L_R（水平）而非 ℓ^R（份额）（cosmetic）

`equilibrium_solve` line 433–434 / `fixed_tau_model.welfare`：`u_θ = ūbar_θ · (ℓ_R·Lbar)^{βθ}`，sorting 方程 `_solve_lR_lF_given_tau` 用的是 `ℓ_R^{βθ}`（份额）。差一个常数因子 `Lbar^{βθ}`，对所有 i 相同，**比值/百分比不受影响**；但与 summary §1.5 写的 `(ℓ_i^L)^{βθ}` 字面不符。要么改代码用份额，要么在 §1.5 注明用 L_R（含 Lbar 常数）。无数值后果。

---

## 3. 给 Phase 0 重跑的 checklist

- [ ] 修 P1：`run_tidal_lane_diagnostics_gate5.main` 改 `solver_overrides_for_params(params, max_outer=args.max_outer)`；重跑时 tol 1e-4→1e-5 对齐主规格。
- [ ] 验 P3：重跑后看 baseline `max_log_change` 是否仍被 floor 边主导；输出「treated 边命中 τ_ff floor 计数」。treated 边逐迭代收敛单独确认。
- [ ] 改 P2 文档：summary §5.1 表加一列 bpr，§8.3 W̄ LT band 从「0.15–0.37%」更新为含 bpr 的「up to ~0.85%」。
- [ ] Phase 1 前处理 P4：exog 层 CF2 改 anchored 口径，或附录标注。
- [ ] Phase 2 前处理 P5：标准 CF 表的 CF2 走 by_rank(pair_selection)。
- [ ] （可选）P6：份额 vs 水平统一，纯文档。

---

## 4. 没有发现的问题（明确排除）

- inversion（`invert_fundamentals`）：交替 + gmean 归一 + valid-mask + chi 末端回收，逻辑正确；residual 字段保留可查拟合。
- `(I−A)^{-1}` 谱半径处理、EPS clip、damping 在 log 空间——数值实现稳健。
- 选边 `score_bpr_st` 公式 `Ξ_s·delay_s − Ξ_f·delay_f`（delay 对 τ_ff 截断非负）与 summary §1.6 一致。
- person-min、Wbar%、sorting wedge 的 sec/min/Lbar 换算全部对得上。
