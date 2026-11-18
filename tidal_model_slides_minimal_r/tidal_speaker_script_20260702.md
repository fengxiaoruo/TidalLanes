# Directional Congestion Slides Speaker Script

Source deck: `tidal.pdf` (same folder as this script), **45 slides** (44 content + References), compiled July 2, 2026.

---

## 整体逻辑脉络（汇报前先过一遍）

**一句话：** 高峰拥堵是有方向的；对称成本会系统性误测福利；在内生拥堵的一般均衡里，潮汐车道用零净车道公里实现可观的福利改善。

**四段叙事：**


| 段落             | Slides | 要证明什么                                                         | 图表在链条里的角色                  |
| -------------- | ------ | ------------------------------------------------------------- | -------------------------- |
| **A. 方向性是事实**  | 3–10   | 职住分离 → 通勤有方向；方向性在全国、城市内 OD、路段三个尺度都成立                          | 从空间结构到测量对象，逐层收窄到“政策能动手的地方” |
| **B. 方向性进入模型** | 11–22  | 有向边时间 → 路径成本 → OD 重力 → 拥堵反馈 → 福利；对称输入会改基准分配                   | 模型把 A 段的测量对象变成可算的反事实输入     |
| **C. 潮汐政策效应**  | 23–28  | TCS 选廊道 → ST 省时 → LT sorting 部分抵消平均耗时 → 但 $\bar W$ 仍为正；对比修路扩张 | 结果页回答“便宜的政策值不值、选哪里”        |
| **D. 收尾与延伸**   | 29–30  | 三条结论 + 深圳实地与参数识别                                              | 把结构结果接到下一步实证               |


**图表逻辑链（按出现顺序）：**

1. **speed_eg.png（Data）** — 数据本身是有向的；为后面 $t_{kl}^{obs}$、$t_{kl}^{ff}$ 奠基。
2. **pat_jobs_housing_balance.png（职住错配）** — 空间上 jobs 比 residents 更集中；这是“早高峰向内、晚高峰向外”的结构来源。
3. **pat_national_ratio_hist + pat_congestion_am_scatter（全国）** — 方向性不是北京特例；越拥堵的城市 asymmetry 越强。
4. **pat_sz_flow_ratio_hist（深圳 OD 暴露）** — 真实通勤者比“所有网格对”更暴露在低 SAR 上；测量要加权。
5. **pat_bj_centerline_map + hist + tidal_lane_schematic（路段）** — 方向性在 centerline 上可测；第三列示意图说明潮汐车道如何在不增宽路面的前提下改方向容量。
6. **fund_ubar / fund_abar（基本面）** — 反演出的 $\bar u,\bar A$；就业更贴 productivity，居住更贴 access。
7. **map_treated_overlay（TCS 选址）** — 慢向 flow×excess delay 最大的廊道；红点=收车道方向。
8. **sort_lR / sort_lF（Sorting）** — 去均值份额变化；endpoint 节点住宅升、就业变化更集中。
9. **map_link_welfare_elast（弹性地图）** — 局部排名：边增益 × 上游居住 access × 下游就业 access。

---

## Framing (English one-liner)

This project starts from a simple measurement problem—peak-period congestion is directional—then asks how much a symmetric commuting-cost model misses, and what a low-cost capacity reallocation (tidal lanes) can deliver once route traffic, congestion feedback, and residence–workplace sorting are solved together.

---

## Greek Letter And Notation Reading Guide

Use simple English names when presenting. Pinyin cues are for rehearsal only.

- $\alpha$: workplace agglomeration; $\beta$: residential crowding; $\theta$: cost elasticity; $\lambda$: speed–flow elasticity
- $\tau_{ij}$: bilateral commuting cost; $\Xi_{kl}$: directed link traffic; $\bar W$: welfare index
- $\phi_a$, $\psi_b$: upstream residence-access and downstream job-access weights (Result 5 / appendix)

When speaking: say “edge time from k to l”, “commuting cost from i to j”, “residents in i”—do not read every subscript.

---

## Main Deck Script

### Slide 1. Title — Directional Congestion

Today I talk about **directional congestion** and **tidal-lane reallocation**.

The core idea: traffic is not only congested—it is congested **in a direction**. Morning inflow to job centres and evening outflow create different travel costs on the two sides of the same corridor.

This matters for measurement (symmetric costs hide friction) and for policy (tidal lanes reallocate lanes without new road width).

**Transition:** Empirical patterns first, then the model, then counterfactuals.

---

### Slide 2. Roadmap

Eight sections on the slide; the logic is tighter: **motivation → data & directionality → model → estimation → results → next steps**.

The bridge: descriptive evidence motivates a **directed** model; the directed model evaluates a **directional** policy.

---

### Slide 3. Motivation

**Inner meaning:** Why directionality is an economic object, not a traffic-engineering detail.

Peak-direction imbalance comes from where people live vs. where jobs are. Return trips reverse flows, so welfare-relevant daily cost includes **both commute and return**.

Tidal lanes reallocate existing capacity: +1 lane slow direction, −1 lane fast direction, fixed total width. That opens a **sorting** margin—access changes can move residence and jobs.

**Transition:** Three contributions: measure directionality, quantify symmetry error, evaluate tidal lanes in GE.

---

### Slide 4. This Paper

**Three bullets = three papers inside one talk.**

1. **Measure directionality.** National median SAR 0.878; 58% of flow below 0.90; more congested cities → lower SAR ($r=-0.48$). Within-city OD exposure stronger (0.819); segments comparable (0.884).
2. **Quantify what symmetry misses.** Same model, symmetric costs: $\bar W$ shifts **+1.10%**, 3.36% of residents and 6.15% of jobs move—**before any policy**.
3. **Evaluate tidal lanes.** Top-100 TCS corridors; ST $\bar W$ **+0.87%**, LT **+0.32%**, **zero net lane-km** vs. ~1,100 lane-km for comparable expansion welfare.

**Transition:** Literature placement, then data.

---

### Slide 5. Literature

**Inner meaning:** Place the paper in two literatures, then state what is **new** and what is still **calibrated**.

**Spatial equilibrium and agglomeration.** Krugman, Eaton–Kortum, Allen–Arkolakis, Ahlfeldt et al.—quantitative spatial models where access, productivity, and location interact. Allen–Arkolakis (2022) is the direct frame for **transport in spatial GE with endogenous congestion**.

**Transportation policy and congestion.** Vickrey, Duranton–Turner, Fajgelbaum–Schaal on congestion theory and optimal networks; Barwick et al. (2024) and Yang et al. (2020) on Beijing congestion, pricing, and sorting.

**Calibration and contribution.** Beijing speed–density estimates motivate **$\lambda=1.1$** (Barwick). The forward plan: **estimate $\lambda$ directly** from field flow and speed data at tidal-lane deployments (links to Next Steps / Field Visit).

**Our margin:** not “build everywhere”—**reallocate lanes across directions** at fixed total width. That requires directed costs and directed traffic.

**Transition:** Data inputs that implement directed measurement.

---

### Slide 6. Data

**Figure (speed_eg.png):** Example of **directional** Baidu speeds—same corridor, two directions can differ.

Three inputs: (1) Baidu 2022 home–work OD; (2) 2025 directed speed panel (10-min, one week); (3) geometry, functional class, lanes.

These map to $L_i^R, L_j^F, L_{ij}$, $t_{kl}^{obs}$, $t_{kl}^{ff}$, $n_{kl}$, and the directed grid network.

**Transition:** First fact—spatial separation gives commuting a direction.

---

### Slide 7. Jobs–Housing Mismatch

**Figure (pat_jobs_housing_balance.png):** Jobs cluster more than residences on the 3 km grid—residential mass in outer cells, employment mass in the core.

**Three bullets—read as a chain:**

1. **Separation.** Duncan $D=0.26$ vs. random-allocation floor 0.004—far from balanced mixing. Cross-city benchmark in appendix (Beijing ranks high on separation).
2. **Direction.** AM flows are **radially inbound**: 74.5% of core-vs-reverse trips head toward the job-weighted centre; inbound/reverse ratio **2.9×** (detail: appendix polar plot).
3. **Implication.** Separation + direction → peak load is asymmetric → **opening for tidal lanes** (reallocate toward the slow peak direction).

**Do not say** the old “64.8% / −4.7 km” stats—they are not on this slide anymore.

**Transition:** From spatial structure to direct asymmetry measurement.

---

### Slide 8. National Directional Asymmetry

**Definition:** SAR = min(cost_ij, cost_ji) / max(…); lower = stronger directionality.

**Left figure (histogram):** Distribution across top-100 congested cities—mass below 0.90, not a tail event.

**Right figure (scatter):** Higher commute-peak congestion index ↔ lower SAR ($r=-0.48$). Directionality co-moves with congestion severity.

**Inner meaning:** This is a **national feature**, not Beijing idiosyncrasy.

**Transition:** National pattern → are real commuters exposed within a city?

---

### Slide 9. Observed Commuters Face Directional Friction

**Figure (pat_sz_flow_ratio_hist):** Distribution of SAR **weighted by Shenzhen commuter flows**.

Two statistics side by side:

- All grid pairs: median SAR 0.879; 57.5% ≤ 0.90.
- **Observed commuters:** 4.4M face median SAR **0.819**; **71.6%** on pairs ≤ 0.90.

**Inner meaning:** Commuters sit on the **asymmetric** part of the cost surface—weighting matters for “first-order exposure.”

**Transition:** OD pairs are not where policy acts; segments are.

---

### Slide 10. Segment-Level Policy Margin

Three panels—read left to right as **measurement → distribution → mechanism**.

**A. Map:** Where asymmetric segments sit in Beijing—spatially clustered on the network, not uniform noise.

**B. Histogram:** Segment-level SAR distribution—median 0.884; 54.7% ≤ 0.90.

**C. Schematic (`tidal_lane_schematic.pdf`):** How a tidal lane **acts**—same physical road width, one lane shifted from the fast direction to the slow direction during the peak window. This is the policy object the model implements.

**Inner meaning:** Directionality is measurable at the segment level; the schematic makes the **zero net lane-km** margin visually concrete before the model section.

**Talking line:** “We measure asymmetry on centerlines, and the policy moves capacity across directions without building new pavement.”

**Transition:** Facts justify a model where every edge has a direction.

---

---

### Slides 11–17. Model Block

**Slide 11 — Environment.** Two directed links per road; night $t^{ff}$ can differ by direction; tidal policy shifts lanes at fixed $n_{kl}+n_{lk}$; $t_{kl}$ endogenous in $\Xi_{kl}/n_{kl}$.

**Slide 12 — Individual choice.** Route-level Fréchet choice; $C_{ij,r}$ = product of edge times. *Why routes:* traffic loads links through paths.

**Slide 13 — Bilateral costs.** $\tau_{ij}$ aggregates routes; gravity for $L_{ij}$; $\tau_{ij}\neq\tau_{ji}$ in general.

**Slide 14 — Agglomeration/crowding.** $w_j=\bar A_j(L_j^F)^\alpha$; $u_i=\bar u_i(L_i^R)^\beta$. Sorting margin: access shocks move both residence and jobs.

**Slide 15 — Network matrix.** $A_{kl}=t_{kl}^{-\theta}$, $B=(I-A)^{-1}$, $\tau_{ij}=B_{ij}^{-1/\theta}$—edge frictions compound through the network.

**Slide 16 — Feedback loop.** $\kappa^{kl}*{ij}$ → $\Xi*{kl}$ → $t_{kl}=t_{kl}^{ff}(\Xi_{kl}/n_{kl})^\lambda$; $\bar W$ from Fréchet denominator (full opportunity set, not mean time only).

**Slide 17 — Equilibrium.** All objects mutually consistent; directed throughout. Existence and uniqueness follow **Allen–Arkolakis (2022), Proposition 1** (details: appendix `appexist`). One line only on the main slide—do not derive conditions unless asked.

**Model-block transition:** How objects enter data and counterfactuals.

---

---

### Slide 18. Estimation: Data and Parameters

Table maps data → model objects. Parameters: $\theta=6.83$, $\alpha=0.05$, $\beta=-0.10$, $\lambda=1.1$ (Barwick 2024 for $\lambda$; field estimation planned—see Literature / Next Steps).

Say “theta, alpha, beta, lambda” aloud; don’t dwell on symbols.

---

### Slide 19. Estimation: Anchor and Counterfactuals

Four steps: (1) invert $\bar u,\bar A$ to match baseline marginals; (2) **anchor** congestion to observed speeds (eq. on slide); (3) tidal = shift one lane slow ← fast; (4) solve full GE, compare equilibria.

**Key sentence:** Counterfactual times move **relative to observed baseline**, not from free-flow alone.

---

### Slide 20. Short Run vs. Long Run

**ST:** fix $L^R,L^F$; re-equilibrate routes, $\Xi$, $t$, $\tau$.

**LT:** allow sorting + congestion feedback.

**Decomposition:** ST direct | GE wedge | sorting—explains why **route-cost averages** and **$\bar W$** can diverge.

---

### Slide 21. Baseline Fundamentals (inverted)

**Left map ($\bar u_i$):** Inverted residential amenity—where households would like to live given access.

**Right map ($\bar A_j$):** Inverted workplace productivity—where jobs are fundamentally productive.

**corr($\bar A$, employment) = 0.67** vs. **corr($\bar u$, residents) = 0.30** → jobs pinned by productivity; residence more access-driven.

Treated TCS corridors = **high-flow employment bottlenecks** (TCS vs. slow-direction $\Xi$: corr ≈ +0.91 on top-100).

**Transition:** Before policy—what if we force symmetry on costs?

---

### Slide 22. What Symmetry Misses

**Experiment:** Only change—replace each reciprocal pair’s $t^{obs}, t^{ff}$ by geometric means; everything else fixed.

**Table:** $\Delta\bar W=+1.10%$; residence dissimilarity 3.36%; workplace 6.15%; OD cost +0.10%.

**Inner meaning:** Symmetry is not harmless normalization—it **reallocates the baseline** before any tidal shock. Hub shock example: symmetric model attenuates OD-cost response.

**Transition:** With directed baseline, evaluate a directional policy.

---

### Slide 23. Where the Tidal Policy Lands

**Figure (map_treated_overlay):** Red edges = slow direction receiving +1 lane among TCS top-100.

**TCS formula:** slow-minus-fast **congestion-delay mass** $\Xi_s(t_s^{obs}-t_s^{ff})-\Xi_f(t_f^{obs}-t_f^{ff})$—flow × excess travel time in the slow direction minus the same in the fast direction.

**Inner meaning:** Policy targets the **central high-flow spine**, not random slow links.

---

### Slide 24. Result 1 — ST / GE / Sorting Cascade

**Table logic:** Start ST direct **−1.63 s/commuter** (−2,400 person-min/day) → add GE wedge +0.99 s → add sorting +0.71 s → LT total **+0.06 s** (+95 person-min/day).

**How to say the “only 0.06 seconds” question:**

- This is a **citywide OD-weighted average** across all baseline road commuters—not per-user time on treated links.
- On treated slow links: **−20.3%** ST, **−13.6%** LT—local engineering savings are real.
- Sorting + congestion feedback **re-weight** who travels where; average commute time can net to ≈0 while $\bar W$ stays up (next slide).

**Exposure:** 56.4% of OD mass on shortest paths through treated corridors.

---

### Slide 25. Result 2 — Welfare: ST ↑, LT ↑

**Main message:** $\bar W$ **+0.87%** ST, **+0.32%** LT—welfare rises in **both** horizons.

Route-cost row: −1.63 s ST → +0.06 s LT (decomposition in table).

**Inner meaning:** Route-cost seconds = OD-weighted travel-time diagnostic. $\bar W$ = value of the **full residence–workplace opportunity set**. LT sorting absorbs average time savings, but access gains keep welfare positive.

**Do not** re-introduce the full welfare formula on this slide—the slide intentionally stays verbal.

---

### Slide 26. Result 3 — Long-Run Sorting

**Table (top):** Mean $\Delta\log\ell$ by treated-corridor **endpoint nodes** vs. rest—unweighted cell means, not population-weighted.

- Endpoints: $\ell^R$ **+0.017**; $\ell^F$ −0.006.
- Rest: both negative (reshuffling—city totals fixed at $\bar L$).

**Maps:** Demeaned $\Delta\log\ell^R$, $\Delta\log\ell^F$; black circles = endpoint nodes on treated corridors.

**Inner meaning:**

- **Residential shares rise near endpoints**—lower OD costs raise residential market access ($\Pi_i$).
- **Employment gains** concentrated in a smaller set of red cells; $\bar A_j$ fixed but $w_j=\bar A_j(L_j^F)^\alpha$ updates.
- Negative mean $\Delta\log\ell^F$ in table ≠ “no job gains anywhere”—gains are **localized**; demeaned logs + many small negative cells pull the mean down.

**Transition:** Is this a good deal vs. building lanes?

---

### Slide 27. Result 4 — Transportation Policy Comparison

**Main table (two policies only):**


| Policy         | ST $\bar W$ | LT $\bar W$ | LT route cost      | Net lane-km |
| -------------- | ----------- | ----------- | ------------------ | ----------- |
| Tidal          | +0.87%      | +0.32%      | +95 person-min/day | **0**       |
| Road Expansion | +1.26%      | +0.44%      | +75 person-min/day | **1,140**   |


**Road Expansion (main slide name):** Re-rank corridors by baseline **flow × excess delay**; add **one lane in each direction** on top-100—physical capacity build-out.

**Three talking points:**

1. Tidal delivers **+0.32% LT $\bar W$ at zero net lane-km**—cheap margin.
2. Comparable LT welfare via expansion needs **~1,140 lane-km** and reaches +0.44%.
3. Expansion row is a **capacity benchmark**, not a construction-cost estimate. (Appendix also has uniform expansion for “same lane-km budget, spread everywhere”.)

**Do not say “Q-delay expansion” on the main slide**—that label is appendix-only; main slide uses **Road Expansion**.

---

### Slide 28. Result 5 — Where to Put the Lanes

**Figure (map_link_welfare_elast):** Spatial distribution of local **welfare elasticity**—where one marginal lane would matter most for $\bar W$.

**Formula (appendix detail):** $d\bar W^\theta = dA_{ab}\phi_a\psi_b$ — edge gain × upstream residence access × downstream job access.

**Table (top-100 selection rules):**


| Rule               | LT $\bar W$ | person-min/day |
| ------------------ | ----------- | -------------- |
| Welfare elasticity | **+0.35%**  | −50            |
| TCS                | +0.32%      | +95            |
| Speed only         | +0.13%      | +1,430         |


**Inner meaning:**

- **Flow matters:** speed-only vs. welfare elasticity rank correlation **0.08**; flow×delay vs. elasticity **0.65**.
- Welfare elasticity and TCS both put capacity on the **high-flow spine**; speed-only picks slow but **low-flow** edges—bad for welfare and average time.
- Score is a **local ranking device**; reported LT $\bar W$ still from full GE re-solve.

**Targeting recipe:** combine **flow**, **speed asymmetry**, and **access weights** ($\phi$, $\psi$).

---

### Slide 29. Conclusion

Three numbered messages—match slide exactly:

1. **Directional congestion is widespread**—national, OD exposure, segments.
2. **Ignoring direction mis-measures welfare**—+1.10% $\bar W$, 3.36% / 6.15% location moves.
3. **Tidal lanes: cost-effective welfare gains**—ST −1.63 s & $\bar W$ +0.87%; LT $\bar W$ +0.32%; ~1,100 lane-km to match via expansion; target using flow + asymmetry + access.

---

### Slide 30. Next Steps

1. **Shenzhen field measurement**—video, counts, speeds; RDD around switch times (appendix photos). Primary path to **estimate $\lambda$** from local flow–speed at tidal deployments.
2. **Identify structural parameters**—$\alpha$, $\beta$, $\theta$, $\lambda$; calibrate or estimate from data rather than literature imports alone.
3. **Two cities, two peaks, multiple grids**—Beijing + Shenzhen, AM + PM, grid robustness.

**Closing line:** A directional fact → directed spatial equilibrium → a zero-net-capacity policy margin becomes quantitatively visible.

---

## Appendix And Backup Script

Use only when asked. Slide numbers match current **45-frame** deck.

### Slide 31. Appendix: Data Construction

Pipeline: geometry → directed links; AM speeds; 3×3 km grid OD; largest connected component + detour gate; lane lookup ($n_{kl}$, median ≈ 3.37).

---

### Slide 32. Appendix: Grid–Grid Travel Cost

**Figure (app_edgeproj_adjacent_tau.png):** Distribution of AM travel times on **adjacent grid connectors**—shows routed directional costs entering the model.

Shortest path per grid pair on road graph → $t_{kl}$ on connector → $A_{kl}=t_{kl}^{-\theta}$ → $\tau_{ij}=B_{ij}^{-1/\theta}$.

---

### Slide 33. Appendix: Estimation Algorithm

**Baseline inversion:** Match $(L_i^R,L_j^F)$ at observed times; recover $\bar u_i^\theta,\bar A_j^\theta$; OD from gravity, not direct OD fit.

**Counterfactual solve:** Outer loop on $t_{kl}$ with anchored congestion; inner loop on shares and market access.

*(Old “two-column Inverting Fundamentals + Solver” slides are merged here.)*

---

### Slide 34. Appendix: Jobs–Housing Dissimilarity Benchmarks

**Figure (app_duncan_cross_city.png):** Cross-city Duncan $D$—Beijing **0.20**, rank **1/100** (median 0.09).

**Caveat:** $D$ is aspatial; rises with city size—Beijing’s lead is not purely a unit-count artefact, but $D$ ignores distance.

**Use when asked:** “How extreme is Beijing’s separation vs. other congested cities?”

---

### Slide 35. Appendix: Radial AM Inbound–Reverse Asymmetry

**Figure (app_inbound_sector.png):** Polar plot—share of AM trips inbound vs. reverse by home-bearing sector.

Road-mode OD; inbound = home ≥15 km from job-weighted centre & work ≤10 km.

**Stats:** Pooled inbound **74.5%**; ratio **2.9×**; 11/16 sectors >0.70.

**Inner meaning:** Directionality is not circular/statistical artefact—inbound and reverse share one centre.

---

### Slide 36. Appendix: Model-Implied Directional Asymmetry

**Figure (map_dir_asym.png):** **Model output** $\Xi$, not raw speed—$\log_{10}$ high/low flow ratio; width = total flow.

Median imbalance **2.9×**; p90 **20×**; spine concentration.

**Use when asked:** “Does the equilibrium model reproduce directional loading?”

---

### Slide 37. Appendix: Parameter Interpretation

Plain-language $\theta,\alpha,\beta,\lambda$ and how they interact in sorting.

**$\lambda$:** Currently 1.1 from Barwick; Literature slide flags plan to estimate from **field flow–speed data** at tidal deployments.

---

### Slide 38. Appendix: Sensitivity Summary

Vary $\alpha$ (fixed $\beta=-0.10$) and $\beta$ (fixed $\alpha=0.05$). **LT $\bar W$ positive in every converged cell**; route-time stats move more because sorting shifts OD weights.

Footer links to **Sensitivity Details** and **Theta Sensitivity** (`appsensdetail`, `appsens_theta`).

---

### Slide 39. Appendix: Sensitivity Details

Full $\alpha$–$\beta$ grid; ST direct stable (~−1.63 s, +0.87% ST $\bar W$).

**$\theta$ grid** on dedicated next slide—each $\theta$ **re-inverts fundamentals and re-solves GE** (not a rescaling exercise).

$\lambda=0.57$: legacy lower-slope robustness, not mainline.

Asymmetry-ratio selection rule weaker than TCS—can flip LT $\bar W$ negative.

---

### Slide 40. Appendix: Theta Sensitivity

**Table:** $\theta\in3,5,6.83,8,10$; columns ST $\bar W$, LT $\bar W$, resident dissim, job dissim.


| $\theta$ | ST $\bar W$ | LT $\bar W$ |
| -------- | ----------- | ----------- |
| 3        | +1.99%      | +0.75%      |
| 5        | +1.19%      | +0.45%      |
| **6.83** | **+0.87%**  | **+0.32%**  |
| 8        | +0.74%      | +0.27%      |
| 10       | +0.59%      | +0.21%      |


**Inner meaning:**

- LT $\bar W$ **stays positive** across the grid; magnitudes **fall** as $\theta$ rises (households respond more sharply to costs, so the marginal gain from one reallocation is smaller in percentage terms).
- Resident dissim **falls** and job dissim **rises** with $\theta$—sorting pattern shifts with cost sensitivity.
- **Baseline $\bar W$ levels are not comparable across $\theta$**—each row re-inverts fundamentals. Compare **$\Delta$ / percentage changes**, not levels.

**If asked “does lower $\theta$ mean tidal lanes work better?”:** Percentage welfare gains are larger at lower $\theta$, but that reflects a different calibration of the whole spatial system—not a free lunch from picking a smaller $\theta$.

---

### Slide 41. Appendix: Policy Definitions and Selected Effects

**Three policies:**

1. **Tidal reallocation** — TCS top-100; +1 slow, −1 fast; 0 lane-km.
2. **Road Expansion** — flow×delay top-100; +1 lane **both** directions; 1,140 lane-km; LT $\bar W$ +0.44%.
3. **Uniform expansion** — +2.4% lanes everywhere to match ~1,092 lane-km budget; LT $\bar W$ +0.10%.

**Contrast:** targeting vs. spreading the same build budget.

---

### Slide 42. Appendix: Welfare-Elasticity Score

Derivation: $dB=B(dA)B$ → $d\bar W^\theta=dA_{ab}\phi_a\psi_b$.

$\phi_a$ = upstream residence-access weight; $\psi_b$ = downstream workplace-opportunity weight.

Ranking device only—full GE for levels.

---

### Slide 43. Appendix: Field Visit

**Six photos, three columns** — Shenzhen tidal-lane corridors on the ground.

**Purpose:** (1) confirm implemented institutional setting; (2) motivate video/RDD next step to validate local flow–speed response; (3) **estimate $\lambda$** from on-site flow and speed (per Literature slide).

**If asked how this connects:** Structural counterfactual uses observed speeds + modelled lane shifts; field work validates the **local** mechanism and identifies $\lambda$ on implemented corridors.

---

### Slide 44. Appendix: Existence and Uniqueness (`appexist`)

**Source:** Allen–Arkolakis (2022), Proposition 1.

**Existence:** On a strongly connected network, a strictly positive equilibrium always exists (Brouwer fixed point).

**Uniqueness (sufficient):** For $\alpha,\beta\in[-1,1]$,
$\alpha\le\frac12(\frac1\theta-\lambda)$ and $\beta\le\frac12(\frac1\theta-\lambda)$.

**Intuition:** Agglomeration spillovers ($\alpha$, $\beta$) must not be so strong that the equilibrium operator has spectral radius above one; higher congestion $\lambda$ tightens the bound.

**Example on slide:** At $(\alpha,\beta)=(0.05,0.10)$—both spillovers agglomerative—the iteration **fails to converge**. Mainline uses $\beta=-0.10$ (dispersion), which is inside the admissible region.

**Use when asked:** “Is the counterfactual well posed?” Point to Proposition 1 and note the condition is **sufficient, not necessary**.

---

### Slide 45. References

Bibliography slide (`references.bib`)—natbib, author–year. **Do not read aloud.** Say: “Full references are on the last slide; happy to point to any citation.”

---

## Quick Q&A Prep


| Question                                 | Short answer                                                                                                                                          |
| ---------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| “Only 0.06 s saved?”                     | Citywide OD-weighted average; treated links save ~13–20%; sorting + feedback re-weight flows.                                                         |
| “Why is $\bar W$ up if time is flat?”    | $\bar W$ values access to all i–j pairs, not mean commute time.                                                                                       |
| “Why mean $\Delta\log\ell^F<0$?”         | Unweighted cell mean; gains localized; rest cells lose share (fixed total).                                                                           |
| “Car-only vs car+bus OD?”                | Model baseline uses road-mode OD; pattern figures may differ—harmonization in progress (mode-ratio scaling).                                          |
| “Q-delay vs Road Expansion?”             | Main slide **Road Expansion**; appendix adds uniform expansion for same lane-km budget.                                                               |
| “Does equilibrium exist / is it unique?” | Allen–Arkolakis Prop. 1; existence always; uniqueness sufficient condition on $\alpha,\beta$ vs. $\theta,\lambda$; see `appexist`.                    |
| “What if $\theta$ is different?”         | Dedicated $\theta$ grid: LT $\bar W$ stays positive; magnitudes decline with $\theta$; re-invert fundamentals each time—compare $\Delta$, not levels. |
| “Where does $\lambda=1.1$ come from?”    | Barwick Beijing speed–density; plan to estimate from Shenzhen field flow–speed.                                                                       |


---

## Revision log

**July 2, 2026 (pm)** — synced to latest slides:

- Slide count **42 → 45** (+ `appsens_theta`, `appexist`, References).
- **Slide 5 Literature:** natbib structure; Vickrey/Duranton/Fajgelbaum; $\lambda$ from Barwick + field estimation plan.
- **Slide 10:** third panel `tidal_lane_schematic.pdf` (“How a tidal lane acts”).
- **Slide 17:** existence/uniqueness → Allen–Arkolakis Prop. 1 + `appexist` appendix.
- **Slide 40 (`appsens_theta`):** full $\theta$ table and interpretation (re-invert each $\theta$).
- **Slide 44 (`appexist`):** existence/uniqueness backup script.
- Fixed $\kappa^{kl}_{ij}$ typo in model-block notes.

**July 2, 2026 (am)** — initial 42-slide sync (Duncan/inbound, Road Expansion, field visit, chart logic chain).