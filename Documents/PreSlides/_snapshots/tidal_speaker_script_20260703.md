# Directional Congestion — Speaker Script (advisor-revised deck)

Source deck: `tidal_advisor_revision_segmentonly_20260703.tex` / `.pdf`, **46 frames** (31 main + 15 appendix), CES Chengdu 2026.
Supersedes `tidal_speaker_script_20260702.md`.

> **Latest changes baked in (2026-07-03 pm):**
> - The four-city OD histogram is **OD/trip-level**, so it moved **out of the opener into the appendix** (`appfourcity`, p.32), linked from the National Trip-Level slide. Title now goes straight to the roadmap.
> - **0.33 threshold appears only at the road-segment level** (deliberate 递进: trip/OD data show *prevalence*; segment data pick *candidate roads*).
> - **Gu, Jiang, Zhang & Zou (2021, AEJ:Applied) "Subways and Road Congestion"** added to the Literature slide (public transit).
> - Still open placeholder: **construction cost per lane-km** on Result 4 — flagged **[FILL cost]**.

---

## 整体逻辑脉络（汇报前先过一遍）

**一句话：** 高峰拥堵是有方向的；对称成本会系统性误测福利；在内生拥堵的一般均衡里，潮汐车道用零净车道公里实现可观的福利改善。

**贯穿全程的提醒：**
1. Motivation 的第二、三个 bullet 已经预告后面**两个练习**（symmetry 误测 + 潮汐反事实）——讲的时候反复回指："这就是我们待会儿做的第一个/第二个练习。"
2. 描述性证据的**递进**：全国 trip / OD 数据说明方向性**普遍**；到路段层面才谈 **0.33 阈值**和**具体候选路段**（北京 179 条）——因为只有路段数据能落到"政策能动手的方向级对象"。

**四段叙事（与分节一致）：**

| 段落 | Section / Frames | 要证明什么 | 图表角色 |
| --- | --- | --- | --- |
| **A. 方向性是事实** | Introduction, 帧 3–10 | 方向性在全国trip、城市内OD、路段三尺度都成立，根源是职住错配 | 从"普遍"收窄到"路段候选" |
| **B. 方向性进入模型** | Model, 帧 11–17 | 有向边时间 → 路径成本 → OD重力 → 拥堵反馈 → 福利 | 把 A 段测量对象变成可算的反事实输入 |
| **C. 估计 + 两个练习** | Estimation + Counterfactuals, 帧 18–30 | 反演基本面；对称误测(练习1) → 潮汐反事实(练习2) → 对比修路 | 结果页回答"便宜的政策值不值、选哪里" |
| **D. 收尾与延伸** | Next Steps, 帧 31 | 三条结论 + 深圳实地与参数识别 | 把结构结果接到下一步实证 |

**相对 7-02 版的结构变化：** 描述性重排为 **全国 trip → 通勤者暴露 → 路段 → 职住错配**（职住错配收在 Model 前）；潮汐示意图 + 政策定义**移到 Counterfactuals**；anchor 页**去掉潮汐**（基线不含潮汐）；四城图进**附录**。

---

## Framing (English one-liner)

This project starts from a simple measurement fact—peak-period congestion is directional—asks how much a symmetric commuting-cost model misses (exercise 1), and what a low-cost capacity reallocation, tidal lanes, delivers once route traffic, congestion feedback, and residence–workplace sorting are solved together (exercise 2).

---

## Notation reading guide

- $\alpha$ workplace agglomeration; $\beta$ residential crowding; $\theta$ cost elasticity; $\lambda$ speed–flow elasticity.
- $\tau_{ij}$ bilateral commuting cost; $\Xi_{kl}$ directed link traffic; $\bar W$ welfare index; $\phi_a,\psi_b$ upstream residence-access / downstream job-access weights (Result 5).
- Say "edge time from k to l", "commuting cost from i to j"—don't read every subscript. Say "Fréchet" (accent fixed).

---

## Main deck script

### Frame 1 — Title
Directional congestion and tidal-lane reallocation. One idea: traffic is not just congested, it is congested **in a direction**—morning inflow to job centres vs. evening outflow give the two sides of one corridor different costs. Matters for **measurement** (symmetry hides friction) and **policy** (tidal lanes move capacity without new pavement). Venue: CES Chengdu, 2026.

### Frame 2 — Roadmap
Five sections: **Introduction · Model · Estimation · Counterfactuals · Next Steps.** Bridge: descriptive evidence motivates a *directed* model; the directed model runs *two exercises*—(1) what symmetry misses, (2) the tidal-lane counterfactual. Plant the "two exercises" flag here.

---

### Frame 3 — Motivation
Three bullets, each seeding what comes later:
1. **"Directional congestion is widely observed on urban roads, particularly during peak hours."** Sub-point: commuting traffic is shaped by where workers live vs. where jobs are. (The *importance* bullet.)
2. **"The literature often ignores this asymmetry."** *Examples*: spatial-GE applications summarize road frictions at bilateral/route level (Ahlfeldt et al. 2015; Allen–Arkolakis 2022). *Except*: bottleneck models acknowledge peak-direction conditions (Vickrey 1969); reversible-lane operations sit outside spatial GE. → **hints exercise 1**.
3. **"Tidal lanes are a low-cost way to address it."** Sub-point: little is known about their short- and long-run effects. → **hints exercise 2**.
Right column keeps the clickable field video.
**Say aloud:** "Bullets two and three are the two exercises I'll do later."

### Frame 4 — This Paper
Three bullets = three contributions:
1. **"Measure directionality, using speed data at both the trip and the road-segment level."** National flow-weighted median SAR **0.878**; **58%** of flow ≤ 0.90; more congested cities have **a lower** SAR ($r=-0.48$, $n=99$). *(0.33 framing is saved for the segment slide — don't front-load it here.)*
2. **"Quantify what symmetry misses, using a spatial GE model."** Holding fundamentals/parameters fixed, replacing directed with reciprocal-symmetric costs changes $\bar W$ by **+1.10%** and moves **3.36%** residents / **6.15%** jobs.
3. **"Evaluate counterfactuals under endogenous-congestion GE."** Tidal top-100 = **reallocate one lane on each of 100 reciprocal corridors** toward the slow AM direction. $\bar W$ **+0.87% ST**, **+0.32% LT**. Capacity-expansion benchmark builds **1,100 lane-km**; tidal adds **zero**.

### Frame 5 — Literature (three bullets)
1. **Directional congestion** — mostly transport journals (peak-direction bottlenecks, reversible-lane operations). *This paper* measures directionality at national trip level and road-segment level with high-frequency speed data.
2. **Spatial equilibrium** — Krugman, Eaton–Kortum, Allen 2014, Ahlfeldt et al. 2015. *This paper* applies spatial GE with directional congestion and estimates counterfactual equilibria from high-frequency data.
3. **Transportation policy and congestion** — **demand vs. supply**: demand-side = congestion pricing & sorting (Vickrey 1969; Barwick et al. 2024; Yang et al. 2020); supply-side = road capacity, fundamental law, optimal networks (Duranton–Turner 2011; Fajgelbaum–Schaal 2020); **public transit as a substitute — subways relieve road congestion (Gu, Jiang, Zhang & Zou 2021, AEJ:Applied)**. *This paper* evaluates tidal lanes as one congestion-alleviation policy.
> *Gu et al. (2021): 45 subway-line launches in China; nearby rush-hour road speed +~4% in year one, largest on initially congested roads — a clean public-transit counterpart to our supply-side reallocation.*

### Frame 6 — Data
- **Directed speed** — national **trip-level** routes across Chinese cities (millions of trips); **road-segment** panel at 10-min frequency, one week in 2025.
- **Commuting flows** — Baidu 2022 home–work OD.
- **Road characteristics** — geometry, functional class, lane records.
Figure `speed_eg.png`: same corridor, two directions differ. Map to $L^R,L^F,L_{ij}$, $t^{obs}_{kl}$, $t^{ff}_{kl}$, $n_{kl}$, directed grid network.

### Frame 7 — National Trip-Level Directionality
**Definition** SAR $=\min/\max$ of directed costs; lower = stronger asymmetry.
- Flow-weighted median SAR **0.878**; **58%** of pair-flow ≤ 0.90 at AM peak.
- Higher Baidu commute-peak index ⇔ stronger AM asymmetry ($r=-0.48$).
- City-by-city histograms are in the appendix (`appfourcity`) — offer if asked ("Beijing/Chengdu run lower, Guangzhou higher").
Left histogram, right scatter. **Talking bridge (important):** "Trip-level data already show asymmetry is widespread—but they're not ideal for *choosing candidate roads*. That's where the road-segment data come in."

### Frame 8 — Commuters Face Directional Friction
("Observed" removed.)
- Unweighted grid-pair matrix: median SAR **0.879**; **57.5%** ≤ 0.90.
- **Commuter exposure:** counting commuters not grid-pairs, **4.4M** face median SAR **0.819**; **71.6%** on pairs ≤ 0.90.
Figure `pat_sz_flow_ratio_hist.png`. **Inner meaning:** commuters sit on the *asymmetric* part of the cost surface—weighting matters.

### Frame 9 — Road Segment Level Directionality  ← 递进落点
Subtitle now says it explicitly: *trip-level data show prevalence; road-segment data pin down candidate corridors.*
- Road segments (**Beijing AM, 3,670 two-way centerlines**): median SAR **0.884**; **54.7%** ≤ 0.90.
- **Tidal-lane candidates:** the local **traffic-bureau screen SAR ≤ 1/3** flags **179** Beijing centerlines (**4.9%**) as segments with tidal potential — the direction-level object a tidal policy can actually act on.
Panels: A local asymmetry map, B SAR distribution. **Say:** "0.33 is the cutoff the traffic bureau itself uses to consider a reversible lane; at the segment level we can point to the 179 roads that qualify." (Schematic + policy definition now live in Counterfactuals, frame 23.)

### Frame 10 — Directionality Arises from Jobs–Housing Mismatch
(Last Introduction slide, before Model; third bullet removed.)
- **Separation:** jobs cluster far more than residences—Duncan $D=0.26$ vs. random floor 0.004 (→ `appduncan`).
- **Direction:** AM flow radial-inward—**74.5%** of core-vs-reverse trips head to centre; inbound/reverse **2.9×**; 11/16 sectors >0.70 (→ `appinbound`).
Figure `pat_jobs_housing_balance.png`. **Transition:** separation + direction ⇒ asymmetric peak load ⇒ a directed model.

---

### Frames 11–17 — Model block
- **11 Environment.** Two directed links per road; night $t^{ff}$ may differ by direction; $t_{kl}$ endogenous in $\Xi_{kl}/n_{kl}$; $t_{kl}\neq t_{lk}$.
- **12 Individual location choice.** Route-level Fréchet: $V_{ij,r}=(u_iw_j/C_{ij,r})\varepsilon$; $C_{ij,r}$ = product of edge times; probabilities $\pi_{ij,r}$.
- **13 Bilateral costs & flows.** $\tau_{ij}=[\sum_r C_{ij,r}^{-\theta}]^{-1/\theta}$; gravity $L_{ij}$; $\tau_{ij}\neq\tau_{ji}$; $\Pi_i,P_j$ market access.
- **14 Amenity/productivity.** $w_j=\bar A_j(L_j^F)^\alpha$, $u_i=\bar u_i(L_i^R)^\beta$; market-clearing $l^R,l^F$. *(Slide note: $\theta=6.83,\alpha=0.05,\beta=-0.10$; exponents 1.68 residence / 0.66 workplace; $\beta<0$ disperses, $\alpha>0$ agglomerates.)*
- **15 Route vs. commuting cost.** $A_{kl}=t_{kl}^{-\theta}$, $B=(I-A)^{-1}$, $\tau_{ij}=B_{ij}^{-1/\theta}$—edge frictions compound through the network.
- **16 Feedback loop.** $\kappa^{kl}_{ij}$ → $\Xi_{kl}=\sum\kappa L_{ij}$ → $t_{kl}=t^{ff}_{kl}(\Xi_{kl}/n_{kl})^\lambda$; welfare from the **primitive Fréchet denominator** $\bar W=[\sum\tau^{-\theta}u^\theta w^\theta]^{1/\theta}$.
- **17 Equilibrium.** All objects mutually consistent; existence & uniqueness → **Allen–Arkolakis (2022) Prop. 1** (one line; → `appexist`).

---

### Frame 18 — Estimation: Data and Parameters
Data table maps sources → objects ($t^{obs}$ = weekday AM 07–09, May 19–25 2025; $t^{ff}$ = night 22–06). Parameters $\theta=6.83$; $\alpha=+0.05$; $\beta=-0.10$; **$\lambda=1.1$ (Barwick et al. 2024)—future versions will estimate it from field flow & speed data.**

### Frame 19 — Estimation: Anchor and Solve
(No tidal content—baseline estimated *without* tidal lanes.) (1) **invert fundamentals** $(\bar u,\bar A)$ to match the observed allocation at observed times; (2) **anchor congestion** to observed speeds, $t_{kl}=\max(t^{ff}, t^{obs}[(\Xi/\Xi^{obs})(n^{obs}/n)]^\lambda)$—times move *relative to observed baseline*; (3) **solve in levels**, compare two equilibria.

### Frame 20 — Short Run vs. Long Run
ST: pin $L^R,L^F$; re-equilibrate routes, $\Xi$, $t$, $\tau$. LT: allow sorting + congestion feedback. **Decomposition** of $\Delta\bar\tau^{LT}$: **ST direct** + **GE wedge** + **sorting**—why average route cost and $\bar W$ can diverge. (Advisor: "helpful.")

### Frame 21 — Baseline Fundamentals (inverted)
Maps `fund_ubar`, `fund_abar`. **corr($\bar A$, employment) 0.67** vs. **corr($\bar u$, residents) 0.30**: jobs pinned by productivity, residence access-driven. Tidal hint: treated corridors land on **high-flow employment bottlenecks** (TCS vs. $\Xi_s$ corr ≈ +0.91 on top-100). → `appfund`.

---

### Frame 22 — What Symmetry Misses  *(Exercise 1)*
**Say:** "First exercise." Hold network, lanes, fundamentals, parameters, solver, welfare formula fixed; replace only reciprocal-pair $t^{obs},t^{ff}$ by pairwise geometric means. Table: $\Delta\bar W$ **+1.10%**, residence dissim **3.36%**, workplace **6.15%**, OD cost +0.10%. **Inner meaning:** symmetry reallocates the baseline *before any shock*. Hub-productivity shock: symmetric model attenuates OD-cost response (+1.81% vs +1.93%).

### Frame 23 — Counterfactual: Tidal-Lane Reallocation  *(Exercise 2 setup)*
(Tidal schematic + policy definition now live here.) Label slow AM direction $s$, fast $f$; add one lane to $s$, remove one from $f$, hold $n_s+n_f$ fixed. Top-100 = 100 corridors by flow × excess-delay mass. Feasibility screened at road-segment level first. Schematic `tidal_lane_schematic.pdf`. **Say:** "Zero new pavement—only which direction the existing lanes serve at peak."

### Frame 24 — Where the Tidal Policy Lands
Figure `map_treated_overlay.png`; red edges = lane-receiving (slow) directions. **TCS** $=\Xi_s(t_s^{obs}-t_s^{ff})-\Xi_f(t_f^{obs}-t_f^{ff})$ = slow-minus-fast congestion-delay mass. Positive TCS ⇒ a lane toward $s$ targets the direction with more baseline flow × excess time. Policy hits the **central high-flow spine**.

### Frame 25 — Result 1. ST / GE / Sorting Cascade
Table: **ST direct −1.63 s/commuter** (−2,400 person-min/day) → **+GE wedge +0.99** → **+sorting +0.71** → **LT total +0.06 s** (+95 person-min/day). **Exposure:** 56.4% of baseline OD mass on shortest paths through treated corridors. Treated slow links save **20.3% ST / 13.6% LT**.
**If asked "only 0.06 s?":** citywide OD-weighted average; per-treated-link savings 13–20%; sorting + feedback reweight flows.

### Frame 26 — Result 2. Welfare: ST ↑, LT ↑
$\bar W$ **+0.87% ST**, **+0.32% LT**—up in both horizons. Route-cost row: −1.63 s ST → +0.06 s LT. **Inner meaning:** route-cost seconds = travel-time diagnostic; $\bar W$ = value of the *full residence–workplace opportunity set*.
- **Mode-switching caveat (bullet on slide):** all benefits here are priced as **driving-time savings**; the model does **not** endogenize mode switching, so gains to travelers who switch *into* driving are **outside this accounting** — $\bar W$ is a lower bound on that margin. Be ready to discuss.

### Frame 27 — Result 3. Long-Run Sorting
Totals fixed at $\bar L$; maps show demeaned share changes. Table: treated endpoints $\Delta\log\ell^R$ **+0.017**, $\ell^F$ −0.006; rest both negative. Maps `sort_lR`, `sort_lF`; black circles = endpoints. **Residential shares rise near endpoints** (lower OD costs raise market access); **employment gains** concentrate in a few red cells. Negative mean $\Delta\log\ell^F$ ≠ "no job gains"—localized.

### Frame 28 — Result 4. Transportation Policy Comparison
Table: **Tidal** +0.87% / +0.32% / +95 pm-day / **0 net lane-km** / near-zero capex; **Road Expansion** +1.26% / +0.44% / +75 / **1,140 lane-km** / **[FILL cost]**.
- Tidal lifts LT $\bar W$ +0.32% at zero net lane-km; matching that by building ≈**1,140 lane-km**.
- **[FILL cost]** advisor asked to **add construction cost per lane-km** — deck still shows "[TODO: verify local construction cost]." Until sourced, call Road Expansion a *capacity benchmark*. → `apppolicy` (also uniform-expansion row).

### Frame 29 — Result 5. Where to Put the Lanes
Figure `map_link_welfare_elast.png`; $d\bar W^\theta=dA_{ab}\phi_a\psi_b$ = edge gain × upstream residence access × downstream job access. Selection-rule table: **welfare elast. +0.35% (−50)**, **TCS +0.32% (+95)**, **speed-only +0.13% (+1,430)**. **Flow matters:** speed-only vs. elasticity rank corr **0.08**; flow×delay corr **0.65**. Recipe: flow + speed asymmetry + access weights.

### Frame 30 — Conclusion
Three messages (**no "first-order"**):
1. Directional congestion is **widespread**—national (median SAR 0.878; 58% ≤ 0.90), stronger within-city exposure (median 0.82), persists to segments.
2. Ignoring direction **mis-measures welfare**—symmetric surface shifts $\bar W$ +1.10%, moves 3.36% residents / 6.15% jobs.
3. Tidal lanes = **cost-effective welfare gains**—ST −1.63 s & $\bar W$ +0.87%; LT $\bar W$ +0.32%; comparable supply-side welfare needs ≈1,100 lane-km; target with flow + asymmetry + access.

### Frame 31 — Next Steps
1. **Reduced-form evidence on the effects of tidal lanes** — on-site video at deployed tidal segments; local flow–speed relation; RDD around the switch (→ `appfieldvisit`). Primary path to **estimate $\lambda$**.
2. **Identify/recover key structural parameters** $\alpha,\beta,\theta,\lambda$.
3. **Two cities, two peaks, multiple grids** — Beijing + Shenzhen, AM + PM, grid robustness.
**Closing:** a directional fact → a directed spatial equilibrium → a zero-net-capacity policy margin becomes quantitatively visible.

---

## Appendix & backup (use when asked)

- **32 `appfourcity` Directional congestion across cities** — *the four-city OD histogram that used to open the talk.* OD/trip-level AM directionality ratio by city (Beijing 0.81 / Guangzhou 0.86 / Chengdu 0.80 / Xi'an 0.82 median). Explicitly OD-pair cost, **not** segment speed. Linked from frame 7; use if the audience wants city-level detail.
- **33 `appdata` Data construction** — geometry→directed links; AM speeds; 3×3 km grid OD; largest connected component + detour gate; lanes $n_{kl}$ (1,103 values, p50 ≈ 3.37).
- **34 `appcost` Grid–grid travel cost** — `app_edgeproj_adjacent_tau.png`; shortest path per grid pair → $t_{kl}$ → $A_{kl}=t^{-\theta}$ → $\tau_{ij}=B_{ij}^{-1/\theta}$; $G=5$, $N=1{,}104$.
- **35 `appfund` Estimation algorithm** — baseline inversion matches $(L^R,L^F)$ (OD from gravity); counterfactual = outer loop on $t$ (anchored), inner loop on shares & market access.
- **36 `appduncan` Duncan benchmarks** — Beijing township $D=0.20$, rank 1/100 (median 0.09); $D$ aspatial, rises with size.
- **37 `appinbound` Radial AM inbound–reverse** — inbound 74.5%, ratio 2.9×, 11/16 sectors >0.70; non-circular.
- **38 `appmodelasym` Model-implied asymmetry** — `map_dir_asym.png`: model output $\Xi$ (not raw speed); median 2.9×, p90 20×.
- **39 `appparams` Parameter interpretation** — plain-language $\theta,\alpha,\beta,\lambda$; $\lambda$ currently 1.1 (Barwick), field estimation planned.
- **40 `appsens` Sensitivity summary** — vary $\alpha$ / $\beta$; **LT $\bar W$ positive in every converged cell**.
- **41 `appsensdetail` Sensitivity details** — full α–β grid; ST direct stable (~−1.63 s); $\lambda=0.57$ legacy; asymmetry-ratio selection weaker than TCS.
- **42 `appsens_theta` Theta sensitivity** — $\theta\in\{3,5,6.83,8,10\}$; LT $\bar W$ positive, magnitude falls as $\theta$ rises; compare Δ not levels.
- **43 `apppolicy` Policy definitions** — Tidal (0 lane-km) / Road Expansion (1,140 lane-km, +0.44%) / Uniform (1,092 lane-km, +0.10%); targeting vs. spreading.
- **44 `appelast` Welfare-elasticity score** — $dB=B(dA)B$ → $d\bar W^\theta=dA_{ab}\phi_a\psi_b$; ranking device only.
- **45 `appfieldvisit` Field visit** — six Shenzhen tidal-corridor photos; motivates video/RDD and $\lambda$ estimation.
- **46 `appexist` Existence & uniqueness** — Allen–Arkolakis Prop. 1; existence always (Brouwer); uniqueness sufficient $\alpha,\beta\le\frac12(\frac1\theta-\lambda)$; at $(0.05,0.10)$ both agglomerative → no convergence; mainline $\beta=-0.10$ admissible.

*(References via `\nobibliography`—no separate frame.)*

---

## Quick Q&A prep

| Question | Short answer |
| --- | --- |
| "Only 0.06 s saved?" | Citywide OD-weighted average; treated links save 13–20%; sorting + feedback reweight flows. |
| "Why is $\bar W$ up if time is flat?" | $\bar W$ values access to all i–j pairs, not mean commute time. |
| **"People switching modes into driving?"** | **Not endogenized; those benefits are outside the accounting, so $\bar W$ is a lower bound; flagged on Result 2 and as a next-step.** |
| "Why 0.33?" | The SAR cutoff the local traffic-control bureau uses to consider a reversible/tidal lane; at segment level, 179 Beijing centerlines (4.9%) qualify. |
| "Why only segment-level 0.33?" | Trip/OD data show prevalence but can't name a road; the direction-level candidate object lives on segments. |
| "Public transit as an alternative?" | Gu et al. (2021, AEJ:Applied): subways cut nearby road congestion; complements our supply-side reallocation. |
| "Why mean $\Delta\log\ell^F<0$?" | Unweighted cell mean; gains localized; rest cells lose share (fixed total). |
| "Tidal vs. building roads?" | Tidal 0 net lane-km for +0.32% LT; comparable welfare by expansion ≈1,140 lane-km (+0.44%); cost/lane-km **[FILL cost]**. |
| "Exist / unique?" | Allen–Arkolakis Prop. 1; existence always; uniqueness sufficient condition on $\alpha,\beta$ vs. $\theta,\lambda$; `appexist`. |
| "$\lambda=1.1$?" | Barwick Beijing speed–density; plan to estimate from Shenzhen field flow–speed. |

---

## Revision log
**July 3, 2026 (pm, v2)** — after advisor + student round:
- Four-city OD histogram **moved from opener to appendix** (`appfourcity`, p.32), linked from National Trip-Level (p.7); title → roadmap directly. All main frames shifted −1.
- **0.33 threshold kept only at road-segment level**; segment slide reframed as the 递进 landing point (Beijing 179 centerlines / 4.9% tidal candidates; 0.33 = bureau screen). Removed trip-level/This-Paper 0.33 asks.
- **Gu, Jiang, Zhang & Zou (2021, AEJ:Applied)** added to Literature (public transit) + `references.bib`; deck recompiled, all citations resolve.
- Kept from v1: two-exercises framing, Fréchet accent, λ future-estimation note, Result 2 mode-switching caveat, Conclusion "first-order" dropped, Next Steps "reduced-form evidence."
- Still open: **[FILL cost]** construction cost per lane-km on Result 4.

**July 3, 2026 (am, v1)** — initial rebuild for advisor-revised deck.
