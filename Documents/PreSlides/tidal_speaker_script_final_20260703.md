# Directional Congestion — Full Speaker Script (round-2 deck, final)

Deck: `tidal_advisor_revision_round2_20260703.pdf`, 45 frames (31 main + 14 appendix), CES Chengdu 2026.
Delivery-ready prose. Numbers match the compiled deck. Timing target ≈ 18–20 min main + Q&A.

---

## Opening story (前 90 秒，重点)

Three beats. Aim: the audience *sees* the object before any model.

**Beat 1 — the lived fact (hook).**
"Every one of us has sat in a morning jam heading *into* downtown while the *other* side of the same road runs almost free — and by evening it flips. Congestion is not just a level. It has a **direction**."
Pause. Then: that direction changes the home-to-work commuting cost relative to the return trip, so it changes the access matrix inside any spatial equilibrium model.

**Beat 2 — why it exists and why it's missed.**
"That direction is not random. Jobs cluster in the core, housing spreads out, so the morning peak is radial-inward. Yet most spatial-GE models collapse a road into a single bilateral cost — the two directions get averaged away. Transport research does measure peak-direction speeds and studies reversible lanes, but those objects rarely enter a spatial-GE counterfactual with endogenous congestion and sorting."
Wording stays conservative: spatial-GE work *abstracts from* segment-level directionality — a modeling choice, not an error.

**Beat 3 — the policy and the three contributions.**
"There is already a cheap answer on the street: a **tidal lane** — at peak, move one lane from the empty direction to the jammed one, no new pavement. This paper does three things: **measure** how directional congestion is, from national trips down to individual road segments; **quantify what a symmetric model misses**; and **evaluate tidal lanes** in equilibrium, benchmarked against actually building roads."

Optional cold-open: play the clickable field video on the Motivation slide first — the on-street lane reversal is the most direct picture of "zero-construction directional capacity" — then deliver Beat 1 over it.

---

## Frame 1 — Title

"Today: directional congestion and tidal-lane reallocation. The core fact — a road is not simply congested or clear; at peak, the same road can be jammed one way and fast the other. That direction-specific cost matters because commuters choose residence, workplace, and route; and because one policy, tidal lanes, reallocates capacity across directions without adding width."

## Frame 2 — Roadmap

"Five parts: motivation, data, model, estimation, counterfactuals. The logic is cumulative — the facts show where directionality appears, the model shows how it enters equilibrium, the counterfactual asks what a directed capacity policy delivers. The model will run **two exercises**: what symmetry misses, and the tidal counterfactual." (Plant the two exercises.)

## Frame 3 — Motivation

"Peak-direction imbalance is widely observed, and the reason is spatial — workers and jobs are not co-located, so peak demand has a strong direction.
Most spatial-GE applications summarize frictions at the bilateral or route level; that abstracts from segment-level directionality.
The exception is the transport literature — peak-period bottlenecks and reversible-lane operations. We connect that measurement object to a spatial-equilibrium counterfactual.
Tidal lanes are why this matters for policy: they move capacity to the slow direction at fixed width, so their welfare effect can't be read off a symmetric road cost." (Bullets 2–3 = the two exercises.)

## Frame 4 — This Paper

"Three things.
First, **measure** directionality with trip- and segment-level speed data. National trip level: flow-weighted **median speed asymmetry ratio 0.878**. Beijing segment level: the one-third benchmark flags **179 AM two-way centerlines — about 4.9% of the sample and 213 km of road**.
Second, **what symmetry misses**: holding model, fundamentals, and parameters fixed, replacing directed with reciprocal-symmetric costs shifts measured welfare by **+1.10%** and moves **3.36%** of residents, **6.15%** of jobs.
Third, **tidal counterfactuals** under endogenous congestion and sorting: the top-100 policy shifts one lane to the slow AM direction; welfare rises **+0.87% short-run** and stays **+0.32% long-run**, at **zero net lane-km** versus about **1,100 lane-km** for comparable expansion welfare."

## Frame 5 — Literature

"Three strands.
**Directional congestion** — bottleneck models, measured urban speeds, reversible-lane studies (Vickrey; Arnott et al.; Couture et al.; Akbar et al.; Wolshon). *We measure directionality at trip and segment level with high-frequency speed data.*
**Quantitative spatial equilibrium** — the residence–workplace–market-access structure (Krugman; Eaton–Kortum; Allen; Ahlfeldt et al.). *We put directional congestion inside it and solve counterfactuals.*
**Transportation policy and congestion** — demand-side pricing and sorting (Vickrey; Kreindler; Barwick et al.; Yang et al.), supply-side capacity and optimal networks (Duranton–Turner; Fajgelbaum–Schaal), and public transit (Gu, Jiang, Zhang & Zou). *Tidal lanes are a different margin — a directed capacity reallocation.*"
(The blue "This paper" line in each strand is the takeaway — say only what we add.)

## Frame 6 — Data

"Three inputs. **Directed speed** — national trip-level routes across cities, plus a Beijing road-segment panel at 10-minute frequency over a week in 2025. **Commuting flows** — Baidu 2022 home–work OD. **Road characteristics** — geometry, class, lanes. Figure: the same corridor, two directions, different speeds."

## Frame 7 — National Trip-Level Directionality

"The speed asymmetry ratio is the smaller directed cost over the larger; one means symmetric, lower means more directional. Across the top-100 congested cities, the flow-weighted **median is 0.878**. The right panel ties it to severity: more congested cities have lower ratios. This is broad prevalence — but trip data can't tell us *which road* could take a tidal lane. That's the bridge to segments."

## Frame 8 — Commuters Face Directional Friction

"Are real commuters on the asymmetric part of the surface? In Shenzhen, unweighted grid pairs already show asymmetry; weight by observed commuter flows and it's stronger — **flow-weighted median 0.819, 71.6% of commuters on pairs below 0.90**. Weighting matters."

## Frame 9 — Road Segment Level Directionality

"This is the policy-relevant layer. Beijing AM segments: **median 0.884**, and the broad 0.90 threshold says asymmetry is common. For tidal scope, the stricter **one-third benchmark flags 179 centerlines — 4.9%, 213 km** — the local traffic-bureau's own cutoff for considering a reversible lane. This is the direction-level object the policy can actually act on."

## Frame 10 — Directionality Arises from Jobs–Housing Mismatch

"Why does the peak have a direction? Jobs cluster more than residences — Duncan index **0.26**, far above the random floor of 0.004. And AM flow is radial-inward — **74.5%** of core-versus-reverse trips head to the center, inbound-to-reverse **2.9×**. Separation plus direction gives asymmetric peak load — which is exactly what a directed model needs to capture."

---

## Frames 11–17 — Model

**11 Environment.** "Locations host residents and jobs. Each physical road is two directed links. Define **t^ff_{kl} as free-flow traversal time** on the directed link and **n_{kl} as its lane capacity**. Realized time t_{kl} is endogenous — it rises with traffic per lane — and directed, so t_{kl} need not equal t_{lk}." (Pure setup; no data provenance here.)

**12 Individual location choice.** "A worker picks residence, workplace, and route. Utility multiplies residence amenity, workplace wage, one over route cost, and an i.i.d. Fréchet shock. Route cost is the product of directed edge times — that's what lets segment traffic feed back into commuting cost. Choice probabilities are the Fréchet shares."

**13 Bilateral costs and flows.** "Bridge from routes to OD flows. For a fixed origin–destination the worker has many routes; under Fréchet they aggregate into an inclusive value — τ_{ij}^{−θ} is the sum of route terms C_{ij,r}^{−θ}. Impose residence and workplace marginals and flows take a **doubly-constrained gravity** form; **Π_i and P_j are the balancing market-access terms** — Π_i is the low-cost job access reachable from i, P_j the labor access into j. Since costs are built from directed edges, τ_{ij} ≠ τ_{ji}." (Full derivation in the appendix note.)

**14 Amenity, productivity, location.** "Workplace productivity rises with local employment through α; residential amenity falls with local residents through negative β. The two equations are the fixed point for residence and workplace shares; **χ is just a normalization constant** making shares sum to one. The content is the feedback: access shifts where people and jobs locate, which then shifts productivity and amenity."

**15 Route cost vs. commuting cost.** "Put directed edge costs in a matrix A with a_{kl} = t_{kl}^{−θ}. B = (I−A)^{−1} sums over paths of all lengths — the network multiplier — and τ_{ij} = B_{ij}^{−1/θ} converts network access back into a bilateral commuting cost."

**16 Feedback and welfare.** "For each OD pair the model computes expected use of each directed link; aggregate over flows to get **directed link traffic Ξ_{kl}** (the capital-Xi). Traffic raises edge time through the speed–flow relation. Welfare **W̄ is the Fréchet expected-utility scale**: the expected maximum over residence–workplace–route options collapses to this inclusive value, so up to a constant W̄ **is** the utility level from the full opportunity set."

**17 Equilibrium.** "An equilibrium is residence and workplace shares, OD flows, bilateral costs, edge times, link traffic, and market access, mutually consistent across choice, market clearing, agglomeration, the directed network, and congestion. Sufficient existence and uniqueness conditions are in the appendix (Allen–Arkolakis Prop. 1)."

---

## Frames 18–21 — Estimation

**18 Data and parameters.** "The table maps data to model objects: observed AM edge times from weekday AM speeds, free-flow from a night window, geometry and lanes into the directed network. Parameters θ = 6.83, α = +0.05, β = −0.10, λ = 1.1 (Barwick et al.)." (Say the future-estimation plan aloud; it's off the slide.)

**19 Anchor and solve.** "Step one: invert fundamentals so the observed allocation is an equilibrium at observed directed times. Step two: anchor counterfactual congestion around observed edge times — time moves with traffic per lane relative to baseline but can't fall below free-flow. Step three: re-solve for each scenario, fundamentals fixed; effects are differences between equilibria."

**20 Short run vs. long run.** "Short run: residence and workplace fixed; routes, traffic, times, costs adjust. Long run: shares also sort, with congestion feedback. The decomposition splits the change into ST direct, the GE congestion wedge, and sorting — which is why average route cost and welfare can diverge."

**21 Baseline fundamentals.** "Inverted amenities and productivity. Productivity correlates with employment at 0.67 versus 0.30 for amenity with residents — jobs are pinned by productivity, residence is access-driven. Treated corridors sit on high-flow employment bottlenecks (correlation ≈ +0.91), so the policy isn't picking arbitrary roads."

---

## Frames 22–30 — Counterfactuals

**22 What symmetry misses (Exercise 1).** "First exercise. Change only the symmetry of reciprocal edge times — replace each pair's observed and free-flow times with their geometric mean — everything else fixed. Measured welfare moves **+1.10%**, residence and workplace distributions move 3.36% and 6.15%, before any policy shock. The lesson isn't a universal bias number; it's that symmetry reallocates the baseline in a spatial model."

**23 Tidal-lane reallocation (Exercise 2 setup).** "For each reciprocal corridor label the slower AM direction s and faster f. The policy sets n_s' = n_s + 1 and n_f' = n_f − 1, total capacity in the pair unchanged. The diagram now carries those letters — the slow orange side gains a lane, the fast green side gives one up. Top-100 corridors are ranked by flow × excess-delay mass. Zero new pavement — only which direction the lanes serve at peak."

**24 Where the policy lands.** "The Tidal Candidate Score compares congestion-delay mass in the slow direction against the fast direction. Positive means a lane toward the slow side targets the direction with more baseline flow × excess time. Red edges are the lane-receiving directions among the top-100 — they sit on the central high-flow spine."

**25 Result 1 — cascade.** "Short-run direct effect: **−1.63 seconds per commuter** (−2,400 person-min/day). Add the GE wedge (+0.99) and sorting (+0.71) and the long-run average is **+0.06 s** (+95 person-min/day). That doesn't erase the engineering gain — on treated slow links time falls **20% short-run, 14% long-run**. The citywide average is flat because the network and OD weights re-adjust. Exposure: 56% of OD mass routes through treated corridors."

**26 Result 2 — welfare.** "Welfare rises in **both** horizons — **+0.87% ST, +0.32% LT**. Average route time and welfare needn't move together: route time averages realized OD costs; welfare values access to the whole residence–workplace opportunity set. One caveat on the slide — we don't endogenize mode switching, so gains to travelers who switch *into* driving are outside this accounting; +0.32% is a lower bound on that margin."

**27 Result 3 — sorting.** "Maps show reshuffling, not growth — totals are fixed. Residential shares rise near treated endpoints because lower OD costs raise residential market access. Employment gains are more concentrated, consistent with the workplace agglomeration force. The negative average change elsewhere is mechanical — fixed totals — not the absence of local gains."

**28 Result 4 — policy comparison.** "Tidal: +0.32% long-run welfare at **zero net lane-km**. Targeted road expansion reaches +0.44% but adds about **1,140 lane-km**. On cost, use an **urban-road** benchmark, not an inter-city expressway: urban arterial construction is on the order of CNY 30m per km, so a widened lane is roughly **CNY 5m per lane-km**, and the expansion benchmark is **order CNY 6bn**. The figure is soft; the point is the contrast — comparable long-run welfare costs several billion to build or almost nothing to reallocate."

**29 Result 5 — where to put the lanes.** "The welfare-elasticity score asks where one marginal lane matters most — edge gain × upstream residence access × downstream job access. Ranking by it gives +0.35%; TCS +0.32%; speed-only just +0.13%, because speed-only picks slow but low-flow roads (rank correlation with welfare elasticity only 0.08 vs 0.65 for flow × delay). Target with flow, asymmetry, and access together."

**30 Conclusion.** "Three messages. Measurement: directional congestion is present across cities, in commuter exposure, and down to segments. Model: symmetric costs move measured welfare and location shares before any shock. Policy: tidal lanes deliver positive welfare at zero net lane-km, and targeting should combine congestion and spatial access."

## Frame 31 — Next Steps

"First, reduced-form evidence on implemented tidal lanes — on-site video, counts, directed speeds, and an RDD around the switch — the main path to estimate λ. Second, estimate the structural parameters more tightly from data. Third, extend across cities, peaks, and grid resolutions. Closing: directional congestion turns a traffic-engineering fact into a spatial-equilibrium policy margin."

---

## Appendix cues (use only when asked)

- **appdata / appcost / appfund** — pipeline, grid–grid cost, inversion + solver algorithm.
- **appduncan** — Beijing Duncan 0.20, rank 1/100; D is aspatial.
- **appinbound** — polar inbound 74.5%, 2.9×, non-circular.
- **appmodelasym** — model-implied Ξ imbalance (median 2.9×, p90 20×). *(No forward button in the deck — reachable only by scrolling; tell me if you want a jump added.)*
- **appparams / appsens / appsensdetail / appsens_theta** — parameter meanings; LT W̄ positive in every converged α–β cell and across the θ grid (compare Δ, not levels).
- **apppolicy** — tidal / road-expansion / uniform; cost on the urban-road benchmark.
- **appelast** — dW̄^θ = dA_{ab} φ_a ψ_b derivation.
- **appfieldvisit** — Shenzhen tidal-corridor photos.
- **appexist** — Allen–Arkolakis Prop. 1 (back-link now returns to Equilibrium).

## Quick Q&A

- **"Only 0.06 s?"** Citywide OD-weighted average; treated links save 13–20%; sorting reweights flows.
- **"Welfare up but time flat?"** W̄ values access to all pairs, not mean commute time.
- **"Mode switchers?"** Not endogenized; +0.32% is a lower bound; flagged as a next step.
- **"Why 0.33?"** The traffic-bureau's own cutoff for considering a reversible lane; 179 Beijing centerlines (4.9%) qualify.
- **"Tidal vs building?"** 0 vs ~1,140 lane-km; urban-road cost ≈ CNY 5m/lane-km → order CNY 6bn.
- **"Exist / unique?"** Allen–Arkolakis Prop. 1; existence always; uniqueness sufficient condition on α,β vs θ,λ.
- **"Where does λ=1.1 come from?"** Barwick Beijing speed–density; plan to estimate from Shenzhen field data.

