# Directional vs symmetric travel-cost measurement: what changes vs the literature

**Date:** 2026-06-26 · **Question:** holding the common observed residence/employment
distribution fixed, how do results differ when travel costs are measured *directionally*
(our data) vs *symmetrically* (as in prior QSE applications, which use AADT / average speeds)?

## Setup / mechanism

Everyone uses the same observed marginals (L_R residents, L_F jobs) and OD flows. What differs
across studies is the travel-cost object τ. Fundamentals (ū_i, Ā_j) are **not observed — they
are inverted** to rationalise the observed allocation:

```
(l_i^R)^{1-βθ} = χ Σ_j τ_ij^{-θ} ū_i^θ Ā_j^θ (l_j^F)^{αθ}
(l_j^F)^{1-αθ} = χ Σ_i τ_ij^{-θ} ū_i^θ Ā_j^θ (l_i^R)^{βθ}
```

So with the **same** (L_R, L_F) but a **different** τ, the inversion returns **different**
(ū, Ā). Fundamentals are a residual that absorbs the travel-cost assumption.

**Hypothesis:** a central job hub that is expensive to reach in the directional (peak-inbound)
cost must be assigned high Ā to rationalise its job concentration; symmetric costs understate
that inbound cost, so the same concentration is "explained" by easier access → lower inferred Ā.
I.e. symmetric should under-attribute productivity to central/peak-accessible locations.

## Model period (confirmed)

The square-grid input is **AM-only** (all 4632 edges `period=AM`, `tau_obs_source=stage06_t_edges_AM`).
OD is **home→work** (`home_i`→`work_i`, `commuters_road`). So an AM commute i→j uses the AM i→j
(inbound, congested) edge — consistent with "AM peak = home→work direction." The model does **not**
include PM; a full daily commute would add the PM work→home (reverse) leg, the mirror image. The
evaluated tidal lane is therefore an AM policy.

## Scheme B — speed averaging (DONE)

Symmetric τ = **harmonic mean** of the two directional times (= travel time at the averaged
speed, since both directions share the link length): `τ_sym = 2/(1/τ_AB + 1/τ_BA)`. Invert
fundamentals under directional τ_obs vs τ_sym; same observed marginals.

**Results (script `outputs/scheme_B.py`):**
- Fundamentals barely move: `dln Ā_j` std **0.029** (p5/p95 −6%/+3%), `dln ū_i` std **0.030**.
- Spatial bias is in the hypothesised direction but **very weak**: corr(log jobs, dln Ā) = **−0.058**,
  corr(dist-to-job-center, dln Ā) = **+0.096**; top-100 vs bottom-100 job grids dln Ā = −0.0085 vs
  −0.0075 (essentially identical).
- Baseline welfare levels are not directly comparable (different τ + normalisation); the clean
  objects are the fundamentals relocation above and a common counterfactual.

**Reading:** consistent with CF1 — directional asymmetry is strong at the edge/adjacent level but
**attenuates at the OD level** (flow-weighted bilateral ratio ≈ 0.976), so symmetrising barely
changes the inversion. The economic geography (fundamentals) is **not materially revised** by
using directional data.

## Scheme A — flow averaging (TO DO)

Construct symmetric link traffic `Ξ_sym_kl = (Ξ_AB + Ξ_BA)/2`, assign to both directions, then
derive the symmetric speed/cost from `Ξ_sym` via the congestion function; invert fundamentals and
compare as in Scheme B. Corresponds to studies that only have **total link traffic (e.g. AADT)**.

**Implementation note / caveat:** directional link traffic Ξ is **not directly observed** in our
data (the recurring unobserved-flow problem). So Scheme A must source Ξ from either (i) the
OD-implied link loads (route-assign the observed L_ij), or (ii) the model-assigned Ξ. Choice (i)
is cleaner (data-side); (ii) makes it model-dependent. Decide before running. Because Scheme A
goes through the congestion function, it also depends on the congestion form (power law vs BPR)
and λ — unlike Scheme B which is purely a speed average.

## Other impacts of the directional → symmetric choice

1. **Parameter identification (λ):** the congestion elasticity is identified from the speed-flow
   regression `ln(1/speed) ~ ln(traffic/lane)`. The identifying variation in our data comes largely
   from **directional** differences on the same road; symmetric averaging (speed or flow) removes
   that variation → attenuated / biased λ̂. This is a concrete reason directional data helps.
2. **Counterfactual analysis:** the tidal-lane policy is **undefined** under symmetric costs (you
   cannot reallocate capacity between two directions that are assumed identical). More generally,
   any counterfactual's predicted welfare/distribution differs because fundamentals differ — but
   here that difference is small (Scheme B).

## Honest takeaway

For this Beijing data, the value of directional measurement is **not** in materially revising
fundamentals, welfare, or the economic geography (the symmetric bias on those is small, ~3% on
fundamentals, because directionality washes out at the OD level). It is in (a) **enabling the
directional policy** (tidal lanes, undefined under symmetry) and (b) **identifying λ** from
directional speed-flow variation. The paper should frame the contribution accordingly.

Scripts: `outputs/scheme_B.py` (done), `outputs/cf1_symmetric.py` (CF1 cost symmetrisation).

## TWO DIFFERENT QUESTIONS — do not conflate

The "symmetric vs directional" label hides two distinct questions with very different magnitudes:

1. **Cost-measurement symmetrisation** (CF1, Scheme B above): if you *measure/assume* costs
   symmetrically instead of directionally, how do welfare / fundamentals change? → **small**
   (~0.06% welfare, ~3% fundamentals), because route aggregation makes OD costs nearly symmetric
   anyway (flow-weighted bilateral ratio ≈0.976). This is an inference/measurement question.

2. **Demand-concentration congestion** (below): the AM/PM peak is one-directional, so demand
   *concentrates* on one direction; under convex congestion this generates more total delay than
   if balanced. → **large** (~+65%). This is the physical cost of directional demand, and it is
   the fact that motivates the paper.

The earlier "directionality barely matters" results were all question (1). The user's original
intuition ("directionality → more congestion") is question (2), and it is large.

## Demand-concentration experiment (the motivating fact)

**Question:** holding each corridor's TOTAL flow fixed, how much extra total congestion delay does
the directional concentration of demand create, vs balanced demand?

**Why convexity is the mechanism:** congestion is convex (travel time rises faster-than-linearly in
load), so by Jensen concentrating a given total flow on one direction beats balancing it:
g(concentrated)+g(empty) > 2·g(average).

**Design / clean implementation (script `outputs/demand_concentration2.py`):**
- Both directions of a road share τ_ff and capacity c·n. Back out the OBSERVED load per directed
  edge from its congestion: `x_obs = ((τ_obs/τ_ff − 1)/a)^{1/b}` (busy dir high x, off dir low x).
- Balance loads: `x_bal = (x_AB + x_BA)/2`.
- Congestion delay (vehicle-time above free-flow) per directed edge: flow ∝ x·n, excess time ∝
  τ_ff·a·x^b ⇒ **delay ∝ n·τ_ff·x^{b+1}**. Convex in x (b+1>1) ⇒ directional > balanced.

**Result:** directional concentration generates **+64% to +71% more congestion delay** than balanced
(b=1.1→+64%, b=2→+71%, b=4→+67%; robust to b). ≈65% of the convex-attributable congestion delay is
due to the directional concentration of commuting demand.

**Implementation pitfall (recorded so we don't repeat it):** a first version anchored each direction
to its *own* observed time (`τ=τ_obs·(Ξ/Ξ_obs)^λ`). That gives the two directions *different* effective
congestion functions (the off direction's low τ_obs becomes its anchor), breaking the shared-capacity
premise of the Jensen comparison — and it wrongly produced "directional has LESS delay (−15%)". The
correct version backs out loads under a SHARED per-corridor function and balances the loads.

**Interpretation:** this is the motivating fact — directional peak demand is costly *because* congestion
is convex; a tidal lane is the policy response (add capacity to the over-loaded direction, lowering its
x and the convex penalty). Caveats: "balanced demand" is a non-implementable benchmark, so +65% is an
accounting decomposition ("size of the problem"), not a policy; it uses BPR to back out loads (excess %
robust to b); holds total per-corridor flow fixed.
