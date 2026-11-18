# BPR (regime-dependent) congestion function — form, what it replaces, anchoring, parameters

**Date:** 2026-06-26 · **Companions:** `congestion_magnitude_and_anchored_form_20260626.md`,
`lambda_stability_findings_20260619.md`

## Why

The constant-elasticity power law `v = v0·(n/Ξ)^λ` forces *every* edge to slow when a lane is
removed, regardless of whether it is congested — it has no notion of spare capacity. Reality
(and the Shenzhen tidal lane: the off-peak direction showed "no obvious queuing" after losing a
lane) has a **regime structure**: removing a lane from an uncongested direction barely changes
its speed; capacity only matters near the congestion threshold. A BPR / fundamental-diagram form
captures this; the power law cannot.

## 1. What it replaces (one equation only)

It replaces **only the congestion technology** — `Model_v4` eq. (iceberg)'s speed/travel-time map:

```
OLD (power law):   τ_kl^min = t̄_kl · (Ξ_kl / n_kl)^λ          ⟺   v = v0 (n/Ξ)^λ
NEW (BPR):         τ_kl^min = t̄_kl · [ 1 + a · ( Ξ_kl / (c·n_kl) )^b ]
```

`t̄_kl = ℓ_kl/v0_kl` = free-flow time; `c` = capacity per lane; `a, b` = BPR shape parameters.

**Everything downstream is unchanged:** iceberg `t_kl = (τ_kl^min)^{1/θ}`, route aggregator
`A_kl = t_kl^{-θ}`, `τ_ij^{-θ} = (I−A)^{-1}`, gravity, market access, fundamentals, equilibrium
definition. The congestion technology and the route aggregator are independent modules — the
aggregator only needs link costs `t_kl` as inputs — so this is a single-equation swap.

## 2. The load variable x

```
x_kl = Ξ_kl / (c·n_kl) = flow / total capacity   (saturation / load: fraction of capacity used)
```

BPR routes all congestion through `x`: `τ = t̄·[1 + a·x^b]`. Small `x` → near free-flow; `x → 1`
→ steep slowdown. We never observe Ξ or c in absolute units; the anchoring below works around it.

## 3. Anchoring (no external capacity data needed)

**(a) Back out baseline load from observed slowdown.** We know each edge's observed congestion
`τ_obs/t̄`. BPR says `τ_obs/t̄ = 1 + a·x_obs^b`, so

```
x_obs = ( (τ_obs/t̄ − 1) / a )^{1/b}        ;   if τ_obs < t̄  →  x_obs = 0
```

Meaning: rather than measure flow/capacity, infer each edge's baseline saturation from *how slow
it actually is*. No congestion (τ_obs/t̄=1) → x_obs=0; very congested → x_obs large. The edge case
τ_obs<t̄ (14% of edges, "observed faster than free-flow") is set to x_obs=0 (treated uncongested).

**(b) How load changes in the counterfactual, and why c cancels.** With `x = Ξ/(c·n)`:

```
x_cf / x_obs = [Ξ_cf/(c·n_cf)] / [Ξ_obs/(c·n_obs)] = (Ξ_cf/Ξ_obs)·(n_obs/n_cf)
⟹  x_cf = x_obs · (Ξ_cf/Ξ_obs) · (n_obs/n_cf)
```

The unknown `c` appears identically in numerator and denominator and divides out, because we only
ever use `x` through the ratio to its anchored baseline. Intuition: we don't need "vehicles/hour
this lane can carry"; we only need "did this road get more or less full relative to baseline,"
which is pinned by the traffic change (Ξ_cf/Ξ_obs) and the lane change (n_obs/n_cf). Then

```
τ_cf = t̄ · [ 1 + a · x_cf^b ]        (≥ t̄ automatically — no free-flow floor needed)
```

So the whole pipeline needs only **(a, b)**; observed congestion pins x_obs; capacity c is never
used. Baseline reproduces τ_obs by construction (at baseline x_cf = x_obs).

## 4. Does λ change meaning? Yes.

λ disappears as a primitive. The elasticity of travel time w.r.t. load is no longer constant:

```
power law:  ∂ln τ / ∂ln(Ξ/n) = λ                       (constant everywhere)
BPR:        ∂ln τ / ∂ln x     = a·b·x^b / (1 + a·x^b)    (varies: →0 uncongested, →b as x→∞)
```

- **b** is the "λ in the congested limit" — the asymptotic high-congestion elasticity.
- The old **λ = 1.1 becomes a *local* calibration target**: choose (a,b) so the *local* BPR
  elasticity at the relevant (peak) operating point ≈ 1.1. It is no longer a global constant.

## 5. Other parameters and the transmission mechanism

- **θ, α, β: meanings unchanged.** θ still governs the route aggregator `(I−A)^{-1}` and the
  iceberg transform `t=(τ^min)^{1/θ}`; α, β still live in the fundamentals. None touch the
  congestion technology. New parameters: **a, b** (c is anchored out).
- **Transmission chain structurally identical:** `n → τ^min → t → τ_ij → L_ij → market access →
  spatial allocation → welfare`, with the `Ξ ↔ τ` feedback. The single change: the `n,Ξ → τ`
  link becomes **state-dependent** — a lane change matters a lot on congested edges, ~nothing on
  uncongested ones (power law made it uniform/proportional). This is the realism gain.
- **Scale caveat:** the anchored power law was scale-free in Ξ (only Ξ/Ξ_obs mattered). BPR is
  **not** scale-free — the regime depends on where x sits relative to capacity, so the operating
  point x_obs must be pinned (we do it via observed congestion). This re-introduces the
  "how close to capacity are we" question (cf. the AADT-vs-peak units discussion).

## 6. Numerical example (a=0.15, b=4; remove a lane 3→2, n_obs/n_cf=1.5, Ξ≈const)

| edge | baseline τ/t̄ | inferred x_obs | x_cf after −1 lane | new τ/t̄ | power law would give |
|---|---:|---:|---:|---:|---:|
| nearly uncongested | 1.01 | 0.51 | 0.76 | **1.05** (≈unchanged ✓) | 1.5^1.1 = **1.56** (−56% slower) |
| already loaded | 1.10 | 0.90 | 1.35 | **1.51** (large slowdown) | 1.56 |

Same lane removal: the empty edge barely changes (1.01→1.05); the loaded edge slows a lot
(1.10→1.51). The power law crudely penalises both by ~56%. "Off-peak direction loses a lane but
speed ≈ unchanged" holds under BPR, is impossible under the power law.

## 7. Calibration tension to settle

The example also shows that with **b=4**, an edge at τ/t̄=1.10 already implies x_obs≈0.90 (90% of
capacity) — i.e. *not* in the spare-capacity regime. Genuine "slack" only exists for τ/t̄ very
close to 1. So **b controls whether our mostly-mild-congestion edges (median τ/t̄≈1.09) are judged
"slack" or "near-capacity,"** which directly drives how cheap off-direction lane removal is. And
Yang/Barwick's λ≈1.1 was estimated near capacity (local elasticity near b there), whereas our
operating point is milder — so matching 1.1 as the *local* elasticity at our operating point, vs.
using a literature b=4, are different choices that must be made deliberately.

## 8. Implementation

Anchored BPR update inside the equilibrium fixed point:
`x_obs = ((τ_obs/τ_ff − 1)/a)^{1/b}` (precomputed, τ_obs<τ_ff → 0);
each iter: `x_cf = x_obs·(Ξ/Ξ_obs)·(n_obs/n_cf)`; `τ ← τ_ff·(1 + a·x_cf^b)`. No floor needed.
Long-run uses market-access Ξ (gravity re-solves L_R,L_F); short-run uses OD-decomposition Ξ with
fixed L_ij. Default a=0.15, b=4 (standard BPR), to be revisited per §7.

## 9. Empirical results (Rule B, Beijing square AM, 2026-06-26)

BPR with **standard a=0.15, b=4 ≈ the power law**: short-run welfare −0.099% (power law −0.099%),
long-run +0.121% (power law +0.143%), off-direction still slows +23%. Reason: with b=4 the
anchoring puts the median edge at x_obs=0.88 (88% of capacity) — Beijing's mild observed
congestion (τ/τ_ff≈1.09) is judged "near capacity," not slack. So a textbook BPR does NOT by
itself deliver the Shenzhen "off-direction barely changes" behavior for this data.

**b is the lever.** At fixed traffic the off-direction slowdown ≈ (baseline congestion excess) ×
(lane ratio)^b — it depends on **b**, not on a or the operating point. b-sensitivity (short-run):

| b | off-dir Δτ (lane removed) | busy-dir Δτ (lane added) | short-run welfare |
|---:|---:|---:|---:|
| 1.0 | **+4.5%** | −4.0% | −0.041% |
| 2.0 | +9.6% | −6.8% | −0.048% |
| 4.0 | +23% | −10% | −0.099% |
| power law | +31% | −14% | −0.099% |

**Recommended calibration: b ≈ 1.1, not the textbook 4.** Since b is the congested-limit
elasticity and Yang/Barwick's λ=1.1 *is* a (near-)congested elasticity, setting **b ≈ 1.1**
simultaneously (i) matches the borrowed λ=1.1, (ii) delivers the Shenzhen "off-direction barely
slows" pattern (b=1 → +4.5%), and (iii) is far more realistic than b=4 for mild congestion.
**This is exactly what the Shenzhen tidal lane identifies**: its before/after speeds observe the
capacity→speed response, i.e. b.

**Two-horizon structure is robust to b**: short-run stays slightly negative even at b=1 (−0.04%),
and long-run sorting still flips it positive — the "short-negative, long-positive" story does not
depend on b. Scripts: outputs/bpr_run.py, outputs/bpr_bsweep.py.
