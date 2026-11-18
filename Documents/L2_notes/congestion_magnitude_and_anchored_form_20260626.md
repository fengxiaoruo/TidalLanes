# Congestion-function magnitude misalignment, and why the model needs an observed-anchored form

**Date:** 2026-06-26 · **Context:** estimation framework, congestion channel (Step 5) ·
**Companion:** `lambda_stability_findings_20260619.md`

## TL;DR

Borrowing the Beijing speed–density elasticity λ ≈ 1.1 into the **level-form** congestion
update is *dimensionally inconsistent*. The problem is not the value 1.1 — it is that the
model's congestion argument Ξ/n is in arbitrary commuter-mass units and spans ~10 orders of
magnitude, whereas real congestion τ_obs/τ_ff spans only ~1.0–1.9×. Raising a 10-orders-of-
magnitude object to the power 1.1 produces per-edge travel costs ranging from "teleport"
(≈0) to 68× free-flow, none of which resemble observed travel times. This forces extreme
numerical rescaling (κ ≈ 14,000), makes 69% of edges faster than free-flow, and drowns the
policy signal. The fix is to **anchor the congestion term to the observed state** so the
arbitrary scale cancels and λ governs only the response to a capacity change, evaluated at
the correct operating point.

---

## 1. The original model's congestion specification

From `TidalLanes_Model_v4.tex` / `full_estimation`, the directed link speed and iceberg cost are

```
v_kl = v0_kl · (n_kl / Ξ_kl)^λ                         (speed law)
t_kl = (ℓ_kl / v_kl)^{1/θ}                             (iceberg cost, dimensionless)
A_kl = t_kl^{-θ} = 1 / τ_kl     where τ_kl = ℓ_kl/v_kl (minutes)
τ_ij^{-θ} = [(I − A)^{-1}]_ij                          (route aggregator, all paths)
Ξ_kl = t_kl^{-θ} · P_k^{-θ} · Π_l^{-θ}                 (expected directed link traffic)
```

and the **level-form congestion update** used inside the solver is

```
τ_kl ← τ_ff_kl · (Ξ_kl / n_kl)^λ                       (LEVEL FORM)
```

The congestion *argument* the elasticity acts on is the **raw** ratio `g_kl = Ξ_kl / n_kl`.

Two facts about this object matter:

1. **Ξ has no physical scale.** Ξ_kl is "expected commuting traffic" in units of commuter
   mass. Its level is set by total population `Lbar` (≈95k) and the normalisation of the
   market-access terms P, Π — not by vehicles per km per lane. So `g = Ξ/n` is *not* a
   density in the sense of the source regression.
2. **λ = 1.1 was estimated for a bounded density.** Barwick et al. (AER 2024) and Yang et
   al. (AEJ:Pol 2020) relate speed (km/h) to density (vehicles/km/lane) — a quantity bounded
   in the tens. The elasticity is meaningful *at that operating point*.

A constant-elasticity power law is scale-free **only if the model is anchored so the operating
point is right** (rescaling Ξ by a constant just rescales v0/fundamentals). The level form does
**not** anchor it, so the mismatch between (1) and (2) is not absorbed — it is amplified to the
1.1 power.

---

## 2. Diagnostic evidence (square grid, AM peak, v11)

We compare, edge by edge at the observed allocation:

- **observed congestion** the model *should* reproduce: `c_obs = τ_obs / τ_ff`
- **model congestion argument**: `g = Ξ_obs / n`  (Ξ_obs computed at the observed allocation)
- **what λ=1.1 implies**: `g^1.1`

| quantity | p5 | median | p95 | range | mean |
|---|---:|---:|---:|---|---:|
| observed `c_obs = τ_obs/τ_ff` | 0.967 | **1.089** | 1.881 | 0.5 … ~9 | 1.219 |
| model `g = Ξ_obs/n` | ~0 | 0.941 | 46.4 | **7.4e-8 … 5.6e+2** | — |
| model `g^1.1` | ~0 | 0.935 | **68.0** | ~0 … ~700 | — |

Key readings:

- **Real congestion is mild and tightly bounded:** median 1.089 (a 9% slowdown), p95 1.88;
  86.7% of edges are slower than free-flow. The true target lives in ≈[1, 2].
- **The model's argument spans ~10 orders of magnitude** (7.4×10⁻⁸ to 5.6×10²).
- **`g^1.1` produces congestion multipliers from ≈0 to 68×** — nothing like the observed 1–1.9×.
- **The median direction even flips:** model `g^1.1` median 0.935 (faster than free-flow) vs
  observed median 1.089 (slower).
- **The argument barely tracks reality:** `corr(ln g, ln c_obs) = 0.105`. (This is the same
  fact as the internal diagnostic λ̂ ≈ 0.012 — the model-assigned traffic is gravity/market-
  access-driven, not real flow.)
- **Scale gap:** to reproduce `c_obs` with λ=1.1, `g` would need a median of 1.081; the raw `g`
  median is 0.941 (ratio 0.77) — and the dispersion gap (10 orders vs 2×) is the dominant
  problem, not the median offset.

So the misalignment the project suspected is real and severe, and it is concentrated in **Ξ/n**
(the congestion argument and the travel-cost magnitudes it drives), not in θ, α, β (which are
dimensionless elasticities). A secondary issue: because `A_kl = 1/τ_kl` with τ in minutes, the
absolute *units* of travel time feed nonlinearly into `(I−A)^{-1}` (minutes vs hours give
different equilibria), so the cost magnitude is not innocuous either.

---

## 3. Consequences observed in the solver

Running λ=1.1 in the original level form (with an added in-loop spectral-radius guard so it can
even converge):

- **68.9% of edges end up faster than free-flow** (worst edge ≈ 22,000× free-flow speed),
  because `g < 1` on most edges and `g^1.1` then drives τ far below τ_ff.
- The route matrix `(I−A)^{-1}` becomes **ill-conditioned** (rcond ~1e-30); the spectral-radius
  rescaling κ must climb to **≈14,000** to keep it solvable.
- The tidal-lane counterfactual welfare effect is **≈0 (+0.0002%)** — the teleport-edges
  dominate the network and the policy signal is drowned.

These are symptoms of the magnitude problem in Section 2, not independent bugs.

---

## 4. The fix: observed-anchored (exact-hat) congestion form

Replace the raw argument with the **ratio to the observed state**:

```
τ_kl ← max( τ_ff_kl ,  τ_obs_kl · ( (Ξ_kl / Ξ_obs_kl) · (n_obs_kl / n_kl) )^λ )   (ANCHORED)
```

where Ξ_obs_kl is the model-assigned traffic at the **observed** allocation and n_obs_kl the
baseline lane count.

Why this resolves the misalignment:

1. **Baseline reproduces observed travel costs by construction.** At baseline, Ξ=Ξ_obs and
   n=n_obs ⇒ the ratio is 1 ⇒ τ = τ_obs. Verified numerically: the inverted fundamentals recover
   the observed residence marginals to max-abs **3.2e-6**, so the observed allocation is an exact
   fixed point.
2. **The arbitrary scale of Ξ cancels** — only `Ξ/Ξ_obs` enters, a dimensionless deviation
   centred at 1. The 10-orders-of-magnitude scale problem disappears.
3. **λ now governs only the *response* to a change** (capacity reallocation or induced traffic),
   evaluated around the correct operating point (the observed cost τ_obs). This is exactly the
   estimand the elasticity 1.1 is meant to be.

Numerically this is night and day: κ stays at **1.54**, the solver converges in 42 iterations,
and the tidal-lane counterfactual produces a clean (if small) welfare number instead of noise.

---

## 5. Side-by-side comparison

| dimension | **Original (level form)** | Free-flow cap | **Observed-anchored (exact-hat)** |
|---|---|---|---|
| congestion update | `τ_ff·(Ξ/n)^λ` | `τ_ff·max(1,Ξ/n)^λ` | `max(τ_ff, τ_obs·((Ξ/Ξ_obs)(n_obs/n))^λ)` |
| congestion argument | raw `Ξ/n` (10 orders of mag.) | raw `Ξ/n`, floored at 1 | dimensionless ratio centred at 1 |
| baseline fit to data | none (τ ≠ τ_obs) | none | **τ = τ_obs by construction** (resid 3.2e-6) |
| dimensional consistency w/ λ=1.1 | **broken** | partial (threshold still scale-dependent) | **clean** (scale cancels) |
| % edges faster than free-flow | 69% (max 22,000×) | 0% (capped) | 0% |
| spectral-radius κ needed | **≈14,000** | 1.62 | **1.54** |
| `(I−A)^{-1}` conditioning | ill-conditioned (rcond ~1e-30) | fine | fine |
| λ=1.1 convergence | only with guard; meaningless levels | yes (37 iters) | yes (42 iters) |
| baseline Wbar | 4.70208 | 4.34306 | 4.50162 |
| tidal CF2 welfare (top-100) | +0.0002% | +0.0031% | **−0.0593%** |
| what λ multiplies | mis-scaled level | floored level | **log-deviation from observed** |

(Wbar levels differ across rows because each implies a different baseline congestion calibration;
only the within-row baseline-vs-CF comparison is meaningful.)

Note the policy conclusion is not robust across forms: the level forms give a tiny positive
gain, the anchored form gives a small **negative** effect. The anchored form is the one whose
baseline matches data and whose numerics are well-posed, so its sign should be taken seriously —
and it suggests the "preliminary positive welfare gain" in the extended abstract was partly an
artifact of the un-anchored specification.

---

## 6. Recommendation

1. **Adopt the observed-anchored form as the baseline congestion specification** in
   `estimation_v2` (Step 5). Do not attempt to "retune λ" to line up magnitudes — `Ξ/n` has no
   natural density scale, so it cannot be aligned in levels; anchoring is the correct fix.
2. **Keep λ as an external/borrowed elasticity** (1.1), but acting only on log-deviations. Note
   this is consistent with exact-hat counterfactuals, which the model already intends.
3. **Separately verify that 1.1 is a true (log-log) elasticity**, not the level slope from
   Barwick Table 8 (−1.098 km/h per veh/km/lane). The anchoring fixes the *operating point*; it
   does not fix the *units of λ itself*. The Shenzhen tidal-lane segment (a real capacity
   reallocation with before/after speeds) is a candidate to identify a genuine elasticity at the
   exact policy margin.

## 7. Caveats / open issues

- **Baseline can no longer be "tested" against data** — the anchored form makes τ = τ_obs by
  construction, so baseline fit is imposed, not validated. Validation must come from
  counterfactual responses (e.g. the Shenzhen experiment), not baseline levels.
- **Model Ξ is still not a validated traffic measure** (`corr(ln g, ln c_obs)=0.10`). Inside the
  anchored form this matters for the *induced-demand response* (how Ξ moves when capacity
  changes). Observed link-flow data (Shenzhen, or Beijing if available) would let us condition on
  the real traffic change rather than trust the model's assignment.
- **Static vs dynamic.** The QSE delivers steady-state travel times, not queues; queue-length
  evidence (e.g. Shenzhen "−50% westbound") validates direction qualitatively but maps to the
  model only through travel-time levels.
- **External validity of λ** across cities/road classes remains an assumption to state explicitly.
```
