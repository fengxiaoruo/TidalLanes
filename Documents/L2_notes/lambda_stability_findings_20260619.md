# λ = 1.1 stability check — full_estimation square AM (2026-06-19)

Quick sandbox experiment to see whether the Beijing speed-density elasticity (~1.1)
can be dropped into the current `full_estimation` solver, and how the tidal-lane
counterfactual and convergence behave as λ rises. Inputs: `manual_centerline_rules_v11`,
square grid, AM peak (N=2736 nodes, M=4087 edges, Lbar≈95k). Inversion of fundamentals
is λ-independent and was done once (residual ≈4.3e-4; the convergence flag is False only
because of the tight 1e-8 std-of-log-χ criterion).

## Results

| λ | damping | scenario | converged | iters | final Δ (max|Δlog t|) | Wbar | note |
|------|------|----------|-----------|-------|------|------|------|
| 0.0714 | 0.5 | baseline | **yes** | 16 | 6.2e-6 | 4.5298 | clean convergence; λ̂_internal = 0.012 |
| 0.0714 | 0.5 | CF2 tidal top-100 | **yes** | 16 | 6.2e-6 | 4.5300 | welfare **+0.004%** vs baseline |
| 0.30 | 0.5 | baseline | no | 54 | 1.74 | 4.414 | stable limit cycle, never settles |
| 0.30 | 0.15 | baseline | no | 54 | 0.134 | 4.415 | residual ~13× smaller, still not at tol |
| 1.10 | 0.5 | baseline | no | 24 | 22.4 | 10.33 | **(I−A)⁻¹ ill-conditioned**, rcond ~1e-24…1e-35; Wbar garbage |
| 1.10 | 0.15 | baseline | no | 24 | 1.65 | 4.356 | still ill-conditioned; not converged |

## Takeaways

1. **The current solver only converges at the small placeholder λ≈0.07.** At that value
   the tidal-lane welfare gain is tiny (+0.004%) — consistent with weak congestion feedback.

2. **λ=0.3 already breaks convergence.** Default damping (0.5) produces a persistent
   oscillation (Δ≈1.7); damping 0.15 shrinks the residual by ~13× but still does not reach
   tolerance in 54 iterations.

3. **λ=1.1 breaks the linear algebra, not just the step size.** The market-access solve
   `(I−A)⁻¹` becomes numerically singular (rcond down to 1e-35). The spectral-radius safety
   factor κ is tuned **once** from the baseline tau and is held fixed; at λ=1.1 the
   equilibrium link times move enough that A's row sums approach/exceed 1, so the inverse is
   meaningless (Wbar jumps to 10.3 then 4.36 depending on damping). Stronger damping tames the
   step (Δ 22→1.6) but not the conditioning.

4. **Internal vs imposed elasticity gap.** At the model-assigned traffic Ξ, the internal
   λ̂ ≈ 0.012 — i.e., the assigned flows imply almost no congestion elasticity, ~90× below the
   1.1 we want to impose. The high-λ instability partly reflects that congestion feedback is
   being applied to a noisy traffic proxy (all-or-nothing shortest-path Ξ), not just a solver
   tuning issue.

## What this means for adopting 1.1

λ=1.1 cannot be used in the current solver as-is. Before it is usable we need, at minimum:

- recompute κ (spectral-radius rescaling) **inside** the outer loop from the current tau,
  or set `kappa_target_rowsum` well below 0.9, with a hard guard that spectral radius < 1;
- replace the dense `scipy.linalg.solve` with a guarded sparse / Neumann-series solve;
- adaptive damping or Anderson acceleration (fixed damping alone is insufficient);
- separately, improve the traffic object Ξ so the congestion channel is applied to a
  credible flow (the λ̂≈0.01 vs 1.1 gap is the deeper issue).

Reproduce: `outputs/lambda_sweep.py` (λ=0.07 baseline+CF2) and `outputs/damping_test.py`
(λ×damping baseline grid), resumable, sandbox.

## Addendum — route-dispersion (θ_route) scan

Tested whether decoupling the route-choice dispersion from the commuting θ (currently
θ_route = θ = 6.83, which makes the Type-I-EV assignment quasi-deterministic) and lowering
it would give a more realistic link-load Ξ. For each θ_route, A_kl = t_kl^{-θ_route} on the
observed network; Ξ computed from **observed** OD flows via the exact AA route decomposition
Ξ_kl = A_kl·(MᵀHMᵀ)_kl, H = L_obs/M, M = (I−A)⁻¹; then the λ-diagnostic regression.

| θ_route | ρ(A) raw | scale forced | rcond(I−A) | λ̂ | R² |
|--------:|-----:|-----:|-----:|-----:|-----:|
| 6.83 | **0.880** | 0.65 | 0.27 | 0.0054 | 0.012 |
| 4.00 | 1.467 | 0.42 | 0.24 | 0.0062 | 0.015 |
| 2.00 | 2.188 | 0.31 | 0.20 | 0.0068 | 0.018 |
| 1.00 | 2.856 | 0.26 | 0.14 | 0.0073 | 0.021 |
| 0.50 | 3.324 | 0.24 | 0.11 | 0.0077 | 0.024 |

**Findings.** (1) Stability worsens with dispersion: only θ_route=6.83 has ρ(A)<1 (path-sum
converges naturally); every lower value has ρ(A)>1, forcing aggressive cost rescaling and worse
conditioning. (2) Realism barely moves: λ̂≈0.005–0.008, R²≈0.01–0.02 across the whole range —
even maximal spreading leaves Ξ explaining ~2% of observed edge-congestion variation.

**Conclusion.** Route dispersion is the wrong lever. λ̂≈0.01 is not caused by shortest-path
routing; it is that link flows are unobserved, so any model-implied Ξ is driven by OD/gravity
structure, not real traffic, and cannot recover the speed-flow relationship from one
cross-section. Recommendation: keep θ_route = θ = 6.83 (AA-faithful, only stable setting; do
not add it as a free parameter); impose λ=1.1 as an external calibration (it cannot be
identified here); and fix the λ=1.1 instability directly via in-loop κ + spectral-radius guard.

Reproduce: `outputs/route_disp_scan.py` (sandbox).

## Fix #3 — stabilized solver, and the real root cause

Implemented a stabilized outer loop (no repo edits): **(a)** recompute κ *each iteration*
to hold max row-sum of A ≤ 0.9 (non-shrinking spectral-radius guard), **(b)** adaptive
damping. Ran λ=1.1 baseline + tidal CF2 (top-100) at a common κ.

**Result — the numerics are fixable.** λ=1.1 baseline now converges (43 iters, Δ=9.6e-6),
no ill-conditioning. *But* κ had to climb to ≈14,000 and the tidal welfare effect was ≈0
(+0.0002%). Diagnosing the converged baseline showed why:

- **68.9% of edges end up faster than free-flow** (τ_min < τ_ff); 34% exceed 10× free-flow;
  the worst edge implies **22,000× free-flow speed**.

This is the real defect: the speed law `v = v₀·(n/Ξ)^λ` is **unbounded above** — on
low-traffic edges (Ξ/n<1) it makes travel *faster* than free-flow without limit, and λ=1.1
amplifies that into near-zero link times. Those teleport-edges blow up the route sum (forcing
κ≈14k) and drown the policy signal.

**Fix: free-flow cap** `τ = τ_ff·max(1, Ξ/n)^λ` (congestion only slows; no edge beats free-flow).
Re-running λ=1.1:

| variant | converged | iters | κ | share at free-flow | Wbar base | Wbar CF2 | tidal welfare |
|---|---|---|---|---|---|---|---|
| uncapped + in-loop κ | yes | 43 | ~14,000 | (69% *above* ff) | 4.70208 | 4.70209 | +0.0002% |
| **free-flow cap** | yes | 37 | **1.62** | 46% at ff | 4.34306 | 4.34320 | **+0.0031%** |

With the cap, κ stays ~1.6, conditioning is fine, ~46% of edges sit at free-flow (sensible for
an AM peak), and the tidal-lane CF2 gives a clean, small, positive welfare gain.

**Takeaways.** (1) The λ=1.1 instability is fixable — in-loop κ + adaptive damping is necessary,
but (2) the deeper bug is the missing free-flow cap; without it the level-form speed law is
ill-posed at high λ. (3) Even done correctly, a narrow tidal-lane policy (1 lane on 100 grid
pairs) moves *aggregate* welfare little (~0.003%) — that is economically reasonable for a
targeted, low-cost intervention; the paper should lean on corridor-level and distributional
effects, not just aggregate Wbar. (4) Remaining caveat: the cap threshold at Ξ/n=1 still depends
on the arbitrary scale of model Ξ; the cleaner long-run fix is to **calibrate the congestion
scale so the baseline reproduces observed travel times** (i.e. anchor the congestion update to
the observed state, the exact-hat/ratio form) — which also makes baseline τ match data by
construction.

Reproduce: `outputs/stable_run.py` (uncapped, in-loop κ) and `outputs/stable_cap_run.py`
(free-flow cap), both resumable, sandbox.

## Observed-anchored (exact-hat) form — and a sign flip in the tidal welfare effect

The free-flow cap still depends on the arbitrary scale of model Ξ (threshold at Ξ/n=1).
The clean fix is to anchor the congestion update to the observed state:

    τ_kl = max(τ_ff, τ_obs_kl · ( (Ξ_kl/Ξ_obs_kl)·(n_obs_kl/n_kl) )^λ )

so the observed allocation is a fixed point at baseline (Ξ=Ξ_obs ⇒ τ=τ_obs) and the scale of Ξ
cancels. Verified: the inverted fundamentals reproduce the observed residence marginals to
max-abs 3.2e-6, so **baseline = observed exactly** (Wbar_base=4.50162, κ=1.535, well-conditioned).

Tidal CF2 (top-100 by τ-asymmetry, +1 lane slow / −1 lane fast, λ=1.1) converges in 42 iters
(κ stays 1.535):

| form | baseline Wbar | CF2 Wbar | tidal welfare | κ |
|---|---|---|---|---|
| uncapped + in-loop κ | 4.70208 | 4.70209 | +0.0002% | ~14,000 |
| free-flow cap | 4.34306 | 4.34320 | +0.0031% | 1.62 |
| **observed-anchored (exact-hat)** | 4.50162 | 4.49895 | **−0.0593%** | 1.54 |

**The sign flips.** In the most principled specification, the naive tidal-lane policy *reduces*
aggregate welfare by ~0.06%. The earlier "preliminary positive welfare gain" (extended abstract)
looks like an artifact of the un-anchored / ill-conditioned level form.

**Why negative (working interpretation).** The policy *removes* a lane from the fast direction to
give it to the slow direction, selected purely by observed speed asymmetry. Under convex
congestion (λ=1.1), degrading the fast direction is costly, and τ-asymmetry does not identify the
*demand-weighted* welfare margin — a direction can be "slow" without carrying much commuting flow,
so capacity is shifted toward low-value directions while hurting higher-flow ones.

**Implications for the paper.** (1) The tidal welfare sign is implementation-sensitive — this is a
feature to study, not hide: it motivates the directional GE model and a smarter treatment rule
(select by directional *flow*-weighted congestion, not speed asymmetry; consider add-only vs
budget-neutral reallocation). (2) Re-examine the abstract's positive-gain claim. (3) Numerically,
the observed-anchored form is the recommended baseline: well-conditioned (κ≈1.5), baseline matches
data by construction, no free-flow pathology. Caveats: still the simple top-100 Δ=1 rule;
sensitive to λ; aggregate Wbar is blunt; model Ξ remains unvalidated against real link flows.

Reproduce: `outputs/ratio_run.py` (sandbox, resumable).
