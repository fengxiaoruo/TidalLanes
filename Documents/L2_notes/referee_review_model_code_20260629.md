# Referee review — TidalLanes model & code (gate5 bundle)

**Date:** 2026-06-29
**Scope reviewed:** `model_input_edgeproj_v2_lcc_gate5_v1` bundle; Stage 1 (`directional_cost/`), Stage 2 (`full_estimation/`), congestion-form comparison, tidal-lane CFs. Code read end-to-end; the bundle was loaded and the headline numbers reproduced locally (see §0).
**Stance:** adversarial referee. I assume the structure (AA/ARSW iceberg QSM with a Leontief `(I−A)^{-1}` cost aggregator) is sound and focus on what would get the paper rejected or sent back.

---

## 0. What I reproduced

Loaded the locked bundle and re-ran the inversion + Stage-2 baseline:

| Object | Value |
|---|---|
| N nodes / M directed edges / Lbar | 1104 / 2642 / 88,110 |
| Spectral radius ρ(A) at κ=1 | **0.507** (max row-sum 0.854) — condition holds comfortably |
| Inversion | converges, residuals ≈ 0 (marginals reproduced exactly) |
| χ | 4.1e-10 (pure normalization artifact; see §4) |
| Wbar exogenous-τ / endogenous(legacy λ=0.071) | 1.861 / **1.817** |
| Endogenous baseline mean τ vs observed | **−13.9%** (median τ/τ_obs = 0.927) |
| Tidal top-50, long-run ΔWbar | **−0.030%** (short-run +0.011%) |
| Edges with τ_obs < τ_ff (data) | **8.3%** |
| Observed OD: positive cells / density / intrazonal share | 83,030 / 6.8% / 8.0% |

These match the team's own `congestion_lambda11` note, so the critiques below are about the model as it actually runs, not a misread.

---

## 1. Major issues (would block acceptance)

### 1.1 The headline policy produces a tiny, wrong-signed welfare effect
The paper is called *TidalLanes*, yet the central experiment — reallocate one lane from the fast to the slow direction on the top-50 asymmetric pairs — gives **ΔWbar = −0.03% in the long run** (and only +0.01% short-run). A referee stops here and asks: does the model say tidal lanes are *bad*, or is the experiment mis-specified? I believe it is partly the latter, and it is fixable:

- **Selection is on travel-time asymmetry, not directional volume.** `select_tidal_lane_pairs` ranks pairs by `τ_slow/τ_fast`. Tidal lanes raise welfare by matching capacity to the *high-flow* direction. Travel-time ratio and flow (Ξ) imbalance are not the same object; ranking on τ can add a lane to a low-volume "slow" direction and strip one from a high-volume "fast" direction, which *raises* aggregate cost under convex congestion. **Recommend** selecting (or at least re-ranking) on the directional Ξ imbalance `Ξ_kl/Ξ_lk` from the baseline equilibrium, and reporting how often the τ-slow direction is *not* the Ξ-heavy direction.
- **The shock is economically negligible.** 50 pairs × 1 lane out of 2642 directed edges, with a congestion elasticity of 0.071, cannot move aggregate welfare measurably. The near-zero magnitude is mechanical. Either scale the policy to something defensible (a corridor program) or reframe the contribution around *distributional*/directional cost reallocation rather than aggregate Wbar.
- Net: the current result reads as "tidal lanes don't matter," which is almost certainly an artifact of selection rule + shock size + suppressed elasticity (§1.2), not an economic finding.

### 1.2 The shipped Stage-2 baseline does not reproduce observed travel times
`run_stage2_gate5.py` (the main driver) runs `congestion_form="legacy"` with a cross-section "diagnostic" λ. That baseline sits **14% below observed τ** (median τ/τ_obs = 0.93). So the model's endogenous congestion *understates* congestion relative to the data it is calibrated on, and every congestion-driven CF is then read off a baseline that is in the wrong place. The team already diagnosed this and built the `anchored`, λ=1.1 form that reproduces τ_obs to +0.1% — but that fix lives **only in `run_congestion_lambda11_comparison.py`**, not in the main pipeline. As of this bundle, `EndogenousCongestion_Stage2_gate5.tex` is presumably reporting the legacy baseline.
**Recommend:** promote `congestion_form=anchored`, λ_phys=1.1 to the default in `run_stage2_gate5.py`/`solve_baseline_endogenous`, regenerate all Stage-2 CFs on it, and demote legacy/density to a robustness appendix. Right now "latest recommended spec" and "spec that generated the manuscript numbers" are two different things — a reviewer who reruns the repo will get different numbers than the paper.

### 1.3 λ (congestion elasticity) is assumed, not identified — so no congestion CF magnitude is identified
`parameters.py` is admirably candid: λ is a placeholder (0.488/θ ≈ 0.071, AA Seattle). The "diagnostic" mode estimates λ from a cross-section of (τ_obs/τ_ff) on (Ξ/n) — but Ξ is model-implied and the probe→rerun loop makes it one-step circular (the `θ̂` circularity already in your audit memo). The anchored spec then *imports* λ=1.1 from Barwick (Beijing), a different city. **Consequence:** every counterfactual that runs through (E6) has a welfare/cost magnitude that is a deterministic function of an un-estimated, transplanted elasticity. This is the single biggest threat to the quantitative claims. You don't need to fully solve identification, but the paper must (a) state this prominently as a limitation, (b) show CF results across a defensible λ band (e.g., 0.07 / 0.49 / 1.1) as the headline robustness, not a footnote, and (c) be precise that λ_phys=1.1 enters physical τ while λ_AA=1.1/θ≈0.16 would enter the iceberg t — the `CONGESTION_ELASTICITY_PARAMETER_MAPPING.md` derivation is correct, but make sure the code's anchored form (which multiplies physical τ_obs) is documented as using λ_phys, not λ_AA, in the manuscript.

### 1.4 CF1 ("symmetric vs directional cost") does not identify what it claims under endogenous congestion
`run_cf1_symmetric` symmetrizes τ only as the *initial condition*, then lets the endogenous loop run. But the congestion update regenerates directional τ from τ_ff and the equilibrium Ξ (legacy), or from the directional τ_obs anchor (anchored). So at convergence τ re-acquires its directional structure and CF1 ≈ baseline — the "remove directional cost" experiment is undone by the solver. The docstring even concedes "we still let the congestion technology update." To make CF1 well-posed you must symmetrize the *technology*, not the starting guess: symmetrize τ_ff (and, in anchored mode, the τ_obs anchor and Ξ_obs) so the fixed point itself is symmetric. As written, CF1's small effect is not evidence that directional costs don't matter.

---

## 2. Substantive concerns (need a response, not necessarily new code)

- **2N fundamentals cannot discipline O(N²) bilateral flows.** The inversion reproduces the residence/workplace *marginals* exactly (I confirmed residuals ≈ 0), which is the flip side of fitting bilateral OD poorly (your own notes: off-diagonal L1 ≈ 50–67%). Tidal-lane policy is intrinsically about specific corridors/OD pairs, so CFs rest on an OD structure the model fits badly. This needs an explicit honesty paragraph and ideally a bilateral-fit moment (gravity shifter / SMM) before corridor-level claims. The `od_fit` menu memo already scopes this; the manuscript should not over-claim corridor effects until step 5 of that menu is done.
- **The Leontief `(I−A)^{-1}` aggregator sums over all walks, and κ has economic content.** Bilateral cost `τ_ij^{-θ} = [(I−A)^{-1}]_{ij}` weights every multi-link walk, not shortest paths; κ rescales A and therefore changes how much detours contribute. The comment "counterfactual ratios are invariant to κ" is only approximately true — scaling τ by κ does **not** scale `(I−A)^{-1}` by a constant (it is nonlinear in A), so Wbar ratios are κ-dependent in principle. You are saved in practice because Stage 2 *fixes* κ=1 across baseline and all CFs (`_solver_kwargs` passes `fund.kappa`), and ρ(A)=0.51 leaves margin. Make the κ-fixing explicit in the text and drop or qualify the "invariant" claim; show a κ-robustness row.
- **8.3% of edges have τ_obs < τ_ff.** Observed peak time faster than free-flow is physically impossible and signals noise in the τ_ff/τ_obs construction (edge-projection routing). The `max(τ_ff, ·)` floor hides it in the solver but it pollutes the diagnostic λ regression and the anchored Ξ_obs. Report the share, and ideally winsorize or re-derive τ_ff so τ_ff ≤ τ_obs by construction.
- **Intrazonal cost is an arbitrary proxy.** `intrazonal_cost_minutes = 0.5·median(τ)` and the calibrated `intra_cost_scale` directly set the predicted intrazonal share (8% observed). Several OD-fit numbers move with this knob; justify it or show fit is not driven by it.
- **Welfare is a within-model index, not money-metric.** `Wbar = (Σ ūᵢ^θ L_Rᵢ^{βθ} Πᵢ^{-θ})^{1/θ}` with β<0 is fine as an ordinal scale, but only % changes at fixed κ/normalization are interpretable. The code is careful; make sure the paper never reports Wbar levels or converts them to currency.

---

## 3. Minor / code-hygiene

- **Mislabeled "L1 pct" in fundamentals comparison.** `fundamentals_comparison_summary` returns `100*0.5*Σ|u_scen−u_base|` over gmean-normalized fundamentals (not probability vectors), so it is not a percentage of any mass. Rename or normalize; a reader scanning the table will misread it.
- **Diagnostic-λ circularity.** `lambda_diagnostic` consumes `baseline_endog.Xi_kl`, which depends on the λ used to build that baseline. The probe→rerun in `run_stage2_gate5` mitigates but doesn't remove it. At minimum document that λ_hat is conditional on the probe λ.
- **Fractional lanes.** CF2/tidal allow `delta=0.5` and the lane fallback fills missing `n_kl` with the network mean (non-integer). Here `lanes_fallback_used=False` (good, no synthetic lanes in this bundle), but moving 0.5 of a lane is physically odd — keep δ integer for the headline.
- **κ picked from the max row-sum** (`_pick_kappa`) is outlier-sensitive: one dense node could inflate κ network-wide. Moot at κ=1 here, but note it for other grids.
- **Market-access fixed point is undamped** (`_market_access` starts at 1, alternates). Converges for this data; could oscillate on denser grids — consider a damping guard.
- **`run_stage2_gate5` default `--lambda-mode diagnostic`** means the canonical command in `MEMORY.md`/README silently uses the cross-section λ, not the recommended anchored λ=1.1. Align the documented command with the recommended spec.

---

## 4. Things that are correct and worth keeping (pre-empt referee suspicion)

- Spectral-radius/(E7) handling is sound: ρ(A)=0.51 at κ=1, κ fixed across scenarios — counterfactuals are internally comparable.
- The anchored congestion form is the *exact-hat* of the physical-time technology `τ = τ̄·D^λ` (τ = τ_obs·(D/D_obs)^λ with ℓ cancelling in the ratio). Mathematically clean and the right way to pin the baseline to data.
- χ = 4.1e-10 is **not** a bug — it is forced by the gmean(v)=gmean(w)=1 normalization and is absorbed; share/welfare comparisons are unaffected.
- The ST/LT/sorting decomposition (`tidal_lane_diagnostics.od_cost_decomposition`) is a genuinely nice device for separating first-order, GE, and reallocation channels — lead with it.
- Inversion is exact on marginals (residuals ≈ 0), so the "fundamentals are policy-invariant" claim used in CFs is internally valid.

---

## 5. Prioritized to-do before circulating

1. **Re-rank the tidal experiment on directional Ξ imbalance**, scale the policy to something defensible, and re-examine the sign. (§1.1)
2. **Make `anchored`, λ=1.1 the Stage-2 default** and regenerate the manuscript numbers off the data-reproducing baseline. (§1.2)
3. **Promote the λ-band (0.07 / 0.49 / 1.1) robustness to the headline** and state λ is imported, not identified. (§1.3)
4. **Fix CF1** by symmetrizing the technology (τ_ff + anchor), not the initial guess. (§1.4)
5. Add a bilateral-OD-fit moment before any corridor-level claim; report the τ_obs<τ_ff share and κ-robustness. (§2)
