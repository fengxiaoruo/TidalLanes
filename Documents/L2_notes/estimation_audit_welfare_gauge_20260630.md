# Estimation Audit — Welfare Gauge Fix Verification (2026-06-30)

**Scope:** independent multi-method verification of the *updated* estimation results, focused
on the welfare-measure change (the "important update") and the solving details.
**Verdict:** the welfare change is a **correct bug fix**, not a manufactured result. All
solving-detail checks pass. One minor robustness caveat (κ-sensitivity) + one stale-docstring
note. No canonical outputs were overwritten during the audit (verification re-solved in memory
and only printed; mtimes of `welfare_sorting_*`, `alpha_beta_sensitivity_*` unchanged).

---

## 1. What changed

`equilibrium_solve.py` (and `run_welfare_sorting_diagnostics_gate5.py`) switched the **welfare
object** from the gauge-dependent residence-MA sum to the **gauge-invariant Fréchet denominator**:

- **New (main):** `W̄ = [ Σ_ij τ_ij^{-θ} u_i^θ w_j^θ ]^{1/θ}` = `(Σ m_ij)^{1/θ}`, the AA expected-utility welfare. Equivalently `Σ_i u_i^θ M_i` with gauge-free resident access `M_i = Σ_j τ_ij^{-θ} w_j^θ`.
- **Old (now `Wbar_pi_raw`, "diagnostics only"):** `Σ_i u_i^θ Π_i^{-θ}` — uses the market-access scalars `(Π,P)`, which carry a one-parameter gauge freedom.

This flips LT welfare from **−0.85% (old) to +0.32% (new)**.

## 2. The old measure was gauge-contaminated (computational proof)

Re-solved baseline + LT (score_bpr_st, top100, pos05), then evaluated both welfare objects:

| measure | baseline | LT | W̄ %LT |
|---|---|---|---|
| **Fréchet `Στ^{-θ}u^θw^θ`** (gauge-free) | 2820.07 | 2883.13 | **+0.3243%** |
| old `ΣuΠ^{-θ}` (Π as the solver iterated it) | 69.44 | 65.51 | **−0.8486%** |
| old `ΣuΠ^{-θ}` (Π re-gauged to gmean$=1$) | 414.6 | 420.6 | **+0.2101%** |

- `gmean(Π)` **drifted 1.299 → 1.313** between baseline and LT: the `_market_access` fixed-point
  iteration lands on a different normalization in each scenario, and that drift contaminates the
  ratio of `ΣuΠ^{-θ}`.
- Gauge stress test: scaling Π by 2 changes `ΣuΠ^{-θ}` by `1−2^{-θ}≈−99%`; the Fréchet object is
  unchanged (does not use Π).
- **Conclusion:** the old **−0.85% "welfare loss via Π" was an artifact** of the Π gauge drift.
  Properly gauged, even the old measure is positive (+0.21%), agreeing in sign with the Fréchet
  (+0.32%). The new positive result is correct.

Note: traffic `Ξ_kl = t^{-θ} P_k^{-θ} Π_l^{-θ}` is gauge-invariant (one Π × one P per edge → c
cancels), so the **equilibrium, sorting, and commute decomposition were never affected** — only
the welfare read-out was.

## 3. Solving-detail checks (all pass)

- **Linear system valid:** max row-sum of `A` = 0.797 < 1 ⇒ ρ(A) ≤ 0.80 < 1; `(I−A)^{-1}` well-posed.
- **Mass conservation:** Σ ℓ^R = Σ ℓ^F = 1.000000.
- **Baseline reproduces data:** median `τ/τ^{obs}` = 1.0001 (8.3% of edges sit at the `τ^{ff}` floor where `τ^{obs}<τ^{ff}` — known, unchanged).
- **Commute identity exact:** ST + GE + sorting − total = −6e-16 s. (ST −1.63, GE +0.99, sort +0.71, total +0.06 — unchanged.)
- **Welfare 4-layer identity exact:** layers (①+172.2, ②−109.1, ③+0.88, ④−0.90) sum to ΔW̄^θ within 3e-13.
- **Two independent welfare paths agree:** `equilibrium_solve.Wbar` and diagnostics `primitive_welfare_summary` both give +0.3243% LT, +0.8714% ST.
- **Convergence:** baseline/ST/LT `n_outer`=65, `converged=True`; inversion `converged=True` (n_iter 455).
- **Sorting now ~welfare-neutral:** in the gauge-free decomposition the sorting layers ③④ net ≈ −0.02 (vs the old gauge-artifact −2.9/−1.9). The LT gain is `① ST τ (+172) + ② GE τ (−109)`; sorting barely moves welfare.

## 4. Robustness

- **Sign robust across the α–β grid:** new Fréchet `W̄ LT` is **positive in every converged cell**
  (+0.05% … +0.37%); best at α=−0.12,β=−0.10 (+0.37%). Old measure was negative everywhere — a
  systematic gauge artifact, now systematically corrected.
- **κ-sensitivity (minor caveat).** With *consistent re-inversion* at each κ (baseline still
  reproduces obs, median 1.0001):
  - κ=1 (calibrated minimal) → W̄ %LT **+0.32%**
  - κ=3 → **+0.27%**  (Δ ≈ 0.06 pp)
  κ controls how much non-shortest routes diffuse, so the CF response is *genuinely* κ-dependent;
  it is not a pure gauge. The run uses the calibrated minimal κ=1. Sign and approximate magnitude
  are robust. **Action:** the `_build_A_matrix` docstring claim "counterfactual ratios are
  invariant to κ" is **slightly overstated** — there is a small residual κ-dependence even after
  re-inversion; reword to "approximately invariant; small residual route-diffusion dependence."

## 5. Documentation consistency (already done by user)

- Deck `tidal.tex`: Result 2 now "Welfare: ST ↑, LT ↑ under the primitive definition" with the
  Fréchet formula and +0.87% / +0.32%; Conclusion and This-Paper updated; no stale "loss via Π".
- `model_and_results_summary_20260630.md` §3 documents the gauge fix; old −0.849% kept only as a
  raw-Π gauge diagnostic.

## 6. Open / recommended

1. Reword the κ-invariance docstring (§4). LOW.
2. Keep the old `Wbar_pi_raw` labeled "gauge diagnostic, not a welfare result" wherever it appears. DONE in code.
3. (Narrative) The LT story is now "ST saves time, LT commute ≈ 0, **LT welfare positive**, sorting
   ~neutral." Earlier memos saying "loss via Π / sorting destroys welfare" are superseded.
