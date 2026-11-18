# Discussant Comment to Change Map, 2026-07-01

## A1. CF1 Endogenous Symmetric Cost

- Severity: A
- Response stance: accept and revise
- Issue: the endogenous CF1 result may be a mechanical anchor result.
- Planned change: remove the economic interpretation unless the symmetric shock enters an anchored primitive.
- Location: Result 4; `counterfactual_levels.py`; `stage2_counterfactuals.py`

## A2. CF3 Fixed-Tau Versus Endogenous Shock

- Severity: A
- Response stance: accept and revise
- Issue: fixed-tau and endogenous CF3 are different primitive shocks.
- Planned change: split measurement counterfactuals from capacity policies; delete "same shock" language.
- Location: Result 4; output metadata

## A3. P3 Qdelay Lane-Kilometres

- Severity: A
- Response stance: accept and revise
- Issue: P3 qdelay lane-kilometres reuse P2 lane-kilometres.
- Planned change: compute lane-kilometres from P3's own selected capacity vector, or implement budget matching.
- Location: Result 5; `run_policy_comparison_gate5.py`

## A4. P4/P5 Horizon Labels

- Severity: A
- Response stance: accept and revise
- Issue: P4/P5 short-run and long-run labels are invalid.
- Planned change: add short-run residence and workplace pins for all horizon-labeled capacity policies.
- Location: policy appendix; `run_policy_comparison_gate5.py`

## A5. Cost-Effectiveness Horizon Averaging

- Severity: A
- Response stance: accept and revise
- Issue: the cost-effectiveness file averages ST and LT but labels the column LT.
- Planned change: filter to long-run rows or rename mean-over-horizons columns.
- Location: `policy_cost_effectiveness.csv`; policy appendix

## A6. First-Best Pricing

- Severity: A/B
- Response stance: accept and revise
- Issue: P6 pricing implementation is provisional.
- Planned change: move P6 out of the headline; implement a generalized-cost toll wedge and scenario-flow revenue before reporting.
- Location: policy appendix; `stage2_counterfactuals.py`

## B1. Directed Equilibrium Definition

- Severity: B
- Response stance: accept and revise
- Issue: the equilibrium slide contradicts the directed environment.
- Planned change: define equilibrium on directed links and remove `t_kl = t_lk`.
- Location: equilibrium slide

## B2. Kappa Invariance

- Severity: B
- Response stance: accept and revise
- Issue: `kappa` invariance is too strong.
- Planned change: replace with limited sensitivity over audited values.
- Location: appendix fundamentals

## B3. Congestion Parameter Provenance

- Severity: B
- Response stance: accept and revise
- Issue: effective congestion lambda is confused with legacy placeholder lambda.
- Planned change: add effective configuration metadata and update prose.
- Location: `parameters.py`; `run_stage2_gate5.py`; appendix solver

## B4. Anchored BPR Notation

- Severity: B
- Response stance: accept and revise
- Issue: anchored BPR notation is not clear enough.
- Planned change: present the anchored equation in the body or flag it before the appendix; separate edge minutes from bilateral cost notation.
- Location: model and estimation slides

## B5. Existence and Uniqueness

- Severity: B
- Response stance: accept and revise
- Issue: existence and uniqueness attribution is too strong.
- Planned change: limit the theoretical claim to the AA block; report numerical convergence for full directed congestion.
- Location: equilibrium slide; appendix solver

## C1. Local Notes Outside The Package

- Severity: C
- Response stance: accept and revise
- Issue: the package refers to local notes outside the package.
- Planned change: remove local references or copy a compact note summary into `docs/`.
- Location: `tidal.tex`; package docs

## S1. Result 2 Decomposition

- Severity: B
- Response stance: clarify existing logic
- Issue: the decomposition may be confused with edge-speed averages.
- Planned change: label the decomposition as OD/person-weighted generalized commuting cost.
- Location: Result 2

## S2. Robustness And Calibration Caveat

- Severity: B
- Response stance: accept and revise
- Issue: robustness should disclose the missing directed traffic panel and external calibration.
- Planned change: add a short robustness and calibration caveat.
- Location: robustness or estimation appendix

## T1. Tests For Accounting And Horizon Definitions

- Severity: A
- Response stance: accept and revise
- Issue: tests are missing for lane-kilometre accounting, cost-effectiveness horizons, short-run pinning, and CF1 shock entry.
- Planned change: add the minimal tests to `tests/` or `scripts/check_package.py`.
- Location: package validation scripts

## R1. Smoke Model Runtime

- Severity: C
- Response stance: partially accommodate
- Issue: `make smoke-model` may be too slow for reviewers.
- Planned change: keep full validation, but add a faster smoke target or rename the current target as mini-full validation.
- Location: `Makefile`; `scripts/`

