# Session handoff — directional congestion / TidalLanes (2026-06-28)

Hand-off for continuing in another agent. Summarises key findings, decisions, and prioritised
next steps. Companion notes are cited inline; memory files: `tidallanes-congestion-elasticity`,
`tidallanes-data-conventions`, `tidallanes-reference-papers`.

## 0. One-paragraph state

The paper is about **directional congestion** (prevalence + the cost of ignoring it); tidal lanes
are ONE application. The structural model is an AA-style QSE with directional bilateral costs and
(for the policy) endogenous congestion. This session: (a) fixed the congestion-function numerics
and identified the right specification (observed-anchored + BPR, b≈1.1); (b) re-framed the
contribution (the durable contribution is endogenous directional congestion + its policy/welfare
bias, NOT directional cost data, which is commoditising); (c) uncovered serious DATA-PIPELINE
problems (grid-cost version inconsistency, 58% of population in isolated cells, a broken raw_v2)
that mean **no counterfactual number is trustworthy yet** and a grid-cost/coverage decision is the
current blocker.

## 1. Congestion function — settled specification

- **Borrow λ ≈ 1.1** (Beijing speed–density, Yang/Barwick). Caveat: Barwick Table 8 is a LEVEL
  coefficient (−1.098), not necessarily a log-log elasticity — treat as calibration, verify.
- **Level form `τ=τ_ff·(Ξ/n)^λ` is broken**: model Ξ/n spans ~10 orders of magnitude, corr with
  observed congestion only 0.10, internal λ̂≈0.01; at λ=1.1 → 69% of edges faster than free-flow,
  κ≈14000, garbage. (`congestion_magnitude_and_anchored_form_20260626.md`)
- **USE the observed-anchored (exact-hat) form** `τ=τ_obs·((Ξ/Ξ_obs)(n_obs/n))^λ`: baseline=observed
  by construction for ANY λ (verified, marginals recovered to 3e-6), well-conditioned (κ≈1.5).
  λ acts ONLY on deviations from baseline; do NOT calibrate λ to fit observed (that forces λ→0.01).
- **Solver**: in-loop κ recompute + adaptive damping (λ=1.1 needs it). `lambda_stability_findings_20260619.md`
- **BPR (regime-dependent) form** `τ=τ_ff·[1+a·x^b]`, anchored via `x_obs=((τ_obs/τ_ff−1)/a)^{1/b}`,
  `x_cf=x_obs·(Ξ/Ξ_obs)(n_obs/n)` (capacity c cancels). **b≈1.1 recommended** (regime structure:
  uncongested direction barely slows on lane removal → matches Shenzhen "no queuing"; standard b=4
  ≈ power law for this mild congestion). `bpr_congestion_form_20260626.md`

## 2. Model mechanics (for whoever rebuilds)

- **Two flow concepts:** (A) back out flow from congestion `Ξ=n·(τ_obs/τ_ff)^{1/λ}` — circular,
  measurement only, makes λ unidentifiable; (B) gravity/route assignment — behavioural, needed for
  counterfactuals. **Use B + anchor to observed congestion + borrow λ.** Real flow is NEVER observed.
- **θ vs λ:** θ = how flow responds to cost (route + location choice, the assignment); λ = how cost
  responds to flow (congestion technology). Counterfactual = fixed point of the two.
- **Counterfactual mechanism:** you change lanes n; the model SOLVES the new traffic Ξ as the
  behavioural response (θ: reroute + resort), congestion feeds back (λ). Short-run = reroute only
  (fix L_ij); long-run = + spatial sorting.

## 3. Counterfactual results so far (NOT yet trustworthy — see §5)

- Treatment rule decides the sign: naive speed-asymmetry top-100 → −0.06%; **flow-weighted (Rule B)
  → +0.12%** (full-2736). Robust to λ∈{0.5,1.1,1.5}.
- Two horizons: short-run (no sorting) ≈ −0.04% to −0.10%; long-run (sorting) flips positive. The
  welfare gain is substitution/sorting-driven; at fixed behaviour commutes get slightly costlier.
- **Demand-concentration experiment (the motivating fact, robust):** directional demand
  concentration generates **+64–71% MORE total congestion delay** than balanced demand (convex
  congestion). Script `outputs/demand_concentration2.py`. This is the large, durable channel.
- CF1 (symmetric vs directional COST, exogenous congestion): welfare effect tiny (~0.06%);
  fundamentals shift ~3% (network-aggregated) or ~28% (measured directional OD) — BUT fundamentals
  cancel in exact-hat, so this does NOT translate to counterfactual bias. (`symmetric_vs_directional_measurement_20260626.md`)

## 4. Framing / contribution (re-oriented this session)

- **Don't hang the paper on "directional cost data"** — it's small (measurement bias ~0.06%) AND
  commoditising (newer QSM papers already scrape two-way OD).
- **Durable contribution = endogenous directional congestion model + (i) the efficiency cost of
  directional congestion, (ii) directional policy evaluation (undefined under symmetric), (iii) the
  bias when standard counterfactuals ignore the directional congestion RESPONSE.** The big number
  is the endogenous-congestion convexity channel (+65%), not measurement.
- **Two-stage structure:** Stage 1 = exogenous directional cost, simple model → "directionality is
  prevalent and you mis-rank/mis-target if you ignore it" (incidence, not aggregate welfare). Stage
  2 = endogenous directional congestion → policy (tidal lane, short+long run) + the congestion-
  response bias. Stage 1 uses observed cost; Stage 2 anchors to observed congestion + borrowed λ +
  Shenzhen-identified b.
- **AB-test plan (user's) diagnosis:** good structure; but (a) the measurement tests (market access,
  sorting) are SMALL — lead with incidence + the endogenous-congestion-response test; (b) the "B"
  baseline should be "directional cost + SYMMETRIC/exogenous congestion", not naive symmetric cost
  (the field already uses directional cost); (c) keep the exact-hat-compatible test to defeat the
  fundamentals-cancellation objection.
- **CHECK the two Shenzhen Wall QSM papers** (memory `tidallanes-reference-papers`, "asymmetric road
  tech") — if they already do endogenous directional congestion the gap narrows; confirm before
  committing the contribution.

## 5. DATA PIPELINE — the current BLOCKER (must resolve before trusting any CF)

(`data_pipeline_versions_and_conventions_20260628.md`, fig `Documents/L3_figs/centerline_vs_v1_cost_coverage.png`)

- **Model currently uses CENTERLINE (stage06) grid cost** (`qsm_input_edges_square`,
  tau_obs_source=stage06_t_edges_AM), NOT v1/raw. Centerline cost = straight-line centroid
  distance ÷ harmonic speed (NO routing). v1 (kunpeng edgeproj) = real road routing + connectors.
  They are ~uncorrelated (corr 0.09) and v1 is ~3.5× longer — because they measure different things
  (heuristic vs routed-with-connector-overhead). The draft's headline asymmetry 0.793 is from
  raw_edgeproj/v1, but the model runs on centerline (0.928) — **inconsistent; reconcile.**
- **CONVENTION ADOPTED: restrict the model to the LARGEST CONNECTED COMPONENT.** All future runs use
  this; key versions re-run under it.
- **Severe coverage gap under centerline:** largest CC = 1514 cells carrying only **64% of
  residents** (Lbar drops 95k→40k; ~58% of commuting flow involves an isolated cell). Under this
  clean scope, **Rule B tidal welfare FLIPS to −0.15%** (the +0.12% was contaminated by isolated
  nodes). So prior CF numbers are not reliable.
- **v1 has MUCH better coverage: CC covers 97% of residents** (vs centerline 64%) — strong reason to
  prefer v1. But v1's inflated times come from the **connector mechanism** (grid centroid → road
  access, up-along-down detours, e.g. adjacent pair 3.33→8.19 min). v1's smaller cell-count CC =
  connector feasibility fails in sparse periphery (dense core still 97% pop).
- **raw_connected_v2 is BROKEN — a spatial clip of central Beijing only** (9,888 edges vs full Baidu
  24,763; v1 kunpeng ≈23,730). The manual-editing workspace only loaded the clipped centre; the 336
  manual edits were applied on a partial base → only 609 valid grid edges. **STATUS: confirm with
  the kunpeng pipeline owner before fixing.** Fix = re-apply manual_edge_ops on v1's full graph
  (full-city) OR restrict grid to the v2 clip extent (central-city study).
- **Data time mismatch:** OD = 2022-11; Beijing speeds = 2025-05; AM-only, road-mode commuters.

## 6. NEXT STEPS (prioritised)

1. **Resolve the grid-cost / coverage decision (BLOCKER).** Choose: fix v1's connector overhead
   (best — keeps real routing + 97% coverage), or keep centerline (clean times but 64% coverage,
   no routing), or a hybrid. This gates every CF number. Diagnose v1 connectors first
   (route-audit for centerline vs v1 on a few pairs; the existing `comparison_grid_methods` audit
   machinery decomposes time into road vs connector).
2. **Re-run ALL key models under the largest-connected-component convention** with the chosen
   grid-cost version; report differences (expect sign changes). Scripts to adapt: `outputs/cc_rerun.py`.
3. **Reconcile draft asymmetry inconsistency** (0.793 raw vs 0.928 centerline) — pick one version
   for both descriptive stats and the model.
4. **Confirm/fix raw_connected_v2 with kunpeng** (spatial clip).
5. **Identify b (congestion technology) from the Shenzhen tidal-lane natural experiment**
   (before/after segment speeds + lane change); use to replace the borrowed-λ-as-b assumption.
6. **Build the key quantitative experiments** for the paper: endogenous directional vs symmetric
   congestion response to a common shock (the bias); directional-shock incidence (who benefits);
   freeze-vs-allow-sorting decomposition; exact-hat-compatible version.
7. **Continue `estimation_v2`** clean rebuild (Step 1 `load_nodes` done; bake in: connected-component
   restriction, observed-anchored BPR congestion, flow-weighted treatment) —
   `data_work/src/model/estimation_v2/`.

## 7. Artifacts created this session

Notes (`Documents/L2_notes/`): `lambda_stability_findings_20260619.md`,
`congestion_magnitude_and_anchored_form_20260626.md`, `bpr_congestion_form_20260626.md`,
`symmetric_vs_directional_measurement_20260626.md`, `data_pipeline_versions_and_conventions_20260628.md`,
this file. Figure: `Documents/L3_figs/centerline_vs_v1_cost_coverage.png`.
Clean-rebuild: `data_work/src/model/estimation_v2/` (README + primitives.py load_nodes).
Scratch scripts (sandbox `outputs/`, reusable): `ratio_run.py` (anchored), `treat_rule_run.py`
(flow-weighted Rule B + helpers), `short_run.py` (no-sorting), `bpr_run.py`/`bpr_bsweep.py`,
`demand_concentration2.py`, `cf1_symmetric.py`, `scheme_B.py`, `directional_od_bias.py`,
`cc_rerun.py` (connected-component re-run), `fig_centerline_vs_v1.py`.
Memory: `tidallanes-congestion-elasticity`, `tidallanes-data-conventions`, `tidallanes-reference-papers`.
