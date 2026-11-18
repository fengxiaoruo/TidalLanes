# Response to Discussant Audit, 2026-07-01

This note responds point by point to `tidal_project_discussant_report(1).pdf`. The audit is useful and mostly correct. The main implication is that the current package is internally reproducible, but several counterfactual comparisons should be revised before the deck is used as a final presentation version.

## Overall Response

We accept the main diagnosis. The packaged code reproduces the reference outputs, but reproducibility inside the package is not enough for a defensible policy comparison. The high-risk issues are concentrated in Result 4, Result 5, the policy appendix, and the first-best pricing benchmark. We will revise those parts before presenting the policy ranking as a headline result.

The revision will separate two objects that are currently mixed:

- Measurement counterfactuals, where the observed bilateral or edge cost matrix is altered and the exercise asks how the spatial allocation responds under a fixed-cost object.
- Capacity counterfactuals, where lane capacity or a congestion primitive is altered and traffic, route costs, and sorting re-equilibrate.

This separation will also govern the slide order. The tidal-lane policy will remain the main capacity counterfactual. Fixed-cost symmetrization and fixed-cost speed reductions will be moved to measurement or robustness slides unless we implement the same primitive shock under fixed-Xi and full-equilibrium variants.

## A1. Result 4, CF1 Symmetric Cost

**Comment.** The endogenous CF1 symmetric-cost result may be mechanical because the symmetrized cost enters only as `tau_min_init`, while the anchored congestion update still uses the original directed observed-cost anchor.

**Response.** We agree. In the current implementation, the endogenous CF1 line should not be interpreted as an economic result that directional symmetrization is absorbed by endogenous congestion. The shock does not enter the observed anchor or free-flow primitive. It therefore behaves like a numerical initialization exercise under the original anchored congestion system.

**Revision.** We will remove the claim that symmetrization collapses under endogenous feedback. If CF1 remains in the deck, it will be labeled as a fixed-cost measurement counterfactual. A separate endogenous version will be reported only if the symmetric shock is imposed on a primitive that actually enters the congestion equation, such as `tau_obs_anchor` or a free-flow/capacity primitive.

**Action.**

- Result 4: rewrite or split the table.
- Code: add an explicit `shock_type` or similarly named switch for CF1 variants.
- Test: fail if an advertised endogenous CF1 only changes `tau_min_init`.

## A2. Result 4, CF3 Fixed-Tau Versus Endogenous Shock

**Comment.** The slide states that the same primitives and shocks are compared with and without congestion feedback, but fixed-tau CF3 divides observed travel time by 1.10, whereas endogenous CF3 scales lane capacity by 1.10.

**Response.** We agree. The current table compares a fixed travel-time measurement shock to a capacity shock. Those are not the same primitive shock. The sentence "same primitives and shocks" is inaccurate.

**Revision.** Result 4 will be reframed around two classes of counterfactuals. Fixed-tau exercises will be presented as measurement benchmarks. Capacity policies will be presented as changes in `n_kl` under the anchored congestion technology. If we want a genuine exogenous-versus-endogenous comparison, both sides must perturb the same primitive, for example a fixed-Xi mechanical capacity response versus a full-equilibrium capacity response.

**Action.**

- Slide text: delete "same primitives and shocks."
- Table design: separate fixed-cost benchmarks from capacity-policy counterfactuals.
- Code: make the primitive being shocked explicit in output columns.

## A3. Result 5, P3 Qdelay Expansion Lane-Kilometres

**Comment.** P3 qdelay expansion reuses P2 lane-kilometres. The actual P3 top100 qdelay expansion is about 1,140.30 lane-km rather than 1,091.50 lane-km.

**Response.** We agree. This is a code-level accounting error. P3 cannot be described as a same-lane-km benchmark until lane-kilometres are computed from its own selected edges or the qdelay selection is budget-matched.

**Revision.** Each policy will compute `lane_km_added` from its own counterfactual capacity vector. For the slides, we will either report P3 with its actual lane-km and label it as not budget-matched, or implement budget-matched qdelay selection before retaining it as a same-cost benchmark.

**Action.**

- Code: compute P3 lane-km from P3 selected edges.
- Slide: change P3 lane-km to actual lane-km or remove P3 from the same-budget table.
- Test: assert `lane_km_added = sum((n_cf - n_0) * ell)` for each policy.

## A4. P4/P5 Short-Run and Long-Run Labels

**Comment.** P4/P5 are inside the horizon loop, but the code does not pin residence and workplace distributions in the short run. Their short-run and long-run outputs are therefore identical.

**Response.** We agree. The current P4/P5 horizon labels are not valid. They should not be used for a short-run versus long-run interpretation.

**Revision.** We will add the same horizon handling used in the tidal and expansion policies. Short-run capacity policies must pin `l_R` and `l_F` at baseline. Long-run policies may allow sorting.

**Action.**

- Code: pass `l_R_fixed=baseline.l_R` and `l_F_fixed=baseline.l_F` for short-run P4/P5.
- Output: add a flag that records whether sorting was pinned.
- Test: assert short-run residence and workplace distributions match baseline.

## A5. Cost-Effectiveness Table Averages ST and LT

**Comment.** `policy_cost_effectiveness.csv` averages short-run and long-run rows but names the column `Wbar_pct_change_lt`.

**Response.** We agree. The current cost-effectiveness file is mislabeled. It should not be interpreted as a long-run cost-effectiveness table.

**Revision.** The cost-effectiveness output will be rebuilt from long-run rows only, or it will contain separate short-run, long-run, and horizon-average columns. The slide should use the long-run column for the infrastructure benchmark.

**Action.**

- Code: filter `horizon == "long_run"` before computing LT cost-effectiveness.
- Output: rename any averaged column as `Wbar_pct_change_mean_over_horizons`.
- Test: assert LT cost-effectiveness equals the long-run policy row.

## A6. P6 First-Best Pricing

**Comment.** The first-best pricing implementation adds tolls to the free-flow floor and computes revenue using baseline traffic rather than scenario traffic. The `+25.1%` realized welfare result is provisional.

**Response.** We agree. P6 is not ready to be a headline policy comparison. The current implementation is useful as a diagnostic, but it does not yet implement a clean generalized-cost toll wedge with scenario-flow revenue accounting.

**Revision.** P6 will be removed from the main headline and clearly labeled as illustrative if retained in the appendix. A proper pricing counterfactual will keep physical travel time and toll wedges separate, compute revenue from counterfactual traffic, and state the rebate incidence.

**Action.**

- Slide: drop P6 from the headline infrastructure comparison.
- Appendix: label P6 as provisional or remove it until the implementation is revised.
- Code: implement tolls as generalized-cost wedges, not as a free-flow travel-time floor.

## B1. Equilibrium Slide Contradicts Directed Environment

**Comment.** The model environment emphasizes directed links and `t_kl != t_lk`, but the equilibrium slide states `t_kl = t_lk` on each physical road.

**Response.** We agree. This is a slide notation error and should be fixed immediately.

**Revision.** The equilibrium slide will define primitives on directed links. It will refer to directed free-flow times, directed capacity, and an anchored congestion map from `(Xi_kl, n_kl)` to directed realized edge costs.

**Action.**

- Slide: replace the symmetric-road sentence.
- Notation: avoid using the same lambda symbol for congestion elasticity and any equilibrium multiplier.

## B2. Kappa Invariance Statement

**Comment.** The appendix says counterfactual ratios are invariant to `kappa`, but code comments and audit results say the ratios are only approximately insensitive.

**Response.** We agree. The word "invariant" is too strong.

**Revision.** The slide will say that `kappa` is fixed across baseline and counterfactuals, and that headline ratios exhibit limited sensitivity over audited values. If space permits, a one-line sensitivity table will be added in the appendix.

**Action.**

- Slide: replace "invariant" with "limited sensitivity over audited kappa values."
- Docs: align code comments and slide text.

## B3. Congestion Parameter Provenance

**Comment.** `parameters.json` records the legacy placeholder lambda `0.07144`, while the main solver and slides use effective anchored congestion elasticity `1.1`.

**Response.** We agree. The current metadata is confusing, even though the numerical solver uses the intended effective congestion lambda.

**Revision.** Outputs will report the full effective configuration: `theta`, `alpha`, `beta`, route-scale `kappa`, legacy parameter lambda if retained for compatibility, congestion form, effective congestion lambda, anchor source, and selection rule.

**Action.**

- Code: write `effective_config.json` in each main output folder.
- Docs: update `parameters.py` docstring and package source notes.
- Slide: state `lambda_phys = 1.1` as the effective anchored congestion parameter.

## B4. Anchored BPR Equation and Notation

**Comment.** The body slides present a simple power form, while the implementation uses an observed-anchor equation. Edge minutes and bilateral commuting costs also use similar notation.

**Response.** We agree. The current notation makes it too hard to distinguish conceptual technology, anchored implementation, edge minutes, and bilateral costs.

**Revision.** The main estimation slide will present the anchored equation directly, or the body slide will explicitly state that the counterfactual implementation uses the anchored form. We will also consider using `m_kl` for edge travel minutes and `C_ij` for bilateral commuting cost to reduce ambiguity.

**Action.**

- Slide: move the anchored implementation out of appendix-only status.
- Notation: separate edge minutes from bilateral commuting cost.

## B5. Existence and Uniqueness

**Comment.** The statement that existence and uniqueness follow from Allen and Arkolakis is too strong for the extended directed congestion model.

**Response.** We agree. The AA block has its own theoretical conditions, but the full directed anchored congestion fixed point is solved numerically.

**Revision.** The slide will distinguish the no-congestion AA block from the full directed-congestion equilibrium. The full model will be described as a damped fixed point with convergence checks unless we add a formal contraction result.

**Action.**

- Slide: replace the unconditional existence/uniqueness claim.
- Appendix: report numerical convergence diagnostics.

## C1. Absolute or External Paths in Standalone Package

**Comment.** The slides and code refer to local notes that are not included in the standalone package.

**Response.** We agree. A shareable package should not rely on a local path outside the package.

**Revision.** We will either copy the relevant note summary into `docs/` or remove the local note reference from slides. Source notes should point to package-relative documentation.

**Action.**

- Slide: remove `Documents/L2_notes/...` references.
- Package: add a compact parameter and estimation note under `docs/`.

## Slide-Specific Responses

**Model environment.** Accepted. We will distinguish directed free-flow asymmetry from congestion-induced directional asymmetry.

**Result 2 decomposition.** Accepted. We will label the decomposition as OD/person-weighted generalized commuting cost, not average edge speed or average edge travel time.

**Result 4.** Accepted. We will split measurement counterfactuals from capacity-policy counterfactuals.

**Result 5.** Accepted. We will update P3 lane-km or remove P3 from same-budget comparison until budget matching is implemented.

**Appendix fundamentals.** Accepted. The `kappa` statement will be weakened and supported with sensitivity evidence.

**Appendix solver.** Accepted. Effective congestion lambda and anchor source will be reported package-locally.

**Appendix policy and P6.** Accepted. P6 will be provisional until the toll wedge and revenue accounting are revised.

**Robustness slide.** Accepted. We will state that the raw directed traffic panel is still missing and that the congestion elasticity is externally calibrated.

## Code and Testing Plan

We will implement the audit's priority-one fixes before treating Result 4 and Result 5 as final:

1. Add an explicit shock taxonomy for counterfactuals.
2. Recompute lane-kilometres from each policy's own capacity vector.
3. Add short-run sorting pins for all horizon-labeled capacity policies.
4. Rebuild cost-effectiveness outputs from long-run rows or explicitly name averaged columns.
5. Move P6 pricing to provisional status until generalized-cost tolls and scenario-flow revenue are implemented.

Minimal tests will cover lane-kilometre accounting, LT-only cost-effectiveness, short-run marginal pinning, P4/P5 horizon behavior, and CF1 shock entry.

## Presentation Plan Before Code Fixes

If the deck must be presented before the code fixes are complete, we will make three defensive edits:

1. Remove the "same shock" language from Result 4.
2. Remove the claim that P3 is a same-lane-kilometre benchmark, or drop P3 from the main infrastructure table.
3. Remove first-best pricing as a headline number and keep it, at most, as an illustrative appendix item.

These changes preserve the stable parts of the deck: directional congestion measurement, the directed-link model setup, the primitive welfare object, and the tidal P1 headline values that are reproducible inside the package.
