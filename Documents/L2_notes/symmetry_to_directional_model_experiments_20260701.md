# Symmetry-to-Directionality Experiments for the Model

**Date:** 2026-07-01  
**Purpose:** Turn the second model contribution into a concrete experiment suite: what changes when a model that looks symmetric in OD demand or travel cost is exposed to directional road costs and endogenous directional congestion.

## Recommendation

Use two layers.

1. **Exogenous travel-cost layer.** This is the clean diagnostic layer. It isolates the measurement wedge from replacing a directed cost surface with a symmetric one, without letting congestion feedback blur the mechanism. Use it to answer: "If the same fundamentals face symmetric rather than directed costs, how do welfare, sorting, and OD costs differ?"
2. **Endogenous anchored layer.** This is the main-model layer. It lets traffic per lane, edge times, and sorting re-equilibrate. Use it to answer: "After congestion adjusts, which symmetry mistakes survive and which are dampened?"

The paper should report both when the object is "what symmetry misses." The exogenous layer is easier to explain; the endogenous layer is the structural result.

## Current Objects Already in the Pipeline

- `run_stage2_gate5.py` already writes:
  - `counterfactuals/structural_cf_exogenous.csv`
  - `counterfactuals/structural_cf_endogenous.csv`
- Legacy CF1 symmetric costs already exist:
  - `CF1_symmetric_geometric` in the endogenous table
  - `CF1_symmetric_geometric_exog` in the exogenous table
- The current welfare-gauge fix means all `Wbar` columns in these tables now use the primitive Frechet denominator.
- The model-implied directional traffic map already exists in the slides appendix:
  - `Documents/PreSlides/figs/map_dir_asym.png`
  - Current headline: median directed traffic ratio about `2.9x`, p90 about `20x`.

**Important audit note.** The legacy endogenous CF1 symmetric run uses the symmetric cost field as an initial value, but the anchored congestion update still uses the directed observed-cost anchor. It is therefore not a strict symmetric-cost world. The dedicated runner below fixes this by symmetrizing both `tau_obs_anchor` and `tau_ff` for reciprocal edge pairs, while keeping primitives, parameters, fundamentals, welfare formula, and solver tolerances fixed.

## Experiment 1: Symmetric vs Directed Travel Costs

**Question.** What changes if the model replaces the directed road-cost surface with a reciprocal-pair symmetric cost?

**Implementation.**

- Exogenous version: hold travel costs at the symmetric field and solve shares. This isolates the measurement wedge.
- Endogenous version: use the symmetric field as the cost anchor and allow traffic and congestion to re-equilibrate.

**Current output from the dedicated runner.**

- Exogenous symmetric geometric cost surface: `Wbar +1.10%`, residence dissimilarity `3.37%`, workplace dissimilarity `6.16%`, OD-weighted bilateral cost `+0.13%`.
- Endogenous symmetric geometric cost surface with symmetric anchor: `Wbar +1.10%`, residence dissimilarity `3.36%`, workplace dissimilarity `6.15%`, OD-weighted bilateral cost `+0.10%`.

**Interpretation.** Symmetry is not an innocuous normalization: even before adding a policy or a place shock, it changes the spatial equilibrium enough to move roughly `3--6%` of residents/jobs in dissimilarity terms. The endogenous layer does not remove this wedge once the symmetric world is given its own symmetric congestion anchor.

## Experiment 2: Sorting Direction Under Symmetric vs Directed Costs

**Question.** Does directional congestion change where residents and jobs move after a transport shock?

**Proposed diagnostics.**

- For each scenario, compare:
  - residence dissimilarity and workplace dissimilarity;
  - mean `Delta log l_R` and `Delta log l_F` around treated slow-direction endpoints;
  - top residence and workplace movers;
  - correlation between `Delta log l_R` and distance to treated corridors or employment hubs.

**Why it matters.** If the symmetric model smooths the slow-direction bottlenecks, it can move residents or jobs toward the wrong side of the network. This is a cleaner mechanism statement than only comparing aggregate welfare.

**Best layer.** Start exogenous for a clean sign test. Then repeat in endogenous mode as the main result.

**Current output.** The symmetric cost surface increases the residence--employment centroid gap by about `0.029` grid units in both exogenous and endogenous layers. Under tidal lanes, slow-direction endpoints gain residents in both worlds, but the symmetric world predicts a larger response: mean `Delta log l_R` at slow endpoints is `+0.019` versus `+0.017` in the directed world, and mean `Delta log l_F` is more negative (`-0.020` versus `-0.006`).

## Experiment 3: Job-Hub Productivity Shock

**Question.** Does a productivity shock in job-heavy nodes transmit differently under directed congestion?

**Implementation.**

- Use the existing hub-productivity policy family:
  - `C4_hub_productivity_then_tidal` in the composite outputs;
  - or define a standalone `hub_productivity_10pct` policy with and without symmetric costs.
- Run the shock under:
  - directed costs;
  - symmetric costs;
  - directed endogenous congestion;
  - symmetric endogenous congestion.

**Expected mechanism.** A job-hub productivity shock raises workplace attractiveness. With directed congestion, the commuting burden depends on the inbound direction and on the roads feeding the hub. The symmetric model can understate or misplace the residential adjustment because it treats inbound and outbound access as the same transport object.

**Diagnostics.**

- Change in job concentration at hub nodes.
- Residence inflow into hub-accessible residential corridors.
- OD-weighted commute-cost change.
- Welfare change.
- Link-flow directional ratio around the hub.

**Current output.** A `10%` hub-productivity shock raises welfare in both cost worlds, but the symmetric world changes the transmission margins:

- Directed exogenous: `Wbar +1.19%`, OD cost `+2.05%`, residence dissimilarity `0.78%`, workplace dissimilarity `1.27%`.
- Symmetric exogenous: `Wbar +1.21%`, OD cost `+1.94%`, residence dissimilarity `0.74%`, workplace dissimilarity `1.22%`.
- Directed endogenous: `Wbar +1.17%`, OD cost `+1.93%`, residence dissimilarity `0.72%`, workplace dissimilarity `1.08%`.
- Symmetric endogenous: `Wbar +1.18%`, OD cost `+1.81%`, residence dissimilarity `0.68%`, workplace dissimilarity `1.03%`.

The symmetric model slightly understates the commuting-cost burden of hub growth and dampens the sorting response.

## Experiment 4: Residential Amenity or Housing-Supply Shock

**Question.** Does directionality alter the welfare and sorting impact of making outer-ring residential locations more attractive?

**Implementation.**

- Use existing outer-ring amenity policy:
  - `C2_outer_ring_amenity_then_tidal`.
- Add a symmetric-cost counterpart.

**Expected mechanism.** If residential amenities pull workers outward, the implied commute direction changes. Directional costs determine whether that sorting creates new slow-direction exposure or uses relatively fast reverse-commute corridors.

**Best layer.** Endogenous is preferable because the shock changes both where people live and the direction in which they load the road network.

**Current output.** A `10%` outer-ring amenity shock has a small welfare effect in both worlds but directionality affects sorting intensity:

- Directed exogenous: `Wbar +0.090%`, residence dissimilarity `0.412%`, workplace dissimilarity `0.369%`.
- Symmetric exogenous: `Wbar +0.085%`, residence dissimilarity `0.393%`, workplace dissimilarity `0.351%`.
- Directed endogenous: `Wbar +0.094%`, residence dissimilarity `0.362%`, workplace dissimilarity `0.314%`.
- Symmetric endogenous: `Wbar +0.092%`, residence dissimilarity `0.343%`, workplace dissimilarity `0.295%`.

The symmetric world again dampens the relocation response because it removes part of the directional commute penalty associated with outer-ring residence choices.

## Experiment 5: Transport Construction and Tidal Lanes

**Question.** Does a symmetric model rank transport policies differently?

**Policies.**

- Tidal lane reallocation, zero net lane-km.
- Same-corridor expansion, about `1,092` lane-km.
- qdelay expansion, about `1,140` lane-km in the directed mainline.
- Uniform expansion benchmark.

**Current mainline.**

- P1 tidal LT welfare: `+0.324%`, zero net lane-km.
- P2 same-corridor expansion LT welfare: `+0.382%`, about `1,092` lane-km.
- P3 qdelay expansion LT welfare: `+0.444%`, about `1,140` lane-km.

**Additional symmetry diagnostic.** Re-run these policies under a symmetric travel-cost surface. If policy rankings change, the paper can argue that directionality matters for choosing where to add or reallocate capacity, not only for measuring baseline congestion.

**Current output.** The ranking is stable in this first run, but the symmetric model reports larger targeted-policy welfare effects. These policy rows reselect top-100 corridors within each cost world, so they are a model-design diagnostic rather than a fixed-corridor treatment comparison:

- Directed endogenous: tidal `+0.324%`; same-corridor expansion `+0.382%`; qdelay expansion `+0.444%`; uniform expansion `+0.100%`.
- Symmetric endogenous: tidal `+0.522%`; same-corridor expansion `+0.569%`; qdelay expansion `+0.625%`; uniform expansion `+0.117%`.

This supports a cleaner claim: symmetry does not necessarily flip the ranking in the main policy set, but it materially changes magnitudes and predicts stronger sorting around treated corridors.

## Where to Add This in the Current Model Section

1. **This Paper slide.** Use one bullet: "Quantify what symmetry misses in the spatial model." Keep the national-to-segment facts as the first contribution and tidal policy as the third contribution.
2. **After Baseline Fundamentals.** Add a short model-result slide: "Directed costs imply directional traffic." Use the current `map_dir_asym.png` and the `2.9x` / `20x` summary.
3. **Before Tidal-Lane Results.** Add a diagnostic slide comparing symmetric vs directed costs:
   - exogenous CF1;
   - endogenous CF1;
   - sorting and OD-cost metrics.
4. **Appendix.** Put the productivity shock, outer-ring amenity shock, and policy-ranking diagnostics in backup until the full suite is rerun.

## Minimal Next Script

Added runner:

```text
data_work/src/model/full_estimation/run_symmetry_directionality_suite_gate5.py
```

It produces:

```text
data_work/outputs/symmetry_directionality_suite_gate5_bpr_pos05_v1/
  summary_by_scenario.csv
  suite_meta.json
```

The script reuses:

- `solve_equilibrium`
- `stage2_counterfactuals.summarise_vs_baseline`
- existing policy helper functions from `stage2_counterfactuals.py`

## Reporting Rule

The main text can now claim that the first-pass "what symmetry misses" suite is implemented and produces three robust statements:

1. Directional congestion is documented from national township pairs to local road segments.
2. The model already generates strongly directional baseline link traffic.
3. Replacing directed costs with a symmetric surface changes welfare by about `1.10%` and sorting by `3--6%` dissimilarity in both exogenous and endogenous layers.
4. Symmetry dampens place-shock sorting responses and changes policy magnitudes, even though the first policy-ranking comparison remains ordered as qdelay expansion > same-corridor expansion > tidal reallocation > uniform expansion.
