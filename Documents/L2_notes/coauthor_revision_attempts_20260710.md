# Coauthor revision attempts, 2026-07-10

## Output package

Main output:

`data_work/outputs/coauthor_revision_attempts_20260710_v1/`

Main script:

`data_work/src/model/full_estimation/run_coauthor_revision_attempts_20260710.py`

This is a first-pass diagnostic package for the five comments in `meeting_notes_20260710.md`. It is intended for result screening before slides or paper integration.

## 1. National jobs-housing evidence

Output folder:

`01_national_large_city_jobs_housing/`

At the 30% employment-core definition, the center-bound minus reverse share is:

- all cities: 0.87 pp;
- top 100 by inter-town flow: 2.23 pp;
- top 50: 3.17 pp;
- top 20: 4.50 pp;
- Beijing, Shanghai, Guangzhou, Shenzhen: 6.18 pp.

Across employment-core thresholds from 20% to 50%, the sign remains positive for every group. The top-100 result is especially stable, with a range of 0.14 pp across thresholds. This supports moving the main descriptive figure toward major cities instead of the all-city average.

## 2. Symmetric-input fundamentals

Output folder:

`02_symmetric_fundamental_distribution/`

The new diagnostic compares the fully re-estimated symmetric input world against the directed input world at the node level. The average effect remains limited, but the spatial profiles are more informative:

- residence fundamental, weighted L2 distance: 0.055 log point;
- workplace fundamental, weighted L2 distance: 0.049 log point;
- residence fundamental distance gradient: +0.0045 log point per 10 km;
- workplace fundamental distance gradient: -0.0026 log point per 10 km.

The current distance measure is distance to the employment-weighted grid center, not a true CBD or ring-road geometry. A true sixth-ring example still needs external ring/CBD geometry before it becomes a main Beijing figure.

## 3. Tidal time savings exposure

Output folder:

`03_tidal_time_savings_exposure/`

For the top-100 tidal-lane experiment:

- shortest-path OD exposure: 49,663 road commuters, 56.36% of the road-mode sample;
- short-run gross savings on treated links: 83,117 person-min/day;
- short-run net savings after fast-direction losses: 76,560 person-min/day;
- long-run gross savings on treated links: 48,252 person-min/day;
- long-run net savings after fast-direction losses: 42,845 person-min/day.

This directly answers the requested "how many people and how much total time" framing. The person-time totals are link-passage totals based on model link traffic, while the 56.36% exposure is an OD shortest-path exposure count.

## 4. Sorting fundamentals and wage trends

Output folder:

`04_sorting_fundamentals_wage_trends/`

The node-level sorting table now adds fixed fundamentals and endogenous components:

- `log_ubar` and `log_abar` are fixed under the tidal policy;
- effective residential amenity changes mechanically with `beta * Delta log l_R`;
- the wage component changes mechanically with `alpha * Delta log l_F`.

The fitted slopes recover the model parameters:

- `Delta log u_effective` on `Delta log l_R`: -0.100;
- `Delta log wage` on `Delta log l_F`: +0.050.

Distance gradients are limited:

- effective amenity gradient: +0.00023 log point per 10 km;
- wage gradient: -0.00024 log point per 10 km.

This means the current slide should not claim a separate fundamentals response. It can use these diagnostics to make the sorting channel explicit and transparent.

## 5. Population growth and urban expansion smoke

Output folder:

`05_population_growth_urban_expansion_smoke/`

Smoke design:

- keep baseline fundamentals fixed;
- scale commuting mass by 1.00, 1.15, and 1.30;
- use the original congestion anchor, so higher population raises traffic relative to the observed anchor;
- compare top-100 tidal reallocation with same-corridor bidirectional expansion;
- add an outer-residential-amenity 10% variant as the urban-expansion proxy.

All baseline and policy solves converged.

Long-run welfare effects in the compact city case:

- population scale 1.00: tidal +0.324%, same-corridor expansion +0.381%;
- population scale 1.15: tidal +0.302%, same-corridor expansion +0.359%;
- population scale 1.30: tidal +0.282%, same-corridor expansion +0.337%.

The outer-residential-amenity variant slightly lowers both effects. In this smoke design, population growth attenuates the percentage welfare effect, and same-corridor expansion remains above tidal reallocation. The selected top-100 tidal pairs are unchanged across the smoke grid.

This is not yet a final counterfactual. Congestion pricing is excluded because the toll and rebate accounting needs a separate closed welfare block.

