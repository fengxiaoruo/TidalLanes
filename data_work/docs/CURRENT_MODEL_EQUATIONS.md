# Current Model Equations

This note rewrites the **current implemented model** in [`spatial_equilibrium.py`](/Users/fxr/Desktop/TidalLanes/data_work/src/model/spatial_equilibrium.py) using notation aligned with the paper draft.

The goal is to document the model that is actually running in code, not the final target model in the notes.

## 1. Objects and Notation

Let the city be partitioned into grid cells indexed by `i,j,k,l = 1,\dots,N`.

- `L_i`: residents living in grid `i`
- `H_j`: jobs located in grid `j`
- `M_{ij}`: commuting flow from residence `i` to workplace `j`
- `t_{kl}`: direct travel time on directed edge `(k,l)`
- `tau_{ij}`: bilateral commuting cost from `i` to `j`
- `n_{kl}`: lane capacity on directed edge `(k,l)`
- `phi_{kl}`: flow assigned to edge `(k,l)`
- `\bar L = \sum_i L_i`: total population

Observed objects in the current code are:

\[
L_i^{obs}, \qquad H_j^{obs}, \qquad M_{ij}^{obs}, \qquad t_{kl}^{obs}, \qquad t_{kl}^{ff}, \qquad n_{kl}^{obs}.
\]

The current implementation uses the directed grid graph constructed in stage 06 and the QSM-style exports from stage 08.

## 2. Direct Edge Cost and Bilateral Commuting Cost

For each directed edge `(k,l)`, the model reads both the observed edge time `t_{kl}^{obs}` and the free-flow edge time `t_{kl}^{ff}` from the data pipeline.

In the current code, bilateral commuting costs are approximated by a **positive-probability route system** on the directed grid graph rather than an all-or-nothing shortest path.

For each destination `j`, define the continuation value `Z_i(j)` recursively as

\[
Z_i(j) = \sum_{(i,m)\in\mathcal E} t_{im}^{-\theta} Z_m(j),
\qquad
Z_j(j)=1.
\]

Then the current code defines

\[
\tau_{ij}^{-\theta} = Z_i(j),
\qquad
\tau_{ij} = Z_i(j)^{-1/\theta}.
\]

This is a soft route aggregator on the grid network: lower-cost paths receive larger weights, but non-minimum paths can still receive positive probability.

For intrazonal commuting, the code assigns a proxy cost:

\[
\tau_{ii} = \frac{1}{2}\operatorname{median}\{ t_{kl} : (k,l)\in \mathcal E,\; t_{kl}>0 \}.
\]

## 3. Gravity-Style Diagnostic for `theta`

The code estimates a diagnostic elasticity `\theta` from the observed OD matrix and soft bilateral commuting costs using a two-way fixed-effects gravity regression:

\[
\log M_{ij}^{obs} = \mu_i + \nu_j - \theta \log \tau_{ij} + \varepsilon_{ij}.
\]

This diagnostic estimate is saved, but the equilibrium solver normally uses an externally supplied value of `\theta`.

## 4. Cross-Sectional Diagnostic for `lambda`

Observed OD flows are assigned using the positive-probability route system, producing observed edge flows:

\[
\phi_{kl}^{obs}.
\]

The code then defines observed per-lane density:

\[
\rho_{kl}^{obs} = \frac{\phi_{kl}^{obs}}{n_{kl}^{obs}}.
\]

The congestion diagnostic is a lane-bin fixed-effects regression of the form:

\[
\log \left(\frac{t_{kl}^{obs}}{t_{kl}^{ff}}\right)
= \eta_{\text{lane-bin}(kl)} + \lambda \log \rho_{kl}^{obs} + u_{kl}.
\]

Again, this estimate is diagnostic only; the equilibrium solver normally uses an externally supplied `\lambda`.

## 5. Fundamentals

Define residential and employment shares:

\[
\ell_i^L = \frac{L_i}{\bar L},
\qquad
\ell_j^H = \frac{H_j}{\bar L}.
\]

The current implementation uses two node-level fundamentals:

- `\bar u_i^\theta`: residential amenity term
- `\bar a_j^\theta`: employment productivity term

The code works directly with these `\theta`-power objects.

## 6. Commuting Mass and Choice Weights

Given current commuting costs and current population and job distributions, define:

\[
U_i^\theta = \bar u_i^\theta (\ell_i^L)^{\beta\theta},
\qquad
A_j^\theta = \bar a_j^\theta (\ell_j^H)^{\alpha\theta}.
\]

Then the OD commuting mass is:

\[
m_{ij} = \tau_{ij}^{-\theta} U_i^\theta A_j^\theta.
\]

The implied commuting flow is:

\[
M_{ij} = \bar L \cdot \frac{m_{ij}}{\sum_{r,s} m_{rs}}.
\]

Hence equilibrium residents and jobs satisfy:

\[
L_i = \sum_j M_{ij},
\qquad
H_j = \sum_i M_{ij}.
\]

This is the fixed-point system solved by the inner equilibrium loop.

## 7. Inversion of Baseline Fundamentals

Using the observed distributions `L_i^{obs}` and `H_j^{obs}`, the code inverts `\bar u_i^\theta` and `\bar a_j^\theta` so that the observed city is rationalized under observed commuting costs.

Let:

\[
\ell_i^{L,obs} = \frac{L_i^{obs}}{\bar L},
\qquad
\ell_j^{H,obs} = \frac{H_j^{obs}}{\bar L}.
\]

Then the inverted fundamentals satisfy the iterative system:

\[
\bar u_i^\theta
=
\frac{(\ell_i^{L,obs})^{1-\beta\theta}}
{\sum_j \tau_{ij}^{-\theta}\,\bar a_j^\theta\,(\ell_j^{H,obs})^{\alpha\theta}},
\]

\[
\bar a_j^\theta
=
\frac{(\ell_j^{H,obs})^{1-\alpha\theta}}
{\sum_i \tau_{ij}^{-\theta}\,\bar u_i^\theta\,(\ell_i^{L,obs})^{\beta\theta}}.
\]

The code repeatedly updates these objects and normalizes them by their geometric means.

## 8. Edge Flows

The current model now uses a **soft route assignment** on the directed grid graph.

For a fixed destination `j`, the implied probability of moving from node `i` to adjacent node `m` is

\[
P_{im}(j)
=
\frac{t_{im}^{-\theta} Z_m(j)}{Z_i(j)}.
\]

Hence lower-cost moves receive higher probability, but all supported outgoing links can carry positive mass.

Given OD flow `M_{ij}`, the expected flow on edge `(k,l)` is the sum of expected route visits:

\[
\phi_{kl}
=
\sum_{i,j} M_{ij}\,\pi_{ij}^{kl},
\]

where `\pi_{ij}^{kl}` is the implied edge-use intensity from the soft assignment recursion.

This is still a reduced-form route system on the grid network, but it is closer to Allen--Arkolakis style link sharing than all-or-nothing shortest-path allocation.

## 9. Congestion Equation

This is where lane capacity enters the model directly.

Let observed and counterfactual per-lane density be:

\[
\rho_{kl}^{obs} = \frac{\phi_{kl}^{obs}}{n_{kl}^{obs}},
\qquad
\rho_{kl}^{cf} = \frac{\phi_{kl}^{cf}}{n_{kl}^{cf}}.
\]

The current code uses free-flow travel time as the congestion primitive:

\[
t_{kl}^{cf}
=
t_{kl}^{ff}
\left(
\frac{\phi_{kl}^{cf}}{n_{kl}^{cf}}
\right)^{\lambda}.
\]

So lane capacity affects equilibrium and counterfactual travel times through congestion, while `t_{kl}^{ff}` plays the role of the infrastructure-side primitive.

## 10. Outer Congestion Fixed Point

The model solves a nested fixed point:

1. start from current edge times `t_{kl}`
2. compute soft route costs `\tau_{ij}`
3. solve the inner population--employment fixed point
4. assign OD flows to edges and get `\phi_{kl}`
5. update edge times through the congestion equation
6. damp the update:

\[
t_{kl}^{new,\,damped}
=
\delta t_{kl}^{new} + (1-\delta)t_{kl}^{old}
\]

7. iterate until

\[
\max_{(k,l)} \left| \log t_{kl}^{new} - \log t_{kl}^{old} \right|
< \text{tol}.
\]

## 11. Welfare

Given commuting masses `m_{ij}`, the code reports welfare as:

\[
W = \left( \sum_{i,j} m_{ij} \right)^{1/\theta}.
\]

This is the scalar welfare object currently used in summaries and counterfactual comparisons.

## 12. Tidal-Lane Counterfactual

The code supports two broad ways of choosing treated edges:

- top congested edges
- top tidal-asymmetry edges

Once a set of treated directed edges is selected, lane reallocation is implemented as:

\[
n_{kl}^{cf} = n_{kl}^{obs} + \Delta n
\]

for each treated direction, and if the reverse edge `(l,k)` exists,

\[
n_{lk}^{cf} = \max\{0.25,\; n_{lk}^{obs} - \Delta n\}.
\]

The new lane vector `n^{cf}` is then fed back into the congestion equation and the congested equilibrium is resolved.

## 13. Summary

The current implemented model is:

- a directed grid model with observed and free-flow edge travel times
- soft bilateral commuting costs with positive-probability route choice
- gravity-style commuting mass allocation
- inversion of residential and employment fundamentals
- soft route assignment
- congestion driven by edge flow per lane
- tidal-lane policies implemented as directional lane reallocation

In compact notation, the implemented system is:

\[
\tau_{ij}^{-\theta}=Z_i(j),
\qquad
Z_i(j)=\sum_{(i,m)\in\mathcal E} t_{im}^{-\theta} Z_m(j),
\qquad
Z_j(j)=1,
\]

\[
M_{ij} \propto \tau_{ij}^{-\theta}\bar u_i^\theta(\ell_i^L)^{\beta\theta}\bar a_j^\theta(\ell_j^H)^{\alpha\theta},
\]

\[
L_i = \sum_j M_{ij},
\qquad
H_j = \sum_i M_{ij},
\]

\[
\phi_{kl}
=
\sum_{i,j} M_{ij}\,\pi_{ij}^{kl},
\]

\[
t_{kl}^{cf}
=
t_{kl}^{ff}
\left(
\frac{\phi_{kl}^{cf}}{n_{kl}^{cf}}
\right)^{\lambda}.
\]

This is the exact model logic currently implemented in code.
