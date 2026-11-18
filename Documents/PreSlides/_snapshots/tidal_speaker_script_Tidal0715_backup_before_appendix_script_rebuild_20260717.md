# Directional Congestion: 25-Minute Speaker Script

Deck: `Tidal0715.pdf`  
Venue: SMUE Wuhan, 2026  
Main presentation: Slides 1--25  
Appendix: Slides 26--43, for questions only

## Timing plan

| Slide | Title | Target | Cumulative |
|---:|---|---:|---:|
| 1 | Directional Congestion | 0:20 | 0:20 |
| 2 | Roadmap | 0:30 | 0:50 |
| 3 | Directional Congestion in Cities | 1:20 | 2:10 |
| 4 | Motivation | 1:55 | 4:05 |
| 5 | This Paper | 1:45 | 5:50 |
| 6 | Literature | 1:10 | 7:00 |
| 7 | Directionality Appears at National and Road-Segment Scales | 2:40 | 9:40 |
| 8 | Jobs--Housing Separation Generates Directional Commuting | 2:20 | 12:00 |
| 9 | Model: Environment | 0:35 | 12:35 |
| 10 | Individual Location Choice | 0:40 | 13:15 |
| 11 | Bilateral Commuting Costs and Flows | 0:45 | 14:00 |
| 12 | Amenity, Productivity, and Location Choices | 0:40 | 14:40 |
| 13 | Feedback between Location Choice, Route Traffic, and Traversal Cost | 1:10 | 15:50 |
| 14 | Equilibrium | 0:40 | 16:30 |
| 15 | Estimation: Data and Parameters | 1:05 | 17:35 |
| 16 | Directed Link Times Are Constructed from Segment Speeds | 1:00 | 18:35 |
| 17 | Estimation: Invert, Anchor, and Solve | 0:55 | 19:30 |
| 18 | Baseline Fundamentals | 0:35 | 20:05 |
| 19 | What Symmetry Misses | 0:25 | 20:30 |
| 20 | Where the Tidal Policy Lands | 0:45 | 21:15 |
| 21 | Short Run vs. Long Run | 0:50 | 22:05 |
| 22 | Short-Run Time Gains Are Reversed by Long-Run Reallocation | 1:25 | 23:30 |
| 23 | Long-Run Sorting | 0:30 | 24:00 |
| 24 | Transportation Policy Comparison | 0:35 | 24:35 |
| 25 | Conclusion | 0:25 | 25:00 |

Section targets are 7:00 for the opening and introduction, 5:00 for the directional facts, 4:30 for the model, 4:00 for data and estimation, and 4:30 for counterfactuals and results.

## Before presenting

- Slide 19 uses arithmetic symmetrization as the main physical-time comparison. The fixed-fundamentals 1.10 percent welfare result remains an appendix diagnostic and is not the full re-estimation result.
- Keep the model section in $t_{kl}$ and $\tau_{ij}$. Introduce physical minutes $m_{kl}$ only on Slide 16 and on the physical-time result slides, using $t_{kl}=m_{kl}^{1/\theta}$. Never convert $\tau_{ij}$ into seconds.
- Slide 22 reports citywide expected additive physical time from equilibrium link passages and minutes. The old treated-link segment-person-time accounting is retained only on Appendix Slide 30 as a local engineering-incidence measure.

## Slide 1. Directional Congestion

Good morning. During the peak, the same road can be congested in one direction and relatively clear in the other. I ask how this directionality enters spatial equilibrium, and whether tidal lanes can exploit it without adding road width.

[Click to Roadmap.]

## Slide 2. Roadmap

The talk proceeds from the empirical object to the policy counterfactual. I begin with directional congestion and its spatial source. I then introduce a route-inclusive quantitative spatial model, explain how the directed network is measured and anchored, and conclude with the short-run and long-run effects of tidal-lane reallocation.

[Click to the city photographs.]

## Slide 3. Directional Congestion in Cities

These photographs come from Beijing, Wuhan, Shenzhen, and Kuala Lumpur. They are not intended as a comparable dataset. They make the object visible before I define it statistically.

In each case, the relevant contrast is within the same physical corridor and at the same time. One direction carries a dense queue, while the reciprocal direction has spare capacity. A conventional congestion measure records that the road is congested on average. A directional measure records that the two sides of the road are in different economic states.

That distinction matters for two reasons. Commuters face the cost from home to work, not an average of both directions. The feasible policy also operates by direction: a tidal lane transfers capacity from the less congested side to the more congested side during a peak window.

[Click to Motivation.]

## Slide 4. Motivation

Directional congestion is tied to urban spatial structure. Jobs concentrate in employment centers, while residences occupy a broader area. During the morning peak, this separation creates a center-bound demand imbalance. The reverse direction can retain spare capacity even when the inbound direction is congested.

[Optional video cue: play no more than 20 seconds.]

The video provides the policy intuition. A tidal lane changes which direction can deploy a lane at a particular time. Total road width is fixed. The intervention is therefore different from an ordinary capacity expansion.

Most quantitative spatial models represent commuting frictions through bilateral or route-level costs. That is appropriate for many questions, but reciprocal link asymmetry is often averaged out before the equilibrium is solved. Transportation research studies peak-direction bottlenecks and reversible lanes, yet those engineering objects rarely enter a spatial-equilibrium counterfactual with endogenous congestion and long-run sorting.

The open question is therefore not only whether the slow direction becomes faster. We need to know how route traffic responds, whether congestion moves elsewhere, and how residence and workplace choices adjust over time.

[Click to This Paper.]

## Slide 5. This Paper

The paper makes three contributions.

First, I measure directionality at two spatial scales. At the national trip level, the flow-weighted median speed asymmetry ratio is 0.878 across the top 100 congested cities. Within Beijing, 179 two-way centerlines satisfy a stringent one-third ratio threshold during the morning peak, covering about 213 kilometers.

Second, I quantify what reciprocal symmetry misses. The more congested direction carries 56.3 percent of AM link passages, so symmetry understates experienced travel time by about 5 percent. It also understates peripheral peak travel time and, in turn, central residential fundamentals.

Third, I evaluate a top-100 tidal policy under endogenous congestion. One lane moves toward the slow morning direction on each treated reciprocal corridor. Expected utility rises by 0.87 percent in the short run and 0.32 percent in the long run. A targeted expansion reaches a somewhat greater long-run gain, but requires roughly 1,100 additional lane-kilometers.

Across all commuters, total time saved is 1,031 hours per morning peak in the short run. Long-run reallocation raises total time 352 hours above baseline, even though expected utility remains above baseline.

[Click to Literature.]

## Slide 6. Literature

The paper connects three literatures.

The first studies directional congestion through bottlenecks, high-frequency urban speeds, and reversible-lane operations. I contribute measurement that runs from national trips to individual directed road segments.

The second is quantitative spatial equilibrium. This literature links commuting costs to residence, employment, productivity, amenities, and market access. I retain that structure but allow link congestion and capacity to differ by direction.

The third evaluates transportation policy through pricing, road expansion, optimal networks, and public transit. Tidal lanes occupy a distinct supply margin. They reallocate a fixed amount of road width across directions, so the policy has no counterpart in a symmetric network representation.

[Click to the directional facts.]

## Slide 7. Directionality Appears at National and Road-Segment Scales

The first bullet defines the speed asymmetry ratio. For a reciprocal pair, it is the lower directional speed divided by the higher directional speed. The ratio lies between zero and one. A value of one means equal speeds, and lower values indicate stronger directional imbalance.

Start with the national trip-level evidence on the left. The histogram plots the distribution across the top 100 congested cities. The flow-weighted median ratio is 0.878, and 58 percent of pair-flow occurs on pairs with ratios at or below 0.90. Directionality is therefore not confined to a few extreme observations.

The adjacent scatter asks whether the ratio varies systematically with congestion severity. More congested cities tend to have lower ratios. The correlation is minus 0.48 among 99 cities. I interpret this as a descriptive city-level association: congestion severity and directional imbalance co-move, but the scatter is not a causal design.

The right side moves from trips to the road-segment margin on which a tidal lane operates. Among 3,670 Beijing two-way centerlines, the median ratio is 0.884. The map plots the spatial distribution of this directed asymmetry. Using the stricter one-third threshold identifies 179 candidate corridors, approximately 213 kilometers.

The national and Beijing objects are deliberately different. National trip routes establish prevalence across cities. Directed centerlines identify where a capacity transfer could occur within one city. The next slide explains why peak commuting demand has a systematic direction in the first place.

[Click to Jobs--Housing Separation.]

## Slide 8. Jobs--Housing Separation Generates Directional Commuting

The left column asks whether center-bound commuting is a general feature of major cities. Within each city, I rank towns by workplace-side commuter mass. Core towns cover the first 30 percent of that mass, and the remaining towns form the periphery.

Each vertical stem in the lower-left figure is one of the top 20 cities by inter-town commuting flow. Stem height is the center-bound commuting share minus the reverse share. Every gap is positive. The flow-weighted gap is 4.5 percentage points in the top-20 sample. It remains positive as the sample expands, at 3.2 percentage points for the top 50 and 2.2 percentage points for the top 100. Center-bound AM commuting is therefore not driven by one city or one sample cutoff.

The right column gives the Beijing mechanism in greater spatial detail. The residential-density map is dispersed across the city. Job density is more concentrated. The Duncan dissimilarity index is 0.26, compared with 0.004 under random allocation. This is a descriptive measure of jobs--housing separation, not a causal estimate.

In Beijing, peripheral-to-core commuting exceeds the reverse by 9.4 percentage points. Beijing therefore illustrates both the spatial separation and the corresponding commuting imbalance.

These facts motivate a model in which residence, workplace, route, and directed congestion are determined jointly.

[Click to Model: Environment.]

## Slide 9. Model: Environment

Locations host residents and jobs, and each physical road is represented by two directed links. Free-flow link cost and realized link cost are denoted by $t_{kl}^{ff}$ and $t_{kl}$. Realized link cost responds to model-implied traffic per lane, so the two directions can differ in equilibrium. A tidal policy changes the lane split while holding total corridor width fixed.

[Click to Individual Location Choice.]

## Slide 10. Individual Location Choice

An individual jointly chooses a residence, a workplace, and a route. Utility combines residential amenity, workplace wage, route cost, and an idiosyncratic Fréchet term. Directed link costs multiply along a route. The route belongs inside the choice object because a change on one segment can redirect traffic even when the residence--workplace pair remains unchanged.

[Click to Bilateral Commuting Costs.]

## Slide 11. Bilateral Commuting Costs and Flows

For each residence--workplace pair, the first equation aggregates all feasible routes into the bilateral commuting cost index, tau i j. Summing route-specific choices then delivers the OD flow L i j and the gravity equation. Pi and P are the origin and destination market-access terms that reconcile OD choices with residence and workplace totals. Directed links imply that tau i j generally differs from tau j i. Tau is an index used in choice and welfare equations, not OD travel minutes.

[Click to Amenities and Productivity.]

## Slide 12. Amenity, Productivity, and Location Choices

Local productivity combines a workplace fundamental with employment density through alpha. Residential amenity combines its fundamental with residential density through beta. Positive alpha captures workplace agglomeration; negative beta captures residential crowding. The two equations determine residence and workplace shares given bilateral commuting costs. These endogenous location choices form the long-run sorting margin.

[Click to the congestion feedback.]

## Slide 13. Feedback between Location Choice, Route Traffic, and Traversal Cost

This slide contains the model's endogenous-congestion channel.

For one commuter traveling from i to j, kappa i j k l is the expected number of passages over directed link k l, conditional on that OD pair. It integrates over the commuter's route distribution. On a simple route network, where the same directed link is used at most once, kappa is the probability that the route uses that link. In the all-route representation it is an expected passage count, so repeated passages are counted. It converts an OD commuter into segment exposure. Multiplying that expected use by the OD flow and summing over every origin and destination gives Xi k l, the model-implied directed link load.

Traffic per lane then raises the directed link cost $t_{kl}$. A lane change affects the denominator directly; route substitution and location choice affect Xi in equilibrium. The updated link cost re-enters route choice, changes bilateral costs, and alters the OD flows that generated the load. Congestion is therefore a fixed point, not a one-time engineering calculation. Lambda is calibrated as the elasticity of physical time with respect to traffic per lane. Because $t_{kl}=m_{kl}^{1/\theta}$, the corresponding elasticity in the link-cost equation is $\lambda/\theta$.

The welfare expression is the Fréchet expected-utility index over the complete residence--workplace--route opportunity set. This is why welfare and an average realized route-cost statistic need not move one for one.

[Click to Equilibrium.]

## Slide 14. Equilibrium

An equilibrium contains residence and workplace populations, OD flows, bilateral costs, directed link loads, directed link costs, and market-access terms. Choice probabilities, gravity, amenities, productivity, and the congestion relation must hold jointly. The empirical implementation recovers the fundamentals that rationalize the baseline allocation, anchors link costs to observed conditions, and then resolves this system after each lane reallocation.

[Click to Data and Parameters.]

## Slide 15. Estimation: Data and Parameters

This slide lists raw inputs and their coverage, not model variables constructed from them. Home and workplace locations and the road-commuting matrix come from Baidu 2022. Directed speeds come from the 10-minute road-segment panel for May 19 through 25, 2025. The morning window is seven to nine, and the free-flow window is ten at night to five in the morning. Road geometry, functional class, and lane records complete the network inputs. The spatial units are three-by-three-kilometer Beijing grid cells and their directed adjacencies.

The lower table fixes the four calibrated parameters used throughout the main specification. Theta is 6.83 and governs Fréchet dispersion in commuting cost, following the quantitative spatial literature cited in the table. Alpha is 0.05 for workplace agglomeration, and beta is minus 0.10 for residential crowding. Lambda is 1.1 for the response of physical time to traffic per lane, calibrated to Barwick and coauthors. The baseline contains 1,104 cells, 2,642 directed links, 310,525 OD pairs, and 88,110 road commuters.

[Click to the link-cost construction.]

## Slide 16. From Segment Speeds to Directed Link Costs

For each ordered pair of adjacent grid cells, I select the path that minimizes observed morning minutes on the directed raw-road network. The figure shows one actual Beijing pair of three-by-three-kilometer cells. The selected path contains five raw-road pieces. Their traversal times are 0.24, 1.18, 1.94, 1.98, and 2.95 minutes, which sum to 8.28 minutes. The two grid-to-road access links add another 0.73 minute, so the physical link time is 9.01 minutes.

I then apply observed and free-flow segment speeds to this same selected path. Because length is in kilometers and speed is in kilometers per hour, multiplying length divided by speed by 60 converts hours into minutes. The model applies $t_{kl}=m_{kl}^{1/\theta}$ before routes are aggregated into the bilateral cost index. Reversing the grid-pair order can select a different directed path, so the two link times need not coincide.

The small details link opens Appendix Slide 27. That slide records the access radii, connector limit, AM-time path weight, detour gate, lane proxy, and two selected-route examples.

[Click to Invert, Anchor, and Solve.]

## Slide 17. Estimation: Invert, Anchor, and Solve

The implementation has three steps.

First, I invert residential amenities and workplace productivities so that the observed residence and workplace marginals form an equilibrium under observed directed link costs.

Second, I calculate Xi zero, the model-implied directed link load at that baseline allocation. It is not an observed vehicle count. The congestion equation is anchored to observed directed link cost. Counterfactual $t_{kl}$ moves with the change in model load relative to Xi zero and with the change in lanes relative to baseline. The direct link-cost elasticity is $\lambda/\theta$, and $t_{kl}^{ff}$ provides the lower bound.

Because the update depends on ratios, the arbitrary level of model-implied commuter load cancels. Each scenario then recomputes link costs, bilateral costs, route traffic, residence and workplace shares, market access, and expected utility with the recovered fundamentals held fixed.

[Click to Baseline Fundamentals.]

## Slide 18. Baseline Fundamentals

These maps contain inverted objects, not directly observed amenities or productivity. The inversion matches baseline residence and workplace marginals at observed directed times. Workplace productivity is more strongly correlated with employment than residential amenity is with residents. The treated corridors also coincide with high-flow employment bottlenecks, so the policy does not select isolated low-demand links.

[Click to What Symmetry Misses.]

## Slide 19. What Symmetry Misses

The revised exercise estimates two separate measurement systems. The directional world retains directed observed and free-flow physical minutes. The symmetric world replaces each reciprocal pair by its arithmetic mean, then re-inverts fundamentals and resolves equilibrium.

The left table isolates directional exposure. The slower direction carries 56.3 percent of baseline model-implied passages, compared with 43.7 percent in the faster direction. Equalizing reciprocal times lowers passage-weighted physical time by 5.0 percent with passages held fixed and by 5.5 percent after routes, OD flows, fundamentals, and equilibrium are re-estimated, even though the unweighted mean link time is unchanged.

The right figure reports the symmetric-minus-directed log revision in recovered fundamentals. Symmetry understates central residential fundamentals by understating peak travel times from the periphery. Residence and workplace fundamentals are jointly re-inverted, so this profile is a spatial-attribution result rather than a one-equation causal decomposition. These are differences between two measurement systems, not changes caused by a transport policy.

[Click to Where the Tidal Policy Lands.]

## Slide 20. Where the Tidal Policy Lands

For each reciprocal corridor, s denotes the slower morning direction and f the faster direction. The intervention gives one lane to s and removes one lane from f, preserving total width.

The Tidal Candidate Score compares flow times excess delay in the slow direction with the same object in the fast direction. A high score identifies a corridor where the baseline congestion-delay mass is concentrated on one side. The red links on the map are the lane-receiving directions among the top 100 reciprocal pairs. They concentrate on the central high-flow network rather than on isolated slow roads.

[Click to Short Run versus Long Run.]

## Slide 21. Short Run vs. Long Run

The short run fixes residence and workplace shares but allows routes, link traffic, link costs, and bilateral costs to adjust. The long run also allows residence, workplace, and OD shares to sort.

For each horizon, $s_{ij}$ is the OD commuter share and $\bar\tau$ is the OD-share-weighted bilateral cost index. The first term measures the baseline-to-short-run change in bilateral costs at baseline OD weights. The second retains those weights and adds the short-run-to-long-run cost feedback. The third captures the complete baseline-to-long-run OD reallocation, evaluated at long-run costs.

The three terms add exactly to $\bar\tau^{LT}-\bar\tau^0$. This is a decomposition of the model's bilateral cost index, not physical minutes. The next slide reports additive physical time as a separate outcome.

[Click to Results.]

## Slide 22. Short-Run Time Gains Are Reversed by Long-Run Reallocation

The single table reports both physical time and welfare across the two adjustment horizons. Across all commuters, the policy saves 1,031 hours in one morning peak during the short run. Long-run sorting and congestion feedback reverse that gain, raising total commuting time 352 hours above baseline.

Expected utility rises by 0.87 percent in the short run and remains 0.32 percent above baseline in the long run. Welfare also reflects productivity, amenities, wages, and market access, so its sign need not match the sign of total commuting time.

[Click to Long-Run Sorting.]

## Slide 23. Long-Run Sorting

The maps report reallocation, not citywide growth, because total residents and jobs are fixed. Residential shares rise near treated endpoints as market access changes. Employment gains are more spatially concentrated. The negative changes elsewhere partly reflect the adding-up constraint: gains in one set of cells require offsetting share losses in another set.

[Click to Policy Comparison.]

## Slide 24. Transportation Policy Comparison

Tidal reallocation raises long-run expected utility by 0.32 percent with zero net lane-kilometers. Targeted road expansion reaches 0.44 percent but adds about 1,140 lane-kilometers. At the slide's urban-arterial benchmark, that expansion is an order-six-billion-yuan investment. The comparison isolates the policy margin: reassigning existing directional capacity versus constructing additional capacity. The time column remains omitted because the expansion and tidal policies do not share a common treated-link set.

[Click to Conclusion.]

## Slide 25. Conclusion

Directional congestion is widespread. The more congested direction carries 56.3 percent of AM link passages, so symmetry understates experienced travel time by about 5 percent. It also understates peripheral peak travel time and, in turn, central residential fundamentals. Tidal reallocation delivers short-run time savings and positive long-run welfare with zero net lane-kilometers. Thank you.

## Appendix Q&A map

These slides do not enter the 25-minute clock.

### Slide 26. Fixed-Fundamentals Symmetry Diagnostic

Use this slide only to explain the 1.10 percent benchmark. Fundamentals are held fixed, so this is not the primary full re-estimation estimand.

### Slide 27. From Grid Pairs to Directed Road Paths

Use this slide to explain the pair-specific routing algorithm and its parameter choices. Each centroid has up to three access candidates within 2.5 kilometers. Dijkstra minimizes observed AM minutes on the directed road graph, the detour gate is five, and observed and free-flow minutes use the same selected road sequence.

### Slide 28. Route Costs and Bilateral Commuting Costs

Use this slide to explain how the directed adjacency matrix aggregates feasible paths into the bilateral cost index. Keep physical link minutes distinct from multiplicative link cost.

### Slide 29. Expected Physical Travel Time

Use this slide to distinguish additive route minutes from multiplicative iceberg route costs and to show the link-passage identity used for the headline time calculation.

### Slide 30. Treated-Link Time Incidence

Use this slide only for the old 1,276 and 714 person-hour figures. They are local engineering-incidence totals across treated links, not citywide experienced travel time.

### Slide 31. Estimation Algorithm

Use this slide to clarify the nested solver: invert fundamentals at the observed allocation, construct the baseline Xi anchor, update directed link costs, and iterate location shares and market access until convergence.

### Slide 32. Jobs--Housing Dissimilarity Benchmarks

Use this slide for the construction and interpretation of the Duncan index. It measures spatial separation and is not a causal estimand.

### Slide 33. Model-Implied Directional Asymmetry

Use this slide to distinguish equilibrium link load Xi from observed vehicle flow. The figure is a model allocation over directed links.

### Slide 34. Parameter Interpretation

Use this slide for the economic roles and sources of theta, alpha, beta, and the congestion elasticity.

### Slides 35--37. Sensitivity

Use these slides to establish whether the welfare sign survives alternative alpha, beta, and theta calibrations. The treated-link person-hour magnitude is reported only for the retained primary specification. Compare counterfactual changes, not welfare levels across theta values.

### Slide 38. Policy Definitions and Selected Effects

Use this slide to distinguish tidal reallocation, targeted expansion, uniform expansion, and the pricing benchmarks.

### Slides 39--40. Targeting Tidal Lanes

Use these slides to explain why flow and market access matter for targeting. Speed-only selection can emphasize slow but low-flow links.

### Slide 41. Next Steps

Use this slide if asked about validation, parameter estimation, additional cities, peak windows, or mode switching.

### Slide 42. Field Visit

Use this slide to discuss the Shenzhen measurement plan and the proposed local speed--flow design around an operational lane-direction switch.

### Slide 43. Existence and Uniqueness

Use this slide for the Allen--Arkolakis sufficient conditions. Do not derive them during the main presentation.
