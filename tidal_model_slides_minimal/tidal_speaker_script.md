# Directional Congestion: 25-Minute Speaker Script

Deck: `Tidal0715.pdf`  
Venue: SMUE Wuhan, 2026  
Main presentation: Slides 1--28  
Appendix: Slides 29--43, for questions only

## Timing plan


| Slide | Title                                                               | Target | Cumulative |
| ----- | ------------------------------------------------------------------- | ------ | ---------- |
| 1     | Directional Congestion                                              | 0:20   | 0:20       |
| 2     | Roadmap                                                             | 0:25   | 0:45       |
| 3     | Directional Congestion in Cities                                    | 1:15   | 2:00       |
| 4     | Motivation                                                          | 1:45   | 3:45       |
| 5     | This Paper                                                          | 1:35   | 5:20       |
| 6     | Literature                                                          | 0:50   | 6:10       |
| 7     | Data                                                                | 0:30   | 6:40       |
| 8     | National Trip-Level Directionality                                  | 1:15   | 7:55       |
| 9     | Road Segment Level Directionality                                   | 1:05   | 9:00       |
| 10    | Directionality Arises from Jobs--Housing Mismatch                   | 2:00   | 11:00      |
| 11    | Model: Environment                                                  | 0:35   | 11:35      |
| 12    | Individual Location Choice                                          | 0:40   | 12:15      |
| 13    | Bilateral Commuting Costs and Flows                                 | 0:45   | 13:00      |
| 14    | Amenity, Productivity, and Location Choices                         | 0:40   | 13:40      |
| 15    | Feedback between Location Choice, Route Traffic, and Traversal Cost | 1:10   | 14:50      |
| 16    | Equilibrium                                                         | 0:40   | 15:30      |
| 17    | Estimation                                                          | 1:05   | 16:35      |
| 18    | From Segment Speeds to Directed Link Costs                          | 1:00   | 17:35      |
| 19    | Estimation: Invert, Anchor, and Solve                               | 0:55   | 18:30      |
| 20    | Baseline Fundamentals                                               | 0:35   | 19:05      |
| 21    | What Symmetry Misses                                                | 0:25   | 19:30      |
| 22    | Tidal Lanes                                                         | 0:45   | 20:15      |
| 23    | Decomposition of Equilibrium Welfare                                | 0:55   | 21:10      |
| 24    | Welfare                                                             | 1:50   | 23:00      |
| 25    | Residential and Workplace Sorting                                   | 0:40   | 23:40      |
| 26    | Transportation Policy Comparison                                    | 0:40   | 24:20      |
| 27    | Conclusion                                                          | 0:30   | 24:30      |
| 28    | Next Steps                                                          | 0:30   | 25:00      |


Section targets are 6:40 through the data overview, 4:20 for the directional facts, 4:30 for the model, 3:35 for estimation, and 5:55 for counterfactuals, conclusion, and next steps.

## Before presenting

- Do **not** say short run / long run or ST / LT. The headline object is the **sorting equilibrium**. Holding locations fixed is a **decomposition device**, not a model equilibrium.
- Preferred labels: **Holding locations fixed**, **Full equilibrium**, **Direct effect**, **Congestion GE / congestion feedback**, **Sorting**.
- Slide 21 compares two fully re-estimated measurement systems. It is not a policy counterfactual. Keep the earlier fixed-fundamentals diagnostic out of the talk and appendix.
- Keep three objects separate throughout the talk: physical minutes $m_{kl}$, link iceberg cost $t_{kl}=m_{kl}^{1/\theta}$, and the all-route bilateral cost index $\tau_{ij}$. Never describe $\tau_{ij}$ as minutes or convert it into seconds.
- Slide 23 decomposes the OD-weighted model cost index built from $t_{kl}$ and $\tau_{ij}$. Slide 24 reports citywide expected additive physical time from equilibrium link passages and link minutes. Appendix Slide 32 reports a narrower treated-link incidence calculation.
- The model section should sound like one coherent QSM, not a list of contrasts. Mark the differences only where they enter: directed links on Slide 11, route choice on Slide 12, endogenous link congestion on Slide 15, and the empirical anchor on Slides 17--19.

## Model narration guide

The model story has four steps.

1. **Keep the familiar QSM core.** People choose where to live and work. Amenities, productivity, wages, and market access determine location shares.
2. **Put routes inside commuting choice.** A standard QSM usually starts from an OD cost $\tau_{ij}$. Here an individual chooses an OD pair and a route, and $\tau_{ij}$ is generated by all feasible routes.
3. **Generate congestion inside the model.** OD-route choices imply expected passage counts on every directed link. Link traffic per lane changes link costs, which then feed back into route, OD, residence, and workplace choice.
4. **Separate economic cost from physical time.** Minutes add along a route. Iceberg link costs multiply along a route. The bilateral index aggregates all routes. Policy time effects are calculated from additive link minutes, not from $\tau_{ij}$.

Use the following translation when a time object first appears:


| Object                                              | Meaning                                            | Aggregation             |
| --------------------------------------------------- | -------------------------------------------------- | ----------------------- |
| $m_{kl}$                                            | Physical time on directed link $(k,l)$, in minutes | Add across links        |
| $t_{kl}=m_{kl}^{1/\theta}$                          | Link-level iceberg cost                            | Multiply across links   |
| $C_{ij,r}=\prod_{(k,l)\in r}t_{kl}$                 | Cost of route $r$                                  | One route               |
| $\tau_{ij}=[\sum_r C_{ij,r}^{-\theta}]^{-1/\theta}$ | All-route bilateral cost index                     | Aggregate across routes |
| $M_{ij,r}=\sum_{(k,l)\in r}m_{kl}$                  | Physical minutes on route $r$                      | Additive time outcome   |


## Slide 1. Directional Congestion

Good morning. During the peak, the same road can be congested in one direction and relatively clear in the other. I ask how this directionality enters spatial equilibrium, and whether tidal lanes can exploit it without adding road width.

[Click to Roadmap.]

## Slide 2. Roadmap

The talk has six parts: Introduction, Measurement, Model, Estimation, Counterfactuals, and Next Steps. I begin with the question and contributions, then measure directional congestion and its jobs--housing source. I next introduce a route-inclusive quantitative spatial model, explain how the directed network is anchored for estimation, and turn to the equilibrium effects of tidal-lane reallocation, decomposed into a fixed-location direct effect and sorting feedback. I close with next steps.

[Click to the city photographs.]

## Slide 3. Directional Congestion in Cities

These photographs come from Beijing, Wuhan, Shenzhen, and Kuala Lumpur. They are not intended as a comparable dataset. They make the object visible before I define it statistically.

In each case, the relevant contrast is within the same physical corridor and at the same time. One direction carries a dense queue, while the reciprocal direction has spare capacity. A conventional congestion measure records that the road is congested on average. A directional measure records that the two sides of the road are in different economic states.

That distinction matters for two reasons. Commuters face the cost from home to work, not an average of both directions. The feasible policy also operates by direction: a tidal lane transfers capacity from the less congested side to the more congested side during a peak window.

[Click to Motivation.]

## Slide 4. Motivation

Directional congestion is tied to urban spatial structure. Jobs concentrate in employment centers, while residences occupy a broader area. During the morning peak, this separation creates a center-bound demand imbalance. The reverse direction can retain spare capacity even when the inbound direction is congested.

[Optional video cue: play no more than 20 seconds.]

The details button beside the video opens the descriptive comparison and the proposed RDiT design in Slides 42--43.

The video provides the policy intuition. A tidal lane changes which direction can deploy a lane at a particular time. Total road width is fixed. The intervention is therefore different from an ordinary capacity expansion.

Most quantitative spatial models represent commuting frictions through bilateral or route-level costs. That is appropriate for many questions, but reciprocal link asymmetry is often averaged out before the equilibrium is solved. Transportation research studies peak-direction bottlenecks and reversible lanes, yet those engineering objects rarely enter a spatial-equilibrium counterfactual with endogenous congestion and sorting.

The open question is therefore not only whether the slow direction becomes faster. We need to know how route traffic responds, whether congestion moves elsewhere, and how residence and workplace choices adjust once people can re-sort.

[Click to This Paper.]

## Slide 5. This Paper

The paper makes three contributions.

First, I measure directionality at two spatial scales. At the national trip level, the flow-weighted median speed asymmetry ratio is 0.878 across the top 100 congested cities. Within Beijing, 179 two-way centerlines satisfy a stringent one-third ratio threshold during the morning peak, covering about 213 kilometers.

Second, I quantify what reciprocal symmetry misses. The more congested direction carries 56.3 percent of AM link passages, so symmetry understates experienced travel time by about 5 percent. It also understates peak travel time from outer residential areas and therefore central residential fundamentals.

Third, I evaluate a top-100 tidal policy under endogenous congestion. One lane moves toward the slow morning direction on each treated reciprocal corridor. Holding locations fixed, total AM-peak time falls by 1,031 hours and welfare rises by 0.87 percent. In the full sorting equilibrium, the time gain reverses to 352 hours above baseline, while welfare remains positive at 0.32 percent. A targeted expansion reaches a somewhat greater equilibrium gain, but requires about 1,140 additional lane-kilometers.

[Click to Literature.]

## Slide 6. Literature

The paper connects three literatures.

The first studies directional congestion through bottlenecks, high-frequency urban speeds, and reversible-lane operations. I contribute measurement that runs from national trips to individual directed road segments.

The second is quantitative spatial equilibrium. This literature links commuting costs to residence, employment, productivity, amenities, and market access. I retain that structure—especially the network-and-congestion framework of Treb Allen and Costas Arkolakis—but allow link congestion and capacity to differ by direction.

The third evaluates transportation policy through pricing, road expansion, optimal networks, and public transit. Here I want to show particular respect to Gilles Duranton. Together with Matthew Turner, his work on the fundamental law of road congestion established how strongly travel demand responds to added road capacity, and it remains the benchmark for thinking about supply-side interventions. Related work with Victor Couture and others has also shaped how we measure urban speed and congestion. Our exercise is complementary, not a substitute for that agenda: instead of asking how much congestion returns when we build more lane-kilometers, we ask what can be gained by reallocating a fixed amount of road width across directions. Tidal lanes therefore occupy a distinct supply margin that has no counterpart in a symmetric network representation.

[Click to Data.]

## Slide 7. Data

Start with the segment-level speed panel. Baidu records directed road-segment speeds at ten-minute frequency for one week in May 2025. I use the weekday AM peak window and a night free-flow window.

The national trip-level layer comes from Baidu Maps driving-route queries for township commuting pairs. Sample construction is on the next slide.

Baidu 2022 supplies home-to-work commuting flows on 100-meter grids. Road geometry, functional class, and lane records define directed capacity. The map previews the morning speed imbalance recovered within Beijing.

[Click to National Trip-Level Directionality.]

## Slide 8. National Trip-Level Directionality

The asymmetry ratio is the lower directional speed divided by the higher directional speed on a reciprocal pair. One means equal speeds; lower values mean stronger directional imbalance.

The sample is the top 100 congested cities. Within each city I keep the top 50 township pairs by two-way commuting flow, and I query Baidu driving routes at the AM peak in both directions.

The histogram shows the pooled distribution. The flow-weighted median asymmetry ratio is 0.878, and 58 percent of flow faces an asymmetry ratio at or below 0.90. Directionality is therefore not confined to a few extreme observations.

The scatter asks whether the ratio varies with congestion severity. More congested cities tend to have lower ratios. I treat this as a descriptive city-level association, not a causal design.

Trip-level evidence establishes prevalence across cities. It does not yet identify which road could receive a tidal lane. That is the next slide.

[Click to Road Segment Level Directionality.]

## Slide 9. Road Segment Level Directionality

I use the same speed asymmetry ratio, now on directed Beijing centerlines during the weekday AM peak.

Among 3,670 two-way centerlines, the median ratio is 0.884, and 54.7 percent have ratios at or below 0.90. The map shows where the imbalance is spatially concentrated. The stricter one-third threshold flags 179 candidate corridors, about 4.9 percent of the sample and roughly 213 kilometers.

National trip routes establish prevalence. Directed centerlines identify the policy-relevant road margin. The next slide explains why peak commuting demand has a systematic direction in the first place.

[Click to Jobs--Housing Mismatch.]

## Slide 10. Directionality Arises from Jobs--Housing Mismatch

The left column asks whether center-bound commuting is a general feature of major cities. Within each city, I rank towns by workplace-side commuter mass. Core towns cover the first 30 percent of that mass, and the remaining towns form the periphery.

Each vertical stem in the lower-left figure is one of the top 20 cities by inter-town commuting flow. Stem height is the center-bound commuting share minus the reverse share. All 20 gaps are positive, and the flow-weighted gap is 4.5 percentage points.

The right column gives the Beijing mechanism in greater spatial detail. The residential-density map is dispersed across the city. Job density is more concentrated. The Duncan dissimilarity index is 0.26, compared with 0.004 under random allocation. This is a descriptive measure of jobs--housing separation, not a causal estimate.

In Beijing, peripheral-to-core commuting exceeds the reverse by 9.4 percentage points. Beijing therefore illustrates both the spatial separation and the corresponding commuting imbalance.

These facts motivate a model in which residence, workplace, route, and directed congestion are determined jointly.

[Click to Model: Environment.]

## Slide 11. Model: Environment

I now move to the model. Most of the location block is standard QSM. Locations host residents and jobs, and workers choose where to live and work.

The first change is the network. Each physical road is represented by two directed links. The two directions can have different free-flow costs, realized costs, traffic, and lane capacity. A tidal policy changes the lane split while holding total corridor width fixed.

At this point, $t_{kl}$ is an economic link cost. I will return to its relation with physical minutes when I discuss the data.

[Click to Individual Location Choice.]

## Slide 12. Individual Location Choice

An individual jointly chooses a residence, a workplace, and a route. Utility combines residential amenity, workplace wage, route cost, and an idiosyncratic Fréchet term.

This is where the Allen--Arkolakis setting extends the usual OD choice. In a standard QSM, the worker chooses $i$ and $j$ and takes a bilateral cost as given. Here the choice is $(i,j,r)$. The route belongs inside the choice object because a change on one road segment can redirect traffic even when the home and workplace do not change.

The directed iceberg costs $t_{kl}$ multiply along route $r$ to form $C_{ij,r}$. The multiplication comes from iceberg-cost accounting. It does not mean that physical minutes multiply.

[Click to Bilateral Commuting Costs.]

## Slide 13. Bilateral Commuting Costs and Flows

For each residence--workplace pair, the first equation aggregates all feasible routes into the bilateral cost index $\tau_{ij}$. Summing route-specific choices then delivers the OD flow $L_{ij}$ and the familiar gravity equation.

This is useful because adding route choice does not discard the standard QSM structure. After routes are aggregated, we still have bilateral gravity with origin and destination market-access terms. The difference is that $\tau_{ij}$ is produced by the directed road network and all feasible routes. It is not imported as an exogenous OD travel-time matrix.

Directed links imply that $\tau_{ij}$ generally differs from $\tau_{ji}$. Most important, $\tau_{ij}$ is a cost index used in choice and welfare equations. It is not OD driving time in minutes.

[Click to Amenities and Productivity.]

## Slide 14. Amenity, Productivity, and Location Choices

This slide returns to the familiar QSM location block. Local productivity combines a workplace fundamental with employment density through $\alpha$. Residential amenity combines its fundamental with residential density through $\beta$. Positive $\alpha$ captures workplace agglomeration, and negative $\beta$ captures residential crowding.

Given bilateral commuting costs, these equations determine residence and workplace shares. This is the sorting margin. When I later decompose equilibrium welfare, the fixed-location step holds these shares fixed; the full equilibrium re-solves them.

[Click to the congestion feedback.]

## Slide 15. Feedback between Location Choice, Route Traffic, and Traversal Cost

This slide contains the second main extension: congestion is endogenous at the directed-link level.

For one commuter from $i$ to $j$, $\kappa_{ij}^{kl}$ is the expected use of directed link $(k,l)$ under the commuter's route distribution. On a simple path, it is just the probability that the selected route uses that link. Multiplying this expected use by $L_{ij}$ and summing over all OD pairs gives $\Xi_{kl}$, the model-implied commuter load on that directed link.

Traffic per lane then raises $t_{kl}$. A lane change affects capacity directly. Route substitution changes $\Xi_{kl}$, and location choice changes it again. The new link cost feeds back into route choice, bilateral costs, OD flows, and location shares. Congestion is therefore a fixed point, not a one-time speed adjustment.

$\lambda$ is the elasticity of physical link time with respect to traffic per lane. Because $t_{kl}=m_{kl}^{1/\theta}$, the elasticity in the link-cost equation is $\lambda/\theta$.

The welfare expression is the expected-utility index over the complete residence--workplace--route choice set. Welfare can therefore remain positive even if one physical-time statistic later reverses sign.

[Click to Equilibrium.]

## Slide 16. Equilibrium

An equilibrium contains residence and workplace populations, OD flows, bilateral costs, directed link loads, directed link costs, and market-access terms. Choice, gravity, amenities, productivity, and congestion must all hold at the same time.

The Allen--Arkolakis core provides sufficient theoretical conditions for its network-location system. Our complete empirical system also includes observed-cost anchors, a free-flow lower bound, and directed endogenous congestion. We solve that full system as a damped numerical fixed point and verify convergence and adding-up conditions. We do not claim a separate uniqueness theorem for the complete anchored system.

[Click to Estimation.]

## Slide 17. Estimation

The upper table maps each model object to its data source. Baidu 2022 supplies residence, workplace, and bilateral commuting flows. The observed link-cost measure is constructed from weekday AM speeds between seven and nine. The free-flow measure is constructed from speeds between ten at night and six in the morning across all days. Road geometry and lanes determine link length and capacity. The three-kilometer grid defines the directed adjacent-grid network.

The lower table reports four calibrated parameters. $\theta=6.83$ is the cost elasticity. $\alpha=0.05$ governs production agglomeration, and $\beta=-0.10$ governs residential crowding. $\lambda=1.1$ is the physical travel-time elasticity with respect to traffic per lane.

[Click to the link-cost construction.]

## Slide 18. From Segment Speeds to Directed Link Costs

The left panel illustrates how raw road data become one directed grid link. The right panel reports the resulting observed physical minutes across the complete directed adjacent-grid network.

For each ordered pair of adjacent grid cells, I select the path that minimizes observed morning minutes on the directed raw-road network. The displayed example crosses two neighboring grids. Partial host-road segments remain part of the raw-road path, while the two off-road connectors enter separately.

For every raw segment, length divided by speed is measured in hours because length is in kilometers and speed is in kilometers per hour. Multiplying by 60 converts that value into minutes. I use the observed and free-flow speeds on the same selected path to construct $m_{kl}^{obs}$ and $m_{kl}^{ff}$.

Now the distinction among time objects becomes important. Physical segment minutes add along the path. The model then maps link minutes into iceberg cost using $t_{kl}=m_{kl}^{1/\theta}$. Iceberg link costs multiply along a route because proportional trade or travel costs compound. All route costs are then aggregated into $\tau_{ij}$. Thus the sum-to-product change comes from the transformation from physical time to iceberg cost, not from a change in how clocks work.

Reversing the grid-pair order can select a different directed path, so the two link times need not coincide. The details link opens Appendix Slide 29, which records the routing parameters and a connector-assisted example.

[Click to Invert, Anchor, and Solve.]

## Slide 19. Estimation: Invert, Anchor, and Solve

The implementation has three steps.

First, I invert residential amenities and workplace productivities so that the model matches the observed residence and workplace marginals under the observed directed network. Again, I match the marginals, not the full OD matrix.

The details link opens Appendix Slide 33 (estimation algorithm). From there, a second details link opens Appendix Slide 34 for bilateral OD fit after absorbing origin and destination fixed effects; Slide 34 returns to 33, and 33 returns to the main Invert–Anchor–Solve slide.

Second, I compute $\Xi_{kl}^{0}$, the model-implied baseline commuter load on each directed link. This is not observed traffic flow. It is the load implied by model OD flows and route probabilities.

Third, I replace baseline lanes with the tidal-lane allocation and initialize physical minutes at $m^{obs}$. Each iteration maps minutes into link cost through $t=m^{1/\theta}$, recomputes model link traffic, and updates physical minutes with congestion elasticity $\lambda=1.1$. The solver damps the minute update in logs and repeats until the traffic and time vectors are mutually consistent.

$\Xi^0$ is not an observed vehicle count. The congestion mapping assumes that proportional changes in model link traffic track proportional changes in congestion-relevant traffic. Equivalently, the baseline traffic index satisfies $q^0=n^0(m^{obs}/m^{ff})^{1/\lambda}$ and $\widehat q=\widehat\Xi$; $q^0$ cancels from the implemented ratio equation. The fixed-location step pins residence and workplace shares; full equilibrium re-solves them each iteration.

[Click to Baseline Fundamentals.]

## Slide 20. Baseline Fundamentals

These maps contain inverted fundamentals, not directly observed amenities or productivity. The inversion recovers the objects needed to rationalize the baseline residence and workplace marginals under the observed directed network.

Employment is largely explained by productivity fundamentals---the correlation is 0.67---while residential amenity correlates with residents only at 0.30, so residence is more access-driven. The treated corridors also overlap high-flow employment bottlenecks, so the policy is not selecting isolated slow links with little commuter exposure.

[Click to What Symmetry Misses.]

## Slide 21. What Symmetry Misses

The revised exercise estimates two separate measurement systems. The directional world retains directed observed and free-flow physical minutes. The symmetric world replaces each reciprocal pair by its arithmetic mean, then re-inverts fundamentals and resolves equilibrium.

The left table isolates directional exposure. The slower direction carries 56.3 percent of baseline model-implied passages, compared with 43.7 percent in the faster direction. Equalizing reciprocal times lowers passage-weighted physical time by 5.0 percent with passages held fixed and by 5.5 percent after routes, OD flows, fundamentals, and equilibrium are re-estimated, even though the unweighted mean link time is unchanged.

The right figure reports the symmetric-minus-directed log revision in recovered fundamentals. Symmetry understates peak travel time from the periphery, so inverted central residential amenities come out too low. Residence and workplace fundamentals are jointly re-inverted, so this profile is a spatial-attribution result rather than a one-equation causal decomposition. These are differences between two measurement systems, not changes caused by a transport policy.

[Click to Tidal Lanes.]

## Slide 22. Tidal Lanes

First define the policy. For each reciprocal corridor, $s$ is the slower morning direction and $f$ the faster direction. The intervention gives one lane to $s$ and removes one lane from $f$, preserving total width. This is capacity reallocation, not capacity expansion.

The figures show the lane transfer and the treated map. Red edges are the lane-receiving directions among the top 100 pairs. They concentrate on the central high-flow network rather than on isolated slow roads.

Only after defining the policy do I turn to targeting. The Tidal Candidate Score compares flow times excess delay in the slow direction with the same object in the fast direction. A high score identifies a corridor where the baseline congestion-delay mass is concentrated on one side.

[Click to Decomposition of Equilibrium Welfare.]

## Slide 23. Decomposition of Equilibrium Welfare

The headline object is the sorting equilibrium. Holding locations fixed is a decomposition device: residence and employment are pinned by assumption, while route traffic, edge times, and bilateral costs re-equilibrate. Full equilibrium then allows residence, workplace, and OD shares to sort with congestion feedback.

Why does the direct gain get eroded? Lower private travel costs induce re-sorting, but individuals do not internalize the congestion externality their choices impose on others. That feedback shows up in the next terms.

The decomposition is written for the OD-weighted bilateral cost index. The first term is the **direct effect**: baseline OD weights, baseline to fixed-location costs. The second is **congestion GE**: same baseline weights, fixed-location to full-equilibrium congestion. The third is **sorting**: OD weights move from baseline to equilibrium, evaluated at equilibrium costs. These three terms add exactly to $\bar\tau^{\mathrm{eq}}-\bar\tau^{0}$.

This page concerns model commuting cost. It does not interpret $\tau_{ij}$ as physical minutes; the next slide reports additive physical time separately.

[Click to Results.]

## Slide 24. Welfare

Panel A decomposes total physical time. Holding locations fixed saves 1,031 hours in one morning peak. Congestion feedback through link times offsets 650 hours. Link-passage reallocation offsets another 733 hours. Full equilibrium leaves total physical commuting time 352 hours above baseline.

Panel B follows the same three-term decomposition as the preceding slide. The direct effect raises welfare by 0.8714 percent. Congestion GE subtracts 0.5470 percentage point. Sorting combines the workplace-productivity and residential-crowding channels and subtracts 0.0001 percentage point. The three terms add exactly to the equilibrium welfare gain of 0.3243 percent.

The two panels answer different questions. Physical time counts additive link minutes. Welfare also reflects the equilibrium residence-workplace opportunity set, productivity, and residential amenities. Its sign need not match the sign of physical time.

[Click to Residential and Workplace Sorting.]

## Slide 25. Residential and Workplace Sorting

The maps report reallocation, not citywide growth, because total residents and jobs are fixed. Residential shares rise near treated endpoints as market access changes. Employment gains are more spatially concentrated. The negative changes elsewhere partly reflect the adding-up constraint: gains in one set of cells require offsetting share losses in another set.

[Click to Policy Comparison.]

## Slide 26. Transportation Policy Comparison

Direct $\bar W$ holds locations fixed; equilibrium $\bar W$ is the sorting equilibrium. Tidal reallocation raises equilibrium expected utility by 0.32 percent with zero net lane-kilometers. Targeted road expansion reaches 0.44 percent but adds about 1,140 lane-kilometers. At the slide's urban-arterial benchmark, that expansion is an order-six-billion-yuan investment. The comparison isolates the policy margin: reassigning existing directional capacity versus constructing additional capacity.

[Click to Conclusion.]

## Slide 27. Conclusion

Directional congestion is widespread. The more congested direction carries 56.3 percent of AM link passages, so symmetry understates experienced travel time by about 5 percent. It also understates peripheral peak travel time and, in turn, central residential fundamentals. Direct effect: time falls by 1,031 hours and welfare rises by 0.87 percent. Full equilibrium: time rises by 352 hours, while welfare remains at plus 0.32 percent. Expansion needs about 1,140 lane-kilometers for comparable equilibrium welfare; tidal uses zero.

[Click to Next Steps.]

## Slide 28. Next Steps

Three extensions. First, reduced-form evidence from on-site video at deployed tidal segments: recover the local flow--speed relation and use the lane switch as an RDD. The video bullet links to the 0709 evidence page; the RDD bullet links to the Opening-Time RDiT design. Second, identify or calibrate $\alpha$, $\beta$, $\theta$, and $\lambda$ more tightly from data. Third, extend the full pipeline to Beijing and Shenzhen, AM and PM, and alternative grids. Thank you.

## Appendix Q&A map

These slides do not enter the 25-minute clock.

### Slide 29. Selecting the Shortest Directed Road Path

Each centroid is connected to at most three nearby directed road edges within 2.5 kilometers. For every ordered adjacent grid pair, all origin-edge and destination-edge combinations are compared. Dijkstra finds the path with the lowest observed AM road minutes for each combination, after adding access time at both endpoints. The candidate with the lowest total minutes is retained. The reverse direction is solved independently because the road graph and edge weights are directed.

### Slide 30. Route Costs and Bilateral Commuting Costs

Use this slide to explain how the directed adjacency matrix aggregates feasible paths into the bilateral cost index. Keep physical link minutes distinct from multiplicative link cost. The current mainline has common route rescaling equal to one, so the displayed identity $A_{kl}=1/m_{kl}=t_{kl}^{-\theta}$ matches the code exactly.

### Slide 31. Expected Physical Travel Time

Use this slide to distinguish additive route minutes from multiplicative iceberg route costs and to show the link-passage identity used for the headline time calculation.

### Slide 32. Treated-Link Time Incidence

Use this slide to distinguish treated-link incidence from the citywide expected-time total. The treated-link statistic counts the time change on treated directions for commuters who traverse those links; it is not the headline citywide measure. Table columns are Fixed loc. versus Equil.

### Slide 33. Estimation Algorithm

This slide clarifies the two stages. The baseline inversion matches residence and workplace marginals and constructs the model-implied $\Xi^0$ anchor. The counterfactual initializes $m$ at observed minutes, computes $t=m^{1/\theta}$ and model link traffic, updates $m$ with exponent $\lambda=1.1$, and applies log damping with $\omega=0.25$. Fixed-location pins $(L^R,L^F)$; full equilibrium re-solves them. The final line records the maintained mapping $\widehat q=\widehat\Xi$ and explains why the code does not store the implied baseline $q^0$ separately.

### Slide 34. Bilateral OD Fit beyond Marginals

The fundamentals inversion matches residence and workplace marginals, not the bilateral OD matrix. Observed and predicted log flows are projected separately on origin and destination fixed effects, and the correlation is computed across pairs with both residuals. The residual correlation is 0.413 across 83,030 positive observed pairs. It captures pair-specific commuting variation beyond origin and destination scale, not one-for-one calibration of flow residuals.

### Slide 35. Jobs--Housing Dissimilarity Benchmarks

The left panel places Beijing in the distribution of township-level Duncan indices across the top 100 cities. The right panel relates the index to city size. Beijing remains the highest observed value in this sample.

### Slide 36. Parameter Interpretation

Use this slide for the economic role and direct margin of $\theta$, $\alpha$, $\beta$, and $\lambda$. A higher $\lambda$ means that travel time reacts more strongly when traffic per lane changes. All four main values are calibrated externally.

### Slides 37--38. Sensitivity

Use these slides to discuss alternative $\alpha$, $\beta$, and $\theta$ values. Equilibrium welfare remains positive in every converged $\alpha$--$\beta$ cell and across the reported $\theta$ grid. Each $\theta$ value requires a new inversion and a new equilibrium solve, so compare counterfactual changes rather than baseline welfare levels across rows. Table headers use Direct $\bar W$ and Equil. $\bar W$.

### Slide 39. Policy Definitions and Selected Effects

Use this slide to define tidal reallocation, targeted expansion, and uniform expansion on a common table. Direct holds residence and workplace shares at baseline; full equilibrium re-solves sorting. Record the lane-kilometer margin.

### Slides 40--41. Targeting Tidal Lanes

Use these slides to explain why flow and market access matter for targeting. The welfare-elasticity score is a local ranking device. The reported welfare effects still come from fully solving the endogenous sorting equilibrium.

### Slide 42. Video Evidence Before and After Opening

Use this slide to report the 0709 video pilot. Flow rises in both directions, while median matched travel time falls only in the capacity-gaining direction. The comparison uses one day and two separated windows, so it is descriptive rather than a causal estimate. The design link opens the repeated-date RDiT specification.

### Slide 43. Opening-Time RDiT Design

Use this slide to define the proposed causal design. Scheduled opening time supplies the cutoff, actual lane operation is coded from video, and outcomes are measured in five-minute bins. Identification requires a predetermined cutoff and smooth potential outcomes absent opening. The bandwidth $h$ retains observations within $h$ minutes on either side of the cutoff. If actual operation differs from the schedule, the cutoff indicator instruments actual opening in a fuzzy RDiT. Formal estimation requires repeated dates and date-clustered inference; robustness checks vary the bin width and $h$.