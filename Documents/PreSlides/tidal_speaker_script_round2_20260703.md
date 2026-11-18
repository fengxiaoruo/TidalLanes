# Directional Congestion, Round 2 Speaker Script

Source deck: `tidal_advisor_revision_round2_20260703.pdf`.

## Opening Story

Goal: in about 60–90 seconds, make the audience *see* one object before any model — the same physical road, at the same moment, in two different economic states — then land the gap and the policy. Three beats.

**Beat 1 — the lived fact (hook).** Open with something everyone in the room has felt:

> "Every one of us has sat in a morning jam heading *into* downtown while the *other* side of the same road runs free — and by evening it flips. Congestion is not just a level. It has a **direction**."

Say it plainly and let it sit. The point is that direction changes the home-to-work commuting cost relative to the reverse trip, so it changes the access matrix that enters any spatial equilibrium model.

**Beat 2 — why it exists, and why it is missed.** Make the gap narrow and concrete:

> "That direction is not random. Jobs cluster in the core, housing spreads out, so the morning peak is radial-inward. Yet most spatial-GE models collapse a road into a single bilateral cost — the two directions get averaged away. Transport research does measure peak-direction speeds and studies reversible lanes, but those objects rarely enter a spatial-GE counterfactual with endogenous congestion and sorting."

Keep the wording conservative: spatial-GE work *abstracts from* segment-level directionality — it is a modeling choice, not a mistake.

**Beat 3 — the policy and the three contributions.** Close on the fix:

> "There is already a cheap answer on the street: a **tidal lane** — at peak, move one lane from the empty direction to the jammed one, with no new pavement. The paper does three things: **measure** how directional congestion is, from national trips down to individual road segments; **quantify what a symmetric model misses**; and **evaluate tidal lanes** in equilibrium, benchmarked against actually building roads."

Then go to the roadmap and plant the two exercises: "the model will run two exercises — what symmetry misses, and the tidal counterfactual."

> **Optional cold-open:** if you want the room hooked before you say a word, play the clickable field video on the Motivation slide first — the on-street lane reversal is the most direct picture of "zero-construction directional capacity," then deliver Beat 1 over it.

## Slide 1. Title

Today I study directional congestion and tidal-lane reallocation.

The core fact is simple. A road is not only congested or uncongested. During peak hours, the same physical road can be congested in one direction and much faster in the other.

That direction-specific cost matters because commuters choose residence, workplace, and route. It also matters because one policy, tidal lanes, reallocates capacity across directions without adding road width.

## Slide 2. Roadmap

The talk has five parts.

First, I motivate the measurement object. Second, I describe the directed speed and commuting-flow data. Third, I build the spatial equilibrium model. Fourth, I explain estimation. Fifth, I use the model for counterfactuals.

The logic is cumulative. The descriptive facts tell us where directionality appears. The model tells us how that directionality enters equilibrium. The counterfactual asks what a directed capacity policy can deliver.

## Slide 3. Motivation

Peak-direction imbalance is widely observed on urban roads. The reason is spatial. Workers and jobs are not located in the same places, so commuting demand has a strong direction during the peak.

Most spatial-GE applications summarize travel frictions at the bilateral or route level. That is often enough for many questions, but it abstracts from segment-level directionality.

The exception is the transport literature, which measures peak-period congestion and studies reversible-lane operations. This paper connects that measurement object to a spatial equilibrium counterfactual.

Tidal lanes are the policy reason this matters. They move capacity toward the slow direction at fixed road width, so their welfare effect cannot be evaluated with a symmetric road cost.

## Slide 4. This Paper

The paper does three things.

First, it measures directionality using trip-level and road-segment speed data. At the national trip level, the flow-weighted median speed asymmetry ratio is 0.878. At the Beijing segment level, the one-third benchmark flags 179 AM two-way centerlines, about 4.9 percent of the two-way sample and 213 km of road.

Second, it asks what symmetry misses. Holding the model, fundamentals, and parameters fixed, replacing directed costs with reciprocal symmetric costs changes measured welfare and reallocates residents and jobs.

Third, it evaluates tidal-lane counterfactuals under endogenous congestion and sorting. The top-100 policy shifts one lane toward the slow AM direction. Welfare rises in the short run and remains positive in the long run.

## Slide 5. Literature

The first literature is directional congestion. Bottleneck models, measured urban-speed papers, and reversible-lane studies give the relevant traffic object.

This paper adds a spatial-equilibrium layer: it measures directionality at the trip and segment levels and then evaluates a policy whose treatment is inherently directional.

The second literature is quantitative spatial equilibrium. This gives us the residence, workplace, and market-access structure.

The third literature is transportation policy and congestion. Congestion pricing, road capacity, public transit, and optimal networks are existing policy margins. Tidal lanes are a different margin: a directed capacity reallocation.

## Slide 6. Data

The empirical inputs have three parts.

Directed speed data measure trip-level directionality across cities and segment-level directionality within Beijing.

Commuting flows give the residence-workplace OD structure.

Road characteristics give the geometry and lane counts needed to map speed and capacity into a directed network.

## Slide 7. National Trip-Level Directionality

The speed asymmetry ratio is the smaller directed cost divided by the greater directed cost. A value of one means the two directions look the same. Lower values mean stronger directionality.

Across the top 100 congested cities in the Baidu 2025 ranking, the flow-weighted median is 0.878.

The right panel connects this object to congestion severity. Cities with higher commute-peak congestion tend to have lower asymmetry ratios.

This is a broad prevalence fact. It motivates the problem, but trip-level data do not tell us which road segment could receive a tidal lane.

## Slide 8. Commuters Face Directional Friction

The Shenzhen grid-level evidence asks whether real commuters are exposed to the asymmetric part of the cost surface.

Unweighted grid pairs already have substantial asymmetry. Once we weight by observed commuter flows, exposure is stronger. The flow-weighted median ratio is 0.819.

This is the bridge from a descriptive cost surface to commuter exposure.

## Slide 9. Road Segment Level Directionality

This is the policy-relevant measurement layer.

At the Beijing AM segment level, the median speed asymmetry ratio is 0.884. The broader 0.90 threshold says directionality is common.

For tidal-lane scope, the stricter one-third benchmark identifies 179 AM two-way centerlines, about 4.9 percent of the two-way sample and 213 km. This is not the main prevalence definition. It is an application-scope benchmark at the segment level.

## Slide 10. Jobs-Housing Mismatch

This slide explains why the peak has a direction.

Jobs cluster more than residences in the study grid. The Duncan index is 0.26, far above the random-allocation floor.

AM flows are also radial-inward. About 74.5 percent of core-versus-reverse trips head to the center, with an inbound-to-reverse ratio of 2.9.

Together, separation and direction create asymmetric peak load.

## Slide 11. Model Environment

The model starts with locations that host residents and jobs.

Each physical road is represented by two directed links. The object t_{kl}^{ff} is free-flow traversal time on link k,l, and n_{kl} is lane capacity in that direction.

The realized time t_{kl} is endogenous. It rises with traffic per lane. If traffic differs across the two directions, the two realized times differ as well.

## Slide 12. Individual Location Choice

A worker chooses residence, workplace, and route.

Utility combines residence amenity, workplace wage, route cost, and an idiosyncratic Frechet shock.

The route cost is the product of directed edge times along the route. This route-level formulation is what lets segment traffic feed back into commuting cost.

## Slide 13. Bilateral Commuting Costs and Flows

This slide is the bridge from route choice to OD commuting flows.

For a fixed origin and destination, the worker has many feasible routes. Under Frechet route shocks, those routes aggregate into an inclusive value. That is the first equation: \tau_{ij}^{-\theta} is the sum of route attractiveness terms C_{ij,r}^{-\theta}.

Once we impose residence and workplace marginals, OD flows take a doubly constrained gravity form. The terms \Pi_i and P_j are the balancing market-access terms. They make the row sums equal residence totals and the column sums equal workplace totals.

Because all route costs are built from directed edge times, \tau_{ij} need not equal \tau_{ji}.

## Slide 14. Amenity, Productivity, and Location Choices

Workplace productivity rises with local employment through \alpha. Residential amenity falls with local residents through negative \beta.

Equations (6) and (7) are the fixed-point conditions for residence and workplace shares. The symbol \chi is only a common normalization constant. It makes the shares sum to one.

The economic content is the feedback: commuting access affects where residents and jobs locate, and those locations then affect productivity and amenities.

## Slide 15. Route Cost vs. Commuting Cost

The matrix A puts directed edge costs into a network object. The entry a_{kl}=t_{kl}^{-\theta} is the attractiveness of crossing link k,l.

The matrix B=(I-A)^{-1} sums over paths of all lengths. It is the network multiplier.

Then B_{ij} gives the route-inclusive access from i to j, and \tau_{ij}=B_{ij}^{-1/\theta} converts it back into a bilateral commuting cost.

## Slide 16. Feedback and Welfare

For each OD pair, the model computes expected use of each directed link. Aggregating over OD flows gives directed link traffic \Xi_{kl}.

Traffic then raises edge time through the speed-flow relation.

The welfare object \bar W is the Frechet expected-utility scale. Frechet matters because the expected maximum over residence-workplace-route options collapses to an inclusive-value expression. Up to a constant, \bar W is the utility level from the full opportunity set.

## Slide 17. Equilibrium

An equilibrium consists of residence shares, workplace shares, OD flows, bilateral costs, edge times, link traffic, and market-access terms.

The system closes when choices, market clearing, agglomeration, the directed network, and congestion are mutually consistent.

The appendix gives the sufficient existence and uniqueness conditions.

## Slide 18. Estimation

The estimation slide maps data to model objects.

Observed AM edge times come from weekday AM speeds. Free-flow times come from a night window. Road geometry and lane records map the physical network into directed edges.

The parameters are currently calibrated from the literature. I will discuss future estimation in the talk rather than placing that sentence on the slide.

## Slide 19. Anchor and Solve

The first step is to invert fundamentals so the observed allocation is an equilibrium at observed directed travel times.

The second step anchors counterfactual congestion around observed edge times. Edge time moves with traffic per lane relative to baseline, but it cannot fall below free-flow time.

The third step is to re-solve the equilibrium for each scenario with fundamentals fixed.

## Slide 20. Short Run vs. Long Run

In the short run, residence and workplace shares are fixed. Routes, traffic, edge times, and bilateral costs adjust.

In the long run, residence and workplace shares also sort.

The decomposition separates the direct short-run change, the congestion feedback wedge, and the sorting term.

## Slide 21. Baseline Fundamentals

The maps report inverted residence amenities and workplace productivity.

Workplace productivity is more tightly correlated with employment than residence amenity is with residents.

The treated corridors land on high-flow employment bottlenecks, so the tidal policy is not selecting arbitrary roads.

## Slide 22. What Symmetry Misses

This diagnostic changes only the symmetry of reciprocal edge times. Network, lanes, fundamentals, parameters, solver, and welfare formula are held fixed.

Measured welfare changes by 1.10 percent, and the residence and workplace distributions move before any policy shock.

The interpretation is not that symmetry is always wrong by the same amount. The point is that symmetry can change the baseline allocation in a spatial model.

## Slide 23. Tidal-Lane Reallocation

For each reciprocal corridor, label the slower AM direction s and the faster direction f.

The policy sets n_s'=n_s+1 and n_f'=n_f-1. Total lane capacity in the reciprocal pair is unchanged.

The figure maps those letters into the diagram. The slow orange side receives capacity, and the fast green side gives up one lane.

## Slide 24. Where the Tidal Policy Lands

The TCS score compares congestion-delay mass in the slow direction with congestion-delay mass in the fast direction.

A positive score means a lane shifted toward the slow direction targets the side with more baseline flow times excess travel time.

On the map, red edges are lane-receiving directions among the top-100 corridors.

## Slide 25. Result 1

The short-run direct effect saves 1.63 seconds per commuter.

Congestion feedback and sorting absorb most of that average route-time gain in the long run.

This does not mean the local engineering effect disappears. On treated slow-direction links, travel time falls sharply. The citywide average is flatter because the network and OD weights adjust.

## Slide 26. Result 2

Welfare rises in both horizons.

Average route time and welfare do not have to move together. Route-time statistics average realized OD costs. Welfare values access to the full residence-workplace opportunity set.

The current model does not endogenize mode switching, so benefits for travelers who switch from other modes to driving are outside this accounting.

## Slide 27. Result 3

The maps report reshuffling, not citywide growth. Total residents and jobs are fixed.

Residential shares rise near treated endpoints because lower OD costs raise residential market access.

Employment gains are more concentrated, which is consistent with the agglomeration force on the workplace side.

## Slide 28. Policy Comparison

Tidal reallocation delivers a long-run welfare gain of 0.32 percent with zero net lane-km.

The targeted road-expansion benchmark reaches 0.44 percent but adds about 1,140 lane-km.

On cost, use an urban-road benchmark rather than an inter-city expressway. Urban arterial construction runs on the order of CNY 30 million per km, so a single widened lane is roughly CNY 5 million per lane-km; the 1,140 lane-km expansion is then on the order of CNY 6 billion. The precise figure is soft — the rhetorical point is the contrast: comparable long-run welfare either costs several billion in construction or almost nothing by reallocating existing lanes.

The comparison separates the two margins: moving capacity across directions versus building new capacity.

## Slide 29. Where to Put the Lanes

The welfare-elasticity score asks where one marginal lane would matter most for welfare.

It combines the direct gain on the edge, upstream residence access, and downstream workplace opportunity.

The speed-only rule performs worse because it can select slow but low-flow roads.

The targeting lesson is to combine flow, speed asymmetry, and access weights.

## Slide 30. Conclusion

The first message is measurement: directional congestion is present across cities, within-city commuter exposure, and road segments.

The second message is model-based: symmetric costs can move measured welfare and location shares before any policy shock.

The third message is policy: tidal lanes produce positive welfare gains with zero net lane-km, and their targeting should account for both congestion and spatial access.

## Slide 31. Next Steps

First, collect reduced-form evidence on implemented tidal lanes.

Second, estimate the structural parameters more tightly from data.

Third, extend the pipeline across cities, peaks, and grid resolutions.

Closing sentence: directional congestion turns a traffic-engineering fact into a spatial-equilibrium policy margin.