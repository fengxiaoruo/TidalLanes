# Directional Congestion — Detailed Oral Speaker Script

Main deck, Slides 1–30. Source: `tidal.pdf` (July 2026).

How to use this script:
- Each paragraph is a sentence or short cue you can say aloud.
- Lines in **[square brackets]** are stage directions, not spoken aloud.
- Greek letters: say *theta, alpha, beta, lambda*; say *W bar* for $\bar W$.

---

## Slide 1. Title — Directional Congestion

Good morning / good afternoon. Thank you for being here.

Today I will talk about directional congestion and tidal-lane reallocation.

The question is simple to state: peak traffic is not just slow—it is often slow in one direction and much faster in the other.

I will ask three things. First, how common is that pattern? Second, what do we miss if we treat travel costs as symmetric? Third, what can tidal lanes deliver once we put this into a spatial equilibrium model with congestion feedback?

[Transition — click to Roadmap] Let me start with the roadmap.

---

## Slide 2. Roadmap

The table of contents looks long, but the logic is actually quite tight.

I start with motivation—why directionality matters for both measurement and policy.

Then I show the data and a set of facts on directional asymmetry, from the city level down to individual road segments.

After that I build the model, explain how we take it to data, and report the counterfactual results.

I close with next steps.

The bridge through the whole talk is this: the descriptive facts motivate a directed model, and the directed model is what lets us evaluate a directional policy like tidal lanes.

[Transition] So let me begin with motivation.

---

## Slide 3. Motivation

### Block 1 — Peak-direction imbalance (point to first bullet)

Start with something everyone in this room has seen in a big city.

Peak-direction imbalance is very common.

On the same road, one direction can be heavily congested, while the other direction is relatively free.

That is what we mean by directional congestion: it is not just that the road is busy—it is busy in one direction.

This pattern does not come out of nowhere.

Commuting traffic is shaped by where workers live and where jobs are located.

In large cities, jobs and housing are often far apart—jobs–housing separation is very common.

We will show some direct evidence for that a few slides later.

### Block 2 — Daily two-way cost (point to second bullet)

Because people both live and work, the city gets two opposite peaks every weekday.

In the morning, many people crowd into job centers.

In the evening, the same system reverses, and people head home.

So if we care about the value of time, we should count both the commute and the return trip.

If we only look at one direction, we miss a big part of the daily travel burden.

### Block 3 — Policy margin (point to third bullet)

On the supply side, one infrastructure policy speaks directly to this problem: tidal lanes.

Tidal lanes reallocate existing lanes across directions.

You do not widen the road—you move capacity from the fast direction to the slow direction at peak times.

In the short run, that can ease congestion on the slow side.

In the long run, it can also change route choices and residence–workplace sorting, because access changes on both sides of the city.

### Block 4 — Video (~10 s; click play on the right)

Let me show you what this looks like on the ground.

From the video, we can see a tidal-lane truck automatically deploying the physical barrier.

As the truck moves along the lane, the barriers shift from the old centerline to the new one—so capacity shifts to the peak direction.

*[Transition — after video]*

That is the policy margin we study in this paper.

Next let me summarize what the project contributes.

---

## Slide 4. This Paper

The paper does three things.

**First — measure directionality.**

Across the top 100 congested Chinese cities in the Baidu 2025 ranking, the flow-weighted median speed asymmetry ratio is 0.878.

Remember: lower means more asymmetry.

About 58 percent of pair-flow sits below 0.90.

More congested cities tend to have stronger asymmetry—the correlation is about negative 0.48.

Within-city commuter exposure is even stronger: the median ratio is 0.819.

And at the road-segment level, the pattern is still there—median about 0.884.

**Second — quantify what symmetry misses.**

We hold the model, the fundamentals, and the parameters fixed.

The only change is to replace directed travel costs with reciprocal symmetric costs.

Even before any policy shock, measured welfare moves by about 1.10 percent.

And about 3.36 percent of residents and 6.15 percent of jobs shift location.

So symmetry is not a harmless shortcut—it changes the baseline allocation.

**Third — evaluate tidal lanes in general equilibrium.**

We pick the top 100 corridors by a flow-times-delay score and shift one lane toward the slow direction.

Welfare rises by 0.87 percent in the short run and 0.32 percent in the long run.

And this comes with zero net lane-kilometres.

By contrast, supply-side policies with similar long-run welfare gains require on the order of 1,100 lane-kilometres of new capacity.

[Transition] Before the data, let me briefly place the paper in the literature.

---

## Slide 5. Literature

The first literature is quantitative spatial equilibrium and agglomeration.

Classic economic geography and modern quantitative spatial models—Krugman, Eaton and Kortum, Allen and Arkolakis, Ahlfeldt and co-authors—give us a framework where access, productivity, and location interact.

Allen and Arkolakis (2022) is especially close, because it puts transport into spatial general equilibrium with endogenous congestion.

The second literature is transportation policy and congestion.

Vickrey, Duranton and Turner, and Fajgelbaum and Schaal speak to congestion theory and optimal networks.

Barwick et al. and Yang et al. give us Beijing evidence on congestion, pricing, and sorting.

Our contribution sits at the intersection.

We are not mainly asking how much it costs to build a new road everywhere.

We ask what happens when you reallocate lanes across directions at fixed total road width.

That margin needs directed costs and directed traffic in the model.

On calibration: Beijing speed–density estimates motivate lambda equals 1.1.

Going forward, we plan to estimate lambda directly from field flow and speed data at tidal-lane deployments.

[Transition] Here are the data we use.

---

## Slide 6. Data

We combine three types of data.

First, Baidu 2022 home–work commuting flow data. That gives us residence–workplace OD flows and location marginals.

Second, a Baidu directed road-speed panel at ten-minute frequency over one week in 2025. That gives us observed directional speeds at the segment level.

Third, road geometry, functional class, and lane records. Those are needed to build the directed network and to evaluate capacity changes.

The figure on the right is just one example of the speed data.

The key point is that the observations are directional: the same corridor can look very different depending on which way you are driving.

That is what lets us build observed edge times from k to l and compare them to the reverse direction.

[Transition] With these inputs, the first empirical fact is spatial: jobs and residences are separated, and that gives commuting a direction.

---

## Slide 7. Jobs–Housing Mismatch

This slide gives the spatial reason why peak travel has a direction.

Look at the map on the right: jobs cluster much more than residences on our three-kilometre Beijing grid.

You see residential mass in outer cells and employment mass toward the core.

Quantitatively, the Duncan index of jobs–housing dissimilarity is 0.26.

That is far above a random-allocation floor of 0.004.

We also benchmark Beijing against other cities in the appendix.

On direction: morning flows are strongly radial-inward.

About 74.5 percent of core-versus-reverse trips head toward the job-weighted center.

The inbound-to-reverse ratio is about 2.9 to 1.

Put those two facts together—separation plus direction—and peak load becomes asymmetric.

That is the opening for tidal lanes: reallocate capacity toward the slow peak direction.

[Transition] Next I show that directional asymmetry is not just a Beijing story.

---

## Slide 8. National Directional Asymmetry

Here is how we measure asymmetry at the national level.

For each pair of locations, we compute a speed asymmetry ratio: the smaller directed cost divided by the larger.

If the ratio is one, the pair is symmetric.

If it is below one, one direction is slower.

The histogram on the left shows the distribution across the top 100 congested cities.

There is a lot of mass below 0.90—this is not a rare tail event.

The scatter on the right relates asymmetry to the Baidu commute-peak congestion index.

More congested cities tend to have lower asymmetry ratios.

The correlation is about negative 0.48.

So directional asymmetry co-moves with congestion severity at the city level.

[Transition] The national pattern is clear. The next question is whether real commuters are exposed to it within a city.

---

## Slide 9. Observed Commuters Face Directional Friction

This slide uses Shenzhen grid-level commuting costs.

If you look at all potential grid pairs in the speed matrix, asymmetry is already common.

The median ratio is 0.879, and about 57.5 percent of pairs are at or below 0.90.

But commuters are not spread uniformly across all pairs.

Once we weight by actual commuter flows, the exposure becomes stronger.

About 4.4 million observed commuters face a median ratio of 0.819.

And 71.6 percent of them commute on pairs with ratio at or below 0.90.

So the asymmetry is not only a property of hypothetical OD pairs.

Real commuters sit on the more asymmetric part of the cost surface.

Weighting matters if we want a first-order measure of exposure.

[Transition] One more step: policy does not act on OD pairs directly. It acts on road segments.

---

## Slide 10. Segment-Level Policy Margin

Read the three panels from left to right: measurement, distribution, mechanism.

Panel A maps where asymmetric segments sit in Beijing.

They are spatially clustered on the network—not random noise everywhere.

Panel B gives the segment-level distribution.

The median asymmetry ratio is 0.884, and about 54.7 percent of segments are at or below 0.90.

Panel C is a schematic of how a tidal lane works in the model.

Same physical road width: one lane is shifted from the fast direction to the slow direction during the peak window.

That is the policy object we implement in the counterfactual—zero net lane-kilometres, capacity moved across directions.

So by the time we enter the model, we have already narrowed the measurement object to the level where tidal lanes actually operate.

[Transition] Those facts justify a model where every edge has a direction.

---

## Slide 11. Model — Environment

The model has a set of locations that host residents and jobs.

Each physical road is represented as two directed links—k to l and l to k.

Free-flow travel times come from night-time speeds, and they can differ by direction.

Lanes can also differ by direction.

A tidal-lane policy keeps total capacity in a reciprocal pair fixed and shifts one lane from the fast direction to the slow direction.

The realized edge time is endogenous and directed.

It rises with traffic per lane.

So if more commuting load falls on one direction, travel time in that direction goes up.

That is the first feedback we need for a directional congestion policy.

[Transition] Given directed edge times, households choose where to live, where to work, and which route to take.

---

## Slide 12. Individual Location Choice

An individual chooses a residence, a workplace, and a route.

Utility depends on residence amenity, the wage at the job location, and the route cost.

The route cost is the product of edge times along the route.

There is also an idiosyncratic preference shock—Fréchet heterogeneity.

The model gives clean choice probabilities: options with higher amenity, higher wage, and lower route cost are chosen more often.

We write the problem at the route level on purpose.

Traffic on a segment depends on which routes people pick, not only on where they start and end.

[Transition] When we aggregate over routes, we get a bilateral commuting cost between each residence–workplace pair.

---

## Slide 13. Bilateral Commuting Costs and Flows

The bilateral commuting cost from i to j summarizes all feasible routes between them.

It enters a gravity equation for residence–workplace flows.

Lower commuting cost raises the flow from i to j, holding market-access terms fixed.

Because the network is directed, the cost from i to j does not have to equal the cost from j to i.

That is exactly where the empirical directionality enters the economic model.

Pi-i and P-j are origin and destination market-access terms that clear the system.

[Transition] The next ingredient is that amenities and productivity can respond to where people and jobs locate.

---

## Slide 14. Amenity, Productivity, and Location Choices

Workplace productivity depends on local employment through an agglomeration parameter—alpha.

Residential amenity depends on local residents through a crowding parameter—beta, which is negative here.

So employment concentration can raise productivity, while residential pile-up lowers amenity.

The two fixed-point equations on the slide describe how residents and jobs spread across space given commuting costs.

This matters for tidal lanes.

If a policy changes access to a job center, it can change workplace attractiveness.

If it changes access to residential areas, it can change where households want to live.

Those responses are the long-run sorting margin.

[Transition] To make the route system tractable, we write bilateral costs through a directed network matrix.

---

## Slide 15. Route Cost vs. Commuting Cost

Each directed edge enters an adjacency matrix with entries proportional to edge time to the power minus theta.

Theta governs how sensitive choices are to travel costs.

Summing over all feasible route lengths gives a network multiplier—essentially an infinite sum of path products.

Bilateral commuting cost is then a function of that multiplier.

The intuition is simple: edge-level frictions compound through the network and become OD-level commuting costs.

You do not need to spell out every path in words—this matrix form does that for us.

[Transition] Once OD choices are made, flows come back to the road network and change congestion.

---

## Slide 16. Feedback Loop

For each OD trip, we can compute expected use of each directed link.

Add those up over all OD pairs and you get directed link traffic.

That traffic raises edge time through a speed–flow relation with elasticity lambda.

Higher lambda means congestion responds more strongly to traffic per lane.

Welfare is evaluated from the Fréchet denominator.

The important point is that welfare depends on the full set of residence–workplace opportunities—not just average travel time.

That distinction will matter when we read the results.

[Transition] An equilibrium ties all of these objects together.

---

## Slide 17. Equilibrium

Given fundamentals and parameters, an equilibrium is a full set of objects: residence shares, workplace shares, OD flows, bilateral costs, edge times, directed traffic, and market access.

Residents choose residence–workplace–route options.

Flows satisfy commuting gravity and clear residence and workplace markets.

Amenities and productivity follow the agglomeration and crowding equations.

Directed network costs generate bilateral costs.

Link traffic and edge times are mutually consistent.

Existence and uniqueness follow Allen and Arkolakis (2022), Proposition 1.

I will not derive the conditions on the main slide unless someone asks—details are in the appendix.

[Transition] With the model in place, here is how we take it to data.

---

## Slide 18. Estimation — Data and Parameters

The table maps data objects to model objects.

Residence and workplace marginals and OD flows come from Baidu 2022 commuting data.

Observed AM edge times come from weekday morning speeds, seven to nine a.m., in May 2025.

Free-flow times come from the night window—ten p.m. to six a.m.

Road length and lane counts come from geometry and lane records.

The network is a Beijing three-kilometre grid with directed adjacent links.

On parameters: theta is 6.83—cost sensitivity, from the urban spatial literature.

Alpha is 0.05—workplace agglomeration.

Beta is negative 0.10—residential crowding.

Lambda is 1.1—the speed–flow elasticity, motivated by Barwick et al.

In the talk I will say theta, alpha, beta, and lambda rather than reading every symbol.

[Transition] The next slide is the counterfactual recipe.

---

## Slide 19. Estimation — Anchor and Counterfactuals

There are four steps.

Step one: invert fundamentals.

We recover residential amenity and workplace productivity so that the observed allocation is an equilibrium at observed directed travel times.

Step two: anchor counterfactual congestion to observed speeds.

Counterfactual edge times move with traffic per lane relative to the baseline, but they cannot fall below free-flow time.

This matters because we do not want the counterfactual driven only by a theoretical free-flow formula.

The observed edge time is the anchor.

Step three: the tidal policy keeps total capacity in each reciprocal pair fixed and shifts one lane from the fast direction to the slow direction.

Step four: for each scenario, we re-solve the full general equilibrium and compare equilibria.

Effects are differences between two equilibria, with fundamentals held fixed.

[Transition] We report both short-run and long-run effects.

---

## Slide 20. Short Run vs. Long Run

In the short run, we pin residence and employment at baseline.

Routes, traffic, edge times, and bilateral costs re-equilibrate.

In the long run, residence shares, workplace shares, and OD shares can sort.

That adds market-access responses, congestion feedback, and agglomeration.

The decomposition on the slide splits long-run route-cost changes into three pieces.

ST direct is the immediate route and congestion response with baseline OD weights.

The GE wedge is the extra change from moving from short-run to long-run congestion, still using baseline OD weights.

Sorting is how OD weights themselves move in the long run.

This decomposition helps explain why average route time and welfare can move differently.

[Transition] Before the policy, the inversion tells us where the model places fundamentals.

---

## Slide 21. Baseline Fundamentals

These two maps show inverted residence amenity and workplace productivity.

They are chosen so the model matches observed residence and workplace shares at observed travel times.

The correlation between workplace productivity and employment is 0.67.

The correlation between residence amenity and residents is only 0.30.

So jobs are more tightly pinned by productivity, while residential location is more access-driven.

The treated tidal corridors also line up with high-flow employment bottlenecks.

Among the selected top-100 pairs, the tidal candidate score is highly correlated with slow-direction traffic—correlation about plus 0.91.

So the policy is not landing on random edges.

[Transition] With the baseline fitted, I first ask a measurement question: what if we force the system to be symmetric?

---

## Slide 22. What Symmetry Misses

This is a clean diagnostic.

We hold the network, lanes, inverted fundamentals, parameters, solver, and welfare formula fixed.

The only change is to replace each reciprocal pair's observed and free-flow times by their geometric means.

So it is the same model—only the input costs become symmetric.

Measured welfare rises by 1.10 percent relative to the directed baseline.

Residence dissimilarity moves by 3.36 percent; workplace dissimilarity by 6.15 percent.

Average OD cost moves by about 0.10 percent.

The interpretation is that symmetry reallocates the baseline before any tidal shock.

Under a hub-productivity shock in a robustness exercise, the symmetric model also dampens the OD-cost response.

[Transition] That motivates evaluating a policy whose whole point is directionality.

---

## Slide 23. Where the Tidal Policy Lands

For each reciprocal pair, we label the slower AM direction s and the faster direction f.

The tidal candidate score compares congestion-delay mass in the slow direction minus the same object in the fast direction.

In words: flow times excess travel time in the slow direction, minus flow times excess travel time in the fast direction.

A positive score means that shifting a lane toward the slow direction targets the side with more baseline flow times delay.

On the map, red edges are the lane-receiving directions among the top-100 pairs.

They are not scattered at random.

They concentrate on the central high-flow spine of the network.

[Transition] I now report the policy effects, starting with the route-time cascade.

---

## Slide 24. Result 1 — ST / GE / Sorting Cascade

Start with the short-run direct effect: minus 1.63 seconds per commuter, or about minus 2,400 person-minutes per day.

Negative means time savings.

Then congestion feedback adds back 0.99 seconds.

Sorting adds another 0.71 seconds.

The long-run total is only plus 0.06 seconds per commuter—about 95 person-minutes per day.

That small citywide number often surprises people, so let me be clear.

This is a citywide OD-weighted average across all baseline road commuters—not the time saving for someone who actually uses a treated link.

On the treated slow-direction links themselves, travel time falls by 20.3 percent in the short run and 13.6 percent in the long run.

The local engineering gain is real.

But once the whole network and OD weights adjust, the average can net to roughly zero.

About 56.4 percent of baseline OD commuter mass lies on shortest paths through treated corridors.

[Transition] The next slide asks whether welfare moves the same way as average route time.

---

## Slide 25. Result 2 — Welfare

The main message on this slide is that welfare rises in both horizons.

Welfare increases by 0.87 percent in the short run and 0.32 percent in the long run.

At the same time, the OD route-cost total is negative in the short run but slightly positive in the long run.

That is exactly why the decomposition matters.

Route-cost seconds are an OD-weighted travel-time diagnostic.

Welfare is different: it values access to the full residence–workplace opportunity set.

Long-run sorting absorbs part of the average time saving, but access gains keep welfare positive.

So the precise statement is not 'everyone saves commute time in the long run.'

It is: the policy improves access in a way households and firms value, even when the average commute-time statistic is flat.

[Transition] To see why the average time statistic is flat, we need the sorting maps.

---

## Slide 26. Result 3 — Long-Run Sorting

These maps report demeaned changes in residence and workplace shares.

Total city population and total jobs are fixed—we are looking at reshuffling, not citywide growth.

Positive cells gain share relative to baseline; negative cells lose share.

Black circles mark endpoint nodes on treated corridors.

Residential shares rise near those endpoints because lower OD costs raise residential market access.

Employment gains are more concentrated—in a smaller set of red cells.

The table at the top reports mean log changes for endpoint nodes versus the rest.

These are unweighted cell means, not population-weighted moments.

Endpoint residential share rises on average.

Endpoint employment mean is slightly negative—not because there are no job gains anywhere, but because gains are localized and many other cells lose share when totals are fixed.

[Transition] Is this a good deal compared with building new lanes?

---

## Slide 27. Result 4 — Policy Comparison

This table compares tidal reallocation with a road-expansion benchmark.

Tidal reallocation delivers long-run welfare of plus 0.32 percent with zero net lane-kilometres.

Road expansion—adding one lane in each direction on the top-100 flow-times-delay corridors—reaches plus 0.44 percent, but adds about 1,140 lane-kilometres.

The point of the comparison is not to say expansion is impossible.

It is to separate two margins: reallocating capacity across directions versus adding physical capacity.

Tidal lanes deliver lower raw welfare gains than targeted expansion, but the cost margin is completely different because net lane-kilometres are zero.

The expansion row is a capacity benchmark, not a construction-cost estimate.

The appendix also compares uniform expansion for the same lane-kilometre budget.

[Transition] If a city wants to use this margin, the next question is where the lane shifts should go.

---

## Slide 28. Result 5 — Where to Put the Lanes

This slide compares alternative rules for picking the top-100 corridors.

The map shows local welfare elasticity—where one marginal lane would matter most for welfare.

The score decomposes into three pieces: the direct gain on the edge, upstream residence access, and downstream job access.

Under welfare-elasticity targeting, long-run welfare is plus 0.35 percent.

Under the TCS rule—the one we use in the main spec—long-run welfare is plus 0.32 percent.

Under speed-only targeting, long-run welfare is only plus 0.13 percent, and the person-minute outcome is much worse.

Flow matters.

The rank correlation between speed-only selection and welfare elasticity is only 0.08.

The correlation between flow-times-delay and welfare elasticity is 0.65.

Welfare elasticity and TCS both put capacity on the high-flow spine.

Speed-only also picks slow but low-flow edges—which is bad for both welfare and average time.

The takeaway for targeting: combine flow, speed asymmetry, and access weights.

[Transition] Let me summarize.

---

## Slide 29. Conclusion

Three messages.

First, directional congestion is widespread.

It shows up across cities, in observed within-city commuter exposure, and down to individual road segments.

Second, ignoring direction mis-measures welfare.

A symmetric cost surface moves measured welfare by about 1.10 percent and reallocates a meaningful share of residents and jobs—even before any policy.

Third, tidal lanes are a cost-effective way to capture part of those gains.

Short-run route cost falls and welfare rises.

Long-run sorting absorbs part of the time gain, but welfare remains positive.

And this comes with zero net lane-kilometres, while comparable expansion welfare requires on the order of 1,100 lane-kilometres.

When choosing corridors, combine flow, asymmetry, and access—not speed alone.

[Transition] I close with next steps.

---

## Slide 30. Next Steps

First, field measurement on implemented Shenzhen tidal lanes.

We want on-site traffic video, vehicle counts, and directed speeds around real switches.

That is also the primary path to estimate lambda from local flow and speed.

Second, identify structural parameters—alpha, beta, theta, and lambda—from data where possible, rather than importing everything from the literature.

Third, extend scope: run the full pipeline for both Beijing and Shenzhen, for AM and PM peaks, and across multiple grid resolutions.

Closing line:

A directional congestion fact leads naturally to a directed spatial-equilibrium model, and that model makes a zero-net-capacity policy margin quantitatively visible.

Thank you. I am happy to take questions.

---
