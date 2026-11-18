# Speaker notes: discussion with Brad Ross, November 2026

Deck: `slides_brad_202611.pdf` (17 main pages, 5 appendix pages). Numbers use the main specification as of 2026-11-15: 2025 platform OD, OSM network, congestion elasticity 0.35, theta 6.83. Sources for every number are listed at the end.

使用说明：每页分三块。Say 是要讲的话，可以照着说。Ask 是这一页要问 Brad 的问题。If asked 是他可能追问的点和回答。时间按 45 分钟安排，讲 20 分钟，留 25 分钟讨论。前三个议题我们有新结果，第四个议题主要听他的意见。

## Time plan

| Block | Pages | Minutes |
|---|---|---|
| Opening and setup | 2-3 | 3 |
| Congestion technology | 4-11 | 19 |
| First and second order | 12-13 | 6 |
| Policy margins | 14-15 | 6 |
| Departure time | 16 | 8 |
| Questions and next steps | 17 | 3 |

If time is short, skip pages 3, 7, 11 and 15. Do not skip pages 5, 6, 8, 9, 12 and 16.

---

## Page 2. Since October

**Say.** Last time you suggested I look harder at the congestion technology and try more versions. That is most of what I did. Three things changed.

First, the congestion elasticity. I was using 1.1. With the new speed and flow data the elasticity of travel time to flow is about 0.35 at peak load, and it rises with load.

Second, I split the welfare effect of tidal lanes into a first-order term and the rest, which you asked about. In the short run the first-order term is 97 percent of the total. In the long run a third of the gain goes away, and all of that is people changing where they live and work.

Third, I ran the same split for seven policies. Capacity policies save time when they open and lose the saving after sorting. Tolls go the other way. Tidal lanes get 96 percent of the welfare of widening the same corridors in both directions, with no new lane-kilometres. That statement is for commuters, and I will qualify it on pages 8 and 9.

What I do not have is departure time. The model has one period. That is the part where I most want your view.

**Ask.** Nothing here. Ask whether he wants to start with congestion or with the decomposition.

---

## Page 3. Setup

**Say.** One page of reminder. Route choice on a directed network as in Allen and Arkolakis, with residence and workplace choice on top. Beijing, 3 kilometre cells, about 2,900 directed edges, morning peak.

Travel time on an edge depends on flow per lane. The function is anchored at the observed peak: at observed flow and observed lanes it returns the observed time. There is a free-flow floor.

A tidal lane moves one lane from the light direction to the heavy direction on the 100 most asymmetric corridors. Lane-kilometres do not change.

Short run means locations are fixed and routes adjust. Long run means both adjust.

**If asked: why a grid and not roads?** The grid edge bundles the roads that cross a cell boundary. Policies are defined on edges. I come back to whether that aggregation is right on page 6.

**If asked: what is the flow?** Commuting car trips from mobile-phone OD, routed by the model. Non-commuting traffic enters only through the anchor, which assumes it scales with commuting flow. That assumption is weak in one direction: on tidal corridors the light-to-heavy flow ratio is 0.29 for commuting and 0.48 for all traffic.

---

## Page 4. Congestion Elasticity

**Say.** This table is the main change. All four rows are elasticities of travel time with respect to flow, estimated on one week of 10-minute data, with flow instrumented by OD demand assigned to free-flow shortest paths.

Row one is the specification in Almagro and coauthors: a free-flow floor plus a constant elasticity above it, with the floor set to the night-time mean. That gives 0.245 on grid edges and 0.42 on road segments. The two levels disagree.

Row two estimates the floor together with the slope. The data move the floor up by about 3 percent, and the elasticity becomes 0.35.

Rows three and four drop the floor. I keep only high-load hours and run a linear IV. That gives 0.35 above the 70th percentile of load and 0.42 for 7 to 9 in the morning. Edges and segments now agree.

So two different routes give about 0.35 at peak. The 0.25 was produced by where I put the floor.

On the old number: 1.1 was an elasticity of speed with respect to density. Flow is density times speed, so the elasticity of time to flow is epsilon over one minus epsilon. For epsilon above one that is the hypercongested branch, and time as a function of flow is not defined there. An elasticity of 0.35 for time to flow corresponds to 0.26 for speed to density.

**Ask.**
1. You estimate the flow-density curve by constrained least squares without an instrument. Is that because sensor data by lane at five minutes do not need one, or because there is no good instrument?
2. What is the main threat to assigned OD demand as an instrument for flow? My worry is that demand shocks at the hour level also move incidents and signal timing.

**If asked: why is IV about twice OLS?** I do not have a clean answer. Sampling noise in flow at the edge level is small, so classical measurement error does not explain a factor of two. Reverse causality is the other candidate: slow hours shed flow, which pushes OLS down.

**If asked: only one week?** Yes, 19 to 25 May 2025. I can ask the platform for more weeks.

**If asked: stale data?** About 18 percent of segment speeds are constant fills. Dropping them raises the floor estimate from 0.245 to 0.296. The appendix page has this.

---

## Page 5. Free-Flow Floor

**Say.** This is why I stopped trusting the floor specification. The horizontal axis shifts the floor up or down relative to the night mean. Three percent up, and the elasticity goes from 0.25 to 0.37. Five percent up, 0.56.

The mechanism is simple. The floor decides where congestion starts. Raise it, and only the high-flow hours stay on the sloped branch, where the true curve is steeper. The relation between time and flow is convex, so the kink location sets the slope.

And the night mean is not the right floor. Daytime hours with the lowest load are still 3.5 percent slower than the night mean. Signals and pedestrians are there in the day even with no congestion. Two joint estimators put the floor 2.6 to 3 percent above the night mean, which is the shaded band.

The sampling error of the floor is negligible. The problem is its definition.

**Ask.** In your flow-density estimation you tie the slope at zero density to the speed limit, so free flow comes from outside the data. Would you do the same here, set the floor from speed limits or a free-flow speed, or estimate it?

**If asked: can you estimate it jointly?** Yes, but then the slope is not pinned down. The shift is estimated precisely, 0.026 with a standard error of 0.005. The elasticity has a 95 percent interval from 0.26 to 0.48. Edge-specific floors are not identified with about 110 hours per edge.

---

## Page 6. Elasticity and Load

**Say.** Left panel: travel time against flow within edge, 20 bins. It is flat at low flow and then bends upward smoothly. There is no kink, so a floor plus one slope is an approximation to a convex curve.

Right panel: the linear IV on subsamples with higher and higher load. 0.16 on all hours, 0.29 above the median, 0.35 above the 70th percentile, 0.44 above the 85th. I select on assigned demand, the instrument, not on speed, so the selection does not bias the slope.

I take 0.35 as the main value because the model is anchored at the 7 to 9 peak and the counterfactual moves edges around that point. I report 0.29 to 0.44 as the range, and I keep 0.25 as a lower bound that follows the method in the literature.

**Ask (if time).** A grid edge is a bundle of parallel roads. Should I estimate a curve on road segments and let route choice aggregate it, or is an edge-level elasticity acceptable? Without the floor the two levels give the same number, which I did not expect.

**If asked: BPR?** A standard BPR function with exponent 4 has a local elasticity of about 0.36 at a volume-to-capacity ratio of 0.9 and 0.23 at 0.8. So 0.35 is in the usual range for a busy arterial. My own BPR fit on these data has a lower exponent, 1.65.

---

## Page 7. Load-Dependent Form

**Say.** If the elasticity rises with load, one constant is wrong in a specific way for tidal lanes. The heavy direction is high on the curve: in the morning peak those edges are at the 96th percentile of their own daytime load. The light direction is at the 61st. So I expected a constant elasticity to overstate what the light direction loses.

I tried it, and the answer is less clean than I hoped. The table has four versions.

Row two keeps 0.35 at the observed point and lets the elasticity fall as the heavy edge decongests. Welfare drops by about 10 percent, because the edge slides down its own curve once it gets the lane.

Row three gives heavy edges a higher elasticity than light edges and keeps the average at 0.35. Welfare rises by 15 percent in the short run.

Row four takes the levels from a quadratic fit, which puts heavy edges at 0.29. Welfare falls by 15 percent.

So the sign depends on the level I assign to the heavy direction, and the estimators disagree on that level: a quadratic in log flow gives 0.29 at heavy positions, the 7 to 9 subsample gives 0.42.

The light direction hardly matters. Moving its elasticity from 0.10 to 0.50 changes welfare in the third decimal. That was the surprise, and the next page explains it.

**Ask.** In your model the per-lane curve does this automatically. Would you prefer a full curve to a constant elasticity anchored at the observation, with one week of data?

**If asked: is the curvature identified?** In the quadratic, yes: the slope of the elasticity in log flow is 0.21 with a standard error of 0.02. A three-segment spline says the curve stops steepening above the middle of the load range. The shape at the top is not settled.

**If asked: does the comparison with expansion change?** No. Tidal lanes stay at 95 to 99 percent of same-corridor expansion in every version.

---

## Page 8. Flows by Direction

**Say.** This is the most important thing I found this week, and it is a weakness.

On the 100 tidal corridors the model's flow in the light direction is 2 percent of the flow in the heavy direction. In the road data, counting all vehicles from 7 to 9, it is 67 percent.

The model has morning commuters going from home to work, and nothing else. On these corridors that is close to one-way traffic.

The tidal lane cuts time by about 10 percent on the heavy edge and raises it by about 15 percent on the light edge. The model weights that 15 percent by almost nobody.

The second row shows what that does. I hold flows fixed, change the lanes, and add up time gained and lost. With model flows the loss on the light side is 3 percent of the gain on the heavy side. With observed flows it is 81 percent. The net time saving with observed flows is about one fifth of the net saving with model flows. This is a partial calculation on 68 corridors with flows held fixed, not an equilibrium result.

So the welfare number I report is the welfare of morning commuters. Other vehicles in the light direction lose time, and the model does not count them.

This also brings the load-dependent form back. With observed flows as weights, a higher elasticity on the heavy side than on the light side roughly doubles the net time saving under two of the three schedules I estimated, because the light side now matters.

The next page puts that traffic into the model.

**Ask.**
1. You scale sample flows to total flows with one factor. Here the factor would differ by direction: non-commuting traffic is far more symmetric than commuting traffic. How would you bring it in?
2. Would you count the time of non-commuting vehicles in welfare, and at what value?
3. Is there evidence on how real tidal or reversible lanes affect the light direction? I have not found an economic evaluation.

**If asked: why did you not see this before?** An earlier check compared commuting OD with all-purpose OD, both routed on the network, and found a light-to-heavy ratio of 0.29 against 0.48. It used observed OD, not the model's equilibrium flows, and it was not restricted to the corridors the model selects. The selection picks the most asymmetric corridors by model flow, which makes it extreme.

**If asked: does this overturn the result?** It does not change the sign for commuters. It means the number is an upper bound on the gain to all road users, and possibly a loose one.

---

## Page 9. Background Traffic in the Model

**Say.** So I put non-commuting traffic into the congestion function as a fixed flow on each edge. Travel time now depends on commuting flow plus background flow. At baseline the function still returns observed times, and with zero background it returns the old model exactly. Background comes from observed road flows net of commuters, and in total it is about two and a half times commuting flow.

Three results.

Commuter welfare survives. It is 0.64 percent in the short run, the same as before, and 0.47 in the long run, a little higher. Background traffic dampens the congestion that sorting brings back.

Counted over all vehicles, the loss on the light side is no longer negligible. It offsets 41 percent of the gain on the heavy side, against 2 percent before. All vehicles together still save time in the short run, 33 seconds per commuter.

The comparison with expansion changes. For commuter welfare, tidal lanes are still at 95 percent of same-corridor expansion. For time saved by all vehicles they are at about half. Expansion helps both directions, and a tidal lane takes from one of them. Across the versions I tried the share runs from a quarter to 86 percent, depending on how much background there is and which direction it sits in.

So the statement I have been making, that tidal lanes get 96 percent of the benefit of expansion at no capacity cost, is true for morning commuters. It is not true for road users as a whole.

One thing background does not fix. The light-to-heavy flow ratio rises from 2 percent to 29 percent, still well below the 67 percent in the data. Observed commuters alone have a ratio of 52 percent. The model's commuting flow is too one-directional, which I think comes from the same place as its distance decay being too steep.

**Ask.**
1. Should the headline be commuter welfare, or time for all vehicles with commuter welfare as one part?
2. How would you value the time of non-commuting vehicles?
3. Fixed background on edges, or a background OD that reroutes? I tried rerouting once: all vehicles save 39 seconds and the share of expansion is 78 percent.
4. Re-selecting corridors on total flow keeps 87 of the 100. Is choosing corridors on commuting flow defensible?

**If asked: why does long-run all-vehicle time rise?** It is mostly commuters choosing longer commutes, as before. Background vehicles lose about 2 seconds per commuter in the long run.

**If asked: how sure is the scale?** Not very. The flow data count sampled people. Scales from 0.5 to 3.4 times commuting flow leave commuter welfare almost unchanged and move the all-vehicle share of expansion from 0.86 to 0.42.

---

## Page 10. Flow and Density per Lane

**Say.** I tried your object on Beijing data: 18,000 road segments, 10-minute speed and flow, one week. Each curve is within segment. Flow is relative to the segment's own mean and speed is relative to its own free-flow speed, so no lane count and no flow scale enters the picture.

Read it from left to right, fast to slow. All three road classes have the rising free-flow branch.

Expressways then turn down. Flow peaks at about 40 percent of free-flow speed, and at the lowest speeds it is 88 percent of the peak. They are on the falling branch in about a third of weekday peak slots. So the throughput drop is visible at 10 minutes.

Arterials and secondary roads keep rising and flatten. That looks like a capacity plateau set by intersections. They are on a falling branch in 2 percent of peak slots.

Two things I could not do. First, capacity per lane. My flow is a count of phone-sample passages, so it is an index with an unknown scale, and it counts people. Second, your lane scaling. I have no observed lane counts. Lanes are assigned from road class, six, four and two, so lane count and road class are the same variable. With those lanes the per-lane curves do not collapse, but I cannot tell lane scaling from a wrong lane map or from different sampling rates by class.

One caution on method. Density is built from flow, so binning on density overstates capacity by 50 to 70 percent on arterials. I bin on speed, and I checked with a split sample of the flow measure.

A rough calculation with the within-segment curve, flows held fixed: on an arterial with the heavy direction at 70 percent of free-flow speed and the light direction at 95 percent, moving one lane raises heavy-direction speed by about 28 percent, lowers light-direction speed by about 13 percent, and cuts total travel time by about 10 percent. On the most asymmetric pairs the heavy direction is already queued, and the static curve gives no usable answer.

**Ask.**
1. With one week of data and no sensor counts, which parts of the fundamental diagram would you trust?
2. For lane scaling I need lane counts by segment, for example from OpenStreetMap, and ideally vehicle counts. Is there a cheaper test of the per-lane form?
3. Arterials look like a plateau and not a falling branch. In your model, how are signalised roads treated?
4. Once the heavy direction is queued the static diagram fails. Is that the point where a tidal lane needs your sending and receiving functions?

**If asked: does this agree with the elasticity of 0.35?** In order of magnitude. The within-segment curve implies an elasticity of time to flow of about 0.2 at 92 percent of free-flow speed and 0.3 to 0.5 at 87 percent. The two use different variation: time of day here, day to day in the IV.

**If asked: data quality?** About 18 percent of speed slots are stale constant runs, and Baidu has five default speed values that make up 20 percent of all slots. Both are dropped. Keeping the default values changes results by less than 1 percent.

---

## Page 11. Elasticity and the Tidal Result

**Say.** What the elasticity does to the main result. Welfare scales with it: 0.31 percent in the long run at 0.25, 0.40 at 0.35, 0.46 at 0.44. Short-run time saved goes from 22 to 37 seconds per commuter.

What does not move: the 100 corridors are identical at every value, because selection uses baseline flows only. The sign of every effect is the same. And tidal lanes give 96 percent of the welfare of same-corridor expansion at both 0.25 and 0.35.

One cost of 0.35: it is above the sufficient condition for uniqueness, which is 0.317 in this model. I solved each equilibrium from seven starting points. At a tight tolerance they agree to six decimal places. That is numerical evidence, not a proof.

**If asked: why does long-run time rise?** Lower commuting costs on the treated corridors pull residents and jobs further apart along them. Average commuting time goes up by 15 seconds while welfare rises, because people choose the longer commute.

---

## Page 12. First and Second Order

**Say.** You asked last time how much of the effect is first order. This is the decomposition at theta 6.83.

The first-order term holds flows, routes and locations at baseline. Only the lane change moves link times, and I weight by baseline link use. That gives 0.607 percent.

Then I let the lane change have its exact effect at fixed flows, which adds 0.057. Rerouting brings congestion feedback, which takes away 0.037. Those two nearly cancel, so short-run welfare is 0.627 and the first-order term is 97 percent of it.

In the long run sorting removes 0.230, and welfare is 0.397.

In minutes: the direct saving is 30 seconds per commuter. Rerouting and congestion give back 11. Sorting gives back 34. Long-run commuting time is 15 seconds longer than at baseline.

The second-order terms grow with the elasticity. At 0.25 the first-order share was 98 percent and the long-run to short-run ratio was 0.71. At 0.35 they are 97 percent and 0.63.

This is close to what you find in a different margin. Your added lane raises speed by 13.6 km/h with departure choices fixed and by 4.2 in equilibrium, so about 70 percent is undone by induced driving, with no location choice. Here routes and congestion undo about a third of the time saving, and sorting undoes the rest.

**Ask.**
1. Do you split your welfare numbers into a direct effect and equilibrium feedback?
2. My short-run welfare uses a formula that assumes free location choice, while locations are held fixed. How would you define short-run welfare?
3. Long-run minutes go up and welfare goes up. Would you lead with minutes or with welfare?

**If asked: how does this compare with Tsivanidis?** He reports that the first-order approximation is about half of full equilibrium welfare for TransMilenio. [Verify the exact share before quoting.] His shock is large and non-marginal. Ours is one lane on 100 corridors.

---

## Page 13. Role of theta

**Say.** I expected theta to matter for the split between first and second order. It does not. Across theta from 3.4 to 10 the first-order share stays at 0.96 to 0.97, and the long-run to short-run ratio stays at 0.62 to 0.64. Time savings are identical. Welfare scales as one over theta.

The reason is the cost specification. Link cost is minutes to the power one over theta, following Allen and Arkolakis, so theta cancels in the route weights. Route shares and flows do not depend on it. Theta only converts time into welfare.

What changes the structure is separating two dispersions that the baseline model ties together: how strongly people respond to cost across routes, and how strongly they respond across residence and workplace pairs. I call the ratio eta. With eta equal to one the model's commuting falls with distance 4.7 times faster than in the data. Eta of 0.21 matches the observed decay.

With eta at 0.21 three things change. Welfare falls to about a quarter. The first-order share falls to 0.87. And long-run travel time now falls, by 20 seconds, where before it rose. Sorting is weaker, so it no longer undoes the time saving.

So the welfare level depends much more on eta than on theta or on the congestion elasticity.

**Ask.**
1. How would you identify route dispersion separately from location dispersion? I have observed OD flows and route-level times, but no price variation.
2. Your value of time comes from experimental prices. Without price variation, what should pin down the welfare value of a minute: the observed gravity semi-elasticity, or an outside value of time?

**If asked: what is welfare per minute saved?** 1.26 percent per minute in the main specification and 0.17 with eta at 0.21. Converting the observed gravity semi-elasticity through theta gives a number in between. [Recompute at 0.35 before quoting; the 0.73 in the notes is at the old elasticity.]

---

## Page 14. Direct and Long-Run Effects

**Say.** You asked for more ground-transport policies with direct and long-run effects side by side. Light bars are time saved on impact, dark bars after sorting.

For every capacity policy the pattern is the same. Time is saved when it opens, 13 to 30 seconds per commuter, and the long-run change is negative. Welfare stays positive but falls to 63 to 70 percent of its short-run value.

Tolls are the reverse. The cordon toll saves 33 seconds on impact and 95 in the long run. The Pigouvian toll saves 102 and then 276. Sorting works with the toll.

For tidal lanes the comparison I care about is with widening the same corridors in both directions. That adds 891 lane-kilometres and gives 0.414 percent. Tidal lanes add nothing and give 0.397.

Tidal lanes sit between the two groups in one respect. They make the heavy direction cheaper, which induces demand, and the light direction dearer, which suppresses it.

**Ask.**
1. You normalise by adding a lane to every freeway. I normalise by same-corridor expansion. Should that kind of benchmark be net of construction cost?
2. Your added lane keeps about 30 percent of its speed gain. If residences and jobs could move, would you expect that share to rise or fall?
3. Is a tidal lane combined with a toll on the light direction worth computing?

**If asked: is the cordon toll better than expansion?** They are about equal: 0.424 against 0.419 and 0.414. The gap is not solver error, but it is small, and toll welfare uses a different formula based on realised route choice. At an elasticity of 0.25 the cordon toll was below tidal lanes.

**If asked: is the Pigouvian toll solved exactly?** The toll is updated by successive averaging and converges slowly. Six starting points give the same welfare, 1.096.

---

## Page 15. More Ground-Transport Policies

**Say.** Three more policies fit the model as it is. None has been run yet.

A bus lane removes one lane in each direction. It is the mirror image of expansion and gives an upper bound on the loss to cars, since there is no mode choice.

A driving restriction scales car demand by 0.8. It lets me compare a quantity control with a toll that removes the same traffic.

Signal priority lowers free-flow time on the heavy direction only. It separates two reasons tidal lanes work: being directional, and adding a lane.

**Ask.** In a static model with no mode choice, is it better to represent a restriction or a bus lane as an exogenous cut in demand, or as a cost wedge?

---

## Page 16. Departure Time

**Say.** This is where I have the least. A tidal lane is open two or three hours a day. The model has one period, with all-day OD and 7 to 9 travel times. Only 56 percent of commute departures fall in that window. So the model cannot say what a tidal lane does to the timing of trips.

Your paper has departure time, routes and a time-varying network equilibrium, with no location choice. I have location choice and a directed network, with no departure time. I did not find a paper with all three.

I see three ways to go, from simple to hard.

Option one: a static route equilibrium for each 30-minute period, with location choice based on expected commuting cost across periods. The lane changes only in the periods when it is open. No new parameters. No substitution across periods.

Option two: add a logit choice of departure period on top, with costs of arriving early and late. This needs schedule costs, a dispersion parameter and a distribution of desired arrival times, and uniqueness has to be checked again.

Option three: dynamic loading with queues that carry over, as in your model. That gives up the closed form.

On identification for option two: Kreindler uses experimental prices and you use rain. I have neither. What I have is the opening window of the tidal lane, which changes capacity at a known clock time every day, and one corridor with traffic video around the switching time.

**Ask.**
1. Are your time-of-day preferences fixed effects by period, or early and late costs? How did you choose 15 minutes?
2. Do you have a uniqueness result for the dynamic equilibrium, or do you rely on multiple starts?
3. If you added location choice to your model, would you treat departure time as a short-run choice with locations fixed, or nest the two through expected utility?
4. How large is the error from option one, in your experience?
5. Can the opening window of a tidal lane identify substitution across departure periods?
6. Departure times from phone data are noisy. How do you treat that when you weight the OD data?

**If asked: what data do you have?** One week of 10-minute and 30-minute OD with departure time, and 10-minute segment speeds for the same week.

---

## Page 17. Questions

**Say.** These are the six questions where your answer would change what I do next. Go through whichever have not come up.

After the meeting, write down for each: his answer, whether it changes the specification, and what to run.

---

## Appendix pages

- **Estimation Design.** Use if he asks about the sample, the instrument, clustering, or stale speeds.
- **Alternative Floors.** Use on page 5 if he asks about other floors. Every floor below the night mean lowers the estimate; the data ask for a higher one.
- **Numerical Uniqueness.** Use on page 11.
- **First-Order Term.** Use on page 12 if he asks for the formula.
- **Policy Comparison.** Use on page 14 for exact numbers.

## Things to verify before quoting

- Kashner and Ross: the meaning of the reported "0.11%" for travel time with respect to monthly flow.
- Tsivanidis: the exact first-order share of full equilibrium welfare.
- The welfare value per minute from the gravity semi-elasticity has not been recomputed at an elasticity of 0.35.
- Whether a quantitative paper with bottleneck congestion and location choice exists (one candidate in RSUE was not accessible).

## Where the numbers come from

| Numbers | Output directory under `data_work/outputs/` |
|---|---|
| Elasticity estimates, floor shifts, binned shape, bootstrap | `congestion_floor_sensitivity_v1` |
| Tidal result by elasticity; uniqueness check | `beta_c_rerun_b035_summary_v1` |
| Load-dependent form; flows by direction | `load_dependent_elasticity_trial_v1` |
| Flow and density per lane | `flow_density_per_lane_v1` |
| Background traffic in the model | `background_traffic_trial_v1` |
| Decomposition, theta and eta | `theta_first_second_order_osm1b_b035_alldaytt0709_v1` |
| Policies | `policy_comparison_osm1b_ce_floor_b035_fl001_ks_od2025_alldaytt0709_v1` |
| Multi-start check for policies | `policy_multistart_osm1b_ce_floor_b035_fl001_ks_od2025_alldaytt0709_v1` |
| His paper and the literature | `Documents/L2_notes/brad_meeting_prep_20261113.md` |
