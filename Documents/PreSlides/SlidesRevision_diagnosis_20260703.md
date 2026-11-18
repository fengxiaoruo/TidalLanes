# Diagnosis — `tidal_advisor_revision_segmentonly_20260703` vs. advisor comments

Checked every item in `SlidesRevision.docx` against the latest deck (segmentonly, 46 frames). Page numbers below are the advisor's (old deck).

## ✅ Done correctly
- **P1** Title date → "CES Chengdu, 2026".
- **P2** Roadmap merged to 5 sections; **Counterfactuals** added after Estimation.
- **P3** Motivation rewritten: 3 bullets (widely observed / literature ignores / tidal low-cost) with *Examples*+*Except* sub-bullets and the "little known about ST/LT effects" line. New **"Directional Congestion" four-city page added before the roadmap** (advisor's preferred option).
- **P4** "Measure directionality…trip and road-segment"; "Quantify what symmetry misses…spatial GE"; "have **a** lower SAR"; tidal one-lane-per-100 clarified; capacity-expansion "**1,100 lane-km…tidal add zero**".
- **P5** Literature → 3 bullets (Directional congestion / Spatial equilibrium / Transportation policy) with **demand vs. supply** split; "This paper evaluates tidal lanes as one congestion-alleviation policy."
- **Descriptive reorder** matches advisor's pattern: trip-level → commuters suffer → road-segment → job-housing.
- **P6** Data → "Directed speed" (national trip + road segment) / "Commuting flows" / "Road characteristics".
- **P7** Job-housing slide moved before Model; title "Directionality Arises from Jobs–Housing Mismatch"; redundant third bullet removed.
- **P8** Title "National Trip-Level Directionality".
- **P9** "Observed" removed from title and bullet.
- **P10** Title "Road Segment Level Directionality"; emphasizes segments identify candidate roads; **tidal schematic (Figure C) + policy moved to Counterfactuals**.
- **P13** "Fréchet" accent fixed everywhere (verified: no un-accented "Frechet").
- **P18** λ note: "future versions will estimate it from field flow and speed data."
- **P19** Anchor/Solve slide de-tidal'd (baseline estimated without tidal lanes).
- **P21** Fundamentals keeps a hint on tidal corridors.
- **P24** Result 2 adds the mode-switching caveat (benefits of switchers into driving are outside the accounting).
- **P29** Conclusion "first-order" removed.
- **P30** Next Steps first bullet → "Reduced-form evidence on the effects of tidal lanes."
- **P33–36, 38–40, 44** Back links: **all `\navback`/`\app` targets resolve to existing labels** — verified, no broken links.

## ⚠️ Not done / partial — needs your attention
1. **P4 first sub-bullet (This Paper).** Advisor asked to **drop** the redundant "median SAR 0.878; 58% below 0.90" and replace with **"SAR below 0.33 for xx% of flows"** (noting 0.33 = local traffic-bureau threshold). Deck still shows the old 0.878/0.90 line. → decide: keep 0.90 prevalence or switch to the 0.33 framing.
2. **P4 second sub-bullet.** Advisor wanted **"within-city: SAR below 0.33 for xx% of road segments."** Instead it reads a generic "Road-segment data identify candidate corridors." → the specific 0.33 stat is missing.
3. **P8 National Trip-Level.** Advisor wanted an added **"xx% of trips feature SAR ≤ 0.33."** The segmentonly file actually **deleted** the 0.33 line that the *main* revision (`tidal_advisor_revision_20260703.tex`) still has. → currently no 0.33 stat at trip level.
4. **P10 Road Segment.** Advisor wanted **"xx% of road segments feature SAR ≤ 0.33"** stressed. Deck gives the **count (179 centerlines)** and 54.7% ≤ 0.90, but not the % below 0.33.
5. **P27 Policy comparison.** Advisor asked to **add construction cost per lane-km**. Deck still shows "[TODO: verify local construction cost]".
6. **P5 Literature (minor).** Advisor suggested a public-transit / subway AEJ cite under supply-side; not on the slide.

## ⚑ Divergence to resolve: two revision files
`tidal_advisor_revision_20260703.tex` **keeps** the strict SAR ≤ 1/3 screen numbers at trip/OD/segment level (12 of 7,460 township pairs; 605 grid pairs / ~34,200 commuters; 179 of 3,670 centerlines).
`tidal_advisor_revision_segmentonly_20260703.tex` (the newest) **removes** them everywhere except the segment count.

The "segmentonly" cut is consistent with the advisor's own remark on P8 ("trip data suggest asymmetry but aren't good for choosing candidate roads — that's the value of segment data"). But the advisor **also explicitly asked** for the 0.33 percentages at trip and within-city level (items 1–4 above). So the two files embody two readings. **Pick one**: either (a) commit to segment-only 0.33 screening and reword items 1–2 so they don't promise trip-level 0.33 stats, or (b) fill the "xx%" numbers everywhere from the data.

## Resolved this round (2026-07-03 pm)
- **Divergence settled → segment-only 0.33.** Kept the 0.33 threshold **only at the road-segment level**, framed as a progression (trip/OD data show prevalence; segment data name candidate roads). Segment slide reframed: Beijing **179 centerlines (4.9%)** as tidal candidates; noted 0.33 = local traffic-bureau screen. Items 1–4 above are intentionally *not* filled at trip/This-Paper level.
- **Item 6 (public transit) done.** Added **Gu, Jiang, Zhang & Zou (2021), "Subways and Road Congestion," AEJ:Applied 13(2):83–115** to `references.bib` and the Literature slide (supply-side / public transit). Deck recompiled; all citations resolve.
- **Four-city figure relocated.** It's OD/trip-level, so the pre-roadmap opener page was removed and the figure moved to a new appendix slide `appfourcity` (p.32), linked from the National Trip-Level slide. Title now goes straight to the roadmap.
- **Still open:** item 5 — construction cost per lane-km on Result 4 (still a TODO placeholder).

## Where the "xx% ≤ 0.33" numbers live
The percentages aren't precomputed. Sources: SAR distributions under `data_work/outputs/.../metrics/descriptive_audit/` (segment + grid) and the national trip pipeline. A short script over those SAR columns (`share where sar ≤ 1/3`, flow-weighted for trips/commuters) will produce all four numbers. Say the word and I'll compute them.
