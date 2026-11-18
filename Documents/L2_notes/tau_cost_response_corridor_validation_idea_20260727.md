# Idea note — validating directional cost response for corridor design

**Date:** 2026-07-27  
**Status:** research idea / implementation sketch (not started as a dedicated pipeline)  
**Related:** national Baidu 2024/2025 OD crawl; Shenzhen multi-tidal post-only; Beijing corridor scoring; Caitian / Liuxian reduced-form tracks

---

## 1. What this idea is

**Claim in one sentence.**  
Measured directional commuting-cost changes (and the mapping from lane reallocation into \(\tau\)) should be tied to real corridor geometry, optionally aggregated to coarser spatial units for cross-city comparison, and checked against Shenzhen’s already-built tidal lanes.

**What it is not.**  
This is **not** classical identification of the level of \(\tau_{ij}\) as a structural parameter. In the QSM, \(\tau_{ij}\) is a constructed bilateral cost index from the directed network. The object here is closer to:

1. **Policy → cost mapping:** tidal-lane (or directional capacity) shock \(\Rightarrow \Delta\) speed / time \(\Rightarrow \Delta\tau_{ij}\) (or a local cost-response slope).
2. **External validation of directional \(\tau\):** do high-asymmetry / high-score corridors coincide with places that already got tidal lanes?
3. **Design relevance:** can measured cost asymmetry be aggregated from segments up to corridor / district–county units in a way that is useful for route design (“动线”), not only for edge-level CF ranking?

**Clarifying language for later writing.**  
Prefer “identify / validate the cost response that enters \(\tau\)” or “validate directional \(\tau\) against implemented corridors,” not “identify \(\tau_{ij}\).”

---

## 2. Why county / coarser aggregation appears

Fine units (Baidu `roadseg`, grid edges) are right for local incidence and model CF. They are noisy and hard to compare across cities for **pattern + design** evidence.

Coarser units matter when:

- **National OD evidence** starts from township pairs and historically used **county centroids** for Baidu routematrix endpoints (`national_tidal_commuting_planB_20260426.md`).
- **Cross-city scatter** (congestion index vs asymmetry) needs a stable city- or corridor-comparable object.
- **Corridor design** is about continuous directed corridors, not isolated treated edges; segment scores should be rolled up to corridor / street / district packages before comparing to Shenzhen shp lanes.

Suggested hierarchy (already partly used in Shenzhen multilevel bridge):

```text
segment / grid-edge  →  corridor (动线)  →  district/county OD  →  city summary
```

Use the finest unit that still maps cleanly onto an implementable corridor. County is a **fallback for national OD endpoints and cross-city aggregation**, not a replacement for Shenzhen segment/corridor analysis.

---

## 3. Data already in hand

| Layer | Asset | Role in this idea |
|---|---|---|
| National city base 2024 | `national_tidal_commuting_baidu2024_top100_congestion_v1/` + `baidu_2024_city_congestion_base.csv` | City ranking / congestion index (x-axis candidate) |
| National city base 2025 | `national_tidal_commuting_baidu2025_top100_congestion_v1/` | Current preferred national crawl sample |
| National OD pairs | `.../data/top_commuting_pairs.csv` | Township (or county-centroid) bilateral directed times once crawl complete |
| Shenzhen existing tidal lanes | `raw_data/gis/深圳潮汐车道shp/` + `深圳市潮汐车道2.xlsx` | Ground-truth implemented corridors |
| Shenzhen Baidu speeds | `raw_data/speed_shenzhen_all_wgs84.csv` (week 2025-05-19–25) | Within-day / post-only directional speeds on treatment vs control |
| Shenzhen multi-tidal retained | `outputs/shenzhen_multi_tidal_post_only_v1/` | Corridor matching, controls, event-study-style within-day contrasts |
| Shenzhen grid OD costs | `shenzhen_gridlevel_*_v1/` | Middle layer: flow-weighted \(\tau\)-like asymmetry inside one city |
| Liuxian DID design | `Documents/L2_notes/shenzhen_liuxian_did_purchase_design_20260626.md` + gaode outputs | True pre/post opening design (opening 2026-03-23); Baidu week is pre-shock for Liuxian |
| Beijing model \(\tau\) / lanes | gate5 Stage2 + `tidal_lane_diagnostics_*` | Model \(\Delta\tau\) under tidal CF |
| Beijing corridor shortlist | `outputs/tidal_corridor_candidates_gate5_v1/corridor_candidates_ranked.csv` | Design-facing ranking (asym, congestion, lane slack, OD align) |
| Caitian video | `flowvedio/...` + identification plan | Local flow/speed response around switch times (disciplines local cost response, not citywide \(\tau\)) |

**Hard limits (do not overclaim).**

- Shenzhen Baidu week is **post-only** for older lanes; Liuxian is still **pre-opening** in that week.
- National crawl may be incomplete; city–asymmetry scatter was still open as of 2026-07-09 memo.
- Video pilot is local measurement design, not a citywide \(\tau\) estimator.

---

## 4. Concrete work plan with current data

Do this in three small packages. Each package has a deliverable that can stand alone.

### Package A — Shenzhen: implemented corridors as ground truth (highest priority)

**Goal.** Ask whether measured directional cost asymmetry / active-window speed gaps line up with **already built** tidal corridors, and how large a within-day cost-like response looks.

**Steps.**

1. Lock treatment corridors from shp + Tianyu table (already done in multi-tidal post-only): 文锦北 / 彩田 / 新洲 / 红岭 / 留仙（留仙 = pre-shock placebo）.
2. For each corridor–window, keep the retained length-weighted harmonic speed on `policy_like` vs `opposite_like`, treatment vs refined controls (`shenzhen_multi_tidal_post_only_v1`).
3. Define a **cost-response proxy** (not yet \(\tau_{ij}\)):
   - corridor time proxy \(m = L / v\) on each direction;
   - asymmetry \(m_{\mathrm{slow}}/m_{\mathrm{fast}}\) in active vs reference windows;
   - optional iceberg-style map \(t \propto m^{1/\theta}\) only as a sensitivity layer, with \(\theta=6.83\).
4. Produce one validation table:
   - rows = tidal corridor × peak window;
   - columns = active-window \(\Delta\log m\) (policy vs opposite), treatment−control contrast, control list, analysis length.
5. Overlay Shenzhen grid flow-weighted OD asymmetry near each corridor catchment (optional second panel): do implemented corridors sit in high flow-weighted asymmetry zones?

**Output folder (suggested):** `data_work/outputs/shenzhen_tidal_cost_response_validation_v1/`  
**Reuse:** post-only scripts + gridlevel spatial metrics; do not rebuild matching from scratch.

### Package B — National: 2024/2025 congestion × directed OD cost asymmetry

**Goal.** Finish the open city-level pattern: congestion index vs directed-cost asymmetry, with transparent spatial endpoints.

**Steps.**

1. Prefer **2025** crawl config for new runs; keep **2024** as the documented comparison city base.
2. After AM/PM(/NIGHT) matrices exist for top pairs:
   - pair-level asymmetry \(= \max(t_{od},t_{do})/\min(\cdot)\);
   - city summary = flow-weighted or top-pair mean/median asymmetry.
3. Scatter: x = `source_congestion_index`, y = city asymmetry summary (the figure agreed 2026-05-02).
4. **Aggregation rule:** report township-endpoint results as primary if parquet centers are used; report county-centroid robustness only if needed for coverage or historical plan-B comparability. Label endpoint type in the table.
5. Optional Shenzhen cell in the national cloud: same OD-asymmetry definition on Shenzhen top pairs, then annotate with “city has multiple implemented tidal corridors.”

**Output:** extend `national_tidal_commuting_baidu2025_top100_congestion_v1/metrics/` (or a sibling `..._asymmetry_scatter_v1/`).

### Package C — Beijing model: \(\Delta\tau\) + corridor design crosswalk

**Goal.** Connect structural CF cost changes to a design-facing corridor package, and state what Shenzhen validates.

**Steps.**

1. From mainline tidal diagnostics, export treated-edge \(\Delta\tau\) (ST/LT) and roll up to **corridor strings** (same physical undirected pair / named road where available).
2. Merge with `tidal_corridor_candidates_gate5_v1` scores (`score_st` / `score_lt`, lane slack, OD align).
3. Add a binary/flag column for “would resemble Shenzhen-style implementable corridor” using engineering filters already listed (length, fast-side lanes, no bus conflict placeholder).
4. Write a short memo paragraph for slides/paper:
   - Model delivers \(\Delta\tau\) and welfare under endogenous congestion;
   - Shenzhen packages A (and Liuxian when pre/post arrive) discipline the **local cost response** and **which corridors get built**;
   - National package B shows prevalence of directional cost asymmetry, not the causal \(\Delta\tau\).

**Do not** claim Beijing \(\Delta\tau\) is estimated from Shenzhen speeds. Claim **qualitative / magnitude discipline** and **selection validation**.

---

## 5. What would count as success

| Question | Minimal evidence |
|---|---|
| Is directional cost asymmetry real and common? | National scatter + Shenzhen grid flow-weighted shares |
| Do implemented tidal corridors look “high asymmetry / high need”? | Package A table: treatment corridors vs controls in active windows |
| How does lane switch change local cost-like outcomes? | Caitian switch windows + (later) Liuxian pre/post; video for flow, Baidu/Gaode for speed |
| Does model \(\Delta\tau\) point at designable corridors? | Package C rollup: high \(\Delta\tau\) / high score corridors pass lane-slack and geometry filters |

---

## 6. Suggested next action (smallest start)

Start with **Package A only**:

1. Read retained multi-tidal corridor summaries and event-study coefficients.
2. Build one CSV: corridor × window cost-proxy and treatment−control contrast.
3. One-page note: which Shenzhen corridors show a clear directional cost gap in the active window, and which do not (尤其留仙作为 pre-shock placebo).

Then decide whether to promote this into `nextstep.md` priority or keep it as a lower-priority evidence track.

---

## 7. Pointers

- National workflow: `data_work/docs/NATIONAL_TIDAL_COMMUTING_WORKFLOW.md`
- National plan B / county centroids: `Documents/L2_notes/national_tidal_commuting_planB_20260426.md`
- Shenzhen multi-tidal handoff: `data_work/docs/SHENZHEN_MULTI_TIDAL_BAIDU_HANDOFF_20260628.md`
- Shenzhen grid positioning: `Documents/L2_notes/shenzhen_gridlevel_flow_weighted_asymmetry_positioning_20260630.md`
- Beijing corridor scoring: `Documents/L2_notes/p1_edgeproj_qa_tidal_corridor_handoff_20260629.md`
- Liuxian DID purchase design: `Documents/L2_notes/shenzhen_liuxian_did_purchase_design_20260626.md`
- Meeting open item (national scatter): `Documents/L2_notes/meeting_discussion_memo_20260709.tex`
