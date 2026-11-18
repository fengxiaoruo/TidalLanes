# Data-pipeline versions, the v2 bug, and modelling conventions

**Date:** 2026-06-28 · Records the grid-travel-cost version situation, the raw_connected_v2
defect (to confirm with the kunpeng pipeline owner), and the agreed connected-component convention.

**UPDATE 2026-06-28:** Travel cost **locked (final)** to `model_input_edgeproj_v2_lcc_gate5_v1` (edgeproj_v2 + LCC + detour gate G=5; after fixing an index bug that had halved the LCC edges). See `Documents/L2_notes/travel_cost_lock_in_20260628.md`.

## Grid-travel-cost versions — which one the model uses, and how they differ

The current model input (`outputs/manual_centerline_rules_v11/data/qsm_input_edges_square.parquet`,
`tau_obs_source = stage06_t_edges_AM`) uses the **centerline-grid (stage06)** travel cost — NOT
the raw_connected / kunpeng raw-grid versions. The raw versions were parallel experiments and were
never wired into the qsm_input.

The three constructions are **very different** (square, AM):

| version | mean grid travel time | median directional ratio (asymmetry) | corr vs centerline |
|---|---:|---:|---:|
| **centerline / stage06 (CURRENT MODEL)** | **5.2 min** | **0.928** (mild) | — |
| raw_node | 12.9 min | 0.938 | 0.21 |
| **raw_edgeproj (= raw_connected v1 / kunpeng)** | **14.9 min** | **0.793** (strong) | 0.23 |

So centerline vs v1 are NOT interchangeable: v1 has ~3× the travel time, only ~0.23 correlation
with centerline, and much stronger directional asymmetry (0.793 vs 0.928). The draft's headline
asymmetry 0.793 comes from raw_edgeproj (v1), while the model runs on centerline (0.928) — an
internal inconsistency to reconcile. raw_edgeproj's stronger asymmetry may be partly real
directional congestion and partly long-detour artifacts (topology gaps) — needs separate check.

## raw_connected_v2 defect — SPATIAL CLIP (confirm with kunpeng before fixing)

**Symptom:** v2 produces only 609 valid square-AM grid edges (vs v1's ~3493), making it unusable.

**Root cause (diagnosed 2026-06-28):** v2 is NOT "v1 + connectivity enhancement." Its source
graph (`src/raw_connected_v2/Processed_Data/directed_edges.xlsx`, 9,888 edges, same before and
after normalize — so normalization is innocent) is a **spatial clip of central Beijing only**:

| | lon range | lat range | scale |
|---|---|---|---|
| v1 (kunpeng, complete) | 115.43–117.43 | 39.48–41.06 | full municipality |
| **v2 (raw_connected)** | **116.10–116.64** | **39.81–40.16** | **central core only (~46×40 km)** |

Original Baidu network ≈ **24,763** segments; v1 kunpeng ≈ 23,730 (≈ complete); v2 ≈ 9,888 (~40%).
Road overlap: v2 shares 8,980 of v1's roads, has 861 v2-only (manual adds), and is **missing
14,749 of v1's roads — all in the outer area outside the central clip**. Node id space is shared.

**Interpretation:** the manual-editing workspace (`Processed_Data`, with `manual_edge_ops.jsonl`,
`manual_edge_registry` (336 manual adds), `split_undo_stack`) only ever loaded the **clipped
central region**; the un-clipped outer roads never entered it. The 336 manual edits were applied on
top of this partial central base, not on the full network. Because the model's grid spans the full
municipality, outer adjacent-grid pairs have no v2 path → 609 valid edges.

**STATUS: TO CONFIRM WITH KUNPENG, then fix.** Two fix paths depending on study area:
- Full Beijing: re-apply `manual_edge_ops.jsonl` (336 ops) on top of v1's complete kunpeng graph.
- Central-city study: v2 is complete for the central area; instead restrict the grid system to the
  v2 clip extent. (Tidal-lane / jobs-housing phenomena are concentrated in the core, so a clean
  central-city scope may be preferable — pending OD/speed completeness in that extent.)

## CONVENTION (agreed 2026-06-28): restrict the model to the largest connected component

The node table has 2,736 cells but spans 962 components; only the largest (~1,774–1,776 cells,
the cells appearing in edges) is road-connected. The other ~960 cells are isolated (no edges,
unreachable, contribute 0 to welfare). **Going forward, the model is explicitly restricted to the
largest connected component.** All subsequent work uses this scope, and key model versions are
re-run under it.

## Data time points (known mismatches)

| object | time |
|---|---|
| Beijing OD commuting flow (`commute_202211.csv`) | **2022-11** |
| Beijing road speeds (`speed_Beijing_all_wgs84.csv`) | **2025-05** |
| National township commuting (`commuting_town_2021.dta`) | 2021 |
| Shenzhen commuting (`commute_shenzhen_202411`) | 2024-11 |

The Beijing model combines 2022-11 commuting with 2025-05 speeds (~2.5-year gap) — a caveat to
disclose. The OD is a home→work commuting matrix (not a peak-hour count), road-mode commuters only.
