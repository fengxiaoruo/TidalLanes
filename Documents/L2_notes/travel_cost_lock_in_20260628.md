# Travel-cost lock-in decision (2026-06-28)

## Locked choice (final for v1 data)

`**model_input_edgeproj_v2_lcc_gate5_v1**` is the final travel-cost matrix for the v1 data: raw_connected v1 **edgeproj_v2** pair-specific routed travel costs, restricted to the **largest connected component (LCC)**, with a **detour gate at G = 5** (adjacent edges whose routed length exceeds five times the straight-line centroid distance are dropped before the LCC is taken).

All structural model runs use `--version-id model_input_edgeproj_v2_lcc_gate5_v1` unless a run is explicitly labelled a robustness check. Downstream inputs are built from this version.

This supersedes the earlier `model_input_edgeproj_v2_lcc_v1` (no-gate) selection. The no-gate bundle is retained as a robustness comparison; the centerline heuristic is retained only as a coarse robustness baseline.

## Two corrections behind the final lock-in

1. **Index bug in the bundle builder.** `_restrict_lcc` previously filtered edges by `edges.i in nodes.node_i`. Those two integer index spaces are unrelated in the base QSM (`edges.i/j` carry the stage06 t-edge order, 12 to 3356; `nodes.node_i` runs 0 to 2735), so the filter dropped roughly half the LCC edges and mislinked the rest to the wrong grid. The fix joins on `grid_id` only and rebuilds one fresh contiguous node index shared by nodes, edges, and OD. Edge count rose from 1462 to 3198, isolated nodes fell from 588 to 0, and `edges.i` now resolves to the correct grid for 100% of rows.
2. **Detour gate.** With the corrected bundle, a long-detour tail remained: adjacent grid pairs separated by about 2.3 km but routed 80 to 90 km because a barrier (river, expressway, network gap) forced the pair-specific shortest path far around. The gate removes this tail.

## Detour gate: rationale and cost

For least-cost route choice the artifacts are largely self-correcting, since no traveller selects an 80 km path between adjacent cells; the equilibrium routes around them multi-hop. The gate matters for the edge-level directional-asymmetry signal, which is read off single edges and is the descriptive object the project relies on. A one-way barrier detour registers as extreme asymmetry that is mechanical, not tidal congestion.


| metric                                | no gate        | gate G = 5     |
| ------------------------------------- | -------------- | -------------- |
| nodes (LCC grids)                     | 1153           | 1104           |
| edges                                 | 3198           | 2642           |
| OD pairs                              | 330,497        | 310,525        |
| OD road commuters (city share)        | 91,134 (95.8%) | 88,110 (92.6%) |
| resident share in scope               | 93.7%          | 91.8%          |
| edge tau median (min)                 | 9.64           | 8.71           |
| edge tau p99 (min)                    | 100.1          | 29.5           |
| edge tau max (min)                    | 254.2          | 52.3           |
| median directional ratio              | 0.793          | 0.811          |
| extreme asymmetry share (ratio < 0.5) | 15.6%          | 8.9%           |


The gate drops 523 pre-LCC edges and about 17% of LCC edges, but only about 4% of grids and 3% of road commuters, because dropped pairs stay connected multi-hop. It cuts the p99 edge time from 100 to 30 minutes and roughly halves the extreme-asymmetry share, a sign that a large part of the apparent strong asymmetry was detour artifact.

## Why not the alternatives


| Candidate                              | Verdict                                                                                                                                          |
| -------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| **centerline / stage06 (old default)** | Rejected as primary: no road routing (centroid distance over speed), weak asymmetry (median ratio 0.914), unfiltered graph spans 962 components. |
| **centerline_lcc**                     | Robustness only: higher OD coverage but heuristic costs.                                                                                         |
| **edgeproj_v2_lcc, no gate**           | Retained as robustness: correct routing but the long-detour tail inflates edge times and asymmetry.                                              |
| **raw_connected_v2 edgeproj**          | Rejected: central-Beijing spatial clip leaves few valid edges.                                                                                   |


## Geometry note

The city is Beijing (about 116.4E, 40.2N); detour distances and maps use UTM zone 50N (EPSG:32650). An earlier draft used zone 51N, which rotated the grid about 4.3 degrees visually through meridian convergence but changed the gated edge set by a single edge.

## Locked outputs and scripts


| Purpose                                   | Path                                                                              |
| ----------------------------------------- | --------------------------------------------------------------------------------- |
| QSM bundle (FINAL)                        | `data_work/outputs/model_input_edgeproj_v2_lcc_gate5_v1/data/`                    |
| QSM bundle (no-gate robustness)           | `data_work/outputs/model_input_edgeproj_v2_lcc_v1/data/`                          |
| Side-by-side comparison                   | `data_work/outputs/model_input_edgeproj_v2_lcc_gate5_v1/COMPARISON_vs_no_gate.md` |
| Bundle builder (now with `--detour-gate`) | `data_work/src/model/travel_cost_bundle/build_locked_bundle.py`                   |
| Descriptive-audit figures                 | `data_work/src/model/travel_cost_bundle/plot_descriptive_audit.py`                |


## Downstream status (action required)

The earlier model reruns (`directional_cost_edgeproj_v2_lcc_v1`, `full_estimation_edgeproj_v2_lcc_v1`, the Replication AA input, and Spec C) were produced on the pre-fix 1462-edge bundle and are invalid. They must be regenerated on `model_input_edgeproj_v2_lcc_gate5_v1`.

## Caveats (still open)

1. Higher tau levels than the heuristic are expected (connector plus routing); use observed-anchored congestion in Stage 2.
2. `raw_connected_v2` fix still pending confirmation.
3. OD (2022-11) versus speeds (2025-05) time mismatch unchanged.

## Rebuild command

```bash
cd data_work
python -m src.model.travel_cost_bundle.build_locked_bundle \
  --cost-version edgeproj_v2_lcc --detour-gate 5 \
  --output-version-id model_input_edgeproj_v2_lcc_gate5_v1
python -m src.model.travel_cost_bundle.plot_descriptive_audit \
  --version-id model_input_edgeproj_v2_lcc_gate5_v1
```

