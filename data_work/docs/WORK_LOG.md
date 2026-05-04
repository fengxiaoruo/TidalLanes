# Work Log

This file records completed work, locked decisions, and retained outputs. `nextstep.md` is the live plan; this file is the running log.

## 2026-05-02: collaborator meeting notes recorded and live plan reset

- Added meeting notes at `Documents/L2_notes/meeting_notes_20260502.md`.
- Synced documents discussed in the meeting:
  - `Documents/L1_manuscripts/TidalLanes_0429.tex`
  - `Documents/L1_manuscripts/MODEL_replication_tidallanes.tex`
  - `Documents/L1_manuscripts/TidalLanes_Model_v4.tex`
- Retained decisions and open points:
  - missing or one-direction-only figure segments should be plotted in light gray;
  - grid-level travel-cost construction remains unsettled;
  - the national evidence should add a city-level scatter with congestion index on the x-axis and asymmetry on the y-axis, with additional crawl data if useful;
  - parameter choices should be cross-checked against Professor You Wei's related work.
- Updated `nextstep.md` so the live plan now prioritizes the full-estimation framework, pattern evidence, national scatter figure, Shenzhen reduced-form feasibility, and later parameter/data-detail refinement.

## 2026-05-02: national crawl city list switched to top-100 congestion-source cities

- Added a congestion-source city-selection branch to `data_work/src/national_tidal_commuting/town_2021_top_pairs.py`.
- New config:
  - `data_work/config/national_tidal_commuting_top100_congestion_2023_v1.json`
- City-selection source:
  - `data_work/raw_data/20190909-15百城拥堵数据-20230616.xlsx`
- Ranking rule:
  - compute city-level `source_congestion_index = mean(畅通速度 / 平均速度)` over all rows in the Excel file;
  - sort cities by this index in descending order;
  - match source city names such as `北京` to DTA city names such as `北京市`;
  - keep the previous within-city pair rule unchanged: top 50 unordered township pairs plus up to 30 supplemental long-distance pairs.
- Validation build completed with:
  - output root `data_work/outputs/national_tidal_commuting_top100_congestion_2023_v1/`
  - `100` selected source cities
  - `7461` selected OD pairs in `data/top_commuting_pairs.csv`
  - `99` cities with at least one selected pair
  - `source_congestion_city_ranking.csv` written under `metrics/`
- Caveat:
  - the Excel file name includes `20230616`, but the table's `日期` field runs from `20190909` to `20190915`;
  - Sanya is in the selected source city list, but current town-center coverage leaves no exportable non-same-town pair after the existing cleaning rules.
- Updated `data_work/docs/NATIONAL_TIDAL_COMMUTING_WORKFLOW.md` with the new config, outputs, and date caveat.

## 2026-05-02: official Baidu 2024 100-city congestion base downloaded and extracted

- Downloaded the official Baidu Map report PDF:
  - `data_work/raw_data/baidu_2024_china_city_traffic_report.pdf`
- Added extraction script:
  - `data_work/src/national_tidal_commuting/extract_baidu_2024_city_base.py`
- Extracted and retained a local 100-city base:
  - `data_work/raw_data/baidu_2024_city_congestion_base.csv`
  - `data_work/raw_data/baidu_2024_city_congestion_base.xlsx`
  - `data_work/raw_data/baidu_2024_city_congestion_base_metadata.json`
- The city base includes:
  - 2024 commute-peak congestion rank and index from attachment 1;
  - 2024 weekend congestion rank and index from attachment 2;
  - 2024 average commute duration and distance from attachment 3.
- Added config:
  - `data_work/config/national_tidal_commuting_baidu2024_top100_congestion_v1.json`
- Updated `town_2021_top_pairs.py` so the city-selection branch can read either an Excel speed table or a CSV with an already-defined congestion-index column.
- Validation build completed:
  - output root `data_work/outputs/national_tidal_commuting_baidu2024_top100_congestion_v1/`
  - `100` selected source cities
  - `7461` selected OD pairs
  - `99` cities with at least one selected pair
  - Sanya remains zero-pair under current town-coordinate coverage and existing cleaning rules.
- This 2024 official Baidu city base should supersede the older local Excel file for the next national crawl unless a deliberate historical comparison is needed.

## 2026-04-29: future Baidu crawl variants should retain `tactics=0`, `tactics=2`, and `tactics=3`

- Retained decision for the next national commuting crawl code revision: support collecting `directionlite/v1/driving` outputs for the same OD pairs and real clock windows under all three route strategies.
- Interpretation to preserve in documentation and output metadata:
  - `tactics=0`: regular/default fast route strategy; useful as a common-strategy baseline.
  - `tactics=2`: congestion-avoidance strategy; closest to the intended object of a real-time congestion-adjusted recommended fastest route.
  - `tactics=3`: shortest-distance strategy; useful as a route-choice robustness check.
- The main paper specification can later choose between `tactics=0` and `tactics=2`; the other strategy should be retained as a robustness version, with `tactics=3` used to diagnose whether route selection rather than directional congestion drives the asymmetry.
- Also recorded in `data_work/docs/NATIONAL_TIDAL_COMMUTING_WORKFLOW.md` under the Baidu crawl section so this decision is visible when modifying crawl code.

## 2026-04-27: direction-agnostic AM asymmetry assets added for national top-pair evidence

- Added `data_work/src/national_tidal_commuting/generate_bidirectional_asymmetry_assets.py`.
- This asset builder intentionally does **not** rely on a pre-labeled `home/work` commuting direction when measuring within-pair asymmetry.
- For each pair and time window, it now constructs:
  - `slow_sec = max(t1, t2)`
  - `fast_sec = min(t1, t2)`
  - `asym_ratio = slow_sec / fast_sec`
  - `symmetry_ratio = fast_sec / slow_sec`
  - `abs_log_ratio = |log(asym_ratio)|`
- `NIGHT` remains the low-congestion comparison baseline; the new outputs therefore focus on:
  - same-time bidirectional asymmetry in `AM`
  - how much that asymmetry exceeds the nighttime baseline
- The new analysis keeps the three sampling scopes separate:
  - `top50`
  - `longdist30`
  - `union80`
- It also exports `longdist30_only` as a technical supplement so overlap between `top50` and the long-distance pool remains transparent.
- Current overlap structure in `national_tidal_commuting_top50cities_2021_v1`:
  - `top50 = 2500`
  - `longdist30 = 1500`
  - overlap `= 264`
  - `longdist30_only = 1236`
- Retained outputs:
  - `data_work/outputs/national_tidal_commuting_top50cities_2021_v1/metrics/am_bidirectional_asymmetry_national_summary.csv`
  - `data_work/outputs/national_tidal_commuting_top50cities_2021_v1/metrics/am_bidirectional_asymmetry_city_summary.csv`
  - `data_work/outputs/national_tidal_commuting_top50cities_2021_v1/metrics/am_bidirectional_asymmetry_pair_subset.csv`
  - `data_work/outputs/national_tidal_commuting_top50cities_2021_v1/figures/bidirectional_asymmetry_assets/`

## 2026-04-27: AM-to-NIGHT aligned asymmetry comparison added

- The bidirectional asymmetry asset builder now also computes a night-adjusted comparison that keeps the `AM` direction ordering fixed.
- For each pair:
  - if `AM` has `ab <= ba`, then compare `ab/ba` in `AM` against `ab/ba` in `NIGHT`
  - if `AM` has `ba < ab`, then compare `ba/ab` in `AM` against `ba/ab` in `NIGHT`
- Exported fields now include:
  - `night_ratio_aligned_to_am`
  - `am_over_night_aligned_ratio`
  - `night_over_am_aligned_ratio`
  - `aligned_ratio_gap`
  - indicators for whether `AM` is more asymmetric than `NIGHT`
- Interpretation:
  - `am_over_night_aligned_ratio < 1` means `AM` is more asymmetric than the same pair's nighttime directional baseline
  - `night_over_am_aligned_ratio > 1` is the inverse presentation of the same idea
- This allows the paper/slides version to emphasize excess morning asymmetry relative to nighttime rather than raw same-time asymmetry alone.

## 2026-04-27: `AM` analysis baseline switched from the late session to the complete 07:08-08:14 session

- The later `AM` crawl session `20260427T085411` was dropped after direct comparison showed materially weaker asymmetry than the earlier complete session.
- Retained `AM` session:
  - `20260427T070801`
  - time span `2026-04-27 07:08:01` to `08:14:32` (Asia/Shanghai)
  - `7472 / 7472` successful directional requests
- Deleted session:
  - `20260427T085411`
  - time span `2026-04-27 08:54:11` to `10:00:51`
  - `7470 / 7472` successful directional requests
- Comparison on the full `3736`-pair sample showed the earlier session is clearly stronger:
  - raw `AM min/max` median moved from about `0.927` to `0.905`
  - share with `AM min/max <= 0.90` rose from about `41.6%` to `51.6%`
  - median aligned `AM/NIGHT` ratio moved from about `0.954` to `0.919`
  - share with `AM` more asymmetric than `NIGHT` rose from about `69.8%` to `76.8%`
- After dropping the late session, `summarize_results.py`, `generate_am_asymmetry_assets.py`, `generate_bidirectional_asymmetry_assets.py`, and the ad hoc raw-histogram assets were all regenerated so current outputs now consistently use the earlier `AM` round.

## 2026-04-28: top-20 congestion-city note generated for AM, PM, and the later overnight baseline

- Added `src/national_tidal_commuting/generate_top20_congestion_note.py`.
- This note fixes the analysis sample to:
  - the top 20 cities ranked by weighted mean AM excess gap relative to the later overnight baseline
  - the top 50 commuting pairs within each selected city
- Retained time windows:
  - `AM`: session `20260427T070801`, `07:08:01--08:14:32`
  - `PM`: session `20260427T171554`, `17:15:54--18:26:28`
  - later overnight baseline: session `20260427T003434`, `00:34:35--01:48:51`
  - for that later overnight session, two failed directional rows are backfilled from the earlier complete night run `20260426T222257`
- New retained outputs:
  - metrics: `data_work/outputs/national_tidal_commuting_top50cities_2021_v1/metrics/top20_congestion_top50_note/`
  - figures: `data_work/outputs/national_tidal_commuting_top50cities_2021_v1/figures/top20_congestion_top50_note/`
  - TeX note: `Documents/L4_pipeline_outputs/national_tidal_commuting_notes/national_tidal_commuting_top20_congestion_note_20260428.tex`
  - compiled PDF: `Documents/L4_pipeline_outputs/national_tidal_commuting_notes/national_tidal_commuting_top20_congestion_note_20260428.pdf`

## 2026-04-28: `centerline_grid_v11` square baseline run completed for model/data alignment line

### Purpose

- Execute the pending full baseline run on the current retained `centerline_grid_v11` input line and record convergence / data-quality diagnostics before deciding whether to continue on centerline inputs or switch effort to `raw-grid`.

### Run command

```bash
python3 data_work/src/model/run_spatial_equilibrium.py \
  --version-id manual_centerline_rules_v11 \
  --grid-type square \
  --model-output-subdir model_square_baseline_centerline_v11
```

### Retained outputs

- `data_work/outputs/manual_centerline_rules_v11/model_square_baseline_centerline_v11/equilibrium_summary.csv`
- `data_work/outputs/manual_centerline_rules_v11/model_square_baseline_centerline_v11/calibration_summary.json`
- `data_work/outputs/manual_centerline_rules_v11/model_square_baseline_centerline_v11/edge_counterfactual_results.csv`

### Key diagnostics

- Baseline converged in `36` iterations (`solver_max_log_change = 9.31e-07`).
- `edge_tau_ff_imputed_share = 0.0`.
- `edge_missing_lane_share = 0.0`.
- Edge result export now contains explicit minutes and iceberg fields in parallel:
  - `tau_obs_min`, `tau_ff_min`
  - `t_obs_iceberg`, `t_ff_iceberg`
  - `t_baseline_eq_min`, `t_baseline_eq_iceberg`

### Calibration diagnostics retained

- `theta_diagnostic.theta_hat = 0.7363` (`n_obs = 3049`), while `theta_used = 6.83`.
- `lambda_diagnostic.lambda_hat = 0.01` (`n_edges = 4087`), while `lambda_used = 0.15`.
- These diagnostics remain calibration references only; the run still uses external defaults.

## 2026-04-28: path-based directional lane estimate added to `raw_edgeproj_v2` grid-cost outputs

### Purpose

- Add a second lane-estimation version that follows the realized pair-specific shortest path under `raw_connected_v1_grid_travel_time_edgeproj_v2`, and keep it separate from the existing centerline-derived grid-grid lane aggregation.

### Code update

- `data_work/src/raw_connected_v1/grid_travel_time/build_raw_connected_v1_grid_travel_time_edgeproj.py`
  - pair-specific route tracing now also exports:
    - `lane_est_path_raw_edgeproj_len_weighted`
    - `lane_est_path_raw_edgeproj_bottleneck`
  - values are computed along each directed shortest path (`home_grid -> work_grid`) using path segments on the raw road graph.
  - run metadata now records:
    - `path_lane_source`
    - `path_lane_fallback`

### Outputs refreshed in-place (no new version directory)

- `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/data/raw_connected_vs_old_adjacent_adjacent_{grid}_{period}.csv`
- refreshed for all three grid systems and both periods:
  - `square`, `hex`, `voronoi`
  - `AM`, `PM`

### Current retained caveat

- The current raw road graph does not carry direct lane fields and lacks a fully explicit key bridge to the stage lane table IDs.
- Under this constraint, path-lane values currently rely heavily on fallback lane proxy assignment; this produces valid path-based columns but weak cross-edge lane heterogeneity.

## 2026-04-28: AA-style path traffic proxy run saved as separate `raw_edgeproj_v2_traffic` output

### Purpose

- Implement a separate, non-overwriting version of the `raw_edgeproj_v2` grid-cost outputs that adds an AA-style path traffic proxy for model input construction.
- Keep the original `raw_connected_v1_grid_travel_time_edgeproj_v2` outputs untouched.

### Code and output separation

- New script:
  - `data_work/src/raw_connected_v1/grid_travel_time/build_raw_connected_v1_grid_travel_time_edgeproj_traffic.py`
- Separate output root:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2_traffic/`

### Added fields

- `path_total_road_length_m`
- `path_total_lanemiles_m_lane`
- `path_avg_aadt_proxy_len_weighted`

The last field is the AA-style path traffic proxy:

```text
path_avg_aadt_proxy_len_weighted = sum(aadt_proxy_edge * road_length_edge) / sum(road_length_edge)
```

where `aadt_proxy_edge` is obtained from a BPR-style inversion using observed travel time, free-flow travel time, lane proxy, and the default parameters recorded in `run_config.json`.

### Run completed

- Refreshed in the separate output root for:
  - `square`, `hex`, `voronoi`
  - `AM`, `PM`
- Retained files:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2_traffic/data/raw_connected_vs_old_adjacent_adjacent_square_AM.csv`
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2_traffic/data/raw_connected_vs_old_adjacent_adjacent_square_PM.csv`
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2_traffic/data/raw_connected_vs_old_adjacent_adjacent_hex_AM.csv`
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2_traffic/data/raw_connected_vs_old_adjacent_adjacent_hex_PM.csv`
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2_traffic/data/raw_connected_vs_old_adjacent_adjacent_voronoi_AM.csv`
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2_traffic/data/raw_connected_vs_old_adjacent_adjacent_voronoi_PM.csv`

### Quick validation

- Non-missing traffic proxy coverage:
  - square: `79.1%` in AM, `79.1%` in PM
  - hex: `83.6%` in AM, `83.6%` in PM
  - voronoi: `85.8%` in AM, `85.8%` in PM
- Mean `path_avg_aadt_proxy_len_weighted`:
  - square: `44,956` AM, `48,988` PM
  - hex: `45,478` AM, `49,982` PM
  - voronoi: `48,175` AM, `53,186` PM

## 2026-04-26: national tidal commuting AK source logic hardened for local smoke tests

- Unified Baidu AK loading into `src/national_tidal_commuting/common.py` so `run_full_crawl.py` and `query_baidu_routematrix.py` now share the same behavior.
- Added `--ak-source {auto,local,env}` to both entry points.
- New default is `auto`: prefer local `src/national_tidal_commuting/ak_local.py`, then fall back to environment variable `BAIDU_MAP_AUTH_TOKEN`.
- When both sources exist, the script now prints which source it chose and warns if the environment variable looks suspicious (for example an unrelated `sk-...` token).
- Updated `scripts/run_baidu_travel_panels.sh` so it no longer hard-fails when the environment variable is absent; it now passes through the same AK-selection logic as the Python entry points.
- Updated `docs/NATIONAL_TIDAL_COMMUTING_WORKFLOW.md` to document the new default and the explicit override `--ak-source env`.

## 2026-04-26: national tidal commuting request layer switched to Baidu `directionlite/v1/driving`

- Replaced the live-query layer from `routematrix/v2/driving` to single-OD `directionlite/v1/driving` so the current local AK can run successfully.
- `query_baidu_routematrix.py` and `run_full_crawl.py` now send one request per OD-direction row instead of batching multiple destinations into one matrix call.
- The output CSV / JSONL / progress file structure was intentionally kept stable so downstream inspection remains easy.
- Current panel mapping under `directionlite/v1/driving`:
  - `FREE -> tactics=0`
  - `AM/PM -> tactics=2`
  - `NIGHT -> tactics=0`
- Important retained caveat: under `directionlite/v1/driving`, `FREE` is only a lightweight baseline and is not the same estimand as the old `routematrix/v2` no-traffic baseline.
- Live smoke test completed successfully on `top2cities` with status `0` throughout:
  - `data_work/outputs/national_tidal_commuting_top2cities_2021_test/api/crawl_sessions/20260426T204833/`

## 2026-04-26: stricter live-time option added for AM / PM / NIGHT runs

- Added `--strict-live-window` to `run_full_crawl.py` and `query_baidu_routematrix.py`.
- In this mode, the workflow is intentionally stricter:
  - only one panel per run
  - `FREE` is disallowed
  - the local Shanghai clock must fall inside a narrower live window
- Current strict windows:
  - `AM`: `07:00–09:00`
  - `PM`: `17:00–19:30`
  - `NIGHT`: `22:00–次日01:00`
- This mode is meant for users who prefer to treat the third run as a real low-congestion nighttime observation rather than a same-time synthetic baseline.
- `summarize_results.py` was also updated to prefer `NIGHT` over legacy `FREE` when constructing the low-congestion comparison baseline.

## 2026-04-26: top-pair construction tightened and expanded for town-level commuting

- Town-level pair construction now drops same-township flows before unordered pair ranking.
- It also drops residual zero-distance pairs caused by different town codes sharing the same representative point.
- For each city, the retained list is now:
  - base `top_n_pairs = 50` by commuter volume
  - plus a supplemental long-distance set of up to `30` pairs ranked by commuter volume among pairs with `euclid_dist_km >= city_radius_est_km / 3`
  - final export is the union, so each city has `<= 80` pairs
- City radius is estimated from the convex-hull area of unique township representative points and converted by `r = sqrt(area / pi)`.
- Rebuilt `national_tidal_commuting_top50cities_2021_v1` outputs now contain `3736` pairs in total.
- After this rebuild:
  - exact same-point pairs in the exported sample fell from `1670` to `0`
  - median pair distance increased to about `9.14 km`
  - `49` of `50` cities now export more than `50` pairs, and `16` cities reach the full `80`

## Workflow rule

- `nextstep.md` only keeps the current status, the immediate next action, and short plan bullets.
- Completed work, locked parameters, historical comparisons, and retained decisions are recorded here.
- When a task is finished, remove it from `nextstep.md` and append the result here instead of leaving stale history in the live plan.

## 2026-04-26: AA commuting replication adaptation note added for speed-input version

- Added `Documents/L2_notes/AA_commuting_replication_adaptation_notes_20260426.md`.
- The note records how to adapt the AA Seattle commuting replication package to this project's data conditions while keeping the original traffic / cost / exact-hat structure as intact as possible.
- Locked high-level implementation principle:
  - input layer uses directed speed, free-flow speed, lane count, and a calibrated speed-flow mapping to recover edge traffic state;
  - model core continues to use the AA commuting inversion, baseline equilibrium, and exact-hat logic;
  - output layer maps counterfactual edge states back to time and speed.
- Updated `nextstep.md` so future model `tex` revisions and estimation-code preparation explicitly refer back to this note before changing the solver or data interface.

## 2026-04-26: national top-OD tidal-commuting trial toolkit created

### Scope

- Added a standalone toolkit for national motivation evidence based on high-flow commuting pairs rather than random within-city OD sampling.
- The toolkit is intentionally separate from the retained Beijing stage pipeline and is designed to scale to a 20-city trial once additional commuting files are available.

### New code and config

- New scripts under `data_work/src/national_tidal_commuting/`:
  - `build_top_pairs.py`
  - `query_baidu_routematrix.py`
  - `summarize_results.py`
- New sample config:
  - `data_work/config/national_tidal_commuting_planb_trial_v1.json`
- New implementation memo:
  - `Documents/L2_notes/national_tidal_commuting_planB_20260426.md`

### Locked implementation choices (updated)

- Main sample design follows plan B: top **undirected** pairs inside each city, not random spatial sampling.
- Pair selection: aggregate directed → collapse to unordered; rank by \((\text{OD}+\text{DO})/2 = (pop\_{ab}+pop\_{ba})/2\); take top `n` per city. **No** city-core filter, no distance band, no near-duplicate drop (those rules were removed from code).
- Canonical `home`/`work` in each row: the direction with the larger of the two directed volumes (`pop_ab >= pop_ba` after fold).

### API design

- The main live-query script uses Baidu `routematrix/v2/driving`.
- Under ordinary permissions, the implementation does **not** fake future departure times.
- Instead, the workflow is split into three separate live runs:
  - `AM`
  - `PM`
  - `NIGHT`
- Each run logs:
  - `query_window`
  - `query_date`
  - `query_time_local`
  - `request_batch_id`
  - raw JSON responses
  - flat per-pair result rows

### Current data coverage

- The repo currently contains directly reusable commuting inputs for:
  - Beijing: `data_work/raw_data/commute_202211.csv`
  - Shenzhen: `data_work/raw_data/commute_shenzhen_202411_for_stage07.csv`
- The sample config is therefore a two-city trial config.
- No 20-city raw commuting bundle is currently stored in this repo, so scaling beyond Beijing and Shenzhen only requires config expansion after new files are added.

## 2026-04-26: national 2021 town matrix + 2019 county map (50 cities × 50 undirected pairs)

- Primary inputs: `commuting_town_2021.dta` and `县（等积投影）.shp` (2019 审图号 bundle); `source: "town_2021"` in config.
- **50 cities** by intra-metro `commuters_2021` sum; for each, **up to 50** undirected town pairs ranked by **(OD+DO)/2**; no `compute_city_core` / no distance or dedup filters in code (avoids the former `invalid_city_core` empty-city case for small geographies like Zhongshan).
- Output target size **2500** rows when every city has at least 50 unique undirected pairs; otherwise `sample_shortfall_flag` on the row block for that city.
- Implementation memo: `Documents/L2_notes/national_tidal_commuting_planB_20260426.md`.
- `resolve_path` supports `data_work/...` from repo-relative paths; run `build_top_pairs` from the `data_work/` directory.

## 2026-04-20: raw-centerline baseline locked at `manual_centerline_rules_v11`

### Matching status

- Retained final version: `data_work/outputs/manual_centerline_rules_v11`
- Retained strategy: `plan1 / v5a`
- Discarded strategy: `fullskeleton_v1`
- `manual_centerline_groups.json` expanded through the latest retained manual additions

### Locked construction logic

- Manual raw edges are excluded from the default skeleton input.
- Manual centerlines are built separately from grouped raw corridors.
- Manual corridors generate exact `raw_edge_id -> cline_id` crosswalks.
- `stage02` uses exact matching for manual raw edges and baseline distance-cap matching for the rest.
- Current stage01 execution path is `5m` skeleton with tiling.

### Locked parameters

- Stage01 config: `data_work/config/manual_centerline_plan1.json`
- `manual_exclude_from_skeleton_input = true`
- `manual_replace_default_centerline = false`
- `BUF_WIDTH = 50.0`
- `RES = 5.0`
- `MIN_CL_LEN = 50.0`
- `SKELETON_MAX_RASTER_MPIX = 600`
- `manual_midline_step = 50.0`
- `manual_simplify_tol = 10.0`
- `manual_connect_snap_m = 35.0`
- `manual_pair_max_dist_m = 120.0`
- `min_seg_len_baseline = 50.0`
- `cut_buf = 40.0`
- `snap_tol = 30.0`
- `min_seg_gap = 5.0`
- `split_search_dist = 80.0`
- `node_snap_tol = 35.0`
- `node_buffer = 40.0`
- `merge_cut_dist = 30.0`
- `min_split_seg_len = 120.0`
- `dist_cap = 180.0`

### Key stage01-02 results

- `manual group = 129`
- `manual exact raw edges = 963`
- `manual centerline fragments = 310`
- `split_match_rate = 0.994243`
- `raw_edge_match_rate = 0.994242`
- `matched_split_segments = 23,662`
- `unmatched_split_segments = 137`
- `matched_raw_edges = 23,672`
- `unmatched_raw_edges = 1,091`
- unmatched raw length share `= 1.27%`
- unmatched raw-edge share `= 4.41%`

### Interpretation of remaining unmatched segments

- The remaining unmatched share is small in total length and concentrated in a narrow long-corridor tail.
- Short unmatched fragments are numerous but account for little total length.
- Further manual work should focus on a short list of long and geometrically complex corridors rather than network-wide cleanup.

### Output retention

- Retained:
  - `data_work/outputs/manual_centerline_rules_v11`
  - `data_work/outputs/comparison/`
- Non-final raw-centerline intermediate versions were removed from `data_work/outputs/`

## 2026-04-20: paper and documentation refresh completed

### Documentation updates

- `data_work/README.md` updated to reflect the retained raw-centerline workflow
- `data_work/docs/CODEX_HANDOFF_NOTE.md` updated to point new sessions to `manual_centerline_rules_v11`
- `data_work/outputs/comparison/raw_centerline_match_version_summary.md` updated to summarize retained versions and decisions

### TeX and figure updates

- `Documents/TidalLanes_0320.tex` Section 3 updated around the retained raw-centerline workflow
- Image path issues were fixed and the file compiles successfully
- Section 3 tables were restored to mostly default layout without unnecessary `resizebox`
- Matching diagnostics now emphasize high first-step coverage and the concentration of residual unmatched corridors

### Current appendix-facing summary stats already in use

- Baseline-kept network summary:
  - raw: `23,795`
  - raw_split: `23,799`
  - undirected_centerline: `5,945`
  - directed_centerline: `11,890`
- Manual exact-match corridors:
  - `963` raw edges
  - `3,846.0 km`
  - count share `3.9%`
- Directed centerline coverage:
  - all directions covered share `0.7059`
- Undirected coverage:
  - both directions covered share `0.6219`
  - one direction covered share `0.1680`
  - none covered share `0.2101`

## 2026-04-20: stage03-04 completed on top of `manual_centerline_rules_v11`

### Run command

```bash
python3 data_work/src/stages/run_full_pipeline.py \
  --config data_work/config/manual_centerline_plan1.json \
  --version-id manual_centerline_rules_v11 \
  --output-dir data_work/outputs \
  --from-stage stage03 --to-stage stage04
```

### Main outputs

- `data_work/outputs/manual_centerline_rules_v11/data/centerline_speed_master.parquet`
- `data_work/outputs/manual_centerline_rules_v11/data/centerline_asymmetry_table.parquet`
- `data_work/outputs/manual_centerline_rules_v11/data/centerline_asymmetry_summary.csv`
- `data_work/outputs/manual_centerline_rules_v11/data/centerline_asymmetry_am_pm_comparison.csv`
- `data_work/outputs/manual_centerline_rules_v11/data/centerline_tidal_lane_candidates.csv`
- `data_work/outputs/manual_centerline_rules_v11/data/centerline_lane_master.parquet`
- `data_work/outputs/manual_centerline_rules_v11/metrics/stage03_speed_summary.csv`
- `data_work/outputs/manual_centerline_rules_v11/metrics/stage03b_asymmetry_summary.csv`
- `data_work/outputs/manual_centerline_rules_v11/metrics/stage04_lane_summary.csv`
- `data_work/outputs/manual_centerline_rules_v11/figures/hist_speed_ratio_am_pm.png`

### Stage03 speed attachment

- loaded speed observations: `24,336,848`
- matched speed observations: `23,286,622`
- centerline-time rows: `394,468`
- directed centerlines with speed: `8,349`
- mean speed: `36.30 km/h`
- median speed: `33.33 km/h`
- period means:
  - `AM = 35.19 km/h`
  - `PM = 33.33 km/h`
  - `FF_2205 = 38.46 km/h`

### Stage03b asymmetry

- asymmetry table rows: `9,315`
- centerlines with both directions observed:
  - `AM = 3,670`
  - `PM = 3,669`
- `unsym1` share among both-direction centerlines:
  - `AM = 11.04%`
  - `PM = 9.84%`
- unique centerlines flagged as `unsym1` in at least one peak: `638`
- overlap:
  - `AM only = 277`
  - `PM only = 233`
  - `Both AM and PM = 128`
- reversed faster direction count: `1,032`
- strict tidal-lane candidates: `14`

### Stage04 lane attachment

- total directed rows in `centerline_lane_master`: `45,752`
- rows with valid lane estimate: `8,400`
- mean lane estimate: `3.13`
- median lane estimate: `3.32`
- mean opposite-direction lane ratio: `1.036`
- valid rows with symmetric lane flag: `86.17%`
- projection-supported lane rows: `0`

### Working interpretation

- The speed attachment step ran successfully on top of the retained `v11` match output.
- PM peak speeds are lower than AM peak speeds in the aggregate, which is directionally plausible for the Beijing commuting sample.
- Directional asymmetry is present but not pervasive: around one-tenth of pairable centerlines meet the baseline `unsym1` threshold in each peak.
- The strict tidal-candidate screen is highly selective, leaving only `14` centerlines after requiring both strong asymmetry and reversal between AM and PM.
- The next task is therefore interpretive rather than mechanical: check whether these candidate and asymmetry patterns line up with the corridors we expect, then decide whether any additional manual corridor work is still worth doing.

## 2026-04-21: `TidalLanes_0320.tex` refreshed to the retained `v11` pipeline

### Section 3 updates

- Replaced the old four-network comparison with a five-network comparison:
  - raw
  - raw\_split
  - undirected centerline (all)
  - directed centerline (all)
  - directed centerline (baseline)
- Added the baseline-rule explanation directly in the appendix text and notes:
  - valid `LineString`
  - centerline length at least `50 m`
- Updated the matching discussion to use the new four-status directional coverage figures for baseline centerlines and raw edges.
- Added a lane-count subsection after the match assessment, including baseline lane coverage and the lane-coverage map.

### Section 4 updates

- Replaced the older speed-diagnostics references with current `manual_centerline_rules_v11` outputs.
- Added a stage03 summary table using the retained run:
  - matched raw speed observations
  - directed centerline-hour rows
  - unique directed centerlines
  - overall, AM, PM, and late-night means
- Added current directed-centerline speed distribution and diurnal figures generated from the retained run.

### Section 5 updates

- Updated asymmetry counts, overlap counts, and tidal-candidate counts to the retained `v11` outputs.
- Repointed the asymmetry histogram, tidal-candidate map, and AM/PM asymmetry maps to the current output directory.

### Compile status

- `Documents/TidalLanes_0320.tex` compiles successfully after the refresh.
- Remaining warnings are overfull boxes and float-placement adjustments outside the newly updated matching and speed sections; there are no undefined references after the rerun.

## 2026-04-21: final match review and dated TeX draft refresh

### Final match review assets

- Rebuilt `figures/final_match_review` so the centerline coverage map now uses four directional-status classes:
  - both `AB` and `BA` matched
  - `AB` only
  - `BA` only
  - neither matched
- Added both baseline and all-centerline versions, plus a raw-edge version mapped to the same corridor-level directional coverage logic.
- Exported accompanying GIS layers and a length-share table for the four status classes.

### New figure assets

- `data_work/outputs/manual_centerline_rules_v11/figures/final_match_review/map_final_match_centerline_coverage.png`
- `data_work/outputs/manual_centerline_rules_v11/figures/final_match_review/map_final_match_centerline_coverage_all.png`
- `data_work/outputs/manual_centerline_rules_v11/figures/final_match_review/map_final_match_raw_edges_4status.png`
- `data_work/outputs/manual_centerline_rules_v11/figures/paper_appendix/fig_length_cdf_5networks_v11.png`
- `data_work/outputs/manual_centerline_rules_v11/figures/paper_appendix/fig_centerline_speed_distribution_v11.png`
- `data_work/outputs/manual_centerline_rules_v11/figures/paper_appendix/fig_centerline_speed_diurnal_v11.png`

### Dated draft snapshot

- The updated appendix draft was preserved as a dated copy:
  - `Documents/TidalLanes_0421.tex`
- The front matter remains unchanged relative to the refreshed `Documents/TidalLanes_0320.tex`; the dated copy exists to preserve the 2026-04-21 snapshot of the appendix rewrite.

## 2026-04-21: appendix figure retention tightened for `TidalLanes_0421.tex`

### Appendix retention decision

- Retained in the current appendix-facing draft:
  - final match coverage map
  - lane coverage map
  - speed distribution figure
  - diurnal speed profile
  - asymmetry histogram
  - tidal candidate map
- Not retained in the current appendix layout:
  - separate AM asymmetry map

## 2026-04-22: raw-grid method comparison reread sharpened the immediate next step

### What was checked

- Re-read the current comparison outputs under `data_work/outputs/comparison_grid_methods/`
- Rechecked connector summaries for:
  - `raw_connected_v1_grid_travel_time_edgeproj_v2`
  - `raw_connected_v1_grid_travel_time_relaxed_v4`

### Retained comparison takeaways

- `raw_edgeproj_v2` remains the preferred raw-grid direction if the project wants broader feasible coverage:
  - square coverage `47.4%` vs `36.8%` for `raw_node_v4`
  - hex coverage `49.4%` vs `38.4%`
  - voronoi coverage `87.5%` vs `80.4%`
- `raw_edgeproj_v2` also materially shortens connector access distance relative to `raw_node_v4`:
  - square mean connector distance `831 m` vs `1,094 m`
  - hex `850 m` vs `1,105 m`
  - voronoi `432 m` vs `676 m`
- But the extra coverage is not a free gain:
  - adjacent travel time is still much higher than centerline and is also higher than `raw_node_v4`
  - median directional ratio moves toward centerline relative to `raw_node_v4`, but remains far from the centerline benchmark
  - correlation with centerline travel times remains very low for both raw-grid methods

### Interpretation locked for planning

- The decision is no longer simply “choose `raw_edgeproj_v2` or `raw_node_v4`”.
- The immediate bottleneck is to explain the quality of the extra `raw_edgeproj_v2` connections:
  - which newly connected adjacent pairs are reasonable improvements
  - which ones are driven by long detours from topology gaps or poor access-point placement
- Therefore the project should not yet move straight into downstream `lane length / lane count` construction.
- The next operational step is a targeted audit of:
  - long-detour adjacent pairs under `raw_edgeproj_v2`
  - still-unconnected square / hex grids
  - whether the dominant problem is network topology, connector reach, or edge-projection access-point choice

### Rationale

- The asymmetry histogram carries the quantitative distribution more clearly than the two dense peak-specific maps.
- The tidal-candidate map remains useful because the final candidate set is sparse and directly inspectable.
- The lane-coverage map remains useful because it supports the main measurement claim that missing lane assignments are concentrated in a short peripheral tail rather than in the arterial backbone.

### Current interpretation for next-stage handoff

- The retained `v11` asymmetry signal is present but selective:
  - `405` AM `unsym1` centerlines
  - `361` PM `unsym1` centerlines
  - `638` unique centerlines flagged in at least one peak
  - `14` strict tidal candidates after requiring both strong asymmetry and reversal across peaks
- The current paper-facing evidence is sufficient for a targeted corridor review and a go/no-go decision on using the `v11` outputs in the downstream grid cost / model workflow.

## 2026-04-21: raw-grid travel-cost conservative rerun started

### Current task framing

- `stage06` centerline-grid travel cost is retained as a comparison benchmark, not the preferred forward path for raw-grid cost construction.
- The current work focus is the raw_connected_v1 raw-grid route:
  - keep shortest-path routing
  - avoid relying on extra edge deletion that could damage connectivity
  - prioritize conservative grid-to-road attachment and explicit diagnostics

### Specific implementation goal

- Build a more conservative raw_connected_v1 raw-grid shortest-path version under the current topology state.
- Compare that raw-grid version against the retained centerline-grid version on key descriptive travel-cost and directional-pattern statistics.
- Keep in mind that one downstream network version currently mixes a centerline-based network object with raw speed inputs; this is acceptable for now but should be tracked as a possible source of smoothed local asymmetry or topology-speed mismatch.

## 2026-04-21: conservative raw_connected_v1 raw-grid travel-cost `v1` generated and benchmarked

### Files added / refreshed

- Conservative raw-grid output root:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_conservative_v1`
- Main raw-grid build script updated:
  - `data_work/src/raw_connected_v1/grid_travel_time/build_raw_connected_v1_grid_travel_time.py`
- New comparison diagnostic:
  - `data_work/src/diagnostics/compare_grid_cost_versions.py`

### Conservative raw-grid design used in this run

- Kept directed shortest-path routing on the raw_connected_v1 road graph.
- Did not rely on dropping tiny positive-time edges to clean the graph.
- Tightened grid-to-road attachment instead:
  - `connector_count = 1`
  - `connector_max_dist_m = 1500`
  - `connector_min_node_degree = 2`
  - no low-degree fallback
- Reused cached road-segment speed tables and compared against `data_work/outputs/raw_rebuild_validation`.

### Connector coverage diagnostics

- Square:
  - total grids `= 3,357`
  - grids with connectors `= 1,158`
  - grids without connectors `= 2,199`
  - mean connector distance `= 706.5 m`
  - p90 connector distance `= 1,290.4 m`
- Hex:
  - total grids `= 4,604`
  - grids with connectors `= 1,614`
  - grids without connectors `= 2,990`
  - mean connector distance `= 706.6 m`
  - p90 connector distance `= 1,289.2 m`
- Voronoi:
  - total grids `= 1,225`
  - grids with connectors `= 985`
  - grids without connectors `= 240`
  - mean connector distance `= 444.4 m`
  - p90 connector distance `= 982.6 m`

### OD-level comparison against centerline-grid baseline

- Diagnostic file:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_conservative_v1/diagnostics_compare/od_compare_summary_AM.csv`
- Weighted mean travel time differences:
  - square: `18.44` min (`stage06`) vs `60.45` min (`raw_connected_v1_raw_grid`)
  - hex: `12.02` min (`stage06`) vs `59.78` min (`raw_connected_v1_raw_grid`)
  - voronoi: `16.33` min (`stage06`) vs `59.60` min (`raw_connected_v1_raw_grid`)
- Interpretation:
  - this conservative raw-grid version is far slower than the retained centerline-grid benchmark
  - the result is too conservative to replace the current benchmark directly
  - the main bottleneck is now more likely connector design and incomplete bottom-layer connectivity than shortest-path logic itself

### Adjacent-pair comparison against centerline-grid baseline

- Diagnostic file:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_conservative_v1/diagnostics_compare/adjacent_compare_summary_AM.csv`
- Mean adjacent travel time:
  - square: `3.76` min (`stage06`) vs `17.51` min (`raw_connected_v1_raw_grid`)
  - hex: `2.74` min (`stage06`) vs `16.34` min (`raw_connected_v1_raw_grid`)
  - voronoi: `3.37` min (`stage06`) vs `17.81` min (`raw_connected_v1_raw_grid`)
- Adjacent-pair correlation remains low:
  - square `= 0.097`
  - hex `= 0.080`
  - voronoi `= 0.114`
- Interpretation:
  - beyond a level shift, the local ranking pattern is also unstable
  - this points to grid-entry / connector design and topology support as first-order issues

### Directional asymmetry comparison

- Diagnostic file:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_conservative_v1/diagnostics_compare/adjacent_directional_asymmetry_summary_AM.csv`
- Stage06 adjacent-pair directional time ratios are more asymmetric than the current conservative raw-grid version:
  - square median ratio:
    - `stage06 = 0.427`
    - `raw_connected_v1_raw_grid = 0.907`
  - hex median ratio:
    - `stage06 = 0.433`
    - `raw_connected_v1_raw_grid = 0.907`
  - voronoi median ratio:
    - `stage06 = 0.655`
    - `raw_connected_v1_raw_grid = 0.855`
- Share of strongly asymmetric adjacent pairs (`ratio < 0.5`):
  - square:
    - `stage06 = 57.7%`
    - `raw_connected_v1_raw_grid = 14.5%`
  - hex:
    - `stage06 = 56.5%`
    - `raw_connected_v1_raw_grid = 14.1%`
  - voronoi:
    - `stage06 = 35.8%`
    - `raw_connected_v1_raw_grid = 19.6%`

### Interpretation of the smoothing concern

- The current diagnostics do not support the simple claim that the centerline-grid version is the one mechanically smoothing local directional detail relative to the current raw-grid conservative version.
- In the present comparison, the conservative raw-grid version is actually more directionally symmetric at the adjacent-grid level.
- This should not be over-interpreted as evidence that raw-grid intrinsically smooths asymmetry less or more.
- A more plausible reading is:
  - the current raw-grid conservative version is dominated by sparse and restrictive grid connectors plus incomplete bottom-layer topology
  - those constraints suppress local directional variation before it can appear in grid-to-grid costs
- The hybrid-network concern should therefore remain active, but it cannot yet be cleanly diagnosed without a less distorted raw-grid connector design.

## 2026-04-21: raw_connected_v1 raw-grid relaxed rerun `v2` completed

### Goal of this rerun

- The conservative rerun was too restrictive for the user’s stated objective.
- The target here was to move back toward the earlier “normal working” version:
  - keep basic shortest-path safeguards
  - avoid obvious pathologies such as self-loops and zero-time links
  - restore more practical connector reach so that basic grid connectivity is not sacrificed

### Parameters restored toward the earlier working version

- Script updated:
  - `data_work/src/raw_connected_v1/grid_travel_time/build_raw_connected_v1_grid_travel_time.py`
- New default settings used for `relaxed_v2`:
  - `connector_count = 3`
  - `connector_max_dist_m = 2500`
  - `centroid_search_radii_m = 300,600,900,1500,2500`
  - `connector_min_node_degree = 1`
  - `allow_low_degree_fallback = 0`
  - `min_edge_time_min = 0.02`
- Interpretation of the parameter choice:
  - `connector_count = 3` restores multiple candidate attachments per grid, which helps basic reachability and reduces overdependence on one local node
  - `connector_max_dist_m = 2500` returns to the old search envelope instead of the conservative `1500 m` cap, which had clearly cut off too many square and hex grids
  - `connector_min_node_degree = 1` removes the extra conservative penalty against low-degree nodes; dead-end entry is still possible, but only on a positive-time directed graph
  - `min_edge_time_min = 0.02` keeps a light floor on extremely tiny edges without aggressively deleting positive travel-time links
  - Basic safeguards still remain:
    - no self-loop directed edges
    - no nonpositive travel-time edges
    - duplicate `from_node -> to_node` links still collapse to the fastest remaining edge

### Output root

- Relaxed raw-grid output:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_relaxed_v2`
- Network diagnostics added:
  - `data_work/src/diagnostics/plot_raw_connected_grid_network_diagnostics.py`
- Diagnostic outputs:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_relaxed_v2/diagnostics_compare/`
  - `data_work/outputs/raw_connected_v1_grid_travel_time_relaxed_v2/diagnostics_network/`

### Network coverage: source raw_connected_v1 graph vs graph actually used in travel-cost computation

- The relaxed rerun does **not** materially trim the raw_connected_v1 road graph itself.
- Diagnostic summaries:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_relaxed_v2/diagnostics_network/network_coverage_summary_square_AM.csv`
  - `data_work/outputs/raw_connected_v1_grid_travel_time_relaxed_v2/diagnostics_network/network_coverage_summary_hex_AM.csv`
  - `data_work/outputs/raw_connected_v1_grid_travel_time_relaxed_v2/diagnostics_network/network_coverage_summary_voronoi_AM.csv`
- Common road-network retention statistics across grid systems:
  - source directed edges `= 24,757`
  - used directed edges `= 23,983`
  - edge-count retention `= 96.87%`
  - source total length `= 18,507.3 km`
  - used total length `= 18,414.3 km`
  - length retention `= 99.50%`
  - source unique nodes in edges `= 14,746`
  - used unique nodes in edges `= 14,744`
- Interpretation:
  - the graph used for shortest path is visually and quantitatively very close to the source raw_connected_v1 directed graph
  - the main distortion is **not** that the main road graph was heavily deleted
  - the remaining issue is mostly how grids are attached into that graph

### Grid connector coverage under `relaxed_v2`

- Square:
  - total grids `= 3,357`
  - connected grids `= 1,238`
  - connected share `= 36.9%`
  - mean connector distance `= 1,093.9 m`
  - p90 connector distance `= 2,055.6 m`
- Hex:
  - total grids `= 4,604`
  - connected grids `= 1,768`
  - connected share `= 38.4%`
  - mean connector distance `= 1,103.5 m`
  - p90 connector distance `= 2,077.7 m`
- Voronoi:
  - total grids `= 1,225`
  - connected grids `= 986`
  - connected share `= 80.5%`
  - mean connector distance `= 675.2 m`
  - p90 connector distance `= 1,499.6 m`

### Comparison with the conservative rerun

- Relative to `conservative_v1`, the relaxed rerun restores some connector reach:
  - square connected share:
    - `34.5% -> 36.9%`
  - hex connected share:
    - `35.1% -> 38.4%`
  - voronoi connected share:
    - `80.4% -> 80.5%`
- Connector distances rise back toward the earlier working version:
  - this is expected, because restored connectivity is partly coming from allowing farther centroid-to-road attachment again
- Interpretation:
  - the relaxed version is directionally better than the conservative one for basic coverage
  - but square / hex coverage is still materially below what would be comfortable for a final production version

### OD-level comparison against the stage06 benchmark

- Diagnostic file:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_relaxed_v2/diagnostics_compare/od_compare_summary_AM.csv`
- Weighted mean travel time:
  - square: `18.63` min (`stage06`) vs `49.12` min (`relaxed_v2`)
  - hex: `12.40` min (`stage06`) vs `47.71` min (`relaxed_v2`)
  - voronoi: `16.65` min (`stage06`) vs `48.63` min (`relaxed_v2`)
- Median ratio (`raw_connected_v1 / stage06`):
  - square `= 2.240`
  - hex `= 2.966`
  - voronoi `= 2.537`
- OD correlation improves relative to the conservative version:
  - square `= 0.876`
  - hex `= 0.802`
  - voronoi `= 0.894`
- Interpretation:
  - the relaxed version is still systematically slower than `stage06`
  - but it is noticeably closer to a usable benchmark than the conservative version
  - global ranking alignment is now fairly strong again at the OD level

### Adjacent-pair comparison

- Diagnostic file:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_relaxed_v2/diagnostics_compare/adjacent_compare_summary_AM.csv`
- Mean adjacent travel time:
  - square: `3.60` min (`stage06`) vs `11.97` min (`relaxed_v2`)
  - hex: `2.70` min (`stage06`) vs `10.44` min (`relaxed_v2`)
  - voronoi: `3.39` min (`stage06`) vs `12.99` min (`relaxed_v2`)
- Median adjacent ratio (`raw_connected_v1 / stage06`):
  - square `= 3.062`
  - hex `= 3.628`
  - voronoi `= 3.169`
- Adjacent-pair correlation remains low:
  - square `= 0.097`
  - hex `= 0.110`
  - voronoi `= 0.161`
- Interpretation:
  - the local edge-level pattern is still not stable enough
  - this again points back to connector design and bottom-layer topology rather than to the core shortest-path routine

### Directional asymmetry pattern

- Diagnostic file:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_relaxed_v2/diagnostics_compare/adjacent_directional_asymmetry_summary_AM.csv`
- Adjacent undirected-pair median time ratio:
  - square:
    - `stage06 = 0.427`
    - `relaxed_v2 = 0.938`
  - hex:
    - `stage06 = 0.433`
    - `relaxed_v2 = 0.936`
  - voronoi:
    - `stage06 = 0.655`
    - `relaxed_v2 = 0.883`
- Share of strongly asymmetric adjacent pairs (`ratio < 0.5`):
  - square:
    - `stage06 = 57.7%`
    - `relaxed_v2 = 11.7%`
  - hex:
    - `stage06 = 56.5%`
    - `relaxed_v2 = 10.9%`
  - voronoi:
    - `stage06 = 35.8%`
    - `relaxed_v2 = 15.1%`
- Interpretation:
  - the relaxed raw-grid version is still much more symmetric than `stage06` at the adjacent-pair level
  - this does **not** yet imply that the centerline-based graph is the only source of smoothing
  - a more plausible reading is still that current raw-grid local asymmetry is being damped by connector geometry and incomplete underlying topology

### Figures produced for visual inspection

- Shared source-vs-used graph map:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_relaxed_v2/diagnostics_network/map_source_vs_used_AM.png`
- Used graph plus connector maps:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_relaxed_v2/diagnostics_network/map_used_with_connectors_square_AM.png`
  - `data_work/outputs/raw_connected_v1_grid_travel_time_relaxed_v2/diagnostics_network/map_used_with_connectors_hex_AM.png`
  - `data_work/outputs/raw_connected_v1_grid_travel_time_relaxed_v2/diagnostics_network/map_used_with_connectors_voronoi_AM.png`
- Intended use:
  - confirm that the road graph entering shortest-path computation is visually close to the source raw_connected_v1 graph
  - inspect whether the practical loss is coming from sparse grid attachment rather than graph deletion

### Current take-away

- For the user’s current objective, `relaxed_v2` is a better working version than the conservative rerun.
- It restores the old parameter logic closely enough to serve as the current reference raw-grid build.
- However, it is still not ready to replace the retained centerline-grid benchmark as the sole production travel-cost input.
- The next bottleneck remains:
  - better centroid-to-road / grid-to-road attachment design
  - continued manual cleanup of the bottom-layer raw_connected_v1 topology

## 2026-04-21: square-grid adjacent route audit figures generated for `relaxed_v2`

### Purpose

- Produce a first manual QA set for the latest raw-grid connector and routing strategy.
- The user requested random adjacent-grid local maps that show:
  - the surrounding road network
  - the currently selected shortest path under the active raw-grid strategy

### Files added

- New diagnostic script:
  - `data_work/src/diagnostics/sample_raw_connected_adjacent_route_audit.py`
- Output directory:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_relaxed_v2/route_audit_square_AM`

### Current implementation choice

- This first audit pass uses:
  - `grid_type = square`
  - `period = AM`
  - `n_samples = 20`
  - `seed = 20260421`
- Sampling frame:
  - adjacent square-grid pairs with finite `raw_connected_travel_time_min`
- Each figure includes:
  - the two sampled grids
  - all used raw_connected_v1 road edges in a local buffered window
  - the chosen shortest-path road segments
  - the specific connector segments used at the origin and destination side

### Output inventory

- `20` route-audit figures:
  - `01_sq_11_20_to_sq_10_20.png`
  - `02_sq_11_51_to_sq_12_51.png`
  - `03_sq_14_28_to_sq_15_28.png`
  - `04_sq_16_51_to_sq_16_50.png`
  - `05_sq_21_31_to_sq_21_30.png`
  - `06_sq_23_43_to_sq_24_43.png`
  - `07_sq_24_34_to_sq_23_34.png`
  - `08_sq_25_40_to_sq_25_41.png`
  - `09_sq_26_31_to_sq_26_30.png`
  - `10_sq_28_41_to_sq_28_42.png`
  - `11_sq_28_45_to_sq_29_45.png`
  - `12_sq_29_35_to_sq_29_34.png`
  - `13_sq_30_27_to_sq_29_27.png`
  - `14_sq_31_29_to_sq_32_29.png`
  - `15_sq_35_29_to_sq_34_29.png`
  - `16_sq_35_55_to_sq_34_55.png`
  - `17_sq_38_48_to_sq_37_48.png`
  - `18_sq_51_27_to_sq_51_28.png`
  - `19_sq_52_18_to_sq_52_19.png`
  - `20_sq_7_31_to_sq_6_31.png`
- Summary tables:
  - `sampled_adjacent_pairs.csv`
  - `sampled_adjacent_route_summary.csv`
  - `run_note.txt`

### Quick descriptive takeaways from the 20 sampled square pairs

- Mean route time decomposition:
  - mean road-path time `= 9.02 min`
  - mean connector time `= 3.75 min`
- Connector rank used on the sampled routes:
  - home side:
    - rank `1`: `11`
    - rank `2`: `3`
    - rank `3`: `6`
  - work side:
    - rank `1`: `8`
    - rank `2`: `9`
    - rank `3`: `3`
- `3` of the `20` sampled adjacent pairs use zero road edges and are connected entirely through connector-to-connector linkage:
  - `15_sq_35_29_to_sq_34_29.png`
  - `18_sq_51_27_to_sq_51_28.png`
  - `20_sq_7_31_to_sq_6_31.png`

### Interpretation

- This first audit set is useful precisely because it should reveal whether some adjacent-grid paths are being driven too much by connector geometry instead of actual road traversal.
- The presence of zero-road-edge adjacent routes is not automatically wrong, but these cases should be among the first manual inspections.
- Frequent use of rank-3 connectors in the sampled routes also suggests that connector choice remains a first-order determinant of local travel cost in the current raw-grid build.

## 2026-04-21: relaxed raw-grid `v3` rerun completed under strict period-specific speed use

### What changed relative to `relaxed_v2`

- `data_work/src/raw_connected_v1/grid_travel_time/build_raw_connected_v1_grid_travel_time.py` was updated so that:
  - `AM` runs use only `speed_am_kmh`
  - `PM` runs use only `speed_pm_kmh`
  - missing period-specific speeds are no longer filled from `FF` or `overall`
  - connector node local speeds are also derived only from retained period-specific road edges
- The `v3` output root is:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_relaxed_v3`

### Period-specific speed missingness

- Directed edges with missing AM speed:
  - total `= 24,763`
  - missing `= 270`
  - share `= 1.0903%`
- Directed edges with missing PM speed:
  - total `= 24,763`
  - missing `= 271`
  - share `= 1.0944%`
- Interpretation:
  - period-specific speed missingness is small but nonzero
  - under `v3`, these edges are no longer retained through cross-period fallback

### Cleaned `v3` configuration and speed-source metadata

- The output config and metrics were cleaned so they no longer imply cross-period speed fallback.
- `run_config.json` now explicitly records:
  - `strict_period_speed_only = true`
  - `connector_speed_rule = node_local_period_median_only`
  - `speed_stats.period_speed_source = speed_am_kmh` for the current AM build
- `FF` was also added as a valid explicit speed source option for future uncongested runs.

### `v3` versus `v2`: key quantitative differences

- Comparison file:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_relaxed_v3/diagnostics_compare_v2_vs_v3/v2_vs_v3_key_metrics.csv`
- Wide comparison file:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_relaxed_v3/diagnostics_compare_v2_vs_v3/v2_vs_v3_key_metrics_wide.csv`
- Main pattern:
  - `v3` is slightly tighter and slightly slower than `v2`, but not dramatically different
- Coverage:
  - square connected grids: `1238 -> 1235`
  - hex connected grids: `1768 -> 1766`
  - voronoi connected grids: `986 -> 984`
- Road graph retention:
  - used directed-edge share: `96.87% -> 95.85%`
  - used length share: `99.50% -> 98.94%`
- Weighted mean travel time:
  - square: `49.12 -> 50.47 min`
  - hex: `47.71 -> 48.37 min`
  - voronoi: `48.63 -> 49.90 min`
- OD correlation:
  - square: `0.876 -> 0.870`
  - hex: `0.802 -> 0.800`
  - voronoi: `0.894 -> 0.884`
- Interpretation:
  - removing cross-period fallback does not cause a collapse in the graph
  - but it does make the retained raw-grid network slightly sparser and slightly slower
  - the core bottleneck still appears to be connector design and graph construction, not speed interpolation alone

### `v3` square-grid route audit generated

- Output directory:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_relaxed_v3/route_audit_square_AM`
- This updated the same square-grid adjacent route audit under the stricter period-only speed rule.
- Compared with the earlier `v2` audit:
  - zero-road-edge sampled cases dropped from `3 / 20` to `1 / 20`
  - mean connector time dropped from `3.75` to `3.31 min`
  - mean road time dropped from `9.02` to `7.62 min`
- Interpretation:
  - under the sampled square-grid cases, the stricter speed rule does not worsen the local route pictures
  - if anything, the sampled routes appear slightly less dominated by pure connector-only movement

## 2026-04-21: next connector experiment chosen

### Decision

- Keep `relaxed_v3` as the current baseline raw-grid version.
- Start a **parallel** experiment rather than editing the baseline in place.

### New experiment goal

- Replace node-based connector attachment with a road-edge projection attachment:
  - instead of snapping a grid centroid to a nearby road node
  - project it to a nearby reasonable road edge
  - treat that projected access point as the road entry point
- Preserve separate code, separate output directories, and separate diagnostics so the baseline remains reproducible.

### Why this branch is worth trying

- The current node-based connector can overreact to:
  - dead-end nodes
  - awkward local direction structure
  - centroid sensitivity
- A projected access point on a nearby road segment may better represent:
  - where a grid would realistically enter the road system
  - a shorter and more natural centroid-to-road connection

### Required comparison outputs for the parallel branch

- Save all branch outputs separately from `relaxed_v3`
- Generate route-audit figures analogous to the baseline audit
- Generate coverage and travel-cost summaries analogous to the baseline diagnostics
- Produce a direct baseline-versus-branch comparison of connector behavior and route plausibility

## 2026-04-21: edge-projection connector parallel branch implemented and benchmarked

### Goal

- Keep `relaxed_v3` unchanged as the reproducible baseline.
- Build a parallel branch where grid connectors attach to projected access points on nearby reasonable road edges rather than to nearby road nodes.

### New files added

- Parallel build script:
  - `data_work/src/raw_connected_v1/grid_travel_time/build_raw_connected_v1_grid_travel_time_edgeproj.py`
- Parallel network diagnostics:
  - `data_work/src/diagnostics/plot_raw_connected_grid_network_diagnostics_edgeproj.py`
- Parallel route audit:
  - `data_work/src/diagnostics/sample_raw_connected_adjacent_route_audit_edgeproj.py`

### Output root

- Parallel branch output:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v1`

### What the parallel method changes

- Connector target is no longer a nearby node.
- For each grid:
  - search nearby road edges
  - project the grid centroid onto a nearby candidate edge
  - treat the projected point as an access point
  - split the host directed edge at projected access points
  - compute grid-to-grid costs through these split access nodes
- This branch still preserves:
  - strict period-only speed use
  - separate output directories and diagnostics
  - the same comparison framework against the retained centerline-grid baseline

### Main quantitative comparison against `relaxed_v3`

- Comparison files:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v1/diagnostics_compare_baseline_v3_vs_edgeproj/baseline_v3_vs_edgeproj_key_metrics.csv`
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v1/diagnostics_compare_baseline_v3_vs_edgeproj/baseline_v3_vs_edgeproj_key_metrics_wide.csv`

### Coverage gains

- Square connected grids:
  - `1,235 -> 1,591`
  - connected share `36.8% -> 47.4%`
- Hex connected grids:
  - `1,766 -> 2,273`
  - connected share `38.4% -> 49.4%`
- Voronoi connected grids:
  - `984 -> 1,072`
  - connected share `80.3% -> 87.5%`
- Mean connector distance falls substantially:
  - square: `1,094 m -> 831 m`
  - hex: `1,104 m -> 850 m`
  - voronoi: `674 m -> 433 m`
- Interpretation:
  - edge projection clearly improves connector coverage and shortens centroid-to-road attachment distance
  - this confirms that node-based snapping was one important source of under-connection

### Travel-cost tradeoff

- Weighted mean OD travel time becomes higher under edge projection:
  - square: `50.47 -> 60.07 min`
  - hex: `48.37 -> 59.86 min`
  - voronoi: `49.90 -> 57.60 min`
- OD correlation against the retained centerline-grid benchmark falls:
  - square: `0.870 -> 0.804`
  - hex: `0.800 -> 0.737`
  - voronoi: `0.884 -> 0.809`
- Adjacent-pair mean travel time also rises:
  - square: `12.35 -> 14.17 min`
  - hex: `10.47 -> 13.52 min`
  - voronoi: `13.24 -> 16.81 min`
- Interpretation:
  - the parallel branch gains connectivity, but it currently does so at the cost of longer implied travel times and weaker agreement with the centerline-grid benchmark
  - this likely reflects that the new access geometry is more permissive, but the resulting split-edge graph still needs further discipline before it becomes a clear improvement overall

### Directional pattern

- Adjacent-pair median time ratio moves slightly downward under edge projection:
  - square: `0.938 -> 0.890`
  - hex: `0.937 -> 0.886`
  - voronoi: `0.879 -> 0.839`
- Share of strongly asymmetric adjacent pairs (`ratio < 0.5`) rises:
  - square: `12.2% -> 14.4%`
  - hex: `10.8% -> 14.7%`
  - voronoi: `16.4% -> 19.8%`
- Interpretation:
  - edge projection does recover a bit more local directional variation than the node-based baseline
  - but it is still much more symmetric than the retained centerline-grid benchmark

### Edge-projection route audit

- Output directory:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v1/route_audit_square_AM`
- `20` square-grid adjacent route-audit figures were generated under the same random seed.
- Summary file:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v1/route_audit_square_AM/sampled_adjacent_route_summary.csv`
- Quick takeaways:
  - zero-road-edge sampled cases: `0 / 20`
  - mean connector time `= 1.84 min`
  - mean road time `= 9.41 min`
- Compared with baseline `relaxed_v3`:
  - baseline had `1 / 20` zero-road-edge cases
  - baseline mean connector time `= 3.31 min`
  - baseline mean road time `= 7.62 min`
- Interpretation:
  - the edge-projection branch does exactly what it was meant to do geometrically:
    - connectors become shorter
    - sampled routes rely less on pure connector-only movement
  - but the road-path component in the sampled routes becomes longer on average
  - this is consistent with the aggregate statistics showing better attachment but slower implied end-to-end movement

### Diagnostics generated

- Comparison summaries:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v1/diagnostics_compare/`
- Network diagnostics:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v1/diagnostics_network/`
- Baseline-versus-branch comparison:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v1/diagnostics_compare_baseline_v3_vs_edgeproj/`

### Current take-away

- The edge-projection connector branch is promising as a geometry fix:
  - higher grid coverage
  - shorter connector distances
  - fewer obviously connector-only local routes in the square-grid audit
- But in its current form it is **not yet** a clean replacement for the baseline:
  - travel times are higher
  - benchmark alignment is weaker
  - the split-edge access construction likely needs another round of discipline
- The most natural next question is whether edge projection should be combined with:
  - pair-specific shortest-path construction
  - tighter candidate-edge filters
  - or both

## 2026-04-22: pair-specific shortest path implemented for both node-based and edgeproj; three-version comparison generated

### What changed

- **`compute_raw_connected_grid_costs` in node-based script was using bidirectional connector edges**:
  - `rows = [road_edges, grid→road, road→grid]` — all grid centroids were mutual waypoints
  - This allowed shortest paths to route through other grid centroids as intermediate nodes
- **`compute_grid_costs_pairwise` in edgeproj script built a combined graph with all access nodes**:
  - Other grids' access points (road split nodes) were present in every pair's routing graph

### New functions added

- `compute_raw_connected_grid_costs_adjacent_pairwise` in `build_raw_connected_v1_grid_travel_time.py`:
  - Builds pure road CSR graph once
  - For each adjacent pair: adds HOME virtual node (one-directional: home→road) and WORK virtual node (one-directional: road→work)
  - No other grid's connector edges enter the graph
- `compute_grid_costs_adjacent_pairwise` in `build_raw_connected_v1_grid_travel_time_edgeproj.py`:
  - For each access point on directed edge `(u→v)` at fraction `f`:
    - HOME → downstream road node `v` with cost `connector_time + edge_time*(1-f)`
    - Upstream road node `u_w` → WORK with cost `edge_time_w*f_w + connector_time_w`
  - Pure road graph only; no split-edge graph needed for pair-specific runs
  - Activated via `--pair-specific` flag

### Bug fix: period suffix hardcoded as 'AM' in node-based `run_for_grid`

- Fixed `suffix = f"..._{'AM'}"` → `suffix = f"..._{period}"`
- Added `period` as explicit parameter to `run_for_grid`

### New output directories

- `data_work/outputs/raw_connected_v1_grid_travel_time_relaxed_v4` — node-based with pair-specific routing
- `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2` — edge-projection with pair-specific routing

### Three-version comparison: key metrics (AM, adjacent pairs)

| version | grid | connected% | mean connector (m) | mean adj time (min) | median dir ratio | share asym<0.5 | corr vs centerline |
|---|---|---|---|---|---|---|---|
| centerline | square | — | — | 3.44 | 0.427 | 57.7% | — |
| raw_node | square | 36.8% | 1094 | 12.93 | 0.938 | 12.6% | 0.099 |
| raw_edgeproj | square | 47.4% | 831 | 14.81 | 0.793 | 14.9% | 0.088 |
| centerline | hex | — | — | 2.67 | 0.433 | 56.5% | — |
| raw_node | hex | 38.4% | 1105 | 11.50 | 0.937 | 12.0% | 0.073 |
| raw_edgeproj | hex | 49.4% | 850 | 13.49 | 0.796 | 15.0% | 0.093 |
| centerline | voronoi | — | — | 3.57 | 0.655 | 35.8% | — |
| raw_node | voronoi | 80.4% | 676 | 13.98 | 0.882 | 16.7% | 0.136 |
| raw_edgeproj | voronoi | 87.5% | 432 | 16.39 | 0.802 | 18.5% | 0.104 |

Full comparison outputs: `data_work/outputs/comparison_grid_methods/`

### Key interpretation

- Pair-specific routing for `raw_node` v4 gives essentially the same travel times as `relaxed_v3` (12.93 vs ~12.35 min for square adjacent) — the bidirectional connector issue in v3 was not causing material shortcuts
- `raw_edgeproj` v2 (pair-specific) also closely matches `edgeproj_v1` (14.81 vs 14.17 min) — access-node graph contamination was minor
- `raw_edgeproj` recovers ~10 percentage points of grid coverage vs `raw_node` (47% vs 37% for square), with ~20% shorter connectors
- Both raw-grid methods show much weaker directional asymmetry than centerline (median ratio ~0.94 vs ~0.43)
- Square/hex coverage remains a bottleneck (37–49%); voronoi coverage is stronger (80–88%)
- Correlation with centerline is still low (~0.07–0.14 for adjacent pairs), confirming the raw-grid results still reflect local connector geometry more than true road asymmetry

## 2026-04-22: pair-specific route-audit scripts aligned to latest methods; shared random 20-pair audit generated

### Audit-script correction

- `data_work/src/diagnostics/sample_raw_connected_adjacent_route_audit.py` was updated so the route audit now reconstructs the current pair-specific node-based graph instead of the older all-grids augmented graph.
- `data_work/src/diagnostics/sample_raw_connected_adjacent_route_audit_edgeproj.py` was updated so the route audit now reconstructs the current pair-specific edge-projection logic directly from the retained road graph and projected access geometry.
- Both audit scripts now accept a shared `home_grid` / `work_grid` pair list so the same random adjacent pairs can be compared across versions.

### Additional check on the pair-specific fix

- Aggregate means were close before and after the pair-specific correction, but the local tail is not negligible.
- For square AM adjacent pairs:
  - `raw_node`: `5.0%` of comparable pairs move by more than `1` minute and `3.6%` move by more than `5` minutes from `relaxed_v3` to `relaxed_v4`
  - `raw_edgeproj`: `30.8%` of comparable pairs move by more than `1` minute and `4.0%` move by more than `5` minutes from `edgeproj_v1` to `edgeproj_v2`
- Interpretation:
  - the earlier mixed-graph bug was not dominating the aggregate means
  - but it did matter for a nontrivial subset of local adjacent-pair routes, especially in the edge-projection branch

### Shared random square-grid audit (`20` pairs, AM)

- Shared sample file:
  - `data_work/outputs/comparison_grid_methods/shared_random_pairs_square_AM_20260422.csv`
- Node-based audit:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_relaxed_v4/route_audit_square_AM_shared20`
- Edge-projection audit:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/route_audit_square_AM_shared20`
- Merged comparison table:
  - `data_work/outputs/comparison_grid_methods/shared20_square_AM_route_audit_comparison.csv`

### Shared-random audit takeaways

- `raw_node_v4`:
  - zero-road-edge sampled cases `= 4 / 20`
  - mean connector time `= 3.57 min`
  - mean road-path time `= 5.04 min`
  - mean total adjacent time `= 8.57 min`
- `raw_edgeproj_v2`:
  - zero-road-edge sampled cases `= 0 / 20`
  - mean connector time `= 1.97 min`
  - mean road-path time `= 11.47 min`
  - mean total adjacent time `= 14.74 min`
- Interpretation:
  - edge projection materially reduces connector-only local routes and shortens centroid-to-road access
  - the cost is a longer road-path component, with a long-detour tail that now becomes the main diagnostic target

### Comparison figures generated

- Shared-20 total-time comparison:
  - `data_work/outputs/comparison_grid_methods/fig_shared20_total_time_compare_square_AM.png`
- Shared-20 paired examples for paper:
  - `data_work/outputs/comparison_grid_methods/fig_pair_examples_square_AM.png`
- All side-by-side shared-20 pair figures:
  - `data_work/outputs/comparison_grid_methods/shared20_square_AM_side_by_side/`

### Documentation refresh

- `Documents/L1_manuscripts/TidalLanes_0421.tex` was updated with:
  - the current raw-grid method split
  - the pair-specific-fix interpretation
  - the three-version aggregate comparison
  - the shared-random 20-pair local route audit
  - the current preference for `raw_edgeproj_v2` as the forward branch, with the long-detour tail left as the next diagnostic task

## 2026-04-22: lightweight project-management and agent-onboarding files tightened

### Goal

- Keep the project restart / handoff workflow light rather than introducing a heavy operating-log system.
- Make it cheaper for any newly attached agent to align on current status, current bottleneck, and next action.

### Changes made

- `nextstep.md` was lightly extended with:
  - a 3-line project summary
  - a short `当前开放问题` section
- `AGENTS.md` now points new sessions to a root-level quickstart file for low-token alignment.
- New file added:
  - `agent_quickstart.md`

### Retained workflow choice

- Do **not** add a heavy parallel project log.
- Keep the existing split:
  - `nextstep.md` for current status and next action
  - `data_work/docs/WORK_LOG.md` for completed work and retained decisions
- Use `agent_quickstart.md` as the minimal handoff / onboarding entrypoint for new agents.

## 2026-04-22: raw-grid comparison figure and table layout tightened in `TidalLanes_0421.tex`

### User-facing adjustments

- The local route-comparison figure in the raw-grid section was expanded beyond the earlier two-example layout.
- The method-comparison table in the same section was reformatted to avoid an overly wide layout.

### What changed

- Generated a new three-case comparison figure:
  - `data_work/outputs/comparison_grid_methods/fig_pair_examples_square_AM_3cases.png`
- Replaced the earlier two-case writeup with a three-case discussion covering:
  - a connector-only node-based case
  - a case where edge projection is faster
  - a remaining long-detour edge-projection case
- Reformatted `Table 11` in `Documents/L1_manuscripts/TidalLanes_0421.tex` into a compact three-row summary by grid type with abbreviated column headers.

### Compile status

- `Documents/L1_manuscripts/TidalLanes_0421.tex` recompiles successfully after the figure/table adjustment.
- The earlier raw-grid-specific float-too-large / table-width issues were removed; remaining warnings are legacy layout warnings elsewhere in the document.

## 2026-04-23: `model_v1` implementation checklist added

### Purpose

- Converted the temporary `model_v1` review and data-gap diagnosis into an execution checklist.
- The checklist is intentionally process-only and can be deleted once the model/data alignment work is complete.

### New note

- `Documents/L2_notes/model_v1_implementation_checklist_temp_20260423.md`

### Key execution choices recorded

- Build model input bundles before interpreting estimation:
  - `centerline_grid_v11` for the current locked centerline version
  - `raw_edgeproj_v2` for the current raw-grid forward branch
- Separate observed minutes from AA model costs:
  - `tau_*_min` for physical travel time
  - `t_*_iceberg` for model transport costs
- Treat current `theta` and `lambda` regressions as diagnostic calibration until data and identification are strengthened.
- Add data-quality gates for free-flow time, lane/capacity coverage, OD graph support, and convergence before using welfare or counterfactual results.

## 2026-04-23: first `raw_edgeproj_v2` detour-tail decomposition completed

### Purpose

- Decompose the current edge-projection adjacent-pair long tail into connector time, endpoint host-edge time, and middle road-path time.
- Keep the diagnostic separate from model inputs because the raw-grid connector tail is not yet ready to serve as the welfare baseline.

### Script added

- `data_work/src/diagnostics/summarize_edgeproj_detour_tail.py`

### Outputs

- `data_work/outputs/comparison_grid_methods/edgeproj_v2_detour_tail_square_AM/`
- `data_work/outputs/comparison_grid_methods/edgeproj_v2_detour_tail_square_PM/`
- `data_work/outputs/comparison_grid_methods/edgeproj_v2_detour_tail_hex_AM/`
- `data_work/outputs/comparison_grid_methods/edgeproj_v2_detour_tail_voronoi_AM/`
- Combined summary:
  - `data_work/outputs/comparison_grid_methods/edgeproj_v2_detour_tail_summary_selected.csv`

### Main diagnostic result

- Across the four checked slices, the top-200 adjacent-pair tail is dominated by road-path time rather than connector time.
- Median connector share in the selected tail:
  - square AM: `2.85%`
  - square PM: `2.79%`
  - hex AM: `2.40%`
  - voronoi AM: `1.57%`
- Median tail road time:
  - square AM: `55.75 min`
  - square PM: `56.66 min`
  - hex AM: `62.58 min`
  - voronoi AM: `55.41 min`
- Share of selected tail pairs with road time above `75%` of total:
  - square AM: `100.0%`
  - square PM: `100.0%`
  - hex AM: `99.5%`
  - voronoi AM: `100.0%`

### Interpretation

- The first-order problem in the extreme `raw_edgeproj_v2` tail is not that centroid-to-road connectors are mechanically long.
- The more likely source is road-graph detour after access, caused by bottom-layer topology gaps, one-way directionality, or access edges that attach to locally plausible but globally poorly connected road segments.
- The next diagnostic should map the top-tail pairs and inspect whether they cluster around a small number of disconnected corridors or access-edge choices.

## 2026-04-23: stage02 matching score distance-dominance diagnostic completed

### Purpose

- Check whether the retained stage02 candidate-selection rule is still dominated by mean geometric distance after adding the angular component.
- Use the current `manual_centerline_rules_v11` outputs, excluding the `963` manual exact-match raw edges from the dominance test.

### Outputs

- `data_work/outputs/manual_centerline_rules_v11/metrics/stage02_distance_vs_angle_dominance_summary.csv`
- `data_work/outputs/manual_centerline_rules_v11/metrics/stage02_candidate_score_vs_distance_diagnostic.csv`
- `data_work/outputs/manual_centerline_rules_v11/metrics/stage02_candidate_score_components_long.csv`

### Main diagnostic result

- Among `22,713` non-manual baseline matches, the distance component exceeds the angular component in `95.84%` of final selected matches.
- The median distance-component share of the selected score is `94.78%`; the mean share is `88.40%`.
- Reconstructing the full candidate set gives `302,128` candidate rows and `136,680` candidates within the distance cap.
- The candidate chosen by the full score,
  - `score = d_mean + 0.1 * angle_diff`,
  is the same as the candidate chosen by distance alone in `99.45%` of non-manual baseline matches.
- Only `0.55%` of non-manual baseline matches select a higher-distance candidate because the angular gain offsets the distance penalty.

### Interpretation

- The retained stage02 matching rule is still distance-dominated.
- The angular term mainly acts as a tie-breaker or stabilizer when nearby centerline candidates have similar mean perpendicular distance.
- This supports the current Section 3.6 description: direction affects candidate selection, but final validity is governed by the `d_mean <= 180 m` distance cap.

## 2026-04-23: appendix map comparing undirected centerline and baseline filter added

### Purpose

- Add a paper-facing figure that lets the appendix reader see, at full-network scale, what the baseline filter removes from the undirected centerline system.
- Keep the figure-generation logic reusable rather than producing a one-off manual export.

### Code and output location rule

- Reusable figure-generation code for this class of appendix maps lives under:
  - `data_work/src/diagnostics/`
- Paper-facing final figures for one retained run live under:
  - `data_work/outputs/{version_id}/figures/paper_appendix/`
- This run follows that rule:
  - code added to `data_work/src/diagnostics/plot_match_diagnostics.py`
  - figure written to `data_work/outputs/manual_centerline_rules_v11/figures/paper_appendix/fig_undirected_vs_baseline_centerline_maps.png`

### Figure content

- Left panel: full undirected centerline network.
- Right panel: baseline-kept undirected centerline network (`keep_baseline = 1`).
- The figure is referenced in `Documents/L1_manuscripts/TidalLanes_0421.tex` Section 3.7.
- A local zoomed companion figure using the same layout was also exported to:
  - `data_work/outputs/manual_centerline_rules_v11/figures/paper_appendix/fig_undirected_vs_baseline_centerline_maps_tiananmen_15km.png`
- The zoomed version is clipped to the `15 km` Tiananmen-radius window already used elsewhere in the appendix diagnostics.

### Reusable convention

- If a new figure is primarily a paper or appendix asset, place the code in `data_work/src/diagnostics/` and write the final exported figure to `outputs/{version_id}/figures/paper_appendix/`.
- If a figure is mainly for GIS inspection or temporary QA, keep it in a diagnostic or review subdirectory instead of mixing it into `paper_appendix/`.

## 2026-04-23: lane-ratio histogram added for the lane-count appendix subsection

### Purpose

- Add a paper-facing diagnostic for the opposite-direction lane ratio so the appendix can directly assess whether the lane proxy is concentrated near one.

### Code and output location

- Reusable figure-generation code was added to:
  - `data_work/src/diagnostics/plot_lane_diagnostics.py`
- The retained paper-facing figure was written to:
  - `data_work/outputs/manual_centerline_rules_v11/figures/paper_appendix/fig_lane_ratio_hist_baseline_v11.png`

### Distribution summary

- Non-missing opposite-direction lane ratios: `7,394`
- Mean ratio: `1.036`
- Median ratio: `1.000`
- Share exactly equal to `1.0`: `83.6%`
- Share at or below `1.1`: `92.7%`
- Share at or below `1.5`: `97.9%`

### Interpretation

- The lane-ratio mass is tightly concentrated at one, with only a modest right tail.
- This supports the appendix interpretation that the lane proxy is usually balanced across directions on corridors where both-direction lane estimates are observed.
- The paper-facing histogram was later truncated at ratio `= 1.5` so the main mass near one is visually legible; this truncation still retains `97.9%` of observations.

## 2026-04-23: `TidalLanes_0421.tex` appendix verification and scope cleanup completed

### Purpose

- Continue paper-facing verification and revision of the dated appendix draft `Documents/L1_manuscripts/TidalLanes_0421.tex`.
- Check that Section 4 and Section 5 tables and figures point to the current retained `manual_centerline_rules_v11` outputs.
- Remove model-facing material from this appendix-facing draft so the file stays focused on the retained descriptive and construction diagnostics.

### Main edits

- Re-verified the Section 4 speed tables against:
  - `data_work/outputs/manual_centerline_rules_v11/metrics/stage03_speed_summary.csv`
  - `data_work/outputs/manual_centerline_rules_v11/data/centerline_speed_master.parquet`
- Re-verified the Section 5 asymmetry and tidal-candidate counts against:
  - `data_work/outputs/manual_centerline_rules_v11/metrics/stage03b_asymmetry_summary.csv`
  - `data_work/outputs/manual_centerline_rules_v11/data/centerline_asymmetry_summary.csv`
  - `data_work/outputs/manual_centerline_rules_v11/data/centerline_asymmetry_am_pm_comparison.csv`
  - `data_work/outputs/manual_centerline_rules_v11/data/centerline_tidal_lane_candidates.csv`
- Unified table-note layout so paper tables now use left-aligned notes rather than inheriting `\centering`.
- Removed the former Section 8 and Section 9 material from this draft:
  - `Model`
  - `Model Estimation`

### Compile status

- `Documents/L1_manuscripts/TidalLanes_0421.tex` compiles successfully after the cleanup.
- Remaining warnings are float-placement adjustments and one float-heavy page, not undefined references or control-sequence errors.

## 2026-04-23: Section 6 grid-system appendix updated to baseline-centered `v11` logic

### Purpose

- Bring the grid-construction appendix into line with the retained `manual_centerline_rules_v11` baseline centerline workflow.
- Remove the remaining mismatch in which the text still described Voronoi and grid-comparison objects from the older `raw_rebuild_validation` line.

### Code change

- `data_work/src/stages/stage05_build_grids.py` now filters `centerline_dir_master.parquet` to `keep_baseline = True` before constructing square, hex, and Voronoi grids.
- This makes the Stage 05 grid outputs explicitly baseline-centered and consistent with the rest of the retained appendix pipeline.

### New retained Stage 05 outputs

- `data_work/outputs/manual_centerline_rules_v11/data/grid_square_master.parquet`
- `data_work/outputs/manual_centerline_rules_v11/data/grid_hex_master.parquet`
- `data_work/outputs/manual_centerline_rules_v11/data/grid_voronoi_master.parquet`
- `data_work/outputs/manual_centerline_rules_v11/metrics/stage05_grid_comparison_stats.csv`

### Updated baseline-centered grid statistics

- Square:
  - `3,357` cells
  - `7,560 / 11,890 = 63.6%` baseline directed centerline segments fully within one cell
  - `4,214.52 / 19,391.30 km = 21.7%` of baseline directed centerline length fully within one cell
- Hex:
  - `4,604` cells
  - `6,924 / 11,890 = 58.2%`
  - `3,486.66 / 19,391.30 km = 18.0%`
- Voronoi:
  - `1,213` cells
  - `7,096 / 11,890 = 59.7%`
  - `3,058.18 / 19,391.30 km = 15.8%`

### Interpretation

- Square and hex geometry did not change; they remain exogenous tessellations over the study area.
- Their appendix statistics changed because the Stage 05 counting base now uses the retained baseline directed centerline backbone rather than the older unrestricted centerline line.
- Voronoi did change geometrically, because its seed construction is road-network-induced. After switching to the retained baseline centerline backbone, the Voronoi system now contains `1,213` cells rather than the older notebook-line count.

### Figure updates

- Added a new paper-facing comparison figure:
  - `data_work/outputs/manual_centerline_rules_v11/figures/paper_appendix/fig_grid_comparison_v11_baseline.png`
- This figure replaces the old archive-sourced grid-comparison image in `TidalLanes_0421.tex`.
- The Voronoi boundary color was later revised to blue in the retained paper-facing figure.

### Prose alignment in `TidalLanes_0421.tex`

- Section 6 introduction now distinguishes:
  - square and hex as exogenous tessellations
  - Voronoi as the only network-induced partition
- Section 6.1, 6.2, and 6.3 were rewritten to keep this distinction explicit and internally consistent.

## 2026-04-23: route distance and average speed added to pair-specific shortest-path outputs

### Purpose

- Extend the grid-to-grid pair-specific shortest-path computation to record, for each finite-cost adjacent pair, the total route distance (`route_length_m`) and the implied average travel speed (`route_avg_speed_kmh`) in addition to the existing travel time.
- These fields support downstream model inputs where physical distance and speed are needed separately from travel time.

### Code changes

- `data_work/src/raw_connected_v1/grid_travel_time/build_raw_connected_v1_grid_travel_time_edgeproj.py`:
  - `compute_grid_costs_adjacent_pairwise` now requests `return_predecessors=True` from `shortest_path`.
  - Builds a `(from_node, to_node) -> length_m` lookup covering road edges plus connector edges (home: `connector_dist_m + outbound_edge_length`; work: `inbound_edge_length + connector_dist_m`).
  - Traces the shortest-path predecessor chain from WORK to HOME and sums lengths.
  - Returns `route_length_m` and `route_avg_speed_kmh = (length_m / 1000) / (time_min / 60)` per pair.
- `data_work/src/raw_connected_v1/grid_travel_time/build_raw_connected_v1_grid_travel_time.py`:
  - Same predecessor-tracing logic added to `compute_raw_connected_grid_costs_adjacent_pairwise`.
  - Home connector length = `connector_dist_m`; work connector length = `connector_dist_m`.

### Outputs regenerated (AM and PM, all grid types)

- `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/data/raw_connected_vs_old_adjacent_adjacent_{grid}_{period}.csv`
- `data_work/outputs/raw_connected_v1_grid_travel_time_relaxed_v4/data/raw_connected_vs_old_adjacent_adjacent_{grid}_{period}.csv`

### Key descriptive results (square AM)

- `edgeproj_v2`: mean route length `7,717 m`, mean avg speed `35.4 km/h`, mean time `14.81 min`
- `relaxed_v4`: mean route length `6,738 m`, mean avg speed `35.0 km/h`, mean time `12.93 min`
- NaN count matches pairs with no finite travel time (connector unavailable); no speed outliers above 200 km/h or below 1 km/h.

## 2026-04-23: `manual_centerline_rules_v11` square QSM input handoff rebuilt for `model_v1`

### Purpose

- Start the `model/data` alignment line from the current locked centerline version rather than the older `raw_rebuild_validation` default.
- Close the first pipeline-to-model handoff gap by exporting square-grid QSM inputs with explicit time and lane-quality metadata.

### Code changes

- `data_work/src/stages/stage08_build_qsm_inputs.py` was extended so Stage 08 now:
  - exports `tau_obs_min` and `tau_ff_min` alongside the legacy `t_min` / `t_ff_min` names
  - records `tau_ff_missing_original`, `tau_ff_imputed_flag`, `tau_ff_source`, and `tau_consistency_flag`
  - aggregates lane support from `grid_links_square_long.csv` plus `centerline_lane_master.parquet`
  - writes edge-level `lane_quality_flag`, `lanes_directional`, and `capacity_proxy`
  - writes a grid-specific `data_quality_*_qsm.csv`
- Model entry defaults were updated so:
  - `data_work/src/model/run_spatial_equilibrium.py`
  - `data_work/src/model/run_counterfactual_suite.py`
  now default to `manual_centerline_rules_v11` instead of `raw_rebuild_validation`

### Pipeline run completed

- Re-ran:

```bash
python3.11 data_work/src/stages/run_full_pipeline.py \
  --config data_work/config/manual_centerline_plan1.json \
  --version-id manual_centerline_rules_v11 \
  --output-dir data_work/outputs \
  --from-stage stage06 --to-stage stage08 \
  --grid-type square
```

- This regenerated:
  - `grid_links_square_long.csv`
  - `grid_nodes_square.csv`
  - `OD_square_reachable_AM.csv`
  - `qsm_input_nodes_square.parquet`
  - `qsm_input_edges_square.parquet`
  - `qsm_input_od_square.parquet`
  - `qsm_input_parameters_square.json`
  - `data_quality_square_qsm.csv`

### Main retained diagnostics

- `qsm_input_edges_square.parquet` now contains the intended handoff fields:
  - `tau_obs_min`
  - `tau_ff_min`
  - `tau_ff_imputed_flag`
  - `tau_ff_source`
  - `lane_quality_flag`
  - `lanes_directional`
  - `capacity_proxy`
- `data_quality_square_qsm.csv` reports:
  - `n_nodes = 2,736`
  - `n_edges = 4,632`
  - `n_od_pairs = 377,708`
  - `od_total_commuters = 7,598,920`
  - `reachable_od_pair_share = 0.8530`
  - `reachable_od_commuters_share = 0.9543`
  - `edge_tau_ff_imputed_share = 0.0`
  - `edge_missing_lane_share_count = 0.0`
  - `edge_tau_obs_lt_tau_ff_share = 0.1431`

### Smoke test

- A one-iteration runner smoke test now completes successfully on the new default centerline input:
  - `data_work/outputs/manual_centerline_rules_v11/model_square_smoke_qsm_v11`

### Interpretation

- The project now has a usable square-grid `centerline_grid_v11` model handoff on the current locked version.
- The next bottleneck moves downstream from Stage 08 export to the model loader / solver:
  - separate minutes from iceberg objects inside `spatial_equilibrium.py`
  - stop relying on silently overloaded time-field names
  - then extend the same input-bundle logic to other grid systems and the `raw_edgeproj_v2` branch

## 2026-04-23: raw-grid coverage maps and population overlays generated for `raw_edgeproj_v2`

### Purpose

- Produce map-ready diagnostics for the current raw-grid forward branch using the grid system actually attached to that run.
- Quantify how much of the grid system and OD-implied population are covered by finite raw-grid adjacent travel-cost links.

### Latest alignment correction

- This diagnostic block was later refreshed so the retained `raw_edgeproj_v2` output root now points to:
  - `data_work/outputs/manual_centerline_rules_v11`
- The refresh was necessary because:
  - `square` and `hex` grid IDs are unchanged between the older baseline and `manual_centerline_rules_v11`
  - but `voronoi` changes materially under `manual_centerline_rules_v11` (`1,225 -> 1,213` cells), so old population and raw-grid coverage outputs were not version-consistent
- After the refresh:
  - `manual_centerline_rules_v11` now has complete `stage06-08` outputs for `square`, `hex`, and `voronoi`
  - `raw_connected_v1_grid_travel_time_edgeproj_v2` was rerun for both `AM` and `PM` against the `manual_centerline_rules_v11` baseline
  - the coverage figures and summaries were regenerated on top of the refreshed run config

### Code added

- New reusable diagnostic script:
  - `data_work/src/diagnostics/plot_raw_grid_coverage_maps.py`

### Outputs written

- Figures:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/figures/grid_coverage_maps/raw_grid_coverage_square_AM.png`
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/figures/grid_coverage_maps/raw_grid_coverage_square_PM.png`
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/figures/grid_coverage_maps/raw_grid_coverage_hex_AM.png`
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/figures/grid_coverage_maps/raw_grid_coverage_hex_PM.png`
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/figures/grid_coverage_maps/raw_grid_coverage_voronoi_AM.png`
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/figures/grid_coverage_maps/raw_grid_coverage_voronoi_PM.png`
- Summary tables:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/metrics/grid_coverage_maps/raw_grid_coverage_summary_all.csv`
  - one grid-period summary per file under the same directory
- Derived population / coverage tables:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/data/grid_coverage_maps/grid_population_summary_{grid}.csv`
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/data/grid_coverage_maps/grid_population_coverage_{grid}_{period}.csv`
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/data/grid_coverage_maps/raw_grid_connected_edges_{grid}_{period}.csv`
- Metadata note:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/metrics/grid_coverage_maps/README_raw_grid_coverage_maps.json`

### Figure design retained

- The maps intentionally follow the right-hand style of the earlier `aa_style` grid figure:
  - Beijing boundary
  - grid boundary
  - residential population circles at grid centroids
  - finite raw-grid adjacent links colored by travel time
- New layer added for this task:
  - the largest weakly connected component boundary is drawn as a red thick outline

### Coverage definition used

- A grid is counted as covered in a given period if it appears as an endpoint of at least one finite row in:
  - `raw_connected_vs_old_adjacent_adjacent_{grid}_{period}.csv`
- The red outline uses the largest weakly connected component of the undirected graph induced by those finite adjacent pairs.

### Main summary numbers after refresh to `manual_centerline_rules_v11`

- Square:
  - covered grids `= 1,427 / 3,357 = 42.5%`
  - largest-component grids `= 1,153 / 3,357 = 34.3%`
  - covered residents share `= 95.5%`
  - covered jobs share `= 97.2%`
  - OD population with both endpoints in the largest component `= 91.3%`
- Hex:
  - covered grids `= 1,893 / 4,604 = 41.1%` in `AM`
  - covered grids `= 1,893 / 4,604 = 41.1%` and largest-component grids `= 1,571 / 4,604 = 34.1%` in `PM`
  - covered residents share `= 95.6%`
  - covered jobs share `= 96.5%`
  - OD population with both endpoints in the largest component `≈ 90.6%`
- Voronoi:
  - covered grids `= 1,066 / 1,213 = 87.9%`
  - largest-component grids `= 996 / 1,213 = 82.1%`
  - covered residents share `= 97.5%`
  - covered jobs share `= 97.9%`
  - OD population with both endpoints in the largest component `= 94.2%`

### Interpretation

- The raw-grid branch still covers a minority of square / hex cells by count, but those connected cells contain most observed residents and jobs.
- After refreshing to `manual_centerline_rules_v11`, `voronoi` remains much stronger on both grid-count coverage and population coverage, and is now version-consistent with the latest retained grid construction.
- The largest-component red outline is meaningfully smaller than the full set of covered cells for square / hex, confirming that the current raw-grid issue is not only sparse coverage but also fragmentation into multiple connected pieces.

### Later figure-label extension

- The six `raw_grid_coverage_{grid}_{period}.png` figures were later refreshed so the title block now reports both:
  - `Covered grids` / `covered residents`
  - `Maximumconnected covered grids` / `Maximumconnected covered residents`
- The same exact metrics were also written into:
  - `metrics/grid_coverage_maps/raw_grid_coverage_summary_all.csv`
  using explicit alias columns:
  - `maximumconnected_covered_grids`
  - `maximumconnected_covered_grid_share`
  - `maximumconnected_covered_residents`
  - `maximumconnected_covered_resident_share`

## 2026-04-24: grid-level raw-grid tidal asymmetry maps and summary tables generated

### Purpose

- Extend the latest `raw_edgeproj_v2` diagnostics from coverage-only maps to directional asymmetry diagnostics at the adjacent grid-pair level.
- Keep the asymmetry outputs strictly tied to the refreshed `manual_centerline_rules_v11`-aligned raw-grid run.

### Code update

- `data_work/src/diagnostics/plot_raw_grid_coverage_maps.py` was extended so the same post-processing pass now also:
  - reconstructs undirected adjacent grid pairs with both directions observed
  - computes `ab` and `ba` raw travel times
  - computes directional ratio `min(ab, ba) / max(ab, ba)`
  - computes asymmetry intensity `= 1 - ratio`
  - records faster / slower direction and absolute time gap

### Outputs written

- Figures:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/figures/grid_tidal_asymmetry/raw_grid_tidal_asymmetry_square_AM.png`
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/figures/grid_tidal_asymmetry/raw_grid_tidal_asymmetry_square_PM.png`
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/figures/grid_tidal_asymmetry/raw_grid_tidal_asymmetry_hex_AM.png`
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/figures/grid_tidal_asymmetry/raw_grid_tidal_asymmetry_hex_PM.png`
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/figures/grid_tidal_asymmetry/raw_grid_tidal_asymmetry_voronoi_AM.png`
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/figures/grid_tidal_asymmetry/raw_grid_tidal_asymmetry_voronoi_PM.png`
- Pair-level tables:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/data/grid_tidal_asymmetry/raw_grid_tidal_asymmetry_pairs_{grid}_{period}.csv`
- Summary tables:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/metrics/grid_tidal_asymmetry/raw_grid_tidal_asymmetry_summary_all.csv`
  - plus one grid-period summary per file under the same directory

### Summary metrics retained

- Square:
  - `AM` median directional ratio `= 0.793`
  - `PM` median directional ratio `= 0.798`
  - share with ratio `< 0.8`:
    - `AM = 47.3%`
    - `PM = 46.9%`
- Hex:
  - `AM` median directional ratio `= 0.795`
  - `PM` median directional ratio `= 0.798`
  - share with ratio `< 0.8`:
    - `AM = 47.0%`
    - `PM = 46.8%`
- Voronoi:
  - `AM` median directional ratio `= 0.793`
  - `PM` median directional ratio `= 0.810`
  - share with ratio `< 0.8`:
    - `AM = 47.4%`
    - `PM = 44.5%`

### Interpretation

- Under the latest retained raw-grid version, adjacent grid-pair asymmetry remains materially weaker than the centerline benchmark, but it is not negligible.
- The raw-grid directional ratio is centered around roughly `0.79-0.81` across all three grid systems.
- Voronoi again performs best on coverage, while its PM asymmetry distribution is slightly less extreme than AM.

## 2026-04-24: first-pass `model_v1` field alignment completed on `centerline_grid_v11`

### Code updates

- `data_work/src/stages/stage08_build_qsm_inputs.py`
  - now keeps `tau_obs_min` / `tau_ff_min` explicitly alongside legacy `t_min` / `t_ff_min`
  - writes empty placeholder columns for `t_obs_iceberg` / `t_ff_iceberg` so the downstream model layer can fill them deterministically
  - emits an explicit warning when `tau_ff_min` has to be imputed from observed minutes, instead of doing it silently
- `data_work/src/model/spatial_equilibrium.py`
  - `ModelInputs` now separates minute objects from iceberg objects:
    - `edge_tau_obs_min`
    - `edge_tau_ff_min`
    - `edge_t_obs_iceberg`
    - `edge_t_ff_iceberg`
  - loader now reads `tau_obs_min` / `tau_ff_min` first, carries `tau_ff_imputed_flag`, and constructs iceberg costs with explicit `delta0`
  - equilibrium summaries now report:
    - `solver_max_log_change`
    - `edge_tau_ff_imputed_share`
    - `edge_missing_lane_share`
  - congestion update no longer allows equilibrium travel time to fall below free-flow time
- `data_work/src/model/run_spatial_equilibrium.py`
  - adds `--iceberg-delta0`
  - output edge results now save both minute and iceberg costs plus imputation / lane-quality flags
  - calibration output now distinguishes:
    - `theta_diagnostic`
    - `lambda_diagnostic`
    - `theta_used`
    - `lambda_used`
- `data_work/src/model/run_counterfactual_suite.py`
  - updated to use the new minute/iceberg field split

### Verification

- Loader smoke test on `manual_centerline_rules_v11` square inputs completed successfully:
  - `n_nodes = 2736`
  - `n_edges = 4087`
  - `edge_tau_ff_imputed_share = 0.0`
- Smoke runs completed successfully:
  - `data_work/outputs/manual_centerline_rules_v11/model_square_baseline_smoke/`
  - `data_work/outputs/manual_centerline_rules_v11/model_square_baseline_smoke2/`
  - `data_work/outputs/manual_centerline_rules_v11/model_square_suite_smoke/`

### Retained interpretation

- The `centerline_grid_v11` line is now past the pure field-alignment stage: loader and runner can already read the new QSM minute fields and construct iceberg costs consistently.
- The next model task is no longer “edit the loader first”; it is to run and inspect a fuller baseline for convergence and data-quality behavior before deciding whether to keep refining the centerline-grid prototype or switch effort to a `raw_edgeproj_v2` input bundle.

## 2026-04-24: voronoi raw-grid comparison outputs refreshed to the current retained baseline

### Issue fixed

- The voronoi row used in the raw-grid comparison table of `Documents/L1_manuscripts/TidalLanes_0421.tex` was still tied to an older denominator (`1225` total grids) from the earlier comparison summary chain.
- The current retained baseline uses `1213` voronoi grids, so the comparison outputs needed to be realigned before treating the table as current.

### Code update

- `data_work/src/diagnostics/compare_grid_cost_versions.py`
  - now reads grid totals directly from the current baseline `grid_*_master.parquet` when available
  - no longer relies mechanically on stale `total_grids` values preserved inside older connector-summary metrics

### Refreshed outputs

- `data_work/outputs/comparison_grid_methods/three_version_adjacent_summary_AM.csv`
- `data_work/outputs/comparison_grid_methods/three_version_adjacent_summary_PM.csv`
- `data_work/outputs/comparison_grid_methods/three_version_connector_summary.csv`
- `data_work/outputs/comparison_grid_methods/three_version_full_comparison_AM.csv`
- `data_work/outputs/comparison_grid_methods/three_version_full_comparison_PM.csv`

### Retained voronoi values now in use

- Connector share:
  - `raw_node = 985 / 1213 = 0.812`
  - `raw_edgeproj_v2 = 1097 / 1213 = 0.904`
- Mean connector distance:
  - `raw_node = 676 m`
  - `raw_edgeproj_v2 = 414 m`
- AM adjacent travel time medians:
  - `centerline = 5.86 min`
  - `raw_node = 8.77 min`
  - `raw_edgeproj_v2 = 10.24 min`
- AM median directional ratio:
  - `centerline = 0.904`
  - `raw_node = 0.882`
  - `raw_edgeproj_v2 = 0.793`

### Manuscript sync

- `Documents/L1_manuscripts/TidalLanes_0421.tex` voronoi row in Table `rawgrid-method-comparison` was updated to the refreshed values.
- The accompanying sentence was tightened so it no longer describes the old voronoi comparison as if it matched the square/hex pattern exactly.

## 2026-04-26: `Replication_apply` workspace created for direct AA-format application to current square-grid data

### What was added

- New self-contained workspace:
  - `Replication_apply/`
- Preserved original Matlab snapshot:
  - `Replication_apply/original_matlab_code_snapshot/`
- Working application copies:
  - `Replication_apply/analysis/`
  - `Replication_apply/counterfactuals/seattle/`
  - `Replication_apply/python_commuting/`
- Dedicated canonical AA-format input bundle:
  - `Replication_apply/aa_input_square_v1/`
- Input-preparation layer:
  - `Replication_apply/prepare_from_data_work_square_v1/`

### Input bundle generated

- `Replication_apply/aa_input_square_v1/node_lr_lf_seattle.csv`
- `Replication_apply/aa_input_square_v1/sparse_adjmat_seattle.csv`
- `Replication_apply/aa_input_square_v1/sparse_commute_seattle.csv`

The builder also syncs the same three files into:

- `Replication_apply/counterfactuals/seattle/`

so the working Matlab and Python copies can keep AA-style relative paths.

### Current source mapping

- Nodes:
  - `data_work/outputs/manual_centerline_rules_v11/data/qsm_input_nodes_square.parquet`
- Edges:
  - `data_work/outputs/manual_centerline_rules_v11/data/qsm_input_edges_square.parquet`
- OD:
  - `data_work/outputs/manual_centerline_rules_v11/data/qsm_input_od_square.parquet`

### Working first-pass rules

- Node IDs are remapped from local 0-based indices to consecutive AA-style 1-based indices.
- The current square input keeps only nodes active in at least one of:
  - positive `residents` or `jobs`
  - positive `reachable_AM` `commuters_road`
  - AM edge incidence
- The current `traffic` column in `sparse_adjmat_seattle.csv` is a first-pass monotone proxy based on:
  - `tau_obs_min`
  - `tau_ff_min`
  - `capacity_proxy`
- Node longitude and latitude columns are still placeholder zeros in this quick-run version.

### Code adjustments kept minimal

- Original Matlab copies are preserved unchanged under `original_matlab_code_snapshot/`.
- Working Matlab copies under `Replication_apply/` were only adjusted to infer `N` from `node_lr_lf_seattle.csv` rather than hard-coding `217`.
- No other commuting solver logic was intentionally changed at this stage.

### Current runnable status

- Python commuting prediction now runs inside `Replication_apply/` and writes:
  - `Replication_apply/data/seattle/derived/predicted_lij_python.csv`
- Current run size:
  - `n_nodes = 2729`
  - `n_edges = 4087`
  - `n_od_rows = 377708`

### Immediate next use

- Once Matlab licensing is restored, use `Replication_apply/analysis/seattle/predict_commuting.m` as the first Matlab-side validation entrypoint.
- Compare that Matlab output against `Replication_apply/data/seattle/derived/predicted_lij_python.csv` before moving on to counterfactual application.

## 2026-04-29: `MODEL_replication_tidallanes.tex` replication-apply audit

### What was checked

- Compared `Documents/L2_notes/MODEL_replication_tidallanes_PLAN.md` with `Documents/L1_manuscripts/MODEL_replication_tidallanes.tex`.
- Verified current `Replication_apply` scripts and outputs:
  - `Replication_apply/python_commuting/tidal_lane_specC.py`
  - `Replication_apply/prepare_from_data_work_square_v2/build_inputs_from_data_work_square_v2.py`
  - `Replication_apply/results/tidal_lane/selected_pairs_specC.csv`
  - `Replication_apply/results/tidal_lane/chi_lr_lf_specC.csv`
- Verified the Beijing square-grid source inputs under:
  - `data_work/outputs/manual_centerline_rules_v11/data/`

### Locked findings

- Current Spec C uses `Replication_apply/aa_input_square_v2/`, synced into `Replication_apply/counterfactuals/seattle/`.
- The current Beijing source version remains `manual_centerline_rules_v11`, square grid, AM period, `reachable_AM` OD sample.
- The current AA-style input sizes are:
  - nodes: `2729`
  - sparse adjacency / traffic rows: `4087`
  - OD rows: `377708`
- `aa_input_square_v2` differs materially from `aa_input_square_v1` in the `sparse_adjmat_seattle.csv` traffic input; v2 uses lane-priority plus BPR inversion rather than the first-pass capacity proxy.
- `MODEL_replication_tidallanes.tex` should not describe `sparse_adjmat_seattle.csv` as an iceberg-cost or `tau_ij` file. In the current Python port, the third column is read as `Xi_ij`, the edge-level baseline traffic input.
- Traceability note: the interim `99` treated-pair count in this audit was later corrected by the grid-ID remapping fix documented below.

### Audit note

- Detailed note saved at:
  - `Documents/L2_notes/MODEL_replication_tidallanes_audit_20260429.md`

## 2026-04-29: `Replication_apply` lane-source consistency fixed for Spec C

### Code updates

- `Replication_apply/prepare_from_data_work_square_v2/build_inputs_from_data_work_square_v2.py`
  - removed the fallback from raw-edge-projection path lane estimates to centerline `lanes_directional`;
  - current lane priority is:
    1. `lane_est_path_raw_edgeproj_len_weighted`
    2. `lane_est_path_raw_edgeproj_bottleneck`
    3. numeric fallback `2.0`
- `Replication_apply/python_commuting/tidal_lane_specC.py`
  - now merges the same raw-edge-projection path lane estimates before building the treated-pair BPR shock;
  - exports `lane_source_ij`, `lane_source_ji`, `lane_source_slow`, and `lane_source_fast` in `selected_pairs_specC.csv`;
  - no treated direction uses centerline `lanes_directional`.

### Rebuilt inputs and results

- Rebuilt `Replication_apply/aa_input_square_v2/` and synced it to `Replication_apply/counterfactuals/seattle/`.
- New baseline lane-source counts for the 4,087 exported directed edges:
  - raw-edge-projection length-weighted lane: `3304`
  - raw-edge-projection bottleneck lane: `0`
  - numeric fallback `2.0`: `783`
- Reran `tidal_lane_specC.py`.
- Traceability note: this intermediate run still showed `99` treated pairs before the endpoint remapping fix documented below.

### Updated Spec C result

- `t_bar_hat` slow range: `[0.8205, 0.8756]`
- `t_bar_hat` fast range: `[1.2009, 1.5058]`
- Average slow-direction travel-time change: `-12.8%`
- Average fast-direction travel-time change: `+21.7%`
- `chi_hat = 0.9994614034`
- `(chi_hat - 1) * 1e6 = -538.5966`
- Resident max gain/loss:
  - `sq_26_50`: `+50.6%`
  - `sq_14_10`: `-20.3%`
- Worker max gain/loss:
  - `sq_36_65`: `+43.5%`
  - `sq_53_46`: `-23.4%`

### Manuscript and figures

- Updated `Documents/L1_manuscripts/MODEL_replication_tidallanes.tex`.
- Regenerated:
  - `Documents/L3_figs/fig1_tbar_shock.{pdf,png}`
  - `Documents/L3_figs/fig2_population_changes.{pdf,png}`
  - `Documents/L3_figs/fig3_tau_obs.{pdf,png}`
- Recompiled `MODEL_replication_tidallanes.pdf` successfully.

## 2026-04-29: AA node mapping corrected to use grid IDs rather than stale edge `i/j`

### Issue

- `qsm_input_edges_square.parquet` carries `i/j` labels from the stage06 grid-edge export.
- `qsm_input_nodes_square.parquet` carries `node_i` labels from the stage07 OD-and-population node map.
- These are different numbering systems. For example:
  - edge row `sq_7_47 -> sq_8_47` had `i=3247`, `j=3295`;
  - the node table maps `sq_7_47 -> 1697`, `sq_8_47 -> 1725`.
- Therefore the previous `Replication_apply` builder was incorrectly relying on stale edge-level `i/j` labels instead of the stable `grid_o/grid_d` identifiers.

### Fix

- `Replication_apply/prepare_from_data_work_square_v2/build_inputs_from_data_work_square_v2.py`
  - now builds active nodes from grid IDs;
  - maps edge endpoints by `grid_o/grid_d -> node_i_aa_1based`;
  - keeps the raw-edge-projection lane hierarchy introduced above.
- `Replication_apply/python_commuting/tidal_lane_specC.py`
  - now maps treated-pair shocks by `grid_o/grid_d -> node_i_aa_1based`;
  - no longer uses raw `i/j` labels for AA matrix placement.

### Rebuilt results

- Rebuilt `aa_input_square_v2`:
  - nodes: `2736`
  - edges: `4632`
  - OD rows: `377708`
- Re-ran Spec C:
  - treated pairs: `100`
  - skipped pairs: `0`
  - asymmetry ratio range: `2.08--14.76`
  - `chi_hat = 0.9990852297`
  - `(chi_hat - 1) * 1e6 = -914.7703`
- Updated `MODEL_replication_tidallanes.tex`, regenerated the three Spec C figures, and recompiled the PDF.

## 2026-04-29: `edgeproj_v2` travel costs wired into `Replication_apply`

### Purpose

- Use the retained `raw_connected_v1_grid_travel_time_edgeproj_v2` path definition as the model edge-cost source.
- Keep observed time, free-flow time, lane proxy, and BPR traffic inversion on the same raw-edge-projection path.
- Stop mixing stage06 centerline-grid `tau_obs_min` / `tau_ff_min` with raw-edge-projection lane estimates in the AA input builder.

### Code updates

- `data_work/src/raw_connected_v1/grid_travel_time/build_raw_connected_v1_grid_travel_time_edgeproj.py`
  - now attaches raw-road free-flow speed from the 22:00--05:00 speed table;
  - computes path-level `raw_connected_tau_ff_min` along the same realized pair-specific path used for `raw_connected_travel_time_min`;
  - exports model-facing aliases `tau_obs_min` and `tau_ff_min`;
  - exports `lane_resolved_for_bpr` and `xi_bpr_edgeproj`;
  - records the observed-time, free-flow-time, and BPR definitions in `run_config.json`.
- `Replication_apply/prepare_from_data_work_square_v2/build_inputs_from_data_work_square_v2.py`
  - now reads square AM edge costs from `raw_connected_v1_grid_travel_time_edgeproj_v2`;
  - maps `home_grid/work_grid` to AA node IDs through the existing grid mapping;
  - computes `Xi_ij = lanes * max(tau_obs/tau_ff, 1)^(1/delta1)` using raw-edge-projection path costs.
- `Replication_apply/python_commuting/tidal_lane_specC.py`
  - now builds the treated-pair asymmetry table from the same raw-edge-projection square AM file;
  - uses raw-path `tau_obs_min`, raw-path `tau_ff_min`, and raw-path lane estimates for the Spec C BPR shock.

### Refreshed outputs

- Rebuilt in place for `square`, `hex`, and `voronoi`, both `AM` and `PM`:
  - `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/data/raw_connected_vs_old_adjacent_adjacent_{grid}_{period}.csv`
- Rebuilt AA inputs:
  - `Replication_apply/aa_input_square_v2/node_lr_lf_seattle.csv`
  - `Replication_apply/aa_input_square_v2/sparse_adjmat_seattle.csv`
  - `Replication_apply/aa_input_square_v2/sparse_commute_seattle.csv`
  - synced copies under `Replication_apply/counterfactuals/seattle/`
- Rebuilt metadata:
  - `Replication_apply/prepare_from_data_work_square_v2/build_summary_square.json`
  - `Replication_apply/prepare_from_data_work_square_v2/node_index_mapping_square.csv`
- Reran:
  - `Replication_apply/python_commuting/tidal_lane_specC.py`

### Retained diagnostics

- `raw_connected_vs_old_adjacent_adjacent_square_AM.csv`:
  - rows: `4,632`
  - finite raw-edge-projection observed/free-flow paths: `3,666`
  - mean `tau_obs_min`: `14.91`
  - mean `tau_ff_min`: `11.55`
  - finite `xi_bpr_edgeproj`: `3,666`
- Rebuilt AA input:
  - nodes: `2,723`
  - edge rows: `3,666`
  - OD rows: `377,708`
  - lane sources among exported edges:
    - raw-edge-projection length-weighted lane: `3,664`
    - raw-edge-projection bottleneck lane: `0`
    - numeric fallback `2.0`: `2`
- Spec C after the raw-edge-projection switch:
  - candidate pairs after the length filter: `1,763`
  - treated pairs: `100`
  - skipped pairs: `0`
  - asymmetry ratio range: `3.24--25.17`
  - `chi_hat = 0.9998669097`
  - `(chi_hat - 1) * 1e6 = -133.0903`

### Interpretation

- `manual_centerline_rules_v11` remains the source for grid IDs, node population, and reachable OD.
- Model edge costs now come from `raw_connected_v1_grid_travel_time_edgeproj_v2`, not from stage06 centerline-grid travel costs.
- The current free-flow cost is not `old_travel_time_min`; it is the raw-edge-projection path time recomputed with free-flow speeds on the same realized path.
- `old_travel_time_min` remains only a comparison field against the prior stage06 adjacent-grid benchmark.
- The manuscript and figures were not refreshed in this pass, so any paper-facing Spec C numbers should be updated before circulation.

## 2026-04-29: Spec C Figure 5/6 analogue plots generated

### Purpose

- Create current-result figures analogous to the original paper's link-level welfare maps and congestion comparison figures, using the already retained Spec C top-100 tidal-lane counterfactual.
- Scope note: this is not a strict per-link 1% marginal welfare elasticity run. It visualizes the current Spec C treated-link shock, equilibrium population response, and shock diagnostics.

### Code and inputs

- Added:
  - `Replication_apply/python_commuting/plot_tidal_lane_specC_results.py`
- Read:
  - `Replication_apply/results/tidal_lane/selected_pairs_specC.csv`
  - `Replication_apply/results/tidal_lane/chi_lr_lf_specC.csv`
  - `Replication_apply/prepare_from_data_work_square_v2/node_index_mapping_square.csv`
  - `data_work/outputs/manual_centerline_rules_v11/data/grid_square_master.parquet`

### Outputs

- Figures:
  - `Replication_apply/results/tidal_lane/figures/specC_treated_link_shock_map.{png,pdf}`
  - `Replication_apply/results/tidal_lane/figures/specC_population_response_map.{png,pdf}`
  - `Replication_apply/results/tidal_lane/figures/specC_shock_scatter.{png,pdf}`
  - `Replication_apply/results/tidal_lane/figures/specC_results_figure5_6_analogue.{png,pdf}`
- Summary:
  - `Replication_apply/results/tidal_lane/specC_plot_summary.csv`

### Retained numbers

- `chi_hat = 0.9998669097`
- Welfare gain: `133.0903` ppm
- Treated pairs: `100`
- Directed treated links: `200`
- Asymmetry ratio range: `3.24--25.17`; median `4.60`
- Mean slow-direction cost reduction: `10.59%`
- Mean fast-direction cost increase: `20.69%`

## 2026-04-29: `MODEL_replication_tidallanes.tex` narrowed to Spec C only

- Removed the section comparing multiple counterfactual specifications from `Documents/L1_manuscripts/MODEL_replication_tidallanes.tex`.
- Removed the explanatory text about earlier alternative specifications and the separate symmetric-versus-asymmetric counterfactual discussion.
- Updated the future-work roadmap so it no longer lists full-symmetrisation as a planned counterfactual.
- Recompiled `Documents/L1_manuscripts/MODEL_replication_tidallanes.pdf` with `pdflatex`.

## 2026-04-30: `MODEL_replication_tidallanes.tex` Sections 1--2 tightened

- Revised Sections 1--2 of `Documents/L1_manuscripts/MODEL_replication_tidallanes.tex` to reduce code-path detail and foreground the model contract.
- Retained the core data objects: AA sparse inputs, Beijing node/OD/edge counts, directed raw-path travel times, free-flow times, lane proxy, BPR traffic-state input, and solver outputs.
- Reframed the solver description around the minimal baseline and counterfactual fixed-point logic rather than named implementation files.
- Recompiled `Documents/L1_manuscripts/MODEL_replication_tidallanes.pdf` with `pdflatex`.

## 2026-04-30: `MODEL_replication_tidallanes.tex` Section 4 simplified

- Removed the old Section 4.1, 4.2, and 4.4 subsections from `Documents/L1_manuscripts/MODEL_replication_tidallanes.tex`.
- Condensed the former Section 4.3 into a short refinement section that states the current exact-hat scope, the value of richer Beijing OD and travel-time data, and the main path toward fundamentals recovery or Wardrop-consistent traffic assignment.
- Recompiled `Documents/L1_manuscripts/MODEL_replication_tidallanes.pdf` with `pdflatex`.

## 2026-04-30: `MODEL_replication_tidallanes.tex` Section 3 simplified

- Rewrote Section 3 of `Documents/L1_manuscripts/MODEL_replication_tidallanes.tex` around four points: what Spec C changes, how it is applied, what remains fixed, and what the directional / welfare / location results are.
- Removed the separate treated-link observed-time figure and population-response figure from the appendix.
- Initially retained only the cost-shock distribution figure (`fig1_tbar_shock`); this was superseded by the later figure-selection update below.
- Recompiled `Documents/L1_manuscripts/MODEL_replication_tidallanes.pdf` with `pdflatex`; the document is now 8 pages.

## 2026-04-30: `MODEL_replication_tidallanes.tex` spatial-response interpretation added

- Added `Replication_apply/python_commuting/analyze_specC_spatial_response.py`.
- The script computes Spec C resident and workplace responses by distance from the active square-grid system's geometric centre and exports:
  - `Replication_apply/results/tidal_lane/specC_spatial_response_summary.csv`
  - `Replication_apply/results/tidal_lane/specC_spatial_response_by_distance.csv`
  - `Replication_apply/results/tidal_lane/specC_node_population_changes.csv`
- Generated a treated grid-link map:
  - `Documents/L3_figs/fig4_treated_grid_links.{pdf,png}`
- Generated node-level people-change scatter plots:
  - `Documents/L3_figs/fig5_resident_change_by_distance.{pdf,png}`
  - `Documents/L3_figs/fig6_worker_change_by_distance.{pdf,png}`
- The distance scatter plots now include LOWESS nonparametric fitted curves, and the manuscript defines the model centre as the centroid of the union of all active square-grid polygons.
- Updated Section 3 of `Documents/L1_manuscripts/MODEL_replication_tidallanes.tex` to replace the old node-extrema population table with a distance-to-centre interpretation and table.
- Updated the appendix to include the treated asymmetry scatter, the treated grid-link map, and separate resident/worker distance scatter plots.
- Recompiled `Documents/L1_manuscripts/MODEL_replication_tidallanes.pdf` with `pdflatex`; the document is now 11 pages.
