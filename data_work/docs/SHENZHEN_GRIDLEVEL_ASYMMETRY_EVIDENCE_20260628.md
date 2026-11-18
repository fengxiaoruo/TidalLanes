# Shenzhen Grid-Level Directional Asymmetry Evidence

This note records the retained grid-level evidence built from the Shenzhen `fishnet1000` bidirectional travel-time matrix. The role of this dataset is to fill the middle layer between township-level OD evidence and segment-level road evidence. It is a within-city, all-pair OD cost layer.

## 1. Input And Code Path

Code:

- `data_work/src/shenzhen_gridlevel/generate_gridlevel_asymmetry_evidence.py`

Raw input:

- `data_work/raw_data/shenzhen_gridlevel/fishnet1000_pairtime_hat.csv`

Retained output root:

- `data_work/outputs/shenzhen_gridlevel_asymmetry_evidence_v1/`

The script uses `car_time` as the main travel-time field, following the provider guidance. `car_time_detour` is not used in this evidence layer. The sample is restricted to unordered reciprocal grid pairs where both directed `car_time` values are at least 1 minute.

## 2. Unit Of Observation

The raw file is a complete `1711 x 1711` matrix. Each row is one directed grid OD pair:

- `fid_home -> fid_work`
- `fid_work -> fid_home`

The evidence script merges each directed OD with its reverse direction and keeps only one unordered pair. This converts the raw directed matrix into reciprocal pair observations:

```text
time_ab = car_time(fid_home -> fid_work)
time_ba = car_time(fid_work -> fid_home)
fast_time = min(time_ab, time_ba)
slow_time = max(time_ab, time_ba)
```

The retained reciprocal sample has 1,462,815 valid grid pairs.

## 3. Main Measures

Two ratios are retained because they answer different questions.

`slow_over_fast_time` measures how much slower the slow direction is:

```text
slow_over_fast_time = slow_time / fast_time
```

`symmetry_ratio_time` puts the metric on the same lower-is-more-asymmetric scale used in existing township and segment evidence:

```text
symmetry_ratio_time = fast_time / slow_time
```

A value of 1 means the two directions have the same travel time. A value of 0.80 means the faster direction takes only 80 percent of the slower direction's time. In slow-over-fast terms, this is equivalent to `1 / 0.80 = 1.25`, so the slow direction takes 25 percent longer.

The script also records:

- `time_gap_min = slow_time - fast_time`
- `mean_dist_km = (car_dist + reverse car_dist) / 2`
- distance-bin summaries based on `mean_dist_km`

## 4. Headline Results

For the 1,462,815 reciprocal grid pairs:

- Median `slow_over_fast_time`: 1.137.
- Median `symmetry_ratio_time`: 0.879.
- Share with `symmetry_ratio_time <= 0.90`: 57.5 percent.
- Share with `symmetry_ratio_time <= 0.83`: 35.1 percent.
- Share with `symmetry_ratio_time <= 0.77`: 21.7 percent.
- Share with `symmetry_ratio_time <= 0.50`: 0.91 percent.
- Median absolute directional time gap: 6.62 minutes.

The threshold interpretation is direct. More than half of within-city grid pairs have at least an 11.1 percent travel-time gap between the two directions. About one fifth have at least a 29.9 percent gap.

## 5. Distance Gradient

The grid-level evidence is strongest in relative terms for shorter OD pairs:

- 0 to 5 km pairs have median `symmetry_ratio_time` of 0.833, with 68.1 percent below the 0.90 threshold.
- 20 to 40 km pairs have median `symmetry_ratio_time` of 0.866, with 61.4 percent below the 0.90 threshold.
- 80 km and above pairs have median `symmetry_ratio_time` of 0.920, with 39.2 percent below the 0.90 threshold.

This pattern is useful for interpretation. Very local OD pairs can have substantial directional route or network differences, while longer OD pairs average over more links and become more symmetric in proportional terms. The absolute time gap remains economically meaningful, because longer trips can have larger minute gaps even when the proportional gap falls.

## 6. Spatial-Layer Bridge

The retained bridge table puts four layers on a common symmetry-ratio scale:

- Township OD, national top-50 AM sample: median ratio 0.903, 52.2 percent below 0.90.
- Shenzhen grid OD, fishnet1000 all reciprocal pairs: median ratio 0.879, 57.5 percent below 0.90.
- Beijing adjacent grid, raw-connected square cells AM: median ratio 0.728, 68.8 percent below 0.90.
- Beijing centerline segment AM: median ratio 0.884, 54.7 percent below 0.90.

The grid-level result is the middle layer in the research design. It is more granular than township OD and captures within-city OD cost heterogeneity, but it is less local than road-segment evidence because it aggregates each route into an OD travel time.

## 7. Retained Figures

Grid-level figures:

- `data_work/outputs/shenzhen_gridlevel_asymmetry_evidence_v1/figures/figure_gridlevel_symmetry_ratio_hist.png`
- `data_work/outputs/shenzhen_gridlevel_asymmetry_evidence_v1/figures/figure_gridlevel_threshold_curve.png`
- `data_work/outputs/shenzhen_gridlevel_asymmetry_evidence_v1/figures/figure_gridlevel_asymmetry_by_distance_bin.png`
- `data_work/outputs/shenzhen_gridlevel_asymmetry_evidence_v1/figures/figure_gridlevel_reciprocal_time_hexbin.png`

Layer-bridge figures:

- `data_work/outputs/shenzhen_gridlevel_asymmetry_evidence_v1/figures/figure_multilevel_asymmetry_bridge.png`
- `data_work/outputs/shenzhen_gridlevel_asymmetry_evidence_v1/figures/figure_multilevel_asymmetry_threshold_shares.png`

Retained tables:

- `data_work/outputs/shenzhen_gridlevel_asymmetry_evidence_v1/metrics/gridlevel_asymmetry_summary.json`
- `data_work/outputs/shenzhen_gridlevel_asymmetry_evidence_v1/metrics/gridlevel_threshold_table.csv`
- `data_work/outputs/shenzhen_gridlevel_asymmetry_evidence_v1/metrics/gridlevel_distance_bin_summary.csv`
- `data_work/outputs/shenzhen_gridlevel_asymmetry_evidence_v1/metrics/multilevel_bridge_summary.csv`

## 8. Current Caveats

The grid-level file still lacks geometry and an ID crosswalk. This means the current run cannot produce a map, cannot attach grid-level residents or jobs, and cannot flow-weight the OD-pair distribution.

The evidence is unweighted. It describes the prevalence of directionality across potential grid pairs, not the prevalence faced by actual commuters. Once the `fishnet1000` geometry or crosswalk is found, the next version should attach observed commuter flows and compare unweighted versus flow-weighted asymmetry.

The dataset is static. It does not distinguish AM, PM, off-peak, or day-level variation. It therefore supports the general claim that within-city OD travel costs are directionally asymmetric, but it does not identify tidal-lane policy timing or peak-period congestion dynamics.

## 9. Next Tests

The next empirical step is to connect the fishnet1000 IDs to real spatial and commuter information:

1. Find fishnet1000 geometry or a crosswalk from `fid_home` / `fid_work` to grid500.
2. Attach observed OD commuter flows and recompute all ratios with flow weights.
3. Split OD pairs by home-work direction, CBD orientation, river or highway barrier crossings, and local network density.
4. Compare the grid-level static pattern with segment-level peak-period speed asymmetry in Shenzhen corridors.
5. Use the flow-weighted grid evidence as the bridge between descriptive directionality and the simplified directional-cost model.

