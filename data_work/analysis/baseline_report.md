# Baseline Diagnostics Report

Generated from existing intermediate datasets only.

## Overview

- Split-segment match rate: 98.43%
- Raw-edge match rate (any split matched): 98.46%
- Total centerline length: 18,884,370.474 m
- Matched centerline length share: 78.91%
- Mean speed: 36.075 km/h
- Median speed: 33.170 km/h

## Speed Distribution

| Metric | Value |
|---|---|
| `mean_speed_kmh` | 36.075 km/h |
| `median_speed_kmh` | 33.170 km/h |
| `p10_speed_kmh` | 23.828 km/h |
| `p90_speed_kmh` | 53.206 km/h |

### Histogram Bins

| Bin | Count | Range |
|---|---:|---|
| `bin_00_count` | 3854 | [1.094, 11.531) km/h |
| `bin_01_count` | 16013 | [11.531, 21.968) km/h |
| `bin_02_count` | 165429 | [21.968, 32.405) km/h |
| `bin_03_count` | 108105 | [32.405, 42.842) km/h |
| `bin_04_count` | 59949 | [42.842, 53.279) km/h |
| `bin_05_count` | 27371 | [53.279, 63.716) km/h |
| `bin_06_count` | 7900 | [63.716, 74.153) km/h |
| `bin_07_count` | 2502 | [74.153, 84.590) km/h |
| `bin_08_count` | 882 | [84.590, 95.027) km/h |
| `bin_09_count` | 255 | [95.027, 105.464) km/h |

## Grid Connectivity

| Scope | Nodes | Edges | Mean Degree | Note |
|---|---:|---:|---:|---|
| `hex3km_AM` | 2057 | 5841 | 2.907 |  |
| `hex3km_PM` |  |  |  | Missing edge file: t_edges_hex3km_PM.csv |
| `square3km_AM` | 1591 | 4146 | 2.666 |  |
| `square3km_PM` |  |  |  | Missing edge file: t_edges_square3km_PM.csv |
| `voronoi3km_AM` | 1167 | 3792 | 3.296 |  |
| `voronoi3km_PM` |  |  |  | Missing edge file: t_edges_voronoi3km_PM.csv |

## Grid-to-Grid Travel Time

| Grid | Mean | Median | P10 | P90 | Max |
|---|---:|---:|---:|---:|---:|
| `hex3km` | 2.698 | 2.399 | 0.474 | 4.933 | 69.936 |
| `square3km` | 3.452 | 3.020 | 0.635 | 6.308 | 74.917 |
| `voronoi3km` | 3.597 | 2.945 | 0.926 | 6.754 | 67.543 |

## Missing Inputs

- `hex3km_PM`: Missing edge file: t_edges_hex3km_PM.csv
- `square3km_PM`: Missing edge file: t_edges_square3km_PM.csv
- `voronoi3km_PM`: Missing edge file: t_edges_voronoi3km_PM.csv
