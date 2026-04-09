## Kunpeng Grid Travel Time

This folder builds grid-to-grid AM travel time directly from the Kunpeng
directed road network without modifying any files under
`data_work/src/fromKunpeng/New_Strategy/Processed_Data/`.

### Inputs

- `data_work/src/fromKunpeng/New_Strategy/Processed_Data/directed_edges.xlsx`
- `data_work/src/fromKunpeng/New_Strategy/Processed_Data/nodes.xlsx`
- `data_work/raw_data/speed_Beijing_all_wgs84.csv`
- baseline grids / OD tables / old grid-graph travel costs from
  `data_work/outputs/match_projection_complete_v7_latest/data`

### Main idea

1. Use Kunpeng's directed road graph as the routing network.
2. Aggregate Baidu speed observations to segment-level AM / PM / FF speeds.
3. Remove problematic self-loops and non-positive-cost edges.
4. Attach each grid centroid to several nearby usable road nodes.
5. Run shortest-path routing on the augmented graph.
6. Compare the resulting grid-to-grid travel time against the existing
   grid-graph-based travel cost.

### Outputs

By default results are written to:

- `data_work/outputs/kunpeng_grid_travel_time_v1/data`
- `data_work/outputs/kunpeng_grid_travel_time_v1/metrics`

### Example

```bash
python -m src.fromKunpeng.grid_travel_time.build_kunpeng_grid_travel_time
```
