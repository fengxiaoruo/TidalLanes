# Raw Topology Prototype

This module is a standalone prototype for repairing the topology of the raw
Beijing road network without mixing code into the existing stage pipeline.

## Scope

The current prototype focuses on three tasks:

1. clean the raw road shapefile into a projected, deduplicated baseline
2. snap nearby endpoints and node the network at exact geometric intersections
3. export graph diagnostics for connectivity and dead ends

## Entry Point

```bash
python -m src.raw_topology.run_raw_topology_pipeline
```

Outputs are written by default to:

- `data_work/outputs/raw_topology_mvp/data`
- `data_work/outputs/raw_topology_mvp/metrics`

## Current Limits

- This is an MVP topology repair, not a final routing graph.
- Overpass and underpass false joins are not yet screened out.
- Directionality is not yet encoded into the graph edges.
- Source lineage is assigned by local geometric overlap, which is good enough
  for diagnostics but should be tightened before production routing.
