"""
Edge x 10-minute pace index from the raw 2025-05-19..25 speed file (gate5)
==========================================================================

Reads ``raw_data/speed_Beijing_all_wgs84.csv`` in chunks (roadseg_id, speed and
the yyyymmddHHMM ``hour`` stamp only) and gives each road segment to gate5
directed edges with the same length-allocation rule used for RN pairs
(Platform/utils/map_rn_to_model_edges.length_alloc): the segment's x and y
displacement, in 3 km cell units, goes to the edge whose centre-to-centre span
holds the segment midpoint. Segment weight on an edge is length_m times its
share of the cell-unit displacement on that axis.

Edge pace (min/km) for a slot is the weight-averaged segment pace 60/speed over
the segments observed in that slot; ``coverage`` is the observed share of the
edge's total segment weight. This is a corridor speed index, not the routed
edgeproj travel time; with edge fixed effects only its variation is used.

Output: data_work/outputs/<output-name>/ (refuses to overwrite)
    segment_edge_weights.parquet, edge_pace_10min.parquet, speed_build_summary.json
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import shapely

ROOT = Path(__file__).resolve().parents[3]   # -> data_work/
REPO = ROOT.parent
sys.path.insert(0, str(REPO / "Platform" / "utils"))
import map_rn_to_model_edges as rnmap      # noqa: E402

SEGMENTS = ROOT / "outputs/raw_rebuild_validation_gridcost_harmonic_v1/data/raw_segment_master.parquet"
EDGES = ROOT / "outputs/model_input_edgeproj_v2_lcc_gate5_v1/data/qsm_input_edges_square.parquet"


def segment_weights(meta: dict) -> pd.DataFrame:
    s = pd.read_parquet(SEGMENTS, columns=["roadseg_id", "geometry", "length_m", "keep_baseline"])
    s = s[s.keep_baseline].drop_duplicates("roadseg_id")
    g = shapely.from_wkb(s.geometry.values)                       # EPSG:3857
    a = shapely.get_coordinates(shapely.get_point(g, 0))
    b = shapely.get_coordinates(shapely.get_point(g, -1))
    # length_alloc works in lon/lat; invert the 3857 projection for the endpoints.
    lon = lambda x: np.degrees(x / rnmap.R)
    lat = lambda y: np.degrees(2 * np.arctan(np.exp(y / rnmap.R)) - np.pi / 2)
    p = pd.DataFrame({"rn_pair_id": s.roadseg_id.values, "lon_from": lon(a[:, 0]), "lat_from": lat(a[:, 1]),
                      "lon_to": lon(b[:, 0]), "lat_to": lat(b[:, 1])})
    edges = pd.read_parquet(EDGES, columns=["grid_o", "grid_d"])
    al = rnmap.length_alloc(p, meta["x_origin_3857"], meta["y_origin_3857"], set(zip(edges.grid_o, edges.grid_d)))
    al = al[al.allocated].rename(columns={"rn_pair_id": "roadseg_id"})
    straight = np.hypot(b[:, 0] - a[:, 0], b[:, 1] - a[:, 1]) / 3000
    share = pd.Series(np.where(straight > 0, 1 / np.maximum(straight, 1e-9), 0), index=s.roadseg_id.values)
    al["w"] = al.cells * al.roadseg_id.map(share) * al.roadseg_id.map(dict(zip(s.roadseg_id, s.length_m)))
    return al[["roadseg_id", "grid_from", "grid_to", "w"]]


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--speed-csv", default=str(ROOT / "raw_data/speed_Beijing_all_wgs84.csv"))
    p.add_argument("--output-name", default="all_traffic_speed_panel_rn202505_gate5_v1")
    p.add_argument("--chunksize", type=int, default=4_000_000)
    args = p.parse_args(argv)
    out = ROOT / "outputs" / args.output_name / "speed"
    if out.exists():
        raise SystemExit(f"{out} exists; choose a new --output-name")
    out.mkdir(parents=True)

    meta = json.loads((REPO / "Platform/reference_inputs/manifest.json").read_text())
    w = segment_weights(meta)
    w.to_parquet(out / "segment_edge_weights.parquet", index=False)
    keep = set(w.roadseg_id)

    parts, n_rows, n_kept = [], 0, 0
    for c in pd.read_csv(args.speed_csv, usecols=["roadseg_id", "speed", "hour"], chunksize=args.chunksize):
        n_rows += len(c)
        c = c[c.roadseg_id.isin(keep) & (c.speed > 0) & c.hour.notna()]
        n_kept += len(c)
        stamp = c.hour.astype("int64")
        c = pd.DataFrame({"roadseg_id": c.roadseg_id.values, "date": (stamp // 10000).astype("int32").values,
                          "slot10": ((stamp // 100 % 100) * 6 + stamp % 100 // 10).astype("int16").values,
                          "pace": (60.0 / c.speed).values})
        m = c.merge(w, on="roadseg_id")
        m["wp"] = m.w * m.pace
        parts.append(m.groupby(["grid_from", "grid_to", "date", "slot10"]).agg(wp=("wp", "sum"), w_obs=("w", "sum")).reset_index())
        print(f"rows {n_rows:,} kept {n_kept:,}", flush=True)

    e = pd.concat(parts).groupby(["grid_from", "grid_to", "date", "slot10"], as_index=False).sum()
    tot = w.groupby(["grid_from", "grid_to"]).w.sum().rename("w_total")
    e = e.merge(tot.reset_index(), on=["grid_from", "grid_to"])
    e["pace_min_per_km"] = e.wp / e.w_obs
    e["coverage"] = e.w_obs / e.w_total
    e[["grid_from", "grid_to", "date", "slot10", "pace_min_per_km", "coverage"]].to_parquet(out / "edge_pace_10min.parquet", index=False)
    summary = {"speed_rows_read": n_rows, "speed_rows_on_allocated_segments": n_kept,
               "segments_allocated": int(w.roadseg_id.nunique()), "edges_with_segments": int(len(tot)),
               "edge_slot_rows": int(len(e)), "dates": sorted(int(d) for d in e.date.unique()),
               "coverage_p10_p50_p90": [round(float(x), 3) for x in e.coverage.quantile([.1, .5, .9])]}
    (out / "speed_build_summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
