"""
All-purpose RN traffic on gate5 edges x 10 minutes (Speed 2025-05-19..25 counts)
================================================================================

Reads Speed's formal RN pressure counts (read-only; passage date x directed RN
pair x 6-hour block, 36 ten-minute values per count) and maps
``n_valid_transitions`` onto gate5 directed edges with the length-allocation
rule of Platform/utils/map_rn_to_model_edges.length_alloc. Counts are all
clean mode1 trips, every purpose, unweighted sample passes; Speed applied no
minimum-cell rule, so only edge-level sums are written here, and edge x slot
rows below 5 allocated passes are dropped from the written panel (they are kept
in the totals of the summary).

Output: data_work/outputs/<output-name>/rn_all/ (refuses to overwrite)
    edge_flow_10min.parquet, rn_all_build_summary.json
"""
from __future__ import annotations

import argparse
import glob
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]   # -> data_work/
REPO = ROOT.parent
sys.path.insert(0, str(REPO / "Platform" / "utils"))
import map_rn_to_model_edges as rnmap      # noqa: E402

SPEED = Path("/Users/fxr/Library/CloudStorage/Dropbox/Projects/Projects_FengGu/Speed/Road_processing/GPS data/Speed202505")
COUNTS = SPEED / "03_rn_pair_10min_pressure/exports/formal_counts"
EDGES = ROOT / "outputs/model_input_edgeproj_v2_lcc_gate5_v1/data/qsm_input_edges_square.parquet"
MIN_WRITE = 5


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--output-name", default="all_traffic_speed_panel_rn202505_gate5_v1")
    args = p.parse_args(argv)
    out = ROOT / "outputs" / args.output_name / "rn_all"
    if out.exists():
        raise SystemExit(f"{out} exists; choose a new --output-name")
    out.mkdir(parents=True)

    meta = json.loads((REPO / "Platform/reference_inputs/manifest.json").read_text())
    nodes = pd.concat([pd.read_csv(f, dtype={"rn_id": str}) for f in sorted(rnmap.SPEED_DICT.glob("rn_node_dictionary_202505_b*.csv"))])
    nodes = nodes[nodes.coordinate_missing.eq(0)].drop_duplicates("rn_id").set_index("rn_id")
    edges = pd.read_parquet(EDGES, columns=["grid_o", "grid_d"])
    edge_key = set(zip(edges.grid_o, edges.grid_d))

    cols = [f"n_valid_transitions_s{s:02d}" for s in range(36)]
    files = sorted(glob.glob(str(COUNTS / "rn_pressure_counts_*.csv")))
    pairs = pd.DataFrame({"rn_pair_id": pd.unique(np.concatenate(
        [pd.read_csv(f, usecols=["rn_pair_id"], dtype=str, encoding="utf-8-sig").rn_pair_id.unique() for f in files]))})
    pairs[["rn_from", "rn_to"]] = pairs.rn_pair_id.str.split("_", n=1, expand=True)
    for end in ["from", "to"]:
        n = nodes.reindex(pairs[f"rn_{end}"])
        pairs[f"lon_{end}"], pairs[f"lat_{end}"] = n.lon.values, n.lat.values
    no_coord = set(pairs.rn_pair_id[pairs.lon_from.isna() | pairs.lon_to.isna()])
    al = rnmap.length_alloc(pairs, meta["x_origin_3857"], meta["y_origin_3857"], edge_key)[["rn_pair_id", "grid_from", "grid_to", "cells", "allocated"]]
    print(f"pairs {len(pairs):,}; no coordinate {len(no_coord):,}; allocation rows {len(al):,}", flush=True)

    parts, tot = [], {"passes_all": 0.0, "passes_no_coord": 0.0, "length_flow_all": 0.0, "length_flow_on_model_edges": 0.0}
    for i, f in enumerate(files):
        d = pd.read_csv(f, usecols=["date", "rn_pair_id", "time_block_6h", *cols], dtype={"rn_pair_id": str}, encoding="utf-8-sig")
        long = d.melt(id_vars=["date", "rn_pair_id", "time_block_6h"], value_vars=cols, var_name="s", value_name="n").dropna(subset=["n"])
        long["slot10"] = (long.time_block_6h * 36 + long.s.str[-2:].astype(int)).astype("int16")
        tot["passes_all"] += float(long.n.sum())
        tot["passes_no_coord"] += float(long.n[long.rn_pair_id.isin(no_coord)].sum())
        m = long.merge(al, on="rn_pair_id")
        m["flow"] = m.n * m.cells
        tot["length_flow_all"] += float(m.flow.sum())
        m = m[m.allocated]
        tot["length_flow_on_model_edges"] += float(m.flow.sum())
        parts.append(m.groupby(["grid_from", "grid_to", "date", "slot10"]).flow.sum().reset_index())
        if i % 25 == 0:
            print(f"{i + 1}/{len(files)} {Path(f).name}", flush=True)

    e = pd.concat(parts).groupby(["grid_from", "grid_to", "date", "slot10"], as_index=False).flow.sum()
    e.rename(columns={"flow": "len_flow_all"}, inplace=True)
    summary = {"files": len(files), **{k: round(v) for k, v in tot.items()},
               "share_length_flow_on_model_edges": round(tot["length_flow_on_model_edges"] / tot["length_flow_all"], 4),
               "edge_slot_rows": int(len(e)), "edge_slot_rows_below_5_dropped": int((e.len_flow_all < MIN_WRITE).sum()),
               "edges_with_flow": int(e.groupby(["grid_from", "grid_to"]).ngroups),
               "dates": sorted(int(x) for x in e.date.unique())}
    e[e.len_flow_all >= MIN_WRITE].to_parquet(out / "edge_flow_10min.parquet", index=False)
    (out / "rn_all_build_summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
