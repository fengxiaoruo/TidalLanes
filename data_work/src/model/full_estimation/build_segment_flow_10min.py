"""
Segment x 10-minute RN flow, with a split-sample version
=========================================================

segment_speed_flow_rn202505_v1 stores segment flow only per hour. This script
rebuilds the same flow per 10-minute slot, using the RN-pair -> Baidu segment
match already saved there (segment_rn_match.parquet), and the same rule as
build_segment_speed_flow_panel.py:

    flow(seg, date, slot) = sum_pairs n_valid_transitions * chord_m / sum_pairs chord_m

i.e. the chord-length-weighted mean number of Unicom RN valid transitions over
the segment's matched RN pairs (sample SIM passages per 10 minutes, no expansion).
The denominator is the chord sum over all matched pairs of the segment, as in the
hourly panel, so summing the 6 slots of an hour reproduces segment_flow_hour.

Split sample: the matched pairs of each segment are split into halves A and B by
a hash of rn_pair_id (stable). flow_A, flow_B use the same formula within each
half (denominator = chord sum of that half). Segments with a single pair get
flow_B missing. A and B count different RN pairs, so pair-specific detection
noise is independent between them; the Unicom sampling of which vehicles carry
SIMs is common to both.

Inputs read only:
    outputs/segment_speed_flow_rn202505_v1/segment_rn_match.parquet
    Speed202505/03_rn_pair_10min_pressure/exports/formal_counts/rn_pressure_counts_*.csv
Output: data_work/outputs/<output-name>/segment_flow_10min.parquet, build_summary.json
(refuses to overwrite)
"""
from __future__ import annotations

import argparse
import glob
import json
import zlib
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]   # -> data_work/
SEG = ROOT / "outputs/segment_speed_flow_rn202505_v1"
SPEED = Path("/Users/fxr/Library/CloudStorage/Dropbox/Projects/Projects_FengGu/Speed/Road_processing/GPS data/Speed202505")
COUNTS = SPEED / "03_rn_pair_10min_pressure/exports/formal_counts"


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-name", default="segment_flow_10min_rn202505_v1")
    args = ap.parse_args(argv)
    out = ROOT / "outputs" / args.output_name
    if out.exists():
        raise SystemExit(f"{out} exists; choose a new --output-name")
    out.mkdir(parents=True)

    m = pd.read_parquet(SEG / "segment_rn_match.parquet")
    m["half"] = np.array([zlib.crc32(s.encode()) % 2 for s in m.rn_pair_id], dtype="int8")
    # segments with one pair: put it in A
    one = m.groupby("roadseg_id").rn_pair_id.transform("size") == 1
    m.loc[one, "half"] = 0
    wsum = m.groupby("roadseg_id").chord_m.sum()
    wsum_h = m.groupby(["roadseg_id", "half"]).chord_m.sum().unstack()
    mi = m.set_index("rn_pair_id")

    files = sorted(glob.glob(str(COUNTS / "rn_pressure_counts_*.csv")))
    cols = [f"n_valid_transitions_s{s:02d}" for s in range(36)]
    parts = []
    for i, f in enumerate(files):
        d = pd.read_csv(f, usecols=["date", "rn_pair_id", "time_block_6h", *cols], dtype={"rn_pair_id": str}, encoding="utf-8-sig")
        d = d[d.rn_pair_id.isin(mi.index)]
        long = d.melt(id_vars=["date", "rn_pair_id", "time_block_6h"], value_vars=cols, var_name="s", value_name="n").dropna(subset=["n"])
        long = long[long.n > 0]
        long["slot10"] = (long.time_block_6h * 36 + long.s.str[-2:].astype(int)).astype("int16")
        long["roadseg_id"] = long.rn_pair_id.map(mi.roadseg_id)
        long["half"] = long.rn_pair_id.map(mi.half)
        long["nw"] = long.n * long.rn_pair_id.map(mi.chord_m)
        parts.append(long.groupby(["roadseg_id", "date", "slot10", "half"]).nw.sum().reset_index())
        if i % 25 == 0:
            print(f"rn {i + 1}/{len(files)}", flush=True)
    fl = pd.concat(parts).groupby(["roadseg_id", "date", "slot10", "half"], as_index=False).nw.sum()
    w = fl.pivot_table(index=["roadseg_id", "date", "slot10"], columns="half", values="nw", fill_value=0.0)
    w.columns = ["nwA", "nwB"]
    w = w.reset_index()
    seg = w.roadseg_id
    w["flow"] = (w.nwA + w.nwB) / seg.map(wsum)
    w["flow_A"] = w.nwA / seg.map(wsum_h[0])
    w["flow_B"] = w.nwB / seg.map(wsum_h[1])          # NaN where the segment has no B pair
    w = w.drop(columns=["nwA", "nwB"])
    w["date"] = w.date.astype("int32")
    w.to_parquet(out / "segment_flow_10min.parquet", index=False)

    # check against the hourly panel
    h = w.assign(hour=(w.slot10 // 6).astype("int16")).groupby(["roadseg_id", "date", "hour"], as_index=False).flow.sum()
    ref = pd.read_parquet(SEG / "segment_flow_hour.parquet")
    c = ref.merge(h, on=["roadseg_id", "date", "hour"], how="left", suffixes=("_ref", "_new"))
    summary = {
        "rn_files": len(files), "rows_segment_slot": int(len(w)), "segments": int(w.roadseg_id.nunique()),
        "segments_with_both_halves": int(wsum_h[1].notna().sum()),
        "check_vs_segment_flow_hour": {"rows_ref": int(len(ref)), "rows_matched": int(c.flow_new.notna().sum()),
                                       "max_abs_diff": float((c.flow_ref - c.flow_new).abs().max())},
        "unit": "chord-weighted mean Unicom RN valid transitions per 10 min (sample SIM passages, unexpanded)",
    }
    (out / "build_summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
