"""
Segment-level speed x flow panel: Unicom RN pairs matched to Baidu speed segments
================================================================================

Matching (directed). Each RN pair is a chord between two RN nodes (Speed node
dictionary). A pair is matched to a Baidu speed segment (raw_segment_master,
unsplit raw_geometry, EPSG:3857) if
    - the chord midpoint lies within MAX_DIST_M of the segment line,
    - the chord is at least MIN_CHORD_M long, and
    - the chord heading is within MAX_ANGLE of the segment's local travel
      direction (line tangent at the projected point, signed by the segment's
      8-point semantic direction dir_deg_final).
Among valid candidates the nearest segment wins.

Segment flow is the chord-length-weighted mean of n_valid_transitions over
the segment's matched pairs (vehicles passing the segment, sample units),
written per segment x hour; rows below 5 passes are not written. Segment pace is 60/speed from the raw speed file.

Coverage is reported both ways: the share of RN vehicle-km (passes x chord
length) on matched pairs, and the share of Baidu segments with matched pairs.

Inputs are read only (Speed RN counts and node dictionary, raw speed file).
Output: data_work/outputs/<output-name>/ (refuses to overwrite)
    segment_rn_match.parquet, segment_flow_hour.parquet, segment_pace_10min.parquet,
    segment_meta.parquet, segment_build_summary.json
"""
from __future__ import annotations

import argparse
import glob
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
SPEED = Path("/Users/fxr/Library/CloudStorage/Dropbox/Projects/Projects_FengGu/Speed/Road_processing/GPS data/Speed202505")
COUNTS = SPEED / "03_rn_pair_10min_pressure/exports/formal_counts"
MAX_DIST_M, MIN_CHORD_M, MAX_ANGLE = 20.0, 20.0, 30.0
R = rnmap.R


def angdiff(a, b):
    return np.abs((np.asarray(a) - np.asarray(b) + 180) % 360 - 180)


def heading(dx, dy):
    """Compass heading in degrees (0 = north, 90 = east)."""
    return np.degrees(np.arctan2(dx, dy)) % 360


def rn_pairs(files: list[str]) -> pd.DataFrame:
    nodes = pd.concat([pd.read_csv(f, dtype={"rn_id": str}) for f in sorted(rnmap.SPEED_DICT.glob("rn_node_dictionary_202505_b*.csv"))])
    nodes = nodes[nodes.coordinate_missing.eq(0)].drop_duplicates("rn_id").set_index("rn_id")
    p = pd.DataFrame({"rn_pair_id": pd.unique(np.concatenate(
        [pd.read_csv(f, usecols=["rn_pair_id"], dtype=str, encoding="utf-8-sig").rn_pair_id.unique() for f in files]))})
    p[["rn_from", "rn_to"]] = p.rn_pair_id.str.split("_", n=1, expand=True)
    for end in ["from", "to"]:
        n = nodes.reindex(p[f"rn_{end}"])
        p[f"x_{end}"] = R * np.radians(n.lon.values)
        p[f"y_{end}"] = R * np.log(np.tan(np.pi / 4 + np.radians(n.lat.values) / 2))
    p = p.dropna(subset=["x_from", "x_to"]).reset_index(drop=True)
    p["chord_m"] = np.hypot(p.x_to - p.x_from, p.y_to - p.y_from)
    p["head"] = heading(p.x_to - p.x_from, p.y_to - p.y_from)
    return p


def match(p: pd.DataFrame, seg: pd.DataFrame) -> pd.DataFrame:
    lines = shapely.from_wkb(seg.raw_geometry.values)
    tree = shapely.STRtree(lines)
    q = p[p.chord_m >= MIN_CHORD_M]
    mid = shapely.points((q.x_from + q.x_to).values / 2, (q.y_from + q.y_to).values / 2)
    pi, si = tree.query(mid, predicate="dwithin", distance=MAX_DIST_M)
    c = pd.DataFrame({"i": q.index.values[pi], "s": si})
    pts, ln = mid[pi], lines[si]
    c["dist"] = shapely.distance(pts, ln)
    at = shapely.line_locate_point(ln, pts)
    L = shapely.length(ln)
    a = shapely.get_coordinates(shapely.line_interpolate_point(ln, np.clip(at - 10, 0, L)))
    b = shapely.get_coordinates(shapely.line_interpolate_point(ln, np.clip(at + 10, 0, L)))
    t = heading(b[:, 0] - a[:, 0], b[:, 1] - a[:, 1])
    sem = seg.dir_deg_final.to_numpy(dtype=float)[si]
    t = np.where(angdiff(t, sem) > 90, (t + 180) % 360, t)
    c["angle"] = angdiff(p.loc[c.i, "head"].values, t)
    c = c[c.angle <= MAX_ANGLE].sort_values(["i", "dist"]).drop_duplicates("i")
    out = p.loc[c.i, ["rn_pair_id", "chord_m"]].reset_index(drop=True)
    out["roadseg_id"] = seg.roadseg_id.values[c.s.values]
    out["dist_m"], out["angle_deg"] = c.dist.values, c.angle.values
    return out


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--speed-csv", default=str(ROOT / "raw_data/speed_Beijing_all_wgs84.csv"))
    ap.add_argument("--output-name", default="segment_speed_flow_rn202505_v1")
    args = ap.parse_args(argv)
    out = ROOT / "outputs" / args.output_name
    if out.exists():
        raise SystemExit(f"{out} exists; choose a new --output-name")
    out.mkdir(parents=True)

    seg = pd.read_parquet(SEGMENTS, columns=["roadseg_id", "raw_geometry", "dir_deg_final", "road_class", "roadname"]).drop_duplicates("roadseg_id").reset_index(drop=True)
    seg["seg_len_m"] = shapely.length(shapely.from_wkb(seg.raw_geometry.values))
    files = sorted(glob.glob(str(COUNTS / "rn_pressure_counts_*.csv")))
    p = rn_pairs(files)
    m = match(p, seg)
    m.to_parquet(out / "segment_rn_match.parquet", index=False)
    print(f"pairs {len(p):,}; matched {len(m):,} to {m.roadseg_id.nunique():,} segments", flush=True)

    cols = [f"n_valid_transitions_s{s:02d}" for s in range(36)]
    w = m.set_index("rn_pair_id").chord_m
    allchord = p.set_index("rn_pair_id").chord_m
    parts, vkm_all, vkm_matched = [], 0.0, 0.0
    for i, f in enumerate(files):
        d = pd.read_csv(f, usecols=["date", "rn_pair_id", "time_block_6h", *cols], dtype={"rn_pair_id": str}, encoding="utf-8-sig")
        long = d.melt(id_vars=["date", "rn_pair_id", "time_block_6h"], value_vars=cols, var_name="s", value_name="n").dropna(subset=["n"])
        long = long[long.n > 0]
        ch = long.rn_pair_id.map(allchord)
        vkm_all += float((long.n * ch).sum())
        long = long[long.rn_pair_id.isin(w.index)]
        long["w"] = long.rn_pair_id.map(w)
        vkm_matched += float((long.n * long.w).sum())
        long["slot10"] = (long.time_block_6h * 36 + long.s.str[-2:].astype(int)).astype("int16")
        long["roadseg_id"] = long.rn_pair_id.map(m.set_index("rn_pair_id").roadseg_id)
        long["nw"] = long.n * long.w
        parts.append(long.groupby(["roadseg_id", "date", "slot10"]).nw.sum().reset_index())
        if i % 50 == 0:
            print(f"rn {i + 1}/{len(files)}", flush=True)
    fl = pd.concat(parts).groupby(["roadseg_id", "date", "slot10"], as_index=False).nw.sum()
    wsum = m.groupby("roadseg_id").chord_m.sum()
    fl["flow"] = fl.nw / fl.roadseg_id.map(wsum)
    # written at segment x hour, rows below 5 sample passes dropped (project export rule)
    fl["hour"] = (fl.slot10 // 6).astype("int16")
    fh = fl.groupby(["roadseg_id", "date", "hour"], as_index=False).flow.sum()
    fh[fh.flow >= 5].to_parquet(out / "segment_flow_hour.parquet", index=False)

    keep = set(m.roadseg_id)
    sp = []
    for c in pd.read_csv(args.speed_csv, usecols=["roadseg_id", "speed", "hour"], chunksize=4_000_000):
        c = c[c.roadseg_id.isin(keep) & (c.speed > 0) & c.hour.notna()]
        st = c.hour.astype("int64")
        sp.append(pd.DataFrame({"roadseg_id": c.roadseg_id.values, "date": (st // 10000).astype("int32").values,
                                "slot10": ((st // 100 % 100) * 6 + st % 100 // 10).astype("int16").values,
                                "pace_min_per_km": (60.0 / c.speed).values}))
    sp = pd.concat(sp).groupby(["roadseg_id", "date", "slot10"], as_index=False).pace_min_per_km.mean()
    sp.to_parquet(out / "segment_pace_10min.parquet", index=False)

    meta = seg.drop(columns=["raw_geometry"]).merge(
        m.groupby("roadseg_id").agg(n_pairs=("rn_pair_id", "size"), matched_chord_m=("chord_m", "sum")).reset_index(), on="roadseg_id", how="left")
    meta["matched_share_of_length"] = (meta.matched_chord_m / meta.seg_len_m).clip(upper=5)
    meta.to_parquet(out / "segment_meta.parquet", index=False)
    summary = {
        "rn_pairs_with_coordinates": int(len(p)), "rn_pairs_matched": int(len(m)),
        "baidu_segments": int(len(seg)), "baidu_segments_matched": int(m.roadseg_id.nunique()),
        "share_rn_vehicle_km_on_matched_pairs": round(vkm_matched / vkm_all, 4),
        "share_baidu_length_matched": round(float(seg.seg_len_m[seg.roadseg_id.isin(keep)].sum() / seg.seg_len_m.sum()), 4),
        "matched_by_road_class": {int(k): int(v) for k, v in meta[meta.n_pairs.notna()].road_class.value_counts().items()},
        "segment_hour_rows_flow": int((fh.flow >= 5).sum()), "segment_slot_rows_pace": int(len(sp)),
        "rule": f"midpoint <= {MAX_DIST_M} m, chord >= {MIN_CHORD_M} m, heading within {MAX_ANGLE} deg of signed local tangent",
    }
    (out / "segment_build_summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
