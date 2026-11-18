"""
Directed Baidu speed on OSM route arcs (nextstep D step 1)
=========================================================

Speed's V1-OSM panel (bj_base) averages all Baidu segments mapped to one OSM edge,
often both carriageway directions, so it cannot be used for directional travel times.
Here each directed OSM route arc (Speed V1-OSM route_v3, layer suffix ignored, classes
tertiary and above incl. links) is matched to a Baidu speed segment
(raw_segment_master, unsplit raw geometry, EPSG:3857) if
    - the arc midpoint lies within MAX_DIST_M of the Baidu line, and
    - the arc heading is within MAX_ANGLE of the Baidu segment's local travel
      direction (tangent at the projected point, signed by its semantic direction
      dir_deg_final), the rule used for RN chords in build_segment_speed_flow_panel.py.
Among valid candidates the nearest wins. Arcs shorter than MIN_ARC_M take the heading
of their parent OSM edge chain. Speed's undirected roadseg -> osm_idx table
(mapping_all.csv) is only used to report agreement.

Speeds (raw Baidu file, every segment that has an arc match): mean pace by
roadseg x date x hour, 0519-0525.

Inputs read only. Output: data_work/outputs/<output-name>/ (refuses to overwrite)
    arc_baidu_match.parquet  edge_idx, roadseg_id, dist_m, angle_deg, in_speed_mapping
    arcs_major.parquet       edge_idx, from/to xy keys, class, length, parent id, observed flag
    baidu_pace_hour.parquet  roadseg_id, date, hour, pace_min_per_km, n10
    summary.json             coverage by class and direction, agreement with Speed's flag
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
import shapely
from pyproj import Transformer

ROOT = Path(__file__).resolve().parents[2]
SPEED = Path("/Users/fxr/Library/CloudStorage/Dropbox/Projects/Projects_FengGu/Speed/Road_processing/V1-OSM/data")
ARCS = SPEED / "intermediate/route_v3/beijing_arcs.parquet"
FOLD = SPEED / "intermediate/route_v3/beijing_fold_costs_20250519_v1.parquet"
MAPPING = SPEED / "raw/matching/beijing/mapping_all.csv"
SEGMENTS = ROOT / "outputs/raw_rebuild_validation_gridcost_harmonic_v1/data/raw_segment_master.parquet"
SPEED_CSV = ROOT / "raw_data/speed_Beijing_all_wgs84.csv"
MAJOR = {"motorway", "motorway_link", "trunk", "trunk_link", "primary", "primary_link",
         "secondary", "secondary_link", "tertiary", "tertiary_link"}
MAX_DIST_M, MAX_ANGLE, MIN_ARC_M = 20.0, 30.0, 10.0
T = Transformer.from_crs(4326, 3857, always_xy=True)


def angdiff(a, b):
    return np.abs((np.asarray(a) - np.asarray(b) + 180) % 360 - 180)


def heading(dx, dy):
    return np.degrees(np.arctan2(dx, dy)) % 360


def load_arcs() -> pd.DataFrame:
    a = pd.read_parquet(ARCS, columns=["edge_idx", "edge_id", "segment_id", "route_segment_id", "road_class", "from_node_id", "to_node_id", "length_m"])
    a = a[a.road_class.isin(MAJOR)].reset_index(drop=True)
    for s in ["from", "to"]:
        p = a[f"{s}_node_id"].str.extract(r"^([0-9.]+),([0-9.]+)\|")
        a[f"{s}_xy"] = p[0] + "," + p[1]
        a[f"{s}_x"], a[f"{s}_y"] = T.transform(p[0].astype(float).to_numpy(), p[1].astype(float).to_numpy())
    a["head"] = heading(a.to_x - a.from_x, a.to_y - a.from_y)
    # short arcs take the heading of their parent OSM edge in the same travel direction
    a["sub"] = a.route_segment_id.str.extract(r"::S(\d+)$")[0].astype(int)
    a["pdir"] = a.segment_id + "::" + a.edge_id.str[-1]
    a = a.sort_values(["pdir", "sub"])
    g = a.groupby("pdir")
    sx, sy = g.from_x.transform("first"), g.from_y.transform("first")
    ex, ey = g.to_x.transform("last"), g.to_y.transform("last")
    ph = heading(ex - sx, ey - sy)
    a["head_used"] = np.where(a.length_m < MIN_ARC_M, ph, a["head"])
    return a.sort_index().drop(columns=["from_node_id", "to_node_id"])


def match(a: pd.DataFrame, seg: pd.DataFrame) -> pd.DataFrame:
    lines = shapely.from_wkb(seg.raw_geometry.values)
    tree = shapely.STRtree(lines)
    mid = shapely.points((a.from_x + a.to_x).values / 2, (a.from_y + a.to_y).values / 2)
    pi, si = tree.query(mid, predicate="dwithin", distance=MAX_DIST_M)
    c = pd.DataFrame({"i": pi, "s": si})
    pts, ln = mid[pi], lines[si]
    c["dist"] = shapely.distance(pts, ln)
    at = shapely.line_locate_point(ln, pts)
    L = shapely.length(ln)
    p0 = shapely.get_coordinates(shapely.line_interpolate_point(ln, np.clip(at - 10, 0, L)))
    p1 = shapely.get_coordinates(shapely.line_interpolate_point(ln, np.clip(at + 10, 0, L)))
    t = heading(p1[:, 0] - p0[:, 0], p1[:, 1] - p0[:, 1])
    sem = seg.dir_deg_final.to_numpy(dtype=float)[si]
    t = np.where(angdiff(t, sem) > 90, (t + 180) % 360, t)
    c["angle"] = angdiff(a.head_used.to_numpy()[pi], t)
    c = c[c.angle <= MAX_ANGLE].sort_values(["i", "dist"]).drop_duplicates("i")
    return pd.DataFrame({"edge_idx": a.edge_idx.to_numpy()[c.i], "roadseg_id": seg.roadseg_id.to_numpy()[c.s],
                         "dist_m": c.dist.to_numpy(), "angle_deg": c.angle.to_numpy()})


def pace(keep: set) -> pd.DataFrame:
    sp = []
    for c in pd.read_csv(SPEED_CSV, usecols=["roadseg_id", "speed", "hour"], chunksize=4_000_000):
        c = c[c.roadseg_id.isin(keep) & (c.speed > 0) & c.hour.notna()]
        st = c.hour.astype("int64")
        sp.append(pd.DataFrame({"roadseg_id": c.roadseg_id.values, "date": (st // 10000).astype("int32").values,
                                "hour": (st // 100 % 100).astype("int16").values, "pace": (60.0 / c.speed).values}))
    sp = pd.concat(sp)
    return sp.groupby(["roadseg_id", "date", "hour"]).pace.agg(pace_min_per_km="mean", n10="size").reset_index()


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-name", default="osm_directed_speed_v1")
    args = ap.parse_args(argv)
    out = ROOT / "outputs" / args.output_name
    if out.exists():
        raise SystemExit(f"{out} exists; choose a new --output-name")
    out.mkdir(parents=True)
    a = load_arcs()
    seg = pd.read_parquet(SEGMENTS, columns=["roadseg_id", "raw_geometry", "dir_deg_final", "road_class"]).drop_duplicates("roadseg_id").reset_index(drop=True)
    m = match(a, seg)
    mp = pd.read_csv(MAPPING).dropna(subset=["osm_idx"])
    mp = set(zip(mp.roadseg_id.astype(str), "BE_OSM_" + mp.osm_idx.astype(int).astype(str)))
    par = a.set_index("edge_idx").segment_id
    m["in_speed_mapping"] = [(r, par[e]) in mp for r, e in zip(m.roadseg_id, m.edge_idx)]
    m.to_parquet(out / "arc_baidu_match.parquet", index=False)
    print(f"arcs {len(a):,}; matched {len(m):,} to {m.roadseg_id.nunique():,} Baidu segments", flush=True)

    sp = pace(set(m.roadseg_id))
    sp.to_parquet(out / "baidu_pace_hour.parquet", index=False)
    has_speed = set(sp.roadseg_id)
    m["has_speed"] = m.roadseg_id.isin(has_speed)
    a["obs_directed"] = a.edge_idx.isin(m.edge_idx[m.has_speed])
    fold = pd.read_parquet(FOLD, columns=["edge_idx", "has_speed_observation"]).set_index("edge_idx").has_speed_observation
    a["obs_speed_flag"] = a.edge_idx.map(fold).fillna(False).astype(bool)
    a.drop(columns=["from_x", "from_y", "to_x", "to_y"]).to_parquet(out / "arcs_major.parquet", index=False)

    km = a.length_m / 1000
    by = a.assign(km=km).groupby("road_class").apply(lambda s: pd.Series({
        "km": round(s.km.sum(), 0),
        "directed_obs_share": round(s.km[s.obs_directed].sum() / s.km.sum(), 3),
        "speed_flag_share": round(s.km[s.obs_speed_flag].sum() / s.km.sum(), 3)}))
    summary = {
        "rule": f"arc midpoint <= {MAX_DIST_M} m from Baidu line, heading within {MAX_ANGLE} deg of signed local tangent; arcs < {MIN_ARC_M} m use parent heading",
        "arcs_major": int(len(a)), "km_major": round(float(km.sum()), 1),
        "arcs_matched": int(len(m)), "baidu_segments_matched": int(m.roadseg_id.nunique()),
        "baidu_segments_with_speed": int(len(has_speed)),
        "directed_obs_km_share": round(float(km[a.obs_directed].sum() / km.sum()), 4),
        "speed_flag_km_share": round(float(km[a.obs_speed_flag].sum() / km.sum()), 4),
        "both_flags_km_share": round(float(km[a.obs_directed & a.obs_speed_flag].sum() / km.sum()), 4),
        "speed_flag_only_km_share": round(float(km[~a.obs_directed & a.obs_speed_flag].sum() / km.sum()), 4),
        "directed_only_km_share": round(float(km[a.obs_directed & ~a.obs_speed_flag].sum() / km.sum()), 4),
        "match_agrees_with_speed_mapping_share": round(float(m.in_speed_mapping.mean()), 4),
        "match_dist_m_p50_p90": [round(float(x), 1) for x in m.dist_m.quantile([.5, .9])],
        "by_class": by.to_dict(orient="index"),
    }
    (out / "summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False))
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
