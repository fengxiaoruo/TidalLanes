"""
Routed grid-pair travel cost on the centerline network, 2025 OD, gate5 rules
===========================================================================
The existing centerline cost (stage06) is grid-centroid distance over a harmonic speed,
with no routing. This builds a routed version on the same centerline system:

Arcs    = directed centerlines kept for the model (centerline_dir_master.keep_qsm) of a
          centerline run (v11: manual_centerline_rules_v11; v3: centerline_v3_rules_v11_v1).
Topology: the centerline layer is not a routable network (skeleton endpoints stop 5-7 m
          short of each other; T-junctions carry no node). Each centerline endpoint is
          joined to every other centerline within --snap (EPSG:3857 units, about 0.77 m
          true per unit at Beijing): the other centerline is split at the projection
          point and the endpoint is merged into that split node. Only endpoints snap, so
          mid-line crossings (grade-separated roads) stay unconnected. Pieces inherit the
          speeds and lanes of their directed centerline.
          Speeds from centerline_speed_master (distance-weighted): AM = weekday AM-peak
          rows, PM = weekday PM-peak rows, FF = 22-05 h rows (the gate5 free-flow window).
          Directed centerlines without AM and FF speed (mostly short junction pieces:
          about 30% by count, 8% by length) enter only if at most 300 m long, with the mean
          speed of the observed arcs they meet (the short-gap rule of OSM scheme 1B);
          longer ones are dropped. A missing PM speed is filled with the AM speed.
          Lanes = centerline_lane_master.lane_est_length_weighted (fallback: raw weighted,
          then 2).
Pairs   = the routing of the OSM grid costs (src/network/build_osm_grid_travel_time.py),
          unchanged: largest strongly connected arc component, model-origin 3 km square
          grid, directed 4-neighbour pairs, tau = mean shortest time over access-node pairs
          (3 x 3 lattice per cell), centre-node AM path for km, detour and lanes.
Bundle  = as src/model/travel_cost_bundle/build_osm_model_bundle.py: detour gate 5, cells
          of the gate5 OD bundle only (default) or all reached cells, largest strongly
          connected cell component, nodes / OD from model_input_edgeproj_v2_lcc_gate5_od2025_v2.
Inputs read only. Output: data_work/outputs/<version-id>/ (refuses to overwrite)
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from pyproj import Transformer

DATA_WORK = Path(__file__).resolve().parents[3]
if str(DATA_WORK) not in sys.path:
    sys.path.insert(0, str(DATA_WORK))

from src.network import build_osm_grid_travel_time as og  # noqa: E402
from src.model.travel_cost_bundle import build_osm_model_bundle as ob  # noqa: E402

O = DATA_WORK / "outputs"
GAP_M = 300.0
SOURCES = {"v11": "manual_centerline_rules_v11", "v3": "centerline_v3_rules_v11_v1"}
T_LL = Transformer.from_crs(3857, 4326, always_xy=True)


class _UF:
    def __init__(self):
        self.p = {}

    def find(self, x):
        self.p.setdefault(x, x)
        while self.p[x] != x:
            self.p[x] = self.p[self.p[x]]
            x = self.p[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.p[rb] = ra


def arc_table(run: str, snap: float) -> tuple[pd.DataFrame, dict]:
    data = O / run / "data"
    d = gpd.read_parquet(data / "centerline_dir_master.parquet")
    d = d[d.keep_qsm].copy()
    sp = pd.read_parquet(data / "centerline_speed_master.parquet")
    sp = sp[sp.has_valid_speed.fillna(False)]
    out = {}
    for p, m in {"AM": sp.is_weekday & sp.is_am_peak, "PM": sp.is_weekday & sp.is_pm_peak, "FF": sp.is_freeflow_2205}.items():
        g = sp[m].groupby(["cline_id", "dir"])[["total_dist_m", "total_time_h"]].sum()
        out[p] = (g.total_dist_m / g.total_time_h).rename(f"v_{p}")  # = cl_speed_kmh (km/h) row by row
    v = pd.concat(out.values(), axis=1).reset_index()
    ln = pd.read_parquet(data / "centerline_lane_master.parquet", columns=["cline_id", "dir", "lane_est_length_weighted", "lane_est_raw_weighted"])
    d = d.merge(v, on=["cline_id", "dir"], how="left").merge(ln, on=["cline_id", "dir"], how="left")
    info = {"directed_centerlines_kept": int(len(d)), "km_kept_3857": round(float(d.length_m.sum() / 1000), 1)}
    d["obs"] = d.v_AM.gt(0) & d.v_FF.gt(0)
    info["length_share_with_AM_and_FF_speed"] = round(float(d.length_m[d.obs].sum() / d.length_m.sum()), 4)
    info["count_share_with_AM_and_FF_speed"] = round(float(d.obs.mean()), 4)
    d["v_PM"] = d.v_PM.where(d.v_PM.gt(0), d.v_AM)
    d["lanes"] = d.lane_est_length_weighted.where(d.lane_est_length_weighted.gt(0), d.lane_est_raw_weighted)
    info["lanes_fallback_2_share"] = round(float((~d.lanes.gt(0)).mean()), 4)
    d["lanes"] = d.lanes.where(d.lanes.gt(0), 2.0)

    # undirected line per centerline, in AB orientation
    geo = gpd.read_parquet(data / "centerline_dir_master.parquet")
    geo = geo[(geo.dir == "AB") & geo.cline_id.isin(d.cline_id)].drop_duplicates("cline_id").set_index("cline_id").geometry
    ids = geo.index.to_numpy()
    lines = gpd.GeoSeries(geo.values, crs=geo.crs)
    L = lines.length.to_numpy()
    cuts = {k: {0.0, float(L[k])} for k in range(len(ids))}
    uf = _UF()
    ends = []
    for k, g in enumerate(lines):
        ends += [(k, 0.0, g.coords[0]), (k, float(L[k]), g.coords[-1])]
    pts = gpd.GeoSeries(gpd.points_from_xy([e[2][0] for e in ends], [e[2][1] for e in ends]), crs=geo.crs)
    hit_e, hit_l = lines.sindex.query(pts.buffer(snap), predicate="intersects")
    n_join, joined = 0, set()
    for e, l in zip(hit_e, hit_l):
        k, s0, xy = ends[e]
        if l == k:
            continue
        joined.add(e)
        pos = round(float(lines.iloc[l].project(pts.iloc[e])), 1)
        cuts[l].add(pos)
        uf.union((k, round(s0, 1)), (l, pos))
        n_join += 1
    info["snap_3857_units"] = snap
    info["endpoint_joins"] = int(n_join)
    info["endpoints_joined_share"] = round(len(joined) / len(ends), 4)

    # pieces and node coordinates
    rows = []
    coord = {}
    for k, g in enumerate(lines):
        cs = sorted(round(c, 1) for c in cuts[k])
        cs = [c for i, c in enumerate(cs) if i == 0 or c - cs[i - 1] >= 0.5]
        for a, b in zip(cs[:-1], cs[1:]):
            pa, pb = g.interpolate(a), g.interpolate(b)
            na, nb = uf.find((k, a)), uf.find((k, b))
            coord.setdefault(na, (pa.x, pa.y)); coord.setdefault(nb, (pb.x, pb.y))
            rows.append((ids[k], na, nb, b - a, pa.y))
    pc = pd.DataFrame(rows, columns=["cline_id", "na", "nb", "len3857", "y"])
    keys = list(coord)
    lo, la = T_LL.transform(np.array([coord[q][0] for q in keys]), np.array([coord[q][1] for q in keys]))
    xy = {q: f"{a:.7f},{b:.7f}" for q, a, b in zip(keys, lo, la)}
    lat = T_LL.transform(np.zeros(len(pc)), pc.y.to_numpy())[1]
    pc["length_m"] = pc.len3857 * np.cos(np.radians(lat))
    arcs = []
    for dname, (fa, fb) in {"AB": ("na", "nb"), "BA": ("nb", "na")}.items():
        m = pc.merge(d[d.dir == dname][["cline_id", "v_AM", "v_PM", "v_FF", "lanes", "obs"]], on="cline_id")
        arcs.append(pd.DataFrame({"cline_id": m.cline_id, "dir": dname, "from_xy": m[fa].map(xy), "to_xy": m[fb].map(xy),
                                  "length_m": m.length_m, "v_AM": m.v_AM, "v_PM": m.v_PM, "v_FF": m.v_FF, "lanes": m.lanes,
                                  "obs": m.obs}))
    a = pd.concat(arcs, ignore_index=True)
    a = a[a.from_xy != a.to_xy].reset_index(drop=True)
    # unobserved directed centerlines up to GAP_M (true) long take the mean speed of the
    # observed arcs meeting them (node means, averaged over both ends; fallback: city
    # median); longer unobserved centerlines are dropped (rule of OSM scheme 1B)
    cl_len = a.groupby(["cline_id", "dir"]).length_m.transform("sum")
    a = a[a.obs | (cl_len <= GAP_M)].reset_index(drop=True)
    for p in ["AM", "PM", "FF"]:
        o = a[a.obs]
        node = pd.concat([pd.Series(o[f"v_{p}"].to_numpy(), index=o.from_xy), pd.Series(o[f"v_{p}"].to_numpy(), index=o.to_xy)]).groupby(level=0).mean()
        imp = pd.concat([a.from_xy.map(node), a.to_xy.map(node)], axis=1).mean(axis=1).fillna(o[f"v_{p}"].median())
        a[f"v_{p}"] = a[f"v_{p}"].where(a.obs, imp)
    info["gap_m"] = GAP_M
    info["arc_km_share_imputed_short_gap"] = round(float(a.length_m[~a.obs].sum() / a.length_m.sum()), 4)
    info["arcs"] = int(len(a))
    info["true_km_directed"] = round(float(a.length_m.sum() / 1000), 1)
    return a, info


def pairs(G: og.Graph, x0: float, y0: float) -> pd.DataFrame:
    P = og.grid_pairs(G, x0, y0, 3000.0)
    return P.merge(og.multi_access(G, x0, y0, 3000.0, P), on=["grid_from", "grid_to"])


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", choices=list(SOURCES), required=True)
    ap.add_argument("--cells", choices=["od_bundle", "network"], default="od_bundle")
    ap.add_argument("--snap", type=float, default=15.0, help="endpoint snapping tolerance, EPSG:3857 units")
    ap.add_argument("--topology-only", action="store_true", help="print topology diagnostics, write nothing")
    ap.add_argument("--version-id", default=None)
    args = ap.parse_args(argv)
    vid = args.version_id or f"model_input_centerline_{args.source}_routed_gate5_od2025_v1"
    out = O / vid
    if out.exists() and not args.topology_only:
        raise SystemExit(f"{out} exists; choose a new --version-id")
    meta = json.loads(og.MANIFEST.read_text())
    x0, y0 = meta["x_origin_3857"], meta["y_origin_3857"]
    a, info = arc_table(SOURCES[args.source], args.snap)
    G = og.Graph(a)
    info["arc_scc_km_share"] = round(G.km_share, 4)
    if args.topology_only:
        print(json.dumps(info, ensure_ascii=False, default=float))
        return
    P = pairs(G, x0, y0)
    L = ob.path_lanes(G, P, x0, y0, 3000.0)
    (out / "data").mkdir(parents=True)
    P.to_parquet(out / "data/grid_pairs_routed.parquet", index=False)
    info.update(ob.assemble(P, L, out, vid, source_label=f"centerline_{args.source}_routed", cells=args.cells))
    (out / "BUNDLE_META.json").write_text(json.dumps(info, indent=2, ensure_ascii=False, default=float))
    print(json.dumps(info, indent=2, ensure_ascii=False, default=float))


if __name__ == "__main__":
    main()
