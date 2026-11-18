"""
Unicom RN flow on the OSM route graph (nextstep D step 5)
========================================================

RN pairs (rn_baidu_match_network_v1/rn_pair_class.parquet: chord coordinates, weekday
and AM-peak sample passes per day) are placed on the directed OSM route graph (Speed
V1-OSM route_v3, all motor classes, layer suffix dropped):
    1 each RN node is snapped to the nearest OSM node; kept if within SNAP_M
    2 the pair is routed from its snapped from-node to its snapped to-node by shortest
      length; accepted if path length <= MAX_STRETCH x chord + SLACK_M
    3 the pair's passes are added to every OSM arc on the path
Outputs arc-level weekday and AM flow, coverage by vehicle-km and by OSM class, and
grid-edge flows (arcs whose two ends lie in different cells) for the model-origin 3 km
grid, compared with the chord length allocation used so far (cell_pair_flow).
Inputs read only. Output: data_work/outputs/<output-name>/ (refuses to overwrite)
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
from pyproj import Transformer
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import dijkstra
from scipy.spatial import cKDTree

ROOT = Path(__file__).resolve().parents[2]
ARCS = Path("/Users/fxr/Library/CloudStorage/Dropbox/Projects/Projects_FengGu/Speed/Road_processing/V1-OSM/data/intermediate/route_v3/beijing_arcs.parquet")
PAIRS = ROOT / "outputs/rn_baidu_match_network_v1/rn_pair_class.parquet"
CELLPAIR = ROOT / "outputs/rn_baidu_match_network_v1/cell_pair_flow.parquet"
GATE5 = ROOT / "outputs/model_input_edgeproj_v2_lcc_gate5_v1/data/qsm_input_edges_square.parquet"
MANIFEST = ROOT.parent / "Platform/reference_inputs/manifest.json"
SNAP_M, MAX_STRETCH, SLACK_M = 15.0, 1.5, 50.0
BATCH = 120
T4326_UTM = Transformer.from_crs(4326, 32650, always_xy=True)
T3857_UTM = Transformer.from_crs(3857, 32650, always_xy=True)
T4326_3857 = Transformer.from_crs(4326, 3857, always_xy=True)


def osm_graph():
    a = pd.read_parquet(ARCS, columns=["edge_idx", "road_class", "from_node_id", "to_node_id", "length_m"])
    for s in ["from", "to"]:
        a[f"{s}_xy"] = a[f"{s}_node_id"].str.replace(r"\|L-?\d+$", "", regex=True)
    codes, uniq = pd.factorize(pd.concat([a.from_xy, a.to_xy], ignore_index=True))
    n = len(a)
    a["u"], a["v"] = codes[:n], codes[n:]
    ll = pd.Series(uniq).str.split(",", expand=True).astype(float)
    ux, uy = T4326_UTM.transform(ll[0].to_numpy(), ll[1].to_numpy())
    # one arc per (u, v): shortest
    a = a.sort_values("length_m").drop_duplicates(["u", "v"]).reset_index(drop=True)
    G = coo_matrix((a.length_m.to_numpy(), (a.u.to_numpy(), a.v.to_numpy())), shape=(len(uniq),) * 2).tocsr()
    return a.drop(columns=["from_node_id", "to_node_id"]), G, ux, uy, ll


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-name", default="rn_osm_flow_v1")
    args = ap.parse_args(argv)
    out = ROOT / "outputs" / args.output_name
    if out.exists():
        raise SystemExit(f"{out} exists; choose a new --output-name")
    out.mkdir(parents=True)
    a, G, ox, oy, ll = osm_graph()
    print(f"osm nodes {G.shape[0]:,} arcs {len(a):,}", flush=True)
    p = pd.read_parquet(PAIRS, columns=["rn_pair_id", "rn_from", "rn_to", "x_from", "y_from", "x_to", "y_to", "chord_m", "n_wd", "n_am", "vkm_wd", "vkm_am"])
    p = p[(p.n_wd > 0) | (p.n_am > 0)].reset_index(drop=True)
    # snap RN nodes
    nodes = pd.concat([p[["rn_from", "x_from", "y_from"]].set_axis(["rn", "x", "y"], axis=1),
                       p[["rn_to", "x_to", "y_to"]].set_axis(["rn", "x", "y"], axis=1)]).drop_duplicates("rn")
    nx_, ny_ = T3857_UTM.transform(nodes.x.to_numpy(), nodes.y.to_numpy())
    d, j = cKDTree(np.c_[ox, oy]).query(np.c_[nx_, ny_])
    nodes["osm"], nodes["snap_m"] = j, d
    snap = nodes.set_index("rn")
    p["u"], p["v"] = p.rn_from.map(snap.osm).to_numpy(), p.rn_to.map(snap.osm).to_numpy()
    p["du"], p["dv"] = p.rn_from.map(snap.snap_m).to_numpy(), p.rn_to.map(snap.snap_m).to_numpy()
    # chord in real metres (3857 is stretched by 1/cos(lat))
    fx, fy = T3857_UTM.transform(p.x_from.to_numpy(), p.y_from.to_numpy())
    tx, ty = T3857_UTM.transform(p.x_to.to_numpy(), p.y_to.to_numpy())
    p["chord_real_m"] = np.hypot(tx - fx, ty - fy)
    ok_snap = (p.du <= SNAP_M) & (p.dv <= SNAP_M) & (p.u != p.v)
    res = {"rn_nodes": int(len(nodes)), "rn_nodes_snap_le_5m": round(float((nodes.snap_m <= 5).mean()), 4),
           "rn_nodes_snap_le_15m": round(float((nodes.snap_m <= SNAP_M).mean()), 4),
           "snap_m_p50_p90": [round(float(x), 2) for x in nodes.snap_m.quantile([.5, .9])],
           "pairs": int(len(p)), "pairs_both_snapped": int(ok_snap.sum()),
           "vkm_wd_share_both_snapped": round(float(p.vkm_wd[ok_snap].sum() / p.vkm_wd.sum()), 4)}
    print(json.dumps(res), flush=True)
    # route
    q = p[ok_snap].copy()
    q["lim"] = MAX_STRETCH * q.chord_real_m + SLACK_M
    q = q.sort_values(["u"])
    src = q.u.unique()
    lim_by_src = q.groupby("u").lim.max()
    # order sources by limit so each batch has similar search radius
    src = lim_by_src.sort_values().index.to_numpy()
    arc_of = {(int(x), int(y)): i for i, (x, y) in enumerate(zip(a.u.to_numpy(), a.v.to_numpy()))}
    plen = np.full(len(p), np.nan)
    fl_wd = np.zeros(len(a)); fl_am = np.zeros(len(a))
    qi = q.groupby("u").indices
    qidx = q.index.to_numpy()
    for b in range(0, len(src), BATCH):
        s = src[b:b + BATCH]
        D, pred = dijkstra(G, directed=True, indices=s, limit=float(lim_by_src[s].max()), return_predecessors=True)
        for k, su in enumerate(s):
            for r in qidx[qi[su]]:
                tv = p.v.iat[r]
                L = D[k, tv]
                if not np.isfinite(L) or L > p.at[r, "chord_real_m"] * MAX_STRETCH + SLACK_M:
                    continue
                plen[r] = L
                us, vs, x = [], [], tv
                while x != su:
                    y = pred[k, x]
                    us.append(y); vs.append(x); x = y
                ids = [arc_of[(int(y), int(x))] for y, x in zip(us, vs)]
                fl_wd[ids] += p.n_wd.iat[r]
                fl_am[ids] += p.n_am.iat[r]
        if (b // BATCH) % 200 == 0:
            print(f"batch {b // BATCH + 1}/{-(-len(src) // BATCH)}", flush=True)
    p["path_m"] = plen
    routed = p.path_m.notna()
    res["pairs_routed"] = int(routed.sum())
    res["vkm_wd_share_routed"] = round(float(p.vkm_wd[routed].sum() / p.vkm_wd.sum()), 4)
    res["vkm_am_share_routed"] = round(float(p.vkm_am[routed].sum() / p.vkm_am.sum()), 4)
    res["path_over_chord_p50_p90"] = [round(float(x), 3) for x in (p.path_m[routed] / p.chord_real_m[routed]).quantile([.5, .9])]
    a["flow_wd"], a["flow_am"] = fl_wd, fl_am
    km = a.length_m / 1000
    res["arc_vkm_wd_by_class_share"] = (km * a.flow_wd).groupby(a.road_class).sum().pipe(lambda s: (s / s.sum()).round(4)).sort_values(ascending=False).head(12).to_dict()
    a.to_parquet(out / "osm_arc_rn_flow.parquet", index=False)
    p[["rn_pair_id", "u", "v", "du", "dv", "chord_real_m", "path_m", "n_wd", "n_am"]].to_parquet(out / "rn_pair_osm_route.parquet", index=False)
    # grid edges, model-origin 3 km
    meta = json.loads(MANIFEST.read_text())
    X0, Y0 = meta["x_origin_3857"], meta["y_origin_3857"]
    x3, y3 = T4326_3857.transform(ll[0].to_numpy(), ll[1].to_numpy())
    cr, cc = np.floor((y3 - Y0) / 3000).astype(int), np.floor((x3 - X0) / 3000).astype(int)
    cu = pd.Series([f"sq_{r}_{c}" for r, c in zip(cr[a.u], cc[a.u])])
    cv = pd.Series([f"sq_{r}_{c}" for r, c in zip(cr[a.v], cc[a.v])])
    cross = (cu != cv).to_numpy()
    ge = pd.DataFrame({"grid_from": cu[cross].to_numpy(), "grid_to": cv[cross].to_numpy(),
                       "f_wd_osm": fl_wd[cross], "f_am_osm": fl_am[cross]}).groupby(["grid_from", "grid_to"], as_index=False).sum()
    ge.to_parquet(out / "grid_edge_flow_sq3000_model_origin.parquet", index=False)
    cp = pd.read_parquet(CELLPAIR, columns=["grid_from", "grid_to", "f_wd", "cls"])
    m = ge.merge(cp, on=["grid_from", "grid_to"], how="outer").fillna({"f_wd_osm": 0, "f_wd": 0})
    pos = (m.f_wd_osm > 0) & (m.f_wd > 0)
    g5 = pd.read_parquet(GATE5, columns=["grid_o", "grid_d"]).rename(columns={"grid_o": "grid_from", "grid_d": "grid_to"})
    mg = m.merge(g5, on=["grid_from", "grid_to"])
    res["grid_sq3000"] = {
        "cell_pairs_osm": int((m.f_wd_osm > 0).sum()), "cell_pairs_chord_alloc": int((m.f_wd > 0).sum()),
        "corr_log_flow_both_positive": round(float(np.corrcoef(np.log(m.f_wd_osm[pos]), np.log(m.f_wd[pos]))[0, 1]), 3),
        "ratio_osm_over_chord_p50": round(float((m.f_wd_osm[pos] / m.f_wd[pos]).median()), 3),
        "gate5_edges_corr_log": round(float(np.corrcoef(np.log(mg.f_wd_osm[(mg.f_wd_osm > 0) & (mg.f_wd > 0)]), np.log(mg.f_wd[(mg.f_wd_osm > 0) & (mg.f_wd > 0)]))[0, 1]), 3),
        "gate5_edges_with_osm_flow": round(float((mg.f_wd_osm > 0).mean()), 4)}
    (out / "summary.json").write_text(json.dumps(res, indent=2, ensure_ascii=False))
    print(json.dumps(res, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
