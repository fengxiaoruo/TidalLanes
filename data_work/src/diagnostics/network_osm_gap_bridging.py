"""
Scheme 1B: observed OSM roads plus short gap-bridging pieces only
=================================================================

Start from OSM arcs with a Baidu speed observation (layer suffix dropped). Add an
unobserved arc of class tertiary-and-above only if both its end nodes lie within
network distance L of an observed arc (distance measured along tertiary-and-above
roads, ignoring direction). This fills junction pieces, short unmatched stretches and
small breaks (gaps up to about 2L) without extending into roads far from any
observation. For L in a grid of values: strong / weak components, share of added km,
and grid reachability on the largest SCC (model-origin 3 km with Unicom flow weights,
and 6 km), using network_grid_reachability.py.
Inputs read only. Output: data_work/outputs/<output-name>/ (refuses to overwrite)
"""
from __future__ import annotations

import argparse
import json

import numpy as np
import pandas as pd
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import dijkstra

import network_grid_reachability as gr
from network_osm_observed_variants import FOLD, comps

LS = [0, 100, 300, 600, 1000, 2000]


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-name", default="network_osm_gap_bridging_v1")
    args = ap.parse_args(argv)
    out = gr.ROOT / "outputs" / args.output_name
    if out.exists():
        raise SystemExit(f"{out} exists; choose a new --output-name")
    out.mkdir(parents=True)
    meta = json.loads(gr.MANIFEST.read_text())
    X0, Y0 = meta["x_origin_3857"], meta["y_origin_3857"]
    a = gr.osm_arcs()
    idx = pd.read_parquet(gr.ARCS, columns=["edge_idx"]).edge_idx
    f = pd.read_parquet(FOLD, columns=["edge_idx", "has_speed_observation"]).set_index("edge_idx").has_speed_observation
    a["obs"] = idx.map(f).fillna(False).to_numpy()
    maj = a.road_class.isin(gr.MAJOR).to_numpy()
    # distance from every major-road node to the nearest observed-arc node, undirected
    m = a[maj | a.obs.to_numpy()]
    codes, uniq = pd.factorize(pd.concat([m.from_xy, m.to_xy], ignore_index=True))
    k = len(m)
    iu, iv = codes[:k], codes[k:]
    w = m.length_m.to_numpy()
    g = coo_matrix((np.r_[w, w], (np.r_[iu, iv], np.r_[iv, iu])), shape=(len(uniq),) * 2).tocsr()
    src = np.unique(np.r_[iu[m.obs.to_numpy()], iv[m.obs.to_numpy()]])
    dist = dijkstra(g, directed=False, indices=src, min_only=True, limit=max(LS) + 1)
    dn = pd.Series(dist, index=uniq)
    a["d_from"] = a.from_xy.map(dn).fillna(np.inf).to_numpy()
    a["d_to"] = a.to_xy.map(dn).fillna(np.inf).to_numpy()
    cp = pd.read_parquet(gr.CELLPAIR)
    flow = cp[["grid_from", "grid_to", "f_wd", "cls"]].copy()
    thr = json.loads((gr.ROOT / "outputs/rn_baidu_match_network_v1/match_network_summary.json").read_text())["network"]["threshold_f_wd"]
    flow["cand"] = np.where(flow.cls.isin(["no_edge_between_gate5_nodes", "reverse_only"]) & (flow.f_wd >= thr), flow.cls, "")
    obs_km = a.length_m[a.obs].sum() / 1000
    res = {}
    for L in LS:
        keep = a.obs.to_numpy() | (maj & (a.d_from.to_numpy() <= L) & (a.d_to.to_numpy() <= L))
        d = a[keep]
        e = gr.osm_net(d, False)
        r = {"km": round(float(d.length_m.sum() / 1000), 1), "added_km_over_observed": round(float(d.length_m.sum() / 1000 / obs_km - 1), 3),
             "components": comps(e)}
        net = gr.Net(e)
        r["lcc"] = net.info
        r["sq3000_model_origin"], _ = gr.grid_eval(net, X0, Y0, 3000.0, flow)
        r["sq6000_model_origin"], _ = gr.grid_eval(net, X0, Y0, 6000.0, None)
        res[f"L{L}"] = r
        print(L, json.dumps({"km": r["km"], "added": r["added_km_over_observed"], "scc": r["components"]["strong"],
                             "cells3": r["sq3000_model_origin"]["cells_with_road"],
                             "flow_le5": r["sq3000_model_origin"]["rn_flow_weighted"]["all_adjacent_rn_flow"]["flow_share_detour_le5"],
                             "flow_le2": r["sq3000_model_origin"]["rn_flow_weighted"]["all_adjacent_rn_flow"]["flow_share_detour_le2"]}, ensure_ascii=False), flush=True)
        (out / "summary.json").write_text(json.dumps(res, indent=2, ensure_ascii=False, default=float))
    print("done")


if __name__ == "__main__":
    main()
