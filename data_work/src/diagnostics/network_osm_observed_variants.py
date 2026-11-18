"""
OSM network restricted to Baidu-observed roads, and how far speed could be borrowed
==================================================================================

Scheme 2 (no imputation): keep only OSM route arcs that carry a Baidu speed observation
(Speed V1-OSM fold costs, has_speed_observation; the flag is identical across 0519-0523),
layer suffix dropped. Variant: also keep unobserved arcs of length <= 50 m in classes
tertiary and above (junction pieces left when OSM is split at intersections).
For each: strong / weak components on the whole network, and grid reachability on the
largest SCC (same measures as network_grid_reachability.py; 3 km and 6 km, model origin
and half-cell shift). OSM tertiary-and-above (all arcs) is repeated as the reference.

Scheme 1 diagnostic (no estimation): for unobserved arcs in tertiary-and-above classes,
km share that (a) touch an observed arc at a node, (b) lie within 3 hops of an observed
arc, (c) share a road name with an observed arc. These bound how much an
upstream/downstream or same-road imputation could cover before falling back to class.
Inputs read only. Output: data_work/outputs/<output-name>/ (refuses to overwrite)
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import connected_components

import network_grid_reachability as gr

FOLD = gr.ARCS.parent / "beijing_fold_costs_20250519_v1.parquet"


def comps(e: pd.DataFrame) -> dict:
    codes, uniq = pd.factorize(pd.concat([e.u, e.v], ignore_index=True))
    m, n = len(e), len(uniq)
    iu, iv = codes[:m], codes[m:]
    g = coo_matrix((np.ones(m), (iu, iv)), shape=(n, n)).tocsr()
    km = e.km.to_numpy()
    o = {"nodes": int(n), "arcs": int(m), "km": round(float(km.sum()), 1)}
    for kind in ["strong", "weak"]:
        k, lab = connected_components(g, directed=True, connection=kind)
        same = lab[iu] == lab[iv]
        kk = np.bincount(lab[iu][same], weights=km[same], minlength=k)
        o[kind] = {"n_components": int(k), "largest_km_share": round(float(kk.max() / km.sum()), 4),
                   "components_ge_1km": int((kk >= 1).sum())}
    return o


def borrow(a: pd.DataFrame) -> dict:
    maj = a[a.road_class.isin(gr.MAJOR)].copy()
    obs = maj.obs.to_numpy()
    codes, uniq = pd.factorize(pd.concat([maj.from_xy, maj.to_xy], ignore_index=True))
    m, n = len(maj), len(uniq)
    iu, iv = codes[:m], codes[m:]
    near = np.zeros(n, bool)
    near[iu[obs]] = True; near[iv[obs]] = True
    hop = {}
    reach = near.copy()
    adj = coo_matrix((np.ones(2 * m), (np.r_[iu, iv], np.r_[iv, iu])), shape=(n, n)).tocsr()
    for h in range(1, 4):
        arc_hit = reach[iu] | reach[iv]
        hop[h] = arc_hit
        reach = reach | (adj @ reach.astype(float) > 0)
    km = maj.length_m.to_numpy() / 1000
    un = ~obs
    names = set(maj.roadname[obs & maj.roadname.fillna("").ne("")])
    same_name = maj.roadname.isin(names).to_numpy()
    tot = km[un].sum()
    out = {"major_km": round(float(km.sum()), 1), "major_observed_km_share": round(float(km[obs].sum() / km.sum()), 4),
           "unobserved_major_km": round(float(tot), 1),
           "share_touching_observed_arc": round(float(km[un & hop[1]].sum() / tot), 4),
           "share_within_2_hops": round(float(km[un & hop[2]].sum() / tot), 4),
           "share_within_3_hops": round(float(km[un & hop[3]].sum() / tot), 4),
           "share_same_name_as_observed": round(float(km[un & same_name].sum() / tot), 4),
           "share_unnamed": round(float(km[un & maj.roadname.fillna("").eq("").to_numpy()].sum() / tot), 4)}
    by = maj.assign(km=km, un=un, hit=hop[1] | same_name).groupby("road_class").apply(
        lambda s: pd.Series({"km": s.km.sum(), "obs_share": s.km[~s.un].sum() / s.km.sum(),
                             "unobs_borrowable_share": s.km[s.un & s.hit].sum() / max(s.km[s.un].sum(), 1e-9)}))
    out["by_class"] = by.round(3).to_dict(orient="index")
    return out


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-name", default="network_osm_observed_variants_v1")
    args = ap.parse_args(argv)
    out = gr.ROOT / "outputs" / args.output_name
    if out.exists():
        raise SystemExit(f"{out} exists; choose a new --output-name")
    out.mkdir(parents=True)
    meta = json.loads(gr.MANIFEST.read_text())
    X0, Y0 = meta["x_origin_3857"], meta["y_origin_3857"]
    a = gr.osm_arcs()
    f = pd.read_parquet(FOLD, columns=["edge_idx", "has_speed_observation"])
    a = a.reset_index(drop=True)
    arcs_idx = pd.read_parquet(gr.ARCS, columns=["edge_idx"]).edge_idx.to_numpy()
    a["obs"] = pd.Series(arcs_idx).map(f.set_index("edge_idx").has_speed_observation).fillna(False).to_numpy()
    res = {"scheme1_borrow_diagnostic": borrow(a)}
    print(json.dumps(res, ensure_ascii=False), flush=True)
    cp = pd.read_parquet(gr.CELLPAIR)
    flow = cp[["grid_from", "grid_to", "f_wd", "cls"]].copy()
    thr = json.loads((gr.ROOT / "outputs/rn_baidu_match_network_v1/match_network_summary.json").read_text())["network"]["threshold_f_wd"]
    flow["cand"] = np.where(flow.cls.isin(["no_edge_between_gate5_nodes", "reverse_only"]) & (flow.f_wd >= thr), flow.cls, "")
    maj = a.road_class.isin(gr.MAJOR)
    variants = {"obs_only": a[a.obs],
                "obs_plus_junction50": a[a.obs | (maj & (a.length_m <= 50))],
                "osm_major_all": a[maj]}
    grids = [("sq3000_model_origin", X0, Y0, 3000.0), ("sq3000_half_shift", X0 + 1500, Y0 + 1500, 3000.0),
             ("sq6000_model_origin", X0, Y0, 6000.0), ("sq6000_half_shift", X0 + 3000, Y0 + 3000, 6000.0)]
    res["scheme2"] = {}
    for k, d in variants.items():
        e = gr.osm_net(d, False)
        r = {"components": comps(e)}
        net = gr.Net(e)
        r["lcc"] = net.info
        for gk, x0, y0, s in grids:
            r[gk], _ = gr.grid_eval(net, x0, y0, s, flow if gk == "sq3000_model_origin" else None)
        print(k, json.dumps(r, ensure_ascii=False, default=float), flush=True)
        res["scheme2"][k] = r
        (out / "summary.json").write_text(json.dumps(res, indent=2, ensure_ascii=False, default=float))
    print("done")


if __name__ == "__main__":
    main()
