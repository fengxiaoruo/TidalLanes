"""
Far-OD routed instrument (nextstep A3 (a) remainder)
====================================================

The routed-OD instrument z (run_od_iv_hypercongestion_gate5.py) adds every routed trip
to every edge on its free-flow path. Trips that start or end next to an edge share
local shocks with that edge (an event, a local demand surge, the same SIM sample).
Here z is rebuilt for each edge from trips whose origin cell AND destination cell are
more than D cells (Chebyshev, 3 km cells) from both end cells of the edge:
    D = -1  baseline (all trips; must reproduce the existing z)
    D =  0  drop trips starting or ending in the edge's own two cells
    D =  1  also drop trips from / to the 8 neighbouring cells (about 3-6 km)
    D =  2  also the next ring (about 6-9 km)
Same OD cache, cells, network, free-flow paths and time shift as the existing z.
Linear IV re-estimated on a common sample (unit-hours with z > 0 under every D):
edge V1 (edge FE + date x hour FE) and segment S1 (segment FE + date x hour FE),
main window 06-22 h, 00-24 h as robustness.
Inputs read only. Output: data_work/outputs/<output-name>/ (refuses to overwrite)
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import sparse
from scipy.sparse.csgraph import dijkstra

sys.path.insert(0, str(Path(__file__).resolve().parent))
from run_all_traffic_speed_panel_gate5 import EDGES, REPO, ROOT, slope, within  # noqa: E402
import run_od_iv_hypercongestion_gate5 as odiv  # noqa: E402
import run_congestion_tail_followup as tf  # noqa: E402

D_LIST = [-1, 0, 1, 2]
K = ["grid_from", "grid_to", "date", "hour"]


def rc(cells: pd.Series) -> np.ndarray:
    p = cells.str.split("_", expand=True)
    return np.c_[p[1].astype(int), p[2].astype(int)]


def routed_far(od: pd.DataFrame, nodes: list[str], edges: pd.DataFrame) -> tuple[dict, dict]:
    """As odiv.routed_flow, but returns hourly z for each exclusion radius D."""
    ix = {g: i for i, g in enumerate(nodes)}
    n = len(nodes)
    xy = rc(pd.Series(nodes))
    u, v = edges.grid_from.map(ix).to_numpy(), edges.grid_to.map(ix).to_numpy()
    G = sparse.csr_matrix((edges.tau_ff_min.to_numpy(), (u, v)), shape=(n, n))
    dist, pred = dijkstra(G, directed=True, return_predecessors=True)
    eid = np.full((n, n), -1, dtype=np.int32)
    eid[u, v] = np.arange(len(edges))

    od = od.assign(oi=od.o.map(ix), di=od.d.map(ix))
    pairs = od[["oi", "di"]].drop_duplicates().reset_index(drop=True)
    pairs = pairs[np.isfinite(dist[pairs.oi, pairs.di])].reset_index(drop=True)
    pairs["p"] = np.arange(len(pairs))
    od = od.merge(pairs, on=["oi", "di"])
    T = len(odiv.DATES) * 144
    od["t"] = od.date.map({d: k for k, d in enumerate(odiv.DATES)}) * 144 + od.slot
    OD = sparse.csr_matrix((od.trips.to_numpy(), (od.p.to_numpy(), od.t.to_numpy())), shape=(len(pairs), T))

    o, cur = pairs.oi.to_numpy(), pairs.di.to_numpy().copy()
    dd = pairs.di.to_numpy()
    rec_p, rec_e, rec_k, alive = [], [], [], np.ones(len(pairs), bool)
    while alive.any():
        a = np.flatnonzero(alive)
        prev = pred[o[a], cur[a]]
        rec_p.append(a)
        rec_e.append(eid[prev, cur[a]])
        rec_k.append(np.floor((5 + dist[o[a], prev]) / 10).astype(int))
        cur[a] = prev
        alive[a] = prev != o[a]
    P, E, Kk = np.concatenate(rec_p), np.concatenate(rec_e), np.concatenate(rec_k)

    def cheb(a_nodes, b_nodes):
        return np.abs(xy[a_nodes] - xy[b_nodes]).max(axis=1)
    eu, ev = u[E], v[E]
    dist_o = np.minimum(cheb(o[P], eu), cheb(o[P], ev))
    dist_d = np.minimum(cheb(dd[P], eu), cheb(dd[P], ev))
    near = np.minimum(dist_o, dist_d)

    hours = np.repeat(np.arange(T // 6), 6)
    out, info = {}, {"od_pairs_routed": int(len(pairs)), "path_records": int(len(P))}
    trip_rec = np.asarray(OD.sum(axis=1)).ravel()[P]
    for D in D_LIST:
        keep = near > D
        Z = np.zeros((len(edges), T))
        for k in np.unique(Kk[keep]):
            s = keep & (Kk == k)
            A = sparse.csr_matrix((np.ones(s.sum()), (E[s], P[s])), shape=(len(edges), len(pairs)))
            Zk = (A @ OD).toarray()
            Z[:, k:] += Zk[:, :T - k]
        Zh = np.add.reduceat(Z, np.arange(0, T, 6), axis=1)          # edges x hours
        H = Zh.shape[1]
        out[D] = pd.DataFrame({"grid_from": np.repeat(edges.grid_from.values, H), "grid_to": np.repeat(edges.grid_to.values, H),
                               "date": np.tile(np.repeat(odiv.DATES, 24), len(edges)).astype("int64"),
                               "hour": np.tile(np.arange(H) % 24, len(edges)).astype("int64"), f"z{D}": Zh.ravel()})
        info[f"D{D}_share_of_trip_edge_records_kept"] = round(float(trip_rec[keep].sum() / trip_rec.sum()), 4)
        print(f"D={D} done; trip-edge share kept {info[f'D{D}_share_of_trip_edge_records_kept']}", flush=True)
    del hours
    return out, info


def estimate(h: pd.DataFrame, unit: str, cl: str, zcols: list[str]) -> dict:
    s = h[(h[zcols] > 0).all(axis=1)].copy()
    s["y"], s["x"] = np.log(s.pace), np.log(s.flow)
    for c in zcols:
        s["l" + c] = np.log(s[c])
    r = within(s, ["y", "x"] + ["l" + c for c in zcols], [[unit], ["date", "hour"]])
    o = {"n_common": int(len(s)), "units": int(s[unit].nunique()), "n_full_before_common": int(len(h)),
         "OLS": slope(r.y, r.x, s[cl])}
    for c in zcols:
        o[f"IV_{c}"] = slope(r.y, r.x, s[cl], z=r["l" + c])
        o[f"IV_{c}"]["first_stage_coef"] = round(float((r["l" + c] @ r.x) / (r["l" + c] @ r["l" + c])), 4)
        o[f"corr_resid_{c}_vs_z-1"] = round(float(np.corrcoef(r["l" + c], r["lz-1"])[0, 1]), 3)
    return o


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-name", default="congestion_far_od_iv_v1")
    args = ap.parse_args(argv)
    out = ROOT / "outputs" / args.output_name
    if out.exists():
        raise SystemExit(f"{out} exists; choose a new --output-name")
    out.mkdir(parents=True)

    meta = json.loads((REPO / "Platform/reference_inputs/manifest.json").read_text())
    nodes = pd.read_parquet(odiv.NODES, columns=["grid_id"]).grid_id.tolist()
    edges = pd.read_parquet(EDGES, columns=["grid_o", "grid_d", "tau_ff_min"]).rename(columns={"grid_o": "grid_from", "grid_d": "grid_to"})
    od, _ = odiv.grid_od(meta, set(nodes))
    zs, info = routed_far(od, nodes, edges)
    z = zs[D_LIST[0]]
    for D in D_LIST[1:]:
        z = z.merge(zs[D], on=K)
    z.to_parquet(out / "edge_far_od_z_hour.parquet", index=False)
    zcols = [f"z{D}" for D in D_LIST]

    # check: D = -1 reproduces the existing z
    old = pd.read_parquet(tf.GATE5 / "od_iv/edge_routed_od_flow_10min.parquet")
    old["hour"] = old.slot10 // 6
    old = old.groupby(K).z_routed.sum().reset_index()
    chk = old.merge(z[K + ["z-1"]], on=K)
    info["baseline_max_abs_diff"] = float((chk.z_routed - chk["z-1"]).abs().max())
    info["z_share_of_baseline_total"] = {c: round(float(z[c].sum() / z["z-1"].sum()), 4) for c in zcols}
    print(json.dumps(info), flush=True)

    res = {"routing": info}
    t = tf.edge10()
    for lab, hrs in [("06-22h", (6, 21)), ("00-24h", (0, 23))]:
        e = tf.edge_hourly(t, hrs).drop(columns="z").merge(z, on=K, how="left").fillna({c: 0.0 for c in zcols})
        res[f"edge_V1_{lab}"] = estimate(e, "unit", "unit", zcols)
        sg = tf.seg_panel(hrs).drop(columns=["z_routed", "zr"]).merge(z, on=K, how="left")
        sg = sg[sg.gedge != ">"]
        res[f"segment_S1_{lab}"] = estimate(sg, "roadseg_id", "gedge", zcols)
        print(lab, json.dumps({k: {c: res[k][f"IV_{c}"]["b"] for c in zcols} for k in [f"edge_V1_{lab}", f"segment_S1_{lab}"]}), flush=True)
        (out / "summary.json").write_text(json.dumps(res, indent=2, ensure_ascii=False))
    print("done")


if __name__ == "__main__":
    main()
