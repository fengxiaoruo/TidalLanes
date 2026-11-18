"""
Grid-chain versus direct-road OD travel times (OSM 1B)
======================================================
Question: the model builds OD costs by chaining adjacent-cell edge times over the grid
graph; does that reproduce OD times obtained by routing directly on the road network?

For every ordered pair of model cells (i != j) and periods AM / FF:
    T_road   direct shortest time on the 1B road network, mean over 3 x 3 access-node
             pairs (same access rule as the edge times tm_*)
    T_gsp    additive shortest path on the grid graph with the bundle edge times
             (tau_obs_min for AM, tau_ff_min for FF): isolates geometric gridding
    L_model  -log[(I - A)^{-1}]_ij with A_kl = 1/(kappa t_kl): the AA route-sum cost the
             model uses (multiplicative along paths); kappa from the 1B policy run
    L_best   min over grid paths of sum log(kappa t_kl): the single best multiplicative path
Reports correlations overall, commuter-weighted, by distance band, after two-way
(origin, destination) demeaning, and for directional asymmetry and AM-FF change.
Inputs read only. Output: data_work/outputs/<output-name>/ (refuses to overwrite)
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import dijkstra
from scipy.spatial import cKDTree

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src/network"))
import build_osm_grid_travel_time as bg  # noqa: E402

BUNDLE = ROOT / "outputs/model_input_osm1b_L300_gate5_od2025_v2/data"
ARCS = ROOT / "outputs/osm_grid_travel_time_v2/arc_speeds_1A.parquet"
KAPPA_FILE = ROOT / "outputs/policy_comparison_osm1b_ce_floor_b025_fl001_ks_od2025_v1"


def kappa_1b() -> float:
    for f in KAPPA_FILE.rglob("*.json"):
        try:
            k = json.loads(f.read_text()).get("kappa")
        except Exception:
            continue
        if isinstance(k, (int, float)):
            return float(k)
    raise SystemExit("kappa not found")


def access_nodes(G, x0, y0, size, cells):
    tree = cKDTree(np.c_[G.x, G.y])
    r = np.floor((G.y - y0) / size).astype(int)
    c = np.floor((G.x - x0) / size).astype(int)
    f = (np.arange(3) + 0.5) / 3
    acc = {}
    for g in cells:
        _, rr, cc = g.split("_"); rr, cc = int(rr), int(cc)
        pts = np.array([[x0 + (cc + fx) * size, y0 + (rr + fy) * size] for fx in f for fy in f])
        _, idx = tree.query(pts)
        idx = np.unique(idx[(r[idx] == rr) & (c[idx] == cc)])
        if len(idx):
            acc[g] = idx
    return acc


def road_matrix(G, acc, cells, period):
    W = G.arc_of[period][1]
    n = len(cells)
    T = np.full((n, n), np.nan)
    have = [k for k, g in enumerate(cells) if g in acc]
    dest_nodes = np.concatenate([acc[cells[k]] for k in have])
    dest_off = np.cumsum([0] + [len(acc[cells[k]]) for k in have])
    for b in range(0, len(have), 32):
        batch = have[b:b + 32]
        src = np.concatenate([acc[cells[k]] for k in batch])
        D = dijkstra(W, directed=True, indices=src, limit=400.0)[:, dest_nodes]
        so = np.cumsum([0] + [len(acc[cells[k]]) for k in batch])
        for bi, k in enumerate(batch):
            Dk = D[so[bi]:so[bi + 1]]
            for di, h in enumerate(have):
                m = Dk[:, dest_off[di]:dest_off[di + 1]]
                m = m[np.isfinite(m)]
                if len(m):
                    T[k, h] = m.mean()
        print(f"  road {period} {b + len(batch)}/{len(have)}", flush=True)
    return T


def two_way_resid(M, mask, iters=50):
    R = np.where(mask, M, np.nan)
    for _ in range(iters):
        R = R - np.nanmean(R, axis=1, keepdims=True)
        R = R - np.nanmean(R, axis=0, keepdims=True)
    return R


def corr(x, y, w=None):
    ok = np.isfinite(x) & np.isfinite(y)
    x, y = x[ok], y[ok]
    if w is None:
        return round(float(np.corrcoef(x, y)[0, 1]), 3)
    w = w[ok]; w = w / w.sum()
    mx, my = (w * x).sum(), (w * y).sum()
    return round(float((w * (x - mx) * (y - my)).sum() / np.sqrt((w * (x - mx) ** 2).sum() * (w * (y - my) ** 2).sum())), 3)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-name", default="grid_chain_vs_road_od_osm1b_v1")
    args = ap.parse_args(argv)
    out = ROOT / "outputs" / args.output_name
    if out.exists():
        raise SystemExit(f"{out} exists; choose a new --output-name")

    nodes = pd.read_parquet(BUNDLE / "qsm_input_nodes_square.parquet").sort_values("node_i")
    edges = pd.read_parquet(BUNDLE / "qsm_input_edges_square.parquet")
    od = pd.read_parquet(BUNDLE / "qsm_input_od_square.parquet")
    cells = nodes.grid_id.tolist()
    N = len(cells)
    kappa = kappa_1b()
    out.mkdir(parents=True)

    a = pd.read_parquet(ARCS)
    b = a[bg.gap_keep(a)].copy()
    G = bg.Graph(b)
    meta = json.loads(bg.MANIFEST.read_text())
    x0, y0 = meta["x_origin_3857"], meta["y_origin_3857"]
    acc = access_nodes(G, x0, y0, 3000.0, cells)

    # cell centres (UTM km) for straight-line distance
    rc = np.array([[int(s) for s in g.split("_")[1:]] for g in cells])
    cx, cy = bg.T3857_UTM.transform(x0 + (rc[:, 1] + 0.5) * 3000, y0 + (rc[:, 0] + 0.5) * 3000)
    dist = np.hypot(cx[:, None] - cx[None, :], cy[:, None] - cy[None, :]) / 1000

    flows = np.zeros((N, N))
    np.add.at(flows, (od.origin_node.to_numpy(), od.destination_node.to_numpy()), od.commuters_road.to_numpy())

    res = {"kappa": kappa, "cells": N, "edges": int(len(edges)), "cells_with_access": len(acc)}
    mats = {}
    for p, col in [("AM", "tau_obs_min"), ("FF", "tau_ff_min")]:
        t = edges[col].to_numpy()
        Wg = coo_matrix((t, (edges.i, edges.j)), shape=(N, N)).tocsr()
        Tg = dijkstra(Wg, directed=True)
        Wl = coo_matrix((np.log(kappa * t), (edges.i, edges.j)), shape=(N, N)).tocsr()
        Lb = dijkstra(Wl, directed=True)
        A = np.zeros((N, N)); np.add.at(A, (edges.i.to_numpy(), edges.j.to_numpy()), 1.0 / (kappa * t))
        X = np.linalg.solve(np.eye(N) - A, np.eye(N))
        Lm = -np.log(np.clip(X, 1e-300, None))
        Tr = road_matrix(G, acc, cells, p)
        mats[p] = dict(T_road=Tr, T_gsp=Tg, L_model=Lm, L_best=Lb)
        np.savez_compressed(out / f"matrices_{p}.npz", **mats[p], dist_km=dist, flows=flows)

    off = ~np.eye(N, dtype=bool)
    bands = [0, 5, 10, 20, 40, np.inf]
    for p, m in mats.items():
        Tr, Tg, Lm, Lb = m["T_road"], m["T_gsp"], m["L_model"], m["L_best"]
        ok = off & np.isfinite(Tr) & np.isfinite(Tg) & (Tr > 0)
        w = flows[ok]
        r = {"pairs": int(ok.sum()), "commuter_share_covered": round(float(flows[ok].sum() / flows[off].sum()), 4)}
        r["median_T_gsp_over_T_road"] = round(float(np.median(Tg[ok] / Tr[ok])), 3)
        q = Tg[ok] / Tr[ok]; o = np.argsort(q)
        r["wmedian_T_gsp_over_T_road"] = round(float(q[o][np.searchsorted(np.cumsum(w[o]) / w.sum(), 0.5)]), 3)
        r["corr_log_T_gsp_log_T_road"] = corr(np.log(Tg[ok]), np.log(Tr[ok]))
        r["corr_log_T_gsp_log_T_road_w"] = corr(np.log(Tg[ok]), np.log(Tr[ok]), w)
        r["corr_L_model_T_road"] = corr(Lm[ok], Tr[ok])
        r["corr_L_model_T_road_w"] = corr(Lm[ok], Tr[ok], w)
        r["corr_L_model_log_T_road"] = corr(Lm[ok], np.log(Tr[ok]))
        r["corr_L_model_T_gsp"] = corr(Lm[ok], Tg[ok])
        r["corr_L_model_L_best"] = corr(Lm[ok], Lb[ok])
        # two-way FE residuals (what the gravity equation uses)
        R_m = two_way_resid(Lm, ok); R_r = two_way_resid(Tr, ok); R_g = two_way_resid(Tg, ok)
        r["fe_corr_L_model_T_road"] = corr(R_m[ok], R_r[ok])
        r["fe_corr_L_model_T_road_w"] = corr(R_m[ok], R_r[ok], w)
        r["fe_corr_T_gsp_T_road"] = corr(R_g[ok], R_r[ok])
        # slope of L_model on T_road (per minute) and implied theta-scaled semi-elasticity
        X1 = Tr[ok]; Y1 = Lm[ok]
        r["slope_L_model_per_min_T_road"] = round(float(np.polyfit(X1, Y1, 1)[0]), 4)
        byband = {}
        for lo, hi in zip(bands[:-1], bands[1:]):
            s = ok & (dist >= lo) & (dist < hi)
            if s.sum() < 50:
                continue
            byband[f"{lo}-{hi}km"] = {
                "pairs": int(s.sum()), "commuter_share": round(float(flows[s].sum() / flows[ok].sum()), 3),
                "median_T_road": round(float(np.median(Tr[s])), 1),
                "median_T_gsp_over_T_road": round(float(np.median(Tg[s] / Tr[s])), 3),
                "corr_log_T_gsp_log_T_road": corr(np.log(Tg[s]), np.log(Tr[s])),
                "corr_L_model_T_road": corr(Lm[s], Tr[s]),
            }
        r["by_distance"] = byband
        # direction: log(T_ij / T_ji)
        up = ok & ok.T & np.triu(np.ones((N, N), bool), 1)
        r["dir_corr_gsp_road"] = corr(np.log(Tg[up] / Tg.T[up]), np.log(Tr[up] / Tr.T[up]))
        r["dir_corr_model_road"] = corr(Lm[up] - Lm.T[up], np.log(Tr[up] / Tr.T[up]))
        wu = (flows + flows.T)[up]
        r["dir_corr_model_road_w"] = corr(Lm[up] - Lm.T[up], np.log(Tr[up] / Tr.T[up]), wu)
        res[p] = r
    # AM - FF congestion change, and tidal (direction x period) component
    am, ff = mats["AM"], mats["FF"]
    ok = off & np.isfinite(am["T_road"]) & np.isfinite(ff["T_road"]) & np.isfinite(am["T_gsp"])
    dr = np.log(am["T_road"] / ff["T_road"]); dg = np.log(am["T_gsp"] / ff["T_gsp"]); dm = am["L_model"] - ff["L_model"]
    res["AM_minus_FF"] = {"corr_gsp_road": corr(dg[ok], dr[ok]), "corr_model_road": corr(dm[ok], dr[ok]),
                          "corr_model_road_w": corr(dm[ok], dr[ok], flows[ok])}
    up = ok & ok.T & np.triu(np.ones((N, N), bool), 1)
    tr = dr - dr.T; tg = dg - dg.T; tm = dm - dm.T
    res["AM_minus_FF"]["dir_corr_gsp_road"] = corr(tg[up], tr[up])
    res["AM_minus_FF"]["dir_corr_model_road"] = corr(tm[up], tr[up])
    (out / "summary.json").write_text(json.dumps(res, indent=2, ensure_ascii=False))
    print(json.dumps(res, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
