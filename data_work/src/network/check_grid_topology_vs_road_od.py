"""
Grid-chain versus direct-road OD times by grid topology (OSM 1B)
================================================================
Follow-up of check_grid_chain_vs_road_od.py (2026-11-10). Topologies:
    sq   square 3 km cells from a model bundle (4- or 8-neighbour edges as in the bundle)
    hex  hexagons of the same area (3000^2 in EPSG:3857 units), cells whose centre lies
         in a model-bundle square; 6-neighbour edges built here with the bundle rules
         (access-node mean times, centre-node path detour gate 5, largest strongly
         connected component)
Two edge definitions, each compared with the direct road time of the same definition so
that the validation logic is consistent:
    access  edge = mean time over access-node pairs (what the model uses); direct road
            time = mean over access-node pairs of origin and destination cells
    centre  edge = centre-node to centre-node time; direct road = centre to centre
Objects per OD pair: T_road (direct), T_gsp (additive grid shortest path),
L_model = -log[(I - A)^{-1}]_ij with A = 1/(kappa t), kappa = smallest value with
spectral radius <= 0.95 (the mainline rule).
Almagro et al. (2026, App. F.2) correction: Poisson fit of T_road on log straight-line
distance with log T_gsp as offset, E[T_road] = T_gsp exp(a + b log d); reported for
validation only (the AA route-sum model has no OD-level slot for it).
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
from scipy.sparse.csgraph import connected_components, dijkstra
from scipy.spatial import cKDTree

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src/network"))
import build_osm_grid_travel_time as bg  # noqa: E402

O = ROOT / "outputs"
ARCS = O / "osm_grid_travel_time_v2/arc_speeds_1A.parquet"
SQ4_BUNDLE = O / "model_input_osm1b_L300_gate5_od2025_v2"
SIZE = 3000.0
GATE = 5.0
RHO = 0.95
SQRT3 = np.sqrt(3.0)


# ------------------------------------------------------------------ geometry
class Cells:
    """cells: ids, centres (3857), membership of road nodes, access nodes, centre node."""

    def __init__(self, ids, cx, cy, member, lattice):
        self.ids, self.cx, self.cy = list(ids), np.asarray(cx), np.asarray(cy)
        self.member = member          # road node -> cell index or -1
        self.lattice = lattice        # list of (n_pts, 2) arrays per cell


def square_cells(G, ids, x0, y0):
    rc = np.array([[int(s) for s in g.split("_")[1:]] for g in ids])
    cx, cy = x0 + (rc[:, 1] + 0.5) * SIZE, y0 + (rc[:, 0] + 0.5) * SIZE
    r = np.floor((G.y - y0) / SIZE).astype(int); c = np.floor((G.x - x0) / SIZE).astype(int)
    pos = {(a, b): k for k, (a, b) in enumerate(rc)}
    member = np.array([pos.get((a, b), -1) for a, b in zip(r, c)])
    f = (np.arange(3) + 0.5) / 3
    lattice = [np.array([[x0 + (cc + fx) * SIZE, y0 + (rr + fy) * SIZE] for fx in f for fy in f]) for rr, cc in rc]
    return Cells(ids, cx, cy, member, lattice), rc


def hex_cells(G, sq_ids, x0, y0):
    R = np.sqrt(2 * SIZE ** 2 / (3 * SQRT3))           # pointy-top hexagon of area SIZE^2
    rc = np.array([[int(s) for s in g.split("_")[1:]] for g in sq_ids])
    xmin, xmax = x0 + rc[:, 1].min() * SIZE, x0 + (rc[:, 1].max() + 1) * SIZE
    ymin, ymax = y0 + rc[:, 0].min() * SIZE, y0 + (rc[:, 0].max() + 1) * SIZE
    dx, dy = SQRT3 * R, 1.5 * R
    cand = []
    for row in range(int((ymax - ymin) / dy) + 3):
        for col in range(int((xmax - xmin) / dx) + 3):
            cand.append((row, col, xmin + col * dx + (dx / 2 if row % 2 else 0.0), ymin + row * dy))
    cand = pd.DataFrame(cand, columns=["row", "col", "x", "y"])
    sq = set(map(tuple, rc))
    keep = [(int(np.floor((y - y0) / SIZE)), int(np.floor((x - x0) / SIZE))) in sq for x, y in zip(cand.x, cand.y)]
    cand = cand[keep].reset_index(drop=True)
    ids = [f"hx_{a}_{b}" for a, b in zip(cand.row, cand.col)]
    # a point belongs to the nearest hexagon centre (Voronoi cell of the lattice = hexagon);
    # nodes nearer a dropped candidate are outside all kept cells
    allc = []
    for row in range(-1, int((ymax - ymin) / dy) + 4):
        for col in range(-1, int((xmax - xmin) / dx) + 4):
            allc.append((xmin + col * dx + (dx / 2 if row % 2 else 0.0), ymin + row * dy))
    allc = np.array(allc)
    _, near = cKDTree(allc).query(np.c_[G.x, G.y])
    lut = {(round(x, 3), round(y, 3)): k for k, (x, y) in enumerate(zip(cand.x, cand.y))}
    member = np.array([lut.get((round(allc[n, 0], 3), round(allc[n, 1], 3)), -1) for n in near])
    ang = np.deg2rad(np.arange(6) * 60 + 30)
    lattice = [np.r_[[[x, y]], np.c_[x + 0.55 * R * np.cos(ang), y + 0.55 * R * np.sin(ang)]] for x, y in zip(cand.x, cand.y)]
    cells = Cells(ids, cand.x.to_numpy(), cand.y.to_numpy(), member, lattice)
    nb = []
    pos = {(a, b): k for k, (a, b) in enumerate(zip(cand.row, cand.col))}
    for k, (a, b) in enumerate(zip(cand.row, cand.col)):
        offs = [(0, 1), (0, -1), (1, 0), (-1, 0), (1, -1), (-1, -1)] if a % 2 == 0 else [(0, 1), (0, -1), (1, 0), (-1, 0), (1, 1), (-1, 1)]
        for da, db in offs:
            j = pos.get((a + da, b + db))
            if j is not None:
                nb.append((k, j))
    return cells, np.array(nb), R


def access_nodes(G, cells):
    tree = cKDTree(np.c_[G.x, G.y])
    acc, centre = {}, {}
    by_cell = pd.Series(np.arange(G.N)).groupby(cells.member).apply(np.asarray)
    for k in range(len(cells.ids)):
        if k not in by_cell.index:
            continue
        _, idx = tree.query(cells.lattice[k])
        idx = np.unique(idx[cells.member[idx] == k])
        nodes = by_cell[k]
        centre[k] = int(nodes[np.argmin((G.x[nodes] - cells.cx[k]) ** 2 + (G.y[nodes] - cells.cy[k]) ** 2)])
        if len(idx):
            acc[k] = idx
    return acc, centre


# ------------------------------------------------------------------ routing
def road_access_matrix(W, acc, n, limit=400.0, tag=""):
    T = np.full((n, n), np.nan)
    have = sorted(acc)
    dest = np.concatenate([acc[k] for k in have]); doff = np.cumsum([0] + [len(acc[k]) for k in have])
    for b in range(0, len(have), 32):
        batch = have[b:b + 32]
        D = dijkstra(W, directed=True, indices=np.concatenate([acc[k] for k in batch]), limit=limit)[:, dest]
        so = np.cumsum([0] + [len(acc[k]) for k in batch])
        for bi, k in enumerate(batch):
            Dk = D[so[bi]:so[bi + 1]]
            for di, h in enumerate(have):
                m = Dk[:, doff[di]:doff[di + 1]]
                m = m[np.isfinite(m)]
                if len(m):
                    T[k, h] = m.mean()
        if (b // 32) % 10 == 0:
            print(f"  road access {tag} {b + len(batch)}/{len(have)}", flush=True)
    return T


def road_centre_matrix(W, centre, n):
    T = np.full((n, n), np.nan)
    ks = sorted(centre)
    D = dijkstra(W, directed=True, indices=[centre[k] for k in ks])
    cols = [centre[k] for k in ks]
    T[np.ix_(ks, ks)] = D[:, cols]
    return T


def centre_path_km(G, centre, pairs):
    arcs, W = G.arc_of["AM"]
    aidx = pd.Series(arcs.i.to_numpy(), index=pd.MultiIndex.from_arrays([arcs.u.to_numpy(), arcs.v.to_numpy()]))
    km = G.a.length_m.to_numpy() / 1000
    out = np.full(len(pairs), np.nan)
    src = sorted(set(pairs[:, 0]))
    for b in range(0, len(src), 64):
        batch = src[b:b + 64]
        D, pred = dijkstra(W, directed=True, indices=[centre[k] for k in batch], return_predecessors=True, limit=120.0)
        row = {k: i for i, k in enumerate(batch)}
        for e in np.where(np.isin(pairs[:, 0], batch))[0]:
            i, j = pairs[e]
            s, v = centre[i], centre[j]
            if not np.isfinite(D[row[i], v]):
                continue
            us, vs = [], []
            while v != s:
                u = pred[row[i], v]; us.append(u); vs.append(v); v = u
            ids = aidx.reindex(pd.MultiIndex.from_arrays([us, vs])).to_numpy()
            out[e] = km[ids].sum()
    return out


# ------------------------------------------------------------------ statistics
def corr(x, y, w=None):
    ok = np.isfinite(x) & np.isfinite(y)
    x, y = x[ok], y[ok]
    if w is None:
        return round(float(np.corrcoef(x, y)[0, 1]), 3)
    w = w[ok] / w[ok].sum()
    mx, my = (w * x).sum(), (w * y).sum()
    return round(float((w * (x - mx) * (y - my)).sum() / np.sqrt((w * (x - mx) ** 2).sum() * (w * (y - my) ** 2).sum())), 3)


def wmedian(q, w):
    o = np.argsort(q)
    return round(float(q[o][np.searchsorted(np.cumsum(w[o]) / w.sum(), 0.5)]), 3)


def two_way_resid(M, mask, iters=50):
    R = np.where(mask, M, np.nan)
    for _ in range(iters):
        R = R - np.nanmean(R, axis=1, keepdims=True)
        R = R - np.nanmean(R, axis=0, keepdims=True)
    return R


def poisson_offset(y, offset, x, w=None, iters=50):
    """Poisson GLM y ~ exp(offset + a + b x) by Newton; returns (a, b)."""
    X = np.c_[np.ones_like(x), x]
    w = np.ones_like(y) if w is None else w
    beta = np.zeros(2)
    beta[0] = np.log((w * y).sum() / (w * np.exp(offset)).sum())
    for _ in range(iters):
        mu = np.exp(offset + X @ beta)
        g = X.T @ (w * (y - mu)); H = (X * (w * mu)[:, None]).T @ X
        step = np.linalg.solve(H, g); beta += step
        if np.abs(step).max() < 1e-10:
            break
    return float(beta[0]), float(beta[1])


def spectral_kappa(edges_i, edges_j, t_floor, N):
    """mainline rule: smallest kappa >= 1 with rho(A) <= 0.95 at the floor times
    min(t_AM, t_FF); one kappa for both periods so AM - FF differences are not
    contaminated by a kappa change."""
    A1 = np.zeros((N, N)); np.add.at(A1, (edges_i, edges_j), 1.0 / t_floor)
    rho1 = float(np.max(np.abs(np.linalg.eigvals(A1))))
    return max(1.0, rho1 / RHO), rho1


def model_objects(edges_i, edges_j, t, N, kappa):
    Wg = coo_matrix((t, (edges_i, edges_j)), shape=(N, N)).tocsr()
    Tg = dijkstra(Wg, directed=True)
    A1 = np.zeros((N, N)); np.add.at(A1, (edges_i, edges_j), 1.0 / t)
    X = np.linalg.solve(np.eye(N) - A1 / kappa, np.eye(N))
    return Tg, -np.log(np.clip(X, 1e-300, None))


def evaluate(Tr, Tg, Lm, dist, flows, off):
    ok = off & np.isfinite(Tr) & np.isfinite(Tg) & (Tr > 0)
    w = flows[ok]
    q = Tg[ok] / Tr[ok]
    r = {"pairs": int(ok.sum()), "commuter_share_covered": round(float(flows[ok].sum() / flows[off].sum()), 4),
         "median_T_gsp_over_T_road": round(float(np.median(q)), 3), "wmedian_T_gsp_over_T_road": wmedian(q, w),
         "corr_log_T_gsp_log_T_road": corr(np.log(Tg[ok]), np.log(Tr[ok])),
         "corr_L_model_T_road": corr(Lm[ok], Tr[ok]), "corr_L_model_T_road_w": corr(Lm[ok], Tr[ok], w)}
    Rm, Rr, Rg = two_way_resid(Lm, ok), two_way_resid(Tr, ok), two_way_resid(Tg, ok)
    r["fe_corr_L_model_T_road"] = corr(Rm[ok], Rr[ok]); r["fe_corr_L_model_T_road_w"] = corr(Rm[ok], Rr[ok], w)
    r["fe_corr_T_gsp_T_road"] = corr(Rg[ok], Rr[ok])
    # Almagro et al. App. F.2 correction, fitted on pairs up to 40 km (commuting range)
    s = ok & (dist > 0) & (dist < 40)
    for lab, ww in [("unweighted", None), ("commuter_weighted", flows[s] + 1e-9)]:
        a, b = poisson_offset(Tr[s], np.log(Tg[s]), np.log(dist[s]), ww)
        psi = np.exp(a + b * np.log(np.where(dist > 0, dist, np.nan)))
        qc = (Tg * psi)[ok] / Tr[ok]
        r[f"almagro_{lab}"] = {"a": round(a, 4), "b": round(b, 4),
                               "psi_at_3_10_30km": [round(float(np.exp(a + b * np.log(d))), 3) for d in (3, 10, 30)],
                               "wmedian_corrected_ratio": wmedian(qc, w),
                               "corr_log_corrected": corr(np.log((Tg * psi)[ok]), np.log(Tr[ok]))}
    bands, byband = [0, 5, 10, 20, 40, np.inf], {}
    for lo, hi in zip(bands[:-1], bands[1:]):
        s = ok & (dist >= lo) & (dist < hi)
        if s.sum() < 50:
            continue
        byband[f"{lo}-{hi}km"] = {"pairs": int(s.sum()), "commuter_share": round(float(flows[s].sum() / flows[ok].sum()), 3),
                                  "median_T_road": round(float(np.median(Tr[s])), 1),
                                  "median_T_gsp_over_T_road": round(float(np.median(Tg[s] / Tr[s])), 3),
                                  "corr_log_T_gsp_log_T_road": corr(np.log(Tg[s]), np.log(Tr[s])),
                                  "corr_L_model_T_road": corr(Lm[s], Tr[s])}
    r["by_distance"] = byband
    N = Tr.shape[0]
    up = ok & ok.T & np.triu(np.ones((N, N), bool), 1)
    r["dir_corr_gsp_road"] = corr(np.log(Tg[up] / Tg.T[up]), np.log(Tr[up] / Tr.T[up]))
    r["dir_corr_model_road"] = corr(Lm[up] - Lm.T[up], np.log(Tr[up] / Tr.T[up]))
    return r, ok


def tidal(am, ff, ok_am, ok_ff, flows):
    ok = ok_am & ok_ff
    N = ok.shape[0]
    dr = np.log(am["T_road"] / ff["T_road"]); dg = np.log(am["T_gsp"] / ff["T_gsp"]); dm = am["L_model"] - ff["L_model"]
    up = ok & ok.T & np.triu(np.ones((N, N), bool), 1)
    return {"corr_gsp_road": corr(dg[ok], dr[ok]), "corr_model_road": corr(dm[ok], dr[ok]),
            "corr_model_road_w": corr(dm[ok], dr[ok], flows[ok]),
            "dir_corr_gsp_road": corr((dg - dg.T)[up], (dr - dr.T)[up]),
            "dir_corr_model_road": corr((dm - dm.T)[up], (dr - dr.T)[up])}


# ------------------------------------------------------------------ main
def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--topology", choices=["sq", "hex"], required=True)
    ap.add_argument("--bundle", default=SQ4_BUNDLE.name, help="square bundle (sq) whose edges are evaluated")
    ap.add_argument("--reuse-road", default="grid_chain_vs_road_od_osm1b_v1",
                    help="output with access-mean road matrices for the sq4 cell set ('' to recompute)")
    ap.add_argument("--output-name", required=True)
    args = ap.parse_args(argv)
    out = O / args.output_name
    if out.exists():
        raise SystemExit(f"{out} exists; choose a new --output-name")

    a = pd.read_parquet(ARCS)
    G = bg.Graph(a[bg.gap_keep(a)].copy())
    meta = json.loads(bg.MANIFEST.read_text())
    x0, y0 = meta["x_origin_3857"], meta["y_origin_3857"]
    sq4_nodes = pd.read_parquet(SQ4_BUNDLE / "data/qsm_input_nodes_square.parquet").sort_values("node_i")
    sq_od = pd.read_parquet(SQ4_BUNDLE / "data/qsm_input_od_square.parquet")
    res = {"topology": args.topology}

    if args.topology == "sq":
        b = O / args.bundle / "data"
        nodes = pd.read_parquet(b / "qsm_input_nodes_square.parquet").sort_values("node_i")
        E = pd.read_parquet(b / "qsm_input_edges_square.parquet")
        cells, _ = square_cells(G, nodes.grid_id.tolist(), x0, y0)
        N = len(cells.ids)
        ei, ej = E.i.to_numpy(), E.j.to_numpy()
        t_acc = {"AM": E.tau_obs_min.to_numpy(), "FF": E.tau_ff_min.to_numpy()}
        idx = dict(zip(cells.ids, range(N)))
        flows = np.zeros((N, N))
        m = sq_od.home_grid.isin(idx) & sq_od.work_grid.isin(idx)
        np.add.at(flows, (sq_od.home_grid[m].map(idx).to_numpy(), sq_od.work_grid[m].map(idx).to_numpy()), sq_od.commuters_road[m].to_numpy())
        res.update({"bundle": args.bundle, "edges": int(len(E)),
                    "diagonal_edges": int(sum(abs(int(g.split("_")[1]) - int(h.split("_")[1])) == 1 and abs(int(g.split("_")[2]) - int(h.split("_")[2])) == 1 for g, h in zip(E.grid_o, E.grid_d)))})
    else:
        cells, nb, R = hex_cells(G, sq4_nodes.grid_id.tolist(), x0, y0)
        res["hex_circumradius_3857"] = round(float(R), 1)
    N = len(cells.ids)
    acc, centre = access_nodes(G, cells)
    out.mkdir(parents=True)

    road = {}
    for p in ["AM", "FF"]:
        W = G.arc_of[p][1]
        road[("centre", p)] = road_centre_matrix(W, centre, N)
    reuse = O / args.reuse_road if args.reuse_road else None
    if args.topology == "sq" and reuse and (reuse / "matrices_AM.npz").exists() and nodes.grid_id.tolist() == sq4_nodes.grid_id.tolist():
        for p in ["AM", "FF"]:
            road[("access", p)] = np.load(reuse / f"matrices_{p}.npz")["T_road"]
        res["road_access_reused_from"] = reuse.name
    else:
        for p in ["AM", "FF"]:
            road[("access", p)] = road_access_matrix(G.arc_of[p][1], acc, N, tag=p)

    if args.topology == "hex":
        # edges: access-mean times, centre-path detour gate, largest SCC
        nb = nb[[(i in acc) and (j in acc) and (i in centre) and (j in centre) for i, j in nb]]
        t_am, t_ff = road[("access", "AM")][nb[:, 0], nb[:, 1]], road[("access", "FF")][nb[:, 0], nb[:, 1]]
        dist_c = np.hypot(cells.cx[nb[:, 0]] - cells.cx[nb[:, 1]], cells.cy[nb[:, 0]] - cells.cy[nb[:, 1]])
        ux, uy = bg.T3857_UTM.transform(cells.cx, cells.cy)
        dkm = np.hypot(ux[nb[:, 0]] - ux[nb[:, 1]], uy[nb[:, 0]] - uy[nb[:, 1]]) / 1000
        pkm = centre_path_km(G, centre, nb)
        keep = np.isfinite(t_am) & np.isfinite(t_ff) & (t_am > 0) & np.isfinite(pkm) & (pkm / dkm <= GATE)
        res.update({"hex_pairs": int(len(nb)), "hex_pairs_dropped_detour_or_unroutable": int((~keep).sum())})
        nb = nb[keep]; t_am, t_ff = t_am[keep], t_ff[keep]
        g = coo_matrix((np.ones(len(nb)), (nb[:, 0], nb[:, 1])), shape=(N, N))
        _, lab = connected_components(g, directed=True, connection="strong")
        big = lab == np.bincount(lab).argmax()
        ks = np.where(big)[0]
        remap = -np.ones(N, int); remap[ks] = np.arange(len(ks))
        e_ok = big[nb[:, 0]] & big[nb[:, 1]]
        ei, ej = remap[nb[e_ok, 0]], remap[nb[e_ok, 1]]
        t_acc = {"AM": t_am[e_ok], "FF": t_ff[e_ok]}
        road = {k: v[np.ix_(ks, ks)] for k, v in road.items()}
        cells.ids = [cells.ids[k] for k in ks]; cells.cx, cells.cy = cells.cx[ks], cells.cy[ks]
        N = len(ks)
        # commuter weights: square OD flows assigned by square-centre location to hexagons
        rc = np.array([[int(s) for s in gid.split("_")[1:]] for gid in sq4_nodes.grid_id])
        sx, sy = x0 + (rc[:, 1] + 0.5) * SIZE, y0 + (rc[:, 0] + 0.5) * SIZE
        _, hk = cKDTree(np.c_[cells.cx, cells.cy]).query(np.c_[sx, sy])
        sq_to_hex = dict(zip(sq4_nodes.grid_id, hk))
        flows = np.zeros((N, N))
        m = sq_od.home_grid.isin(sq_to_hex) & sq_od.work_grid.isin(sq_to_hex)
        np.add.at(flows, (sq_od.home_grid[m].map(sq_to_hex).to_numpy(), sq_od.work_grid[m].map(sq_to_hex).to_numpy()), sq_od.commuters_road[m].to_numpy())
        res.update({"cells": N, "edges": int(len(ei)), "median_edge_t_AM": round(float(np.median(t_acc["AM"])), 2)})
        pd.DataFrame({"grid_o": [cells.ids[i] for i in ei], "grid_d": [cells.ids[j] for j in ej],
                      "tm_AM": t_acc["AM"], "tm_FF": t_acc["FF"]}).to_parquet(out / "hex_edges.parquet", index=False)
    else:
        res["cells"] = N

    ux, uy = bg.T3857_UTM.transform(cells.cx, cells.cy)
    dist = np.hypot(ux[:, None] - ux[None, :], uy[:, None] - uy[None, :]) / 1000
    off = ~np.eye(N, dtype=bool)
    for edef in ["access", "centre"]:
        mats, oks = {}, {}
        tt = {p: (t_acc[p] if edef == "access" else road[("centre", p)][ei, ej]) for p in ["AM", "FF"]}
        good = np.isfinite(tt["AM"]) & np.isfinite(tt["FF"]) & (tt["AM"] > 0) & (tt["FF"] > 0)
        kappa, rho1 = spectral_kappa(ei[good], ej[good], np.minimum(tt["AM"], tt["FF"])[good], N)
        for p in ["AM", "FF"]:
            t = tt[p]
            Tg, Lm = model_objects(ei[good], ej[good], t[good], N, kappa)
            Tr = road[(edef, p)]
            r, ok = evaluate(Tr, Tg, Lm, dist, flows, off)
            r.update({"kappa": round(kappa, 4), "rho_floor_kappa1": round(rho1, 4), "median_edge_t": round(float(np.median(t[good])), 2)})
            res[f"{edef}_{p}"] = r
            mats[p] = {"T_road": Tr, "T_gsp": Tg, "L_model": Lm}; oks[p] = ok
            np.savez_compressed(out / f"matrices_{edef}_{p}.npz", **mats[p], dist_km=dist, flows=flows)
        res[f"{edef}_AM_minus_FF"] = tidal(mats["AM"], mats["FF"], oks["AM"], oks["FF"], flows)
        (out / "summary.json").write_text(json.dumps(res, indent=2, ensure_ascii=False))
    pd.Series(cells.ids).to_csv(out / "cells.csv", index=False, header=["grid_id"])
    print(json.dumps(res, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
