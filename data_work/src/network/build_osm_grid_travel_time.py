"""
OSM arc speeds (schemes 1A and 1B) and directed adjacent grid-pair travel times
==============================================================================
(nextstep D steps 2-3; rules in Documents/L2_notes/observed_vs_imputed_data_use_20261106.md)

Arc speeds, three periods, weekdays 0519-0523: AM = 07-09 h, PM = 17-19 h, FF = 02-05 h.
Observed arcs (osm_directed_speed_v1) take the mean pace of their Baidu segment.
Unobserved arcs (classes tertiary and above):
    free-flow speed  = median observed FF speed of the same class x zone (fallback class)
    period speed     = free-flow speed x borrowed relative speed r = v_period / v_FF
    r                = hop-weighted (1/h) mean of r over observed arcs of the same class
                       family reachable within 3 hops upstream or downstream
                       (speed_source = "neighbour"); otherwise the median r of the same
                       family x zone x period (speed_source = "class_zone")
    zone             = distance band from Tiananmen: <5, 5-9, 9-15, 15-25, >=25 km
Schemes:
    1B  observed arcs plus unobserved arcs whose both ends are within L = 300 m (along
        major roads, undirected) of an observed arc; the added arcs carry the imputed
        speed (speed_source "short_gap")
    1A  all arcs tertiary and above
Grid pairs: largest strongly connected component of each scheme; square grids in
EPSG:3857 (model origin 3 km and half-cell shift, 6 km); access node = road node nearest
the cell centre; for each directed 4-neighbour pair (8 with --neighbours 8), shortest-time path per period,
path km, and the share of path time on observed arcs (centre-node path); plus tm_* =
mean time over all access-node pairs, access nodes = road nodes nearest a 3 x 3 lattice
inside each cell (the main travel-time measure; the single centre node is too noisy). The model-origin 3 km pairs are
compared with the current gate5 (v1) bundle and the v3 gate5 bundle (AM time and FF).
Inputs read only. Output: data_work/outputs/<output-name>/ (refuses to overwrite)
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
from pyproj import Transformer
from scipy.sparse import coo_matrix, csr_matrix
from scipy.sparse.csgraph import connected_components, dijkstra

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "outputs/osm_directed_speed_v1"
MANIFEST = ROOT.parent / "Platform/reference_inputs/manifest.json"
GATE5_V1 = ROOT / "outputs/model_input_edgeproj_v2_lcc_gate5_v1/data/qsm_input_edges_square.parquet"
GATE5_V3 = ROOT / "outputs/model_input_raw_connected_v3_edgeproj_v2_lcc_gate5_v1/data/qsm_input_edges_square.parquet"
WEEKDAYS = [20250519, 20250520, 20250521, 20250522, 20250523]
PERIODS = {"AM": [7, 8], "PM": [17, 18], "FF": [2, 3, 4]}
OFFSETS = [(0, 1), (0, -1), (1, 0), (-1, 0)]   # --neighbours 8 adds the four diagonals
AM_DAYS = WEEKDAYS      # --am-days: AM pace from a subset of weekdays (split-day check); PM and FF keep all
FAMILY = {"motorway": 1, "motorway_link": 1, "trunk": 1, "trunk_link": 1, "primary": 2, "primary_link": 2,
          "secondary": 2, "secondary_link": 2, "tertiary": 3, "tertiary_link": 3}
ZONES = [0, 5, 9, 15, 25, np.inf]
TAM = (116.3975, 39.9087)
L_GAP = 300.0
HOPS = 3
BORROW = "relative"     # 1A robustness (a): "absolute" borrows v_period itself
FALLBACK = "class_zone" # 1A robustness (c): "class" ignores zones in the fallback medians
T3857 = Transformer.from_crs(4326, 3857, always_xy=True)
TUTM = Transformer.from_crs(4326, 32650, always_xy=True)
T3857_UTM = Transformer.from_crs(3857, 32650, always_xy=True)


# -------------------------------------------------------------- arc speeds
HOUR_WEIGHTS: dict[str, dict[int, float]] = {}   # --hour-weights commute: period -> {hour: weight}; empty = equal


def observed_pace() -> pd.DataFrame:
    p = pd.read_parquet(SRC / "baidu_pace_hour.parquet")
    p = p[p.date.isin(WEEKDAYS)]
    out = {}
    for k, hrs in PERIODS.items():
        q = p[p.hour.isin(hrs)]
        if k == "AM":
            q = q[q.date.isin(AM_DAYS)]
        if k in HOUR_WEIGHTS:
            # weighted mean of hourly pace (min/km): average time per km of a trip whose
            # departure hour follows the weights; hours missing on a segment are renormalised
            h = q.groupby(["roadseg_id", "hour"]).pace_min_per_km.mean().reset_index()
            h["w"] = h.hour.map(HOUR_WEIGHTS[k])
            h["wp"] = h.w * h.pace_min_per_km
            g = h.groupby("roadseg_id")[["wp", "w"]].sum()
            out[k] = g.wp / g.w
        else:
            out[k] = q.groupby("roadseg_id").pace_min_per_km.mean()
    return pd.DataFrame(out)


def commute_hour_weights(hours: list[int], purpose: str) -> dict[int, float]:
    """Mode1 departures by hour for purpose HW/WH, R sample, 19 weekdays (platform 0112)."""
    d = pd.read_csv(ROOT.parent / "Platform/runs/202505_v01/query-results/TL25M05_V01_0112_purpose_share_r.csv")
    d = d[(d.in_r == 1) & (d.mode_group == "mode1") & (d.purpose == purpose)]
    w = d.groupby(d.slot10 // 6).n_trips.sum()
    w = w.reindex(hours).fillna(0.0)
    return (w / w.sum()).round(6).to_dict()


def arc_table() -> pd.DataFrame:
    a = pd.read_parquet(SRC / "arcs_major.parquet")
    m = pd.read_parquet(SRC / "arc_baidu_match.parquet")[["edge_idx", "roadseg_id"]]
    obs = observed_pace()
    m = m.merge(obs, left_on="roadseg_id", right_index=True)
    m = m.dropna(subset=list(PERIODS))
    a = a.merge(m, on="edge_idx", how="left")
    a["obs"] = a[list(PERIODS)].notna().all(axis=1)
    lon = a.from_xy.str.split(",").str[0].astype(float).to_numpy()
    lat = a.from_xy.str.split(",").str[1].astype(float).to_numpy()
    x, y = TUTM.transform(lon, lat)
    x0, y0 = TUTM.transform(*TAM)
    a["zone"] = pd.cut(np.hypot(x - x0, y - y0) / 1000, ZONES, labels=False, right=False)
    a["fam"] = a.road_class.map(FAMILY)
    a["lon"], a["lat"] = lon, lat
    return a.reset_index(drop=True)


def impute(a: pd.DataFrame) -> pd.DataFrame:
    a = a.copy()
    vff = 60.0 / a.FF
    # free-flow speed for unobserved arcs: class x zone median of observed, fallback class
    zkey = a.zone if FALLBACK == "class_zone" else pd.Series(0, index=a.index)
    cz = vff[a.obs].groupby([a.road_class[a.obs], zkey[a.obs]]).median()
    cc = vff[a.obs].groupby(a.road_class[a.obs]).median()
    ff_imp = pd.Series([cz.get((c, z), np.nan) for c, z in zip(a.road_class, zkey)], index=a.index).fillna(a.road_class.map(cc))
    a["vff"] = vff.where(a.obs, ff_imp)
    # arc adjacency within the same family, upstream and downstream, no U-turns
    n = len(a)
    nodes, codes = np.unique(np.r_[a.from_xy.to_numpy(), a.to_xy.to_numpy()], return_inverse=True)
    fu, tu = codes[:n], codes[n:]
    out_by_node = pd.Series(np.arange(n)).groupby(fu).apply(np.asarray)
    rows, cols = [], []
    for i in range(n):
        js = out_by_node.get(tu[i])
        if js is None:
            continue
        js = js[(tu[js] != fu[i]) & (a.fam.to_numpy()[js] == a.fam.to_numpy()[i])]
        rows.append(np.full(len(js), i)); cols.append(js)
    rows, cols = np.concatenate(rows), np.concatenate(cols)
    A = coo_matrix((np.ones(len(rows)), (rows, cols)), shape=(n, n)).tocsr()
    A = ((A + A.T) > 0).astype(float).tocsr()          # downstream and upstream
    obs = a.obs.to_numpy().astype(float)
    reached = csr_matrix((np.ones(n), (np.arange(n), np.arange(n))), shape=(n, n))
    frontier = reached.copy()
    num = {k: np.zeros(n) for k in ["AM", "PM"]}
    den = np.zeros(n)
    if BORROW == "relative":
        rel = {k: np.nan_to_num((a.FF / a[k]).to_numpy()) for k in ["AM", "PM"]}   # v_k / v_ff = pace_ff / pace_k
    else:
        rel = {k: np.nan_to_num((60.0 / a[k]).to_numpy()) for k in ["AM", "PM"]}  # v_k itself
    for h in range(1, HOPS + 1):
        frontier = ((frontier @ A) > 0).astype(float)
        frontier = (frontier - frontier.multiply(reached)).tocsr()
        frontier.eliminate_zeros()
        reached = ((reached + frontier) > 0).astype(float).tocsr()
        den += (frontier @ obs) / h
        for k in num:
            num[k] += (frontier @ (obs * rel[k])) / h
    zone_med = {}
    for k in ["AM", "PM"]:
        r_obs = pd.Series(rel[k], index=a.index)[a.obs]
        if BORROW == "relative":
            gk, gz = a.fam, zkey
        else:                                   # absolute speeds: class, not family, in the fallback
            gk, gz = a.road_class, zkey
        zone_med[k] = r_obs.groupby([gk[a.obs], gz[a.obs]]).median()
        fam_med = r_obs.groupby(gk[a.obs]).median()
        r_nb = np.where(den > 0, num[k] / np.where(den > 0, den, 1), np.nan)
        r_cz = pd.Series([zone_med[k].get((f, z), np.nan) for f, z in zip(gk, gz)], index=a.index).fillna(gk.map(fam_med))
        r_use = np.where(np.isfinite(r_nb), r_nb, r_cz)
        v_imp = a.vff * r_use if BORROW == "relative" else np.minimum(r_use, a.vff)   # absolute: never above own free flow
        a[f"v_{k}"] = np.where(a.obs, 60.0 / a[k].fillna(np.inf), v_imp)
    a["v_FF"] = a.vff
    a["speed_source"] = np.where(a.obs, "observed", np.where(den > 0, "neighbour", "class_zone"))
    return a


def gap_keep(a: pd.DataFrame) -> np.ndarray:
    codes, uniq = pd.factorize(pd.concat([a.from_xy, a.to_xy], ignore_index=True))
    n = len(a)
    iu, iv = codes[:n], codes[n:]
    w = a.length_m.to_numpy()
    g = coo_matrix((np.r_[w, w], (np.r_[iu, iv], np.r_[iv, iu])), shape=(len(uniq),) * 2).tocsr()
    src = np.unique(np.r_[iu[a.obs.to_numpy()], iv[a.obs.to_numpy()]])
    d = dijkstra(g, directed=False, indices=src, min_only=True, limit=L_GAP + 1)
    return a.obs.to_numpy() | ((d[iu] <= L_GAP) & (d[iv] <= L_GAP))


# ----------------------------------------------------------------- routing
class Graph:
    def __init__(self, a: pd.DataFrame):
        codes, uniq = pd.factorize(pd.concat([a.from_xy, a.to_xy], ignore_index=True))
        n = len(a)
        iu, iv = codes[:n], codes[n:]
        g = coo_matrix((np.ones(n), (iu, iv)), shape=(len(uniq),) * 2).tocsr()
        _, lab = connected_components(g, directed=True, connection="strong")
        big = lab == np.bincount(lab).argmax()
        k = big[iu] & big[iv]
        new = -np.ones(len(uniq), int); new[big] = np.arange(big.sum())
        self.a = a[k].reset_index(drop=True)
        self.iu, self.iv = new[iu[k]], new[iv[k]]
        self.N = int(big.sum())
        xy = pd.Series(uniq[big]).str.split(",", expand=True).astype(float)
        self.x, self.y = T3857.transform(xy[0].to_numpy(), xy[1].to_numpy())
        self.km_share = float(self.a.length_m.sum() / a.length_m.sum())
        # arc lookup for path tracing: (u, v) -> fastest arc index, per period
        self.arc_of = {}
        for p in PERIODS:
            t = self.a.length_m.to_numpy() / 1000 / self.a[f"v_{p}"].to_numpy() * 60
            df = pd.DataFrame({"u": self.iu, "v": self.iv, "t": t, "i": np.arange(len(t))}).sort_values("t").drop_duplicates(["u", "v"])
            self.arc_of[p] = (df, coo_matrix((df.t, (df.u, df.v)), shape=(self.N, self.N)).tocsr())


def grid_pairs(G: Graph, x0: float, y0: float, size: float) -> pd.DataFrame:
    r = np.floor((G.y - y0) / size).astype(int)
    c = np.floor((G.x - x0) / size).astype(int)
    cx, cy = x0 + (c + 0.5) * size, y0 + (r + 0.5) * size
    df = pd.DataFrame({"r": r, "c": c, "d2": (G.x - cx) ** 2 + (G.y - cy) ** 2, "node": np.arange(G.N)})
    acc = df.loc[df.groupby(["r", "c"]).d2.idxmin(), ["r", "c", "node"]].reset_index(drop=True)
    ux, uy = T3857_UTM.transform((x0 + (acc.c + 0.5) * size).to_numpy(), (y0 + (acc.r + 0.5) * size).to_numpy())
    pos = {k: i for i, k in enumerate(zip(acc.r, acc.c))}
    pr = [(i, pos[(rr + dr, cc + dc)]) for (rr, cc), i in pos.items() for dr, dc in OFFSETS if (rr + dr, cc + dc) in pos]
    P = pd.DataFrame(pr, columns=["a", "b"])
    P["grid_from"] = [f"sq_{acc.r[i]}_{acc.c[i]}" for i in P.a]
    P["grid_to"] = [f"sq_{acc.r[i]}_{acc.c[i]}" for i in P.b]
    P["dist_km"] = np.hypot(ux[P.a] - ux[P.b], uy[P.a] - uy[P.b]) / 1000
    nodes = acc.node.to_numpy()
    obs = G.a.obs.to_numpy()
    km = G.a.length_m.to_numpy() / 1000
    for p in PERIODS:
        arcs, W = G.arc_of[p]
        aidx = pd.Series(arcs.i.to_numpy(), index=pd.MultiIndex.from_arrays([arcs.u.to_numpy(), arcs.v.to_numpy()]))
        tvec = np.zeros(len(G.a)); tvec[arcs.i.to_numpy()] = arcs.t.to_numpy()
        t_out, km_out, obs_out = np.full(len(P), np.nan), np.full(len(P), np.nan), np.full(len(P), np.nan)
        src = np.sort(P.a.unique())
        for k in range(0, len(src), 64):
            s = src[k:k + 64]
            D, pred = dijkstra(W, directed=True, indices=nodes[s], return_predecessors=True, limit=120.0)
            row = {v: i for i, v in enumerate(s)}
            for j in np.where(P.a.isin(s))[0]:
                ri, tgt = row[P.a[j]], nodes[P.b[j]]
                if not np.isfinite(D[ri, tgt]):
                    continue
                us, vs = [], []
                v = tgt
                while v != nodes[P.a[j]]:
                    u = pred[ri, v]
                    us.append(u); vs.append(v); v = u
                ids = aidx.reindex(pd.MultiIndex.from_arrays([us, vs])).to_numpy()
                tt = tvec[ids]
                t_out[j], km_out[j] = tt.sum(), km[ids].sum()
                obs_out[j] = tt[obs[ids]].sum() / tt.sum()
        P[f"t_{p}"], P[f"km_{p}"], P[f"obs_share_{p}"] = t_out, km_out, obs_out
    return P.drop(columns=["a", "b"])


def multi_access(G: Graph, x0: float, y0: float, size: float, P: pd.DataFrame) -> pd.DataFrame:
    """Mean shortest time over all access-node pairs; access nodes are the road nodes
    nearest a 3 x 3 lattice inside each cell (kept only if they fall in the cell)."""
    from scipy.spatial import cKDTree
    tree = cKDTree(np.c_[G.x, G.y])
    r = np.floor((G.y - y0) / size).astype(int)
    c = np.floor((G.x - x0) / size).astype(int)
    cells = pd.Series(1, index=pd.MultiIndex.from_arrays([r, c])).groupby(level=[0, 1]).size().index
    acc = {}
    f = (np.arange(3) + 0.5) / 3
    for rr, cc in cells:
        pts = np.array([[x0 + (cc + fx) * size, y0 + (rr + fy) * size] for fx in f for fy in f])
        _, idx = tree.query(pts)
        idx = np.unique(idx[(r[idx] == rr) & (c[idx] == cc)])
        if len(idx):
            acc[f"sq_{rr}_{cc}"] = idx
    out = P[["grid_from", "grid_to"]].copy()
    src_cells = sorted(set(out.grid_from) & set(acc))
    for p in PERIODS:
        W = G.arc_of[p][1]
        vals = {}
        for k in range(0, len(src_cells), 24):
            batch = src_cells[k:k + 24]
            nodes = np.concatenate([acc[g] for g in batch])
            D = dijkstra(W, directed=True, indices=nodes, limit=150.0)
            off = np.cumsum([0] + [len(acc[g]) for g in batch])
            for bi, g in enumerate(batch):
                Dg = D[off[bi]:off[bi + 1]]
                for h in out.grid_to[out.grid_from == g]:
                    if h in acc:
                        m = Dg[:, acc[h]]
                        m = m[np.isfinite(m)]
                        vals[(g, h)] = (m.mean() if len(m) else np.nan, len(m))
        out[f"tm_{p}"] = [vals.get(k, (np.nan, 0))[0] for k in zip(out.grid_from, out.grid_to)]
    out["n_access_pairs"] = [vals.get(k, (np.nan, 0))[1] for k in zip(out.grid_from, out.grid_to)]
    return out


def multi_access_minor(G: Graph, x0: float, y0: float, size: float, P: pd.DataFrame, acc_obj) -> pd.DataFrame:
    """--access minor: mean time over all pairs of 3 x 3 lattice POINTS of the two cells;
    each point reaches the routing graph over OSM minor roads (src.network.minor_access).
    Cells are those with at least one routing node, as in multi_access."""
    from src.network import minor_access as ma
    r = np.floor((G.y - y0) / size).astype(int)
    c = np.floor((G.x - x0) / size).astype(int)
    cells = pd.Series(1, index=pd.MultiIndex.from_arrays([r, c])).groupby(level=[0, 1]).size().index
    f = (np.arange(3) + 0.5) / 3
    names, px, py = [], [], []
    for rr, cc in cells:
        for fx in f:
            for fy in f:
                names.append(f"sq_{rr}_{cc}"); px.append(x0 + (cc + fx) * size); py.append(y0 + (rr + fy) * size)
    lon, lat = Transformer.from_crs(3857, 4326, always_xy=True).transform(np.array(px), np.array(py))
    out_c, in_c = acc_obj.connectors(lon, lat)
    pts_of = pd.Series(np.arange(len(names))).groupby(np.array(names)).apply(np.asarray).to_dict()
    out = P[["grid_from", "grid_to"]].copy()
    src_cells = sorted(set(out.grid_from) & set(pts_of))
    o_pts = np.concatenate([pts_of[g] for g in src_cells])
    off = dict(zip(src_cells, np.cumsum([0] + [len(pts_of[g]) for g in src_cells])[:-1]))
    for p in PERIODS:
        T = ma.point_times(G.arc_of[p][1], G.N, out_c, in_c, len(names), o_pts, limit=150.0)
        vals = {}
        for g, h in zip(out.grid_from, out.grid_to):
            if g in off and h in pts_of:
                m = T[off[g]:off[g] + len(pts_of[g])][:, pts_of[h]]
                m = m[np.isfinite(m)]
                vals[(g, h)] = (m.mean() if len(m) else np.nan, len(m))
        out[f"tm_{p}"] = [vals.get(k, (np.nan, 0))[0] for k in zip(out.grid_from, out.grid_to)]
    out["n_access_pairs"] = [vals.get(k, (np.nan, 0))[1] for k in zip(out.grid_from, out.grid_to)]
    return out


def summarise(P: pd.DataFrame) -> dict:
    o = {"pairs": int(len(P)), "routable_AM": int(P.t_AM.notna().sum())}
    for p in PERIODS:
        o[f"t_{p}_median_min"] = round(float(P[f"t_{p}"].median()), 2)
        o[f"obs_share_{p}_p25_p50_p75"] = [round(float(v), 3) for v in P[f"obs_share_{p}"].quantile([.25, .5, .75])]
    for p in PERIODS:
        o[f"tm_{p}_median_min"] = round(float(P[f"tm_{p}"].median()), 2)
    o["tm_AM_over_FF_median"] = round(float((P.tm_AM / P.tm_FF).median()), 3)
    km = P.set_index(["grid_from", "grid_to"]).tm_AM
    rv = km.reindex(pd.MultiIndex.from_arrays([km.index.get_level_values(1), km.index.get_level_values(0)])).to_numpy()
    rr = km.to_numpy() / rv
    o["tm_AM_directional_ratio_median_min_over_max"] = round(float(np.nanmedian(np.minimum(rr, 1 / rr))), 3)
    o["detour_AM_km_median"] = round(float((P.km_AM / P.dist_km).median()), 3)
    o["AM_over_FF_median"] = round(float((P.t_AM / P.t_FF).median()), 3)
    k = P.set_index(["grid_from", "grid_to"]).t_AM
    rev = k.reindex(pd.MultiIndex.from_arrays([k.index.get_level_values(1), k.index.get_level_values(0)])).to_numpy()
    ratio = k.to_numpy() / rev
    o["AM_directional_ratio_median_min_over_max"] = round(float(np.nanmedian(np.minimum(ratio, 1 / ratio))), 3)
    return o


def compare(P: pd.DataFrame, bundle: Path) -> dict:
    # model times are tau_obs_min / tau_ff_min; t_min / t_ff_min in these bundles are the inherited
    # v11 centerline times (runs before 2026-11-06 compared with those by mistake)
    e = pd.read_parquet(bundle, columns=["grid_o", "grid_d", "tau_obs_min", "tau_ff_min"]).rename(
        columns={"grid_o": "grid_from", "grid_d": "grid_to", "tau_obs_min": "t_min", "tau_ff_min": "t_ff_min"})
    d = P.merge(e, on=["grid_from", "grid_to"])
    ok = d.t_AM.notna()
    return {"pairs_in_both": int(ok.sum()), "bundle_edges": int(len(e)),
            "bundle_edges_found_here": round(float(ok.sum() / len(e)), 4),
            "corr_log_t_AM": round(float(np.corrcoef(np.log(d.t_AM[ok]), np.log(d.t_min[ok]))[0, 1]), 3),
            "corr_log_t_FF": round(float(np.corrcoef(np.log(d.t_FF[ok]), np.log(d.t_ff_min[ok]))[0, 1]), 3),
            "median_ratio_here_over_bundle_AM": round(float((d.t_AM[ok] / d.t_min[ok]).median()), 3),
            "corr_log_tm_AM": round(float(np.corrcoef(np.log(d.tm_AM[d.tm_AM.notna()]), np.log(d.t_min[d.tm_AM.notna()]))[0, 1]), 3),
            "corr_log_tm_FF": round(float(np.corrcoef(np.log(d.tm_FF[d.tm_FF.notna()]), np.log(d.t_ff_min[d.tm_FF.notna()]))[0, 1]), 3),
            "corr_log_tm_AM_obs_share_ge_0.8": round(float(np.corrcoef(np.log(d.tm_AM[(d.obs_share_AM >= 0.8) & d.tm_AM.notna()]), np.log(d.t_min[(d.obs_share_AM >= 0.8) & d.tm_AM.notna()]))[0, 1]), 3),
            "median_ratio_tm_over_bundle_AM": round(float((d.tm_AM / d.t_min).median()), 3)}


def main(argv: list[str] | None = None) -> None:
    global HOPS, BORROW, FALLBACK, L_GAP, AM_DAYS, OFFSETS
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-name", default="osm_grid_travel_time_v2")
    ap.add_argument("--hops", type=int, default=HOPS, help="0 = no neighbour borrowing (class medians only)")
    ap.add_argument("--borrow", choices=["relative", "absolute"], default=BORROW)
    ap.add_argument("--fallback", choices=["class_zone", "class"], default=FALLBACK)
    ap.add_argument("--l-gap", type=float, default=L_GAP)
    ap.add_argument("--schemes", default="1b,1a", help="comma list of 1b, 1a")
    ap.add_argument("--grids", default="all", help="all, or sq3000_model_origin only")
    ap.add_argument("--am-days", default=None, help="comma list of yyyymmdd weekdays for the AM pace (default all five)")
    ap.add_argument("--neighbours", type=int, choices=[4, 8], default=4, help="grid adjacency: 4 (rook) or 8 (queen)")
    ap.add_argument("--am-hours", default=None, help="comma list of clock hours for AM (default 7,8)")
    ap.add_argument("--pm-hours", default=None, help="comma list of clock hours for PM (default 17,18)")
    ap.add_argument("--hour-weights", choices=["equal", "commute"], default="equal",
                    help="commute: weight hours by mode1 HW (AM) / WH (PM) departures (platform 0112)")
    ap.add_argument("--access", choices=["nearest", "minor"], default="nearest",
                    help="tm_*: nearest = road node nearest each 3x3 lattice point (default); "
                         "minor = lattice points reach the network over OSM minor roads (src.network.minor_access)")
    ap.add_argument("--access-speed", type=float, default=20.0, help="--access minor: km/h on minor roads and snap legs")
    ap.add_argument("--access-max-min", type=float, default=6.0, help="--access minor: max minutes on minor roads")
    args = ap.parse_args(argv)
    if args.am_hours:
        PERIODS["AM"] = [int(x) for x in args.am_hours.split(",")]
    if args.pm_hours:
        PERIODS["PM"] = [int(x) for x in args.pm_hours.split(",")]
    if args.hour_weights == "commute":
        HOUR_WEIGHTS["AM"] = commute_hour_weights(PERIODS["AM"], "HW")
        HOUR_WEIGHTS["PM"] = commute_hour_weights(PERIODS["PM"], "WH")
    if args.neighbours == 8:
        OFFSETS = OFFSETS + [(1, 1), (1, -1), (-1, 1), (-1, -1)]
    HOPS, BORROW, FALLBACK, L_GAP = args.hops, args.borrow, args.fallback, args.l_gap
    if args.am_days:
        AM_DAYS = [int(x) for x in args.am_days.split(",")]
        assert set(AM_DAYS) <= set(WEEKDAYS), AM_DAYS
    out = ROOT / "outputs" / args.output_name
    if out.exists():
        raise SystemExit(f"{out} exists; choose a new --output-name")
    out.mkdir(parents=True)
    meta = json.loads(MANIFEST.read_text())
    X0, Y0 = meta["x_origin_3857"], meta["y_origin_3857"]
    a = impute(arc_table())
    a.drop(columns=["lon", "lat"]).to_parquet(out / "arc_speeds_1A.parquet", index=False)
    km = a.length_m / 1000
    res = {"params": {"hops": HOPS, "borrow": BORROW, "fallback": FALLBACK, "l_gap_m": L_GAP, "am_days": AM_DAYS, "neighbours": args.neighbours,
                      "periods": PERIODS, "hour_weights": HOUR_WEIGHTS or "equal", "access": args.access}}
    if args.access == "minor":
        res["params"].update(access_speed_kmh=args.access_speed, access_max_min=args.access_max_min)
        from src.network import minor_access as ma
        minor = ma.load_minor_arcs()
    res["arc_speed_sources_km_share_1A"] = (km.groupby(a.speed_source).sum() / km.sum()).round(4).to_dict()
    keep = gap_keep(a)
    b = a[keep].copy()
    b["speed_source"] = np.where(b.obs, "observed", "short_gap")
    res["arc_speed_sources_km_share_1B"] = (b.length_m.groupby(b.speed_source).sum() / b.length_m.sum()).round(4).to_dict()
    print(json.dumps(res, ensure_ascii=False), flush=True)
    grids = [("sq3000_model_origin", X0, Y0, 3000.0), ("sq3000_half_shift", X0 + 1500, Y0 + 1500, 3000.0),
             ("sq6000_model_origin", X0, Y0, 6000.0)]
    if args.grids != "all":
        grids = [g for g in grids if g[0] == args.grids]
    todo = {"1b": (f"osm1b_L{int(L_GAP)}", b), "1a": ("osm1a", a)}
    for scheme, d in [todo[s] for s in args.schemes.split(",")]:
        G = Graph(d)
        res[scheme] = {"lcc_km_share": round(G.km_share, 4)}
        for gk, x0, y0, s in grids:
            P = grid_pairs(G, x0, y0, s)
            if args.access == "minor":
                A = ma.Access(G, minor, v_access=args.access_speed, t_max=args.access_max_min)
                P = P.merge(multi_access_minor(G, x0, y0, s, P, A), on=["grid_from", "grid_to"])
            else:
                P = P.merge(multi_access(G, x0, y0, s, P), on=["grid_from", "grid_to"])
            P.to_parquet(out / f"pairs_{scheme}_{gk}.parquet", index=False)
            r = summarise(P)
            if gk == "sq3000_model_origin":
                r["vs_gate5_v1"] = compare(P, GATE5_V1)
                r["vs_gate5_v3"] = compare(P, GATE5_V3)
            res[scheme][gk] = r
            print(scheme, gk, json.dumps(r, ensure_ascii=False), flush=True)
            (out / "summary.json").write_text(json.dumps(res, indent=2, ensure_ascii=False, default=float))
    print("done")


if __name__ == "__main__":
    main()
