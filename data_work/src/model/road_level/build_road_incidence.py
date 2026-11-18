"""
Road layer for a grid bundle: which road arcs each grid edge uses (OSM 1B)
==========================================================================
Purpose: let policy (lanes) and congestion act on physical roads instead of grid edges.
A grid edge's road content is its centre-node AM shortest path, the same path the bundle
builder uses for lanes (build_osm_model_bundle.path_lanes).

Outputs, in data_work/outputs/<output-name>/ (refuses to overwrite):
  units.parquet      one row per road unit (an OSM arc of scheme 1B used by at least one
                     edge path, plus the opposite-carriageway arcs of those arcs):
                     unit, arc_row, length_m, lanes, t_AM_min, t_FF_min, road_class,
                     opp_unit (-1 if none), opp_kind (reverse | parallel | none), on_path
  incidence.parquet  grid_o, grid_d, unit, share (unit's share of the edge path AM time)
  meta.json
Opposite carriageway: the exact reverse arc (same two nodes) if it exists; otherwise the
nearest arc whose bearing differs from the opposite bearing by <= 30 degrees, whose
midpoint is within 60 m, and whose class family is the same (OSM maps most Beijing
arterials as two one-way ways).
Inputs read only.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.sparse.csgraph import dijkstra
from scipy.spatial import cKDTree

DATA_WORK = Path(__file__).resolve().parents[3]
if str(DATA_WORK) not in sys.path:
    sys.path.insert(0, str(DATA_WORK))

from src.network import build_osm_grid_travel_time as og  # noqa: E402
from src.model.travel_cost_bundle import build_osm_model_bundle as ob  # noqa: E402

O = DATA_WORK / "outputs"
ARCS = O / "osm_grid_travel_time_v2/arc_speeds_1A.parquet"
OPP_MAX_M = 60.0
OPP_MAX_DEG = 30.0


def centre_nodes(G, x0, y0, size=3000.0):
    r = np.floor((G.y - y0) / size).astype(int)
    c = np.floor((G.x - x0) / size).astype(int)
    cx, cy = x0 + (c + 0.5) * size, y0 + (r + 0.5) * size
    df = pd.DataFrame({"r": r, "c": c, "d2": (G.x - cx) ** 2 + (G.y - cy) ** 2, "node": np.arange(G.N)})
    acc = df.loc[df.groupby(["r", "c"]).d2.idxmin()]
    return {f"sq_{a}_{b}": int(n) for a, b, n in zip(acc.r, acc.c, acc.node)}


def trace_paths(G, E, node_of):
    arcs, W = G.arc_of["AM"]
    aidx = pd.Series(arcs.i.to_numpy(), index=pd.MultiIndex.from_arrays([arcs.u.to_numpy(), arcs.v.to_numpy()]))
    paths = {}
    src = sorted(set(E.grid_o))
    for k in range(0, len(src), 64):
        batch = src[k:k + 64]
        D, pred = dijkstra(W, directed=True, indices=[node_of[g] for g in batch], return_predecessors=True, limit=120.0)
        for bi, g in enumerate(batch):
            s = node_of[g]
            for h in E.grid_d[E.grid_o == g]:
                v = node_of[h]
                if not np.isfinite(D[bi, v]):
                    continue
                us, vs = [], []
                while v != s:
                    u = pred[bi, v]; us.append(u); vs.append(v); v = u
                paths[(g, h)] = aidx.reindex(pd.MultiIndex.from_arrays([us, vs])).to_numpy()
    return paths


def opposite_arcs(a: pd.DataFrame, rows: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """For arc rows `rows`, the row of the opposite-carriageway arc (or -1) and its kind."""
    key = pd.Series(np.arange(len(a)), index=pd.MultiIndex.from_arrays([a.from_xy.to_numpy(), a.to_xy.to_numpy()]))
    key = key[~key.index.duplicated(keep="first")]   # parallel duplicate arcs: keep one
    rev = key.reindex(pd.MultiIndex.from_arrays([a.to_xy.to_numpy()[rows], a.from_xy.to_numpy()[rows]])).to_numpy()
    out = np.where(np.isfinite(rev), rev, -1).astype(int)
    kind = np.where(out >= 0, "reverse", "none").astype(object)
    fx = a.from_xy.str.split(",", expand=True).astype(float).to_numpy()
    tx = a.to_xy.str.split(",", expand=True).astype(float).to_numpy()
    mx, my = og.TUTM.transform((fx[:, 0] + tx[:, 0]) / 2, (fx[:, 1] + tx[:, 1]) / 2)
    tree = cKDTree(np.c_[mx, my])
    head = a["head"].to_numpy(dtype=float)
    fam = a.fam.to_numpy()
    for k in np.where(out < 0)[0]:
        r = rows[k]
        cand = tree.query_ball_point([mx[r], my[r]], OPP_MAX_M)
        best, bd = -1, np.inf
        for c in cand:
            if c == r or fam[c] != fam[r]:
                continue
            dh = abs(((head[c] - head[r]) % 360) - 180)
            if dh > OPP_MAX_DEG:
                continue
            d = np.hypot(mx[c] - mx[r], my[c] - my[r])
            if d < bd:
                best, bd = c, d
        if best >= 0:
            out[k], kind[k] = best, "parallel"
    return out, kind


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--bundle", required=True, help="model_input_* bundle (OSM 1B)")
    ap.add_argument("--output-name", default=None)
    args = ap.parse_args(argv)
    name = args.output_name or f"road_layer_{args.bundle}_v1"
    out = O / name
    if out.exists():
        raise SystemExit(f"{out} exists; choose a new --output-name")

    meta = json.loads(og.MANIFEST.read_text())
    x0, y0 = meta["x_origin_3857"], meta["y_origin_3857"]
    a = pd.read_parquet(ARCS)
    a["lanes"], _ = ob.arc_lanes(a)
    a = a[og.gap_keep(a)].copy().reset_index(drop=True)
    G = og.Graph(a)
    g = G.a  # largest strongly connected component, row = arc index used in paths
    E = pd.read_parquet(O / args.bundle / "data/qsm_input_edges_square.parquet")
    node_of = centre_nodes(G, x0, y0)
    paths = trace_paths(G, E, node_of)
    missing = [k for k in zip(E.grid_o, E.grid_d) if k not in paths]

    t_am = g.length_m.to_numpy() / 1000 / g.v_AM.to_numpy() * 60
    t_ff = g.length_m.to_numpy() / 1000 / g.v_FF.to_numpy() * 60
    used = np.unique(np.concatenate(list(paths.values())))
    opp, kind = opposite_arcs(g, used)
    rows = np.unique(np.r_[used, opp[opp >= 0]])
    unit_of = -np.ones(len(g), int); unit_of[rows] = np.arange(len(rows))
    opp_full = -np.ones(len(g), int); kind_full = np.full(len(g), "none", dtype=object)
    opp_full[used], kind_full[used] = opp, kind
    U = pd.DataFrame({
        "unit": np.arange(len(rows)), "arc_row": rows,
        "edge_idx": g.edge_idx.to_numpy()[rows], "road_class": g.road_class.to_numpy()[rows],
        "length_m": g.length_m.to_numpy()[rows], "lanes": g.lanes.to_numpy()[rows],
        "t_AM_min": t_am[rows], "t_FF_min": t_ff[rows], "obs": g.obs.to_numpy()[rows],
        "opp_unit": np.where(opp_full[rows] >= 0, unit_of[np.maximum(opp_full[rows], 0)], -1),
        "opp_kind": kind_full[rows], "on_path": np.isin(rows, used),
    })
    inc = []
    for (go, gd), ids in paths.items():
        tt = t_am[ids]
        s = pd.Series(tt / tt.sum()).groupby(unit_of[ids]).sum()
        inc.append(pd.DataFrame({"grid_o": go, "grid_d": gd, "unit": s.index.to_numpy(), "share": s.to_numpy()}))
    inc = pd.concat(inc, ignore_index=True)

    out.mkdir(parents=True)
    U.to_parquet(out / "units.parquet", index=False)
    inc.to_parquet(out / "incidence.parquet", index=False)
    km = U.length_m / 1000
    onp = U.on_path
    info = {
        "bundle": args.bundle, "edges": int(len(E)), "edges_without_path": len(missing),
        "units": int(len(U)), "units_on_path": int(onp.sum()), "unit_km_on_path": round(float(km[onp].sum()), 1),
        "opp_share_km_on_path": {k: round(float(km[onp & (U.opp_kind == k)].sum() / km[onp].sum()), 4) for k in ["reverse", "parallel", "none"]},
        "edges_per_unit_km_weighted": round(float((km[onp] * inc.groupby("unit").size().reindex(U.unit[onp]).to_numpy()).sum() / km[onp].sum()), 3),
        "opp_rule": {"max_midpoint_m": OPP_MAX_M, "max_bearing_dev_deg": OPP_MAX_DEG, "same_family": True},
    }
    (out / "meta.json").write_text(json.dumps(info, indent=2, ensure_ascii=False))
    print(json.dumps(info, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
