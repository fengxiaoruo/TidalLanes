"""Model network times from Baidu's exact query points (nearest node), to compare under one OD rule."""
import sys, glob, numpy as np, pandas as pd
sys.path.insert(0, "/Users/fxr/Library/CloudStorage/Dropbox/Projects/TidalLanes/data_work")
from src.network import build_osm_grid_travel_time as gt
from scipy.spatial import cKDTree
from scipy.sparse.csgraph import dijkstra
O = "/Users/fxr/Library/CloudStorage/Dropbox/Projects/TidalLanes/data_work/outputs"
V = f"{O}/grid_pair_baidu_validation_v3"
m = pd.read_csv(f"{V}/data/sample_manifest.csv")
def ll(s):
    lat, lon = s.str.split(",", expand=True).astype(float).T.values
    return lon, lat
olon, olat = ll(m.origin_text); dlon, dlat = ll(m.destination_text)
a = gt.impute(gt.arc_table())
b = a[gt.gap_keep(a)].copy()
rows = []
for scheme, df in [("1b", b), ("1a", a)]:
    G = gt.Graph(df)
    tree = cKDTree(np.c_[G.x, G.y])
    ox, oy = gt.T3857.transform(olon, olat); dx, dy = gt.T3857.transform(dlon, dlat)
    so, io = tree.query(np.c_[ox, oy]); sd, id_ = tree.query(np.c_[dx, dy])
    coslat = np.cos(np.radians(olat))
    res = pd.DataFrame({"request_id": m.request_id, "scheme": scheme, "snap_o_m": so * coslat, "snap_d_m": sd * coslat})
    km_arc = G.a.length_m.to_numpy() / 1000
    for p in ["AM", "PM", "FF"]:
        arcs, W = G.arc_of[p]
        aidx = pd.Series(arcs.i.to_numpy(), index=pd.MultiIndex.from_arrays([arcs.u.to_numpy(), arcs.v.to_numpy()]))
        tvec = np.zeros(len(G.a)); tvec[arcs.i.to_numpy()] = arcs.t.to_numpy()
        src = np.unique(io)
        D, pred = dijkstra(W, directed=True, indices=src, return_predecessors=True, limit=180.0)
        row = {v: i for i, v in enumerate(src)}
        t_out = np.full(len(m), np.nan); k_out = np.full(len(m), np.nan)
        for j in range(len(m)):
            ri, s, tgt = row[io[j]], io[j], id_[j]
            if s == tgt: t_out[j] = 0; k_out[j] = 0; continue
            if not np.isfinite(D[ri, tgt]): continue
            us, vs, v = [], [], tgt
            while v != s:
                u = pred[ri, v]; us.append(u); vs.append(v); v = u
            ids = aidx.reindex(pd.MultiIndex.from_arrays([us, vs])).to_numpy()
            t_out[j] = tvec[ids].sum(); k_out[j] = km_arc[ids].sum()
        res[f"t_{p}"] = t_out; res[f"km_{p}"] = k_out
    rows.append(res)
    print(scheme, "done", flush=True)
out = pd.concat(rows)
out.to_parquet("/private/tmp/claude-502/-Users-fxr-Library-CloudStorage-Dropbox-Projects-TidalLanes/f939346f-8863-48ed-9518-e8ef6245b92a/scratchpad/gp_same_od.parquet", index=False)
