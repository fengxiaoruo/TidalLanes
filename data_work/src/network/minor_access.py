"""
Access to the routing network over OSM minor roads (2026-11-11)
===============================================================
A query point (cell lattice point, or a Baidu query point) reaches the routing network
(1B / 1A major arcs) over OSM minor roads only: residential, unclassified, living_street.
Minor roads are used only for the two access legs, never for the middle of the trip, so
the trip between access nodes stays on arcs with measured (or 1B-imputed) speeds.

Access leg cost (minutes, the same in every period, minor roads have no speed data):
    snap  : straight line from the point to its nearest node of the minor+major node set,
            at v_access
    minor : shortest path on minor arcs from that node to any major node within
            t_max minutes, at v_access
    fallback: a point that reaches no major node gets a straight-line connector to its
            nearest major node, length * detour, at v_access
The out-leg uses minor arcs forwards, the in-leg backwards (one-way minor arcs respected).

Inputs read only: Speed V1-OSM route_v3 arcs (read-only project).
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
from pyproj import Transformer
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import dijkstra
from scipy.spatial import cKDTree

ARCS = Path("/Users/fxr/Library/CloudStorage/Dropbox/Projects/Projects_FengGu/Speed/Road_processing/V1-OSM/data/intermediate/route_v3/beijing_arcs.parquet")
MINOR = ("residential", "unclassified", "living_street")
TUTM = Transformer.from_crs(4326, 32650, always_xy=True)


def load_minor_arcs(classes=MINOR) -> pd.DataFrame:
    a = pd.read_parquet(ARCS, columns=["road_class", "from_node_id", "to_node_id", "length_m"])
    a = a[a.road_class.isin(classes)].reset_index(drop=True)
    for s in ["from", "to"]:
        p = a[f"{s}_node_id"].str.extract(r"^([0-9.]+),([0-9.]+)\|")
        a[f"{s}_xy"] = p[0] + "," + p[1]
    return a[["from_xy", "to_xy", "length_m"]]


def _xy(keys: pd.Index) -> tuple[np.ndarray, np.ndarray]:
    ll = keys.str.split(",", expand=True)
    lon = np.asarray(ll.get_level_values(0), float)
    lat = np.asarray(ll.get_level_values(1), float)
    return TUTM.transform(lon, lat)


class Access:
    def __init__(self, G, minor: pd.DataFrame, v_access: float = 15.0, t_max: float = 6.0, detour: float = 1.3):
        """G: build_osm_grid_travel_time.Graph (major arcs, largest SCC)."""
        self.v, self.t_max, self.detour = v_access, t_max, detour
        gkey = pd.concat([pd.Series(G.iu, index=G.a.from_xy.to_numpy()), pd.Series(G.iv, index=G.a.to_xy.to_numpy())])
        gkey = gkey[~gkey.index.duplicated()]
        keys = pd.Index(pd.unique(np.r_[gkey.index.to_numpy(), minor.from_xy.to_numpy(), minor.to_xy.to_numpy()]))
        self.n = len(keys)
        self.major_of = -np.ones(self.n, int)                      # global node -> G node (or -1)
        self.major_of[keys.get_indexer(gkey.index)] = gkey.to_numpy()
        x, y = _xy(keys)
        self.tree_all = cKDTree(np.c_[x, y])
        mj = np.where(self.major_of >= 0)[0]
        self.major_nodes = mj
        self.tree_major = cKDTree(np.c_[x[mj], y[mj]])
        fu, tu = keys.get_indexer(minor.from_xy), keys.get_indexer(minor.to_xy)
        t = minor.length_m.to_numpy() / 1000 / v_access * 60
        self.Wf = coo_matrix((t, (fu, tu)), shape=(self.n, self.n)).tocsr()
        self.Wb = self.Wf.T.tocsr()

    def connectors(self, lon: np.ndarray, lat: np.ndarray, batch: int = 64):
        """Returns (out, in_): DataFrames with columns pt, node (G index), cost (min)."""
        px, py = TUTM.transform(np.asarray(lon, float), np.asarray(lat, float))
        sd, si = self.tree_all.query(np.c_[px, py])
        s0 = sd / 1000 / self.v * 60
        fd, fi = self.tree_major.query(np.c_[px, py])
        fb_node = self.major_of[self.major_nodes[fi]]
        fb_cost = self.detour * fd / 1000 / self.v * 60
        res = []
        for W in (self.Wf, self.Wb):
            rows = []
            for k in range(0, len(si), batch):
                src = si[k:k + batch]
                D = dijkstra(W, directed=True, indices=src, limit=self.t_max)
                D = D[:, self.major_nodes]
                r, c = np.nonzero(np.isfinite(D))
                rows.append(pd.DataFrame({"pt": k + r, "node": self.major_of[self.major_nodes[c]], "cost": s0[k + r] + D[r, c]}))
            df = pd.concat(rows)
            miss = np.setdiff1d(np.arange(len(si)), df.pt.unique())
            df = pd.concat([df, pd.DataFrame({"pt": miss, "node": fb_node[miss], "cost": fb_cost[miss]})])
            res.append(df.sort_values("cost").drop_duplicates(["pt", "node"]).reset_index(drop=True))
        self.last_snap_m, self.last_fallback = sd, None
        return res[0], res[1]


def point_times(W, N: int, out: pd.DataFrame, in_: pd.DataFrame, P: int, o_pts: np.ndarray,
                limit: float = 240.0, batch: int = 64) -> np.ndarray:
    """Shortest times from points o_pts to every point (as destination) through the
    major graph W (N nodes) with access connectors. Returns array len(o_pts) x P."""
    Wc = W.tocoo()
    r = np.r_[Wc.row, N + out.pt.to_numpy(), in_.node.to_numpy()]
    c = np.r_[Wc.col, out.node.to_numpy(), N + P + in_.pt.to_numpy()]
    w = np.r_[Wc.data, out.cost.to_numpy() + 1e-9, in_.cost.to_numpy() + 1e-9]
    M = coo_matrix((w, (r, c)), shape=(N + 2 * P,) * 2).tocsr()
    res = np.full((len(o_pts), P), np.inf)
    for k in range(0, len(o_pts), batch):
        D = dijkstra(M, directed=True, indices=N + o_pts[k:k + batch], limit=limit)
        res[k:k + batch] = D[:, N + P:]
    return res
