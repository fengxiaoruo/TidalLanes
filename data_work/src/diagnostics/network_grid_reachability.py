"""
Road-network connectivity seen from a grid system, for several networks and grids
=================================================================================

Step 1  OSM layer check. Speed's V1-OSM route graph keys nodes by "lon,lat|L<layer>",
        so a bridge/tunnel arc never meets a ground arc. Dropping the layer suffix merges
        nodes that share a coordinate. For every merged coordinate we ask whether each
        layer group touches it at an endpoint of its parent OSM edge (bridge end meeting
        a road: a real junction) or only at an interior vertex (suspicious: possible
        false merge of crossing roads). Counts plus a map sheet of sampled cases.

Step 2  For each network (largest strongly connected component only) and each grid
        system (square, EPSG:3857, cell size and offset as parameters; not tied to gate5):
          - cells with at least one road node
          - directed 4-neighbour cell pairs (both cells with road): share with a direct
            road link (an edge from a node in cell a to a node in cell b), and share whose
            shortest road path between the cells' access nodes (node nearest the cell
            centre) is <= 2x and <= 5x the real centre-to-centre distance
          - on the model-aligned 3 km grid: the same measures weighted by Unicom RN
            weekday flow between the two cells, overall and for the 717 "no edge between
            gate5 nodes" and 247 "reverse only" candidate pairs (rn_baidu_match_network_v1)

Networks: v1 (= Baidu raw), v3 (manual connections), osm_all (motor classes, layer
dropped), osm_major (tertiary and above, layer dropped). Inputs read only.
Output: data_work/outputs/<output-name>/ (refuses to overwrite)
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pyproj import Transformer
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import connected_components, dijkstra
from scipy.spatial import cKDTree

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
SPEED = Path("/Users/fxr/Library/CloudStorage/Dropbox/Projects/Projects_FengGu/Speed/Road_processing")
ARCS = SPEED / "V1-OSM/data/intermediate/route_v3/beijing_arcs.parquet"
MANIFEST = ROOT.parent / "Platform/reference_inputs/manifest.json"
CELLPAIR = ROOT / "outputs/rn_baidu_match_network_v1/cell_pair_flow.parquet"
MAJOR = {"motorway", "motorway_link", "trunk", "trunk_link", "primary", "primary_link",
         "secondary", "secondary_link", "tertiary", "tertiary_link"}
T3857 = Transformer.from_crs(4326, 3857, always_xy=True)
TUTM = Transformer.from_crs(4326, 32650, always_xy=True)
T3857_UTM = Transformer.from_crs(3857, 32650, always_xy=True)
LAYER_COL = {"L0": "#b9b8b3", "L1": "#2a78d6", "L2": "#1baf7a", "L3": "#7a4fd6", "L-1": "#eb6834"}
INK, INK2 = "#0b0b0b", "#52514e"


# ------------------------------------------------------------------ networks
def osm_arcs() -> pd.DataFrame:
    a = pd.read_parquet(ARCS, columns=["edge_id", "source_edge_id", "route_segment_id", "road_class", "roadname",
                                       "from_node_id", "to_node_id", "length_m"])
    for s in ["from", "to"]:
        p = a[f"{s}_node_id"].str.extract(r"^([0-9.]+),([0-9.]+)\|(L-?\d+)$")
        a[f"{s}_lon"], a[f"{s}_lat"], a[f"{s}_layer"] = p[0].astype(float), p[1].astype(float), p[2]
        a[f"{s}_xy"] = p[0] + "," + p[1]
    return a


def osm_net(a: pd.DataFrame, major: bool) -> pd.DataFrame:
    d = a[a.road_class.isin(MAJOR)] if major else a
    return pd.DataFrame({"u": d.from_xy, "v": d.to_xy, "km": d.length_m / 1000,
                         "ulon": d.from_lon, "ulat": d.from_lat, "vlon": d.to_lon, "vlat": d.to_lat})


def baidu_net(p: Path) -> pd.DataFrame:
    e = pd.read_excel(p)
    km = e.length_m / 1000 if "length_m" in e else np.hypot(e.end_x_m - e.start_x_m, e.end_y_m - e.start_y_m) / 1000
    return pd.DataFrame({"u": e.from_node_id.astype(str), "v": e.to_node_id.astype(str), "km": km,
                         "ulon": e.start_lon, "ulat": e.start_lat, "vlon": e.end_lon, "vlat": e.end_lat})


class Net:
    """Largest strongly connected component of a directed road network."""

    def __init__(self, e: pd.DataFrame):
        e = e.dropna(subset=["ulon", "ulat", "vlon", "vlat"])
        e = e[e.u != e.v]
        codes, uniq = pd.factorize(pd.concat([e.u, e.v], ignore_index=True))
        m, n = len(e), len(uniq)
        iu, iv = codes[:m], codes[m:]
        lon = np.zeros(n); lat = np.zeros(n)
        lon[iu], lat[iu] = e.ulon.to_numpy(), e.ulat.to_numpy()
        lon[iv], lat[iv] = e.vlon.to_numpy(), e.vlat.to_numpy()
        g = coo_matrix((np.ones(m), (iu, iv)), shape=(n, n)).tocsr()
        _, lab = connected_components(g, directed=True, connection="strong")
        big = np.bincount(lab).argmax()
        keep = lab == big
        new = -np.ones(n, int); new[keep] = np.arange(keep.sum())
        k = keep[iu] & keep[iv]
        self.iu, self.iv, self.km = new[iu[k]], new[iv[k]], e.km.to_numpy()[k]
        self.n = int(keep.sum())
        self.lon, self.lat = lon[keep], lat[keep]
        self.x, self.y = T3857.transform(self.lon, self.lat)
        self.g = coo_matrix((self.km, (self.iu, self.iv)), shape=(self.n, self.n)).tocsr()
        self.info = {"lcc_nodes": self.n, "lcc_edges": int(k.sum()), "lcc_km": round(float(self.km.sum()), 1),
                     "lcc_km_share": round(float(self.km.sum() / e.km.sum()), 4)}


# ---------------------------------------------------------------------- grids
def cells(net: Net, x0: float, y0: float, size: float) -> tuple[np.ndarray, np.ndarray]:
    return np.floor((net.y - y0) / size).astype(int), np.floor((net.x - x0) / size).astype(int)


def grid_eval(net: Net, x0: float, y0: float, size: float, flow: pd.DataFrame | None, limit_mult: float = 6.0) -> tuple[dict, pd.DataFrame]:
    r, c = cells(net, x0, y0, size)
    cid = pd.Series(list(zip(r, c)))
    # access node: road node nearest the cell centre
    cx, cy = x0 + (c + 0.5) * size, y0 + (r + 0.5) * size
    d2 = (net.x - cx) ** 2 + (net.y - cy) ** 2
    df = pd.DataFrame({"r": r, "c": c, "d2": d2, "node": np.arange(net.n)})
    acc = df.loc[df.groupby(["r", "c"]).d2.idxmin(), ["r", "c", "node"]].reset_index(drop=True)
    cellset = set(zip(acc.r, acc.c))
    # real distance between neighbouring centres (3857 is stretched by 1/cos(lat))
    ccx, ccy = x0 + (acc.c + 0.5) * size, y0 + (acc.r + 0.5) * size
    ux, uy = T3857_UTM.transform(ccx.to_numpy(), ccy.to_numpy())
    acc["ux"], acc["uy"] = ux, uy
    pos = {k: i for i, k in enumerate(zip(acc.r, acc.c))}
    # directed 4-neighbour pairs among road cells
    pr = []
    for (rr, cc), i in pos.items():
        for dr, dc in [(0, 1), (0, -1), (1, 0), (-1, 0)]:
            j = pos.get((rr + dr, cc + dc))
            if j is not None:
                pr.append((i, j))
    P = pd.DataFrame(pr, columns=["a", "b"])
    P["dist_km"] = np.hypot(acc.ux.to_numpy()[P.a] - acc.ux.to_numpy()[P.b], acc.uy.to_numpy()[P.a] - acc.uy.to_numpy()[P.b]) / 1000
    # direct links
    ce = pd.Series(list(zip(r[net.iu], c[net.iu]))), pd.Series(list(zip(r[net.iv], c[net.iv])))
    ia = ce[0].map(pos).to_numpy(); ib = ce[1].map(pos).to_numpy()
    direct = set(zip(ia[ia != ib], ib[ia != ib]))
    P["direct"] = [(a, b) in direct for a, b in zip(P.a, P.b)]
    # shortest paths, batched over source cells
    lim = limit_mult * float(P.dist_km.max()) if len(P) else 0
    P["path_km"] = np.inf
    src = np.sort(P.a.unique())
    nodes = acc.node.to_numpy()
    for k in range(0, len(src), 64):
        s = src[k:k + 64]
        D = dijkstra(net.g, directed=True, indices=nodes[s], limit=lim)
        row = {a: i for i, a in enumerate(s)}
        m = P.a.isin(s)
        P.loc[m, "path_km"] = D[P.a[m].map(row).to_numpy(), nodes[P.b[m].to_numpy()]]
    P["detour"] = P.path_km / P.dist_km
    P["grid_from"] = [f"sq_{acc.r[i]}_{acc.c[i]}" for i in P.a]
    P["grid_to"] = [f"sq_{acc.r[i]}_{acc.c[i]}" for i in P.b]
    res = {"cells_with_road": int(len(acc)), "directed_adjacent_pairs": int(len(P)),
           "share_direct_link": round(float(P.direct.mean()), 4),
           "share_detour_le2": round(float((P.detour <= 2).mean()), 4),
           "share_detour_le5": round(float((P.detour <= 5).mean()), 4),
           "detour_p50": round(float(P.detour.replace(np.inf, np.nan).median()), 3)}
    if flow is not None:
        f = flow.merge(P[["grid_from", "grid_to", "direct", "detour"]], on=["grid_from", "grid_to"], how="left")
        f["direct"] = f.direct.fillna(False).astype(bool)
        f["detour"] = f.detour.fillna(np.inf)
        w = {}
        for lab, s in [("all_adjacent_rn_flow", f), ("cand_no_edge_717", f[f.cand == "no_edge_between_gate5_nodes"]),
                       ("cand_reverse_only_247", f[f.cand == "reverse_only"]), ("gate5_edges", f[f.cls == "gate5_edge"])]:
            tw = s.f_wd.sum()
            w[lab] = {"pairs": int(len(s)), "flow_share_direct": round(float(s.f_wd[s.direct].sum() / tw), 4),
                      "flow_share_detour_le2": round(float(s.f_wd[s.detour <= 2].sum() / tw), 4),
                      "flow_share_detour_le5": round(float(s.f_wd[s.detour <= 5].sum() / tw), 4),
                      "pairs_detour_le5": int((s.detour <= 5).sum())}
        res["rn_flow_weighted"] = w
    return res, P


# -------------------------------------------------------------------- step 1
def layer_check(a: pd.DataFrame, out: Path) -> dict:
    a = a.copy()
    a["seg"] = a.route_segment_id.str.extract(r"::S(\d+)$")[0].astype(int)
    first = a.sort_values("seg").groupby("source_edge_id").from_xy.first()
    last = a.sort_values("seg").groupby("source_edge_id").to_xy.last()
    ends = set(zip(first.index, first)) | set(zip(last.index, last))
    inc = pd.concat([a[["source_edge_id", "from_xy", "from_layer", "road_class", "roadname"]].set_axis(["src", "xy", "layer", "cls", "name"], axis=1),
                     a[["source_edge_id", "to_xy", "to_layer", "road_class", "roadname"]].set_axis(["src", "xy", "layer", "cls", "name"], axis=1)]).drop_duplicates(["src", "xy", "layer"])
    nl = inc.groupby("xy").layer.nunique()
    multi = inc[inc.xy.isin(nl[nl > 1].index)].copy()
    multi["is_end"] = [(s, x) in ends for s, x in zip(multi.src, multi.xy)]
    # a layer group "touches at an end" if any of its parent edges ends there
    lg = multi.groupby(["xy", "layer"]).is_end.any().reset_index()
    pt = lg.groupby("xy").is_end.agg(["all", "any"])
    kind = np.where(pt["all"], "all_layers_at_parent_end", np.where(pt["any"], "mixed_end_and_interior", "all_interior"))
    pt["kind"] = kind
    res = {"coordinates_with_multiple_layers": int(len(pt)), "by_kind": pt.kind.value_counts().to_dict()}
    # sample and draw
    rng = np.random.default_rng(1106)
    samp = []
    for k, n in [("all_interior", 4), ("mixed_end_and_interior", 4), ("all_layers_at_parent_end", 4)]:
        ids = pt.index[pt.kind == k].to_numpy()
        samp += [(k, x) for x in rng.choice(ids, min(n, len(ids)), replace=False)]
    rows = []
    fig, ax = plt.subplots(3, 4, figsize=(16, 12.5))
    for axx, (k, xy) in zip(ax.ravel(), samp):
        lon, lat = map(float, xy.split(","))
        x0, y0 = TUTM.transform(lon, lat)
        box = 250
        near = a[(abs(a.from_lon - lon) < 0.004) & (abs(a.from_lat - lat) < 0.004)]
        fx, fy = TUTM.transform(near.from_lon.to_numpy(), near.from_lat.to_numpy())
        tx, ty = TUTM.transform(near.to_lon.to_numpy(), near.to_lat.to_numpy())
        for L in ["L0", "L-1", "L1", "L2", "L3"]:
            m = (near.from_layer == L).to_numpy()
            for i in np.where(m)[0]:
                axx.plot([fx[i] - x0, tx[i] - x0], [fy[i] - y0, ty[i] - y0], color=LAYER_COL[L], lw=2.4 if L != "L0" else 1.4, solid_capstyle="round")
        axx.scatter([0], [0], s=90, facecolor="none", edgecolor=INK, lw=1.6, zorder=5)
        here = multi[multi.xy == xy]
        lab = "; ".join(f"{r.layer} {r.cls}{' ' + r.name if isinstance(r.name, str) and r.name else ''}{' (end)' if r.is_end else ''}" for r in here.itertuples())
        rows.append({"kind": k, "lon": lon, "lat": lat, "incident": lab})
        axx.set(xlim=(-box, box), ylim=(-box, box), aspect="equal", xticks=[], yticks=[])
        axx.set_title(f"{k.replace('_', ' ')}\n{lon:.5f}, {lat:.5f}", fontsize=10, color=INK, loc="left")
        for s in axx.spines.values():
            s.set_color("#e4e3df")
    for L, col in LAYER_COL.items():
        ax[0, 0].plot([], [], color=col, lw=2.4, label=f"layer {L}")
    fig.legend(*ax[0, 0].get_legend_handles_labels(), loc="lower center", ncol=5, frameon=False, fontsize=11)
    fig.text(0.01, 0.005, "OSM route arcs within about 300 m; circle = coordinate shared by arcs on different layers. Each panel 500 m across.", fontsize=9, color=INK2)
    fig.tight_layout(rect=(0, 0.04, 1, 1))
    fig.savefig(out / "fig_osm_layer_merge_samples.png", dpi=170)
    plt.close(fig)
    pd.DataFrame(rows).to_csv(out / "osm_layer_merge_samples.csv", index=False)
    pt.reset_index().to_csv(out / "osm_layer_merge_points.csv", index=False)
    return res


# ---------------------------------------------------------------------- main
def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-name", default="network_grid_reachability_v1")
    ap.add_argument("--sizes", default="3000,6000", help="square cell sizes in EPSG:3857 units")
    args = ap.parse_args(argv)
    out = ROOT / "outputs" / args.output_name
    if out.exists():
        raise SystemExit(f"{out} exists; choose a new --output-name")
    out.mkdir(parents=True)
    meta = json.loads(MANIFEST.read_text())
    X0, Y0 = meta["x_origin_3857"], meta["y_origin_3857"]
    a = osm_arcs()
    res = {"step1_osm_layer": layer_check(a, out)}
    print(json.dumps(res, ensure_ascii=False), flush=True)
    cp = pd.read_parquet(CELLPAIR)
    flow = cp[["grid_from", "grid_to", "f_wd", "cls"]].copy()
    thr = json.loads((ROOT / "outputs/rn_baidu_match_network_v1/match_network_summary.json").read_text())["network"]["threshold_f_wd"]
    flow["cand"] = np.where(flow.cls.isin(["no_edge_between_gate5_nodes", "reverse_only"]) & (flow.f_wd >= thr), flow.cls, "")
    nets = {"v1": lambda: baidu_net(SRC / "raw_connected_v1/Processed_Data/directed_edges.xlsx"),
            "v3": lambda: baidu_net(SRC / "raw_connected_v3/Processed_Data/directed_edges.xlsx"),
            "osm_all": lambda: osm_net(a, False),
            "osm_major": lambda: osm_net(a, True)}
    grids = []
    for s in [float(v) for v in args.sizes.split(",")]:
        grids.append((f"sq{int(s)}_model_origin", X0, Y0, s))
        grids.append((f"sq{int(s)}_half_shift", X0 + s / 2, Y0 + s / 2, s))
    res["step2"] = {}
    pairs_out = []
    for nk, f in nets.items():
        net = Net(f())
        r = {"network": net.info}
        for gk, x0, y0, s in grids:
            fl = flow if gk == "sq3000_model_origin" else None
            r[gk], P = grid_eval(net, x0, y0, s, fl)
            if gk == "sq3000_model_origin":
                pairs_out.append(P.assign(network=nk)[["network", "grid_from", "grid_to", "dist_km", "direct", "path_km", "detour"]])
            print(nk, gk, json.dumps(r[gk], ensure_ascii=False), flush=True)
        res["step2"][nk] = r
        (out / "summary.json").write_text(json.dumps(res, indent=2, ensure_ascii=False, default=float))
    pd.concat(pairs_out).to_parquet(out / "pairs_sq3000_model_origin.parquet", index=False)
    print("done")


if __name__ == "__main__":
    main()
