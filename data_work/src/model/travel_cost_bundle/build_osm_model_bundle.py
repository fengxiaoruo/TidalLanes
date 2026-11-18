"""
Model input bundle on the OSM network (schemes 1A / 1B), 2025 OD, gate5 rules
============================================================================
Purpose: rerun the mainline on a different road network with everything else held
fixed (OD, parameters), so the effect of the network alone can be read off.

Edges   = directed 4-neighbour pairs on the model-origin 3 km grid from
          outputs/osm_grid_travel_time_v2 (pairs_<scheme>_sq3000_model_origin):
            tau_obs_min = tm_AM, tau_ff_min = tm_FF (mean over access-node pairs)
            route_length_m = total_link_len_m = centre-node AM path km
          detour gate 5 (centre-node AM path km / cell-centre distance), as gate5.
Lanes   = length-weighted mean over the centre-node AM path (bottleneck kept too).
          Arc lanes: arcs matched to a Baidu segment take its road_class_lane_mean
          (raw_segment_master, the gate5 lane table); unmatched arcs take the
          length-weighted modal Baidu lane value of matched arcs of the same OSM class.
Nodes   = cells in the largest strongly connected component of the gated edges;
          --cells od_bundle (default, *_v2) first keeps only edges between cells of the
          OD bundle below, --cells network (*_v1) keeps every cell the network reaches.
          residents / jobs / OD from model_input_edgeproj_v2_lcc_gate5_od2025_v2;
          network cells outside that bundle enter with zero residents and jobs, and OD
          is kept only between cells of that bundle, so the OD sample changes only by
          the cells the new network cannot reach.
Inputs read only. Output: data_work/outputs/<version-id>/ (refuses to overwrite)
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

DATA_WORK = Path(__file__).resolve().parents[3]
if str(DATA_WORK) not in sys.path:
    sys.path.insert(0, str(DATA_WORK))

from src.network import build_osm_grid_travel_time as og  # noqa: E402

O = DATA_WORK / "outputs"
SRC_TT = O / "osm_grid_travel_time_v2"
OD_BUNDLE = O / "model_input_edgeproj_v2_lcc_gate5_od2025_v2"
SEG = O / "manual_centerline_rules_v11/data/raw_segment_master.parquet"
GATE = 5.0
SCHEMES = {"osm1a": "osm1a", "osm1b": "osm1b_L300"}


def arc_lanes(a: pd.DataFrame) -> tuple[pd.Series, dict]:
    seg = pd.read_parquet(SEG, columns=["roadseg_id", "road_class_lane_mean"]).dropna()
    lut = seg.groupby(seg.roadseg_id.astype(str)).road_class_lane_mean.first()
    ln = a.roadseg_id.astype(str).map(lut)
    m = ln.notna()
    w = pd.DataFrame({"cls": a.road_class[m], "ln": ln[m], "km": a.length_m[m]})
    mode = w.groupby(["cls", "ln"]).km.sum().reset_index().sort_values("km").drop_duplicates("cls", keep="last").set_index("cls").ln
    fallback = float(w.groupby("ln").km.sum().idxmax())
    ln = ln.fillna(a.road_class.map(mode)).fillna(fallback)
    info = {"km_share_lanes_from_baidu_segment": round(float(a.length_m[m].sum() / a.length_m.sum()), 4),
            "osm_class_modal_lanes": {k: float(v) for k, v in mode.items()}, "fallback_lanes": fallback}
    return ln, info


def path_lanes(G: og.Graph, P: pd.DataFrame, x0: float, y0: float, size: float) -> pd.DataFrame:
    """Centre-node AM path per pair (same access rule and arc choice as og.grid_pairs):
    time, km, length-weighted lanes, bottleneck lanes."""
    r = np.floor((G.y - y0) / size).astype(int)
    c = np.floor((G.x - x0) / size).astype(int)
    cx, cy = x0 + (c + 0.5) * size, y0 + (r + 0.5) * size
    df = pd.DataFrame({"r": r, "c": c, "d2": (G.x - cx) ** 2 + (G.y - cy) ** 2, "node": np.arange(G.N)})
    acc = df.loc[df.groupby(["r", "c"]).d2.idxmin(), ["r", "c", "node"]]
    node_of = {f"sq_{a}_{b}": n for a, b, n in zip(acc.r, acc.c, acc.node)}
    arcs, W = G.arc_of["AM"]
    aidx = pd.Series(arcs.i.to_numpy(), index=pd.MultiIndex.from_arrays([arcs.u.to_numpy(), arcs.v.to_numpy()]))
    tvec = np.zeros(len(G.a)); tvec[arcs.i.to_numpy()] = arcs.t.to_numpy()
    km = G.a.length_m.to_numpy() / 1000
    lanes = G.a.lanes.to_numpy()
    res = {}
    src = sorted(set(P.grid_from))
    for k in range(0, len(src), 64):
        batch = src[k:k + 64]
        D, pred = dijkstra(W, directed=True, indices=[node_of[g] for g in batch], return_predecessors=True, limit=120.0)
        for bi, g in enumerate(batch):
            s = node_of[g]
            for h in P.grid_to[P.grid_from == g]:
                tgt = node_of[h]
                if not np.isfinite(D[bi, tgt]):
                    continue
                us, vs, v = [], [], tgt
                while v != s:
                    u = pred[bi, v]; us.append(u); vs.append(v); v = u
                ids = aidx.reindex(pd.MultiIndex.from_arrays([us, vs])).to_numpy()
                res[(g, h)] = (tvec[ids].sum(), km[ids].sum(), float((lanes[ids] * km[ids]).sum() / km[ids].sum()), float(lanes[ids].min()))
    out = pd.DataFrame([(g, h, *v) for (g, h), v in res.items()],
                       columns=["grid_from", "grid_to", "t_AM_check", "path_km", "lanes_len_weighted", "lanes_bottleneck"])
    return out


def assemble(P: pd.DataFrame, L: pd.DataFrame, out: Path, vid: str, *, source_label: str, cells: str) -> dict:
    """Gate, restrict cells, take the strongly connected cell component and write the
    nodes / edges / OD bundle into out/data. P: grid pairs with tm_*, t_AM, obs_share_AM,
    dist_km, n_access_pairs; L: path_lanes() output. Returns summary fields."""
    E = P.merge(L, on=["grid_from", "grid_to"], how="left")
    t_check = float(np.nanmax(np.abs(E.t_AM_check - E.t_AM))) if E.t_AM_check.notna().any() else np.nan
    E = E[E.tm_AM.notna() & E.tm_FF.notna() & (E.tm_AM > 0) & (E.tm_FF > 0) & E.path_km.notna()].copy()
    n_valid = len(E)
    E["detour"] = E.path_km / E.dist_km
    E = E[E.detour <= GATE].copy()
    n_gated = n_valid - len(E)
    od_cells = set(pd.read_parquet(OD_BUNDLE / "data/qsm_input_nodes_square.parquet", columns=["grid_id"]).grid_id)
    if cells == "od_bundle":
        # same cell set as the gate5 OD bundle: only the network changes
        E = E[E.grid_from.isin(od_cells) & E.grid_to.isin(od_cells)].copy()

    # largest strongly connected component over cells
    codes, uniq = pd.factorize(pd.concat([E.grid_from, E.grid_to], ignore_index=True))
    n = len(E)
    g = coo_matrix((np.ones(n), (codes[:n], codes[n:])), shape=(len(uniq),) * 2)
    _, lab = connected_components(g, directed=True, connection="strong")
    big = set(uniq[lab == np.bincount(lab).argmax()])
    E = E[E.grid_from.isin(big) & E.grid_to.isin(big)].copy()

    src_nodes = pd.read_parquet(OD_BUNDLE / "data/qsm_input_nodes_square.parquet")
    src_od = pd.read_parquet(OD_BUNDLE / "data/qsm_input_od_square.parquet")
    cells = sorted(big)
    nodes = pd.DataFrame({"grid_id": cells}).merge(src_nodes.drop(columns=["node_i"]), on="grid_id", how="left")
    nodes["in_od_bundle"] = nodes.residents.notna().astype(int)
    for c in ["residents", "jobs", "residents_2022", "jobs_2022"]:
        nodes[c] = nodes[c].fillna(0.0)
    nodes["node_i"] = np.arange(len(nodes))
    nodes["component"] = 1
    nodes["keep_for_model"] = 1
    nodes["in_largest_component"] = 1
    nodes["grid_type"] = "square"
    nodes["version_id"] = vid
    idx = dict(zip(nodes.grid_id, nodes.node_i))

    od = src_od[src_od.home_grid.isin(idx) & src_od.work_grid.isin(idx)].copy()
    od["home_i"] = od.home_grid.map(idx).astype(int)
    od["work_i"] = od.work_grid.map(idx).astype(int)
    od["origin_node"], od["destination_node"] = od.home_i, od.work_i
    od["version_id"] = vid
    # residents / jobs are the OD marginals of the kept OD (as in the source bundle)
    nodes["residents"] = nodes.grid_id.map(od.groupby("home_grid").commuters_road.sum()).fillna(0.0)
    nodes["jobs"] = nodes.grid_id.map(od.groupby("work_grid").commuters_road.sum()).fillna(0.0)

    edges = pd.DataFrame({
        "grid_o": E.grid_from.to_numpy(), "grid_d": E.grid_to.to_numpy(),
        "i": E.grid_from.map(idx).to_numpy(), "j": E.grid_to.map(idx).to_numpy(),
        "tau_obs_min": E.tm_AM.to_numpy(), "tau_ff_min": E.tm_FF.to_numpy(),
        "t_min": E.tm_AM.to_numpy(), "t_ff_min": E.tm_FF.to_numpy(),
        "route_length_m": E.path_km.to_numpy() * 1000, "total_link_len_m": E.path_km.to_numpy() * 1000,
        "lanes_directional": E.lanes_len_weighted.to_numpy(), "capacity_proxy": E.lanes_len_weighted.to_numpy(),
        "lane_est_length_weighted": E.lanes_len_weighted.to_numpy(),
        "lane_est_path_len_weighted": E.lanes_len_weighted.to_numpy(), "lane_est_path_bottleneck": E.lanes_bottleneck.to_numpy(),
        "lanes_directional_source": "path_len_weighted",
        "obs_share_AM_centre_path": E.obs_share_AM.to_numpy(), "detour_centre_path": E.detour.to_numpy(),
        "n_access_pairs": E.n_access_pairs.to_numpy(),
        "period": "AM", "grid_type": "square", "version_id": vid,
        "tau_obs_source": f"{source_label}:tm_AM", "tau_ff_source": f"{source_label}:tm_FF",
        "tau_ff_imputed_flag": 0, "tau_ff_missing_original": 0,
    })
    edges["tau_consistency_flag"] = np.where(edges.tau_obs_min >= edges.tau_ff_min, "ok", "tau_obs_lt_tau_ff")

    (out / "data").mkdir(parents=True, exist_ok=True)
    nodes.to_parquet(out / "data/qsm_input_nodes_square.parquet", index=False)
    edges.to_parquet(out / "data/qsm_input_edges_square.parquet", index=False)
    od.to_parquet(out / "data/qsm_input_od_square.parquet", index=False)

    src_total = float(src_od.commuters_road.sum())
    src_cross = src_od.home_grid != src_od.work_grid
    kept_cross = od.home_grid != od.work_grid
    rev = edges.merge(edges, left_on=["grid_o", "grid_d"], right_on=["grid_d", "grid_o"], suffixes=("", "_r"))
    rr = rev.tau_obs_min / rev.tau_obs_min_r
    info = {
        "version_id": vid, "cells": cells, "pairs_source": len(P),
        "max_abs_diff_centre_path_time_vs_source_min": t_check,
        "edges_valid": n_valid, "edges_dropped_detour_gate5": n_gated, "edges_final": int(len(edges)),
        "nodes": int(len(nodes)), "nodes_not_in_od_bundle": int((nodes.in_od_bundle == 0).sum()),
        "od_bundle_nodes": int(len(src_nodes)), "od_bundle_nodes_reached": int(nodes.in_od_bundle.sum()),
        "commuters_road_share_kept": round(float(od.commuters_road.sum() / src_total), 4),
        "cross_cell_commuters_share_kept": round(float(od.commuters_road[kept_cross].sum() / src_od.commuters_road[src_cross].sum()), 4),
        "tau_obs_median": round(float(edges.tau_obs_min.median()), 3), "tau_ff_median": round(float(edges.tau_ff_min.median()), 3),
        "tau_obs_lt_tau_ff_share": round(float((edges.tau_obs_min < edges.tau_ff_min).mean()), 4),
        "directional_ratio_median_min_over_max": round(float(np.minimum(rr, 1 / rr).median()), 3),
        "lanes_median": float(edges.lanes_directional.median()), "lanes_mean": round(float(edges.lanes_directional.mean()), 3),
        "detour_gate": GATE, "od_source": OD_BUNDLE.name,
    }
    return info


def main(argv: list[str] | None = None) -> None:
    global OD_BUNDLE
    ap = argparse.ArgumentParser()
    ap.add_argument("--scheme", choices=list(SCHEMES), required=True)
    ap.add_argument("--version-id", default=None)
    ap.add_argument("--cells", choices=["od_bundle", "network"], default="od_bundle",
                    help="od_bundle: keep only cells of the gate5 OD bundle (main comparison); "
                         "network: all cells the OSM network reaches (v1 bundles)")
    ap.add_argument("--od-bundle", default=OD_BUNDLE.name,
                    help="gate5 bundle whose nodes / OD are carried over (OD variants: build_od2025_gate5_bundle --od-variant)")
    ap.add_argument("--tt-source", default="osm_grid_travel_time_v2",
                    help="osm_grid_travel_time output to read (split-day bundles use --am-days variants)")
    args = ap.parse_args(argv)
    OD_BUNDLE = O / args.od_bundle
    src_tt = O / args.tt_source
    vid = args.version_id or f"model_input_{SCHEMES[args.scheme]}_gate5_od2025_" + ("v2" if args.cells == "od_bundle" else "v1")
    out = O / vid
    if out.exists():
        raise SystemExit(f"{out} exists; choose a new --version-id")

    meta = json.loads(og.MANIFEST.read_text())
    x0, y0 = meta["x_origin_3857"], meta["y_origin_3857"]
    a = pd.read_parquet(src_tt / "arc_speeds_1A.parquet")
    a["lanes"], lane_info = arc_lanes(a)
    if args.scheme == "osm1b":
        # same rule and parameter as the stored 1B pairs (L = 300 m)
        # (osm_grid_travel_time_v2 predates the params block; it ran with the default 300 m)
        og.L_GAP = float(json.loads((src_tt / "summary.json").read_text()).get("params", {}).get("l_gap_m", 300.0))
        assert SCHEMES["osm1b"] == f"osm1b_L{int(og.L_GAP)}"
        a = a[og.gap_keep(a)].copy()
    G = og.Graph(a.reset_index(drop=True))
    P = pd.read_parquet(src_tt / f"pairs_{SCHEMES[args.scheme]}_sq3000_model_origin.parquet")
    L = path_lanes(G, P, x0, y0, 3000.0)
    info = assemble(P, L, out, vid, source_label=f"{src_tt.name}:{SCHEMES[args.scheme]}", cells=args.cells)
    info.update({"scheme": SCHEMES[args.scheme], "lane_rule": lane_info,
                 "tt_source": args.tt_source, "od_bundle": args.od_bundle, "cells_rule": args.cells})
    (out / "BUNDLE_META.json").write_text(json.dumps(info, indent=2, ensure_ascii=False, default=float))
    print(json.dumps(info, indent=2, ensure_ascii=False, default=float))


if __name__ == "__main__":
    main()
