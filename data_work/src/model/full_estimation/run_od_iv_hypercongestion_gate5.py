"""
Routed-OD instrument and edge-level hypercongestion check (gate5, 2025-05-19..25)
================================================================================

Instrument. Speed's geohash6 OD cache (all mode1 trips, every purpose, date x
departure 10-min slot; read-only) is mapped to 3 km cells and routed on fixed
free-flow shortest paths over the gate5 network (tau_ff_min). A trip departing
in slot s reaches edge (u, v) in slot s + floor((5 + t_ff(o, u)) / 10). The
predicted edge flow Z depends only on who travels where and when, not on the
edge's realised congestion or route choice, so it instruments the observed
all-purpose edge flow in the hourly pace-flow panel of
run_all_traffic_speed_panel_gate5.py.

Hypercongestion. Weekday 10-minute edge profiles (pooled 0519-0523, 30-minute
centred rolling mean) are checked edge by edge: a slot is on the backward-bending
branch if its pace is at least 10% above the pace at the edge's maximum flow,
its flow is at most 90% of that maximum, and it is congested (pace at least
1.3 x the edge minimum). Edges with at least 3 such slots (30 minutes) in a peak
window are flagged.

Needs the duckdb package. Inputs: speed/ and rn_all/ in the panel output dir.
Output: data_work/outputs/<output-name>/od_iv/ (refuses to overwrite)
"""
from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from scipy import sparse
from scipy.sparse.csgraph import dijkstra

sys.path.insert(0, str(Path(__file__).resolve().parent))
from run_all_traffic_speed_panel_gate5 import EDGES, MIN_COVERAGE, REPO, ROOT, WEEKDAYS, slope, within  # noqa: E402

OD_DB = Path("/Users/fxr/Library/CloudStorage/Dropbox/Projects/Projects_FengGu/Speed/Road_processing/GPS data/outputs/grid_od_and_roads_20250519_20250525/cache/week_od.duckdb")
NODES = ROOT / "outputs/model_input_edgeproj_v2_lcc_gate5_od2025_v2/data/qsm_input_nodes_square.parquet"
DATES = [20250519, 20250520, 20250521, 20250522, 20250523, 20250524, 20250525]
PEAK_HOURS = [7, 8, 17, 18]
PEAK_WINDOWS = [range(39, 60), range(99, 120)]            # 06:30-10:00, 16:30-20:00
R = 6378137.0
B32 = "0123456789bcdefghjkmnpqrstuvwxyz"


def geohash_centre(g: str) -> tuple[float, float]:
    lo, la, even = [-180.0, 180.0], [-90.0, 90.0], True
    for ch in g:
        v = B32.index(ch)
        for bit in (16, 8, 4, 2, 1):
            r = lo if even else la
            mid = (r[0] + r[1]) / 2
            r[0 if v & bit else 1] = mid
            even = not even
    return (lo[0] + lo[1]) / 2, (la[0] + la[1]) / 2


def grid_od(meta: dict, cells: set) -> tuple[pd.DataFrame, float]:
    """Mode1 trips by 3 km origin cell x destination cell x date x departure slot (o != d, both gate5 nodes)."""
    # spill to a local temp dir, never next to the read-only Speed cache
    con = duckdb.connect(str(OD_DB), read_only=True, config={"temp_directory": tempfile.mkdtemp(prefix="tl_od_iv_")})
    gh = [g for (g,) in con.sql("select distinct origin_grid_id from od union select distinct destination_grid_id from od").fetchall() if g != "-1"]
    lonlat = np.array([geohash_centre(g) for g in gh])
    col = np.floor((R * np.radians(lonlat[:, 0]) - meta["x_origin_3857"]) / 3000).astype(int)
    row = np.floor((R * np.log(np.tan(np.pi / 4 + np.radians(lonlat[:, 1]) / 2)) - meta["y_origin_3857"]) / 3000).astype(int)
    m = pd.DataFrame({"gh": gh, "cell": [f"sq_{r}_{c}" for r, c in zip(row, col)]})
    m = m[m.cell.isin(cells)]
    con.register("ghmap", m)
    od = con.sql("""
        select o.cell as o, d.cell as d, date, departure_slot as slot, sum(n_unique_trips)::double as trips
        from od join ghmap o on od.origin_grid_id = o.gh join ghmap d on od.destination_grid_id = d.gh
        where mode_value = '1' and o.cell <> d.cell
        group by 1, 2, 3, 4""").df()
    tot = con.sql("select sum(n_unique_trips)::double from od where mode_value = '1'").fetchone()[0]
    return od, float(tot)


def routed_flow(od: pd.DataFrame, nodes: list[str], edges: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Free-flow all-or-nothing assignment of od, time-shifted by the free-flow time to each edge."""
    ix = {g: i for i, g in enumerate(nodes)}
    n = len(nodes)
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
    T = len(DATES) * 144
    od["t"] = od.date.map({d: k for k, d in enumerate(DATES)}) * 144 + od.slot
    OD = sparse.csr_matrix((od.trips.to_numpy(), (od.p.to_numpy(), od.t.to_numpy())), shape=(len(pairs), T))

    o, cur = pairs.oi.to_numpy(), pairs.di.to_numpy().copy()
    rec_p, rec_e, rec_k, alive = [], [], [], np.ones(len(pairs), bool)
    while alive.any():
        a = np.flatnonzero(alive)
        prev = pred[o[a], cur[a]]
        rec_p.append(a)
        rec_e.append(eid[prev, cur[a]])
        rec_k.append(np.floor((5 + dist[o[a], prev]) / 10).astype(int))
        cur[a] = prev
        alive[a] = prev != o[a]
    P, E, K = np.concatenate(rec_p), np.concatenate(rec_e), np.concatenate(rec_k)
    Z = np.zeros((len(edges), T))
    for k in np.unique(K):
        s = K == k
        A = sparse.csr_matrix((np.ones(s.sum()), (E[s], P[s])), shape=(len(edges), len(pairs)))
        Zk = (A @ OD).toarray()
        Z[:, k:] += Zk[:, :T - k]
    z = pd.DataFrame({"grid_from": np.repeat(edges.grid_from.values, T), "grid_to": np.repeat(edges.grid_to.values, T),
                      "date": np.tile(np.repeat(DATES, 144), len(edges)).astype("int32"),
                      "slot10": np.tile(np.arange(T) % 144, len(edges)).astype("int16"), "z_routed": Z.ravel()})
    info = {"od_pairs_routed": int(len(pairs)), "trips_routed": round(float(od.trips.sum())),
            "path_edges_mean": round(len(P) / len(pairs), 2), "max_shift_slots": int(K.max())}
    return z, info


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--output-name", default="all_traffic_speed_panel_rn202505_gate5_v1")
    args = p.parse_args(argv)
    base = ROOT / "outputs" / args.output_name
    out = base / "od_iv"
    if out.exists():
        raise SystemExit(f"{out} exists; choose a new --output-name")
    out.mkdir(parents=True)
    res: dict = {}

    meta = json.loads((REPO / "Platform/reference_inputs/manifest.json").read_text())
    nodes = pd.read_parquet(NODES, columns=["grid_id"]).grid_id.tolist()
    edges = pd.read_parquet(EDGES, columns=["grid_o", "grid_d", "tau_ff_min", "lanes_directional"]).rename(columns={"grid_o": "grid_from", "grid_d": "grid_to"})
    edges["n_kl"] = edges.lanes_directional.where(edges.lanes_directional > 0, edges.lanes_directional.mean())

    # 1. routed-OD instrument
    od, trips_all = grid_od(meta, set(nodes))
    z, info = routed_flow(od, nodes, edges)
    res["routed_od"] = {"mode1_trips_all": round(trips_all), **info,
                        "share_trips_routed": round(info["trips_routed"] / trips_all, 4),
                        "note": "o != d at 3 km cells and both in the gate5 node set; geohash6 centre gives the cell"}
    z.to_parquet(out / "edge_routed_od_flow_10min.parquet", index=False)

    pace = pd.read_parquet(base / "speed/edge_pace_10min.parquet")
    flow = pd.read_parquet(base / "rn_all/edge_flow_10min.parquet")
    k = ["grid_from", "grid_to", "date", "slot10"]
    t10 = pace.merge(flow, on=k, how="left").fillna({"len_flow_all": 0.0}).merge(z, on=k, how="left").fillna({"z_routed": 0.0})
    t10["hour"] = t10.slot10 // 6
    h = t10[t10.coverage >= MIN_COVERAGE].groupby(["grid_from", "grid_to", "date", "hour"]).agg(
        pace=("pace_min_per_km", "mean"), flow=("len_flow_all", "sum"), zr=("z_routed", "sum"), n_slots=("slot10", "size")).reset_index()
    h = h[(h.n_slots == 6) & h.hour.between(6, 21) & (h.flow >= 5) & (h.zr > 0)].merge(edges[["grid_from", "grid_to", "n_kl"]], on=["grid_from", "grid_to"])
    h["edge"] = h.grid_from + ">" + h.grid_to
    h["y"], h["x"], h["z"] = np.log(h.pace), np.log(h.flow / h.n_kl), np.log(h.zr / h.n_kl)
    h["weekday"] = h.date.isin(WEEKDAYS)

    iv = {}
    specs = [("V1_edge_FE+date_x_hour_FE", h, [["edge"], ["date", "hour"]]),
             ("V2_edge_x_hour_FE+date_x_hour_FE (day-to-day incl. weekend)", h, [["edge", "hour"], ["date", "hour"]]),
             ("V3_as_V2_weekdays_only", h[h.weekday], [["edge", "hour"], ["date", "hour"]]),
             ("V4_weekday_peak_hours_edge_FE+date_x_hour_FE", h[h.weekday & h.hour.isin(PEAK_HOURS)], [["edge"], ["date", "hour"]])]
    for lab, d, fes in specs:
        r = within(d, ["y", "x", "z"], fes)
        iv[lab] = {"OLS": slope(r.y, r.x, d.edge), "IV_routed_od": slope(r.y, r.x, d.edge, z=r.z),
                   "first_stage_coef": round(float((r.z @ r.x) / (r.z @ r.z)), 4),
                   "reduced_form": slope(r.y, r.z, d.edge)}
    iv["sample"] = {"edge_hours": int(len(h)), "edges": int(h.edge.nunique()), "hours": "06:00-22:00"}
    iv["corr_log_flow_vs_log_routed_raw"] = round(float(np.corrcoef(h.x, h.z)[0, 1]), 4)
    res["panel_hourly_routed_od_iv"] = iv

    # 2. edge-level hypercongestion, weekday 10-min profiles
    w = t10[t10.date.isin(WEEKDAYS) & (t10.coverage >= MIN_COVERAGE)].groupby(["grid_from", "grid_to", "slot10"]).agg(
        pace=("pace_min_per_km", "mean"), flow=("len_flow_all", "mean"), n_days=("date", "nunique")).reset_index()
    w = w[(w.n_days == len(WEEKDAYS)) & w.slot10.between(36, 131)].sort_values(["grid_from", "grid_to", "slot10"])
    g = w.groupby(["grid_from", "grid_to"])
    w["pace_s"] = g.pace.transform(lambda s: s.rolling(3, center=True, min_periods=2).mean())
    w["flow_s"] = g.flow.transform(lambda s: s.rolling(3, center=True, min_periods=2).mean())
    g = w.groupby(["grid_from", "grid_to"])
    w["qmax"] = g.flow_s.transform("max")
    w["p_at_qmax"] = w.pace_s.where(w.flow_s.eq(w.qmax)).groupby([w.grid_from, w.grid_to]).transform("max")
    w["pace_rel"] = w.pace_s / g.pace_s.transform("min")
    w["peak"] = w.slot10.isin(list(PEAK_WINDOWS[0]) + list(PEAK_WINDOWS[1]))
    w["backward"] = w.peak & (w.pace_s >= 1.1 * w.p_at_qmax) & (w.flow_s <= 0.9 * w.qmax) & (w.pace_rel >= 1.3)
    e = w.groupby(["grid_from", "grid_to"]).agg(
        qmax=("qmax", "first"), p_at_qmax=("p_at_qmax", "first"), pace_min=("pace_s", "min"), pace_max=("pace_s", "max"),
        backward_slots=("backward", "sum"),
        backward_am=("backward", lambda s: int((s & w.loc[s.index, "slot10"].lt(80)).sum())),
        flow_min_backward_rel=("flow_s", lambda s: float((s[w.loc[s.index, "backward"]] / w.loc[s.index, "qmax"]).min()) if w.loc[s.index, "backward"].any() else np.nan),
        min_flow_slots=("slot10", "size")).reset_index()
    e = e.merge(edges[["grid_from", "grid_to", "lanes_directional"]], on=["grid_from", "grid_to"])
    e["flagged"] = e.backward_slots >= 3
    # day-level robustness: the same rule on each weekday separately (no pooling), count of days flagged
    d = t10[t10.date.isin(WEEKDAYS) & (t10.coverage >= MIN_COVERAGE) & t10.slot10.between(36, 131)].sort_values(["grid_from", "grid_to", "date", "slot10"]).copy()
    gd = d.groupby(["grid_from", "grid_to", "date"])
    d["pace_s"] = gd.pace_min_per_km.transform(lambda s: s.rolling(3, center=True, min_periods=2).mean())
    d["flow_s"] = gd.len_flow_all.transform(lambda s: s.rolling(3, center=True, min_periods=2).mean())
    gd = d.groupby(["grid_from", "grid_to", "date"])
    d["qmax"] = gd.flow_s.transform("max")
    d["p_at_qmax"] = d.pace_s.where(d.flow_s.eq(d.qmax)).groupby([d.grid_from, d.grid_to, d.date]).transform("max")
    d["pace_rel"] = d.pace_s / gd.pace_s.transform("min")
    d["backward"] = d.slot10.isin(list(PEAK_WINDOWS[0]) + list(PEAK_WINDOWS[1])) & (d.pace_s >= 1.1 * d.p_at_qmax) & (d.flow_s <= 0.9 * d.qmax) & (d.pace_rel >= 1.3)
    days = (d.groupby(["grid_from", "grid_to", "date"]).backward.sum() >= 3).groupby(["grid_from", "grid_to"]).sum().rename("days_flagged")
    e = e.merge(days.reset_index(), on=["grid_from", "grid_to"], how="left").fillna({"days_flagged": 0})
    e.sort_values(["backward_slots", "pace_max"], ascending=False).to_csv(out / "edge_hypercongestion_weekday.csv", index=False)
    res["hypercongestion_edge_level"] = {
        "edges_checked": int(len(e)),
        "edges_flagged_pooled": int(e.flagged.sum()), "share_flagged_pooled": round(float(e.flagged.mean()), 4),
        "edges_flagged_am_ge3": int((e.backward_am >= 3).sum()),
        "edges_flagged_pooled_and_ge3_of_5_days": int((e.flagged & (e.days_flagged >= 3)).sum()),
        "days_flagged_distribution": {int(k): int(v) for k, v in e.days_flagged.value_counts().sort_index().items()},
        "flagged_min_flow_rel_p10_p50_p90": [round(float(x), 3) for x in e.flow_min_backward_rel[e.flagged].quantile([.1, .5, .9])],
        "flagged_lanes_median": float(e.lanes_directional[e.flagged].median()) if e.flagged.any() else None,
        "all_lanes_median": float(e.lanes_directional.median()),
        "rule": "peak 06:30-10:00 / 16:30-20:00; pace >= 1.1 x pace at max flow, flow <= 0.9 x max flow, pace >= 1.3 x edge min; >= 3 ten-minute slots",
    }
    (out / "od_iv_hypercongestion_summary.json").write_text(json.dumps(res, indent=2, ensure_ascii=False))
    print(json.dumps(res, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
