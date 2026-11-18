"""
Congestion elasticity lambda under the strictest RN-Baidu matching (edge and segment level)
==========================================================================================

Step 0 reproduces the 12 baseline numbers (S1/S2/S3 x OLS/IV x gate5 edge/segment,
06-22 h, 0519-0525) with this file's own flow builders; the run stops if any
differs from the stored result by more than TOL.

Strict match. Starting from the original matches (segment_rn_match: midpoint
<= 20 m, chord >= 20 m, heading <= 30 deg, nearest wins), chords are removed in
order:
    1 parallel ambiguity   a second valid candidate segment within 5 m of the winner's distance
    2 break / bend         either chord endpoint more than 20 m from the matched segment line
    3 parallel overlap     two chords on one segment overlapping > 20 m along it, no shared node,
                           lateral offsets (signed midpoint distance) differing by > 8 m:
                           the one farther from the Baidu line is dropped
    4 nested overlap       two chords sharing a node and overlapping > 20 m: the longer is dropped
    5 expressway mismatch  Baidu class 2 while the nearest OSM road is tertiary,
                           residential/unclassified or service
Vehicle-km shares use weekday vehicle-km (vkm_wd of rn_pair_class).

Segment level: flow = chord-length-weighted mean passes over the kept chords;
matched share of length recomputed from kept chords; other sample rules as in
run_segment_lambda.py. IV = routed OD of the segment's largest-weight gate5 edge.
SE clustered by segment (comparable to the stored results, whose field
se_cluster_edge is in fact by segment) and by that gate5 edge.

Edge level: q = kept chords allocated by length_alloc (passes x cells), summed
over the hour, per lane; t = segment_edge_weights-weighted pace over the Baidu
segments that still hold a kept chord, coverage against those segments' total
weight (>= 0.8 in all 6 slots); rules otherwise as run_od_iv_hypercongestion_gate5.py.

Also: lambda after every cumulative step, each rule on its own, an
original-match "same roads" variant (q and t from all matched chords and
segments), and strict + stale-slot removal (flag_runs, >= 60 min constant pace).

Inputs read only. Output: data_work/outputs/<output-name>/ (refuses to overwrite)
"""
from __future__ import annotations

import argparse
import glob
import json
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.csv as pc
import shapely
from scipy import sparse

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parents[1] / "diagnostics"))
from run_all_traffic_speed_panel_gate5 import EDGES, MIN_COVERAGE, ROOT, WEEKDAYS, rnmap, slope  # noqa: E402
import build_segment_speed_flow_panel as bp  # noqa: E402
import run_segment_lambda as sl  # noqa: E402
from plot_rn_baidu_match_citywide import CLASS, CLOSE_M, candidates  # noqa: E402
from run_segment_lambda_robustness import flag_runs  # noqa: E402

REPO = ROOT.parent
SEGP = ROOT / "outputs/segment_speed_flow_rn202505_v1"
GATE5 = sl.GATE5
DATES = [20250519, 20250520, 20250521, 20250522, 20250523, 20250524, 20250525]
T = len(DATES) * 144
END_M, OVERLAP_M, OFFSET_M = 20.0, 20.0, 8.0
MINOR = ["tertiary", "residential/unclassified", "service"]
TOL = 0.01
SPECS = [("S1", "S1 all day: unit FE + date x hour FE"), ("S2", "S2 day-to-day: unit x hour FE + date x hour FE"),
         ("S3", "S3 weekday peak hours 7, 8, 17, 18")]


# ---------------------------------------------------------------- estimation
def within(df: pd.DataFrame, cols: list[str], fes: list[list[str]], iters: int = 30) -> pd.DataFrame:
    """Same alternating projections as run_all_traffic_speed_panel_gate5.within, on integer group codes."""
    r = df[cols].to_numpy(float).copy()
    codes = [df.groupby(f, sort=False).ngroup().to_numpy() for f in fes]
    cnts = [np.bincount(k) for k in codes]
    for _ in range(iters):
        for k, c in zip(codes, cnts):
            for j in range(r.shape[1]):
                r[:, j] -= (np.bincount(k, r[:, j], minlength=len(c)) / c)[k]
    return pd.DataFrame(r, columns=cols, index=df.index)


def est(y, x, clusters: dict, z=None) -> dict:
    """slope() once per clustering; names say what the cluster is."""
    o = {}
    for name, cl in clusters.items():
        s = slope(y, x, cl, z=z)
        o.setdefault("b", s["b"])
        o["n"] = s["n"]
        o[f"se_cluster_{name}"] = s["se_cluster_edge"]
        o[f"n_clusters_{name}"] = s["n_clusters"]
        if "first_stage_F_cluster" in s:
            o[f"first_stage_F_cluster_{name}"] = s["first_stage_F_cluster"]
    return o


def fit(h: pd.DataFrame, unit: str, clusters: list[str]) -> dict:
    res = {}
    for key, _ in SPECS:
        d = h[h.weekday & h.hour.isin(sl.PEAK)] if key == "S3" else h
        fes = [[unit, "hour"], ["date", "hour"]] if key == "S2" else [[unit], ["date", "hour"]]
        r = within(d, ["y", "x"], fes)
        res[key] = {"OLS": est(r.y, r.x, {c: d[c] for c in clusters})}
        s = d.dropna(subset=["zr"])
        r = within(s, ["y", "x", "zr"], fes)
        res[key]["IV"] = est(r.y, r.x, {c: s[c] for c in clusters}, z=r.zr)
    return res


# ---------------------------------------------------------------- strict filters
def strict_flags(p: pd.DataFrame, seg: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Per matched chord: geometry relative to its matched segment and the stand-alone flags 1, 2, 5."""
    q = p[p.cat.eq("matched")].sort_index().copy()
    c = candidates(q, seg)
    first = c[c["rank"].eq(0)].set_index("i")
    second = c[c["rank"].eq(1)].set_index("i")
    sidx = pd.Series(np.arange(len(seg)), index=seg.roadseg_id)
    q["s"] = sidx.reindex(q.roadseg_id).to_numpy()
    check = {"rerun_reproduces_original_match": round(float((first.s.reindex(q.index).to_numpy() == q.s.to_numpy()).mean()), 4)}
    d1, d2 = first.dist.reindex(q.index), second.dist.reindex(q.index)
    q["f1"] = ((d2 - d1) < CLOSE_M).fillna(False).to_numpy()

    L = shapely.from_wkb(seg.raw_geometry.values)[q.s.to_numpy()]
    a, b = shapely.points(q.x_from, q.y_from), shapely.points(q.x_to, q.y_to)
    mid = shapely.points((q.x_from + q.x_to) / 2, (q.y_from + q.y_to) / 2)
    q["end_m"] = np.maximum(shapely.distance(a, L), shapely.distance(b, L))
    q["f2"] = q.end_m > END_M

    sa, sb, sm, ln = shapely.line_locate_point(L, a), shapely.line_locate_point(L, b), shapely.line_locate_point(L, mid), shapely.length(L)
    q["lo"], q["hi"] = np.minimum(sa, sb), np.maximum(sa, sb)
    t0 = shapely.get_coordinates(shapely.line_interpolate_point(L, np.clip(sm - 10, 0, ln)))
    t1 = shapely.get_coordinates(shapely.line_interpolate_point(L, np.clip(sm + 10, 0, ln)))
    pr = shapely.get_coordinates(shapely.line_interpolate_point(L, sm))
    mx, my = ((q.x_from + q.x_to) / 2).to_numpy(), ((q.y_from + q.y_to) / 2).to_numpy()
    cross = (t1[:, 0] - t0[:, 0]) * (my - pr[:, 1]) - (t1[:, 1] - t0[:, 1]) * (mx - pr[:, 0])
    q["offset"] = np.sign(cross) * shapely.distance(mid, L)      # signed lateral offset, segment-digitised side
    q["f5"] = (seg.road_class.to_numpy()[q.s.to_numpy()] == 2) & q.osm_class.isin(MINOR).to_numpy()
    return q, check


def overlap_drops(q: pd.DataFrame, keep: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Rules 3 and 4 among chords in `keep` (same segment pairs). Returns drop masks (rule 3, rule 4)."""
    k = q[keep][["roadseg_id", "rn_from", "rn_to", "lo", "hi", "offset", "chord_m"]].copy()
    k["j"] = np.flatnonzero(keep)
    m = k.merge(k, on="roadseg_id", suffixes=("_a", "_b"))
    m = m[m.j_a < m.j_b]
    ov = np.minimum(m.hi_a, m.hi_b) - np.maximum(m.lo_a, m.lo_b)
    shared = (m.rn_from_a.eq(m.rn_from_b) | m.rn_from_a.eq(m.rn_to_b) | m.rn_to_a.eq(m.rn_from_b) | m.rn_to_a.eq(m.rn_to_b))
    r3 = m[(ov > OVERLAP_M) & ~shared & ((m.offset_a - m.offset_b).abs() > OFFSET_M)]
    far = np.where(r3.offset_a.abs() > r3.offset_b.abs(), r3.j_a, r3.j_b)
    r4 = m[(ov > OVERLAP_M) & shared]
    longer = np.where(r4.chord_m_a > r4.chord_m_b, r4.j_a, r4.j_b)
    d3, d4 = np.zeros(len(q), bool), np.zeros(len(q), bool)
    d3[far], d4[longer] = True, True
    return d3, d4


# ---------------------------------------------------------------- flows
def read_counts(files: list[str], pair_ix: pd.Series, A_all: sparse.csr_matrix, m_ix: np.ndarray, cache: Path | None):
    """One pass over the RN counts: baseline edge flow (all pairs, edges x T) and matched-pair passes (pairs x T)."""
    if cache is not None and (cache / "Pm.npz").exists():
        return np.load(cache / "E_all.npy"), sparse.load_npz(cache / "Pm.npz")
    cols = [f"n_valid_transitions_s{s:02d}" for s in range(36)]
    dix = {d: k for k, d in enumerate(DATES)}
    E = np.zeros((A_all.shape[0], T))
    Pm = sparse.csr_matrix((len(m_ix), T))
    sel = sparse.csr_matrix((np.ones(len(m_ix)), (np.arange(len(m_ix)), m_ix)), shape=(len(m_ix), A_all.shape[1]))
    t0 = time.time()
    for i, f in enumerate(files):
        d = pc.read_csv(f, convert_options=pc.ConvertOptions(include_columns=["date", "rn_pair_id", "time_block_6h", *cols],
                                                           column_types={"rn_pair_id": "string"})).to_pandas()
        code = pair_ix.reindex(d.rn_pair_id).to_numpy()
        ok = ~np.isnan(code)
        a = d[cols].to_numpy(float)[ok]
        base = (d.date.map(dix).to_numpy()[ok] * 144 + d.time_block_6h.to_numpy()[ok] * 36)
        r, s = np.nonzero(a > 0)
        P = sparse.csr_matrix((a[r, s], (code[ok].astype(int)[r], base[r] + s)), shape=(A_all.shape[1], T))
        E += (A_all @ P).toarray()
        Pm = Pm + sel @ P
        if i % 50 == 0:
            print(f"counts {i + 1}/{len(files)} {time.time() - t0:.0f}s", flush=True)
    if cache is not None:
        cache.mkdir(parents=True, exist_ok=True)
        np.save(cache / "E_all.npy", E)
        sparse.save_npz(cache / "Pm.npz", Pm)
    return E, Pm


def to_long(M: np.ndarray, keys: pd.DataFrame, name: str, min_val: float, per: int) -> pd.DataFrame:
    """Dense unit x time matrix -> long rows with value >= min_val (per = 144 for 10-min, 24 for hour)."""
    r, c = np.nonzero(M >= min_val)
    out = keys.iloc[r].reset_index(drop=True)
    out["date"] = np.asarray(DATES, dtype="int64")[c // per]
    out["slot10" if per == 144 else "hour"] = (c % per).astype("int64")
    out[name] = M[r, c]
    return out


# ---------------------------------------------------------------- panels
class Data:
    """Everything the panels need, loaded once."""

    def __init__(self, cache: Path | None):
        meta = json.loads((REPO / "Platform/reference_inputs/manifest.json").read_text())
        edges = pd.read_parquet(EDGES, columns=["grid_o", "grid_d", "lanes_directional"]).rename(columns={"grid_o": "grid_from", "grid_d": "grid_to"})
        edges["n_kl"] = edges.lanes_directional.where(edges.lanes_directional > 0, edges.lanes_directional.mean())
        self.edges = edges[["grid_from", "grid_to", "n_kl"]].reset_index(drop=True)
        eix = pd.Series(np.arange(len(edges)), index=pd.MultiIndex.from_frame(edges[["grid_from", "grid_to"]]))

        self.p = pd.read_parquet(CLASS)
        nodes = pd.concat([pd.read_csv(f, dtype={"rn_id": str}) for f in sorted(rnmap.SPEED_DICT.glob("rn_node_dictionary_202505_b*.csv"))])
        nodes = nodes[nodes.coordinate_missing.eq(0)].drop_duplicates("rn_id").set_index("rn_id")
        pr = self.p[["rn_pair_id", "rn_from", "rn_to"]].copy()
        for end in ["from", "to"]:
            n = nodes.reindex(pr[f"rn_{end}"])
            pr[f"lon_{end}"], pr[f"lat_{end}"] = n.lon.values, n.lat.values
        al = rnmap.length_alloc(pr, meta["x_origin_3857"], meta["y_origin_3857"], set(eix.index))
        al = al[al.allocated]
        pair_ix = pd.Series(np.arange(len(self.p)), index=self.p.rn_pair_id)
        self.A_all = sparse.csr_matrix((al.cells.to_numpy(), (eix.reindex(pd.MultiIndex.from_frame(al[["grid_from", "grid_to"]])).to_numpy(),
                                                               pair_ix.reindex(al.rn_pair_id).to_numpy())), shape=(len(edges), len(self.p)))
        # matched pairs, in segment_rn_match order
        self.m = pd.read_parquet(SEGP / "segment_rn_match.parquet")
        m_ix = pair_ix.reindex(self.m.rn_pair_id).to_numpy()
        self.A_m = self.A_all[:, m_ix]
        files = sorted(glob.glob(str(bp.COUNTS / "rn_pressure_counts_*.csv")))
        self.E_all, self.Pm = read_counts(files, pair_ix, self.A_all, m_ix, cache)
        self.files = len(files)

        self.meta = pd.read_parquet(SEGP / "segment_meta.parquet")
        self.segs = pd.Index(self.meta.roadseg_id)
        self.S = sparse.csr_matrix((np.ones(len(self.m)), (self.segs.get_indexer(self.m.roadseg_id), np.arange(len(self.m)))),
                                   shape=(len(self.segs), len(self.m)))
        # Baidu pace (segment x 10 min) and stale flag
        t = flag_runs(pd.read_parquet(SEGP / "segment_pace_10min.parquet"))
        self.pace10 = t[["roadseg_id", "date", "slot10", "pace_min_per_km", "stale"]]
        # routed-OD instrument and segment -> gate5 edge weights
        z = pd.read_parquet(GATE5 / "od_iv/edge_routed_od_flow_10min.parquet")
        self.z10 = z
        z = z.assign(hour=z.slot10 // 6).groupby(["grid_from", "grid_to", "date", "hour"], as_index=False).z_routed.sum()
        self.zh = z
        self.sw = pd.read_parquet(GATE5 / "speed/segment_edge_weights.parquet")
        self.sw_main = self.sw.sort_values("w").drop_duplicates("roadseg_id", keep="last")[["roadseg_id", "grid_from", "grid_to"]]
        self.edge_pace_base = pd.read_parquet(GATE5 / "speed/edge_pace_10min.parquet")
        self.eix = eix

    # ---- segment level
    def segment_flow(self, keep: np.ndarray) -> tuple[pd.DataFrame, pd.Series]:
        """keep: boolean over self.m rows. Hourly segment flow (>= 5) and matched share of length."""
        ch = self.m.chord_m.to_numpy() * keep
        W = self.S @ sparse.diags(ch)
        wsum = np.asarray(W.sum(axis=1)).ravel()
        F = (W @ self.Pm).toarray()
        has = wsum > 0
        F = F[has] / wsum[has, None]
        Fh = F.reshape(len(F), len(DATES), 24, 6).sum(-1).reshape(len(F), -1)
        fh = to_long(Fh, pd.DataFrame({"roadseg_id": self.segs[has]}), "flow", 5.0, 24)
        share = pd.Series((wsum / self.meta.seg_len_m.to_numpy()).clip(max=5), index=self.segs)[has]
        return fh, share

    def segment_panel(self, keep: np.ndarray, drop_stale: bool = False) -> pd.DataFrame:
        fh, share = self.segment_flow(keep)
        t = self.pace10[~self.pace10.stale] if drop_stale else self.pace10
        t = t.assign(hour=t.slot10 // 6)
        h = t.groupby(["roadseg_id", "date", "hour"]).agg(pace=("pace_min_per_km", "mean"), n=("slot10", "size")).reset_index()
        h = h[(h.n >= 4) & h.hour.between(6, 21)]
        h = h.merge(fh, on=["roadseg_id", "date", "hour"])
        good = share[share >= sl.MIN_MATCH_SHARE].index
        mf = h[h.date.isin(WEEKDAYS)].groupby("roadseg_id").flow.mean()
        h = h[h.roadseg_id.isin(set(good) & set(mf[mf >= sl.MIN_MEAN_FLOW].index)) & (h.flow >= 5)]
        h = h.merge(self.sw_main, on="roadseg_id", how="left").merge(self.zh, on=["grid_from", "grid_to", "date", "hour"], how="left")
        h["y"], h["x"] = np.log(h.pace), np.log(h.flow)
        h["zr"] = np.log(h.z_routed.where(h.z_routed > 0))
        h["weekday"] = h.date.isin(WEEKDAYS)
        h["segment"] = h.roadseg_id
        h["edge"] = (h.grid_from + ">" + h.grid_to).fillna("seg:" + h.roadseg_id)
        return h

    # ---- edge level
    def edge_flow(self, keep: np.ndarray | None) -> pd.DataFrame:
        """keep None -> baseline (all pairs); else boolean over matched pairs."""
        E = self.E_all if keep is None else (self.A_m @ sparse.diags(keep.astype(float)) @ self.Pm).toarray()
        return to_long(E, self.edges[["grid_from", "grid_to"]], "len_flow_all", 5.0, 144)

    def edge_pace(self, segs: set | None, drop_stale: bool = False) -> pd.DataFrame:
        """segs None -> stored baseline edge_pace_10min; else weighted pace over the given Baidu segments."""
        if segs is None:
            return self.edge_pace_base
        w = self.sw[self.sw.roadseg_id.isin(segs)]
        t = self.pace10[self.pace10.roadseg_id.isin(segs)]
        if drop_stale:
            t = t[~t.stale]
        six = pd.Index(sorted(segs))
        ei = self.eix.reindex(pd.MultiIndex.from_frame(w[["grid_from", "grid_to"]])).to_numpy()
        W = sparse.csr_matrix((w.w.to_numpy(), (ei, six.get_indexer(w.roadseg_id))), shape=(len(self.edges), len(six)))
        col = (pd.Series(t.date.to_numpy()).map({d: k for k, d in enumerate(DATES)}).to_numpy() * 144 + t.slot10.to_numpy())
        ri = six.get_indexer(t.roadseg_id)
        P = sparse.csr_matrix((t.pace_min_per_km.to_numpy(), (ri, col)), shape=(len(six), T))
        O = sparse.csr_matrix((np.ones(len(t)), (ri, col)), shape=(len(six), T))
        wp, wo = (W @ P).toarray(), (W @ O).toarray()
        wt = np.asarray(W.sum(axis=1)).ravel()
        e = to_long(wo, self.edges[["grid_from", "grid_to"]], "w_obs", 1e-12, 144)
        r, c = np.nonzero(wo >= 1e-12)
        e["pace_min_per_km"] = wp[r, c] / wo[r, c]
        e["coverage"] = wo[r, c] / wt[r]
        return e[["grid_from", "grid_to", "date", "slot10", "pace_min_per_km", "coverage"]]

    def edge_panel(self, pace: pd.DataFrame, flow: pd.DataFrame) -> pd.DataFrame:
        k = ["grid_from", "grid_to", "date", "slot10"]
        t10 = pace.merge(flow, on=k, how="left").fillna({"len_flow_all": 0.0}).merge(self.z10, on=k, how="left").fillna({"z_routed": 0.0})
        t10["hour"] = t10.slot10 // 6
        h = t10[t10.coverage >= MIN_COVERAGE].groupby(["grid_from", "grid_to", "date", "hour"]).agg(
            pace=("pace_min_per_km", "mean"), flow=("len_flow_all", "sum"), zr_raw=("z_routed", "sum"), n_slots=("slot10", "size")).reset_index()
        h = h[(h.n_slots == 6) & h.hour.between(6, 21) & (h.flow >= 5) & (h.zr_raw > 0)].merge(self.edges, on=["grid_from", "grid_to"])
        h["edge"] = h.grid_from + ">" + h.grid_to
        h["y"], h["x"], h["zr"] = np.log(h.pace), np.log(h.flow / h.n_kl), np.log(h.zr_raw / h.n_kl)
        h["weekday"] = h.date.isin(WEEKDAYS)
        return h


def run_variant(D: Data, keep: np.ndarray | None, same_roads: bool = True, drop_stale: bool = False) -> dict:
    """keep None = baseline construction. Returns estimates and sample sizes at both levels."""
    if keep is None:
        hs = D.segment_panel(np.ones(len(D.m), bool))
        he = D.edge_panel(D.edge_pace(None), D.edge_flow(None))
    else:
        hs = D.segment_panel(keep, drop_stale)
        segs = set(D.m.roadseg_id[keep]) if same_roads else None
        he = D.edge_panel(D.edge_pace(segs, drop_stale), D.edge_flow(keep))
    return {"segment": fit(hs, "segment", ["segment", "edge"]), "edge": fit(he, "edge", ["edge"]),
            "sample": {"segment_hours": int(len(hs)), "segments": int(hs.roadseg_id.nunique()),
                       "segment_main_edges": int(hs.dropna(subset=["grid_from"]).edge.nunique()),
                       "edge_hours": int(len(he)), "edges": int(he.edge.nunique())}}


def stored_baseline() -> dict:
    e = json.loads((GATE5 / "od_iv/od_iv_hypercongestion_summary.json").read_text())["panel_hourly_routed_od_iv"]
    s = json.loads((SEGP / "lambda/segment_lambda_summary.json").read_text())
    ek = {"S1": "V1_edge_FE+date_x_hour_FE", "S2": "V2_edge_x_hour_FE+date_x_hour_FE (day-to-day incl. weekend)", "S3": "V4_weekday_peak_hours_edge_FE+date_x_hour_FE"}
    sk = {"S1": "S1_segment_FE+date_x_hour_FE", "S2": "S2_segment_x_hour_FE+date_x_hour_FE", "S3": "S3_weekday_peak_hours"}
    return {"edge": {k: {"OLS": e[v]["OLS"], "IV": e[v]["IV_routed_od"]} for k, v in ek.items()},
            "segment": {k: {"OLS": s[v]["OLS"], "IV": s[v]["IV_routed_od_edge"]} for k, v in sk.items()}}


def cells(res: dict) -> dict:
    return {(lev, k, est_): res[lev][k][est_]["b"] for lev in ["edge", "segment"] for k, _ in SPECS for est_ in ["OLS", "IV"]}


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-name", default="lambda_strict_match_v1")
    ap.add_argument("--output-root", default=str(ROOT / "outputs"))
    ap.add_argument("--cache-dir", default=None, help="optional dir to cache the RN count pass (not inside outputs)")
    args = ap.parse_args(argv)
    out = Path(args.output_root) / args.output_name
    if out.exists():
        raise SystemExit(f"{out} exists; choose a new --output-name")
    out.mkdir(parents=True)
    t0 = time.time()
    D = Data(Path(args.cache_dir) if args.cache_dir else None)
    print(f"data loaded {time.time() - t0:.0f}s", flush=True)
    summary: dict = {"inputs": {"rn_count_files": D.files, "rn_pair_class": str(CLASS.relative_to(ROOT)),
                                "segment_panel": str(SEGP.relative_to(ROOT)), "gate5_panel": str(GATE5.relative_to(ROOT))}}

    # ---- step 0: reproduce
    old = json.loads((SEGP / "segment_build_summary.json").read_text())
    fh0, share0 = D.segment_flow(np.ones(len(D.m), bool))
    sf = pd.read_parquet(SEGP / "segment_flow_hour.parquet").merge(fh0, on=["roadseg_id", "date", "hour"], how="outer", suffixes=("_old", "_new"))
    ef = pd.read_parquet(GATE5 / "rn_all/edge_flow_10min.parquet").merge(D.edge_flow(None), on=["grid_from", "grid_to", "date", "slot10"], how="outer", suffixes=("_old", "_new"))
    ms = D.meta.set_index("roadseg_id").matched_share_of_length.dropna()
    summary["step0_build_check"] = {
        "segment_flow_hour_rows_old_new": [int(sf.flow_old.notna().sum()), int(sf.flow_new.notna().sum())],
        "segment_flow_max_abs_diff": float((sf.flow_old - sf.flow_new).abs().max()),
        "edge_flow_10min_rows_old_new": [int(ef.len_flow_all_old.notna().sum()), int(ef.len_flow_all_new.notna().sum())],
        "edge_flow_max_abs_diff": float((ef.len_flow_all_old - ef.len_flow_all_new).abs().max()),
        "matched_share_max_abs_diff": float((ms - share0.reindex(ms.index)).abs().max()),
        "stored_segment_flow_hour_rows": old["segment_hour_rows_flow"],
    }
    print(json.dumps(summary["step0_build_check"], indent=1), flush=True)
    base = run_variant(D, None)
    stored = stored_baseline()
    rep = cells(base)
    diffs = {f"{l}|{k}|{e}": round(rep[(l, k, e)] - stored[l][k][e]["b"], 4) for (l, k, e) in rep}
    summary["step0_reproduction"] = {"max_abs_diff": max(abs(v) for v in diffs.values()), "diff_by_cell": diffs, "estimates": base}
    (out / "summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary["step0_reproduction"]["diff_by_cell"], indent=1), flush=True)
    if summary["step0_reproduction"]["max_abs_diff"] > TOL:
        raise SystemExit(f"baseline not reproduced within {TOL}; see {out / 'summary.json'}")

    # ---- strict filters
    seg = pd.read_parquet(bp.SEGMENTS, columns=["roadseg_id", "raw_geometry", "dir_deg_final", "road_class", "roadname"]).drop_duplicates("roadseg_id").reset_index(drop=True)
    q, chk = strict_flags(D.p, seg)
    q = q.set_index("rn_pair_id").reindex(D.m.rn_pair_id).reset_index()        # align with segment_rn_match rows
    summary["strict_check"] = chk
    vkm = q.vkm_wd.fillna(0).to_numpy()
    vkm_all = float(D.p.vkm_wd.fillna(0).sum())
    alone3, alone4 = overlap_drops(q, np.ones(len(q), bool))
    alone = {1: q.f1.to_numpy(), 2: q.f2.to_numpy(), 3: alone3, 4: alone4, 5: q.f5.to_numpy()}
    rules = {1: "parallel ambiguity: 2nd valid candidate within 5 m", 2: "break/bend: an endpoint > 20 m from matched segment",
             3: "parallel overlap > 20 m, no shared node, offsets differ > 8 m: farther dropped",
             4: "nested overlap > 20 m sharing a node: longer dropped", 5: "Baidu class 2 but nearest OSM minor"}
    keeps = {0: np.ones(len(q), bool)}
    k = keeps[0].copy()
    for s in range(1, 6):
        if s in (3, 4):
            d3, d4 = overlap_drops(q, k)
            drop = d3 if s == 3 else d4
        else:
            drop = alone[s] & k
        k = k & ~drop
        keeps[s] = k.copy()
    tm = vkm.sum()
    rows = []
    for s in range(0, 6):
        kk, prev = keeps[s], keeps[max(s - 1, 0)]
        rem = prev & ~kk
        segs_k = set(q.roadseg_id[kk])
        a_used = np.asarray((D.A_m[:, kk] != 0).sum(axis=1)).ravel()
        rows.append({"step": s, "rule": "original match" if s == 0 else rules[s],
                     "chords_removed": int(rem.sum()), "vkm_removed_share_of_matched": round(float(vkm[rem].sum() / tm), 4),
                     "segments_touched_by_removal": int(q.roadseg_id[rem].nunique()),
                     "chords_remaining": int(kk.sum()), "vkm_remaining_share_of_matched": round(float(vkm[kk].sum() / tm), 4),
                     "vkm_remaining_share_of_all_rn": round(float(vkm[kk].sum() / vkm_all), 4),
                     "segments_remaining": len(segs_k),
                     "edges_with_kept_chords": int((a_used > 0).sum()),
                     "edges_with_kept_segments": int(D.sw[D.sw.roadseg_id.isin(segs_k)].groupby(["grid_from", "grid_to"]).ngroups),
                     "standalone_chords": None if s == 0 else int(alone[s].sum()),
                     "standalone_vkm_share_of_matched": None if s == 0 else round(float(vkm[alone[s]].sum() / tm), 4),
                     "standalone_segments": None if s == 0 else int(q.roadseg_id[alone[s]].nunique())})
    steps = pd.DataFrame(rows)

    # ---- estimates by step
    variants = {"0_baseline": base}
    variants["0b_original_match_same_roads"] = run_variant(D, keeps[0])
    for s in range(1, 6):
        print(f"step {s} {time.time() - t0:.0f}s", flush=True)
        variants[f"{s}_cumulative"] = run_variant(D, keeps[s])
    for s in range(1, 6):
        variants[f"alone_{s}"] = run_variant(D, ~alone[s])
    variants["5_strict_plus_drop_stale_slots"] = run_variant(D, keeps[5], drop_stale=True)
    summary["variants"] = variants
    vname = {0: "0b_original_match_same_roads", **{s: f"{s}_cumulative" for s in range(1, 6)}}
    steps["estimation_sample_segments"] = [variants[vname[s]]["sample"]["segments"] for s in steps.step]
    steps["estimation_sample_edges"] = [variants[vname[s]]["sample"]["edges"] for s in steps.step]
    steps.to_csv(out / "strict_filter_steps.csv", index=False)

    # ---- comparison table
    strict = variants["5_cumulative"]
    tab = []
    for lev in ["edge", "segment"]:
        for key, lab in SPECS:
            for e in ["OLS", "IV"]:
                st, sb, rb = strict[lev][key][e], stored[lev][key][e], base[lev][key][e]
                tab.append({"level": lev, "spec": lab, "estimator": e,
                            "baseline_stored": sb["b"], "baseline_reproduced": rb["b"], "strict": st["b"], "strict_minus_baseline": round(st["b"] - sb["b"], 4),
                            "baseline_se_cluster_segment" if lev == "segment" else "baseline_se_cluster_edge": sb["se_cluster_edge"],
                            "baseline_se_cluster_gate5_edge_recomputed": rb["se_cluster_edge"],
                            "strict_se_cluster_segment": st.get("se_cluster_segment"), "strict_se_cluster_gate5_edge": st["se_cluster_edge"],
                            "strict_first_stage_F_cluster_segment": st.get("first_stage_F_cluster_segment"),
                            "strict_first_stage_F_cluster_gate5_edge": st.get("first_stage_F_cluster_edge"),
                            "strict_n": st["n"], "strict_n_clusters_gate5_edge": st["n_clusters_edge"], "strict_n_segments": st.get("n_clusters_segment"),
                            "strict_plus_drop_stale": variants["5_strict_plus_drop_stale_slots"][lev][key][e]["b"]})
    tab = pd.DataFrame(tab)
    tab.to_csv(out / "lambda_strict_vs_baseline.csv", index=False)
    byv = pd.DataFrame([{"variant": v, **{f"{l}|{k}|{e}": b for (l, k, e), b in cells(r).items()}, **r["sample"]} for v, r in variants.items()])
    byv.to_csv(out / "lambda_by_variant.csv", index=False)
    summary["rules"] = {"strict_steps": rules, "vkm": "weekday vehicle-km (passes x chord) from rn_pair_class.vkm_wd",
                        "segment_se": "se_cluster_segment = by Baidu segment (= stored field se_cluster_edge); se_cluster_edge = by largest-weight gate5 edge (segments without one are their own cluster)",
                        "hours": "06-22", "dates": DATES}
    (out / "summary.json").write_text(json.dumps(summary, indent=2))
    print(tab.to_string())
    print(f"done {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
