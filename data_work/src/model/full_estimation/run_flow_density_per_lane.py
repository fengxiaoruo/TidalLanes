"""
Per-lane flow-density relation (fundamental diagram) on Baidu speed segments
============================================================================

Question: does flow per lane, as a function of density per lane, look like the
cell-transmission object f(rho; L) = L * f_lane(rho / L) used by Ater, Ross,
Shany and Vasserman (NBER w35887), and what does it imply for a tidal lane?

Data (read only)
    speed   outputs/segment_speed_flow_rn202505_v1/segment_pace_10min.parquet
            (Baidu pace, min/km, 10-min slots, 2025-05-19..25); stale fills dropped
            with run_segment_lambda_robustness.flag_runs (>= 6 equal slots).
    flow    outputs/segment_flow_10min_rn202505_v1/segment_flow_10min.parquet
            (build_segment_flow_10min.py): chord-weighted mean Unicom RN valid
            transitions per 10 min = sample SIM passages, not vehicles, unexpanded.
            A slot with a pace value and no RN row is a zero count.
    lanes   road_class -> lanes {2: 6, 3: 4, 4: 2} (stage02 ROADTYPE_LANE_MAP).
            Imputed from road class; no observed lane count exists in the project.
    z       outputs/all_traffic_speed_panel_rn202505_gate5_v1/od_iv/edge_routed_od_flow_10min.parquet
            (routed commuting OD on the segment's main gate5 edge).

Units: v km/h; q = passes per hour (10-min count x 6), "U/h" with U = one Unicom
sample passage; rho = q / v in U/km; per lane = divided by imputed L. Only speeds
and speed ratios are in physical units; q and rho levels are an index.

Main estimator. Each 10-min slot obeys q = rho * v, so measurement error in q
moves a point along a ray of constant speed. Means of q/L and rho/L within bins of
observed speed are unaffected by mean-zero error in q, and under scale
heterogeneity of the lane-scaling form (q = s f(rho / s)) they trace s_bar * f.
Binning on rho (built from q) is shown as the naive comparison. A concave
Bernstein polynomial through the origin is fitted to both; Greenshields and
triangular fits are benchmarks. Segment-cluster Poisson bootstrap.

Output: data_work/outputs/<output-name>/ (refuses to overwrite)
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import scipy.sparse as sp
import shapely
from scipy.optimize import lsq_linear
from scipy.special import comb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from run_all_traffic_speed_panel_gate5 import ROOT, WEEKDAYS  # noqa: E402
from plot_congestion_technology_gate5 import BLUE, ORANGE, AQUA, INK, INK2, style  # noqa: E402
from run_segment_lambda_robustness import flag_runs  # noqa: E402
import run_segment_lambda as sl  # noqa: E402

SEG = ROOT / "outputs/segment_speed_flow_rn202505_v1"
FLOW10 = ROOT / "outputs/segment_flow_10min_rn202505_v1/segment_flow_10min.parquet"
GATE5 = ROOT / "outputs/all_traffic_speed_panel_rn202505_gate5_v1"
MASTER = ROOT / "outputs/raw_rebuild_validation_gridcost_harmonic_v1/data/raw_segment_master.parquet"
ELAST = ROOT / "outputs/congestion_floor_sensitivity_v1"
LANES = {2: 6.0, 3: 4.0, 4: 2.0}
CLASS_NAME = {2: "class 2 expressway (L=6)", 3: "class 3 arterial (L=4)", 4: "class 4 secondary (L=2)"}
COL = {2: ORANGE, 3: BLUE, 4: AQUA}
VBINS = np.r_[np.arange(2, 100.1, 2), np.inf]          # km/h
NRHO = 300                                             # fine rho bins for the naive fit
DEG = 6
MIN_BIN_N, MIN_BIN_SEG = 2000, 50
B_REPS = 200
WITHIN_REPS = 100
GREY = "#8d8b85"
FILL_KMH = [60 / 2.542373, 60 / 2.097902, 60 / 3.225806, 60 / 2.857143, 20.0]
DROP_FILL = True
XBINS = np.r_[np.arange(0.10, 1.201, 0.05), np.inf]   # speed / segment v90


# ----------------------------------------------------------------------------- data
def load(out: Path) -> tuple[pd.DataFrame, dict]:
    meta = pd.read_parquet(SEG / "segment_meta.parquet")
    fh = pd.read_parquet(SEG / "segment_flow_hour.parquet")
    mf = fh[fh.date.isin(WEEKDAYS) & fh.hour.between(6, 21)].groupby("roadseg_id").flow.mean()
    good = set(meta[meta.matched_share_of_length >= sl.MIN_MATCH_SHARE].roadseg_id) & set(mf[mf >= sl.MIN_MEAN_FLOW].index)
    t = pd.read_parquet(SEG / "segment_pace_10min.parquet")
    t = t[t.roadseg_id.isin(good)]
    t = flag_runs(t)
    info = {"segments_in_sample_rule": len(good), "pace_slots_sample_segments": int(len(t)),
            "stale_share_all_hours": round(float(t.stale.mean()), 4),
            "stale_share_06_22": round(float(t.stale[t.slot10.between(36, 131)].mean()), 4)}
    # Baidu default speeds: a handful of exact values carry ~19% of all slots (23.6, 28.6, 18.6,
    # 21.0, 20.0 km/h); runs shorter than RUN_MIN escape flag_runs, so they are dropped by value too.
    pr = t.pace_min_per_km.round(6)
    fill = pr.isin([round(60 / v, 6) for v in FILL_KMH])
    info["fill_value_share_all"] = round(float(fill.mean()), 4)
    info["fill_value_share_of_nonstale"] = round(float(fill[~t.stale].mean()), 4)
    keep = ~t.stale & (~fill if DROP_FILL else True)
    t = t[keep].drop(columns=["run_len", "stale", "is_seg_mode"])
    info["drop_fill_values"] = DROP_FILL
    f = pd.read_parquet(FLOW10)
    t = t.merge(f, on=["roadseg_id", "date", "slot10"], how="left")
    info["share_slots_zero_flow_filled"] = round(float(t.flow.isna().mean()), 4)
    t["flow"] = t.flow.fillna(0.0)
    two = t.roadseg_id.isin(f.loc[f.flow_B.notna(), "roadseg_id"].unique())
    t["flow_A"] = t.flow_A.fillna(0.0)
    t["flow_B"] = np.where(two, t.flow_B.fillna(0.0), np.nan)
    t = t.merge(meta[["roadseg_id", "road_class", "roadname", "seg_len_m"]], on="roadseg_id")
    t = t[t.road_class.isin(LANES)]
    t["L"] = t.road_class.map(LANES)
    t["hour"] = (t.slot10 // 6).astype("int16")
    t["v"] = 60.0 / t.pace_min_per_km
    t["q"] = 6.0 * t.flow                     # U per hour
    t["rho"] = t.q / t.v                      # U per km
    t["qL"], t["rhoL"] = t.q / t.L, t.rho / t.L
    t["qAL"] = 6.0 * t.flow_A / t.L
    t["qBL"] = 6.0 * t.flow_B / t.L
    t["weekday"] = t.date.isin(WEEKDAYS)
    t["vf_seg"] = t.groupby("roadseg_id").v.transform(lambda s: s.quantile(0.90))
    t["x"] = t.v / t.vf_seg
    # routed-OD demand on the segment's main gate5 edge
    sw = pd.read_parquet(GATE5 / "speed/segment_edge_weights.parquet").sort_values("w").drop_duplicates("roadseg_id", keep="last")
    z = pd.read_parquet(GATE5 / "od_iv/edge_routed_od_flow_10min.parquet")
    t = t.merge(sw[["roadseg_id", "grid_from", "grid_to"]], on="roadseg_id", how="left").merge(
        z, on=["grid_from", "grid_to", "date", "slot10"], how="left")
    t["seg_i"] = t.roadseg_id.astype("category").cat.codes.astype("int32")
    info.update({"rows_used": int(len(t)), "segments_used": int(t.roadseg_id.nunique()),
                 "segments_by_class": {int(k): int(v) for k, v in t.groupby("road_class").roadseg_id.nunique().items()},
                 "segments_with_split_sample": int(t.loc[t.flow_B.notna(), "roadseg_id"].nunique()),
                 "share_rows_with_z": round(float(t.z_routed.notna().mean()), 4),
                 "dates": sorted(int(d) for d in t.date.unique())})
    return t, info


def hourly(t: pd.DataFrame) -> pd.DataFrame:
    h = t.groupby(["roadseg_id", "seg_i", "road_class", "L", "date", "hour"], observed=True).agg(
        pace=("pace_min_per_km", "mean"), n=("slot10", "size"), flow=("flow", "sum"), z=("z_routed", "sum"),
        vf_seg=("vf_seg", "first")).reset_index()
    h = h[h.n >= 4].copy()
    h["v"] = 60.0 / h.pace
    h["q"] = h.flow * 6.0 / h.n              # per hour, scaled for missing stale slots
    h["rho"] = h.q / h.v
    h["qL"], h["rhoL"] = h.q / h.L, h.rho / h.L
    h["x"] = h.v / h.vf_seg
    return h


# ----------------------------------------------------------------------------- binning + bootstrap
def seg_bin_sums(d: pd.DataFrame, b: np.ndarray, nb: int, cols: list[str]) -> dict:
    """Sparse segment x bin matrices of counts and sums (for cluster bootstrap)."""
    ok = (b >= 0) & (b < nb)
    seg = d.seg_i.to_numpy()[ok]
    bb = b[ok]
    S = int(d.seg_i.max()) + 1
    m = {"n": sp.csr_matrix((np.ones(ok.sum()), (seg, bb)), shape=(S, nb))}
    for c in cols:
        m[c] = sp.csr_matrix((d[c].to_numpy()[ok], (seg, bb)), shape=(S, nb))
    return m


def agg(m: dict, w: np.ndarray | None = None) -> pd.DataFrame:
    f = (lambda M: np.asarray(M.sum(axis=0)).ravel()) if w is None else (lambda M: np.asarray(w @ M).ravel())
    n = f(m["n"])
    o = {"n": n}
    for c, M in m.items():
        if c != "n":
            with np.errstate(invalid="ignore", divide="ignore"):
                o[c] = f(M) / n
    o["nseg"] = np.asarray((m["n"] > 0).sum(axis=0)).ravel()
    return pd.DataFrame(o)


# ----------------------------------------------------------------------------- shape-constrained fit
def bern(t: np.ndarray, K: int) -> np.ndarray:
    t = np.clip(t, 0, 1)[:, None]
    k = np.arange(K + 1)[None, :]
    return comb(K, k) * t ** k * (1 - t) ** (K - k)


def fit_concave(x: np.ndarray, y: np.ndarray, w: np.ndarray, R: float, K: int = DEG) -> np.ndarray:
    """Weighted LS, Bernstein degree K on [0, R], b0 = 0, second differences <= 0 (concave)."""
    B = bern(x / R, K)[:, 1:]
    M = np.zeros((K, K))              # b_k = k d1 - sum_{i=2..k} (k-i+1) e_i
    for k in range(1, K + 1):
        M[k - 1, 0] = k
        for i in range(2, k + 1):
            M[k - 1, i - 1] = -(k - i + 1)
    sw = np.sqrt(w)
    lo = np.r_[-np.inf, np.zeros(K - 1)]
    r = lsq_linear((B @ M) * sw[:, None], y * sw, bounds=(lo, np.full(K, np.inf)), method="bvls")
    return np.r_[0.0, M @ r.x]


class Knots:
    """Piecewise-linear curve through (g, f), g[0] = 0; flat beyond the last knot."""
    def __init__(self, g, f):
        self.g, self.f = np.asarray(g, float), np.asarray(f, float)
        self.s = np.diff(self.f) / np.diff(self.g)


def fit_cpl(x: np.ndarray, y: np.ndarray, w: np.ndarray) -> Knots:
    """Concave piecewise-linear LS through the origin, knots at the data points (slopes non-increasing)."""
    o = np.argsort(x)
    x, y, w = x[o], y[o], w[o]
    dr = np.diff(np.r_[0.0, x])
    m = len(x)
    A = np.tril(np.ones((m, m))) * dr[None, :]        # f(x_j) = sum_{i<=j} s_i dr_i
    M = np.zeros((m, m))                               # s_i = s_1 - sum_{l=2..i} e_l
    M[:, 0] = 1.0
    for i in range(1, m):
        M[i, 1:i + 1] = -1.0
    sw = np.sqrt(w)
    r = lsq_linear((A @ M) * sw[:, None], y * sw, bounds=(np.r_[-np.inf, np.zeros(m - 1)], np.full(m, np.inf)), method="bvls")
    return Knots(np.r_[0.0, x], np.r_[0.0, A @ M @ r.x])


def bern_eval(b, x: np.ndarray, R: float) -> np.ndarray:
    if isinstance(b, Knots):
        return np.interp(x, b.g, b.f)
    return bern(x / R, len(b) - 1) @ b


def bern_deriv(b, x: np.ndarray, R: float) -> np.ndarray:
    if isinstance(b, Knots):
        i = np.clip(np.searchsorted(b.g, x, side="right") - 1, 0, len(b.s) - 1)
        return np.where(np.asarray(x) > b.g[-1], 0.0, b.s[i])
    K = len(b) - 1
    return K / R * (bern(x / R, K - 1) @ np.diff(b))


def summarize(b: np.ndarray, R: float) -> dict:
    g = np.linspace(0, R, 4001)
    f = bern_eval(b, g, R)
    k = int(np.argmax(f))
    vf = float(bern_deriv(b, np.array([0.0]), R)[0])
    rc, qc = float(g[k]), float(f[k])
    vc = qc / rc if rc > 0 else np.nan
    return {"vf_kmh": vf, "rho_c_per_lane": rc, "q_c_per_lane": qc, "v_c_kmh": vc,
            "x_c": vc / vf if vf > 0 else np.nan, "interior_max": bool(0 < k < len(g) - 1),
            "f_at_R_over_qc": float(f[-1] / qc) if qc > 0 else np.nan}


def greenshields(x, y, w):
    X = np.c_[x, -x ** 2] * np.sqrt(w)[:, None]
    a, b = np.linalg.lstsq(X, y * np.sqrt(w), rcond=None)[0]
    rc = a / (2 * b) if b > 0 else np.nan
    return {"vf_kmh": float(a), "rho_c_per_lane": float(rc), "q_c_per_lane": float(a * rc / 2) if b > 0 else np.nan,
            "v_c_kmh": float(a / 2), "x_c": 0.5}


def triangular(x, y, w, R):
    best = None
    for rc in np.linspace(0.05 * R, 0.95 * R, 91):
        a = x <= rc
        vf = np.sum(w[a] * x[a] * y[a]) / np.sum(w[a] * x[a] ** 2)
        qc = vf * rc
        bb = ~a
        if bb.sum() >= 2:
            # q = qc - wv (x - rc) on the congested side, wv >= 0
            dx = x[bb] - rc
            wv = max(0.0, np.sum(w[bb] * dx * (qc - y[bb])) / np.sum(w[bb] * dx ** 2))
        else:
            wv = 0.0
        pred = np.where(a, vf * x, qc - wv * (x - rc))
        ssr = float(np.sum(w * (y - pred) ** 2))
        if best is None or ssr < best[0]:
            best = (ssr, vf, rc, qc, wv)
    _, vf, rc, qc, wv = best
    return {"vf_kmh": float(vf), "rho_c_per_lane": float(rc), "q_c_per_lane": float(qc), "v_c_kmh": float(vf),
            "x_c": 1.0, "backward_wave_kmh": float(wv)}


# ----------------------------------------------------------------------------- curves per class
def class_curves(d: pd.DataFrame, rng: np.random.Generator, label: str) -> tuple[dict, dict]:
    """Speed-binned and rho-binned per-lane curves + fits + bootstrap, one road class (or pooled)."""
    vb = np.searchsorted(VBINS, d.v.to_numpy(), side="right") - 1
    nvb = len(VBINS) - 1
    mv = seg_bin_sums(d, vb, nvb, ["qL", "rhoL", "v"])
    R = float(d.rhoL.quantile(0.995))
    rb = np.minimum((d.rhoL.to_numpy() / R * NRHO).astype(int), NRHO)
    mr = seg_bin_sums(d, rb, NRHO, ["qL", "rhoL", "v"])
    S = mv["n"].shape[0]
    # segments that belong to this sample
    present = np.asarray(mv["n"].sum(axis=1)).ravel() > 0

    def one(w):
        a = agg(mv, w)
        a["v_lo"] = VBINS[:-1]
        keep = (a.n >= (MIN_BIN_N if w is None else 1)) & a.qL.notna()
        if w is None:
            keep &= a.nseg >= MIN_BIN_SEG
        pv = a[keep]
        bv = fit_concave(pv.rhoL.to_numpy(), pv.qL.to_numpy(), np.ones(len(pv)), R)
        r = agg(mr, w)
        pr = r[r.n > 0]
        br = fit_concave(pr.rhoL.to_numpy(), pr.qL.to_numpy(), pr.n.to_numpy(), R)
        return a, keep, bv, r, br

    a, keep, bv, r, br = one(None)
    keep_bins = keep.to_numpy()
    pv = a[keep_bins]
    res = {"R_rho_p995": R, "rows": int(len(d)), "segments": int(present.sum()),
           "speed_binned_bernstein": summarize(bv, R), "rho_binned_bernstein": summarize(br, R),
           "speed_binned_cpl": summarize(fit_cpl(pv.rhoL.to_numpy(), pv.qL.to_numpy(), np.ones(len(pv))), R),
           "speed_binned_greenshields": greenshields(pv.rhoL.to_numpy(), pv.qL.to_numpy(), np.ones(len(pv))),
           "speed_binned_triangular": triangular(pv.rhoL.to_numpy(), pv.qL.to_numpy(), np.ones(len(pv)), R),
           "speed_bin_with_max_mean_qL": {"v_lo": float(pv.v_lo[pv.qL.idxmax()]), "qL": float(pv.qL.max()),
                                          "rhoL": float(pv.rhoL[pv.qL.idxmax()])}}
    # bootstrap (segment clusters, Poisson weights), on bins kept in the point estimate
    reps, curves_v, curves_b = [], [], []
    grid = np.linspace(0, R, 61)
    for _ in range(B_REPS):
        w = rng.poisson(1.0, S).astype(float)
        av = agg(mv, w)
        pvb = av[keep_bins & av.qL.notna().to_numpy() & (av.n > 0).to_numpy()]
        b1 = fit_concave(pvb.rhoL.to_numpy(), pvb.qL.to_numpy(), np.ones(len(pvb)), R)
        s1 = summarize(b1, R)
        reps.append(s1)
        curves_v.append(av.qL.to_numpy())
        curves_b.append(bern_eval(b1, grid, R))
    rp = pd.DataFrame(reps)
    res["speed_binned_bernstein_boot_se"] = {k: float(rp[k].std()) for k in ["vf_kmh", "rho_c_per_lane", "q_c_per_lane", "v_c_kmh", "x_c"]}
    res["speed_binned_bernstein_boot_ci95"] = {k: [float(rp[k].quantile(.025)), float(rp[k].quantile(.975))]
                                               for k in ["vf_kmh", "rho_c_per_lane", "q_c_per_lane", "v_c_kmh", "x_c"]}
    store = {"label": label, "a": a, "keep": keep_bins, "bv": bv, "r": r, "br": br, "R": R, "grid": grid,
             "boot_bin_qL": np.array(curves_v), "boot_curve": np.array(curves_b), "boot_summ": rp, "mv": mv}
    return res, store


# ----------------------------------------------------------------------------- within-segment curve
def fe_bins(y: np.ndarray, seg: np.ndarray, b: np.ndarray, w: np.ndarray, nseg: int, nb: int, iters: int = 25) -> np.ndarray:
    """y = a_seg + c_bin + e by weighted alternating projections; returns c (bin effects)."""
    ws = np.bincount(seg, w, nseg)
    wb = np.bincount(b, w, nb)
    a = np.zeros(nseg)
    c = np.zeros(nb)
    for _ in range(iters):
        r = y - a[seg]
        with np.errstate(invalid="ignore", divide="ignore"):
            c = np.bincount(b, w * r, nb) / wb
        r = y - np.nan_to_num(c)[b]
        with np.errstate(invalid="ignore", divide="ignore"):
            a = np.nan_to_num(np.bincount(seg, w * r, nseg) / ws)
    return c, float(np.average(a[seg], weights=w))


def within_curve(d: pd.DataFrame, rng: np.random.Generator, reps: int = B_REPS) -> tuple[dict, dict]:
    """Speed-flow and flow-density shape inside segments. q is divided by the segment's mean flow,
    speed by its own v90 (x). Bin effects of x with segment FE remove scale, lane count and
    between-segment composition. Density in the same units: rho_n = q_n / x."""
    d = d[d.x < XBINS[-2]]
    b = np.searchsorted(XBINS, d.x.to_numpy(), side="right") - 1
    ok = b >= 0
    d, b = d[ok], b[ok]
    nb = len(XBINS) - 1
    seg = d.roadseg_id.astype("category").cat.codes.to_numpy()
    nseg = int(seg.max()) + 1
    y = (d.q / d.groupby("roadseg_id").q.transform("mean")).to_numpy()
    xm = np.bincount(b, d.x.to_numpy(), nb) / np.maximum(np.bincount(b, minlength=nb), 1)
    n = np.bincount(b, minlength=nb)
    nsg = pd.Series(seg).groupby(b).nunique().reindex(range(nb), fill_value=0).to_numpy()
    keep = (n >= MIN_BIN_N) & (nsg >= MIN_BIN_SEG)

    def curve(w):
        c, abar = fe_bins(y, seg, b, w, nseg, nb)
        qn = c + abar
        return qn, qn / xm

    def fit(qn, rn):
        k = keep & np.isfinite(qn)
        R = float(np.nanmax(rn[k]))
        bb = fit_cpl(rn[k], qn[k], np.ones(k.sum()))
        return bb, R

    qn, rn = curve(np.ones(len(y)))
    bb, R = fit(qn, rn)
    s0 = summarize(bb, R)
    kk0 = keep & np.isfinite(qn)
    s_bern = summarize(fit_concave(rn[kk0], qn[kk0], np.ones(kk0.sum()), R, K=8), R)
    # empirical (unfitted) features
    kk = keep & np.isfinite(qn)
    kmax = int(np.nanargmax(np.where(kk, qn, np.nan)))
    low = int(np.where(kk)[0].min())
    emp = {"x_at_max_qn": float(xm[kmax]), "max_qn": float(qn[kmax]), "qn_at_lowest_x": float(qn[low]), "lowest_x": float(xm[low]),
           "drop_lowest_over_max": float(qn[low] / qn[kmax]), "qn_at_x_0.9_1.0": float(np.nanmean(qn[(xm >= 0.9) & (xm < 1.0) & kk]))}
    bq, bs, be = [], [], []
    for _ in range(reps):
        w = rng.poisson(1.0, nseg).astype(float)[seg]
        q1, r1 = curve(w)
        b1, R1 = fit(q1, r1)
        bq.append(q1)
        bs.append(summarize(b1, R1))
        k1 = kk & np.isfinite(q1)
        km1 = int(np.nanargmax(np.where(k1, q1, np.nan)))
        be.append({"x_at_max_qn": xm[km1], "drop_lowest_over_max": q1[low] / q1[km1]})
    bs, be, bq = pd.DataFrame(bs), pd.DataFrame(be), np.array(bq)
    res = {"rows": int(len(y)), "segments": nseg, "fit": s0, "fit_bernstein_deg8": s_bern, "x_c_v90_units": s0["v_c_kmh"],
           "x_c_v90_units_ci95": [float(bs.v_c_kmh.quantile(.025)), float(bs.v_c_kmh.quantile(.975))],
           "fit_ci95": {k: [float(bs[k].quantile(.025)), float(bs[k].quantile(.975))] for k in ["vf_kmh", "rho_c_per_lane", "q_c_per_lane", "x_c"]},
           "empirical": emp,
           "empirical_ci95": {k: [float(be[k].quantile(.025)), float(be[k].quantile(.975))] for k in be.columns},
           "units": "q_n = q / segment mean q; x = v / segment v90; rho_n = q_n / x; fit = concave piecewise-linear (equal bin weights); vf_kmh and v_c_kmh of the fit are in x units"}
    tab = pd.DataFrame({"x_lo": XBINS[:-1], "x_mean": xm, "n": n, "segments": nsg, "kept": keep, "qn": qn, "qn_se": np.nanstd(bq, axis=0),
                        "rho_n": rn, "qn_fit_at_rho_n": bern_eval(bb, np.nan_to_num(rn), R)})
    # local time-flow elasticity between adjacent kept bins: -dln x / dln q (t = 1/v)
    kt = tab[tab.kept].sort_values("x_mean", ascending=False)
    lx, lq = np.log(kt.x_mean.to_numpy()), np.log(kt.qn.to_numpy())
    eb = -(np.diff(lx)) / np.diff(lq)
    bl = np.log(np.array(bq)[:, kt.index.to_numpy()])
    ebs = np.nanstd(-(np.diff(lx))[None, :] / np.diff(bl, axis=1), axis=0)
    tab["local_elasticity_to_next_slower_bin"] = np.nan
    tab["local_elasticity_se"] = np.nan
    tab.loc[kt.index[:-1], "local_elasticity_to_next_slower_bin"] = eb
    tab.loc[kt.index[:-1], "local_elasticity_se"] = ebs
    return res, {"tab": tab, "b": bb, "R": R, "boot_qn": bq, "keep": keep}


# ----------------------------------------------------------------------------- lane scaling
def lane_scaling(st: dict, rng: np.random.Generator) -> tuple[dict, pd.DataFrame]:
    """Compare per-lane curves across road classes (= imputed lane counts)."""
    rows, out = [], {}
    # (a) at matched speed: mean rho/L and q/L within speed bins, class c minus class 3
    common = st[2]["keep"] & st[3]["keep"] & st[4]["keep"]
    for c in (2, 4):
        dq = st[c]["a"].qL.to_numpy() - st[3]["a"].qL.to_numpy()
        se = np.nanstd(st[c]["boot_bin_qL"] - st[3]["boot_bin_qL"], axis=0)
        for k in np.where(common)[0]:
            rows.append({"comparison": f"class {c} - class 3", "metric": "q/L at matched speed bin", "v_lo": VBINS[k],
                         "value_c": st[c]["a"].qL[k], "value_3": st[3]["a"].qL[k], "diff": dq[k], "se_boot": se[k],
                         "ratio": st[c]["a"].qL[k] / st[3]["a"].qL[k]})
    # (b) at matched per-lane density: fitted f_lane on a common grid
    Rm = min(st[c]["R"] for c in (2, 3, 4))
    g = np.linspace(0.1 * Rm, 0.9 * Rm, 5)
    curves, boots = {}, {}
    for c in (2, 3, 4):
        curves[c] = bern_eval(st[c]["bv"], g, st[c]["R"])
        boots[c] = np.array([np.interp(g, st[c]["grid"], bc) for bc in st[c]["boot_curve"]])
    for c in (2, 4):
        d = curves[c] - curves[3]
        Db = boots[c] - boots[3]
        V = np.cov(Db.T)
        wald = float(d @ np.linalg.pinv(V) @ d)
        for j, x in enumerate(g):
            rows.append({"comparison": f"class {c} - class 3", "metric": "fitted f_lane at matched rho/L", "rhoL": x,
                         "value_c": curves[c][j], "value_3": curves[3][j], "diff": d[j], "se_boot": float(Db[:, j].std()),
                         "ratio": curves[c][j] / curves[3][j]})
        out[f"wald_equal_f_lane_class{c}_vs_3_5pts"] = {"stat": round(wald, 1), "df": len(g),
                                                        "note": "bootstrap covariance; chi2(5) 5% critical value 11.07"}
    # (c) capacity per lane and implied relative lanes
    cap = {c: st[c]["boot_summ"].q_c_per_lane.to_numpy() for c in (2, 3, 4)}
    cap0 = {c: summarize(st[c]["bv"], st[c]["R"])["q_c_per_lane"] for c in (2, 3, 4)}
    for c in (2, 4):
        ratio = cap0[c] / cap0[3]
        rb = cap[c] / cap[3]
        out[f"capacity_per_lane_ratio_class{c}_over_3"] = {"ratio": round(ratio, 3), "ci95": [round(float(np.quantile(rb, .025)), 3), round(float(np.quantile(rb, .975)), 3)],
                                                          "equal_under_lane_scaling": 1.0}
        out[f"implied_effective_lanes_class{c}_if_class3_has_4"] = {
            "point": round(4 * LANES[c] / 4 * ratio, 2), "imputed": LANES[c],
            "note": "direction capacity ratio x 4; = imputed L only if per-lane capacity is equal"}
    out["matched_rho_grid"] = [round(float(x), 2) for x in g]
    return out, pd.DataFrame(rows)


# ----------------------------------------------------------------------------- noise checks
def z_check(d: pd.DataFrame) -> pd.DataFrame:
    z = d[d.z_routed > 0].copy()
    z["zrel"] = z.z_routed / z.groupby("roadseg_id").z_routed.transform(lambda s: s.quantile(0.95))
    z["zbin"] = pd.cut(z.zrel, [0, .1, .2, .3, .4, .5, .6, .7, .8, .9, 1.0, 1.2, np.inf])
    g = z.groupby(["road_class", "zbin"], observed=True).agg(
        n=("v", "size"), zrel=("zrel", "median"), v_mean=("v", "mean"), x_mean=("x", "mean"),
        qL_mean=("qL", "mean"), rhoL_mean=("rhoL", "mean"), share_hour_7_8=("hour", lambda s: float(s.isin([7, 8]).mean())))
    return g.reset_index()


def split_check(d: pd.DataFrame, st: dict) -> tuple[pd.DataFrame, dict]:
    s = d[d.flow_B.notna()].copy()
    s["rhoAL"] = s.qAL / s.v
    rows, info = [], {}
    for c, g in s.groupby("road_class"):
        R = st[c]["R"]
        g = g.assign(bin=np.minimum((g.rhoAL / R * 20).astype(int), 20))
        a = g.groupby("bin").agg(n=("v", "size"), rhoAL=("rhoAL", "mean"), qAL=("qAL", "mean"), qBL=("qBL", "mean"), v=("v", "mean"))
        a["road_class"] = c
        rows.append(a.reset_index())
        # reliability of the 10-min count: within-segment correlation of A and B
        aa = g.qAL - g.groupby("roadseg_id").qAL.transform("mean")
        bb = g.qBL - g.groupby("roadseg_id").qBL.transform("mean")
        info[int(c)] = {"rows": int(len(g)), "segments": int(g.roadseg_id.nunique()),
                        "corr_within_segment_A_B": round(float(np.corrcoef(aa, bb)[0, 1]), 3),
                        "mean_qAL_over_qBL": round(float(g.qAL.mean() / g.qBL.mean()), 3)}
    return pd.concat(rows), info


# ----------------------------------------------------------------------------- elasticity
def elasticity_curve(b: np.ndarray, R: float) -> pd.DataFrame:
    s = summarize(b, R)
    g = np.linspace(R / 2000, s["rho_c_per_lane"], 2000)[:-1]
    f = bern_eval(b, g, R)
    fp = bern_deriv(b, g, R)
    v = f / g
    return pd.DataFrame({"rhoL": g, "qL": f, "v": v, "x": v / s["vf_kmh"], "elasticity": v / fp - 1})


def elasticity_compare(h: pd.DataFrame, st: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    """st: per class dict with 'bv' (Bernstein coefs) and 'R'; here the within-segment fits."""
    curves = []
    for c in (2, 3, 4):
        e = elasticity_curve(st[c]["bv"], st[c]["R"])
        e["x"] = e["v"]            # within-segment units: speed / segment v90
        e["road_class"] = c
        curves.append(e)
    ec = pd.concat(curves)
    tab = []
    xs = [0.98, 0.95, 0.9, 0.85, 0.8, 0.75, 0.7, 0.65, 0.6]
    for c in (2, 3, 4):
        e = ec[ec.road_class == c].sort_values("x")
        for x in xs:
            tab.append({"road_class": c, "x": x, "elasticity": float(np.interp(x, e.x, e.elasticity)) if x >= e.x.min() else np.nan})
    xtab = pd.DataFrame(tab)
    # project samples (segment hours 06-22, all 7 days, as run_congestion_floor_sensitivity)
    w = h[h.hour.between(6, 21)].copy()
    zz = w.z.where(w.z > 0)
    for p in (0.5, 0.7, 0.85):
        w[f"zp{int(p * 100)}"] = zz > zz.groupby(w.roadseg_id).transform(lambda s, p=p: s.quantile(p))
    samples = {"all hours in window": np.ones(len(w), bool), "z > unit p50": w.zp50, "z > unit p70": w.zp70,
               "z > unit p85": w.zp85, "hours 07-09": w.hour.isin([7, 8])}
    proj = {}
    for lev in ("edge", "segment"):
        p = pd.read_csv(ELAST / f"5_local_linear_iv_{lev}.csv")
        p = p[(p.sample_window == "06-22h") & (p.spec == "IV ln q by ln z, unit FE + date x hour FE")]
        proj[lev] = p.set_index("selection").beta
    rows = []
    for lab, m in samples.items():
        s = w[m]
        for c in (2, 3, 4):
            e = ec[ec.road_class == c].sort_values("x")
            xc = summarize(st[c]["bv"], st[c]["R"])["v_c_kmh"]
            sc = s[s.road_class == c]
            xi = sc.x.clip(upper=1.0).to_numpy()
            rising = xi > xc
            ei = np.interp(xi[rising], e.x, e.elasticity)
            rows.append({"sample": lab, "road_class": c, "segment_hours": int(len(sc)), "x_median": float(np.median(xi)),
                         "elasticity_at_median_x": float(np.interp(np.median(xi), e.x, e.elasticity)) if np.median(xi) > xc else np.nan,
                         "elasticity_mean_rising_branch": float(np.mean(ei)), "elasticity_median_rising_branch": float(np.median(ei)),
                         "share_beyond_capacity_x_below_xc": float(1 - rising.mean()), "x_c": xc,
                         "project_beta_edge": float(proj["edge"].get(lab, np.nan)),
                         "project_beta_segment": float(proj["segment"].get(lab, np.nan))})
    return xtab, pd.DataFrame(rows), ec


# ----------------------------------------------------------------------------- tidal back-of-envelope
def pair_segments(segs: set) -> pd.DataFrame:
    s = pd.read_parquet(MASTER, columns=["roadseg_id", "raw_geometry", "dir_deg_final", "road_class", "roadname"]).drop_duplicates("roadseg_id")
    s = s[s.roadseg_id.isin(segs)].reset_index(drop=True)
    g = shapely.from_wkb(s.raw_geometry.values)
    mid = shapely.line_interpolate_point(g, 0.5, normalized=True)
    i, j = shapely.STRtree(g).query(mid, predicate="dwithin", distance=80)
    d = pd.DataFrame({"i": i, "j": j})
    d = d[(d.i < d.j)]
    d = d[(s.roadname.values[d.i] == s.roadname.values[d.j]) & (s.road_class.values[d.i] == s.road_class.values[d.j])]
    ad = np.abs((s.dir_deg_final.to_numpy(float)[d.i] - s.dir_deg_final.to_numpy(float)[d.j] + 180) % 360 - 180)
    d = d[ad >= 135].copy()
    d["dist_m"] = shapely.distance(mid[d.i.values], g[d.j.values])
    d = d.sort_values("dist_m").drop_duplicates("i").drop_duplicates("j")
    return pd.DataFrame({"seg_a": s.roadseg_id.values[d.i], "seg_b": s.roadseg_id.values[d.j], "road_class": s.road_class.values[d.i],
                         "roadname": s.roadname.values[d.i], "dist_m": d.dist_m.values})


def rho_from_x(b, R, x, vf):
    """Point on the fitted curve with speed x * vf (speed falls monotonically in rho)."""
    g = np.linspace(R / 4000, R, 4000)
    v = bern_eval(b, g, R) / g
    k = int(np.argmin(np.abs(v - x * vf)))
    return g[k]


def free_branch_rho(b, R, q):
    s = summarize(b, R)
    g = np.linspace(0, s["rho_c_per_lane"], 4000)
    f = bern_eval(b, g, R)
    if q > f.max():
        return np.nan
    return float(np.interp(q, f, g))


def tidal(d: pd.DataFrame, st: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    am = d[d.weekday & d.hour.isin([7, 8])]
    a = am.groupby("roadseg_id").agg(n=("v", "size"), pace=("pace_min_per_km", "mean"), qL=("qL", "mean"),
                                     vf_seg=("vf_seg", "first"), L=("L", "first"))
    a["v"] = 60.0 / a.pace
    a["x"] = (a.v / a.vf_seg).clip(upper=1.0)
    a = a[a.n >= 30]
    p = pair_segments(set(a.index))
    p = p[p.seg_a.isin(a.index) & p.seg_b.isin(a.index)]
    for s_ in ("a", "b"):
        p = p.join(a[["x", "v", "qL", "L"]].add_suffix(f"_{s_}"), on=f"seg_{s_}")
    heavy_a = p.x_a < p.x_b
    p["x_heavy"] = np.where(heavy_a, p.x_a, p.x_b)
    p["x_light"] = np.where(heavy_a, p.x_b, p.x_a)
    p["seg_heavy"] = np.where(heavy_a, p.seg_a, p.seg_b)
    p["seg_light"] = np.where(heavy_a, p.seg_b, p.seg_a)
    p["qL_obs_ratio_heavy_light"] = np.where(heavy_a, p.qL_a / p.qL_b, p.qL_b / p.qL_a)
    p["L"] = p.L_a
    p["gap"] = p.x_light - p.x_heavy

    def move(r):
        c = int(r.road_class)
        b, R = st[c]["bv"], st[c]["R"]
        sm = summarize(b, R)
        vf, L = 1.0, r.L          # within-segment units: x = v / v90
        o = {}
        for side, x, dL in (("heavy", r.x_heavy, +1), ("light", r.x_light, -1)):
            rho0 = rho_from_x(b, R, x, vf)
            q0 = float(bern_eval(b, np.array([rho0]), R)[0])
            q1 = q0 * L / (L + dL)
            rho1 = free_branch_rho(b, R, q1)
            v0, v1 = x * vf, (q1 / rho1 if rho1 and rho1 > 0 else np.nan)
            o.update({f"{side}_on_falling_branch_before": bool(rho0 > sm["rho_c_per_lane"]),
                      f"{side}_qL_before_over_cap": q0 / sm["q_c_per_lane"], f"{side}_qL_after_over_cap": q1 / sm["q_c_per_lane"],
                      f"{side}_x_before": x, f"{side}_x_after": v1 / vf if np.isfinite(v1) else np.nan,
                      f"{side}_speed_change_pct": 100 * (v1 / v0 - 1) if np.isfinite(v1) else np.nan})
        # flow-weighted travel time per km, both directions, relative (index scale cancels within class)
        qh, ql = o["heavy_qL_before_over_cap"], o["light_qL_before_over_cap"]
        if np.isfinite(o["heavy_x_after"]) and np.isfinite(o["light_x_after"]):
            t0 = qh / r.x_heavy + ql / r.x_light
            t1 = qh / o["heavy_x_after"] + ql / o["light_x_after"]
            o["total_time_change_pct"] = 100 * (t1 / t0 - 1)
        else:
            o["total_time_change_pct"] = np.nan
        return pd.Series(o)

    p = p.join(p.apply(move, axis=1))
    top = p[p.L >= 4].sort_values("gap", ascending=False)
    rep = []
    for c in (2, 3, 4):
        q = p[(p.road_class == c)]
        if len(q) < 20:
            continue
        for grp, qq in (("top decile of speed-ratio gap", 0.9), ("top quartile of speed-ratio gap", 0.75)):
            hi = q[q.gap >= q.gap.quantile(qq)]
            rep.append({"road_class": c, "group": grp, "pairs": int(len(q)), "pairs_in_group": int(len(hi)),
                        "median_gap": float(hi.gap.median()), "share_x_heavy_below_0.2": float((hi.x_heavy < 0.2).mean()),
                        "median_x_heavy": float(hi.x_heavy.median()), "median_x_light": float(hi.x_light.median()),
                        "median_heavy_speed_change_pct": float(hi.heavy_speed_change_pct.median()),
                        "median_light_speed_change_pct": float(hi.light_speed_change_pct.median()),
                        "share_light_over_capacity_after": float(hi.light_speed_change_pct.isna().mean()),
                        "median_total_time_change_pct": float(hi.total_time_change_pct.median()),
                        "share_heavy_on_falling_branch": float(hi.heavy_on_falling_branch_before.mean())})
    # transparent grid: hypothetical heavy/light states, observed lane counts, flows held fixed
    g = pd.DataFrame([{"road_class": c, "L": LANES[c], "x_heavy": xh, "x_light": xl}
                      for c in (2, 3, 4) for xh in (0.9, 0.8, 0.7, 0.6, 0.5, 0.4) for xl in (0.95, 0.9)])
    g = g.join(g.apply(move, axis=1))
    return p, pd.DataFrame(rep), top, g


# ----------------------------------------------------------------------------- figures
def fig_within(wst: dict, wres: dict, out: Path) -> None:
    fig, ax = plt.subplots(1, 2, figsize=(13, 4.8))
    for c in (2, 3, 4):
        t = wst[c]["tab"]
        t = t[t.kept]
        ax[0].errorbar(t.x_mean, t.qn, yerr=1.96 * t.qn_se, fmt="o-", color=COL[c], ms=3.5, lw=1.4, capsize=0, label=CLASS_NAME[c])
        ax[1].plot(t.rho_n, t.qn, "o", color=COL[c], ms=3.5)
        g = np.linspace(0, wst[c]["R"], 300)
        sm = wres["10min"][c]["fit"]
        ax[1].plot(g, bern_eval(wst[c]["bv"], g, wst[c]["R"]), color=COL[c], lw=2, label=f"class {c}: capacity at v/v90 = {sm['v_c_kmh']:.2f}")
    ax[0].set(xlabel="speed / segment's own v90", ylabel="flow / segment's mean flow (segment FE)")
    ax[0].invert_xaxis()
    ax[0].set_title("A  Within-segment speed-flow (left = free, right = slow)", fontsize=10.5, color=INK, loc="left")
    ax[1].set(xlabel="density, same units: (q / mean q) / (v / v90)", ylabel="flow / segment's mean flow")
    ax[1].set_title("B  Within-segment flow-density with concave fit", fontsize=10.5, color=INK, loc="left")
    for a in ax:
        style(a)
        a.legend(frameon=False, fontsize=8)
    fig.text(0.01, 0.005, "Segment x 10-min, stale and default-value pace dropped. Bin effects with segment fixed effects; 95% segment-cluster bootstrap bars. "
             "No lane count or flow scale enters.", fontsize=8.5, color=INK2)
    fig.tight_layout(rect=(0, 0.03, 1, 1))
    fig.savefig(out / "fig0_within_segment_shape.png", dpi=180)
    fig.savefig(out / "fig0_within_segment_shape.pdf")
    plt.close(fig)


def fig_cloud(d: pd.DataFrame, st: dict, out: Path) -> None:
    fig, ax = plt.subplots(1, 3, figsize=(16, 5.0))
    for a, c in zip(ax, (2, 3, 4)):
        s = st[c]
        dd = d[d.road_class == c]
        a.hexbin(dd.rhoL, dd.qL, gridsize=70, extent=(0, s["R"], 0, dd.qL.quantile(0.995)), bins="log", cmap="Greys", mincnt=5, linewidths=0)
        pv = s["a"][s["keep"]]
        a.plot(pv.rhoL, pv.qL, "o", color=COL[c], ms=4, label="mean within speed bin (robust to noise in q)")
        rr = s["r"][s["r"].n >= 500]
        a.plot(rr.rhoL, rr.qL, "-", color=INK2, lw=1, label="mean within rho bin (naive)")
        g = np.linspace(0, s["R"], 300)
        a.plot(g, bern_eval(s["bv"], g, s["R"]), color=COL[c], lw=2.4, label="concave Bernstein fit (speed bins)")
        lo, hi = np.quantile(s["boot_curve"], [0.025, 0.975], axis=0)
        a.fill_between(s["grid"], lo, hi, color=COL[c], alpha=0.25, lw=0)
        sm = summarize(s["bv"], s["R"])
        a.axvline(sm["rho_c_per_lane"], color=INK2, ls=":", lw=0.8)
        a.set(xlabel="density per lane, rho/L  (U/km/lane)", ylabel="flow per lane, q/L  (U/h/lane)", xlim=(0, 1.05 * pv.rhoL.max()))
        a.set_title(f"{CLASS_NAME[c]}\nvf {sm['vf_kmh']:.0f} km/h, speed at capacity {sm['v_c_kmh']:.0f} km/h", fontsize=11, color=INK, loc="left")
        style(a)
    ax[0].legend(frameon=False, fontsize=8, loc="upper right")
    fig.text(0.01, 0.005, "Segment x 10-min, 2025-05-19..25, stale pace fills dropped. U = one Unicom sample passage (index, not vehicles). "
             "L imputed from road class. Grey: density of observations (log). Band: 95% segment-cluster bootstrap.", fontsize=8.5, color=INK2)
    fig.tight_layout(rect=(0, 0.03, 1, 1))
    fig.savefig(out / "fig1_per_lane_flow_density_by_class.png", dpi=180)
    fig.savefig(out / "fig1_per_lane_flow_density_by_class.pdf")
    plt.close(fig)


def fig_lanes(st: dict, out: Path) -> None:
    fig, ax = plt.subplots(1, 3, figsize=(16, 4.8))
    for c in (2, 3, 4):
        s = st[c]
        pv = s["a"][s["keep"]]
        g = np.linspace(0, s["R"], 300)
        ax[0].plot(g, bern_eval(s["bv"], g, s["R"]), color=COL[c], lw=2.2, label=CLASS_NAME[c])
        lo, hi = np.quantile(s["boot_curve"], [0.025, 0.975], axis=0)
        ax[0].fill_between(s["grid"], lo, hi, color=COL[c], alpha=0.2, lw=0)
        ax[1].plot(pv.rhoL, pv.v, "o-", color=COL[c], ms=3, lw=1.4, label=CLASS_NAME[c])
        ax[2].plot(pv.qL * LANES[c], pv.v, "o-", color=COL[c], ms=3, lw=1.4, label=CLASS_NAME[c])
    ax[0].set(xlabel="rho/L (U/km/lane)", ylabel="q/L (U/h/lane)")
    ax[0].set_title("A  Fitted per-lane curves: collapse expected under lane scaling", fontsize=10.5, color=INK, loc="left")
    ax[1].set(xlabel="rho/L (U/km/lane), mean within speed bin", ylabel="speed (km/h)")
    ax[1].set_title("B  Speed against per-lane density", fontsize=10.5, color=INK, loc="left")
    ax[2].set(xlabel="q per direction (U/h), mean within speed bin", ylabel="speed (km/h)")
    ax[2].set_title("C  Speed against flow (direction total)", fontsize=10.5, color=INK, loc="left")
    for a in ax:
        style(a)
        a.legend(frameon=False, fontsize=8)
    fig.text(0.01, 0.005, "Lane count = road class (6/4/2): differences across curves cannot be split into lane, road-design, and flow-scale effects.", fontsize=8.5, color=INK2)
    fig.tight_layout(rect=(0, 0.03, 1, 1))
    fig.savefig(out / "fig2_curves_by_lane_count_and_speed_density.png", dpi=180)
    fig.savefig(out / "fig2_curves_by_lane_count_and_speed_density.pdf")
    plt.close(fig)


def fig_checks(wst: dict, st: dict, sth: dict, zt: pd.DataFrame, spl: pd.DataFrame, fb: pd.DataFrame, ec: pd.DataFrame, cmp_: pd.DataFrame, out: Path) -> None:
    fig, ax = plt.subplots(2, 3, figsize=(16, 9.2))
    a = ax[0, 0]
    for c in (2, 3, 4):
        z = zt[(zt.road_class == c) & (zt.n >= 500)]
        a.plot(z.rhoL_mean, z.qL_mean, "o-", color=COL[c], ms=4, label=f"{CLASS_NAME[c]}: bins of routed demand z")
        pv = st[c]["a"][st[c]["keep"]]
        a.plot(pv.rhoL, pv.qL, ":", color=COL[c], lw=1)
    a.set(xlabel="mean rho/L", ylabel="mean q/L")
    a.set_title("A  Traced by routed-OD demand bins (dotted: speed bins)", fontsize=10, color=INK, loc="left")
    a.legend(frameon=False, fontsize=7.5)
    a = ax[0, 1]
    for c in (2, 3, 4):
        s = spl[(spl.road_class == c) & (spl.n >= 500)]
        a.plot(s.rhoAL, s.qAL, "-", color=COL[c], lw=1.2)
        a.plot(s.rhoAL, s.qBL, "o--", color=COL[c], ms=3.5, lw=1.2, label=f"class {c}: q_B/L on bins of rho_A/L")
    a.set(xlabel="rho_A/L (half A of RN pairs)", ylabel="mean q/L (solid: half A, dashed: half B)")
    a.set_title("B  Split sample: x from half A, y from half B", fontsize=10, color=INK, loc="left")
    a.legend(frameon=False, fontsize=7.5)
    a = ax[0, 2]
    for c in (2, 3, 4):
        p10 = st[c]["a"][st[c]["keep"]]
        ph = sth[c]["a"][sth[c]["keep"]]
        a.plot(p10.rhoL, p10.qL, "o-", color=COL[c], ms=3, lw=1.2, label=f"class {c}, 10-min")
        a.plot(ph.rhoL, ph.qL, "s--", color=COL[c], ms=3, lw=1.0, alpha=0.7, label=f"class {c}, hourly")
    a.set(xlabel="rho/L", ylabel="q/L (U/h/lane)")
    a.set_title("C  10-minute against hourly (speed bins)", fontsize=10, color=INK, loc="left")
    a.legend(frameon=False, fontsize=7.5, ncol=2)
    a = ax[1, 0]
    for c in (2, 3, 4):
        f = fb[fb.road_class == c]
        a.plot(f.hour, 100 * f.share_below_xc, "o-", color=COL[c], ms=3, label=CLASS_NAME[c])
    a.set(xlabel="hour (weekdays)", ylabel="% of segment-slots with v/v90 below x_c")
    a.set_title("D  Share on the falling branch, by hour", fontsize=10, color=INK, loc="left")
    a.legend(frameon=False, fontsize=8)
    a = ax[1, 1]
    for c in (2, 3, 4):
        t = wst[c]["tab"]
        t = t[t.kept & t.local_elasticity_to_next_slower_bin.notna() & (t.x_mean > 0.5) & (t.x_mean < 1.05)]
        a.errorbar(t.x_mean - 0.025, t.local_elasticity_to_next_slower_bin, yerr=1.96 * t.local_elasticity_se.fillna(0),
                   fmt="o-", color=COL[c], ms=4, lw=1.6, capsize=0, label=CLASS_NAME[c])
    for lab, val, ls in (("all 06-22h", 0.16, "-"), ("z>p50", 0.29, "--"), ("z>p70", 0.35, "--"), ("z>p85", 0.44, "--"), ("07-09h", 0.42, ":")):
        a.axhline(val, color=GREY, lw=0.9, ls=ls)
        a.text(1.0, val, f" {lab} {val}", fontsize=7.5, color=INK2, va="center")
    a.set(xlabel="speed / segment v90 (midpoint of adjacent bins)", ylabel="local d ln t / d ln q, within segment", ylim=(0, 2), xlim=(0.5, 1.02))
    a.invert_xaxis()
    a.set_title("E  Implied time-flow elasticity vs project IV estimates", fontsize=10, color=INK, loc="left")
    a.legend(frameon=False, fontsize=8, loc="upper left")
    a = ax[1, 2]
    for c in (2, 3, 4):
        s = cmp_[cmp_.road_class == c]
        a.plot(s.x_median, s.elasticity_mean_rising_branch, "o", color=COL[c], ms=6, label=f"class {c}: mean implied over rising-branch hours")
        a.plot(s.x_median, s.project_beta_segment, "x", color=COL[c], ms=7)
    a.set(xlabel="median speed / segment v90 in the sample", ylabel="elasticity")
    a.set_title("F  Implied (dots) vs segment IV estimate (x) by sample", fontsize=10, color=INK, loc="left")
    a.legend(frameon=False, fontsize=7.5)
    for a in ax.flat:
        style(a)
    fig.tight_layout()
    fig.savefig(out / "fig3_noise_checks_falling_branch_elasticity.png", dpi=180)
    fig.savefig(out / "fig3_noise_checks_falling_branch_elasticity.pdf")
    plt.close(fig)


# ----------------------------------------------------------------------------- main
def jsonable(o):
    if isinstance(o, dict):
        return {str(k): jsonable(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [jsonable(v) for v in o]
    if isinstance(o, (np.floating, float)):
        return None if not np.isfinite(o) else round(float(o), 4)
    if isinstance(o, (np.integer,)):
        return int(o)
    if isinstance(o, np.bool_):
        return bool(o)
    return o


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-name", default="flow_density_per_lane_v1")
    ap.add_argument("--seed", type=int, default=20250519)
    ap.add_argument("--keep-fill-values", action="store_true", help="sensitivity: keep Baidu default-speed values in short runs")
    ap.add_argument("--boot-reps", type=int, default=200, help="pooled-curve bootstrap reps; within-segment uses half")
    args = ap.parse_args(argv)
    global DROP_FILL, B_REPS, WITHIN_REPS
    DROP_FILL = not args.keep_fill_values
    B_REPS, WITHIN_REPS = args.boot_reps, max(args.boot_reps // 2, 10)
    out = ROOT / "outputs" / args.output_name
    if out.exists():
        raise SystemExit(f"{out} exists; choose a new --output-name")
    out.mkdir(parents=True)
    rng = np.random.default_rng(args.seed)
    t0 = time.time()
    res = {"units": {"v": "km/h (60 / Baidu pace)", "q": "U/h, U = one Unicom RN valid transition (sample SIM passage), chord-weighted mean over the segment's RN pairs; not vehicles, not expanded",
                     "rho": "U/km = q / v", "L": "imputed from road class {2: 6, 3: 4, 4: 2}; not observed",
                     "identified_in_physical_units": "speeds, speed ratios, elasticities, shapes; not capacity levels"}}
    d, res["data"] = load(out)
    print(json.dumps(res["data"], indent=1), f"{time.time() - t0:.0f}s", flush=True)

    # descriptive moments of the 10-min count
    res["data"]["flow_10min_count_quantiles_by_class"] = {int(c): [round(float(x), 2) for x in (g.q / 6).quantile([.1, .5, .9, .99])]
                                                         for c, g in d.groupby("road_class")}

    st, res["fits_10min"] = {}, {}
    for c in (2, 3, 4):
        r, st[c] = class_curves(d[d.road_class == c], rng, f"class {c}")
        res["fits_10min"][c] = r
        print("class", c, json.dumps(jsonable(r["speed_binned_bernstein"])), f"{time.time() - t0:.0f}s", flush=True)
    # hourly check
    h = hourly(d)
    sth, res["fits_hourly"] = {}, {}
    for c in (2, 3, 4):
        r, sth[c] = class_curves(h[h.road_class == c], rng, f"class {c} hourly")
        res["fits_hourly"][c] = {k: r[k] for k in ("rows", "segments", "speed_binned_bernstein", "rho_binned_bernstein", "speed_binned_bernstein_boot_se")}
    print("hourly done", f"{time.time() - t0:.0f}s", flush=True)

    # tables of binned curves
    tabs = []
    for lab, S in (("10min", st), ("hourly", sth)):
        for c in (2, 3, 4):
            a = S[c]["a"].assign(v_lo=VBINS[:-1], v_hi=VBINS[1:], kept=S[c]["keep"], road_class=c, binning="speed", level=lab)
            a["qL_se_boot"] = np.nanstd(S[c]["boot_bin_qL"], axis=0)
            tabs.append(a)
            r = S[c]["r"].assign(road_class=c, binning="rho", level=lab)
            tabs.append(r[r.n > 0])
    pd.concat(tabs).to_csv(out / "1_binned_curves.csv", index=False)
    fitrows = []
    for lab, F in (("10min", res["fits_10min"]), ("hourly", res["fits_hourly"])):
        for c, r in F.items():
            for m in ("speed_binned_bernstein", "rho_binned_bernstein", "speed_binned_greenshields", "speed_binned_triangular"):
                if m in r:
                    fitrows.append({"level": lab, "road_class": c, "L_imputed": LANES[c], "estimator": m, **r[m],
                                    **({f"se_{k}": v for k, v in r["speed_binned_bernstein_boot_se"].items()} if m == "speed_binned_bernstein" else {})})
    pd.DataFrame(fitrows).to_csv(out / "2_fit_parameters.csv", index=False)

    # within-segment shape (main shape evidence; no lane count or flow scale involved)
    wres, wst, wtabs = {}, {}, []
    for lab, D in (("10min", d), ("hourly", h)):
        wres[lab] = {}
        for c in (2, 3, 4):
            r, s_ = within_curve(D[D.road_class == c], rng, WITHIN_REPS if lab == "10min" else 40)
            wres[lab][c] = r
            wtabs.append(s_["tab"].assign(road_class=c, level=lab))
            if lab == "10min":
                wst[c] = {"bv": s_["b"], "R": s_["R"], "tab": s_["tab"], "keep": s_["keep"]}
            print("within", lab, c, json.dumps(jsonable(r["empirical"])), json.dumps(jsonable(r["fit"])), f"{time.time() - t0:.0f}s", flush=True)
    res["within_segment"] = wres
    pd.concat(wtabs).to_csv(out / "0_within_segment_curves.csv", index=False)

    # falling branch: share of slots beyond capacity, by hour (weekdays)
    fb = []
    for c in (2, 3, 4):
        vc = summarize(st[c]["bv"], st[c]["R"])["v_c_kmh"]
        xc = summarize(wst[c]["bv"], wst[c]["R"])["v_c_kmh"]   # within units: v / v90
        g = d[(d.road_class == c) & d.weekday]
        s = g.assign(below=g.v < vc, belowx=g.x < xc).groupby("hour").agg(
            n=("v", "size"), share_below_vc=("below", "mean"), share_below_xc=("belowx", "mean"))
        s["road_class"], s["v_c_kmh_pooled"], s["x_c_within"] = c, vc, xc
        fb.append(s.reset_index())
    fb = pd.concat(fb)
    fb.to_csv(out / "3_falling_branch_share_by_hour.csv", index=False)
    res["falling_branch"] = {}
    for c in (2, 3, 4):
        g = d[d.road_class == c]
        vc = summarize(st[c]["bv"], st[c]["R"])["v_c_kmh"]
        xc = summarize(wst[c]["bv"], wst[c]["R"])["v_c_kmh"]   # within units: v / v90
        wd = g[g.weekday & g.hour.between(6, 21)]
        pk = wd[wd.hour.isin([7, 8])]
        res["falling_branch"][c] = {
            "within_x_c": xc, "pooled_v_c_kmh": vc,
            "share_all_slots_x_below_xc": float((g.x < xc).mean()),
            "share_weekday_06_22_x_below_xc": float((wd.x < xc).mean()),
            "share_weekday_07_09_x_below_xc": float((pk.x < xc).mean()),
            "share_weekday_17_19_x_below_xc": float((wd[wd.hour.isin([17, 18])].x < xc).mean()),
            "share_weekday_06_22_v_below_pooled_vc": float((wd.v < vc).mean()),
            "share_segments_ever_x_below_xc_weekday_peaks": float((wd[wd.hour.isin([7, 8, 17, 18])].groupby("roadseg_id").x.min() < xc).mean())}

    # lane scaling
    res["lane_scaling"], ls = lane_scaling(st, rng)
    ls.to_csv(out / "4_lane_scaling_comparisons.csv", index=False)
    print("lane scaling", json.dumps(jsonable(res["lane_scaling"])), flush=True)

    # noise checks
    zt = z_check(d)
    zt.to_csv(out / "5a_routed_demand_bins.csv", index=False)
    spl, res["split_sample"] = split_check(d, st)
    spl.to_csv(out / "5b_split_sample_bins.csv", index=False)

    # elasticity
    xtab, cmp_, ec = elasticity_compare(h, wst)
    xtab.to_csv(out / "6a_implied_elasticity_by_speed_ratio.csv", index=False)
    cmp_.to_csv(out / "6b_implied_vs_project_elasticity.csv", index=False)

    # tidal
    pairs, rep, top, tgrid = tidal(d, wst)
    tgrid.to_csv(out / "7d_tidal_grid_hypothetical_states.csv", index=False)
    pairs.to_csv(out / "7a_tidal_pairs_all.csv", index=False)
    rep.to_csv(out / "7b_tidal_representative_by_class.csv", index=False)
    top.head(20).to_csv(out / "7c_tidal_top20_asymmetric_pairs_L4plus.csv", index=False)
    res["tidal"] = {"pairs_found": int(len(pairs)), "by_class": rep.to_dict(orient="records")}

    fig_within(wst, wres, out)
    fig_cloud(d, st, out)
    fig_lanes(st, out)
    fig_checks(wst, st, sth, zt, spl, fb, ec, cmp_, out)
    (out / "summary.json").write_text(json.dumps(jsonable(res), indent=2, ensure_ascii=False))
    print(json.dumps(jsonable(res), indent=1, ensure_ascii=False))
    print(f"done {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
