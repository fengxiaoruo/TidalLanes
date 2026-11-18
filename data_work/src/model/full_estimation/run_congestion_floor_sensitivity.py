"""
Free-flow floor sensitivity of the congestion elasticity (audit follow-up, 2026-11-13)
======================================================================================

Baseline (congestion_iv_followup_v1, edge 06-22 h, beta = 0.245):
    ln t = max{ ln T0_u + delta , mu_u + d_{date,hour} + beta ln q } + eps
    T0_u = mean pace 00-05 h (al.night_floor), delta = 0
    mu_u, d_{date,hour}: per-group mean-zero residuals (fu.Floor2 alternating bisection)
    beta: sum eps * w~ = 0, w~ = ln z residualised on unit and date x hour FE (NIV-z)
          (or w~ = ln q residualised the same way: "NLS-like" moment of fu.Floor2)

Sections (edge = gate5 directed edge; segment = Baidu segment, clusters = main gate5 edge):
    1 reproduce the baseline with fu.Floor2 itself and with this file's solver
    2 common shift grid delta in [-0.08, 0.08]
    3 alternative floors (00-05 mean, 02-05 mean, 00-05 median / p10, all-hour p10),
      with and without stale constant-fill slots (>= 60 min exact-constant Baidu pace,
      flag_runs of run_segment_lambda_robustness.py). Edge pace without stale slots is
      rebuilt from segment pace with segment_edge_weights (as run_lambda_strict_match.py);
      the same rebuild without dropping is reported as a bridge.
    4a delta estimated jointly: (i) NIV-z plus the moment mean(eps | classified on floor) = 0,
       classification = predicted index <= floor; (ii) SSR profiled over (beta, delta)
    4b unit-specific floor: y = f_u + (s - c_u)_+, s = d_{date,hour} + beta ln q, (f_u, c_u)
       by per-unit least squares over kink candidates at the unit's own s values
       (Hansen 2017 continuous threshold), common beta by NIV-z moment or by SSR
    5 linear IV without any floor on high-load observations selected on z (unit pct) or clock
    6 cluster bootstrap (units, as al.floor_fit): A T0 fixed, B T0 re-drawn per unit copy
      from a date-block resample of its own night hours; baseline and 4a(i)
    7 binned within-unit shape (20 quantile bins)

Existing modules are imported, not edited. Inputs read only.
Output: data_work/outputs/<output-name>/ (refuses to overwrite)
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import optimize, sparse

sys.path.insert(0, str(Path(__file__).resolve().parent))
from run_all_traffic_speed_panel_gate5 import MIN_COVERAGE, ROOT, WEEKDAYS, slope, within  # noqa: E402
import run_congestion_tail_followup as tf  # noqa: E402
import run_congestion_almagro_spec as al  # noqa: E402
import run_congestion_iv_followup as fu  # noqa: E402
import run_segment_lambda as sl  # noqa: E402
from run_segment_lambda_robustness import flag_runs  # noqa: E402

WIN = (6, 21)
SHIFTS = np.round(np.arange(-0.08, 0.0801, 0.01), 2)
NLS_DELTAS = np.round(np.arange(-0.15, 0.1501, 0.01), 2)
DATES = [20250519, 20250520, 20250521, 20250522, 20250523, 20250524, 20250525]
T0 = time.time()


def log(*a):
    print(f"[{time.time() - T0:7.0f}s]", *a, flush=True)


def r(x, k=4):
    return None if x is None or not np.isfinite(x) else round(float(x), k)


# ======================================================================= data
def edge10_from(pace: pd.DataFrame) -> pd.DataFrame:
    """tf.edge10 with the pace table as an argument."""
    t = pace.merge(pd.read_parquet(tf.GATE5 / "rn_all/edge_flow_10min.parquet"), on=tf.K, how="left").fillna({"len_flow_all": 0.0})
    t = t.merge(pd.read_parquet(tf.GATE5 / "od_iv/edge_routed_od_flow_10min.parquet"), on=tf.K, how="left").fillna({"z_routed": 0.0})
    t["hour"] = t.slot10 // 6
    t["unit"] = t.grid_from + ">" + t.grid_to
    return t[t.coverage >= MIN_COVERAGE]


def edge_data_from(pace: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """al.edge_data with the pace table as an argument; also returns the hourly table used for floors."""
    t = edge10_from(pace)
    hh = t.groupby(["unit", "grid_from", "grid_to", "date", "hour"]).agg(
        pace=("pace_min_per_km", "mean"), flow=("len_flow_all", "sum"), z=("z_routed", "sum"), n=("slot10", "size")).reset_index()
    full = hh[hh.n == 6].rename(columns={"unit": "u"})
    h = full[full.flow >= 5].copy()
    h["cl"] = h.u
    city = t.groupby(["date", "hour"]).z_routed.sum().rename("city")
    own = t.groupby(["unit", "date", "hour"]).z_routed.sum().rename("own").reset_index().rename(columns={"unit": "u"})
    h = h.merge(city.reset_index(), on=["date", "hour"]).merge(own, on=["u", "date", "hour"])
    h["city_loo"] = h.city - h.own
    return h[["u", "cl", "date", "hour", "pace", "flow", "z", "city_loo"]], full[["u", "date", "hour", "pace"]]


def seg_data_from(t10: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """tf.seg_panel((0, 23)) + al.seg_data with the 10-min segment pace table as an argument."""
    t = t10[["roadseg_id", "date", "slot10", "pace_min_per_km"]].copy()
    t["hour"] = t.slot10 // 6
    hh = t.groupby(["roadseg_id", "date", "hour"]).agg(pace=("pace_min_per_km", "mean"), n=("slot10", "size")).reset_index()
    p = hh[hh.n >= 4]
    h = p.merge(pd.read_parquet(tf.SEG / "segment_flow_hour.parquet"), on=["roadseg_id", "date", "hour"])
    meta = pd.read_parquet(tf.SEG / "segment_meta.parquet")
    good = meta[meta.matched_share_of_length >= sl.MIN_MATCH_SHARE].roadseg_id
    h6 = h[h.hour.between(6, 21) & h.date.isin(WEEKDAYS)]
    mf = h6.groupby("roadseg_id").flow.mean()
    keep = set(good) & set(mf[mf >= sl.MIN_MEAN_FLOW].index)
    h = h[h.roadseg_id.isin(keep) & (h.flow >= 5)]
    sw = pd.read_parquet(tf.GATE5 / "speed/segment_edge_weights.parquet").sort_values("w").drop_duplicates("roadseg_id", keep="last")
    z = pd.read_parquet(tf.GATE5 / "od_iv/edge_routed_od_flow_10min.parquet")
    z["hour"] = z.slot10 // 6
    city = z.groupby(["date", "hour"]).z_routed.sum().rename("city").reset_index()
    z = z.groupby(["grid_from", "grid_to", "date", "hour"]).z_routed.sum().reset_index()
    h = h.merge(sw[["roadseg_id", "grid_from", "grid_to"]], on="roadseg_id", how="left").merge(z, on=["grid_from", "grid_to", "date", "hour"], how="left")
    h["gedge"] = h.grid_from.fillna("") + ">" + h.grid_to.fillna("")
    h = h.rename(columns={"roadseg_id": "u"})
    h = h[h.gedge != ">"].copy()
    h["cl"] = h.gedge
    h = h.merge(city, on=["date", "hour"])
    h["z"] = h.z_routed.fillna(0.0)
    h["city_loo"] = h.city - h.z
    return h[["u", "cl", "date", "hour", "pace", "flow", "z", "city_loo"]], p.rename(columns={"roadseg_id": "u"})[["u", "date", "hour", "pace"]]


def edge_pace_from_segments(t10: pd.DataFrame, drop_stale: bool) -> pd.DataFrame:
    """Weighted edge pace over the Baidu segments present in segment_pace_10min (run_lambda_strict_match.edge_pace)."""
    sw = pd.read_parquet(tf.GATE5 / "speed/segment_edge_weights.parquet")
    segs = pd.Index(sorted(set(sw.roadseg_id) & set(t10.roadseg_id)))
    w = sw[sw.roadseg_id.isin(segs)]
    t = t10[t10.roadseg_id.isin(segs)]
    if drop_stale:
        t = t[~t.stale]
    ek = w[["grid_from", "grid_to"]].drop_duplicates().reset_index(drop=True)
    eix = pd.MultiIndex.from_frame(ek)
    ei = eix.get_indexer(pd.MultiIndex.from_frame(w[["grid_from", "grid_to"]]))
    W = sparse.csr_matrix((w.w.to_numpy(), (ei, segs.get_indexer(w.roadseg_id))), shape=(len(ek), len(segs)))
    T = len(DATES) * 144
    col = pd.Series(t.date.to_numpy()).map({d: k for k, d in enumerate(DATES)}).to_numpy() * 144 + t.slot10.to_numpy()
    ri = segs.get_indexer(t.roadseg_id)
    P = sparse.csr_matrix((t.pace_min_per_km.to_numpy(), (ri, col)), shape=(len(segs), T))
    O = sparse.csr_matrix((np.ones(len(t)), (ri, col)), shape=(len(segs), T))
    wp, wo = (W @ P).toarray(), (W @ O).toarray()
    wt = np.asarray(W.sum(axis=1)).ravel()
    rr, cc = np.nonzero(wo > 1e-12)
    return pd.DataFrame({"grid_from": ek.grid_from.to_numpy()[rr], "grid_to": ek.grid_to.to_numpy()[rr],
                         "date": np.array(DATES)[cc // 144].astype("int32"), "slot10": (cc % 144).astype("int16"),
                         "pace_min_per_km": wp[rr, cc] / wo[rr, cc], "coverage": wo[rr, cc] / wt[rr]})


def floors(p: pd.DataFrame) -> dict[str, pd.Series]:
    """Alternative log floors from an hourly pace table (u, hour, pace)."""
    n = p[p.hour.between(0, 4)].groupby("u").pace
    n2 = p[p.hour.between(2, 4)].groupby("u").pace
    a = p.groupby("u").pace
    sz, sz2 = n.size(), n2.size()
    out = {"base_mean_00_05": np.log(n.mean()[sz >= al.MIN_NIGHT_HOURS]),
           "a_mean_02_05": np.log(n2.mean()[sz2 >= al.MIN_NIGHT_HOURS]),
           "b1_median_00_05": np.log(n.median()[sz >= al.MIN_NIGHT_HOURS]),
           "b2_p10_00_05": np.log(n.quantile(0.10)[sz >= al.MIN_NIGHT_HOURS]),
           "c_p10_all_hours": np.log(a.quantile(0.10)[a.size() >= al.MIN_NIGHT_HOURS])}
    return {k: v.rename("lt0") for k, v in out.items()}


# ================================================================== estimators
def codes(d: pd.DataFrame, fes: list[list[str]]) -> list[np.ndarray]:
    return [d.groupby(f, sort=False).ngroup().to_numpy() for f in fes]


def fwithin(arrs: list[np.ndarray], cs: list[np.ndarray], iters: int = 30) -> list[np.ndarray]:
    """Same alternating projections as run_all_traffic_speed_panel_gate5.within, on integer codes."""
    cn = [np.bincount(c) for c in cs]
    out = []
    for v in arrs:
        x = np.asarray(v, float).copy()
        for _ in range(iters):
            for c, n in zip(cs, cn):
                x -= (np.bincount(c, x, minlength=len(n)) / n)[c]
        out.append(x)
    return out


def wtilde(d: pd.DataFrame, col: str) -> np.ndarray:
    return fwithin([d[col].to_numpy()], codes(d, [["u"], ["date", "hour"]]))[0]


class FloorShift(fu.Floor2):
    """fu.Floor2 with the floor shifted by a common delta (moments unchanged)."""

    def __init__(self, d: pd.DataFrame, w: np.ndarray):
        super().__init__(d, w)
        self.f0 = self.f.copy()

    def shift(self, delta: float) -> "FloorShift":
        self.f = self.f0 + delta
        return self

    def stats(self, beta: float) -> dict:
        e, act = self.resid(beta)
        return {"active_share": r(act.mean(), 3), "share_hours_below_floor": r((self.y < self.f).mean(), 3),
                "mean_resid_on_floor_classified": r(e[~act].mean()) if (~act).any() else None,
                "ssr_mean": r(float(e @ e) / len(e), 6)}


def root_beta(F, guess: float = 0.25, step: float = 0.03, xtol: float = 1e-4, lim=(-0.5, 2.5)) -> float:
    """Bracket from the guess (expanding toward the smaller |g|), then Brent."""
    a, ga = guess, F.g(guess)
    b, gb = a + step, F.g(a + step)
    k = 0
    while np.sign(ga) == np.sign(gb):
        k += 1
        if k > 25 or not (lim[0] < min(a, b) and max(a, b) < lim[1]):
            return float("nan")
        if abs(gb) < abs(ga):
            a, ga, b = b, gb, b + (b - a) * 1.6
            gb = F.g(b)
        else:
            b, gb, a = a, ga, a - (b - a) * 1.6
            ga = F.g(a)
    lo, hi = (a, b) if a < b else (b, a)
    return float(optimize.brentq(F.g, lo, hi, xtol=xtol))


def scan(F, grid) -> tuple[int, list]:
    gv = np.array([F.g(b) for b in grid])
    return int((np.diff(np.sign(gv)) != 0).sum()), [r(x, 7) for x in gv]


def fit_spec(d: pd.DataFrame, wcol: str = "lz", grid=None, guess: float = 0.25) -> dict:
    F = FloorShift(d, wtilde(d, wcol))
    b = root_beta(F, guess)
    o = {"beta": r(b), "n": int(len(d)), "units": int(d.u.nunique())}
    if np.isfinite(b):
        o.update(F.stats(b))
    if grid is not None:
        o["sign_changes_on_scan_grid"], _ = scan(F, grid)
    return o


# ------------------------------------------------------------ 4a joint shift
def joint_iv(F: FloorShift, d0: float = 0.0, b0: float = 0.25, step: float = 0.02,
             xtol_d: float = 2e-4, xtol_b: float = 1e-4, table: list | None = None) -> dict:
    """delta solving mean(eps | predicted index <= floor) = 0, beta(delta) from the NIV moment."""
    memo = {}

    def h(delta):
        delta = float(delta)
        if delta not in memo:
            F.shift(delta)
            g = memo[max(memo, key=lambda k: -abs(k - delta))][1] if memo else b0
            b = root_beta(F, g if np.isfinite(g) else b0, step=0.02, xtol=xtol_b)
            m = F.stats(b)["mean_resid_on_floor_classified"] if np.isfinite(b) else None
            memo[delta] = (np.nan if m is None else m, b)
            if table is not None:
                table.append({"delta": delta, "beta": b, "m2": memo[delta][0]})
        return memo[delta][0]

    a, b = d0 - step, d0 + step
    ha, hb = h(a), h(b)
    k = 0
    while np.isfinite(ha) and np.isfinite(hb) and np.sign(ha) == np.sign(hb) and k < 12:
        k += 1
        if abs(hb) < abs(ha):
            a, ha, b = b, hb, b + (b - a)
            hb = h(b)
        else:
            b, hb, a = a, ha, a - (b - a)
            ha = h(a)
    if not (np.isfinite(ha) and np.isfinite(hb)) or np.sign(ha) == np.sign(hb):
        return {"delta": None, "beta": None}
    lo, hi = min(a, b), max(a, b)
    dh = optimize.brentq(h, lo, hi, xtol=xtol_d)
    F.shift(dh)
    bh = memo[float(dh)][1]
    out = {"delta": r(dh), "beta": r(bh)}
    out.update(F.stats(bh))
    F.shift(0.0)
    return out


def nls_profile(F: FloorShift, deltas, bounds=(0.0, 1.2), xatol=2e-3) -> pd.DataFrame:
    rows = []
    for dl in deltas:
        F.shift(dl)
        f = lambda b: float(np.mean(F.resid(b)[0] ** 2))
        res = optimize.minimize_scalar(f, bounds=bounds, method="bounded", options={"xatol": xatol})
        rows.append({"delta": dl, "beta_ssr_min": round(float(res.x), 4), "ssr_mean": float(res.fun),
                     "at_bound": bool(min(abs(res.x - bounds[0]), abs(res.x - bounds[1])) < 5 * xatol)})
        log("  nls", dl, rows[-1]["beta_ssr_min"], rows[-1]["ssr_mean"])
    F.shift(0.0)
    p = pd.DataFrame(rows)
    i = p.ssr_mean.idxmin()
    n = len(F.y)
    p["LR_homosk"] = n * (p.ssr_mean - p.ssr_mean[i]) / p.ssr_mean[i]
    return p


# --------------------------------------------------------- 4b unit-specific floor
class UnitKink:
    """y = f_u + (s - c_u)_+, s = d_t + beta x; (f_u, c_u) by per-unit LS over kinks at the unit's s values."""

    def __init__(self, d: pd.DataFrame, w: np.ndarray):
        self.cu, _ = pd.factorize(d.u)
        self.ct, _ = pd.factorize(d.date.astype(str) + "_" + d.hour.astype(str))
        self.y, self.x, self.lt0 = d.y.to_numpy(), d.lq.to_numpy(), d.lt0.to_numpy()
        self.w = w
        self.Gu, self.Gt = self.cu.max() + 1, self.ct.max() + 1

    def unit_fit(self, s):
        n = len(s)
        o = np.lexsort((s, self.cu))
        cs, ys, ss = self.cu[o], self.y[o], s[o]
        vs = ys - ss
        st = np.r_[0, np.flatnonzero(cs[1:] != cs[:-1]) + 1]
        grp = np.repeat(np.arange(len(st)), np.diff(np.r_[st, n]))

        def gc(a):
            c = np.cumsum(a)
            return c - np.r_[0.0, c][st][grp]

        def gt(a):
            return np.add.reduceat(a, st)[grp]

        N = gt(np.ones(n)); k = gc(np.ones(n))
        SyA, SyyA, SvA, SvvA = gc(ys), gc(ys * ys), gc(vs), gc(vs * vs)
        SvB, SvvB, nB = gt(vs) - SvA, gt(vs * vs) - SvvA, N - k
        Sr = SyA + SvB + ss * nB
        ssr = SyyA + SvvB + 2 * ss * SvB + ss * ss * nB - Sr ** 2 / N
        mn = np.minimum.reduceat(ssr, st)
        cand = np.flatnonzero(ssr <= mn[grp] + 1e-10)
        _, first = np.unique(grp[cand], return_index=True)
        best = cand[first]
        c = np.empty(self.Gu); f = np.empty(self.Gu)
        c[cs[st]] = ss[best]; f[cs[st]] = Sr[best] / N[best]
        return f, c

    def resid(self, beta, iters=4):
        bx = beta * self.x
        dl = np.zeros(self.Gt)
        for _ in range(iters):
            s = dl[self.ct] + bx
            f, c = self.unit_fit(s)
            act = s > c[self.cu]
            v = (self.y - f[self.cu] + c[self.cu] - bx)
            cnt = np.bincount(self.ct[act], minlength=self.Gt)
            upd = np.bincount(self.ct[act], v[act], minlength=self.Gt)
            dl = np.where(cnt > 0, upd / np.maximum(cnt, 1), dl)
            dl -= dl.mean()
        s = dl[self.ct] + bx
        f, c = self.unit_fit(s)
        fit = f[self.cu] + np.maximum(0.0, s - c[self.cu])
        return self.y - fit, s > c[self.cu], f

    def g(self, beta):
        return float(self.resid(beta)[0] @ self.w) / len(self.y)

    def ssr(self, beta):
        e = self.resid(beta)[0]
        return float(e @ e) / len(e)


def unit_floor(d: pd.DataFrame) -> dict:
    U = UnitKink(d, wtilde(d, "lz"))
    grid = np.round(np.arange(0.0, 2.001, 0.08), 3)
    gv = np.array([U.g(b) for b in grid])
    sv = np.array([U.ssr(b) for b in grid])
    o = {"scan": pd.DataFrame({"beta": grid, "g_niv": gv, "ssr_mean": sv})}
    s = np.where(np.sign(gv[:-1]) != np.sign(gv[1:]))[0]
    o["iv_sign_changes"] = int(len(s))
    bi = float(optimize.brentq(U.g, grid[s[0]], grid[s[0] + 1], xtol=1e-4)) if len(s) else float("nan")
    i = int(np.argmin(sv))
    res = optimize.minimize_scalar(U.ssr, bounds=(grid[max(i - 1, 0)], grid[min(i + 1, len(grid) - 1)]), method="bounded", options={"xatol": 1e-3})
    bn = float(res.x)
    for lab, b in [("NIV_z", bi), ("SSR_min", bn)]:
        if not np.isfinite(b):
            o[lab] = {"beta": None}
            continue
        e, act, f = U.resid(b)
        fl = pd.Series(f).rename("f")
        lt0 = pd.Series(U.lt0).groupby(U.cu).first()
        dev = (fl - lt0.reindex(fl.index).to_numpy())
        o[lab] = {"beta": r(b), "active_share": r(act.mean(), 3), "ssr_mean": r(float(e @ e) / len(e), 6),
                  "unit_floor_minus_night_lnT0_p10_p25_p50_p75_p90": [r(x, 3) for x in dev.quantile([.1, .25, .5, .75, .9])],
                  "share_units_never_on_floor": r(float((pd.Series(act).groupby(U.cu).mean() == 1).mean()), 3),
                  "share_units_always_on_floor": r(float((pd.Series(act).groupby(U.cu).mean() == 0).mean()), 3)}
    return o


# ------------------------------------------------------------------ 5 linear local
def local_linear(d: pd.DataFrame, lab: str) -> list[dict]:
    rows = []
    pct = {q: d.groupby("u").z.transform(lambda s, q=q: s.quantile(q)) for q in (0.5, 0.7, 0.85)}
    sels = [("all hours in window", np.ones(len(d), bool))]
    sels += [(f"z > unit p{int(q * 100)}", (d.z > pct[q]).to_numpy()) for q in (0.5, 0.7, 0.85)]
    sels += [("hours 07-09", d.hour.isin([7, 8]).to_numpy()), ("hours 17-19", d.hour.isin([17, 18]).to_numpy()),
             ("hours 07-09 and 17-19", d.hour.isin([7, 8, 17, 18]).to_numpy())]
    for nm, m in sels:
        s = d[m].reset_index(drop=True)
        s = s[s.groupby("u").u.transform("size") >= 2].reset_index(drop=True)
        cu = codes(s, [["u"]])
        cut = codes(s, [["u"], ["date", "hour"]])
        y1, q1, z1, c1 = fwithin([s.y, s.lq, s.lz, s.lc], cu)
        y2, q2, z2 = fwithin([s.y, s.lq, s.lz], cut)
        for spec, args in [("IV ln q by ln z, unit FE", (y1, q1, s.cl, z1)),
                           ("IV ln q by citywide load, unit FE", (y1, q1, s.cl, c1)),
                           ("IV ln q by ln z, unit FE + date x hour FE", (y2, q2, s.cl, z2)),
                           ("OLS ln t on ln q, unit FE + date x hour FE", (y2, q2, s.cl, None))]:
            o = slope(args[0], args[1], args[2], z=args[3])
            rows.append({"sample_window": lab, "selection": nm, "spec": spec, "beta": o["b"], "se_cluster": o["se_cluster_edge"],
                         "first_stage_F_cluster": o.get("first_stage_F_cluster"), "n": o["n"], "clusters": o["n_clusters"],
                         "units": int(s.u.nunique())})
    return rows


# ----------------------------------------------------------------- 7 binned shape
def binned(d: pd.DataFrame, lab: str) -> pd.DataFrame:
    out = []
    ry = d.y - d.groupby("u").y.transform("mean")
    yf = d.y - d.lt0
    for xc in ["lq", "lz"]:
        rx = d[xc] - d.groupby("u")[xc].transform("mean")
        b = pd.qcut(rx, 20, labels=False, duplicates="drop")
        g = pd.DataFrame({"bin": b, "rx": rx, "ry": ry, "yf": yf, "below": (d.y < d.lt0)}).groupby("bin")
        t = g.agg(n=("rx", "size"), x_demeaned_mean=("rx", "mean"), ln_pace_demeaned_mean=("ry", "mean"),
                  ln_t_minus_lnT0_mean=("yf", "mean"), ln_t_minus_lnT0_p25=("yf", lambda s: s.quantile(.25)),
                  ln_t_minus_lnT0_p50=("yf", "median"), ln_t_minus_lnT0_p75=("yf", lambda s: s.quantile(.75)),
                  share_below_floor=("below", "mean")).reset_index()
        t.insert(0, "x", "ln q (observed)" if xc == "lq" else "ln z (routed OD)")
        t.insert(0, "window", lab)
        out.append(t)
    return pd.concat(out).round(5)


# -------------------------------------------------------------------- 6 bootstrap
def night_blocks(full: pd.DataFrame, units: pd.Index) -> tuple[np.ndarray, np.ndarray]:
    n = full[full.hour.between(*al.NIGHT)]
    g = n.groupby(["u", "date"]).pace.agg(["sum", "size"])
    s = g["sum"].unstack("date").reindex(index=units, columns=DATES).fillna(0.0)
    c = g["size"].unstack("date").reindex(index=units, columns=DATES).fillna(0.0)
    return s.to_numpy(), c.to_numpy()


def bootstrap(d: pd.DataFrame, full: pd.DataFrame, B: int, b_hat: float, joint_hat: dict, rng, out: Path) -> dict:
    cl = d.cl.to_numpy()
    ucl = np.unique(cl)
    idx = pd.Series(np.arange(len(d))).groupby(cl).apply(np.asarray).to_dict()
    units = pd.Index(sorted(d.u.unique()))
    S, C = night_blocks(full, units)
    uix = units.get_indexer(d.u)
    rows = []
    for rep in range(B):
        t = time.time()
        pick = rng.choice(ucl, len(ucl), replace=True)
        rws = np.concatenate([idx[c] for c in pick])
        copy = np.repeat(np.arange(len(pick)), [len(idx[c]) for c in pick])
        dd = d.iloc[rws].reset_index(drop=True)
        dd["u"] = dd.u.astype(str) + "#" + copy.astype(str)
        w = wtilde(dd, "lz")
        rec = {"rep": rep}
        # T0 per unit copy re-drawn from a date-block resample of its own night hours
        u0 = uix[rws]
        first = np.r_[0, np.flatnonzero(np.diff(copy)) + 1]
        cu0 = u0[first]
        dr = rng.integers(0, len(DATES), size=(len(pick), len(DATES)))
        ss = S[cu0[:, None], dr].sum(1)
        cc = C[cu0[:, None], dr].sum(1)
        ok = cc >= al.MIN_NIGHT_HOURS
        lt0b = np.where(ok, np.log(np.where(ok, ss, 1.0) / np.maximum(cc, 1)), np.nan)
        lt0b = np.where(ok, lt0b, dd.lt0.to_numpy()[first])
        rec["B_units_T0_fallback"] = int((~ok).sum())
        for var in ["A", "B"]:
            de = dd if var == "A" else dd.assign(lt0=lt0b[copy])
            F = FloorShift(de, w)
            rec[f"{var}_beta_fixed"] = root_beta(F, b_hat, step=0.02, xtol=5e-4)
            if joint_hat.get("delta") is not None:
                j = joint_iv(F, d0=joint_hat["delta"], b0=joint_hat["beta"], step=0.015, xtol_d=1e-3, xtol_b=5e-4)
                rec[f"{var}_joint_beta"], rec[f"{var}_joint_delta"] = j["beta"], j["delta"]
        rec["sec"] = round(time.time() - t, 1)
        rows.append(rec)
        log("boot", rec)
        pd.DataFrame(rows).to_csv(out / "6_bootstrap_reps_edge.csv", index=False)
    bs = pd.DataFrame(rows)
    sd = lambda c: r(float(np.nanstd(pd.to_numeric(bs[c], errors="coerce"), ddof=1))) if c in bs else None
    ok = lambda c: int(pd.to_numeric(bs[c], errors="coerce").notna().sum()) if c in bs else 0
    pc = lambda c: [r(x) for x in np.nanpercentile(pd.to_numeric(bs[c], errors="coerce"), [2.5, 97.5])] if c in bs else None
    res = {"reps": B, "clusters": int(len(ucl))}
    for c in ["A_beta_fixed", "B_beta_fixed", "A_joint_beta", "A_joint_delta", "B_joint_beta", "B_joint_delta"]:
        res[c] = {"sd": sd(c), "p2.5_p97.5": pc(c), "ok": ok(c)}
    return res


# ======================================================================= driver
def run_level(lab: str, h: pd.DataFrame, full: pd.DataFrame, extra: dict, out: Path, a: argparse.Namespace, rng) -> dict:
    res = {}
    edge = lab == "edge"
    scan_grid = np.round(np.arange(0.0, 1.001, 0.05), 3) if edge else None
    t0 = al.night_floor(full)
    d = al.sample(h, t0, WIN)
    d00 = al.sample(h, t0, (0, 23))

    # 1 reproduction
    log(lab, "1 reproduction")
    w = wtilde(d, "lz")
    w_pd = within(d, ["lz"], [["u"], ["date", "hour"]]).lz.to_numpy()
    rep = {"n": int(len(d)), "units": int(d.u.nunique()), "max_abs_diff_fast_vs_pandas_within": float(np.abs(w - w_pd).max())}
    if edge or a.seg_reproduce_floor2:
        bt, act = fu.Floor2(d, w_pd).solve()
        rep["fu_Floor2_solve"] = {"beta": r(bt), "active_share": r(act, 3)}
    F = FloorShift(d, w)
    b0 = root_beta(F, 0.25 if edge else 0.42)
    rep["this_solver"] = {"beta": r(b0), **F.stats(b0)}
    if scan_grid is not None:
        rep["this_solver"]["sign_changes_on_scan_grid"], rep["scan_g"] = scan(F, scan_grid)
    Fq = FloorShift(d, wtilde(d, "lq"))
    bq = root_beta(Fq, 0.11 if edge else 0.15)
    rep["NLS_like_moment"] = {"beta": r(bq), **Fq.stats(bq)}
    res["1_reproduction"] = rep
    log(lab, rep)
    target = {"edge": 0.245, "segment": 0.420}[lab]
    if not (np.isfinite(b0) and abs(b0 - target) <= 0.005):
        res["STOP"] = f"baseline {b0} not within 0.005 of {target}"
        return res

    # 2 shift grid
    log(lab, "2 shift grid")
    rows, bz, bqz = [], {0.0: b0}, {0.0: bq}
    for dl in sorted(a.shifts, key=lambda x: (abs(x), x)):
        near = min(bz, key=lambda k: abs(k - dl))
        F.shift(dl)
        b = root_beta(F, bz[near] if np.isfinite(bz[near]) else b0)
        bz[dl] = b
        row = {"delta": dl, "beta_NIV_z": r(b), **F.stats(b)}
        if edge:
            Fq.shift(dl)
            bq2 = root_beta(Fq, bqz[near] if np.isfinite(bqz[near]) else bq)
            bqz[dl] = bq2
            row["beta_NLS_like_moment"] = r(bq2)
            row["active_share_NLS_like"] = Fq.stats(bq2)["active_share"]
            row["sign_changes_on_scan_grid"], _ = scan(F, scan_grid)
        rows.append(row)
        log(lab, row)
    F.shift(0.0); Fq.shift(0.0)
    sg = pd.DataFrame(rows).sort_values("delta")
    sg.to_csv(out / f"2_shift_grid_{lab}.csv", index=False)
    res["2_shift_grid"] = sg.to_dict("records")

    # 3 alternative floors
    log(lab, "3 alternative floors")
    rows = []
    for src, (hh, ff) in {"stored": (h, full), **extra}.items():
        for nm, lt in floors(ff).items():
            dd = al.sample(hh, lt, WIN)
            o = fit_spec(dd, "lz", grid=scan_grid, guess=b0)
            o.update({"pace_source": src, "floor": nm,
                      "lnT0_minus_base_mean_on_common_units": r(float((lt - al.night_floor(ff)).dropna().mean()))})
            rows.append(o)
            log(lab, o)
            pd.DataFrame(rows).to_csv(out / f"3_alt_floors_{lab}.csv", index=False)
    res["3_alt_floors"] = rows

    # 4a joint shift
    log(lab, "4a joint shift")
    tab = []
    j = joint_iv(F, 0.0, b0, table=tab)
    res["4a_joint_IV"] = j
    log(lab, "4a IV", j)
    deltas = a.nls_deltas if edge else a.nls_deltas[::2]
    p = nls_profile(F, deltas)
    p.to_csv(out / f"4a_nls_ssr_profile_{lab}.csv", index=False)
    i = p.ssr_mean.idxmin()
    res["4a_joint_NLS_SSR"] = {"delta": float(p.delta[i]), "beta": float(p.beta_ssr_min[i]), "ssr_mean": float(p.ssr_mean[i]),
                               "beta_ssr_min_at_delta0": float(p.beta_ssr_min[p.delta.abs().idxmin()]),
                               "delta_set_LR_homosk_le_3.84": [float(p.delta[p.LR_homosk <= 3.84].min()), float(p.delta[p.LR_homosk <= 3.84].max())],
                               "ssr_relative_range_over_grid": r(float(p.ssr_mean.max() / p.ssr_mean.min() - 1), 6),
                               "grid_min_at_edge_of_grid": bool(i in (p.index[0], p.index[-1]))}
    log(lab, "4a NLS", res["4a_joint_NLS_SSR"])
    (out / f"{lab}_progress.json").write_text(json.dumps(res, indent=2, ensure_ascii=False, default=float))

    # 4b unit-specific floor
    if edge or a.seg_4b:
        log(lab, "4b unit floor")
        try:
            u = unit_floor(d)
            u.pop("scan").to_csv(out / f"4b_unit_floor_scan_{lab}.csv", index=False)
            res["4b_unit_floor"] = u
        except Exception as ex:  # report, do not hide
            res["4b_unit_floor"] = {"failed": repr(ex)}
        log(lab, res["4b_unit_floor"])
    else:
        res["4b_unit_floor"] = "not computed at this level"

    # 5 floor-free local linear IV
    log(lab, "5 local linear")
    rows = local_linear(d, "06-22h") + local_linear(d00, "00-24h")
    pd.DataFrame(rows).to_csv(out / f"5_local_linear_iv_{lab}.csv", index=False)
    res["5_local_linear"] = rows

    # 7 binned
    pd.concat([binned(d, "06-22h"), binned(d00, "00-24h")]).to_csv(out / f"7_binned_shape_{lab}.csv", index=False)
    (out / f"{lab}_progress.json").write_text(json.dumps(res, indent=2, ensure_ascii=False, default=float))

    # 6 bootstrap
    if edge and a.boot > 0:
        log(lab, "6 bootstrap", a.boot)
        res["6_bootstrap"] = bootstrap(d, full, a.boot, b0, j, rng, out)
    else:
        res["6_bootstrap"] = "not computed at this level"
    (out / f"{lab}_progress.json").write_text(json.dumps(res, indent=2, ensure_ascii=False, default=float))
    return res


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-name", default="congestion_floor_sensitivity_v1")
    ap.add_argument("--output-root", default=str(ROOT / "outputs"))
    ap.add_argument("--boot", type=int, default=100)
    ap.add_argument("--levels", default="edge,segment")
    ap.add_argument("--seg-4b", action="store_true")
    ap.add_argument("--seg-reproduce-floor2", action="store_true")
    ap.add_argument("--dev", action="store_true", help="short grids, for testing the code path")
    ap.add_argument("--into-existing", action="store_true",
                    help="write a further level into an existing output directory; refuses if that level's files exist")
    a = ap.parse_args(argv)
    a.shifts = [-0.03, 0.0, 0.03] if a.dev else list(SHIFTS)
    a.nls_deltas = [-0.04, -0.02, 0.0, 0.02, 0.04] if a.dev else list(NLS_DELTAS)
    out = Path(a.output_root) / a.output_name
    levels = a.levels.split(",")
    if a.into_existing:
        clash = [p for lab in levels for p in out.glob(f"*{lab}*")] if out.exists() else []
        if not out.exists() or clash:
            raise SystemExit(f"--into-existing needs {out} to exist without files for {levels}: {clash}")
    elif out.exists():
        raise SystemExit(f"{out} exists; choose a new --output-name")
    out.mkdir(parents=True, exist_ok=a.into_existing)
    summ = out / f"summary_{'_'.join(levels)}.json"
    rng = np.random.default_rng(20261113)
    res = {"notes": {"baseline": "congestion_iv_followup_v1 edge 06-22h floor_NIV_z_dateXhourFE = 0.245 (segment 0.420)",
                     "stale_rule": "segment 10-min pace in exact-constant runs >= 6 slots (60 min), flag_runs",
                     "window": "06-22 h unless stated; dates 0519-0525", "boot_reps": a.boot}}
    log("loading segment 10-min pace and flagging stale runs")
    t10 = flag_runs(pd.read_parquet(tf.SEG / "segment_pace_10min.parquet"))[["roadseg_id", "date", "slot10", "pace_min_per_km", "stale"]]
    res["notes"]["stale_share_segment_slots_all_hours"] = r(float(t10.stale.mean()))
    for lab in levels:
        log("level", lab)
        if lab == "edge":
            h, full = edge_data_from(pd.read_parquet(tf.GATE5 / "speed/edge_pace_10min.parquet"))
            h0, t00 = al.edge_data()
            chk = h.merge(h0, on=["u", "date", "hour"], suffixes=("", "_0"))
            res["notes"]["edge_loader_check"] = {"rows": [len(h), len(h0)], "max_abs_pace_diff": float((chk.pace - chk.pace_0).abs().max()),
                                                 "t0_max_abs_diff": float((al.night_floor(full) - t00).abs().max())}
            extra = {"rebuilt_from_segments_no_drop": edge_data_from(edge_pace_from_segments(t10, False)),
                     "rebuilt_from_segments_drop_stale": edge_data_from(edge_pace_from_segments(t10, True))}
        else:
            h, full = seg_data_from(t10)
            h0, t00 = al.seg_data()
            chk = h.merge(h0, on=["u", "date", "hour"], suffixes=("", "_0"))
            res["notes"]["segment_loader_check"] = {"rows": [len(h), len(h0)], "max_abs_pace_diff": float((chk.pace - chk.pace_0).abs().max()),
                                                    "max_abs_z_diff": float((chk.z - chk.z_0).abs().max()),
                                                    "t0_max_abs_diff": float((al.night_floor(full) - t00).abs().max())}
            del h0, chk
            extra = {"drop_stale": seg_data_from(t10[~t10.stale])}
        res[lab] = run_level(lab, h, full, extra, out, a, rng)
        summ.write_text(json.dumps(res, indent=2, ensure_ascii=False, default=float))
        if "STOP" in res[lab]:
            log("STOP", res[lab]["STOP"])
            break
    log("done")


if __name__ == "__main__":
    main()
