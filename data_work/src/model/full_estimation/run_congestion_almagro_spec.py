"""
Literature-comparison specifications for the congestion function (nextstep A2 (a)(b), A3 (a))
============================================================================================

Puts our panel through the Almagro et al. (NBER w32185, 2026 revision) estimator
and compares it with ours on the same sample.

    floor + constant elasticity (Almagro eq. 16/19):
        ln t_eh = max{ ln T0_e , mu_e + beta ln F_eh } + eps_eh
        T0_e    = mean pace of the unit over 00-05 h (all sample days), held fixed
        moments : sum_h eps_eh = 0 for every unit (mu_e by per-unit bisection), and
                  sum eps * (w - mean_e w) = 0 for beta, with
                    NLS : w = ln F                      (Almagro cols 1/4)
                    NIV : w = ln C_h, C_h = citywide routed-OD load in date-hour h,
                          leaving out the unit's own z  (Almagro col 6 / Kreindler 2024)
        F = observed Unicom flow q, or assigned routed-OD flow z (Almagro's F is assigned)
        SE: cluster bootstrap (B reps, 00-24 h only), T0 held at the full-sample estimate
    linear comparators on the same sample, unit FE only unless stated:
        OLS ln t on ln q
        OLS ln t on ln z         (assigned flow used directly as the regressor = reduced form)
        IV  ln q by ln C_h       (citywide hourly load)
        IV  ln q by ln z with date x hour FE (our V1 / S1)

Samples: dates 0519-0525 (one week, so hour-of-week = date x hour and no averaging),
hours 00-24 and 06-22; units need >= 3 full hours in 00-05 for T0. Edge: gate5
directed edge. Segment: Baidu segment, z and clusters from its main gate5 edge.
Inputs read only. Output: data_work/outputs/<output-name>/ (refuses to overwrite)
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from run_all_traffic_speed_panel_gate5 import ROOT, slope, within  # noqa: E402
import run_congestion_tail_followup as tf  # noqa: E402

NIGHT = (0, 4)          # hours whose mean pace defines T0 (00:00-05:00)
MIN_NIGHT_HOURS = 3
B = 50                  # bootstrap reps (00-24 h window only)
BETA_GRID = np.round(np.arange(0.0, 0.801, 0.01), 3)


# --------------------------------------------------------------------- data
def edge_data() -> tuple[pd.DataFrame, pd.Series]:
    t = tf.edge10()
    hh = t.groupby(["unit", "grid_from", "grid_to", "date", "hour"]).agg(
        pace=("pace_min_per_km", "mean"), flow=("len_flow_all", "sum"), z=("z_routed", "sum"), n=("slot10", "size")).reset_index()
    full = hh[hh.n == 6]
    t0 = night_floor(full.rename(columns={"unit": "u"}))
    h = full[(full.flow >= 5)].rename(columns={"unit": "u"})
    h["cl"] = h.u
    # citywide routed-OD load by date-hour, all edges (before sample filters)
    city = t.groupby(["date", "hour"]).z_routed.sum().rename("city")
    own = t.groupby(["unit", "date", "hour"]).z_routed.sum().rename("own").reset_index().rename(columns={"unit": "u"})
    h = h.merge(city.reset_index(), on=["date", "hour"]).merge(own, on=["u", "date", "hour"])
    h["city_loo"] = h.city - h.own
    return h[["u", "cl", "date", "hour", "pace", "flow", "z", "city_loo"]], t0


def seg_data() -> tuple[pd.DataFrame, pd.Series]:
    h = tf.seg_panel((0, 23)).rename(columns={"roadseg_id": "u"})
    h = h[h.gedge != ">"].copy()
    h["cl"] = h.gedge
    t10 = pd.read_parquet(tf.SEG / "segment_pace_10min.parquet")
    t10["hour"] = t10.slot10 // 6
    p = t10.groupby(["roadseg_id", "date", "hour"]).agg(pace=("pace_min_per_km", "mean"), n=("slot10", "size")).reset_index()
    t0 = night_floor(p[p.n >= 4].rename(columns={"roadseg_id": "u"}))
    z = pd.read_parquet(tf.GATE5 / "od_iv/edge_routed_od_flow_10min.parquet")
    z["hour"] = z.slot10 // 6
    city = z.groupby(["date", "hour"]).z_routed.sum().rename("city").reset_index()
    h = h.merge(city, on=["date", "hour"])
    h["z"] = h.z_routed.fillna(0.0)
    h["city_loo"] = h.city - h.z
    return h[["u", "cl", "date", "hour", "pace", "flow", "z", "city_loo"]], t0


def night_floor(p: pd.DataFrame) -> pd.Series:
    n = p[p.hour.between(*NIGHT)]
    g = n.groupby("u").pace.agg(["mean", "size"])
    return np.log(g["mean"][g["size"] >= MIN_NIGHT_HOURS]).rename("lt0")


def sample(h: pd.DataFrame, t0: pd.Series, hours: tuple[int, int]) -> pd.DataFrame:
    d = h[h.hour.between(*hours) & (h.z > 0)].merge(t0.reset_index(), on="u")
    d = d.assign(y=np.log(d.pace), lq=np.log(d.flow), lz=np.log(d.z), lc=np.log(d.city_loo))
    return d.reset_index(drop=True)


# ---------------------------------------------------------------- estimator
class Floor:
    """Profiled moment estimator for ln t = max{lt0, mu_u + beta x} + eps."""

    def __init__(self, d: pd.DataFrame, x: str, w: str):
        self.code, uniq = pd.factorize(d.u)
        self.G = len(uniq)
        self.y, self.x, self.f = d.y.to_numpy(), d[x].to_numpy(), d.lt0.to_numpy()
        wv = d[w].to_numpy()
        self.wt = wv - self.mean(wv)[self.code]
        self.cnt = np.bincount(self.code, minlength=self.G)

    def mean(self, v):
        return np.bincount(self.code, v, minlength=self.G) / np.bincount(self.code, minlength=self.G)

    def mu(self, beta: float, iters: int = 32) -> np.ndarray:
        bx = beta * self.x
        lo = self.mean(self.f - bx) - 3.0     # all observations at the floor
        hi = self.mean(self.y - bx) + 0.5     # sum eps < 0 here
        lo = np.minimum(lo, hi - 0.5)
        for _ in range(iters):
            m = 0.5 * (lo + hi)
            s = np.bincount(self.code, self.y - np.maximum(self.f, m[self.code] + bx), minlength=self.G)
            pos = s > 0
            lo, hi = np.where(pos, m, lo), np.where(pos, hi, m)
        return 0.5 * (lo + hi)

    def g(self, beta: float) -> float:
        m = self.mu(beta)
        e = self.y - np.maximum(self.f, m[self.code] + beta * self.x)
        return float(e @ self.wt) / len(self.y)

    def solve(self, grid=BETA_GRID, iters: int = 25) -> tuple[float, np.ndarray]:
        gv = np.array([self.g(b) for b in grid])
        s = np.where(np.sign(gv[:-1]) != np.sign(gv[1:]))[0]
        if len(s) == 0:
            return float("nan"), gv
        a, b = grid[s[0]], grid[s[0] + 1]
        ga = gv[s[0]]
        for _ in range(iters):
            c = 0.5 * (a + b)
            gc = self.g(c)
            if np.sign(gc) == np.sign(ga):
                a, ga = c, gc
            else:
                b = c
        return 0.5 * (a + b), gv

    def active_share(self, beta: float) -> float:
        m = self.mu(beta)
        return float((m[self.code] + beta * self.x > self.f).mean())


def floor_fit(d: pd.DataFrame, x: str, w: str, boot: int, rng) -> dict:
    est = Floor(d, x, w)
    b, gv = est.solve()
    out = {"beta": round(b, 4), "active_share": round(est.active_share(b), 3) if np.isfinite(b) else None,
           "n": int(len(d)), "units": int(est.G), "moment_sign_changes_on_grid": int((np.diff(np.sign(gv)) != 0).sum())}
    if boot and np.isfinite(b):
        cl = d.cl.to_numpy()
        ucl = np.unique(cl)
        idx = pd.Series(np.arange(len(d))).groupby(cl).apply(np.asarray).to_dict()
        bs = []
        for r in range(boot):
            pick = rng.choice(ucl, len(ucl), replace=True)
            rows = np.concatenate([idx[c] for c in pick])
            dd = d.iloc[rows].copy()
            dd["u"] = dd.u.astype(str) + "#" + np.repeat(np.arange(len(pick)), [len(idx[c]) for c in pick]).astype(str)
            bb, _ = Floor(dd, x, w).solve(grid=np.round(np.arange(max(0, b - 0.2), b + 0.2001, 0.02), 3), iters=12)
            bs.append(bb)
        bs = np.array(bs)
        out["se_boot_cluster"] = round(float(np.nanstd(bs, ddof=1)), 4)
        out["boot_ok"] = int(np.isfinite(bs).sum())
    return out


def linear(d: pd.DataFrame) -> dict:
    u = [["u"]]
    r = within(d, ["y", "lq", "lz", "lc"], u)
    rt = within(d, ["y", "lq", "lz"], [["u"], ["date", "hour"]])
    o = {"OLS ln t on ln q, unit FE": slope(r.y, r.lq, d.cl),
         "OLS ln t on ln z (assigned flow as regressor), unit FE": slope(r.y, r.lz, d.cl),
         "IV ln q by citywide load, unit FE": slope(r.y, r.lq, d.cl, z=r.lc),
         "OLS ln t on ln q, unit FE + date x hour FE": slope(rt.y, rt.lq, d.cl),
         "OLS ln t on ln z, unit FE + date x hour FE (reduced form)": slope(rt.y, rt.lz, d.cl),
         "IV ln q by ln z, unit FE + date x hour FE (V1/S1)": slope(rt.y, rt.lq, d.cl, z=rt.lz)}
    o["first_stage_ln_q_on_ln_z_dxh"] = round(float((rt.lz @ rt.lq) / (rt.lz @ rt.lz)), 4)
    o["first_stage_ln_q_on_ln_city_unitFE"] = round(float((r.lc @ r.lq) / (r.lc @ r.lc)), 4)
    o["corr_within_ln_q_ln_z_unitFE"] = round(float(np.corrcoef(r.lq, r.lz)[0, 1]), 4)
    return o


def level(lab: str, h: pd.DataFrame, t0: pd.Series, boot: int, rng, out: Path) -> dict:
    res = {}
    for win in [(0, 23), (6, 21)]:
        d = sample(h, t0, win)
        k = f"{win[0]:02d}-{win[1] + 1:02d}h"
        r = {"sample": {"unit_hours": int(len(d)), "units": int(d.u.nunique()), "clusters": int(d.cl.nunique()),
                        "share_hours_below_floor": round(float((d.y < d.lt0).mean()), 3)},
             "linear": linear(d), "floor": {}}
        for x in ["lq", "lz"]:
            for w, name in [(x, "NLS"), ("lc", "NIV citywide load")]:
                r["floor"][f"{name}, F = {'observed q' if x == 'lq' else 'assigned z'}"] = floor_fit(d, x, w, boot if win == (0, 23) else 0, rng)
                print(lab, k, x, name, r["floor"][f"{name}, F = {'observed q' if x == 'lq' else 'assigned z'}"], flush=True)
        res[k] = r
        (out / f"{lab}_progress.json").write_text(json.dumps(res, indent=2, ensure_ascii=False))
    return res


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-name", default="congestion_almagro_spec_v1")
    ap.add_argument("--boot", type=int, default=B)
    args = ap.parse_args(argv)
    out = ROOT / "outputs" / args.output_name
    if out.exists():
        raise SystemExit(f"{out} exists; choose a new --output-name")
    out.mkdir(parents=True)
    rng = np.random.default_rng(20261105)
    res = {"notes": {"T0": "mean pace 00-05 h, all sample days, >= 3 full hours",
                     "citywide_load": "sum of routed-OD z over all gate5 edges in the date-hour, own unit's z left out",
                     "dates": "20250519-20250525", "boot_reps": args.boot}}
    res["edge"] = level("edge", *edge_data(), args.boot, rng, out)
    res["segment"] = level("segment", *seg_data(), args.boot, rng, out)
    (out / "summary.json").write_text(json.dumps(res, indent=2, ensure_ascii=False))
    print(json.dumps(res, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
