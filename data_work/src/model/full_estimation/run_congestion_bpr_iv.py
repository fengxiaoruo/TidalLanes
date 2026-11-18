"""
BPR exponent by demand IV (nextstep A3 (b))
===========================================
The model's `bpr` congestion form is tau = f (1 + m_obs dev^b): only the exponent b is a
free parameter (the markup m_obs comes from the observed state). The 2026-11-04 value
b = 4.25 was a WLS fit to binned within-edge means, not an IV estimate. This estimates b
with the identification of the floor model in run_congestion_iv_followup.py:

    ln t = ln T0_u + ln(1 + exp(mu_u + delta_{date,hour} + b ln q)) + eps

i.e. t = T0 (1 + a_ut q^b) with a_ut = exp(mu_u + delta) absorbing a and the capacity
c^-b of BPR. T0_u = mean pace 00-05 h (as the floor model). Moments: sum eps = 0 within
every unit and every date x hour (mu, delta by alternating per-group bisection; the
softplus is increasing in its index so each group has a unique root), and
sum eps * w~ = 0 for b, with w~ = ln z (routed-OD flow) residualised on unit and
date x hour fixed effects (NIV-z, main), or w~ = ln q residualised the same way (NLS-like).
Also reported: the implied local elasticity d ln t / d ln q = b m / (1 + m), m = exp(index),
over all observations and over weekday 07-09 h, for comparison with the ce_floor beta.
Samples as run_congestion_almagro_spec.py (0519-0525; 06-22 h main, 00-24 h robustness;
edge and segment). No bootstrap. Inputs read only.
Output: data_work/outputs/<output-name>/ (refuses to overwrite)
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from run_all_traffic_speed_panel_gate5 import ROOT, within  # noqa: E402
import run_congestion_almagro_spec as al  # noqa: E402

GRID = np.round(np.arange(0.25, 10.01, 0.25), 3)


def softplus(v):
    return np.logaddexp(0.0, v)


class BPR:
    def __init__(self, d: pd.DataFrame, w: np.ndarray):
        self.cu, _ = pd.factorize(d.u)
        self.ct, _ = pd.factorize(d.date.astype(str) + "_" + d.hour.astype(str))
        self.y, self.x, self.f = d.y.to_numpy(), d.lq.to_numpy(), d.lt0.to_numpy()
        self.g_ = self.y - self.f                      # log markup target
        self.inv = np.log(np.expm1(np.clip(self.g_, 1e-4, None)))
        self.w = w
        self.Gu, self.Gt = self.cu.max() + 1, self.ct.max() + 1

    def _solve(self, code, G, base, iters=45):
        # m_g with sum_g (y - f - softplus(m_g + base)) = 0
        t = self.inv - base
        lo = np.full(G, np.inf); hi = np.full(G, -np.inf)
        np.minimum.at(lo, code, t); np.maximum.at(hi, code, t)
        lo, hi = lo - 2.0, hi + 2.0
        for _ in range(iters):
            m = 0.5 * (lo + hi)
            s = np.bincount(code, self.g_ - softplus(m[code] + base), minlength=G)
            pos = s > 0
            lo, hi = np.where(pos, m, lo), np.where(pos, hi, m)
        return 0.5 * (lo + hi)

    def fit(self, b):
        bx = b * self.x
        dl = np.zeros(self.Gt)
        for _ in range(8):
            mu = self._solve(self.cu, self.Gu, bx + dl[self.ct])
            dl = self._solve(self.ct, self.Gt, bx + mu[self.cu])
            dl -= dl.mean()
        mu = self._solve(self.cu, self.Gu, bx + dl[self.ct])
        idx = mu[self.cu] + dl[self.ct] + bx
        return self.g_ - softplus(idx), idx

    def g(self, b):
        e, _ = self.fit(b)
        return float(e @ self.w) / len(e)

    def solve(self):
        gv = np.array([self.g(b) for b in GRID])
        s = np.where(np.sign(gv[:-1]) != np.sign(gv[1:]))[0]
        if not len(s):
            return float("nan"), {"g_grid_min": float(gv.min()), "g_grid_max": float(gv.max())}
        a, c, ga = GRID[s[0]], GRID[s[0] + 1], gv[s[0]]
        for _ in range(20):
            mid = 0.5 * (a + c); gm = self.g(mid)
            if np.sign(gm) == np.sign(ga):
                a, ga = mid, gm
            else:
                c = mid
        bt = 0.5 * (a + c)
        e, idx = self.fit(bt)
        m = np.exp(idx)
        el = bt * m / (1 + m)
        return bt, {"n_roots_on_grid": int(len(s)), "markup_median": float(np.median(m)),
                    "local_elasticity_median": float(np.median(el)), "local_elasticity_mean": float(el.mean()),
                    "r2_log_time": float(1 - e.var() / self.y.var()), "_el": el}


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-name", default="congestion_bpr_iv_v1")
    args = ap.parse_args(argv)
    out = ROOT / "outputs" / args.output_name
    if out.exists():
        raise SystemExit(f"{out} exists; choose a new --output-name")
    out.mkdir(parents=True)
    res = {}
    for lab, loader in [("edge", al.edge_data), ("segment", al.seg_data)]:
        h, t0 = loader()
        res[lab] = {}
        for win in [(6, 21), (0, 23)]:
            d = al.sample(h, t0, win)
            k = f"{win[0]:02d}-{win[1] + 1:02d}h"
            r = within(d, ["lq", "lz"], [["u"], ["date", "hour"]])
            am = (pd.to_datetime(d.date.astype(str)).dt.dayofweek < 5).to_numpy() & d.hour.between(7, 8).to_numpy()
            o = {"n": int(len(d)), "units": int(d.u.nunique())}
            for nm, w in [("bpr_NIV_z_dateXhourFE", r.lz.to_numpy()), ("bpr_NLS_dateXhourFE", r.lq.to_numpy())]:
                bt, info = BPR(d, w).solve()
                el = info.pop("_el", None)
                if el is not None:
                    info["local_elasticity_median_weekday_07_09h"] = float(np.median(el[am]))
                o[nm] = {"b": round(bt, 4), **{q: round(v, 4) if isinstance(v, float) else v for q, v in info.items()}}
                print(lab, k, nm, o[nm], flush=True)
            res[lab][k] = o
            (out / "summary.json").write_text(json.dumps(res, indent=2, ensure_ascii=False, default=float))
    print("done")


if __name__ == "__main__":
    main()
