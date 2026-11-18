"""
IV follow-ups for the congestion function (nextstep A2 tests, A3 (a) remainder)
==============================================================================

1 Floor + constant elasticity with our identification:
      ln t = max{ ln T0_u , mu_u + delta_{date,hour} + beta ln q } + eps
  T0_u = mean pace 00-05 h; moments: sum eps = 0 within every unit and within every
  date x hour (mu and delta by alternating per-group bisection), and
  sum eps * w~ = 0 for beta, with w~ = ln z (routed-OD flow) residualised on unit and
  date x hour fixed effects (NIV-z), or w~ = ln q residualised the same way (NLS-like).
  Directly comparable with the linear V1 / S1 (unit FE + date x hour FE, z as IV).
2 First stage by load: within V1/S1 residuals, mean residual ln q and ln z by decile of
  the unit's relative routed demand z / z95; local first-stage slope between deciles.
Samples as run_congestion_almagro_spec.py (0519-0525, 00-24 h and 06-22 h; edge and
segment). Inputs read only. Output: data_work/outputs/<output-name>/ (refuses to overwrite)
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

GRID = np.round(np.arange(0.0, 0.801, 0.02), 3)


class Floor2:
    def __init__(self, d: pd.DataFrame, w: np.ndarray):
        self.cu, _ = pd.factorize(d.u)
        self.ct, _ = pd.factorize(d.date.astype(str) + "_" + d.hour.astype(str))
        self.y, self.x, self.f = d.y.to_numpy(), d.lq.to_numpy(), d.lt0.to_numpy()
        self.w = w
        self.Gu, self.Gt = self.cu.max() + 1, self.ct.max() + 1

    def _solve(self, code, G, base, iters=30):
        # find m_g with sum_g (y - max(f, m_g + base)) = 0
        cnt = np.bincount(code, minlength=G)
        lo = np.bincount(code, self.f - base, minlength=G) / cnt - 3.0
        hi = np.bincount(code, self.y - base, minlength=G) / cnt + 0.5
        lo = np.minimum(lo, hi - 0.5)
        for _ in range(iters):
            m = 0.5 * (lo + hi)
            s = np.bincount(code, self.y - np.maximum(self.f, m[code] + base), minlength=G)
            pos = s > 0
            lo, hi = np.where(pos, m, lo), np.where(pos, hi, m)
        return 0.5 * (lo + hi)

    def resid(self, beta):
        bx = beta * self.x
        dl = np.zeros(self.Gt)
        for _ in range(6):
            mu = self._solve(self.cu, self.Gu, bx + dl[self.ct])
            dl = self._solve(self.ct, self.Gt, bx + mu[self.cu])
            dl -= dl.mean()
        mu = self._solve(self.cu, self.Gu, bx + dl[self.ct])
        idx = mu[self.cu] + dl[self.ct] + bx
        return self.y - np.maximum(self.f, idx), idx > self.f

    def g(self, beta):
        e, _ = self.resid(beta)
        return float(e @ self.w) / len(e)

    def solve(self):
        gv = np.array([self.g(b) for b in GRID])
        s = np.where(np.sign(gv[:-1]) != np.sign(gv[1:]))[0]
        if not len(s):
            return float("nan"), None
        a, b, ga = GRID[s[0]], GRID[s[0] + 1], gv[s[0]]
        for _ in range(18):
            c = 0.5 * (a + b); gc = self.g(c)
            if np.sign(gc) == np.sign(ga):
                a, ga = c, gc
            else:
                b = c
        bt = 0.5 * (a + b)
        return bt, float(self.resid(bt)[1].mean())


def first_stage_by_load(d: pd.DataFrame) -> pd.DataFrame:
    r = within(d, ["lq", "lz"], [["u"], ["date", "hour"]])
    zr = d.z / d.groupby("u").z.transform(lambda s: s.quantile(0.95))
    b = pd.qcut(zr, 10, labels=False, duplicates="drop")
    g = pd.DataFrame({"bin": b, "rq": r.lq, "rz": r.lz, "zr": zr}).groupby("bin").agg(n=("rq", "size"), zr_med=("zr", "median"), rq=("rq", "mean"), rz=("rz", "mean"))
    g["local_slope"] = g.rq.diff() / g.rz.diff()
    return g.round(4)


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-name", default="congestion_iv_followup_v1")
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
            o = {"n": int(len(d))}
            for nm, w in [("floor_NIV_z_dateXhourFE", r.lz.to_numpy()), ("floor_NLS_dateXhourFE", r.lq.to_numpy())]:
                bt, act = Floor2(d, w).solve()
                o[nm] = {"beta": round(bt, 4), "active_share": round(act, 3) if act is not None else None}
                print(lab, k, nm, o[nm], flush=True)
            fs = first_stage_by_load(d)
            fs.to_csv(out / f"{lab}_{k}_first_stage_by_z_decile.csv")
            o["first_stage_by_z_decile"] = {str(i): {"zr_med": row.zr_med, "rq": row.rq, "rz": row.rz, "local_slope": row.local_slope} for i, row in fs.iterrows()}
            res[lab][k] = o
            (out / "summary.json").write_text(json.dumps(res, indent=2, ensure_ascii=False, default=float))
    print("done")


if __name__ == "__main__":
    main()
