"""Benchmark the grid jobs-housing Duncan D (=0.26):

  (A) RANDOMIZATION FLOOR on the study grid -- how far 0.26 sits above the
      finite-cell noise floor (jobs drawn ~ residents distribution).
  (B) CROSS-CITY RANK -- township-level jobs-housing Duncan D for the top-100
      cities (streamed from the 1.2 GB commuting dta), placing Beijing in the
      national distribution on consistent township units.

Run: python data_work/src/diagnostics/duncan_benchmark.py
"""
from __future__ import annotations

from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
GRID_CSV = ROOT / "outputs" / "beijing_commuting_direction_evidence_v1" / "data" / "grid_population_clusters_square.csv"
DTA = ROOT / "raw_data" / "commuting_town_2021.dta"
OUT_CSV = ROOT / "outputs" / "beijing_commuting_direction_evidence_v1" / "metrics" / "duncan_cross_city.csv"


def duncan(j: np.ndarray, r: np.ndarray) -> float:
    j = np.asarray(j, float); r = np.asarray(r, float)
    if j.sum() <= 0 or r.sum() <= 0:
        return np.nan
    return 0.5 * np.abs(j / j.sum() - r / r.sum()).sum()


# ---------------- (A) grid randomization floor ----------------
def grid_floor(n_sim: int = 400, seed: int = 20260702) -> dict:
    g = pd.read_csv(GRID_CSV)
    jobs = pd.to_numeric(g["jobs"], errors="coerce").fillna(0).clip(lower=0).to_numpy()
    res = pd.to_numeric(g["residents"], errors="coerce").fillna(0).clip(lower=0).to_numpy()
    d_obs = duncan(jobs, res)

    rng = np.random.default_rng(seed)
    p = res / res.sum()
    J = int(round(jobs.sum()))
    sims = np.empty(n_sim)
    for k in range(n_sim):
        jsim = rng.multinomial(J, p)  # jobs located as if drawn from residents
        sims[k] = duncan(jsim, res)
    return {
        "d_obs": d_obs,
        "n_cells": int((res + jobs > 0).sum()),
        "total_jobs": J,
        "floor_mean": float(sims.mean()),
        "floor_p95": float(np.quantile(sims, 0.95)),
        "excess_over_floor": float(d_obs - sims.mean()),
        "sd_above_floor": float((d_obs - sims.mean()) / sims.std(ddof=1)),
    }


# ---------------- (B) cross-city township D ----------------
def cross_city(top_n: int = 100) -> pd.DataFrame:
    res_acc: dict = defaultdict(float)   # (city, town) -> residents (origin mass)
    job_acc: dict = defaultdict(float)   # (city, town) -> jobs (dest mass)
    city_tot: dict = defaultdict(float)
    cols = ["origin_prov_name", "dest_prov_name", "origin_city_name", "dest_city_name",
            "origin_town_code", "dest_town_code", "commuters_2021"]
    reader = pd.read_stata(DTA, columns=cols, chunksize=400_000)
    for chunk in reader:
        m = (chunk["origin_prov_name"] == chunk["dest_prov_name"]) & (
            chunk["origin_city_name"] == chunk["dest_city_name"])
        c = chunk.loc[m]
        if c.empty:
            continue
        city = c["origin_prov_name"].str.strip() + "||" + c["origin_city_name"].str.strip()
        w = pd.to_numeric(c["commuters_2021"], errors="coerce").fillna(0.0)
        ot = c["origin_town_code"].astype(str)
        dt = c["dest_town_code"].astype(str)
        for key, val in w.groupby([city, ot]).sum().items():
            res_acc[key] += float(val)
        for key, val in w.groupby([city, dt]).sum().items():
            job_acc[key] += float(val)
        for key, val in w.groupby(city).sum().items():
            city_tot[key] += float(val)

    # assemble per-city township vectors
    towns_by_city: dict = defaultdict(set)
    for (city, town) in res_acc:
        towns_by_city[city].add(town)
    for (city, town) in job_acc:
        towns_by_city[city].add(town)

    rows = []
    for city, towns in towns_by_city.items():
        towns = sorted(towns)
        r = np.array([res_acc.get((city, t), 0.0) for t in towns])
        j = np.array([job_acc.get((city, t), 0.0) for t in towns])
        rows.append({
            "city_key": city,
            "n_townships": len(towns),
            "total_commuters": city_tot.get(city, 0.0),
            "duncan_D": duncan(j, r),
        })
    df = pd.DataFrame(rows)
    df = df[(df["n_townships"] >= 10)].sort_values("total_commuters", ascending=False).head(top_n).reset_index(drop=True)
    df = df.sort_values("duncan_D", ascending=False).reset_index(drop=True)
    df["rank"] = np.arange(1, len(df) + 1)
    return df


def main() -> None:
    print("=" * 60)
    print("(A) GRID RANDOMIZATION FLOOR for D_obs")
    fl = grid_floor()
    print(f"  D_obs (grid)            = {fl['d_obs']:.3f}   ({fl['n_cells']} cells, {fl['total_jobs']:,} jobs)")
    print(f"  null floor mean / p95   = {fl['floor_mean']:.3f} / {fl['floor_p95']:.3f}")
    print(f"  excess over floor       = {fl['excess_over_floor']:.3f}  ({fl['sd_above_floor']:.0f} SD above)")

    print("\n(B) CROSS-CITY TOWNSHIP-LEVEL D (top-100 by commuters)")
    df = cross_city()
    df.to_csv(OUT_CSV, index=False)
    bj = df[df["city_key"].str.contains("北京")]
    med = df["duncan_D"].median()
    print(f"  cities ranked           = {len(df)}")
    print(f"  D  min/median/max       = {df['duncan_D'].min():.3f} / {med:.3f} / {df['duncan_D'].max():.3f}")
    if len(bj):
        b = bj.iloc[0]
        pct = 100 * (df["duncan_D"] < b["duncan_D"]).mean()
        print(f"  BEIJING township D      = {b['duncan_D']:.3f}  -> rank {int(b['rank'])}/{len(df)}  ({pct:.0f}th pctile, {b['n_townships']} townships)")
    print("  top 6 cities by D:")
    for _, x in df.head(6).iterrows():
        print(f"    {int(x['rank']):>2}. {x['city_key']:<24} D={x['duncan_D']:.3f}  ({int(x['n_townships'])} twn)")
    print(f"\nwrote {OUT_CSV}")


if __name__ == "__main__":
    main()
