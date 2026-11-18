"""Jobs-housing mismatch metrics for the "Directional Asymmetry" slide.

Replaces the near-tautological "64.8% of commuters end closer to employment
clusters" statistic with two non-circular, canonical measures:

  (1) SEPARATION  : Duncan dissimilarity index D between the jobs and residents
                    distributions across grid cells (0 = balanced, 1 = fully
                    segregated), plus top-decile concentration and the
                    job- vs residence-weighted centroid separation.
  (2) DIRECTION   : radial AM inbound-vs-reverse asymmetry around the
                    job-weighted city center (the actual tidal signal).

Reads only CSVs already produced by
generate_beijing_commuting_direction_evidence.py -> no parquet/geo deps.

Run:  python data_work/src/diagnostics/jobs_housing_mismatch_metrics.py
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]  # .../data_work
EVID = ROOT / "outputs" / "beijing_commuting_direction_evidence_v1"
GRID_CSV = EVID / "data" / "grid_population_clusters_square.csv"
SECTOR_CSV = EVID / "metrics" / "inbound_sector_summary.csv"
OUT_JSON = EVID / "metrics" / "jobs_housing_mismatch_metrics.json"


def separation_metrics(grid: pd.DataFrame) -> dict:
    jobs = pd.to_numeric(grid["jobs"], errors="coerce").fillna(0.0).clip(lower=0)
    res = pd.to_numeric(grid["residents"], errors="coerce").fillna(0.0).clip(lower=0)
    J, R = jobs.sum(), res.sum()
    sj, sr = jobs / J, res / R

    # Duncan dissimilarity index
    D = 0.5 * np.abs(sj - sr).sum()

    # top-decile job concentration
    thr = jobs.quantile(0.90)
    top = jobs >= thr
    jobs_in_top = jobs[top].sum() / J
    res_in_top = res[top].sum() / R

    # job- vs residence-weighted centroid separation (km)
    x, y = pd.to_numeric(grid["x_m"], errors="coerce"), pd.to_numeric(grid["y_m"], errors="coerce")
    jcx, jcy = np.average(x, weights=jobs), np.average(y, weights=jobs)
    rcx, rcy = np.average(x, weights=res), np.average(y, weights=res)
    centroid_sep_km = float(np.hypot(jcx - rcx, jcy - rcy) / 1000.0)

    return {
        "n_grids": int(len(grid)),
        "duncan_dissimilarity_D": float(D),
        "top10pct_job_grids_share_of_jobs": float(jobs_in_top),
        "top10pct_job_grids_share_of_residents": float(res_in_top),
        "job_vs_residence_centroid_separation_km": centroid_sep_km,
    }


def direction_metrics(sector: pd.DataFrame) -> dict:
    inb = sector["inbound_flow"].sum()
    rev = sector["reverse_am_flow"].sum()
    share = sector["inbound_share_of_two_way"]
    return {
        "inner_km_work_core": float(sector["inner_km"].iloc[0]),
        "outer_km_home_ring": float(sector["outer_km"].iloc[0]),
        "n_sectors": int(len(sector)),
        "pooled_inbound_share_of_two_way": float(inb / (inb + rev)),
        "pooled_inbound_reverse_ratio": float(inb / rev),
        "n_sectors_inbound_share_gt_0.7": int((share > 0.7).sum()),
        "sector_inbound_share_min": float(share.min()),
        "sector_inbound_share_median": float(share.median()),
        "sector_inbound_share_max": float(share.max()),
    }


def main() -> None:
    grid = pd.read_csv(GRID_CSV)
    sector = pd.read_csv(SECTOR_CSV)

    sep = separation_metrics(grid)
    dirm = direction_metrics(sector)
    out = {"separation": sep, "direction": dirm}
    OUT_JSON.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")

    print("=" * 62)
    print("JOBS-HOUSING MISMATCH METRICS  (car+bus commuters, Beijing grid)")
    print("=" * 62)
    print("\n[1] SEPARATION (non-circular)")
    print(f"  Duncan dissimilarity index D          = {sep['duncan_dissimilarity_D']:.3f}")
    print(f"  Top-10% job-dense grids hold           : {sep['top10pct_job_grids_share_of_jobs']*100:.1f}% of jobs")
    print(f"                              vs only     : {sep['top10pct_job_grids_share_of_residents']*100:.1f}% of residents")
    print(f"  Job- vs residence-centroid separation = {sep['job_vs_residence_centroid_separation_km']:.1f} km")
    print(f"  (n grids = {sep['n_grids']})")
    print("\n[2] DIRECTION  (radial inbound vs reverse, tidal signal)")
    print(f"  Core work <= {dirm['inner_km_work_core']:.0f} km, home ring >= {dirm['outer_km_home_ring']:.0f} km from job-weighted center")
    print(f"  Pooled AM inbound share of two-way    = {dirm['pooled_inbound_share_of_two_way']*100:.1f}%")
    print(f"  Pooled inbound/reverse ratio          = {dirm['pooled_inbound_reverse_ratio']:.2f}x")
    print(f"  Sectors with inbound share > 0.70     = {dirm['n_sectors_inbound_share_gt_0.7']}/{dirm['n_sectors']}")
    print(f"  Sector inbound share min/median/max   = {dirm['sector_inbound_share_min']:.2f} / {dirm['sector_inbound_share_median']:.2f} / {dirm['sector_inbound_share_max']:.2f}")
    print(f"\nwrote {OUT_JSON}")


if __name__ == "__main__":
    main()
