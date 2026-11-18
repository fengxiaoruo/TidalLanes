"""
Compare OSM grid-pair travel-time robustness variants with the main versions
===========================================================================
Main: osm_grid_travel_time_v2 (1A: relative speed, 3 hops, class x zone fallback;
1B: L = 300 m). Variants: outputs/osm_grid_rob_*_v1 (model-origin 3 km grid only).
On directed pairs present in both: correlation of log tm_AM, log tm_FF, log(AM/FF) and
the directional log ratio log(tm_AM(i->j) / tm_AM(j->i)); median ratio variant / main;
plus each variant's own summary (pairs, AM/FF, directional ratio).
Inputs read only. Output: data_work/outputs/<output-name>/ (refuses to overwrite)
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
MAIN = ROOT / "outputs/osm_grid_travel_time_v2"
GK = "sq3000_model_origin"
VARIANTS = [("1B L=600 m", "osm_grid_rob_1b_L600_v1", "osm1b_L600", "osm1b_L300"),
            ("1A (a) absolute speed", "osm_grid_rob_1a_absolute_v1", "osm1a", "osm1a"),
            ("1A (b) hops 1", "osm_grid_rob_1a_hops1_v1", "osm1a", "osm1a"),
            ("1A (b) hops 5", "osm_grid_rob_1a_hops5_v1", "osm1a", "osm1a"),
            ("1A (c) class-only fallback", "osm_grid_rob_1a_fallback_class_v1", "osm1a", "osm1a"),
            ("1A (d) class medians only", "osm_grid_rob_1a_classonly_v1", "osm1a", "osm1a")]
K = ["grid_from", "grid_to"]


def dirlog(P: pd.DataFrame) -> pd.Series:
    s = np.log(P.set_index(K).tm_AM)
    rev = s.reindex(pd.MultiIndex.from_arrays([s.index.get_level_values(1), s.index.get_level_values(0)])).to_numpy()
    return pd.Series(s.to_numpy() - rev, index=s.index)


def corr(a, b):
    ok = np.isfinite(a) & np.isfinite(b)
    return round(float(np.corrcoef(a[ok], b[ok])[0, 1]), 3)


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-name", default="osm_grid_robustness_compare_v1")
    args = ap.parse_args(argv)
    out = ROOT / "outputs" / args.output_name
    if out.exists():
        raise SystemExit(f"{out} exists; choose a new --output-name")
    out.mkdir(parents=True)
    rows = []
    for lab, d, sch, main_sch in VARIANTS:
        V = pd.read_parquet(ROOT / "outputs" / d / f"pairs_{sch}_{GK}.parquet")
        M = pd.read_parquet(MAIN / f"pairs_{main_sch}_{GK}.parquet")
        vs = json.loads((ROOT / "outputs" / d / "summary.json").read_text())
        m = M.merge(V, on=K, suffixes=("_m", "_v"))
        m = m[m.tm_AM_m.notna() & m.tm_AM_v.notna()]
        dv, dm = dirlog(V), dirlog(M)
        dd = pd.concat([dm.rename("m"), dv.rename("v")], axis=1, join="inner").dropna()
        rows.append({
            "variant": lab, "main": main_sch, "pairs_variant": int(V.tm_AM.notna().sum()), "pairs_main": int(M.tm_AM.notna().sum()),
            "pairs_both": int(len(m)),
            "corr_log_tm_AM": corr(np.log(m.tm_AM_m), np.log(m.tm_AM_v)),
            "corr_log_tm_FF": corr(np.log(m.tm_FF_m), np.log(m.tm_FF_v)),
            "corr_log_AM_over_FF": corr(np.log(m.tm_AM_m / m.tm_FF_m), np.log(m.tm_AM_v / m.tm_FF_v)),
            "corr_directional_log_ratio": corr(dd.m.to_numpy(), dd.v.to_numpy()),
            "sign_agree_directional": round(float((np.sign(dd.m) == np.sign(dd.v)).mean()), 3),
            "median_tm_AM_ratio_v_over_m": round(float((m.tm_AM_v / m.tm_AM_m).median()), 3),
            "AM_over_FF_median_v": round(float((V.tm_AM / V.tm_FF).median()), 3),
            "AM_over_FF_median_m": round(float((M.tm_AM / M.tm_FF).median()), 3),
            "dir_ratio_median_v": vs[sch][GK]["tm_AM_directional_ratio_median_min_over_max"],
            "corr_log_tm_AM_vs_gate5_v1": vs[sch][GK]["vs_gate5_v1"]["corr_log_tm_AM"],
            "lcc_km_share": vs[sch]["lcc_km_share"],
            "speed_sources_km_share": vs.get("arc_speed_sources_km_share_1B" if sch.startswith("osm1b") else "arc_speed_sources_km_share_1A"),
        })
    r = pd.DataFrame(rows)
    r.drop(columns="speed_sources_km_share").to_csv(out / "robustness_vs_main.csv", index=False)
    (out / "summary.json").write_text(json.dumps(rows, indent=2, ensure_ascii=False))
    with pd.option_context("display.width", 250, "display.max_columns", 30):
        print(r.drop(columns="speed_sources_km_share").T)


if __name__ == "__main__":
    main()
