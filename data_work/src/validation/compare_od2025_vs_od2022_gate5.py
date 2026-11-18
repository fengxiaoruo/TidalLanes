"""
2025-05 platform OD vs the 2022-11 commuting OD on the gate5 grid
=================================================================

Both bundles are read only:
    od2022: model_input_edgeproj_v2_lcc_gate5_v1        (commute_202211.csv)
        pop            = home-work people, all modes
        commuters_car  = people whose identified mode is car (type_car)
    od2025: model_input_edgeproj_v2_lcc_gate5_od2025_v2 (platform 202505_v01, strict)
        hw_people_month = monthly joint home-work SIMs, all modes
        car_am_people   = SIMs with an AM-peak (07-09) HW mode1 trip on any of 19 weekdays
        car_am_trips_day= those trips per weekday
2025 cells below 5 SIMs were suppressed on export, so 2025 marginals are lower
bounds and the OD comparison is on pairs present in both.

Output: data_work/outputs/<output-name>/ (refuses to overwrite)
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

ROOT = Path(__file__).resolve().parents[2]   # -> data_work/
B22 = ROOT / "outputs/model_input_edgeproj_v2_lcc_gate5_v1/data"
B25 = ROOT / "outputs/model_input_edgeproj_v2_lcc_gate5_od2025_v2/data"


def rc(s: pd.Series) -> pd.DataFrame:
    return s.str.extract(r"sq_(\d+)_(\d+)").astype(float)


def dist_km(o: pd.Series, d: pd.Series) -> np.ndarray:
    a, b = rc(o), rc(d)
    return 3.0 * np.hypot(a[0].values - b[0].values, a[1].values - b[1].values)


def wq(x: np.ndarray, w: np.ndarray, q: list[float]) -> list[float]:
    i = np.argsort(x)
    c = np.cumsum(w[i]) / w.sum()
    return [round(float(x[i][np.searchsorted(c, p)]), 2) for p in q]


def corr(a: pd.Series, b: pd.Series) -> dict:
    m = (a > 0) & (b > 0)
    return {"n": int(m.sum()), "pearson_log": round(float(np.corrcoef(np.log(a[m]), np.log(b[m]))[0, 1]), 4),
            "spearman": round(float(spearmanr(a[m], b[m]).correlation), 4)}


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--output-name", default="od2025_vs_od2022_gate5_v1")
    args = p.parse_args(argv)
    out = ROOT / "outputs" / args.output_name
    if out.exists():
        raise SystemExit(f"{out} exists; choose a new --output-name")
    out.mkdir(parents=True)

    k = ["home_grid", "work_grid"]
    a = pd.read_parquet(B22 / "qsm_input_od_square.parquet", columns=k + ["pop", "commuters_car"])
    b = pd.read_parquet(B25 / "qsm_input_od_square.parquet", columns=k + ["hw_people_month", "car_am_people", "car_am_trips_day"])
    od = a.merge(b, on=k, how="outer").fillna(0.0)
    od["dist_km"] = dist_km(od.home_grid, od.work_grid)
    od["intra"] = od.home_grid.eq(od.work_grid)
    cols = {"2022_all_pop": "pop", "2022_car": "commuters_car", "2025_all_hw_month": "hw_people_month",
            "2025_car_am_people": "car_am_people", "2025_car_am_trips_day": "car_am_trips_day"}
    res: dict = {"totals_on_gate5": {lab: round(float(od[c].sum())) for lab, c in cols.items()},
                 "od_pairs_nonzero": {lab: int((od[c] > 0).sum()) for lab, c in cols.items()}}

    res["distance_km"] = {}
    for lab, c in cols.items():
        w = od[c].to_numpy()
        res["distance_km"][lab] = {"intrazonal_share": round(float(w[od.intra].sum() / w.sum()), 4),
                                   "mean_inter": round(float((od.dist_km * w)[~od.intra].sum() / w[~od.intra].sum()), 2),
                                   "p25_p50_p75_p90_all": wq(od.dist_km.to_numpy(), w, [.25, .5, .75, .9]),
                                   "share_gt_10km": round(float(w[od.dist_km > 10].sum() / w.sum()), 4)}

    home = od.groupby("home_grid")[list(cols.values())].sum()
    work = od.groupby("work_grid")[list(cols.values())].sum()
    res["marginals"] = {
        "home_all_2022_vs_all_2025": corr(home["pop"], home.hw_people_month),
        "work_all_2022_vs_all_2025": corr(work["pop"], work.hw_people_month),
        "home_car_2022_vs_car_am_2025": corr(home.commuters_car, home.car_am_people),
        "work_car_2022_vs_car_am_2025": corr(work.commuters_car, work.car_am_people),
    }
    for side, m in [("home", home), ("work", work)]:
        t22, t25 = set(m["pop"].nlargest(50).index), set(m.hw_people_month.nlargest(50).index)
        res["marginals"][f"{side}_top50_overlap_all"] = len(t22 & t25)
        s22, s25 = m["pop"] / m["pop"].sum(), m.hw_people_month / m.hw_people_month.sum()
        res["marginals"][f"{side}_share_abs_diff_half_sum_all"] = round(float((s22 - s25).abs().sum() / 2), 4)
    # job centre of mass and spread (cell units -> km)
    for side, m in [("home", home), ("work", work)]:
        x = rc(m.index.to_series())
        for lab, c in [("2022", "pop"), ("2025", "hw_people_month")]:
            w = m[c].to_numpy()
            cy, cx = (x[0] * w).sum() / w.sum(), (x[1] * w).sum() / w.sum()
            sd = 3.0 * np.sqrt((((x[0] - cy) ** 2 + (x[1] - cx) ** 2) * w).sum() / w.sum())
            res.setdefault("spatial_spread_km", {})[f"{side}_{lab}"] = round(float(sd), 2)

    inter = od[~od.intra]
    res["od_level_inter_cell"] = {
        "all_2022_vs_all_2025": corr(inter["pop"], inter.hw_people_month),
        "car_2022_vs_car_am_2025": corr(inter.commuters_car, inter.car_am_people),
        "share_2022_pop_on_pairs_present_2025": round(float(inter["pop"][inter.hw_people_month > 0].sum() / inter["pop"].sum()), 4),
        "share_2025_hw_on_pairs_present_2022": round(float(inter.hw_people_month[inter["pop"] > 0].sum() / inter.hw_people_month.sum()), 4),
    }
    # car share by home node, both years
    cs = pd.DataFrame({"car22": home.commuters_car / home["pop"], "car25": home.car_am_people / home.hw_people_month,
                       "w": home["pop"]}).replace([np.inf], np.nan).dropna()
    cs = cs[(home.loc[cs.index, "pop"] >= 200) & (home.loc[cs.index, "hw_people_month"] >= 200)]
    res["car_share_by_home_node"] = {
        "city_2022_car_over_pop": round(float(home.commuters_car.sum() / home["pop"].sum()), 4),
        "city_2025_car_am_over_hw_month": round(float(home.car_am_people.sum() / home.hw_people_month.sum()), 4),
        "corr_across_home_nodes": round(float(np.corrcoef(cs.car22, cs.car25)[0, 1]), 4), "nodes": int(len(cs)),
        "note": "2022 car = identified mode share of all people (identified only for ~5% of pop); 2025 = AM-peak HW mode1 SIMs over monthly HW SIMs; definitions differ",
    }
    home.join(work, lsuffix="_home", rsuffix="_work", how="outer").to_csv(out / "node_marginals_2022_2025.csv")
    (out / "od2025_vs_od2022_summary.json").write_text(json.dumps(res, indent=2, ensure_ascii=False))
    print(json.dumps(res, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
