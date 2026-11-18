"""
All-purpose traffic, commuter share and speed-flow panel (gate5, 2025-05-19..25)
================================================================================

Inputs (written by build_speed_edge_10min_gate5.py and build_rn_all_edge_10min_gate5.py
into the same output directory):
    speed/edge_pace_10min.parquet   edge x date x slot10 pace index (min/km)
    rn_all/edge_flow_10min.parquet  edge x date x slot10 all-purpose mode1 length flow
plus the project's own strict commuter exports (RDW per date x window, RS30
pooled 0519-0523 x 30 min), allocated to edges by the same length rule.

Checks, all descriptive or reduced-form:
    1. commuter share s_C = commuter flow / all-purpose flow, AM and PM windows;
    2. clock check: city-wide flow and pace profiles and their best lag;
    3. panel elasticity of pace w.r.t. flow per lane with edge and time effects;
    4. hypercongestion: does flow fall while pace keeps rising at the peak;
    5. D3 (within-edge AM vs PM) with all-purpose flow vs commuter flow.

Output: data_work/outputs/<output-name>/analysis/ (refuses to overwrite)
"""
from __future__ import annotations

import argparse
import glob
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]   # -> data_work/
REPO = ROOT.parent
sys.path.insert(0, str(REPO / "Platform" / "utils"))
import map_rn_to_model_edges as rnmap      # noqa: E402

QR = REPO / "Platform/runs/202505_v01/query-results"
EDGES = ROOT / "outputs/model_input_edgeproj_v2_lcc_gate5_v1/data/qsm_input_edges_square.parquet"
WEEKDAYS = [20250519, 20250520, 20250521, 20250522, 20250523]
AM_SLOTS, PM_SLOTS = range(42, 54), range(102, 114)       # 07:00-09:00, 17:00-19:00
MIN_COVERAGE = 0.8


def within(df: pd.DataFrame, cols: list[str], fes: list[list[str]], iters: int = 30) -> pd.DataFrame:
    """Residualise cols on several sets of fixed effects by alternating projections."""
    r = df[cols].astype(float).copy()
    keys = [df[f].astype(str).agg("|".join, axis=1) if len(f) > 1 else df[f[0]] for f in fes]
    for _ in range(iters):
        for k in keys:
            r -= r.groupby(k.values).transform("mean")
    return r


def slope(y, x, cluster, z=None) -> dict:
    """OLS (or just-identified IV with z) of residualised y on x; SE clustered by `cluster`."""
    y, x = np.asarray(y, float), np.asarray(x, float)
    z = x if z is None else np.asarray(z, float)
    b = (z @ y) / (z @ x)
    u = y - b * x
    g = pd.Series(z * u).groupby(np.asarray(cluster)).sum().to_numpy()
    se = float(np.sqrt((g ** 2).sum()) / abs(z @ x))
    out = {"b": round(float(b), 4), "se_cluster_edge": round(se, 4), "n": int(len(y)), "n_clusters": int(len(g))}
    if z is not x:
        pi = (z @ x) / (z @ z)
        v = x - pi * z
        gf = pd.Series(z * v).groupby(np.asarray(cluster)).sum().to_numpy()
        out["first_stage_F_cluster"] = round(float((pi / (np.sqrt((gf ** 2).sum()) / (z @ z))) ** 2), 1)
    return out


def commuter_edges(meta: dict, edge_key: set) -> tuple[pd.DataFrame, pd.DataFrame]:
    nodes = pd.concat([pd.read_csv(f, dtype={"rn_id": str}) for f in sorted(rnmap.SPEED_DICT.glob("rn_node_dictionary_202505_b*.csv"))])
    nodes = nodes[nodes.coordinate_missing.eq(0)].drop_duplicates("rn_id").set_index("rn_id")
    rdw = pd.concat([pd.read_csv(f, dtype={"rn_pair_id": str}) for f in sorted(glob.glob(str(QR / "TL25M05_V01_04*_RDW_*.csv")))])
    s30 = pd.concat([pd.read_csv(f, dtype={"rn_pair_id": str}) for f in sorted(glob.glob(str(QR / "TL25M05_V01_04*_RS30_b*.csv")))])
    pr = pd.DataFrame({"rn_pair_id": pd.unique(np.concatenate([rdw.rn_pair_id.unique(), s30.rn_pair_id.unique()]))})
    pr[["rn_from", "rn_to"]] = pr.rn_pair_id.str.split("_", n=1, expand=True)
    for end in ["from", "to"]:
        n = nodes.reindex(pr[f"rn_{end}"])
        pr[f"lon_{end}"], pr[f"lat_{end}"] = n.lon.values, n.lat.values
    al = rnmap.length_alloc(pr, meta["x_origin_3857"], meta["y_origin_3857"], edge_key)
    al = al[al.allocated][["rn_pair_id", "grid_from", "grid_to", "cells"]]
    d = rdw.merge(al, on="rn_pair_id").assign(flow=lambda t: t.n_pass * t.cells)
    d = d.groupby(["grid_from", "grid_to", "date", "purpose"]).flow.sum().unstack("purpose").fillna(0).reset_index()
    s = s30.merge(al, on="rn_pair_id").assign(flow=lambda t: t.n_pass * t.cells)
    s = s.groupby(["grid_from", "grid_to", "purpose", "pass_slot30"]).flow.sum().reset_index()
    return d, s


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--output-name", default="all_traffic_speed_panel_rn202505_gate5_v1")
    args = p.parse_args(argv)
    base = ROOT / "outputs" / args.output_name
    out = base / "analysis"
    if out.exists():
        raise SystemExit(f"{out} exists; choose a new --output-name")
    out.mkdir(parents=True)
    res: dict = {}

    meta = json.loads((REPO / "Platform/reference_inputs/manifest.json").read_text())
    edges = pd.read_parquet(EDGES, columns=["grid_o", "grid_d", "lanes_directional"]).rename(columns={"grid_o": "grid_from", "grid_d": "grid_to"})
    edges["n_kl"] = edges.lanes_directional.where(edges.lanes_directional > 0, edges.lanes_directional.mean())
    edge_key = set(zip(edges.grid_from, edges.grid_to))
    pace = pd.read_parquet(base / "speed/edge_pace_10min.parquet")
    flow = pd.read_parquet(base / "rn_all/edge_flow_10min.parquet")
    k = ["grid_from", "grid_to", "date", "slot10"]
    t10 = pace.merge(flow, on=k, how="left").fillna({"len_flow_all": 0.0})

    # 1. commuter share
    cd, cs = commuter_edges(meta, edge_key)
    wk = flow[flow.date.isin(WEEKDAYS)]
    am = wk[wk.slot10.isin(AM_SLOTS)].groupby(["grid_from", "grid_to", "date"]).len_flow_all.sum().rename("all_am")
    pm = wk[wk.slot10.isin(PM_SLOTS)].groupby(["grid_from", "grid_to", "date"]).len_flow_all.sum().rename("all_pm")
    sh = cd.merge(am.reset_index(), on=["grid_from", "grid_to", "date"], how="outer").merge(pm.reset_index(), on=["grid_from", "grid_to", "date"], how="outer").fillna(0)
    e5 = sh.groupby(["grid_from", "grid_to"])[["HW", "WH", "all_am", "all_pm"]].sum().reset_index()
    e5 = e5[e5.set_index(["grid_from", "grid_to"]).index.isin(list(edge_key))]
    e5["sC_am"] = e5.HW / e5.all_am.replace(0, np.nan)
    e5["sC_pm"] = e5.WH / e5.all_pm.replace(0, np.nan)
    e5.to_csv(out / "edge_commuter_share_weekday.csv", index=False)
    q = lambda s: [round(float(x), 3) for x in s.dropna().quantile([.1, .25, .5, .75, .9])]
    res["commuter_share"] = {
        "city_am_HW_over_all": round(float(e5.HW.sum() / e5.all_am.sum()), 4),
        "city_pm_WH_over_all": round(float(e5.WH.sum() / e5.all_pm.sum()), 4),
        "edge_am_p10_p25_p50_p75_p90": q(e5.sC_am[e5.all_am >= 100]),
        "edge_pm_p10_p25_p50_p75_p90": q(e5.sC_pm[e5.all_pm >= 100]),
        "edges_with_sC_am_gt_1": int((e5.sC_am > 1).sum()),
        "corr_log_all_am_vs_log_HW": round(float(np.corrcoef(np.log(e5.all_am[(e5.HW > 0) & (e5.all_am > 0)]), np.log(e5.HW[(e5.HW > 0) & (e5.all_am > 0)]))[0, 1]), 4),
        "note": "commuter = strict joint home-work SIMs (HW in AM, WH in PM); pairs with <5 SIMs suppressed on export, so s_C is a lower bound",
    }
    wk30 = wk.assign(pass_slot30=wk.slot10 // 3).groupby("pass_slot30").len_flow_all.sum()
    c30 = cs[cs.set_index(["grid_from", "grid_to"]).index.isin(list(edge_key))].groupby(["pass_slot30", "purpose"]).flow.sum().unstack("purpose").fillna(0)
    prof = pd.concat([wk30.rename("all"), c30], axis=1).fillna(0)
    prof["share_HW"], prof["share_WH"] = prof.HW / prof["all"], prof.WH / prof["all"]
    prof.to_csv(out / "city_commuter_share_by_slot30_weekday.csv")
    res["commuter_share"]["slot30_peak_share_HW"] = round(float(prof.share_HW.max()), 4)
    res["commuter_share"]["slot30_peak_share_HW_at"] = f"{int(prof.share_HW.idxmax()) // 2:02d}:{int(prof.share_HW.idxmax()) % 2 * 30:02d}"

    # 2. clock check (weekdays, city-wide)
    w10 = t10[t10.date.isin(WEEKDAYS) & (t10.coverage >= MIN_COVERAGE)]
    cf = w10.groupby("slot10").len_flow_all.sum()
    cp = w10.groupby("slot10").pace_min_per_km.mean()
    lags = {}
    for L in range(-12, 13):
        a, b = np.log(cf.loc[30:132]), np.log(cp.shift(-L).loc[30:132])
        lags[L] = float(np.corrcoef(a, b)[0, 1])
    best = max(lags, key=lags.get)
    hhmm = lambda s: f"{int(s) // 6:02d}:{int(s) % 6 * 10:02d}"
    res["clock_check"] = {"flow_peak_am": hhmm(cf.loc[30:72].idxmax()), "flow_peak_pm": hhmm(cf.loc[96:132].idxmax()),
                          "pace_peak_am": hhmm(cp.loc[30:72].idxmax()), "pace_peak_pm": hhmm(cp.loc[96:132].idxmax()),
                          "best_lag_slots_pace_after_flow": int(best), "corr_at_best": round(lags[best], 4), "corr_at_0": round(lags[0], 4)}
    pd.DataFrame({"flow_all": cf, "pace_mean": cp}).to_csv(out / "city_profile_10min_weekday.csv")

    # 3. panel, hourly cells 06:00-22:00
    t10["hour"] = t10.slot10 // 6
    h = t10[(t10.coverage >= MIN_COVERAGE)].groupby(["grid_from", "grid_to", "date", "hour"]).agg(
        pace=("pace_min_per_km", "mean"), flow=("len_flow_all", "sum"), n_slots=("slot10", "size")).reset_index()
    h = h[(h.n_slots == 6) & h.hour.between(6, 21) & (h.flow >= 5)].merge(edges[["grid_from", "grid_to", "n_kl"]], on=["grid_from", "grid_to"])
    h["edge"] = h.grid_from + ">" + h.grid_to
    h["y"], h["x"] = np.log(h.pace), np.log(h.flow / h.n_kl)
    h["weekday"] = h.date.isin(WEEKDAYS)
    loo = h.groupby(["edge", "hour"]).x.transform(lambda s: (s.sum() - s) / (len(s) - 1) if len(s) > 1 else np.nan)
    h["z_loo"] = loo
    panel = {}
    r = within(h, ["y", "x"], [["edge"], ["date", "hour"]])
    panel["P1_edge_FE+date_x_hour_FE"] = slope(r.y, r.x, h.edge)
    hz = h.dropna(subset=["z_loo"])
    rz = within(hz, ["y", "x", "z_loo"], [["edge"], ["date", "hour"]])
    panel["P1_IV_leave_one_day_out_same_edge_hour"] = slope(rz.y, rz.x, hz.edge, z=rz.z_loo)
    r = within(h, ["y", "x"], [["edge", "hour"], ["date", "hour"]])
    panel["P2_edge_x_hour_FE+date_x_hour_FE (day-to-day incl. weekend)"] = slope(r.y, r.x, h.edge)
    hw = h[h.weekday]
    r = within(hw, ["y", "x"], [["edge", "hour"], ["date", "hour"]])
    panel["P3_as_P2_weekdays_only"] = slope(r.y, r.x, hw.edge)
    hp = h[h.weekday & h.hour.isin([7, 8, 17, 18])]
    r = within(hp, ["y", "x"], [["edge"], ["date", "hour"]])
    panel["P4_weekday_peak_hours_edge_FE+date_x_hour_FE"] = slope(r.y, r.x, hp.edge)
    res["panel_hourly"] = panel
    res["panel_hourly"]["sample"] = {"edge_hours": int(len(h)), "edges": int(h.edge.nunique()), "hours": "06:00-22:00", "min_coverage": MIN_COVERAGE}

    # 4. hypercongestion, weekday edge x hour means
    m = h[h.weekday].groupby(["edge", "hour"]).agg(pace=("pace", "mean"), flow=("flow", "mean")).reset_index()
    m["pace_rel"] = m.pace / m.groupby("edge").pace.transform("min")
    m["flow_rel"] = m.flow / m.groupby("edge").flow.transform("max")
    at_pp = m.loc[m.groupby("edge").pace.idxmax()]
    res["hypercongestion"] = {
        "edges": int(m.edge.nunique()),
        "share_edges_max_pace_hour_in_peak": round(float(at_pp.hour.isin([7, 8, 17, 18]).mean()), 4),
        "flow_at_max_pace_hour_over_max_flow_p10_p50_p90": q(at_pp.flow_rel),
        "share_edges_flow_at_max_pace_below_90pct_of_max": round(float((at_pp.flow_rel < 0.9).mean()), 4),
        "share_edges_max_pace_rel_ge_1.5": round(float((at_pp.pace_rel >= 1.5).mean()), 4),
    }
    for lab, sel in [("uncongested_pace_rel_lt_1.2", m.pace_rel < 1.2), ("congested_pace_rel_ge_1.3", m.pace_rel >= 1.3)]:
        sub = h[h.weekday].merge(m.loc[sel, ["edge", "hour"]], on=["edge", "hour"])
        if sub.edge.nunique() > 30:
            r = within(sub, ["y", "x"], [["edge", "hour"], ["date", "hour"]])
            res["hypercongestion"][f"P3_slope_{lab}"] = slope(r.y, r.x, sub.edge)
    rb = within(h[h.weekday], ["y", "x"], [["edge"]])
    rb["bin"] = pd.qcut(rb.x, 10, labels=False)
    res["hypercongestion"]["binned_within_edge_logpace_by_logflow_decile"] = [round(float(v), 4) for v in rb.groupby("bin").y.mean()]
    m.to_csv(out / "edge_hour_profile_weekday.csv", index=False)

    # 5. D3 with all-purpose vs commuter flow (pooled weekdays)
    pk = t10[t10.date.isin(WEEKDAYS) & (t10.coverage >= MIN_COVERAGE)].copy()
    pk["win"] = np.where(pk.slot10.isin(AM_SLOTS), "am", np.where(pk.slot10.isin(PM_SLOTS), "pm", None))
    pk = pk.dropna(subset=["win"]).groupby(["grid_from", "grid_to", "win"]).agg(pace=("pace_min_per_km", "mean"), flow=("len_flow_all", "sum")).unstack("win")
    pk.columns = [f"{a}_{b}" for a, b in pk.columns]
    d3 = pk.reset_index().merge(e5[["grid_from", "grid_to", "HW", "WH"]], on=["grid_from", "grid_to"])
    d3 = d3[(d3[["flow_am", "flow_pm", "HW", "WH"]] > 0).all(axis=1)]
    yv = np.log(d3.pace_am / d3.pace_pm)
    blocks = (d3.grid_from.str.extract(r"sq_(\d+)_(\d+)").astype(int) // 6).astype(str).agg("_".join, axis=1)
    res["D3_am_minus_pm_pace_index"] = {
        "all_purpose_flow_OLS": slope(yv - yv.mean(), np.log(d3.flow_am / d3.flow_pm) - np.log(d3.flow_am / d3.flow_pm).mean(), blocks),
        "commuter_flow_OLS": slope(yv - yv.mean(), np.log(d3.HW / d3.WH) - np.log(d3.HW / d3.WH).mean(), blocks),
        "all_purpose_flow_IV_commuter_flow": slope(yv - yv.mean(), np.log(d3.flow_am / d3.flow_pm) - np.log(d3.flow_am / d3.flow_pm).mean(), blocks,
                                                   z=np.log(d3.HW / d3.WH) - np.log(d3.HW / d3.WH).mean()),
        "edges": int(len(d3)), "se_note": "clustered by 18 km blocks of the origin cell",
    }
    (out / "all_traffic_speed_panel_summary.json").write_text(json.dumps(res, indent=2, ensure_ascii=False))
    print(json.dumps(res, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
