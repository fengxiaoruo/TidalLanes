"""
Why does travel time fall beyond q95? Checks on the gate5 congestion-shape curve
==============================================================================

The binned curve (plot_congestion_technology_gate5.py) bins edge-hours by
realised flow q/q95 and falls in the last two bins. Candidate explanations:

    H1 queueing: jammed hours carry less flow, so they sit left of q95 and
       the busiest hours are those that still flow
    H2 noise in q: extreme-q bins collect positive measurement error
    H3 edge aggregation: t and q come from different roads within an edge

Checks (weekdays 0519-0523, 06-22 h, same sample rules as the curve):

    A composition of each q/q95 bin: peak-hour share, hour mix, relative speed
    B rebin by demand instead of realised flow:
        loo  = mean ln q of the same unit and hour on the other weekdays
        z    = routed-OD instrument (z_routed), relative to its own 95th pct
       H1 predicts time keeps rising at the top of demand; H2 predicts the
       fall disappears too (noise is not in the demand proxies)
    C speed-flow diagram: relative speed v/v90 (v90 = unit's 90th-pct speed)
       against q/q95; mean q/q95 by speed bin. H1 predicts flow falls at the
       lowest speeds (backward-bending branch)
    D curve by edge hypercongestion flag (od_iv/edge_hypercongestion_weekday.csv)

A-C at edge and segment level (segment: Baidu speed and Unicom flow on the
same road, which removes H3). Inputs read only. q is a Unicom sample count,
so only within-unit relative flow is used.

Output: data_work/outputs/<output-name>/ (refuses to overwrite)
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from run_all_traffic_speed_panel_gate5 import MIN_COVERAGE, ROOT, WEEKDAYS  # noqa: E402
from plot_congestion_technology_gate5 import BINS, BLUE, INK, INK2, ORANGE, AQUA, style  # noqa: E402
import run_segment_lambda as sl  # noqa: E402

GATE5 = ROOT / "outputs/all_traffic_speed_panel_rn202505_gate5_v1"
SEG = ROOT / "outputs/segment_speed_flow_rn202505_v1"
PEAK = [7, 8, 17, 18]
VBINS = np.r_[0, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, np.inf]
GREY = "#8d8b85"
MIN_BIN_N = 500   # bins with fewer unit-hours are not drawn (still in the CSV)


def edge_panel() -> pd.DataFrame:
    k = ["grid_from", "grid_to", "date", "slot10"]
    t = pd.read_parquet(GATE5 / "speed/edge_pace_10min.parquet").merge(
        pd.read_parquet(GATE5 / "rn_all/edge_flow_10min.parquet"), on=k, how="left").fillna({"len_flow_all": 0.0})
    t = t.merge(pd.read_parquet(GATE5 / "od_iv/edge_routed_od_flow_10min.parquet"), on=k, how="left").fillna({"z_routed": 0.0})
    t["hour"] = t.slot10 // 6
    h = t[(t.coverage >= MIN_COVERAGE) & t.date.isin(WEEKDAYS)].groupby(["grid_from", "grid_to", "date", "hour"]).agg(
        pace=("pace_min_per_km", "mean"), flow=("len_flow_all", "sum"), z=("z_routed", "sum"), n=("slot10", "size")).reset_index()
    h = h[(h.n == 6) & h.hour.between(6, 21) & (h.flow >= 5)].copy()
    h["unit"] = h.grid_from + ">" + h.grid_to
    hc = pd.read_csv(GATE5 / "od_iv/edge_hypercongestion_weekday.csv")
    flag = set((hc.grid_from + ">" + hc.grid_to)[hc.flagged.astype(bool)])
    h["flagged"] = h.unit.isin(flag)
    return h[["unit", "date", "hour", "pace", "flow", "z", "flagged"]]


def seg_panel() -> pd.DataFrame:
    h = sl.panel(SEG)
    h = h[h.weekday].rename(columns={"roadseg_id": "unit"})
    h["z"] = h.z_routed
    return h[["unit", "date", "hour", "pace", "flow", "z"]].copy()


def prep(h: pd.DataFrame) -> pd.DataFrame:
    h = h.copy()
    g = h.groupby("unit")
    h["y"], h["x"] = np.log(h.pace), np.log(h.flow)
    h["ry"] = h.y - g.y.transform("mean")
    h["qrel"] = h.flow / g.flow.transform(lambda s: s.quantile(0.95))
    h["v"] = 60.0 / h.pace
    h["vrel"] = h.v / g.v.transform(lambda s: s.quantile(0.90))
    # demand proxies
    gh = h.groupby(["unit", "hour"])
    s, n = gh.flow.transform("sum"), gh.flow.transform("size")
    h["loo"] = ((s - h.flow) / (n - 1)).where(n > 1)
    h["loo_rel"] = h.loo / h.groupby("unit").loo.transform(lambda s: s.quantile(0.95))
    zz = h.z.where(h.z > 0)
    h["z_rel"] = zz / zz.groupby(h.unit).transform(lambda s: s.quantile(0.95))
    h["peak"] = h.hour.isin(PEAK)
    return h


def tcurve(h: pd.DataFrame, col: str) -> pd.DataFrame:
    d = h.dropna(subset=[col])
    d = d.assign(ry=d.y - d.groupby("unit").y.transform("mean"), bin=pd.cut(d[col], BINS))
    g = d.groupby("bin", observed=True).agg(n=("ry", "size"), xr=(col, "median"), ly=("ry", "mean"))
    g["t_index"] = np.exp(g.ly - g.ly.iloc[0])
    return g.drop(columns="ly")


def composition(h: pd.DataFrame) -> pd.DataFrame:
    d = h.assign(bin=pd.cut(h.qrel, BINS))
    g = d.groupby("bin", observed=True).agg(n=("v", "size"), qr=("qrel", "median"), peak_share=("peak", "mean"),
                                            vrel_mean=("vrel", "mean"), slow_share=("vrel", lambda s: float((s < 0.6).mean())),
                                            hour_mean=("hour", "mean"))
    hours = d.groupby(["bin", "hour"], observed=True).size().unstack(fill_value=0)
    hours = hours.div(hours.sum(axis=1), axis=0).add_prefix("h")
    return g.join(hours)


def speed_flow(h: pd.DataFrame) -> pd.DataFrame:
    d = h.assign(vbin=pd.cut(h.vrel, VBINS))
    return d.groupby("vbin", observed=True).agg(n=("qrel", "size"), vr=("vrel", "median"), q_mean=("qrel", "mean"),
                                                q_p50=("qrel", "median"), q_p90=("qrel", lambda s: s.quantile(0.9)),
                                                peak_share=("peak", "mean"))


def run_level(h: pd.DataFrame, lab: str, out: Path) -> dict:
    h = prep(h)
    comp = composition(h)
    comp.to_csv(out / f"{lab}_A_composition_by_qbin.csv")
    curves = {c: tcurve(h, c) for c in ["qrel", "loo_rel", "z_rel"]}
    pd.concat(curves, names=["binned_by"]).to_csv(out / f"{lab}_B_time_by_demand_bins.csv")
    sf = speed_flow(h)
    sf.to_csv(out / f"{lab}_C_flow_by_speed_bin.csv")
    slow = h[h.vrel < 0.6]
    res = {
        "unit_hours": int(len(h)), "units": int(h.unit.nunique()),
        "A_peak_share_by_qbin": {str(k): round(float(v), 3) for k, v in comp.peak_share.items()},
        "A_vrel_mean_by_qbin": {str(k): round(float(v), 3) for k, v in comp.vrel_mean.items()},
        "A_slow_hours_vrel_lt_0.6": {"share_of_hours": round(float(len(slow) / len(h)), 4),
                                     "qrel_p25_p50_p75": [round(float(x), 3) for x in slow.qrel.quantile([.25, .5, .75])],
                                     "share_with_qrel_gt_1": round(float((slow.qrel > 1).mean()), 4)},
        "B_t_index_by_bin": {c: {str(k): round(float(v), 4) for k, v in g.t_index.items()} for c, g in curves.items()},
        "C_mean_qrel_by_speed_bin": {str(k): round(float(v), 3) for k, v in sf.q_mean.items()},
    }
    if "flagged" in h:
        dd = {}
        for f, s in h.groupby("flagged"):
            g = tcurve(s, "qrel")
            dd["flagged" if f else "not_flagged"] = {"units": int(s.unit.nunique()), "t_index": {str(k): round(float(v), 4) for k, v in g.t_index.items()}}
        res["D_curve_by_hypercongestion_flag"] = dd
    return res, h, curves, sf


def figure(levels: dict, out: Path) -> None:
    plt.rcParams.update({"font.size": 12})
    fig, ax = plt.subplots(2, 2, figsize=(13, 9.2))
    for j, (lab, (h, curves, sf)) in enumerate(levels.items()):
        a = ax[0, j]
        for c, col, name in [("qrel", BLUE, "realised flow q / q95"), ("loo_rel", ORANGE, "usual flow, same hour other days"),
                             ("z_rel", AQUA, "routed-OD demand z / z95")]:
            g = curves[c][curves[c].n >= MIN_BIN_N]
            a.plot(g.xr, g.t_index, color=col, lw=2.2, marker="o", ms=5, label=name)
        a.axvline(1, color=INK2, lw=0.8, ls=":")
        a.set(xlim=(0.1, 1.35), xlabel="load relative to the unit's own 95th percentile", ylabel="travel time (lowest bin = 1)")
        a.set_title(f"{lab.capitalize()}: travel time by three load measures", fontsize=13, color=INK, loc="left")
        a.legend(frameon=False, fontsize=10, loc="upper left")
        b = ax[1, j]
        s = h.sample(min(len(h), 60000), random_state=0)
        b.scatter(s.qrel, s.vrel, s=2, color=GREY, alpha=0.12, lw=0, rasterized=True)
        b.plot(sf.q_mean, sf.vr, color=BLUE, lw=2.4, marker="o", ms=6, label="mean flow within each speed bin")
        b.set(xlim=(0, 1.6), ylim=(0.2, 1.25), xlabel="q / q95", ylabel="speed / unit's 90th-pct speed")
        b.set_title(f"{lab.capitalize()}: speed against flow", fontsize=13, color=INK, loc="left")
        b.legend(frameon=False, fontsize=10, loc="lower left")
        for x in (a, b):
            style(x)
    fig.text(0.01, 0.005, "Weekdays 0519-0523, 06-22 h, hourly. Edge: gate5 directed edges. Segment: Baidu segments with matched Unicom flow. Grey dots: 60,000 random unit-hours. Bins with < 500 unit-hours not drawn.",
             fontsize=9, color=INK2)
    fig.tight_layout(rect=(0, 0.02, 1, 1))
    fig.savefig(out / "fig15_falling_tail_checks.png", dpi=200)
    plt.close(fig)


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-name", default="congestion_falling_tail_v1")
    args = ap.parse_args(argv)
    out = ROOT / "outputs" / args.output_name
    if out.exists():
        raise SystemExit(f"{out} exists; choose a new --output-name")
    out.mkdir(parents=True)
    res, figs = {}, {}
    for lab, f in [("edge", edge_panel), ("segment", seg_panel)]:
        r, h, curves, sf = run_level(f(), lab, out)
        res[lab] = r
        figs[lab] = (h, curves, sf)
    figure(figs, out)
    (out / "summary.json").write_text(json.dumps(res, indent=2, ensure_ascii=False))
    print(json.dumps(res, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
