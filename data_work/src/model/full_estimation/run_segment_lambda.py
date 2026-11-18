"""
Segment-level speed-flow elasticity (Baidu speed segments x matched Unicom RN flow)
=================================================================================

Inputs: build_segment_speed_flow_panel.py output (segment flow and pace by
10 minutes), plus the gate5 routed-OD instrument (od_iv/) mapped to each
segment through its largest-weight gate5 edge (speed/segment_edge_weights).

Hourly cells 06-22 h with at least 4 of 6 paced slots. Sample segments: matched
RN chord length at least half the segment length and weekday mean hourly flow
of at least MIN_MEAN_FLOW. ln pace is regressed on ln flow (sample vehicles per
hour; lanes are constant within segment and drop out with segment effects).

    S1 segment FE + date x hour FE           (OLS; IV leave-one-day-out; IV routed OD)
    S2 segment x hour FE + date x hour FE    (day-to-day)
    S3 weekday peak hours (7, 8, 17, 18)
    by road class; local slopes by q/q95 (segment's own 95th-percentile hourly flow)

Output: data_work/outputs/<panel-name>/lambda/ (refuses to overwrite)
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
from run_all_traffic_speed_panel_gate5 import ROOT, WEEKDAYS, slope, within  # noqa: E402
from plot_congestion_technology_gate5 import BINS, BLUE, INK, INK2, style  # noqa: E402

GATE5 = ROOT / "outputs/all_traffic_speed_panel_rn202505_gate5_v1"
PEAK = [7, 8, 17, 18]
MIN_MEAN_FLOW, MIN_MATCH_SHARE = 20.0, 0.5


def panel(base: Path) -> pd.DataFrame:
    t = pd.read_parquet(base / "segment_pace_10min.parquet")
    t["hour"] = t.slot10 // 6
    h = t.groupby(["roadseg_id", "date", "hour"]).agg(pace=("pace_min_per_km", "mean"), n=("slot10", "size")).reset_index()
    h = h[(h.n >= 4) & h.hour.between(6, 21)]
    h = h.merge(pd.read_parquet(base / "segment_flow_hour.parquet"), on=["roadseg_id", "date", "hour"])
    meta = pd.read_parquet(base / "segment_meta.parquet")
    good = meta[meta.matched_share_of_length >= MIN_MATCH_SHARE].roadseg_id
    mf = h[h.date.isin(WEEKDAYS)].groupby("roadseg_id").flow.mean()
    keep = set(good) & set(mf[mf >= MIN_MEAN_FLOW].index)
    h = h[h.roadseg_id.isin(keep) & (h.flow >= 5)].merge(meta[["roadseg_id", "road_class"]], on="roadseg_id")
    # routed-OD instrument of the segment's main gate5 edge
    sw = pd.read_parquet(GATE5 / "speed/segment_edge_weights.parquet").sort_values("w").drop_duplicates("roadseg_id", keep="last")
    z = pd.read_parquet(GATE5 / "od_iv/edge_routed_od_flow_10min.parquet")
    z["hour"] = z.slot10 // 6
    z = z.groupby(["grid_from", "grid_to", "date", "hour"]).z_routed.sum().reset_index()
    h = h.merge(sw[["roadseg_id", "grid_from", "grid_to"]], on="roadseg_id", how="left").merge(z, on=["grid_from", "grid_to", "date", "hour"], how="left")
    h["y"], h["x"] = np.log(h.pace), np.log(h.flow)
    h["zr"] = np.log(h.z_routed.where(h.z_routed > 0))
    h["z_loo"] = h.groupby(["roadseg_id", "hour"]).x.transform(lambda s: (s.sum() - s) / (len(s) - 1) if len(s) > 1 else np.nan)
    h["weekday"] = h.date.isin(WEEKDAYS)
    return h


def spec(d: pd.DataFrame, fes: list[list[str]]) -> dict:
    r = within(d, ["y", "x"], fes)
    o = {"OLS": slope(r.y, r.x, d.roadseg_id)}
    for lab, zc in [("IV_leave_one_day_out", "z_loo"), ("IV_routed_od_edge", "zr")]:
        s = d.dropna(subset=[zc])
        rz = within(s, ["y", "x", zc], fes)
        o[lab] = slope(rz.y, rz.x, s.roadseg_id, z=rz[zc])
    return o


def curve(h: pd.DataFrame, out: Path) -> list[dict]:
    w = h[h.weekday].copy()
    w["ry"] = w.y - w.groupby("roadseg_id").y.transform("mean")
    w["rx"] = w.x - w.groupby("roadseg_id").x.transform("mean")
    w["qrel"] = w.flow / w.groupby("roadseg_id").flow.transform(lambda s: s.quantile(0.95))
    w["bin"] = pd.cut(w.qrel, BINS)
    g = w.groupby("bin", observed=True).agg(n=("ry", "size"), qr=("qrel", "median"), lx=("rx", "mean"), ly=("ry", "mean"))
    g["slope"] = g.ly.diff() / g.lx.diff()
    g["t_index"] = np.exp(g.ly - g.ly.iloc[0])
    fig, (l, r) = plt.subplots(1, 2, figsize=(10, 4))
    l.plot(g.qr, g.t_index, color=BLUE, lw=2, marker="o", ms=4)
    l.set(xlabel="q / q95 (segment's own 95th-percentile hourly flow; not capacity)", ylabel="travel-time index (lowest-flow bin = 1)")
    l.set_title("Segment level: time index by load", fontsize=10, color=INK, loc="left")
    m = g.slope.notna()
    r.plot(g.qr[m], g.slope[m], color=BLUE, lw=2, marker="o", ms=4)
    r.axhline(0, color=INK2, lw=0.8)
    r.set(xlabel="q / q95", ylabel="local elasticity d ln t / d ln q (OLS, within segment)")
    r.set_title("Segment level: local elasticity by load", fontsize=10, color=INK, loc="left")
    for ax in (l, r):
        style(ax)
    fig.text(0.01, 0.01, f"Baidu speed segments matched to Unicom RN flow; weekdays 0519-0523, 06-22 h; {len(w):,} segment-hours, {w.roadseg_id.nunique():,} segments.",
             fontsize=8, color=INK2)
    fig.tight_layout(rect=(0, 0.05, 1, 1))
    fig.savefig(out / "fig3_segment_q_over_q95.png", dpi=200)
    plt.close(fig)
    g.to_csv(out / "segment_curve_by_q_over_q95.csv")
    return [{"q_over_q95_median": round(float(a), 3), "n": int(n), "t_index": round(float(t), 4),
             "local_slope": None if np.isnan(s) else round(float(s), 3)} for a, n, t, s in zip(g.qr, g.n, g.t_index, g.slope)]


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--panel-name", default="segment_speed_flow_rn202505_v1")
    args = ap.parse_args(argv)
    base = ROOT / "outputs" / args.panel_name
    out = base / "lambda"
    if out.exists():
        raise SystemExit(f"{out} exists; choose a new --panel-name")
    out.mkdir(parents=True)
    h = panel(base)
    res = {"sample": {"segment_hours": int(len(h)), "segments": int(h.roadseg_id.nunique()),
                      "segments_by_road_class": {int(k): int(v) for k, v in h.drop_duplicates("roadseg_id").road_class.value_counts().items()},
                      "rule": f"matched chord >= {MIN_MATCH_SHARE} x segment length; weekday mean hourly flow >= {MIN_MEAN_FLOW}; hourly flow >= 5"}}
    res["S1_segment_FE+date_x_hour_FE"] = spec(h, [["roadseg_id"], ["date", "hour"]])
    res["S2_segment_x_hour_FE+date_x_hour_FE"] = spec(h, [["roadseg_id", "hour"], ["date", "hour"]])
    res["S3_weekday_peak_hours"] = spec(h[h.weekday & h.hour.isin(PEAK)], [["roadseg_id"], ["date", "hour"]])
    res["by_road_class_S1"] = {int(c): spec(d, [["roadseg_id"], ["date", "hour"]]) for c, d in h.groupby("road_class") if d.roadseg_id.nunique() > 50}
    res["by_road_class_S3"] = {int(c): spec(d, [["roadseg_id"], ["date", "hour"]]) for c, d in h[h.weekday & h.hour.isin(PEAK)].groupby("road_class") if d.roadseg_id.nunique() > 50}
    res["curve_q_over_q95"] = curve(h, out)
    (out / "segment_lambda_summary.json").write_text(json.dumps(res, indent=2))
    print(json.dumps(res, indent=2))


if __name__ == "__main__":
    main()
