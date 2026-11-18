"""
Follow-up to run_congestion_falling_tail_check.py (nextstep A1)
==============================================================

    1 normaliser: is the falling tail an artefact of dividing by q95?
        (a) curve with q/q90, q/q95, q/q99
        (b) per edge, a ratio that uses no percentile of all hours:
            q_jam  = median flow of the edge's slow hours (v/v90 < 0.6)
            q_top  = 95th pct flow of the edge's free hours (v/v90 >= 0.9)
            ratio  = q_jam / q_top  (< 1 would mean jammed hours carry less than the busiest free-flowing hours)
    2 10-minute speed-flow table and figure (edge level; flow per 10 min)
    3 routed-OD tail: composition of z/z95 bins (peak share, hour, speed, realised q/q95)
    4 0-24 h: lambda (edge OLS/IV, V1-V4 as run_od_iv_hypercongestion_gate5.py;
      segment OLS/IV S1-S3, SE clustered by gate5 edge) and the shape curve,
      06-22 h vs 00-24 h on the same code path

Weekdays 0519-0523 for curves and tables; lambda samples as in the source
scripts (0519-0525). Inputs read only.
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
from run_all_traffic_speed_panel_gate5 import EDGES, MIN_COVERAGE, ROOT, WEEKDAYS, slope, within  # noqa: E402
from plot_congestion_technology_gate5 import BINS, BLUE, INK, INK2, ORANGE, AQUA, style  # noqa: E402
import run_segment_lambda as sl  # noqa: E402

GATE5 = ROOT / "outputs/all_traffic_speed_panel_rn202505_gate5_v1"
SEG = ROOT / "outputs/segment_speed_flow_rn202505_v1"
PEAK = [7, 8, 17, 18]
VBINS = np.r_[0, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, np.inf]
GREY = "#8d8b85"
K = ["grid_from", "grid_to", "date", "slot10"]


def edge10() -> pd.DataFrame:
    t = pd.read_parquet(GATE5 / "speed/edge_pace_10min.parquet").merge(
        pd.read_parquet(GATE5 / "rn_all/edge_flow_10min.parquet"), on=K, how="left").fillna({"len_flow_all": 0.0})
    t = t.merge(pd.read_parquet(GATE5 / "od_iv/edge_routed_od_flow_10min.parquet"), on=K, how="left").fillna({"z_routed": 0.0})
    t["hour"] = t.slot10 // 6
    t["unit"] = t.grid_from + ">" + t.grid_to
    return t[t.coverage >= MIN_COVERAGE]


def edge_hourly(t: pd.DataFrame, hours: tuple[int, int]) -> pd.DataFrame:
    h = t.groupby(["unit", "grid_from", "grid_to", "date", "hour"]).agg(
        pace=("pace_min_per_km", "mean"), flow=("len_flow_all", "sum"), z=("z_routed", "sum"), n=("slot10", "size")).reset_index()
    return h[(h.n == 6) & h.hour.between(*hours) & (h.flow >= 5)].copy()


def rel(h: pd.DataFrame, q: float = 0.95) -> pd.DataFrame:
    h = h.copy()
    g = h.groupby("unit")
    h["y"] = np.log(h.pace)
    h["qrel"] = h.flow / g.flow.transform(lambda s: s.quantile(q))
    h["v"] = 60.0 / h.pace
    h["vrel"] = h.v / g.v.transform(lambda s: s.quantile(0.90))
    return h


def tcurve(h: pd.DataFrame, col: str) -> pd.DataFrame:
    d = h.dropna(subset=[col])
    d = d.assign(ry=d.y - d.groupby("unit").y.transform("mean"), bin=pd.cut(d[col], BINS))
    g = d.groupby("bin", observed=True).agg(n=("ry", "size"), xr=(col, "median"), ly=("ry", "mean"))
    g["t_index"] = np.exp(g.ly - g.ly.iloc[0])
    return g.drop(columns="ly")


def r4(s: pd.Series) -> dict:
    return {str(k): round(float(v), 4) for k, v in s.items()}


# 1 ---------------------------------------------------------------------------
def normaliser(hw: pd.DataFrame, out: Path) -> dict:
    res = {}
    cur = {}
    for q in (0.90, 0.95, 0.99):
        h = rel(hw, q)
        g = tcurve(h, "qrel")
        cur[f"q{int(q * 100)}"] = g
        k = g.t_index.idxmax()
        res[f"curve_q{int(q * 100)}"] = {"peak_bin": str(k), "peak_t_index": round(float(g.t_index[k]), 4),
                                         "share_hours_beyond_peak_bin": round(float(g.n[g.index > k].sum() / g.n.sum()), 4),
                                         "t_index": r4(g.t_index)}
    pd.concat(cur, names=["normaliser"]).to_csv(out / "1a_curve_by_normaliser.csv")
    h = rel(hw, 0.95)
    jam = h[h.vrel < 0.6].groupby("unit").flow.agg(["median", "size"])
    top = h[h.vrel >= 0.9].groupby("unit").flow.agg(lambda s: s.quantile(0.95))
    e = jam[jam["size"] >= 5].join(top.rename("q_top"), how="inner")
    e["ratio"] = e["median"] / e.q_top
    e.to_csv(out / "1b_edge_jam_vs_top_flow.csv")
    res["edge_ratio_qjam_over_qtop"] = {"edges_with_ge5_slow_hours": int(len(e)),
                                        "p10_p25_p50_p75_p90": [round(float(x), 3) for x in e.ratio.quantile([.1, .25, .5, .75, .9])],
                                        "share_below_1": round(float((e.ratio < 1).mean()), 4),
                                        "rule": "q_jam = median flow of hours with v/v90 < 0.6; q_top = 95th pct flow of hours with v/v90 >= 0.9"}
    return res, cur, e


# 2 ---------------------------------------------------------------------------
def ten_minute(t: pd.DataFrame, out: Path) -> tuple[dict, pd.DataFrame, pd.DataFrame]:
    d = t[t.date.isin(WEEKDAYS) & t.hour.between(6, 21) & (t.len_flow_all >= 1)].copy()
    g = d.groupby("unit")
    d["qrel"] = d.len_flow_all / g.len_flow_all.transform(lambda s: s.quantile(0.95))
    d["v"] = 60.0 / d.pace_min_per_km
    d["vrel"] = d.v / g.v.transform(lambda s: s.quantile(0.90))
    d["peak"] = d.hour.isin(PEAK)
    sf = d.assign(vbin=pd.cut(d.vrel, VBINS)).groupby("vbin", observed=True).agg(
        n=("qrel", "size"), vr=("vrel", "median"), q_mean=("qrel", "mean"), q_p50=("qrel", "median"), peak_share=("peak", "mean"))
    sf.to_csv(out / "2_flow_by_speed_bin_10min.csv")
    slow = d[d.vrel < 0.6]
    return ({"slots": int(len(d)), "units": int(d.unit.nunique()), "mean_qrel_by_speed_bin": r4(sf.q_mean),
             "slow_slots_share": round(float(len(slow) / len(d)), 4),
             "slow_slots_qrel_p50": round(float(slow.qrel.median()), 3)}, sf, d)


# 3 ---------------------------------------------------------------------------
def z_tail(hw: pd.DataFrame, out: Path) -> dict:
    h = rel(hw, 0.95)
    zz = h.z.where(h.z > 0)
    h["z_rel"] = zz / zz.groupby(h.unit).transform(lambda s: s.quantile(0.95))
    h["peak"] = h.hour.isin(PEAK)
    d = h.dropna(subset=["z_rel"]).assign(bin=lambda x: pd.cut(x.z_rel, BINS))
    g = d.groupby("bin", observed=True).agg(n=("v", "size"), zr=("z_rel", "median"), peak_share=("peak", "mean"),
                                            am_share=("hour", lambda s: float(s.isin([7, 8]).mean())),
                                            hour_mean=("hour", "mean"), vrel_mean=("vrel", "mean"),
                                            qrel_mean=("qrel", "mean"), qrel_p50=("qrel", "median"))
    hours = d.groupby(["bin", "hour"], observed=True).size().unstack(fill_value=0)
    g = g.join(hours.div(hours.sum(axis=1), axis=0).add_prefix("h"))
    g.to_csv(out / "3_composition_by_zbin.csv")
    # where does z peak against where pace peaks, by hour (city mean of within-edge logs)
    p = h.assign(lz=np.log(zz)).dropna(subset=["lz"])
    p = p.assign(ry=p.y - p.groupby("unit").y.transform("mean"), rz=p.lz - p.groupby("unit").lz.transform("mean"),
                 rq=np.log(p.flow) - p.groupby("unit").flow.transform(lambda s: np.log(s).mean()))
    hp = p.groupby("hour")[["ry", "rq", "rz"]].mean()
    hp.to_csv(out / "3_hour_profile_t_q_z.csv")
    return {"by_zbin": {c: r4(g[c]) for c in ["n", "peak_share", "am_share", "hour_mean", "vrel_mean", "qrel_mean"]},
            "hour_of_max": {"ln_t": int(hp.ry.idxmax()), "ln_q": int(hp.rq.idxmax()), "ln_z": int(hp.rz.idxmax())},
            "hour_profile": {c: r4(hp[c]) for c in hp}}


# 4 ---------------------------------------------------------------------------
def edge_lambda(t: pd.DataFrame, hours: tuple[int, int]) -> dict:
    edges = pd.read_parquet(EDGES, columns=["grid_o", "grid_d", "lanes_directional"]).rename(columns={"grid_o": "grid_from", "grid_d": "grid_to"})
    edges["n_kl"] = edges.lanes_directional.where(edges.lanes_directional > 0, edges.lanes_directional.mean())
    h = edge_hourly(t, hours)
    h = h[h.z > 0].merge(edges[["grid_from", "grid_to", "n_kl"]], on=["grid_from", "grid_to"])
    h["y"], h["x"], h["zl"] = np.log(h.pace), np.log(h.flow / h.n_kl), np.log(h.z / h.n_kl)
    h["weekday"] = h.date.isin(WEEKDAYS)
    specs = [("V1 edge FE + date x hour FE", h, [["unit"], ["date", "hour"]]),
             ("V2 edge x hour FE + date x hour FE", h, [["unit", "hour"], ["date", "hour"]]),
             ("V3 as V2, weekdays", h[h.weekday], [["unit", "hour"], ["date", "hour"]]),
             ("V4 weekday peak 7,8,17,18", h[h.weekday & h.hour.isin(PEAK)], [["unit"], ["date", "hour"]])]
    o = {}
    for lab, d, fes in specs:
        r = within(d, ["y", "x", "zl"], fes)
        o[lab] = {"OLS": slope(r.y, r.x, d.unit), "IV": slope(r.y, r.x, d.unit, z=r.zl)}
    o["sample"] = {"edge_hours": int(len(h)), "edges": int(h.unit.nunique()), "hours": f"{hours[0]:02d}-{hours[1] + 1:02d}"}
    return o


def seg_panel(hours: tuple[int, int]) -> pd.DataFrame:
    """run_segment_lambda.panel with the hour window as a parameter (same rules otherwise)."""
    t = pd.read_parquet(SEG / "segment_pace_10min.parquet")
    t["hour"] = t.slot10 // 6
    h = t.groupby(["roadseg_id", "date", "hour"]).agg(pace=("pace_min_per_km", "mean"), n=("slot10", "size")).reset_index()
    h = h[(h.n >= 4) & h.hour.between(*hours)]
    h = h.merge(pd.read_parquet(SEG / "segment_flow_hour.parquet"), on=["roadseg_id", "date", "hour"])
    meta = pd.read_parquet(SEG / "segment_meta.parquet")
    good = meta[meta.matched_share_of_length >= sl.MIN_MATCH_SHARE].roadseg_id
    # sample segments fixed by the 06-22 h weekday mean-flow rule, so both windows use the same segments
    h6 = h[h.hour.between(6, 21) & h.date.isin(WEEKDAYS)]
    mf = h6.groupby("roadseg_id").flow.mean()
    keep = set(good) & set(mf[mf >= sl.MIN_MEAN_FLOW].index)
    h = h[h.roadseg_id.isin(keep) & (h.flow >= 5)]
    sw = pd.read_parquet(GATE5 / "speed/segment_edge_weights.parquet").sort_values("w").drop_duplicates("roadseg_id", keep="last")
    z = pd.read_parquet(GATE5 / "od_iv/edge_routed_od_flow_10min.parquet")
    z["hour"] = z.slot10 // 6
    z = z.groupby(["grid_from", "grid_to", "date", "hour"]).z_routed.sum().reset_index()
    h = h.merge(sw[["roadseg_id", "grid_from", "grid_to"]], on="roadseg_id", how="left").merge(z, on=["grid_from", "grid_to", "date", "hour"], how="left")
    h["y"], h["x"] = np.log(h.pace), np.log(h.flow)
    h["zr"] = np.log(h.z_routed.where(h.z_routed > 0))
    h["gedge"] = h.grid_from.fillna("") + ">" + h.grid_to.fillna("")
    h["weekday"] = h.date.isin(WEEKDAYS)
    return h


def seg_lambda(h: pd.DataFrame) -> dict:
    specs = [("S1 segment FE + date x hour FE", h, [["roadseg_id"], ["date", "hour"]]),
             ("S2 segment x hour FE + date x hour FE", h, [["roadseg_id", "hour"], ["date", "hour"]]),
             ("S3 weekday peak 7,8,17,18", h[h.weekday & h.hour.isin(PEAK)], [["roadseg_id"], ["date", "hour"]])]
    o = {}
    for lab, d, fes in specs:
        r = within(d, ["y", "x"], fes)
        s = d.dropna(subset=["zr"])
        rz = within(s, ["y", "x", "zr"], fes)
        o[lab] = {"OLS": slope(r.y, r.x, d.gedge), "IV": slope(rz.y, rz.x, s.gedge, z=rz.zr)}
    o["sample"] = {"segment_hours": int(len(h)), "segments": int(h.roadseg_id.nunique()), "se": "clustered by the segment's main gate5 edge"}
    return o


def window_compare(t: pd.DataFrame, out: Path) -> tuple[dict, dict]:
    res, curves = {}, {}
    for lab, hrs in [("06-22", (6, 21)), ("00-24", (0, 23))]:
        e = edge_lambda(t, hrs)
        sp = seg_panel(hrs)
        s = seg_lambda(sp)
        hw = rel(edge_hourly(t[t.date.isin(WEEKDAYS)], hrs))
        g = tcurve(hw, "qrel")
        curves[lab] = g
        res[lab] = {"edge": e, "segment": s, "edge_curve_t_index": r4(g.t_index),
                    "edge_curve_n": {str(k): int(v) for k, v in g.n.items()}}
    pd.concat(curves, names=["window"]).to_csv(out / "4_edge_curve_by_window.csv")
    return res, curves


def figure(cur: dict, e: pd.DataFrame, sf10: pd.DataFrame, d10: pd.DataFrame, wc: dict, out: Path) -> None:
    plt.rcParams.update({"font.size": 12})
    fig, ax = plt.subplots(2, 2, figsize=(13, 9.2))
    a = ax[0, 0]
    for (k, g), c in zip(cur.items(), [ORANGE, BLUE, AQUA]):
        g = g[g.n >= 500]
        a.plot(g.xr, g.t_index, color=c, lw=2.2, marker="o", ms=5, label=f"q / {k}")
    a.axvline(1, color=INK2, lw=0.8, ls=":")
    a.set(xlabel="q / edge's own percentile of hourly flow", ylabel="travel time (lowest bin = 1)")
    a.set_title("Edge curve with three normalisers", fontsize=13, color=INK, loc="left")
    a.legend(frameon=False, fontsize=10, loc="upper left")
    b = ax[0, 1]
    b.hist(e.ratio.clip(0, 2), bins=np.linspace(0, 2, 41), color=BLUE, alpha=0.8, edgecolor="white", lw=0.5)
    b.axvline(1, color=INK2, lw=0.8, ls=":")
    b.axvline(e.ratio.median(), color=INK, lw=1.4)
    b.set(xlabel="median flow of jammed hours / 95th-pct flow of free hours", ylabel="edges")
    b.set_title(f"Flow in jammed hours vs busiest free hours: median {e.ratio.median():.2f}, {len(e):,} edges", fontsize=13, color=INK, loc="left")
    c = ax[1, 0]
    s = d10.sample(min(len(d10), 60000), random_state=0)
    c.scatter(s.qrel, s.vrel, s=2, color=GREY, alpha=0.12, lw=0, rasterized=True)
    c.plot(sf10.q_mean, sf10.vr, color=BLUE, lw=2.4, marker="o", ms=6, label="mean flow within each speed bin")
    c.set(xlim=(0, 1.8), ylim=(0.2, 1.25), xlabel="q / q95 (10-minute flow)", ylabel="speed / edge's 90th-pct speed")
    c.set_title("Edge, 10-minute: speed against flow", fontsize=13, color=INK, loc="left")
    c.legend(frameon=False, fontsize=10, loc="lower left")
    dd = ax[1, 1]
    for (k, g), col in zip(wc.items(), [BLUE, ORANGE]):
        g = g[g.n >= 500]
        dd.plot(g.xr, g.t_index, color=col, lw=2.2, marker="o", ms=5, label=f"{k} h")
    dd.axvline(1, color=INK2, lw=0.8, ls=":")
    dd.set(xlabel="q / q95", ylabel="travel time (lowest bin = 1)")
    dd.set_title("Edge curve, 06-22 h against 00-24 h", fontsize=13, color=INK, loc="left")
    dd.legend(frameon=False, fontsize=10, loc="upper left")
    for x in ax.flat:
        style(x)
    fig.text(0.01, 0.005, "Weekdays 0519-0523, gate5 directed edges. Jammed: v/v90 < 0.6 (edges with >= 5 such hours); free: v/v90 >= 0.9. Bins with < 500 obs not drawn.",
             fontsize=9, color=INK2)
    fig.tight_layout(rect=(0, 0.02, 1, 1))
    fig.savefig(out / "fig16_tail_followup.png", dpi=200)
    plt.close(fig)


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-name", default="congestion_tail_followup_v1")
    args = ap.parse_args(argv)
    out = ROOT / "outputs" / args.output_name
    if out.exists():
        raise SystemExit(f"{out} exists; choose a new --output-name")
    out.mkdir(parents=True)
    t = edge10()
    hw = edge_hourly(t[t.date.isin(WEEKDAYS)], (6, 21))
    res = {}
    res["1_normaliser"], cur, e = normaliser(hw, out)
    res["2_ten_minute"], sf10, d10 = ten_minute(t, out)
    res["3_z_tail"] = z_tail(hw, out)
    res["4_window"], wc = window_compare(t, out)
    figure(cur, e, sf10, d10, wc, out)
    (out / "summary.json").write_text(json.dumps(res, indent=2, ensure_ascii=False))
    print(json.dumps(res, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
