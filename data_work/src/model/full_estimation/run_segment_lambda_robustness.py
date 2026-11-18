"""
Segment-level lambda robustness: stale (constant) Baidu speed, single-day shock,
and the "flat until congestion" load-regime question
================================================================================

Reuses the panel and estimators of run_segment_lambda.py (Baidu speed segments x
Unicom RN flow, 0519-0525, hourly cells 06-22 h).

1. Constant-run diagnostics on 10-minute pace. A run is a sequence of
   consecutive 10-minute slots (same segment, same date) with exactly equal
   stored pace. Slots in runs of >= RUN_MIN slots (60 min) are flagged as stale.
2. Variants, each re-estimated with S1/S2/S3 x (OLS, IV leave-one-day-out,
   IV routed OD) and the q/q95 curve:
     V0 baseline (reproduces run_segment_lambda.py)
     V1 drop stale slots before the hourly mean (hour still needs >= 4 slots)
     V2 drop segments whose weekday 06-22 h stale share exceeds SEG_STALE_MAX
     V3 drop 2025-05-22
     V4 V1 + V3
3. Load regimes: regime = typical load of the segment-hour (weekday mean flow
   at that hour / segment's own q95), fixed within segment x hour, so it is not
   selected on the day's own flow. Within each regime, S2 (segment x hour FE +
   date x hour FE) gives the day-to-day elasticity at that load.

Output: data_work/outputs/<panel-name>/lambda_robustness_v1/ (refuses to overwrite)
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
from run_all_traffic_speed_panel_gate5 import ROOT, WEEKDAYS  # noqa: E402
from plot_congestion_technology_gate5 import BINS, BLUE, INK, INK2, style  # noqa: E402
import run_segment_lambda as sl  # noqa: E402

RUN_MIN = 6            # 6 x 10 min = 60 min
SEG_STALE_MAX = 0.20
DROP_DAY = 20250522
REGIMES = [0, 0.5, 0.7, 0.9, np.inf]
ORANGE, GREY = "#B5643A", "#8A9BA8"


def flag_runs(t: pd.DataFrame) -> pd.DataFrame:
    t = t.sort_values(["roadseg_id", "date", "slot10"]).reset_index(drop=True)
    seg, d, s, p = (t[c].to_numpy() for c in ["roadseg_id", "date", "slot10", "pace_min_per_km"])
    new = np.ones(len(t), bool)
    new[1:] = (seg[1:] != seg[:-1]) | (d[1:] != d[:-1]) | (s[1:] != s[:-1] + 1) | (p[1:] != p[:-1])
    rid = np.cumsum(new)
    t["run_len"] = pd.Series(rid).map(pd.Series(rid).value_counts()).to_numpy()
    t["stale"] = t.run_len >= RUN_MIN
    c = t.groupby(["roadseg_id", "pace_min_per_km"]).size().reset_index(name="k")
    mode = c.sort_values("k").drop_duplicates("roadseg_id", keep="last").set_index("roadseg_id").pace_min_per_km
    t["is_seg_mode"] = t.pace_min_per_km.to_numpy() == t.roadseg_id.map(mode).to_numpy()
    return t


def panel_from(t: pd.DataFrame, base: Path) -> pd.DataFrame:
    """Same construction as run_segment_lambda.panel, from an in-memory 10-min table."""
    t = t.copy()
    t["hour"] = t.slot10 // 6
    h = t.groupby(["roadseg_id", "date", "hour"]).agg(pace=("pace_min_per_km", "mean"), n=("slot10", "size"),
                                                       stale_share=("stale", "mean")).reset_index()
    h = h[(h.n >= 4) & h.hour.between(6, 21)]
    h = h.merge(pd.read_parquet(base / "segment_flow_hour.parquet"), on=["roadseg_id", "date", "hour"])
    meta = pd.read_parquet(base / "segment_meta.parquet")
    good = meta[meta.matched_share_of_length >= sl.MIN_MATCH_SHARE].roadseg_id
    mf = h[h.date.isin(WEEKDAYS)].groupby("roadseg_id").flow.mean()
    keep = set(good) & set(mf[mf >= sl.MIN_MEAN_FLOW].index)
    h = h[h.roadseg_id.isin(keep) & (h.flow >= 5)].merge(meta[["roadseg_id", "road_class"]], on="roadseg_id")
    sw = pd.read_parquet(sl.GATE5 / "speed/segment_edge_weights.parquet").sort_values("w").drop_duplicates("roadseg_id", keep="last")
    z = pd.read_parquet(sl.GATE5 / "od_iv/edge_routed_od_flow_10min.parquet")
    z["hour"] = z.slot10 // 6
    z = z.groupby(["grid_from", "grid_to", "date", "hour"]).z_routed.sum().reset_index()
    h = h.merge(sw[["roadseg_id", "grid_from", "grid_to"]], on="roadseg_id", how="left").merge(z, on=["grid_from", "grid_to", "date", "hour"], how="left")
    h["y"], h["x"] = np.log(h.pace), np.log(h.flow)
    h["zr"] = np.log(h.z_routed.where(h.z_routed > 0))
    g = h.groupby(["roadseg_id", "hour"]).x
    h["z_loo"] = (g.transform("sum") - h.x) / (g.transform("size") - 1)
    h.loc[g.transform("size") <= 1, "z_loo"] = np.nan
    h["weekday"] = h.date.isin(WEEKDAYS)
    return h


def specs(h: pd.DataFrame) -> dict:
    pk = h[h.weekday & h.hour.isin(sl.PEAK)]
    return {"S1": sl.spec(h, [["roadseg_id"], ["date", "hour"]]),
            "S2": sl.spec(h, [["roadseg_id", "hour"], ["date", "hour"]]),
            "S3_peak": sl.spec(pk, [["roadseg_id"], ["date", "hour"]])}


def curve(h: pd.DataFrame) -> pd.DataFrame:
    w = h[h.weekday].copy()
    w["ry"] = w.y - w.groupby("roadseg_id").y.transform("mean")
    w["rx"] = w.x - w.groupby("roadseg_id").x.transform("mean")
    w["qrel"] = w.flow / w.groupby("roadseg_id").flow.transform(lambda s: s.quantile(0.95))
    w["bin"] = pd.cut(w.qrel, BINS)
    g = w.groupby("bin", observed=True).agg(n=("ry", "size"), qr=("qrel", "median"), lx=("rx", "mean"), ly=("ry", "mean"),
                                            stale=("stale_share", "mean"))
    g["slope"] = g.ly.diff() / g.lx.diff()
    g["t_index"] = np.exp(g.ly - g.ly.iloc[0])
    return g


def regimes(h: pd.DataFrame) -> dict:
    w = h[h.weekday].copy()
    q95 = w.groupby("roadseg_id").flow.quantile(0.95)
    typ = w.groupby(["roadseg_id", "hour"]).flow.mean().rename("typ").reset_index()
    typ["typ_rel"] = typ.typ / typ.roadseg_id.map(q95)
    w = w.merge(typ[["roadseg_id", "hour", "typ_rel"]], on=["roadseg_id", "hour"])
    w["regime"] = pd.cut(w.typ_rel, REGIMES, right=False)
    out = {}
    for r, d in w.groupby("regime", observed=True):
        if d.roadseg_id.nunique() < 200:
            continue
        o = sl.spec(d, [["roadseg_id", "hour"], ["date", "hour"]])
        o["segment_hours"], o["segments"] = int(len(d)), int(d.roadseg_id.nunique())
        o["stale_share_of_slots"] = round(float(d.stale_share.mean()), 4)
        out[str(r)] = o
    return out


def diagnostics(t: pd.DataFrame, base: Path) -> tuple[dict, pd.DataFrame]:
    d = t[t.slot10.between(36, 131)].copy()          # 06:00-22:00
    d["hour"] = d.slot10 // 6
    meta = pd.read_parquet(base / "segment_meta.parquet")[["roadseg_id", "road_class"]]
    seg = d[d.date.isin(WEEKDAYS)].groupby("roadseg_id").stale.mean()
    res = {
        "slots_06_22": int(len(d)),
        "exact_zero_change_share": round(float((d.run_len > 1).mean()), 4),
        "stale_share_slots": round(float(d.stale.mean()), 4),
        "stale_slots_equal_segment_mode": round(float(d.loc[d.stale, "is_seg_mode"].mean()), 4),
        "nonstale_slots_equal_segment_mode": round(float(d.loc[~d.stale, "is_seg_mode"].mean()), 4),
        "run_length_slot_weighted_quantiles": {str(q): int(d.run_len.quantile(q)) for q in [0.5, 0.75, 0.9, 0.95, 0.99]},
        "segments_with_weekday_stale_share_gt": {str(c): round(float((seg > c).mean()), 4) for c in [0.05, 0.1, 0.2, 0.5]},
        "stale_share_by_road_class": {int(k): round(float(v), 4) for k, v in d.merge(meta, on="roadseg_id").groupby("road_class").stale.mean().items()},
        "stale_share_by_date": {int(k): round(float(v), 4) for k, v in d.groupby("date").stale.mean().items()},
    }
    by_hour = d.groupby("hour").agg(stale=("stale", "mean"), zero_change=("run_len", lambda s: (s > 1).mean())).reset_index()
    return res, by_hour, seg


def plot(curves: dict, by_hour: pd.DataFrame, regs: dict, out: Path) -> None:
    fig, ax = plt.subplots(1, 3, figsize=(15, 4.2))
    a = ax[0]
    a.plot(by_hour.hour, 100 * by_hour.stale, color=ORANGE, lw=2, marker="o", ms=3, label="in >=60-min constant run")
    a.plot(by_hour.hour, 100 * by_hour.zero_change, color=BLUE, lw=2, marker="o", ms=3, label="equal to previous slot")
    a.set(xlabel="hour (Beijing)", ylabel="share of 10-min pace slots (%)")
    a.set_title("A  Constant Baidu pace by hour", fontsize=10, color=INK, loc="left")
    a.legend(frameon=False, fontsize=8)
    a = ax[1]
    for lab, col in [("V0_baseline", GREY), ("V1_drop_stale_slots", BLUE), ("V2_drop_stale_segments", ORANGE)]:
        g = curves[lab]
        a.plot(g.qr, g.t_index, color=col, lw=2, marker="o", ms=3, label=lab)
    a.set(xlabel="q / q95 (segment's own; not capacity)", ylabel="time index (lowest-load bin = 1)")
    a.set_title("B  Time index by load", fontsize=10, color=INK, loc="left")
    a.legend(frameon=False, fontsize=8)
    a = ax[2]
    xs = np.arange(len(next(iter(regs.values()))))
    width = 0.38
    for i, (lab, col) in enumerate([("V0_baseline", GREY), ("V1_drop_stale_slots", BLUE)]):
        r = regs[lab]
        keys = list(r.keys())
        b = [r[k]["IV_routed_od_edge"]["b"] for k in keys]
        se = [r[k]["IV_routed_od_edge"]["se_cluster_edge"] for k in keys]
        a.bar(np.arange(len(keys)) + (i - 0.5) * width, b, width, yerr=[1.96 * s for s in se], color=col, label=lab)
        a.set_xticks(np.arange(len(keys)), keys)
    a.axhline(0, color=INK2, lw=0.8)
    a.set(xlabel="typical load of segment-hour (weekday mean / q95)", ylabel="day-to-day elasticity (IV routed OD)")
    a.set_title("C  Elasticity by typical load", fontsize=10, color=INK, loc="left")
    a.legend(frameon=False, fontsize=8)
    for a in ax:
        style(a)
    fig.tight_layout()
    fig.savefig(out / "fig_lambda_robustness.png", dpi=200)
    plt.close(fig)


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--panel-name", default="segment_speed_flow_rn202505_v1")
    args = ap.parse_args(argv)
    base = ROOT / "outputs" / args.panel_name
    out = base / "lambda_robustness_v1"
    if out.exists():
        raise SystemExit(f"{out} exists; bump the version")
    out.mkdir(parents=True)

    t = flag_runs(pd.read_parquet(base / "segment_pace_10min.parquet"))
    diag, by_hour, seg_stale = diagnostics(t, base)
    by_hour.to_csv(out / "stale_by_hour.csv", index=False)
    print(json.dumps(diag, indent=1), flush=True)

    bad_segs = set(seg_stale[seg_stale > SEG_STALE_MAX].index)
    h0 = panel_from(t, base)
    h1 = panel_from(t[~t.stale], base)
    variants = {"V0_baseline": h0, "V1_drop_stale_slots": h1,
                "V2_drop_stale_segments": h0[~h0.roadseg_id.isin(bad_segs)],
                "V3_drop_0522": h0[h0.date != DROP_DAY], "V4_drop_stale_slots_and_0522": h1[h1.date != DROP_DAY]}
    res = {"diagnostics": diag, "rules": {"stale_run_min_slots": RUN_MIN, "segment_stale_share_max": SEG_STALE_MAX,
                                          "drop_day": DROP_DAY, "regime_edges": [str(x) for x in REGIMES]},
           "variants": {}}
    curves, regs = {}, {}
    for lab, h in variants.items():
        print("running", lab, flush=True)
        v = {"segment_hours": int(len(h)), "segments": int(h.roadseg_id.nunique()), **specs(h)}
        curves[lab] = curve(h)
        curves[lab].to_csv(out / f"curve_{lab}.csv")
        v["curve_local_slope"] = {str(k): (None if np.isnan(s) else round(float(s), 3)) for k, s in curves[lab].slope.items()}
        v["curve_stale_share"] = {str(k): round(float(s), 4) for k, s in curves[lab].stale.items()}
        if lab in ("V0_baseline", "V1_drop_stale_slots", "V4_drop_stale_slots_and_0522"):
            regs[lab] = regimes(h)
            v["by_typical_load_regime_S2"] = regs[lab]
        res["variants"][lab] = v
        (out / "lambda_robustness_summary.json").write_text(json.dumps(res, indent=2))
    plot(curves, by_hour, regs, out)
    print(json.dumps(res["variants"], indent=1))


if __name__ == "__main__":
    main()
