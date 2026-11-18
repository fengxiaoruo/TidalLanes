"""
Shape of the congestion technology: candidate forms and the gate5 data
======================================================================

Figure 1 (illustrative, parameters assumed): travel time relative to free flow
and local elasticity d ln t / d ln q against q/c for
    constant elasticity  t = t_c (q/c)^lambda, lambda in {0.2, 1.1}, scaled to meet BPR at q = c
    BPR                  t = t0 [1 + a (q/c)^b], a = 0.15, b = 4
Figure 2 (data, weekdays, hourly, 06-22 h): within-edge travel-time index
against q/q95, where q95 is the edge's own 95th-percentile hourly all-purpose
flow. q95 is NOT an identified capacity c; the x-axis is labelled accordingly.
Local slopes are between adjacent bins; 95% bands from 200 edge-cluster
bootstrap draws.

Inputs: speed/ and rn_all/ of the panel output dir (read only).
Output: data_work/outputs/<output-name>/technology/ (refuses to overwrite)
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

BLUE, ORANGE, AQUA = "#2a78d6", "#eb6834", "#1baf7a"
INK, INK2, GRID = "#0b0b0b", "#52514e", "#e4e3df"
BINS = np.r_[0, np.arange(0.2, 1.21, 0.1), np.inf]


def style(ax) -> None:
    ax.grid(True, color=GRID, lw=0.6)
    ax.set_axisbelow(True)
    for s in ["top", "right"]:
        ax.spines[s].set_visible(False)
    for s in ["left", "bottom"]:
        ax.spines[s].set_color(INK2)
    ax.tick_params(colors=INK2, labelsize=9)


def theory(out: Path) -> None:
    x = np.linspace(0.01, 1.4, 300)
    a, b = 0.15, 4.0
    bpr = 1 + a * x ** b
    fig, (l, r) = plt.subplots(1, 2, figsize=(10, 4))
    l.plot(x, bpr, color=BLUE, lw=2)
    l.text(1.4, bpr[-1], "  BPR (a=0.15, b=4)", color=INK, va="center", fontsize=9)
    for lam, c in [(0.2, ORANGE), (1.1, AQUA)]:
        y = (1 + a) * x ** lam
        l.plot(x, y, color=c, lw=2)
        l.text(1.4, y[-1], f"  constant, λ={lam}", color=INK, va="center", fontsize=9)
    l.axhline(1, color=INK2, lw=0.8, ls=":")
    l.set(xlabel="q / c (flow over capacity)", ylabel="t / t0 (travel time over free flow)", ylim=(0, 2.0), xlim=(0, 1.4))
    l.set_title("Level: BPR has a free-flow floor and is convex;\nconstant elasticity goes to 0 at q=0 (concave if λ<1)", fontsize=10, color=INK, loc="left")
    e = a * b * x ** b / (1 + a * x ** b)
    r.plot(x, e, color=BLUE, lw=2)
    r.text(1.15, 0.95, "BPR  ", color=INK, ha="right", va="center", fontsize=9)
    for lam, c in [(0.2, ORANGE), (1.1, AQUA)]:
        r.plot(x, np.full_like(x, lam), color=c, lw=2)
        r.text(1.4, lam, f"  constant, λ={lam}", color=INK, va="center", fontsize=9)
    r.set(xlabel="q / c", ylabel="local elasticity  d ln t / d ln q", xlim=(0, 1.4), ylim=(0, 1.3))
    r.set_title("Local elasticity: BPR rises with q/c;\nconstant form is flat", fontsize=10, color=INK, loc="left")
    for ax in (l, r):
        style(ax)
    fig.text(0.01, 0.01, "Illustrative only: a, b and λ are assumed, not estimated. Constant forms scaled to equal BPR at q = c.", fontsize=8, color=INK2)
    fig.tight_layout(rect=(0, 0.04, 0.9, 1))
    fig.savefig(out / "fig1_candidate_forms.png", dpi=200)
    plt.close(fig)


def curve(h: pd.DataFrame) -> pd.DataFrame:
    g = h.groupby("bin", observed=True).agg(n=("ry", "size"), qr=("qrel", "median"), lx=("rx", "mean"), ly=("ry", "mean"))
    g["slope"] = g.ly.diff() / g.lx.diff()
    return g


def data(base: Path, out: Path) -> dict:
    k = ["grid_from", "grid_to", "date", "slot10"]
    t = pd.read_parquet(base / "speed/edge_pace_10min.parquet").merge(
        pd.read_parquet(base / "rn_all/edge_flow_10min.parquet"), on=k, how="left").fillna({"len_flow_all": 0.0})
    t["hour"] = t.slot10 // 6
    h = t[(t.coverage >= MIN_COVERAGE) & t.date.isin(WEEKDAYS)].groupby(["grid_from", "grid_to", "date", "hour"]).agg(
        pace=("pace_min_per_km", "mean"), flow=("len_flow_all", "sum"), n=("slot10", "size")).reset_index()
    h = h[(h.n == 6) & h.hour.between(6, 21) & (h.flow >= 5)].copy()
    h["edge"] = h.grid_from + ">" + h.grid_to
    h["y"], h["x"] = np.log(h.pace), np.log(h.flow)
    h["ry"] = h.y - h.groupby("edge").y.transform("mean")
    h["rx"] = h.x - h.groupby("edge").x.transform("mean")
    h["qrel"] = h.flow / h.groupby("edge").flow.transform(lambda s: s.quantile(0.95))
    h["bin"] = pd.cut(h.qrel, BINS)
    g = curve(h)
    # time index relative to the lowest-flow bin, in levels
    g["t_index"] = np.exp(g.ly - g.ly.iloc[0])
    edges = h.edge.unique()
    rng = np.random.default_rng(0)
    by = dict(tuple(h.groupby("edge")))
    boot_s, boot_t = [], []
    for _ in range(200):
        s = pd.concat([by[e] for e in rng.choice(edges, len(edges))])
        bg = curve(s).reindex(g.index)
        boot_s.append(bg.slope.values)
        boot_t.append(np.exp(bg.ly - bg.ly.iloc[0]).values)
    g["slope_lo"], g["slope_hi"] = np.nanpercentile(boot_s, [2.5, 97.5], axis=0)
    g["t_lo"], g["t_hi"] = np.nanpercentile(boot_t, [2.5, 97.5], axis=0)
    g.to_csv(out / "data_curve_by_q_over_q95.csv")

    fig, (l, r) = plt.subplots(1, 2, figsize=(10, 4))
    l.fill_between(g.qr, g.t_lo, g.t_hi, color=BLUE, alpha=0.18, lw=0)
    l.plot(g.qr, g.t_index, color=BLUE, lw=2, marker="o", ms=4)
    l.set(xlabel="q / q95  (hourly flow over the edge's own 95th-percentile flow; not capacity)",
          ylabel="travel-time index (lowest-flow bin = 1)")
    l.set_title("Level: flat at low flow, rising and convex near q95", fontsize=10, color=INK, loc="left")
    m = g.slope.notna()
    r.fill_between(g.qr[m], g.slope_lo[m], g.slope_hi[m], color=BLUE, alpha=0.18, lw=0)
    r.plot(g.qr[m], g.slope[m], color=BLUE, lw=2, marker="o", ms=4)
    r.axhline(0, color=INK2, lw=0.8)
    r.set(xlabel="q / q95", ylabel="local elasticity  d ln t / d ln q (OLS, within edge)")
    r.set_title("Local elasticity rises with load, turns negative beyond q95", fontsize=10, color=INK, loc="left")
    for ax in (l, r):
        style(ax)
    fig.text(0.01, 0.01, f"Gate5 edges, weekdays 0519-0523, 06-22 h, {len(h):,} edge-hours, {h.edge.nunique():,} edges.\n"
             "Corridor speed index; all-purpose mode1 flow; OLS within edge; 95% bands from 200 edge bootstraps.",
             fontsize=8, color=INK2)
    fig.tight_layout(rect=(0, 0.07, 1, 1))
    fig.savefig(out / "fig2_data_q_over_q95.png", dpi=200)
    plt.close(fig)
    return {"edge_hours": int(len(h)), "edges": int(h.edge.nunique()),
            "bins": [{"q_over_q95_median": round(float(a), 3), "n": int(n), "t_index": round(float(ti), 4),
                      "local_slope": None if np.isnan(s) else round(float(s), 3),
                      "ci95": None if np.isnan(s) else [round(float(lo), 3), round(float(hi), 3)]}
                     for a, n, ti, s, lo, hi in zip(g.qr, g.n, g.t_index, g.slope, g.slope_lo, g.slope_hi)]}


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--output-name", default="all_traffic_speed_panel_rn202505_gate5_v1")
    args = p.parse_args(argv)
    base = ROOT / "outputs" / args.output_name
    out = base / "technology"
    if out.exists():
        raise SystemExit(f"{out} exists; choose a new --output-name")
    out.mkdir(parents=True)
    theory(out)
    res = data(base, out)
    (out / "technology_summary.json").write_text(json.dumps(res, indent=2))
    print(json.dumps(res, indent=2))


if __name__ == "__main__":
    main()
