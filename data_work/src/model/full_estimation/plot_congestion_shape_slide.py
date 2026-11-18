"""
Slide version of the gate5 congestion-shape figure (fig2), larger type and annotated zones
=========================================================================================

Redraws the binned curve written by plot_congestion_technology_gate5.py
(technology/data_curve_by_q_over_q95.csv; read only). No new estimation.

    left   travel-time index by bin of q/q95 (lowest-flow bin = 1), 95% edge-bootstrap band
    right  local elasticity between adjacent bins:
               (mean within-edge ln t, bin k  - same, bin k-1) / (mean within-edge ln q, bin k - same, bin k-1)
           drawn at the midpoint of the two bins' median q/q95 (fig2 drew it at bin k)

Output: data_work/outputs/<output-name>/ (refuses to overwrite)
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from plot_congestion_technology_gate5 import BLUE, INK, INK2, style  # noqa: E402

ROOT = Path(__file__).resolve().parents[3]
CURVE = ROOT / "outputs/all_traffic_speed_panel_rn202505_gate5_v1/technology/data_curve_by_q_over_q95.csv"
ZONES = [(0.0, 0.5, "flat"), (0.5, 1.05, "rising"), (1.05, 1.35, "falling")]
SHADE = ["#ffffff", "#eef4fc", "#f6f5f2"]


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-name", default="congestion_shape_slide_v1")
    args = ap.parse_args(argv)
    out = ROOT / "outputs" / args.output_name
    if out.exists():
        raise SystemExit(f"{out} exists; choose a new --output-name")
    out.mkdir(parents=True)
    g = pd.read_csv(CURVE)
    mid = (g.qr + g.qr.shift()) / 2
    m = g.slope.notna()
    plt.rcParams.update({"font.size": 12})
    fig, (l, r) = plt.subplots(1, 2, figsize=(13, 4.8))
    for ax in (l, r):
        for (a, b, lab), c in zip(ZONES, SHADE):
            ax.axvspan(a, b, color=c, lw=0, zorder=0)
        ax.axvline(1.0, color=INK2, lw=0.8, ls=":", zorder=1)
        ax.set_xlim(0.1, 1.32)
    l.fill_between(g.qr, g.t_lo, g.t_hi, color=BLUE, alpha=0.18, lw=0, zorder=2)
    l.plot(g.qr, g.t_index, color=BLUE, lw=2.2, marker="o", ms=6, zorder=3)
    l.set(xlabel="q / q95", ylabel="travel time relative to lowest-flow bin")
    l.set_title("Travel time", fontsize=13, color=INK, loc="left")
    k = g.t_index.idxmax()
    l.annotate(f"+{(g.t_index[k] - 1) * 100:.1f}%", xy=(g.qr[k], g.t_index[k]), xytext=(g.qr[k] - 0.3, g.t_index[k] - 0.005),
               fontsize=11, color=INK, arrowprops=dict(arrowstyle="-", color=INK2, lw=0.8))
    r.fill_between(mid[m], g.slope_lo[m], g.slope_hi[m], color=BLUE, alpha=0.18, lw=0, zorder=2)
    r.plot(mid[m], g.slope[m], color=BLUE, lw=2.2, marker="o", ms=6, zorder=3)
    r.axhline(0, color=INK2, lw=0.9)
    r.set(xlabel="q / q95", ylabel="local elasticity  Δ ln t / Δ ln q")
    r.set_title("Local elasticity between adjacent bins", fontsize=13, color=INK, loc="left")
    ytop = r.get_ylim()[1]
    for a, b, lab in ZONES:
        r.text((a + min(b, 1.32)) / 2 if a > 0 else 0.3, ytop * 0.92, lab, ha="center", fontsize=11, color=INK2)
        l.text((a + min(b, 1.32)) / 2 if a > 0 else 0.3, l.get_ylim()[1] - 0.008, lab, ha="center", va="top", fontsize=11, color=INK2)
    for ax in (l, r):
        style(ax)
        ax.tick_params(labelsize=11)
    fig.tight_layout()
    fig.savefig(out / "fig12_congestion_shape_slide.png", dpi=200)
    plt.close(fig)
    print("ok")


if __name__ == "__main__":
    main()
