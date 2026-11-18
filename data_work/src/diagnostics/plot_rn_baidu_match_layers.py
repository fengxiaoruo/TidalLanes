"""
Unicom road network, Baidu speed segments and match status in one window (three panels)
=======================================================================================

Reads the RN pair classification written by plot_rn_baidu_match_network.py
(outputs/rn_baidu_match_network_v1/rn_pair_class.parquet) and the Baidu
segment geometry used by build_segment_speed_flow_panel.py. Read only.

    (a) Unicom links with weekday passes (chords >= 20 m), line width ~ log passes
    (b) Baidu speed segments
    (c) Unicom links coloured by match status, over the Baidu segments

Output: data_work/outputs/<output-name>/ (refuses to overwrite)
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.collections import LineCollection
from matplotlib.lines import Line2D

sys.path.insert(0, str(Path(__file__).resolve().parent))
import plot_rn_baidu_match_network as mn  # noqa: E402

ROOT = mn.ROOT
CLASS = ROOT / "outputs/rn_baidu_match_network_v1/rn_pair_class.parquet"
HALF = 1500.0   # 3 km window
BAIDU = "#8d8b85"


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-name", default="rn_baidu_match_layers_v1")
    args = ap.parse_args(argv)
    out = ROOT / "outputs" / args.output_name
    if out.exists():
        raise SystemExit(f"{out} exists; choose a new --output-name")
    out.mkdir(parents=True)

    cx, cy = mn.to3857(*mn.GUOMAO)
    box = (float(cx) - HALF, float(cy) - HALF, float(cx) + HALF, float(cy) + HALF)
    p = pd.read_parquet(CLASS)
    p = p[mn.in_box(p, box) & (p.chord_m >= mn.bp.MIN_CHORD_M) & (p.n_wd > 0)]
    seg = pd.read_parquet(mn.bp.SEGMENTS, columns=["roadseg_id", "raw_geometry"]).drop_duplicates("roadseg_id")
    bl = mn.baidu_lines(seg, box)

    fig, axs = plt.subplots(1, 3, figsize=(15, 5.6))
    a, b, c = axs
    a.add_collection(LineCollection(mn.segs(p), colors=mn.INK, linewidths=mn.lw_by_flow(p.n_wd, 0.2, 1.4), capstyle="round"))
    a.set_title(f"(a) Unicom road network: {len(p):,} links with traffic", fontsize=10, color=mn.INK, loc="left")
    b.add_collection(LineCollection(bl, colors=BAIDU, linewidths=1.4, capstyle="round"))
    b.set_title(f"(b) Baidu speed segments: {len(bl):,}", fontsize=10, color=mn.INK, loc="left")
    c.add_collection(LineCollection(bl, colors="#d9d7d0", linewidths=4.0, zorder=1, capstyle="round"))
    mn.draw_rn(c, p, ["no_baidu_nearby", "heading_mismatch", "matched"], lw=(0.4, 1.8))
    sh = p.groupby("cat").vkm_wd.sum() / p.vkm_wd.sum()
    c.set_title(f"(c) Match status: {sh.get('matched', 0):.0%} of vehicle-km matched", fontsize=10, color=mn.INK, loc="left")
    for ax in axs:
        ax.set_xlim(box[0], box[2]), ax.set_ylim(box[1], box[3])
        mn.style(ax, map_=True)
    h = [Line2D([], [], color=mn.BLUE, lw=2, label="matched to a Baidu segment"),
         Line2D([], [], color=mn.AQUA, lw=2, label="near Baidu, direction differs > 30°"),
         Line2D([], [], color=mn.ORANGE, lw=2, label="no Baidu segment within 20 m"),
         Line2D([], [], color="#d9d7d0", lw=5, label="Baidu segment")]
    fig.legend(handles=h, loc="lower center", bbox_to_anchor=(0.5, 0.035), ncol=4, frameon=False, fontsize=9)
    fig.text(0.01, 0.008, "3 km window centred on Guomao. Unicom links are node-to-node chords >= 20 m with weekday passes (0519-0523); width ~ log passes.",
             fontsize=8, color=mn.INK2)
    fig.tight_layout(rect=(0, 0.1, 1, 1))
    fig.savefig(out / "fig10_rn_baidu_match_layers.png", dpi=200)
    plt.close(fig)
    print(len(p), len(bl), sh.round(4).to_dict())


if __name__ == "__main__":
    main()
