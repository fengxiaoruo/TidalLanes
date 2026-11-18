"""
Slide version of the model-network comparison (fig6 c, d), two large panels
==========================================================================

Reads the tables written by plot_rn_baidu_match_network.py (read only):
outputs/rn_baidu_match_network_v1/{cell_pair_flow, gate5_edge_rn_flow}.parquet
and the threshold in match_network_summary.json. Lines join 3 km cell centres;
each line is one adjacent cell pair (either direction).

    left   current model network (gate5 edges)
    right  same grid with Unicom flow: gate5 edge with flow above threshold,
           gate5 edge with weak flow, candidate new edge (flow above threshold, no gate5 edge)

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
import pandas as pd
from matplotlib.lines import Line2D

sys.path.insert(0, str(Path(__file__).resolve().parent))
import plot_rn_baidu_match_network as mn  # noqa: E402

ROOT = mn.ROOT
SRC = ROOT / "outputs/rn_baidu_match_network_v1"


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-name", default="network_versions_slide_v1")
    args = ap.parse_args(argv)
    out = ROOT / "outputs" / args.output_name
    if out.exists():
        raise SystemExit(f"{out} exists; choose a new --output-name")
    out.mkdir(parents=True)
    meta = json.loads(mn.MANIFEST.read_text())
    s = json.loads((SRC / "match_network_summary.json").read_text())["network"]
    thr = s["threshold_f_wd"]
    cp = pd.read_parquet(SRC / "cell_pair_flow.parquet")
    e = pd.read_parquet(SRC / "gate5_edge_rn_flow.parquet")
    nodes = set(pd.read_parquet(mn.BUNDLE / "qsm_input_nodes_square.parquet", columns=["grid_id"]).grid_id)
    nx, ny = mn.cell_xy(pd.Series(sorted(nodes)), meta)
    box = (nx.min() - 3000, ny.min() - 3000, nx.max() + 3000, ny.max() + 3000)
    plt.rcParams.update({"font.size": 13})
    fig, (a, b) = plt.subplots(1, 2, figsize=(14, 6.9))
    mn.grid_layer(a, e, cp, meta, nodes, "baidu", thr)
    a.set_title(f"Model network now: {len(nodes):,} cells, {len(e):,} directed edges", fontsize=14, color=mn.INK, loc="left")
    mn.grid_layer(b, e, cp, meta, nodes, "rn", thr)
    b.set_title("Same cells, with Unicom flow", fontsize=14, color=mn.INK, loc="left")
    fig.legend(handles=[Line2D([], [], color=mn.BLUE, lw=2, label="model edge with Unicom flow"),
                      Line2D([], [], color=mn.GREY, lw=2, ls=":", label=f"model edge, weak flow ({s['gate5_edges_below_threshold']} directed)"),
                      Line2D([], [], color=mn.ORANGE, lw=2.5, label=f"Unicom flow, no model edge ({s['new_directed_edges_above_threshold']} directed)")],
             frameon=False, fontsize=12, loc="lower center", ncol=3)
    for ax in (a, b):
        ax.set_xlim(box[0], box[2]), ax.set_ylim(box[1], box[3])
        mn.style(ax, map_=True)
    fig.tight_layout(rect=(0, 0.06, 1, 1))
    fig.savefig(out / "fig14_network_versions_slide.png", dpi=200, bbox_inches="tight", pad_inches=0.05)
    plt.close(fig)
    print("ok")


if __name__ == "__main__":
    main()
