"""
Citywide match status of Unicom links, and how often a nearby parallel road competes
====================================================================================

Figure 11 (two panels, gate5 extent):
    (a) Unicom links with weekday passes coloured by match status
    (b) share of weekday Unicom vehicle-km matched to a Baidu segment, by 3 km cell
        (cells with fewer than MIN_CELL_VKM vehicle-km left blank)

Ambiguity check (written to ambiguity_summary.json). For every matched chord,
all Baidu segments passing the same rule (midpoint within 20 m, heading within
30 deg of the signed local tangent) are listed again. A match is
    contested      if a second valid segment exists;
    close-contested if that segment is within CLOSE_M of the winner's distance;
    and is split by whether the runner-up has a different road name or a
    different Baidu road class (main vs side road, elevated vs ground).
Also reported: matched vehicle-km whose nearest OSM road is a minor class
while the Baidu segment is class 2 (possible side road given a main-road speed).

Inputs read only: outputs/rn_baidu_match_network_v1/rn_pair_class.parquet,
Baidu segments of build_segment_speed_flow_panel.py, the gate5 bundle.
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
import shapely
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.lines import Line2D

sys.path.insert(0, str(Path(__file__).resolve().parent))
import plot_rn_baidu_match_network as mn  # noqa: E402

bp = mn.bp
ROOT = mn.ROOT
CLASS = ROOT / "outputs/rn_baidu_match_network_v1/rn_pair_class.parquet"
CLOSE_M, MIN_CELL_VKM = 5.0, 2000.0
BLUES = LinearSegmentedColormap.from_list("blues", ["#cde2fb", "#86b6ef", "#3987e5", "#1c5cab", "#0d366b"])


def candidates(q: pd.DataFrame, seg: pd.DataFrame) -> pd.DataFrame:
    """All valid (distance and heading) Baidu candidates of each chord in q, same rule as bp.match."""
    lines = shapely.from_wkb(seg.raw_geometry.values)
    tree = shapely.STRtree(lines)
    mid = shapely.points((q.x_from + q.x_to).values / 2, (q.y_from + q.y_to).values / 2)
    pi, si = tree.query(mid, predicate="dwithin", distance=bp.MAX_DIST_M)
    c = pd.DataFrame({"i": q.index.values[pi], "s": si})
    pts, ln = mid[pi], lines[si]
    c["dist"] = shapely.distance(pts, ln)
    at, L = shapely.line_locate_point(ln, pts), shapely.length(ln)
    a = shapely.get_coordinates(shapely.line_interpolate_point(ln, np.clip(at - 10, 0, L)))
    b = shapely.get_coordinates(shapely.line_interpolate_point(ln, np.clip(at + 10, 0, L)))
    t = bp.heading(b[:, 0] - a[:, 0], b[:, 1] - a[:, 1])
    sem = seg.dir_deg_final.to_numpy(dtype=float)[si]
    t = np.where(bp.angdiff(t, sem) > 90, (t + 180) % 360, t)
    head = bp.heading(q.x_to - q.x_from, q.y_to - q.y_from).to_numpy()
    c["angle"] = bp.angdiff(head[np.searchsorted(q.index.values, c.i.values)], t)
    c = c[c.angle <= bp.MAX_ANGLE].sort_values(["i", "dist"])
    c["rank"] = c.groupby("i").cumcount()
    return c


def ambiguity(p: pd.DataFrame, seg: pd.DataFrame) -> dict:
    q = p[p.cat.eq("matched")].sort_index()
    c = candidates(q, seg)
    first = c[c["rank"].eq(0)].set_index("i")
    second = c[c["rank"].eq(1)].set_index("i")
    d = pd.DataFrame({"vkm": q.vkm_wd, "s1": first.s, "d1": first.dist}).join(second[["s", "dist"]].rename(columns={"s": "s2", "dist": "d2"}))
    # the re-run must reproduce the original match
    same = (seg.roadseg_id.values[d.s1.astype(int)] == q.roadseg_id.values)
    name, rc = seg.roadname.fillna("").to_numpy(), seg.road_class.to_numpy()
    has2 = d.s2.notna().to_numpy()
    s1, s2 = d.s1.astype(int).to_numpy(), d.s2.fillna(0).astype(int).to_numpy()
    close = has2 & ((d.d2 - d.d1).fillna(np.inf).to_numpy() < CLOSE_M)
    diffname = has2 & (name[s1] != name[s2])
    diffclass = has2 & (rc[s1] != rc[s2])
    w = d.vkm.to_numpy()
    sh = lambda m: round(float(w[m].sum() / w.sum()), 4)  # noqa: E731
    minor = q.osm_class.isin(["tertiary", "residential/unclassified", "service"]).to_numpy()
    cls2 = rc[s1] == 2
    return {
        "matched_pairs": int(len(q)), "rerun_reproduces_original_match": round(float(same.mean()), 4),
        "share_matched_vkm": {
            "one_valid_candidate": sh(~has2),
            "second_valid_candidate": sh(has2),
            "second_within_5m_of_winner": sh(close),
            "second_within_5m_different_name": sh(close & diffname),
            "second_within_5m_different_baidu_class": sh(close & diffclass),
            "baidu_class2_but_nearest_osm_minor": sh(cls2 & minor),
        },
        "rule": f"valid = midpoint <= {bp.MAX_DIST_M} m and heading <= {bp.MAX_ANGLE} deg; close = runner-up distance minus winner distance < {CLOSE_M} m",
    }


def fig11(p: pd.DataFrame, out: Path) -> dict:
    """Two separate square maps: fig11a links by status, fig11b matched share by 3 km cell."""
    meta = json.loads(mn.MANIFEST.read_text())
    nodes = pd.read_parquet(mn.BUNDLE / "qsm_input_nodes_square.parquet", columns=["grid_id"]).grid_id
    nx, ny = mn.cell_xy(nodes, meta)
    box = (nx.min() - 3000, ny.min() - 3000, nx.max() + 3000, ny.max() + 3000)
    w = p[mn.in_box(p, box) & p.n_wd.gt(0) & p.cat.ne("short_chord")]
    tot = w.groupby("cat").vkm_wd.sum() / w.vkm_wd.sum()
    note = "Model (gate5) area. Weekday passes 0519-0523; chords >= 20 m."

    fig, a = plt.subplots(figsize=(8, 7.6))
    mn.draw_rn(a, w, ["no_baidu_nearby", "heading_mismatch", "matched"], z=2, lw=(0.05, 0.7))
    a.set_title(f"Unicom links by match status: {tot.get('matched', 0):.1%} of vehicle-km matched in the model area",
                fontsize=10, color=mn.INK, loc="left")
    fig.legend(handles=[Line2D([], [], color=mn.BLUE, lw=2, label="matched to a Baidu segment"),
                        Line2D([], [], color=mn.AQUA, lw=2, label="near Baidu, direction differs > 30°"),
                        Line2D([], [], color=mn.ORANGE, lw=2, label="no Baidu segment within 20 m")],
               frameon=False, fontsize=8.5, loc="lower center", ncol=3, bbox_to_anchor=(0.5, 0.03))
    a.set_xlim(box[0], box[2]), a.set_ylim(box[1], box[3])
    mn.style(a, map_=True)
    fig.text(0.01, 0.008, note + " Line width ~ log passes.", fontsize=7.5, color=mn.INK2)
    fig.tight_layout(rect=(0, 0.07, 1, 1))
    fig.savefig(out / "fig11a_rn_match_citywide_links.png", dpi=200)
    plt.close(fig)

    xm, ym = (w.x_from + w.x_to) / 2, (w.y_from + w.y_to) / 2
    col = np.floor((xm - meta["x_origin_3857"]) / 3000).astype(int)
    row = np.floor((ym - meta["y_origin_3857"]) / 3000).astype(int)
    g = pd.DataFrame({"c": col, "r": row, "v": w.vkm_wd, "m": w.vkm_wd.where(w.cat.eq("matched"), 0.0)}).groupby(["c", "r"]).sum()
    g = g[g.v >= MIN_CELL_VKM]
    g["sh"] = g.m / g.v
    fig, b = plt.subplots(figsize=(8, 7.6))
    for (cc, rr), s in g.sh.items():
        x0, y0 = meta["x_origin_3857"] + cc * 3000, meta["y_origin_3857"] + rr * 3000
        b.add_patch(plt.Rectangle((x0, y0), 3000, 3000, facecolor=BLUES(s), edgecolor="white", lw=0.4))
    b.set_xlim(box[0], box[2]), b.set_ylim(box[1], box[3])
    mn.style(b, map_=True)
    sm = plt.cm.ScalarMappable(cmap=BLUES, norm=plt.Normalize(0, 1))
    cb = fig.colorbar(sm, ax=b, orientation="horizontal", fraction=0.04, pad=0.02, aspect=40)
    cb.set_label("share of the cell's Unicom vehicle-km matched to Baidu", fontsize=8.5, color=mn.INK2)
    cb.ax.tick_params(labelsize=8, colors=mn.INK2)
    q = g.sh.quantile([0.1, 0.5, 0.9])
    b.set_title(f"Matched share by 3 km cell: median {q[0.5]:.0%}, p10 {q[0.1]:.0%}, p90 {q[0.9]:.0%}", fontsize=10, color=mn.INK, loc="left")
    fig.text(0.01, 0.008, note + f" Cells with fewer than {MIN_CELL_VKM:,.0f} sample vehicle-km left blank.", fontsize=7.5, color=mn.INK2)
    fig.tight_layout(rect=(0, 0.03, 1, 1))
    fig.savefig(out / "fig11b_rn_match_citywide_cells.png", dpi=200)
    plt.close(fig)
    return {"matched_vkm_share_model_area": round(float(tot.get("matched", 0)), 4),
            "matched_share_cell_p10_p50_p90": [round(float(v), 3) for v in q], "cells": int(len(g))}


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-name", default="rn_baidu_match_citywide_v1")
    args = ap.parse_args(argv)
    out = ROOT / "outputs" / args.output_name
    if out.exists():
        raise SystemExit(f"{out} exists; choose a new --output-name")
    out.mkdir(parents=True)
    p = pd.read_parquet(CLASS)
    seg = pd.read_parquet(bp.SEGMENTS, columns=["roadseg_id", "raw_geometry", "dir_deg_final", "road_class", "roadname"]).drop_duplicates("roadseg_id").reset_index(drop=True)
    res = {"fig11": fig11(p, out), "ambiguity": ambiguity(p, seg)}
    (out / "ambiguity_summary.json").write_text(json.dumps(res, indent=2, ensure_ascii=False))
    print(json.dumps(res, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
