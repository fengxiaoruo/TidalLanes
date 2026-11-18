"""
RN -> Baidu segment matching: what it does, how good it is, and two model networks
==================================================================================

Data stage (reads only; Speed RN counts and node dictionary, Baidu segments, Speed OSM edges):
    - every RN pair with coordinates is classed as
        matched           (build_segment_speed_flow_panel.match rule)
        heading_mismatch  (a Baidu segment within 20 m of the chord midpoint, none within 30 deg)
        no_baidu_nearby   (no Baidu segment within 20 m)
        short_chord       (chord < 20 m; not attempted)
    - weekday (0519-0523) passes per pair, all day and 07-09 h
    - nearest OSM edge within 20 m (Speed V1-OSM full OSM network): road class and name,
      used as an independent check (name agreement) and to say what Baidu misses
    - RN length-flow allocated to rook-adjacent 3 km cell pairs (Platform map_rn_to_model_edges.length_alloc),
      classed against the gate5 nodes and edges
Plot stage: fig4 match zoom, fig5 match quality, fig6 network comparison, fig7 network zoom.

Output: data_work/outputs/<output-name>/ (refuses to overwrite; --replot redraws figures from that
directory's own tables only)
"""
from __future__ import annotations

import argparse
import glob
import json
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import shapely
from matplotlib.collections import LineCollection
from matplotlib.lines import Line2D

ROOT = Path(__file__).resolve().parents[2]  # -> data_work/
sys.path.insert(0, str(ROOT / "src/model/full_estimation"))
import build_segment_speed_flow_panel as bp  # noqa: E402

rnmap = bp.rnmap
R = bp.R
OSM_EDGES = Path("/Users/fxr/Library/CloudStorage/Dropbox/Projects/Projects_FengGu/Speed/Road_processing/V1-OSM/data/intermediate/beijing_osm_edges.parquet")
BUNDLE = ROOT / "outputs/model_input_edgeproj_v2_lcc_gate5_v1/data"
MANIFEST = ROOT.parent / "Platform/reference_inputs/manifest.json"
WEEKDAYS = [20250519, 20250520, 20250521, 20250522, 20250523]
CAT_ORDER = ["matched", "heading_mismatch", "no_baidu_nearby", "short_chord"]
BLUE, ORANGE, AQUA, GREY = "#2a78d6", "#eb6834", "#1baf7a", "#b9b8b3"
INK, INK2, GRID = "#0b0b0b", "#52514e", "#e4e3df"
CAT_COLOR = {"matched": BLUE, "heading_mismatch": AQUA, "no_baidu_nearby": ORANGE, "short_chord": GREY}
OSM_GROUP = {"motorway": "expressway", "motorway_link": "expressway", "trunk": "expressway", "trunk_link": "expressway",
             "primary": "primary/secondary", "primary_link": "primary/secondary", "secondary": "primary/secondary",
             "secondary_link": "primary/secondary", "tertiary": "tertiary", "tertiary_link": "tertiary",
             "residential": "residential/unclassified", "unclassified": "residential/unclassified",
             "living_street": "residential/unclassified", "service": "service"}
OSM_ORDER = ["expressway", "primary/secondary", "tertiary", "residential/unclassified", "service", "other or none"]
GUOMAO = (116.4615, 39.9087)


def to3857(lon, lat):
    lon, lat = np.asarray(lon, float), np.asarray(lat, float)
    return R * np.radians(lon), R * np.log(np.tan(np.pi / 4 + np.radians(lat) / 2))


def to4326(x, y):
    return np.degrees(np.asarray(x) / R), np.degrees(2 * np.arctan(np.exp(np.asarray(y) / R)) - np.pi / 2)


def style(ax, map_=False) -> None:
    if map_:
        ax.set_aspect("equal")
        ax.set_xticks([]), ax.set_yticks([])
        for s in ax.spines.values():
            s.set_color(GRID)
        return
    ax.grid(True, color=GRID, lw=0.6)
    ax.set_axisbelow(True)
    for s in ["top", "right"]:
        ax.spines[s].set_visible(False)
    for s in ["left", "bottom"]:
        ax.spines[s].set_color(INK2)
    ax.tick_params(colors=INK2, labelsize=9)


# ----------------------------------------------------------------------------- data stage
def pair_totals(files: list[str]) -> pd.DataFrame:
    cols = [f"n_valid_transitions_s{s:02d}" for s in range(36)]
    am = [f"n_valid_transitions_s{s:02d}" for s in range(6, 18)]  # block 1 starts 06:00 -> 07:00-09:00
    parts = []
    for i, f in enumerate(files):
        d = pd.read_csv(f, usecols=["date", "rn_pair_id", "time_block_6h", *cols], dtype={"rn_pair_id": str}, encoding="utf-8-sig")
        d = d[d.date.astype("int64").isin(WEEKDAYS)]
        if d.empty:
            continue
        v = d[cols].fillna(0).to_numpy()
        a = np.where(d.time_block_6h.eq(1).to_numpy()[:, None], d[am].fillna(0).to_numpy(), 0)
        parts.append(pd.DataFrame({"rn_pair_id": d.rn_pair_id.values, "n_wd": v.sum(1), "n_am": a.sum(1)}).groupby("rn_pair_id").sum())
        if i % 50 == 0:
            print(f"counts {i + 1}/{len(files)}", flush=True)
    return pd.concat(parts).groupby(level=0).sum().rename_axis("rn_pair_id").reset_index()


def classify(p: pd.DataFrame, seg: pd.DataFrame, m: pd.DataFrame) -> pd.Series:
    lines = shapely.from_wkb(seg.raw_geometry.values)
    tree = shapely.STRtree(lines)
    mid = shapely.points((p.x_from + p.x_to).values / 2, (p.y_from + p.y_to).values / 2)
    pi, _ = tree.query(mid, predicate="dwithin", distance=bp.MAX_DIST_M)
    near = np.zeros(len(p), bool)
    near[np.unique(pi)] = True
    cat = np.where(near, "heading_mismatch", "no_baidu_nearby").astype(object)
    cat[p.rn_pair_id.isin(set(m.rn_pair_id)).to_numpy()] = "matched"
    cat[(p.chord_m < bp.MIN_CHORD_M).to_numpy()] = "short_chord"
    return pd.Series(cat, index=p.index)


def osm_nearest(p: pd.DataFrame) -> pd.DataFrame:
    o = pd.read_parquet(OSM_EDGES, columns=["road_class", "road_name", "geometry"])
    g = shapely.from_wkb(o.geometry.values)
    g = shapely.transform(g, lambda c: np.column_stack(to3857(c[:, 0], c[:, 1])))
    tree = shapely.STRtree(g)
    mid = shapely.points((p.x_from + p.x_to).values / 2, (p.y_from + p.y_to).values / 2)
    (pi, oi), dist = tree.query_nearest(mid, max_distance=bp.MAX_DIST_M, return_distance=True, all_matches=False)
    out = pd.DataFrame({"osm_class": "other or none", "osm_name": ""}, index=p.index)
    out.iloc[pi, 0] = o.road_class.map(OSM_GROUP).fillna("other or none").values[oi]
    out.iloc[pi, 1] = o.road_name.fillna("").values[oi]
    return out


def build(out: Path) -> None:
    seg = pd.read_parquet(bp.SEGMENTS, columns=["roadseg_id", "raw_geometry", "dir_deg_final", "road_class", "roadname"]).drop_duplicates("roadseg_id").reset_index(drop=True)
    files = sorted(glob.glob(str(bp.COUNTS / "rn_pressure_counts_*.csv")))
    p = bp.rn_pairs(files)
    m = pd.read_parquet(ROOT / "outputs/segment_speed_flow_rn202505_v1/segment_rn_match.parquet")
    p["cat"] = classify(p, seg, m)
    p = p.join(osm_nearest(p))
    p = p.merge(m[["rn_pair_id", "roadseg_id", "dist_m", "angle_deg"]], on="rn_pair_id", how="left")
    p = p.merge(pair_totals(files), on="rn_pair_id", how="left").fillna({"n_wd": 0.0, "n_am": 0.0})
    p["vkm_wd"] = p.n_wd * p.chord_m / 1000
    p["vkm_am"] = p.n_am * p.chord_m / 1000
    p.drop(columns=["head"]).to_parquet(out / "rn_pair_class.parquet", index=False)

    meta = json.loads(MANIFEST.read_text())
    edges = pd.read_parquet(BUNDLE / "qsm_input_edges_square.parquet", columns=["grid_o", "grid_d"])
    nodes = set(pd.read_parquet(BUNDLE / "qsm_input_nodes_square.parquet", columns=["grid_id"]).grid_id)
    ek = set(zip(edges.grid_o, edges.grid_d))
    q = p.copy()
    q["lon_from"], q["lat_from"] = to4326(q.x_from, q.y_from)
    q["lon_to"], q["lat_to"] = to4326(q.x_to, q.y_to)
    al = rnmap.length_alloc(q[["rn_pair_id", "lon_from", "lat_from", "lon_to", "lat_to"]], meta["x_origin_3857"], meta["y_origin_3857"], ek)
    al = al.merge(p[["rn_pair_id", "n_wd", "n_am", "cat"]], on="rn_pair_id")
    al["f_wd"], al["f_am"] = al.n_wd * al.cells, al.n_am * al.cells
    al["f_wd_baidu"] = al.f_wd.where(al.cat.eq("matched"), 0.0)
    cp = al.groupby(["grid_from", "grid_to"], as_index=False)[["f_wd", "f_am", "f_wd_baidu"]].sum()
    on = pd.Series([k in ek for k in zip(cp.grid_from, cp.grid_to)])
    rev = pd.Series([(t, f) in ek for f, t in zip(cp.grid_from, cp.grid_to)])
    both = cp.grid_from.isin(nodes) & cp.grid_to.isin(nodes)
    cp["cls"] = np.select([on, both & rev, both], ["gate5_edge", "reverse_only", "no_edge_between_gate5_nodes"], "outside_gate5")
    cp.to_parquet(out / "cell_pair_flow.parquet", index=False)
    e = edges.rename(columns={"grid_o": "grid_from", "grid_d": "grid_to"}).merge(cp, on=["grid_from", "grid_to"], how="left").fillna(0.0)
    e.to_parquet(out / "gate5_edge_rn_flow.parquet", index=False)


# ----------------------------------------------------------------------------- plot stage
def cell_xy(ids: pd.Series, meta: dict) -> tuple[np.ndarray, np.ndarray]:
    rc = ids.str.extract(r"sq_(\d+)_(\d+)").astype(float)
    return meta["x_origin_3857"] + (rc[1].values + 0.5) * 3000, meta["y_origin_3857"] + (rc[0].values + 0.5) * 3000


def segs(df, x0="x_from", y0="y_from", x1="x_to", y1="y_to"):
    return np.stack([np.column_stack([df[x0], df[y0]]), np.column_stack([df[x1], df[y1]])], axis=1)


def baidu_lines(seg: pd.DataFrame, box=None) -> list:
    g = shapely.from_wkb(seg.raw_geometry.values)
    if box is not None:
        g = g[shapely.intersects(g, shapely.box(*box))]
    return [shapely.get_coordinates(x) for x in g]


def in_box(p: pd.DataFrame, box) -> pd.Series:
    xm, ym = (p.x_from + p.x_to) / 2, (p.y_from + p.y_to) / 2
    return xm.between(box[0], box[2]) & ym.between(box[1], box[3])


def lw_by_flow(v, lo=0.3, hi=2.2):
    s = np.log1p(np.asarray(v, float))
    return lo + (hi - lo) * s / max(s.max(), 1e-9)


def pick_gap_window(p: pd.DataFrame, cx: float, cy: float, half=2500.0) -> tuple:
    """5 km window inside 15 km of the centre with the most weekday RN vehicle-km off Baidu."""
    x, y = (p.x_from + p.x_to) / 2, (p.y_from + p.y_to) / 2
    off = p.cat.eq("no_baidu_nearby")
    k = pd.DataFrame({"i": np.floor(x / 2500), "j": np.floor(y / 2500), "v": p.vkm_wd.where(off, 0.0)})
    g = k.groupby(["i", "j"]).v.sum().reset_index()
    g["cx"], g["cy"] = (g.i + 0.5) * 2500, (g.j + 0.5) * 2500
    g = g[np.hypot(g.cx - cx, g.cy - cy) < 15000]
    # 2x2 block sums -> 5 km windows
    s = {(a, b): v for a, b, v in zip(g.i, g.j, g.v)}
    best = max(s, key=lambda ab: sum(s.get((ab[0] + di, ab[1] + dj), 0) for di in (0, 1) for dj in (0, 1)))
    x0, y0 = (best[0] + 1) * 2500, (best[1] + 1) * 2500
    return (x0 - half, y0 - half, x0 + half, y0 + half)


def draw_rn(ax, p, cats, flow="n_wd", z=2, lw=(0.6, 2.0), alpha=1.0):
    for k, c in enumerate(cats):
        d = p[p.cat.eq(c)]
        if len(d):
            ax.add_collection(LineCollection(segs(d), colors=CAT_COLOR[c], linewidths=lw_by_flow(d[flow], *lw), zorder=z + k,
                                             capstyle="round", alpha=alpha))


def fig4(p, seg, boxes, out):
    fig, axs = plt.subplots(1, 2, figsize=(12, 6.2))
    titles = ["(a) Interchange detail, Guomao, 1.6 km window", "(b) 5 km window with the most RN vehicle-km off Baidu"]
    for ax, box, t in zip(axs, boxes, titles):
        ax.add_collection(LineCollection(baidu_lines(seg, box), colors="#d9d7d0", linewidths=4.5, zorder=1, capstyle="round"))
        w = p[in_box(p, box)]
        draw_rn(ax, w, ["no_baidu_nearby", "heading_mismatch", "matched"])
        ax.set_xlim(box[0], box[2]), ax.set_ylim(box[1], box[3])
        style(ax, map_=True)
        ax.set_title(t, fontsize=10, color=INK, loc="left")
    h = [Line2D([], [], color="#d9d7d0", lw=5, label="Baidu speed segment"),
         Line2D([], [], color=BLUE, lw=2, label="RN pair matched to a Baidu segment"),
         Line2D([], [], color=AQUA, lw=2, label="RN pair near Baidu, direction differs > 30°"),
         Line2D([], [], color=ORANGE, lw=2, label="RN pair, no Baidu segment within 20 m")]
    fig.legend(handles=h, loc="lower center", ncol=4, frameon=False, fontsize=9)
    fig.text(0.01, 0.005, "RN pairs drawn as node-to-node chords (>= 20 m); line width ~ log weekday passes, 0519-0523. Chords shorter than 20 m not drawn.",
             fontsize=8, color=INK2)
    fig.tight_layout(rect=(0, 0.07, 1, 0.95))
    fig.savefig(out / "fig4_rn_baidu_match_zoom.png", dpi=200)
    plt.close(fig)


def name_agree(a: str, b: str) -> bool:
    a, b = str(a).strip(), str(b).strip()
    return bool(a) and bool(b) and (a == b or a in b or b in a)


def fig5(p, seg, out) -> dict:
    mt = p[p.cat.eq("matched")].merge(seg[["roadseg_id", "roadname", "road_class"]], on="roadseg_id", how="left")
    both = mt[(mt.roadname.fillna("").str.strip() != "") & (mt.osm_name.str.strip() != "")]
    agree = np.array([name_agree(a, b) for a, b in zip(both.roadname, both.osm_name)])
    wv = both.vkm_wd.to_numpy()
    tot = p.groupby("cat").vkm_wd.sum().reindex(CAT_ORDER).fillna(0)
    off = p[p.cat.isin(["no_baidu_nearby", "heading_mismatch"])]
    osm_off = off.groupby("osm_class").vkm_wd.sum().reindex(OSM_ORDER).fillna(0)
    osm_m = mt.groupby("osm_class").vkm_wd.sum().reindex(OSM_ORDER).fillna(0)

    fig, axs = plt.subplots(1, 3, figsize=(14, 4.2))
    a = axs[0]
    a.hist(mt.dist_m, bins=np.arange(0, 20.5, 1), weights=mt.vkm_wd / mt.vkm_wd.sum(), color=BLUE, rwidth=0.85)
    a.set(xlabel="distance from RN chord midpoint to Baidu line (m)", ylabel="share of matched vehicle-km")
    a.set_title(f"(a) Distance: median {mt.dist_m.median():.1f} m", fontsize=10, color=INK, loc="left")
    b = axs[1]
    b.hist(mt.angle_deg, bins=np.arange(0, 31, 1.5), weights=mt.vkm_wd / mt.vkm_wd.sum(), color=BLUE, rwidth=0.85)
    b.set(xlabel="heading difference, RN chord vs Baidu travel direction (°)", ylabel="share of matched vehicle-km")
    b.set_title(f"(b) Direction: median {mt.angle_deg.median():.1f}°", fontsize=10, color=INK, loc="left")
    c = axs[2]
    y = np.arange(len(OSM_ORDER))[::-1]
    sm, so = osm_m / osm_m.sum(), osm_off / osm_off.sum()
    c.barh(y + 0.2, sm.values, height=0.38, color=BLUE, label="matched to Baidu")
    c.barh(y - 0.2, so.values, height=0.38, color=ORANGE, label="not matched (no Baidu nearby or direction off)")
    c.set_yticks(y, OSM_ORDER, fontsize=9)
    c.set(xlabel="share of weekday RN vehicle-km in group")
    c.set_title("(c) OSM class of the road under the RN chord", fontsize=10, color=INK, loc="left")
    c.legend(frameon=False, fontsize=8, loc="center right")
    for ax in axs:
        style(ax)
    sh = tot / tot.sum()
    fig.text(0.01, 0.01, f"Weekday RN vehicle-km: matched {sh['matched']:.0%}, direction off {sh['heading_mismatch']:.0%}, "
             f"no Baidu within 20 m {sh['no_baidu_nearby']:.0%}, chord < 20 m {sh['short_chord']:.0%}. "
             f"Road-name check (Baidu name vs nearest OSM name, both named): agree on {np.average(agree, weights=wv):.0%} of vehicle-km.\n"
             "'other or none' = chord midpoint more than 20 m from any OSM road: mostly long chords (over 1 km) that cut across curves, so part of the unmatched flow is a chord-geometry limit, not a Baidu gap.",
             fontsize=8, color=INK2)
    fig.tight_layout(rect=(0, 0.08, 1, 1))
    fig.savefig(out / "fig5_rn_baidu_match_quality.png", dpi=200)
    plt.close(fig)
    return {"vkm_share_by_category": {k: round(float(v), 4) for k, v in sh.items()},
            "am_vkm_share_by_category": {k: round(float(v), 4) for k, v in (p.groupby("cat").vkm_am.sum() / p.vkm_am.sum()).items()},
            "matched_pairs": int(len(mt)), "matched_dist_m_p50_p90": [round(float(x), 2) for x in mt.dist_m.quantile([.5, .9])],
            "matched_angle_deg_p50_p90": [round(float(x), 2) for x in mt.angle_deg.quantile([.5, .9])],
            "name_check_pairs": int(len(both)), "name_agree_share_pairs": round(float(agree.mean()), 4),
            "name_agree_share_vkm": round(float(np.average(agree, weights=wv)), 4),
            "osm_class_share_matched_vkm": {k: round(float(v), 4) for k, v in sm.items()},
            "osm_class_share_unmatched_vkm": {k: round(float(v), 4) for k, v in so.items()},
            "unique_rn_chord_km": round(float(p.chord_m.sum() / 1000)),
            "unique_rn_chord_km_matched": round(float(p.chord_m[p.cat.eq("matched")].sum() / 1000))}


def grid_layer(ax, e, cp, meta, nodes, mode, thr):
    nx, ny = cell_xy(pd.Series(sorted(nodes)), meta)
    if mode == "baidu":
        u = e[e.grid_from < e.grid_to]
        x0, y0 = cell_xy(u.grid_from, meta)
        x1, y1 = cell_xy(u.grid_to, meta)
        ax.add_collection(LineCollection(np.stack([np.c_[x0, y0], np.c_[x1, y1]], 1), colors=BLUE, linewidths=0.9))
    else:
        g = cp[cp.cls.ne("outside_gate5")].copy()
        g["a"], g["b"] = np.minimum(g.grid_from, g.grid_to), np.maximum(g.grid_from, g.grid_to)
        g["in_gate5"] = g.cls.eq("gate5_edge")
        u = g.groupby(["a", "b"]).agg(f=("f_wd", "max"), in_gate5=("in_gate5", "max")).reset_index()
        for sel, col, lw in [(u.in_gate5 & (u.f >= thr), BLUE, 0.9), (u.in_gate5 & (u.f < thr), GREY, 0.9), (~u.in_gate5 & (u.f >= thr), ORANGE, 1.6)]:
            d = u[sel]
            x0, y0 = cell_xy(d.a, meta)
            x1, y1 = cell_xy(d.b, meta)
            ax.add_collection(LineCollection(np.stack([np.c_[x0, y0], np.c_[x1, y1]], 1), colors=col, linewidths=lw, linestyles="-" if col != GREY else ":"))
    ax.scatter(nx, ny, s=1.5, color=INK2, zorder=3, lw=0)


def fig6(p, seg, e, cp, meta, nodes, out, thr) -> dict:
    nx, ny = cell_xy(pd.Series(sorted(nodes)), meta)
    box = (nx.min() - 3000, ny.min() - 3000, nx.max() + 3000, ny.max() + 3000)
    w = p[in_box(p, box) & p.n_wd.gt(0) & p.cat.ne("short_chord")]
    fig, axs = plt.subplots(2, 2, figsize=(13, 13))
    a = axs[0, 0]
    col = {2: INK, 3: BLUE, 4: AQUA}
    for rc in [4, 3, 2]:
        s = seg[seg.road_class.eq(rc)]
        a.add_collection(LineCollection(baidu_lines(s), colors=col[rc], linewidths={2: 1.0, 3: 0.6, 4: 0.4}[rc]))
    a.set_title(f"(a) Version A road layer: Baidu speed segments ({len(seg):,})", fontsize=11, color=INK, loc="left")
    a.legend(handles=[Line2D([], [], color=col[k], lw=1.5, label=l) for k, l in [(2, "class 2 ring/expressway"), (3, "class 3 arterial"), (4, "class 4 secondary")]],
             frameon=False, fontsize=8, loc="lower left")
    b = axs[0, 1]
    draw_rn(b, w, ["no_baidu_nearby", "heading_mismatch", "matched"], z=2, lw=(0.05, 0.7))
    b.set_title(f"(b) Version B road layer: Unicom RN pairs with weekday flow ({len(w):,})", fontsize=11, color=INK, loc="left")
    b.legend(handles=[Line2D([], [], color=BLUE, lw=2, label="has Baidu speed"), Line2D([], [], color=AQUA, lw=2, label="Baidu nearby, direction off"),
                      Line2D([], [], color=ORANGE, lw=2, label="no Baidu speed")], frameon=False, fontsize=8, loc="lower left")
    c = axs[1, 0]
    grid_layer(c, e, cp, meta, nodes, "baidu", thr)
    c.set_title(f"(c) Version A model network: gate5, {len(nodes):,} nodes, {len(e):,} directed edges", fontsize=11, color=INK, loc="left")
    d = axs[1, 1]
    grid_layer(d, e, cp, meta, nodes, "rn", thr)
    g = cp[cp.cls.eq("no_edge_between_gate5_nodes") & (cp.f_wd >= thr)]
    weak = e[e.f_wd < thr]
    d.set_title(f"(d) Version B model network: adjacent gate5 nodes with RN flow", fontsize=11, color=INK, loc="left")
    d.legend(handles=[Line2D([], [], color=BLUE, lw=1.5, label="gate5 edge, RN flow above threshold"),
                      Line2D([], [], color=GREY, lw=1.5, ls=":", label=f"gate5 edge, weak RN flow ({len(weak)} directed)"),
                      Line2D([], [], color=ORANGE, lw=2, label=f"new edge: RN flow, no gate5 edge ({len(g)} directed)")],
             frameon=False, fontsize=8, loc="lower left")
    for ax in axs.ravel():
        ax.set_xlim(box[0], box[2]), ax.set_ylim(box[1], box[3])
        style(ax, map_=True)
    fig.text(0.01, 0.005, "Threshold for an RN-supported edge: weekday length-allocated RN flow at least the 5th percentile of flow on gate5 edges. "
             "Edges drawn undirected (either direction). Grid nodes are 3 km cells.", fontsize=8, color=INK2)
    fig.tight_layout(rect=(0, 0.015, 1, 1))
    fig.savefig(out / "fig6_network_versions.png", dpi=170)
    plt.close(fig)
    inside = cp[cp.cls.ne("outside_gate5")]
    return {"threshold_f_wd": round(float(thr), 1), "gate5_edges_below_threshold": int(len(weak)),
            "new_directed_edges_above_threshold": int(len(g)),
            "new_edges_share_of_flow_between_gate5_nodes": round(float(g.f_wd.sum() / inside.f_wd.sum()), 4),
            "new_edges_baidu_speed_share_of_their_flow_p50": round(float((g.f_wd_baidu / g.f_wd).median()), 4),
            "flow_share_by_cell_pair_class": {k: round(float(v), 4) for k, v in (cp.groupby("cls").f_wd.sum() / cp.f_wd.sum()).items()},
            "am_flow_share_by_cell_pair_class": {k: round(float(v), 4) for k, v in (cp.groupby("cls").f_am.sum() / cp.f_am.sum()).items()},
            "gate5_edges_baidu_speed_share_of_flow_p10_p50": [round(float(x), 4) for x in (e.f_wd_baidu / e.f_wd.replace(0, np.nan)).quantile([.1, .5])]}


def fig7(p, seg, e, cp, meta, nodes, box, out, thr):
    fig, axs = plt.subplots(1, 2, figsize=(12, 6.4))
    w = p[in_box(p, box) & p.cat.ne("short_chord") & p.n_wd.gt(0)]
    for ax, mode in zip(axs, ["baidu", "rn"]):
        if mode == "baidu":
            ax.add_collection(LineCollection(baidu_lines(seg, box), colors=INK2, linewidths=1.2, zorder=1))
        else:
            draw_rn(ax, w, ["matched", "heading_mismatch", "no_baidu_nearby"], z=1)
        grid_layer(ax, e, cp, meta, nodes, mode, thr)
        for c in ax.collections[-4:-1] if mode == "rn" else ax.collections[-2:-1]:
            c.set_linewidth(3.0), c.set_alpha(0.55), c.set_zorder(4)
        xs = np.arange(np.floor((box[0] - meta["x_origin_3857"]) / 3000), np.ceil((box[2] - meta["x_origin_3857"]) / 3000) + 1) * 3000 + meta["x_origin_3857"]
        ys = np.arange(np.floor((box[1] - meta["y_origin_3857"]) / 3000), np.ceil((box[3] - meta["y_origin_3857"]) / 3000) + 1) * 3000 + meta["y_origin_3857"]
        for x in xs:
            ax.axvline(x, color=GRID, lw=0.8, zorder=0)
        for y in ys:
            ax.axhline(y, color=GRID, lw=0.8, zorder=0)
        ax.set_xlim(box[0], box[2]), ax.set_ylim(box[1], box[3])
        style(ax, map_=True)
    axs[0].set_title("(a) Version A: Baidu segments and gate5 edges", fontsize=10, color=INK, loc="left")
    axs[1].set_title("(b) Version B: RN pairs and RN-supported cell edges", fontsize=10, color=INK, loc="left")
    h = [Line2D([], [], color=INK2, lw=1.2, label="Baidu segment"), Line2D([], [], color=BLUE, lw=2, label="RN pair with Baidu speed / gate5 edge"),
         Line2D([], [], color=ORANGE, lw=2, label="RN pair without Baidu speed / new cell edge"), Line2D([], [], color=GREY, lw=2, ls=":", label="gate5 edge, weak RN flow")]
    fig.legend(handles=h, loc="lower center", ncol=4, frameon=False, fontsize=9)
    fig.text(0.01, 0.005, "Same window as fig4(b). Thin grey lines: 3 km model cells. Thick translucent lines: model edges between cell centres.", fontsize=8, color=INK2)
    fig.tight_layout(rect=(0, 0.07, 1, 0.95))
    fig.savefig(out / "fig7_network_versions_zoom.png", dpi=200)
    plt.close(fig)


def plot(out: Path) -> None:
    p = pd.read_parquet(out / "rn_pair_class.parquet")
    cp = pd.read_parquet(out / "cell_pair_flow.parquet")
    e = pd.read_parquet(out / "gate5_edge_rn_flow.parquet")
    seg = pd.read_parquet(bp.SEGMENTS, columns=["roadseg_id", "raw_geometry", "road_class", "roadname"]).drop_duplicates("roadseg_id").reset_index(drop=True)
    meta = json.loads(MANIFEST.read_text())
    nodes = set(pd.read_parquet(BUNDLE / "qsm_input_nodes_square.parquet", columns=["grid_id"]).grid_id)
    gx, gy = to3857(*GUOMAO)
    gx, gy = float(gx), float(gy)
    b1 = (gx - 800, gy - 800, gx + 800, gy + 800)
    b2 = pick_gap_window(p, gx, gy)
    thr = float(e.f_wd[e.f_wd > 0].quantile(0.05))
    fig4(p, seg, [b1, b2], out)
    res = {"windows_lonlat": {"guomao": [round(float(v), 4) for v in (*to4326(b1[0], b1[1]), *to4326(b1[2], b1[3]))],
                              "gap_window": [round(float(v), 4) for v in (*to4326(b2[0], b2[1]), *to4326(b2[2], b2[3]))]}}
    w2 = p[in_box(p, b2)]
    res["gap_window_top_osm_names_off_baidu"] = w2[w2.cat.eq("no_baidu_nearby")].groupby("osm_name").vkm_wd.sum().nlargest(8).round(0).to_dict()
    res["match"] = fig5(p, seg, out)
    res["network"] = fig6(p, seg, e, cp, meta, nodes, out, thr)
    fig7(p, seg, e, cp, meta, nodes, b2, out, thr)
    (out / "match_network_summary.json").write_text(json.dumps(res, indent=2, ensure_ascii=False))
    print(json.dumps(res, indent=2, ensure_ascii=False))


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-name", default="rn_baidu_match_network_v1")
    ap.add_argument("--replot", action="store_true", help="redraw figures from this directory's own tables")
    args = ap.parse_args(argv)
    out = ROOT / "outputs" / args.output_name
    if args.replot:
        if not (out / "rn_pair_class.parquet").exists():
            raise SystemExit(f"{out} has no tables; run without --replot first")
    else:
        if out.exists():
            raise SystemExit(f"{out} exists; choose a new --output-name or use --replot")
        out.mkdir(parents=True)
        build(out)
    plot(out)


if __name__ == "__main__":
    main()
