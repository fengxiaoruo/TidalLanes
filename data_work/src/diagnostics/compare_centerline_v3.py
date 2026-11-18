"""
Centerline on raw_connected_v3 (v11 corridor rules) vs the other grid-edge networks
==================================================================================
Model-origin 3 km square grid. Networks:
    cl_v11   outputs/manual_centerline_rules_v11 (centerline on Baidu raw v1)
    cl_v3    outputs/centerline_v3_rules_v11_v1  (same rules, v3 roads incl. manual connectors)
    gate5    outputs/model_input_edgeproj_v2_lcc_gate5_v1 (current model bundle; tau_obs_min / tau_ff_min)
(v1 of this comparison read gate5 t_min / t_ff_min, i.e. v11 centerline times; use v2)
    osm1a / osm1b  outputs/osm_grid_travel_time_v2 (multi-access mean times tm_*)
For each: directed edges, strongly / weakly connected components over grid cells, and
the share of Unicom weekday grid-edge flow (rn_osm_flow_v1, adjacent cells routed on
OSM) that falls on an edge of the network. Pairwise on common directed edges:
correlation of log AM time, log FF time, log(AM/FF), and the directional log ratio.
Inputs read only. Output: data_work/outputs/<output-name>/ (refuses to overwrite)
"""
from __future__ import annotations

import argparse
import itertools
import json
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import connected_components

ROOT = Path(__file__).resolve().parents[2]
O = ROOT / "outputs"
K = ["grid_from", "grid_to"]


def bundle(p: Path, cols=("t_min", "t_ff_min")) -> pd.DataFrame:
    # centerline bundles: t_min / t_ff_min are their own times; the gate5 bundle's model times
    # are tau_obs_min / tau_ff_min (its t_min / t_ff_min are inherited v11 centerline values)
    d = pd.read_parquet(p, columns=["grid_o", "grid_d", *cols])
    return d.rename(columns={"grid_o": "grid_from", "grid_d": "grid_to", cols[0]: "AM", cols[1]: "FF"})


def osm(s: str) -> pd.DataFrame:
    d = pd.read_parquet(O / f"osm_grid_travel_time_v2/pairs_{s}_sq3000_model_origin.parquet", columns=K + ["tm_AM", "tm_FF"])
    return d.rename(columns={"tm_AM": "AM", "tm_FF": "FF"}).dropna()


def comps(d: pd.DataFrame) -> dict:
    codes, uniq = pd.factorize(pd.concat([d.grid_from, d.grid_to], ignore_index=True))
    n = len(d)
    G = coo_matrix((np.ones(n), (codes[:n], codes[n:])), shape=(len(uniq),) * 2)
    out = {"cells": int(len(uniq))}
    for c in ["strong", "weak"]:
        k, lab = connected_components(G, directed=True, connection=c)
        out[f"{c}_components"] = int(k)
        out[f"{c}_largest_cell_share"] = round(float(np.bincount(lab).max() / len(uniq)), 4)
    return out


def dlog(d: pd.DataFrame) -> pd.Series:
    s = np.log(d.set_index(K).AM)
    rev = s.reindex(pd.MultiIndex.from_arrays([s.index.get_level_values(1), s.index.get_level_values(0)]))
    return pd.Series(s.to_numpy() - rev.to_numpy(), index=s.index).dropna()


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-name", default="centerline_v3_compare_v2")
    args = ap.parse_args(argv)
    out = O / args.output_name
    if out.exists():
        raise SystemExit(f"{out} exists; choose a new --output-name")
    out.mkdir(parents=True)
    nets = {"cl_v11": bundle(O / "manual_centerline_rules_v11/data/qsm_input_edges_square.parquet"),
            "cl_v3": bundle(O / "centerline_v3_rules_v11_v1/data/qsm_input_edges_square.parquet"),
            "gate5": bundle(O / "model_input_edgeproj_v2_lcc_gate5_v1/data/qsm_input_edges_square.parquet", ("tau_obs_min", "tau_ff_min")),
            "osm1a": osm("osm1a"), "osm1b": osm("osm1b_L300")}
    fl = pd.read_parquet(O / "rn_osm_flow_v1/grid_edge_flow_sq3000_model_origin.parquet", columns=K + ["f_wd_osm"])
    rc = lambda s: s.str.split("_", expand=True).iloc[:, 1:].astype(int).to_numpy()
    adj = np.abs(rc(fl.grid_from) - rc(fl.grid_to)).sum(axis=1) == 1
    fl = fl[adj]
    res = {"networks": {}, "pairwise": []}
    for k, d in nets.items():
        a, b = rc(d.grid_from), rc(d.grid_to)
        m = fl.merge(d[K].assign(on=1), on=K, how="left")
        res["networks"][k] = {"directed_edges": int(len(d)),
                              "share_4_neighbour": round(float((np.abs(a - b).sum(axis=1) == 1).mean()), 4),
                              "unicom_adjacent_flow_share_on_network": round(float(m.f_wd_osm[m.on.notna()].sum() / m.f_wd_osm.sum()), 4),
                              "AM_over_FF_median": round(float((d.AM / d.FF).median()), 3),
                              "AM_min_median": round(float(d.AM.median()), 2), **comps(d)}
    for x, y in itertools.combinations(nets, 2):
        m = nets[x].merge(nets[y], on=K, suffixes=("_x", "_y"))
        dd = pd.concat([dlog(nets[x]).rename("x"), dlog(nets[y]).rename("y")], axis=1, join="inner")
        res["pairwise"].append({
            "a": x, "b": y, "common_edges": int(len(m)),
            "corr_log_AM": round(float(np.corrcoef(np.log(m.AM_x), np.log(m.AM_y))[0, 1]), 3),
            "corr_log_FF": round(float(np.corrcoef(np.log(m.FF_x), np.log(m.FF_y))[0, 1]), 3),
            "corr_log_AM_over_FF": round(float(np.corrcoef(np.log(m.AM_x / m.FF_x), np.log(m.AM_y / m.FF_y))[0, 1]), 3),
            "corr_directional_log_ratio": round(float(np.corrcoef(dd.x, dd.y)[0, 1]), 3) if len(dd) > 2 else None,
            "sign_agree_directional": round(float((np.sign(dd.x) == np.sign(dd.y)).mean()), 3) if len(dd) else None,
            "median_AM_ratio_b_over_a": round(float((m.AM_y / m.AM_x).median()), 3)})
    pd.DataFrame(res["networks"]).T.to_csv(out / "networks.csv")
    pd.DataFrame(res["pairwise"]).to_csv(out / "pairwise.csv", index=False)
    (out / "summary.json").write_text(json.dumps(res, indent=2, ensure_ascii=False))
    with pd.option_context("display.width", 250, "display.max_columns", 30):
        print(pd.DataFrame(res["networks"]).T)
        print(pd.DataFrame(res["pairwise"]))


if __name__ == "__main__":
    main()
