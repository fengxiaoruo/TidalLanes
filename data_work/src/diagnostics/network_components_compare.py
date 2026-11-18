"""
Strong / weak connected components of the candidate road networks
=================================================================

Networks (all read only):
    baidu_raw   Baidu speed network before manual edits (Speed 手工路网 backup_before_manual)
    v1          raw_connected_v1 (kunpeng), current gate5 mainline source
    v2          raw_connected_v2 (central clip, manual edits)
    v3          raw_connected_v3 (full city, manual edits)
    osm_all     Speed V1-OSM route_v3 directed arcs, all motor classes
    osm_major   same, classes tertiary and above (incl. links)

For each: nodes, directed edges, km, number of SCC and WCC, share of nodes / km
in the largest SCC and WCC, size of the 2nd-largest component, and the count of
components of at least 1 km. Edge km: length_m if present, else endpoint distance.
Output: data_work/outputs/<output-name>/ (refuses to overwrite)
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import connected_components

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
SPEED = Path("/Users/fxr/Library/CloudStorage/Dropbox/Projects/Projects_FengGu/Speed/Road_processing")
ARCS = SPEED / "V1-OSM/data/intermediate/route_v3/beijing_arcs.parquet"
MAJOR = {"motorway", "motorway_link", "trunk", "trunk_link", "primary", "primary_link",
         "secondary", "secondary_link", "tertiary", "tertiary_link"}


def baidu_like(p: Path) -> pd.DataFrame:
    e = pd.read_excel(p)
    if "length_m" in e:
        km = e.length_m / 1000
    else:
        km = np.hypot(e.end_x_m - e.start_x_m, e.end_y_m - e.start_y_m) / 1000
    return pd.DataFrame({"u": e.from_node_id.astype(str), "v": e.to_node_id.astype(str), "km": km,
                         "manual": e.edge_id.astype(str).str.startswith("MANUAL")})


def osm(major: bool) -> pd.DataFrame:
    a = pd.read_parquet(ARCS, columns=["from_node_idx", "to_node_idx", "length_m", "road_class"])
    if major:
        a = a[a.road_class.isin(MAJOR)]
    return pd.DataFrame({"u": a.from_node_idx.astype(str), "v": a.to_node_idx.astype(str), "km": a.length_m / 1000,
                         "manual": False})


def components(e: pd.DataFrame) -> dict:
    codes, uniq = pd.factorize(pd.concat([e.u, e.v], ignore_index=True))
    n, m = len(uniq), len(e)
    iu, iv = codes[:m], codes[m:]
    g = coo_matrix((np.ones(m), (iu, iv)), shape=(n, n)).tocsr()
    out = {"nodes": int(n), "edges": int(m), "km": round(float(e.km.sum()), 1),
           "manual_edges": int(e.manual.sum())}
    for kind in ["strong", "weak"]:
        k, lab = connected_components(g, directed=True, connection=kind)
        size = np.bincount(lab, minlength=k)
        # km of an edge counts toward a component only if both ends are in it
        same = lab[iu] == lab[iv]
        km = np.bincount(lab[iu][same], weights=e.km.to_numpy()[same], minlength=k)
        order = np.argsort(-km)
        out[kind] = {"n_components": int(k),
                     "largest_node_share": round(float(size[order[0]] / n), 4),
                     "largest_km_share": round(float(km[order[0]] / e.km.sum()), 4),
                     "second_km": round(float(km[order[1]]), 1) if k > 1 else 0.0,
                     "components_ge_1km": int((km >= 1).sum()),
                     "singleton_nodes": int((size == 1).sum())}
    return out


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-name", default="network_components_compare_v1")
    args = ap.parse_args(argv)
    out = ROOT / "outputs" / args.output_name
    if out.exists():
        raise SystemExit(f"{out} exists; choose a new --output-name")
    nets = {
        "baidu_raw": lambda: baidu_like(SPEED / "手工路网_北京_Processed_Data/directed_edges_backup_before_manual.xlsx"),
        "v1": lambda: baidu_like(SRC / "raw_connected_v1/Processed_Data/directed_edges.xlsx"),
        "v2": lambda: baidu_like(SRC / "raw_connected_v2/Processed_Data/directed_edges.xlsx"),
        "v3": lambda: baidu_like(SRC / "raw_connected_v3/Processed_Data/directed_edges.xlsx"),
        "osm_all": lambda: osm(False),
        "osm_major": lambda: osm(True),
    }
    res = {}
    for k, f in nets.items():
        res[k] = components(f())
        print(k, json.dumps(res[k], ensure_ascii=False), flush=True)
    out.mkdir(parents=True)
    (out / "summary.json").write_text(json.dumps(res, indent=2, ensure_ascii=False))
    rows = [{"network": k, **{f"{c}_{kk}": vv for c in ["strong", "weak"] for kk, vv in v[c].items()},
             **{kk: v[kk] for kk in ["nodes", "edges", "km", "manual_edges"]}} for k, v in res.items()]
    pd.DataFrame(rows).to_csv(out / "components.csv", index=False)


if __name__ == "__main__":
    main()
