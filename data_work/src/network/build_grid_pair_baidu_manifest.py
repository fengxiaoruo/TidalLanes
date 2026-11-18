"""
Sample of directed adjacent grid pairs for a Baidu route-API benchmark (nextstep D step 4)
=======================================================================================

Universe: reciprocal 3 km model-origin pairs (both directions present) that exist in the
OSM scheme 1B pairs (osm_grid_travel_time_v2) and in the current gate5 (v1) bundle.
Stratified by (i) observed share of the 1B path time, both directions >= 0.8 or not, and
(ii) tercile of disagreement |ln(tm_AM 1B / tau_obs_min gate5)| averaged over the two directions.
The gate5 model times are tau_obs_min / tau_ff_min (edgeproj). The bundle's t_min / t_ff_min
are the inherited v11 centerline times; v1 of this sample stratified on those by mistake
(2026-11-06), so v2 is the sample to crawl.
N_UNDIRECTED undirected pairs, equal allocation across the 6 strata, both directions kept.
Origin and destination are the WGS84 cell centres (Baidu snaps them to the nearest road),
no waypoints. Written in the column layout read by
src.raw_baidu_validation.query_baidu_direction --sample-manifest.
v3 (2026-11-09, daily quota 5000): --n-undirected 700 --keep-from grid_pair_baidu_validation_v2
keeps every v2 pair and tops each stratum up to n/6 with new draws (1400 directed rows,
3 panels = 4200 requests per day).
Output: data_work/outputs/<output-name>/data/sample_manifest.csv (refuses to overwrite)
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
from pyproj import Transformer

ROOT = Path(__file__).resolve().parents[2]
PAIRS = ROOT / "outputs/osm_grid_travel_time_v2/pairs_osm1b_L300_sq3000_model_origin.parquet"
GATE5 = ROOT / "outputs/model_input_edgeproj_v2_lcc_gate5_v1/data/qsm_input_edges_square.parquet"
MANIFEST = ROOT.parent / "Platform/reference_inputs/manifest.json"
N_UNDIRECTED = 300
TO_WGS = Transformer.from_crs(3857, 4326, always_xy=True)


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-name", default="grid_pair_baidu_validation_v2")
    ap.add_argument("--n-undirected", type=int, default=N_UNDIRECTED)
    ap.add_argument("--keep-from", default=None, help="earlier sample version whose pairs are kept")
    args = ap.parse_args(argv)
    out = ROOT / "outputs" / args.output_name
    if out.exists():
        raise SystemExit(f"{out} exists; choose a new --output-name")
    meta = json.loads(MANIFEST.read_text())
    p = pd.read_parquet(PAIRS)
    g = pd.read_parquet(GATE5, columns=["grid_o", "grid_d", "tau_obs_min", "tau_ff_min", "t_min", "t_ff_min"]).rename(columns={"grid_o": "grid_from", "grid_d": "grid_to"})
    d = p.merge(g, on=["grid_from", "grid_to"]).dropna(subset=["tm_AM"])
    d["key"] = [tuple(sorted(x)) for x in zip(d.grid_from, d.grid_to)]
    d["dis"] = np.abs(np.log(d.tm_AM / d.tau_obs_min))
    u = d.groupby("key").agg(n=("dis", "size"), dis=("dis", "mean"), obs_min=("obs_share_AM", "min"))
    u = u[u.n == 2].copy()
    u["obs_hi"] = u.obs_min >= 0.8
    u["dis_t"] = pd.qcut(u.dis, 3, labels=["low", "mid", "high"])
    rng = np.random.default_rng(20261106)
    per = args.n_undirected // 6
    keep = set()
    if args.keep_from:
        k = pd.read_csv(ROOT / "outputs" / args.keep_from / "data/sample_manifest.csv", usecols=["grid_from", "grid_to"])
        keep = {tuple(sorted(x)) for x in zip(k.grid_from, k.grid_to)}
        missing = keep - set(u.index)
        if missing:
            raise SystemExit(f"{len(missing)} kept pairs not in the universe")
    pick = []
    for _, s in u.groupby(["obs_hi", "dis_t"], observed=True):
        old = s[s.index.isin(keep)]
        rest = s[~s.index.isin(keep)]
        n_new = max(0, min(per, len(s)) - len(old))
        pick.append(pd.concat([old, rest.sample(n_new, random_state=int(rng.integers(1e9)))]))
    u = pd.concat(pick)
    s = d[d.key.isin(set(u.index))].merge(u[["obs_hi", "dis_t"]], left_on="key", right_index=True)

    def centre(ids: pd.Series):
        rc = ids.str.extract(r"sq_(-?\d+)_(-?\d+)").astype(float)
        x = meta["x_origin_3857"] + (rc[1] + 0.5) * 3000
        y = meta["y_origin_3857"] + (rc[0] + 0.5) * 3000
        return TO_WGS.transform(x.to_numpy(), y.to_numpy())

    olon, olat = centre(s.grid_from)
    dlon, dlat = centre(s.grid_to)
    m = pd.DataFrame({
        "request_id": s.grid_from + "__" + s.grid_to,
        "split_id": np.arange(len(s)),
        "raw_edge_id": s.grid_from + "__" + s.grid_to,
        "roadseg_id": "", "roadname": "", "road_class": "",
        "length_m": (s.km_AM * 1000).round(1),
        "n_waypoints": 0, "waypoints_latlon": "",
        "origin_text": [f"{a:.6f},{b:.6f}" for a, b in zip(olat, olon)],
        "destination_text": [f"{a:.6f},{b:.6f}" for a, b in zip(dlat, dlon)],
        "gps_direction": np.nan, "has_direct_speed": s.obs_hi.to_numpy(),
        "matched_centerline": False, "skel_dir": "", "cline_id": "", "dir": "",
        # benchmark columns kept for the comparison step (ignored by the query script)
        "grid_from": s.grid_from, "grid_to": s.grid_to, "stratum_obs_hi": s.obs_hi, "stratum_disagree": s.dis_t.astype(str),
        "tm_AM_osm1b": s.tm_AM, "tm_FF_osm1b": s.tm_FF, "tm_PM_osm1b": s.tm_PM, "obs_share_AM": s.obs_share_AM,
        "tau_obs_gate5": s.tau_obs_min, "tau_ff_gate5": s.tau_ff_min,
        "t_am_centerline_v11": s.t_min, "t_ff_centerline_v11": s.t_ff_min,
    })
    (out / "data").mkdir(parents=True)
    m.to_csv(out / "data/sample_manifest.csv", index=False)
    summ = {"directed_rows": int(len(m)), "undirected_pairs": int(len(u)), "kept_from": args.keep_from, "n_kept_undirected": len(keep),
            "by_stratum": u.groupby(["obs_hi", "dis_t"], observed=True).size().rename("n").reset_index().astype(str).to_dict(orient="records"),
            "universe_undirected_pairs": int(d.key.nunique())}
    (out / "data/sample_summary.json").write_text(json.dumps(summ, indent=2, ensure_ascii=False))
    print(json.dumps(summ, ensure_ascii=False))


if __name__ == "__main__":
    main()
