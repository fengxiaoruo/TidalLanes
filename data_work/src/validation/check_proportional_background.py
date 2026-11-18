"""
Test of assumption A: total car traffic is proportional to commuter traffic edge by edge
=========================================================================================
The congestion block only uses Xi / Xi_obs, so commuter-only flows are fine if, on every
edge, all car traffic is a constant multiple of commuter traffic. This script routes
several OD matrices through the model's own route kernel at baseline travel times and
compares the edge loads.

Route kernel (same objects as equilibrium_solve): A_kl = 1 / (kappa * tau_kl),
B = (I - A)^-1, edge flow of an OD matrix L:  Xi_kl = A_kl * (B' (L / B) B')_kl.
Same-cell OD is dropped (it does not load grid edges).

OD matrices (cars, departing 06-10, 19 weekdays, cells >= 5 SIM):
  model     model input commuters_road (HW, primary home -> primary work cells, 0712)
  hw_ep     HW trips by actual trip endpoints, all users (0751)
  all_ep    all purposes by actual trip endpoints, all users (0751-0759)
  non_hw    all_ep - hw_ep
Inputs read only. Output: data_work/outputs/<output-name>/ (refuses to overwrite).
"""
from __future__ import annotations

import argparse
import glob
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

DATA_WORK = Path(__file__).resolve().parents[2]
if str(DATA_WORK) not in sys.path:
    sys.path.insert(0, str(DATA_WORK))

from src.model.full_estimation import build_primitives as bp  # noqa: E402
from src.model.full_estimation import equilibrium_solve as es  # noqa: E402

QR = DATA_WORK.parent / "Platform/runs/202505_v01/query-results"


def endpoint_od(band: str, purposes: list[str], idx: dict[str, int], n: int) -> tuple[np.ndarray, float]:
    tag = band.replace("_", "")
    files = sorted(glob.glob(str(QR / f"TL25M05_V01_07[56]?_EP_{tag}_*.csv")))
    d = pd.concat([pd.read_csv(f, dtype={"grid_o": str, "grid_d": str}) for f in files])
    d = d[(d.band == band) & d.purpose.isin(purposes)]
    tot = float(d.n_trips.sum())
    d = d[d.grid_o.isin(idx) & d.grid_d.isin(idx)]
    L = np.zeros((n, n))
    np.add.at(L, (d.grid_o.map(idx).to_numpy(), d.grid_d.map(idx).to_numpy()), d.n_trips.to_numpy() / 19.0)
    return L, float(d.n_trips.sum()) / tot


def route(L: np.ndarray, a_e: np.ndarray, B: np.ndarray, ei: np.ndarray, ej: np.ndarray) -> np.ndarray:
    L = L.copy()
    np.fill_diagonal(L, 0.0)
    M = np.divide(L, B, out=np.zeros_like(L), where=B > 0)
    G = B.T @ M @ B.T
    return a_e * G[ei, ej]   # per edge, so parallel edges between one node pair each keep their own share


def q(x: pd.Series) -> dict:
    return {k: round(float(v), 3) for k, v in x.quantile([0.1, 0.25, 0.5, 0.75, 0.9]).items()}


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--version-id", default="model_input_osm1b_L300_gate5_od2025_v2_dep0610tt0610")
    ap.add_argument("--tidal-dir", default="tidal_lane_diagnostics_osm1b_ce_floor_b025_fl001_ks_od2025_dep0610tt0610_v2")
    ap.add_argument("--output-name", default="proportional_background_check_osm1b_dep0610_v1")
    args = ap.parse_args(argv)
    out = DATA_WORK / "outputs" / args.output_name
    if out.exists():
        raise SystemExit(f"{out} exists; choose a new --output-name")

    prim = bp.load_primitives(DATA_WORK / "outputs" / args.version_id, period="AM")
    kappa = json.loads((DATA_WORK / "outputs" / args.tidal_dir / "kappa.json").read_text())["kappa"]
    n = prim.n_nodes
    A = es._build_A_matrix(prim.edge_i, prim.edge_j, prim.tau_obs_min, n, kappa=kappa)
    B = np.asarray(es._compute_tau_inv_theta(A))
    a_e = 1.0 / np.clip(kappa * prim.tau_obs_min, es.EPS, None)
    ei, ej = prim.edge_i, prim.edge_j
    idx = {g: i for i, g in enumerate(prim.node_grid_ids)}

    ods = {"model": prim.L_ij_obs.astype(float)}
    ods["hw_ep"], cov_hw = endpoint_od("dep_0610", ["HW"], idx, n)
    ods["all_ep"], cov_all = endpoint_od("dep_0610", ["HW", "WH", "home_other", "work_other", "other"], idx, n)
    ods["non_hw"] = np.clip(ods["all_ep"] - ods["hw_ep"], 0.0, None)
    X = pd.DataFrame({k: route(v, a_e, B, ei, ej) for k, v in ods.items()})
    X["node_i"], X["node_j"] = ei, ej
    X["km"] = prim.ell_kl_km

    res = {"version_id": args.version_id, "kappa": kappa,
           "od_totals_per_day_offdiag": {k: round(float(v.sum() - np.trace(v))) for k, v in ods.items()},
           "endpoint_trips_with_both_ends_in_model_cells": {"hw_ep": round(cov_hw, 3), "all_ep": round(cov_all, 3)}}
    pos = (X.model > 0) & (X.all_ep > 0)
    lg = np.log(X[pos][["model", "hw_ep", "all_ep", "non_hw"]].clip(lower=1e-9))
    res["corr_log_edge_flow"] = {f"{a}~{b}": round(float(lg[a].corr(lg[b])), 3)
                                 for a, b in [("model", "hw_ep"), ("model", "all_ep"), ("model", "non_hw"), ("hw_ep", "all_ep")]}
    # relative load: edge share of all traffic over edge share of model commuters (1 = proportional)
    r = (X.all_ep / X.all_ep.sum()) / (X.model / X.model.sum())
    w = X.model / X.model.sum()
    res["relative_load_all_over_model_quantiles"] = q(r[pos])
    res["relative_load_flow_weighted_share_within_0.8_1.25"] = round(float(w[pos & r.between(0.8, 1.25)].sum()), 3)
    res["relative_load_flow_weighted_share_within_0.67_1.5"] = round(float(w[pos & r.between(2 / 3, 1.5)].sum()), 3)
    res["commuter_share_hw_ep_over_all_ep_quantiles"] = q((X.hw_ep / X.all_ep)[pos])

    # directional asymmetry: log(X_kl / X_lk) on edges with both directions
    rev = X.set_index(["node_i", "node_j"])
    key = list(zip(X.node_j, X.node_i))
    both = pd.Series([k in rev.index for k in key], index=X.index) & pos
    asym = {}
    for c in ["model", "hw_ep", "all_ep", "non_hw"]:
        back = rev.loc[[key[i] for i in X.index[both]], c].to_numpy()
        asym[c] = np.log(np.clip(X.loc[both, c].to_numpy(), 1e-9, None) / np.clip(back, 1e-9, None))
    am = asym["model"]
    res["directional_asymmetry"] = {
        "n_directed_edges": int(both.sum()),
        "mean_abs_log_ratio": {c: round(float(np.mean(np.abs(v))), 3) for c, v in asym.items()},
        "slope_all_on_model": round(float(np.polyfit(am, asym["all_ep"], 1)[0]), 3),
        "slope_non_hw_on_model": round(float(np.polyfit(am, asym["non_hw"], 1)[0]), 3),
        "corr_model_all": round(float(np.corrcoef(am, asym["all_ep"])[0, 1]), 3),
    }

    # treated corridors of the tidal top100 run
    e = pd.read_csv(DATA_WORK / "outputs" / args.tidal_dir / "tidal_treated_edge_detail.csv")
    t = X.merge(e[["node_i", "node_j", "role"]].drop_duplicates(), on=["node_i", "node_j"])
    tr = {}
    for role in ["slow", "fast"]:
        s = t[t.role == role]
        tr[role] = {"n": int(len(s)), "model_share_of_all_model_flow": round(float(s.model.sum() / X.model.sum()), 4),
                    "all_share_of_all_flow": round(float(s.all_ep.sum() / X.all_ep.sum()), 4),
                    "hw_over_all": round(float(s.hw_ep.sum() / s.all_ep.sum()), 3)}
    sl, fa = t[t.role == "slow"], t[t.role == "fast"]
    tr["fast_over_slow_flow_ratio"] = {c: round(float(fa[c].sum() / sl[c].sum()), 3) for c in ["model", "hw_ep", "all_ep", "non_hw"]}
    tr["network_hw_over_all"] = round(float(X.hw_ep.sum() / X.all_ep.sum()), 3)
    res["tidal_top100_corridors"] = tr

    out.mkdir(parents=True)
    X.to_parquet(out / "edge_flows.parquet", index=False)
    (out / "summary.json").write_text(json.dumps(res, indent=2, ensure_ascii=False))
    print(json.dumps(res, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
