"""
Congestion elasticity lambda with observed RN traffic: identification trials (gate5)
====================================================================================

Target (v4 eq. lambda_regression, iceberg-time form):

    ln(tau_kl / tau_ff_kl) = lambda * ln(Xi_kl / n_kl) + eps_kl

Xi_kl is observed RN traffic (run_observed_traffic_gate5 inputs): AM = HW, PM = WH,
strict, pooled 0519-0523, per day. Main measure length-allocated, robustness
boundary crossings.

Designs
    D1  cross-section AM, lane-bin demeaned (replicates the diagnostic);
    D2  reciprocal-pair difference AM: same road corridor in both directions,
        differences out corridor-level road quality, signals, length;
    D3  within-edge AM - PM: same directed edge, two peaks; differences out all
        time-invariant edge characteristics (tau_ff cancels);
    D4  D3 within reciprocal pairs (double difference).

Each design by OLS and by IV. Instrument: 2025 commuting OD (window export,
strict, mode1; AM = HW, PM = WH) assigned to the network at free-flow times
with the model's route kernel,
    Xi^IV_kl = A_kl * sum_ij L_ij * X_ik * X_lj / X_ij,  X = (I - A)^{-1},
A_kl = 1/(kappa * tau_ff_kl). It shifts traffic through where people live and
work, not through edge-specific congestion shocks. Old model OD (commuters_road)
is a second instrument for comparison.

Output: data_work/outputs/lambda_identification_rn202505_gate5_v1/
"""
from __future__ import annotations

import argparse
import glob
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]   # -> data_work/
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.model.full_estimation import build_primitives as bp        # noqa: E402
from src.model.full_estimation import parameters as pm              # noqa: E402
from src.model.full_estimation import equilibrium_solve as eq       # noqa: E402

N_DAYS = 5
RN_DAYS = [20250519, 20250520, 20250521, 20250522, 20250523]
BLOCKS = (3, 6)
EPS = 1e-12


def assign_od(prim, L: np.ndarray, kappa: float) -> np.ndarray:
    """Edge traffic from an OD matrix under the model route kernel at free-flow times."""
    A = eq._build_A_matrix(prim.edge_i, prim.edge_j, prim.tau_ff_min, prim.n_nodes, kappa=kappa)
    X = eq._compute_tau_inv_theta(A)
    L = L.copy()
    np.fill_diagonal(L, 0.0)
    Q = L / np.clip(X, EPS, None)
    M = X.T @ Q @ X.T
    return A[prim.edge_i, prim.edge_j] * M[prim.edge_i, prim.edge_j]


def od_matrix(prim, df: pd.DataFrame, o: str, d: str, w: str) -> np.ndarray:
    idx = {g: k for k, g in enumerate(prim.node_grid_ids)}
    df = df[df[o].isin(idx) & df[d].isin(idx)]
    L = np.zeros((prim.n_nodes, prim.n_nodes))
    np.add.at(L, (df[o].map(idx).to_numpy(), df[d].map(idx).to_numpy()), df[w].to_numpy(dtype=float))
    return L


def ols(y, x, z=None, controls=None, clusters=None):
    """OLS or just-identified 2SLS with HC1 se, plus cluster-robust se for each grouping in ``clusters``
    (dict name -> group labels). Returns slope on x (and robust first-stage F if z)."""
    n = len(y)
    C = np.ones((n, 1)) if controls is None else np.column_stack([np.ones(n), controls])
    W = np.column_stack([x, C])
    out = {"n": int(n)}
    if z is None:
        Z = W
    else:
        Z = np.column_stack([z, C])
        pi, *_ = np.linalg.lstsq(Z, x, rcond=None)
        u1 = x - Z @ pi
        ZtZi = np.linalg.inv(Z.T @ Z)
        V1 = ZtZi @ (Z.T * u1 ** 2) @ Z @ ZtZi * n / (n - Z.shape[1])
        out["first_stage_coef"] = float(pi[0])
        out["first_stage_F_robust"] = float(pi[0] ** 2 / V1[0, 0])
    PZ = Z @ np.linalg.solve(Z.T @ Z, Z.T @ W)
    b = np.linalg.solve(PZ.T @ W, PZ.T @ y)
    e = y - W @ b
    B = np.linalg.inv(PZ.T @ W)
    V = B @ (PZ.T * e ** 2) @ PZ @ B.T * n / (n - W.shape[1])
    out.update(lambda_hat=float(b[0]), se=float(np.sqrt(V[0, 0])))
    for cname, g in (clusters or {}).items():
        g = pd.factorize(np.asarray(g))[0]
        S = np.zeros((g.max() + 1, W.shape[1]))
        np.add.at(S, g, PZ * e[:, None])
        G = S.shape[0]
        Vc = B @ (S.T @ S) @ B.T * G / (G - 1) * (n - 1) / (n - W.shape[1])
        out[f"se_cluster_{cname}"] = float(np.sqrt(Vc[0, 0]))
        out[f"n_clusters_{cname}"] = int(G)
    return out


def lane_demean(df, cols):
    g = np.round(df.n_kl).astype(int)
    return {c: (df[c] - df[c].groupby(g).transform("mean")).to_numpy() for c in cols}


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--repo-root", default=str(ROOT.parent))
    p.add_argument("--version-id", default="model_input_edgeproj_v2_lcc_gate5_v1")
    p.add_argument("--pm-times", default="data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/data/raw_connected_vs_old_adjacent_adjacent_square_PM.csv")
    p.add_argument("--loads", default="Platform/runs/202505_v01/local_model/model_edge_rn_load_window_strict.csv")
    p.add_argument("--od-window", default="Platform/runs/202505_v01/query-results/TL25M05_V01_040[78]_WIN_*.csv")
    p.add_argument("--od-date-window", default="Platform/runs/202505_v01/query-results/TL25M05_V01_04*_DW_*.csv")
    p.add_argument("--old-od-version", default="model_input_edgeproj_v2_lcc_gate5_v1")
    p.add_argument("--output-name", default="lambda_identification_rn202505_gate5_v2")
    args = p.parse_args(argv)
    repo = Path(args.repo_root).resolve()
    out_root = repo / "data_work" / "outputs" / args.output_name
    if out_root.exists():
        raise SystemExit(f"{out_root} exists; choose a new --output-name")
    out_root.mkdir(parents=True)

    prim = bp.load_primitives(repo / "data_work" / "outputs" / args.version_id,
                              grid_type="square", period="AM", od_pop_col="commuters_road")
    params = pm.Parameters()
    kappa = eq._pick_kappa(prim.edge_i, prim.edge_j, prim.tau_ff_min, prim.n_nodes)

    e = pd.DataFrame({"grid_o": prim.node_grid_ids[prim.edge_i], "grid_d": prim.node_grid_ids[prim.edge_j],
                      "i": prim.edge_i, "j": prim.edge_j, "tau_am": prim.tau_obs_min, "tau_ff": prim.tau_ff_min,
                      "n_kl": eq._resolve_lanes(prim.n_kl)})
    pmt = pd.read_csv(repo / args.pm_times).rename(columns={"home_grid": "grid_o", "work_grid": "grid_d", "tau_obs_min": "tau_pm"})
    e = e.merge(pmt[["grid_o", "grid_d", "tau_pm"]], on=["grid_o", "grid_d"], how="left")
    loads = pd.read_csv(repo / args.loads).rename(columns={"grid_from": "grid_o", "grid_to": "grid_d"})
    e = e.merge(loads[["grid_o", "grid_d", "len_flow_HW", "len_flow_WH", "n_pass_HW", "n_pass_WH"]], on=["grid_o", "grid_d"], how="left")
    assert len(e) == prim.n_edges and e.tau_pm.notna().all()

    dw = pd.concat([pd.read_csv(f, dtype={"grid_o": str, "grid_d": str}) for f in sorted(glob.glob(str(repo / args.od_date_window)))])
    win = pd.concat([pd.read_csv(f, dtype={"grid_o": str, "grid_d": str}) for f in sorted(glob.glob(str(repo / args.od_window)))])
    win = win[(win.variant == "strict") & (win.mode_group == "mode1")]
    dw = dw[(dw.variant == "strict") & (dw.mode_group == "mode1")]
    rn5 = dw.date.isin(RN_DAYS)
    sources = {"iv25_pool19": win, "iv25_rn5days": dw[rn5], "iv25_other14days": dw[~rn5]}
    od = {}
    for k, src in sources.items():
        od[k] = (od_matrix(prim, src[src.purpose == "HW"], "grid_o", "grid_d", "n_trips"),
                 od_matrix(prim, src[src.purpose == "WH"], "grid_o", "grid_d", "n_trips"))
    old = bp.load_primitives(repo / "data_work" / "outputs" / args.old_od_version,
                             grid_type="square", period="AM", od_pop_col="commuters_road")
    assert (old.node_grid_ids == prim.node_grid_ids).all()
    od["iv_oldOD"] = (old.L_ij_obs, old.L_ij_obs.T)
    for k, (am, pm_) in od.items():
        e[f"{k}_am"] = assign_od(prim, am, kappa)
        e[f"{k}_pm"] = assign_od(prim, pm_, kappa)

    res = {"kappa_ff": kappa, "lambda_placeholder": float(params.lambda_),
           "od_trips_in_model_nodes": {k: {"HW": float(a.sum()), "WH": float(b.sum())} for k, (a, b) in od.items()},
           "corr_log_tau_am_pm_over_ff": float(np.corrcoef(np.log(e.tau_am / e.tau_ff), np.log(e.tau_pm / e.tau_ff))[0, 1])}
    rc = e.grid_o.str.extract(r"sq_(\d+)_(\d+)").astype(int)
    for b in BLOCKS:
        e[f"block{b}"] = (rc[0] // b).astype(str) + "_" + (rc[1] // b).astype(str)
    rev = e.reset_index().merge(e.reset_index()[["index", "i", "j"]].rename(columns={"index": "r", "i": "j", "j": "i"}), on=["i", "j"])
    rev = rev[rev["index"] < rev["r"]]

    rows = []
    for name, (am_col, pm_col) in {"length_flow": ("len_flow_HW", "len_flow_WH"), "crossings": ("n_pass_HW", "n_pass_WH")}.items():
        d = e.copy()
        d["y"] = np.log(d.tau_am / d.tau_ff)
        d["x"] = np.log(d[am_col] / N_DAYS / d.n_kl)
        d["x_pm"] = np.log(d[pm_col] / N_DAYS / d.n_kl)
        d["dy_t"] = np.log(d.tau_am / d.tau_pm)
        d["dx_t"] = d.x - d.x_pm
        zcols = []
        for k in od:
            d[f"z_{k}"] = np.log(d[f"{k}_am"] / d.n_kl)
            d[f"dz_t_{k}"] = d[f"z_{k}"] - np.log(d[f"{k}_pm"] / d.n_kl)
            zcols += [f"z_{k}", f"dz_t_{k}"]
        d = d[np.isfinite(d[["y", "x", "x_pm", "dy_t", *zcols]]).all(axis=1)]
        cl = {f"block{b}": d[f"block{b}"].to_numpy() for b in BLOCKS}
        dm = lane_demean(d, ["y", "x", *[f"z_{k}" for k in od]])
        keep = ["y", "x", "dy_t", "dx_t", *zcols]
        pr = rev.merge(d[keep], left_on="index", right_index=True) \
                .merge(d[keep], left_on="r", right_index=True, suffixes=("", "_rev"))
        pcl = {f"block{b}": pr[f"block{b}"].to_numpy() for b in BLOCKS}
        diff = lambda c: (pr[c] - pr[f"{c}_rev"]).to_numpy()
        designs = {
            "D1_cross_section_AM": (dm["y"], dm["x"], lambda k: dm[f"z_{k}"], cl),
            "D2_pair_diff_AM": (diff("y"), diff("x"), lambda k: diff(f"z_{k}"), pcl),
            "D3_within_edge_AM_PM": (d.dy_t.to_numpy(), d.dx_t.to_numpy(), lambda k: d[f"dz_t_{k}"].to_numpy(), cl),
            "D4_pair_x_period": (diff("dy_t"), diff("dx_t"), lambda k: diff(f"dz_t_{k}"), pcl),
        }
        for dname, (y, x, zf, c) in designs.items():
            rows.append({"measure": name, "design": dname, "estimator": "OLS", **ols(y, x, clusters=c)})
            for k in od:
                rows.append({"measure": name, "design": dname, "estimator": f"IV_{k}", **ols(y, x, zf(k), clusters=c)})
    e.to_csv(out_root / "edge_identification_data.csv", index=False)
    tab = pd.DataFrame(rows)
    tab.to_csv(out_root / "lambda_identification_table.csv", index=False)
    res["table"] = rows
    (out_root / "lambda_identification_summary.json").write_text(json.dumps(res, indent=2, default=float))
    show = ["measure", "design", "estimator", "n", "lambda_hat", "se", *[f"se_cluster_block{b}" for b in BLOCKS], "first_stage_F_robust"]
    print(tab[show].to_string(index=False, float_format=lambda v: f"{v:.4f}"))
    print(json.dumps({k: v for k, v in res.items() if k != "table"}, indent=1, default=float))

if __name__ == "__main__":
    main()
