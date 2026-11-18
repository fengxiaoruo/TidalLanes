"""Scan the link time elasticity gamma = theta*delta0 against observed OD (2026-11-11).

Also scans eta (nested Frechet: route dispersion rho_route separate from location theta,
tau_ij^(-theta) = R_ij^eta, eta = theta/rho_route; mainline eta = 1).

Mainline (AA 2022 preferred): t_kl = m_kl^(1/theta), so A_kl = 1/(kappa*m_kl), gamma = 1.
Generalised: t_kl = m_kl^delta0, A_kl = (kappa*m_kl)^(-gamma), gamma = theta*delta0,
tau_ij^(-theta) = [(I-A)^(-1)]_ij unchanged.

For each gamma, kappa follows the spectral rule (rho(A) <= target at min(tau_ff, tau_obs));
rho scales as kappa^(-gamma). The baseline inversion matches residence and workplace
marginals, so the model OD at observed link minutes is the biproportional fit of
R = (I-A)^(-1) to the observed marginals. Moments compared with observed road OD:
    PPML semi-elasticity on shortest-path minutes (origin and destination FE, off-diagonal)
    same-cell share, mean shortest-path minutes, mean grid distance (cells)

Inputs read only. Output: data_work/outputs/<output-name>/ (refuses to overwrite)
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import linalg as sla
from scipy.optimize import brentq
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import dijkstra

DATA_WORK = Path(__file__).resolve().parents[3]
if str(DATA_WORK) not in sys.path:
    sys.path.insert(0, str(DATA_WORK))

from src.model.full_estimation import build_primitives as bp  # noqa: E402

EPS = 1e-300


def build_A(prim, m, kappa, gamma):
    A = np.zeros((prim.n_nodes,) * 2)
    np.add.at(A, (prim.edge_i, prim.edge_j), (kappa * m) ** (-gamma))
    return A


def rho(A):
    return float(np.max(np.abs(np.linalg.eigvals(A))))


def ipf(K, r, c, n=500):
    a, b = np.ones(len(r)), np.ones(len(c))
    for _ in range(n):
        a = r / np.maximum(K @ b, EPS)
        b = c / np.maximum(K.T @ a, EPS)
    return a[:, None] * K * b[None, :]


def ppml_semi(Y, X, mask):
    Y = np.where(mask, Y, 0.0)
    Xc = X - X[mask].mean()
    r, c = Y.sum(1), Y.sum(0)

    def score(nu):
        K = np.where(mask, np.exp(-nu * Xc), 0.0)
        mu = ipf(K, r, c, 300)
        return float(((Y - mu) * X)[mask].sum())
    return brentq(score, 1e-4, 1.0)


def moments(P, SP, cells, mask):
    P = P / P.sum()
    off = mask
    return {
        "semi_elasticity_per_min": ppml_semi(P, SP, off),
        "same_cell_share": float(np.trace(P)),
        "mean_sp_min": float((P * np.where(np.isfinite(SP), SP, 0)).sum()),
        "mean_sp_min_offdiag": float((P * np.where(off, SP, 0)).sum() / P[off].sum()),
        "mean_cells": float((P * cells).sum()),
    }


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--version-id", default="model_input_osm1b_L300_gate5_od2025_v2_alldaytt0709")
    ap.add_argument("--output-name", default="link_time_gamma_scan_osm1b_alldaytt0709_v1")
    ap.add_argument("--gammas", default="1.0,0.8,0.6,0.5,0.4,0.3,0.2,0.1,0.05")
    ap.add_argument("--rho-target", type=float, default=0.95)
    ap.add_argument("--etas", default="1.0",
                    help="location-choice exponent on the route composite, tau^-theta = R^eta (eta = theta/rho_route); 1 = mainline")
    a = ap.parse_args(argv)
    out = DATA_WORK / "outputs" / a.output_name
    if out.exists():
        raise SystemExit(f"{out} exists; choose a new --output-name")
    prim = bp.load_primitives(DATA_WORK / "outputs" / a.version_id, period="AM")
    N = prim.n_nodes
    m_obs = np.asarray(prim.tau_obs_min, float)
    floor = np.minimum(np.asarray(prim.tau_ff_min, float), m_obs)
    SP = dijkstra(coo_matrix((m_obs, (prim.edge_i, prim.edge_j)), shape=(N, N)).tocsr(), directed=True)
    nodes = pd.read_parquet(DATA_WORK / "outputs" / a.version_id / "data/qsm_input_nodes_square.parquet").sort_values("node_i")
    g = nodes.grid_id.str.extract(r"sq_(\d+)_(\d+)").astype(int).to_numpy()
    cells = np.hypot(g[:, 0][:, None] - g[:, 0][None], g[:, 1][:, None] - g[:, 1][None])
    mask = ~np.eye(N, dtype=bool) & np.isfinite(SP)
    L = np.asarray(prim.L_ij_obs, float)
    r, c = L.sum(1), L.sum(0)

    rows = [{"spec": "observed", **moments(L, SP, cells, mask)}]
    print(rows[-1], flush=True)
    for gamma, eta in [(float(g_), float(e_)) for g_ in a.gammas.split(",") for e_ in a.etas.split(",")]:
        rho1 = rho(build_A(prim, floor, 1.0, gamma))
        kappa = max(1.0, (rho1 / a.rho_target) ** (1.0 / gamma))
        A = build_A(prim, m_obs, kappa, gamma)
        R = sla.solve(np.eye(N) - A, np.eye(N), check_finite=False)
        neg = float(np.mean(R < 0))
        R = np.clip(R, 0.0, None)
        P = ipf(R ** eta, r, c)
        row = {"spec": f"gamma_{gamma:g}_eta_{eta:g}", "gamma": gamma, "eta": eta, "delta0_x_theta": gamma, "kappa": kappa,
               "rho_floor": rho(build_A(prim, floor, kappa, gamma)), "rho_obs": rho(A),
               "R_negative_share": neg, **moments(P, SP, cells, mask)}
        rows.append(row)
        print({k: (round(v, 4) if isinstance(v, float) else v) for k, v in row.items()}, flush=True)
    out.mkdir(parents=True)
    df = pd.DataFrame(rows)
    df.to_csv(out / "gamma_scan.csv", index=False)
    (out / "meta.json").write_text(json.dumps({"version_id": a.version_id, "rho_target": a.rho_target,
        "note": "model OD = biproportional fit of (I-A)^-1 at observed minutes to observed marginals"}, indent=2))


if __name__ == "__main__":
    main()
