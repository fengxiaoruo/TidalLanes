"""Mechanism diagnostics: kappa, propagation, routing, inversion."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy import linalg as sla
from scipy.sparse import csr_matrix
from scipy.sparse.csgraph import dijkstra

ROOT = Path(__file__).resolve().parents[4]
DATA_WORK = ROOT / "data_work"
if str(DATA_WORK) not in sys.path:
    sys.path.insert(0, str(DATA_WORK))

from src.model.directional_cost import fixed_tau_model as ft  # noqa: E402
from src.model.full_estimation import build_primitives as bp  # noqa: E402
from src.model.full_estimation import invert_fundamentals as inv  # noqa: E402
from src.model.full_estimation import parameters as pm  # noqa: E402


EPS = 1e-12


def _load_context(version_id: str, output_name: str | None):
    repo = ROOT
    version_root = repo / "data_work" / "outputs" / version_id
    out_root = repo / "data_work" / "outputs" / (output_name or f"mechanics_{version_id}")
    prim = bp.load_primitives(version_root, grid_type="square", period="AM")
    params = pm.Parameters()
    tau_sym, _ = ft.build_symmetric_cost(prim, method="geometric")
    kappa = max(
        ft.pick_kappa(prim.edge_i, prim.tau_obs_min, prim.n_nodes, 0.9),
        ft.pick_kappa(prim.edge_i, tau_sym, prim.n_nodes, 0.9),
    )
    return prim, params, tau_sym, kappa, out_root


def _edge_row_sums(edge_i: np.ndarray, tau: np.ndarray, n_nodes: int,
                   kappa: float = 1.0) -> np.ndarray:
    raw = np.zeros(n_nodes)
    np.add.at(raw, edge_i, 1.0 / np.clip(kappa * tau, EPS, None))
    return raw


def plot_kappa(prim, kappa: float, fig_dir: Path) -> None:
    raw = _edge_row_sums(prim.edge_i, prim.tau_obs_min, prim.n_nodes, kappa=1.0)
    scaled = _edge_row_sums(prim.edge_i, prim.tau_obs_min, prim.n_nodes, kappa=kappa)
    target = 0.9

    fig, axes = plt.subplots(1, 2, figsize=(11.5, 4.4))
    axes[0].hist(raw, bins=50, color="#64748b", alpha=0.85, edgecolor="white")
    axes[0].axvline(target, color="#dc2626", linewidth=1.5, label=f"target rowsum = {target}")
    axes[0].axvline(float(raw.max()), color="#1d4ed8", linewidth=1.2, linestyle="--",
                    label=f"max raw = {raw.max():.3f}")
    axes[0].set_xlabel("Row sum of A_kl = 1/tau_kl (no kappa)")
    axes[0].set_ylabel("Number of origin nodes")
    axes[0].set_title("Why kappa is needed")
    axes[0].legend(fontsize=8)

    axes[1].hist(scaled, bins=50, color="#0f766e", alpha=0.85, edgecolor="white")
    axes[1].axvline(target, color="#dc2626", linewidth=1.5, label=f"target = {target}")
    axes[1].axvline(float(scaled.max()), color="#1d4ed8", linewidth=1.2, linestyle="--",
                    label=f"max scaled = {scaled.max():.3f}")
    axes[1].set_xlabel(f"Row sum with kappa = {kappa:.4f}")
    axes[1].set_ylabel("Number of origin nodes")
    axes[1].set_title("After uniform kappa rescaling")
    axes[1].legend(fontsize=8)
    fig.suptitle("Spectral-radius safety: kappa = max(1, max_rowsum / 0.9)", y=1.02, fontsize=11)
    fig.tight_layout()
    fig.savefig(fig_dir / "kappa_row_sum_diagnostic.png", dpi=220, bbox_inches="tight")
    plt.close(fig)


def _shortest_path_minutes(prim, kappa: float) -> np.ndarray:
    """All-pairs shortest path on directed grid using edge times kappa*tau."""
    n = prim.n_nodes
    data = (kappa * prim.tau_obs_min).astype(float)
    graph = csr_matrix((data, (prim.edge_i, prim.edge_j)), shape=(n, n))
    dist = dijkstra(graph, directed=True, indices=np.arange(n))
    dist[~np.isfinite(dist)] = np.nan
    return dist


def plot_routing_comparison(prim, params, kappa: float, fig_dir: Path) -> None:
    tau_inv = ft.build_tau_inv_theta(prim, prim.tau_obs_min, kappa=kappa)
    theta = params.theta
    tau_soft_min = np.clip(tau_inv, EPS, None) ** (-1.0 / theta)
    sp_min = _shortest_path_minutes(prim, kappa)

    l_R = prim.L_R_obs / prim.L_R_obs.sum()
    top_origins = np.argsort(-l_R)[:5]
    rows = []
    for i in top_origins:
        for j in np.argsort(-prim.L_ij_obs[i])[:8]:
            if prim.L_ij_obs[i, j] <= 0:
                continue
            soft = float(tau_soft_min[i, j])
            sp = float(sp_min[i, j]) if np.isfinite(sp_min[i, j]) else np.nan
            rows.append({
                "origin_idx": int(i),
                "dest_idx": int(j),
                "grid_origin": prim.node_grid_ids[i],
                "grid_dest": prim.node_grid_ids[j],
                "L_ij_obs": float(prim.L_ij_obs[i, j]),
                "tau_soft_min": soft,
                "tau_sp_min": sp,
                "ratio_soft_over_sp": soft / sp if np.isfinite(sp) and sp > 0 else np.nan,
            })
    df = pd.DataFrame(rows)
    df.to_csv(fig_dir.parent / "diagnostics" / "routing_soft_vs_shortestpath_sample.csv", index=False)

    finite = df[np.isfinite(df["ratio_soft_over_sp"])].copy()
    fig, axes = plt.subplots(1, 2, figsize=(11.5, 4.6))
    axes[0].scatter(finite["tau_sp_min"], finite["tau_soft_min"], s=18 + 80 * (
        finite["L_ij_obs"] / finite["L_ij_obs"].max()
    ), alpha=0.65, c="#0f766e", edgecolors="none")
    lim = max(finite["tau_sp_min"].max(), finite["tau_soft_min"].max()) * 1.05
    axes[0].plot([0, lim], [0, lim], "--", color="#64748b", linewidth=1)
    axes[0].set_xlabel("Shortest-path time (min)")
    axes[0].set_ylabel("Soft Frechet bilateral time (min)")
    axes[0].set_title("Top-origin OD pairs: soft vs SP")

    axes[1].hist(finite["ratio_soft_over_sp"], bins=30, color="#1d4ed8", alpha=0.85)
    med = float(finite["ratio_soft_over_sp"].median())
    axes[1].axvline(med, color="#dc2626", linestyle="--", label=f"median = {med:.3f}")
    axes[1].set_xlabel("Soft time / shortest-path time")
    axes[1].set_ylabel("Count (sampled OD pairs)")
    axes[1].set_title("Soft routing is longer than SP (expected)")
    axes[1].legend()
    fig.tight_layout()
    fig.savefig(fig_dir / "routing_soft_vs_shortestpath.png", dpi=220)
    plt.close(fig)


def plot_one_hop_weights(prim, params, kappa: float, fig_dir: Path) -> None:
    """For one high-flow origin, show first-step link shares toward top destinations."""
    n = prim.n_nodes
    theta = params.theta
    tau_inv = ft.build_tau_inv_theta(prim, prim.tau_obs_min, kappa=kappa)
    A = np.zeros((n, n))
    inv = 1.0 / np.clip(kappa * prim.tau_obs_min, EPS, None)
    np.add.at(A, (prim.edge_i, prim.edge_j), inv)

    origin = int(np.argmax(prim.L_R_obs))
    dests = np.argsort(-prim.L_ij_obs[origin])[:4]
    dests = [int(j) for j in dests if prim.L_ij_obs[origin, j] > 0]

    fig, axes = plt.subplots(2, 2, figsize=(10.5, 8.5))
    axes = axes.ravel()
    for ax, dest in zip(axes, dests):
        z = tau_inv[:, dest] ** (-1.0 / theta)  # proportional to Z_m(dest) in minutes^(-1)? 
        # One-step weight from origin to neighbor m toward dest j:
        # w_{m|i,j} propto A_{im} * tau_inv[m,j]
        neigh = np.where(A[origin] > 0)[0]
        weights = A[origin, neigh] * tau_inv[neigh, dest]
        weights = weights / weights.sum()
        labels = [str(prim.node_grid_ids[m]) for m in neigh]
        order = np.argsort(-weights)[:12]
        ax.barh(np.arange(len(order)), weights[order][::-1], color="#3c6e71")
        ax.set_yticks(np.arange(len(order)))
        ax.set_yticklabels([labels[k] for k in order][::-1], fontsize=7)
        ax.set_xlabel("First-step route share")
        ax.set_title(f"{prim.node_grid_ids[origin]} -> {prim.node_grid_ids[dest]}\n"
                     f"obs flow={prim.L_ij_obs[origin, dest]:.0f}")
    fig.suptitle("Soft routing: first-hop link shares (not all-or-nothing SP)", y=1.01)
    fig.tight_layout()
    fig.savefig(fig_dir / "routing_first_hop_shares.png", dpi=220, bbox_inches="tight")
    plt.close(fig)


def plot_workplace_choice(prim, params, kappa: float, fig_dir: Path) -> None:
    fund = inv.invert_fundamentals(prim, params, kappa=kappa, max_iter=800, tol=1.5e-4)
    eq = ft.solve_static_equilibrium(
        prim, prim.tau_obs_min, fund.ubar_theta, fund.abar_theta, fund.chi, params,
        kappa=kappa, label="directed",
    )
    od = ft.predicted_od_shares(eq, fund.abar_theta, params)
    l_R = prim.L_R_obs / prim.L_R_obs.sum()

    fig, axes = plt.subplots(1, 3, figsize=(12.5, 4.2))
    for ax, origin in zip(axes, np.argsort(-l_R)[:3]):
        origin = int(origin)
        obs_j = np.argsort(-prim.L_ij_obs[origin])[:8]
        obs_j = [int(j) for j in obs_j if prim.L_ij_obs[origin, j] > 0]
        obs_share = prim.L_ij_obs[origin, obs_j] / prim.L_ij_obs[origin].sum()
        model_share = od[origin, obs_j] / np.clip(od[origin].sum(), EPS, None)
        x = np.arange(len(obs_j))
        w = 0.35
        ax.bar(x - w / 2, obs_share, width=w, label="obs OD share", color="#64748b")
        ax.bar(x + w / 2, model_share, width=w, label="model gravity share", color="#0f766e")
        ax.set_xticks(x)
        ax.set_xticklabels([str(prim.node_grid_ids[j]) for j in obs_j], rotation=45, ha="right", fontsize=7)
        ax.set_ylabel("Share among top workplaces")
        ax.set_title(f"Origin {prim.node_grid_ids[origin]}")
    axes[0].legend(fontsize=8)
    fig.suptitle("Workplace choice: observed OD vs model (same residence margin)", y=1.02)
    fig.tight_layout()
    fig.savefig(fig_dir / "workplace_choice_obs_vs_model.png", dpi=220, bbox_inches="tight")
    plt.close(fig)


def plot_inversion_convergence(prim, params, kappa: float, fig_dir: Path) -> None:
    """Re-run inversion with per-iteration logging."""
    theta = params.theta
    alpha = params.alpha
    beta = params.beta
    N = prim.n_nodes
    L_R = np.maximum(prim.L_R_obs, 1e-6)
    L_F = np.maximum(prim.L_F_obs, 1e-6)
    l_R = L_R / L_R.sum()
    l_F = L_F / L_F.sum()
    valid = (prim.L_R_obs > 0) & (prim.L_F_obs > 0)
    tau_edge = prim.tau_obs_min
    inv_w = 1.0 / np.clip(kappa * tau_edge, EPS, None)
    A = np.zeros((N, N))
    np.add.at(A, (prim.edge_i, prim.edge_j), inv_w)
    tau_inv_theta = sla.solve(np.eye(N) - A, np.eye(N), check_finite=False)
    tau_inv_theta = np.clip(tau_inv_theta, EPS, None)

    v = np.ones(N)
    w = np.ones(N)
    pR, pF = 1.0 - beta * theta, 1.0 - alpha * theta
    qR, qF = beta * theta, alpha * theta
    deltas = []
    chi_std = []
    for it in range(1, 601):
        a = tau_inv_theta @ (w * (l_F ** qF))
        v_new = (l_R ** pR) / np.clip(a, EPS, None)
        g = float(np.exp(np.mean(np.log(np.clip(v_new[valid], EPS, None)))))
        v_new = v_new / g
        b = tau_inv_theta.T @ (v_new * (l_R ** qR))
        w_new = (l_F ** pF) / np.clip(b, EPS, None)
        g = float(np.exp(np.mean(np.log(np.clip(w_new[valid], EPS, None)))))
        w_new = w_new / g
        a_chk = tau_inv_theta @ (w_new * (l_F ** qF))
        b_chk = tau_inv_theta.T @ (v_new * (l_R ** qR))
        chi_R = (l_R ** pR) / np.clip(v_new * a_chk, EPS, None)
        chi_F = (l_F ** pF) / np.clip(w_new * b_chk, EPS, None)
        delta = float(max(np.std(np.log(chi_R[valid])), np.std(np.log(chi_F[valid]))))
        deltas.append(delta)
        chi_std.append(delta)
        v, w = v_new, w_new
        if delta < 1.5e-4:
            break

    fig, ax = plt.subplots(figsize=(7.5, 4.6))
    ax.semilogy(range(1, len(deltas) + 1), deltas, color="#1d4ed8", linewidth=1.5)
    ax.axhline(1.5e-4, color="#dc2626", linestyle="--", label="tol = 1.5e-4")
    ax.set_xlabel("Alternating iteration")
    ax.set_ylabel("std(log chi) across valid nodes")
    ax.set_title("Fundamentals inversion: fixed-point convergence (not gravity FE)")
    ax.legend()
    fig.tight_layout()
    fig.savefig(fig_dir / "inversion_convergence.png", dpi=220)
    plt.close(fig)

    meta = {"n_iter_logged": len(deltas), "final_delta": deltas[-1], "init_guess": "v=w=1"}
    (fig_dir.parent / "diagnostics" / "inversion_convergence_meta.json").write_text(
        json.dumps(meta, indent=2)
    )


def main(argv=None) -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--version-id", default="model_input_edgeproj_v2_lcc_gate5_v1")
    p.add_argument("--output-name", default="directional_cost_mechanics_gate5_v1")
    args = p.parse_args(argv)

    prim, params, _, kappa, out_root = _load_context(args.version_id, args.output_name)
    fig_dir = out_root / "figures"
    diag_dir = out_root / "diagnostics"
    fig_dir.mkdir(parents=True, exist_ok=True)
    diag_dir.mkdir(parents=True, exist_ok=True)

    plt.style.use("seaborn-v0_8-whitegrid")
    plot_kappa(prim, kappa, fig_dir)
    plot_routing_comparison(prim, params, kappa, fig_dir)
    plot_one_hop_weights(prim, params, kappa, fig_dir)
    plot_workplace_choice(prim, params, kappa, fig_dir)
    plot_inversion_convergence(prim, params, kappa, fig_dir)

    summary = {
        "version_id": args.version_id,
        "kappa": kappa,
        "target_rowsum": 0.9,
        "n_nodes": prim.n_nodes,
        "n_edges": prim.n_edges,
        "notes": {
            "kappa": "Uniform rescaling so row sums of 1/(kappa*tau) stay below target_rowsum.",
            "inversion": "Alternating fixed-point on structural eqs; init v=w=1; no gravity FE.",
            "routing": "Frechet soft aggregation via (I-A)^(-1); not link-level UE.",
        },
    }
    (diag_dir / "mechanics_summary.json").write_text(json.dumps(summary, indent=2))
    print(f"Wrote figures to {fig_dir}")


if __name__ == "__main__":
    main()
