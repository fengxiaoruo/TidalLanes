"""Visual explainer for fundamentals inversion vs gravity FE."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy import linalg as sla

ROOT = Path(__file__).resolve().parents[4]
DATA_WORK = ROOT / "data_work"
if str(DATA_WORK) not in sys.path:
    sys.path.insert(0, str(DATA_WORK))

from src.model.directional_cost import fixed_tau_model as ft  # noqa: E402
from src.model.full_estimation import build_primitives as bp  # noqa: E402
from src.model.full_estimation import invert_fundamentals as inv  # noqa: E402
from src.model.full_estimation import parameters as pm  # noqa: E402


EPS = 1e-12


def _toy_inversion_steps() -> dict:
    """3-node hand example mirroring invert_fundamentals logic."""
    theta, alpha, beta = 6.83, -0.12, -0.10
    pR, pF = 1.0 - beta * theta, 1.0 - alpha * theta
    qR, qF = beta * theta, alpha * theta

    # Bilateral accessibility index T_ij ~ tau_ij^{-theta} (toy numbers).
    T = np.array([
        [1.00, 0.50, 0.20],
        [0.30, 1.00, 0.40],
        [0.25, 0.35, 1.00],
    ], dtype=float)

    l_R = np.array([0.50, 0.30, 0.20])
    l_F = np.array([0.25, 0.45, 0.30])
    valid = np.ones(3, dtype=bool)

    v = np.ones(3)
    w = np.ones(3)
    history = []
    for it in range(1, 8):
        a = T @ (w * (l_F ** qF))
        v_new = (l_R ** pR) / np.clip(a, EPS, None)
        g = float(np.exp(np.mean(np.log(v_new))))
        v_new = v_new / g
        b = T.T @ (v_new * (l_R ** qR))
        w_new = (l_F ** pF) / np.clip(b, EPS, None)
        g = float(np.exp(np.mean(np.log(w_new))))
        w_new = w_new / g
        a_chk = T @ (w_new * (l_F ** qF))
        b_chk = T.T @ (v_new * (l_R ** qR))
        chi_R = (l_R ** pR) / np.clip(v_new * a_chk, EPS, None)
        chi_F = (l_F ** pF) / np.clip(w_new * b_chk, EPS, None)
        delta = float(max(np.std(np.log(chi_R)), np.std(np.log(chi_F))))
        history.append({
            "iter": it,
            "v": v_new.tolist(),
            "w": w_new.tolist(),
            "chi_R": chi_R.tolist(),
            "chi_F": chi_F.tolist(),
            "delta": delta,
        })
        v, w = v_new, w_new
        if delta < 1e-6:
            break

    chi = float(np.exp(0.5 * (np.mean(np.log(chi_R)) + np.mean(np.log(chi_F)))))
    a = T @ (w * (l_F ** qF))
    b = T.T @ (v * (l_R ** qR))
    l_R_pred = (chi * v * a) ** (1.0 / pR)
    l_F_pred = (chi * w * b) ** (1.0 / pF)
    l_R_pred /= l_R_pred.sum()
    l_F_pred /= l_F_pred.sum()

    return {
        "theta": theta, "alpha": alpha, "beta": beta,
        "l_R": l_R.tolist(), "l_F": l_F.tolist(),
        "final_v": v.tolist(), "final_w": w.tolist(),
        "chi": chi,
        "l_R_pred": l_R_pred.tolist(),
        "l_F_pred": l_F_pred.tolist(),
        "history": history,
        "note": "Toy 3x3 T matrix; shows chi flattening as v,w adjust.",
    }


def plot_toy_chi(history: list[dict], fig_dir: Path) -> None:
    iters = [h["iter"] for h in history]
    chi_r0 = [h["chi_R"][0] for h in history]
    chi_r1 = [h["chi_R"][1] for h in history]
    chi_f1 = [h["chi_F"][1] for h in history]
    deltas = [h["delta"] for h in history]

    fig, axes = plt.subplots(1, 2, figsize=(11.0, 4.2))
    axes[0].plot(iters, chi_r0, "o-", label=r"$\chi^R_0$ (residence eq., node 0)")
    axes[0].plot(iters, chi_r1, "s-", label=r"$\chi^R_1$")
    axes[0].plot(iters, chi_f1, "^-", label=r"$\chi^F_1$ (workplace eq., node 1)")
    axes[0].set_xlabel("Iteration")
    axes[0].set_ylabel("Implied scalar from each node")
    axes[0].set_title("Toy 3-node: per-node $\\chi$ must agree at solution")
    axes[0].legend(fontsize=8)

    axes[1].semilogy(iters, deltas, "o-", color="#1d4ed8")
    axes[1].set_xlabel("Iteration")
    axes[1].set_ylabel("std(log chi) across nodes")
    axes[1].set_title("Convergence: dispersion of implied $\\chi$")
    fig.tight_layout()
    fig.savefig(fig_dir / "inversion_toy_chi_convergence.png", dpi=220)
    plt.close(fig)


def plot_toy_shares(toy: dict, fig_dir: Path) -> None:
    nodes = ["0", "1", "2"]
    x = np.arange(3)
    w = 0.35
    fig, axes = plt.subplots(1, 2, figsize=(9.5, 4.0))
    axes[0].bar(x - w / 2, toy["l_R"], width=w, label="observed $l^R$", color="#64748b")
    axes[0].bar(x + w / 2, toy["l_R_pred"], width=w, label="implied after inversion", color="#0f766e")
    axes[0].set_xticks(x)
    axes[0].set_xticklabels(nodes)
    axes[0].set_title("Residence shares")
    axes[0].legend(fontsize=8)

    axes[1].bar(x - w / 2, toy["l_F"], width=w, label="observed $l^F$", color="#64748b")
    axes[1].bar(x + w / 2, toy["l_F_pred"], width=w, label="implied after inversion", color="#0f766e")
    axes[1].set_xticks(x)
    axes[1].set_xticklabels(nodes)
    axes[1].set_title("Workplace shares")
    axes[1].legend(fontsize=8)
    fig.suptitle("Toy example: inverted fundamentals reproduce observed marginals", y=1.02)
    fig.tight_layout()
    fig.savefig(fig_dir / "inversion_toy_marginal_fit.png", dpi=220, bbox_inches="tight")
    plt.close(fig)


def plot_gate5_convergence(prim, params, kappa: float, fig_dir: Path) -> inv.InversionResult:
    """Re-run inversion with per-iteration logging for gate5."""
    theta = params.theta
    alpha = params.alpha
    beta = params.beta
    N = prim.n_nodes
    l_R = np.maximum(prim.L_R_obs, 1e-6)
    l_F = np.maximum(prim.L_F_obs, 1e-6)
    l_R = l_R / l_R.sum()
    l_F = l_F / l_F.sum()
    valid = (prim.L_R_obs > 0) & (prim.L_F_obs > 0)
    fund_probe = inv.invert_fundamentals(prim, params, kappa=kappa, max_iter=1, tol=1e-12)
    T = fund_probe.tau_obs_inv_theta
    pR, pF = 1.0 - beta * theta, 1.0 - alpha * theta
    qR, qF = beta * theta, alpha * theta
    v = np.ones(N)
    w = np.ones(N)
    deltas = []
    for it in range(1, 601):
        a = T @ (w * (l_F ** qF))
        v_new = (l_R ** pR) / np.clip(a, EPS, None)
        g = float(np.exp(np.mean(np.log(np.clip(v_new[valid], EPS, None)))))
        v_new = v_new / g
        b = T.T @ (v_new * (l_R ** qR))
        w_new = (l_F ** pF) / np.clip(b, EPS, None)
        g = float(np.exp(np.mean(np.log(np.clip(w_new[valid], EPS, None)))))
        w_new = w_new / g
        a_chk = T @ (w_new * (l_F ** qF))
        b_chk = T.T @ (v_new * (l_R ** qR))
        chi_R = (l_R ** pR) / np.clip(v_new * a_chk, EPS, None)
        chi_F = (l_F ** pF) / np.clip(w_new * b_chk, EPS, None)
        log_chiR = np.log(np.clip(chi_R[valid], EPS, None))
        log_chiF = np.log(np.clip(chi_F[valid], EPS, None))
        log_chiR = log_chiR[np.isfinite(log_chiR)]
        log_chiF = log_chiF[np.isfinite(log_chiF)]
        delta = float(max(np.std(log_chiR) if log_chiR.size else np.inf,
                            np.std(log_chiF) if log_chiF.size else np.inf))
        deltas.append(delta)
        v, w = v_new, w_new
        if delta < 1.5e-4:
            break

    fig, ax = plt.subplots(figsize=(7.2, 4.4))
    ax.semilogy(range(1, len(deltas) + 1), deltas, color="#1d4ed8", linewidth=1.5)
    ax.axhline(1.5e-4, color="#dc2626", linestyle="--", label="tol = 1.5e-4")
    ax.set_xlabel("Alternating iteration")
    ax.set_ylabel("std(log chi) on valid nodes")
    ax.set_title("Gate5: fundamentals inversion convergence")
    ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(fig_dir / "inversion_gate5_convergence.png", dpi=220)
    plt.close(fig)

    return inv.invert_fundamentals(prim, params, kappa=kappa, max_iter=800, tol=1.5e-4)


def plot_gate5_fundamentals(prim, fund: inv.InversionResult, fig_dir: Path, diag_dir: Path) -> None:
    valid = (prim.L_R_obs > 0) & (prim.L_F_obs > 0)
    v, w = fund.ubar_theta, fund.abar_theta
    mass = prim.L_R_obs + prim.L_F_obs
    top = np.argsort(-mass[valid])[:40]
    idx_valid = np.where(valid)[0][top]
    df = pd.DataFrame({
        "grid_id": prim.node_grid_ids[idx_valid],
        "L_R_obs": prim.L_R_obs[idx_valid],
        "L_F_obs": prim.L_F_obs[idx_valid],
        "ubar_theta": v[idx_valid],
        "abar_theta": w[idx_valid],
        "log_ubar": np.log(v[idx_valid]),
        "log_abar": np.log(w[idx_valid]),
    })
    df.to_csv(diag_dir / "inversion_top_nodes_fundamentals.csv", index=False)

    fig, axes = plt.subplots(1, 2, figsize=(11.0, 4.6))
    axes[0].scatter(np.log(prim.L_R_obs[idx_valid] + EPS), np.log(v[idx_valid]), s=28, alpha=0.75, c="#0f766e")
    axes[0].set_xlabel(r"$\log L^{obs,R}_i$")
    axes[0].set_ylabel(r"$\log \bar u_i^\theta$ (inverted)")
    axes[0].set_title("Not a simple FE residual: amenity vs observed residents")

    axes[1].scatter(np.log(prim.L_F_obs[idx_valid] + EPS), np.log(w[idx_valid]), s=28, alpha=0.75, c="#1d4ed8")
    axes[1].set_xlabel(r"$\log L^{obs,F}_j$")
    axes[1].set_ylabel(r"$\log \bar A_j^\theta$ (inverted)")
    axes[1].set_title("Productivity vs observed jobs (highly entangled)")
    fig.tight_layout()
    fig.savefig(fig_dir / "inversion_gate5_fundamentals_vs_marginals.png", dpi=220)
    plt.close(fig)


def plot_fe_vs_structural(prim, params, kappa: float, fig_dir: Path) -> None:
    """Contrast gravity FE decomposition with structural inversion targets."""
    theta = params.theta
    fund = inv.invert_fundamentals(prim, params, kappa=kappa, max_iter=800, tol=1.5e-4)
    T = fund.tau_obs_inv_theta
    tau_ij = np.clip(T, EPS, None) ** (-1.0 / theta)

    o, d, pop = [], [], []
    for i in range(prim.n_nodes):
        for j in range(prim.n_nodes):
            if prim.L_ij_obs[i, j] > 0:
                o.append(i)
                d.append(j)
                pop.append(prim.L_ij_obs[i, j])
    o = np.asarray(o, dtype=int)
    d = np.asarray(d, dtype=int)
    pop = np.asarray(pop, dtype=float)
    y = np.log(pop)
    x = -theta * np.log(tau_ij[o, d])

    mu = np.zeros(prim.n_nodes)
    nu = np.zeros(prim.n_nodes)
    for _ in range(100):
        mu_new = np.zeros(prim.n_nodes)
        np.add.at(mu_new, o, y - x - nu[d])
        counts = np.zeros(prim.n_nodes)
        np.add.at(counts, o, 1.0)
        mu = np.divide(mu_new, np.maximum(counts, 1.0))
        nu_new = np.zeros(prim.n_nodes)
        np.add.at(nu_new, d, y - x - mu[o])
        counts = np.zeros(prim.n_nodes)
        np.add.at(counts, d, 1.0)
        nu = np.divide(nu_new, np.maximum(counts, 1.0))

    valid = (prim.L_R_obs > 0) & (prim.L_F_obs > 0)
    idx = np.where(valid)[0]
    if len(idx) > 400:
        rng = np.random.default_rng(0)
        idx = rng.choice(idx, size=400, replace=False)

    fig, axes = plt.subplots(1, 2, figsize=(11.0, 4.6))
    axes[0].scatter(mu[idx], np.log(fund.ubar_theta[idx]), s=12, alpha=0.5, c="#64748b")
    axes[0].set_xlabel(r"Gravity origin FE $\mu_i$ (from log $L_{ij}$)")
    axes[0].set_ylabel(r"Structural $\log \bar u_i^\theta$")
    axes[0].set_title("Origin FE ≠ inverted amenity\n(different estimands)")

    axes[1].scatter(nu[idx], np.log(fund.abar_theta[idx]), s=12, alpha=0.5, c="#64748b")
    axes[1].set_xlabel(r"Gravity destination FE $\nu_j$")
    axes[1].set_ylabel(r"Structural $\log \bar A_j^\theta$")
    axes[1].set_title("Destination FE ≠ inverted productivity")
    fig.tight_layout()
    fig.savefig(fig_dir / "inversion_fe_vs_structural.png", dpi=220)
    plt.close(fig)


def plot_algorithm_schematic(fig_dir: Path) -> None:
    fig, ax = plt.subplots(figsize=(10.5, 5.5))
    ax.axis("off")
    boxes = [
        (0.05, 0.62, "Observed inputs\n$l^{R,obs}, l^{F,obs}$\n$\\tau^{obs}$ → $T=(I-A)^{-1}$"),
        (0.38, 0.62, "Guess start\n$\\bar u_i^\\theta=\\bar A_j^\\theta=1$"),
        (0.71, 0.62, "Update $\\bar u$\nfrom residence eq."),
        (0.71, 0.28, "Update $\\bar A$\nfrom workplace eq."),
        (0.38, 0.28, "Check: each node\nimplies same $\\chi$?"),
        (0.05, 0.28, "Output\n$(\\bar u,\\bar A,\\chi)$\n+ solve equilibrium"),
    ]
    for x, y, text in boxes:
        ax.text(x, y, text, ha="center", va="center", fontsize=9,
                bbox=dict(boxstyle="round,pad=0.45", facecolor="#f8fafc", edgecolor="#64748b"))
    arrows = [
        ((0.22, 0.62), (0.36, 0.62)),
        ((0.54, 0.62), (0.69, 0.62)),
        ((0.82, 0.56), (0.82, 0.36)),
        ((0.69, 0.28), (0.54, 0.28)),
        ((0.36, 0.28), (0.18, 0.28)),
        ((0.38, 0.52), (0.38, 0.36)),
    ]
    for (x0, y0), (x1, y1) in arrows:
        ax.annotate("", xy=(x1, y1), xytext=(x0, y0),
                    arrowprops=dict(arrowstyle="->", color="#334155", lw=1.2))
    ax.text(0.38, 0.44, "no → loop", ha="center", fontsize=8, color="#dc2626")
    ax.text(0.22, 0.44, "yes", ha="center", fontsize=8, color="#0f766e")
    ax.set_title("Fundamentals inversion = structural fixed point (not gravity regression)", pad=12)
    fig.tight_layout()
    fig.savefig(fig_dir / "inversion_algorithm_schematic.png", dpi=220, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    version_id = "model_input_edgeproj_v2_lcc_gate5_v1"
    out_root = ROOT / "data_work" / "outputs" / "directional_cost_inversion_explainer_gate5_v1"
    fig_dir = out_root / "figures"
    diag_dir = out_root / "diagnostics"
    fig_dir.mkdir(parents=True, exist_ok=True)
    diag_dir.mkdir(parents=True, exist_ok=True)

    plt.style.use("seaborn-v0_8-whitegrid")
    plot_algorithm_schematic(fig_dir)

    toy = _toy_inversion_steps()
    (diag_dir / "toy_inversion_example.json").write_text(json.dumps(toy, indent=2))
    plot_toy_chi(toy["history"], fig_dir)
    plot_toy_shares(toy, fig_dir)

    prim = bp.load_primitives(ROOT / "data_work" / "outputs" / version_id, grid_type="square", period="AM")
    params = pm.Parameters()
    tau_sym, _ = ft.build_symmetric_cost(prim, method="geometric")
    kappa = max(
        ft.pick_kappa(prim.edge_i, prim.tau_obs_min, prim.n_nodes),
        ft.pick_kappa(prim.edge_i, tau_sym, prim.n_nodes),
    )
    fund = plot_gate5_convergence(prim, params, kappa, fig_dir)
    plot_gate5_fundamentals(prim, fund, fig_dir, diag_dir)
    plot_fe_vs_structural(prim, params, kappa, fig_dir)

    summary = {
        "version_id": version_id,
        "kappa": kappa,
        "n_iter": fund.n_iter,
        "converged": fund.converged,
        "chi": fund.chi,
        "max_log_change": fund.max_log_change,
        "figure_dir": str(fig_dir),
    }
    (diag_dir / "summary.json").write_text(json.dumps(summary, indent=2))
    print(f"Wrote explainer figures to {fig_dir}")


if __name__ == "__main__":
    main()
