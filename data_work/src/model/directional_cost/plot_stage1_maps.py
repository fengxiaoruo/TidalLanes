"""Grid maps and policy-CF charts for Stage 1 gate5."""
from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def _grid_plot(ax, df, col, title, cmap="RdBu_r", q=0.02):
    v = df[col].to_numpy(float)
    vmin, vmax = np.quantile(v, q), np.quantile(v, 1 - q)
    if vmin == vmax:
        vmin, vmax = v.min(), v.max()
    sc = ax.scatter(df["sq_x"], df["sq_y"], c=v, s=8, cmap=cmap, vmin=vmin, vmax=vmax, linewidths=0)
    ax.set_title(title, fontsize=10)
    ax.set_aspect("equal")
    ax.invert_yaxis()
    plt.colorbar(sc, ax=ax, fraction=0.046, pad=0.04)


def main(argv=None) -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--output-root", default="data_work/outputs/directional_cost_edgeproj_v2_lcc_gate5_v1")
    args = p.parse_args(argv)

    root = Path(args.output_root)
    fig_dir = root / "figures"
    fig_dir.mkdir(parents=True, exist_ok=True)
    maps = pd.read_csv(root / "diagnostics" / "node_market_access_maps.csv")
    cf = pd.read_csv(root / "counterfactuals" / "policy_cf_summary.csv")
    wedge = pd.read_csv(root / "counterfactuals" / "policy_cf_world_wedge.csv")
    plt.style.use("seaborn-v0_8-whitegrid")

    # --- Market access maps (3 panels) ---
    fig, axes = plt.subplots(1, 3, figsize=(12.5, 4.2))
    _grid_plot(axes[0], maps, "log_pi_inv_dir", r"Residence access $\log \Pi_i^{-\theta}$ (directed)")
    _grid_plot(axes[1], maps, "log_pi_inv_sym", r"Residence access $\log \Pi_i^{-\theta}$ (symmetric)")
    _grid_plot(axes[2], maps, "log_pi_inv_sym_minus_dir", r"Sym $-$ dir: $\Delta\log \Pi_i^{-\theta}$")
    fig.suptitle("Residential Market Access", y=1.02, fontsize=12)
    fig.tight_layout()
    fig.savefig(fig_dir / "map_residence_access.png", dpi=220, bbox_inches="tight")
    plt.close(fig)

    fig, axes = plt.subplots(1, 3, figsize=(12.5, 4.2))
    _grid_plot(axes[0], maps, "log_p_inv_dir", r"Firm access $\log P_j^{-\theta}$ (directed)")
    _grid_plot(axes[1], maps, "log_p_inv_sym", r"Firm access $\log P_j^{-\theta}$ (symmetric)")
    _grid_plot(axes[2], maps, "log_p_inv_sym_minus_dir", r"Sym $-$ dir: $\Delta\log P_j^{-\theta}$")
    fig.suptitle("Workplace / Firm Market Access", y=1.02, fontsize=12)
    fig.tight_layout()
    fig.savefig(fig_dir / "map_firm_access.png", dpi=220, bbox_inches="tight")
    plt.close(fig)

    fig, axes = plt.subplots(1, 3, figsize=(12.5, 4.2))
    _grid_plot(axes[0], maps, "log_wage_index_dir", r"Wage index $\log A_j$ (directed)", cmap="viridis")
    _grid_plot(axes[1], maps, "log_wage_index_sym", r"Wage index $\log A_j$ (symmetric)", cmap="viridis")
    _grid_plot(axes[2], maps, "log_wage_sym_minus_dir", r"Sym $-$ dir: $\Delta\log A_j$")
    fig.suptitle(r"Effective Productivity $A_j=\bar A_j(L_j^F)^\alpha$", y=1.02, fontsize=12)
    fig.tight_layout()
    fig.savefig(fig_dir / "map_wage_productivity.png", dpi=220, bbox_inches="tight")
    plt.close(fig)

    # --- Policy CF: directed vs symmetric bars ---
    policies = cf["policy"].unique()
    x = np.arange(len(policies))
    w = 0.35
    fig, axes = plt.subplots(2, 2, figsize=(11, 8))
    for ax, col, ylab in [
        (axes[0, 0], "Wbar_pct_change", r"$\Delta \bar W$ (%)"),
        (axes[0, 1], "residence_dissimilarity_pct", "Residence dissimilarity (%)"),
        (axes[1, 0], "workplace_dissimilarity_pct", "Workplace dissimilarity (%)"),
        (axes[1, 1], "od_dissimilarity_pct", "Predicted OD dissimilarity (%)"),
    ]:
        d = cf[cf["world"] == "directed"].set_index("policy").loc[policies, col]
        s = cf[cf["world"] == "symmetric"].set_index("policy").loc[policies, col]
        ax.bar(x - w / 2, d, w, label="Directed world", color="#1d4ed8")
        ax.bar(x + w / 2, s, w, label="Symmetric world", color="#9a3412")
        ax.set_xticks(x)
        ax.set_xticklabels([p.replace("_", "\n") for p in policies], fontsize=8)
        ax.set_ylabel(ylab)
        ax.legend(fontsize=8)
    fig.suptitle("Non-directional policy CFs: same shock, two calibration worlds", y=1.01)
    fig.tight_layout()
    fig.savefig(fig_dir / "policy_cf_by_world.png", dpi=220, bbox_inches="tight")
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(8.5, 4.2))
    ax.bar(wedge["policy"], wedge["wedge_sym_minus_dir_Wbar_pp"], color="#475569")
    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_ylabel(r"$\Delta\bar W^{sym CF} - \Delta\bar W^{dir CF}$ (pp)")
    ax.set_title("Calibration-world wedge: symmetric minus directed welfare response")
    ax.set_xticks(range(len(wedge)))
    ax.set_xticklabels([p.replace("_", "\n") for p in wedge["policy"]], fontsize=8)
    fig.tight_layout()
    fig.savefig(fig_dir / "policy_cf_welfare_wedge.png", dpi=220, bbox_inches="tight")
    plt.close(fig)


if __name__ == "__main__":
    main()
