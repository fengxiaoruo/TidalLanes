"""Plot retained diagnostics for the directional-cost model."""
from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


def main(argv=None) -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--output-root", default="data_work/outputs/directional_cost_square_v1")
    args = p.parse_args(argv)

    root = Path(args.output_root)
    fig_dir = root / "figures"
    fig_dir.mkdir(parents=True, exist_ok=True)

    pairs = pd.read_csv(root / "diagnostics" / "reciprocal_pair_asymmetry.csv")
    intensity = pd.read_csv(root / "simulations" / "asymmetry_intensity.csv")
    threshold = pd.read_csv(root / "simulations" / "asymmetry_threshold.csv")

    plt.style.use("seaborn-v0_8-whitegrid")

    fig, ax = plt.subplots(figsize=(7.2, 4.6))
    ax.hist(pairs["tau_ratio"], bins=60, color="#3c6e71", alpha=0.85)
    ax.axvline(1.10, color="#9a3412", linewidth=1.2)
    ax.axvline(1.50, color="#9a3412", linewidth=1.2, linestyle="--")
    ax.axvline(2.00, color="#9a3412", linewidth=1.2, linestyle=":")
    ax.set_xlim(1, min(6, pairs["tau_ratio"].quantile(0.995)))
    ax.set_xlabel("Reciprocal-pair travel-time ratio")
    ax.set_ylabel("Number of reciprocal pairs")
    ax.set_title("Observed Directional Congestion Asymmetry")
    fig.tight_layout()
    fig.savefig(fig_dir / "asymmetry_ratio_histogram.png", dpi=220)
    plt.close(fig)

    fig, ax1 = plt.subplots(figsize=(7.2, 4.6))
    ax1.plot(intensity["asymmetry_intensity"], intensity["Wbar_pct_change"], marker="o", color="#1d4ed8")
    ax1.set_xlabel("Asymmetry intensity, rho")
    ax1.set_ylabel("Welfare change vs symmetric baseline (%)", color="#1d4ed8")
    ax1.tick_params(axis="y", labelcolor="#1d4ed8")
    ax2 = ax1.twinx()
    ax2.plot(intensity["asymmetry_intensity"], intensity["residence_dissimilarity_pct"], marker="s", color="#7c2d12")
    ax2.set_ylabel("Residence dissimilarity (%)", color="#7c2d12")
    ax2.tick_params(axis="y", labelcolor="#7c2d12")
    ax1.set_title("Asymmetry Intensity Simulation")
    fig.tight_layout()
    fig.savefig(fig_dir / "simulation_asymmetry_intensity.png", dpi=220)
    plt.close(fig)

    fig, ax1 = plt.subplots(figsize=(7.2, 4.6))
    ax1.plot(threshold["threshold_tau_ratio"], threshold["Wbar_pct_change"], marker="o", color="#1d4ed8")
    ax1.set_xlabel("Minimum reciprocal-pair ratio kept directed")
    ax1.set_ylabel("Welfare change vs symmetric baseline (%)", color="#1d4ed8")
    ax1.tick_params(axis="y", labelcolor="#1d4ed8")
    ax2 = ax1.twinx()
    ax2.plot(
        threshold["threshold_tau_ratio"],
        100 * threshold["share_reciprocal_edges_kept_directed"],
        marker="s",
        color="#7c2d12",
    )
    ax2.set_ylabel("Reciprocal edges kept directed (%)", color="#7c2d12")
    ax2.tick_params(axis="y", labelcolor="#7c2d12")
    ax1.set_title("Asymmetry Threshold Simulation")
    fig.tight_layout()
    fig.savefig(fig_dir / "simulation_asymmetry_threshold.png", dpi=220)
    plt.close(fig)


if __name__ == "__main__":
    main()

