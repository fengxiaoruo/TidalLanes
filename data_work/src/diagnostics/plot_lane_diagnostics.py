"""
Plot paper-facing lane diagnostics for a retained versioned run.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]


def parse_args():
    parser = argparse.ArgumentParser(description="Plot lane diagnostics")
    parser.add_argument("--version-id", required=True, help="Version identifier under outputs/.")
    parser.add_argument("--output-dir", default="outputs", help="Base output directory.")
    return parser.parse_args()


def load_lane_master(version_root: Path) -> pd.DataFrame:
    data_dir = version_root / "data"
    return pd.read_parquet(data_dir / "centerline_lane_master.parquet")


def plot_lane_ratio_histogram(path: Path, lane_master: pd.DataFrame):
    ratios = pd.to_numeric(lane_master["opposite_dir_lane_ratio"], errors="coerce").dropna()
    ratios_plot = ratios.loc[ratios <= 1.5].copy()

    fig, ax = plt.subplots(figsize=(7.2, 4.8), dpi=240)
    bins = np.arange(1.0, 1.505, 0.025)
    ax.hist(ratios_plot, bins=bins, color="#4c78a8", alpha=0.85, edgecolor="white", linewidth=0.5)
    ax.axvline(1.0, color="#d62728", linestyle="--", linewidth=1.4)
    ax.axvline(1.5, color="#f28e2b", linestyle="--", linewidth=1.4)
    ax.set_xlim(1.0, 1.5)
    ax.set_xlabel("Opposite-direction lane ratio")
    ax.set_ylabel("Directed centerlines")
    ax.set_title("Distribution of opposite-direction lane ratios (truncated at 1.5)")
    ax.grid(True, axis="y", alpha=0.25)
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def run(version_id: str, output_dir: str):
    version_root = Path(output_dir) / version_id
    paper_appendix_dir = version_root / "figures" / "paper_appendix"
    paper_appendix_dir.mkdir(parents=True, exist_ok=True)

    lane_master = load_lane_master(version_root)
    plot_lane_ratio_histogram(
        paper_appendix_dir / "fig_lane_ratio_hist_baseline_v11.png",
        lane_master,
    )
    print(f"[plot_lane_diagnostics] saved figures to {paper_appendix_dir}")


def main():
    args = parse_args()
    run(args.version_id, args.output_dir)


if __name__ == "__main__":
    main()
