"""Plot tidal-lane short-run vs long-run comparison."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))


def main(argv=None) -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--repo-root", default=".")
    p.add_argument("--output-name", default="directional_cost_edgeproj_v2_lcc_gate5_v1")
    args = p.parse_args(argv)

    repo = Path(args.repo_root).resolve()
    if not (repo / "data_work").exists() and (repo.parent / "data_work").exists():
        repo = repo.parent

    cf_dir = repo / "data_work" / "outputs" / args.output_name / "counterfactuals" / "tidal_lane"
    fig_dir = repo / "data_work" / "outputs" / args.output_name / "figures"
    fig_dir.mkdir(parents=True, exist_ok=True)

    sl = pd.read_csv(cf_dir / "tidal_lane_short_vs_long.csv")
    bpr = sl[sl["method"] == "bpr"].copy()
    direct = sl[sl["method"] == "direct_tau"].copy()

    fig, axes = plt.subplots(1, 2, figsize=(10, 4.2))

    ax = axes[0]
    for top_n, grp in bpr.groupby("top_n"):
        ax.scatter(
            grp["Wbar_pct_change_short_run"],
            grp["Wbar_pct_change_long_run"],
            label=f"BPR top-{top_n}",
            s=35,
            alpha=0.85,
        )
    if not direct.empty:
        ax.scatter(
            direct["Wbar_pct_change_short_run"],
            direct["Wbar_pct_change_long_run"],
            marker="x",
            c="black",
            s=55,
            label="direct $\\tau$ scale",
        )
    lims = [
        min(ax.get_xlim()[0], ax.get_ylim()[0]),
        max(ax.get_xlim()[1], ax.get_ylim()[1]),
    ]
    ax.plot(lims, lims, "k--", lw=0.8, alpha=0.5)
    ax.axhline(0, c="0.7", lw=0.6)
    ax.axvline(0, c="0.7", lw=0.6)
    ax.set_xlabel("Short-run $\\Delta\\bar W$ (\\%, fixed shares)")
    ax.set_ylabel("Long-run $\\Delta\\bar W$ (\\%, sorting)")
    ax.set_title("Tidal lane: short vs long welfare")
    ax.legend(fontsize=7, loc="lower right")

    ax = axes[1]
    for top_n, grp in bpr.groupby("top_n"):
        ax.scatter(
            grp["od_dissimilarity_pct_short_run"],
            grp["od_dissimilarity_pct_long_run"],
            label=f"BPR top-{top_n}",
            s=35,
            alpha=0.85,
        )
    if not direct.empty:
        ax.scatter(
            direct["od_dissimilarity_pct_short_run"],
            direct["od_dissimilarity_pct_long_run"],
            marker="x",
            c="black",
            s=55,
            label="direct $\\tau$ scale",
        )
    ax.plot([0, ax.get_xlim()[1]], [0, ax.get_xlim()[1]], "k--", lw=0.8, alpha=0.5)
    ax.set_xlabel("Short-run OD dissimilarity (\\%)")
    ax.set_ylabel("Long-run OD dissimilarity (\\%)")
    ax.set_title("Commuting-flow response")
    ax.legend(fontsize=7, loc="lower right")

    fig.tight_layout()
    out = fig_dir / "tidal_lane_short_vs_long.png"
    fig.savefig(out, dpi=160, bbox_inches="tight")
    plt.close(fig)
    print(f"Wrote {out}")


if __name__ == "__main__":
    main()
