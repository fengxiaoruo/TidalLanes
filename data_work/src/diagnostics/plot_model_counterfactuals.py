from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.diagnostics.mpl_setup import ensure_mpl_cache


def parse_args():
    parser = argparse.ArgumentParser(description="Plot structural-model counterfactual suite.")
    parser.add_argument("--version-id", default="raw_rebuild_validation")
    parser.add_argument("--output-dir", default="data_work/outputs")
    parser.add_argument("--model-subdir", default="model_square_suite")
    return parser.parse_args()


def main():
    args = parse_args()
    ensure_mpl_cache("data_work/outputs")
    import matplotlib.pyplot as plt

    plt.style.use("default")

    model_root = Path(args.output_dir) / args.version_id / args.model_subdir
    fig_dir = model_root / "figures"
    fig_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(model_root / "counterfactual_suite_summary.csv")
    df = df[df["scenario_family"].notna()].copy()

    policy = df[df["scenario_family"].isin(["symmetric", "congestion", "tidal"])].copy()
    if policy.empty:
        raise RuntimeError("No policy rows found in counterfactual suite summary.")

    family_order = ["symmetric", "congestion", "tidal"]
    color_map = {
        "symmetric": "#b35806",
        "congestion": "#2b8cbe",
        "tidal": "#1b7837",
    }

    fig, axes = plt.subplots(1, 2, figsize=(12, 5), constrained_layout=True)

    for fam in family_order:
        d = policy[policy["scenario_family"] == fam].sort_values(["top_n", "lambda_congestion"])
        label = fam if fam == "symmetric" else fam + " lane"
        axes[0].plot(
            d["lambda_congestion"],
            d["delta_welfare_pct_vs_baseline"],
            marker="o",
            linewidth=2,
            color=color_map[fam],
            label=label,
        )
        axes[1].plot(
            d["lambda_congestion"],
            d["delta_commute_min_vs_baseline"],
            marker="o",
            linewidth=2,
            color=color_map[fam],
            label=label,
        )

    axes[0].axhline(0, color="#666666", linewidth=1, linestyle="--")
    axes[1].axhline(0, color="#666666", linewidth=1, linestyle="--")
    axes[0].set_title("Welfare Change vs Baseline")
    axes[1].set_title("Commute-Time Change vs Baseline")
    axes[0].set_xlabel("Congestion Elasticity")
    axes[1].set_xlabel("Congestion Elasticity")
    axes[0].set_ylabel("Percent")
    axes[1].set_ylabel("Minutes")
    axes[0].legend(frameon=False)

    fig.savefig(fig_dir / "counterfactual_sensitivity_summary.png", dpi=220)
    fig.savefig(fig_dir / "counterfactual_sensitivity_summary.pdf")
    plt.close(fig)

    topn_only = policy[policy["top_n"].notna()].copy()
    if not topn_only.empty:
        fig, ax = plt.subplots(figsize=(8, 5), constrained_layout=True)
        for fam in ["congestion", "tidal"]:
            d = topn_only[topn_only["scenario_family"] == fam].sort_values(["top_n", "lambda_congestion"])
            for top_n in sorted(d["top_n"].dropna().unique()):
                sub = d[d["top_n"] == top_n]
                ax.plot(
                    sub["lambda_congestion"],
                    sub["delta_welfare_pct_vs_baseline"],
                    marker="o",
                    linewidth=2,
                    color=color_map[fam],
                    alpha=0.65 if int(top_n) == 5 else 1.0,
                    label=f"{fam} top {int(top_n)}",
                )
        ax.axhline(0, color="#666666", linewidth=1, linestyle="--")
        ax.set_title("Policy Welfare Effects by Treatment Rule")
        ax.set_xlabel("Congestion Elasticity")
        ax.set_ylabel("Percent vs Baseline")
        handles, labels = ax.get_legend_handles_labels()
        seen = set()
        keep_h = []
        keep_l = []
        for h, l in zip(handles, labels):
            if l in seen:
                continue
            seen.add(l)
            keep_h.append(h)
            keep_l.append(l)
        ax.legend(keep_h, keep_l, frameon=False)
        fig.savefig(fig_dir / "counterfactual_policy_welfare_by_rule.png", dpi=220)
        fig.savefig(fig_dir / "counterfactual_policy_welfare_by_rule.pdf")
        plt.close(fig)


if __name__ == "__main__":
    main()
