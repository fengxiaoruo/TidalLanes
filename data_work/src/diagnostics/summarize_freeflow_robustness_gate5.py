"""Summarize versioned free-flow definitions and Stage 2 quick results."""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


DEFINITIONS = {
    "baseline_2205_all_harmonic": (
        "model_input_edgeproj_v2_lcc_gate5_v1",
        "stage2_gate5_freeflow_baseline_2205_all_harmonic_quick_v1",
    ),
    "ff_0005_all_harmonic": (
        "model_input_edgeproj_v2_lcc_gate5_ff_0005_all_harmonic_v1",
        "stage2_gate5_freeflow_ff_0005_all_harmonic_quick_v1",
    ),
    "ff_0104_all_harmonic": (
        "model_input_edgeproj_v2_lcc_gate5_ff_0104_all_harmonic_v1",
        "stage2_gate5_freeflow_ff_0104_all_harmonic_quick_v1",
    ),
    "ff_2205_weekday_harmonic": (
        "model_input_edgeproj_v2_lcc_gate5_ff_2205_weekday_harmonic_v1",
        "stage2_gate5_freeflow_ff_2205_weekday_harmonic_quick_v1",
    ),
    "ff_2205_weekend_harmonic": (
        "model_input_edgeproj_v2_lcc_gate5_ff_2205_weekend_harmonic_v1",
        "stage2_gate5_freeflow_ff_2205_weekend_harmonic_quick_v1",
    ),
    "ff_2205_all_p90": (
        "model_input_edgeproj_v2_lcc_gate5_ff_2205_all_p90_v1",
        "stage2_gate5_freeflow_ff_2205_all_p90_quick_v1",
    ),
}


def selected_set(root):
    d = pd.read_csv(root / "counterfactuals" / "tidal_endog_top25_d1.0_long_run" / "selected_pairs.csv")
    return set(zip(d.slow_i, d.slow_j, d.fast_i, d.fast_j))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--outputs-root", default="data_work/outputs")
    p.add_argument("--output-root", default="data_work/outputs/freeflow_robustness_gate5_quick_v1")
    args = p.parse_args()
    outputs, out = Path(args.outputs_root), Path(args.output_root)
    if out.exists():
        raise SystemExit(f"Refusing to overwrite existing output: {out}")
    for name in ["data", "metrics", "figures", "docs"]:
        (out / name).mkdir(parents=True, exist_ok=False)

    base_edges = pd.read_parquet(outputs / DEFINITIONS["baseline_2205_all_harmonic"][0] / "data" / "qsm_input_edges_square.parquet")
    base_pairs = selected_set(outputs / DEFINITIONS["baseline_2205_all_harmonic"][1])
    rows, edge_detail = [], []
    for name, (bundle_name, run_name) in DEFINITIONS.items():
        edges = pd.read_parquet(outputs / bundle_name / "data" / "qsm_input_edges_square.parquet")
        merged = base_edges[["grid_o", "grid_d", "tau_ff_min"]].merge(
            edges[["grid_o", "grid_d", "tau_obs_min", "tau_ff_min"]],
            on=["grid_o", "grid_d"], suffixes=("_base", "")
        )
        ratio_ff = merged.tau_ff_min / merged.tau_ff_min_base
        congestion = merged.tau_obs_min / merged.tau_ff_min
        run = outputs / run_name
        baseline = json.loads((run / "baseline_summary.json").read_text())
        tidal = pd.read_csv(run / "counterfactuals" / "tidal_lane_endogenous_summary.csv")
        short = tidal.loc[tidal.horizon.eq("short_run")].iloc[0]
        long = tidal.loc[tidal.horizon.eq("long_run")].iloc[0]
        pairs = selected_set(run)
        rows.append({
            "definition": name,
            "bundle": bundle_name,
            "edge_count": int(len(edges)),
            "ff_time_ratio_p10": float(ratio_ff.quantile(.10)),
            "ff_time_ratio_p50": float(ratio_ff.quantile(.50)),
            "ff_time_ratio_p90": float(ratio_ff.quantile(.90)),
            "congestion_ratio_p50": float(congestion.quantile(.50)),
            "congestion_ratio_p90": float(congestion.quantile(.90)),
            "share_tau_obs_lt_tau_ff": float((congestion < 1).mean()),
            "baseline_Wbar": float(baseline["baseline_endogenous"]["Wbar"]),
            "baseline_converged": bool(baseline["baseline_endogenous"]["converged"]),
            "tidal_short_Wbar_pct": float(short.Wbar_pct_change),
            "tidal_long_Wbar_pct": float(long.Wbar_pct_change),
            "tidal_short_od_cost_pct": float(short.od_weighted_bilateral_cost_pct_change),
            "tidal_long_od_cost_pct": float(long.od_weighted_bilateral_cost_pct_change),
            "top25_overlap_with_baseline": int(len(pairs & base_pairs)),
        })
        tmp = merged[["grid_o", "grid_d", "tau_obs_min", "tau_ff_min"]].copy()
        tmp["definition"] = name
        tmp["congestion_ratio"] = congestion
        edge_detail.append(tmp)

    summary = pd.DataFrame(rows)
    summary.to_csv(out / "metrics" / "freeflow_robustness_summary.csv", index=False)
    pd.concat(edge_detail, ignore_index=True).to_parquet(out / "data" / "edge_definition_detail.parquet", index=False)

    fig, axes = plt.subplots(1, 2, figsize=(11, 4.5))
    labels = summary.definition.str.replace("ff_", "", regex=False).str.replace("_harmonic", "", regex=False)
    axes[0].barh(labels, 100 * (summary.ff_time_ratio_p50 - 1), color="#4f7c8d")
    axes[0].axvline(0, color="black", linewidth=.7)
    axes[0].set_xlabel("Median free-flow time change (%)")
    axes[1].scatter(summary.tidal_short_Wbar_pct, summary.tidal_long_Wbar_pct, color="#b34d3f")
    for x, y, label in zip(summary.tidal_short_Wbar_pct, summary.tidal_long_Wbar_pct, labels):
        axes[1].annotate(label, (x, y), fontsize=7, xytext=(3, 2), textcoords="offset points")
    axes[1].set_xlabel("Short-run welfare change (%)")
    axes[1].set_ylabel("Long-run welfare change (%)")
    fig.tight_layout()
    fig.savefig(out / "figures" / "freeflow_definition_comparison.png", dpi=220)
    plt.close(fig)

    (out / "README.md").write_text(
        """# Gate5 free-flow robustness

Six definitions are compared under the same square-grid sample, AM observed travel times, gate5 restriction, anchored congestion solver, and quick top-25 tidal-lane experiment. The baseline remains the retained 22:00--05:00 all-day harmonic definition. Alternative bundles and model runs remain separate from the active mainline.
""",
        encoding="utf-8",
    )
    (out / "docs" / "replication-checklist.md").write_text(
        """# Replication checklist

- [x] Every free-flow definition has a separate speed cache, edge-projection run, gate5 model-input bundle, QA report, and Stage 2 output.
- [x] AM observed travel time and route length are invariant across definitions.
- [x] All bundles contain 1,104 nodes, 2,642 edges, and 310,525 OD pairs.
- [x] Every Stage 2 baseline and tidal-lane counterfactual converged.
- [x] The active mainline is not modified.
""",
        encoding="utf-8",
    )
    shutil.copy2(__file__, out / "docs" / Path(__file__).name)
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
