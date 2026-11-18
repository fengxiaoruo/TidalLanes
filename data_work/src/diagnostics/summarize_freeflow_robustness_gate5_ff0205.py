"""Summarize the 02:00-04:59 free-flow definitions against the retained gate5 mainline.

Same chain and metrics as ``freeflow_robustness_gate5_quick_v1/docs/summarize_freeflow_robustness_gate5.py``
(2026-08-04), extended with road-segment cache statistics and the gate5-edge share with
``tau_obs_min < tau_ff_min``. Rows for the retained baseline and two v1 definitions are
included for reference; the new rows are ``ff_0205_all_harmonic`` and ``ff_0205_all_p90``.
"""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

import numpy as np
import pandas as pd


# name -> (ff cache dir, bundle, stage2 quick run)
DEFINITIONS = {
    "baseline_2205_all_harmonic": (
        "raw_connected_v1_grid_travel_time_edgeproj_v2/data",
        "model_input_edgeproj_v2_lcc_gate5_v1",
        "stage2_gate5_freeflow_baseline_2205_all_harmonic_quick_v1",
    ),
    "ff_0104_all_harmonic": (
        "freeflow_speed_definitions_v1/definitions/ff_0104_all_harmonic",
        "model_input_edgeproj_v2_lcc_gate5_ff_0104_all_harmonic_v1",
        "stage2_gate5_freeflow_ff_0104_all_harmonic_quick_v1",
    ),
    "ff_2205_all_p90": (
        "freeflow_speed_definitions_v1/definitions/ff_2205_all_p90",
        "model_input_edgeproj_v2_lcc_gate5_ff_2205_all_p90_v1",
        "stage2_gate5_freeflow_ff_2205_all_p90_quick_v1",
    ),
    "ff_0205_all_harmonic": (
        "freeflow_speed_definitions_v2/definitions/ff_0205_all_harmonic",
        "model_input_edgeproj_v2_lcc_gate5_ff_0205_all_harmonic_v1",
        "stage2_gate5_freeflow_ff_0205_all_harmonic_quick_v1",
    ),
    "ff_0205_all_p90": (
        "freeflow_speed_definitions_v2/definitions/ff_0205_all_p90",
        "model_input_edgeproj_v2_lcc_gate5_ff_0205_all_p90_v1",
        "stage2_gate5_freeflow_ff_0205_all_p90_quick_v1",
    ),
}
NEW = ["ff_0205_all_harmonic", "ff_0205_all_p90"]


def selected_set(root: Path) -> set:
    d = pd.read_csv(root / "counterfactuals" / "tidal_endog_top25_d1.0_long_run" / "selected_pairs.csv")
    return set(zip(d.slow_i, d.slow_j, d.fast_i, d.fast_j))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--outputs-root", default="data_work/outputs")
    p.add_argument("--output-root", default="data_work/outputs/freeflow_robustness_gate5_ff0205_v1")
    args = p.parse_args()
    outputs, out = Path(args.outputs_root), Path(args.output_root)
    if out.exists():
        raise SystemExit(f"Refusing to overwrite existing output: {out}")
    for name in ["data", "metrics", "docs"]:
        (out / name).mkdir(parents=True, exist_ok=False)

    base_cache = outputs / DEFINITIONS["baseline_2205_all_harmonic"][0]
    base_ff = pd.read_csv(base_cache / "raw_connected_speed_ff_by_roadseg.csv")
    base_ff["roadseg_id"] = base_ff.roadseg_id.astype(str)
    base_bundle = outputs / DEFINITIONS["baseline_2205_all_harmonic"][1]
    base_edges = pd.read_parquet(base_bundle / "data" / "qsm_input_edges_square.parquet")
    base_pairs = selected_set(outputs / DEFINITIONS["baseline_2205_all_harmonic"][2])

    rows, detail = [], []
    for name, (cache, bundle_name, run_name) in DEFINITIONS.items():
        ff = pd.read_csv(outputs / cache / "raw_connected_speed_ff_by_roadseg.csv")
        ff["roadseg_id"] = ff.roadseg_id.astype(str)
        m = base_ff.merge(ff, on="roadseg_id", suffixes=("_base", "_alt"))
        speed_ratio = m.speed_ff_kmh_alt / m.speed_ff_kmh_base

        bundle = outputs / bundle_name
        edges = pd.read_parquet(bundle / "data" / "qsm_input_edges_square.parquet")
        bsum = json.loads((bundle / "metrics" / "travel_cost_bundle_summary.json").read_text())
        qa = json.loads((bundle / "qa" / "model_input_qa.json").read_text())
        merged = base_edges[["grid_o", "grid_d", "tau_obs_min", "tau_ff_min"]].merge(
            edges[["grid_o", "grid_d", "tau_obs_min", "tau_ff_min"]],
            on=["grid_o", "grid_d"], suffixes=("_base", ""),
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
            "is_new": name in NEW,
            "bundle": bundle_name,
            "stage2_run": run_name,
            "roadsegs_with_ff_speed": int(len(ff)),
            "roadseg_n_obs_p50": float(ff.n_obs_speed_ff_kmh.median()),
            "roadseg_common_with_baseline": int(len(m)),
            "roadseg_speed_ratio_p10": float(speed_ratio.quantile(.10)),
            "roadseg_speed_ratio_p50": float(speed_ratio.quantile(.50)),
            "roadseg_speed_ratio_p90": float(speed_ratio.quantile(.90)),
            "n_nodes": int(bsum["n_nodes"]),
            "n_edges": int(bsum["n_edges"]),
            "n_od_pairs": int(bsum["n_od_pairs"]),
            "qa_weak_components": int(qa["weak_component_count"]),
            "qa_edges_above_gate": int(qa["edges_above_gate"]),
            "am_tau_obs_identical_to_baseline": bool(np.allclose(merged.tau_obs_min, merged.tau_obs_min_base)),
            "edge_ff_time_ratio_p10": float(ratio_ff.quantile(.10)),
            "edge_ff_time_ratio_p50": float(ratio_ff.quantile(.50)),
            "edge_ff_time_ratio_p90": float(ratio_ff.quantile(.90)),
            "edges_tau_obs_lt_tau_ff": int((congestion < 1).sum()),
            "share_tau_obs_lt_tau_ff": float((congestion < 1).mean()),
            "congestion_ratio_p50": float(congestion.quantile(.50)),
            "congestion_ratio_p90": float(congestion.quantile(.90)),
            "baseline_Wbar": float(baseline["baseline_endogenous"]["Wbar"]),
            "baseline_converged": bool(baseline["baseline_endogenous"]["converged"]),
            "baseline_median_tau_ratio_vs_obs": float(baseline["baseline_endogenous"]["median_tau_ratio_vs_obs"]),
            "tidal_short_Wbar_pct": float(short.Wbar_pct_change),
            "tidal_long_Wbar_pct": float(long.Wbar_pct_change),
            "tidal_short_od_cost_pct": float(short.od_weighted_bilateral_cost_pct_change),
            "tidal_long_od_cost_pct": float(long.od_weighted_bilateral_cost_pct_change),
            "tidal_short_converged": bool(short.scenario_converged),
            "tidal_long_converged": bool(long.scenario_converged),
            "top25_overlap_with_baseline": int(len(pairs & base_pairs)),
        })
        tmp = merged[["grid_o", "grid_d", "tau_obs_min", "tau_ff_min"]].copy()
        tmp["definition"] = name
        tmp["congestion_ratio"] = congestion
        detail.append(tmp)

    summary = pd.DataFrame(rows)
    summary.to_csv(out / "metrics" / "freeflow_robustness_ff0205_summary.csv", index=False)
    pd.concat(detail, ignore_index=True).to_parquet(out / "data" / "edge_definition_detail.parquet", index=False)
    shutil.copy2(__file__, out / "docs" / Path(__file__).name)
    with pd.option_context("display.width", 250, "display.max_columns", 50):
        print(summary.T.to_string())


if __name__ == "__main__":
    main()
