"""
Mainline tidal results on different road networks (2025 OD, mainline parameters)
===============================================================================
Runs compared (run_tidal_lane_diagnostics_gate5 defaults: anchored, lambda_phys 1.1,
alpha 0.05, beta -0.10, score_bpr_st):
    (each also at lambda_phys 0.2, *_lam020; overlap then against gate5 at 0.2)
    gate5        tidal_lane_diagnostics_gate5_bpr_pos05_physical_time_od2025_v2
    osm1a        ..._osm1a_gate5_..._od2025_v2       (gate5 cells only)
    osm1b        ..._osm1b_L300_gate5_..._od2025_v2  (gate5 cells only)
    osm1b_allcells ..._osm1b_L300_gate5_..._od2025_v1 (every cell the network reaches)
For each: bundle size, baseline convergence, W-bar change and physical time per spec,
and overlap of the selected top-k corridors with gate5 (as undirected grid-cell pairs).
Inputs read only. Output: data_work/outputs/<output-name>/ (refuses to overwrite)
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

O = Path(__file__).resolve().parents[2] / "outputs"
RUNS = {
    "gate5": ("model_input_edgeproj_v2_lcc_gate5_od2025_v2", "tidal_lane_diagnostics_gate5_bpr_pos05_physical_time_od2025_v2"),
    "osm1a": ("model_input_osm1a_gate5_od2025_v2", "tidal_lane_diagnostics_osm1a_gate5_bpr_pos05_physical_time_od2025_v2"),
    "osm1b": ("model_input_osm1b_L300_gate5_od2025_v2", "tidal_lane_diagnostics_osm1b_L300_gate5_bpr_pos05_physical_time_od2025_v2"),
    "osm1b_allcells": ("model_input_osm1b_L300_gate5_od2025_v1", "tidal_lane_diagnostics_osm1b_L300_gate5_bpr_pos05_physical_time_od2025_v1"),
    # same, lambda_phys = 0.2 (near this data's edge-level estimates); overlap is against gate5 at 0.2
    "gate5_lam020": ("model_input_edgeproj_v2_lcc_gate5_od2025_v2", "tidal_lane_diagnostics_gate5_bpr_pos05_physical_time_lam020_od2025_v2"),
    "osm1a_lam020": ("model_input_osm1a_gate5_od2025_v2", "tidal_lane_diagnostics_osm1a_bpr_pos05_physical_time_lam020_od2025_v2"),
    "osm1b_lam020": ("model_input_osm1b_L300_gate5_od2025_v2", "tidal_lane_diagnostics_osm1b_bpr_pos05_physical_time_lam020_od2025_v2"),
}
SPECS = ["top25_d1.0", "top50_d1.0", "top100_d1.0"]


def corridors(bundle: str, run: str, spec: str) -> list[frozenset]:
    nodes = pd.read_parquet(O / bundle / "data/qsm_input_nodes_square.parquet").sort_values("node_i")
    gid = nodes.grid_id.to_numpy()
    s = pd.read_csv(O / run / spec / "selected_pairs.csv")
    return [frozenset((gid[a], gid[b])) for a, b in zip(s.slow_i, s.slow_j)]


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-name", default="network_mainline_compare_v2")
    args = ap.parse_args(argv)
    out = O / args.output_name
    if out.exists():
        raise SystemExit(f"{out} exists; choose a new --output-name")
    rows = []
    for k, (b, r) in RUNS.items():
        if not (O / r / "tidal_st_lt_compact.csv").exists():
            print("missing", r)
            continue
        e = pd.read_parquet(O / b / "data/qsm_input_edges_square.parquet", columns=["tau_obs_min", "tau_ff_min"])
        n = pd.read_parquet(O / b / "data/qsm_input_nodes_square.parquet", columns=["residents"])
        base = json.loads((O / r / "baseline/equilibrium_meta.json").read_text())
        c = pd.read_csv(O / r / "tidal_st_lt_compact.csv").set_index("spec")
        for spec in SPECS:
            ov = {}
            ref_key = "gate5_lam020" if k.endswith("_lam020") else "gate5"
            if k != ref_key:
                mine, ref = set(corridors(b, r, spec)), set(corridors(*RUNS[ref_key], spec))
                ov = {"overlap_with_gate5": len(mine & ref)}
            rows.append({"network": k, "spec": spec, "nodes": len(n), "edges": len(e),
                         "commuters": round(float(n.residents.sum())),
                         "edge_AM_over_FF_median": round(float((e.tau_obs_min / e.tau_ff_min).median()), 3),
                         "baseline_converged": base["converged"], "baseline_outer": base["n_outer"],
                         "Wbar_st_pct": round(float(c.loc[spec, "Wbar_pct_change_st"]), 3),
                         "Wbar_lt_pct": round(float(c.loc[spec, "Wbar_pct_change_lt"]), 3),
                         "phys_time_base_min": round(float(c.loc[spec, "avg_physical_time_base_min"]), 2),
                         "saved_st_sec": round(float(c.loc[spec, "physical_time_saved_st_sec_per_commuter"]), 1),
                         "saved_lt_sec": round(float(c.loc[spec, "physical_time_saved_lt_sec_per_commuter"]), 1),
                         **ov})
    d = pd.DataFrame(rows)
    out.mkdir(parents=True)
    d.to_csv(out / "network_mainline_compare.csv", index=False)
    (out / "summary.json").write_text(json.dumps({"runs": RUNS, "rows": rows}, indent=2, ensure_ascii=False, default=str))
    with pd.option_context("display.width", 250, "display.max_columns", 30):
        print(d)


if __name__ == "__main__":
    main()
