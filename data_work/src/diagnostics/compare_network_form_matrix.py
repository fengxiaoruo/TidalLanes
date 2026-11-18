"""
Tidal mainline results: road network x congestion function (2025 OD)
====================================================================
Runs: outputs/tidal_lane_diagnostics_<net>_<form>_<tag>_od2025_v1, made by
run_tidal_lane_diagnostics_gate5.py with mainline alpha / beta / selection and
    ce_floor  constant elasticity with a free-flow floor, beta = 0.25 (tag b025_fl001_s4)
    bpr       anchored BPR, b = 4.25 (2026-11-04 binned fit)    (tag b425_fl001_s4)
    bpr_iv    anchored BPR, b = 1.65 (demand-IV estimate, edge 06-22 h,
              run_congestion_bpr_iv.py)                         (tag b165_fl001_s4)
both with --xi-floor-rel 0.01. Networks and bundles:
    gate5        model_input_edgeproj_v2_lcc_gate5_od2025_v2 (Baidu raw v1, edgeproj, current)
    v3gate5      model_input_raw_connected_v3_edgeproj_v2_lcc_gate5_lanefix_od2025_v1
    osm1a, osm1b model_input_osm1a_gate5_od2025_v2, model_input_osm1b_L300_gate5_od2025_v2
    clv11routed, clv3routed  model_input_centerline_{v11,v3}_routed_gate5_od2025_v1
Per run and spec: convergence of baseline and both counterfactuals, W-bar change, average
physical commute time, seconds saved, and overlap of selected corridors (undirected cell
pairs) with gate5 under the same function.
Inputs read only. Output: data_work/outputs/<output-name>/ (refuses to overwrite)
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

O = Path(__file__).resolve().parents[2] / "outputs"
NETS = {
    "gate5": "model_input_edgeproj_v2_lcc_gate5_od2025_v2",
    "v3gate5": "model_input_raw_connected_v3_edgeproj_v2_lcc_gate5_lanefix_od2025_v1",
    "osm1a": "model_input_osm1a_gate5_od2025_v2",
    "osm1b": "model_input_osm1b_L300_gate5_od2025_v2",
    "clv11routed": "model_input_centerline_v11_routed_gate5_od2025_v1",
    "clv3routed": "model_input_centerline_v3_routed_gate5_od2025_v1",
}
FORMS = {"ce_floor": ("ce_floor", "b025_fl001_s4"), "bpr": ("bpr", "b425_fl001_s4"), "bpr_iv": ("bpr", "b165_fl001_s4")}
SPECS = ["top25_d1.0", "top50_d1.0", "top100_d1.0"]


def run_dir(net: str, form: str) -> Path:
    f, tag = FORMS[form]
    return O / f"tidal_lane_diagnostics_{net}_{f}_{tag}_od2025_v1"


def corridors(net: str, form: str, spec: str) -> set[frozenset]:
    nodes = pd.read_parquet(O / NETS[net] / "data/qsm_input_nodes_square.parquet").sort_values("node_i")
    gid = nodes.grid_id.to_numpy()
    s = pd.read_csv(run_dir(net, form) / spec / "selected_pairs.csv")
    return {frozenset((gid[a], gid[b])) for a, b in zip(s.slow_i, s.slow_j)}


def meta(p: Path) -> dict:
    return json.loads((p / "equilibrium_meta.json").read_text())


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-name", default="network_form_matrix_v2")
    args = ap.parse_args(argv)
    out = O / args.output_name
    if out.exists():
        raise SystemExit(f"{out} exists; choose a new --output-name")
    rows = []
    for form in FORMS:
        for net in NETS:
            r = run_dir(net, form)
            if not (r / "tidal_st_lt_compact.csv").exists():
                rows.append({"form": form, "network": net, "status": "missing"})
                continue
            n = pd.read_parquet(O / NETS[net] / "data/qsm_input_nodes_square.parquet", columns=["residents"])
            e = pd.read_parquet(O / NETS[net] / "data/qsm_input_edges_square.parquet", columns=["tau_obs_min"])
            c = pd.read_csv(r / "tidal_st_lt_compact.csv").set_index("spec")
            b = meta(r / "baseline")
            for spec in SPECS:
                st, lt = meta(r / spec / "short_run"), meta(r / spec / "long_run")
                row = {"form": form, "network": net, "spec": spec, "status": "ok",
                       "nodes": len(n), "edges": len(e), "commuters": round(float(n.residents.sum())),
                       "baseline_converged": b["converged"], "st_converged": st["converged"], "lt_converged": lt["converged"],
                       "Wbar_st_pct": round(float(c.loc[spec, "Wbar_pct_change_st"]), 3),
                       "Wbar_lt_pct": round(float(c.loc[spec, "Wbar_pct_change_lt"]), 3),
                       "phys_time_base_min": round(float(c.loc[spec, "avg_physical_time_base_min"]), 2),
                       "saved_st_sec": round(float(c.loc[spec, "physical_time_saved_st_sec_per_commuter"]), 1),
                       "saved_lt_sec": round(float(c.loc[spec, "physical_time_saved_lt_sec_per_commuter"]), 1)}
                if net != "gate5" and (run_dir("gate5", form) / spec / "selected_pairs.csv").exists():
                    row["overlap_with_gate5"] = len(corridors(net, form, spec) & corridors("gate5", form, spec))
                rows.append(row)
    d = pd.DataFrame(rows)
    out.mkdir(parents=True)
    d.to_csv(out / "network_form_matrix.csv", index=False)
    (out / "summary.json").write_text(json.dumps({"bundles": NETS, "forms": FORMS, "rows": rows}, indent=2, ensure_ascii=False, default=str))
    with pd.option_context("display.width", 260, "display.max_columns", 30, "display.max_rows", 100):
        print(d)


if __name__ == "__main__":
    main()
