from __future__ import annotations

import json
import shutil
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
REPL_ROOT = ROOT / "Replication_apply"
SOURCE_DIR = ROOT / "data_work" / "outputs" / "model_input_edgeproj_v2_lcc_v1" / "data"
OUT_COUNTER = REPL_ROOT / "counterfactuals" / "seattle_edgeproj_v2_lcc"
OUT_PREP = REPL_ROOT / "prepare_from_data_work_square_edgeproj_v2_lcc"
OUT_INPUT = REPL_ROOT / "aa_input_square_edgeproj_v2_lcc"

DELTA1 = 0.488
EXPONENT = 1.0 / DELTA1
LANES_FALLBACK = 2.0


def build_node_file() -> tuple[pd.DataFrame, dict[int, int]]:
    nodes = pd.read_parquet(SOURCE_DIR / "qsm_input_nodes_square.parquet").copy()
    nodes = nodes.sort_values("node_i").reset_index(drop=True)
    old_to_new = {int(old): int(new) for new, old in enumerate(nodes["node_i"].astype(int).tolist(), start=1)}
    node_out = pd.DataFrame(
        {
            0: nodes["node_i"].astype(int).map(old_to_new).astype(int),
            1: pd.to_numeric(nodes["residents"], errors="coerce").fillna(0.0),
            2: pd.to_numeric(nodes["jobs"], errors="coerce").fillna(0.0),
            3: np.zeros(len(nodes), dtype=float),
            4: np.zeros(len(nodes), dtype=float),
        }
    )
    mapping = nodes[["grid_id", "node_i"]].copy()
    mapping["node_i_aa_1based"] = mapping["node_i"].astype(int).map(old_to_new).astype(int)
    mapping.to_csv(OUT_PREP / "node_index_mapping_square.csv", index=False)
    return node_out, old_to_new


def build_od_file(old_to_new: dict[int, int]) -> pd.DataFrame:
    od = pd.read_parquet(SOURCE_DIR / "qsm_input_od_square.parquet").copy()
    od = od[od["sample_definition"] == "reachable_AM"].copy()
    od["home_i_old"] = pd.to_numeric(od["home_i"], errors="coerce").astype(int)
    od["work_i_old"] = pd.to_numeric(od["work_i"], errors="coerce").astype(int)
    od = od[od["home_i_old"].isin(old_to_new) & od["work_i_old"].isin(old_to_new)].copy()
    flow = pd.to_numeric(od["commuters_road"], errors="coerce").fillna(0.0)
    return pd.DataFrame(
        {
            0: od["home_i_old"].map(old_to_new).astype(int),
            1: od["work_i_old"].map(old_to_new).astype(int),
            2: flow,
        }
    )


def build_edge_file(old_to_new: dict[int, int]) -> pd.DataFrame:
    edges = pd.read_parquet(SOURCE_DIR / "qsm_input_edges_square.parquet").copy()
    edges["i_old"] = pd.to_numeric(edges["i"], errors="coerce").astype(int)
    edges["j_old"] = pd.to_numeric(edges["j"], errors="coerce").astype(int)
    edges = edges[edges["i_old"].isin(old_to_new) & edges["j_old"].isin(old_to_new)].copy()

    lanes = pd.to_numeric(edges["lanes_directional"], errors="coerce").fillna(LANES_FALLBACK)
    tau_obs = pd.to_numeric(edges["tau_obs_min"], errors="coerce")
    tau_ff = pd.to_numeric(edges["tau_ff_min"], errors="coerce")
    length = pd.to_numeric(edges["total_link_len_m"], errors="coerce").fillna(0.0)
    valid = np.isfinite(tau_obs) & np.isfinite(tau_ff) & (tau_obs > 0) & (tau_ff > 0)
    edges = edges.loc[valid].copy()
    lanes = lanes.loc[valid]
    tau_obs = tau_obs.loc[valid]
    tau_ff = tau_ff.loc[valid]
    length = length.loc[valid]

    congestion_ratio = (tau_obs / tau_ff).replace([np.inf, -np.inf], np.nan).fillna(1.0).clip(lower=1.0)
    traffic = lanes * (congestion_ratio ** EXPONENT)

    return pd.DataFrame(
        {
            0: edges["i_old"].map(old_to_new).astype(int),
            1: edges["j_old"].map(old_to_new).astype(int),
            2: traffic.astype(float),
            3: length.astype(float),
            4: tau_obs.astype(float),
        }
    )


def main() -> None:
    OUT_COUNTER.mkdir(parents=True, exist_ok=True)
    OUT_PREP.mkdir(parents=True, exist_ok=True)
    OUT_INPUT.mkdir(parents=True, exist_ok=True)

    node_out, old_to_new = build_node_file()
    od_out = build_od_file(old_to_new)
    edge_out = build_edge_file(old_to_new)

    node_path = OUT_INPUT / "node_lr_lf_seattle.csv"
    od_path = OUT_INPUT / "sparse_commute_seattle.csv"
    edge_path = OUT_INPUT / "sparse_adjmat_seattle.csv"
    node_out.to_csv(node_path, header=False, index=False)
    od_out.to_csv(od_path, header=False, index=False)
    edge_out.to_csv(edge_path, header=False, index=False)
    for p in [node_path, od_path, edge_path]:
        shutil.copy2(p, OUT_COUNTER / p.name)

    summary = {
        "source_version": "model_input_edgeproj_v2_lcc_v1",
        "scope": "largest_connected_component",
        "node_rows": int(len(node_out)),
        "edge_rows": int(len(edge_out)),
        "od_rows": int(len(od_out)),
        "traffic_formula": "lanes * (tau_obs/tau_ff)^(1/delta1)",
        "delta1": DELTA1,
    }
    (OUT_PREP / "build_summary_square.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"saved={OUT_INPUT}")


if __name__ == "__main__":
    main()
