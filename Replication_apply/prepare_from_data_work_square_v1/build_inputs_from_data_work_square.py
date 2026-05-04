from __future__ import annotations

import json
from pathlib import Path
import shutil

import numpy as np
import pandas as pd


ROOT = Path("/Users/fxr/Desktop/TidalLanes")
REPL_ROOT = ROOT / "Replication_apply"
SOURCE_DIR = ROOT / "data_work" / "outputs" / "manual_centerline_rules_v11" / "data"
OUT_COUNTER = REPL_ROOT / "counterfactuals" / "seattle"
OUT_DERIVED = REPL_ROOT / "data" / "seattle" / "derived"
OUT_PREP = REPL_ROOT / "prepare_from_data_work_square_v1"
OUT_INPUT = REPL_ROOT / "aa_input_square_v1"


def get_active_node_ids() -> set[int]:
    nodes = pd.read_parquet(SOURCE_DIR / "qsm_input_nodes_square.parquet").copy()
    nodes = nodes[nodes["keep_for_model"] == 1].copy()
    nodes["node_i"] = pd.to_numeric(nodes["node_i"], errors="coerce")
    nodes["residents"] = pd.to_numeric(nodes["residents"], errors="coerce").fillna(0.0)
    nodes["jobs"] = pd.to_numeric(nodes["jobs"], errors="coerce").fillna(0.0)
    active = set(nodes.loc[(nodes["residents"] > 0) | (nodes["jobs"] > 0), "node_i"].dropna().astype(int).tolist())

    od = pd.read_parquet(SOURCE_DIR / "qsm_input_od_square.parquet").copy()
    od = od[od["sample_definition"] == "reachable_AM"].copy()
    od["commuters_road"] = pd.to_numeric(od["commuters_road"], errors="coerce").fillna(0.0)
    od["home_i"] = pd.to_numeric(od["home_i"], errors="coerce")
    od["work_i"] = pd.to_numeric(od["work_i"], errors="coerce")
    od = od[(od["commuters_road"] > 0) & od["home_i"].notna() & od["work_i"].notna()].copy()
    active.update(od["home_i"].astype(int).tolist())
    active.update(od["work_i"].astype(int).tolist())

    edges = pd.read_parquet(SOURCE_DIR / "qsm_input_edges_square.parquet").copy()
    edges = edges[edges["period"] == "AM"].copy()
    edges["i"] = pd.to_numeric(edges["i"], errors="coerce")
    edges["j"] = pd.to_numeric(edges["j"], errors="coerce")
    edges = edges[edges["i"].notna() & edges["j"].notna()].copy()
    active.update(edges["i"].astype(int).tolist())
    active.update(edges["j"].astype(int).tolist())
    return active


def build_node_file(active_node_ids: set[int]) -> tuple[pd.DataFrame, dict[int, int]]:
    nodes = pd.read_parquet(SOURCE_DIR / "qsm_input_nodes_square.parquet").copy()
    nodes = nodes[nodes["keep_for_model"] == 1].copy()
    nodes["node_i"] = pd.to_numeric(nodes["node_i"], errors="coerce")
    nodes = nodes[nodes["node_i"].notna()].copy()
    nodes["node_i"] = nodes["node_i"].astype(int)
    nodes = nodes[nodes["node_i"].isin(active_node_ids)].copy()
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
    mapping["node_i_old"] = mapping["node_i"].astype(int)
    mapping["node_i_aa_1based"] = mapping["node_i_old"].map(old_to_new).astype(int)
    mapping.to_csv(OUT_PREP / "node_index_mapping_square.csv", index=False)

    return node_out, old_to_new


def build_od_file(old_to_new: dict[int, int]) -> pd.DataFrame:
    od = pd.read_parquet(SOURCE_DIR / "qsm_input_od_square.parquet").copy()
    od = od[od["sample_definition"] == "reachable_AM"].copy()
    od["home_i_old"] = pd.to_numeric(od["home_i"], errors="coerce")
    od["work_i_old"] = pd.to_numeric(od["work_i"], errors="coerce")
    od = od[od["home_i_old"].notna() & od["work_i_old"].notna()].copy()
    od["home_i_old"] = od["home_i_old"].astype(int)
    od["work_i_old"] = od["work_i_old"].astype(int)
    od = od[od["home_i_old"].isin(old_to_new) & od["work_i_old"].isin(old_to_new)].copy()

    flow = pd.to_numeric(od["commuters_road"], errors="coerce").fillna(0.0)
    od_out = pd.DataFrame(
        {
            0: od["home_i_old"].map(old_to_new).astype(int),
            1: od["work_i_old"].map(old_to_new).astype(int),
            2: flow,
        }
    )
    return od_out


def build_edge_file(old_to_new: dict[int, int]) -> pd.DataFrame:
    edges = pd.read_parquet(SOURCE_DIR / "qsm_input_edges_square.parquet").copy()
    edges = edges[edges["period"] == "AM"].copy()
    edges["i_old"] = pd.to_numeric(edges["i"], errors="coerce")
    edges["j_old"] = pd.to_numeric(edges["j"], errors="coerce")
    edges = edges[edges["i_old"].notna() & edges["j_old"].notna()].copy()
    edges["i_old"] = edges["i_old"].astype(int)
    edges["j_old"] = edges["j_old"].astype(int)
    edges = edges[edges["i_old"].isin(old_to_new) & edges["j_old"].isin(old_to_new)].copy()

    tau_obs = pd.to_numeric(edges["tau_obs_min"], errors="coerce")
    tau_ff = pd.to_numeric(edges["tau_ff_min"], errors="coerce")
    cap = pd.to_numeric(edges["capacity_proxy"], errors="coerce")
    length = pd.to_numeric(edges["total_link_len_m"], errors="coerce")

    tau_ff_safe = tau_ff.where((tau_ff > 0) & np.isfinite(tau_ff), np.nan)
    cap_safe = cap.where((cap > 0) & np.isfinite(cap), 1.0).fillna(1.0)
    congestion_ratio = (tau_obs / tau_ff_safe).replace([np.inf, -np.inf], np.nan).fillna(1.0)

    # First-pass AA-style traffic proxy:
    # positive, monotone in congestion, and scaled by directional capacity.
    traffic = cap_safe * np.clip(congestion_ratio - 1.0, 0.01, None)

    edge_out = pd.DataFrame(
        {
            0: edges["i_old"].map(old_to_new).astype(int),
            1: edges["j_old"].map(old_to_new).astype(int),
            2: traffic.astype(float),
            3: length.fillna(0.0).astype(float),
            4: tau_obs.fillna(0.0).astype(float),
        }
    )
    return edge_out


def write_summary(node_out: pd.DataFrame, od_out: pd.DataFrame, edge_out: pd.DataFrame) -> None:
    summary = {
        "source_version": "manual_centerline_rules_v11",
        "grid_type": "square",
        "node_rows": int(len(node_out)),
        "edge_rows": int(len(edge_out)),
        "od_rows": int(len(od_out)),
        "traffic_min": float(edge_out[2].min()) if len(edge_out) else None,
        "traffic_mean": float(edge_out[2].mean()) if len(edge_out) else None,
        "traffic_max": float(edge_out[2].max()) if len(edge_out) else None,
        "filter_rule": "keep keep_for_model nodes that are active in at least one of: positive residents/jobs, positive reachable_AM commuters_road, or AM edge incidence.",
        "note": "Coordinates in node_lr_lf_seattle.csv are placeholder zeros in this first-pass quick-run version.",
    }
    with open(OUT_PREP / "build_summary_square.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)


def main() -> None:
    OUT_COUNTER.mkdir(parents=True, exist_ok=True)
    OUT_DERIVED.mkdir(parents=True, exist_ok=True)
    OUT_PREP.mkdir(parents=True, exist_ok=True)
    OUT_INPUT.mkdir(parents=True, exist_ok=True)

    active_node_ids = get_active_node_ids()
    node_out, old_to_new = build_node_file(active_node_ids)
    od_out = build_od_file(old_to_new)
    edge_out = build_edge_file(old_to_new)

    node_path = OUT_INPUT / "node_lr_lf_seattle.csv"
    od_path = OUT_INPUT / "sparse_commute_seattle.csv"
    edge_path = OUT_INPUT / "sparse_adjmat_seattle.csv"

    node_out.to_csv(node_path, header=False, index=False)
    od_out.to_csv(od_path, header=False, index=False)
    edge_out.to_csv(edge_path, header=False, index=False)

    shutil.copy2(node_path, OUT_COUNTER / node_path.name)
    shutil.copy2(od_path, OUT_COUNTER / od_path.name)
    shutil.copy2(edge_path, OUT_COUNTER / edge_path.name)

    write_summary(node_out, od_out, edge_out)

    print(f"saved={node_path}")
    print(f"saved={od_path}")
    print(f"saved={edge_path}")
    print(f"synced={OUT_COUNTER / node_path.name}")
    print(f"synced={OUT_COUNTER / od_path.name}")
    print(f"synced={OUT_COUNTER / edge_path.name}")
    print(f"saved={OUT_PREP / 'build_summary_square.json'}")
    print(f"saved={OUT_PREP / 'node_index_mapping_square.csv'}")


if __name__ == "__main__":
    main()
