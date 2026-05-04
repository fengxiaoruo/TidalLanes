from __future__ import annotations

import json
from pathlib import Path
import shutil

import numpy as np
import pandas as pd


ROOT = Path("/Users/fxr/Desktop/TidalLanes")
REPL_ROOT = ROOT / "Replication_apply"
SOURCE_DIR = ROOT / "data_work" / "outputs" / "manual_centerline_rules_v11" / "data"
EDGEPROJ_CSV = (
    ROOT
    / "data_work"
    / "outputs"
    / "raw_connected_v1_grid_travel_time_edgeproj_v2"
    / "data"
    / "raw_connected_vs_old_adjacent_adjacent_square_AM.csv"
)
OUT_COUNTER = REPL_ROOT / "counterfactuals" / "seattle"
OUT_DERIVED = REPL_ROOT / "data" / "seattle" / "derived"
OUT_PREP = REPL_ROOT / "prepare_from_data_work_square_v2"
OUT_INPUT = REPL_ROOT / "aa_input_square_v2"

# Paper's congestion regression coefficient (Seattle, Table A.X):
#   log(tau_obs / tau_ff) = delta1 * log(AADT / lanes)
# Inverted:
#   AADT = lanes * (tau_obs / tau_ff) ^ (1 / delta1)
DELTA1 = 0.488
EXPONENT = 1.0 / DELTA1          # ≈ 2.049
LANES_FALLBACK = 2.0              # absolute fallback when no lane data available


def get_active_grid_ids() -> set[str]:
    nodes = pd.read_parquet(SOURCE_DIR / "qsm_input_nodes_square.parquet").copy()
    nodes = nodes[nodes["keep_for_model"] == 1].copy()
    nodes["grid_id"] = nodes["grid_id"].astype(str)
    nodes["residents"] = pd.to_numeric(nodes["residents"], errors="coerce").fillna(0.0)
    nodes["jobs"] = pd.to_numeric(nodes["jobs"], errors="coerce").fillna(0.0)
    active = set(nodes.loc[(nodes["residents"] > 0) | (nodes["jobs"] > 0), "grid_id"].dropna().astype(str).tolist())

    od = pd.read_parquet(SOURCE_DIR / "qsm_input_od_square.parquet").copy()
    od = od[od["sample_definition"] == "reachable_AM"].copy()
    od["commuters_road"] = pd.to_numeric(od["commuters_road"], errors="coerce").fillna(0.0)
    od["home_grid"] = od["home_grid"].astype(str)
    od["work_grid"] = od["work_grid"].astype(str)
    od = od[(od["commuters_road"] > 0) & od["home_grid"].notna() & od["work_grid"].notna()].copy()
    active.update(od["home_grid"].astype(str).tolist())
    active.update(od["work_grid"].astype(str).tolist())

    edges = pd.read_csv(EDGEPROJ_CSV).copy()
    edges["home_grid"] = edges["home_grid"].astype(str)
    edges["work_grid"] = edges["work_grid"].astype(str)
    tau_obs = pd.to_numeric(edges["tau_obs_min"], errors="coerce")
    tau_ff = pd.to_numeric(edges["tau_ff_min"], errors="coerce")
    edges = edges[
        edges["home_grid"].notna()
        & edges["work_grid"].notna()
        & np.isfinite(tau_obs)
        & np.isfinite(tau_ff)
        & (tau_obs > 0)
        & (tau_ff > 0)
    ].copy()
    active.update(edges["home_grid"].astype(str).tolist())
    active.update(edges["work_grid"].astype(str).tolist())
    return active


def build_node_file(active_grid_ids: set[str]) -> tuple[pd.DataFrame, dict[int, int], dict[str, int]]:
    nodes = pd.read_parquet(SOURCE_DIR / "qsm_input_nodes_square.parquet").copy()
    nodes = nodes[nodes["keep_for_model"] == 1].copy()
    nodes["grid_id"] = nodes["grid_id"].astype(str)
    nodes["node_i"] = pd.to_numeric(nodes["node_i"], errors="coerce")
    nodes = nodes[nodes["node_i"].notna()].copy()
    nodes["node_i"] = nodes["node_i"].astype(int)
    nodes = nodes[nodes["grid_id"].isin(active_grid_ids)].copy()
    nodes = nodes.sort_values("node_i").reset_index(drop=True)

    old_to_new = {int(old): int(new) for new, old in enumerate(nodes["node_i"].astype(int).tolist(), start=1)}
    grid_to_new = {str(grid): int(new) for new, grid in enumerate(nodes["grid_id"].astype(str).tolist(), start=1)}

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

    return node_out, old_to_new, grid_to_new


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


def build_edge_file(grid_to_new: dict[str, int]) -> pd.DataFrame:
    """
    Build the sparse adjacency / traffic matrix for AA counterfactual input.

    Traffic inversion formula (from paper's congestion regression):
        log(tau_obs / tau_ff) = delta1 * log(AADT / lanes)
        => AADT = lanes * (tau_obs / tau_ff) ^ (1 / delta1)

    Edge travel costs, free-flow costs, and lanes are all read from the
    raw_connected_v1 edge-projection path output so the BPR inversion uses one
    internally consistent grid-to-grid path definition.
    """
    # ---- load edges ----
    edges = pd.read_csv(EDGEPROJ_CSV).copy()
    edges["grid_o_str"] = edges["home_grid"].astype(str)
    edges["grid_d_str"] = edges["work_grid"].astype(str)
    edges = edges[edges["grid_o_str"].isin(grid_to_new) & edges["grid_d_str"].isin(grid_to_new)].copy()

    # ---- resolve lane estimate with priority fallback ----
    lanes_lw = pd.to_numeric(edges["lane_est_path_raw_edgeproj_len_weighted"], errors="coerce")
    lanes_bn = pd.to_numeric(edges["lane_est_path_raw_edgeproj_bottleneck"], errors="coerce")
    lanes = (
        lanes_lw
        .where(lanes_lw.notna() & (lanes_lw > 0))
        .fillna(lanes_bn.where(lanes_bn.notna() & (lanes_bn > 0)))
        .fillna(LANES_FALLBACK)
    )

    # ---- travel times ----
    tau_obs = pd.to_numeric(edges["tau_obs_min"], errors="coerce")
    tau_ff  = pd.to_numeric(edges["tau_ff_min"],  errors="coerce")
    length  = pd.to_numeric(edges["route_length_m"], errors="coerce")
    valid = np.isfinite(tau_obs) & np.isfinite(tau_ff) & (tau_obs > 0) & (tau_ff > 0)
    edges = edges.loc[valid].copy()
    lanes = lanes.loc[valid]
    lanes_lw = lanes_lw.loc[valid]
    lanes_bn = lanes_bn.loc[valid]
    tau_obs = tau_obs.loc[valid]
    tau_ff = tau_ff.loc[valid]
    length = length.loc[valid]

    tau_ff_safe = tau_ff.where((tau_ff > 0) & np.isfinite(tau_ff), np.nan)

    # congestion ratio ≥ 1 (free-flow = 1, congested > 1)
    congestion_ratio = (tau_obs / tau_ff_safe).replace([np.inf, -np.inf], np.nan).fillna(1.0)
    congestion_ratio = congestion_ratio.clip(lower=1.0)

    # ---- paper formula: AADT = lanes * (tau_obs/tau_ff)^(1/delta1) ----
    # Each row is one directional edge (AM period), so asymmetry is natural.
    traffic = lanes * (congestion_ratio ** EXPONENT)

    # Diagnostics
    lane_source = (
        lanes_lw.notna() & (lanes_lw > 0)
    )
    n_lw   = int(lane_source.sum())
    n_bn   = int((~lane_source & lanes_bn.notna() & (lanes_bn > 0)).sum())
    n_fb   = int(len(edges) - n_lw - n_bn)
    print(f"  Lane source: raw_edgeproj_len_weighted={n_lw}, raw_edgeproj_bottleneck={n_bn}, fallback(2.0)={n_fb}")
    print(f"  Traffic stats: min={traffic.min():.4f}, mean={traffic.mean():.4f}, max={traffic.max():.4f}")

    edge_out = pd.DataFrame(
        {
            0: edges["grid_o_str"].map(grid_to_new).astype(int),
            1: edges["grid_d_str"].map(grid_to_new).astype(int),
            2: traffic.astype(float),
            3: length.fillna(0.0).astype(float),
            4: tau_obs.fillna(0.0).astype(float),
        }
    )
    return edge_out


def write_summary(node_out: pd.DataFrame, od_out: pd.DataFrame, edge_out: pd.DataFrame) -> None:
    summary = {
        "source_version": "manual_centerline_rules_v11",
        "edge_cost_source_version": "raw_connected_v1_grid_travel_time_edgeproj_v2",
        "lane_source_version": "raw_connected_v1_grid_travel_time_edgeproj_v2",
        "grid_type": "square",
        "traffic_formula": "lanes * (tau_obs/tau_ff)^(1/delta1)",
        "tau_obs_source": "raw_connected_travel_time_min from edgeproj_v2",
        "tau_ff_source": "raw_connected_tau_ff_min from the same edgeproj_v2 path using 22:00-05:00 speed",
        "delta1": DELTA1,
        "exponent_1_over_delta1": EXPONENT,
        "lanes_fallback": LANES_FALLBACK,
        "lane_priority": [
            "lane_est_path_raw_edgeproj_len_weighted",
            "lane_est_path_raw_edgeproj_bottleneck",
            str(LANES_FALLBACK),
        ],
        "asymmetry": "preserved — each directional AM edge computed independently",
        "node_rows": int(len(node_out)),
        "edge_rows": int(len(edge_out)),
        "od_rows": int(len(od_out)),
        "traffic_min": float(edge_out[2].min()) if len(edge_out) else None,
        "traffic_mean": float(edge_out[2].mean()) if len(edge_out) else None,
        "traffic_max": float(edge_out[2].max()) if len(edge_out) else None,
        "filter_rule": "keep keep_for_model grids that are active in at least one of: positive residents/jobs, positive reachable_AM commuters_road, or AM grid-edge incidence.",
        "note": "Coordinates in node_lr_lf_seattle.csv are placeholder zeros; v2 uses paper's congestion inversion formula for traffic.",
    }
    with open(OUT_PREP / "build_summary_square.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)


def main() -> None:
    OUT_COUNTER.mkdir(parents=True, exist_ok=True)
    OUT_DERIVED.mkdir(parents=True, exist_ok=True)
    OUT_PREP.mkdir(parents=True, exist_ok=True)
    OUT_INPUT.mkdir(parents=True, exist_ok=True)

    active_grid_ids = get_active_grid_ids()
    node_out, old_to_new, grid_to_new = build_node_file(active_grid_ids)
    od_out = build_od_file(old_to_new)
    edge_out = build_edge_file(grid_to_new)

    node_path = OUT_INPUT / "node_lr_lf_seattle.csv"
    od_path   = OUT_INPUT / "sparse_commute_seattle.csv"
    edge_path = OUT_INPUT / "sparse_adjmat_seattle.csv"

    node_out.to_csv(node_path, header=False, index=False)
    od_out.to_csv(od_path,     header=False, index=False)
    edge_out.to_csv(edge_path, header=False, index=False)

    shutil.copy2(node_path, OUT_COUNTER / node_path.name)
    shutil.copy2(od_path,   OUT_COUNTER / od_path.name)
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
