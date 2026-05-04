"""
Stage 08: Build QSM Inputs

Purpose:
- Export QSM-ready nodes, edges, OD, and parameter inputs
- Make sample restrictions and definition versions explicit
"""

import argparse
import json
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

TAU_OBS_DEFINITION = "grid_centroid_distance_over_v_harm_min"
TAU_FF_DEFINITION = "grid_centroid_distance_over_v_ff_harm_min"
ICEBERG_TRANSFORM_DEFINITION = "not_applied_stage08_minutes_only"
SAMPLE_DEFINITION = "reachable_AM"
ROAD_MODE_RULE = "car_only_baseline_with_car_bus_alternative_saved"
USE_POP_AS_COMMUTERS_ROAD = False


def parse_args():
    parser = argparse.ArgumentParser(description="Stage 08: Build QSM inputs")
    parser.add_argument("--config", default=None, help="Optional config file path.")
    parser.add_argument("--version-id", required=True, help="Version identifier for outputs.")
    parser.add_argument("--output-dir", default="outputs", help="Base output directory for versioned results.")
    parser.add_argument(
        "--grid-type",
        default="all",
        choices=["all", "square", "hex", "voronoi"],
        help="Grid system to process.",
    )
    return parser.parse_args()


def load_stage08_runtime(config_path: str | None) -> dict:
    runtime = {"use_pop_as_commuters_road": False}
    if not config_path:
        return runtime
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Stage08 config not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    stage_payload = payload.get("stage08", payload)
    if "use_pop_as_commuters_road" in stage_payload:
        runtime["use_pop_as_commuters_road"] = bool(stage_payload["use_pop_as_commuters_road"])
    return runtime


def configure_stage08_runtime(config_path: str | None) -> dict:
    global USE_POP_AS_COMMUTERS_ROAD, ROAD_MODE_RULE
    runtime = load_stage08_runtime(config_path)
    USE_POP_AS_COMMUTERS_ROAD = bool(runtime["use_pop_as_commuters_road"])
    if USE_POP_AS_COMMUTERS_ROAD:
        ROAD_MODE_RULE = "pop_total_as_road_commuters_type_disabled"
    else:
        ROAD_MODE_RULE = "car_only_baseline_with_car_bus_alternative_saved"
    return runtime


def save_config_snapshot(version_root: Path, config_path: str | None, grid_type: str):
    payload = {
        "stage": "stage08_build_qsm_inputs",
        "config_path": config_path,
        "grid_type": grid_type,
        "travel_time_definition": TAU_OBS_DEFINITION,
        "free_flow_definition": TAU_FF_DEFINITION,
        "tau_obs_definition": TAU_OBS_DEFINITION,
        "tau_ff_definition": TAU_FF_DEFINITION,
        "iceberg_transform_definition": ICEBERG_TRANSFORM_DEFINITION,
        "sample_definition": SAMPLE_DEFINITION,
        "road_mode_rule": ROAD_MODE_RULE,
        "use_pop_as_commuters_road": bool(USE_POP_AS_COMMUTERS_ROAD),
    }
    (version_root / "config_snapshot.stage08.json").write_text(
        json.dumps(payload, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )


def normalize_grid_id_value(value):
    if pd.isna(value):
        return pd.NA
    if isinstance(value, (int, np.integer)):
        return str(int(value))
    if isinstance(value, (float, np.floating)):
        if not np.isfinite(value):
            return pd.NA
        return str(int(value)) if float(value).is_integer() else format(float(value), ".15g")
    text = str(value).strip()
    if text == "" or text.lower() in {"nan", "none", "<na>"}:
        return pd.NA
    try:
        num = float(text)
    except ValueError:
        return text
    if not np.isfinite(num):
        return pd.NA
    return str(int(num)) if num.is_integer() else format(num, ".15g")


def normalize_grid_id_series(series: pd.Series) -> pd.Series:
    return series.map(normalize_grid_id_value).astype("string")


def build_edge_lane_summary(links_long: pd.DataFrame, lanes: pd.DataFrame) -> tuple[pd.DataFrame, float]:
    lane_ref = lanes[["cline_id", "dir", "lane_est_length_weighted"]].copy()
    lane_ref["lane_est_length_weighted"] = pd.to_numeric(lane_ref["lane_est_length_weighted"], errors="coerce")
    lane_mean = float(lane_ref["lane_est_length_weighted"].dropna().mean())
    if not np.isfinite(lane_mean) or lane_mean <= 0:
        lane_mean = 2.0

    long_use = links_long[["grid_o", "grid_d", "cline_id", "dir", "len_m"]].copy()
    long_use["grid_o"] = normalize_grid_id_series(long_use["grid_o"])
    long_use["grid_d"] = normalize_grid_id_series(long_use["grid_d"])
    long_use["len_m"] = pd.to_numeric(long_use["len_m"], errors="coerce").fillna(0.0)
    long_use = long_use[(long_use["grid_o"].notna()) & (long_use["grid_d"].notna()) & (long_use["len_m"] > 0)].copy()
    long_use = long_use.merge(lane_ref, on=["cline_id", "dir"], how="left")
    long_use["has_lane_obs"] = long_use["lane_est_length_weighted"].notna()
    long_use["lane_support_len_m"] = np.where(long_use["has_lane_obs"], long_use["len_m"], 0.0)
    long_use["lane_weighted_sum"] = np.where(
        long_use["has_lane_obs"],
        long_use["len_m"] * long_use["lane_est_length_weighted"],
        0.0,
    )
    long_use["parts_with_lane"] = long_use["has_lane_obs"].astype(int)

    edge_lane = long_use.groupby(["grid_o", "grid_d"], as_index=False).agg(
        total_link_len_m=("len_m", "sum"),
        lane_support_len_m=("lane_support_len_m", "sum"),
        lane_weighted_sum=("lane_weighted_sum", "sum"),
        link_parts=("len_m", "size"),
        link_parts_with_lane=("parts_with_lane", "sum"),
    )
    edge_lane["lane_est_length_weighted"] = np.where(
        edge_lane["lane_support_len_m"] > 0,
        edge_lane["lane_weighted_sum"] / edge_lane["lane_support_len_m"],
        np.nan,
    )
    edge_lane["lane_missing_length_share"] = np.where(
        edge_lane["total_link_len_m"] > 0,
        1.0 - edge_lane["lane_support_len_m"] / edge_lane["total_link_len_m"],
        np.nan,
    )
    edge_lane["lane_missing_part_share"] = np.where(
        edge_lane["link_parts"] > 0,
        1.0 - edge_lane["link_parts_with_lane"] / edge_lane["link_parts"],
        np.nan,
    )
    edge_lane["lane_quality_flag"] = np.where(
        edge_lane["lane_support_len_m"] <= 0,
        "missing",
        np.where(edge_lane["lane_missing_length_share"] > 0, "partial", "ok"),
    )
    edge_lane["lanes_directional"] = edge_lane["lane_est_length_weighted"].fillna(lane_mean)
    edge_lane["capacity_proxy"] = edge_lane["lanes_directional"]
    return edge_lane, lane_mean


def build_data_quality(
    version_root: Path,
    grid_type: str,
    nodes_qsm: pd.DataFrame,
    edges_qsm: pd.DataFrame,
    od_qsm: pd.DataFrame,
    od_all: pd.DataFrame | None,
) -> pd.DataFrame:
    total_edge_len_m = float(pd.to_numeric(edges_qsm.get("total_link_len_m"), errors="coerce").fillna(0.0).sum())
    lane_missing_mask = edges_qsm["lane_quality_flag"].eq("missing")
    lane_partial_mask = edges_qsm["lane_quality_flag"].eq("partial")
    edge_len = pd.to_numeric(edges_qsm.get("total_link_len_m"), errors="coerce").fillna(0.0)
    components = pd.to_numeric(nodes_qsm.get("component"), errors="coerce")
    largest_component_node_share = float(components.value_counts(normalize=True, dropna=True).max()) if components.notna().any() else np.nan

    if od_all is not None and len(od_all) > 0:
        reachable_od_pair_share = float(len(od_qsm) / len(od_all))
        reachable_od_commuters_share = float(
            pd.to_numeric(od_qsm["commuters_total"], errors="coerce").fillna(0.0).sum()
            / pd.to_numeric(od_all["pop"], errors="coerce").fillna(0.0).sum()
        ) if pd.to_numeric(od_all["pop"], errors="coerce").fillna(0.0).sum() > 0 else np.nan
        node_ids = set(nodes_qsm["grid_id"].astype("string").dropna().tolist())
        od_nodes_not_in_graph_count = int(
            (~normalize_grid_id_series(od_all["home_grid"]).isin(node_ids)).sum()
            + (~normalize_grid_id_series(od_all["work_grid"]).isin(node_ids)).sum()
        )
    else:
        reachable_od_pair_share = 1.0
        reachable_od_commuters_share = 1.0
        od_nodes_not_in_graph_count = 0

    payload = {
        "version_id": version_root.name,
        "grid_type": grid_type,
        "sample_definition": SAMPLE_DEFINITION,
        "road_mode_rule": ROAD_MODE_RULE,
        "n_nodes": int(len(nodes_qsm)),
        "n_edges": int(len(edges_qsm)),
        "n_od_pairs": int(len(od_qsm)),
        "od_total_commuters": float(pd.to_numeric(od_qsm["commuters_total"], errors="coerce").fillna(0.0).sum()),
        "od_total_road_commuters": float(pd.to_numeric(od_qsm["commuters_road"], errors="coerce").fillna(0.0).sum()),
        "edge_missing_tau_obs_share": float((~np.isfinite(edges_qsm["tau_obs_min"]) | (edges_qsm["tau_obs_min"] <= 0)).mean()),
        "edge_missing_tau_ff_share": float(pd.to_numeric(edges_qsm["tau_ff_missing_original"], errors="coerce").fillna(0).mean()),
        "edge_tau_ff_imputed_share": float(pd.to_numeric(edges_qsm["tau_ff_imputed_flag"], errors="coerce").fillna(0).mean()),
        "edge_tau_obs_lt_tau_ff_share": float((edges_qsm["tau_obs_min"] < edges_qsm["tau_ff_min"]).mean()),
        "edge_missing_lane_share_count": float(lane_missing_mask.mean()),
        "edge_missing_lane_share_length": float(edge_len[lane_missing_mask].sum() / total_edge_len_m) if total_edge_len_m > 0 else np.nan,
        "edge_partial_lane_share_count": float(lane_partial_mask.mean()),
        "edge_partial_lane_share_length": float(edge_len[lane_partial_mask].sum() / total_edge_len_m) if total_edge_len_m > 0 else np.nan,
        "od_nodes_not_in_graph_count": od_nodes_not_in_graph_count,
        "largest_component_node_share": largest_component_node_share,
        "reachable_od_pair_share": reachable_od_pair_share,
        "reachable_od_commuters_share": reachable_od_commuters_share,
    }
    return pd.DataFrame([payload])


def run_for_grid(version_root: Path, grid_type: str):
    data_dir = version_root / "data"
    metrics_dir = version_root / "metrics"
    nodes = pd.read_csv(data_dir / f"grid_nodes_{grid_type}.csv")
    edges = pd.read_csv(data_dir / f"t_edges_{grid_type}_AM.csv")
    od = pd.read_csv(data_dir / f"OD_{grid_type}_reachable_AM.csv")
    od_all_path = data_dir / f"OD_{grid_type}.csv"
    od_all = pd.read_csv(od_all_path) if od_all_path.exists() else None
    pop = pd.read_csv(data_dir / f"grid_population_summary_{grid_type}.csv")
    links_long = pd.read_csv(data_dir / f"grid_links_{grid_type}_long.csv")
    lanes = pd.read_parquet(data_dir / "centerline_lane_master.parquet")

    nodes["grid_id"] = normalize_grid_id_series(nodes["grid_id"])
    pop["grid_id"] = normalize_grid_id_series(pop["grid_id"])
    edges["grid_o"] = normalize_grid_id_series(edges["grid_o"])
    edges["grid_d"] = normalize_grid_id_series(edges["grid_d"])
    od["home_grid"] = normalize_grid_id_series(od["home_grid"])
    od["work_grid"] = normalize_grid_id_series(od["work_grid"])

    nodes_qsm = nodes.merge(pop, on="grid_id", how="left")
    nodes_qsm["grid_type"] = grid_type
    nodes_qsm["version_id"] = version_root.name
    nodes_qsm["node_source"] = "stage07_build_od_and_population"
    nodes_qsm["keep_for_model"] = 1
    nodes_qsm["drop_reason"] = pd.NA

    edges_qsm = edges.copy()
    edges_qsm["tau_obs_min"] = pd.to_numeric(edges_qsm["t_min"], errors="coerce")
    edges_qsm["tau_ff_min"] = pd.to_numeric(edges_qsm.get("t_ff_min"), errors="coerce")
    tau_ff_missing = ~np.isfinite(edges_qsm["tau_ff_min"]) | (edges_qsm["tau_ff_min"] <= 0)
    edges_qsm["tau_ff_missing_original"] = tau_ff_missing.astype(int)
    edges_qsm["tau_ff_imputed_flag"] = tau_ff_missing.astype(int)
    if bool(tau_ff_missing.any()):
        warnings.warn(
            f"[stage08] {version_root.name} {grid_type}: imputing tau_ff_min from tau_obs_min for {int(tau_ff_missing.sum())} edges",
            stacklevel=2,
        )
    edges_qsm.loc[tau_ff_missing, "tau_ff_min"] = edges_qsm.loc[tau_ff_missing, "tau_obs_min"]
    edges_qsm["t_min"] = edges_qsm["tau_obs_min"]
    edges_qsm["t_ff_min"] = edges_qsm["tau_ff_min"]
    edges_qsm["t_obs_iceberg"] = np.nan
    edges_qsm["t_ff_iceberg"] = np.nan
    edges_qsm["tau_consistency_flag"] = np.where(
        edges_qsm["tau_obs_min"] >= edges_qsm["tau_ff_min"],
        "ok",
        "tau_obs_lt_tau_ff",
    )
    edge_lane, lane_mean = build_edge_lane_summary(links_long[links_long["period"] == "AM"].copy(), lanes)
    edges_qsm = edges_qsm.merge(edge_lane, on=["grid_o", "grid_d"], how="left")
    edges_qsm["lane_quality_flag"] = edges_qsm["lane_quality_flag"].fillna("missing")
    edges_qsm["lane_est_length_weighted"] = pd.to_numeric(edges_qsm["lane_est_length_weighted"], errors="coerce")
    edges_qsm["lanes_directional"] = pd.to_numeric(edges_qsm["lanes_directional"], errors="coerce").fillna(lane_mean)
    edges_qsm["capacity_proxy"] = pd.to_numeric(edges_qsm["capacity_proxy"], errors="coerce").fillna(edges_qsm["lanes_directional"])
    edges_qsm["grid_type"] = grid_type
    edges_qsm["version_id"] = version_root.name
    edges_qsm["period"] = "AM"
    edges_qsm["travel_time_definition"] = TAU_OBS_DEFINITION
    edges_qsm["free_flow_definition"] = TAU_FF_DEFINITION
    edges_qsm["tau_obs_definition"] = TAU_OBS_DEFINITION
    edges_qsm["tau_ff_definition"] = TAU_FF_DEFINITION
    edges_qsm["iceberg_transform_definition"] = ICEBERG_TRANSFORM_DEFINITION
    edges_qsm["tau_obs_source"] = "stage06_t_edges_AM"
    edges_qsm["tau_ff_source"] = np.where(
        edges_qsm["tau_ff_imputed_flag"] == 1,
        "imputed_from_tau_obs_min",
        "stage06_t_edges_AM",
    )

    od_qsm = od.copy()
    for col in ["pop", "type_bus", "type_car"]:
        if col in od_qsm.columns:
            od_qsm[col] = pd.to_numeric(od_qsm[col], errors="coerce").fillna(0.0)
    od_qsm["origin_grid"] = od_qsm["home_grid"]
    od_qsm["destination_grid"] = od_qsm["work_grid"]
    od_qsm["origin_node"] = pd.to_numeric(od_qsm["home_i"], errors="coerce").astype("Int64")
    od_qsm["destination_node"] = pd.to_numeric(od_qsm["work_i"], errors="coerce").astype("Int64")
    od_qsm["commuters_total"] = pd.to_numeric(od_qsm["pop"], errors="coerce").fillna(0.0)
    if USE_POP_AS_COMMUTERS_ROAD:
        od_qsm["commuters_car"] = 0.0
        od_qsm["commuters_car_bus"] = 0.0
        od_qsm["commuters_road"] = od_qsm["commuters_total"]
    else:
        od_qsm["commuters_car"] = pd.to_numeric(od_qsm.get("type_car"), errors="coerce").fillna(0.0)
        od_qsm["commuters_car_bus"] = (
            pd.to_numeric(od_qsm.get("type_car"), errors="coerce").fillna(0.0)
            + pd.to_numeric(od_qsm.get("type_bus"), errors="coerce").fillna(0.0)
        )
        od_qsm["commuters_road"] = od_qsm["commuters_car"]
    od_qsm["grid_type"] = grid_type
    od_qsm["version_id"] = version_root.name
    od_qsm["sample_definition"] = SAMPLE_DEFINITION
    od_qsm["road_mode_rule"] = ROAD_MODE_RULE

    lane_summary = {
        "lane_mean": lane_mean,
        "lane_rows": int(len(lanes)),
    }
    data_quality = build_data_quality(version_root, grid_type, nodes_qsm, edges_qsm, od_qsm, od_all)

    nodes_qsm.to_parquet(data_dir / f"qsm_input_nodes_{grid_type}.parquet", index=False)
    edges_qsm.to_parquet(data_dir / f"qsm_input_edges_{grid_type}.parquet", index=False)
    od_qsm.to_parquet(data_dir / f"qsm_input_od_{grid_type}.parquet", index=False)
    data_quality.to_csv(data_dir / f"data_quality_{grid_type}_qsm.csv", index=False)
    (data_dir / f"qsm_input_parameters_{grid_type}.json").write_text(
        json.dumps(
            {
                "version_id": version_root.name,
                "grid_type": grid_type,
                "travel_time_definition": TAU_OBS_DEFINITION,
                "free_flow_definition": TAU_FF_DEFINITION,
                "tau_obs_definition": TAU_OBS_DEFINITION,
                "tau_ff_definition": TAU_FF_DEFINITION,
                "iceberg_transform_definition": ICEBERG_TRANSFORM_DEFINITION,
                "sample_definition": SAMPLE_DEFINITION,
                "road_mode_rule": ROAD_MODE_RULE,
                "lane_summary": lane_summary,
                "data_quality_file": f"data_quality_{grid_type}_qsm.csv",
            },
            ensure_ascii=True,
            indent=2,
        ),
        encoding="utf-8",
    )

    pd.DataFrame(
        [
            {
                "grid_type": grid_type,
                "qsm_nodes": len(nodes_qsm),
                "qsm_edges": len(edges_qsm),
                "qsm_od_pairs": len(od_qsm),
                "tau_ff_imputed_edges": int(edges_qsm["tau_ff_imputed_flag"].sum()),
                "missing_lane_edges": int(edges_qsm["lane_quality_flag"].eq("missing").sum()),
            }
        ]
    ).to_csv(metrics_dir / f"stage08_{grid_type}_summary.csv", index=False)


def run(config_path: str | None, version_id: str, output_dir: str, grid_type: str):
    version_root = Path(output_dir) / version_id
    (version_root / "data").mkdir(parents=True, exist_ok=True)
    (version_root / "metrics").mkdir(parents=True, exist_ok=True)
    runtime = configure_stage08_runtime(config_path)
    save_config_snapshot(version_root, config_path, grid_type)
    print(f"[stage08] runtime={runtime}")
    grid_types = ["square", "hex", "voronoi"] if grid_type == "all" else [grid_type]
    for gt in grid_types:
        run_for_grid(version_root, gt)
        print(f"[stage08] completed grid_type={gt}")


def main():
    args = parse_args()
    run(args.config, args.version_id, args.output_dir, args.grid_type)


if __name__ == "__main__":
    main()
