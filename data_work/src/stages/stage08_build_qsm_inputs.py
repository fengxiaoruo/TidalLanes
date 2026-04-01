"""
Stage 08: Build QSM Inputs

Purpose:
- Export QSM-ready nodes, edges, OD, and parameter inputs
- Make sample restrictions and definition versions explicit
"""

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd


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


def save_config_snapshot(version_root: Path, config_path: str | None, grid_type: str):
    payload = {
        "stage": "stage08_build_qsm_inputs",
        "config_path": config_path,
        "grid_type": grid_type,
        "travel_time_definition": "grid_centroid_distance_over_v_harm_min",
        "free_flow_definition": "grid_centroid_distance_over_v_ff_harm_min",
        "sample_definition": "reachable_AM",
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


def run_for_grid(version_root: Path, grid_type: str):
    data_dir = version_root / "data"
    metrics_dir = version_root / "metrics"
    nodes = pd.read_csv(data_dir / f"grid_nodes_{grid_type}.csv")
    edges = pd.read_csv(data_dir / f"t_edges_{grid_type}_AM.csv")
    od = pd.read_csv(data_dir / f"OD_{grid_type}_reachable_AM.csv")
    pop = pd.read_csv(data_dir / f"grid_population_summary_{grid_type}.csv")
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

    edges_qsm = edges.copy()
    edges_qsm["grid_type"] = grid_type
    edges_qsm["version_id"] = version_root.name
    edges_qsm["travel_time_definition"] = "grid_centroid_distance_over_v_harm_min"
    edges_qsm["free_flow_definition"] = "grid_centroid_distance_over_v_ff_harm_min"

    od_qsm = od.copy()
    od_qsm["grid_type"] = grid_type
    od_qsm["version_id"] = version_root.name
    od_qsm["sample_definition"] = "reachable_AM"

    lane_summary = {
        "lane_mean": float(pd.to_numeric(lanes["lane_est_length_weighted"], errors="coerce").mean()),
        "lane_rows": int(len(lanes)),
    }

    nodes_qsm.to_parquet(data_dir / f"qsm_input_nodes_{grid_type}.parquet", index=False)
    edges_qsm.to_parquet(data_dir / f"qsm_input_edges_{grid_type}.parquet", index=False)
    od_qsm.to_parquet(data_dir / f"qsm_input_od_{grid_type}.parquet", index=False)
    (data_dir / f"qsm_input_parameters_{grid_type}.json").write_text(
        json.dumps(
            {
                "version_id": version_root.name,
                "grid_type": grid_type,
                "travel_time_definition": "grid_centroid_distance_over_v_harm_min",
                "free_flow_definition": "grid_centroid_distance_over_v_ff_harm_min",
                "sample_definition": "reachable_AM",
                "lane_summary": lane_summary,
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
            }
        ]
    ).to_csv(metrics_dir / f"stage08_{grid_type}_summary.csv", index=False)


def run(config_path: str | None, version_id: str, output_dir: str, grid_type: str):
    version_root = Path(output_dir) / version_id
    (version_root / "data").mkdir(parents=True, exist_ok=True)
    (version_root / "metrics").mkdir(parents=True, exist_ok=True)
    save_config_snapshot(version_root, config_path, grid_type)
    grid_types = ["square", "hex", "voronoi"] if grid_type == "all" else [grid_type]
    for gt in grid_types:
        run_for_grid(version_root, gt)
        print(f"[stage08] completed grid_type={gt}")


def main():
    args = parse_args()
    run(args.config, args.version_id, args.output_dir, args.grid_type)


if __name__ == "__main__":
    main()
