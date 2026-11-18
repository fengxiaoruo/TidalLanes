"""Apply car-ratio OD scaling to an existing locked QSM bundle (square grid).

Creates a new version_id without overwriting the source bundle.
"""
from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[3]
DEFAULT_SOURCE = REPO / "outputs" / "model_input_edgeproj_v2_lcc_gate5_v1"


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build car-ratio scaled QSM bundle from an existing gate5 bundle")
    p.add_argument("--source-version-id", default="model_input_edgeproj_v2_lcc_gate5_v1")
    p.add_argument("--output-version-id", default="model_input_edgeproj_v2_lcc_gate5_car_ratio_v1")
    p.add_argument("--output-dir", default=str(REPO / "outputs"))
    p.add_argument("--grid-type", default="square", choices=["square", "hex", "voronoi"])
    p.add_argument("--shrink-prior-n", type=float, default=30.0)
    return p.parse_args()


def apply_scaling(
    source_root: Path,
    out_root: Path,
    *,
    grid_type: str,
    shrink_prior_n: float,
) -> dict:
    from src.stages.mode_share_scaling import (
        ModeShareScalingConfig,
        apply_car_mode_share_scaling,
        scaled_node_population,
    )

    out_root.mkdir(parents=True, exist_ok=True)
    src_data = source_root / "data"
    out_data = out_root / "data"
    out_data.mkdir(parents=True, exist_ok=True)
    (out_root / "metrics").mkdir(parents=True, exist_ok=True)

    od = pd.read_parquet(src_data / f"qsm_input_od_{grid_type}.parquet")
    od_scaled, scaling_summary = apply_car_mode_share_scaling(
        od,
        config=ModeShareScalingConfig(shrink_prior_n=shrink_prior_n),
    )

    od_scaled["commuters_road"] = od_scaled["commuters_road_scaled"]
    od_scaled["commuters_car"] = od_scaled["commuters_car_scaled"]
    od_scaled["commuters_car_bus"] = od_scaled["commuters_car_bus_scaled"]
    od_scaled["road_mode_rule"] = "car_ratio_scaled_hierarchical_fallback"
    od_scaled["version_id"] = out_root.name

    nodes = pd.read_parquet(src_data / f"qsm_input_nodes_{grid_type}.parquet")
    pop_scaled = scaled_node_population(od_scaled)
    nodes = nodes.drop(columns=[c for c in ["residents", "jobs", "job_resident_ratio"] if c in nodes.columns])
    nodes = nodes.merge(pop_scaled, on="grid_id", how="left")
    nodes["residents"] = pd.to_numeric(nodes["residents"], errors="coerce").fillna(0.0)
    nodes["jobs"] = pd.to_numeric(nodes["jobs"], errors="coerce").fillna(0.0)
    nodes["version_id"] = out_root.name

    edges = pd.read_parquet(src_data / f"qsm_input_edges_{grid_type}.parquet")
    edges["version_id"] = out_root.name

    od_scaled.to_parquet(out_data / f"qsm_input_od_{grid_type}.parquet", index=False)
    nodes.to_parquet(out_data / f"qsm_input_nodes_{grid_type}.parquet", index=False)
    edges.to_parquet(out_data / f"qsm_input_edges_{grid_type}.parquet", index=False)

    for fname in [
        f"data_quality_{grid_type}_qsm.csv",
        f"qsm_input_parameters_{grid_type}.json",
    ]:
        src = src_data / fname
        if src.exists():
            shutil.copy2(src, out_data / fname)

    scaling_summary.to_csv(out_data / f"mode_share_scaling_summary_{grid_type}.csv", index=False)
    od_scaled[
        [
            "home_grid",
            "work_grid",
            "pop",
            "identified",
            "type_car",
            "car_share",
            "mode_share_source",
            "commuters_road_scaled",
        ]
    ].to_csv(out_data / f"mode_share_scaling_od_audit_{grid_type}.csv", index=False)

    src_summary_path = source_root / "metrics" / "travel_cost_bundle_summary.json"
    summary = json.loads(src_summary_path.read_text(encoding="utf-8")) if src_summary_path.exists() else {}
    summary.update(
        {
            "output_version_id": out_root.name,
            "source_version_id": source_root.name,
            "road_mode_rule": "car_ratio_scaled_hierarchical_fallback",
            "od_road_commuters_direct": float(pd.to_numeric(od["commuters_road"], errors="coerce").fillna(0).sum()),
            "od_road_commuters_scaled": float(od_scaled["commuters_road_scaled"].sum()),
            "od_road_scaling_factor": float(
                od_scaled["commuters_road_scaled"].sum()
                / max(pd.to_numeric(od["commuters_road"], errors="coerce").fillna(0).sum(), 1e-12)
            ),
            "residents_scaled_sum": float(nodes["residents"].sum()),
            "jobs_scaled_sum": float(nodes["jobs"].sum()),
            "shrink_prior_n": shrink_prior_n,
        }
    )
    (out_root / "metrics" / "travel_cost_bundle_summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    (out_root / "config_snapshot.car_ratio_scaling.json").write_text(
        json.dumps(
            {
                "source_version_id": source_root.name,
                "output_version_id": out_root.name,
                "grid_type": grid_type,
                "road_mode_rule": "car_ratio_scaled_hierarchical_fallback",
                "shrink_prior_n": shrink_prior_n,
                "fallback_order": [
                    "od_pair",
                    "origin_grid",
                    "work_grid",
                    "distance_core_bin",
                    "citywide",
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return summary


def main() -> None:
    args = _parse_args()
    source_root = Path(args.output_dir) / args.source_version_id
    out_root = Path(args.output_dir) / args.output_version_id
    if not source_root.is_dir():
        raise FileNotFoundError(f"source bundle not found: {source_root}")
    summary = apply_scaling(
        source_root,
        out_root,
        grid_type=args.grid_type,
        shrink_prior_n=args.shrink_prior_n,
    )
    print(f"[car_ratio_scaling] wrote {out_root}")
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
