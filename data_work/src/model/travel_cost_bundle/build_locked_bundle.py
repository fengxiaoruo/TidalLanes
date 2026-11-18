"""
Build a locked QSM input bundle with explicit travel-cost source and LCC scope.

Usage (from data_work/):
    python -m src.model.travel_cost_bundle.build_locked_bundle \\
        --cost-version edgeproj_v2_lcc \\
        --output-version-id model_input_edgeproj_v2_lcc_v1
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from src.model.travel_cost_bundle.lcc import largest_component_grids

REPO = Path(__file__).resolve().parents[3]
DEFAULT_BASE = REPO / "outputs" / "manual_centerline_rules_v11"
GRID_MASTER = REPO / "outputs" / "manual_centerline_rules_v11" / "data" / "grid_square_master.parquet"
UTM_EPSG = 32650  # Beijing (~116.4E) -> UTM zone 50N
DEFAULT_EDGEPROJ = (
    REPO
    / "outputs"
    / "raw_connected_v1_grid_travel_time_edgeproj_v2"
    / "data"
    / "raw_connected_vs_old_adjacent_adjacent_square_AM.csv"
)

TAU_OBS_CENTERLINE = "grid_centroid_distance_over_v_harm_min"
TAU_OBS_EDGEPROJ = "raw_edgeproj_v2_pair_specific_routed_minutes"
TAU_FF_EDGEPROJ = "raw_edgeproj_v2_pair_specific_ff_minutes"


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build locked QSM bundle with LCC restriction")
    p.add_argument(
        "--cost-version",
        choices=["centerline_lcc", "edgeproj_v2_lcc"],
        required=True,
        help="Travel-cost construction to lock in.",
    )
    p.add_argument("--output-version-id", required=True)
    p.add_argument(
        "--source-label",
        default="raw_connected_v1",
        help="Provenance tag for the raw network source (e.g. raw_connected_v1, raw_connected_v3).",
    )
    p.add_argument("--base-version-root", default=str(DEFAULT_BASE))
    p.add_argument("--edgeproj-csv", default=str(DEFAULT_EDGEPROJ))
    p.add_argument("--grid-type", default="square", choices=["square", "hex", "voronoi"])
    p.add_argument("--period", default="AM")
    p.add_argument(
        "--detour-gate",
        type=float,
        default=None,
        help="Drop adjacent edges whose route_length_m / straight-line centroid "
        "distance exceeds this factor (applied before LCC). Default: no gate.",
    )
    p.add_argument("--output-dir", default=str(REPO / "outputs"))
    return p.parse_args()


def _load_base(base_root: Path, grid_type: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    data = base_root / "data"
    nodes = pd.read_parquet(data / f"qsm_input_nodes_{grid_type}.parquet")
    edges = pd.read_parquet(data / f"qsm_input_edges_{grid_type}.parquet")
    od = pd.read_parquet(data / f"qsm_input_od_{grid_type}.parquet")
    return nodes, edges, od


def _apply_edgeproj_costs(edges: pd.DataFrame, edgeproj_csv: Path) -> pd.DataFrame:
    proj = pd.read_csv(edgeproj_csv)
    proj["home_grid"] = proj["home_grid"].astype(str)
    proj["work_grid"] = proj["work_grid"].astype(str)
    proj = proj.rename(columns={"home_grid": "grid_o", "work_grid": "grid_d"})
    keep = [
        "grid_o",
        "grid_d",
        "tau_obs_min",
        "tau_ff_min",
        "route_length_m",
        "lane_est_path_raw_edgeproj_len_weighted",
        "lane_est_path_raw_edgeproj_bottleneck",
    ]
    optional_keep = [
        "lane_path_support_len_m",
        "lane_path_total_road_len_m",
        "lane_path_support_share",
        "lane_path_all_missing_flag",
        "lane_resolved_for_bpr",
        "lane_resolved_source",
    ]
    keep.extend([c for c in optional_keep if c in proj.columns])
    proj = proj[keep].copy()
    proj["tau_obs_min"] = pd.to_numeric(proj["tau_obs_min"], errors="coerce")
    proj["tau_ff_min"] = pd.to_numeric(proj["tau_ff_min"], errors="coerce")
    proj["route_length_m"] = pd.to_numeric(proj["route_length_m"], errors="coerce")
    for col in [
        "lane_path_support_len_m",
        "lane_path_total_road_len_m",
        "lane_path_support_share",
        "lane_path_all_missing_flag",
        "lane_resolved_for_bpr",
    ]:
        if col in proj.columns:
            proj[col] = pd.to_numeric(proj[col], errors="coerce")
    proj = proj[
        np.isfinite(proj["tau_obs_min"])
        & (proj["tau_obs_min"] > 0)
        & np.isfinite(proj["tau_ff_min"])
        & (proj["tau_ff_min"] > 0)
    ].copy()

    out = edges.copy()
    out["grid_o"] = out["grid_o"].astype(str)
    out["grid_d"] = out["grid_d"].astype(str)
    out = out.merge(proj, on=["grid_o", "grid_d"], how="inner", suffixes=("_old", ""))
    out["total_link_len_m"] = out["route_length_m"].fillna(out["total_link_len_m"])
    lanes_lw = pd.to_numeric(out["lane_est_path_raw_edgeproj_len_weighted"], errors="coerce")
    lanes_bn = pd.to_numeric(out["lane_est_path_raw_edgeproj_bottleneck"], errors="coerce")
    lanes_resolved = (
        pd.to_numeric(out["lane_resolved_for_bpr"], errors="coerce")
        if "lane_resolved_for_bpr" in out.columns
        else pd.Series(np.nan, index=out.index)
    )
    lanes_old = pd.to_numeric(out["lanes_directional"], errors="coerce")
    lane_out = pd.Series(np.nan, index=out.index, dtype=float)
    lane_source = pd.Series("missing", index=out.index, dtype=object)
    for values, label in [
        (lanes_lw, "path_len_weighted"),
        (lanes_bn, "path_bottleneck"),
        (lanes_resolved, "edgeproj_explicit_fallback"),
        (lanes_old, "legacy_bundle_lanes_directional"),
    ]:
        valid = lane_out.isna() & values.notna() & (values > 0)
        lane_out.loc[valid] = values.loc[valid].astype(float)
        lane_source.loc[valid] = label
    out["lanes_directional"] = lane_out
    out["lanes_directional_source"] = lane_source
    out["capacity_proxy"] = out["lanes_directional"]
    out["tau_obs_source"] = TAU_OBS_EDGEPROJ
    out["tau_ff_source"] = TAU_FF_EDGEPROJ
    out["tau_obs_definition"] = TAU_OBS_EDGEPROJ
    out["tau_ff_definition"] = TAU_FF_EDGEPROJ
    out["travel_time_definition"] = TAU_OBS_EDGEPROJ
    out["free_flow_definition"] = TAU_FF_EDGEPROJ
    out["tau_ff_imputed_flag"] = 0
    out["tau_ff_missing_original"] = 0
    out["tau_consistency_flag"] = np.where(
        out["tau_obs_min"] >= out["tau_ff_min"], "ok", "tau_obs_lt_tau_ff"
    )
    return out


def _filter_centerline_finite(edges: pd.DataFrame) -> pd.DataFrame:
    out = edges.copy()
    tau_obs = pd.to_numeric(out["tau_obs_min"], errors="coerce")
    tau_ff = pd.to_numeric(out["tau_ff_min"], errors="coerce")
    keep = np.isfinite(tau_obs) & (tau_obs > 0) & np.isfinite(tau_ff) & (tau_ff > 0)
    out = out[keep].copy()
    out["tau_obs_source"] = "stage06_t_edges_AM"
    return out


def _apply_detour_gate(edges: pd.DataFrame, gate: float) -> tuple[pd.DataFrame, dict]:
    """Drop adjacent edges whose routed length is an implausible detour.

    detour = route_length_m / straight-line centroid distance. Edges with
    detour > gate are treated as topological artifacts (e.g. a barrier forces
    the pair-specific shortest path far around) and removed; the pair can still
    be traversed multi-hop through the remaining graph.
    """
    import geopandas as gpd

    g = gpd.read_parquet(GRID_MASTER).to_crs(UTM_EPSG)
    cent = g.geometry.centroid
    xy = {str(gid): (float(x), float(y)) for gid, x, y in zip(g["grid_id"], cent.x.values, cent.y.values)}

    out = edges.copy()
    out["grid_o"] = out["grid_o"].astype(str)
    out["grid_d"] = out["grid_d"].astype(str)

    def dgeo(o: str, d: str) -> float:
        if o in xy and d in xy:
            (x1, y1), (x2, y2) = xy[o], xy[d]
            return float(np.hypot(x1 - x2, y1 - y2))
        return np.nan

    d_geo = np.array([dgeo(o, d) for o, d in zip(out["grid_o"], out["grid_d"])])
    route_len = pd.to_numeric(out["route_length_m"], errors="coerce").to_numpy()
    detour = route_len / d_geo
    keep = ~(detour > gate)  # keep NaN detour (no geometry) to avoid silent drops
    info = {
        "detour_gate": float(gate),
        "edges_before_gate": int(len(out)),
        "edges_dropped_by_gate": int((~keep).sum()),
        "max_detour_kept": float(np.nanmax(detour[keep])) if keep.any() else np.nan,
    }
    return out[keep].copy(), info


def _restrict_lcc(
    nodes: pd.DataFrame,
    edges: pd.DataFrame,
    od: pd.DataFrame,
    *,
    grid_type: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, set[str]]:
    """Restrict to the largest connected component and re-index consistently.

    The base QSM uses two unrelated integer index spaces: ``nodes.node_i``
    (grid-node table) and ``edges.i/j`` (stage06 t-edge order). They must NOT
    be cross-filtered. The only reliable join key is ``grid_id``. We therefore
    filter everything by grid_id membership in the LCC and then rebuild ONE
    fresh contiguous node index (0..N-1) shared by nodes, edges, and OD.
    """
    lcc = largest_component_grids(edges, "grid_o", "grid_d")

    nodes = nodes.copy()
    nodes["grid_id"] = nodes["grid_id"].astype(str)
    nodes = nodes[nodes["grid_id"].isin(lcc)].copy()
    nodes = nodes.sort_values("grid_id").reset_index(drop=True)

    # fresh contiguous index keyed on grid_id
    nodes["node_i"] = np.arange(len(nodes), dtype=int)
    grid_to_idx = dict(zip(nodes["grid_id"], nodes["node_i"].astype(int)))

    edges = edges.copy()
    edges["grid_o"] = edges["grid_o"].astype(str)
    edges["grid_d"] = edges["grid_d"].astype(str)
    edges = edges[edges["grid_o"].isin(lcc) & edges["grid_d"].isin(lcc)].copy()
    edges["i"] = edges["grid_o"].map(grid_to_idx).astype(int)
    edges["j"] = edges["grid_d"].map(grid_to_idx).astype(int)

    od = od.copy()
    od = od[od["sample_definition"] == "reachable_AM"].copy()
    od["home_grid"] = od["home_grid"].astype(str)
    od["work_grid"] = od["work_grid"].astype(str)
    od = od[od["home_grid"].isin(lcc) & od["work_grid"].isin(lcc)].copy()
    od["home_i"] = od["home_grid"].map(grid_to_idx).astype(int)
    od["work_i"] = od["work_grid"].map(grid_to_idx).astype(int)
    if "origin_node" in od.columns:
        od["origin_node"] = od["home_i"]
    if "destination_node" in od.columns:
        od["destination_node"] = od["work_i"]

    nodes["component"] = 1
    nodes["keep_for_model"] = 1
    nodes["in_largest_component"] = 1
    return nodes, edges, od, lcc


def _summarize(
    *,
    cost_version: str,
    output_version_id: str,
    base_version: str,
    nodes: pd.DataFrame,
    edges: pd.DataFrame,
    od: pd.DataFrame,
    lcc_grids: set[str],
) -> dict:
    residents = pd.to_numeric(nodes["residents"], errors="coerce").fillna(0.0)
    jobs = pd.to_numeric(nodes["jobs"], errors="coerce").fillna(0.0)
    od_pop = pd.to_numeric(od["commuters_road"], errors="coerce").fillna(0.0)
    od_total = float(od_pop.sum())

    e = edges.copy()
    e["grid_o"] = e["grid_o"].astype(str)
    e["grid_d"] = e["grid_d"].astype(str)
    rev = e.rename(columns={"grid_o": "grid_d", "grid_d": "grid_o"})
    both = e.merge(rev, on=["grid_o", "grid_d"], suffixes=("_fwd", "_rev"), how="inner")
    ratios = both["tau_obs_min_fwd"] / both["tau_obs_min_rev"]
    ratios = ratios.replace([np.inf, -np.inf], np.nan).dropna()
    ratios = ratios[(ratios > 0) & (ratios <= 1)]
    sym = np.minimum(ratios, 1.0 / ratios)

    return {
        "cost_version": cost_version,
        "output_version_id": output_version_id,
        "base_version": base_version,
        "scope": "largest_connected_component",
        "n_nodes": int(len(nodes)),
        "n_edges": int(len(edges)),
        "n_od_pairs": int(len(od)),
        "lcc_grid_count": int(len(lcc_grids)),
        "resident_share_in_scope": float(residents.sum() / 7963068.0),
        "job_share_in_scope": float(jobs.sum() / 7963068.0),
        "od_road_commuters": od_total,
        "od_road_commuters_share_city": float(od_total / 95145.58895419701),
        "mean_tau_obs_min": float(pd.to_numeric(edges["tau_obs_min"], errors="coerce").mean()),
        "median_tau_obs_min": float(pd.to_numeric(edges["tau_obs_min"], errors="coerce").median()),
        "median_directional_ratio": float(sym.median()) if len(sym) else np.nan,
        "share_asym_lt_0_5": float((sym < 0.5).mean()) if len(sym) else np.nan,
        "both_direction_pairs": int(len(sym)),
        "tau_obs_source": str(edges["tau_obs_source"].iloc[0]) if len(edges) else "",
    }


def build_bundle(args: argparse.Namespace) -> Path:
    base_root = Path(args.base_version_root)
    out_root = Path(args.output_dir) / args.output_version_id
    data_dir = out_root / "data"
    metrics_dir = out_root / "metrics"
    data_dir.mkdir(parents=True, exist_ok=True)
    metrics_dir.mkdir(parents=True, exist_ok=True)

    nodes, edges, od = _load_base(base_root, args.grid_type)
    if args.cost_version == "edgeproj_v2_lcc":
        edges = _apply_edgeproj_costs(edges, Path(args.edgeproj_csv))
    else:
        edges = _filter_centerline_finite(edges)

    gate_info: dict | None = None
    if args.detour_gate is not None:
        edges, gate_info = _apply_detour_gate(edges, args.detour_gate)

    nodes, edges, od, lcc = _restrict_lcc(nodes, edges, od, grid_type=args.grid_type)

    nodes["version_id"] = args.output_version_id
    edges["version_id"] = args.output_version_id
    od["version_id"] = args.output_version_id

    summary = _summarize(
        cost_version=args.cost_version,
        output_version_id=args.output_version_id,
        base_version=base_root.name,
        nodes=nodes,
        edges=edges,
        od=od,
        lcc_grids=lcc,
    )
    summary["source_label"] = args.source_label
    summary["edgeproj_csv"] = str(Path(args.edgeproj_csv)) if args.cost_version == "edgeproj_v2_lcc" else ""
    summary["detour_gate"] = args.detour_gate if args.detour_gate is not None else "none"
    if gate_info is not None:
        summary.update(gate_info)

    nodes.to_parquet(data_dir / f"qsm_input_nodes_{args.grid_type}.parquet", index=False)
    edges.to_parquet(data_dir / f"qsm_input_edges_{args.grid_type}.parquet", index=False)
    od.to_parquet(data_dir / f"qsm_input_od_{args.grid_type}.parquet", index=False)
    pd.DataFrame([summary]).to_csv(data_dir / f"data_quality_{args.grid_type}_qsm.csv", index=False)
    (metrics_dir / "travel_cost_bundle_summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    (out_root / "config_snapshot.travel_cost_bundle.json").write_text(
        json.dumps(
            {
                "cost_version": args.cost_version,
                "source_label": args.source_label,
                "base_version_root": str(base_root),
                "edgeproj_csv": args.edgeproj_csv,
                "grid_type": args.grid_type,
                "period": args.period,
                "detour_gate": args.detour_gate if args.detour_gate is not None else "none",
                "scope": "largest_connected_component",
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return out_root


def main() -> None:
    args = _parse_args()
    out_root = build_bundle(args)
    print(f"[travel_cost_bundle] wrote {out_root}")


if __name__ == "__main__":
    main()
