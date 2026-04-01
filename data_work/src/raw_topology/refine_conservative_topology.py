"""
Step 4 for the standalone raw-topology prototype:
- identify likely false geometric joins
- conservatively disconnect those joins
- export refined nodes/edges and suspicious-node review layers
"""

from __future__ import annotations

import argparse
import json
import math
from collections import Counter
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

from src.raw_topology.utils import ensure_output_dirs, graph_component_labels


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT = ROOT / "outputs" / "raw_topology_mvp"


def parse_args():
    parser = argparse.ArgumentParser(description="Conservatively refine the raw topology graph")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT), help="Prototype output directory")
    parser.add_argument(
        "--straight-angle-min",
        type=float,
        default=150.0,
        help="Minimum within-road angle to treat a crossing as a likely through movement",
    )
    parser.add_argument(
        "--high-confidence-angle-min",
        type=float,
        default=178.0,
        help="Minimum straight angle for high-confidence false joins",
    )
    return parser.parse_args()


def _point_key(x: float, y: float, precision: int = 3) -> tuple[float, float]:
    return (round(float(x), precision), round(float(y), precision))


def _bearing_from_node(line, node_xy: tuple[float, float], at_u: bool) -> float:
    coords = list(line.coords)
    if at_u:
        a = coords[0]
        b = coords[1]
    else:
        a = coords[-1]
        b = coords[-2]
    dx = b[0] - a[0]
    dy = b[1] - a[1]
    ang = math.degrees(math.atan2(dy, dx))
    return (ang + 360.0) % 360.0


def _angle_diff(a: float, b: float) -> float:
    d = abs(a - b) % 360.0
    return min(d, 360.0 - d)


def detect_suspicious_nodes(
    edges: gpd.GeoDataFrame,
    nodes: gpd.GeoDataFrame,
    endpoint_clusters: gpd.GeoDataFrame,
    straight_angle_min: float,
    high_confidence_angle_min: float,
) -> gpd.GeoDataFrame:
    endpoint_counts = Counter(
        _point_key(row.x_snap, row.y_snap)
        for row in endpoint_clusters[["x_snap", "y_snap"]].itertuples(index=False)
    )
    node_lookup = nodes.set_index("node_id")[["x", "y", "geometry", "component_id"]]
    road_meta = edges[["roadseg_id", "roadname", "semantic", "roadtype"]].drop_duplicates("roadseg_id").set_index("roadseg_id")

    incidence_records = []
    for row in edges.itertuples(index=False):
        for side, node_id in [("u", int(row.u)), ("v", int(row.v))]:
            node_row = node_lookup.loc[node_id]
            node_xy = (float(node_row["x"]), float(node_row["y"]))
            bearing = _bearing_from_node(row.geometry, node_xy, at_u=(side == "u"))
            incidence_records.append(
                {
                    "node_id": node_id,
                    "noded_edge_id": int(row.noded_edge_id),
                    "roadseg_id": row.roadseg_id,
                    "roadtype": row.roadtype,
                    "bearing_deg": bearing,
                    "side": side,
                }
            )
    incidence = pd.DataFrame(incidence_records)
    node_summary = (
        incidence.groupby("node_id")
        .agg(
            degree=("noded_edge_id", "count"),
            unique_roadseg=("roadseg_id", "nunique"),
        )
        .reset_index()
    )
    node_summary = node_summary.merge(nodes[["node_id", "x", "y", "component_id"]], on="node_id", how="left")
    node_summary["endpoint_support_count"] = node_summary.apply(
        lambda row: endpoint_counts.get(_point_key(row["x"], row["y"]), 0),
        axis=1,
    )

    suspicious_rows = []
    for row in node_summary.itertuples(index=False):
        if int(row.degree) != 4 or int(row.unique_roadseg) != 2 or int(row.endpoint_support_count) != 0:
            continue
        local = incidence.loc[incidence["node_id"] == row.node_id].copy()
        counts = local["roadseg_id"].value_counts()
        if sorted(counts.tolist()) != [2, 2]:
            continue
        straight_angles = []
        roadseg_ids = []
        roadtypes = []
        keep = True
        for roadseg_id, group in local.groupby("roadseg_id"):
            bearings = group["bearing_deg"].tolist()
            if len(bearings) != 2:
                keep = False
                break
            diff = _angle_diff(bearings[0], bearings[1])
            straight_angles.append(diff)
            roadseg_ids.append(roadseg_id)
            roadtypes.append(int(group["roadtype"].iloc[0]))
            if diff < straight_angle_min:
                keep = False
                break
        if not keep:
            continue
        roadname_a = str(road_meta.loc[roadseg_ids[0], "roadname"]) if roadseg_ids[0] in road_meta.index else ""
        roadname_b = str(road_meta.loc[roadseg_ids[1], "roadname"]) if roadseg_ids[1] in road_meta.index else ""
        semantic_a = str(road_meta.loc[roadseg_ids[0], "semantic"]) if roadseg_ids[0] in road_meta.index else ""
        semantic_b = str(road_meta.loc[roadseg_ids[1], "semantic"]) if roadseg_ids[1] in road_meta.index else ""
        same_roadname = roadname_a == roadname_b
        both_have_junction = ("交叉路口" in semantic_a or "路口" in semantic_a) and ("交叉路口" in semantic_b or "路口" in semantic_b)
        min_angle = float(min(straight_angles))
        max_angle = float(max(straight_angles))
        if min_angle >= high_confidence_angle_min and (not same_roadname) and (not both_have_junction):
            confidence = "high"
        elif min_angle >= high_confidence_angle_min:
            confidence = "medium"
        else:
            confidence = "low"
        suspicious_rows.append(
            {
                "node_id": int(row.node_id),
                "degree": int(row.degree),
                "unique_roadseg": int(row.unique_roadseg),
                "component_id": int(row.component_id),
                "endpoint_support_count": int(row.endpoint_support_count),
                "roadseg_id_a": roadseg_ids[0],
                "roadseg_id_b": roadseg_ids[1],
                "roadtype_a": roadtypes[0],
                "roadtype_b": roadtypes[1],
                "straight_angle_a": float(straight_angles[0]),
                "straight_angle_b": float(straight_angles[1]),
                "min_angle": min_angle,
                "max_angle": max_angle,
                "roadname_a": roadname_a,
                "roadname_b": roadname_b,
                "same_roadname": same_roadname,
                "semantic_a": semantic_a,
                "semantic_b": semantic_b,
                "both_have_junction": both_have_junction,
                "confidence": confidence,
                "geometry": node_lookup.loc[row.node_id, "geometry"],
                "reason": "midblock_x_crossing_no_endpoint_support",
            }
        )
    suspicious = gpd.GeoDataFrame(suspicious_rows, geometry="geometry", crs=nodes.crs)
    return suspicious


def refine_edges(edges: gpd.GeoDataFrame, nodes: gpd.GeoDataFrame, suspicious: gpd.GeoDataFrame) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    refined = edges.copy()
    node_meta = nodes[["node_id", "x", "y", "geometry"]].copy()
    next_node_id = int(node_meta["node_id"].max()) + 1 if not node_meta.empty else 0
    clone_rows = []

    applied = suspicious.loc[suspicious["confidence"] == "high"].copy()
    for row in applied.itertuples(index=False):
        node_id = int(row.node_id)
        incident_mask = (refined["u"] == node_id) | (refined["v"] == node_id)
        incident = refined.loc[incident_mask].copy()
        for group_idx, (roadseg_id, group) in enumerate(incident.groupby("roadseg_id"), start=1):
            new_node_id = next_node_id
            next_node_id += 1
            clone_rows.append(
                {
                    "node_id": new_node_id,
                    "x": float(row.geometry.x),
                    "y": float(row.geometry.y),
                    "geometry": row.geometry,
                    "repair_parent_node_id": node_id,
                    "repair_group": group_idx,
                    "repair_roadseg_id": roadseg_id,
                }
            )
            edge_ids = group["noded_edge_id"].tolist()
            use_u = refined["noded_edge_id"].isin(edge_ids) & (refined["u"] == node_id)
            use_v = refined["noded_edge_id"].isin(edge_ids) & (refined["v"] == node_id)
            refined.loc[use_u, "u"] = new_node_id
            refined.loc[use_v, "v"] = new_node_id

    used_nodes = pd.Index(sorted(set(refined["u"]).union(set(refined["v"]))), dtype=int)
    base_nodes = node_meta.loc[node_meta["node_id"].isin(used_nodes)].copy()
    clone_nodes = gpd.GeoDataFrame(clone_rows, geometry="geometry", crs=nodes.crs) if clone_rows else gpd.GeoDataFrame(columns=["node_id", "x", "y", "geometry"], geometry="geometry", crs=nodes.crs)
    refined_nodes = pd.concat([base_nodes, clone_nodes], ignore_index=True, sort=False)
    refined_nodes = gpd.GeoDataFrame(refined_nodes, geometry="geometry", crs=nodes.crs)
    labels = graph_component_labels(refined[["u", "v"]])
    refined["component_id"] = refined["u"].map(labels).astype("Int64")
    refined_nodes["component_id"] = refined_nodes["node_id"].map(labels).astype("Int64")
    return refined, refined_nodes


def main():
    args = parse_args()
    output_root = Path(args.output_root)
    paths = ensure_output_dirs(output_root)

    edges = gpd.read_parquet(paths.data_dir / "raw_topology_edges.parquet")
    nodes = gpd.read_parquet(paths.data_dir / "raw_topology_nodes.parquet")
    endpoint_clusters = gpd.read_parquet(paths.data_dir / "raw_endpoint_clusters.parquet")

    suspicious = detect_suspicious_nodes(
        edges,
        nodes,
        endpoint_clusters,
        args.straight_angle_min,
        args.high_confidence_angle_min,
    )
    refined_edges, refined_nodes = refine_edges(edges, nodes, suspicious)

    suspicious.to_parquet(paths.data_dir / "raw_topology_suspicious_nodes.parquet", index=False)
    suspicious.loc[suspicious["confidence"] == "high"].to_parquet(
        paths.data_dir / "raw_topology_suspicious_nodes_high.parquet",
        index=False,
    )
    suspicious.loc[suspicious["confidence"] != "high"].to_parquet(
        paths.data_dir / "raw_topology_suspicious_nodes_review.parquet",
        index=False,
    )
    refined_edges.to_parquet(paths.data_dir / "raw_topology_edges_refined.parquet", index=False)
    refined_nodes.to_parquet(paths.data_dir / "raw_topology_nodes_refined.parquet", index=False)

    suspicious_ids = set(suspicious["node_id"].tolist())
    context_edges = edges.loc[edges["u"].isin(suspicious_ids) | edges["v"].isin(suspicious_ids)].copy()
    context_edges.to_parquet(paths.data_dir / "raw_topology_suspicious_edge_context.parquet", index=False)

    summary = {
        "straight_angle_min": args.straight_angle_min,
        "high_confidence_angle_min": args.high_confidence_angle_min,
        "suspicious_nodes": int(len(suspicious)),
        "suspicious_high_confidence": int((suspicious["confidence"] == "high").sum()),
        "suspicious_review_only": int((suspicious["confidence"] != "high").sum()),
        "suspicious_context_edges": int(len(context_edges)),
        "refined_edges_total": int(len(refined_edges)),
        "refined_nodes_total": int(len(refined_nodes)),
        "components_before": int(edges["component_id"].nunique()),
        "components_after": int(refined_edges["component_id"].nunique()),
    }
    (paths.metrics_dir / "refine_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
