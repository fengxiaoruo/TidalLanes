"""Diagnose centerline topology gaps using supporting paths in the raw road graph.

The source centerline version is read-only. This script writes a candidate-only,
versioned diagnostic package and deliberately does not create a replacement
centerline master or rerun downstream stages.
"""

from __future__ import annotations

import argparse
import json
import shutil
from collections import defaultdict
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import networkx as nx
import numpy as np
import pandas as pd
from scipy.spatial import cKDTree
from shapely.geometry import LineString
from shapely.ops import linemerge, unary_union

from bridge_centerline_endpoints_experiment import build_component_labels, endpoints


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", default="data_work/outputs/manual_centerline_rules_v11")
    parser.add_argument(
        "--output-root",
        default="data_work/outputs/centerline_rawpath_topology_candidates_v1",
    )
    parser.add_argument("--component-snap-m", type=float, default=35.0)
    parser.add_argument("--raw-node-snap-m", type=float, default=20.0)
    parser.add_argument("--max-access-m", type=float, default=80.0)
    parser.add_argument("--max-direct-gap-m", type=float, default=500.0)
    parser.add_argument("--max-route-m", type=float, default=1000.0)
    parser.add_argument("--review-top-n", type=int, default=30)
    return parser.parse_args()


def prepare_output(path: Path, source: Path) -> None:
    if path.resolve() == source.resolve():
        raise SystemExit("Refusing to write into the source version.")
    if path.exists():
        raise SystemExit(f"Refusing to overwrite existing output: {path}")
    for name in ("data", "metrics", "figures", "docs"):
        (path / name).mkdir(parents=True, exist_ok=False)


def line_endpoints_table(centerline: gpd.GeoDataFrame) -> pd.DataFrame:
    rows = []
    for pos, row in enumerate(centerline.itertuples(index=False)):
        pts = endpoints(row.geometry)
        for end_idx, (x, y) in enumerate(pts):
            rows.append(
                {
                    "cl_pos": pos,
                    "cline_id": int(row.cline_id),
                    "comp_id": int(row.comp_id),
                    "end_idx": end_idx,
                    "x": float(x),
                    "y": float(y),
                }
            )
    return pd.DataFrame(rows)


def snapped_raw_graph(raw: gpd.GeoDataFrame, snap_m: float):
    node_lookup: dict[tuple[int, int], int] = {}
    node_xy: list[tuple[float, float]] = []

    def node_id(xy):
        key = (int(round(xy[0] / snap_m)), int(round(xy[1] / snap_m)))
        if key not in node_lookup:
            node_lookup[key] = len(node_xy)
            node_xy.append((float(xy[0]), float(xy[1])))
        return node_lookup[key]

    graph = nx.Graph()
    edge_geoms = {}
    for row in raw.itertuples(index=False):
        geom = row.geometry
        if geom is None or geom.is_empty or geom.geom_type != "LineString":
            continue
        coords = list(geom.coords)
        if len(coords) < 2:
            continue
        u, v = node_id(coords[0]), node_id(coords[-1])
        if u == v:
            continue
        length = float(geom.length)
        old = graph.get_edge_data(u, v)
        if old is None or length < old["weight"]:
            graph.add_edge(u, v, weight=length, raw_edge_id=int(row.raw_edge_id))
            edge_geoms[tuple(sorted((u, v)))] = geom
    return graph, np.asarray(node_xy), edge_geoms


def path_geometry(path: list[int], edge_geoms: dict, node_xy: np.ndarray):
    pieces = []
    for u, v in zip(path[:-1], path[1:]):
        geom = edge_geoms.get(tuple(sorted((u, v))))
        if geom is None:
            geom = LineString([node_xy[u], node_xy[v]])
        pieces.append(geom)
    if not pieces:
        return None
    unioned = unary_union(pieces)
    merged = unioned if unioned.geom_type == "LineString" else linemerge(unioned)
    if merged.geom_type == "LineString":
        return merged
    return max(merged.geoms, key=lambda g: g.length)


def confidence(row) -> str:
    max_access = max(row.access_from_m, row.access_to_m)
    if max_access <= 40 and row.raw_route_m <= 250 and row.route_stretch <= 2.0:
        return "high"
    if max_access <= 60 and row.raw_route_m <= 500 and row.route_stretch <= 3.5:
        return "medium"
    return "low"


def write_docs(output: Path, args, counts: dict) -> None:
    docs = {
        "analysis-plan.md": """# Analysis plan\n\n1. Reproduce weak components in the retained centerline baseline.\n2. Build an undirected support graph from retained raw road segments.\n3. Map centerline endpoints to nearby raw-graph nodes.\n4. Find the shortest raw-road-supported route from each non-LCC component to the LCC.\n5. Rank candidates by access distance, route length, stretch, and component importance.\n6. Export candidates for review only; do not mutate the centerline baseline.\n""",
        "variable-dictionary.md": """# Variable dictionary\n\n- `comp_id`: weak component under 35 m centerline endpoint snapping.\n- `component_n_cl`: number of retained centerlines in the non-LCC component.\n- `component_length_m`: retained centerline length in that component.\n- `access_from_m`: distance from the non-LCC centerline endpoint to its nearest raw node.\n- `access_to_m`: distance from the reached raw node to the nearest LCC centerline endpoint.\n- `raw_route_m`: shortest route length on the undirected raw-road support graph.\n- `direct_gap_m`: Euclidean distance between the two centerline endpoints.\n- `route_stretch`: `(access_from + raw_route + access_to) / direct_gap`.\n- `confidence`: mechanical review tier; not an acceptance decision.\n""",
        "sample-lineage.md": f"""# Sample lineage\n\n- Source version: `{args.source_root}` (read-only).\n- Centerlines: `centerline_master.parquet`, restricted to `keep_baseline == True`.\n- Raw support: `raw_segment_master.parquet`, restricted to valid baseline LineStrings.\n- Baseline weak components: {counts['n_components']}.\n- Non-LCC components evaluated: {counts['n_non_lcc']}.\n- Components with an exported raw-supported candidate: {counts['n_candidates']}.\n- No source rows are edited, deleted, or rewritten.\n""",
        "geospatial-data-notes.md": f"""# Geospatial data notes\n\n- CRS is inherited from the source and measured in metres.\n- Centerline components use {args.component_snap_m:g} m endpoint snapping.\n- Raw graph nodes use {args.raw_node_snap_m:g} m endpoint snapping.\n- Raw roads are treated as undirected only for geometric topology support; this output is not a directed travel-cost network.\n- Candidate routes must satisfy endpoint access <= {args.max_access_m:g} m, direct gap <= {args.max_direct_gap_m:g} m, and raw route <= {args.max_route_m:g} m.\n- Grade separation, barriers, and legal turns are not fully encoded; candidates require spatial review before acceptance.\n""",
        "exposure-definition.md": """# Exposure definition\n\nNot applicable. This package diagnoses network topology and does not define a treatment or empirical exposure.\n""",
        "regression-spec-matrix.md": """# Regression specification matrix\n\nNot applicable. No regression is estimated in this topology diagnostic.\n""",
        "table-shells.md": """# Table shells\n\nThe candidate table reports component importance, endpoint access, raw-route length, direct gap, stretch, and review tier. The metrics table compares component coverage across tiers.\n""",
        "replication-checklist.md": f"""# Replication checklist\n\n- [x] Source and output versions are explicit.\n- [x] Existing output directories are never overwritten.\n- [x] Parameters are stored in `metrics/run_manifest.json`.\n- [x] Candidate geometries and tabular attributes are both exported.\n- [x] No candidate is promoted into the production centerline automatically.\n\nRun from the project root:\n\n```bash\npython data_work/src/diagnostics/diagnose_centerline_rawpath_topology.py --source-root {args.source_root} --output-root {args.output_root}\n```\n""",
    }
    for name, body in docs.items():
        (output / "docs" / name).write_text(body, encoding="utf-8")


def main() -> None:
    args = parse_args()
    source = Path(args.source_root)
    output = Path(args.output_root)
    prepare_output(output, source)

    centerline_all = gpd.read_parquet(source / "data" / "centerline_master.parquet")
    raw = gpd.read_parquet(source / "data" / "raw_segment_master.parquet")
    centerline = centerline_all.loc[centerline_all["keep_baseline"]].copy().reset_index(drop=True)
    raw = raw.loc[
        raw["keep_baseline"]
        & raw["is_valid_geometry"]
        & raw["is_linestring"]
    ].copy().reset_index(drop=True)
    if raw.crs != centerline.crs:
        raw = raw.to_crs(centerline.crs)

    labels, _ = build_component_labels(centerline, args.component_snap_m)
    centerline["comp_id"] = labels
    comp = centerline.groupby("comp_id").agg(
        component_n_cl=("cline_id", "size"), component_length_m=("length_m", "sum")
    )
    lcc_id = int(comp["component_n_cl"].idxmax())
    ep = line_endpoints_table(centerline)

    graph, raw_xy, edge_geoms = snapped_raw_graph(raw, args.raw_node_snap_m)
    raw_tree = cKDTree(raw_xy)
    ep_dist, ep_node = raw_tree.query(ep[["x", "y"]].to_numpy(), k=1)
    ep["raw_node"] = ep_node.astype(int)
    ep["raw_access_m"] = ep_dist.astype(float)

    lcc_ep = ep.loc[(ep.comp_id == lcc_id) & (ep.raw_access_m <= args.max_access_m)].copy()
    raw_node_to_lcc_ep = {}
    for row in lcc_ep.itertuples(index=False):
        current = raw_node_to_lcc_ep.get(int(row.raw_node))
        if current is None or row.raw_access_m < current.raw_access_m:
            raw_node_to_lcc_ep[int(row.raw_node)] = row
    sources = list(raw_node_to_lcc_ep)
    distances, paths = nx.multi_source_dijkstra(graph, sources=sources, cutoff=args.max_route_m, weight="weight")

    candidates = []
    geoms = []
    for comp_id, group in ep.loc[ep.comp_id != lcc_id].groupby("comp_id"):
        best = None
        for row in group.itertuples(index=False):
            node = int(row.raw_node)
            if row.raw_access_m > args.max_access_m or node not in distances:
                continue
            path = paths[node]
            source_node = int(path[0])
            target = raw_node_to_lcc_ep[source_node]
            direct = float(np.hypot(row.x - target.x, row.y - target.y))
            if direct <= args.component_snap_m or direct > args.max_direct_gap_m:
                continue
            raw_route = float(distances[node])
            total = float(row.raw_access_m + raw_route + target.raw_access_m)
            stretch = total / direct
            record = {
                "comp_id": int(comp_id),
                "from_cline_id": int(row.cline_id),
                "from_end_idx": int(row.end_idx),
                "to_cline_id": int(target.cline_id),
                "to_end_idx": int(target.end_idx),
                "access_from_m": float(row.raw_access_m),
                "access_to_m": float(target.raw_access_m),
                "raw_route_m": raw_route,
                "direct_gap_m": direct,
                "total_supported_m": total,
                "route_stretch": stretch,
                "from_x": float(row.x),
                "from_y": float(row.y),
                "to_x": float(target.x),
                "to_y": float(target.y),
                "raw_path_nodes": len(path),
                "path": path,
            }
            if best is None or (total, stretch) < (best[0]["total_supported_m"], best[0]["route_stretch"]):
                best = (record, path_geometry(path, edge_geoms, raw_xy))
        if best is not None:
            record, raw_geom = best
            record.update(comp.loc[int(comp_id)].to_dict())
            candidates.append(record)
            connectors = [
                LineString([(record["from_x"], record["from_y"]), raw_xy[record["path"][-1]]]),
                raw_geom,
                LineString([raw_xy[record["path"][0]], (record["to_x"], record["to_y"])]),
            ]
            connectors = [g for g in connectors if g is not None and not g.is_empty and g.length > 0]
            unioned = unary_union(connectors)
            geoms.append(unioned if unioned.geom_type == "LineString" else linemerge(unioned))

    table = pd.DataFrame(candidates)
    if not table.empty:
        table["confidence"] = table.apply(confidence, axis=1)
        table["importance_score"] = np.log1p(table["component_length_m"]) / np.log1p(table["total_supported_m"])
        table = table.sort_values(
            ["confidence", "importance_score", "total_supported_m"],
            ascending=[True, False, True],
            key=lambda s: s.map({"high": 0, "medium": 1, "low": 2}) if s.name == "confidence" else s,
        ).reset_index(drop=True)
        table.insert(0, "candidate_id", [f"RAWPATH_{i:04d}" for i in range(1, len(table) + 1)])
        geom_by_comp = {int(r["comp_id"]): g for r, g in zip(candidates, geoms)}
        candidate_gdf = gpd.GeoDataFrame(
            table.drop(columns=["path"]),
            geometry=[geom_by_comp[int(c)] for c in table.comp_id],
            crs=centerline.crs,
        )
    else:
        candidate_gdf = gpd.GeoDataFrame(table, geometry=[], crs=centerline.crs)

    export_table = table.drop(columns=["path"], errors="ignore")
    export_table.to_csv(output / "data" / "rawpath_topology_candidates.csv", index=False)
    candidate_gdf.to_file(output / "data" / "rawpath_topology_candidates.geojson", driver="GeoJSON")
    comp.reset_index().to_csv(output / "data" / "centerline_component_inventory.csv", index=False)

    tier = export_table.groupby("confidence", dropna=False).agg(
        candidates=("candidate_id", "size"),
        connected_centerlines=("component_n_cl", "sum"),
        connected_length_km=("component_length_m", lambda x: x.sum() / 1000),
    ).reset_index() if not export_table.empty else pd.DataFrame()
    tier.to_csv(output / "metrics" / "candidate_tier_summary.csv", index=False)

    review = candidate_gdf.head(args.review_top_n)
    if len(review):
        fig, ax = plt.subplots(figsize=(12, 12))
        centerline.plot(ax=ax, color="#c7c7c7", linewidth=0.25)
        centerline.loc[centerline.comp_id == lcc_id].plot(ax=ax, color="#5f7f8f", linewidth=0.35)
        review.plot(ax=ax, column="confidence", categorical=True, legend=True, linewidth=1.4)
        ax.set_axis_off()
        ax.set_title(f"Top {len(review)} raw-road-supported centerline topology candidates")
        fig.tight_layout()
        fig.savefig(output / "figures" / "candidate_overview.png", dpi=220)
        plt.close(fig)

    counts = {
        "n_centerlines": int(len(centerline)),
        "n_components": int(len(comp)),
        "n_non_lcc": int(len(comp) - 1),
        "lcc_id": lcc_id,
        "n_raw_segments": int(len(raw)),
        "n_raw_nodes": int(graph.number_of_nodes()),
        "n_raw_edges": int(graph.number_of_edges()),
        "n_candidates": int(len(export_table)),
        "confidence_counts": export_table["confidence"].value_counts().to_dict() if len(export_table) else {},
    }
    manifest = {
        "version": output.name,
        "source_root": str(source),
        "output_root": str(output),
        "source_untouched": True,
        "candidate_only": True,
        "parameters": vars(args),
        "counts": counts,
    }
    (output / "metrics" / "run_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    write_docs(output, args, counts)
    shutil.copy2(__file__, output / "docs" / "diagnose_centerline_rawpath_topology.py")
    (output / "README.md").write_text(
        f"""# Raw-road-supported centerline topology candidates\n\nSource remains untouched: `{source}`.\n\nThis is a candidate-only diagnostic. It does not replace the centerline master, assign speeds, or rerun QSM inputs. Review `data/rawpath_topology_candidates.csv`, the GeoJSON, and `figures/candidate_overview.png`.\n\nKey counts: {counts['n_components']} baseline components; {counts['n_candidates']} non-LCC components have an admissible raw-road-supported candidate. Confidence tiers are mechanical filters and are not automatic acceptance decisions.\n""",
        encoding="utf-8",
    )
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
