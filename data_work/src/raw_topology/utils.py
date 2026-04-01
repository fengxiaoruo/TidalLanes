"""
Utilities for the standalone raw-road topology prototype.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely import MultiLineString, get_parts, node
from shapely.geometry import LineString, Point
from sklearn.cluster import DBSCAN


TARGET_EPSG = 3857


@dataclass
class PrototypePaths:
    version_root: Path
    data_dir: Path
    metrics_dir: Path


def ensure_output_dirs(output_root: Path) -> PrototypePaths:
    data_dir = output_root / "data"
    metrics_dir = output_root / "metrics"
    data_dir.mkdir(parents=True, exist_ok=True)
    metrics_dir.mkdir(parents=True, exist_ok=True)
    return PrototypePaths(version_root=output_root, data_dir=data_dir, metrics_dir=metrics_dir)


def load_raw_roads(path: Path) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(path)
    if gdf.crs is None or gdf.crs.to_epsg() != TARGET_EPSG:
        gdf = gdf.to_crs(TARGET_EPSG)
    gdf = gdf.explode(index_parts=False).reset_index(drop=True)
    gdf = gdf.loc[gdf.geometry.notna() & (~gdf.geometry.is_empty)].copy()
    gdf = gdf.loc[gdf.geometry.geom_type == "LineString"].copy()
    gdf["raw_edge_id"] = np.arange(len(gdf), dtype=int)
    gdf["length_m"] = gdf.geometry.length
    return gdf.reset_index(drop=True)


def deduplicate_exact_geometries(gdf: gpd.GeoDataFrame) -> tuple[gpd.GeoDataFrame, int]:
    out = gdf.copy()
    out["geometry_wkb"] = out.geometry.apply(lambda geom: geom.wkb_hex)
    before = len(out)
    out = out.drop_duplicates(subset=["roadseg_id", "geometry_wkb"]).copy()
    removed = before - len(out)
    out = out.drop(columns=["geometry_wkb"])
    out["raw_edge_id"] = np.arange(len(out), dtype=int)
    out["length_m"] = out.geometry.length
    return out.reset_index(drop=True), removed


def _endpoint_array(gdf: gpd.GeoDataFrame) -> tuple[np.ndarray, np.ndarray]:
    rows = []
    meta = []
    for row in gdf.itertuples(index=False):
        coords = list(row.geometry.coords)
        rows.append(coords[0])
        meta.append((int(row.raw_edge_id), 0))
        rows.append(coords[-1])
        meta.append((int(row.raw_edge_id), 1))
    return np.asarray(rows, dtype=float), np.asarray(meta, dtype=int)


def cluster_endpoints(gdf: gpd.GeoDataFrame, snap_tol_m: float) -> tuple[gpd.GeoDataFrame, pd.DataFrame]:
    coords, meta = _endpoint_array(gdf)
    model = DBSCAN(eps=snap_tol_m, min_samples=1)
    labels = model.fit_predict(coords)
    endpoint_df = pd.DataFrame(
        {
            "raw_edge_id": meta[:, 0],
            "endpoint_idx": meta[:, 1],
            "x": coords[:, 0],
            "y": coords[:, 1],
            "cluster_id": labels.astype(int),
        }
    )
    centers = (
        endpoint_df.groupby("cluster_id")[["x", "y"]]
        .mean()
        .rename(columns={"x": "x_snap", "y": "y_snap"})
        .reset_index()
    )
    endpoint_df = endpoint_df.merge(centers, on="cluster_id", how="left")
    return (
        gpd.GeoDataFrame(endpoint_df, geometry=gpd.points_from_xy(endpoint_df["x_snap"], endpoint_df["y_snap"]), crs=gdf.crs),
        centers,
    )


def _replace_line_endpoints(geom: LineString, start_xy: tuple[float, float], end_xy: tuple[float, float]) -> LineString | None:
    coords = list(geom.coords)
    if len(coords) < 2:
        return None
    coords[0] = start_xy
    coords[-1] = end_xy
    if coords[0] == coords[-1]:
        return None
    out = LineString(coords)
    if out.is_empty or out.length <= 0:
        return None
    return out


def snap_line_endpoints(gdf: gpd.GeoDataFrame, endpoint_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    lookup = (
        endpoint_gdf[["raw_edge_id", "endpoint_idx", "x_snap", "y_snap"]]
        .sort_values(["raw_edge_id", "endpoint_idx"])
        .set_index(["raw_edge_id", "endpoint_idx"])
    )
    rows = []
    for row in gdf.itertuples(index=False):
        start_xy = tuple(lookup.loc[(int(row.raw_edge_id), 0), ["x_snap", "y_snap"]].to_numpy())
        end_xy = tuple(lookup.loc[(int(row.raw_edge_id), 1), ["x_snap", "y_snap"]].to_numpy())
        geom = _replace_line_endpoints(row.geometry, start_xy, end_xy)
        if geom is None:
            continue
        payload = row._asdict()
        payload["geometry"] = geom
        payload["length_m"] = geom.length
        rows.append(payload)
    return gpd.GeoDataFrame(rows, geometry="geometry", crs=gdf.crs).reset_index(drop=True)


def node_network(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    merged = MultiLineString(list(gdf.geometry))
    noded = node(merged)
    parts = [geom for geom in get_parts(noded) if geom.length > 0]
    return gpd.GeoDataFrame({"noded_edge_id": np.arange(len(parts), dtype=int)}, geometry=parts, crs=gdf.crs)


def assign_lineage(noded_edges: gpd.GeoDataFrame, source_gdf: gpd.GeoDataFrame, overlap_tol_m: float = 1.0) -> gpd.GeoDataFrame:
    sindex = source_gdf.sindex
    records = []
    for row in noded_edges.itertuples(index=False):
        geom = row.geometry
        candidate_idx = list(sindex.intersection(geom.bounds))
        best_idx = None
        best_overlap = -1.0
        for idx in candidate_idx:
            source_geom = source_gdf.geometry.iloc[idx]
            overlap = geom.intersection(source_geom.buffer(overlap_tol_m)).length
            if overlap > best_overlap:
                best_overlap = overlap
                best_idx = idx
        payload = {"noded_edge_id": int(row.noded_edge_id), "geometry": geom, "length_m": geom.length}
        if best_idx is not None:
            src = source_gdf.iloc[best_idx]
            for col in source_gdf.columns:
                if col != "geometry":
                    payload[col] = src[col]
            payload["source_overlap_m"] = float(best_overlap)
        else:
            payload["source_overlap_m"] = 0.0
        records.append(payload)
    return gpd.GeoDataFrame(records, geometry="geometry", crs=source_gdf.crs)


def build_nodes_and_edges(edge_gdf: gpd.GeoDataFrame, node_tol_m: float = 0.5) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    endpoint_records = []
    for row in edge_gdf.itertuples(index=False):
        coords = list(row.geometry.coords)
        endpoint_records.append({"edge_id": int(row.noded_edge_id), "endpoint_idx": 0, "x": coords[0][0], "y": coords[0][1]})
        endpoint_records.append({"edge_id": int(row.noded_edge_id), "endpoint_idx": 1, "x": coords[-1][0], "y": coords[-1][1]})
    endpoint_df = pd.DataFrame(endpoint_records)
    scaled = endpoint_df[["x", "y"]].to_numpy() / max(node_tol_m, 1e-6)
    labels = DBSCAN(eps=1.0, min_samples=1).fit_predict(scaled)
    endpoint_df["node_id"] = labels.astype(int)
    node_xy = endpoint_df.groupby("node_id")[["x", "y"]].mean().reset_index()
    nodes = gpd.GeoDataFrame(node_xy, geometry=gpd.points_from_xy(node_xy["x"], node_xy["y"]), crs=edge_gdf.crs)

    uv = endpoint_df.pivot(index="edge_id", columns="endpoint_idx", values="node_id").reset_index()
    uv.columns = ["noded_edge_id", "u", "v"]
    edges = edge_gdf.merge(uv, on="noded_edge_id", how="left")
    edges["u"] = edges["u"].astype(int)
    edges["v"] = edges["v"].astype(int)
    return nodes, edges


def save_geodataframe(gdf: gpd.GeoDataFrame, path: Path):
    if path.suffix == ".parquet":
        gdf.to_parquet(path, index=False)
    else:
        gdf.to_file(path)


def graph_component_labels(edges: pd.DataFrame) -> dict[int, int]:
    adj: dict[int, set[int]] = {}
    for row in edges.itertuples(index=False):
        u = int(row.u)
        v = int(row.v)
        adj.setdefault(u, set()).add(v)
        adj.setdefault(v, set()).add(u)
    labels: dict[int, int] = {}
    comp_id = 0
    for node_id in adj:
        if node_id in labels:
            continue
        stack = [node_id]
        labels[node_id] = comp_id
        while stack:
            cur = stack.pop()
            for nbr in adj[cur]:
                if nbr not in labels:
                    labels[nbr] = comp_id
                    stack.append(nbr)
        comp_id += 1
    return labels
