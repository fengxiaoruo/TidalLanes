from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import shortest_path
from shapely.geometry import Point

from .build_raw_connected_v1_grid_travel_time import (
    DEFAULT_BASELINE_VERSION,
    DEFAULT_RAW_CONNECTED_DIR,
    DEFAULT_OUTPUT_ROOT,
    DEFAULT_SPEED_CACHE_ROOT,
    DEFAULT_SPEED_PATH,
    GRID_TYPES,
    PeriodSpeedTables,
    aggregate_speed_by_segment,
    build_adjacent_pairs,
    build_node_lookup,
    clean_road_edges,
    compute_old_adjacent_costs,
    compute_old_grid_costs,
    ensure_dirs,
    load_cached_speed_tables,
    load_grid_3857,
    load_raw_connected_graph,
    normalize_grid_id_series,
    parse_radius_list,
    save_speed_tables,
    summarize_comparison,
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Build grid-to-grid travel time from the raw_connected_v1 network using road-edge projection connectors."
    )
    parser.add_argument("--raw-network-dir", default=str(DEFAULT_RAW_CONNECTED_DIR))
    parser.add_argument("--baseline-version-root", default=str(DEFAULT_BASELINE_VERSION))
    parser.add_argument("--speed-path", default=str(DEFAULT_SPEED_PATH))
    parser.add_argument(
        "--output-root",
        default=str(Path(str(DEFAULT_OUTPUT_ROOT)).with_name("raw_connected_v1_grid_travel_time_edgeproj_v2_traffic")),
    )
    parser.add_argument(
        "--speed-cache-dir",
        default=str(DEFAULT_SPEED_CACHE_ROOT),
        help="Optional existing data dir containing cached road-segment speed tables.",
    )
    parser.add_argument("--grid-type", choices=("all", *GRID_TYPES), default="all")
    parser.add_argument("--period", choices=("AM", "PM", "FF"), default="AM")
    parser.add_argument("--connector-count", type=int, default=3)
    parser.add_argument("--connector-max-dist-m", type=float, default=2500.0)
    parser.add_argument("--min-edge-time-min", type=float, default=0.02)
    parser.add_argument("--speed-chunksize", type=int, default=500_000)
    parser.add_argument("--pair-scope", choices=("all", "adjacent"), default="adjacent")
    parser.add_argument("--comparison-level", choices=("od", "adjacent"), default="adjacent")
    parser.add_argument("--centroid-search-radii-m", default="300,600,900,1500,2500")
    parser.add_argument("--connector-min-node-degree", type=int, default=1)
    parser.add_argument("--allow-low-degree-fallback", action="store_true")
    parser.add_argument("--projection-frac-round", type=int, default=6)
    parser.add_argument(
        "--pair-specific",
        action="store_true",
        help="Use pair-specific shortest path: only home/work connector nodes enter each pair's graph.",
    )
    parser.add_argument("--bpr-alpha", type=float, default=0.15)
    parser.add_argument("--bpr-beta", type=float, default=4.0)
    parser.add_argument("--cap-per-lane-hr", type=float, default=1900.0)
    parser.add_argument("--k-factor", type=float, default=0.10,
                        help="AADT-to-peak-hour ratio. Default 0.10 -> AADT = 10 x peak-hour flow.")
    return parser.parse_args()


def build_edge_projection_connectors(
    grid_3857: gpd.GeoDataFrame,
    road_edges: gpd.GeoDataFrame,
    node_df: pd.DataFrame,
    connector_count: int,
    connector_max_dist_m: float,
    search_radii_m: list[float],
    connector_min_node_degree: int,
    allow_low_degree_fallback: bool,
    frac_round: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    edge_cols = [
        "edge_id",
        "from_node_id",
        "to_node_id",
        "geometry",
        "travel_time_min",
        "length_m",
        "speed_kmh",
    ]
    edge_df = gpd.GeoDataFrame(road_edges[edge_cols].copy(), geometry="geometry", crs=road_edges.crs)
    degree_lookup = node_df.set_index("node_id")["degree"].to_dict()
    edge_df["from_degree"] = edge_df["from_node_id"].map(degree_lookup).fillna(0).astype(int)
    edge_df["to_degree"] = edge_df["to_node_id"].map(degree_lookup).fillna(0).astype(int)
    edge_df["max_endpoint_degree"] = edge_df[["from_degree", "to_degree"]].max(axis=1)
    edge_df["is_leaf_edge"] = edge_df["max_endpoint_degree"] <= 1
    sindex = edge_df.sindex

    records = []
    for grid_row in grid_3857.itertuples(index=False):
        chosen = pd.DataFrame()
        pt = Point(float(grid_row.cx), float(grid_row.cy))
        for radius in search_radii_m:
            if radius > connector_max_dist_m:
                continue
            cand_idx = list(sindex.intersection(pt.buffer(radius).bounds))
            if not cand_idx:
                continue
            cand = edge_df.iloc[cand_idx].copy()
            cand["connector_dist_m"] = cand.geometry.distance(pt)
            cand = cand[np.isfinite(cand["connector_dist_m"]) & (cand["connector_dist_m"] <= radius)].copy()
            if cand.empty:
                continue
            cand["eligible_degree"] = cand["max_endpoint_degree"] >= connector_min_node_degree
            cand["edge_penalty"] = np.where(cand["eligible_degree"], 0, 1)
            cand["leaf_penalty"] = np.where(cand["is_leaf_edge"], 1, 0)
            cand["proj_dist_along_m"] = cand.geometry.map(lambda geom: float(geom.project(pt)))
            cand["proj_fraction"] = cand["proj_dist_along_m"] / cand["length_m"].clip(lower=1e-9)
            cand["proj_fraction"] = cand["proj_fraction"].clip(lower=0.0, upper=1.0)
            cand["proj_fraction_round"] = cand["proj_fraction"].round(frac_round)
            cand["proj_geom"] = cand.geometry.map(
                lambda geom, d=float("nan"): geom.interpolate(d)  # placeholder, reset below
            )
            cand["proj_geom"] = [
                geom.interpolate(dist) for geom, dist in zip(cand.geometry, cand["proj_dist_along_m"])
            ]
            cand["proj_x_m"] = cand["proj_geom"].map(lambda g: float(g.x))
            cand["proj_y_m"] = cand["proj_geom"].map(lambda g: float(g.y))
            cand = cand.sort_values(
                ["edge_penalty", "leaf_penalty", "connector_dist_m", "max_endpoint_degree", "speed_kmh"],
                ascending=[True, True, True, False, False],
            )
            cand = cand.drop_duplicates(subset=["edge_id", "proj_fraction_round"], keep="first")
            eligible = cand[cand["eligible_degree"]].copy()
            if len(eligible) >= connector_count:
                chosen = eligible.head(connector_count).copy()
                break
            if allow_low_degree_fallback:
                chosen = cand.head(connector_count).copy()
                if len(chosen) >= connector_count:
                    break
        if chosen.empty:
            continue
        for rank, row in enumerate(chosen.itertuples(index=False), start=1):
            connector_time = 60.0 * (float(row.connector_dist_m) / 1000.0) / float(row.speed_kmh)
            records.append(
                {
                    "grid_id": grid_row.grid_id,
                    "host_edge_id": row.edge_id,
                    "from_node_id": int(row.from_node_id),
                    "to_node_id": int(row.to_node_id),
                    "connector_rank": rank,
                    "connector_dist_m": float(row.connector_dist_m),
                    "connector_speed_kmh": float(row.speed_kmh),
                    "connector_time_min": connector_time,
                    "cx": float(grid_row.cx),
                    "cy": float(grid_row.cy),
                    "access_x_m": float(row.proj_x_m),
                    "access_y_m": float(row.proj_y_m),
                    "access_fraction": float(row.proj_fraction),
                    "access_fraction_round": float(row.proj_fraction_round),
                    "max_endpoint_degree": int(row.max_endpoint_degree),
                    "eligible_degree": int(bool(row.eligible_degree)),
                    "is_leaf_edge": int(bool(row.is_leaf_edge)),
                    "host_edge_speed_kmh": float(row.speed_kmh),
                }
            )
    connectors = pd.DataFrame(records)
    if connectors.empty:
        raise RuntimeError("No edge-projection connectors built.")
    grid_nodes = (
        connectors.groupby("grid_id", as_index=False)
        .agg(
            n_connectors=("host_edge_id", "size"),
            min_connector_dist_m=("connector_dist_m", "min"),
            mean_connector_dist_m=("connector_dist_m", "mean"),
            cx=("cx", "first"),
            cy=("cy", "first"),
        )
    )
    return connectors, grid_nodes


def build_split_graph(
    road_edges: gpd.GeoDataFrame,
    connectors: pd.DataFrame,
    frac_round: int,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[int, int], dict[str, int]]:
    road_node_ids = sorted(pd.unique(pd.concat([road_edges["from_node_id"], road_edges["to_node_id"]], ignore_index=True)).tolist())
    road_node_map = {int(nid): idx for idx, nid in enumerate(road_node_ids)}

    access = connectors[[
        "host_edge_id",
        "access_fraction_round",
        "access_fraction",
        "access_x_m",
        "access_y_m",
    ]].drop_duplicates().copy()
    access = access.sort_values(["host_edge_id", "access_fraction_round"]).reset_index(drop=True)
    access["access_node_id"] = [f"ap_{i}" for i in range(len(access))]
    access_node_map = {
        row.access_node_id: len(road_node_map) + i
        for i, row in enumerate(access.itertuples(index=False))
    }
    access_lookup = {
        (row.host_edge_id, float(row.access_fraction_round)): row.access_node_id
        for row in access.itertuples(index=False)
    }

    split_rows = []
    access_group = defaultdict(list)
    for row in access.itertuples(index=False):
        access_group[row.host_edge_id].append((float(row.access_fraction_round), float(row.access_fraction), row.access_node_id))

    for edge_row in road_edges.itertuples(index=False):
        edge_key = edge_row.edge_id
        host_access = sorted(access_group.get(edge_key, []), key=lambda x: x[1])
        if not host_access:
            split_rows.append(
                {
                    "host_edge_id": edge_key,
                    "from_graph_node": road_node_map[int(edge_row.from_node_id)],
                    "to_graph_node": road_node_map[int(edge_row.to_node_id)],
                    "seg_fraction_start": 0.0,
                    "seg_fraction_end": 1.0,
                    "travel_time_min": float(edge_row.travel_time_min),
                    "length_m": float(edge_row.length_m),
                    "geometry": edge_row.geometry,
                    "edge_type": "road",
                }
            )
            continue

        fractions = [(0.0, road_node_map[int(edge_row.from_node_id)])]
        for _, frac_exact, access_node_id in host_access:
            frac = min(max(float(frac_exact), 0.0), 1.0)
            if frac <= 1e-8 or frac >= 1 - 1e-8:
                continue
            fractions.append((frac, access_node_map[access_node_id]))
        fractions.append((1.0, road_node_map[int(edge_row.to_node_id)]))
        fractions = sorted(fractions, key=lambda x: x[0])
        dedup = [fractions[0]]
        for frac, node_idx in fractions[1:]:
            if abs(frac - dedup[-1][0]) < 1e-8 and node_idx == dedup[-1][1]:
                continue
            dedup.append((frac, node_idx))
        for (fa, na), (fb, nb) in zip(dedup[:-1], dedup[1:]):
            if fb <= fa:
                continue
            seg_geom = edge_row.geometry.interpolate(fa, normalized=True)
            seg_geom2 = edge_row.geometry.interpolate(fb, normalized=True)
            # approximate subsegment by cutting with interpolation on original geometry
            line = edge_row.geometry
            start_d = line.length * fa
            end_d = line.length * fb
            coords = [line.interpolate(start_d).coords[0]]
            for c in line.coords:
                p = Point(c)
                d = line.project(p)
                if start_d < d < end_d:
                    coords.append(c)
            coords.append(line.interpolate(end_d).coords[0])
            split_rows.append(
                {
                    "host_edge_id": edge_key,
                    "from_graph_node": na,
                    "to_graph_node": nb,
                    "seg_fraction_start": fa,
                    "seg_fraction_end": fb,
                    "travel_time_min": float(edge_row.travel_time_min) * (fb - fa),
                    "length_m": float(edge_row.length_m) * (fb - fa),
                    "geometry": gpd.GeoSeries.from_wkt([Point(coords[0]).wkt]).iloc[0] if len(coords) < 2 else None,
                    "edge_type": "road",
                }
            )
            if split_rows[-1]["geometry"] is None:
                from shapely.geometry import LineString

                split_rows[-1]["geometry"] = LineString(coords)

    split_edges = gpd.GeoDataFrame(split_rows, geometry="geometry", crs=3857)
    connectors = connectors.copy()
    connectors["access_node_id"] = connectors.apply(
        lambda r: access_lookup[(r["host_edge_id"], float(round(r["access_fraction"], frac_round)))], axis=1
    )
    connectors["access_graph_node"] = connectors["access_node_id"].map(access_node_map).astype(int)
    return split_edges.reset_index(drop=True), connectors.reset_index(drop=True), road_node_map, access_node_map


def build_graph_csr(split_edges: gpd.GeoDataFrame, n_total: int):
    rows = split_edges["from_graph_node"].to_numpy(dtype=int)
    cols = split_edges["to_graph_node"].to_numpy(dtype=int)
    w = split_edges["travel_time_min"].to_numpy(dtype=float)
    return coo_matrix((w, (rows, cols)), shape=(n_total, n_total)).tocsr()


def build_raw_edge_lane_lookup(baseline_root: Path) -> tuple[dict[str, float], float]:
    data_dir = baseline_root / "data"
    match_path = data_dir / "raw_to_centerline_match_master.parquet"
    lane_path = data_dir / "centerline_lane_master.parquet"
    seg_path = data_dir / "raw_segment_master.parquet"

    lane_master = pd.read_parquet(lane_path)[["cline_id", "dir", "lane_est_length_weighted"]].copy()
    lane_master["lane_est_length_weighted"] = pd.to_numeric(
        lane_master["lane_est_length_weighted"], errors="coerce"
    )
    lane_master = lane_master[lane_master["lane_est_length_weighted"].notna()].copy()

    match = pd.read_parquet(match_path)[
        ["raw_edge_id", "matched_final", "cline_id_final", "dir_final", "s_from", "s_to"]
    ].copy()
    match = match[pd.to_numeric(match["matched_final"], errors="coerce") == 1].copy()
    match["matched_length_m"] = pd.to_numeric(match["s_to"], errors="coerce") - pd.to_numeric(
        match["s_from"], errors="coerce"
    )
    match["matched_length_m"] = pd.to_numeric(match["matched_length_m"], errors="coerce")
    match = match[match["matched_length_m"] > 0].copy()
    match = match.merge(
        lane_master,
        left_on=["cline_id_final", "dir_final"],
        right_on=["cline_id", "dir"],
        how="left",
    )
    match = match[match["lane_est_length_weighted"].notna()].copy()
    match["lane_w"] = match["lane_est_length_weighted"] * match["matched_length_m"]
    lane_by_match = (
        match.groupby("raw_edge_id", as_index=False)
        .agg(lane_w=("lane_w", "sum"), len_w=("matched_length_m", "sum"))
    )
    lane_by_match["lane_est"] = np.where(
        lane_by_match["len_w"] > 0,
        lane_by_match["lane_w"] / lane_by_match["len_w"],
        np.nan,
    )

    seg = pd.read_parquet(seg_path)
    road_class_source = "road_class" if "road_class" in seg.columns else "roadtype"
    seg["road_class_num"] = pd.to_numeric(seg.get(road_class_source), errors="coerce")
    seg["lane_est_segment"] = pd.to_numeric(seg.get("road_class_lane_mean"), errors="coerce")
    seg["lane_est_segment"] = seg["lane_est_segment"].fillna(seg["road_class_num"].map({2: 6.0, 3: 4.0, 4: 2.0}))
    seg["length_m"] = pd.to_numeric(seg.get("length_m"), errors="coerce")
    seg_use = seg[
        seg["raw_edge_id"].notna() & seg["lane_est_segment"].notna() & seg["length_m"].notna() & (seg["length_m"] > 0)
    ].copy()
    seg_use["lane_w"] = seg_use["lane_est_segment"] * seg_use["length_m"]
    lane_by_seg = (
        seg_use.groupby("raw_edge_id", as_index=False)
        .agg(lane_w=("lane_w", "sum"), len_w=("length_m", "sum"))
    )
    lane_by_seg["lane_est"] = np.where(
        lane_by_seg["len_w"] > 0,
        lane_by_seg["lane_w"] / lane_by_seg["len_w"],
        np.nan,
    )

    lane_lookup: dict[str, float] = {}
    for r in lane_by_seg.itertuples(index=False):
        if np.isfinite(r.lane_est):
            lane_lookup[str(r.raw_edge_id)] = float(r.lane_est)
    for r in lane_by_match.itertuples(index=False):
        if np.isfinite(r.lane_est):
            lane_lookup[str(r.raw_edge_id)] = float(r.lane_est)

    lane_values = np.array([v for v in lane_lookup.values() if np.isfinite(v)], dtype=float)
    lane_fallback = float(np.nanmean(lane_values)) if lane_values.size else 2.0
    if not np.isfinite(lane_fallback) or lane_fallback <= 0:
        lane_fallback = 2.0
    return lane_lookup, lane_fallback


def edge_aadt_proxy(
    tau_obs_min: np.ndarray,
    tau_ff_min: np.ndarray,
    lanes: np.ndarray,
    alpha: float = 0.15,
    beta: float = 4.0,
    cap_per_lane_hr: float = 1900.0,
    k_factor: float = 0.10,
) -> np.ndarray:
    """BPR-inverted AADT proxy per directed road edge.

    flow_hr = lanes * cap_per_lane_hr * ((max(0, tau_obs/tau_ff - 1) / alpha) ** (1/beta))
    AADT    = flow_hr / k_factor
    """
    tau_obs_min = np.asarray(tau_obs_min, dtype=float)
    tau_ff_min = np.asarray(tau_ff_min, dtype=float)
    lanes = np.asarray(lanes, dtype=float)
    safe_ff = np.where(np.isfinite(tau_ff_min) & (tau_ff_min > 0), tau_ff_min, np.nan)
    ratio = tau_obs_min / safe_ff
    excess = np.where(np.isfinite(ratio), np.maximum(0.0, ratio - 1.0) / max(alpha, 1e-9), np.nan)
    qc = np.where(np.isfinite(excess), excess ** (1.0 / max(beta, 1e-9)), np.nan)
    flow_hr = lanes * cap_per_lane_hr * qc
    aadt = flow_hr / max(k_factor, 1e-9)
    aadt = np.where(np.isfinite(aadt) & (aadt >= 0), aadt, 0.0)
    return aadt


def compute_grid_costs_adjacent_pairwise(
    road_edges: gpd.GeoDataFrame,
    connectors: pd.DataFrame,
    road_node_map: dict[int, int],
    adjacent_pairs: pd.DataFrame,
    lane_lookup: dict[str, float],
    lane_fallback: float,
    aadt_lookup: dict[str, float] | None = None,
    aadt_fallback: float = 0.0,
) -> pd.DataFrame:
    """Pair-specific shortest path using edge-projection access points.

    For each adjacent (home, work) pair:
    - HOME virtual node connects to downstream road nodes of home access points:
        cost = connector_time + host_edge_time * (1 - access_fraction)
    - Upstream road nodes of work access points connect to WORK virtual node:
        cost = host_edge_time * access_fraction + connector_time
    - Pure road graph handles all intermediate routing; no other grid nodes present.
    Returns travel_time_min, route_length_m, route_avg_speed_kmh,
    and path-based lane estimates per pair.
    """
    edge_time_lookup = road_edges.set_index("edge_id")["travel_time_min"].to_dict()
    edge_length_lookup = road_edges.set_index("edge_id")["length_m"].to_dict()
    road_lane = road_edges["edge_id"].astype(str).map(lane_lookup)
    road_lane = pd.to_numeric(road_lane, errors="coerce").fillna(lane_fallback)
    road_lane = np.where(np.isfinite(road_lane) & (road_lane > 0), road_lane, lane_fallback)
    aadt_lookup = aadt_lookup or {}
    road_aadt = road_edges["edge_id"].astype(str).map(aadt_lookup)
    road_aadt = pd.to_numeric(road_aadt, errors="coerce").fillna(aadt_fallback).to_numpy(dtype=float)
    connectors = connectors.copy()
    connectors["host_edge_time_min"] = connectors["host_edge_id"].map(edge_time_lookup).fillna(0.0)
    connectors["host_edge_length_m"] = connectors["host_edge_id"].map(edge_length_lookup).fillna(0.0)
    connectors["outbound_edge_time"] = connectors["host_edge_time_min"] * (1.0 - connectors["access_fraction"].clip(0.0, 1.0))
    connectors["inbound_edge_time"] = connectors["host_edge_time_min"] * connectors["access_fraction"].clip(0.0, 1.0)
    connectors["outbound_edge_length"] = connectors["host_edge_length_m"] * (1.0 - connectors["access_fraction"].clip(0.0, 1.0))
    connectors["inbound_edge_length"] = connectors["host_edge_length_m"] * connectors["access_fraction"].clip(0.0, 1.0)

    N_road = len(road_node_map)
    road_rows = road_edges["from_node_id"].map(road_node_map).to_numpy(dtype=int)
    road_cols = road_edges["to_node_id"].map(road_node_map).to_numpy(dtype=int)
    road_wts = road_edges["travel_time_min"].to_numpy(dtype=float)
    road_lens = road_edges["length_m"].to_numpy(dtype=float)
    road_lane_vals = np.asarray(road_lane, dtype=float)

    conn_by_grid = {gid: grp for gid, grp in connectors.groupby("grid_id")}

    HOME_IDX = N_road
    WORK_IDX = N_road + 1
    n_total = N_road + 2

    out_rows = []
    for pair in adjacent_pairs.itertuples(index=False):
        home, work = str(pair.home_grid), str(pair.work_grid)
        if home not in conn_by_grid or work not in conn_by_grid:
            out_rows.append({
                "home_grid": home, "work_grid": work,
                "raw_connected_travel_time_min": np.nan,
                "route_length_m": np.nan,
                "route_avg_speed_kmh": np.nan,
                "lane_est_path_raw_edgeproj_len_weighted": np.nan,
                "lane_est_path_raw_edgeproj_bottleneck": np.nan,
                "path_total_road_length_m": np.nan,
                "path_total_lanemiles_m_lane": np.nan,
                "path_avg_aadt_proxy_len_weighted": np.nan,
            })
            continue
        home_conn = conn_by_grid[home]
        work_conn = conn_by_grid[work]
        # HOME -> downstream road node (connector dist + outbound road portion)
        ci_h = np.full(len(home_conn), HOME_IDX, dtype=int)
        cj_h = home_conn["to_node_id"].map(road_node_map).to_numpy(dtype=int)
        cw_h = (home_conn["connector_time_min"] + home_conn["outbound_edge_time"]).to_numpy(dtype=float)
        cl_h = (home_conn["connector_dist_m"] + home_conn["outbound_edge_length"]).to_numpy(dtype=float)
        cl_h_road = home_conn["outbound_edge_length"].to_numpy(dtype=float)
        lane_h = home_conn["host_edge_id"].astype(str).map(lane_lookup)
        lane_h = pd.to_numeric(lane_h, errors="coerce").fillna(lane_fallback).to_numpy(dtype=float)
        aadt_h = home_conn["host_edge_id"].astype(str).map(aadt_lookup)
        aadt_h = pd.to_numeric(aadt_h, errors="coerce").fillna(aadt_fallback).to_numpy(dtype=float)
        # upstream road node -> WORK (inbound road portion + connector dist)
        ci_w = work_conn["from_node_id"].map(road_node_map).to_numpy(dtype=int)
        cj_w = np.full(len(work_conn), WORK_IDX, dtype=int)
        cw_w = (work_conn["inbound_edge_time"] + work_conn["connector_time_min"]).to_numpy(dtype=float)
        cl_w = (work_conn["inbound_edge_length"] + work_conn["connector_dist_m"]).to_numpy(dtype=float)
        cl_w_road = work_conn["inbound_edge_length"].to_numpy(dtype=float)
        lane_w = work_conn["host_edge_id"].astype(str).map(lane_lookup)
        lane_w = pd.to_numeric(lane_w, errors="coerce").fillna(lane_fallback).to_numpy(dtype=float)
        aadt_w = work_conn["host_edge_id"].astype(str).map(aadt_lookup)
        aadt_w = pd.to_numeric(aadt_w, errors="coerce").fillna(aadt_fallback).to_numpy(dtype=float)
        all_rows = np.concatenate([road_rows, ci_h, ci_w])
        all_cols = np.concatenate([road_cols, cj_h, cj_w])
        all_wts = np.concatenate([road_wts, cw_h, cw_w])
        all_lens = np.concatenate([road_lens, cl_h, cl_w])
        all_road_lens = np.concatenate([road_lens, cl_h_road, cl_w_road])
        all_lane_vals = np.concatenate([road_lane_vals, lane_h, lane_w])
        all_aadt_vals = np.concatenate([road_aadt, aadt_h, aadt_w])
        # (from_node, to_node) -> length lookup for path tracing; first occurrence wins
        len_dict: dict[tuple[int, int], float] = {}
        road_len_dict: dict[tuple[int, int], float] = {}
        lane_dict: dict[tuple[int, int], float] = {}
        aadt_dict: dict[tuple[int, int], float] = {}
        for fr, to, ln, rln, lnv, av in zip(
            all_rows.tolist(),
            all_cols.tolist(),
            all_lens.tolist(),
            all_road_lens.tolist(),
            all_lane_vals.tolist(),
            all_aadt_vals.tolist(),
        ):
            key = (int(fr), int(to))
            if key not in len_dict:
                len_dict[key] = float(ln)
                road_len_dict[key] = float(rln)
                lane_dict[key] = float(lnv)
                aadt_dict[key] = float(av)
        graph = coo_matrix((all_wts, (all_rows, all_cols)), shape=(n_total, n_total)).tocsr()
        dist_result, pred_result = shortest_path(
            graph, directed=True, indices=[HOME_IDX], unweighted=False, method="D",
            return_predecessors=True,
        )
        dist_row = dist_result[0]
        pred_row = pred_result[0]
        tt = float(dist_row[WORK_IDX]) if np.isfinite(dist_row[WORK_IDX]) else np.nan
        route_length_m: float = np.nan
        lane_est_path_weighted: float = np.nan
        lane_est_path_bottleneck: float = np.nan
        path_total_road_length_m: float = np.nan
        path_total_lanemiles_m_lane: float = np.nan
        path_avg_aadt_proxy_len_weighted: float = np.nan
        if np.isfinite(tt):
            path_nodes: list[int] = []
            cur = WORK_IDX
            itr = 0
            while cur != HOME_IDX and cur != -9999 and itr <= n_total:
                path_nodes.append(cur)
                cur = int(pred_row[cur])
                itr += 1
            if cur == HOME_IDX:
                path_nodes.append(HOME_IDX)
                path_nodes = list(reversed(path_nodes))
                arc_keys = [(path_nodes[i], path_nodes[i + 1]) for i in range(len(path_nodes) - 1)]
                route_length_m = float(sum(len_dict.get(k, 0.0) for k in arc_keys))
                lane_weight_sum = 0.0
                lane_len_sum = 0.0
                aadt_weight_sum = 0.0
                lane_vals_path: list[float] = []
                for k in arc_keys:
                    seg_road_len = float(road_len_dict.get(k, 0.0))
                    seg_lane = float(lane_dict.get(k, np.nan))
                    seg_aadt = float(aadt_dict.get(k, 0.0))
                    if seg_road_len > 0 and np.isfinite(seg_lane) and seg_lane > 0:
                        lane_weight_sum += seg_road_len * seg_lane
                        lane_len_sum += seg_road_len
                        aadt_weight_sum += seg_road_len * (seg_aadt if np.isfinite(seg_aadt) else 0.0)
                        lane_vals_path.append(seg_lane)
                if lane_len_sum > 0:
                    lane_est_path_weighted = lane_weight_sum / lane_len_sum
                    path_total_road_length_m = lane_len_sum
                    path_total_lanemiles_m_lane = lane_weight_sum
                    path_avg_aadt_proxy_len_weighted = aadt_weight_sum / lane_len_sum
                if lane_vals_path:
                    lane_est_path_bottleneck = float(np.min(lane_vals_path))
        avg_speed = (
            (route_length_m / 1000.0) / (tt / 60.0)
            if np.isfinite(tt) and tt > 0 and np.isfinite(route_length_m) and route_length_m > 0
            else np.nan
        )
        out_rows.append({
            "home_grid": home,
            "work_grid": work,
            "raw_connected_travel_time_min": tt,
            "route_length_m": route_length_m,
            "route_avg_speed_kmh": avg_speed,
            "lane_est_path_raw_edgeproj_len_weighted": lane_est_path_weighted,
            "lane_est_path_raw_edgeproj_bottleneck": lane_est_path_bottleneck,
            "path_total_road_length_m": path_total_road_length_m,
            "path_total_lanemiles_m_lane": path_total_lanemiles_m_lane,
            "path_avg_aadt_proxy_len_weighted": path_avg_aadt_proxy_len_weighted,
        })
    return pd.DataFrame(out_rows)


def compute_grid_costs_pairwise(graph, connectors: pd.DataFrame) -> pd.DataFrame:
    grid_ids = sorted(connectors["grid_id"].astype(str).unique().tolist())
    conn_by_grid = {
        gid: grp[["access_graph_node", "connector_time_min"]].copy()
        for gid, grp in connectors.groupby("grid_id")
    }
    rows = []
    for home_grid in grid_ids:
        src = conn_by_grid[home_grid]
        src_idx = src["access_graph_node"].to_numpy(dtype=int)
        src_ct = src["connector_time_min"].to_numpy(dtype=float)
        dist = shortest_path(graph, directed=True, indices=src_idx, unweighted=False, method="D")
        dist = np.asarray(dist, dtype=float)
        if dist.ndim == 1:
            dist = dist.reshape(1, -1)
        dist_from_home = np.min(dist + src_ct[:, None], axis=0)
        for work_grid in grid_ids:
            dst = conn_by_grid[work_grid]
            dst_idx = dst["access_graph_node"].to_numpy(dtype=int)
            dst_ct = dst["connector_time_min"].to_numpy(dtype=float)
            vals = dist_from_home[dst_idx] + dst_ct
            tt = float(np.nanmin(vals)) if len(vals) else np.nan
            if not np.isfinite(tt):
                tt = np.nan
            rows.append(
                {
                    "home_grid": home_grid,
                    "work_grid": work_grid,
                    "raw_connected_travel_time_min": tt,
                }
            )
    return pd.DataFrame(rows)


def run_for_grid(
    grid_type: str,
    baseline_root: Path,
    output_root: Path,
    period: str,
    node_df: pd.DataFrame,
    road_edges: gpd.GeoDataFrame,
    connector_count: int,
    connector_max_dist_m: float,
    pair_scope: str,
    comparison_level: str,
    search_radii_m: list[float],
    connector_min_node_degree: int,
    allow_low_degree_fallback: bool,
    frac_round: int,
    pair_specific: bool = False,
    lane_lookup: dict[str, float] | None = None,
    lane_fallback: float = 2.0,
    aadt_lookup: dict[str, float] | None = None,
    aadt_fallback: float = 0.0,
):
    data_dir, metrics_dir = ensure_dirs(output_root)
    grid_3857 = load_grid_3857(baseline_root=baseline_root, grid_type=grid_type)
    adjacent_pairs = build_adjacent_pairs(grid_3857)
    connectors, grid_nodes = build_edge_projection_connectors(
        grid_3857=grid_3857,
        road_edges=road_edges,
        node_df=node_df,
        connector_count=connector_count,
        connector_max_dist_m=connector_max_dist_m,
        search_radii_m=search_radii_m,
        connector_min_node_degree=connector_min_node_degree,
        allow_low_degree_fallback=allow_low_degree_fallback,
        frac_round=frac_round,
    )
    connectors.to_csv(data_dir / f"raw_connected_grid_connectors_{pair_scope}_{grid_type}.csv", index=False)
    if pair_specific:
        road_node_ids = sorted(
            pd.unique(pd.concat([road_edges["from_node_id"], road_edges["to_node_id"]], ignore_index=True)).tolist()
        )
        road_node_map = {int(nid): idx for idx, nid in enumerate(road_node_ids)}
        raw_connected_cost = compute_grid_costs_adjacent_pairwise(
            road_edges=road_edges,
            connectors=connectors,
            road_node_map=road_node_map,
            adjacent_pairs=adjacent_pairs,
            lane_lookup=lane_lookup or {},
            lane_fallback=lane_fallback,
            aadt_lookup=aadt_lookup or {},
            aadt_fallback=aadt_fallback,
        )
    else:
        split_edges, connectors, road_node_map, access_node_map = build_split_graph(
            road_edges=road_edges,
            connectors=connectors,
            frac_round=frac_round,
        )
        split_edges.to_parquet(data_dir / f"raw_connected_split_edges_{grid_type}.parquet", index=False)
        graph = build_graph_csr(split_edges, len(road_node_map) + len(access_node_map))
        raw_connected_cost = compute_grid_costs_pairwise(graph, connectors)
    if pair_scope == "adjacent":
        raw_connected_cost = adjacent_pairs.merge(raw_connected_cost, on=["home_grid", "work_grid"], how="left")
    if comparison_level == "adjacent":
        old_cost = compute_old_adjacent_costs(baseline_root=baseline_root, grid_type=grid_type)
    else:
        old_cost = compute_old_grid_costs(baseline_root=baseline_root, grid_type=grid_type)
        if pair_scope == "adjacent":
            old_cost = adjacent_pairs.merge(old_cost, on=["home_grid", "work_grid"], how="left")
    comp = old_cost.merge(raw_connected_cost, on=["home_grid", "work_grid"], how="left")
    comp["diff_min"] = comp["raw_connected_travel_time_min"] - comp["old_travel_time_min"]
    comp["ratio_raw_connected_over_old"] = comp["raw_connected_travel_time_min"] / comp["old_travel_time_min"]
    summary = summarize_comparison(comp, grid_type, comparison_level=comparison_level)
    connector_summary = pd.DataFrame(
        [
            {
                "grid_type": grid_type,
                "comparison_level": comparison_level,
                "pair_scope": pair_scope,
                "total_grids": int(grid_3857["grid_id"].nunique()),
                "grids_with_connectors": int(grid_nodes["grid_id"].nunique()),
                "grids_without_connectors": int(grid_3857["grid_id"].nunique() - grid_nodes["grid_id"].nunique()),
                "connector_rows": int(len(connectors)),
                "mean_connector_dist_m": float(connectors["connector_dist_m"].mean()),
                "p90_connector_dist_m": float(connectors["connector_dist_m"].quantile(0.90)),
                "share_leaf_connectors": float(connectors["is_leaf_edge"].mean()),
                "share_below_min_degree_connectors": float(1.0 - connectors["eligible_degree"].mean()),
                "adjacent_pair_count": int(len(adjacent_pairs)),
                "connector_min_node_degree": int(connector_min_node_degree),
                "allow_low_degree_fallback": int(bool(allow_low_degree_fallback)),
                "pair_specific_routing": int(pair_specific),
                "connector_speed_rule": "host_edge_period_speed_only",
                "connector_geometry_rule": "edge_projection_access_point",
            }
        ]
    )
    suffix = f"{comparison_level}_{pair_scope}_{grid_type}_{period}"
    comp.to_csv(data_dir / f"raw_connected_vs_old_{suffix}.csv", index=False)
    adjacent_pairs.to_csv(data_dir / f"grid_adjacent_pairs_{grid_type}.csv", index=False)
    summary.to_csv(metrics_dir / f"raw_connected_vs_old_summary_{suffix}.csv", index=False)
    connector_summary.to_csv(metrics_dir / f"raw_connected_connector_summary_{pair_scope}_{grid_type}.csv", index=False)
    return summary, connector_summary


def main():
    args = parse_args()
    raw_network_dir = Path(args.raw_network_dir)
    baseline_root = Path(args.baseline_version_root)
    output_root = Path(args.output_root)
    speed_cache_dir = Path(args.speed_cache_dir) if args.speed_cache_dir else None
    search_radii_m = parse_radius_list(args.centroid_search_radii_m)
    if not search_radii_m:
        raise ValueError("centroid_search_radii_m must provide at least one positive radius")

    data_dir, metrics_dir = ensure_dirs(output_root)
    speed_tables = load_cached_speed_tables(data_dir)
    if speed_tables is None and speed_cache_dir is not None and speed_cache_dir != data_dir:
        speed_tables = load_cached_speed_tables(speed_cache_dir)
    if speed_tables is None:
        print("[raw-connected-edgeproj] aggregating raw speed by road segment", flush=True)
        speed_tables = aggregate_speed_by_segment(Path(args.speed_path), chunksize=args.speed_chunksize)
        save_speed_tables(speed_tables, data_dir)
    else:
        print("[raw-connected-edgeproj] using cached road-segment speed tables", flush=True)
        save_speed_tables(speed_tables, data_dir)
    print("[raw-connected-edgeproj] loading raw_connected_v1 graph", flush=True)
    nodes, edges, speed_stats = load_raw_connected_graph(raw_network_dir, speed_tables, args.period)
    road_edges = clean_road_edges(edges, min_edge_time_min=args.min_edge_time_min)
    road_geom = gpd.GeoSeries.from_wkt(road_edges["geometry"], crs="EPSG:4326", on_invalid="ignore")
    valid_geom = road_geom.notna() & (~road_geom.is_empty)
    road_edges = gpd.GeoDataFrame(road_edges.loc[valid_geom].copy(), geometry=road_geom.loc[valid_geom], crs="EPSG:4326").to_crs(3857)
    road_edges.to_csv(data_dir / f"raw_connected_road_edges_{args.period}.csv", index=False)
    node_df = build_node_lookup(nodes, road_edges)
    node_df.to_csv(data_dir / "raw_connected_road_nodes_usable.csv", index=False)
    lane_lookup, lane_fallback = build_raw_edge_lane_lookup(baseline_root=baseline_root)

    ff_table = speed_tables.ff[["roadseg_id", "speed_ff_kmh"]].drop_duplicates("roadseg_id").copy()
    ff_table["roadseg_id"] = ff_table["roadseg_id"].astype("string")
    road_edges["roadseg_id"] = road_edges["road_id"].astype("string")
    road_edges = road_edges.merge(ff_table, on="roadseg_id", how="left")
    road_edges["speed_ff_kmh"] = pd.to_numeric(road_edges["speed_ff_kmh"], errors="coerce")
    ff_missing = ~np.isfinite(road_edges["speed_ff_kmh"]) | (road_edges["speed_ff_kmh"] <= 0)
    road_edges.loc[ff_missing, "speed_ff_kmh"] = pd.to_numeric(road_edges.loc[ff_missing, "speed_kmh"], errors="coerce")
    road_edges["tau_ff_min"] = 60.0 * (road_edges["length_m"] / 1000.0) / road_edges["speed_ff_kmh"]
    road_edges["tau_obs_min"] = pd.to_numeric(road_edges["travel_time_min"], errors="coerce")
    edge_lane_for_aadt = road_edges["edge_id"].astype(str).map(lane_lookup)
    edge_lane_for_aadt = pd.to_numeric(edge_lane_for_aadt, errors="coerce").fillna(lane_fallback).to_numpy(dtype=float)
    road_edges["aadt_proxy"] = edge_aadt_proxy(
        road_edges["tau_obs_min"].to_numpy(dtype=float),
        road_edges["tau_ff_min"].to_numpy(dtype=float),
        edge_lane_for_aadt,
        alpha=args.bpr_alpha,
        beta=args.bpr_beta,
        cap_per_lane_hr=args.cap_per_lane_hr,
        k_factor=args.k_factor,
    )
    aadt_lookup = dict(zip(road_edges["edge_id"].astype(str), road_edges["aadt_proxy"].astype(float)))
    aadt_fallback = float(np.nanmean(road_edges["aadt_proxy"].to_numpy(dtype=float))) if len(road_edges) else 0.0
    if not np.isfinite(aadt_fallback) or aadt_fallback < 0:
        aadt_fallback = 0.0

    grid_types = GRID_TYPES if args.grid_type == "all" else (args.grid_type,)
    summaries = []
    connector_summaries = []
    for grid_type in grid_types:
        print(f"[raw-connected-edgeproj] building grid costs for grid_type={grid_type}", flush=True)
        summary, connector_summary = run_for_grid(
            grid_type=grid_type,
            baseline_root=baseline_root,
            output_root=output_root,
            period=args.period,
            node_df=node_df,
            road_edges=road_edges,
            connector_count=args.connector_count,
            connector_max_dist_m=args.connector_max_dist_m,
            pair_scope=args.pair_scope,
            comparison_level=args.comparison_level,
            search_radii_m=search_radii_m,
            connector_min_node_degree=args.connector_min_node_degree,
            allow_low_degree_fallback=args.allow_low_degree_fallback,
            frac_round=args.projection_frac_round,
            pair_specific=args.pair_specific,
            lane_lookup=lane_lookup,
            lane_fallback=lane_fallback,
            aadt_lookup=aadt_lookup,
            aadt_fallback=aadt_fallback,
        )
        summaries.append(summary)
        connector_summaries.append(connector_summary)

    pd.concat(summaries, ignore_index=True).to_csv(
        metrics_dir / f"raw_connected_vs_old_summary_all_{args.comparison_level}_{args.pair_scope}_{args.period}.csv",
        index=False,
    )
    pd.concat(connector_summaries, ignore_index=True).to_csv(
        metrics_dir / f"raw_connected_connector_summary_all_{args.pair_scope}.csv",
        index=False,
    )
    (output_root / "run_config.json").write_text(
        json.dumps(
            {
                "method": "edge_projection_connector",
                "raw_network_dir": str(raw_network_dir),
                "baseline_root": str(baseline_root),
                "speed_path": str(Path(args.speed_path)),
                "period": args.period,
                "grid_type": args.grid_type,
                "connector_count": args.connector_count,
                "connector_max_dist_m": args.connector_max_dist_m,
                "projection_frac_round": args.projection_frac_round,
                "speed_cache_dir": str(speed_cache_dir) if speed_cache_dir else None,
                "pair_scope": args.pair_scope,
                "comparison_level": args.comparison_level,
                "centroid_search_radii_m": search_radii_m,
                "connector_min_node_degree": args.connector_min_node_degree,
                "allow_low_degree_fallback": args.allow_low_degree_fallback,
                "min_edge_time_min": args.min_edge_time_min,
                "speed_chunksize": args.speed_chunksize,
                "strict_period_speed_only": True,
                "pair_specific_routing": args.pair_specific,
                "connector_speed_rule": "host_edge_period_speed_only",
                "connector_geometry_rule": "edge_projection_access_point",
                "speed_stats": speed_stats,
                "cleaned_road_edges": int(len(road_edges)),
                "usable_road_nodes": int(len(node_df)),
                "path_lane_source": "path-based raw-edge lane proxy from centerline match + road class fallback",
                "path_lane_fallback": lane_fallback,
                "aadt_proxy_source": "BPR_inversion(speed_obs, speed_ff, lanes)",
                "bpr_alpha": args.bpr_alpha,
                "bpr_beta": args.bpr_beta,
                "cap_per_lane_hr": args.cap_per_lane_hr,
                "k_factor_aadt_to_peak_hour": args.k_factor,
                "aadt_proxy_fallback": aadt_fallback,
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    print(f"[raw-connected-edgeproj] complete output_root={output_root}", flush=True)


if __name__ == "__main__":
    main()
