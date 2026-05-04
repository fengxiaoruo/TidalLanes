from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from pyproj import Transformer
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import shortest_path
from scipy.spatial import cKDTree


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_RAW_CONNECTED_DIR = ROOT / "src" / "raw_connected_v1" / "Processed_Data"
DEFAULT_BASELINE_VERSION = ROOT / "outputs" / "raw_rebuild_validation"
DEFAULT_SPEED_PATH = ROOT / "raw_data" / "speed_Beijing_all_wgs84.csv"
DEFAULT_OUTPUT_ROOT = ROOT / "outputs" / "raw_connected_v1_grid_travel_time_v1"
DEFAULT_SPEED_CACHE_ROOT = ROOT / "outputs" / "raw_connected_v1_grid_travel_time_v1" / "data"

GRID_TYPES = ("square", "hex", "voronoi")


@dataclass
class PeriodSpeedTables:
    am: pd.DataFrame
    pm: pd.DataFrame
    ff: pd.DataFrame
    overall: pd.DataFrame


def parse_args():
    parser = argparse.ArgumentParser(
        description="Build grid-to-grid travel time from the raw_connected_v1 directed road network."
    )
    parser.add_argument("--raw-network-dir", default=str(DEFAULT_RAW_CONNECTED_DIR))
    parser.add_argument("--baseline-version-root", default=str(DEFAULT_BASELINE_VERSION))
    parser.add_argument("--speed-path", default=str(DEFAULT_SPEED_PATH))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument(
        "--speed-cache-dir",
        default=str(DEFAULT_SPEED_CACHE_ROOT),
        help="Optional existing data dir containing cached road-segment speed tables.",
    )
    parser.add_argument("--grid-type", choices=("all", *GRID_TYPES), default="all")
    parser.add_argument("--period", choices=("AM", "PM", "FF"), default="AM")
    parser.add_argument(
        "--connector-count",
        type=int,
        default=3,
        help="Attach each grid to this many nearby usable road nodes.",
    )
    parser.add_argument(
        "--connector-max-dist-m",
        type=float,
        default=2500.0,
        help="Maximum centroid-to-node connector distance.",
    )
    parser.add_argument(
        "--connector-speed-kmh",
        type=float,
        default=25.0,
        help="Fallback connector speed when local node speed is unavailable.",
    )
    parser.add_argument(
        "--min-edge-time-min",
        type=float,
        default=0.02,
        help="Optional soft threshold for dropping tiny edge times.",
    )
    parser.add_argument(
        "--speed-chunksize",
        type=int,
        default=500_000,
        help="CSV chunk size for streaming raw speed aggregation.",
    )
    parser.add_argument(
        "--pair-scope",
        choices=("all", "adjacent"),
        default="adjacent",
        help="Whether to export all grid pairs or only adjacent grid pairs.",
    )
    parser.add_argument(
        "--comparison-level",
        choices=("od", "adjacent"),
        default="adjacent",
        help="Compare against old OD shortest-path costs or old adjacent-grid edge costs.",
    )
    parser.add_argument(
        "--centroid-search-radii-m",
        default="300,600,900,1500,2500",
        help="Comma-separated expanding radii used to find centroid-near anchor nodes.",
    )
    parser.add_argument(
        "--connector-min-node-degree",
        type=int,
        default=1,
        help="Minimum road-node degree allowed for grid connectors.",
    )
    parser.add_argument(
        "--allow-low-degree-fallback",
        action="store_true",
        help="If set, allow fallback to lower-degree nodes when no preferred connector is available.",
    )
    return parser.parse_args()


def ensure_dirs(output_root: Path) -> tuple[Path, Path]:
    data_dir = output_root / "data"
    metrics_dir = output_root / "metrics"
    data_dir.mkdir(parents=True, exist_ok=True)
    metrics_dir.mkdir(parents=True, exist_ok=True)
    return data_dir, metrics_dir


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


def parse_speed_datetime(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["day"] = pd.to_numeric(out["day"], errors="coerce")
    out["hour"] = pd.to_numeric(out["hour"], errors="coerce")
    out["hour_int"] = out["hour"].round().astype("Int64")
    out["dt"] = pd.to_datetime(
        out["hour_int"].astype(str),
        format="%Y%m%d%H%M",
        errors="coerce",
    )
    out["hour_of_day"] = out["dt"].dt.hour
    out["is_weekday"] = out["dt"].dt.weekday < 5
    out["is_am_peak"] = (out["hour_of_day"] >= 7) & (out["hour_of_day"] < 9)
    out["is_pm_peak"] = (out["hour_of_day"] >= 17) & (out["hour_of_day"] < 19)
    out["is_freeflow_2205"] = (out["hour_of_day"] >= 22) | (out["hour_of_day"] <= 5)
    return out


def harmonic_mean_speed(speed_series: pd.Series) -> float:
    vals = pd.to_numeric(speed_series, errors="coerce").to_numpy(dtype=float)
    vals = vals[np.isfinite(vals) & (vals > 0)]
    if len(vals) == 0:
        return np.nan
    return float(len(vals) / np.sum(1.0 / vals))


def _update_speed_bucket(
    store: dict[str, list[float]],
    road_ids: pd.Series,
    speeds: pd.Series,
) -> None:
    tmp = pd.DataFrame({"roadseg_id": road_ids.astype("string"), "speed": pd.to_numeric(speeds, errors="coerce")})
    tmp = tmp[tmp["roadseg_id"].notna() & tmp["speed"].gt(0)].copy()
    if tmp.empty:
        return
    grp = tmp.groupby("roadseg_id", as_index=False).agg(
        obs_n=("speed", "size"),
        inv_speed_sum=("speed", lambda s: float(np.sum(1.0 / s.to_numpy(dtype=float)))),
    )
    for row in grp.itertuples(index=False):
        key = str(row.roadseg_id)
        if key not in store:
            store[key] = [0.0, 0.0]
        store[key][0] += float(row.obs_n)
        store[key][1] += float(row.inv_speed_sum)


def _bucket_to_df(store: dict[str, list[float]], speed_col: str) -> pd.DataFrame:
    rows = []
    obs_col = f"n_obs_{speed_col}"
    for roadseg_id, (obs_n, inv_speed_sum) in store.items():
        speed_val = np.nan
        if obs_n > 0 and inv_speed_sum > 0:
            speed_val = float(obs_n / inv_speed_sum)
        rows.append({"roadseg_id": roadseg_id, speed_col: speed_val, obs_col: int(obs_n)})
    return pd.DataFrame(rows)


def aggregate_speed_by_segment(speed_path: Path, chunksize: int) -> PeriodSpeedTables:
    usecols = ["roadseg_id", "speed", "day", "hour"]
    am_store: dict[str, list[float]] = {}
    pm_store: dict[str, list[float]] = {}
    ff_store: dict[str, list[float]] = {}
    overall_store: dict[str, list[float]] = {}

    for chunk_idx, chunk in enumerate(pd.read_csv(speed_path, usecols=usecols, chunksize=chunksize), start=1):
        chunk = parse_speed_datetime(chunk)
        chunk["roadseg_id"] = chunk["roadseg_id"].astype("string")
        chunk["speed"] = pd.to_numeric(chunk["speed"], errors="coerce")
        chunk = chunk[chunk["roadseg_id"].notna() & chunk["speed"].gt(0) & chunk["dt"].notna()].copy()
        if chunk.empty:
            continue
        weekday = chunk["is_weekday"].fillna(False)
        _update_speed_bucket(overall_store, chunk["roadseg_id"], chunk["speed"])
        _update_speed_bucket(am_store, chunk.loc[weekday & chunk["is_am_peak"].fillna(False), "roadseg_id"], chunk.loc[weekday & chunk["is_am_peak"].fillna(False), "speed"])
        _update_speed_bucket(pm_store, chunk.loc[weekday & chunk["is_pm_peak"].fillna(False), "roadseg_id"], chunk.loc[weekday & chunk["is_pm_peak"].fillna(False), "speed"])
        _update_speed_bucket(ff_store, chunk.loc[chunk["is_freeflow_2205"].fillna(False), "roadseg_id"], chunk.loc[chunk["is_freeflow_2205"].fillna(False), "speed"])
        if chunk_idx % 20 == 0:
            print(f"[raw-connected-grid-cost] processed speed chunks={chunk_idx}", flush=True)

    return PeriodSpeedTables(
        am=_bucket_to_df(am_store, "speed_am_kmh"),
        pm=_bucket_to_df(pm_store, "speed_pm_kmh"),
        ff=_bucket_to_df(ff_store, "speed_ff_kmh"),
        overall=_bucket_to_df(overall_store, "speed_all_kmh"),
    )


def save_speed_tables(speed_tables: PeriodSpeedTables, data_dir: Path) -> None:
    speed_tables.am.to_csv(data_dir / "raw_connected_speed_am_by_roadseg.csv", index=False)
    speed_tables.pm.to_csv(data_dir / "raw_connected_speed_pm_by_roadseg.csv", index=False)
    speed_tables.ff.to_csv(data_dir / "raw_connected_speed_ff_by_roadseg.csv", index=False)
    speed_tables.overall.to_csv(data_dir / "raw_connected_speed_all_by_roadseg.csv", index=False)


def load_cached_speed_tables(data_dir: Path) -> PeriodSpeedTables | None:
    am_path = data_dir / "raw_connected_speed_am_by_roadseg.csv"
    pm_path = data_dir / "raw_connected_speed_pm_by_roadseg.csv"
    ff_path = data_dir / "raw_connected_speed_ff_by_roadseg.csv"
    all_path = data_dir / "raw_connected_speed_all_by_roadseg.csv"
    if not (am_path.exists() and pm_path.exists() and ff_path.exists() and all_path.exists()):
        am_path = data_dir / "kunpeng_speed_am_by_roadseg.csv"
        pm_path = data_dir / "kunpeng_speed_pm_by_roadseg.csv"
        ff_path = data_dir / "kunpeng_speed_ff_by_roadseg.csv"
        all_path = data_dir / "kunpeng_speed_all_by_roadseg.csv"
    if not (am_path.exists() and pm_path.exists() and ff_path.exists() and all_path.exists()):
        return None
    return PeriodSpeedTables(
        am=pd.read_csv(am_path),
        pm=pd.read_csv(pm_path),
        ff=pd.read_csv(ff_path),
        overall=pd.read_csv(all_path),
    )


def parse_radius_list(text: str) -> list[float]:
    vals = []
    for part in str(text).split(","):
        part = part.strip()
        if not part:
            continue
        vals.append(float(part))
    return sorted(v for v in vals if np.isfinite(v) and v > 0)


def load_raw_connected_graph(raw_network_dir: Path, speed_tables: PeriodSpeedTables, period: str) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, float]]:
    nodes = pd.read_excel(raw_network_dir / "nodes.xlsx")
    edges = pd.read_excel(raw_network_dir / "directed_edges.xlsx")

    nodes["node_id"] = pd.to_numeric(nodes["node_id"], errors="coerce").astype("Int64")
    nodes = nodes[nodes["node_id"].notna()].copy()
    nodes["node_id"] = nodes["node_id"].astype(int)

    edges["road_id"] = edges["road_id"].astype("string")
    edges["from_node_id"] = pd.to_numeric(edges["from_node_id"], errors="coerce").astype("Int64")
    edges["to_node_id"] = pd.to_numeric(edges["to_node_id"], errors="coerce").astype("Int64")
    edges = edges[edges["road_id"].notna() & edges["from_node_id"].notna() & edges["to_node_id"].notna()].copy()
    edges["from_node_id"] = edges["from_node_id"].astype(int)
    edges["to_node_id"] = edges["to_node_id"].astype(int)

    geom = gpd.GeoSeries.from_wkt(edges["geometry"], crs="EPSG:4326", on_invalid="ignore")
    valid_geom = geom.notna() & (~geom.is_empty)
    edges = edges.loc[valid_geom].copy()
    geom = geom.loc[valid_geom].to_crs(3857)
    edges["length_m"] = geom.length.astype(float).to_numpy()

    period_map = {
        "AM": ("speed_am_kmh", speed_tables.am),
        "PM": ("speed_pm_kmh", speed_tables.pm),
        "FF": ("speed_ff_kmh", speed_tables.ff),
    }
    period_col, period_table = period_map[period]
    edges = edges.merge(period_table, left_on="road_id", right_on="roadseg_id", how="left", suffixes=("", "_perioddup"))

    keep_cols = [c for c in edges.columns if not c.endswith("_perioddup")]
    edges = edges[keep_cols].copy()

    network_period_speed = harmonic_mean_speed(edges[period_col]) if period_col in edges else np.nan
    edges["speed_kmh"] = pd.to_numeric(edges.get(period_col), errors="coerce")
    edges.loc[~np.isfinite(edges["speed_kmh"]) | (edges["speed_kmh"] <= 0), "speed_kmh"] = np.nan

    stats = {
        "period_speed_source": period_col,
        "strict_period_only": 1,
        "network_period_speed_kmh": float(network_period_speed) if np.isfinite(network_period_speed) else np.nan,
        "share_edges_with_direct_period_speed": float(pd.to_numeric(edges.get(period_col), errors="coerce").gt(0).mean()),
    }
    return nodes, edges, stats


def clean_road_edges(edges: pd.DataFrame, min_edge_time_min: float) -> pd.DataFrame:
    out = edges.copy()
    out["travel_time_min"] = 60.0 * (out["length_m"] / 1000.0) / out["speed_kmh"]
    out = out[np.isfinite(out["travel_time_min"]) & (out["travel_time_min"] > 0)].copy()
    if min_edge_time_min > 0:
        out = out[out["travel_time_min"] >= min_edge_time_min].copy()
    out = out[np.isfinite(out["length_m"]) & (out["length_m"] > 0)].copy()
    out = out[out["from_node_id"] != out["to_node_id"]].copy()
    out = out.sort_values(["from_node_id", "to_node_id", "travel_time_min", "length_m"])
    out = out.drop_duplicates(subset=["from_node_id", "to_node_id"], keep="first").copy()
    return out.reset_index(drop=True)


def build_node_lookup(nodes: pd.DataFrame, road_edges: pd.DataFrame) -> pd.DataFrame:
    deg = pd.concat(
        [
            road_edges[["from_node_id"]].rename(columns={"from_node_id": "node_id"}),
            road_edges[["to_node_id"]].rename(columns={"to_node_id": "node_id"}),
        ],
        ignore_index=True,
    )
    deg = deg.groupby("node_id").size().rename("degree").reset_index()
    out = nodes.merge(deg, on="node_id", how="left")
    out["degree"] = out["degree"].fillna(0).astype(int)
    out = out[out["degree"] > 0].copy()
    return out


def build_local_connector_speed(node_df: pd.DataFrame, road_edges: pd.DataFrame) -> pd.DataFrame:
    speed_from = road_edges[["from_node_id", "speed_kmh"]].rename(columns={"from_node_id": "node_id"})
    speed_to = road_edges[["to_node_id", "speed_kmh"]].rename(columns={"to_node_id": "node_id"})
    node_speed = pd.concat([speed_from, speed_to], ignore_index=True)
    node_speed = node_speed.groupby("node_id", as_index=False).agg(local_speed_kmh=("speed_kmh", "median"))
    out = node_df.merge(node_speed, on="node_id", how="left")
    out["local_speed_kmh"] = pd.to_numeric(out["local_speed_kmh"], errors="coerce")
    out.loc[~np.isfinite(out["local_speed_kmh"]) | (out["local_speed_kmh"] <= 0), "local_speed_kmh"] = np.nan
    out["is_leaf_node"] = out["degree"] <= 1
    return out


def compute_old_grid_costs(baseline_root: Path, grid_type: str) -> pd.DataFrame:
    data_dir = baseline_root / "data"
    edges = pd.read_csv(data_dir / f"t_edges_{grid_type}_AM.csv")
    od = pd.read_csv(data_dir / f"OD_{grid_type}_reachable_AM.csv")
    t_nodes = pd.read_csv(data_dir / f"t_nodes_{grid_type}.csv")
    od["home_grid"] = normalize_grid_id_series(od["home_grid"])
    od["work_grid"] = normalize_grid_id_series(od["work_grid"])
    t_nodes["grid_id"] = normalize_grid_id_series(t_nodes["grid_id"])

    edges["t_min"] = pd.to_numeric(edges["t_min"], errors="coerce")
    edges = edges[np.isfinite(edges["t_min"]) & (edges["t_min"] > 0)].copy()
    n = int(max(edges["i"].max(), edges["j"].max()) + 1)
    graph = coo_matrix((edges["t_min"], (edges["i"], edges["j"])), shape=(n, n)).tocsr()
    dist = shortest_path(graph, directed=True, unweighted=False)

    node_lookup = t_nodes.rename(columns={"grid_id": "home_grid", "node_i": "home_tnode"})
    out = od.merge(node_lookup, on="home_grid", how="left")
    node_lookup2 = t_nodes.rename(columns={"grid_id": "work_grid", "node_i": "work_tnode"})
    out = out.merge(node_lookup2, on="work_grid", how="left")
    out["old_travel_time_min"] = dist[out["home_tnode"].astype(int).values, out["work_tnode"].astype(int).values]
    out.loc[~np.isfinite(out["old_travel_time_min"]), "old_travel_time_min"] = np.nan
    return out


def compute_old_adjacent_costs(baseline_root: Path, grid_type: str) -> pd.DataFrame:
    data_dir = baseline_root / "data"
    edges = pd.read_csv(data_dir / f"t_edges_{grid_type}_AM.csv")
    edges["grid_o"] = normalize_grid_id_series(edges["grid_o"])
    edges["grid_d"] = normalize_grid_id_series(edges["grid_d"])
    edges["t_min"] = pd.to_numeric(edges["t_min"], errors="coerce")
    out = edges[edges["grid_o"].notna() & edges["grid_d"].notna() & np.isfinite(edges["t_min"]) & (edges["t_min"] > 0)].copy()
    out = out.rename(
        columns={
            "grid_o": "home_grid",
            "grid_d": "work_grid",
            "t_min": "old_travel_time_min",
        }
    )
    return out[["home_grid", "work_grid", "old_travel_time_min"]].copy()


def load_grid_3857(baseline_root: Path, grid_type: str) -> gpd.GeoDataFrame:
    data_dir = baseline_root / "data"
    grid = gpd.read_parquet(data_dir / f"grid_{grid_type}_master.parquet")
    grid = grid[["grid_id", "geometry"]].copy()
    grid["grid_id"] = normalize_grid_id_series(grid["grid_id"])
    grid = grid[grid["grid_id"].notna()].copy()
    grid_3857 = grid.to_crs(3857)
    cent = grid_3857.geometry.centroid
    grid_3857["cx"] = cent.x
    grid_3857["cy"] = cent.y
    return grid_3857.reset_index(drop=True)


def build_adjacent_pairs(grid_3857: gpd.GeoDataFrame) -> pd.DataFrame:
    sindex = grid_3857.sindex
    rows = []
    for i, row in enumerate(grid_3857.itertuples(index=False)):
        cand_idx = list(sindex.intersection(row.geometry.bounds))
        for j in cand_idx:
            if j == i:
                continue
            other = grid_3857.iloc[int(j)]
            inter = row.geometry.boundary.intersection(other.geometry.boundary)
            if inter.is_empty or inter.length <= 1.0:
                continue
            rows.append(
                {
                    "home_grid": row.grid_id,
                    "work_grid": other.grid_id,
                    "shared_boundary_m": float(inter.length),
                }
            )
    adj = pd.DataFrame(rows).drop_duplicates(subset=["home_grid", "work_grid"]).reset_index(drop=True)
    return adj


def build_grid_connectors(
    grid_3857: gpd.GeoDataFrame,
    node_df: pd.DataFrame,
    connector_count: int,
    connector_max_dist_m: float,
    fallback_connector_speed_kmh: float,
    search_radii_m: list[float],
    connector_min_node_degree: int,
    allow_low_degree_fallback: bool,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    node_xy = node_df[["node_id", "x_m", "y_m", "local_speed_kmh", "degree"]].copy()
    tree = cKDTree(node_xy[["x_m", "y_m"]].to_numpy(dtype=float))
    rec = []
    max_query_radius = float(max(search_radii_m + [connector_max_dist_m]))
    for grid_row in grid_3857.itertuples(index=False):
        pt = np.asarray([float(grid_row.cx), float(grid_row.cy)], dtype=float)
        cand_idx = list(tree.query_ball_point(pt, r=max_query_radius))
        if not cand_idx:
            continue
        cand = node_xy.iloc[cand_idx].copy()
        cand["connector_dist_m"] = np.sqrt((cand["x_m"] - pt[0]) ** 2 + (cand["y_m"] - pt[1]) ** 2)
        cand = cand[np.isfinite(cand["connector_dist_m"]) & (cand["connector_dist_m"] <= connector_max_dist_m)].copy()
        cand = cand[np.isfinite(cand["local_speed_kmh"]) & (cand["local_speed_kmh"] > 0)].copy()
        if cand.empty:
            continue
        cand["eligible_degree"] = cand["degree"] >= connector_min_node_degree
        chosen = pd.DataFrame()
        for radius in search_radii_m:
            local = cand[cand["connector_dist_m"] <= radius].copy()
            if local.empty:
                continue
            eligible = local[local["eligible_degree"]].copy()
            if len(eligible) >= connector_count:
                use = eligible
            elif allow_low_degree_fallback:
                use = local
            else:
                continue
            use["leaf_penalty"] = np.where(use["degree"] > 1, 0, 1)
            use["degree_penalty"] = np.where(use["eligible_degree"], 0, 1)
            use = use.sort_values(
                ["degree_penalty", "leaf_penalty", "connector_dist_m", "degree", "local_speed_kmh"],
                ascending=[True, True, True, False, False],
            )
            chosen = use.head(connector_count).copy()
            if len(chosen) >= connector_count:
                break
        if chosen.empty and allow_low_degree_fallback:
            cand["leaf_penalty"] = np.where(cand["degree"] > 1, 0, 1)
            cand["degree_penalty"] = np.where(cand["eligible_degree"], 0, 1)
            chosen = cand.sort_values(
                ["degree_penalty", "leaf_penalty", "connector_dist_m", "degree", "local_speed_kmh"],
                ascending=[True, True, True, False, False],
            ).head(connector_count).copy()
        if chosen.empty:
            continue
        for rank, node_row in enumerate(chosen.itertuples(index=False), start=1):
            dist_m = float(node_row.connector_dist_m)
            speed_kmh = float(node_row.local_speed_kmh)
            rec.append(
                {
                    "grid_id": grid_row.grid_id,
                    "node_id": int(node_row.node_id),
                    "connector_rank": rank,
                    "connector_dist_m": dist_m,
                    "connector_speed_kmh": speed_kmh,
                    "connector_time_min": 60.0 * (dist_m / 1000.0) / speed_kmh,
                    "cx": float(grid_row.cx),
                    "cy": float(grid_row.cy),
                    "node_degree": int(node_row.degree),
                    "eligible_degree": int(bool(node_row.eligible_degree)),
                }
            )

    connectors = pd.DataFrame(rec)
    if connectors.empty:
        raise RuntimeError("No grid connectors built.")

    grid_nodes = (
        connectors.groupby("grid_id", as_index=False)
        .agg(
            n_connectors=("node_id", "size"),
            min_connector_dist_m=("connector_dist_m", "min"),
            mean_connector_dist_m=("connector_dist_m", "mean"),
            cx=("cx", "first"),
            cy=("cy", "first"),
        )
    )
    return connectors, grid_nodes


def compute_raw_connected_grid_costs(
    node_df: pd.DataFrame,
    road_edges: pd.DataFrame,
    connectors: pd.DataFrame,
) -> pd.DataFrame:
    road_node_ids = sorted(node_df["node_id"].astype(int).unique().tolist())
    road_map = {nid: i for i, nid in enumerate(road_node_ids)}
    grid_ids = sorted(connectors["grid_id"].astype(str).unique().tolist())
    grid_map = {gid: len(road_map) + i for i, gid in enumerate(grid_ids)}

    ei = road_edges["from_node_id"].map(road_map).to_numpy(dtype=int)
    ej = road_edges["to_node_id"].map(road_map).to_numpy(dtype=int)
    ew = road_edges["travel_time_min"].to_numpy(dtype=float)

    ci = connectors["grid_id"].map(grid_map).to_numpy(dtype=int)
    cj = connectors["node_id"].map(road_map).to_numpy(dtype=int)
    cw = connectors["connector_time_min"].to_numpy(dtype=float)

    n_total = len(road_map) + len(grid_map)
    rows = np.concatenate([ei, ci, cj])
    cols = np.concatenate([ej, cj, ci])
    weights = np.concatenate([ew, cw, cw])
    graph = coo_matrix((weights, (rows, cols)), shape=(n_total, n_total)).tocsr()

    grid_index = np.array([grid_map[gid] for gid in grid_ids], dtype=int)
    dist = shortest_path(graph, directed=True, unweighted=False, indices=grid_index)
    grid_dist = dist[:, grid_index]

    grid_pairs = []
    for i, home_grid in enumerate(grid_ids):
        for j, work_grid in enumerate(grid_ids):
            tt = float(grid_dist[i, j])
            if not np.isfinite(tt):
                tt = np.nan
            grid_pairs.append(
                {
                    "home_grid": home_grid,
                    "work_grid": work_grid,
                    "raw_connected_travel_time_min": tt,
                }
            )
    return pd.DataFrame(grid_pairs)


def compute_raw_connected_grid_costs_adjacent_pairwise(
    node_df: pd.DataFrame,
    road_edges: pd.DataFrame,
    connectors: pd.DataFrame,
    adjacent_pairs: pd.DataFrame,
) -> pd.DataFrame:
    """Pair-specific shortest path: only home and work connector edges enter each pair's graph.
    Returns travel_time_min, route_length_m, and route_avg_speed_kmh per pair.
    """
    road_node_ids = sorted(node_df["node_id"].astype(int).unique().tolist())
    road_map = {nid: i for i, nid in enumerate(road_node_ids)}
    N_road = len(road_map)

    road_rows = road_edges["from_node_id"].map(road_map).to_numpy(dtype=int)
    road_cols = road_edges["to_node_id"].map(road_map).to_numpy(dtype=int)
    road_wts = road_edges["travel_time_min"].to_numpy(dtype=float)
    road_lens = road_edges["length_m"].to_numpy(dtype=float)

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
            })
            continue
        home_conn = conn_by_grid[home]
        work_conn = conn_by_grid[work]
        ci_h = np.full(len(home_conn), HOME_IDX, dtype=int)
        cj_h = home_conn["node_id"].map(road_map).to_numpy(dtype=int)
        cw_h = home_conn["connector_time_min"].to_numpy(dtype=float)
        cl_h = home_conn["connector_dist_m"].to_numpy(dtype=float)
        ci_w = work_conn["node_id"].map(road_map).to_numpy(dtype=int)
        cj_w = np.full(len(work_conn), WORK_IDX, dtype=int)
        cw_w = work_conn["connector_time_min"].to_numpy(dtype=float)
        cl_w = work_conn["connector_dist_m"].to_numpy(dtype=float)
        all_rows = np.concatenate([road_rows, ci_h, ci_w])
        all_cols = np.concatenate([road_cols, cj_h, cj_w])
        all_wts = np.concatenate([road_wts, cw_h, cw_w])
        all_lens = np.concatenate([road_lens, cl_h, cl_w])
        # (from_node, to_node) -> length lookup for path tracing; first occurrence wins
        len_dict: dict[tuple[int, int], float] = {}
        for fr, to, ln in zip(all_rows.tolist(), all_cols.tolist(), all_lens.tolist()):
            key = (int(fr), int(to))
            if key not in len_dict:
                len_dict[key] = float(ln)
        graph = coo_matrix((all_wts, (all_rows, all_cols)), shape=(n_total, n_total)).tocsr()
        dist_result, pred_result = shortest_path(
            graph, directed=True, indices=[HOME_IDX], unweighted=False, method="D",
            return_predecessors=True,
        )
        dist_row = dist_result[0]
        pred_row = pred_result[0]
        tt = float(dist_row[WORK_IDX]) if np.isfinite(dist_row[WORK_IDX]) else np.nan
        route_length_m: float = np.nan
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
                route_length_m = float(sum(
                    len_dict.get((path_nodes[i], path_nodes[i + 1]), 0.0)
                    for i in range(len(path_nodes) - 1)
                ))
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
        })
    return pd.DataFrame(out_rows)


def summarize_comparison(comp: pd.DataFrame, grid_type: str, comparison_level: str) -> pd.DataFrame:
    valid = comp[
        comp["old_travel_time_min"].notna()
        & comp["raw_connected_travel_time_min"].notna()
        & np.isfinite(comp["old_travel_time_min"])
        & np.isfinite(comp["raw_connected_travel_time_min"])
    ].copy()

    if valid.empty:
        return pd.DataFrame(
            [
                {
                    "grid_type": grid_type,
                    "comparison_level": comparison_level,
                    "pair_count_total": int(len(comp)),
                    "pair_count_comparable": 0,
                }
            ]
        )

    if "pop" in valid.columns:
        w = pd.to_numeric(valid["pop"], errors="coerce").fillna(0).to_numpy(dtype=float)
    else:
        w = np.ones(len(valid), dtype=float)
    if not np.isfinite(w).any() or w.sum() <= 0:
        w = np.ones(len(valid), dtype=float)

    diff = valid["raw_connected_travel_time_min"] - valid["old_travel_time_min"]
    ratio = valid["raw_connected_travel_time_min"] / valid["old_travel_time_min"]
    corr = np.corrcoef(valid["old_travel_time_min"], valid["raw_connected_travel_time_min"])[0, 1] if len(valid) > 1 else np.nan

    return pd.DataFrame(
        [
            {
                "grid_type": grid_type,
                "comparison_level": comparison_level,
                "pair_count_total": int(len(comp)),
                "pair_count_comparable": int(len(valid)),
                "weighted_old_mean_min": float(np.average(valid["old_travel_time_min"], weights=w)),
                "weighted_raw_connected_mean_min": float(np.average(valid["raw_connected_travel_time_min"], weights=w)),
                "weighted_diff_mean_min": float(np.average(diff, weights=w)),
                "median_diff_min": float(diff.median()),
                "p10_diff_min": float(diff.quantile(0.10)),
                "p90_diff_min": float(diff.quantile(0.90)),
                "weighted_abs_diff_mean_min": float(np.average(np.abs(diff), weights=w)),
                "median_ratio": float(ratio.replace([np.inf, -np.inf], np.nan).dropna().median()),
                "share_raw_connected_slower": float(np.average((diff > 0).astype(float), weights=w)),
                "share_raw_connected_faster": float(np.average((diff < 0).astype(float), weights=w)),
                "pearson_corr": float(corr) if np.isfinite(corr) else np.nan,
            }
        ]
    )


def run_for_grid(
    grid_type: str,
    baseline_root: Path,
    output_root: Path,
    period: str,
    node_df: pd.DataFrame,
    road_edges: pd.DataFrame,
    connector_count: int,
    connector_max_dist_m: float,
    fallback_connector_speed_kmh: float,
    pair_scope: str,
    comparison_level: str,
    search_radii_m: list[float],
    connector_min_node_degree: int,
    allow_low_degree_fallback: bool,
):
    data_dir, metrics_dir = ensure_dirs(output_root)
    grid_3857 = load_grid_3857(baseline_root=baseline_root, grid_type=grid_type)
    adjacent_pairs = build_adjacent_pairs(grid_3857)

    connectors, grid_nodes = build_grid_connectors(
        grid_3857=grid_3857,
        node_df=node_df,
        connector_count=connector_count,
        connector_max_dist_m=connector_max_dist_m,
        fallback_connector_speed_kmh=fallback_connector_speed_kmh,
        search_radii_m=search_radii_m,
        connector_min_node_degree=connector_min_node_degree,
        allow_low_degree_fallback=allow_low_degree_fallback,
    )
    raw_connected_cost = compute_raw_connected_grid_costs_adjacent_pairwise(
        node_df=node_df, road_edges=road_edges, connectors=connectors, adjacent_pairs=adjacent_pairs
    )
    if pair_scope == "adjacent":
        raw_connected_cost = adjacent_pairs.merge(raw_connected_cost, on=["home_grid", "work_grid"], how="left")
    if comparison_level == "adjacent":
        old_cost = compute_old_adjacent_costs(baseline_root=baseline_root, grid_type=grid_type)
    else:
        old_cost = compute_old_grid_costs(baseline_root=baseline_root, grid_type=grid_type)
        if pair_scope == "adjacent":
            old_cost = adjacent_pairs.merge(old_cost, on=["home_grid", "work_grid"], how="left")
    comp = old_cost.merge(raw_connected_cost, on=["home_grid", "work_grid"], how="left", suffixes=("", "_adj"))
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
                "share_leaf_connectors": float((connectors["node_degree"] <= 1).mean()),
                "share_below_min_degree_connectors": float(1.0 - connectors["eligible_degree"].mean()),
                "adjacent_pair_count": int(len(adjacent_pairs)),
                "connector_min_node_degree": int(connector_min_node_degree),
                "allow_low_degree_fallback": int(bool(allow_low_degree_fallback)),
                "connector_speed_rule": "node_local_period_median_only",
            }
        ]
    )

    suffix = f"{comparison_level}_{pair_scope}_{grid_type}_{period}"
    comp.to_csv(data_dir / f"raw_connected_vs_old_{suffix}.csv", index=False)
    connectors.to_csv(data_dir / f"raw_connected_grid_connectors_{pair_scope}_{grid_type}.csv", index=False)
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
        print("[raw-connected-grid-cost] aggregating raw speed by road segment", flush=True)
        speed_tables = aggregate_speed_by_segment(Path(args.speed_path), chunksize=args.speed_chunksize)
        save_speed_tables(speed_tables, data_dir)
    else:
        print("[raw-connected-grid-cost] using cached road-segment speed tables", flush=True)
        save_speed_tables(speed_tables, data_dir)
    print("[raw-connected-grid-cost] loading raw_connected_v1 graph", flush=True)
    nodes, edges, speed_stats = load_raw_connected_graph(raw_network_dir, speed_tables, args.period)
    road_edges = clean_road_edges(edges, min_edge_time_min=args.min_edge_time_min)
    node_df = build_node_lookup(nodes, road_edges)
    node_df = build_local_connector_speed(
        node_df=node_df,
        road_edges=road_edges,
    )

    road_edges.to_csv(data_dir / f"raw_connected_road_edges_{args.period}.csv", index=False)
    node_df.to_csv(data_dir / "raw_connected_road_nodes_usable.csv", index=False)

    grid_types = GRID_TYPES if args.grid_type == "all" else (args.grid_type,)
    summaries = []
    connector_summaries = []
    for grid_type in grid_types:
        print(f"[raw-connected-grid-cost] building grid costs for grid_type={grid_type}", flush=True)
        summary, connector_summary = run_for_grid(
            grid_type=grid_type,
            baseline_root=baseline_root,
            output_root=output_root,
            period=args.period,
            node_df=node_df,
            road_edges=road_edges,
            connector_count=args.connector_count,
            connector_max_dist_m=args.connector_max_dist_m,
            fallback_connector_speed_kmh=args.connector_speed_kmh,
            pair_scope=args.pair_scope,
            comparison_level=args.comparison_level,
            search_radii_m=search_radii_m,
            connector_min_node_degree=args.connector_min_node_degree,
            allow_low_degree_fallback=args.allow_low_degree_fallback,
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
                "raw_network_dir": str(raw_network_dir),
                "baseline_root": str(baseline_root),
                "speed_path": str(Path(args.speed_path)),
                "period": args.period,
                "grid_type": args.grid_type,
                "connector_count": args.connector_count,
                "connector_max_dist_m": args.connector_max_dist_m,
                "connector_speed_kmh": args.connector_speed_kmh,
                "connector_speed_rule": "node_local_period_median_only",
                "speed_cache_dir": str(speed_cache_dir) if speed_cache_dir else None,
                "pair_scope": args.pair_scope,
                "comparison_level": args.comparison_level,
                "centroid_search_radii_m": search_radii_m,
                "connector_min_node_degree": args.connector_min_node_degree,
                "allow_low_degree_fallback": args.allow_low_degree_fallback,
                "min_edge_time_min": args.min_edge_time_min,
                "speed_chunksize": args.speed_chunksize,
                "strict_period_speed_only": True,
                "speed_stats": speed_stats,
                "cleaned_road_edges": int(len(road_edges)),
                "usable_road_nodes": int(len(node_df)),
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    print(f"[raw-connected-grid-cost] complete output_root={output_root}", flush=True)


if __name__ == "__main__":
    main()
