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
DEFAULT_KUNPENG_DIR = ROOT / "src" / "fromKunpeng" / "New_Strategy" / "Processed_Data"
DEFAULT_BASELINE_VERSION = ROOT / "outputs" / "match_projection_complete_v7_latest"
DEFAULT_SPEED_PATH = ROOT / "raw_data" / "speed_Beijing_all_wgs84.csv"
DEFAULT_OUTPUT_ROOT = ROOT / "outputs" / "kunpeng_grid_travel_time_v1"

GRID_TYPES = ("square", "hex", "voronoi")


@dataclass
class PeriodSpeedTables:
    am: pd.DataFrame
    pm: pd.DataFrame
    ff: pd.DataFrame
    overall: pd.DataFrame


def parse_args():
    parser = argparse.ArgumentParser(
        description="Build grid-to-grid travel time from the Kunpeng directed road network."
    )
    parser.add_argument("--kunpeng-dir", default=str(DEFAULT_KUNPENG_DIR))
    parser.add_argument("--baseline-version-root", default=str(DEFAULT_BASELINE_VERSION))
    parser.add_argument("--speed-path", default=str(DEFAULT_SPEED_PATH))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--grid-type", choices=("all", *GRID_TYPES), default="all")
    parser.add_argument("--period", choices=("AM", "PM"), default="AM")
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
        help="Drop non-positive / implausibly tiny edge times below this threshold.",
    )
    parser.add_argument(
        "--speed-chunksize",
        type=int,
        default=500_000,
        help="CSV chunk size for streaming raw speed aggregation.",
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
            print(f"[kunpeng-grid-cost] processed speed chunks={chunk_idx}", flush=True)

    return PeriodSpeedTables(
        am=_bucket_to_df(am_store, "speed_am_kmh"),
        pm=_bucket_to_df(pm_store, "speed_pm_kmh"),
        ff=_bucket_to_df(ff_store, "speed_ff_kmh"),
        overall=_bucket_to_df(overall_store, "speed_all_kmh"),
    )


def save_speed_tables(speed_tables: PeriodSpeedTables, data_dir: Path) -> None:
    speed_tables.am.to_csv(data_dir / "kunpeng_speed_am_by_roadseg.csv", index=False)
    speed_tables.pm.to_csv(data_dir / "kunpeng_speed_pm_by_roadseg.csv", index=False)
    speed_tables.ff.to_csv(data_dir / "kunpeng_speed_ff_by_roadseg.csv", index=False)
    speed_tables.overall.to_csv(data_dir / "kunpeng_speed_all_by_roadseg.csv", index=False)


def load_cached_speed_tables(data_dir: Path) -> PeriodSpeedTables | None:
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


def load_kunpeng_graph(kunpeng_dir: Path, speed_tables: PeriodSpeedTables, period: str) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, float]]:
    nodes = pd.read_excel(kunpeng_dir / "nodes.xlsx")
    edges = pd.read_excel(kunpeng_dir / "directed_edges.xlsx")

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

    edges = edges.merge(speed_tables.overall, left_on="road_id", right_on="roadseg_id", how="left")
    edges = edges.merge(speed_tables.ff, left_on="road_id", right_on="roadseg_id", how="left", suffixes=("", "_ffdup"))
    period_col = "speed_am_kmh" if period == "AM" else "speed_pm_kmh"
    period_obs_col = "n_obs_speed_am_kmh" if period == "AM" else "n_obs_speed_pm_kmh"
    period_table = speed_tables.am if period == "AM" else speed_tables.pm
    edges = edges.merge(period_table, left_on="road_id", right_on="roadseg_id", how="left", suffixes=("", "_perioddup"))

    keep_cols = [c for c in edges.columns if not c.endswith("_ffdup") and not c.endswith("_perioddup")]
    edges = edges[keep_cols].copy()

    network_period_speed = harmonic_mean_speed(edges[period_col]) if period_col in edges else np.nan
    network_ff_speed = harmonic_mean_speed(edges["speed_ff_kmh"])
    network_all_speed = harmonic_mean_speed(edges["speed_all_kmh"])
    fallback_speed = network_period_speed
    if not np.isfinite(fallback_speed):
        fallback_speed = network_ff_speed
    if not np.isfinite(fallback_speed):
        fallback_speed = network_all_speed

    edges["speed_kmh"] = pd.to_numeric(edges.get(period_col), errors="coerce")
    edges.loc[~np.isfinite(edges["speed_kmh"]) | (edges["speed_kmh"] <= 0), "speed_kmh"] = pd.to_numeric(
        edges.get("speed_ff_kmh"), errors="coerce"
    )
    edges.loc[~np.isfinite(edges["speed_kmh"]) | (edges["speed_kmh"] <= 0), "speed_kmh"] = pd.to_numeric(
        edges.get("speed_all_kmh"), errors="coerce"
    )
    if np.isfinite(fallback_speed) and fallback_speed > 0:
        edges.loc[~np.isfinite(edges["speed_kmh"]) | (edges["speed_kmh"] <= 0), "speed_kmh"] = fallback_speed

    stats = {
        "fallback_speed_kmh": float(fallback_speed) if np.isfinite(fallback_speed) else np.nan,
        "network_period_speed_kmh": float(network_period_speed) if np.isfinite(network_period_speed) else np.nan,
        "network_ff_speed_kmh": float(network_ff_speed) if np.isfinite(network_ff_speed) else np.nan,
        "network_all_speed_kmh": float(network_all_speed) if np.isfinite(network_all_speed) else np.nan,
        "share_edges_with_direct_period_speed": float(pd.to_numeric(edges.get(period_col), errors="coerce").gt(0).mean()),
    }
    return nodes, edges, stats


def clean_road_edges(edges: pd.DataFrame, min_edge_time_min: float) -> pd.DataFrame:
    out = edges.copy()
    out["travel_time_min"] = 60.0 * (out["length_m"] / 1000.0) / out["speed_kmh"]
    out = out[np.isfinite(out["travel_time_min"]) & (out["travel_time_min"] >= min_edge_time_min)].copy()
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


def build_local_connector_speed(node_df: pd.DataFrame, road_edges: pd.DataFrame, fallback_speed_kmh: float) -> pd.DataFrame:
    speed_from = road_edges[["from_node_id", "speed_kmh"]].rename(columns={"from_node_id": "node_id"})
    speed_to = road_edges[["to_node_id", "speed_kmh"]].rename(columns={"to_node_id": "node_id"})
    node_speed = pd.concat([speed_from, speed_to], ignore_index=True)
    node_speed = node_speed.groupby("node_id", as_index=False).agg(local_speed_kmh=("speed_kmh", "median"))
    out = node_df.merge(node_speed, on="node_id", how="left")
    out["local_speed_kmh"] = pd.to_numeric(out["local_speed_kmh"], errors="coerce")
    out.loc[~np.isfinite(out["local_speed_kmh"]) | (out["local_speed_kmh"] <= 0), "local_speed_kmh"] = fallback_speed_kmh
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


def build_grid_connectors(
    baseline_root: Path,
    grid_type: str,
    node_df: pd.DataFrame,
    connector_count: int,
    connector_max_dist_m: float,
    fallback_connector_speed_kmh: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    data_dir = baseline_root / "data"
    grid = gpd.read_parquet(data_dir / f"grid_{grid_type}_master.parquet")
    grid = grid[["grid_id", "geometry"]].copy()
    grid["grid_id"] = normalize_grid_id_series(grid["grid_id"])
    grid = grid[grid["grid_id"].notna()].copy()
    grid_3857 = grid.to_crs(3857)
    cent = grid_3857.geometry.centroid
    grid_3857["cx"] = cent.x
    grid_3857["cy"] = cent.y

    node_xy = node_df[["node_id", "x_m", "y_m", "local_speed_kmh", "degree"]].copy()
    tree = cKDTree(node_xy[["x_m", "y_m"]].to_numpy())
    k = min(max(connector_count, 1), len(node_xy))
    dists, idxs = tree.query(grid_3857[["cx", "cy"]].to_numpy(), k=k)
    if k == 1:
        dists = dists[:, None]
        idxs = idxs[:, None]

    rec = []
    for row_idx, grid_row in enumerate(grid_3857.itertuples(index=False)):
        for rank in range(k):
            dist_m = float(dists[row_idx, rank])
            if not np.isfinite(dist_m) or dist_m > connector_max_dist_m:
                continue
            node_row = node_xy.iloc[int(idxs[row_idx, rank])]
            speed_kmh = float(node_row.local_speed_kmh) if np.isfinite(node_row.local_speed_kmh) else fallback_connector_speed_kmh
            if not np.isfinite(speed_kmh) or speed_kmh <= 0:
                speed_kmh = fallback_connector_speed_kmh
            rec.append(
                {
                    "grid_id": grid_row.grid_id,
                    "node_id": int(node_row.node_id),
                    "connector_rank": rank + 1,
                    "connector_dist_m": dist_m,
                    "connector_speed_kmh": speed_kmh,
                    "connector_time_min": 60.0 * (dist_m / 1000.0) / speed_kmh,
                    "cx": float(grid_row.cx),
                    "cy": float(grid_row.cy),
                }
            )

    connectors = pd.DataFrame(rec)
    if connectors.empty:
        raise RuntimeError(f"No grid connectors built for grid_type={grid_type}")

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


def compute_kunpeng_grid_costs(
    node_df: pd.DataFrame,
    road_edges: pd.DataFrame,
    connectors: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
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
                    "kunpeng_travel_time_min": tt,
                }
            )
    return pd.DataFrame(grid_pairs), pd.DataFrame({"grid_id": grid_ids, "grid_node_index": grid_index})


def summarize_comparison(comp: pd.DataFrame, grid_type: str) -> pd.DataFrame:
    valid = comp[
        comp["old_travel_time_min"].notna()
        & comp["kunpeng_travel_time_min"].notna()
        & np.isfinite(comp["old_travel_time_min"])
        & np.isfinite(comp["kunpeng_travel_time_min"])
    ].copy()

    if valid.empty:
        return pd.DataFrame(
            [
                {
                    "grid_type": grid_type,
                    "od_pairs_total": int(len(comp)),
                    "od_pairs_comparable": 0,
                }
            ]
        )

    w = pd.to_numeric(valid["pop"], errors="coerce").fillna(0).to_numpy(dtype=float)
    if not np.isfinite(w).any() or w.sum() <= 0:
        w = np.ones(len(valid), dtype=float)

    diff = valid["kunpeng_travel_time_min"] - valid["old_travel_time_min"]
    ratio = valid["kunpeng_travel_time_min"] / valid["old_travel_time_min"]
    corr = np.corrcoef(valid["old_travel_time_min"], valid["kunpeng_travel_time_min"])[0, 1] if len(valid) > 1 else np.nan

    return pd.DataFrame(
        [
            {
                "grid_type": grid_type,
                "od_pairs_total": int(len(comp)),
                "od_pairs_comparable": int(len(valid)),
                "weighted_old_mean_min": float(np.average(valid["old_travel_time_min"], weights=w)),
                "weighted_kunpeng_mean_min": float(np.average(valid["kunpeng_travel_time_min"], weights=w)),
                "weighted_diff_mean_min": float(np.average(diff, weights=w)),
                "median_diff_min": float(diff.median()),
                "p10_diff_min": float(diff.quantile(0.10)),
                "p90_diff_min": float(diff.quantile(0.90)),
                "weighted_abs_diff_mean_min": float(np.average(np.abs(diff), weights=w)),
                "median_ratio": float(ratio.replace([np.inf, -np.inf], np.nan).dropna().median()),
                "share_kunpeng_slower": float(np.average((diff > 0).astype(float), weights=w)),
                "share_kunpeng_faster": float(np.average((diff < 0).astype(float), weights=w)),
                "pearson_corr": float(corr) if np.isfinite(corr) else np.nan,
            }
        ]
    )


def run_for_grid(
    grid_type: str,
    baseline_root: Path,
    output_root: Path,
    node_df: pd.DataFrame,
    road_edges: pd.DataFrame,
    fallback_speed_kmh: float,
    connector_count: int,
    connector_max_dist_m: float,
    fallback_connector_speed_kmh: float,
):
    data_dir, metrics_dir = ensure_dirs(output_root)

    connectors, grid_nodes = build_grid_connectors(
        baseline_root=baseline_root,
        grid_type=grid_type,
        node_df=node_df,
        connector_count=connector_count,
        connector_max_dist_m=connector_max_dist_m,
        fallback_connector_speed_kmh=fallback_connector_speed_kmh,
    )
    kunpeng_cost, _ = compute_kunpeng_grid_costs(node_df=node_df, road_edges=road_edges, connectors=connectors)
    old_cost = compute_old_grid_costs(baseline_root=baseline_root, grid_type=grid_type)
    comp = old_cost.merge(kunpeng_cost, on=["home_grid", "work_grid"], how="left")
    comp["diff_min"] = comp["kunpeng_travel_time_min"] - comp["old_travel_time_min"]
    comp["ratio_kunpeng_over_old"] = comp["kunpeng_travel_time_min"] / comp["old_travel_time_min"]

    summary = summarize_comparison(comp, grid_type)
    connector_summary = pd.DataFrame(
        [
            {
                "grid_type": grid_type,
                "grids_with_connectors": int(grid_nodes["grid_id"].nunique()),
                "connector_rows": int(len(connectors)),
                "mean_connector_dist_m": float(connectors["connector_dist_m"].mean()),
                "p90_connector_dist_m": float(connectors["connector_dist_m"].quantile(0.90)),
                "fallback_connector_speed_kmh": float(fallback_connector_speed_kmh),
                "network_fallback_speed_kmh": float(fallback_speed_kmh),
            }
        ]
    )

    comp.to_csv(data_dir / f"kunpeng_vs_old_od_cost_{grid_type}_AM.csv", index=False)
    connectors.to_csv(data_dir / f"kunpeng_grid_connectors_{grid_type}.csv", index=False)
    summary.to_csv(metrics_dir / f"kunpeng_vs_old_summary_{grid_type}_AM.csv", index=False)
    connector_summary.to_csv(metrics_dir / f"kunpeng_connector_summary_{grid_type}.csv", index=False)
    return summary, connector_summary


def main():
    args = parse_args()
    kunpeng_dir = Path(args.kunpeng_dir)
    baseline_root = Path(args.baseline_version_root)
    output_root = Path(args.output_root)

    data_dir, metrics_dir = ensure_dirs(output_root)
    speed_tables = load_cached_speed_tables(data_dir)
    if speed_tables is None:
        print("[kunpeng-grid-cost] aggregating raw speed by road segment", flush=True)
        speed_tables = aggregate_speed_by_segment(Path(args.speed_path), chunksize=args.speed_chunksize)
        save_speed_tables(speed_tables, data_dir)
    else:
        print("[kunpeng-grid-cost] using cached road-segment speed tables", flush=True)
    print("[kunpeng-grid-cost] loading Kunpeng graph", flush=True)
    nodes, edges, speed_stats = load_kunpeng_graph(kunpeng_dir, speed_tables, args.period)
    road_edges = clean_road_edges(edges, min_edge_time_min=args.min_edge_time_min)
    node_df = build_node_lookup(nodes, road_edges)
    node_df = build_local_connector_speed(
        node_df=node_df,
        road_edges=road_edges,
        fallback_speed_kmh=speed_stats["fallback_speed_kmh"] if np.isfinite(speed_stats["fallback_speed_kmh"]) else args.connector_speed_kmh,
    )

    road_edges.to_csv(data_dir / f"kunpeng_road_edges_{args.period}.csv", index=False)
    node_df.to_csv(data_dir / "kunpeng_road_nodes_usable.csv", index=False)

    grid_types = GRID_TYPES if args.grid_type == "all" else (args.grid_type,)
    summaries = []
    connector_summaries = []
    for grid_type in grid_types:
        print(f"[kunpeng-grid-cost] building grid costs for grid_type={grid_type}", flush=True)
        summary, connector_summary = run_for_grid(
            grid_type=grid_type,
            baseline_root=baseline_root,
            output_root=output_root,
            node_df=node_df,
            road_edges=road_edges,
            fallback_speed_kmh=float(speed_stats["fallback_speed_kmh"]) if np.isfinite(speed_stats["fallback_speed_kmh"]) else args.connector_speed_kmh,
            connector_count=args.connector_count,
            connector_max_dist_m=args.connector_max_dist_m,
            fallback_connector_speed_kmh=args.connector_speed_kmh,
        )
        summaries.append(summary)
        connector_summaries.append(connector_summary)

    pd.concat(summaries, ignore_index=True).to_csv(metrics_dir / f"kunpeng_vs_old_summary_all_{args.period}.csv", index=False)
    pd.concat(connector_summaries, ignore_index=True).to_csv(metrics_dir / "kunpeng_connector_summary_all.csv", index=False)
    (output_root / "run_config.json").write_text(
        json.dumps(
            {
                "kunpeng_dir": str(kunpeng_dir),
                "baseline_root": str(baseline_root),
                "speed_path": str(Path(args.speed_path)),
                "period": args.period,
                "grid_type": args.grid_type,
                "connector_count": args.connector_count,
                "connector_max_dist_m": args.connector_max_dist_m,
                "connector_speed_kmh": args.connector_speed_kmh,
                "min_edge_time_min": args.min_edge_time_min,
                "speed_chunksize": args.speed_chunksize,
                "speed_stats": speed_stats,
                "cleaned_road_edges": int(len(road_edges)),
                "usable_road_nodes": int(len(node_df)),
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    print(f"[kunpeng-grid-cost] complete output_root={output_root}", flush=True)


if __name__ == "__main__":
    main()
