from __future__ import annotations

import argparse
import heapq
import json
import math
import re
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import numpy as np
import pandas as pd
from shapely.geometry import box


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_TIDAL_SINGLE = (
    PROJECT_ROOT / "data_work/raw_data/gis/深圳潮汐车道shp/2_单线.shp"
)
DEFAULT_TIDAL_DOUBLE = (
    PROJECT_ROOT / "data_work/raw_data/gis/深圳潮汐车道shp/2_双线.shp"
)
DEFAULT_ROADS = PROJECT_ROOT / "data_work/raw_data/gis/roads_baidu/shenzhen_roads.shp"
DEFAULT_SPEED = PROJECT_ROOT / "data_work/raw_data/speed_shenzhen_all_wgs84.csv"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data_work/outputs/shenzhen_caitian_baidu_analysis_v1"

PROJECTED_CRS = "EPSG:32650"
MAIN_BUFFER_M = 60
BUFFER_SENSITIVITY_M = [30, 60, 100]
NETWORK_DISTANCE_M = 3000

EVENT_START_MIN = 16 * 60 + 30
ACTIVE_WINDOW = (EVENT_START_MIN, 21 * 60 + 30)
REFERENCE_WINDOW = (15 * 60, EVENT_START_MIN)
POST_WINDOW = (21 * 60 + 30, 22 * 60 + 30)
NIGHT_WINDOW = (0, 5 * 60)
EVENT_SAMPLE_WINDOW = (15 * 60, 22 * 60 + 30)

POLICY_CORE_CHAIN_IDS = [
    "RS_NB1521432208_NB1521432620_1",
    "RS_NB1521432620_NB1587184510_1",
    "RS_NB1587184510_NB1587175525_1",
    "RS_NB1587175525_NB1521428246_1",
    "RS_NB1521428246_NB1526678800_1",
    "RS_NB1526678800_NB1526683015_1",
]
OPPOSITE_CORE_CHAIN_IDS = [
    "RS_NB1526683014_NB1526678928_1",
    "RS_NB1526678928_NB1580292270_1",
    "RS_NB1580292270_NB1521428398_1",
    "RS_NB1521428398_NB1553454627_1",
    "RS_NB1553454627_NB1584548780_1",
    "RS_NB1584548780_NB1521432253_1",
    "RS_NB1521432253_NB1521439708_1",
]

CONTROL_CONFIG = {
    "huanggang": {
        "label": "Huanggang Rd",
        "roadname": "皇岗路",
        "role": "main_control",
        "bbox": (114.055, 22.525, 114.075, 22.580),
    },
    "jintian": {
        "label": "Jintian Rd",
        "roadname": "金田路",
        "role": "robustness_control",
        "bbox": (114.050, 22.525, 114.064, 22.553),
    },
    "yitian": {
        "label": "Yitian Rd",
        "roadname": "益田路",
        "role": "robustness_control",
        "bbox": (114.044, 22.525, 114.056, 22.553),
    },
    "xiangmihu": {
        "label": "Xiangmihu Rd",
        "roadname": "香蜜湖路",
        "role": "robustness_control",
        "bbox": (114.010, 22.525, 114.030, 22.560),
    },
}
CONTROL_EVENT_SPECS = [
    ("huanggang", "control_huanggang", "Huanggang"),
    ("jintian", "control_jintian", "Jintian"),
    ("yitian", "control_yitian", "Yitian"),
    ("xiangmihu", "control_xiangmihu", "Xiangmihu"),
]

TIDAL_ROADNAMES_TO_EXCLUDE = {
    "彩田路",
    "新彩隧道",
    "文锦北路",
    "布吉路",
    "红岭北路",
    "新洲路",
    "留仙大道",
    "惠深沿海高速",
    "下梅林一街",
    "香梅路",
    "东昌路",
}

WINDOWS = {
    "night_0000_0500": NIGHT_WINDOW,
    "reference_1500_1630": REFERENCE_WINDOW,
    "active_1630_2130": ACTIVE_WINDOW,
    "post_2130_2230": POST_WINDOW,
}

WINDOW_LABELS = {
    "night_0000_0500": "00:00-05:00",
    "reference_1500_1630": "15:00-16:30",
    "active_1630_2130": "16:30-21:30",
    "post_2130_2230": "21:30-22:30",
}

DIRECTION_LABELS = {
    "policy_direction_futian_to_longhua": "Futian -> Longhua",
    "opposite_direction_longhua_to_futian": "Longhua -> Futian",
    "connector_or_cross": "Connector/cross",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run post-only Caitian Road Baidu tidal-lane diagnostics."
    )
    parser.add_argument("--tidal-single", type=Path, default=DEFAULT_TIDAL_SINGLE)
    parser.add_argument("--tidal-double", type=Path, default=DEFAULT_TIDAL_DOUBLE)
    parser.add_argument("--roads", type=Path, default=DEFAULT_ROADS)
    parser.add_argument("--speed", type=Path, default=DEFAULT_SPEED)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def configure_plot() -> None:
    plt.rcParams.update(
        {
            "font.sans-serif": [
                "PingFang SC",
                "Hiragino Sans GB",
                "Microsoft YaHei",
                "Noto Sans CJK SC",
                "Arial Unicode MS",
                "DejaVu Sans",
            ],
            "axes.unicode_minus": False,
            "font.size": 9,
            "axes.titlesize": 11,
            "axes.labelsize": 9,
            "legend.fontsize": 8,
            "figure.dpi": 220,
            "savefig.dpi": 220,
        }
    )


def ensure_output_dirs(output_dir: Path) -> dict[str, Path]:
    dirs = {
        "root": output_dir,
        "figures": output_dir / "figures",
        "data": output_dir / "data",
        "metrics": output_dir / "metrics",
    }
    for path in dirs.values():
        path.mkdir(parents=True, exist_ok=True)
    return dirs


def save_figure(fig: plt.Figure, path: Path) -> None:
    fig.savefig(path, bbox_inches="tight")
    fig.savefig(path.with_suffix(".pdf"), bbox_inches="tight")
    fig.savefig(path.with_suffix(".svg"), bbox_inches="tight")
    plt.close(fig)


def parse_roadseg_nodes(roadseg_id: str) -> tuple[str, str]:
    match = re.match(r"RS_(NB\d+)_(NB\d+)_", str(roadseg_id))
    if not match:
        return "", ""
    return match.group(1), match.group(2)


def endpoint_coords(geometry) -> tuple[float, float, float, float]:
    coords = list(geometry.coords)
    start_lon, start_lat = coords[0]
    end_lon, end_lat = coords[-1]
    return start_lon, start_lat, end_lon, end_lat


def semantic_direction(semantic: str) -> str:
    if not isinstance(semantic, str) or "," not in semantic:
        return ""
    return semantic.rsplit(",", 1)[-1]


def bearing_degrees(start_lon: float, start_lat: float, end_lon: float, end_lat: float) -> float:
    dy = (end_lat - start_lat) * 111_320
    dx = (end_lon - start_lon) * 111_320 * math.cos(math.radians((start_lat + end_lat) / 2))
    return (math.degrees(math.atan2(dx, dy)) + 360) % 360


def direction_group(start_lat: float, end_lat: float, direction_text: str) -> str:
    if end_lat - start_lat > 0.00025:
        return "policy_direction_futian_to_longhua"
    if start_lat - end_lat > 0.00025:
        return "opposite_direction_longhua_to_futian"
    if direction_text in {"南向北", "东南向西北", "西南向东北"}:
        return "policy_direction_futian_to_longhua"
    if direction_text in {"北向南", "西北向东南", "东北向西南"}:
        return "opposite_direction_longhua_to_futian"
    return "connector_or_cross"


def add_road_geometry_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    out = gdf.copy()
    projected = out.to_crs(PROJECTED_CRS)
    out["length_m"] = projected.geometry.length.to_numpy()
    endpoints = out.geometry.apply(endpoint_coords)
    out[["start_lon", "start_lat", "end_lon", "end_lat"]] = pd.DataFrame(
        endpoints.tolist(), index=out.index
    )
    out["direction_text"] = out["semantic"].apply(semantic_direction)
    out["direction_group"] = out.apply(
        lambda row: direction_group(row.start_lat, row.end_lat, row.direction_text),
        axis=1,
    )
    out["bearing_deg"] = out.apply(
        lambda row: bearing_degrees(row.start_lon, row.start_lat, row.end_lon, row.end_lat),
        axis=1,
    )
    bounds = out.geometry.bounds
    out["min_lon"] = bounds.minx
    out["max_lon"] = bounds.maxx
    out["min_lat"] = bounds.miny
    out["max_lat"] = bounds.maxy
    centroids = projected.geometry.centroid.to_crs(out.crs)
    out["centroid_lon"] = centroids.x
    out["centroid_lat"] = centroids.y
    nodes = out["roadseg_id"].apply(parse_roadseg_nodes)
    out[["from_node", "to_node"]] = pd.DataFrame(nodes.tolist(), index=out.index)
    return out


def build_tidal_buffers(tidal_single: gpd.GeoDataFrame) -> dict[int, object]:
    single_utm = tidal_single.to_crs(PROJECTED_CRS)
    line_union = single_utm.geometry.union_all()
    return {meters: line_union.buffer(meters) for meters in BUFFER_SENSITIVITY_M}


def compute_buffer_matches(
    roads: gpd.GeoDataFrame,
    tidal_buffers: dict[int, object],
) -> tuple[dict[int, gpd.GeoDataFrame], pd.DataFrame]:
    roads_utm = roads.to_crs(PROJECTED_CRS).copy()
    matches = {}
    rows = []
    for buffer_m, buffer_geom in tidal_buffers.items():
        sub = roads_utm[roads_utm.intersects(buffer_geom)].copy()
        if sub.empty:
            sub["intersect_length_m"] = []
            sub["overlap_share"] = []
        else:
            sub["intersect_length_m"] = sub.geometry.intersection(buffer_geom).length
            sub["overlap_share"] = sub["intersect_length_m"] / sub["length_m"].replace(0, np.nan)
        out = sub.to_crs(roads.crs)
        out["buffer_m"] = buffer_m
        matches[buffer_m] = out
        rows.append(
            {
                "buffer_m": buffer_m,
                "n_segments": int(out["roadseg_id"].nunique()),
                "total_segment_length_m": float(out["length_m"].sum()),
                "total_intersect_length_m": float(out["intersect_length_m"].sum()),
                "n_caitian_or_xincai": int(
                    out["roadname"].isin({"彩田路", "新彩隧道"}).sum()
                ),
            }
        )
    return matches, pd.DataFrame(rows)


def make_treatment_core(roads: gpd.GeoDataFrame, buffer_60: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    core_ids = POLICY_CORE_CHAIN_IDS + OPPOSITE_CORE_CHAIN_IDS
    core_sequence = {rid: idx + 1 for idx, rid in enumerate(POLICY_CORE_CHAIN_IDS)}
    core_sequence.update({rid: idx + 1 for idx, rid in enumerate(OPPOSITE_CORE_CHAIN_IDS)})
    core = roads[roads["roadseg_id"].isin(core_ids)].copy()
    if core["roadseg_id"].nunique() != len(core_ids):
        missing = sorted(set(core_ids).difference(core["roadseg_id"]))
        raise ValueError(f"Missing expected Caitian treatment core roadseg_id values: {missing}")
    overlap = buffer_60[["roadseg_id", "intersect_length_m", "overlap_share"]]
    core = core.merge(overlap, on="roadseg_id", how="left")
    core["intersect_length_m"] = core["intersect_length_m"].fillna(0.0)
    core["overlap_share"] = core["overlap_share"].fillna(0.0)
    core["corridor_id"] = "caitian"
    core["corridor_label"] = "Caitian Rd"
    core["segment_role"] = "treatment_core"
    core["analysis_role"] = "treatment"
    core["is_caitian"] = 1
    core["is_main_control"] = 0
    core["core_chain_candidate"] = True
    core["core_sequence"] = core["roadseg_id"].map(core_sequence)
    core["policy_direction"] = core["direction_group"].eq(
        "policy_direction_futian_to_longhua"
    ).astype(int)
    return core.sort_values(["direction_group", "core_sequence"]).copy()


def make_auxiliary_treatment_candidates(
    roads: gpd.GeoDataFrame,
    buffer_60: gpd.GeoDataFrame,
    core_ids: set[str],
) -> gpd.GeoDataFrame:
    aux = buffer_60[~buffer_60["roadseg_id"].isin(core_ids)].copy()
    aux = aux[
        aux["roadname"].isin(
            {"彩田路", "新彩隧道", "北环彩田立交桥", "皇岗彩田立交", "彩田路辅路"}
        )
        | aux["semantic"].fillna("").str.contains("彩田")
    ].copy()
    aux["corridor_id"] = "caitian_auxiliary"
    aux["corridor_label"] = "Caitian auxiliary"
    aux["segment_role"] = "treatment_auxiliary"
    aux["analysis_role"] = "auxiliary"
    aux["is_caitian"] = 1
    aux["is_main_control"] = 0
    aux["core_chain_candidate"] = False
    aux["policy_direction"] = aux["direction_group"].eq(
        "policy_direction_futian_to_longhua"
    ).astype(int)
    return aux.sort_values(["roadname", "roadseg_id"]).copy()


def select_controls(roads: gpd.GeoDataFrame, main_buffer_geom) -> gpd.GeoDataFrame:
    roads_utm = roads.to_crs(PROJECTED_CRS)
    excluded_by_buffer = roads_utm.intersects(main_buffer_geom)
    controls = []
    for control_id, cfg in CONTROL_CONFIG.items():
        min_lon, min_lat, max_lon, max_lat = cfg["bbox"]
        bounds_mask = roads.intersects(box(min_lon, min_lat, max_lon, max_lat))
        sub = roads[
            roads["roadname"].eq(cfg["roadname"])
            & bounds_mask
            & ~excluded_by_buffer.to_numpy()
            & ~roads["roadname"].isin(TIDAL_ROADNAMES_TO_EXCLUDE)
        ].copy()
        sub["corridor_id"] = f"control_{control_id}"
        sub["corridor_label"] = cfg["label"]
        sub["segment_role"] = cfg["role"]
        sub["analysis_role"] = "control"
        sub["is_caitian"] = 0
        sub["is_main_control"] = int(control_id == "huanggang")
        sub["core_chain_candidate"] = True
        sub["policy_direction"] = sub["direction_group"].eq(
            "policy_direction_futian_to_longhua"
        ).astype(int)
        sub["control_id"] = control_id
        controls.append(sub)
    out = pd.concat(controls, ignore_index=True)
    out = gpd.GeoDataFrame(out, geometry="geometry", crs=roads.crs)
    out["core_sequence"] = (
        out.sort_values(["corridor_id", "direction_group", "start_lat"])
        .groupby(["corridor_id", "direction_group"])
        .cumcount()
        + 1
    )
    return out.sort_values(["corridor_id", "direction_group", "core_sequence"]).copy()


def dijkstra(adjacency: dict[str, list[tuple[str, float, str]]], origin: str, max_dist: float):
    distances = {origin: 0.0}
    heap = [(0.0, origin)]
    while heap:
        dist, node = heapq.heappop(heap)
        if dist > distances[node]:
            continue
        for nbr, weight, _edge_id in adjacency.get(node, []):
            new_dist = dist + weight
            if new_dist <= max_dist and new_dist < distances.get(nbr, np.inf):
                distances[nbr] = new_dist
                heapq.heappush(heap, (new_dist, nbr))
    return distances


def build_adjacency(roads: gpd.GeoDataFrame, undirected: bool = False):
    adjacency: dict[str, list[tuple[str, float, str]]] = {}
    for row in roads[["roadseg_id", "from_node", "to_node", "length_m"]].itertuples(index=False):
        if not row.from_node or not row.to_node:
            continue
        adjacency.setdefault(row.from_node, []).append((row.to_node, float(row.length_m), row.roadseg_id))
        if undirected:
            adjacency.setdefault(row.to_node, []).append((row.from_node, float(row.length_m), row.roadseg_id))
    return adjacency


def reverse_adjacency(roads: gpd.GeoDataFrame):
    adjacency: dict[str, list[tuple[str, float, str]]] = {}
    for row in roads[["roadseg_id", "from_node", "to_node", "length_m"]].itertuples(index=False):
        if not row.from_node or not row.to_node:
            continue
        adjacency.setdefault(row.to_node, []).append((row.from_node, float(row.length_m), row.roadseg_id))
    return adjacency


def build_network_segments(roads: gpd.GeoDataFrame, treatment_core: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    forward = build_adjacency(roads)
    backward = reverse_adjacency(roads)
    undirected = build_adjacency(roads, undirected=True)
    treatment_ids = set(treatment_core["roadseg_id"])
    core_lookup = treatment_core.set_index("roadseg_id")

    searches = []
    for direction_group, ids in [
        ("policy_direction_futian_to_longhua", POLICY_CORE_CHAIN_IDS),
        ("opposite_direction_longhua_to_futian", OPPOSITE_CORE_CHAIN_IDS),
    ]:
        first = core_lookup.loc[ids[0]]
        last = core_lookup.loc[ids[-1]]
        searches.extend(
            [
                {
                    "chain_direction": direction_group,
                    "network_position": "upstream",
                    "origin_node": first.from_node,
                    "adjacency": backward,
                    "edge_distance_node": "to_node",
                    "fallback_adjacency": undirected,
                },
                {
                    "chain_direction": direction_group,
                    "network_position": "downstream",
                    "origin_node": last.to_node,
                    "adjacency": forward,
                    "edge_distance_node": "from_node",
                    "fallback_adjacency": undirected,
                },
            ]
        )

    rows = []
    for search in searches:
        distances = dijkstra(search["adjacency"], search["origin_node"], NETWORK_DISTANCE_M)
        fallback_used = False
        if len(distances) <= 1:
            distances = dijkstra(
                search["fallback_adjacency"], search["origin_node"], NETWORK_DISTANCE_M
            )
            fallback_used = True
        node_col = search["edge_distance_node"]
        candidate = roads[roads[node_col].isin(distances)].copy()
        candidate["network_distance_m"] = candidate[node_col].map(distances)
        candidate = candidate[
            candidate["network_distance_m"] + candidate["length_m"] <= NETWORK_DISTANCE_M
        ].copy()
        candidate = candidate[~candidate["roadseg_id"].isin(treatment_ids)].copy()
        if candidate.empty:
            continue
        candidate["chain_direction"] = search["chain_direction"]
        candidate["network_position"] = search["network_position"]
        candidate["fallback_used"] = fallback_used
        rows.append(candidate)
    if not rows:
        return gpd.GeoDataFrame(columns=list(roads.columns), geometry="geometry", crs=roads.crs)
    network = pd.concat(rows, ignore_index=True)
    network = gpd.GeoDataFrame(network, geometry="geometry", crs=roads.crs)
    network["distance_bin"] = pd.cut(
        network["network_distance_m"],
        bins=[0, 1000, 2000, 3000],
        labels=["0-1km", "1-2km", "2-3km"],
        include_lowest=True,
        right=True,
    )
    network = network.sort_values(
        ["chain_direction", "network_position", "network_distance_m", "roadseg_id"]
    )
    return network


def make_segment_catalog(
    treatment_core: gpd.GeoDataFrame,
    treatment_auxiliary: gpd.GeoDataFrame,
    controls: gpd.GeoDataFrame,
    network: gpd.GeoDataFrame,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    analysis_segments = pd.concat(
        [treatment_core, controls],
        ignore_index=True,
    )
    analysis_segments = gpd.GeoDataFrame(analysis_segments, geometry="geometry", crs=treatment_core.crs)
    auxiliary = pd.concat([treatment_auxiliary, network], ignore_index=True)
    auxiliary = gpd.GeoDataFrame(auxiliary, geometry="geometry", crs=treatment_core.crs)
    return analysis_segments, auxiliary


def read_selected_speed_panel(speed_path: Path, selected_ids: set[str]) -> pd.DataFrame:
    parts = []
    usecols = ["roadseg_id", "speed", "day", "hour"]
    for chunk in pd.read_csv(speed_path, usecols=usecols, chunksize=1_000_000):
        sub = chunk[chunk["roadseg_id"].isin(selected_ids)].copy()
        if not sub.empty:
            parts.append(sub)
    if not parts:
        raise ValueError("No speed observations found for selected Caitian/control segments.")
    speed = pd.concat(parts, ignore_index=True)
    hour_str = speed["hour"].astype(str).str.zfill(12)
    speed["datetime"] = pd.to_datetime(hour_str, format="%Y%m%d%H%M", errors="raise")
    speed["date"] = pd.to_datetime(speed["datetime"].dt.date)
    speed["dow"] = speed["datetime"].dt.dayofweek
    speed["day_type"] = np.where(speed["dow"] < 5, "weekday", "weekend")
    speed["minute_of_day"] = speed["datetime"].dt.hour * 60 + speed["datetime"].dt.minute
    speed["speed_kmh"] = pd.to_numeric(speed["speed"], errors="coerce")
    speed = speed[(speed["speed_kmh"] > 0) & speed["datetime"].notna()].copy()
    speed["datetime_id"] = speed["datetime"].dt.strftime("%Y%m%d%H%M")
    speed["date_id"] = speed["date"].dt.strftime("%Y%m%d")
    return speed


def attach_segment_attributes(speed: pd.DataFrame, segments: gpd.GeoDataFrame) -> pd.DataFrame:
    attr_cols = [
        "roadseg_id",
        "roadname",
        "semantic",
        "direction_group",
        "direction_text",
        "length_m",
        "corridor_id",
        "corridor_label",
        "analysis_role",
        "segment_role",
        "is_caitian",
        "is_main_control",
        "policy_direction",
    ]
    attr = segments[attr_cols].drop_duplicates("roadseg_id")
    out = speed.merge(attr, on="roadseg_id", how="inner")
    out["segment_travel_time_min"] = out["length_m"] / 1000.0 / out["speed_kmh"] * 60.0
    out["log_travel_time"] = np.log(out["segment_travel_time_min"])
    return out


def build_speed_coverage(speed: pd.DataFrame, segments: gpd.GeoDataFrame) -> pd.DataFrame:
    coverage = (
        speed.groupby("roadseg_id", as_index=False)
        .agg(
            n_obs=("speed_kmh", "size"),
            n_dates=("date", "nunique"),
            first_datetime=("datetime", "min"),
            last_datetime=("datetime", "max"),
            mean_speed_kmh=("speed_kmh", "mean"),
        )
    )
    attr = segments[
        ["roadseg_id", "roadname", "semantic", "corridor_id", "direction_group", "length_m"]
    ].drop_duplicates("roadseg_id")
    out = attr.merge(coverage, on="roadseg_id", how="left")
    out["complete_1008_obs"] = out["n_obs"].eq(1008)
    return out.sort_values(["corridor_id", "direction_group", "roadseg_id"])


def build_corridor_10min_speed(segment_panel: pd.DataFrame) -> pd.DataFrame:
    expected = (
        segment_panel.groupby(["corridor_id", "direction_group"])["roadseg_id"]
        .nunique()
        .to_dict()
    )
    grouped = (
        segment_panel.groupby(
            [
                "corridor_id",
                "corridor_label",
                "analysis_role",
                "is_caitian",
                "is_main_control",
                "direction_group",
                "datetime",
                "date",
                "dow",
                "day_type",
                "minute_of_day",
            ],
            as_index=False,
        )
        .agg(
            travel_time_min=("segment_travel_time_min", "sum"),
            total_length_m=("length_m", "sum"),
            mean_segment_speed_kmh=("speed_kmh", "mean"),
            n_segments=("roadseg_id", "nunique"),
        )
    )
    grouped["expected_segments"] = grouped.apply(
        lambda row: expected[(row["corridor_id"], row["direction_group"])],
        axis=1,
    )
    grouped = grouped[grouped["n_segments"].eq(grouped["expected_segments"])].copy()
    grouped["corridor_speed_kmh"] = (
        grouped["total_length_m"] / 1000.0 / (grouped["travel_time_min"] / 60.0)
    )
    grouped["policy_direction"] = grouped["direction_group"].eq(
        "policy_direction_futian_to_longhua"
    ).astype(int)
    grouped["date_id"] = grouped["date"].dt.strftime("%Y%m%d")
    grouped["datetime_id"] = grouped["datetime"].dt.strftime("%Y%m%d%H%M")
    return grouped.sort_values(["corridor_id", "direction_group", "datetime"])


def window_filter(df: pd.DataFrame, window: tuple[int, int]) -> pd.DataFrame:
    lo, hi = window
    return df[(df["minute_of_day"] >= lo) & (df["minute_of_day"] < hi)].copy()


def build_daily_window_summary(corridor: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for window_name, window in WINDOWS.items():
        sub = window_filter(corridor, window)
        grouped = (
            sub.groupby(
                [
                    "corridor_id",
                    "corridor_label",
                    "analysis_role",
                    "is_caitian",
                    "is_main_control",
                    "direction_group",
                    "date",
                    "dow",
                    "day_type",
                ],
                as_index=False,
            )
            .agg(
                corridor_speed_kmh=("corridor_speed_kmh", "mean"),
                travel_time_min=("travel_time_min", "mean"),
                n_10min=("corridor_speed_kmh", "size"),
            )
        )
        grouped["window"] = window_name
        grouped["window_label"] = WINDOW_LABELS[window_name]
        rows.append(grouped)
    return pd.concat(rows, ignore_index=True)


def build_diurnal_summary(corridor: pd.DataFrame) -> pd.DataFrame:
    return (
        corridor.groupby(
            [
                "corridor_id",
                "corridor_label",
                "analysis_role",
                "direction_group",
                "day_type",
                "minute_of_day",
            ],
            as_index=False,
        )
        .agg(
            corridor_speed_kmh=("corridor_speed_kmh", "mean"),
            travel_time_min=("travel_time_min", "mean"),
            n_obs=("corridor_speed_kmh", "size"),
        )
        .sort_values(["corridor_id", "direction_group", "day_type", "minute_of_day"])
    )


def build_ratios(corridor: pd.DataFrame, daily_window: pd.DataFrame):
    direction_pivot = corridor.pivot_table(
        index=["corridor_id", "corridor_label", "datetime", "date", "day_type", "minute_of_day"],
        columns="direction_group",
        values="corridor_speed_kmh",
        aggfunc="first",
    ).reset_index()
    if {
        "policy_direction_futian_to_longhua",
        "opposite_direction_longhua_to_futian",
    }.issubset(direction_pivot.columns):
        direction_pivot["policy_over_opposite_speed"] = (
            direction_pivot["policy_direction_futian_to_longhua"]
            / direction_pivot["opposite_direction_longhua_to_futian"]
        )
    else:
        direction_pivot["policy_over_opposite_speed"] = np.nan

    window_pivot = daily_window.pivot_table(
        index=[
            "corridor_id",
            "corridor_label",
            "analysis_role",
            "is_caitian",
            "is_main_control",
            "direction_group",
            "date",
            "dow",
            "day_type",
        ],
        columns="window",
        values=["corridor_speed_kmh", "travel_time_min"],
        aggfunc="first",
    )
    window_pivot.columns = [f"{metric}_{window}" for metric, window in window_pivot.columns]
    window_pivot = window_pivot.reset_index()
    window_pivot["active_over_reference_speed"] = (
        window_pivot["corridor_speed_kmh_active_1630_2130"]
        / window_pivot["corridor_speed_kmh_reference_1500_1630"]
    )
    daily_ratio = window_pivot.copy()

    avg_ratio = (
        daily_ratio[daily_ratio["day_type"].eq("weekday")]
        .groupby(
            [
                "corridor_id",
                "corridor_label",
                "analysis_role",
                "is_caitian",
                "is_main_control",
                "direction_group",
            ],
            as_index=False,
        )
        .agg(
            mean_active_over_reference_speed=("active_over_reference_speed", "mean"),
            n_weekdays=("date", "nunique"),
        )
    )
    treatment_policy = avg_ratio[
        avg_ratio["corridor_id"].eq("caitian")
        & avg_ratio["direction_group"].eq("policy_direction_futian_to_longhua")
    ]
    base = (
        float(treatment_policy["mean_active_over_reference_speed"].iloc[0])
        if not treatment_policy.empty
        else np.nan
    )
    avg_ratio["ratio_of_ratios_vs_caitian_policy"] = (
        base / avg_ratio["mean_active_over_reference_speed"]
    )
    return direction_pivot, daily_ratio, avg_ratio


def build_network_window_summary(speed: pd.DataFrame, network: gpd.GeoDataFrame) -> pd.DataFrame:
    if network.empty:
        return pd.DataFrame()
    attr_cols = [
        "roadseg_id",
        "roadname",
        "chain_direction",
        "network_position",
        "distance_bin",
        "network_distance_m",
        "length_m",
    ]
    attr = network[attr_cols].drop_duplicates("roadseg_id")
    panel = speed.merge(attr, on="roadseg_id", how="inner")
    if panel.empty:
        return pd.DataFrame()
    panel["segment_travel_time_min"] = panel["length_m"] / 1000.0 / panel["speed_kmh"] * 60.0
    rows = []
    for window_name, window in {
        "reference_1500_1630": REFERENCE_WINDOW,
        "active_1630_2130": ACTIVE_WINDOW,
    }.items():
        sub = window_filter(panel, window)
        grouped = (
            sub.groupby(
                ["chain_direction", "network_position", "distance_bin", "date", "day_type"],
                observed=True,
                as_index=False,
            )
            .agg(
                travel_time_min=("segment_travel_time_min", "sum"),
                total_length_m=("length_m", "sum"),
                n_obs=("speed_kmh", "size"),
                n_segments=("roadseg_id", "nunique"),
            )
        )
        grouped["corridor_speed_kmh"] = (
            grouped["total_length_m"] / 1000.0 / (grouped["travel_time_min"] / 60.0)
        )
        grouped["window"] = window_name
        rows.append(grouped)
    out = pd.concat(rows, ignore_index=True)
    pivot = out.pivot_table(
        index=["chain_direction", "network_position", "distance_bin", "date", "day_type"],
        columns="window",
        values="corridor_speed_kmh",
        aggfunc="first",
        observed=True,
    ).reset_index()
    pivot["active_over_reference_speed"] = (
        pivot["active_1630_2130"] / pivot["reference_1500_1630"]
    )
    return pivot


def cluster_covariance(x: np.ndarray, residual: np.ndarray, clusters: np.ndarray) -> np.ndarray:
    xtx = np.einsum("ni,nj->ij", x, x)
    try:
        xtx_inv = np.linalg.inv(xtx)
    except np.linalg.LinAlgError:
        xtx_inv = np.linalg.pinv(xtx)
    meat = np.zeros((x.shape[1], x.shape[1]))
    for cluster in pd.unique(clusters):
        loc = clusters == cluster
        score = np.einsum("ni,n->i", x[loc], residual[loc])
        meat += np.outer(score, score)
    n_obs, n_params = x.shape
    n_clusters = len(pd.unique(clusters))
    if n_clusters > 1 and n_obs > n_params:
        correction = (n_clusters / (n_clusters - 1)) * ((n_obs - 1) / (n_obs - n_params))
    else:
        correction = 1.0
    sandwich = np.einsum("ij,jk,kl->il", xtx_inv, meat, xtx_inv)
    return correction * sandwich


def residualize_matrix(
    matrix: np.ndarray,
    groups: list[pd.Series],
    max_iter: int = 100,
    tol: float = 1e-10,
) -> tuple[np.ndarray, int]:
    residual = matrix.astype(float).copy()
    for iteration in range(1, max_iter + 1):
        previous = residual.copy()
        for group in groups:
            means = (
                pd.DataFrame(residual)
                .groupby(group.to_numpy(), sort=False)
                .transform("mean")
                .to_numpy()
            )
            residual -= means
        if np.max(np.abs(residual - previous)) < tol:
            return residual, iteration
    return residual, max_iter


def fit_event_study(
    df: pd.DataFrame,
    outcome_col: str,
    interaction_col: str,
    unit_col: str,
    reference_bin: int,
    model_name: str,
) -> tuple[pd.DataFrame, dict[str, object]]:
    sample = df.dropna(subset=[outcome_col, "event_bin_min", interaction_col]).copy()
    finite_mask = (
        np.isfinite(sample[outcome_col].to_numpy(dtype=float))
        & np.isfinite(sample["event_bin_min"].to_numpy(dtype=float))
        & np.isfinite(sample[interaction_col].to_numpy(dtype=float))
    )
    sample = sample.loc[finite_mask].copy()
    event_bins = sorted(int(x) for x in sample["event_bin_min"].unique())
    estimated_bins = [x for x in event_bins if x != reference_bin]
    event_matrix_parts = []
    for event_bin in estimated_bins:
        event_matrix_parts.append(
            (
                sample[interaction_col].eq(1) & sample["event_bin_min"].eq(event_bin)
            ).astype(float)
        )
    y = sample[outcome_col].to_numpy(dtype=float)
    event_x = pd.concat(event_matrix_parts, axis=1).to_numpy(dtype=float)
    residualized, n_resid_iter = residualize_matrix(
        np.column_stack([y, event_x]),
        groups=[
            sample[unit_col].astype(str),
            sample["date_id"].astype(str),
            sample["minute_of_day"].astype(str),
        ],
    )
    y_tilde = residualized[:, 0]
    x_tilde = residualized[:, 1:]
    col_norms = np.sqrt(np.sum(x_tilde**2, axis=0))
    keep_cols = col_norms > 1e-10
    beta_full = np.full(len(estimated_bins), np.nan)
    se_full = np.full(len(estimated_bins), np.nan)
    if keep_cols.any():
        x_kept = x_tilde[:, keep_cols]
        xtx = np.einsum("ni,nj->ij", x_kept, x_kept)
        xty = np.einsum("ni,n->i", x_kept, y_tilde)
        try:
            beta = np.linalg.solve(xtx, xty)
        except np.linalg.LinAlgError:
            beta = np.einsum("ij,j->i", np.linalg.pinv(xtx), xty)
        fitted = np.einsum("ni,i->n", x_kept, beta)
        residual = y_tilde - fitted
        vcov = cluster_covariance(x_kept, residual, sample[unit_col].astype(str).to_numpy())
        beta_full[keep_cols] = beta
        se_full[keep_cols] = np.sqrt(np.maximum(np.diag(vcov), 0.0))
    else:
        fitted = np.zeros_like(y_tilde)
        residual = y_tilde.copy()
    coef_lookup = dict(zip(estimated_bins, beta_full))
    se_lookup = dict(zip(estimated_bins, se_full))

    rows = []
    for event_bin in event_bins:
        if event_bin == reference_bin:
            coef = 0.0
            std_err = np.nan
        else:
            coef = float(coef_lookup[event_bin])
            std_err = float(se_lookup[event_bin])
        rows.append(
            {
                "model": model_name,
                "outcome": outcome_col,
                "event_bin_min": event_bin,
                "event_bin_label": event_bin_label(event_bin),
                "reference_bin_min": reference_bin,
                "coef": coef,
                "std_error": std_err,
                "ci95_low": coef - 1.96 * std_err if np.isfinite(std_err) else np.nan,
                "ci95_high": coef + 1.96 * std_err if np.isfinite(std_err) else np.nan,
                "n_obs": int(len(sample)),
            }
        )
    diagnostics = {
        "model": model_name,
        "outcome": outcome_col,
        "n_obs": int(len(sample)),
        "n_units": int(sample[unit_col].nunique()),
        "n_dates": int(sample["date_id"].nunique()),
        "n_minutes": int(sample["minute_of_day"].nunique()),
        "reference_bin_min": int(reference_bin),
        "residualization_iterations": int(n_resid_iter),
        "r_squared_residualized": float(
            1 - np.sum(residual**2) / np.sum((y_tilde - y_tilde.mean()) ** 2)
        ),
    }
    return pd.DataFrame(rows), diagnostics


def event_bin_label(event_bin_min: int) -> str:
    sign = "+" if event_bin_min >= 0 else ""
    return f"{sign}{event_bin_min}m"


def build_event_samples(segment_panel: pd.DataFrame, corridor: pd.DataFrame):
    base = segment_panel[
        segment_panel["day_type"].eq("weekday")
        & (segment_panel["minute_of_day"] >= EVENT_SAMPLE_WINDOW[0])
        & (segment_panel["minute_of_day"] < EVENT_SAMPLE_WINDOW[1])
    ].copy()
    base["event_time_min"] = base["minute_of_day"] - EVENT_START_MIN
    base["event_bin_min"] = np.floor(base["event_time_min"] / 30).astype(int) * 30
    base["policy_direction_interaction"] = base["policy_direction"].astype(int)
    base["caitian_policy_direction_interaction"] = (
        base["is_caitian"].astype(int) * base["policy_direction"].astype(int)
    )
    base["segment_id"] = base["roadseg_id"].astype(str)

    event_corridors = ["caitian"] + [spec[1] for spec in CONTROL_EVENT_SPECS]
    caitian = base[base["corridor_id"].eq("caitian")].copy()
    ddd = base[base["corridor_id"].isin(event_corridors)].copy()

    corridor_sample = corridor[
        corridor["day_type"].eq("weekday")
        & (corridor["minute_of_day"] >= EVENT_SAMPLE_WINDOW[0])
        & (corridor["minute_of_day"] < EVENT_SAMPLE_WINDOW[1])
        & corridor["corridor_id"].isin(event_corridors)
    ].copy()
    corridor_sample["event_time_min"] = corridor_sample["minute_of_day"] - EVENT_START_MIN
    corridor_sample["event_bin_min"] = (
        np.floor(corridor_sample["event_time_min"] / 30).astype(int) * 30
    )
    corridor_sample["corridor_direction_id"] = (
        corridor_sample["corridor_id"] + "_" + corridor_sample["direction_group"]
    )
    corridor_sample["caitian_policy_direction_interaction"] = (
        corridor_sample["is_caitian"].astype(int)
        * corridor_sample["policy_direction"].astype(int)
    )
    return caitian, ddd, corridor_sample


def plot_map(
    roads: gpd.GeoDataFrame,
    tidal_single: gpd.GeoDataFrame,
    tidal_double: gpd.GeoDataFrame,
    treatment_core: gpd.GeoDataFrame,
    treatment_auxiliary: gpd.GeoDataFrame,
    controls: gpd.GeoDataFrame,
    network: gpd.GeoDataFrame,
    output_path: Path,
) -> None:
    layers = [
        tidal_single,
        tidal_double,
        treatment_core,
        treatment_auxiliary,
        controls,
        network,
    ]
    bounds = pd.concat(
        [layer.to_crs(PROJECTED_CRS).geometry.bounds for layer in layers if not layer.empty]
    )
    minx, miny = bounds[["minx", "miny"]].min()
    maxx, maxy = bounds[["maxx", "maxy"]].max()
    pad = 700
    base = roads.to_crs(PROJECTED_CRS)
    fig, ax = plt.subplots(figsize=(7.4, 8.2))
    base.cx[minx - pad : maxx + pad, miny - pad : maxy + pad].plot(
        ax=ax, color="#d8d8d8", linewidth=0.35, alpha=0.55
    )
    if not network.empty:
        network.to_crs(PROJECTED_CRS).plot(ax=ax, color="#999999", linewidth=0.55, alpha=0.28)
    treatment_auxiliary.to_crs(PROJECTED_CRS).plot(
        ax=ax, color="#999999", linewidth=0.85, alpha=0.45, label="Caitian auxiliary"
    )
    for corridor_id, sub in controls.to_crs(PROJECTED_CRS).groupby("corridor_id"):
        sub.plot(ax=ax, linewidth=1.4, alpha=0.78, label=sub["corridor_label"].iloc[0])
    for direction, sub in treatment_core.to_crs(PROJECTED_CRS).groupby("direction_group"):
        color = "#2b6cb0" if direction == "policy_direction_futian_to_longhua" else "#c44e52"
        sub.plot(ax=ax, color=color, linewidth=3.0, alpha=0.95, label=DIRECTION_LABELS[direction])
    tidal_double.to_crs(PROJECTED_CRS).plot(
        ax=ax, color="#111111", linewidth=0.65, alpha=0.35, label="Tidal-lane shp"
    )
    ax.set_xlim(minx - pad, maxx + pad)
    ax.set_ylim(miny - pad, maxy + pad)
    ax.set_title("Caitian Road treatment, controls, and 3 km network candidates", loc="left")
    ax.set_xlabel("UTM easting (m)")
    ax.set_ylabel("UTM northing (m)")
    ax.grid(color="#e6e6e6", linewidth=0.5)
    for spine in ["top", "right"]:
        ax.spines[spine].set_visible(False)
    ax.legend(frameon=False, loc="lower left", ncol=1)
    fig.text(
        0.01,
        0.01,
        "Notes: Thick blue/red lines are the core Caitian treatment chains. "
        "Grey lines are 3 km upstream/downstream candidates from the Baidu directed graph.",
        fontsize=7.2,
        color="#444444",
    )
    fig.tight_layout(rect=(0, 0.04, 1, 1))
    save_figure(fig, output_path)


def plot_diurnal(diurnal: pd.DataFrame, output_path: Path) -> None:
    corridors = ["caitian", "control_huanggang", "control_jintian", "control_yitian", "control_xiangmihu"]
    plot_data = diurnal[
        diurnal["corridor_id"].isin(corridors)
        & diurnal["day_type"].isin(["weekday", "weekend"])
    ].copy()
    labels = (
        plot_data[["corridor_id", "corridor_label"]]
        .drop_duplicates()
        .set_index("corridor_id")["corridor_label"]
        .to_dict()
    )
    fig, axes = plt.subplots(len(corridors), 2, figsize=(12.0, 10.0), sharex=True, sharey=False)
    colors = {
        "policy_direction_futian_to_longhua": "#2b6cb0",
        "opposite_direction_longhua_to_futian": "#c44e52",
    }
    for i, corridor_id in enumerate(corridors):
        for j, day_type in enumerate(["weekday", "weekend"]):
            ax = axes[i, j]
            sub = plot_data[
                plot_data["corridor_id"].eq(corridor_id) & plot_data["day_type"].eq(day_type)
            ]
            for direction, part in sub.groupby("direction_group"):
                if direction not in colors:
                    continue
                ax.plot(
                    part["minute_of_day"] / 60.0,
                    part["corridor_speed_kmh"],
                    color=colors[direction],
                    linewidth=1.3,
                    marker="o",
                    markersize=1.8,
                    label=DIRECTION_LABELS[direction],
                )
            ax.axvspan(16.5, 21.5, color="#eeeeee", alpha=0.65)
            if i == 0:
                ax.set_title(day_type.title(), loc="left", fontweight="bold")
            ax.text(
                0.01,
                0.88,
                labels.get(corridor_id, corridor_id),
                transform=ax.transAxes,
                fontsize=8.5,
                fontweight="bold",
            )
            ax.grid(axis="y", color="#e5e5e5", linewidth=0.5)
            for spine in ["top", "right"]:
                ax.spines[spine].set_visible(False)
    axes[-1, 0].set_xlabel("Hour of day")
    axes[-1, 1].set_xlabel("Hour of day")
    for ax in axes[:, 0]:
        ax.set_ylabel("Speed (km/h)")
    axes[0, 1].legend(frameon=False, loc="upper right")
    fig.suptitle("Caitian and control corridor diurnal speed profiles", x=0.01, ha="left")
    fig.tight_layout(rect=(0, 0.02, 1, 0.96))
    save_figure(fig, output_path)


def plot_daily_windows(daily: pd.DataFrame, output_path: Path) -> None:
    windows = ["reference_1500_1630", "active_1630_2130", "post_2130_2230"]
    sub = daily[daily["window"].isin(windows) & daily["day_type"].eq("weekday")].copy()
    summary = (
        sub.groupby(["corridor_id", "corridor_label", "direction_group", "window"], as_index=False)
        .agg(corridor_speed_kmh=("corridor_speed_kmh", "mean"))
    )
    corridors = ["caitian", "control_huanggang", "control_jintian", "control_yitian", "control_xiangmihu"]
    fig, axes = plt.subplots(len(corridors), 1, figsize=(9.0, 9.0), sharex=True)
    x = np.arange(len(windows))
    width = 0.36
    colors = {
        "policy_direction_futian_to_longhua": "#2b6cb0",
        "opposite_direction_longhua_to_futian": "#c44e52",
    }
    for ax, corridor_id in zip(axes, corridors):
        part = summary[summary["corridor_id"].eq(corridor_id)]
        label = part["corridor_label"].iloc[0] if not part.empty else corridor_id
        for offset, direction in [(-width / 2, "policy_direction_futian_to_longhua"), (width / 2, "opposite_direction_longhua_to_futian")]:
            vals = (
                part[part["direction_group"].eq(direction)]
                .set_index("window")
                .reindex(windows)["corridor_speed_kmh"]
            )
            ax.bar(x + offset, vals, width=width, color=colors[direction], alpha=0.86, label=DIRECTION_LABELS[direction])
        ax.set_ylabel("km/h")
        ax.set_title(label, loc="left", fontweight="bold", fontsize=9)
        ax.grid(axis="y", color="#e5e5e5", linewidth=0.5)
        for spine in ["top", "right"]:
            ax.spines[spine].set_visible(False)
    axes[-1].set_xticks(x)
    axes[-1].set_xticklabels([WINDOW_LABELS[w] for w in windows], rotation=20, ha="right")
    axes[0].legend(frameon=False, loc="upper right")
    fig.suptitle("Weekday average speeds by diagnostic window", x=0.01, ha="left")
    fig.tight_layout(rect=(0, 0.04, 1, 0.96))
    save_figure(fig, output_path)


def plot_ratios(direction_ratio: pd.DataFrame, daily_ratio: pd.DataFrame, output_path: Path) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(12.0, 4.5))
    ratio_diurnal = (
        direction_ratio[direction_ratio["day_type"].eq("weekday")]
        .groupby(["corridor_id", "corridor_label", "minute_of_day"], as_index=False)
        .agg(policy_over_opposite_speed=("policy_over_opposite_speed", "mean"))
    )
    for corridor_id, part in ratio_diurnal.groupby("corridor_id"):
        lw = 2.2 if corridor_id == "caitian" else 1.1
        alpha = 0.95 if corridor_id == "caitian" else 0.65
        axes[0].plot(
            part["minute_of_day"] / 60.0,
            part["policy_over_opposite_speed"],
            linewidth=lw,
            marker="o",
            markersize=2.0 if corridor_id == "caitian" else 1.6,
            alpha=alpha,
            label=part["corridor_label"].iloc[0],
        )
    axes[0].axhline(1, color="#333333", linewidth=0.8, linestyle="--")
    axes[0].axvspan(16.5, 21.5, color="#eeeeee", alpha=0.65)
    axes[0].set_title("a. Directional speed ratio", loc="left", fontweight="bold")
    axes[0].set_xlabel("Hour")
    axes[0].set_ylabel("Policy direction / opposite speed")
    axes[0].legend(frameon=False, fontsize=7)

    plot_daily = daily_ratio[
        daily_ratio["day_type"].eq("weekday")
        & daily_ratio["direction_group"].eq("policy_direction_futian_to_longhua")
    ].copy()
    order = ["caitian", "control_huanggang", "control_jintian", "control_yitian", "control_xiangmihu"]
    summary = (
        plot_daily.groupby(["corridor_id", "corridor_label"], as_index=False)
        .agg(active_over_reference_speed=("active_over_reference_speed", "mean"))
        .set_index("corridor_id")
        .reindex(order)
        .reset_index()
    )
    axes[1].bar(
        np.arange(len(summary)),
        summary["active_over_reference_speed"],
        color=["#2b6cb0"] + ["#999999"] * (len(summary) - 1),
        alpha=0.85,
    )
    axes[1].axhline(1, color="#333333", linewidth=0.8, linestyle="--")
    axes[1].set_xticks(np.arange(len(summary)))
    axes[1].set_xticklabels(summary["corridor_label"], rotation=25, ha="right")
    axes[1].set_ylabel("16:30-21:30 / 15:00-16:30 speed")
    axes[1].set_title("b. Active/reference ratio, policy direction", loc="left", fontweight="bold")
    for ax in axes:
        ax.grid(axis="y", color="#e5e5e5", linewidth=0.5)
        for spine in ["top", "right"]:
            ax.spines[spine].set_visible(False)
    fig.tight_layout()
    save_figure(fig, output_path)


def plot_network(network_summary: pd.DataFrame, output_path: Path) -> None:
    if network_summary.empty:
        return
    summary = (
        network_summary[network_summary["day_type"].eq("weekday")]
        .groupby(["chain_direction", "network_position", "distance_bin"], observed=True, as_index=False)
        .agg(active_over_reference_speed=("active_over_reference_speed", "mean"))
    )
    fig, axes = plt.subplots(1, 2, figsize=(10.5, 4.2), sharey=True)
    colors = {
        "policy_direction_futian_to_longhua": "#2b6cb0",
        "opposite_direction_longhua_to_futian": "#c44e52",
    }
    present_directions: set[str] = set()
    for ax, position in zip(axes, ["upstream", "downstream"]):
        sub = summary[summary["network_position"].eq(position)]
        pivot = sub.pivot_table(
            index="distance_bin",
            columns="chain_direction",
            values="active_over_reference_speed",
            observed=False,
        )
        present_directions.update(pivot.columns)
        pivot.plot(
            kind="bar",
            ax=ax,
            color=[colors.get(c, "#999999") for c in pivot.columns],
            alpha=0.85,
        )
        ax.axhline(1, color="#333333", linewidth=0.8, linestyle="--")
        ax.set_title(position.title(), loc="left", fontweight="bold")
        ax.set_xlabel("Directed distance from treatment endpoint")
        ax.set_ylabel("Active/reference speed")
        legend = ax.get_legend()
        if legend is not None:
            legend.remove()
        ax.grid(axis="y", color="#e5e5e5", linewidth=0.5)
        for spine in ["top", "right"]:
            ax.spines[spine].set_visible(False)
    legend_order = [
        "policy_direction_futian_to_longhua",
        "opposite_direction_longhua_to_futian",
    ]
    handles = [
        Patch(facecolor=colors[k], edgecolor=colors[k], label=DIRECTION_LABELS.get(k, k))
        for k in legend_order
        if k in present_directions
    ]
    if handles:
        fig.legend(handles=handles, frameon=False, loc="center left", bbox_to_anchor=(0.88, 0.56))
    fig.suptitle("3 km network active/reference speed ratio", x=0.01, ha="left")
    fig.tight_layout(rect=(0, 0, 0.86, 0.94))
    save_figure(fig, output_path)


def plot_event_study(coefficients: pd.DataFrame, output_path: Path, title: str) -> None:
    fig, ax = plt.subplots(figsize=(8.2, 4.2))
    data = coefficients.sort_values("event_bin_min")
    outcome = data["outcome"].iloc[0] if "outcome" in data.columns and not data.empty else ""
    ylabel = "Coefficient, log minutes" if outcome == "log_travel_time" else "Coefficient, km/h"
    ax.errorbar(
        data["event_bin_min"],
        data["coef"],
        yerr=[
            data["coef"] - data["ci95_low"],
            data["ci95_high"] - data["coef"],
        ],
        fmt="o-",
        color="#2b6cb0",
        linewidth=1.25,
        markersize=4,
        capsize=2.5,
    )
    ax.axhline(0, color="#333333", linewidth=0.8)
    ax.axvline(0, color="#555555", linewidth=0.8, linestyle="--")
    ax.axvspan(0, ACTIVE_WINDOW[1] - ACTIVE_WINDOW[0], color="#eeeeee", alpha=0.65)
    ax.set_title(title, loc="left", fontweight="bold")
    ax.set_xlabel("Minutes relative to 16:30")
    ax.set_ylabel(ylabel)
    ax.grid(axis="y", color="#e5e5e5", linewidth=0.5)
    for spine in ["top", "right"]:
        ax.spines[spine].set_visible(False)
    fig.tight_layout()
    save_figure(fig, output_path)


def write_tables(
    dirs: dict[str, Path],
    buffer_matches: dict[int, gpd.GeoDataFrame],
    buffer_summary: pd.DataFrame,
    treatment_core: gpd.GeoDataFrame,
    treatment_auxiliary: gpd.GeoDataFrame,
    controls: gpd.GeoDataFrame,
    network: gpd.GeoDataFrame,
    analysis_segments: gpd.GeoDataFrame,
    auxiliary_segments: gpd.GeoDataFrame,
    selected_speed: pd.DataFrame,
    segment_panel: pd.DataFrame,
    speed_coverage: pd.DataFrame,
    corridor: pd.DataFrame,
    daily_window: pd.DataFrame,
    diurnal: pd.DataFrame,
    direction_ratio: pd.DataFrame,
    daily_ratio: pd.DataFrame,
    avg_ratio: pd.DataFrame,
    network_summary: pd.DataFrame,
    event_outputs: dict[str, tuple[pd.DataFrame, dict[str, object]]],
) -> None:
    for buffer_m, match in buffer_matches.items():
        match.drop(columns="geometry").to_csv(
            dirs["data"] / f"treatment_buffer_matches_{buffer_m}m.csv",
            index=False,
            encoding="utf-8-sig",
        )
    buffer_summary.to_csv(
        dirs["metrics"] / "treatment_buffer_match_summary.csv",
        index=False,
        encoding="utf-8-sig",
    )
    treatment_core.to_file(dirs["data"] / "treatment_core_segments.geojson", driver="GeoJSON")
    treatment_core.drop(columns="geometry").to_csv(
        dirs["metrics"] / "treatment_core_segments.csv",
        index=False,
        encoding="utf-8-sig",
    )
    treatment_auxiliary.drop(columns="geometry").to_csv(
        dirs["metrics"] / "treatment_auxiliary_segments.csv",
        index=False,
        encoding="utf-8-sig",
    )
    controls.to_file(dirs["data"] / "control_candidate_segments.geojson", driver="GeoJSON")
    controls.drop(columns="geometry").to_csv(
        dirs["metrics"] / "control_candidate_segments.csv",
        index=False,
        encoding="utf-8-sig",
    )
    network.to_file(dirs["data"] / "network_segments_3km.geojson", driver="GeoJSON")
    network.drop(columns="geometry").to_csv(
        dirs["metrics"] / "network_segments_3km.csv",
        index=False,
        encoding="utf-8-sig",
    )
    analysis_segments.to_file(dirs["data"] / "analysis_segment_catalog.geojson", driver="GeoJSON")
    analysis_segments.drop(columns="geometry").to_csv(
        dirs["data"] / "analysis_segment_catalog.csv",
        index=False,
        encoding="utf-8-sig",
    )
    auxiliary_segments.drop(columns="geometry").to_csv(
        dirs["data"] / "auxiliary_segment_catalog.csv",
        index=False,
        encoding="utf-8-sig",
    )
    selected_speed.to_csv(
        dirs["data"] / "selected_speed_panel_raw.csv",
        index=False,
        encoding="utf-8-sig",
    )
    segment_panel.to_csv(
        dirs["data"] / "selected_segment_speed_panel.csv",
        index=False,
        encoding="utf-8-sig",
    )
    speed_coverage.to_csv(
        dirs["metrics"] / "speed_coverage_by_segment.csv",
        index=False,
        encoding="utf-8-sig",
    )
    corridor.to_csv(
        dirs["data"] / "corridor_10min_speed.csv",
        index=False,
        encoding="utf-8-sig",
    )
    daily_window.to_csv(
        dirs["data"] / "daily_window_summary.csv",
        index=False,
        encoding="utf-8-sig",
    )
    diurnal.to_csv(
        dirs["data"] / "diurnal_speed_by_daytype.csv",
        index=False,
        encoding="utf-8-sig",
    )
    direction_ratio.to_csv(
        dirs["data"] / "directional_speed_ratios_10min.csv",
        index=False,
        encoding="utf-8-sig",
    )
    daily_ratio.to_csv(
        dirs["data"] / "active_reference_daily_ratios.csv",
        index=False,
        encoding="utf-8-sig",
    )
    avg_ratio.to_csv(
        dirs["metrics"] / "active_reference_ratio_summary.csv",
        index=False,
        encoding="utf-8-sig",
    )
    if not network_summary.empty:
        network_summary.to_csv(
            dirs["data"] / "network_active_reference_by_distance.csv",
            index=False,
            encoding="utf-8-sig",
        )
    diagnostics = {}
    for name, (coef, diag) in event_outputs.items():
        coef.to_csv(
            dirs["metrics"] / f"{name}_coefficients.csv",
            index=False,
            encoding="utf-8-sig",
        )
        diagnostics[name] = diag
    with (dirs["metrics"] / "event_study_diagnostics.json").open("w", encoding="utf-8") as fh:
        json.dump(diagnostics, fh, ensure_ascii=False, indent=2)


def main() -> None:
    args = parse_args()
    configure_plot()
    dirs = ensure_output_dirs(args.output_dir)

    roads = add_road_geometry_columns(gpd.read_file(args.roads))
    tidal_single = gpd.read_file(args.tidal_single)
    tidal_double = gpd.read_file(args.tidal_double)
    tidal_buffers = build_tidal_buffers(tidal_single)
    buffer_matches, buffer_summary = compute_buffer_matches(roads, tidal_buffers)

    treatment_core = make_treatment_core(roads, buffer_matches[MAIN_BUFFER_M])
    treatment_auxiliary = make_auxiliary_treatment_candidates(
        roads, buffer_matches[MAIN_BUFFER_M], set(treatment_core["roadseg_id"])
    )
    controls = select_controls(roads, tidal_buffers[MAIN_BUFFER_M])
    network = build_network_segments(roads, treatment_core)
    analysis_segments, auxiliary_segments = make_segment_catalog(
        treatment_core, treatment_auxiliary, controls, network
    )

    selected_ids = set(analysis_segments["roadseg_id"]).union(set(network["roadseg_id"]))
    selected_speed = read_selected_speed_panel(args.speed, selected_ids)
    segment_panel = attach_segment_attributes(selected_speed, analysis_segments)
    speed_coverage = build_speed_coverage(selected_speed, analysis_segments)
    corridor = build_corridor_10min_speed(segment_panel)
    daily_window = build_daily_window_summary(corridor)
    diurnal = build_diurnal_summary(corridor)
    direction_ratio, daily_ratio, avg_ratio = build_ratios(corridor, daily_window)
    network_summary = build_network_window_summary(selected_speed, network)
    caitian_event, ddd_event, corridor_event = build_event_samples(segment_panel, corridor)

    event_outputs = {}
    event_outputs["event_study_treatment_only_speed"] = fit_event_study(
        caitian_event,
        outcome_col="speed_kmh",
        interaction_col="policy_direction_interaction",
        unit_col="segment_id",
        reference_bin=-30,
        model_name="Treatment-only direction ES, speed",
    )
    for suffix, control_id, control_label in CONTROL_EVENT_SPECS:
        control_sample = ddd_event[
            ddd_event["corridor_id"].isin(["caitian", control_id])
        ].copy()
        event_outputs[f"event_study_ddd_speed_{suffix}"] = fit_event_study(
            control_sample,
            outcome_col="speed_kmh",
            interaction_col="caitian_policy_direction_interaction",
            unit_col="segment_id",
            reference_bin=-30,
            model_name=f"Caitian vs {control_label} DDD ES, speed",
        )
    event_outputs["event_study_treatment_only_log_travel_time"] = fit_event_study(
        caitian_event,
        outcome_col="log_travel_time",
        interaction_col="policy_direction_interaction",
        unit_col="segment_id",
        reference_bin=-30,
        model_name="Treatment-only direction ES, log travel time",
    )
    corridor_huanggang = corridor_event[
        corridor_event["corridor_id"].isin(["caitian", "control_huanggang"])
    ].copy()
    event_outputs["event_study_corridor_ddd_speed_huanggang"] = fit_event_study(
        corridor_huanggang,
        outcome_col="corridor_speed_kmh",
        interaction_col="caitian_policy_direction_interaction",
        unit_col="corridor_direction_id",
        reference_bin=-30,
        model_name="Corridor-level Caitian vs Huanggang DDD ES, speed",
    )
    write_tables(
        dirs,
        buffer_matches,
        buffer_summary,
        treatment_core,
        treatment_auxiliary,
        controls,
        network,
        analysis_segments,
        auxiliary_segments,
        selected_speed,
        segment_panel,
        speed_coverage,
        corridor,
        daily_window,
        diurnal,
        direction_ratio,
        daily_ratio,
        avg_ratio,
        network_summary,
        event_outputs,
    )

    plot_map(
        roads,
        tidal_single,
        tidal_double,
        treatment_core,
        treatment_auxiliary,
        controls,
        network,
        dirs["figures"] / "caitian_treatment_control_network_map.png",
    )
    plot_diurnal(diurnal, dirs["figures"] / "caitian_control_diurnal_speed_by_daytype.png")
    plot_daily_windows(daily_window, dirs["figures"] / "caitian_control_daily_window_speeds.png")
    plot_ratios(direction_ratio, daily_ratio, dirs["figures"] / "caitian_control_speed_ratios.png")
    plot_network(network_summary, dirs["figures"] / "caitian_network_active_reference_by_distance.png")
    event_plot_titles = {
        "event_study_treatment_only_speed": "Post-only event study: Caitian direction difference",
        "event_study_treatment_only_log_travel_time": "Post-only event study: Caitian log travel time",
        "event_study_corridor_ddd_speed_huanggang": "Post-only event study: corridor speed, Caitian vs Huanggang",
    }
    for suffix, _control_id, control_label in CONTROL_EVENT_SPECS:
        event_plot_titles[f"event_study_ddd_speed_{suffix}"] = (
            f"Post-only event study: Caitian vs {control_label} DDD"
        )
    for name, title in event_plot_titles.items():
        if name not in event_outputs:
            continue
        plot_event_study(event_outputs[name][0], dirs["figures"] / f"{name}.png", title)

    metadata = {
        "analysis_window": "16:30-21:30",
        "reference_window": "15:00-16:30",
        "post_window": "21:30-22:30",
        "operation_window_config": "data_work/config/shenzhen_tidal_lane_operation_windows_v1.json",
        "interpretation": "post-only within-day event-time analysis, not pre/post causal event study",
        "sources": {
            "tidal_single": str(args.tidal_single),
            "tidal_double": str(args.tidal_double),
            "roads": str(args.roads),
            "speed": str(args.speed),
        },
        "output_dir": str(args.output_dir),
        "n_treatment_core_segments": int(treatment_core["roadseg_id"].nunique()),
        "n_control_segments": int(controls["roadseg_id"].nunique()),
        "n_network_segments": int(network["roadseg_id"].nunique()) if not network.empty else 0,
        "buffer_sensitivity_m": BUFFER_SENSITIVITY_M,
        "network_distance_m": NETWORK_DISTANCE_M,
        "main_control": "control_huanggang",
        "robustness_controls": ["control_jintian", "control_yitian", "control_xiangmihu"],
        "public_reference_urls": [
            "https://szjj.sz.gov.cn/m/YD_JGDT/content/post_12699765.html",
            "https://ga.sz.gov.cn/ZWGK/QT/GSGG/content/post_12158588.html",
        ],
    }
    with (dirs["metrics"] / "run_metadata.json").open("w", encoding="utf-8") as fh:
        json.dump(metadata, fh, ensure_ascii=False, indent=2)
    event_study_specifications = {
        "sample": "Post-only weekdays, 15:00-22:30, 10-minute Baidu speed observations",
        "event_time_zero": "16:30 in the main specification",
        "omitted_bin": "16:00-16:30, event_bin_min = -30",
        "fixed_effects": ["segment", "date", "minute_of_day"],
        "cluster": "segment",
        "treatment_only_direction_es": (
            "speed_{i d t} = alpha_i + delta_d + mu_t + "
            "sum_k beta_k PolicyDirection_i * EventBin_{k,t} + epsilon_{i d t}"
        ),
        "treatment_control_ddd_es": (
            "speed_{i d t} = alpha_i + delta_d + mu_t + "
            "sum_k beta_k Caitian_i * PolicyDirection_i * EventBin_{k,t} + epsilon_{i d t}"
        ),
        "robustness": [
            "DDD speed with Huanggang, Jintian, Yitian, and Xiangmihu controls",
            "Treatment-only log travel time",
            "Corridor-level harmonic speed for Caitian vs Huanggang",
        ],
        "interpretation": "Within-day post-only event-time association, not a pre/post causal opening effect",
    }
    with (dirs["metrics"] / "event_study_specifications.json").open("w", encoding="utf-8") as fh:
        json.dump(event_study_specifications, fh, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
