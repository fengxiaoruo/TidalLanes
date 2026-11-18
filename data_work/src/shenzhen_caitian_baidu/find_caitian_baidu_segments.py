from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from shapely.geometry import Point
from pyproj import Geod


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_ROADS = PROJECT_ROOT / "data_work/raw_data/gis/roads_baidu/shenzhen_roads.shp"
DEFAULT_SPEED = PROJECT_ROOT / "data_work/raw_data/speed_shenzhen_all_wgs84.csv"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data_work/outputs/shenzhen_caitian_baidu_candidates_v1"

PROJECTED_CRS = "EPSG:32650"

# User-provided approximate endpoints. The first point is retained only as a diagnostic,
# because it is too far from the Shenzhen Caitian Road corridor to define a 4 km segment.
USER_NORTH_POINT = (114.408944115753, 22.7662587886022)
USER_SOUTH_POINT = (114.075325484056, 22.5567281137146)
DECIMAL_SHIFT_GUESS_NORTH_POINT = (114.0408944115753, 22.57662587886022)

CORRIDOR_ROADNAMES = {"彩田路", "新彩隧道"}
CONNECTOR_ROADNAMES = {"北环彩田立交桥", "皇岗彩田立交", "彩田路辅路"}
TEXT_PATTERNS = ("彩田", "新彩")

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

# Empirical bounds from the Baidu road geometries for 新彩隧道 to 红荔路.
MIN_CORRIDOR_LAT = 22.5510
MAX_CORRIDOR_LAT = 22.5920
MIN_CORRIDOR_LON = 114.0460
MAX_CORRIDOR_LON = 114.0645


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Find Baidu road segments covering Caitian Road between Xincai Tunnel and Hongli Road."
    )
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


def endpoint_coords(geometry) -> tuple[float, float, float, float]:
    coords = list(geometry.coords)
    start_lon, start_lat = coords[0]
    end_lon, end_lat = coords[-1]
    return start_lon, start_lat, end_lon, end_lat


def bearing_degrees(start_lon: float, start_lat: float, end_lon: float, end_lat: float) -> float:
    dy = (end_lat - start_lat) * 111_320
    dx = (end_lon - start_lon) * 111_320 * math.cos(math.radians((start_lat + end_lat) / 2))
    return (math.degrees(math.atan2(dx, dy)) + 360) % 360


def semantic_direction(semantic: str) -> str:
    if not isinstance(semantic, str) or "," not in semantic:
        return ""
    return semantic.rsplit(",", 1)[-1]


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


def add_geometry_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    out = gdf.copy()
    projected = out.to_crs(PROJECTED_CRS)
    out["length_m"] = projected.geometry.length.to_numpy()
    endpoints = out.geometry.apply(endpoint_coords)
    out[["start_lon", "start_lat", "end_lon", "end_lat"]] = pd.DataFrame(
        endpoints.tolist(), index=out.index
    )
    out["bearing_deg"] = out.apply(
        lambda row: bearing_degrees(row.start_lon, row.start_lat, row.end_lon, row.end_lat),
        axis=1,
    )
    out["direction_text"] = out["semantic"].apply(semantic_direction)
    out["direction_group"] = out.apply(
        lambda row: direction_group(row.start_lat, row.end_lat, row.direction_text),
        axis=1,
    )
    bounds = out.geometry.bounds
    out["min_lon"] = bounds.minx
    out["max_lon"] = bounds.maxx
    out["min_lat"] = bounds.miny
    out["max_lat"] = bounds.maxy
    projected_centroid = projected.geometry.centroid.to_crs(out.crs)
    out["centroid_lon"] = projected_centroid.x
    out["centroid_lat"] = projected_centroid.y
    return out


def in_corridor_bounds(gdf: gpd.GeoDataFrame) -> pd.Series:
    return (
        (gdf["centroid_lat"] >= MIN_CORRIDOR_LAT)
        & (gdf["centroid_lat"] <= MAX_CORRIDOR_LAT)
        & (gdf["centroid_lon"] >= MIN_CORRIDOR_LON)
        & (gdf["centroid_lon"] <= MAX_CORRIDOR_LON)
    )


def build_candidates(roads: gpd.GeoDataFrame) -> tuple[gpd.GeoDataFrame, pd.DataFrame]:
    roads = add_geometry_columns(roads)
    text_mask = roads["roadname"].fillna("").str.contains("|".join(TEXT_PATTERNS)) | roads[
        "semantic"
    ].fillna("").str.contains("|".join(TEXT_PATTERNS))
    text_candidates = roads.loc[text_mask].copy()

    main_mask = roads["roadname"].isin(CORRIDOR_ROADNAMES) & in_corridor_bounds(roads)
    connector_mask = (
        roads["roadname"].isin(CONNECTOR_ROADNAMES)
        & roads["semantic"].fillna("").str.contains("彩田")
        & in_corridor_bounds(roads)
    )
    corridor = roads.loc[main_mask | connector_mask].copy()
    corridor["candidate_class"] = np.select(
        [
            corridor["roadname"].isin(CORRIDOR_ROADNAMES),
            corridor["roadname"].isin(CONNECTOR_ROADNAMES),
        ],
        ["main_corridor_or_tunnel", "connector_or_ramp"],
        default="other",
    )
    corridor["policy_direction_candidate"] = corridor["direction_group"].eq(
        "policy_direction_futian_to_longhua"
    )
    corridor["core_chain_candidate"] = corridor["roadseg_id"].isin(
        POLICY_CORE_CHAIN_IDS + OPPOSITE_CORE_CHAIN_IDS
    )
    core_sequence = {rid: idx + 1 for idx, rid in enumerate(POLICY_CORE_CHAIN_IDS)}
    core_sequence.update({rid: idx + 1 for idx, rid in enumerate(OPPOSITE_CORE_CHAIN_IDS)})
    corridor["core_sequence"] = corridor["roadseg_id"].map(core_sequence)

    corridor = add_sequence(corridor)
    text_table = text_candidates.drop(columns="geometry").sort_values(
        ["centroid_lat", "roadname", "roadseg_id"], ascending=[False, True, True]
    )
    return corridor, text_table


def add_sequence(corridor: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    pieces = []
    for group, df in corridor.groupby("direction_group", sort=False):
        if group == "policy_direction_futian_to_longhua":
            df = df.sort_values(["start_lat", "end_lat", "roadseg_id"], ascending=[True, True, True])
        elif group == "opposite_direction_longhua_to_futian":
            df = df.sort_values(["start_lat", "end_lat", "roadseg_id"], ascending=[False, False, True])
        else:
            df = df.sort_values(["centroid_lat", "roadseg_id"], ascending=[False, True])
        df = df.copy()
        df["sequence_in_direction"] = np.arange(1, len(df) + 1)
        pieces.append(df)
    out = pd.concat(pieces, ignore_index=False)
    return out.sort_values(
        ["direction_group", "sequence_in_direction", "roadname", "roadseg_id"]
    ).copy()


def speed_summary(speed_path: Path) -> pd.DataFrame:
    usecols = ["roadseg_id", "speed", "day", "hour"]
    speed = pd.read_csv(speed_path, usecols=usecols)
    summary = (
        speed.groupby("roadseg_id", as_index=False)
        .agg(
            n_speed_obs=("speed", "size"),
            mean_speed_kmh=("speed", "mean"),
            min_speed_kmh=("speed", "min"),
            max_speed_kmh=("speed", "max"),
            first_day=("day", "min"),
            last_day=("day", "max"),
            first_hour=("hour", "min"),
            last_hour=("hour", "max"),
        )
        .sort_values("roadseg_id")
    )
    return summary


def attach_speed(corridor: gpd.GeoDataFrame, text_table: pd.DataFrame, speed: pd.DataFrame):
    corridor = corridor.merge(speed, on="roadseg_id", how="left")
    corridor["has_speed_panel"] = corridor["n_speed_obs"].fillna(0).gt(0)
    text_table = text_table.merge(speed, on="roadseg_id", how="left")
    text_table["has_speed_panel"] = text_table["n_speed_obs"].fillna(0).gt(0)
    return corridor, text_table


def user_point_diagnostics(corridor: gpd.GeoDataFrame) -> dict[str, float | str]:
    geod = Geod(ellps="WGS84")
    _, _, user_distance = geod.inv(
        USER_NORTH_POINT[0], USER_NORTH_POINT[1], USER_SOUTH_POINT[0], USER_SOUTH_POINT[1]
    )
    _, _, shifted_distance = geod.inv(
        DECIMAL_SHIFT_GUESS_NORTH_POINT[0],
        DECIMAL_SHIFT_GUESS_NORTH_POINT[1],
        USER_SOUTH_POINT[0],
        USER_SOUTH_POINT[1],
    )
    corridor_projected = corridor.to_crs(PROJECTED_CRS)
    user_point = gpd.GeoSeries([Point(USER_NORTH_POINT)], crs="EPSG:4326").to_crs(PROJECTED_CRS).iloc[0]
    south_point = gpd.GeoSeries([Point(USER_SOUTH_POINT)], crs="EPSG:4326").to_crs(PROJECTED_CRS).iloc[0]
    shifted_point = gpd.GeoSeries([Point(DECIMAL_SHIFT_GUESS_NORTH_POINT)], crs="EPSG:4326").to_crs(
        PROJECTED_CRS
    ).iloc[0]
    union = corridor_projected.geometry.union_all()
    return {
        "user_supplied_endpoint_distance_m": float(user_distance),
        "decimal_shift_guess_endpoint_distance_m": float(shifted_distance),
        "user_north_point_distance_to_selected_corridor_m": float(user_point.distance(union)),
        "user_south_point_distance_to_selected_corridor_m": float(south_point.distance(union)),
        "decimal_shift_guess_north_point_distance_to_selected_corridor_m": float(
            shifted_point.distance(union)
        ),
        "note": (
            "The supplied first coordinate is retained as a diagnostic only; "
            "the selected corridor is based on Baidu road names and geometries."
        ),
    }


def table_columns() -> list[str]:
    return [
        "sequence_in_direction",
        "roadseg_id",
        "roadname",
        "candidate_class",
        "core_chain_candidate",
        "core_sequence",
        "direction_group",
        "policy_direction_candidate",
        "direction_text",
        "semantic",
        "length_m",
        "start_lon",
        "start_lat",
        "end_lon",
        "end_lat",
        "bearing_deg",
        "n_speed_obs",
        "mean_speed_kmh",
        "first_day",
        "last_day",
        "has_speed_panel",
    ]


def plot_corridor(corridor: gpd.GeoDataFrame, output_path: Path) -> None:
    plot_gdf = corridor.to_crs(PROJECTED_CRS)
    colors = {
        "policy_direction_futian_to_longhua": "#2b6cb0",
        "opposite_direction_longhua_to_futian": "#c44e52",
        "connector_or_cross": "#666666",
    }
    labels = {
        "policy_direction_futian_to_longhua": "Futian -> Longhua",
        "opposite_direction_longhua_to_futian": "Longhua -> Futian",
        "connector_or_cross": "Connector/ramp",
    }
    fig, ax = plt.subplots(figsize=(6.3, 7.2))
    for group, sub in plot_gdf.groupby("direction_group"):
        linewidth = np.where(
            sub["core_chain_candidate"],
            3.0,
            np.where(sub["candidate_class"].eq("connector_or_ramp"), 1.15, 1.75),
        )
        alpha = np.where(sub["core_chain_candidate"], 0.92, 0.32)
        sub.plot(
            ax=ax,
            color=colors.get(group, "#666666"),
            linewidth=linewidth,
            alpha=alpha,
            label=labels.get(group, group),
        )
    for _, row in plot_gdf.iterrows():
        if not row["core_chain_candidate"]:
            continue
        point = row.geometry.interpolate(0.5, normalized=True)
        ax.text(
            point.x,
            point.y,
            str(int(row["core_sequence"])),
            fontsize=7,
            ha="center",
            va="center",
            color="#111111",
            bbox={"facecolor": "white", "edgecolor": "none", "alpha": 0.75, "pad": 0.6},
        )

    ax.set_title("Baidu segments near Caitian Road, Xincai Tunnel to Hongli Road", loc="left")
    ax.set_xlabel("UTM easting (m)")
    ax.set_ylabel("UTM northing (m)")
    ax.legend(frameon=False, loc="lower left")
    ax.grid(color="#e5e5e5", linewidth=0.5)
    for spine in ["top", "right"]:
        ax.spines[spine].set_visible(False)
    fig.text(
        0.01,
        0.01,
        "Notes: Number labels mark sequence within each direction group. "
        "Thick lines are core continuous chains; faint lines are nearby auxiliary connector/ramp candidates.",
        fontsize=7.2,
        color="#444444",
    )
    fig.tight_layout(rect=(0, 0.045, 1, 1))
    fig.savefig(output_path, bbox_inches="tight")
    fig.savefig(output_path.with_suffix(".pdf"), bbox_inches="tight")
    fig.savefig(output_path.with_suffix(".svg"), bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    args = parse_args()
    configure_plot()
    dirs = ensure_output_dirs(args.output_dir)

    roads = gpd.read_file(args.roads)
    corridor, text_candidates = build_candidates(roads)
    speeds = speed_summary(args.speed)
    corridor, text_candidates = attach_speed(corridor, text_candidates, speeds)

    table = corridor.drop(columns="geometry").copy()
    table = table[table_columns()].sort_values(
        ["direction_group", "sequence_in_direction", "roadname", "roadseg_id"]
    )
    table.to_csv(
        dirs["metrics"] / "caitian_baidu_corridor_segments.csv",
        index=False,
        encoding="utf-8-sig",
    )
    core_table = table[table["core_chain_candidate"]].sort_values(
        ["direction_group", "core_sequence", "roadseg_id"]
    )
    core_table.to_csv(
        dirs["metrics"] / "caitian_baidu_core_chain_segments.csv",
        index=False,
        encoding="utf-8-sig",
    )
    text_candidates.to_csv(
        dirs["metrics"] / "caitian_baidu_text_candidates.csv",
        index=False,
        encoding="utf-8-sig",
    )
    corridor.to_file(
        dirs["data"] / "caitian_baidu_corridor_segments.geojson",
        driver="GeoJSON",
    )
    plot_corridor(corridor, dirs["figures"] / "caitian_baidu_corridor_segments_map.png")

    direction_summary = (
        table.groupby(["direction_group", "candidate_class"], as_index=False)
        .agg(
            n_segments=("roadseg_id", "nunique"),
            total_length_m=("length_m", "sum"),
            n_with_speed=("has_speed_panel", "sum"),
        )
        .to_dict(orient="records")
    )
    core_direction_summary = (
        core_table.groupby("direction_group", as_index=False)
        .agg(
            n_segments=("roadseg_id", "nunique"),
            total_length_m=("length_m", "sum"),
            n_with_speed=("has_speed_panel", "sum"),
        )
        .to_dict(orient="records")
    )
    metadata = {
        "roads_source": str(args.roads),
        "speed_source": str(args.speed),
        "output_dir": str(args.output_dir),
        "selection_rule": {
            "main_corridor_roadnames": sorted(CORRIDOR_ROADNAMES),
            "connector_roadnames_retained_separately": sorted(CONNECTOR_ROADNAMES),
            "corridor_bounds_wgs84": {
                "min_lon": MIN_CORRIDOR_LON,
                "max_lon": MAX_CORRIDOR_LON,
                "min_lat": MIN_CORRIDOR_LAT,
                "max_lat": MAX_CORRIDOR_LAT,
            },
        },
        "user_point_diagnostics": user_point_diagnostics(corridor),
        "direction_summary": direction_summary,
        "core_direction_summary": core_direction_summary,
        "core_chain_segment_ids": {
            "policy_direction_futian_to_longhua": POLICY_CORE_CHAIN_IDS,
            "opposite_direction_longhua_to_futian": OPPOSITE_CORE_CHAIN_IDS,
        },
        "n_corridor_segments": int(table["roadseg_id"].nunique()),
        "n_core_chain_segments": int(core_table["roadseg_id"].nunique()),
        "n_text_candidates": int(text_candidates["roadseg_id"].nunique()),
        "figure": str(dirs["figures"] / "caitian_baidu_corridor_segments_map.png"),
    }
    with (dirs["metrics"] / "caitian_baidu_candidate_metadata.json").open(
        "w", encoding="utf-8"
    ) as fh:
        json.dump(metadata, fh, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
