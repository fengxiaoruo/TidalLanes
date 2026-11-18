from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT / "data_work"))

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from shapely.geometry import LineString

from src.baidu_validation_common.spatial import gcj02_to_wgs84


DEFAULT_GAODE_DIR = PROJECT_ROOT / "data_work/raw_data/Shenzhen_Gaode"
DEFAULT_BAIDU_ROADS = PROJECT_ROOT / "data_work/raw_data/gis/roads_baidu/shenzhen_roads.shp"
DEFAULT_BAIDU_SPEED = PROJECT_ROOT / "data_work/raw_data/speed_shenzhen_all_wgs84.csv"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data_work/outputs/shenzhen_liuxian_gaode_initial_v1"

SEARCH_BUFFER_M = 120.0
NEARBY_BUFFER_M = 30.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Match Liuxian Avenue Gaode segments to Baidu raw speed road segments."
    )
    parser.add_argument("--gaode-dir", type=Path, default=DEFAULT_GAODE_DIR)
    parser.add_argument("--baidu-roads", type=Path, default=DEFAULT_BAIDU_ROADS)
    parser.add_argument("--baidu-speed", type=Path, default=DEFAULT_BAIDU_SPEED)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def ensure_output_dirs(output_dir: Path) -> dict[str, Path]:
    dirs = {
        "figures": output_dir / "figures",
        "data": output_dir / "data",
        "metrics": output_dir / "metrics",
    }
    for path in dirs.values():
        path.mkdir(parents=True, exist_ok=True)
    return dirs


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
            "font.size": 10,
            "figure.dpi": 220,
            "savefig.dpi": 220,
        }
    )


def parse_coord(coord: str) -> tuple[float, float]:
    lon, lat = coord.split(",")
    return float(lon), float(lat)


def line_from_gaode_gcj(start: str, end: str) -> LineString:
    return LineString([parse_coord(start), parse_coord(end)])


def line_from_gaode_wgs(start: str, end: str) -> LineString:
    return LineString([gcj02_to_wgs84(*parse_coord(start)), gcj02_to_wgs84(*parse_coord(end))])


def bearing_degrees(line: LineString) -> float:
    start = line.coords[0]
    end = line.coords[-1]
    lon1, lat1 = start
    lon2, lat2 = end
    dy = (lat2 - lat1) * 111_320
    dx = (lon2 - lon1) * 111_320 * math.cos(math.radians((lat1 + lat2) / 2))
    return (math.degrees(math.atan2(dx, dy)) + 360) % 360


def chain_from_bearing(bearing: float) -> str:
    return "eastbound_chain" if 0 < bearing < 180 else "westbound_chain"


def load_gaode_segments(gaode_dir: Path) -> gpd.GeoDataFrame:
    frames = []
    for path in sorted(gaode_dir.glob("*.csv")):
        df = pd.read_csv(path, encoding="utf-8-sig", dtype=str)
        for col in df.columns:
            df[col] = df[col].astype(str).str.strip()
        df["datetime"] = pd.to_datetime(
            df["日期"].str.zfill(8) + df["时间"].str.zfill(4),
            format="%Y%m%d%H%M",
            errors="raise",
        )
        df["period_short"] = np.where(df["datetime"] < pd.Timestamp("2026-03-23"), "pre", "post")
        frames.append(df)
    panel = pd.concat(frames, ignore_index=True)
    seg = (
        panel.groupby(
            [
                "路段编号",
                "路段名称（唯一）",
                "路段方向",
                "路段长度",
                "路段起点坐标",
                "路段终点坐标",
            ],
            as_index=False,
        )
        .agg(
            pre_obs=("period_short", lambda x: int((x == "pre").sum())),
            post_obs=("period_short", lambda x: int((x == "post").sum())),
        )
        .copy()
    )
    seg["geometry_gcj02"] = seg.apply(
        lambda row: line_from_gaode_gcj(row["路段起点坐标"], row["路段终点坐标"]),
        axis=1,
    )
    seg["geometry"] = seg.apply(
        lambda row: line_from_gaode_wgs(row["路段起点坐标"], row["路段终点坐标"]),
        axis=1,
    )
    gdf = gpd.GeoDataFrame(seg, geometry="geometry", crs="EPSG:4326")
    gdf["gaode_bearing_wgs_deg"] = gdf.geometry.map(bearing_degrees)
    gdf["chain_direction"] = gdf["gaode_bearing_wgs_deg"].map(chain_from_bearing)
    gdf["prepost_coverage"] = np.select(
        [
            (gdf["pre_obs"] > 0) & (gdf["post_obs"] > 0),
            (gdf["pre_obs"] == 0) & (gdf["post_obs"] > 0),
            (gdf["pre_obs"] > 0) & (gdf["post_obs"] == 0),
        ],
        ["common", "post_only", "pre_only"],
        default="unknown",
    )
    return gdf


def match_baidu_roads(gaode: gpd.GeoDataFrame, baidu_roads_path: Path) -> gpd.GeoDataFrame:
    roads = gpd.read_file(baidu_roads_path)
    metric_crs = roads.estimate_utm_crs()
    roads_m = roads.to_crs(metric_crs)
    gaode_m = gaode.to_crs(metric_crs)
    target_union = gaode_m.geometry.union_all()

    roads_m["dist_to_gaode_wgs_m"] = roads_m.geometry.distance(target_union)
    candidates = roads_m[roads_m["dist_to_gaode_wgs_m"] <= SEARCH_BUFFER_M].copy()
    candidates = candidates.to_crs("EPSG:4326")
    candidates["dist_to_gaode_wgs_m"] = roads_m.loc[candidates.index, "dist_to_gaode_wgs_m"].values
    candidates["baidu_bearing_deg"] = candidates.geometry.map(bearing_degrees)
    candidates["chain_direction"] = candidates["baidu_bearing_deg"].map(chain_from_bearing)
    candidates["match_class"] = np.select(
        [
            candidates["roadname"].eq("留仙大道"),
            candidates["dist_to_gaode_wgs_m"] <= NEARBY_BUFFER_M,
        ],
        ["main_liuxian", "nearby_non_liuxian"],
        default="nearby_context",
    )
    candidates = candidates.sort_values(["match_class", "chain_direction", "dist_to_gaode_wgs_m"])
    return candidates


def extract_speed_panel(speed_csv: Path, roadseg_ids: set[str]) -> pd.DataFrame:
    usecols = ["cityname", "roadseg_id", "roadname", "semantic", "location", "geometry", "speed", "day", "hour"]
    pieces = []
    for chunk in pd.read_csv(speed_csv, usecols=usecols, chunksize=500_000):
        sub = chunk[chunk["roadseg_id"].isin(roadseg_ids)].copy()
        if not sub.empty:
            pieces.append(sub)
    if not pieces:
        return pd.DataFrame(columns=usecols)
    panel = pd.concat(pieces, ignore_index=True)
    panel["hour_str"] = panel["hour"].astype(str).str.zfill(12)
    panel["datetime"] = pd.to_datetime(panel["hour_str"], format="%Y%m%d%H%M", errors="coerce")
    panel["date"] = panel["datetime"].dt.date.astype(str)
    panel["dow"] = panel["datetime"].dt.dayofweek
    panel["day_type"] = np.where(panel["dow"] < 5, "Weekday", "Weekend")
    panel["minute_of_day"] = panel["datetime"].dt.hour * 60 + panel["datetime"].dt.minute
    panel["speed"] = pd.to_numeric(panel["speed"], errors="coerce")
    return panel


def window_from_minute(minute: int) -> str:
    if 15 * 60 <= minute < 17 * 60 + 30:
        return "weekday_1500_1730"
    if 17 * 60 + 30 <= minute < 20 * 60:
        return "weekday_1730_2000"
    return "other"


def summarize_speed(panel: pd.DataFrame, candidates: gpd.GeoDataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if panel.empty:
        return pd.DataFrame(), pd.DataFrame()
    attrs = candidates.drop(columns="geometry").copy()
    keep = [
        "roadseg_id",
        "roadname",
        "roadtype",
        "semantic",
        "location",
        "dist_to_gaode_wgs_m",
        "baidu_bearing_deg",
        "chain_direction",
        "match_class",
    ]
    panel = panel.merge(attrs[keep], on=["roadseg_id", "roadname", "semantic", "location"], how="left")
    panel["window"] = panel["minute_of_day"].map(window_from_minute)
    panel["is_weekday_window"] = panel["day_type"].eq("Weekday") & panel["window"].ne("other")

    by_segment = (
        panel.groupby(
            [
                "match_class",
                "chain_direction",
                "roadseg_id",
                "roadname",
                "roadtype",
                "semantic",
                "location",
                "dist_to_gaode_wgs_m",
                "baidu_bearing_deg",
            ],
            as_index=False,
        )
        .agg(
            n_obs=("speed", "size"),
            n_days=("date", "nunique"),
            mean_speed_all=("speed", "mean"),
            mean_speed_weekday_1500_1730=(
                "speed",
                lambda x: x[
                    panel.loc[x.index, "day_type"].eq("Weekday")
                    & panel.loc[x.index, "window"].eq("weekday_1500_1730")
                ].mean(),
            ),
            mean_speed_weekday_1730_2000=(
                "speed",
                lambda x: x[
                    panel.loc[x.index, "day_type"].eq("Weekday")
                    & panel.loc[x.index, "window"].eq("weekday_1730_2000")
                ].mean(),
            ),
        )
        .sort_values(["match_class", "chain_direction", "dist_to_gaode_wgs_m", "roadseg_id"])
    )

    window_summary = (
        panel[panel["is_weekday_window"] & panel["match_class"].eq("main_liuxian")]
        .groupby(["chain_direction", "window"], as_index=False)
        .agg(
            mean_speed=("speed", "mean"),
            median_speed=("speed", "median"),
            n_obs=("speed", "size"),
            n_segments=("roadseg_id", "nunique"),
            n_days=("date", "nunique"),
        )
        .sort_values(["chain_direction", "window"])
    )
    return by_segment, window_summary


def plot_match_map(
    gaode: gpd.GeoDataFrame,
    candidates: gpd.GeoDataFrame,
    output_path: Path,
) -> None:
    metric_crs = candidates.estimate_utm_crs()
    g_m = gaode.to_crs(metric_crs)
    c_m = candidates.to_crs(metric_crs)
    fig, ax = plt.subplots(figsize=(7.6, 5.0))
    c_m[c_m["match_class"].ne("main_liuxian")].plot(
        ax=ax,
        color="#bdbdbd",
        linewidth=1.0,
        alpha=0.8,
        label="Nearby non-Liuxian candidates",
    )
    for chain, color, label in [
        ("eastbound_chain", "#c44e52", "Baidu Liuxian eastbound"),
        ("westbound_chain", "#4c78a8", "Baidu Liuxian westbound"),
    ]:
        c_m[(c_m["match_class"].eq("main_liuxian")) & (c_m["chain_direction"].eq(chain))].plot(
            ax=ax,
            color=color,
            linewidth=2.4,
            label=label,
        )
        g_m[g_m["chain_direction"].eq(chain)].plot(
            ax=ax,
            color=color,
            linewidth=1.2,
            linestyle="--",
            alpha=0.9,
            label=f"Gaode target {chain.replace('_chain', '')}",
        )
    ax.set_title("Gaode Liuxian target and matched Baidu raw road segments", loc="left")
    ax.set_axis_off()
    ax.legend(loc="lower right", frameon=True, fontsize=7)
    fig.tight_layout()
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    args = parse_args()
    configure_plot()
    dirs = ensure_output_dirs(args.output_dir)

    gaode = load_gaode_segments(args.gaode_dir)
    candidates = match_baidu_roads(gaode, args.baidu_roads)
    candidate_ids = set(candidates["roadseg_id"].astype(str))
    speed_panel = extract_speed_panel(args.baidu_speed, candidate_ids)
    by_segment, window_summary = summarize_speed(speed_panel, candidates)

    gaode.drop(columns="geometry").to_csv(
        dirs["metrics"] / "baidu_match_gaode_target_segments_wgs84.csv",
        index=False,
        encoding="utf-8-sig",
    )
    candidates.drop(columns="geometry").to_csv(
        dirs["metrics"] / "baidu_liuxian_underlying_candidates.csv",
        index=False,
        encoding="utf-8-sig",
    )
    speed_panel.to_csv(
        dirs["data"] / "baidu_liuxian_underlying_speed_panel_20250519_20250525.csv",
        index=False,
        encoding="utf-8-sig",
    )
    by_segment.to_csv(
        dirs["metrics"] / "baidu_liuxian_underlying_speed_by_segment.csv",
        index=False,
        encoding="utf-8-sig",
    )
    window_summary.to_csv(
        dirs["metrics"] / "baidu_liuxian_underlying_speed_window_summary.csv",
        index=False,
        encoding="utf-8-sig",
    )
    plot_match_map(
        gaode,
        candidates,
        dirs["figures"] / "baidu_liuxian_underlying_match_map.png",
    )

    with (dirs["metrics"] / "baidu_liuxian_underlying_match_metadata.json").open(
        "w", encoding="utf-8"
    ) as fh:
        json.dump(
            {
                "gaode_coord_assumption": "Gaode raw coordinates are treated as GCJ-02 and converted to WGS84 before matching.",
                "baidu_roads": str(args.baidu_roads),
                "baidu_speed": str(args.baidu_speed),
                "baidu_speed_dates": sorted(speed_panel["day"].dropna().astype(int).unique().tolist())
                if not speed_panel.empty
                else [],
                "search_buffer_m": SEARCH_BUFFER_M,
                "nearby_buffer_m": NEARBY_BUFFER_M,
                "n_gaode_target_segments": int(len(gaode)),
                "n_baidu_candidates": int(len(candidates)),
                "n_main_liuxian_candidates": int(candidates["match_class"].eq("main_liuxian").sum()),
                "n_speed_panel_rows_extracted": int(len(speed_panel)),
            },
            fh,
            ensure_ascii=False,
            indent=2,
        )


if __name__ == "__main__":
    main()
