from __future__ import annotations

import argparse
import io
import json
import math
import re
import urllib.error
import urllib.request
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import scipy.sparse as sp
import scipy.sparse.linalg as spla
from matplotlib.lines import Line2D
from matplotlib.patches import Rectangle
from PIL import Image
from shapely.geometry import LineString, MultiLineString
from shapely.ops import unary_union


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_TIDAL_DIR = PROJECT_ROOT / "data_work/raw_data/gis/深圳潮汐车道shp"
DEFAULT_ROADS = PROJECT_ROOT / "data_work/raw_data/gis/roads_baidu/shenzhen_roads.shp"
DEFAULT_SPEED = PROJECT_ROOT / "data_work/raw_data/speed_shenzhen_all_wgs84.csv"
DEFAULT_ROUTE_XLSX = PROJECT_ROOT / "data_work/raw_data/深圳市潮汐车道2.xlsx"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data_work/outputs/shenzhen_multi_tidal_post_only_v1"
PROJECTED_CRS = "EPSG:32650"
WEB_MERCATOR_CRS = "EPSG:3857"

BUFFER_M = 70
SENSITIVITY_BUFFERS_M = [30, 60, 70, 100]
TIDAL_EXCLUSION_BUFFER_M = 100
CONTROL_SEARCH_M = 3500
CONTROL_AXIS_MAX_DIFF_DEG = 32
CONTROL_MIN_DISTANCE_M = 120
CONTROL_MIN_LENGTH_RATIO = 0.25
CONTROL_MAX_LENGTH_RATIO = 4.00
CORE_AXIS_MAX_DIFF_DEG = 35
CORE_MAX_DISTANCE_M = 35
CORE_MIN_INTERSECT_M = 50
MIN_CONTROL_LENGTH_M = 700
MAX_CONTROLS = 5
REFERENCE_MINUTES = 90
POST_MINUTES = 120
EVENT_PRE_MINUTES = 120
EVENT_POST_MINUTES = 120
REFERENCE_BIN = -30
OSM_ZOOM = 14
OSM_TILE_URL = "https://tile.openstreetmap.org/{z}/{x}/{y}.png"
OSM_CACHE_DIR = PROJECT_ROOT / "data_work/outputs/.osm_tile_cache"
EXCLUDED_CONTROL_ROADNAMES = {"无名路"}
EXCLUDED_CONTROL_NAME_KEYWORDS = ("无名", "高速", "快速", "隧道", "立交", "匝道")

POLICY = "policy_like"
OPPOSITE = "opposite_like"
DIRECTION_ORDER = [POLICY, OPPOSITE]
DIRECTION_LABELS = {
    POLICY: "Policy-like direction",
    OPPOSITE: "Opposite direction",
}

ROAD_CONFIGS = {
    "1": {
        "road_key": "wenjinbei",
        "road_name_cn": "文锦北路",
        "target_names": ["文锦北路"],
        "include_main": True,
        "notes": "User confirmed 文锦北路 PM is outbound / 出关. AM direction is coded as the reverse direction by assumption.",
        "windows": [
            {
                "window_key": "am_0700_0930",
                "label": "07:00-09:30",
                "start_min": 7 * 60,
                "end_min": 9 * 60 + 30,
                "policy_bearing_deg": 180.0,
                "policy_direction_note": "AM inbound, opposite of PM outbound, coded southbound-like",
            },
            {
                "window_key": "pm_1800_2000",
                "label": "18:00-20:00",
                "start_min": 18 * 60,
                "end_min": 20 * 60,
                "policy_bearing_deg": 0.0,
                "policy_direction_note": "PM outbound / 出关, coded northbound-like",
            },
        ],
    },
    "2": {
        "road_key": "caitian",
        "road_name_cn": "彩田路",
        "target_names": ["彩田路", "新彩隧道"],
        "include_main": True,
        "notes": "福田至龙华出关方向.",
        "windows": [
            {
                "window_key": "pm_1630_2130",
                "label": "16:30-21:30",
                "start_min": 16 * 60 + 30,
                "end_min": 21 * 60 + 30,
                "policy_bearing_deg": 0.0,
                "policy_direction_note": "Futian to Longhua, northbound-like",
            }
        ],
    },
    "3": {
        "road_key": "huishen_yanhai_expressway",
        "road_name_cn": "惠深沿海高速",
        "target_names": ["惠深沿海高速", "大梅沙隧道"],
        "include_main": False,
        "notes": "Highway / 盐坝高速-type sample, excluded from main urban-road analysis.",
        "windows": [],
    },
    "4": {
        "road_key": "xinzhou",
        "road_name_cn": "新洲路",
        "target_names": ["新洲路"],
        "include_main": True,
        "notes": "User confirmed 新洲路 is between 莲花路 and 梅华路, policy direction 北行, 直达梅丽路.",
        "windows": [
            {
                "window_key": "pm_1730_2000",
                "label": "17:30-20:00",
                "start_min": 17 * 60 + 30,
                "end_min": 20 * 60,
                "policy_bearing_deg": 0.0,
                "policy_direction_note": "northbound",
            }
        ],
    },
    "5": {
        "road_key": "hongling",
        "road_name_cn": "红岭路",
        "target_names": ["红岭路", "红岭北路"],
        "include_main": True,
        "notes": "User confirmed 红岭路 is between 泥岗路 and 笋岗路, policy direction 南行.",
        "windows": [
            {
                "window_key": "am_0730_0930",
                "label": "07:30-09:30",
                "start_min": 7 * 60 + 30,
                "end_min": 9 * 60 + 30,
                "policy_bearing_deg": 180.0,
                "policy_direction_note": "southbound",
            }
        ],
    },
    "6": {
        "road_key": "liuxian",
        "road_name_cn": "留仙大道",
        "target_names": ["留仙大道"],
        "include_main": True,
        "notes": "West-to-east / southwest-to-northeast policy direction.",
        "windows": [
            {
                "window_key": "pm_1730_2000",
                "label": "17:30-20:00",
                "start_min": 17 * 60 + 30,
                "end_min": 20 * 60,
                "policy_bearing_deg": 45.0,
                "policy_direction_note": "west-to-east / southwest-to-northeast",
            }
        ],
    },
}

TIDAL_ROADNAMES = {
    "文锦北路",
    "彩田路",
    "新彩隧道",
    "惠深沿海高速",
    "大梅沙隧道",
    "新洲路",
    "红岭路",
    "红岭北路",
    "留仙大道",
}


def is_excluded_control_name(value: object) -> bool:
    name = "" if pd.isna(value) else str(value).strip()
    if not name or name in EXCLUDED_CONTROL_ROADNAMES:
        return True
    return any(keyword in name for keyword in EXCLUDED_CONTROL_NAME_KEYWORDS)


def is_same_tidal_family_name(value: object, cfg: dict[str, object]) -> bool:
    name = "" if pd.isna(value) else str(value).strip()
    if not name:
        return False
    tokens = {str(cfg["road_name_cn"])[:2]}
    tokens.update(str(target)[:2] for target in cfg.get("target_names", []) if len(str(target)) >= 2)
    return any(token and token in name for token in tokens)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run post-only Baidu diagnostics for multiple Shenzhen tidal lanes."
    )
    parser.add_argument("--tidal-dir", type=Path, default=DEFAULT_TIDAL_DIR)
    parser.add_argument("--roads", type=Path, default=DEFAULT_ROADS)
    parser.add_argument("--speed", type=Path, default=DEFAULT_SPEED)
    parser.add_argument("--route-xlsx", type=Path, default=DEFAULT_ROUTE_XLSX)
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
            "xtick.labelsize": 8,
            "ytick.labelsize": 8,
            "figure.dpi": 180,
            "savefig.dpi": 300,
        }
    )


def ensure_dirs(output_dir: Path) -> dict[str, Path]:
    dirs = {
        "root": output_dir,
        "data": output_dir / "data",
        "metrics": output_dir / "metrics",
        "figures": output_dir / "figures",
        "maps": output_dir / "figures/maps",
        "maps_osm": output_dir / "figures/maps_osm_masked",
        "maps_plain": output_dir / "figures/maps_plain",
        "summary_figures": output_dir / "figures/summary",
    }
    for path in dirs.values():
        path.mkdir(parents=True, exist_ok=True)
    return dirs


def save_figure(fig: plt.Figure, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, bbox_inches="tight")
    fig.savefig(output_path.with_suffix(".pdf"), bbox_inches="tight")
    fig.savefig(output_path.with_suffix(".svg"), bbox_inches="tight")
    plt.close(fig)


def as_line(geom) -> LineString:
    if isinstance(geom, LineString):
        return geom
    if isinstance(geom, MultiLineString):
        return max(list(geom.geoms), key=lambda g: g.length)
    if hasattr(geom, "geoms"):
        lines = [g for g in geom.geoms if isinstance(g, LineString)]
        if lines:
            return max(lines, key=lambda g: g.length)
    raise ValueError("Geometry does not contain a line.")


def bearing_from_lonlat(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    mean_lat = math.radians((lat1 + lat2) / 2)
    dx = (lon2 - lon1) * math.cos(mean_lat)
    dy = lat2 - lat1
    return (math.degrees(math.atan2(dx, dy)) + 360) % 360


def bearing_from_geometry(geom) -> float:
    line = as_line(geom)
    coords = list(line.coords)
    lon1, lat1 = coords[0][:2]
    lon2, lat2 = coords[-1][:2]
    return bearing_from_lonlat(lon1, lat1, lon2, lat2)


def angle_diff(a: float, b: float) -> float:
    return abs((a - b + 180) % 360 - 180)


def axis_diff(a: float, b: float) -> float:
    return min(angle_diff(a, b), angle_diff((a + 180) % 360, b))


def minute_label(minute: int) -> str:
    return f"{minute // 60:02d}:{minute % 60:02d}"


def weekday_label(date_value: object) -> str:
    date = pd.to_datetime(date_value)
    names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    return f"{date.date()} ({names[date.weekday()]})"


def add_road_attributes(roads: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    roads = roads.to_crs("EPSG:4326").copy()
    roads["roadname"] = roads["roadname"].fillna("").astype(str)
    roads["semantic"] = roads["semantic"].fillna("").astype(str)
    roads["bearing_deg"] = roads.geometry.map(bearing_from_geometry)
    roads["axis_deg"] = roads["bearing_deg"] % 180
    projected = roads.to_crs(PROJECTED_CRS)
    roads["length_m"] = projected.geometry.length
    centroids = projected.geometry.centroid.to_crs("EPSG:4326")
    roads["centroid_lon"] = centroids.x
    roads["centroid_lat"] = centroids.y
    return roads


def treatment_axis_target(cfg: dict[str, object], fallback_bearing: float | None = None) -> float:
    windows = cfg.get("windows") or []
    if windows:
        return float(windows[0]["policy_bearing_deg"]) % 180
    if fallback_bearing is not None and np.isfinite(fallback_bearing):
        return float(fallback_bearing) % 180
    return 0.0


def add_treatment_match_flags(
    matches: gpd.GeoDataFrame,
    cfg: dict[str, object],
    axis_target: float,
) -> gpd.GeoDataFrame:
    matches = matches.copy()
    matches["target_name_match"] = matches["roadname"].isin(cfg["target_names"])
    matches["axis_diff_to_tidal_deg"] = matches["bearing_deg"].map(
        lambda x: axis_diff(float(x), axis_target)
    )
    matches["axis_aligned_to_tidal"] = matches["axis_diff_to_tidal_deg"].le(CORE_AXIS_MAX_DIFF_DEG)
    matches["core_treatment"] = (
        matches["target_name_match"]
        | (
            matches["axis_aligned_to_tidal"]
            & matches["distance_to_single_m"].le(CORE_MAX_DISTANCE_M)
            & matches["intersect_length_m"].ge(CORE_MIN_INTERSECT_M)
        )
    )
    matches["match_quality"] = np.where(matches["core_treatment"], "core", "auxiliary_intersection")
    return matches


def lonlat_to_tile(lon: float, lat: float, zoom: int) -> tuple[int, int]:
    lat = max(min(lat, 85.05112878), -85.05112878)
    lat_rad = math.radians(lat)
    n = 2**zoom
    x_tile = int((lon + 180.0) / 360.0 * n)
    y_tile = int((1.0 - math.log(math.tan(lat_rad) + 1.0 / math.cos(lat_rad)) / math.pi) / 2.0 * n)
    return max(0, min(n - 1, x_tile)), max(0, min(n - 1, y_tile))


def mercator_to_lonlat(x: float, y: float) -> tuple[float, float]:
    radius = 6378137.0
    lon = math.degrees(x / radius)
    lat = math.degrees(2 * math.atan(math.exp(y / radius)) - math.pi / 2)
    return lon, lat


def tile_bounds_mercator(x_tile: int, y_tile: int, zoom: int) -> tuple[float, float, float, float]:
    origin = 20037508.342789244
    n = 2**zoom
    tile = 2 * origin / n
    minx = -origin + x_tile * tile
    maxx = -origin + (x_tile + 1) * tile
    maxy = origin - y_tile * tile
    miny = origin - (y_tile + 1) * tile
    return minx, miny, maxx, maxy


def add_osm_basemap(ax: plt.Axes, bounds_3857: tuple[float, float, float, float], zoom: int = OSM_ZOOM) -> bool:
    xmin, ymin, xmax, ymax = bounds_3857
    lon_min, lat_min = mercator_to_lonlat(xmin, ymin)
    lon_max, lat_max = mercator_to_lonlat(xmax, ymax)
    x0, y1 = lonlat_to_tile(lon_min, lat_min, zoom)
    x1, y0 = lonlat_to_tile(lon_max, lat_max, zoom)
    x_min, x_max = sorted((x0, x1))
    y_min, y_max = sorted((y0, y1))
    if (x_max - x_min + 1) * (y_max - y_min + 1) > 64 and zoom > 11:
        return add_osm_basemap(ax, bounds_3857, zoom - 1)
    tile_size = 256
    mosaic = Image.new("RGB", ((x_max - x_min + 1) * tile_size, (y_max - y_min + 1) * tile_size))
    OSM_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        for x_tile in range(x_min, x_max + 1):
            for y_tile in range(y_min, y_max + 1):
                cache_path = OSM_CACHE_DIR / f"{zoom}_{x_tile}_{y_tile}.png"
                if cache_path.exists():
                    tile = Image.open(cache_path).convert("RGB")
                else:
                    url = OSM_TILE_URL.format(z=zoom, x=x_tile, y=y_tile)
                    request = urllib.request.Request(
                        url,
                        headers={"User-Agent": "TidalLanes-analysis/1.0 (local research map QA)"},
                    )
                    with urllib.request.urlopen(request, timeout=4) as response:
                        payload = response.read()
                    cache_path.write_bytes(payload)
                    tile = Image.open(io.BytesIO(payload)).convert("RGB")
                mosaic.paste(tile, ((x_tile - x_min) * tile_size, (y_tile - y_min) * tile_size))
    except Exception as exc:
        if isinstance(exc, KeyboardInterrupt):
            raise
        return False
    left, _bottom, _right, top = tile_bounds_mercator(x_min, y_min, zoom)
    _left2, bottom, right, _top2 = tile_bounds_mercator(x_max, y_max, zoom)
    ax.imshow(np.asarray(mosaic), extent=(left, right, bottom, top), origin="upper", alpha=0.92)
    return True


def read_speed_ids(speed_path: Path) -> set[str]:
    ids: set[str] = set()
    for chunk in pd.read_csv(speed_path, usecols=["roadseg_id"], chunksize=500_000):
        ids.update(chunk["roadseg_id"].astype(str).unique())
    return ids


def read_selected_speed(speed_path: Path, selected_ids: set[str]) -> pd.DataFrame:
    chunks = []
    for chunk in pd.read_csv(speed_path, chunksize=500_000):
        sub = chunk[chunk["roadseg_id"].astype(str).isin(selected_ids)].copy()
        if not sub.empty:
            chunks.append(sub)
    if not chunks:
        return pd.DataFrame()
    speed = pd.concat(chunks, ignore_index=True)
    speed["datetime"] = pd.to_datetime(speed["hour"].astype(str), format="%Y%m%d%H%M")
    speed["date"] = speed["datetime"].dt.date.astype(str)
    speed["dow"] = speed["datetime"].dt.dayofweek
    speed["day_type"] = np.where(speed["dow"] < 5, "weekday", "weekend")
    speed["minute_of_day"] = speed["datetime"].dt.hour * 60 + speed["datetime"].dt.minute
    speed["speed_kmh"] = pd.to_numeric(speed["speed"], errors="coerce")
    speed["date_id"] = speed["datetime"].dt.strftime("%Y%m%d")
    return speed


def discover_number_mapping(tidal_dir: Path) -> pd.DataFrame:
    rows = []
    for number, cfg in ROAD_CONFIGS.items():
        shp = tidal_dir / f"{number}_双线.shp"
        single = tidal_dir / f"{number}_单线.shp"
        names: list[str] = []
        fclasses: list[str] = []
        if shp.exists():
            gdf = gpd.read_file(shp)
            if "name" in gdf.columns:
                names = sorted(x for x in gdf["name"].dropna().astype(str).unique() if x)
            if "fclass" in gdf.columns:
                fclasses = sorted(x for x in gdf["fclass"].dropna().astype(str).unique() if x)
        bounds = None
        single_bearing = np.nan
        if single.exists():
            s = gpd.read_file(single).to_crs("EPSG:4326")
            bounds = ",".join(f"{x:.6f}" for x in s.total_bounds)
            single_bearing = bearing_from_geometry(s.geometry.iloc[0])
        axis_target = treatment_axis_target(cfg, single_bearing)
        rows.append(
            {
                "tidal_number": number,
                "inferred_road_name": cfg["road_name_cn"],
                "road_key": cfg["road_key"],
                "include_main_analysis": cfg["include_main"],
                "double_shp_names": ";".join(names),
                "double_shp_fclasses": ";".join(fclasses),
                "single_bearing_deg": single_bearing,
                "analysis_axis_deg": axis_target,
                "single_bounds_wgs84": bounds,
                "operation_windows": ";".join(w["label"] for w in cfg["windows"]),
                "direction_notes": "; ".join(w.get("policy_direction_note", "") for w in cfg["windows"]),
                "notes": cfg["notes"],
            }
        )
    return pd.DataFrame(rows)


def parse_length_km(value: object) -> float:
    if pd.isna(value):
        return np.nan
    text = str(value)
    match = re.search(r"([0-9]+(?:\.[0-9]+)?)", text)
    return float(match.group(1)) if match else np.nan


def normalize_time_token(hour: str, minute: str | None = None) -> str:
    h = int(hour)
    m = int(minute) if minute is not None and minute != "" else 0
    return f"{h:02d}:{m:02d}"


def normalize_time_windows(text: object) -> str:
    if pd.isna(text):
        return ""
    value = str(text)
    value = value.replace("---", "-").replace("—", "-").replace("－", "-").replace("–", "-")
    pattern = re.compile(
        r"(?P<h1>[0-2]?\d)(?:[:：](?P<m1>[0-5]\d))?\s*(?:时(?:(?P<m1_cn>[0-5]?\d)分?)?)?\s*(?:-|至|到)\s*"
        r"(?P<h2>[0-2]?\d)(?:[:：](?P<m2>[0-5]\d))?\s*(?:时(?:(?P<m2_cn>[0-5]?\d)分?)?)?"
    )
    windows = []
    for match in pattern.finditer(value):
        m1 = match.group("m1") or match.group("m1_cn")
        m2 = match.group("m2") or match.group("m2_cn")
        windows.append(
            f"{normalize_time_token(match.group('h1'), m1)}-{normalize_time_token(match.group('h2'), m2)}"
        )
    return ";".join(windows)


def official_windows_for_config(cfg: dict[str, object]) -> str:
    windows = []
    for window in cfg.get("windows", []):
        windows.append(f"{minute_label(int(window['start_min']))}-{minute_label(int(window['end_min']))}")
    return ";".join(windows)


def read_route_definitions(route_xlsx: Path) -> pd.DataFrame:
    if not route_xlsx.exists():
        return pd.DataFrame()
    raw = pd.read_excel(route_xlsx, sheet_name="Sheet2")
    raw.columns = [str(c).strip() for c in raw.columns]
    if "route_name" not in raw.columns:
        return pd.DataFrame()
    raw["route_name"] = raw["route_name"].ffill()
    rows = []
    for idx, row in raw.iterrows():
        route = row.get("route_name")
        if pd.isna(route):
            continue
        route = str(route).strip()
        if route == "" or route.lower() == "nan":
            continue
        cfg = next((c for c in ROAD_CONFIGS.values() if c["road_key"] == route), None)
        number = next((n for n, c in ROAD_CONFIGS.items() if c["road_key"] == route), None)
        tianyu_window = normalize_time_windows(row.get("开通时间"))
        official_window = official_windows_for_config(cfg) if cfg else ""
        start_lon = pd.to_numeric(row.get("起点经度"), errors="coerce")
        start_lat = pd.to_numeric(row.get("起点纬度"), errors="coerce")
        end_lon = pd.to_numeric(row.get("终点经度"), errors="coerce")
        end_lat = pd.to_numeric(row.get("终点纬度"), errors="coerce")
        endpoint_valid = bool(
            np.isfinite(start_lon)
            and np.isfinite(start_lat)
            and np.isfinite(end_lon)
            and np.isfinite(end_lat)
            and 113.6 <= start_lon <= 114.7
            and 22.3 <= start_lat <= 22.9
            and 113.6 <= end_lon <= 114.7
            and 22.3 <= end_lat <= 22.9
        )
        endpoint_length_km = np.nan
        if endpoint_valid:
            line = gpd.GeoSeries(
                [LineString([(start_lon, start_lat), (end_lon, end_lat)])],
                crs="EPSG:4326",
            ).to_crs(PROJECTED_CRS)
            endpoint_length_km = float(line.length.iloc[0] / 1000)
        tianyu_length_km = parse_length_km(row.get("长度"))
        endpoint_length_flag = ""
        if np.isfinite(endpoint_length_km) and np.isfinite(tianyu_length_km):
            if endpoint_length_km > max(tianyu_length_km * 3, tianyu_length_km + 5):
                endpoint_length_flag = "endpoint_length_inconsistent_with_tianyu_length"
        rows.append(
            {
                "excel_row": int(idx + 2),
                "tidal_number": number,
                "road_key": route,
                "road_name_cn": cfg["road_name_cn"] if cfg else row.get("route_name"),
                "tianyu_segment_text": row.get("路段"),
                "tianyu_open_time_raw": row.get("开通时间"),
                "tianyu_window_norm": tianyu_window,
                "official_or_project_window_norm": official_window,
                "window_conflict": bool(tianyu_window and official_window and tianyu_window not in official_window),
                "tianyu_length_raw": row.get("长度"),
                "tianyu_length_km": tianyu_length_km,
                "endpoint_start_address": row.get("起点地址"),
                "endpoint_start_lon": start_lon,
                "endpoint_start_lat": start_lat,
                "endpoint_end_address": row.get("终点地址"),
                "endpoint_end_lon": end_lon,
                "endpoint_end_lat": end_lat,
                "endpoint_valid": endpoint_valid,
                "endpoint_length_km": endpoint_length_km,
                "endpoint_length_flag": endpoint_length_flag,
                "tianyu_note": row.get("备注"),
            }
        )
    return pd.DataFrame(rows)


def build_route_source_conflicts(route_definitions: pd.DataFrame) -> pd.DataFrame:
    if route_definitions.empty:
        return pd.DataFrame()
    conflicts = route_definitions[
        route_definitions["window_conflict"]
        | route_definitions["endpoint_length_flag"].astype(str).ne("")
    ].copy()
    if conflicts.empty:
        return conflicts
    conflicts["conflict_note"] = ""
    conflicts.loc[conflicts["window_conflict"], "conflict_note"] = (
        "Tianyu time window differs from official/project window."
    )
    length_conflict = conflicts["endpoint_length_flag"].astype(str).ne("")
    conflicts.loc[length_conflict, "conflict_note"] = (
        conflicts.loc[length_conflict, "conflict_note"].astype(str)
        + " Endpoint coordinates imply a corridor length inconsistent with Tianyu length."
    ).str.strip()
    return conflicts


def build_all_tidal_exclusion(tidal_dir: Path) -> object:
    buffers = []
    for number in ROAD_CONFIGS:
        p = tidal_dir / f"{number}_单线.shp"
        if not p.exists():
            continue
        gdf = gpd.read_file(p).to_crs(PROJECTED_CRS)
        buffers.extend(list(gdf.buffer(TIDAL_EXCLUSION_BUFFER_M)))
    return unary_union(buffers)


def match_treatment_segments(
    roads: gpd.GeoDataFrame,
    tidal_dir: Path,
    speed_ids: set[str],
) -> gpd.GeoDataFrame:
    rows = []
    roads_proj = roads.to_crs(PROJECTED_CRS)
    for number, cfg in ROAD_CONFIGS.items():
        single_path = tidal_dir / f"{number}_单线.shp"
        if not single_path.exists():
            continue
        single = gpd.read_file(single_path).to_crs(PROJECTED_CRS)
        single_union = single.geometry.union_all()
        single_bearing = bearing_from_geometry(single.to_crs("EPSG:4326").geometry.iloc[0])
        axis_target = treatment_axis_target(cfg, single_bearing)
        buffer_geom = single_union.buffer(BUFFER_M)
        candidates = roads_proj[roads_proj.intersects(buffer_geom)].copy()
        if candidates.empty:
            continue
        candidates["intersect_length_m"] = candidates.geometry.intersection(buffer_geom).length
        candidates["distance_to_single_m"] = candidates.geometry.distance(single_union)
        name_match = candidates["roadname"].isin(cfg["target_names"])
        length_match = candidates["intersect_length_m"] >= 20
        match = candidates[name_match | length_match].copy()
        if match.empty:
            match = candidates.sort_values("distance_to_single_m").head(10).copy()
        match = add_treatment_match_flags(match, cfg, axis_target)
        match["analysis_length_m"] = np.minimum(match["intersect_length_m"], match["length_m"])
        match["analysis_length_source"] = "tidal_lane_buffer_overlap"
        match = match.to_crs("EPSG:4326")
        match["tidal_number"] = number
        match["road_key"] = cfg["road_key"]
        match["tidal_road_name"] = cfg["road_name_cn"]
        match["is_highway_excluded"] = not cfg["include_main"]
        match["has_speed_observation"] = match["roadseg_id"].isin(speed_ids)
        rows.append(match)
    if not rows:
        return gpd.GeoDataFrame(columns=list(roads.columns) + ["tidal_number"], geometry="geometry", crs="EPSG:4326")
    return gpd.GeoDataFrame(pd.concat(rows, ignore_index=True), geometry="geometry", crs="EPSG:4326")


def build_match_sensitivity(
    roads: gpd.GeoDataFrame,
    tidal_dir: Path,
    speed_ids: set[str],
) -> pd.DataFrame:
    rows = []
    roads_proj = roads.to_crs(PROJECTED_CRS)
    for number, cfg in ROAD_CONFIGS.items():
        single_path = tidal_dir / f"{number}_单线.shp"
        if not single_path.exists():
            continue
        single = gpd.read_file(single_path).to_crs(PROJECTED_CRS)
        single_union = single.geometry.union_all()
        single_bearing = bearing_from_geometry(single.to_crs("EPSG:4326").geometry.iloc[0])
        axis_target = treatment_axis_target(cfg, single_bearing)
        for buffer_m in SENSITIVITY_BUFFERS_M:
            buffer_geom = single_union.buffer(buffer_m)
            candidates = roads_proj[roads_proj.intersects(buffer_geom)].copy()
            if candidates.empty:
                rows.append(
                    {
                        "tidal_number": number,
                        "road_key": cfg["road_key"],
                        "tidal_road_name": cfg["road_name_cn"],
                        "buffer_m": buffer_m,
                        "n_intersect_segments": 0,
                        "n_core_segments": 0,
                        "n_core_with_speed": 0,
                        "core_full_roadseg_length_m": 0.0,
                        "core_overlap_analysis_length_m": 0.0,
                    }
                )
                continue
            candidates["intersect_length_m"] = candidates.geometry.intersection(buffer_geom).length
            candidates["distance_to_single_m"] = candidates.geometry.distance(single_union)
            flagged = add_treatment_match_flags(candidates, cfg, axis_target)
            core = flagged[flagged["core_treatment"]].copy()
            rows.append(
                {
                    "tidal_number": number,
                    "road_key": cfg["road_key"],
                    "tidal_road_name": cfg["road_name_cn"],
                    "buffer_m": buffer_m,
                    "n_intersect_segments": int(candidates["roadseg_id"].nunique()),
                    "n_core_segments": int(core["roadseg_id"].nunique()),
                    "n_core_with_speed": int(core["roadseg_id"].astype(str).isin(speed_ids).sum()),
                    "core_full_roadseg_length_m": float(core["length_m"].sum()) if not core.empty else 0.0,
                    "core_overlap_analysis_length_m": (
                        float(np.minimum(core["intersect_length_m"], core["length_m"]).sum())
                        if not core.empty
                        else 0.0
                    ),
                }
            )
    return pd.DataFrame(rows)


def select_control_candidates(
    roads: gpd.GeoDataFrame,
    treatment: gpd.GeoDataFrame,
    all_tidal_exclusion,
    speed_ids: set[str],
) -> tuple[pd.DataFrame, gpd.GeoDataFrame]:
    selected_rows = []
    catalog_layers = []
    roads_proj = roads.to_crs(PROJECTED_CRS)
    exclusion_hits = roads_proj.geometry.intersects(all_tidal_exclusion)
    excluded_name = roads_proj["roadname"].map(is_excluded_control_name)
    base_candidates = roads_proj[
        ~exclusion_hits
        & ~excluded_name
        & ~roads_proj["roadname"].isin(TIDAL_ROADNAMES)
        & roads_proj["roadseg_id"].astype(str).isin(speed_ids)
    ].copy()
    for number, cfg in ROAD_CONFIGS.items():
        if not cfg["include_main"]:
            continue
        treat = treatment[treatment["tidal_number"].eq(number)].copy()
        if "core_treatment" in treat.columns and treat["core_treatment"].any():
            treat = treat[treat["core_treatment"]].copy()
        if treat.empty:
            continue
        treat_proj = treat.to_crs(PROJECTED_CRS)
        treat_union = treat_proj.geometry.union_all()
        if "analysis_length_m" in treat_proj.columns:
            target_length_m = float(pd.to_numeric(treat_proj["analysis_length_m"], errors="coerce").sum())
        else:
            target_length_m = float(treat_proj.geometry.length.sum())
        if not np.isfinite(target_length_m) or target_length_m <= 0:
            target_length_m = float(treat_proj.geometry.length.sum())
        axis_target = cfg["windows"][0]["policy_bearing_deg"] % 180
        cand = base_candidates[base_candidates.geometry.distance(treat_union) <= CONTROL_SEARCH_M].copy()
        cand = cand[~cand["roadname"].map(lambda x: is_same_tidal_family_name(x, cfg))].copy()
        cand["axis_diff_deg"] = cand["bearing_deg"].map(lambda x: axis_diff(float(x), axis_target))
        cand = cand[cand["axis_diff_deg"] <= CONTROL_AXIS_MAX_DIFF_DEG].copy()
        if cand.empty:
            continue
        cand["distance_to_treatment_m"] = cand.geometry.distance(treat_union)
        grouped = (
            cand.groupby("roadname", as_index=False)
            .agg(
                n_segments=("roadseg_id", "nunique"),
                total_length_m=("length_m", "sum"),
                min_distance_to_treatment_m=("distance_to_treatment_m", "min"),
                mean_axis_diff_deg=("axis_diff_deg", "mean"),
                mean_roadtype=("roadtype", "mean"),
            )
        )
        grouped = grouped[
            grouped["total_length_m"].ge(MIN_CONTROL_LENGTH_M)
            & grouped["n_segments"].ge(2)
            & grouped["min_distance_to_treatment_m"].ge(CONTROL_MIN_DISTANCE_M)
        ].copy()
        if grouped.empty:
            continue
        grouped["target_treatment_analysis_length_m"] = target_length_m
        grouped["length_ratio_to_treatment"] = grouped["total_length_m"] / max(target_length_m, 1.0)
        grouped["length_log_distance"] = np.abs(
            np.log(grouped["length_ratio_to_treatment"].clip(lower=1e-6))
        )
        length_screened = grouped[
            grouped["length_ratio_to_treatment"].between(
                CONTROL_MIN_LENGTH_RATIO,
                CONTROL_MAX_LENGTH_RATIO,
                inclusive="both",
            )
        ].copy()
        if not length_screened.empty:
            grouped = length_screened
        grouped["score"] = (
            grouped["min_distance_to_treatment_m"]
            + grouped["mean_axis_diff_deg"] * 45
            + grouped["length_log_distance"] * 550
            - np.minimum(grouped["total_length_m"], target_length_m * 1.5) * 0.06
        )
        grouped = grouped.sort_values(["score", "min_distance_to_treatment_m"]).head(MAX_CONTROLS)
        grouped["tidal_number"] = number
        grouped["road_key"] = cfg["road_key"]
        grouped["tidal_road_name"] = cfg["road_name_cn"]
        grouped["control_rank"] = np.arange(1, len(grouped) + 1)
        grouped["control_id"] = grouped["roadname"].map(lambda x: f"control_{x}")
        selected_rows.append(grouped)
        keep_names = set(grouped["roadname"])
        layer = cand[cand["roadname"].isin(keep_names)].copy()
        layer = layer.merge(
            grouped[["roadname", "control_rank", "control_id"]],
            on="roadname",
            how="left",
        )
        layer["tidal_number"] = number
        layer["road_key"] = cfg["road_key"]
        layer["tidal_road_name"] = cfg["road_name_cn"]
        catalog_layers.append(layer.to_crs("EPSG:4326"))
    control_table = pd.concat(selected_rows, ignore_index=True) if selected_rows else pd.DataFrame()
    control_segments = (
        gpd.GeoDataFrame(pd.concat(catalog_layers, ignore_index=True), geometry="geometry", crs="EPSG:4326")
        if catalog_layers
        else gpd.GeoDataFrame(columns=list(roads.columns), geometry="geometry", crs="EPSG:4326")
    )
    return control_table, control_segments


def build_analysis_catalog(
    treatment: gpd.GeoDataFrame,
    control_segments: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    parts = []
    treat = treatment[treatment["is_highway_excluded"].eq(False)].copy()
    if "core_treatment" in treat.columns and treat["core_treatment"].any():
        treat = treat[treat["core_treatment"]].copy()
    if not treat.empty:
        if "analysis_length_m" not in treat.columns:
            treat["analysis_length_m"] = treat["length_m"]
        if "analysis_length_source" not in treat.columns:
            treat["analysis_length_source"] = "full_treatment_roadseg_length_fallback"
        treat["corridor_id"] = treat["road_key"].map(lambda x: f"treat_{x}")
        treat["corridor_label"] = treat["tidal_road_name"]
        treat["role"] = "treatment"
        treat["control_rank"] = 0
        parts.append(treat)
    if not control_segments.empty:
        controls = control_segments.copy()
        controls["corridor_id"] = controls.apply(
            lambda r: f"{r['road_key']}__{r['control_id']}", axis=1
        )
        controls["corridor_label"] = controls["roadname"]
        controls["role"] = "control"
        controls["is_highway_excluded"] = False
        controls["core_treatment"] = False
        controls["match_quality"] = "control_candidate"
        controls["analysis_length_m"] = controls["length_m"]
        controls["analysis_length_source"] = "full_control_roadseg_length"
        parts.append(controls)
    catalog = gpd.GeoDataFrame(pd.concat(parts, ignore_index=True), geometry="geometry", crs="EPSG:4326")
    catalog = catalog.drop_duplicates(["tidal_number", "roadseg_id", "corridor_id"]).copy()
    return catalog


def attach_speed_attributes(speed: pd.DataFrame, catalog: gpd.GeoDataFrame) -> pd.DataFrame:
    attrs = catalog.drop(columns="geometry")[
        [
            "tidal_number",
            "road_key",
            "tidal_road_name",
            "roadseg_id",
            "roadname",
            "semantic",
            "bearing_deg",
            "axis_deg",
            "length_m",
            "analysis_length_m",
            "analysis_length_source",
            "corridor_id",
            "corridor_label",
            "role",
            "control_rank",
            "core_treatment",
            "match_quality",
        ]
    ].drop_duplicates(["tidal_number", "roadseg_id", "corridor_id"])
    panel = speed.merge(attrs, on="roadseg_id", how="inner", suffixes=("", "_attr"))
    panel["analysis_length_m"] = pd.to_numeric(panel["analysis_length_m"], errors="coerce")
    panel["segment_travel_time_min"] = panel["analysis_length_m"] / 1000 / panel["speed_kmh"] * 60
    return panel


def build_segment_speed_coverage(panel: pd.DataFrame) -> pd.DataFrame:
    if panel.empty:
        return pd.DataFrame()
    attrs = [
        "tidal_number",
        "road_key",
        "tidal_road_name",
        "corridor_id",
        "corridor_label",
        "role",
        "control_rank",
        "roadseg_id",
        "roadname",
        "bearing_deg",
        "length_m",
        "analysis_length_m",
        "analysis_length_source",
        "core_treatment",
        "match_quality",
    ]
    coverage = (
        panel.groupby(attrs, dropna=False, as_index=False)
        .agg(
            n_obs=("speed_kmh", "count"),
            n_dates=("date", "nunique"),
            n_10min_slots=("minute_of_day", "nunique"),
            min_datetime=("datetime", "min"),
            max_datetime=("datetime", "max"),
            mean_speed_kmh=("speed_kmh", "mean"),
        )
    )
    coverage["expected_obs_if_full"] = coverage["n_dates"] * 144
    coverage["obs_share_within_observed_dates"] = coverage["n_obs"] / coverage["expected_obs_if_full"]
    return coverage


def build_corridor_length_summary(catalog: gpd.GeoDataFrame) -> pd.DataFrame:
    if catalog.empty:
        return pd.DataFrame()
    summary = (
        catalog.drop(columns="geometry")
        .groupby(
            [
                "tidal_number",
                "road_key",
                "tidal_road_name",
                "role",
                "corridor_id",
                "corridor_label",
                "control_rank",
            ],
            dropna=False,
            as_index=False,
        )
        .agg(
            n_segments=("roadseg_id", "nunique"),
            full_roadseg_length_m=("length_m", "sum"),
            analysis_length_m=("analysis_length_m", "sum"),
            min_bearing_deg=("bearing_deg", "min"),
            max_bearing_deg=("bearing_deg", "max"),
        )
    )
    summary["full_roadseg_length_km"] = summary["full_roadseg_length_m"] / 1000
    summary["analysis_length_km"] = summary["analysis_length_m"] / 1000
    return summary.sort_values(
        ["tidal_number", "role", "control_rank", "corridor_label"]
    ).reset_index(drop=True)


def build_direction_length_summary(catalog: gpd.GeoDataFrame) -> pd.DataFrame:
    if catalog.empty:
        return pd.DataFrame()
    rows = []
    base = catalog.drop(columns="geometry").copy()
    for number, cfg in ROAD_CONFIGS.items():
        if not cfg["include_main"]:
            continue
        sub = base[base["tidal_number"].astype(str).eq(str(number))].copy()
        if sub.empty:
            continue
        for window in cfg["windows"]:
            policy_bearing = float(window["policy_bearing_deg"])
            tmp = sub.copy()
            tmp["window_key"] = window["window_key"]
            tmp["operation_window_label"] = window["label"]
            tmp["direction_group"] = tmp["bearing_deg"].map(lambda x: classify_direction(x, policy_bearing))
            summary = (
                tmp.groupby(
                    [
                        "tidal_number",
                        "road_key",
                        "tidal_road_name",
                        "window_key",
                        "operation_window_label",
                        "role",
                        "corridor_id",
                        "corridor_label",
                        "control_rank",
                        "direction_group",
                    ],
                    dropna=False,
                    as_index=False,
                )
                .agg(
                    n_segments=("roadseg_id", "nunique"),
                    full_roadseg_length_m=("length_m", "sum"),
                    analysis_length_m=("analysis_length_m", "sum"),
                )
            )
            rows.append(summary)
    if not rows:
        return pd.DataFrame()
    out = pd.concat(rows, ignore_index=True)
    out["full_roadseg_length_km"] = out["full_roadseg_length_m"] / 1000
    out["analysis_length_km"] = out["analysis_length_m"] / 1000
    return out.sort_values(
        ["tidal_number", "window_key", "role", "control_rank", "corridor_label", "direction_group"]
    ).reset_index(drop=True)


def build_treatment_only_window_trends(
    window_daily: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if window_daily.empty:
        return pd.DataFrame(), pd.DataFrame()
    treatment = window_daily[window_daily["role"].eq("treatment")].copy()
    treatment["lane_change_role"] = np.where(
        treatment["direction_group"].eq(POLICY),
        "lane_added_policy_direction",
        "lane_reduced_opposite_direction",
    )
    index_cols = [
        "tidal_number",
        "road_key",
        "tidal_road_name",
        "window_key",
        "operation_window_label",
        "direction_group",
        "lane_change_role",
    ]
    daily_index_cols = index_cols + ["date", "dow"]
    daily = (
        treatment.pivot_table(
            index=daily_index_cols,
            columns="window",
            values="speed_kmh",
            aggfunc="first",
        )
        .reset_index()
        .rename_axis(None, axis=1)
    )
    for col in ["reference", "active", "post"]:
        if col not in daily.columns:
            daily[col] = np.nan
    daily["active_minus_reference_kmh"] = daily["active"] - daily["reference"]
    daily["active_over_reference"] = daily["active"] / daily["reference"]
    daily["post_minus_active_kmh"] = daily["post"] - daily["active"]

    summary = (
        daily.groupby(index_cols, as_index=False)
        .agg(
            reference_speed_kmh=("reference", "mean"),
            active_speed_kmh=("active", "mean"),
            post_speed_kmh=("post", "mean"),
            active_minus_reference_kmh=("active_minus_reference_kmh", "mean"),
            active_over_reference=("active_over_reference", "mean"),
            post_minus_active_kmh=("post_minus_active_kmh", "mean"),
            n_weekdays=("date", "nunique"),
        )
    )
    return summary, daily


def classify_direction(bearing: float, policy_bearing: float) -> str:
    return POLICY if angle_diff(float(bearing), policy_bearing) <= 90 else OPPOSITE


def window_bounds(window: dict[str, object]) -> tuple[int, int, int, int, int, int]:
    start = int(window["start_min"])
    end = int(window["end_min"])
    ref_start = max(0, start - REFERENCE_MINUTES)
    post_end = min(24 * 60, end + POST_MINUTES)
    plot_start = max(0, start - EVENT_PRE_MINUTES)
    plot_end = min(24 * 60, end + EVENT_POST_MINUTES)
    return ref_start, start, end, post_end, plot_start, plot_end


def build_corridor_speed(panel: pd.DataFrame, cfg: dict[str, object], window: dict[str, object]) -> pd.DataFrame:
    policy_bearing = float(window["policy_bearing_deg"])
    sub = panel[panel["tidal_number"].astype(str).eq(str(cfg["tidal_number"]))].copy()
    sub["direction_group"] = sub["bearing_deg"].map(lambda x: classify_direction(x, policy_bearing))
    grouped = (
        sub.groupby(
            [
                "tidal_number",
                "road_key",
                "tidal_road_name",
                "corridor_id",
                "corridor_label",
                "role",
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
            full_roadseg_length_m=("length_m", "sum"),
            total_length_m=("analysis_length_m", "sum"),
            n_segments=("roadseg_id", "nunique"),
        )
    )
    grouped["corridor_speed_kmh"] = (
        grouped["total_length_m"] / 1000 / (grouped["travel_time_min"] / 60)
    )
    return grouped


def build_window_summary(corridor: pd.DataFrame, window: dict[str, object]) -> pd.DataFrame:
    ref_start, start, end, post_end, _plot_start, _plot_end = window_bounds(window)
    windows = {
        "reference": (ref_start, start, f"{minute_label(ref_start)}-{minute_label(start)}"),
        "active": (start, end, f"{minute_label(start)}-{minute_label(end)}"),
        "post": (end, post_end, f"{minute_label(end)}-{minute_label(post_end)}"),
    }
    rows = []
    weekday = corridor[corridor["day_type"].eq("weekday")].copy()
    for window_name, (lo, hi, label) in windows.items():
        sub = weekday[(weekday["minute_of_day"] >= lo) & (weekday["minute_of_day"] < hi)].copy()
        summary = (
            sub.groupby(
                [
                    "tidal_number",
                    "road_key",
                    "tidal_road_name",
                    "corridor_id",
                    "corridor_label",
                    "role",
                    "direction_group",
                    "date",
                    "dow",
                ],
                as_index=False,
            )
            .agg(
                speed_kmh=("corridor_speed_kmh", "mean"),
                travel_time_min=("travel_time_min", "mean"),
                n_10min=("minute_of_day", "nunique"),
            )
        )
        summary["window"] = window_name
        summary["window_label"] = label
        rows.append(summary)
    out = pd.concat(rows, ignore_index=True)
    out["window_key"] = window["window_key"]
    out["operation_window_label"] = window["label"]
    return out


def build_window_metrics(window_daily: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    index_cols = [
        "tidal_number",
        "road_key",
        "tidal_road_name",
        "corridor_id",
        "corridor_label",
        "role",
        "direction_group",
        "date",
        "dow",
        "window_key",
        "operation_window_label",
    ]
    wide = window_daily.pivot_table(
        index=index_cols,
        columns="window",
        values="speed_kmh",
        aggfunc="first",
    ).reset_index()
    wide["active_over_reference_speed"] = wide["active"] / wide["reference"]
    wide["post_over_reference_speed"] = wide["post"] / wide["reference"]

    asym = wide.pivot_table(
        index=[
            "tidal_number",
            "road_key",
            "tidal_road_name",
            "corridor_id",
            "corridor_label",
            "role",
            "date",
            "dow",
            "window_key",
            "operation_window_label",
        ],
        columns="direction_group",
        values=["reference", "active", "post"],
        aggfunc="first",
    ).reset_index()
    asym.columns = ["_".join([str(x) for x in col if x]) for col in asym.columns]
    for window in ["reference", "active", "post"]:
        p = f"{window}_{POLICY}"
        o = f"{window}_{OPPOSITE}"
        if p in asym.columns and o in asym.columns:
            asym[f"policy_over_opposite_{window}"] = asym[p] / asym[o]
    if {"policy_over_opposite_active", "policy_over_opposite_reference"}.issubset(asym.columns):
        asym["dlog_asym_active_vs_reference"] = (
            np.log(asym["policy_over_opposite_active"])
            - np.log(asym["policy_over_opposite_reference"])
        )
    return wide, asym


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


def residualize_matrix_sparse(
    matrix: np.ndarray,
    groups: list[pd.Series],
    atol: float = 1e-10,
    btol: float = 1e-10,
) -> tuple[np.ndarray, int]:
    n_obs = matrix.shape[0]
    rows = []
    cols = []
    offset = 0
    for group in groups:
        codes, uniques = pd.factorize(group.astype(str), sort=False)
        rows.append(np.arange(n_obs))
        cols.append(codes + offset)
        offset += len(uniques)
    if offset == 0:
        return matrix.astype(float).copy(), 0
    row_idx = np.concatenate(rows)
    col_idx = np.concatenate(cols)
    data = np.ones(len(row_idx), dtype=float)
    fixed_effects = sp.csr_matrix((data, (row_idx, col_idx)), shape=(n_obs, offset))
    residual = np.empty_like(matrix, dtype=float)
    max_iterations = 0
    for col in range(matrix.shape[1]):
        result = spla.lsmr(fixed_effects, matrix[:, col].astype(float), atol=atol, btol=btol, maxiter=2000)
        gamma = result[0]
        max_iterations = max(max_iterations, int(result[2]))
        residual[:, col] = matrix[:, col] - fixed_effects @ gamma
    return residual, max_iterations


def cluster_covariance(x: np.ndarray, residual: np.ndarray, clusters: np.ndarray) -> np.ndarray:
    if x.size == 0:
        return np.empty((0, 0))
    xtx = np.einsum("ni,nj->ij", x, x)
    eigenvalues, eigenvectors = np.linalg.eigh(xtx)
    threshold = 1e-10 * max(float(np.max(np.abs(eigenvalues))), 1.0)
    inv_eigenvalues = np.zeros_like(eigenvalues)
    positive = eigenvalues > threshold
    inv_eigenvalues[positive] = 1.0 / eigenvalues[positive]
    xtx_inv = np.einsum("ik,k,jk->ij", eigenvectors, inv_eigenvalues, eigenvectors, optimize=False)
    meat = np.zeros((x.shape[1], x.shape[1]))
    for cluster in pd.unique(clusters):
        loc = clusters == cluster
        score = np.einsum("ni,n->i", x[loc], residual[loc])
        meat += np.outer(score, score)
    n_obs, n_params = x.shape
    n_clusters = len(pd.unique(clusters))
    correction = 1.0
    if n_clusters > 1 and n_obs > n_params:
        correction = (n_clusters / (n_clusters - 1)) * ((n_obs - 1) / (n_obs - n_params))
    return correction * np.einsum("ij,jk,kl->il", xtx_inv, meat, xtx_inv)


def fit_event_study(
    sample: pd.DataFrame,
    outcome_col: str,
    treatment_col: str,
    unit_col: str,
    model_name: str,
) -> tuple[pd.DataFrame, dict[str, object]]:
    sample = sample.dropna(subset=[outcome_col, "event_bin_min", treatment_col]).copy()
    finite_input = (
        np.isfinite(pd.to_numeric(sample[outcome_col], errors="coerce"))
        & np.isfinite(pd.to_numeric(sample["event_bin_min"], errors="coerce"))
        & np.isfinite(pd.to_numeric(sample[treatment_col], errors="coerce"))
    )
    sample = sample[finite_input].copy()
    event_bins = sorted(int(x) for x in sample["event_bin_min"].unique())
    estimated_bins = [x for x in event_bins if x != REFERENCE_BIN]
    if not estimated_bins:
        return pd.DataFrame(), {"model": model_name, "n_obs": int(len(sample)), "error": "no estimated bins"}
    x_parts = [
        (sample[treatment_col].eq(1) & sample["event_bin_min"].eq(event_bin)).astype(float)
        for event_bin in estimated_bins
    ]
    y = sample[outcome_col].to_numpy(dtype=float)
    x = pd.concat(x_parts, axis=1).to_numpy(dtype=float)
    residualized, n_iter = residualize_matrix_sparse(
        np.column_stack([y, x]),
        groups=[
            sample[unit_col].astype(str),
            sample["date"].astype(str),
            sample["minute_of_day"].astype(str),
        ],
    )
    y_tilde = residualized[:, 0]
    x_tilde = residualized[:, 1:]
    finite = np.isfinite(y_tilde) & np.all(np.isfinite(x_tilde), axis=1)
    y_tilde = y_tilde[finite]
    x_tilde = x_tilde[finite]
    cluster_ids = sample[unit_col].astype(str).to_numpy()[finite]
    keep = np.sqrt(np.sum(x_tilde**2, axis=0)) > 1e-10
    beta_full = np.full(len(estimated_bins), np.nan)
    se_full = np.full(len(estimated_bins), np.nan)
    if keep.any():
        x_kept = x_tilde[:, keep]
        beta, *_ = np.linalg.lstsq(x_kept, y_tilde, rcond=None)
        fitted = np.einsum("ij,j->i", x_kept, beta, optimize=False)
        residual = y_tilde - fitted
        vcov = cluster_covariance(x_kept, residual, cluster_ids)
        beta_full[keep] = beta
        se_full[keep] = np.sqrt(np.maximum(np.diag(vcov), 0))
    rows = []
    lookup = dict(zip(estimated_bins, beta_full))
    se_lookup = dict(zip(estimated_bins, se_full))
    for event_bin in event_bins:
        if event_bin == REFERENCE_BIN:
            coef = 0.0
            se = np.nan
        else:
            coef = float(lookup[event_bin])
            se = float(se_lookup[event_bin])
        rows.append(
            {
                "model": model_name,
                "event_bin_min": event_bin,
                "coef": coef,
                "std_error": se,
                "ci95_low": coef - 1.96 * se if np.isfinite(se) else np.nan,
                "ci95_high": coef + 1.96 * se if np.isfinite(se) else np.nan,
                "n_obs": int(len(sample)),
            }
        )
    diagnostics = {
        "model": model_name,
        "n_obs": int(len(sample)),
        "n_obs_finite_after_residualization": int(len(y_tilde)),
        "n_units": int(sample[unit_col].nunique()),
        "n_dates": int(sample["date"].nunique()),
        "n_minutes": int(sample["minute_of_day"].nunique()),
        "residualization_method": "scipy_sparse_lsmr_fixed_effect_projection",
        "residualization_max_lsmr_iterations": int(n_iter),
        "reference_bin_min": REFERENCE_BIN,
    }
    return pd.DataFrame(rows), diagnostics


def build_event_study(
    panel: pd.DataFrame,
    cfg: dict[str, object],
    window: dict[str, object],
    control_segments: gpd.GeoDataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, object]]:
    _ref_start, start, end, _post_end, plot_start, plot_end = window_bounds(window)
    policy_bearing = float(window["policy_bearing_deg"])
    road_key = cfg["road_key"]
    road_panel = panel[
        panel["road_key"].eq(road_key)
        & (panel["minute_of_day"] >= plot_start)
        & (panel["minute_of_day"] < plot_end)
        & panel["day_type"].eq("weekday")
    ].copy()
    road_panel["direction_group"] = road_panel["bearing_deg"].map(
        lambda x: classify_direction(x, policy_bearing)
    )
    road_panel["event_time_min"] = road_panel["minute_of_day"] - start
    road_panel["event_bin_min"] = np.floor(road_panel["event_time_min"] / 30).astype(int) * 30
    road_panel["treated"] = road_panel["role"].eq("treatment").astype(int)
    road_panel["unit_id"] = road_panel["roadseg_id"].astype(str)
    coef_rows = []
    sample_rows = []
    diagnostics = {}
    control_ids = sorted(
        control_segments[control_segments["road_key"].eq(road_key)]["control_id"].dropna().unique()
    )
    control_sets = {"pooled_controls": control_ids}
    control_sets.update({cid: [cid] for cid in control_ids})
    for control_set, keep_controls in control_sets.items():
        if not keep_controls:
            continue
        keep_corridors = {f"treat_{road_key}"}
        keep_corridors.update(f"{road_key}__{cid}" for cid in keep_controls)
        for direction in DIRECTION_ORDER:
            sample = road_panel[
                road_panel["corridor_id"].isin(keep_corridors)
                & road_panel["direction_group"].eq(direction)
            ].copy()
            if sample["treated"].nunique() < 2 or sample.empty:
                continue
            sample["control_set"] = control_set
            sample["window_key"] = window["window_key"]
            sample_rows.append(sample)
            model_name = f"{cfg['road_name_cn']} {window['label']} {direction} {control_set}"
            coef, diag = fit_event_study(sample, "speed_kmh", "treated", "unit_id", model_name)
            if coef.empty:
                continue
            coef["tidal_number"] = cfg["tidal_number"]
            coef["road_key"] = road_key
            coef["tidal_road_name"] = cfg["road_name_cn"]
            coef["window_key"] = window["window_key"]
            coef["operation_window_label"] = window["label"]
            coef["direction_group"] = direction
            coef["direction_label"] = DIRECTION_LABELS[direction]
            coef["control_set"] = control_set
            coef_rows.append(coef)
            diagnostics[f"{road_key}::{window['window_key']}::{direction}::{control_set}"] = diag
    coef_out = pd.concat(coef_rows, ignore_index=True) if coef_rows else pd.DataFrame()
    sample_out = pd.concat(sample_rows, ignore_index=True) if sample_rows else pd.DataFrame()
    return coef_out, sample_out, diagnostics


def plot_map(
    roads: gpd.GeoDataFrame,
    treatment: gpd.GeoDataFrame,
    controls: gpd.GeoDataFrame,
    cfg: dict[str, object],
    output_path: Path,
    use_osm: bool,
    add_mask: bool,
) -> None:
    treat = treatment[treatment["tidal_number"].eq(cfg["tidal_number"])].copy()
    treat_core = treat[treat.get("core_treatment", pd.Series(False, index=treat.index)).eq(True)].copy()
    treat_aux = treat[treat.get("core_treatment", pd.Series(False, index=treat.index)).eq(False)].copy()
    ctrl = controls[controls["road_key"].eq(cfg["road_key"])].copy()
    if treat.empty:
        return
    layers = [(treat_core if not treat_core.empty else treat).to_crs(WEB_MERCATOR_CRS)]
    if not ctrl.empty:
        layers.append(ctrl.to_crs(WEB_MERCATOR_CRS))
    bounds = pd.concat([layer.geometry.bounds for layer in layers])
    minx, miny = bounds[["minx", "miny"]].min()
    maxx, maxy = bounds[["maxx", "maxy"]].max()
    pad = 900
    roads_proj = roads.to_crs(WEB_MERCATOR_CRS)
    fig, ax = plt.subplots(figsize=(7.2, 7.6))
    bounds_3857 = (minx - pad, miny - pad, maxx + pad, maxy + pad)
    has_osm = add_osm_basemap(ax, bounds_3857) if use_osm else False
    if has_osm and add_mask:
        ax.add_patch(
            Rectangle(
                (minx - pad, miny - pad),
                (maxx - minx) + 2 * pad,
                (maxy - miny) + 2 * pad,
                facecolor="white",
                edgecolor="none",
                alpha=0.42,
                zorder=1,
            )
        )
    if not has_osm:
        roads_proj.cx[minx - pad : maxx + pad, miny - pad : maxy + pad].plot(
            ax=ax, color="#d7d7d7", linewidth=0.35, alpha=0.70, zorder=1
        )
    else:
        roads_proj.cx[minx - pad : maxx + pad, miny - pad : maxy + pad].plot(
            ax=ax, color="#ffffff", linewidth=0.24, alpha=0.42, zorder=2
        )
    if not ctrl.empty:
        palette = ["#d95f02", "#1b9e77", "#7570b3", "#e7298a", "#66a61e"]
        for idx, (name, part) in enumerate(ctrl.to_crs(WEB_MERCATOR_CRS).groupby("roadname")):
            part.plot(
                ax=ax,
                color=palette[idx % len(palette)],
                linewidth=1.8,
                alpha=0.88,
                label=name,
                zorder=4,
            )
    if not treat_aux.empty:
        treat_aux.to_crs(WEB_MERCATOR_CRS).plot(
            ax=ax,
            color="#9ecae1",
            linewidth=1.1,
            alpha=0.62,
            label=f"{cfg['road_name_cn']} auxiliary",
            zorder=3,
        )
    (treat_core if not treat_core.empty else treat).to_crs(WEB_MERCATOR_CRS).plot(
        ax=ax,
        color="#08519c",
        linewidth=3.1,
        label=f"{cfg['road_name_cn']} core",
        zorder=5,
    )
    ax.set_xlim(minx - pad, maxx + pad)
    ax.set_ylim(miny - pad, maxy + pad)
    source = "OSM basemap with mask" if has_osm and add_mask else "Baidu road-network only"
    ax.set_title(f"{cfg['road_name_cn']}: matched core treatment and candidate controls ({source})", loc="left")
    ax.grid(color="#e8e8e8", linewidth=0.35, alpha=0.45)
    ax.set_axis_off()
    ax.legend(frameon=False, fontsize=7, loc="best")
    fig.tight_layout()
    save_figure(fig, output_path)


def plot_timeseries(
    corridor: pd.DataFrame,
    cfg: dict[str, object],
    window: dict[str, object],
    output_path: Path,
) -> None:
    _ref_start, start, end, _post_end, plot_start, plot_end = window_bounds(window)
    plot = corridor[
        corridor["road_key"].eq(cfg["road_key"])
        & corridor["day_type"].eq("weekday")
        & (corridor["minute_of_day"] >= plot_start)
        & (corridor["minute_of_day"] < plot_end)
    ].copy()
    if plot.empty:
        return
    mean = (
        plot.groupby(["corridor_id", "corridor_label", "role", "direction_group", "minute_of_day"], as_index=False)
        .agg(speed_kmh=("corridor_speed_kmh", "mean"))
    )
    corridor_order = (
        mean[["corridor_id", "corridor_label", "role"]]
        .drop_duplicates()
        .sort_values(["role", "corridor_label"])
    )
    treat_ids = corridor_order[corridor_order["role"].eq("treatment")]["corridor_id"].tolist()
    control_ids = corridor_order[corridor_order["role"].eq("control")]["corridor_id"].tolist()
    ordered_ids = treat_ids + control_ids
    palette = ["#2b6cb0", "#8c8c8c", "#b0b0b0", "#d0d0d0", "#f2a65a", "#7f7f7f"]
    colors = {cid: palette[i % len(palette)] for i, cid in enumerate(ordered_ids)}
    fig, axes = plt.subplots(1, 2, figsize=(12.2, 4.3), sharey=True)
    for ax, direction in zip(axes, DIRECTION_ORDER):
        sub = mean[mean["direction_group"].eq(direction)]
        for cid in ordered_ids:
            part = sub[sub["corridor_id"].eq(cid)]
            if part.empty:
                continue
            role = part["role"].iloc[0]
            ax.plot(
                part["minute_of_day"] / 60,
                part["speed_kmh"],
                color=colors[cid],
                linewidth=1.8 if role == "treatment" else 1.05,
                marker="o",
                markersize=2.5 if role == "treatment" else 1.8,
                alpha=0.96 if role == "treatment" else 0.78,
                label=part["corridor_label"].iloc[0],
            )
        ax.axvline(start / 60, color="#111111", linewidth=0.8, linestyle="--")
        ax.axvline(end / 60, color="#111111", linewidth=0.8, linestyle=":")
        ax.axvspan(start / 60, end / 60, color="#eeeeee", alpha=0.62)
        ax.set_xlim(plot_start / 60, plot_end / 60)
        ax.set_title(DIRECTION_LABELS[direction], loc="left", fontweight="bold")
        ax.set_xlabel("Hour")
        ax.grid(axis="y", color="#e5e5e5", linewidth=0.5)
        for spine in ["top", "right"]:
            ax.spines[spine].set_visible(False)
    axes[0].set_ylabel("Mean speed, km/h")
    handles, labels = axes[1].get_legend_handles_labels()
    if handles:
        fig.legend(handles, labels, frameon=False, loc="center right", bbox_to_anchor=(1.0, 0.5))
    fig.suptitle(
        f"{cfg['road_name_cn']} {window['label']} weekday 10-minute speeds",
        x=0.01,
        ha="left",
    )
    fig.tight_layout(rect=(0, 0, 0.88, 0.92))
    save_figure(fig, output_path)


def plot_event_study(coefficients: pd.DataFrame, cfg: dict[str, object], window: dict[str, object], output_path: Path) -> None:
    sub = coefficients[
        coefficients["road_key"].eq(cfg["road_key"])
        & coefficients["window_key"].eq(window["window_key"])
        & coefficients["control_set"].eq("pooled_controls")
    ].copy()
    if sub.empty:
        return
    fig, axes = plt.subplots(1, 2, figsize=(11.0, 4.2), sharey=True)
    colors = {POLICY: "#2b6cb0", OPPOSITE: "#c44e52"}
    for ax, direction in zip(axes, DIRECTION_ORDER):
        part = sub[sub["direction_group"].eq(direction)].sort_values("event_bin_min")
        if part.empty:
            continue
        ax.errorbar(
            part["event_bin_min"],
            part["coef"],
            yerr=[part["coef"] - part["ci95_low"], part["ci95_high"] - part["coef"]],
            fmt="o-",
            color=colors[direction],
            linewidth=1.1,
            markersize=3.0,
            capsize=2.0,
        )
        start = int(window["start_min"])
        end = int(window["end_min"])
        ax.axhline(0, color="#222222", linewidth=0.8)
        ax.axvline(0, color="#555555", linewidth=0.8, linestyle="--")
        ax.axvline(end - start, color="#555555", linewidth=0.8, linestyle=":")
        ax.axvspan(0, end - start, color="#eeeeee", alpha=0.62)
        ax.set_title(DIRECTION_LABELS[direction], loc="left", fontweight="bold")
        ax.set_xlabel(f"Minutes relative to {minute_label(start)}")
        ax.grid(axis="y", color="#e5e5e5", linewidth=0.5)
        for spine in ["top", "right"]:
            ax.spines[spine].set_visible(False)
    axes[0].set_ylabel("Treatment-control DID coefficient, km/h")
    fig.suptitle(f"{cfg['road_name_cn']} {window['label']} event-study DID, pooled controls", x=0.01, ha="left")
    fig.tight_layout(rect=(0, 0, 1, 0.92))
    save_figure(fig, output_path)


def road_window_order() -> list[tuple[str, str, str]]:
    order = []
    for _number, cfg in ROAD_CONFIGS.items():
        if not cfg["include_main"]:
            continue
        for window in cfg["windows"]:
            order.append((cfg["road_key"], window["window_key"], f"{cfg['road_name_cn']} {window['label']}"))
    return order


def event_window_duration_lookup() -> dict[tuple[str, str], int]:
    durations = {}
    for _number, cfg in ROAD_CONFIGS.items():
        if not cfg["include_main"]:
            continue
        for window in cfg["windows"]:
            durations[(cfg["road_key"], window["window_key"])] = int(window["end_min"]) - int(window["start_min"])
    return durations


def plot_window_change_summary(window_metrics: pd.DataFrame, output_path: Path) -> None:
    if window_metrics.empty:
        return
    rows = []
    group_cols = [
        "tidal_number",
        "road_key",
        "tidal_road_name",
        "window_key",
        "operation_window_label",
        "direction_group",
    ]
    for keys, sub in window_metrics.groupby(group_cols):
        treatment = sub[sub["role"].eq("treatment")]
        controls = sub[sub["role"].eq("control")]
        if treatment.empty or controls.empty:
            continue
        treatment_change = (treatment["active"] - treatment["reference"]).mean()
        control_change = (controls["active"] - controls["reference"]).mean()
        rows.append(
            {
                "road_key": keys[1],
                "tidal_road_name": keys[2],
                "window_key": keys[3],
                "operation_window_label": keys[4],
                "direction_group": keys[5],
                "diff_change_kmh": treatment_change - control_change,
            }
        )
    summary = pd.DataFrame(rows)
    if summary.empty:
        return
    order = road_window_order()
    labels = [label for road_key, window_key, label in order]
    positions = {(road_key, window_key): idx for idx, (road_key, window_key, _label) in enumerate(order)}
    colors = {POLICY: "#2b6cb0", OPPOSITE: "#c44e52"}
    offsets = {POLICY: -0.18, OPPOSITE: 0.18}
    fig, ax = plt.subplots(figsize=(10.8, 4.6))
    for direction in DIRECTION_ORDER:
        part = summary[summary["direction_group"].eq(direction)].copy()
        if part.empty:
            continue
        x = [positions[(r.road_key, r.window_key)] + offsets[direction] for r in part.itertuples()]
        ax.bar(
            x,
            part["diff_change_kmh"],
            width=0.34,
            color=colors[direction],
            alpha=0.88,
            label=DIRECTION_LABELS[direction],
        )
    ax.axhline(0, color="#222222", linewidth=0.8)
    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels, rotation=28, ha="right")
    ax.set_ylabel("Treatment minus controls, active-reference change (km/h)")
    ax.set_title("Active-window speed change relative to pre-window, refined controls", loc="left")
    ax.grid(axis="y", color="#e5e5e5", linewidth=0.5)
    ax.legend(frameon=False, loc="best")
    for spine in ["top", "right"]:
        ax.spines[spine].set_visible(False)
    fig.tight_layout()
    save_figure(fig, output_path)


def plot_asymmetry_summary(asymmetry: pd.DataFrame, output_path: Path) -> None:
    if asymmetry.empty or "dlog_asym_active_vs_reference" not in asymmetry.columns:
        return
    rows = []
    group_cols = [
        "tidal_number",
        "road_key",
        "tidal_road_name",
        "window_key",
        "operation_window_label",
    ]
    for keys, sub in asymmetry.groupby(group_cols):
        treatment = sub[sub["role"].eq("treatment")]
        controls = sub[sub["role"].eq("control")]
        if treatment.empty or controls.empty:
            continue
        rows.append(
            {
                "road_key": keys[1],
                "tidal_road_name": keys[2],
                "window_key": keys[3],
                "operation_window_label": keys[4],
                "diff_dlog_asym": (
                    treatment["dlog_asym_active_vs_reference"].mean()
                    - controls["dlog_asym_active_vs_reference"].mean()
                ),
            }
        )
    summary = pd.DataFrame(rows)
    if summary.empty:
        return
    order = road_window_order()
    summary["position"] = summary.apply(
        lambda r: next(
            (idx for idx, (road_key, window_key, _label) in enumerate(order)
             if road_key == r["road_key"] and window_key == r["window_key"]),
            np.nan,
        ),
        axis=1,
    )
    summary = summary.dropna(subset=["position"]).sort_values("position")
    labels = [f"{r.tidal_road_name} {r.operation_window_label}" for r in summary.itertuples()]
    colors = np.where(summary["diff_dlog_asym"] >= 0, "#2b6cb0", "#c44e52")
    fig, ax = plt.subplots(figsize=(9.8, 4.2))
    ax.bar(range(len(summary)), summary["diff_dlog_asym"], color=colors, alpha=0.88)
    ax.axhline(0, color="#222222", linewidth=0.8)
    ax.set_xticks(range(len(summary)))
    ax.set_xticklabels(labels, rotation=28, ha="right")
    ax.set_ylabel("Treatment minus controls, dlog(policy/opposite)")
    ax.set_title("Directional asymmetry change during active window, refined controls", loc="left")
    ax.grid(axis="y", color="#e5e5e5", linewidth=0.5)
    for spine in ["top", "right"]:
        ax.spines[spine].set_visible(False)
    fig.tight_layout()
    save_figure(fig, output_path)


def plot_event_active_summary(coefficients: pd.DataFrame, output_path: Path) -> None:
    if coefficients.empty:
        return
    durations = event_window_duration_lookup()
    pooled = coefficients[coefficients["control_set"].eq("pooled_controls")].copy()
    rows = []
    for keys, sub in pooled.groupby(["road_key", "tidal_road_name", "window_key", "operation_window_label", "direction_group"]):
        duration = durations.get((keys[0], keys[2]))
        if duration is None:
            continue
        active = sub[(sub["event_bin_min"] >= 0) & (sub["event_bin_min"] < duration)]
        if active.empty:
            continue
        rows.append(
            {
                "road_key": keys[0],
                "tidal_road_name": keys[1],
                "window_key": keys[2],
                "operation_window_label": keys[3],
                "direction_group": keys[4],
                "active_mean_coef": active["coef"].mean(),
            }
        )
    summary = pd.DataFrame(rows)
    if summary.empty:
        return
    order = road_window_order()
    labels = [label for road_key, window_key, label in order]
    positions = {(road_key, window_key): idx for idx, (road_key, window_key, _label) in enumerate(order)}
    colors = {POLICY: "#2b6cb0", OPPOSITE: "#c44e52"}
    offsets = {POLICY: -0.18, OPPOSITE: 0.18}
    fig, ax = plt.subplots(figsize=(10.8, 4.6))
    for direction in DIRECTION_ORDER:
        part = summary[summary["direction_group"].eq(direction)].copy()
        if part.empty:
            continue
        x = [positions[(r.road_key, r.window_key)] + offsets[direction] for r in part.itertuples()]
        ax.bar(
            x,
            part["active_mean_coef"],
            width=0.34,
            color=colors[direction],
            alpha=0.88,
            label=DIRECTION_LABELS[direction],
        )
    ax.axhline(0, color="#222222", linewidth=0.8)
    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels, rotation=28, ha="right")
    ax.set_ylabel("Mean active-bin event-study coefficient (km/h)")
    ax.set_title("Pooled-control event-study active-window coefficients, refined controls", loc="left")
    ax.grid(axis="y", color="#e5e5e5", linewidth=0.5)
    ax.legend(frameon=False, loc="best")
    for spine in ["top", "right"]:
        ax.spines[spine].set_visible(False)
    fig.tight_layout()
    save_figure(fig, output_path)


def main() -> None:
    args = parse_args()
    configure_plot()
    dirs = ensure_dirs(args.output_dir)

    roads = add_road_attributes(gpd.read_file(args.roads))
    speed_ids = read_speed_ids(args.speed)
    mapping = discover_number_mapping(args.tidal_dir)
    route_definitions = read_route_definitions(args.route_xlsx)
    route_conflicts = build_route_source_conflicts(route_definitions)
    all_exclusion = build_all_tidal_exclusion(args.tidal_dir)
    treatment = match_treatment_segments(roads, args.tidal_dir, speed_ids)
    match_sensitivity = build_match_sensitivity(roads, args.tidal_dir, speed_ids)
    control_table, control_segments = select_control_candidates(
        roads, treatment, all_exclusion, speed_ids
    )
    catalog = build_analysis_catalog(treatment, control_segments)
    selected_ids = set(catalog["roadseg_id"].astype(str).unique())
    speed = read_selected_speed(args.speed, selected_ids)
    panel = attach_speed_attributes(speed, catalog)
    segment_coverage = build_segment_speed_coverage(panel)

    corridor_rows = []
    window_rows = []
    metric_rows = []
    asym_rows = []
    event_coef_rows = []
    event_sample_rows = []
    event_diagnostics: dict[str, object] = {}
    for number, cfg0 in ROAD_CONFIGS.items():
        cfg = dict(cfg0)
        cfg["tidal_number"] = number
        if not cfg["include_main"]:
            continue
        for window in cfg["windows"]:
            corridor = build_corridor_speed(panel, cfg, window)
            if corridor.empty:
                continue
            corridor["window_key"] = window["window_key"]
            corridor["operation_window_label"] = window["label"]
            corridor_rows.append(corridor)
            daily = build_window_summary(corridor, window)
            window_rows.append(daily)
            metrics, asym = build_window_metrics(daily)
            metric_rows.append(metrics)
            asym_rows.append(asym)
            coef, event_sample, diag = build_event_study(panel, cfg, window, control_segments)
            if not coef.empty:
                event_coef_rows.append(coef)
            if not event_sample.empty:
                event_sample_rows.append(event_sample)
            event_diagnostics.update(diag)
            plot_timeseries(
                corridor,
                cfg,
                window,
                dirs["figures"] / f"{cfg['road_key']}_{window['window_key']}_weekday_timeseries.png",
            )
        plot_map(
            roads,
            treatment,
            control_segments,
            cfg,
            dirs["maps_osm"] / f"{cfg['road_key']}_treatment_controls_osm_masked.png",
            use_osm=True,
            add_mask=True,
        )
        plot_map(
            roads,
            treatment,
            control_segments,
            cfg,
            dirs["maps_plain"] / f"{cfg['road_key']}_treatment_controls_plain.png",
            use_osm=False,
            add_mask=False,
        )

    corridor_out = pd.concat(corridor_rows, ignore_index=True) if corridor_rows else pd.DataFrame()
    window_daily = pd.concat(window_rows, ignore_index=True) if window_rows else pd.DataFrame()
    window_metrics = pd.concat(metric_rows, ignore_index=True) if metric_rows else pd.DataFrame()
    asymmetry = pd.concat(asym_rows, ignore_index=True) if asym_rows else pd.DataFrame()
    event_coef = pd.concat(event_coef_rows, ignore_index=True) if event_coef_rows else pd.DataFrame()
    event_sample = pd.concat(event_sample_rows, ignore_index=True) if event_sample_rows else pd.DataFrame()
    length_summary = build_corridor_length_summary(catalog)
    direction_length_summary = build_direction_length_summary(catalog)
    treatment_trend_summary, treatment_trend_daily = build_treatment_only_window_trends(window_daily)

    if not event_coef.empty:
        for number, cfg0 in ROAD_CONFIGS.items():
            if not cfg0["include_main"]:
                continue
            cfg = dict(cfg0)
            cfg["tidal_number"] = number
            for window in cfg["windows"]:
                plot_event_study(
                    event_coef,
                    cfg,
                    window,
                    dirs["figures"] / f"{cfg['road_key']}_{window['window_key']}_event_study_did_pooled.png",
                )
    plot_window_change_summary(
        window_metrics,
        dirs["summary_figures"] / "active_reference_treatment_control_change_summary.png",
    )
    plot_asymmetry_summary(
        asymmetry,
        dirs["summary_figures"] / "directional_asymmetry_treatment_control_summary.png",
    )
    plot_event_active_summary(
        event_coef,
        dirs["summary_figures"] / "event_study_active_window_summary.png",
    )

    mapping.to_csv(dirs["metrics"] / "tidal_number_mapping.csv", index=False, encoding="utf-8-sig")
    route_definitions.to_csv(dirs["metrics"] / "tianyu_excel_route_definitions.csv", index=False, encoding="utf-8-sig")
    route_conflicts.to_csv(dirs["metrics"] / "route_source_conflicts.csv", index=False, encoding="utf-8-sig")
    match_sensitivity.to_csv(dirs["metrics"] / "match_sensitivity_by_road.csv", index=False, encoding="utf-8-sig")
    treatment.to_file(dirs["data"] / "matched_treatment_segments.geojson", driver="GeoJSON")
    treatment.drop(columns="geometry").to_csv(
        dirs["metrics"] / "matched_treatment_segments.csv",
        index=False,
        encoding="utf-8-sig",
    )
    control_table.to_csv(dirs["metrics"] / "control_candidates_by_tidal_lane.csv", index=False, encoding="utf-8-sig")
    control_segments.to_file(dirs["data"] / "control_candidate_segments.geojson", driver="GeoJSON")
    control_segments.drop(columns="geometry").to_csv(
        dirs["metrics"] / "control_candidate_segments.csv",
        index=False,
        encoding="utf-8-sig",
    )
    catalog.to_file(dirs["data"] / "analysis_segment_catalog.geojson", driver="GeoJSON")
    catalog.drop(columns="geometry").to_csv(dirs["data"] / "analysis_segment_catalog.csv", index=False, encoding="utf-8-sig")
    panel.to_csv(dirs["data"] / "selected_segment_speed_panel.csv", index=False, encoding="utf-8-sig")
    segment_coverage.to_csv(dirs["metrics"] / "segment_speed_coverage.csv", index=False, encoding="utf-8-sig")
    length_summary.to_csv(dirs["metrics"] / "treatment_control_corridor_length_summary.csv", index=False, encoding="utf-8-sig")
    direction_length_summary.to_csv(
        dirs["metrics"] / "treatment_control_direction_length_summary.csv",
        index=False,
        encoding="utf-8-sig",
    )
    corridor_out.to_csv(dirs["data"] / "corridor_10min_speed.csv", index=False, encoding="utf-8-sig")
    window_daily.to_csv(dirs["data"] / "window_daily_summary.csv", index=False, encoding="utf-8-sig")
    window_metrics.to_csv(dirs["metrics"] / "window_direction_metrics.csv", index=False, encoding="utf-8-sig")
    asymmetry.to_csv(dirs["metrics"] / "window_directional_asymmetry.csv", index=False, encoding="utf-8-sig")
    treatment_trend_summary.to_csv(
        dirs["metrics"] / "treatment_only_window_trends.csv",
        index=False,
        encoding="utf-8-sig",
    )
    treatment_trend_daily.to_csv(
        dirs["data"] / "treatment_only_window_trends_daily.csv",
        index=False,
        encoding="utf-8-sig",
    )
    event_coef.to_csv(dirs["metrics"] / "event_study_did_coefficients.csv", index=False, encoding="utf-8-sig")
    event_sample.to_csv(dirs["data"] / "event_study_did_sample.csv", index=False, encoding="utf-8-sig")
    with (dirs["metrics"] / "event_study_did_diagnostics.json").open("w", encoding="utf-8") as fh:
        json.dump(event_diagnostics, fh, ensure_ascii=False, indent=2)
    spec = {
        "version": "shenzhen_multi_tidal_post_only_v1",
        "tidal_number_mapping": mapping.to_dict(orient="records"),
        "tianyu_excel_source": str(args.route_xlsx),
        "route_source_conflicts": route_conflicts.to_dict(orient="records"),
        "excluded": "Tidal number 3 is highway / 惠深沿海高速 and excluded from main urban-road analysis.",
        "control_rule": {
            "search_distance_m": CONTROL_SEARCH_M,
            "axis_diff_max_deg": CONTROL_AXIS_MAX_DIFF_DEG,
            "minimum_distance_to_treatment_m": CONTROL_MIN_DISTANCE_M,
            "exclusion_buffer_m": TIDAL_EXCLUSION_BUFFER_M,
            "minimum_control_length_m": MIN_CONTROL_LENGTH_M,
            "minimum_length_ratio_to_treatment_analysis_length": CONTROL_MIN_LENGTH_RATIO,
            "maximum_length_ratio_to_treatment_analysis_length": CONTROL_MAX_LENGTH_RATIO,
            "excluded_control_roadnames": sorted(EXCLUDED_CONTROL_ROADNAMES),
            "excluded_control_name_keywords": list(EXCLUDED_CONTROL_NAME_KEYWORDS),
            "max_controls": MAX_CONTROLS,
        },
        "core_treatment_rule": {
            "buffer_m": BUFFER_M,
            "axis_diff_max_deg": CORE_AXIS_MAX_DIFF_DEG,
            "distance_to_single_max_m": CORE_MAX_DISTANCE_M,
            "intersect_length_min_m": CORE_MIN_INTERSECT_M,
            "definition": (
                "A matched Baidu segment is core treatment if its road name matches the target road "
                "or it is aligned with the tidal-lane axis, close to the single-line shapefile, "
                "and has enough overlap. Other intersecting segments are retained as auxiliary only."
            ),
            "length_weighting": (
                "Treatment corridor speeds use the matched road segment speed but weight each segment by "
                "its overlap length with the tidal-lane buffer, not by the full Baidu roadseg length. "
                "Controls use the full selected Baidu roadseg length."
            ),
        },
        "event_study_did_specification": (
            "speed_{i d m} = alpha_i + delta_d + lambda_m + "
            "sum_{k != -30} beta_k TreatmentCorridor_i * 1[event_bin_m = k] + epsilon_{i d m}. "
            "Models are estimated separately by tidal lane, operation window, direction, and control set."
        ),
        "fixed_effects": ["road segment", "date", "minute_of_day"],
        "cluster": "road segment",
        "interpretation": "Post-only within-day treatment-control comparison, not pre/post opening effect.",
    }
    with (dirs["metrics"] / "analysis_specification.json").open("w", encoding="utf-8") as fh:
        json.dump(spec, fh, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
