from __future__ import annotations

import argparse
import math
import time
import urllib.request
from io import BytesIO
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.patheffects as pe
import numpy as np
import pandas as pd
from matplotlib.colors import Normalize, TwoSlopeNorm
from matplotlib.lines import Line2D
from PIL import Image


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data_work/outputs/shenzhen_caitian_baidu_analysis_v1"
DEFAULT_TIDAL_DOUBLE = PROJECT_ROOT / "data_work/raw_data/gis/深圳潮汐车道shp/2_双线.shp"

WEB_MERCATOR_CRS = "EPSG:3857"
TILE_URLS = [
    "https://tile.openstreetmap.org/{z}/{x}/{y}.png",
    "https://a.tile.openstreetmap.org/{z}/{x}/{y}.png",
    "https://b.tile.openstreetmap.org/{z}/{x}/{y}.png",
    "https://a.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png",
]
OSM_USER_AGENT = "TidalLanes research visualization"
ACTIVE_WINDOW = (16 * 60 + 30, 21 * 60 + 30)
REFERENCE_WINDOW = (15 * 60, 16 * 60 + 30)

DIRECTION_LABELS = {
    "policy_direction_futian_to_longhua": "Futian -> Longhua",
    "opposite_direction_longhua_to_futian": "Longhua -> Futian",
}
ROLE_COLORS = {
    "caitian": "#2b6cb0",
    "control_huanggang": "#4c78a8",
    "control_jintian": "#59a14f",
    "control_yitian": "#b07aa1",
    "control_xiangmihu": "#f28e2b",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot Caitian Baidu results over OpenStreetMap basemap."
    )
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--tidal-double", type=Path, default=DEFAULT_TIDAL_DOUBLE)
    parser.add_argument("--zoom", type=int, default=None)
    return parser.parse_args()


def configure_plot() -> None:
    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "axes.titlesize": 12,
            "axes.labelsize": 9,
            "xtick.labelsize": 8,
            "ytick.labelsize": 8,
            "legend.fontsize": 8,
            "figure.dpi": 180,
            "savefig.dpi": 300,
        }
    )


def save_figure(fig: plt.Figure, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, bbox_inches="tight")
    fig.savefig(output_path.with_suffix(".pdf"), bbox_inches="tight")
    fig.savefig(output_path.with_suffix(".svg"), bbox_inches="tight")
    plt.close(fig)


def lonlat_to_tile(lon: float, lat: float, zoom: int) -> tuple[int, int]:
    lat = min(max(lat, -85.05112878), 85.05112878)
    n = 2**zoom
    x = int((lon + 180.0) / 360.0 * n)
    lat_rad = math.radians(lat)
    y = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
    return max(0, min(n - 1, x)), max(0, min(n - 1, y))


def lonlat_to_mercator(lon: float, lat: float) -> tuple[float, float]:
    radius = 6378137.0
    lat = min(max(lat, -85.05112878), 85.05112878)
    x = radius * math.radians(lon)
    y = radius * math.log(math.tan(math.pi / 4.0 + math.radians(lat) / 2.0))
    return x, y


def tile_bounds_mercator(x: int, y: int, zoom: int) -> tuple[float, float, float, float]:
    n = 2**zoom

    def tile_to_lonlat(tx: int, ty: int) -> tuple[float, float]:
        lon = tx / n * 360.0 - 180.0
        lat_rad = math.atan(math.sinh(math.pi * (1.0 - 2.0 * ty / n)))
        return lon, math.degrees(lat_rad)

    west, north = tile_to_lonlat(x, y)
    east, south = tile_to_lonlat(x + 1, y + 1)
    minx, maxy = lonlat_to_mercator(west, north)
    maxx, miny = lonlat_to_mercator(east, south)
    return minx, miny, maxx, maxy


def tile_count_for_zoom(bounds4326: tuple[float, float, float, float], zoom: int) -> int:
    min_lon, min_lat, max_lon, max_lat = bounds4326
    x0, y0 = lonlat_to_tile(min_lon, max_lat, zoom)
    x1, y1 = lonlat_to_tile(max_lon, min_lat, zoom)
    return (abs(x1 - x0) + 1) * (abs(y1 - y0) + 1)


def choose_zoom(bounds4326: tuple[float, float, float, float], requested: int | None) -> int:
    if requested is not None:
        return requested
    for zoom in range(14, 11, -1):
        if tile_count_for_zoom(bounds4326, zoom) <= 36:
            return zoom
    return 12


def fetch_tile(z: int, x: int, y: int, cache_dir: Path) -> Image.Image:
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / f"{z}_{x}_{y}.png"
    if cache_path.exists():
        return Image.open(cache_path).convert("RGB")
    last_error: Exception | None = None
    for url in TILE_URLS:
        for _attempt in range(2):
            try:
                request = urllib.request.Request(
                    url.format(z=z, x=x, y=y),
                    headers={"User-Agent": OSM_USER_AGENT},
                )
                with urllib.request.urlopen(request, timeout=20) as response:
                    tile = Image.open(BytesIO(response.read())).convert("RGB")
                tile.save(cache_path)
                time.sleep(0.08)
                return tile
            except Exception as exc:
                last_error = exc
                time.sleep(0.25)
    print(f"Warning: failed to fetch tile z={z} x={x} y={y}: {last_error}")
    return Image.new("RGB", (256, 256), "#f2f2f2")


def osm_mosaic(
    bounds4326: tuple[float, float, float, float],
    cache_dir: Path,
    zoom: int,
) -> tuple[Image.Image, tuple[float, float, float, float]]:
    min_lon, min_lat, max_lon, max_lat = bounds4326
    x0, y0 = lonlat_to_tile(min_lon, max_lat, zoom)
    x1, y1 = lonlat_to_tile(max_lon, min_lat, zoom)
    xs = range(min(x0, x1), max(x0, x1) + 1)
    ys = range(min(y0, y1), max(y0, y1) + 1)
    tile_size = 256
    mosaic = Image.new("RGB", (len(xs) * tile_size, len(ys) * tile_size), "white")
    for ix, x in enumerate(xs):
        for iy, y in enumerate(ys):
            tile = fetch_tile(zoom, x, y, cache_dir)
            mosaic.paste(tile, (ix * tile_size, iy * tile_size))
    west, _south, _east, north = tile_bounds_mercator(min(xs), min(ys), zoom)
    _west, south, east, _north = tile_bounds_mercator(max(xs), max(ys), zoom)
    return mosaic, (west, east, south, north)


def bounds4326(layers: list[gpd.GeoDataFrame], pad_fraction: float = 0.06) -> tuple[float, float, float, float]:
    bounds = pd.concat([layer.to_crs("EPSG:4326").geometry.bounds for layer in layers if not layer.empty])
    minx, miny = bounds[["minx", "miny"]].min()
    maxx, maxy = bounds[["maxx", "maxy"]].max()
    dx = max(maxx - minx, 0.004) * pad_fraction
    dy = max(maxy - miny, 0.004) * pad_fraction
    return float(minx - dx), float(miny - dy), float(maxx + dx), float(maxy + dy)


def add_osm_background(
    ax: plt.Axes,
    bounds: tuple[float, float, float, float],
    cache_dir: Path,
    zoom: int,
) -> None:
    image, extent = osm_mosaic(bounds, cache_dir, zoom)
    ax.imshow(image, extent=extent, origin="upper", alpha=0.82)
    minx, miny = lonlat_to_mercator(bounds[0], bounds[1])
    maxx, maxy = lonlat_to_mercator(bounds[2], bounds[3])
    ax.set_xlim(minx, maxx)
    ax.set_ylim(miny, maxy)


def label_corridors(ax: plt.Axes, segments3857: gpd.GeoDataFrame) -> None:
    labels = (
        segments3857[segments3857["corridor_id"].notna()]
        .dissolve(by=["corridor_id", "corridor_label"], as_index=False)
        [["corridor_id", "corridor_label", "geometry"]]
    )
    for _, row in labels.iterrows():
        point = row.geometry.representative_point()
        ax.text(
            point.x,
            point.y,
            row["corridor_label"],
            fontsize=7.2,
            color="#111111",
            ha="center",
            va="center",
            path_effects=[pe.withStroke(linewidth=2.2, foreground="white")],
            zorder=10,
        )


def clean_map_axes(ax: plt.Axes) -> None:
    ax.set_axis_off()
    ax.set_aspect("equal")
    ax.text(
        0.005,
        0.01,
        "Basemap: OpenStreetMap contributors; CARTO fallback where needed",
        transform=ax.transAxes,
        fontsize=6.5,
        color="#555555",
        path_effects=[pe.withStroke(linewidth=1.8, foreground="white")],
    )


def weekday_segment_metrics(panel: pd.DataFrame) -> pd.DataFrame:
    weekday = panel[panel["day_type"].eq("weekday")].copy()
    active = weekday[
        (weekday["minute_of_day"] >= ACTIVE_WINDOW[0])
        & (weekday["minute_of_day"] < ACTIVE_WINDOW[1])
    ]
    reference = weekday[
        (weekday["minute_of_day"] >= REFERENCE_WINDOW[0])
        & (weekday["minute_of_day"] < REFERENCE_WINDOW[1])
    ]
    active_mean = active.groupby("roadseg_id", as_index=False).agg(
        active_speed_kmh=("speed_kmh", "mean")
    )
    reference_mean = reference.groupby("roadseg_id", as_index=False).agg(
        reference_speed_kmh=("speed_kmh", "mean")
    )
    out = active_mean.merge(reference_mean, on="roadseg_id", how="outer")
    out["active_over_reference_speed"] = out["active_speed_kmh"] / out["reference_speed_kmh"]
    return out


def plot_overview(
    segments: gpd.GeoDataFrame,
    network: gpd.GeoDataFrame,
    tidal: gpd.GeoDataFrame,
    output_path: Path,
    cache_dir: Path,
    zoom: int | None,
) -> None:
    bounds = bounds4326([segments, network, tidal])
    zoom = choose_zoom(bounds, zoom)
    fig, ax = plt.subplots(figsize=(8.4, 9.0))
    add_osm_background(ax, bounds, cache_dir, zoom)
    seg3857 = segments.to_crs(WEB_MERCATOR_CRS)
    net3857 = network.to_crs(WEB_MERCATOR_CRS)
    tidal3857 = tidal.to_crs(WEB_MERCATOR_CRS)
    if not net3857.empty:
        net3857.plot(ax=ax, color="#666666", linewidth=1.0, alpha=0.35, zorder=2)
    for corridor_id, part in seg3857.groupby("corridor_id"):
        if corridor_id == "caitian":
            continue
        part.plot(
            ax=ax,
            color=ROLE_COLORS.get(corridor_id, "#999999"),
            linewidth=3.0,
            alpha=0.88,
            zorder=4,
        )
    for direction, part in seg3857[seg3857["corridor_id"].eq("caitian")].groupby("direction_group"):
        color = "#2b6cb0" if direction == "policy_direction_futian_to_longhua" else "#c44e52"
        part.plot(ax=ax, color=color, linewidth=5.0, alpha=0.96, zorder=6)
    tidal3857.plot(ax=ax, color="#111111", linewidth=1.0, alpha=0.75, zorder=7)
    label_corridors(ax, seg3857)
    handles = [
        Line2D([0], [0], color="#2b6cb0", lw=4.0, label="Caitian, Futian -> Longhua"),
        Line2D([0], [0], color="#c44e52", lw=4.0, label="Caitian, Longhua -> Futian"),
        Line2D([0], [0], color="#666666", lw=2.0, alpha=0.55, label="3 km network"),
        Line2D([0], [0], color="#111111", lw=1.2, label="Tidal-lane shp"),
    ]
    ax.legend(handles=handles, loc="lower right", frameon=True, framealpha=0.88)
    ax.set_title("Caitian treatment, controls, and directed network on OSM", loc="left")
    clean_map_axes(ax)
    save_figure(fig, output_path)


def plot_metric_map(
    segments: gpd.GeoDataFrame,
    tidal: gpd.GeoDataFrame,
    metric_col: str,
    output_path: Path,
    title: str,
    colorbar_label: str,
    cache_dir: Path,
    zoom: int | None,
    cmap: str,
    norm,
) -> None:
    plot_data = segments[segments[metric_col].notna()].copy()
    bounds = bounds4326([plot_data, tidal])
    zoom = choose_zoom(bounds, zoom)
    fig, ax = plt.subplots(figsize=(8.4, 9.0))
    add_osm_background(ax, bounds, cache_dir, zoom)
    data3857 = plot_data.to_crs(WEB_MERCATOR_CRS)
    tidal3857 = tidal.to_crs(WEB_MERCATOR_CRS)
    data3857.plot(ax=ax, color="#bdbdbd", linewidth=5.2, alpha=0.42, zorder=2)
    data3857.plot(
        ax=ax,
        column=metric_col,
        cmap=cmap,
        norm=norm,
        linewidth=np.where(data3857["corridor_id"].eq("caitian"), 5.0, 3.4),
        alpha=0.96,
        zorder=4,
    )
    tidal3857.plot(ax=ax, color="#111111", linewidth=1.0, alpha=0.70, zorder=6)
    label_corridors(ax, data3857)
    scalar = plt.cm.ScalarMappable(norm=norm, cmap=cmap)
    cbar = fig.colorbar(scalar, ax=ax, fraction=0.034, pad=0.01)
    cbar.set_label(colorbar_label)
    ax.set_title(title, loc="left")
    clean_map_axes(ax)
    save_figure(fig, output_path)


def plot_network_metric_map(
    treatment: gpd.GeoDataFrame,
    network: gpd.GeoDataFrame,
    metric_col: str,
    output_path: Path,
    cache_dir: Path,
    zoom: int | None,
) -> None:
    plot_data = network[network[metric_col].notna()].copy()
    bounds = bounds4326([treatment, plot_data])
    zoom = choose_zoom(bounds, zoom)
    fig, ax = plt.subplots(figsize=(8.0, 9.0))
    add_osm_background(ax, bounds, cache_dir, zoom)
    net3857 = plot_data.to_crs(WEB_MERCATOR_CRS)
    treat3857 = treatment.to_crs(WEB_MERCATOR_CRS)
    norm = TwoSlopeNorm(vmin=0.45, vcenter=1.0, vmax=1.15)
    net3857.plot(ax=ax, color="#cccccc", linewidth=4.2, alpha=0.35, zorder=2)
    net3857.plot(
        ax=ax,
        column=metric_col,
        cmap="RdBu",
        norm=norm,
        linewidth=3.6,
        alpha=0.95,
        zorder=4,
    )
    for direction, part in treat3857.groupby("direction_group"):
        color = "#2b6cb0" if direction == "policy_direction_futian_to_longhua" else "#c44e52"
        part.plot(ax=ax, color=color, linewidth=5.0, alpha=0.95, zorder=6)
    scalar = plt.cm.ScalarMappable(norm=norm, cmap="RdBu")
    cbar = fig.colorbar(scalar, ax=ax, fraction=0.034, pad=0.01)
    cbar.set_label("16:30-21:30 / 15:00-16:30 speed")
    ax.set_title("3 km directed network active/reference ratio on OSM", loc="left")
    clean_map_axes(ax)
    save_figure(fig, output_path)


def main() -> None:
    args = parse_args()
    configure_plot()
    figures = args.output_dir / "figures"
    data_dir = args.output_dir / "data"
    cache_dir = data_dir / "osm_tile_cache"
    segments = gpd.read_file(data_dir / "analysis_segment_catalog.geojson")
    network = gpd.read_file(data_dir / "network_segments_3km.geojson")
    tidal = gpd.read_file(args.tidal_double).to_crs("EPSG:4326")
    panel = pd.read_csv(data_dir / "selected_segment_speed_panel.csv")
    raw_panel = pd.read_csv(data_dir / "selected_speed_panel_raw.csv")
    metrics = weekday_segment_metrics(panel)
    network_metrics = weekday_segment_metrics(raw_panel)
    segments = segments.merge(metrics, on="roadseg_id", how="left")
    network = network.merge(network_metrics, on="roadseg_id", how="left")
    treatment = segments[segments["corridor_id"].eq("caitian")].copy()

    plot_overview(
        segments,
        network,
        tidal,
        figures / "osm_caitian_treatment_controls_network.png",
        cache_dir,
        args.zoom,
    )
    plot_metric_map(
        segments,
        tidal,
        "active_speed_kmh",
        figures / "osm_weekday_active_speed_segments.png",
        "Weekday 16:30-21:30 segment speed on OSM",
        "Mean speed, km/h",
        cache_dir,
        args.zoom,
        cmap="RdYlGn",
        norm=Normalize(vmin=8, vmax=60),
    )
    plot_metric_map(
        segments,
        tidal,
        "active_over_reference_speed",
        figures / "osm_weekday_active_reference_ratio_segments.png",
        "Weekday active/reference speed ratio on OSM",
        "16:30-21:30 / 15:00-16:30 speed",
        cache_dir,
        args.zoom,
        cmap="RdBu",
        norm=TwoSlopeNorm(vmin=0.35, vcenter=1.0, vmax=1.25),
    )
    plot_network_metric_map(
        treatment,
        network,
        "active_over_reference_speed",
        figures / "osm_network_active_reference_ratio.png",
        cache_dir,
        args.zoom,
    )
    osm_manifest = pd.DataFrame(
        [
            {
                "figure": path.name,
                "description": description,
            }
            for path, description in [
                (
                    figures / "osm_caitian_treatment_controls_network.png",
                    "Treatment/control/network overlay on OpenStreetMap.",
                ),
                (
                    figures / "osm_weekday_active_speed_segments.png",
                    "Segment-level weekday active-window speed on OpenStreetMap.",
                ),
                (
                    figures / "osm_weekday_active_reference_ratio_segments.png",
                    "Segment-level weekday active/reference ratio on OpenStreetMap.",
                ),
                (
                    figures / "osm_network_active_reference_ratio.png",
                    "3 km directed-network active/reference ratio on OpenStreetMap.",
                ),
            ]
        ]
    )
    osm_manifest.to_csv(args.output_dir / "metrics/osm_overlay_manifest.csv", index=False)


if __name__ == "__main__":
    main()
