from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import geopandas as gpd
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from shapely.affinity import translate
from shapely.geometry import GeometryCollection, MultiLineString, box

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT / "data_work"))

from src.shenzhen_liuxian_gaode.compare_baidu_shifted_overlap_with_gaode_weeks import (  # noqa: E402
    CHAIN_ORDER,
    SHIFT_DIAGNOSTIC,
    SOURCE_ORDER,
    SOURCE_TITLES,
    add_week_index,
    find_shifted_overlap_baidu_segments,
    load_or_extract_baidu_panel,
    shifted_gaode_segments,
)
from src.shenzhen_liuxian_gaode.event_study_liuxian import (  # noqa: E402
    DEFAULT_INPUT_DIR,
    DEFAULT_OUTPUT_DIR,
    POLICY_DATE,
    configure_plot,
    ensure_output_dirs,
)
from src.shenzhen_liuxian_gaode.match_baidu_underlying_segments import (  # noqa: E402
    DEFAULT_BAIDU_ROADS,
    DEFAULT_BAIDU_SPEED,
    load_gaode_segments,
)
from src.shenzhen_liuxian_gaode.plot_chain_window_speeds import assign_chain_direction  # noqa: E402


WINDOWS = {
    "night_0000_0500": {
        "label": "Night\n00:00-05:00",
        "lo": 0,
        "hi": 5 * 60,
        "weekdays_only": False,
    },
    "weekday_1500_1730": {
        "label": "Weekday reference\n15:00-17:30",
        "lo": 15 * 60,
        "hi": 17 * 60 + 30,
        "weekdays_only": True,
    },
    "weekday_1730_2000": {
        "label": "Weekday tidal window\n17:30-20:00",
        "lo": 17 * 60 + 30,
        "hi": 20 * 60,
        "weekdays_only": True,
    },
}
WINDOW_ORDER = list(WINDOWS)
SPEED_VMIN = 10.0
SPEED_VMAX = 60.0
BAIDU_CLIP_BUFFER_M = 25.0
DIRECTION_DISPLAY_OFFSET_M = 8.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Map key-window average speeds on shifted Liuxian Avenue overlap."
    )
    parser.add_argument("--gaode-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--baidu-roads", type=Path, default=DEFAULT_BAIDU_ROADS)
    parser.add_argument("--baidu-speed", type=Path, default=DEFAULT_BAIDU_SPEED)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def explode_line_parts(geom):
    if geom is None or geom.is_empty:
        return []
    if geom.geom_type == "LineString":
        return [geom]
    if isinstance(geom, MultiLineString):
        return list(geom.geoms)
    if isinstance(geom, GeometryCollection):
        lines = []
        for part in geom.geoms:
            lines.extend(explode_line_parts(part))
        return lines
    return []


def filter_window(df: pd.DataFrame, window: str) -> pd.DataFrame:
    spec = WINDOWS[window]
    sub = df[(df["minute_of_day"] >= spec["lo"]) & (df["minute_of_day"] < spec["hi"])].copy()
    if spec["weekdays_only"]:
        sub = sub[sub["datetime"].dt.dayofweek < 5].copy()
    return sub


def period_from_datetime(series: pd.Series) -> str:
    min_date = series.min().strftime("%Y%m%d")
    max_date = series.max().strftime("%Y%m%d")
    prefix = "pre" if series.min() < POLICY_DATE else "post"
    return f"{prefix}_{min_date}_{max_date}"


def load_full_gaode_panel(gaode_dir: Path) -> pd.DataFrame:
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
        df["period"] = period_from_datetime(df["datetime"])
        df["date"] = pd.to_datetime(df["datetime"].dt.date)
        df["dow"] = df["datetime"].dt.dayofweek
        df["minute_of_day"] = df["datetime"].dt.hour * 60 + df["datetime"].dt.minute
        df["speed_kmh"] = pd.to_numeric(df["速度"], errors="coerce")
        df["segment_length_m"] = pd.to_numeric(df["路段长度"], errors="coerce")
        df["segment_id"] = df["路段编号"].astype(str)
        df["source_file"] = path.name
        frames.append(df)
    if not frames:
        raise FileNotFoundError(f"No CSV files found under {gaode_dir}")
    panel = pd.concat(frames, ignore_index=True)
    panel = panel[(panel["speed_kmh"] > 0) & (panel["segment_length_m"] > 0)].copy()
    return panel


def build_gaode_segment_speeds(gaode_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    panel = load_full_gaode_panel(gaode_dir)
    panel, inventory = assign_chain_direction(panel)
    panel["source_period"] = np.where(panel["date"] < POLICY_DATE, "gaode_pre", "gaode_post")
    rows = []
    for window in WINDOW_ORDER:
        sub = filter_window(panel, window)
        grouped = (
            sub.groupby(
                [
                    "source_period",
                    "chain_direction",
                    "segment_id",
                    "路段编号",
                    "路段名称（唯一）",
                    "路段方向",
                    "路段长度",
                ],
                as_index=False,
            )
            .agg(
                mean_speed_kmh=("speed_kmh", "mean"),
                median_speed_kmh=("speed_kmh", "median"),
                n_obs=("speed_kmh", "size"),
                n_days=("date", "nunique"),
            )
        )
        grouped["window"] = window
        rows.append(grouped)
    return pd.concat(rows, ignore_index=True), inventory


def build_baidu_segment_speeds(
    output_dir: Path,
    speed_csv: Path,
    selected_baidu_m: gpd.GeoDataFrame,
) -> pd.DataFrame:
    ids = set(selected_baidu_m["roadseg_id"].astype(str))
    panel = load_or_extract_baidu_panel(output_dir, speed_csv, ids)
    attrs = selected_baidu_m.drop(columns="geometry")[
        ["roadseg_id", "roadname", "semantic", "location", "chain_direction"]
    ].copy()
    panel["roadseg_id"] = panel["roadseg_id"].astype(str)
    attrs["roadseg_id"] = attrs["roadseg_id"].astype(str)
    panel = panel.merge(attrs, on=["roadseg_id", "roadname", "semantic", "location"], how="inner")
    panel["speed_kmh"] = pd.to_numeric(panel["speed"], errors="coerce")
    panel["source_period"] = "baidu_2025"
    panel["datetime"] = pd.to_datetime(panel["datetime"])
    panel["date"] = pd.to_datetime(panel["date"])

    rows = []
    for window in WINDOW_ORDER:
        sub = filter_window(panel, window)
        grouped = (
            sub.groupby(
                ["source_period", "chain_direction", "roadseg_id", "roadname", "semantic", "location"],
                as_index=False,
            )
            .agg(
                mean_speed_kmh=("speed_kmh", "mean"),
                median_speed_kmh=("speed_kmh", "median"),
                n_obs=("speed_kmh", "size"),
                n_days=("date", "nunique"),
            )
        )
        grouped["window"] = window
        rows.append(grouped)
    return pd.concat(rows, ignore_index=True)


def build_map_layers(
    gaode_dir: Path,
    roads: gpd.GeoDataFrame,
    metric_crs,
    output_dir: Path,
    speed_csv: Path,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, pd.DataFrame]:
    shift_table = pd.read_csv(SHIFT_DIAGNOSTIC)
    gaode_segments = load_gaode_segments(gaode_dir)
    gaode_shifted_m = shifted_gaode_segments(gaode_segments, shift_table, metric_crs)
    gaode_all_m = gaode_shifted_m.copy()
    selected_baidu_m = find_shifted_overlap_baidu_segments(gaode_shifted_m, roads.to_crs(metric_crs))

    gaode_speeds, gaode_inventory = build_gaode_segment_speeds(gaode_dir)
    gaode_layer = gaode_all_m.rename(columns={"路段编号": "segment_id_for_join"}).copy()
    gaode_layer["segment_id_for_join"] = gaode_layer["segment_id_for_join"].astype(str)
    gaode_speeds["segment_id_for_join"] = gaode_speeds["segment_id"].astype(str)
    gaode_map = gaode_layer.merge(gaode_speeds, on=["segment_id_for_join", "chain_direction"], how="inner")
    gaode_map["source_period"] = gaode_map["source_period"].astype(str)
    gaode_map["geometry_plot"] = gaode_map.geometry
    gaode_map["source_geometry"] = "shifted_gaode_segment"

    baidu_speeds = build_baidu_segment_speeds(output_dir, speed_csv, selected_baidu_m)
    baidu_layer = selected_baidu_m.copy()
    target_buffers = {
        chain: gaode_shifted_m.loc[gaode_shifted_m["chain_direction"].eq(chain), "geometry"].union_all().buffer(
            BAIDU_CLIP_BUFFER_M
        )
        for chain in CHAIN_ORDER
    }
    baidu_layer["geometry_plot"] = baidu_layer.apply(
        lambda row: row.geometry.intersection(target_buffers[row["chain_direction"]]),
        axis=1,
    )
    baidu_layer["roadseg_id"] = baidu_layer["roadseg_id"].astype(str)
    baidu_speeds["roadseg_id"] = baidu_speeds["roadseg_id"].astype(str)
    baidu_map = baidu_layer.merge(
        baidu_speeds,
        on=["roadseg_id", "roadname", "semantic", "location", "chain_direction"],
        how="left",
    )
    baidu_map["source_period"] = "baidu_2025"
    baidu_map["source_geometry"] = "baidu_roadseg_clipped_to_shifted_gaode_buffer"

    keep_cols = [
        "source_period",
        "window",
        "chain_direction",
        "mean_speed_kmh",
        "median_speed_kmh",
        "n_obs",
        "n_days",
        "source_geometry",
        "geometry_plot",
    ]
    gaode_plot = gpd.GeoDataFrame(gaode_map[keep_cols], geometry="geometry_plot", crs=metric_crs)
    baidu_plot = gpd.GeoDataFrame(baidu_map[keep_cols], geometry="geometry_plot", crs=metric_crs)
    plot_layers = gpd.GeoDataFrame(pd.concat([baidu_plot, gaode_plot], ignore_index=True), geometry="geometry_plot", crs=metric_crs)

    segment_table = pd.concat(
        [
            baidu_map.drop(columns=["geometry", "geometry_plot"], errors="ignore"),
            gaode_map.drop(columns=["geometry", "geometry_original_m", "geometry_plot"], errors="ignore"),
        ],
        ignore_index=True,
        sort=False,
    )
    return plot_layers, gaode_shifted_m, segment_table


def plot_context(
    ax,
    roads_m: gpd.GeoDataFrame,
    extent: tuple[float, float, float, float],
    *,
    linewidth: float = 0.8,
    alpha: float = 0.8,
) -> None:
    minx, maxx, miny, maxy = extent
    bbox = box(minx, miny, maxx, maxy)
    context = roads_m[roads_m.intersects(bbox)].copy()
    if not context.empty:
        context.plot(ax=ax, color="#d9d9d9", linewidth=linewidth, alpha=alpha, zorder=1)


def direction_display_offsets(gaode_shifted_m: gpd.GeoDataFrame, offset_m: float) -> dict[str, tuple[float, float]]:
    center = gaode_shifted_m.geometry.union_all().centroid
    offsets = {}
    for chain in CHAIN_ORDER:
        chain_center = gaode_shifted_m.loc[
            gaode_shifted_m["chain_direction"].eq(chain), "geometry"
        ].union_all().centroid
        dx = chain_center.x - center.x
        dy = chain_center.y - center.y
        norm = float(np.hypot(dx, dy))
        if norm < 1e-6:
            dx, dy, norm = (1.0 if chain == "eastbound_chain" else -1.0), 0.0, 1.0
        offsets[chain] = (offset_m * dx / norm, offset_m * dy / norm)
    return offsets


def apply_direction_display_offsets(
    gdf: gpd.GeoDataFrame,
    offsets: dict[str, tuple[float, float]],
) -> gpd.GeoDataFrame:
    out = gdf.copy()
    geometry_col = out.geometry.name
    out[geometry_col] = out.apply(
        lambda row: translate(
            row[geometry_col],
            xoff=offsets[row["chain_direction"]][0],
            yoff=offsets[row["chain_direction"]][1],
        ),
        axis=1,
    )
    return gpd.GeoDataFrame(out, geometry=geometry_col, crs=gdf.crs)


def draw_speed_layer(ax, layer: gpd.GeoDataFrame, cmap, norm, *, linewidth_scale: float = 1.0) -> None:
    geometry_col = layer.geometry.name
    for _, row in layer.iterrows():
        parts = explode_line_parts(row[geometry_col])
        if not parts:
            continue
        speed = row["mean_speed_kmh"]
        has_speed = pd.notna(speed)
        color = cmap(norm(float(speed))) if has_speed else "#bdbdbd"
        alpha = 0.96 if has_speed else 0.65
        linewidth = (3.2 if has_speed else 2.0) * linewidth_scale
        linestyle = "-" if has_speed else (0, (2, 2))
        for geom in parts:
            x, y = geom.xy
            ax.plot(x, y, color="#ffffff", linewidth=linewidth + 1.1, alpha=0.85, zorder=2)
            ax.plot(
                x,
                y,
                color=color,
                linewidth=linewidth,
                linestyle=linestyle,
                solid_capstyle="round",
                alpha=alpha,
                zorder=3,
            )


def add_direction_labels(ax, gaode_shifted_m: gpd.GeoDataFrame) -> None:
    for chain, label, dy in [
        ("eastbound_chain", "SW -> NE", 18),
        ("westbound_chain", "NE -> SW", -22),
    ]:
        line = gaode_shifted_m.loc[gaode_shifted_m["chain_direction"].eq(chain), "geometry"].union_all()
        point = line.interpolate(0.5, normalized=True)
        ax.text(
            point.x,
            point.y + dy,
            label,
            fontsize=7.5,
            ha="center",
            va="center",
            color="#222222",
            bbox={"facecolor": "white", "edgecolor": "none", "alpha": 0.7, "pad": 1.0},
            zorder=5,
        )


def plot_speed_maps(
    plot_layers: gpd.GeoDataFrame,
    gaode_shifted_m: gpd.GeoDataFrame,
    roads_m: gpd.GeoDataFrame,
    output_path: Path,
    *,
    zoomed: bool = False,
) -> None:
    cmap = mpl.colormaps["RdYlGn"]
    norm = mpl.colors.Normalize(vmin=SPEED_VMIN, vmax=SPEED_VMAX)
    display_offsets = direction_display_offsets(gaode_shifted_m, DIRECTION_DISPLAY_OFFSET_M)
    plot_layers_display = apply_direction_display_offsets(plot_layers, display_offsets)
    gaode_shifted_display = apply_direction_display_offsets(gaode_shifted_m, display_offsets)

    bounds = gaode_shifted_display.total_bounds
    minx, miny, maxx, maxy = bounds
    if zoomed:
        padx = max(35.0, (maxx - minx) * 0.06)
        pady = max(35.0, (maxy - miny) * 0.06)
        context_linewidth = 1.0
        context_alpha = 0.55
        speed_line_scale = 1.15
        figsize = (12.6, 9.6)
        title_suffix = " (zoomed)"
    else:
        padx = max(120.0, (maxx - minx) * 0.18)
        pady = max(100.0, (maxy - miny) * 0.18)
        context_linewidth = 0.8
        context_alpha = 0.8
        speed_line_scale = 1.0
        figsize = (11.8, 8.8)
        title_suffix = ""
    extent = (minx - padx, maxx + padx, miny - pady, maxy + pady)

    fig, axes = plt.subplots(len(SOURCE_ORDER), len(WINDOW_ORDER), figsize=figsize)
    for row, source in enumerate(SOURCE_ORDER):
        for col, window in enumerate(WINDOW_ORDER):
            ax = axes[row, col]
            plot_context(
                ax,
                roads_m,
                extent,
                linewidth=context_linewidth,
                alpha=context_alpha,
            )
            sub = plot_layers_display[
                plot_layers_display["source_period"].eq(source)
                & plot_layers_display["window"].eq(window)
            ].copy()
            draw_speed_layer(ax, sub, cmap, norm, linewidth_scale=speed_line_scale)
            if row == 0:
                ax.set_title(WINDOWS[window]["label"], fontsize=10.5)
            if col == 0:
                ax.text(
                    -0.03,
                    0.5,
                    SOURCE_TITLES[source].replace("\n", "\n"),
                    transform=ax.transAxes,
                    rotation=90,
                    ha="right",
                    va="center",
                    fontsize=9.5,
                )
            if row == 0 and col == 0:
                add_direction_labels(ax, gaode_shifted_display)
            ax.set_xlim(extent[0], extent[1])
            ax.set_ylim(extent[2], extent[3])
            ax.set_aspect("equal", adjustable="box")
            ax.set_axis_off()

    sm = mpl.cm.ScalarMappable(norm=norm, cmap=cmap)
    sm.set_array([])
    cbar = fig.colorbar(sm, ax=axes.ravel().tolist(), orientation="horizontal", fraction=0.035, pad=0.03)
    cbar.set_label("Average speed (km/h)")
    cbar.set_ticks([10, 20, 30, 40, 50, 60])
    fig.suptitle(
        f"Liuxian Avenue key-window average speed maps on shifted Baidu-Gaode overlap{title_suffix}",
        x=0.01,
        ha="left",
    )
    fig.text(
        0.01,
        0.012,
        "Notes: Gaode geometry is shifted using the estimated Gaode-to-Baidu translation. "
        "Baidu geometries are clipped to the shifted target overlap for display; speeds are raw roadseg averages. "
        "Directions are offset outward by 8 m for visualization only. "
        "Grey dashed lines indicate selected Baidu segments without speed observations in the 2025 week.",
        fontsize=7.5,
        color="#444444",
    )
    if zoomed:
        fig.subplots_adjust(left=0.11, right=0.98, top=0.88, bottom=0.14, wspace=0.015, hspace=0.08)
    else:
        fig.subplots_adjust(left=0.11, right=0.98, top=0.88, bottom=0.14, wspace=0.03, hspace=0.12)
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    args = parse_args()
    configure_plot()
    dirs = ensure_output_dirs(args.output_dir)
    roads = gpd.read_file(args.baidu_roads)
    metric_crs = roads.estimate_utm_crs()
    roads_m = roads.to_crs(metric_crs)

    plot_layers, gaode_shifted_m, segment_table = build_map_layers(
        args.gaode_dir,
        roads,
        metric_crs,
        args.output_dir,
        args.baidu_speed,
    )
    plot_layers.drop(columns=plot_layers.geometry.name).to_csv(
        dirs["data"] / "shifted_overlap_segment_window_speeds_for_maps.csv",
        index=False,
        encoding="utf-8-sig",
    )
    segment_table.to_csv(
        dirs["metrics"] / "shifted_overlap_segment_window_speed_map_source_table.csv",
        index=False,
        encoding="utf-8-sig",
    )
    plot_speed_maps(
        plot_layers,
        gaode_shifted_m,
        roads_m,
        dirs["figures"] / "shifted_overlap_key_window_speed_maps.png",
    )
    plot_speed_maps(
        plot_layers,
        gaode_shifted_m,
        roads_m,
        dirs["figures"] / "shifted_overlap_key_window_speed_maps_zoomed.png",
        zoomed=True,
    )
    with (dirs["metrics"] / "shifted_overlap_key_window_speed_maps_metadata.json").open(
        "w", encoding="utf-8"
    ) as fh:
        json.dump(
            {
                "windows": WINDOWS,
                "source_order": SOURCE_ORDER,
                "speed_color_scale_kmh": {"vmin": SPEED_VMIN, "vmax": SPEED_VMAX},
                "baidu_clip_buffer_m": BAIDU_CLIP_BUFFER_M,
                "direction_display_offset_m": DIRECTION_DISPLAY_OFFSET_M,
                "gaode_segment_sample": "all observed segments within each source period; post-only endpoints are included in gaode_post map cells and absent from gaode_pre map cells",
                "shift_diagnostic": str(SHIFT_DIAGNOSTIC),
                "figure": str(dirs["figures"] / "shifted_overlap_key_window_speed_maps.png"),
                "zoomed_figure": str(dirs["figures"] / "shifted_overlap_key_window_speed_maps_zoomed.png"),
            },
            fh,
            ensure_ascii=False,
            indent=2,
        )


if __name__ == "__main__":
    main()
