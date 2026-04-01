"""
Plot centerline asymmetry diagnostics for a versioned run.
"""

import argparse
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from shapely.geometry import Point


TIANANMEN_LONLAT = (116.397389, 39.908722)
RADIUS_M = 150_000


def parse_args():
    parser = argparse.ArgumentParser(description="Plot centerline asymmetry diagnostics")
    parser.add_argument("--version-id", required=True, help="Version identifier under outputs/.")
    parser.add_argument("--output-dir", default="outputs", help="Base output directory.")
    return parser.parse_args()


def load_inputs(version_root: Path):
    data_dir = version_root / "data"
    figures_dir = version_root / "figures"
    cl_dir = gpd.read_parquet(data_dir / "centerline_dir_master.parquet")
    asym = pd.read_parquet(data_dir / "centerline_asymmetry_table.parquet")
    tidal = pd.read_csv(data_dir / "centerline_tidal_lane_candidates.csv")
    return cl_dir, asym, tidal, figures_dir


def within_radius(gdf: gpd.GeoDataFrame, radius_m: float):
    center = gpd.GeoSeries([Point(TIANANMEN_LONLAT)], crs="EPSG:4326").to_crs(gdf.crs).iloc[0]
    midpts = gdf.geometry.interpolate(0.5, normalized=True)
    return gdf[midpts.distance(center) <= radius_m].copy()


def plot_overlay(path: Path, base_gdf: gpd.GeoDataFrame, hi_gdf: gpd.GeoDataFrame, title: str):
    fig, ax = plt.subplots(figsize=(12, 12), dpi=220)
    base_gdf.plot(ax=ax, color="#bdbdbd", linewidth=0.35, alpha=0.5, rasterized=True)
    ab = hi_gdf[hi_gdf["dir"] == "AB"].copy()
    ba = hi_gdf[hi_gdf["dir"] == "BA"].copy()
    if len(ab):
        ab.plot(ax=ax, color="#de2d26", linewidth=1.8, alpha=0.95, label="AB", rasterized=True)
    if len(ba):
        ba.plot(ax=ax, color="#2171b5", linewidth=1.8, alpha=0.95, label="BA", rasterized=True)
    ax.set_title(title)
    ax.set_axis_off()
    handles, labels = ax.get_legend_handles_labels()
    if labels:
        ax.legend()
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def run(version_id: str, output_dir: str):
    version_root = Path(output_dir) / version_id
    cl_dir, asym, tidal, figures_dir = load_inputs(version_root)
    figures_dir.mkdir(parents=True, exist_ok=True)

    cl_dir = within_radius(cl_dir, RADIUS_M)
    base = cl_dir[["cline_id", "dir", "geometry"]].copy()

    for peak in ["AM", "PM"]:
        sub = asym[asym["peak"] == peak].copy()
        for metric in ["unsym1", "unsym2", "unsym3"]:
            use = sub[sub[metric] == True].copy()
            if use.empty:
                continue
            use["dir"] = use["faster_dir"].astype("string")
            hi = base.merge(use[["cline_id", "dir"]], on=["cline_id", "dir"], how="inner")
            plot_overlay(
                figures_dir / f"map_asym_{peak}_{metric}_150km.png",
                base,
                hi,
                f"{peak} {metric} asymmetric segments (150km)",
            )

    if not tidal.empty:
        tidal["cline_id"] = pd.to_numeric(tidal["cl_key"], errors="coerce")
        tidal = tidal.dropna(subset=["cline_id"]).copy()
        tidal["cline_id"] = tidal["cline_id"].astype(int)
        tidal_long = pd.concat(
            [
                tidal[["cline_id"]].assign(dir=tidal["faster_am"].astype("string")),
                tidal[["cline_id"]].assign(dir=tidal["faster_pm"].astype("string")),
            ],
            ignore_index=True,
        ).drop_duplicates()
        hi_tidal = base.merge(tidal_long, on=["cline_id", "dir"], how="inner")
        plot_overlay(figures_dir / "map_tidal_candidates_150km.png", base, hi_tidal, "Tidal lane candidates (150km)")

    print(f"[plot_asymmetry_diagnostics] saved figures to {figures_dir}")


def main():
    args = parse_args()
    run(args.version_id, args.output_dir)


if __name__ == "__main__":
    main()
