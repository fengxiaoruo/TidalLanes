"""
Plot match diagnostics for a versioned run.
"""

import argparse
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from shapely.ops import unary_union


ROOT = Path(__file__).resolve().parents[2]
RAW_PATH = ROOT / "raw_data" / "gis" / "roads_baidu" / "beijing_roads.shp"
TARGET_EPSG = 3857
BUF_WIDTH = 50.0
RADIUS_KM = 15.0
RADIUS_M = RADIUS_KM * 1000.0
TIANANMEN_LONLAT = (116.397389, 39.908722)
MAP_BLUE = "#1f77b4"


def parse_args():
    parser = argparse.ArgumentParser(description="Plot match diagnostics")
    parser.add_argument("--version-id", required=True, help="Version identifier under outputs/.")
    parser.add_argument("--output-dir", default="outputs", help="Base output directory.")
    return parser.parse_args()


def ensure_3857(gdf):
    if gdf.crs is None:
        raise ValueError("GeoDataFrame has no CRS")
    return gdf.to_crs(TARGET_EPSG) if gdf.crs.to_epsg() != TARGET_EPSG else gdf


def load_inputs(version_root: Path):
    data_dir = version_root / "data"
    raw = ensure_3857(gpd.read_file(RAW_PATH))
    raw = raw.reset_index(drop=True)
    if "raw_edge_id" not in raw.columns:
        raw["raw_edge_id"] = raw.index.astype(int)
    centerline = ensure_3857(gpd.read_parquet(data_dir / "centerline_master.parquet"))
    cl_dir = ensure_3857(gpd.read_parquet(data_dir / "centerline_dir_master.parquet"))
    raw_split = ensure_3857(gpd.read_parquet(data_dir / "raw_split_centerline.parquet"))
    raw_split = harmonize_raw_split_columns(raw_split)
    return raw, centerline, cl_dir, raw_split


def harmonize_raw_split_columns(raw_split: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    out = raw_split.copy()
    rename_if_present = {
        "cline_id_y": "cline_id",
        "dir_y": "dir",
    }
    for src, dst in rename_if_present.items():
        if src in out.columns and dst not in out.columns:
            out = out.rename(columns={src: dst})

    if "cline_id" not in out.columns and "cline_id_x" in out.columns:
        out["cline_id"] = out["cline_id_x"]
    if "dir" not in out.columns and "dir_x" in out.columns:
        out["dir"] = out["dir_x"]

    for col in ["cline_id", "skel_dir", "matched", "split_id"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce") if col != "dir" else out[col]
    if "dir" in out.columns:
        out["dir"] = out["dir"].astype("string")
    return out


def roi_gdf(crs):
    center = gpd.GeoSeries.from_xy([TIANANMEN_LONLAT[0]], [TIANANMEN_LONLAT[1]], crs="EPSG:4326").to_crs(crs).iloc[0]
    return gpd.GeoDataFrame(geometry=[center.buffer(RADIUS_M)], crs=crs)


def save_simple_map(path: Path, gdf_lines=None, gdf_polys=None, title="", color=MAP_BLUE, draw_nodes=False):
    fig, ax = plt.subplots(1, 1, figsize=(8, 8), dpi=250)
    if gdf_polys is not None and len(gdf_polys):
        gdf_polys.plot(ax=ax, color=color, alpha=0.18, linewidth=0)
    if gdf_lines is not None and len(gdf_lines):
        gdf_lines.plot(ax=ax, color=color, linewidth=0.4, alpha=0.85, rasterized=True)
        if draw_nodes:
            xs, ys = [], []
            for geom in gdf_lines.geometry.values:
                if geom is None or geom.is_empty:
                    continue
                geoms = [geom] if geom.geom_type == "LineString" else list(getattr(geom, "geoms", []))
                for line in geoms:
                    for x, y in line.coords:
                        xs.append(float(x))
                        ys.append(float(y))
            if xs:
                ax.scatter(xs, ys, s=4, facecolors="white", edgecolors=color, linewidths=0.5, alpha=0.9)
    ax.set_title(title)
    ax.set_axis_off()
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def plot_cdf(path: Path, raw, raw_split, centerline, cl_dir):
    fig, ax = plt.subplots(figsize=(7, 5), dpi=200)
    datasets = [
        ("raw", raw.geometry.length.astype(float), "#1f77b4"),
        ("raw_split", raw_split.geometry.length.astype(float), "#ff7f0e"),
        ("undirected_centerline", centerline.geometry.length.astype(float), "#2ca02c"),
        ("directed_centerline", cl_dir.geometry.length.astype(float), "#9467bd"),
    ]
    for label, vals, color in datasets:
        x = np.sort(vals.values)
        y = np.arange(1, len(x) + 1) / len(x)
        ax.plot(x, y, label=label, color=color, linewidth=1.5)
    ax.set_xlabel("Segment length (m)")
    ax.set_ylabel("CDF")
    ax.set_title("Length CDF across four network layers")
    ax.grid(True, alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def plot_split_match_map(path: Path, raw_split, roi):
    matched = raw_split[pd.to_numeric(raw_split["matched"], errors="coerce").fillna(0).astype(int) == 1].copy()
    unmatched = raw_split[pd.to_numeric(raw_split["matched"], errors="coerce").fillna(0).astype(int) == 0].copy()
    matched = gpd.clip(matched, roi)
    unmatched = gpd.clip(unmatched, roi)
    fig, ax = plt.subplots(figsize=(8, 8), dpi=250)
    if len(matched):
        matched.plot(ax=ax, color="#3182bd", linewidth=0.5, alpha=0.8, label="matched", rasterized=True)
    if len(unmatched):
        unmatched.plot(ax=ax, color="#de2d26", linewidth=0.7, alpha=0.9, label="unmatched", rasterized=True)
    ax.set_title("Raw split matched vs unmatched")
    ax.legend()
    ax.set_axis_off()
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def plot_split_match_map_full_extent(path: Path, raw_split):
    matched = raw_split[pd.to_numeric(raw_split["matched"], errors="coerce").fillna(0).astype(int) == 1].copy()
    unmatched = raw_split[pd.to_numeric(raw_split["matched"], errors="coerce").fillna(0).astype(int) == 0].copy()
    fig, ax = plt.subplots(figsize=(9, 9), dpi=250)
    if len(matched):
        matched.plot(ax=ax, color="#1f77b4", linewidth=0.45, alpha=0.90, label="matched", zorder=2, rasterized=True)
    if len(unmatched):
        unmatched.plot(ax=ax, color="#ff7f0e", linewidth=1.1, alpha=0.95, label="unmatched", zorder=3, rasterized=True)
    ax.set_title("Raw split -> directed centerline matching\n(matched vs unmatched)")
    ax.set_axis_off()
    handles, labels = ax.get_legend_handles_labels()
    seen, h2, l2 = set(), [], []
    for h, l in zip(handles, labels):
        if l not in seen:
            h2.append(h)
            l2.append(l)
            seen.add(l)
    if h2:
        ax.legend(h2, l2, loc="lower left", frameon=True)
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def plot_n_splits_hist(path: Path, raw_split):
    matched = raw_split[pd.to_numeric(raw_split["matched"], errors="coerce").fillna(0).astype(int) == 1].copy()
    match_counts = (
        matched.groupby(["skel_dir", "cline_id", "dir"], as_index=False)
        .agg(n_splits=("split_id", "count"))
    )
    fig, axes = plt.subplots(1, 2, figsize=(12, 4), dpi=200)
    axes[0].hist(match_counts["n_splits"], bins=min(50, int(match_counts["n_splits"].max())), color="#3182bd", alpha=0.75, edgecolor="black")
    axes[0].set_title("Distribution of n_splits per centerline")
    axes[0].set_xlabel("n_splits")
    axes[0].set_ylabel("Frequency")
    axes[0].grid(True, alpha=0.3)
    clipped = match_counts["n_splits"].clip(upper=20)
    axes[1].hist(clipped, bins=20, color="#fc8d59", alpha=0.75, edgecolor="black")
    axes[1].set_title("Distribution of n_splits (capped at 20)")
    axes[1].set_xlabel("n_splits")
    axes[1].set_ylabel("Frequency")
    axes[1].grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def plot_match_quality(path: Path, raw_split, cl_dir):
    splits = raw_split.copy()
    if "seg_len_m" not in splits.columns:
        splits["seg_len_m"] = splits.geometry.length.astype(float)
    matched = splits[pd.to_numeric(splits["matched"], errors="coerce").fillna(0).astype(int) == 1].copy()
    match_counts = (
        matched.groupby(["skel_dir", "cline_id", "dir"], as_index=False)
        .agg(matched_len_sum=("seg_len_m", "sum"), n_splits=("split_id", "count"))
    )
    cl_use = cl_dir.copy()
    cl_use["cl_len_m"] = cl_use.geometry.length.astype(float)
    quality = cl_use.merge(match_counts, on=["skel_dir", "cline_id", "dir"], how="left")
    quality = quality[quality["matched_len_sum"].notna()].copy()
    quality["coverage_ratio"] = quality["matched_len_sum"] / quality["cl_len_m"]
    quality["diff_pct"] = (quality["matched_len_sum"] - quality["cl_len_m"]) / quality["cl_len_m"] * 100.0

    fig, axes = plt.subplots(2, 2, figsize=(14, 10), dpi=200)
    axes[0, 0].hist(quality["coverage_ratio"], bins=50, color="#3182bd", alpha=0.75, edgecolor="black")
    axes[0, 0].axvline(1.0, color="red", linestyle="--", linewidth=1.5)
    axes[0, 0].set_title("Coverage ratio")
    axes[0, 1].hist(quality["diff_pct"], bins=50, color="#9ecae1", alpha=0.75, edgecolor="black")
    axes[0, 1].axvline(0.0, color="red", linestyle="--", linewidth=1.5)
    axes[0, 1].set_title("Difference percent")
    axes[1, 0].scatter(quality["cl_len_m"], quality["matched_len_sum"], s=8, alpha=0.3, color="#08519c")
    lim = max(float(quality["cl_len_m"].max()), float(quality["matched_len_sum"].max()))
    axes[1, 0].plot([0, lim], [0, lim], color="red", linestyle="--")
    axes[1, 0].set_xlabel("Centerline length (m)")
    axes[1, 0].set_ylabel("Matched raw length (m)")
    axes[1, 0].set_title("Length comparison")
    axes[1, 1].scatter(quality["n_splits"], quality["coverage_ratio"], s=8, alpha=0.3, color="#238b45")
    axes[1, 1].set_xlabel("n_splits")
    axes[1, 1].set_ylabel("Coverage ratio")
    axes[1, 1].set_title("Coverage vs n_splits")
    for ax in axes.ravel():
        ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def plot_undirected_coverage_4classes(path: Path, raw_split, centerline, cl_dir):
    matched = raw_split[pd.to_numeric(raw_split["matched"], errors="coerce").fillna(0).astype(int) == 1].copy()
    hits = (
        matched["skel_dir"]
        .dropna()
        .value_counts()
        .rename("n_hits")
        .reset_index()
        .rename(columns={"index": "skel_dir"})
    )

    dir_cov = cl_dir[["skel_dir", "cline_id", "dir"]].merge(hits, on="skel_dir", how="left")
    dir_cov["n_hits"] = pd.to_numeric(dir_cov["n_hits"], errors="coerce").fillna(0).astype(int)
    dir_cov["covered"] = (dir_cov["n_hits"] > 0).astype(int)

    undir_cov = dir_cov.groupby(["cline_id", "dir"])["covered"].max().unstack("dir", fill_value=0).reset_index()
    for col in ["AB", "BA"]:
        if col not in undir_cov.columns:
            undir_cov[col] = 0

    def status_row(row):
        ab = int(row["AB"])
        ba = int(row["BA"])
        if ab == 1 and ba == 1:
            return "both_AB_BA"
        if ab == 1 and ba == 0:
            return "AB_only"
        if ab == 0 and ba == 1:
            return "BA_only"
        return "none"

    undir_cov["status"] = undir_cov.apply(status_row, axis=1)

    gdf_undir = centerline[["cline_id", "geometry"]].copy()
    gdf_undir = gdf_undir.merge(undir_cov[["cline_id", "AB", "BA", "status"]], on="cline_id", how="left")
    gdf_undir["status"] = gdf_undir["status"].fillna("none")

    fig, ax = plt.subplots(figsize=(9, 9), dpi=250)
    style_order = [
        ("both_AB_BA", "#1f77b4", 0.70, "both AB & BA"),
        ("AB_only", "#2ca02c", 0.80, "AB only"),
        ("BA_only", "#9467bd", 0.80, "BA only"),
        ("none", "#ff7f0e", 1.00, "none"),
    ]
    for status, color, linewidth, label in style_order:
        sub = gdf_undir[gdf_undir["status"] == status]
        if len(sub):
            sub.plot(ax=ax, color=color, linewidth=linewidth, alpha=0.90, label=label, rasterized=True)
    ax.set_title("Undirected centerlines coverage status\n(based on AB/BA being hit by matched raw split)")
    ax.set_axis_off()
    handles, labels = ax.get_legend_handles_labels()
    if handles:
        ax.legend(handles, labels, loc="lower left", frameon=True)
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def run(version_id: str, output_dir: str):
    version_root = Path(output_dir) / version_id
    figures_dir = version_root / "figures"
    figures_dir.mkdir(parents=True, exist_ok=True)
    raw, centerline, cl_dir, raw_split = load_inputs(version_root)
    roi = roi_gdf(raw.crs)

    save_simple_map(figures_dir / "map_raw_roads_15km.png", gdf_lines=gpd.clip(raw[["raw_edge_id", "geometry"]], roi), title="RAW Roads (15km)", draw_nodes=True)
    road_surface = unary_union(raw.geometry.buffer(BUF_WIDTH))
    road_surface_gdf = gpd.GeoDataFrame(geometry=[road_surface], crs=raw.crs)
    save_simple_map(figures_dir / "map_buffer_surface_15km.png", gdf_polys=gpd.clip(road_surface_gdf, roi), title=f"Buffered road surface ({int(BUF_WIDTH)}m)")
    save_simple_map(figures_dir / "map_centerline_15km.png", gdf_lines=gpd.clip(centerline[["cline_id", "geometry"]], roi), title="Centerline (15km)", draw_nodes=True)
    plot_cdf(figures_dir / "fig_length_cdf_4networks.png", raw, raw_split, centerline, cl_dir)
    plot_split_match_map_full_extent(figures_dir / "map_raw_split_matched_vs_unmatched.png", raw_split)
    plot_split_match_map(figures_dir / "map_raw_split_matched_vs_unmatched_15km.png", raw_split, roi)
    plot_undirected_coverage_4classes(figures_dir / "map_undirected_centerline_coverage_4classes.png", raw_split, centerline, cl_dir)
    plot_n_splits_hist(figures_dir / "hist_n_splits_distribution.png", raw_split)
    plot_match_quality(figures_dir / "match_quality_centerline.png", raw_split, cl_dir)
    print(f"[plot_match_diagnostics] saved figures to {figures_dir}")


def main():
    args = parse_args()
    run(args.version_id, args.output_dir)


if __name__ == "__main__":
    main()
