import argparse
import sys
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def parse_args():
    parser = argparse.ArgumentParser(description="Export final match review shapefiles and figures.")
    parser.add_argument("--version-id", required=True, help="Version identifier under outputs/.")
    parser.add_argument("--output-dir", default="outputs", help="Base output directory.")
    return parser.parse_args()


def shorten_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    rename_map = {
        "raw_edge_id": "raw_id",
        "raw_seg_idx": "seg_idx",
        "roadseg_id": "roadseg_id",
        "roadname": "roadname",
        "manual_group_id": "grp_id",
        "manual_exact_match": "manual_ex",
        "matched_final": "matched",
        "match_method_final": "match_mth",
        "cline_id_final": "cline_id",
        "dist_mean_final": "dist_mean",
        "angle_diff_final": "ang_diff",
        "need_split": "need_split",
        "split_piece_count": "split_cnt",
        "build_source": "build_src",
        "covered_final": "covered",
        "matched_split_count": "hit_cnt",
    }
    used = set()
    cols = {}
    for col in gdf.columns:
        if col == "geometry":
            continue
        candidate = rename_map.get(col, col[:10])[:10]
        base = candidate
        idx = 1
        while candidate in used:
            suffix = str(idx)
            candidate = f"{base[:10-len(suffix)]}{suffix}"
            idx += 1
        used.add(candidate)
        cols[col] = candidate
    return gdf.rename(columns=cols)


def export_shp(gdf: gpd.GeoDataFrame, path: Path):
    out = gdf.copy()
    geom_name = out.geometry.name
    drop_cols = []
    for col in out.columns:
        if col == geom_name:
            continue
        if hasattr(out[col], "geom_type"):
            drop_cols.append(col)
    if drop_cols:
        out = out.drop(columns=drop_cols)
    out = shorten_columns(out)
    out.to_file(path)


def plot_lines(path: Path, layers, title: str, figsize=(10, 10), dpi=240):
    fig, ax = plt.subplots(figsize=figsize, dpi=dpi)
    for gdf, color, linewidth, alpha, label, zorder in layers:
        if gdf is not None and len(gdf):
            gdf.plot(ax=ax, color=color, linewidth=linewidth, alpha=alpha, label=label, zorder=zorder, rasterized=True)
    ax.set_title(title)
    ax.set_axis_off()
    handles, labels = ax.get_legend_handles_labels()
    if handles:
        seen = set()
        hh, ll = [], []
        for h, l in zip(handles, labels):
            if l not in seen:
                hh.append(h)
                ll.append(l)
                seen.add(l)
        ax.legend(hh, ll, loc="lower left", frameon=True)
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def plot_status_map(path: Path, status_gdf: gpd.GeoDataFrame, title: str, note: str | None = None, figsize=(10, 10), dpi=240):
    fig, ax = plt.subplots(figsize=figsize, dpi=dpi)
    style_order = [
        ("both_AB_BA", "#1f77b4", 0.65, 0.92, "both AB & BA matched", 3),
        ("AB_only", "#2ca25f", 0.82, 0.96, "AB only matched", 4),
        ("BA_only", "#756bb1", 0.82, 0.96, "BA only matched", 4),
        ("none", "#e6550d", 0.98, 0.98, "neither AB nor BA matched", 5),
    ]
    for status, color, linewidth, alpha, label, zorder in style_order:
        sub = status_gdf.loc[status_gdf["status"] == status].copy()
        if len(sub):
            sub.plot(ax=ax, color=color, linewidth=linewidth, alpha=alpha, label=label, zorder=zorder, rasterized=True)
    ax.set_title(title)
    ax.set_axis_off()
    handles, labels = ax.get_legend_handles_labels()
    if handles:
        seen = set()
        hh, ll = [], []
        for h, l in zip(handles, labels):
            if l not in seen:
                hh.append(h)
                ll.append(l)
                seen.add(l)
        ax.legend(hh, ll, loc="lower left", frameon=True)
    if note:
        ax.text(
            0.02,
            0.02,
            note,
            transform=ax.transAxes,
            ha="left",
            va="bottom",
            fontsize=9,
            bbox=dict(boxstyle="round,pad=0.35", facecolor="white", edgecolor="0.7", alpha=0.95),
        )
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def main():
    args = parse_args()
    version_root = Path(args.output_dir) / args.version_id
    data_dir = version_root / "data"
    gis_dir = version_root / "gis_review_final"
    fig_dir = version_root / "figures" / "final_match_review"
    gis_dir.mkdir(parents=True, exist_ok=True)
    fig_dir.mkdir(parents=True, exist_ok=True)

    raw_seg = gpd.read_parquet(data_dir / "raw_segment_master.parquet")
    match = pd.read_parquet(data_dir / "raw_to_centerline_match_master.parquet")
    centerline = gpd.read_parquet(data_dir / "centerline_master.parquet")
    centerline_dir = gpd.read_parquet(data_dir / "centerline_dir_master.parquet")

    split = raw_seg.merge(
        match[
            [
                "split_id",
                "matched_final",
                "match_method_final",
                "cline_id_final",
                "dist_mean_final",
                "angle_diff_final",
            ]
        ],
        on="split_id",
        how="left",
    )
    split["matched_final"] = pd.to_numeric(split["matched_final"], errors="coerce").fillna(0).astype(int)

    matched_split = split.loc[split["matched_final"] == 1].copy()
    unmatched_split = split.loc[split["matched_final"] == 0].copy()

    edge_status = (
        split.groupby("raw_edge_id", as_index=False)
        .agg(
            matched_final=("matched_final", "max"),
            split_count=("split_id", "count"),
            matched_split_count=("matched_final", "sum"),
        )
    )
    raw_edge_geom = raw_seg.sort_values("raw_edge_id").drop_duplicates(subset=["raw_edge_id"])[
        ["raw_edge_id", "roadseg_id", "roadname", "manual_group_id", "need_split", "geometry"]
    ].copy()
    raw_edge_status = raw_edge_geom.merge(edge_status, on="raw_edge_id", how="left")
    raw_edge_status["matched_final"] = pd.to_numeric(raw_edge_status["matched_final"], errors="coerce").fillna(0).astype(int)
    matched_raw_edges = raw_edge_status.loc[raw_edge_status["matched_final"] == 1].copy()
    unmatched_raw_edges = raw_edge_status.loc[raw_edge_status["matched_final"] == 0].copy()

    cl_hits = (
        match.loc[pd.to_numeric(match["matched_final"], errors="coerce").fillna(0).astype(int) == 1]
        .groupby("cline_id_final", as_index=False)
        .agg(matched_split_count=("split_id", "count"))
        .rename(columns={"cline_id_final": "cline_id"})
    )
    centerline_status = centerline.merge(cl_hits, on="cline_id", how="left")
    centerline_status["matched_split_count"] = pd.to_numeric(centerline_status["matched_split_count"], errors="coerce").fillna(0).astype(int)
    centerline_status["covered_final"] = (centerline_status["matched_split_count"] > 0).astype(int)
    matched_centerline = centerline_status.loc[centerline_status["covered_final"] == 1].copy()
    unmatched_centerline = centerline_status.loc[centerline_status["covered_final"] == 0].copy()

    dir_hits = (
        match.loc[pd.to_numeric(match["matched_final"], errors="coerce").fillna(0).astype(int) == 1]
        .groupby("skel_dir_final", as_index=False)
        .agg(n_hits=("split_id", "count"))
        .rename(columns={"skel_dir_final": "skel_dir"})
    )
    dir_cov = centerline_dir[["skel_dir", "cline_id", "dir"]].merge(dir_hits, on="skel_dir", how="left")
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
    centerline_4status_all = centerline.merge(undir_cov[["cline_id", "AB", "BA", "status"]], on="cline_id", how="left")
    centerline_4status_all["status"] = centerline_4status_all["status"].fillna("none")
    centerline_4status_baseline = centerline_4status_all.copy()
    if "keep_baseline" in centerline_4status_baseline.columns:
        centerline_4status_baseline = centerline_4status_baseline.loc[
            centerline_4status_baseline["keep_baseline"].fillna(False)
        ].copy()

    cl_status_flags = undir_cov[["cline_id", "AB", "BA", "status"]].copy()
    raw_split_status = match.merge(cl_status_flags, left_on="cline_id_final", right_on="cline_id", how="left")
    for col in ["AB", "BA"]:
        raw_split_status[col] = pd.to_numeric(raw_split_status[col], errors="coerce").fillna(0).astype(int)
    raw_edge_4status = (
        raw_split_status.groupby("raw_edge_id", as_index=False)
        .agg(AB=("AB", "max"), BA=("BA", "max"))
    )
    raw_edge_4status["status"] = raw_edge_4status.apply(status_row, axis=1)
    raw_edge_4status = raw_edge_geom.merge(raw_edge_4status, on="raw_edge_id", how="left")
    raw_edge_4status["AB"] = pd.to_numeric(raw_edge_4status["AB"], errors="coerce").fillna(0).astype(int)
    raw_edge_4status["BA"] = pd.to_numeric(raw_edge_4status["BA"], errors="coerce").fillna(0).astype(int)
    raw_edge_4status["status"] = raw_edge_4status["status"].fillna("none")

    export_shp(matched_split, gis_dir / "matched_raw_split_segments.shp")
    export_shp(unmatched_split, gis_dir / "unmatched_raw_split_segments.shp")
    export_shp(matched_raw_edges, gis_dir / "matched_raw_edges.shp")
    export_shp(unmatched_raw_edges, gis_dir / "unmatched_raw_edges.shp")
    export_shp(matched_centerline, gis_dir / "matched_centerline_segments.shp")
    export_shp(unmatched_centerline, gis_dir / "unmatched_centerline_segments.shp")
    export_shp(centerline_4status_baseline, gis_dir / "centerline_coverage_4status_baseline.shp")
    export_shp(centerline_4status_all, gis_dir / "centerline_coverage_4status_all.shp")
    export_shp(raw_edge_4status, gis_dir / "raw_edge_coverage_4status.shp")

    plot_lines(
        fig_dir / "map_final_match_split_segments.png",
        [
            (matched_split, "#2b6cb0", 0.35, 0.85, "matched split", 2),
            (unmatched_split, "#d94841", 0.75, 0.95, "unmatched split", 3),
        ],
        "Final Match: Raw Split Segments",
    )
    plot_lines(
        fig_dir / "map_final_match_raw_edges.png",
        [
            (matched_raw_edges, "#1b9e77", 0.35, 0.70, "matched raw edge", 2),
            (unmatched_raw_edges, "#d95f02", 0.95, 0.95, "unmatched raw edge", 3),
        ],
        "Final Match: Raw Edge Status",
    )
    plot_status_map(
        fig_dir / "map_final_match_centerline_coverage.png",
        centerline_4status_baseline,
        "Final Match: Baseline Centerline Coverage by Direction",
        note="keep_baseline = valid LineString and not short (length >= 50 m)",
    )
    plot_status_map(
        fig_dir / "map_final_match_centerline_coverage_all.png",
        centerline_4status_all,
        "Final Match: All Centerline Coverage by Direction",
    )
    plot_status_map(
        fig_dir / "map_final_match_raw_edges_4status.png",
        raw_edge_4status,
        "Final Match: Raw Edge Status by Covered Centerline Direction",
    )

    summary = pd.DataFrame(
        [
            {
                "matched_split_segments": int(len(matched_split)),
                "unmatched_split_segments": int(len(unmatched_split)),
                "matched_raw_edges": int(len(matched_raw_edges)),
                "unmatched_raw_edges": int(len(unmatched_raw_edges)),
                "matched_centerline_segments": int(len(matched_centerline)),
                "unmatched_centerline_segments": int(len(unmatched_centerline)),
            }
        ]
    )
    summary.to_csv(gis_dir / "final_match_review_summary.csv", index=False)

    length_rows = []
    for scope_name, gdf in [("centerline_baseline", centerline_4status_baseline), ("centerline_all", centerline_4status_all), ("raw_edge", raw_edge_4status)]:
        length_m = gdf.geometry.length.astype(float)
        total_count = len(gdf)
        total_length = float(length_m.sum())
        for status in ["both_AB_BA", "AB_only", "BA_only", "none"]:
            sub = gdf.loc[gdf["status"] == status].copy()
            sub_length = float(sub.geometry.length.astype(float).sum())
            length_rows.append(
                {
                    "scope": scope_name,
                    "status": status,
                    "count": int(len(sub)),
                    "count_share": (len(sub) / total_count) if total_count else np.nan,
                    "length_m": sub_length,
                    "length_share": (sub_length / total_length) if total_length else np.nan,
                }
            )
    pd.DataFrame(length_rows).to_csv(gis_dir / "final_match_review_length_shares_4status.csv", index=False)
    print(f"[final-match-review] saved GIS layers to {gis_dir}")
    print(f"[final-match-review] saved figures to {fig_dir}")


if __name__ == "__main__":
    main()
