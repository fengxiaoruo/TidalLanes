"""
Audit Stage01/Stage02 split candidates.

Purpose:
- Pull the most suspicious over-split raw edges
- Pull long need_split-but-not-split raw edges
- Rank centerlines that absorb unusually many raw split segments
- Export local maps so stage01 vs stage02 failure modes can be reviewed quickly
"""

import argparse
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from shapely.geometry import Point


ROOT = Path(__file__).resolve().parents[2]
RAW_PATH = ROOT / "raw_data" / "gis" / "roads_baidu" / "beijing_roads.shp"
TARGET_EPSG = 3857
LOCAL_BUFFER_M = 250.0
TOP_N = 15


def parse_args():
    parser = argparse.ArgumentParser(description="Audit Stage01/Stage02 split candidates")
    parser.add_argument("--version-id", required=True, help="Version identifier under outputs/.")
    parser.add_argument("--output-dir", default="outputs", help="Base output directory.")
    parser.add_argument("--top-n", type=int, default=TOP_N, help="Number of top cases to export per category.")
    parser.add_argument("--buffer-m", type=float, default=LOCAL_BUFFER_M, help="Local review buffer around each case.")
    return parser.parse_args()


def ensure_3857(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if gdf.crs is None:
        raise ValueError("GeoDataFrame CRS is missing")
    return gdf.to_crs(TARGET_EPSG) if gdf.crs.to_epsg() != TARGET_EPSG else gdf


def load_inputs(version_root: Path):
    data_dir = version_root / "data"
    raw = ensure_3857(gpd.read_file(RAW_PATH))
    raw = raw.reset_index(drop=True)
    if "raw_edge_id" not in raw.columns:
        raw["raw_edge_id"] = raw.index.astype(int)
    raw_seg = ensure_3857(gpd.read_parquet(data_dir / "raw_segment_master.parquet"))
    match = pd.read_parquet(data_dir / "raw_to_centerline_match_master.parquet")
    centerline = ensure_3857(gpd.read_parquet(data_dir / "centerline_master.parquet"))
    cl_dir = ensure_3857(gpd.read_parquet(data_dir / "centerline_dir_master.parquet"))
    return raw, raw_seg, match, centerline, cl_dir


def build_centerline_nodes(centerline: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    rows = []
    for row in centerline.loc[centerline["keep_baseline"].fillna(False)].itertuples(index=False):
        geom = row.geometry
        if geom.is_empty or geom.geom_type != "LineString":
            continue
        coords = list(geom.coords)
        rows.append({"cline_id": int(row.cline_id), "node_role": "start", "node_key": coords[0], "geometry": Point(coords[0])})
        rows.append({"cline_id": int(row.cline_id), "node_role": "end", "node_key": coords[-1], "geometry": Point(coords[-1])})
    nodes = gpd.GeoDataFrame(rows, geometry="geometry", crs=centerline.crs)
    if nodes.empty:
        return nodes
    degree = nodes.groupby("node_key").size().rename("degree").reset_index()
    nodes = nodes.merge(degree, on="node_key", how="left")
    nodes["is_major_node"] = nodes["degree"].fillna(0).astype(int) != 2
    return nodes


def build_raw_edge_summary(raw_seg: gpd.GeoDataFrame, match: pd.DataFrame, raw: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    mm = match[["split_id", "matched_final"]].copy()
    mm["matched_final"] = pd.to_numeric(mm["matched_final"], errors="coerce").fillna(0).astype(int)
    seg = raw_seg.merge(mm, on="split_id", how="left")
    seg["matched_final"] = seg["matched_final"].fillna(0).astype(int)

    group_cols = [
        "raw_edge_id",
        "roadseg_id",
        "roadname",
        "roadtype",
        "need_split",
        "split_reason_endpoint_change",
        "split_reason_major_node",
        "major_node_cut_count_hint",
    ]
    present = [c for c in group_cols if c in seg.columns]
    edge = (
        seg.groupby(present, dropna=False)
        .agg(
            n_split=("split_id", "count"),
            matched_split_segments=("matched_final", "sum"),
            total_length_m=("length_m", "sum"),
            mean_segment_length_m=("length_m", "mean"),
            min_segment_length_m=("length_m", "min"),
            max_segment_length_m=("length_m", "max"),
        )
        .reset_index()
    )
    edge["unmatched_split_segments"] = edge["n_split"] - edge["matched_split_segments"]
    edge["split_density_per_km"] = np.where(edge["total_length_m"] > 0, edge["n_split"] / (edge["total_length_m"] / 1000.0), np.nan)
    edge = edge.merge(raw[["raw_edge_id", "geometry"]], on="raw_edge_id", how="left")
    return gpd.GeoDataFrame(edge, geometry="geometry", crs=raw.crs)


def sort_with_existing_cols(df: pd.DataFrame, by: list[str], ascending: list[bool]) -> pd.DataFrame:
    use_cols = [col for col in by if col in df.columns]
    if not use_cols:
        return df
    use_asc = [ascending[i] for i, col in enumerate(by) if col in df.columns]
    return df.sort_values(use_cols, ascending=use_asc)


def select_existing_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    return df[[col for col in cols if col in df.columns]].copy()


def build_centerline_pressure(centerline: gpd.GeoDataFrame, match: pd.DataFrame) -> pd.DataFrame:
    matched = match.loc[pd.to_numeric(match["matched_final"], errors="coerce").fillna(0).astype(int) == 1].copy()
    matched["cline_id_final"] = pd.to_numeric(matched["cline_id_final"], errors="coerce").astype("Int64")
    counts = (
        matched.dropna(subset=["cline_id_final"])
        .groupby("cline_id_final")
        .agg(
            matched_split_segments=("split_id", "count"),
            unique_raw_edges=("raw_edge_id", "nunique"),
        )
        .reset_index()
        .rename(columns={"cline_id_final": "cline_id"})
    )
    out = centerline[["cline_id", "length_m", "keep_baseline", "is_short_centerline"]].merge(counts, on="cline_id", how="left")
    out["matched_split_segments"] = pd.to_numeric(out["matched_split_segments"], errors="coerce").fillna(0).astype(int)
    out["unique_raw_edges"] = pd.to_numeric(out["unique_raw_edges"], errors="coerce").fillna(0).astype(int)
    out["split_pressure_per_km"] = np.where(out["length_m"] > 0, out["matched_split_segments"] / (out["length_m"] / 1000.0), np.nan)
    return out.sort_values(["matched_split_segments", "split_pressure_per_km", "length_m"], ascending=[False, False, False])


def add_local_context(case_gdf: gpd.GeoDataFrame, raw_seg: gpd.GeoDataFrame, centerline: gpd.GeoDataFrame, nodes: gpd.GeoDataFrame, buffer_m: float):
    if case_gdf.empty:
        return case_gdf.copy()
    raw_sindex = raw_seg.sindex
    cl_sindex = centerline.sindex
    node_sindex = nodes.sindex if len(nodes) else None

    local_rows = []
    for row in case_gdf.itertuples(index=False):
        geom = row.geometry
        area = geom.buffer(buffer_m)
        raw_idx = list(raw_sindex.query(area))
        cl_idx = list(cl_sindex.query(area))
        node_idx = list(node_sindex.query(area)) if node_sindex is not None else []

        raw_local = raw_seg.iloc[raw_idx] if raw_idx else raw_seg.iloc[[]]
        cl_local = centerline.iloc[cl_idx] if cl_idx else centerline.iloc[[]]
        node_local = nodes.iloc[node_idx] if node_idx else nodes.iloc[[]]

        local_rows.append(
            {
                "raw_edge_id": int(row.raw_edge_id),
                "local_raw_segments": int(len(raw_local)),
                "local_centerlines": int(len(cl_local)),
                "local_short_centerlines": int(cl_local["is_short_centerline"].fillna(False).sum()) if len(cl_local) else 0,
                "local_nodes": int(len(node_local)),
                "local_major_nodes": int(node_local["is_major_node"].fillna(False).sum()) if len(node_local) else 0,
                "local_max_node_degree": int(node_local["degree"].max()) if len(node_local) else 0,
            }
        )
    return case_gdf.merge(pd.DataFrame(local_rows), on="raw_edge_id", how="left")


def save_case_map(path: Path, raw_edge_row, raw_seg: gpd.GeoDataFrame, centerline: gpd.GeoDataFrame, nodes: gpd.GeoDataFrame, buffer_m: float):
    area = raw_edge_row.geometry.buffer(buffer_m)
    raw_local = raw_seg.iloc[list(raw_seg.sindex.query(area))].copy()
    cl_local = centerline.iloc[list(centerline.sindex.query(area))].copy()
    node_local = nodes.iloc[list(nodes.sindex.query(area))].copy() if len(nodes) else nodes.copy()

    raw_local = raw_local[raw_local.intersects(area)]
    cl_local = cl_local[cl_local.intersects(area)]
    node_local = node_local[node_local.intersects(area)] if len(node_local) else node_local

    fig, ax = plt.subplots(figsize=(7, 7), dpi=220)
    if len(cl_local):
        cl_local.plot(ax=ax, color="#9ecae1", linewidth=0.9, alpha=0.85)
    if len(raw_local):
        raw_local.plot(ax=ax, color="#636363", linewidth=0.8, alpha=0.7)
        raw_local.loc[raw_local["raw_edge_id"] == raw_edge_row.raw_edge_id].plot(ax=ax, color="#d95f0e", linewidth=1.8, alpha=0.95)
    if len(node_local):
        major = node_local[node_local["is_major_node"].fillna(False)]
        minor = node_local[~node_local["is_major_node"].fillna(False)]
        if len(minor):
            minor.plot(ax=ax, color="#3182bd", markersize=8, alpha=0.6)
        if len(major):
            major.plot(ax=ax, color="#de2d26", markersize=16, alpha=0.85)
    gpd.GeoSeries([area.boundary], crs=raw_seg.crs).plot(ax=ax, color="#31a354", linewidth=1.0, alpha=0.9)
    ax.set_title(f"raw_edge_id={int(raw_edge_row.raw_edge_id)} n_split={int(raw_edge_row.n_split)}")
    ax.set_axis_off()
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def run(version_id: str, output_dir: str, top_n: int, buffer_m: float):
    version_root = Path(output_dir) / version_id
    audit_dir = version_root / "metrics" / "stage01_stage02_audit"
    figures_dir = version_root / "figures" / "stage01_stage02_audit"
    audit_dir.mkdir(parents=True, exist_ok=True)
    figures_dir.mkdir(parents=True, exist_ok=True)

    raw, raw_seg, match, centerline, _ = load_inputs(version_root)
    nodes = build_centerline_nodes(centerline)
    edge = build_raw_edge_summary(raw_seg, match, raw)
    pressure = build_centerline_pressure(centerline, match)

    oversplit = edge.loc[edge["n_split"] >= 20].sort_values(
        ["n_split", "split_density_per_km", "total_length_m"],
        ascending=[False, False, False],
    ).head(top_n)
    oversplit = add_local_context(oversplit, raw_seg, centerline, nodes, buffer_m)

    needsplit_unsplit = edge.loc[edge["need_split"].fillna(False) & (edge["n_split"] == 1)].copy()
    needsplit_unsplit = sort_with_existing_cols(
        needsplit_unsplit,
        ["major_node_cut_count_hint", "total_length_m"],
        [False, False],
    ).head(top_n)
    needsplit_unsplit = add_local_context(needsplit_unsplit, raw_seg, centerline, nodes, buffer_m)

    pressure_top = pressure.head(top_n).copy()

    oversplit.drop(columns="geometry", errors="ignore").to_csv(audit_dir / "top_oversplit_raw_edges.csv", index=False)
    needsplit_unsplit.drop(columns="geometry", errors="ignore").to_csv(audit_dir / "top_needsplit_not_split_raw_edges.csv", index=False)
    pressure_top.drop(columns="geometry", errors="ignore").to_csv(audit_dir / "top_centerline_fragment_pressure.csv", index=False)

    select_existing_cols(oversplit, ["raw_edge_id", "n_split", "split_density_per_km", "geometry"]).to_file(
        audit_dir / "top_oversplit_raw_edges.geojson",
        driver="GeoJSON",
    )
    select_existing_cols(needsplit_unsplit, ["raw_edge_id", "major_node_cut_count_hint", "total_length_m", "geometry"]).to_file(
        audit_dir / "top_needsplit_not_split_raw_edges.geojson",
        driver="GeoJSON",
    )
    centerline.merge(pressure_top[["cline_id", "matched_split_segments", "split_pressure_per_km"]], on="cline_id", how="inner").to_file(
        audit_dir / "top_centerline_fragment_pressure.geojson",
        driver="GeoJSON",
    )

    for row in oversplit.itertuples(index=False):
        save_case_map(figures_dir / f"oversplit_raw_edge_{int(row.raw_edge_id)}.png", row, raw_seg, centerline, nodes, buffer_m)
    for row in needsplit_unsplit.itertuples(index=False):
        save_case_map(figures_dir / f"needsplit_not_split_raw_edge_{int(row.raw_edge_id)}.png", row, raw_seg, centerline, nodes, buffer_m)

    node_summary = (
        nodes.groupby("degree")
        .size()
        .rename("n_nodes")
        .reset_index()
        .sort_values("degree")
    )
    node_summary.to_csv(audit_dir / "centerline_node_degree_summary.csv", index=False)

    print(f"[audit_stage01_stage02_split_candidates] saved audit tables to {audit_dir}")
    print(f"[audit_stage01_stage02_split_candidates] saved local maps to {figures_dir}")


def main():
    args = parse_args()
    run(args.version_id, args.output_dir, args.top_n, args.buffer_m)


if __name__ == "__main__":
    main()
