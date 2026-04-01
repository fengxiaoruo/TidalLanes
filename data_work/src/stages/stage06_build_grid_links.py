"""
Stage 06: Build Grid Links

Purpose:
- Map centerlines to grid sequences
- Build grid-link parts and aggregated grid-link tables
- Preserve alternative travel-time definitions

Current source notebook:
- code/04_GirdlevelData.ipynb
"""

import argparse
import json
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import LineString
from shapely.ops import linemerge


def parse_args():
    parser = argparse.ArgumentParser(description="Stage 06: Build grid links")
    parser.add_argument("--config", default=None, help="Optional config file path.")
    parser.add_argument("--version-id", required=True, help="Version identifier for outputs.")
    parser.add_argument("--output-dir", default="outputs", help="Base output directory for versioned results.")
    parser.add_argument(
        "--grid-type",
        default="all",
        choices=["all", "square", "hex", "voronoi"],
        help="Grid system to process.",
    )
    return parser.parse_args()


def save_config_snapshot(version_root: Path, config_path: str | None, grid_type: str):
    payload = {
        "stage": "stage06_build_grid_links",
        "config_path": config_path,
        "grid_type": grid_type,
        "edge_travel_time_definition": "grid_centroid_distance_over_v_harm_min",
        "free_flow_speed_definition": "2200_0500_mean_speed_total_distance_over_total_time",
        "free_flow_travel_time_definition": "grid_centroid_distance_over_v_ff_harm_min",
        "speed_definition": "harmonic_speed_total_distance_over_total_time",
        "legacy_travel_time_definition": "tt_len_weighted_mean_min",
    }
    (version_root / "config_snapshot.stage06.json").write_text(
        json.dumps(payload, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )


def ensure_crs(gdf, target="EPSG:3857"):
    if gdf.crs is None:
        raise ValueError("GeoDataFrame has no CRS.")
    return gdf.to_crs(target) if str(gdf.crs) != str(target) else gdf


def normalize_grid_id_value(value):
    if pd.isna(value):
        return pd.NA
    if isinstance(value, (int, np.integer)):
        return str(int(value))
    if isinstance(value, (float, np.floating)):
        if not np.isfinite(value):
            return pd.NA
        return str(int(value)) if float(value).is_integer() else format(float(value), ".15g")
    text = str(value).strip()
    if text == "" or text.lower() in {"nan", "none", "<na>"}:
        return pd.NA
    try:
        num = float(text)
    except ValueError:
        return text
    if not np.isfinite(num):
        return pd.NA
    return str(int(num)) if num.is_integer() else format(num, ".15g")


def normalize_grid_id_series(series: pd.Series) -> pd.Series:
    return series.map(normalize_grid_id_value).astype("string")


def filter_by_flag(df, flag_col: str):
    if flag_col in df.columns:
        return df[df[flag_col].fillna(False)].copy()
    return df.copy()


def to_lines(geom):
    if geom is None or geom.is_empty:
        return []
    if geom.geom_type == "LineString":
        return [geom]
    if geom.geom_type == "MultiLineString":
        return [g for g in geom.geoms if g is not None and not g.is_empty]
    return []


def safe_linemerge(geom):
    if geom is None or geom.is_empty:
        return []
    if geom.geom_type == "LineString":
        return [geom]
    if geom.geom_type == "MultiLineString":
        try:
            mg = linemerge(geom)
            return [mg] if mg.geom_type == "LineString" else list(mg.geoms)
        except Exception:
            return list(geom.geoms)
    return []


def midpoint(ls: LineString):
    try:
        return ls.interpolate(0.5, normalized=True)
    except Exception:
        return ls.centroid


def harmonic_speed_kmh(total_len_m, sum_len_over_v_ms):
    if sum_len_over_v_ms <= 0:
        return np.nan
    return (total_len_m / sum_len_over_v_ms) * 3.6


def segment_to_grid_sequence(line_geom, grid_sindex, grid_sub):
    cand_idx = list(grid_sindex.intersection(line_geom.bounds))
    if not cand_idx:
        return []
    pieces = []
    for row in grid_sub.iloc[cand_idx].itertuples(index=False):
        if not line_geom.intersects(row.geometry):
            continue
        inter = line_geom.intersection(row.geometry)
        if inter.is_empty:
            continue
        for ls in to_lines(inter):
            L = float(ls.length)
            if L <= 0:
                continue
            s = float(line_geom.project(midpoint(ls)))
            gid = normalize_grid_id_value(row.grid_id)
            if pd.isna(gid):
                continue
            pieces.append((gid, L, s))
    if not pieces:
        return []
    pieces.sort(key=lambda x: x[2])
    seq = []
    for gid, L, _ in pieces:
        if (not seq) or (seq[-1][0] != gid):
            seq.append([gid, L])
        else:
            seq[-1][1] += L
    return [(gid, float(L)) for gid, L in seq]


def load_inputs(version_root: Path, grid_type: str):
    data_dir = version_root / "data"
    cl_dir = ensure_crs(gpd.read_parquet(data_dir / "centerline_dir_master.parquet"), "EPSG:3857")
    cl_speed = pd.read_parquet(data_dir / "centerline_speed_master.parquet")

    grid_map = {
        "square": data_dir / "grid_square_master.parquet",
        "hex": data_dir / "grid_hex_master.parquet",
        "voronoi": data_dir / "grid_voronoi_master.parquet",
    }
    grid = ensure_crs(gpd.read_parquet(grid_map[grid_type]), "EPSG:3857")
    grid["grid_id"] = normalize_grid_id_series(grid["grid_id"])
    grid = grid[grid["grid_id"].notna()].copy()
    cl_dir = filter_by_flag(cl_dir, "keep_baseline")
    return cl_dir, cl_speed, grid


def build_peak_centerline_speed(cl_dir, cl_speed):
    spd_wd = cl_speed[cl_speed["weekday_label"] == "weekday"].copy()

    def aggregate_speed(df, period, speed_col):
        if df.empty:
            return pd.DataFrame(columns=["skel_dir", "cline_id", "dir", "period", speed_col])
        out = (
            df.groupby(["skel_dir", "cline_id", "dir"], as_index=False)
            .agg(total_dist_m=("total_dist_m", "sum"), total_time_h=("total_time_h", "sum"))
        )
        out[speed_col] = np.where(
            out["total_time_h"] > 0,
            out["total_dist_m"] / out["total_time_h"],
            np.nan,
        )
        out["period"] = period
        return out.drop(columns=["total_dist_m", "total_time_h"])

    spd_peak = pd.concat(
        [
            aggregate_speed(spd_wd[spd_wd["is_am_peak"]].copy(), "AM", "cl_speed_kmh"),
            aggregate_speed(spd_wd[spd_wd["is_pm_peak"]].copy(), "PM", "cl_speed_kmh"),
        ],
        ignore_index=True,
    )
    spd_ff = aggregate_speed(cl_speed[cl_speed["is_freeflow_2205"]].copy(), "FF", "cl_speed_ff_kmh")
    spd_ff = spd_ff.drop(columns=["period"], errors="ignore")
    cl_peak = cl_dir.merge(spd_peak, on=["skel_dir", "cline_id", "dir"], how="inner")
    cl_peak = cl_peak.merge(spd_ff, on=["skel_dir", "cline_id", "dir"], how="left")
    return gpd.GeoDataFrame(cl_peak, geometry="geometry", crs=cl_dir.crs)


def build_links(cl_peak, grid):
    grid_sub = grid[["grid_id", "geometry"]].copy()
    sindex = grid_sub.sindex
    rec = []

    for row in cl_peak.itertuples(index=False):
        v_kmh = float(row.cl_speed_kmh) if row.cl_speed_kmh is not None else np.nan
        v_ff_kmh = float(row.cl_speed_ff_kmh) if getattr(row, "cl_speed_ff_kmh", None) is not None else np.nan
        if (not np.isfinite(v_kmh)) or (v_kmh <= 0):
            continue
        if (not np.isfinite(v_ff_kmh)) or (v_ff_kmh <= 0):
            v_ff_kmh = v_kmh
        v_ms = v_kmh / 3.6
        v_ff_ms = v_ff_kmh / 3.6

        for geom in safe_linemerge(row.geometry):
            if geom is None or geom.is_empty or geom.length <= 0:
                continue
            seq = segment_to_grid_sequence(geom, sindex, grid_sub)
            if len(seq) <= 1:
                continue
            for k in range(len(seq) - 1):
                go, len_m = seq[k]
                gd, _ = seq[k + 1]
                if go == gd:
                    continue
                rec.append(
                    {
                        "period": row.period,
                        "skel_dir": row.skel_dir,
                        "cline_id": row.cline_id,
                        "dir": row.dir,
                        "grid_o": go,
                        "grid_d": gd,
                        "len_m": float(len_m),
                        "tt_s": float(len_m / v_ms),
                        "tt_ff_s": float(len_m / v_ff_ms),
                        "v_kmh": v_kmh,
                        "v_ff_kmh": v_ff_kmh,
                    }
                )

    links_long = pd.DataFrame(rec)
    if links_long.empty:
        raise RuntimeError("No grid-to-grid links created.")
    links_long["grid_o"] = normalize_grid_id_series(links_long["grid_o"])
    links_long["grid_d"] = normalize_grid_id_series(links_long["grid_d"])
    links_long = links_long[links_long["grid_o"].notna() & links_long["grid_d"].notna()].copy()

    gcols = ["period", "grid_o", "grid_d"]
    tmp = links_long.copy()
    tmp["w_v"] = tmp["len_m"] * tmp["v_kmh"]
    tmp["w_v_ff"] = tmp["len_m"] * tmp["v_ff_kmh"]
    tmp["tt_len_w"] = tmp["tt_s"] * tmp["len_m"]
    tmp["tt_ff_len_w"] = tmp["tt_ff_s"] * tmp["len_m"]
    lenw = tmp.groupby(gcols, as_index=False).agg(sum_len_m=("len_m", "sum"), sum_wv=("w_v", "sum"))
    lenw["v_len_w_kmh"] = lenw["sum_wv"] / lenw["sum_len_m"]
    lenw_ff = tmp.groupby(gcols, as_index=False).agg(sum_len_m=("len_m", "sum"), sum_wv_ff=("w_v_ff", "sum"))
    lenw_ff["v_ff_len_w_kmh"] = lenw_ff["sum_wv_ff"] / lenw_ff["sum_len_m"]

    tmp["len_over_v"] = tmp["len_m"] / (tmp["v_kmh"] / 3.6)
    tmp["len_over_v_ff"] = tmp["len_m"] / (tmp["v_ff_kmh"] / 3.6)
    harm = tmp.groupby(gcols, as_index=False).agg(
        total_len_m=("len_m", "sum"),
        total_tt_s=("tt_s", "sum"),
        total_tt_ff_s=("tt_ff_s", "sum"),
        n_parts=("len_m", "size"),
        sum_len_over_v_ms=("len_over_v", "sum"),
        sum_len_over_v_ff_ms=("len_over_v_ff", "sum"),
        tt_len_w_sum=("tt_len_w", "sum"),
        tt_ff_len_w_sum=("tt_ff_len_w", "sum"),
        tt_mean_s=("tt_s", "mean"),
        tt_ff_mean_s=("tt_ff_s", "mean"),
    )
    agg = harm.merge(lenw[gcols + ["v_len_w_kmh"]], on=gcols, how="left")
    agg = agg.merge(lenw_ff[gcols + ["v_ff_len_w_kmh"]], on=gcols, how="left")
    agg["v_harm_kmh"] = agg.apply(lambda r: harmonic_speed_kmh(r.total_len_m, r.sum_len_over_v_ms), axis=1)
    agg["v_ff_harm_kmh"] = agg.apply(lambda r: harmonic_speed_kmh(r.total_len_m, r.sum_len_over_v_ff_ms), axis=1)
    agg["total_len_km"] = agg["total_len_m"] / 1000.0
    agg["total_tt_min"] = agg["total_tt_s"] / 60.0
    agg["total_tt_ff_min"] = agg["total_tt_ff_s"] / 60.0
    agg["tt_mean_part_min"] = agg["tt_mean_s"] / 60.0
    agg["tt_ff_mean_part_min"] = agg["tt_ff_mean_s"] / 60.0
    agg["tt_len_weighted_mean_min"] = np.where(agg["total_len_m"] > 0, agg["tt_len_w_sum"] / agg["total_len_m"] / 60.0, np.nan)
    agg["tt_ff_len_weighted_mean_min"] = np.where(
        agg["total_len_m"] > 0,
        agg["tt_ff_len_w_sum"] / agg["total_len_m"] / 60.0,
        np.nan,
    )
    agg["tt_weighted_mean_min"] = agg["tt_len_weighted_mean_min"]
    centroids = grid[["grid_id", "geometry"]].copy()
    centroids["grid_id"] = normalize_grid_id_series(centroids["grid_id"])
    centroids = centroids.dropna(subset=["grid_id"]).copy()
    centroids["centroid"] = centroids.geometry.centroid
    centroids = centroids.set_index("grid_id")["centroid"]
    agg["grid_o"] = normalize_grid_id_series(agg["grid_o"])
    agg["grid_d"] = normalize_grid_id_series(agg["grid_d"])
    agg["grid_dist_m"] = [
        float(centroids.loc[go].distance(centroids.loc[gd])) if (go in centroids.index and gd in centroids.index) else np.nan
        for go, gd in zip(agg["grid_o"], agg["grid_d"])
    ]
    agg["grid_dist_km"] = agg["grid_dist_m"] / 1000.0
    agg["edge_tt_harmonic_grid_min"] = np.where(
        np.isfinite(agg["grid_dist_km"]) & np.isfinite(agg["v_harm_kmh"]) & (agg["v_harm_kmh"] > 0),
        60.0 * agg["grid_dist_km"] / agg["v_harm_kmh"],
        np.nan,
    )
    agg["edge_t_ff_min"] = np.where(
        np.isfinite(agg["grid_dist_km"]) & np.isfinite(agg["v_ff_harm_kmh"]) & (agg["v_ff_harm_kmh"] > 0),
        60.0 * agg["grid_dist_km"] / agg["v_ff_harm_kmh"],
        np.nan,
    )
    agg["edge_tt_legacy_min"] = agg["tt_len_weighted_mean_min"]
    agg["edge_tt_min"] = agg["edge_tt_harmonic_grid_min"]
    agg = agg[
        [
            "period",
            "grid_o",
            "grid_d",
            "n_parts",
            "grid_dist_km",
            "total_len_km",
            "total_tt_min",
            "total_tt_ff_min",
            "tt_mean_part_min",
            "tt_ff_mean_part_min",
            "tt_len_weighted_mean_min",
            "tt_ff_len_weighted_mean_min",
            "tt_weighted_mean_min",
            "edge_tt_harmonic_grid_min",
            "edge_t_ff_min",
            "edge_tt_legacy_min",
            "edge_tt_min",
            "v_len_w_kmh",
            "v_harm_kmh",
            "v_ff_len_w_kmh",
            "v_ff_harm_kmh",
        ]
    ]
    return links_long, agg


def build_within_stats(cl_dir, grid, grid_type):
    grid_sindex = grid.sindex
    grid_sub = grid[["grid_id", "geometry"]].copy()
    within = np.zeros(len(cl_dir), dtype=bool)
    lengths = np.zeros(len(cl_dir), dtype=float)
    for i, g in enumerate(cl_dir.geometry):
        if g is None or g.is_empty:
            continue
        lengths[i] = float(g.length)
        cand_idx = list(grid_sindex.intersection(g.bounds))
        for row in grid_sub.iloc[cand_idx].itertuples(index=False):
            if g.within(row.geometry):
                within[i] = True
                break
    seg_total = int(len(cl_dir))
    seg_within = int(within.sum())
    len_total_km = float(lengths.sum() / 1000.0)
    len_within_km = float(lengths[within].sum() / 1000.0)
    return pd.DataFrame(
        [
            {
                "grid_type": grid_type,
                "segment_total": seg_total,
                "segment_within": seg_within,
                "segment_within_share": seg_within / seg_total if seg_total else np.nan,
                "len_total_km": len_total_km,
                "len_within_km": len_within_km,
                "len_within_share": len_within_km / len_total_km if len_total_km > 0 else np.nan,
            }
        ]
    )


def build_t_edges(agg, grid):
    grid_ids = sorted(normalize_grid_id_series(grid["grid_id"]).dropna().tolist())
    id2i = {gid: i for i, gid in enumerate(grid_ids)}
    node_map = pd.DataFrame({"grid_id": grid_ids, "node_i": np.arange(len(grid_ids), dtype=int)})
    out = {}
    for period in ["AM", "PM"]:
        df = agg[agg["period"] == period].copy()
        if df.empty:
            out[period] = pd.DataFrame(columns=["grid_o", "grid_d", "i", "j", "t_min"])
            continue
        df["grid_o"] = normalize_grid_id_series(df["grid_o"])
        df["grid_d"] = normalize_grid_id_series(df["grid_d"])
        df = df[np.isfinite(df["edge_tt_min"]) & (df["edge_tt_min"] > 0)].copy()
        df = df[df["grid_o"].notna() & df["grid_d"].notna()].copy()
        df = df[df["grid_o"].isin(id2i) & df["grid_d"].isin(id2i)].copy()
        df["i"] = df["grid_o"].map(id2i).astype(int)
        df["j"] = df["grid_d"].map(id2i).astype(int)
        df["t_min"] = df["edge_tt_min"].astype(float)
        df["t_ff_min"] = pd.to_numeric(df["edge_t_ff_min"], errors="coerce").astype(float)
        df = df.groupby(["grid_o", "grid_d", "i", "j"], as_index=False).agg(
            t_min=("t_min", "min"),
            t_ff_min=("t_ff_min", "min"),
        )
        out[period] = df
    return node_map, out


def run_for_grid(version_root: Path, grid_type: str):
    data_dir = version_root / "data"
    metrics_dir = version_root / "metrics"
    cl_dir, cl_speed, grid = load_inputs(version_root, grid_type)
    cl_peak = build_peak_centerline_speed(cl_dir, cl_speed)
    cl_peak.to_parquet(data_dir / f"cl_speed_peak_geo_{grid_type}.parquet", index=False)
    links_long, agg = build_links(cl_peak, grid)
    within = build_within_stats(cl_dir, grid, grid_type)
    node_map, edge_map = build_t_edges(agg, grid)

    links_long.to_csv(data_dir / f"grid_links_{grid_type}_long.csv", index=False)
    agg.to_csv(data_dir / f"grid_links_{grid_type}_agg.csv", index=False)
    within.to_csv(data_dir / f"grid_{grid_type}_within_stats.csv", index=False)
    node_map.to_csv(data_dir / f"t_nodes_{grid_type}.csv", index=False)
    edge_map["AM"].to_csv(data_dir / f"t_edges_{grid_type}_AM.csv", index=False)
    edge_map["PM"].to_csv(data_dir / f"t_edges_{grid_type}_PM.csv", index=False)

    pd.DataFrame(
        [
            {
                "grid_type": grid_type,
                "links_long": len(links_long),
                "links_agg": len(agg),
                "nodes": len(node_map),
                "edges_am": len(edge_map["AM"]),
                "edges_pm": len(edge_map["PM"]),
            }
        ]
    ).to_csv(metrics_dir / f"stage06_{grid_type}_summary.csv", index=False)


def run(config_path: str | None, version_id: str, output_dir: str, grid_type: str):
    version_root = Path(output_dir) / version_id
    data_dir = version_root / "data"
    metrics_dir = version_root / "metrics"
    data_dir.mkdir(parents=True, exist_ok=True)
    metrics_dir.mkdir(parents=True, exist_ok=True)
    save_config_snapshot(version_root, config_path, grid_type)

    grid_types = ["square", "hex", "voronoi"] if grid_type == "all" else [grid_type]
    for gt in grid_types:
        run_for_grid(version_root, gt)
        print(f"[stage06] completed grid_type={gt}")


def main():
    args = parse_args()
    run(args.config, args.version_id, args.output_dir, args.grid_type)


if __name__ == "__main__":
    main()
