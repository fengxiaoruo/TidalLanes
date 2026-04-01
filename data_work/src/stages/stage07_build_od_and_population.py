"""
Stage 07: Build OD and Population

Purpose:
- Match commute observations to grids
- Aggregate OD flows
- Build residents, jobs, and reachability summaries

Current source notebook:
- code/04_GirdlevelData.ipynb
"""

import argparse
import json
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
COMMUTE_PATH = ROOT / "raw_data" / "commute_202211.csv"
TARGET_CRS = "EPSG:3857"
MODE_COLS = ["type_walk", "type_bike", "type_sub", "type_bus", "type_car"]


def parse_args():
    parser = argparse.ArgumentParser(description="Stage 07: Build OD and population")
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
        "stage": "stage07_build_od_and_population",
        "config_path": config_path,
        "grid_type": grid_type,
        "commute_path": str(COMMUTE_PATH),
    }
    (version_root / "config_snapshot.stage07.json").write_text(
        json.dumps(payload, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )


def guess_crs_from_xy(x, y):
    x = pd.Series(x).dropna()
    y = pd.Series(y).dropna()
    if x.empty or y.empty:
        return None
    if (x.between(-180, 180).mean() > 0.95) and (y.between(-90, 90).mean() > 0.95):
        return "EPSG:4326"
    return TARGET_CRS


def make_points_gdf(df, xcol, ycol, crs_guess, target_crs=TARGET_CRS):
    gdf = gpd.GeoDataFrame(df.copy(), geometry=gpd.points_from_xy(df[xcol], df[ycol]), crs=crs_guess)
    if str(gdf.crs) != str(target_crs):
        gdf = gdf.to_crs(target_crs)
    return gdf


def clean_sjoin_cols(gdf):
    for c in ["index_right", "index_left"]:
        if c in gdf.columns:
            gdf = gdf.drop(columns=c)
    return gdf


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


def load_grid_and_edges(version_root: Path, grid_type: str):
    data_dir = version_root / "data"
    grid = gpd.read_parquet(data_dir / f"grid_{grid_type}_master.parquet")
    edges_am = pd.read_csv(data_dir / f"t_edges_{grid_type}_AM.csv")
    if grid.crs is None:
        raise ValueError("Grid file has no CRS.")
    grid = grid.to_crs(TARGET_CRS)
    grid["grid_id"] = normalize_grid_id_series(grid["grid_id"])
    grid = grid[grid["grid_id"].notna()].copy()
    edges_am["grid_o"] = normalize_grid_id_series(edges_am["grid_o"])
    edges_am["grid_d"] = normalize_grid_id_series(edges_am["grid_d"])
    edges_am = edges_am[edges_am["grid_o"].notna() & edges_am["grid_d"].notna()].copy()
    return grid, edges_am


def match_commute_to_grid(grid: gpd.GeoDataFrame):
    df = pd.read_csv(COMMUTE_PATH)
    grid_sub = grid[["grid_id", "geometry"]].copy()

    home_crs = guess_crs_from_xy(df["home_x"], df["home_y"])
    work_crs = guess_crs_from_xy(df["work_x"], df["work_y"])

    gdf_home = make_points_gdf(df, "home_x", "home_y", home_crs, TARGET_CRS)
    home_join = gpd.sjoin(gdf_home, grid_sub, how="left", predicate="within")
    home_join = home_join.rename(columns={"grid_id": "home_grid"})
    home_join = clean_sjoin_cols(home_join)

    gdf_work = home_join.drop(columns="geometry").copy()
    gdf_work = make_points_gdf(gdf_work, "work_x", "work_y", work_crs, TARGET_CRS)
    work_join = gpd.sjoin(gdf_work, grid_sub, how="left", predicate="within")
    work_join = work_join.rename(columns={"grid_id": "work_grid"})
    work_join = clean_sjoin_cols(work_join)

    df_g = pd.DataFrame(work_join.drop(columns="geometry"))
    df_g["home_grid"] = normalize_grid_id_series(df_g["home_grid"])
    df_g["work_grid"] = normalize_grid_id_series(df_g["work_grid"])
    return df_g


def build_mode_summary(df_g: pd.DataFrame):
    mode_totals = df_g[MODE_COLS].fillna(0).sum().rename("count").reset_index().rename(columns={"index": "mode"})
    identified_total = float(mode_totals["count"].sum())
    pop_total = float(df_g["pop"].fillna(0).sum())
    mode_totals["share_in_identified_pct"] = 100 * (mode_totals["count"] / identified_total) if identified_total > 0 else np.nan
    mode_totals["share_in_pop_pct"] = 100 * (mode_totals["count"] / pop_total) if pop_total > 0 else np.nan
    return mode_totals


def build_od(df_g: pd.DataFrame):
    for c in ["pop"] + MODE_COLS:
        df_g[c] = pd.to_numeric(df_g[c], errors="coerce").fillna(0.0)
    df_g["home_grid"] = normalize_grid_id_series(df_g["home_grid"])
    df_g["work_grid"] = normalize_grid_id_series(df_g["work_grid"])
    use = df_g[df_g["home_grid"].notna() & df_g["work_grid"].notna()].copy()
    agg_dict = {"pop": "sum"}
    agg_dict.update({c: "sum" for c in MODE_COLS})
    od = use.groupby(["home_grid", "work_grid"], as_index=False).agg(agg_dict)
    od["identified"] = od[MODE_COLS].sum(axis=1)
    od["identified_share_of_pop"] = np.where(od["pop"] > 0, od["identified"] / od["pop"], np.nan)
    return od


def build_node_map_and_reachability(od: pd.DataFrame, edges: pd.DataFrame):
    edges["grid_o"] = normalize_grid_id_series(edges["grid_o"])
    edges["grid_d"] = normalize_grid_id_series(edges["grid_d"])
    edges = edges[edges["grid_o"].notna() & edges["grid_d"].notna()].copy()
    od["home_grid"] = normalize_grid_id_series(od["home_grid"])
    od["work_grid"] = normalize_grid_id_series(od["work_grid"])
    grid_from_edges = pd.unique(pd.concat([edges["grid_o"], edges["grid_d"]], ignore_index=True))
    grid_from_od = pd.unique(pd.concat([od["home_grid"], od["work_grid"]], ignore_index=True))
    grid_all = pd.Index(pd.unique(pd.Series(np.concatenate([grid_from_edges, grid_from_od])))).dropna().astype("string")
    node_map = pd.DataFrame({"grid_id": grid_all})
    node_map["node_i"] = np.arange(len(node_map), dtype=int)
    id2i = dict(zip(node_map["grid_id"], node_map["node_i"]))

    parent = np.arange(len(node_map), dtype=int)
    rank = np.zeros(len(node_map), dtype=int)

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra == rb:
            return
        if rank[ra] < rank[rb]:
            parent[ra] = rb
        elif rank[ra] > rank[rb]:
            parent[rb] = ra
        else:
            parent[rb] = ra
            rank[ra] += 1

    e = edges[edges["grid_o"].isin(id2i) & edges["grid_d"].isin(id2i)].copy()
    ai = e["grid_o"].map(id2i).astype(int).values
    bi = e["grid_d"].map(id2i).astype(int).values
    for a, b in zip(ai, bi):
        union(a, b)

    comp = np.array([find(i) for i in range(len(node_map))], dtype=int)
    node_map["component"] = comp

    od2 = od.copy()
    od2["home_i"] = od2["home_grid"].map(id2i)
    od2["work_i"] = od2["work_grid"].map(id2i)
    od2 = od2.dropna(subset=["home_i", "work_i"]).copy()
    od2["home_i"] = od2["home_i"].astype(int)
    od2["work_i"] = od2["work_i"].astype(int)
    od2["comp_home"] = comp[od2["home_i"].values]
    od2["comp_work"] = comp[od2["work_i"].values]
    od_reach = od2[od2["comp_home"] == od2["comp_work"]].copy()
    od_reach = od_reach.drop(columns=["comp_home", "comp_work"])
    return node_map, e, od_reach


def build_population(df_g: pd.DataFrame):
    use = df_g[df_g["home_grid"].notna() & df_g["work_grid"].notna()].copy()
    use["pop"] = pd.to_numeric(use["pop"], errors="coerce").fillna(0.0)
    use["home_grid"] = normalize_grid_id_series(use["home_grid"])
    use["work_grid"] = normalize_grid_id_series(use["work_grid"])

    residents = use.groupby("home_grid", as_index=False).agg(residents=("pop", "sum")).rename(columns={"home_grid": "grid_id"})
    jobs = use.groupby("work_grid", as_index=False).agg(jobs=("pop", "sum")).rename(columns={"work_grid": "grid_id"})
    grid_sum = residents.merge(jobs, on="grid_id", how="outer").fillna(0.0)
    grid_sum["residents"] = grid_sum["residents"].astype(float)
    grid_sum["jobs"] = grid_sum["jobs"].astype(float)
    grid_sum["job_resident_ratio"] = np.where(grid_sum["residents"] > 0, grid_sum["jobs"] / grid_sum["residents"], np.nan)
    return residents, jobs, grid_sum


def run_for_grid(version_root: Path, grid_type: str):
    data_dir = version_root / "data"
    metrics_dir = version_root / "metrics"
    grid, edges_am = load_grid_and_edges(version_root, grid_type)
    df_g = match_commute_to_grid(grid)
    mode_summary = build_mode_summary(df_g.copy())
    od = build_od(df_g.copy())
    node_map, edges_used, od_reach = build_node_map_and_reachability(od, edges_am.copy())
    residents, jobs, grid_sum = build_population(df_g.copy())

    df_g.to_csv(data_dir / f"commute_{grid_type}_matched.csv", index=False)
    mode_summary.to_csv(data_dir / f"commute_mode_share_summary_{grid_type}.csv", index=False)
    od.to_csv(data_dir / f"OD_{grid_type}.csv", index=False)
    node_map.to_csv(data_dir / f"grid_nodes_{grid_type}.csv", index=False)
    od_reach.to_csv(data_dir / f"OD_{grid_type}_reachable_AM.csv", index=False)
    residents.to_csv(data_dir / f"grid_residents_{grid_type}.csv", index=False)
    jobs.to_csv(data_dir / f"grid_jobs_{grid_type}.csv", index=False)
    grid_sum.to_csv(data_dir / f"grid_population_summary_{grid_type}.csv", index=False)

    summary = pd.DataFrame(
        [
            {
                "grid_type": grid_type,
                "nodes": len(node_map),
                "edges": len(edges_used),
                "OD_pairs_all": len(od),
                "OD_pairs_reachable": len(od_reach),
                "reachable_share_pairs": len(od_reach) / len(od) if len(od) else np.nan,
                "pop_all": float(od["pop"].sum()),
                "pop_reachable": float(od_reach["pop"].sum()),
                "reachable_share_pop": float(od_reach["pop"].sum() / od["pop"].sum()) if od["pop"].sum() > 0 else np.nan,
            }
        ]
    )
    summary.to_csv(data_dir / f"network_{grid_type}_AM_summary.csv", index=False)
    summary.to_csv(metrics_dir / f"stage07_{grid_type}_summary.csv", index=False)


def run(config_path: str | None, version_id: str, output_dir: str, grid_type: str):
    version_root = Path(output_dir) / version_id
    (version_root / "data").mkdir(parents=True, exist_ok=True)
    (version_root / "metrics").mkdir(parents=True, exist_ok=True)
    save_config_snapshot(version_root, config_path, grid_type)

    grid_types = ["square", "hex", "voronoi"] if grid_type == "all" else [grid_type]
    for gt in grid_types:
        run_for_grid(version_root, gt)
        print(f"[stage07] completed grid_type={gt}")


def main():
    args = parse_args()
    run(args.config, args.version_id, args.output_dir, args.grid_type)


if __name__ == "__main__":
    main()
