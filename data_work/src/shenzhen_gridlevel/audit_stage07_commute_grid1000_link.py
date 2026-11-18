"""Audit whether the Stage07 Shenzhen commute CSV links cleanly to fishnet1000.

The Stage07 CSV contains raw home/work plot IDs and coordinates. This script
checks whether those points can be assigned directly to the fishnet1000
basemap, compares the resulting coverage with the retained grid500-aggregate
path, and decomposes why the final inter-cell pairtime exposure is around
80 percent of total observed commuters.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import geopandas as gpd
import pandas as pd


def ensure_dirs(root: Path) -> None:
    for sub in ["data", "metrics"]:
        (root / sub).mkdir(parents=True, exist_ok=True)


def write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False, default=float))


def collect_unique_points(commute_path: Path, chunksize: int) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    home_parts = []
    work_parts = []
    rows = 0
    pop_sum = 0.0
    car_sum = 0.0
    missing = {}
    usecols = [
        "home_plot_id",
        "home_x",
        "home_y",
        "work_plot_id",
        "work_x",
        "work_y",
        "pop",
        "type_car",
        "In_beijing",
    ]
    for chunk in pd.read_csv(commute_path, usecols=usecols, chunksize=chunksize):
        rows += len(chunk)
        chunk["pop"] = pd.to_numeric(chunk["pop"], errors="coerce").fillna(0.0)
        chunk["type_car"] = pd.to_numeric(chunk["type_car"], errors="coerce").fillna(0.0)
        pop_sum += float(chunk["pop"].sum())
        car_sum += float(chunk["type_car"].sum())
        for col in usecols:
            missing[col] = missing.get(col, 0) + int(chunk[col].isna().sum())
        home_parts.append(chunk[["home_plot_id", "home_x", "home_y"]].drop_duplicates())
        work_parts.append(chunk[["work_plot_id", "work_x", "work_y"]].drop_duplicates())
    home = pd.concat(home_parts, ignore_index=True).drop_duplicates()
    work = pd.concat(work_parts, ignore_index=True).drop_duplicates()
    audit = {
        "stage07_commute_rows": int(rows),
        "stage07_pop_sum": pop_sum,
        "stage07_type_car_sum": car_sum,
        "stage07_missing_values": missing,
        "stage07_unique_home_plot_ids": int(home["home_plot_id"].nunique()),
        "stage07_unique_work_plot_ids": int(work["work_plot_id"].nunique()),
        "stage07_home_plot_ids_with_multiple_coordinates": int((home.groupby("home_plot_id").size() > 1).sum()),
        "stage07_work_plot_ids_with_multiple_coordinates": int((work.groupby("work_plot_id").size() > 1).sum()),
    }
    return home, work, audit


def spatial_join_points(points: pd.DataFrame, id_col: str, x_col: str, y_col: str, basemap: gpd.GeoDataFrame) -> pd.DataFrame:
    gdf = gpd.GeoDataFrame(
        points.copy(),
        geometry=gpd.points_from_xy(points[x_col], points[y_col]),
        crs=4326,
    )
    joined = gpd.sjoin(gdf, basemap[["fid1000", "geometry"]].to_crs(4326), predicate="within", how="left")
    out = pd.DataFrame(joined.drop(columns="geometry"))
    return out[[id_col, x_col, y_col, "fid1000"]]


def audit_grid500_map(home: pd.DataFrame, work: pd.DataFrame, grid500_map_path: Path) -> dict:
    grid500 = pd.read_stata(grid500_map_path, convert_categoricals=False)
    grid_ids = set(grid500["grid_id"].astype(str))
    home_ids = set(home["home_plot_id"].astype(str))
    work_ids = set(work["work_plot_id"].astype(str))
    all_ids = home_ids | work_ids
    return {
        "grid500_map_path": str(grid500_map_path),
        "grid500_map_rows": int(len(grid500)),
        "grid500_map_unique_grid_id": int(grid500["grid_id"].nunique()),
        "grid500_map_unique_all_id": int(grid500["all_id"].nunique()),
        "home_plot_ids_in_grid500_map": int(len(home_ids & grid_ids)),
        "home_plot_ids_in_grid500_map_share": len(home_ids & grid_ids) / len(home_ids),
        "work_plot_ids_in_grid500_map": int(len(work_ids & grid_ids)),
        "work_plot_ids_in_grid500_map_share": len(work_ids & grid_ids) / len(work_ids),
        "all_plot_ids_in_grid500_map": int(len(all_ids & grid_ids)),
        "all_plot_ids_in_grid500_map_share": len(all_ids & grid_ids) / len(all_ids),
    }


def classify_direct_csv_flow(
    commute_path: Path,
    home_map: dict,
    work_map: dict,
    pairtime_ids: set[int],
    chunksize: int,
) -> tuple[pd.DataFrame, dict]:
    stats: dict[str, dict[str, float]] = {}

    def add(label: str, rows: int, pop: float, car: float) -> None:
        bucket = stats.setdefault(label, {"rows": 0, "pop": 0.0, "type_car": 0.0})
        bucket["rows"] += int(rows)
        bucket["pop"] += float(pop)
        bucket["type_car"] += float(car)

    for chunk in pd.read_csv(
        commute_path,
        usecols=["home_plot_id", "work_plot_id", "pop", "type_car"],
        chunksize=chunksize,
    ):
        chunk["pop"] = pd.to_numeric(chunk["pop"], errors="coerce").fillna(0.0)
        chunk["type_car"] = pd.to_numeric(chunk["type_car"], errors="coerce").fillna(0.0)
        chunk["home_fid1000"] = chunk["home_plot_id"].map(home_map)
        chunk["work_fid1000"] = chunk["work_plot_id"].map(work_map)

        total_pop = float(chunk["pop"].sum())
        total_car = float(chunk["type_car"].sum())
        add("total_stage07_csv", len(chunk), total_pop, total_car)

        mapped_home = chunk["home_fid1000"].notna()
        mapped_work = chunk["work_fid1000"].notna()
        both_mapped = mapped_home & mapped_work
        add("both_home_work_points_in_fishnet1000_basemap", int(both_mapped.sum()), chunk.loc[both_mapped, "pop"].sum(), chunk.loc[both_mapped, "type_car"].sum())

        home_unmatched = ~mapped_home & mapped_work
        work_unmatched = mapped_home & ~mapped_work
        both_unmatched = ~mapped_home & ~mapped_work
        add("home_point_outside_fishnet1000_only", int(home_unmatched.sum()), chunk.loc[home_unmatched, "pop"].sum(), chunk.loc[home_unmatched, "type_car"].sum())
        add("work_point_outside_fishnet1000_only", int(work_unmatched.sum()), chunk.loc[work_unmatched, "pop"].sum(), chunk.loc[work_unmatched, "type_car"].sum())
        add("both_points_outside_fishnet1000", int(both_unmatched.sum()), chunk.loc[both_unmatched, "pop"].sum(), chunk.loc[both_unmatched, "type_car"].sum())

        active_home = chunk["home_fid1000"].isin(pairtime_ids)
        active_work = chunk["work_fid1000"].isin(pairtime_ids)
        both_active = both_mapped & active_home & active_work
        add("both_fids_in_pairtime_active_ids", int(both_active.sum()), chunk.loc[both_active, "pop"].sum(), chunk.loc[both_active, "type_car"].sum())

        mapped_not_active = both_mapped & ~both_active
        add("mapped_but_one_or_both_fids_not_in_pairtime", int(mapped_not_active.sum()), chunk.loc[mapped_not_active, "pop"].sum(), chunk.loc[mapped_not_active, "type_car"].sum())

        same_active = both_active & (chunk["home_fid1000"] == chunk["work_fid1000"])
        inter_active = both_active & (chunk["home_fid1000"] != chunk["work_fid1000"])
        add("same_fishnet1000_active_id_internal_commute", int(same_active.sum()), chunk.loc[same_active, "pop"].sum(), chunk.loc[same_active, "type_car"].sum())
        add("final_inter_fishnet1000_pairtime_exposure", int(inter_active.sum()), chunk.loc[inter_active, "pop"].sum(), chunk.loc[inter_active, "type_car"].sum())

    total_pop = stats["total_stage07_csv"]["pop"]
    total_car = stats["total_stage07_csv"]["type_car"]
    rows = []
    order = [
        "total_stage07_csv",
        "both_home_work_points_in_fishnet1000_basemap",
        "both_fids_in_pairtime_active_ids",
        "same_fishnet1000_active_id_internal_commute",
        "final_inter_fishnet1000_pairtime_exposure",
        "mapped_but_one_or_both_fids_not_in_pairtime",
        "home_point_outside_fishnet1000_only",
        "work_point_outside_fishnet1000_only",
        "both_points_outside_fishnet1000",
    ]
    for label in order:
        vals = stats.get(label, {"rows": 0, "pop": 0.0, "type_car": 0.0})
        rows.append(
            {
                "sample_step": label,
                "rows": int(vals["rows"]),
                "pop": float(vals["pop"]),
                "type_car": float(vals["type_car"]),
                "share_of_total_pop": vals["pop"] / total_pop if total_pop else None,
                "share_of_total_type_car": vals["type_car"] / total_car if total_car else None,
            }
        )
    return pd.DataFrame(rows), stats


def main(argv=None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--commute-csv", default="data_work/raw_data/commute_shenzhen_202411_for_stage07.csv")
    parser.add_argument("--pairtime", default="data_work/raw_data/shenzhen_gridlevel/fishnet1000_pairtime_hat.csv")
    parser.add_argument("--basemap", default="data_work/raw_data/shenzhen_gridlevel/shp/shenzhen_fishnet1000_info_combined.shp")
    parser.add_argument("--grid500-map", default="data_work/raw_data/commuting_grid_fishnet500.dta")
    parser.add_argument("--retained-coverage-json", default="data_work/outputs/shenzhen_gridlevel_spatial_asymmetry_v1/metrics/flow_crosswalk_coverage.json")
    parser.add_argument("--output-root", default="data_work/outputs/shenzhen_gridlevel_stage07_commute_link_audit_v1")
    parser.add_argument("--chunksize", type=int, default=500000)
    args = parser.parse_args(argv)

    out = Path(args.output_root)
    ensure_dirs(out)

    commute_path = Path(args.commute_csv)
    basemap = gpd.read_file(args.basemap)[["TARGET_FID", "geometry"]].rename(columns={"TARGET_FID": "fid1000"})
    basemap["fid1000"] = basemap["fid1000"].astype(int)
    pairtime_ids = set(pd.read_csv(args.pairtime, usecols=["fid_home"])["fid_home"].astype(int).unique())

    home, work, stage07_audit = collect_unique_points(commute_path, args.chunksize)
    home_join = spatial_join_points(home, "home_plot_id", "home_x", "home_y", basemap)
    work_join = spatial_join_points(work, "work_plot_id", "work_x", "work_y", basemap)
    home_join.to_csv(out / "data" / "stage07_home_plot_to_fishnet1000.csv", index=False)
    work_join.to_csv(out / "data" / "stage07_work_plot_to_fishnet1000.csv", index=False)

    home_map = home_join.dropna(subset=["fid1000"]).drop_duplicates("home_plot_id").set_index("home_plot_id")["fid1000"].astype(int).to_dict()
    work_map = work_join.dropna(subset=["fid1000"]).drop_duplicates("work_plot_id").set_index("work_plot_id")["fid1000"].astype(int).to_dict()
    direct_coverage, raw_stats = classify_direct_csv_flow(commute_path, home_map, work_map, pairtime_ids, args.chunksize)
    direct_coverage.to_csv(out / "metrics" / "stage07_direct_to_fishnet1000_coverage.csv", index=False)

    grid500_audit = audit_grid500_map(home, work, Path(args.grid500_map))

    retained_comparison = {}
    retained_path = Path(args.retained_coverage_json)
    if retained_path.exists():
        retained = json.loads(retained_path.read_text())
        retained_comparison = {
            "retained_grid500_aggregate_total_flow": retained.get("total_flow"),
            "retained_grid500_aggregate_final_exposure_flow": retained.get("final_exposure_flow"),
            "retained_grid500_aggregate_final_exposure_share": retained.get("share_total_flow_in_final_exposure"),
            "retained_grid500_aggregate_flow_with_both_pairtime_ids": retained.get("flow_with_both_pairtime_ids"),
            "retained_grid500_aggregate_flow_with_both_mapping": retained.get("flow_with_both_fishnet1000_mapping"),
        }

    direct_final = direct_coverage.set_index("sample_step").loc["final_inter_fishnet1000_pairtime_exposure"]
    direct_active = direct_coverage.set_index("sample_step").loc["both_fids_in_pairtime_active_ids"]
    comparison = pd.DataFrame(
        [
            {
                "path": "stage07_csv_direct_point_in_polygon",
                "total_flow": stage07_audit["stage07_pop_sum"],
                "flow_with_both_pairtime_ids": float(direct_active["pop"]),
                "final_inter_pairtime_flow": float(direct_final["pop"]),
                "final_inter_pairtime_share": float(direct_final["share_of_total_pop"]),
            },
            {
                "path": "retained_grid500_aggregate_point_in_polygon",
                "total_flow": retained_comparison.get("retained_grid500_aggregate_total_flow"),
                "flow_with_both_pairtime_ids": retained_comparison.get("retained_grid500_aggregate_flow_with_both_pairtime_ids"),
                "final_inter_pairtime_flow": retained_comparison.get("retained_grid500_aggregate_final_exposure_flow"),
                "final_inter_pairtime_share": retained_comparison.get("retained_grid500_aggregate_final_exposure_share"),
            },
        ]
    )
    comparison.to_csv(out / "metrics" / "stage07_vs_retained_grid500_coverage_comparison.csv", index=False)

    summary = {
        "commute_csv": str(commute_path),
        "basemap": str(args.basemap),
        "pairtime": str(args.pairtime),
        **stage07_audit,
        **grid500_audit,
        "direct_home_points_matched_to_fishnet1000": int(home_join["fid1000"].notna().sum()),
        "direct_home_points_total": int(len(home_join)),
        "direct_home_points_match_share": float(home_join["fid1000"].notna().mean()),
        "direct_work_points_matched_to_fishnet1000": int(work_join["fid1000"].notna().sum()),
        "direct_work_points_total": int(len(work_join)),
        "direct_work_points_match_share": float(work_join["fid1000"].notna().mean()),
        "pairtime_active_fids": int(len(pairtime_ids)),
        **retained_comparison,
    }
    write_json(out / "metrics" / "stage07_commute_grid1000_link_audit.json", summary)


if __name__ == "__main__":
    main()
