from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


GRID_TYPES = ("square", "hex", "voronoi")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Compare centerline-grid and raw_connected_v1 travel-cost versions (up to three versions)."
    )
    parser.add_argument("--baseline-root", required=True, help="Version root containing stage06 grid-edge outputs.")
    parser.add_argument("--raw-node-root", default=None, help="Output root from node-based raw_connected_v1 build script.")
    parser.add_argument("--edgeproj-root", default=None, help="Output root from edgeproj build script (raw-edgeproj connector).")
    parser.add_argument("--period", default="AM", choices=["AM", "PM"])
    parser.add_argument("--out-dir", default=None, help="Output directory. Defaults to comparison_grid_methods/ under workspace.")
    return parser.parse_args()


def _paired_ratio(a: pd.Series, b: pd.Series) -> pd.Series:
    aa = pd.to_numeric(a, errors="coerce").astype(float)
    bb = pd.to_numeric(b, errors="coerce").astype(float)
    out = np.minimum(aa, bb) / np.maximum(aa, bb)
    out[(~np.isfinite(aa)) | (~np.isfinite(bb)) | (aa <= 0) | (bb <= 0)] = np.nan
    return pd.Series(out)


def load_stage06_adjacent(baseline_root: Path, grid_type: str, period: str) -> pd.DataFrame | None:
    path = baseline_root / "data" / f"t_edges_{grid_type}_{period}.csv"
    if not path.exists():
        return None
    df = pd.read_csv(path)
    return df.rename(columns={"grid_o": "home_grid", "grid_d": "work_grid", "t_min": "travel_time_min"})[
        ["home_grid", "work_grid", "travel_time_min"]
    ].copy()


def load_raw_connected_adjacent(root: Path, grid_type: str, period: str) -> pd.DataFrame | None:
    paths = [
        root / "data" / f"raw_connected_vs_old_adjacent_adjacent_{grid_type}_{period}.csv",
        root / "data" / f"kunpeng_vs_old_adjacent_adjacent_{grid_type}_{period}.csv",
    ]
    for path in paths:
        if path.exists():
            df = pd.read_csv(path)
            if "kunpeng_travel_time_min" in df.columns and "raw_connected_travel_time_min" not in df.columns:
                df = df.rename(columns={"kunpeng_travel_time_min": "raw_connected_travel_time_min"})
            return df
    return None


def load_grid_total(root: Path, grid_type: str) -> int | None:
    direct_grid_path = root / "data" / f"grid_{grid_type}_master.parquet"
    if direct_grid_path.exists():
        return int(len(pd.read_parquet(direct_grid_path, columns=["grid_id"])))
    config_path = root / "run_config.json"
    if not config_path.exists():
        return None
    config = pd.read_json(config_path, typ="series")
    baseline_root = Path(str(config.get("baseline_root", "")))
    if not baseline_root.exists():
        return None
    grid_path = baseline_root / "data" / f"grid_{grid_type}_master.parquet"
    if not grid_path.exists():
        return None
    return int(len(pd.read_parquet(grid_path, columns=["grid_id"])))


def load_connector_summary(root: Path, grid_type: str, total_grids_override: int | None = None) -> dict | None:
    connector_paths = [
        root / "data" / f"raw_connected_grid_connectors_adjacent_{grid_type}.csv",
        root / "data" / f"raw_connected_grid_connectors_all_{grid_type}.csv",
        root / "data" / f"kunpeng_grid_connectors_adjacent_{grid_type}.csv",
        root / "data" / f"kunpeng_grid_connectors_all_{grid_type}.csv",
    ]
    connector_df = None
    for path in connector_paths:
        if path.exists():
            connector_df = pd.read_csv(path)
            break

    paths = [
        root / "metrics" / f"raw_connected_connector_summary_adjacent_{grid_type}.csv",
        root / "metrics" / f"kunpeng_connector_summary_adjacent_{grid_type}.csv",
    ]
    for path in paths:
        if path.exists():
            df = pd.read_csv(path)
            if df.empty:
                return None
            row = df.iloc[0]
            total_grids = total_grids_override
            if total_grids is None:
                total_grids = load_grid_total(root, grid_type)
            if total_grids is None:
                total_grids = int(row.get("total_grids", 0))
            if connector_df is not None and "grid_id" in connector_df.columns:
                grids_with_connectors = int(connector_df["grid_id"].dropna().astype("string").nunique())
            else:
                grids_with_connectors = int(row.get("grids_with_connectors", 0))
            return {
                "total_grids": total_grids,
                "grids_with_connectors": grids_with_connectors,
                "share_connected": float(grids_with_connectors) / max(float(total_grids), 1),
                "mean_connector_dist_m": float(row.get("mean_connector_dist_m", np.nan)),
                "p90_connector_dist_m": float(row.get("p90_connector_dist_m", np.nan)),
            }
    return None


def summarize_version_adjacent(
    df: pd.DataFrame,
    time_col: str,
    grid_type: str,
    version: str,
    period: str,
) -> dict:
    df = df.copy()
    df["home_grid"] = df["home_grid"].astype("string")
    df["work_grid"] = df["work_grid"].astype("string")
    df[time_col] = pd.to_numeric(df[time_col], errors="coerce")
    valid = df[np.isfinite(df[time_col]) & (df[time_col] > 0)].copy()

    valid["pair_key"] = valid.apply(
        lambda r: "|".join(sorted((str(r["home_grid"]), str(r["work_grid"])))), axis=1
    )
    pair_counts = valid.groupby("pair_key").size().rename("dir_count").reset_index()
    both = pair_counts[pair_counts["dir_count"] >= 2]["pair_key"]
    asym_valid = valid[valid["pair_key"].isin(both)].copy()

    if asym_valid.empty:
        median_ratio = np.nan
        share_lt_05 = np.nan
    else:
        pair_stat = asym_valid.groupby("pair_key", as_index=False).agg(
            t_a=(time_col, "min"), t_b=(time_col, "max")
        )
        ratio = _paired_ratio(pair_stat["t_a"], pair_stat["t_b"])
        ratio_f = ratio[np.isfinite(ratio)]
        median_ratio = float(ratio_f.median()) if len(ratio_f) else np.nan
        share_lt_05 = float((ratio_f < 0.5).mean()) if len(ratio_f) else np.nan

    return {
        "version": version,
        "grid_type": grid_type,
        "period": period,
        "directed_adj_pairs_valid": int(len(valid)),
        "undirected_pairs_both_dir": int(len(pair_counts[pair_counts["dir_count"] >= 2])),
        "mean_travel_time_min": float(valid[time_col].mean()) if len(valid) else np.nan,
        "median_travel_time_min": float(valid[time_col].median()) if len(valid) else np.nan,
        "median_directional_ratio": median_ratio,
        "share_asym_lt_0_5": share_lt_05,
    }


def summarize_pairwise_correlation(
    base_df: pd.DataFrame,
    comp_df: pd.DataFrame,
    base_col: str,
    comp_col: str,
    grid_type: str,
    version: str,
    period: str,
) -> dict:
    base = base_df.copy()
    comp = comp_df.copy()
    base["home_grid"] = base["home_grid"].astype("string")
    base["work_grid"] = base["work_grid"].astype("string")
    comp["home_grid"] = comp["home_grid"].astype("string")
    comp["work_grid"] = comp["work_grid"].astype("string")
    merged = base.merge(comp, on=["home_grid", "work_grid"], how="inner")
    merged[base_col] = pd.to_numeric(merged[base_col], errors="coerce")
    merged[comp_col] = pd.to_numeric(merged[comp_col], errors="coerce")
    valid = merged[
        np.isfinite(merged[base_col]) & np.isfinite(merged[comp_col])
        & (merged[base_col] > 0) & (merged[comp_col] > 0)
    ].copy()
    if len(valid) < 2:
        return {"version": version, "grid_type": grid_type, "period": period, "n_comparable": 0, "pearson_corr_vs_centerline": np.nan}
    corr = np.corrcoef(valid[base_col], valid[comp_col])[0, 1]
    diff = valid[comp_col] - valid[base_col]
    ratio = valid[comp_col] / valid[base_col]
    return {
        "version": version,
        "grid_type": grid_type,
        "period": period,
        "n_comparable": int(len(valid)),
        "mean_centerline_t_min": float(valid[base_col].mean()),
        "mean_version_t_min": float(valid[comp_col].mean()),
        "median_ratio_vs_centerline": float(ratio.median()),
        "pearson_corr_vs_centerline": float(corr) if np.isfinite(corr) else np.nan,
    }


def main():
    args = parse_args()
    baseline_root = Path(args.baseline_root)
    raw_node_root = Path(args.raw_node_root) if args.raw_node_root else None
    edgeproj_root = Path(args.edgeproj_root) if args.edgeproj_root else None
    period = args.period

    if args.out_dir:
        out_dir = Path(args.out_dir)
    else:
        out_dir = baseline_root.parent / "comparison_grid_methods"
    out_dir.mkdir(parents=True, exist_ok=True)

    summary_rows = []
    corr_rows = []
    connector_rows = []
    baseline_grid_totals = {grid_type: load_grid_total(baseline_root, grid_type) for grid_type in GRID_TYPES}

    for grid_type in GRID_TYPES:
        stage06 = load_stage06_adjacent(baseline_root, grid_type, period)
        if stage06 is not None:
            summary_rows.append(summarize_version_adjacent(stage06, "travel_time_min", grid_type, "centerline", period))

        for label, root in [("raw_node", raw_node_root), ("raw_edgeproj", edgeproj_root)]:
            if root is None:
                continue
            adj = load_raw_connected_adjacent(root, grid_type, period)
            if adj is None:
                continue
            summary_rows.append(summarize_version_adjacent(adj, "raw_connected_travel_time_min", grid_type, label, period))
            conn = load_connector_summary(root, grid_type, total_grids_override=baseline_grid_totals.get(grid_type))
            if conn is not None:
                connector_rows.append({"version": label, "grid_type": grid_type, **conn})
            if stage06 is not None:
                corr_rows.append(
                    summarize_pairwise_correlation(
                        stage06, adj,
                        "travel_time_min", "raw_connected_travel_time_min",
                        grid_type, label, period,
                    )
                )

    summary_df = pd.DataFrame(summary_rows)
    corr_df = pd.DataFrame(corr_rows)
    connector_df = pd.DataFrame(connector_rows)

    summary_df.to_csv(out_dir / f"three_version_adjacent_summary_{period}.csv", index=False)
    corr_df.to_csv(out_dir / f"three_version_correlation_vs_centerline_{period}.csv", index=False)
    if not connector_df.empty:
        connector_df.to_csv(out_dir / f"three_version_connector_summary.csv", index=False)

    merged = summary_df.merge(
        corr_df.drop(columns=["mean_centerline_t_min"], errors="ignore"),
        on=["version", "grid_type", "period"],
        how="left",
    ).merge(
        connector_df,
        on=["version", "grid_type"],
        how="left",
    )
    merged.to_csv(out_dir / f"three_version_full_comparison_{period}.csv", index=False)
    print(f"[compare] outputs written to {out_dir}", flush=True)
    print(merged[["version", "grid_type", "mean_travel_time_min", "median_directional_ratio", "share_asym_lt_0_5",
                   "pearson_corr_vs_centerline", "share_connected", "mean_connector_dist_m"]].to_string(index=False))


if __name__ == "__main__":
    main()
