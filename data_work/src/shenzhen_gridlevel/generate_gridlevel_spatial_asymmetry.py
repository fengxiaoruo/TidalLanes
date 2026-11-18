"""Spatialize Shenzhen grid-level directional-asymmetry evidence.

This script uses the fishnet1000 polygon basemap to map OD travel-time
asymmetry and to attach observed grid500 commuting flows to fishnet1000 cells
through point-in-polygon assignment. Raw inputs are read-only.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.colors import TwoSlopeNorm
from matplotlib.patches import Patch


EPS = 1e-12
THRESHOLDS = [0.90, 0.83, 0.77, 0.67, 0.50]
SLOW_THRESHOLDS = [1.10, 1.25, 1.50, 2.00]


def ensure_dirs(root: Path) -> None:
    for sub in ["data", "figures", "metrics"]:
        (root / sub).mkdir(parents=True, exist_ok=True)


def write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False, default=float))


def threshold_label(value: float) -> str:
    return f"{value:.2f}".replace(".", "_")


def weighted_mean(values: pd.Series, weights: pd.Series) -> float:
    v = values.to_numpy(float)
    w = weights.to_numpy(float)
    mask = np.isfinite(v) & np.isfinite(w) & (w > 0)
    if not mask.any():
        return float("nan")
    return float(np.sum(v[mask] * w[mask]) / np.sum(w[mask]))


def weighted_quantile(values: pd.Series, weights: pd.Series, quantile: float) -> float:
    v = values.to_numpy(float)
    w = weights.to_numpy(float)
    mask = np.isfinite(v) & np.isfinite(w) & (w > 0)
    if not mask.any():
        return float("nan")
    v = v[mask]
    w = w[mask]
    order = np.argsort(v)
    v = v[order]
    w = w[order]
    cutoff = quantile * np.sum(w)
    return float(v[np.searchsorted(np.cumsum(w), cutoff, side="left")])


def describe_weighted_sample(df: pd.DataFrame, weight_col: str, label: str) -> dict[str, float | int | str]:
    w = df[weight_col]
    out: dict[str, float | int | str] = {
        "weight_scheme": label,
        "weight_col": weight_col,
        "od_pair_count": int(len(df)),
        "weight_sum": float(w.sum()),
        "weighted_mean_symmetry_ratio": weighted_mean(df["symmetry_ratio_time"], w),
        "weighted_median_symmetry_ratio": weighted_quantile(df["symmetry_ratio_time"], w, 0.50),
        "weighted_mean_slow_over_fast": weighted_mean(df["slow_over_fast_time"], w),
        "weighted_median_slow_over_fast": weighted_quantile(df["slow_over_fast_time"], w, 0.50),
        "weighted_mean_signed_log_ratio": weighted_mean(df["directed_log_ratio"], w),
        "weighted_median_signed_log_ratio": weighted_quantile(df["directed_log_ratio"], w, 0.50),
        "weighted_mean_directed_time_gap_min": weighted_mean(df["directed_time_gap_min"], w),
        "weighted_median_directed_time_gap_min": weighted_quantile(df["directed_time_gap_min"], w, 0.50),
    }
    for thr in THRESHOLDS:
        out[f"weighted_share_symmetry_le_{threshold_label(thr)}"] = weighted_mean(
            (df["symmetry_ratio_time"] <= thr).astype(float), w
        )
    for thr in SLOW_THRESHOLDS:
        out[f"weighted_share_actual_direction_slow_ge_{threshold_label(thr)}"] = weighted_mean(
            (df["directed_ratio_vs_reverse"] >= thr).astype(float), w
        )
        out[f"weighted_share_actual_direction_fast_ge_{threshold_label(thr)}"] = weighted_mean(
            (df["directed_ratio_vs_reverse"] <= 1.0 / thr).astype(float), w
        )
    return out


def load_basemap(path: Path) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(path)
    if "TARGET_FID" not in gdf.columns:
        raise ValueError(f"{path} does not contain TARGET_FID")
    keep = [
        c
        for c in [
            "TARGET_FID",
            "QH_NAME",
            "QH_CODE",
            "Econzone",
            "rent",
            "Dist_line",
            "Dist_net",
            "Dist_chk",
            "lon",
            "lat",
            "geometry",
        ]
        if c in gdf.columns
    ]
    gdf = gdf[keep].copy()
    gdf = gdf.rename(columns={"TARGET_FID": "fid1000"})
    gdf["fid1000"] = gdf["fid1000"].astype(int)
    return gdf


def build_directed_pairtime(pairtime_path: Path) -> pd.DataFrame:
    cols = ["fid_home", "fid_work", "car_time", "car_dist"]
    df = pd.read_csv(pairtime_path, usecols=cols)
    rev = df.rename(
        columns={
            "fid_home": "fid_work",
            "fid_work": "fid_home",
            "car_time": "car_time_reverse",
            "car_dist": "car_dist_reverse",
        }
    )
    directed = df.merge(rev, on=["fid_home", "fid_work"], how="inner", validate="one_to_one")
    directed = directed.loc[
        (directed["fid_home"] != directed["fid_work"]) &
        (directed["car_time"] >= 1.0) &
        (directed["car_time_reverse"] >= 1.0)
    ].copy()
    t = directed["car_time"].to_numpy(float)
    tr = directed["car_time_reverse"].to_numpy(float)
    directed["directed_ratio_vs_reverse"] = t / np.clip(tr, EPS, None)
    directed["directed_log_ratio"] = np.log(np.clip(t, EPS, None) / np.clip(tr, EPS, None))
    directed["directed_time_gap_min"] = t - tr
    directed["fast_time_min"] = np.minimum(t, tr)
    directed["slow_time_min"] = np.maximum(t, tr)
    directed["slow_over_fast_time"] = directed["slow_time_min"] / np.clip(directed["fast_time_min"], EPS, None)
    directed["symmetry_ratio_time"] = directed["fast_time_min"] / np.clip(directed["slow_time_min"], EPS, None)
    directed["mean_dist_km"] = (directed["car_dist"].to_numpy(float) + directed["car_dist_reverse"].to_numpy(float)) / 2.0
    return directed


def build_unweighted_cell_metrics(directed: pd.DataFrame) -> pd.DataFrame:
    grouped = directed.groupby("fid_home", observed=False)
    out = grouped.agg(
        potential_destinations=("fid_work", "count"),
        unweighted_mean_symmetry_ratio=("symmetry_ratio_time", "mean"),
        unweighted_median_symmetry_ratio=("symmetry_ratio_time", "median"),
        unweighted_mean_signed_log_ratio=("directed_log_ratio", "mean"),
        unweighted_median_signed_log_ratio=("directed_log_ratio", "median"),
        unweighted_mean_directed_time_gap_min=("directed_time_gap_min", "mean"),
        unweighted_median_directed_time_gap_min=("directed_time_gap_min", "median"),
        unweighted_mean_dist_km=("mean_dist_km", "mean"),
    ).reset_index().rename(columns={"fid_home": "fid1000"})
    for thr in THRESHOLDS:
        out[f"unweighted_share_symmetry_le_{threshold_label(thr)}"] = grouped["symmetry_ratio_time"].apply(
            lambda x, threshold=thr: float((x <= threshold).mean())
        ).to_numpy()
    for thr in SLOW_THRESHOLDS:
        out[f"unweighted_share_outbound_slow_ge_{threshold_label(thr)}"] = grouped["directed_ratio_vs_reverse"].apply(
            lambda x, threshold=thr: float((x >= threshold).mean())
        ).to_numpy()
        out[f"unweighted_share_outbound_fast_ge_{threshold_label(thr)}"] = grouped["directed_ratio_vs_reverse"].apply(
            lambda x, threshold=thr: float((x <= 1.0 / threshold).mean())
        ).to_numpy()
    return out


def build_grid500_crosswalk(
    flow: pd.DataFrame,
    basemap: gpd.GeoDataFrame,
    role: str,
    id_col: str,
    lon_col: str,
    lat_col: str,
) -> pd.DataFrame:
    points = flow[[id_col, lon_col, lat_col]].dropna().drop_duplicates().copy()
    points = points.loc[points[id_col].astype(str) != "-1"].copy()
    points_gdf = gpd.GeoDataFrame(
        points,
        geometry=gpd.points_from_xy(points[lon_col], points[lat_col]),
        crs=4326,
    )
    polygons = basemap[["fid1000", "geometry"]].to_crs(4326)
    joined = gpd.sjoin(points_gdf, polygons, predicate="within", how="left")
    joined = joined[[id_col, lon_col, lat_col, "fid1000"]].copy()
    joined["role"] = role
    joined = joined.rename(
        columns={
            id_col: "grid500_all_id",
            lon_col: "lon_wgs",
            lat_col: "lat_wgs",
        }
    )
    joined["fid1000"] = joined["fid1000"].astype("Int64")
    return joined


def build_flow_exposure(
    flow_path: Path,
    basemap: gpd.GeoDataFrame,
    directed_pairtime: pd.DataFrame,
    output_root: Path,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, object]]:
    cols = [
        "resi_all_id",
        "work_all_id",
        "flow",
        "num_asset_car",
        "resi_lon_wgs",
        "resi_lat_wgs",
        "work_lon_wgs",
        "work_lat_wgs",
    ]
    flow = pd.read_stata(flow_path, columns=cols, convert_categoricals=False)
    total_flow = float(flow["flow"].sum())
    total_car_asset = float(flow["num_asset_car"].sum())

    resi_crosswalk = build_grid500_crosswalk(
        flow, basemap, "residence", "resi_all_id", "resi_lon_wgs", "resi_lat_wgs"
    )
    work_crosswalk = build_grid500_crosswalk(
        flow, basemap, "workplace", "work_all_id", "work_lon_wgs", "work_lat_wgs"
    )
    crosswalk = pd.concat([resi_crosswalk, work_crosswalk], ignore_index=True)
    crosswalk.to_csv(output_root / "data" / "grid500_to_fishnet1000_crosswalk.csv", index=False)

    resi_map = resi_crosswalk[["grid500_all_id", "fid1000"]].rename(
        columns={"grid500_all_id": "resi_all_id", "fid1000": "resi_fid1000"}
    )
    work_map = work_crosswalk[["grid500_all_id", "fid1000"]].rename(
        columns={"grid500_all_id": "work_all_id", "fid1000": "work_fid1000"}
    )
    flow_mapped = flow.merge(resi_map, on="resi_all_id", how="left").merge(work_map, on="work_all_id", how="left")
    flow_mapped["valid_work_id"] = flow_mapped["work_all_id"].astype(str) != "-1"

    pairtime_ids = set(directed_pairtime["fid_home"].unique()) | set(directed_pairtime["fid_work"].unique())
    flow_mapped["both_pairtime_ids"] = (
        flow_mapped["resi_fid1000"].isin(pairtime_ids) &
        flow_mapped["work_fid1000"].isin(pairtime_ids)
    )
    flow_mapped["inter_fishnet_pairtime_ids"] = (
        flow_mapped["both_pairtime_ids"] &
        (flow_mapped["resi_fid1000"] != flow_mapped["work_fid1000"])
    )

    aggregate = (
        flow_mapped.loc[flow_mapped["resi_fid1000"].notna() & flow_mapped["work_fid1000"].notna()]
        .groupby(["resi_fid1000", "work_fid1000"], as_index=False)
        .agg(flow=("flow", "sum"), num_asset_car=("num_asset_car", "sum"))
    )
    aggregate["resi_fid1000"] = aggregate["resi_fid1000"].astype(int)
    aggregate["work_fid1000"] = aggregate["work_fid1000"].astype(int)

    time_cols = [
        "fid_home",
        "fid_work",
        "car_time",
        "car_time_reverse",
        "directed_ratio_vs_reverse",
        "directed_log_ratio",
        "directed_time_gap_min",
        "slow_over_fast_time",
        "symmetry_ratio_time",
        "mean_dist_km",
    ]
    exposure = aggregate.merge(
        directed_pairtime[time_cols],
        left_on=["resi_fid1000", "work_fid1000"],
        right_on=["fid_home", "fid_work"],
        how="inner",
        validate="one_to_one",
    )
    exposure = exposure.drop(columns=["fid_home", "fid_work"])
    exposure = exposure.loc[
        (exposure["resi_fid1000"] != exposure["work_fid1000"]) &
        (exposure["flow"] > 0)
    ].copy()

    coverage: dict[str, object] = {
        "flow_path": str(flow_path),
        "total_rows": int(len(flow)),
        "total_flow": total_flow,
        "total_num_asset_car": total_car_asset,
        "valid_work_id_rows": int(flow_mapped["valid_work_id"].sum()),
        "valid_work_id_flow": float(flow_mapped.loc[flow_mapped["valid_work_id"], "flow"].sum()),
        "rows_with_both_fishnet1000_mapping": int(
            (flow_mapped["resi_fid1000"].notna() & flow_mapped["work_fid1000"].notna()).sum()
        ),
        "flow_with_both_fishnet1000_mapping": float(
            flow_mapped.loc[
                flow_mapped["resi_fid1000"].notna() & flow_mapped["work_fid1000"].notna(),
                "flow",
            ].sum()
        ),
        "rows_with_both_pairtime_ids": int(flow_mapped["both_pairtime_ids"].sum()),
        "flow_with_both_pairtime_ids": float(flow_mapped.loc[flow_mapped["both_pairtime_ids"], "flow"].sum()),
        "rows_final_inter_fishnet_pairtime_ids": int(flow_mapped["inter_fishnet_pairtime_ids"].sum()),
        "flow_final_inter_fishnet_pairtime_ids": float(
            flow_mapped.loc[flow_mapped["inter_fishnet_pairtime_ids"], "flow"].sum()
        ),
        "aggregate_fishnet1000_pairs": int(len(aggregate)),
        "aggregate_fishnet1000_flow": float(aggregate["flow"].sum()),
        "final_exposure_pairs": int(len(exposure)),
        "final_exposure_flow": float(exposure["flow"].sum()),
        "final_exposure_num_asset_car": float(exposure["num_asset_car"].sum()),
        "residence_crosswalk_points": int((crosswalk["role"] == "residence").sum()),
        "residence_crosswalk_matched": int(
            crosswalk.loc[crosswalk["role"] == "residence", "fid1000"].notna().sum()
        ),
        "workplace_crosswalk_points": int((crosswalk["role"] == "workplace").sum()),
        "workplace_crosswalk_matched": int(
            crosswalk.loc[crosswalk["role"] == "workplace", "fid1000"].notna().sum()
        ),
    }
    coverage["share_total_flow_in_final_exposure"] = (
        coverage["final_exposure_flow"] / total_flow if total_flow > 0 else float("nan")
    )
    coverage["share_valid_work_flow_in_final_exposure"] = (
        coverage["final_exposure_flow"] / coverage["valid_work_id_flow"]
        if coverage["valid_work_id_flow"] > 0
        else float("nan")
    )

    return exposure, crosswalk, coverage


def summarize_unweighted_reciprocal(directed: pd.DataFrame) -> pd.DataFrame:
    reciprocal = directed.loc[directed["fid_home"] < directed["fid_work"]].copy()
    rows = [
        {
            "weight_scheme": "unweighted_potential_grid_pairs",
            "od_pair_count": int(len(reciprocal)),
            "weight_sum": float(len(reciprocal)),
            "weighted_mean_symmetry_ratio": float(reciprocal["symmetry_ratio_time"].mean()),
            "weighted_median_symmetry_ratio": float(reciprocal["symmetry_ratio_time"].median()),
            "weighted_mean_slow_over_fast": float(reciprocal["slow_over_fast_time"].mean()),
            "weighted_median_slow_over_fast": float(reciprocal["slow_over_fast_time"].median()),
        }
    ]
    for thr in THRESHOLDS:
        rows[0][f"weighted_share_symmetry_le_{threshold_label(thr)}"] = float(
            (reciprocal["symmetry_ratio_time"] <= thr).mean()
        )
    return pd.DataFrame(rows)


def build_flow_cell_metrics(exposure: pd.DataFrame) -> pd.DataFrame:
    origin = exposure.groupby("resi_fid1000", observed=False).apply(
        lambda g: pd.Series(
            {
                "origin_flow": float(g["flow"].sum()),
                "origin_num_asset_car": float(g["num_asset_car"].sum()),
                "origin_od_pairs": int(len(g)),
                "origin_mean_symmetry_ratio_floww": weighted_mean(g["symmetry_ratio_time"], g["flow"]),
                "origin_share_symmetry_le_0_90_floww": weighted_mean(
                    (g["symmetry_ratio_time"] <= 0.90).astype(float), g["flow"]
                ),
                "origin_mean_signed_log_ratio_floww": weighted_mean(g["directed_log_ratio"], g["flow"]),
                "origin_mean_directed_time_gap_min_floww": weighted_mean(g["directed_time_gap_min"], g["flow"]),
                "origin_share_actual_slow_ge_1_10_floww": weighted_mean(
                    (g["directed_ratio_vs_reverse"] >= 1.10).astype(float), g["flow"]
                ),
                "origin_share_actual_fast_ge_1_10_floww": weighted_mean(
                    (g["directed_ratio_vs_reverse"] <= 1.0 / 1.10).astype(float), g["flow"]
                ),
            }
        )
    ).reset_index().rename(columns={"resi_fid1000": "fid1000"})

    workplace = exposure.groupby("work_fid1000", observed=False).apply(
        lambda g: pd.Series(
            {
                "workplace_flow": float(g["flow"].sum()),
                "workplace_num_asset_car": float(g["num_asset_car"].sum()),
                "workplace_od_pairs": int(len(g)),
                "workplace_mean_symmetry_ratio_floww": weighted_mean(g["symmetry_ratio_time"], g["flow"]),
                "workplace_share_symmetry_le_0_90_floww": weighted_mean(
                    (g["symmetry_ratio_time"] <= 0.90).astype(float), g["flow"]
                ),
                "workplace_mean_inbound_signed_log_ratio_floww": weighted_mean(g["directed_log_ratio"], g["flow"]),
                "workplace_mean_inbound_time_gap_min_floww": weighted_mean(g["directed_time_gap_min"], g["flow"]),
                "workplace_share_inbound_slow_ge_1_10_floww": weighted_mean(
                    (g["directed_ratio_vs_reverse"] >= 1.10).astype(float), g["flow"]
                ),
                "workplace_share_inbound_fast_ge_1_10_floww": weighted_mean(
                    (g["directed_ratio_vs_reverse"] <= 1.0 / 1.10).astype(float), g["flow"]
                ),
            }
        )
    ).reset_index().rename(columns={"work_fid1000": "fid1000"})

    return origin.merge(workplace, on="fid1000", how="outer")


def summarize_group_exposure(
    exposure: pd.DataFrame,
    basemap: gpd.GeoDataFrame,
    fid_col: str,
    group_cols: list[str],
    group_role: str,
) -> pd.DataFrame:
    labels = basemap[["fid1000", *[c for c in group_cols if c in basemap.columns]]].copy()
    labels = labels.rename(columns={"fid1000": fid_col})
    df = exposure.merge(labels, on=fid_col, how="left")
    valid_group_cols = [c for c in group_cols if c in df.columns]
    if not valid_group_cols:
        return pd.DataFrame()
    group_keys = valid_group_cols
    rows = []
    for key, g in df.groupby(group_keys, dropna=False, observed=False):
        if not isinstance(key, tuple):
            key = (key,)
        row: dict[str, object] = {
            "group_role": group_role,
            "od_pair_count": int(len(g)),
            "flow_sum": float(g["flow"].sum()),
            "num_asset_car_sum": float(g["num_asset_car"].sum()),
            "weighted_mean_symmetry_ratio": weighted_mean(g["symmetry_ratio_time"], g["flow"]),
            "weighted_median_symmetry_ratio": weighted_quantile(g["symmetry_ratio_time"], g["flow"], 0.50),
            "weighted_mean_slow_over_fast": weighted_mean(g["slow_over_fast_time"], g["flow"]),
            "weighted_median_slow_over_fast": weighted_quantile(g["slow_over_fast_time"], g["flow"], 0.50),
            "weighted_mean_signed_log_ratio": weighted_mean(g["directed_log_ratio"], g["flow"]),
            "weighted_median_signed_log_ratio": weighted_quantile(g["directed_log_ratio"], g["flow"], 0.50),
            "weighted_share_actual_direction_slow_ge_1_10": weighted_mean(
                (g["directed_ratio_vs_reverse"] >= 1.10).astype(float), g["flow"]
            ),
            "weighted_share_actual_direction_fast_ge_1_10": weighted_mean(
                (g["directed_ratio_vs_reverse"] <= 1.0 / 1.10).astype(float), g["flow"]
            ),
        }
        for col, value in zip(group_keys, key):
            row[col] = value
        for thr in [0.90, 0.83, 0.77]:
            row[f"weighted_share_symmetry_le_{threshold_label(thr)}"] = weighted_mean(
                (g["symmetry_ratio_time"] <= thr).astype(float), g["flow"]
            )
        rows.append(row)
    out = pd.DataFrame(rows)
    if not out.empty:
        out = out.sort_values(["flow_sum", "od_pair_count"], ascending=[False, False])
    return out


def write_spatial_group_summaries(exposure: pd.DataFrame, basemap: gpd.GeoDataFrame, output_root: Path) -> None:
    specs = [
        ("resi_fid1000", ["QH_NAME", "QH_CODE"], "origin_street", "flow_weighted_asymmetry_by_origin_street.csv"),
        ("work_fid1000", ["QH_NAME", "QH_CODE"], "workplace_street", "flow_weighted_asymmetry_by_workplace_street.csv"),
        ("resi_fid1000", ["Econzone"], "origin_econzone", "flow_weighted_asymmetry_by_origin_econzone.csv"),
        ("work_fid1000", ["Econzone"], "workplace_econzone", "flow_weighted_asymmetry_by_workplace_econzone.csv"),
    ]
    all_rows = []
    for fid_col, group_cols, role, filename in specs:
        table = summarize_group_exposure(exposure, basemap, fid_col, group_cols, role)
        if table.empty:
            continue
        table.to_csv(output_root / "metrics" / filename, index=False)
        all_rows.append(table)
    if all_rows:
        pd.concat(all_rows, ignore_index=True, sort=False).to_csv(
            output_root / "metrics" / "flow_weighted_asymmetry_spatial_groups_all.csv",
            index=False,
        )


def plot_polygon_map(
    gdf: gpd.GeoDataFrame,
    column: str,
    path: Path,
    title: str,
    cmap: str,
    diverging: bool = False,
    vmin: float | None = None,
    vmax: float | None = None,
) -> None:
    plot_gdf = gdf.to_crs(4326)
    fig, ax = plt.subplots(figsize=(8.2, 6.2))
    base = plot_gdf.copy()
    base.plot(ax=ax, color="#f0f0f0", edgecolor="white", linewidth=0.08)
    kwargs: dict[str, object] = {
        "column": column,
        "ax": ax,
        "cmap": cmap,
        "legend": True,
        "missing_kwds": {"color": "#f0f0f0", "edgecolor": "white", "linewidth": 0.05},
        "edgecolor": "none",
    }
    if diverging:
        vals = plot_gdf[column].replace([np.inf, -np.inf], np.nan).dropna()
        if vals.empty:
            norm = TwoSlopeNorm(vmin=-1.0, vcenter=0.0, vmax=1.0)
        else:
            lim = float(np.nanpercentile(np.abs(vals), 97))
            lim = max(lim, 0.01)
            norm = TwoSlopeNorm(vmin=-lim, vcenter=0.0, vmax=lim)
        kwargs["norm"] = norm
    else:
        if vmin is not None:
            kwargs["vmin"] = vmin
        if vmax is not None:
            kwargs["vmax"] = vmax
    plot_gdf.plot(**kwargs)
    plot_gdf.boundary.plot(ax=ax, color="#ffffff", linewidth=0.03, alpha=0.35)
    ax.set_axis_off()
    ax.set_title(title, fontsize=13)
    fig.tight_layout()
    fig.savefig(path, dpi=240)
    plt.close(fig)


def plot_coverage_map(gdf: gpd.GeoDataFrame, path: Path) -> None:
    plot_gdf = gdf.to_crs(4326).copy()
    conditions = [
        plot_gdf["in_pairtime_matrix"].fillna(False) & plot_gdf["has_observed_flow"].fillna(False),
        plot_gdf["in_pairtime_matrix"].fillna(False),
        plot_gdf["has_observed_flow"].fillna(False),
    ]
    choices = ["Pairtime and observed flow", "Pairtime only", "Observed flow only"]
    plot_gdf["coverage_class"] = np.select(conditions, choices, default="Basemap only")
    colors = {
        "Pairtime and observed flow": "#2A6F97",
        "Pairtime only": "#8ECAE6",
        "Observed flow only": "#F4A261",
        "Basemap only": "#D9D9D9",
    }
    fig, ax = plt.subplots(figsize=(8.2, 6.2))
    for key, color in colors.items():
        sub = plot_gdf.loc[plot_gdf["coverage_class"] == key]
        if not sub.empty:
            sub.plot(ax=ax, color=color, edgecolor="white", linewidth=0.05)
    handles = [
        Patch(facecolor=color, edgecolor="none", label=key)
        for key, color in colors.items()
        if (plot_gdf["coverage_class"] == key).any()
    ]
    ax.legend(handles=handles, frameon=False, loc="lower left", fontsize=8)
    ax.set_axis_off()
    ax.set_title("Fishnet1000 Coverage: Pairtime Matrix And Observed Flow")
    fig.tight_layout()
    fig.savefig(path, dpi=240)
    plt.close(fig)


def plot_threshold_comparison(summary: pd.DataFrame, path: Path) -> None:
    keep = summary.loc[
        summary["weight_scheme"].isin(
            ["unweighted_potential_grid_pairs", "observed_flow", "car_asset_holders"]
        )
    ].copy()
    cols = [f"weighted_share_symmetry_le_{threshold_label(t)}" for t in [0.90, 0.83, 0.77]]
    labels = ["<= 0.90", "<= 0.83", "<= 0.77"]
    xlabels = {
        "unweighted_potential_grid_pairs": "Potential pairs",
        "observed_flow": "Observed flow",
        "car_asset_holders": "Car-asset flow",
    }
    keep["label"] = keep["weight_scheme"].map(xlabels)
    x = np.arange(len(keep))
    width = 0.24
    colors = ["#4C78A8", "#F58518", "#54A24B"]
    fig, ax = plt.subplots(figsize=(8.4, 5.0))
    for i, (col, label, color) in enumerate(zip(cols, labels, colors)):
        values = keep[col].astype(float).to_numpy()
        ax.bar(x + (i - 1) * width, values, width=width, label=label, color=color)
        for j, value in enumerate(values):
            if np.isfinite(value):
                ax.text(j + (i - 1) * width, value + 0.012, f"{value:.0%}", ha="center", va="bottom", fontsize=8)
    ax.set_xticks(x)
    ax.set_xticklabels(keep["label"])
    ax.set_ylim(0, 0.75)
    ax.set_ylabel("Share below symmetry threshold")
    ax.set_title("Potential Grid Pairs Versus Observed Commuter Exposure")
    ax.legend(title="Symmetry ratio", frameon=False, ncols=3, loc="upper right")
    ax.grid(axis="y", alpha=0.25)
    fig.tight_layout()
    fig.savefig(path, dpi=240)
    plt.close(fig)


def plot_flow_exposure_asymmetry_scatter(exposure: pd.DataFrame, out_path: Path, *, dpi: int = 300) -> dict[str, float]:
    use = exposure.loc[exposure["flow"] > 0].copy()
    weights = use["flow"].astype(float)
    wmed = weighted_quantile(use["symmetry_ratio_time"], weights, 0.50)
    wshare90 = weighted_mean((use["symmetry_ratio_time"] <= 0.90).astype(float), weights)
    slow10 = weighted_mean((use["directed_ratio_vs_reverse"] >= 1.10).astype(float), weights)

    fig, ax = plt.subplots(figsize=(8.8, 5.8))
    bg = use.loc[use["flow"] >= 10]
    ax.scatter(
        bg["symmetry_ratio_time"],
        np.log10(bg["flow"].clip(lower=1.0)),
        s=7,
        c="#bdbdbd",
        alpha=0.10,
        edgecolors="none",
        rasterized=True,
        zorder=1,
    )

    top = use.nlargest(800, "flow")
    smax = float(np.sqrt(top["flow"].max()))
    sizes = 14 + 95 * np.sqrt(top["flow"].astype(float) / smax)
    sc = ax.scatter(
        top["symmetry_ratio_time"],
        np.log10(top["flow"].astype(float)),
        s=sizes,
        c=top["symmetry_ratio_time"].astype(float),
        cmap="RdYlBu_r",
        vmin=0.45,
        vmax=1.0,
        alpha=0.82,
        edgecolors="white",
        linewidths=0.35,
        zorder=3,
    )

    ax.axvline(0.90, color="#666666", linestyle="--", linewidth=1.1, zorder=2)
    ax.axvline(wmed, color="#c44e52", linestyle="--", linewidth=1.1, zorder=2)
    ax.text(
        0.03,
        0.97,
        (
            f"Flow-weighted median = {wmed:.3f}\n"
            f"{100 * wshare90:.1f}% of commuters on pairs with ratio $\\leq 0.90$\n"
            f"{100 * slow10:.1f}% travel $\\geq 10\\%$ slower than reverse"
        ),
        transform=ax.transAxes,
        va="top",
        ha="left",
        fontsize=9,
        bbox=dict(boxstyle="round,pad=0.35", facecolor="white", alpha=0.92, edgecolor="none"),
    )
    cbar = fig.colorbar(sc, ax=ax, fraction=0.046, pad=0.02)
    cbar.set_label("Symmetry ratio (min / max travel time)")
    cbar.ax.tick_params(labelsize=8)

    ax.set_xlim(0.42, 1.01)
    ax.set_xlabel("Symmetry ratio: min / max travel time")
    ax.set_ylabel("Commuters (log$_{10}$ scale)")
    ax.set_title("Observed OD pairs: commuter volume vs directional asymmetry")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    fig.tight_layout()
    fig.savefig(out_path, dpi=dpi, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return {
        "weighted_median_symmetry_ratio": wmed,
        "weighted_share_symmetry_le_0_90": wshare90,
        "weighted_share_actual_direction_slow_ge_1_10": slow10,
    }


def plot_preslide_flow_ratio_histogram(
    directed: pd.DataFrame,
    exposure: pd.DataFrame,
    out_path: Path,
    *,
    dpi: int = 300,
) -> dict[str, float]:
    from src.diagnostics.preslide_asymmetry_style import (
        FIG_HIST,
        SAR_COLOR_EMPHASIS,
        SAR_COLOR_REFERENCE,
        configure_preslide_asymmetry_mpl,
        finalize_sar_figure,
        plot_sar_median_line,
        plot_sar_step_histogram,
        style_sar_axes,
    )

    configure_preslide_asymmetry_mpl()
    reciprocal = directed.loc[directed["fid_home"] < directed["fid_work"]].copy()
    pot = reciprocal["symmetry_ratio_time"].astype(float)
    obs_ratio = exposure["symmetry_ratio_time"].astype(float)
    obs_flow = exposure["flow"].astype(float)

    pot_med = float(pot.median())
    pot_share90 = float((pot <= 0.90).mean())
    obs_wmed = weighted_quantile(obs_ratio, obs_flow, 0.50)
    obs_wshare90 = weighted_mean((obs_ratio <= 0.90).astype(float), obs_flow)
    slow10 = weighted_mean((exposure["directed_ratio_vs_reverse"] >= 1.10).astype(float), obs_flow)

    fig, ax = plt.subplots(figsize=FIG_HIST)
    plot_sar_step_histogram(
        ax,
        pot.to_numpy(),
        color=SAR_COLOR_REFERENCE,
        label="Potential pairs",
    )
    plot_sar_step_histogram(
        ax,
        obs_ratio.to_numpy(),
        weights=obs_flow.to_numpy(),
        color=SAR_COLOR_EMPHASIS,
        label="Observed flow",
    )
    plot_sar_median_line(ax, pot_med, color=SAR_COLOR_REFERENCE)
    plot_sar_median_line(ax, obs_wmed, color=SAR_COLOR_EMPHASIS)
    style_sar_axes(ax, ylabel="Pairs / commuters")
    ax.legend(frameon=False, loc="upper left")
    finalize_sar_figure(fig)
    fig.savefig(out_path, dpi=dpi, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return {
        "potential_median_symmetry_ratio": pot_med,
        "potential_share_symmetry_le_0_90": pot_share90,
        "observed_weighted_median_symmetry_ratio": obs_wmed,
        "observed_weighted_share_symmetry_le_0_90": obs_wshare90,
        "observed_weighted_share_actual_direction_slow_ge_1_10": slow10,
    }


def main(argv=None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--pairtime",
        default="data_work/raw_data/shenzhen_gridlevel/fishnet1000_pairtime_hat.csv",
    )
    parser.add_argument(
        "--basemap",
        default="data_work/raw_data/shenzhen_gridlevel/shp/shenzhen_fishnet1000_info_combined.shp",
    )
    parser.add_argument(
        "--flow-dta",
        default="data_work/raw_data/resid_work_flow_grid500.dta",
    )
    parser.add_argument(
        "--output-root",
        default="data_work/outputs/shenzhen_gridlevel_spatial_asymmetry_v1",
    )
    args = parser.parse_args(argv)

    output_root = Path(args.output_root)
    ensure_dirs(output_root)
    pairtime_path = Path(args.pairtime)
    basemap_path = Path(args.basemap)
    flow_path = Path(args.flow_dta)

    basemap = load_basemap(basemap_path)
    directed = build_directed_pairtime(pairtime_path)
    pairtime_ids = set(directed["fid_home"].unique()) | set(directed["fid_work"].unique())

    unweighted_cell = build_unweighted_cell_metrics(directed)
    exposure, crosswalk, coverage = build_flow_exposure(flow_path, basemap, directed, output_root)
    flow_cell = build_flow_cell_metrics(exposure)

    grid = basemap.merge(unweighted_cell, on="fid1000", how="left").merge(flow_cell, on="fid1000", how="left")
    grid["in_pairtime_matrix"] = grid["fid1000"].isin(pairtime_ids)
    flow_fids = set(exposure["resi_fid1000"].unique()) | set(exposure["work_fid1000"].unique())
    grid["has_observed_flow"] = grid["fid1000"].isin(flow_fids)

    basemap_audit = {
        "basemap_path": str(basemap_path),
        "basemap_rows": int(len(basemap)),
        "basemap_crs": str(basemap.crs),
        "basemap_geometry_types": basemap.geometry.geom_type.value_counts().to_dict(),
        "basemap_bounds": [float(x) for x in basemap.total_bounds],
        "fid_column": "TARGET_FID renamed to fid1000",
        "basemap_unique_fids": int(basemap["fid1000"].nunique()),
        "pairtime_active_fids": int(len(pairtime_ids)),
        "basemap_pairtime_overlap_fids": int(len(set(basemap["fid1000"]) & pairtime_ids)),
        "pairtime_fids_missing_from_basemap": int(len(pairtime_ids - set(basemap["fid1000"]))),
        "basemap_fids_not_in_pairtime": int(len(set(basemap["fid1000"]) - pairtime_ids)),
    }
    write_json(output_root / "metrics" / "grid1000_basemap_audit.json", basemap_audit)
    write_json(output_root / "metrics" / "flow_crosswalk_coverage.json", coverage)

    unweighted_summary = summarize_unweighted_reciprocal(directed)
    weighted_rows = [
        describe_weighted_sample(exposure, "flow", "observed_flow"),
        describe_weighted_sample(exposure.loc[exposure["num_asset_car"] > 0].copy(), "num_asset_car", "car_asset_holders"),
    ]
    weighted_summary = pd.concat([unweighted_summary, pd.DataFrame(weighted_rows)], ignore_index=True, sort=False)
    weighted_summary.to_csv(output_root / "metrics" / "flow_weighted_asymmetry_summary.csv", index=False)

    exposure.to_csv(output_root / "data" / "flow_grid1000_od_asymmetry.csv", index=False)
    write_spatial_group_summaries(exposure, basemap, output_root)
    exposure.sort_values(["flow", "directed_ratio_vs_reverse"], ascending=[False, False]).head(1000).to_csv(
        output_root / "data" / "top_flow_od_pairs_by_volume.csv",
        index=False,
    )
    exposure.sort_values("directed_ratio_vs_reverse", ascending=False).head(1000).to_csv(
        output_root / "data" / "top_flow_od_pairs_by_directional_disadvantage.csv",
        index=False,
    )
    grid.drop(columns="geometry").to_csv(output_root / "data" / "grid1000_cell_metrics.csv", index=False)
    grid.to_file(output_root / "data" / "grid1000_spatial_metrics.gpkg", layer="grid1000_metrics", driver="GPKG")

    cell_summary_cols = [
        "potential_destinations",
        "unweighted_mean_symmetry_ratio",
        "unweighted_share_symmetry_le_0_90",
        "unweighted_mean_signed_log_ratio",
        "origin_flow",
        "origin_mean_signed_log_ratio_floww",
        "origin_share_actual_slow_ge_1_10_floww",
        "workplace_flow",
        "workplace_mean_inbound_signed_log_ratio_floww",
        "workplace_share_inbound_slow_ge_1_10_floww",
    ]
    cell_summary = []
    for col in cell_summary_cols:
        if col in grid.columns:
            s = grid[col].replace([np.inf, -np.inf], np.nan).dropna()
            if not s.empty:
                cell_summary.append(
                    {
                        "variable": col,
                        "count": int(s.count()),
                        "mean": float(s.mean()),
                        "p10": float(s.quantile(0.10)),
                        "p50": float(s.quantile(0.50)),
                        "p90": float(s.quantile(0.90)),
                        "min": float(s.min()),
                        "max": float(s.max()),
                    }
                )
    pd.DataFrame(cell_summary).to_csv(output_root / "metrics" / "grid1000_cell_metric_summary.csv", index=False)

    plot_coverage_map(grid, output_root / "figures" / "figure_grid1000_pairtime_flow_coverage.png")
    plot_polygon_map(
        grid,
        "unweighted_share_symmetry_le_0_90",
        output_root / "figures" / "figure_grid1000_unweighted_share_symmetry_le_0_90.png",
        "Potential-Destination Asymmetry Share By Origin Cell",
        "YlOrRd",
        vmin=0,
        vmax=1,
    )
    plot_polygon_map(
        grid,
        "unweighted_mean_signed_log_ratio",
        output_root / "figures" / "figure_grid1000_unweighted_outbound_disadvantage.png",
        "Unweighted Outbound Directional Disadvantage",
        "RdBu_r",
        diverging=True,
    )
    plot_polygon_map(
        grid,
        "origin_mean_signed_log_ratio_floww",
        output_root / "figures" / "figure_grid1000_flow_origin_disadvantage.png",
        "Observed-Flow Weighted Residence-Origin Disadvantage",
        "RdBu_r",
        diverging=True,
    )
    plot_polygon_map(
        grid,
        "workplace_mean_inbound_signed_log_ratio_floww",
        output_root / "figures" / "figure_grid1000_flow_workplace_inbound_disadvantage.png",
        "Observed-Flow Weighted Workplace-Inbound Disadvantage",
        "RdBu_r",
        diverging=True,
    )
    plot_polygon_map(
        grid,
        "origin_flow",
        output_root / "figures" / "figure_grid1000_origin_flow_volume.png",
        "Observed Commuter Flow By Residence Cell",
        "PuBu",
    )
    plot_polygon_map(
        grid,
        "workplace_flow",
        output_root / "figures" / "figure_grid1000_workplace_flow_volume.png",
        "Observed Commuter Flow By Workplace Cell",
        "PuRd",
    )
    plot_threshold_comparison(
        weighted_summary,
        output_root / "figures" / "figure_grid1000_flow_weighted_threshold_comparison.png",
    )
    scatter_stats = plot_flow_exposure_asymmetry_scatter(
        exposure,
        output_root / "figures" / "figure_grid1000_flow_exposure_asymmetry_scatter.png",
    )
    hist_stats = plot_preslide_flow_ratio_histogram(
        directed,
        exposure,
        output_root / "figures" / "figure_grid1000_flow_ratio_histogram.png",
    )
    preslide_dir = Path(__file__).resolve().parents[2].parent / "Documents" / "PreSlides" / "figs"
    preslide_dir.mkdir(parents=True, exist_ok=True)
    plot_preslide_flow_ratio_histogram(
        directed,
        exposure,
        preslide_dir / "pat_sz_flow_ratio_hist.png",
        dpi=300,
    )
    write_json(output_root / "metrics" / "flow_exposure_scatter_stats.json", scatter_stats)
    write_json(output_root / "metrics" / "flow_ratio_histogram_stats.json", hist_stats)


if __name__ == "__main__":
    main()
