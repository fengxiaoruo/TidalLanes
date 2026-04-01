"""
Generate paper-ready figures and summary tables requested in Documents/ToDoList.docx.

This script is intentionally standalone: it reads an existing versioned output folder
and writes new paper assets without changing the main pipeline.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

paper_cache_root = ROOT / "Documents" / "todo_outputs"
paper_cache_root.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("MPLCONFIGDIR", str((paper_cache_root / ".mpl_cache").resolve()))
os.environ.setdefault("XDG_CACHE_HOME", str((paper_cache_root / ".cache").resolve()))

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.colors import Normalize

from data_work.src.diagnostics.mpl_setup import ensure_mpl_cache


CITY_CENTER_LON = 116.397389
CITY_CENTER_LAT = 39.908722


def parse_args():
    parser = argparse.ArgumentParser(description="Generate todo-list paper assets from an existing output version.")
    parser.add_argument("--version-id", default="raw_rebuild_validation", help="Version folder under output-dir.")
    parser.add_argument("--output-dir", default="data_work/outputs", help="Base output directory.")
    parser.add_argument("--paper-dir", default="Documents/todo_outputs", help="Where to write paper assets.")
    return parser.parse_args()


def weighted_mean(values: pd.Series, weights: pd.Series) -> float:
    mask = values.notna() & weights.notna() & (weights > 0)
    if not mask.any():
        return np.nan
    return float(np.average(values.loc[mask], weights=weights.loc[mask]))


def save_fig(fig: plt.Figure, out_dir: Path, stem: str):
    out_dir.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_dir / f"{stem}.png", dpi=300, bbox_inches="tight")
    fig.savefig(out_dir / f"{stem}.pdf", dpi=300, bbox_inches="tight")
    plt.close(fig)


def load_inputs(version_root: Path):
    data_dir = version_root / "data"
    grid = gpd.read_parquet(data_dir / "grid_square_master.parquet")
    grid_res = pd.read_csv(data_dir / "grid_residents_square.csv")
    grid_jobs = pd.read_csv(data_dir / "grid_jobs_square.csv")
    speed = pd.read_parquet(data_dir / "centerline_speed_master.parquet")
    asym = pd.read_parquet(data_dir / "centerline_asymmetry_table.parquet")
    cl_dir = gpd.read_parquet(data_dir / "centerline_dir_master.parquet")
    cl = gpd.read_parquet(data_dir / "centerline_master.parquet")
    lanes = pd.read_parquet(data_dir / "centerline_lane_master.parquet")
    return grid, grid_res, grid_jobs, speed, asym, cl_dir, cl, lanes


def prepare_grid_table(grid: gpd.GeoDataFrame, grid_res: pd.DataFrame, grid_jobs: pd.DataFrame) -> gpd.GeoDataFrame:
    g = grid.merge(grid_res, on="grid_id", how="left").merge(grid_jobs, on="grid_id", how="left")
    g["residents"] = pd.to_numeric(g["residents"], errors="coerce").fillna(0.0)
    g["jobs"] = pd.to_numeric(g["jobs"], errors="coerce").fillna(0.0)
    g["area_km2"] = pd.to_numeric(g["area_km2"], errors="coerce")
    center = gpd.GeoSeries([gpd.points_from_xy([CITY_CENTER_LON], [CITY_CENTER_LAT], crs="EPSG:4326")[0]], crs="EPSG:4326").to_crs(3857).iloc[0]
    g_3857 = g.to_crs(3857)
    g["dist_center_km"] = g_3857.geometry.centroid.distance(center) / 1000.0
    return g


def plot_spatial_distribution(grid: gpd.GeoDataFrame, out_dir: Path):
    fig, axes = plt.subplots(1, 2, figsize=(13, 6))
    for ax, col, title, cmap in [
        (axes[0], "residents", "Residents", plt.cm.Blues),
        (axes[1], "jobs", "Jobs", plt.cm.Reds),
    ]:
        vals = pd.to_numeric(grid[col], errors="coerce").fillna(0.0)
        pct = vals.rank(method="average", pct=True)
        pct = np.where(vals > 0, pct, 0.0)
        grid.assign(_pct=pct).plot(
            column="_pct",
            cmap=cmap,
            linewidth=0.0,
            ax=ax,
            legend=True,
            legend_kwds={"shrink": 0.75, "label": f"{title} percentile"},
            vmin=0.0,
            vmax=1.0,
        )
        ax.set_title(title, fontsize=13)
        ax.set_axis_off()
    fig.suptitle("Spatial Distribution of Residents and Jobs", fontsize=16)
    save_fig(fig, out_dir, "figure1_spatial_distribution_residents_jobs")


def summarize_speed_by_hour(speed: pd.DataFrame) -> pd.DataFrame:
    use = speed.copy()
    use["hour_of_day"] = pd.to_numeric(use["hour_of_day"], errors="coerce")
    use["cl_speed_kmh"] = pd.to_numeric(use["cl_speed_kmh"], errors="coerce")
    use["n_obs"] = pd.to_numeric(use["n_obs"], errors="coerce").fillna(0.0)
    use["cl_len_m"] = pd.to_numeric(use["cl_len_m"], errors="coerce").fillna(0.0)
    use = use.loc[use["hour_of_day"].notna() & use["cl_speed_kmh"].notna()].copy()
    use["weight"] = use["n_obs"] * use["cl_len_m"]
    out = (
        use.groupby(["weekday_label", "hour_of_day"], as_index=False)
        .apply(lambda x: pd.Series({"avg_speed_kmh": weighted_mean(x["cl_speed_kmh"], x["weight"])}))
        .reset_index(drop=True)
        .sort_values(["weekday_label", "hour_of_day"])
    )
    return out


def plot_speed_by_hour(speed_hour: pd.DataFrame, out_dir: Path):
    fig, axes = plt.subplots(1, 2, figsize=(13, 5), sharey=True)
    for ax, label, title in [
        (axes[0], "weekday", "Weekdays"),
        (axes[1], "weekend", "Weekends"),
    ]:
        sub = speed_hour.loc[speed_hour["weekday_label"] == label]
        ax.plot(sub["hour_of_day"], sub["avg_speed_kmh"], color="#1f77b4", linewidth=2.5)
        ax.set_title(title, fontsize=13)
        ax.set_xlabel("Hour of day")
        ax.grid(alpha=0.2)
        ax.set_xlim(0, 23)
        ax.set_xticks(range(0, 24, 3))
    axes[0].set_ylabel("Average speed (km/h)")
    fig.suptitle("Average Commuting Speed by Hour", fontsize=16)
    save_fig(fig, out_dir, "figure2_average_commuting_speed_by_hour")


def summarize_asymmetry_by_hour(speed: pd.DataFrame) -> pd.DataFrame:
    use = speed.copy()
    use = use.loc[use["dir"].isin(["AB", "BA"])].copy()
    use["hour_of_day"] = pd.to_numeric(use["hour_of_day"], errors="coerce")
    use["cl_speed_kmh"] = pd.to_numeric(use["cl_speed_kmh"], errors="coerce")
    use["cl_len_m"] = pd.to_numeric(use["cl_len_m"], errors="coerce")
    use = use.loc[use["hour_of_day"].notna() & use["cl_speed_kmh"].notna()].copy()
    piv = (
        use.pivot_table(
            index=["cline_id", "weekday_label", "hour_of_day"],
            columns="dir",
            values="cl_speed_kmh",
            aggfunc="mean",
        )
        .reset_index()
    )
    len_ref = use.groupby("cline_id", as_index=False)["cl_len_m"].mean()
    piv = piv.merge(len_ref, on="cline_id", how="left")
    piv = piv.loc[piv["AB"].notna() & piv["BA"].notna()].copy()
    denom = piv[["AB", "BA"]].max(axis=1)
    piv["asym_ratio"] = np.where(denom > 0, (piv["AB"] - piv["BA"]).abs() / denom, np.nan)
    out = (
        piv.groupby(["weekday_label", "hour_of_day"], as_index=False)
        .apply(lambda x: pd.Series({"asym_ratio": weighted_mean(x["asym_ratio"], x["cl_len_m"])}))
        .reset_index(drop=True)
        .sort_values(["weekday_label", "hour_of_day"])
    )
    return out


def plot_asymmetry_by_hour(asym_hour: pd.DataFrame, out_dir: Path):
    fig, axes = plt.subplots(1, 2, figsize=(13, 5), sharey=True)
    for ax, label, title in [
        (axes[0], "weekday", "Weekdays"),
        (axes[1], "weekend", "Weekends"),
    ]:
        sub = asym_hour.loc[asym_hour["weekday_label"] == label]
        ax.plot(sub["hour_of_day"], sub["asym_ratio"], color="#d95f02", linewidth=2.5)
        ax.set_title(title, fontsize=13)
        ax.set_xlabel("Hour of day")
        ax.grid(alpha=0.2)
        ax.set_xlim(0, 23)
        ax.set_xticks(range(0, 24, 3))
    axes[0].set_ylabel("Directional asymmetry ratio")
    fig.suptitle("Tidal Commuting by Hour", fontsize=16)
    save_fig(fig, out_dir, "figure3_tidal_commuting_by_hour")


def plot_spatial_tidal_distribution(asym: pd.DataFrame, cl: gpd.GeoDataFrame, out_dir: Path):
    use = asym.copy()
    use["ratio"] = pd.to_numeric(use["ratio"], errors="coerce")
    use["asym_ratio"] = 1.0 - use["ratio"]
    use = use.loc[use["peak"].isin(["AM", "PM"]) & use["asym_ratio"].notna() & (~use["miss_one_dir"].fillna(True))].copy()
    geo = cl[["cline_id", "geometry"]].merge(use[["cline_id", "peak", "asym_ratio"]], on="cline_id", how="inner")
    geo = gpd.GeoDataFrame(geo, geometry="geometry", crs=cl.crs)
    vmax = float(geo["asym_ratio"].quantile(0.99)) if len(geo) else 0.3
    fig, axes = plt.subplots(1, 2, figsize=(13, 6))
    for ax, peak in zip(axes, ["AM", "PM"]):
        sub = geo.loc[geo["peak"] == peak].copy()
        if len(sub):
            sub.plot(
                column="asym_ratio",
                cmap="magma",
                linewidth=0.8,
                ax=ax,
                legend=True,
                legend_kwds={"shrink": 0.75, "label": "1 - min(speedAB,speedBA)/max(...)"},
                norm=Normalize(vmin=0.0, vmax=vmax),
            )
        ax.set_title(f"{peak} peak", fontsize=13)
        ax.set_axis_off()
    fig.suptitle("Spatial Distribution of Tidal Commuting", fontsize=16)
    save_fig(fig, out_dir, "figure4_spatial_distribution_tidal_commuting")


def panel_summary(series: pd.Series) -> dict[str, float]:
    s = pd.to_numeric(series, errors="coerce")
    return {
        "Mean": float(s.mean()),
        "SD": float(s.std()),
        "Median": float(s.median()),
    }


def build_summary_table(
    grid: gpd.GeoDataFrame,
    cl: gpd.GeoDataFrame,
    lanes: pd.DataFrame,
    speed: pd.DataFrame,
    asym: pd.DataFrame,
) -> pd.DataFrame:
    panel_a = {
        "Residents": panel_summary(grid["residents"]),
        "Workers": panel_summary(grid["jobs"]),
        "Area (km2)": panel_summary(grid["area_km2"]),
        "Distance to city center (km)": panel_summary(grid["dist_center_km"]),
    }

    lane_by_cline = (
        lanes.assign(
            lane_est=lambda x: pd.to_numeric(x["lane_est_length_weighted"], errors="coerce")
            .combine_first(pd.to_numeric(x["lane_est_raw_weighted"], errors="coerce"))
        )
        .groupby("cline_id", as_index=False)["lane_est"]
        .mean()
    )
    speed_use = speed.copy()
    speed_use["cl_speed_kmh"] = pd.to_numeric(speed_use["cl_speed_kmh"], errors="coerce")
    speed_use["n_obs"] = pd.to_numeric(speed_use["n_obs"], errors="coerce").fillna(0.0)
    speed_all = speed_use.groupby("cline_id", as_index=False).apply(
        lambda x: pd.Series({"avg_speed_kmh": weighted_mean(x["cl_speed_kmh"], x["n_obs"])})
    ).reset_index(drop=True)
    speed_am = speed_use.loc[speed_use["period"] == "AM"].groupby("cline_id", as_index=False).apply(
        lambda x: pd.Series({"am_speed_kmh": weighted_mean(x["cl_speed_kmh"], x["n_obs"])})
    ).reset_index(drop=True)
    speed_pm = speed_use.loc[speed_use["period"] == "PM"].groupby("cline_id", as_index=False).apply(
        lambda x: pd.Series({"pm_speed_kmh": weighted_mean(x["cl_speed_kmh"], x["n_obs"])})
    ).reset_index(drop=True)
    asym_use = asym.copy()
    asym_use["ratio"] = pd.to_numeric(asym_use["ratio"], errors="coerce")
    asym_use["asym_ratio"] = 1.0 - asym_use["ratio"]
    asym_by_cline = asym_use.groupby("cline_id", as_index=False)["asym_ratio"].mean()

    seg = cl[["cline_id", "length_m"]].merge(lane_by_cline, on="cline_id", how="left")
    seg = seg.merge(speed_all, on="cline_id", how="left")
    seg = seg.merge(speed_am, on="cline_id", how="left")
    seg = seg.merge(speed_pm, on="cline_id", how="left")
    seg = seg.merge(asym_by_cline, on="cline_id", how="left")

    panel_b = {
        "Length (m)": panel_summary(seg["length_m"]),
        "Number of lanes": panel_summary(seg["lane_est"]),
        "Average speed (km/h)": panel_summary(seg["avg_speed_kmh"]),
        "Morning peak speed (km/h)": panel_summary(seg["am_speed_kmh"]),
        "Evening peak speed (km/h)": panel_summary(seg["pm_speed_kmh"]),
        "Asymmetry ratio": panel_summary(seg["asym_ratio"]),
    }

    rows = []
    for panel_name, panel_data in [("Panel A. Grid-level", panel_a), ("Panel B. Segment-level", panel_b)]:
        for var, stats in panel_data.items():
            rows.append({"Panel": panel_name, "Variable": var, **stats})
    return pd.DataFrame(rows)


def write_summary_table(table: pd.DataFrame, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)
    table.to_csv(out_dir / "table1_summary_statistics.csv", index=False)

    lines = [
        "\\begin{table}[!htbp]",
        "\\centering",
        "\\caption{Summary Statistics}",
        "\\begin{tabular}{llrrr}",
        "\\toprule",
        "Panel & Variable & Mean & SD & Median \\\\",
        "\\midrule",
    ]
    current_panel = None
    for row in table.itertuples(index=False):
        panel = row.Panel if row.Panel != current_panel else ""
        current_panel = row.Panel
        lines.append(
            f"{panel} & {row.Variable} & {row.Mean:,.3f} & {row.SD:,.3f} & {row.Median:,.3f} \\\\"
        )
    lines += ["\\bottomrule", "\\end{tabular}", "\\end{table}"]
    (out_dir / "table1_summary_statistics.tex").write_text("\n".join(lines), encoding="utf-8")


def main():
    args = parse_args()
    version_root = Path(args.output_dir) / args.version_id
    paper_root = Path(args.paper_dir)
    fig_dir = paper_root / "figs"
    table_dir = paper_root / "tables"
    data_dir = paper_root / "data"
    ensure_mpl_cache(str(paper_root))

    grid, grid_res, grid_jobs, speed, asym, cl_dir, cl, lanes = load_inputs(version_root)
    grid = prepare_grid_table(grid, grid_res, grid_jobs)
    speed_hour = summarize_speed_by_hour(speed)
    asym_hour = summarize_asymmetry_by_hour(speed)
    summary_table = build_summary_table(grid, cl, lanes, speed, asym)

    data_dir.mkdir(parents=True, exist_ok=True)
    speed_hour.to_csv(data_dir / "figure2_speed_by_hour.csv", index=False)
    asym_hour.to_csv(data_dir / "figure3_asymmetry_by_hour.csv", index=False)

    plot_spatial_distribution(grid, fig_dir)
    plot_speed_by_hour(speed_hour, fig_dir)
    plot_asymmetry_by_hour(asym_hour, fig_dir)
    plot_spatial_tidal_distribution(asym, cl, fig_dir)
    write_summary_table(summary_table, table_dir)

    print(f"[todo-paper] wrote figures to {fig_dir}")
    print(f"[todo-paper] wrote tables to {table_dir}")
    print(f"[todo-paper] wrote intermediate csvs to {data_dir}")


if __name__ == "__main__":
    main()
