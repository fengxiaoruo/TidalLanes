"""
Plot speed diagnostics for a versioned run.
"""

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


ROOT = Path(__file__).resolve().parents[2]
RAW_SPEED_PATH = ROOT / "raw_data" / "speed_Beijing_all_wgs84.csv"


def parse_args():
    parser = argparse.ArgumentParser(description="Plot speed diagnostics")
    parser.add_argument("--version-id", required=True, help="Version identifier under outputs/.")
    parser.add_argument("--output-dir", default="outputs", help="Base output directory.")
    return parser.parse_args()


def load_speed_raw():
    df = pd.read_csv(RAW_SPEED_PATH)
    df["speed"] = pd.to_numeric(df["speed"], errors="coerce")
    df["roadseg_id"] = df["roadseg_id"].astype("string")
    df["hour"] = pd.to_numeric(df["hour"], errors="coerce")
    df["hour_int"] = df["hour"].round().astype("Int64")
    df["dt"] = pd.to_datetime(df["hour_int"].astype(str), format="%Y%m%d%H%M", errors="coerce")
    df["hour_of_day"] = df["dt"].dt.hour
    df["is_weekday"] = df["dt"].dt.weekday < 5
    df["weekday_label"] = np.where(df["is_weekday"], "weekday", "weekend")
    return df


def load_inputs(version_root: Path):
    data_dir = version_root / "data"
    raw_segments = pd.read_parquet(data_dir / "raw_segment_master.parquet")
    match = pd.read_parquet(data_dir / "raw_to_centerline_match_master.parquet")
    cl_speed = pd.read_parquet(data_dir / "centerline_speed_master.parquet")
    return raw_segments, match, cl_speed


def prepare_raw_obs(raw_df, raw_segments, match):
    seg = raw_segments[["split_id", "raw_edge_id", "roadseg_id", "length_m"]].copy()
    seg["roadseg_id"] = seg["roadseg_id"].astype("string")
    seg["len_m"] = pd.to_numeric(seg["length_m"], errors="coerce")
    mm = match[["split_id", "raw_edge_id", "matched_final"]].copy()
    mm["matched_final"] = pd.to_numeric(mm["matched_final"], errors="coerce")
    seg = seg.merge(mm, on=["split_id", "raw_edge_id"], how="left")
    raw_obs = raw_df.merge(seg[["roadseg_id", "len_m", "matched_final"]], on="roadseg_id", how="left")
    raw_obs = raw_obs[(raw_obs["matched_final"] == 1) & raw_obs["len_m"].gt(0) & raw_obs["speed"].gt(0) & raw_obs["dt"].notna()].copy()
    return raw_obs


def length_weighted_mean(df, value_col, weight_col):
    val = pd.to_numeric(df[value_col], errors="coerce")
    wt = pd.to_numeric(df[weight_col], errors="coerce")
    mask = val.notna() & wt.gt(0)
    if not mask.any():
        return np.nan
    return float((val[mask] * wt[mask]).sum() / wt[mask].sum())


def plot_hist_raw_all(path: Path, raw_obs: pd.DataFrame):
    fig, ax = plt.subplots(figsize=(6, 4), dpi=200)
    ax.hist(raw_obs["speed"].dropna(), bins=60, color="#3182bd", alpha=0.75, edgecolor="white")
    ax.set_title("Raw speed distribution")
    ax.set_xlabel("Speed (km/h)")
    ax.set_ylabel("Count")
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def plot_hist_raw_weekday_weekend(path: Path, raw_obs: pd.DataFrame):
    fig, ax = plt.subplots(figsize=(6, 4), dpi=200)
    for label, color in [("weekday", "#1f77b4"), ("weekend", "#ff7f0e")]:
        sub = raw_obs[raw_obs["weekday_label"] == label]["speed"].dropna()
        ax.hist(sub, bins=60, density=True, histtype="step", linewidth=1.5, label=label, color=color)
    ax.set_title("Raw speed distribution by weekday/weekend")
    ax.set_xlabel("Speed (km/h)")
    ax.set_ylabel("Density")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def plot_hist_raw_vs_centerline(path: Path, raw_obs: pd.DataFrame, cl_speed: pd.DataFrame):
    raw_speed = raw_obs["speed"].dropna()
    cl_speed_vals = pd.to_numeric(cl_speed["cl_speed_kmh"], errors="coerce").dropna()
    vmax = max(raw_speed.quantile(0.99), cl_speed_vals.quantile(0.99))
    bins = np.linspace(0, vmax, 60)
    fig, ax = plt.subplots(figsize=(6, 4), dpi=200)
    ax.hist(raw_speed, bins=bins, density=True, histtype="step", linewidth=1.5, label="raw (segment)", color="#1f77b4")
    ax.hist(cl_speed_vals, bins=bins, density=True, histtype="step", linewidth=1.5, label="centerline (time-weighted)", color="#ff7f0e")
    ax.set_title("Speed distribution: raw vs centerline")
    ax.set_xlabel("Speed (km/h)")
    ax.set_ylabel("Density")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def plot_diurnal(df_in: pd.DataFrame, title: str, path: Path):
    fig, ax = plt.subplots(figsize=(7, 4), dpi=200)
    for label, color in [("weekday", "#1f77b4"), ("weekend", "#ff7f0e")]:
        sub = df_in[df_in["weekday_label"] == label]
        ax.plot(sub["hour_of_day"], sub["mean_speed_w"], marker="o", label=label, color=color, linewidth=1.5, markersize=4)
    ax.axvspan(7, 9, color="#9ecae1", alpha=0.2)
    ax.axvspan(17, 19, color="#fdae6b", alpha=0.2)
    ax.set_title(title)
    ax.set_xlabel("Hour of day")
    ax.set_ylabel("Mean speed (weighted, km/h)")
    ax.set_xticks(range(0, 24, 2))
    ax.grid(True, alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def plot_diurnal_combined(path: Path, raw_hour: pd.DataFrame, cl_hour: pd.DataFrame):
    fig, ax = plt.subplots(figsize=(8, 4), dpi=200)
    ax.axvspan(7, 9, color="#9ecae1", alpha=0.2, label="AM peak")
    ax.axvspan(17, 19, color="#fdae6b", alpha=0.2, label="PM peak")
    for label, color, df_plot, prefix in [
        ("raw weekday", "#08519c", raw_hour, "weekday"),
        ("raw weekend", "#6baed6", raw_hour, "weekend"),
        ("center weekday", "#b35806", cl_hour, "weekday"),
        ("center weekend", "#f1a340", cl_hour, "weekend"),
    ]:
        sub = df_plot[df_plot["weekday_label"] == prefix]
        ax.plot(sub["hour_of_day"], sub["mean_speed_w"], marker="o", label=label, color=color, linewidth=1.5, markersize=4)
    ax.set_title("Speed of a day: raw vs centerline")
    ax.set_xlabel("Hour of day")
    ax.set_ylabel("Mean speed (weighted, km/h)")
    ax.set_xticks(range(0, 24, 2))
    ax.grid(True, alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def run(version_id: str, output_dir: str):
    version_root = Path(output_dir) / version_id
    figures_dir = version_root / "figures"
    figures_dir.mkdir(parents=True, exist_ok=True)

    raw_df = load_speed_raw()
    raw_segments, match, cl_speed = load_inputs(version_root)
    raw_obs = prepare_raw_obs(raw_df, raw_segments, match)

    raw_hour = (
        raw_obs.groupby(["weekday_label", "hour_of_day"], as_index=False)
        .apply(lambda d: pd.Series({"mean_speed_w": length_weighted_mean(d, "speed", "len_m")}))
        .reset_index(drop=True)
    )
    cl_hour = (
        cl_speed.groupby(["weekday_label", "hour_of_day"], as_index=False)
        .apply(lambda d: pd.Series({"mean_speed_w": length_weighted_mean(d, "cl_speed_kmh", "cl_len_m")}))
        .reset_index(drop=True)
    )

    plot_hist_raw_all(figures_dir / "hist_speed_raw_all.png", raw_obs)
    plot_hist_raw_weekday_weekend(figures_dir / "hist_speed_raw_weekday_weekend.png", raw_obs)
    plot_hist_raw_vs_centerline(figures_dir / "hist_speed_raw_vs_centerline_all.png", raw_obs, cl_speed)
    plot_diurnal(raw_hour, "Raw speed (weekday vs weekend)", figures_dir / "diurnal_raw_weekday_weekend.png")
    plot_diurnal(cl_hour, "Centerline speed (time-weighted, weekday vs weekend)", figures_dir / "diurnal_centerline_weekday_weekend.png")
    plot_diurnal_combined(figures_dir / "diurnal_raw_centerline_combined.png", raw_hour, cl_hour)
    print(f"[plot_speed_diagnostics] saved figures to {figures_dir}")


def main():
    args = parse_args()
    run(args.version_id, args.output_dir)


if __name__ == "__main__":
    main()
