"""
Plot speed diagnostics for a versioned run.
"""

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


ROOT = Path(__file__).resolve().parents[2]
RAW_SPEED_PATH = ROOT / "raw_data" / "speed_Beijing_all_wgs84.csv"
BIN_WIDTH_MIN = 0.25
SCENARIO_SPECS = [
    ("FREE_WEEKEND", "weekend", "is_freeflow_2205"),
    ("FREE_WORK", "weekday", "is_freeflow_2205"),
    ("AMP_WEEKEND", "weekend", "is_am_peak"),
    ("AMP_WORK", "weekday", "is_am_peak"),
    ("PMP_WEEKEND", "weekend", "is_pm_peak"),
    ("PMP_WORK", "weekday", "is_pm_peak"),
]
ROAD_CLASS_LABELS = {
    2: "expressway",
    3: "arterial",
    4: "secondary",
}
ROAD_CLASS_COLORS = {
    "expressway": "#08306b",
    "arterial": "#2171b5",
    "secondary": "#41ab5d",
    "other": "#969696",
}


def parse_args():
    parser = argparse.ArgumentParser(description="Plot speed diagnostics")
    parser.add_argument("--version-id", required=True, help="Version identifier under outputs/.")
    parser.add_argument("--output-dir", default="outputs", help="Base output directory.")
    return parser.parse_args()


def resolve_raw_speed_path(version_root: Path) -> Path:
    snap = version_root / "config_snapshot.stage03.json"
    if snap.exists():
        payload = json.loads(snap.read_text(encoding="utf-8"))
        cfg_path = payload.get("raw_speed_path")
        if cfg_path:
            p = Path(cfg_path)
            if not p.is_absolute():
                p = Path.cwd() / p
            if p.exists():
                return p
    return RAW_SPEED_PATH


def load_speed_raw(version_root: Path):
    speed_path = resolve_raw_speed_path(version_root)
    df = pd.read_csv(speed_path)
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
    cl_dir = pd.read_parquet(data_dir / "centerline_dir_master.parquet")
    return raw_segments, match, cl_speed, cl_dir


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


def build_centerline_road_class(raw_segments: pd.DataFrame, match: pd.DataFrame, version_root: Path) -> pd.DataFrame:
    split = pd.read_parquet(version_root / "data" / "raw_split_centerline.parquet")
    seg = raw_segments[["split_id", "raw_edge_id", "road_class", "length_m"]].copy()
    seg["road_class"] = pd.to_numeric(seg["road_class"], errors="coerce")
    seg["length_m"] = pd.to_numeric(seg["length_m"], errors="coerce")
    mm = match[["split_id", "raw_edge_id", "matched_final"]].copy()
    mm["matched_final"] = pd.to_numeric(mm["matched_final"], errors="coerce")
    sp = split[["split_id", "raw_edge_id", "skel_dir", "cline_id", "dir", "matched"]].copy()
    sp["matched"] = pd.to_numeric(sp["matched"], errors="coerce")
    use = mm[mm["matched_final"] == 1].merge(sp[sp["matched"] == 1], on=["split_id", "raw_edge_id"], how="inner")
    use = use.merge(seg, on=["split_id", "raw_edge_id"], how="inner")
    use = use[use["road_class"].notna() & use["length_m"].gt(0)].copy()
    class_w = (
        use.groupby(["skel_dir", "cline_id", "dir", "road_class"], as_index=False)["length_m"]
        .sum()
        .sort_values(["skel_dir", "cline_id", "dir", "length_m"], ascending=[True, True, True, False])
        .drop_duplicates(["skel_dir", "cline_id", "dir"])
    )
    class_w["road_class_label"] = class_w["road_class"].map(ROAD_CLASS_LABELS).fillna("other")
    return class_w[["skel_dir", "cline_id", "dir", "road_class", "road_class_label"]]


def build_scenario_centerline_travel_times(cl_speed: pd.DataFrame, cl_dir: pd.DataFrame) -> pd.DataFrame:
    use = cl_speed.copy()
    use["cl_speed_kmh"] = pd.to_numeric(use["cl_speed_kmh"], errors="coerce")
    use["cl_len_m"] = pd.to_numeric(use["cl_len_m"], errors="coerce")
    use["total_dist_m"] = pd.to_numeric(use["total_dist_m"], errors="coerce")
    use["total_time_h"] = pd.to_numeric(use["total_time_h"], errors="coerce")
    use = use[use["has_valid_speed"] & use["cl_speed_kmh"].gt(0) & use["cl_len_m"].gt(0)].copy()
    if "sample_keep_baseline" in use.columns:
        use = use[use["sample_keep_baseline"].fillna(False)].copy()
    base_keys = cl_dir.loc[cl_dir["keep_baseline"].fillna(False), ["skel_dir", "cline_id", "dir"]].drop_duplicates()
    use = use.merge(base_keys.assign(_keep=1), on=["skel_dir", "cline_id", "dir"], how="inner")

    rows = []
    for scenario, day_label, peak_col in SCENARIO_SPECS:
        sub = use[(use["weekday_label"] == day_label) & use[peak_col].fillna(False)].copy()
        if sub.empty:
            continue
        agg = (
            sub.groupby(["skel_dir", "cline_id", "dir"], as_index=False)
            .agg(
                total_dist_m=("total_dist_m", "sum"),
                total_time_h=("total_time_h", "sum"),
                cl_len_m=("cl_len_m", "first"),
            )
        )
        agg["cl_speed_kmh"] = np.where(agg["total_time_h"] > 0, agg["total_dist_m"] / agg["total_time_h"], np.nan)
        agg["travel_time_min"] = (agg["cl_len_m"] / 1000.0) / agg["cl_speed_kmh"] * 60.0
        agg = agg[np.isfinite(agg["travel_time_min"]) & agg["travel_time_min"].gt(0)].copy()
        agg["scenario"] = scenario
        rows.append(agg)
    if not rows:
        raise ValueError("No centerline travel-time observations for scenario panels.")
    return pd.concat(rows, ignore_index=True)


def travel_time_display_cap(values: pd.Series, *, quantile: float = 0.995) -> float:
    positive = pd.to_numeric(values, errors="coerce").dropna()
    positive = positive[positive > 0]
    if positive.empty:
        return 30.0
    cap = float(positive.quantile(quantile))
    return max(cap, BIN_WIDTH_MIN * 4)


def plot_centerline_travel_time_scenarios(path: Path, tt_df: pd.DataFrame):
    tt_df = tt_df.copy()
    tt_df["road_class_label"] = tt_df["road_class_label"].fillna("other")
    x_cap = travel_time_display_cap(tt_df["travel_time_min"])
    bins = np.arange(0.0, x_cap + BIN_WIDTH_MIN, BIN_WIDTH_MIN)
    class_order = [c for c in ["expressway", "arterial", "secondary", "other"] if c in set(tt_df["road_class_label"])]

    fig, axes = plt.subplots(2, 3, figsize=(13.5, 7.2), dpi=220, sharex=True, sharey=False)
    axes = axes.ravel()
    for ax, (scenario, _, _) in zip(axes, SCENARIO_SPECS):
        sub = tt_df[(tt_df["scenario"] == scenario) & tt_df["travel_time_min"].le(x_cap)].copy()
        if sub.empty:
            ax.set_title(scenario)
            ax.text(0.5, 0.5, "No data", ha="center", va="center", transform=ax.transAxes)
            continue
        bottom = np.zeros(len(bins) - 1, dtype=float)
        n_total = len(sub)
        for label in class_order:
            vals = sub.loc[sub["road_class_label"] == label, "travel_time_min"].to_numpy(dtype=float)
            if len(vals) == 0:
                continue
            counts, _ = np.histogram(vals, bins=bins)
            density = counts / (n_total * BIN_WIDTH_MIN)
            ax.bar(
                bins[:-1],
                density,
                width=BIN_WIDTH_MIN,
                align="edge",
                bottom=bottom,
                color=ROAD_CLASS_COLORS.get(label, "#969696"),
                edgecolor="white",
                linewidth=0.35,
                label=label,
            )
            bottom += density
        ax.set_title(f"{scenario} (N={len(sub):,})")
        ax.set_xlim(0, x_cap)
        ax.grid(True, axis="y", alpha=0.25, linewidth=0.8)

    handles, labels = axes[0].get_legend_handles_labels()
    if handles:
        fig.legend(handles, labels, loc="upper center", ncol=len(class_order), frameon=False, bbox_to_anchor=(0.5, 1.02))
    fig.supxlabel("Centerline travel time, minutes")
    fig.supylabel("Density")
    fig.suptitle("Fine-bin histogram of centerline travel time by scenario", y=1.06, fontsize=13)
    excluded = int((tt_df["travel_time_min"] > x_cap).sum())
    note = (
        "One observation per baseline-kept directed centerline. "
        f"Travel time = centerline length / scenario time-weighted speed. "
        f"Bins are {BIN_WIDTH_MIN:g} minutes wide and stacked by road class. "
        f"x-axis truncated at the {99.5:.1f}th percentile ({x_cap:.2f} min; excludes {excluded:,} observations)."
    )
    fig.text(0.5, -0.02, note, ha="center", va="top", fontsize=9, color="#4b5563")
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


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

    raw_df = load_speed_raw(version_root)
    raw_segments, match, cl_speed, cl_dir = load_inputs(version_root)
    raw_obs = prepare_raw_obs(raw_df, raw_segments, match)
    road_class = build_centerline_road_class(raw_segments, match, version_root)
    tt_scenarios = build_scenario_centerline_travel_times(cl_speed, cl_dir)
    tt_scenarios = tt_scenarios.merge(road_class, on=["skel_dir", "cline_id", "dir"], how="left")
    tt_scenarios["road_class_label"] = tt_scenarios["road_class_label"].fillna("other")

    paper_appendix_dir = figures_dir / "paper_appendix"
    paper_appendix_dir.mkdir(parents=True, exist_ok=True)

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
    plot_centerline_travel_time_scenarios(
        figures_dir / "fig_centerline_travel_time_scenarios_finebin.png",
        tt_scenarios,
    )
    plot_centerline_travel_time_scenarios(
        paper_appendix_dir / "fig_centerline_travel_time_scenarios_finebin.png",
        tt_scenarios,
    )
    tt_scenarios.to_parquet(figures_dir / "centerline_travel_time_by_scenario.parquet", index=False)
    print(f"[plot_speed_diagnostics] saved figures to {figures_dir}")


def main():
    args = parse_args()
    run(args.version_id, args.output_dir)


if __name__ == "__main__":
    main()
