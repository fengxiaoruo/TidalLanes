from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_INPUT_DIR = PROJECT_ROOT / "data_work/raw_data/Shenzhen_Gaode"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data_work/outputs/shenzhen_liuxian_gaode_initial_v1"
TREATMENT_DIRECTION = "西向东"
PERIOD_ORDER = ["pre_20260302_20260308", "post_20260413_20260419"]
PERIOD_COLORS = {
    "pre_20260302_20260308": "#3b6ea8",
    "post_20260413_20260419": "#c44e52",
}
DAY_TYPE_ORDER = ["Weekday", "Weekend"]
WINDOWS = {
    "night_0000_0500": (0, 5 * 60),
    "am_peak_0730_0930": (7 * 60 + 30, 9 * 60 + 30),
    "midday_1200_1500": (12 * 60, 15 * 60),
    "afternoon15_1500_1600": (15 * 60, 16 * 60),
    "pm_broad_1630_2030": (16 * 60 + 30, 20 * 60 + 30),
    "pm_core_1730_2000": (17 * 60 + 30, 20 * 60),
}
REFERENCE_WINDOWS = ["night_0000_0500", "afternoon15_1500_1600", "midday_1200_1500"]
PEAK_WINDOWS = ["pm_core_1730_2000", "pm_broad_1630_2030", "am_peak_0730_0930"]
WINDOW_LABELS = {
    "night_0000_0500": "Night 00:00-05:00",
    "am_peak_0730_0930": "AM peak 07:30-09:30",
    "midday_1200_1500": "Midday 12:00-15:00",
    "afternoon15_1500_1600": "15:00-16:00",
    "pm_broad_1630_2030": "PM broad 16:30-20:30",
    "pm_core_1730_2000": "PM core 17:30-20:00",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build treatment-direction travel time profiles for Liuxian Avenue."
    )
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def configure_plot() -> None:
    plt.rcParams.update(
        {
            "font.sans-serif": [
                "PingFang SC",
                "Hiragino Sans GB",
                "Microsoft YaHei",
                "Noto Sans CJK SC",
                "Arial Unicode MS",
                "DejaVu Sans",
            ],
            "axes.unicode_minus": False,
            "font.size": 10,
            "axes.titlesize": 10,
            "axes.labelsize": 9,
            "legend.fontsize": 8,
            "figure.dpi": 220,
            "savefig.dpi": 220,
        }
    )


def ensure_output_dirs(output_dir: Path) -> dict[str, Path]:
    dirs = {
        "root": output_dir,
        "figures": output_dir / "figures",
        "data": output_dir / "data",
        "metrics": output_dir / "metrics",
    }
    for path in dirs.values():
        path.mkdir(parents=True, exist_ok=True)
    return dirs


def period_from_datetime(series: pd.Series) -> str:
    min_date = series.min().strftime("%Y%m%d")
    max_date = series.max().strftime("%Y%m%d")
    prefix = "pre" if series.min() < pd.Timestamp("2026-03-23") else "post"
    return f"{prefix}_{min_date}_{max_date}"


def load_one_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, encoding="utf-8-sig", dtype=str)
    for col in df.columns:
        df[col] = df[col].astype(str).str.strip()

    df["datetime"] = pd.to_datetime(
        df["日期"].str.zfill(8) + df["时间"].str.zfill(4),
        format="%Y%m%d%H%M",
        errors="raise",
    )
    df["period"] = period_from_datetime(df["datetime"])
    df["date"] = df["datetime"].dt.date.astype(str)
    df["dow"] = df["datetime"].dt.dayofweek
    df["dow_name"] = df["datetime"].dt.day_name()
    df["day_type"] = np.where(df["dow"] < 5, "Weekday", "Weekend")
    df["minute_of_day"] = df["datetime"].dt.hour * 60 + df["datetime"].dt.minute
    df["hour"] = df["datetime"].dt.hour
    df["speed_kmh"] = pd.to_numeric(df["速度"], errors="coerce")
    df["segment_length_m"] = pd.to_numeric(df["路段长度"], errors="coerce")
    df["source_file"] = path.name
    return df


def load_panel(input_dir: Path) -> pd.DataFrame:
    files = sorted(input_dir.glob("*.csv"))
    if not files:
        raise FileNotFoundError(f"No CSV files found under {input_dir}")
    panel = pd.concat([load_one_csv(path) for path in files], ignore_index=True)
    segment_sets = {
        period: set(group["路段编号"].unique()) for period, group in panel.groupby("period")
    }
    common_segments = set.intersection(*segment_sets.values())
    return panel[panel["路段编号"].isin(common_segments)].copy()


def build_treatment_corridor(panel: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    treatment = panel[panel["路段方向"] == TREATMENT_DIRECTION].copy()
    treatment["segment_travel_time_min"] = (
        treatment["segment_length_m"] / 1000.0 / treatment["speed_kmh"] * 60.0
    )

    segment_set = (
        treatment[
            [
                "period",
                "路段编号",
                "路段名称（唯一）",
                "路段方向",
                "路段长度",
                "路段起点坐标",
                "路段终点坐标",
            ]
        ]
        .drop_duplicates()
        .sort_values(["路段编号", "period"])
    )

    corridor = (
        treatment.groupby(
            [
                "period",
                "date",
                "dow",
                "dow_name",
                "day_type",
                "datetime",
                "minute_of_day",
                "hour",
            ],
            as_index=False,
        )
        .agg(
            travel_time_min=("segment_travel_time_min", "sum"),
            n_segments=("路段编号", "nunique"),
            total_length_m=("segment_length_m", "sum"),
        )
        .sort_values(["period", "datetime"])
    )

    expected_segments = treatment["路段编号"].nunique()
    corridor = corridor[corridor["n_segments"] == expected_segments].copy()
    corridor["corridor_speed_kmh"] = (
        corridor["total_length_m"] / 1000.0 / (corridor["travel_time_min"] / 60.0)
    )
    corridor["hour_float"] = corridor["minute_of_day"] / 60.0
    return corridor, segment_set


def sorted_periods(periods: pd.Series) -> list[str]:
    seen = set(periods.dropna().unique())
    ordered = [period for period in PERIOD_ORDER if period in seen]
    return ordered + sorted(seen - set(ordered))


def period_label(period: str) -> str:
    if period.startswith("pre"):
        return "Pre: 2026-03-02 to 2026-03-08"
    if period.startswith("post"):
        return "Post: 2026-04-13 to 2026-04-19"
    return period


def add_policy_window(ax) -> None:
    ax.axvspan(17.5, 20.0, color="#e9b7b7", alpha=0.22, linewidth=0)


def aggregate_profiles(corridor: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    profile_10min = (
        corridor.groupby(["period", "day_type", "minute_of_day"], as_index=False)
        .agg(
            mean_travel_time_min=("travel_time_min", "mean"),
            median_travel_time_min=("travel_time_min", "median"),
            mean_corridor_speed_kmh=("corridor_speed_kmh", "mean"),
            n_obs=("travel_time_min", "size"),
        )
        .sort_values(["period", "day_type", "minute_of_day"])
    )
    profile_10min["hour_float"] = profile_10min["minute_of_day"] / 60.0

    profile_hour = (
        corridor.groupby(["period", "day_type", "hour"], as_index=False)
        .agg(
            mean_travel_time_min=("travel_time_min", "mean"),
            median_travel_time_min=("travel_time_min", "median"),
            mean_corridor_speed_kmh=("corridor_speed_kmh", "mean"),
            n_obs=("travel_time_min", "size"),
        )
        .sort_values(["period", "day_type", "hour"])
    )
    return profile_10min, profile_hour


def compute_window_means(corridor: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for window_name, (lo, hi) in WINDOWS.items():
        sub = corridor[(corridor["minute_of_day"] >= lo) & (corridor["minute_of_day"] < hi)]
        grouped = (
            sub.groupby(["period", "date", "dow", "dow_name", "day_type"], as_index=False)
            .agg(
                travel_time_min=("travel_time_min", "mean"),
                corridor_speed_kmh=("corridor_speed_kmh", "mean"),
                n_obs=("travel_time_min", "size"),
            )
        )
        grouped["window"] = window_name
        rows.append(grouped)
    return pd.concat(rows, ignore_index=True)


def compute_reference_ratios(corridor: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    daily_windows = compute_window_means(corridor)
    wide = daily_windows.pivot_table(
        index=["period", "date", "dow", "dow_name", "day_type"],
        columns="window",
        values="travel_time_min",
        aggfunc="first",
    ).reset_index()
    wide.columns.name = None

    ratio_rows = []
    for peak in PEAK_WINDOWS:
        for ref in REFERENCE_WINDOWS:
            tmp = wide[
                ["period", "date", "dow", "dow_name", "day_type", peak, ref]
            ].dropna()
            tmp = tmp.rename(columns={peak: "peak_travel_time_min", ref: "reference_travel_time_min"})
            tmp["peak_window"] = peak
            tmp["reference_window"] = ref
            tmp["ratio"] = tmp["peak_travel_time_min"] / tmp["reference_travel_time_min"]
            tmp["log_difference"] = np.log(tmp["peak_travel_time_min"]) - np.log(
                tmp["reference_travel_time_min"]
            )
            ratio_rows.append(tmp)
    ratios = pd.concat(ratio_rows, ignore_index=True)

    summary = (
        ratios[ratios["day_type"] == "Weekday"]
        .groupby(["peak_window", "reference_window", "period"], as_index=False)
        .agg(
            mean_peak_travel_time_min=("peak_travel_time_min", "mean"),
            mean_reference_travel_time_min=("reference_travel_time_min", "mean"),
            mean_ratio=("ratio", "mean"),
            mean_log_difference=("log_difference", "mean"),
            n_days=("ratio", "size"),
        )
    )

    wide_summary = summary.pivot_table(
        index=["peak_window", "reference_window"],
        columns="period",
        values=[
            "mean_peak_travel_time_min",
            "mean_reference_travel_time_min",
            "mean_ratio",
            "mean_log_difference",
        ],
        aggfunc="first",
    )
    wide_summary.columns = [f"{metric}_{period}" for metric, period in wide_summary.columns]
    wide_summary = wide_summary.reset_index()
    if "mean_ratio_post_20260413_20260419" in wide_summary:
        wide_summary["post_minus_pre_ratio"] = (
            wide_summary["mean_ratio_post_20260413_20260419"]
            - wide_summary["mean_ratio_pre_20260302_20260308"]
        )
        wide_summary["post_over_pre_ratio"] = (
            wide_summary["mean_ratio_post_20260413_20260419"]
            / wide_summary["mean_ratio_pre_20260302_20260308"]
        )
    if "mean_log_difference_post_20260413_20260419" in wide_summary:
        wide_summary["post_minus_pre_log_difference"] = (
            wide_summary["mean_log_difference_post_20260413_20260419"]
            - wide_summary["mean_log_difference_pre_20260302_20260308"]
        )
    return ratios, wide_summary


def add_daily_reference_ratios(corridor: pd.DataFrame, reference_window: str) -> pd.DataFrame:
    lo, hi = WINDOWS[reference_window]
    refs = (
        corridor[(corridor["minute_of_day"] >= lo) & (corridor["minute_of_day"] < hi)]
        .groupby(["period", "date"], as_index=False)
        .agg(reference_travel_time_min=("travel_time_min", "mean"))
    )
    out = corridor.merge(refs, on=["period", "date"], how="left", validate="many_to_one")
    out["ratio_to_reference"] = out["travel_time_min"] / out["reference_travel_time_min"]
    out["log_minus_reference"] = np.log(out["travel_time_min"]) - np.log(
        out["reference_travel_time_min"]
    )
    out["reference_window"] = reference_window
    return out


def plot_travel_time_profile(
    df: pd.DataFrame,
    out_path: Path,
    *,
    x_col: str,
    y_col: str,
    title: str,
    y_label: str,
    point_marker: bool = False,
) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.2), sharex=True, sharey=True)
    for ax, day_type in zip(axes, DAY_TYPE_ORDER):
        add_policy_window(ax)
        for period in sorted_periods(df["period"]):
            sub = df[(df["day_type"] == day_type) & (df["period"] == period)].sort_values(x_col)
            if sub.empty:
                continue
            ax.plot(
                sub[x_col],
                sub[y_col],
                color=PERIOD_COLORS.get(period),
                linewidth=1.9,
                marker="o" if point_marker else None,
                markersize=2.8 if point_marker else 0,
                label=period_label(period),
            )
        ax.set_title(day_type)
        ax.set_xlabel("Hour of day")
        ax.grid(True, alpha=0.22, linewidth=0.7)
        ax.set_xlim(0, 23.99 if x_col == "hour_float" else 23)
        ax.set_xticks([0, 6, 12, 18, 24] if x_col == "hour_float" else [0, 6, 12, 18, 23])
    axes[0].set_ylabel(y_label)
    handles, labels = axes[0].get_legend_handles_labels()
    if handles:
        fig.legend(handles, labels, loc="upper center", ncol=2, frameon=False, bbox_to_anchor=(0.5, 1.08))
    fig.suptitle(title, y=1.1, fontsize=12)
    fig.text(
        0.5,
        -0.05,
        "Treatment direction: Liuxian Avenue west-to-east common segments. Shaded band: 17:30-20:00.",
        ha="center",
        va="top",
        fontsize=8.5,
        color="#4b5563",
    )
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)


def plot_reference_ratio_profile(corridor: pd.DataFrame, reference_window: str, out_path: Path) -> None:
    normalized = add_daily_reference_ratios(corridor, reference_window)
    profile = (
        normalized.groupby(["period", "day_type", "minute_of_day"], as_index=False)
        .agg(
            mean_ratio_to_reference=("ratio_to_reference", "mean"),
            n_obs=("ratio_to_reference", "size"),
        )
        .sort_values(["period", "day_type", "minute_of_day"])
    )
    profile["hour_float"] = profile["minute_of_day"] / 60.0
    plot_travel_time_profile(
        profile,
        out_path,
        x_col="hour_float",
        y_col="mean_ratio_to_reference",
        title=f"Treatment Direction Travel Time Relative to {WINDOW_LABELS[reference_window]}",
        y_label="Travel time ratio",
    )


def plot_daily_pm_core(corridor: pd.DataFrame, out_path: Path) -> None:
    lo, hi = WINDOWS["pm_core_1730_2000"]
    daily = (
        corridor[
            (corridor["day_type"] == "Weekday")
            & (corridor["minute_of_day"] >= lo)
            & (corridor["minute_of_day"] < hi)
        ]
        .groupby(["period", "dow", "dow_name"], as_index=False)
        .agg(mean_travel_time_min=("travel_time_min", "mean"))
        .sort_values(["period", "dow"])
    )
    fig, ax = plt.subplots(figsize=(7.4, 4.2))
    for period in sorted_periods(daily["period"]):
        sub = daily[daily["period"] == period]
        ax.plot(
            sub["dow"],
            sub["mean_travel_time_min"],
            color=PERIOD_COLORS.get(period),
            marker="o",
            linewidth=1.9,
            label=period_label(period),
        )
    ax.set_xticks(range(5), ["Mon", "Tue", "Wed", "Thu", "Fri"])
    ax.set_ylabel("Mean travel time, minutes")
    ax.set_title("Weekday PM Core Travel Time by Weekday")
    ax.grid(True, axis="y", alpha=0.25)
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)


def plot_peak_ratio_bars(ratio_summary: pd.DataFrame, out_path: Path) -> None:
    use = ratio_summary[ratio_summary["peak_window"] == "pm_core_1730_2000"].copy()
    refs = ["night_0000_0500", "afternoon15_1500_1600", "midday_1200_1500"]
    use = use.set_index("reference_window").reindex(refs).reset_index()
    x = np.arange(len(use))
    width = 0.34
    fig, ax = plt.subplots(figsize=(7.8, 4.2))
    ax.bar(
        x - width / 2,
        use["mean_ratio_pre_20260302_20260308"],
        width,
        label="Pre",
        color=PERIOD_COLORS["pre_20260302_20260308"],
        alpha=0.86,
    )
    ax.bar(
        x + width / 2,
        use["mean_ratio_post_20260413_20260419"],
        width,
        label="Post",
        color=PERIOD_COLORS["post_20260413_20260419"],
        alpha=0.86,
    )
    ax.set_xticks(x, ["Night\n00:00-05:00", "15:00\n15:00-16:00", "Midday\n12:00-15:00"])
    ax.set_ylabel("PM core travel time / reference travel time")
    ax.set_title("Weekday PM Core Travel Time Relative to Same-Day Reference")
    ax.grid(True, axis="y", alpha=0.25)
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)


def write_outputs(
    corridor: pd.DataFrame,
    segment_set: pd.DataFrame,
    profile_10min: pd.DataFrame,
    profile_hour: pd.DataFrame,
    ratios: pd.DataFrame,
    ratio_summary: pd.DataFrame,
    dirs: dict[str, Path],
    input_dir: Path,
) -> list[Path]:
    outputs = [
        (dirs["data"] / "treatment_westeast_corridor_10min.csv", corridor),
        (dirs["data"] / "treatment_westeast_diurnal_10min.csv", profile_10min),
        (dirs["data"] / "treatment_westeast_diurnal_hourly.csv", profile_hour),
        (dirs["data"] / "treatment_westeast_peak_reference_ratios_daily.csv", ratios),
        (dirs["metrics"] / "treatment_westeast_peak_reference_ratio_summary.csv", ratio_summary),
        (dirs["metrics"] / "treatment_westeast_segment_set.csv", segment_set),
    ]
    for path, df in outputs:
        df.to_csv(path, index=False)

    metadata = {
        "input_dir": str(input_dir),
        "direction": TREATMENT_DIRECTION,
        "n_corridor_rows": int(len(corridor)),
        "n_segments": int(segment_set["路段编号"].nunique()),
        "total_length_m": float(corridor["total_length_m"].iloc[0]) if not corridor.empty else None,
        "windows": WINDOWS,
        "reference_windows": REFERENCE_WINDOWS,
        "peak_windows": PEAK_WINDOWS,
    }
    metadata_path = dirs["metrics"] / "treatment_westeast_travel_time_metadata.json"
    metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return [path for path, _ in outputs] + [metadata_path]


def main() -> None:
    args = parse_args()
    configure_plot()
    dirs = ensure_output_dirs(args.output_dir)
    panel = load_panel(args.input_dir)
    corridor, segment_set = build_treatment_corridor(panel)
    profile_10min, profile_hour = aggregate_profiles(corridor)
    ratios, ratio_summary = compute_reference_ratios(corridor)

    output_paths = write_outputs(
        corridor, segment_set, profile_10min, profile_hour, ratios, ratio_summary, dirs, args.input_dir
    )

    figures = [
        (
            profile_10min,
            dirs["figures"] / "treatment_westeast_travel_time_10min_weekday_weekend.png",
            "hour_float",
            "mean_travel_time_min",
            "Treatment Direction Travel Time Profiles (10-minute)",
            "Travel time, minutes",
            False,
        ),
        (
            profile_hour,
            dirs["figures"] / "treatment_westeast_travel_time_hourly_weekday_weekend.png",
            "hour",
            "mean_travel_time_min",
            "Treatment Direction Travel Time Profiles (hourly)",
            "Travel time, minutes",
            True,
        ),
    ]
    for df, path, x_col, y_col, title, y_label, marker in figures:
        plot_travel_time_profile(
            df,
            path,
            x_col=x_col,
            y_col=y_col,
            title=title,
            y_label=y_label,
            point_marker=marker,
        )
        output_paths.append(path)

    ratio_figs = {
        "night_0000_0500": dirs["figures"]
        / "treatment_westeast_ratio_to_night_10min_weekday_weekend.png",
        "afternoon15_1500_1600": dirs["figures"]
        / "treatment_westeast_ratio_to_afternoon15_10min_weekday_weekend.png",
    }
    for ref_window, path in ratio_figs.items():
        plot_reference_ratio_profile(corridor, ref_window, path)
        output_paths.append(path)

    daily_path = dirs["figures"] / "treatment_westeast_daily_pm_core_travel_time.png"
    plot_daily_pm_core(corridor, daily_path)
    output_paths.append(daily_path)

    bar_path = dirs["figures"] / "treatment_westeast_peak_reference_ratio_bars.png"
    plot_peak_ratio_bars(ratio_summary, bar_path)
    output_paths.append(bar_path)

    print("Generated treatment-direction outputs:")
    for path in output_paths:
        print(path.relative_to(PROJECT_ROOT))


if __name__ == "__main__":
    main()
