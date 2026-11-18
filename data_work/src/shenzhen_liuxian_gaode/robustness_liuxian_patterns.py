from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_INPUT_DIR = PROJECT_ROOT / "data_work/raw_data/Shenzhen_Gaode"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data_work/outputs/shenzhen_liuxian_gaode_initial_v1"

PERIOD_ORDER = ["pre_20260302_20260308", "post_20260413_20260419"]
PERIOD_COLORS = {
    "pre_20260302_20260308": "#3b6ea8",
    "post_20260413_20260419": "#c44e52",
}
DIRECTION_ORDER = ["西向东", "东向西", "北向南", "南向北"]
WINDOWS = {
    "night_0000_0500": (0, 5 * 60),
    "midday_1200_1500": (12 * 60, 15 * 60),
    "afternoon15_1500_1600": (15 * 60, 16 * 60),
    "pm_core_1730_2000": (17 * 60 + 30, 20 * 60),
    "pm_broad_1630_2030": (16 * 60 + 30, 20 * 60 + 30),
    "pm_1630_1700": (16 * 60 + 30, 17 * 60),
    "pm_1700_1730": (17 * 60, 17 * 60 + 30),
    "pm_1730_1800": (17 * 60 + 30, 18 * 60),
    "pm_1800_1830": (18 * 60, 18 * 60 + 30),
    "pm_1830_1900": (18 * 60 + 30, 19 * 60),
    "pm_1900_1930": (19 * 60, 19 * 60 + 30),
    "pm_1930_2000": (19 * 60 + 30, 20 * 60),
    "pm_2000_2030": (20 * 60, 20 * 60 + 30),
}
REFERENCE_WINDOWS = ["night_0000_0500", "afternoon15_1500_1600", "midday_1200_1500"]
PM_SUBWINDOWS = [
    "pm_1630_1700",
    "pm_1700_1730",
    "pm_1730_1800",
    "pm_1800_1830",
    "pm_1830_1900",
    "pm_1900_1930",
    "pm_1930_2000",
    "pm_2000_2030",
]
WINDOW_LABELS = {
    "night_0000_0500": "Night",
    "midday_1200_1500": "Midday",
    "afternoon15_1500_1600": "15:00",
    "pm_core_1730_2000": "17:30-20:00",
    "pm_broad_1630_2030": "16:30-20:30",
    "pm_1630_1700": "16:30",
    "pm_1700_1730": "17:00",
    "pm_1730_1800": "17:30",
    "pm_1800_1830": "18:00",
    "pm_1830_1900": "18:30",
    "pm_1900_1930": "19:00",
    "pm_1930_2000": "19:30",
    "pm_2000_2030": "20:00",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run extra robustness checks for Liuxian Gaode data.")
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


def sorted_periods(values: pd.Series) -> list[str]:
    seen = set(values.dropna().unique())
    ordered = [period for period in PERIOD_ORDER if period in seen]
    return ordered + sorted(seen - set(ordered))


def period_label(period: str) -> str:
    if period.startswith("pre"):
        return "Pre"
    if period.startswith("post"):
        return "Post"
    return period


def load_panel(input_dir: Path) -> pd.DataFrame:
    frames = []
    for path in sorted(input_dir.glob("*.csv")):
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
        df["speed_kmh"] = pd.to_numeric(df["速度"], errors="coerce")
        df["segment_length_m"] = pd.to_numeric(df["路段长度"], errors="coerce")
        frames.append(df)
    if not frames:
        raise FileNotFoundError(f"No CSV files found under {input_dir}")
    panel = pd.concat(frames, ignore_index=True)
    segment_sets = {
        period: set(group["路段编号"].unique()) for period, group in panel.groupby("period")
    }
    common_segments = set.intersection(*segment_sets.values())
    panel = panel[panel["路段编号"].isin(common_segments)].copy()
    panel["segment_travel_time_min"] = panel["segment_length_m"] / 1000 / panel["speed_kmh"] * 60
    return panel


def build_direction_corridor(panel: pd.DataFrame) -> pd.DataFrame:
    direction_counts = panel.groupby("路段方向")["路段编号"].nunique().to_dict()
    corridor = (
        panel.groupby(
            ["period", "date", "dow", "dow_name", "day_type", "datetime", "minute_of_day", "路段方向"],
            as_index=False,
        )
        .agg(
            travel_time_min=("segment_travel_time_min", "sum"),
            n_segments=("路段编号", "nunique"),
            total_length_m=("segment_length_m", "sum"),
        )
        .sort_values(["路段方向", "period", "datetime"])
    )
    corridor = corridor[
        corridor.apply(lambda row: row["n_segments"] == direction_counts[row["路段方向"]], axis=1)
    ].copy()
    corridor["corridor_speed_kmh"] = corridor["total_length_m"] / 1000 / (
        corridor["travel_time_min"] / 60
    )
    return corridor


def window_means(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    rows = []
    for window, (lo, hi) in WINDOWS.items():
        sub = df[(df["minute_of_day"] >= lo) & (df["minute_of_day"] < hi)]
        grouped = (
            sub.groupby(group_cols, as_index=False)
            .agg(
                travel_time_min_mean=("travel_time_min", "mean"),
                travel_time_min_median=("travel_time_min", "median"),
                speed_kmh_mean=("corridor_speed_kmh", "mean")
                if "corridor_speed_kmh" in sub.columns
                else ("speed_kmh", "mean"),
                n_obs=("travel_time_min" if "travel_time_min" in sub.columns else "speed_kmh", "size"),
            )
        )
        grouped["window"] = window
        rows.append(grouped)
    return pd.concat(rows, ignore_index=True)


def direction_ratio_summary(corridor: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    daily = window_means(
        corridor[corridor["day_type"] == "Weekday"],
        ["period", "date", "dow", "dow_name", "路段方向"],
    )
    wide = daily.pivot_table(
        index=["period", "date", "dow", "dow_name", "路段方向"],
        columns="window",
        values="travel_time_min_mean",
        aggfunc="first",
    ).reset_index()
    wide.columns.name = None
    rows = []
    for ref in REFERENCE_WINDOWS:
        tmp = wide[["period", "date", "dow", "dow_name", "路段方向", "pm_core_1730_2000", ref]].dropna()
        tmp = tmp.rename(
            columns={"pm_core_1730_2000": "pm_core_travel_time_min", ref: "reference_travel_time_min"}
        )
        tmp["reference_window"] = ref
        tmp["ratio"] = tmp["pm_core_travel_time_min"] / tmp["reference_travel_time_min"]
        tmp["log_difference"] = np.log(tmp["pm_core_travel_time_min"]) - np.log(
            tmp["reference_travel_time_min"]
        )
        rows.append(tmp)
    ratios = pd.concat(rows, ignore_index=True)
    summary = (
        ratios.groupby(["路段方向", "reference_window", "period"], as_index=False)
        .agg(
            mean_ratio=("ratio", "mean"),
            mean_log_difference=("log_difference", "mean"),
            mean_pm_core_travel_time_min=("pm_core_travel_time_min", "mean"),
            mean_reference_travel_time_min=("reference_travel_time_min", "mean"),
            n_days=("ratio", "size"),
        )
    )
    wide_summary = summary.pivot_table(
        index=["路段方向", "reference_window"],
        columns="period",
        values=["mean_ratio", "mean_log_difference"],
        aggfunc="first",
    )
    wide_summary.columns = [f"{metric}_{period}" for metric, period in wide_summary.columns]
    wide_summary = wide_summary.reset_index()
    wide_summary["post_minus_pre_ratio"] = (
        wide_summary["mean_ratio_post_20260413_20260419"]
        - wide_summary["mean_ratio_pre_20260302_20260308"]
    )
    wide_summary["post_minus_pre_log_difference"] = (
        wide_summary["mean_log_difference_post_20260413_20260419"]
        - wide_summary["mean_log_difference_pre_20260302_20260308"]
    )
    return ratios, wide_summary


def segment_ratio_summary(panel: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    seg = panel[(panel["路段方向"] == "西向东") & (panel["day_type"] == "Weekday")].copy()
    seg = seg.rename(columns={"segment_travel_time_min": "travel_time_min"})
    daily = window_means(seg, ["period", "date", "dow", "dow_name", "路段编号", "路段名称（唯一）"])
    wide = daily.pivot_table(
        index=["period", "date", "dow", "dow_name", "路段编号", "路段名称（唯一）"],
        columns="window",
        values="travel_time_min_mean",
        aggfunc="first",
    ).reset_index()
    wide.columns.name = None
    rows = []
    for ref in REFERENCE_WINDOWS:
        tmp = wide[
            ["period", "date", "dow", "dow_name", "路段编号", "路段名称（唯一）", "pm_core_1730_2000", ref]
        ].dropna()
        tmp = tmp.rename(
            columns={"pm_core_1730_2000": "pm_core_travel_time_min", ref: "reference_travel_time_min"}
        )
        tmp["reference_window"] = ref
        tmp["ratio"] = tmp["pm_core_travel_time_min"] / tmp["reference_travel_time_min"]
        rows.append(tmp)
    ratios = pd.concat(rows, ignore_index=True)
    summary = (
        ratios.groupby(["路段编号", "路段名称（唯一）", "reference_window", "period"], as_index=False)
        .agg(mean_ratio=("ratio", "mean"), n_days=("ratio", "size"))
    )
    wide_summary = summary.pivot_table(
        index=["路段编号", "路段名称（唯一）", "reference_window"],
        columns="period",
        values="mean_ratio",
        aggfunc="first",
    ).reset_index()
    wide_summary["post_minus_pre_ratio"] = (
        wide_summary["post_20260413_20260419"] - wide_summary["pre_20260302_20260308"]
    )
    wide_summary["post_over_pre_ratio"] = (
        wide_summary["post_20260413_20260419"] / wide_summary["pre_20260302_20260308"]
    )
    return ratios, wide_summary


def west_east_robustness(corridor: pd.DataFrame) -> pd.DataFrame:
    we = corridor[(corridor["路段方向"] == "西向东") & (corridor["day_type"] == "Weekday")]
    daily = window_means(we, ["period", "date", "dow", "dow_name"])
    wide = daily.pivot_table(
        index=["period", "date", "dow", "dow_name"],
        columns="window",
        values=["travel_time_min_mean", "travel_time_min_median"],
        aggfunc="first",
    )
    wide.columns = [f"{metric}_{window}" for metric, window in wide.columns]
    wide = wide.reset_index()

    samples = {
        "all_weekdays": lambda d: d,
        "exclude_friday": lambda d: d[d["dow"] != 4],
        "tue_to_thu": lambda d: d[d["dow"].isin([1, 2, 3])],
        "mon_to_thu": lambda d: d[d["dow"].isin([0, 1, 2, 3])],
    }
    outcomes = {
        "pm_core_raw_mean_tt": lambda d: d["travel_time_min_mean_pm_core_1730_2000"],
        "pm_core_raw_median_tt": lambda d: d["travel_time_min_median_pm_core_1730_2000"],
        "pm_core_over_night": lambda d: d["travel_time_min_mean_pm_core_1730_2000"]
        / d["travel_time_min_mean_night_0000_0500"],
        "pm_core_over_15": lambda d: d["travel_time_min_mean_pm_core_1730_2000"]
        / d["travel_time_min_mean_afternoon15_1500_1600"],
        "pm_core_over_midday": lambda d: d["travel_time_min_mean_pm_core_1730_2000"]
        / d["travel_time_min_mean_midday_1200_1500"],
    }
    rows = []
    for sample_name, sample_fn in samples.items():
        sample = sample_fn(wide)
        for outcome_name, outcome_fn in outcomes.items():
            tmp = sample[["period"]].copy()
            tmp["value"] = outcome_fn(sample)
            summary = tmp.groupby("period")["value"].mean()
            pre = summary.get("pre_20260302_20260308", np.nan)
            post = summary.get("post_20260413_20260419", np.nan)
            rows.append(
                {
                    "sample": sample_name,
                    "outcome": outcome_name,
                    "pre": pre,
                    "post": post,
                    "post_minus_pre": post - pre,
                    "post_over_pre": post / pre,
                    "n_pre_days": int((sample["period"] == "pre_20260302_20260308").sum()),
                    "n_post_days": int((sample["period"] == "post_20260413_20260419").sum()),
                }
            )
    return pd.DataFrame(rows)


def pm_subwindow_summary(corridor: pd.DataFrame) -> pd.DataFrame:
    we = corridor[
        (corridor["路段方向"] == "西向东")
        & (corridor["day_type"] == "Weekday")
        & (corridor["minute_of_day"] >= 16 * 60 + 30)
        & (corridor["minute_of_day"] < 20 * 60 + 30)
    ].copy()
    rows = []
    for window in PM_SUBWINDOWS:
        lo, hi = WINDOWS[window]
        sub = we[(we["minute_of_day"] >= lo) & (we["minute_of_day"] < hi)]
        grouped = (
            sub.groupby(["period", "date", "dow", "dow_name"], as_index=False)
            .agg(travel_time_min=("travel_time_min", "mean"))
        )
        grouped["window"] = window
        rows.append(grouped)
    daily = pd.concat(rows, ignore_index=True)
    summary = (
        daily.groupby(["window", "period"], as_index=False)
        .agg(mean_travel_time_min=("travel_time_min", "mean"), n_days=("travel_time_min", "size"))
    )
    wide = summary.pivot(index="window", columns="period", values="mean_travel_time_min").reset_index()
    wide["post_minus_pre"] = wide["post_20260413_20260419"] - wide["pre_20260302_20260308"]
    wide["post_over_pre"] = wide["post_20260413_20260419"] / wide["pre_20260302_20260308"]
    wide["window_order"] = wide["window"].map({window: i for i, window in enumerate(PM_SUBWINDOWS)})
    return wide.sort_values("window_order")


def plot_pm_subwindows(pm_sub: pd.DataFrame, path: Path) -> None:
    fig, ax = plt.subplots(figsize=(8.4, 4.3))
    x = np.arange(len(pm_sub))
    for period in PERIOD_ORDER:
        ax.plot(
            x,
            pm_sub[period],
            marker="o",
            linewidth=2,
            color=PERIOD_COLORS[period],
            label=period_label(period),
        )
    ax.axvspan(2 - 0.5, 6 + 0.5, color="#e9b7b7", alpha=0.22, linewidth=0)
    ax.set_xticks(x, [WINDOW_LABELS[w] for w in pm_sub["window"]], rotation=0)
    ax.set_ylabel("Mean travel time, minutes")
    ax.set_title("West-to-East Travel Time by PM Subwindow")
    ax.grid(True, axis="y", alpha=0.25)
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def plot_direction_ratios(direction_summary: pd.DataFrame, path: Path, reference_window: str) -> None:
    use = direction_summary[direction_summary["reference_window"] == reference_window].copy()
    use["order"] = use["路段方向"].map({d: i for i, d in enumerate(DIRECTION_ORDER)})
    use = use.sort_values("order")
    x = np.arange(len(use))
    width = 0.34
    fig, ax = plt.subplots(figsize=(8.0, 4.3))
    ax.bar(
        x - width / 2,
        use["mean_ratio_pre_20260302_20260308"],
        width,
        color=PERIOD_COLORS["pre_20260302_20260308"],
        label="Pre",
        alpha=0.86,
    )
    ax.bar(
        x + width / 2,
        use["mean_ratio_post_20260413_20260419"],
        width,
        color=PERIOD_COLORS["post_20260413_20260419"],
        label="Post",
        alpha=0.86,
    )
    ax.set_xticks(x, use["路段方向"])
    ax.set_ylabel("PM core travel time / reference")
    ax.set_title(f"PM Core Relative Travel Time by Direction, Reference: {WINDOW_LABELS[reference_window]}")
    ax.grid(True, axis="y", alpha=0.25)
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def plot_segment_ratios(segment_summary: pd.DataFrame, path: Path, reference_window: str) -> None:
    use = segment_summary[segment_summary["reference_window"] == reference_window].copy()
    use = use.sort_values("路段编号")
    x = np.arange(len(use))
    width = 0.34
    fig, ax = plt.subplots(figsize=(8.2, 4.3))
    ax.bar(
        x - width / 2,
        use["pre_20260302_20260308"],
        width,
        color=PERIOD_COLORS["pre_20260302_20260308"],
        label="Pre",
        alpha=0.86,
    )
    ax.bar(
        x + width / 2,
        use["post_20260413_20260419"],
        width,
        color=PERIOD_COLORS["post_20260413_20260419"],
        label="Post",
        alpha=0.86,
    )
    labels = [f"{row['路段编号']}\n{row['路段名称（唯一）']}" for _, row in use.iterrows()]
    ax.set_xticks(x, labels)
    ax.set_ylabel("PM core travel time / reference")
    ax.set_title(f"West-to-East Segment Ratios, Reference: {WINDOW_LABELS[reference_window]}")
    ax.grid(True, axis="y", alpha=0.25)
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    args = parse_args()
    configure_plot()
    dirs = ensure_output_dirs(args.output_dir)
    panel = load_panel(args.input_dir)
    corridor = build_direction_corridor(panel)

    direction_ratios, direction_summary = direction_ratio_summary(corridor)
    segment_ratios, segment_summary = segment_ratio_summary(panel)
    robustness = west_east_robustness(corridor)
    pm_sub = pm_subwindow_summary(corridor)

    outputs = {
        dirs["data"] / "robustness_direction_peak_reference_ratios_daily.csv": direction_ratios,
        dirs["metrics"] / "robustness_direction_peak_reference_ratio_summary.csv": direction_summary,
        dirs["data"] / "robustness_segment_peak_reference_ratios_daily.csv": segment_ratios,
        dirs["metrics"] / "robustness_segment_peak_reference_ratio_summary.csv": segment_summary,
        dirs["metrics"] / "robustness_westeast_sensitivity_summary.csv": robustness,
        dirs["metrics"] / "robustness_westeast_pm_subwindow_summary.csv": pm_sub,
    }
    for path, df in outputs.items():
        df.to_csv(path, index=False)

    figure_paths = [
        dirs["figures"] / "robustness_westeast_pm_subwindow_travel_time.png",
        dirs["figures"] / "robustness_direction_ratio_to_afternoon15.png",
        dirs["figures"] / "robustness_direction_ratio_to_night.png",
        dirs["figures"] / "robustness_westeast_segment_ratio_to_afternoon15.png",
        dirs["figures"] / "robustness_westeast_segment_ratio_to_night.png",
    ]
    plot_pm_subwindows(pm_sub, figure_paths[0])
    plot_direction_ratios(direction_summary, figure_paths[1], "afternoon15_1500_1600")
    plot_direction_ratios(direction_summary, figure_paths[2], "night_0000_0500")
    plot_segment_ratios(segment_summary, figure_paths[3], "afternoon15_1500_1600")
    plot_segment_ratios(segment_summary, figure_paths[4], "night_0000_0500")

    print("Generated robustness outputs:")
    for path in list(outputs) + figure_paths:
        print(path.relative_to(PROJECT_ROOT))


if __name__ == "__main__":
    main()
