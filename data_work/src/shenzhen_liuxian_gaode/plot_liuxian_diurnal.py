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

DIRECTION_ORDER = ["西向东", "东向西", "北向南", "南向北"]
DAY_TYPE_ORDER = ["Weekday", "Weekend"]
PERIOD_COLORS = {
    "pre_20260302_20260308": "#3b6ea8",
    "post_20260413_20260419": "#c44e52",
}
PERIOD_ORDER = ["pre_20260302_20260308", "post_20260413_20260419"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot Liuxian Avenue Gaode speed profiles by direction and day type."
    )
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument(
        "--all-segments",
        action="store_true",
        help="Use all available segments instead of the common segment set across periods.",
    )
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


def load_one_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, encoding="utf-8-sig", dtype=str)
    for col in df.columns:
        df[col] = df[col].astype(str).str.strip()

    df["datetime"] = pd.to_datetime(
        df["日期"].str.zfill(8) + df["时间"].str.zfill(4),
        format="%Y%m%d%H%M",
        errors="raise",
    )
    df["date"] = df["datetime"].dt.date.astype(str)
    df["dow"] = df["datetime"].dt.dayofweek
    df["day_type"] = np.where(df["dow"] < 5, "Weekday", "Weekend")
    df["minute_of_day"] = df["datetime"].dt.hour * 60 + df["datetime"].dt.minute
    df["hour"] = df["datetime"].dt.hour
    df["time_label"] = df["datetime"].dt.strftime("%H:%M")
    df["speed"] = pd.to_numeric(df["速度"], errors="coerce")
    df["delay_index"] = pd.to_numeric(df["拥堵延时指数"], errors="coerce")
    df["segment_length_m"] = pd.to_numeric(df["路段长度"], errors="coerce")

    min_date = df["datetime"].min().strftime("%Y%m%d")
    max_date = df["datetime"].max().strftime("%Y%m%d")
    prefix = "pre" if df["datetime"].min() < pd.Timestamp("2026-03-23") else "post"
    df["period"] = f"{prefix}_{min_date}_{max_date}"
    df["source_file"] = path.name
    return df


def load_panel(input_dir: Path, *, all_segments: bool) -> tuple[pd.DataFrame, dict[str, object]]:
    files = sorted(input_dir.glob("*.csv"))
    if not files:
        raise FileNotFoundError(f"No CSV files found under {input_dir}")

    frames = [load_one_csv(path) for path in files]
    panel = pd.concat(frames, ignore_index=True)
    panel = panel.sort_values(["period", "路段编号", "datetime"]).reset_index(drop=True)

    segment_sets = {
        period: set(group["路段编号"].unique()) for period, group in panel.groupby("period")
    }
    common_segments = set.intersection(*segment_sets.values())
    sample_panel = panel if all_segments else panel[panel["路段编号"].isin(common_segments)].copy()
    sample_panel["sample_scope"] = "all_segments" if all_segments else "common_segments"

    summary = {
        "input_dir": str(input_dir),
        "files": [path.name for path in files],
        "sample_scope": "all_segments" if all_segments else "common_segments",
        "periods": sorted(panel["period"].unique().tolist()),
        "rows_raw": int(len(panel)),
        "rows_used": int(len(sample_panel)),
        "n_segments_raw_by_period": {
            period: int(group["路段编号"].nunique()) for period, group in panel.groupby("period")
        },
        "n_segments_used_by_period": {
            period: int(group["路段编号"].nunique()) for period, group in sample_panel.groupby("period")
        },
        "common_segment_count": int(len(common_segments)),
        "post_only_segments": sorted(
            list(segment_sets.get("post_20260413_20260419", set()) - common_segments)
        ),
        "pre_only_segments": sorted(
            list(segment_sets.get("pre_20260302_20260308", set()) - common_segments)
        ),
    }
    return sample_panel, summary


def aggregate_profiles(panel: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    group_cols_10min = ["period", "路段方向", "day_type", "minute_of_day"]
    profile_10min = (
        panel.groupby(group_cols_10min, as_index=False)
        .agg(
            mean_speed=("speed", "mean"),
            median_speed=("speed", "median"),
            mean_delay_index=("delay_index", "mean"),
            median_delay_index=("delay_index", "median"),
            n_obs=("speed", "size"),
            n_segments=("路段编号", "nunique"),
        )
        .sort_values(group_cols_10min)
    )
    profile_10min["hour_float"] = profile_10min["minute_of_day"] / 60.0

    group_cols_hour = ["period", "路段方向", "day_type", "hour"]
    profile_hour = (
        panel.groupby(group_cols_hour, as_index=False)
        .agg(
            mean_speed=("speed", "mean"),
            median_speed=("speed", "median"),
            mean_delay_index=("delay_index", "mean"),
            median_delay_index=("delay_index", "median"),
            n_obs=("speed", "size"),
            n_segments=("路段编号", "nunique"),
        )
        .sort_values(group_cols_hour)
    )

    core = panel[
        (panel["dow"] < 5)
        & (panel["minute_of_day"] >= 17 * 60 + 30)
        & (panel["minute_of_day"] < 20 * 60)
    ].copy()
    core_summary = (
        core.groupby(["period", "路段方向"], as_index=False)
        .agg(
            mean_speed=("speed", "mean"),
            median_speed=("speed", "median"),
            mean_delay_index=("delay_index", "mean"),
            n_obs=("speed", "size"),
            n_segments=("路段编号", "nunique"),
        )
        .sort_values(["路段方向", "period"])
    )
    return profile_10min, profile_hour, core_summary


def period_label(period: str) -> str:
    if period.startswith("pre"):
        return "Pre: 2026-03-02 to 2026-03-08"
    if period.startswith("post"):
        return "Post: 2026-04-13 to 2026-04-19"
    return period


def sorted_periods(periods: pd.Series) -> list[str]:
    seen = set(periods.dropna().unique())
    ordered = [period for period in PERIOD_ORDER if period in seen]
    return ordered + sorted(seen - set(ordered))


def direction_title(direction: str) -> str:
    mapping = {
        "西向东": "West to East",
        "东向西": "East to West",
        "北向南": "North to South",
        "南向北": "South to North",
    }
    return f"{direction} / {mapping.get(direction, direction)}"


def add_policy_window(ax) -> None:
    ax.axvspan(17.5, 20.0, color="#e9b7b7", alpha=0.22, linewidth=0)


def plot_profile(
    df: pd.DataFrame,
    out_path: Path,
    *,
    x_col: str,
    y_col: str,
    y_label: str,
    title: str,
    point_marker: bool = False,
) -> None:
    periods = sorted_periods(df["period"])
    fig, axes = plt.subplots(
        len(DIRECTION_ORDER),
        len(DAY_TYPE_ORDER),
        figsize=(12.8, 9.6),
        sharex=True,
        sharey=True,
    )

    for row, direction in enumerate(DIRECTION_ORDER):
        for col, day_type in enumerate(DAY_TYPE_ORDER):
            ax = axes[row, col]
            add_policy_window(ax)
            for period in periods:
                sub = df[
                    (df["路段方向"] == direction)
                    & (df["day_type"] == day_type)
                    & (df["period"] == period)
                ].sort_values(x_col)
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
            if row == 0:
                ax.set_title(day_type)
            if col == 0:
                ax.set_ylabel(f"{direction_title(direction)}\n{y_label}")
            ax.grid(True, axis="both", alpha=0.22, linewidth=0.7)
            ax.set_xlim(0, 23.99 if x_col == "hour_float" else 23)
            ax.set_xticks([0, 6, 12, 18, 24] if x_col == "hour_float" else [0, 6, 12, 18, 23])

    handles, labels = axes[0, 0].get_legend_handles_labels()
    if handles:
        fig.legend(handles, labels, loc="upper center", ncol=2, frameon=False, bbox_to_anchor=(0.5, 1.01))
    fig.supxlabel("Hour of day")
    fig.suptitle(title, y=1.04, fontsize=12)
    fig.text(
        0.5,
        -0.01,
        "Sample: common Liuxian Avenue segments across both weeks. Shaded band: official weekday tidal-lane window, 17:30-20:00.",
        ha="center",
        va="top",
        fontsize=8.5,
        color="#4b5563",
    )
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)


def write_outputs(
    panel: pd.DataFrame,
    profile_10min: pd.DataFrame,
    profile_hour: pd.DataFrame,
    core_summary: pd.DataFrame,
    metadata: dict[str, object],
    dirs: dict[str, Path],
) -> list[Path]:
    paths: list[Path] = []
    suffix = str(metadata["sample_scope"])
    profile_10min_path = dirs["data"] / f"diurnal_10min_by_direction_daytype_{suffix}.csv"
    profile_hour_path = dirs["data"] / f"hourly_by_direction_daytype_{suffix}.csv"
    core_path = dirs["metrics"] / f"core_weekday_1730_2000_by_direction_{suffix}.csv"
    segment_path = dirs["metrics"] / "segment_inventory_used.csv"
    metadata_path = dirs["metrics"] / "run_metadata.json"

    profile_10min.to_csv(profile_10min_path, index=False)
    profile_hour.to_csv(profile_hour_path, index=False)
    core_summary.to_csv(core_path, index=False)
    panel[
        [
            "period",
            "路段编号",
            "路段名称（唯一）",
            "路段方向",
            "路段长度",
            "路段起点坐标",
            "路段终点坐标",
        ]
    ].drop_duplicates().sort_values(["路段方向", "路段编号", "period"]).to_csv(segment_path, index=False)
    metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    paths.extend([profile_10min_path, profile_hour_path, core_path, segment_path, metadata_path])
    return paths


def main() -> None:
    args = parse_args()
    configure_plot()
    dirs = ensure_output_dirs(args.output_dir)
    panel, metadata = load_panel(args.input_dir, all_segments=args.all_segments)
    profile_10min, profile_hour, core_summary = aggregate_profiles(panel)

    output_paths = write_outputs(panel, profile_10min, profile_hour, core_summary, metadata, dirs)

    figures = [
        (
            profile_10min,
            dirs["figures"]
            / f"speed_diurnal_10min_by_direction_daytype_{metadata['sample_scope']}.png",
            "hour_float",
            "mean_speed",
            "Speed",
            "Liuxian Avenue Speed Profiles by Direction and Day Type (10-minute)",
            False,
        ),
        (
            profile_10min,
            dirs["figures"]
            / f"delay_diurnal_10min_by_direction_daytype_{metadata['sample_scope']}.png",
            "hour_float",
            "mean_delay_index",
            "Delay index",
            "Liuxian Avenue Delay Index Profiles by Direction and Day Type (10-minute)",
            False,
        ),
        (
            profile_hour,
            dirs["figures"] / f"speed_hourly_by_direction_daytype_{metadata['sample_scope']}.png",
            "hour",
            "mean_speed",
            "Speed",
            "Liuxian Avenue Speed Profiles by Direction and Day Type (hourly)",
            True,
        ),
        (
            profile_hour,
            dirs["figures"] / f"delay_hourly_by_direction_daytype_{metadata['sample_scope']}.png",
            "hour",
            "mean_delay_index",
            "Delay index",
            "Liuxian Avenue Delay Index Profiles by Direction and Day Type (hourly)",
            True,
        ),
    ]
    for df, path, x_col, y_col, y_label, title, marker in figures:
        plot_profile(
            df,
            path,
            x_col=x_col,
            y_col=y_col,
            y_label=y_label,
            title=title,
            point_marker=marker,
        )
        output_paths.append(path)

    print("Generated outputs:")
    for path in output_paths:
        print(path.relative_to(PROJECT_ROOT))


if __name__ == "__main__":
    main()
