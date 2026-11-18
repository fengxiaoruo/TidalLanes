from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT / "data_work"))

from src.shenzhen_liuxian_gaode.event_study_liuxian import (  # noqa: E402
    DEFAULT_INPUT_DIR,
    DEFAULT_OUTPUT_DIR,
    POLICY_DATE,
    configure_plot,
    ensure_output_dirs,
)
from src.shenzhen_liuxian_gaode.plot_chain_window_speeds import (  # noqa: E402
    CHAIN_LABELS,
    assign_chain_direction,
)


WINDOWS = {
    "night_0000_0500": {
        "label": "00:00-05:00",
        "lo": 0,
        "hi": 5 * 60,
        "weekdays_only": False,
    },
    "weekday_1500_1730": {
        "label": "15:00-17:30",
        "lo": 15 * 60,
        "hi": 17 * 60 + 30,
        "weekdays_only": True,
    },
    "weekday_1730_2000": {
        "label": "17:30-20:00",
        "lo": 17 * 60 + 30,
        "hi": 20 * 60,
        "weekdays_only": True,
    },
    "weekday_2000_2200": {
        "label": "20:00-22:00",
        "lo": 20 * 60,
        "hi": 22 * 60,
        "weekdays_only": True,
    },
}

REFERENCE_WINDOW = "weekday_1500_1730"
ACTIVE_WINDOW = "weekday_1730_2000"
CHAIN_ORDER = ["eastbound_chain", "westbound_chain"]
CHAIN_SHORT = {
    "eastbound_chain": "SW -> NE",
    "westbound_chain": "NE -> SW",
}
CHAIN_COLORS = {
    "eastbound_chain": "#b64b4b",
    "westbound_chain": "#3f6f9f",
}
DAYTYPE_COLORS = {
    "Weekday": "#222222",
    "Weekend": "#7a7a7a",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze the full Gaode post-week segment sample on Liuxian Avenue."
    )
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def period_from_datetime(series: pd.Series) -> str:
    min_date = series.min().strftime("%Y%m%d")
    max_date = series.max().strftime("%Y%m%d")
    prefix = "pre" if series.min() < POLICY_DATE else "post"
    return f"{prefix}_{min_date}_{max_date}"


def parse_coord(coord: str) -> tuple[float, float]:
    lon, lat = coord.split(",")
    return float(lon), float(lat)


def midpoint_score(start: str, end: str) -> float:
    lon1, lat1 = parse_coord(start)
    lon2, lat2 = parse_coord(end)
    return (lon1 + lon2) / 2 + (lat1 + lat2) / 2


def load_full_gaode_panel(input_dir: Path) -> pd.DataFrame:
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
        df["date"] = pd.to_datetime(df["datetime"].dt.date)
        df["dow"] = df["datetime"].dt.dayofweek
        df["minute_of_day"] = df["datetime"].dt.hour * 60 + df["datetime"].dt.minute
        df["speed_kmh"] = pd.to_numeric(df["速度"], errors="coerce")
        df["segment_length_m"] = pd.to_numeric(df["路段长度"], errors="coerce")
        df["delay_index"] = pd.to_numeric(df["拥堵延时指数"], errors="coerce")
        df["segment_id"] = df["路段编号"].astype(str)
        df["source_file"] = path.name
        frames.append(df)
    if not frames:
        raise FileNotFoundError(f"No CSV files found under {input_dir}")
    panel = pd.concat(frames, ignore_index=True)
    panel = panel[(panel["speed_kmh"] > 0) & (panel["segment_length_m"] > 0)].copy()
    panel["segment_travel_time_min"] = (
        panel["segment_length_m"] / 1000.0 / panel["speed_kmh"] * 60.0
    )
    panel["source_period"] = np.where(panel["date"] < POLICY_DATE, "gaode_pre", "gaode_post")
    panel["day_type"] = np.where(panel["dow"] < 5, "Weekday", "Weekend")
    panel["dow_name"] = panel["datetime"].dt.day_name()
    panel["date_label"] = panel["datetime"].dt.strftime("%m-%d")
    return panel


def filter_window(df: pd.DataFrame, window: str) -> pd.DataFrame:
    spec = WINDOWS[window]
    sub = df[(df["minute_of_day"] >= spec["lo"]) & (df["minute_of_day"] < spec["hi"])].copy()
    if spec["weekdays_only"]:
        sub = sub[sub["dow"] < 5].copy()
    return sub


def corridor_speed(group: pd.DataFrame) -> float:
    denom = (group["segment_length_m"] / group["speed_kmh"]).sum()
    if denom <= 0:
        return np.nan
    return float(group["segment_length_m"].sum() / denom)


def build_post_sample(panel: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    panel, inventory = assign_chain_direction(panel)
    segment_sets = {
        period: set(group["segment_id"].unique()) for period, group in panel.groupby("source_period")
    }
    common_segments = set.intersection(*segment_sets.values()) if len(segment_sets) > 1 else set()
    inventory = inventory.copy()
    inventory["prepost_coverage"] = np.where(
        inventory["segment_id"].isin(common_segments), "common", "post_only"
    )
    inventory["midpoint_score"] = inventory.apply(
        lambda row: midpoint_score(row["路段起点坐标"], row["路段终点坐标"]),
        axis=1,
    )
    post = panel[panel["source_period"].eq("gaode_post")].copy()
    post = post.merge(
        inventory[["segment_id", "prepost_coverage", "midpoint_score"]],
        on="segment_id",
        how="left",
    )
    return post, inventory[inventory["segment_id"].isin(post["segment_id"].unique())].copy()


def build_corridor_panel(post: pd.DataFrame) -> pd.DataFrame:
    expected = post.groupby("chain_direction")["segment_id"].nunique().to_dict()
    corridor = (
        post.groupby(
            [
                "chain_direction",
                "chain_label",
                "date",
                "date_label",
                "dow",
                "dow_name",
                "day_type",
                "datetime",
                "minute_of_day",
            ],
            as_index=False,
        )
        .apply(
            lambda group: pd.Series(
                {
                    "corridor_speed_kmh": corridor_speed(group),
                    "travel_time_min": group["segment_travel_time_min"].sum(),
                    "mean_segment_speed_kmh": group["speed_kmh"].mean(),
                    "mean_delay_index": group["delay_index"].mean(),
                    "n_segments": group["segment_id"].nunique(),
                    "total_length_m": group["segment_length_m"].sum(),
                }
            ),
            include_groups=False,
        )
        .reset_index(drop=True)
    )
    corridor = corridor[
        corridor.apply(lambda row: row["n_segments"] == expected[row["chain_direction"]], axis=1)
    ].copy()
    return corridor.sort_values(["chain_direction", "datetime"])


def build_daily_window_summary(corridor: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for window in WINDOWS:
        sub = filter_window(corridor, window)
        grouped = (
            sub.groupby(
                [
                    "chain_direction",
                    "chain_label",
                    "date",
                    "date_label",
                    "dow",
                    "dow_name",
                    "day_type",
                ],
                as_index=False,
            )
            .agg(
                corridor_speed_kmh=("corridor_speed_kmh", "mean"),
                travel_time_min=("travel_time_min", "mean"),
                mean_delay_index=("mean_delay_index", "mean"),
                n_10min=("corridor_speed_kmh", "size"),
                n_segments=("n_segments", "max"),
                total_length_m=("total_length_m", "max"),
            )
        )
        grouped["window"] = window
        grouped["window_label"] = WINDOWS[window]["label"]
        rows.append(grouped)
    return pd.concat(rows, ignore_index=True)


def build_window_summary(daily: pd.DataFrame) -> pd.DataFrame:
    summary = (
        daily.groupby(["chain_direction", "chain_label", "window", "window_label"], as_index=False)
        .agg(
            mean_daily_corridor_speed_kmh=("corridor_speed_kmh", "mean"),
            median_daily_corridor_speed_kmh=("corridor_speed_kmh", "median"),
            mean_daily_travel_time_min=("travel_time_min", "mean"),
            mean_delay_index=("mean_delay_index", "mean"),
            n_days=("date", "nunique"),
            n_segments=("n_segments", "max"),
            total_length_m=("total_length_m", "max"),
        )
    )
    return summary.sort_values(["chain_direction", "window"])


def build_daily_ratios(daily: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    weekdays = daily[daily["dow"] < 5].copy()
    pivot = weekdays[weekdays["window"].isin([REFERENCE_WINDOW, ACTIVE_WINDOW])].pivot_table(
        index=["date", "date_label", "dow_name", "chain_direction", "chain_label"],
        columns="window",
        values=["corridor_speed_kmh", "travel_time_min"],
        aggfunc="first",
    )
    pivot.columns = [f"{metric}_{window}" for metric, window in pivot.columns]
    within = pivot.reset_index()
    within["active_over_reference_speed"] = (
        within[f"corridor_speed_kmh_{ACTIVE_WINDOW}"]
        / within[f"corridor_speed_kmh_{REFERENCE_WINDOW}"]
    )
    within["active_over_reference_travel_time"] = (
        within[f"travel_time_min_{ACTIVE_WINDOW}"]
        / within[f"travel_time_min_{REFERENCE_WINDOW}"]
    )
    within["active_speed_drop_pct"] = 100 * (1 - within["active_over_reference_speed"])
    within = within.sort_values(["date", "chain_direction"]).copy()

    direction = weekdays[weekdays["window"].isin([REFERENCE_WINDOW, ACTIVE_WINDOW])].pivot_table(
        index=["date", "date_label", "dow_name", "window", "window_label"],
        columns="chain_direction",
        values="corridor_speed_kmh",
        aggfunc="first",
    ).reset_index()
    direction["swne_over_nesw_speed_ratio"] = (
        direction["eastbound_chain"] / direction["westbound_chain"]
    )
    direction = direction.sort_values(["date", "window"]).copy()

    rr = within.pivot_table(
        index=["date", "date_label", "dow_name"],
        columns="chain_direction",
        values="active_over_reference_speed",
        aggfunc="first",
    ).reset_index()
    rr["ratio_of_ratios_swne_over_nesw"] = (
        rr["eastbound_chain"] / rr["westbound_chain"]
    )
    rr["relative_change_pct"] = 100 * (rr["ratio_of_ratios_swne_over_nesw"] - 1)
    rr = rr.sort_values("date").copy()
    return within, direction, rr


def build_segment_window_summary(post: pd.DataFrame, inventory: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows = []
    for window in WINDOWS:
        sub = filter_window(post, window)
        grouped = (
            sub.groupby(
                [
                    "chain_direction",
                    "chain_label",
                    "segment_id",
                    "路段编号",
                    "路段名称（唯一）",
                    "路段方向",
                    "prepost_coverage",
                ],
                as_index=False,
            )
            .agg(
                segment_length_m=("segment_length_m", "first"),
                mean_speed_kmh=("speed_kmh", "mean"),
                median_speed_kmh=("speed_kmh", "median"),
                mean_travel_time_min=("segment_travel_time_min", "mean"),
                mean_delay_index=("delay_index", "mean"),
                n_obs=("speed_kmh", "size"),
                n_days=("date", "nunique"),
            )
        )
        grouped["window"] = window
        grouped["window_label"] = WINDOWS[window]["label"]
        rows.append(grouped)
    long = pd.concat(rows, ignore_index=True)

    segment_ratio = long[long["window"].isin([REFERENCE_WINDOW, ACTIVE_WINDOW])].pivot_table(
        index=[
            "chain_direction",
            "chain_label",
            "segment_id",
            "路段编号",
            "路段名称（唯一）",
            "路段方向",
            "segment_length_m",
            "prepost_coverage",
        ],
        columns="window",
        values=["mean_speed_kmh", "mean_travel_time_min", "mean_delay_index"],
        aggfunc="first",
    )
    segment_ratio.columns = [f"{metric}_{window}" for metric, window in segment_ratio.columns]
    segment_ratio = segment_ratio.reset_index()
    segment_ratio["active_over_reference_speed"] = (
        segment_ratio[f"mean_speed_kmh_{ACTIVE_WINDOW}"]
        / segment_ratio[f"mean_speed_kmh_{REFERENCE_WINDOW}"]
    )
    segment_ratio["active_over_reference_travel_time"] = (
        segment_ratio[f"mean_travel_time_min_{ACTIVE_WINDOW}"]
        / segment_ratio[f"mean_travel_time_min_{REFERENCE_WINDOW}"]
    )
    segment_ratio["active_speed_drop_pct"] = 100 * (1 - segment_ratio["active_over_reference_speed"])

    active_totals = segment_ratio.groupby("chain_direction")[f"mean_travel_time_min_{ACTIVE_WINDOW}"].transform(
        "sum"
    )
    segment_ratio["active_travel_time_share_pct"] = (
        100 * segment_ratio[f"mean_travel_time_min_{ACTIVE_WINDOW}"] / active_totals
    )
    order_cols = inventory[["segment_id", "midpoint_score"]].copy()
    segment_ratio = segment_ratio.merge(order_cols, on="segment_id", how="left")
    long = long.merge(order_cols, on="segment_id", how="left")
    return long, segment_ratio.sort_values(["chain_direction", "midpoint_score"])


def plot_diurnal(corridor: pd.DataFrame, output_path: Path) -> None:
    weekday = corridor[corridor["day_type"].eq("Weekday")].copy()
    diurnal = (
        weekday.groupby(["chain_direction", "minute_of_day"], as_index=False)
        .agg(corridor_speed_kmh=("corridor_speed_kmh", "mean"))
        .sort_values(["chain_direction", "minute_of_day"])
    )
    fig, ax = plt.subplots(figsize=(10.8, 4.9))
    ax.axvspan(15, 17.5, color="#d9e6f2", alpha=0.55, lw=0, zorder=0)
    ax.axvspan(17.5, 20, color="#f3d6d6", alpha=0.6, lw=0, zorder=0)
    for chain in CHAIN_ORDER:
        sub = diurnal[diurnal["chain_direction"].eq(chain)]
        ax.plot(
            sub["minute_of_day"] / 60,
            sub["corridor_speed_kmh"],
            color=CHAIN_COLORS[chain],
            linewidth=1.8,
            label=CHAIN_SHORT[chain],
        )
    ax.set_title("Gaode post week full-sample weekday diurnal speed", loc="left", fontweight="bold")
    ax.set_xlabel("Hour of day")
    ax.set_ylabel("Corridor speed (km/h)")
    ax.set_xticks(np.arange(0, 25, 3))
    ax.set_xlim(0, 24)
    ax.set_ylim(10, max(62, diurnal["corridor_speed_kmh"].max() + 3))
    ax.grid(axis="y", color="#d9d9d9", linewidth=0.6, alpha=0.8)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.legend(frameon=False, loc="upper right")
    fig.text(
        0.01,
        0.01,
        "Notes: Weekday speed is averaged across 2026-04-13 to 2026-04-17. "
        "Corridor speed is length-weighted harmonic speed over all observed post-week segments in each chain. "
        "Blue band marks 15:00-17:30; red band marks 17:30-20:00.",
        fontsize=7.5,
        color="#444444",
    )
    fig.tight_layout(rect=(0, 0.08, 1, 0.98))
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)


def plot_daily_ratios(
    within: pd.DataFrame,
    direction: pd.DataFrame,
    rr: pd.DataFrame,
    output_path: Path,
) -> None:
    dates = rr["date_label"] + "\n" + rr["dow_name"].str.slice(0, 3)
    x = np.arange(len(dates))
    width = 0.34
    fig, axes = plt.subplots(1, 3, figsize=(13.0, 4.1))

    ax = axes[0]
    for offset, chain in [(-width / 2, "eastbound_chain"), (width / 2, "westbound_chain")]:
        sub = within[within["chain_direction"].eq(chain)].sort_values("date")
        ax.bar(
            x + offset,
            sub["active_over_reference_speed"],
            width=width,
            color=CHAIN_COLORS[chain],
            alpha=0.86,
            label=CHAIN_SHORT[chain],
        )
    ax.axhline(1, color="#333333", linewidth=0.8, linestyle="--")
    ax.set_title("a. Active / reference speed", loc="left", fontweight="bold")
    ax.set_ylabel("17:30-20:00 / 15:00-17:30")
    ax.set_xticks(x)
    ax.set_xticklabels(dates, fontsize=8)
    ax.set_ylim(0.35, 1.05)
    ax.legend(frameon=False, loc="lower left")
    ax.grid(axis="y", color="#e0e0e0", linewidth=0.6)

    ax = axes[1]
    for window, color, marker in [
        (REFERENCE_WINDOW, "#777777", "o"),
        (ACTIVE_WINDOW, "#111111", "s"),
    ]:
        sub = direction[direction["window"].eq(window)].sort_values("date")
        ax.plot(
            x,
            sub["swne_over_nesw_speed_ratio"],
            color=color,
            marker=marker,
            linewidth=1.5,
            markersize=4.5,
            label=WINDOWS[window]["label"],
        )
    ax.axhline(1, color="#333333", linewidth=0.8, linestyle="--")
    ax.set_title("b. Directional speed ratio", loc="left", fontweight="bold")
    ax.set_ylabel("SW -> NE speed / NE -> SW speed")
    ax.set_xticks(x)
    ax.set_xticklabels(dates, fontsize=8)
    ax.set_ylim(0.75, max(1.8, direction["swne_over_nesw_speed_ratio"].max() + 0.08))
    ax.legend(frameon=False, loc="upper left")
    ax.grid(axis="y", color="#e0e0e0", linewidth=0.6)

    ax = axes[2]
    colors = np.where(rr["ratio_of_ratios_swne_over_nesw"] >= 1, "#2b8cbe", "#cb4b4b")
    ax.bar(x, rr["ratio_of_ratios_swne_over_nesw"], color=colors, alpha=0.86)
    ax.axhline(1, color="#333333", linewidth=0.8, linestyle="--")
    ax.set_title("c. Ratio of ratios", loc="left", fontweight="bold")
    ax.set_ylabel("(SW -> NE active/ref) / (NE -> SW active/ref)")
    ax.set_xticks(x)
    ax.set_xticklabels(dates, fontsize=8)
    ax.set_ylim(0.75, max(1.45, rr["ratio_of_ratios_swne_over_nesw"].max() + 0.08))
    ax.grid(axis="y", color="#e0e0e0", linewidth=0.6)

    for ax in axes:
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
    fig.suptitle("Gaode post week full-sample active/reference speed ratios", x=0.01, ha="left")
    fig.text(
        0.01,
        0.01,
        "Notes: The full post sample uses 8 segments in each chain. Values below 1 in panel a mean the active window is slower than 15:00-17:30.",
        fontsize=7.5,
        color="#444444",
    )
    fig.tight_layout(rect=(0, 0.1, 1, 0.9))
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)


def plot_segment_ratios(segment_ratio: pd.DataFrame, output_path: Path) -> None:
    fig, axes = plt.subplots(2, 1, figsize=(11.0, 6.8), sharey=True)
    for ax, chain in zip(axes, CHAIN_ORDER):
        sub = segment_ratio[segment_ratio["chain_direction"].eq(chain)].copy()
        ascending = chain == "eastbound_chain"
        sub = sub.sort_values("midpoint_score", ascending=ascending)
        x = np.arange(len(sub))
        colors = np.where(sub["prepost_coverage"].eq("post_only"), "#de8f05", CHAIN_COLORS[chain])
        ax.bar(x, sub["active_over_reference_speed"], color=colors, alpha=0.86)
        ax.axhline(1, color="#333333", linewidth=0.8, linestyle="--")
        ax.set_title(f"{CHAIN_SHORT[chain]} segment ratios", loc="left", fontweight="bold")
        ax.set_ylabel("Active / reference speed")
        ax.set_xticks(x)
        labels = [
            f"{int(float(row['路段编号']))}\n{row['路段名称（唯一）']}"
            for _, row in sub.iterrows()
        ]
        ax.set_xticklabels(labels, fontsize=7, rotation=0)
        ax.grid(axis="y", color="#e0e0e0", linewidth=0.6)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
    axes[-1].set_xlabel("Segment ordered along chain direction; orange bars are post-only endpoints")
    fig.suptitle("Gaode post week segment-level active/reference speed ratio", x=0.01, ha="left")
    fig.text(
        0.01,
        0.01,
        "Notes: Active window is 17:30-20:00 on weekdays; reference window is 15:00-17:30 on weekdays.",
        fontsize=7.5,
        color="#444444",
    )
    fig.tight_layout(rect=(0, 0.075, 1, 0.94))
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    args = parse_args()
    configure_plot()
    dirs = ensure_output_dirs(args.output_dir)

    panel = load_full_gaode_panel(args.input_dir)
    post, inventory = build_post_sample(panel)
    corridor = build_corridor_panel(post)
    daily = build_daily_window_summary(corridor)
    window_summary = build_window_summary(daily)
    within, direction, rr = build_daily_ratios(daily)
    segment_window, segment_ratio = build_segment_window_summary(post, inventory)

    corridor.to_csv(
        dirs["data"] / "gaode_post_full_10min_corridor_speed.csv",
        index=False,
        encoding="utf-8-sig",
    )
    daily.to_csv(
        dirs["data"] / "gaode_post_full_daily_window_summary.csv",
        index=False,
        encoding="utf-8-sig",
    )
    within.to_csv(
        dirs["data"] / "gaode_post_full_daily_active_reference_ratios.csv",
        index=False,
        encoding="utf-8-sig",
    )
    direction.to_csv(
        dirs["data"] / "gaode_post_full_daily_direction_speed_ratios_by_window.csv",
        index=False,
        encoding="utf-8-sig",
    )
    inventory.to_csv(
        dirs["metrics"] / "gaode_post_full_segment_inventory.csv",
        index=False,
        encoding="utf-8-sig",
    )
    window_summary.to_csv(
        dirs["metrics"] / "gaode_post_full_window_summary.csv",
        index=False,
        encoding="utf-8-sig",
    )
    segment_window.to_csv(
        dirs["metrics"] / "gaode_post_full_segment_window_summary.csv",
        index=False,
        encoding="utf-8-sig",
    )
    segment_ratio.to_csv(
        dirs["metrics"] / "gaode_post_full_segment_active_reference_ratios.csv",
        index=False,
        encoding="utf-8-sig",
    )
    rr.to_csv(
        dirs["metrics"] / "gaode_post_full_ratio_of_ratios.csv",
        index=False,
        encoding="utf-8-sig",
    )

    plot_diurnal(
        corridor,
        dirs["figures"] / "gaode_post_full_diurnal_by_daytype_chain.png",
    )
    plot_daily_ratios(
        within,
        direction,
        rr,
        dirs["figures"] / "gaode_post_full_daily_window_ratios.png",
    )
    plot_segment_ratios(
        segment_ratio,
        dirs["figures"] / "gaode_post_full_segment_active_reference.png",
    )

    metadata = {
        "input_dir": str(args.input_dir),
        "output_dir": str(args.output_dir),
        "source_period": "gaode_post",
        "post_dates": {
            "min": post["date"].min().strftime("%Y-%m-%d"),
            "max": post["date"].max().strftime("%Y-%m-%d"),
        },
        "segment_scope": "all observed Gaode post-week segments; no common-segment restriction",
        "n_segments_by_chain": post.groupby("chain_direction")["segment_id"].nunique().to_dict(),
        "n_10min_by_chain": corridor.groupby("chain_direction")["datetime"].nunique().to_dict(),
        "windows": WINDOWS,
        "diurnal_figure_scope": "weekday only; both chain directions overlaid in one panel",
        "figures": {
            "diurnal": str(dirs["figures"] / "gaode_post_full_diurnal_by_daytype_chain.png"),
            "daily_ratios": str(dirs["figures"] / "gaode_post_full_daily_window_ratios.png"),
            "segment_ratios": str(dirs["figures"] / "gaode_post_full_segment_active_reference.png"),
        },
    }
    with (dirs["metrics"] / "gaode_post_full_analysis_metadata.json").open(
        "w", encoding="utf-8"
    ) as fh:
        json.dump(metadata, fh, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
