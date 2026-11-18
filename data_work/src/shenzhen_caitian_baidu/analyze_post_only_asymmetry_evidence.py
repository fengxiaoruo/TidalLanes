from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data_work/outputs/shenzhen_caitian_baidu_analysis_v1"

POLICY_DIRECTION = "policy_direction_futian_to_longhua"
OPPOSITE_DIRECTION = "opposite_direction_longhua_to_futian"
CONTROL_ORDER = [
    "control_huanggang",
    "control_jintian",
    "control_yitian",
    "control_xiangmihu",
]
CORRIDOR_ORDER = ["caitian"] + CONTROL_ORDER
WINDOWS = {
    "night_0000_0500": ("00:00-05:00", 0, 5 * 60),
    "reference_1500_1630": ("15:00-16:30", 15 * 60, 16 * 60 + 30),
    "active_1630_2130": ("16:30-21:30", 16 * 60 + 30, 21 * 60 + 30),
    "post_2130_2230": ("21:30-22:30", 21 * 60 + 30, 22 * 60 + 30),
}
EVIDENCE_WINDOWS = [
    "active_1630_2130",
    "post_2130_2230",
    "night_0000_0500",
]
WINDOW_LABELS = {key: value[0] for key, value in WINDOWS.items()}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Post-only directional-asymmetry evidence for Caitian Road."
    )
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def configure_plot() -> None:
    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "axes.titlesize": 11,
            "axes.labelsize": 9,
            "xtick.labelsize": 8,
            "ytick.labelsize": 8,
            "legend.fontsize": 8,
            "figure.dpi": 180,
            "savefig.dpi": 300,
        }
    )


def save_figure(fig: plt.Figure, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, bbox_inches="tight")
    fig.savefig(output_path.with_suffix(".pdf"), bbox_inches="tight")
    fig.savefig(output_path.with_suffix(".svg"), bbox_inches="tight")
    plt.close(fig)


def make_asymmetry_panel(corridor: pd.DataFrame) -> pd.DataFrame:
    wide = corridor.pivot_table(
        index=[
            "corridor_id",
            "corridor_label",
            "analysis_role",
            "is_caitian",
            "is_main_control",
            "date",
            "dow",
            "day_type",
            "minute_of_day",
        ],
        columns="direction_group",
        values=["corridor_speed_kmh", "travel_time_min"],
    ).reset_index()
    wide.columns = ["_".join([str(x) for x in col if x]) for col in wide.columns]
    wide["speed_policy_kmh"] = wide[f"corridor_speed_kmh_{POLICY_DIRECTION}"]
    wide["speed_opposite_kmh"] = wide[f"corridor_speed_kmh_{OPPOSITE_DIRECTION}"]
    wide["travel_time_policy_min"] = wide[f"travel_time_min_{POLICY_DIRECTION}"]
    wide["travel_time_opposite_min"] = wide[f"travel_time_min_{OPPOSITE_DIRECTION}"]
    wide["speed_asym_ratio"] = wide["speed_policy_kmh"] / wide["speed_opposite_kmh"]
    wide["log_speed_asym"] = np.log(wide["speed_asym_ratio"])
    wide["travel_time_asym_ratio"] = (
        wide["travel_time_policy_min"] / wide["travel_time_opposite_min"]
    )
    wide["log_travel_time_asym"] = np.log(wide["travel_time_asym_ratio"])
    return wide


def make_window_daily(asymmetry: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for window, (label, start_min, end_min) in WINDOWS.items():
        sub = asymmetry[
            (asymmetry["minute_of_day"] >= start_min)
            & (asymmetry["minute_of_day"] < end_min)
        ].copy()
        summary = (
            sub.groupby(
                [
                    "corridor_id",
                    "corridor_label",
                    "analysis_role",
                    "is_caitian",
                    "is_main_control",
                    "date",
                    "dow",
                    "day_type",
                ],
                as_index=False,
            )
            .agg(
                speed_policy_kmh=("speed_policy_kmh", "mean"),
                speed_opposite_kmh=("speed_opposite_kmh", "mean"),
                speed_asym_ratio=("speed_asym_ratio", "mean"),
                log_speed_asym=("log_speed_asym", "mean"),
                travel_time_asym_ratio=("travel_time_asym_ratio", "mean"),
                log_travel_time_asym=("log_travel_time_asym", "mean"),
                n_10min=("minute_of_day", "nunique"),
            )
        )
        summary["window"] = window
        summary["window_label"] = label
        rows.append(summary)
    return pd.concat(rows, ignore_index=True)


def make_asymmetry_window_deltas(window_daily: pd.DataFrame) -> pd.DataFrame:
    value_cols = [
        "speed_asym_ratio",
        "log_speed_asym",
        "travel_time_asym_ratio",
        "log_travel_time_asym",
    ]
    index_cols = [
        "corridor_id",
        "corridor_label",
        "analysis_role",
        "is_caitian",
        "is_main_control",
        "date",
        "dow",
        "day_type",
    ]
    wide = window_daily.pivot_table(
        index=index_cols,
        columns="window",
        values=value_cols,
    ).reset_index()
    wide.columns = ["_".join([str(x) for x in col if x]) for col in wide.columns]
    for window in EVIDENCE_WINDOWS:
        wide[f"dlog_speed_asym_{window}"] = (
            wide[f"log_speed_asym_{window}"] - wide["log_speed_asym_reference_1500_1630"]
        )
        wide[f"ratio_of_asym_ratios_{window}"] = np.exp(
            wide[f"dlog_speed_asym_{window}"]
        )
        wide[f"dlog_travel_time_asym_{window}"] = (
            wide[f"log_travel_time_asym_{window}"]
            - wide["log_travel_time_asym_reference_1500_1630"]
        )
    return wide


def make_control_ddd(deltas: pd.DataFrame) -> pd.DataFrame:
    weekday = deltas[deltas["day_type"].eq("weekday")].copy()
    caitian = weekday[weekday["corridor_id"].eq("caitian")].set_index("date")
    rows = []
    for control_id in CONTROL_ORDER:
        control = weekday[weekday["corridor_id"].eq(control_id)].set_index("date")
        if control.empty:
            continue
        control_label = control["corridor_label"].iloc[0]
        for window in EVIDENCE_WINDOWS:
            c_col = f"dlog_speed_asym_{window}"
            paired = caitian[[c_col]].join(
                control[[c_col]], how="inner", lsuffix="_caitian", rsuffix="_control"
            )
            daily = paired[f"{c_col}_caitian"] - paired[f"{c_col}_control"]
            rows.append(
                {
                    "control_id": control_id,
                    "control_label": control_label,
                    "window": window,
                    "window_label": WINDOW_LABELS[window],
                    "n_weekdays": int(daily.notna().sum()),
                    "mean_ddd_log_points": float(daily.mean()),
                    "se_across_weekdays": float(daily.std(ddof=1) / np.sqrt(daily.notna().sum())),
                    "min_daily_ddd": float(daily.min()),
                    "max_daily_ddd": float(daily.max()),
                    "all_weekdays_positive": bool((daily > 0).all()),
                    "implied_percent_advantage": float((np.exp(daily.mean()) - 1.0) * 100.0),
                }
            )
    out = pd.DataFrame(rows)
    out["ci95_low"] = out["mean_ddd_log_points"] - 1.96 * out["se_across_weekdays"]
    out["ci95_high"] = out["mean_ddd_log_points"] + 1.96 * out["se_across_weekdays"]
    return out


def make_daily_control_ddd(deltas: pd.DataFrame) -> pd.DataFrame:
    weekday = deltas[deltas["day_type"].eq("weekday")].copy()
    caitian = weekday[weekday["corridor_id"].eq("caitian")].set_index("date")
    rows = []
    for control_id in CONTROL_ORDER:
        control = weekday[weekday["corridor_id"].eq(control_id)].set_index("date")
        if control.empty:
            continue
        control_label = control["corridor_label"].iloc[0]
        for window in EVIDENCE_WINDOWS:
            c_col = f"dlog_speed_asym_{window}"
            paired = caitian[[c_col]].join(
                control[[c_col]], how="inner", lsuffix="_caitian", rsuffix="_control"
            )
            paired["daily_ddd_log_points"] = (
                paired[f"{c_col}_caitian"] - paired[f"{c_col}_control"]
            )
            for date, row in paired.iterrows():
                rows.append(
                    {
                        "control_id": control_id,
                        "control_label": control_label,
                        "date": date,
                        "window": window,
                        "window_label": WINDOW_LABELS[window],
                        "daily_ddd_log_points": float(row["daily_ddd_log_points"]),
                        "implied_percent_advantage": float(
                            (np.exp(row["daily_ddd_log_points"]) - 1.0) * 100.0
                        ),
                    }
                )
    return pd.DataFrame(rows)


def make_event_time_ddd(asymmetry: pd.DataFrame) -> pd.DataFrame:
    weekday = asymmetry[
        asymmetry["day_type"].eq("weekday")
        & (asymmetry["minute_of_day"] >= 15 * 60)
        & (asymmetry["minute_of_day"] < 22 * 60 + 30)
    ].copy()
    weekday["event_time_min"] = weekday["minute_of_day"] - (16 * 60 + 30)
    weekday["event_bin_min"] = np.floor(weekday["event_time_min"] / 30).astype(int) * 30
    by_bin = (
        weekday.groupby(["corridor_id", "corridor_label", "date", "event_bin_min"], as_index=False)
        .agg(log_speed_asym=("log_speed_asym", "mean"))
    )
    rows = []
    for control_id in CONTROL_ORDER:
        caitian = by_bin[by_bin["corridor_id"].eq("caitian")]
        control = by_bin[by_bin["corridor_id"].eq(control_id)]
        if control.empty:
            continue
        control_label = control["corridor_label"].iloc[0]
        merged = caitian.merge(
            control,
            on=["date", "event_bin_min"],
            suffixes=("_caitian", "_control"),
        )
        merged["dd_log_asym"] = (
            merged["log_speed_asym_caitian"] - merged["log_speed_asym_control"]
        )
        ref = merged[merged["event_bin_min"].eq(-30)][["date", "dd_log_asym"]].rename(
            columns={"dd_log_asym": "reference_dd_log_asym"}
        )
        merged = merged.merge(ref, on="date", how="left")
        merged["ddd_log_asym"] = (
            merged["dd_log_asym"] - merged["reference_dd_log_asym"]
        )
        summary = (
            merged.groupby("event_bin_min", as_index=False)
            .agg(
                mean_ddd_log_points=("ddd_log_asym", "mean"),
                se_across_weekdays=(
                    "ddd_log_asym",
                    lambda x: x.std(ddof=1) / np.sqrt(x.notna().sum()),
                ),
                n_weekdays=("ddd_log_asym", "count"),
            )
        )
        summary["control_id"] = control_id
        summary["control_label"] = control_label
        rows.append(summary)
    out = pd.concat(rows, ignore_index=True)
    out["ci95_low"] = out["mean_ddd_log_points"] - 1.96 * out["se_across_weekdays"]
    out["ci95_high"] = out["mean_ddd_log_points"] + 1.96 * out["se_across_weekdays"]
    return out


def plot_window_asymmetry(window_daily: pd.DataFrame, output_path: Path) -> None:
    weekday = window_daily[window_daily["day_type"].eq("weekday")].copy()
    plot_windows = ["reference_1500_1630", "active_1630_2130", "post_2130_2230"]
    summary = (
        weekday[weekday["window"].isin(plot_windows)]
        .groupby(["corridor_id", "corridor_label", "window", "window_label"], as_index=False)
        .agg(speed_asym_ratio=("speed_asym_ratio", "mean"))
    )
    fig, ax = plt.subplots(figsize=(8.8, 4.4))
    x = np.arange(len(CORRIDOR_ORDER))
    width = 0.24
    colors = {
        "reference_1500_1630": "#8c8c8c",
        "active_1630_2130": "#2b6cb0",
        "post_2130_2230": "#c44e52",
    }
    for j, window in enumerate(plot_windows):
        part = summary[summary["window"].eq(window)].set_index("corridor_id").reindex(CORRIDOR_ORDER)
        ax.bar(
            x + (j - 1.0) * width,
            part["speed_asym_ratio"],
            width=width,
            color=colors[window],
            alpha=0.88,
            label=WINDOW_LABELS[window],
        )
    labels = (
        summary[["corridor_id", "corridor_label"]]
        .drop_duplicates()
        .set_index("corridor_id")
        .reindex(CORRIDOR_ORDER)["corridor_label"]
    )
    ax.axhline(1.0, color="#111111", linewidth=0.8, linestyle="--")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=25, ha="right")
    ax.set_ylabel("Policy-direction speed / opposite-direction speed")
    ax.set_title("Directional asymmetry by window, weekdays", loc="left", fontweight="bold")
    ax.legend(frameon=False, ncol=2)
    ax.grid(axis="y", color="#e5e5e5", linewidth=0.5)
    for spine in ["top", "right"]:
        ax.spines[spine].set_visible(False)
    fig.tight_layout()
    save_figure(fig, output_path)


def plot_ddd_support(ddd: pd.DataFrame, output_path: Path) -> None:
    plot_windows = ["active_1630_2130", "post_2130_2230", "night_0000_0500"]
    colors = {
        "active_1630_2130": "#2b6cb0",
        "post_2130_2230": "#c44e52",
        "night_0000_0500": "#8c8c8c",
    }
    fig, ax = plt.subplots(figsize=(8.6, 4.6))
    x = np.arange(len(CONTROL_ORDER))
    width = 0.18
    for j, window in enumerate(plot_windows):
        part = ddd[ddd["window"].eq(window)].set_index("control_id").reindex(CONTROL_ORDER)
        ax.bar(
            x + (j - 1.0) * width,
            part["mean_ddd_log_points"],
            width=width,
            color=colors[window],
            alpha=0.88,
            label=WINDOW_LABELS[window],
        )
        ax.errorbar(
            x + (j - 1.0) * width,
            part["mean_ddd_log_points"],
            yerr=1.96 * part["se_across_weekdays"],
            fmt="none",
            ecolor="#333333",
            elinewidth=0.8,
            capsize=2,
        )
    labels = (
        ddd[["control_id", "control_label"]]
        .drop_duplicates()
        .set_index("control_id")
        .reindex(CONTROL_ORDER)["control_label"]
    )
    ax.axhline(0, color="#111111", linewidth=0.8)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=25, ha="right")
    ax.set_ylabel("Caitian-control DDD in log directional speed ratio")
    ax.set_title("Post-only asymmetry DDD, weekdays", loc="left", fontweight="bold")
    ax.legend(frameon=False, ncol=2)
    ax.grid(axis="y", color="#e5e5e5", linewidth=0.5)
    for spine in ["top", "right"]:
        ax.spines[spine].set_visible(False)
    fig.tight_layout()
    save_figure(fig, output_path)


def plot_event_ddd(event_ddd: pd.DataFrame, output_path: Path) -> None:
    fig, axes = plt.subplots(2, 2, figsize=(9.2, 6.2), sharex=True, sharey=True)
    for ax, control_id in zip(axes.flat, CONTROL_ORDER):
        sub = event_ddd[event_ddd["control_id"].eq(control_id)].sort_values("event_bin_min")
        label = sub["control_label"].iloc[0] if not sub.empty else control_id
        ax.plot(
            sub["event_bin_min"],
            sub["mean_ddd_log_points"],
            color="#2b6cb0",
            marker="o",
            linewidth=1.25,
            markersize=3.4,
        )
        ax.fill_between(
            sub["event_bin_min"],
            sub["ci95_low"],
            sub["ci95_high"],
            color="#2b6cb0",
            alpha=0.16,
            linewidth=0,
        )
        ax.axhline(0, color="#111111", linewidth=0.8)
        ax.axvline(0, color="#555555", linewidth=0.8, linestyle="--")
        ax.axvspan(0, 300, color="#eeeeee", alpha=0.65)
        ax.set_title(f"Caitian vs {label}", loc="left", fontweight="bold", fontsize=9.2)
        ax.grid(axis="y", color="#e5e5e5", linewidth=0.5)
        for spine in ["top", "right"]:
            ax.spines[spine].set_visible(False)
    for ax in axes[:, 0]:
        ax.set_ylabel("DDD log speed asymmetry")
    for ax in axes[-1, :]:
        ax.set_xlabel("Minutes relative to 16:30")
    fig.suptitle("Event-time DDD in directional asymmetry, weekdays", x=0.01, ha="left")
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    save_figure(fig, output_path)


def plot_daily_active_ddd(daily_ddd: pd.DataFrame, output_path: Path) -> None:
    plot_data = daily_ddd[daily_ddd["window"].isin(["active_1630_2130", "post_2130_2230"])].copy()
    fig, axes = plt.subplots(1, 2, figsize=(9.2, 3.8), sharey=True)
    colors = {
        "active_1630_2130": "#2b6cb0",
        "post_2130_2230": "#c44e52",
    }
    for ax, window in zip(axes, ["active_1630_2130", "post_2130_2230"]):
        sub = plot_data[plot_data["window"].eq(window)]
        x = np.arange(len(CONTROL_ORDER))
        for i, control_id in enumerate(CONTROL_ORDER):
            part = sub[sub["control_id"].eq(control_id)].sort_values("date")
            jitter = np.linspace(-0.08, 0.08, len(part)) if len(part) else []
            ax.scatter(
                np.repeat(i, len(part)) + jitter,
                part["daily_ddd_log_points"],
                color=colors[window],
                s=26,
                alpha=0.88,
                edgecolor="white",
                linewidth=0.45,
                zorder=3,
            )
            ax.hlines(
                part["daily_ddd_log_points"].mean(),
                i - 0.22,
                i + 0.22,
                color="#111111",
                linewidth=1.2,
                zorder=4,
            )
        labels = (
            sub[["control_id", "control_label"]]
            .drop_duplicates()
            .set_index("control_id")
            .reindex(CONTROL_ORDER)["control_label"]
        )
        ax.axhline(0, color="#111111", linewidth=0.8)
        ax.set_xticks(x)
        ax.set_xticklabels(labels, rotation=25, ha="right")
        ax.set_title(WINDOW_LABELS[window], loc="left", fontweight="bold")
        ax.grid(axis="y", color="#e5e5e5", linewidth=0.5)
        for spine in ["top", "right"]:
            ax.spines[spine].set_visible(False)
    axes[0].set_ylabel("Daily Caitian-control DDD in log directional speed ratio")
    fig.suptitle("Daily post-only asymmetry DDD, weekdays", x=0.01, ha="left")
    fig.tight_layout(rect=(0, 0, 1, 0.92))
    save_figure(fig, output_path)


def main() -> None:
    args = parse_args()
    configure_plot()
    data_dir = args.output_dir / "data"
    metrics_dir = args.output_dir / "metrics"
    figures_dir = args.output_dir / "figures"
    corridor = pd.read_csv(data_dir / "corridor_10min_speed.csv")
    asymmetry = make_asymmetry_panel(corridor)
    window_daily = make_window_daily(asymmetry)
    deltas = make_asymmetry_window_deltas(window_daily)
    ddd = make_control_ddd(deltas)
    daily_ddd = make_daily_control_ddd(deltas)
    event_ddd = make_event_time_ddd(asymmetry)

    asymmetry.to_csv(data_dir / "post_only_directional_asymmetry_10min.csv", index=False)
    window_daily.to_csv(data_dir / "post_only_asymmetry_window_daily.csv", index=False)
    deltas.to_csv(data_dir / "post_only_asymmetry_window_deltas.csv", index=False)
    daily_ddd.to_csv(data_dir / "post_only_asymmetry_daily_ddd.csv", index=False)
    ddd.to_csv(metrics_dir / "post_only_asymmetry_ddd_summary.csv", index=False)
    event_ddd.to_csv(metrics_dir / "post_only_asymmetry_event_ddd.csv", index=False)
    spec = {
        "estimand": "Post-only directional asymmetry change in Caitian relative to controls",
        "directional_asymmetry": "log(policy-direction corridor speed / opposite-direction corridor speed)",
        "reference_window": "15:00-16:30",
        "main_active_window": "16:30-21:30",
        "ddd": (
            "Delta log asymmetry in Caitian minus delta log asymmetry in each control, "
            "where delta is relative to the 15:00-16:30 reference window."
        ),
        "event_time_ddd": (
            "For each weekday and control, first difference Caitian-control log asymmetry, "
            "then difference each event bin from the -30 minute reference bin. "
            "Reported intervals use weekday variation only and are descriptive."
        ),
        "interpretation": (
            "This is supportive post-only evidence if Caitian's policy direction becomes "
            "relatively faster than the opposite direction during the active window compared "
            "with non-active windows and compared with controls. It is not a pre/post opening effect."
        ),
    }
    with (metrics_dir / "post_only_asymmetry_evidence_spec.json").open("w", encoding="utf-8") as fh:
        json.dump(spec, fh, ensure_ascii=False, indent=2)

    plot_window_asymmetry(
        window_daily,
        figures_dir / "post_only_asymmetry_window_comparison.png",
    )
    plot_ddd_support(
        ddd,
        figures_dir / "post_only_asymmetry_ddd_support.png",
    )
    plot_event_ddd(
        event_ddd,
        figures_dir / "post_only_asymmetry_event_ddd.png",
    )
    plot_daily_active_ddd(
        daily_ddd,
        figures_dir / "post_only_asymmetry_daily_active_ddd.png",
    )


if __name__ == "__main__":
    main()
