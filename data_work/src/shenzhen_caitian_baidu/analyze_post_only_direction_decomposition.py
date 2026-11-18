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
DIRECTION_ORDER = [POLICY_DIRECTION, OPPOSITE_DIRECTION]
DIRECTION_LABELS = {
    POLICY_DIRECTION: "Futian -> Longhua",
    OPPOSITE_DIRECTION: "Longhua -> Futian",
}
CONTROL_ORDER = [
    "control_huanggang",
    "control_jintian",
    "control_yitian",
    "control_xiangmihu",
]
WINDOWS = {
    "reference_1500_1630": ("15:00-16:30", 15 * 60, 16 * 60 + 30),
    "active_1630_2130": ("16:30-21:30", 16 * 60 + 30, 21 * 60 + 30),
    "post_2130_2230": ("21:30-22:30", 21 * 60 + 30, 22 * 60 + 30),
    "night_0000_0500": ("00:00-05:00", 0, 5 * 60),
}
COMPARISON_WINDOWS = [
    "active_1630_2130",
    "post_2130_2230",
]
WINDOW_LABELS = {key: value[0] for key, value in WINDOWS.items()}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Decompose post-only Caitian evidence by policy and opposite directions."
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


def build_window_direction_panel(corridor: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for window, (label, start_min, end_min) in WINDOWS.items():
        sub = corridor[
            corridor["day_type"].eq("weekday")
            & (corridor["minute_of_day"] >= start_min)
            & (corridor["minute_of_day"] < end_min)
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
                    "direction_group",
                ],
                as_index=False,
            )
            .agg(
                speed_kmh=("corridor_speed_kmh", "mean"),
                travel_time_min=("travel_time_min", "mean"),
                n_10min=("minute_of_day", "nunique"),
            )
        )
        summary["window"] = window
        summary["window_label"] = label
        rows.append(summary)
    return pd.concat(rows, ignore_index=True)


def build_direction_deltas(window_panel: pd.DataFrame) -> pd.DataFrame:
    index_cols = [
        "corridor_id",
        "corridor_label",
        "analysis_role",
        "is_caitian",
        "is_main_control",
        "date",
        "dow",
        "direction_group",
    ]
    wide = window_panel.pivot_table(
        index=index_cols,
        columns="window",
        values=["speed_kmh", "travel_time_min"],
    ).reset_index()
    wide.columns = ["_".join([str(x) for x in col if x]) for col in wide.columns]
    for window in COMPARISON_WINDOWS + ["night_0000_0500"]:
        wide[f"log_speed_change_{window}"] = (
            np.log(wide[f"speed_kmh_{window}"]) - np.log(wide["speed_kmh_reference_1500_1630"])
        )
        wide[f"speed_change_kmh_{window}"] = (
            wide[f"speed_kmh_{window}"] - wide["speed_kmh_reference_1500_1630"]
        )
        wide[f"speed_ratio_{window}"] = (
            wide[f"speed_kmh_{window}"] / wide["speed_kmh_reference_1500_1630"]
        )
        wide[f"log_travel_time_change_{window}"] = (
            np.log(wide[f"travel_time_min_{window}"])
            - np.log(wide["travel_time_min_reference_1500_1630"])
        )
    return wide


def build_direction_dd(deltas: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    caitian = deltas[deltas["corridor_id"].eq("caitian")].copy()
    rows = []
    daily_rows = []
    for control_id in CONTROL_ORDER:
        control = deltas[deltas["corridor_id"].eq(control_id)].copy()
        if control.empty:
            continue
        control_label = control["corridor_label"].iloc[0]
        for direction in DIRECTION_ORDER:
            caitian_dir = caitian[caitian["direction_group"].eq(direction)].set_index("date")
            control_dir = control[control["direction_group"].eq(direction)].set_index("date")
            for window in COMPARISON_WINDOWS:
                log_col = f"log_speed_change_{window}"
                kmh_col = f"speed_change_kmh_{window}"
                ratio_col = f"speed_ratio_{window}"
                merged = caitian_dir[[log_col, kmh_col, ratio_col]].join(
                    control_dir[[log_col, kmh_col, ratio_col]],
                    how="inner",
                    lsuffix="_caitian",
                    rsuffix="_control",
                )
                merged["dd_log_speed"] = (
                    merged[f"{log_col}_caitian"] - merged[f"{log_col}_control"]
                )
                merged["dd_speed_kmh"] = (
                    merged[f"{kmh_col}_caitian"] - merged[f"{kmh_col}_control"]
                )
                for date, row in merged.iterrows():
                    daily_rows.append(
                        {
                            "control_id": control_id,
                            "control_label": control_label,
                            "direction_group": direction,
                            "direction_label": DIRECTION_LABELS[direction],
                            "window": window,
                            "window_label": WINDOW_LABELS[window],
                            "date": date,
                            "caitian_speed_ratio": float(row[f"{ratio_col}_caitian"]),
                            "control_speed_ratio": float(row[f"{ratio_col}_control"]),
                            "dd_log_speed": float(row["dd_log_speed"]),
                            "dd_speed_kmh": float(row["dd_speed_kmh"]),
                        }
                    )
                rows.append(
                    {
                        "control_id": control_id,
                        "control_label": control_label,
                        "direction_group": direction,
                        "direction_label": DIRECTION_LABELS[direction],
                        "window": window,
                        "window_label": WINDOW_LABELS[window],
                        "n_weekdays": int(merged["dd_log_speed"].notna().sum()),
                        "mean_caitian_speed_ratio": float(merged[f"{ratio_col}_caitian"].mean()),
                        "mean_control_speed_ratio": float(merged[f"{ratio_col}_control"].mean()),
                        "mean_dd_log_speed": float(merged["dd_log_speed"].mean()),
                        "se_dd_log_speed": float(
                            merged["dd_log_speed"].std(ddof=1)
                            / np.sqrt(merged["dd_log_speed"].notna().sum())
                        ),
                        "mean_dd_speed_kmh": float(merged["dd_speed_kmh"].mean()),
                        "min_daily_dd_log_speed": float(merged["dd_log_speed"].min()),
                        "max_daily_dd_log_speed": float(merged["dd_log_speed"].max()),
                        "all_weekdays_positive": bool((merged["dd_log_speed"] > 0).all()),
                        "all_weekdays_negative": bool((merged["dd_log_speed"] < 0).all()),
                        "implied_percent_relative_gain": float(
                            (np.exp(merged["dd_log_speed"].mean()) - 1.0) * 100.0
                        ),
                    }
                )
    summary = pd.DataFrame(rows)
    summary["ci95_low"] = summary["mean_dd_log_speed"] - 1.96 * summary["se_dd_log_speed"]
    summary["ci95_high"] = summary["mean_dd_log_speed"] + 1.96 * summary["se_dd_log_speed"]
    return summary, pd.DataFrame(daily_rows)


def plot_direction_speed_changes(deltas: pd.DataFrame, output_path: Path) -> None:
    plot_data = deltas[deltas["corridor_id"].isin(["caitian"] + CONTROL_ORDER)].copy()
    fig, axes = plt.subplots(1, 2, figsize=(10.2, 4.2), sharey=True)
    colors = {"caitian": "#2b6cb0"}
    labels = (
        plot_data[["corridor_id", "corridor_label"]]
        .drop_duplicates()
        .set_index("corridor_id")
        .reindex(["caitian"] + CONTROL_ORDER)["corridor_label"]
    )
    x = np.arange(len(labels))
    for ax, direction in zip(axes, DIRECTION_ORDER):
        sub = (
            plot_data[plot_data["direction_group"].eq(direction)]
            .groupby("corridor_id", as_index=False)
            .agg(speed_ratio=("speed_ratio_active_1630_2130", "mean"))
            .set_index("corridor_id")
            .reindex(["caitian"] + CONTROL_ORDER)
        )
        ax.bar(
            x,
            sub["speed_ratio"],
            color=[colors.get(idx, "#9e9e9e") for idx in sub.index],
            alpha=0.9,
        )
        ax.axhline(1.0, color="#111111", linewidth=0.8, linestyle="--")
        ax.set_title(DIRECTION_LABELS[direction], loc="left", fontweight="bold")
        ax.set_xticks(x)
        ax.set_xticklabels(labels, rotation=25, ha="right")
        ax.grid(axis="y", color="#e5e5e5", linewidth=0.5)
        for spine in ["top", "right"]:
            ax.spines[spine].set_visible(False)
    axes[0].set_ylabel("16:30-21:30 / 15:00-16:30 speed")
    fig.suptitle("Direction-specific active/reference speed ratio, weekdays", x=0.01, ha="left")
    fig.tight_layout(rect=(0, 0, 1, 0.92))
    save_figure(fig, output_path)


def plot_direction_dd(summary: pd.DataFrame, output_path: Path) -> None:
    active = summary[summary["window"].eq("active_1630_2130")].copy()
    fig, axes = plt.subplots(1, 2, figsize=(10.2, 4.2), sharey=True)
    x = np.arange(len(CONTROL_ORDER))
    for ax, direction in zip(axes, DIRECTION_ORDER):
        sub = active[active["direction_group"].eq(direction)].set_index("control_id").reindex(CONTROL_ORDER)
        ax.bar(
            x,
            sub["mean_dd_log_speed"],
            color="#2b6cb0" if direction == POLICY_DIRECTION else "#c44e52",
            alpha=0.84,
        )
        ax.errorbar(
            x,
            sub["mean_dd_log_speed"],
            yerr=1.96 * sub["se_dd_log_speed"],
            fmt="none",
            ecolor="#333333",
            elinewidth=0.8,
            capsize=2,
        )
        labels = sub["control_label"]
        ax.axhline(0, color="#111111", linewidth=0.8)
        ax.set_xticks(x)
        ax.set_xticklabels(labels, rotation=25, ha="right")
        ax.set_title(DIRECTION_LABELS[direction], loc="left", fontweight="bold")
        ax.grid(axis="y", color="#e5e5e5", linewidth=0.5)
        for spine in ["top", "right"]:
            ax.spines[spine].set_visible(False)
    axes[0].set_ylabel("Caitian-control DD in log speed change")
    fig.suptitle("Direction-specific DD, active window vs reference, weekdays", x=0.01, ha="left")
    fig.tight_layout(rect=(0, 0, 1, 0.92))
    save_figure(fig, output_path)


def plot_daily_direction_dd(daily: pd.DataFrame, output_path: Path) -> None:
    active = daily[daily["window"].eq("active_1630_2130")].copy()
    fig, axes = plt.subplots(1, 2, figsize=(10.2, 4.0), sharey=True)
    for ax, direction in zip(axes, DIRECTION_ORDER):
        sub = active[active["direction_group"].eq(direction)]
        x = np.arange(len(CONTROL_ORDER))
        for i, control_id in enumerate(CONTROL_ORDER):
            part = sub[sub["control_id"].eq(control_id)].sort_values("date")
            jitter = np.linspace(-0.08, 0.08, len(part)) if len(part) else []
            ax.scatter(
                np.repeat(i, len(part)) + jitter,
                part["dd_log_speed"],
                color="#2b6cb0" if direction == POLICY_DIRECTION else "#c44e52",
                alpha=0.86,
                s=26,
                edgecolor="white",
                linewidth=0.45,
                zorder=3,
            )
            ax.hlines(
                part["dd_log_speed"].mean(),
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
        ax.set_title(DIRECTION_LABELS[direction], loc="left", fontweight="bold")
        ax.grid(axis="y", color="#e5e5e5", linewidth=0.5)
        for spine in ["top", "right"]:
            ax.spines[spine].set_visible(False)
    axes[0].set_ylabel("Daily Caitian-control DD in log speed change")
    fig.suptitle("Daily direction-specific DD, active window, weekdays", x=0.01, ha="left")
    fig.tight_layout(rect=(0, 0, 1, 0.92))
    save_figure(fig, output_path)


def main() -> None:
    args = parse_args()
    configure_plot()
    data_dir = args.output_dir / "data"
    metrics_dir = args.output_dir / "metrics"
    figures_dir = args.output_dir / "figures"
    corridor = pd.read_csv(data_dir / "corridor_10min_speed.csv")
    window_panel = build_window_direction_panel(corridor)
    deltas = build_direction_deltas(window_panel)
    summary, daily = build_direction_dd(deltas)

    window_panel.to_csv(data_dir / "post_only_direction_window_daily.csv", index=False)
    deltas.to_csv(data_dir / "post_only_direction_window_deltas.csv", index=False)
    daily.to_csv(data_dir / "post_only_direction_daily_dd.csv", index=False)
    summary.to_csv(metrics_dir / "post_only_direction_dd_summary.csv", index=False)
    spec = {
        "estimand": "Direction-specific post-only active/reference speed change in Caitian relative to each control.",
        "outcome": "log corridor speed and speed in km/h, computed from corridor harmonic speed.",
        "reference_window": "15:00-16:30",
        "main_active_window": "16:30-21:30",
        "control_for_time_trend": (
            "For each direction and weekday, the control corridor's active/reference change "
            "is subtracted from Caitian's active/reference change."
        ),
        "policy_direction": DIRECTION_LABELS[POLICY_DIRECTION],
        "opposite_direction": DIRECTION_LABELS[OPPOSITE_DIRECTION],
        "interpretation": (
            "A positive policy-direction DD supports a relative speed gain in the policy direction. "
            "An opposite-direction DD close to zero would support limited displacement cost; "
            "a negative value indicates that the opposite direction slows relative to controls. "
            "The design remains post-only and does not estimate an opening effect."
        ),
    }
    with (metrics_dir / "post_only_direction_decomposition_spec.json").open("w", encoding="utf-8") as fh:
        json.dump(spec, fh, ensure_ascii=False, indent=2)

    plot_direction_speed_changes(
        deltas,
        figures_dir / "post_only_direction_active_reference_ratios.png",
    )
    plot_direction_dd(
        summary,
        figures_dir / "post_only_direction_dd_by_control.png",
    )
    plot_daily_direction_dd(
        daily,
        figures_dir / "post_only_direction_daily_dd.png",
    )


if __name__ == "__main__":
    main()
