from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data_work/outputs/shenzhen_caitian_baidu_analysis_v1"

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from run_caitian_post_only_analysis import fit_event_study, save_figure  # noqa: E402


POLICY_DIRECTION = "policy_direction_futian_to_longhua"
OPPOSITE_DIRECTION = "opposite_direction_longhua_to_futian"
DIRECTION_ORDER = [POLICY_DIRECTION, OPPOSITE_DIRECTION]
DIRECTION_LABELS = {
    POLICY_DIRECTION: "Futian -> Longhua",
    OPPOSITE_DIRECTION: "Longhua -> Futian",
}
CONTROL_LABELS = {
    "pooled_controls": "Pooled controls",
    "control_huanggang": "Huanggang Rd",
    "control_jintian": "Jintian Rd",
    "control_yitian": "Yitian Rd",
    "control_xiangmihu": "Xiangmihu Rd",
}
CONTROL_SETS = {
    "pooled_controls": [
        "control_huanggang",
        "control_jintian",
        "control_yitian",
        "control_xiangmihu",
    ],
    "control_huanggang": ["control_huanggang"],
    "control_jintian": ["control_jintian"],
    "control_yitian": ["control_yitian"],
    "control_xiangmihu": ["control_xiangmihu"],
}
EVENT_START_MIN = 16 * 60 + 30
ACTIVE_END_MIN = 21 * 60 + 30
SAMPLE_WINDOW = (14 * 60 + 30, 23 * 60 + 30)
REFERENCE_BIN = -30


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Post-only 16:30 event-study DID for Caitian Road against controls."
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


def build_did_sample(
    panel: pd.DataFrame,
    direction: str,
    control_set: str,
    control_ids: list[str],
) -> pd.DataFrame:
    sample = panel[
        panel["day_type"].eq("weekday")
        & panel["direction_group"].eq(direction)
        & panel["corridor_id"].isin(["caitian"] + control_ids)
        & (panel["minute_of_day"] >= SAMPLE_WINDOW[0])
        & (panel["minute_of_day"] < SAMPLE_WINDOW[1])
    ].copy()
    sample["treated_caitian"] = sample["corridor_id"].eq("caitian").astype(int)
    sample["event_time_min"] = sample["minute_of_day"] - EVENT_START_MIN
    sample["event_bin_min"] = np.floor(sample["event_time_min"] / 30).astype(int) * 30
    sample["segment_id"] = sample["roadseg_id"].astype(str)
    sample["control_set"] = control_set
    sample["control_label"] = CONTROL_LABELS[control_set]
    sample["direction_label"] = DIRECTION_LABELS[direction]
    return sample


def estimate_event_studies(panel: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, object]]:
    sample_rows = []
    coef_rows = []
    diagnostics: dict[str, object] = {}
    for control_set, control_ids in CONTROL_SETS.items():
        for direction in DIRECTION_ORDER:
            sample = build_did_sample(panel, direction, control_set, control_ids)
            sample_rows.append(sample)
            model_name = f"{DIRECTION_LABELS[direction]} vs {CONTROL_LABELS[control_set]}"
            coef, diag = fit_event_study(
                sample,
                outcome_col="speed_kmh",
                interaction_col="treated_caitian",
                unit_col="segment_id",
                reference_bin=REFERENCE_BIN,
                model_name=model_name,
            )
            coef["control_set"] = control_set
            coef["control_label"] = CONTROL_LABELS[control_set]
            coef["direction_group"] = direction
            coef["direction_label"] = DIRECTION_LABELS[direction]
            coef_rows.append(coef)
            diagnostics[f"{control_set}::{direction}"] = diag
    return pd.concat(coef_rows, ignore_index=True), pd.concat(sample_rows, ignore_index=True), diagnostics


def plot_one_axis(ax: plt.Axes, data: pd.DataFrame, color: str, title: str) -> None:
    data = data.sort_values("event_bin_min")
    ax.errorbar(
        data["event_bin_min"],
        data["coef"],
        yerr=[
            data["coef"] - data["ci95_low"],
            data["ci95_high"] - data["coef"],
        ],
        fmt="o-",
        color=color,
        linewidth=1.1,
        markersize=3.2,
        capsize=2.0,
    )
    ax.axhline(0, color="#222222", linewidth=0.8)
    ax.axvline(0, color="#555555", linewidth=0.8, linestyle="--")
    ax.axvline(ACTIVE_END_MIN - EVENT_START_MIN, color="#555555", linewidth=0.8, linestyle=":")
    ax.axvspan(0, ACTIVE_END_MIN - EVENT_START_MIN, color="#eeeeee", alpha=0.62)
    ax.set_title(title, loc="left", fontweight="bold")
    ax.set_xlabel("Minutes relative to 16:30")
    ax.grid(axis="y", color="#e5e5e5", linewidth=0.5)
    for spine in ["top", "right"]:
        ax.spines[spine].set_visible(False)


def plot_pooled(coefficients: pd.DataFrame, output_path: Path) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(11.0, 4.2), sharey=True)
    colors = {
        POLICY_DIRECTION: "#2b6cb0",
        OPPOSITE_DIRECTION: "#c44e52",
    }
    for ax, direction in zip(axes, DIRECTION_ORDER):
        sub = coefficients[
            coefficients["control_set"].eq("pooled_controls")
            & coefficients["direction_group"].eq(direction)
        ].copy()
        plot_one_axis(ax, sub, colors[direction], DIRECTION_LABELS[direction])
    axes[0].set_ylabel("Caitian-control DID coefficient, km/h")
    fig.suptitle("Post-only event-study DID, Caitian vs pooled controls", x=0.01, ha="left")
    fig.tight_layout(rect=(0, 0, 1, 0.92))
    save_figure(fig, output_path)


def plot_by_control(coefficients: pd.DataFrame, output_path: Path) -> None:
    control_order = [
        "control_huanggang",
        "control_jintian",
        "control_yitian",
        "control_xiangmihu",
    ]
    fig, axes = plt.subplots(2, 4, figsize=(14.0, 6.7), sharex=True, sharey=True)
    colors = {
        POLICY_DIRECTION: "#2b6cb0",
        OPPOSITE_DIRECTION: "#c44e52",
    }
    for row, direction in enumerate(DIRECTION_ORDER):
        for col, control_set in enumerate(control_order):
            ax = axes[row, col]
            sub = coefficients[
                coefficients["control_set"].eq(control_set)
                & coefficients["direction_group"].eq(direction)
            ].copy()
            title = CONTROL_LABELS[control_set] if row == 0 else ""
            plot_one_axis(ax, sub, colors[direction], title)
            if col == 0:
                ax.set_ylabel(f"{DIRECTION_LABELS[direction]}\nDID coef, km/h")
            else:
                ax.set_ylabel("")
    fig.suptitle("Post-only event-study DID by control corridor", x=0.01, ha="left")
    fig.tight_layout(rect=(0, 0, 1, 0.94))
    save_figure(fig, output_path)


def main() -> None:
    args = parse_args()
    configure_plot()
    data_dir = args.output_dir / "data"
    metrics_dir = args.output_dir / "metrics"
    figures_dir = args.output_dir / "figures"
    panel = pd.read_csv(data_dir / "selected_segment_speed_panel.csv")
    coefficients, sample, diagnostics = estimate_event_studies(panel)

    sample.to_csv(data_dir / "caitian_1630_event_study_did_sample.csv", index=False)
    coefficients.to_csv(metrics_dir / "caitian_1630_event_study_did_coefficients.csv", index=False)
    with (metrics_dir / "caitian_1630_event_study_did_diagnostics.json").open(
        "w", encoding="utf-8"
    ) as fh:
        json.dump(diagnostics, fh, ensure_ascii=False, indent=2)
    spec = {
        "estimand": "Post-only within-day event-study DID for Caitian Road implementation window.",
        "outcome": "10-minute road-segment speed in km/h.",
        "sample": "Weekdays in the 2025-05-19 to 2025-05-25 Baidu week, 14:30-23:30.",
        "event_time_zero": "16:30, the corrected Caitian Road tidal-lane start time.",
        "active_window": "16:30-21:30",
        "omitted_bin": "16:00-16:30, event_bin_min = -30",
        "directions": DIRECTION_LABELS,
        "controls": CONTROL_LABELS,
        "fixed_effects": ["road segment", "date", "minute_of_day"],
        "cluster": "road segment",
        "specification": (
            "speed_{i d m} = alpha_i + delta_d + lambda_m + "
            "sum_{k != -30} beta_k Caitian_i * 1[event_bin_m = k] + epsilon_{i d m}. "
            "The model is estimated separately by direction and control set."
        ),
        "interpretation": (
            "beta_k is the Caitian-control speed gap in event bin k minus the gap in "
            "16:00-16:30, net of road-segment, date, and minute-of-day fixed effects. "
            "This is a post-only within-day comparison, not an opening-effect event study."
        ),
    }
    with (metrics_dir / "caitian_1630_event_study_did_spec.json").open("w", encoding="utf-8") as fh:
        json.dump(spec, fh, ensure_ascii=False, indent=2)

    plot_pooled(
        coefficients,
        figures_dir / "caitian_1630_event_study_did_pooled_controls.png",
    )
    plot_by_control(
        coefficients,
        figures_dir / "caitian_1630_event_study_did_by_control.png",
    )


if __name__ == "__main__":
    main()
