from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from event_study_liuxian import (
    DEFAULT_INPUT_DIR,
    DEFAULT_OUTPUT_DIR,
    PM_CORE,
    POLICY_DATE,
    TREATMENT_DIRECTION,
    choose_reference_event_day,
    cluster_covariance,
    configure_plot,
    ensure_output_dirs,
    load_panel,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Estimate a speed-level event study for the Liuxian Avenue Gaode sample."
    )
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def build_pm_speed_sample(panel: pd.DataFrame) -> pd.DataFrame:
    lo, hi = PM_CORE
    sample = panel[
        (panel["dow"] < 5)
        & (panel["minute_of_day"] >= lo)
        & (panel["minute_of_day"] < hi)
    ].copy()
    return sample.dropna(subset=["speed_kmh", "event_day", "treated"])


def build_pm_speed_all_days_sample(panel: pd.DataFrame) -> pd.DataFrame:
    lo, hi = PM_CORE
    sample = panel[(panel["minute_of_day"] >= lo) & (panel["minute_of_day"] < hi)].copy()
    sample["day_type"] = np.where(sample["dow"] < 5, "Weekday", "Weekend")
    sample["dow_name"] = sample["datetime"].dt.day_name()
    return sample.dropna(subset=["speed_kmh", "event_day", "treated"])


def fit_speed_event_study(
    df: pd.DataFrame,
    *,
    reference_event_day: int,
    model_name: str,
) -> tuple[pd.DataFrame, dict[str, float | int | str]]:
    event_days = sorted(int(day) for day in df["event_day"].dropna().unique())
    estimated_days = [day for day in event_days if day != reference_event_day]

    parts = [
        pd.Series(1.0, index=df.index, name="const"),
        pd.get_dummies(df["segment_id"].astype(str), prefix="segment", drop_first=True, dtype=float),
        pd.get_dummies(df["datetime_id"].astype(str), prefix="time", drop_first=True, dtype=float),
    ]
    event_columns = {}
    for day in estimated_days:
        col = f"treated_x_event_{day}"
        parts.append(((df["treated"].eq(1)) & (df["event_day"].eq(day))).astype(float).rename(col))
        event_columns[day] = col

    x_df = pd.concat(parts, axis=1)
    x = x_df.to_numpy(dtype=float)
    y = df["speed_kmh"].to_numpy(dtype=float)

    beta, *_ = np.linalg.lstsq(x, y, rcond=None)
    with np.errstate(divide="ignore", over="ignore", invalid="ignore"):
        fitted = x @ beta
    if not np.isfinite(beta).all() or not np.isfinite(fitted).all():
        raise FloatingPointError("Speed event-study regression produced non-finite estimates")

    residual = y - fitted
    vcov = cluster_covariance(x, residual, df["segment_id"].astype(str).to_numpy())
    se = np.sqrt(np.maximum(np.diag(vcov), 0.0))
    coef_lookup = dict(zip(x_df.columns, beta))
    se_lookup = dict(zip(x_df.columns, se))

    rows = []
    for day in event_days:
        if day == reference_event_day:
            coef = 0.0
            std_err = np.nan
        else:
            col = event_columns[day]
            coef = float(coef_lookup[col])
            std_err = float(se_lookup[col])
        rows.append(
            {
                "model": model_name,
                "event_day": day,
                "reference_event_day": reference_event_day,
                "coef_speed_kmh": coef,
                "std_error_speed_kmh": std_err,
                "ci95_low_speed_kmh": coef - 1.96 * std_err if np.isfinite(std_err) else np.nan,
                "ci95_high_speed_kmh": coef + 1.96 * std_err if np.isfinite(std_err) else np.nan,
                "calendar_date": (POLICY_DATE + pd.Timedelta(days=day)).strftime("%Y-%m-%d"),
                "dow_name": (POLICY_DATE + pd.Timedelta(days=day)).day_name(),
                "day_type": "Weekday"
                if (POLICY_DATE + pd.Timedelta(days=day)).dayofweek < 5
                else "Weekend",
            }
        )

    diagnostics = {
        "model": model_name,
        "outcome": "speed_kmh",
        "n_obs": int(len(df)),
        "n_segments": int(df["segment_id"].nunique()),
        "n_time_fe": int(df["datetime_id"].nunique()),
        "n_clusters": int(df["segment_id"].nunique()),
        "n_parameters": int(x.shape[1]),
        "reference_event_day": int(reference_event_day),
        "reference_calendar_date": (
            POLICY_DATE + pd.Timedelta(days=reference_event_day)
        ).strftime("%Y-%m-%d"),
        "r_squared": float(1 - np.sum(residual**2) / np.sum((y - y.mean()) ** 2)),
    }
    return pd.DataFrame(rows), diagnostics


def plot_speed_event_study(coefs: pd.DataFrame, output_path: Path) -> None:
    data = coefs.sort_values("event_day").copy()

    fig, ax = plt.subplots(figsize=(8.2, 4.6))
    for side, color, label in [
        ("pre", "#3b6ea8", "Pre-policy observed days"),
        ("post", "#c44e52", "Post-policy observed days"),
    ]:
        part = data[data["event_day"] < 0] if side == "pre" else data[data["event_day"] > 0]
        finite = part[np.isfinite(part["std_error_speed_kmh"])]
        if not finite.empty:
            yerr = np.vstack(
                [
                    finite["coef_speed_kmh"] - finite["ci95_low_speed_kmh"],
                    finite["ci95_high_speed_kmh"] - finite["coef_speed_kmh"],
                ]
            )
            ax.errorbar(
                finite["event_day"],
                finite["coef_speed_kmh"],
                yerr=yerr,
                fmt="o",
                color=color,
                ecolor=color,
                elinewidth=1.0,
                capsize=2.8,
                markersize=4.2,
                label=label,
            )
        omitted = part[~np.isfinite(part["std_error_speed_kmh"])]
        if not omitted.empty:
            ax.scatter(
                omitted["event_day"],
                omitted["coef_speed_kmh"],
                marker="D",
                s=34,
                color=color,
                label="Omitted pre day" if side == "pre" else None,
                zorder=3,
            )
        if len(part) > 1:
            ax.plot(part["event_day"], part["coef_speed_kmh"], color=color, linewidth=1.0, alpha=0.55)

    observed_days = sorted(data["event_day"].unique())
    ax.axhline(0, color="#555555", linewidth=0.8)
    ax.axvline(0, color="#222222", linewidth=0.9, linestyle="--")
    ax.set_xticks(observed_days)
    ax.set_xticklabels([str(int(day)) for day in observed_days], rotation=45, ha="right")
    ax.set_ylabel("Effect on treated west-to-east speed (km/h)")
    ax.set_xlabel("Event day relative to 2026-03-23 policy start")
    ax.set_title(
        "Liuxian Avenue west-to-east speed event study",
        loc="left",
        fontweight="bold",
    )
    ax.grid(axis="y", color="#d9d9d9", linewidth=0.6, alpha=0.8)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.legend(loc="upper left", frameon=False, ncol=2)
    fig.text(
        0.01,
        0.01,
        "Notes: Outcome is segment speed in km/h. Sample is weekdays, 17:30-20:00. "
        "FE absorb segment and exact 10-minute timestamp. 95% CI uses segment-clustered CR1 SEs.",
        fontsize=7.5,
        color="#444444",
    )
    fig.tight_layout(rect=(0, 0.055, 1, 1))
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)


def plot_speed_event_study_with_weekends(coefs: pd.DataFrame, output_path: Path) -> None:
    data = coefs.sort_values("event_day").copy()

    fig, ax = plt.subplots(figsize=(8.6, 4.8))
    styles = [
        ("pre_weekday", data[(data["event_day"] < 0) & (data["day_type"] == "Weekday")], "#3b6ea8", "o", "Pre weekday"),
        ("pre_weekend", data[(data["event_day"] < 0) & (data["day_type"] == "Weekend")], "#3b6ea8", "s", "Pre weekend placebo"),
        ("post_weekday", data[(data["event_day"] > 0) & (data["day_type"] == "Weekday")], "#c44e52", "o", "Post weekday"),
        ("post_weekend", data[(data["event_day"] > 0) & (data["day_type"] == "Weekend")], "#c44e52", "s", "Post weekend placebo"),
    ]
    for _, part, color, marker, label in styles:
        finite = part[np.isfinite(part["std_error_speed_kmh"])]
        if finite.empty:
            continue
        yerr = np.vstack(
            [
                finite["coef_speed_kmh"] - finite["ci95_low_speed_kmh"],
                finite["ci95_high_speed_kmh"] - finite["coef_speed_kmh"],
            ]
        )
        ax.errorbar(
            finite["event_day"],
            finite["coef_speed_kmh"],
            yerr=yerr,
            fmt=marker,
            color=color,
            ecolor=color,
            elinewidth=1.0,
            capsize=2.8,
            markersize=4.3,
            markerfacecolor="white" if marker == "s" else color,
            markeredgewidth=1.2,
            label=label,
        )

    omitted = data[~np.isfinite(data["std_error_speed_kmh"])]
    if not omitted.empty:
        ax.scatter(
            omitted["event_day"],
            omitted["coef_speed_kmh"],
            marker="D",
            s=36,
            color="#3b6ea8",
            label="Omitted day",
            zorder=3,
        )

    observed_days = sorted(data["event_day"].unique())
    ax.axhline(0, color="#555555", linewidth=0.8)
    ax.axvline(0, color="#222222", linewidth=0.9, linestyle="--")
    ax.set_xticks(observed_days)
    ax.set_xticklabels([str(int(day)) for day in observed_days], rotation=45, ha="right")
    ax.set_ylabel("Effect on treated west-to-east speed (km/h)")
    ax.set_xlabel("Event day relative to 2026-03-23 policy start")
    ax.set_title(
        "Liuxian Avenue speed event study with weekend placebo days",
        loc="left",
        fontweight="bold",
    )
    ax.grid(axis="y", color="#d9d9d9", linewidth=0.6, alpha=0.8)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.legend(loc="upper left", frameon=False, ncol=2)
    fig.text(
        0.01,
        0.01,
        "Notes: Outcome is segment speed in km/h. Sample is all days, 17:30-20:00. "
        "Weekend points are placebo days because the tidal lane operates on weekdays. "
        "FE absorb segment and exact 10-minute timestamp. 95% CI uses segment-clustered CR1 SEs.",
        fontsize=7.5,
        color="#444444",
    )
    fig.tight_layout(rect=(0, 0.065, 1, 1))
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    args = parse_args()
    configure_plot()
    dirs = ensure_output_dirs(args.output_dir)

    panel = load_panel(args.input_dir)
    sample = build_pm_speed_sample(panel)
    event_days = sorted(int(day) for day in sample["event_day"].unique())
    reference_event_day = choose_reference_event_day(event_days)

    sample.to_csv(
        dirs["data"] / "event_study_speed_pm_sample.csv",
        index=False,
        encoding="utf-8-sig",
    )
    coefs, diagnostics = fit_speed_event_study(
        sample,
        reference_event_day=reference_event_day,
        model_name="speed_pm_did",
    )
    coefs.to_csv(
        dirs["metrics"] / "event_study_speed_pm_coefficients.csv",
        index=False,
        encoding="utf-8-sig",
    )
    with (dirs["metrics"] / "event_study_speed_metadata.json").open("w", encoding="utf-8") as fh:
        json.dump(
            {
                "policy_date": POLICY_DATE.strftime("%Y-%m-%d"),
                "treatment_direction": TREATMENT_DIRECTION,
                "outcome": "speed_kmh",
                "sample": "weekdays, 17:30-20:00",
                "fixed_effects": ["segment FE", "exact 10-minute timestamp FE"],
                "cluster": "segment_id",
                "diagnostics": diagnostics,
            },
            fh,
            ensure_ascii=False,
            indent=2,
        )
    plot_speed_event_study(coefs, dirs["figures"] / "event_study_speed_westeast_pm.png")

    all_days_sample = build_pm_speed_all_days_sample(panel)
    all_days_sample.to_csv(
        dirs["data"] / "event_study_speed_pm_with_weekend_sample.csv",
        index=False,
        encoding="utf-8-sig",
    )
    all_days_coefs, all_days_diagnostics = fit_speed_event_study(
        all_days_sample,
        reference_event_day=reference_event_day,
        model_name="speed_pm_did_with_weekend_placebos",
    )
    all_days_coefs.to_csv(
        dirs["metrics"] / "event_study_speed_pm_with_weekend_coefficients.csv",
        index=False,
        encoding="utf-8-sig",
    )
    with (
        dirs["metrics"] / "event_study_speed_with_weekend_metadata.json"
    ).open("w", encoding="utf-8") as fh:
        json.dump(
            {
                "policy_date": POLICY_DATE.strftime("%Y-%m-%d"),
                "treatment_direction": TREATMENT_DIRECTION,
                "outcome": "speed_kmh",
                "sample": "all days, 17:30-20:00",
                "weekend_interpretation": "placebo days because the tidal lane operates on weekdays",
                "fixed_effects": ["segment FE", "exact 10-minute timestamp FE"],
                "cluster": "segment_id",
                "diagnostics": all_days_diagnostics,
            },
            fh,
            ensure_ascii=False,
            indent=2,
        )
    plot_speed_event_study_with_weekends(
        all_days_coefs,
        dirs["figures"] / "event_study_speed_westeast_pm_with_weekend.png",
    )


if __name__ == "__main__":
    main()
