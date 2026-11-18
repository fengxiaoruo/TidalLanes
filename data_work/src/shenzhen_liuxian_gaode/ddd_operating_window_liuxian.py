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
    POLICY_DATE,
    PM_CORE,
    TREATMENT_DIRECTION,
    configure_plot,
    ensure_output_dirs,
    load_panel,
)


SHOULDER_WINDOW = (15 * 60, 22 * 60)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Estimate operating-window DDD models for Liuxian Avenue."
    )
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def demean_by_group(values: np.ndarray, group_codes: np.ndarray) -> np.ndarray:
    out = values.copy()
    for code in np.unique(group_codes):
        loc = group_codes == code
        out[loc] -= out[loc].mean(axis=0)
    return out


def residualize_two_way(
    values: np.ndarray,
    fe1: np.ndarray,
    fe2: np.ndarray,
    *,
    max_iter: int = 500,
    tolerance: float = 1e-10,
) -> tuple[np.ndarray, int, float]:
    residual = np.asarray(values, dtype=float).copy()
    if residual.ndim == 1:
        residual = residual.reshape(-1, 1)

    last = residual.copy()
    for iteration in range(1, max_iter + 1):
        residual = demean_by_group(residual, fe1)
        residual = demean_by_group(residual, fe2)
        change = float(np.max(np.abs(residual - last)))
        if change < tolerance:
            return residual, iteration, change
        last = residual.copy()
    return residual, max_iter, change


def cluster_covariance(x: np.ndarray, residual: np.ndarray, clusters: np.ndarray) -> np.ndarray:
    with np.errstate(divide="ignore", over="ignore", invalid="ignore"):
        xtx_inv = np.linalg.pinv(x.T @ x)
    meat = np.zeros((x.shape[1], x.shape[1]))
    for cluster in pd.unique(clusters):
        loc = clusters == cluster
        with np.errstate(divide="ignore", over="ignore", invalid="ignore"):
            score = x[loc].T @ residual[loc]
        if not np.isfinite(score).all():
            raise FloatingPointError(f"Non-finite cluster score for cluster {cluster}")
        meat += np.outer(score, score)

    n_obs, n_params = x.shape
    n_clusters = len(pd.unique(clusters))
    if n_clusters > 1 and n_obs > n_params:
        correction = (n_clusters / (n_clusters - 1)) * ((n_obs - 1) / (n_obs - n_params))
    else:
        correction = 1.0
    with np.errstate(divide="ignore", over="ignore", invalid="ignore"):
        covariance = correction * xtx_inv @ meat @ xtx_inv
    if not np.isfinite(covariance).all():
        raise FloatingPointError("Non-finite cluster-robust covariance matrix")
    return covariance


def fit_absorbed_ols(
    df: pd.DataFrame,
    outcome_col: str,
    regressor_cols: list[str],
    fe1_col: str,
    fe2_col: str,
    cluster_col: str,
    model_name: str,
) -> tuple[pd.DataFrame, dict[str, int | float | str]]:
    sample = df.dropna(subset=[outcome_col, *regressor_cols, fe1_col, fe2_col, cluster_col]).copy()
    y = sample[outcome_col].to_numpy(dtype=float).reshape(-1, 1)
    x = sample[regressor_cols].to_numpy(dtype=float)
    fe1 = pd.factorize(sample[fe1_col].astype(str), sort=True)[0]
    fe2 = pd.factorize(sample[fe2_col].astype(str), sort=True)[0]

    y_resid, y_iter, y_change = residualize_two_way(y, fe1, fe2)
    x_resid, x_iter, x_change = residualize_two_way(x, fe1, fe2)
    with np.errstate(divide="ignore", over="ignore", invalid="ignore"):
        beta = np.linalg.pinv(x_resid.T @ x_resid) @ (x_resid.T @ y_resid)
        fitted = x_resid @ beta
    if not np.isfinite(beta).all() or not np.isfinite(fitted).all():
        raise FloatingPointError("Absorbed OLS produced non-finite estimates")
    residual = (y_resid - fitted).ravel()
    vcov = cluster_covariance(x_resid, residual, sample[cluster_col].astype(str).to_numpy())
    se = np.sqrt(np.maximum(np.diag(vcov), 0.0))

    rows = []
    for idx, regressor in enumerate(regressor_cols):
        coef = float(beta[idx, 0])
        std_err = float(se[idx])
        rows.append(
            {
                "model": model_name,
                "term": regressor,
                "coef_log_points": coef,
                "std_error": std_err,
                "t_stat": coef / std_err if std_err > 0 else np.nan,
                "ci95_low_log_points": coef - 1.96 * std_err,
                "ci95_high_log_points": coef + 1.96 * std_err,
                "effect_pct": 100 * (np.exp(coef) - 1),
                "ci95_low_pct": 100 * (np.exp(coef - 1.96 * std_err) - 1),
                "ci95_high_pct": 100 * (np.exp(coef + 1.96 * std_err) - 1),
            }
        )

    diagnostics = {
        "model": model_name,
        "n_obs": int(len(sample)),
        "n_regressors": int(len(regressor_cols)),
        "n_fe1": int(sample[fe1_col].nunique()),
        "n_fe2": int(sample[fe2_col].nunique()),
        "n_clusters": int(sample[cluster_col].nunique()),
        "fe1": fe1_col,
        "fe2": fe2_col,
        "cluster": cluster_col,
        "y_residualize_iterations": int(y_iter),
        "x_residualize_iterations": int(x_iter),
        "y_residualize_final_change": y_change,
        "x_residualize_final_change": x_change,
        "residualized_r_squared": float(
            1 - np.sum(residual**2) / np.sum((y_resid.ravel() - y_resid.mean()) ** 2)
        ),
    }
    return pd.DataFrame(rows), diagnostics


def prepare_sample(panel: pd.DataFrame, sample_name: str) -> pd.DataFrame:
    pm_lo, pm_hi = PM_CORE
    sample = panel[panel["dow"] < 5].copy()
    if sample_name == "weekday_shoulder_1500_2200":
        lo, hi = SHOULDER_WINDOW
        sample = sample[(sample["minute_of_day"] >= lo) & (sample["minute_of_day"] < hi)].copy()
    elif sample_name != "weekday_all_day":
        raise ValueError(f"Unknown sample_name: {sample_name}")

    sample["post"] = (sample["date"] >= POLICY_DATE).astype(int)
    sample["active_window"] = (
        (sample["minute_of_day"] >= pm_lo) & (sample["minute_of_day"] < pm_hi)
    ).astype(int)
    sample["treat_post_active"] = (
        sample["treated"] * sample["post"] * sample["active_window"]
    )
    sample["segment_minute_id"] = sample["segment_id"].astype(str) + "_" + sample[
        "minute_of_day"
    ].astype(str)
    sample["date_minute_id"] = sample["date_id"].astype(str) + "_" + sample[
        "minute_of_day"
    ].astype(str)
    sample["date_active_id"] = sample["date_id"].astype(str) + "_" + sample[
        "active_window"
    ].astype(str)
    return sample


def prepare_event_sample(panel: pd.DataFrame, sample_name: str) -> tuple[pd.DataFrame, list[str], int]:
    sample = prepare_sample(panel, sample_name)
    event_days = sorted(int(day) for day in sample["event_day"].unique())
    pre_days = [day for day in event_days if day < 0]
    reference_day = max(pre_days)
    regressor_cols = []
    for day in event_days:
        if day == reference_day:
            continue
        col = f"treated_active_event_{day}"
        sample[col] = (
            sample["treated"].eq(1)
            & sample["active_window"].eq(1)
            & sample["event_day"].eq(day)
        ).astype(int)
        regressor_cols.append(col)
    return sample, regressor_cols, reference_day


def plot_event_ddd(coefs: pd.DataFrame, reference_day: int, output_path: Path) -> None:
    model_titles = [
        ("ddd_event_weekday_all_day_date_fe", "A. Segment-minute FE + date FE"),
        ("ddd_event_weekday_all_day_date_active_fe", "B. Segment-minute FE + date x active-window FE"),
    ]
    fig, axes = plt.subplots(2, 1, figsize=(8.2, 6.4), sharex=True)

    for ax, (model, title) in zip(axes, model_titles):
        event = coefs[
            (coefs["model"] == model) & coefs["term"].str.startswith("treated_active_event_")
        ].copy()
        event["event_day"] = event["term"].str.replace(
            "treated_active_event_", "", regex=False
        ).astype(int)
        omitted = pd.DataFrame(
            [
                {
                    "model": model,
                    "term": f"treated_active_event_{reference_day}",
                    "coef_log_points": 0.0,
                    "std_error": np.nan,
                    "effect_pct": 0.0,
                    "ci95_low_pct": np.nan,
                    "ci95_high_pct": np.nan,
                    "event_day": reference_day,
                }
            ]
        )
        event = pd.concat([event, omitted], ignore_index=True).sort_values("event_day")

        for side, color, label in [
            ("pre", "#3b6ea8", "Pre-policy observed days"),
            ("post", "#c44e52", "Post-policy observed days"),
        ]:
            part = event[event["event_day"] < 0] if side == "pre" else event[event["event_day"] > 0]
            finite = part[np.isfinite(part["std_error"])]
            if not finite.empty:
                yerr = np.vstack(
                    [
                        finite["effect_pct"] - finite["ci95_low_pct"],
                        finite["ci95_high_pct"] - finite["effect_pct"],
                    ]
                )
                ax.errorbar(
                    finite["event_day"],
                    finite["effect_pct"],
                    yerr=yerr,
                    fmt="o",
                    color=color,
                    ecolor=color,
                    elinewidth=1.0,
                    capsize=2.8,
                    markersize=4.2,
                    label=label,
                )
            ref = part[~np.isfinite(part["std_error"])]
            if not ref.empty:
                ax.scatter(
                    ref["event_day"],
                    ref["effect_pct"],
                    marker="D",
                    s=32,
                    color=color,
                    label="Omitted pre day" if side == "pre" else None,
                    zorder=3,
                )
            if len(part) > 1:
                ax.plot(part["event_day"], part["effect_pct"], color=color, linewidth=1.0, alpha=0.55)

        ax.axhline(0, color="#555555", linewidth=0.8)
        ax.axvline(0, color="#222222", linewidth=0.9, linestyle="--")
        ax.set_ylabel("Effect on active-window\ntreated travel time (%)")
        ax.set_title(title, loc="left", fontweight="bold")
        ax.grid(axis="y", color="#d9d9d9", linewidth=0.6, alpha=0.8)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    observed_days = sorted(
        coefs.loc[coefs["term"].str.startswith("treated_active_event_"), "term"]
        .str.replace("treated_active_event_", "", regex=False)
        .astype(int)
        .unique()
        .tolist()
        + [reference_day]
    )
    axes[-1].set_xticks(observed_days)
    axes[-1].set_xticklabels([str(int(day)) for day in observed_days], rotation=45, ha="right")
    axes[-1].set_xlabel("Event day relative to 2026-03-23 policy start")
    axes[0].legend(loc="upper left", frameon=False, ncol=2)
    fig.suptitle("Liuxian Avenue operating-window DDD event study", y=0.98, fontsize=12)
    fig.text(
        0.01,
        0.01,
        "Notes: Outcome is log segment travel time. Treated = west-to-east segments. Active window = weekday 17:30-20:00. "
        "95% CI uses segment-clustered CR1 SEs.",
        fontsize=7.5,
        color="#444444",
    )
    fig.tight_layout(rect=(0, 0.05, 1, 0.95))
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    args = parse_args()
    configure_plot()
    dirs = ensure_output_dirs(args.output_dir)
    panel = load_panel(args.input_dir)

    results = []
    diagnostics = []
    fe_specs = [
        ("date_fe", "date_id", "segment-minute FE + date FE"),
        ("date_active_fe", "date_active_id", "segment-minute FE + date x active-window FE"),
        ("exact_timestamp_fe", "date_minute_id", "segment-minute FE + exact timestamp FE"),
    ]

    for sample_name in ["weekday_all_day", "weekday_shoulder_1500_2200"]:
        sample = prepare_sample(panel, sample_name)
        sample.to_csv(
            dirs["data"] / f"ddd_operating_window_{sample_name}_sample.csv",
            index=False,
            encoding="utf-8-sig",
        )
        for fe_label, fe2_col, fe_description in fe_specs:
            coef, diag = fit_absorbed_ols(
                sample,
                outcome_col="log_travel_time",
                regressor_cols=["treat_post_active"],
                fe1_col="segment_minute_id",
                fe2_col=fe2_col,
                cluster_col="segment_id",
                model_name=f"ddd_{sample_name}_{fe_label}",
            )
            results.append(coef)
            diagnostics.append({**diag, "fe_description": fe_description})

    event_sample, event_cols, reference_day = prepare_event_sample(panel, "weekday_all_day")
    event_results = []
    for fe_label, fe2_col, fe_description in fe_specs[:2]:
        event_coef, event_diag = fit_absorbed_ols(
            event_sample,
            outcome_col="log_travel_time",
            regressor_cols=event_cols,
            fe1_col="segment_minute_id",
            fe2_col=fe2_col,
            cluster_col="segment_id",
            model_name=f"ddd_event_weekday_all_day_{fe_label}",
        )
        event_results.append(event_coef)
        results.append(event_coef)
        diagnostics.append(
            {**event_diag, "reference_event_day": reference_day, "fe_description": fe_description}
        )

    result_df = pd.concat(results, ignore_index=True)
    result_df.to_csv(
        dirs["metrics"] / "ddd_operating_window_coefficients.csv",
        index=False,
        encoding="utf-8-sig",
    )
    with (dirs["metrics"] / "ddd_operating_window_metadata.json").open("w", encoding="utf-8") as fh:
        json.dump(
            {
                "policy_date": POLICY_DATE.strftime("%Y-%m-%d"),
                "treatment_direction": TREATMENT_DIRECTION,
                "active_window": "weekday 17:30-20:00",
                "shoulder_window": "weekday 15:00-22:00",
                "fixed_effect_specifications": [
                    "segment-minute FE + date FE",
                    "segment-minute FE + date x active-window FE",
                    "segment-minute FE + exact timestamp FE",
                ],
                "diagnostics": diagnostics,
            },
            fh,
            ensure_ascii=False,
            indent=2,
        )

    plot_event_ddd(
        pd.concat(event_results, ignore_index=True),
        reference_day,
        dirs["figures"] / "ddd_operating_window_event_study.png",
    )


if __name__ == "__main__":
    main()
