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

POLICY_DATE = pd.Timestamp("2026-03-23")
TREATMENT_DIRECTION = "西向东"
PM_CORE = (17 * 60 + 30, 20 * 60)
AFTERNOON_REFERENCE = (15 * 60, 16 * 60)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Estimate event-study coefficients for the Liuxian Avenue Gaode sample."
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
            "axes.titlesize": 11,
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
    prefix = "pre" if series.min() < POLICY_DATE else "post"
    return f"{prefix}_{min_date}_{max_date}"


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
        df["date"] = pd.to_datetime(df["datetime"].dt.date)
        df["dow"] = df["datetime"].dt.dayofweek
        df["minute_of_day"] = df["datetime"].dt.hour * 60 + df["datetime"].dt.minute
        df["speed_kmh"] = pd.to_numeric(df["速度"], errors="coerce")
        df["segment_length_m"] = pd.to_numeric(df["路段长度"], errors="coerce")
        df["source_file"] = path.name
        frames.append(df)
    if not frames:
        raise FileNotFoundError(f"No CSV files found under {input_dir}")

    panel = pd.concat(frames, ignore_index=True)
    segment_sets = {
        period: set(group["路段编号"].unique()) for period, group in panel.groupby("period")
    }
    common_segments = set.intersection(*segment_sets.values())
    panel = panel[panel["路段编号"].isin(common_segments)].copy()
    panel = panel[(panel["speed_kmh"] > 0) & (panel["segment_length_m"] > 0)].copy()
    panel["segment_travel_time_min"] = (
        panel["segment_length_m"] / 1000.0 / panel["speed_kmh"] * 60.0
    )
    panel["log_travel_time"] = np.log(panel["segment_travel_time_min"])
    panel["event_day"] = (panel["date"] - POLICY_DATE).dt.days
    panel["treated"] = panel["路段方向"].eq(TREATMENT_DIRECTION).astype(int)
    panel["segment_id"] = panel["路段编号"].astype(str)
    panel["datetime_id"] = panel["datetime"].dt.strftime("%Y%m%d%H%M")
    panel["date_id"] = panel["date"].dt.strftime("%Y%m%d")
    return panel


def choose_reference_event_day(event_days: list[int]) -> int:
    pre_days = [day for day in event_days if day < 0]
    if not pre_days:
        raise ValueError("No pre-policy event days are available for normalization.")
    return max(pre_days)


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


def fit_event_study(
    df: pd.DataFrame,
    outcome_col: str,
    unit_fe_col: str,
    time_fe_col: str,
    cluster_col: str,
    reference_event_day: int,
    model_name: str,
) -> tuple[pd.DataFrame, dict[str, float | int | str]]:
    event_days = sorted(int(day) for day in df["event_day"].dropna().unique())
    estimated_days = [day for day in event_days if day != reference_event_day]

    parts = [
        pd.Series(1.0, index=df.index, name="const"),
        pd.get_dummies(df[unit_fe_col].astype(str), prefix="unit", drop_first=True, dtype=float),
        pd.get_dummies(df[time_fe_col].astype(str), prefix="time", drop_first=True, dtype=float),
    ]
    event_columns = {}
    for day in estimated_days:
        col = f"treated_x_event_{day}"
        parts.append(((df["treated"].eq(1)) & (df["event_day"].eq(day))).astype(float).rename(col))
        event_columns[day] = col

    x_df = pd.concat(parts, axis=1)
    x = x_df.to_numpy(dtype=float)
    y = df[outcome_col].to_numpy(dtype=float)

    beta, *_ = np.linalg.lstsq(x, y, rcond=None)
    with np.errstate(divide="ignore", over="ignore", invalid="ignore"):
        fitted = x @ beta
    if not np.isfinite(beta).all() or not np.isfinite(fitted).all():
        raise FloatingPointError("Event-study regression produced non-finite estimates")
    residual = y - fitted
    vcov = cluster_covariance(x, residual, df[cluster_col].astype(str).to_numpy())
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
                "coef_log_points": coef,
                "std_error": std_err,
                "ci95_low_log_points": coef - 1.96 * std_err if np.isfinite(std_err) else np.nan,
                "ci95_high_log_points": coef + 1.96 * std_err if np.isfinite(std_err) else np.nan,
                "effect_pct": 100 * (np.exp(coef) - 1),
                "ci95_low_pct": 100 * (np.exp(coef - 1.96 * std_err) - 1)
                if np.isfinite(std_err)
                else np.nan,
                "ci95_high_pct": 100 * (np.exp(coef + 1.96 * std_err) - 1)
                if np.isfinite(std_err)
                else np.nan,
                "calendar_date": (POLICY_DATE + pd.Timedelta(days=day)).strftime("%Y-%m-%d"),
            }
        )

    diagnostics = {
        "model": model_name,
        "n_obs": int(len(df)),
        "n_units": int(df[unit_fe_col].nunique()),
        "n_time_fe": int(df[time_fe_col].nunique()),
        "n_clusters": int(df[cluster_col].nunique()),
        "n_parameters": int(x.shape[1]),
        "reference_event_day": int(reference_event_day),
        "reference_calendar_date": (POLICY_DATE + pd.Timedelta(days=reference_event_day)).strftime(
            "%Y-%m-%d"
        ),
        "r_squared": float(1 - np.sum(residual**2) / np.sum((y - y.mean()) ** 2)),
    }
    return pd.DataFrame(rows), diagnostics


def build_pm10min_sample(panel: pd.DataFrame) -> pd.DataFrame:
    lo, hi = PM_CORE
    sample = panel[
        (panel["dow"] < 5) & (panel["minute_of_day"] >= lo) & (panel["minute_of_day"] < hi)
    ].copy()
    sample = sample.dropna(subset=["log_travel_time", "event_day", "treated"])
    return sample


def build_pm15_reference_sample(panel: pd.DataFrame) -> pd.DataFrame:
    pm_lo, pm_hi = PM_CORE
    ref_lo, ref_hi = AFTERNOON_REFERENCE
    pieces = []
    for window, lo, hi in [
        ("pm_core", pm_lo, pm_hi),
        ("afternoon15", ref_lo, ref_hi),
    ]:
        sub = panel[
            (panel["dow"] < 5) & (panel["minute_of_day"] >= lo) & (panel["minute_of_day"] < hi)
        ]
        grouped = (
            sub.groupby(
                [
                    "segment_id",
                    "路段名称（唯一）",
                    "路段方向",
                    "date",
                    "date_id",
                    "dow",
                    "event_day",
                    "treated",
                ],
                as_index=False,
            )
            .agg(travel_time_min=("segment_travel_time_min", "mean"), n_obs=("segment_id", "size"))
        )
        grouped["window"] = window
        pieces.append(grouped)

    daily = pd.concat(pieces, ignore_index=True)
    wide = daily.pivot_table(
        index=[
            "segment_id",
            "路段名称（唯一）",
            "路段方向",
            "date",
            "date_id",
            "dow",
            "event_day",
            "treated",
        ],
        columns="window",
        values="travel_time_min",
        aggfunc="first",
    ).reset_index()
    wide.columns.name = None
    wide = wide.dropna(subset=["pm_core", "afternoon15"]).copy()
    wide["log_pm_over_15"] = np.log(wide["pm_core"]) - np.log(wide["afternoon15"])
    return wide


def plot_coefficients(coef_frames: list[pd.DataFrame], output_path: Path) -> None:
    fig, axes = plt.subplots(2, 1, figsize=(8.2, 6.4), sharex=True)
    panel_specs = [
        (
            "pm_10min_did",
            "A. PM window DID event study",
            "Effect on treated segments (%)",
            axes[0],
        ),
        (
            "pm_over_15_daily_did",
            "B. Daily PM/15:00 ratio DID event study",
            "Effect on treated ratio (%)",
            axes[1],
        ),
    ]
    coef_all = pd.concat(coef_frames, ignore_index=True)

    for model, title, ylabel, ax in panel_specs:
        data = coef_all[coef_all["model"] == model].sort_values("event_day")
        for side, color, label in [
            ("pre", "#3b6ea8", "Pre-policy observed days"),
            ("post", "#c44e52", "Post-policy observed days"),
        ]:
            part = data[data["event_day"] < 0] if side == "pre" else data[data["event_day"] > 0]
            part = part.sort_values("event_day")
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
                    s=30,
                    color=color,
                    label="Omitted pre day" if side == "pre" else None,
                    zorder=3,
                )
            if len(part) > 1:
                ax.plot(part["event_day"], part["effect_pct"], color=color, linewidth=1.0, alpha=0.55)

        ax.axhline(0, color="#555555", linewidth=0.8)
        ax.axvline(0, color="#222222", linewidth=0.9, linestyle="--")
        ax.set_title(title, loc="left", fontweight="bold")
        ax.set_ylabel(ylabel)
        ax.grid(axis="y", color="#d9d9d9", linewidth=0.6, alpha=0.8)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    observed_days = sorted(coef_all["event_day"].unique())
    axes[-1].set_xticks(observed_days)
    axes[-1].set_xticklabels([str(int(day)) for day in observed_days], rotation=45, ha="right")
    axes[-1].tick_params(axis="x", labelsize=8)
    axes[-1].set_xlabel("Event day relative to 2026-03-23 policy start")
    axes[0].legend(loc="upper left", frameon=False, ncol=2)
    fig.suptitle("Liuxian Avenue west-to-east tidal-lane event study", y=0.98, fontsize=12)
    fig.text(
        0.01,
        0.01,
        "Notes: Treated = west-to-east common segments; controls = other common Liuxian segments. "
        "Omitted day is the last observed pre-policy weekday. 95% CI uses segment-clustered CR1 SEs.",
        fontsize=7.5,
        color="#444444",
    )
    fig.tight_layout(rect=(0, 0.045, 1, 0.955))
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    args = parse_args()
    configure_plot()
    dirs = ensure_output_dirs(args.output_dir)

    panel = load_panel(args.input_dir)
    event_days = sorted(int(day) for day in panel.loc[panel["dow"] < 5, "event_day"].unique())
    reference_event_day = choose_reference_event_day(event_days)

    pm10 = build_pm10min_sample(panel)
    pm10.to_csv(dirs["data"] / "event_study_pm10min_sample.csv", index=False, encoding="utf-8-sig")
    pm10_coef, pm10_diag = fit_event_study(
        pm10,
        outcome_col="log_travel_time",
        unit_fe_col="segment_id",
        time_fe_col="datetime_id",
        cluster_col="segment_id",
        reference_event_day=reference_event_day,
        model_name="pm_10min_did",
    )

    pm15 = build_pm15_reference_sample(panel)
    pm15.to_csv(dirs["data"] / "event_study_pm_over_15_daily_sample.csv", index=False, encoding="utf-8-sig")
    pm15_coef, pm15_diag = fit_event_study(
        pm15,
        outcome_col="log_pm_over_15",
        unit_fe_col="segment_id",
        time_fe_col="date_id",
        cluster_col="segment_id",
        reference_event_day=reference_event_day,
        model_name="pm_over_15_daily_did",
    )

    pm10_coef.to_csv(
        dirs["metrics"] / "event_study_pm10min_coefficients.csv",
        index=False,
        encoding="utf-8-sig",
    )
    pm15_coef.to_csv(
        dirs["metrics"] / "event_study_pm_over_15_daily_coefficients.csv",
        index=False,
        encoding="utf-8-sig",
    )
    with (dirs["metrics"] / "event_study_metadata.json").open("w", encoding="utf-8") as fh:
        json.dump(
            {
                "policy_date": POLICY_DATE.strftime("%Y-%m-%d"),
                "treatment_direction": TREATMENT_DIRECTION,
                "pm_core_window": "17:30-20:00",
                "afternoon_reference_window": "15:00-16:00",
                "reference_event_day": reference_event_day,
                "models": [pm10_diag, pm15_diag],
            },
            fh,
            ensure_ascii=False,
            indent=2,
        )

    plot_coefficients(
        [pm10_coef, pm15_coef],
        dirs["figures"] / "event_study_treatment_westeast_pm.png",
    )


if __name__ == "__main__":
    main()
