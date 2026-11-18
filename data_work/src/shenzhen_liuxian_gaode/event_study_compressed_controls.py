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

from src.shenzhen_liuxian_gaode.analyze_post_full_segments import load_full_gaode_panel  # noqa: E402
from src.shenzhen_liuxian_gaode.event_study_liuxian import (  # noqa: E402
    DEFAULT_INPUT_DIR,
    DEFAULT_OUTPUT_DIR,
    PM_CORE,
    POLICY_DATE,
    configure_plot,
    ensure_output_dirs,
)
from src.shenzhen_liuxian_gaode.plot_chain_window_speeds import assign_chain_direction  # noqa: E402


REFERENCE_WINDOW = (15 * 60, 17 * 60 + 30)
ACTIVE_WINDOW = PM_CORE
CHAIN_TREATMENT = "eastbound_chain"
RAW_TREATMENT = "西向东"
RAW_OPPOSITE = "东向西"

SPEC_ORDER = [
    "chain_common_opposite",
    "chain_active_reference_ddd",
]
SPEC_TITLES = {
    "chain_common_opposite": "A. Chain DID, opposite-direction control",
    "chain_all_observed_unbalanced": "B. Chain DID, all observed segments",
    "raw_westeast_all_other_common": "C. Raw-label DID, all other common controls",
    "chain_active_reference_ddd": "B. Active/reference DDD",
}
SPEC_NOTES = {
    "chain_common_opposite": "Treated = SW -> NE common segments; control = NE -> SW common segments.",
    "chain_all_observed_unbalanced": "Adds post-only endpoints; useful as sensitivity, not a strict balanced DID.",
    "raw_westeast_all_other_common": "Treated = raw west-to-east common segments; controls = all other common segments.",
    "chain_active_reference_ddd": "Treated = SW -> NE; control variation also uses 15:00-17:30 as non-active window.",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compressed-axis event-study variants for the Liuxian Avenue Gaode sample."
    )
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def add_balanced_flags(panel: pd.DataFrame) -> pd.DataFrame:
    out = panel.copy()
    segment_sets = {
        period: set(group["segment_id"].unique()) for period, group in out.groupby("source_period")
    }
    common = set.intersection(*segment_sets.values())
    out["prepost_coverage"] = np.where(out["segment_id"].isin(common), "common", "post_only")
    return out


def add_compressed_day_index(panel: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    out = panel.copy()
    dates = (
        out.loc[out["dow"] < 5, ["source_period", "date", "date_label", "dow_name"]]
        .drop_duplicates()
        .sort_values(["source_period", "date"])
        .copy()
    )
    pre = dates[dates["source_period"].eq("gaode_pre")].sort_values("date").copy()
    post = dates[dates["source_period"].eq("gaode_post")].sort_values("date").copy()
    pre["compressed_day"] = np.arange(-len(pre), 0)
    post["compressed_day"] = np.arange(1, len(post) + 1)
    date_index = pd.concat([pre, post], ignore_index=True)
    date_index["plot_pos"] = np.arange(len(date_index))
    date_index["period_plot_label"] = np.where(
        date_index["source_period"].eq("gaode_pre"), "Pre", "Post"
    )
    date_index["tick_label"] = (
        date_index["period_plot_label"]
        + "\n"
        + date_index["date_label"]
        + "\n"
        + date_index["dow_name"].str.slice(0, 3)
    )
    out = out.merge(
        date_index[["date", "compressed_day", "plot_pos", "tick_label"]],
        on="date",
        how="left",
    )
    reference_day = -1
    return out, date_index, reference_day


def build_design_sample(
    panel: pd.DataFrame,
    *,
    spec_name: str,
) -> pd.DataFrame:
    lo, hi = ACTIVE_WINDOW
    sample = panel[
        (panel["dow"] < 5)
        & (panel["minute_of_day"] >= lo)
        & (panel["minute_of_day"] < hi)
    ].copy()
    if spec_name == "chain_common_opposite":
        sample = sample[sample["prepost_coverage"].eq("common")].copy()
        sample["treated_spec"] = sample["chain_direction"].eq(CHAIN_TREATMENT).astype(int)
    elif spec_name == "chain_all_observed_unbalanced":
        sample["treated_spec"] = sample["chain_direction"].eq(CHAIN_TREATMENT).astype(int)
    elif spec_name == "raw_westeast_all_other_common":
        sample = sample[sample["prepost_coverage"].eq("common")].copy()
        sample["treated_spec"] = sample["路段方向"].eq(RAW_TREATMENT).astype(int)
    else:
        raise ValueError(f"Unknown DID spec: {spec_name}")
    sample["datetime_id"] = sample["datetime"].dt.strftime("%Y%m%d%H%M")
    sample["cluster_id"] = sample["segment_id"].astype(str)
    return sample.dropna(subset=["speed_kmh", "treated_spec", "compressed_day", "datetime_id"])


def build_ddd_sample(panel: pd.DataFrame) -> pd.DataFrame:
    lo = REFERENCE_WINDOW[0]
    hi = ACTIVE_WINDOW[1]
    sample = panel[
        (panel["dow"] < 5)
        & (panel["minute_of_day"] >= lo)
        & (panel["minute_of_day"] < hi)
        & panel["prepost_coverage"].eq("common")
    ].copy()
    active_lo, active_hi = ACTIVE_WINDOW
    sample["treated_spec"] = sample["chain_direction"].eq(CHAIN_TREATMENT).astype(int)
    sample["active_window"] = (
        (sample["minute_of_day"] >= active_lo) & (sample["minute_of_day"] < active_hi)
    ).astype(int)
    sample["segment_minute_id"] = sample["segment_id"].astype(str) + "_" + sample[
        "minute_of_day"
    ].astype(str)
    sample["date_active_id"] = sample["date"].dt.strftime("%Y%m%d") + "_" + sample[
        "active_window"
    ].astype(str)
    sample["cluster_id"] = sample["segment_id"].astype(str)
    return sample.dropna(subset=["speed_kmh", "treated_spec", "active_window", "compressed_day"])


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


def cluster_covariance_stable(x: np.ndarray, residual: np.ndarray, clusters: np.ndarray) -> np.ndarray:
    xtx = np.einsum("ni,nj->ij", x, x)
    xtx_inv = np.linalg.pinv(xtx)
    meat = np.zeros((x.shape[1], x.shape[1]))
    for cluster in pd.unique(clusters):
        loc = clusters == cluster
        score = np.einsum("ni,n->i", x[loc], residual[loc])
        meat += np.outer(score, score)

    n_obs, n_params = x.shape
    n_clusters = len(pd.unique(clusters))
    if n_clusters > 1 and n_obs > n_params:
        correction = (n_clusters / (n_clusters - 1)) * ((n_obs - 1) / (n_obs - n_params))
    else:
        correction = 1.0
    covariance = correction * xtx_inv.dot(meat).dot(xtx_inv)
    if not np.isfinite(covariance).all():
        raise FloatingPointError("Non-finite cluster-robust covariance matrix")
    return covariance


def fit_event_model(
    df: pd.DataFrame,
    *,
    spec_name: str,
    reference_day: int,
    unit_fe_col: str,
    time_fe_col: str,
    interaction_type: str,
) -> tuple[pd.DataFrame, dict[str, int | float | str]]:
    event_days = sorted(int(day) for day in df["compressed_day"].dropna().unique())
    estimated_days = [day for day in event_days if day != reference_day]

    parts = []
    event_columns: dict[int, str] = {}
    for day in estimated_days:
        col = f"event_{day}"
        if interaction_type == "did":
            values = df["treated_spec"].eq(1) & df["compressed_day"].eq(day)
        elif interaction_type == "ddd":
            values = (
                df["treated_spec"].eq(1)
                & df["active_window"].eq(1)
                & df["compressed_day"].eq(day)
            )
        else:
            raise ValueError(f"Unknown interaction type: {interaction_type}")
        parts.append(values.astype(float).rename(col))
        event_columns[day] = col

    x_df = pd.concat(parts, axis=1)
    x = x_df.to_numpy(dtype=float)
    y = df["speed_kmh"].to_numpy(dtype=float).reshape(-1, 1)
    fe1 = pd.factorize(df[unit_fe_col].astype(str), sort=True)[0]
    fe2 = pd.factorize(df[time_fe_col].astype(str), sort=True)[0]
    y_resid, y_iter, y_change = residualize_two_way(y, fe1, fe2)
    x_resid, x_iter, x_change = residualize_two_way(x, fe1, fe2)
    beta, *_ = np.linalg.lstsq(x_resid, y_resid, rcond=None)
    fitted = np.sum(x_resid * beta.ravel(), axis=1).reshape(-1, 1)
    if not np.isfinite(beta).all() or not np.isfinite(fitted).all():
        raise FloatingPointError(f"{spec_name} produced non-finite estimates")
    residual = (y_resid - fitted).ravel()
    vcov = cluster_covariance_stable(x_resid, residual, df["cluster_id"].astype(str).to_numpy())
    se = np.sqrt(np.maximum(np.diag(vcov), 0.0))
    coef_lookup = dict(zip(x_df.columns, beta.ravel()))
    se_lookup = dict(zip(x_df.columns, se))

    rows = []
    for day in event_days:
        if day == reference_day:
            coef = 0.0
            std_err = np.nan
        else:
            col = event_columns[day]
            coef = float(coef_lookup[col])
            std_err = float(se_lookup[col])
        rows.append(
            {
                "spec": spec_name,
                "compressed_day": day,
                "reference_day": reference_day,
                "coef_speed_kmh": coef,
                "std_error_speed_kmh": std_err,
                "ci95_low_speed_kmh": coef - 1.96 * std_err if np.isfinite(std_err) else np.nan,
                "ci95_high_speed_kmh": coef + 1.96 * std_err if np.isfinite(std_err) else np.nan,
                "interaction_type": interaction_type,
            }
        )

    diagnostics = {
        "spec": spec_name,
        "interaction_type": interaction_type,
        "n_obs": int(len(df)),
        "n_segments": int(df["segment_id"].nunique()),
        "n_treated_segments": int(df.loc[df["treated_spec"].eq(1), "segment_id"].nunique()),
        "n_control_segments": int(df.loc[df["treated_spec"].eq(0), "segment_id"].nunique()),
        "n_time_fe": int(df[time_fe_col].nunique()),
        "n_unit_fe": int(df[unit_fe_col].nunique()),
        "n_clusters": int(df["cluster_id"].nunique()),
        "n_event_parameters": int(x.shape[1]),
        "unit_fe": unit_fe_col,
        "time_fe": time_fe_col,
        "reference_day": int(reference_day),
        "y_residualize_iterations": int(y_iter),
        "x_residualize_iterations": int(x_iter),
        "y_residualize_final_change": y_change,
        "x_residualize_final_change": x_change,
        "residualized_r_squared": float(
            1 - np.sum(residual**2) / np.sum((y_resid.ravel() - y_resid.mean()) ** 2)
        ),
    }
    if "prepost_coverage" in df.columns:
        diagnostics["post_only_segments"] = int(
            df.loc[df["prepost_coverage"].eq("post_only"), "segment_id"].nunique()
        )
    return pd.DataFrame(rows), diagnostics


def attach_date_index(coefs: pd.DataFrame, date_index: pd.DataFrame) -> pd.DataFrame:
    out = coefs.merge(
        date_index[
            [
                "compressed_day",
                "date",
                "date_label",
                "dow_name",
                "source_period",
                "plot_pos",
                "tick_label",
            ]
        ],
        on="compressed_day",
        how="left",
    )
    out["period_side"] = np.where(out["compressed_day"] < 0, "pre", "post")
    return out.sort_values(["spec", "plot_pos"])


def plot_compressed_event_study(
    coefs: pd.DataFrame,
    date_index: pd.DataFrame,
    output_path: Path,
) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(12.0, 4.4), sharex=True, sharey=True)
    axes_flat = np.atleast_1d(axes).ravel()
    colors = {"pre": "#3b6ea8", "post": "#c44e52"}
    x_ticks = date_index["plot_pos"].to_numpy()
    x_labels = date_index["tick_label"].tolist()

    for ax, spec in zip(axes_flat, SPEC_ORDER):
        sub = coefs[coefs["spec"].eq(spec)].sort_values("plot_pos").copy()
        for side in ["pre", "post"]:
            part = sub[sub["period_side"].eq(side)].copy()
            finite = part[np.isfinite(part["std_error_speed_kmh"])]
            if not finite.empty:
                yerr = np.vstack(
                    [
                        finite["coef_speed_kmh"] - finite["ci95_low_speed_kmh"],
                        finite["ci95_high_speed_kmh"] - finite["coef_speed_kmh"],
                    ]
                )
                ax.errorbar(
                    finite["plot_pos"],
                    finite["coef_speed_kmh"],
                    yerr=yerr,
                    fmt="o",
                    color=colors[side],
                    ecolor=colors[side],
                    elinewidth=0.95,
                    capsize=2.6,
                    markersize=4.0,
                    label="Pre" if side == "pre" else "Post",
                )
            omitted = part[~np.isfinite(part["std_error_speed_kmh"])]
            if not omitted.empty:
                ax.scatter(
                    omitted["plot_pos"],
                    omitted["coef_speed_kmh"],
                    marker="D",
                    s=34,
                    color=colors[side],
                    label="Omitted day" if side == "pre" else None,
                    zorder=4,
                )
            if len(part) > 1:
                ax.plot(
                    part["plot_pos"],
                    part["coef_speed_kmh"],
                    color=colors[side],
                    linewidth=1.0,
                    alpha=0.55,
                )
        ax.axhline(0, color="#555555", linewidth=0.8)
        ax.axvline(4.5, color="#222222", linewidth=0.9, linestyle="--")
        ax.set_title(SPEC_TITLES[spec], loc="left", fontweight="bold")
        ax.text(
            0.01,
            0.02,
            SPEC_NOTES[spec],
            transform=ax.transAxes,
            fontsize=7.1,
            color="#444444",
            va="bottom",
        )
        ax.grid(axis="y", color="#d9d9d9", linewidth=0.6, alpha=0.8)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    axes_flat[0].set_ylabel("Effect on treated active speed (km/h)")
    for ax in axes_flat:
        ax.set_xticks(x_ticks)
        ax.set_xticklabels(x_labels, fontsize=7.5)
    axes_flat[0].legend(frameon=False, loc="upper left", ncol=3)
    fig.suptitle(
        "Liuxian Avenue compressed-axis speed event studies",
        x=0.01,
        ha="left",
        fontsize=12.5,
    )
    fig.text(
        0.01,
        0.012,
        "Notes: The x-axis places observed pre and post weekdays next to each other; the dashed line separates pre from post. "
        "Panel A uses weekday 17:30-20:00 segment speeds with segment and exact timestamp fixed effects. "
        "Panel B uses weekday 15:00-20:00 speeds with segment-by-minute and date-by-active-window fixed effects.",
        fontsize=7.5,
        color="#444444",
    )
    fig.tight_layout(rect=(0, 0.09, 1, 0.92))
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    args = parse_args()
    configure_plot()
    dirs = ensure_output_dirs(args.output_dir)

    panel = load_full_gaode_panel(args.input_dir)
    panel, _ = assign_chain_direction(panel)
    panel = add_balanced_flags(panel)
    panel, date_index, reference_day = add_compressed_day_index(panel)

    all_coefs = []
    diagnostics = []
    sample_rows = []

    for spec in ["chain_common_opposite"]:
        sample = build_design_sample(panel, spec_name=spec)
        sample_rows.append(sample.assign(spec=spec))
        coef, diag = fit_event_model(
            sample,
            spec_name=spec,
            reference_day=reference_day,
            unit_fe_col="segment_id",
            time_fe_col="datetime_id",
            interaction_type="did",
        )
        all_coefs.append(coef)
        diagnostics.append(diag)

    ddd_sample = build_ddd_sample(panel)
    sample_rows.append(ddd_sample.assign(spec="chain_active_reference_ddd"))
    ddd_coef, ddd_diag = fit_event_model(
        ddd_sample,
        spec_name="chain_active_reference_ddd",
        reference_day=reference_day,
        unit_fe_col="segment_minute_id",
        time_fe_col="date_active_id",
        interaction_type="ddd",
    )
    all_coefs.append(ddd_coef)
    diagnostics.append(ddd_diag)

    coef_df = attach_date_index(pd.concat(all_coefs, ignore_index=True), date_index)
    coef_df.to_csv(
        dirs["metrics"] / "event_study_compressed_control_coefficients.csv",
        index=False,
        encoding="utf-8-sig",
    )
    pd.concat(sample_rows, ignore_index=True, sort=False).to_csv(
        dirs["data"] / "event_study_compressed_control_samples.csv",
        index=False,
        encoding="utf-8-sig",
    )
    date_index.to_csv(
        dirs["metrics"] / "event_study_compressed_date_index.csv",
        index=False,
        encoding="utf-8-sig",
    )

    metadata = {
        "policy_date": POLICY_DATE.strftime("%Y-%m-%d"),
        "reference_compressed_day": reference_day,
        "reference_date": date_index.loc[
            date_index["compressed_day"].eq(reference_day), "date"
        ].iloc[0].strftime("%Y-%m-%d"),
        "outcome": "speed_kmh",
        "active_window": "weekday 17:30-20:00",
        "reference_window_for_ddd": "weekday 15:00-17:30",
        "axis": "compressed observed weekdays; no calendar gap between pre and post weeks",
        "post_only_handling": (
            "The retained specifications use common segments observed in both weeks. "
            "Post-only endpoints are excluded because they lack within-segment pre-policy observations."
        ),
        "spec_notes": SPEC_NOTES,
        "diagnostics": diagnostics,
        "figure": str(dirs["figures"] / "event_study_compressed_control_methods.png"),
    }
    with (dirs["metrics"] / "event_study_compressed_control_metadata.json").open(
        "w", encoding="utf-8"
    ) as fh:
        json.dump(metadata, fh, ensure_ascii=False, indent=2)

    plot_compressed_event_study(
        coef_df,
        date_index,
        dirs["figures"] / "event_study_compressed_control_methods.png",
    )


if __name__ == "__main__":
    main()
