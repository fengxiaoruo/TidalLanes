#!/usr/bin/env python3
"""Method audit diagnostics for the outbound video RDiT analyses."""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import statsmodels.api as sm
from statsmodels.stats.diagnostic import acorr_ljungbox
from statsmodels.stats.stattools import durbin_watson

from analyze_outbound_rdit_specification_search_v1 import (
    EVENTS, OUTPUT as SEARCH_OUTPUT, build_panel, event_sample, load_region_events,
)

SKILL = Path("/Users/fxr/.codex/skills/top-journal-scientific-figures")
sys.path.insert(0, str(SKILL))
from scripts.journal_style import PALETTE, apply_axis_style, save_publication_figure, setup_publication_style  # noqa: E402

OUTPUT = SEARCH_OUTPUT.parent / "outbound_rdit_method_audit_v1"
KEY_OUTCOMES = ["north_total", "south_total", "bidirectional_total", "north_minus_south", "north_share"]


def fit_full(sample: pd.DataFrame, outcome: str, maxlags: int = 5) -> tuple[dict, np.ndarray]:
    x = sample.dropna(subset=[outcome]).copy()
    X = pd.DataFrame({
        "const": 1.0, "post": x.post.astype(float), "running": x.running_min,
        "post_running": x.post * x.running_min,
    }, index=x.index)
    raw = sm.OLS(x[outcome], X).fit()
    fit = raw.get_robustcov_results(cov_type="HAC", maxlags=maxlags)
    names = list(X.columns)
    def take(name: str) -> tuple[float, float, float]:
        j = names.index(name)
        return float(fit.params[j]), float(fit.bse[j]), float(fit.pvalues[j])
    tau, tau_se, tau_p = take("post")
    slope_pre, slope_pre_se, slope_pre_p = take("running")
    slope_change, slope_change_se, slope_change_p = take("post_running")
    residual = np.asarray(raw.resid)
    lb_lags = [lag for lag in (1, 5, 10) if lag < len(residual) / 2]
    lb = acorr_ljungbox(residual, lags=lb_lags, return_df=True) if lb_lags else pd.DataFrame()
    row = dict(n=len(x), pre_bins=int((x.post == 0).sum()), post_bins=int((x.post == 1).sum()),
               tau=tau, tau_se_hac=tau_se, tau_p_hac=tau_p,
               pre_slope=slope_pre, pre_slope_se_hac=slope_pre_se, pre_slope_p_hac=slope_pre_p,
               slope_change=slope_change, slope_change_se_hac=slope_change_se,
               slope_change_p_hac=slope_change_p, post_slope=slope_pre+slope_change,
               residual_acf1=float(pd.Series(residual).autocorr(1)),
               durbin_watson=float(durbin_watson(residual)))
    for lag in (1, 5, 10):
        row[f"ljung_box_p_lag{lag}"] = float(lb.loc[lag, "lb_pvalue"]) if lag in lb.index else np.nan
    return row, residual


def main() -> None:
    (OUTPUT / "figures").mkdir(parents=True, exist_ok=True)
    events, segments = load_region_events()
    panel = build_panel(events, segments, 1)
    baseline_rows, lag_rows, availability_rows = [], [], []
    for event in EVENTS:
        for bw in (30, 37, 45, 60):
            sample = event_sample(panel, event, bw, 0)
            for family, outcome in (("total", "north_total"), ("region", "north_fixed")):
                x = sample.dropna(subset=[outcome])
                pre = x[x.post.eq(0)]; post = x[x.post.eq(1)]
                availability_rows.append(dict(event=event["event"], bandwidth_min=bw,
                    outcome_family=family, outcome=outcome, n=len(x), pre_bins=len(pre), post_bins=len(post),
                    nearest_pre_min=float(pre.running_min.max()) if len(pre) else np.nan,
                    nearest_post_min=float(post.running_min.min()) if len(post) else np.nan,
                    farthest_pre_min=float(pre.running_min.min()) if len(pre) else np.nan,
                    farthest_post_min=float(post.running_min.max()) if len(post) else np.nan))
        sample = event_sample(panel, event, 60, 0)
        for outcome in KEY_OUTCOMES:
            row, _ = fit_full(sample, outcome, 5)
            baseline_rows.append(dict(event=event["event"], outcome=outcome, bandwidth_min=60,
                                      bin_minutes=1, polynomial=1, slope_form="separate",
                                      kernel="uniform", hac_maxlags=5, **row))
            x = sample.dropna(subset=[outcome]).copy()
            X = pd.DataFrame({"const": 1.0, "post": x.post.astype(float), "running": x.running_min,
                              "post_running": x.post*x.running_min}, index=x.index)
            raw = sm.OLS(x[outcome], X).fit()
            for lag in (0, 1, 3, 5, 10, 15):
                fit = raw.get_robustcov_results(cov_type="HAC", maxlags=lag)
                lag_rows.append(dict(event=event["event"], outcome=outcome, hac_maxlags=lag,
                    n=len(x), tau=float(fit.params[1]), se=float(fit.bse[1]), p_value=float(fit.pvalues[1])))
    pd.DataFrame(baseline_rows).to_csv(OUTPUT / "baseline_full_coefficients_and_serial_diagnostics.csv", index=False)
    pd.DataFrame(lag_rows).to_csv(OUTPUT / "hac_lag_sensitivity.csv", index=False)
    pd.DataFrame(availability_rows).to_csv(OUTPUT / "event_window_availability.csv", index=False)

    # Systematic pseudo-cutoffs inside uninterrupted stable footage.
    complete = panel[panel.complete_bin].sort_values("bin_start").copy()
    actual_times = [pd.Timestamp(e["cutoff"]) for e in EVENTS] + [pd.Timestamp(e["transition_start"]) for e in EVENTS]
    placebo_rows = []
    candidates = complete["bin_start"].drop_duplicates().sort_values()
    for cutoff in candidates:
        if cutoff.minute % 5 != 0 or min(abs((cutoff-a).total_seconds()) for a in actual_times) < 30*60:
            continue
        window = complete[complete.bin_start.between(cutoff-pd.Timedelta(minutes=15),
                                                     cutoff+pd.Timedelta(minutes=15), inclusive="both")].copy()
        expected = pd.date_range(cutoff-pd.Timedelta(minutes=15), cutoff+pd.Timedelta(minutes=15), freq="1min")
        if len(window) != len(expected) or set(window.bin_start) != set(expected):
            continue
        window["running_min"] = (window.bin_start-cutoff).dt.total_seconds()/60
        window["post"] = window.bin_start.ge(cutoff).astype(int)
        for outcome in KEY_OUTCOMES:
            row, _ = fit_full(window, outcome, 5)
            placebo_rows.append(dict(placebo_cutoff=cutoff, outcome=outcome, bandwidth_min=15, **row))
    placebo = pd.DataFrame(placebo_rows)
    placebo.to_csv(OUTPUT / "systematic_placebo_cutoffs_15min.csv", index=False)

    actual = pd.DataFrame(baseline_rows)
    setup_publication_style()
    fig, axes = plt.subplots(1, 3, figsize=(9.2, 3.2))
    for ax, outcome in zip(axes, ["north_total", "south_total", "north_minus_south"]):
        p = placebo[placebo.outcome.eq(outcome)]
        ax.hist(p.tau, bins=14, color="#B8C9DC", edgecolor="white")
        for event, color in (("20260724_opening", PALETTE[3]), ("20260724_recovery", PALETTE[0])):
            value = actual[(actual.event.eq(event)) & actual.outcome.eq(outcome)].tau.iloc[0]
            ax.axvline(value, color=color, lw=1.5, label=event.replace("2026", ""))
        ax.set_title(outcome.replace("_", " ").title(), fontsize=8)
        ax.set_xlabel("Estimated pseudo/actual discontinuity")
        apply_axis_style(ax)
    axes[0].set_ylabel("Pseudo-cutoff count")
    axes[-1].legend(frameon=False, fontsize=6.5)
    fig.tight_layout()
    save_publication_figure(fig, OUTPUT / "figures" / "placebo_distributions_vs_24jul_actual")
    plt.close(fig)
    print(f"output={OUTPUT}\nplacebo estimates={len(placebo)}")


if __name__ == "__main__":
    main()
