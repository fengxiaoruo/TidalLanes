#!/usr/bin/env python3
"""Broad, versioned RDiT specification search for 23/24 July outbound video."""

from __future__ import annotations

import json
import math
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import statsmodels.api as sm

from analyze_20260723_24_state_dependent_v5 import CONFIG, SOURCE, build_event_and_segment_tables


SKILL = Path("/Users/fxr/.codex/skills/top-journal-scientific-figures")
sys.path.insert(0, str(SKILL))
from scripts.journal_style import (  # noqa: E402
    PALETTE, apply_axis_style, save_publication_figure, setup_publication_style,
)

OUTPUT = SOURCE / "analysis_basic_v1/outbound_rdit_specification_search_v1"
EVENTS = [
    dict(event="20260723_opening", label="23 Jul opening", cutoff="2026-07-23 16:26:34",
         transition_start="2026-07-23 16:11:09", favored="south_to_north"),
    dict(event="20260723_recovery", label="23 Jul recovery", cutoff="2026-07-23 21:46:35",
         transition_start="2026-07-23 21:31:35", favored="north_to_south"),
    dict(event="20260724_opening", label="24 Jul opening", cutoff="2026-07-24 16:29:01",
         transition_start="2026-07-24 16:07:43.048", favored="south_to_north"),
    dict(event="20260724_recovery", label="24 Jul recovery", cutoff="2026-07-24 21:42:41",
         transition_start="2026-07-24 21:34:16", favored="north_to_south"),
]
BIN_MINUTES = (1, 2, 5)
BANDWIDTHS = (10, 15, 20, 30, 37, 45, 60)
DONUTS = (0, 2, 5)

OUTCOMES = {
    "north_total": "Northbound total flow",
    "south_total": "Southbound total flow",
    "bidirectional_total": "Bidirectional total flow",
    "north_fixed": "Northbound fixed-lane flow",
    "south_fixed": "Southbound fixed-lane flow",
    "fixed_total": "Both fixed-lane regions",
    "tidal_region_total": "Tidal-variable-region flow",
    "north_minus_south": "Northbound minus southbound",
    "north_fixed_minus_south_fixed": "North fixed minus south fixed",
    "north_minus_south_fixed": "North total minus south fixed control",
    "north_fixed_minus_south": "North fixed control minus south total",
    "north_share": "Northbound share of total flow",
    "log_north_south_ratio": "Log northbound/southbound flow ratio",
}


def load_region_events() -> tuple[pd.DataFrame, pd.DataFrame]:
    plan = pd.read_csv(SOURCE / "run_plan.csv")
    config = json.loads(CONFIG.read_text(encoding="utf-8"))
    states = {str(x["阶段编号"]): x for x in config["states"]}
    marks = {str(x["标记编号"]): x for x in config["marks"]}
    parts = []
    for row in plan.itertuples(index=False):
        state = states[str(row.stage_id)]
        if str(state["采集方向"]) != "出城":
            continue
        mark = marks[str(state["坐标编号"])]
        data = pd.read_csv(Path(row.measurement_dir) / "gate_crossings.csv")
        data = data[data["gate"].eq("near")].copy()
        data["region"] = pd.NA
        if mark.get("kind") != "ordinary":
            x0, x1, x2, x3 = [float(mark[f"x1_{k}"]) for k in ("left", "boundary1", "boundary2", "right")]
            data["region"] = pd.cut(data["crossing_x"], [x0, x1, x2, x3], right=False,
                                    labels=["fixed_north", "tidal_variable", "fixed_south"])
        start = pd.Timestamp(f'{state["日期"]} {state["实际开始"]}')
        data["recorded_at"] = start + pd.to_timedelta(data["timestamp_s"] - float(row.start_s), unit="s")
        data["stage_id"] = row.stage_id
        parts.append(data[["recorded_at", "stage_id", "direction", "region"]])
    _, segments = build_event_and_segment_tables()
    segments = segments[segments["collection_direction"].eq("出城")].copy()
    tidal_stages = {str(row.stage_id) for row in plan.itertuples(index=False)
                    if states[str(row.stage_id)]["采集方向"] == "出城"
                    and marks[str(states[str(row.stage_id)]["坐标编号"])].get("kind") != "ordinary"}
    segments["region_capable"] = segments["stage_id"].astype(str).isin(tidal_stages)
    return pd.concat(parts, ignore_index=True), segments


def coverage(segments: pd.DataFrame, minutes: int) -> pd.DataFrame:
    rows = []
    for r in segments.drop_duplicates("stage_id").itertuples(index=False):
        start, end = pd.Timestamp(f"{r.date} {r.start_local}"), pd.Timestamp(f"{r.date} {r.end_local}")
        cursor = start.floor(f"{minutes}min")
        while cursor < end:
            stop = cursor + pd.Timedelta(minutes=minutes)
            seconds = max(0.0, (min(end, stop) - max(start, cursor)).total_seconds())
            if seconds:
                rows.append({"bin_start": cursor, "covered_seconds": seconds,
                             "region_covered_seconds": seconds if bool(r.region_capable) else 0.0})
            cursor = stop
    return pd.DataFrame(rows).groupby("bin_start", as_index=False)[
        ["covered_seconds", "region_covered_seconds"]].sum()


def build_panel(events: pd.DataFrame, segments: pd.DataFrame, minutes: int) -> pd.DataFrame:
    base = coverage(segments, minutes)
    event = events.copy()
    event["bin_start"] = event["recorded_at"].dt.floor(f"{minutes}min")
    total = event.groupby(["bin_start", "direction"]).size().unstack(fill_value=0)
    total = total.rename(columns={"south_to_north": "north_count", "north_to_south": "south_count"}).reset_index()
    counts = event[event["region"].notna()].groupby(
        ["bin_start", "region", "direction"], observed=True).size().rename("n").reset_index()
    counts["key"] = counts["region"].astype(str) + "__" + counts["direction"]
    wide = counts.pivot_table(index="bin_start", columns="key", values="n", fill_value=0).reset_index()
    panel = base.merge(total, on="bin_start", how="left").merge(wide, on="bin_start", how="left")
    count_cols = [x for x in panel.columns if "__" in str(x)] + ["north_count", "south_count"]
    panel[count_cols] = panel[count_cols].fillna(0)
    rate = 60.0 / panel["region_covered_seconds"].replace(0, np.nan)
    def col(region: str, direction: str) -> pd.Series:
        name = f"{region}__{direction}"
        return panel[name] * rate if name in panel else pd.Series(0.0, index=panel.index)
    panel["north_fixed"] = col("fixed_north", "south_to_north")
    panel["south_fixed"] = col("fixed_south", "north_to_south")
    panel["tidal_north"] = col("tidal_variable", "south_to_north")
    panel["tidal_south"] = col("tidal_variable", "north_to_south")
    panel["north_total"] = panel["north_count"] * 60.0 / panel["covered_seconds"]
    panel["south_total"] = panel["south_count"] * 60.0 / panel["covered_seconds"]
    panel["bidirectional_total"] = panel["north_total"] + panel["south_total"]
    panel["fixed_total"] = panel["north_fixed"] + panel["south_fixed"]
    panel["tidal_region_total"] = panel["tidal_north"] + panel["tidal_south"]
    panel["north_minus_south"] = panel["north_total"] - panel["south_total"]
    panel["north_fixed_minus_south_fixed"] = panel["north_fixed"] - panel["south_fixed"]
    panel["north_minus_south_fixed"] = panel["north_total"] - panel["south_fixed"]
    panel["north_fixed_minus_south"] = panel["north_fixed"] - panel["south_total"]
    panel["north_share"] = panel["north_total"] / panel["bidirectional_total"].replace(0, np.nan)
    panel["log_north_south_ratio"] = np.log((panel["north_total"] + 0.5) / (panel["south_total"] + 0.5))
    panel["complete_bin"] = panel["covered_seconds"].ge(minutes * 60 * 0.90)
    panel["bin_minutes"] = minutes
    return panel


def event_sample(panel: pd.DataFrame, event: dict, bandwidth: int, donut: int) -> pd.DataFrame:
    cutoff, transition = pd.Timestamp(event["cutoff"]), pd.Timestamp(event["transition_start"])
    x = panel[panel["complete_bin"]].copy()
    x["running_min"] = (x["bin_start"] - cutoff).dt.total_seconds() / 60
    x = x[x["running_min"].abs().le(bandwidth)]
    x = x[(x["bin_start"] < transition) | (x["bin_start"] >= cutoff + pd.Timedelta(minutes=donut))]
    x["post"] = (x["bin_start"] >= cutoff).astype(int)
    return x


def estimate(x: pd.DataFrame, outcome: str, polynomial: int, separate: bool,
             kernel: str, se_type: str, bin_minutes: int) -> dict | None:
    d = x.dropna(subset=[outcome]).copy()
    required = max(3, polynomial + 2)
    if (d["post"].eq(0).sum() < required) or (d["post"].eq(1).sum() < required):
        return None
    design = {"const": np.ones(len(d)), "post": d["post"].to_numpy(float)}
    for degree in range(1, polynomial + 1):
        z = d["running_min"].to_numpy(float) ** degree
        design[f"running_{degree}"] = z
        if separate:
            design[f"post_running_{degree}"] = z * d["post"].to_numpy(float)
    X = pd.DataFrame(design, index=d.index)
    if kernel == "triangular":
        edge = max(abs(d["running_min"]).max(), 1e-9)
        weights = np.maximum(1e-6, 1 - abs(d["running_min"]) / edge)
    else:
        weights = np.ones(len(d))
    fit = sm.WLS(d[outcome], X, weights=weights).fit()
    if se_type == "hac":
        fit = fit.get_robustcov_results(cov_type="HAC", maxlags=max(1, math.ceil(5 / bin_minutes)))
    else:
        fit = fit.get_robustcov_results(cov_type="HC1")
    names = list(X.columns); j = names.index("post")
    tau, se, p = float(fit.params[j]), float(fit.bse[j]), float(fit.pvalues[j])
    return dict(n=len(d), pre_bins=int(d["post"].eq(0).sum()), post_bins=int(d["post"].eq(1).sum()),
                tau=tau, se=se, ci95_low=tau-1.96*se, ci95_high=tau+1.96*se, p_value=p,
                pre_mean=float(d.loc[d["post"].eq(0), outcome].mean()),
                post_mean=float(d.loc[d["post"].eq(1), outcome].mean()))


def plot_event_outcomes(event: dict, panel: pd.DataFrame) -> None:
    sample = event_sample(panel, event, 60, 0)
    transition = (pd.Timestamp(event["transition_start"]) - pd.Timestamp(event["cutoff"])).total_seconds() / 60
    for outcome, label in OUTCOMES.items():
        setup_publication_style()
        fig, ax = plt.subplots(figsize=(4.5, 3.2))
        ax.axvspan(transition, 0, color="#D9D9D9", alpha=.65, lw=0)
        ax.axvline(0, color="#222222", ls="--", lw=.9)
        ax.scatter(sample["running_min"], sample[outcome], s=18, color=PALETTE[0], edgecolor="white", lw=.3)
        for post, part in sample.dropna(subset=[outcome]).groupby("post"):
            if len(part) >= 3:
                w = np.maximum(1e-6, 1 - abs(part["running_min"]) / max(abs(sample["running_min"])))
                coef = np.polyfit(part["running_min"], part[outcome], 1, w=w)
                xx = np.linspace(part["running_min"].min(), part["running_min"].max(), 100)
                ax.plot(xx, np.polyval(coef, xx), color=PALETTE[3], lw=1.5)
        ax.set_title(f'{event["label"]}: {label}', fontsize=9)
        ax.set_xlabel("Minutes from stable-state completion")
        ax.set_ylabel(label)
        apply_axis_style(ax)
        fig.tight_layout()
        save_publication_figure(fig, OUTPUT / "figures" / "event_studies" / f'{event["event"]}__{outcome}')
        plt.close(fig)


def plot_bandwidth(selected: pd.DataFrame) -> None:
    key = ["north_total", "south_total", "bidirectional_total", "north_fixed", "south_fixed",
           "tidal_region_total", "north_minus_south", "north_fixed_minus_south_fixed"]
    for event in EVENTS:
        setup_publication_style()
        fig, axes = plt.subplots(2, 4, figsize=(10.5, 5.2))
        for ax, outcome in zip(axes.flat, key):
            s = selected[(selected.event.eq(event["event"])) & selected.outcome.eq(outcome)].sort_values("bandwidth_min")
            ax.errorbar(s.bandwidth_min, s.tau, yerr=1.96*s.se, color=PALETTE[0], marker="o", ms=3, capsize=2)
            ax.axhline(0, color="#777777", lw=.7)
            ax.set_title(OUTCOMES[outcome], fontsize=8)
            ax.set_xlabel("Bandwidth (min)"); apply_axis_style(ax)
        fig.suptitle(f'{event["label"]}: local-linear uniform-kernel estimates', fontsize=10)
        fig.tight_layout()
        save_publication_figure(fig, OUTPUT / "figures" / f'{event["event"]}__bandwidth_robustness')
        plt.close(fig)


def main() -> None:
    (OUTPUT / "figures" / "event_studies").mkdir(parents=True, exist_ok=True)
    events, segments = load_region_events()
    panels = {m: build_panel(events, segments, m) for m in BIN_MINUTES}
    for m, panel in panels.items():
        panel.to_csv(OUTPUT / f"outbound_region_flow_panel_{m}min.csv", index=False)
    rows = []
    for event in EVENTS:
        for m, panel in panels.items():
            for bw in BANDWIDTHS:
                for donut in DONUTS:
                    sample = event_sample(panel, event, bw, donut)
                    for poly in (0, 1, 2):
                        for separate in ((False,) if poly == 0 else (False, True)):
                            for kernel in ("uniform", "triangular"):
                                for se_type in ("hac", "hc1"):
                                    for outcome in OUTCOMES:
                                        est = estimate(sample, outcome, poly, separate, kernel, se_type, m)
                                        if est is not None:
                                            rows.append(dict(event=event["event"], event_label=event["label"],
                                                favored_after=event["favored"], outcome=outcome,
                                                outcome_label=OUTCOMES[outcome], bin_minutes=m, bandwidth_min=bw,
                                                stable_donut_min=donut, polynomial=poly,
                                                slope_form="separate" if separate else "common",
                                                kernel=kernel, se_type=se_type, **est))
        plot_event_outcomes(event, panels[5])
    result = pd.DataFrame(rows)
    result.insert(0, "spec_id", [f"S{i:06d}" for i in range(1, len(result)+1)])
    result["significance"] = np.select([result.p_value < .01, result.p_value < .05, result.p_value < .10],
                                         ["p<0.01", "p<0.05", "p<0.10"], default="p>=0.10")
    result["direction"] = np.where(result.tau > 0, "positive", np.where(result.tau < 0, "negative", "zero"))
    result.to_csv(OUTPUT / "all_specification_estimates.csv", index=False)
    selected = result[(result.bin_minutes.eq(1)) & (result.stable_donut_min.eq(0)) &
                      (result.polynomial.eq(1)) & result.slope_form.eq("separate") &
                      result.kernel.eq("uniform") & result.se_type.eq("hac")]
    selected.to_csv(OUTPUT / "preferred_bandwidth_series.csv", index=False)
    preferred = selected[selected.bandwidth_min.eq(30)].copy()
    preferred.to_csv(OUTPUT / "preferred_30min_results.csv", index=False)
    summary = (result.groupby(["event", "outcome"], as_index=False)
               .agg(specifications=("spec_id", "size"), median_tau=("tau", "median"),
                    share_positive=("tau", lambda x: float((x > 0).mean())),
                    share_p05=("p_value", lambda x: float((x < .05).mean())),
                    min_tau=("tau", "min"), max_tau=("tau", "max")))
    summary.to_csv(OUTPUT / "specification_stability_summary.csv", index=False)
    credible = result[(result["polynomial"].le(1)) & (result["pre_bins"].ge(8)) &
                      (result["post_bins"].ge(8)) & result["se"].notna()]
    credible_summary = (credible.groupby(["event", "outcome"], as_index=False)
        .agg(specifications=("spec_id", "size"), median_tau=("tau", "median"),
             q10_tau=("tau", lambda x: float(x.quantile(.10))),
             q90_tau=("tau", lambda x: float(x.quantile(.90))),
             share_positive=("tau", lambda x: float((x > 0).mean())),
             share_p05=("p_value", lambda x: float((x < .05).mean()))))
    credible_summary.to_csv(OUTPUT / "credible_specification_stability_summary.csv", index=False)
    plot_bandwidth(selected)
    print(f"output={OUTPUT}\nestimates={len(result):,}\nfigures={len(list((OUTPUT/'figures').rglob('*.pdf')))}")


if __name__ == "__main__":
    main()
