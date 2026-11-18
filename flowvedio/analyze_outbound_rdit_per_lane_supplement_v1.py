#!/usr/bin/env python3
"""Per-lane flow supplement to the outbound RDiT specification search."""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from analyze_outbound_rdit_specification_search_v1 import (
    BANDWIDTHS, BIN_MINUTES, DONUTS, EVENTS, OUTPUT as PARENT_OUTPUT,
    build_panel, estimate, event_sample, load_region_events,
)

SKILL = Path("/Users/fxr/.codex/skills/top-journal-scientific-figures")
sys.path.insert(0, str(SKILL))
from scripts.journal_style import PALETTE, apply_axis_style, save_publication_figure, setup_publication_style  # noqa: E402

OUTPUT = PARENT_OUTPUT.parent / "outbound_rdit_per_lane_supplement_v1"
OUTCOMES = {
    "north_per_lane": "Northbound flow per lane",
    "south_per_lane": "Southbound flow per lane",
    "bidirectional_per_lane": "Bidirectional flow per road lane",
    "north_minus_south_per_lane": "North minus south flow per lane",
    "log_north_south_per_lane_ratio": "Log north/south per-lane flow ratio",
    "favored_per_lane": "Post-favored direction flow per lane",
    "other_per_lane": "Other direction flow per lane",
    "favored_minus_other_per_lane": "Post-favored minus other flow per lane",
    "log_favored_other_per_lane_ratio": "Log favored/other per-lane flow ratio",
    "north_fixed_per_lane": "North fixed-region flow per fixed lane",
    "south_fixed_per_lane": "South fixed-region flow per fixed lane",
    "fixed_difference_per_lane": "North minus south fixed-region flow per lane",
}


def add_per_lane(sample: pd.DataFrame, event: dict) -> pd.DataFrame:
    x = sample.copy()
    north_after = event["favored"] == "south_to_north"
    x["north_lanes"] = np.where(x["post"].eq(1), 4 if north_after else 3, 3 if north_after else 4)
    x["south_lanes"] = 7 - x["north_lanes"]
    x["north_per_lane"] = x["north_total"] / x["north_lanes"]
    x["south_per_lane"] = x["south_total"] / x["south_lanes"]
    x["bidirectional_per_lane"] = x["bidirectional_total"] / 7.0
    x["north_minus_south_per_lane"] = x["north_per_lane"] - x["south_per_lane"]
    x["log_north_south_per_lane_ratio"] = np.log(
        (x["north_per_lane"] + 0.5) / (x["south_per_lane"] + 0.5))
    if north_after:
        x["favored_per_lane"], x["other_per_lane"] = x["north_per_lane"], x["south_per_lane"]
    else:
        x["favored_per_lane"], x["other_per_lane"] = x["south_per_lane"], x["north_per_lane"]
    x["favored_minus_other_per_lane"] = x["favored_per_lane"] - x["other_per_lane"]
    x["log_favored_other_per_lane_ratio"] = np.log(
        (x["favored_per_lane"] + 0.5) / (x["other_per_lane"] + 0.5))
    x["north_fixed_per_lane"] = x["north_fixed"] / 3.0
    x["south_fixed_per_lane"] = x["south_fixed"] / 3.0
    x["fixed_difference_per_lane"] = x["north_fixed_per_lane"] - x["south_fixed_per_lane"]
    return x


def plot_event(event: dict, panel: pd.DataFrame) -> None:
    sample = add_per_lane(event_sample(panel, event, 60, 0), event)
    transition = (pd.Timestamp(event["transition_start"]) - pd.Timestamp(event["cutoff"])).total_seconds() / 60
    keys = ["north_per_lane", "south_per_lane", "bidirectional_per_lane",
            "favored_minus_other_per_lane", "log_favored_other_per_lane_ratio",
            "fixed_difference_per_lane"]
    setup_publication_style()
    fig, axes = plt.subplots(2, 3, figsize=(9.2, 5.5))
    for ax, outcome in zip(axes.flat, keys):
        ax.axvspan(transition, 0, color="#D9D9D9", alpha=.65, lw=0)
        ax.axvline(0, color="#222222", ls="--", lw=.9)
        ax.scatter(sample.running_min, sample[outcome], s=17, color=PALETTE[0], edgecolor="white", lw=.3)
        for _, part in sample.dropna(subset=[outcome]).groupby("post"):
            if len(part) >= 3:
                coef = np.polyfit(part.running_min, part[outcome], 1)
                xx = np.linspace(part.running_min.min(), part.running_min.max(), 100)
                ax.plot(xx, np.polyval(coef, xx), color=PALETTE[3], lw=1.4)
        ax.axhline(0, color="#888888", lw=.6)
        ax.set_title(OUTCOMES[outcome], fontsize=8)
        ax.set_xlabel("Minutes from stable-state completion")
        apply_axis_style(ax)
    fig.suptitle(f'{event["label"]}: per-lane flow outcomes', fontsize=10)
    fig.tight_layout()
    save_publication_figure(fig, OUTPUT / "figures" / f'{event["event"]}__per_lane_event_study')
    plt.close(fig)


def main() -> None:
    (OUTPUT / "figures").mkdir(parents=True, exist_ok=True)
    events, segments = load_region_events()
    panels = {m: build_panel(events, segments, m) for m in BIN_MINUTES}
    rows = []
    analysis_panels = []
    for event in EVENTS:
        for m, panel in panels.items():
            for bw in BANDWIDTHS:
                for donut in DONUTS:
                    sample = add_per_lane(event_sample(panel, event, bw, donut), event)
                    if bw == 60 and donut == 0:
                        keep = sample.copy(); keep["event"] = event["event"]
                        analysis_panels.append(keep)
                    for poly in (0, 1, 2):
                        for separate in ((False,) if poly == 0 else (False, True)):
                            for kernel in ("uniform", "triangular"):
                                for se_type in ("hac", "hc1"):
                                    for outcome, label in OUTCOMES.items():
                                        est = estimate(sample, outcome, poly, separate, kernel, se_type, m)
                                        if est is not None:
                                            rows.append(dict(event=event["event"], event_label=event["label"],
                                                favored_after=event["favored"], outcome=outcome, outcome_label=label,
                                                bin_minutes=m, bandwidth_min=bw, stable_donut_min=donut,
                                                polynomial=poly, slope_form="separate" if separate else "common",
                                                kernel=kernel, se_type=se_type, **est))
        plot_event(event, panels[5])
    result = pd.DataFrame(rows)
    result.insert(0, "spec_id", [f"PL{i:06d}" for i in range(1, len(result)+1)])
    result["significance"] = np.select([result.p_value < .01, result.p_value < .05, result.p_value < .10],
                                        ["p<0.01", "p<0.05", "p<0.10"], default="p>=0.10")
    result["direction"] = np.where(result.tau > 0, "positive", np.where(result.tau < 0, "negative", "zero"))
    result.to_csv(OUTPUT / "all_per_lane_specification_estimates.csv", index=False)
    pd.concat(analysis_panels, ignore_index=True).to_csv(OUTPUT / "per_lane_analysis_panels_60min.csv", index=False)
    baseline = result[(result.bin_minutes.eq(1)) & (result.bandwidth_min.eq(60)) &
        (result.stable_donut_min.eq(0)) & (result.polynomial.eq(1)) &
        result.slope_form.eq("separate") & result.kernel.eq("uniform") & result.se_type.eq("hac")]
    baseline.to_csv(OUTPUT / "per_lane_baseline_60min.csv", index=False)
    credible = result[(result.polynomial.le(1)) & (result.pre_bins.ge(8)) & (result.post_bins.ge(8))]
    stability = credible.groupby(["event", "outcome"], as_index=False).agg(
        specifications=("spec_id", "size"), median_tau=("tau", "median"),
        q10_tau=("tau", lambda x: float(x.quantile(.10))), q90_tau=("tau", lambda x: float(x.quantile(.90))),
        share_positive=("tau", lambda x: float((x > 0).mean())),
        share_p05=("p_value", lambda x: float((x < .05).mean())))
    stability.to_csv(OUTPUT / "per_lane_credible_stability_summary.csv", index=False)
    print(f"output={OUTPUT}\nestimates={len(result):,}\nfigures=4 sets")


if __name__ == "__main__":
    main()
