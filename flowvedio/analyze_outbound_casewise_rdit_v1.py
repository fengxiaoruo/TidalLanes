#!/usr/bin/env python3
"""Case-by-case local-linear RDiT for outbound-camera directional flow."""

from pathlib import Path
import sys

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import statsmodels.api as sm

from analyze_20260723_24_camera_alignment_v1 import flow_panel
from analyze_20260723_24_camera_near_gate_v1 import load_near_events
from analyze_20260723_24_state_dependent_v5 import SOURCE, build_event_and_segment_tables


SKILL = Path("/Users/fxr/.codex/skills/top-journal-scientific-figures")
sys.path.insert(0, str(SKILL))
from scripts.journal_style import (  # noqa: E402
    PALETTE, add_panel_label, apply_axis_style, save_publication_figure,
    setup_publication_style,
)

OUTPUT = SOURCE / "analysis_basic_v1/outbound_casewise_rdit_v1"
EVENTS = [
    {
        "event": "20260723_opening", "label": "23 Jul: northbound opening",
        "cutoff": "2026-07-23 16:26:34", "transition_start": "2026-07-23 16:11:09",
        "favored_after": "south_to_north", "geometry_start": "2026-07-23 15:58:34",
        "geometry_end": "2026-07-23 17:08:42", "design_grade": "weak_same_geometry",
        "design_note": "Same geometry, but only 12 complete pre-treatment minutes remain.",
    },
    {
        "event": "20260723_recovery", "label": "23 Jul: southbound recovery",
        "cutoff": "2026-07-23 21:46:35", "transition_start": "2026-07-23 21:31:35",
        "favored_after": "north_to_south", "geometry_start": "2026-07-23 20:11:35",
        "geometry_end": "2026-07-23 22:17:41.870", "design_grade": "usable_exploratory",
        "design_note": "Same camera and geometry; a 15-minute operation donut is excluded.",
    },
    {
        "event": "20260724_opening", "label": "24 Jul: northbound opening",
        "cutoff": "2026-07-24 16:29:01", "transition_start": "2026-07-24 16:07:43.048",
        "favored_after": "south_to_north", "geometry_start": "2026-07-24 14:42:02",
        "geometry_end": "2026-07-24 18:31:54.673", "design_grade": "invalid_for_causal_rdit",
        "design_note": "Pre and post use different videos and geometries, with a recording gap.",
    },
    {
        "event": "20260724_recovery", "label": "24 Jul: southbound recovery",
        "cutoff": "2026-07-24 21:42:41", "transition_start": "2026-07-24 21:34:16",
        "favored_after": "north_to_south", "geometry_start": "2026-07-24 20:02:56",
        "geometry_end": "2026-07-24 22:16:24.672", "design_grade": "usable_exploratory",
        "design_note": "Same camera and geometry; an 8-minute-25-second operation donut is excluded.",
    },
]
OUTCOMES = {
    "south_to_north": "Northbound total",
    "north_to_south": "Southbound total",
    "total_flow": "Bidirectional total",
    "favored_minus_unfavored": "Post-favored minus other direction",
}
COLORS = {"south_to_north": PALETTE[0], "north_to_south": PALETTE[3]}


def make_panel(panel: pd.DataFrame, event: dict, bandwidth: int) -> pd.DataFrame:
    cutoff = pd.Timestamp(event["cutoff"])
    transition_start = pd.Timestamp(event["transition_start"])
    base = panel[
        panel["collection_direction"].eq("出城")
        & panel["complete_bin"]
        & panel["bin_start"].between(cutoff - pd.Timedelta(minutes=bandwidth),
                                      cutoff + pd.Timedelta(minutes=bandwidth), inclusive="left")
        & panel["bin_start"].ge(pd.Timestamp(event["geometry_start"]))
        & panel["bin_start"].lt(pd.Timestamp(event["geometry_end"]))
    ].copy()
    base = base[(base["bin_start"] < transition_start) | (base["bin_start"] >= cutoff)]
    wide = base.pivot_table(index="bin_start", columns="direction",
                            values="flow_veh_per_min", aggfunc="first").reset_index()
    wide["running_min"] = (wide["bin_start"] - cutoff).dt.total_seconds() / 60
    wide["post"] = (wide["bin_start"] >= cutoff).astype(int)
    wide["total_flow"] = wide["south_to_north"] + wide["north_to_south"]
    favored = event["favored_after"]
    other = "north_to_south" if favored == "south_to_north" else "south_to_north"
    wide["favored_minus_unfavored"] = wide[favored] - wide[other]
    wide["event"] = event["event"]
    wide["bandwidth_min"] = bandwidth
    return wide


def estimate(sample: pd.DataFrame, outcome: str) -> dict:
    x = sample.dropna(subset=[outcome, "running_min", "post"]).copy()
    design = pd.DataFrame({
        "const": 1.0, "post": x["post"].astype(float), "running": x["running_min"],
        "post_running": x["post"] * x["running_min"],
    })
    fit = sm.OLS(x[outcome], design).fit(cov_type="HAC", cov_kwds={"maxlags": 5})
    tau, se = float(fit.params["post"]), float(fit.bse["post"])
    return {
        "n": len(x), "pre_bins": int((x["post"] == 0).sum()),
        "post_bins": int((x["post"] == 1).sum()), "tau": tau, "se_hac": se,
        "ci95_low": tau - 1.96 * se, "ci95_high": tau + 1.96 * se,
        "p_hac": float(fit.pvalues["post"]),
        "pre_mean": float(x.loc[x["post"] == 0, outcome].mean()),
        "post_mean": float(x.loc[x["post"] == 1, outcome].mean()),
    }


def add_fit(ax, data: pd.DataFrame, outcome: str, color: str) -> None:
    for _, part in data.groupby("post"):
        part = part.dropna(subset=[outcome, "running_min"])
        if len(part) < 2:
            continue
        coef = np.polyfit(part["running_min"], part[outcome], 1)
        xx = np.linspace(part["running_min"].min(), part["running_min"].max(), 100)
        ax.plot(xx, np.polyval(coef, xx), color=color, lw=1.45, zorder=2)


def plot_event(event: dict, data: pd.DataFrame) -> None:
    setup_publication_style()
    fig, axes = plt.subplots(1, 2, figsize=(7.1, 3.2))
    transition = (pd.Timestamp(event["transition_start"]) - pd.Timestamp(event["cutoff"])).total_seconds() / 60
    for ax in axes:
        ax.axvspan(transition, 0, color="#D9D9D9", alpha=0.70, lw=0)
        ax.axvline(0, color="#222222", ls="--", lw=0.9)
        ax.set_xlim(-62, 62)
        ax.set_xticks([-60, -30, 0, 30, 60])
        ax.set_xlabel("Minutes from stable-state completion")
        apply_axis_style(ax)
    for direction in ["south_to_north", "north_to_south"]:
        axes[0].scatter(data["running_min"], data[direction], s=17, color=COLORS[direction],
                        edgecolor="white", lw=0.3, alpha=0.82,
                        label=OUTCOMES[direction], zorder=3)
        add_fit(axes[0], data, direction, COLORS[direction])
    axes[0].set_ylim(0, 85)
    axes[0].set_ylabel("Total flow (vehicles/min; 5-min bins)")
    axes[0].set_title("Directional flows")
    axes[0].legend(frameon=False, fontsize=7.2, loc="upper right")
    net_color = PALETTE[2]
    axes[1].scatter(data["running_min"], data["favored_minus_unfavored"], s=17,
                    color=net_color, edgecolor="white", lw=0.3, alpha=0.82, zorder=3)
    add_fit(axes[1], data, "favored_minus_unfavored", net_color)
    axes[1].axhline(0, color="#777777", lw=0.7)
    axes[1].set_ylim(-65, 65)
    axes[1].set_ylabel("Post-favored minus other (vehicles/min)")
    axes[1].set_title("Directional reallocation")
    axes[0].text(transition / 2, 82, "Operation donut", ha="center", va="top",
                 fontsize=7, color="#555555", rotation=90 if abs(transition) < 12 else 0)
    add_panel_label(axes[0], "a", x=-0.12, y=1.03)
    add_panel_label(axes[1], "b", x=-0.12, y=1.03)
    fig.suptitle(event["label"], fontsize=10, y=1.01)
    fig.subplots_adjust(left=0.09, right=0.99, bottom=0.17, top=0.80, wspace=0.27)
    save_publication_figure(fig, OUTPUT / f'{event["event"]}_event_study_v1')
    plt.close(fig)


def main() -> None:
    OUTPUT.mkdir(parents=True, exist_ok=True)
    near = load_near_events()
    _, segments = build_event_and_segment_tables()
    results, panels = [], []
    panels_by_bin = {b: flow_panel(near, segments, b) for b in [1, 5]}
    for event in EVENTS:
        for bandwidth in [30, 45, 60]:
            sample = make_panel(panels_by_bin[1], event, bandwidth)
            sample["bin_minutes"] = 1
            panels.append(sample)
            for outcome in OUTCOMES:
                results.append({
                    "event": event["event"], "event_label": event["label"],
                    "design_grade": event["design_grade"], "design_note": event["design_note"],
                    "favored_after": event["favored_after"], "outcome": outcome,
                    "outcome_label": OUTCOMES[outcome], "bandwidth_min": bandwidth,
                    "bin_minutes": 1, **estimate(sample, outcome),
                })
        plot_event(event, make_panel(panels_by_bin[5], event, 60))
    result = pd.DataFrame(results)
    result.to_csv(OUTPUT / "casewise_rdit_estimates_all_bandwidths_v1.csv", index=False)
    pd.concat(panels, ignore_index=True).to_csv(OUTPUT / "casewise_rdit_analysis_panel_v1.csv", index=False)
    result[result["bandwidth_min"] == 60].to_csv(OUTPUT / "casewise_rdit_main_60min_v1.csv", index=False)
    result[(result["outcome"] == "favored_minus_unfavored")].to_csv(
        OUTPUT / "casewise_rdit_netflow_bandwidth_robustness_v1.csv", index=False)
    print(result[result["bandwidth_min"] == 60][
        ["event", "design_grade", "outcome", "n", "pre_bins", "post_bins", "tau", "se_hac", "p_hac"]
    ].to_string(index=False))


if __name__ == "__main__":
    main()
