#!/usr/bin/env python3
"""Overlay recorded operation-period flow on the retained outbound RDiT plots.

Operation observations are descriptive only. Stable observations continue to
define the local-linear fits and all regression estimates.
"""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from analyze_outbound_rdit_specification_search_v1 import EVENTS, OUTCOMES, event_sample


ROOT = Path(__file__).resolve().parent
BASE = ROOT / "batch_outputs/20260723_24_state_dependent_recognition_v5"
SPEC = BASE / "analysis_basic_v1/outbound_rdit_specification_search_v1"
OP = BASE / "operation_period_recognition_v1"
OUT = BASE / "analysis_basic_v1/outbound_rdit_operation_overlay_v1"
FIG = OUT / "figures/event_studies"
FIG.mkdir(parents=True, exist_ok=True)

SKILL = Path("/Users/fxr/.codex/skills/top-journal-scientific-figures")
sys.path.insert(0, str(SKILL))
from scripts.journal_style import PALETTE, apply_axis_style, save_publication_figure, setup_publication_style  # noqa: E402

PLOT_OUTCOMES = ["north_total", "south_total", "bidirectional_total", "north_minus_south"]
OPERATION_COLOR = "#7B3294"


def coverage_rows(start: pd.Timestamp, end: pd.Timestamp, minutes: int) -> list[dict[str, object]]:
    rows = []
    cursor = start.floor(f"{minutes}min")
    while cursor < end:
        stop = cursor + pd.Timedelta(minutes=minutes)
        seconds = max(0.0, (min(stop, end) - max(cursor, start)).total_seconds())
        if seconds > 0:
            rows.append({"bin_start": cursor, "covered_seconds": seconds})
        cursor = stop
    return rows


def operation_panel(minutes: int) -> pd.DataFrame:
    plan = pd.read_csv(OP / "run_plan.csv")
    count_parts, cover_parts = [], []
    for row in plan.itertuples(index=False):
        start = pd.Timestamp(f"{row.date} {row.start_local}")
        end = pd.Timestamp(f"{row.date} {row.end_local}")
        cover = pd.DataFrame(coverage_rows(start, end, minutes))
        cover["stage_id"] = row.stage_id
        cover["date"] = row.date
        cover["event"] = row.event
        cover_parts.append(cover)

        crossings = pd.read_csv(Path(row.measurement_dir) / "gate_crossings.csv")
        crossings = crossings[crossings.gate.eq("near")].copy()
        crossings["recorded_at"] = start + pd.to_timedelta(crossings.timestamp_s - float(row.start_s), unit="s")
        crossings["bin_start"] = crossings.recorded_at.dt.floor(f"{minutes}min")
        counts = crossings.groupby(["bin_start", "direction"]).size().unstack(fill_value=0).reset_index()
        counts["stage_id"] = row.stage_id
        count_parts.append(counts)

    coverage = pd.concat(cover_parts, ignore_index=True)
    counts = pd.concat(count_parts, ignore_index=True)
    panel = coverage.merge(counts, on=["stage_id", "bin_start"], how="left")
    for direction in ["south_to_north", "north_to_south"]:
        if direction not in panel:
            panel[direction] = 0
        panel[direction] = panel[direction].fillna(0)
    scale = 60 / panel.covered_seconds
    panel["north_total"] = panel.south_to_north * scale
    panel["south_total"] = panel.north_to_south * scale
    panel["bidirectional_total"] = panel.north_total + panel.south_total
    panel["north_minus_south"] = panel.north_total - panel.south_total
    panel["bin_minutes"] = minutes
    panel["coverage_share"] = panel.covered_seconds / (minutes * 60)
    panel["point_eligible"] = panel.covered_seconds.ge(60)
    return panel.sort_values(["date", "event", "bin_start"])


def plot_one(event: dict[str, str], outcome: str, stable_panel: pd.DataFrame, operation: pd.DataFrame) -> None:
    stable = event_sample(stable_panel, event, 60, 0)
    event_date = pd.Timestamp(event["cutoff"]).strftime("%Y-%m-%d")
    op = operation[(operation.date.astype(str).eq(event_date)) & operation.event.eq(event["event"].split("_")[1])]
    op = op[op.point_eligible].copy()
    cutoff = pd.Timestamp(event["cutoff"])
    op["running_min"] = (op.bin_start - cutoff).dt.total_seconds() / 60
    transition = (pd.Timestamp(event["transition_start"]) - cutoff).total_seconds() / 60

    setup_publication_style()
    fig, ax = plt.subplots(figsize=(4.5, 3.2))
    ax.axvspan(transition, 0, color="#D9D9D9", alpha=.65, lw=0)
    ax.axvline(0, color="#222222", ls="--", lw=.9)
    ax.scatter(
        stable.running_min, stable[outcome], s=18, color=PALETTE[0], edgecolor="white", lw=.3,
        label="Stable state", zorder=3,
    )
    ax.scatter(
        op.running_min, op[outcome], s=28, marker="D", color=OPERATION_COLOR, edgecolor="white", lw=.4,
        label="Operation period", zorder=4,
    )
    for _, part in stable.dropna(subset=[outcome]).groupby("post"):
        if len(part) >= 3:
            weights = np.maximum(1e-6, 1 - abs(part.running_min) / max(abs(stable.running_min)))
            coef = np.polyfit(part.running_min, part[outcome], 1, w=weights)
            xx = np.linspace(part.running_min.min(), part.running_min.max(), 100)
            ax.plot(xx, np.polyval(coef, xx), color=PALETTE[3], lw=1.5)
    ax.set_title(f'{event["label"]}: {OUTCOMES[outcome]}', fontsize=9)
    ax.set_xlabel("Minutes from stable-state completion")
    ax.set_ylabel(OUTCOMES[outcome])
    ax.legend(frameon=False, fontsize=6.5, loc="best", handletextpad=.4)
    apply_axis_style(ax)
    fig.tight_layout()
    save_publication_figure(fig, FIG / f'{event["event"]}__{outcome}__with_operation')
    plt.close(fig)


def main() -> None:
    stable = pd.read_csv(SPEC / "outbound_region_flow_panel_5min.csv", parse_dates=["bin_start"])
    one = operation_panel(1)
    five = operation_panel(5)
    one.to_csv(OUT / "operation_flow_points_1min.csv", index=False)
    five.to_csv(OUT / "operation_flow_points_5min.csv", index=False)
    summary = one[one.point_eligible].groupby(["date", "event", "stage_id"], as_index=False).agg(
        eligible_recorded_minutes=("covered_seconds", lambda x: x.sum() / 60),
        eligible_one_minute_bins_count=("bin_start", "size"),
        northbound_mean=("north_total", "mean"),
        southbound_mean=("south_total", "mean"),
        north_minus_south_mean=("north_minus_south", "mean"),
    )
    summary.to_csv(OUT / "operation_point_summary.csv", index=False)
    for event in EVENTS:
        for outcome in PLOT_OUTCOMES:
            plot_one(event, outcome, stable, five)


if __name__ == "__main__":
    main()
