#!/usr/bin/env python3
"""Plot outbound-camera near-gate event-study panels for three valid events."""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from analyze_20260723_24_camera_alignment_v1 import flow_panel
from analyze_20260723_24_camera_near_gate_v1 import load_near_events
from analyze_20260723_24_outbound_rdit_near_v1 import EVENTS, OUTPUT, event_panel
from analyze_20260723_24_state_dependent_v5 import build_event_and_segment_tables


SKILL = Path("/Users/fxr/.codex/skills/top-journal-scientific-figures")
sys.path.insert(0, str(SKILL))
from scripts.journal_style import PALETTE, add_panel_label, apply_axis_style, save_publication_figure, setup_publication_style


LABELS = {
    "20260723_opening": "23 Jul: northbound opening",
    "20260723_recovery": "23 Jul: southbound recovery",
    "20260724_recovery": "24 Jul: southbound recovery",
}
COLORS = {"south_to_north": PALETTE[0], "north_to_south": PALETTE[3]}
NAMES = {"south_to_north": "Northbound", "north_to_south": "Southbound"}


def add_fit(ax, data: pd.DataFrame, direction: str) -> None:
    color = COLORS[direction]
    for post, part in data.groupby("post"):
        clean = part.dropna(subset=[direction, "running_min"])
        if len(clean) < 2:
            continue
        coef = np.polyfit(clean["running_min"], clean[direction], 1)
        xs = np.linspace(clean["running_min"].min(), clean["running_min"].max(), 100)
        ax.plot(xs, np.polyval(coef, xs), color=color, linewidth=1.5, alpha=0.95)


def main() -> None:
    setup_publication_style()
    near = load_near_events()
    _, segments = build_event_and_segment_tables()
    panel = flow_panel(near, segments, 5)
    fig, axes = plt.subplots(1, 3, figsize=(10.2, 3.45), sharey=True)
    for idx, (ax, event) in enumerate(zip(axes, EVENTS)):
        data = event_panel(panel, event, 60)
        transition_start = (pd.Timestamp(event["transition_start"]) - pd.Timestamp(event["cutoff"])).total_seconds() / 60
        ax.axvspan(transition_start, 0, color="#D9D9D9", alpha=0.65, linewidth=0)
        ax.axvline(0, color="black", linestyle="--", linewidth=0.9)
        for direction in ("south_to_north", "north_to_south"):
            ax.scatter(
                data["running_min"], data[direction], s=18, color=COLORS[direction],
                alpha=0.78, edgecolor="white", linewidth=0.35, label=NAMES[direction], zorder=3,
            )
            add_fit(ax, data, direction)
        ax.text(
            (transition_start / 2), 83, "Lane operation", ha="center", va="top",
            fontsize=7.3, color="#555555", rotation=90 if abs(transition_start) < 10 else 0,
        )
        ax.set_title(LABELS[event["event"]], pad=7)
        ax.set_xlim(-62, 62)
        ax.set_ylim(0, 85)
        ax.set_xticks([-60, -30, 0, 30, 60])
        ax.set_xlabel("Minutes from stable state completion")
        apply_axis_style(ax)
        add_panel_label(ax, chr(ord("a") + idx), x=-0.12, y=1.02)
    axes[0].set_ylabel("Vehicle flow (vehicles/min, 5-min bins)")
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, 1.02), ncol=2, handletextpad=0.5, columnspacing=1.4)
    fig.subplots_adjust(left=0.07, right=0.995, bottom=0.18, top=0.80, wspace=0.18)
    save_publication_figure(fig, OUTPUT / "outbound_near_gate_rdit_eventstudy_v1")
    plt.close(fig)


if __name__ == "__main__":
    main()
