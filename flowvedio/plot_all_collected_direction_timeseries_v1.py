#!/usr/bin/env python3
"""Plot all collected five-minute total-flow samples by geographic direction."""

from pathlib import Path

import matplotlib as mpl
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd


ROOT = Path(__file__).resolve().parent
ANALYSIS = ROOT / "batch_outputs/20260723_24_state_dependent_recognition_v5/analysis_basic_v1"
INPUT = ANALYSIS / "camera_alignment_near_gate_v1/near_gate_camera_flow_panel_5min.csv"
OUTPUT = ANALYSIS / "all_collected_direction_timeseries_v1"

PANELS = [
    ("2026-07-23", "进城"),
    ("2026-07-23", "出城"),
    ("2026-07-24", "进城"),
    ("2026-07-24", "出城"),
]
PANEL_LABEL = {"进城": "Inbound camera", "出城": "Outbound camera"}
STATE_COLOR = {"向北开放": "#DDEBF5", "向南开放": "#F7E4D7"}
STATE_LABEL = {"向北开放": "Tidal lane open northbound", "向南开放": "Tidal lane open southbound"}
SERIES = {
    "northbound": ("south_to_north", "Northbound total flow", "#4C78A8"),
    "southbound": ("north_to_south", "Southbound total flow", "#E15759"),
}


def setup_style() -> None:
    mpl.rcParams.update({
        "font.family": "sans-serif",
        "font.sans-serif": ["Arial", "Helvetica", "Liberation Sans", "DejaVu Sans"],
        "font.size": 8.5,
        "axes.titlesize": 9,
        "axes.labelsize": 8.5,
        "xtick.labelsize": 7.5,
        "ytick.labelsize": 7.5,
        "axes.linewidth": 0.8,
        "xtick.major.width": 0.8,
        "ytick.major.width": 0.8,
        "xtick.major.size": 3,
        "ytick.major.size": 3,
        "svg.fonttype": "none",
        "pdf.fonttype": 42,
        "ps.fonttype": 42,
        "savefig.facecolor": "white",
    })


def continuous_runs(group: pd.DataFrame) -> list[pd.DataFrame]:
    group = group.sort_values("bin_start").copy()
    run_id = group["bin_start"].diff().gt(pd.Timedelta(minutes=6)).cumsum()
    return [part for _, part in group.groupby(run_id)]


def as_clock_time(values: pd.Series) -> pd.Series:
    """Put observations from different dates on one dummy-date clock axis."""
    return values.map(lambda x: pd.Timestamp(2000, 1, 1, x.hour, x.minute, x.second, x.microsecond))


def plot_direction(df: pd.DataFrame, key: str, direction: str, title: str, color: str) -> None:
    fig, axes = plt.subplots(2, 2, figsize=(7.1, 5.35), sharex=True, sharey=True,
                             constrained_layout=True)
    axes = axes.ravel()

    for idx, ((date, camera), ax) in enumerate(zip(PANELS, axes)):
        panel = df[
            (df["date"].astype(str) == date)
            & (df["collection_direction"] == camera)
            & (df["direction"] == direction)
        ].copy()

        for state, state_data in panel.groupby("tidal_state"):
            for run in continuous_runs(state_data):
                clock = as_clock_time(run["bin_start"])
                start = clock.min() - pd.Timedelta(minutes=2.5)
                end = clock.max() + pd.Timedelta(minutes=7.5)
                ax.axvspan(start, end, color=STATE_COLOR[state], alpha=0.72, lw=0, zorder=0)
                ax.plot(clock, run["flow_veh_per_min"], color=color,
                        lw=1.1, marker="o", markersize=2.6, markeredgewidth=0, zorder=3)

        clock_day = pd.Timestamp("2000-01-01")
        ax.set_xlim(clock_day + pd.Timedelta(hours=14.5), clock_day + pd.Timedelta(hours=22.5))
        ax.set_ylim(0, 75)
        ax.xaxis.set_major_locator(mdates.HourLocator(interval=1))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
        ax.yaxis.grid(True, color="#D9D9D9", lw=0.55, alpha=0.75)
        ax.xaxis.grid(False)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.set_title(f"{pd.Timestamp(date).strftime('%-d %b')} · {PANEL_LABEL[camera]}", loc="left", pad=5)
        ax.text(-0.13, 1.04, chr(ord("a") + idx), transform=ax.transAxes,
                fontsize=11, fontweight="bold", ha="left", va="top")

    fig.suptitle(f"All collected samples: {title}", fontsize=11, x=0.075, ha="left")
    fig.supylabel("Total flow (vehicles/min; complete 5-min bins)", fontsize=8.5)
    fig.supxlabel("Clock time", fontsize=8.5)
    handles = [
        mpl.patches.Patch(facecolor=STATE_COLOR[s], edgecolor="none", label=STATE_LABEL[s])
        for s in ["向北开放", "向南开放"]
    ]
    fig.legend(handles=handles, loc="upper right", bbox_to_anchor=(0.985, 1.015),
               frameon=False, ncol=2, fontsize=7.5, handlelength=1.4, columnspacing=1.2)
    fig.text(0.075, -0.018,
             "White intervals were not sampled. Lines never connect across recording gaps or tidal states.",
             ha="left", va="top", fontsize=7.2, color="#555555")

    base = OUTPUT / f"all_samples_{key}_total_flow_timeseries_v1"
    fig.savefig(base.with_suffix(".png"), dpi=600, bbox_inches="tight")
    fig.savefig(base.with_suffix(".svg"), bbox_inches="tight")
    fig.savefig(base.with_suffix(".pdf"), bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    setup_style()
    OUTPUT.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(INPUT, parse_dates=["bin_start"])
    df = df[df["complete_bin"]].copy()
    df.to_csv(OUTPUT / "all_samples_direction_total_flow_plot_data_v1.csv", index=False)
    for key, (direction, title, color) in SERIES.items():
        plot_direction(df, key, direction, title, color)


if __name__ == "__main__":
    main()
