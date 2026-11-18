#!/usr/bin/env python3
"""Plot northbound and southbound total flow together for outbound-camera samples."""

from pathlib import Path

import matplotlib as mpl
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd


ROOT = Path(__file__).resolve().parent
ANALYSIS = ROOT / "batch_outputs/20260723_24_state_dependent_recognition_v5/analysis_basic_v1"
INPUT = ANALYSIS / "camera_alignment_near_gate_v1/near_gate_camera_flow_panel_5min.csv"
OUTPUT = ANALYSIS / "all_collected_direction_timeseries_v1"

COLORS = {"south_to_north": "#4C78A8", "north_to_south": "#E15759"}
LABELS = {"south_to_north": "Northbound total", "north_to_south": "Southbound total"}
STATE_COLOR = {"向北开放": "#DDEBF5", "向南开放": "#F7E4D7"}


def clock(values: pd.Series) -> pd.Series:
    return values.map(lambda x: pd.Timestamp(2000, 1, 1, x.hour, x.minute, x.second, x.microsecond))


def runs(group: pd.DataFrame) -> list[pd.DataFrame]:
    group = group.sort_values("bin_start")
    run_id = group["bin_start"].diff().gt(pd.Timedelta(minutes=6)).cumsum()
    return [part for _, part in group.groupby(run_id)]


def main() -> None:
    mpl.rcParams.update({
        "font.family": "sans-serif",
        "font.sans-serif": ["Arial", "Helvetica", "Liberation Sans", "DejaVu Sans"],
        "font.size": 8.5, "axes.titlesize": 9.5, "axes.labelsize": 8.5,
        "xtick.labelsize": 7.5, "ytick.labelsize": 7.5,
        "axes.linewidth": 0.8, "svg.fonttype": "none", "pdf.fonttype": 42,
        "savefig.facecolor": "white",
    })
    OUTPUT.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(INPUT, parse_dates=["bin_start"])
    df = df[(df["collection_direction"] == "出城") & df["complete_bin"]].copy()

    fig, axes = plt.subplots(2, 1, figsize=(7.1, 5.25), sharex=True, sharey=True,
                             constrained_layout=True)
    for idx, (date, ax) in enumerate(zip(["2026-07-23", "2026-07-24"], axes)):
        panel = df[df["date"].astype(str) == date]

        # Shade only intervals actually covered by complete samples.
        coverage = panel[["tidal_state", "bin_start"]].drop_duplicates()
        for state, state_data in coverage.groupby("tidal_state"):
            for run in runs(state_data):
                x = clock(run["bin_start"])
                ax.axvspan(x.min() - pd.Timedelta(minutes=2.5),
                           x.max() + pd.Timedelta(minutes=7.5),
                           facecolor=STATE_COLOR[state], alpha=0.72, lw=0, zorder=0)

        for direction in ["south_to_north", "north_to_south"]:
            direction_data = panel[panel["direction"] == direction]
            for state, state_data in direction_data.groupby("tidal_state"):
                for run in runs(state_data):
                    ax.plot(clock(run["bin_start"]), run["flow_veh_per_min"],
                            color=COLORS[direction], lw=1.2, marker="o", markersize=2.8,
                            markeredgewidth=0, zorder=3,
                            label=LABELS[direction] if idx == 0 and state == "向南开放" and run.index.equals(runs(state_data)[0].index) else None)

        base = pd.Timestamp("2000-01-01")
        ax.set_xlim(base + pd.Timedelta(hours=14.5), base + pd.Timedelta(hours=22.5))
        ax.set_ylim(0, 75)
        ax.xaxis.set_major_locator(mdates.HourLocator(interval=1))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
        ax.yaxis.grid(True, color="#D9D9D9", lw=0.55, alpha=0.75)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.set_title(f"{pd.Timestamp(date).strftime('%-d %B 2026')}", loc="left", pad=5)
        ax.text(-0.075, 1.04, chr(ord("a") + idx), transform=ax.transAxes,
                fontsize=11, fontweight="bold", ha="left", va="top")

    line_handles = [mpl.lines.Line2D([], [], color=COLORS[d], marker="o", ms=3,
                                    lw=1.2, label=LABELS[d])
                    for d in ["south_to_north", "north_to_south"]]
    state_handles = [
        mpl.patches.Patch(facecolor=STATE_COLOR["向北开放"], edgecolor="none",
                          label="Tidal lane open northbound"),
        mpl.patches.Patch(facecolor=STATE_COLOR["向南开放"], edgecolor="none",
                          label="Tidal lane open southbound"),
    ]
    fig.legend(handles=line_handles + state_handles, loc="upper right",
               bbox_to_anchor=(0.99, 1.005), frameon=False, ncol=2,
               fontsize=7.5, handlelength=1.8, columnspacing=1.25)
    fig.suptitle("Outbound camera: total directional flow across all samples",
                 fontsize=11, x=0.085, y=1.055, ha="left")
    fig.supylabel("Total flow (vehicles/min; complete 5-min bins)", fontsize=8.5)
    fig.supxlabel("Clock time", fontsize=8.5)
    fig.text(0.085, -0.018,
             "White intervals were not sampled. Lines do not connect across recording gaps or tidal states.",
             ha="left", va="top", fontsize=7.2, color="#555555")

    base = OUTPUT / "outbound_camera_combined_direction_total_flow_timeseries_v1"
    fig.savefig(base.with_suffix(".png"), dpi=600, bbox_inches="tight")
    fig.savefig(base.with_suffix(".svg"), bbox_inches="tight")
    fig.savefig(base.with_suffix(".pdf"), bbox_inches="tight")
    df.to_csv(OUTPUT / "outbound_camera_combined_direction_plot_data_v1.csv", index=False)
    plt.close(fig)


if __name__ == "__main__":
    main()
