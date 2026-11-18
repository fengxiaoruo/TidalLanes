#!/usr/bin/env python3
"""Plot July 24 outbound-camera total directional flows around tidal-lane opening."""

from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd


ROOT = Path(__file__).resolve().parent
ANALYSIS = ROOT / "batch_outputs/20260723_24_state_dependent_recognition_v5/analysis_basic_v1"
INPUT = ANALYSIS / "camera_alignment_near_gate_v1/near_gate_camera_flow_panel_5min.csv"
OUTPUT = ANALYSIS / "outbound_tidal_region_opening_20260724_v1"

START = pd.Timestamp("2026-07-24 14:35:00")
END = pd.Timestamp("2026-07-24 18:35:00")
PRE_START = pd.Timestamp("2026-07-24 15:37:43.048")
PRE_END = pd.Timestamp("2026-07-24 16:07:43.048")
GAP_END = pd.Timestamp("2026-07-24 16:26:46")
POST_START = pd.Timestamp("2026-07-24 16:29:01")
POST_END = pd.Timestamp("2026-07-24 16:59:01")

SERIES = {
    "northbound": {
        "direction": "south_to_north",
        "title": "Northbound total flow",
        "color": "#0072B2",
        "pre_mean": 40.4667,
        "post_mean": 53.9667,
    },
    "southbound": {
        "direction": "north_to_south",
        "title": "Southbound total flow",
        "color": "#D55E00",
        "pre_mean": 43.3667,
        "post_mean": 28.6000,
    },
}


def configure_style() -> None:
    plt.rcParams.update({
        "font.family": "DejaVu Sans",
        "font.size": 9,
        "axes.titlesize": 11,
        "axes.labelsize": 9,
        "xtick.labelsize": 8,
        "ytick.labelsize": 8,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.linewidth": 0.8,
        "savefig.bbox": "tight",
        "savefig.facecolor": "white",
    })


def plot_one(data: pd.DataFrame, key: str, spec: dict) -> None:
    fig, ax = plt.subplots(figsize=(7.2, 4.15), constrained_layout=True)

    ax.axvspan(PRE_START, PRE_END, color="#56B4E9", alpha=0.11, lw=0)
    ax.axvspan(PRE_END, GAP_END, color="#7F7F7F", alpha=0.18, lw=0)
    ax.axvspan(GAP_END, POST_START, color="#E69F00", alpha=0.20, lw=0)
    ax.axvspan(POST_START, POST_END, color="#009E73", alpha=0.10, lw=0)

    for _, segment in data.groupby("tidal_state", sort=False):
        segment = segment.sort_values("bin_start")
        ax.plot(
            segment["bin_start"], segment["flow_veh_per_min"],
            color=spec["color"], marker="o", markersize=3.0,
            markeredgewidth=0, linewidth=1.25, zorder=3,
        )

    ax.hlines(spec["pre_mean"], PRE_START, PRE_END, color="#222222", lw=2.0, zorder=4)
    ax.hlines(spec["post_mean"], POST_START, POST_END, color="#222222", lw=2.0, zorder=4)
    ax.text(PRE_START + pd.Timedelta(minutes=15), spec["pre_mean"] + 2.0,
            f"Mean {spec['pre_mean']:.1f}", ha="center", va="bottom", fontsize=8)
    ax.text(POST_START + pd.Timedelta(minutes=15), spec["post_mean"] + 2.0,
            f"Mean {spec['post_mean']:.1f}", ha="center", va="bottom", fontsize=8)

    ax.axvline(POST_START, color="#333333", lw=1.0, ls="--", zorder=2)
    trans = ax.get_xaxis_transform()
    ax.text(PRE_START + pd.Timedelta(minutes=15), 0.975, "Last 30 min\npre-opening",
            transform=trans, ha="center", va="top", fontsize=7.5, color="#24536B")
    ax.text(PRE_END + (GAP_END - PRE_END) / 2, 0.975, "No recording",
            transform=trans, ha="center", va="top", fontsize=7.5, color="#4D4D4D")
    operation_mid = mdates.date2num((GAP_END + (POST_START-GAP_END)/2).to_pydatetime())
    annotation_x = mdates.date2num(pd.Timestamp("2026-07-24 17:18:00").to_pydatetime())
    ax.annotate("Operation tail\n(2 min 15 s)", xy=(operation_mid, 0.86),
                xycoords=trans, xytext=(annotation_x, 0.83),
                textcoords=trans, fontsize=7.5, ha="left", va="center",
                arrowprops={"arrowstyle": "->", "lw": 0.7, "color": "#8C6200"},
                color="#8C6200")
    ax.text(POST_START + pd.Timedelta(minutes=15), 0.975, "First 30 min\nstable post-opening",
            transform=trans, ha="center", va="top", fontsize=7.5, color="#006B4E")

    ax.set_title(f"24 July outbound camera: {spec['title']}", loc="left", pad=9)
    ax.set_ylabel("Total flow (vehicles/min; 5-min bins)")
    ax.set_xlabel("Clock time")
    ax.set_xlim(START, END)
    ax.set_ylim(0, 75)
    ax.xaxis.set_major_locator(mdates.MinuteLocator(byminute=[0, 30]))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    ax.yaxis.grid(True, color="#D9D9D9", lw=0.6, alpha=0.7)
    ax.xaxis.grid(False)
    ax.text(0.0, -0.22,
            "Shaded windows use exact video timestamps; partial 5-min bins are omitted from the line.",
            transform=ax.transAxes, ha="left", va="top", fontsize=7, color="#555555")

    base = OUTPUT / f"{key}_total_flow_timeseries_v1"
    fig.savefig(base.with_suffix(".png"), dpi=600)
    fig.savefig(base.with_suffix(".svg"))
    fig.savefig(base.with_suffix(".pdf"))
    plt.close(fig)


def main() -> None:
    configure_style()
    OUTPUT.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(INPUT, parse_dates=["bin_start"])
    df = df[
        (df["date"].astype(str) == "2026-07-24")
        & (df["collection_direction"] == "出城")
        & (df["complete_bin"])
        & (df["bin_start"].between(START, END))
    ].copy()
    export = []
    for key, spec in SERIES.items():
        one = df[df["direction"] == spec["direction"]].copy()
        plot_one(one, key, spec)
        one.insert(0, "display_direction", key)
        export.append(one)
    pd.concat(export, ignore_index=True).to_csv(
        OUTPUT / "direction_total_flow_timeseries_plot_data_v1.csv", index=False
    )


if __name__ == "__main__":
    main()
