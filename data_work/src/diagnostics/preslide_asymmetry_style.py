"""Shared styling for PreSlides pages 8--10 (AM speed asymmetry ratio figures)."""

from __future__ import annotations

import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np

SAR_DEFINITION_LINES = (
    r"Speed asymmetry ratio $=\min(x_{AB},x_{BA})/\max(x_{AB},x_{BA})$.",
    r"$x$ = peak travel time (OD) or peak speed (centerline).",
    r"Lower ratio indicates stronger directional asymmetry.",
)
SAR_DEFINITION = "\n".join(SAR_DEFINITION_LINES)
SAR_TITLE = "AM speed asymmetry ratio distribution"
SAR_XLABEL = "Speed asymmetry ratio"
SAR_COLOR_SINGLE = "#4C78A8"
SAR_COLOR_REFERENCE = "#4C72B0"
SAR_COLOR_EMPHASIS = "#C44E52"
SAR_COLOR_SCATTER = "#2B6CB0"
SAR_CMAP = "YlOrRd"
SAR_VMIN = 0.55
SAR_VMAX = 1.0
SAR_BINS = np.linspace(0.5, 1.0, 26)
SAR_XLIM = (0.48, 1.01)
FIG_HIST = (7.2, 4.8)
FIG_HIST_DUAL = (7.2, 5.0)
FIG_SCATTER = (7.2, 4.8)
FIG_MAP = (7.2, 6.4)

FONT = {
    "title": 12,
    "label": 11,
    "tick": 10,
    "stats": 10,
    "legend": 9.5,
    "defn": 8,
    "cbar_label": 10,
    "cbar_tick": 9,
}


def configure_preslide_asymmetry_mpl() -> None:
    mpl.rcParams.update(
        {
            "axes.titlesize": FONT["title"],
            "axes.labelsize": FONT["label"],
            "xtick.labelsize": FONT["tick"],
            "ytick.labelsize": FONT["tick"],
            "legend.fontsize": FONT["legend"],
        }
    )


def style_sar_axes(ax: plt.Axes, *, ylabel: str) -> None:
    ax.set_xlabel(SAR_XLABEL)
    ax.set_ylabel(ylabel)
    ax.set_xlim(*SAR_XLIM)
    ax.spines[["top", "right"]].set_visible(False)


def add_sar_stats_box(ax: plt.Axes, text: str) -> None:
    ax.text(
        0.03,
        0.97,
        text,
        transform=ax.transAxes,
        va="top",
        ha="left",
        fontsize=FONT["stats"],
        bbox=dict(boxstyle="round,pad=0.35", facecolor="white", alpha=0.92, edgecolor="none"),
    )


def add_sar_definition_once(fig: plt.Figure) -> None:
    fig.text(
        0.5,
        0.015,
        SAR_DEFINITION,
        transform=fig.transFigure,
        fontsize=FONT["defn"],
        va="bottom",
        ha="center",
        multialignment="center",
    )


def plot_sar_bar_histogram(
    ax: plt.Axes,
    values: np.ndarray,
    *,
    weights: np.ndarray | None = None,
    color: str = SAR_COLOR_SINGLE,
    label: str | None = None,
    alpha: float = 0.82,
) -> None:
    ax.hist(
        values,
        bins=SAR_BINS,
        weights=weights,
        color=color,
        alpha=alpha,
        edgecolor="white",
        linewidth=0.45,
        label=label,
    )


def plot_sar_step_histogram(
    ax: plt.Axes,
    values: np.ndarray,
    *,
    weights: np.ndarray | None = None,
    color: str = SAR_COLOR_SINGLE,
    label: str | None = None,
    linewidth: float = 2.0,
) -> None:
    ax.hist(
        values,
        bins=SAR_BINS,
        weights=weights,
        histtype="step",
        linewidth=linewidth,
        color=color,
        label=label,
    )


def plot_sar_median_line(ax: plt.Axes, median: float, *, color: str = "#222222") -> None:
    ax.axvline(median, color=color, linestyle="--", linewidth=1.1, alpha=0.9)


def finalize_sar_figure(fig: plt.Figure, *, bottom_margin: float | None = None) -> None:
    if bottom_margin is not None:
        fig.tight_layout(rect=(0, bottom_margin, 1, 1))
    else:
        fig.tight_layout()
