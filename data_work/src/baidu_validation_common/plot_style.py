"""Unified matplotlib style for raw vs centerline Baidu validation figures."""

from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

QC_LO, QC_HI = 0.6, 1.8
PANEL_ORDER = ["FREE", "AM", "PM"]
PANEL_SCENARIO = {"FREE": "FREE_WORK", "AM": "AMP_WORK", "PM": "PMP_WORK"}
PANEL_COLORS = {"FREE": "#3182bd", "AM": "#e6550d", "PM": "#31a354"}
PANEL_FIGSIZE_ROW = (12, 3.8)
PANEL_FIGSIZE_HIST = (12, 3.6)
SINGLE_FIGSIZE = (7, 4)
FIG_DPI = 220
SCATTER_SIZE = 6
SCATTER_ALPHA = 0.24
REF_LINE_COLOR = "#636363"
QC_BAND_COLOR = "#bdbdbd"


def apply_validation_rc():
    plt.rcParams.update(
        {
            "font.size": 10,
            "axes.titlesize": 10,
            "axes.labelsize": 9,
            "legend.fontsize": 8,
            "figure.dpi": FIG_DPI,
        }
    )


def cap_axis(values: pd.Series, q: float = 0.995) -> float:
    v = pd.to_numeric(values, errors="coerce").dropna()
    v = v[v > 0]
    return float(max(v.quantile(q), 1.0)) if len(v) else 30.0


def save_fig(fig, path):
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def plot_dist_ratio_hist(
    df: pd.DataFrame,
    path,
    *,
    length_label: str,
    title: str = "Route bind ratio by panel",
):
    apply_validation_rc()
    fig, axes = plt.subplots(1, 3, figsize=PANEL_FIGSIZE_HIST, dpi=FIG_DPI, sharey=True)
    bins = np.linspace(0, 3, 61)
    for ax, panel in zip(axes, PANEL_ORDER):
        sub = df[(df["panel"] == panel) & df["api_status"].eq(0)]
        if sub.empty:
            ax.set_title(f"{panel} (no data)")
            continue
        ax.hist(
            sub["distance_over_length"].dropna(),
            bins=bins,
            color=PANEL_COLORS[panel],
            alpha=0.75,
            edgecolor="white",
        )
        ax.axvspan(QC_LO, QC_HI, color=QC_BAND_COLOR, alpha=0.35)
        ax.set_title(f"{panel} (n={len(sub):,})")
        ax.set_xlabel(length_label)
        ax.grid(True, axis="y", alpha=0.25)
    axes[0].set_ylabel("Count")
    fig.suptitle(title, y=1.02)
    save_fig(fig, path)


def plot_travel_time_hist(
    df: pd.DataFrame,
    path,
    *,
    time_col: str,
    title: str,
    xlabel: str = "Travel time (min)",
):
    apply_validation_rc()
    fig, axes = plt.subplots(1, 3, figsize=PANEL_FIGSIZE_HIST, dpi=FIG_DPI, sharey=True)
    cap = cap_axis(df.loc[df["bind_ok"], time_col])
    bins = np.linspace(0, cap, 50)
    for ax, panel in zip(axes, PANEL_ORDER):
        sub = df[(df["panel"] == panel) & df["bind_ok"]]
        if sub.empty:
            ax.set_title(f"{panel} (no data)")
            continue
        ax.hist(
            sub[time_col].dropna(),
            bins=bins,
            color=PANEL_COLORS[panel],
            alpha=0.75,
            edgecolor="white",
        )
        ax.set_title(f"{panel} bind_ok (n={len(sub):,})")
        ax.set_xlabel(xlabel)
        ax.grid(True, axis="y", alpha=0.25)
    axes[0].set_ylabel("Count")
    fig.suptitle(title, y=1.02)
    save_fig(fig, path)


def plot_reference_vs_api_scatter(
    df: pd.DataFrame,
    path,
    *,
    ref_col: str,
    api_col: str,
    ref_label: str,
    api_label: str,
    title: str,
    panel_titles: dict[str, str] | None = None,
):
    apply_validation_rc()
    fig, axes = plt.subplots(1, 3, figsize=PANEL_FIGSIZE_ROW, dpi=FIG_DPI)
    for ax, panel in zip(axes, PANEL_ORDER):
        sub = df[(df["panel"] == panel) & df["bind_ok"] & df[ref_col].notna()]
        if sub.empty:
            ax.set_title(f"{panel} (no data)")
            continue
        x = sub[ref_col]
        y = sub[api_col]
        cap = cap_axis(pd.concat([x, y]))
        ax.scatter(x, y, s=SCATTER_SIZE, alpha=SCATTER_ALPHA, c=PANEL_COLORS[panel], edgecolors="none")
        ax.plot([0, cap], [0, cap], "--", color=REF_LINE_COLOR, linewidth=1)
        corr = x.corr(y) if len(sub) >= 30 else np.nan
        panel_title = (panel_titles or {}).get(panel, panel)
        ax.set_title(f"{panel_title} (n={len(sub):,}, r={corr:.2f})")
        ax.set_xlim(0, cap)
        ax.set_ylim(0, cap)
        ax.set_xlabel(ref_label)
        ax.set_ylabel(api_label)
        ax.grid(True, alpha=0.25)
    fig.suptitle(title, y=1.02)
    save_fig(fig, path)


def plot_bland_altman(
    df: pd.DataFrame,
    path,
    *,
    ref_col: str,
    api_col: str,
    diff_col: str,
    title: str,
    ylabel: str,
    panel_titles: dict[str, str] | None = None,
):
    apply_validation_rc()
    fig, axes = plt.subplots(1, 3, figsize=PANEL_FIGSIZE_ROW, dpi=FIG_DPI)
    for ax, panel in zip(axes, PANEL_ORDER):
        sub = df[(df["panel"] == panel) & df["bind_ok"] & df[ref_col].notna()]
        if sub.empty:
            ax.set_title(f"{panel} (no data)")
            continue
        mean_v = (sub[api_col] + sub[ref_col]) / 2
        diff_v = sub[diff_col]
        ax.scatter(mean_v, diff_v, s=SCATTER_SIZE, alpha=SCATTER_ALPHA, c=PANEL_COLORS[panel], edgecolors="none")
        ax.axhline(0, color=REF_LINE_COLOR, linewidth=1)
        panel_title = (panel_titles or {}).get(panel, panel)
        ax.set_title(f"{panel_title} (n={len(sub):,})")
        ax.set_xlabel("Mean travel time (min)")
        ax.set_ylabel(ylabel)
        ax.grid(True, alpha=0.25)
    fig.suptitle(title, y=1.02)
    save_fig(fig, path)


def plot_time_ratio_hist(
    df: pd.DataFrame,
    path,
    *,
    ratio_col: str,
    title: str,
    xlabel: str,
    panel_titles: dict[str, str] | None = None,
):
    apply_validation_rc()
    fig, ax = plt.subplots(figsize=SINGLE_FIGSIZE, dpi=FIG_DPI)
    bins = np.linspace(0, 4, 81)
    for panel in PANEL_ORDER:
        sub = df[(df["panel"] == panel) & df["bind_ok"] & df[ratio_col].notna()]
        if sub.empty:
            continue
        panel_title = (panel_titles or {}).get(panel, panel)
        ax.hist(
            sub[ratio_col].clip(0, 4),
            bins=bins,
            histtype="step",
            linewidth=1.5,
            label=f"{panel_title} (n={len(sub):,})",
            color=PANEL_COLORS[panel],
            density=True,
        )
    ax.axvspan(0.8, 1.25, color=QC_BAND_COLOR, alpha=0.35, label="[0.8, 1.25]")
    ax.set_xlabel(xlabel)
    ax.set_ylabel("Density")
    ax.legend()
    ax.grid(True, alpha=0.25)
    ax.set_title(title)
    save_fig(fig, path)


def plot_speed_scatter(
    df: pd.DataFrame,
    path,
    *,
    ref_speed_col: str,
    api_speed_col: str,
    ref_label: str,
    api_label: str,
    title: str,
    panel_titles: dict[str, str] | None = None,
):
    apply_validation_rc()
    fig, axes = plt.subplots(1, 3, figsize=PANEL_FIGSIZE_ROW, dpi=FIG_DPI)
    for ax, panel in zip(axes, PANEL_ORDER):
        sub = df[
            (df["panel"] == panel)
            & df["bind_ok"]
            & df[ref_speed_col].notna()
            & df[api_speed_col].gt(0)
            & df[api_speed_col].lt(200)
        ]
        if sub.empty:
            ax.set_title(f"{panel} (no data)")
            continue
        x = sub[ref_speed_col]
        y = sub[api_speed_col]
        cap = cap_axis(pd.concat([x, y]), q=0.99)
        ax.scatter(x, y, s=SCATTER_SIZE, alpha=SCATTER_ALPHA, c=PANEL_COLORS[panel], edgecolors="none")
        ax.plot([0, cap], [0, cap], "--", color=REF_LINE_COLOR, linewidth=1)
        corr = x.corr(y) if len(sub) >= 30 else np.nan
        panel_title = (panel_titles or {}).get(panel, panel)
        ax.set_title(f"{panel_title} (n={len(sub):,}, r={corr:.2f})")
        ax.set_xlim(0, cap)
        ax.set_ylim(0, cap)
        ax.set_xlabel(ref_label)
        ax.set_ylabel(api_label)
        ax.grid(True, alpha=0.25)
    fig.suptitle(title, y=1.02)
    save_fig(fig, path)


def plot_spatial_hists(spatial: pd.DataFrame, path_overlap, path_dist, *, geom_label: str):
    apply_validation_rc()
    fig, axes = plt.subplots(1, 3, figsize=PANEL_FIGSIZE_HIST, dpi=FIG_DPI, sharey=True)
    for ax, panel in zip(axes, PANEL_ORDER):
        sub = spatial[spatial["panel"] == panel]
        if sub.empty:
            ax.set_title(f"{panel} (no data)")
            continue
        ax.hist(
            sub["overlap_geom_in_route_buffer"].dropna(),
            bins=30,
            color=PANEL_COLORS[panel],
            alpha=0.75,
            edgecolor="white",
        )
        ax.axvline(0.5, color=REF_LINE_COLOR, linestyle="--", linewidth=1)
        ax.set_title(f"{panel} (n={len(sub):,})")
        ax.set_xlabel(f"{geom_label} overlap in route buffer @50m")
        ax.grid(True, axis="y", alpha=0.25)
    axes[0].set_ylabel("Count")
    fig.suptitle(f"Route–{geom_label} spatial overlap", y=1.02)
    save_fig(fig, path_overlap)

    fig, axes = plt.subplots(1, 3, figsize=PANEL_FIGSIZE_HIST, dpi=FIG_DPI, sharey=True)
    for ax, panel in zip(axes, PANEL_ORDER):
        sub = spatial[spatial["panel"] == panel]
        if sub.empty:
            ax.set_title(f"{panel} (no data)")
            continue
        ax.hist(
            sub["mean_dist_geom_to_route_m"].dropna(),
            bins=30,
            color=PANEL_COLORS[panel],
            alpha=0.75,
            edgecolor="white",
        )
        ax.axvline(80, color=REF_LINE_COLOR, linestyle="--", linewidth=1)
        ax.set_title(f"{panel} (n={len(sub):,})")
        ax.set_xlabel(f"Mean distance {geom_label}→route (m)")
        ax.grid(True, axis="y", alpha=0.25)
    axes[0].set_ylabel("Count")
    fig.suptitle(f"Route–{geom_label} mean distance", y=1.02)
    save_fig(fig, path_dist)


def plot_spatial_scatter(spatial: pd.DataFrame, path, *, geom_label: str, length_label: str):
    apply_validation_rc()
    fig, ax = plt.subplots(figsize=(6.5, 4.5), dpi=FIG_DPI)
    for panel in PANEL_ORDER:
        sub = spatial[spatial["panel"] == panel]
        if sub.empty:
            continue
        ax.scatter(
            sub["distance_over_length"],
            sub["overlap_geom_in_route_buffer"],
            s=8,
            alpha=0.3,
            c=PANEL_COLORS[panel],
            label=panel,
            edgecolors="none",
        )
    ax.axvspan(QC_LO, QC_HI, color=QC_BAND_COLOR, alpha=0.25)
    ax.set_xlabel(length_label)
    ax.set_ylabel(f"{geom_label} overlap in route buffer @50m")
    ax.set_title(f"Spatial fit vs route bind ratio ({geom_label})")
    ax.legend()
    ax.grid(True, alpha=0.25)
    save_fig(fig, path)
