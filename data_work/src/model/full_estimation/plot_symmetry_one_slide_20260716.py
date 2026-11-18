"""Build the one-slide arithmetic-symmetry result figure."""
from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, Rectangle
from matplotlib.ticker import FuncFormatter
import numpy as np
import pandas as pd


DATA_WORK = Path(__file__).resolve().parents[3]
RESULT = DATA_WORK / "outputs" / "symmetric_input_mean_method_comparison_20260716_v1"
METRICS = RESULT / "metrics"
FIGURES = RESULT / "figures"
SLIDE_FIGURES = DATA_WORK.parent / "Documents" / "PreSlides" / "figs"

BLUE = "#2F6B9A"
BLUE_DARK = "#174A73"
ORANGE = "#D97732"
ORANGE_DARK = "#A64B1A"
GRAY = "#7A7A7A"
LIGHT_GRAY = "#D9D9D9"
THETA = 6.83


def setup_style() -> None:
    plt.rcParams.update(
        {
            "font.family": "sans-serif",
            "font.sans-serif": ["Arial", "Helvetica", "Liberation Sans", "DejaVu Sans"],
            "font.size": 9.0,
            "axes.titlesize": 10.5,
            "axes.labelsize": 9.0,
            "xtick.labelsize": 8.0,
            "ytick.labelsize": 8.0,
            "legend.fontsize": 8.0,
            "axes.linewidth": 0.8,
            "pdf.fonttype": 42,
            "ps.fonttype": 42,
            "svg.fonttype": "none",
        }
    )


def weighted_mean(group: pd.DataFrame, value: str, weight: str) -> float:
    return float(np.average(group[value], weights=group[weight]))


def build_profile(nodes: pd.DataFrame) -> pd.DataFrame:
    bins = [0.0, 10.0, 18.0, 28.0, 38.0, np.inf]
    labels = ["0-10", "10-18", "18-28", "28-38", "38+"]
    nodes = nodes.copy()
    nodes["distance_band"] = pd.cut(
        nodes["distance_to_employment_center_km"], bins=bins, labels=labels, include_lowest=True
    )
    rows = []
    for label, group in nodes.groupby("distance_band", observed=True):
        rows.append(
            {
                "distance_band": str(label),
                "distance_km": float(
                    np.average(group["distance_to_employment_center_km"], weights=group["residents"] + group["jobs"])
                ),
                "residence_pct": 100.0 * weighted_mean(
                    group, "delta_log_residence_fundamental", "residents"
                ),
                "workplace_pct": 100.0 * weighted_mean(
                    group, "delta_log_workplace_fundamental", "jobs"
                ),
            }
        )
    return pd.DataFrame(rows)


def build_slide_metrics() -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    baseline = pd.read_csv(METRICS / "mean_method_baseline_summary.csv").query(
        "method == 'arithmetic'"
    ).iloc[0]
    pairs = pd.read_csv(METRICS / "reciprocal_pair_exposure_covariance.csv")
    fundamentals = pd.read_csv(METRICS / "fundamental_summary.csv").query(
        "method == 'arithmetic'"
    )
    nodes = pd.read_csv(METRICS / "node_fundamental_deltas_arithmetic.csv")
    districts = pd.read_csv(METRICS / "fundamental_district_summary.csv").query(
        "method == 'arithmetic' and district == '朝阳区'"
    )

    pair_mean = 0.5 * (pairs["slow_time_minutes"] + pairs["fast_time_minutes"])
    slow_old = float(np.sum(pairs["slow_passages"] * pairs["slow_time_minutes"]))
    slow_new = float(np.sum(pairs["slow_passages"] * pair_mean))
    fast_old = float(np.sum(pairs["fast_passages"] * pairs["fast_time_minutes"]))
    fast_new = float(np.sum(pairs["fast_passages"] * pair_mean))

    directed_minutes = float(baseline["directed_mean_physical_minutes"])
    fixed_minutes = directed_minutes + float(baseline["fixed_passage_time_bias_seconds"]) / 60.0
    time_metrics = pd.DataFrame(
        [
            {"metric": "Slow direction", "pct_change": 100.0 * (slow_new / slow_old - 1.0)},
            {"metric": "Fast direction", "pct_change": 100.0 * (fast_new / fast_old - 1.0)},
            {"metric": "Unweighted link mean", "pct_change": 0.0},
            {"metric": "Fixed baseline passages", "pct_change": 100.0 * (fixed_minutes / directed_minutes - 1.0)},
            {
                "metric": "Re-estimated equilibrium",
                "pct_change": 100.0 * (
                    float(baseline["symmetric_mean_physical_minutes"]) / directed_minutes - 1.0
                ),
            },
        ]
    )
    profile = build_profile(nodes)

    residence = fundamentals.query("metric == 'delta_log_residence_fundamental'").iloc[0]
    workplace = fundamentals.query("metric == 'delta_log_workplace_fundamental'").iloc[0]
    chaoyang_res = districts.query("metric == 'delta_log_residence_fundamental'").iloc[0]
    chaoyang_work = districts.query("metric == 'delta_log_workplace_fundamental'").iloc[0]
    callouts = {
        "slow_passage_share": float(pairs["slow_passages"].sum() / pairs["pair_passages"].sum()),
        "resident_tail_share": float(residence["weighted_share_abs_ge_005"]),
        "job_tail_share": float(workplace["weighted_share_abs_ge_005"]),
        "chaoyang_residence_pct": 100.0 * float(chaoyang_res["weighted_mean"]),
        "chaoyang_workplace_pct": 100.0 * float(chaoyang_work["weighted_mean"]),
    }
    return time_metrics, profile, callouts


def draw_figure(time_metrics: pd.DataFrame, profile: pd.DataFrame, callouts: dict) -> plt.Figure:
    setup_style()
    fig = plt.figure(figsize=(12.2, 4.65), facecolor="white")
    outer = fig.add_gridspec(1, 2, width_ratios=[0.92, 1.15], wspace=0.28)
    left = outer[0].subgridspec(2, 1, height_ratios=[0.82, 1.28], hspace=0.38)
    ax_pair = fig.add_subplot(left[0])
    ax_net = fig.add_subplot(left[1])
    ax_profile = fig.add_subplot(outer[1])

    pair = time_metrics.iloc[:2]
    y = np.arange(2)
    colors = [BLUE, ORANGE]
    ax_pair.barh(y, pair["pct_change"], color=colors, height=0.55)
    ax_pair.axvline(0, color="#222222", lw=0.8)
    slow_share = 100.0 * callouts["slow_passage_share"]
    ax_pair.set_yticks(
        y,
        [
            f"Slow direction\n({slow_share:.1f}% of passages)",
            f"Fast direction\n({100.0 - slow_share:.1f}% of passages)",
        ],
    )
    ax_pair.invert_yaxis()
    ax_pair.set_xlim(-22.5, 22.5)
    ax_pair.set_xticks([-20, -10, 0, 10, 20])
    ax_pair.xaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{value:.0f}%"))
    ax_pair.set_title("A. Directional exposure, not the link-time mean", loc="left", fontweight="bold")
    for yi, (_, row) in enumerate(pair.iterrows()):
        value = float(row["pct_change"])
        ha = "left" if value < 0 else "right"
        dx = 1.0 if value < 0 else -1.0
        ax_pair.text(value + dx, yi, f"{value:+.1f}%", va="center", ha=ha, color="white", fontweight="bold")

    network = time_metrics.iloc[2:]
    y2 = np.arange(3)
    ax_net.barh(y2, network["pct_change"], color=[LIGHT_GRAY, BLUE, BLUE_DARK], height=0.58)
    ax_net.axvline(0, color="#222222", lw=0.8)
    ax_net.set_yticks(y2, network["metric"])
    ax_net.invert_yaxis()
    ax_net.set_xlim(-6.2, 0.7)
    ax_net.set_xticks([-6, -4, -2, 0])
    ax_net.xaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{value:.0f}%"))
    ax_net.set_xlabel("Change under arithmetic symmetrization")
    for yi, (_, row) in enumerate(network.iterrows()):
        value = float(row["pct_change"])
        if abs(value) < 0.05:
            ax_net.text(0.12, yi, "0.0%", va="center", ha="left", color="#333333", fontweight="bold")
        else:
            ax_net.text(value + 0.12, yi, f"{value:.1f}%", va="center", ha="left", color="white", fontweight="bold")

    ax_profile.axhline(0, color="#222222", lw=0.8)
    ax_profile.plot(
        profile["distance_km"],
        profile["residence_pct"],
        color=ORANGE_DARK,
        marker="o",
        lw=2.1,
        ms=5.0,
        label="Residence fundamental",
    )
    ax_profile.plot(
        profile["distance_km"],
        profile["workplace_pct"],
        color=BLUE_DARK,
        marker="s",
        lw=2.1,
        ms=4.6,
        label="Workplace fundamental",
    )
    ax_profile.set_title("B. Symmetry rotates recovered fundamentals", loc="left", fontweight="bold")
    ax_profile.set_xlabel("Distance to employment center (km)")
    ax_profile.set_ylabel("Symmetric minus directed (100 x log difference)")
    ax_profile.set_xlim(3, max(52, float(profile["distance_km"].max()) + 3))
    ax_profile.set_ylim(-1.75, 1.55)
    ax_profile.yaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{value:.1f}%"))
    ax_profile.legend(loc="upper right", frameon=False)
    ax_profile.text(
        0.03,
        0.05,
        "At least a 5% revision\n"
        f"Residents: {100 * callouts['resident_tail_share']:.1f}%\n"
        f"Jobs: {100 * callouts['job_tail_share']:.1f}%",
        transform=ax_profile.transAxes,
        ha="left",
        va="bottom",
        fontsize=8.2,
        bbox={"boxstyle": "round,pad=0.35", "facecolor": "white", "edgecolor": "#B8B8B8", "alpha": 0.95},
    )
    ax_profile.text(
        0.97,
        0.05,
        "Chaoyang\n"
        f"Residence: {callouts['chaoyang_residence_pct']:+.1f}%\n"
        f"Workplace: {callouts['chaoyang_workplace_pct']:+.1f}%",
        transform=ax_profile.transAxes,
        ha="right",
        va="bottom",
        fontsize=8.2,
        bbox={"boxstyle": "round,pad=0.35", "facecolor": "white", "edgecolor": "#B8B8B8", "alpha": 0.95},
    )

    for ax in (ax_pair, ax_net, ax_profile):
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.tick_params(direction="out", length=3, width=0.7)
        ax.grid(axis="x", color="#E7E7E7", lw=0.6, zorder=0)
        ax.set_axisbelow(True)
    fig.subplots_adjust(left=0.105, right=0.985, bottom=0.14, top=0.93)
    return fig


def build_fundamental_level_profiles(nodes: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    work = nodes.copy()
    profiles = []
    fits = []
    specifications = [
        ("Residence fundamental", "ubar_theta", "residents"),
        ("Workplace fundamental", "abar_theta", "jobs"),
    ]
    for metric, source, weight_col in specifications:
        weights = work[weight_col].to_numpy(dtype=float)
        distance = work["distance_to_employment_center_km"].to_numpy(dtype=float)
        directed = np.log(work[f"{source}_directed"].to_numpy(dtype=float)) / THETA
        symmetric = np.log(work[f"{source}_symmetric"].to_numpy(dtype=float)) / THETA
        common_mean = float(np.average(directed, weights=weights))
        work["_directed_level"] = 100.0 * (directed - common_mean)
        work["_symmetric_level"] = 100.0 * (symmetric - common_mean)
        order = np.argsort(distance)
        weighted_midpoint = (np.cumsum(weights[order]) - 0.5 * weights[order]) / weights.sum()
        weighted_bin = np.empty(len(work), dtype=int)
        weighted_bin[order] = np.minimum((10.0 * weighted_midpoint).astype(int), 9)
        work["_weighted_distance_bin"] = weighted_bin

        for band, group in work.groupby("_weighted_distance_bin", observed=True):
            profiles.append(
                {
                    "metric": metric,
                    "distance_bin": int(band) + 1,
                    "distance_km": float(
                        np.average(group["distance_to_employment_center_km"], weights=group[weight_col])
                    ),
                    "directed_level": weighted_mean(group, "_directed_level", weight_col),
                    "symmetric_level": weighted_mean(group, "_symmetric_level", weight_col),
                    "weight_mass": float(group[weight_col].sum()),
                }
            )

        X = np.column_stack([np.ones(len(distance)), distance])
        sw = np.sqrt(weights / np.mean(weights))
        for world, values in (("Directed", work["_directed_level"]), ("Symmetric", work["_symmetric_level"])):
            coefficient = np.linalg.lstsq(
                X * sw[:, None], values.to_numpy(dtype=float) * sw, rcond=None
            )[0]
            fits.append(
                {
                    "metric": metric,
                    "world": world,
                    "intercept": float(coefficient[0]),
                    "slope_per_km": float(coefficient[1]),
                    "slope_per_10km": float(10.0 * coefficient[1]),
                    "common_normalization": common_mean,
                }
            )
    return pd.DataFrame(profiles), pd.DataFrame(fits)


def draw_exposure_simple(time_metrics: pd.DataFrame, callouts: dict) -> plt.Figure:
    setup_style()
    fig, ax = plt.subplots(figsize=(11.2, 1.15), facecolor="white")
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")

    slow_share = float(callouts["slow_passage_share"])
    x0, y0, width, height = 0.025, 0.30, 0.43, 0.46
    ax.add_patch(Rectangle((x0, y0), width * slow_share, height, color=BLUE_DARK, lw=0))
    ax.add_patch(
        Rectangle(
            (x0 + width * slow_share, y0),
            width * (1.0 - slow_share),
            height,
            color=ORANGE,
            lw=0,
        )
    )
    ax.text(
        x0 + 0.5 * width * slow_share,
        y0 + 0.5 * height,
        f"{100 * slow_share:.1f}%\nSlower-direction passages",
        ha="center",
        va="center",
        color="white",
        fontsize=10.0,
        fontweight="bold",
    )
    ax.text(
        x0 + width * slow_share + 0.5 * width * (1.0 - slow_share),
        y0 + 0.5 * height,
        f"{100 * (1.0 - slow_share):.1f}%\nFaster-direction passages",
        ha="center",
        va="center",
        color="white",
        fontsize=10.0,
        fontweight="bold",
    )

    fixed = float(
        time_metrics.loc[time_metrics["metric"] == "Fixed baseline passages", "pct_change"].iloc[0]
    )
    equilibrium = float(
        time_metrics.loc[time_metrics["metric"] == "Re-estimated equilibrium", "pct_change"].iloc[0]
    )
    ax.annotate("", xy=(0.535, 0.53), xytext=(0.475, 0.53), arrowprops={"arrowstyle": "->", "lw": 1.4, "color": GRAY})
    boxes = [
        (0.63, "Fixed baseline passages", fixed, BLUE),
        (0.86, "Full re-estimation", equilibrium, BLUE_DARK),
    ]
    for x, label, value, color in boxes:
        patch = FancyBboxPatch(
            (x - 0.095, 0.26),
            0.19,
            0.54,
            boxstyle="round,pad=0.012,rounding_size=0.02",
            linewidth=0.9,
            edgecolor=color,
            facecolor="white",
        )
        ax.add_patch(patch)
        ax.text(x, 0.63, label, ha="center", va="center", fontsize=9.0, color="#333333")
        ax.text(x, 0.40, f"{value:.1f}%", ha="center", va="center", fontsize=15.0, fontweight="bold", color=color)
    ax.annotate("", xy=(0.755, 0.53), xytext=(0.725, 0.53), arrowprops={"arrowstyle": "->", "lw": 1.2, "color": GRAY})
    fig.subplots_adjust(left=0.01, right=0.99, bottom=0.03, top=0.97)
    return fig


def draw_fundamental_levels(profile: pd.DataFrame, fits: pd.DataFrame) -> plt.Figure:
    setup_style()
    fig, axes = plt.subplots(1, 2, figsize=(11.2, 3.05), facecolor="white")
    for ax, metric in zip(axes, ("Residence fundamental", "Workplace fundamental")):
        data = profile.query("metric == @metric").sort_values("distance_km")
        fit = fits.query("metric == @metric").set_index("world")
        x = data["distance_km"].to_numpy(dtype=float)
        x_grid = np.linspace(float(x.min()), float(x.max()), 200)
        directed = data["directed_level"].to_numpy(dtype=float)
        symmetric = data["symmetric_level"].to_numpy(dtype=float)
        for xi, yd, ys in zip(x, directed, symmetric):
            ax.plot([xi - 0.32, xi + 0.32], [yd, ys], color="#A8A8A8", lw=0.9, zorder=1)
        ax.scatter(
            x - 0.32,
            directed,
            s=28,
            color=BLUE_DARK,
            edgecolor="white",
            linewidth=0.5,
            label="Directed (asymmetric)",
            zorder=3,
        )
        ax.scatter(
            x + 0.32,
            symmetric,
            s=30,
            marker="D",
            color=ORANGE,
            edgecolor="white",
            linewidth=0.5,
            label="Symmetric",
            zorder=3,
        )
        for world, color, style in (
            ("Directed", BLUE_DARK, "-"),
            ("Symmetric", ORANGE, "--"),
        ):
            row = fit.loc[world]
            ax.plot(
                x_grid,
                row["intercept"] + row["slope_per_km"] * x_grid,
                color=color,
                ls=style,
                lw=1.8,
                zorder=2,
            )
        ax.set_title(metric, loc="left", fontweight="bold")
        ax.set_xlabel("Distance to employment center (km)")
        ax.set_xlim(max(0.0, float(x.min()) - 2.0), float(x.max()) + 2.0)
        ax.grid(color="#E7E7E7", lw=0.6)
        ax.set_axisbelow(True)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.tick_params(direction="out", length=3, width=0.7)
        directed_slope = float(fit.loc["Directed", "slope_per_10km"])
        symmetric_slope = float(fit.loc["Symmetric", "slope_per_10km"])
        ax.text(
            0.98,
            0.96,
            "Fitted gradient per 10 km\n"
            f"Directed: {directed_slope:+.1f}\n"
            f"Symmetric: {symmetric_slope:+.1f}",
            transform=ax.transAxes,
            ha="right",
            va="top",
            fontsize=7.7,
            bbox={"boxstyle": "round,pad=0.28", "facecolor": "white", "edgecolor": "#C0C0C0", "alpha": 0.95},
        )
        mass_label = "residents" if metric.startswith("Residence") else "jobs"
        ax.text(
            0.02,
            0.04,
            f"Each point contains about 10% of {mass_label}",
            transform=ax.transAxes,
            ha="left",
            va="bottom",
            fontsize=7.5,
            color="#555555",
        )
    axes[0].set_ylabel("Normalized log fundamental (100 x log points)")
    axes[0].legend(loc="lower right", frameon=False)
    fig.subplots_adjust(left=0.075, right=0.985, bottom=0.18, top=0.88, wspace=0.20)
    return fig


def build_relative_change_fits(nodes: pd.DataFrame) -> pd.DataFrame:
    rows = []
    specifications = [
        ("Residence fundamental", "delta_log_residence_fundamental", "residents"),
        ("Workplace fundamental", "delta_log_workplace_fundamental", "jobs"),
    ]
    distance = nodes["distance_to_employment_center_km"].to_numpy(dtype=float)
    X = np.column_stack([np.ones(len(distance)), distance])
    for metric, value_col, weight_col in specifications:
        values = 100.0 * nodes[value_col].to_numpy(dtype=float)
        weights = nodes[weight_col].to_numpy(dtype=float)
        sw = np.sqrt(weights / np.mean(weights))
        coefficient = np.linalg.lstsq(
            X * sw[:, None], values * sw, rcond=None
        )[0]
        rows.append(
            {
                "metric": metric,
                "intercept": float(coefficient[0]),
                "slope_per_km": float(coefficient[1]),
                "slope_per_10km": float(10.0 * coefficient[1]),
            }
        )
    return pd.DataFrame(rows)


def draw_fundamental_relative_change(profile: pd.DataFrame, fits: pd.DataFrame) -> plt.Figure:
    setup_style()
    fig, ax = plt.subplots(figsize=(5.4, 3.35), facecolor="white")
    specifications = [
        ("Residence fundamental", "residence_pct", ORANGE_DARK, "o"),
        ("Workplace fundamental", "workplace_pct", BLUE_DARK, "s"),
    ]
    x_grid = np.linspace(4.0, 53.0, 200)
    for metric, value_col, color, marker in specifications:
        row = fits.query("metric == @metric").iloc[0]
        slope = float(row["slope_per_10km"])
        ax.scatter(
            profile["distance_km"],
            profile[value_col],
            s=44,
            marker=marker,
            color=color,
            edgecolor="white",
            linewidth=0.7,
            zorder=3,
        )
        ax.plot(
            x_grid,
            float(row["intercept"]) + float(row["slope_per_km"]) * x_grid,
            color=color,
            lw=2.1,
            label=f"{metric.replace(' fundamental', '')}: {slope:+.2f} per 10 km",
            zorder=2,
        )

    ax.axhline(0.0, color="#333333", lw=1.0, zorder=1)
    ax.set_xlim(3.0, 54.0)
    ax.set_ylim(-1.75, 1.45)
    ax.set_xlabel("Distance to employment center (km)")
    ax.set_ylabel("Symmetric minus directed\n(100 × log difference)")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{value:+.1f}" if value else "0.0"))
    ax.grid(color="#E7E7E7", lw=0.6)
    ax.set_axisbelow(True)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.tick_params(direction="out", length=3, width=0.7)
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, 1.14), ncol=2, frameon=False)
    fig.subplots_adjust(left=0.18, right=0.985, bottom=0.19, top=0.82)
    return fig


def save_figure(fig: plt.Figure, output_stem: str, slide_stem: str) -> None:
    for suffix in ("pdf", "svg", "png"):
        kwargs = {"dpi": 400} if suffix == "png" else {}
        fig.savefig(
            FIGURES / f"{output_stem}.{suffix}",
            bbox_inches="tight",
            facecolor="white",
            **kwargs,
        )
        fig.savefig(
            SLIDE_FIGURES / f"{slide_stem}.{suffix}",
            bbox_inches="tight",
            facecolor="white",
            **kwargs,
        )
    plt.close(fig)


def main() -> None:
    FIGURES.mkdir(parents=True, exist_ok=True)
    SLIDE_FIGURES.mkdir(parents=True, exist_ok=True)
    time_metrics, profile, callouts = build_slide_metrics()
    time_metrics.to_csv(METRICS / "slide_time_percentage_metrics.csv", index=False)
    profile.to_csv(METRICS / "slide_fundamental_distance_profile.csv", index=False)
    pd.DataFrame([callouts]).to_csv(METRICS / "slide_callout_metrics.csv", index=False)
    nodes = pd.read_csv(METRICS / "node_fundamental_deltas_arithmetic.csv")
    level_profile, level_fits = build_fundamental_level_profiles(nodes)
    level_profile.to_csv(METRICS / "slide_fundamental_level_profile.csv", index=False)
    level_fits.to_csv(METRICS / "slide_fundamental_level_gradients.csv", index=False)
    relative_fits = build_relative_change_fits(nodes)
    relative_fits.to_csv(METRICS / "slide_fundamental_relative_change_gradients.csv", index=False)

    fig = draw_figure(time_metrics, profile, callouts)
    save_figure(fig, "figure_symmetry_one_slide_arithmetic", "symmetry_one_slide_arithmetic")
    save_figure(
        draw_exposure_simple(time_metrics, callouts),
        "figure_symmetry_exposure_simple",
        "symmetry_exposure_simple",
    )
    save_figure(
        draw_fundamental_levels(level_profile, level_fits),
        "figure_symmetry_fundamental_levels",
        "symmetry_fundamental_levels",
    )
    save_figure(
        draw_fundamental_relative_change(profile, relative_fits),
        "figure_symmetry_fundamental_relative_change",
        "symmetry_fundamental_relative_change",
    )


if __name__ == "__main__":
    main()
