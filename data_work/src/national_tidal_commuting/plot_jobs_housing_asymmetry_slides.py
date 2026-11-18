from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
from pathlib import Path

import matplotlib as mpl

mpl.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
PROJECT_ROOT = ROOT.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.diagnostics.mpl_setup import ensure_mpl_cache

DEFAULT_JOBS_PATH = (
    ROOT
    / "outputs"
    / "coauthor_revision_attempts_20260710_v1"
    / "01_national_large_city_jobs_housing"
    / "metrics"
    / "top40_city_30pct_summary.csv"
)
DEFAULT_GROUP_PATH = (
    ROOT
    / "outputs"
    / "coauthor_revision_attempts_20260710_v1"
    / "01_national_large_city_jobs_housing"
    / "metrics"
    / "large_city_group_threshold_summary.csv"
)
DEFAULT_ASYM_PATH = (
    ROOT
    / "outputs"
    / "national_tidal_commuting_baidu2025_top100_congestion_v1"
    / "metrics"
    / "triple_crawl_20260514_15"
    / "city_summary.csv"
)
DEFAULT_OUTPUT_DIR = ROOT / "outputs" / "national_jobs_housing_asymmetry_slides_v1"
DEFAULT_SLIDE_FIG_DIR = PROJECT_ROOT / "Documents" / "PreSlides" / "figs"

CITY_EN = {
    "北京市": "Beijing",
    "上海市": "Shanghai",
    "重庆市": "Chongqing",
    "成都市": "Chengdu",
    "广州市": "Guangzhou",
    "深圳市": "Shenzhen",
    "西安市": "Xi'an",
    "天津市": "Tianjin",
    "郑州市": "Zhengzhou",
    "武汉市": "Wuhan",
    "杭州市": "Hangzhou",
    "苏州市": "Suzhou",
    "沈阳市": "Shenyang",
    "长沙市": "Changsha",
    "青岛市": "Qingdao",
    "石家庄市": "Shijiazhuang",
    "南京市": "Nanjing",
    "济南市": "Jinan",
    "合肥市": "Hefei",
    "宁波市": "Ningbo",
    "昆明市": "Kunming",
    "福州市": "Fuzhou",
}

COLORS = {
    "p_to_c": "#2563a6",
    "c_to_p": "#d95f02",
    "diff_pos": "#168a5a",
    "diff_neg": "#b23a48",
    "grid": "#d9dde5",
    "text": "#1f2937",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build slide figures connecting jobs-housing directionality with city-level AM asymmetry."
    )
    parser.add_argument("--jobs-path", type=Path, default=DEFAULT_JOBS_PATH)
    parser.add_argument("--group-path", type=Path, default=DEFAULT_GROUP_PATH)
    parser.add_argument("--asym-path", type=Path, default=DEFAULT_ASYM_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--slide-fig-dir", type=Path, default=DEFAULT_SLIDE_FIG_DIR)
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument("--copy-to-slides", action="store_true")
    return parser.parse_args()


def configure_matplotlib(output_dir: Path) -> None:
    ensure_mpl_cache(str(output_dir))
    mpl.rcParams.update(
        {
            "font.sans-serif": [
                "Arial",
                "Helvetica",
                "Liberation Sans",
                "DejaVu Sans",
                "PingFang SC",
                "Noto Sans CJK SC",
            ],
            "font.family": "sans-serif",
            "axes.unicode_minus": False,
            "svg.fonttype": "none",
            "pdf.fonttype": 42,
            "ps.fonttype": 42,
            "figure.dpi": 180,
            "savefig.dpi": 450,
            "axes.linewidth": 0.8,
            "xtick.major.width": 0.8,
            "ytick.major.width": 0.8,
            "xtick.major.size": 3.0,
            "ytick.major.size": 3.0,
            "axes.labelsize": 8.5,
            "axes.titlesize": 9.3,
            "xtick.labelsize": 7.4,
            "ytick.labelsize": 7.4,
            "legend.fontsize": 7.5,
        }
    )


def clean_city_label(value: str) -> str:
    value = str(value).strip()
    value = re.sub(r"[（(].*?[）)]", "", value)
    return value.strip()


def display_city_name(city_name: str, rank: int | float | None = None) -> str:
    label = CITY_EN.get(city_name, clean_city_label(city_name).removesuffix("市"))
    if rank is None or pd.isna(rank):
        return label
    return f"{int(rank):02d} {label}"


def correlation(x: pd.Series, y: pd.Series) -> tuple[int, float, float]:
    use = pd.DataFrame({"x": x, "y": y}).dropna()
    if len(use) < 3:
        return len(use), float("nan"), float("nan")
    pearson = float(use["x"].corr(use["y"]))
    spearman = float(use["x"].rank().corr(use["y"].rank()))
    return len(use), pearson, spearman


def ensure_dirs(output_dir: Path) -> dict[str, Path]:
    dirs = {
        "root": output_dir,
        "figures": output_dir / "figures",
        "metrics": output_dir / "metrics",
    }
    for path in dirs.values():
        path.mkdir(parents=True, exist_ok=True)
    return dirs


def load_city_data(args: argparse.Namespace) -> tuple[pd.DataFrame, pd.DataFrame]:
    jobs = pd.read_csv(args.jobs_path)
    asym = pd.read_csv(args.asym_path)

    jobs = jobs.copy()
    jobs["city_clean"] = jobs["city_name"].map(clean_city_label)
    jobs = jobs.sort_values("rank_by_inter_town_flow").head(args.top_n).copy()
    jobs["p_to_c_pct"] = 100.0 * jobs["P_to_C_share"]
    jobs["c_to_p_pct"] = 100.0 * jobs["C_to_P_share"]
    jobs["imbalance_pp"] = 100.0 * (jobs["P_to_C_share"] - jobs["C_to_P_share"])

    asym = asym.copy()
    asym["city_clean"] = asym["city_label"].map(clean_city_label)
    keep_cols = [
        "city_id",
        "city_label",
        "city_clean",
        "balanced_pairs",
        "low_coverage_flag",
        "weighted_am_minmax_ratio_median",
        "weighted_pm_minmax_ratio_median",
        "weighted_am_minmax_ratio_mean",
        "weighted_pm_minmax_ratio_mean",
        "am_home_to_work_slower_share",
        "pm_work_to_home_slower_share",
        "reversal_share",
    ]
    merged = jobs.merge(asym[keep_cols], on="city_clean", how="left", suffixes=("", "_asym"))
    merged["am_sar_median"] = merged["weighted_am_minmax_ratio_median"]
    merged["am_asymmetry_degree"] = 1.0 - merged["am_sar_median"]
    merged["city_display"] = [
        display_city_name(city, rank)
        for city, rank in zip(merged["city_name"], merged["rank_by_inter_town_flow"])
    ]
    merged["city_short"] = [display_city_name(city) for city in merged["city_name"]]
    merged["imbalance_sign"] = np.where(merged["imbalance_pp"] >= 0, "positive", "negative")

    group = pd.read_csv(args.group_path)
    group_30 = group[np.isclose(group["core_workplace_share"], 0.3)].copy()
    return merged, group_30


def write_metrics(merged: pd.DataFrame, group_30: pd.DataFrame, dirs: dict[str, Path]) -> dict[str, object]:
    metrics_dir = dirs["metrics"]
    merged.to_csv(metrics_dir / "top20_city_jobs_housing_asymmetry.csv", index=False)
    group_30.to_csv(metrics_dir / "group_threshold_summary_30pct.csv", index=False)

    y_cols = {
        "center_bound_imbalance_pp": merged["imbalance_pp"],
        "P_to_C_share_pct": merged["p_to_c_pct"],
        "C_to_P_share_pct": merged["c_to_p_pct"],
        "P_to_C_over_C_to_P": merged["P_to_C_over_C_to_P"],
    }
    rows = []
    for metric, values in y_cols.items():
        n, pearson_sar, spearman_sar = correlation(merged["am_sar_median"], values)
        _, pearson_degree, spearman_degree = correlation(merged["am_asymmetry_degree"], values)
        rows.append(
            {
                "metric": metric,
                "n": n,
                "pearson_with_am_sar": pearson_sar,
                "spearman_with_am_sar": spearman_sar,
                "pearson_with_am_asymmetry_degree": pearson_degree,
                "spearman_with_am_asymmetry_degree": spearman_degree,
            }
        )
    corr = pd.DataFrame(rows)
    corr.to_csv(metrics_dir / "top20_correlation_summary.csv", index=False)

    summary = {
        "top_n": int(len(merged)),
        "positive_imbalance_cities": int((merged["imbalance_pp"] > 0).sum()),
        "negative_imbalance_cities": int((merged["imbalance_pp"] < 0).sum()),
        "mean_p_to_c_share_pct": float(merged["p_to_c_pct"].mean()),
        "mean_c_to_p_share_pct": float(merged["c_to_p_pct"].mean()),
        "mean_imbalance_pp": float(merged["imbalance_pp"].mean()),
        "flow_weighted_top20_imbalance_pp": float(
            group_30.loc[group_30["city_group"].eq("top20_flow"), "center_bound_imbalance_pp"].iloc[0]
        ),
        "flow_weighted_top20_p_to_c_share_pct": float(
            100.0 * group_30.loc[group_30["city_group"].eq("top20_flow"), "P_to_C_share"].iloc[0]
        ),
        "flow_weighted_top20_c_to_p_share_pct": float(
            100.0 * group_30.loc[group_30["city_group"].eq("top20_flow"), "C_to_P_share"].iloc[0]
        ),
        "flow_weighted_all_city_imbalance_pp": float(
            group_30.loc[group_30["city_group"].eq("all_cities"), "center_bound_imbalance_pp"].iloc[0]
        ),
        "correlations": rows,
    }
    (metrics_dir / "summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False))
    return summary


def save_figure(
    fig: mpl.figure.Figure,
    dirs: dict[str, Path],
    file_stem: str,
    slide_stem: str,
    copy_to_slides: bool,
    slide_fig_dir: Path,
) -> dict[str, str]:
    outputs: dict[str, str] = {}
    for ext in ("png", "pdf"):
        out_path = dirs["figures"] / f"{file_stem}.{ext}"
        fig.savefig(out_path, bbox_inches="tight", facecolor="white")
        outputs[ext] = str(out_path)
        if copy_to_slides:
            slide_fig_dir.mkdir(parents=True, exist_ok=True)
            shutil.copy2(out_path, slide_fig_dir / f"{slide_stem}.{ext}")
    return outputs


def add_regression_line(ax: mpl.axes.Axes, x: pd.Series, y: pd.Series, color: str) -> None:
    use = pd.DataFrame({"x": x, "y": y}).dropna()
    if len(use) < 3:
        return
    slope, intercept = np.polyfit(use["x"], use["y"], deg=1)
    x_grid = np.linspace(use["x"].min(), use["x"].max(), 100)
    ax.plot(x_grid, intercept + slope * x_grid, color=color, lw=1.2, alpha=0.85)


def style_axes(ax: mpl.axes.Axes) -> None:
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(True, axis="both", color=COLORS["grid"], lw=0.55, alpha=0.7)
    ax.set_axisbelow(True)


def plot_top20_in_out(
    merged: pd.DataFrame,
    dirs: dict[str, Path],
    copy_to_slides: bool,
    slide_fig_dir: Path,
) -> dict[str, str]:
    plot_df = merged.sort_values("rank_by_inter_town_flow").copy()
    y = np.arange(len(plot_df))
    fig, (ax_share, ax_diff) = plt.subplots(
        1,
        2,
        figsize=(9.3, 6.2),
        gridspec_kw={"width_ratios": [1.48, 0.82], "wspace": 0.14},
    )
    h = 0.36
    ax_share.barh(y - h / 2, plot_df["p_to_c_pct"], height=h, color=COLORS["p_to_c"], label="Periphery to core")
    ax_share.barh(y + h / 2, plot_df["c_to_p_pct"], height=h, color=COLORS["c_to_p"], label="Core to periphery")
    ax_share.set_yticks(y)
    ax_share.set_yticklabels(plot_df["city_display"], fontsize=8.1)
    ax_share.invert_yaxis()
    ax_share.set_xlabel("Share of inter-town commuters (%)", fontsize=8.8)
    ax_share.set_title("A. Directional commuting shares", loc="left", fontweight="bold")
    ax_share.legend(frameon=False, loc="lower right")
    style_axes(ax_share)
    ax_share.grid(False, axis="y")

    diff_colors = np.where(plot_df["imbalance_pp"] >= 0, COLORS["diff_pos"], COLORS["diff_neg"])
    ax_diff.barh(y, plot_df["imbalance_pp"], color=diff_colors, height=0.52)
    ax_diff.axvline(0, color="#60646c", lw=0.8)
    ax_diff.set_yticks(y)
    ax_diff.set_yticklabels([])
    ax_diff.invert_yaxis()
    ax_diff.set_xlabel("P to C minus C to P (pp)", fontsize=8.8)
    ax_diff.set_title("B. Gap", loc="left", fontweight="bold")
    style_axes(ax_diff)
    ax_diff.grid(False, axis="y")
    max_diff = max(1.0, float(plot_df["imbalance_pp"].abs().max()))
    ax_diff.set_xlim(min(0.0, plot_df["imbalance_pp"].min()) - 0.8, max_diff + 1.8)
    for yi, value in zip(y, plot_df["imbalance_pp"]):
        ha = "left" if value >= 0 else "right"
        offset = 0.22 if value >= 0 else -0.22
        ax_diff.text(value + offset, yi, f"{value:+.1f}", va="center", ha=ha, fontsize=7.5, color=COLORS["text"])

    fig.subplots_adjust(left=0.16, right=0.985, top=0.94, bottom=0.12)
    outputs = save_figure(fig, dirs, "top20_inbound_outbound_imbalance", "pat_jobs_housing_top20_in_out", copy_to_slides, slide_fig_dir)
    plt.close(fig)
    return outputs


def plot_scatter_panels(
    merged: pd.DataFrame,
    corr_summary: dict[str, object],
    dirs: dict[str, Path],
    copy_to_slides: bool,
    slide_fig_dir: Path,
) -> dict[str, str]:
    fig, axes = plt.subplots(1, 2, figsize=(10.5, 4.3), sharex=True)
    specs = [
        ("imbalance_pp", "P to C minus C to P (pp)", "A. Imbalance", COLORS["diff_pos"]),
        ("p_to_c_pct", "P to C share (%)", "B. Center-bound share", COLORS["p_to_c"]),
    ]
    label_cities = {"Beijing", "Shanghai", "Guangzhou", "Shenzhen", "Hangzhou", "Zhengzhou", "Chongqing", "Chengdu"}
    corr_lookup = {row["metric"]: row for row in corr_summary["correlations"]}
    for ax, (y_col, y_label, title, line_color) in zip(axes, specs):
        point_colors = np.where(merged["imbalance_pp"] >= 0, COLORS["diff_pos"], COLORS["diff_neg"])
        size = 34 + 55 * (merged["city_inter_town_commuters"] / merged["city_inter_town_commuters"].max())
        ax.scatter(
            merged["am_sar_median"],
            merged[y_col],
            s=size,
            color=point_colors,
            edgecolor="white",
            linewidth=0.8,
            alpha=0.92,
            zorder=3,
        )
        add_regression_line(ax, merged["am_sar_median"], merged[y_col], line_color)
        for _, row in merged.iterrows():
            if row["city_short"] in label_cities:
                ax.text(
                    row["am_sar_median"] + 0.0015,
                    row[y_col] + 0.12,
                    row["city_short"],
                    fontsize=6.2,
                    color=COLORS["text"],
                )
        metric_key = "center_bound_imbalance_pp" if y_col == "imbalance_pp" else "P_to_C_share_pct"
        corr = corr_lookup[metric_key]
        ax.text(
            0.02,
            0.95,
            f"Pearson r = {corr['pearson_with_am_sar']:.2f}\nSpearman rho = {corr['spearman_with_am_sar']:.2f}",
            transform=ax.transAxes,
            ha="left",
            va="top",
            fontsize=7.3,
            bbox={"facecolor": "white", "edgecolor": "#c8ccd2", "boxstyle": "round,pad=0.25", "alpha": 0.9},
        )
        ax.set_title(title, loc="left", fontweight="bold")
        ax.set_ylabel(y_label)
        ax.set_xlabel("AM SAR, lower means stronger asymmetry")
        style_axes(ax)
    axes[0].set_xlim(0.75, 0.93)
    fig.subplots_adjust(left=0.07, right=0.985, top=0.88, bottom=0.16, wspace=0.22)
    outputs = save_figure(fig, dirs, "asym_ratio_scatter_two_panel", "pat_jobs_housing_asym_scatter", copy_to_slides, slide_fig_dir)
    plt.close(fig)
    return outputs


def plot_dumbbell(
    merged: pd.DataFrame,
    dirs: dict[str, Path],
    copy_to_slides: bool,
    slide_fig_dir: Path,
) -> dict[str, str]:
    fig, ax = plt.subplots(figsize=(6.3, 4.9))
    plot_df = merged.sort_values("am_sar_median").copy()
    for _, row in plot_df.iterrows():
        color = COLORS["diff_pos"] if row["imbalance_pp"] >= 0 else COLORS["diff_neg"]
        ax.plot(
            [row["am_sar_median"], row["am_sar_median"]],
            [row["c_to_p_pct"], row["p_to_c_pct"]],
            color=color,
            lw=1.4,
            alpha=0.75,
            zorder=1,
        )
    ax.scatter(
        plot_df["am_sar_median"],
        plot_df["p_to_c_pct"],
        color=COLORS["p_to_c"],
        edgecolor="white",
        linewidth=0.7,
        s=45,
        label="Periphery to core",
        zorder=3,
    )
    ax.scatter(
        plot_df["am_sar_median"],
        plot_df["c_to_p_pct"],
        color=COLORS["c_to_p"],
        edgecolor="white",
        linewidth=0.7,
        marker="s",
        s=38,
        label="Core to periphery",
        zorder=3,
    )
    label_cities = {"Beijing", "Shanghai", "Guangzhou", "Shenzhen", "Hangzhou", "Zhengzhou", "Chongqing", "Chengdu", "Shenyang"}
    for i, (_, row) in enumerate(plot_df.iterrows()):
        if row["city_short"] not in label_cities:
            continue
        offset = 0.18 if i % 2 == 0 else -0.34
        ax.text(
            row["am_sar_median"] + 0.0013,
            row["p_to_c_pct"] + offset,
            row["city_short"],
            fontsize=6.0,
            color=COLORS["text"],
        )
    ax.set_xlabel("AM SAR, lower means stronger asymmetry")
    ax.set_ylabel("Share of inter-town commuters (%)")
    ax.set_title("Directional shares at each city SAR", loc="left", fontweight="bold")
    style_axes(ax)
    ax.legend(frameon=False, loc="lower right")
    ax.set_xlim(0.75, 0.93)
    y_min = min(plot_df["c_to_p_pct"].min(), plot_df["p_to_c_pct"].min()) - 1.4
    y_max = max(plot_df["c_to_p_pct"].max(), plot_df["p_to_c_pct"].max()) + 1.7
    ax.set_ylim(y_min, y_max)
    fig.subplots_adjust(left=0.13, right=0.985, top=0.90, bottom=0.15)
    outputs = save_figure(fig, dirs, "asym_ratio_dumbbell", "pat_jobs_housing_asym_dumbbell", copy_to_slides, slide_fig_dir)
    plt.close(fig)
    return outputs


def main() -> None:
    args = parse_args()
    dirs = ensure_dirs(args.output_dir)
    configure_matplotlib(args.output_dir)
    merged, group_30 = load_city_data(args)
    missing = merged[merged["am_sar_median"].isna()][["city_name", "city_clean"]]
    if not missing.empty:
        raise ValueError(f"Missing city-level asymmetry rows: {missing.to_dict('records')}")

    summary = write_metrics(merged, group_30, dirs)
    figure_outputs = {
        "top20_in_out": plot_top20_in_out(merged, dirs, args.copy_to_slides, args.slide_fig_dir),
        "scatter": plot_scatter_panels(merged, summary, dirs, args.copy_to_slides, args.slide_fig_dir),
        "dumbbell": plot_dumbbell(merged, dirs, args.copy_to_slides, args.slide_fig_dir),
    }
    manifest = {
        "inputs": {
            "jobs_path": str(args.jobs_path),
            "group_path": str(args.group_path),
            "asym_path": str(args.asym_path),
        },
        "outputs": figure_outputs,
        "summary": summary,
    }
    (dirs["root"] / "manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False))
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
