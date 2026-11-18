from __future__ import annotations

from pathlib import Path

import matplotlib as mpl

mpl.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
CITY_PATH = (
    ROOT
    / "data_work"
    / "outputs"
    / "coauthor_revision_attempts_20260710_v1"
    / "01_national_large_city_jobs_housing"
    / "metrics"
    / "city_ranked_30pct_summary.csv"
)
OUTPUT_STEM = ROOT / "Documents" / "PreSlides" / "figs" / "pat_jobs_housing_top20_gap_strip"
OUTPUT_COLUMN_STEM = ROOT / "Documents" / "PreSlides" / "figs" / "pat_jobs_housing_top20_gap_column"
OUTPUT_LOLLIPOP_STEM = ROOT / "Documents" / "PreSlides" / "figs" / "pat_jobs_housing_top20_gap_lollipop"

CITY_EN = {
    "北京市": "Beijing", "上海市": "Shanghai", "重庆市": "Chongqing",
    "成都市": "Chengdu", "广州市": "Guangzhou", "深圳市": "Shenzhen",
    "西安市": "Xi'an", "天津市": "Tianjin", "郑州市": "Zhengzhou",
    "武汉市": "Wuhan", "杭州市": "Hangzhou", "苏州市": "Suzhou",
    "沈阳市": "Shenyang", "长沙市": "Changsha", "青岛市": "Qingdao",
    "石家庄市": "Shijiazhuang", "南京市": "Nanjing", "济南市": "Jinan",
    "合肥市": "Hefei", "宁波市": "Ningbo",
}


def main() -> None:
    city = pd.read_csv(CITY_PATH)
    city = city.loc[city["rank_by_inter_town_flow"].le(20)].copy()
    city = city.sort_values("center_bound_imbalance_pp").reset_index(drop=True)

    mpl.rcParams.update(
        {
            "font.family": "sans-serif",
            "font.sans-serif": ["Arial", "Helvetica", "Liberation Sans", "DejaVu Sans"],
            "axes.linewidth": 0.8,
            "xtick.major.width": 0.8,
            "xtick.major.size": 3.0,
            "pdf.fonttype": 42,
            "ps.fonttype": 42,
            "svg.fonttype": "none",
        }
    )

    x = city["center_bound_imbalance_pp"].to_numpy()
    jitter_pattern = np.array([0.43, 0.50, 0.37, 0.55, 0.46])
    y = np.resize(jitter_pattern, len(city))

    fig, ax = plt.subplots(figsize=(10.0, 1.05))
    ax.scatter(
        x,
        y,
        s=48,
        color="#4C78A8",
        edgecolor="white",
        linewidth=0.7,
        zorder=3,
    )
    ax.axvline(0, color="#1f1f1f", lw=1.0, zorder=1)
    ax.text(
        0.18,
        0.62,
        "20 of 20 cities have a positive gap",
        color="#1f1f1f",
        fontsize=10.5,
        fontweight="bold",
        ha="left",
        va="center",
    )
    ax.set_xlim(-0.45, 10.1)
    ax.set_ylim(0.27, 0.67)
    ax.set_yticks([])
    ax.set_xlabel("Center-bound minus reverse commuting share (percentage points)", fontsize=9.2)
    ax.tick_params(axis="x", labelsize=8.5)
    ax.spines["left"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["top"].set_visible(False)
    ax.grid(axis="x", color="#D9DDE5", lw=0.6, alpha=0.8)
    ax.set_axisbelow(True)

    fig.subplots_adjust(left=0.035, right=0.99, top=0.96, bottom=0.37)
    for suffix in ("pdf", "png", "svg"):
        path = OUTPUT_STEM.with_suffix(f".{suffix}")
        kwargs = {"bbox_inches": "tight", "pad_inches": 0.03}
        if suffix == "png":
            kwargs["dpi"] = 600
        fig.savefig(path, **kwargs)
    plt.close(fig)

    y_column = np.resize(np.array([0.42, 0.54, 0.34, 0.62, 0.47]), len(city))
    fig, ax = plt.subplots(figsize=(4.6, 2.35))
    ax.scatter(
        x,
        y_column,
        s=52,
        color="#4C78A8",
        edgecolor="white",
        linewidth=0.7,
        zorder=3,
    )
    ax.axvline(0, color="#1f1f1f", lw=1.0, zorder=1)
    ax.text(
        0.18,
        0.80,
        "20 of 20 cities have a positive gap",
        color="#1f1f1f",
        fontsize=10.0,
        fontweight="bold",
        ha="left",
        va="center",
    )
    ax.text(
        9.85,
        0.25,
        "Each dot is one city",
        color="#555555",
        fontsize=8.0,
        ha="right",
        va="center",
    )
    ax.set_xlim(-0.45, 10.1)
    ax.set_ylim(0.18, 0.87)
    ax.set_yticks([])
    ax.set_xlabel("Center-bound minus reverse commuting share (pp)", fontsize=9.2)
    ax.tick_params(axis="x", labelsize=8.5)
    ax.spines["left"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["top"].set_visible(False)
    ax.grid(axis="x", color="#D9DDE5", lw=0.6, alpha=0.8)
    ax.set_axisbelow(True)
    fig.subplots_adjust(left=0.08, right=0.99, top=0.96, bottom=0.24)
    for suffix in ("pdf", "png", "svg"):
        path = OUTPUT_COLUMN_STEM.with_suffix(f".{suffix}")
        kwargs = {"bbox_inches": "tight", "pad_inches": 0.03}
        if suffix == "png":
            kwargs["dpi"] = 600
        fig.savefig(path, **kwargs)
    plt.close(fig)

    # City-by-city lollipop chart for the two-column main slide.
    city_ranked = city.sort_values("rank_by_inter_town_flow").reset_index(drop=True)
    city_ranked["city_en"] = city_ranked["city_name"].map(CITY_EN).fillna(city_ranked["city_name"])
    xpos = np.arange(len(city_ranked))
    gap = city_ranked["center_bound_imbalance_pp"].to_numpy()

    fig, ax = plt.subplots(figsize=(7.2, 3.15))
    ax.vlines(xpos, 0, gap, color="#91A9C3", linewidth=1.5, zorder=2)
    ax.scatter(xpos, gap, s=28, color="#2F6FA5", edgecolor="white", linewidth=0.55, zorder=3)
    ax.axhline(0, color="#202020", linewidth=0.8, zorder=1)
    ax.set_xlim(-0.65, len(city_ranked) - 0.35)
    ax.set_ylim(0, 10.25)
    ax.set_ylabel("Core-bound minus reverse share (pp)", fontsize=9.0)
    ax.set_xticks(xpos)
    ax.set_xticklabels(city_ranked["city_en"], rotation=62, ha="right", rotation_mode="anchor")
    ax.tick_params(axis="x", labelsize=7.2, pad=1.5)
    ax.tick_params(axis="y", labelsize=8.0)
    ax.spines["right"].set_visible(False)
    ax.spines["top"].set_visible(False)
    ax.grid(axis="y", color="#D9DDE5", linewidth=0.55, alpha=0.85)
    ax.set_axisbelow(True)
    fig.subplots_adjust(left=0.105, right=0.995, top=0.97, bottom=0.40)
    for suffix in ("pdf", "png", "svg"):
        path = OUTPUT_LOLLIPOP_STEM.with_suffix(f".{suffix}")
        kwargs = {"bbox_inches": "tight", "pad_inches": 0.025}
        if suffix == "png":
            kwargs["dpi"] = 600
        fig.savefig(path, **kwargs)
    plt.close(fig)


if __name__ == "__main__":
    main()
