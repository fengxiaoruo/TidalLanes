"""
从指定的三轮 crawl_sessions（FREE / AM / PM 各一轮）读取 session_matrix_flat.csv，
生成独立统计目录：metrics + figures + Markdown 说明。

不依赖 api 目录下其它历史 session；显式传入 --session-dir 可覆盖默认三轮。

示例：
  cd data_work
  python3.11 -m src.national_tidal_commuting.report_triple_crawl_campaign \\
    --config config/national_tidal_commuting_baidu2025_top100_congestion_v1.json \\
    --report-id triple_crawl_20260514_15
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.diagnostics.mpl_setup import ensure_mpl_cache
from src.national_tidal_commuting.common import ensure_version_dirs, get_version_root, load_config
from scipy import stats

from src.national_tidal_commuting.summarize_results import (
    build_pair_summary,
    plot_hist,
    plot_rank_vs_asym,
    plot_representative_cities,
    summarize_city,
)


def _city_tick_labels(df: pd.DataFrame) -> pd.Series:
    """Use city_id on axes (ASCII) instead of Chinese city_label."""
    if "city_id" in df.columns:
        return df["city_id"].astype(str)
    return df.index.astype(str)


MINMAX_YLABEL = "Speed asymmetry ratio"
SAR_DEFINITION = (
    r"Speed asymmetry ratio $=\min(x_{AB},x_{BA})/\max(x_{AB},x_{BA})$; "
    r"$x$ = peak travel time (OD) or peak speed (centerlines); lower = stronger asymmetry."
)


def plot_city_bar_en(city_summary: pd.DataFrame, value_col: str, title: str, out_path: Path) -> None:
    use = city_summary.sort_values(value_col, ascending=True).copy()
    fig, ax = plt.subplots(figsize=(12, 5.5))
    ax.bar(_city_tick_labels(use), use[value_col], color="#2b6cb0")
    ax.set_title(title)
    ax.set_ylabel(MINMAX_YLABEL)
    ax.set_xlabel("City (city_id)")
    ax.tick_params(axis="x", rotation=90, labelsize=7)
    fig.tight_layout()
    fig.savefig(out_path, dpi=180)
    plt.close(fig)


def plot_reversal_en(city_summary: pd.DataFrame, out_path: Path) -> None:
    use = city_summary.sort_values("reversal_share", ascending=False).copy()
    fig, ax = plt.subplots(figsize=(12, 5.5))
    ax.bar(_city_tick_labels(use), use["reversal_share"], color="#805ad5")
    ax.set_title("Reversal share by city (AM vs PM dominant direction)")
    ax.set_ylabel("Weighted share")
    ax.set_xlabel("City (city_id)")
    ax.tick_params(axis="x", rotation=90, labelsize=7)
    fig.tight_layout()
    fig.savefig(out_path, dpi=180)
    plt.close(fig)


def plot_representative_cities_en(pair_summary: pd.DataFrame, out_path: Path) -> None:
    use = pair_summary[pair_summary["balanced_sample_flag"]].copy()
    top_cities = (
        use.groupby(["city_id", "city_label"], as_index=False)["pop_total_pair"]
        .sum()
        .sort_values("pop_total_pair", ascending=False)
        .head(4)
    )
    if top_cities.empty:
        return
    fig, axes = plt.subplots(len(top_cities), 1, figsize=(10, 3.4 * len(top_cities)), sharex=False)
    if len(top_cities) == 1:
        axes = [axes]
    for ax, city in zip(axes, top_cities.itertuples(index=False)):
        sub = use[use["city_id"] == city.city_id].sort_values("rank_in_city").head(5).copy()
        x = np.arange(len(sub))
        ax.plot(x, sub["ab_am_sec"] / 60.0, marker="o", color="#2b6cb0", label="AM home->work")
        ax.plot(x, sub["ba_am_sec"] / 60.0, marker="o", color="#63b3ed", label="AM work->home")
        ax.plot(x, sub["ab_pm_sec"] / 60.0, marker="s", color="#dd6b20", label="PM home->work")
        ax.plot(x, sub["ba_pm_sec"] / 60.0, marker="s", color="#f6ad55", label="PM work->home")
        ax.set_title(str(city.city_id))
        ax.set_ylabel("Minutes")
        ax.set_xticks(x, [f"#{int(v)}" for v in sub["rank_in_city"]])
    axes[0].legend(frameon=False, ncols=2)
    axes[-1].set_xlabel("Top commuting pair rank in city")
    fig.tight_layout()
    fig.savefig(out_path, dpi=180)
    plt.close(fig)

DEFAULT_SESSION_IDS = ("20260515T020001", "20260515T070000", "20260514T173000")


def load_selected_sessions(api_dir: Path, session_ids: tuple[str, ...]) -> pd.DataFrame:
    parts = []
    for sid in session_ids:
        path = api_dir / "crawl_sessions" / sid / "session_matrix_flat.csv"
        if not path.exists():
            raise FileNotFoundError(f"Missing session flat: {path}")
        df = pd.read_csv(path)
        df["source_session_id"] = sid
        parts.append(df)
    out = pd.concat(parts, ignore_index=True)
    out["query_time_local"] = pd.to_datetime(out["query_time_local"], errors="coerce")
    out = out.sort_values("query_time_local")
    key_cols = ["city_id", "pair_id", "query_window", "origin_type", "dest_type"]
    chosen = []
    for _, grp in out.groupby(key_cols, sort=False):
        grp = grp.sort_values("query_time_local")
        ok = grp[grp["api_status"] == 0]
        chosen.append(ok.tail(1) if not ok.empty else grp.tail(1))
    return pd.concat(chosen, ignore_index=True).reset_index(drop=True)


def plot_duration_triple_overlay(pair_summary: pd.DataFrame, out_path: Path) -> None:
    use = pair_summary[pair_summary["balanced_sample_flag"]].copy()
    fig, ax = plt.subplots(figsize=(8, 5))
    for col, lab, c in [
        ("ab_free_sec", "FREE home->work (min)", "#4a5568"),
        ("ab_am_sec", "AM home->work (min)", "#2b6cb0"),
        ("ab_pm_sec", "PM home->work (min)", "#dd6b20"),
    ]:
        s = pd.to_numeric(use[col], errors="coerce").dropna() / 60.0
        ax.hist(s, bins=40, alpha=0.45, color=c, label=lab, density=True)
    ax.set_title("Home->work travel time (pooled, balanced pairs)")
    ax.set_xlabel("Minutes")
    ax.set_ylabel("Density")
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(out_path, dpi=180)
    plt.close(fig)


def plot_am_vs_pm_ratio_scatter(pair_summary: pd.DataFrame, out_path: Path) -> None:
    use = pair_summary[pair_summary["balanced_sample_flag"]].copy()
    x = use["am_minmax_ratio"].astype(float)
    y = use["pm_minmax_ratio"].astype(float)
    w = np.sqrt(np.clip(use["pop_total_pair"].fillna(0).astype(float), 1, None))
    fig, ax = plt.subplots(figsize=(6.5, 6.5))
    ax.scatter(x, y, s=np.clip(w, 8, 120), alpha=0.35, color="#2c5282", edgecolors="none")
    lim = (0.45, 1.0)
    ax.plot(lim, lim, color="#718096", linestyle="--", linewidth=1)
    ax.set_xlim(lim)
    ax.set_ylim(lim)
    ax.set_xlabel(f"AM: {MINMAX_YLABEL}")
    ax.set_ylabel(f"PM: {MINMAX_YLABEL}")
    ax.set_title("Directional asymmetry: AM vs PM (marker size ~ sqrt pair weight)")
    fig.tight_layout()
    fig.savefig(out_path, dpi=180)
    plt.close(fig)


def plot_peak_penalty_hist(pair_summary: pd.DataFrame, out_path: Path) -> None:
    use = pair_summary[pair_summary["balanced_sample_flag"]].copy()
    am_p = pd.to_numeric(use["am_peak_penalty_home_work"], errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    pm_p = pd.to_numeric(use["pm_peak_penalty_work_home"], errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.hist(am_p.clip(0.5, 3.0), bins=35, alpha=0.55, color="#2b6cb0", label="AM / FREE (home->work)", density=True)
    ax.hist(pm_p.clip(0.5, 3.0), bins=35, alpha=0.45, color="#dd6b20", label="PM / FREE (work->home)", density=True)
    ax.set_title("Peak vs FREE duration ratio (clipped 0.5–3 for display)")
    ax.set_xlabel("Ratio")
    ax.set_ylabel("Density")
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(out_path, dpi=180)
    plt.close(fig)


def _merge_city_congestion_meta(city_summary: pd.DataFrame, pair_summary: pd.DataFrame) -> pd.DataFrame:
    meta = pair_summary.groupby(["city_id", "city_label"], as_index=False).agg(
        source_congestion_index=("source_congestion_index", "first"),
        source_congestion_rank=("source_congestion_rank", "first"),
    )
    return city_summary.merge(meta, on=["city_id", "city_label"], how="left")


def _plot_congestion_asymmetry_panel(
    ax: plt.Axes,
    merged: pd.DataFrame,
    ycol: str,
    ylab: str,
    color: str,
) -> dict | None:
    sub = merged.dropna(subset=["source_congestion_index", ycol])
    x = sub["source_congestion_index"].astype(float)
    y = sub[ycol].astype(float)
    ax.scatter(x, y, s=42, alpha=0.65, color=color, edgecolors="white", linewidths=0.4)
    if len(sub) < 2:
        ax.set_xlabel("Baidu 2025 commute-peak congestion index")
        ax.set_ylabel("AM median speed asymmetry ratio")
        ax.set_title("AM median speed asymmetry ratio")
        return None

    slope, intercept, r, p, _ = stats.linregress(x, y)
    xs = np.linspace(x.min(), x.max(), 50)
    ax.plot(xs, intercept + slope * xs, color="#4a5568", linestyle="--", linewidth=1.2)
    rho, p_s = stats.spearmanr(x, y)
    from src.diagnostics.preslide_asymmetry_style import FONT

    ax.text(
        0.04,
        0.96,
        f"n={len(sub)}\nPearson r={r:.2f} (p={p:.3f})\nSpearman rho={rho:.2f} (p={p_s:.3f})",
        transform=ax.transAxes,
        va="top",
        fontsize=FONT["stats"],
        bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.85, edgecolor="none"),
    )
    ax.set_xlabel("Baidu 2025 commute-peak congestion index")
    ax.set_ylabel("AM median speed asymmetry ratio")
    ax.set_title("AM median speed asymmetry ratio")
    return {
        "panel": ycol,
        "n": len(sub),
        "pearson_r": r,
        "pearson_p": p,
        "spearman_rho": rho,
        "spearman_p": p_s,
    }


def plot_pooled_ratio_hist_am_pm(
    pair_summary: pd.DataFrame,
    out_path: Path,
    *,
    dpi: int = 240,
    include_definition: bool = False,
) -> None:
    use = pair_summary[pair_summary["balanced_sample_flag"]].copy()
    fig, ax = plt.subplots(figsize=(8.4, 5.6))
    for col, label, color in [
        ("am_minmax_ratio", "AM", "#c44e52"),
        ("pm_minmax_ratio", "PM", "#dd8452"),
    ]:
        ax.hist(
            use[col].dropna(),
            bins=36,
            range=(0.35, 1.0),
            histtype="step",
            linewidth=2.0,
            color=color,
            label=label,
        )
    ax.set_title("Speed asymmetry ratio across AM and PM peaks")
    ax.set_xlabel("Speed asymmetry ratio")
    ax.set_ylabel("Number of town pairs")
    ax.legend(frameon=False)
    if include_definition:
        ax.text(
            0.02,
            0.02,
            SAR_DEFINITION,
            transform=ax.transAxes,
            fontsize=6,
            va="bottom",
            ha="left",
            bbox=dict(boxstyle="round,pad=0.25", facecolor="white", alpha=0.82, edgecolor="none"),
        )
    fig.tight_layout()
    fig.savefig(out_path, dpi=dpi, bbox_inches="tight", facecolor="white")
    plt.close(fig)


def plot_congestion_vs_am_asymmetry(
    city_summary: pd.DataFrame,
    pair_summary: pd.DataFrame,
    out_path: Path,
    *,
    dpi: int = 240,
) -> dict | None:
    merged = _merge_city_congestion_meta(city_summary, pair_summary)
    fig, ax = plt.subplots(figsize=(6.4, 5.6))
    stat = _plot_congestion_asymmetry_panel(
        ax,
        merged,
        "weighted_am_minmax_ratio_median",
        "AM median speed asymmetry ratio",
        "#2b6cb0",
    )
    fig.suptitle("City congestion vs speed asymmetry ratio (top-100 sample)", fontsize=12, y=1.02)
    fig.tight_layout()
    fig.savefig(out_path, dpi=dpi, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return stat


def plot_congestion_vs_asymmetry(
    city_summary: pd.DataFrame,
    pair_summary: pd.DataFrame,
    fig_path: Path,
    csv_path: Path,
    corr_path: Path,
) -> None:
    merged = _merge_city_congestion_meta(city_summary, pair_summary)
    merged.to_csv(csv_path, index=False)

    fig, axes = plt.subplots(1, 2, figsize=(12, 5.2))
    panels = [
        ("weighted_am_minmax_ratio_median", "AM weighted median min/max ratio", "#2b6cb0"),
        ("weighted_pm_minmax_ratio_median", "PM weighted median min/max ratio", "#dd6b20"),
    ]
    stats_rows = []
    for ax, (ycol, ylab, color) in zip(axes, panels):
        stat = _plot_congestion_asymmetry_panel(ax, merged, ycol, ylab, color)
        if stat is not None:
            stats_rows.append(stat)

    fig.suptitle("City congestion vs directional asymmetry (top-100 sample)", fontsize=12, y=1.02)
    fig.tight_layout()
    fig.savefig(fig_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    pd.DataFrame(stats_rows).to_csv(corr_path, index=False)


def plot_preslide_national_am_histogram(
    pair_summary: pd.DataFrame,
    out_path: Path,
    *,
    dpi: int = 300,
    include_definition: bool = False,
) -> None:
    from src.diagnostics.preslide_asymmetry_style import (
        FIG_HIST,
        FONT,
        SAR_COLOR_SINGLE,
        SAR_TITLE,
        SAR_XLABEL,
        add_sar_definition_once,
        add_sar_stats_box,
        configure_preslide_asymmetry_mpl,
        finalize_sar_figure,
        plot_sar_bar_histogram,
        plot_sar_median_line,
        style_sar_axes,
    )

    configure_preslide_asymmetry_mpl()
    use = pair_summary[pair_summary["balanced_sample_flag"]].copy()
    values = use["am_minmax_ratio"].astype(float).to_numpy()
    weights = use["pop_total_pair"].astype(float).to_numpy()
    s = use.sort_values("am_minmax_ratio")
    cw = s["pop_total_pair"].cumsum()
    wmed = float(s.loc[cw >= s["pop_total_pair"].sum() / 2, "am_minmax_ratio"].iloc[0])
    wshare90 = float(np.average((values <= 0.90).astype(float), weights=weights))

    fig, ax = plt.subplots(figsize=FIG_HIST)
    plot_sar_bar_histogram(ax, values, weights=weights, color=SAR_COLOR_SINGLE)
    plot_sar_median_line(ax, wmed)
    add_sar_stats_box(
        ax,
        f"AM town pairs: {len(use):,}\n"
        f"weighted median = {wmed:.3f}\n"
        f"share $\\leq 0.90$ = {100 * wshare90:.1f}%",
    )
    if include_definition:
        add_sar_definition_once(fig)
    ax.set_title(SAR_TITLE)
    style_sar_axes(ax, ylabel="Pair-flow weight")
    finalize_sar_figure(fig, bottom_margin=0.17 if include_definition else None)
    fig.savefig(out_path, dpi=dpi, bbox_inches="tight", facecolor="white")
    plt.close(fig)


def plot_preslide_national_am_scatter(
    city_summary: pd.DataFrame,
    pair_summary: pd.DataFrame,
    out_path: Path,
    *,
    dpi: int = 300,
) -> dict | None:
    from src.diagnostics.preslide_asymmetry_style import (
        FIG_SCATTER,
        FONT,
        SAR_COLOR_SCATTER,
        SAR_TITLE,
        configure_preslide_asymmetry_mpl,
        finalize_sar_figure,
        style_sar_axes,
    )

    configure_preslide_asymmetry_mpl()
    merged = _merge_city_congestion_meta(city_summary, pair_summary)
    fig, ax = plt.subplots(figsize=FIG_SCATTER)
    stat = _plot_congestion_asymmetry_panel(
        ax,
        merged,
        "weighted_am_minmax_ratio_median",
        "AM median speed asymmetry ratio",
        SAR_COLOR_SCATTER,
    )
    ax.set_title("AM speed asymmetry ratio vs congestion")
    ax.set_ylabel("City median SAR")
    ax.set_xlabel("Baidu 2025 commute-peak congestion index")
    ax.spines[["top", "right"]].set_visible(False)
    finalize_sar_figure(fig)
    fig.savefig(out_path, dpi=dpi, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return stat


def export_preslide_national_figures(
    city_summary: pd.DataFrame,
    pair_summary: pd.DataFrame,
    out_dir: Path,
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    plot_preslide_national_am_histogram(
        pair_summary,
        out_dir / "pat_national_ratio_hist.png",
        dpi=300,
        include_definition=False,
    )
    plot_preslide_national_am_scatter(
        city_summary,
        pair_summary,
        out_dir / "pat_congestion_am_scatter.png",
        dpi=300,
    )


def write_markdown_report(
    out_md: Path,
    *,
    version_root: Path,
    session_ids: tuple[str, ...],
    pair_summary: pd.DataFrame,
    city_summary: pd.DataFrame,
    n_fail: int,
) -> None:
    bal = pair_summary[pair_summary["balanced_sample_flag"]]
    n_bal = int(len(bal))
    n_pair = int(len(pair_summary))
    med_am = float(np.nanmedian(bal["am_minmax_ratio"]))
    med_pm = float(np.nanmedian(bal["pm_minmax_ratio"]))
    rev_mean = float(np.nanmean(bal["reversal_flag"])) if n_bal else float("nan")

    lines = [
        "# 百城通勤潮汐样本：三轮爬取（FREE / AM / PM）统计摘要",
        "",
        "## 数据与口径",
        "",
        f"- **version 根目录**：`{version_root}`",
        f"- **纳入的 crawl_run_id**（按 FREE → AM → PM 逻辑顺序列出）：`{', '.join(session_ids)}`",
        "- **OD 样本**：与 `data/top_commuting_pairs.csv` 一致（百城 × 每城最多 50 对 + 长距离补充，共约 7461 条无向对）。",
        "- **接口**：`directionlite/v1/driving`；FREE 使用 `tactics=0`，AM/PM 使用 `tactics=2`（躲避拥堵）。",
        "- **去重规则**：同一 `(city_id, pair_id, query_window, origin_type, dest_type)` 若有多条，取 `query_time_local` 最晚且 `api_status=0` 的一条；若无成功记录则保留最后一条（通常用于暴露失败）。",
        "",
        "## 覆盖与质量",
        "",
        f"- **pair 级行数**：{n_pair}",
        f"- **balanced_sample_flag=True**（AM/PM/FREE 六向时长均 >0）：**{n_bal}**",
        f"- **API 失败条数（在纳入的 flat 中 api_status≠0）**：{n_fail}",
        "",
        "## 全样本描述性指标（balanced 子集）",
        "",
        f"- **AM 方向不对称 min/max 时间比 中位数**（越低越不对称）：{med_am:.4f}",
        f"- **PM 方向不对称 min/max 时间比 中位数**：{med_pm:.4f}",
        f"- **AM vs PM 优势方向反转比例（reversal_flag 简单均值）**：{rev_mean:.3f}",
        "",
        "城市级加权指标见 `city_summary.csv`；pair 级见 `pair_summary.csv`。",
        "",
        "## 输出文件",
        "",
        "| 文件 | 说明 |",
        "|------|------|",
        "| `pair_summary.csv` | 与 `summarize_results` 同构的 pair 汇总 |",
        "| `city_summary.csv` | 分城加权不对称与反转等 |",
        "| `session_manifest.json` | 三轮 run 清单与路径 |",
        "| `api_failures_in_campaign.csv` | 若存在失败请求，逐行列出 |",
        "| `figure_*.png` | 图表（见下节） |",
        "",
        "## 图表导读（与 `--report-id` 同名的 figures 子目录）",
        "",
        "- **figure1 / figure2**：分城加权中位数 **min(两向时长)/max(两向时长)**（0–1，越低越不对称），分别对应 **AM** 与 **PM**。",
        "- **figure3**：balanced 子样本下 **AM 与 PM min/max 时间比** 的直方图叠画。",
        "- **figure4**：各城 **AM 与 PM 优势方向是否相反**（`reversal_flag`）的加权占比。",
        "- **figure5**：pair 在城市内 **排名** 与 **AM min/max 时间比**，点面积 ∝ **√pair 权重**。",
        "- **figure6**：通勤量最高的若干城中 **排名前 5 pair** 的 AM/PM 四向时长（分钟）。",
        "- **figure7**：**FREE / AM / PM** 下 **home→work** 时长（分钟）的密度叠画。",
        "- **figure8**：**AM min/max 时间比** 对 **PM min/max 时间比** 的散点；对角线表示两峰不对称程度一致。",
        "- **figure9**：高峰相对 FREE 的时长倍数分布（显示端 0.5–3 裁剪）。",
        "- **figure10**：拥堵指数 vs AM/PM 城市不对称散点（英文轴标签；城市名见 CSV）。",
        "",
        "All figure axis titles and legends use **English only**; city bar charts use `city_id` on the x-axis.",
        "",
        "## 复现",
        "",
        "```bash",
        "cd data_work",
        "python3.11 -m src.national_tidal_commuting.report_triple_crawl_campaign \\",
        "  --config config/national_tidal_commuting_baidu2025_top100_congestion_v1.json \\",
        "  --report-id triple_crawl_20260514_15 \\",
        "  --session-ids 20260515T020001,20260515T070000,20260514T173000",
        "```",
        "",
        "---",
        "",
        "*由 `report_triple_crawl_campaign.py` 自动生成。*",
        "",
    ]
    out_md.write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Triple FREE/AM/PM crawl campaign tables + figures + MD.")
    p.add_argument("--config", required=True)
    p.add_argument("--version-id", default=None)
    p.add_argument("--output-dir", default=None)
    p.add_argument(
        "--report-id",
        default="triple_crawl_campaign_report",
        help="metrics/figures 子目录名（位于 version_root 下）",
    )
    p.add_argument(
        "--session-ids",
        default=",".join(DEFAULT_SESSION_IDS),
        help="逗号分隔 crawl_run_id，顺序不限；默认 20260515 FREE/AM + 20260514 PM",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    version_root = get_version_root(config, args.version_id, args.output_dir)
    dirs = ensure_version_dirs(version_root)
    ensure_mpl_cache(str(version_root.parent))

    session_ids = tuple(s.strip() for s in args.session_ids.split(",") if s.strip())
    api_dir = dirs["api"]
    results = load_selected_sessions(api_dir, session_ids)

    # failures in raw concat (before dedup)
    raw_parts = []
    for sid in session_ids:
        p = api_dir / "crawl_sessions" / sid / "session_matrix_flat.csv"
        raw_parts.append(pd.read_csv(p))
    raw_all = pd.concat(raw_parts, ignore_index=True)
    fails = raw_all[raw_all["api_status"] != 0].copy()

    top_pairs_path = dirs["data"] / "top_commuting_pairs.csv"
    if not top_pairs_path.exists():
        raise FileNotFoundError(f"Missing {top_pairs_path}")
    pairs = pd.read_csv(top_pairs_path)

    pair_summary = build_pair_summary(pairs, results)
    city_summary = summarize_city(pair_summary)

    report_root = dirs["metrics"] / args.report_id
    fig_root = dirs["figures"] / args.report_id
    report_root.mkdir(parents=True, exist_ok=True)
    fig_root.mkdir(parents=True, exist_ok=True)

    pair_summary.to_csv(report_root / "pair_summary.csv", index=False)
    city_summary.to_csv(report_root / "city_summary.csv", index=False)
    fails.to_csv(report_root / "api_failures_in_campaign.csv", index=False)

    manifests = {}
    for sid in session_ids:
        mp = api_dir / "crawl_sessions" / sid / "run_manifest.json"
        if mp.exists():
            manifests[sid] = json.loads(mp.read_text(encoding="utf-8"))
    (report_root / "session_manifest.json").write_text(
        json.dumps(
            {
                "session_ids": list(session_ids),
                "manifests": manifests,
                "pair_summary_csv": str((report_root / "pair_summary.csv").resolve()),
                "city_summary_csv": str((report_root / "city_summary.csv").resolve()),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    plot_city_bar_en(
        city_summary,
        "weighted_am_minmax_ratio_median",
        "AM directional asymmetry by city (weighted median min/max ratio)",
        fig_root / "figure1_am_asymmetry_by_city.png",
    )
    plot_city_bar_en(
        city_summary,
        "weighted_pm_minmax_ratio_median",
        "PM directional asymmetry by city (weighted median min/max ratio)",
        fig_root / "figure2_pm_asymmetry_by_city.png",
    )
    plot_hist(pair_summary, fig_root / "figure3_pooled_directional_ratio_hist.png")
    plot_reversal_en(city_summary, fig_root / "figure4_reversal_share_by_city.png")
    plot_rank_vs_asym(pair_summary, fig_root / "figure5_rank_vs_am_asymmetry.png")
    plot_representative_cities_en(pair_summary, fig_root / "figure6_representative_city_top_pairs.png")
    plot_duration_triple_overlay(pair_summary, fig_root / "figure7_free_am_pm_home_work_duration_density.png")
    plot_am_vs_pm_ratio_scatter(pair_summary, fig_root / "figure8_am_vs_pm_directional_ratio_scatter.png")
    plot_peak_penalty_hist(pair_summary, fig_root / "figure9_peak_over_free_ratio_hist.png")
    plot_congestion_vs_asymmetry(
        city_summary,
        pair_summary,
        fig_root / "figure10_congestion_index_vs_city_asymmetry.png",
        report_root / "city_congestion_vs_asymmetry.csv",
        report_root / "congestion_asymmetry_correlation.csv",
    )

    write_markdown_report(
        report_root / "TRIPLE_CRAWL_REPORT.md",
        version_root=version_root.resolve(),
        session_ids=session_ids,
        pair_summary=pair_summary,
        city_summary=city_summary,
        n_fail=int(len(fails)),
    )

    print(f"[report_triple_crawl_campaign] wrote {report_root}")
    print(f"[report_triple_crawl_campaign] wrote {fig_root}")


if __name__ == "__main__":
    main()
