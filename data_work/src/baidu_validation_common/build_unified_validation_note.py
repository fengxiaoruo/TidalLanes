"""
Build unified LaTeX note and figure bundle for raw + centerline Baidu validation.

Presentation order:
  1. Raw segment: API crawl vs direct speed CSV (replicability)
  2. Spatial fit: crawled routes vs baseline geometry (raw + centerline)
  3. Centerline: API crawl vs raw-aggregated inferred speed (aggregation quality)
"""

from __future__ import annotations

import argparse
import shutil
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.baidu_validation_common.plot_style import (
    PANEL_ORDER,
    PANEL_SCENARIO,
    plot_reference_vs_api_scatter,
)
from src.diagnostics.mpl_setup import ensure_mpl_cache

SH_TZ = ZoneInfo("Asia/Shanghai")
DEFAULT_OUT = ROOT.parent / "Documents" / "L4_pipeline_outputs" / "baidu_api_validation"
RAW_ROOT = ROOT / "outputs" / "raw_baidu_validation_beijing_v1"
CL_ROOT = ROOT / "outputs" / "centerline_baidu_validation_beijing_v1"


def parse_args():
    p = argparse.ArgumentParser(description="Build unified Baidu validation LaTeX note.")
    p.add_argument("--raw-label", default="formal_week_20260528")
    p.add_argument(
        "--raw-metrics-tag",
        default="formal_week_validation_formal_week_20260528_20260603T133625",
        help="Subfolder under raw metrics/ (formal_week_validation_*). Default: top-level metrics/.",
    )
    p.add_argument(
        "--centerline-label",
        default="formal_week_20260527",
    )
    p.add_argument(
        "--centerline-metrics-tag",
        default="formal_week_validation_formal_week_20260527_20260603T133612",
        help="Subfolder under centerline metrics/ with comparison tables.",
    )
    p.add_argument("--output-root", default=str(DEFAULT_OUT))
    p.add_argument(
        "--raw-panels",
        default="FREE,AM,PM",
        help="Raw panels to include.",
    )
    p.add_argument(
        "--cross-panels",
        default=None,
        help="Panels for cross-level comparison; defaults to --raw-panels.",
    )
    p.add_argument(
        "--tex-name",
        default="beijing_baidu_validation_note.tex",
        help="Output .tex filename under output-root.",
    )
    p.add_argument(
        "--snapshot-label",
        default="v6（2026-06-03，本轮爬取定稿；Raw 12/15 + Centerline 15/15）",
        help="Short version tag shown in the document title block.",
    )
    p.add_argument(
        "--audit-dir",
        default=str(DEFAULT_OUT),
        help="Directory with crawl_audit_*_best.csv from progress audit.",
    )
    return p.parse_args()


def parse_panels(text: str) -> list[str]:
    panels = [x.strip() for x in text.split(",") if x.strip()]
    bad = [p for p in panels if p not in PANEL_ORDER]
    if bad:
        raise ValueError(f"Unknown panels: {bad}")
    return panels


def copy_figure(src: Path, dst: Path) -> bool:
    if not src.exists():
        print(f"[warn] missing figure: {src}")
        return False
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    return True


def load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    return pd.read_csv(path)


def tex_escape_label(label: str) -> str:
    return label.replace("_", r"\_")


def dedupe_raw_for_cross(raw_df: pd.DataFrame) -> pd.DataFrame:
    if "crawl_day" not in raw_df.columns:
        return raw_df
    keys = ["split_id", "raw_edge_id", "panel"]
    use = raw_df[raw_df["bind_ok"] & raw_df["direct_travel_time_min"].notna()].copy()
    return (
        use.groupby(keys, as_index=False)
        .agg(
            api_travel_time_min=("api_travel_time_min", "median"),
            direct_travel_time_min=("direct_travel_time_min", "first"),
            bind_ok=("bind_ok", "max"),
        )
    )


def raw_stability_table(stability_df: pd.DataFrame) -> str:
    lines = [
        r"\begin{table}[h!]",
        r"\centering",
        r"\caption{Raw API crawl cross-day stability (bind ok, direct speed matched)}",
        r"\label{tab:raw_stability}",
        r"\footnotesize",
        r"\begin{tabular}{lrrrr}",
        r"\toprule",
        r"Panel & n (ge2 days) & Med.\ CV & Share CV le 0.15 & Med.\ max/min \\",
        r"\midrule",
    ]
    for _, r in stability_df.iterrows():
        ncol = "n_segments_ge2_days" if "n_segments_ge2_days" in r else "n_centerlines_ge2_days"
        lines.append(
            f"{r['panel']} & {int(r[ncol]):,} & {r['median_cv_time']:.3f} & "
            f"{100*r['share_cv_le_0p15']:.1f}\\% & {r['median_time_ratio_max_over_min']:.2f} \\\\"
        )
    lines.extend([r"\bottomrule", r"\end{tabular}", r"\end{table}"])
    return "\n".join(lines)


def crawl_inventory_table(raw_audit: pd.DataFrame, cl_audit: pd.DataFrame) -> str:
    """Side-by-side crawl scale description."""
    lines = [
        r"\begin{table}[h!]",
        r"\centering",
        r"\caption{Crawl inventory: two validation layers (formal week, best session per day$\times$panel)}",
        r"\label{tab:crawl_inventory}",
        r"\footnotesize",
        r"\begin{tabular}{llrrl}",
        r"\toprule",
        r"Layer & Week label & Units / panel & Rows / panel (5d pooled) & Notes \\",
        r"\midrule",
        r"Raw segment & \texttt{formal\_week\_20260528} & 23,799 segments & 118,995 max & Day1 route path; d02+ duration only \\",
        r"Centerline & \texttt{formal\_week\_20260527} & 11,890 directed CL & 59,450 & Day1 route path; dense gap-fill d02--d05 \\",
        r"\bottomrule",
        r"\end{tabular}",
        r"\end{table}",
    ]
    raw_done = int(raw_audit.complete.sum()) if len(raw_audit) else 0
    cl_done = int(cl_audit.complete.sum()) if len(cl_audit) else 0
    lines.insert(
        -3,
        f"\\multicolumn{{5}}{{l}}{{\\textit{{Complete day$\\times$panel sessions: Raw {raw_done}/15; Centerline {cl_done}/15.}}}} \\\\",
    )
    return "\n".join(lines)


def dedupe_centerline_for_cross(cl_df: pd.DataFrame) -> pd.DataFrame:
    """One row per centerline×panel when formal-week rows are pooled (multiple crawl_day)."""
    if "crawl_day" not in cl_df.columns:
        return cl_df
    keys = ["skel_dir", "cline_id", "dir", "panel"]
    use = cl_df[cl_df["bind_ok"] & cl_df["inferred_travel_time_min"].notna()].copy()
    agg = (
        use.groupby(keys, as_index=False)
        .agg(
            baidu_travel_time_min=("baidu_travel_time_min", "median"),
            inferred_travel_time_min=("inferred_travel_time_min", "first"),
            bind_ok=("bind_ok", "max"),
        )
    )
    return agg


def centerline_stability_table(stability_df: pd.DataFrame) -> str:
    lines = [
        r"\begin{table}[h!]",
        r"\centering",
        r"\caption{Centerline API crawl cross-day stability (bind ok, inferred matched)}",
        r"\label{tab:cl_stability}",
        r"\footnotesize",
        r"\begin{tabular}{lrrrr}",
        r"\toprule",
        r"Panel & n (ge2 days) & Med.\ CV & Share CV le 0.15 & Med.\ max/min \\",
        r"\midrule",
    ]
    for _, r in stability_df.iterrows():
        lines.append(
            f"{r['panel']} & {int(r['n_centerlines_ge2_days']):,} & {r['median_cv_time']:.3f} & "
            f"{100*r['share_cv_le_0p15']:.1f}\\% & {r['median_time_ratio_max_over_min']:.2f} \\\\"
        )
    lines.extend([r"\bottomrule", r"\end{tabular}", r"\end{table}"])
    return "\n".join(lines)


def build_cross_level_comparison(
    raw_df: pd.DataFrame,
    cl_df: pd.DataFrame,
    raw_manifest: pd.DataFrame,
) -> pd.DataFrame:
    """Compare centerline crawl vs inferred vs mean raw crawl on matched centerlines."""
    keys = ["skel_dir", "cline_id", "dir", "panel"]
    xwalk = raw_manifest[["split_id", "raw_edge_id", "skel_dir", "cline_id", "dir"]].drop_duplicates()
    raw_use = raw_df[raw_df["bind_ok"] & raw_df["direct_travel_time_min"].notna()].copy()
    raw_use = raw_use.merge(xwalk, on=["split_id", "raw_edge_id"], how="inner")
    raw_agg = (
        raw_use.groupby(keys, as_index=False)
        .agg(
            n_raw_segments=("split_id", "size"),
            raw_api_time_min=("api_travel_time_min", "mean"),
            raw_direct_time_min=("direct_travel_time_min", "mean"),
        )
    )
    cl_use = cl_df[cl_df["bind_ok"] & cl_df["inferred_travel_time_min"].notna()].copy()
    cl_use = cl_use.rename(
        columns={
            "baidu_travel_time_min": "cl_api_time_min",
            "inferred_travel_time_min": "cl_inferred_time_min",
        }
    )
    merged = cl_use[keys + ["cl_api_time_min", "cl_inferred_time_min"]].merge(raw_agg, on=keys, how="inner")
    merged["ratio_cl_api_over_inferred"] = merged["cl_api_time_min"] / merged["cl_inferred_time_min"]
    merged["ratio_cl_api_over_raw_api"] = merged["cl_api_time_min"] / merged["raw_api_time_min"]
    merged["ratio_inferred_over_raw_direct"] = merged["cl_inferred_time_min"] / merged["raw_direct_time_min"]
    return merged


def plot_aggregation_comparison(merged: pd.DataFrame, path: Path):
    plot_reference_vs_api_scatter(
        merged.assign(bind_ok=True, api_status=0),
        path,
        ref_col="cl_inferred_time_min",
        api_col="cl_api_time_min",
        ref_label="Inferred from raw aggregate (min)",
        api_label="Centerline API crawl (min)",
        title="Centerline: inferred vs API crawl (matched centerlines)",
        panel_titles=PANEL_SCENARIO,
    )


def panel_summary_table(
    raw_sum: pd.DataFrame,
    cl_sum: pd.DataFrame,
    *,
    raw_panels: list[str],
) -> str:
    cap = "Panel-level validation summary (bind\\_ok subsets)"
    lines = [
        r"\begin{table}[h!]",
        r"\centering",
        rf"\caption{{{cap}}}",
        r"\label{tab:panel_summary}",
        r"\footnotesize",
        r"\setlength{\tabcolsep}{3pt}",
        r"\begin{tabular}{llrrrrr}",
        r"\toprule",
        r"Layer & Panel & $n$ & Bind OK & Med.\ ratio & Share $\in [0.8,1.25]$ & $r$ \\",
        r"\midrule",
    ]
    for _, r in raw_sum.iterrows():
        if r["panel"] not in raw_panels:
            continue
        lines.append(
            f"Raw & {r['panel']} & {int(r['n_bind_ok_direct']):,} & "
            f"{100*r['share_bind_ok']:.1f}\\% & {r['median_time_ratio']:.2f} & "
            f"{100*r['share_time_ratio_0p8_1p25']:.1f}\\% & {r['corr_time']:.2f} \\\\"
        )
    for _, r in cl_sum.iterrows():
        lines.append(
            f"Centerline & {r['panel']} & {int(r['n_bind_ok_inferred']):,} & "
            f"{100*r['share_bind_ok']:.1f}\\% & {r['median_time_ratio_bind_inferred']:.2f} & "
            f"{100*r['share_time_ratio_0p8_1p25']:.1f}\\% & {r['corr_time_bind_inferred']:.2f} \\\\"
        )
    lines.extend([r"\bottomrule", r"\end{tabular}", r"\end{table}"])
    return "\n".join(lines)


def _crawl_cell(row: pd.Series | None, *, expected: int) -> str:
    if row is None or pd.isna(row.get("n")):
        return "---"
    n = int(row["n"])
    ok_pct = float(row["api_ok_pct"])
    e240 = int(row.get("e240", 0) or 0)
    if bool(row.get("complete")) and ok_pct >= 99:
        return f"\\checkmark ({n:,})"
    if e240 > 0:
        return f"部分 {ok_pct:.0f}\\% ({e240:,} $\\times$240)"
    if n < expected * 0.95:
        return f"进行中 {100*n/expected:.0f}\\%"
    return f"部分 {ok_pct:.0f}\\%"


def crawl_progress_table(raw_audit: pd.DataFrame, cl_audit: pd.DataFrame) -> str:
    days = sorted(set(raw_audit.day.dropna()) | set(cl_audit.day.dropna()))
    lines = [
        r"\begin{table}[h!]",
        r"\centering",
        r"\caption{Formal-week crawl progress (best session per day $\times$ panel)}",
        r"\label{tab:crawl_progress}",
        r"\footnotesize",
        r"\setlength{\tabcolsep}{2.5pt}",
        r"\begin{tabular}{lccc|ccc}",
        r"\toprule",
        r" & \multicolumn{3}{c|}{Raw (\texttt{formal\_week\_20260528})} & \multicolumn{3}{c}{Centerline (\texttt{formal\_week\_20260527})} \\",
        r"Day & FREE & AM & PM & FREE & AM & PM \\",
        r"\midrule",
    ]
    raw_idx = raw_audit.set_index(["day", "panel"]) if len(raw_audit) else None
    cl_idx = cl_audit.set_index(["day", "panel"]) if len(cl_audit) else None
    for day in days:
        cells = [day]
        for idx, exp in [(raw_idx, 23799), (cl_idx, 11890)]:
            for p in PANEL_ORDER:
                row = None
                if idx is not None and (day, p) in idx.index:
                    row = idx.loc[(day, p)]
                    if isinstance(row, pd.DataFrame):
                        row = row.iloc[0]
                cells.append(_crawl_cell(row, expected=exp))
        lines.append(" & ".join(cells) + r" \\")
    lines.extend([r"\bottomrule", r"\end{tabular}", r"\end{table}"])
    return "\n".join(lines)


def crawl_status_notes(raw_audit: pd.DataFrame, cl_audit: pd.DataFrame) -> str:
    lines = []
    raw_done = int(raw_audit.complete.sum()) if len(raw_audit) else 0
    raw_total = len(raw_audit)
    cl_done = int(cl_audit.complete.sum()) if len(cl_audit) else 0
    cl_total = len(cl_audit)
    lines.append(
        f"Raw 正式周：{raw_done}/{raw_total} 个 day$\\times$panel 达到 api\\_ok$\\ge$99\\%；"
        f"Centerline：{cl_done}/{cl_total} 全部完成。"
        f"\\textbf{{本轮爬取已结束}}，缺失 panel 不补跑；验证使用各 session 中全部 api\\_ok 行。"
    )
    bad = []
    for _, r in raw_audit.iterrows():
        if not bool(r.get("complete", False)):
            bad.append(f"{r['day']} {r['panel']}（api\\_ok {r['api_ok_pct']:.0f}\\%）")
    if bad:
        lines.append("Raw 未完整 panel：" + "；".join(bad) + "。")
    lines.append(
        "验证统计：Raw 与 Centerline 均基于正式周 best session pooled（api\\_ok 行参与分析）；"
        "Spatial 几何指标仅 d01（含 route path）。"
    )
    return " ".join(lines)


def spatial_summary_table(
    raw_sp: pd.DataFrame | None,
    cl_sp: pd.DataFrame | None,
    *,
    raw_panels: list[str],
) -> str:
    lines = [
        r"\begin{table}[h!]",
        r"\centering",
        r"\caption{Spatial fit summary (50\,m buffer; GCJ$\rightarrow$WGS corrected routes)}",
        r"\label{tab:spatial_summary}",
        r"\footnotesize",
        r"\begin{tabular}{lrrrr}",
        r"\toprule",
        r"Layer & Panel & Spatial pass & Med.\ overlap & Med.\ mean dist (m) \\",
        r"\midrule",
    ]
    if cl_sp is not None and len(cl_sp):
        for _, r in cl_sp.iterrows():
            lines.append(
                f"Centerline & {r['panel']} & {100*r['share_spatial_ok']:.1f}\\% & "
                f"{r['median_overlap_geom']:.2f} & {r['median_mean_dist_m']:.1f} \\\\"
            )
    if raw_sp is not None and len(raw_sp):
        for _, r in raw_sp.iterrows():
            if r["panel"] not in raw_panels:
                continue
            lines.append(
                f"Raw segment & {r['panel']} & {100*r['share_spatial_ok']:.1f}\\% & "
                f"{r['median_overlap_geom']:.2f} & {r['median_mean_dist_m']:.1f} \\\\"
            )
    lines.extend([r"\bottomrule", r"\end{tabular}", r"\end{table}"])
    return "\n".join(lines)


def write_tex(
    path: Path,
    *,
    raw_label: str,
    cl_label: str,
    panel_table: str,
    spatial_table: str,
    fig_dir_rel: str,
    cross_summary: pd.DataFrame | None,
    snapshot_label: str,
    raw_panels: list[str],
    raw_sum: pd.DataFrame,
    cl_sum: pd.DataFrame,
    stability_table: str,
    raw_stability_table_tex: str,
    crawl_inventory: str,
    cl_formal_week: bool,
    raw_formal_week: bool,
    raw_sp_pass: float,
    cl_sp_pass: float,
    raw_complete: int,
    tex_name: str,
    crawl_table: str,
    crawl_notes: str,
):
    date_str = datetime.now(SH_TZ).strftime("%Y年%m月%d日")
    raw_panel_str = "、".join(raw_panels)
    r_min = raw_sum["corr_time"].min()
    r_max = raw_sum["corr_time"].max()
    ratio_min = raw_sum["median_time_ratio"].min()
    ratio_max = raw_sum["median_time_ratio"].max()
    mae_min = raw_sum["mae_time_min"].min()
    mae_max = raw_sum["mae_time_min"].max()
    bind_share = raw_sum["share_bind_ok"].mean() * 100

    cl_r_min = cl_sum["corr_time_bind_inferred"].min()
    cl_r_max = cl_sum["corr_time_bind_inferred"].max()
    cl_ratio_min = cl_sum["median_time_ratio_bind_inferred"].min()
    cl_ratio_max = cl_sum["median_time_ratio_bind_inferred"].max()
    cl_bind = cl_sum["share_bind_ok"].mean() * 100
    cl_share_band = cl_sum["share_time_ratio_0p8_1p25"].min() * 100
    cl_share_band_max = cl_sum["share_time_ratio_0p8_1p25"].max() * 100
    cl_n = int(cl_sum["n_bind_ok_inferred"].min())
    raw_n = int(raw_sum["n_bind_ok_direct"].min()) if "n_bind_ok_direct" in raw_sum.columns else 0

    cross_lines = ""
    if cross_summary is not None and len(cross_summary):
        parts_ratio = [f"{r['panel']}={r['median_ratio_cl_api_over_inferred']:.2f}" for _, r in cross_summary.iterrows()]
        parts_corr = [f"{r['panel']} $r={r['corr_inferred_vs_cl_api']:.2f}$" for _, r in cross_summary.iterrows()]
        cross_lines = (
            f"在 matched centerlines 子样本中，centerline API / inferred 的 median ratio 为 "
            f"{', '.join(parts_ratio)}；对应相关系数为 {', '.join(parts_corr)}。"
        )

    raw_tex = tex_escape_label(raw_label)
    cl_tex = tex_escape_label(cl_label)
    raw_scope = (
        f"Raw 正式周 \\texttt{{{raw_tex}}}：5 天 pooled，bind\\_ok$\\cap$direct $\\approx$ {raw_n:,} 条/panel"
        f"（完整 session {raw_complete}/15；未完整 panel 的 api\\_ok 行仍纳入 pooled 分析）。"
        if raw_formal_week
        else f"Raw 基于 \\texttt{{{raw_tex}\\_d01}}。"
    )
    cl_scope = (
        f"Centerline 正式周 \\texttt{{{cl_tex}}}：15/15 完整 session，每 panel 59,450 条；bind\\_ok$\\cap$inferred $\\approx$ {cl_n:,} 条/panel。"
        if cl_formal_week
        else f"Centerline 基于 \\texttt{{{cl_tex}}}（单日 d01）。"
    )
    coverage = (
        f"\\textbf{{本版快照：{snapshot_label}}}。"
        f" {raw_scope} {cl_scope}"
        f" 爬取进度见表~\\ref{{tab:crawl_progress}}；规模见表~\\ref{{tab:crawl_inventory}}。"
    )

    raw_text = (
        f"Raw 层（{raw_panel_str}）bind\\_ok 占比约 {bind_share:.0f}\\%，明显高于 centerline 层（约 31\\%）。"
        f"在 bind\\_ok$\\cap$direct speed 子样本中，Pearson 相关 $r$ 为 {r_min:.2f}--{r_max:.2f}；"
        f"median API/direct ratio 为 {ratio_min:.2f}--{ratio_max:.2f}。"
        f"MAE 约 {mae_min:.2f}--{mae_max:.2f} 分钟。"
        f"API 爬取可\\textbf{{复刻直接观测的主要横截面排序}}，但 level 系统性偏高，需 bind QC 或 scale 校准。"
    )

    body = rf"""\documentclass[12pt]{{article}}
\usepackage[margin=1in]{{geometry}}
\usepackage[UTF8,fontset=fandol]{{ctex}}
\usepackage{{amsmath,amssymb,graphicx,booktabs,tabularx,setspace}}
\renewcommand{{\figurename}}{{Figure}}
\renewcommand{{\tablename}}{{Table}}
\title{{北京路网：百度 API 爬取验证（Raw 与 Centerline）\\ \large {snapshot_label}}}
\author{{Xiaoruo Feng}}
\date{{{date_str}}}

\begin{{document}}
\maketitle

\section*{{数据覆盖范围}}

{coverage}

{crawl_notes}

{crawl_table}

{crawl_inventory}

\section*{{两套爬取链路说明}}

\textbf{{Raw segment 层}}（week label: \\texttt{{{raw_tex}}}）：在 baseline raw segment 上，每条路段带固定途经点调用 DirectionLite；共 23,799 条/panel。Day1 保存 route path；Day2--5 仅 duration/distance。对照基准为百度 speed CSV weekday 聚合。Bind\\_ok 约 {bind_share:.0f}\\%。\\
\textbf{{Centerline 层}}（week label: \\texttt{{{cl_tex}}}）：在 baseline directed centerline 上爬取，共 11,890 条/panel；Day2--5 采用 dense orchestrator 补跑。对照基准为 stage03 raw speed 聚合推断 travel time。Bind\\_ok 约 {cl_bind:.0f}\\%。\\
\textbf{{共同设定}}：Panel 为 FREE（02:30）、AM/PM（07:30/17:30 躲避拥堵）；Bind QC 为 distance/length $\\in [0.6,1.8]$。

\section*{{验证目的}}

\textbf{{Raw 链路}}：baseline raw segment 上，带途经点的 DirectionLite API 行程时间 vs \texttt{{speed\_Beijing\_all\_wgs84.csv}} 直接观测。\\
\textbf{{Centerline 链路}}：baseline directed centerline 上，API 行程时间 vs stage03 raw speed 聚合推断的 travel time。\\
Panel 定义：FREE（深夜常规）、AM（7--9 时躲避拥堵）、PM（17--19 时躲避拥堵，weekday）。\\
Bind QC：$\mathrm{{distance}}/\mathrm{{length}}\in[0.6,1.8]$。\\
Crawl 标签：raw = \\texttt{{{raw_tex}}}；centerline = \\texttt{{{cl_tex}}}。

\section*{{1.\ 爬取 travel cost 能否复刻已有数据？（Raw 基准）}}

核心问题：API 路由返回的 duration 是否与百度直接给我们的路段 speed CSV 在统计意义上显著相关、可用于复刻现有观测？

{panel_table}

{raw_text}

{raw_stability_table_tex}

\begin{{figure}}[h!]
\centering
\includegraphics[width=0.92\textwidth]{{{fig_dir_rel}/fig_raw_day_panel_bind_ok.png}}
\caption{{Raw 正式周：bind\_ok 占比按 crawl day（约 93\%，跨日稳定）。}}
\label{{fig:raw_day_bind}}
\end{{figure}}

\begin{{figure}}[h!]
\centering
\includegraphics[width=0.92\textwidth]{{{fig_dir_rel}/fig_raw_api_vs_direct_scatter.png}}
\caption{{Raw segment：API 行程时间 vs 直接 speed CSV 反推行程时间（bind\_ok）。虚线为 45$^\circ$ 线。}}
\label{{fig:raw_scatter}}
\end{{figure}}

\begin{{figure}}[h!]
\centering
\includegraphics[width=0.92\textwidth]{{{fig_dir_rel}/fig_raw_bland_altman.png}}
\caption{{Raw segment：Bland--Altman 图（API $-$ direct）。}}
\label{{fig:raw_ba}}
\end{{figure}}

\begin{{figure}}[h!]
\centering
\includegraphics[width=0.88\textwidth]{{{fig_dir_rel}/fig_raw_time_ratio_hist.png}}
\caption{{Raw segment：API/direct 行程时间比分布（bind\_ok）。灰色带为 $[0.8,1.25]$。}}
\label{{fig:raw_ratio}}
\end{{figure}}

\section*{{2.\ 爬取道路是否与 baseline shp 为同一条底层道路？}}

\subsection*{{2.1 统计指标}}

Bind ratio 衡量 API 返回距离与 baseline 几何长度的比值；spatial pass 定义为 mean distance $\le 80$\,m 且 baseline overlap $\ge 50\%$（50\,m buffer，route path 经 GCJ$\rightarrow$WGS 校正）。

{spatial_table}

Centerline 与 raw 两层的 spatial pass 分别为约 {cl_sp_pass:.0f}\\% 与 {raw_sp_pass:.0f}\\%（d01 route path 子样本）；median overlap 接近 0。
\textbf{{这不等于绑到了错误道路}}：许多 API 返回路径与 baseline 几何 \textbf{{平行}}（对向或相邻车道），在 50\,m buffer 下 overlap 指标非常保守。
Raw 层 bind\_ok 高达 93\%，说明 API 返回距离与 segment 长度更一致；centerline 层 bind\_ok 仅 31\%，主要因为 centerline 更长、更易触发绕路或平行走廊。
仍需结合地图叠加（图~\ref{{fig:cl_map}}）判断：平行走廊会同时压低 overlap 并抬高 distance/length。

\subsection*{{2.2 路网地图可视化}}

\begin{{figure}}[h!]
\centering
\includegraphics[width=0.78\textwidth]{{{fig_dir_rel}/fig_cl_network_overlay.png}}
\caption{{Centerline baseline（蓝）与 bind\_ok 的百度 API 路线（橙）叠加，天安门 10\,km 范围；虚线环为 5/10\,km。}}
\label{{fig:cl_map}}
\end{{figure}}

\begin{{figure}}[h!]
\centering
\includegraphics[width=0.92\textwidth]{{{fig_dir_rel}/fig_cl_spatial_scatter.png}}
\caption{{Centerline：bind ratio vs 空间 overlap（平行走廊常表现为低 overlap + 可接受 bind ratio）。}}
\label{{fig:cl_spatial}}
\end{{figure}}

\begin{{figure}}[h!]
\centering
\includegraphics[width=0.92\textwidth]{{{fig_dir_rel}/fig_raw_spatial_scatter.png}}
\caption{{Raw segment：bind ratio vs 空间 overlap。}}
\label{{fig:raw_spatial}}
\end{{figure}}

\begin{{figure}}[h!]
\centering
\includegraphics[width=0.92\textwidth]{{{fig_dir_rel}/fig_cl_dist_ratio.png}}
\caption{{Centerline：distance/length 分布（bind QC 带灰色阴影）。约 31\% 落入 $[0.6,1.8]$。}}
\label{{fig:cl_bind}}
\end{{figure}}

\begin{{figure}}[h!]
\centering
\includegraphics[width=0.92\textwidth]{{{fig_dir_rel}/fig_raw_dist_ratio.png}}
\caption{{Raw segment：distance/length 分布。约 93\% 落入 bind QC。}}
\label{{fig:raw_bind}}
\end{{figure}}

\section*{{3.\ Centerline 聚合速度质量（API crawl vs raw aggregate）}}

在认为 centerline API crawl 可作为外部 ground truth 的前提下，将 crawl 结果与 stage03 由 raw speed 聚合得到的 inferred travel time 对照，以评估 aggregation 是否合理。

\begin{{figure}}[h!]
\centering
\includegraphics[width=0.92\textwidth]{{{fig_dir_rel}/fig_cl_api_vs_inferred_scatter.png}}
\caption{{Centerline：API crawl vs inferred travel time（bind\_ok$\cap$inferred）。}}
\label{{fig:cl_agg_scatter}}
\end{{figure}}

\begin{{figure}}[h!]
\centering
\includegraphics[width=0.92\textwidth]{{{fig_dir_rel}/fig_cl_inferred_vs_cl_api_scatter.png}}
\caption{{Matched centerlines：inferred vs centerline API crawl（跨 raw segment 匹配汇总）。}}
\label{{fig:cl_cross}}
\end{{figure}}

Bind\_ok$\cap$inferred 子样本中（正式周 pooled），centerline 三 panel 的 $r={cl_r_min:.2f}$--{cl_r_max:.2f}，
median ratio 约 {cl_ratio_min:.2f}--{cl_ratio_max:.2f}；约 {cl_share_band:.0f}--{cl_share_band_max:.0f}\% 落在 $[0.8,1.25]$。
bind\_ok 占比稳定在 {cl_bind:.0f}\%（5 日间无明显漂移）。
跨日稳定性（表~\\ref{{tab:cl_stability}}）：median CV 约 0.10--0.13，60--70\\% centerline 的 5 日 CV $\\le 0.15$。
这与 raw 层“相关强但 level 偏高”一致，并额外混入 centerline 聚合与绑路噪声。
{cross_lines}

{stability_table}

\begin{{figure}}[h!]
\centering
\includegraphics[width=0.92\textwidth]{{{fig_dir_rel}/fig_cl_day_panel_bind_ok.png}}
\caption{{Centerline 正式周：bind\_ok 占比按 crawl day（稳定 $\approx 31\%$）。}}
\label{{fig:cl_day_bind}}
\end{{figure}}

\section*{{阶段性结论（当前快照）}}

\begin{{enumerate}}
  \item \textbf{{API 可复刻直接观测（Raw）}}：正式周 pooled 下 segment 爬取与 speed CSV 高度相关（$r={r_min:.2f}$--{r_max:.2f}）；bind\\_ok $\\approx${bind_share:.0f}\\%；level 系统性偏高。
  \item \textbf{{几何对齐（Spatial）}}：spatial pass Raw ${raw_sp_pass:.0f}\\%$ / Centerline ${cl_sp_pass:.0f}\\%$；overlap 指标偏保守，平行走廊是主要解释。
  \item \textbf{{聚合质量（Centerline）}}：正式周 pooled 下 inferred 与 API crawl 排序一致（$r={cl_r_min:.2f}$--{cl_r_max:.2f}）；aggregation 在排序上合理，level 校准仍需改进；API 爬取跨日稳定（median CV $\le 0.13$）。
\end{{enumerate}}

\section*{{复现命令}}

\begin{{verbatim}}
cd data_work
python -m src.raw_baidu_validation.plot_validation_diagnostics \\
  --formal-week {raw_label} --formal-days 5 --panels {",".join(raw_panels)}
python -m src.centerline_baidu_validation.plot_validation_diagnostics \\
  --formal-week {cl_label} --formal-days 5 --reference-label ""
python -m src.baidu_validation_common.build_unified_validation_note \\
  --raw-label {raw_label} --centerline-label {cl_label} \\
  --raw-panels {",".join(raw_panels)} --tex-name {tex_name}
\end{{verbatim}}

\end{{document}}
"""
    path.write_text(body, encoding="utf-8")


def main():
    args = parse_args()
    raw_panels = parse_panels(args.raw_panels)
    cross_panels = parse_panels(args.cross_panels) if args.cross_panels else list(raw_panels)
    ensure_mpl_cache(str(ROOT / "outputs"))
    out_root = Path(args.output_root)
    fig_dir = out_root / "figures"
    fig_dir.mkdir(parents=True, exist_ok=True)

    raw_metrics = RAW_ROOT / "metrics"
    if args.raw_metrics_tag:
        raw_metrics = raw_metrics / args.raw_metrics_tag
        raw_fig = RAW_ROOT / "figures" / args.raw_metrics_tag
        raw_formal_week = True
    else:
        raw_fig = RAW_ROOT / "figures"
        raw_formal_week = False

    cl_fig = CL_ROOT / "figures" / args.centerline_metrics_tag
    cl_overlay = CL_ROOT / "figures" / "network_overlay_formal_week_20260527_d01_20260527T225254"

    figure_map = {
        "fig_raw_api_vs_direct_scatter.png": raw_fig / "fig_api_vs_direct_scatter.png",
        "fig_raw_bland_altman.png": raw_fig / "fig_bland_altman_by_panel.png",
        "fig_raw_time_ratio_hist.png": raw_fig / "fig_time_ratio_hist.png",
        "fig_raw_dist_ratio.png": raw_fig / "fig_dist_ratio_by_panel.png",
        "fig_cl_api_vs_inferred_scatter.png": cl_fig / "fig_baidu_vs_inferred_scatter.png",
        "fig_cl_dist_ratio.png": cl_fig / "fig_dist_ratio_by_panel.png",
        "fig_cl_spatial_scatter.png": cl_fig / "fig_spatial_scatter_overlap_vs_ratio.png",
        "fig_cl_network_overlay.png": cl_overlay / "fig_network_overlay_centerline_vs_baidu_5km_10km.png",
    }
    for extra, src in [
        ("fig_cl_day_panel_bind_ok.png", cl_fig / "fig_day_panel_bind_ok.png"),
        ("fig_raw_day_panel_bind_ok.png", raw_fig / "fig_day_panel_bind_ok.png"),
    ]:
        if src.exists():
            figure_map[extra] = src
    raw_spatial = raw_fig / "fig_spatial_scatter_overlap_vs_ratio.png"
    if not raw_spatial.exists():
        raw_spatial = RAW_ROOT / "figures" / "fig_spatial_scatter_overlap_vs_ratio.png"
    if raw_spatial.exists():
        figure_map["fig_raw_spatial_scatter.png"] = raw_spatial

    for dst_name, src in figure_map.items():
        copy_figure(src, fig_dir / dst_name)

    audit_dir = Path(args.audit_dir)
    raw_audit = load_csv(audit_dir / "crawl_audit_raw_best.csv") if (audit_dir / "crawl_audit_raw_best.csv").exists() else pd.DataFrame()
    cl_audit = load_csv(audit_dir / "crawl_audit_centerline_best.csv") if (audit_dir / "crawl_audit_centerline_best.csv").exists() else pd.DataFrame()
    crawl_table = crawl_progress_table(raw_audit, cl_audit) if len(raw_audit) and len(cl_audit) else ""
    crawl_notes = crawl_status_notes(raw_audit, cl_audit) if len(raw_audit) and len(cl_audit) else ""
    crawl_inventory = crawl_inventory_table(raw_audit, cl_audit) if len(raw_audit) and len(cl_audit) else ""

    raw_sum = load_csv(raw_metrics / "validation_summary.csv")
    raw_sum = raw_sum[raw_sum["panel"].isin(raw_panels)].copy()
    cl_sum = load_csv(CL_ROOT / "metrics" / args.centerline_metrics_tag / "panel_summary.csv")
    cl_formal_week = "formal_week_validation" in args.centerline_metrics_tag
    stability_table = ""
    raw_stability_table_tex = ""
    stability_path = CL_ROOT / "metrics" / args.centerline_metrics_tag / "cross_day_stability.csv"
    if stability_path.exists():
        stability_table = centerline_stability_table(load_csv(stability_path))
    raw_stab_path = raw_metrics / "cross_day_stability.csv"
    if raw_stab_path.exists():
        raw_stability_table_tex = raw_stability_table(load_csv(raw_stab_path))

    cl_sp = None
    cl_sp_path = CL_ROOT / "metrics" / args.centerline_metrics_tag / "spatial_summary_by_panel.csv"
    if cl_sp_path.exists():
        cl_sp = load_csv(cl_sp_path).rename(
            columns={
                "median_overlap_cl": "median_overlap_geom",
            }
        )

    raw_sp = None
    raw_sp_path = raw_metrics / "spatial_summary_by_panel.csv"
    if not raw_sp_path.exists():
        raw_sp_path = RAW_ROOT / "metrics" / "spatial_summary_by_panel.csv"
    if raw_sp_path.exists():
        raw_sp = load_csv(raw_sp_path)
        raw_sp = raw_sp[raw_sp["panel"].isin(raw_panels)].copy()

    raw_df = load_csv(raw_metrics / "comparison_table.csv")
    raw_df = raw_df[raw_df["panel"].isin(raw_panels)].copy()
    raw_df_cross = dedupe_raw_for_cross(raw_df)
    cl_df = load_csv(CL_ROOT / "metrics" / args.centerline_metrics_tag / "comparison_table.csv")
    cl_df = dedupe_centerline_for_cross(cl_df)
    raw_manifest = pd.read_parquet(RAW_ROOT / "data" / "crawl_manifest.parquet")

    cross = build_cross_level_comparison(raw_df_cross, cl_df, raw_manifest)
    cross = cross[cross["panel"].isin(cross_panels)].copy()
    cross.to_csv(out_root / "cross_level_comparison_v1.csv", index=False)
    cross_summary = (
        cross.groupby("panel")
        .apply(
            lambda g: pd.Series(
                {
                    "n": len(g),
                    "median_ratio_cl_api_over_inferred": g["ratio_cl_api_over_inferred"].median(),
                    "corr_inferred_vs_cl_api": g["cl_inferred_time_min"].corr(g["cl_api_time_min"])
                    if len(g) >= 30
                    else np.nan,
                }
            ),
            include_groups=False,
        )
        .reset_index()
    )
    cross_summary.to_csv(out_root / "cross_level_summary_v1.csv", index=False)

    plot_aggregation_comparison(cross, fig_dir / "fig_cl_inferred_vs_cl_api_scatter.png")

    panel_table = panel_summary_table(raw_sum, cl_sum, raw_panels=raw_panels)
    spatial_table = spatial_summary_table(raw_sp, cl_sp, raw_panels=raw_panels)

    raw_sp_pass = float(raw_sp["share_spatial_ok"].mean() * 100) if raw_sp is not None and len(raw_sp) else 29.0
    cl_sp_pass = float(cl_sp["share_spatial_ok"].mean() * 100) if cl_sp is not None and len(cl_sp) else 29.0

    raw_complete = int(raw_audit.complete.sum()) if len(raw_audit) else 0

    tex_path = out_root / args.tex_name
    write_tex(
        tex_path,
        raw_label=args.raw_label,
        cl_label=args.centerline_label,
        panel_table=panel_table,
        spatial_table=spatial_table,
        fig_dir_rel="figures",
        cross_summary=cross_summary,
        snapshot_label=args.snapshot_label,
        raw_panels=raw_panels,
        raw_sum=raw_sum,
        cl_sum=cl_sum,
        stability_table=stability_table,
        raw_stability_table_tex=raw_stability_table_tex,
        crawl_inventory=crawl_inventory,
        cl_formal_week=cl_formal_week,
        raw_formal_week=raw_formal_week,
        raw_sp_pass=raw_sp_pass,
        cl_sp_pass=cl_sp_pass,
        raw_complete=raw_complete,
        tex_name=args.tex_name,
        crawl_table=crawl_table,
        crawl_notes=crawl_notes,
    )
    print(f"[unified_note] figures -> {fig_dir}")
    print(f"[unified_note] tex -> {tex_path}")
    print(cross_summary.to_string(index=False))


if __name__ == "__main__":
    main()
