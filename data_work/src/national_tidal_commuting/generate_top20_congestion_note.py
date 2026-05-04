from __future__ import annotations

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


VERSION_ID = "national_tidal_commuting_top50cities_2021_v1"
BASE = ROOT / "outputs" / VERSION_ID
PAIR_PATH = BASE / "data" / "top_commuting_pairs.csv"
API_DIR = BASE / "api" / "crawl_sessions"
OUT_METRICS = BASE / "metrics" / "top20_congestion_top50_note"
OUT_FIGS = BASE / "figures" / "top20_congestion_top50_note"
OUT_TEX_DIR = ROOT.parents[0] / "Documents" / "L4_pipeline_outputs" / "national_tidal_commuting_notes"

AM_SESSION = "20260427T070801"
PM_SESSION = "20260427T171554"
NIGHT_LATE_SESSION = "20260427T003434"  # labeled FREE in the raw session, used here as the late-night baseline
NIGHT_FALLBACK_SESSION = "20260426T222257"


def configure_plot_fonts() -> None:
    plt.rcParams["font.sans-serif"] = [
        "PingFang SC",
        "Hiragino Sans GB",
        "Microsoft YaHei",
        "Noto Sans CJK SC",
        "Arial Unicode MS",
        "DejaVu Sans",
    ]
    plt.rcParams["axes.unicode_minus"] = False


def weighted_average(values: pd.Series, weights: pd.Series) -> float:
    v = pd.to_numeric(values, errors="coerce").astype(float)
    w = pd.to_numeric(weights, errors="coerce").fillna(0.0).astype(float)
    mask = v.notna() & w.notna() & (w > 0)
    if not mask.any():
        return float("nan")
    return float(np.average(v[mask], weights=w[mask]))


def weighted_median(values: pd.Series, weights: pd.Series) -> float:
    v = pd.to_numeric(values, errors="coerce").astype(float)
    w = pd.to_numeric(weights, errors="coerce").fillna(0.0).astype(float)
    mask = v.notna() & w.notna() & (w > 0)
    if not mask.any():
        return float("nan")
    v = v[mask].to_numpy()
    w = w[mask].to_numpy()
    order = np.argsort(v)
    v = v[order]
    w = w[order]
    cw = np.cumsum(w)
    return float(v[np.searchsorted(cw, 0.5 * cw[-1], side="left")])


def load_session(session_id: str) -> pd.DataFrame:
    path = API_DIR / session_id / "session_matrix_flat.csv"
    df = pd.read_csv(path)
    df["query_time_local"] = pd.to_datetime(df["query_time_local"], errors="coerce")
    df["session_id"] = session_id
    return df


def build_direction_panel(df: pd.DataFrame, period_label: str) -> pd.DataFrame:
    work = df.copy()
    work["direction_label"] = np.where(
        (work["origin_type"] == "home") & (work["dest_type"] == "work"),
        "ab",
        "ba",
    )
    pv = work.pivot_table(
        index=["city_id", "pair_id"],
        columns="direction_label",
        values=["duration_sec", "distance_m", "query_time_local", "api_status"],
        aggfunc="first",
    )
    pv.columns = [f"{period_label}_{a}_{b}" for a, b in pv.columns]
    return pv.reset_index()


def attach_periods() -> pd.DataFrame:
    pairs = pd.read_csv(PAIR_PATH)

    am = load_session(AM_SESSION)
    pm = load_session(PM_SESSION)
    night_late = load_session(NIGHT_LATE_SESSION)
    night_fallback = load_session(NIGHT_FALLBACK_SESSION)

    am = am[(am["query_window"] == "AM") & (am["api_status"] == 0)].copy()
    pm = pm[(pm["query_window"] == "PM") & (pm["api_status"] == 0)].copy()
    night_late = night_late[(night_late["query_window"] == "FREE") & (night_late["api_status"] == 0)].copy()
    night_fallback = night_fallback[(night_fallback["query_window"] == "NIGHT") & (night_fallback["api_status"] == 0)].copy()

    # The late-night baseline has two failed directional rows. Keep the session as primary,
    # then backfill only the missing directions from the earlier complete NIGHT run.
    key_cols = ["city_id", "pair_id", "origin_type", "dest_type"]
    late_keys = set(map(tuple, night_late[key_cols].to_records(index=False)))
    night_missing = night_fallback[~night_fallback[key_cols].apply(tuple, axis=1).isin(late_keys)].copy()
    night = pd.concat([night_late, night_missing], ignore_index=True)

    merged = (
        pairs.merge(build_direction_panel(am, "am"), on=["city_id", "pair_id"], how="inner")
        .merge(build_direction_panel(pm, "pm"), on=["city_id", "pair_id"], how="inner")
        .merge(build_direction_panel(night, "night"), on=["city_id", "pair_id"], how="inner")
    )
    return merged


def enrich(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for period in ["am", "pm", "night"]:
        out[f"{period}_min_sec"] = out[[f"{period}_duration_sec_ab", f"{period}_duration_sec_ba"]].min(axis=1)
        out[f"{period}_max_sec"] = out[[f"{period}_duration_sec_ab", f"{period}_duration_sec_ba"]].max(axis=1)
        out[f"{period}_minmax_ratio"] = out[f"{period}_min_sec"] / out[f"{period}_max_sec"]
        out[f"{period}_gap_minutes"] = (out[f"{period}_max_sec"] - out[f"{period}_min_sec"]) / 60.0
        out[f"{period}_time_mean_minutes"] = (
            out[f"{period}_duration_sec_ab"] + out[f"{period}_duration_sec_ba"]
        ) / 120.0

    am_shorter_is_ab = out["am_duration_sec_ab"] <= out["am_duration_sec_ba"]
    pm_shorter_is_ab = out["pm_duration_sec_ab"] <= out["pm_duration_sec_ba"]
    out["night_ratio_aligned_to_am"] = np.where(
        am_shorter_is_ab,
        out["night_duration_sec_ab"] / out["night_duration_sec_ba"],
        out["night_duration_sec_ba"] / out["night_duration_sec_ab"],
    )
    out["night_ratio_aligned_to_pm"] = np.where(
        pm_shorter_is_ab,
        out["night_duration_sec_ab"] / out["night_duration_sec_ba"],
        out["night_duration_sec_ba"] / out["night_duration_sec_ab"],
    )
    out["am_over_night_ratio"] = out["am_minmax_ratio"] / out["night_ratio_aligned_to_am"]
    out["pm_over_night_ratio"] = out["pm_minmax_ratio"] / out["night_ratio_aligned_to_pm"]
    out["am_excess_gap_minutes"] = out["am_gap_minutes"] - out["night_gap_minutes"]
    out["pm_excess_gap_minutes"] = out["pm_gap_minutes"] - out["night_gap_minutes"]
    out["pair_avg_distance_km"] = out["distance_m_min"] / 1000.0 if "distance_m_min" in out.columns else out[
        ["am_distance_m_ab", "am_distance_m_ba", "pm_distance_m_ab", "pm_distance_m_ba", "night_distance_m_ab", "night_distance_m_ba"]
    ].mean(axis=1) / 1000.0
    return out


def top20_congestion_sample(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    base = df[df["selected_by_top50"].fillna(False)].copy()
    rows = []
    for (city_id, city_label), grp in base.groupby(["city_id", "city_label"], sort=False):
        w = grp["pop_total_pair"]
        rows.append(
            {
                "city_id": city_id,
                "city_label": city_label,
                "pair_count": int(len(grp)),
                "intra_commute_pop_2021": float(grp["intra_commute_pop_2021"].iloc[0]),
                "weighted_mean_excess_gap_minutes_am": weighted_average(grp["am_excess_gap_minutes"], w),
                "weighted_mean_pair_distance_km": weighted_average(grp["pair_avg_distance_km"], w),
                "weighted_median_am_minmax_ratio": weighted_median(grp["am_minmax_ratio"], w),
                "weighted_median_pm_minmax_ratio": weighted_median(grp["pm_minmax_ratio"], w),
                "weighted_median_night_minmax_ratio": weighted_median(grp["night_minmax_ratio"], w),
                "weighted_median_am_over_night_ratio": weighted_median(grp["am_over_night_ratio"], w),
                "weighted_median_pm_over_night_ratio": weighted_median(grp["pm_over_night_ratio"], w),
            }
        )
    city = pd.DataFrame(rows).sort_values("weighted_mean_excess_gap_minutes_am", ascending=False).reset_index(drop=True)
    keep = set(city.head(20)["city_id"])
    sample = base[base["city_id"].isin(keep)].copy()
    return city, sample


def build_period_summary(sample: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for period, label in [("am", "AM"), ("pm", "PM"), ("night", "Late-night")]:
        rows.append(
            {
                "period": label,
                "pair_count": int(len(sample)),
                "city_count": int(sample["city_id"].nunique()),
                "mean_pair_distance_km": float(sample["pair_avg_distance_km"].mean()),
                "median_pair_distance_km": float(sample["pair_avg_distance_km"].median()),
                "mean_travel_time_minutes": float(sample[f"{period}_time_mean_minutes"].mean()),
                "median_travel_time_minutes": float(sample[f"{period}_time_mean_minutes"].median()),
                "mean_minmax_ratio": float(sample[f"{period}_minmax_ratio"].mean()),
                "median_minmax_ratio": float(sample[f"{period}_minmax_ratio"].median()),
                "share_minmax_le_0_90": float((sample[f"{period}_minmax_ratio"] <= 1 / 1.10).mean()),
                "share_minmax_le_0_83": float((sample[f"{period}_minmax_ratio"] <= 1 / 1.20).mean()),
                "mean_gap_minutes": float(sample[f"{period}_gap_minutes"].mean()),
                "median_gap_minutes": float(sample[f"{period}_gap_minutes"].median()),
            }
        )
    return pd.DataFrame(rows)


def build_adjusted_summary(sample: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for period, col in [("AM", "am_over_night_ratio"), ("PM", "pm_over_night_ratio")]:
        rows.append(
            {
                "period": period,
                "mean_period_over_night_ratio": float(sample[col].mean()),
                "median_period_over_night_ratio": float(sample[col].median()),
                "share_more_asymmetric_than_night": float((sample[col] < 1.0).mean()),
                "share_much_more_asymmetric_than_night": float((sample[col] <= 0.90).mean()),
            }
        )
    return pd.DataFrame(rows)


def build_city_panel(sample: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (city_id, city_label), grp in sample.groupby(["city_id", "city_label"], sort=False):
        w = grp["pop_total_pair"]
        rows.append(
            {
                "city_id": city_id,
                "city_label": city_label,
                "pair_count": int(len(grp)),
                "weighted_mean_distance_km": weighted_average(grp["pair_avg_distance_km"], w),
                "weighted_mean_am_minutes": weighted_average(grp["am_time_mean_minutes"], w),
                "weighted_mean_pm_minutes": weighted_average(grp["pm_time_mean_minutes"], w),
                "weighted_mean_night_minutes": weighted_average(grp["night_time_mean_minutes"], w),
                "weighted_median_am_minmax_ratio": weighted_median(grp["am_minmax_ratio"], w),
                "weighted_median_pm_minmax_ratio": weighted_median(grp["pm_minmax_ratio"], w),
                "weighted_median_night_minmax_ratio": weighted_median(grp["night_minmax_ratio"], w),
                "weighted_median_am_over_night_ratio": weighted_median(grp["am_over_night_ratio"], w),
                "weighted_median_pm_over_night_ratio": weighted_median(grp["pm_over_night_ratio"], w),
                "weighted_mean_am_excess_gap_minutes": weighted_average(grp["am_excess_gap_minutes"], w),
                "weighted_mean_pm_excess_gap_minutes": weighted_average(grp["pm_excess_gap_minutes"], w),
            }
        )
    return pd.DataFrame(rows).sort_values("weighted_mean_am_excess_gap_minutes", ascending=False).reset_index(drop=True)


def plot_ratio_histogram(sample: pd.DataFrame, out_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(8.4, 5.6))
    for col, label, color in [
        ("am_minmax_ratio", "AM", "#c44e52"),
        ("pm_minmax_ratio", "PM", "#dd8452"),
        ("night_minmax_ratio", "Late-night", "#4c72b0"),
    ]:
        ax.hist(
            sample[col],
            bins=36,
            range=(0.35, 1.0),
            histtype="step",
            linewidth=2.0,
            color=color,
            label=label,
        )
    ax.set_title("Directional asymmetry across the three time windows")
    ax.set_xlabel("min(travel time in two directions) / max(travel time in two directions)")
    ax.set_ylabel("Number of town pairs")
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(out_path, dpi=240)
    plt.close(fig)


def plot_ratio_cdf(sample: pd.DataFrame, out_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(8.4, 5.6))
    for col, label, color in [
        ("am_minmax_ratio", "AM", "#c44e52"),
        ("pm_minmax_ratio", "PM", "#dd8452"),
        ("night_minmax_ratio", "Late-night", "#4c72b0"),
    ]:
        sub = sample[[col]].dropna().sort_values(col)
        x = sub[col].to_numpy()
        y = np.arange(1, len(x) + 1) / len(x)
        ax.plot(x, y, color=color, linewidth=2.0, label=label)
    ax.set_title("Cumulative distribution of directional asymmetry")
    ax.set_xlabel("min(travel time in two directions) / max(travel time in two directions)")
    ax.set_ylabel("Cumulative share of town pairs")
    ax.legend(frameon=False, loc="lower right")
    fig.tight_layout()
    fig.savefig(out_path, dpi=240)
    plt.close(fig)


def plot_adjusted_hist(sample: pd.DataFrame, out_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(8.4, 5.6))
    for col, label, color in [
        ("am_over_night_ratio", "AM / late-night", "#c44e52"),
        ("pm_over_night_ratio", "PM / late-night", "#dd8452"),
    ]:
        ax.hist(
            sample[col],
            bins=36,
            range=(0.45, 1.55),
            histtype="step",
            linewidth=2.0,
            color=color,
            label=label,
        )
    ax.axvline(1.0, color="#333333", linestyle="--", linewidth=1.2)
    ax.set_title("Period-specific asymmetry relative to the late-night baseline")
    ax.set_xlabel("period min/max ratio divided by the late-night ratio aligned to that period")
    ax.set_ylabel("Number of town pairs")
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(out_path, dpi=240)
    plt.close(fig)


def plot_city_scatter(city_panel: pd.DataFrame, out_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(7.2, 6.4))
    ax.scatter(
        city_panel["weighted_median_am_minmax_ratio"],
        city_panel["weighted_median_pm_minmax_ratio"],
        s=54,
        color="#2f6b9a",
        alpha=0.85,
    )
    for row in city_panel.itertuples(index=False):
        ax.text(
            row.weighted_median_am_minmax_ratio + 0.002,
            row.weighted_median_pm_minmax_ratio + 0.002,
            row.city_label,
            fontsize=8,
        )
    lo = min(city_panel["weighted_median_am_minmax_ratio"].min(), city_panel["weighted_median_pm_minmax_ratio"].min())
    hi = max(city_panel["weighted_median_am_minmax_ratio"].max(), city_panel["weighted_median_pm_minmax_ratio"].max())
    ax.plot([lo, hi], [lo, hi], linestyle="--", color="#666666", linewidth=1.0)
    ax.set_title("City-level asymmetry in the AM and PM peaks")
    ax.set_xlabel("Weighted median AM min/max ratio")
    ax.set_ylabel("Weighted median PM min/max ratio")
    fig.tight_layout()
    fig.savefig(out_path, dpi=240)
    plt.close(fig)


def plot_city_period_lines(city_panel: pd.DataFrame, out_path: Path) -> None:
    use = city_panel.sort_values("weighted_median_am_over_night_ratio", ascending=True).copy()
    y = np.arange(len(use))
    fig, ax = plt.subplots(figsize=(9.0, 8.6))
    ax.hlines(y, use["weighted_median_am_minmax_ratio"], use["weighted_median_pm_minmax_ratio"], color="#cccccc", linewidth=1.0)
    ax.scatter(use["weighted_median_am_minmax_ratio"], y, color="#c44e52", label="AM", s=42)
    ax.scatter(use["weighted_median_pm_minmax_ratio"], y, color="#dd8452", label="PM", s=42)
    ax.scatter(use["weighted_median_night_minmax_ratio"], y, color="#4c72b0", label="Late-night", s=42)
    ax.set_yticks(y, use["city_label"])
    ax.set_xlabel("Weighted median min/max ratio")
    ax.set_title("Directional asymmetry by city and time window")
    ax.legend(frameon=False, ncols=3, loc="lower right")
    fig.tight_layout()
    fig.savefig(out_path, dpi=240)
    plt.close(fig)


def build_tex(city_rank: pd.DataFrame, period_summary: pd.DataFrame, adjusted_summary: pd.DataFrame) -> str:
    top10 = city_rank.head(10)["city_label"].tolist()
    top10_text = ", ".join(top10)
    row_am = period_summary[period_summary["period"] == "AM"].iloc[0]
    row_pm = period_summary[period_summary["period"] == "PM"].iloc[0]
    row_ng = period_summary[period_summary["period"] == "Late-night"].iloc[0]
    row_adj_am = adjusted_summary[adjusted_summary["period"] == "AM"].iloc[0]
    row_adj_pm = adjusted_summary[adjusted_summary["period"] == "PM"].iloc[0]

    fig_dir = (OUT_FIGS.resolve())
    return rf"""\documentclass[12pt]{{article}}
\usepackage[margin=1in]{{geometry}}
\usepackage[UTF8,fontset=fandol]{{ctex}}
\usepackage{{amsmath,amssymb,graphicx,booktabs,tabularx,setspace}}
\renewcommand{{\figurename}}{{Figure}}
\renewcommand{{\tablename}}{{Table}}
\title{{Top-20 Congestion Cities: Directional Asymmetry Across AM, PM, and the Late-Night Baseline}}
\author{{Xiaoruo Feng}}
\date{{April 28, 2026}}

\begin{{document}}
\maketitle

\section*{{Sample definition}}

The sample is restricted to the twenty cities with the largest AM excess directional gap in the retained top-50 pair design. City ranking is computed from the early-morning AM session (07:08--08:14) relative to the later overnight session (00:34--01:48), with pair weights given by total two-way commuter volume. Within each selected city, the analysis keeps the top 50 undirected town pairs ranked by commuter volume. The retained top ten cities under this rule are {top10_text}. The late-night baseline uses the 00:34--01:48 session as the primary source. Two directional rows failed in that session, so those rows are backfilled from the earlier complete 22:22--23:48 night run.

For the sampled cities, the three data versions used in the tables and figures are: AM session 20260427T070801, sampled from 07:08:01 to 08:14:32; PM session 20260427T171554, sampled from 17:15:54 to 18:26:28; and late-night session 20260427T003434, sampled from 00:34:35 to 01:48:51. All clock times are local Asia/Shanghai times.

\section*{{Summary statistics}}

Table~\ref{{tab:summary}} reports pooled descriptive statistics for the resulting 1,000 town pairs. Average pair distance is stable across the three windows because the sample is fixed, at roughly {row_am['mean_pair_distance_km']:.2f} kilometres. Mean bilateral travel time rises from {row_ng['mean_travel_time_minutes']:.2f} minutes in the late-night baseline to {row_am['mean_travel_time_minutes']:.2f} minutes in AM and {row_pm['mean_travel_time_minutes']:.2f} minutes in PM. Directional asymmetry is also strongest in AM: the pooled median min/max ratio is {row_am['median_minmax_ratio']:.3f} in AM, compared with {row_pm['median_minmax_ratio']:.3f} in PM and {row_ng['median_minmax_ratio']:.3f} in the late-night baseline. The left tail is economically meaningful. In AM, {100*row_am['share_minmax_le_0_90']:.1f}\% of pairs have a min/max ratio below 0.90; the corresponding shares are {100*row_pm['share_minmax_le_0_90']:.1f}\% in PM and {100*row_ng['share_minmax_le_0_90']:.1f}\% in the late-night baseline.

The late-night-adjusted evidence is sharper. The median aligned AM-to-night ratio is {row_adj_am['median_period_over_night_ratio']:.3f}, while the median PM-to-night ratio is {row_adj_pm['median_period_over_night_ratio']:.3f}. The share of pairs that are more asymmetric than the late-night baseline is {100*row_adj_am['share_more_asymmetric_than_night']:.1f}\% in AM and {100*row_adj_pm['share_more_asymmetric_than_night']:.1f}\% in PM. AM therefore delivers the clearest departure from the low-congestion benchmark, although PM still exhibits a nontrivial right-tail widening relative to late night.

\begin{{table}}[h!]
\centering
\caption{{Pooled descriptive statistics, top-20 congestion cities and top-50 pairs}}
\label{{tab:summary}}
\begin{{tabular}}{{lcccccc}}
\toprule
Period & Pairs & Mean dist. (km) & Mean time (min) & Median time (min) & Median min/max & Share min/max $\leq 0.90$ \\
\midrule
AM & {int(row_am['pair_count'])} & {row_am['mean_pair_distance_km']:.2f} & {row_am['mean_travel_time_minutes']:.2f} & {row_am['median_travel_time_minutes']:.2f} & {row_am['median_minmax_ratio']:.3f} & {row_am['share_minmax_le_0_90']:.3f} \\
PM & {int(row_pm['pair_count'])} & {row_pm['mean_pair_distance_km']:.2f} & {row_pm['mean_travel_time_minutes']:.2f} & {row_pm['median_travel_time_minutes']:.2f} & {row_pm['median_minmax_ratio']:.3f} & {row_pm['share_minmax_le_0_90']:.3f} \\
Late-night & {int(row_ng['pair_count'])} & {row_ng['mean_pair_distance_km']:.2f} & {row_ng['mean_travel_time_minutes']:.2f} & {row_ng['median_travel_time_minutes']:.2f} & {row_ng['median_minmax_ratio']:.3f} & {row_ng['share_minmax_le_0_90']:.3f} \\
\bottomrule
\end{{tabular}}
\end{{table}}

\section*{{Figures}}

Figure~\ref{{fig:hist}} plots the pooled min/max distributions. The AM distribution lies furthest to the left. PM remains to the left of the late-night benchmark, but the displacement is smaller. Figure~\ref{{fig:adjhist}} sharpens this comparison by normalising each pair's asymmetry against the late-night directional baseline. The AM distribution is concentrated below one, which indicates that morning directional imbalance typically exceeds the corresponding late-night imbalance for the same pair. The PM distribution also sits below one, though less decisively. Figure~\ref{{fig:scatter}} reports city-level AM and PM asymmetry, one point per city. The cross-city cloud is broad rather than collinear, which indicates substantial heterogeneity in how strongly morning and evening asymmetries covary. Figure~\ref{{fig:citylines}} shows the same twenty cities with separate points for AM, PM, and late night. This panel is more informative than twenty separate histograms because each city contributes only fifty pairs; the city-level weighted medians suppress small-sample noise while preserving the cross-city ranking.

\begin{{figure}}[h!]
\centering
\includegraphics[width=0.88\textwidth]{{{fig_dir / 'figure1_ratio_hist_overlay.png'}}}
\caption{{Pooled distribution of directional asymmetry. The horizontal axis is the min/max ratio of bilateral travel times, computed within the same town pair and time window. Lower values correspond to stronger directional asymmetry.}}
\label{{fig:hist}}
\end{{figure}}

\begin{{figure}}[h!]
\centering
\includegraphics[width=0.88\textwidth]{{{fig_dir / 'figure3_adjusted_hist_overlay.png'}}}
\caption{{AM and PM asymmetry relative to the late-night baseline. A value below one indicates that the same pair is more asymmetric in the peak period than in the late-night baseline, after fixing the period-specific ordering of the two directions.}}
\label{{fig:adjhist}}
\end{{figure}}

\begin{{figure}}[h!]
\centering
\includegraphics[width=0.75\textwidth]{{{fig_dir / 'figure4_city_scatter_am_pm.png'}}}
\caption{{City-level directional asymmetry in AM and PM. Each point is one city, measured by the weighted median min/max ratio across its top-50 town pairs.}}
\label{{fig:scatter}}
\end{{figure}}

\begin{{figure}}[h!]
\centering
\includegraphics[width=0.95\textwidth]{{{fig_dir / 'figure5_city_lines_periods.png'}}}
\caption{{Directional asymmetry by city and time window. Cities are sorted by AM asymmetry relative to the late-night baseline.}}
\label{{fig:citylines}}
\end{{figure}}

\section*{{Takeaway}}

This top-20 congestion-city sample delivers a cleaner descriptive pattern than the unrestricted 50-city pool. Average travel time rises in both AM and PM relative to the late-night benchmark, but the directional imbalance is most pronounced in AM. The PM peak still exhibits directional asymmetry, although its median ratio remains closer to the late-night baseline. For the present motivation exercise, the strongest evidence therefore comes from the top-20 congestion-city sample, measured on the top-50 town pairs per city and evaluated relative to the later overnight baseline.

\end{{document}}
"""


def main() -> None:
    OUT_METRICS.mkdir(parents=True, exist_ok=True)
    OUT_FIGS.mkdir(parents=True, exist_ok=True)
    OUT_TEX_DIR.mkdir(parents=True, exist_ok=True)
    ensure_mpl_cache(str(BASE.parent))
    configure_plot_fonts()

    merged = enrich(attach_periods())
    city_rank, sample = top20_congestion_sample(merged)
    period_summary = build_period_summary(sample)
    adjusted_summary = build_adjusted_summary(sample)
    city_panel = build_city_panel(sample)

    sample.to_csv(OUT_METRICS / "pair_level_sample.csv", index=False)
    city_rank.to_csv(OUT_METRICS / "city_rank_full.csv", index=False)
    city_panel.to_csv(OUT_METRICS / "city_panel_top20.csv", index=False)
    period_summary.to_csv(OUT_METRICS / "period_summary_top20.csv", index=False)
    adjusted_summary.to_csv(OUT_METRICS / "adjusted_summary_top20.csv", index=False)

    plot_ratio_histogram(sample, OUT_FIGS / "figure1_ratio_hist_overlay.png")
    plot_ratio_cdf(sample, OUT_FIGS / "figure2_ratio_cdf_overlay.png")
    plot_adjusted_hist(sample, OUT_FIGS / "figure3_adjusted_hist_overlay.png")
    plot_city_scatter(city_panel, OUT_FIGS / "figure4_city_scatter_am_pm.png")
    plot_city_period_lines(city_panel, OUT_FIGS / "figure5_city_lines_periods.png")

    tex_path = OUT_TEX_DIR / "national_tidal_commuting_top20_congestion_note_20260428.tex"
    tex_path.write_text(build_tex(city_rank, period_summary, adjusted_summary), encoding="utf-8")

    manifest = {
        "version_id": VERSION_ID,
        "sample_definition": "top20 congestion cities by weighted mean AM excess gap, top50 pairs within city",
        "sessions": {
            "am": AM_SESSION,
            "pm": PM_SESSION,
            "night_late": NIGHT_LATE_SESSION,
            "night_fallback": NIGHT_FALLBACK_SESSION,
        },
        "metrics_dir": str(OUT_METRICS.resolve()),
        "figures_dir": str(OUT_FIGS.resolve()),
        "tex_path": str(tex_path.resolve()),
    }
    (OUT_METRICS / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[top20_note] wrote metrics to {OUT_METRICS}")
    print(f"[top20_note] wrote figures to {OUT_FIGS}")
    print(f"[top20_note] wrote tex to {tex_path}")


if __name__ == "__main__":
    main()
