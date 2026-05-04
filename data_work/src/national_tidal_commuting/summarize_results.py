from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.diagnostics.mpl_setup import ensure_mpl_cache
from src.national_tidal_commuting.common import abs_log_ratio, ensure_version_dirs, get_version_root, load_config, safe_divide, weighted_mean, weighted_median


def parse_args():
    parser = argparse.ArgumentParser(description="Summarize Baidu matrix results for national top-OD evidence.")
    parser.add_argument("--config", required=True, help="JSON config path.")
    parser.add_argument("--version-id", default=None, help="Optional version override.")
    parser.add_argument("--output-dir", default=None, help="Optional output dir override.")
    return parser.parse_args()


def latest_results(api_dir: Path) -> pd.DataFrame:
    files = sorted(api_dir.glob("baidu_matrix_results_*.csv"))
    files += sorted(api_dir.glob("crawl_sessions/*/session_matrix_flat.csv"))
    if not files:
        return pd.DataFrame()
    parts = []
    for path in files:
        df = pd.read_csv(path)
        df["source_file"] = path.name
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


def build_pair_summary(pairs: pd.DataFrame, results: pd.DataFrame) -> pd.DataFrame:
    work = results.copy()
    work["direction_label"] = np.where(
        (work["origin_type"] == "home") & (work["dest_type"] == "work"),
        "home_to_work",
        "work_to_home",
    )
    pivot = work.pivot_table(
        index=["city_id", "pair_id"],
        columns=["query_window", "direction_label"],
        values=["duration_sec", "distance_m", "api_status"],
        aggfunc="first",
    )
    pivot.columns = ["_".join(str(x) for x in col).lower() for col in pivot.columns]
    pivot = pivot.reset_index()
    out = pairs.merge(pivot, on=["city_id", "pair_id"], how="left")
    out["ab_am_sec"] = out.get("duration_sec_am_home_to_work")
    out["ba_am_sec"] = out.get("duration_sec_am_work_to_home")
    out["ab_pm_sec"] = out.get("duration_sec_pm_home_to_work")
    out["ba_pm_sec"] = out.get("duration_sec_pm_work_to_home")
    # 当前更严格口径下，优先把 NIGHT 当作低拥堵对照；若只有旧 FREE 文件则回退兼容。
    if "duration_sec_night_home_to_work" in out.columns:
        out["ab_free_sec"] = out["duration_sec_night_home_to_work"]
    elif "duration_sec_free_home_to_work" in out.columns:
        out["ab_free_sec"] = out["duration_sec_free_home_to_work"]
    else:
        out["ab_free_sec"] = np.nan
    if "duration_sec_night_work_to_home" in out.columns:
        out["ba_free_sec"] = out["duration_sec_night_work_to_home"]
    elif "duration_sec_free_work_to_home" in out.columns:
        out["ba_free_sec"] = out["duration_sec_free_work_to_home"]
    else:
        out["ba_free_sec"] = np.nan
    out["ab_night_sec"] = out["ab_free_sec"]
    out["ba_night_sec"] = out["ba_free_sec"]
    out["am_directional_ratio"] = out.apply(lambda r: safe_divide(r["ab_am_sec"], r["ba_am_sec"]), axis=1)
    out["pm_directional_ratio"] = out.apply(lambda r: safe_divide(r["ab_pm_sec"], r["ba_pm_sec"]), axis=1)
    out["commute_favored_ratio_am"] = out["am_directional_ratio"]
    out["commute_favored_ratio_pm"] = out["pm_directional_ratio"]
    out["am_peak_penalty_home_work"] = out.apply(lambda r: safe_divide(r["ab_am_sec"], r["ab_free_sec"]), axis=1)
    out["pm_peak_penalty_work_home"] = out.apply(lambda r: safe_divide(r["ba_pm_sec"], r["ba_free_sec"]), axis=1)
    out["am_abs_log_ratio"] = out.apply(lambda r: abs_log_ratio(r["ab_am_sec"], r["ba_am_sec"]), axis=1)
    out["pm_abs_log_ratio"] = out.apply(lambda r: abs_log_ratio(r["ab_pm_sec"], r["ba_pm_sec"]), axis=1)
    out["am_home_to_work_slower"] = (out["ab_am_sec"] > out["ba_am_sec"]).astype("float")
    out["pm_work_to_home_slower"] = (out["ba_pm_sec"] > out["ab_pm_sec"]).astype("float")
    out["reversal_flag"] = np.where(
        (out[["ab_am_sec", "ba_am_sec", "ab_pm_sec", "ba_pm_sec"]].notna().all(axis=1))
        & (((out["ab_am_sec"] > out["ba_am_sec"]) & (out["ab_pm_sec"] < out["ba_pm_sec"])) | ((out["ab_am_sec"] < out["ba_am_sec"]) & (out["ab_pm_sec"] > out["ba_pm_sec"]))),
        1,
        0,
    )
    out["balanced_sample_flag"] = out[
        ["ab_am_sec", "ba_am_sec", "ab_pm_sec", "ba_pm_sec", "ab_free_sec", "ba_free_sec"]
    ].gt(0).all(axis=1)
    dist_cols = [c for c in out.columns if c.startswith("distance_m_")]
    out["distance_m_min"] = out[dist_cols].min(axis=1) if dist_cols else np.nan
    out["distance_m_max"] = out[dist_cols].max(axis=1) if dist_cols else np.nan
    out["outlier_distance_flag"] = (out["distance_m_min"] < 2000) | (out["distance_m_max"] > 40000)
    out["implied_speed_am_home_work_kmh"] = out.apply(
        lambda r: safe_divide((r.get("distance_m_am_home_to_work", np.nan) or np.nan) * 3.6, r["ab_am_sec"]),
        axis=1,
    )
    out["implied_speed_pm_work_home_kmh"] = out.apply(
        lambda r: safe_divide((r.get("distance_m_pm_work_to_home", np.nan) or np.nan) * 3.6, r["ba_pm_sec"]),
        axis=1,
    )
    out["outlier_speed_flag"] = (
        out["implied_speed_am_home_work_kmh"].lt(3)
        | out["implied_speed_am_home_work_kmh"].gt(90)
        | out["implied_speed_pm_work_home_kmh"].lt(3)
        | out["implied_speed_pm_work_home_kmh"].gt(90)
    )
    return out


def summarize_city(pair_summary: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (city_id, city_label), grp in pair_summary.groupby(["city_id", "city_label"], sort=False):
        use = grp[grp["balanced_sample_flag"]].copy()
        weights = use["pop_total_pair"].fillna(0.0).astype(float).values
        rows.append(
            {
                "city_id": city_id,
                "city_label": city_label,
                "balanced_pairs": int(len(use)),
                "low_coverage_flag": bool(len(use) < 30),
                "weighted_am_asymmetry_median": weighted_median(use["am_abs_log_ratio"], weights),
                "weighted_pm_asymmetry_median": weighted_median(use["pm_abs_log_ratio"], weights),
                "weighted_am_asymmetry_mean": weighted_mean(use["am_abs_log_ratio"].fillna(0).values, weights) if len(use) else np.nan,
                "weighted_pm_asymmetry_mean": weighted_mean(use["pm_abs_log_ratio"].fillna(0).values, weights) if len(use) else np.nan,
                "am_home_to_work_slower_share": weighted_mean(use["am_home_to_work_slower"].fillna(0).values, weights) if len(use) else np.nan,
                "pm_work_to_home_slower_share": weighted_mean(use["pm_work_to_home_slower"].fillna(0).values, weights) if len(use) else np.nan,
                "reversal_share": weighted_mean(use["reversal_flag"].fillna(0).values, weights) if len(use) else np.nan,
                "am_peak_penalty_home_work_mean": weighted_mean(use["am_peak_penalty_home_work"].fillna(0).values, weights) if len(use) else np.nan,
                "pm_peak_penalty_work_home_mean": weighted_mean(use["pm_peak_penalty_work_home"].fillna(0).values, weights) if len(use) else np.nan,
                "top10_pair_pop_share": safe_divide(use.sort_values("rank_in_city")["pop_total_pair"].head(10).sum(), use["pop_total_pair"].sum()),
            }
        )
    return pd.DataFrame(rows)


def plot_city_bar(city_summary: pd.DataFrame, value_col: str, title: str, out_path: Path):
    use = city_summary.sort_values(value_col, ascending=False).copy()
    fig, ax = plt.subplots(figsize=(11, 5.5))
    ax.bar(use["city_label"], use[value_col], color="#2b6cb0")
    ax.set_title(title)
    ax.set_ylabel("|log ratio|")
    ax.tick_params(axis="x", rotation=45)
    fig.tight_layout()
    fig.savefig(out_path, dpi=180)
    plt.close(fig)


def plot_hist(pair_summary: pd.DataFrame, out_path: Path):
    use = pair_summary[pair_summary["balanced_sample_flag"]].copy()
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.hist(use["am_directional_ratio"].dropna(), bins=30, alpha=0.55, color="#2b6cb0", label="AM")
    ax.hist(use["pm_directional_ratio"].dropna(), bins=30, alpha=0.45, color="#dd6b20", label="PM")
    ax.set_title("Pooled directional ratio distribution")
    ax.set_xlabel("home->work / work->home")
    ax.set_ylabel("Pairs")
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(out_path, dpi=180)
    plt.close(fig)


def plot_reversal(city_summary: pd.DataFrame, out_path: Path):
    use = city_summary.sort_values("reversal_share", ascending=False).copy()
    fig, ax = plt.subplots(figsize=(11, 5.5))
    ax.bar(use["city_label"], use["reversal_share"], color="#805ad5")
    ax.set_title("Reversal share by city")
    ax.set_ylabel("Weighted share")
    ax.tick_params(axis="x", rotation=45)
    fig.tight_layout()
    fig.savefig(out_path, dpi=180)
    plt.close(fig)


def plot_rank_vs_asym(pair_summary: pd.DataFrame, out_path: Path):
    use = pair_summary[pair_summary["balanced_sample_flag"]].copy()
    fig, ax = plt.subplots(figsize=(8, 5))
    sizes = np.clip(np.sqrt(use["pop_total_pair"].fillna(0).values), 10, 160)
    ax.scatter(use["rank_in_city"], use["am_abs_log_ratio"], s=sizes, alpha=0.55, color="#2f855a", edgecolors="none")
    ax.set_title("Pair rank and AM asymmetry")
    ax.set_xlabel("Rank in city")
    ax.set_ylabel("|log(home->work / work->home)|")
    fig.tight_layout()
    fig.savefig(out_path, dpi=180)
    plt.close(fig)


def plot_representative_cities(pair_summary: pd.DataFrame, out_path: Path):
    use = pair_summary[pair_summary["balanced_sample_flag"]].copy()
    top_cities = (
        use.groupby(["city_id", "city_label"], as_index=False)["pop_total_pair"].sum().sort_values("pop_total_pair", ascending=False).head(4)
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
        ax.set_title(str(city.city_label))
        ax.set_ylabel("Minutes")
        ax.set_xticks(x, [f"#{int(v)}" for v in sub["rank_in_city"]])
    axes[0].legend(frameon=False, ncols=2)
    axes[-1].set_xlabel("Top commuting pair rank")
    fig.tight_layout()
    fig.savefig(out_path, dpi=180)
    plt.close(fig)


def main():
    args = parse_args()
    config = load_config(args.config)
    version_root = get_version_root(config, args.version_id, args.output_dir)
    dirs = ensure_version_dirs(version_root)
    ensure_mpl_cache(str(version_root.parent))
    top_pairs_path = dirs["data"] / "top_commuting_pairs.csv"
    if not top_pairs_path.exists():
        raise FileNotFoundError(f"Missing top pair sample file: {top_pairs_path}")
    pairs = pd.read_csv(top_pairs_path)
    results = latest_results(dirs["api"])
    if results.empty:
        raise RuntimeError(f"No Baidu result CSVs found in {dirs['api']}")

    pair_summary = build_pair_summary(pairs, results)
    city_summary = summarize_city(pair_summary)

    pair_summary_path = dirs["metrics"] / "top_commuting_pairs_pair_summary.csv"
    city_summary_path = dirs["metrics"] / "top_commuting_pairs_city_metrics.csv"
    pair_summary.to_csv(pair_summary_path, index=False)
    city_summary.to_csv(city_summary_path, index=False)

    plot_city_bar(city_summary, "weighted_am_asymmetry_median", "AM commuting-direction asymmetry by city", dirs["figures"] / "figure1_am_asymmetry_by_city.png")
    plot_city_bar(city_summary, "weighted_pm_asymmetry_median", "PM commuting-direction asymmetry by city", dirs["figures"] / "figure2_pm_asymmetry_by_city.png")
    plot_hist(pair_summary, dirs["figures"] / "figure3_pooled_directional_ratio_hist.png")
    plot_reversal(city_summary, dirs["figures"] / "figure4_reversal_share_by_city.png")
    plot_rank_vs_asym(pair_summary, dirs["figures"] / "figure5_rank_vs_am_asymmetry.png")
    plot_representative_cities(pair_summary, dirs["figures"] / "figure6_representative_city_top_pairs.png")

    (version_root / "config_snapshot.summarize_results.json").write_text(
        json.dumps(
            {
                "stage": "national_tidal_commuting.summarize_results",
                "config_path": config["_config_path"],
                "version_root": str(version_root.resolve()),
                "pair_summary_path": str(pair_summary_path.resolve()),
                "city_summary_path": str(city_summary_path.resolve()),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"[summarize_results] wrote pair summary to {pair_summary_path}")
    print(f"[summarize_results] wrote city summary to {city_summary_path}")


if __name__ == "__main__":
    main()
