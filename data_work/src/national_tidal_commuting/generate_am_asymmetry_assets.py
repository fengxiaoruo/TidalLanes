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


def parse_args():
    parser = argparse.ArgumentParser(description="Generate descriptive AM asymmetry figures and summary stats.")
    parser.add_argument("--config", required=True, help="JSON config path.")
    parser.add_argument("--version-id", default=None, help="Optional version override.")
    parser.add_argument("--output-dir", default=None, help="Optional output dir override.")
    return parser.parse_args()


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


def prepare_sample(pair_summary: pd.DataFrame) -> pd.DataFrame:
    req_cols = ["ab_am_sec", "ba_am_sec", "ab_night_sec", "ba_night_sec", "pop_total_pair"]
    use = pair_summary.copy()
    mask = use[req_cols].notna().all(axis=1) & use[req_cols].gt(0).all(axis=1)
    use = use[mask].copy()
    use["am_ratio"] = use["ab_am_sec"] / use["ba_am_sec"]
    use["night_ratio"] = use["ab_night_sec"] / use["ba_night_sec"]
    use["am_abs_log_ratio"] = np.abs(np.log(use["am_ratio"]))
    use["night_abs_log_ratio"] = np.abs(np.log(use["night_ratio"]))
    use["commute_penalty_ratio"] = use["ab_am_sec"] / use["ab_night_sec"]
    use["counter_penalty_ratio"] = use["ba_am_sec"] / use["ba_night_sec"]
    use["tidal_gap_minutes"] = (
        (use["ab_am_sec"] - use["ab_night_sec"]) - (use["ba_am_sec"] - use["ba_night_sec"])
    ) / 60.0
    use["am_commute_slower"] = (use["ab_am_sec"] > use["ba_am_sec"]).astype(float)
    use["night_commute_slower"] = (use["ab_night_sec"] > use["ba_night_sec"]).astype(float)
    use["am_ratio_gt_1_1"] = (use["am_ratio"] >= 1.10).astype(float)
    use["am_ratio_gt_1_2"] = (use["am_ratio"] >= 1.20).astype(float)
    use["night_ratio_gt_1_1"] = (use["night_ratio"] >= 1.10).astype(float)
    use["night_ratio_gt_1_2"] = (use["night_ratio"] >= 1.20).astype(float)
    use["tidal_gap_gt_1min"] = (use["tidal_gap_minutes"] > 1).astype(float)
    use["tidal_gap_gt_3min"] = (use["tidal_gap_minutes"] > 3).astype(float)
    use["tidal_gap_gt_5min"] = (use["tidal_gap_minutes"] > 5).astype(float)
    return use


def build_national_summary(use: pd.DataFrame) -> pd.DataFrame:
    w = use["pop_total_pair"]
    rows = [
        ("pair_count", float(len(use))),
        ("weighted_mean_am_ratio", weighted_average(use["am_ratio"], w)),
        ("weighted_mean_night_ratio", weighted_average(use["night_ratio"], w)),
        ("weighted_median_am_abs_log_ratio", weighted_median(use["am_abs_log_ratio"], w)),
        ("weighted_median_night_abs_log_ratio", weighted_median(use["night_abs_log_ratio"], w)),
        ("weighted_share_am_commute_slower", weighted_average(use["am_commute_slower"], w)),
        ("weighted_share_night_commute_slower", weighted_average(use["night_commute_slower"], w)),
        ("weighted_share_am_ratio_ge_1_10", weighted_average(use["am_ratio_gt_1_1"], w)),
        ("weighted_share_am_ratio_ge_1_20", weighted_average(use["am_ratio_gt_1_2"], w)),
        ("weighted_share_night_ratio_ge_1_10", weighted_average(use["night_ratio_gt_1_1"], w)),
        ("weighted_share_night_ratio_ge_1_20", weighted_average(use["night_ratio_gt_1_2"], w)),
        ("weighted_mean_commute_penalty_ratio", weighted_average(use["commute_penalty_ratio"], w)),
        ("weighted_mean_counter_penalty_ratio", weighted_average(use["counter_penalty_ratio"], w)),
        ("weighted_mean_tidal_gap_minutes", weighted_average(use["tidal_gap_minutes"], w)),
        ("weighted_share_tidal_gap_gt_1min", weighted_average(use["tidal_gap_gt_1min"], w)),
        ("weighted_share_tidal_gap_gt_3min", weighted_average(use["tidal_gap_gt_3min"], w)),
        ("weighted_share_tidal_gap_gt_5min", weighted_average(use["tidal_gap_gt_5min"], w)),
    ]
    return pd.DataFrame(rows, columns=["metric", "value"])


def build_city_summary(use: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (city_id, city_label), grp in use.groupby(["city_id", "city_label"], sort=False):
        w = grp["pop_total_pair"]
        rows.append(
            {
                "city_id": city_id,
                "city_label": city_label,
                "pair_count": int(len(grp)),
                "weighted_mean_am_ratio": weighted_average(grp["am_ratio"], w),
                "weighted_mean_night_ratio": weighted_average(grp["night_ratio"], w),
                "weighted_median_am_abs_log_ratio": weighted_median(grp["am_abs_log_ratio"], w),
                "weighted_median_night_abs_log_ratio": weighted_median(grp["night_abs_log_ratio"], w),
                "weighted_share_am_commute_slower": weighted_average(grp["am_commute_slower"], w),
                "weighted_share_night_commute_slower": weighted_average(grp["night_commute_slower"], w),
                "weighted_share_am_ratio_ge_1_10": weighted_average(grp["am_ratio_gt_1_1"], w),
                "weighted_share_am_ratio_ge_1_20": weighted_average(grp["am_ratio_gt_1_2"], w),
                "weighted_share_night_ratio_ge_1_10": weighted_average(grp["night_ratio_gt_1_1"], w),
                "weighted_share_night_ratio_ge_1_20": weighted_average(grp["night_ratio_gt_1_2"], w),
                "weighted_mean_tidal_gap_minutes": weighted_average(grp["tidal_gap_minutes"], w),
            }
        )
    out = pd.DataFrame(rows)
    out["rank_am_asymmetry"] = out["weighted_median_am_abs_log_ratio"].rank(ascending=False, method="first").astype(int)
    return out.sort_values("weighted_median_am_abs_log_ratio", ascending=False).reset_index(drop=True)


def _weighted_hist(ax, values: pd.Series, weights: pd.Series, *, bins: int, color: str, label: str, alpha: float):
    ax.hist(
        values.to_numpy(),
        bins=bins,
        weights=weights.to_numpy(),
        density=True,
        color=color,
        alpha=alpha,
        label=label,
        edgecolor="white",
        linewidth=0.4,
    )


def plot_ratio_hist(use: pd.DataFrame, out_path: Path):
    fig, ax = plt.subplots(figsize=(8.2, 5.2))
    _weighted_hist(ax, use["am_ratio"], use["pop_total_pair"], bins=36, color="#c44e52", label="AM", alpha=0.55)
    _weighted_hist(ax, use["night_ratio"], use["pop_total_pair"], bins=36, color="#4c72b0", label="Night", alpha=0.45)
    ax.axvline(1.0, color="#222222", linestyle="--", linewidth=1)
    ax.set_title("Directional travel-time ratio distribution")
    ax.set_xlabel("home-to-work travel time / work-to-home travel time")
    ax.set_ylabel("Weighted density")
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(out_path, dpi=220)
    plt.close(fig)


def plot_abs_log_cdf(use: pd.DataFrame, out_path: Path):
    fig, ax = plt.subplots(figsize=(8.2, 5.2))
    for col, color, label in [
        ("am_abs_log_ratio", "#c44e52", "AM"),
        ("night_abs_log_ratio", "#4c72b0", "Night"),
    ]:
        sub = use[[col, "pop_total_pair"]].dropna().sort_values(col)
        x = sub[col].to_numpy()
        w = sub["pop_total_pair"].to_numpy()
        y = np.cumsum(w) / np.sum(w)
        ax.plot(x, y, color=color, linewidth=2.2, label=label)
    ax.set_title("Cumulative distribution of directional asymmetry")
    ax.set_xlabel(r"$| \log(t_{hw} / t_{wh}) |$")
    ax.set_ylabel("Weighted cumulative share")
    ax.legend(frameon=False, loc="lower right")
    fig.tight_layout()
    fig.savefig(out_path, dpi=220)
    plt.close(fig)


def plot_city_asymmetry(city_summary: pd.DataFrame, out_path: Path):
    top = city_summary.head(20).sort_values("weighted_median_am_abs_log_ratio", ascending=True)
    fig, ax = plt.subplots(figsize=(8.6, 7.4))
    ax.barh(top["city_label"], top["weighted_median_am_abs_log_ratio"], color="#c44e52", alpha=0.85)
    ax.set_title("Top cities by AM directional asymmetry")
    ax.set_xlabel(r"Weighted median $| \log(t_{hw} / t_{wh}) |$")
    ax.set_ylabel("")
    fig.tight_layout()
    fig.savefig(out_path, dpi=220)
    plt.close(fig)


def plot_prevalence_scatter(city_summary: pd.DataFrame, out_path: Path):
    fig, ax = plt.subplots(figsize=(7.4, 6.0))
    x = city_summary["weighted_share_am_ratio_ge_1_10"]
    y = city_summary["weighted_share_am_commute_slower"]
    ax.scatter(x, y, s=46, color="#c44e52", alpha=0.75)
    for row in city_summary.head(12).itertuples(index=False):
        ax.text(row.weighted_share_am_ratio_ge_1_10 + 0.003, row.weighted_share_am_commute_slower + 0.003, row.city_label, fontsize=8)
    ax.set_title("City-level prevalence of AM directional asymmetry")
    ax.set_xlabel("Weighted share with ratio >= 1.10")
    ax.set_ylabel("Weighted share where home-to-work is slower")
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    fig.tight_layout()
    fig.savefig(out_path, dpi=220)
    plt.close(fig)


def plot_tidal_gap_hist(use: pd.DataFrame, out_path: Path):
    fig, ax = plt.subplots(figsize=(8.2, 5.2))
    ax.hist(
        use["tidal_gap_minutes"].to_numpy(),
        bins=40,
        weights=use["pop_total_pair"].to_numpy(),
        density=True,
        color="#55a868",
        alpha=0.7,
        edgecolor="white",
        linewidth=0.4,
    )
    ax.axvline(0.0, color="#222222", linestyle="--", linewidth=1)
    ax.set_title("Extra AM congestion in the commute direction")
    ax.set_xlabel("[(AM - Night) home-to-work] - [(AM - Night) work-to-home], minutes")
    ax.set_ylabel("Weighted density")
    fig.tight_layout()
    fig.savefig(out_path, dpi=220)
    plt.close(fig)


def main():
    args = parse_args()
    config = load_config(args.config)
    version_root = get_version_root(config, args.version_id, args.output_dir)
    dirs = ensure_version_dirs(version_root)
    ensure_mpl_cache(str(version_root.parent))
    configure_plot_fonts()

    pair_summary_path = dirs["metrics"] / "top_commuting_pairs_pair_summary.csv"
    if not pair_summary_path.exists():
        raise FileNotFoundError(f"Missing pair summary: {pair_summary_path}")
    pair_summary = pd.read_csv(pair_summary_path)
    use = prepare_sample(pair_summary)
    if use.empty:
        raise RuntimeError("No usable AM + NIGHT sample found.")

    out_fig_dir = dirs["figures"] / "am_asymmetry_assets"
    out_fig_dir.mkdir(parents=True, exist_ok=True)

    national = build_national_summary(use)
    city = build_city_summary(use)

    national_path = dirs["metrics"] / "am_asymmetry_national_summary.csv"
    city_path = dirs["metrics"] / "am_asymmetry_city_summary.csv"
    use_path = dirs["metrics"] / "am_asymmetry_pair_subset.csv"
    national.to_csv(national_path, index=False)
    city.to_csv(city_path, index=False)
    use.to_csv(use_path, index=False)

    plot_ratio_hist(use, out_fig_dir / "figure_am_vs_night_directional_ratio_hist.png")
    plot_abs_log_cdf(use, out_fig_dir / "figure_am_vs_night_abslog_cdf.png")
    plot_city_asymmetry(city, out_fig_dir / "figure_top20_city_am_asymmetry.png")
    plot_prevalence_scatter(city, out_fig_dir / "figure_city_prevalence_scatter.png")
    plot_tidal_gap_hist(use, out_fig_dir / "figure_am_tidal_gap_hist.png")

    snapshot = {
        "stage": "national_tidal_commuting.generate_am_asymmetry_assets",
        "config_path": config["_config_path"],
        "version_root": str(version_root.resolve()),
        "national_summary_path": str(national_path.resolve()),
        "city_summary_path": str(city_path.resolve()),
        "pair_subset_path": str(use_path.resolve()),
        "figure_dir": str(out_fig_dir.resolve()),
        "usable_pairs": int(len(use)),
    }
    (version_root / "config_snapshot.am_asymmetry_assets.json").write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"[generate_am_asymmetry_assets] wrote national summary to {national_path}")
    print(f"[generate_am_asymmetry_assets] wrote city summary to {city_path}")
    print(f"[generate_am_asymmetry_assets] wrote pair subset to {use_path}")
    print(f"[generate_am_asymmetry_assets] wrote figures to {out_fig_dir}")


if __name__ == "__main__":
    main()
