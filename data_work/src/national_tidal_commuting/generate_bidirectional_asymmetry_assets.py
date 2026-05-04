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
    parser = argparse.ArgumentParser(description="Generate direction-agnostic AM asymmetry assets.")
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
    use["am_fast_sec"] = use[["ab_am_sec", "ba_am_sec"]].min(axis=1)
    use["am_slow_sec"] = use[["ab_am_sec", "ba_am_sec"]].max(axis=1)
    use["night_fast_sec"] = use[["ab_night_sec", "ba_night_sec"]].min(axis=1)
    use["night_slow_sec"] = use[["ab_night_sec", "ba_night_sec"]].max(axis=1)
    use["am_asym_ratio"] = use["am_fast_sec"] / use["am_slow_sec"]
    use["night_asym_ratio"] = use["night_fast_sec"] / use["night_slow_sec"]
    use["am_symmetry_ratio"] = use["am_asym_ratio"]
    use["night_symmetry_ratio"] = use["night_asym_ratio"]
    use["am_abs_log_ratio"] = np.abs(np.log(use["am_asym_ratio"]))
    use["night_abs_log_ratio"] = np.abs(np.log(use["night_asym_ratio"]))
    use["am_gap_minutes"] = (use["am_slow_sec"] - use["am_fast_sec"]) / 60.0
    use["night_gap_minutes"] = (use["night_slow_sec"] - use["night_fast_sec"]) / 60.0
    use["excess_gap_minutes"] = use["am_gap_minutes"] - use["night_gap_minutes"]
    use["delta_abs_log_ratio"] = use["am_abs_log_ratio"] - use["night_abs_log_ratio"]
    # Keep the AM direction ordering fixed, then ask whether the same ordering
    # is more or less asymmetric at night.
    use["am_shorter_is_ab"] = use["ab_am_sec"] <= use["ba_am_sec"]
    use["night_ratio_aligned_to_am"] = np.where(
        use["am_shorter_is_ab"],
        use["ab_night_sec"] / use["ba_night_sec"],
        use["ba_night_sec"] / use["ab_night_sec"],
    )
    use["am_over_night_aligned_ratio"] = use["am_asym_ratio"] / use["night_ratio_aligned_to_am"]
    use["night_over_am_aligned_ratio"] = use["night_ratio_aligned_to_am"] / use["am_asym_ratio"]
    use["aligned_ratio_gap"] = use["night_ratio_aligned_to_am"] - use["am_asym_ratio"]
    use["am_more_asymmetric_than_night"] = (use["am_over_night_aligned_ratio"] < 1.0).astype(float)
    use["am_much_more_asymmetric_than_night"] = (use["am_over_night_aligned_ratio"] <= 0.90).astype(float)
    use["am_ratio_le_0_90"] = (use["am_asym_ratio"] <= 1 / 1.10).astype(float)
    use["am_ratio_le_0_83"] = (use["am_asym_ratio"] <= 1 / 1.20).astype(float)
    use["am_ratio_le_0_77"] = (use["am_asym_ratio"] <= 1 / 1.30).astype(float)
    use["night_ratio_le_0_90"] = (use["night_asym_ratio"] <= 1 / 1.10).astype(float)
    use["night_ratio_le_0_83"] = (use["night_asym_ratio"] <= 1 / 1.20).astype(float)
    use["night_ratio_le_0_77"] = (use["night_asym_ratio"] <= 1 / 1.30).astype(float)
    return use


def with_sample_groups(use: pd.DataFrame) -> pd.DataFrame:
    frames = []
    specs = [
        ("union80", pd.Series(True, index=use.index), "Top50 + long-distance union"),
        ("top50", use["selected_by_top50"].fillna(False).astype(bool), "Top 50 by commuter volume"),
        ("longdist30", use["selected_by_longdist_top30"].fillna(False).astype(bool), "Top 30 above city-radius threshold"),
        (
            "longdist30_only",
            use["selected_by_longdist_top30"].fillna(False).astype(bool) & ~use["selected_by_top50"].fillna(False).astype(bool),
            "Long-distance supplement only",
        ),
    ]
    for sample_group, mask, sample_label in specs:
        sub = use[mask].copy()
        if sub.empty:
            continue
        sub["sample_group"] = sample_group
        sub["sample_label"] = sample_label
        frames.append(sub)
    return pd.concat(frames, ignore_index=True)


def summarize_subset(sub: pd.DataFrame) -> dict:
    w = sub["pop_total_pair"]
    return {
        "pair_count": int(len(sub)),
        "weighted_mean_am_minmax_ratio": weighted_average(sub["am_asym_ratio"], w),
        "weighted_median_am_minmax_ratio": weighted_median(sub["am_asym_ratio"], w),
        "weighted_mean_am_symmetry_ratio": weighted_average(sub["am_symmetry_ratio"], w),
        "weighted_median_am_symmetry_ratio": weighted_median(sub["am_symmetry_ratio"], w),
        "weighted_mean_am_abs_log_ratio": weighted_average(sub["am_abs_log_ratio"], w),
        "weighted_median_am_abs_log_ratio": weighted_median(sub["am_abs_log_ratio"], w),
        "weighted_mean_night_minmax_ratio": weighted_average(sub["night_asym_ratio"], w),
        "weighted_median_night_minmax_ratio": weighted_median(sub["night_asym_ratio"], w),
        "weighted_mean_night_symmetry_ratio": weighted_average(sub["night_symmetry_ratio"], w),
        "weighted_median_night_symmetry_ratio": weighted_median(sub["night_symmetry_ratio"], w),
        "weighted_mean_night_abs_log_ratio": weighted_average(sub["night_abs_log_ratio"], w),
        "weighted_median_night_abs_log_ratio": weighted_median(sub["night_abs_log_ratio"], w),
        "weighted_mean_night_ratio_aligned_to_am": weighted_average(sub["night_ratio_aligned_to_am"], w),
        "weighted_median_night_ratio_aligned_to_am": weighted_median(sub["night_ratio_aligned_to_am"], w),
        "weighted_mean_am_over_night_aligned_ratio": weighted_average(sub["am_over_night_aligned_ratio"], w),
        "weighted_median_am_over_night_aligned_ratio": weighted_median(sub["am_over_night_aligned_ratio"], w),
        "weighted_mean_night_over_am_aligned_ratio": weighted_average(sub["night_over_am_aligned_ratio"], w),
        "weighted_median_night_over_am_aligned_ratio": weighted_median(sub["night_over_am_aligned_ratio"], w),
        "weighted_mean_aligned_ratio_gap": weighted_average(sub["aligned_ratio_gap"], w),
        "weighted_median_aligned_ratio_gap": weighted_median(sub["aligned_ratio_gap"], w),
        "weighted_share_am_more_asymmetric_than_night": weighted_average(sub["am_more_asymmetric_than_night"], w),
        "weighted_share_am_much_more_asymmetric_than_night": weighted_average(sub["am_much_more_asymmetric_than_night"], w),
        "weighted_share_am_ratio_le_0_90": weighted_average(sub["am_ratio_le_0_90"], w),
        "weighted_share_am_ratio_le_0_83": weighted_average(sub["am_ratio_le_0_83"], w),
        "weighted_share_am_ratio_le_0_77": weighted_average(sub["am_ratio_le_0_77"], w),
        "weighted_share_night_ratio_le_0_90": weighted_average(sub["night_ratio_le_0_90"], w),
        "weighted_share_night_ratio_le_0_83": weighted_average(sub["night_ratio_le_0_83"], w),
        "weighted_share_night_ratio_le_0_77": weighted_average(sub["night_ratio_le_0_77"], w),
        "weighted_mean_am_gap_minutes": weighted_average(sub["am_gap_minutes"], w),
        "weighted_median_am_gap_minutes": weighted_median(sub["am_gap_minutes"], w),
        "weighted_mean_night_gap_minutes": weighted_average(sub["night_gap_minutes"], w),
        "weighted_median_night_gap_minutes": weighted_median(sub["night_gap_minutes"], w),
        "weighted_mean_excess_gap_minutes": weighted_average(sub["excess_gap_minutes"], w),
        "weighted_median_excess_gap_minutes": weighted_median(sub["excess_gap_minutes"], w),
        "weighted_mean_delta_abs_log_ratio": weighted_average(sub["delta_abs_log_ratio"], w),
        "weighted_median_delta_abs_log_ratio": weighted_median(sub["delta_abs_log_ratio"], w),
    }


def build_national_summary(use: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for sample_group, grp in use.groupby("sample_group", sort=False):
        row = {"sample_group": sample_group, "sample_label": grp["sample_label"].iloc[0]}
        row.update(summarize_subset(grp))
        rows.append(row)
    return pd.DataFrame(rows)


def build_city_summary(use: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (sample_group, city_id, city_label), grp in use.groupby(["sample_group", "city_id", "city_label"], sort=False):
        row = {
            "sample_group": sample_group,
            "sample_label": grp["sample_label"].iloc[0],
            "city_id": city_id,
            "city_label": city_label,
        }
        row.update(summarize_subset(grp))
        rows.append(row)
    out = pd.DataFrame(rows)
    out["rank_weighted_median_am_minmax_ratio"] = (
        out.groupby("sample_group")["weighted_median_am_minmax_ratio"].rank(ascending=True, method="first").astype(int)
    )
    return out.sort_values(["sample_group", "weighted_median_am_minmax_ratio"], ascending=[True, True]).reset_index(drop=True)


def _weighted_hist(ax, values: pd.Series, weights: pd.Series, *, bins: np.ndarray, color: str, label: str, alpha: float):
    ax.hist(
        values.to_numpy(),
        bins=bins,
        weights=weights.to_numpy(),
        density=True,
        color=color,
        alpha=alpha,
        label=label,
        edgecolor="white",
        linewidth=0.45,
    )


def plot_am_ratio_hist(use: pd.DataFrame, out_path: Path):
    fig, axes = plt.subplots(1, 2, figsize=(12.0, 4.8), sharey=True)
    bins = np.linspace(0.35, 1.0, 30)
    panels = [("top50", "Top50"), ("longdist30", "Long-distance 30")]
    for ax, (sample_group, title) in zip(axes, panels):
        sub = use[use["sample_group"] == sample_group].copy()
        if sub.empty:
            continue
        _weighted_hist(ax, sub["am_asym_ratio"], sub["pop_total_pair"], bins=bins, color="#c44e52", label="AM", alpha=0.60)
        _weighted_hist(ax, sub["night_asym_ratio"], sub["pop_total_pair"], bins=bins, color="#4c72b0", label="Night", alpha=0.45)
        ax.axvline(1.0, color="#222222", linestyle="--", linewidth=1)
        ax.set_title(title)
        ax.set_xlabel("faster direction / slower direction")
    axes[0].set_ylabel("Weighted density")
    axes[0].legend(frameon=False)
    fig.suptitle("Direction-agnostic asymmetry ratio distribution", y=1.02)
    fig.tight_layout()
    fig.savefig(out_path, dpi=220)
    plt.close(fig)


def plot_symmetry_cdf(use: pd.DataFrame, out_path: Path):
    fig, axes = plt.subplots(1, 2, figsize=(12.0, 4.8), sharey=True)
    panels = [("top50", "Top50"), ("longdist30", "Long-distance 30")]
    for ax, (sample_group, title) in zip(axes, panels):
        sub = use[use["sample_group"] == sample_group].copy()
        if sub.empty:
            continue
        for col, color, label in [
            ("am_symmetry_ratio", "#c44e52", "AM"),
            ("night_symmetry_ratio", "#4c72b0", "Night"),
        ]:
            cur = sub[[col, "pop_total_pair"]].dropna().sort_values(col)
            x = cur[col].to_numpy()
            w = cur["pop_total_pair"].to_numpy()
            y = np.cumsum(w) / np.sum(w)
            ax.plot(x, y, color=color, linewidth=2.1, label=label)
        ax.set_title(title)
        ax.set_xlabel("faster direction / slower direction")
        ax.set_xlim(0.35, 1.0)
    axes[0].set_ylabel("Weighted cumulative share")
    axes[0].legend(frameon=False, loc="lower right")
    fig.suptitle("Direction-agnostic symmetry ratio CDF", y=1.02)
    fig.tight_layout()
    fig.savefig(out_path, dpi=220)
    plt.close(fig)


def plot_city_bar(city_summary: pd.DataFrame, out_path: Path):
    fig, axes = plt.subplots(1, 2, figsize=(13.0, 7.2), sharex=False)
    panels = [("top50", "Top50"), ("longdist30", "Long-distance 30")]
    for ax, (sample_group, title) in zip(axes, panels):
        sub = city_summary[city_summary["sample_group"] == sample_group].copy()
        sub = sub.sort_values("weighted_median_am_minmax_ratio", ascending=True).head(15).sort_values("weighted_median_am_minmax_ratio")
        ax.barh(sub["city_label"], sub["weighted_median_am_minmax_ratio"], color="#c44e52", alpha=0.85)
        ax.set_title(title)
        ax.set_xlabel("Weighted median faster/slower ratio")
    fig.suptitle("Cities with the strongest AM directional asymmetry", y=1.02)
    fig.tight_layout()
    fig.savefig(out_path, dpi=220)
    plt.close(fig)


def plot_city_scatter(city_summary: pd.DataFrame, out_path: Path):
    wide = (
        city_summary[city_summary["sample_group"].isin(["top50", "longdist30"])]
        .pivot(index=["city_id", "city_label"], columns="sample_group", values="weighted_median_am_minmax_ratio")
        .reset_index()
        .dropna()
    )
    fig, ax = plt.subplots(figsize=(7.0, 6.4))
    ax.scatter(wide["top50"], wide["longdist30"], color="#2b6cb0", alpha=0.78, s=42)
    hi = wide.nsmallest(10, "longdist30")
    for row in hi.itertuples(index=False):
        ax.text(row.top50 + 0.003, row.longdist30 + 0.003, row.city_label, fontsize=8)
    low = min(wide["top50"].min(), wide["longdist30"].min())
    high = max(wide["top50"].max(), wide["longdist30"].max())
    ax.plot([low, high], [low, high], linestyle="--", color="#666666", linewidth=1)
    ax.set_xlabel("City weighted median AM min/max ratio, Top50")
    ax.set_ylabel("City weighted median AM min/max ratio, Long-distance 30")
    ax.set_title("City asymmetry in the two sampling schemes")
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
    use = with_sample_groups(prepare_sample(pair_summary))
    national = build_national_summary(use)
    city = build_city_summary(use)

    out_fig_dir = dirs["figures"] / "bidirectional_asymmetry_assets"
    out_fig_dir.mkdir(parents=True, exist_ok=True)
    pair_path = dirs["metrics"] / "am_bidirectional_asymmetry_pair_subset.csv"
    national_path = dirs["metrics"] / "am_bidirectional_asymmetry_national_summary.csv"
    city_path = dirs["metrics"] / "am_bidirectional_asymmetry_city_summary.csv"

    use.to_csv(pair_path, index=False)
    national.to_csv(national_path, index=False)
    city.to_csv(city_path, index=False)

    plot_am_ratio_hist(use, out_fig_dir / "figure_am_asymmetry_ratio_hist_by_sample.png")
    plot_symmetry_cdf(use, out_fig_dir / "figure_am_symmetry_cdf_by_sample.png")
    plot_city_bar(city, out_fig_dir / "figure_top_cities_am_asymmetry_by_sample.png")
    plot_city_scatter(city, out_fig_dir / "figure_city_top50_vs_longdist30_scatter.png")

    manifest = {
        "stage": "national_tidal_commuting.generate_bidirectional_asymmetry_assets",
        "version_root": str(version_root.resolve()),
        "pair_summary_path": str(pair_summary_path.resolve()),
        "national_summary_path": str(national_path.resolve()),
        "city_summary_path": str(city_path.resolve()),
        "pair_subset_path": str(pair_path.resolve()),
        "figure_dir": str(out_fig_dir.resolve()),
    }
    (version_root / "config_snapshot.bidirectional_asymmetry_assets.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"[generate_bidirectional_asymmetry_assets] wrote national summary to {national_path}")
    print(f"[generate_bidirectional_asymmetry_assets] wrote city summary to {city_path}")
    print(f"[generate_bidirectional_asymmetry_assets] wrote pair subset to {pair_path}")
    print(f"[generate_bidirectional_asymmetry_assets] wrote figures to {out_fig_dir}")


if __name__ == "__main__":
    main()
