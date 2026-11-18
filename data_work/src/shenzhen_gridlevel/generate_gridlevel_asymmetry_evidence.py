"""Generate grid-level directional-asymmetry evidence for Shenzhen.

The script treats the fishnet1000 pair-time matrix as a static within-city
OD travel-cost layer. It uses `car_time` as the main travel-time measure,
constructs reciprocal unordered grid pairs, and writes figures and metrics
that can be compared with existing township-level and segment-level evidence.

Raw data are read-only.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


EPS = 1e-12
RATIO_THRESHOLDS = [0.90, 0.83, 0.77, 0.67, 0.50]


def write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False, default=float))


def ensure_dirs(root: Path) -> None:
    for sub in ["data", "figures", "metrics"]:
        (root / sub).mkdir(parents=True, exist_ok=True)


def threshold_label(thr: float) -> str:
    return f"{thr:.2f}".replace(".", "_")


def summarize_numeric(s: pd.Series, prefix: str) -> dict[str, float | int]:
    clean = s.replace([np.inf, -np.inf], np.nan).dropna()
    qs = clean.quantile([0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99])
    out: dict[str, float | int] = {
        f"{prefix}_count": int(clean.count()),
        f"{prefix}_mean": float(clean.mean()),
        f"{prefix}_std": float(clean.std()),
        f"{prefix}_min": float(clean.min()),
        f"{prefix}_max": float(clean.max()),
    }
    out.update({f"{prefix}_p{int(q * 100):02d}": float(v) for q, v in qs.items()})
    return out


def build_reciprocal_pairs(raw_path: Path) -> pd.DataFrame:
    cols = ["fid_home", "fid_work", "car_time", "car_dist"]
    df = pd.read_csv(raw_path, usecols=cols)
    reverse = df.rename(
        columns={
            "fid_home": "fid_work",
            "fid_work": "fid_home",
            "car_time": "car_time_reverse",
            "car_dist": "car_dist_reverse",
        }
    )
    paired = df.merge(reverse, on=["fid_home", "fid_work"], how="inner", validate="one_to_one")
    paired = paired.loc[paired["fid_home"] < paired["fid_work"]].copy()
    paired = paired.loc[
        (paired["car_time"] >= 1.0) &
        (paired["car_time_reverse"] >= 1.0)
    ].copy()

    t1 = paired["car_time"].to_numpy(float)
    t2 = paired["car_time_reverse"].to_numpy(float)
    d1 = paired["car_dist"].to_numpy(float)
    d2 = paired["car_dist_reverse"].to_numpy(float)

    paired["fast_time_min"] = np.minimum(t1, t2)
    paired["slow_time_min"] = np.maximum(t1, t2)
    paired["slow_over_fast_time"] = paired["slow_time_min"] / np.clip(paired["fast_time_min"], EPS, None)
    paired["symmetry_ratio_time"] = paired["fast_time_min"] / np.clip(paired["slow_time_min"], EPS, None)
    paired["abs_log_time_ratio"] = np.abs(np.log(np.clip(t1, EPS, None) / np.clip(t2, EPS, None)))
    paired["time_gap_min"] = paired["slow_time_min"] - paired["fast_time_min"]
    paired["slower_direction"] = np.where(t1 >= t2, "home_to_work", "work_to_home")

    paired["mean_dist_km"] = (d1 + d2) / 2.0
    paired["min_dist_km"] = np.minimum(d1, d2)
    paired["max_dist_km"] = np.maximum(d1, d2)
    paired["distance_ratio"] = paired["min_dist_km"] / np.clip(paired["max_dist_km"], EPS, None)
    return paired


def make_threshold_table(pairs: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for thr in RATIO_THRESHOLDS:
        rows.append(
            {
                "metric": "symmetry_ratio_time",
                "threshold": thr,
                "direction": "<=",
                "equivalent_slow_over_fast": 1.0 / thr,
                "pair_count": int(len(pairs)),
                "share": float((pairs["symmetry_ratio_time"] <= thr).mean()),
                "count": int((pairs["symmetry_ratio_time"] <= thr).sum()),
            }
        )
    for thr in [1.10, 1.25, 1.50, 2.00]:
        rows.append(
            {
                "metric": "slow_over_fast_time",
                "threshold": thr,
                "direction": ">=",
                "equivalent_slow_over_fast": thr,
                "pair_count": int(len(pairs)),
                "share": float((pairs["slow_over_fast_time"] >= thr).mean()),
                "count": int((pairs["slow_over_fast_time"] >= thr).sum()),
            }
        )
    return pd.DataFrame(rows)


def make_distance_bin_table(pairs: pd.DataFrame) -> pd.DataFrame:
    bins = [0, 5, 10, 20, 40, 80, np.inf]
    labels = ["0-5", "5-10", "10-20", "20-40", "40-80", "80+"]
    tmp = pairs.copy()
    tmp["mean_dist_bin_km"] = pd.cut(
        tmp["mean_dist_km"],
        bins=bins,
        labels=labels,
        right=False,
        include_lowest=True,
    )
    grouped = tmp.groupby("mean_dist_bin_km", observed=False)
    out = grouped.agg(
        pair_count=("symmetry_ratio_time", "size"),
        median_symmetry_ratio=("symmetry_ratio_time", "median"),
        p25_symmetry_ratio=("symmetry_ratio_time", lambda x: x.quantile(0.25)),
        p75_symmetry_ratio=("symmetry_ratio_time", lambda x: x.quantile(0.75)),
        mean_symmetry_ratio=("symmetry_ratio_time", "mean"),
        median_slow_over_fast=("slow_over_fast_time", "median"),
        mean_time_gap_min=("time_gap_min", "mean"),
        median_time_gap_min=("time_gap_min", "median"),
        mean_dist_km=("mean_dist_km", "mean"),
    ).reset_index()
    for thr in RATIO_THRESHOLDS:
        out[f"share_symmetry_le_{threshold_label(thr)}"] = grouped["symmetry_ratio_time"].apply(
            lambda x, threshold=thr: float((x <= threshold).mean())
        ).to_numpy()
    return out


def load_multilevel_bridge(grid_summary: dict, pairs: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    rows.append(
        {
            "level": "Township OD",
            "sample": "National top-50 city-pair sample, AM",
            "comparison_unit": "commuting OD pair",
            "metric": "weighted AM min/max travel-time ratio",
            "pair_count": np.nan,
            "median_symmetry_ratio": np.nan,
            "share_symmetry_le_0_90": np.nan,
            "share_symmetry_le_0_83": np.nan,
            "share_symmetry_le_0_77": np.nan,
            "source": "data_work/outputs/national_tidal_commuting_top50cities_2021_v1/metrics/am_bidirectional_asymmetry_national_summary.csv",
            "notes": "Loaded when available.",
        }
    )

    township_path = Path(
        "data_work/outputs/national_tidal_commuting_top50cities_2021_v1/"
        "metrics/am_bidirectional_asymmetry_national_summary.csv"
    )
    if township_path.exists():
        t = pd.read_csv(township_path)
        if "sample_group" in t.columns and (t["sample_group"] == "top50").any():
            r = t.loc[t["sample_group"] == "top50"].iloc[0]
        else:
            r = t.iloc[0]
        rows[-1].update(
            {
                "pair_count": int(r["pair_count"]),
                "median_symmetry_ratio": float(r["weighted_median_am_minmax_ratio"]),
                "share_symmetry_le_0_90": float(r["weighted_share_am_ratio_le_0_90"]),
                "share_symmetry_le_0_83": float(r["weighted_share_am_ratio_le_0_83"]),
                "share_symmetry_le_0_77": float(r["weighted_share_am_ratio_le_0_77"]),
            }
        )

    rows.append(
        {
            "level": "Grid OD",
            "sample": "Shenzhen fishnet1000 all reciprocal pairs",
            "comparison_unit": "within-city grid pair",
            "metric": "unweighted min/max car-time ratio",
            "pair_count": int(grid_summary["n_valid_reciprocal_pairs"]),
            "median_symmetry_ratio": float(grid_summary["symmetry_ratio_time_p50"]),
            "share_symmetry_le_0_90": float((pairs["symmetry_ratio_time"] <= 0.90).mean()),
            "share_symmetry_le_0_83": float((pairs["symmetry_ratio_time"] <= 0.83).mean()),
            "share_symmetry_le_0_77": float((pairs["symmetry_ratio_time"] <= 0.77).mean()),
            "source": "data_work/raw_data/shenzhen_gridlevel/fishnet1000_pairtime_hat.csv",
            "notes": "Static all-pair OD travel-time matrix, not flow-weighted.",
        }
    )

    adjacent_path = Path(
        "data_work/outputs/raw_connected_v2_grid_travel_time_edgeproj_v1/"
        "metrics/grid_tidal_asymmetry/raw_grid_tidal_asymmetry_summary_all.csv"
    )
    adjacent_pair_path = Path(
        "data_work/outputs/raw_connected_v2_grid_travel_time_edgeproj_v1/"
        "data/grid_tidal_asymmetry/raw_grid_tidal_asymmetry_pairs_square_AM.csv"
    )
    if adjacent_path.exists():
        a = pd.read_csv(adjacent_path)
        a = a.loc[(a["grid_type"] == "square") & (a["period"] == "AM")]
        if not a.empty:
            r = a.iloc[0]
            share_083 = np.nan
            share_077 = np.nan
            if adjacent_pair_path.exists():
                ap = pd.read_csv(adjacent_pair_path)
                ap = ap.loc[ap["ratio"].notna()].copy()
                if not ap.empty:
                    share_083 = float((ap["ratio"] <= 0.83).mean())
                    share_077 = float((ap["ratio"] <= 0.77).mean())
            rows.append(
                {
                    "level": "Adjacent Grid",
                    "sample": "Beijing raw-connected square cells, AM",
                    "comparison_unit": "adjacent grid pair",
                    "metric": "unweighted directional travel-time ratio",
                    "pair_count": int(r["undirected_pairs_both_dir"]),
                    "median_symmetry_ratio": float(r["median_directional_ratio"]),
                    "share_symmetry_le_0_90": float(r["share_asym_lt_0_9"]),
                    "share_symmetry_le_0_83": share_083,
                    "share_symmetry_le_0_77": share_077,
                    "source": str(adjacent_path),
                    "notes": "Road-network adjacent-cell evidence, city-specific.",
                }
            )

    segment_path = Path("data_work/outputs/manual_centerline_rules_v11/data/centerline_asymmetry_table.csv")
    if segment_path.exists():
        seg = pd.read_csv(segment_path)
        seg = seg.loc[(seg["peak"] == "AM") & seg["ratio"].notna()].copy()
        if not seg.empty:
            rows.append(
                {
                    "level": "Segment",
                    "sample": "Beijing centerline segments, AM",
                    "comparison_unit": "road segment direction pair",
                    "metric": "unweighted min/max directional speed ratio",
                    "pair_count": int(len(seg)),
                    "median_symmetry_ratio": float(seg["ratio"].median()),
                    "share_symmetry_le_0_90": float((seg["ratio"] <= 0.90).mean()),
                    "share_symmetry_le_0_83": float((seg["ratio"] <= 0.83).mean()),
                    "share_symmetry_le_0_77": float((seg["ratio"] <= 0.77).mean()),
                    "source": str(segment_path),
                    "notes": "Speed ratio rather than time ratio. Lower values still mean stronger directionality.",
                }
            )
    return pd.DataFrame(rows)


def save_histogram(pairs: pd.DataFrame, out: Path) -> None:
    fig, ax = plt.subplots(figsize=(8.2, 5.2))
    ax.hist(
        pairs["symmetry_ratio_time"],
        bins=np.linspace(0.35, 1.0, 66),
        color="#3A6EA5",
        edgecolor="white",
        linewidth=0.3,
    )
    for thr, color in [(0.90, "#8F2D56"), (0.83, "#C44536"), (0.77, "#D99B2B"), (0.67, "#3B7A57"), (0.50, "#313638")]:
        ax.axvline(thr, color=color, linewidth=1.2, linestyle="--")
        ax.text(thr, ax.get_ylim()[1] * 0.92, f"{thr:.2f}", rotation=90, va="top", ha="right", fontsize=9, color=color)
    ax.set_title("Shenzhen Grid-Level Directional Asymmetry")
    ax.set_xlabel("Symmetry ratio, fast car time / slow car time")
    ax.set_ylabel("Reciprocal grid-pair count")
    ax.text(0.36, ax.get_ylim()[1] * 0.82, "Lower values mean stronger directional congestion", fontsize=9)
    fig.tight_layout()
    fig.savefig(out / "figures" / "figure_gridlevel_symmetry_ratio_hist.png", dpi=220)
    plt.close(fig)


def save_distance_bin_figure(bin_table: pd.DataFrame, out: Path) -> None:
    plot_df = bin_table.loc[bin_table["pair_count"] > 0].copy()
    x = np.arange(len(plot_df))
    fig, ax1 = plt.subplots(figsize=(8.8, 5.2))
    bars = ax1.bar(
        x,
        plot_df["share_symmetry_le_0_90"],
        color="#E7B65A",
        width=0.72,
        label="Share with ratio <= 0.90",
    )
    ax1.set_ylim(0, 1)
    ax1.set_ylabel("Share asymmetric at 0.90 threshold")
    ax1.set_xticks(x)
    ax1.set_xticklabels(plot_df["mean_dist_bin_km"].astype(str))
    ax1.set_xlabel("Mean OD distance bin (km)")
    ax2 = ax1.twinx()
    ax2.plot(
        x,
        plot_df["median_symmetry_ratio"],
        color="#1D3557",
        marker="o",
        linewidth=2,
        label="Median symmetry ratio",
    )
    ax2.set_ylim(0.72, 0.96)
    ax2.set_ylabel("Median symmetry ratio")
    ax1.bar_label(bars, labels=[f"{v:.0%}" for v in plot_df["share_symmetry_le_0_90"]], padding=2, fontsize=8)
    handles1, labels1 = ax1.get_legend_handles_labels()
    handles2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(handles1 + handles2, labels1 + labels2, loc="lower right", frameon=False)
    ax1.set_title("Grid-Level Asymmetry by OD Distance")
    fig.tight_layout()
    fig.savefig(out / "figures" / "figure_gridlevel_asymmetry_by_distance_bin.png", dpi=220)
    plt.close(fig)


def save_threshold_curve(pairs: pd.DataFrame, out: Path) -> pd.DataFrame:
    thresholds = np.round(np.arange(1.00, 2.01, 0.05), 2)
    curve = pd.DataFrame(
        {
            "slow_over_fast_threshold": thresholds,
            "share_pairs_at_or_above": [
                float((pairs["slow_over_fast_time"] >= threshold).mean())
                for threshold in thresholds
            ],
        }
    )
    curve.to_csv(out / "metrics" / "gridlevel_slow_over_fast_threshold_curve.csv", index=False)
    fig, ax = plt.subplots(figsize=(8.0, 5.0))
    ax.plot(
        curve["slow_over_fast_threshold"],
        curve["share_pairs_at_or_above"],
        color="#2F6F73",
        linewidth=2.2,
    )
    ax.scatter([1.10, 1.25, 1.50, 2.00], [
        float((pairs["slow_over_fast_time"] >= threshold).mean())
        for threshold in [1.10, 1.25, 1.50, 2.00]
    ], color="#B23A48", zorder=3)
    ax.set_ylim(0, 1)
    ax.set_xlim(1.0, 2.0)
    ax.set_xlabel("Slow direction / fast direction car time")
    ax.set_ylabel("Share of reciprocal grid pairs")
    ax.set_title("How Prevalent Is Directional Congestion?")
    ax.grid(axis="y", alpha=0.25)
    fig.tight_layout()
    fig.savefig(out / "figures" / "figure_gridlevel_threshold_curve.png", dpi=220)
    plt.close(fig)
    return curve


def save_scatter(pairs: pd.DataFrame, out: Path) -> None:
    sample = pairs.sample(n=min(120_000, len(pairs)), random_state=20260628)
    fig, ax = plt.subplots(figsize=(6.4, 6.0))
    ax.hexbin(
        sample["car_time"],
        sample["car_time_reverse"],
        gridsize=65,
        bins="log",
        cmap="viridis",
        mincnt=1,
    )
    max_lim = float(np.nanpercentile(sample[["car_time", "car_time_reverse"]].to_numpy(), 99.5))
    max_lim = max(max_lim, 10.0)
    ax.plot([0, max_lim], [0, max_lim], color="white", linewidth=1.2)
    ax.plot([0, max_lim], [0, max_lim / 1.25], color="#E8C547", linewidth=0.9, linestyle="--")
    ax.plot([0, max_lim / 1.25], [0, max_lim], color="#E8C547", linewidth=0.9, linestyle="--")
    ax.set_xlim(0, max_lim)
    ax.set_ylim(0, max_lim)
    ax.set_xlabel("Home to work car time (min)")
    ax.set_ylabel("Work to home car time (min)")
    ax.set_title("Reciprocal Grid-Pair Travel Times")
    fig.tight_layout()
    fig.savefig(out / "figures" / "figure_gridlevel_reciprocal_time_hexbin.png", dpi=220)
    plt.close(fig)


def save_multilevel_figure(bridge: pd.DataFrame, out: Path) -> None:
    plot_df = bridge.dropna(subset=["median_symmetry_ratio"]).copy()
    plot_df["order"] = plot_df["level"].map(
        {"Township OD": 0, "Grid OD": 1, "Adjacent Grid": 2, "Segment": 3}
    ).fillna(9)
    plot_df = plot_df.sort_values("order")
    y = np.arange(len(plot_df))
    fig, ax = plt.subplots(figsize=(9.2, 5.2))
    ax.scatter(plot_df["median_symmetry_ratio"], y, s=90, color="#2A5C7A", zorder=3, label="Median symmetry ratio")
    for _, r in plot_df.iterrows():
        ax.hlines(
            y=int(r["order"]),
            xmin=0.50,
            xmax=float(r["median_symmetry_ratio"]),
            color="#AFC7D5",
            linewidth=2,
            zorder=1,
        )
    for idx, r in plot_df.reset_index(drop=True).iterrows():
        ax.text(
            float(r["median_symmetry_ratio"]) + 0.01,
            idx,
            f"{float(r['median_symmetry_ratio']):.3f}",
            va="center",
            fontsize=9,
        )
    ax.set_yticks(y)
    ax.set_yticklabels(plot_df["level"] + "\n" + plot_df["sample"])
    ax.set_xlim(0.50, 0.96)
    ax.set_xlabel("Median symmetry ratio, lower means more directional asymmetry")
    ax.set_title("Directional Asymmetry Across Spatial Layers")
    ax.axvline(0.90, color="#B23A48", linewidth=1.0, linestyle="--")
    ax.text(0.902, len(plot_df) - 0.3, "0.90", color="#B23A48", fontsize=9)
    ax.grid(axis="x", alpha=0.25)
    fig.tight_layout()
    fig.savefig(out / "figures" / "figure_multilevel_asymmetry_bridge.png", dpi=220)
    plt.close(fig)


def save_multilevel_threshold_figure(bridge: pd.DataFrame, out: Path) -> None:
    plot_df = bridge.copy()
    plot_df["order"] = plot_df["level"].map(
        {"Township OD": 0, "Grid OD": 1, "Adjacent Grid": 2, "Segment": 3}
    ).fillna(9)
    plot_df = plot_df.sort_values("order")
    cols = ["share_symmetry_le_0_90", "share_symmetry_le_0_83", "share_symmetry_le_0_77"]
    labels = ["<= 0.90", "<= 0.83", "<= 0.77"]
    colors = ["#4C78A8", "#F58518", "#54A24B"]
    x = np.arange(len(plot_df))
    width = 0.23
    fig, ax = plt.subplots(figsize=(9.6, 5.4))
    for i, (col, label, color) in enumerate(zip(cols, labels, colors)):
        values = plot_df[col].astype(float).to_numpy()
        ax.bar(x + (i - 1) * width, values, width=width, color=color, label=label)
    ax.set_ylim(0, 0.80)
    ax.set_ylabel("Share of pairs below threshold")
    ax.set_xticks(x)
    ax.set_xticklabels(plot_df["level"], rotation=0)
    ax.set_title("How Often Does Directional Asymmetry Cross Common Thresholds?")
    ax.legend(title="Symmetry ratio", frameon=False, ncols=3, loc="upper right")
    ax.grid(axis="y", alpha=0.25)
    for i, col in enumerate(cols):
        for j, value in enumerate(plot_df[col].astype(float).to_numpy()):
            if np.isfinite(value):
                ax.text(
                    j + (i - 1) * width,
                    value + 0.015,
                    f"{value:.0%}",
                    ha="center",
                    va="bottom",
                    fontsize=8,
                )
    fig.tight_layout()
    fig.savefig(out / "figures" / "figure_multilevel_asymmetry_threshold_shares.png", dpi=220)
    plt.close(fig)


def write_readme(out: Path, summary: dict) -> None:
    lines = [
        "# Shenzhen Grid-Level Asymmetry Evidence",
        "",
        "This retained run constructs within-city directional-asymmetry evidence from the Shenzhen fishnet1000 pair-time matrix.",
        "",
        "## Main inputs",
        "",
        "- Raw matrix: `data_work/raw_data/shenzhen_gridlevel/fishnet1000_pairtime_hat.csv`",
        "- Main travel-time field: `car_time`",
        "- Unit of analysis: unordered reciprocal grid pair with both directed travel times at least 1 minute",
        "",
        "## Core measures",
        "",
        "- `slow_over_fast_time`: slower directed car time divided by faster directed car time.",
        "- `symmetry_ratio_time`: faster directed car time divided by slower directed car time. Lower values mean stronger directional asymmetry.",
        "- `time_gap_min`: slower directed car time minus faster directed car time.",
        "",
        "## Headline numbers",
        "",
        f"- Valid reciprocal grid pairs: {summary['n_valid_reciprocal_pairs']:,}.",
        f"- Median symmetry ratio: {summary['symmetry_ratio_time_p50']:.3f}.",
        f"- Share with symmetry ratio <= 0.90: {summary['share_symmetry_ratio_le_0_90']:.1%}.",
        f"- Share with symmetry ratio <= 0.83: {summary['share_symmetry_ratio_le_0_83']:.1%}.",
        f"- Median slow/fast car-time ratio: {summary['slow_over_fast_time_p50']:.3f}.",
        "",
        "## Caveats",
        "",
        "- The raw folder does not include grid geometry or an identifier crosswalk, so this run does not produce a map.",
        "- The matrix is not flow-weighted. The output characterizes all potential grid pairs, not observed commuter-weighted exposure.",
        "- `car_time_detour` is not used here because the data provider advised using `car_time` as the main field.",
    ]
    (out / "README.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main(argv=None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        default="data_work/raw_data/shenzhen_gridlevel/fishnet1000_pairtime_hat.csv",
    )
    parser.add_argument(
        "--output-root",
        default="data_work/outputs/shenzhen_gridlevel_asymmetry_evidence_v1",
    )
    args = parser.parse_args(argv)

    raw_path = Path(args.input)
    out = Path(args.output_root)
    ensure_dirs(out)

    pairs = build_reciprocal_pairs(raw_path)
    pairs = pairs.sort_values(["fid_home", "fid_work"]).reset_index(drop=True)

    summary: dict[str, float | int | str] = {
        "input_path": str(raw_path),
        "output_root": str(out),
        "main_time_field": "car_time",
        "n_valid_reciprocal_pairs": int(len(pairs)),
    }
    summary.update(summarize_numeric(pairs["symmetry_ratio_time"], "symmetry_ratio_time"))
    summary.update(summarize_numeric(pairs["slow_over_fast_time"], "slow_over_fast_time"))
    summary.update(summarize_numeric(pairs["time_gap_min"], "time_gap_min"))
    summary.update(summarize_numeric(pairs["mean_dist_km"], "mean_dist_km"))
    for thr in RATIO_THRESHOLDS:
        key = threshold_label(thr)
        summary[f"share_symmetry_ratio_le_{key}"] = float((pairs["symmetry_ratio_time"] <= thr).mean())
        summary[f"count_symmetry_ratio_le_{key}"] = int((pairs["symmetry_ratio_time"] <= thr).sum())

    write_json(out / "metrics" / "gridlevel_asymmetry_summary.json", summary)

    threshold_table = make_threshold_table(pairs)
    threshold_table.to_csv(out / "metrics" / "gridlevel_threshold_table.csv", index=False)

    distance_bin_table = make_distance_bin_table(pairs)
    distance_bin_table.to_csv(out / "metrics" / "gridlevel_distance_bin_summary.csv", index=False)

    bridge = load_multilevel_bridge(summary, pairs)
    bridge.to_csv(out / "metrics" / "multilevel_bridge_summary.csv", index=False)

    pairs.sort_values("slow_over_fast_time", ascending=False).head(1000).to_csv(
        out / "data" / "top_gridlevel_asymmetric_pairs.csv",
        index=False,
    )
    pairs.sample(n=min(20000, len(pairs)), random_state=20260628).to_csv(
        out / "data" / "gridlevel_reciprocal_pair_sample.csv",
        index=False,
    )

    save_histogram(pairs, out)
    save_distance_bin_figure(distance_bin_table, out)
    save_threshold_curve(pairs, out)
    save_scatter(pairs, out)
    save_multilevel_figure(bridge, out)
    save_multilevel_threshold_figure(bridge, out)
    write_readme(out, summary)


if __name__ == "__main__":
    main()
