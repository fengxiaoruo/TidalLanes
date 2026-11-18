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
from src.national_tidal_commuting.common import ensure_version_dirs, resolve_path
from src.national_tidal_commuting.generate_center_periphery_commuting import (
    FLOW_ORDER,
    clean_chunk,
    first_existing_path,
    keep_intra_city,
    read_stata_chunks,
)


DEFAULT_VERSION_ID = "national_center_periphery_group_diagnostics_v1"
DEFAULT_BASE_VERSION = "national_center_periphery_commuting_v1"
DEFAULT_THRESHOLDS = [0.20, 0.25, 0.30, 0.40, 0.50]
FIRST_TIER_CITY_KEYS = {"北京市||北京市", "上海市||上海市", "广东省||广州市", "广东省||深圳市"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build threshold and city-group diagnostics for center-periphery commuting.")
    parser.add_argument("--version-id", default=DEFAULT_VERSION_ID)
    parser.add_argument("--base-version-id", default=DEFAULT_BASE_VERSION)
    parser.add_argument("--output-dir", default="data_work/outputs")
    parser.add_argument("--commute-dta-path", default="")
    parser.add_argument("--chunksize", type=int, default=750_000)
    parser.add_argument("--thresholds", default=",".join(f"{x:.2f}" for x in DEFAULT_THRESHOLDS))
    parser.add_argument("--max-chunks", type=int, default=0)
    return parser.parse_args()


def configure_plots(cache_root: Path) -> None:
    ensure_mpl_cache(str(cache_root))
    plt.rcParams.update(
        {
            "font.sans-serif": [
                "PingFang SC",
                "Noto Sans CJK SC",
                "Arial Unicode MS",
                "Helvetica",
                "Arial",
                "DejaVu Sans",
            ],
            "font.family": "sans-serif",
            "axes.unicode_minus": False,
            "figure.dpi": 180,
            "savefig.dpi": 450,
            "axes.linewidth": 0.8,
            "axes.labelsize": 8,
            "axes.titlesize": 9,
            "xtick.labelsize": 7,
            "ytick.labelsize": 7,
            "legend.fontsize": 7,
        }
    )


def parse_thresholds(text: str) -> list[float]:
    values = [float(x.strip()) for x in text.split(",") if x.strip()]
    if not values:
        raise ValueError("No thresholds supplied.")
    bad = [x for x in values if not (0 < x <= 1)]
    if bad:
        raise ValueError(f"Thresholds must be in (0, 1]: {bad}")
    return sorted(set(values))


def classify_flow(origin_center: pd.Series, dest_center: pd.Series) -> pd.Series:
    return np.select(
        [
            origin_center & dest_center,
            origin_center & ~dest_center,
            ~origin_center & dest_center,
            ~origin_center & ~dest_center,
        ],
        FLOW_ORDER,
        default="missing",
    )


def append_threshold_flow_parts(
    intra: pd.DataFrame,
    center_lookups: dict[float, dict[int, bool]],
    parts: list[pd.DataFrame],
) -> None:
    base = intra.loc[~intra["origin_town_code"].eq(intra["dest_town_code"])].copy()
    if base.empty:
        return
    for threshold, lookup in center_lookups.items():
        use = base.copy()
        use["origin_center"] = use["origin_town_code"].map(lookup)
        use["dest_center"] = use["dest_town_code"].map(lookup)
        use = use.loc[use["origin_center"].notna() & use["dest_center"].notna()].copy()
        if use.empty:
            continue
        use["origin_center"] = use["origin_center"].astype(bool)
        use["dest_center"] = use["dest_center"].astype(bool)
        use["flow_type"] = classify_flow(use["origin_center"], use["dest_center"])
        grp = (
            use.groupby(["city_key", "origin_prov_name", "origin_city_name", "flow_type"], as_index=False)[
                "commuters_2021"
            ]
            .sum()
            .rename(
                columns={
                    "origin_prov_name": "prov_name",
                    "origin_city_name": "city_name",
                    "commuters_2021": "commuters",
                }
            )
        )
        grp["core_workplace_share"] = threshold
        parts.append(grp)


def combine_flow_parts(parts: list[pd.DataFrame]) -> pd.DataFrame:
    flow = pd.concat(parts, ignore_index=True)
    flow = flow.groupby(["core_workplace_share", "city_key", "prov_name", "city_name", "flow_type"], as_index=False)[
        "commuters"
    ].sum()
    total = (
        flow.groupby(["core_workplace_share", "city_key"], as_index=False)["commuters"]
        .sum()
        .rename(columns={"commuters": "city_inter_town_commuters"})
    )
    flow = flow.merge(total, on=["core_workplace_share", "city_key"], how="left")
    flow["share_city"] = flow["commuters"] / flow["city_inter_town_commuters"].replace(0.0, np.nan)
    return flow


def build_city_wide(flow: pd.DataFrame) -> pd.DataFrame:
    wide = flow.pivot_table(
        index=["core_workplace_share", "city_key", "prov_name", "city_name", "city_inter_town_commuters"],
        columns="flow_type",
        values="commuters",
        aggfunc="sum",
        fill_value=0.0,
        observed=False,
    ).reset_index()
    for col in FLOW_ORDER:
        if col not in wide.columns:
            wide[col] = 0.0
        wide[f"{col}_share"] = wide[col] / wide["city_inter_town_commuters"].replace(0.0, np.nan)
    wide["P_to_C_over_C_to_P"] = wide["P_to_C"] / wide["C_to_P"].replace(0.0, np.nan)
    wide["P_to_C_minus_C_to_P_share"] = (wide["P_to_C"] - wide["C_to_P"]) / wide["city_inter_town_commuters"].replace(
        0.0, np.nan
    )
    wide["rank_by_inter_town_flow"] = (
        wide.groupby("core_workplace_share")["city_inter_town_commuters"].rank(method="first", ascending=False).astype(int)
    )
    wide["city_group"] = np.select(
        [
            wide["city_key"].isin(FIRST_TIER_CITY_KEYS),
            wide["rank_by_inter_town_flow"].le(20),
            wide["rank_by_inter_town_flow"].le(50),
            wide["rank_by_inter_town_flow"].le(100),
        ],
        ["first_tier_manual", "top20_flow", "top50_flow", "top100_flow"],
        default="other",
    )
    return wide.sort_values(["core_workplace_share", "rank_by_inter_town_flow"])


def summarize_groups(city_wide: pd.DataFrame) -> pd.DataFrame:
    rows = []
    group_specs = [
        ("all_cities", lambda df: pd.Series(True, index=df.index)),
        ("top100_flow", lambda df: df["rank_by_inter_town_flow"].le(100)),
        ("top50_flow", lambda df: df["rank_by_inter_town_flow"].le(50)),
        ("top20_flow", lambda df: df["rank_by_inter_town_flow"].le(20)),
        ("first_tier_manual", lambda df: df["city_key"].isin(FIRST_TIER_CITY_KEYS)),
    ]
    for threshold, data in city_wide.groupby("core_workplace_share", sort=True):
        for group_name, mask_fn in group_specs:
            sub = data.loc[mask_fn(data)].copy()
            if sub.empty:
                continue
            total = float(sub["city_inter_town_commuters"].sum())
            values = {flow_type: float(sub[flow_type].sum()) for flow_type in FLOW_ORDER}
            pc = values["P_to_C"]
            cp = values["C_to_P"]
            row = {
                "core_workplace_share": threshold,
                "city_group": group_name,
                "n_cities": int(len(sub)),
                "inter_town_commuters": total,
                "P_to_C_over_C_to_P": pc / cp if cp > 0 else np.inf,
                "P_to_C_minus_C_to_P_share": (pc - cp) / total if total > 0 else np.nan,
            }
            for flow_type, value in values.items():
                row[flow_type] = value
                row[f"{flow_type}_share"] = value / total if total > 0 else np.nan
            rows.append(row)
    return pd.DataFrame(rows)


def plot_threshold_sensitivity(group_summary: pd.DataFrame, out_stem: Path) -> None:
    order = ["all_cities", "top100_flow", "top50_flow", "top20_flow", "first_tier_manual"]
    labels = {
        "all_cities": "All cities",
        "top100_flow": "Top 100",
        "top50_flow": "Top 50",
        "top20_flow": "Top 20",
        "first_tier_manual": "First-tier",
    }
    colors = {
        "all_cities": "#6B7280",
        "top100_flow": "#4C78A8",
        "top50_flow": "#59A14F",
        "top20_flow": "#F28E2B",
        "first_tier_manual": "#C44E52",
    }
    fig, ax = plt.subplots(figsize=(5.8, 3.7), constrained_layout=True)
    for group in order:
        sub = group_summary.loc[group_summary["city_group"].eq(group)].sort_values("core_workplace_share")
        if sub.empty:
            continue
        ax.plot(
            sub["core_workplace_share"] * 100,
            sub["P_to_C_minus_C_to_P_share"] * 100,
            marker="o",
            linewidth=1.8,
            markersize=4,
            color=colors[group],
            label=labels[group],
        )
    ax.axhline(0, color="#333333", linewidth=0.8)
    ax.axvline(30, color="#333333", linewidth=0.8, linestyle="--", alpha=0.65)
    ax.set_xlabel("Employment-core threshold (% of workplace commuter mass)")
    ax.set_ylabel("P->C minus C->P share (pp)")
    ax.set_title("Center-bound imbalance is strongest in large-city groups")
    ax.legend(frameon=False, ncols=2, loc="upper right")
    for suffix in [".png", ".pdf", ".svg"]:
        fig.savefig(out_stem.with_suffix(suffix), bbox_inches="tight")
    plt.close(fig)


def plot_group_matrix_30(group_summary: pd.DataFrame, out_stem: Path) -> None:
    groups = ["all_cities", "top50_flow", "top20_flow", "first_tier_manual"]
    titles = ["All cities", "Top 50", "Top 20", "First-tier"]
    fig, axes = plt.subplots(1, 4, figsize=(8.4, 2.5), constrained_layout=True)
    vmax = 0.0
    matrices: list[np.ndarray] = []
    for group in groups:
        row = group_summary.loc[
            group_summary["core_workplace_share"].round(3).eq(0.300) & group_summary["city_group"].eq(group)
        ].iloc[0]
        mat = np.array(
            [
                [row["C_to_C_share"], row["C_to_P_share"]],
                [row["P_to_C_share"], row["P_to_P_share"]],
            ]
        )
        matrices.append(mat)
        vmax = max(vmax, float(np.nanmax(mat)))
    for ax, title, mat in zip(axes, titles, matrices):
        ax.imshow(mat * 100, cmap="YlOrRd", vmin=0, vmax=vmax * 100)
        ax.set_title(title)
        ax.set_xticks([0, 1], ["Dest C", "Dest P"])
        ax.set_yticks([0, 1], ["Orig C", "Orig P"])
        for i in range(2):
            for j in range(2):
                ax.text(j, i, f"{mat[i, j] * 100:.1f}", ha="center", va="center", fontsize=7.2)
    fig.suptitle("Inter-town center-periphery matrix at the 30% employment-core threshold")
    for suffix in [".png", ".pdf", ".svg"]:
        fig.savefig(out_stem.with_suffix(suffix), bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    args = parse_args()
    thresholds = parse_thresholds(args.thresholds)
    out_root = resolve_path(args.output_dir) / args.version_id
    dirs = ensure_version_dirs(out_root)
    configure_plots(out_root.parent)

    base_root = resolve_path(args.output_dir) / args.base_version_id
    town_path = base_root / "data" / "town_center_classification.csv"
    if not town_path.exists():
        raise FileNotFoundError(f"Missing base town classification: {town_path}")
    town = pd.read_csv(town_path)
    center_lookups: dict[float, dict[int, bool]] = {}
    for threshold in thresholds:
        center = town.loc[town["workplace_commuter_mass"].gt(0), ["town_code", "prev_cum_workplace_share"]].copy()
        center["is_center"] = center["prev_cum_workplace_share"] < threshold
        center_lookups[threshold] = center.set_index("town_code")["is_center"].to_dict()

    dta_path = first_existing_path(
        args.commute_dta_path,
        ["raw_data/town/commuting_town_2021.dta", "raw_data/commuting_town_2021.dta"],
    )
    parts: list[pd.DataFrame] = []
    n_chunks = 0
    n_intra_rows = 0
    for chunk in read_stata_chunks(dta_path, args.chunksize):
        n_chunks += 1
        clean = clean_chunk(chunk)
        intra = keep_intra_city(clean)
        n_intra_rows += int(len(intra))
        append_threshold_flow_parts(intra, center_lookups, parts)
        print(f"[threshold-pass] chunk={n_chunks:,} intra={len(intra):,}")
        if args.max_chunks > 0 and n_chunks >= args.max_chunks:
            break

    flow = combine_flow_parts(parts)
    city_wide = build_city_wide(flow)
    group_summary = summarize_groups(city_wide)
    flow.to_csv(dirs["metrics"] / "threshold_city_flows_long.csv", index=False)
    city_wide.to_csv(dirs["metrics"] / "threshold_city_flows_wide.csv", index=False)
    group_summary.to_csv(dirs["metrics"] / "threshold_group_summary.csv", index=False)
    plot_threshold_sensitivity(group_summary, dirs["figures"] / "figure_threshold_sensitivity_center_bound_imbalance")
    if 0.30 in thresholds:
        plot_group_matrix_30(group_summary, dirs["figures"] / "figure_group_2x2_at_30pct")

    manifest = {
        "version_id": args.version_id,
        "base_version_id": args.base_version_id,
        "commute_dta_path": str(dta_path),
        "thresholds": thresholds,
        "n_chunks": n_chunks,
        "n_intra_rows": n_intra_rows,
        "city_groups": {
            "all_cities": "all cities with inter-town flow",
            "top100_flow": "top 100 cities by inter-town commuters",
            "top50_flow": "top 50 cities by inter-town commuters",
            "top20_flow": "top 20 cities by inter-town commuters",
            "first_tier_manual": sorted(FIRST_TIER_CITY_KEYS),
        },
        "outputs": {
            "threshold_city_flows_long": str(dirs["metrics"] / "threshold_city_flows_long.csv"),
            "threshold_city_flows_wide": str(dirs["metrics"] / "threshold_city_flows_wide.csv"),
            "threshold_group_summary": str(dirs["metrics"] / "threshold_group_summary.csv"),
        },
        "notes": [
            "The diagnostic uses inter-town intra-city OD only.",
            "Center definitions are recomputed from the base city-specific workplace cumulative shares for each threshold.",
            "First-tier is a manual city group: Beijing, Shanghai, Guangzhou, Shenzhen.",
        ],
    }
    (dirs["metrics"] / "run_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[done] wrote {out_root}")


if __name__ == "__main__":
    main()
