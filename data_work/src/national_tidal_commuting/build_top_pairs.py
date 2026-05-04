from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.national_tidal_commuting.common import (
    ensure_version_dirs,
    get_version_root,
    haversine_km,
    load_config,
    normalize_coord_text,
    normalize_id_series,
    resolve_path,
)

REQUIRED_COLS = ["home_x", "home_y", "work_x", "work_y", "pop"]
OPTIONAL_ID_COLS = ["home_plot_id", "work_plot_id"]


def parse_args():
    parser = argparse.ArgumentParser(description="Build top commuting pairs for national tidal-commuting evidence.")
    parser.add_argument("--config", required=True, help="JSON config path.")
    parser.add_argument("--version-id", default=None, help="Optional version override.")
    parser.add_argument("--output-dir", default=None, help="Optional output dir override.")
    return parser.parse_args()


def endpoint_key(df: pd.DataFrame, side: str) -> pd.Series:
    id_col = f"{side}_plot_id"
    if id_col in df.columns:
        ids = normalize_id_series(df[id_col])
        coord = df.apply(lambda r: f"{normalize_coord_text(r[f'{side}_x'])},{normalize_coord_text(r[f'{side}_y'])}", axis=1)
        return np.where(ids.notna(), ids.astype(str), coord)
    return df.apply(lambda r: f"{normalize_coord_text(r[f'{side}_x'])},{normalize_coord_text(r[f'{side}_y'])}", axis=1)


def aggregate_pairs(df: pd.DataFrame) -> pd.DataFrame:
    use = df.copy()
    use["home_key"] = endpoint_key(use, "home")
    use["work_key"] = endpoint_key(use, "work")
    keep_cols = [c for c in OPTIONAL_ID_COLS if c in use.columns]
    agg = (
        use.groupby(["home_key", "work_key"], as_index=False)
        .agg(
            {
                "home_x": "first",
                "home_y": "first",
                "work_x": "first",
                "work_y": "first",
                "pop": "sum",
                **{c: "first" for c in keep_cols},
            }
        )
        .rename(columns={"pop": "pop_directed"})
    )
    agg["pair_key"] = np.where(
        agg["home_key"] <= agg["work_key"],
        agg["home_key"] + "||" + agg["work_key"],
        agg["work_key"] + "||" + agg["home_key"],
    )
    agg["is_forward"] = agg["home_key"] <= agg["work_key"]
    return agg


def collapse_unordered_pairs(directed: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for pair_key, grp in directed.groupby("pair_key", sort=False):
        forward = grp[grp["is_forward"]].copy()
        reverse = grp[~grp["is_forward"]].copy()
        f_pop = float(forward["pop_directed"].sum()) if not forward.empty else 0.0
        r_pop = float(reverse["pop_directed"].sum()) if not reverse.empty else 0.0
        if f_pop >= r_pop:
            base = forward.iloc[0] if not forward.empty else reverse.iloc[0]
            pop_ab, pop_ba = f_pop, r_pop
        else:
            base = reverse.iloc[0]
            pop_ab, pop_ba = r_pop, f_pop
        home_key = str(base["home_key"])
        work_key = str(base["work_key"])
        home_x, home_y = float(base["home_x"]), float(base["home_y"])
        work_x, work_y = float(base["work_x"]), float(base["work_y"])
        home_plot_id = base.get("home_plot_id", pd.NA)
        work_plot_id = base.get("work_plot_id", pd.NA)
        rows.append(
            {
                "pair_key": pair_key,
                "home_key": home_key,
                "work_key": work_key,
                "home_plot_id": home_plot_id,
                "work_plot_id": work_plot_id,
                "home_lon": home_x,
                "home_lat": home_y,
                "work_lon": work_x,
                "work_lat": work_y,
                "pop_ab": pop_ab,
                "pop_ba": pop_ba,
                "pop_total_pair": pop_ab + pop_ba,
            }
        )
    out = pd.DataFrame(rows)
    out["euclid_dist_km"] = haversine_km(out["home_lon"], out["home_lat"], out["work_lon"], out["work_lat"])
    return out


def finalize_undirected_top_n(
    unordered: pd.DataFrame,
    top_n: int,
    city_id: str,
    city_label: str,
    *,
    supplement_n: int = 0,
    min_dist_km: float | None = None,
) -> pd.DataFrame:
    """主样本取 top_n；可额外并入满足最小距离阈值的 top supplement_n 长距离 pair。"""
    if unordered.empty:
        return pd.DataFrame()
    u = unordered.copy()
    pab = pd.to_numeric(u["pop_ab"], errors="coerce").fillna(0.0)
    pba = pd.to_numeric(u["pop_ba"], errors="coerce").fillna(0.0)
    u["commuter_undir_mean"] = (pab + pba) / 2.0
    u = u.sort_values(
        ["commuter_undir_mean", "pop_total_pair", "pop_ab"],
        ascending=[False, False, False],
    ).reset_index(drop=True)
    u["rank_by_commuter_undir_mean"] = np.arange(1, len(u) + 1, dtype=int)
    base = u.head(int(top_n)).copy().reset_index(drop=True)
    base["selected_by_top50"] = True
    base["selected_by_longdist_top30"] = False
    w = base
    if supplement_n > 0 and min_dist_km is not None and np.isfinite(min_dist_km):
        long_pool = u[u["euclid_dist_km"] >= float(min_dist_km)].copy().reset_index(drop=True)
        long_pool["rank_in_longdist_pool"] = np.arange(1, len(long_pool) + 1, dtype=int)
        supp = long_pool.head(int(supplement_n)).copy()
        if not supp.empty:
            supp["selected_by_top50"] = supp["pair_key"].isin(set(base["pair_key"]))
            supp["selected_by_longdist_top30"] = True
            w = pd.concat([base, supp], ignore_index=True)
            w = w.sort_values(
                ["selected_by_top50", "selected_by_longdist_top30", "commuter_undir_mean", "euclid_dist_km", "pop_total_pair", "pop_ab"],
                ascending=[False, False, False, False, False, False],
            ).drop_duplicates("pair_key", keep="first").reset_index(drop=True)
        else:
            w["rank_in_longdist_pool"] = np.nan
    else:
        w["rank_in_longdist_pool"] = np.nan
    w["rank_in_city"] = np.arange(1, len(w) + 1, dtype=int)
    w["pair_id"] = [f"{city_id}_pair_{i:03d}" for i in range(1, len(w) + 1)]
    w["city_id"] = city_id
    w["city_label"] = city_label
    w["sample_shortfall_flag"] = bool(len(u) < int(top_n))
    return w


def process_city(city_cfg: dict, config: dict) -> tuple[pd.DataFrame, dict]:
    commute_path = resolve_path(city_cfg["commute_path"])
    read_cols = [c for c in REQUIRED_COLS + OPTIONAL_ID_COLS if c]
    df = pd.read_csv(commute_path, usecols=lambda c: c in read_cols)
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"{commute_path} is missing required columns: {missing}")
    for col in REQUIRED_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["home_x", "home_y", "work_x", "work_y", "pop"]).copy()
    df = df[df["pop"] > 0].copy()
    city_id = city_cfg["city_id"]
    city_label = city_cfg.get("city_label", city_id)
    top_n = int(config.get("top_n_pairs", 50))

    directed = aggregate_pairs(df)
    unordered = collapse_unordered_pairs(directed)
    selected = finalize_undirected_top_n(unordered, top_n, city_id, city_label)
    n_und = int(len(unordered))
    sh = bool(n_und < top_n) if n_und else True
    summary = {
        "city_id": city_id,
        "city_label": city_label,
        "commute_path": str(commute_path),
        "raw_rows": int(len(df)),
        "n_directed_edges": int(len(directed)),
        "n_undirected_pairs": n_und,
        "n_pairs_exported": int(len(selected)),
        "sample_shortfall_flag": sh,
        "commuter_undir_mean_sum_exported": float(selected["commuter_undir_mean"].sum()) if len(selected) else 0.0,
    }
    return selected, summary


def main():
    args = parse_args()
    config = load_config(args.config)
    if config.get("source", "per_city_csv") == "town_2021":
        from src.national_tidal_commuting.town_2021_top_pairs import run_town_2021_build

        run_town_2021_build(config, args.version_id, args.output_dir)
        return
    version_root = get_version_root(config, args.version_id, args.output_dir)
    dirs = ensure_version_dirs(version_root)
    data_dir = dirs["data"]
    metrics_dir = dirs["metrics"]

    selections = []
    summaries = []
    for city_cfg in config.get("cities", []):
        selected, summary = process_city(city_cfg, config)
        selections.append(selected)
        summaries.append(summary)

    all_selected = pd.concat(selections, ignore_index=True) if selections else pd.DataFrame()
    city_summary = pd.DataFrame(summaries)

    top_pairs_path = data_dir / "top_commuting_pairs.csv"
    summary_path = metrics_dir / "top_commuting_pairs_city_summary.csv"
    all_selected.to_csv(top_pairs_path, index=False)
    city_summary.to_csv(summary_path, index=False)
    (version_root / "config_snapshot.top_pairs.json").write_text(
        json.dumps(
            {
                "stage": "national_tidal_commuting.build_top_pairs",
                "config_path": config["_config_path"],
                "version_root": str(version_root.resolve()),
                "selected_pairs_path": str(top_pairs_path.resolve()),
                "city_summary_path": str(summary_path.resolve()),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"[build_top_pairs] wrote {len(all_selected)} selected pairs to {top_pairs_path}")
    print(f"[build_top_pairs] wrote city summary to {summary_path}")


if __name__ == "__main__":
    main()
