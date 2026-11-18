"""Hierarchical car-mode share scaling for OD flows.

For each OD pair, assign car share via fallback:
  od_pair -> origin_grid -> work_grid -> distance_core_bin -> citywide

Grid-level ratios are shrunk toward the citywide ratio using identified sample size.
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

MODE_COLS = ["type_walk", "type_bike", "type_sub", "type_bus", "type_car"]
EPS = 1e-12


@dataclass(frozen=True)
class ModeShareScalingConfig:
    shrink_prior_n: float = 30.0
    distance_bins: int = 5
    hub_quantile_for_core: float = 0.5


def parse_grid_xy(grid_id: str) -> tuple[float, float]:
    parts = str(grid_id).split("_")
    if len(parts) >= 3:
        return float(parts[1]), float(parts[2])
    return 0.0, 0.0


def shrink_ratio(raw: float, citywide: float, n_identified: float, prior_n: float) -> float:
    n = max(float(n_identified), 0.0)
    if n <= 0:
        return float(citywide)
    return float((n * raw + prior_n * citywide) / (n + prior_n))


def _car_ratio(type_car: float, identified: float) -> float | None:
    if identified <= 0:
        return None
    return float(type_car / identified)


def citywide_car_ratio(df: pd.DataFrame) -> float:
    identified = pd.to_numeric(df["identified"], errors="coerce").fillna(0.0).sum()
    type_car = pd.to_numeric(df["type_car"], errors="coerce").fillna(0.0).sum()
    if identified <= 0:
        raise ValueError("citywide car ratio requires positive identified mass")
    return float(type_car / identified)


def _aggregate_ratio_table(
    df: pd.DataFrame,
    key_col: str,
    citywide: float,
    prior_n: float,
) -> pd.DataFrame:
    use = df.copy()
    use[key_col] = use[key_col].astype(str)
    use["type_car"] = pd.to_numeric(use["type_car"], errors="coerce").fillna(0.0)
    use["identified"] = pd.to_numeric(use["identified"], errors="coerce").fillna(0.0)
    agg = use.groupby(key_col, as_index=False).agg(
        type_car=("type_car", "sum"),
        identified=("identified", "sum"),
    )
    agg["raw_ratio"] = np.where(agg["identified"] > 0, agg["type_car"] / agg["identified"], np.nan)
    agg["car_share"] = [
        shrink_ratio(r if np.isfinite(r) else citywide, citywide, n, prior_n)
        for r, n in zip(agg["raw_ratio"], agg["identified"])
    ]
    return agg[[key_col, "identified", "raw_ratio", "car_share"]]


def _employment_hub_xy(df: pd.DataFrame) -> tuple[float, float]:
    use = df.copy()
    use["work_grid"] = use["work_grid"].astype(str)
    use["pop"] = pd.to_numeric(use["pop"], errors="coerce").fillna(0.0)
    xy = use["work_grid"].map(lambda g: parse_grid_xy(g))
    xs = np.array([p[0] for p in xy], dtype=float)
    ys = np.array([p[1] for p in xy], dtype=float)
    w = use["pop"].to_numpy(dtype=float)
    w_sum = w.sum()
    if w_sum <= 0:
        return 0.0, 0.0
    return float(np.average(xs, weights=w)), float(np.average(ys, weights=w))


def _grid_dist_to_hub(df: pd.DataFrame, hub_x: float, hub_y: float) -> pd.Series:
    grids = pd.unique(pd.concat([df["home_grid"], df["work_grid"]], ignore_index=True).astype(str))
    dist = {}
    for gid in grids:
        x, y = parse_grid_xy(gid)
        dist[gid] = float(np.hypot(x - hub_x, y - hub_y))
    return dist


def _add_distance_core_bin(
    df: pd.DataFrame,
    *,
    hub_x: float,
    hub_y: float,
    distance_bins: int,
    hub_quantile_for_core: float,
) -> pd.DataFrame:
    out = df.copy()
    out["home_grid"] = out["home_grid"].astype(str)
    out["work_grid"] = out["work_grid"].astype(str)
    hx = out["home_grid"].map(lambda g: parse_grid_xy(g)[0])
    hy = out["home_grid"].map(lambda g: parse_grid_xy(g)[1])
    wx = out["work_grid"].map(lambda g: parse_grid_xy(g)[0])
    wy = out["work_grid"].map(lambda g: parse_grid_xy(g)[1])
    out["od_distance"] = np.hypot(hx - wx, hy - wy)

    grid_dist = _grid_dist_to_hub(out, hub_x, hub_y)
    home_dist = out["home_grid"].map(grid_dist)
    work_dist = out["work_grid"].map(grid_dist)
    out["mean_dist_to_hub"] = 0.5 * (home_dist + work_dist)

    grid_pop = out.groupby("home_grid", as_index=False)["pop"].sum().rename(columns={"home_grid": "grid_id"})
    grid_pop["grid_id"] = grid_pop["grid_id"].astype(str)
    grid_pop["dist_to_hub"] = grid_pop["grid_id"].map(grid_dist)
    positive = grid_pop[grid_pop["pop"] > 0]
    core_cut = float(positive["dist_to_hub"].quantile(hub_quantile_for_core)) if len(positive) else 0.0
    out["is_core"] = (out["mean_dist_to_hub"] <= core_cut).astype(int)

    out["distance_bin"] = pd.qcut(
        out["od_distance"],
        q=min(distance_bins, out["od_distance"].nunique()),
        duplicates="drop",
    ).astype(str)
    out["distance_core_bin"] = out["distance_bin"] + "|core=" + out["is_core"].astype(str)
    return out


def apply_car_mode_share_scaling(
    od: pd.DataFrame,
    *,
    config: ModeShareScalingConfig | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return scaled OD table and a source summary."""
    cfg = config or ModeShareScalingConfig()
    out = od.copy()
    for col in ["pop"] + MODE_COLS + ["identified"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
        elif col == "pop":
            raise ValueError("OD table requires pop column")
        elif col in MODE_COLS:
            out[col] = 0.0
        elif col == "identified":
            out["identified"] = out[MODE_COLS].sum(axis=1)

    out["home_grid"] = out["home_grid"].astype(str)
    out["work_grid"] = out["work_grid"].astype(str)
    citywide = citywide_car_ratio(out)

    origin_tbl = _aggregate_ratio_table(out, "home_grid", citywide, cfg.shrink_prior_n)
    work_tbl = _aggregate_ratio_table(out, "work_grid", citywide, cfg.shrink_prior_n)
    origin_map = {
        row.home_grid: row.car_share
        for row in origin_tbl.itertuples(index=False)
        if row.identified > 0
    }
    work_map = {
        row.work_grid: row.car_share
        for row in work_tbl.itertuples(index=False)
        if row.identified > 0
    }

    binned = _add_distance_core_bin(
        out,
        hub_x=_employment_hub_xy(out)[0],
        hub_y=_employment_hub_xy(out)[1],
        distance_bins=cfg.distance_bins,
        hub_quantile_for_core=cfg.hub_quantile_for_core,
    )
    bin_tbl = _aggregate_ratio_table(binned, "distance_core_bin", citywide, cfg.shrink_prior_n)
    bin_map = {
        row.distance_core_bin: row.car_share
        for row in bin_tbl.itertuples(index=False)
        if row.identified > 0
    }

    car_share = np.full(len(binned), citywide, dtype=float)
    source = np.full(len(binned), "citywide", dtype=object)

    identified = binned["identified"].to_numpy(dtype=float)
    type_car = binned["type_car"].to_numpy(dtype=float)
    od_pair_mask = identified > 0
    car_share[od_pair_mask] = type_car[od_pair_mask] / identified[od_pair_mask]
    source[od_pair_mask] = "od_pair"

    home_grids = binned["home_grid"].astype(str).to_numpy()
    work_grids = binned["work_grid"].astype(str).to_numpy()
    dist_bins = binned["distance_core_bin"].astype(str).to_numpy()

    for idx in np.flatnonzero(~od_pair_mask):
        origin_share = origin_map.get(home_grids[idx])
        if origin_share is not None and np.isfinite(origin_share):
            car_share[idx] = float(origin_share)
            source[idx] = "origin_grid"
            continue
        work_share = work_map.get(work_grids[idx])
        if work_share is not None and np.isfinite(work_share):
            car_share[idx] = float(work_share)
            source[idx] = "work_grid"
            continue
        bin_share = bin_map.get(dist_bins[idx])
        if bin_share is not None and np.isfinite(bin_share):
            car_share[idx] = float(bin_share)
            source[idx] = "distance_core_bin"

    binned["car_share"] = car_share
    binned["mode_share_source"] = source
    binned["commuters_car_scaled"] = binned["pop"] * binned["car_share"]
    binned["commuters_road_scaled"] = binned["commuters_car_scaled"]
    binned["commuters_car_bus_scaled"] = binned["commuters_car_scaled"]  # car-only road user spec

    summary = (
        binned.groupby("mode_share_source", as_index=False)
        .agg(
            n_pairs=("home_grid", "size"),
            pop=("pop", "sum"),
            commuters_car_scaled=("commuters_car_scaled", "sum"),
            mean_car_share=("car_share", "mean"),
        )
        .sort_values("commuters_car_scaled", ascending=False)
    )
    summary["share_of_pop_pct"] = 100.0 * summary["pop"] / max(binned["pop"].sum(), EPS)
    summary["share_of_road_commuters_pct"] = (
        100.0 * summary["commuters_car_scaled"] / max(binned["commuters_car_scaled"].sum(), EPS)
    )
    meta = pd.DataFrame(
        [
            {
                "citywide_car_ratio": citywide,
                "shrink_prior_n": cfg.shrink_prior_n,
                "pop_total": float(binned["pop"].sum()),
                "commuters_road_direct": float(binned["type_car"].sum()),
                "commuters_road_scaled": float(binned["commuters_car_scaled"].sum()),
                "scaling_factor_vs_direct": float(
                    binned["commuters_car_scaled"].sum() / max(binned["type_car"].sum(), EPS)
                ),
            }
        ]
    )
    return binned, pd.concat([meta, summary], ignore_index=True)


def scaled_node_population(od_scaled: pd.DataFrame) -> pd.DataFrame:
    """Grid-level residents/jobs from scaled road commuters."""
    use = od_scaled.copy()
    use["home_grid"] = use["home_grid"].astype(str)
    use["work_grid"] = use["work_grid"].astype(str)
    road = pd.to_numeric(use["commuters_road_scaled"], errors="coerce").fillna(0.0)
    use["commuters_road_scaled"] = road
    residents = (
        use.groupby("home_grid", as_index=False)["commuters_road_scaled"]
        .sum()
        .rename(columns={"home_grid": "grid_id", "commuters_road_scaled": "residents"})
    )
    jobs = (
        use.groupby("work_grid", as_index=False)["commuters_road_scaled"]
        .sum()
        .rename(columns={"work_grid": "grid_id", "commuters_road_scaled": "jobs"})
    )
    out = residents.merge(jobs, on="grid_id", how="outer").fillna(0.0)
    out["job_resident_ratio"] = np.where(out["residents"] > 0, out["jobs"] / out["residents"], np.nan)
    return out
