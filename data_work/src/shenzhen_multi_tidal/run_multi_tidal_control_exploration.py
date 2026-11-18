from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from run_multi_tidal_post_only_analysis import (  # noqa: E402
    CONTROL_AXIS_MAX_DIFF_DEG,
    CONTROL_MAX_LENGTH_RATIO,
    CONTROL_MIN_LENGTH_RATIO,
    CONTROL_MIN_DISTANCE_M,
    DIRECTION_LABELS,
    DIRECTION_ORDER,
    MIN_CONTROL_LENGTH_M,
    OPPOSITE,
    POLICY,
    PROJECTED_CRS,
    ROAD_CONFIGS,
    TIDAL_EXCLUSION_BUFFER_M,
    add_road_attributes,
    build_all_tidal_exclusion,
    classify_direction,
    configure_plot,
    is_excluded_control_name,
    is_same_tidal_family_name,
    match_treatment_segments,
    minute_label,
    read_selected_speed,
    read_speed_ids,
    save_figure,
    window_bounds,
)
from run_multi_tidal_refinement_analysis import (  # noqa: E402
    build_package_panel,
    collect_package_segments,
    fit_local_jump,
    summarize_package_corridor,
)


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_V1_DIR = PROJECT_ROOT / "data_work/outputs/shenzhen_multi_tidal_post_only_v1"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data_work/outputs/shenzhen_multi_tidal_exploration_v1"
DEFAULT_ROADS = PROJECT_ROOT / "data_work/raw_data/gis/roads_baidu/shenzhen_roads.shp"
DEFAULT_SPEED = PROJECT_ROOT / "data_work/raw_data/speed_shenzhen_all_wgs84.csv"
DEFAULT_TIDAL_DIR = PROJECT_ROOT / "data_work/raw_data/gis/深圳潮汐车道shp"

EXPANDED_SEARCH_M = 8000
FAR_MIN_1500_M = 1500
FAR_MIN_2000_M = 2000
MIN_POOL_DISTANCE_M = 800
MAX_POOL_CONTROLS = 40
SHORT_WINDOWS = [15, 30, 45, 60]
CORRIDOR_PACKAGES = ["core_only", "core_plus_1km", "core_plus_3km"]
PORTFOLIOS = [
    "refined_v1",
    "far_1500m",
    "far_2000m",
    "similar_top3",
    "far_similar_top3",
    "similar_best_single",
]
SPECS = [
    "local_jump_policy",
    "local_jump_opposite",
    "active_ref_ddd_speed_policy",
    "active_ref_ddd_logtt_policy",
    "asymmetry_active_ref_ddd",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Multi-scheme control and specification exploration for Shenzhen tidal lanes."
    )
    parser.add_argument("--v1-dir", type=Path, default=DEFAULT_V1_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--roads", type=Path, default=DEFAULT_ROADS)
    parser.add_argument("--speed", type=Path, default=DEFAULT_SPEED)
    parser.add_argument("--tidal-dir", type=Path, default=DEFAULT_TIDAL_DIR)
    return parser.parse_args()


def ensure_dirs(output_dir: Path) -> dict[str, Path]:
    dirs = {
        "root": output_dir,
        "data": output_dir / "data",
        "metrics": output_dir / "metrics",
        "figures": output_dir / "figures",
        "summary": output_dir / "figures/summary",
    }
    for path in dirs.values():
        path.mkdir(parents=True, exist_ok=True)
    return dirs


def build_expanded_control_pool(
    roads: gpd.GeoDataFrame,
    treatment: gpd.GeoDataFrame,
    all_exclusion,
    speed_ids: set[str],
) -> pd.DataFrame:
    rows = []
    roads_proj = roads.to_crs(PROJECTED_CRS)
    exclusion_hits = roads_proj.geometry.intersects(all_exclusion)
    excluded_name = roads_proj["roadname"].map(is_excluded_control_name)
    base = roads_proj[
        ~exclusion_hits
        & ~excluded_name
        & ~roads_proj["roadname"].isin(
            {
                "文锦北路", "彩田路", "新彩隧道", "惠深沿海高速", "大梅沙隧道",
                "新洲路", "红岭路", "红岭北路", "留仙大道",
            }
        )
        & roads_proj["roadseg_id"].astype(str).isin(speed_ids)
    ].copy()
    for number, cfg in ROAD_CONFIGS.items():
        if not cfg["include_main"]:
            continue
        treat = treatment[
            treatment["tidal_number"].astype(str).eq(str(number))
            & treatment.get("core_treatment", False).eq(True)
        ].copy()
        if treat.empty:
            continue
        treat_proj = treat.to_crs(PROJECTED_CRS)
        treat_union = treat_proj.geometry.union_all()
        target_length = float(treat_proj["analysis_length_m"].sum())
        axis_target = float(cfg["windows"][0]["policy_bearing_deg"]) % 180
        cand = base[base.geometry.distance(treat_union) <= EXPANDED_SEARCH_M].copy()
        cand = cand[~cand["roadname"].map(lambda x: is_same_tidal_family_name(x, cfg))].copy()
        cand["distance_to_treatment_m"] = cand.geometry.distance(treat_union)
        cand["axis_diff_deg"] = cand["bearing_deg"].map(lambda x: axis_diff(float(x), axis_target))
        cand = cand[cand["axis_diff_deg"] <= CONTROL_AXIS_MAX_DIFF_DEG].copy()
        cand = cand[cand["distance_to_treatment_m"] >= MIN_POOL_DISTANCE_M].copy()
        if cand.empty:
            continue
        grouped = (
            cand.groupby("roadname", as_index=False)
            .agg(
                n_segments=("roadseg_id", "nunique"),
                total_length_m=("length_m", "sum"),
                min_distance_to_treatment_m=("distance_to_treatment_m", "min"),
                mean_axis_diff_deg=("axis_diff_deg", "mean"),
            )
        )
        grouped = grouped[
            grouped["total_length_m"].ge(MIN_CONTROL_LENGTH_M)
            & grouped["n_segments"].ge(2)
            & grouped["min_distance_to_treatment_m"].ge(CONTROL_MIN_DISTANCE_M)
        ].copy()
        if grouped.empty:
            continue
        grouped["target_treatment_analysis_length_m"] = target_length
        grouped["length_ratio_to_treatment"] = grouped["total_length_m"] / max(target_length, 1.0)
        grouped["length_log_distance"] = np.abs(np.log(grouped["length_ratio_to_treatment"].clip(lower=1e-6)))
        length_ok = grouped["length_ratio_to_treatment"].between(
            CONTROL_MIN_LENGTH_RATIO, CONTROL_MAX_LENGTH_RATIO, inclusive="both"
        )
        if length_ok.any():
            grouped = grouped[length_ok | grouped["length_ratio_to_treatment"].between(0.15, 6.0)].copy()
        grouped["distance_score"] = grouped["min_distance_to_treatment_m"] / 1000.0
        grouped["geometry_score"] = (
            grouped["min_distance_to_treatment_m"] * 0.001
            + grouped["mean_axis_diff_deg"] * 0.03
            + grouped["length_log_distance"] * 0.35
        )
        grouped = grouped.sort_values(["geometry_score", "min_distance_to_treatment_m"]).head(MAX_POOL_CONTROLS)
        grouped["tidal_number"] = number
        grouped["road_key"] = cfg["road_key"]
        grouped["tidal_road_name"] = cfg["road_name_cn"]
        grouped["control_id"] = grouped["roadname"].map(lambda x: f"control_{x}")
        keep_names = set(grouped["roadname"])
        layer = cand[cand["roadname"].isin(keep_names)].copy()
        layer = layer.merge(
            grouped[
                [
                    "roadname",
                    "control_id",
                    "min_distance_to_treatment_m",
                    "mean_axis_diff_deg",
                    "total_length_m",
                    "length_ratio_to_treatment",
                ]
            ],
            on="roadname",
            how="left",
        )
        layer["tidal_number"] = number
        layer["road_key"] = cfg["road_key"]
        layer["tidal_road_name"] = cfg["road_name_cn"]
        rows.append(layer.drop(columns="geometry"))
    if not rows:
        return pd.DataFrame()
    out = pd.concat(rows, ignore_index=True)
    out["corridor_id"] = out.apply(lambda r: f"{r['road_key']}__{r['control_id']}", axis=1)
    out["corridor_label"] = out["roadname"]
    out["role"] = "control"
    out["analysis_length_m"] = out["length_m"]
    out["analysis_length_source"] = "full_control_roadseg_length"
    return out.drop_duplicates(["road_key", "roadseg_id", "corridor_id"]).reset_index(drop=True)


def axis_diff(a: float, b: float) -> float:
    return min(abs((a - b + 180) % 360 - 180), abs(((a + 180) % 360 - b + 180) % 360 - 180))


def attach_treatment_attrs(treatment: gpd.GeoDataFrame) -> pd.DataFrame:
    treat = treatment[treatment.get("core_treatment", False).eq(True)].copy()
    treat = treat.drop(columns="geometry", errors="ignore")
    treat["corridor_id"] = treat["road_key"].map(lambda x: f"treat_{x}")
    treat["corridor_label"] = treat["tidal_road_name"]
    treat["role"] = "treatment"
    treat["control_id"] = "treatment"
    return treat


def build_exploration_panel(
    speed_path: Path,
    treat_attrs: pd.DataFrame,
    control_pool: pd.DataFrame,
    selected_corridor_ids: set[str],
) -> pd.DataFrame:
    pool = control_pool[control_pool["corridor_id"].isin(selected_corridor_ids)].copy()
    treat = treat_attrs[treat_attrs["corridor_id"].isin(selected_corridor_ids)].copy()
    attrs = pd.concat([treat, pool], ignore_index=True)
    selected_ids = set(attrs["roadseg_id"].astype(str).unique())
    speed = read_selected_speed(speed_path, selected_ids)
    if speed.empty:
        return pd.DataFrame()
    keep_cols = [
        "tidal_number", "road_key", "tidal_road_name", "roadseg_id", "roadname", "bearing_deg",
        "length_m", "analysis_length_m", "corridor_id", "corridor_label", "role", "control_id",
        "min_distance_to_treatment_m",
    ]
    attrs = attrs[[c for c in keep_cols if c in attrs.columns]].drop_duplicates(["roadseg_id", "corridor_id"])
    panel = speed.merge(attrs, on="roadseg_id", how="inner")
    panel["analysis_length_m"] = pd.to_numeric(panel["analysis_length_m"], errors="coerce")
    panel.loc[panel["analysis_length_m"].isna(), "analysis_length_m"] = panel["length_m"]
    panel["segment_travel_time_min"] = panel["analysis_length_m"] / 1000 / panel["speed_kmh"] * 60
    panel["log_travel_time"] = np.log(panel["segment_travel_time_min"].clip(lower=1e-6))
    panel["log_speed"] = np.log(panel["speed_kmh"].clip(lower=1e-6))
    return panel


def compute_similarity_features(
    panel: pd.DataFrame,
    cfg: dict[str, object],
    window: dict[str, object],
    pool_table: pd.DataFrame,
) -> pd.DataFrame:
    road_key = cfg["road_key"]
    start = int(window["start_min"])
    end = int(window["end_min"])
    ref_start, _, _, _, _, _ = window_bounds(window)
    policy_bearing = float(window["policy_bearing_deg"])
    sub = panel[panel["road_key"].eq(road_key) & panel["day_type"].eq("weekday")].copy()
    if sub.empty:
        return pd.DataFrame()
    sub["direction_group"] = sub["bearing_deg"].map(lambda x: classify_direction(x, policy_bearing))
    inactive = sub[~sub["minute_of_day"].between(start, end)].copy()
    reference = sub[sub["minute_of_day"].between(ref_start, start)].copy()
    active = sub[sub["minute_of_day"].between(start, end)].copy()
    rows = []
    for direction in DIRECTION_ORDER:
        treat_inactive = (
            inactive[inactive["role"].eq("treatment") & inactive["direction_group"].eq(direction)]
            .groupby("minute_of_day", as_index=False)["speed_kmh"]
            .mean()
        )
        treat_ref = reference[
            reference["role"].eq("treatment") & reference["direction_group"].eq(direction)
        ]["speed_kmh"].mean()
        treat_active = active[
            active["role"].eq("treatment") & active["direction_group"].eq(direction)
        ]["speed_kmh"].mean()
        for control_name, ctrl_sub in inactive[
            inactive["role"].eq("control") & inactive["direction_group"].eq(direction)
        ].groupby("corridor_label"):
            ctrl_profile = ctrl_sub.groupby("minute_of_day", as_index=False)["speed_kmh"].mean()
            merged = treat_inactive.merge(ctrl_profile, on="minute_of_day", suffixes=("_t", "_c"))
            inactive_corr = np.nan
            if len(merged) >= 8:
                inactive_corr = float(merged["speed_kmh_t"].corr(merged["speed_kmh_c"]))
            ref_ctrl = reference[
                reference["role"].eq("control")
                & reference["direction_group"].eq(direction)
                & reference["corridor_label"].eq(control_name)
            ]["speed_kmh"].mean()
            active_ctrl = active[
                active["role"].eq("control")
                & active["direction_group"].eq(direction)
                & active["corridor_label"].eq(control_name)
            ]["speed_kmh"].mean()
            meta = pool_table[
                (pool_table["road_key"].eq(road_key))
                & (pool_table["roadname"].eq(control_name))
            ].iloc[0]
            ref_gap = np.nan
            active_gap = np.nan
            if np.isfinite(treat_ref) and np.isfinite(ref_ctrl) and ref_ctrl > 0:
                ref_gap = float(treat_ref - ref_ctrl)
            if np.isfinite(treat_active) and np.isfinite(active_ctrl):
                active_gap = float(treat_active - active_ctrl)
            similarity_score = np.nan
            if np.isfinite(inactive_corr):
                dist_km = float(meta["min_distance_to_treatment_m"]) / 1000.0
                axis_pen = float(meta["mean_axis_diff_deg"]) / max(CONTROL_AXIS_MAX_DIFF_DEG, 1.0)
                ref_pen = abs(ref_gap) / 20.0 if np.isfinite(ref_gap) else 0.5
                length_pen = float(meta.get("length_log_distance", 0.0))
                similarity_score = (
                    1.35 * inactive_corr
                    + 0.45 * min(dist_km / 3.0, 1.5)
                    - 0.55 * axis_pen
                    - 0.35 * ref_pen
                    - 0.15 * length_pen
                )
            rows.append(
                {
                    "road_key": road_key,
                    "road_name_cn": cfg["road_name_cn"],
                    "window_key": window["window_key"],
                    "direction_group": direction,
                    "control_name": control_name,
                    "control_id": meta["control_id"],
                    "corridor_id": meta["corridor_id"],
                    "min_distance_m": float(meta["min_distance_to_treatment_m"]),
                    "mean_axis_diff_deg": float(meta["mean_axis_diff_deg"]),
                    "inactive_profile_corr": inactive_corr,
                    "reference_speed_gap_kmh": ref_gap,
                    "active_speed_gap_kmh": active_gap,
                    "similarity_score": similarity_score,
                }
            )
    return pd.DataFrame(rows)


def select_portfolios(
    similarity: pd.DataFrame,
    v1_controls: pd.DataFrame,
    direction: str = POLICY,
) -> pd.DataFrame:
    rows = []
    for (road_key, window_key), sub in similarity.groupby(["road_key", "window_key"]):
        part = sub[sub["direction_group"].eq(direction)].copy()
        if part.empty:
            continue
        part = part.sort_values("similarity_score", ascending=False, na_position="last")
        v1_names = set(
            v1_controls[v1_controls["road_key"].eq(road_key)]["roadname"].astype(str)
        )
        refined = part[part["control_name"].isin(v1_names)].head(5)
        far1500 = part[part["min_distance_m"] >= FAR_MIN_1500_M].head(5)
        far2000 = part[part["min_distance_m"] >= FAR_MIN_2000_M].head(5)
        similar_top3 = part.head(3)
        far_similar = part[part["min_distance_m"] >= FAR_MIN_1500_M].head(3)
        best_single = part.head(1)
        for portfolio, subset in [
            ("refined_v1", refined),
            ("far_1500m", far1500),
            ("far_2000m", far2000),
            ("similar_top3", similar_top3),
            ("far_similar_top3", far_similar),
            ("similar_best_single", best_single),
        ]:
            if subset.empty:
                continue
            for rank, row in enumerate(subset.itertuples(), start=1):
                rows.append(
                    {
                        "road_key": road_key,
                        "window_key": window_key,
                        "portfolio": portfolio,
                        "portfolio_rank": rank,
                        "control_name": row.control_name,
                        "control_id": row.control_id,
                        "corridor_id": row.corridor_id,
                        "min_distance_m": row.min_distance_m,
                        "similarity_score": row.similarity_score,
                        "inactive_profile_corr": row.inactive_profile_corr,
                    }
                )
    return pd.DataFrame(rows)


def portfolio_corridor_ids(portfolio_table: pd.DataFrame, road_key: str, window_key: str, portfolio: str) -> set[str]:
    sub = portfolio_table[
        portfolio_table["road_key"].eq(road_key)
        & portfolio_table["window_key"].eq(window_key)
        & portfolio_table["portfolio"].eq(portfolio)
    ]
    ids = {f"treat_{road_key}"}
    ids.update(sub["corridor_id"].dropna().astype(str).unique())
    return ids


def corridor_daily_outcomes(
    port_panel: pd.DataFrame,
    cfg: dict[str, object],
    window: dict[str, object],
    package: str,
    package_panel: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if package == "core_only":
        sub = port_panel.copy()
    else:
        if package_panel is None or package_panel.empty:
            return pd.DataFrame()
        treat_ext = package_panel[package_panel["corridor_package"].eq(package)].copy()
        if treat_ext.empty:
            return pd.DataFrame()
        treat_ext["role"] = "treatment"
        treat_ext["corridor_id"] = f"treat_{cfg['road_key']}"
        treat_ext["corridor_label"] = cfg["road_name_cn"]
        ctrl = port_panel[port_panel["role"].eq("control")].copy()
        sub = pd.concat([treat_ext, ctrl], ignore_index=True)
    if sub.empty:
        return pd.DataFrame()
    policy_bearing = float(window["policy_bearing_deg"])
    ref_start, start, end, _, _, _ = window_bounds(window)
    sub["direction_group"] = sub["bearing_deg"].map(lambda x: classify_direction(x, policy_bearing))
    if package == "core_only":
        treat_ids = set(port_panel[port_panel["role"].eq("treatment")]["roadseg_id"].astype(str))
        sub.loc[sub["roadseg_id"].astype(str).isin(treat_ids), "corridor_id"] = f"treat_{cfg['road_key']}"
        sub.loc[sub["roadseg_id"].astype(str).isin(treat_ids), "role"] = "treatment"
    grouped = (
        sub.groupby(
            ["corridor_id", "corridor_label", "role", "direction_group", "date", "minute_of_day"],
            as_index=False,
        )
        .agg(
            travel_time_min=("segment_travel_time_min", "sum"),
            total_length_m=("analysis_length_m", "sum"),
            speed_kmh=("speed_kmh", "mean"),
        )
    )
    grouped["corridor_speed_kmh"] = grouped["total_length_m"] / 1000 / (grouped["travel_time_min"] / 60)
    grouped["log_travel_time"] = np.log(grouped["travel_time_min"].clip(lower=1e-6))
    weekday = grouped.copy()
    weekday = weekday.merge(
        sub[["date", "day_type"]].drop_duplicates(),
        on="date",
        how="left",
    )
    weekday = weekday[weekday["day_type"].eq("weekday")]
    rows = []
    for keys, part in weekday.groupby(["corridor_id", "corridor_label", "role", "direction_group", "date"]):
        ref = part[(part["minute_of_day"] >= ref_start) & (part["minute_of_day"] < start)]
        act = part[(part["minute_of_day"] >= start) & (part["minute_of_day"] < end)]
        if ref.empty or act.empty:
            continue
        rows.append(
            {
                "corridor_id": keys[0],
                "corridor_label": keys[1],
                "role": keys[2],
                "direction_group": keys[3],
                "date": keys[4],
                "reference_speed_kmh": ref["corridor_speed_kmh"].mean(),
                "active_speed_kmh": act["corridor_speed_kmh"].mean(),
                "reference_logtt": ref["log_travel_time"].mean(),
                "active_logtt": act["log_travel_time"].mean(),
            }
        )
    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows)
    out["active_minus_reference_speed"] = out["active_speed_kmh"] - out["reference_speed_kmh"]
    out["active_minus_reference_logtt"] = out["active_logtt"] - out["reference_logtt"]
    out["active_over_reference_speed"] = out["active_speed_kmh"] / out["reference_speed_kmh"]
    return out


def estimate_portfolio_specs(
    panel: pd.DataFrame,
    package_panels: dict[tuple[str, str, str], pd.DataFrame],
    portfolio_table: pd.DataFrame,
    cfg: dict[str, object],
    window: dict[str, object],
) -> pd.DataFrame:
    road_key = cfg["road_key"]
    window_key = window["window_key"]
    start = int(window["start_min"])
    rows = []
    for package in CORRIDOR_PACKAGES:
        package_panel = package_panels.get((road_key, window_key, package))
        for portfolio in PORTFOLIOS:
            corridor_ids = portfolio_corridor_ids(portfolio_table, road_key, window_key, portfolio)
            if len(corridor_ids) <= 1:
                continue
            port_panel = panel[panel["corridor_id"].isin(corridor_ids)].copy()
            if port_panel.empty:
                continue
            daily = corridor_daily_outcomes(port_panel, cfg, window, package, package_panel)
            port_daily = daily.copy()
            if port_daily.empty:
                continue
            controls = port_daily[port_daily["role"].eq("control")]
            treat = port_daily[port_daily["role"].eq("treatment")]
            if controls.empty or treat.empty:
                continue
            for half_window in SHORT_WINDOWS:
                lo = max(0, start - half_window)
                hi = min(24 * 60, start + half_window)
                sample = port_panel[
                    port_panel["day_type"].eq("weekday")
                    & port_panel["minute_of_day"].between(lo, hi)
                ].copy()
                if sample.empty:
                    continue
                policy_bearing = float(window["policy_bearing_deg"])
                sample["direction_group"] = sample["bearing_deg"].map(
                    lambda x: classify_direction(x, policy_bearing)
                )
                sample["post_start"] = (sample["minute_of_day"] >= start).astype(int)
                sample["treated"] = sample["role"].eq("treatment").astype(int)
                sample["unit_id"] = sample["roadseg_id"].astype(str)
                for direction, spec_suffix in [(POLICY, "local_jump_policy"), (OPPOSITE, "local_jump_opposite")]:
                    dir_sample = sample[sample["direction_group"].eq(direction)].copy()
                    if dir_sample["treated"].nunique() < 2:
                        continue
                    coef, diag = fit_local_jump(
                        dir_sample,
                        "speed_kmh",
                        "treated",
                        "post_start",
                        "unit_id",
                        spec_suffix,
                    )
                    if coef.empty:
                        continue
                    rows.append(
                        _result_row(
                            cfg, window, package, portfolio, spec_suffix, direction, half_window, coef, diag, portfolio_table
                        )
                    )

            for direction in [POLICY]:
                t = treat[treat["direction_group"].eq(direction)]
                c = controls[controls["direction_group"].eq(direction)]
                if t.empty or c.empty:
                    continue
                treat_change_speed = t["active_minus_reference_speed"].mean()
                control_change_speed = c["active_minus_reference_speed"].mean()
                treat_change_logtt = t["active_minus_reference_logtt"].mean()
                control_change_logtt = c["active_minus_reference_logtt"].mean()
                ddd_speed = treat_change_speed - control_change_speed
                ddd_logtt = treat_change_logtt - control_change_logtt
                t_asym = treat.copy()
                t_asym = t_asym.merge(
                    treat[treat["direction_group"].eq(OPPOSITE)][
                        ["date", "active_over_reference_speed"]
                    ].rename(columns={"active_over_reference_speed": "opp_ratio"}),
                    on="date",
                    how="left",
                )
                c_asym = controls.copy()
                c_asym = c_asym.merge(
                    controls[controls["direction_group"].eq(OPPOSITE)][
                        ["date", "corridor_id", "active_over_reference_speed"]
                    ].rename(columns={"active_over_reference_speed": "opp_ratio"}),
                    on=["date", "corridor_id"],
                    how="left",
                )
                t_pol = t_asym[t_asym["direction_group"].eq(POLICY)].copy()
                t_pol["log_asym"] = np.log(
                    (t_pol["active_over_reference_speed"] / t_pol["opp_ratio"]).clip(lower=1e-6)
                )
                c_pol = c_asym[c_asym["direction_group"].eq(POLICY)].copy()
                c_pol["log_asym"] = np.log(
                    (c_pol["active_over_reference_speed"] / c_pol["opp_ratio"]).clip(lower=1e-6)
                )
                ddd_asym = t_pol["log_asym"].mean() - c_pol["log_asym"].mean()
                for spec_name, coef_val, unit in [
                    ("active_ref_ddd_speed_policy", ddd_speed, "km/h"),
                    ("active_ref_ddd_logtt_policy", ddd_logtt, "log_min"),
                    ("asymmetry_active_ref_ddd", ddd_asym, "log_ratio"),
                ]:
                    rows.append(
                        {
                            "tidal_number": cfg["tidal_number"],
                            "road_key": road_key,
                            "road_name_cn": cfg["road_name_cn"],
                            "window_key": window_key,
                            "operation_window_label": window["label"],
                            "corridor_package": package,
                            "control_portfolio": portfolio,
                            "specification": spec_name,
                            "direction_group": direction,
                            "half_window_min": np.nan,
                            "coef": float(coef_val),
                            "std_error": np.nan,
                            "ci95_low": np.nan,
                            "ci95_high": np.nan,
                            "n_obs": int(len(port_daily)),
                            "n_controls": int(c["corridor_id"].nunique()),
                            "mean_control_distance_m": float(
                                portfolio_table[
                                    (portfolio_table["road_key"].eq(road_key))
                                    & (portfolio_table["window_key"].eq(window_key))
                                    & (portfolio_table["portfolio"].eq(portfolio))
                                ]["min_distance_m"].mean()
                            ),
                            "unit": unit,
                            "supports_congestion_relief": _supports(spec_name, float(coef_val)),
                        }
                    )
    return pd.DataFrame(rows)


def _result_row(cfg, window, package, portfolio, spec_name, direction, half_window, coef, diag, portfolio_table):
    val = float(coef["coef"].iloc[0])
    se = float(coef["std_error"].iloc[0]) if pd.notna(coef["std_error"].iloc[0]) else np.nan
    dist = portfolio_table[
        (portfolio_table["road_key"].eq(cfg["road_key"]))
        & (portfolio_table["window_key"].eq(window["window_key"]))
        & (portfolio_table["portfolio"].eq(portfolio))
    ]["min_distance_m"].mean()
    return {
        "tidal_number": cfg["tidal_number"],
        "road_key": cfg["road_key"],
        "road_name_cn": cfg["road_name_cn"],
        "window_key": window["window_key"],
        "operation_window_label": window["label"],
        "corridor_package": package,
        "control_portfolio": portfolio,
        "specification": spec_name,
        "direction_group": direction,
        "half_window_min": half_window,
        "coef": val,
        "std_error": se,
        "ci95_low": float(coef["ci95_low"].iloc[0]) if pd.notna(coef["ci95_low"].iloc[0]) else np.nan,
        "ci95_high": float(coef["ci95_high"].iloc[0]) if pd.notna(coef["ci95_high"].iloc[0]) else np.nan,
        "n_obs": int(coef["n_obs"].iloc[0]),
        "n_controls": np.nan,
        "mean_control_distance_m": float(dist) if pd.notna(dist) else np.nan,
        "unit": "km/h",
        "supports_congestion_relief": _supports(spec_name, val, se),
        "diagnostics": diag,
    }


def _supports(spec_name: str, coef: float, se: float = np.nan) -> bool:
    if not np.isfinite(coef):
        return False
    if "logtt" in spec_name:
        good = coef < 0
    elif "asymmetry" in spec_name:
        good = coef > 0
    elif "opposite" in spec_name:
        good = coef < 0
    else:
        good = coef > 0
    if np.isfinite(se) and se > 0:
        z = abs(coef / se)
        return good and z >= 1.645
    return good


def build_scorecard(results: pd.DataFrame) -> pd.DataFrame:
    if results.empty:
        return pd.DataFrame()
    rows = []
    group_cols = ["road_key", "road_name_cn", "window_key", "corridor_package", "control_portfolio", "specification"]
    for keys, sub in results.groupby(group_cols):
        policy = sub[sub["direction_group"].eq(POLICY)]
        if policy.empty:
            continue
        row = dict(zip(group_cols, keys))
        row["mean_control_distance_m"] = policy["mean_control_distance_m"].dropna().mean()
        row["n_controls"] = policy["n_controls"].dropna().max()
        row["coef"] = policy["coef"].mean()
        row["supports_congestion_relief"] = bool(policy["supports_congestion_relief"].any())
        row["n_half_windows_tested"] = int(policy["half_window_min"].notna().sum())
        rows.append(row)
    score = pd.DataFrame(rows)
    if score.empty:
        return score
    score["score"] = (
        score["supports_congestion_relief"].astype(int) * 3
        + score["coef"].clip(lower=-20, upper=20) / 10.0
        + score["mean_control_distance_m"].fillna(0) / 5000.0
    )
    return score.sort_values(["road_key", "window_key", "score"], ascending=[True, True, False])


def plot_scorecard_heatmap(scorecard: pd.DataFrame, output_path: Path) -> None:
    if scorecard.empty:
        return
    focus = scorecard[
        scorecard["specification"].isin(
            [
                "local_jump_policy",
                "active_ref_ddd_speed_policy",
                "active_ref_ddd_logtt_policy",
                "asymmetry_active_ref_ddd",
            ]
        )
        & scorecard["corridor_package"].eq("core_only")
    ].copy()
    if focus.empty:
        return
    focus["label"] = focus.apply(
        lambda r: f"{r['road_name_cn']}\n{r['control_portfolio']}\n{r['specification']}",
        axis=1,
    )
    pivot = focus.pivot_table(index="label", values="coef", aggfunc="first")
    fig, ax = plt.subplots(figsize=(8.5, max(4.5, 0.28 * len(pivot))))
    vmax = max(5.0, float(np.nanmax(np.abs(pivot.to_numpy()))))
    im = ax.imshow(pivot.to_numpy(), aspect="auto", cmap="RdBu_r", vmin=-vmax, vmax=vmax)
    ax.set_xticks([0])
    ax.set_xticklabels(["Coefficient"])
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index)
    ax.set_title("Exploration scorecard (core corridor, selected specs)", loc="left")
    fig.colorbar(im, ax=ax, fraction=0.03, pad=0.02)
    fig.tight_layout()
    save_figure(fig, output_path)


def plot_portfolio_distance(similarity: pd.DataFrame, output_path: Path) -> None:
    if similarity.empty:
        return
    sub = similarity[similarity["direction_group"].eq(POLICY)].copy()
    fig, ax = plt.subplots(figsize=(10.5, 4.8))
    roads = list(dict.fromkeys(sub["road_name_cn"]))
    for idx, road in enumerate(roads):
        part = sub[sub["road_name_cn"].eq(road)].sort_values("similarity_score", ascending=False).head(12)
        ax.scatter(
            part["min_distance_m"] / 1000.0,
            [idx] * len(part) + np.linspace(-0.12, 0.12, len(part)),
            s=30 + 40 * part["inactive_profile_corr"].fillna(0.2),
            c=part["similarity_score"].fillna(0),
            cmap="viridis",
            alpha=0.85,
        )
        for row in part.itertuples():
            ax.text(row.min_distance_m / 1000.0 + 0.03, idx, row.control_name, fontsize=7, va="center")
    ax.set_yticks(range(len(roads)))
    ax.set_yticklabels(roads)
    ax.set_xlabel("Min distance to treatment (km)")
    ax.set_title("Expanded control pool: distance vs similarity (policy direction)", loc="left")
    ax.grid(axis="x", color="#e5e5e5", linewidth=0.5)
    fig.tight_layout()
    save_figure(fig, output_path)


def plot_short_window_by_portfolio(results: pd.DataFrame, output_path: Path) -> None:
    sub = results[
        results["specification"].eq("local_jump_policy")
        & results["corridor_package"].eq("core_only")
        & results["half_window_min"].eq(30)
    ].copy()
    if sub.empty:
        return
    order = []
    for _n, cfg in ROAD_CONFIGS.items():
        if cfg["include_main"]:
            for w in cfg["windows"]:
                order.append((cfg["road_key"], w["window_key"]))
    labels = []
    vals = {p: [] for p in PORTFOLIOS}
    for road_key, window_key in order:
        cfg = next(c for c in ROAD_CONFIGS.values() if c["road_key"] == road_key)
        wlabel = next(w["label"] for w in cfg["windows"] if w["window_key"] == window_key)
        labels.append(f"{cfg['road_name_cn']} {wlabel}")
        for portfolio in PORTFOLIOS:
            row = sub[
                (sub["road_key"].eq(road_key))
                & (sub["window_key"].eq(window_key))
                & (sub["control_portfolio"].eq(portfolio))
            ]
            vals[portfolio].append(float(row["coef"].iloc[0]) if not row.empty else np.nan)
    x = np.arange(len(labels))
    width = 0.12
    fig, ax = plt.subplots(figsize=(11.5, 4.8))
    for i, portfolio in enumerate(PORTFOLIOS):
        ax.bar(x + (i - 2.5) * width, vals[portfolio], width=width, label=portfolio, alpha=0.88)
    ax.axhline(0, color="#222222", linewidth=0.8)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=25, ha="right")
    ax.set_ylabel("Local jump at window start (km/h)")
    ax.set_title("±30 min policy-direction local jump by control portfolio", loc="left")
    ax.legend(frameon=False, fontsize=7, ncol=2)
    ax.grid(axis="y", color="#e5e5e5", linewidth=0.5)
    fig.tight_layout()
    save_figure(fig, output_path)


def main() -> None:
    args = parse_args()
    configure_plot()
    dirs = ensure_dirs(args.output_dir)

    v1_controls = pd.read_csv(args.v1_dir / "metrics/control_candidates_by_tidal_lane.csv")
    treatment_gdf = gpd.read_file(args.v1_dir / "data/matched_treatment_segments.geojson")
    roads = add_road_attributes(gpd.read_file(args.roads))
    speed_ids = read_speed_ids(args.speed)
    all_exclusion = build_all_tidal_exclusion(args.tidal_dir)
    control_pool = build_expanded_control_pool(roads, treatment_gdf, all_exclusion, speed_ids)
    treat_attrs = attach_treatment_attrs(treatment_gdf)

    all_corridor_ids = set(control_pool["corridor_id"].astype(str)) | set(treat_attrs["corridor_id"].astype(str))
    panel = build_exploration_panel(args.speed, treat_attrs, control_pool, all_corridor_ids)

    similarity_rows = []
    package_panels: dict[tuple[str, str, str], pd.DataFrame] = {}
    for number, cfg0 in ROAD_CONFIGS.items():
        if not cfg0["include_main"]:
            continue
        cfg = dict(cfg0)
        cfg["tidal_number"] = number
        pool_table = control_pool[control_pool["road_key"].eq(cfg["road_key"])]
        for window in cfg["windows"]:
            similarity_rows.append(compute_similarity_features(panel, cfg, window, pool_table))
            for package in CORRIDOR_PACKAGES:
                if package == "core_only":
                    continue
                segs = collect_package_segments(roads, treatment_gdf, cfg, window, package, speed_ids, all_exclusion)
                if segs.empty:
                    continue
                package_panels[(cfg["road_key"], window["window_key"], package)] = build_package_panel(
                    segs, args.speed
                )
    similarity = pd.concat(similarity_rows, ignore_index=True) if similarity_rows else pd.DataFrame()
    portfolio_table = select_portfolios(similarity, v1_controls, direction=POLICY)

    result_rows = []
    for number, cfg0 in ROAD_CONFIGS.items():
        if not cfg0["include_main"]:
            continue
        cfg = dict(cfg0)
        cfg["tidal_number"] = number
        for window in cfg["windows"]:
            est = estimate_portfolio_specs(panel, package_panels, portfolio_table, cfg, window)
            if not est.empty:
                result_rows.append(est)
    results = pd.concat(result_rows, ignore_index=True) if result_rows else pd.DataFrame()
    if not results.empty:
        results = results.drop(columns=["diagnostics"], errors="ignore")
    scorecard = build_scorecard(results)

    best = pd.DataFrame()
    if not scorecard.empty:
        best = (
            scorecard.sort_values(["road_key", "window_key", "score"], ascending=[True, True, False])
            .groupby(["road_key", "window_key"], as_index=False)
            .first()
        )

    control_pool.drop(columns=["geometry"], errors="ignore").to_csv(
        dirs["data"] / "expanded_control_pool_segments.csv", index=False, encoding="utf-8-sig"
    )
    similarity.to_csv(dirs["metrics"] / "control_similarity_features.csv", index=False, encoding="utf-8-sig")
    portfolio_table.to_csv(dirs["metrics"] / "control_portfolios.csv", index=False, encoding="utf-8-sig")
    results.to_csv(dirs["metrics"] / "exploration_spec_results.csv", index=False, encoding="utf-8-sig")
    scorecard.to_csv(dirs["metrics"] / "exploration_scorecard.csv", index=False, encoding="utf-8-sig")
    best.to_csv(dirs["metrics"] / "exploration_best_scheme_by_road.csv", index=False, encoding="utf-8-sig")

    plot_portfolio_distance(similarity, dirs["summary"] / "expanded_control_distance_similarity.png")
    plot_short_window_by_portfolio(results, dirs["summary"] / "local_jump_30min_by_portfolio.png")
    plot_scorecard_heatmap(scorecard, dirs["summary"] / "exploration_scorecard_heatmap.png")

    spec = {
        "version": "shenzhen_multi_tidal_exploration_v1",
        "goal": "Search for econometric evidence of congestion relief under alternative controls, corridor lengths, and short-window shocks.",
        "expanded_search_m": EXPANDED_SEARCH_M,
        "far_min_distances_m": {"far_1500m": FAR_MIN_1500_M, "far_2000m": FAR_MIN_2000_M},
        "portfolios": PORTFOLIOS,
        "corridor_packages": CORRIDOR_PACKAGES,
        "short_windows_minutes": SHORT_WINDOWS,
        "specifications": SPECS,
        "interpretation": (
            "Post-only Baidu week 2025-05-19 to 2025-05-25. "
            "active_ref specs compare active-window change relative to the same-day reference window, "
            "then subtract control corridors. local_jump specs use short windows around the policy start time."
        ),
    }
    with (dirs["metrics"] / "exploration_specification.json").open("w", encoding="utf-8") as fh:
        json.dump(spec, fh, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
