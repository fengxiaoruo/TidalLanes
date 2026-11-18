from __future__ import annotations

import argparse
import heapq
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
    BUFFER_M,
    DIRECTION_LABELS,
    DIRECTION_ORDER,
    OPPOSITE,
    POLICY,
    PROJECTED_CRS,
    ROAD_CONFIGS,
    TIDAL_EXCLUSION_BUFFER_M,
    add_road_attributes,
    add_treatment_match_flags,
    as_line,
    axis_diff,
    build_all_tidal_exclusion,
    classify_direction,
    cluster_covariance,
    configure_plot,
    fit_event_study,
    is_excluded_control_name,
    is_same_tidal_family_name,
    match_treatment_segments,
    minute_label,
    read_selected_speed,
    read_speed_ids,
    residualize_matrix_sparse,
    save_figure,
    treatment_axis_target,
    window_bounds,
)


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_V1_DIR = PROJECT_ROOT / "data_work/outputs/shenzhen_multi_tidal_post_only_v1"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data_work/outputs/shenzhen_multi_tidal_refinement_v1"
DEFAULT_ROADS = PROJECT_ROOT / "data_work/raw_data/gis/roads_baidu/shenzhen_roads.shp"
DEFAULT_SPEED = PROJECT_ROOT / "data_work/raw_data/speed_shenzhen_all_wgs84.csv"
DEFAULT_TIDAL_DIR = PROJECT_ROOT / "data_work/raw_data/gis/深圳潮汐车道shp"

NEAR_SPILLOVER_M = 1000
MEDIUM_SPILLOVER_M = 3000
SHORT_WINDOW_MINUTES = [30, 60, 90]
CORRIDOR_PACKAGES = ["core_only", "core_plus_1km", "core_plus_3km"]
CONTROL_TIERS = ["near_spillover", "medium_spillover", "far_control"]
MAIN_CONTROL_TIERS = ["far_control", "medium_spillover"]
ENDPOINT_SNAP_M = 5.0
PLACEO_EVENT_START = 12 * 60


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Refinement diagnostics for Shenzhen multi-tidal Baidu post-only analysis."
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


def load_v1_tables(v1_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame]:
    panel = pd.read_csv(v1_dir / "data/selected_segment_speed_panel.csv", low_memory=False)
    control_table = pd.read_csv(v1_dir / "metrics/control_candidates_by_tidal_lane.csv")
    treatment = gpd.read_file(v1_dir / "data/matched_treatment_segments.geojson")
    control_segments = gpd.read_file(v1_dir / "data/control_candidate_segments.geojson")
    return panel, control_table, treatment, control_segments


def snap_point(x: float, y: float, tol: float = ENDPOINT_SNAP_M) -> tuple[float, float]:
    return (round(x / tol) * tol, round(y / tol) * tol)


def build_endpoint_adjacency(roads_proj: gpd.GeoDataFrame) -> dict[str, list[tuple[str, float, str]]]:
    adjacency: dict[str, list[tuple[str, float, str]]] = {}
    for row in roads_proj.itertuples(index=False):
        line = as_line(row.geometry)
        coords = list(line.coords)
        start = snap_point(coords[0][0], coords[0][1])
        end = snap_point(coords[-1][0], coords[-1][1])
        length = float(row.length_m)
        seg_id = str(row.roadseg_id)
        start_key = f"{start[0]:.1f},{start[1]:.1f}"
        end_key = f"{end[0]:.1f},{end[1]:.1f}"
        adjacency.setdefault(start_key, []).append((end_key, length, seg_id))
        adjacency.setdefault(end_key, []).append((start_key, length, seg_id))
    return adjacency


def dijkstra(adjacency: dict[str, list[tuple[str, float, str]]], origin: str, max_dist: float) -> dict[str, float]:
    distances = {origin: 0.0}
    heap: list[tuple[float, str]] = [(0.0, origin)]
    while heap:
        dist, node = heapq.heappop(heap)
        if dist > distances.get(node, np.inf):
            continue
        if dist > max_dist:
            continue
        for nbr, weight, _seg in adjacency.get(node, []):
            new_dist = dist + weight
            if new_dist <= max_dist and new_dist < distances.get(nbr, np.inf):
                distances[nbr] = new_dist
                heapq.heappush(heap, (new_dist, nbr))
    return distances


def segment_endpoint_keys(geom) -> tuple[str, str]:
    line = as_line(geom)
    coords = list(line.coords)
    start = snap_point(coords[0][0], coords[0][1])
    end = snap_point(coords[-1][0], coords[-1][1])
    return f"{start[0]:.1f},{start[1]:.1f}", f"{end[0]:.1f},{end[1]:.1f}"


def policy_unit_vector(bearing_deg: float) -> tuple[float, float]:
    rad = np.deg2rad(bearing_deg)
    return float(np.sin(rad)), float(np.cos(rad))


def projection_on_axis(points: np.ndarray, origin: np.ndarray, unit: tuple[float, float]) -> np.ndarray:
    vec = points - origin
    return vec[:, 0] * unit[0] + vec[:, 1] * unit[1]


def assign_control_tiers(
    treatment: gpd.GeoDataFrame,
    control_segments: gpd.GeoDataFrame,
) -> pd.DataFrame:
    treat_proj = treatment[treatment.get("core_treatment", False).eq(True)].to_crs(PROJECTED_CRS)
    ctrl_proj = control_segments.to_crs(PROJECTED_CRS).copy()
    rows = []
    for road_key, treat_sub in treat_proj.groupby("road_key"):
        treat_union = treat_sub.geometry.union_all()
        ctrl_sub = ctrl_proj[ctrl_proj["road_key"].eq(road_key)].copy()
        if ctrl_sub.empty:
            continue
        ctrl_sub["distance_to_treatment_m"] = ctrl_sub.geometry.distance(treat_union)
        corridor_distance = (
            ctrl_sub.groupby(["roadname", "control_id"], as_index=False)["distance_to_treatment_m"]
            .min()
            .rename(columns={"distance_to_treatment_m": "corridor_min_distance_m"})
        )
        ctrl_sub = ctrl_sub.merge(corridor_distance, on=["roadname", "control_id"], how="left")
        ctrl_sub["control_tier"] = np.select(
            [
                ctrl_sub["corridor_min_distance_m"] < NEAR_SPILLOVER_M,
                ctrl_sub["corridor_min_distance_m"] < MEDIUM_SPILLOVER_M,
            ],
            ["near_spillover", "medium_spillover"],
            default="far_control",
        )
        rows.append(ctrl_sub.drop(columns="geometry"))
    if not rows:
        return pd.DataFrame()
    out = pd.concat(rows, ignore_index=True)
    return out.sort_values(["road_key", "control_tier", "corridor_min_distance_m"]).reset_index(drop=True)


def primary_control_tier(tier_table: pd.DataFrame, road_key: str) -> str:
    sub = tier_table[tier_table["road_key"].eq(road_key)]
    for tier in MAIN_CONTROL_TIERS:
        if sub["control_tier"].eq(tier).any():
            return tier
    return "near_spillover"


def summarize_control_tiers(tier_table: pd.DataFrame) -> pd.DataFrame:
    if tier_table.empty:
        return pd.DataFrame()
    summary = (
        tier_table.groupby(
            ["road_key", "tidal_road_name", "control_tier", "roadname", "control_id"],
            as_index=False,
        )
        .agg(
            n_segments=("roadseg_id", "nunique"),
            corridor_min_distance_m=("corridor_min_distance_m", "first"),
            total_length_m=("length_m", "sum"),
        )
    )
    return summary.sort_values(["road_key", "control_tier", "corridor_min_distance_m"]).reset_index(drop=True)


def fit_local_jump(
    sample: pd.DataFrame,
    outcome_col: str,
    treatment_col: str,
    post_col: str,
    unit_col: str,
    model_name: str,
) -> tuple[pd.DataFrame, dict[str, object]]:
    sample = sample.dropna(subset=[outcome_col, treatment_col, post_col]).copy()
    finite = np.isfinite(pd.to_numeric(sample[outcome_col], errors="coerce"))
    sample = sample[finite].copy()
    if sample.empty or sample[treatment_col].nunique() < 2 or sample[post_col].nunique() < 2:
        return pd.DataFrame(), {"model": model_name, "error": "insufficient variation"}
    interaction = (sample[treatment_col].eq(1) & sample[post_col].eq(1)).astype(float)
    y = sample[outcome_col].to_numpy(dtype=float)
    x = interaction.to_numpy(dtype=float).reshape(-1, 1)
    residualized, n_iter = residualize_matrix_sparse(
        np.column_stack([y, x]),
        groups=[
            sample[unit_col].astype(str),
            sample["date"].astype(str),
            sample["minute_of_day"].astype(str),
        ],
    )
    y_tilde = residualized[:, 0]
    x_tilde = residualized[:, 1]
    finite2 = np.isfinite(y_tilde) & np.isfinite(x_tilde)
    y_tilde = y_tilde[finite2]
    x_tilde = x_tilde[finite2]
    cluster_ids = sample[unit_col].astype(str).to_numpy()[finite2]
    if np.sqrt(np.sum(x_tilde**2)) <= 1e-10:
        return pd.DataFrame(), {"model": model_name, "error": "collinear treatment interaction"}
    beta, *_ = np.linalg.lstsq(x_tilde.reshape(-1, 1), y_tilde, rcond=None)
    fitted = x_tilde * beta[0]
    residual = y_tilde - fitted
    vcov = cluster_covariance(x_tilde.reshape(-1, 1), residual, cluster_ids)
    se = float(np.sqrt(max(vcov[0, 0], 0)))
    coef = float(beta[0])
    row = pd.DataFrame(
        [
            {
                "model": model_name,
                "coef": coef,
                "std_error": se,
                "ci95_low": coef - 1.96 * se,
                "ci95_high": coef + 1.96 * se,
                "n_obs": int(len(sample)),
            }
        ]
    )
    diag = {
        "model": model_name,
        "n_obs": int(len(sample)),
        "n_units": int(sample[unit_col].nunique()),
        "residualization_max_lsmr_iterations": int(n_iter),
    }
    return row, diag


def tier_control_ids(tier_table: pd.DataFrame, road_key: str, tier: str) -> list[str]:
    sub = tier_table[(tier_table["road_key"].eq(road_key)) & (tier_table["control_tier"].eq(tier))]
    return sorted(f"control_{name}" for name in sub["roadname"].dropna().unique())


def build_short_window_samples(
    panel: pd.DataFrame,
    tier_table: pd.DataFrame,
    cfg: dict[str, object],
    window: dict[str, object],
    half_window_min: int,
    control_tier: str,
) -> pd.DataFrame:
    road_key = cfg["road_key"]
    start = int(window["start_min"])
    lo = max(0, start - half_window_min)
    hi = min(24 * 60, start + half_window_min)
    policy_bearing = float(window["policy_bearing_deg"])
    control_ids = tier_control_ids(tier_table, road_key, control_tier)
    keep_corridors = {f"treat_{road_key}", *{f"{road_key}__{cid}" for cid in control_ids}}
    sub = panel[
        panel["road_key"].eq(road_key)
        & panel["day_type"].eq("weekday")
        & panel["minute_of_day"].between(lo, hi, inclusive="both")
        & panel["corridor_id"].isin(keep_corridors)
    ].copy()
    if sub.empty:
        return sub
    sub["direction_group"] = sub["bearing_deg"].map(lambda x: classify_direction(x, policy_bearing))
    sub["post_start"] = (sub["minute_of_day"] >= start).astype(int)
    sub["event_time_min"] = sub["minute_of_day"] - start
    sub["event_bin_min"] = np.floor(sub["event_time_min"] / 10).astype(int) * 10
    sub["treated"] = sub["role"].eq("treatment").astype(int)
    sub["unit_id"] = sub["roadseg_id"].astype(str)
    sub["half_window_min"] = half_window_min
    sub["control_tier"] = control_tier
    return sub


def run_short_window_analysis(
    panel: pd.DataFrame,
    tier_table: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, object]]:
    jump_rows = []
    event_rows = []
    diagnostics: dict[str, object] = {}
    for number, cfg0 in ROAD_CONFIGS.items():
        if not cfg0["include_main"]:
            continue
        cfg = dict(cfg0)
        cfg["tidal_number"] = number
        road_key = cfg["road_key"]
        tiers_to_run = [t for t in CONTROL_TIERS if tier_control_ids(tier_table, road_key, t)]
        if not tiers_to_run:
            continue
        for window in cfg["windows"]:
            for half_window in SHORT_WINDOW_MINUTES:
                for control_tier in tiers_to_run:
                    sample = build_short_window_samples(
                        panel, tier_table, cfg, window, half_window, control_tier
                    )
                    if sample.empty:
                        continue
                    for direction in DIRECTION_ORDER:
                        dir_sample = sample[sample["direction_group"].eq(direction)].copy()
                        if dir_sample["treated"].nunique() < 2:
                            continue
                        model_base = (
                            f"{cfg['road_name_cn']} {window['label']} {direction} "
                            f"±{half_window}min {control_tier}"
                        )
                        jump, jump_diag = fit_local_jump(
                            dir_sample,
                            "speed_kmh",
                            "treated",
                            "post_start",
                            "unit_id",
                            f"{model_base} local jump",
                        )
                        if not jump.empty:
                            jump["tidal_number"] = number
                            jump["road_key"] = road_key
                            jump["window_key"] = window["window_key"]
                            jump["direction_group"] = direction
                            jump["half_window_min"] = half_window
                            jump["control_tier"] = control_tier
                            jump["primary_tier_for_road"] = primary_control_tier(tier_table, road_key)
                            jump_rows.append(jump)
                            diagnostics[f"jump::{model_base}"] = jump_diag
                        event, event_diag = fit_event_study(
                            dir_sample,
                            "speed_kmh",
                            "treated",
                            "unit_id",
                            f"{model_base} 10min bins",
                        )
                        if not event.empty:
                            event["tidal_number"] = number
                            event["road_key"] = road_key
                            event["window_key"] = window["window_key"]
                            event["direction_group"] = direction
                            event["half_window_min"] = half_window
                            event["control_tier"] = control_tier
                            event["primary_tier_for_road"] = primary_control_tier(tier_table, road_key)
                            event_rows.append(event)
                            diagnostics[f"event::{model_base}"] = event_diag
    jump_out = pd.concat(jump_rows, ignore_index=True) if jump_rows else pd.DataFrame()
    event_out = pd.concat(event_rows, ignore_index=True) if event_rows else pd.DataFrame()
    return jump_out, event_out, diagnostics


def collect_package_segments(
    roads: gpd.GeoDataFrame,
    treatment: gpd.GeoDataFrame,
    cfg: dict[str, object],
    window: dict[str, object],
    package: str,
    speed_ids: set[str],
    all_exclusion,
) -> pd.DataFrame:
    treat = treatment[
        treatment["road_key"].eq(cfg["road_key"]) & treatment.get("core_treatment", False).eq(True)
    ].copy()
    if treat.empty:
        return pd.DataFrame()
    roads_proj = roads.to_crs(PROJECTED_CRS)
    treat_proj = treat.to_crs(PROJECTED_CRS)
    treat_union = treat_proj.geometry.union_all()
    policy_bearing = float(window["policy_bearing_deg"])
    unit = policy_unit_vector(policy_bearing)
    centroids = np.column_stack([treat_proj.geometry.centroid.x, treat_proj.geometry.centroid.y])
    origin = centroids.mean(axis=0)
    projections = projection_on_axis(centroids, origin, unit)
    order = treat_proj.iloc[np.argsort(projections)].copy()
    upstream_key, _ = segment_endpoint_keys(order.geometry.iloc[0])
    _, downstream_key = segment_endpoint_keys(order.geometry.iloc[-1])

    if package == "core_only":
        selected = treat.copy()
        selected["corridor_package"] = package
        selected["network_distance_m"] = 0.0
        return selected.drop(columns="geometry", errors="ignore")

    radius_m = 1000 if package == "core_plus_1km" else 3000
    adjacency = build_endpoint_adjacency(roads_proj)
    reachable_nodes: set[str] = set()
    for origin_key in {upstream_key, downstream_key}:
        reachable_nodes.update(dijkstra(adjacency, origin_key, radius_m).keys())

    candidate = roads_proj[roads_proj["roadseg_id"].astype(str).isin(speed_ids)].copy()
    candidate = candidate[~candidate.geometry.intersects(all_exclusion)].copy()
    candidate = candidate[~candidate["roadname"].map(is_excluded_control_name)].copy()
    candidate = candidate[~candidate["roadname"].map(lambda x: is_same_tidal_family_name(x, cfg))].copy()
    candidate["axis_diff_deg"] = candidate["bearing_deg"].map(lambda x: axis_diff(float(x), policy_bearing % 180))
    candidate = candidate[candidate["axis_diff_deg"] <= 32].copy()
    endpoint_keys = candidate.geometry.map(lambda g: segment_endpoint_keys(g))
    candidate["start_key"] = endpoint_keys.map(lambda x: x[0])
    candidate["end_key"] = endpoint_keys.map(lambda x: x[1])
    in_network = candidate[
        candidate["start_key"].isin(reachable_nodes) | candidate["end_key"].isin(reachable_nodes)
    ].copy()
    geom_buffer = treat_union.buffer(radius_m)
    in_buffer = candidate[candidate.geometry.intersects(geom_buffer)].copy()
    extra = pd.concat([in_network, in_buffer], ignore_index=True).drop_duplicates("roadseg_id")
    core_ids = set(treat["roadseg_id"].astype(str))
    extra = extra[~extra["roadseg_id"].astype(str).isin(core_ids)].copy()
    extra["corridor_package"] = package
    extra["network_distance_m"] = extra.geometry.distance(treat_union)
    core = treat.copy()
    core["corridor_package"] = package
    core["network_distance_m"] = 0.0
    combined = pd.concat(
        [core.drop(columns="geometry", errors="ignore"), extra.drop(columns="geometry", errors="ignore")],
        ignore_index=True,
    )
    combined["road_key"] = cfg["road_key"]
    combined["tidal_number"] = cfg["tidal_number"]
    return combined


def build_package_panel(
    package_catalog: pd.DataFrame,
    speed_path: Path,
) -> pd.DataFrame:
    if package_catalog.empty:
        return pd.DataFrame()
    selected_ids = set(package_catalog["roadseg_id"].astype(str).unique())
    speed = read_selected_speed(speed_path, selected_ids)
    if speed.empty:
        return pd.DataFrame()
    attrs = package_catalog.drop_duplicates(["roadseg_id", "corridor_package"])
    panel = speed.merge(attrs, on="roadseg_id", how="inner")
    panel["analysis_length_m"] = pd.to_numeric(panel.get("analysis_length_m", panel["length_m"]), errors="coerce")
    panel.loc[panel["analysis_length_m"].isna(), "analysis_length_m"] = panel["length_m"]
    panel["segment_travel_time_min"] = panel["analysis_length_m"] / 1000 / panel["speed_kmh"] * 60
    panel["log_travel_time"] = np.log(panel["segment_travel_time_min"].clip(lower=1e-6))
    return panel


def summarize_package_corridor(
    package_panel: pd.DataFrame,
    cfg: dict[str, object],
    window: dict[str, object],
    package: str,
) -> pd.DataFrame:
    if package_panel.empty:
        return pd.DataFrame()
    policy_bearing = float(window["policy_bearing_deg"])
    ref_start, start, end, post_end, _, _ = window_bounds(window)
    sub = package_panel[package_panel["corridor_package"].eq(package)].copy()
    sub["direction_group"] = sub["bearing_deg"].map(lambda x: classify_direction(x, policy_bearing))
    sub["role"] = "treatment_package"
    grouped = (
        sub.groupby(
            ["road_key", "corridor_package", "direction_group", "datetime", "date", "day_type", "minute_of_day"],
            as_index=False,
        )
        .agg(
            travel_time_min=("segment_travel_time_min", "sum"),
            total_length_m=("analysis_length_m", "sum"),
            n_segments=("roadseg_id", "nunique"),
        )
    )
    grouped["corridor_speed_kmh"] = grouped["total_length_m"] / 1000 / (grouped["travel_time_min"] / 60)
    grouped["log_travel_time"] = np.log(grouped["travel_time_min"].clip(lower=1e-6))
    weekday = grouped[grouped["day_type"].eq("weekday")].copy()

    def window_mean(lo: int, hi: int) -> pd.DataFrame:
        part = weekday[(weekday["minute_of_day"] >= lo) & (weekday["minute_of_day"] < hi)].copy()
        return (
            part.groupby(["road_key", "corridor_package", "direction_group"], as_index=False)
            .agg(
                speed_kmh=("corridor_speed_kmh", "mean"),
                log_travel_time=("log_travel_time", "mean"),
                n_segments=("n_segments", "max"),
            )
        )

    active = window_mean(start, end)
    active["window"] = "active"
    reference = window_mean(ref_start, start)
    reference["window"] = "reference"
    out = pd.concat([reference, active], ignore_index=True)
    out["tidal_number"] = cfg["tidal_number"]
    out["road_name_cn"] = cfg["road_name_cn"]
    out["window_key"] = window["window_key"]
    out["operation_window_label"] = window["label"]
    return out


def screen_control_quality(
    panel: pd.DataFrame,
    tier_table: pd.DataFrame,
    cfg: dict[str, object],
    window: dict[str, object],
) -> pd.DataFrame:
    road_key = cfg["road_key"]
    start = int(window["start_min"])
    end = int(window["end_min"])
    policy_bearing = float(window["policy_bearing_deg"])
    control_tier = primary_control_tier(tier_table, road_key)
    treat = panel[(panel["road_key"].eq(road_key)) & panel["role"].eq("treatment")].copy()
    control_ids = tier_control_ids(tier_table, road_key, control_tier)
    controls = panel[
        panel["road_key"].eq(road_key)
        & panel["role"].eq("control")
        & panel["corridor_id"].isin({f"{road_key}__{cid}" for cid in control_ids})
    ].copy()
    if treat.empty or controls.empty:
        return pd.DataFrame()
    treat["direction_group"] = treat["bearing_deg"].map(lambda x: classify_direction(x, policy_bearing))
    controls["direction_group"] = controls["bearing_deg"].map(lambda x: classify_direction(x, policy_bearing))
    weekday = treat[treat["day_type"].eq("weekday")].copy()
    weekday_ctrl = controls[controls["day_type"].eq("weekday")].copy()
    inactive = weekday[~weekday["minute_of_day"].between(start, end)].copy()
    inactive_ctrl = weekday_ctrl[~weekday_ctrl["minute_of_day"].between(start, end)].copy()
    ref_start = max(0, start - 90)
    reference = weekday[weekday["minute_of_day"].between(ref_start, start)].copy()
    reference_ctrl = weekday_ctrl[weekday_ctrl["minute_of_day"].between(ref_start, start)].copy()

    rows = []
    for direction in DIRECTION_ORDER:
        treat_profile = (
            inactive[inactive["direction_group"].eq(direction)]
            .groupby("minute_of_day", as_index=False)["speed_kmh"]
            .mean()
        )
        for control_name, ctrl_sub in inactive_ctrl[inactive_ctrl["direction_group"].eq(direction)].groupby(
            "corridor_label"
        ):
            ctrl_profile = ctrl_sub.groupby("minute_of_day", as_index=False)["speed_kmh"].mean()
            merged = treat_profile.merge(ctrl_profile, on="minute_of_day", suffixes=("_treat", "_control"))
            corr = np.nan
            if len(merged) >= 6:
                corr = float(merged["speed_kmh_treat"].corr(merged["speed_kmh_control"]))
            ref_treat = reference[reference["direction_group"].eq(direction)]["speed_kmh"].mean()
            ref_ctrl = reference_ctrl[reference_ctrl["corridor_label"].eq(control_name)]["speed_kmh"].mean()
            placebo_sample = weekday[
                weekday["direction_group"].eq(direction)
                & weekday["minute_of_day"].between(PLACEO_EVENT_START - 60, PLACEO_EVENT_START + 60)
            ].copy()
            placebo_sample["post_start"] = (placebo_sample["minute_of_day"] >= PLACEO_EVENT_START).astype(int)
            placebo_sample["treated"] = placebo_sample["role"].eq("treatment").astype(int)
            placebo_sample["unit_id"] = placebo_sample["roadseg_id"].astype(str)
            placebo_ctrl = weekday_ctrl[
                weekday_ctrl["direction_group"].eq(direction)
                & weekday_ctrl["corridor_label"].eq(control_name)
                & weekday_ctrl["minute_of_day"].between(PLACEO_EVENT_START - 60, PLACEO_EVENT_START + 60)
            ].copy()
            placebo = pd.concat([placebo_sample, placebo_ctrl], ignore_index=True)
            placebo_coef = np.nan
            if placebo["treated"].nunique() == 2:
                jump, _ = fit_local_jump(
                    placebo,
                    "speed_kmh",
                    "treated",
                    "post_start",
                    "unit_id",
                    "placebo",
                )
                if not jump.empty:
                    placebo_coef = float(jump["coef"].iloc[0])
            rows.append(
                {
                    "road_key": road_key,
                    "road_name_cn": cfg["road_name_cn"],
                    "window_key": window["window_key"],
                    "direction_group": direction,
                    "control_name": control_name,
                    "control_tier": control_tier,
                    "inactive_profile_corr": corr,
                    "reference_speed_diff_kmh": ref_treat - ref_ctrl,
                    "placebo_local_jump_kmh": placebo_coef,
                }
            )
    return pd.DataFrame(rows)


def build_liuxian_placebo_summary(
    jump_coef: pd.DataFrame,
    short_event: pd.DataFrame,
    tier_table: pd.DataFrame,
) -> pd.DataFrame:
    primary_tier = primary_control_tier(tier_table, "liuxian")
    rows = []
    for df, label in [(jump_coef, "local_jump"), (short_event, "short_event_mean_active")]:
        sub = df[
            df["road_key"].eq("liuxian") & df["control_tier"].eq(primary_tier)
        ].copy()
        if sub.empty:
            continue
        if label == "local_jump":
            grouped = (
                sub.groupby(["direction_group", "half_window_min"], as_index=False)
                .agg(coef=("coef", "mean"), std_error=("std_error", "mean"))
            )
            grouped["metric"] = label
            rows.append(grouped)
        else:
            active = sub[(sub["event_bin_min"] >= 0) & (sub["event_bin_min"] <= 120)].copy()
            grouped = (
                active.groupby(["direction_group", "half_window_min"], as_index=False)
                .agg(coef=("coef", "mean"))
            )
            grouped["metric"] = label
            rows.append(grouped)
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def plot_short_window_summary(jump_coef: pd.DataFrame, tier_table: pd.DataFrame, output_path: Path) -> None:
    if jump_coef.empty:
        return
    primary_rows = []
    for road_key in jump_coef["road_key"].dropna().unique():
        tier = primary_control_tier(tier_table, road_key)
        part = jump_coef[
            jump_coef["road_key"].eq(road_key) & jump_coef["control_tier"].eq(tier)
        ].copy()
        if not part.empty:
            primary_rows.append(part)
    sub = pd.concat(primary_rows, ignore_index=True) if primary_rows else pd.DataFrame()
    if sub.empty:
        return
    roads = sub[["road_key", "tidal_number"]].drop_duplicates()
    road_order = []
    for number, cfg in ROAD_CONFIGS.items():
        if cfg["include_main"]:
            road_order.append(cfg["road_key"])
    colors = {POLICY: "#2b6cb0", OPPOSITE: "#c44e52"}
    fig, axes = plt.subplots(len(SHORT_WINDOW_MINUTES), 1, figsize=(10.5, 3.2 * len(SHORT_WINDOW_MINUTES)), sharex=True)
    if len(SHORT_WINDOW_MINUTES) == 1:
        axes = [axes]
    labels = []
    for road_key in road_order:
        cfg = next(c for c in ROAD_CONFIGS.values() if c["road_key"] == road_key)
        for window in cfg["windows"]:
            labels.append((road_key, window["window_key"], f"{cfg['road_name_cn']} {window['label']}"))
    positions = {(rk, wk): i for i, (rk, wk, _lbl) in enumerate(labels)}
    for ax, half_window in zip(axes, SHORT_WINDOW_MINUTES):
        part = sub[sub["half_window_min"].eq(half_window)].copy()
        for direction in DIRECTION_ORDER:
            dpart = part[part["direction_group"].eq(direction)].copy()
            if dpart.empty:
                continue
            x = [positions[(r.road_key, r.window_key)] for r in dpart.itertuples()]
            offset = -0.15 if direction == POLICY else 0.15
            ax.bar(
                [xi + offset for xi in x],
                dpart["coef"],
                width=0.28,
                color=colors[direction],
                alpha=0.88,
                label=DIRECTION_LABELS[direction],
            )
        ax.axhline(0, color="#222222", linewidth=0.8)
        ax.set_ylabel("Local jump coef (km/h)")
        ax.set_title(f"±{half_window} min local jump, primary control tier", loc="left")
        ax.grid(axis="y", color="#e5e5e5", linewidth=0.5)
    axes[-1].set_xticks(range(len(labels)))
    axes[-1].set_xticklabels([lbl for _rk, _wk, lbl in labels], rotation=28, ha="right")
    handles, legend_labels = axes[0].get_legend_handles_labels()
    if handles:
        fig.legend(handles, legend_labels, loc="upper center", ncol=2, frameon=False)
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    save_figure(fig, output_path)


def plot_control_quality_heatmap(screening: pd.DataFrame, output_path: Path) -> None:
    if screening.empty:
        return
    metric = screening.pivot_table(
        index=["road_name_cn", "control_name"],
        columns="direction_group",
        values="inactive_profile_corr",
        aggfunc="first",
    )
    if metric.empty:
        return
    fig, ax = plt.subplots(figsize=(6.8, max(3.5, 0.35 * len(metric))))
    im = ax.imshow(metric.to_numpy(), aspect="auto", cmap="RdYlGn", vmin=-1, vmax=1)
    ax.set_xticks(range(len(metric.columns)))
    ax.set_xticklabels([DIRECTION_LABELS[c] for c in metric.columns], rotation=20, ha="right")
    ax.set_yticks(range(len(metric.index)))
    ax.set_yticklabels([f"{a} | {b}" for a, b in metric.index])
    ax.set_title("Inactive-hour diurnal correlation, treatment vs far controls", loc="left")
    fig.colorbar(im, ax=ax, fraction=0.03, pad=0.02)
    fig.tight_layout()
    save_figure(fig, output_path)


def plot_package_comparison(package_summary: pd.DataFrame, output_path: Path) -> None:
    if package_summary.empty:
        return
    active = package_summary[package_summary["window"].eq("active")].copy()
    if active.empty:
        return
    labels = []
    for number, cfg in ROAD_CONFIGS.items():
        if not cfg["include_main"]:
            continue
        for window in cfg["windows"]:
            labels.append((cfg["road_key"], window["window_key"], f"{cfg['road_name_cn']} {window['label']}"))
    positions = {(rk, wk): i for i, (rk, wk, _lbl) in enumerate(labels)}
    package_order = CORRIDOR_PACKAGES
    colors = {POLICY: "#2b6cb0", OPPOSITE: "#c44e52"}
    fig, axes = plt.subplots(1, 2, figsize=(12.5, 4.8), sharex=True)
    for ax, outcome, ylabel in zip(
        axes,
        ["speed_kmh", "log_travel_time"],
        ["Active-window speed (km/h)", "Active-window log travel time"],
    ):
        width = 0.22
        for pkg_idx, package in enumerate(package_order):
            part = active[active["corridor_package"].eq(package)].copy()
            for dir_idx, direction in enumerate(DIRECTION_ORDER):
                dpart = part[part["direction_group"].eq(direction)].copy()
                if dpart.empty:
                    continue
                x = [
                    positions[(r.road_key, r.window_key)] + (pkg_idx - 1) * width + (dir_idx - 0.5) * width * 0.45
                    for r in dpart.itertuples()
                ]
                ax.bar(
                    x,
                    dpart[outcome],
                    width=width * 0.42,
                    color=colors[direction],
                    alpha=0.55 + 0.15 * pkg_idx,
                    label=f"{DIRECTION_LABELS[direction]} | {package}" if ax is axes[0] else None,
                )
        ax.set_xticks(range(len(labels)))
        ax.set_xticklabels([lbl for _rk, _wk, lbl in labels], rotation=28, ha="right")
        ax.set_ylabel(ylabel)
        ax.grid(axis="y", color="#e5e5e5", linewidth=0.5)
    axes[0].set_title("Corridor package comparison", loc="left")
    fig.tight_layout()
    save_figure(fig, output_path)


def plot_liuxian_placebo(placebo_summary: pd.DataFrame, output_path: Path) -> None:
    if placebo_summary.empty:
        return
    jump = placebo_summary[placebo_summary["metric"].eq("local_jump")].copy()
    if jump.empty:
        return
    colors = {POLICY: "#2b6cb0", OPPOSITE: "#c44e52"}
    fig, ax = plt.subplots(figsize=(7.2, 4.0))
    for direction in DIRECTION_ORDER:
        part = jump[jump["direction_group"].eq(direction)].sort_values("half_window_min")
        if part.empty:
            continue
        ax.errorbar(
            part["half_window_min"],
            part["coef"],
            yerr=1.96 * part["std_error"],
            fmt="o-",
            color=colors[direction],
            capsize=3,
            label=DIRECTION_LABELS[direction],
        )
    ax.axhline(0, color="#222222", linewidth=0.8)
    ax.set_xlabel("Half-window around 17:30 (minutes)")
    ax.set_ylabel("Treatment-control local jump (km/h)")
    ax.set_title("Liuxian Avenue pre-shock placebo, primary control tier", loc="left")
    ax.grid(axis="y", color="#e5e5e5", linewidth=0.5)
    ax.legend(frameon=False)
    fig.tight_layout()
    save_figure(fig, output_path)


def main() -> None:
    args = parse_args()
    configure_plot()
    dirs = ensure_dirs(args.output_dir)

    panel, _control_table, treatment, control_segments = load_v1_tables(args.v1_dir)
    tier_table = assign_control_tiers(treatment, control_segments)
    tier_summary = summarize_control_tiers(tier_table)

    jump_coef, short_event, short_diag = run_short_window_analysis(panel, tier_table)

    primary_tier_rows = []
    for road_key in panel["road_key"].dropna().unique():
        tier = primary_control_tier(tier_table, str(road_key))
        primary_tier_rows.append({"road_key": road_key, "primary_control_tier": tier})
    primary_tier_table = pd.DataFrame(primary_tier_rows).drop_duplicates()

    roads = add_road_attributes(gpd.read_file(args.roads))
    speed_ids = read_speed_ids(args.speed)
    all_exclusion = build_all_tidal_exclusion(args.tidal_dir)
    package_catalog_rows = []
    package_summary_rows = []
    for number, cfg0 in ROAD_CONFIGS.items():
        if not cfg0["include_main"]:
            continue
        cfg = dict(cfg0)
        cfg["tidal_number"] = number
        for window in cfg["windows"]:
            for package in CORRIDOR_PACKAGES:
                package_segments = collect_package_segments(
                    roads, treatment, cfg, window, package, speed_ids, all_exclusion
                )
                if package_segments.empty:
                    continue
                package_segments["window_key"] = window["window_key"]
                package_catalog_rows.append(package_segments)
                package_panel = build_package_panel(package_segments, args.speed)
                summary = summarize_package_corridor(package_panel, cfg, window, package)
                if not summary.empty:
                    package_summary_rows.append(summary)

    package_catalog = pd.concat(package_catalog_rows, ignore_index=True) if package_catalog_rows else pd.DataFrame()
    package_summary = pd.concat(package_summary_rows, ignore_index=True) if package_summary_rows else pd.DataFrame()

    screening_rows = []
    for number, cfg0 in ROAD_CONFIGS.items():
        if not cfg0["include_main"]:
            continue
        cfg = dict(cfg0)
        cfg["tidal_number"] = number
        for window in cfg["windows"]:
            screening_rows.append(screen_control_quality(panel, tier_table, cfg, window))
    control_screening = pd.concat([x for x in screening_rows if not x.empty], ignore_index=True)

    liuxian_placebo = build_liuxian_placebo_summary(jump_coef, short_event, tier_table)

    tier_table.to_csv(dirs["metrics"] / "control_tiers_by_segment.csv", index=False, encoding="utf-8-sig")
    tier_summary.to_csv(dirs["metrics"] / "control_tier_summary.csv", index=False, encoding="utf-8-sig")
    primary_tier_table.to_csv(dirs["metrics"] / "primary_control_tier_by_road.csv", index=False, encoding="utf-8-sig")
    jump_coef.to_csv(dirs["metrics"] / "short_window_local_jump_coefficients.csv", index=False, encoding="utf-8-sig")
    short_event.to_csv(dirs["metrics"] / "short_window_event_study_coefficients.csv", index=False, encoding="utf-8-sig")
    package_catalog.to_csv(dirs["data"] / "corridor_package_segment_catalog.csv", index=False, encoding="utf-8-sig")
    package_summary.to_csv(dirs["metrics"] / "corridor_package_window_summary.csv", index=False, encoding="utf-8-sig")
    control_screening.to_csv(dirs["metrics"] / "control_quality_screening.csv", index=False, encoding="utf-8-sig")
    liuxian_placebo.to_csv(dirs["metrics"] / "liuxian_placebo_summary.csv", index=False, encoding="utf-8-sig")
    with (dirs["metrics"] / "short_window_diagnostics.json").open("w", encoding="utf-8") as fh:
        json.dump(short_diag, fh, ensure_ascii=False, indent=2)

    plot_short_window_summary(jump_coef, tier_table, dirs["summary"] / "short_window_local_jump_primary_tier.png")
    plot_control_quality_heatmap(control_screening, dirs["summary"] / "control_quality_inactive_corr_heatmap.png")
    plot_package_comparison(package_summary, dirs["summary"] / "corridor_package_active_window_comparison.png")
    plot_liuxian_placebo(liuxian_placebo, dirs["summary"] / "liuxian_pre_shock_placebo.png")

    spec = {
        "version": "shenzhen_multi_tidal_refinement_v1",
        "source_v1": str(args.v1_dir),
        "control_tiers_m": {
            "near_spillover": f"0-{NEAR_SPILLOVER_M}",
            "medium_spillover": f"{NEAR_SPILLOVER_M}-{MEDIUM_SPILLOVER_M}",
            "far_control": f">={MEDIUM_SPILLOVER_M}",
        },
        "primary_control_tier_rule": (
            "Use far_control when any corridor has min distance >= 3000 m; "
            "otherwise use medium_spillover (1000-3000 m); "
            "otherwise near_spillover."
        ),
        "short_windows_minutes": SHORT_WINDOW_MINUTES,
        "corridor_packages": CORRIDOR_PACKAGES,
        "local_jump_specification": (
            "speed_{i d m} = alpha_i + delta_d + lambda_m + "
            "beta Treat_i * PostStart_m + epsilon_{i d m}, where PostStart_m = 1[minute >= window start]."
        ),
        "control_quality_metrics": [
            "inactive_profile_corr",
            "reference_speed_diff_kmh",
            "placebo_local_jump_kmh at 12:00",
        ],
        "liuxian_interpretation": (
            "Liuxian Avenue Baidu week is pre-shock relative to the 2026-03-23 opening. "
            "Placebo section checks for policy-direction-specific local jumps that should be absent."
        ),
        "interpretation": "Refinement on post-only within-day comparisons; not pre/post opening effects.",
    }
    with (dirs["metrics"] / "refinement_specification.json").open("w", encoding="utf-8") as fh:
        json.dump(spec, fh, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
