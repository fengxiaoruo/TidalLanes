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

from run_multi_tidal_control_exploration import (  # noqa: E402
    attach_treatment_attrs,
    axis_diff,
    build_expanded_control_pool,
    build_exploration_panel,
)
from run_multi_tidal_post_only_analysis import (  # noqa: E402
    DIRECTION_ORDER,
    OPPOSITE,
    POLICY,
    ROAD_CONFIGS,
    add_road_attributes,
    build_all_tidal_exclusion,
    classify_direction,
    cluster_covariance,
    configure_plot,
    fit_event_study,
    read_speed_ids,
    residualize_matrix_sparse,
    save_figure,
    window_bounds,
)
from run_multi_tidal_refinement_analysis import fit_local_jump  # noqa: E402


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_V1_DIR = PROJECT_ROOT / "data_work/outputs/shenzhen_multi_tidal_post_only_v1"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data_work/outputs/shenzhen_multi_tidal_pooled_psm_did_v1"
DEFAULT_ROADS = PROJECT_ROOT / "data_work/raw_data/gis/roads_baidu/shenzhen_roads.shp"
DEFAULT_SPEED = PROJECT_ROOT / "data_work/raw_data/speed_shenzhen_all_wgs84.csv"
DEFAULT_TIDAL_DIR = PROJECT_ROOT / "data_work/raw_data/gis/深圳潮汐车道shp"

SHORT_WINDOWS = [30, 45, 60]
MAX_MATCHES = 3
MIN_INACTIVE_CORR = 0.5
MIN_MATCH_DISTANCE_M = 1500
MATCH_RELAX_INACTIVE_CORR = 0.35
MATCH_RELAX_DISTANCE_M = 800
PLACEBO_EVENT_START_MIN = 12 * 60
PLACEBO_COHORT_KEY = "liuxian"
EVENT_BIN_WIDTH = 10


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pooled PSM-style matched DID for Shenzhen multi-tidal Baidu post-only week."
    )
    parser.add_argument("--v1-dir", type=Path, default=DEFAULT_V1_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--roads", type=Path, default=DEFAULT_ROADS)
    parser.add_argument("--speed", type=Path, default=DEFAULT_SPEED)
    parser.add_argument("--tidal-dir", type=Path, default=DEFAULT_TIDAL_DIR)
    parser.add_argument(
        "--include-liuxian-main",
        action="store_true",
        help="Include 留仙大道 pre-shock cohort in the main pooled sample (default: placebo only).",
    )
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


def cohort_id(road_key: str, window_key: str) -> str:
    return f"{road_key}__{window_key}"


def cohort_meta_rows(include_liuxian_main: bool) -> pd.DataFrame:
    rows = []
    for number, cfg0 in ROAD_CONFIGS.items():
        if not cfg0["include_main"]:
            continue
        cfg = dict(cfg0)
        cfg["tidal_number"] = number
        for window in cfg["windows"]:
            cid = cohort_id(cfg["road_key"], window["window_key"])
            sample_role = "main"
            if cfg["road_key"] == PLACEBO_COHORT_KEY and not include_liuxian_main:
                sample_role = "placebo_only"
            rows.append(
                {
                    "cohort_id": cid,
                    "tidal_number": number,
                    "road_key": cfg["road_key"],
                    "road_name_cn": cfg["road_name_cn"],
                    "window_key": window["window_key"],
                    "operation_window_label": window["label"],
                    "start_min": int(window["start_min"]),
                    "end_min": int(window["end_min"]),
                    "policy_bearing_deg": float(window["policy_bearing_deg"]),
                    "sample_role": sample_role,
                }
            )
    return pd.DataFrame(rows)


def build_corridor_match_features(
    panel: pd.DataFrame,
    pool_table: pd.DataFrame,
    meta: pd.Series,
    direction: str = POLICY,
) -> tuple[pd.DataFrame, pd.Series]:
    road_key = meta["road_key"]
    start = int(meta["start_min"])
    end = int(meta["end_min"])
    ref_start, _, _, _, _, _ = window_bounds(
        {"start_min": start, "end_min": end, "policy_bearing_deg": meta["policy_bearing_deg"]}
    )
    policy_bearing = float(meta["policy_bearing_deg"])
    sub = panel[panel["road_key"].eq(road_key) & panel["day_type"].eq("weekday")].copy()
    sub["direction_group"] = sub["bearing_deg"].map(lambda x: classify_direction(x, policy_bearing))
    sub = sub[sub["direction_group"].eq(direction)].copy()
    inactive = sub[~sub["minute_of_day"].between(start, end)].copy()
    reference = sub[sub["minute_of_day"].between(ref_start, start)].copy()
    treat_inactive = (
        inactive[inactive["role"].eq("treatment")]
        .groupby("minute_of_day", as_index=False)["speed_kmh"]
        .mean()
    )
    treat_ref_speed = reference[reference["role"].eq("treatment")]["speed_kmh"].mean()
    treat_length = sub[sub["role"].eq("treatment")]["analysis_length_m"].sum()
    treat_row = pd.Series(
        {
            "corridor_id": f"treat_{road_key}",
            "corridor_label": meta["road_name_cn"],
            "role": "treatment",
            "inactive_profile_corr": 1.0,
            "reference_speed_kmh": float(treat_ref_speed),
            "log_length_m": float(np.log(max(treat_length, 1.0))),
            "min_distance_m": 0.0,
            "axis_diff_deg": 0.0,
            "is_treatment": 1,
        }
    )
    rows = []
    for control_name, ctrl_sub in inactive[inactive["role"].eq("control")].groupby("corridor_label"):
        meta_row = pool_table[
            (pool_table["road_key"].eq(road_key)) & (pool_table["roadname"].eq(control_name))
        ]
        if meta_row.empty:
            continue
        meta_row = meta_row.iloc[0]
        ctrl_profile = ctrl_sub.groupby("minute_of_day", as_index=False)["speed_kmh"].mean()
        merged = treat_inactive.merge(ctrl_profile, on="minute_of_day", suffixes=("_t", "_c"))
        inactive_corr = np.nan
        if len(merged) >= 8:
            inactive_corr = float(merged["speed_kmh_t"].corr(merged["speed_kmh_c"]))
        ref_ctrl = reference[
            (reference["role"].eq("control")) & (reference["corridor_label"].eq(control_name))
        ]["speed_kmh"].mean()
        ctrl_length = sub[
            (sub["role"].eq("control")) & (sub["corridor_label"].eq(control_name))
        ]["analysis_length_m"].sum()
        rows.append(
            {
                "corridor_id": meta_row["corridor_id"],
                "corridor_label": control_name,
                "role": "control",
                "inactive_profile_corr": inactive_corr,
                "reference_speed_kmh": float(ref_ctrl),
                "log_length_m": float(np.log(max(ctrl_length, 1.0))),
                "min_distance_m": float(meta_row["min_distance_to_treatment_m"]),
                "axis_diff_deg": float(meta_row["mean_axis_diff_deg"]),
                "is_treatment": 0,
            }
        )
    controls = pd.DataFrame(rows)
    if controls.empty:
        return controls, treat_row
    controls["ref_speed_gap_kmh"] = controls["reference_speed_kmh"] - float(treat_ref_speed)
    treat_row["ref_speed_gap_kmh"] = 0.0
    return controls, treat_row


def mahalanobis_distance(controls: pd.DataFrame, treat_row: pd.Series) -> pd.Series:
    features = [
        "inactive_profile_corr",
        "reference_speed_kmh",
        "log_length_m",
        "axis_diff_deg",
        "min_distance_m",
    ]
    x = controls[features].astype(float).copy()
    t = treat_row[features].astype(float)
    mu = x.mean()
    sd = x.std(ddof=0).replace(0, 1.0)
    x_std = (x - mu) / sd
    t_std = (t - mu) / sd
    dist = np.sqrt(((x_std - t_std) ** 2).sum(axis=1))
    return dist


def match_controls_for_cohort(
    panel: pd.DataFrame,
    pool_table: pd.DataFrame,
    meta: pd.Series,
    direction: str = POLICY,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    controls, treat_row = build_corridor_match_features(panel, pool_table, meta, direction=direction)
    balance_before = pd.concat(
        [
            treat_row.to_frame().T.assign(stage="treatment"),
            controls.assign(stage="control_pool"),
        ],
        ignore_index=True,
    )
    if controls.empty:
        return pd.DataFrame(), balance_before
    controls = controls.copy()
    controls["mahalanobis_distance"] = mahalanobis_distance(controls, treat_row)

    def _select(min_corr: float, min_dist: float) -> pd.DataFrame:
        eligible = controls[
            controls["inactive_profile_corr"].ge(min_corr)
            & controls["min_distance_m"].ge(min_dist)
        ].copy()
        if len(eligible) < MAX_MATCHES:
            eligible = controls.sort_values("mahalanobis_distance").head(MAX_MATCHES)
        else:
            eligible = eligible.sort_values(
                ["mahalanobis_distance", "inactive_profile_corr"],
                ascending=[True, False],
            ).head(MAX_MATCHES)
        return eligible

    matched = _select(MIN_INACTIVE_CORR, MIN_MATCH_DISTANCE_M)
    match_rule = "strict"
    if len(matched) < MAX_MATCHES:
        matched = _select(MATCH_RELAX_INACTIVE_CORR, MIN_MATCH_DISTANCE_M)
        match_rule = "relaxed_corr"
    if len(matched) < 1:
        matched = _select(MATCH_RELAX_INACTIVE_CORR, MATCH_RELAX_DISTANCE_M)
        match_rule = "relaxed_corr_distance"
    if matched.empty:
        return pd.DataFrame(), balance_before

    matched = matched.copy()
    matched["match_rank"] = np.arange(1, len(matched) + 1)
    matched["match_rule"] = match_rule
    matched["cohort_id"] = meta["cohort_id"]
    matched["road_key"] = meta["road_key"]
    matched["window_key"] = meta["window_key"]
    matched["direction_group"] = direction
    balance_after = pd.concat(
        [
            treat_row.to_frame().T.assign(stage="treatment"),
            matched.assign(stage="matched_control"),
        ],
        ignore_index=True,
    )
    balance = pd.concat(
        [
            balance_before.assign(balance_stage="before_matching"),
            balance_after.assign(balance_stage="after_matching"),
        ],
        ignore_index=True,
    )
    return matched, balance


def build_matched_segment_panel(
    panel: pd.DataFrame,
    matches: pd.DataFrame,
    meta: pd.DataFrame,
) -> pd.DataFrame:
    if matches.empty:
        return pd.DataFrame()
    rows = []
    for _, cohort in meta.iterrows():
        cid = cohort["cohort_id"]
        road_key = cohort["road_key"]
        cohort_matches = matches[matches["cohort_id"].eq(cid)].copy()
        if cohort_matches.empty:
            continue
        keep_corridors = set(cohort_matches["corridor_id"].astype(str)) | {f"treat_{road_key}"}
        sub = panel[panel["road_key"].eq(road_key) & panel["corridor_id"].astype(str).isin(keep_corridors)].copy()
        if sub.empty:
            continue
        sub["cohort_id"] = cid
        sub["window_key"] = cohort["window_key"]
        sub["start_min"] = int(cohort["start_min"])
        sub["end_min"] = int(cohort["end_min"])
        sub["policy_bearing_deg"] = float(cohort["policy_bearing_deg"])
        sub["sample_role"] = cohort["sample_role"]
        sub["operation_window_label"] = cohort["operation_window_label"]
        rows.append(sub)
    if not rows:
        return pd.DataFrame()
    out = pd.concat(rows, ignore_index=True)
    out["global_unit_id"] = out["cohort_id"].astype(str) + "::" + out["roadseg_id"].astype(str)
    out["cohort_start_min"] = out["start_min"]
    out["event_time_min"] = out["minute_of_day"] - out["cohort_start_min"]
    out["event_bin_min"] = np.floor(out["event_time_min"] / EVENT_BIN_WIDTH).astype(int) * EVENT_BIN_WIDTH
    out["post_start"] = (out["minute_of_day"] >= out["cohort_start_min"]).astype(int)
    out["treated_segment"] = out["role"].eq("treatment").astype(int)
    out["am_pm_group"] = np.where(out["cohort_start_min"] < 12 * 60, "am", "pm")
    dir_rows = []
    for cid, part in out.groupby("cohort_id"):
        bearing = float(part["policy_bearing_deg"].iloc[0])
        part = part.copy()
        part["direction_group"] = part["bearing_deg"].map(lambda x, b=bearing: classify_direction(x, b))
        dir_rows.append(part)
    return pd.concat(dir_rows, ignore_index=True)


def valid_control_corridors(matches: pd.DataFrame, cohort_id_value: str, direction: str) -> set[str]:
    sub = matches[
        matches["cohort_id"].eq(cohort_id_value)
        & matches["direction_group"].eq(direction)
    ]
    return set(sub["corridor_id"].astype(str).unique())


def build_regression_sample(
    matched_panel: pd.DataFrame,
    matches: pd.DataFrame,
    half_window_min: int,
    direction: str,
    sample_role: str = "main",
    event_start_override: int | None = None,
) -> pd.DataFrame:
    sub = matched_panel[
        matched_panel["direction_group"].eq(direction) & matched_panel["day_type"].eq("weekday")
    ].copy()
    if sample_role != "all":
        sub = sub[sub["sample_role"].eq(sample_role)].copy()
    if sub.empty:
        return sub
    rows = []
    for cid, part in sub.groupby("cohort_id"):
        controls = valid_control_corridors(matches, cid, direction)
        road_key = part["road_key"].iloc[0]
        keep = {f"treat_{road_key}"} | controls
        chunk = part[part["corridor_id"].astype(str).isin(keep)].copy()
        if chunk.empty:
            continue
        start = int(event_start_override if event_start_override is not None else chunk["cohort_start_min"].iloc[0])
        lo = max(0, start - half_window_min)
        hi = min(24 * 60, start + half_window_min)
        chunk = chunk[chunk["minute_of_day"].between(lo, hi, inclusive="both")].copy()
        if chunk.empty:
            continue
        chunk["cohort_start_min"] = start
        chunk["event_time_min"] = chunk["minute_of_day"] - start
        chunk["event_bin_min"] = np.floor(chunk["event_time_min"] / EVENT_BIN_WIDTH).astype(int) * EVENT_BIN_WIDTH
        chunk["post_start"] = (chunk["minute_of_day"] >= start).astype(int)
        chunk["half_window_min"] = half_window_min
        rows.append(chunk)
    if not rows:
        return pd.DataFrame()
    return pd.concat(rows, ignore_index=True)


def fit_pooled_local_jump_with_cohort_fe(
    sample: pd.DataFrame,
    outcome_col: str,
    model_name: str,
    include_cohort_fe: bool = False,
) -> tuple[pd.DataFrame, dict[str, object]]:
    sample = sample.dropna(subset=[outcome_col, "treated_segment", "post_start"]).copy()
    finite = np.isfinite(pd.to_numeric(sample[outcome_col], errors="coerce"))
    sample = sample[finite].copy()
    if sample.empty or sample["treated_segment"].nunique() < 2 or sample["post_start"].nunique() < 2:
        return pd.DataFrame(), {"model": model_name, "error": "insufficient variation"}

    interaction = (sample["treated_segment"].eq(1) & sample["post_start"].eq(1)).astype(float)
    y = sample[outcome_col].to_numpy(dtype=float)
    x_cols = [interaction.to_numpy(dtype=float)]
    groups = [
        sample["global_unit_id"].astype(str),
        sample["date"].astype(str),
        sample["minute_of_day"].astype(str),
    ]
    if include_cohort_fe:
        cohort_interactions = []
        cohorts = sorted(sample["cohort_id"].astype(str).unique())
        for cid in cohorts[1:]:
            cohort_interactions.append(
                (interaction * sample["cohort_id"].astype(str).eq(cid).astype(float)).to_numpy(dtype=float)
            )
        if cohort_interactions:
            x_cols.extend(cohort_interactions)
    x = np.column_stack(x_cols)
    residualized, n_iter = residualize_matrix_sparse(np.column_stack([y, x]), groups=groups)
    y_tilde = residualized[:, 0]
    x_tilde = residualized[:, 1:]
    finite2 = np.isfinite(y_tilde) & np.all(np.isfinite(x_tilde), axis=1)
    y_tilde = y_tilde[finite2]
    x_tilde = x_tilde[finite2]
    cluster_ids = sample["global_unit_id"].astype(str).to_numpy()[finite2]
    if np.sqrt(np.sum(x_tilde[:, 0] ** 2)) <= 1e-10:
        return pd.DataFrame(), {"model": model_name, "error": "collinear treatment interaction"}
    beta, *_ = np.linalg.lstsq(x_tilde, y_tilde, rcond=None)
    fitted = np.einsum("ij,j->i", x_tilde, beta, optimize=False)
    residual = y_tilde - fitted
    vcov = cluster_covariance(x_tilde, residual, cluster_ids)
    se_main = float(np.sqrt(max(vcov[0, 0], 0)))
    coef_main = float(beta[0])
    row = pd.DataFrame(
        [
            {
                "model": model_name,
                "term": "treat_x_post_start",
                "coef": coef_main,
                "std_error": se_main,
                "ci95_low": coef_main - 1.96 * se_main,
                "ci95_high": coef_main + 1.96 * se_main,
                "n_obs": int(len(sample)),
                "n_cohorts": int(sample["cohort_id"].nunique()),
                "include_cohort_fe": include_cohort_fe,
            }
        ]
    )
    diag = {
        "model": model_name,
        "n_obs": int(len(sample)),
        "n_units": int(sample["global_unit_id"].nunique()),
        "n_cohorts": int(sample["cohort_id"].nunique()),
        "residualization_max_lsmr_iterations": int(n_iter),
        "include_cohort_fe": include_cohort_fe,
    }
    return row, diag


def fit_cohort_specific_local_jump(
    sample: pd.DataFrame,
    outcome_col: str,
    half_window_min: int,
    direction: str,
) -> pd.DataFrame:
    rows = []
    for cid, part in sample.groupby("cohort_id"):
        coef, diag = fit_local_jump(
            part,
            outcome_col=outcome_col,
            treatment_col="treated_segment",
            post_col="post_start",
            unit_col="global_unit_id",
            model_name=f"cohort_{cid}",
        )
        if coef.empty:
            continue
        meta = part.iloc[0]
        rows.append(
            {
                "cohort_id": cid,
                "road_key": meta["road_key"],
                "window_key": meta["window_key"],
                "road_name_cn": meta.get("road_name_cn", meta.get("tidal_road_name", "")),
                "operation_window_label": meta.get("operation_window_label", ""),
                "sample_role": meta["sample_role"],
                "direction_group": direction,
                "half_window_min": half_window_min,
                "coef": float(coef["coef"].iloc[0]),
                "std_error": float(coef["std_error"].iloc[0]),
                "ci95_low": float(coef["ci95_low"].iloc[0]),
                "ci95_high": float(coef["ci95_high"].iloc[0]),
                "n_obs": int(coef["n_obs"].iloc[0]),
                "n_units": int(diag.get("n_units", np.nan)),
            }
        )
    return pd.DataFrame(rows)


def build_corridor_daily_asymmetry(
    matched_panel: pd.DataFrame,
    matches: pd.DataFrame,
    meta: pd.DataFrame,
) -> pd.DataFrame:
    rows = []
    meta_lookup = meta.set_index("cohort_id")
    for cid, part in matched_panel.groupby("cohort_id"):
        row_meta = meta_lookup.loc[cid]
        controls = valid_control_corridors(matches, cid, POLICY)
        keep = {f"treat_{row_meta['road_key']}"} | controls
        part = part[part["corridor_id"].astype(str).isin(keep)].copy()
        start = int(row_meta["start_min"])
        end = int(row_meta["end_min"])
        ref_start, _, _, _, _, _ = window_bounds(
            {
                "start_min": start,
                "end_min": end,
                "policy_bearing_deg": row_meta["policy_bearing_deg"],
            }
        )
        weekday = part[part["day_type"].eq("weekday")].copy()
        if weekday.empty:
            continue
        grouped = (
            weekday.groupby(
                ["corridor_id", "corridor_label", "role", "direction_group", "date", "minute_of_day"],
                as_index=False,
            )
            .agg(
                travel_time_min=("segment_travel_time_min", "sum"),
                total_length_m=("analysis_length_m", "sum"),
            )
        )
        grouped["speed_kmh"] = grouped["total_length_m"] / 1000 / (grouped["travel_time_min"] / 60)
        for keys, daypart in grouped.groupby(["corridor_id", "corridor_label", "role", "date"]):
            ref = daypart[daypart["minute_of_day"].between(ref_start, start)]
            act = daypart[daypart["minute_of_day"].between(start, end)]
            if ref.empty or act.empty:
                continue
            pol_ref = ref[ref["direction_group"].eq(POLICY)]["speed_kmh"].mean()
            opp_ref = ref[ref["direction_group"].eq(OPPOSITE)]["speed_kmh"].mean()
            pol_act = act[act["direction_group"].eq(POLICY)]["speed_kmh"].mean()
            opp_act = act[act["direction_group"].eq(OPPOSITE)]["speed_kmh"].mean()
            if not all(np.isfinite([pol_ref, opp_ref, pol_act, opp_act])) or min(opp_ref, opp_act) <= 0:
                continue
            ratio_ref = pol_ref / opp_ref
            ratio_act = pol_act / opp_act
            rows.append(
                {
                    "cohort_id": cid,
                    "road_key": row_meta["road_key"],
                    "window_key": row_meta["window_key"],
                    "sample_role": row_meta["sample_role"],
                    "corridor_id": keys[0],
                    "corridor_label": keys[1],
                    "role": keys[2],
                    "date": keys[3],
                    "policy_over_opposite_reference": ratio_ref,
                    "policy_over_opposite_active": ratio_act,
                    "dlog_asym_active_vs_reference": float(np.log(ratio_act) - np.log(ratio_ref)),
                }
            )
    return pd.DataFrame(rows)


def estimate_pooled_asymmetry(corridor_daily: pd.DataFrame, sample_role: str = "main") -> pd.DataFrame:
    sub = corridor_daily[corridor_daily["sample_role"].eq(sample_role)].copy()
    if sub.empty:
        return pd.DataFrame()
    rows = []
    for cid, part in sub.groupby("cohort_id"):
        treat = part[part["role"].eq("treatment")]
        ctrl = part[part["role"].eq("control")]
        if treat.empty or ctrl.empty:
            continue
        for date, treat_day in treat.groupby("date"):
            ctrl_day = ctrl[ctrl["date"].eq(date)]
            if ctrl_day.empty:
                continue
            ddd = float(treat_day["dlog_asym_active_vs_reference"].iloc[0] - ctrl_day["dlog_asym_active_vs_reference"].mean())
            rows.append(
                {
                    "cohort_id": cid,
                    "road_key": treat_day["road_key"].iloc[0],
                    "window_key": treat_day["window_key"].iloc[0],
                    "sample_role": treat_day["sample_role"].iloc[0],
                    "date": date,
                    "ddd_dlog_asym": ddd,
                }
            )
    daily = pd.DataFrame(rows)
    if daily.empty:
        return pd.DataFrame()
    summary_rows = []
    for cid, part in daily.groupby("cohort_id"):
        mean_ddd = float(part["ddd_dlog_asym"].mean())
        se = float(part["ddd_dlog_asym"].std(ddof=1) / np.sqrt(len(part))) if len(part) > 1 else np.nan
        summary_rows.append(
            {
                "level": "cohort",
                "cohort_id": cid,
                "road_key": part["road_key"].iloc[0],
                "window_key": part["window_key"].iloc[0],
                "sample_role": part["sample_role"].iloc[0],
                "n_days": int(len(part)),
                "coef": mean_ddd,
                "std_error": se,
                "ci95_low": mean_ddd - 1.96 * se if np.isfinite(se) else np.nan,
                "ci95_high": mean_ddd + 1.96 * se if np.isfinite(se) else np.nan,
            }
        )
    pooled_mean = float(daily["ddd_dlog_asym"].mean())
    cohorts = daily["cohort_id"].unique()
    if len(cohorts) > 1:
        cohort_means = daily.groupby("cohort_id")["ddd_dlog_asym"].mean()
        pooled_se = float(cohort_means.std(ddof=1) / np.sqrt(len(cohort_means)))
    else:
        pooled_se = summary_rows[0]["std_error"] if summary_rows else np.nan
    summary_rows.append(
        {
            "level": "pooled",
            "cohort_id": "all",
            "road_key": "all",
            "window_key": "all",
            "sample_role": sample_role,
            "n_days": int(len(daily)),
            "coef": pooled_mean,
            "std_error": pooled_se,
            "ci95_low": pooled_mean - 1.96 * pooled_se if np.isfinite(pooled_se) else np.nan,
            "ci95_high": pooled_mean + 1.96 * pooled_se if np.isfinite(pooled_se) else np.nan,
        }
    )
    return pd.DataFrame(summary_rows)


def run_all_regressions(
    matched_panel: pd.DataFrame,
    matches: pd.DataFrame,
    sample_roles: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, object]]:
    pooled_rows = []
    cohort_rows = []
    event_rows = []
    diagnostics: dict[str, object] = {}
    for sample_role in sample_roles:
        for direction in DIRECTION_ORDER:
            for half_window in SHORT_WINDOWS:
                sample = build_regression_sample(
                    matched_panel,
                    matches,
                    half_window_min=half_window,
                    direction=direction,
                    sample_role=sample_role,
                )
                if sample.empty:
                    continue
                for include_cohort_fe in [False, True]:
                    coef, diag = fit_pooled_local_jump_with_cohort_fe(
                        sample,
                        outcome_col="speed_kmh",
                        model_name=f"pooled_{sample_role}_{direction}_{half_window}",
                        include_cohort_fe=include_cohort_fe,
                    )
                    if coef.empty:
                        continue
                    out = coef.iloc[0].to_dict()
                    out.update(
                        {
                            "sample_role": sample_role,
                            "direction_group": direction,
                            "half_window_min": half_window,
                            "outcome": "speed_kmh",
                            "specification": "pooled_local_jump",
                        }
                    )
                    pooled_rows.append(out)
                    diagnostics[f"{sample_role}_{direction}_{half_window}_cohortfe_{include_cohort_fe}"] = diag
                cohort_part = fit_cohort_specific_local_jump(
                    sample,
                    outcome_col="speed_kmh",
                    half_window_min=half_window,
                    direction=direction,
                )
                if not cohort_part.empty:
                    cohort_part["sample_role"] = sample_role
                    cohort_part["outcome"] = "speed_kmh"
                    cohort_part["specification"] = "cohort_local_jump"
                    cohort_rows.append(cohort_part)
                if half_window == 30 and direction == POLICY:
                    coef_es, diag_es = fit_event_study(
                        sample,
                        outcome_col="speed_kmh",
                        treatment_col="treated_segment",
                        unit_col="global_unit_id",
                        model_name=f"pooled_event_{sample_role}_{direction}",
                    )
                    if not coef_es.empty:
                        coef_es = coef_es.assign(
                            sample_role=sample_role,
                            direction_group=direction,
                            half_window_min=half_window,
                            specification="pooled_event_study",
                        )
                        event_rows.append(coef_es)
                        diagnostics[f"event_{sample_role}_{direction}"] = diag_es
    return (
        pd.DataFrame(pooled_rows),
        pd.concat(cohort_rows, ignore_index=True) if cohort_rows else pd.DataFrame(),
        pd.concat(event_rows, ignore_index=True) if event_rows else pd.DataFrame(),
        diagnostics,
    )


def run_placebo_regressions(matched_panel: pd.DataFrame, matches: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for direction in DIRECTION_ORDER:
        sample = build_regression_sample(
            matched_panel,
            matches,
            half_window_min=30,
            direction=direction,
            sample_role="all",
            event_start_override=PLACEBO_EVENT_START_MIN,
        )
        if sample.empty:
            continue
        coef, _diag = fit_pooled_local_jump_with_cohort_fe(
            sample,
            outcome_col="speed_kmh",
            model_name=f"placebo_1200_{direction}",
            include_cohort_fe=False,
        )
        if coef.empty:
            continue
        out = coef.iloc[0].to_dict()
        out.update(
            {
                "placebo_type": "non_policy_noon",
                "event_start_min": PLACEBO_EVENT_START_MIN,
                "direction_group": direction,
                "half_window_min": 30,
                "specification": "pooled_local_jump_placebo",
            }
        )
        rows.append(out)
    liuxian = matched_panel[matched_panel["road_key"].eq(PLACEBO_COHORT_KEY)].copy()
    if not liuxian.empty:
        for direction in DIRECTION_ORDER:
            sample = build_regression_sample(
                liuxian,
                matches,
                half_window_min=30,
                direction=direction,
                sample_role="placebo_only",
            )
            if sample.empty:
                continue
            coef, _diag = fit_pooled_local_jump_with_cohort_fe(
                sample,
                outcome_col="speed_kmh",
                model_name=f"liuxian_pre_shock_{direction}",
                include_cohort_fe=False,
            )
            if coef.empty:
                continue
            out = coef.iloc[0].to_dict()
            out.update(
                {
                    "placebo_type": "liuxian_pre_shock",
                    "event_start_min": int(sample["cohort_start_min"].iloc[0]),
                    "direction_group": direction,
                    "half_window_min": 30,
                    "specification": "pooled_local_jump_placebo",
                }
            )
            rows.append(out)
    return pd.DataFrame(rows)


def plot_pooled_event_study(event_df: pd.DataFrame, output_path: Path, sample_role: str = "main") -> None:
    sub = event_df[
        (event_df["sample_role"].eq(sample_role))
        & (event_df["direction_group"].eq(POLICY))
        & (event_df["specification"].eq("pooled_event_study"))
    ].copy()
    if sub.empty:
        return
    sub = sub.sort_values("event_bin_min")
    fig, ax = plt.subplots(figsize=(8.5, 4.8))
    x = sub["event_bin_min"] / 60.0
    y = sub["coef"]
    se = sub["std_error"]
    ax.errorbar(x, y, yerr=1.96 * se, fmt="o-", color="#1f77b4", ecolor="#555555", capsize=2)
    ax.axhline(0, color="#222222", linewidth=0.8)
    ax.axvline(0, color="#888888", linewidth=0.8, linestyle="--")
    ax.set_xlabel("Event time (hours relative to cohort window start)")
    ax.set_ylabel("Treat − control speed (km/h)")
    ax.set_title("Pooled PSM-matched event study (policy direction, ±30 min sample)", loc="left")
    ax.grid(axis="y", color="#e5e5e5", linewidth=0.5)
    fig.tight_layout()
    save_figure(fig, output_path)


def plot_cohort_heterogeneity(cohort_df: pd.DataFrame, output_path: Path, sample_role: str = "main") -> None:
    sub = cohort_df[
        (cohort_df["sample_role"].eq(sample_role))
        & (cohort_df["direction_group"].eq(POLICY))
        & (cohort_df["half_window_min"].eq(30))
        & (cohort_df["specification"].eq("cohort_local_jump"))
    ].copy()
    if sub.empty:
        return
    sub = sub.sort_values("coef")
    labels = sub.apply(
        lambda r: f"{r['road_name_cn']}\n{r['operation_window_label']}" if pd.notna(r.get("operation_window_label")) else r["cohort_id"],
        axis=1,
    )
    fig, ax = plt.subplots(figsize=(8.5, max(4.5, 0.45 * len(sub))))
    y = np.arange(len(sub))
    ax.errorbar(
        sub["coef"],
        y,
        xerr=1.96 * sub["std_error"],
        fmt="o",
        color="#1f77b4",
        ecolor="#555555",
        capsize=2,
    )
    ax.axvline(0, color="#222222", linewidth=0.8)
    pooled = sub["coef"].mean()
    ax.axvline(pooled, color="#d62728", linewidth=1.0, linestyle="--", label=f"Cohort mean = {pooled:.2f}")
    ax.set_yticks(y)
    ax.set_yticklabels(labels)
    ax.set_xlabel("Local jump at window start (km/h)")
    ax.set_title("Cohort-specific PSM-matched local jumps (policy direction, ±30 min)", loc="left")
    ax.legend(frameon=False, fontsize=8)
    ax.grid(axis="x", color="#e5e5e5", linewidth=0.5)
    fig.tight_layout()
    save_figure(fig, output_path)


def plot_match_balance(balance_df: pd.DataFrame, output_path: Path) -> None:
    sub = balance_df[balance_df["balance_stage"].eq("after_matching")].copy()
    if sub.empty:
        return
    metrics = [
        ("inactive_profile_corr", "Inactive profile corr"),
        ("reference_speed_kmh", "Reference speed (km/h)"),
        ("min_distance_m", "Distance to treatment (m)"),
    ]
    fig, axes = plt.subplots(1, len(metrics), figsize=(10.5, 3.8))
    if len(metrics) == 1:
        axes = [axes]
    for ax, (col, label) in zip(axes, metrics):
        treat = sub[sub["stage"].eq("treatment")][col]
        ctrl = sub[sub["stage"].eq("matched_control")][col]
        ax.boxplot(
            [ctrl.dropna(), treat.dropna()],
            tick_labels=["Matched controls", "Treatment"],
            showfliers=False,
        )
        ax.set_title(label, loc="left")
        ax.grid(axis="y", color="#e5e5e5", linewidth=0.5)
    fig.suptitle("PSM balance after matching (all cohorts stacked)", y=1.02, fontsize=11)
    fig.tight_layout()
    save_figure(fig, output_path)


def main() -> None:
    args = parse_args()
    configure_plot()
    dirs = ensure_dirs(args.output_dir)

    cohort_meta = cohort_meta_rows(include_liuxian_main=args.include_liuxian_main)
    treatment_gdf = gpd.read_file(args.v1_dir / "data/matched_treatment_segments.geojson")
    roads = add_road_attributes(gpd.read_file(args.roads))
    speed_ids = read_speed_ids(args.speed)
    all_exclusion = build_all_tidal_exclusion(args.tidal_dir)
    control_pool = build_expanded_control_pool(roads, treatment_gdf, all_exclusion, speed_ids)
    treat_attrs = attach_treatment_attrs(treatment_gdf)

    all_corridor_ids = set(control_pool["corridor_id"].astype(str)) | set(treat_attrs["corridor_id"].astype(str))
    base_panel = build_exploration_panel(args.speed, treat_attrs, control_pool, all_corridor_ids)
    if base_panel.empty:
        raise RuntimeError("Exploration panel is empty; cannot run pooled PSM DID.")

    match_rows = []
    balance_rows = []
    for _, meta in cohort_meta.iterrows():
        for direction in DIRECTION_ORDER:
            matched, balance = match_controls_for_cohort(base_panel, control_pool, meta, direction=direction)
            if balance is not None and not balance.empty:
                balance = balance.assign(
                    cohort_id=meta["cohort_id"],
                    road_key=meta["road_key"],
                    window_key=meta["window_key"],
                    direction_group=direction,
                )
                balance_rows.append(balance)
            if matched is None or matched.empty:
                continue
            match_rows.append(matched)
    matches = pd.concat(match_rows, ignore_index=True) if match_rows else pd.DataFrame()
    balance_table = pd.concat(balance_rows, ignore_index=True) if balance_rows else pd.DataFrame()

    matched_panel = build_matched_segment_panel(base_panel, matches, cohort_meta)
    if "road_name_cn" not in matched_panel.columns:
        matched_panel = matched_panel.merge(
            cohort_meta[["cohort_id", "road_name_cn"]],
            on="cohort_id",
            how="left",
        )

    pooled_results, cohort_results, event_results, diagnostics = run_all_regressions(
        matched_panel,
        matches,
        sample_roles=["main"],
    )
    corridor_daily = build_corridor_daily_asymmetry(matched_panel, matches, cohort_meta)
    asymmetry_results = estimate_pooled_asymmetry(corridor_daily, sample_role="main")
    placebo_results = run_placebo_regressions(matched_panel, matches)

    matched_panel.to_csv(dirs["data"] / "pooled_matched_segment_panel.csv", index=False, encoding="utf-8-sig")
    matches.to_csv(dirs["metrics"] / "psm_matches.csv", index=False, encoding="utf-8-sig")
    balance_table.to_csv(dirs["metrics"] / "psm_balance_table.csv", index=False, encoding="utf-8-sig")
    cohort_meta.to_csv(dirs["metrics"] / "cohort_catalog.csv", index=False, encoding="utf-8-sig")
    pooled_results.to_csv(dirs["metrics"] / "pooled_local_jump_results.csv", index=False, encoding="utf-8-sig")
    cohort_results.to_csv(dirs["metrics"] / "pooled_cohort_heterogeneity.csv", index=False, encoding="utf-8-sig")
    event_results.to_csv(dirs["metrics"] / "pooled_event_study_coefficients.csv", index=False, encoding="utf-8-sig")
    corridor_daily.to_csv(dirs["data"] / "pooled_corridor_daily_asymmetry.csv", index=False, encoding="utf-8-sig")
    asymmetry_results.to_csv(dirs["metrics"] / "pooled_asymmetry_ddd.csv", index=False, encoding="utf-8-sig")
    placebo_results.to_csv(dirs["metrics"] / "pooled_placebo_results.csv", index=False, encoding="utf-8-sig")

    plot_pooled_event_study(event_results, dirs["summary"] / "pooled_event_study_policy.png")
    plot_cohort_heterogeneity(cohort_results, dirs["summary"] / "pooled_cohort_heterogeneity_policy.png")
    plot_match_balance(balance_table, dirs["summary"] / "psm_balance_after_matching.png")

    spec = {
        "version": "shenzhen_multi_tidal_pooled_psm_did_v1",
        "design": "Stacked multi-site within-day DID with cohort-specific event time zero.",
        "matching": {
            "method": "Mahalanobis nearest-neighbor on corridor pre-window covariates",
            "max_matches": MAX_MATCHES,
            "min_inactive_corr": MIN_INACTIVE_CORR,
            "min_distance_m": MIN_MATCH_DISTANCE_M,
            "features": [
                "inactive_profile_corr",
                "reference_speed_kmh",
                "log_length_m",
                "axis_diff_deg",
                "min_distance_m",
            ],
        },
        "main_regression": {
            "outcome": "segment speed_kmh",
            "treatment": "treated_segment × post_start",
            "fixed_effects": ["global segment id", "date", "minute_of_day"],
            "cluster": "global segment id",
            "short_windows_minutes": SHORT_WINDOWS,
        },
        "sample": {
            "baidu_week": "2025-05-19 to 2025-05-25",
            "include_liuxian_main": args.include_liuxian_main,
            "liuxian_role": "placebo_only unless --include-liuxian-main",
        },
        "interpretation": (
            "Post-only directional pattern only. Pooled beta is a common treat×post-start effect "
            "after PSM-style matching and high-dimensional FE projection."
        ),
        "diagnostics": diagnostics,
    }
    with (dirs["metrics"] / "pooled_psm_did_specification.json").open("w", encoding="utf-8") as fh:
        json.dump(spec, fh, ensure_ascii=False, indent=2, default=str)

    print(f"Wrote pooled PSM DID outputs to {args.output_dir}")
    if not pooled_results.empty:
        focus = pooled_results[
            (pooled_results["sample_role"].eq("main"))
            & (pooled_results["direction_group"].eq(POLICY))
            & (pooled_results["half_window_min"].eq(30))
            & (~pooled_results["include_cohort_fe"])
        ]
        if not focus.empty:
            row = focus.iloc[0]
            print(
                f"Main pooled policy local jump (±30 min): {row['coef']:.3f} km/h "
                f"(SE={row['std_error']:.3f}, n={int(row['n_obs'])})"
            )


if __name__ == "__main__":
    main()
