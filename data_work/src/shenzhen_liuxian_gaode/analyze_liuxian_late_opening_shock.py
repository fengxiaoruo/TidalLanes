from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_INPUT_DIR = PROJECT_ROOT / "data_work/raw_data/Shenzhen_Gaode"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data_work/outputs/shenzhen_liuxian_gaode_late_opening_shock_v1"

POLICY_DATE = pd.Timestamp("2026-03-23")
SHOCK_DATE = pd.Timestamp("2026-04-24")
PLANNED_OPEN_MINUTE = 17 * 60 + 30
ASSUMED_ACTUAL_OPEN_MINUTE = 18 * 60 + 15
POLICY_CHAIN = "eastbound_chain"

CHAIN_LABELS = {
    "eastbound_chain": "Policy direction: SW -> NE / west-to-east",
    "westbound_chain": "Opposite direction: NE -> SW / east-to-west",
}
CHAIN_COLORS = {
    "eastbound_chain": "#1f77b4",
    "westbound_chain": "#d55e00",
}
WEEK_LABELS = {
    "week_20260302_20260308": "Pre-policy week: 2026-03-02 to 03-08",
    "week_20260413_20260419": "Normal post week: 2026-04-13 to 04-19",
    "week_20260420_20260426": "Shock week: 2026-04-20 to 04-26",
}
WEEK_COLORS = {
    "week_20260302_20260308": "#4c78a8",
    "week_20260413_20260419": "#54a24b",
    "week_20260420_20260426": "#c44e52",
}
WINDOWS = {
    "ref_1700_1730": (17 * 60, 17 * 60 + 30),
    "delayed_strict_1730_1810": (17 * 60 + 30, 18 * 60 + 10),
    "delayed_extended_1730_1820": (17 * 60 + 30, 18 * 60 + 20),
    "post_actual_1820_2000": (18 * 60 + 20, 20 * 60),
    "full_active_1730_2000": (17 * 60 + 30, 20 * 60),
}
WINDOW_LABELS = {
    "ref_1700_1730": "17:00-17:30 reference",
    "delayed_strict_1730_1810": "17:30-18:10 delayed, strict",
    "delayed_extended_1730_1820": "17:30-18:20 delayed, incl. transition",
    "post_actual_1820_2000": "18:20-20:00 after actual opening",
    "full_active_1730_2000": "17:30-20:00 full planned active",
}
CONTROL_GROUPS = {
    "all_post_normal_weekdays": {
        "label": "All normal post weekdays, 2026-04-13 to 04-23",
        "dates": [
            "2026-04-13",
            "2026-04-14",
            "2026-04-15",
            "2026-04-16",
            "2026-04-17",
            "2026-04-20",
            "2026-04-21",
            "2026-04-22",
            "2026-04-23",
        ],
    },
    "same_week_mon_thu": {
        "label": "Same week Mon-Thu, 2026-04-20 to 04-23",
        "dates": ["2026-04-20", "2026-04-21", "2026-04-22", "2026-04-23"],
    },
    "prior_friday": {
        "label": "Prior Friday only, 2026-04-17",
        "dates": ["2026-04-17"],
    },
}
POST_FRIDAY_BASELINE = pd.Timestamp("2026-04-17")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze the 2026-04-24 Liuxian Avenue late-opening shock."
    )
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def configure_plot() -> None:
    plt.rcParams.update(
        {
            "font.sans-serif": [
                "PingFang SC",
                "Hiragino Sans GB",
                "Microsoft YaHei",
                "Noto Sans CJK SC",
                "Arial Unicode MS",
                "DejaVu Sans",
            ],
            "axes.unicode_minus": False,
            "font.size": 10,
            "axes.titlesize": 10,
            "axes.labelsize": 9,
            "legend.fontsize": 8,
            "figure.dpi": 220,
            "savefig.dpi": 220,
        }
    )


def ensure_output_dirs(output_dir: Path) -> dict[str, Path]:
    dirs = {
        "root": output_dir,
        "figures": output_dir / "figures",
        "data": output_dir / "data",
        "metrics": output_dir / "metrics",
    }
    for path in dirs.values():
        path.mkdir(parents=True, exist_ok=True)
    return dirs


def save_figure(fig: plt.Figure, output_path: Path) -> None:
    fig.savefig(output_path, bbox_inches="tight")
    if output_path.suffix.lower() == ".png":
        fig.savefig(output_path.with_suffix(".pdf"), bbox_inches="tight")
        fig.savefig(output_path.with_suffix(".svg"), bbox_inches="tight")


def parse_coord(coord: str) -> tuple[float, float]:
    lon, lat = str(coord).split(",")
    return float(lon), float(lat)


def bearing_degrees(start: str, end: str) -> float:
    lon1, lat1 = parse_coord(start)
    lon2, lat2 = parse_coord(end)
    dy = (lat2 - lat1) * 111_320
    dx = (lon2 - lon1) * 111_320 * math.cos(math.radians((lat1 + lat2) / 2))
    return (math.degrees(math.atan2(dx, dy)) + 360) % 360


def week_id_from_datetime(series: pd.Series) -> str:
    return f"week_{series.min().strftime('%Y%m%d')}_{series.max().strftime('%Y%m%d')}"


def read_one_file(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, encoding="utf-8-sig", dtype=str)
    for col in df.columns:
        df[col] = df[col].astype(str).str.strip()
    df["datetime"] = pd.to_datetime(
        df["日期"].str.zfill(8) + df["时间"].str.zfill(4),
        format="%Y%m%d%H%M",
        errors="raise",
    )
    df["date"] = pd.to_datetime(df["datetime"].dt.date)
    df["date_str"] = df["date"].dt.strftime("%Y-%m-%d")
    df["dow"] = df["datetime"].dt.dayofweek
    df["dow_name"] = df["datetime"].dt.day_name()
    df["day_type"] = np.where(df["dow"] < 5, "Weekday", "Weekend")
    df["minute_of_day"] = df["datetime"].dt.hour * 60 + df["datetime"].dt.minute
    df["time_label"] = df["datetime"].dt.strftime("%H:%M")
    df["speed_kmh"] = pd.to_numeric(df["速度"], errors="coerce")
    df["delay_index"] = pd.to_numeric(df["拥堵延时指数"], errors="coerce")
    df["segment_length_m"] = pd.to_numeric(df["路段长度"], errors="coerce")
    df["source_file"] = path.name
    df["purchase_week"] = week_id_from_datetime(df["datetime"])
    return df


def load_panel(input_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    files = sorted(input_dir.glob("*.csv"))
    if not files:
        raise FileNotFoundError(f"No CSV files found under {input_dir}")

    frames = [read_one_file(path) for path in files]
    panel = pd.concat(frames, ignore_index=True)
    panel = panel[(panel["speed_kmh"] > 0) & (panel["segment_length_m"] > 0)].copy()
    panel["segment_travel_time_min"] = panel["segment_length_m"] / 1000 / panel["speed_kmh"] * 60
    panel["log_speed"] = np.log(panel["speed_kmh"])
    panel["segment_id"] = panel["路段编号"].astype(str)

    audit_rows = []
    for file_name, group in panel.groupby("source_file", sort=True):
        seg = group[
            [
                "segment_id",
                "路段名称（唯一）",
                "路段方向",
                "segment_length_m",
                "路段起点坐标",
                "路段终点坐标",
            ]
        ].drop_duplicates()
        n_days = group["date_str"].nunique()
        n_segments = seg["segment_id"].nunique()
        expected = n_days * n_segments * 144
        audit_rows.append(
            {
                "source_file": file_name,
                "purchase_week": group["purchase_week"].iloc[0],
                "date_min": group["date_str"].min(),
                "date_max": group["date_str"].max(),
                "n_days": int(n_days),
                "n_segments": int(n_segments),
                "directed_length_m": float(seg["segment_length_m"].sum()),
                "n_rows": int(len(group)),
                "expected_full_10min_rows": int(expected),
                "obs_share": float(len(group) / expected),
            }
        )
    return panel, pd.DataFrame(audit_rows).sort_values("date_min")


def build_segment_inventory(panel: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "segment_id",
        "路段编号",
        "路段名称（唯一）",
        "路段方向",
        "segment_length_m",
        "路段起点坐标",
        "路段终点坐标",
    ]
    inventory = panel[cols].drop_duplicates().copy()
    inventory["bearing_deg"] = inventory.apply(
        lambda row: bearing_degrees(row["路段起点坐标"], row["路段终点坐标"]),
        axis=1,
    )
    inventory[["start_lon", "start_lat"]] = inventory["路段起点坐标"].apply(
        lambda coord: pd.Series(parse_coord(coord))
    )
    inventory[["end_lon", "end_lat"]] = inventory["路段终点坐标"].apply(
        lambda coord: pd.Series(parse_coord(coord))
    )
    inventory["center_lon"] = (inventory["start_lon"] + inventory["end_lon"]) / 2
    inventory["center_lat"] = (inventory["start_lat"] + inventory["end_lat"]) / 2
    inventory["position_score"] = inventory["center_lon"] + inventory["center_lat"]
    inventory["chain_direction"] = np.where(
        (inventory["bearing_deg"] > 0) & (inventory["bearing_deg"] < 180),
        "eastbound_chain",
        "westbound_chain",
    )
    inventory["chain_label"] = inventory["chain_direction"].map(CHAIN_LABELS)

    presence = (
        panel[["segment_id", "purchase_week"]]
        .drop_duplicates()
        .assign(present=1)
        .pivot_table(index="segment_id", columns="purchase_week", values="present", fill_value=0)
        .reset_index()
    )
    inventory = inventory.merge(presence, on="segment_id", how="left")
    week_cols = [col for col in inventory.columns if str(col).startswith("week_")]
    for col in week_cols:
        inventory[col] = inventory[col].map({1: "Y", 0: ""})
    return inventory.sort_values(["chain_direction", "position_score", "segment_id"]).copy()


def attach_chain_direction(panel: pd.DataFrame, inventory: pd.DataFrame) -> pd.DataFrame:
    mapping = inventory.set_index("segment_id")["chain_direction"]
    out = panel.copy()
    out["chain_direction"] = out["segment_id"].map(mapping)
    out["chain_label"] = out["chain_direction"].map(CHAIN_LABELS)
    out["policy_direction"] = out["chain_direction"].eq(POLICY_CHAIN).astype(int)
    out["event_minute_1730"] = out["minute_of_day"] - PLANNED_OPEN_MINUTE
    out["is_shock_day"] = out["date"].eq(SHOCK_DATE).astype(int)
    out["is_post_policy"] = out["date"].ge(POLICY_DATE).astype(int)
    return out


def filter_segment_scope(panel: pd.DataFrame, scope: str) -> pd.DataFrame:
    if scope == "common_all_weeks":
        sets = {
            week: set(group["segment_id"].unique()) for week, group in panel.groupby("purchase_week")
        }
        keep = set.intersection(*sets.values())
        return panel[panel["segment_id"].isin(keep)].copy()
    if scope == "post_16_segments":
        return panel[
            panel["purchase_week"].isin(["week_20260413_20260419", "week_20260420_20260426"])
        ].copy()
    raise ValueError(f"Unknown segment scope: {scope}")


def build_corridor(panel: pd.DataFrame, scope: str) -> pd.DataFrame:
    sample = filter_segment_scope(panel, scope)
    expected_counts = sample.groupby("chain_direction")["segment_id"].nunique().to_dict()
    corridor = (
        sample.groupby(
            [
                "chain_direction",
                "chain_label",
                "policy_direction",
                "purchase_week",
                "date",
                "date_str",
                "dow",
                "dow_name",
                "day_type",
                "datetime",
                "minute_of_day",
                "time_label",
                "event_minute_1730",
                "is_shock_day",
                "is_post_policy",
            ],
            as_index=False,
        )
        .agg(
            travel_time_min=("segment_travel_time_min", "sum"),
            total_length_m=("segment_length_m", "sum"),
            n_segments=("segment_id", "nunique"),
            mean_segment_speed_kmh=("speed_kmh", "mean"),
        )
        .sort_values(["chain_direction", "datetime"])
    )
    corridor = corridor[
        corridor.apply(lambda row: row["n_segments"] == expected_counts[row["chain_direction"]], axis=1)
    ].copy()
    corridor["corridor_speed_kmh"] = corridor["total_length_m"] / 1000 / (
        corridor["travel_time_min"] / 60
    )
    corridor["scope"] = scope
    return corridor


def wide_directional_corridor(corridor: pd.DataFrame) -> pd.DataFrame:
    wide = corridor.pivot_table(
        index=[
            "scope",
            "purchase_week",
            "date",
            "date_str",
            "dow",
            "dow_name",
            "day_type",
            "datetime",
            "minute_of_day",
            "time_label",
            "event_minute_1730",
            "is_shock_day",
            "is_post_policy",
        ],
        columns="chain_direction",
        values="corridor_speed_kmh",
        aggfunc="first",
    ).reset_index()
    wide.columns.name = None
    wide = wide.dropna(subset=["eastbound_chain", "westbound_chain"]).copy()
    wide["policy_speed_kmh"] = wide["eastbound_chain"]
    wide["opposite_speed_kmh"] = wide["westbound_chain"]
    wide["policy_minus_opposite_kmh"] = wide["policy_speed_kmh"] - wide["opposite_speed_kmh"]
    wide["policy_over_opposite_speed"] = wide["policy_speed_kmh"] / wide["opposite_speed_kmh"]
    wide["log_policy_over_opposite_speed"] = np.log(wide["policy_over_opposite_speed"])
    return wide.sort_values(["datetime"]).copy()


def window_name(minute: int) -> str | None:
    for name, (lo, hi) in WINDOWS.items():
        if lo <= minute < hi:
            return name
    return None


def build_window_means(wide: pd.DataFrame) -> pd.DataFrame:
    rows = []
    group_cols = [
        "scope",
        "purchase_week",
        "date_str",
        "dow",
        "dow_name",
        "day_type",
        "is_shock_day",
        "is_post_policy",
    ]
    for window, (lo, hi) in WINDOWS.items():
        data = wide[(wide["minute_of_day"] >= lo) & (wide["minute_of_day"] < hi)].copy()
        grouped = (
            data.groupby(group_cols, as_index=False)
            .agg(
                policy_speed_kmh=("policy_speed_kmh", "mean"),
                opposite_speed_kmh=("opposite_speed_kmh", "mean"),
                policy_minus_opposite_kmh=("policy_minus_opposite_kmh", "mean"),
                policy_over_opposite_speed=("policy_over_opposite_speed", "mean"),
                log_policy_over_opposite_speed=("log_policy_over_opposite_speed", "mean"),
                n_10min=("policy_speed_kmh", "size"),
            )
            .sort_values(["scope", "date_str"])
        )
        grouped["window"] = window
        grouped["window_label"] = WINDOW_LABELS[window]
        rows.append(grouped)
    return pd.concat(rows, ignore_index=True).sort_values(["scope", "date_str", "window"])


def build_shock_did(window_means: pd.DataFrame) -> pd.DataFrame:
    post = window_means[
        (window_means["scope"] == "post_16_segments")
        & (window_means["day_type"] == "Weekday")
        & (window_means["is_post_policy"] == 1)
    ].copy()
    ref_window = "ref_1700_1730"
    shock = post[post["date_str"].eq(SHOCK_DATE.strftime("%Y-%m-%d"))].copy()
    rows = []
    for control_name, config in CONTROL_GROUPS.items():
        controls = post[post["date_str"].isin(config["dates"])].copy()
        if controls.empty:
            continue
        control_summary = (
            controls.groupby("window", as_index=False)
            .agg(
                control_policy_speed_kmh=("policy_speed_kmh", "mean"),
                control_opposite_speed_kmh=("opposite_speed_kmh", "mean"),
                control_gap_kmh=("policy_minus_opposite_kmh", "mean"),
                control_log_ratio=("log_policy_over_opposite_speed", "mean"),
                n_control_dates=("date_str", "nunique"),
            )
        )
        shock_summary = shock[
            [
                "window",
                "policy_speed_kmh",
                "opposite_speed_kmh",
                "policy_minus_opposite_kmh",
                "log_policy_over_opposite_speed",
            ]
        ].rename(
            columns={
                "policy_speed_kmh": "shock_policy_speed_kmh",
                "opposite_speed_kmh": "shock_opposite_speed_kmh",
                "policy_minus_opposite_kmh": "shock_gap_kmh",
                "log_policy_over_opposite_speed": "shock_log_ratio",
            }
        )
        merged = shock_summary.merge(control_summary, on="window", how="inner")
        if ref_window not in set(merged["window"]):
            raise ValueError("Missing reference window for shock DID.")
        ref = merged[merged["window"].eq(ref_window)].iloc[0]
        for row in merged.itertuples(index=False):
            gap_abnormal = row.shock_gap_kmh - row.control_gap_kmh
            log_ratio_abnormal = row.shock_log_ratio - row.control_log_ratio
            did_gap = (
                row.shock_gap_kmh
                - ref.shock_gap_kmh
                - (row.control_gap_kmh - ref.control_gap_kmh)
            )
            did_log_ratio = (
                row.shock_log_ratio
                - ref.shock_log_ratio
                - (row.control_log_ratio - ref.control_log_ratio)
            )
            rows.append(
                {
                    "control_group": control_name,
                    "control_group_label": config["label"],
                    "window": row.window,
                    "window_label": WINDOW_LABELS[row.window],
                    "shock_policy_speed_kmh": row.shock_policy_speed_kmh,
                    "shock_opposite_speed_kmh": row.shock_opposite_speed_kmh,
                    "shock_gap_kmh": row.shock_gap_kmh,
                    "control_policy_speed_kmh": row.control_policy_speed_kmh,
                    "control_opposite_speed_kmh": row.control_opposite_speed_kmh,
                    "control_gap_kmh": row.control_gap_kmh,
                    "gap_abnormal_shock_minus_control_kmh": gap_abnormal,
                    "did_gap_vs_1700_1730_kmh": did_gap,
                    "log_ratio_abnormal_shock_minus_control": log_ratio_abnormal,
                    "did_log_ratio_vs_1700_1730": did_log_ratio,
                    "implied_normal_operation_support_kmh": -gap_abnormal
                    if row.window.startswith("delayed")
                    else np.nan,
                    "n_control_dates": int(row.n_control_dates),
                }
            )
    return pd.DataFrame(rows)


def cluster_covariance(x: np.ndarray, residual: np.ndarray, clusters: np.ndarray) -> np.ndarray:
    xtx_inv = np.linalg.pinv(x.T @ x)
    meat = np.zeros((x.shape[1], x.shape[1]))
    for cluster in pd.unique(clusters):
        loc = clusters == cluster
        score = x[loc].T @ residual[loc]
        meat += np.outer(score, score)
    n_obs, n_params = x.shape
    n_clusters = len(pd.unique(clusters))
    if n_clusters > 1 and n_obs > n_params:
        correction = (n_clusters / (n_clusters - 1)) * ((n_obs - 1) / (n_obs - n_params))
    else:
        correction = 1.0
    return correction * xtx_inv @ meat @ xtx_inv


def fit_shock_event_study(panel: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, object]]:
    post = filter_segment_scope(panel, "post_16_segments")
    sample = post[
        (post["day_type"] == "Weekday")
        & (post["date"] <= SHOCK_DATE)
        & (post["minute_of_day"] >= 14 * 60 + 30)
        & (post["minute_of_day"] <= 21 * 60 + 30)
    ].copy()
    sample["event_bin"] = sample["event_minute_1730"].astype(int)
    reference_bin = -10
    event_bins = sorted(int(x) for x in sample["event_bin"].unique())
    estimated_bins = [x for x in event_bins if x != reference_bin]
    sample["shock_day"] = sample["date"].eq(SHOCK_DATE).astype(int)
    sample["date_id"] = sample["date"].dt.strftime("%Y%m%d")
    sample["minute_id"] = sample["minute_of_day"].astype(str)

    parts = [
        pd.Series(1.0, index=sample.index, name="const"),
        pd.get_dummies(sample["segment_id"], prefix="segment", drop_first=True, dtype=float),
        pd.get_dummies(sample["date_id"], prefix="date", drop_first=True, dtype=float),
        pd.get_dummies(sample["minute_id"], prefix="minute", drop_first=True, dtype=float),
        (sample["policy_direction"].eq(1) & sample["shock_day"].eq(1))
        .astype(float)
        .rename("policy_x_shock_day"),
    ]
    triple_columns: dict[int, str] = {}
    for event_bin in estimated_bins:
        event_match = sample["event_bin"].eq(event_bin)
        parts.append(
            (sample["policy_direction"].eq(1) & event_match)
            .astype(float)
            .rename(f"policy_x_event_{event_bin}")
        )
        parts.append(
            (sample["shock_day"].eq(1) & event_match)
            .astype(float)
            .rename(f"shock_x_event_{event_bin}")
        )
        col = f"shock_x_policy_x_event_{event_bin}"
        parts.append(
            (sample["shock_day"].eq(1) & sample["policy_direction"].eq(1) & event_match)
            .astype(float)
            .rename(col)
        )
        triple_columns[event_bin] = col

    x_df = pd.concat(parts, axis=1)
    x = x_df.to_numpy(dtype=float)
    y = sample["speed_kmh"].to_numpy(dtype=float)
    beta, *_ = np.linalg.lstsq(x, y, rcond=None)
    fitted = x @ beta
    residual = y - fitted
    vcov = cluster_covariance(x, residual, sample["segment_id"].to_numpy())
    se = np.sqrt(np.maximum(np.diag(vcov), 0))
    beta_lookup = dict(zip(x_df.columns, beta))
    se_lookup = dict(zip(x_df.columns, se))

    rows = []
    for event_bin in event_bins:
        if event_bin == reference_bin:
            coef = 0.0
            std_err = np.nan
        else:
            col = triple_columns[event_bin]
            coef = float(beta_lookup[col])
            std_err = float(se_lookup[col])
        rows.append(
            {
                "event_minute": event_bin,
                "clock_time": f"{(PLANNED_OPEN_MINUTE + event_bin) // 60:02d}:{(PLANNED_OPEN_MINUTE + event_bin) % 60:02d}",
                "reference_event_minute": reference_bin,
                "coef_speed_kmh": coef,
                "std_error_speed_kmh": std_err,
                "ci95_low_speed_kmh": coef - 1.96 * std_err if np.isfinite(std_err) else np.nan,
                "ci95_high_speed_kmh": coef + 1.96 * std_err if np.isfinite(std_err) else np.nan,
                "window": "pre_start"
                if event_bin < 0
                else "late_not_open"
                if event_bin < ASSUMED_ACTUAL_OPEN_MINUTE - PLANNED_OPEN_MINUTE
                else "after_actual_open",
            }
        )
    diagnostics = {
        "specification": (
            "speed_i,d,m = segment FE + date FE + minute FE + Policy_i x event_bin_m "
            "+ ShockDay_d x event_bin_m + Policy_i x ShockDay_d "
            "+ beta_k Policy_i x ShockDay_d x event_bin_m + error"
        ),
        "sample": "Post-policy weekdays from 2026-04-13 through 2026-04-24, 14:30-21:30",
        "outcome": "segment speed in km/h",
        "shock_date": SHOCK_DATE.strftime("%Y-%m-%d"),
        "planned_open_time": "17:30",
        "assumed_actual_open_time": "18:15",
        "reference_event_bin": reference_bin,
        "reference_clock_time": "17:20",
        "n_obs": int(len(sample)),
        "n_segments": int(sample["segment_id"].nunique()),
        "n_dates": int(sample["date_id"].nunique()),
        "n_parameters": int(x.shape[1]),
        "n_clusters": int(sample["segment_id"].nunique()),
        "r_squared": float(1 - np.sum(residual**2) / np.sum((y - y.mean()) ** 2)),
        "cluster_warning": "Only 16 segment clusters; confidence intervals are diagnostic, not definitive.",
    }
    return pd.DataFrame(rows), diagnostics


def add_operation_markers(ax: plt.Axes) -> None:
    ax.axvline(17.5, color="#222222", linestyle="--", linewidth=0.9)
    ax.axvline(18.25, color="#222222", linestyle=":", linewidth=0.9)
    ax.axvspan(17.5, 18.25, color="#f2c14e", alpha=0.18, linewidth=0)
    ax.axvspan(18.25, 20.0, color="#7cb7a8", alpha=0.13, linewidth=0)


def plot_three_week_diurnal(corridor: pd.DataFrame, output_path: Path) -> None:
    data = corridor[
        (corridor["day_type"] == "Weekday")
        & (corridor["minute_of_day"] >= 14 * 60 + 30)
        & (corridor["minute_of_day"] <= 21 * 60 + 30)
    ].copy()
    profile = (
        data.groupby(["purchase_week", "chain_direction", "minute_of_day"], as_index=False)
        .agg(corridor_speed_kmh=("corridor_speed_kmh", "mean"))
    )
    profile["hour"] = profile["minute_of_day"] / 60

    fig, axes = plt.subplots(2, 1, figsize=(9.8, 6.1), sharex=True, sharey=True)
    for ax, chain in zip(axes, ["eastbound_chain", "westbound_chain"]):
        add_operation_markers(ax)
        for week, group in profile[profile["chain_direction"].eq(chain)].groupby("purchase_week"):
            group = group.sort_values("minute_of_day")
            ax.plot(
                group["hour"],
                group["corridor_speed_kmh"],
                marker="o",
                markersize=2.6,
                linewidth=1.35,
                color=WEEK_COLORS.get(week, "#666666"),
                label=WEEK_LABELS.get(week, week),
            )
        ax.set_title(CHAIN_LABELS[chain], loc="left", fontweight="bold")
        ax.set_ylabel("Corridor speed (km/h)")
        ax.grid(axis="y", color="#dddddd", linewidth=0.6)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
    axes[0].legend(frameon=False, loc="upper right")
    axes[-1].set_xlabel("Clock time")
    ticks = np.arange(14.5, 21.6, 0.5)
    axes[-1].set_xticks(ticks)
    axes[-1].set_xticklabels([f"{int(t):02d}:{int(round((t % 1) * 60)):02d}" for t in ticks], rotation=45)
    fig.suptitle(
        "Liuxian Avenue three purchased Gaode weeks: weekday 10-minute corridor speeds",
        x=0.01,
        y=0.995,
        ha="left",
        fontsize=12,
        fontweight="bold",
    )
    fig.text(
        0.01,
        0.015,
        "Notes: Points are 10-minute means; no smoothing. Dashed line = planned 17:30 opening. "
        "Dotted line = assumed 18:15 actual opening on 2026-04-24. "
        "This panel uses the 14 common segments observed in all three weeks.",
        fontsize=7.5,
        color="#444444",
    )
    fig.tight_layout(rect=(0, 0.075, 1, 0.94))
    save_figure(fig, output_path)
    plt.close(fig)


def plot_shock_week_daily(corridor: pd.DataFrame, output_path: Path) -> None:
    data = corridor[
        (corridor["date"] >= pd.Timestamp("2026-04-20"))
        & (corridor["date"] <= pd.Timestamp("2026-04-24"))
        & (corridor["minute_of_day"] >= 14 * 60 + 30)
        & (corridor["minute_of_day"] <= 21 * 60 + 30)
    ].copy()
    data["hour"] = data["minute_of_day"] / 60
    dates = sorted(data["date_str"].unique())
    fig, axes = plt.subplots(len(dates), 1, figsize=(9.8, 10.4), sharex=True, sharey=True)
    for ax, date_str in zip(axes, dates):
        add_operation_markers(ax)
        for chain, group in data[data["date_str"].eq(date_str)].groupby("chain_direction"):
            group = group.sort_values("minute_of_day")
            ax.plot(
                group["hour"],
                group["corridor_speed_kmh"],
                marker="o",
                markersize=2.7,
                linewidth=1.25,
                color=CHAIN_COLORS[chain],
                label=CHAIN_LABELS[chain],
            )
        label = f"{date_str} {data[data['date_str'].eq(date_str)]['dow_name'].iloc[0]}"
        if date_str == SHOCK_DATE.strftime("%Y-%m-%d"):
            label += "  SHOCK"
        ax.set_title(label, loc="left", fontweight="bold")
        ax.set_ylabel("km/h")
        ax.grid(axis="y", color="#dddddd", linewidth=0.55)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
    axes[0].legend(frameon=False, loc="upper right", ncol=2)
    ticks = np.arange(14.5, 21.6, 0.5)
    axes[-1].set_xticks(ticks)
    axes[-1].set_xticklabels([f"{int(t):02d}:{int(round((t % 1) * 60)):02d}" for t in ticks], rotation=45)
    axes[-1].set_xlabel("Clock time")
    fig.suptitle(
        "Shock week daily corridor speeds, policy direction vs opposite direction",
        x=0.01,
        y=0.995,
        ha="left",
        fontsize=12,
        fontweight="bold",
    )
    fig.text(
        0.01,
        0.012,
        "Notes: Uses all 16 Gaode segments in the two April post weeks. Points are raw 10-minute corridor speeds; no smoothing.",
        fontsize=7.5,
        color="#444444",
    )
    fig.tight_layout(rect=(0, 0.045, 1, 0.965))
    save_figure(fig, output_path)
    plt.close(fig)


def plot_asymmetry_timeseries(wide: pd.DataFrame, output_path: Path) -> None:
    data = wide[
        (wide["scope"] == "post_16_segments")
        & (wide["day_type"] == "Weekday")
        & (wide["date"] <= SHOCK_DATE)
        & (wide["minute_of_day"] >= 14 * 60 + 30)
        & (wide["minute_of_day"] <= 21 * 60 + 30)
    ].copy()
    data["hour"] = data["minute_of_day"] / 60
    normal = data[data["date_str"].isin(CONTROL_GROUPS["all_post_normal_weekdays"]["dates"])]
    normal_mean = (
        normal.groupby("minute_of_day", as_index=False)
        .agg(policy_over_opposite_speed=("policy_over_opposite_speed", "mean"))
    )
    normal_mean["hour"] = normal_mean["minute_of_day"] / 60
    prior_friday = data[data["date_str"].eq("2026-04-17")]
    shock = data[data["date"].eq(SHOCK_DATE)]

    fig, ax = plt.subplots(figsize=(9.8, 4.9))
    add_operation_markers(ax)
    for frame, color, label, marker, linewidth in [
        (normal_mean, "#777777", "Normal post weekdays mean", "o", 1.4),
        (prior_friday, "#54a24b", "Prior Friday 2026-04-17", "s", 1.35),
        (shock, "#c44e52", "Shock Friday 2026-04-24", "D", 1.55),
    ]:
        if frame.empty:
            continue
        ax.plot(
            frame["hour"],
            frame["policy_over_opposite_speed"],
            marker=marker,
            markersize=3.1,
            linewidth=linewidth,
            color=color,
            label=label,
        )
    ax.axhline(1, color="#222222", linewidth=0.8)
    ax.set_ylabel("Policy-direction speed / opposite-direction speed")
    ax.set_xlabel("Clock time")
    ticks = np.arange(14.5, 21.6, 0.5)
    ax.set_xticks(ticks)
    ax.set_xticklabels([f"{int(t):02d}:{int(round((t % 1) * 60)):02d}" for t in ticks], rotation=45)
    ax.set_title(
        "Directional asymmetry around the 2026-04-24 late-opening shock",
        loc="left",
        fontweight="bold",
    )
    ax.grid(axis="y", color="#dddddd", linewidth=0.6)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.legend(frameon=False, loc="upper left")
    fig.text(
        0.01,
        0.01,
        "Notes: Ratio above 1 means the policy direction is faster than the opposite direction. "
        "Yellow shading is the reported late-opening interval, 17:30-18:15.",
        fontsize=7.5,
        color="#444444",
    )
    fig.tight_layout(rect=(0, 0.055, 1, 1))
    save_figure(fig, output_path)
    plt.close(fig)


def plot_window_did(did: pd.DataFrame, output_path: Path) -> None:
    plot_windows = [
        "delayed_strict_1730_1810",
        "delayed_extended_1730_1820",
        "post_actual_1820_2000",
        "full_active_1730_2000",
    ]
    plot_data = did[
        did["control_group"].isin(["all_post_normal_weekdays", "same_week_mon_thu", "prior_friday"])
        & did["window"].isin(plot_windows)
    ].copy()
    plot_data["window_order"] = plot_data["window"].map({w: i for i, w in enumerate(plot_windows)})
    plot_data = plot_data.sort_values(["window_order", "control_group"])

    fig, axes = plt.subplots(1, 2, figsize=(11.5, 4.7), sharey=False)
    group_order = ["all_post_normal_weekdays", "same_week_mon_thu", "prior_friday"]
    offsets = {"all_post_normal_weekdays": -0.24, "same_week_mon_thu": 0.0, "prior_friday": 0.24}
    colors = {"all_post_normal_weekdays": "#4c78a8", "same_week_mon_thu": "#54a24b", "prior_friday": "#c44e52"}
    x = np.arange(len(plot_windows))
    for ax, col, title, ylabel in [
        (
            axes[0],
            "gap_abnormal_shock_minus_control_kmh",
            "A. Shock minus normal days, same window",
            "Abnormal policy-minus-opposite gap (km/h)",
        ),
        (
            axes[1],
            "did_gap_vs_1700_1730_kmh",
            "B. Also net of 17:00-17:30 reference",
            "DDD relative to reference window (km/h)",
        ),
    ]:
        for group in group_order:
            part = plot_data[plot_data["control_group"].eq(group)]
            ax.bar(
                x + offsets[group],
                part[col],
                width=0.22,
                color=colors[group],
                alpha=0.9,
                label=CONTROL_GROUPS[group]["label"],
            )
        ax.axhline(0, color="#333333", linewidth=0.8)
        ax.set_xticks(x)
        ax.set_xticklabels([WINDOW_LABELS[w].replace(", ", "\n") for w in plot_windows], fontsize=8)
        ax.set_title(title, loc="left", fontweight="bold")
        ax.set_ylabel(ylabel)
        ax.grid(axis="y", color="#dddddd", linewidth=0.6)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
    axes[0].legend(frameon=False, loc="lower left", fontsize=7.1)
    fig.suptitle(
        "Window-level evidence from the 2026-04-24 late-opening shock",
        x=0.01,
        y=0.995,
        ha="left",
        fontsize=12,
        fontweight="bold",
    )
    fig.text(
        0.01,
        0.012,
        "Negative delayed-window bars mean the policy direction is unusually slow on the shock day relative to the opposite direction. "
        "This is the sign expected if the missing 17:30 opening reduced policy-direction capacity.",
        fontsize=7.5,
        color="#444444",
    )
    fig.tight_layout(rect=(0, 0.065, 1, 0.93))
    save_figure(fig, output_path)
    plt.close(fig)


def plot_event_study(coefs: pd.DataFrame, output_path: Path) -> None:
    data = coefs.sort_values("event_minute").copy()
    finite = data[np.isfinite(data["std_error_speed_kmh"])]
    omitted = data[~np.isfinite(data["std_error_speed_kmh"])]

    fig, ax = plt.subplots(figsize=(10.2, 5.0))
    ax.axvspan(0, ASSUMED_ACTUAL_OPEN_MINUTE - PLANNED_OPEN_MINUTE, color="#f2c14e", alpha=0.18, linewidth=0)
    ax.axvspan(ASSUMED_ACTUAL_OPEN_MINUTE - PLANNED_OPEN_MINUTE, 150, color="#7cb7a8", alpha=0.12, linewidth=0)
    if not finite.empty:
        yerr = np.vstack(
            [
                finite["coef_speed_kmh"] - finite["ci95_low_speed_kmh"],
                finite["ci95_high_speed_kmh"] - finite["coef_speed_kmh"],
            ]
        )
        colors = finite["window"].map(
            {"pre_start": "#4c78a8", "late_not_open": "#c44e52", "after_actual_open": "#54a24b"}
        )
        ax.errorbar(
            finite["event_minute"],
            finite["coef_speed_kmh"],
            yerr=yerr,
            fmt="none",
            ecolor="#777777",
            elinewidth=0.85,
            capsize=2.2,
            alpha=0.65,
        )
        ax.scatter(
            finite["event_minute"],
            finite["coef_speed_kmh"],
            c=colors,
            s=24,
            zorder=3,
        )
        ax.plot(finite["event_minute"], finite["coef_speed_kmh"], color="#555555", linewidth=0.8, alpha=0.75)
    if not omitted.empty:
        ax.scatter(omitted["event_minute"], omitted["coef_speed_kmh"], marker="D", s=38, color="#333333", label="Omitted bin")
    ax.axhline(0, color="#333333", linewidth=0.8)
    ax.axvline(0, color="#222222", linestyle="--", linewidth=0.9)
    ax.axvline(ASSUMED_ACTUAL_OPEN_MINUTE - PLANNED_OPEN_MINUTE, color="#222222", linestyle=":", linewidth=0.9)
    ax.set_xlim(-185, 245)
    ticks = np.arange(-180, 241, 30)
    ax.set_xticks(ticks)
    ax.set_xlabel("Minutes relative to planned 17:30 opening")
    ax.set_ylabel("Shock-day policy-direction abnormal speed (km/h)")
    ax.set_title(
        "Segment-level shock event study: 2026-04-24 late opening",
        loc="left",
        fontweight="bold",
    )
    ax.grid(axis="y", color="#dddddd", linewidth=0.6)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    fig.text(
        0.01,
        0.012,
        "Specification: speed = segment FE + date FE + minute FE + Policy x event-bin + ShockDay x event-bin "
        "+ Policy x ShockDay + Policy x ShockDay x event-bin. Omitted bin is 17:20. "
        "CI is clustered by segment; with 16 clusters it is only diagnostic.",
        fontsize=7.2,
        color="#444444",
    )
    fig.tight_layout(rect=(0, 0.07, 1, 1))
    save_figure(fig, output_path)
    plt.close(fig)


def build_segment_abnormal_panel(panel: pd.DataFrame, inventory: pd.DataFrame) -> pd.DataFrame:
    sample = filter_segment_scope(panel, "post_16_segments")
    sample = sample[
        (sample["day_type"] == "Weekday")
        & (sample["date"] <= SHOCK_DATE)
        & (sample["minute_of_day"] >= 14 * 60 + 30)
        & (sample["minute_of_day"] <= 21 * 60 + 30)
    ].copy()
    normal_dates = CONTROL_GROUPS["all_post_normal_weekdays"]["dates"]
    normal = sample[sample["date_str"].isin(normal_dates)]
    normal_mean = (
        normal.groupby(["segment_id", "minute_of_day"], as_index=False)
        .agg(normal_speed_kmh=("speed_kmh", "mean"))
    )
    shock = sample[sample["date"].eq(SHOCK_DATE)].copy()
    out = shock.merge(normal_mean, on=["segment_id", "minute_of_day"], how="left")
    out["abnormal_speed_kmh"] = out["speed_kmh"] - out["normal_speed_kmh"]
    out = out.merge(
        inventory[
            [
                "segment_id",
                "路段名称（唯一）",
                "路段方向",
                "chain_direction",
                "position_score",
            ]
        ].drop_duplicates("segment_id"),
        on="segment_id",
        how="left",
        suffixes=("", "_inventory"),
    )
    out["chain_direction"] = out["chain_direction_inventory"].fillna(out["chain_direction"])
    out = out.drop(columns=["chain_direction_inventory"])
    return out.sort_values(["chain_direction", "position_score", "minute_of_day"])


def plot_segment_abnormal_heatmap(abnormal: pd.DataFrame, output_path: Path) -> None:
    fig, axes = plt.subplots(2, 1, figsize=(10.6, 7.2), sharex=True)
    im = None
    for ax, chain in zip(axes, ["eastbound_chain", "westbound_chain"]):
        part = abnormal[abnormal["chain_direction"].eq(chain)].copy()
        seg_order = (
            part[["segment_id", "路段名称（唯一）", "position_score"]]
            .drop_duplicates()
            .sort_values("position_score")
        )
        minutes = sorted(part["minute_of_day"].unique())
        matrix = (
            part.pivot_table(
                index="segment_id",
                columns="minute_of_day",
                values="abnormal_speed_kmh",
                aggfunc="first",
            )
            .reindex(index=seg_order["segment_id"], columns=minutes)
            .to_numpy()
        )
        im = ax.imshow(
            matrix,
            aspect="auto",
            cmap="RdBu_r",
            vmin=-25,
            vmax=25,
            interpolation="nearest",
        )
        ax.set_yticks(np.arange(len(seg_order)))
        ax.set_yticklabels(seg_order["路段名称（唯一）"], fontsize=7.6)
        ax.set_title(CHAIN_LABELS[chain], loc="left", fontweight="bold")
        ax.axvline(
            minutes.index(PLANNED_OPEN_MINUTE) - 0.5,
            color="#222222",
            linestyle="--",
            linewidth=0.8,
        )
        actual_grid = min(minutes, key=lambda minute: abs(minute - ASSUMED_ACTUAL_OPEN_MINUTE))
        ax.axvline(
            minutes.index(actual_grid) - 0.5,
            color="#222222",
            linestyle=":",
            linewidth=0.8,
        )
        ax.set_ylabel("Gaode segment")
    ticks = [i for i, minute in enumerate(minutes) if minute % 30 == 0]
    axes[-1].set_xticks(ticks)
    axes[-1].set_xticklabels(
        [f"{minutes[i] // 60:02d}:{minutes[i] % 60:02d}" for i in ticks],
        rotation=45,
        ha="right",
    )
    axes[-1].set_xlabel("Clock time on 2026-04-24")
    if im is not None:
        cbar = fig.colorbar(im, ax=axes, orientation="vertical", fraction=0.025, pad=0.02)
        cbar.set_label("Shock day speed minus normal post weekdays (km/h)")
    fig.suptitle(
        "Segment-level abnormal speed on the 2026-04-24 shock day",
        x=0.01,
        y=0.995,
        ha="left",
        fontsize=12,
        fontweight="bold",
    )
    fig.text(
        0.01,
        0.012,
        "Notes: Each cell is 2026-04-24 segment speed minus the mean speed of normal post-policy weekdays "
        "at the same 10-minute clock time. Dashed line = 17:30; dotted line is the closest grid point to 18:15.",
        fontsize=7.3,
        color="#444444",
    )
    fig.tight_layout(rect=(0, 0.055, 0.96, 0.95))
    save_figure(fig, output_path)
    plt.close(fig)


def build_two_friday_segment_comparison(panel: pd.DataFrame, inventory: pd.DataFrame) -> pd.DataFrame:
    sample = filter_segment_scope(panel, "post_16_segments")
    sample = sample[sample["date"].isin([POST_FRIDAY_BASELINE, SHOCK_DATE])].copy()
    rows = []
    for window in ["ref_1700_1730", "delayed_strict_1730_1810", "delayed_extended_1730_1820", "post_actual_1820_2000"]:
        lo, hi = WINDOWS[window]
        sub = sample[(sample["minute_of_day"] >= lo) & (sample["minute_of_day"] < hi)].copy()
        grouped = (
            sub.groupby(
                [
                    "segment_id",
                    "路段编号",
                    "路段名称（唯一）",
                    "路段方向",
                    "chain_direction",
                    "chain_label",
                    "date_str",
                ],
                as_index=False,
            )
            .agg(
                mean_speed_kmh=("speed_kmh", "mean"),
                mean_delay_index=("delay_index", "mean"),
                n_10min=("speed_kmh", "size"),
            )
        )
        wide = grouped.pivot_table(
            index=[
                "segment_id",
                "路段编号",
                "路段名称（唯一）",
                "路段方向",
                "chain_direction",
                "chain_label",
            ],
            columns="date_str",
            values=["mean_speed_kmh", "mean_delay_index", "n_10min"],
            aggfunc="first",
        )
        wide.columns = [f"{metric}_{date}" for metric, date in wide.columns]
        wide = wide.reset_index()
        wide["window"] = window
        wide["window_label"] = WINDOW_LABELS[window]
        wide["speed_change_0424_minus_0417_kmh"] = (
            wide["mean_speed_kmh_2026-04-24"] - wide["mean_speed_kmh_2026-04-17"]
        )
        wide["speed_ratio_0424_over_0417"] = (
            wide["mean_speed_kmh_2026-04-24"] / wide["mean_speed_kmh_2026-04-17"]
        )
        wide["delay_index_change_0424_minus_0417"] = (
            wide["mean_delay_index_2026-04-24"] - wide["mean_delay_index_2026-04-17"]
        )
        rows.append(wide)
    out = pd.concat(rows, ignore_index=True)
    out = out.merge(
        inventory[["segment_id", "position_score", "center_lon", "center_lat"]].drop_duplicates("segment_id"),
        on="segment_id",
        how="left",
    )
    return out.sort_values(["window", "chain_direction", "position_score", "segment_id"])


def build_two_friday_window_summary(comparison: pd.DataFrame) -> pd.DataFrame:
    return (
        comparison.groupby(["window", "window_label", "chain_direction", "chain_label"], as_index=False)
        .agg(
            mean_speed_0417_kmh=("mean_speed_kmh_2026-04-17", "mean"),
            mean_speed_0424_kmh=("mean_speed_kmh_2026-04-24", "mean"),
            mean_speed_change_0424_minus_0417_kmh=("speed_change_0424_minus_0417_kmh", "mean"),
            median_speed_change_0424_minus_0417_kmh=("speed_change_0424_minus_0417_kmh", "median"),
            mean_speed_ratio_0424_over_0417=("speed_ratio_0424_over_0417", "mean"),
            n_segments=("segment_id", "nunique"),
        )
        .sort_values(["window", "chain_direction"])
    )


def make_dummy_frame(series: pd.Series, prefix: str, *, drop_first: bool = True) -> pd.DataFrame:
    return pd.get_dummies(series.astype(str), prefix=prefix, drop_first=drop_first, dtype=float)


def fit_clustered_ols(
    df: pd.DataFrame,
    parts: list[pd.Series | pd.DataFrame],
    *,
    outcome_col: str,
    coef_col: str,
    model: str,
    description: str,
) -> dict[str, float | int | str]:
    x_df = pd.concat(parts, axis=1)
    # Keep the target coefficient even if a defensive zero-variance cleanup is added later.
    x = x_df.to_numpy(dtype=float)
    y = df[outcome_col].to_numpy(dtype=float)
    beta, *_ = np.linalg.lstsq(x, y, rcond=None)
    fitted = x @ beta
    residual = y - fitted
    vcov = cluster_covariance(x, residual, df["segment_id"].astype(str).to_numpy())
    se = np.sqrt(np.maximum(np.diag(vcov), 0))
    coef_lookup = dict(zip(x_df.columns, beta))
    se_lookup = dict(zip(x_df.columns, se))
    coef = float(coef_lookup[coef_col])
    std_err = float(se_lookup[coef_col])
    return {
        "model": model,
        "description": description,
        "outcome": outcome_col,
        "coef_col": coef_col,
        "coef_speed_kmh": coef,
        "std_error_speed_kmh": std_err,
        "ci95_low_speed_kmh": coef - 1.96 * std_err,
        "ci95_high_speed_kmh": coef + 1.96 * std_err,
        "n_obs": int(len(df)),
        "n_segments": int(df["segment_id"].nunique()),
        "n_minutes": int(df["minute_of_day"].nunique()),
        "r_squared": float(1 - np.sum(residual**2) / np.sum((y - y.mean()) ** 2)),
    }


def build_two_friday_controlled_specs(panel: pd.DataFrame) -> pd.DataFrame:
    sample = filter_segment_scope(panel, "post_16_segments")
    sample = sample[
        (sample["date"].isin([POST_FRIDAY_BASELINE, SHOCK_DATE]))
        & (sample["minute_of_day"] >= 17 * 60)
        & (sample["minute_of_day"] < 18 * 60 + 10)
    ].copy()
    sample["shock_day"] = sample["date"].eq(SHOCK_DATE).astype(float)
    sample["policy"] = sample["policy_direction"].astype(float)
    sample["delayed_window"] = sample["minute_of_day"].ge(PLANNED_OPEN_MINUTE).astype(float)
    sample["policy_x_shock"] = sample["policy"] * sample["shock_day"]
    sample["policy_x_delayed"] = sample["policy"] * sample["delayed_window"]
    sample["shock_x_delayed"] = sample["shock_day"] * sample["delayed_window"]
    sample["shock_x_policy_x_delayed"] = (
        sample["shock_day"] * sample["policy"] * sample["delayed_window"]
    )
    sample["date_id"] = sample["date"].dt.strftime("%Y%m%d")
    sample["minute_id"] = sample["minute_of_day"].astype(str)
    sample["trend_10min"] = (sample["minute_of_day"] - 17 * 60) / 10.0

    const = pd.Series(1.0, index=sample.index, name="const")
    triple = sample["shock_x_policy_x_delayed"].rename("shock_x_policy_x_delayed")

    specs: list[dict[str, float | int | str]] = []
    specs.append(
        fit_clustered_ols(
            sample,
            [
                const,
                sample["shock_day"].rename("shock_day"),
                sample["policy"].rename("policy"),
                sample["delayed_window"].rename("delayed_window"),
                sample["policy_x_shock"].rename("policy_x_shock"),
                sample["policy_x_delayed"].rename("policy_x_delayed"),
                sample["shock_x_delayed"].rename("shock_x_delayed"),
                triple,
            ],
            outcome_col="speed_kmh",
            coef_col="shock_x_policy_x_delayed",
            model="A_raw_ddd",
            description="Two Fridays, 17:00-18:10; raw Shock x PolicyDirection x DelayedWindow DDD.",
        )
    )
    specs.append(
        fit_clustered_ols(
            sample,
            [
                const,
                make_dummy_frame(sample["segment_id"], "segment"),
                make_dummy_frame(sample["date_id"], "date"),
                sample["delayed_window"].rename("delayed_window"),
                sample["policy_x_shock"].rename("policy_x_shock"),
                sample["policy_x_delayed"].rename("policy_x_delayed"),
                sample["shock_x_delayed"].rename("shock_x_delayed"),
                triple,
            ],
            outcome_col="speed_kmh",
            coef_col="shock_x_policy_x_delayed",
            model="B_segment_date_window_fe",
            description="Adds segment FE, date FE, and delayed-window FE.",
        )
    )
    specs.append(
        fit_clustered_ols(
            sample,
            [
                const,
                make_dummy_frame(sample["segment_id"], "segment"),
                make_dummy_frame(sample["date_id"], "date"),
                make_dummy_frame(sample["minute_id"], "minute"),
                sample["policy_x_shock"].rename("policy_x_shock"),
                sample["policy_x_delayed"].rename("policy_x_delayed"),
                sample["shock_x_delayed"].rename("shock_x_delayed"),
                triple,
            ],
            outcome_col="speed_kmh",
            coef_col="shock_x_policy_x_delayed",
            model="C_segment_date_10min_fe",
            description="Adds segment FE, date FE, and exact 10-minute clock-time FE.",
        )
    )

    minute_dummies = make_dummy_frame(sample["minute_id"], "minute")
    policy_minute = minute_dummies.mul(sample["policy"], axis=0)
    policy_minute.columns = [f"policy_x_{col}" for col in policy_minute.columns]
    shock_minute = minute_dummies.mul(sample["shock_day"], axis=0)
    shock_minute.columns = [f"shock_x_{col}" for col in shock_minute.columns]
    specs.append(
        fit_clustered_ols(
            sample,
            [
                const,
                make_dummy_frame(sample["segment_id"], "segment"),
                make_dummy_frame(sample["date_id"], "date"),
                minute_dummies,
                policy_minute,
                shock_minute,
                sample["policy_x_shock"].rename("policy_x_shock"),
                triple,
            ],
            outcome_col="speed_kmh",
            coef_col="shock_x_policy_x_delayed",
            model="D_add_policy_and_shock_minute_controls",
            description=(
                "Adds segment FE, date FE, 10-minute FE, PolicyDirection x minute FE, "
                "and ShockDay x minute FE."
            ),
        )
    )

    segment_dummies_all = make_dummy_frame(sample["segment_id"], "segment", drop_first=False)
    segment_trends = segment_dummies_all.mul(sample["trend_10min"], axis=0)
    segment_trends.columns = [f"{col}_trend" for col in segment_trends.columns]
    specs.append(
        fit_clustered_ols(
            sample,
            [
                const,
                make_dummy_frame(sample["segment_id"], "segment"),
                make_dummy_frame(sample["date_id"], "date"),
                minute_dummies,
                policy_minute,
                shock_minute,
                sample["policy_x_shock"].rename("policy_x_shock"),
                segment_trends,
                triple,
            ],
            outcome_col="speed_kmh",
            coef_col="shock_x_policy_x_delayed",
            model="E_add_segment_linear_time_trends",
            description=(
                "Adds the rich minute controls from D plus segment-specific linear time trends "
                "over 17:00-18:10."
            ),
        )
    )
    out = pd.DataFrame(specs)
    out["shock_date"] = SHOCK_DATE.strftime("%Y-%m-%d")
    out["control_friday"] = POST_FRIDAY_BASELINE.strftime("%Y-%m-%d")
    out["reference_window"] = "17:00-17:30"
    out["delayed_window"] = "17:30-18:10"
    out["cluster_note"] = "Segment-clustered SEs; only 16 clusters, so intervals are diagnostic."
    return out


def plot_two_friday_controlled_specs(specs: pd.DataFrame, output_path: Path) -> None:
    data = specs.copy()
    data["model_short"] = ["A", "B", "C", "D", "E"][: len(data)]
    fig, ax = plt.subplots(figsize=(9.4, 4.9))
    y = np.arange(len(data))
    xerr = np.vstack(
        [
            data["coef_speed_kmh"] - data["ci95_low_speed_kmh"],
            data["ci95_high_speed_kmh"] - data["coef_speed_kmh"],
        ]
    )
    ax.errorbar(
        data["coef_speed_kmh"],
        y,
        xerr=xerr,
        fmt="o",
        color="#4c78a8",
        ecolor="#777777",
        elinewidth=1.0,
        capsize=3.0,
        markersize=5.0,
    )
    ax.axvline(0, color="#333333", linewidth=0.8)
    ax.set_yticks(y)
    ax.set_yticklabels(
        [
            "A Raw DDD",
            "B + segment/date/window FE",
            "C + exact 10-min FE",
            "D + direction/shock minute controls",
            "E + segment time trends",
        ],
        fontsize=8.2,
    )
    ax.invert_yaxis()
    ax.set_xlabel("Shock Friday x policy direction x delayed-window effect (km/h)")
    ax.set_title(
        "Controlled two-Friday DDD estimates for 17:30-18:10",
        loc="left",
        fontweight="bold",
    )
    ax.grid(axis="x", color="#dddddd", linewidth=0.6)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    for row in data.itertuples(index=False):
        ax.text(
            row.coef_speed_kmh,
            y[list(data["model"]).index(row.model)] - 0.25,
            f"{row.coef_speed_kmh:.1f}",
            ha="center",
            va="bottom",
            fontsize=7.4,
            color="#333333",
        )
    fig.text(
        0.01,
        0.012,
        "Notes: Outcome is segment speed. Sample is 2026-04-17 and 2026-04-24, 17:00-18:10. "
        "Coefficient compares policy vs opposite direction, shock vs normal Friday, delayed window vs 17:00-17:30 reference. "
        "95% intervals use segment-clustered SEs with only 16 clusters.",
        fontsize=7.2,
        color="#444444",
    )
    fig.tight_layout(rect=(0, 0.08, 1, 1))
    save_figure(fig, output_path)
    plt.close(fig)


def plot_two_friday_corridor_timeseries(corridor: pd.DataFrame, output_path: Path) -> None:
    data = corridor[
        (corridor["date"].isin([POST_FRIDAY_BASELINE, SHOCK_DATE]))
        & (corridor["minute_of_day"] >= 16 * 60 + 30)
        & (corridor["minute_of_day"] <= 20 * 60)
    ].copy()
    data["hour"] = data["minute_of_day"] / 60
    fig, axes = plt.subplots(2, 1, figsize=(9.6, 5.9), sharex=True, sharey=True)
    styles = {
        "2026-04-17": {"color": "#4c78a8", "label": "Normal post Friday: 2026-04-17", "marker": "o"},
        "2026-04-24": {"color": "#c44e52", "label": "Late-opening Friday: 2026-04-24", "marker": "D"},
    }
    for ax, chain in zip(axes, ["eastbound_chain", "westbound_chain"]):
        add_operation_markers(ax)
        for date_str, group in data[data["chain_direction"].eq(chain)].groupby("date_str"):
            group = group.sort_values("minute_of_day")
            style = styles[date_str]
            ax.plot(
                group["hour"],
                group["corridor_speed_kmh"],
                color=style["color"],
                marker=style["marker"],
                markersize=2.8,
                linewidth=1.35,
                label=style["label"],
            )
        ax.set_title(CHAIN_LABELS[chain], loc="left", fontweight="bold")
        ax.set_ylabel("Corridor speed (km/h)")
        ax.grid(axis="y", color="#dddddd", linewidth=0.6)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
    axes[0].legend(frameon=False, loc="upper right")
    ticks = np.arange(16.5, 20.1, 0.5)
    axes[-1].set_xticks(ticks)
    axes[-1].set_xticklabels([f"{int(t):02d}:{int(round((t % 1) * 60)):02d}" for t in ticks], rotation=45)
    axes[-1].set_xlabel("Clock time")
    fig.suptitle(
        "Two post Fridays: corridor speeds around the delayed-opening window",
        x=0.01,
        y=0.995,
        ha="left",
        fontsize=12,
        fontweight="bold",
    )
    fig.text(
        0.01,
        0.012,
        "Notes: Uses all 16 Gaode segments in the two April post-policy weeks. "
        "Yellow shading marks 17:30-18:15; dotted line marks the assumed 18:15 full-opening time.",
        fontsize=7.3,
        color="#444444",
    )
    fig.tight_layout(rect=(0, 0.06, 1, 0.94))
    save_figure(fig, output_path)
    plt.close(fig)


def plot_two_friday_segment_change(comparison: pd.DataFrame, output_path: Path) -> None:
    plot = comparison[comparison["window"].eq("delayed_strict_1730_1810")].copy()
    fig, axes = plt.subplots(1, 2, figsize=(11.2, 5.3), sharex=True)
    for ax, chain in zip(axes, ["eastbound_chain", "westbound_chain"]):
        part = plot[plot["chain_direction"].eq(chain)].sort_values("position_score")
        colors = np.where(part["speed_change_0424_minus_0417_kmh"] >= 0, "#4c78a8", "#c44e52")
        y = np.arange(len(part))
        ax.barh(y, part["speed_change_0424_minus_0417_kmh"], color=colors, alpha=0.9)
        ax.axvline(0, color="#333333", linewidth=0.8)
        ax.set_yticks(y)
        ax.set_yticklabels(part["路段名称（唯一）"], fontsize=8)
        ax.set_title(CHAIN_LABELS[chain], loc="left", fontweight="bold")
        ax.set_xlabel("2026-04-24 minus 2026-04-17 speed (km/h)")
        ax.grid(axis="x", color="#dddddd", linewidth=0.6)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
    fig.suptitle(
        "Segment-level change in the delayed-opening window, 17:30-18:10",
        x=0.01,
        y=0.995,
        ha="left",
        fontsize=12,
        fontweight="bold",
    )
    fig.text(
        0.01,
        0.012,
        "Notes: Positive values mean the segment was faster on the late-opening Friday than on the prior normal post Friday.",
        fontsize=7.4,
        color="#444444",
    )
    fig.tight_layout(rect=(0, 0.055, 1, 0.92))
    save_figure(fig, output_path)
    plt.close(fig)


def build_three_week_summary(window_means: pd.DataFrame) -> pd.DataFrame:
    data = window_means[
        (window_means["scope"] == "common_all_weeks") & (window_means["day_type"] == "Weekday")
    ].copy()
    return (
        data.groupby(["purchase_week", "window", "window_label"], as_index=False)
        .agg(
            policy_speed_kmh=("policy_speed_kmh", "mean"),
            opposite_speed_kmh=("opposite_speed_kmh", "mean"),
            policy_minus_opposite_kmh=("policy_minus_opposite_kmh", "mean"),
            policy_over_opposite_speed=("policy_over_opposite_speed", "mean"),
            n_dates=("date_str", "nunique"),
        )
        .sort_values(["purchase_week", "window"])
    )


def main() -> None:
    args = parse_args()
    configure_plot()
    dirs = ensure_output_dirs(args.output_dir)

    panel_raw, file_audit = load_panel(args.input_dir)
    inventory = build_segment_inventory(panel_raw)
    panel = attach_chain_direction(panel_raw, inventory)

    corridor_common = build_corridor(panel, "common_all_weeks")
    corridor_post = build_corridor(panel, "post_16_segments")
    corridor = pd.concat([corridor_common, corridor_post], ignore_index=True)
    wide = pd.concat(
        [
            wide_directional_corridor(corridor_common),
            wide_directional_corridor(corridor_post),
        ],
        ignore_index=True,
    )
    window_means = build_window_means(wide)
    three_week_summary = build_three_week_summary(window_means)
    shock_did = build_shock_did(window_means)
    event_coefs, event_diagnostics = fit_shock_event_study(panel)
    segment_abnormal = build_segment_abnormal_panel(panel, inventory)
    two_friday_segments = build_two_friday_segment_comparison(panel, inventory)
    two_friday_summary = build_two_friday_window_summary(two_friday_segments)
    two_friday_controlled = build_two_friday_controlled_specs(panel)

    file_audit.to_csv(dirs["metrics"] / "gaode_file_audit.csv", index=False, encoding="utf-8-sig")
    inventory.to_csv(dirs["metrics"] / "gaode_segment_inventory_three_weeks.csv", index=False, encoding="utf-8-sig")
    corridor.to_csv(dirs["data"] / "liuxian_corridor_10min_panel.csv", index=False, encoding="utf-8-sig")
    wide.to_csv(dirs["data"] / "liuxian_directional_asymmetry_10min_panel.csv", index=False, encoding="utf-8-sig")
    window_means.to_csv(dirs["data"] / "liuxian_window_means_by_date.csv", index=False, encoding="utf-8-sig")
    three_week_summary.to_csv(dirs["metrics"] / "three_week_window_direction_summary.csv", index=False, encoding="utf-8-sig")
    shock_did.to_csv(dirs["metrics"] / "late_opening_shock_window_did.csv", index=False, encoding="utf-8-sig")
    event_coefs.to_csv(dirs["metrics"] / "late_opening_shock_event_study_coefficients.csv", index=False, encoding="utf-8-sig")
    segment_abnormal.to_csv(dirs["data"] / "liuxian_shock_segment_abnormal_panel.csv", index=False, encoding="utf-8-sig")
    two_friday_segments.to_csv(
        dirs["data"] / "liuxian_two_post_fridays_segment_window_comparison.csv",
        index=False,
        encoding="utf-8-sig",
    )
    two_friday_summary.to_csv(
        dirs["metrics"] / "liuxian_two_post_fridays_window_summary.csv",
        index=False,
        encoding="utf-8-sig",
    )
    two_friday_controlled.to_csv(
        dirs["metrics"] / "liuxian_two_post_fridays_controlled_ddd_specs.csv",
        index=False,
        encoding="utf-8-sig",
    )
    with (dirs["metrics"] / "late_opening_shock_event_study_spec.json").open("w", encoding="utf-8") as fh:
        json.dump(event_diagnostics, fh, ensure_ascii=False, indent=2)
    with (dirs["metrics"] / "late_opening_shock_metadata.json").open("w", encoding="utf-8") as fh:
        json.dump(
            {
                "input_dir": str(args.input_dir),
                "output_dir": str(args.output_dir),
                "policy_date": POLICY_DATE.strftime("%Y-%m-%d"),
                "shock_date": SHOCK_DATE.strftime("%Y-%m-%d"),
                "planned_open_time": "17:30",
                "assumed_actual_open_time": "18:15",
                "policy_chain": POLICY_CHAIN,
                "chain_labels": CHAIN_LABELS,
                "window_labels": WINDOW_LABELS,
                "control_groups": CONTROL_GROUPS,
                "scope_common_all_weeks": "14 common Gaode segments observed in all three purchased weeks",
                "scope_post_16_segments": "16 Gaode segments observed in the two April post-policy weeks",
            },
            fh,
            ensure_ascii=False,
            indent=2,
        )

    plot_three_week_diurnal(
        corridor_common,
        dirs["figures"] / "liuxian_three_week_weekday_diurnal_common14.png",
    )
    plot_shock_week_daily(
        corridor_post,
        dirs["figures"] / "liuxian_shock_week_daily_timeseries_post16.png",
    )
    plot_asymmetry_timeseries(
        wide,
        dirs["figures"] / "liuxian_shock_day_asymmetry_timeseries_post16.png",
    )
    plot_window_did(
        shock_did,
        dirs["figures"] / "liuxian_late_opening_window_did.png",
    )
    plot_event_study(
        event_coefs,
        dirs["figures"] / "liuxian_late_opening_event_study_did.png",
    )
    plot_segment_abnormal_heatmap(
        segment_abnormal,
        dirs["figures"] / "liuxian_shock_segment_abnormal_heatmap.png",
    )
    plot_two_friday_corridor_timeseries(
        corridor_post,
        dirs["figures"] / "liuxian_two_post_fridays_corridor_timeseries.png",
    )
    plot_two_friday_segment_change(
        two_friday_segments,
        dirs["figures"] / "liuxian_two_post_fridays_segment_change_1730_1810.png",
    )
    plot_two_friday_controlled_specs(
        two_friday_controlled,
        dirs["figures"] / "liuxian_two_post_fridays_controlled_ddd_specs.png",
    )

    print(f"Wrote Liuxian late-opening shock analysis to {args.output_dir}")
    print(file_audit.to_string(index=False))
    focus = shock_did[
        shock_did["control_group"].eq("all_post_normal_weekdays")
        & shock_did["window"].isin(["delayed_strict_1730_1810", "post_actual_1820_2000"])
    ]
    print(focus[["window", "gap_abnormal_shock_minus_control_kmh", "did_gap_vs_1700_1730_kmh"]].to_string(index=False))


if __name__ == "__main__":
    main()
