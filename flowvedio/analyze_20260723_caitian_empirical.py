#!/usr/bin/env python3
"""Build QA panels and exploratory RDiT estimates for 2026-07-23 Caitian Road."""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import statsmodels.api as sm


ROOT = Path(__file__).resolve().parent
SOURCE = ROOT / "batch_outputs" / "20260723_caitian_empirical_v1"
OUT = SOURCE / "analysis_v1"
TABLES = OUT / "tables"
FIGURES = OUT / "figures"
DOCS = OUT / "docs"

DIRECTIONS = ["south_to_north", "north_to_south"]
DIR_LABELS = {"south_to_north": "South to north", "north_to_south": "North to south"}
COLORS = {"south_to_north": "#d97706", "north_to_south": "#2563eb"}
CUTOFFS = {
    "inbound": pd.Timestamp("2026-07-23 16:26:17"),
    "outbound": pd.Timestamp("2026-07-23 16:26:34"),
}
CAMERA_ADJUSTMENT = pd.Timestamp("2026-07-23 17:08:42")
DEPLOYMENT_STARTS = {
    "inbound": pd.Timestamp("2026-07-23 16:07:17"),
    "outbound": pd.Timestamp("2026-07-23 16:11:09"),
}
BIN_MINUTES = [1, 2, 5]
BANDWIDTHS = [20, 30, 45, 60]
MIN_MATCHES = {1: 3, 2: 5, 5: 10}


def parse_run_plan() -> pd.DataFrame:
    plan = pd.read_csv(SOURCE / "run_plan.csv")
    plan["segment_start_local"] = pd.to_datetime(plan["segment_start_local"])
    plan["segment_start_s"] = plan["segment_start_s"].astype(float)
    plan["segment_end_s"] = plan["segment_end_s"].astype(float)
    plan["measurement_dir"] = plan["measurement_dir"].map(Path)
    return plan


def absolute_time(values: pd.Series, row: pd.Series) -> pd.Series:
    offset = values.astype(float) - float(row["segment_start_s"])
    return row["segment_start_local"] + pd.to_timedelta(offset, unit="s")


def load_measurements(plan: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    matches: list[pd.DataFrame] = []
    crossings: list[pd.DataFrame] = []
    density: list[pd.DataFrame] = []
    qa_rows: list[dict[str, object]] = []
    for _, row in plan.iterrows():
        directory = row["measurement_dir"]
        summary = json.loads((directory / "measurement_summary.json").read_text())

        match = pd.read_csv(directory / "speed_gate_matches.csv")
        match["tracking_direction"] = match["direction"]
        if row["camera_view"] == "inbound":
            match["direction"] = match["direction"].map(
                {"south_to_north": "north_to_south", "north_to_south": "south_to_north"}
            )
        match["recorded_at_local"] = absolute_time(match["second_timestamp_s"], row)
        match["source_file"] = row["source_file"]
        match["camera_view"] = row["camera_view"]
        match["state"] = row["state"]
        match["geometry_id"] = row["geometry_id"]
        matches.append(match)

        crossing = pd.read_csv(directory / "gate_crossings.csv")
        crossing["tracking_direction"] = crossing["direction"]
        expected = np.where(crossing["tracking_direction"].eq("south_to_north"), "far", "near")
        crossing["is_entry_gate"] = crossing["gate"].eq(expected)
        if row["camera_view"] == "inbound":
            crossing["direction"] = crossing["direction"].map(
                {"south_to_north": "north_to_south", "north_to_south": "south_to_north"}
            )
        crossing["recorded_at_local"] = absolute_time(crossing["timestamp_s"], row)
        crossing["source_file"] = row["source_file"]
        crossing["camera_view"] = row["camera_view"]
        crossing["state"] = row["state"]
        crossing["geometry_id"] = row["geometry_id"]
        crossings.append(crossing)

        den = pd.read_csv(directory / "density_by_frame.csv")
        den["tracking_direction"] = den["direction"]
        if row["camera_view"] == "inbound":
            den["direction"] = den["direction"].map(
                {"south_to_north": "north_to_south", "north_to_south": "south_to_north"}
            )
        lane_map = (
            {"south_to_north": 3, "north_to_south": 4}
            if row["state"] == "normal"
            else {"south_to_north": 4, "north_to_south": 3}
        )
        den["lanes"] = den["direction"].map(lane_map)
        den["vehicles_per_lane_in_roi"] = den["visible_vehicle_count"] / den["lanes"]
        den["recorded_at_local"] = absolute_time(den["timestamp_s"], row)
        den["source_file"] = row["source_file"]
        den["camera_view"] = row["camera_view"]
        den["state"] = row["state"]
        den["geometry_id"] = row["geometry_id"]
        density.append(den)

        qa_rows.append(
            {
                "source_file": row["source_file"],
                "camera_view": row["camera_view"],
                "state": row["state"],
                "geometry_id": row["geometry_id"],
                "duration_s": summary["duration_s"],
                "fps": summary["fps"],
                "stride": summary["stride"],
                "processed_frames": summary["processed_frames"],
                "detection_rows": summary["detection_rows"],
                "gate_crossings": summary["gate_crossings"],
                "matched_tracks": summary["matched_speed_tracks"],
                "matches_per_min": summary["matched_speed_tracks"] / summary["duration_s"] * 60,
                "crossings_per_min": summary["gate_crossings"] / summary["duration_s"] * 60,
                "two_gate_match_ratio": 2 * summary["matched_speed_tracks"] / summary["gate_crossings"],
                "direction_mapping": (
                    "tracking labels swapped to physical direction"
                    if row["camera_view"] == "inbound"
                    else "tracking labels equal physical direction"
                ),
            }
        )
    return (
        pd.concat(matches, ignore_index=True),
        pd.concat(crossings, ignore_index=True),
        pd.concat(density, ignore_index=True),
        pd.DataFrame(qa_rows),
    )


def load_transition_measurements() -> tuple[pd.DataFrame, pd.DataFrame]:
    plan_path = SOURCE / "transition_run_plan.csv"
    if not plan_path.exists():
        return pd.DataFrame(), pd.DataFrame()
    plan = pd.read_csv(plan_path)
    plan["segment_start_local"] = pd.to_datetime(plan["segment_start_local"])
    matches: list[pd.DataFrame] = []
    crossings: list[pd.DataFrame] = []
    for _, row in plan.iterrows():
        directory = Path(row["measurement_dir"])
        match = pd.read_csv(directory / "speed_gate_matches.csv")
        match["recorded_at_local"] = row["segment_start_local"] + pd.to_timedelta(
            match["second_timestamp_s"] - float(row["segment_start_s"]), unit="s"
        )
        match["camera_view"] = row["camera_view"]
        match["tracking_direction"] = match["direction"]
        if row["camera_view"] == "inbound":
            match["direction"] = match["direction"].map(
                {"south_to_north": "north_to_south", "north_to_south": "south_to_north"}
            )
        matches.append(match)

        crossing = pd.read_csv(directory / "gate_crossings.csv")
        crossing["recorded_at_local"] = row["segment_start_local"] + pd.to_timedelta(
            crossing["timestamp_s"] - float(row["segment_start_s"]), unit="s"
        )
        crossing["camera_view"] = row["camera_view"]
        crossing["tracking_direction"] = crossing["direction"]
        expected = np.where(crossing["tracking_direction"].eq("south_to_north"), "far", "near")
        crossing["is_entry_gate"] = crossing["gate"].eq(expected)
        if row["camera_view"] == "inbound":
            crossing["direction"] = crossing["direction"].map(
                {"south_to_north": "north_to_south", "north_to_south": "south_to_north"}
            )
        crossings.append(crossing)
    return pd.concat(matches, ignore_index=True), pd.concat(crossings, ignore_index=True)


def make_transition_wide(matches: pd.DataFrame, crossings: pd.DataFrame, minutes: int = 5) -> pd.DataFrame:
    entry = add_bin(crossings[crossings["is_entry_gate"]], minutes)
    flow = (
        entry.groupby(["camera_view", "bin_start", "direction"])
        .size()
        .rename("flow_veh_per_min")
        .div(minutes)
        .reset_index()
    )
    match = add_bin(matches, minutes)
    travel = (
        match.groupby(["camera_view", "bin_start", "direction"])["travel_time_s"]
        .agg(match_count="size", median_travel_time_s="median")
        .reset_index()
    )
    panel = flow.merge(travel, on=["camera_view", "bin_start", "direction"], how="outer")
    panel["bin_center"] = panel["bin_start"] + pd.to_timedelta(minutes / 2, unit="min")
    panel["cutoff_time"] = panel["camera_view"].map(CUTOFFS)
    panel["running_min"] = (panel["bin_center"] - panel["cutoff_time"]).dt.total_seconds() / 60
    wide = panel.pivot_table(
        index=["camera_view", "bin_start", "bin_center", "running_min"],
        columns="direction",
        values=["flow_veh_per_min", "median_travel_time_s", "match_count"],
        aggfunc="first",
    ).reset_index()
    wide.columns = ["_".join([str(v) for v in col if str(v)]) if isinstance(col, tuple) else col for col in wide.columns]
    for outcome in ["flow_veh_per_min", "median_travel_time_s"]:
        wide[f"delta_{outcome}"] = wide[f"{outcome}_south_to_north"] - wide[f"{outcome}_north_to_south"]
    wide["transition"] = True
    return wide


def add_bin(frame: pd.DataFrame, minutes: int) -> pd.DataFrame:
    out = frame.copy()
    out["bin_start"] = out["recorded_at_local"].dt.floor(f"{minutes}min")
    return out


def make_panel(
    matches: pd.DataFrame,
    crossings: pd.DataFrame,
    density: pd.DataFrame,
    minutes: int,
) -> pd.DataFrame:
    keys = ["camera_view", "bin_start", "direction"]
    entry = add_bin(crossings[crossings["is_entry_gate"]], minutes)
    flow = entry.groupby(keys, as_index=False).size().rename(columns={"size": "entry_count"})
    flow["flow_veh_per_min"] = flow["entry_count"] / minutes

    match = add_bin(matches, minutes)
    travel = (
        match.groupby(keys)["travel_time_s"]
        .agg(match_count="size", mean_travel_time_s="mean", median_travel_time_s="median", p90_travel_time_s=lambda x: x.quantile(0.9))
        .reset_index()
    )
    travel["travel_time_valid"] = travel["match_count"].ge(MIN_MATCHES[minutes])

    den = add_bin(density, minutes)
    density_bin = (
        den.groupby(keys)
        .agg(
            density_frames=("visible_vehicle_count", "size"),
            mean_visible_vehicles=("visible_vehicle_count", "mean"),
            median_visible_vehicles=("visible_vehicle_count", "median"),
            mean_vehicles_per_lane=("vehicles_per_lane_in_roi", "mean"),
            lanes=("lanes", "median"),
        )
        .reset_index()
    )

    coverage = (
        den.groupby(["camera_view", "bin_start", "source_file", "geometry_id"])
        .agg(observed_timestamps=("timestamp_s", "nunique"), first_timestamp=("timestamp_s", "min"), last_timestamp=("timestamp_s", "max"))
        .reset_index()
    )
    coverage["coverage_s"] = (coverage["last_timestamp"] - coverage["first_timestamp"]).clip(lower=0)
    coverage = (
        coverage.groupby(["camera_view", "bin_start"], as_index=False)
        .agg(
            coverage_s=("coverage_s", "sum"),
            source_files=("source_file", lambda x: "|".join(sorted(set(x)))),
            geometry_ids=("geometry_id", lambda x: "|".join(sorted(set(x)))),
        )
    )
    coverage["coverage_share"] = (coverage["coverage_s"] / (minutes * 60)).clip(upper=1)

    panel = density_bin.merge(flow, on=keys, how="left").merge(travel, on=keys, how="left")
    panel = panel.merge(coverage, on=["camera_view", "bin_start"], how="left")
    panel["entry_count"] = panel["entry_count"].fillna(0)
    panel["flow_veh_per_min"] = panel["flow_veh_per_min"].fillna(0)
    panel["bin_minutes"] = minutes
    panel["stable_coverage"] = panel["coverage_share"].ge(0.90)
    panel["cutoff_time"] = panel["camera_view"].map(CUTOFFS)
    panel["bin_center"] = panel["bin_start"] + pd.to_timedelta(minutes / 2, unit="min")
    panel["running_min"] = (panel["bin_center"] - panel["cutoff_time"]).dt.total_seconds() / 60
    panel["post"] = panel["running_min"].ge(0).astype(int)
    return panel


def directional_outcomes(panel: pd.DataFrame) -> pd.DataFrame:
    id_cols = [
        "camera_view",
        "bin_start",
        "bin_center",
        "bin_minutes",
        "coverage_share",
        "stable_coverage",
        "running_min",
        "post",
        "source_files",
        "geometry_ids",
    ]
    value_cols = [
        "flow_veh_per_min",
        "median_travel_time_s",
        "mean_vehicles_per_lane",
        "match_count",
    ]
    wide = panel.pivot_table(index=id_cols, columns="direction", values=value_cols, aggfunc="first").reset_index()
    wide.columns = ["_".join([str(v) for v in col if str(v)]) if isinstance(col, tuple) else col for col in wide.columns]
    for outcome in ["flow_veh_per_min", "median_travel_time_s", "mean_vehicles_per_lane"]:
        south = f"{outcome}_south_to_north"
        north = f"{outcome}_north_to_south"
        wide[f"delta_{outcome}"] = wide[south] - wide[north]
        wide[f"sum_{outcome}"] = wide[south] + wide[north]
    wide["travel_time_both_valid"] = (
        wide["match_count_south_to_north"].ge(wide["bin_minutes"].map(MIN_MATCHES))
        & wide["match_count_north_to_south"].ge(wide["bin_minutes"].map(MIN_MATCHES))
    )
    return wide


def fit_local(sample: pd.DataFrame, outcome: str) -> dict[str, object]:
    sample = sample.dropna(subset=[outcome, "running_min"]).copy()
    sample["post_running"] = sample["post"] * sample["running_min"]
    n_pre = int((sample["post"] == 0).sum())
    n_post = int((sample["post"] == 1).sum())
    result: dict[str, object] = {
        "n": len(sample),
        "n_pre": n_pre,
        "n_post": n_post,
        "mean_pre": sample.loc[sample["post"].eq(0), outcome].mean(),
        "mean_post": sample.loc[sample["post"].eq(1), outcome].mean(),
        "nearest_pre_min": sample.loc[sample["post"].eq(0), "running_min"].abs().min(),
        "nearest_post_min": sample.loc[sample["post"].eq(1), "running_min"].abs().min(),
    }
    if len(sample) < 8 or n_pre < 3 or n_post < 3:
        result.update({"tau": np.nan, "se": np.nan, "p_value": np.nan, "ci_low": np.nan, "ci_high": np.nan, "r2": np.nan})
        return result
    design = sm.add_constant(sample[["post", "running_min", "post_running"]])
    maxlags = max(1, min(4, len(sample) // 4))
    model = sm.OLS(sample[outcome], design).fit(
        cov_type="HAC", cov_kwds={"maxlags": maxlags, "use_correction": True}
    )
    tau = float(model.params["post"])
    se = float(model.bse["post"])
    result.update(
        {
            "tau": tau,
            "se": se,
            "p_value": float(model.pvalues["post"]),
            "ci_low": tau - 1.96 * se,
            "ci_high": tau + 1.96 * se,
            "r2": float(model.rsquared),
            "hac_lags": maxlags,
        }
    )
    return result


def estimate_rdit(wide_panels: dict[int, pd.DataFrame]) -> pd.DataFrame:
    outcomes = [
        "flow_veh_per_min_south_to_north",
        "flow_veh_per_min_north_to_south",
        "delta_flow_veh_per_min",
        "median_travel_time_s_south_to_north",
        "median_travel_time_s_north_to_south",
        "delta_median_travel_time_s",
        "mean_vehicles_per_lane_south_to_north",
        "mean_vehicles_per_lane_north_to_south",
        "delta_mean_vehicles_per_lane",
    ]
    rows: list[dict[str, object]] = []
    for minutes, panel in wide_panels.items():
        for camera in CUTOFFS:
            base = panel[(panel["camera_view"].eq(camera)) & panel["stable_coverage"]].copy()
            for bandwidth in BANDWIDTHS:
                sample = base[base["running_min"].abs().le(bandwidth)].copy()
                for outcome in outcomes:
                    outcome_sample = sample
                    if "travel_time" in outcome:
                        outcome_sample = sample[sample["travel_time_both_valid"]]
                    rows.append(
                        {
                            "camera_view": camera,
                            "bin_minutes": minutes,
                            "bandwidth_min": bandwidth,
                            "outcome": outcome,
                            **fit_local(outcome_sample, outcome),
                        }
                    )
    return pd.DataFrame(rows)


def qa_tables(
    matches: pd.DataFrame,
    crossings: pd.DataFrame,
    density: pd.DataFrame,
    run_qa: pd.DataFrame,
) -> None:
    TABLES.mkdir(parents=True, exist_ok=True)
    run_qa.to_csv(TABLES / "qa_by_segment.csv", index=False)
    quantiles = (
        matches.groupby(["camera_view", "geometry_id", "direction"])["travel_time_s"]
        .agg(
            n="size",
            mean="mean",
            p01=lambda x: x.quantile(0.01),
            p10=lambda x: x.quantile(0.10),
            median="median",
            p90=lambda x: x.quantile(0.90),
            p99=lambda x: x.quantile(0.99),
            max="max",
        )
        .reset_index()
    )
    quantiles.to_csv(TABLES / "travel_time_quantiles_by_geometry_direction.csv", index=False)
    class_counts = (
        matches.groupby(["camera_view", "direction", "class"]).size().rename("matched_tracks").reset_index()
    )
    class_counts.to_csv(TABLES / "matched_tracks_by_class.csv", index=False)
    crossing_qa = (
        crossings.groupby(["camera_view", "geometry_id", "direction", "gate"]).size().rename("crossings").reset_index()
    )
    crossing_qa.to_csv(TABLES / "gate_crossing_balance.csv", index=False)
    density_qa = (
        density.groupby(["camera_view", "geometry_id", "direction"])
        .agg(
            frames=("visible_vehicle_count", "size"),
            mean_visible=("visible_vehicle_count", "mean"),
            p99_visible=("visible_vehicle_count", lambda x: x.quantile(0.99)),
            mean_per_lane=("vehicles_per_lane_in_roi", "mean"),
        )
        .reset_index()
    )
    density_qa.to_csv(TABLES / "density_proxy_qa.csv", index=False)


def camera_agreement(panel: pd.DataFrame) -> pd.DataFrame:
    inbound = panel[panel["camera_view"].eq("inbound")]
    outbound = panel[panel["camera_view"].eq("outbound")]
    merged = inbound.merge(outbound, on=["bin_start", "direction"], suffixes=("_inbound", "_outbound"))
    merged = merged[merged["stable_coverage_inbound"] & merged["stable_coverage_outbound"]]
    rows: list[dict[str, object]] = []
    for direction in DIRECTIONS:
        sample = merged[merged["direction"].eq(direction)]
        rows.append(
            {
                "direction": direction,
                "overlapping_bins": len(sample),
                "flow_correlation": sample["flow_veh_per_min_inbound"].corr(sample["flow_veh_per_min_outbound"]),
                "travel_time_correlation": sample["median_travel_time_s_inbound"].corr(sample["median_travel_time_s_outbound"]),
                "median_inbound_outbound_flow_ratio": (
                    sample["flow_veh_per_min_inbound"] / sample["flow_veh_per_min_outbound"]
                ).median(),
            }
        )
    result = pd.DataFrame(rows)
    result.to_csv(TABLES / "camera_agreement_5min.csv", index=False)

    south = merged[merged["direction"].eq("south_to_north")].copy()
    south["period"] = np.where(south["bin_center_inbound"].lt(CUTOFFS["inbound"]), "pre", "post")
    stock_median = south["mean_vehicles_per_lane_inbound"].median()
    diagnostic_rows: list[dict[str, object]] = []
    groups = {
        "all": south,
        "pre": south[south["period"].eq("pre")],
        "post": south[south["period"].eq("post")],
        "low_inbound_stock": south[south["mean_vehicles_per_lane_inbound"].le(stock_median)],
        "high_inbound_stock": south[south["mean_vehicles_per_lane_inbound"].gt(stock_median)],
    }
    for label, sample in groups.items():
        diagnostic_rows.append(
            {
                "sample": label,
                "n_bins": len(sample),
                "flow_correlation": sample["flow_veh_per_min_inbound"].corr(sample["flow_veh_per_min_outbound"]),
                "mean_absolute_flow_gap": (
                    sample["flow_veh_per_min_inbound"] - sample["flow_veh_per_min_outbound"]
                ).abs().mean(),
                "median_inbound_stock": sample["mean_vehicles_per_lane_inbound"].median(),
            }
        )
    pd.DataFrame(diagnostic_rows).to_csv(TABLES / "south_to_north_bottleneck_diagnostic.csv", index=False)
    return result


def fixed_window_comparison(panel: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    stable = panel[panel["stable_coverage"]].copy()
    stable["window"] = np.select(
        [
            stable["running_min"].between(-45, -15, inclusive="both"),
            stable["running_min"].between(0, 30, inclusive="both"),
        ],
        ["pre_stable_minus45_minus15", "post_zero_plus30"],
        default="exclude",
    )
    stable = stable[stable["window"].ne("exclude")]
    outcomes = ["flow_veh_per_min", "median_travel_time_s", "mean_vehicles_per_lane"]
    means = (
        stable.groupby(["camera_view", "direction", "window"])[outcomes]
        .mean()
        .reset_index()
    )
    wide = means.pivot(index=["camera_view", "direction"], columns="window", values=outcomes)
    rows: list[dict[str, object]] = []
    for index, values in wide.iterrows():
        camera, direction = index
        row: dict[str, object] = {"camera_view": camera, "direction": direction}
        for outcome in outcomes:
            pre = values.get((outcome, "pre_stable_minus45_minus15"), np.nan)
            post = values.get((outcome, "post_zero_plus30"), np.nan)
            row[f"pre_{outcome}"] = pre
            row[f"post_{outcome}"] = post
            row[f"change_{outcome}"] = post - pre
            row[f"pct_change_{outcome}"] = (post / pre - 1) * 100 if pd.notna(pre) and pre != 0 else np.nan
        rows.append(row)
    result = pd.DataFrame(rows)
    result.to_csv(TABLES / "fixed_window_comparison_by_camera_direction.csv", index=False)

    pooled = (
        result.groupby("direction")
        .agg(
            pre_flow_veh_per_min=("pre_flow_veh_per_min", "mean"),
            post_flow_veh_per_min=("post_flow_veh_per_min", "mean"),
            pre_median_travel_time_s=("pre_median_travel_time_s", "mean"),
            post_median_travel_time_s=("post_median_travel_time_s", "mean"),
            pre_mean_vehicles_per_lane=("pre_mean_vehicles_per_lane", "mean"),
            post_mean_vehicles_per_lane=("post_mean_vehicles_per_lane", "mean"),
        )
        .reset_index()
    )
    for outcome in ["flow_veh_per_min", "median_travel_time_s", "mean_vehicles_per_lane"]:
        pooled[f"change_{outcome}"] = pooled[f"post_{outcome}"] - pooled[f"pre_{outcome}"]
        pooled[f"pct_change_{outcome}"] = (pooled[f"post_{outcome}"] / pooled[f"pre_{outcome}"] - 1) * 100
    pooled.to_csv(TABLES / "fixed_window_comparison_two_camera_mean.csv", index=False)
    return result, pooled


def plot_timeline(panel: pd.DataFrame, outcome: str, ylabel: str, filename: str) -> None:
    FIGURES.mkdir(parents=True, exist_ok=True)
    fig, axes = plt.subplots(2, 1, figsize=(13, 8), sharex=True, constrained_layout=True)
    for ax, camera in zip(axes, CUTOFFS):
        sub = panel[(panel["camera_view"].eq(camera)) & panel["stable_coverage"]]
        for direction in DIRECTIONS:
            s = sub[sub["direction"].eq(direction)]
            ax.plot(s["bin_center"], s[outcome], marker="o", ms=3, lw=1.2, color=COLORS[direction], label=DIR_LABELS[direction])
        ax.axvline(CUTOFFS[camera], color="#111827", lw=1.2, label="Deployment complete")
        if camera == "outbound":
            ax.axvline(CAMERA_ADJUSTMENT, color="#6b7280", lw=1, ls="--", label="Camera adjustment")
        ax.set_title(camera.capitalize(), loc="left", fontweight="bold")
        ax.set_ylabel(ylabel)
        ax.grid(color="#d1d5db", lw=0.6, alpha=0.7)
        ax.spines[["top", "right"]].set_visible(False)
        ax.legend(frameon=False, ncol=4, fontsize=8)
    axes[-1].set_xlabel("Local time, 2026-07-23")
    fig.savefig(FIGURES / filename, dpi=200, bbox_inches="tight")
    plt.close(fig)


def plot_rdit(
    wide: pd.DataFrame,
    transition: pd.DataFrame,
    outcome: str,
    ylabel: str,
    filename: str,
    bandwidth: int = 45,
) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(13, 5.5), constrained_layout=True)
    for ax, camera in zip(axes, CUTOFFS):
        sub = wide[
            wide["camera_view"].eq(camera)
            & wide["stable_coverage"]
            & wide["running_min"].abs().le(bandwidth)
        ].dropna(subset=[outcome])
        transition_sub = transition[
            transition["camera_view"].eq(camera)
            & transition["running_min"].abs().le(bandwidth)
            & transition["running_min"].le(0)
        ].dropna(subset=[outcome])
        ax.scatter(sub["running_min"], sub[outcome], s=28, color="#374151", alpha=0.85)
        ax.scatter(
            transition_sub["running_min"],
            transition_sub[outcome],
            s=34,
            color="#7c3aed",
            marker="D",
            alpha=0.9,
            label="Deployment in progress",
            zorder=4,
        )
        for post, color in [(0, "#2563eb"), (1, "#dc2626")]:
            side = sub[sub["post"].eq(post)]
            if len(side) >= 2:
                coef = np.polyfit(side["running_min"], side[outcome], 1)
                grid = np.linspace(side["running_min"].min(), side["running_min"].max(), 100)
                ax.plot(grid, coef[0] * grid + coef[1], color=color, lw=2)
        ax.axvline(0, color="#111827", lw=1)
        deployment_running = (DEPLOYMENT_STARTS[camera] - CUTOFFS[camera]).total_seconds() / 60
        ax.axvspan(deployment_running, 0, color="#c4b5fd", alpha=0.18, zorder=0)
        ax.set_title(camera.capitalize(), loc="left", fontweight="bold")
        ax.set_xlabel("Minutes relative to deployment completion")
        ax.set_ylabel(ylabel)
        ax.grid(color="#d1d5db", lw=0.6, alpha=0.7)
        ax.spines[["top", "right"]].set_visible(False)
        ax.legend(frameon=False, fontsize=8)
    fig.savefig(FIGURES / filename, dpi=200, bbox_inches="tight")
    plt.close(fig)


def plot_bandwidth(results: pd.DataFrame, outcome: str, filename: str) -> None:
    frame = results[(results["bin_minutes"].eq(5)) & (results["outcome"].eq(outcome))]
    fig, ax = plt.subplots(figsize=(8, 5.5), constrained_layout=True)
    for camera, marker in [("inbound", "o"), ("outbound", "s")]:
        s = frame[frame["camera_view"].eq(camera)].sort_values("bandwidth_min")
        ax.errorbar(s["bandwidth_min"], s["tau"], yerr=1.96 * s["se"], marker=marker, capsize=3, label=camera)
    ax.axhline(0, color="#111827", lw=1)
    ax.set_xlabel("Bandwidth (minutes)")
    ax.set_ylabel("Estimated discontinuity")
    ax.grid(axis="y", color="#d1d5db", lw=0.6, alpha=0.7)
    ax.spines[["top", "right"]].set_visible(False)
    ax.legend(frameon=False)
    fig.savefig(FIGURES / filename, dpi=200, bbox_inches="tight")
    plt.close(fig)


def plot_primary_direction(
    wide: pd.DataFrame,
    transition: pd.DataFrame,
    direction: str,
    lane_change_label: str,
    output_name: str,
    bandwidth: int = 45,
) -> None:
    camera = "outbound"
    stable = wide[
        wide["camera_view"].eq(camera)
        & wide["stable_coverage"]
        & wide["running_min"].abs().le(bandwidth)
    ].copy()
    construction = transition[
        transition["camera_view"].eq(camera)
        & transition["running_min"].abs().le(bandwidth)
        & transition["running_min"].le(0)
    ].copy()
    outcomes = [
        (f"flow_veh_per_min_{direction}", "Flow (vehicles/min)"),
        (f"median_travel_time_s_{direction}", "Median travel time (s)"),
    ]
    fig, axes = plt.subplots(1, 2, figsize=(7.1, 3.35), constrained_layout=True)
    pre_color = "#4C78A8"
    post_color = "#E15759"
    transition_color = "#8B5CF6"
    deployment_running = (DEPLOYMENT_STARTS[camera] - CUTOFFS[camera]).total_seconds() / 60

    for panel_label, ax, (outcome, ylabel) in zip(["a", "b"], axes, outcomes):
        sample = stable.dropna(subset=[outcome])
        trans = construction.dropna(subset=[outcome])
        ax.axvspan(deployment_running, 0, color="#DDD6FE", alpha=0.42, lw=0, zorder=0)
        for post, color, label in [
            (0, pre_color, "Stable pre"),
            (1, post_color, "Stable post"),
        ]:
            side = sample[sample["post"].eq(post)]
            ax.scatter(
                side["running_min"],
                side[outcome],
                s=22,
                color=color,
                edgecolor="white",
                linewidth=0.5,
                label=label,
                zorder=3,
            )
            if len(side) >= 2:
                coefficient = np.polyfit(side["running_min"], side[outcome], 1)
                grid = np.linspace(side["running_min"].min(), side["running_min"].max(), 100)
                ax.plot(grid, coefficient[0] * grid + coefficient[1], color=color, lw=1.5, zorder=2)
        ax.scatter(
            trans["running_min"],
            trans[outcome],
            s=26,
            marker="D",
            color=transition_color,
            edgecolor="white",
            linewidth=0.5,
            label="Deployment",
            zorder=4,
        )
        ax.axvline(0, color="#111827", lw=0.9)
        ax.text(
            0.985,
            0.97,
            lane_change_label,
            transform=ax.transAxes,
            ha="right",
            va="top",
            fontsize=7.5,
            color="#374151",
            bbox={"facecolor": "white", "edgecolor": "none", "alpha": 0.78, "pad": 1.5},
        )
        ax.text(
            -0.12,
            1.03,
            panel_label,
            transform=ax.transAxes,
            ha="left",
            va="bottom",
            fontsize=11,
            fontweight="bold",
        )
        ax.set_xlabel("Minutes relative to deployment completion")
        ax.set_ylabel(ylabel)
        ax.set_xlim(-bandwidth - 1, bandwidth + 1)
        ax.grid(axis="y", color="#D1D5DB", lw=0.45, alpha=0.65)
        ax.spines[["top", "right"]].set_visible(False)
        ax.tick_params(direction="out", width=0.8, length=3, labelsize=7.5)
        ax.xaxis.label.set_size(8.5)
        ax.yaxis.label.set_size(8.5)

    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, 1.04), ncol=3, frameon=False, fontsize=7.5)
    output_base = FIGURES / output_name
    fig.savefig(output_base.with_suffix(".png"), dpi=600, bbox_inches="tight")
    fig.savefig(output_base.with_suffix(".pdf"), bbox_inches="tight")
    fig.savefig(output_base.with_suffix(".svg"), bbox_inches="tight")
    plt.close(fig)


def write_docs(plan: pd.DataFrame) -> None:
    DOCS.mkdir(parents=True, exist_ok=True)
    (DOCS / "analysis-plan.md").write_text(
        """# 分析计划

测量单位为摄像机视角、道路方向与本地时间 bin。稳定状态样本由布设前和布设完成后的片段构成，施工过程被排除。流量取目标方向进入两线测量区的第一条横断线穿越次数；通行时间取同一轨迹穿越两条横断线的时间差；车辆存量取两线之间方向 ROI 内逐帧可见车辆数。

主经验结果采用出城机位。进城机位靠近南向北上桥瓶颈，车辆在两个机位之间发生储存和释放，不应被视为同一断面的重复计数；该机位用于瓶颈诊断和测量稳健性。

主断点按双机位分别设为 16:26:17 和 16:26:34。局部线性模型允许断点两侧斜率不同，标准误采用 HAC。结果按 1、2、5 分钟 bin 和 20、30、45、60 分钟带宽报告。一天的时间断点估计属于探索性 RDiT，不解释为严格的横截面 RDD 或最终因果估计。
""",
        encoding="utf-8",
    )
    (DOCS / "variable-dictionary.md").write_text(
        """# 变量字典

| 变量 | 定义 |
|---|---|
| `entry_count` | 车辆轨迹穿越目标方向入口测速线的次数 |
| `flow_veh_per_min` | `entry_count / bin分钟数` |
| `travel_time_s` | 同一车辆从入口测速线到出口测速线的秒数 |
| `median_travel_time_s` | 方向和时间 bin 内通行时间中位数 |
| `mean_visible_vehicles` | 两线之间方向 ROI 的逐帧平均可见车辆数 |
| `mean_vehicles_per_lane` | 可见车辆数除以该状态的方向车道数 |
| `running_min` | bin 中点相对本机位布设完成时刻的分钟数 |
| `post` | `running_min >= 0` |
| `delta_*` | 南向北减北向南的方向差分 |
| `coverage_share` | 时间 bin 内稳定视频覆盖比例 |
| `travel_time_valid` | 方向 bin 内双线匹配数量达到最低阈值 |

实际线距尚未锁定，因此不生成 km/h。`speed_kmh_per_100m_assumed` 仅为程序诊断字段，不进入经验分析。

YOLO 追踪层的方向标签表示画面运动方向。出城机位的追踪标签与地理方向一致；进城机位朝向相反，汇总时将两个追踪方向互换为真实地理方向。
""",
        encoding="utf-8",
    )
    (DOCS / "sample-lineage.md").write_text(
        f"""# 样本流向

1. 原始输入为 6 段视频。
2. 施工区间被排除，保留 {len(plan)} 个稳定状态分段。
3. YOLO 和 ByteTrack 生成逐车轨迹、单线穿越、双线匹配及逐帧车辆存量。
4. 单线穿越按入口线聚合为流量；双线成功匹配聚合为通行时间；逐帧 ROI 聚合为车辆存量。
5. 施工窗口采用覆盖整幅道路的双线重新测量，按车辆运动方向区分双向，并显示在 RDiT 图中。
6. 时间 bin 覆盖率低于 90% 的稳定状态观测不进入 RDiT 拟合。
7. 通行时间要求每方向至少 3、5、10 条匹配轨迹，分别对应 1、2、5 分钟 bin。
8. 主估计只比较施工前稳定样本与布设完成后的稳定样本；施工观测不编码为 `post=1`。
""",
        encoding="utf-8",
    )
    (DOCS / "geospatial-data-notes.md").write_text(
        """# 空间测量说明

每个摄像机状态使用独立的两线 geometry。每条横断线在中央分界点被拆为左右画面线段，两条线之间的四边形构成方向 ROI。出城机位左侧对应南向北、右侧对应北向南；进城机位左侧对应北向南、右侧对应南向北。`IMG_0017.MOV` 在 17:08:42 调整机位，调整前后采用不同 geometry。

两条线之间的实际纵向道路距离尚未提供。现阶段的空间对象支持车辆通行时间与可见车辆存量测量，不支持绝对速度或每公里密度。
""",
        encoding="utf-8",
    )
    (DOCS / "exposure-definition.md").write_text(
        """# Treatment 定义

Treatment 为潮汐车道布设完成后的方向性容量重分配。南向北由 3 车道增至 4 车道，北向南由 4 车道减至 3 车道。双机位分别以画面中调整方向后的车流开始出现作为 treatment 起点。

布设期间不是 treatment 的渐进剂量，而是无法稳定定义车道可用状态的过渡期，因此从估计样本中删除。方向差分利用两个方向容量变化符号相反的制度结构，但不能消除同一时点的需求变化。
""",
        encoding="utf-8",
    )
    (DOCS / "regression-spec-matrix.md").write_text(
        """# 回归规格矩阵

| 规格 | 因变量 | Treatment | 趋势 | 样本 | 标准误 |
|---|---|---|---|---|---|
| R1 | 双向流量 | `post` | 断点两侧线性 | 稳定覆盖 | HAC |
| R2 | 方向流量差 | `post` | 断点两侧线性 | 稳定覆盖 | HAC |
| R3 | 双向通行时间中位数 | `post` | 断点两侧线性 | 匹配数量达标 | HAC |
| R4 | 方向通行时间差 | `post` | 断点两侧线性 | 双向匹配数量达标 | HAC |
| R5 | 每车道可见车辆存量 | `post` | 断点两侧线性 | 稳定覆盖 | HAC |

每个规格分别报告双机位、1/2/5 分钟 bin 和 20/30/45/60 分钟带宽。没有额外控制变量、固定效应或权重。
""",
        encoding="utf-8",
    )
    (DOCS / "table-shells.md").write_text(
        """# 表格结构

主表按 outcome 分 panel。列对应 inbound 与 outbound 两个机位；行依次报告 discontinuity、HAC 标准误、断点前均值、样本数、bin 宽度、带宽和最近稳定观测距断点的时间。稳健性表展开全部 bin 与带宽组合。
""",
        encoding="utf-8",
    )
    (DOCS / "replication-checklist.md").write_text(
        """# 复现检查表

- [x] 六段原始视频与九个稳定分段可追溯
- [x] 双线 geometry 与车道状态表已锁定
- [x] 施工过渡期排除
- [x] 机位调整前后使用独立 geometry
- [x] 分钟面板保留摄像机来源与覆盖率
- [x] RDiT 报告 bin 和带宽敏感性
- [x] HAC 标准误与断点两侧样本数已导出
- [ ] 实际线距待补充
- [ ] 至少五个工作日的重复断点待采集
- [ ] 控制道路或未处理时段对照待补充
""",
        encoding="utf-8",
    )


def write_summary(
    results: pd.DataFrame,
    run_qa: pd.DataFrame,
    agreement: pd.DataFrame,
    fixed_window_by_camera: pd.DataFrame,
    fixed_window_two_camera: pd.DataFrame,
) -> None:
    primary = results[
        results["bin_minutes"].eq(5)
        & results["bandwidth_min"].eq(45)
        & results["outcome"].isin(
            [
                "flow_veh_per_min_south_to_north",
                "flow_veh_per_min_north_to_south",
                "delta_flow_veh_per_min",
                "median_travel_time_s_south_to_north",
                "median_travel_time_s_north_to_south",
                "delta_median_travel_time_s",
            ]
        )
    ].copy()
    lines = [
        "# 2026-07-23 彩田路探索性经验结果",
        "",
        f"全量测量包含 {int(run_qa['matched_tracks'].sum()):,} 条双线匹配轨迹和 {int(run_qa['gate_crossings'].sum()):,} 次单线穿越。",
        "",
        "主经验口径采用出城机位。进城机位位于上桥瓶颈附近，仅用于空间拥堵诊断和重复测量 QA。",
        "",
        "探索性 RDiT 采用 5 分钟 bin 和 45 分钟带宽。估计允许断点两侧线性趋势不同，并采用 HAC 标准误。",
        "",
    ]
    for _, row in primary[primary["camera_view"].eq("outbound")].iterrows():
        lines.append(
            f"- {row['camera_view']} / {row['outcome']}：跳变 {row['tau']:.3f}，"
            f"SE {row['se']:.3f}，p={row['p_value']:.3f}，n={int(row['n'])}。"
        )
    lines.extend(
        [
            "",
            "## 固定窗口描述",
            "",
        ]
    )
    outbound_window = fixed_window_by_camera[fixed_window_by_camera["camera_view"].eq("outbound")]
    for _, row in outbound_window.iterrows():
        lines.append(
            f"- {row['direction']}：出城机位流量由 {row['pre_flow_veh_per_min']:.2f} 变为 "
            f"{row['post_flow_veh_per_min']:.2f} 辆/分钟（{row['pct_change_flow_veh_per_min']:.1f}%）；"
            f"通行时间由 {row['pre_median_travel_time_s']:.2f} 变为 "
            f"{row['post_median_travel_time_s']:.2f} 秒（{row['pct_change_median_travel_time_s']:.1f}%）。"
        )
    lines.extend(
        [
            "",
            "固定窗口把断点前 [-45,-15] 分钟的稳定样本与断点后 [0,30] 分钟比较。上述数值仅来自出城机位。",
            "",
            "## 质量与限制",
            "",
            f"共同覆盖的 5 分钟 bin 中，南向北流量相关系数为 {agreement.loc[agreement['direction'].eq('south_to_north'), 'flow_correlation'].iloc[0]:.3f}，"
            f"北向南为 {agreement.loc[agreement['direction'].eq('north_to_south'), 'flow_correlation'].iloc[0]:.3f}。"
            f"进城机位相对出城机位的流量中位比约为 {agreement['median_inbound_outbound_flow_ratio'].median():.3f}。",
            "",
            "南向北在进城机位低车辆存量时，两机位流量相关系数为 0.969，平均绝对流量差为 2.47 辆/分钟；高车辆存量时相关系数为 0.739，平均绝对流量差扩大到 20.26 辆/分钟。该分层结果与上桥瓶颈造成车辆储存和释放的解释一致，但仍包含拥堵遮挡与轨迹识别差异。",
            "",
            "断点前存在施工排除区间，最近稳定观测距离断点约 19 至 24 分钟。RDiT 系数依赖两侧趋势外推，并对带宽敏感；HAC p 值在单日、少量 bin 下不作为正式显著性证据。一天内的交通需求趋势、geometry 随车道边界变化以及施工后的短期扰动仍可能影响结果。",
            "",
            "通行时间以秒计。实际道路距离补充前不报告 km/h，车辆存量也不换算为每公里密度。",
        ]
    )
    (OUT / "RESULTS_SUMMARY.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    primary.to_csv(TABLES / "rdit_5min_45min_all_cameras.csv", index=False)
    primary[primary["camera_view"].eq("outbound")].to_csv(TABLES / "rdit_primary_outbound_5min_45min.csv", index=False)


def main() -> None:
    for directory in (OUT, TABLES, FIGURES, DOCS):
        directory.mkdir(parents=True, exist_ok=True)
    plan = parse_run_plan()
    matches, crossings, density, run_qa = load_measurements(plan)
    transition_matches, transition_crossings = load_transition_measurements()
    qa_tables(matches, crossings, density, run_qa)

    panels: dict[int, pd.DataFrame] = {}
    wide_panels: dict[int, pd.DataFrame] = {}
    for minutes in BIN_MINUTES:
        panel = make_panel(matches, crossings, density, minutes)
        wide = directional_outcomes(panel)
        panels[minutes] = panel
        wide_panels[minutes] = wide
        panel.to_csv(TABLES / f"panel_direction_{minutes}min.csv", index=False)
        wide.to_csv(TABLES / f"panel_directional_outcomes_{minutes}min.csv", index=False)

    results = estimate_rdit(wide_panels)
    results.to_csv(TABLES / "rdit_all_specifications.csv", index=False)

    panel5 = panels[5]
    wide5 = wide_panels[5]
    transition_wide5 = make_transition_wide(transition_matches, transition_crossings, 5)
    transition_wide5.to_csv(TABLES / "transition_directional_outcomes_5min.csv", index=False)
    agreement = camera_agreement(panel5)
    fixed_window_by_camera, fixed_window_two_camera = fixed_window_comparison(panels[1])
    plot_timeline(panel5, "flow_veh_per_min", "Vehicles per minute", "01_flow_timeline_5min.png")
    plot_timeline(panel5, "median_travel_time_s", "Median travel time (seconds)", "02_travel_time_timeline_5min.png")
    plot_timeline(panel5, "mean_vehicles_per_lane", "Visible vehicles per lane", "03_vehicle_stock_timeline_5min.png")
    plot_rdit(wide5, transition_wide5, "delta_flow_veh_per_min", "South-to-north minus north-to-south flow", "04_rdit_directional_flow_5min.png")
    plot_rdit(wide5, transition_wide5, "delta_median_travel_time_s", "Directional travel-time difference (seconds)", "05_rdit_directional_travel_time_5min.png")
    plot_bandwidth(results, "delta_flow_veh_per_min", "06_rdit_flow_bandwidth_sensitivity.png")
    plot_bandwidth(results, "delta_median_travel_time_s", "07_rdit_travel_time_bandwidth_sensitivity.png")
    plot_primary_direction(
        wide5,
        transition_wide5,
        direction="south_to_north",
        lane_change_label="+1 south-to-north lane",
        output_name="08_rdit_added_direction_outbound",
        bandwidth=100,
    )
    plot_primary_direction(
        wide5,
        transition_wide5,
        direction="north_to_south",
        lane_change_label="-1 north-to-south lane",
        output_name="09_rdit_removed_direction_outbound",
        bandwidth=100,
    )

    write_docs(plan)
    write_summary(results, run_qa, agreement, fixed_window_by_camera, fixed_window_two_camera)
    print(
        results[
            results["bin_minutes"].eq(5)
            & results["bandwidth_min"].eq(45)
            & results["outcome"].isin(["delta_flow_veh_per_min", "delta_median_travel_time_s"])
        ][["camera_view", "outcome", "tau", "se", "p_value", "n", "n_pre", "n_post", "nearest_pre_min", "nearest_post_min"]].to_string(index=False)
    )
    print(f"output={OUT}")


if __name__ == "__main__":
    main()
