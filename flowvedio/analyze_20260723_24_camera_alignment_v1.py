#!/usr/bin/env python3
"""Compare contemporaneous flow measurements from the two Caitian cameras."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from analyze_20260723_24_state_dependent_v5 import OUTPUT as BASIC_OUTPUT
from analyze_20260723_24_state_dependent_v5 import build_event_and_segment_tables


OUTPUT = BASIC_OUTPUT / "camera_alignment_v1"
BIN_MINUTES = (1, 3, 5)
LAGS = range(-5, 6)
DIRECTIONS = ("south_to_north", "north_to_south")


def coverage_panel(segments: pd.DataFrame, minutes: int) -> pd.DataFrame:
    freq = f"{minutes}min"
    rows: list[dict[str, object]] = []
    unique = segments.drop_duplicates("stage_id")
    for row in unique.itertuples(index=False):
        start = pd.Timestamp(f"{row.date} {row.start_local}")
        end = pd.Timestamp(f"{row.date} {row.end_local}")
        cursor = start.floor(freq)
        while cursor < end:
            bin_end = cursor + pd.Timedelta(minutes=minutes)
            covered_s = max(0.0, (min(end, bin_end) - max(start, cursor)).total_seconds())
            if covered_s:
                rows.append(
                    {
                        "date": row.date,
                        "collection_direction": row.collection_direction,
                        "tidal_state": row.tidal_state,
                        "bin_start": cursor,
                        "covered_seconds": covered_s,
                    }
                )
            cursor = bin_end
    coverage = pd.DataFrame(rows)
    return (
        coverage.groupby(["date", "collection_direction", "tidal_state", "bin_start"], as_index=False)
        ["covered_seconds"].sum()
    )


def flow_panel(events: pd.DataFrame, segments: pd.DataFrame, minutes: int) -> pd.DataFrame:
    freq = f"{minutes}min"
    coverage = coverage_panel(segments, minutes)
    event = events.copy()
    event["bin_start"] = event["recorded_at"].dt.floor(freq)
    counts = (
        event.groupby(
            ["date", "collection_direction", "tidal_state", "direction", "bin_start"],
            as_index=False,
        ).size().rename(columns={"size": "entry_crossings"})
    )
    directions = pd.DataFrame({"direction": DIRECTIONS})
    panel = coverage.merge(directions, how="cross").merge(
        counts,
        on=["date", "collection_direction", "tidal_state", "direction", "bin_start"],
        how="left",
    )
    panel["entry_crossings"] = panel["entry_crossings"].fillna(0).astype(int)
    panel["flow_veh_per_min"] = panel["entry_crossings"] / panel["covered_seconds"] * 60.0
    panel["complete_bin"] = panel["covered_seconds"].ge(minutes * 60 * 0.90)
    panel["bin_minutes"] = minutes
    return panel


def residual_correlation(frame: pd.DataFrame) -> float:
    if len(frame) < 3:
        return float("nan")
    groups = [frame["date"], frame["tidal_state"]]
    left = frame["flow_veh_per_min_inbound"] - frame["flow_veh_per_min_inbound"].groupby(groups).transform("mean")
    right = frame["flow_veh_per_min_outbound"] - frame["flow_veh_per_min_outbound"].groupby(groups).transform("mean")
    return float(left.corr(right))


def lag_results(panel: pd.DataFrame, minutes: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    base = panel[panel["complete_bin"]].copy()
    inbound = base[base["collection_direction"].eq("进城")].drop(columns="collection_direction")
    outbound = base[base["collection_direction"].eq("出城")].drop(columns="collection_direction")
    pooled_rows: list[dict[str, object]] = []
    dated_rows: list[dict[str, object]] = []
    for lag in LAGS:
        shifted = outbound.copy()
        shifted["match_time"] = shifted["bin_start"] - pd.Timedelta(minutes=lag)
        merged = inbound.merge(
            shifted,
            left_on=["date", "tidal_state", "direction", "bin_start"],
            right_on=["date", "tidal_state", "direction", "match_time"],
            suffixes=("_inbound", "_outbound"),
        )
        for direction in DIRECTIONS:
            sample = merged[merged["direction"].eq(direction)].copy()
            pooled_rows.append(
                {
                    "bin_minutes": minutes,
                    "direction": direction,
                    "lag_outbound_minus_inbound_min": lag,
                    "paired_bins": len(sample),
                    "pearson_raw": sample["flow_veh_per_min_inbound"].corr(sample["flow_veh_per_min_outbound"]),
                    "spearman_raw": sample["flow_veh_per_min_inbound"].corr(sample["flow_veh_per_min_outbound"], method="spearman"),
                    "pearson_within_date_state": residual_correlation(sample),
                    "mean_inbound": sample["flow_veh_per_min_inbound"].mean(),
                    "mean_outbound": sample["flow_veh_per_min_outbound"].mean(),
                    "median_outbound_inbound_ratio": (sample["flow_veh_per_min_outbound"] / sample["flow_veh_per_min_inbound"].replace(0, np.nan)).median(),
                    "mean_absolute_gap": (sample["flow_veh_per_min_outbound"] - sample["flow_veh_per_min_inbound"]).abs().mean(),
                }
            )
            for date, dated in sample.groupby("date"):
                dated_rows.append(
                    {
                        "bin_minutes": minutes,
                        "date": date,
                        "direction": direction,
                        "lag_outbound_minus_inbound_min": lag,
                        "paired_bins": len(dated),
                        "pearson": dated["flow_veh_per_min_inbound"].corr(dated["flow_veh_per_min_outbound"]),
                        "spearman": dated["flow_veh_per_min_inbound"].corr(dated["flow_veh_per_min_outbound"], method="spearman"),
                    }
                )
    return pd.DataFrame(pooled_rows), pd.DataFrame(dated_rows)


def main() -> None:
    OUTPUT.mkdir(parents=True, exist_ok=True)
    events, segments = build_event_and_segment_tables()
    pooled_parts: list[pd.DataFrame] = []
    dated_parts: list[pd.DataFrame] = []
    for minutes in BIN_MINUTES:
        panel = flow_panel(events, segments, minutes)
        panel.to_csv(OUTPUT / f"camera_flow_panel_{minutes}min.csv", index=False)
        pooled, dated = lag_results(panel, minutes)
        pooled_parts.append(pooled)
        dated_parts.append(dated)
    pooled = pd.concat(pooled_parts, ignore_index=True)
    dated = pd.concat(dated_parts, ignore_index=True)
    pooled.to_csv(OUTPUT / "lag_correlations_pooled.csv", index=False)
    dated.to_csv(OUTPUT / "lag_correlations_by_date.csv", index=False)

    eligible = pooled[pooled["paired_bins"].ge(20)].copy()
    best = eligible.loc[
        eligible.groupby(["bin_minutes", "direction"])["pearson_within_date_state"].idxmax()
    ].sort_values(["bin_minutes", "direction"])
    best.to_csv(OUTPUT / "best_lag_by_bin_direction.csv", index=False)
    zero = pooled[pooled["lag_outbound_minus_inbound_min"].eq(0)].copy()
    zero.to_csv(OUTPUT / "same_time_correlations.csv", index=False)
    print("SAME TIME\n" + zero.to_string(index=False))
    print("\nBEST LAG\n" + best.to_string(index=False))


if __name__ == "__main__":
    main()
