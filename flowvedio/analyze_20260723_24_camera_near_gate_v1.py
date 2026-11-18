#!/usr/bin/env python3
"""Camera agreement using the visually reliable near gate for both directions."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from analyze_20260723_24_camera_alignment_v1 import BIN_MINUTES, flow_panel, lag_results
from analyze_20260723_24_state_dependent_v5 import CONFIG, SOURCE, build_event_and_segment_tables


OUTPUT = SOURCE / "analysis_basic_v1/camera_alignment_near_gate_v1"


def load_near_events() -> pd.DataFrame:
    plan = pd.read_csv(SOURCE / "run_plan.csv")
    config = json.loads(CONFIG.read_text(encoding="utf-8"))
    states = {str(row["阶段编号"]): row for row in config["states"]}
    parts: list[pd.DataFrame] = []
    for row in plan.itertuples(index=False):
        state = states[str(row.stage_id)]
        data = pd.read_csv(Path(row.measurement_dir) / "gate_crossings.csv")
        data = data[data["gate"].eq("near")].copy()
        start = pd.Timestamp(f'{state["日期"]} {state["实际开始"]}')
        data["recorded_at"] = start + pd.to_timedelta(data["timestamp_s"] - float(row.start_s), unit="s")
        data["stage_id"] = row.stage_id
        data["date"] = str(state["日期"])
        data["collection_direction"] = str(state["采集方向"])
        data["state"] = str(state["阶段状态"])
        data["tidal_state"] = str(state["潮汐可变区状态"])
        data["source_file"] = row.source_file
        parts.append(data)
    return pd.concat(parts, ignore_index=True)


def main() -> None:
    OUTPUT.mkdir(parents=True, exist_ok=True)
    near = load_near_events()
    _, segments = build_event_and_segment_tables()
    pooled_parts: list[pd.DataFrame] = []
    dated_parts: list[pd.DataFrame] = []
    for minutes in BIN_MINUTES:
        panel = flow_panel(near, segments, minutes)
        panel.to_csv(OUTPUT / f"near_gate_camera_flow_panel_{minutes}min.csv", index=False)
        pooled, dated = lag_results(panel, minutes)
        pooled_parts.append(pooled)
        dated_parts.append(dated)
    pooled = pd.concat(pooled_parts, ignore_index=True)
    dated = pd.concat(dated_parts, ignore_index=True)
    pooled.to_csv(OUTPUT / "near_gate_lag_correlations_pooled.csv", index=False)
    dated.to_csv(OUTPUT / "near_gate_lag_correlations_by_date.csv", index=False)
    zero = pooled[pooled["lag_outbound_minus_inbound_min"].eq(0)].copy()
    zero.to_csv(OUTPUT / "near_gate_same_time_correlations.csv", index=False)
    eligible = pooled[pooled["paired_bins"].ge(20)]
    best = eligible.loc[
        eligible.groupby(["bin_minutes", "direction"])["pearson_within_date_state"].idxmax()
    ].sort_values(["bin_minutes", "direction"])
    best.to_csv(OUTPUT / "near_gate_best_lag.csv", index=False)
    print("SAME TIME\n" + zero.to_string(index=False))
    print("\nBEST\n" + best.to_string(index=False))


if __name__ == "__main__":
    main()
