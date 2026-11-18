#!/usr/bin/env python3
"""Case-by-case outbound RDiT allowing all outbound-camera devices and geometries."""

from copy import deepcopy

import pandas as pd

import analyze_outbound_casewise_rdit_v1 as v1
from analyze_20260723_24_camera_alignment_v1 import flow_panel
from analyze_20260723_24_camera_near_gate_v1 import load_near_events
from analyze_20260723_24_state_dependent_v5 import SOURCE, build_event_and_segment_tables


OUTPUT = SOURCE / "analysis_basic_v1/outbound_casewise_rdit_all_devices_v2"
EVENTS = deepcopy(v1.EVENTS)
for event in EVENTS:
    cutoff = pd.Timestamp(event["cutoff"])
    event["geometry_start"] = str(cutoff - pd.Timedelta(minutes=60))
    event["geometry_end"] = str(cutoff + pd.Timedelta(minutes=60))
    event["design_grade"] = "all_outbound_devices"
    event["design_note"] = (
        "All outbound-camera devices and geometries are admitted under a common near-line flow definition."
    )


def main() -> None:
    OUTPUT.mkdir(parents=True, exist_ok=True)
    v1.OUTPUT = OUTPUT
    near = load_near_events()
    _, segments = build_event_and_segment_tables()
    panels_by_bin = {b: flow_panel(near, segments, b) for b in [1, 5]}
    results, panels = [], []
    for event in EVENTS:
        for bandwidth in [30, 45, 60]:
            sample = v1.make_panel(panels_by_bin[1], event, bandwidth)
            sample["bin_minutes"] = 1
            panels.append(sample)
            for outcome in v1.OUTCOMES:
                results.append({
                    "event": event["event"], "event_label": event["label"],
                    "design_grade": event["design_grade"], "design_note": event["design_note"],
                    "favored_after": event["favored_after"], "outcome": outcome,
                    "outcome_label": v1.OUTCOMES[outcome], "bandwidth_min": bandwidth,
                    "bin_minutes": 1, **v1.estimate(sample, outcome),
                })
        v1.plot_event(event, v1.make_panel(panels_by_bin[5], event, 60))
    result = pd.DataFrame(results)
    result.to_csv(OUTPUT / "casewise_rdit_estimates_all_bandwidths_v2.csv", index=False)
    pd.concat(panels, ignore_index=True).to_csv(OUTPUT / "casewise_rdit_analysis_panel_v2.csv", index=False)
    result[result["bandwidth_min"] == 60].to_csv(OUTPUT / "casewise_rdit_main_60min_v2.csv", index=False)
    result[result["outcome"] == "favored_minus_unfavored"].to_csv(
        OUTPUT / "casewise_rdit_netflow_bandwidth_robustness_v2.csv", index=False
    )
    print(result[result["bandwidth_min"] == 60][
        ["event", "outcome", "n", "pre_bins", "post_bins", "tau", "se_hac", "p_hac"]
    ].to_string(index=False))


if __name__ == "__main__":
    main()
