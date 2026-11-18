#!/usr/bin/env python3
"""Exploratory outbound-camera RDiT using near-gate vehicle flow only."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import statsmodels.api as sm

from analyze_20260723_24_camera_alignment_v1 import flow_panel
from analyze_20260723_24_camera_near_gate_v1 import load_near_events
from analyze_20260723_24_state_dependent_v5 import SOURCE, build_event_and_segment_tables


OUTPUT = SOURCE / "analysis_basic_v1/outbound_rdit_near_v1"
EVENTS = [
    {
        "event": "20260723_opening",
        "cutoff": "2026-07-23 16:26:34",
        "transition_start": "2026-07-23 16:11:09",
        "geometry": "M08",
        "favored_after": "south_to_north",
        "geometry_start": "2026-07-23 15:58:34",
        "geometry_end": "2026-07-23 17:08:42",
    },
    {
        "event": "20260723_recovery",
        "cutoff": "2026-07-23 21:46:35",
        "transition_start": "2026-07-23 21:31:35",
        "geometry": "M10",
        "favored_after": "north_to_south",
        "geometry_start": "2026-07-23 20:11:35",
        "geometry_end": "2026-07-23 22:17:41.870",
    },
    {
        "event": "20260724_recovery",
        "cutoff": "2026-07-24 21:42:41",
        "transition_start": "2026-07-24 21:34:16",
        "geometry": "M18",
        "favored_after": "north_to_south",
        "geometry_start": "2026-07-24 20:02:56",
        "geometry_end": "2026-07-24 22:16:24.672",
    },
]


def fit_local(sample: pd.DataFrame, outcome: str) -> dict[str, float | int]:
    clean = sample.dropna(subset=[outcome, "running_min", "post"]).copy()
    if len(clean) < 8 or clean["post"].nunique() < 2:
        return {"n": len(clean), "tau": np.nan, "se_hac": np.nan, "p_hac": np.nan}
    design = pd.DataFrame(
        {
            "const": 1.0,
            "post": clean["post"].astype(float),
            "running": clean["running_min"],
            "post_running": clean["post"] * clean["running_min"],
        }
    )
    fit = sm.OLS(clean[outcome], design).fit(cov_type="HAC", cov_kwds={"maxlags": 5})
    return {
        "n": len(clean),
        "pre_bins": int((clean["post"] == 0).sum()),
        "post_bins": int((clean["post"] == 1).sum()),
        "tau": float(fit.params["post"]),
        "se_hac": float(fit.bse["post"]),
        "p_hac": float(fit.pvalues["post"]),
    }


def event_panel(panel: pd.DataFrame, event: dict[str, str], bandwidth: int) -> pd.DataFrame:
    cutoff = pd.Timestamp(event["cutoff"])
    transition_start = pd.Timestamp(event["transition_start"])
    geometry_start = pd.Timestamp(event["geometry_start"])
    geometry_end = pd.Timestamp(event["geometry_end"])
    base = panel[
        panel["collection_direction"].eq("出城")
        & panel["complete_bin"]
        & panel["bin_start"].between(cutoff - pd.Timedelta(minutes=bandwidth), cutoff + pd.Timedelta(minutes=bandwidth), inclusive="left")
        & panel["bin_start"].ge(geometry_start)
        & panel["bin_start"].lt(geometry_end)
    ].copy()
    base = base[(base["bin_start"].lt(transition_start)) | (base["bin_start"].ge(cutoff))]
    wide = base.pivot_table(
        index="bin_start", columns="direction", values="flow_veh_per_min", aggfunc="first"
    ).reset_index()
    wide["running_min"] = (wide["bin_start"] - cutoff).dt.total_seconds() / 60.0
    wide["post"] = wide["bin_start"].ge(cutoff).astype(int)
    wide["total_flow"] = wide["south_to_north"] + wide["north_to_south"]
    favored = event["favored_after"]
    unfavored = "north_to_south" if favored == "south_to_north" else "south_to_north"
    wide["favored_minus_unfavored"] = wide[favored] - wide[unfavored]
    wide["event"] = event["event"]
    wide["bandwidth_min"] = bandwidth
    return wide


def main() -> None:
    OUTPUT.mkdir(parents=True, exist_ok=True)
    near = load_near_events()
    _, segments = build_event_and_segment_tables()
    outcomes = ["south_to_north", "north_to_south", "total_flow", "favored_minus_unfavored"]
    results: list[dict[str, object]] = []
    panels: list[pd.DataFrame] = []
    for bin_minutes in (1, 5):
        panel = flow_panel(near, segments, bin_minutes)
        for event in EVENTS:
            for bandwidth in (30, 45, 60):
                sample = event_panel(panel, event, bandwidth)
                sample["bin_minutes"] = bin_minutes
                panels.append(sample)
                for outcome in outcomes:
                    results.append(
                        {
                            "event": event["event"],
                            "favored_after": event["favored_after"],
                            "bin_minutes": bin_minutes,
                            "bandwidth_min": bandwidth,
                            "outcome": outcome,
                            **fit_local(sample, outcome),
                        }
                    )
    result = pd.DataFrame(results)
    result.to_csv(OUTPUT / "outbound_rdit_estimates.csv", index=False)
    pd.concat(panels, ignore_index=True).to_csv(OUTPUT / "outbound_rdit_analysis_panel.csv", index=False)
    core = result[
        result["outcome"].eq("favored_minus_unfavored")
        & result["bin_minutes"].eq(1)
    ]
    print(core.to_string(index=False))


if __name__ == "__main__":
    main()
