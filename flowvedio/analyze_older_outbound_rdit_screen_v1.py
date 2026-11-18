#!/usr/bin/env python3
"""Screen and re-estimate older outbound video windows using the retained RDiT baseline."""

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import statsmodels.api as sm

ROOT = Path(__file__).resolve().parent
SOURCE = ROOT / "batch_outputs/20260710_caitian_identification_final"
OUT = SOURCE / "older_outbound_rdit_same_standard_v1"
OUT.mkdir(parents=True, exist_ok=True)
(OUT / "figures").mkdir(exist_ok=True)

EVENTS = {
    "20260710_opening": {
        "cutoff": pd.Timestamp("2026-07-10 16:30:59"),
        "pre": "normal",
        "post": "tidal_deployed",
    },
    "20260710_recovery": {
        "cutoff": pd.Timestamp("2026-07-10 21:45:47"),
        "pre": "tidal_deployed",
        "post": "normal",
    },
}
OUTCOMES = ["northbound_flow", "southbound_flow", "bidirectional_flow", "north_minus_south"]
BANDWIDTHS = [10, 15, 20, 30, 37, 45, 60]


def minute_coverage(plan: pd.DataFrame) -> pd.DataFrame:
    rows = []
    stable = plan[(plan.group == "outbound") & (plan.counting_status == "count")].copy()
    for _, r in stable.iterrows():
        start, end = pd.Timestamp(r.start_local), pd.Timestamp(r.end_local)
        for minute in pd.date_range(start.floor("min"), end.floor("min"), freq="min"):
            left, right = minute, minute + pd.Timedelta(minutes=1)
            overlap = max(0.0, (min(end, right) - max(start, left)).total_seconds())
            if overlap:
                rows.append({"minute": minute, "state": r.state, "seconds": overlap})
    cov = pd.DataFrame(rows).groupby(["minute", "state"], as_index=False).seconds.sum()
    cov["coverage"] = cov.seconds.clip(upper=60) / 60
    return cov


def build_panel() -> pd.DataFrame:
    plan = pd.read_csv(SOURCE / "run_plan.csv", parse_dates=["start_local", "end_local"])
    cov = minute_coverage(plan)
    cov = cov[cov.coverage >= 0.9].copy()
    counts = pd.read_csv(SOURCE / "counts_by_wall_minute.csv", parse_dates=["wall_minute_local"])
    counts = counts[(counts.group == "outbound") & (counts.counting_status == "count")].copy()
    counts["geo_direction"] = np.where(
        counts.line_name.str.startswith("south_to_north"), "northbound", "southbound"
    )
    wide = counts.groupby(["wall_minute_local", "state", "geo_direction"])["count"].sum().unstack(fill_value=0)
    wide = wide.reset_index().rename(columns={"wall_minute_local": "minute"})
    panel = cov.merge(wide, on=["minute", "state"], how="left")
    panel["northbound"] = panel.get("northbound", 0).fillna(0)
    panel["southbound"] = panel.get("southbound", 0).fillna(0)
    panel["northbound_flow"] = panel.northbound / panel.coverage
    panel["southbound_flow"] = panel.southbound / panel.coverage
    panel["bidirectional_flow"] = panel.northbound_flow + panel.southbound_flow
    panel["north_minus_south"] = panel.northbound_flow - panel.southbound_flow
    return panel


def fit(data: pd.DataFrame, outcome: str, bandwidth: int) -> dict:
    d = data[np.abs(data.running) <= bandwidth].copy()
    npre, npost = int((d.post == 0).sum()), int((d.post == 1).sum())
    base = {"outcome": outcome, "bandwidth_min": bandwidth, "n": len(d), "n_pre": npre, "n_post": npost}
    if min(npre, npost) < 8:
        return {**base, "tau": np.nan, "se": np.nan, "p": np.nan}
    x = pd.DataFrame({"const": 1.0, "post": d.post, "running": d.running,
                      "post_running": d.post * d.running}, index=d.index)
    model = sm.OLS(d[outcome], x).fit(cov_type="HAC", cov_kwds={"maxlags": 5})
    return {**base, "tau": model.params.post, "se": model.bse.post, "p": model.pvalues.post,
            "pre_slope": model.params.running,
            "slope_change": model.params.post_running,
            "r2": model.rsquared}


def main() -> None:
    panel = build_panel()
    all_rows, supports = [], []
    for event, cfg in EVENTS.items():
        cutoff = cfg["cutoff"]
        d = panel[panel.state.isin([cfg["pre"], cfg["post"]])].copy()
        d["running"] = (d.minute + pd.Timedelta(seconds=30) - cutoff).dt.total_seconds() / 60
        d["post"] = (d.state == cfg["post"]).astype(int)
        d = d[((d.running < 0) & (d.state == cfg["pre"])) | ((d.running >= 0) & (d.state == cfg["post"]))]
        in60 = d[np.abs(d.running) <= 60]
        supports.append({
            "event": event, "cutoff": cutoff, "n_pre_60": int((in60.post == 0).sum()),
            "n_post_60": int((in60.post == 1).sum()),
            "nearest_pre_min": in60.loc[in60.post == 0, "running"].max() if (in60.post == 0).any() else np.nan,
            "nearest_post_min": in60.loc[in60.post == 1, "running"].min() if (in60.post == 1).any() else np.nan,
        })
        for outcome in OUTCOMES:
            for bandwidth in BANDWIDTHS:
                all_rows.append({"event": event, "cutoff": cutoff, **fit(d, outcome, bandwidth)})

        fig, axes = plt.subplots(2, 2, figsize=(10, 6.5), sharex=True)
        for ax, outcome in zip(axes.flat, OUTCOMES):
            z = d[np.abs(d.running) <= 60]
            ax.scatter(z.running, z[outcome], s=16, color="#377eb8", alpha=.75)
            ax.axvline(0, color="black", linestyle="--", linewidth=1)
            ax.axhline(0, color="0.6", linewidth=.6)
            ax.set_title(outcome.replace("_", " "))
        fig.suptitle(f"{event}: one-minute stable-state observations")
        fig.supxlabel("Minutes from stable completion")
        fig.tight_layout()
        for ext in ["png", "pdf", "svg"]:
            fig.savefig(OUT / "figures" / f"{event}_event_study.{ext}", dpi=220, bbox_inches="tight")
        plt.close(fig)

    pd.DataFrame(all_rows).to_csv(OUT / "rdit_estimates_all_bandwidths.csv", index=False)
    pd.DataFrame(supports).to_csv(OUT / "event_window_availability.csv", index=False)
    panel.to_csv(OUT / "minute_panel.csv", index=False)


if __name__ == "__main__":
    main()
