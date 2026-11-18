"""
Trip-purpose shares within the R population (platform query 0112)
=================================================================

Reads Platform/runs/202505_v01/query-results/TL25M05_V01_0112_purpose_share_r.csv:
strict-valid trips on 19 weekdays by departure 10-min slot x mode group x
purpose x in_r x in_joint (cells with >= 5 distinct SIMs). Purpose comes from the
start/end stay types of each trip (HW, WH, home_other, work_other, other, unknown).
Writes shares by departure window for R, joint-R, non-R and all users.

Output: data_work/outputs/<output-name>/ (refuses to overwrite)
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]   # -> data_work/
SRC = ROOT.parent / "Platform/runs/202505_v01/query-results/TL25M05_V01_0112_purpose_share_r.csv"
WINDOWS = {"AM_07_09": range(42, 54), "PM_17_19": range(102, 114), "all_day": range(0, 144)}


def shares(d: pd.DataFrame) -> dict:
    t = d.groupby("purpose").n_trips.sum()
    return {"trips_19d": int(t.sum()), **{k: round(float(v), 4) for k, v in (t / t.sum()).items()}}


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-name", default="purpose_share_r_202505_v1")
    args = ap.parse_args(argv)
    out = ROOT / "outputs" / args.output_name
    if out.exists():
        raise SystemExit(f"{out} exists; choose a new --output-name")
    out.mkdir(parents=True)
    d = pd.read_csv(SRC)
    groups = {"R": d.in_r == 1, "R_joint": (d.in_r == 1) & (d.in_joint == 1), "R_not_joint": (d.in_r == 1) & (d.in_joint == 0),
              "non_R": d.in_r == 0, "all_users": d.in_r >= 0}
    res = {}
    for mode, mm in [("mode1", d.mode_group == "mode1"), ("all_modes", d.mode_group.notna())]:
        for w, slots in WINDOWS.items():
            for g, gm in groups.items():
                res.setdefault(mode, {}).setdefault(w, {})[g] = shares(d[mm & gm & d.slot10.isin(slots)])
            s = d[mm & d.slot10.isin(slots)]
            res[mode][w]["R_share_of_all_trips"] = round(float(s.n_trips[s.in_r == 1].sum() / s.n_trips.sum()), 4)
    prof = d[(d.mode_group == "mode1") & (d.in_r == 1)].groupby(["slot10", "purpose"]).n_trips.sum().unstack("purpose").fillna(0)
    prof.div(prof.sum(axis=1), axis=0).to_csv(out / "R_mode1_purpose_share_by_slot10.csv")
    res["note"] = ("departure-time windows; strict-valid trips; R = 19-59, core, home and work stays identified; "
                   "home_other = one end home and the other neither home nor work, either direction; chained commutes "
                   "(home -> stop -> work) appear as home_other + work_other and are not separable here")
    (out / "purpose_share_summary.json").write_text(json.dumps(res, indent=2))
    print(json.dumps({k: res["mode1"][k] for k in ["AM_07_09", "PM_17_19"]}, indent=1))


if __name__ == "__main__":
    main()
