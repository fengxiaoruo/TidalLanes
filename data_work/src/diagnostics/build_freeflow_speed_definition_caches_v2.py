"""Build versioned alternative free-flow speed caches (v2: selectable definitions, chunked read).

Copy of ``build_freeflow_speed_definition_caches.py`` (2026-08-04) with two changes:

* ``--definitions`` selects which named windows to build, so a new output root can hold
  a subset without rebuilding the v1 caches.
* The raw speed file is read in chunks; harmonic definitions accumulate (count, sum 1/v)
  and quantile definitions keep the windowed values only, so peak memory stays small.

Output schema is identical to v1: ``definitions/<name>/`` holds the four road-segment
speed CSVs (only ``raw_connected_speed_ff_by_roadseg.csv`` differs from the baseline
cache) plus ``definition.json``; ``metrics/roadseg_definition_comparison.csv`` compares
each definition with the retained 22:00--05:00 all-day harmonic cache.

Hour convention (same as v1): ``hour_of_day`` is the clock hour of the observation
stamp, so ``h.ge(2) & h.le(4)`` keeps stamps from 02:00 through 04:59.
"""

from __future__ import annotations

import argparse
import json
import shutil
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd


# name -> (hour predicate on hour_of_day Series, weekday filter, method)
# weekday filter: None = all days, True = Mon-Fri only, False = Sat/Sun only
DEFINITIONS = {
    "ff_0005_all_harmonic": (lambda h: h.le(5), None, "harmonic"),
    "ff_0104_all_harmonic": (lambda h: h.ge(1) & h.le(4), None, "harmonic"),
    "ff_0205_all_harmonic": (lambda h: h.ge(2) & h.le(4), None, "harmonic"),
    "ff_0205_all_p90": (lambda h: h.ge(2) & h.le(4), None, "p90"),
    "ff_2205_weekday_harmonic": (lambda h: h.ge(22) | h.le(5), True, "harmonic"),
    "ff_2205_weekend_harmonic": (lambda h: h.ge(22) | h.le(5), False, "harmonic"),
    "ff_2205_all_p90": (lambda h: h.ge(22) | h.le(5), None, "p90"),
}

HOUR_LABEL = {
    "ff_0005_all_harmonic": "00:00-05:59 all days",
    "ff_0104_all_harmonic": "01:00-04:59 all days",
    "ff_0205_all_harmonic": "02:00-04:59 all days (hour_of_day in {2,3,4})",
    "ff_0205_all_p90": "02:00-04:59 all days (hour_of_day in {2,3,4})",
    "ff_2205_weekday_harmonic": "22:00-05:59 Mon-Fri",
    "ff_2205_weekend_harmonic": "22:00-05:59 Sat-Sun",
    "ff_2205_all_p90": "22:00-05:59 all days",
}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--speed-path", default="data_work/raw_data/speed_Beijing_all_wgs84.csv")
    p.add_argument(
        "--baseline-cache",
        default="data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/data",
    )
    p.add_argument("--output-root", default="data_work/outputs/freeflow_speed_definitions_v2")
    p.add_argument(
        "--definitions",
        default="ff_0205_all_harmonic,ff_0205_all_p90",
        help="Comma-separated definition names; see DEFINITIONS in this file.",
    )
    p.add_argument("--chunksize", type=int, default=2_000_000)
    args = p.parse_args()
    names = [n.strip() for n in args.definitions.split(",") if n.strip()]
    unknown = [n for n in names if n not in DEFINITIONS]
    if unknown:
        raise SystemExit(f"Unknown definitions: {unknown}")
    baseline, output = Path(args.baseline_cache), Path(args.output_root)
    if output.exists():
        raise SystemExit(f"Refusing to overwrite existing output: {output}")
    (output / "definitions").mkdir(parents=True)
    (output / "metrics").mkdir()
    (output / "docs").mkdir()

    # accumulators
    harm_count: dict[str, defaultdict] = {n: defaultdict(float) for n in names}
    harm_invsum: dict[str, defaultdict] = {n: defaultdict(float) for n in names}
    quant_vals: dict[str, defaultdict] = {n: defaultdict(list) for n in names}
    day_counts: defaultdict = defaultdict(int)
    n_rows_total = 0
    n_rows_valid = 0

    reader = pd.read_csv(
        args.speed_path, usecols=["roadseg_id", "speed", "hour"], chunksize=args.chunksize,
    )
    for k, d in enumerate(reader, start=1):
        n_rows_total += len(d)
        d["speed"] = pd.to_numeric(d.speed, errors="coerce")
        stamp = pd.to_numeric(d.hour, errors="coerce").round().astype("Int64")
        dt = pd.to_datetime(stamp.astype(str), format="%Y%m%d%H%M", errors="coerce")
        d["hour_of_day"] = dt.dt.hour
        d["weekday"] = dt.dt.weekday < 5
        d["date"] = dt.dt.strftime("%Y-%m-%d")
        d = d.loc[d.roadseg_id.notna() & d.speed.gt(0) & dt.notna()].copy()
        n_rows_valid += len(d)
        for date, cnt in d.groupby("date").size().items():
            day_counts[date] += int(cnt)
        h = d.hour_of_day
        for name in names:
            hour_pred, wk, method = DEFINITIONS[name]
            mask = hour_pred(h)
            if wk is True:
                mask = mask & d.weekday
            elif wk is False:
                mask = mask & ~d.weekday
            sub = d.loc[mask, ["roadseg_id", "speed"]]
            if sub.empty:
                continue
            if method == "harmonic":
                sub = sub.assign(inv=1.0 / sub.speed.to_numpy(float))
                g = sub.groupby("roadseg_id")
                cnt = g.speed.size()
                inv = g.inv.sum()
                for rid, c in cnt.items():
                    harm_count[name][str(rid)] += float(c)
                for rid, v in inv.items():
                    harm_invsum[name][str(rid)] += float(v)
            else:
                for rid, s in sub.groupby("roadseg_id").speed:
                    quant_vals[name][str(rid)].extend(s.to_numpy(float).tolist())
        if k % 5 == 0:
            print(f"[ff-cache-v2] processed chunks={k} rows={n_rows_total:,}", flush=True)

    base_ff = pd.read_csv(baseline / "raw_connected_speed_ff_by_roadseg.csv")
    base_ff.roadseg_id = base_ff.roadseg_id.astype(str)
    comparison_rows = []
    for name in names:
        _, wk, method = DEFINITIONS[name]
        if method == "harmonic":
            keys = sorted(harm_count[name])
            values = np.array([harm_count[name][r] / harm_invsum[name][r] for r in keys], float)
            counts = np.array([harm_count[name][r] for r in keys], int)
        else:
            keys = sorted(quant_vals[name])
            values = np.array([np.quantile(quant_vals[name][r], 0.90) for r in keys], float)
            counts = np.array([len(quant_vals[name][r]) for r in keys], int)
        ff = pd.DataFrame({
            "roadseg_id": keys,
            "speed_ff_kmh": values,
            "n_obs_speed_ff_kmh": counts,
        })
        target = output / "definitions" / name
        target.mkdir()
        for file in [
            "raw_connected_speed_am_by_roadseg.csv",
            "raw_connected_speed_pm_by_roadseg.csv",
            "raw_connected_speed_all_by_roadseg.csv",
        ]:
            shutil.copy2(baseline / file, target / file)
        ff.to_csv(target / "raw_connected_speed_ff_by_roadseg.csv", index=False)
        merged = base_ff.merge(ff, on="roadseg_id", how="outer", suffixes=("_base", "_alt"))
        common = merged.speed_ff_kmh_base.notna() & merged.speed_ff_kmh_alt.notna()
        ratio = merged.loc[common, "speed_ff_kmh_alt"] / merged.loc[common, "speed_ff_kmh_base"]
        comparison_rows.append({
            "definition": name,
            "method": method,
            "hours": HOUR_LABEL[name],
            "roadsegs": int(len(ff)),
            "n_obs_p50": float(np.median(counts)) if len(counts) else np.nan,
            "n_obs_min": int(counts.min()) if len(counts) else 0,
            "n_obs_max": int(counts.max()) if len(counts) else 0,
            "common_with_baseline": int(common.sum()),
            "coverage_vs_baseline": float(common.sum() / len(base_ff)),
            "speed_ratio_p10": float(ratio.quantile(.10)),
            "speed_ratio_p50": float(ratio.quantile(.50)),
            "speed_ratio_p90": float(ratio.quantile(.90)),
        })
        (target / "definition.json").write_text(
            json.dumps(
                {
                    "name": name,
                    "method": method,
                    "hours": HOUR_LABEL[name],
                    "weekday_filter": wk,
                    "speed_path": args.speed_path,
                    "baseline_cache": str(baseline),
                },
                indent=2,
            ),
            encoding="utf-8",
        )

    comparison = pd.DataFrame(comparison_rows)
    comparison.to_csv(output / "metrics" / "roadseg_definition_comparison.csv", index=False)
    pd.DataFrame(
        sorted(day_counts.items()), columns=["date", "n_valid_obs"]
    ).to_csv(output / "metrics" / "speed_obs_by_date.csv", index=False)
    (output / "metrics" / "row_counts.json").write_text(
        json.dumps({"rows_total": n_rows_total, "rows_valid": n_rows_valid}, indent=2),
        encoding="utf-8",
    )
    (output / "README.md").write_text(
        f"""# Alternative free-flow speed definitions (v2)

Definitions built here: {", ".join(names)}.

The retained edge-projection cache remains unchanged. Each subdirectory contains the same AM, PM, and all-period tables as the retained run and a version-specific free-flow table. These caches are inputs for separate robustness reruns, not replacements for the active gate5 bundle. The v1 root `freeflow_speed_definitions_v1` is not modified.

Hour convention: `hour_of_day` is the clock hour of the observation stamp. `ff_0205_*` keeps stamps with `hour_of_day` in {{2, 3, 4}}, i.e. 02:00 through 04:59, on all days (weekdays and weekends). The retained baseline is 22:00 through 05:59 on all days, harmonic mean.

Build command:

```
python3 data_work/src/diagnostics/build_freeflow_speed_definition_caches_v2.py --definitions {args.definitions} --output-root {args.output_root}
```
""",
        encoding="utf-8",
    )
    (output / "docs" / "sample-lineage.md").write_text(
        f"""# Sample lineage

Raw speed observations are read from `{args.speed_path}` in chunks. The retained AM, PM, and all-period caches are copied from `{baseline}`. Only `raw_connected_speed_ff_by_roadseg.csv` varies across definitions. Invalid datetimes and nonpositive speeds are excluded. `metrics/speed_obs_by_date.csv` lists valid observation counts by calendar date.
""",
        encoding="utf-8",
    )
    (output / "docs" / "variable-dictionary.md").write_text(
        """# Variable dictionary

- `speed_ff_kmh`: road-segment free-flow speed under the named window and aggregation rule.
- `n_obs_speed_ff_kmh`: number of valid road-segment observations entering the definition.
- `n_obs_p50`: median observation count per road segment.
- `coverage_vs_baseline`: share of retained baseline road segments with an alternative estimate.
- `speed_ratio_p*`: alternative speed divided by the retained 22:00--05:59 harmonic speed.
""",
        encoding="utf-8",
    )
    shutil.copy2(__file__, output / "docs" / Path(__file__).name)
    print(comparison.to_string(index=False))


if __name__ == "__main__":
    main()
