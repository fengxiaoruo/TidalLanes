"""Build AM observed-speed caches from a day split of the raw speed file.

Replicates the AM aggregation in
``src/raw_connected_v1/grid_travel_time/build_raw_connected_v1_grid_travel_time.py``
(``parse_speed_datetime`` + ``aggregate_speed_by_segment``):

* stamp = round(hour) parsed as ``%Y%m%d%H%M``; rows with invalid stamps dropped
* AM peak = weekday (Mon-Fri) and ``hour_of_day`` in {7, 8}, i.e. 07:00 through 08:59
* speed > 0 and non-missing roadseg_id
* harmonic mean by roadseg_id: n / sum(1/speed); ``n_obs_speed_am_kmh`` = n

The only change is a calendar-date filter per cache (``--split`` entries). The PM,
free-flow, and all-period tables are copied unchanged from the retained baseline cache so
that only ``raw_connected_speed_am_by_roadseg.csv`` differs.

A pooled harmonic mean over all split dates is written to ``metrics`` and compared with
the retained AM cache as a replication check.
"""

from __future__ import annotations

import argparse
import json
import shutil
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd


def parse_splits(text: str) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for item in text.split(";"):
        item = item.strip()
        if not item:
            continue
        name, dates = item.split("=")
        out[name.strip()] = [d.strip() for d in dates.split(",") if d.strip()]
    return out


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--speed-path", default="data_work/raw_data/speed_Beijing_all_wgs84.csv")
    p.add_argument(
        "--baseline-cache",
        default="data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/data",
    )
    p.add_argument("--output-root", default="data_work/outputs/am_day_split_speed_caches_v1")
    p.add_argument(
        "--split",
        default="am_odd_days=2025-05-19,2025-05-21,2025-05-23;am_even_days=2025-05-20,2025-05-22",
        help="name=date,date;name=date,... (ISO dates)",
    )
    p.add_argument("--chunksize", type=int, default=2_000_000)
    args = p.parse_args()
    splits = parse_splits(args.split)
    baseline, output = Path(args.baseline_cache), Path(args.output_root)
    if output.exists():
        raise SystemExit(f"Refusing to overwrite existing output: {output}")
    (output / "definitions").mkdir(parents=True)
    (output / "metrics").mkdir()
    (output / "docs").mkdir()

    count: dict[str, defaultdict] = {n: defaultdict(float) for n in splits}
    invsum: dict[str, defaultdict] = {n: defaultdict(float) for n in splits}
    count_all: defaultdict = defaultdict(float)
    invsum_all: defaultdict = defaultdict(float)
    am_by_date: defaultdict = defaultdict(int)
    n_rows_total = 0

    reader = pd.read_csv(
        args.speed_path, usecols=["roadseg_id", "speed", "day", "hour"], chunksize=args.chunksize,
    )
    for k, d in enumerate(reader, start=1):
        n_rows_total += len(d)
        d["speed"] = pd.to_numeric(d.speed, errors="coerce")
        stamp = pd.to_numeric(d.hour, errors="coerce").round().astype("Int64")
        dt = pd.to_datetime(stamp.astype(str), format="%Y%m%d%H%M", errors="coerce")
        hod = dt.dt.hour
        weekday = (dt.dt.weekday < 5).fillna(False)
        am = weekday & (hod >= 7) & (hod < 9)
        keep = d.roadseg_id.notna() & d.speed.gt(0) & dt.notna() & am.fillna(False)
        d = d.loc[keep, ["roadseg_id", "speed"]].copy()
        d["date"] = dt.loc[keep].dt.strftime("%Y-%m-%d")
        d["inv"] = 1.0 / d.speed.to_numpy(float)
        d["roadseg_id"] = d.roadseg_id.astype(str)
        for date, cnt in d.groupby("date").size().items():
            am_by_date[date] += int(cnt)
        g = d.groupby("roadseg_id")
        for rid, c in g.speed.size().items():
            count_all[rid] += float(c)
        for rid, v in g.inv.sum().items():
            invsum_all[rid] += float(v)
        for name, dates in splits.items():
            sub = d.loc[d.date.isin(dates)]
            if sub.empty:
                continue
            g = sub.groupby("roadseg_id")
            for rid, c in g.speed.size().items():
                count[name][rid] += float(c)
            for rid, v in g.inv.sum().items():
                invsum[name][rid] += float(v)
        if k % 5 == 0:
            print(f"[am-split] processed chunks={k} rows={n_rows_total:,}", flush=True)

    def to_frame(cnt, inv):
        keys = sorted(cnt)
        return pd.DataFrame({
            "roadseg_id": keys,
            "speed_am_kmh": [cnt[r] / inv[r] for r in keys],
            "n_obs_speed_am_kmh": [int(cnt[r]) for r in keys],
        })

    base_am = pd.read_csv(baseline / "raw_connected_speed_am_by_roadseg.csv")
    base_am.roadseg_id = base_am.roadseg_id.astype(str)
    rows = []
    for name, dates in splits.items():
        am_df = to_frame(count[name], invsum[name])
        target = output / "definitions" / name
        target.mkdir()
        for file in [
            "raw_connected_speed_pm_by_roadseg.csv",
            "raw_connected_speed_all_by_roadseg.csv",
            "raw_connected_speed_ff_by_roadseg.csv",
        ]:
            shutil.copy2(baseline / file, target / file)
        am_df.to_csv(target / "raw_connected_speed_am_by_roadseg.csv", index=False)
        merged = base_am.merge(am_df, on="roadseg_id", how="outer", suffixes=("_base", "_alt"))
        common = merged.speed_am_kmh_base.notna() & merged.speed_am_kmh_alt.notna()
        ratio = merged.loc[common, "speed_am_kmh_alt"] / merged.loc[common, "speed_am_kmh_base"]
        rows.append({
            "cache": name,
            "dates": ",".join(dates),
            "roadsegs": int(len(am_df)),
            "n_obs_p50": float(am_df.n_obs_speed_am_kmh.median()),
            "common_with_baseline": int(common.sum()),
            "coverage_vs_baseline": float(common.sum() / len(base_am)),
            "speed_ratio_p10": float(ratio.quantile(.10)),
            "speed_ratio_p50": float(ratio.quantile(.50)),
            "speed_ratio_p90": float(ratio.quantile(.90)),
            "abs_log_ratio_p50": float(np.abs(np.log(ratio)).median()),
        })
        (target / "definition.json").write_text(
            json.dumps({
                "name": name,
                "dates": dates,
                "window": "weekday 07:00-08:59 (hour_of_day in {7,8})",
                "aggregation": "harmonic mean by roadseg_id",
                "speed_path": args.speed_path,
                "baseline_cache": str(baseline),
            }, indent=2),
            encoding="utf-8",
        )

    pooled = to_frame(count_all, invsum_all)
    pooled.to_csv(output / "metrics" / "am_pooled_all_weekdays_by_roadseg.csv", index=False)
    chk = base_am.merge(pooled, on="roadseg_id", how="outer", suffixes=("_base", "_pooled"))
    both = chk.speed_am_kmh_base.notna() & chk.speed_am_kmh_pooled.notna()
    rel = (chk.loc[both, "speed_am_kmh_pooled"] / chk.loc[both, "speed_am_kmh_base"] - 1).abs()
    n_match = int((chk.loc[both, "n_obs_speed_am_kmh_base"] == chk.loc[both, "n_obs_speed_am_kmh_pooled"]).sum())
    replication = {
        "baseline_roadsegs": int(len(base_am)),
        "pooled_roadsegs": int(len(pooled)),
        "common": int(both.sum()),
        "max_abs_rel_speed_diff": float(rel.max()),
        "n_obs_equal_count": n_match,
        "am_obs_by_date": dict(sorted(am_by_date.items())),
        "rows_total": n_rows_total,
    }
    (output / "metrics" / "replication_check_vs_retained_am.json").write_text(
        json.dumps(replication, indent=2), encoding="utf-8",
    )
    comparison = pd.DataFrame(rows)
    comparison.to_csv(output / "metrics" / "am_split_comparison.csv", index=False)
    (output / "README.md").write_text(
        f"""# AM observed-speed caches from a weekday split

Caches: {", ".join(splits)}. Each `definitions/<name>/` holds the four road-segment speed tables; only `raw_connected_speed_am_by_roadseg.csv` differs from the retained cache `{baseline}`. Free-flow, PM, and all-period tables are copied unchanged.

Aggregation replicates the retained AM rule exactly: weekday stamps with `hour_of_day` in {{7, 8}} (07:00 through 08:59), speed > 0, harmonic mean by `roadseg_id`. The only change is the calendar-date filter. `metrics/replication_check_vs_retained_am.json` compares the pooled all-weekday harmonic mean with the retained AM cache.

Build command:

```
python3 data_work/src/diagnostics/build_am_day_split_speed_caches.py --split "{args.split}" --output-root {args.output_root}
```
""",
        encoding="utf-8",
    )
    shutil.copy2(__file__, output / "docs" / Path(__file__).name)
    print(comparison.to_string(index=False))
    print(json.dumps({k: v for k, v in replication.items() if k != "am_obs_by_date"}, indent=2))
    print("AM obs by date:", dict(sorted(am_by_date.items())))


if __name__ == "__main__":
    main()
