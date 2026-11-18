"""Build versioned alternative free-flow speed caches for edge-projection reruns."""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

import numpy as np
import pandas as pd


def harmonic(series):
    x = pd.to_numeric(series, errors="coerce").to_numpy(float)
    x = x[np.isfinite(x) & (x > 0)]
    return float(len(x) / np.sum(1 / x)) if len(x) else np.nan


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--speed-path", default="data_work/raw_data/speed_Beijing_all_wgs84.csv")
    p.add_argument(
        "--baseline-cache",
        default="data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/data",
    )
    p.add_argument(
        "--output-root",
        default="data_work/outputs/freeflow_speed_definitions_v1",
    )
    args = p.parse_args()
    baseline, output = Path(args.baseline_cache), Path(args.output_root)
    if output.exists():
        raise SystemExit(f"Refusing to overwrite existing output: {output}")
    (output / "definitions").mkdir(parents=True)
    (output / "metrics").mkdir()
    (output / "docs").mkdir()

    d = pd.read_csv(args.speed_path, usecols=["roadseg_id", "speed", "hour"])
    d["speed"] = pd.to_numeric(d.speed, errors="coerce")
    stamp = pd.to_numeric(d.hour, errors="coerce").round().astype("Int64")
    dt = pd.to_datetime(stamp.astype(str), format="%Y%m%d%H%M", errors="coerce")
    d["hour_of_day"] = dt.dt.hour
    d["weekday"] = dt.dt.weekday < 5
    d = d.loc[d.roadseg_id.notna() & d.speed.gt(0) & dt.notna()].copy()
    h = d.hour_of_day
    night_2205 = h.ge(22) | h.le(5)
    definitions = {
        "ff_0005_all_harmonic": (h.le(5), "harmonic"),
        "ff_0104_all_harmonic": (h.ge(1) & h.le(4), "harmonic"),
        "ff_2205_weekday_harmonic": (night_2205 & d.weekday, "harmonic"),
        "ff_2205_weekend_harmonic": (night_2205 & ~d.weekday, "harmonic"),
        "ff_2205_all_p90": (night_2205, "p90"),
    }
    base_ff = pd.read_csv(baseline / "raw_connected_speed_ff_by_roadseg.csv")
    base_ff.roadseg_id = base_ff.roadseg_id.astype(str)
    comparison_rows = []
    for name, (mask, method) in definitions.items():
        subset = d.loc[mask, ["roadseg_id", "speed"]].copy()
        grouped = subset.groupby("roadseg_id").speed
        values = grouped.apply(harmonic) if method == "harmonic" else grouped.quantile(0.90)
        counts = grouped.size()
        ff = pd.DataFrame({
            "roadseg_id": values.index.astype(str),
            "speed_ff_kmh": values.to_numpy(float),
            "n_obs_speed_ff_kmh": counts.reindex(values.index).to_numpy(int),
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
            "roadsegs": int(len(ff)),
            "common_with_baseline": int(common.sum()),
            "coverage_vs_baseline": float(common.sum() / len(base_ff)),
            "speed_ratio_p10": float(ratio.quantile(.10)),
            "speed_ratio_p50": float(ratio.quantile(.50)),
            "speed_ratio_p90": float(ratio.quantile(.90)),
        })
        (target / "definition.json").write_text(json.dumps({"name": name, "method": method}, indent=2), encoding="utf-8")

    comparison = pd.DataFrame(comparison_rows)
    comparison.to_csv(output / "metrics" / "roadseg_definition_comparison.csv", index=False)
    (output / "README.md").write_text(
        """# Alternative free-flow speed definitions

The retained edge-projection cache remains unchanged. Each subdirectory contains the same AM, PM, and all-period tables as the retained run and a version-specific free-flow table. These caches are inputs for separate robustness reruns, not replacements for the active gate5 bundle.
""",
        encoding="utf-8",
    )
    (output / "docs" / "sample-lineage.md").write_text(
        f"""# Sample lineage

Raw speed observations are read from `{args.speed_path}`. The retained AM, PM, and all-period caches are copied from `{baseline}`. Only `raw_connected_speed_ff_by_roadseg.csv` varies across definitions. Invalid datetimes and nonpositive speeds are excluded.
""",
        encoding="utf-8",
    )
    (output / "docs" / "variable-dictionary.md").write_text(
        """# Variable dictionary

- `speed_ff_kmh`: road-segment free-flow speed under the named window and aggregation rule.
- `n_obs_speed_ff_kmh`: number of valid road-segment observations entering the definition.
- `coverage_vs_baseline`: share of retained baseline road segments with an alternative estimate.
- `speed_ratio_p*`: alternative speed divided by the retained 22:00--05:00 harmonic speed.
""",
        encoding="utf-8",
    )
    shutil.copy2(__file__, output / "docs" / Path(__file__).name)
    print(comparison.to_string(index=False))


if __name__ == "__main__":
    main()
