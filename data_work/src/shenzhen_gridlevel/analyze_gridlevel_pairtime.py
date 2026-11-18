"""Audit Shenzhen grid-level bidirectional travel-time data.

This script reads the raw 1000 m fishnet pair-time matrix, computes
directionality and detour diagnostics, and compares its identifiers and
measurement level with existing Shenzhen commuting and road-segment data.
It never modifies raw data.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


EPS = 1e-12


def write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False, default=float))


def summarize_series(s: pd.Series, name: str) -> dict:
    qs = s.quantile([0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99])
    out = {
        "variable": name,
        "count": int(s.count()),
        "mean": float(s.mean()),
        "std": float(s.std()),
        "min": float(s.min()),
        "max": float(s.max()),
    }
    out.update({f"p{int(q*100):02d}": float(v) for q, v in qs.items()})
    return out


def build_reverse_pair_frame(df: pd.DataFrame, value_cols: list[str]) -> pd.DataFrame:
    left = df[["fid_home", "fid_work", *value_cols]].copy()
    right = df[["fid_home", "fid_work", *value_cols]].copy()
    right = right.rename(columns={"fid_home": "fid_work", "fid_work": "fid_home"})
    right = right.rename(columns={c: f"{c}_reverse" for c in value_cols})
    out = left.merge(right, on=["fid_home", "fid_work"], how="left", validate="one_to_one")
    return out


def main(argv=None) -> None:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--input",
        default="data_work/raw_data/shenzhen_gridlevel/fishnet1000_pairtime_hat.csv",
    )
    p.add_argument(
        "--output-root",
        default="data_work/outputs/shenzhen_gridlevel_pairtime_v1",
    )
    p.add_argument(
        "--flow-dta",
        default="data_work/raw_data/resid_work_flow_grid500.dta",
    )
    p.add_argument(
        "--grid-map-dta",
        default="data_work/raw_data/commuting_grid_fishnet500.dta",
    )
    p.add_argument(
        "--speed-csv",
        default="data_work/raw_data/speed_shenzhen_all_wgs84.csv",
    )
    args = p.parse_args(argv)

    out = Path(args.output_root)
    (out / "metrics").mkdir(parents=True, exist_ok=True)
    (out / "data").mkdir(parents=True, exist_ok=True)
    (out / "figures").mkdir(parents=True, exist_ok=True)

    raw_path = Path(args.input)
    df = pd.read_csv(raw_path)
    expected_cols = [
        "fid_work",
        "fid_home",
        "car_time_detour",
        "car_dist_detour",
        "car_dist",
        "car_time",
        "time_diff",
        "dist_diff",
    ]
    missing_cols = [c for c in expected_cols if c not in df.columns]
    if missing_cols:
        raise ValueError(f"missing expected columns: {missing_cols}")

    ids = sorted(set(df["fid_home"].unique()) | set(df["fid_work"].unique()))
    id_set = set(ids)
    nonself = df[df["fid_home"] != df["fid_work"]].copy()

    structure = {
        "raw_path": str(raw_path),
        "file_size_mb": raw_path.stat().st_size / (1024 ** 2),
        "n_rows": int(len(df)),
        "n_columns": int(df.shape[1]),
        "columns": list(df.columns),
        "n_unique_fid_home": int(df["fid_home"].nunique()),
        "n_unique_fid_work": int(df["fid_work"].nunique()),
        "n_unique_any_fid": int(len(ids)),
        "fid_min": int(min(ids)),
        "fid_max": int(max(ids)),
        "n_missing_ids_within_min_max": int(len(set(range(min(ids), max(ids) + 1)) - id_set)),
        "is_full_cartesian_over_present_ids": bool(len(df) == len(ids) ** 2),
        "n_expected_cartesian_rows": int(len(ids) ** 2),
        "n_duplicate_home_work_pairs": int(df.duplicated(["fid_home", "fid_work"]).sum()),
        "n_self_pairs": int((df["fid_home"] == df["fid_work"]).sum()),
        "n_missing_values": {c: int(df[c].isna().sum()) for c in df.columns},
    }
    write_json(out / "metrics" / "structure_summary.json", structure)

    variable_dictionary = pd.DataFrame([
        {
            "variable": "fid_home",
            "interpretation": "origin/home fishnet1000 grid id in this pair-time matrix",
            "notes": "Numeric id only; no geometry or coordinate crosswalk is included in the raw folder.",
        },
        {
            "variable": "fid_work",
            "interpretation": "destination/work fishnet1000 grid id in this pair-time matrix",
            "notes": "Together with fid_home forms a complete 1711 x 1711 OD matrix.",
        },
        {
            "variable": "car_time",
            "interpretation": "baseline car travel time",
            "notes": "Units inferred as minutes from implied speed distributions; no date or time-of-day field.",
        },
        {
            "variable": "car_dist",
            "interpretation": "baseline car travel distance",
            "notes": "Units inferred as kilometers from implied speed distributions.",
        },
        {
            "variable": "car_time_detour",
            "interpretation": "detour-adjusted car travel time",
            "notes": "Always weakly above car_time up to numerical tolerance in the current file.",
        },
        {
            "variable": "car_dist_detour",
            "interpretation": "detour-adjusted car travel distance",
            "notes": "Can be below car_dist for some pairs, so it is not a simple max-distance rule.",
        },
        {
            "variable": "time_diff",
            "interpretation": "car_time_detour minus car_time",
            "notes": "Matches computed difference up to float precision.",
        },
        {
            "variable": "dist_diff",
            "interpretation": "car_dist_detour minus car_dist",
            "notes": "Matches computed difference up to float precision, including small negative values.",
        },
    ])
    variable_dictionary.to_csv(out / "data" / "variable_dictionary.csv", index=False)

    # Consistency and travel-time/distance distributions.
    consistency = {
        "max_abs_time_diff_residual": float((df["time_diff"] - (df["car_time_detour"] - df["car_time"])).abs().max()),
        "max_abs_dist_diff_residual": float((df["dist_diff"] - (df["car_dist_detour"] - df["car_dist"])).abs().max()),
        "n_time_detour_below_base": int((df["car_time_detour"] + 1e-6 < df["car_time"]).sum()),
        "n_dist_detour_below_base": int((df["car_dist_detour"] + 1e-6 < df["car_dist"]).sum()),
        "share_nonself_time_diff_positive": float((nonself["time_diff"] > 1e-6).mean()),
        "share_nonself_dist_diff_positive": float((nonself["dist_diff"] > 1e-6).mean()),
        "share_nonself_dist_diff_negative": float((nonself["dist_diff"] < -1e-6).mean()),
    }
    write_json(out / "metrics" / "consistency_summary.json", consistency)

    nonself["speed_base_kmh"] = np.where(
        nonself["car_time"] > EPS,
        nonself["car_dist"] / nonself["car_time"] * 60.0,
        np.nan,
    )
    nonself["speed_detour_kmh"] = np.where(
        nonself["car_time_detour"] > EPS,
        nonself["car_dist_detour"] / nonself["car_time_detour"] * 60.0,
        np.nan,
    )
    summary_rows = []
    for c in [
        "car_time",
        "car_time_detour",
        "time_diff",
        "car_dist",
        "car_dist_detour",
        "dist_diff",
        "speed_base_kmh",
        "speed_detour_kmh",
    ]:
        summary_rows.append(summarize_series(nonself[c].replace([np.inf, -np.inf], np.nan).dropna(), c))
    pd.DataFrame(summary_rows).to_csv(out / "metrics" / "time_distance_summary.csv", index=False)

    # Directionality diagnostics.
    reverse = build_reverse_pair_frame(
        df,
        ["car_time", "car_time_detour", "car_dist", "car_dist_detour", "time_diff", "dist_diff"],
    )
    reverse["has_reverse"] = reverse["car_time_reverse"].notna()
    reciprocal = reverse[(reverse["fid_home"] < reverse["fid_work"]) & reverse["has_reverse"]].copy()
    for c in ["car_time", "car_time_detour", "car_dist", "car_dist_detour"]:
        a = reciprocal[c].to_numpy(float)
        b = reciprocal[f"{c}_reverse"].to_numpy(float)
        reciprocal[f"{c}_ratio"] = np.maximum(a, b) / np.clip(np.minimum(a, b), EPS, None)
        reciprocal[f"{c}_abs_log_ratio"] = np.abs(np.log(np.clip(a, EPS, None) / np.clip(b, EPS, None)))
        reciprocal[f"{c}_diff"] = a - b

    directionality_summary = {
        "all_rows_have_reverse": bool(reverse["has_reverse"].all()),
        "n_reciprocal_pairs": int(len(reciprocal)),
        "n_nonself_car_time_le_0": int((nonself["car_time"] <= EPS).sum()),
        "n_nonself_car_time_detour_le_0": int((nonself["car_time_detour"] <= EPS).sum()),
        "n_reciprocal_pairs_both_base_time_ge_1_min": int(
            ((reciprocal["car_time"] >= 1.0) & (reciprocal["car_time_reverse"] >= 1.0)).sum()
        ),
        "n_reciprocal_pairs_both_detour_time_ge_1_min": int(
            ((reciprocal["car_time_detour"] >= 1.0) & (reciprocal["car_time_detour_reverse"] >= 1.0)).sum()
        ),
    }
    for c in ["car_time", "car_time_detour", "car_dist", "car_dist_detour"]:
        if "time" in c:
            valid_ratio = (reciprocal[c] >= 1.0) & (reciprocal[f"{c}_reverse"] >= 1.0)
        else:
            valid_ratio = (reciprocal[c] > EPS) & (reciprocal[f"{c}_reverse"] > EPS)
        r = reciprocal.loc[valid_ratio, f"{c}_ratio"]
        directionality_summary.update({
            f"{c}_ratio_sample": int(r.shape[0]),
            f"{c}_ratio_mean": float(r.mean()),
            f"{c}_ratio_p50": float(r.quantile(0.50)),
            f"{c}_ratio_p75": float(r.quantile(0.75)),
            f"{c}_ratio_p90": float(r.quantile(0.90)),
            f"{c}_ratio_p95": float(r.quantile(0.95)),
            f"{c}_ratio_p99": float(r.quantile(0.99)),
            f"{c}_share_ratio_ge_1_10": float((r >= 1.10).mean()),
            f"{c}_share_ratio_ge_1_25": float((r >= 1.25).mean()),
            f"{c}_share_ratio_ge_1_50": float((r >= 1.50).mean()),
            f"{c}_share_ratio_ge_2_00": float((r >= 2.00).mean()),
        })
    write_json(out / "metrics" / "directionality_summary.json", directionality_summary)
    top_valid = reciprocal[
        (reciprocal["car_time_detour"] >= 1.0) &
        (reciprocal["car_time_detour_reverse"] >= 1.0)
    ].copy()
    top_valid.sort_values("car_time_detour_ratio", ascending=False).head(1000).to_csv(
        out / "data" / "top_reciprocal_asymmetry_pairs.csv",
        index=False,
    )

    # Compatibility with existing Shenzhen bottom-layer data.
    compatibility: dict[str, object] = {}
    flow_path = Path(args.flow_dta)
    if flow_path.exists():
        flow = pd.read_stata(
            flow_path,
            columns=["resi_all_id", "work_all_id", "flow", "num_asset_car"],
            convert_categoricals=False,
        )
        flow["resi_all_num"] = pd.to_numeric(flow["resi_all_id"], errors="coerce")
        flow["work_all_num"] = pd.to_numeric(flow["work_all_id"], errors="coerce")
        valid = (flow["resi_all_num"] >= 0) & (flow["work_all_num"] >= 0)
        match = flow["resi_all_num"].isin(id_set) & flow["work_all_num"].isin(id_set)
        compatibility["grid500_flow_rows"] = int(len(flow))
        compatibility["grid500_valid_flow_rows"] = int(valid.sum())
        compatibility["grid500_rows_with_both_ids_in_new_fids"] = int(match.sum())
        compatibility["grid500_flow_total"] = float(flow["flow"].sum())
        compatibility["grid500_valid_flow_total"] = float(flow.loc[valid, "flow"].sum())
        compatibility["grid500_matched_flow_total"] = float(flow.loc[match, "flow"].sum())
        compatibility["grid500_matched_share_of_valid_flow"] = float(
            flow.loc[match, "flow"].sum() / max(EPS, flow.loc[valid, "flow"].sum())
        )
        compatibility["grid500_matched_share_of_valid_car_asset_count"] = float(
            flow.loc[match, "num_asset_car"].sum() / max(EPS, flow.loc[valid, "num_asset_car"].sum())
        )
        compatibility["interpretation_grid500"] = (
            "Low numeric-id overlap means fishnet1000 fid cannot be treated as the same id system as "
            "the existing grid500 all_id without a geometry/crosswalk file."
        )

    grid_map_path = Path(args.grid_map_dta)
    if grid_map_path.exists():
        grid_map = pd.read_stata(grid_map_path, columns=["grid_id", "all_id"], convert_categoricals=False)
        grid_map["all_num"] = pd.to_numeric(grid_map["all_id"], errors="coerce")
        compatibility["grid500_map_rows"] = int(len(grid_map))
        compatibility["grid500_map_unique_grid_id"] = int(grid_map["grid_id"].nunique())
        compatibility["grid500_map_unique_all_id"] = int(grid_map["all_num"].nunique())
        compatibility["grid500_map_unique_all_id_in_new_fids"] = int(grid_map.loc[grid_map["all_num"].isin(id_set), "all_num"].nunique())

    speed_path = Path(args.speed_csv)
    if speed_path.exists():
        speed = pd.read_csv(speed_path, usecols=["roadseg_id", "speed", "day", "hour"])
        compatibility["speed_panel_rows"] = int(len(speed))
        compatibility["speed_panel_unique_roadseg"] = int(speed["roadseg_id"].nunique())
        compatibility["speed_panel_unique_day"] = int(speed["day"].nunique())
        compatibility["speed_panel_unique_hour"] = int(speed["hour"].nunique())
        compatibility["speed_panel_speed_mean_kmh"] = float(speed["speed"].mean())
        compatibility["speed_panel_speed_p50_kmh"] = float(speed["speed"].quantile(0.50))
        compatibility["speed_panel_has_grid_od_time"] = False
        compatibility["interpretation_speed_panel"] = (
            "Existing speed_shenzhen_all_wgs84 is a 10-minute directed road-segment speed panel, "
            "whereas fishnet1000_pairtime_hat is a static all-pairs grid OD travel-time matrix."
        )

    write_json(out / "metrics" / "compatibility_with_existing_data.json", compatibility)

    # Figures.
    plt.style.use("seaborn-v0_8-whitegrid")
    fig, ax = plt.subplots(figsize=(7.2, 4.6))
    ax.hist(nonself["car_time_detour"], bins=80, alpha=0.75, label="detour", color="#1f77b4")
    ax.hist(nonself["car_time"], bins=80, alpha=0.55, label="base", color="#ff7f0e")
    ax.set_xlabel("Car travel time, minutes")
    ax.set_ylabel("OD pairs")
    ax.legend()
    ax.set_title("Fishnet1000 OD Travel-Time Distribution")
    fig.tight_layout()
    fig.savefig(out / "figures" / "travel_time_distribution.png", dpi=220)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(7.2, 4.6))
    ax.hist(reciprocal["car_time_detour_ratio"].clip(upper=4), bins=70, color="#3c6e71", alpha=0.85)
    for x, ls in [(1.10, "-"), (1.25, "--"), (1.50, ":"), (2.00, "-.")]:
        ax.axvline(x, color="#9a3412", linestyle=ls, linewidth=1.1)
    ax.set_xlabel("Reciprocal-pair detour-time ratio, clipped at 4")
    ax.set_ylabel("Reciprocal pairs")
    ax.set_title("Fishnet1000 Directional Travel-Time Asymmetry")
    fig.tight_layout()
    fig.savefig(out / "figures" / "directionality_ratio_distribution.png", dpi=220)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(7.2, 4.6))
    ax.hist(nonself["time_diff"], bins=80, color="#5f6f52", alpha=0.85)
    ax.set_xlabel("Detour time minus base time, minutes")
    ax.set_ylabel("OD pairs")
    ax.set_title("Detour Time Penalty")
    fig.tight_layout()
    fig.savefig(out / "figures" / "detour_time_penalty_distribution.png", dpi=220)
    plt.close(fig)

    print(f"Wrote {out}")


if __name__ == "__main__":
    main()
