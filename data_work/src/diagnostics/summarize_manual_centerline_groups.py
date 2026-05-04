import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.stages.stage01_build_centerline import (
    load_manual_centerline_groups,
    load_raw_roads,
    select_manual_group_raw,
)


def parse_args():
    parser = argparse.ArgumentParser(description="Summarize current manual centerline group rules.")
    parser.add_argument("--version-id", required=True, help="Version identifier for existing outputs to compare against.")
    parser.add_argument("--output-dir", default="outputs", help="Base output directory.")
    return parser.parse_args()


def quantile_or_nan(df: pd.DataFrame, col: str, q: float) -> float:
    if df.empty:
        return np.nan
    return float(df[col].quantile(q))


def build_group_summary(raw_valid: pd.DataFrame, manual_raw: pd.DataFrame, groups: list[dict]) -> pd.DataFrame:
    rows = []
    for group in groups:
        gid = group["group_id"]
        mask_name = (
            raw_valid["roadname"].astype("string").isin(group["roadnames"]).fillna(False)
            if group["roadnames"]
            else pd.Series(False, index=raw_valid.index)
        )
        mask_id = (
            raw_valid["roadseg_id"].astype("string").isin(group["roadseg_ids"]).fillna(False)
            if group["roadseg_ids"]
            else pd.Series(False, index=raw_valid.index)
        )
        selected = raw_valid.loc[mask_name | mask_id].copy()
        selected["length_m"] = selected.geometry.length.astype(float)

        assigned = manual_raw.loc[manual_raw["manual_group_id"] == gid].copy()
        assigned["length_m"] = assigned.geometry.length.astype(float) if len(assigned) else pd.Series(dtype=float)

        rows.append(
            {
                "group_id": gid,
                "group_name": group["group_name"],
                "selector_type": (
                    "mixed"
                    if group["roadnames"] and group["roadseg_ids"]
                    else "roadname"
                    if group["roadnames"]
                    else "roadseg_id"
                ),
                "min_raw_length_m": group["min_raw_length_m"],
                "keep_main_component": group["keep_main_component"],
                "main_component_snap_m": group["main_component_snap_m"],
                "n_selected_raw": int(len(selected)),
                "n_assigned_current": int(len(assigned)),
                "total_len_km_selected": float(selected["length_m"].sum() / 1000.0) if len(selected) else 0.0,
                "selected_min_m": float(selected["length_m"].min()) if len(selected) else np.nan,
                "selected_p10_m": quantile_or_nan(selected, "length_m", 0.10),
                "selected_p25_m": quantile_or_nan(selected, "length_m", 0.25),
                "selected_median_m": quantile_or_nan(selected, "length_m", 0.50),
                "selected_p75_m": quantile_or_nan(selected, "length_m", 0.75),
                "selected_p90_m": quantile_or_nan(selected, "length_m", 0.90),
                "selected_max_m": float(selected["length_m"].max()) if len(selected) else np.nan,
                "n_selected_lt_60": int((selected["length_m"] < 60).sum()) if len(selected) else 0,
                "n_selected_lt_120": int((selected["length_m"] < 120).sum()) if len(selected) else 0,
                "share_selected_lt_120": float((selected["length_m"] < 120).mean()) if len(selected) else np.nan,
                "assigned_total_km": float(assigned["length_m"].sum() / 1000.0) if len(assigned) else 0.0,
                "assigned_median_m": float(assigned["length_m"].median()) if len(assigned) else np.nan,
            }
        )
    return pd.DataFrame(rows)


def build_pre_filter_short_segments(raw_valid: pd.DataFrame, groups: list[dict]) -> pd.DataFrame:
    rows = []
    for group in groups:
        mask_name = (
            raw_valid["roadname"].astype("string").isin(group["roadnames"]).fillna(False)
            if group["roadnames"]
            else pd.Series(False, index=raw_valid.index)
        )
        mask_id = (
            raw_valid["roadseg_id"].astype("string").isin(group["roadseg_ids"]).fillna(False)
            if group["roadseg_ids"]
            else pd.Series(False, index=raw_valid.index)
        )
        selected = raw_valid.loc[mask_name | mask_id].copy()
        if selected.empty:
            continue
        selected["length_m"] = selected.geometry.length.astype(float)
        selected = selected.loc[selected["length_m"] < 120].copy()
        if selected.empty:
            continue
        selected["group_id"] = group["group_id"]
        selected["group_name"] = group["group_name"]
        rows.append(
            selected[
                [
                    "group_id",
                    "group_name",
                    "raw_edge_id",
                    "roadseg_id",
                    "roadname",
                    "length_m",
                ]
            ]
        )
    if not rows:
        return pd.DataFrame(columns=["group_id", "group_name", "raw_edge_id", "roadseg_id", "roadname", "length_m"])
    return pd.concat(rows, ignore_index=True).sort_values(["group_id", "length_m", "raw_edge_id"])


def compare_to_existing_output(version_root: Path, current_summary: pd.DataFrame) -> pd.DataFrame:
    report_path = version_root / "metrics" / "stage01_manual_group_report.csv"
    if not report_path.exists():
        out = current_summary[["group_id", "group_name", "n_selected_raw", "n_assigned_current"]].copy()
        out["n_assigned_existing_output"] = np.nan
        out["delta_current_minus_existing"] = np.nan
        return out

    existing = pd.read_csv(report_path)
    if "n_assigned_unique" in existing.columns:
        existing = existing.rename(columns={"n_assigned_unique": "n_assigned_existing_output"})
    else:
        existing["n_assigned_existing_output"] = np.nan

    cols = ["group_id", "n_assigned_existing_output"]
    existing = existing[cols].copy()
    out = current_summary[["group_id", "group_name", "n_selected_raw", "n_assigned_current"]].merge(existing, on="group_id", how="left")
    out["delta_current_minus_existing"] = out["n_assigned_current"] - out["n_assigned_existing_output"]
    return out.sort_values("group_id")


def main():
    args = parse_args()
    version_root = Path(args.output_dir) / args.version_id
    metrics_dir = version_root / "metrics"
    metrics_dir.mkdir(parents=True, exist_ok=True)

    raw = load_raw_roads()
    raw_valid = raw.loc[raw["is_valid_geometry"] & raw["is_linestring"]].copy()
    groups = load_manual_centerline_groups()
    manual_raw, _, report = select_manual_group_raw(raw_valid, groups)

    current_summary = build_group_summary(raw_valid, manual_raw, groups)
    current_vs_existing = compare_to_existing_output(version_root, current_summary)
    pre_filter_short = build_pre_filter_short_segments(raw_valid, groups)

    if not report.empty:
        report.to_csv(metrics_dir / "manual_group_rule_audit_current_filter_steps.csv", index=False)
    current_summary.to_csv(metrics_dir / "manual_group_rule_audit_current_summary.csv", index=False)
    current_vs_existing.to_csv(metrics_dir / "manual_group_rule_audit_current_vs_existing_output.csv", index=False)
    pre_filter_short.to_csv(metrics_dir / "manual_group_rule_audit_pre_filter_short_segments.csv", index=False)

    print(f"[manual-audit] saved current filter steps: {metrics_dir / 'manual_group_rule_audit_current_filter_steps.csv'}")
    print(f"[manual-audit] saved current summary: {metrics_dir / 'manual_group_rule_audit_current_summary.csv'}")
    print(f"[manual-audit] saved current vs existing: {metrics_dir / 'manual_group_rule_audit_current_vs_existing_output.csv'}")
    print(f"[manual-audit] saved pre-filter short segments: {metrics_dir / 'manual_group_rule_audit_pre_filter_short_segments.csv'}")
    print(f"[manual-audit] current code selected manual raw: {len(manual_raw)}")


if __name__ == "__main__":
    main()
