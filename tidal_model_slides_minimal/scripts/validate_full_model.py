from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import pandas as pd

from slide_metric_checks import validate_slide_descriptive_metrics


ROOT = Path(__file__).resolve().parents[1]


RUNS = [
    [
        sys.executable,
        "-m",
        "src.model.full_estimation.run_stage2_gate5",
        "--repo-root",
        ".",
        "--output-name",
        "validation_stage2_full",
        "--max-outer",
        "80",
        "--tol",
        "1e-5",
    ],
    [
        sys.executable,
        "-m",
        "src.model.full_estimation.run_tidal_lane_diagnostics_gate5",
        "--repo-root",
        ".",
        "--output-name",
        "validation_tidal_diagnostics",
        "--max-outer",
        "500",
    ],
    [
        sys.executable,
        "-m",
        "src.model.full_estimation.run_policy_comparison_gate5",
        "--repo-root",
        ".",
        "--stage2-dir",
        "validation_stage2_full",
        "--output-name",
        "validation_policy_comparison",
    ],
    [
        sys.executable,
        "-m",
        "src.model.full_estimation.run_alpha_beta_sensitivity_bpr_gate5",
        "--repo-root",
        ".",
        "--output-name",
        "validation_alpha_beta_sensitivity",
        "--sweep",
        "both",
    ],
    [
        sys.executable,
        "-m",
        "src.model.full_estimation.run_symmetry_directionality_suite_gate5",
        "--repo-root",
        ".",
        "--output-name",
        "validation_symmetry_directionality_suite",
    ],
]


CHECKS = [
    ("stage2 structural exog", "outputs/validation_stage2_full/counterfactuals/structural_cf_exogenous.csv", "outputs/reference_results/structural_cf_exogenous.csv", ["cf", "congestion_mode"]),
    ("stage2 structural endog", "outputs/validation_stage2_full/counterfactuals/structural_cf_endogenous.csv", "outputs/reference_results/structural_cf_endogenous.csv", ["cf", "congestion_mode", "scenario_label"]),
    ("stage2 tidal exog bpr grid", "outputs/validation_stage2_full/counterfactuals/tidal_lane_exogenous_bpr_grid.csv", "outputs/reference_results/tidal_lane_exogenous_bpr_grid.csv", ["spec_name", "horizon", "top_n", "delta_lanes"]),
    ("tidal compact", "outputs/validation_tidal_diagnostics/tidal_st_lt_compact.csv", "outputs/reference_results/tidal_st_lt_compact.csv", ["spec"]),
    ("tidal congestion summary", "outputs/validation_tidal_diagnostics/tidal_st_lt_congestion_summary.csv", "outputs/reference_results/tidal_st_lt_congestion_summary.csv", ["spec"]),
    ("policy results", "outputs/validation_policy_comparison/results_by_policy.csv", "outputs/reference_results/policy_results_by_policy.csv", ["policy_id", "horizon"]),
    ("policy cost effectiveness", "outputs/validation_policy_comparison/cost_effectiveness.csv", "outputs/reference_results/policy_cost_effectiveness.csv", ["policy_id"]),
    ("alpha beta grid", "outputs/validation_alpha_beta_sensitivity/sensitivity_grid.csv", "outputs/reference_results/alpha_beta_sensitivity_grid.csv", ["spec_label"]),
    ("symmetry directionality suite", "outputs/validation_symmetry_directionality_suite/summary_by_scenario.csv", "outputs/reference_results/symmetry_directionality_summary.csv", ["family", "world", "mode", "scenario"]),
]


def run_all() -> None:
    for cmd in RUNS:
        print("\n$ " + " ".join(cmd))
        subprocess.run(cmd, cwd=ROOT, check=True)


def compare_table(name: str, validation: str, reference: str, keys: list[str]) -> tuple[int, int, float, str | None]:
    a = pd.read_csv(ROOT / validation)
    b = pd.read_csv(ROOT / reference)
    for key in keys:
        if key not in a.columns or key not in b.columns:
            raise SystemExit(f"{name}: key missing: {key}")
    a2 = a.set_index(keys).sort_index()
    b2 = b.set_index(keys).sort_index()
    only_a = a2.index.difference(b2.index)
    only_b = b2.index.difference(a2.index)
    if len(only_a) or len(only_b):
        raise SystemExit(f"{name}: key mismatch validation={len(only_a)} reference={len(only_b)}")
    common = a2.index.intersection(b2.index)
    max_abs = 0.0
    worst = None
    for col in a2.columns.intersection(b2.columns):
        if pd.api.types.is_bool_dtype(a2[col]) or pd.api.types.is_bool_dtype(b2[col]):
            continue
        if not (pd.api.types.is_numeric_dtype(a2[col]) and pd.api.types.is_numeric_dtype(b2[col])):
            continue
        diff = (
            pd.to_numeric(a2.loc[common, col], errors="coerce").astype(float)
            - pd.to_numeric(b2.loc[common, col], errors="coerce").astype(float)
        ).abs()
        if diff.notna().any() and float(diff.max()) > max_abs:
            max_abs = float(diff.max())
            worst = col
    return len(a), len(b), max_abs, worst


def compare_policy_all_modes() -> tuple[int, int, float, str | None]:
    a = pd.read_csv(ROOT / "outputs/validation_stage2_full/counterfactuals/policy_cf_all_modes.csv")
    b = pd.read_csv(ROOT / "outputs/reference_results/policy_cf_all_modes.csv")
    for df in (a, b):
        df["_ord"] = df.groupby("policy").cumcount()
    tmp_a = ROOT / "outputs/validation_stage2_full/counterfactuals/_policy_cf_all_modes_keyed.csv"
    tmp_b = ROOT / "outputs/reference_results/_policy_cf_all_modes_keyed.csv"
    a.to_csv(tmp_a, index=False)
    b.to_csv(tmp_b, index=False)
    try:
        return compare_table("stage2 policy all modes", str(tmp_a.relative_to(ROOT)), str(tmp_b.relative_to(ROOT)), ["policy", "_ord"])
    finally:
        tmp_a.unlink(missing_ok=True)
        tmp_b.unlink(missing_ok=True)


def compare_all() -> None:
    print("\nValidation comparisons")
    summary = []
    for item in CHECKS:
        summary.append((item[0], *compare_table(*item)))
    summary.append(("stage2 policy all modes", *compare_policy_all_modes()))
    for name, n_a, n_b, max_abs, worst in summary:
        print(f"{name}: rows {n_a}/{n_b}, max_abs_diff={max_abs:.12g}, worst={worst}")
        if max_abs > 1e-9:
            raise SystemExit(f"{name}: numeric difference exceeds tolerance")
    print("\nSlide descriptive metric checks")
    for line in validate_slide_descriptive_metrics(ROOT):
        print(line)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--compare-only", action="store_true")
    args = parser.parse_args(argv)
    if not args.compare_only:
        run_all()
    compare_all()


if __name__ == "__main__":
    main()
