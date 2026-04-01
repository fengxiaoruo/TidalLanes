"""
Stage 09: Compare Versions

Purpose:
- Compare baseline and upgraded versions
- Summarize diagnostics and output differences across runs
"""

import argparse
from pathlib import Path

import pandas as pd


def parse_args():
    parser = argparse.ArgumentParser(description="Stage 09: Compare versions")
    parser.add_argument("--output-dir", default="outputs", help="Base output directory for versioned results.")
    return parser.parse_args()


def build_summary_for_version(version_root: Path):
    metrics_dir = version_root / "metrics"
    rows = {"version_id": version_root.name}

    mapping = {
        "stage02_match_summary.csv": ["split_match_rate", "raw_edge_match_rate"],
        "stage03_speed_summary.csv": ["mean_speed_kmh", "median_speed_kmh"],
        "stage03b_asymmetry_summary.csv": ["am_unsym1", "pm_unsym1", "reversed_count", "tidal_candidate_count"],
        "stage04_lane_summary.csv": ["mean_lane_est", "mean_opposite_ratio", "share_lane_symmetry_flag"],
        "stage07_square_summary.csv": ["reachable_share_pairs", "reachable_share_pop"],
        "stage08_square_summary.csv": ["qsm_nodes", "qsm_edges", "qsm_od_pairs"],
    }

    for fname, cols in mapping.items():
        path = metrics_dir / fname
        if path.exists():
            df = pd.read_csv(path)
            if len(df):
                for col in cols:
                    if col in df.columns:
                        rows[col] = df.iloc[0][col]

    out = pd.DataFrame([rows])
    out.to_csv(metrics_dir / "summary_metrics.csv", index=False)
    return out


def run(output_dir: str):
    root = Path(output_dir)
    versions = [p for p in root.iterdir() if p.is_dir() and (p / "metrics").exists()]
    all_rows = []
    for version_root in sorted(versions):
        all_rows.append(build_summary_for_version(version_root))

    comparison_dir = root / "comparison"
    comparison_dir.mkdir(parents=True, exist_ok=True)
    if all_rows:
        comp = pd.concat(all_rows, ignore_index=True)
    else:
        comp = pd.DataFrame(columns=["version_id"])
    comp.to_csv(comparison_dir / "across_versions.csv", index=False)
    (comparison_dir / "comparison_report.md").write_text(
        "# Version Comparison\n\n"
        f"Versions summarized: {len(comp)}\n",
        encoding="utf-8",
    )
    print(f"[stage09] saved comparison for {len(comp)} versions")


def main():
    args = parse_args()
    run(args.output_dir)


if __name__ == "__main__":
    main()
