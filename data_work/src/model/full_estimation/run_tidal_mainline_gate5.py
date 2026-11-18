"""Assemble tidal mainline analysis package from Phase 0 diagnostics outputs."""
from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

DATA_WORK = Path(__file__).resolve().parents[3]
if str(DATA_WORK) not in sys.path:
    sys.path.insert(0, str(DATA_WORK))

from src.model.full_estimation.phase_metrics_common import data_work_root  # noqa: E402

DEFAULT_OUT = "tidal_mainline_bpr_pos05_v1"
TIDAL_DIR = "tidal_lane_diagnostics_edgeproj_v2_lcc_gate5_bpr_pos05_v1"
WELFARE_DIR = "welfare_sorting_diagnostics_gate5_bpr_pos05_v1"


def assemble(
    data_work: Path,
    out: Path,
    *,
    tidal_dir: str,
    welfare_dir: str,
) -> None:
    out.mkdir(parents=True, exist_ok=True)
    maps_out = out / "maps"
    maps_out.mkdir(parents=True, exist_ok=True)

    troot = data_work / "outputs" / tidal_dir
    wroot = data_work / "outputs" / welfare_dir

    for src_name, dst_name in (
        ("tidal_st_lt_compact.csv", "tidal_st_lt_compact.csv"),
        ("tidal_st_lt_congestion_summary.csv", "tidal_st_lt_congestion_summary.csv"),
        ("tidal_treated_edge_detail.csv", "tidal_treated_edge_detail.csv"),
    ):
        src = troot / src_name
        if src.exists():
            shutil.copy2(src, out / dst_name)

    pos05 = wroot / "pos05"
    for fname in (
        "wbar_theta_decomposition.json",
        "pi_inv_decomposition.json",
        "endpoint_group_summary.csv",
        "node_sorting_changes.csv",
    ):
        src = pos05 / fname
        if src.exists():
            shutil.copy2(src, out / fname)

    for png in pos05.glob("map_*.png"):
        shutil.copy2(png, maps_out / png.name)

    memo_lines = [
        "# Tidal mainline — bpr+pos05 (gate5)",
        "",
        f"- Tidal diagnostics: `{tidal_dir}`",
        f"- Welfare/sorting: `{welfare_dir}/pos05`",
        "",
        "## Sanity check targets (λ=1.1, top100)",
        "",
        "| Metric | Expected |",
        "|--------|----------|",
        "| ST direct | ≈ −1.63 s |",
        "| sorting wedge | ≈ +0.71 s |",
        "| LT total | ≈ +0.07 s |",
        "| person-min/day | ≈ +95 |",
        "| W̄ ST | ≈ +0.32% |",
        "| W̄ LT | small negative |",
        "",
    ]
    compact = out / "tidal_st_lt_compact.csv"
    if compact.exists():
        import pandas as pd
        df = pd.read_csv(compact)
        top = df[df["spec"] == "top100_d1.0"]
        if not top.empty:
            r = top.iloc[0]
            memo_lines.extend([
                "## Actual (top100_d1.0)",
                "",
                f"- ST direct: {r['direct_st_cost_change_sec']:.2f} s",
                f"- sorting: {r['sorting_wedge_sec']:.2f} s",
                f"- LT total: {r['total_lt_cost_change_sec']:.2f} s",
                f"- W̄ ST: {r['Wbar_pct_change_st']:.3f}%",
                f"- W̄ LT: {r['Wbar_pct_change_lt']:.3f}%",
                "",
            ])
    (out / "memo_tidal_mainline.md").write_text("\n".join(memo_lines))
    (out / "package_meta.json").write_text(json.dumps({
        "tidal_dir": tidal_dir,
        "welfare_dir": welfare_dir,
    }, indent=2))


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo-root", default=str(DATA_WORK))
    p.add_argument("--output-name", default=DEFAULT_OUT)
    p.add_argument("--tidal-dir", default=TIDAL_DIR)
    p.add_argument("--welfare-dir", default=WELFARE_DIR)
    args = p.parse_args(argv)

    data_work = data_work_root(args.repo_root)
    out = data_work / "outputs" / args.output_name
    assemble(data_work, out, tidal_dir=args.tidal_dir, welfare_dir=args.welfare_dir)
    print(f"Assembled {out}")


if __name__ == "__main__":
    main()
