from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
GRID_TYPES = ("square", "hex", "voronoi")
PERIODS = ("AM", "PM")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare retained raw_connected edgeproj outputs across two versions.")
    parser.add_argument("--v1-root", default=str(ROOT / "outputs" / "raw_connected_v1_grid_travel_time_edgeproj_v2"))
    parser.add_argument("--v2-root", default=str(ROOT / "outputs" / "raw_connected_v2_grid_travel_time_edgeproj_v1"))
    parser.add_argument("--out-dir", default=str(ROOT / "outputs" / "raw_connected_v2_grid_travel_time_edgeproj_v1" / "diagnostics_compare_v1_v2"))
    parser.add_argument("--v1-label", default="raw_connected_v1_edgeproj_v2")
    parser.add_argument("--v2-label", default="raw_connected_v2_edgeproj_v1")
    parser.add_argument("--v1-repl-summary", default=str(ROOT.parent / "Replication_apply" / "prepare_from_data_work_square_v2" / "build_summary_square.json"))
    parser.add_argument("--v2-repl-summary", default=str(ROOT.parent / "Replication_apply" / "prepare_from_data_work_square_v2_raw_connected_v2" / "build_summary_square.json"))
    parser.add_argument("--v1-specc-dir", default=str(ROOT.parent / "Replication_apply" / "results" / "tidal_lane"))
    parser.add_argument("--v2-specc-dir", default=str(ROOT.parent / "Replication_apply" / "results" / "tidal_lane_raw_connected_v2"))
    parser.add_argument("--lcc-v1-root", default="", help="Optional LCC bundle dir for v1 (model_input_*).")
    parser.add_argument("--lcc-v2-root", default="", help="Optional LCC bundle dir for v2/v3 (model_input_*).")
    parser.add_argument("--skip-repl", action="store_true", help="Skip Replication/SpecC comparison.")
    return parser.parse_args()


def finite_mask(df: pd.DataFrame) -> pd.Series:
    vals = pd.to_numeric(df["raw_connected_travel_time_min"], errors="coerce")
    return np.isfinite(vals) & (vals > 0)


def summarize_pair_file(path: Path, version: str, grid_type: str, period: str) -> dict:
    df = pd.read_csv(path)
    t = pd.to_numeric(df["raw_connected_travel_time_min"], errors="coerce")
    old = pd.to_numeric(df.get("old_travel_time_min"), errors="coerce")
    tau_ff = pd.to_numeric(df.get("tau_ff_min"), errors="coerce")
    lane = pd.to_numeric(df.get("lane_est_path_raw_edgeproj_len_weighted"), errors="coerce")
    finite = np.isfinite(t) & (t > 0)
    comparable = finite & np.isfinite(old) & (old > 0)
    return {
        "version": version,
        "grid_type": grid_type,
        "period": period,
        "rows": int(len(df)),
        "finite_rows": int(finite.sum()),
        "finite_share": float(finite.mean()) if len(df) else np.nan,
        "mean_time_min": float(t[finite].mean()) if finite.any() else np.nan,
        "median_time_min": float(t[finite].median()) if finite.any() else np.nan,
        "p90_time_min": float(t[finite].quantile(0.90)) if finite.any() else np.nan,
        "comparable_rows": int(comparable.sum()),
        "median_ratio_raw_over_old": float((t[comparable] / old[comparable]).median()) if comparable.any() else np.nan,
        "mean_ratio_raw_over_old": float((t[comparable] / old[comparable]).mean()) if comparable.any() else np.nan,
        "tau_ff_finite_rows": int((np.isfinite(tau_ff) & (tau_ff > 0)).sum()) if tau_ff is not None else 0,
        "lane_finite_rows": int((np.isfinite(lane) & (lane > 0)).sum()) if lane is not None else 0,
    }


def compare_pair_files(
    v1_root: Path, v2_root: Path, v1_label: str, v2_label: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    summary_rows = []
    overlap_rows = []
    for grid_type in GRID_TYPES:
        for period in PERIODS:
            rel = Path("data") / f"raw_connected_vs_old_adjacent_adjacent_{grid_type}_{period}.csv"
            v1_path = v1_root / rel
            v2_path = v2_root / rel
            if not v1_path.exists() or not v2_path.exists():
                continue
            summary_rows.append(summarize_pair_file(v1_path, v1_label, grid_type, period))
            summary_rows.append(summarize_pair_file(v2_path, v2_label, grid_type, period))

            v1 = pd.read_csv(v1_path)
            v2 = pd.read_csv(v2_path)
            keys = ["home_grid", "work_grid"]
            for df in [v1, v2]:
                df["home_grid"] = df["home_grid"].astype(str)
                df["work_grid"] = df["work_grid"].astype(str)
            keep_cols = keys + ["raw_connected_travel_time_min", "tau_ff_min", "route_length_m"]
            m = v1[keep_cols].merge(v2[keep_cols], on=keys, how="outer", suffixes=("_v1", "_v2"), indicator=True)
            t1 = pd.to_numeric(m["raw_connected_travel_time_min_v1"], errors="coerce")
            t2 = pd.to_numeric(m["raw_connected_travel_time_min_v2"], errors="coerce")
            finite1 = np.isfinite(t1) & (t1 > 0)
            finite2 = np.isfinite(t2) & (t2 > 0)
            both = finite1 & finite2
            overlap_rows.append(
                {
                    "grid_type": grid_type,
                    "period": period,
                    "rows_union": int(len(m)),
                    "finite_v1_only": int((finite1 & ~finite2).sum()),
                    "finite_v2_only": int((finite2 & ~finite1).sum()),
                    "finite_both": int(both.sum()),
                    "finite_either": int((finite1 | finite2).sum()),
                    "share_v2_of_v1_finite": float(both.sum() / finite1.sum()) if finite1.sum() else np.nan,
                    "share_v1_of_v2_finite": float(both.sum() / finite2.sum()) if finite2.sum() else np.nan,
                    "median_time_diff_v2_minus_v1": float((t2[both] - t1[both]).median()) if both.any() else np.nan,
                    "mean_time_diff_v2_minus_v1": float((t2[both] - t1[both]).mean()) if both.any() else np.nan,
                    "median_time_ratio_v2_over_v1": float((t2[both] / t1[both]).median()) if both.any() else np.nan,
                    "mean_time_ratio_v2_over_v1": float((t2[both] / t1[both]).mean()) if both.any() else np.nan,
                }
            )
    return pd.DataFrame(summary_rows), pd.DataFrame(overlap_rows)


def read_optional_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path) if path.exists() else pd.DataFrame()


def compare_summary_tables(v1_root: Path, v2_root: Path, subdir: str, filename: str, metrics: list[str]) -> pd.DataFrame:
    v1 = read_optional_csv(v1_root / "metrics" / subdir / filename)
    v2 = read_optional_csv(v2_root / "metrics" / subdir / filename)
    if v1.empty or v2.empty:
        return pd.DataFrame()
    keys = [c for c in ["grid_type", "period"] if c in v1.columns and c in v2.columns]
    rows = []
    m = v1.merge(v2, on=keys, suffixes=("_v1", "_v2"))
    for row in m.to_dict("records"):
        base = {k: row[k] for k in keys}
        for metric in metrics:
            a = pd.to_numeric(pd.Series([row.get(f"{metric}_v1")]), errors="coerce").iloc[0]
            b = pd.to_numeric(pd.Series([row.get(f"{metric}_v2")]), errors="coerce").iloc[0]
            base[f"{metric}_v1"] = a
            base[f"{metric}_v2"] = b
            base[f"{metric}_diff_v2_minus_v1"] = b - a if np.isfinite(a) and np.isfinite(b) else np.nan
        rows.append(base)
    return pd.DataFrame(rows)


def compare_repl(v1_summary_path: Path, v2_summary_path: Path) -> pd.DataFrame:
    v1 = json.loads(v1_summary_path.read_text(encoding="utf-8"))
    v2 = json.loads(v2_summary_path.read_text(encoding="utf-8"))
    metrics = ["node_rows", "edge_rows", "od_rows", "traffic_min", "traffic_mean", "traffic_max"]
    rows = []
    for metric in metrics:
        a = v1.get(metric)
        b = v2.get(metric)
        rows.append(
            {
                "metric": metric,
                "v1": a,
                "v2": b,
                "diff_v2_minus_v1": b - a if isinstance(a, (int, float)) and isinstance(b, (int, float)) else None,
                "ratio_v2_over_v1": b / a if isinstance(a, (int, float)) and isinstance(b, (int, float)) and a else None,
            }
        )
    return pd.DataFrame(rows)


def spec_c_metrics(result_dir: Path, version: str) -> dict:
    selected = pd.read_csv(result_dir / "selected_pairs_specC.csv")
    chi = pd.read_csv(result_dir / "chi_lr_lf_specC.csv")
    return {
        "version": version,
        "treated_pairs": int(len(selected)),
        "asym_ratio_min": float(pd.to_numeric(selected["asym_ratio"], errors="coerce").min()),
        "asym_ratio_max": float(pd.to_numeric(selected["asym_ratio"], errors="coerce").max()),
        "tau_slow_change_mean": float((pd.to_numeric(selected["tau_slow_new"], errors="coerce") / pd.to_numeric(selected["tau_slow_obs"], errors="coerce") - 1).mean()),
        "tau_fast_change_mean": float((pd.to_numeric(selected["tau_fast_new"], errors="coerce") / pd.to_numeric(selected["tau_fast_obs"], errors="coerce") - 1).mean()),
        "tbar_slow_min": float(pd.to_numeric(selected["tbar_slow"], errors="coerce").min()),
        "tbar_slow_max": float(pd.to_numeric(selected["tbar_slow"], errors="coerce").max()),
        "tbar_fast_min": float(pd.to_numeric(selected["tbar_fast"], errors="coerce").min()),
        "tbar_fast_max": float(pd.to_numeric(selected["tbar_fast"], errors="coerce").max()),
        "chi_hat": float(pd.to_numeric(chi["chi_hat"], errors="coerce").iloc[0]),
        "welfare_micro": float((pd.to_numeric(chi["chi_hat"], errors="coerce").iloc[0] - 1) * 1e6),
        "resident_hat_std": float(pd.to_numeric(chi["l_r_hat"], errors="coerce").std()),
        "worker_hat_std": float(pd.to_numeric(chi["l_f_hat"], errors="coerce").std()),
    }


def compare_spec_c(v1_dir: Path, v2_dir: Path, v1_label: str, v2_label: str) -> pd.DataFrame:
    long = pd.DataFrame(
        [
            spec_c_metrics(v1_dir, v1_label),
            spec_c_metrics(v2_dir, v2_label),
        ]
    )
    rows = []
    for metric in [c for c in long.columns if c != "version"]:
        a = long.loc[long["version"] == v1_label, metric].iloc[0]
        b = long.loc[long["version"] == v2_label, metric].iloc[0]
        rows.append(
            {
                "metric": metric,
                "v1": a,
                "v2": b,
                "diff_v2_minus_v1": b - a if isinstance(a, (int, float, np.integer, np.floating)) else None,
                "ratio_v2_over_v1": b / a if isinstance(a, (int, float, np.integer, np.floating)) and a else None,
            }
        )
    return pd.DataFrame(rows)


def compare_lcc_bundles(v1_lcc_root: Path, v2_lcc_root: Path, v1_label: str, v2_label: str) -> pd.DataFrame:
    """Compare two locked LCC bundle summaries (build_locked_bundle output)."""
    def load(root: Path) -> dict:
        p = root / "metrics" / "travel_cost_bundle_summary.json"
        return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}

    a = load(v1_lcc_root)
    b = load(v2_lcc_root)
    if not a or not b:
        return pd.DataFrame()
    metrics = [
        "n_nodes", "n_edges", "n_od_pairs", "lcc_grid_count",
        "resident_share_in_scope", "job_share_in_scope",
        "od_road_commuters", "od_road_commuters_share_city",
        "mean_tau_obs_min", "median_tau_obs_min",
        "median_directional_ratio", "share_asym_lt_0_5", "both_direction_pairs",
    ]
    rows = []
    for m in metrics:
        va, vb = a.get(m), b.get(m)
        diff = vb - va if isinstance(va, (int, float)) and isinstance(vb, (int, float)) else None
        ratio = vb / va if isinstance(va, (int, float)) and isinstance(vb, (int, float)) and va else None
        rows.append({"metric": m, v1_label: va, v2_label: vb, "diff": diff, "ratio": ratio})
    return pd.DataFrame(rows)


def main() -> None:
    args = parse_args()
    v1_root = Path(args.v1_root)
    v2_root = Path(args.v2_root)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    v1_label, v2_label = args.v1_label, args.v2_label

    pair_summary, pair_overlap = compare_pair_files(v1_root, v2_root, v1_label, v2_label)
    pair_summary.to_csv(out_dir / "travel_cost_summary_by_version.csv", index=False)
    pair_overlap.to_csv(out_dir / "travel_cost_v1_v2_overlap.csv", index=False)

    coverage = compare_summary_tables(
        v1_root,
        v2_root,
        "grid_coverage_maps",
        "raw_grid_coverage_summary_all.csv",
        [
            "covered_grids",
            "covered_grid_share",
            "largest_component_grids",
            "largest_component_grid_share",
            "covered_resident_share",
            "covered_job_share",
            "od_both_in_largest_pop_share",
        ],
    )
    coverage.to_csv(out_dir / "grid_coverage_v1_v2_compare.csv", index=False)

    asymmetry = compare_summary_tables(
        v1_root,
        v2_root,
        "grid_tidal_asymmetry",
        "raw_grid_tidal_asymmetry_summary_all.csv",
        [
            "undirected_pairs_both_dir",
            "median_directional_ratio",
            "share_asym_lt_0_8",
            "median_time_gap_min",
        ],
    )
    asymmetry.to_csv(out_dir / "grid_tidal_asymmetry_v1_v2_compare.csv", index=False)

    written = [
        "travel_cost_summary_by_version.csv",
        "travel_cost_v1_v2_overlap.csv",
        "grid_coverage_v1_v2_compare.csv",
        "grid_tidal_asymmetry_v1_v2_compare.csv",
    ]

    if not args.skip_repl:
        if Path(args.v1_repl_summary).exists() and Path(args.v2_repl_summary).exists():
            repl = compare_repl(Path(args.v1_repl_summary), Path(args.v2_repl_summary))
            repl.to_csv(out_dir / "replication_square_input_v1_v2_compare.csv", index=False)
            written.append("replication_square_input_v1_v2_compare.csv")
        else:
            print("[compare] skipping Replication compare (summary json missing for one version)")
        if Path(args.v1_specc_dir).exists() and Path(args.v2_specc_dir).exists():
            spec_c = compare_spec_c(Path(args.v1_specc_dir), Path(args.v2_specc_dir), v1_label, v2_label)
            spec_c.to_csv(out_dir / "tidal_lane_specC_v1_v2_compare.csv", index=False)
            written.append("tidal_lane_specC_v1_v2_compare.csv")
        else:
            print("[compare] skipping SpecC compare (result dir missing for one version)")

    if args.lcc_v1_root and args.lcc_v2_root:
        lcc = compare_lcc_bundles(Path(args.lcc_v1_root), Path(args.lcc_v2_root), v1_label, v2_label)
        if not lcc.empty:
            lcc.to_csv(out_dir / "lcc_bundle_v1_v2_compare.csv", index=False)
            written.append("lcc_bundle_v1_v2_compare.csv")
        else:
            print("[compare] skipping LCC bundle compare (summary json missing)")

    manifest = {
        "v1_root": str(v1_root),
        "v2_root": str(v2_root),
        "v1_label": v1_label,
        "v2_label": v2_label,
        "outputs": written,
    }
    (out_dir / "README_compare.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"wrote={out_dir}")


if __name__ == "__main__":
    main()
