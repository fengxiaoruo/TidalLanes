"""
Summarize route-bind QC pass rates from a crawl session results_flat.csv.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.national_tidal_commuting.common import ensure_version_dirs, get_version_root, load_config

SH_TZ = ZoneInfo("Asia/Shanghai")

QC_LO, QC_HI = 0.6, 1.8
QC_STRICT_LO, QC_STRICT_HI = 0.8, 1.5


def parse_args():
    parser = argparse.ArgumentParser(description="Report QC pass rates for a crawl sample.")
    parser.add_argument("--config", required=True)
    parser.add_argument("--version-id", default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--session-dir", required=True, help="Path to crawl_sessions/<run_id>/")
    parser.add_argument("--label", default="", help="Optional label for output folder name.")
    return parser.parse_args()


def ratio_stats(ratio: pd.Series) -> dict:
    r = pd.to_numeric(ratio, errors="coerce").dropna()
    if r.empty:
        return {"n": 0}
    return {
        "n": int(len(r)),
        "median": float(r.median()),
        "p10": float(r.quantile(0.1)),
        "p90": float(r.quantile(0.9)),
        "share_qc_loose": float(((r >= QC_LO) & (r <= QC_HI)).mean()),
        "share_qc_strict": float(((r >= QC_STRICT_LO) & (r <= QC_STRICT_HI)).mean()),
        "share_le_2": float((r <= 2.0).mean()),
    }


def main():
    args = parse_args()
    config = load_config(args.config)
    version_root = get_version_root(config, args.version_id, args.output_dir)
    dirs = ensure_version_dirs(version_root)

    session_dir = Path(args.session_dir)
    if not session_dir.is_absolute():
        session_dir = version_root / session_dir
    flat_csv = session_dir / "results_flat.csv"
    if not flat_csv.exists():
        raise FileNotFoundError(f"Missing {flat_csv}")

    df = pd.read_csv(flat_csv)
    df["length_m"] = pd.to_numeric(df["length_m"], errors="coerce")
    df["distance_m"] = pd.to_numeric(df["distance_m"], errors="coerce")
    df["api_status"] = pd.to_numeric(df["api_status"], errors="coerce")
    ok = df["api_status"].eq(0) & df["distance_m"].notna() & df["length_m"].gt(0)
    df["distance_over_length"] = np.where(ok, df["distance_m"] / df["length_m"], np.nan)
    df["route_bind_ok_loose"] = ok & df["distance_over_length"].between(QC_LO, QC_HI)
    df["route_bind_ok_strict"] = ok & df["distance_over_length"].between(QC_STRICT_LO, QC_STRICT_HI)

    stamp = datetime.now(SH_TZ).strftime("%Y%m%dT%H%M%S")
    label = args.label or session_dir.name
    out_dir = dirs["metrics"] / f"qc_sample_{label}_{stamp}"
    out_dir.mkdir(parents=True, exist_ok=True)

    overall = ratio_stats(df.loc[ok, "distance_over_length"])
    by_len = []
    if ok.any():
        tmp = df.loc[ok].copy()
        tmp["length_bin"] = pd.qcut(tmp["length_m"], q=4, duplicates="drop")
        for lb, sub in tmp.groupby("length_bin", observed=True):
            st = ratio_stats(sub["distance_over_length"])
            st["length_bin"] = str(lb)
            by_len.append(st)
    by_wp = []
    if ok.any():
        tmp = df.loc[ok].copy()
        tmp["n_waypoints"] = pd.to_numeric(tmp["n_waypoints"], errors="coerce").fillna(0)
        for nwp, sub in tmp.groupby(tmp["n_waypoints"].clip(upper=10)):
            st = ratio_stats(sub["distance_over_length"])
            st["n_waypoints"] = int(nwp)
            by_wp.append(st)

    summary = {
        "session_dir": str(session_dir),
        "n_requested": int(len(df)),
        "n_api_ok": int(ok.sum()),
        "api_success_rate": float(ok.mean()) if len(df) else 0.0,
        "qc_loose": [QC_LO, QC_HI],
        "qc_strict": [QC_STRICT_LO, QC_STRICT_HI],
        "overall": overall,
        "projected_qc_pass_loose_on_inferred_wp_pool": None,
    }

    manifest_path = version_root / "data" / "crawl_manifest.parquet"
    if manifest_path.exists():
        m = pd.read_parquet(manifest_path)
        pool = m[
            m["has_inferred_speed"].astype(bool)
            & pd.to_numeric(m["n_waypoints"], errors="coerce").fillna(0).gt(0)
        ]
        summary["pool_n_inferred_with_waypoints"] = int(len(pool))
        if overall.get("n"):
            summary["projected_qc_pass_loose_on_inferred_wp_pool"] = int(
                round(overall["share_qc_loose"] * len(pool))
            )

    df.to_csv(out_dir / "qc_sample_detail.csv", index=False)
    pd.DataFrame(by_len).to_csv(out_dir / "qc_by_length_bin.csv", index=False)
    pd.DataFrame(by_wp).to_csv(out_dir / "qc_by_n_waypoints.csv", index=False)
    (out_dir / "qc_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# QC sample crawl report",
        "",
        f"- Session: `{session_dir}`",
        f"- Generated: {datetime.now(SH_TZ).isoformat(timespec='seconds')}",
        "",
        "## API",
        "",
        f"- Requested: {summary['n_requested']}",
        f"- API ok: {summary['n_api_ok']} ({100*summary['api_success_rate']:.1f}%)",
        "",
        "## Route bind QC (distance / length_m)",
        "",
        f"- Loose QC [{QC_LO}, {QC_HI}]: **{100*overall.get('share_qc_loose', 0):.1f}%** (median ratio={overall.get('median', float('nan')):.3f})",
        f"- Strict QC [{QC_STRICT_LO}, {QC_STRICT_HI}]: **{100*overall.get('share_qc_strict', 0):.1f}%**",
        f"- share ratio ≤ 2: **{100*overall.get('share_le_2', 0):.1f}%**",
        "",
    ]
    if summary.get("projected_qc_pass_loose_on_inferred_wp_pool") is not None:
        lines.extend(
            [
                "## Projection (inferred + waypoints pool)",
                "",
                f"- Pool size: {summary['pool_n_inferred_with_waypoints']:,}",
                f"- Estimated pass loose QC: ~{summary['projected_qc_pass_loose_on_inferred_wp_pool']:,} rows",
                "",
            ]
        )
    lines.append(f"Detail: `{out_dir}/qc_sample_detail.csv`")
    (out_dir / "QC_SAMPLE_REPORT.md").write_text("\n".join(lines), encoding="utf-8")

    print(f"[report_qc_sample] wrote {out_dir / 'QC_SAMPLE_REPORT.md'}")
    print(
        f"[report_qc_sample] API ok {summary['n_api_ok']}/{summary['n_requested']}; "
        f"loose QC pass {100*overall.get('share_qc_loose', 0):.1f}%; "
        f"strict {100*overall.get('share_qc_strict', 0):.1f}%"
    )
    if summary.get("projected_qc_pass_loose_on_inferred_wp_pool") is not None:
        print(
            f"[report_qc_sample] projected ~{summary['projected_qc_pass_loose_on_inferred_wp_pool']:,} / "
            f"{summary['pool_n_inferred_with_waypoints']:,} inferred+wp rows pass loose QC"
        )


if __name__ == "__main__":
    main()
