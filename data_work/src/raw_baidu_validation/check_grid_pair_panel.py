"""
Quick check of one grid-pair Baidu crawl session (nextstep D step 4)
====================================================================
Reads the newest crawl session for --panel and --label under
outputs/<version>/api/crawl_sessions/, and appends one JSON line to
outputs/<version>/logs/panel_checks.jsonl with: rows, API status counts, success rate,
median Baidu duration and distance, and (on success rows) the correlation of log Baidu
minutes with the 1B and gate5 model times for the matching panel.
Exit code 3 when the success rate is below --min-success (the week runner then stops).
Reads crawl outputs only; writes only the check log.
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
MODEL_COL = {"FREE": ("tm_FF_osm1b", "tau_ff_gate5"), "AM": ("tm_AM_osm1b", "tau_obs_gate5"), "PM": ("tm_PM_osm1b", None)}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--version-id", required=True)
    ap.add_argument("--panel", required=True, choices=list(MODEL_COL))
    ap.add_argument("--label", required=True)
    ap.add_argument("--min-success", type=float, default=0.5)
    a = ap.parse_args(argv)
    v = ROOT / "outputs" / a.version_id
    sess = sorted((v / "api/crawl_sessions").glob(f"{a.panel}_*_{a.label}"))
    rec = {"checked_at": datetime.now(ZoneInfo("Asia/Shanghai")).isoformat(timespec="seconds"),
           "panel": a.panel, "label": a.label}
    if not sess:
        rec["error"] = "no session found"
        ok = False
    else:
        r = pd.read_csv(sess[-1] / "results_flat.csv")
        m = pd.read_csv(v / "data/sample_manifest.csv")
        rec.update(session=sess[-1].name, rows=int(len(r)), rows_expected=int(len(m)),
                   status_counts={str(k): int(n) for k, n in r.api_status.value_counts().items()})
        good = r[(r.api_status == 0) & r.duration_sec.notna()]
        rec["success_rate"] = round(len(good) / max(len(m), 1), 4)
        if len(good):
            rec["median_duration_min"] = round(float(good.duration_sec.median()) / 60, 2)
            rec["median_distance_km"] = round(float(good.distance_m.median()) / 1000, 2)
            j = good.merge(m, on="request_id", suffixes=("", "_m"))
            lb = np.log(j.duration_sec / 60)
            for nm, col in zip(("corr_log_osm1b", "corr_log_gate5"), MODEL_COL[a.panel]):
                if col and col in j:
                    x = np.log(j[col].where(j[col] > 0))
                    ok_ = x.notna() & np.isfinite(lb)
                    rec[nm] = round(float(np.corrcoef(lb[ok_], x[ok_])[0, 1]), 3) if ok_.sum() > 10 else None
        ok = rec["success_rate"] >= a.min_success
    rec["pass"] = bool(ok)
    (v / "logs").mkdir(parents=True, exist_ok=True)
    with (v / "logs/panel_checks.jsonl").open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    print(json.dumps(rec, ensure_ascii=False))
    return 0 if ok else 3


if __name__ == "__main__":
    sys.exit(main())
