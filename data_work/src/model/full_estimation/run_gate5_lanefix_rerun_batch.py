"""Batch rerun gate5 full_estimation after lane-fix travel cost (dual alpha tracks).

See Documents/L2_notes/gate5_lanefix_rerun_plan_20260629.md
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import replace
from pathlib import Path

import numpy as np
import pandas as pd

DATA_WORK = Path(__file__).resolve().parents[3]
if str(DATA_WORK) not in sys.path:
    sys.path.insert(0, str(DATA_WORK))

from src.model.full_estimation import parameters as pm  # noqa: E402
from src.model.full_estimation.equilibrium_solve import (  # noqa: E402
    MAINLINE_CONGESTION_FORM,
    MAINLINE_LAMBDA_PHYS,
    MAINLINE_PAIR_SELECTION,
)
from src.model.full_estimation.run_alpha_robustness_quick_gate5 import (  # noqa: E402
    solver_overrides_for_params,
)
from src.model.full_estimation.run_phase_a_st_metrics_gate5 import (  # noqa: E402
    build_phase_a_table,
)
from src.model.full_estimation.run_stage2_gate5 import run_pipeline  # noqa: E402
from src.model.full_estimation.run_tidal_lane_diagnostics_gate5 import run as run_tidal  # noqa: E402

LOG = logging.getLogger("gate5_lanefix_rerun")

VERSION_ID = "model_input_edgeproj_v2_lcc_gate5_v1"
COMPARISON_DIR = "gate5_lanefix_rerun_comparison_v1"

PARAM_TRACKS = [
    {
        "key": "neg",
        "label": "alpha_neg_seattle",
        "alpha": -0.12,
        "beta": -0.10,
        "alpha_source": "AA(2022) Seattle net workplace externality",
        "beta_source": "AA(2022) Seattle",
    },
    {
        "key": "pos05",
        "label": "alpha_pos_0.05",
        "alpha": 0.05,
        "beta": -0.10,
        "alpha_source": "Literature mid-range gross production spillover (lane-fix mainline)",
        "beta_source": "AA(2022) Seattle beta held",
    },
]


def _data_work_root(arg: str) -> Path:
    p = Path(arg).resolve()
    if (p / "outputs").is_dir() and (p / "src" / "model").is_dir():
        return p
    if (p / "data_work" / "outputs").is_dir():
        return p / "data_work"
    return DATA_WORK


def _make_params(track: dict) -> pm.Parameters:
    base = pm.Parameters()
    return replace(
        base,
        alpha=float(track["alpha"]),
        beta=float(track["beta"]),
        alpha_source=track["alpha_source"],
        beta_source=track["beta_source"],
    )


def _output_names(key: str) -> dict[str, str]:
    return {
        "stage2": f"stage2_endogenous_edgeproj_v2_lcc_gate5_anchored_lanefix_{key}_v1",
        "tidal": f"tidal_lane_diagnostics_edgeproj_v2_lcc_gate5_lanefix_{key}_v1",
        "phase_a": f"phase_a_st_metrics_gate5_lanefix_{key}_v1",
    }


def _solver_kw(params: pm.Parameters) -> dict:
    kw = solver_overrides_for_params(params)
    return {k: v for k, v in kw.items() if v is not None}


def run_track(
    data_work: Path,
    track: dict,
    *,
    quick: bool,
    skip_stage2: bool,
    skip_tidal: bool,
    skip_phase_a: bool,
) -> dict:
    key = track["key"]
    names = _output_names(key)
    params = _make_params(track)
    skw = _solver_kw(params)

    meta = {"track": track, "outputs": names, "solver": skw}

    if not skip_stage2:
        LOG.info("=== Stage2 track=%s alpha=%.2f beta=%.2f ===", key, params.alpha, params.beta)
        out = data_work / "outputs" / names["stage2"]
        out.mkdir(parents=True, exist_ok=True)
        stage2_meta = run_pipeline(
            data_work,
            VERSION_ID,
            out,
            params,
            quick=quick,
            skip_tidal_grid=quick,
            congestion_form=MAINLINE_CONGESTION_FORM,
            congestion_lambda=MAINLINE_LAMBDA_PHYS,
            pair_selection=MAINLINE_PAIR_SELECTION,
            skw_extra=skw,
        )
        meta["stage2"] = stage2_meta

    if not skip_tidal:
        LOG.info("=== Tidal diagnostics track=%s ===", key)
        tout = data_work / "outputs" / names["tidal"]
        tout.mkdir(parents=True, exist_ok=True)
        specs = [(25, 1.0), (100, 1.0)] if quick else [(25, 1.0), (50, 1.0), (50, 0.5), (100, 1.0)]
        tidal_df = run_tidal(
            data_work,
            VERSION_ID,
            tout,
            congestion_form=MAINLINE_CONGESTION_FORM,
            congestion_lambda=MAINLINE_LAMBDA_PHYS,
            pair_selection=MAINLINE_PAIR_SELECTION,
            specs=specs,
            params=params,
            **skw,
        )
        meta["tidal_rows"] = len(tidal_df)

    if not skip_phase_a:
        LOG.info("=== Phase A track=%s ===", key)
        pa_out = data_work / "outputs" / names["phase_a"]
        pa_out.mkdir(parents=True, exist_ok=True)
        pa_df = build_phase_a_table(
            data_work,
            version_id=VERSION_ID,
            tidal_dir=names["tidal"],
            stage2_dir=names["stage2"],
            calib_dir=names["tidal"],  # skip lambda=0.57 unless present
            specs=["top100_d1.0"] if quick else ["top50_d1.0", "top100_d1.0"],
            primary_spec="top100_d1.0",
        )
        pa_df["alpha"] = params.alpha
        pa_df["beta"] = params.beta
        pa_df["track_key"] = key
        pa_df.to_csv(pa_out / "phase_a_st_metrics.csv", index=False)
        meta["phase_a_rows"] = len(pa_df)

    return meta


def build_comparison_table(data_work: Path, tracks: list[dict]) -> pd.DataFrame:
    from src.model.full_estimation import build_primitives as bp

    prim = bp.load_primitives(data_work / "outputs" / VERSION_ID, period="AM")
    lbar = prim.Lbar
    rows = []
    for track in tracks:
        key = track["key"]
        names = _output_names(key)
        stage2_summary = data_work / "outputs" / names["stage2"] / "baseline_summary.json"
        tidal_compact = data_work / "outputs" / names["tidal"] / "tidal_st_lt_compact.csv"
        if not stage2_summary.exists() or not tidal_compact.exists():
            LOG.warning("Skip comparison for %s — outputs missing", key)
            continue
        base = json.loads(stage2_summary.read_text())
        tdf = pd.read_csv(tidal_compact)
        top100 = tdf[tdf["spec"] == "top100_d1.0"]
        if top100.empty:
            top100 = tdf.iloc[[-1]]
        tr = top100.iloc[0]
        rows.append({
            "track_key": key,
            "alpha": track["alpha"],
            "beta": track["beta"],
            "baseline_Wbar": base["baseline_endogenous"]["Wbar"],
            "baseline_converged": base["baseline_endogenous"]["converged"],
            "median_tau_ratio_vs_obs": base["baseline_endogenous"].get("median_tau_ratio_vs_obs"),
            "Wbar_pct_change_st": tr["Wbar_pct_change_st"],
            "Wbar_pct_change_lt": tr["Wbar_pct_change_lt"],
            "direct_st_cost_change_sec": tr["direct_st_cost_change_sec"],
            "sorting_wedge_sec": tr["sorting_wedge_sec"],
            "total_lt_cost_change_sec": tr["total_lt_cost_change_sec"],
            "person_min_day_lt_total": tr["total_lt_cost_change_sec"] / 60.0 * lbar,
            "stage2_dir": names["stage2"],
            "tidal_dir": names["tidal"],
        })
    return pd.DataFrame(rows)


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo-root", default=str(DATA_WORK))
    p.add_argument("--only", choices=("neg", "pos05"), default=None)
    p.add_argument("--quick", action="store_true", help="top25+top100 only; skip exog BPR grid extras")
    p.add_argument("--skip-stage2", action="store_true")
    p.add_argument("--skip-tidal", action="store_true")
    p.add_argument("--skip-phase-a", action="store_true")
    p.add_argument("--comparison-only", action="store_true")
    args = p.parse_args(argv)

    data_work = _data_work_root(args.repo_root)
    comp_out = data_work / "outputs" / COMPARISON_DIR
    comp_out.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(comp_out / "run.log"),
            logging.StreamHandler(),
        ],
    )

    tracks = [t for t in PARAM_TRACKS if args.only is None or t["key"] == args.only]

    if not args.comparison_only:
        for track in tracks:
            run_track(
                data_work,
                track,
                quick=args.quick,
                skip_stage2=args.skip_stage2,
                skip_tidal=args.skip_tidal,
                skip_phase_a=args.skip_phase_a,
            )

    df = build_comparison_table(data_work, PARAM_TRACKS)
    df.to_csv(comp_out / "alpha_tracks_comparison.csv", index=False)
    write_plan = {
        "version_id": VERSION_ID,
        "lane_fix": True,
        "param_tracks": PARAM_TRACKS,
        "comparison_csv": str(comp_out / "alpha_tracks_comparison.csv"),
    }
    (comp_out / "rerun_meta.json").write_text(json.dumps(write_plan, indent=2, default=float))
    LOG.info("Comparison table (%d rows):\n%s", len(df), df.to_string(index=False) if len(df) else "(empty)")
    print(df.to_string(index=False) if len(df) else "No comparison rows yet.")


if __name__ == "__main__":
    main()
