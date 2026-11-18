"""Phase A — ST metrics: corridor % savings, path exposure, person-min (gate5, top100 mainline)."""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

DATA_WORK = Path(__file__).resolve().parents[3]
if str(DATA_WORK) not in sys.path:
    sys.path.insert(0, str(DATA_WORK))

from src.model.full_estimation.corridor_exposure import (  # noqa: E402
    od_path_exposure,
    slow_direction_od_mass,
)
from src.model.full_estimation.equilibrium_solve import MAINLINE_PAIR_SELECTION  # noqa: E402
from src.model.full_estimation.phase_metrics_common import (  # noqa: E402
    DEFAULT_TOP_K,
    DEFAULT_VERSION,
    data_work_root,
    load_fundamentals,
    tidal_selection,
    write_json,
)

LOG = logging.getLogger("phase_a")


def _corridor_pct_stats(detail: pd.DataFrame, spec: str, horizon: str) -> dict:
    slow = detail[(detail["spec"] == spec) & (detail["horizon"] == horizon) & (detail["role"] == "slow")]
    if slow.empty:
        return {}
    suffix = "lt" if horizon == "long_run" else "st"
    pct_save = -slow["delta_tau_pct"]
    xi = slow["Xi_scenario"].values
    dtau = slow["delta_tau_min"].values
    return {
        f"corridor_slow_median_pct_save_{suffix}": float(pct_save.median()),
        f"corridor_slow_mean_pct_save_{suffix}": float(pct_save.mean()),
        f"corridor_xi_weighted_delta_tau_min_{suffix}": float(np.sum(xi * dtau) / max(np.sum(xi), 1e-12)),
        f"corridor_sum_xi_flow_min_{suffix}": float(np.sum(xi * dtau)),
    }


def _person_min(compact_row: pd.Series, lbar: float) -> dict:
    def pm(sec_col: str) -> float:
        return float(compact_row[sec_col]) / 60.0 * lbar

    return {
        "person_min_day_st_direct": pm("direct_st_cost_change_sec"),
        "person_min_day_sorting_wedge": pm("sorting_wedge_sec"),
        "person_min_day_lt_total": pm("total_lt_cost_change_sec"),
    }


def build_phase_a_table(
    data_work: Path,
    *,
    version_id: str = DEFAULT_VERSION,
    tidal_dir: str = "tidal_lane_diagnostics_edgeproj_v2_lcc_gate5_bpr_pos05_v1",
    calib_dir: str = "tidal_lane_diagnostics_edgeproj_v2_lcc_gate5_p1_calib_v1",
    stage2_dir: str = "stage2_endogenous_edgeproj_v2_lcc_gate5_bpr_pos05_v1",
    specs: list[str] | None = None,
    primary_spec: str = "top100_d1.0",
) -> pd.DataFrame:
    prim, _, _ = load_fundamentals(data_work, version_id)
    lbar = prim.Lbar

    specs = specs or ["top50_d1.0", "top100_d1.0"]
    rows: list[dict] = []

    for label, tdir, lam in (
        ("lambda_1.1", tidal_dir, 1.1),
        ("lambda_0.57", calib_dir, 0.572),
    ):
        if not tdir:
            continue
        troot = data_work / "outputs" / tdir
        if not troot.is_dir():
            LOG.warning("Skip missing tidal dir %s", troot)
            continue
        detail = pd.read_csv(troot / "tidal_treated_edge_detail.csv")
        compact = pd.read_csv(troot / "tidal_st_lt_compact.csv")

        for spec in specs:
            if spec not in compact["spec"].values:
                continue
            crow = compact[compact["spec"] == spec].iloc[0]
            top_k = int(crow["top_k"])
            eq_npz = np.load(
                data_work / "outputs" / stage2_dir / "baseline_endogenous" / "equilibrium_arrays.npz",
            )
            tau_base = eq_npz["tau_min_kl"]
            xi_base = eq_npz["Xi_kl"]
            sel = tidal_selection(
                prim, top_k=top_k, delta=1.0,
                pair_selection=MAINLINE_PAIR_SELECTION, xi_kl=xi_base,
            )
            treated = np.unique(sel.treated_pairs.ravel())
            slow_i = prim.edge_i[sel.treated_pairs[:, 0]]
            slow_j = prim.edge_j[sel.treated_pairs[:, 0]]

            row = {
                "run_label": label,
                "spec": spec,
                "top_k": top_k,
                "n_pairs": int(crow["n_pairs"]),
                "lambda_phys": lam,
                "is_primary": spec == primary_spec and label == "lambda_1.1",
                **_corridor_pct_stats(detail, spec, "long_run"),
                **_corridor_pct_stats(detail, spec, "short_run"),
                **_person_min(crow, lbar),
                "avg_commute_lt_sec": float(crow["total_lt_cost_change_sec"]),
                "avg_commute_st_direct_sec": float(crow["direct_st_cost_change_sec"]),
                "Wbar_pct_change_lt": float(crow["Wbar_pct_change_lt"]),
                "Wbar_pct_change_st": float(crow["Wbar_pct_change_st"]),
                **slow_direction_od_mass(prim, slow_i, slow_j),
                **od_path_exposure(prim, tau_base, treated),
            }
            row["od_path_exposed_share_pct"] = row["od_exposed_share_pct"]
            row["person_min_day_lt_if_pop_growth_25pct"] = row["person_min_day_lt_total"] * 1.25
            row["person_min_day_lt_if_pop_growth_50pct"] = row["person_min_day_lt_total"] * 1.5
            rows.append(row)

    return pd.DataFrame(rows)


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo-root", default=str(DATA_WORK))
    p.add_argument("--version-id", default=DEFAULT_VERSION)
    p.add_argument("--output-name", default="phase_a_st_metrics_gate5_bpr_pos05_v1")
    p.add_argument("--tidal-dir", default="tidal_lane_diagnostics_edgeproj_v2_lcc_gate5_bpr_pos05_v1")
    p.add_argument("--stage2-dir", default="stage2_endogenous_edgeproj_v2_lcc_gate5_bpr_pos05_v1")
    p.add_argument(
        "--calib-dir",
        default="tidal_lane_diagnostics_edgeproj_v2_lcc_gate5_p1_calib_v1",
        help="lambda_0.57 robustness tidal dir; pass '' to skip the row",
    )
    p.add_argument("--primary-spec", default="top100_d1.0")
    args = p.parse_args(argv)

    data_work = data_work_root(args.repo_root)
    out = data_work / "outputs" / args.output_name
    out.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s | %(message)s")

    df = build_phase_a_table(
        data_work,
        version_id=args.version_id,
        tidal_dir=args.tidal_dir,
        calib_dir=args.calib_dir,
        stage2_dir=args.stage2_dir,
        primary_spec=args.primary_spec,
    )
    df.to_csv(out / "phase_a_st_metrics.csv", index=False)

    primary = df[df["is_primary"]].iloc[0] if df["is_primary"].any() else df.iloc[0]
    summary = {
        "primary_spec": args.primary_spec,
        "Lbar": float(load_fundamentals(data_work, args.version_id)[0].Lbar),
        "corridor_slow_median_pct_save_lt": float(primary.get("corridor_slow_median_pct_save_lt", np.nan)),
        "od_path_exposed_share_pct": float(primary.get("od_path_exposed_share_pct", primary.get("od_exposed_share_pct", np.nan))),
        "person_min_day_lt_total": float(primary["person_min_day_lt_total"]),
        "person_min_day_st_direct": float(primary["person_min_day_st_direct"]),
    }
    write_json(out / "summary.json", summary)
    LOG.info("Wrote %s (%d rows)", out, len(df))
    cols = ["run_label", "spec", "corridor_slow_median_pct_save_lt", "od_exposed_share_pct", "person_min_day_lt_total"]
    print(df[cols].to_string(index=False))


if __name__ == "__main__":
    main()
