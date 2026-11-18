"""OD-track entry: estimate theta from Beijing OD and run SMM aggregate moments."""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from src.model.full_estimation import build_primitives as bp  # noqa: E402
from src.model.full_estimation import parameters as pm  # noqa: E402
from src.model.directional_cost import fixed_tau_model as ft  # noqa: E402
from src.model.directional_cost import od_fit_estimation as odf  # noqa: E402


def _parse_grid(text: str) -> list[float]:
    return [float(x.strip()) for x in text.split(",") if x.strip()]


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description="OD-track theta PO gravity + SMM calibration")
    p.add_argument("--repo-root", default=".")
    p.add_argument("--version-id", default="model_input_edgeproj_v2_lcc_gate5_v1")
    p.add_argument("--grid-type", default="square", choices=["square", "hex", "voronoi"])
    p.add_argument("--period", default="AM")
    p.add_argument("--od-pop-col", default="commuters_road")
    p.add_argument(
        "--output-name",
        default="directional_cost_edgeproj_v2_lcc_gate5_v1",
        help="Existing directional_cost output dir to write diagnostics into",
    )
    p.add_argument(
        "--intra-spec",
        default="exclude_diag_plus_proxy_calibrated",
        choices=[
            "baseline",
            "exclude_diag",
            "intrazonal_proxy",
            "exclude_diag_plus_proxy",
            "exclude_diag_plus_proxy_calibrated",
        ],
    )
    p.add_argument("--theta-grid", default="1,2,3,4,5,6,6.83,8,10,12,15")
    p.add_argument("--target-rowsum", type=float, default=0.9)
    p.add_argument(
        "--run-method-comparison",
        action="store_true",
        help="Also run tau_spec x od_weight x intra x theta grid and write od_fit_method_comparison.json",
    )
    p.add_argument("--smm-local-grid", default="2,4,6,8")
    args = p.parse_args(argv)

    repo = Path(args.repo_root).resolve()
    out = repo / "data_work" / "outputs" / args.output_name
    diag = out / "diagnostics"
    diag.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s | %(message)s",
        handlers=[logging.FileHandler(out / "od_fit_calibration.log"), logging.StreamHandler()],
    )
    log = logging.getLogger("od_fit_calibration")

    prim = bp.load_primitives(
        repo / "data_work" / "outputs" / args.version_id,
        grid_type=args.grid_type, period=args.period, od_pop_col=args.od_pop_col,
    )
    params = pm.Parameters()
    tau_sym, _ = ft.build_symmetric_cost(prim, method="geometric")
    kappa = max(
        ft.pick_kappa(prim.edge_i, prim.tau_obs_min, prim.n_nodes, args.target_rowsum),
        ft.pick_kappa(prim.edge_i, tau_sym, prim.n_nodes, args.target_rowsum),
    )
    log.info("N=%s kappa=%.4f placeholder theta=%.2f", prim.n_nodes, kappa, params.theta)

    report = odf.run_theta_and_smm_calibration(
        prim, kappa, params,
        theta_grid=_parse_grid(args.theta_grid),
        intra_spec=args.intra_spec,
    )
    odf.write_json(diag / "od_fit_theta_and_smm.json", report)

    cmp_ = report["comparison"]
    log.info(
        "PO theta hat=%.3f | SMM best theta=%.2f (sse=%.3f)",
        report["po_primary_theta_hat"],
        report["smm_best_theta"],
        report["smm_best_relative_sse"],
    )
    for label in ("baseline_placeholder", "po_theta_hat", "smm_best"):
        row = cmp_[label]
        log.info(
            "%s: theta=%.2f global_L1=%.1f%% off_L1=%.1f%% mean_commute=%.2f min",
            label, row["theta"], row["global_l1_pct"], row["off_diagonal_l1_pct"],
            row["predicted_mean_commute_min"],
        )
    log.info("done -> %s", diag / "od_fit_theta_and_smm.json")

    if args.run_method_comparison:
        log.info("starting method comparison grid (may take several minutes)...")
        comparison = odf.run_method_comparison(
            prim, kappa, params,
            smm_local_grid=_parse_grid(args.smm_local_grid),
        )
        odf.write_json(diag / "od_fit_method_comparison.json", comparison)
        top = comparison["top_10"]
        log.info("method comparison: %d cases, top composite fitness=%.2f", comparison["n_cases"], top[0]["composite_fitness"])
        for i, row in enumerate(top[:5], start=1):
            log.info(
                "#%d fitness=%.2f global_L1=%.1f%% off_L1=%.1f%% | %s",
                i, row["composite_fitness"], row["global_l1_pct"],
                row["off_diagonal_l1_pct"], row["method_id"],
            )
        log.info("done -> %s", diag / "od_fit_method_comparison.json")


if __name__ == "__main__":
    main()
