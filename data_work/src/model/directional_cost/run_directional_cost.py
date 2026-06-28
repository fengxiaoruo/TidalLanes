"""Stage-1 fixed-cost model: directed vs symmetric worlds (re-invert fundamentals)."""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from src.model.full_estimation import build_primitives as bp  # noqa: E402
from src.model.full_estimation import parameters as pm  # noqa: E402
from src.model.full_estimation import invert_fundamentals as inv  # noqa: E402
from src.model.directional_cost import fixed_tau_model as ft  # noqa: E402


def _parse_grid(text: str) -> list[float]:
    return [float(x.strip()) for x in text.split(",") if x.strip()]


def _write_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, default=float))


def _kappa(prim, tau_edge: np.ndarray, target_rowsum: float) -> float:
    return max(
        ft.pick_kappa(prim.edge_i, prim.tau_obs_min, prim.n_nodes, target_rowsum),
        ft.pick_kappa(prim.edge_i, tau_edge, prim.n_nodes, target_rowsum),
    )


def _invert_and_solve(prim, params, tau, kappa, label, inv_kw, eq_kw):
    fund = inv.invert_fundamentals(prim, params, kappa=kappa, tau_min_kl=tau, **inv_kw)
    eq = ft.solve_static_equilibrium(
        prim, tau, fund.ubar_theta, fund.abar_theta, fund.chi, params,
        kappa=kappa, label=label, **eq_kw,
    )
    return fund, eq


def main(argv=None) -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--repo-root", default=".")
    p.add_argument("--version-id", default="model_input_edgeproj_v2_lcc_gate5_v1")
    p.add_argument("--grid-type", default="square", choices=["square", "hex", "voronoi"])
    p.add_argument("--period", default="AM")
    p.add_argument("--od-pop-col", default="commuters_road")
    p.add_argument("--output-name", default="directional_cost_edgeproj_v2_lcc_gate5_v1")
    p.add_argument("--sym-method", default="geometric",
                   choices=["geometric", "arithmetic", "speed_arithmetic", "spatial_mean"])
    p.add_argument("--target-rowsum", type=float, default=0.9)
    p.add_argument("--inversion-max-iter", type=int, default=1800)
    p.add_argument("--inversion-tol", type=float, default=1.5e-4)
    p.add_argument("--static-max-iter", type=int, default=1200)
    p.add_argument("--static-tol", type=float, default=1e-9)
    p.add_argument("--intensity-grid", default="0,0.25,0.5,0.75,1")
    p.add_argument("--threshold-grid", default="1,1.05,1.10,1.25,1.50,2.00")
    p.add_argument("--top-od", type=int, default=500)
    args = p.parse_args(argv)

    repo = Path(args.repo_root).resolve()
    out = repo / "data_work" / "outputs" / args.output_name
    diag = out / "diagnostics"
    sim = out / "simulations"
    for d in (out, diag, sim, out / "fundamentals", out / "equilibrium"):
        d.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s | %(message)s",
        handlers=[logging.FileHandler(out / "run.log"), logging.StreamHandler()],
    )
    log = logging.getLogger("directional_cost")

    prim = bp.load_primitives(
        repo / "data_work" / "outputs" / args.version_id,
        grid_type=args.grid_type, period=args.period, od_pop_col=args.od_pop_col,
    )
    bp.write_primitives_summary(prim, out / "primitives")
    params = pm.Parameters()
    params.write(out / "parameters.json")
    log.info("N=%s edges=%s Lbar=%.1f", prim.n_nodes, prim.n_edges, prim.Lbar)

    tau_sym, pair_df = ft.build_symmetric_cost(prim, method=args.sym_method)
    edge_df = pair_df.attrs["edge_df"]
    pair_df.to_csv(diag / "reciprocal_pair_asymmetry.csv", index=False)
    edge_df.to_csv(diag / "edge_cost_comparison.csv", index=False)

    kappa = _kappa(prim, tau_sym, args.target_rowsum)
    log.info("kappa=%.6f", kappa)

    inv_kw = {"max_iter": args.inversion_max_iter, "tol": args.inversion_tol}
    eq_kw = {
        "max_iter": args.static_max_iter, "tol": args.static_tol,
        "initial_l_R": None, "initial_l_F": None,
    }
    l_R_obs, l_F_obs, valid = ft.observed_shares(prim)
    eq_kw["initial_l_R"], eq_kw["initial_l_F"] = l_R_obs, l_F_obs

    fund_dir, directed = _invert_and_solve(
        prim, params, prim.tau_obs_min, kappa, "directed_observed", inv_kw, eq_kw,
    )
    fund_sym, symmetric = _invert_and_solve(
        prim, params, tau_sym, kappa, "symmetric_cost", inv_kw, eq_kw,
    )
    fund_dir.write(out / "fundamentals" / "directed")
    fund_sym.write(out / "fundamentals" / "symmetric")
    directed.write(out / "equilibrium" / "directed_observed")
    symmetric.write(out / "equilibrium" / "symmetric_cost")
    log.info("directed inversion iter=%s chi=%.3e", fund_dir.n_iter, fund_dir.chi)
    log.info("symmetric inversion iter=%s chi=%.3e", fund_sym.n_iter, fund_sym.chi)

    summary = ft.comparison_summary(
        directed, symmetric, prim, params,
        abar_theta=fund_dir.abar_theta, abar_theta_scenario=fund_sym.abar_theta,
    )
    summary.update({
        "interpretation": "symmetric_cost relative to directed_observed",
        "sym_method": args.sym_method,
        "comparison_mode": "reinvert_fundamentals",
        **ft.fundamentals_comparison_summary(fund_dir, fund_sym, valid),
    })
    _write_json(out / "summary_metrics.json", summary)

    ft.node_comparison_frame(prim, directed, symmetric).to_csv(
        diag / "node_directed_vs_symmetric.csv", index=False,
    )
    ft.top_od_changes_frame(
        prim, directed, symmetric, fund_dir.abar_theta, params, top_k=args.top_od,
    ).to_csv(diag / "top_od_flow_changes.csv", index=False)

    fit = {
        "comparison_mode": "reinvert_fundamentals",
        "directed_vs_floored_observed_residence_l1_pct": 100 * 0.5 * float(np.sum(np.abs(directed.l_R - l_R_obs))),
        "directed_vs_floored_observed_workplace_l1_pct": 100 * 0.5 * float(np.sum(np.abs(directed.l_F - l_F_obs))),
        "symmetric_vs_floored_observed_residence_l1_pct": 100 * 0.5 * float(np.sum(np.abs(symmetric.l_R - l_R_obs))),
        "symmetric_vs_floored_observed_workplace_l1_pct": 100 * 0.5 * float(np.sum(np.abs(symmetric.l_F - l_F_obs))),
    }
    _write_json(diag / "equilibrium_fit_to_observed.json", fit)
    _write_json(diag / "directed_fit_to_observed.json", fit)

    reciprocal = edge_df[edge_df["has_reverse"]]
    _write_json(diag / "asymmetry_summary.json", {
        "n_edges": int(prim.n_edges),
        "n_reciprocal_pairs": int(len(pair_df)),
        "pair_tau_ratio_p50": float(pair_df["tau_ratio"].quantile(0.50)),
        "pair_tau_ratio_p90": float(pair_df["tau_ratio"].quantile(0.90)),
        "share_pairs_tau_ratio_ge_1_10": float((pair_df["tau_ratio"] >= 1.10).mean()),
        "share_pairs_tau_ratio_ge_2_00": float((pair_df["tau_ratio"] >= 2.00).mean()),
    })

    # Intensity: re-invert at each blended cost surface.
    intensity_rows = []
    intensity_eq = {}
    for rho in _parse_grid(args.intensity_grid):
        tau_rho = ft.tau_from_asymmetry_intensity(tau_sym, prim.tau_obs_min, rho)
        _, eq_rho = _invert_and_solve(
            prim, params, tau_rho, kappa, f"intensity_{rho:.2f}", inv_kw, eq_kw,
        )
        intensity_eq[rho] = eq_rho
    base = intensity_eq[min(intensity_eq)]
    for rho, eq_rho in intensity_eq.items():
        row = ft.comparison_summary(base, eq_rho, prim, params)
        row["asymmetry_intensity"] = rho
        intensity_rows.append(row)
    pd.DataFrame(intensity_rows).to_csv(sim / "asymmetry_intensity.csv", index=False)

    # Threshold: compare directed-leaning cost to full symmetric world.
    pair_abs = edge_df["pair_abs_log_tau_ratio"].to_numpy()
    has_reverse = edge_df["has_reverse"].to_numpy(dtype=bool)
    threshold_rows = []
    for c in _parse_grid(args.threshold_grid):
        tau_c = ft.tau_from_asymmetry_threshold(tau_sym, prim.tau_obs_min, pair_abs, c)
        _, eq_c = _invert_and_solve(
            prim, params, tau_c, kappa, f"threshold_{c:.2f}", inv_kw, eq_kw,
        )
        row = ft.comparison_summary(symmetric, eq_c, prim, params)
        row["threshold_tau_ratio"] = c
        row["share_reciprocal_edges_kept_directed"] = float(
            ((pair_abs >= np.log(c)) & has_reverse).sum() / max(1, has_reverse.sum())
        )
        threshold_rows.append(row)
    pd.DataFrame(threshold_rows).to_csv(sim / "asymmetry_threshold.csv", index=False)

    _write_json(out / "run_metadata.json", {
        "version_id": args.version_id,
        "output_name": args.output_name,
        "sym_method": args.sym_method,
        "comparison_mode": "reinvert_fundamentals",
        "kappa": kappa,
        "fund_dir_iter": fund_dir.n_iter,
        "fund_sym_iter": fund_sym.n_iter,
        "parameters": params.to_dict(),
    })
    log.info("done -> %s", out)


if __name__ == "__main__":
    main()
