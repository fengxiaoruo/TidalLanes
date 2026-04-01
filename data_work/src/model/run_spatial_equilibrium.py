from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.model.spatial_equilibrium import (
    CalibratedParameters,
    clone_model_with_od_subset,
    compute_soft_shortest_path_assignment,
    estimate_lambda_cross_section,
    estimate_theta_two_way_fe,
    invert_fundamentals,
    load_model_inputs,
    pick_top_congested_edges,
    reallocate_tidal_lanes,
    save_calibration_bundle,
    solve_congested_equilibrium,
    summarise_equilibrium,
)


def parse_args():
    parser = argparse.ArgumentParser(description="Run structural spatial equilibrium prototype.")
    parser.add_argument("--version-id", default="raw_rebuild_validation")
    parser.add_argument("--output-dir", default="data_work/outputs")
    parser.add_argument("--grid-type", default="square", choices=["square", "hex", "voronoi"])
    parser.add_argument("--theta", type=float, default=6.83)
    parser.add_argument("--alpha", type=float, default=-0.12)
    parser.add_argument("--beta", type=float, default=-0.10)
    parser.add_argument("--lambda-congestion", type=float, default=0.15)
    parser.add_argument("--use-estimated-lambda", action="store_true")
    parser.add_argument("--top-n", type=int, default=5)
    parser.add_argument("--add-lane", type=float, default=1.0)
    parser.add_argument("--model-output-subdir", default="model_square_baseline")
    parser.add_argument("--max-iter", type=int, default=100)
    parser.add_argument("--damping", type=float, default=0.35)
    return parser.parse_args()


def main():
    args = parse_args()
    version_root = Path(args.output_dir) / args.version_id
    model_root = version_root / args.model_output_subdir
    model_root.mkdir(parents=True, exist_ok=True)

    model = load_model_inputs(version_root, grid_type=args.grid_type)

    baseline_tau = model.edge_t_obs_min.copy()
    tau_support_obs, tau_invtheta_obs, edge_flow_obs = compute_soft_shortest_path_assignment(
        model.n_nodes,
        model.edge_i,
        model.edge_j,
        baseline_tau,
        model.od_origin,
        model.od_dest,
        theta_route=args.theta,
        od_flow=model.od_pop_obs,
    )
    keep_support = np.isfinite(tau_support_obs) & (tau_support_obs > 0)
    model = clone_model_with_od_subset(model, keep_support)
    tau_support_obs, tau_invtheta_obs, edge_flow_obs = compute_soft_shortest_path_assignment(
        model.n_nodes,
        model.edge_i,
        model.edge_j,
        baseline_tau,
        model.od_origin,
        model.od_dest,
        theta_route=args.theta,
        od_flow=model.od_pop_obs,
    )
    theta_fit = estimate_theta_two_way_fe(
        model.od_origin,
        model.od_dest,
        model.od_pop_obs,
        tau_support_obs,
    )
    if edge_flow_obs is None:
        edge_flow_obs = np.zeros(model.n_edges, dtype=float)
    lambda_fit = estimate_lambda_cross_section(
        model.edge_t_obs_min,
        model.edge_t_ff_min,
        model.edge_lane_obs,
        edge_flow_obs,
    )

    lambda_used = args.lambda_congestion
    lambda_source = "external_default"
    if args.use_estimated_lambda and np.isfinite(lambda_fit.get("lambda_hat", np.nan)):
        lambda_used = float(lambda_fit["lambda_hat"])
        lambda_source = "estimated_cross_section"

    params = CalibratedParameters(
        theta=args.theta,
        alpha=args.alpha,
        beta=args.beta,
        lambda_congestion=lambda_used,
        theta_source="external_default",
        lambda_source=lambda_source,
    )

    fundamentals = invert_fundamentals(
        tau_invtheta_support=tau_invtheta_obs,
        od_origin=model.od_origin,
        od_dest=model.od_dest,
        residents_obs=model.residents_obs,
        jobs_obs=model.jobs_obs,
        theta=params.theta,
        alpha=params.alpha,
        beta=params.beta,
    )

    pd.DataFrame(
        {
            "grid_id": model.node_ids,
            "ubar_theta": fundamentals.ubar_theta,
            "abar_theta": fundamentals.abar_theta,
            "residents_obs": model.residents_obs,
            "jobs_obs": model.jobs_obs,
        }
    ).to_csv(model_root / "fundamentals_inverted.csv", index=False)

    baseline_eq = solve_congested_equilibrium(
        model=model,
        params=params,
        fundamentals=fundamentals,
        max_iter=args.max_iter,
        damping=args.damping,
    )
    treated_idx = pick_top_congested_edges(model, baseline_eq, top_n=args.top_n)
    edge_lane_cf = reallocate_tidal_lanes(model, treated_idx, add_lane=args.add_lane)
    counterfactual_eq = solve_congested_equilibrium(
        model=model,
        params=params,
        fundamentals=fundamentals,
        edge_lane_cf=edge_lane_cf,
        max_iter=args.max_iter,
        damping=args.damping,
    )

    summary = pd.concat(
        [
            summarise_equilibrium(model, baseline_eq, "baseline"),
            summarise_equilibrium(model, counterfactual_eq, f"tidal_top_{args.top_n}"),
        ],
        ignore_index=True,
    )
    summary["delta_welfare_pct"] = summary["welfare"].pct_change() * 100.0
    summary["delta_avg_commute_min"] = summary["weighted_avg_commute_time_min"].diff()
    summary.to_csv(model_root / "equilibrium_summary.csv", index=False)

    pd.DataFrame(
        {
            "edge_idx": np.arange(model.n_edges),
            "i": model.edge_i,
            "j": model.edge_j,
            "lane_obs": model.edge_lane_obs,
            "lane_cf": edge_lane_cf,
            "t_obs_min": model.edge_t_obs_min,
            "t_ff_min": model.edge_t_ff_min,
            "t_baseline_eq_min": baseline_eq.travel_time_min,
            "t_cf_eq_min": counterfactual_eq.travel_time_min,
            "flow_obs_proxy": edge_flow_obs,
            "flow_baseline_eq": baseline_eq.edge_flow,
            "flow_cf_eq": counterfactual_eq.edge_flow,
            "treated": np.isin(np.arange(model.n_edges), treated_idx).astype(int),
        }
    ).to_csv(model_root / "edge_counterfactual_results.csv", index=False)

    pd.DataFrame(
        {
            "grid_id": model.node_ids,
            "residents_obs": model.residents_obs,
            "jobs_obs": model.jobs_obs,
            "residents_baseline_eq": baseline_eq.residents,
            "jobs_baseline_eq": baseline_eq.jobs,
            "residents_cf_eq": counterfactual_eq.residents,
            "jobs_cf_eq": counterfactual_eq.jobs,
        }
    ).to_csv(model_root / "node_counterfactual_results.csv", index=False)

    save_calibration_bundle(model_root, params, theta_fit, lambda_fit)
    print(f"[model] completed -> {model_root}")


if __name__ == "__main__":
    main()
