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
    build_symmetric_edge_times,
    clone_model_with_od_subset,
    compute_soft_shortest_path_assignment,
    invert_fundamentals,
    load_model_inputs,
    pick_top_congested_edges,
    pick_top_tidal_edges,
    reallocate_tidal_lanes,
    solve_congested_equilibrium,
    summarise_equilibrium,
)


def parse_args():
    parser = argparse.ArgumentParser(description="Run structural counterfactual suite.")
    parser.add_argument("--version-id", default="raw_rebuild_validation")
    parser.add_argument("--output-dir", default="data_work/outputs")
    parser.add_argument("--grid-type", default="square", choices=["square", "hex", "voronoi"])
    parser.add_argument("--theta", type=float, default=6.83)
    parser.add_argument("--alpha", type=float, default=-0.12)
    parser.add_argument("--beta", type=float, default=-0.10)
    parser.add_argument("--lambda-list", default="0.05,0.15,0.30")
    parser.add_argument("--topn-list", default="5,10")
    parser.add_argument("--add-lane", type=float, default=1.0)
    parser.add_argument("--model-output-subdir", default="model_square_suite")
    parser.add_argument("--max-iter", type=int, default=60)
    parser.add_argument("--damping", type=float, default=0.35)
    return parser.parse_args()


def _parse_num_list(text: str, cast):
    return [cast(x.strip()) for x in text.split(",") if x.strip()]


def main():
    args = parse_args()
    version_root = Path(args.output_dir) / args.version_id
    model_root = version_root / args.model_output_subdir
    model_root.mkdir(parents=True, exist_ok=True)

    model_full = load_model_inputs(version_root, grid_type=args.grid_type)
    tau_obs, tau_invtheta_obs, _ = compute_soft_shortest_path_assignment(
        model_full.n_nodes,
        model_full.edge_i,
        model_full.edge_j,
        model_full.edge_t_obs_min,
        model_full.od_origin,
        model_full.od_dest,
        theta_route=args.theta,
    )
    keep = np.isfinite(tau_obs) & (tau_obs > 0)
    model = clone_model_with_od_subset(model_full, keep)
    tau_obs, tau_invtheta_obs, _ = compute_soft_shortest_path_assignment(
        model.n_nodes,
        model.edge_i,
        model.edge_j,
        model.edge_t_obs_min,
        model.od_origin,
        model.od_dest,
        theta_route=args.theta,
    )
    fundamentals = invert_fundamentals(
        tau_invtheta_support=tau_invtheta_obs,
        od_origin=model.od_origin,
        od_dest=model.od_dest,
        residents_obs=model.residents_obs,
        jobs_obs=model.jobs_obs,
        theta=args.theta,
        alpha=args.alpha,
        beta=args.beta,
    )

    rows = []
    topn_list = _parse_num_list(args.topn_list, int)
    lambda_list = _parse_num_list(args.lambda_list, float)

    for lam in lambda_list:
        params = CalibratedParameters(
            theta=args.theta,
            alpha=args.alpha,
            beta=args.beta,
            lambda_congestion=lam,
            theta_source="external_default",
            lambda_source="user_grid",
        )
        baseline = solve_congested_equilibrium(
            model,
            params,
            fundamentals,
            max_iter=args.max_iter,
            damping=args.damping,
        )
        rows.append(summarise_equilibrium(model, baseline, f"baseline_lambda_{lam:.2f}").iloc[0].to_dict())

        sym_model = clone_model_with_od_subset(model, np.ones(len(model.od_origin), dtype=bool))
        sym_model.edge_t_obs_min = build_symmetric_edge_times(model)
        symmetric = solve_congested_equilibrium(
            sym_model,
            params,
            fundamentals,
            max_iter=args.max_iter,
            damping=args.damping,
        )
        rows.append(summarise_equilibrium(sym_model, symmetric, f"symmetric_lambda_{lam:.2f}").iloc[0].to_dict())

        for top_n in topn_list:
            treated_cong = pick_top_congested_edges(model, baseline, top_n)
            lanes_cong = reallocate_tidal_lanes(model, treated_cong, add_lane=args.add_lane)
            cf_cong = solve_congested_equilibrium(
                model,
                params,
                fundamentals,
                edge_lane_cf=lanes_cong,
                max_iter=args.max_iter,
                damping=args.damping,
            )
            rows.append(summarise_equilibrium(model, cf_cong, f"congestion_top_{top_n}_lambda_{lam:.2f}").iloc[0].to_dict())

            treated_tidal = pick_top_tidal_edges(model, version_root, top_n)
            lanes_tidal = reallocate_tidal_lanes(model, treated_tidal, add_lane=args.add_lane)
            cf_tidal = solve_congested_equilibrium(
                model,
                params,
                fundamentals,
                edge_lane_cf=lanes_tidal,
                max_iter=args.max_iter,
                damping=args.damping,
            )
            rows.append(summarise_equilibrium(model, cf_tidal, f"tidal_top_{top_n}_lambda_{lam:.2f}").iloc[0].to_dict())

    out = pd.DataFrame(rows)
    out["lambda_congestion"] = out["scenario"].str.extract(r"lambda_(\d+\.\d+)").astype(float)
    out["scenario_family"] = out["scenario"].str.extract(r"^(baseline|symmetric|congestion|tidal)")
    out["top_n"] = pd.to_numeric(out["scenario"].str.extract(r"top_(\d+)")[0], errors="coerce")
    out["baseline_welfare_same_lambda"] = out.groupby("lambda_congestion")["welfare"].transform(
        lambda s: s.iloc[0] if len(s) else np.nan
    )
    out["baseline_commute_same_lambda"] = out.groupby("lambda_congestion")["weighted_avg_commute_time_min"].transform(
        lambda s: s.iloc[0] if len(s) else np.nan
    )
    out["delta_welfare_pct_vs_baseline"] = (
        (out["welfare"] / out["baseline_welfare_same_lambda"]) - 1.0
    ) * 100.0
    out["delta_commute_min_vs_baseline"] = (
        out["weighted_avg_commute_time_min"] - out["baseline_commute_same_lambda"]
    )
    out.to_csv(model_root / "counterfactual_suite_summary.csv", index=False)
    print(f"[suite] completed -> {model_root}")


if __name__ == "__main__":
    main()
