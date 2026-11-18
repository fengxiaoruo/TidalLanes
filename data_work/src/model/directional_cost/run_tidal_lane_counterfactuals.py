"""Run tidal-lane counterfactuals (short-run fixed shares vs long-run sorting)."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from src.model.full_estimation import build_primitives as bp, parameters as pm, invert_fundamentals as inv  # noqa: E402
from src.model.directional_cost import fixed_tau_model as ft  # noqa: E402
from src.model.directional_cost import tidal_lane_counterfactuals as tl  # noqa: E402


def _parse_int_list(text: str) -> list[int]:
    return [int(x.strip()) for x in text.split(",") if x.strip()]


def _parse_float_list(text: str) -> list[float]:
    return [float(x.strip()) for x in text.split(",") if x.strip()]


def build_specs_from_cli(args) -> list[tl.TidalLaneSpec]:
    if args.quick:
        return [
            tl.TidalLaneSpec(name="bpr_top25_d1", top_n=25, delta_lanes=1.0),
            tl.TidalLaneSpec(name="direct_top25_s090_f110", top_n=25, method="direct_tau",
                             slow_tau_scale=0.90, fast_tau_scale=1.10),
        ]
    specs: list[tl.TidalLaneSpec] = []
    for top_n in _parse_int_list(args.top_n_grid):
        for delta in _parse_float_list(args.delta_lanes_grid):
            for ratio in _parse_float_list(args.min_asym_ratio_grid):
                name = f"bpr_top{top_n}_d{str(delta).replace('.', 'p')}_r{str(ratio).replace('.', 'p')}"
                specs.append(tl.TidalLaneSpec(
                    name=name, top_n=top_n, delta_lanes=delta,
                    min_asym_ratio=ratio, method="bpr",
                ))
    if args.include_direct_tau:
        for slow, fast in ((0.90, 1.10), (0.85, 1.15)):
            specs.append(tl.TidalLaneSpec(
                name=f"direct_top{args.direct_top_n}_s{str(slow).replace('.', '')}_f{str(fast).replace('.', '')}",
                top_n=args.direct_top_n,
                method="direct_tau",
                slow_tau_scale=slow,
                fast_tau_scale=fast,
            ))
    return specs


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo-root", default=".")
    p.add_argument("--version-id", default="model_input_edgeproj_v2_lcc_gate5_v1")
    p.add_argument("--output-name", default="directional_cost_edgeproj_v2_lcc_gate5_v1")
    p.add_argument("--quick", action="store_true", help="Run two specs only for smoke test")
    p.add_argument("--top-n-grid", default="10,25,50,100")
    p.add_argument("--delta-lanes-grid", default="0.5,1.0")
    p.add_argument("--min-asym-ratio-grid", default="1.10,1.25,1.50")
    p.add_argument("--include-direct-tau", action="store_true", default=True)
    p.add_argument("--no-direct-tau", action="store_false", dest="include_direct_tau")
    p.add_argument("--direct-top-n", type=int, default=50)
    args = p.parse_args(argv)

    repo = Path(args.repo_root).resolve()
    if not (repo / "data_work").exists() and (repo.parent / "data_work").exists():
        repo = repo.parent
    out = repo / "data_work" / "outputs" / args.output_name
    cf_dir = out / "counterfactuals" / "tidal_lane"
    cf_dir.mkdir(parents=True, exist_ok=True)

    prim = bp.load_primitives(repo / "data_work" / "outputs" / args.version_id, period="AM")
    params = pm.Parameters()
    kappa = ft.pick_kappa(prim.edge_i, prim.tau_obs_min, prim.n_nodes)
    l_R, l_F, _ = ft.observed_shares(prim)
    eq_kw = {"initial_l_R": l_R, "initial_l_F": l_F}

    fund = inv.invert_fundamentals(prim, params, kappa=kappa, tau_min_kl=prim.tau_obs_min)
    eq_base = ft.solve_static_equilibrium(
        prim, prim.tau_obs_min, fund.ubar_theta, fund.abar_theta, fund.chi,
        params, kappa=kappa, label="directed_baseline", **eq_kw,
    )

    pair_pool = tl.build_reciprocal_pair_table(prim)
    pair_pool.to_csv(cf_dir / "candidate_pairs_ranked.csv", index=False)

    specs = build_specs_from_cli(args)
    summary, edges = tl.run_grid(
        prim, params, kappa, prim.tau_obs_min, fund, eq_base, eq_kw, specs=specs,
    )
    summary.to_csv(cf_dir / "tidal_lane_cf_summary.csv", index=False)
    if not edges.empty:
        edges.to_csv(cf_dir / "tidal_lane_edge_detail.csv", index=False)

    pivot = summary.pivot_table(
        index=["spec_name", "method", "top_n", "delta_lanes", "min_asym_ratio", "n_pairs_applied"],
        columns="horizon",
        values=[
            "Wbar_pct_change",
            "residence_dissimilarity_pct",
            "workplace_dissimilarity_pct",
            "od_dissimilarity_pct",
            "p95_abs_log_Pi_change",
            "mean_tau_slow_pct_change",
            "mean_tau_fast_pct_change",
        ],
        aggfunc="first",
    )
    pivot.columns = [f"{metric}_{horizon}" for metric, horizon in pivot.columns]
    pivot = pivot.reset_index()
    pivot.to_csv(cf_dir / "tidal_lane_short_vs_long.csv", index=False)

    meta = {
        "n_candidate_pairs": int(len(pair_pool)),
        "n_specs": len(specs),
        "congestion_propagation": False,
        "baseline_world": "directed_observed_reinvert",
        "short_run": "fixed l_R and l_F at baseline equilibrium",
        "long_run": "re-solve static equilibrium with sorting",
    }
    tl.write_json(cf_dir / "tidal_lane_run_meta.json", meta)

    print(f"Wrote tidal-lane outputs under {cf_dir}")
    print(f"  candidate pairs: {len(pair_pool)}")
    print(f"  specs run: {len(specs)}")
    if not summary.empty:
        best = summary.sort_values("Wbar_pct_change", ascending=False).iloc[0]
        print(
            f"  largest Wbar gain: {best['Wbar_pct_change']:.3f}% "
            f"({best['spec_name']}, {best['horizon']})"
        )


if __name__ == "__main__":
    main()
