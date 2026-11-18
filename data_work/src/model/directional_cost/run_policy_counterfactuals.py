"""Run non-directional fixed-cost counterfactuals in directed vs symmetric worlds."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from src.model.full_estimation import build_primitives as bp, parameters as pm, invert_fundamentals as inv  # noqa: E402
from src.model.directional_cost import fixed_tau_model as ft  # noqa: E402
from src.model.directional_cost import policy_counterfactuals as pc  # noqa: E402


def main(argv=None) -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--repo-root", default=".")
    p.add_argument("--version-id", default="model_input_edgeproj_v2_lcc_gate5_v1")
    p.add_argument("--output-name", default="directional_cost_edgeproj_v2_lcc_gate5_v1")
    args = p.parse_args(argv)

    repo = Path(args.repo_root).resolve()
    out = repo / "data_work" / "outputs" / args.output_name
    cf_dir = out / "counterfactuals"
    diag = out / "diagnostics"
    cf_dir.mkdir(parents=True, exist_ok=True)

    prim = bp.load_primitives(repo / "data_work" / "outputs" / args.version_id, period="AM")
    params = pm.Parameters()
    tau_sym, _ = ft.build_symmetric_cost(prim, method="geometric")
    kappa = max(
        ft.pick_kappa(prim.edge_i, prim.tau_obs_min, prim.n_nodes),
        ft.pick_kappa(prim.edge_i, tau_sym, prim.n_nodes),
    )
    l_R, l_F, _ = ft.observed_shares(prim)
    eq_kw = {"initial_l_R": l_R, "initial_l_F": l_F}

    fund_dir = inv.invert_fundamentals(prim, params, kappa=kappa, tau_min_kl=prim.tau_obs_min)
    fund_sym = inv.invert_fundamentals(prim, params, kappa=kappa, tau_min_kl=tau_sym)
    eq_dir = ft.solve_static_equilibrium(
        prim, prim.tau_obs_min, fund_dir.ubar_theta, fund_dir.abar_theta, fund_dir.chi,
        params, kappa=kappa, label="directed_observed", **eq_kw,
    )
    eq_sym = ft.solve_static_equilibrium(
        prim, tau_sym, fund_sym.ubar_theta, fund_sym.abar_theta, fund_sym.chi,
        params, kappa=kappa, label="symmetric_cost", **eq_kw,
    )

    worlds = {
        "directed": {"fund": fund_dir, "eq": eq_dir, "tau": prim.tau_obs_min},
        "symmetric": {"fund": fund_sym, "eq": eq_sym, "tau": tau_sym},
    }
    df = pc.run_all(prim, params, kappa, worlds, eq_kw)
    df.to_csv(cf_dir / "policy_cf_summary.csv", index=False)

    wedge_rows = []
    for policy in df["policy"].unique():
        sub = df[df["policy"] == policy].set_index("world")
        wedge_rows.append({
            "policy": policy,
            "Wbar_pct_change_directed": float(sub.loc["directed", "Wbar_pct_change"]),
            "Wbar_pct_change_symmetric": float(sub.loc["symmetric", "Wbar_pct_change"]),
            "wedge_sym_minus_dir_Wbar_pp": float(
                sub.loc["symmetric", "Wbar_pct_change"] - sub.loc["directed", "Wbar_pct_change"]
            ),
            "residence_dissim_dir": float(sub.loc["directed", "residence_dissimilarity_pct"]),
            "residence_dissim_sym": float(sub.loc["symmetric", "residence_dissimilarity_pct"]),
            "od_dissim_dir": float(sub.loc["directed", "od_dissimilarity_pct"]),
            "od_dissim_sym": float(sub.loc["symmetric", "od_dissimilarity_pct"]),
        })
    wedge = pd.DataFrame(wedge_rows)
    wedge.to_csv(cf_dir / "policy_cf_world_wedge.csv", index=False)

    maps = pc.node_maps_frame(prim, params, fund_dir, fund_sym, eq_dir, eq_sym)
    maps.to_csv(diag / "node_market_access_maps.csv", index=False)
    print(f"Wrote {cf_dir} and {diag / 'node_market_access_maps.csv'}")


if __name__ == "__main__":
    main()
