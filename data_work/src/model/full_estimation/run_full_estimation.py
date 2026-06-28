"""
CLI driver for the full-estimation pipeline (v1, level-form scaffold)
=====================================================================

Run order:
    1. build_primitives.load_primitives
    2. parameters.Parameters (defaults)
    3. invert_fundamentals.invert_fundamentals       (Step 4)
    4. equilibrium_solve.solve_equilibrium           (baseline check)
    5. lambda_diagnostic.estimate_lambda_cross_section
       (cross-section diagnostic only; does NOT overwrite lambda)
    6. counterfactual_levels.run_cf{1,2,3}_*

Default output root:
    data_work/outputs/full_estimation_square_v1/

Use --output-name to override.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# Make the package importable when run as a plain script
# (matches the convention used in run_spatial_equilibrium.py).
ROOT = Path(__file__).resolve().parents[3]   # -> data_work/
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.model.full_estimation import build_primitives as bp        # noqa: E402
from src.model.full_estimation import parameters as pm              # noqa: E402
from src.model.full_estimation import invert_fundamentals as inv    # noqa: E402
from src.model.full_estimation import equilibrium_solve as eq       # noqa: E402
from src.model.full_estimation import lambda_diagnostic as lam      # noqa: E402
from src.model.full_estimation import counterfactual_levels as cf   # noqa: E402


LOG = logging.getLogger("full_estimation")


def _setup_logging(out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
        handlers=[
            logging.FileHandler(out_dir / "run.log"),
            logging.StreamHandler(),
        ],
    )


def main(argv=None) -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--repo-root", default=".",
                   help="path containing data_work/")
    p.add_argument("--version-id", default="manual_centerline_rules_v11",
                   help="QSM output version under data_work/outputs/")
    p.add_argument("--grid-type", default="square",
                   choices=["square", "hex", "voronoi"])
    p.add_argument("--period", default="AM")
    p.add_argument("--od-pop-col", default="commuters_road")
    p.add_argument("--output-name", default="full_estimation_square_v1")
    p.add_argument("--skip-counterfactual", action="store_true")
    p.add_argument("--cf2-top-k", type=int, default=100)
    p.add_argument("--cf3-factors", default="1.10,1.25,1.50")
    p.add_argument("--max-outer", type=int, default=60)
    p.add_argument("--tol", type=float, default=1e-5)
    p.add_argument("--damping", type=float, default=0.5)
    args = p.parse_args(argv)

    repo = Path(args.repo_root).resolve()
    version_root = repo / "data_work" / "outputs" / args.version_id
    out_root = repo / "data_work" / "outputs" / args.output_name
    out_root.mkdir(parents=True, exist_ok=True)
    _setup_logging(out_root)
    LOG.info(f"output root: {out_root}")

    # --- Step 1
    LOG.info("Step 1 — build_primitives")
    prim = bp.load_primitives(version_root, grid_type=args.grid_type,
                                 period=args.period, od_pop_col=args.od_pop_col)
    bp.write_primitives_summary(prim, out_root / "primitives")
    LOG.info(f"  N={prim.n_nodes} M={prim.n_edges} Lbar={prim.Lbar:.1f}")

    # --- Step 2
    params = pm.Parameters()
    params.write(out_root / "parameters.json")
    LOG.info(f"  parameters: theta={params.theta} alpha={params.alpha} "
              f"beta={params.beta} lambda={params.lambda_:.5f}")

    # --- Step 4 (inversion) — done before equilibrium check
    LOG.info("Step 4 — invert_fundamentals")
    fund = inv.invert_fundamentals(prim, params)
    fund.write(out_root / "fundamentals")
    LOG.info(f"  converged={fund.converged} iters={fund.n_iter} "
              f"delta={fund.max_log_change:.2e} chi={fund.chi:.3e}")

    # --- Baseline equilibrium check
    LOG.info("Equilibrium baseline check")
    LOG.info(f"  kappa={fund.kappa:.4f} (uniform iceberg rescaling)")
    baseline = eq.solve_equilibrium(
        prim, fund.ubar_theta, fund.abar_theta, params,
        max_outer=args.max_outer, tol=args.tol, damping=args.damping,
        kappa=fund.kappa,
    )
    baseline.write(out_root / "baseline_check")
    LOG.info(f"  converged={baseline.converged} outer={baseline.n_outer} "
              f"delta={baseline.max_log_change:.2e} Wbar={baseline.Wbar:.4f}")

    # --- Step 3 (diagnostic only; needs baseline Xi)
    LOG.info("Step 3 — lambda diagnostic (cross-section)")
    lam_diag = lam.estimate_lambda_cross_section(
        tau_obs=prim.tau_obs_min, tau_ff=prim.tau_ff_min,
        n_kl=prim.n_kl, xi_hat=baseline.Xi_kl,
    )
    (out_root / "lambda_diagnostic.json").write_text(
        json.dumps(lam_diag.to_dict(), indent=2, default=float)
    )
    LOG.info(f"  lambda_hat={lam_diag.lambda_hat:.4f} R2={lam_diag.r2:.4f}")

    if args.skip_counterfactual:
        LOG.info("--skip-counterfactual set; done.")
        return

    # --- CF1
    LOG.info("CF1 — symmetric corridor cost")
    eq_cf1 = cf.run_cf1_symmetric(
        prim, params, fund.ubar_theta, fund.abar_theta,
        max_outer=args.max_outer, tol=args.tol, damping=args.damping,
        kappa=fund.kappa,
    )
    cf1_dir = out_root / "counterfactuals" / "CF1_symmetric"
    eq_cf1.write(cf1_dir)
    cf.summarise_diff(baseline, eq_cf1, prim).to_csv(cf1_dir / "summary.csv", index=False)

    # --- CF2
    LOG.info("CF2 — tidal-lane Delta=1, top-K")
    eq_cf2, sel = cf.run_cf2_tidal(
        prim, params, fund.ubar_theta, fund.abar_theta,
        top_k=args.cf2_top_k,
        max_outer=args.max_outer, tol=args.tol, damping=args.damping,
        kappa=fund.kappa,
    )
    cf2_dir = out_root / "counterfactuals" / "CF2_tidal_lane"
    eq_cf2.write(cf2_dir)
    sel.to_frame(prim).to_csv(cf2_dir / "selected_pairs.csv", index=False)
    cf.summarise_diff(baseline, eq_cf2, prim).to_csv(cf2_dir / "summary.csv", index=False)

    # --- CF3 — sweep over factors
    factors = [float(x) for x in args.cf3_factors.split(",")]
    for fac in factors:
        LOG.info(f"CF3 — uniform lane expansion factor={fac}")
        eq_cf3 = cf.run_cf3_uniform(
            prim, params, fund.ubar_theta, fund.abar_theta, factor=fac,
            max_outer=args.max_outer, tol=args.tol, damping=args.damping,
            kappa=fund.kappa,
        )
        sub = out_root / "counterfactuals" / f"CF3_uniform_{fac:.2f}"
        eq_cf3.write(sub)
        cf.summarise_diff(baseline, eq_cf3, prim).to_csv(sub / "summary.csv", index=False)

    LOG.info("done.")


if __name__ == "__main__":
    main()
