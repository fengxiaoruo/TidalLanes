"""
Observed directed link traffic (2025-05 RN, gate5)
==================================================

Brings the platform RN edge loads (Platform/utils/map_rn_to_model_edges.py) into
the gate5 model as observed Xi_kl and runs three checks:

    1. lambda cross-section with observed Xi instead of model-assigned Xi_hat
       (same lane-bin demeaned OLS as lambda_diagnostic);
    2. fit of model-assigned baseline Xi_hat to observed Xi across edges;
    3. direction: within reciprocal pairs, log(Xi_kl/Xi_lk) observed vs model.

Main measure: length-allocated load (len_flow_HW). Robustness: boundary crossings
(n_pass_HW). Both are AM home-to-work, strict, pooled 0519-0523; divided by 5 to
read as a daily count of sampled commuter vehicle passes. Loads that fall off the
model edge set are reported only (coverage), not reassigned.

Output: data_work/outputs/observed_traffic_rn202505_gate5_v1/
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]   # -> data_work/
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.model.full_estimation import build_primitives as bp        # noqa: E402
from src.model.full_estimation import parameters as pm              # noqa: E402
from src.model.full_estimation import invert_fundamentals as inv    # noqa: E402
from src.model.full_estimation import equilibrium_solve as eq       # noqa: E402
from src.model.full_estimation import lambda_diagnostic as lam      # noqa: E402

LOG = logging.getLogger("observed_traffic")
N_DAYS = 5
MEASURES = {"length_flow": "len_flow_HW", "crossings": "n_pass_HW"}


def _setup_logging(out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
        handlers=[logging.FileHandler(out_dir / "run.log"), logging.StreamHandler()],
    )


def _corr(a: np.ndarray, b: np.ndarray) -> float:
    ok = np.isfinite(a) & np.isfinite(b)
    return float(np.corrcoef(a[ok], b[ok])[0, 1]) if ok.sum() > 2 else float("nan")


def _direction(prim, x: np.ndarray) -> np.ndarray:
    """log(x_kl / x_lk) for each edge with a reverse edge, NaN otherwise."""
    idx = {(int(i), int(j)): k for k, (i, j) in enumerate(zip(prim.edge_i, prim.edge_j))}
    out = np.full(prim.n_edges, np.nan)
    for (i, j), k in idx.items():
        r = idx.get((j, i))
        if r is not None and x[k] > 0 and x[r] > 0:
            out[k] = np.log(x[k] / x[r])
    return out


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--repo-root", default=str(ROOT.parent))
    p.add_argument("--version-id", default="model_input_edgeproj_v2_lcc_gate5_v1")
    p.add_argument("--loads", default="Platform/runs/202505_v01/local_model/model_edge_rn_load_window_strict.csv")
    p.add_argument("--coverage", default="Platform/runs/202505_v01/local_model/rn_model_edge_measure_comparison.json")
    p.add_argument("--output-name", default="observed_traffic_rn202505_gate5_v1")
    args = p.parse_args(argv)

    repo = Path(args.repo_root).resolve()
    out_root = repo / "data_work" / "outputs" / args.output_name
    if out_root.exists():
        raise SystemExit(f"{out_root} exists; choose a new --output-name")
    _setup_logging(out_root)

    prim = bp.load_primitives(repo / "data_work" / "outputs" / args.version_id,
                              grid_type="square", period="AM", od_pop_col="commuters_road")
    params = pm.Parameters()
    fund = inv.invert_fundamentals(prim, params)
    baseline = eq.solve_equilibrium(
        prim, fund.ubar_theta, fund.abar_theta, params,
        kappa=fund.kappa, chi=fund.chi, tau_min_init=prim.tau_obs_min.copy(),
        label="baseline_endogenous",
    )
    LOG.info(f"N={prim.n_nodes} M={prim.n_edges} baseline converged={baseline.converged}")

    loads = pd.read_csv(repo / args.loads)
    edges = pd.DataFrame({
        "grid_from": prim.node_grid_ids[prim.edge_i], "grid_to": prim.node_grid_ids[prim.edge_j],
        "tau_obs_min": prim.tau_obs_min, "tau_ff_min": prim.tau_ff_min, "n_kl": prim.n_kl,
        "xi_hat_model": baseline.Xi_kl,
    })
    edges = edges.merge(loads[["grid_from", "grid_to", *MEASURES.values()]], on=["grid_from", "grid_to"], how="left")
    assert len(edges) == prim.n_edges
    for name, col in MEASURES.items():
        edges[f"xi_obs_{name}"] = edges[col].fillna(0.0) / N_DAYS
    edges = edges.drop(columns=list(MEASURES.values()))
    edges.to_csv(out_root / "edge_observed_vs_model_traffic.csv", index=False)

    res: dict = {"n_edges": int(prim.n_edges), "baseline_converged": bool(baseline.converged),
                 "lambda_parameter_placeholder": float(params.lambda_)}
    lam_model = lam.estimate_lambda_cross_section(prim.tau_obs_min, prim.tau_ff_min, prim.n_kl, baseline.Xi_kl)
    res["lambda_model_xi_hat"] = lam_model.to_dict()
    dir_model = _direction(prim, baseline.Xi_kl)
    for name in MEASURES:
        xo = edges[f"xi_obs_{name}"].to_numpy()
        pos = xo > 0
        d = lam.estimate_lambda_cross_section(prim.tau_obs_min[pos], prim.tau_ff_min[pos], prim.n_kl[pos], xo[pos])
        d.method = f"OLS, lane-bin demeaned, observed RN Xi ({name})"
        d.notes = "Diagnostic only; edges with zero observed load dropped."
        dir_obs = _direction(prim, xo)
        res[name] = {
            "edges_with_load": int(pos.sum()),
            "lambda_observed_xi": d.to_dict(),
            "corr_log_xi_hat_vs_log_xi_obs": _corr(np.log(baseline.Xi_kl[pos]), np.log(xo[pos])),
            "corr_log_xi_hat_vs_log_xi_obs_per_lane": _corr(np.log(baseline.Xi_kl[pos] / prim.n_kl[pos]),
                                                            np.log(xo[pos] / prim.n_kl[pos])),
            "reciprocal_edges_both_directions": int(np.isfinite(dir_obs).sum()),
            "corr_direction_model_vs_observed": _corr(dir_model, dir_obs),
        }
        LOG.info(f"{name}: lambda_obs={d.lambda_hat:.4f} (se {d.se:.4f}, R2 {d.r2:.3f}); "
                 f"corr log Xi_hat~Xi_obs={res[name]['corr_log_xi_hat_vs_log_xi_obs']:.3f}; "
                 f"direction corr={res[name]['corr_direction_model_vs_observed']:.3f}")
    res["coverage_reported_not_reassigned"] = json.loads((repo / args.coverage).read_text())["coverage"]
    (out_root / "observed_traffic_summary.json").write_text(json.dumps(res, indent=2, default=float))
    LOG.info(f"lambda (model Xi_hat) = {lam_model.lambda_hat:.4f}; placeholder lambda = {params.lambda_:.4f}")


if __name__ == "__main__":
    main()
