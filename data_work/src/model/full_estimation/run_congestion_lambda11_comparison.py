"""Compare congestion specifications: legacy vs density (Xi/n*ell) vs anchored."""
from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.model.directional_cost.policy_counterfactuals import (  # noqa: E402
    PolicySpec,
    apply_policy_abar,
    apply_policy_tau,
    apply_policy_ubar,
    default_policies,
)
from src.model.full_estimation import build_primitives as bp  # noqa: E402
from src.model.full_estimation import invert_fundamentals as inv  # noqa: E402
from src.model.full_estimation import parameters as pm  # noqa: E402
from src.model.full_estimation import stage2_counterfactuals as s2  # noqa: E402
from src.model.full_estimation.counterfactual_levels import select_tidal_lane_pairs  # noqa: E402
from src.model.full_estimation.equilibrium_solve import (  # noqa: E402
    BEIJING_LAMBDA_PHYS,
    compute_observation_anchors,
    solve_equilibrium,
)


LOG = logging.getLogger("congestion_lambda11_cmp")


@dataclass(frozen=True)
class CongestionSpec:
    name: str
    form: str
    lambda_: float


SPECS = (
    CongestionSpec("legacy_aa071", "legacy", pm.AA_SEATTLE_DELTA1 / pm.AA_SEATTLE_THETA),
    CongestionSpec("density_lambda11", "density", BEIJING_LAMBDA_PHYS),
    CongestionSpec("anchored_lambda11", "anchored", BEIJING_LAMBDA_PHYS),
)


def _repo_root(arg: str) -> Path:
    repo = Path(arg).resolve()
    if not (repo / "data_work").exists() and (repo.parent / "data_work").exists():
        repo = repo.parent
    return repo


def _skw(fund: inv.InversionResult, cspec: CongestionSpec | None = None, **extra) -> dict:
    base = dict(
        kappa=fund.kappa,
        chi=fund.chi,
        max_outer=extra.pop("max_outer", 80),
        tol=extra.pop("tol", 1e-5),
        damping=extra.pop("damping", 0.25 if cspec and cspec.form == "anchored" else 0.5),
    )
    base.update(extra)
    return base


def _cong_kw(spec: CongestionSpec, anchors: dict) -> dict:
    kw = dict(
        congestion_form=spec.form,
        congestion_lambda=spec.lambda_,
        endogenous_congestion=True,
    )
    if spec.form == "anchored":
        kw.update(
            xi_obs_kl=anchors["xi_obs_kl"],
            n_obs_kl=anchors["n_obs_kl"],
            tau_obs_anchor=anchors["tau_obs_anchor"],
        )
    return kw


def _baseline_row(
    spec: CongestionSpec,
    eq,
    prim: bp.Primitives,
) -> dict:
    tau_obs = prim.tau_obs_min
    return {
        "congestion_spec": spec.name,
        "congestion_form": spec.form,
        "lambda": spec.lambda_,
        "scenario": "baseline_endogenous",
        "Wbar": eq.Wbar,
        "converged": eq.converged,
        "n_outer": eq.n_outer,
        "kappa": eq.kappa,
        "mean_tau_vs_obs_pct": 100.0 * float(eq.tau_min_kl.mean() / tau_obs.mean() - 1.0),
        "median_tau_vs_obs": float(np.median(eq.tau_min_kl / np.clip(tau_obs, 1e-12, None))),
        "pct_edges_faster_than_ff": 100.0 * float(np.mean(eq.tau_min_kl < prim.tau_ff_min - 1e-9)),
        "Wbar_pct_change": 0.0,
    }


def _scenario_row(
    spec: CongestionSpec,
    baseline,
    scenario,
    prim: bp.Primitives,
    scenario_name: str,
) -> dict:
    row = _baseline_row(spec, scenario, prim)
    row["scenario"] = scenario_name
    row["Wbar_pct_change"] = 100.0 * (scenario.Wbar / baseline.Wbar - 1.0)
    row["mean_Xi_pct_change"] = 100.0 * float(
        np.mean(scenario.Xi_kl / np.clip(baseline.Xi_kl, 1e-12, None) - 1.0)
    )
    return row


def _policy_endogenous(
    prim, fund, params, spec: PolicySpec, anchors, cspec: CongestionSpec, **skw,
):
    kw = _skw(fund, cspec, **skw)
    kw.update(_cong_kw(cspec, anchors))
    tau_ff = s2._apply_tau_ff_policy(prim.tau_ff_min, prim, spec)
    ubar_cf = apply_policy_ubar(fund.ubar_theta, prim, spec)
    abar_cf = apply_policy_abar(fund.abar_theta, prim, spec)
    return solve_equilibrium(
        prim, ubar_cf, abar_cf, params,
        tau_ff_override=tau_ff,
        tau_min_init=prim.tau_obs_min.copy(),
        label=f"{spec.name}_{cspec.name}",
        **kw,
    )


def _tidal_long_run(prim, fund, params, baseline, anchors, cspec: CongestionSpec, **skw):
    kw = _skw(fund, cspec, **skw)
    kw.update(_cong_kw(cspec, anchors))
    sel = select_tidal_lane_pairs(prim, top_k=50, delta=1.0)
    n_cf = prim.n_kl.copy()
    for slow, fast in sel.treated_pairs:
        n_cf[slow] = n_cf[slow] + 1.0
        n_cf[fast] = n_cf[fast] - 1.0
    eq = solve_equilibrium(
        prim, fund.ubar_theta, fund.abar_theta, params,
        n_kl_override=n_cf,
        tau_min_init=prim.tau_obs_min.copy(),
        label=f"tidal_top50_{cspec.name}",
        **kw,
    )
    return eq


def run_comparison(repo: Path, version_id: str, out: Path, **skw) -> pd.DataFrame:
    out.mkdir(parents=True, exist_ok=True)
    prim = bp.load_primitives(repo / "data_work" / "outputs" / version_id, period="AM")
    params = pm.Parameters()
    fund = inv.invert_fundamentals(prim, params, tol=1.5e-4, max_iter=1800, kappa=1.0)

    anchors = compute_observation_anchors(
        prim, fund.ubar_theta, fund.abar_theta, params,
        chi=fund.chi, kappa=fund.kappa,
    )
    (out / "observation_anchors.json").write_text(json.dumps({
        "xi_obs_median": float(np.median(anchors["xi_obs_kl"])),
        "xi_obs_p95": float(np.quantile(anchors["xi_obs_kl"], 0.95)),
        "density_obs_median": float(np.median(
            anchors["xi_obs_kl"] / np.clip(anchors["n_obs_kl"] * prim.ell_kl_km, 1e-12, None)
        )),
    }, indent=2))

    policies = [p for p in default_policies() if p.name in (
        "uniform_speed_10pct", "hub_access_15pct",
    )]

    rows: list[dict] = []
    for cspec in SPECS:
        LOG.info("=== %s (form=%s, lambda=%.3f) ===", cspec.name, cspec.form, cspec.lambda_)
        kw = _skw(fund, cspec, **skw)
        kw.update(_cong_kw(cspec, anchors))
        baseline = solve_equilibrium(
            prim, fund.ubar_theta, fund.abar_theta, params,
            tau_min_init=prim.tau_obs_min.copy(),
            label=f"baseline_{cspec.name}",
            **kw,
        )
        baseline.write(out / cspec.name / "baseline")
        rows.append(_baseline_row(cspec, baseline, prim))

        for pol in policies:
            eq_p = _policy_endogenous(prim, fund, params, pol, anchors, cspec, **skw)
            rows.append(_scenario_row(cspec, baseline, eq_p, prim, pol.name))

        eq_t = _tidal_long_run(prim, fund, params, baseline, anchors, cspec, **skw)
        rows.append(_scenario_row(cspec, baseline, eq_t, prim, "tidal_top50_long_run"))

    df = pd.DataFrame(rows)
    df.to_csv(out / "congestion_form_comparison.csv", index=False)
    return df


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo-root", default=".")
    p.add_argument("--version-id", default="model_input_edgeproj_v2_lcc_gate5_v1")
    p.add_argument(
        "--output-name",
        default="congestion_lambda11_comparison_edgeproj_v2_lcc_gate5_v1",
    )
    p.add_argument("--max-outer", type=int, default=120)
    p.add_argument("--tol", type=float, default=1e-5)
    args = p.parse_args(argv)

    repo = _repo_root(args.repo_root)
    out = repo / "data_work" / "outputs" / args.output_name
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s | %(message)s")

    df = run_comparison(
        repo, args.version_id, out,
        max_outer=args.max_outer, tol=args.tol,
    )
    LOG.info("Wrote %s", out / "congestion_form_comparison.csv")
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()
