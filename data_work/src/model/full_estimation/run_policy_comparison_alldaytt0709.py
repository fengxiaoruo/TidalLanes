"""Wrapper (2026-11-13): policy comparison on the all-day-OD + 07-09 travel-time mainline.

Calls ``run_policy_comparison_gate5.run_policies`` unchanged and, without modifying any
existing module, records per-scenario diagnostics the suite does not write:

  * spectral radius rho(A) at the scenario's physical link times (kappa fixed by the rule);
  * average physical commuting time (Xi . m / (Lbar kappa)) and seconds saved per commuter;
  * direct / GE / sorting decomposition (``tidal_lane_diagnostics.od_cost_decomposition``)
    for every policy that has both a short-run and a long-run equilibrium.

For P6/P7 the solver's link time is the perceived cost (physical + toll); physical minutes
are taken from the ``tau_phys`` passed to ``realized_welfare_route_choice``.

Usage (P1-P5):
  python run_policy_comparison_alldaytt0709.py suite --output-name <dir> [run_policies args]
Usage (P6/P7):  same with --pricing-impl additive --pricing-only
Summary table:
  python run_policy_comparison_alldaytt0709.py summary --policy-dir <dir> --pricing-dir <dir>
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import replace
from pathlib import Path

import numpy as np
import pandas as pd

DATA_WORK = Path(__file__).resolve().parents[3]
if str(DATA_WORK) not in sys.path:
    sys.path.insert(0, str(DATA_WORK))

from src.model.full_estimation import run_policy_comparison_gate5 as rpc  # noqa: E402
from src.model.full_estimation import stage2_counterfactuals as s2  # noqa: E402
from src.model.full_estimation import build_primitives as bp  # noqa: E402
from src.model.full_estimation.equilibrium_solve import spectral_radius_A  # noqa: E402
from src.model.full_estimation.tidal_lane_diagnostics import od_cost_decomposition  # noqa: E402

LOG = logging.getLogger("policy_comparison_alldaytt0709")


def _suite(argv: list[str]) -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--repo-root", default=str(DATA_WORK))
    p.add_argument("--version-id", required=True)
    p.add_argument("--stage2-dir", required=True)
    p.add_argument("--output-name", required=True)
    p.add_argument("--pair-selection", default="score_bpr_st")
    p.add_argument("--congestion-lambda", type=float, required=True)
    p.add_argument("--horizons", default="short_run,long_run")
    p.add_argument("--congestion-form", required=True)
    p.add_argument("--xi-floor-rel", type=float, default=0.0)
    p.add_argument("--kappa-rule", default="fixed1")
    p.add_argument("--pricing-impl", default="auto")
    p.add_argument("--pricing-only", action="store_true")
    args = p.parse_args(argv)

    data_work = Path(args.repo_root)
    out = data_work / "outputs" / args.output_name
    if out.exists():
        raise SystemExit(f"refusing to overwrite existing output dir {out}")
    out.mkdir(parents=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s | %(message)s",
                        handlers=[logging.FileHandler(out / "run.log"), logging.StreamHandler()])
    cmd = " ".join([sys.executable] + sys.argv)
    (out / "command.txt").write_text(cmd + "\n")
    LOG.info("Command: %s", cmd)

    captured: list[tuple] = []          # (baseline, scenario) in row order
    phys_by_eq: dict[int, np.ndarray] = {}
    orig_summarise = s2.summarise_vs_baseline
    orig_rwrc = s2.realized_welfare_route_choice

    def summarise_capture(baseline, scenario, *a, **k):
        captured.append((baseline, scenario))
        return orig_summarise(baseline, scenario, *a, **k)

    def rwrc_capture(prim, fund, params, eq, tau_phys, *a, **k):
        phys_by_eq[id(eq)] = np.asarray(tau_phys, dtype=float).copy()
        return orig_rwrc(prim, fund, params, eq, tau_phys, *a, **k)

    s2.summarise_vs_baseline = summarise_capture
    s2.realized_welfare_route_choice = rwrc_capture
    stage2_dir = Path(args.stage2_dir)
    if not stage2_dir.is_absolute():
        stage2_dir = data_work / "outputs" / stage2_dir
    horizons = [h.strip() for h in args.horizons.split(",") if h.strip()]
    df = rpc.run_policies(
        data_work, stage2_dir, out,
        version_id=args.version_id, pair_selection=args.pair_selection,
        congestion_lambda=args.congestion_lambda, horizons=horizons,
        congestion_form=args.congestion_form, xi_floor_rel=args.xi_floor_rel,
        kappa_rule=args.kappa_rule, pricing_impl=args.pricing_impl,
        pricing_only=args.pricing_only,
    )
    s2.summarise_vs_baseline = orig_summarise
    s2.realized_welfare_route_choice = orig_rwrc
    if len(captured) != len(df):
        raise RuntimeError(f"captured {len(captured)} equilibria for {len(df)} rows")

    prim = bp.load_primitives(data_work / "outputs" / args.version_id, period="AM")
    params = rpc._load_params(stage2_dir)
    base0 = captured[0][0]

    def physical(eq):
        tau = phys_by_eq.get(id(eq))
        return eq if tau is None else replace(eq, tau_min_kl=tau)

    diag_rows = []
    phys_eq: dict[tuple, object] = {}
    for (base, eq), (_, r) in zip(captured, df.iterrows()):
        eqp = physical(eq)
        rho = spectral_radius_A(prim.edge_i, prim.edge_j, eqp.tau_min_kl, prim.n_nodes, kappa=float(eq.kappa))
        rho_perceived = spectral_radius_A(prim.edge_i, prim.edge_j, eq.tau_min_kl, prim.n_nodes, kappa=float(eq.kappa))
        avg = float(np.dot(eqp.Xi_kl, eqp.tau_min_kl) / (prim.Lbar * float(eqp.kappa)))
        avg0 = float(np.dot(base.Xi_kl, base.tau_min_kl) / (prim.Lbar * float(base.kappa)))
        diag_rows.append({
            "policy_id": r["policy_id"], "horizon": r["horizon"],
            "scenario_converged": bool(eq.converged), "n_outer": int(eq.n_outer),
            "max_log_change": float(eq.max_log_change),
            "rho_A_physical": rho, "rho_A_solver_cost": rho_perceived,
            "avg_physical_time_base_min": avg0, "avg_physical_time_min": avg,
            "physical_time_saved_sec_per_commuter": -60.0 * (avg - avg0),
        })
        phys_eq[(r["policy_id"], r["horizon"])] = (base, eqp)
    diag = pd.DataFrame(diag_rows)
    rho_base = spectral_radius_A(prim.edge_i, prim.edge_j, base0.tau_min_kl, prim.n_nodes, kappa=float(base0.kappa))
    diag.attrs["rho_base"] = rho_base

    dec_rows = []
    for pid in diag["policy_id"].unique():
        if (pid, "short_run") in phys_eq and (pid, "long_run") in phys_eq:
            base, st = phys_eq[(pid, "short_run")]
            _, lt = phys_eq[(pid, "long_run")]
            d = od_cost_decomposition(base, st, lt, prim, params, None)
            dec_rows.append({"policy_id": pid, **d})
    dec = pd.DataFrame(dec_rows)
    diag.to_csv(out / "scenario_diagnostics_alldaytt0709.csv", index=False)
    dec.to_csv(out / "physical_time_decomposition_alldaytt0709.csv", index=False)
    (out / "baseline_check_alldaytt0709.json").write_text(json.dumps({
        "baseline_converged": bool(base0.converged), "baseline_n_outer": int(base0.n_outer),
        "rho_A_baseline": rho_base, "Wbar_base": float(base0.Wbar),
    }, indent=2))
    LOG.info("Diagnostics:\n%s", diag.to_string(index=False))
    LOG.info("Done: %s", out)


def _summary(argv: list[str]) -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--repo-root", default=str(DATA_WORK))
    p.add_argument("--policy-dir", required=True)
    p.add_argument("--pricing-dir", default="")
    args = p.parse_args(argv)
    outs = Path(args.repo_root) / "outputs"
    dirs = [outs / args.policy_dir] + ([outs / args.pricing_dir] if args.pricing_dir else [])
    rows = []
    for d in dirs:
        res = pd.read_csv(d / "results_by_policy.csv")
        diag = pd.read_csv(d / "scenario_diagnostics_alldaytt0709.csv")
        dec = pd.read_csv(d / "physical_time_decomposition_alldaytt0709.csv")
        for pid in res["policy_id"].unique():
            r = res[res["policy_id"] == pid].set_index("horizon")
            g = diag[diag["policy_id"] == pid].set_index("horizon")
            pricing = "Wbar_pct_change_realized_route_choice" in r.columns and pd.notna(
                r["Wbar_pct_change_realized_route_choice"].iloc[0])
            wcol = "Wbar_pct_change_realized_route_choice" if pricing else "Wbar_pct_change"
            st, lt = float(r.loc["short_run", wcol]), float(r.loc["long_run", wcol])
            row = {
                "policy_id": pid,
                "lane_km_added": float(r["lane_km_added"].iloc[0]) if "lane_km_added" in r.columns else 0.0,
                "Wbar_pct_st": st, "Wbar_pct_lt": lt,
                "welfare_measure": wcol,
                "phys_time_saved_st_sec": float(g.loc["short_run", "physical_time_saved_sec_per_commuter"]),
                "phys_time_saved_lt_sec": float(g.loc["long_run", "physical_time_saved_sec_per_commuter"]),
                "lt_over_st_welfare": lt / st if st != 0 else float("nan"),
                "converged_st": bool(g.loc["short_run", "scenario_converged"]),
                "converged_lt": bool(g.loc["long_run", "scenario_converged"]),
                "rho_max": float(g["rho_A_physical"].max()),
                "note": ("ST welfare not interpretable for pricing (route-choice formula is LT only)"
                         if pricing else ""),
            }
            dd = dec[dec["policy_id"] == pid]
            if len(dd):
                row.update({
                    "direct_st_sec": float(dd["direct_st_cost_change_sec"].iloc[0]),
                    "ge_wedge_sec": float(dd["ge_cost_wedge_sec"].iloc[0]),
                    "sorting_wedge_sec": float(dd["sorting_wedge_sec"].iloc[0]),
                    "total_lt_sec": float(dd["total_lt_cost_change_sec"].iloc[0]),
                })
            rows.append(row)
    tab = pd.DataFrame(rows)
    for d in dirs:
        tab.to_csv(d / "summary_direct_vs_longrun.csv", index=False)
    print(tab.to_string(index=False))


if __name__ == "__main__":
    mode, rest = sys.argv[1], sys.argv[2:]
    {"suite": _suite, "summary": _summary}[mode](rest)
