"""Phase C — composite counterfactuals: background policy then tidal top-K (gate5 mainline)."""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

DATA_WORK = Path(__file__).resolve().parents[3]
if str(DATA_WORK) not in sys.path:
    sys.path.insert(0, str(DATA_WORK))

from src.model.directional_cost.policy_counterfactuals import (
    PolicySpec,
    apply_policy_abar,
    apply_policy_ubar,
)
from src.model.full_estimation import stage2_counterfactuals as s2
from src.model.full_estimation.corridor_exposure import od_path_exposure
from src.model.full_estimation.equilibrium_solve import MAINLINE_PAIR_SELECTION, solve_equilibrium
from src.model.full_estimation.phase_metrics_common import (
    DEFAULT_TOP_K,
    DEFAULT_VERSION,
    data_work_root,
    load_fundamentals,
    solver_kwargs,
    tidal_selection,
    write_json,
)
from src.model.full_estimation.tidal_lane_diagnostics import od_cost_decomposition

LOG = logging.getLogger("phase_c")

COMPOSITE_SPECS: list[tuple[str, PolicySpec | None]] = [
    ("tidal_only", None),
    ("C1_fringe_transport_then_tidal", PolicySpec(
        name="fringe_transport_20pct", tau_scale=1.0, tau_scale_on_outer_edges=0.80,
    )),
    ("C2_outer_ring_amenity_then_tidal", PolicySpec(
        name="outer_ring_amenity_10pct", ubar_mult_on_outer_ring=1.10,
    )),
    ("C3_hub_access_then_tidal", PolicySpec(
        name="hub_access_15pct", tau_scale=1.0, tau_scale_on_hub_edges=0.85,
    )),
    ("C4_hub_productivity_then_tidal", PolicySpec(
        name="hub_productivity_10pct", abar_mult_on_hubs=1.10,
    )),
]


def _apply_policy_tau_ff(prim, spec: PolicySpec) -> np.ndarray:
    return s2._apply_tau_ff_policy(prim.tau_ff_min, prim, spec)


def run_tidal_from_state(
    prim,
    fund,
    params,
    state_eq,
    *,
    top_k: int,
    delta: float = 1.0,
    pair_selection: str = MAINLINE_PAIR_SELECTION,
    skw: dict,
    ubar_theta=None,
    abar_theta=None,
    label: str,
):
    ubar = fund.ubar_theta if ubar_theta is None else ubar_theta
    abar = fund.abar_theta if abar_theta is None else abar_theta
    sel = tidal_selection(
        prim, top_k=top_k, delta=delta, pair_selection=pair_selection,
        baseline=state_eq,
    )
    n_cf = prim.n_kl.copy()
    for slow, fast in sel.treated_pairs:
        n_cf[slow] = n_cf[slow] + delta
        n_cf[fast] = n_cf[fast] - delta
    eq = solve_equilibrium(
        prim, ubar, abar, params,
        n_kl_override=n_cf,
        tau_min_init=state_eq.tau_min_kl.copy(),
        endogenous_congestion=True,
        label=label,
        **skw,
    )
    return eq, sel


def run_composite(
    data_work: Path,
    *,
    version_id: str = DEFAULT_VERSION,
    top_k: int = DEFAULT_TOP_K,
    delta: float = 1.0,
    pair_selection: str = MAINLINE_PAIR_SELECTION,
    max_outer: int = 120,
    quick: bool = False,
) -> pd.DataFrame:
    prim, params, fund = load_fundamentals(data_work, version_id)
    skw = solver_kwargs(fund, prim, params, max_outer=max_outer)

    baseline = s2.solve_baseline_endogenous(prim, fund, params, **skw)
    rows: list[dict] = []
    specs = COMPOSITE_SPECS if not quick else [("tidal_only", None), ("C1_fringe_transport_then_tidal", COMPOSITE_SPECS[1][1])]

    for comp_id, bg_spec in specs:
        LOG.info("Composite %s", comp_id)
        if bg_spec is None:
            bg_eq = baseline
            ubar_bg, abar_bg = fund.ubar_theta, fund.abar_theta
            bg_label = "baseline"
        else:
            ubar_bg = apply_policy_ubar(fund.ubar_theta, prim, bg_spec)
            abar_bg = apply_policy_abar(fund.abar_theta, prim, bg_spec)
            tau_ff_bg = _apply_policy_tau_ff(prim, bg_spec)
            bg_eq = solve_equilibrium(
                prim, ubar_bg, abar_bg, params,
                tau_ff_override=tau_ff_bg,
                tau_min_init=baseline.tau_min_kl.copy(),
                endogenous_congestion=True,
                label=f"{bg_spec.name}_background",
                **skw,
            )
            bg_label = bg_spec.name

        sel = tidal_selection(
            prim, top_k=top_k, delta=delta, pair_selection=pair_selection,
            baseline=bg_eq,
        )
        # ST: pinned shares at background
        eq_st_pin = solve_equilibrium(
            prim, ubar_bg, abar_bg, params,
            n_kl_override=_n_override(prim, sel, delta),
            tau_min_init=bg_eq.tau_min_kl.copy(),
            l_R_fixed=bg_eq.l_R, l_F_fixed=bg_eq.l_F,
            endogenous_congestion=True,
            label=f"{comp_id}_tidal_st_pinned",
            **skw,
        )
        eq_lt, _ = run_tidal_from_state(
            prim, fund, params, bg_eq,
            top_k=top_k, delta=delta, pair_selection=pair_selection, skw=skw,
            ubar_theta=ubar_bg, abar_theta=abar_bg,
            label=f"{comp_id}_tidal_lt",
        )

        decomp = od_cost_decomposition(bg_eq, eq_st_pin, eq_lt, prim, params, abar_bg)
        treated = np.unique(sel.treated_pairs.ravel())
        exposure = od_path_exposure(prim, bg_eq.tau_min_kl, treated)

        slow = sel.treated_pairs[:, 0]
        pct_save = []
        for s in slow:
            base_t = bg_eq.tau_min_kl[s]
            lt_t = eq_lt.tau_min_kl[s]
            pct_save.append(100.0 * (1.0 - lt_t / max(base_t, 1e-12)))
        pct_save = np.asarray(pct_save)

        row = {
            "composite_id": comp_id,
            "background": bg_label,
            "top_k": top_k,
            "n_pairs": sel.n_treated,
            "Wbar_bg": bg_eq.Wbar,
            "Wbar_tidal_lt": eq_lt.Wbar,
            "Wbar_pct_change_lt": 100.0 * (eq_lt.Wbar / bg_eq.Wbar - 1.0),
            "Wbar_pct_change_st": 100.0 * (eq_st_pin.Wbar / bg_eq.Wbar - 1.0),
            "corridor_slow_median_pct_save_lt": float(np.median(pct_save)),
            "corridor_slow_mean_pct_save_lt": float(np.mean(pct_save)),
            **decomp,
            **exposure,
            "max_abs_log_l_R_change": float(np.max(np.abs(np.log(np.clip(eq_lt.l_R, 1e-12, None))
                                                         - np.log(np.clip(bg_eq.l_R, 1e-12, None))))),
            "max_abs_log_l_F_change": float(np.max(np.abs(np.log(np.clip(eq_lt.l_F, 1e-12, None))
                                                         - np.log(np.clip(bg_eq.l_F, 1e-12, None))))),
        }
        row["person_min_day_lt_total"] = row["total_lt_cost_change_sec"] / 60.0 * prim.Lbar
        row["od_path_exposed_share_pct"] = row.get("od_exposed_share_pct", np.nan)
        rows.append(row)

        comp_dir = data_work / "outputs" / "phase_c_composite_cf_gate5_v1" / comp_id
        comp_dir.mkdir(parents=True, exist_ok=True)
        bg_eq.write(comp_dir / "background")
        eq_lt.write(comp_dir / "tidal_lt")
        sel.to_frame(prim).to_csv(comp_dir / "selected_pairs.csv", index=False)

    return pd.DataFrame(rows)


def _n_override(prim, sel, delta: float) -> np.ndarray:
    n_cf = prim.n_kl.copy()
    for slow, fast in sel.treated_pairs:
        n_cf[slow] = n_cf[slow] + delta
        n_cf[fast] = n_cf[fast] - delta
    return n_cf


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo-root", default=str(DATA_WORK))
    p.add_argument("--version-id", default=DEFAULT_VERSION)
    p.add_argument("--output-name", default="phase_c_composite_cf_gate5_v1")
    p.add_argument("--top-k", type=int, default=DEFAULT_TOP_K)
    p.add_argument("--quick", action="store_true")
    p.add_argument("--max-outer", type=int, default=120)
    p.add_argument("--pair-selection", default=MAINLINE_PAIR_SELECTION)
    args = p.parse_args(argv)

    data_work = data_work_root(args.repo_root)
    out = data_work / "outputs" / args.output_name
    out.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s | %(message)s",
        handlers=[logging.FileHandler(out / "run.log"), logging.StreamHandler()],
    )

    df = run_composite(
        data_work,
        version_id=args.version_id,
        top_k=args.top_k,
        max_outer=args.max_outer,
        pair_selection=args.pair_selection,
        quick=args.quick,
    )
    df.to_csv(out / "composite_summary.csv", index=False)
    write_json(out / "summary.json", {
        "top_k": args.top_k,
        "n_composites": len(df),
        "primary_row": df[df["composite_id"] == "tidal_only"].iloc[0].to_dict() if len(df) else {},
    })
    LOG.info("Done — %s", out)
    cols = ["composite_id", "corridor_slow_median_pct_save_lt", "od_exposed_share_pct", "person_min_day_lt_total", "Wbar_pct_change_lt"]
    print(df[cols].to_string(index=False))


if __name__ == "__main__":
    main()
