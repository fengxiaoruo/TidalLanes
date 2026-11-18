"""Winner's-curse check for the top-100 tidal selection using a weekday split (gate5).

The mainline selects the top-100 reciprocal pairs by ``score_bpr_st`` on week-averaged
AM speeds.  If the AM speeds are noisy, ranking on the same noise that enters the
evaluation inflates the measured effect.  This script builds the selection on one
half of the week (odd weekdays: Mon/Wed/Fri) and evaluates it on the other half (even
weekdays: Tue/Thu), and vice versa.

For each split bundle it (1) inverts fundamentals and solves the anchored baseline
exactly as ``run_tidal_lane_diagnostics_gate5.py`` does, (2) runs the own top-100
selection ST/LT, (3) applies the other split's selection (matched by grid_id endpoints)
and the mainline top-100 selection, and (4) writes summary rows, pair overlaps, and a
README.  Split selections are also evaluated on the mainline (full-week) bundle.
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

from src.model.full_estimation import build_primitives as bp  # noqa: E402
from src.model.full_estimation import invert_fundamentals as inv  # noqa: E402
from src.model.full_estimation import parameters as pm  # noqa: E402
from src.model.full_estimation import stage2_counterfactuals as s2  # noqa: E402
from src.model.full_estimation.counterfactual_levels import (  # noqa: E402
    TidalLaneSelection,
    bpr_st_pair_score,
)
from src.model.full_estimation.equilibrium_solve import (  # noqa: E402
    MAINLINE_ALPHA,
    MAINLINE_BETA,
    MAINLINE_LAMBDA_PHYS,
    MAINLINE_PAIR_SELECTION,
    EquilibriumResult,
    resolve_kappa,
    solve_equilibrium,
)
from src.model.full_estimation.run_alpha_robustness_quick_gate5 import (  # noqa: E402
    solver_overrides_for_params,
)
from src.model.full_estimation.tidal_lane_diagnostics import summarise_tidal_pair  # noqa: E402

LOG = logging.getLogger("split_day_check")
TOP_K = 100
DELTA = 1.0


# --------------------------------------------------------------------------- #
# Bundle state
# --------------------------------------------------------------------------- #
class BundleState:
    def __init__(self, name: str, version_id: str, data_work: Path, params: pm.Parameters, skw: dict,
                 kappa: float = 1.0):
        self.name = name
        self.version_id = version_id
        self.prim = bp.load_primitives(data_work / "outputs" / version_id, period="AM")
        self.params = params
        self.fund = inv.invert_fundamentals(self.prim, params, tol=1.5e-4, max_iter=1800, kappa=kappa)
        self.kw = s2.build_congestion_solver_kwargs(
            self.prim, self.fund, params,
            **{"congestion_form": "anchored", "congestion_lambda": MAINLINE_LAMBDA_PHYS, **skw},
        )
        self.baseline: EquilibriumResult = s2.solve_baseline_endogenous(
            self.prim, self.fund, params, **self.kw,
        )
        gid = self.prim.node_grid_ids
        self.edge_key = {
            (str(gid[i]), str(gid[j])): int(k)
            for k, (i, j) in enumerate(zip(self.prim.edge_i, self.prim.edge_j))
        }

    def pair_keys(self, sel: TidalLaneSelection) -> list[tuple[str, str, str, str]]:
        gid = self.prim.node_grid_ids
        out = []
        for s, f in sel.treated_pairs:
            out.append((
                str(gid[self.prim.edge_i[s]]), str(gid[self.prim.edge_j[s]]),
                str(gid[self.prim.edge_i[f]]), str(gid[self.prim.edge_j[f]]),
            ))
        return out

    def selection_from_keys(
        self, keys: list[tuple[str, str, str, str]], delta: float = DELTA,
    ) -> tuple[TidalLaneSelection, dict]:
        """Map (slow_o, slow_d, fast_o, fast_d) grid keys onto this bundle's edges."""
        treated, ratios, applied_keys = [], [], []
        n_missing = n_ineligible = n_flipped = 0
        for so, sd, fo, fd in keys:
            s = self.edge_key.get((so, sd))
            f = self.edge_key.get((fo, fd))
            if s is None or f is None:
                n_missing += 1
                continue
            if self.prim.n_kl[f] - delta < 1.0:
                n_ineligible += 1
                continue
            if self.prim.tau_obs_min[s] < self.prim.tau_obs_min[f]:
                n_flipped += 1
            treated.append((s, f))
            ratios.append(float(self.prim.tau_obs_min[s] / max(self.prim.tau_obs_min[f], 1e-9)))
            applied_keys.append((so, sd, fo, fd))
        sel = TidalLaneSelection(
            treated_pairs=np.asarray(treated, dtype=int).reshape(-1, 2),
            asym_ratio=np.asarray(ratios, dtype=float),
            n_treated=len(treated), n_skipped=n_missing + n_ineligible, skip_reason=[],
        )
        info = {
            "n_source_pairs": len(keys),
            "n_applied": len(treated),
            "n_missing_edge": n_missing,
            "n_ineligible_fast_lane": n_ineligible,
            "n_direction_flipped": n_flipped,
            "applied_keys": applied_keys,
        }
        return sel, info

    def score_of(self, sel: TidalLaneSelection) -> np.ndarray:
        return np.array([
            bpr_st_pair_score(self.prim, int(s), int(f), self.baseline.Xi_kl)
            for s, f in sel.treated_pairs
        ], dtype=float)


def run_tidal_with_selection(
    st: BundleState, sel: TidalLaneSelection, *, horizon: str, label: str, delta: float = DELTA,
) -> EquilibriumResult:
    """Variant of stage2_counterfactuals.run_tidal_endogenous with an explicit pair list."""
    kw = s2._solver_kwargs(st.fund, **st.kw)
    n_cf = st.prim.n_kl.copy()
    for slow, fast in sel.treated_pairs:
        n_cf[slow] = n_cf[slow] + delta
        n_cf[fast] = n_cf[fast] - delta
    pin_kw: dict = {}
    if horizon == "short_run":
        pin_kw = dict(l_R_fixed=st.baseline.l_R, l_F_fixed=st.baseline.l_F)
    elif horizon != "long_run":
        raise ValueError(horizon)
    return solve_equilibrium(
        st.prim, st.fund.ubar_theta, st.fund.abar_theta, st.params,
        n_kl_override=n_cf,
        tau_min_init=st.prim.tau_obs_min.copy(),
        endogenous_congestion=True,
        label=f"{label}_{horizon}",
        **pin_kw,
        **kw,
    )


def evaluate(
    st: BundleState, sel: TidalLaneSelection, *, source: str, out: Path, info: dict | None = None,
) -> dict:
    label = f"tidal_top{TOP_K}_d{DELTA}"
    eq_st = run_tidal_with_selection(st, sel, horizon="short_run", label=label)
    eq_lt = run_tidal_with_selection(st, sel, horizon="long_run", label=label)
    out.mkdir(parents=True, exist_ok=True)
    eq_st.write(out / "short_run")
    eq_lt.write(out / "long_run")
    frame = sel.to_frame(st.prim)
    gid = st.prim.node_grid_ids
    for col in ("slow_i", "slow_j", "fast_i", "fast_j"):
        frame[col.replace("_i", "_grid_o").replace("_j", "_grid_d")] = [str(gid[v]) for v in frame[col]]
    frame["score_bpr_st"] = st.score_of(sel)
    frame.to_csv(out / "selected_pairs.csv", index=False)
    row, _, edges = summarise_tidal_pair(
        spec_label=f"{st.name}:{source}", top_k=TOP_K, delta_lanes=DELTA,
        baseline=st.baseline, short_run=eq_st, long_run=eq_lt,
        prim=st.prim, params=st.params, abar_theta=st.fund.abar_theta, sel=sel,
    )
    edges.to_csv(out / "treated_edge_detail.csv", index=False)
    rec = {
        "eval_bundle": st.name,
        "eval_version_id": st.version_id,
        "selection_source": source,
        "n_pairs": int(sel.n_treated),
        "mean_score_bpr_st_in_eval_bundle": float(np.mean(frame["score_bpr_st"])) if len(frame) else np.nan,
        "Wbar_base": float(st.baseline.Wbar),
        "Wbar_pct_change_st": row["Wbar_pct_change_st"],
        "Wbar_pct_change_lt": row["Wbar_pct_change_lt"],
        "direct_st_cost_change_sec": row["direct_st_cost_change_sec"],
        "total_lt_cost_change_sec": row["total_lt_cost_change_sec"],
        "physical_time_saved_st_sec_per_commuter": row["physical_time_saved_st_sec_per_commuter"],
        "physical_time_saved_lt_sec_per_commuter": row["physical_time_saved_lt_sec_per_commuter"],
        "physical_time_saved_st_person_hours": row["physical_time_saved_st_person_hours"],
        "physical_time_saved_lt_person_hours": row["physical_time_saved_lt_person_hours"],
        "st_treated_xi_weighted_delta_tau_min": row.get("st_treated_xi_weighted_delta_tau_min", np.nan),
        "lt_treated_xi_weighted_delta_tau_min": row.get("lt_treated_xi_weighted_delta_tau_min", np.nan),
        "st_treated_mean_delta_tau_min": row.get("st_treated_mean_delta_tau_min", np.nan),
        "lt_treated_mean_delta_tau_min": row.get("lt_treated_mean_delta_tau_min", np.nan),
        "st_converged": bool(eq_st.converged),
        "lt_converged": bool(eq_lt.converged),
        "st_iter": int(eq_st.n_outer),
        "lt_iter": int(eq_lt.n_outer),
    }
    if info:
        rec.update({k: v for k, v in info.items() if k != "applied_keys"})
    return rec


def jaccard(a: set, b: set) -> float:
    return len(a & b) / len(a | b) if (a | b) else np.nan


def undirected_key(k: tuple[str, str, str, str]) -> tuple[str, str]:
    return tuple(sorted((k[0], k[1])))  # type: ignore[return-value]


# --------------------------------------------------------------------------- #
def main(argv=None) -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo-root", default=str(DATA_WORK))
    p.add_argument("--odd-version-id", default="model_input_edgeproj_v2_lcc_gate5_am_odd_days_v1")
    p.add_argument("--even-version-id", default="model_input_edgeproj_v2_lcc_gate5_am_even_days_v1")
    p.add_argument("--mainline-version-id", default="model_input_edgeproj_v2_lcc_gate5_v1")
    p.add_argument(
        "--mainline-top100",
        default="tidal_lane_diagnostics_gate5_bpr_pos05_physical_time_v2/top100_d1.0/selected_pairs.csv",
    )
    p.add_argument("--output-name", default="selection_split_day_check_gate5_v1")
    p.add_argument("--alpha", type=float, default=MAINLINE_ALPHA)
    p.add_argument("--beta", type=float, default=MAINLINE_BETA)
    p.add_argument("--max-outer", type=int, default=500)
    p.add_argument("--congestion-form", choices=("anchored", "ce_floor", "bpr"), default="anchored")
    p.add_argument("--congestion-lambda", type=float, default=MAINLINE_LAMBDA_PHYS)
    p.add_argument("--xi-floor-rel", type=float, default=0.0)
    p.add_argument("--kappa-rule", default="fixed1",
                   help="fixed1, spectral, or a number; set on the mainline bundle and reused for both halves")
    args = p.parse_args(argv)

    data_work = Path(args.repo_root)
    out = data_work / "outputs" / args.output_name
    if out.exists():
        raise SystemExit(f"Refusing to overwrite existing output: {out}")
    out.mkdir(parents=True)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s | %(message)s",
        handlers=[logging.FileHandler(out / "run.log"), logging.StreamHandler()],
    )
    params = replace(
        pm.Parameters(), alpha=float(args.alpha), beta=float(args.beta),
        alpha_source="Literature mid-range gross production spillover (mainline pos05)",
        beta_source="AA(2022) Seattle beta held",
    )
    params.write(out / "parameters.json")
    skw = {k: v for k, v in solver_overrides_for_params(params).items() if v is not None}
    skw["max_outer"] = max(args.max_outer, skw.get("max_outer", args.max_outer))
    skw.update(congestion_form=args.congestion_form, congestion_lambda=args.congestion_lambda)
    if args.xi_floor_rel > 0:
        skw["xi_floor_rel"] = args.xi_floor_rel
    LOG.info("solver kwargs: %s", skw)

    kappa, kinfo = resolve_kappa(bp.load_primitives(data_work / "outputs" / args.mainline_version_id, period="AM"),
                                 rule=args.kappa_rule)
    (out / "kappa.json").write_text(json.dumps({"set_on": args.mainline_version_id, **kinfo}, indent=2, default=float))
    LOG.info("kappa %.4f (%s)", kappa, args.kappa_rule)

    states: dict[str, BundleState] = {}
    for name, vid in (("odd", args.odd_version_id), ("even", args.even_version_id), ("mainline", args.mainline_version_id)):
        LOG.info("Loading %s (%s): invert + anchored baseline", name, vid)
        states[name] = BundleState(name, vid, data_work, params, skw, kappa=kappa)
        st = states[name]
        st.baseline.write(out / name / "baseline")
        LOG.info(
            "%s: N=%d M=%d Wbar=%.6f converged=%s", name, st.prim.n_nodes, st.prim.n_edges,
            st.baseline.Wbar, st.baseline.converged,
        )

    # edge-set comparison across bundles
    edge_sets = {n: set(st.edge_key) for n, st in states.items()}
    common_edges = edge_sets["odd"] & edge_sets["even"] & edge_sets["mainline"]
    bundle_rows = [
        {
            "bundle": n, "version_id": st.version_id, "n_nodes": st.prim.n_nodes,
            "n_edges": st.prim.n_edges, "n_edges_common_all3": len(common_edges),
            "baseline_Wbar": float(st.baseline.Wbar), "baseline_converged": bool(st.baseline.converged),
        }
        for n, st in states.items()
    ]
    pd.DataFrame(bundle_rows).to_csv(out / "bundle_summary.csv", index=False)

    # own selections (score_bpr_st, top-100), exactly as run_tidal_endogenous does
    own_sel: dict[str, TidalLaneSelection] = {}
    own_keys: dict[str, list] = {}
    rows: list[dict] = []
    for name, st in states.items():
        LOG.info("%s: own top-%d selection ST + LT", name, TOP_K)
        _, sel = s2.run_tidal_endogenous(
            st.prim, st.fund, st.params, st.baseline,
            top_k=TOP_K, delta=DELTA, horizon="short_run",
            pair_selection=MAINLINE_PAIR_SELECTION, **st.kw,
        )
        own_sel[name] = sel
        own_keys[name] = st.pair_keys(sel)
        rows.append(evaluate(st, sel, source=f"own_{name}", out=out / name / "own_top100"))

    # mainline top-100 from the retained diagnostics run (indices are mainline-bundle indices)
    ml = pd.read_csv(data_work / "outputs" / args.mainline_top100)
    gid = states["mainline"].prim.node_grid_ids
    mainline_keys = [
        (str(gid[r.slow_i]), str(gid[r.slow_j]), str(gid[r.fast_i]), str(gid[r.fast_j]))
        for r in ml.itertuples(index=False)
    ]
    same = set(mainline_keys) == set(own_keys["mainline"])
    LOG.info("mainline own selection reproduces retained top-100 file: %s", same)

    # cross evaluations
    transfers = [
        ("odd", "even"), ("even", "odd"),
        ("mainline", "odd"), ("mainline", "even"),
        ("odd", "mainline"), ("even", "mainline"),
    ]
    for src, dst in transfers:
        st = states[dst]
        keys = own_keys[src] if src != "mainline" else mainline_keys
        sel, info = st.selection_from_keys(keys)
        LOG.info("%s pairs -> %s bundle: %s", src, dst, {k: v for k, v in info.items() if k != "applied_keys"})
        rows.append(evaluate(st, sel, source=f"from_{src}", out=out / dst / f"from_{src}", info=info))
        # matched comparison: the same surviving pairs evaluated on the source bundle
        if info["n_applied"] < info["n_source_pairs"]:
            st_src = states[src]
            sel_r, info_r = st_src.selection_from_keys(info["applied_keys"])
            assert info_r["n_applied"] == info["n_applied"]
            LOG.info("%s own selection restricted to the %d pairs transferable to %s", src, info_r["n_applied"], dst)
            rows.append(evaluate(
                st_src, sel_r, source=f"own_{src}_restricted_to_{dst}",
                out=out / src / f"own_top100_restricted_to_{dst}", info=info_r,
            ))

    summary = pd.DataFrame(rows)
    summary.to_csv(out / "summary.csv", index=False)

    # overlaps
    sets = {"odd": set(own_keys["odd"]), "even": set(own_keys["even"]), "mainline": set(mainline_keys)}
    usets = {k: {undirected_key(x) for x in v} for k, v in sets.items()}
    overlap = {
        "directed_pair_key": "(slow_grid_o, slow_grid_d, fast_grid_o, fast_grid_d)",
        "odd_even_intersection": len(sets["odd"] & sets["even"]),
        "odd_even_jaccard": jaccard(sets["odd"], sets["even"]),
        "odd_mainline_intersection": len(sets["odd"] & sets["mainline"]),
        "odd_mainline_jaccard": jaccard(sets["odd"], sets["mainline"]),
        "even_mainline_intersection": len(sets["even"] & sets["mainline"]),
        "even_mainline_jaccard": jaccard(sets["even"], sets["mainline"]),
        "all_three_intersection": len(sets["odd"] & sets["even"] & sets["mainline"]),
        "undirected_corridor_odd_even_intersection": len(usets["odd"] & usets["even"]),
        "undirected_corridor_odd_even_jaccard": jaccard(usets["odd"], usets["even"]),
        "undirected_corridor_odd_mainline_intersection": len(usets["odd"] & usets["mainline"]),
        "undirected_corridor_even_mainline_intersection": len(usets["even"] & usets["mainline"]),
        "mainline_own_selection_reproduces_retained_file": bool(same),
    }
    (out / "overlap.json").write_text(json.dumps(overlap, indent=2), encoding="utf-8")

    # winner's-curse ratios
    def pick(bundle, source, col):
        r = summary[(summary.eval_bundle == bundle) & (summary.selection_source == source)]
        if r.empty:  # restricted run not needed when every pair transferred
            src = source.split("_restricted_to_")[0]
            r = summary[(summary.eval_bundle == bundle) & (summary.selection_source == src)]
        return float(r[col].iloc[0])

    curse = {}
    for col in (
        "direct_st_cost_change_sec", "Wbar_pct_change_st", "Wbar_pct_change_lt",
        "st_treated_xi_weighted_delta_tau_min", "lt_treated_xi_weighted_delta_tau_min",
        "physical_time_saved_st_sec_per_commuter",
    ):
        own_odd, own_even = pick("odd", "own_odd", col), pick("even", "own_even", col)
        x_odd, x_even = pick("odd", "from_even", col), pick("even", "from_odd", col)
        own_mean, cross_mean = 0.5 * (own_odd + own_even), 0.5 * (x_odd + x_even)
        # matched: same surviving pairs, in-sample (source bundle) vs out-of-sample (other bundle)
        m_odd = pick("odd", "own_odd_restricted_to_even", col)    # odd pairs on odd, restricted
        m_even = pick("even", "own_even_restricted_to_odd", col)  # even pairs on even, restricted
        matched_own_mean = 0.5 * (m_odd + m_even)
        curse[col] = {
            "own_odd": own_odd, "own_even": own_even, "own_mean": own_mean,
            "cross_on_odd_from_even": x_odd, "cross_on_even_from_odd": x_even, "cross_mean": cross_mean,
            "cross_over_own": cross_mean / own_mean if own_mean else np.nan,
            "own_minus_cross_pct_of_own": 100.0 * (own_mean - cross_mean) / own_mean if own_mean else np.nan,
            "matched_own_odd_restricted": m_odd, "matched_own_even_restricted": m_even,
            "matched_own_mean": matched_own_mean,
            "matched_cross_over_own": cross_mean / matched_own_mean if matched_own_mean else np.nan,
            "matched_own_minus_cross_pct_of_own": 100.0 * (matched_own_mean - cross_mean) / matched_own_mean if matched_own_mean else np.nan,
            "mainline_own": pick("mainline", "own_mainline", col),
            "mainline_from_odd": pick("mainline", "from_odd", col),
            "mainline_from_even": pick("mainline", "from_even", col),
        }
    (out / "winners_curse_ratios.json").write_text(json.dumps(curse, indent=2), encoding="utf-8")
    print(summary[[
        "eval_bundle", "selection_source", "n_pairs", "Wbar_pct_change_st", "Wbar_pct_change_lt",
        "direct_st_cost_change_sec", "st_treated_xi_weighted_delta_tau_min",
        "lt_treated_xi_weighted_delta_tau_min",
    ]].to_string(index=False))
    print(json.dumps(overlap, indent=2))
    LOG.info("Wrote %s", out)


if __name__ == "__main__":
    main()
