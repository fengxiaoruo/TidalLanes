"""
Tidal lanes defined on roads, congestion on roads (2026-11-10)
==============================================================
Same model, parameters, kappa rule, inversion and anchors as
run_tidal_lane_diagnostics_gate5.py; two changes, both through
equilibrium_solve.RoadLayer:
  1. congestion: grid-edge flows are loaded onto road units (OSM arcs) along the edge
     paths; lanes and the flow/lane deviation live on road units; an edge's time is the
     path-time-weighted mean of its units' deviations (see RoadLayer).
  2. policy: a tidal corridor is the road path of one grid edge (slow direction) and the
     opposite carriageway of those arcs (fast direction); +delta lanes on the slow arcs,
     -delta on the opposite arcs. Every grid edge that uses a treated arc is affected.
Selection (road analogue of the mainline score_bpr_st): corridor score =
    sum_slow X_u (t_AM_u - t_FF_u)+  -  sum_opposite X_u (t_AM_u - t_FF_u)+
with X = baseline unit flow; eligible if >= 80% of the corridor km has an opposite arc
with >= 1 + delta lanes (only those arcs are treated); greedy, a corridor is skipped if
less than half of its slow km is new or it would reverse an already treated arc.
Specs: top-K corridors and road-km budgets (distinct treated slow-direction km).
--identity-check: D = H = I (each edge its own road) must reproduce the edge-level run.
Inputs read only. Output: data_work/outputs/<output-name>/ (refuses to overwrite)
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import sparse

DATA_WORK = Path(__file__).resolve().parents[3]
if str(DATA_WORK) not in sys.path:
    sys.path.insert(0, str(DATA_WORK))

from src.model.full_estimation import build_primitives as bp  # noqa: E402
from src.model.full_estimation import invert_fundamentals as inv  # noqa: E402
from src.model.full_estimation import parameters as pm  # noqa: E402
from src.model.full_estimation import stage2_counterfactuals as s2  # noqa: E402
from src.model.full_estimation.equilibrium_solve import (  # noqa: E402
    RoadLayer, compute_observation_anchors, resolve_kappa, solve_equilibrium)
from src.model.full_estimation.run_tidal_lane_diagnostics_gate5 import _skw  # noqa: E402
from src.model.full_estimation.run_tidal_lane_diagnostics_gate5 import MAINLINE_ALPHA, MAINLINE_BETA  # noqa: E402
from src.model.full_estimation.run_alpha_robustness_quick_gate5 import solver_overrides_for_params  # noqa: E402
from dataclasses import replace  # noqa: E402
from src.model.full_estimation.tidal_lane_diagnostics import od_cost_decomposition  # noqa: E402

O = DATA_WORK / "outputs"
OPP_KM_MIN = 0.8
NEW_KM_MIN = 0.5


def load_layer(prim, road_dir: Path):
    U = pd.read_parquet(road_dir / "units.parquet").sort_values("unit").reset_index(drop=True)
    inc = pd.read_parquet(road_dir / "incidence.parquet")
    gid = np.asarray(prim.node_grid_ids)
    eidx = pd.Series(np.arange(prim.n_edges), index=pd.MultiIndex.from_arrays([gid[prim.edge_i], gid[prim.edge_j]]))
    e = eidx.reindex(pd.MultiIndex.from_arrays([inc.grid_o, inc.grid_d])).to_numpy()
    if np.isnan(e).any():
        raise SystemExit("incidence rows without a model edge")
    e = e.astype(int)
    M, K = prim.n_edges, len(U)
    if len(np.unique(e)) != M:
        raise SystemExit("model edges without a road path")
    D = sparse.csr_matrix((np.ones(len(inc)), (e, inc.unit.to_numpy())), shape=(M, K))
    H = sparse.csr_matrix((inc.share.to_numpy(), (e, inc.unit.to_numpy())), shape=(M, K))
    return U, inc.assign(e=e), D, H


def corridors(U, inc, X, delta, z=None):
    """z: per-unit score weight. Default (score 'arc_delay', v1 runs): X_u (t_AM_u - t_FF_u)+.
    score 'edge_delay' (v2): z = H.T @ [Xi_e (tau_obs_e - floor_e)+], the edge-level mainline
    score_bpr_st distributed to units by time share (identical to score_bpr_st when D = H = I)."""
    tdel = np.maximum(U.t_AM_min.to_numpy() - U.t_FF_min.to_numpy(), 0.0)
    w_unit = X * tdel if z is None else z
    km = U.length_m.to_numpy() / 1000
    opp = U.opp_unit.to_numpy()
    lanes = U.lanes.to_numpy()
    rows = []
    for e, g in inc.groupby("e"):
        s = g.unit.to_numpy()
        o = opp[s]
        ok = (o >= 0) & (lanes[np.maximum(o, 0)] - delta >= 1.0)
        if not ok.any() or km[s][ok].sum() < OPP_KM_MIN * km[s].sum():
            continue
        s_t, o_t = s[ok], o[ok]
        score = float(w_unit[s_t].sum() - w_unit[o_t].sum())
        rows.append((e, g.grid_o.iloc[0], g.grid_d.iloc[0], s_t, o_t, float(km[s_t].sum()), score))
    rows.sort(key=lambda r: -r[-1])
    return rows


def select(rows, km_all, *, top_k=None, budget_km=None):
    slow, fast, chosen, tot = set(), set(), [], 0.0
    for r in rows:
        if r[-1] <= 0:
            break
        s, o = r[3], r[4]
        if any(u in fast for u in s) or any(u in slow for u in o):
            continue
        new = [u for u in s if u not in slow]
        if km_all[new].sum() < NEW_KM_MIN * km_all[s].sum():
            continue
        slow.update(s.tolist()); fast.update(o.tolist()); chosen.append(r)
        tot += km_all[new].sum()
        if (top_k and len(chosen) >= top_k) or (budget_km and tot >= budget_km):
            break
    return chosen, np.array(sorted(slow), int), np.array(sorted(fast), int), tot


def select_foc(rows, km_all, layer, xi, tau_ff, tau_obs, form, lam, delta, *,
               top_k=None, budget_km=None, per_km=False):
    """First-order welfare greedy (score 'foc', v3).
    AA envelope at fixed flows: d ln W is proportional to -sum_e Xi_e d ln tau_e (A_kl = 1/(kappa tau_kl)).
    Gain of a corridor given the lanes already chosen:
        G = sum_e Xi_e [ln tau_e(n_cur) - ln tau_e(n_cur + corridor)],
    tau from the road mapping (_road_tau_update) at baseline flows, exact in the lane change
    (no linearisation), so overlaps, floors and the fast-direction loss are all counted.
    Lazy greedy: re-evaluate the best stale candidate, accept it if it still beats the next one.
    per_km: rank by G / new slow km (for km budgets)."""
    import heapq
    from src.model.full_estimation.equilibrium_solve import _road_tau_update

    def ltau(n):
        return np.log(_road_tau_update(form=form, lam=lam, tau_ff=tau_ff, tau_obs=tau_obs,
                                       Xi_kl=xi, road=layer.with_lanes(n)))

    n_cur = layer.n.copy()
    l_cur = ltau(n_cur)
    slow, fast = np.zeros(len(n_cur), bool), np.zeros(len(n_cur), bool)

    def trial(r):
        s, o = r[3], r[4]
        if fast[s].any() or slow[o].any():
            return None
        ns, no = s[~slow[s]], o[~fast[o]]
        newkm = km_all[ns].sum()
        if newkm < NEW_KM_MIN * km_all[s].sum():
            return None
        n_try = n_cur.copy(); n_try[ns] += delta; n_try[no] -= delta
        g = float(xi @ (l_cur - ltau(n_try)))
        return (g / newkm if per_km else g), g, ns, no, newkm, n_try

    heap = []
    for i, r in enumerate(rows):
        t = trial(r)
        if t is not None and t[1] > 0:
            heap.append((-t[0], i))
    heapq.heapify(heap)
    chosen, tot, gains = [], 0.0, []
    while heap:
        _, i = heapq.heappop(heap)
        t = trial(rows[i])
        if t is None or t[1] <= 0:
            continue
        if heap and t[0] < -heap[0][0]:
            heapq.heappush(heap, (-t[0], i))
            continue
        _, g, ns, no, newkm, n_try = t
        n_cur = n_try; l_cur = ltau(n_cur)
        slow[ns] = True; fast[no] = True
        r = rows[i]
        chosen.append((r[0], r[1], r[2], r[3], r[4], r[5], g)); gains.append(g)
        tot += newkm
        if (top_k and len(chosen) >= top_k) or (budget_km and tot >= budget_km):
            break
    return chosen, np.where(slow)[0], np.where(fast)[0], tot, float(np.sum(gains))


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--version-id", required=True)
    ap.add_argument("--road-dir", default=None, help="road_layer_* output (default road_layer_<version-id>_v1)")
    ap.add_argument("--output-name", required=True)
    ap.add_argument("--congestion-form", choices=["ce_floor", "bpr", "anchored"], default="ce_floor")
    ap.add_argument("--congestion-lambda", type=float, default=0.25)
    ap.add_argument("--xi-floor-rel", type=float, default=0.01)
    ap.add_argument("--kappa-rule", default="spectral")
    ap.add_argument("--delta", type=float, default=1.0)
    ap.add_argument("--top-k", default="25,50,100")
    ap.add_argument("--budget-km", default="150,300")
    ap.add_argument("--score", choices=["foc", "edge_delay", "arc_delay"], default="edge_delay",
                    help="corridor score; foc = first-order welfare greedy (v3), edge_delay = mainline "
                         "score_bpr_st on roads (v2), arc_delay = arc-speed delays (v1)")
    ap.add_argument("--opp-km-min", type=float, default=None,
                    help="min share of corridor km with a usable opposite carriageway (default OPP_KM_MIN=0.8)")
    ap.add_argument("--apply-treated", default=None,
                    help="output dir of another road-level run: apply its treated arcs (by arc_row) "
                         "for every spec in it, instead of selecting here (cross-topology check)")
    ap.add_argument("--km-per-km", action="store_true",
                    help="foc only: for km budgets also run the gain-per-km ranking (labels km<B>pk)")
    ap.add_argument("--edge-selection-variants", action="store_true",
                    help="also apply the edge-level top100 selection on roads (decomposition)")
    ap.add_argument("--identity-check", action="store_true",
                    help="D = H = I: must reproduce the edge-level tidal top100 (edge selection)")
    args = ap.parse_args(argv)
    if args.opp_km_min is not None:
        global OPP_KM_MIN
        OPP_KM_MIN = float(args.opp_km_min)
    out = O / args.output_name
    if out.exists():
        raise SystemExit(f"{out} exists; choose a new --output-name")

    prim = bp.load_primitives(O / args.version_id, period="AM")
    params = replace(pm.Parameters(), alpha=float(MAINLINE_ALPHA), beta=float(MAINLINE_BETA))   # as the mainline runner
    skw = {k: v for k, v in solver_overrides_for_params(params).items() if v is not None}
    skw["max_outer"] = max(500, skw.get("max_outer", 500))
    kappa, kinfo = resolve_kappa(prim, args.kappa_rule)
    fund = inv.invert_fundamentals(prim, params, tol=1.5e-4, max_iter=1800, kappa=kappa)
    anchors = compute_observation_anchors(prim, fund.ubar_theta, fund.abar_theta, params, chi=fund.chi, kappa=fund.kappa)
    xi_obs = anchors["xi_obs_kl"]
    kw = _skw(fund, congestion_form=args.congestion_form, congestion_lambda=args.congestion_lambda, **skw)
    kw.update(xi_obs_kl=xi_obs, n_obs_kl=anchors["n_obs_kl"], tau_obs_anchor=anchors["tau_obs_anchor"])
    res = {"version_id": args.version_id, "congestion_form": args.congestion_form, "lambda": args.congestion_lambda,
           "xi_floor_rel": args.xi_floor_rel, "kappa": kappa, "delta": args.delta}

    def solve(label, *, road=None, n_kl=None, pin=None):
        k = dict(kw)
        if pin is not None:
            k.update(l_R_fixed=pin.l_R, l_F_fixed=pin.l_F)
        return solve_equilibrium(prim, fund.ubar_theta, fund.abar_theta, params, tau_min_init=prim.tau_obs_min.copy(),
                                 endogenous_congestion=True, label=label, road=road, n_kl_override=n_kl, **k)

    def summarise(label, base, st, lt, extra):
        d = od_cost_decomposition(base, st, lt, prim, params, fund.abar_theta)
        return {"spec": label, **extra,
                "Wbar_pct_change_st": 100 * (st.Wbar / base.Wbar - 1), "Wbar_pct_change_lt": 100 * (lt.Wbar / base.Wbar - 1),
                "physical_time_saved_st_sec_per_commuter": d["physical_time_saved_st_sec_per_commuter"],
                "physical_time_saved_lt_sec_per_commuter": d["physical_time_saved_lt_sec_per_commuter"],
                "converged_st": st.converged, "converged_lt": lt.converged,
                "n_outer_st": st.n_outer, "n_outer_lt": lt.n_outer}

    out.mkdir(parents=True)
    (out / "kappa.json").write_text(json.dumps(kinfo, indent=2))
    rows_out = []

    if args.identity_check:
        xi_floor = args.xi_floor_rel * float(np.median(xi_obs))
        kw["xi_floor"] = xi_floor
        M = prim.n_edges
        I = sparse.identity(M, format="csr")
        from src.model.full_estimation.equilibrium_solve import _resolve_lanes
        n0 = _resolve_lanes(prim.n_kl)
        layer = RoadLayer(I, I, anchors["n_obs_kl"], n0, xi_obs.copy(), xi_floor)
        base_e = solve("edge_base")
        base_r = solve("road_id_base", road=layer)
        sel = s2.select_tidal_lane_pairs_by_rank(prim, top_k=100, delta=args.delta, rank="score_bpr_st", xi_kl=base_e.Xi_kl)
        n_cf = prim.n_kl.copy()
        for s, f in sel.treated_pairs:
            n_cf[s] += args.delta; n_cf[f] -= args.delta
        st_e, lt_e = solve("edge_st", n_kl=n_cf, pin=base_e), solve("edge_lt", n_kl=n_cf)
        lay_cf = layer.with_lanes(_resolve_lanes(n_cf))
        st_r, lt_r = solve("road_st", road=lay_cf, pin=base_r), solve("road_lt", road=lay_cf)
        rows_out.append(summarise("edge_level_top100", base_e, st_e, lt_e, {}))
        rows_out.append(summarise("road_identity_top100", base_r, st_r, lt_r, {}))
        res["identity_max_abs_dlog_tau_base"] = float(np.max(np.abs(np.log(base_e.tau_min_kl / base_r.tau_min_kl))))
        res["identity_max_abs_dlog_tau_lt"] = float(np.max(np.abs(np.log(lt_e.tau_min_kl / lt_r.tau_min_kl))))
    else:
        road_dir = O / (args.road_dir or f"road_layer_{args.version_id}_v1")
        U, inc, D, H = load_layer(prim, road_dir)
        x_obs = np.asarray(D.T @ xi_obs).ravel()
        on = U.on_path.to_numpy()
        x_floor = args.xi_floor_rel * float(np.median(x_obs[on]))
        n_obs = np.clip(U.lanes.to_numpy(dtype=float), 1.0, None)
        layer = RoadLayer(D, H, n_obs, n_obs.copy(), x_obs, x_floor)
        res.update({"road_dir": road_dir.name, "units": int(len(U)), "x_obs_median_on_path": float(np.median(x_obs[on])),
                    "x_floor": x_floor})
        base = solve("road_base", road=layer)
        res["baseline_converged"] = bool(base.converged)
        res["baseline_max_abs_dlog_tau_vs_obs"] = float(np.max(np.abs(np.log(base.tau_min_kl / prim.tau_obs_min))))
        base.write(out / "baseline")
        X = layer.unit_flow(base.Xi_kl)
        km_all = U.length_m.to_numpy() / 1000
        if args.score == "edge_delay":
            floor = np.minimum(prim.tau_ff_min, prim.tau_obs_min)
            w = base.Xi_kl * np.maximum(prim.tau_obs_min - floor, 0.0)
            z = np.asarray(H.T @ w).ravel()
        else:
            z = None   # foc: eligibility only, the score is computed inside select_foc
        res["score"] = args.score
        res["opp_km_min"] = OPP_KM_MIN
        cand = corridors(U, inc, X, args.delta, z=z)
        res["eligible_corridors"] = len(cand)
        specs = [("top", int(k), False) for k in args.top_k.split(",") if k] + [("km", float(b), False) for b in args.budget_km.split(",") if b]
        if args.score == "foc" and args.km_per_km:
            specs += [("km", float(b), True) for b in args.budget_km.split(",") if b]
        sel_rows, treated_rows = [], []
        tau_obs_a = anchors["tau_obs_anchor"]
        if args.apply_treated:
            T = pd.read_parquet(O / args.apply_treated / "treated_units.parquet")
            unit_of_arc = pd.Series(U.unit.to_numpy(), index=U.arc_row.to_numpy())
            res["apply_treated"] = args.apply_treated
            for label, g in T.groupby("spec", sort=False):
                u = unit_of_arc.reindex(g.arc_row.to_numpy()).to_numpy()
                found = np.isfinite(u)
                slow = u[found & (g.dir.to_numpy() == "slow")].astype(int)
                fast = u[found & (g.dir.to_numpy() == "fast")].astype(int)
                n_cf = n_obs.copy(); n_cf[slow] += args.delta; n_cf[fast] -= args.delta
                n_cf = np.clip(n_cf, 1.0, None)
                lay = layer.with_lanes(n_cf)
                st, lt = solve(f"x_{label}_st", road=lay, pin=base), solve(f"x_{label}_lt", road=lay)
                rows_out.append(summarise(f"applied_{label}", base, st, lt, {
                    "treated_slow_km_source": round(float(g.length_m[g.dir == "slow"].sum() / 1000), 1),
                    "treated_slow_km": round(float(km_all[slow].sum()), 1),
                    "treated_fast_km": round(float(km_all[fast].sum()), 1),
                    "arcs_not_in_layer": int((~found).sum())}))
                print(rows_out[-1], flush=True)
            specs = []
        for kind, v, pk in specs:
            tk, bk = (v if kind == "top" else None), (v if kind == "km" else None)
            fo = np.nan
            if args.score == "foc":
                chosen, slow, fast, tot, fo = select_foc(cand, km_all, layer, base.Xi_kl, prim.tau_ff_min, tau_obs_a,
                                                         args.congestion_form, args.congestion_lambda, args.delta,
                                                         top_k=tk, budget_km=bk, per_km=pk)
            else:
                chosen, slow, fast, tot = select(cand, km_all, top_k=tk, budget_km=bk)
            n_cf = n_obs.copy(); n_cf[slow] += args.delta; n_cf[fast] -= args.delta
            lay = layer.with_lanes(n_cf)
            label = (f"top{v}" if kind == "top" else f"km{int(v)}") + ("pk" if pk else "")
            st, lt = solve(f"{label}_st", road=lay, pin=base), solve(f"{label}_lt", road=lay)
            Xs = layer.unit_flow(st.Xi_kl)
            rows_out.append(summarise(label, base, st, lt, {
                "foc_index": fo, "foc_index_over_xi_sum": fo / float(base.Xi_kl.sum()) if np.isfinite(fo) else np.nan,
                "corridors": len(chosen), "treated_slow_km": round(float(km_all[slow].sum()), 1),
                "treated_fast_km": round(float(km_all[fast].sum()), 1),
                "edges_touched": int(np.unique(inc.e[inc.unit.isin(np.r_[slow, fast])]).size),
                "slow_unit_flow_base_median": float(np.median(X[slow])) if len(slow) else np.nan,
                "slow_unit_flow_change_st_pct": float(100 * (Xs[slow].sum() / X[slow].sum() - 1)) if len(slow) else np.nan}))
            ar = U.arc_row.to_numpy()
            treated_rows.append(pd.DataFrame({"spec": label, "dir": "slow", "arc_row": ar[slow], "length_m": U.length_m.to_numpy()[slow]}))
            treated_rows.append(pd.DataFrame({"spec": label, "dir": "fast", "arc_row": ar[fast], "length_m": U.length_m.to_numpy()[fast]}))
            for r in chosen:
                sel_rows.append({"spec": label, "edge": int(r[0]), "grid_o": r[1], "grid_d": r[2], "slow_km": r[5], "score": r[6]})
            print(label, rows_out[-1], flush=True)
        if args.edge_selection_variants:
            # decomposition: the mainline edge-level top100 selection, applied on roads
            sel = s2.select_tidal_lane_pairs_by_rank(prim, top_k=100, delta=args.delta, rank="score_bpr_st", xi_kl=base.Xi_kl)
            units_of = inc.groupby("e").unit.apply(np.asarray)
            opp = U.opp_unit.to_numpy()
            variants = {}
            sl = np.concatenate([units_of[s] for s, _ in sel.treated_pairs])
            fr = np.concatenate([units_of[f] for _, f in sel.treated_pairs])
            variants["edgesel_revpath_additive"] = (sl, fr, True)
            variants["edgesel_revpath_set"] = (np.unique(sl), np.unique(fr), False)
            o = opp[np.unique(sl)]
            variants["edgesel_opposite_set"] = (np.unique(sl), np.unique(o[o >= 0]), False)
            # only arcs with an opposite carriageway that keeps >= 1 lane (a physical tidal lane)
            su = np.unique(sl); ou = opp[su]
            okp = (ou >= 0) & (U.lanes.to_numpy()[np.maximum(ou, 0)] - args.delta >= 1.0)
            variants["edgesel_opposite_paired"] = (su[okp], np.unique(ou[okp]), False)
            for label, (su, fu, additive) in variants.items():
                n_cf = n_obs.copy()
                if additive:
                    np.add.at(n_cf, su, args.delta); np.add.at(n_cf, fu, -args.delta)
                else:
                    both = np.intersect1d(su, fu)
                    su2, fu2 = np.setdiff1d(su, both), np.setdiff1d(fu, both)
                    n_cf[su2] += args.delta; n_cf[fu2] -= args.delta
                n_cf = np.clip(n_cf, 1.0, None)
                lay = layer.with_lanes(n_cf)
                st, lt = solve(f"{label}_st", road=lay, pin=base), solve(f"{label}_lt", road=lay)
                rows_out.append(summarise(label, base, st, lt, {"corridors": int(sel.n_treated),
                                "treated_slow_km": round(float(km_all[np.unique(su)].sum()), 1),
                                "treated_fast_km": round(float(km_all[np.unique(fu)].sum()), 1)}))
                print(label, rows_out[-1], flush=True)
        pd.DataFrame(sel_rows).to_csv(out / "selected_corridors.csv", index=False)
        if treated_rows:
            pd.concat(treated_rows, ignore_index=True).to_parquet(out / "treated_units.parquet", index=False)
    pd.DataFrame(rows_out).to_csv(out / "tidal_road_level_summary.csv", index=False)
    (out / "run_meta.json").write_text(json.dumps(res, indent=2, default=float))
    print(json.dumps(res, indent=2, default=float))
    print(pd.DataFrame(rows_out).to_string())


if __name__ == "__main__":
    main()
