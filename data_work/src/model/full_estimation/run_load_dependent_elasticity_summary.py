"""Summary tables for the load-dependent elasticity trial (2026-11-15).

Reads estimate/, cf/ and cf_decomp/ of outputs/<output-name>/ and writes (refusing to overwrite):
  summary.csv                    long table: section, item, value
  table_main_results.csv         welfare / time saved / heavy-light direct split / P2 ratio vs the 0.35 benchmark
  table_checks.csv               convergence, rho(A), floor shares, baseline-at-anchor, selection
  table_pe_observed_vs_model_flow_weights.csv
      partial-equilibrium lane-only accounting on the matched tidal pairs (both edges in the gate5 panel):
      dT_e = w_e * tau_obs_e * ((n_e / n_e')^beta_e - 1), flow held fixed, n' = n +/- 1 (>= 1),
      weights w = observed 07-09 panel flow exp(lq_am) or model baseline Xi. Shows how much the
      light-direction loss would weigh if model flows matched observed directional flows.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
DATA_WORK = HERE.parents[2]
sys.path.insert(0, str(DATA_WORK))
from src.model.full_estimation.run_load_dependent_elasticity_cf import edge_loads, load_schedules, make_beta  # noqa: E402

OUT = DATA_WORK / "outputs"


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-name", default="load_dependent_elasticity_trial_v1")
    a = ap.parse_args(argv)
    root = OUT / a.output_name
    if (root / "summary.csv").exists():
        raise SystemExit("summary.csv exists; refusing to overwrite")
    est = root / "estimate"
    r = pd.read_csv(root / "cf" / "results_by_variant.csv")
    rd = pd.read_csv(root / "cf_decomp" / "results_by_variant.csv")
    rd = rd[rd.variant != "bench_native_b035"]
    allr = pd.concat([r, rd], ignore_index=True)
    b = allr[allr.variant == "bench_native_b035"].iloc[0]

    cols = ["variant", "beta_e_mean_heavy_gain_lane", "beta_e_mean_light_lose_lane", "beta_e_mean_other",
            "Wbar_pct_st", "Wbar_pct_lt", "time_saved_sec_st", "time_saved_sec_lt",
            "direct_st_sec", "direct_st_sec_heavy_gain_lane", "direct_st_sec_light_lose_lane", "direct_st_sec_other",
            "direct_st_sec_heavy_gain_lane_time_change_at_base_flow", "direct_st_sec_light_lose_lane_time_change_at_base_flow",
            "P2_Wbar_pct_st", "P2_Wbar_pct_lt", "P1_over_P2_st", "P1_over_P2_lt", "description"]
    T = allr[cols].copy()
    T["Wbar_st_ratio_to_bench"] = T.Wbar_pct_st / b.Wbar_pct_st
    T["Wbar_lt_ratio_to_bench"] = T.Wbar_pct_lt / b.Wbar_pct_lt
    T.to_csv(root / "table_main_results.csv", index=False)
    ck = ["variant", "selection_equals_mainline", "base_converged", "base_n_outer", "base_max_abs_dlog_tau_vs_tau_obs",
          "st_converged", "st_n_outer", "st_max_log_change", "lt_converged", "lt_n_outer", "lt_max_log_change",
          "base_rho_A", "st_rho_A", "lt_rho_A", "st_share_tau_at_floor", "lt_share_tau_at_floor",
          "st_share_flow_below_xi_floor", "lt_share_flow_below_xi_floor", "P2_converged_st", "P2_converged_lt"]
    allr[ck].to_csv(root / "table_checks.csv", index=False)

    # ---- PE accounting with observed vs model flow weights
    e = pd.read_csv(est / "model_edge_load_positions.csv").sort_values("edge_idx").reset_index(drop=True)
    sel = pd.read_csv(OUT / "tidal_lane_diagnostics_osm1b_ce_floor_b035_fl001_ks_od2025_alldaytt0709_v1"
                      / "top100_d1.0" / "selected_pairs.csv")
    sch = load_schedules(est)
    roles = e.role.to_numpy()
    betas = {"flat_0.35": np.full(len(e), 0.35)}
    for nm, sk, scope, norm in [("Q_treated_raw", "Q", "treated", "raw"), ("Q_treated_meanpres", "Q", "treated", "meanpres"),
                                ("S_treated_meanpres", "S", "treated", "meanpres"), ("C_treated_meanpres", "C", "treated", "meanpres"),
                                ("C_treated_raw", "C", "treated", "raw")]:
        ld, _, _ = edge_loads(est, sch[sk]["measure"])
        betas[nm], _ = make_beta(sch[sk], ld, roles, scope, norm, 0.10, 0.50)
    h, l = e.loc[sel.slow_edge_idx].reset_index(drop=True), e.loc[sel.fast_edge_idx].reset_index(drop=True)
    ok = (h.lq_am.notna() & l.lq_am.notna()).to_numpy()
    rows = []
    for wname in ("observed_panel_flow_07_09", "model_baseline_Xi"):
        wh = np.exp(h.lq_am) if wname.startswith("obs") else h.Xi_base
        wl = np.exp(l.lq_am) if wname.startswith("obs") else l.Xi_base
        for bn, bv in betas.items():
            bh, bl = bv[sel.slow_edge_idx.to_numpy()], bv[sel.fast_edge_idx.to_numpy()]
            nh, nl = np.maximum(h.n_lanes, 1), np.maximum(l.n_lanes, 1)
            dh = wh * h.tau_obs * ((nh / (nh + 1)) ** bh - 1)
            dl = wl * l.tau_obs * ((nl / np.maximum(nl - 1, 1)) ** bl - 1)
            gain, loss = float(dh[ok].sum()), float(dl[ok].sum())
            rows.append({"weights": wname, "beta_set": bn, "n_pairs": int(ok.sum()), "heavy_change": gain,
                         "light_change": loss, "net_change": gain + loss, "light_loss_over_heavy_gain": -loss / gain})
    pe = pd.DataFrame(rows)
    for wname in pe.weights.unique():
        m = pe.weights == wname
        pe.loc[m, "net_ratio_to_flat_035"] = pe.loc[m, "net_change"] / pe.loc[m & (pe.beta_set == "flat_0.35"), "net_change"].iloc[0]
    pe.to_csv(root / "table_pe_observed_vs_model_flow_weights.csv", index=False)

    # ---- long summary
    js = json.loads((est / "estimate_summary.json").read_text())
    S = []
    add = lambda sec, item, v: S.append({"section": sec, "item": item, "value": v})  # noqa: E731
    pos = pd.read_csv(est / "1_load_position_by_role.csv")
    for _, x in pos.iterrows():
        add("1_load_position_median", f"{x.role}:{x.measure}", x.p50)
    pr = pd.read_csv(est / "1_pairs_heavy_vs_light.csv")
    for c in ["ln_obs_over_ff", "s_q", "s_z", "pct_q", "cr"]:
        v = pr[f"{c}_heavy_minus_light"].dropna()
        add("1_pairs_heavy_minus_light", f"{c}:median", v.median())
        add("1_pairs_heavy_minus_light", f"{c}:share_heavy_higher", float((v > 0).mean()))
        add("1_pairs_heavy_minus_light", f"{c}:n_pairs", len(v))
    q = js["Q_quadratic"]
    for k in ("b1", "se_b1", "b2", "se_b2", "curvature_c=2b2", "se_c"):
        add("2_schedule_Q_quadratic", k, q[k])
    add("2_schedule_Q_quadratic", "first_stage_F_lq", q["first_stage_F"][0])
    add("2_schedule_Q_quadratic", "first_stage_F_xc2", q["first_stage_F"][1])
    sp = js["S_spline"]
    for i, (s_, se) in enumerate(zip(sp["segment_slopes"], sp["segment_slope_se"])):
        add("2_schedule_S_spline", f"segment{i + 1}_slope", s_)
        add("2_schedule_S_spline", f"segment{i + 1}_se", se)
    C = pd.read_csv(est / "2_C_iv_by_congestion_ratio_tercile.csv")
    for _, x in C[C.selection == "hours 07-09"].iterrows():
        add("2_schedule_C_cr_tercile_0709", f"tercile{int(x.cr_tercile)}_beta", x.beta)
        add("2_schedule_C_cr_tercile_0709", f"tercile{int(x.cr_tercile)}_se", x.se)
    imp = pd.read_csv(est / "2_implied_elasticity_by_role.csv")
    for _, x in imp.iterrows():
        add("2_implied_local_elasticity_median", f"{x.role}:{x.measure}", x.p50)
    for _, x in T.iterrows():
        for k in ["Wbar_pct_st", "Wbar_pct_lt", "time_saved_sec_st", "time_saved_sec_lt", "direct_st_sec_heavy_gain_lane",
                  "direct_st_sec_light_lose_lane", "direct_st_sec_other", "P1_over_P2_lt", "Wbar_st_ratio_to_bench",
                  "Wbar_lt_ratio_to_bench"]:
            add("4_results", f"{x.variant}:{k}", x[k])
    for _, x in pe.iterrows():
        add("4_pe_accounting", f"{x.weights}:{x.beta_set}:light_loss_over_heavy_gain", x.light_loss_over_heavy_gain)
        add("4_pe_accounting", f"{x.weights}:{x.beta_set}:net_ratio_to_flat_035", x.net_ratio_to_flat_035)
    pb = pd.read_csv(root / "cf" / "probe_multistart_tight.csv")
    for (v, hz), g in pb.groupby(["variant", "horizon"]):
        add("checks_probe", f"{v}:{hz}:max_abs_diff_Wbar_pp", g.abs_diff_Wbar_pp_vs_first.max())
        add("checks_probe", f"{v}:{hz}:max_abs_dlog_tau", g.max_abs_dlog_tau_vs_first.max())
        add("checks_probe", f"{v}:{hz}:all_converged", bool(g.converged.all()))
    ck_df = allr[ck]
    add("checks", "all_converged_base_st_lt_P2", bool(ck_df[["base_converged", "st_converged", "lt_converged",
                                                             "P2_converged_st", "P2_converged_lt"]].all().all()))
    add("checks", "max_base_dlog_tau_vs_tau_obs", float(ck_df.base_max_abs_dlog_tau_vs_tau_obs.max()))
    add("checks", "max_rho_A", float(ck_df[["base_rho_A", "st_rho_A", "lt_rho_A"]].max().max()))
    add("checks", "selection_equals_mainline_all", bool(ck_df.selection_equals_mainline.all()))
    for v in ("val_flat_vector_ce", "val_flat_curved_c0"):
        x = allr[allr.variant == v].iloc[0]
        add("checks_validation", f"{v}:abs_diff_Wbar_st_pp", abs(x.Wbar_pct_st - b.Wbar_pct_st))
        add("checks_validation", f"{v}:abs_diff_Wbar_lt_pp", abs(x.Wbar_pct_lt - b.Wbar_pct_lt))
    x, y = allr[allr.variant == "val_bpr_wrapped_b165"].iloc[0], allr[allr.variant == "val_bpr_native_b165"].iloc[0]
    add("checks_validation", "bpr_wrapped_vs_native:abs_diff_Wbar_st_pp", abs(x.Wbar_pct_st - y.Wbar_pct_st))
    add("checks_validation", "bpr_wrapped_vs_native:abs_diff_Wbar_lt_pp", abs(x.Wbar_pct_lt - y.Wbar_pct_lt))
    pd.DataFrame(S).to_csv(root / "summary.csv", index=False)
    print(T[["variant", "Wbar_pct_st", "Wbar_pct_lt", "Wbar_st_ratio_to_bench", "Wbar_lt_ratio_to_bench"]].round(4).to_string())
    print(pe.round(4).to_string())


if __name__ == "__main__":
    main()
