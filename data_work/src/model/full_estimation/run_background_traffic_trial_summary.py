"""Tables for the background-traffic trial (reads outputs/<name>/cf/, writes tables into outputs/<name>/).

Refuses to overwrite summary.csv. Units: seconds per commuter = total change / (Lbar * kappa) * 60,
the mainline normalisation; background is in commuter-equivalent units, so background seconds are
"background vehicle-time per commuter".
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

DATA_WORK = Path(__file__).resolve().parents[3]
OUT = DATA_WORK / "outputs"


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-name", default="background_traffic_trial_v1")
    a = ap.parse_args(argv)
    root = OUT / a.output_name
    cf = root / "cf"
    if (root / "summary.csv").exists():
        raise SystemExit("summary.csv exists; refusing to overwrite")
    d = pd.read_csv(cf / "results.csv")
    same, rese = d[d.selection == "same"].set_index("spec"), d[d.selection == "reselect"].set_index("spec")

    t1 = pd.read_csv(cf / "table_direction_ratio_baseline.csv")
    t1.to_csv(root / "table_1_direction_ratio.csv", index=False)

    def commuter(x):
        return pd.DataFrame({
            "multiple_B_over_Xi": x.multiple, "reroute": x.reroute,
            "light_over_heavy_total_flow_median_base": x.dir_median_light_over_heavy_total_base,
            "Wbar_pct_st": x.P1_st_Wbar_pct, "Wbar_pct_lt": x.P1_lt_Wbar_pct,
            "commuter_time_saved_sec_st": x.P1_st_commuter_time_saved_sec,
            "commuter_time_saved_sec_lt": x.P1_lt_commuter_time_saved_sec,
            "median_pct_dtau_heavy_st": x.P1_st_median_pct_dtau_heavy,
            "median_pct_dtau_light_st": x.P1_st_median_pct_dtau_light,
            "median_pct_dtau_heavy_lt": x.P1_lt_median_pct_dtau_heavy,
            "median_pct_dtau_light_lt": x.P1_lt_median_pct_dtau_light})

    def allveh(x, tg):
        return pd.DataFrame({
            "multiple_B_over_Xi": x.multiple, "reroute": x.reroute,
            "allveh_time_saved_sec": x[f"{tg}_allveh_time_saved_sec"],
            "commuter_time_saved_sec": x[f"{tg}_commuter_time_saved_sec"],
            "background_time_saved_sec": x[f"{tg}_background_time_saved_sec"],
            "heavy_gain_allveh_sec": -x[f"{tg}_dT_allveh_heavy"],
            "light_loss_allveh_sec": x[f"{tg}_dT_allveh_light"],
            "other_change_allveh_sec": x[f"{tg}_dT_allveh_other"],
            "light_loss_over_heavy_gain_allveh": x[f"{tg}_light_loss_over_heavy_gain_allveh"],
            "light_loss_over_heavy_gain_background": x[f"{tg}_light_loss_over_heavy_gain_background"],
            "light_loss_over_heavy_gain_commuter": x[f"{tg}_light_loss_over_heavy_gain_commuter"],
            "allveh_time_change_pct_of_base": x[f"{tg}_allveh_time_change_pct_of_base"],
            "background_gain_heavy_sec": -x[f"{tg}_dT_background_heavy"],
            "background_loss_light_sec": x[f"{tg}_dT_background_light"]})

    commuter(same).to_csv(root / "table_2_commuter_same_selection.csv")
    allveh(same, "P1_st").to_csv(root / "table_3a_allvehicle_st_same_selection.csv")
    allveh(same, "P1_lt").to_csv(root / "table_3b_allvehicle_lt_same_selection.csv")
    r4 = commuter(rese).join(allveh(rese, "P1_st").add_prefix("st_").drop(columns=["st_multiple_B_over_Xi", "st_reroute"]))
    r4.insert(0, "n_pairs_same_as_mainline", rese.n_pairs_same_as_mainline)
    r4.to_csv(root / "table_4_reselected.csv")

    rows = []
    for sel, x in (("same", same), ("reselect", rese)):
        for spec, r in x.iterrows():
            rows.append({"spec": spec, "selection": sel, "multiple_B_over_Xi": r.multiple,
                         "P1_Wbar_st": r.P1_st_Wbar_pct, "P2_Wbar_st": r.P2_st_Wbar_pct, "P1_over_P2_welfare_st": r.P1_over_P2_welfare_st,
                         "P1_Wbar_lt": r.P1_lt_Wbar_pct, "P2_Wbar_lt": r.P2_lt_Wbar_pct, "P1_over_P2_welfare_lt": r.P1_over_P2_welfare_lt,
                         "P1_commuter_saved_st": r.P1_st_commuter_time_saved_sec, "P2_commuter_saved_st": r.P2_st_commuter_time_saved_sec,
                         "P1_allveh_saved_st": r.P1_st_allveh_time_saved_sec, "P2_allveh_saved_st": r.P2_st_allveh_time_saved_sec,
                         "P1_over_P2_allveh_time_st": r.P1_over_P2_allveh_time_st,
                         "P1_allveh_saved_lt": r.P1_lt_allveh_time_saved_sec, "P2_allveh_saved_lt": r.P2_lt_allveh_time_saved_sec,
                         "P1_background_saved_st": r.P1_st_background_time_saved_sec, "P2_background_saved_st": r.P2_st_background_time_saved_sec})
    pd.DataFrame(rows).to_csv(root / "table_5_tidal_vs_expansion.csv", index=False)

    # checks
    eqtags = [f"{p}_{h}" for p in ("P1", "P2") for h in ("st", "lt")]
    ck = []
    for _, r in d.iterrows():
        ck.append({"spec": r.spec, "selection": r.selection,
                   "base_converged_in_1_step": bool(r.base_converged and r.base_n_outer == 1),
                   "base_max_abs_dlog_tau_vs_tau_obs": r.base_max_abs_dlog_tau_vs_tau_obs,
                   "all_cf_converged": bool(all(r[f"{t}_converged"] for t in eqtags)),
                   "max_n_outer": int(max(r[f"{t}_n_outer"] for t in eqtags)),
                   "max_rho_A": max(r[f"{t}_rho_A"] for t in eqtags),
                   "P1_st_share_tau_at_floor": r.P1_st_share_tau_at_floor, "P1_lt_share_tau_at_floor": r.P1_lt_share_tau_at_floor,
                   "P1_st_share_flow_below_xi_floor": r.P1_st_share_flow_below_xi_floor,
                   "P1_lt_share_flow_below_xi_floor": r.P1_lt_share_flow_below_xi_floor})
    ck = pd.DataFrame(ck)
    nat = same.loc["B0_native"]
    val = {
        "B0_native_Wbar_st": nat.P1_st_Wbar_pct, "B0_native_Wbar_lt": nat.P1_lt_Wbar_pct,
        "B0_patched_minus_native_Wbar_st": same.loc["B0_patched"].P1_st_Wbar_pct - nat.P1_st_Wbar_pct,
        "B0_patched_minus_native_Wbar_lt": same.loc["B0_patched"].P1_lt_Wbar_pct - nat.P1_lt_Wbar_pct,
        "B0_allveh_minus_commuter_saved_st": nat.P1_st_allveh_time_saved_sec - nat.P1_st_commuter_time_saved_sec,
        "B0_commuter_saved_minus_od_decomp_st": nat.P1_st_commuter_time_saved_sec - nat.check_mainline_physical_time_saved_st,
        "B0_commuter_saved_minus_od_decomp_lt": nat.P1_lt_commuter_time_saved_sec - nat.check_mainline_physical_time_saved_lt,
        "max_base_abs_dlog_tau_vs_tau_obs_all_specs": float(d.base_max_abs_dlog_tau_vs_tau_obs.max()),
        "all_equilibria_converged": bool(ck.all_cf_converged.all() and ck.base_converged_in_1_step.all()),
        "max_rho_A_all": float(ck.max_rho_A.max()),
    }
    pr = cf / "probe_multistart_tight.csv"
    if pr.exists():
        p = pd.read_csv(pr)
        p.to_csv(root / "table_6b_multistart_probe.csv", index=False)
        for s, g in p.groupby("spec"):
            val[f"probe_{s}_max_abs_diff_Wbar_pp"] = float(g.abs_diff_Wbar_pp_vs_first.max())
            val[f"probe_{s}_max_abs_dlog_tau"] = float(g.max_abs_dlog_tau_vs_first.max())
            val[f"probe_{s}_all_converged"] = bool(g.converged.all())
    ck.to_csv(root / "table_6_checks.csv", index=False)
    (root / "validation.json").write_text(json.dumps(val, indent=2, default=float))

    long = d.drop(columns=["desc"]).melt(id_vars=["spec", "selection"], var_name="metric", value_name="value")
    long.to_csv(root / "summary.csv", index=False)
    print(json.dumps(val, indent=2, default=float))


if __name__ == "__main__":
    main()
