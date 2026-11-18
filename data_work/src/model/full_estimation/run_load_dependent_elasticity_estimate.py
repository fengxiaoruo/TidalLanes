"""Load-dependent congestion elasticity: edge load positions and schedule estimates (trial, 2026-11-15).

Steps 1-2 of the load-dependent elasticity trial (counterfactuals: run_load_dependent_elasticity_cf.py).

Panel: the gate5 edge-hour panel of run_congestion_floor_sensitivity.py (its loaders are imported,
not edited): edge_data_from(edge_pace_10min) -> al.night_floor -> al.sample(06-22 h).
    y = ln pace, lq = ln observed flow q, lz = ln routed-OD flow z (instrument), lt0 = ln 00-05 pace.

Load positions (per gate5 unit u = grid_from>grid_to; weekday 07-09 hours, the model's anchor):
    s_q   = mean_{07-09 wkday} (ln q) - mean_{06-22 sample} (ln q)        within-unit load, observed flow
    s_z   = same with ln z
    pct_q = share of the unit's 06-22 hours with ln q below its 07-09 weekday mean (percentile)
    cr    = mean_{07-09 wkday} (ln pace) - lt0                              congestion ratio vs night floor
    plus, from the model bundle for every 1B edge, ln(tau_obs/tau_ff).
1B edges are matched to gate5 units by (grid_o, grid_d).

Schedules (unit FE + date x hour FE, all 06-22 h observations, SE clustered by unit):
    Q  quadratic 2SLS: y = b1 lq + b2 xc^2,   xc = lq - mean_u lq;  instruments lz, zc^2
       local elasticity e(xc) = b1 + 2 b2 xc
    S  linear spline 2SLS in xc, knots at pooled p33/p67 of xc;
       instruments lz and the same hinge functions of zc at the pooled p33/p67 of zc
    C  split-sample linear IV (07-09 hours, and z > unit p70) by tercile of the unit's cr
Output: data_work/outputs/<output-name>/estimate/ (refuses to overwrite).
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import run_congestion_floor_sensitivity as rfs  # noqa: E402
import run_congestion_almagro_spec as al  # noqa: E402
import run_congestion_tail_followup as tf  # noqa: E402
from run_all_traffic_speed_panel_gate5 import WEEKDAYS  # noqa: E402

DATA_WORK = HERE.parents[2]
OUT = DATA_WORK / "outputs"
BUNDLE = "model_input_osm1b_L300_gate5_od2025_v2_alldaytt0709"
TIDAL_DIR = "tidal_lane_diagnostics_osm1b_ce_floor_b035_fl001_ks_od2025_alldaytt0709_v1"
T0 = time.time()


def log(*a):
    print(f"[{time.time() - T0:6.0f}s]", *a, flush=True)


# ------------------------------------------------------------------ 2SLS
def tsls_cluster(y, X, Z, cl) -> dict:
    """Just-identified 2SLS on already-residualised data; SE clustered by cl."""
    y, X, Z = np.asarray(y, float), np.atleast_2d(np.asarray(X, float).T).T, np.atleast_2d(np.asarray(Z, float).T).T
    ZX = Z.T @ X
    b = np.linalg.solve(ZX, Z.T @ y)
    u = y - X @ b
    codes = pd.factorize(np.asarray(cl))[0]
    G = codes.max() + 1
    S = np.zeros((G, Z.shape[1]))
    np.add.at(S, codes, Z * u[:, None])
    A = np.linalg.inv(ZX)
    V = A @ (S.T @ S) @ A.T
    # first stages: each endogenous on all instruments, cluster-robust Wald F
    fs = []
    for k in range(X.shape[1]):
        pi = np.linalg.lstsq(Z, X[:, k], rcond=None)[0]
        v = X[:, k] - Z @ pi
        Sk = np.zeros((G, Z.shape[1]))
        np.add.at(Sk, codes, Z * v[:, None])
        Zi = np.linalg.inv(Z.T @ Z)
        Vk = Zi @ (Sk.T @ Sk) @ Zi
        fs.append(float(pi @ np.linalg.solve(Vk, pi) / Z.shape[1]))
    return {"b": b, "V": V, "se": np.sqrt(np.diag(V)), "n": int(len(y)), "clusters": int(G), "fs_F": fs}


def load_panel():
    h, full = rfs.edge_data_from(pd.read_parquet(tf.GATE5 / "speed/edge_pace_10min.parquet"))
    t0 = al.night_floor(full)
    d = al.sample(h, t0, rfs.WIN)
    return d


def unit_positions(d: pd.DataFrame) -> pd.DataFrame:
    g = d.groupby("u")
    mq, mz = g.lq.transform("mean"), g.lz.transform("mean")
    d = d.assign(xc=d.lq - mq, zc=d.lz - mz)
    am = d[d.hour.isin([7, 8]) & d.date.isin(WEEKDAYS)]
    pos = am.groupby("u").agg(s_q=("xc", "mean"), s_z=("zc", "mean"), y_am=("y", "mean"), lt0=("lt0", "first"),
                              lq_am=("lq", "mean"), n_am=("y", "size"))
    pos["cr"] = pos.y_am - pos.lt0
    # percentile of the AM mean load within the unit's own 06-22 distribution
    m = d.merge(pos[["lq_am"]].reset_index(), on="u")
    pos["pct_q"] = (m.lq < m.lq_am).groupby(m.u).mean()
    pos["n_day"] = g.size()
    return pos.reset_index(), d


def fit_quadratic(d: pd.DataFrame) -> dict:
    cs = rfs.codes(d, [["u"], ["date", "hour"]])
    y, x1, x2, z1, z2 = rfs.fwithin([d.y, d.lq, d.xc ** 2, d.lz, d.zc ** 2], cs)
    o = tsls_cluster(y, np.c_[x1, x2], np.c_[z1, z2], d.u)
    return o


def fit_spline(d: pd.DataFrame, qs=(1 / 3, 2 / 3)) -> tuple[dict, list, list]:
    kx = [float(d.xc.quantile(q)) for q in qs]
    kz = [float(d.zc.quantile(q)) for q in qs]
    X = [d.lq] + [np.maximum(d.xc - k, 0) for k in kx]
    Z = [d.lz] + [np.maximum(d.zc - k, 0) for k in kz]
    cs = rfs.codes(d, [["u"], ["date", "hour"]])
    r = rfs.fwithin([d.y] + X + Z, cs)
    k = len(X)
    return tsls_cluster(r[0], np.column_stack(r[1:1 + k]), np.column_stack(r[1 + k:]), d.u), kx, kz


def spline_slope(b, kx, s):
    s = np.asarray(s, float)
    e = np.full_like(s, b[0])
    for j, k in enumerate(kx):
        e = e + b[1 + j] * (s > k)
    return e


def spline_se(V, kx, s):
    out = []
    for si in np.atleast_1d(s):
        g = np.r_[1.0, [float(si > k) for k in kx]]
        out.append(float(np.sqrt(g @ V @ g)))
    return np.array(out)


def local_iv(s: pd.DataFrame) -> dict:
    s = s[s.groupby("u").u.transform("size") >= 2].reset_index(drop=True)
    cs = rfs.codes(s, [["u"], ["date", "hour"]])
    y, x, z = rfs.fwithin([s.y, s.lq, s.lz], cs)
    o = tsls_cluster(y, x[:, None], z[:, None], s.u)
    return {"beta": float(o["b"][0]), "se": float(o["se"][0]), "fs_F": o["fs_F"][0], "n": o["n"], "units": o["clusters"]}


def model_edges() -> pd.DataFrame:
    sys.path.insert(0, str(DATA_WORK))
    from src.model.full_estimation import build_primitives as bp
    prim = bp.load_primitives(OUT / BUNDLE, period="AM")
    g = prim.node_grid_ids
    e = pd.DataFrame({"edge_idx": np.arange(prim.n_edges), "grid_o": g[prim.edge_i], "grid_d": g[prim.edge_j],
                      "tau_obs": prim.tau_obs_min, "tau_ff": prim.tau_ff_min, "n_lanes": prim.n_kl,
                      "len_km": prim.ell_kl_km})
    e["u"] = e.grid_o + ">" + e.grid_d
    e["ln_obs_over_ff"] = np.log(e.tau_obs / e.tau_ff)
    sel = pd.read_csv(OUT / TIDAL_DIR / "top100_d1.0" / "selected_pairs.csv")
    e["role"] = "other"
    e.loc[e.edge_idx.isin(sel.slow_edge_idx), "role"] = "heavy_gain_lane"
    e.loc[e.edge_idx.isin(sel.fast_edge_idx), "role"] = "light_lose_lane"
    det = pd.read_csv(OUT / TIDAL_DIR / "tidal_treated_edge_detail.csv")
    det = det[det.horizon == "short_run"][["edge_idx", "Xi_base"]]
    e = e.merge(det, on="edge_idx", how="left")
    return e


def dist(df: pd.DataFrame, cols: list[str], by: str) -> pd.DataFrame:
    rows = []
    for grp, g in df.groupby(by):
        for c in cols:
            v = g[c].dropna()
            rows.append({by: grp, "measure": c, "n": int(len(v)), "mean": v.mean(), "p10": v.quantile(.1),
                         "p25": v.quantile(.25), "p50": v.median(), "p75": v.quantile(.75), "p90": v.quantile(.9)})
    return pd.DataFrame(rows)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-name", default="load_dependent_elasticity_trial_v1")
    a = ap.parse_args(argv)
    out = OUT / a.output_name / "estimate"
    if out.exists():
        raise SystemExit(f"{out} exists; refusing to overwrite")
    out.mkdir(parents=True)
    (out / "command.txt").write_text(" ".join([sys.executable] + sys.argv) + "\n")
    res = {}

    log("loading panel")
    d = load_panel()
    res["panel"] = {"n": int(len(d)), "units": int(d.u.nunique())}
    pos, d = unit_positions(d)
    pos.to_csv(out / "unit_load_positions_gate5.csv", index=False)

    # ---- step 1: positions of the 200 tidal edges
    e = model_edges()
    em = e.merge(pos, on="u", how="left")
    em["matched_panel"] = em.s_q.notna()
    em.to_csv(out / "model_edge_load_positions.csv", index=False)
    cols = ["ln_obs_over_ff", "s_q", "s_z", "pct_q", "cr"]
    dist(em, cols, "role").to_csv(out / "1_load_position_by_role.csv", index=False)
    # pairwise heavy minus light within the same pair
    sel = pd.read_csv(OUT / TIDAL_DIR / "top100_d1.0" / "selected_pairs.csv")
    ix = em.set_index("edge_idx")
    pr = pd.DataFrame({"pair_rank": np.arange(1, len(sel) + 1), "slow": sel.slow_edge_idx, "fast": sel.fast_edge_idx})
    for c in cols + ["Xi_base", "n_lanes"]:
        pr[f"{c}_heavy"] = ix.loc[pr.slow, c].to_numpy()
        pr[f"{c}_light"] = ix.loc[pr.fast, c].to_numpy()
        pr[f"{c}_heavy_minus_light"] = pr[f"{c}_heavy"] - pr[f"{c}_light"]
    pr.to_csv(out / "1_pairs_heavy_vs_light.csv", index=False)
    res["match"] = {r: {"n": int((em.role == r).sum()), "matched": int(em[em.role == r].matched_panel.sum())}
                    for r in em.role.unique()}
    res["pairs_heavy_minus_light_median"] = {c: float(pr[f"{c}_heavy_minus_light"].median()) for c in cols}
    res["pairs_share_heavy_above_light"] = {c: float((pr[f"{c}_heavy_minus_light"].dropna() > 0).mean()) for c in cols}
    log("step 1", res["match"], res["pairs_heavy_minus_light_median"])

    # ---- step 2: schedules
    log("quadratic IV")
    oq = fit_quadratic(d)
    res["Q_quadratic"] = {"b1": float(oq["b"][0]), "b2": float(oq["b"][1]), "se_b1": float(oq["se"][0]),
                          "se_b2": float(oq["se"][1]), "cov_b1b2": float(oq["V"][0, 1]), "curvature_c=2b2": float(2 * oq["b"][1]),
                          "se_c": float(2 * oq["se"][1]), "first_stage_F": oq["fs_F"], "n": oq["n"], "clusters": oq["clusters"]}
    log(res["Q_quadratic"])
    log("spline IV")
    os_, kx, kz = fit_spline(d)
    res["S_spline"] = {"knots_xc": kx, "knots_zc": kz, "b": os_["b"].tolist(), "se": os_["se"].tolist(),
                       "segment_slopes": [float(spline_slope(os_["b"], kx, s)) for s in (kx[0] - 1, (kx[0] + kx[1]) / 2, kx[1] + 1)],
                       "segment_slope_se": spline_se(os_["V"], kx, [kx[0] - 1, (kx[0] + kx[1]) / 2, kx[1] + 1]).tolist(),
                       "first_stage_F": os_["fs_F"], "n": os_["n"]}
    log(res["S_spline"])
    # spline with 2 segments (knot at median) as a check
    os2, kx2, _ = fit_spline(d, qs=(0.5,))
    res["S2_spline_median_knot"] = {"knot_xc": kx2, "segment_slopes": [float(os2["b"][0]), float(os2["b"][0] + os2["b"][1])],
                                    "segment_slope_se": spline_se(os2["V"], kx2, [kx2[0] - 1, kx2[0] + 1]).tolist(),
                                    "first_stage_F": os2["fs_F"]}
    log(res["S2_spline_median_knot"])

    # ---- C: by congestion-ratio tercile of the unit
    log("congestion-ratio groups")
    cuts = pos.cr.quantile([1 / 3, 2 / 3]).to_numpy()
    pos["cr_tercile"] = np.digitize(pos.cr, cuts)
    d = d.merge(pos[["u", "cr_tercile", "cr", "s_q"]], on="u", how="left", suffixes=("", "_unit"))
    pct70 = d.groupby("u").z.transform(lambda s: s.quantile(0.7))
    rows = []
    for t in (0, 1, 2):
        g = pos[pos.cr_tercile == t]
        for nm, m in [("hours 07-09", d.hour.isin([7, 8])), ("z > unit p70", d.z > pct70), ("all 06-22", d.hour >= 0)]:
            o = local_iv(d[m & (d.cr_tercile == t)])
            rows.append({"cr_tercile": t, "cr_lo": float(g.cr.min()), "cr_hi": float(g.cr.max()), "cr_median": float(g.cr.median()),
                         "selection": nm, **o})
    C = pd.DataFrame(rows)
    C.to_csv(out / "2_C_iv_by_congestion_ratio_tercile.csv", index=False)
    res["cr_tercile_cuts"] = cuts.tolist()
    log(C.to_string())

    # ---- implied local elasticity at the tidal edges
    b1, b2 = oq["b"]
    V = oq["V"]
    em["e_Q"] = b1 + 2 * b2 * em.s_q
    em["e_Q_se"] = np.sqrt(V[0, 0] + 4 * em.s_q ** 2 * V[1, 1] + 4 * em.s_q * V[0, 1])
    em["e_S"] = np.where(em.s_q.notna(), spline_slope(os_["b"], kx, em.s_q.fillna(0)), np.nan)
    em["e_S_se"] = np.where(em.s_q.notna(), spline_se(os_["V"], kx, em.s_q.fillna(0)), np.nan)
    em["cr_tercile"] = np.where(em.cr.notna(), np.digitize(em.cr.fillna(0), cuts), -1)
    cmap = C[C.selection == "hours 07-09"].set_index("cr_tercile").beta
    em["e_C"] = em.cr_tercile.map(cmap)
    em.to_csv(out / "model_edge_load_positions.csv", index=False)
    dist(em, ["e_Q", "e_S", "e_C"], "role").to_csv(out / "2_implied_elasticity_by_role.csv", index=False)
    grid = np.round(np.arange(-0.6, 0.81, 0.1), 2)
    pd.DataFrame({"s_q": grid, "e_Q": b1 + 2 * b2 * grid,
                  "e_Q_se": np.sqrt(V[0, 0] + 4 * grid ** 2 * V[1, 1] + 4 * grid * V[0, 1]),
                  "e_S": spline_slope(os_["b"], kx, grid), "e_S_se": spline_se(os_["V"], kx, grid)}).to_csv(
        out / "2_schedule_grid.csv", index=False)
    (out / "estimate_summary.json").write_text(json.dumps(res, indent=2, default=float))
    log("done")


if __name__ == "__main__":
    main()
