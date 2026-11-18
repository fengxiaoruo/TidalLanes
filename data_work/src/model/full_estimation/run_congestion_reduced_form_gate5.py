"""
P0 reduced-form congestion diagnostics on gate5 edge travel costs.

No external flow data. Uses raw-connected-v1 / edgeproj_v2 bundle:
  model_input_edgeproj_v2_lcc_gate5_v1

Tasks:
  1. Descriptive stats for y = ln(tau_obs/tau_ff)
  2. Cross-section: congestion vs capacity observables (n, ell)
  3. BPR-implied load proxy x_obs (anchored from tau only)
  4. Functional-form horse race on observable capacity index z = 1/(n*ell)
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import optimize

DATA_WORK = Path(__file__).resolve().parents[3]  # .../data_work
sys.path.insert(0, str(DATA_WORK))

from src.model.full_estimation import build_primitives as bp  # noqa: E402

EPS = 1e-9


def _aic(n: int, k: int, rss: float) -> float:
    rss = max(rss, EPS)
    return n * np.log(rss / n) + 2 * k


def _bic(n: int, k: int, rss: float) -> float:
    rss = max(rss, EPS)
    return n * np.log(rss / n) + k * np.log(n)


def _fit_ols(y: np.ndarray, X: np.ndarray) -> tuple[np.ndarray, float]:
    beta, *_ = np.linalg.lstsq(X, y, rcond=None)
    resid = y - X @ beta
    rss = float(np.sum(resid**2))
    return beta, rss


def _bpr_x_obs(c: np.ndarray, a: float, b: float) -> np.ndarray:
    """x_obs from tau_obs/tau_ff >= 1; uncongested if c <= 1."""
    excess = np.maximum(c - 1.0, 0.0)
    return np.power(excess / max(a, EPS), 1.0 / max(b, EPS))


def _horse_race_z(df: pd.DataFrame) -> list[dict]:
    """Compare parametric families for ln(c) ~ f(z), z = 1/(n*ell_km)."""
    y = df["ln_c"].to_numpy()
    z = df["z_cap"].to_numpy()
    lz = np.log(np.clip(z, EPS, None))
    n = len(y)

    rows: list[dict] = []

    # M0: constant
    X0 = np.ones((n, 1))
    _, rss0 = _fit_ols(y, X0)
    rows.append(dict(form="M0_constant", k=1, rss=rss0, aic=_aic(n, 1, rss0), bic=_bic(n, 1, rss0)))

    # M1: log-log power in capacity index
    X1 = np.column_stack([np.ones(n), lz])
    b1, rss1 = _fit_ols(y, X1)
    rows.append(
        dict(
            form="M1_power_loglog",
            k=2,
            rss=rss1,
            aic=_aic(n, 2, rss1),
            bic=_bic(n, 2, rss1),
            coef_intercept=float(b1[0]),
            coef_lz=float(b1[1]),
            implied_elasticity_wrt_z=float(b1[1]),
        )
    )

    # M2: log-log + lane-bin FE (3 bins)
    lane_bin = pd.qcut(df["n_kl"], q=3, duplicates="drop")
    dummies = pd.get_dummies(lane_bin, prefix="lane", drop_first=True)
    X2 = np.column_stack([np.ones(n), lz, dummies.to_numpy()])
    _, rss2 = _fit_ols(y, X2)
    k2 = X2.shape[1]
    rows.append(dict(form="M2_power_loglog_laneFE", k=k2, rss=rss2, aic=_aic(n, k2, rss2), bic=_bic(n, k2, rss2)))

    # M3: linear in z
    X3 = np.column_stack([np.ones(n), z])
    _, rss3 = _fit_ols(y, X3)
    rows.append(dict(form="M3_linear_z", k=2, rss=rss3, aic=_aic(n, 2, rss3), bic=_bic(n, 2, rss3)))

    # M4: BPR-style in z: y = ln(1 + a * z^b) — NLS
    def _nls_ab(params: np.ndarray) -> float:
        a, b = params
        if a <= 0 or b <= 0:
            return 1e12
        pred = np.log1p(a * np.power(np.clip(z, EPS, None), b))
        return float(np.sum((y - pred) ** 2))

    best = None
    for a0 in (0.05, 0.15, 0.5, 2.0):
        for b0 in (1.0, 2.0, 4.0, 6.0):
            res = optimize.minimize(_nls_ab, x0=np.array([a0, b0]), method="Nelder-Mead")
            if best is None or res.fun < best.fun:
                best = res
    a_hat, b_hat = best.x
    pred4 = np.log1p(a_hat * np.power(np.clip(z, EPS, None), b_hat))
    rss4 = float(np.sum((y - pred4) ** 2))
    rows.append(
        dict(
            form="M4_bpr_in_z",
            k=2,
            rss=rss4,
            aic=_aic(n, 2, rss4),
            bic=_bic(n, 2, rss4),
            a_hat=float(a_hat),
            b_hat=float(b_hat),
        )
    )

    # M5: exp form y = beta * z^nu
    def _nls_nu(params: np.ndarray) -> float:
        beta, nu = params
        if beta <= 0:
            return 1e12
        pred = beta * np.power(np.clip(z, EPS, None), nu)
        return float(np.sum((y - pred) ** 2))

    best5 = None
    for b0 in (0.01, 0.05, 0.2):
        for n0 in (0.5, 1.0, 1.5, 2.0):
            res = optimize.minimize(_nls_nu, x0=np.array([b0, n0]), method="Nelder-Mead")
            if best5 is None or res.fun < best5.fun:
                best5 = res
    beta_hat, nu_hat = best5.x
    pred5 = beta_hat * np.power(np.clip(z, EPS, None), nu_hat)
    rss5 = float(np.sum((y - pred5) ** 2))
    rows.append(
        dict(
            form="M5_exp_in_z",
            k=2,
            rss=rss5,
            aic=_aic(n, 2, rss5),
            bic=_bic(n, 2, rss5),
            beta_hat=float(beta_hat),
            nu_hat=float(nu_hat),
        )
    )

    out = pd.DataFrame(rows).sort_values("aic")
    out["delta_aic_vs_best"] = out["aic"] - out["aic"].min()
    return out.to_dict("records")


def _local_elasticity_bins(df: pd.DataFrame) -> list[dict]:
    rows = []
    for q, g in df.groupby("lane_bin"):
        if len(g) < 30:
            continue
        lz = np.log(g["z_cap"].to_numpy())
        y = g["ln_c"].to_numpy()
        X = np.column_stack([np.ones(len(g)), lz])
        beta, _ = _fit_ols(y, X)
        rows.append(
            dict(
                lane_bin=str(q),
                n_edges=int(len(g)),
                median_c=float(g["c"].median()),
                elasticity_ln_c_wrt_ln_z=float(beta[1]),
            )
        )
    return rows


def main(argv=None) -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--repo-root", default=str(DATA_WORK), help="data_work root (default: auto)")
    p.add_argument("--version-id", default="model_input_edgeproj_v2_lcc_gate5_v1")
    p.add_argument("--output-name", default="congestion_reduced_form_gate5_v1")
    p.add_argument("--bpr-a", type=float, default=0.15)
    p.add_argument("--bpr-b", type=float, default=4.0)
    p.add_argument("--delta1", type=float, default=0.488)
    args = p.parse_args(argv)

    data_work = Path(args.repo_root).resolve()
    version_root = data_work / "outputs" / args.version_id
    out = data_work / "outputs" / args.output_name
    out.mkdir(parents=True, exist_ok=True)

    prim = bp.load_primitives(version_root)
    df = pd.DataFrame(
        {
            "tau_obs": prim.tau_obs_min,
            "tau_ff": prim.tau_ff_min,
            "ell_km": prim.ell_kl_km,
            "n_kl": prim.n_kl,
            "c": prim.tau_obs_min / prim.tau_ff_min,
        }
    )
    df = df[np.isfinite(df["c"]) & (df["c"] > 0)].copy()
    df["ln_c"] = np.log(df["c"])
    df["y_excess"] = np.log(np.maximum(df["c"], 1.0))
    df["z_cap"] = 1.0 / np.clip(df["n_kl"] * df["ell_km"], EPS, None)
    df["lane_bin"] = pd.qcut(df["n_kl"], q=4, duplicates="drop")

    # BPR anchored load (no external flow)
    df["x_bpr"] = _bpr_x_obs(df["c"].to_numpy(), args.bpr_a, args.bpr_b)
    df["xi_over_n_proxy"] = np.power(np.maximum(df["c"], 1.0), 1.0 / args.delta1)

    # Descriptive
    desc = {
        "n_edges": int(len(df)),
        "c_median": float(df["c"].median()),
        "c_mean": float(df["c"].mean()),
        "c_p10": float(df["c"].quantile(0.10)),
        "c_p90": float(df["c"].quantile(0.90)),
        "c_p99": float(df["c"].quantile(0.99)),
        "ln_c_median": float(df["ln_c"].median()),
        "share_c_le_1": float((df["c"] <= 1.0).mean()),
        "share_c_le_1_05": float((df["c"] <= 1.05).mean()),
        "share_c_ge_1_25": float((df["c"] >= 1.25).mean()),
        "median_excess_pct": float(100.0 * (df["c"].median() - 1.0)),
        "median_n_lanes": float(df["n_kl"].median()),
        "median_ell_km": float(df["ell_km"].median()),
    }

    # Barwick-style reference: at median congestion, local power elasticity = lambda
    # Compare to cross-section slope w.r.t ln(1/n) holding ell
    ln_inv_n = np.log(1.0 / np.clip(df["n_kl"], EPS, None))
    X_n = np.column_stack([np.ones(len(df)), ln_inv_n, np.log(np.clip(df["ell_km"], EPS, None))])
    beta_n, _ = _fit_ols(df["ln_c"].to_numpy(), X_n)
    desc["ols_ln_c_on_ln_inv_n_and_ln_ell"] = {
        "intercept": float(beta_n[0]),
        "coef_ln_inv_n": float(beta_n[1]),
        "coef_ln_ell": float(beta_n[2]),
        "interpretation": "coef_ln_inv_n ≈ elasticity of c w.r.t. 1/n (partial, log-log)",
    }

    # BPR local elasticity at median x
    x_med = float(np.median(df.loc[df["x_bpr"] > 0, "x_bpr"])) if (df["x_bpr"] > 0).any() else 0.0
    if x_med > 0:
        local_bpr_elast = (args.bpr_a * args.bpr_b * x_med**args.bpr_b) / (
            1.0 + args.bpr_a * x_med**args.bpr_b
        )
    else:
        local_bpr_elast = 0.0
    desc["bpr_local_elasticity_at_median_x"] = {
        "a": args.bpr_a,
        "b": args.bpr_b,
        "median_x_bpr": x_med,
        "d_ln_c_d_ln_x": local_bpr_elast,
        "barwick_reference_lambda": 1.1,
    }

    horse = _horse_race_z(df)
    lane_bins = _local_elasticity_bins(df)

    df.to_csv(out / "edge_congestion_features.csv", index=False)
    summary = {
        "version_id": args.version_id,
        "travel_cost_source": "raw_connected_v1 edgeproj_v2 pair-specific (NOT centerline)",
        "no_flow_data": True,
        "descriptive": desc,
        "horse_race_on_z_cap": horse,
        "lane_bin_elasticities": lane_bins,
        "data_intuition": [],
    }

    best = horse[0]
    summary["data_intuition"] = [
        f"Median peak slowdown c=tau_obs/tau_ff is {desc['c_median']:.3f} "
        f"(+{desc['median_excess_pct']:.1f}% over free flow); "
        f"{100*desc['share_c_le_1_05']:.1f}% of edges within 5% of free flow.",
        f"Cross-section OLS: ln(c) rises with ln(1/n) (coef={beta_n[1]:.3f}) "
        f"and ln(length) (coef={beta_n[2]:.3f}) — thinner/shorter links look more congested in observed times.",
        f"Horse race on capacity index z=1/(n*ell): best AIC = {best['form']} "
        f"(next-best ΔAIC = {horse[1]['delta_aic_vs_best']:.1f})." if len(horse) > 1 else "",
        f"Standard BPR (a={args.bpr_a}, b={args.bpr_b}) implied local elasticity at median x "
        f"is {local_bpr_elast:.2f} vs Barwick physical λ=1.1 reference.",
        "Without observed flow, x_bpr and xi/n proxies are monotone transforms of c — "
        "form comparison on z uses only lane count and length (true reduced form).",
    ]

    (out / "summary.json").write_text(json.dumps(summary, indent=2))
    pd.DataFrame(horse).to_csv(out / "horse_race_aic.csv", index=False)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
