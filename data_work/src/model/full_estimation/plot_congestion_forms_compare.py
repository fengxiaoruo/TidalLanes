"""
Candidate congestion technologies drawn on one scale, and fitted to the gate5 edge curve
=======================================================================================

Figure 8 (illustrative, parameters assumed), x = q/c:
    constant elasticity  t/t0 = 1.15 x^lam,               lam = 0.2
    floor + constant     t/t0 = max{1, 1.15 x^beta},        beta = 0.2   (Almagro et al.)
    BPR                  t/t0 = 1 + 0.15 x^4
    linear speed-density V = a + b D, q = V D  =>  t/t0 = 2 / (1 + sqrt(1 - x)) on the
                         uncongested branch, c = -a^2 / (4b)  (Kashner and Ross; Greenshields)
  panels: level, local elasticity, and the Kashner-Ross speed-density and flow-density
  diagrams showing which branch is used.

Figure 9 (data): each form fitted by weighted least squares (weights = edge-hours)
to the binned within-edge travel-time index against q/q95 from
plot_congestion_technology_gate5.py. Bins with median q/q95 above FIT_MAX are
shown but not used, because none of the uncongested-branch forms can fall.
This is a fit to 12 bin means, not an estimate; q95 is not capacity.

Input:  outputs/all_traffic_speed_panel_rn202505_gate5_v1/technology/data_curve_by_q_over_q95.csv (read only)
Output: data_work/outputs/<output-name>/ (refuses to overwrite; --replot redraws from fit_params.json)
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from plot_congestion_technology_gate5 import INK, INK2, style  # noqa: E402

ROOT = Path(__file__).resolve().parents[3]
CURVE = ROOT / "outputs/all_traffic_speed_panel_rn202505_gate5_v1/technology/data_curve_by_q_over_q95.csv"
FIT_MAX = 1.05
COL = {"const": "#2a78d6", "floor": "#eb6834", "bpr": "#1baf7a", "kr": "#eda100"}
LAB = {"const": "Constant elasticity", "floor": "Free-flow floor + constant (Almagro et al.)",
       "bpr": "BPR", "kr": "Linear speed-density (Kashner-Ross)"}
SHORT = {"const": "Constant", "floor": "Floor + constant", "bpr": "BPR", "kr": "Linear speed-density"}


# ---------- forms, t relative to t0 ----------
def f_const(x, A, lam):
    return A * x ** lam


def f_floor(x, T0, xk, beta):
    return T0 * np.maximum(1.0, (x / xk) ** beta)


def f_bpr(x, T0, k, b):
    return T0 * (1 + k * x ** b)


def f_kr(x, T0, c):
    return T0 * 2.0 / (1 + np.sqrt(np.clip(1 - x / c, 0, None)))


def elast(f, x, *p):
    lx = np.log(x)
    return np.gradient(np.log(f(x, *p)), lx)


# ---------- fits (weighted least squares on the level of the index) ----------
def wls_scale(g, y, w):
    """best T0 for y ~ T0 * g"""
    T0 = np.sum(w * g * y) / np.sum(w * g * g)
    return T0, np.sum(w * (y - T0 * g) ** 2)


def fit(x, y, w) -> dict:
    out = {}
    b = np.polyfit(np.log(x), np.log(y), 1, w=np.sqrt(w))
    out["const"] = {"A": float(np.exp(b[1])), "lam": float(b[0])}
    best = (np.inf, None)
    for xk in np.linspace(0.1, 1.0, 181):
        for beta in np.linspace(0.01, 3, 300):
            T0, sse = wls_scale(np.maximum(1, (x / xk) ** beta), y, w)
            best = min(best, (sse, (T0, xk, beta)), key=lambda t: t[0])
    out["floor"] = dict(zip(["T0", "xk", "beta"], map(float, best[1])))
    best = (np.inf, None)
    for bb in np.linspace(1, 15, 281):
        X = np.c_[np.ones_like(x), x ** bb]
        W = np.sqrt(w)[:, None]
        coef = np.linalg.lstsq(X * W, y * W[:, 0], rcond=None)[0]
        sse = np.sum(w * (y - X @ coef) ** 2)
        best = min(best, (sse, (coef[0], coef[1] / coef[0], bb)), key=lambda t: t[0])
    T0, k, bb = map(float, best[1])
    out["bpr"] = {"T0": T0, "k": k, "b": bb, "c_if_a_0.15": float((0.15 / k) ** (1 / bb)) if k > 0 else None}
    best = (np.inf, None)
    for c in np.linspace(x.max() + 1e-3, 6, 2000):
        T0, sse = wls_scale(2 / (1 + np.sqrt(1 - x / c)), y, w)
        best = min(best, (sse, (T0, c)), key=lambda t: t[0])
    out["kr"] = dict(zip(["T0", "c"], map(float, best[1])))
    F = {"const": f_const, "floor": f_floor, "bpr": f_bpr, "kr": f_kr}
    for k_, f in F.items():
        p = [v for kk, v in out[k_].items() if not kk.startswith("c_if")]
        r = y - f(x, *p)
        out[k_]["wrmse"] = float(np.sqrt(np.sum(w * r ** 2) / np.sum(w)))
        out[k_]["n_params"] = len(p)
    return out


# ---------- figure 8: theory ----------
def fig_theory(out: Path) -> None:
    x = np.linspace(0.005, 1.0, 600)
    xe = np.linspace(0.005, 1.4, 700)
    forms = {"const": (f_const, (1.15, 0.2), xe), "floor": (f_floor, (1.0, (1 / 1.15) ** 5, 0.2), xe),
             "bpr": (f_bpr, (1.0, 0.15, 4.0), xe), "kr": (f_kr, (1.0, 1.0), x[:-1])}
    fig, ax = plt.subplots(2, 2, figsize=(11, 8.2))
    a, b, c, d = ax.ravel()
    for k, (f, p, xx) in forms.items():
        a.plot(xx, f(xx, *p), color=COL[k], lw=2, label=LAB[k], zorder=3)
        b.plot(xx, elast(f, xx, *p), color=COL[k], lw=2, label=LAB[k], zorder=3)
    # hypercongested branch of Kashner-Ross, excluded by them
    s = np.sqrt(1 - x[:-1])
    a.plot(x[:-1], 2 / (1 - s), color=COL["kr"], lw=1.4, ls="--", zorder=2)
    a.annotate("Kashner-Ross hypercongested branch\n(same q, lower speed; not used)", xy=(0.85, 3.0), xytext=(0.25, 3.3),
               fontsize=8, color=INK2, arrowprops=dict(arrowstyle="-", color=INK2, lw=0.7))
    a.axhline(1, color=INK2, lw=0.8, ls=":")
    a.axvline(1, color=INK2, lw=0.6, ls=":")
    a.set(xlabel="q / c", ylabel="t / t0 (travel time over free flow)", xlim=(0, 1.4), ylim=(0, 4.2))
    a.set_title("(a) Travel time against flow", fontsize=10, color=INK, loc="left")
    a.annotate("kink: A·q^β meets T0", xy=((1 / 1.15) ** 5, 1.0), xytext=(0.05, 1.7), fontsize=8, color=INK2,
               arrowprops=dict(arrowstyle="-", color=INK2, lw=0.7))
    a.annotate("t/t0 = 2 at capacity\n(speed halves)", xy=(1.0, 2.0), xytext=(1.05, 2.6), fontsize=8, color=INK2,
               arrowprops=dict(arrowstyle="-", color=INK2, lw=0.7))
    a.legend(fontsize=8, frameon=False, loc="lower right")
    b.set(xlabel="q / c", ylabel="local elasticity  d ln t / d ln q", xlim=(0, 1.4), ylim=(0, 1.3))
    b.set_title("(b) Local elasticity", fontsize=10, color=INK, loc="left")
    b.text(0.93, 1.2, "Kashner-Ross → ∞ at q = c", fontsize=8, color=INK2, ha="right")
    b.text(0.75, 0.24, "floor form: 0, then jumps to β", fontsize=8, color=INK2)
    # Kashner-Ross native diagrams (per lane, a = 1, D_jam = 1)
    D = np.linspace(0, 1, 400)
    V = 1 - D
    c.plot(D, V, color=COL["kr"], lw=2, zorder=3)
    c.axvspan(0, 0.5, color=COL["kr"], alpha=0.10, lw=0)
    c.set(xlabel="density D (vehicles per lane-km, jam density = 1)", ylabel="speed V (free flow = 1)", xlim=(0, 1), ylim=(0, 1.05))
    c.set_title("(c) Kashner-Ross: speed falls linearly with density", fontsize=10, color=INK, loc="left")
    c.text(0.03, 0.08, "used: D below the\ncapacity density", fontsize=8, color=INK2)
    c.text(0.55, 0.85, "V = a + b·D,  b < 0\nb varies with speed limit\na = segment FE + road type × year FE", fontsize=8, color=INK2)
    d.plot(D[D <= 0.5], (V * D)[D <= 0.5], color=COL["kr"], lw=2, zorder=3, label="uncongested branch (used)")
    d.plot(D[D >= 0.5], (V * D)[D >= 0.5], color=COL["kr"], lw=1.4, ls="--", zorder=3, label="hypercongested branch (not used)")
    d.scatter([0.5], [0.25], s=40, color=COL["kr"], edgecolor="white", lw=1.5, zorder=4)
    d.annotate("capacity c = −a²/(4b)", xy=(0.5, 0.25), xytext=(0.6, 0.27), fontsize=8, color=INK2)
    d.set(xlabel="density D", ylabel="flow q = V·D per lane", xlim=(0, 1), ylim=(0, 0.3))
    d.set_title("(d) Kashner-Ross: flow-density is a parabola", fontsize=10, color=INK, loc="left")
    d.legend(fontsize=8, frameon=False, loc="lower center")
    for x_ in (a, b, c, d):
        style(x_)
    fig.text(0.01, 0.01, "Illustrative only; parameters assumed, not estimated. Constant forms scaled to equal BPR (a = 0.15, b = 4) at q = c; "
             "λ = β = 0.2.\nKashner-Ross: t/t0 = 2/(1 + √(1 − q/c)) on the used branch; lanes enter through D = q/(lanes·V).",
             fontsize=8, color=INK2)
    fig.tight_layout(rect=(0, 0.05, 1, 1))
    fig.savefig(out / "fig8_congestion_forms_theory.png", dpi=200)
    plt.close(fig)


# ---------- figure 9: fit to data ----------
def fig_fit(g: pd.DataFrame, P: dict, out: Path) -> None:
    use = g.qr <= FIT_MAX
    F = {"const": (f_const, ["A", "lam"]), "floor": (f_floor, ["T0", "xk", "beta"]),
         "bpr": (f_bpr, ["T0", "k", "b"]), "kr": (f_kr, ["T0", "c"])}
    xx = np.linspace(g.qr.min(), FIT_MAX + 0.02, 400)
    fig, (l, r) = plt.subplots(1, 2, figsize=(11, 4.6))
    l.fill_between(g.qr, g.t_lo, g.t_hi, color=INK2, alpha=0.12, lw=0)
    l.scatter(g.qr[use], g.t_index[use], s=36, color=INK, zorder=5, label="data, used in fit")
    l.scatter(g.qr[~use], g.t_index[~use], s=36, facecolor="white", edgecolor=INK, zorder=5, label="data, not used (falling part)")
    m = g.slope.notna()
    r.fill_between(g.qr[m], g.slope_lo[m], g.slope_hi[m], color=INK2, alpha=0.12, lw=0)
    r.scatter(g.qr[m & use], g.slope[m & use], s=36, color=INK, zorder=5)
    r.scatter(g.qr[m & ~use], g.slope[m & ~use], s=36, facecolor="white", edgecolor=INK, zorder=5)
    for k, (f, names) in F.items():
        p = [P[k][n] for n in names]
        q = P[k]
        txt = (f"λ={q['lam']:.2f}" if k == "const" else f"β={q['beta']:.2f}, kink {q['xk']:.2f}" if k == "floor"
               else f"b={q['b']:.1f}" if k == "bpr" else f"c={q['c']:.2f}·q95")
        lab = f"{SHORT[k]}: {txt}; RMSE {P[k]['wrmse'] * 1000:.1f}e-3"
        l.plot(xx, f(xx, *p), color=COL[k], lw=2, label=lab, zorder=3)
        r.plot(xx, elast(f, xx, *p), color=COL[k], lw=2, zorder=3)
    l.set(xlabel="q / q95 (edge's own 95th-percentile hourly flow; not capacity)", ylabel="travel-time index (lowest-flow bin = 1)")
    l.set_title("(a) Fitted levels", fontsize=10, color=INK, loc="left")
    l.legend(fontsize=7.5, frameon=False, loc="lower right")
    r.axhline(0, color=INK2, lw=0.8)
    r.set(xlabel="q / q95", ylabel="local elasticity  d ln t / d ln q", ylim=(-0.5, 1.0))
    r.set_title("(b) Implied local elasticity (dots: data, between adjacent bins)", fontsize=10, color=INK, loc="left")
    for ax in (l, r):
        style(ax)
    fig.text(0.01, 0.01, f"Gate5 edges, weekdays 0519-0523, 06-22 h. Each form fitted by weighted least squares to the {int(use.sum())} bin means "
             f"with q/q95 ≤ {FIT_MAX} (weights = edge-hours).\nA fit to binned means, not an estimate; RMSE is in index units, edge-hour weighted. "
             "Grey bands: 95% edge-bootstrap intervals of the data.", fontsize=8, color=INK2)
    fig.tight_layout(rect=(0, 0.08, 1, 1))
    fig.savefig(out / "fig9_congestion_forms_fit_gate5.png", dpi=200)
    plt.close(fig)


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-name", default="congestion_forms_compare_v1")
    ap.add_argument("--replot", action="store_true")
    args = ap.parse_args(argv)
    out = ROOT / "outputs" / args.output_name
    g = pd.read_csv(CURVE)
    if args.replot:
        P = json.loads((out / "fit_params.json").read_text())["fits"]
    else:
        if out.exists():
            raise SystemExit(f"{out} exists; choose a new --output-name or use --replot")
        out.mkdir(parents=True)
        use = g.qr <= FIT_MAX
        P = fit(g.qr[use].to_numpy(), g.t_index[use].to_numpy(), g.n[use].to_numpy(float))
        (out / "fit_params.json").write_text(json.dumps(
            {"input": str(CURVE.relative_to(ROOT)), "fit_max_q_over_q95": FIT_MAX, "bins_used": int(use.sum()), "fits": P}, indent=2))
    fig_theory(out)
    fig_fit(g, P, out)
    print(json.dumps(P, indent=2))


if __name__ == "__main__":
    main()
