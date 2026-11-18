"""
Slide version of the four candidate congestion functions: one column per form
=============================================================================

Same illustrative parameters as fig8 (plot_congestion_forms_compare.py), x = q/c:
    constant          t/t0 = 1.15 x^0.2
    floor + constant  t/t0 = max{1, 1.15 x^0.2}        (kink at x = 1.15^-5 = 0.50)
    BPR               t/t0 = 1 + 0.15 x^4
    linear speed-density (uncongested branch) t/t0 = 2 / (1 + sqrt(1 - x)); dashed: other branch
Top row: level; bottom row: local elasticity d ln t / d ln q. Shared axes across columns.

Output: data_work/outputs/<output-name>/ (refuses to overwrite)
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
from plot_congestion_forms_compare import COL, elast, f_bpr, f_const, f_floor, f_kr  # noqa: E402
from plot_congestion_technology_gate5 import INK, INK2, style  # noqa: E402

ROOT = Path(__file__).resolve().parents[3]


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-name", default="congestion_forms_slide_v1")
    args = ap.parse_args(argv)
    out = ROOT / "outputs" / args.output_name
    if out.exists():
        raise SystemExit(f"{out} exists; choose a new --output-name")
    out.mkdir(parents=True)

    xe = np.linspace(0.005, 1.4, 700)
    xk = np.linspace(0.005, 0.999, 600)
    forms = [("const", "Constant elasticity", f_const, (1.15, 0.2), xe),
             ("floor", "Free-flow floor + constant", f_floor, (1.0, 1.15 ** -5, 0.2), xe),
             ("bpr", "BPR", f_bpr, (1.0, 0.15, 4.0), xe),
             ("kr", "Linear speed-density", f_kr, (1.0, 1.0), xk)]
    plt.rcParams.update({"font.size": 14})
    fig, ax = plt.subplots(2, 4, figsize=(16, 7.2), sharex=True, sharey="row")
    for j, (k, name, f, p, x) in enumerate(forms):
        top, bot = ax[0, j], ax[1, j]
        top.axhline(1, color=INK2, lw=0.8, ls=":")
        top.plot(x, f(x, *p), color=COL[k], lw=3)
        bot.plot(x, elast(f, x, *p), color=COL[k], lw=3)
        if k == "kr":
            s = np.sqrt(1 - xk)
            top.plot(xk, 2 / (1 - s), color=COL[k], lw=2, ls="--")
            top.text(0.5, 2.85, "queue branch\n(not used)", fontsize=12, color=INK2, ha="center")
            bot.text(0.5, 1.12, "→ ∞ at q = c", fontsize=12, color=INK2, ha="center")
        if k == "floor":
            top.annotate("kink", xy=(1.15 ** -5, 1.0), xytext=(0.15, 1.6), fontsize=12, color=INK2,
                         arrowprops=dict(arrowstyle="-", color=INK2, lw=1))
        top.set_title(name, fontsize=15, color=INK, loc="left")
        for a in (top, bot):
            a.axvline(1, color=INK2, lw=0.6, ls=":")
            style(a)
            a.tick_params(labelsize=12)
        bot.set_xlabel("q / c", fontsize=14)
    ax[0, 0].set(ylim=(0, 3.3), xlim=(0, 1.4))
    ax[0, 0].set_ylabel("t / t0", fontsize=14)
    ax[1, 0].set(ylim=(0, 1.3))
    ax[1, 0].set_ylabel("local elasticity", fontsize=14)
    fig.tight_layout()
    fig.savefig(out / "fig13_congestion_forms_slide.png", dpi=200)
    plt.close(fig)
    print("ok")


if __name__ == "__main__":
    main()
