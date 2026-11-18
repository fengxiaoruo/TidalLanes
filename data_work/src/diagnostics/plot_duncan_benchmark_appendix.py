"""Appendix figure: national jobs-housing Duncan D benchmark.

Reads the already-computed duncan_cross_city.csv (no re-streaming) and draws:
  (A) distribution of township-level D across the top-100 cities, Beijing +
      national median marked;
  (B) scatter of D vs city size (log commuters) with an OLS trend, showing
      Beijing sits above the size gradient (D is aspatial & scales with size).

Run: python data_work/src/diagnostics/plot_duncan_benchmark_appendix.py
"""
from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
CSV = ROOT / "outputs" / "beijing_commuting_direction_evidence_v1" / "metrics" / "duncan_cross_city.csv"
OUT = ROOT.parent / "Documents" / "PreSlides" / "figs" / "app_duncan_cross_city.png"

BLUE, RED, GREY = "#2C6FB0", "#C0392B", "#9AA4AD"


def main() -> None:
    df = pd.read_csv(CSV)
    d = df["duncan_D"].to_numpy()
    med = np.median(d)
    bj = df[df["city_key"].str.contains("北京")].iloc[0]
    sh = df[df["city_key"].str.contains("上海")]
    x = np.log10(df["total_commuters"].to_numpy())
    corr = np.corrcoef(x, d)[0, 1]

    fig, axes = plt.subplots(1, 2, figsize=(9.8, 4.1))

    # (A) distribution
    ax = axes[0]
    ax.hist(d, bins=22, color=GREY, edgecolor="white", linewidth=0.4, alpha=0.9)
    ax.axvline(med, color="black", ls="--", lw=1.1)
    ax.axvline(bj["duncan_D"], color=RED, lw=1.6)
    ax.text(med, ax.get_ylim()[1] * 0.92, f" national median {med:.2f}", fontsize=7.5, color="black")
    ax.text(bj["duncan_D"], ax.get_ylim()[1] * 0.72, f"Beijing {bj['duncan_D']:.2f}\n(1st of {len(df)})",
            fontsize=8, color=RED, ha="right", fontweight="bold")
    ax.set_xlabel("township-level jobs–housing Duncan $D$", fontsize=8.5)
    ax.set_ylabel("number of cities", fontsize=8.5)
    ax.set_title(f"(A) Distribution across top-{len(df)} cities", fontsize=9)
    ax.tick_params(labelsize=7.5)

    # (B) scatter vs size
    ax = axes[1]
    ax.scatter(x, d, s=22, c=GREY, alpha=0.75, edgecolors="white", linewidths=0.3)
    b1, b0 = np.polyfit(x, d, 1)
    xs = np.linspace(x.min(), x.max(), 50)
    ax.plot(xs, b0 + b1 * xs, color=BLUE, lw=1.3, label=f"OLS (corr $={corr:.2f}$)")
    ax.scatter([np.log10(bj["total_commuters"])], [bj["duncan_D"]], s=95, marker="*", c=RED,
               edgecolors="black", linewidths=0.4, zorder=5)
    if len(sh):
        ax.scatter([np.log10(sh["total_commuters"].iloc[0])], [sh["duncan_D"].iloc[0]], s=42, marker="D",
                   c="#E08A1E", edgecolors="black", linewidths=0.3, zorder=5)
    # --- leader-line labels for the highest-D cities (symbols unchanged) ---
    EN = {"北京市||北京市": "Beijing", "上海市||上海市": "Shanghai",
          "新疆维吾尔自治区||乌鲁木齐市": "Urumqi", "贵州省||贵阳市": "Guiyang",
          "辽宁省||沈阳市": "Shenyang", "四川省||成都市": "Chengdu",
          "广东省||广州市": "Guangzhou"}
    # per-city label offset (points) & alignment, tuned to avoid overlap
    OFF = {"Beijing": (12, 3, "left"), "Shanghai": (12, -1, "left"),
           "Urumqi": (9, 8, "left"), "Guiyang": (-6, 13, "right"),
           "Shenyang": (6, 13, "left"), "Chengdu": (-8, 12, "right"),
           "Guangzhou": (10, -12, "left")}
    top = df.sort_values("duncan_D", ascending=False).head(7)
    ax.set_xlim(x.min() - 0.05, x.max() + 0.30)
    ax.set_ylim(d.min() - 0.01, d.max() + 0.02)
    for _, r in top.iterrows():
        name = EN[r["city_key"]]
        dx, dy, ha = OFF[name]
        is_bj = "北京" in r["city_key"]
        ax.annotate(name, xy=(np.log10(r["total_commuters"]), r["duncan_D"]),
                    xytext=(dx, dy), textcoords="offset points",
                    fontsize=6.8, va="center", ha=ha,
                    color=RED if is_bj else "#333333",
                    fontweight="bold" if is_bj else "normal",
                    arrowprops=dict(arrowstyle="-", lw=0.4, color=GREY, shrinkA=0, shrinkB=1.5))

    ax.set_xlabel("city size: $\\log_{10}$ within-city commuters", fontsize=8.5)
    ax.set_ylabel("Duncan $D$", fontsize=8.5)
    ax.set_title("(B) $D$ vs. city size — Beijing above the gradient", fontsize=9)
    ax.legend(fontsize=7.5, loc="upper left", frameon=False)
    ax.tick_params(labelsize=7.5)
    ax.grid(alpha=0.2, lw=0.5)

    fig.tight_layout()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUT, dpi=200, bbox_inches="tight")
    print(f"wrote {OUT}")
    print(f"Beijing D={bj['duncan_D']:.3f} rank 1/{len(df)}; median={med:.3f}; corr(D,log size)={corr:.2f}")


if __name__ == "__main__":
    main()
