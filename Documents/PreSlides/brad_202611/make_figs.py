"""Figures for the November 2026 discussion deck with Brad Ross.
Reads pipeline outputs only; writes PDFs to Documents/L3_figs/brad_202611/.
Run: /Users/fxr/miniconda3/envs/research/bin/python make_figs.py
"""
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "data_work" / "outputs"
FIG = ROOT / "Documents" / "L3_figs" / "brad_202611"
NAVY, GRAY, LIGHT = "#193769", "#8a8a8a", "#9fb4d6"
plt.rcParams.update({"font.size": 9, "axes.spines.top": False, "axes.spines.right": False,
                     "axes.linewidth": .6, "font.family": "sans-serif", "pdf.fonttype": 42,
                     "legend.frameon": False})
FS = OUT / "congestion_floor_sensitivity_v1"


def floor_shift():
    e = pd.read_csv(FS / "2_shift_grid_edge.csv")
    s = pd.read_csv(FS / "2_shift_grid_segment.csv")
    fig, ax = plt.subplots(figsize=(5.6, 2.9))
    e = e[e.delta <= 0.0601]
    s = s[s.delta <= 0.0401]
    ax.plot(100 * e.delta, e.beta_NIV_z, "-o", color=NAVY, ms=3.5, lw=1.4, label="Grid edge")
    ax.plot(100 * s.delta, s.beta_NIV_z, "-s", color=GRAY, ms=3.5, lw=1.2, label="Road segment")
    ax.axvline(0, color="k", lw=.5, ls=":")
    ax.axvspan(2.3, 3.0, color=LIGHT, alpha=.45, lw=0)
    ax.text(2.65, 0.09, "floor the\ndata prefer", ha="center", va="bottom", fontsize=7.5, color=NAVY)
    ax.annotate("0.245", (0, 0.245), textcoords="offset points", xytext=(-26, 6), fontsize=8, color=NAVY)
    ax.annotate("0.370", (3, 0.3698), textcoords="offset points", xytext=(6, -10), fontsize=8, color=NAVY)
    ax.set_xlabel("Shift of the free-flow floor relative to the night mean (%)")
    ax.set_ylabel("Estimated elasticity")
    ax.set_ylim(0, 1.15)
    ax.legend(loc="upper left")
    fig.tight_layout()
    fig.savefig(FIG / "floor_shift.pdf")
    plt.close(fig)


def local_elasticity():
    spec = "IV ln q by ln z, unit FE + date x hour FE"
    sel = ["all hours in window", "z > unit p50", "z > unit p70", "z > unit p85"]
    lab = ["All hours", "Load > p50", "Load > p70", "Load > p85"]
    fig, ax = plt.subplots(figsize=(4.3, 2.9))
    for k, (f, c, m, name) in enumerate([("edge", NAVY, "o", "Grid edge"), ("segment", GRAY, "s", "Road segment")]):
        d = pd.read_csv(FS / f"5_local_linear_iv_{f}.csv")
        d = d[(d.sample_window == "06-22h") & (d.spec == spec)].set_index("selection").loc[sel]
        x = np.arange(4) + (k - .5) * .16
        ax.errorbar(x, d.beta, yerr=1.96 * d.se_cluster, fmt=m, color=c, ms=4.5, lw=1, capsize=2, label=name)
    ax.set_xticks(range(4)); ax.set_xticklabels(lab)
    ax.set_ylabel("Elasticity of time to flow (IV)")
    ax.set_ylim(0, .56)
    ax.legend(loc="upper left")
    fig.tight_layout()
    fig.savefig(FIG / "local_elasticity.pdf")
    plt.close(fig)


def binned_shape():
    d = pd.read_csv(FS / "7_binned_shape_edge.csv")
    d = d[(d.window == "06-22h") & d.x.str.startswith("ln q")]
    fig, ax = plt.subplots(figsize=(4.3, 2.9))
    ax.plot(d.x_demeaned_mean, d.ln_t_minus_lnT0_p50, "-o", color=NAVY, ms=3.5, lw=1.3)
    ax.axhline(0, color="k", lw=.5, ls=":")
    ax.text(-.88, .004, "night mean", fontsize=7.5, color="k", va="bottom")
    ax.set_xlabel("Log flow, within-edge deviation (20 bins)")
    ax.set_ylabel("Log time above night mean (median)")
    ax.set_ylim(-.01, .12)
    fig.tight_layout()
    fig.savefig(FIG / "binned_shape.pdf")
    plt.close(fig)


def decomposition():
    d = pd.read_csv(OUT / "theta_first_second_order_osm1b_b035_alldaytt0709_v1" / "summary.csv").set_index("tag").loc["theta_6p83"]
    steps = [("First-order\nterm", d.fo_Wbar_pct), ("Shock\nsize", d.gap_shock_size_pct),
             ("Congestion\nfeedback", d.gap_externality_pct), ("Sorting", d.gap_sorting_pct)]
    fig, ax = plt.subplots(figsize=(5.6, 2.9))
    lev, x = 0.0, 0
    for name, v in steps:
        ax.bar(x, v, bottom=lev, color=NAVY if v > 0 else GRAY, width=.6)
        ax.text(x, lev + v / 2, f"{v:+.3f}", ha="center", va="center", fontsize=8, color="w")
        lev += v
        if name.startswith("Congestion"):
            ax.plot([-.3, x + .3], [lev, lev], color="k", lw=.5, ls=":")
            ax.text(x, lev + .05, f"short run {d.Wbar_pct_st:.3f}", fontsize=8, ha="center", va="bottom")
        x += 1
    ax.bar(x, d.Wbar_pct_lt, color=LIGHT, width=.6)
    ax.text(x, d.Wbar_pct_lt + .012, f"{d.Wbar_pct_lt:.3f}", ha="center", fontsize=8)
    ax.set_xticks(range(5)); ax.set_xticklabels([s[0] for s in steps] + ["Long run"])
    ax.set_ylabel("Welfare change (%)")
    ax.set_ylim(0, .78)
    fig.tight_layout()
    fig.savefig(FIG / "decomposition.pdf")
    plt.close(fig)


def policies():
    d = pd.read_csv(OUT / "policy_comparison_osm1b_ce_floor_b035_fl001_ks_od2025_alldaytt0709_v1" / "summary_direct_vs_longrun.csv").set_index("policy_id")
    order = [("P1_tidal", "Tidal lanes"), ("P2_expansion_same_corridor", "Expansion,\nsame corridors"),
             ("P3_expansion_qdelay", "Expansion,\ntargeted"), ("P5_uniform_speed_1.10", "All lanes\n+10%"),
             ("P7_cordon_pricing", "Cordon\ntoll"), ("P6_first_best_pricing", "Pigouvian\ntoll")]
    fig, ax = plt.subplots(figsize=(6.2, 2.9))
    x = np.arange(len(order))
    st = [d.loc[k, "phys_time_saved_st_sec"] for k, _ in order]
    lt = [d.loc[k, "phys_time_saved_lt_sec"] for k, _ in order]
    ax.bar(x - .19, st, .38, color=LIGHT, label="Direct (short run)")
    ax.bar(x + .19, lt, .38, color=NAVY, label="Long run")
    for xi, a, b in zip(x, st, lt):
        ax.text(xi - .19, a + 4, f"{a:.0f}", ha="center", fontsize=7.5)
        ax.text(xi + .19, b + (4 if b > 0 else -14), f"{b:.0f}", ha="center", fontsize=7.5)
    ax.axhline(0, color="k", lw=.5)
    ax.set_xticks(x); ax.set_xticklabels([n for _, n in order])
    ax.set_ylabel("Time saved per commuter (seconds)")
    ax.set_ylim(-40, 310)
    ax.legend(loc="upper left")
    fig.tight_layout()
    fig.savefig(FIG / "policies_time.pdf")
    plt.close(fig)


def flow_density():
    d = pd.read_csv(OUT / "flow_density_per_lane_v1" / "0_within_segment_curves.csv")
    d = d[(d.level == "10min") & d.kept]
    fig, ax = plt.subplots(figsize=(5.6, 2.9))
    for c, col, m, name in [(2, NAVY, "o", "Expressway"), (3, GRAY, "s", "Arterial"), (4, LIGHT, "^", "Secondary")]:
        g = d[d.road_class == c].sort_values("x_mean")
        ax.errorbar(g.x_mean, g.qn, yerr=1.96 * g.qn_se, fmt="-" + m, color=col, ms=3.5, lw=1.3, capsize=0, label=name)
    ax.invert_xaxis()
    ax.set_xlabel("Speed relative to the segment's free-flow speed (slower to the right)")
    ax.set_ylabel("Flow relative to segment mean")
    ax.legend(loc="lower right")
    fig.tight_layout()
    fig.savefig(FIG / "flow_density.pdf")
    plt.close(fig)


if __name__ == "__main__":
    FIG.mkdir(parents=True, exist_ok=True)
    for f in (floor_shift, local_elasticity, binned_shape, decomposition, policies, flow_density):
        f()
    print(sorted(p.name for p in FIG.glob("*.pdf")))
