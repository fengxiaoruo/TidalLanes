"""Appendix figure: 16-sector radial AM inbound-vs-reverse asymmetry.

Self-contained: reads only inbound_sector_summary.csv (no geo/parquet deps),
writes a polar bar chart straight into the slide figs/ directory.

Run: python data_work/src/diagnostics/plot_inbound_sector_appendix.py
"""
from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib import cm
from matplotlib.colors import Normalize

ROOT = Path(__file__).resolve().parents[2]
SECTOR_CSV = ROOT / "outputs" / "beijing_commuting_direction_evidence_v1" / "metrics" / "inbound_sector_summary.csv"
OUT = ROOT.parent / "Documents" / "PreSlides" / "figs" / "app_inbound_sector.png"


def main() -> None:
    s = pd.read_csv(SECTOR_CSV).sort_values("sector").reset_index(drop=True)
    theta = np.deg2rad(s["theta_mid_deg"].to_numpy())
    width = np.deg2rad(360.0 / len(s)) * 0.92
    inbound = s["inbound_flow"].to_numpy() / 1000.0
    reverse = s["reverse_am_flow"].to_numpy() / 1000.0
    share = s["inbound_share_of_two_way"].to_numpy()

    inb_tot, rev_tot = s["inbound_flow"].sum(), s["reverse_am_flow"].sum()
    pooled_share = inb_tot / (inb_tot + rev_tot)
    pooled_ratio = inb_tot / rev_tot

    fig = plt.figure(figsize=(9.6, 4.6))

    # --- left: polar inbound flow, colored by inbound share ---
    ax = fig.add_subplot(1, 2, 1, projection="polar")
    ax.set_theta_zero_location("E")
    ax.set_theta_direction(1)
    norm = Normalize(vmin=0.5, vmax=0.9)
    colors = cm.RdBu_r(norm(share))
    ax.bar(theta, inbound, width=width, color=colors, edgecolor="white", linewidth=0.5, alpha=0.95)
    ax.set_title("AM inbound flow by home sector\n(color = inbound share of two-way)", fontsize=9, pad=12)
    ax.set_yticklabels([])
    ax.set_xticks(np.deg2rad(np.arange(0, 360, 45)))
    ax.tick_params(labelsize=7)
    sm = cm.ScalarMappable(norm=norm, cmap="RdBu_r")
    sm.set_array([])
    cb = fig.colorbar(sm, ax=ax, fraction=0.045, pad=0.10)
    cb.set_label("inbound / (inbound+reverse)", fontsize=7.5)
    cb.ax.tick_params(labelsize=7)

    # --- right: paired inbound vs reverse bars by sector ---
    ax2 = fig.add_subplot(1, 2, 2)
    idx = np.arange(len(s))
    ax2.barh(idx + 0.2, inbound, height=0.4, color="#C0392B", label="inbound (toward core)")
    ax2.barh(idx - 0.2, reverse, height=0.4, color="#5B8DB8", label="reverse (toward periphery)")
    ax2.set_yticks(idx)
    ax2.set_yticklabels([f"{int(a)}°" for a in s["theta_mid_deg"]], fontsize=6.5)
    ax2.invert_yaxis()
    ax2.set_xlabel("commuters (thousands)", fontsize=8)
    ax2.set_ylabel("home-sector midpoint angle", fontsize=8)
    ax2.set_title("Inbound vs reverse per sector", fontsize=9)
    ax2.legend(fontsize=7, loc="lower right", frameon=False)
    ax2.tick_params(axis="x", labelsize=7)
    ax2.grid(axis="x", alpha=0.25, linewidth=0.5)

    fig.suptitle(
        f"Radial AM asymmetry around the job-weighted center  —  pooled inbound share "
        f"{pooled_share*100:.1f}%,  inbound/reverse {pooled_ratio:.2f}×  "
        f"(work ≤ {s['inner_km'].iloc[0]:.0f} km, home ≥ {s['outer_km'].iloc[0]:.0f} km)",
        fontsize=9.5,
        y=1.02,
    )
    fig.tight_layout()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUT, dpi=200, bbox_inches="tight")
    print(f"wrote {OUT}")
    print(f"pooled inbound share = {pooled_share*100:.1f}%, ratio = {pooled_ratio:.2f}x")


if __name__ == "__main__":
    main()
