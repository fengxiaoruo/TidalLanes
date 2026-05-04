"""
tidal_lane_alt_spec.py
======================
Alternative specification for tidal lane counterfactual.

Specification B: direct travel-time ratio
-----------------------------------------
t_bar_hat[i,j] = (tau_sym / tau_ij)^epsilon

  Congested direction (tau_ij > tau_sym):
      tau_sym / tau_ij < 1  ->  t_bar_hat < 1  (commuting cost FALLS)
      Physical: tidal lane gives more capacity to slow direction -> road improves
      -> more commuters attracted -> welfare gain for many people

  Uncongested direction (tau_ji < tau_sym):
      tau_sym / tau_ji > 1  ->  t_bar_hat > 1  (commuting cost RISES)
      Physical: tidal lane takes capacity from fast direction -> road slows
      -> fewer commuters on this direction -> small welfare loss for few people

  Net: cost savings accrue to the many (congested direction), cost rises hit the
  few (uncongested direction) -> aggregate welfare IMPROVES.

Contrast with Specification A (tidal_lane_counterfactual.py):
  That script uses the congestion-inversion formula xi = lanes*(tau/tau_ff)^(1/delta1)
  to infer equilibrium flows at tau_sym, then backs out t_bar_hat. Because tau_sym <
  tau_obs for the congested direction (with FIXED lanes), it infers FEWER vehicles,
  hence HIGHER t_bar_hat > 1. This is internally consistent but represents a different
  counterfactual: "same capacity, fewer equilibrium commuters." Welfare WORSENS.

Choice of epsilon:
  epsilon = 1: t_bar proportional to tau (simplest). Produces large shocks for pairs
               with tau_ratio > 5x; solver may diverge for top-20.
  epsilon = 0.1: conservative. Confirmed to converge; sign is clearly negative (improves).
  epsilon = 0.5: intermediate; run locally (needs ~40s for top-20 with inner_update=0.3).
  Recommended: report epsilon=0.1 as main result; note sensitivity to epsilon.

Outputs (results/tidal_lane/)
------
  summary_alt_spec.txt
  chi_lr_lf_alt_eps{e}_top20.csv   for each epsilon in EPSILONS
"""

from __future__ import annotations
import sys, warnings
from pathlib import Path
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

SCRIPT_DIR = Path(__file__).resolve().parent
REPL_ROOT  = SCRIPT_DIR.parent
ROOT       = REPL_ROOT.parent

sys.path.insert(0, str(SCRIPT_DIR))
from data_io import load_seattle_commuting_data
from counterfactual import solve_counterfactual_lr_lf

# ── paths ──────────────────────────────────────────────────────────────────
SOURCE_DIR  = ROOT / "data_work/outputs/manual_centerline_rules_v11/data"
V2_INPUT    = REPL_ROOT / "aa_input_square_v2"
MAPPING_CSV = REPL_ROOT / "prepare_from_data_work_square_v2/node_index_mapping_square.csv"
OUT_DIR     = REPL_ROOT / "results/tidal_lane"

# ── model parameters ────────────────────────────────────────────────────────
THETA  = 6.83
DELTA1 = 0.488
LAMBD  = (1.0 / THETA) * DELTA1
ALPHA  = -0.12
BETA   = -0.1
EPS_FL = 1e-4   # floor for zero-population nodes

TOP_N   = 20
EPSILONS = [0.1, 0.5, 1.0]   # travel-time elasticity of iceberg cost


def build_asymmetry_table() -> pd.DataFrame:
    edges = pd.read_parquet(SOURCE_DIR / "qsm_input_edges_square.parquet")
    edges = edges[edges["period"] == "AM"][
        ["grid_o", "grid_d", "i", "j", "tau_obs_min", "tau_ff_min"]
    ].copy()
    idx = edges.set_index(["grid_o", "grid_d"])
    rows, seen = [], set()
    for (go, gd), row in idx.iterrows():
        if (gd, go) not in idx.index:
            continue
        if (go, gd) in seen or (gd, go) in seen:
            continue
        rev = idx.loc[(gd, go)]
        tau_ij, tau_ji = float(row["tau_obs_min"]), float(rev["tau_obs_min"])
        tau_sym = (tau_ij + tau_ji) / 2.0
        rows.append({
            "grid_o": go, "grid_d": gd,
            "i_raw": int(row["i"]), "j_raw": int(row["j"]),
            "tau_ij": tau_ij, "tau_ji": tau_ji, "tau_sym": tau_sym,
            "asym_ratio": max(tau_ij, tau_ji) / min(tau_ij, tau_ji),
        })
        seen.add((go, gd)); seen.add((gd, go))
    df = pd.DataFrame(rows).sort_values("asym_ratio", ascending=False).reset_index(drop=True)
    return df


def build_t_bar_hat_direct(selected: pd.DataFrame, epsilon: float,
                            raw_to_aa: dict, n: int) -> np.ndarray:
    """
    t_bar_hat[i,j] = (tau_sym / tau_ij)^epsilon
    Congested direction: ratio < 1 -> cost falls
    Uncongested direction: ratio > 1 -> cost rises
    """
    t_bar_hat = np.ones((n, n), dtype=float)
    skipped = 0
    for _, row in selected.iterrows():
        i_aa = raw_to_aa.get(row["i_raw"])
        j_aa = raw_to_aa.get(row["j_raw"])
        if i_aa is None or j_aa is None:
            skipped += 1
            continue
        t_bar_hat[i_aa - 1, j_aa - 1] = (row["tau_sym"] / row["tau_ij"]) ** epsilon
        t_bar_hat[j_aa - 1, i_aa - 1] = (row["tau_sym"] / row["tau_ji"]) ** epsilon
    if skipped:
        print(f"  [warn] {skipped} pairs not in AA node set, skipped")
    return t_bar_hat


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    print("Building asymmetry table …")
    asym_df = build_asymmetry_table()
    selected = asym_df.head(TOP_N).copy()
    selected["rank"] = range(1, TOP_N + 1)
    print(f"  {len(asym_df)} bidirectional pairs; selecting top {TOP_N}")
    print(f"  Asymmetry ratio: {selected['asym_ratio'].max():.2f} (max) "
          f"… {selected['asym_ratio'].min():.2f} (rank {TOP_N})")

    m = pd.read_csv(MAPPING_CSV)
    raw_to_aa = dict(zip(m["node_i"].astype(int), m["node_i_aa_1based"].astype(int)))
    aa_to_grid = m.set_index("node_i_aa_1based")["grid_id"].to_dict()

    print("\nLoading AA baseline data …")
    data = load_seattle_commuting_data(
        adjmat_path  = V2_INPUT / "sparse_adjmat_seattle.csv",
        node_path    = V2_INPUT / "node_lr_lf_seattle.csv",
        commute_path = V2_INPUT / "sparse_commute_seattle.csv",
    )
    n       = data.n
    l_r_raw = np.maximum(data.l_r_raw, EPS_FL)
    l_f_raw = np.maximum(data.l_f_raw, EPS_FL)
    l_r     = l_r_raw / l_r_raw.sum()
    l_f     = l_f_raw / l_f_raw.sum()
    l_bar   = float(np.mean([l_r_raw.sum(), l_f_raw.sum()]))
    xi_ij   = data.xi_ij
    print(f"  N = {n} nodes")

    SEP = "=" * 64
    lines = [
        "TIDAL LANE — SPECIFICATION B: DIRECT TRAVEL-TIME RATIO",
        f"t_bar_hat[i,j] = (tau_sym / tau_ij)^epsilon",
        f"Model: theta={THETA}, delta1={DELTA1}, alpha={ALPHA}, beta={BETA}",
        f"Policy: symmetrize top-{TOP_N} most asymmetric AM-peak pairs",
        "",
        "Selected pairs (top 20 by asymmetry ratio):",
    ]
    for _, r in selected.iterrows():
        slow_tau = max(r["tau_ij"], r["tau_ji"])
        fast_tau = min(r["tau_ij"], r["tau_ji"])
        lines.append(f"  rank {int(r['rank']):2d}: {r['grid_o']} <-> {r['grid_d']}"
                     f"  slow={slow_tau:.1f}min  fast={fast_tau:.1f}min"
                     f"  sym={r['tau_sym']:.1f}min  ratio={r['asym_ratio']:.2f}x")

    results = {}
    for epsilon in EPSILONS:
        print(f"\n── epsilon = {epsilon} ──────────────────────────────────────")
        t_bar_hat = build_t_bar_hat_direct(selected, epsilon, raw_to_aa, n)
        nz = t_bar_hat != 1.0
        below = (t_bar_hat < 1.0) & nz
        above = (t_bar_hat > 1.0) & nz
        print(f"  t_bar_hat < 1 (congested, cost falls): {below.sum()} entries  "
              f"min={t_bar_hat[below].min():.4f}")
        print(f"  t_bar_hat > 1 (uncongested, cost rises): {above.sum()} entries  "
              f"max={t_bar_hat[above].max():.4f}")
        print(f"  Solving …")
        result = solve_counterfactual_lr_lf(
            t_bar_hat      = t_bar_hat,
            t_bar_prod_hat = np.ones(n),
            u_bar_hat      = np.ones(n),
            l_bar_hat      = 1.0,
            l_r=l_r, l_f=l_f, l_bar=l_bar, xi_ij=xi_ij,
            theta=THETA, lambd=LAMBD, alpha=ALPHA, beta=BETA,
            tol=1e-4, inner_update=0.3,
        )
        results[epsilon] = result
        chi = result.chi_hat
        direction = "welfare IMPROVES" if chi < 1 else "welfare WORSENS"
        print(f"  chi_hat = {chi:.10f}  (chi-1)*1e6 = {(chi-1)*1e6:.4f}  {direction}")
        print(f"  l_r_hat std = {result.l_r_hat.std():.3e}  "
              f"max gain = +{result.l_r_hat.max()-1:.4%}  "
              f"max loss = {result.l_r_hat.min()-1:.4%}")

        # Save node-level CSV
        out_df = pd.DataFrame({
            "node_aa": np.arange(1, n + 1),
            "grid_id": [aa_to_grid.get(k, "?") for k in range(1, n + 1)],
            "chi_hat": chi,
            "l_r_hat": result.l_r_hat,
            "l_f_hat": result.l_f_hat,
        })
        eps_str = str(epsilon).replace(".", "p")
        out_df.to_csv(OUT_DIR / f"chi_lr_lf_alt_eps{eps_str}_top{TOP_N}.csv", index=False)

        lines += [
            "", SEP,
            f"epsilon = {epsilon}",
            f"  t_bar_hat range (congested dir): "
            f"{t_bar_hat[below].min():.4f} – {t_bar_hat[below].max():.4f}",
            f"  t_bar_hat range (uncongested dir): "
            f"{t_bar_hat[above].min():.4f} – {t_bar_hat[above].max():.4f}",
            f"  chi_hat = {chi:.10f}",
            f"  (chi_hat - 1) x 10^6 = {(chi-1)*1e6:.4f}  ({direction})",
            f"  l_r_hat: mean={result.l_r_hat.mean():.8f}  std={result.l_r_hat.std():.3e}",
            f"    max gain: node {result.l_r_hat.argmax()+1} "
            f"({aa_to_grid.get(result.l_r_hat.argmax()+1,'?')}) "
            f"+{result.l_r_hat.max()-1:.4%}",
            f"    max loss: node {result.l_r_hat.argmin()+1} "
            f"({aa_to_grid.get(result.l_r_hat.argmin()+1,'?')}) "
            f"{result.l_r_hat.min()-1:.4%}",
            f"  l_f_hat: mean={result.l_f_hat.mean():.8f}  std={result.l_f_hat.std():.3e}",
            f"    max gain: node {result.l_f_hat.argmax()+1} "
            f"({aa_to_grid.get(result.l_f_hat.argmax()+1,'?')}) "
            f"+{result.l_f_hat.max()-1:.4%}",
            f"    max loss: node {result.l_f_hat.argmin()+1} "
            f"({aa_to_grid.get(result.l_f_hat.argmin()+1,'?')}) "
            f"{result.l_f_hat.min()-1:.4%}",
        ]

    # Cross-epsilon comparison
    lines += ["", SEP, "EPSILON SENSITIVITY"]
    for eps in EPSILONS:
        r = results[eps]
        lines.append(f"  eps={eps:4.1f}: (chi-1)*1e6 = {(r.chi_hat-1)*1e6:+8.4f}  "
                     f"l_r std={r.l_r_hat.std():.3e}  "
                     f"{'IMPROVES' if r.chi_hat < 1 else 'WORSENS'}")

    summary = "\n".join(lines)
    (OUT_DIR / "summary_alt_spec.txt").write_text(summary)
    print("\n" + summary)
    print(f"\nOutputs saved to: {OUT_DIR}")


if __name__ == "__main__":
    main()
