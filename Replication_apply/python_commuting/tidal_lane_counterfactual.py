"""
tidal_lane_counterfactual.py
============================
Counterfactual: symmetrize travel costs on the top-N most asymmetric
directed pairs (AM peak), simulating a tidal lane reallocation.

Policy logic
------------
For each selected pair (i→j, j→i):
  - Compute symmetric travel time: tau_sym = (tau_obs_ij + tau_obs_ji) / 2
  - For each direction, recompute "symmetric" traffic flow:
      xi_sym = lanes × (tau_sym / tau_ff) ^ (1/delta1)
  - Construct t_bar_hat via first-order exact-hat inversion:
      t_bar_hat[i,j] = (xi_sym_ij / xi_asym_ij) ^ (-(1+theta*lambda)/theta)
  - This raises costs on the fast direction and lowers them on the slow
    direction — exactly what a tidal lane achieves at equilibrium.

All other edges: t_bar_hat = 1.0 (unchanged).

Outputs (results/tidal_lane/)
------
  summary.txt             – key welfare & population stats
  chi_lr_lf_top20.csv     – node-level chi_hat, l_r_hat, l_f_hat (top-20 policy)
  chi_lr_lf_top50.csv     – same for top-50 policy
  selected_pairs_top20.csv – which pairs were symmetrized
  selected_pairs_top50.csv
"""

from __future__ import annotations
import sys, warnings
from pathlib import Path
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

SCRIPT_DIR = Path(__file__).resolve().parent
REPL_ROOT  = SCRIPT_DIR.parent
ROOT       = REPL_ROOT.parent          # TidalLanes/

sys.path.insert(0, str(SCRIPT_DIR))
from data_io import load_seattle_commuting_data
from counterfactual import solve_counterfactual_lr_lf

# ── paths ──────────────────────────────────────────────────────────────────
SOURCE_DIR  = ROOT / "data_work/outputs/manual_centerline_rules_v11/data"
V2_INPUT    = REPL_ROOT / "aa_input_square_v2"
MAPPING_CSV = REPL_ROOT / "prepare_from_data_work_square_v2/node_index_mapping_square.csv"
OUT_DIR     = REPL_ROOT / "results/tidal_lane"

# ── model parameters (Seattle / AA 2022) ───────────────────────────────────
THETA  = 6.83
DELTA1 = 0.488
LAMBD  = (1.0 / THETA) * DELTA1
ALPHA  = -0.12
BETA   = -0.1
EPS    = 1e-4          # floor for zero-population nodes
EXPONENT = (1 + THETA * LAMBD) / THETA   # ≈ 0.218  (for t_bar_hat inversion)


# ── Step 1: build bidirectional asymmetry table ─────────────────────────────
def build_asymmetry_table() -> pd.DataFrame:
    edges = pd.read_parquet(SOURCE_DIR / "qsm_input_edges_square.parquet")
    edges = edges[edges["period"] == "AM"][
        ["grid_o", "grid_d", "i", "j",
         "tau_obs_min", "tau_ff_min", "lanes_directional"]
    ].copy()

    idx = edges.set_index(["grid_o", "grid_d"])
    rows, seen = [], set()

    for (go, gd), row in idx.iterrows():
        if (gd, go) not in idx.index:
            continue
        if (go, gd) in seen or (gd, go) in seen:
            continue
        rev = idx.loc[(gd, go)]

        tau_ij, tau_ji     = float(row["tau_obs_min"]), float(rev["tau_obs_min"])
        tau_ff_ij, tau_ff_ji = float(row["tau_ff_min"]), float(rev["tau_ff_min"])
        lanes_ij, lanes_ji = float(row["lanes_directional"]), float(rev["lanes_directional"])
        tau_sym = (tau_ij + tau_ji) / 2.0

        # traffic flows (from paper's congestion inversion)
        xi_ij  = lanes_ij  * (max(tau_ij,  tau_ff_ij)  / tau_ff_ij)  ** (1 / DELTA1)
        xi_ji  = lanes_ji  * (max(tau_ji,  tau_ff_ji)  / tau_ff_ji)  ** (1 / DELTA1)
        xi_sym_ij = lanes_ij  * (max(tau_sym, tau_ff_ij)  / tau_ff_ij)  ** (1 / DELTA1)
        xi_sym_ji = lanes_ji  * (max(tau_sym, tau_ff_ji)  / tau_ff_ji)  ** (1 / DELTA1)

        rows.append({
            "grid_o": go, "grid_d": gd,
            "i_raw": int(row["i"]), "j_raw": int(row["j"]),  # raw node_i indices
            "tau_ij": tau_ij, "tau_ji": tau_ji,
            "tau_ff_ij": tau_ff_ij, "tau_ff_ji": tau_ff_ji,
            "tau_sym": tau_sym,
            "lanes_ij": lanes_ij, "lanes_ji": lanes_ji,
            "xi_ij":  xi_ij,  "xi_ji":  xi_ji,
            "xi_sym_ij": xi_sym_ij, "xi_sym_ji": xi_sym_ji,
            "asym_ratio": max(tau_ij, tau_ji) / min(tau_ij, tau_ji),
        })
        seen.add((go, gd)); seen.add((gd, go))

    df = pd.DataFrame(rows).sort_values("asym_ratio", ascending=False).reset_index(drop=True)
    return df


# ── Step 2: map raw node_i → AA 1-based index ───────────────────────────────
def build_node_map() -> dict[int, int]:
    m = pd.read_csv(MAPPING_CSV)
    return dict(zip(m["node_i"].astype(int), m["node_i_aa_1based"].astype(int)))


# ── Step 3: construct t_bar_hat for selected pairs ──────────────────────────
def build_t_bar_hat(selected: pd.DataFrame, n: int,
                    raw_to_aa: dict[int, int]) -> np.ndarray:
    t_bar_hat = np.ones((n, n), dtype=float)

    for _, row in selected.iterrows():
        i_aa = raw_to_aa.get(row["i_raw"])
        j_aa = raw_to_aa.get(row["j_raw"])
        if i_aa is None or j_aa is None:
            print(f"  [warn] pair {row['grid_o']}↔{row['grid_d']} not in AA node set, skipped")
            continue

        xi_ij, xi_sym_ij = row["xi_ij"], row["xi_sym_ij"]
        xi_ji, xi_sym_ji = row["xi_ji"], row["xi_sym_ji"]

        if xi_ij > 0:
            t_bar_hat[i_aa - 1, j_aa - 1] = (xi_sym_ij / xi_ij) ** (-EXPONENT)
        if xi_ji > 0:
            t_bar_hat[j_aa - 1, i_aa - 1] = (xi_sym_ji / xi_ji) ** (-EXPONENT)

    return t_bar_hat


# ── Step 4: run counterfactual ──────────────────────────────────────────────
def run_policy(label: str, t_bar_hat: np.ndarray,
               l_r: np.ndarray, l_f: np.ndarray, l_bar: float,
               xi_ij: np.ndarray, n: int):
    print(f"\n  Solving {label} …")
    result = solve_counterfactual_lr_lf(
        t_bar_hat      = t_bar_hat,
        t_bar_prod_hat = np.ones(n),
        u_bar_hat      = np.ones(n),
        l_bar_hat      = 1.0,
        l_r=l_r, l_f=l_f, l_bar=l_bar,
        xi_ij=xi_ij,
        theta=THETA, lambd=LAMBD, alpha=ALPHA, beta=BETA,
        tol=1e-4, inner_update=0.3,
    )
    return result


# ── Step 5: report ──────────────────────────────────────────────────────────
def report(label: str, result, selected: pd.DataFrame,
           raw_to_aa: dict[int, int], mapping: pd.DataFrame,
           lines: list[str]):
    chi = result.chi_hat
    lr  = result.l_r_hat
    lf  = result.l_f_hat

    # welfare
    lines.append(f"\n{'='*60}")
    lines.append(f"Policy: {label}")
    lines.append(f"  Pairs symmetrized: {len(selected)}")
    lines.append(f"  Asymmetry ratio range: "
                 f"{selected['asym_ratio'].min():.2f} – {selected['asym_ratio'].max():.2f}")
    lines.append(f"\n  chi_hat = {chi:.10f}  "
                 f"({'welfare IMPROVES' if chi < 1 else 'welfare WORSENS'})")
    lines.append(f"  (chi_hat - 1) × 10⁶ = {(chi-1)*1e6:.4f}")

    # population
    lines.append(f"\n  l_r_hat (residents):")
    lines.append(f"    mean={lr.mean():.8f}  std={lr.std():.3e}")
    lines.append(f"    max gain: node {lr.argmax()+1} → +{lr.max()-1:.4%}")
    lines.append(f"    max loss: node {lr.argmin()+1} → {lr.min()-1:.4%}")

    lines.append(f"\n  l_f_hat (workers):")
    lines.append(f"    mean={lf.mean():.8f}  std={lf.std():.3e}")
    lines.append(f"    max gain: node {lf.argmax()+1} → +{lf.max()-1:.4%}")
    lines.append(f"    max loss: node {lf.argmin()+1} → {lf.min()-1:.4%}")

    # top movers (join back to grid_id)
    aa_to_grid = mapping.set_index("node_i_aa_1based")["grid_id"].to_dict()
    lr_dev = np.abs(lr - 1)
    lf_dev = np.abs(lf - 1)
    top_lr_idx = np.argsort(lr_dev)[-10:][::-1]
    lines.append(f"\n  Top 10 nodes by |l_r_hat - 1|:")
    lines.append(f"    {'node':>6}  {'grid_id':<14}  {'l_r_hat':>12}  {'l_f_hat':>12}")
    for idx in top_lr_idx:
        gid = aa_to_grid.get(idx+1, "?")
        lines.append(f"    {idx+1:>6}  {gid:<14}  {lr[idx]:>12.8f}  {lf[idx]:>12.8f}")


# ── Main ────────────────────────────────────────────────────────────────────
def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    print("Building asymmetry table …")
    asym_df = build_asymmetry_table()
    print(f"  Bidirectional pairs: {len(asym_df)}")
    print(f"  Asymmetry ratio p50={asym_df['asym_ratio'].median():.2f}  "
          f"p90={asym_df['asym_ratio'].quantile(.9):.2f}  "
          f"max={asym_df['asym_ratio'].max():.2f}")

    raw_to_aa = build_node_map()
    mapping   = pd.read_csv(MAPPING_CSV)

    print("\nLoading AA baseline data …")
    data = load_seattle_commuting_data(
        adjmat_path  = V2_INPUT / "sparse_adjmat_seattle.csv",
        node_path    = V2_INPUT / "node_lr_lf_seattle.csv",
        commute_path = V2_INPUT / "sparse_commute_seattle.csv",
    )
    n       = data.n
    l_r_raw = np.maximum(data.l_r_raw, EPS)
    l_f_raw = np.maximum(data.l_f_raw, EPS)
    l_r     = l_r_raw / l_r_raw.sum()
    l_f     = l_f_raw / l_f_raw.sum()
    l_bar   = float(np.mean([l_r_raw.sum(), l_f_raw.sum()]))
    xi_ij   = data.xi_ij
    print(f"  N = {n} nodes")

    lines = ["TIDAL LANE COUNTERFACTUAL — SYMMETRIZE TOP-N ASYMMETRIC PAIRS (AM PEAK)",
             f"Model parameters: theta={THETA}, delta1={DELTA1}, alpha={ALPHA}, beta={BETA}",
             f"t_bar_hat inversion exponent: {EXPONENT:.4f}"]

    results = {}
    for top_n in [20, 50]:
        label    = f"Top-{top_n}"
        selected = asym_df.head(top_n).copy()

        # save selected pairs
        selected.to_csv(OUT_DIR / f"selected_pairs_top{top_n}.csv", index=False)

        # print t_bar_hat stats for selected pairs
        print(f"\n  {label}: asym ratio range "
              f"{selected['asym_ratio'].min():.2f}–{selected['asym_ratio'].max():.2f}")
        xi_ratios_ij = selected["xi_sym_ij"] / selected["xi_ij"]
        xi_ratios_ji = selected["xi_sym_ji"] / selected["xi_ji"]
        tbar_ij = xi_ratios_ij ** (-EXPONENT)
        tbar_ji = xi_ratios_ji ** (-EXPONENT)
        print(f"  t_bar_hat (ij): range {tbar_ij.min():.4f}–{tbar_ij.max():.4f}  "
              f"(slow→fast direction, costs fall)")
        print(f"  t_bar_hat (ji): range {tbar_ji.min():.4f}–{tbar_ji.max():.4f}  "
              f"(fast→slow direction, costs rise)")

        t_bar_hat = build_t_bar_hat(selected, n, raw_to_aa)
        result    = run_policy(label, t_bar_hat, l_r, l_f, l_bar, xi_ij, n)
        results[label] = result

        report(label, result, selected, raw_to_aa, mapping, lines)

        # save node-level output
        out_df = pd.DataFrame({
            "node_aa": np.arange(1, n+1),
            "grid_id": [mapping.set_index("node_i_aa_1based")["grid_id"].get(k, "?")
                        for k in range(1, n+1)],
            "chi_hat": result.chi_hat,
            "l_r_hat": result.l_r_hat,
            "l_f_hat": result.l_f_hat,
        })
        out_df.to_csv(OUT_DIR / f"chi_lr_lf_top{top_n}.csv", index=False)

    # compare the two policies
    lines.append(f"\n{'='*60}")
    lines.append("COMPARISON: Top-20 vs Top-50")
    r20 = results["Top-20"]; r50 = results["Top-50"]
    lines.append(f"  chi_hat top-20: {r20.chi_hat:.10f}")
    lines.append(f"  chi_hat top-50: {r50.chi_hat:.10f}")
    lines.append(f"  Incremental welfare gain from adding pairs 21–50: "
                 f"{(r50.chi_hat - r20.chi_hat)*1e6:.4f} × 10⁻⁶")
    lines.append(f"  l_r_hat corr (top-20 vs top-50): "
                 f"{np.corrcoef(r20.l_r_hat, r50.l_r_hat)[0,1]:.4f}")

    summary = "\n".join(lines)
    (OUT_DIR / "summary.txt").write_text(summary)
    print("\n" + summary)
    print(f"\nAll outputs in: {OUT_DIR}")


if __name__ == "__main__":
    main()
