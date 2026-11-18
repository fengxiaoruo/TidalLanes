"""
Score reciprocal grid-edge pairs for tidal-lane corridor candidacy.

Criteria (user-specified, ST vs LT horizons):
  1. Peak travel-time asymmetry (primary for ST)
  2. OD demand alignment (slow-direction vs fast-direction flow)
  3. Lane slack on the donor (fast) direction
  4. Composite ranks for short-run vs long-run policy screens

Does not replace model CF — produces a ranked candidate table for review.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

DATA_WORK = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(DATA_WORK))

from src.model.full_estimation import build_primitives as bp  # noqa: E402

EPS = 1e-12


def _percentile_rank(s: pd.Series) -> pd.Series:
    return s.rank(pct=True, method="average")


def build_candidate_table(
    prim: bp.Primitives,
    *,
    min_length_m: float = 200.0,
    delta_lanes: float = 1.0,
    min_fast_lanes: float = 2.0,
    min_asym_ratio: float = 1.05,
) -> pd.DataFrame:
    pair: dict[tuple[int, int], int] = {}
    for idx, (i, j) in enumerate(zip(prim.edge_i, prim.edge_j)):
        pair[(int(i), int(j))] = int(idx)

    rows: list[dict] = []
    seen: set[tuple[int, int]] = set()
    L = prim.L_ij_obs

    for idx, (i, j) in enumerate(zip(prim.edge_i, prim.edge_j)):
        i, j = int(i), int(j)
        rev = pair.get((j, i))
        if rev is None:
            continue
        key = tuple(sorted((idx, rev)))
        if key in seen:
            continue
        seen.add(key)
        a, b = key
        tau_a, tau_b = float(prim.tau_obs_min[a]), float(prim.tau_obs_min[b])
        if tau_a >= tau_b:
            slow, fast = a, b
        else:
            slow, fast = b, a
        len_m = min(prim.ell_kl_km[slow], prim.ell_kl_km[fast]) * 1000.0
        if len_m < min_length_m:
            continue
        asym = float(prim.tau_obs_min[slow] / max(prim.tau_obs_min[fast], EPS))
        if asym < min_asym_ratio:
            continue

        si, sj = int(prim.edge_i[slow]), int(prim.edge_j[slow])
        fi, fj = int(prim.edge_i[fast]), int(prim.edge_j[fast])
        od_slow = float(L[si, sj])
        od_fast = float(L[fi, fj])
        od_total = od_slow + od_fast
        od_align = od_slow / max(od_fast, EPS)

        n_slow = float(prim.n_kl[slow])
        n_fast = float(prim.n_kl[fast])
        lane_slack = n_fast - delta_lanes - 1.0
        if n_fast < min_fast_lanes or lane_slack < 0:
            continue

        c_slow = float(prim.tau_obs_min[slow] / prim.tau_ff_min[slow])
        c_fast = float(prim.tau_obs_min[fast] / prim.tau_ff_min[fast])

        rows.append({
            "grid_o_slow": prim.node_grid_ids[si],
            "grid_d_slow": prim.node_grid_ids[sj],
            "grid_o_fast": prim.node_grid_ids[fi],
            "grid_d_fast": prim.node_grid_ids[fj],
            "slow_edge_idx": slow,
            "fast_edge_idx": fast,
            "asym_ratio": asym,
            "tau_slow_min": float(prim.tau_obs_min[slow]),
            "tau_fast_min": float(prim.tau_obs_min[fast]),
            "c_slow": c_slow,
            "c_fast": c_fast,
            "len_m": len_m,
            "n_slow": n_slow,
            "n_fast": n_fast,
            "lane_slack_fast": lane_slack,
            "od_slow": od_slow,
            "od_fast": od_fast,
            "od_total": od_total,
            "od_align_slow_over_fast": od_align,
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["pct_asym"] = _percentile_rank(df["asym_ratio"])
    df["pct_od_align"] = _percentile_rank(df["od_align_slow_over_fast"])
    df["pct_od_total"] = _percentile_rank(df["od_total"])
    df["pct_lane_slack"] = _percentile_rank(df["lane_slack_fast"])
    df["pct_cong_slow"] = _percentile_rank(df["c_slow"])

    # ST: asymmetry + congestion on slow leg + lane slack (operational feasibility)
    df["score_st"] = (
        0.45 * df["pct_asym"]
        + 0.25 * df["pct_cong_slow"]
        + 0.20 * df["pct_lane_slack"]
        + 0.10 * df["pct_od_align"]
    )
    # LT: add total corridor demand (sorting / network value)
    df["score_lt"] = (
        0.30 * df["pct_asym"]
        + 0.25 * df["pct_od_total"]
        + 0.20 * df["pct_od_align"]
        + 0.15 * df["pct_lane_slack"]
        + 0.10 * df["pct_cong_slow"]
    )
    df = df.sort_values("score_st", ascending=False).reset_index(drop=True)
    df["rank_st"] = np.arange(1, len(df) + 1)
    df["rank_lt"] = df["score_lt"].rank(ascending=False, method="min").astype(int)
    return df


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--version-id", default="model_input_edgeproj_v2_lcc_gate5_v1")
    p.add_argument("--output-name", default="tidal_corridor_candidates_gate5_v1")
    p.add_argument("--top-k", type=int, default=200)
    p.add_argument("--delta-lanes", type=float, default=1.0)
    args = p.parse_args(argv)

    out = DATA_WORK / "outputs" / args.output_name
    out.mkdir(parents=True, exist_ok=True)

    prim = bp.load_primitives(DATA_WORK / "outputs" / args.version_id)
    df = build_candidate_table(prim, delta_lanes=args.delta_lanes)
    df.head(args.top_k).to_csv(out / "corridor_candidates_ranked.csv", index=False)

    overlap = 0
    if len(df) >= 100:
        top_st = set(df.nlargest(100, "score_st").index)
        top_lt = set(df.nlargest(100, "score_lt").index)
        overlap = len(top_st & top_lt)

    meta = {
        "n_candidates": int(len(df)),
        "top100_st_lt_overlap": overlap,
        "selection_weights": {
            "short_run": "0.45 asym + 0.25 c_slow + 0.20 lane_slack + 0.10 od_align",
            "long_run": "0.30 asym + 0.25 od_total + 0.20 od_align + 0.15 lane_slack + 0.10 c_slow",
        },
        "policy_factors_not_in_score_yet": [
            "bus/transit lane conflicts",
            "enforcement / reversal windows",
            "safety / accident history",
            "political feasibility / pilot corridor status",
        ],
    }
    (out / "summary.json").write_text(json.dumps(meta, indent=2))
    print(json.dumps(meta, indent=2))


if __name__ == "__main__":
    main()
