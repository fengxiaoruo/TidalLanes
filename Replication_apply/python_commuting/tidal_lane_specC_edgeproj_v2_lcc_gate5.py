from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

import tidal_lane_specC as spec

ROOT = Path("/Users/fxr/Dropbox/Projects/TidalLanes")
REPL_ROOT = ROOT / "Replication_apply"

spec.SOURCE_DIR = ROOT / "data_work/outputs/model_input_edgeproj_v2_lcc_gate5_v1/data"
spec.V2_INPUT = REPL_ROOT / "aa_input_square_edgeproj_v2_lcc_gate5"
spec.MAPPING_CSV = (
    REPL_ROOT / "prepare_from_data_work_square_edgeproj_v2_lcc_gate5/node_index_mapping_square.csv"
)
spec.OUT_DIR = REPL_ROOT / "results/tidal_lane_edgeproj_v2_lcc_gate5"


def build_gate5_asymmetry_table() -> pd.DataFrame:
    """Build the Spec C selection set from the gated LCC bundle only."""
    edges = pd.read_parquet(spec.SOURCE_DIR / "qsm_input_edges_square.parquet")[
        [
            "grid_o",
            "grid_d",
            "tau_obs_min",
            "tau_ff_min",
            "total_link_len_m",
            "lane_est_path_raw_edgeproj_len_weighted",
            "lane_est_path_raw_edgeproj_bottleneck",
        ]
    ].copy()
    tau_obs = pd.to_numeric(edges["tau_obs_min"], errors="coerce")
    tau_ff = pd.to_numeric(edges["tau_ff_min"], errors="coerce")
    edges = edges[np.isfinite(tau_obs) & np.isfinite(tau_ff) & (tau_obs > 0) & (tau_ff > 0)].copy()
    edges = spec.attach_raw_edgeproj_lanes(edges)
    idx = edges.set_index(["grid_o", "grid_d"])

    rows, seen = [], set()
    for (go, gd), row in idx.iterrows():
        if (gd, go) not in idx.index:
            continue
        if (go, gd) in seen or (gd, go) in seen:
            continue
        rev = idx.loc[(gd, go)]
        len_ij = float(row["total_link_len_m"])
        len_ji = float(rev["total_link_len_m"])
        if min(len_ij, len_ji) <= spec.MIN_LEN_M:
            seen.add((go, gd))
            seen.add((gd, go))
            continue
        tau_ij = float(row["tau_obs_min"])
        tau_ji = float(rev["tau_obs_min"])
        tau_ff_ij = float(row["tau_ff_min"])
        tau_ff_ji = float(rev["tau_ff_min"])
        lanes_ij = float(row["lanes_resolved"])
        lanes_ji = float(rev["lanes_resolved"])
        xi_ij_v = lanes_ij * (max(tau_ij, tau_ff_ij) / tau_ff_ij) ** (1 / spec.DELTA1)
        xi_ji_v = lanes_ji * (max(tau_ji, tau_ff_ji) / tau_ff_ji) ** (1 / spec.DELTA1)
        rows.append(
            {
                "grid_o": go,
                "grid_d": gd,
                "i_raw": -1,
                "j_raw": -1,
                "tau_ij": tau_ij,
                "tau_ji": tau_ji,
                "tau_ff_ij": tau_ff_ij,
                "tau_ff_ji": tau_ff_ji,
                "lanes_ij": lanes_ij,
                "lanes_ji": lanes_ji,
                "lane_source_ij": row["lane_source"],
                "lane_source_ji": rev["lane_source"],
                "xi_ij": xi_ij_v,
                "xi_ji": xi_ji_v,
                "len_ij": len_ij,
                "len_ji": len_ji,
                "asym_ratio": max(tau_ij, tau_ji) / min(tau_ij, tau_ji),
            }
        )
        seen.add((go, gd))
        seen.add((gd, go))
    return pd.DataFrame(rows).sort_values("asym_ratio", ascending=False).reset_index(drop=True)


spec.build_asymmetry_table = build_gate5_asymmetry_table


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--source-version", default="model_input_edgeproj_v2_lcc_gate5_v1")
    ap.add_argument("--suffix", default="", help="AA input / mapping / results dir suffix; new suffixes refuse to overwrite")
    args = ap.parse_args()
    spec.SOURCE_DIR = ROOT / "data_work/outputs" / args.source_version / "data"
    if args.suffix:
        spec.V2_INPUT = Path(f"{spec.V2_INPUT}{args.suffix}")
        spec.MAPPING_CSV = Path(f"{spec.MAPPING_CSV.parent}{args.suffix}") / spec.MAPPING_CSV.name
        spec.OUT_DIR = Path(f"{spec.OUT_DIR}{args.suffix}")
        if spec.OUT_DIR.exists():
            raise SystemExit(f"{spec.OUT_DIR} exists; choose a new --suffix")
    spec.main()
