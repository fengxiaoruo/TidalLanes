"""
Step 1 — Construct data primitives
==================================

Target (TidalLanes_Model_v4.tex §6.1, Table 1 panel A–B):

    Directed-edge primitives
        ell_kl       directed grid length (km)
        v0_kl        free-flow speed (km/min)
        v_obs_kl     observed peak-period speed (km/min)
        n_kl         directed lane count
        tau_ff_kl    free-flow travel time (min) = ell / v0
        tau_obs_kl   observed travel time (min)  = ell / v_obs

    Population primitives
        L_ij_obs     bilateral commuting flow on the road mode
        L_R_obs[i]   = sum_j L_ij_obs
        L_F_obs[j]   = sum_i L_ij_obs
        Lbar         = sum_i L_R_obs[i]

Source files (default: square grid, AM, version_id="manual_centerline_rules_v11"):
    data_work/outputs/<version_id>/data/qsm_input_nodes_<grid>.parquet
    data_work/outputs/<version_id>/data/qsm_input_edges_<grid>.parquet
    data_work/outputs/<version_id>/data/qsm_input_od_<grid>.parquet
    data_work/outputs/<version_id>/data/qsm_input_parameters_<grid>.json

Open design choices (kept switchable, not locked):
    * grid_type ∈ {"square", "hex", "voronoi"}   (square is the default)
    * period only filters edges where the parquet `period` column matches.
    * grid travel-cost convention: this loader reads whatever the QSM
      stage exports; see `data_work/outputs/comparison_grid_methods/`.

No estimation happens here. We only convert the parquet schema into the
node-contiguous index space the solver uses.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd


EPS = 1e-12


@dataclass
class Primitives:
    """All variables labelled `P` in v4 Table 1, plus row/col remap."""

    # spatial layout
    version_id: str
    grid_type: str
    period: str
    node_grid_ids: np.ndarray           # shape (N,), str grid_id like sq_10_16
    node_keep_mask: np.ndarray          # shape (N,), bool

    # directed edges (M edges, indices in [0,N))
    edge_i: np.ndarray                  # shape (M,)
    edge_j: np.ndarray                  # shape (M,)
    ell_kl_km: np.ndarray               # directed length, km
    v0_kl: np.ndarray                   # free-flow speed, km/min
    v_obs_kl: np.ndarray                # observed peak speed, km/min
    tau_ff_min: np.ndarray              # free-flow time, minutes
    tau_obs_min: np.ndarray             # observed time, minutes
    n_kl: np.ndarray                    # directed lane count

    # population primitives
    L_R_obs: np.ndarray                 # residence marginal
    L_F_obs: np.ndarray                 # workplace marginal
    L_ij_obs: np.ndarray                # dense NxN OD flow (road mode)
    Lbar: float                         # total commuters

    # diagnostics / provenance
    meta: dict = field(default_factory=dict)

    @property
    def n_nodes(self) -> int:
        return len(self.node_grid_ids)

    @property
    def n_edges(self) -> int:
        return len(self.edge_i)


# --------------------------------------------------------------------------- #
# Loader
# --------------------------------------------------------------------------- #
def load_primitives(
    version_root: Path,
    grid_type: str = "square",
    period: str = "AM",
    od_pop_col: str = "commuters_road",
) -> Primitives:
    """Read the three QSM parquet files and return a :class:`Primitives`.

    Parameters
    ----------
    version_root :
        Path like ``data_work/outputs/manual_centerline_rules_v11``.
    grid_type :
        ``"square"`` (default), ``"hex"``, or ``"voronoi"``.
    period :
        Edge period filter (default ``"AM"``). The parquet stores all
        periods in one file with a ``period`` column.
    od_pop_col :
        Which OD-flow column to call ``L_ij_obs``. Default is the road
        commuter count; the meeting note keeps it open which mode to use
        as primary.

    Notes
    -----
    Node reindexing — the QSM parquet uses a non-contiguous ``node_i``
    column. We filter to ``keep_for_model == 1`` and remap to a dense
    range ``0..N-1`` consistent across edges, OD, and the matrix outputs.

    Missing ``tau_ff_min`` rows are flagged ``tau_ff_imputed_flag == 1``
    upstream and have been backfilled with ``tau_obs_min``; we trust the
    upstream choice and do not touch it here.
    """
    data_dir = Path(version_root) / "data"
    nodes = pd.read_parquet(data_dir / f"qsm_input_nodes_{grid_type}.parquet")
    edges = pd.read_parquet(data_dir / f"qsm_input_edges_{grid_type}.parquet")
    od = pd.read_parquet(data_dir / f"qsm_input_od_{grid_type}.parquet")

    # ------- nodes -------
    nodes = nodes.copy()
    nodes["node_i"] = pd.to_numeric(nodes["node_i"], errors="coerce").astype("Int64")
    nodes = nodes.dropna(subset=["node_i"]).copy()
    nodes["node_i"] = nodes["node_i"].astype(int)
    if "keep_for_model" in nodes.columns:
        nodes = nodes[nodes["keep_for_model"] == 1]
    nodes = nodes.sort_values("node_i").reset_index(drop=True)
    raw_to_new = {int(o): n for n, o in enumerate(nodes["node_i"].tolist())}
    N = len(nodes)
    node_grid_ids = nodes["grid_id"].astype(str).to_numpy()

    # ------- edges (filter to period; remap to contiguous index) -------
    e = edges.copy()
    if "period" in e.columns:
        e = e[e["period"].astype(str).str.upper() == period.upper()].copy()
    e["i"] = pd.to_numeric(e["i"], errors="coerce").astype("Int64")
    e["j"] = pd.to_numeric(e["j"], errors="coerce").astype("Int64")
    e = e.dropna(subset=["i", "j"]).copy()
    e["i"] = e["i"].astype(int); e["j"] = e["j"].astype(int)
    e = e[e["i"].isin(raw_to_new) & e["j"].isin(raw_to_new)].copy()
    e["i_new"] = e["i"].map(raw_to_new).astype(int)
    e["j_new"] = e["j"].map(raw_to_new).astype(int)

    tau_obs = pd.to_numeric(e["tau_obs_min"], errors="coerce")
    tau_ff = pd.to_numeric(e["tau_ff_min"], errors="coerce")
    keep_edge = np.isfinite(tau_obs) & (tau_obs > 0) & np.isfinite(tau_ff) & (tau_ff > 0)
    e = e[keep_edge].copy()
    tau_obs = tau_obs[keep_edge].to_numpy(dtype=float)
    tau_ff = tau_ff[keep_edge].to_numpy(dtype=float)
    length_m = pd.to_numeric(e["total_link_len_m"], errors="coerce").fillna(0.0).to_numpy(dtype=float)
    n_kl = pd.to_numeric(e["lanes_directional"], errors="coerce").to_numpy(dtype=float)
    # lanes_directional sometimes NaN; fall back to lane_est_length_weighted.
    if not np.all(np.isfinite(n_kl)):
        fb = pd.to_numeric(e.get("lane_est_length_weighted"), errors="coerce").to_numpy(dtype=float)
        n_kl = np.where(np.isfinite(n_kl) & (n_kl > 0), n_kl, fb)
    n_kl = np.where(np.isfinite(n_kl) & (n_kl > 0), n_kl, np.nanmean(n_kl))

    ell_km = length_m / 1000.0
    # km/min — keep speeds in same units as ell_km / tau
    v0_kl = np.where(tau_ff > 0, ell_km / tau_ff, np.nan)
    v_obs = np.where(tau_obs > 0, ell_km / tau_obs, np.nan)

    edge_i = e["i_new"].to_numpy(dtype=int)
    edge_j = e["j_new"].to_numpy(dtype=int)

    # ------- OD flows -> dense NxN -------
    o = od.copy()
    o_origin = pd.to_numeric(o.get("origin_node", o.get("home_i")), errors="coerce")
    o_dest = pd.to_numeric(o.get("destination_node", o.get("work_i")), errors="coerce")
    o_pop = pd.to_numeric(o.get(od_pop_col), errors="coerce").fillna(0.0)
    mask = o_origin.notna() & o_dest.notna()
    o_origin = o_origin[mask].astype(int)
    o_dest = o_dest[mask].astype(int)
    o_pop = o_pop[mask].to_numpy(dtype=float)
    in_grid = o_origin.isin(raw_to_new) & o_dest.isin(raw_to_new)
    o_origin = o_origin[in_grid].map(raw_to_new).astype(int).to_numpy()
    o_dest = o_dest[in_grid].map(raw_to_new).astype(int).to_numpy()
    o_pop = o_pop[np.asarray(in_grid)]

    L_ij = np.zeros((N, N), dtype=float)
    np.add.at(L_ij, (o_origin, o_dest), o_pop)
    L_R = L_ij.sum(axis=1)
    L_F = L_ij.sum(axis=0)
    Lbar = float(L_R.sum())

    meta = {
        "version_id": Path(version_root).name,
        "grid_type": grid_type,
        "period": period,
        "od_pop_col": od_pop_col,
        "n_nodes": int(N),
        "n_edges": int(len(edge_i)),
        "Lbar": Lbar,
        "lanes_fallback_used": bool((~np.isfinite(pd.to_numeric(e["lanes_directional"], errors="coerce").to_numpy())).any()),
    }
    # If a parameters json sits alongside, read its definition strings into meta.
    pj = data_dir / f"qsm_input_parameters_{grid_type}.json"
    if pj.exists():
        meta["qsm_parameters"] = json.loads(pj.read_text())

    return Primitives(
        version_id=meta["version_id"],
        grid_type=grid_type,
        period=period,
        node_grid_ids=node_grid_ids,
        node_keep_mask=np.ones(N, dtype=bool),
        edge_i=edge_i,
        edge_j=edge_j,
        ell_kl_km=ell_km,
        v0_kl=v0_kl,
        v_obs_kl=v_obs,
        tau_ff_min=tau_ff,
        tau_obs_min=tau_obs,
        n_kl=n_kl,
        L_R_obs=L_R,
        L_F_obs=L_F,
        L_ij_obs=L_ij,
        Lbar=Lbar,
        meta=meta,
    )


def write_primitives_summary(prim: Primitives, out_dir: Path) -> None:
    """Dump a small JSON summary + edge / node CSVs for reproducibility."""
    out_dir = Path(out_dir); out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "primitives_meta.json").write_text(json.dumps(prim.meta, indent=2, default=str))

    pd.DataFrame({
        "node_idx": np.arange(prim.n_nodes),
        "grid_id": prim.node_grid_ids,
        "L_R_obs": prim.L_R_obs,
        "L_F_obs": prim.L_F_obs,
    }).to_csv(out_dir / "primitives_nodes.csv", index=False)

    pd.DataFrame({
        "edge_i": prim.edge_i,
        "edge_j": prim.edge_j,
        "ell_kl_km": prim.ell_kl_km,
        "v0_kl": prim.v0_kl,
        "v_obs_kl": prim.v_obs_kl,
        "tau_ff_min": prim.tau_ff_min,
        "tau_obs_min": prim.tau_obs_min,
        "n_kl": prim.n_kl,
    }).to_csv(out_dir / "primitives_edges.csv", index=False)
