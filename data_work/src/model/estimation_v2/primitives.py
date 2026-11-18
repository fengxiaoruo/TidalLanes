"""
Step 1 — Primitives (data loading)
==================================

Functions that turn the model-ready QSM parquet files into clean, well-understood
in-memory objects. We build them one at a time:

    load_nodes  -> the grid-cell table, with a clean contiguous index   [this commit]
    load_edges  -> the directed road links                              (next)
    load_od     -> the bilateral commuting flows                        (next)
    build_primitives -> assemble everything into one object             (later)

Inputs live in:  data_work/outputs/<version_id>/data/qsm_input_*_<grid>.parquet
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd


def load_nodes(data_dir, grid_type: str = "square") -> pd.DataFrame:
    """Load the grid-cell (node) table and assign a clean contiguous index.

    The model indexes locations as i = 0, 1, ..., N-1. The file already carries an
    integer `node_i`, but to be safe (in case some cells are dropped) we re-derive a
    contiguous index ourselves and keep the original id for later joins.

    Parameters
    ----------
    data_dir : path to ``.../<version_id>/data``
    grid_type : "square" (default), "hex", or "voronoi"

    Returns
    -------
    pandas.DataFrame, one row per model node, indexed 0..N-1, with columns:
        node_i_orig : original integer id from the file (used to join edges/OD)
        grid_id     : human-readable cell id, e.g. "sq_10_16"
        residents   : observed residents  (kept for validation; the model's
                      marginals will come from OD row/col sums — decision (b))
        jobs        : observed jobs        (same)
    Only rows with keep_for_model == 1 are kept.
    """
    path = Path(data_dir) / f"qsm_input_nodes_{grid_type}.parquet"
    df = pd.read_parquet(
        path, columns=["node_i", "grid_id", "residents", "jobs", "keep_for_model"]
    )
    df = df[df["keep_for_model"] == 1].copy()        # keep only modelled cells
    df = df.sort_values("node_i").reset_index(drop=True)   # stable order
    df = df.rename(columns={"node_i": "node_i_orig"})
    df = df[["node_i_orig", "grid_id", "residents", "jobs"]]
    df.index.name = "idx"                            # contiguous 0..N-1
    return df
