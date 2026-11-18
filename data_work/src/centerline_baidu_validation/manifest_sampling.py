"""Shared manifest filtering and stratified sampling for validation crawls."""

from __future__ import annotations

import pandas as pd


def filter_manifest(
    manifest: pd.DataFrame,
    *,
    only_inferred_speed: bool = False,
    require_waypoints: bool = False,
    min_length_m: float | None = None,
    max_length_m: float | None = None,
) -> pd.DataFrame:
    df = manifest.copy()
    df["length_m"] = pd.to_numeric(df["length_m"], errors="coerce")
    df["n_waypoints"] = pd.to_numeric(df["n_waypoints"], errors="coerce").fillna(0)
    df = df[df["length_m"].gt(0)].copy()
    if only_inferred_speed:
        df = df[df["has_inferred_speed"].astype(bool)].copy()
    if require_waypoints:
        df = df[df["n_waypoints"].gt(0)].copy()
    if min_length_m is not None:
        df = df[df["length_m"].ge(float(min_length_m))].copy()
    if max_length_m is not None:
        df = df[df["length_m"].le(float(max_length_m))].copy()
    return df.reset_index(drop=True)


def pick_stratified_sample(
    manifest: pd.DataFrame,
    n: int,
    *,
    seed: int = 42,
    length_min: float = 300.0,
    length_max: float = 5000.0,
) -> pd.DataFrame:
    df = filter_manifest(manifest, min_length_m=length_min, max_length_m=length_max)
    if len(df) <= n:
        return df.reset_index(drop=True)

    df["length_bin"] = pd.qcut(df["length_m"], q=min(4, len(df)), duplicates="drop")
    picks = []
    per_bin = max(1, n // df["length_bin"].nunique())
    for _, sub in df.groupby("length_bin", observed=True):
        sub = sub.sample(frac=1.0, random_state=seed).sort_values(
            ["has_inferred_speed", "n_waypoints"], ascending=[False, False]
        )
        picks.append(sub.head(per_bin))
    out = pd.concat(picks, ignore_index=True).drop_duplicates(subset=["request_id"])
    if len(out) < n:
        extra = df[~df["request_id"].isin(out["request_id"])].sample(frac=1.0, random_state=seed)
        out = pd.concat([out, extra.head(n - len(out))], ignore_index=True)
    return out.head(n).reset_index(drop=True)
