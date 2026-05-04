from __future__ import annotations

import argparse
import json
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_VERSION = "match_projection_complete_v7_latest"
DEFAULT_OUTPUT_DIR = ROOT / "outputs"


def parse_args():
    parser = argparse.ArgumentParser(description="Compare prototype main-network filters C+A vs C+B.")
    parser.add_argument("--version-id", default=DEFAULT_VERSION)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    return parser.parse_args()


def normalize_roadname(value) -> str:
    if pd.isna(value):
        return "__unnamed__"
    text = str(value).strip()
    return text if text else "__unnamed__"


def endpoint_key(x: float, y: float, precision: int = 1) -> tuple[float, float]:
    return (round(float(x), precision), round(float(y), precision))


def build_union_find(n: int):
    parent = list(range(n))
    size = [1] * n

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: int, b: int) -> None:
        ra = find(a)
        rb = find(b)
        if ra == rb:
            return
        if size[ra] < size[rb]:
            ra, rb = rb, ra
        parent[rb] = ra
        size[ra] += size[rb]

    return parent, find, union


def load_inputs(version_root: Path):
    data_dir = version_root / "data"
    raw = gpd.read_parquet(data_dir / "raw_segment_master.parquet")
    match = pd.read_parquet(data_dir / "raw_to_centerline_match_master.parquet")
    cl = gpd.read_parquet(data_dir / "centerline_master.parquet")
    return raw, match, cl


def prepare_segment_table(raw: gpd.GeoDataFrame, match: pd.DataFrame, cl: gpd.GeoDataFrame) -> pd.DataFrame:
    raw = raw.copy()
    raw = raw[raw.geometry.notna() & (~raw.geometry.is_empty)].copy()
    raw["seg_idx"] = np.arange(len(raw), dtype=int)
    coords = raw.geometry.apply(lambda g: list(g.coords))
    raw["start_key"] = [endpoint_key(c[0][0], c[0][1]) for c in coords]
    raw["end_key"] = [endpoint_key(c[-1][0], c[-1][1]) for c in coords]
    raw["roadname_norm"] = raw["roadname"].map(normalize_roadname)
    raw["roadtype_num"] = pd.to_numeric(raw["roadtype"], errors="coerce")
    raw["length_m"] = pd.to_numeric(raw["length_m"], errors="coerce")

    endpoint_counts = (
        pd.concat(
            [
                raw[["start_key"]].rename(columns={"start_key": "node_key"}),
                raw[["end_key"]].rename(columns={"end_key": "node_key"}),
            ],
            ignore_index=True,
        )
        .groupby("node_key")
        .size()
        .rename("raw_degree")
        .reset_index()
    )
    degree_map = dict(zip(endpoint_counts["node_key"], endpoint_counts["raw_degree"]))
    raw["deg_start"] = raw["start_key"].map(degree_map).astype(int)
    raw["deg_end"] = raw["end_key"].map(degree_map).astype(int)
    raw["is_leaf_like"] = (raw["deg_start"] == 1) | (raw["deg_end"] == 1)
    raw["is_internal_like"] = (raw["deg_start"] > 1) & (raw["deg_end"] > 1)

    # Raw connected components based on shared endpoints.
    parent, find, union = build_union_find(len(raw))
    endpoint_to_segments: dict[tuple[float, float], list[int]] = {}
    for row in raw[["seg_idx", "start_key", "end_key"]].itertuples(index=False):
        endpoint_to_segments.setdefault(row.start_key, []).append(int(row.seg_idx))
        endpoint_to_segments.setdefault(row.end_key, []).append(int(row.seg_idx))
    for segs in endpoint_to_segments.values():
        if len(segs) <= 1:
            continue
        base = segs[0]
        for other in segs[1:]:
            union(base, other)
    raw["raw_component_id"] = [find(int(i)) for i in raw["seg_idx"]]

    comp_stats = (
        raw.groupby("raw_component_id", as_index=False)
        .agg(
            component_total_len_m=("length_m", "sum"),
            component_n_segments=("seg_idx", "size"),
            component_n_roadseg=("roadseg_id", "nunique"),
            component_n_named=("roadname_norm", lambda s: int((s != "__unnamed__").sum())),
        )
    )
    raw = raw.merge(comp_stats, on="raw_component_id", how="left")

    raw["bundle_key"] = (
        raw["raw_component_id"].astype(str)
        + "|"
        + raw["roadtype_num"].fillna(-1).astype(int).astype(str)
        + "|"
        + raw["roadname_norm"]
    )
    bundle_stats = (
        raw.groupby("bundle_key", as_index=False)
        .agg(
            bundle_total_len_m=("length_m", "sum"),
            bundle_n_segments=("seg_idx", "size"),
            bundle_n_raw_edges=("raw_edge_id", "nunique"),
            bundle_component_id=("raw_component_id", "first"),
            bundle_named=("roadname_norm", "first"),
            bundle_roadtype=("roadtype_num", "first"),
        )
    )
    raw = raw.merge(bundle_stats, on="bundle_key", how="left")

    cl_meta = cl[["cline_id", "length_m", "keep_baseline", "is_short_centerline"]].rename(
        columns={
            "length_m": "cline_length_m",
            "keep_baseline": "cline_keep_baseline",
        }
    )
    df = raw.merge(
        match[
            [
                "split_id",
                "matched_final",
                "match_method_final",
                "cline_id_final",
                "skel_dir_final",
                "dist_mean_final",
                "angle_diff_final",
            ]
        ],
        on="split_id",
        how="left",
    )
    df = df.merge(cl_meta, left_on="cline_id_final", right_on="cline_id", how="left")

    matched_support = (
        df[df["matched_final"] == 1]
        .groupby("cline_id_final", as_index=False)
        .agg(
            cline_support_len_m=("length_m", "sum"),
            cline_support_n_segments=("split_id", "size"),
        )
    )
    df = df.merge(matched_support, on="cline_id_final", how="left")
    return df


def add_filter_flags(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    major = out["roadtype_num"].isin([2, 3, 4])
    named = out["roadname_norm"] != "__unnamed__"
    very_short = out["length_m"] < 80.0
    short = out["length_m"] < 120.0
    local_stub = short & out["is_leaf_like"]
    tiny_component = (out["component_total_len_m"] < 250.0) & (out["component_n_segments"] <= 3)
    small_bundle = out["bundle_total_len_m"] < 500.0

    # C+A prototype: keep all data, but flag main-corridor candidates using raw bundle support.
    roadtype2_keep = out["roadtype_num"].eq(2)
    roadtype3_keep = out["roadtype_num"].eq(3) & (
        (out["bundle_total_len_m"] >= 900.0)
        | (out["bundle_n_segments"] >= 3)
        | (out["component_total_len_m"] >= 1400.0)
    )
    roadtype4_keep = out["roadtype_num"].eq(4) & (
        ((named) & (out["bundle_total_len_m"] >= 1200.0))
        | ((~named) & (out["component_total_len_m"] >= 1800.0))
        | (out["bundle_n_segments"] >= 5)
    )
    preserve_internal_frag = major & very_short & out["is_internal_like"] & (out["bundle_total_len_m"] >= 1500.0)
    ca_keep = (roadtype2_keep | roadtype3_keep | roadtype4_keep | preserve_internal_frag) & ~(local_stub & small_bundle) & ~(
        tiny_component & out["is_leaf_like"]
    )

    # C+B prototype: keep all data, but flag segments supported by usable baseline centerlines.
    out["is_nuisance_proxy"] = (very_short & out["is_leaf_like"]) | (tiny_component & short)
    out["is_internal_small_proxy"] = very_short & out["is_internal_like"] & (out["matched_final"] == 1)
    out["is_long_unmatched_proxy"] = (out["matched_final"] == 0) & (out["length_m"] >= 800.0)
    cb_keep = (
        (out["matched_final"] == 1)
        & out["cline_keep_baseline"].fillna(False)
        & (out["cline_length_m"].fillna(0) >= 100.0)
        & (out["cline_support_len_m"].fillna(0) >= 200.0)
    )

    # Safety net v1: broad rescue for matched segments supporting usable
    # centerlines, except clear nuisance stubs.
    cb_safe = cb_keep & (~out["is_nuisance_proxy"] | out["is_internal_small_proxy"])
    ca_safe_keep = ca_keep | cb_safe

    # Safety net v2: narrower rescue focused on
    # 1) internal short pieces on otherwise strong corridors
    # 2) very short matched pieces on high-support major-road bundles
    # This is meant to preserve genuine main-road fragments without restoring
    # nearly the full baseline network.
    strong_centerline = (
        cb_keep
        & (out["cline_support_len_m"].fillna(0) >= 600.0)
        & (out["cline_length_m"].fillna(0) >= 180.0)
    )
    strong_bundle_major = (
        out["roadtype_num"].isin([2, 3])
        & (out["bundle_total_len_m"].fillna(0) >= 2200.0)
        & (out["component_total_len_m"].fillna(0) >= 4000.0)
    )
    strong_bundle_secondary = (
        out["roadtype_num"].eq(4)
        & (out["roadname_norm"] != "__unnamed__")
        & (out["bundle_total_len_m"].fillna(0) >= 1800.0)
        & (out["component_total_len_m"].fillna(0) >= 3200.0)
    )
    short_matched_piece = out["length_m"].fillna(np.inf) < 95.0
    ca_safe_v2_keep = ca_keep | (
        strong_centerline
        & (
            out["is_internal_small_proxy"]
            | (short_matched_piece & (strong_bundle_major | strong_bundle_secondary))
        )
        & (~out["is_nuisance_proxy"] | out["is_internal_small_proxy"])
    )

    out["keep_proto_ca"] = ca_keep.fillna(False)
    out["keep_proto_cb"] = cb_keep.fillna(False)
    out["keep_proto_ca_safe"] = ca_safe_keep.fillna(False)
    out["keep_proto_ca_safe_v2"] = ca_safe_v2_keep.fillna(False)
    out["keep_existing_baseline"] = out["keep_baseline"].fillna(False)
    return out


def summarize_filter(df: pd.DataFrame, flag: str, label: str) -> dict[str, float]:
    keep = df[flag].fillna(False)
    matched = df["matched_final"] == 1
    nuisance = df["is_nuisance_proxy"]
    internal_small = df["is_internal_small_proxy"]
    long_unmatched = df["is_long_unmatched_proxy"]
    major = df["roadtype_num"].isin([2, 3, 4])

    def share(mask):
        mask = mask.fillna(False) if isinstance(mask, pd.Series) else mask
        denom = int(mask.sum())
        if denom == 0:
            return np.nan
        return float((keep & mask).sum() / denom)

    kept_len = float(df.loc[keep, "length_m"].sum())
    total_len = float(df["length_m"].sum())
    return {
        "filter_label": label,
        "segments_kept": int(keep.sum()),
        "segments_total": int(len(df)),
        "segment_keep_share": float(keep.mean()),
        "length_kept_km": kept_len / 1000.0,
        "length_total_km": total_len / 1000.0,
        "length_keep_share": kept_len / total_len if total_len > 0 else np.nan,
        "major_length_keep_share": float(
            df.loc[keep & major, "length_m"].sum() / df.loc[major, "length_m"].sum()
        )
        if float(df.loc[major, "length_m"].sum()) > 0
        else np.nan,
        "matched_segment_keep_share": share(matched),
        "internal_small_keep_share": share(internal_small),
        "nuisance_keep_share": share(nuisance),
        "long_unmatched_keep_share": share(long_unmatched),
        "projection_matched_keep_share": share(df["match_method_final"].eq("projection_fallback")),
    }


def build_examples(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "split_id",
        "raw_edge_id",
        "roadseg_id",
        "roadname",
        "roadtype_num",
        "length_m",
        "matched_final",
        "match_method_final",
        "cline_id_final",
        "bundle_total_len_m",
        "component_total_len_m",
        "deg_start",
        "deg_end",
        "is_nuisance_proxy",
        "is_internal_small_proxy",
        "is_long_unmatched_proxy",
        "keep_existing_baseline",
        "keep_proto_ca",
        "keep_proto_cb",
    ]
    return df[cols].copy()


def main():
    args = parse_args()
    version_root = Path(args.output_dir) / args.version_id
    out_dir = version_root / "metrics" / "main_network_filter_compare"
    out_dir.mkdir(parents=True, exist_ok=True)

    raw, match, cl = load_inputs(version_root)
    df = prepare_segment_table(raw, match, cl)
    df = add_filter_flags(df)

    summary = pd.DataFrame(
        [
            summarize_filter(df, "keep_existing_baseline", "existing_keep_baseline"),
            summarize_filter(df, "keep_proto_cb", "proto_C_plus_B"),
            summarize_filter(df, "keep_proto_ca", "proto_C_plus_A"),
            summarize_filter(df, "keep_proto_ca_safe", "proto_C_plus_A_safe"),
            summarize_filter(df, "keep_proto_ca_safe_v2", "proto_C_plus_A_safe_v2"),
        ]
    )
    summary.to_csv(out_dir / "filter_summary.csv", index=False)

    examples = build_examples(df)
    examples.to_csv(out_dir / "segment_flags_examples.csv", index=False)

    # Targeted subsets for quick review.
    examples.loc[examples["is_internal_small_proxy"]].sort_values("length_m").to_csv(
        out_dir / "internal_small_segments.csv",
        index=False,
    )
    examples.loc[examples["is_nuisance_proxy"]].sort_values("length_m").to_csv(
        out_dir / "nuisance_proxy_segments.csv",
        index=False,
    )
    examples.loc[examples["is_long_unmatched_proxy"]].sort_values("length_m", ascending=False).to_csv(
        out_dir / "long_unmatched_segments.csv",
        index=False,
    )

    payload = {
        "version_id": args.version_id,
        "note": "Prototype comparison only. C+A is raw-bundle support; C+B is centerline-support.",
        "files": {
            "summary": str(out_dir / "filter_summary.csv"),
            "segment_flags": str(out_dir / "segment_flags_examples.csv"),
        },
    }
    (out_dir / "run_info.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
