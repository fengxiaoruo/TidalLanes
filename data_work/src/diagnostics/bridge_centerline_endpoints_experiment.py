"""
Endpoint-bridge experiment on an existing centerline master.

This does NOT rebuild centerlines from raw_connected. It copies an existing
centerline version, inserts short straight connectors between nearby endpoints
of different weak components, and writes a separate output version.

Default source: outputs/manual_centerline_rules_v11
Default output: outputs/centerline_endpoint_bridge35_v1
"""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path

import geopandas as gpd
import networkx as nx
import numpy as np
import pandas as pd
from shapely.geometry import LineString, MultiLineString, Point


def parse_args():
    p = argparse.ArgumentParser(description="Bridge near gaps on an existing centerline network.")
    p.add_argument(
        "--source-root",
        default="outputs/manual_centerline_rules_v11",
        help="Existing centerline version root (read-only).",
    )
    p.add_argument(
        "--output-root",
        default="outputs/centerline_endpoint_bridge35_v1",
        help="New version root (will not overwrite source).",
    )
    p.add_argument("--min-gap-m", type=float, default=35.0, help="Minimum endpoint gap to bridge.")
    p.add_argument("--max-gap-m", type=float, default=50.0, help="Maximum endpoint gap to bridge.")
    p.add_argument(
        "--component-snap-m",
        type=float,
        default=35.0,
        help="Snap used to define pre-bridge weak components.",
    )
    p.add_argument(
        "--only-to-lcc",
        action="store_true",
        default=True,
        help="Only bridge non-LCC components to the LCC (default).",
    )
    p.add_argument("--no-only-to-lcc", action="store_false", dest="only_to_lcc")
    p.add_argument(
        "--one-bridge-per-component",
        action="store_true",
        default=True,
        help="Keep only the shortest bridge per non-LCC component (default).",
    )
    p.add_argument("--no-one-bridge-per-component", action="store_false", dest="one_bridge_per_component")
    return p.parse_args()


def endpoints(geom):
    if geom is None or geom.is_empty:
        return []
    if isinstance(geom, LineString):
        lines = [geom]
    elif isinstance(geom, MultiLineString):
        lines = list(geom.geoms)
    else:
        return []
    pts = []
    for ln in lines:
        coords = list(ln.coords)
        if len(coords) >= 2:
            pts.append(coords[0])
            pts.append(coords[-1])
    return pts


def cluster_points(pts, eps: float):
    n = len(pts)
    parent = list(range(n))

    def find(a):
        while parent[a] != a:
            parent[a] = parent[parent[a]]
            a = parent[a]
        return a

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    arr = np.asarray(pts, dtype=float)
    if n == 0:
        return np.array([], dtype=int), 0, arr

    cell = max(float(eps), 1e-9)
    buckets: dict[tuple[int, int], list[int]] = defaultdict(list)
    for i, (x, y) in enumerate(arr):
        buckets[(int(np.floor(x / cell)), int(np.floor(y / cell)))].append(i)

    for (ix, iy), ids in buckets.items():
        cand = []
        for dx in (-1, 0, 1):
            for dy in (-1, 0, 1):
                cand.extend(buckets.get((ix + dx, iy + dy), []))
        for a in ids:
            xa, ya = arr[a]
            for b in cand:
                if b <= a:
                    continue
                xb, yb = arr[b]
                if (xa - xb) ** 2 + (ya - yb) ** 2 <= eps * eps:
                    union(a, b)

    roots = {}
    labels = np.empty(n, dtype=int)
    nid = 0
    for i in range(n):
        r = find(i)
        if r not in roots:
            roots[r] = nid
            nid += 1
        labels[i] = roots[r]
    return labels, nid, arr


def build_component_labels(gdf: gpd.GeoDataFrame, snap_m: float) -> tuple[np.ndarray, list[set[int]]]:
    ep_xy = []
    ep_owner = []
    for i, geom in enumerate(gdf.geometry.values):
        for p in endpoints(geom):
            ep_xy.append(p)
            ep_owner.append(i)
    labels, n_nodes, _ = cluster_points(ep_xy, snap_m)
    owner_to_clusters = defaultdict(set)
    for ep_i, owner in enumerate(ep_owner):
        owner_to_clusters[owner].add(int(labels[ep_i]))

    G = nx.Graph()
    G.add_nodes_from(range(n_nodes))
    for clusters in owner_to_clusters.values():
        cs = list(clusters)
        for a in range(len(cs)):
            for b in range(a + 1, len(cs)):
                G.add_edge(cs[a], cs[b])

    comps = sorted(nx.connected_components(G), key=len, reverse=True)
    cluster_comp = {n: i for i, c in enumerate(comps) for n in c}
    cl_comp = np.empty(len(gdf), dtype=int)
    for owner in range(len(gdf)):
        cids = [cluster_comp[c] for c in owner_to_clusters[owner]]
        cl_comp[owner] = Counter(cids).most_common(1)[0][0]
    return cl_comp, comps


def connectivity_stats(gdf: gpd.GeoDataFrame, snap_m: float) -> dict:
    if gdf.empty:
        return {"n_cl": 0, "n_weak_comp": 0, "lcc_n": 0, "lcc_share": 0.0, "lcc_len_share": 0.0}
    cl_comp, _ = build_component_labels(gdf, snap_m)
    tmp = gdf.copy()
    tmp["_comp"] = cl_comp
    sizes = tmp["_comp"].value_counts()
    lcc = int(sizes.index[0])
    lcc_n = int(sizes.iloc[0])
    lcc_len = float(tmp.loc[tmp["_comp"] == lcc, "length_m"].sum())
    total_len = float(tmp["length_m"].sum())
    return {
        "n_cl": int(len(tmp)),
        "n_weak_comp": int(sizes.shape[0]),
        "lcc_n": lcc_n,
        "lcc_share": lcc_n / len(tmp),
        "lcc_len_share": lcc_len / total_len if total_len > 0 else 0.0,
        "top5_comp_sizes": [int(x) for x in sizes.head(5).tolist()],
    }


def to_lines(geom):
    if geom is None or geom.is_empty:
        return []
    if geom.geom_type == "LineString":
        return [geom]
    if geom.geom_type == "MultiLineString":
        return [g for g in geom.geoms if g is not None and not g.is_empty]
    return []


def segment_to_grid_sequence(line_geom, grid_sindex, grid_sub):
    cand_idx = list(grid_sindex.intersection(line_geom.bounds))
    if not cand_idx:
        return []
    pieces = []
    for row in grid_sub.iloc[cand_idx].itertuples(index=False):
        if not line_geom.intersects(row.geometry):
            continue
        inter = line_geom.intersection(row.geometry)
        if inter.is_empty:
            continue
        for ls in to_lines(inter):
            length = float(ls.length)
            if length <= 0:
                continue
            try:
                mid = ls.interpolate(0.5, normalized=True)
            except Exception:
                mid = ls.centroid
            s = float(line_geom.project(mid))
            pieces.append((str(row.grid_id), length, s))
    if not pieces:
        return []
    pieces.sort(key=lambda x: x[2])
    seq = []
    for gid, length, _ in pieces:
        if (not seq) or (seq[-1][0] != gid):
            seq.append([gid, length])
        else:
            seq[-1][1] += length
    return [(gid, float(length)) for gid, length in seq]


def directed_pairs_from_centerlines(gdf: gpd.GeoDataFrame, grid: gpd.GeoDataFrame) -> set[tuple[str, str]]:
    grid_sub = grid[["grid_id", "geometry"]].copy()
    grid_sub["grid_id"] = grid_sub["grid_id"].astype(str)
    sindex = grid_sub.sindex
    pairs = set()
    for geom in gdf.geometry.values:
        for line in to_lines(geom):
            if line.length <= 0:
                continue
            # both orientations, matching stage06 directed usage
            for oriented in (line, LineString(list(line.coords)[::-1])):
                seq = segment_to_grid_sequence(oriented, sindex, grid_sub)
                for k in range(len(seq) - 1):
                    go, gd = seq[k][0], seq[k + 1][0]
                    if go != gd:
                        pairs.add((go, gd))
    return pairs


def rook_pairs(grid: gpd.GeoDataFrame) -> set[tuple[str, str]]:
    grid = grid.copy()
    grid["grid_id"] = grid["grid_id"].astype(str)
    sindex = grid.sindex
    undirected = set()
    geoms = grid.geometry.values
    ids = grid["grid_id"].tolist()
    for i, geom in enumerate(geoms):
        cand = list(sindex.intersection(geom.bounds))
        for j in cand:
            if j <= i:
                continue
            inter = geom.boundary.intersection(geoms[j].boundary)
            if inter.is_empty:
                continue
            if getattr(inter, "length", 0.0) > 1.0:
                a, b = ids[i], ids[j]
                undirected.add(tuple(sorted((a, b))))
    return undirected


def summarize_rook_coverage(undirected_rook, directed_pairs) -> dict:
    both = one = none = 0
    for a, b in undirected_rook:
        ab = (a, b) in directed_pairs
        ba = (b, a) in directed_pairs
        if ab and ba:
            both += 1
        elif ab or ba:
            one += 1
        else:
            none += 1
    n = len(undirected_rook)
    return {
        "rook_undirected_pairs": n,
        "both_dir": both,
        "one_dir": one,
        "none": none,
        "none_share": none / n if n else 0.0,
        "any_dir_share": (both + one) / n if n else 0.0,
        "directed_pairs": len(directed_pairs),
    }


def choose_bridges(
    baseline: gpd.GeoDataFrame,
    component_snap_m: float,
    min_gap_m: float,
    max_gap_m: float,
    only_to_lcc: bool,
    one_bridge_per_component: bool,
) -> pd.DataFrame:
    cl_comp, _ = build_component_labels(baseline, component_snap_m)
    baseline = baseline.copy()
    baseline["comp_id"] = cl_comp

    # endpoint table
    rows = []
    geoms = baseline.geometry.values
    cline_ids = baseline["cline_id"].astype(int).to_numpy()
    comps = baseline["comp_id"].astype(int).to_numpy()
    for i, geom in enumerate(geoms):
        for end_idx, xy in enumerate(endpoints(geom)):
            rows.append(
                {
                    "cl_pos": i,
                    "cline_id": int(cline_ids[i]),
                    "comp": int(comps[i]),
                    "end_idx": end_idx,
                    "x": float(xy[0]),
                    "y": float(xy[1]),
                }
            )
    ep = pd.DataFrame(rows)
    if ep.empty:
        return pd.DataFrame()

    arr = ep[["x", "y"]].to_numpy(dtype=float)
    # block by coarse cells of max_gap
    cell = max_gap_m
    buckets = defaultdict(list)
    for i, (x, y) in enumerate(arr):
        buckets[(int(np.floor(x / cell)), int(np.floor(y / cell)))].append(i)

    candidates = []
    for (ix, iy), ids in buckets.items():
        cand = []
        for dx in (-1, 0, 1):
            for dy in (-1, 0, 1):
                cand.extend(buckets.get((ix + dx, iy + dy), []))
        for a in ids:
            xa, ya = arr[a]
            ca = int(ep.iloc[a]["comp"])
            cla = int(ep.iloc[a]["cline_id"])
            for b in cand:
                if b <= a:
                    continue
                cb = int(ep.iloc[b]["comp"])
                clb = int(ep.iloc[b]["cline_id"])
                if ca == cb or cla == clb:
                    continue
                xb, yb = arr[b]
                dist = float(np.hypot(xa - xb, ya - yb))
                if dist < min_gap_m or dist > max_gap_m:
                    continue
                if only_to_lcc and (ca != 0 and cb != 0):
                    continue
                # orient: non-LCC endpoint first when possible
                if ca == 0 and cb != 0:
                    a2, b2 = b, a
                    ca2, cb2 = cb, ca
                else:
                    a2, b2 = a, b
                    ca2, cb2 = ca, cb
                candidates.append(
                    {
                        "from_comp": ca2,
                        "to_comp": cb2,
                        "from_cline_id": int(ep.iloc[a2]["cline_id"]),
                        "to_cline_id": int(ep.iloc[b2]["cline_id"]),
                        "from_end_idx": int(ep.iloc[a2]["end_idx"]),
                        "to_end_idx": int(ep.iloc[b2]["end_idx"]),
                        "x0": float(ep.iloc[a2]["x"]),
                        "y0": float(ep.iloc[a2]["y"]),
                        "x1": float(ep.iloc[b2]["x"]),
                        "y1": float(ep.iloc[b2]["y"]),
                        "length_m": dist,
                    }
                )

    cand_df = pd.DataFrame(candidates)
    if cand_df.empty:
        return cand_df

    cand_df = cand_df.sort_values(["from_comp", "length_m", "to_comp"]).reset_index(drop=True)
    if one_bridge_per_component:
        # For each non-LCC component, keep the shortest bridge to LCC.
        keep_idx = []
        seen = set()
        for i, row in cand_df.iterrows():
            key = int(row.from_comp) if int(row.from_comp) != 0 else int(row.to_comp)
            if key == 0 or key in seen:
                continue
            seen.add(key)
            keep_idx.append(i)
        cand_df = cand_df.loc[keep_idx].copy()

    cand_df = cand_df.sort_values("length_m").reset_index(drop=True)
    cand_df.insert(0, "bridge_id", [f"BRIDGE_{i:04d}" for i in range(1, len(cand_df) + 1)])
    return cand_df


def bridges_to_gdf(bridges: pd.DataFrame, crs) -> gpd.GeoDataFrame:
    if bridges.empty:
        return gpd.GeoDataFrame(
            columns=[
                "bridge_id",
                "geometry",
                "build_source",
                "pre_cline_key",
                "manual_group_id",
                "manual_group_name",
                "manual_component_id",
                "length_m",
            ],
            geometry="geometry",
            crs=crs,
        )
    rows = []
    for row in bridges.itertuples(index=False):
        geom = LineString([(row.x0, row.y0), (row.x1, row.y1)])
        rows.append(
            {
                "bridge_id": row.bridge_id,
                "geometry": geom,
                "build_source": "manual_bridge",
                "pre_cline_key": f"bridge__{row.bridge_id}",
                "manual_group_id": pd.NA,
                "manual_group_name": pd.NA,
                "manual_component_id": pd.NA,
                "length_m": float(row.length_m),
                "from_comp": int(row.from_comp),
                "to_comp": int(row.to_comp),
                "from_cline_id": int(row.from_cline_id),
                "to_cline_id": int(row.to_cline_id),
            }
        )
    return gpd.GeoDataFrame(rows, geometry="geometry", crs=crs)


def finalize_with_bridges(base: gpd.GeoDataFrame, bridge_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    cols = [
        "geometry",
        "build_source",
        "pre_cline_key",
        "manual_group_id",
        "manual_group_name",
        "manual_component_id",
    ]
    base_keep = base[cols].copy()
    if bridge_gdf.empty:
        out = base_keep.copy()
    else:
        add = bridge_gdf[cols].copy()
        out = pd.concat([base_keep, add], ignore_index=True)
        out = gpd.GeoDataFrame(out, geometry="geometry", crs=base.crs)

    out = out.reset_index(drop=True)
    out["is_linestring"] = out.geometry.type == "LineString"
    out["length_m"] = out.geometry.length.astype(float)
    out["cline_id"] = np.arange(len(out), dtype=int)
    out["is_short_centerline"] = out["length_m"] < 50.0
    out["is_valid_geometry"] = out.geometry.notna() & (~out.geometry.is_empty)
    # Force bridges into baseline even if shorter than MIN_CL_LEN=50.
    out["keep_baseline"] = (
        out["is_valid_geometry"]
        & out["is_linestring"]
        & ((~out["is_short_centerline"]) | (out["build_source"] == "manual_bridge"))
    )
    out["keep_relaxed"] = out["is_valid_geometry"] & out["is_linestring"]
    out["keep_qsm"] = out["keep_baseline"]
    out["source_version"] = "centerline_endpoint_bridge_experiment"
    return out


def build_directed_centerline(centerline: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    rows = []
    for row in centerline.itertuples(index=False):
        ls = row.geometry
        if ls is None or ls.is_empty or ls.geom_type != "LineString":
            continue
        rows.append(
            {
                "cline_id": int(row.cline_id),
                "dir": "AB",
                "geometry": ls,
                "length_m": float(row.length_m),
                "source_version": row.source_version,
                "build_source": getattr(row, "build_source", pd.NA),
                "manual_group_id": getattr(row, "manual_group_id", pd.NA),
                "keep_baseline": bool(row.keep_baseline),
                "keep_relaxed": bool(row.keep_relaxed),
                "keep_qsm": bool(row.keep_qsm),
                "is_valid_geometry": bool(row.is_valid_geometry),
                "is_short_centerline": bool(row.is_short_centerline),
            }
        )
        rows.append(
            {
                "cline_id": int(row.cline_id),
                "dir": "BA",
                "geometry": LineString(list(ls.coords)[::-1]),
                "length_m": float(row.length_m),
                "source_version": row.source_version,
                "build_source": getattr(row, "build_source", pd.NA),
                "manual_group_id": getattr(row, "manual_group_id", pd.NA),
                "keep_baseline": bool(row.keep_baseline),
                "keep_relaxed": bool(row.keep_relaxed),
                "keep_qsm": bool(row.keep_qsm),
                "is_valid_geometry": bool(row.is_valid_geometry),
                "is_short_centerline": bool(row.is_short_centerline),
            }
        )
    cl_dir = gpd.GeoDataFrame(rows, geometry="geometry", crs=centerline.crs).reset_index(drop=True)
    cl_dir["skel_dir"] = np.arange(len(cl_dir), dtype=int)
    return cl_dir


def main():
    args = parse_args()
    source_root = Path(args.source_root)
    output_root = Path(args.output_root)
    if output_root.resolve() == source_root.resolve():
        raise SystemExit("Refusing to write into the source root; choose a different --output-root.")

    data_in = source_root / "data"
    data_out = output_root / "data"
    metrics_out = output_root / "metrics"
    data_out.mkdir(parents=True, exist_ok=True)
    metrics_out.mkdir(parents=True, exist_ok=True)

    cl = gpd.read_parquet(data_in / "centerline_master.parquet")
    grid = gpd.read_parquet(data_in / "grid_square_master.parquet")
    if grid.crs != cl.crs:
        grid = grid.to_crs(cl.crs)

    baseline = cl[cl["keep_baseline"]].copy().reset_index(drop=True)
    bridges = choose_bridges(
        baseline=baseline,
        component_snap_m=args.component_snap_m,
        min_gap_m=args.min_gap_m,
        max_gap_m=args.max_gap_m,
        only_to_lcc=args.only_to_lcc,
        one_bridge_per_component=args.one_bridge_per_component,
    )
    bridge_gdf = bridges_to_gdf(bridges, cl.crs)
    bridged = finalize_with_bridges(cl, bridge_gdf)
    bridged_baseline = bridged[bridged["keep_baseline"]].copy().reset_index(drop=True)
    cl_dir = build_directed_centerline(bridged)

    before = connectivity_stats(baseline, args.component_snap_m)
    after = connectivity_stats(bridged_baseline, args.component_snap_m)

    rook = rook_pairs(grid)
    pairs_before = directed_pairs_from_centerlines(baseline, grid)
    pairs_after = directed_pairs_from_centerlines(bridged_baseline, grid)
    rook_before = summarize_rook_coverage(rook, pairs_before)
    rook_after = summarize_rook_coverage(rook, pairs_after)
    gained = sorted(pairs_after - pairs_before)

    # write outputs
    bridged.to_parquet(data_out / "centerline_master.parquet", index=False)
    cl_dir.to_parquet(data_out / "centerline_dir_master.parquet", index=False)
    if not bridges.empty:
        bridges.to_csv(data_out / "endpoint_bridges_selected.csv", index=False)
        bridge_gdf.to_file(data_out / "endpoint_bridges_selected.geojson", driver="GeoJSON")
    else:
        pd.DataFrame(columns=["bridge_id"]).to_csv(data_out / "endpoint_bridges_selected.csv", index=False)

    pd.DataFrame(gained, columns=["grid_o", "grid_d"]).to_csv(
        data_out / "adjacent_directed_pairs_gained.csv", index=False
    )

    summary = {
        "experiment": "centerline_endpoint_bridge",
        "note": (
            "Does NOT reconstruct centerlines from raw_connected. "
            "Copies existing centerline master and inserts short endpoint bridges."
        ),
        "source_root": str(source_root),
        "output_root": str(output_root),
        "params": {
            "min_gap_m": args.min_gap_m,
            "max_gap_m": args.max_gap_m,
            "component_snap_m": args.component_snap_m,
            "only_to_lcc": args.only_to_lcc,
            "one_bridge_per_component": args.one_bridge_per_component,
            "force_keep_baseline_for_bridges": True,
        },
        "n_bridges": int(len(bridges)),
        "bridge_length_m": {
            "min": float(bridges.length_m.min()) if len(bridges) else None,
            "median": float(bridges.length_m.median()) if len(bridges) else None,
            "max": float(bridges.length_m.max()) if len(bridges) else None,
            "sum": float(bridges.length_m.sum()) if len(bridges) else 0.0,
        },
        "connectivity_before": before,
        "connectivity_after": after,
        "rook_coverage_before": rook_before,
        "rook_coverage_after": rook_after,
        "directed_pairs_gained": len(gained),
        "source_untouched": True,
    }
    (metrics_out / "bridge_experiment_summary.json").write_text(
        json.dumps(summary, ensure_ascii=True, indent=2), encoding="utf-8"
    )
    pd.DataFrame(
        [
            {
                "metric": "n_bridges",
                "before": 0,
                "after": len(bridges),
            },
            {
                "metric": "weak_components",
                "before": before["n_weak_comp"],
                "after": after["n_weak_comp"],
            },
            {
                "metric": "lcc_share_cl",
                "before": before["lcc_share"],
                "after": after["lcc_share"],
            },
            {
                "metric": "lcc_len_share",
                "before": before["lcc_len_share"],
                "after": after["lcc_len_share"],
            },
            {
                "metric": "rook_any_dir_share",
                "before": rook_before["any_dir_share"],
                "after": rook_after["any_dir_share"],
            },
            {
                "metric": "rook_none_share",
                "before": rook_before["none_share"],
                "after": rook_after["none_share"],
            },
            {
                "metric": "directed_pairs",
                "before": rook_before["directed_pairs"],
                "after": rook_after["directed_pairs"],
            },
        ]
    ).to_csv(metrics_out / "bridge_experiment_comparison.csv", index=False)

    readme = f"""# Centerline endpoint-bridge experiment

Source (untouched): `{source_root}`
Output: `{output_root}`

## What this is
- Copies existing centerline geometry from `{source_root}`
- Adds short straight bridges between nearby endpoints of different weak components
- Gap window: {args.min_gap_m:g}–{args.max_gap_m:g} m
- Component definition snap: {args.component_snap_m:g} m
- Bridges are forced into `keep_baseline` even if length < 50 m

## What this is NOT
- Not a reconstruction of centerlines from raw_connected
- Not a rerun of stage02–stage06 speed matching / QSM inputs
- Speeds are not attached to bridges; this release is topology + geometric adjacent-pair coverage only

## Key counts
- Bridges added: {len(bridges)}
- Weak components: {before['n_weak_comp']} -> {after['n_weak_comp']}
- LCC centerline share: {before['lcc_share']:.1%} -> {after['lcc_share']:.1%}
- Rook pairs with any directed centerline edge: {rook_before['any_dir_share']:.1%} -> {rook_after['any_dir_share']:.1%}
- Directed grid pairs gained: {len(gained)}
"""
    (output_root / "README.md").write_text(readme, encoding="utf-8")

    print(json.dumps(summary, ensure_ascii=True, indent=2))


if __name__ == "__main__":
    main()
