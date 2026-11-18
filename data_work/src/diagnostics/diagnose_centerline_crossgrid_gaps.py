"""Rank raw-road-supported cross-grid gaps in the retained centerline input.

This diagnostic is candidate-only. It reads the retained centerline version and
writes a new version directory without modifying upstream data or QSM inputs.
"""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import networkx as nx
import numpy as np
import pandas as pd
from shapely.geometry import LineString

from bridge_centerline_endpoints_experiment import (
    directed_pairs_from_centerlines,
    rook_pairs,
    segment_to_grid_sequence,
    to_lines,
)


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--source-root", default="data_work/outputs/manual_centerline_rules_v11")
    p.add_argument(
        "--output-root",
        default="data_work/outputs/centerline_crossgrid_gap_candidates_v1",
    )
    p.add_argument("--speed-path", default="data_work/raw_data/speed_Beijing_all_wgs84.csv")
    p.add_argument("--review-top-n", type=int, default=40)
    return p.parse_args()


def refuse_overwrite(output: Path, source: Path):
    if output.resolve() == source.resolve():
        raise SystemExit("Refusing to write into the source version.")
    if output.exists():
        raise SystemExit(f"Refusing to overwrite existing output: {output}")
    for name in ("data", "metrics", "figures", "docs"):
        (output / name).mkdir(parents=True, exist_ok=False)


def orient_geometry(row):
    geom = row.geometry
    if geom is None or geom.is_empty or geom.geom_type != "LineString":
        return None
    bearing = float(row.bear_geom) if pd.notna(row.bear_geom) else np.nan
    direction = float(row.dir_deg_final) if pd.notna(row.dir_deg_final) else np.nan
    if np.isfinite(bearing) and np.isfinite(direction):
        delta = abs((bearing - direction + 180.0) % 360.0 - 180.0)
        if delta > 90.0:
            return LineString(list(geom.coords)[::-1])
    return geom


def raw_pair_support(raw, grid, rook_directed):
    grid_sub = grid[["grid_id", "geometry"]].copy()
    grid_sub["grid_id"] = grid_sub.grid_id.astype(str)
    sindex = grid_sub.sindex
    rows = []
    for row in raw.itertuples(index=False):
        geom = orient_geometry(row)
        if geom is None:
            continue
        seq = segment_to_grid_sequence(geom, sindex, grid_sub)
        for k in range(len(seq) - 1):
            go, length = seq[k]
            gd = seq[k + 1][0]
            if go == gd or (go, gd) not in rook_directed:
                continue
            rows.append(
                {
                    "grid_o": go,
                    "grid_d": gd,
                    "raw_edge_id": int(row.raw_edge_id),
                    "roadseg_id": str(row.roadseg_id),
                    "roadname": str(row.roadname) if pd.notna(row.roadname) else "",
                    "road_class": int(row.road_class),
                    "support_len_m": float(length),
                    "matched_centerline": int(getattr(row, "matched", 0)),
                    "cline_id": getattr(row, "cline_id", pd.NA),
                    "geometry": geom,
                }
            )
    return gpd.GeoDataFrame(rows, geometry="geometry", crs=raw.crs)


def load_speed_support(path: Path):
    usecols = ["roadseg_id", "speed", "day", "hour"]
    speed = pd.read_csv(path, usecols=usecols)
    speed["speed"] = pd.to_numeric(speed.speed, errors="coerce")
    speed["hour_int"] = pd.to_numeric(speed.hour, errors="coerce").round().astype("Int64")
    speed["dt"] = pd.to_datetime(speed.hour_int.astype(str), format="%Y%m%d%H%M", errors="coerce")
    speed["weekday"] = speed.dt.dt.weekday < 5
    h = speed.dt.dt.hour
    speed["am"] = speed.weekday & h.ge(7) & h.lt(9)
    speed["pm"] = speed.weekday & h.ge(17) & h.lt(19)
    speed["ff"] = h.ge(22) | h.le(5)
    out = speed.groupby("roadseg_id", as_index=False).agg(
        speed_obs=("speed", "count"),
        am_obs=("am", "sum"),
        pm_obs=("pm", "sum"),
        ff_obs=("ff", "sum"),
        speed_median=("speed", "median"),
    )
    for col in ["am_obs", "pm_obs", "ff_obs"]:
        out[col] = out[col].astype(int)
    return out


def current_detours(edges):
    graph = nx.DiGraph()
    for row in edges.itertuples(index=False):
        old = graph.get_edge_data(str(row.grid_o), str(row.grid_d))
        value = float(row.t_min)
        if old is None or value < old["weight"]:
            graph.add_edge(str(row.grid_o), str(row.grid_d), weight=value)
    return graph


def write_docs(output, args, summary):
    docs = {
        "analysis-plan.md": """# Analysis plan

1. Separate geometric centerline grid crossings from speed-bearing stage06 edges.
2. Map each directed raw segment to its square-grid crossing sequence.
3. Retain rook-adjacent raw crossings absent from the AM stage06 edge set.
4. Measure raw speed support, centerline matching, road class, direct OD flow, and current-network detours.
5. Rank candidates for robustness construction. No candidate is promoted automatically.
""",
        "variable-dictionary.md": """# Variable dictionary

- `gap_type`: `geometry_missing` or `speed_missing` relative to retained centerline products.
- `raw_support_len_m`: total raw length contributing to the directed grid crossing.
- `speed_support_share`: raw support length associated with a road segment that has AM, PM, and free-flow observations.
- `matched_support_share`: raw support length already matched to a retained centerline.
- `direct_commuters_road`: observed road-mode commuters between the two adjacent grids.
- `current_detour_min`: shortest AM travel time between the grids without the candidate edge.
- `priority_score`: mechanical ranking index combining support, speed coverage, road class, commuters, and detour.
""",
        "sample-lineage.md": f"""# Sample lineage

- Source centerline version: `{args.source_root}` (read-only).
- Raw geometry: `raw_split_centerline.parquet`, restricted to baseline valid LineStrings.
- Effective edge benchmark: `t_edges_square_AM.csv`.
- Speed support: `{args.speed_path}`.
- Exported directed gap candidates: {summary['candidate_pairs']}.
- No source row or existing output was rewritten.
""",
        "geospatial-data-notes.md": """# Geospatial data notes

All network geometry is evaluated in the source projected CRS. Raw direction follows `dir_deg_final`; geometry is reversed when its bearing differs by more than 90 degrees. Only rook-adjacent square-grid crossings are retained. A raw crossing establishes geometric support, not legal intersection connectivity; grade separation remains a limitation.
""",
        "exposure-definition.md": """# Exposure definition

Not applicable. The object is a directed network-input gap rather than an empirical treatment exposure.
""",
        "regression-spec-matrix.md": """# Regression specification matrix

Not applicable. No regression is estimated.
""",
        "table-shells.md": """# Table shells

The main candidate table reports gap type, raw support, speed coverage, centerline matching, road class, observed road-mode commuting, current detour, and the mechanical priority score.
""",
        "replication-checklist.md": f"""# Replication checklist

- [x] Source and output versions are explicit.
- [x] Existing directories cannot be overwritten.
- [x] Raw direction and grid adjacency rules are recorded.
- [x] Candidate attributes and geometries are exported.
- [x] No production edge is created automatically.

```bash
python data_work/src/diagnostics/diagnose_centerline_crossgrid_gaps.py --source-root {args.source_root} --output-root {args.output_root}
```
""",
    }
    for name, text in docs.items():
        (output / "docs" / name).write_text(text, encoding="utf-8")


def main():
    args = parse_args()
    source, output = Path(args.source_root), Path(args.output_root)
    refuse_overwrite(output, source)
    data = source / "data"

    centerline = gpd.read_parquet(data / "centerline_master.parquet")
    centerline = centerline.loc[centerline.keep_baseline].copy()
    grid = gpd.read_parquet(data / "grid_square_master.parquet")
    raw = gpd.read_parquet(data / "raw_split_centerline.parquet")
    raw = raw.loc[raw.keep_baseline & raw.is_valid_geometry & raw.is_linestring].copy()
    if grid.crs != centerline.crs:
        grid = grid.to_crs(centerline.crs)
    if raw.crs != centerline.crs:
        raw = raw.to_crs(centerline.crs)

    rook_undirected = rook_pairs(grid)
    rook_directed = {(a, b) for a, b in rook_undirected} | {(b, a) for a, b in rook_undirected}
    geometry_pairs = directed_pairs_from_centerlines(centerline, grid)
    am = pd.read_csv(data / "t_edges_square_AM.csv", dtype={"grid_o": str, "grid_d": str})
    effective_pairs = set(zip(am.grid_o, am.grid_d))

    raw_support = raw_pair_support(raw, grid, rook_directed)
    speed = load_speed_support(Path(args.speed_path))
    raw_support = raw_support.merge(speed, on="roadseg_id", how="left")
    raw_support["full_speed_support"] = (
        raw_support.am_obs.fillna(0).gt(0)
        & raw_support.pm_obs.fillna(0).gt(0)
        & raw_support.ff_obs.fillna(0).gt(0)
    )
    raw_support["speed_support_len_m"] = np.where(
        raw_support.full_speed_support, raw_support.support_len_m, 0.0
    )
    raw_support["matched_support_len_m"] = raw_support.support_len_m * raw_support.matched_centerline
    missing = raw_support.loc[
        ~pd.MultiIndex.from_frame(raw_support[["grid_o", "grid_d"]]).isin(
            pd.MultiIndex.from_tuples(effective_pairs)
        )
    ].copy()

    agg = missing.groupby(["grid_o", "grid_d"], as_index=False).agg(
        raw_parts=("raw_edge_id", "size"),
        raw_edges=("raw_edge_id", "nunique"),
        roadseg_ids=("roadseg_id", "nunique"),
        raw_support_len_m=("support_len_m", "sum"),
        speed_support_len_m=("speed_support_len_m", "sum"),
        matched_support_len_m=("matched_support_len_m", "sum"),
        best_road_class=("road_class", "min"),
        roadnames=("roadname", lambda x: " | ".join(sorted({v for v in x if v})[:8])),
    )
    agg["speed_support_share"] = agg.speed_support_len_m / agg.raw_support_len_m
    agg["matched_support_share"] = agg.matched_support_len_m / agg.raw_support_len_m
    agg["gap_type"] = [
        "speed_missing" if (o, d) in geometry_pairs else "geometry_missing"
        for o, d in zip(agg.grid_o, agg.grid_d)
    ]

    od = pd.read_parquet(data / "qsm_input_od_square.parquet")
    od_direct = od.groupby(["origin_grid", "destination_grid"], as_index=False).agg(
        direct_commuters_road=("commuters_road", "sum")
    ).rename(columns={"origin_grid": "grid_o", "destination_grid": "grid_d"})
    agg = agg.merge(od_direct, on=["grid_o", "grid_d"], how="left")
    agg.direct_commuters_road = agg.direct_commuters_road.fillna(0.0)

    graph = current_detours(am)
    detours, hops = [], []
    for row in agg.itertuples(index=False):
        try:
            path = nx.shortest_path(graph, row.grid_o, row.grid_d, weight="weight")
            detours.append(float(nx.path_weight(graph, path, weight="weight")))
            hops.append(len(path) - 1)
        except (nx.NetworkXNoPath, nx.NodeNotFound):
            detours.append(np.nan)
            hops.append(np.nan)
    agg["current_detour_min"] = detours
    agg["current_detour_hops"] = hops
    road_priority = (5 - agg.best_road_class.clip(1, 4)) / 4
    agg["priority_score"] = (
        np.log1p(agg.raw_support_len_m)
        * (0.25 + 0.75 * agg.speed_support_share)
        * (0.5 + road_priority)
        * (1 + np.log1p(agg.direct_commuters_road))
        * (1 + np.log1p(agg.current_detour_min.fillna(10)))
    )
    agg = agg.sort_values("priority_score", ascending=False).reset_index(drop=True)
    agg.insert(0, "candidate_id", [f"CROSSGRID_{i:04d}" for i in range(1, len(agg) + 1)])

    top_support = missing.sort_values("support_len_m", ascending=False).drop_duplicates(["grid_o", "grid_d"])
    top_support = top_support.merge(agg[["candidate_id", "grid_o", "grid_d"]], on=["grid_o", "grid_d"])
    candidate_gdf = gpd.GeoDataFrame(
        top_support[["candidate_id", "grid_o", "grid_d", "roadname", "roadseg_id", "geometry"]],
        geometry="geometry",
        crs=raw.crs,
    ).merge(agg, on=["candidate_id", "grid_o", "grid_d"], how="left")

    agg.to_csv(output / "data" / "crossgrid_gap_candidates.csv", index=False)
    candidate_gdf.to_file(output / "data" / "crossgrid_gap_candidates.geojson", driver="GeoJSON")

    review_dir = output / "figures" / "candidate_review"
    review_dir.mkdir()
    for row in candidate_gdf.head(args.review_top_n).itertuples(index=False):
        xmin, ymin, xmax, ymax = row.geometry.bounds
        pad = 4500
        local_grid = grid.cx[xmin-pad:xmax+pad, ymin-pad:ymax+pad]
        local_raw = raw.cx[xmin-pad:xmax+pad, ymin-pad:ymax+pad]
        local_cl = centerline.cx[xmin-pad:xmax+pad, ymin-pad:ymax+pad]
        fig, ax = plt.subplots(figsize=(7, 7))
        local_grid.boundary.plot(ax=ax, color="#d0d0d0", linewidth=0.5)
        local_raw.plot(ax=ax, color="#dddddd", linewidth=0.35)
        local_cl.plot(ax=ax, color="#426b7a", linewidth=0.8)
        gpd.GeoSeries([row.geometry], crs=raw.crs).plot(ax=ax, color="#b2182b", linewidth=2.4)
        ax.set_xlim(xmin-pad, xmax+pad); ax.set_ylim(ymin-pad, ymax+pad); ax.set_axis_off()
        ax.set_title(
            f"{row.candidate_id} | {row.gap_type} | {row.grid_o} to {row.grid_d}\n"
            f"speed support {row.speed_support_share:.0%}; matched {row.matched_support_share:.0%}; detour {row.current_detour_min:.1f} min"
        )
        fig.tight_layout()
        fig.savefig(review_dir / f"{row.candidate_id}.png", dpi=180)
        plt.close(fig)

    summary = {
        "version": output.name,
        "source_root": str(source),
        "source_untouched": True,
        "geometry_directed_pairs": len(geometry_pairs),
        "effective_am_directed_pairs": len(effective_pairs),
        "raw_supported_directed_rook_pairs": int(raw_support[["grid_o", "grid_d"]].drop_duplicates().shape[0]),
        "candidate_pairs": int(len(agg)),
        "gap_type_counts": agg.gap_type.value_counts().to_dict(),
        "full_speed_candidates": int((agg.speed_support_share >= 0.999).sum()),
        "parameters": vars(args),
    }
    (output / "metrics" / "run_manifest.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    write_docs(output, args, summary)
    shutil.copy2(__file__, output / "docs" / Path(__file__).name)
    (output / "README.md").write_text(
        f"""# Centerline cross-grid gap candidates

Source remains unchanged: `{source}`.

This candidate-only diagnostic identifies raw-road-supported directed rook crossings that are absent from the retained AM stage06 edge set. It exports {len(agg)} candidate pairs. No centerline, speed record, or QSM input is modified.
""",
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
