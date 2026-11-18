"""Render local review panels for raw-path centerline topology candidates."""

from __future__ import annotations

import argparse
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd

from bridge_centerline_endpoints_experiment import build_component_labels


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", default="data_work/outputs/manual_centerline_rules_v11")
    parser.add_argument(
        "--candidate-root",
        default="data_work/outputs/centerline_rawpath_topology_candidates_v1",
    )
    parser.add_argument("--component-snap-m", type=float, default=35.0)
    parser.add_argument("--pad-m", type=float, default=800.0)
    args = parser.parse_args()

    source = Path(args.source_root)
    root = Path(args.candidate_root)
    out = root / "figures" / "candidate_review"
    if out.exists():
        raise SystemExit(f"Refusing to overwrite existing review directory: {out}")
    out.mkdir(parents=True)

    centerline = gpd.read_parquet(source / "data" / "centerline_master.parquet")
    centerline = centerline.loc[centerline.keep_baseline].copy().reset_index(drop=True)
    labels, _ = build_component_labels(centerline, args.component_snap_m)
    centerline["comp_id"] = labels
    lcc = int(centerline.comp_id.value_counts().index[0])
    raw = gpd.read_parquet(source / "data" / "raw_segment_master.parquet")
    raw = raw.loc[raw.keep_baseline & raw.is_valid_geometry & raw.is_linestring].copy()
    cand = gpd.read_file(root / "data" / "rawpath_topology_candidates.geojson")
    if raw.crs != centerline.crs:
        raw = raw.to_crs(centerline.crs)
    if cand.crs != centerline.crs:
        cand = cand.to_crs(centerline.crs)

    manifest = []
    for row in cand.itertuples(index=False):
        xmin, ymin, xmax, ymax = row.geometry.bounds
        xmin, ymin, xmax, ymax = xmin-args.pad_m, ymin-args.pad_m, xmax+args.pad_m, ymax+args.pad_m
        bbox = (xmin, ymin, xmax, ymax)
        raw_local = raw.cx[xmin:xmax, ymin:ymax]
        cl_local = centerline.cx[xmin:xmax, ymin:ymax]
        fig, ax = plt.subplots(figsize=(7, 7))
        raw_local.plot(ax=ax, color="#dddddd", linewidth=0.5, label="raw road")
        cl_local.loc[cl_local.comp_id == lcc].plot(ax=ax, color="#355f73", linewidth=1.0, label="centerline LCC")
        cl_local.loc[cl_local.comp_id == row.comp_id].plot(ax=ax, color="#d47f2a", linewidth=1.8, label="non-LCC component")
        gpd.GeoSeries([row.geometry], crs=cand.crs).plot(ax=ax, color="#b2182b", linewidth=2.2, label="supported candidate")
        ax.scatter([row.from_x], [row.from_y], color="#d47f2a", s=28, zorder=5)
        ax.scatter([row.to_x], [row.to_y], color="#355f73", s=28, zorder=5)
        ax.set_xlim(xmin, xmax)
        ax.set_ylim(ymin, ymax)
        ax.set_aspect("equal")
        ax.set_axis_off()
        ax.set_title(
            f"{row.candidate_id} | {row.confidence} | comp {row.comp_id}\n"
            f"component {row.component_length_m/1000:.1f} km; supported route {row.total_supported_m:.0f} m; stretch {row.route_stretch:.2f}"
        )
        handles, labels_text = ax.get_legend_handles_labels()
        by_label = dict(zip(labels_text, handles))
        ax.legend(by_label.values(), by_label.keys(), loc="lower left", fontsize=8)
        fig.tight_layout()
        filename = f"{row.candidate_id}_{row.confidence}.png"
        fig.savefig(out / filename, dpi=180)
        plt.close(fig)
        manifest.append({"candidate_id": row.candidate_id, "confidence": row.confidence, "file": filename})

    pd.DataFrame(manifest).to_csv(out / "review_manifest.csv", index=False)
    print(f"Rendered {len(manifest)} candidate review panels to {out}")


if __name__ == "__main__":
    main()
