from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
GIS_DIR = ROOT / "interim_data" / "gis"
ASYM_DIR = ROOT / "interim_data" / "asym"
OUT_DIR = ROOT / "analysis"

MATCH_PATH = GIS_DIR / "xwalk_split_to_centerline.parquet"
RAW2SPLIT_PATH = GIS_DIR / "xwalk_raw_to_split.parquet"
CENTERLINE_DIR_PATH = GIS_DIR / "step5_centerline_edges_dir.parquet"
SPEED_PATH = ASYM_DIR / "cl_speed_by_time_for_asym.parquet"

OUT_CSV = OUT_DIR / "baseline_metrics.csv"
OUT_MD = OUT_DIR / "baseline_report.md"


def require_file(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Required file not found: {path}")


def pct(x: float) -> str:
    if pd.isna(x):
        return "NA"
    return f"{100.0 * float(x):.2f}%"


def fmt_num(x: float, digits: int = 3) -> str:
    if pd.isna(x):
        return "NA"
    return f"{float(x):,.{digits}f}"


def add_metric(rows, section, scope, metric, value, unit="", note=""):
    rows.append(
        {
            "section": section,
            "scope": scope,
            "metric": metric,
            "value": value,
            "unit": unit,
            "note": note,
        }
    )


def infer_grid_suffix(path: Path) -> str:
    name = path.name
    if name.startswith("grid_links_") and name.endswith("_agg.csv"):
        return name[len("grid_links_") : -len("_agg.csv")]
    if name.startswith("t_edges_") and name.endswith("_AM.csv"):
        return name[len("t_edges_") : -len("_AM.csv")]
    if name.startswith("t_edges_") and name.endswith("_PM.csv"):
        return name[len("t_edges_") : -len("_PM.csv")]
    raise ValueError(f"Cannot infer grid suffix from filename: {name}")


def compute_match_metrics(rows):
    require_file(RAW2SPLIT_PATH)
    require_file(MATCH_PATH)

    raw2split = pd.read_parquet(RAW2SPLIT_PATH)
    split2cl = pd.read_parquet(MATCH_PATH)

    merged = raw2split.merge(split2cl, on=["split_id", "raw_edge_id"], how="left")
    merged["matched"] = pd.to_numeric(merged["matched"], errors="coerce").fillna(0).astype(int)

    split_match_rate = merged["matched"].mean()
    raw_match_rate = (
        merged.groupby("raw_edge_id", as_index=False)["matched"].max()["matched"].mean()
        if "raw_edge_id" in merged.columns
        else np.nan
    )

    add_metric(
        rows,
        "raw_centerline_match",
        "all",
        "split_segments_total",
        int(len(merged)),
        "count",
    )
    add_metric(
        rows,
        "raw_centerline_match",
        "all",
        "split_segments_matched",
        int(merged["matched"].sum()),
        "count",
    )
    add_metric(
        rows,
        "raw_centerline_match",
        "all",
        "split_segment_match_rate",
        float(split_match_rate),
        "share",
    )
    add_metric(
        rows,
        "raw_centerline_match",
        "all",
        "raw_edges_total",
        int(merged["raw_edge_id"].nunique()),
        "count",
    )
    add_metric(
        rows,
        "raw_centerline_match",
        "all",
        "raw_edge_match_rate_any_split",
        float(raw_match_rate),
        "share",
        "Raw edge counted as matched if any split maps to a centerline.",
    )

    return merged


def compute_centerline_coverage(rows, merged):
    require_file(CENTERLINE_DIR_PATH)

    cl_dir = gpd.read_parquet(CENTERLINE_DIR_PATH)
    cl_dir_3857 = cl_dir.to_crs(epsg=3857) if cl_dir.crs and cl_dir.crs.to_epsg() != 3857 else cl_dir
    cl_lengths = cl_dir_3857[["skel_dir"]].copy()
    cl_lengths["cl_len_m"] = cl_dir_3857.geometry.length.astype(float)

    matched = merged[merged["matched"] == 1].copy()
    if matched.empty:
        matched_len_by_cl = pd.DataFrame(columns=["skel_dir", "matched_len_m"])
    else:
        split_geom = matched.merge(
            cl_lengths[["skel_dir"]],
            on="skel_dir",
            how="left",
        )
        # Segment lengths are not stored in xwalk_split_to_centerline; use raw_split split table is disallowed here.
        # Approximate matched coverage via evenly split assignment is avoided. Instead read the saved directed-centerline table
        # and estimate coverage by unique matched centerlines and matched projected span when available.
        use = matched.copy()
        use["s_from"] = pd.to_numeric(use.get("s_from"), errors="coerce")
        use["s_to"] = pd.to_numeric(use.get("s_to"), errors="coerce")
        use["span_m"] = use["s_to"] - use["s_from"]
        use["span_m"] = np.where(use["span_m"] > 0, use["span_m"], np.nan)
        matched_len_by_cl = (
            use.groupby("skel_dir", as_index=False)
            .agg(matched_len_m=("span_m", "sum"))
        )

    coverage = cl_lengths.merge(matched_len_by_cl, on="skel_dir", how="left")
    coverage["matched_len_m"] = coverage["matched_len_m"].fillna(0.0)
    coverage["matched_len_m_capped"] = np.minimum(coverage["matched_len_m"], coverage["cl_len_m"])

    total_len_m = float(coverage["cl_len_m"].sum())
    matched_len_share = (
        float(coverage["matched_len_m_capped"].sum() / total_len_m) if total_len_m > 0 else np.nan
    )
    covered_centerlines = int((coverage["matched_len_m_capped"] > 0).sum())

    add_metric(rows, "centerline_coverage", "all", "centerlines_total", int(len(coverage)), "count")
    add_metric(rows, "centerline_coverage", "all", "centerlines_with_match", covered_centerlines, "count")
    add_metric(rows, "centerline_coverage", "all", "total_centerline_length_m", total_len_m, "m")
    add_metric(
        rows,
        "centerline_coverage",
        "all",
        "matched_length_share",
        matched_len_share,
        "share",
        "Computed from summed matched projected spans, capped by centerline length.",
    )


def compute_speed_metrics(rows):
    require_file(SPEED_PATH)
    spd = pd.read_parquet(SPEED_PATH)
    speed = pd.to_numeric(spd["cl_speed_time"], errors="coerce")
    speed = speed[np.isfinite(speed) & (speed > 0)]

    add_metric(rows, "speed_distribution", "all", "n_speed_records", int(len(speed)), "count")
    add_metric(rows, "speed_distribution", "all", "mean_speed_kmh", float(speed.mean()), "km/h")
    add_metric(rows, "speed_distribution", "all", "median_speed_kmh", float(speed.median()), "km/h")
    add_metric(rows, "speed_distribution", "all", "p10_speed_kmh", float(speed.quantile(0.10)), "km/h")
    add_metric(rows, "speed_distribution", "all", "p90_speed_kmh", float(speed.quantile(0.90)), "km/h")

    hist_counts, bin_edges = np.histogram(speed.to_numpy(), bins=10)
    for i, count in enumerate(hist_counts):
        add_metric(
            rows,
            "speed_distribution_histogram",
            "all",
            f"bin_{i:02d}_count",
            int(count),
            "count",
            f"[{bin_edges[i]:.3f}, {bin_edges[i+1]:.3f}) km/h",
        )


def mean_undirected_degree(edges: pd.DataFrame) -> float:
    if edges.empty:
        return np.nan
    pairs = edges[["grid_o", "grid_d"]].copy()
    pairs["a"] = np.minimum(pairs["grid_o"].astype(str), pairs["grid_d"].astype(str))
    pairs["b"] = np.maximum(pairs["grid_o"].astype(str), pairs["grid_d"].astype(str))
    pairs = pairs[pairs["a"] != pairs["b"]].drop_duplicates(subset=["a", "b"])
    if pairs.empty:
        return np.nan
    deg = pd.concat([pairs["a"], pairs["b"]], ignore_index=True).value_counts()
    return float(deg.mean()) if len(deg) else np.nan


def compute_grid_metrics(rows):
    link_files = sorted(GIS_DIR.glob("grid_links_*_agg.csv"))
    if not link_files:
        raise FileNotFoundError("No grid_links_*_agg.csv files found.")

    for link_path in link_files:
        suffix = infer_grid_suffix(link_path)
        links = pd.read_csv(link_path)

        tt = pd.to_numeric(links.get("total_tt_min"), errors="coerce")
        tt = tt[np.isfinite(tt) & (tt > 0)]

        add_metric(rows, "travel_time", suffix, "n_grid_links", int(len(tt)), "count")
        add_metric(rows, "travel_time", suffix, "mean_travel_time_min", float(tt.mean()), "min")
        add_metric(rows, "travel_time", suffix, "median_travel_time_min", float(tt.median()), "min")
        add_metric(rows, "travel_time", suffix, "p10_travel_time_min", float(tt.quantile(0.10)), "min")
        add_metric(rows, "travel_time", suffix, "p90_travel_time_min", float(tt.quantile(0.90)), "min")
        add_metric(rows, "travel_time", suffix, "max_travel_time_min", float(tt.max()), "min")

    edge_files = sorted(GIS_DIR.glob("t_edges_*_AM.csv")) + sorted(GIS_DIR.glob("t_edges_*_PM.csv"))
    seen = set()
    for edge_path in edge_files:
        suffix = infer_grid_suffix(edge_path)
        period = "AM" if edge_path.name.endswith("_AM.csv") else "PM"
        scope = f"{suffix}_{period}"
        seen.add(scope)

        edges = pd.read_csv(edge_path)
        for col in ["grid_o", "grid_d"]:
            edges[col] = edges[col].astype(str)

        node_count = int(pd.unique(pd.concat([edges["grid_o"], edges["grid_d"]], ignore_index=True)).size)
        edge_count = int(len(edges))
        add_metric(rows, "grid_connectivity", scope, "grid_nodes", node_count, "count")
        add_metric(rows, "grid_connectivity", scope, "grid_edges", edge_count, "count")
        add_metric(
            rows,
            "grid_connectivity",
            scope,
            "mean_degree_undirected",
            mean_undirected_degree(edges),
            "degree",
        )

    for suffix in sorted(infer_grid_suffix(p) for p in GIS_DIR.glob("grid_links_*_agg.csv")):
        for period in ["AM", "PM"]:
            scope = f"{suffix}_{period}"
            if scope not in seen:
                add_metric(
                    rows,
                    "grid_connectivity",
                    scope,
                    "status",
                    np.nan,
                    "",
                    f"Missing edge file: t_edges_{suffix}_{period}.csv",
                )


def write_report(metrics: pd.DataFrame) -> None:
    lines = ["# Baseline Diagnostics Report", "", "Generated from existing intermediate datasets only.", ""]

    match = metrics[metrics["section"] == "raw_centerline_match"].set_index("metric")
    coverage = metrics[metrics["section"] == "centerline_coverage"].set_index("metric")
    speed = metrics[metrics["section"] == "speed_distribution"].set_index("metric")

    lines.extend(
        [
            "## Overview",
            "",
            f"- Split-segment match rate: {pct(match.at['split_segment_match_rate', 'value'])}",
            f"- Raw-edge match rate (any split matched): {pct(match.at['raw_edge_match_rate_any_split', 'value'])}",
            f"- Total centerline length: {fmt_num(coverage.at['total_centerline_length_m', 'value'])} m",
            f"- Matched centerline length share: {pct(coverage.at['matched_length_share', 'value'])}",
            f"- Mean speed: {fmt_num(speed.at['mean_speed_kmh', 'value'])} km/h",
            f"- Median speed: {fmt_num(speed.at['median_speed_kmh', 'value'])} km/h",
            "",
        ]
    )

    lines.extend(["## Speed Distribution", "", "| Metric | Value |", "|---|---|"])
    for metric in ["mean_speed_kmh", "median_speed_kmh", "p10_speed_kmh", "p90_speed_kmh"]:
        lines.append(f"| `{metric}` | {fmt_num(speed.at[metric, 'value'])} km/h |")
    lines.append("")

    hist = metrics[metrics["section"] == "speed_distribution_histogram"]
    lines.extend(["### Histogram Bins", "", "| Bin | Count | Range |", "|---|---:|---|"])
    for row in hist.itertuples(index=False):
        lines.append(f"| `{row.metric}` | {int(row.value)} | {row.note} |")
    lines.append("")

    lines.extend(["## Grid Connectivity", ""])
    conn = metrics[metrics["section"] == "grid_connectivity"].copy()
    if conn.empty:
        lines.append("No edge files found.")
    else:
        lines.extend(["| Scope | Nodes | Edges | Mean Degree | Note |", "|---|---:|---:|---:|---|"])
        scopes = sorted(conn["scope"].unique())
        for scope in scopes:
            sub = conn[conn["scope"] == scope].set_index("metric")
            if "status" in sub.index:
                lines.append(f"| `{scope}` |  |  |  | {sub.at['status', 'note']} |")
                continue
            lines.append(
                f"| `{scope}` | "
                f"{int(sub.at['grid_nodes', 'value'])} | "
                f"{int(sub.at['grid_edges', 'value'])} | "
                f"{fmt_num(sub.at['mean_degree_undirected', 'value'])} |  |"
            )
    lines.append("")

    lines.extend(["## Grid-to-Grid Travel Time", "", "| Grid | Mean | Median | P10 | P90 | Max |", "|---|---:|---:|---:|---:|---:|"])
    tt = metrics[metrics["section"] == "travel_time"]
    for scope in sorted(tt["scope"].unique()):
        sub = tt[tt["scope"] == scope].set_index("metric")
        lines.append(
            f"| `{scope}` | "
            f"{fmt_num(sub.at['mean_travel_time_min', 'value'])} | "
            f"{fmt_num(sub.at['median_travel_time_min', 'value'])} | "
            f"{fmt_num(sub.at['p10_travel_time_min', 'value'])} | "
            f"{fmt_num(sub.at['p90_travel_time_min', 'value'])} | "
            f"{fmt_num(sub.at['max_travel_time_min', 'value'])} |"
        )
    lines.append("")

    missing_notes = metrics[metrics["note"].astype(str).str.contains("Missing edge file", na=False)]
    if not missing_notes.empty:
        lines.extend(["## Missing Inputs", ""])
        for row in missing_notes.itertuples(index=False):
            lines.append(f"- `{row.scope}`: {row.note}")
        lines.append("")

    OUT_MD.write_text("\n".join(lines), encoding="utf-8")


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    rows = []
    merged = compute_match_metrics(rows)
    compute_centerline_coverage(rows, merged)
    compute_speed_metrics(rows)
    compute_grid_metrics(rows)

    metrics = pd.DataFrame(rows)
    metrics.to_csv(OUT_CSV, index=False)
    write_report(metrics)

    print(f"Saved: {OUT_CSV}")
    print(f"Saved: {OUT_MD}")


if __name__ == "__main__":
    main()
