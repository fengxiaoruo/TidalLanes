from __future__ import annotations

import argparse
import shutil
from pathlib import Path

import numpy as np
import pandas as pd
from shapely.geometry import LineString


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PROCESSED_DIR = ROOT / "src" / "raw_connected_v2" / "Processed_Data"
DEFAULT_RAW_SPEED_PATH = ROOT / "raw_data" / "speed_Beijing_all_wgs84.csv"

EDGE_COLS = [
    "edge_id",
    "from_node_id",
    "to_node_id",
    "road_id",
    "roadname",
    "semantic",
    "location",
    "start_lon",
    "start_lat",
    "end_lon",
    "end_lat",
    "start_x_m",
    "start_y_m",
    "end_x_m",
    "end_y_m",
    "geometry",
]

NODE_COLS = ["node_id", "lon", "lat", "x_m", "y_m", "n_endpoints"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Normalize raw_connected_v2 Processed_Data so it can be used with "
            "the raw_connected_v1 grid-travel-time scripts by changing only "
            "the source folder path."
        )
    )
    parser.add_argument("--processed-dir", default=str(DEFAULT_PROCESSED_DIR))
    parser.add_argument("--raw-speed-path", default=str(DEFAULT_RAW_SPEED_PATH))
    parser.add_argument("--speed-chunksize", type=int, default=500_000)
    parser.add_argument(
        "--version-label",
        default="v2",
        help="Version tag used in derived filenames (e.g. raw_connected_<label>_edges_without_speed.csv).",
    )
    parser.add_argument(
        "--backup-dir-name",
        default="_source_before_normalize",
        help="Subdirectory used for one-time copies of pre-normalized files.",
    )
    return parser.parse_args()


REQUIRED_EDGE_INPUT_COLS = ["edge_id", "from_node_id", "to_node_id", "road_id"]
REQUIRED_NODE_INPUT_COLS = ["node_id", "lon", "lat"]
REQUIRED_SPEED_COLS = ["road_id", "timestamp", "speed_kmh"]


def _validate_columns(name: str, have: list[str], required: list[str]) -> None:
    missing = [c for c in required if c not in have]
    if missing:
        raise ValueError(
            f"[normalize] {name} is missing required columns {missing}; "
            f"found {list(have)}. Schema contract not met for this version."
        )


def validate_input_schema(processed_dir: Path) -> None:
    """Fail fast if a version's raw Processed_Data violates the schema contract."""
    edges_head = pd.read_excel(processed_dir / "directed_edges.xlsx", nrows=5)
    _validate_columns("directed_edges.xlsx", list(edges_head.columns), REQUIRED_EDGE_INPUT_COLS)
    nodes_head = pd.read_excel(processed_dir / "nodes.xlsx", nrows=5)
    _validate_columns("nodes.xlsx", list(nodes_head.columns), REQUIRED_NODE_INPUT_COLS)

    panel_path = processed_dir / "speed_panel_isolated.parquet"
    manual_path = processed_dir / "manual_edge_speeds.csv"
    if not panel_path.exists() and not manual_path.exists():
        raise FileNotFoundError(
            f"[normalize] no speed source found in {processed_dir} "
            "(need speed_panel_isolated.parquet or manual_edge_speeds.csv)."
        )
    if panel_path.exists():
        import pyarrow.parquet as pq

        _validate_columns(
            "speed_panel_isolated.parquet", list(pq.ParquetFile(panel_path).schema.names), REQUIRED_SPEED_COLS
        )
    if manual_path.exists():
        manual_head = pd.read_csv(manual_path, nrows=5)
        _validate_columns("manual_edge_speeds.csv", list(manual_head.columns), REQUIRED_SPEED_COLS)
    print(f"[normalize] schema validation passed for {processed_dir}")


def backup_inputs(processed_dir: Path, backup_dir_name: str) -> Path:
    backup_dir = processed_dir / backup_dir_name
    backup_dir.mkdir(parents=True, exist_ok=True)
    for name in [
        "nodes.xlsx",
        "directed_edges.xlsx",
        "manual_edge_speeds.csv",
        "speed_panel_isolated.parquet",
        "manual_edge_registry.xlsx",
    ]:
        src = processed_dir / name
        dst = backup_dir / name
        if src.exists() and not dst.exists():
            shutil.copy2(src, dst)
    return backup_dir


def load_raw_geometry_map(speed_path: Path, road_ids: set[str], chunksize: int) -> dict[str, str]:
    out: dict[str, str] = {}
    usecols = ["roadseg_id", "geometry"]
    for chunk in pd.read_csv(speed_path, usecols=usecols, chunksize=chunksize):
        chunk = chunk[chunk["roadseg_id"].notna() & chunk["geometry"].notna()].copy()
        if chunk.empty:
            continue
        chunk["roadseg_id"] = chunk["roadseg_id"].astype(str)
        chunk = chunk[chunk["roadseg_id"].isin(road_ids - set(out))]
        if chunk.empty:
            continue
        for row in chunk.drop_duplicates("roadseg_id").itertuples(index=False):
            out[str(row.roadseg_id)] = str(row.geometry)
        if len(out) == len(road_ids):
            break
    return out


def line_from_nodes(row: pd.Series) -> str:
    return LineString(
        [
            (float(row["start_lon"]), float(row["start_lat"])),
            (float(row["end_lon"]), float(row["end_lat"])),
        ]
    ).wkt


def normalize_nodes(nodes: pd.DataFrame) -> pd.DataFrame:
    out = nodes.copy()
    for col in NODE_COLS:
        if col not in out.columns:
            out[col] = np.nan
    out["node_id"] = pd.to_numeric(out["node_id"], errors="coerce").astype("Int64")
    out = out[out["node_id"].notna()].copy()
    out["node_id"] = out["node_id"].astype(int)
    for col in ["lon", "lat", "x_m", "y_m", "n_endpoints"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out = out.sort_values("node_id").drop_duplicates("node_id", keep="first")
    return out[NODE_COLS].reset_index(drop=True)


def normalize_edges(edges: pd.DataFrame, nodes: pd.DataFrame, raw_geometry: dict[str, str]) -> pd.DataFrame:
    out = edges.copy()
    for col in EDGE_COLS:
        if col not in out.columns:
            out[col] = np.nan

    out["edge_id"] = out["edge_id"].astype("string")
    out["road_id"] = out["road_id"].astype("string")
    out["from_node_id"] = pd.to_numeric(out["from_node_id"], errors="coerce").astype("Int64")
    out["to_node_id"] = pd.to_numeric(out["to_node_id"], errors="coerce").astype("Int64")
    out = out[out["edge_id"].notna() & out["road_id"].notna() & out["from_node_id"].notna() & out["to_node_id"].notna()].copy()
    out["from_node_id"] = out["from_node_id"].astype(int)
    out["to_node_id"] = out["to_node_id"].astype(int)

    node_lookup = nodes.set_index("node_id")[["lon", "lat", "x_m", "y_m"]]
    out = out.merge(
        node_lookup.add_prefix("from_"),
        left_on="from_node_id",
        right_index=True,
        how="left",
        suffixes=("", "_node_from"),
    )
    out = out.merge(
        node_lookup.add_prefix("to_"),
        left_on="to_node_id",
        right_index=True,
        how="left",
        suffixes=("", "_node_to"),
    )

    fill_pairs = {
        "start_lon": "from_lon",
        "start_lat": "from_lat",
        "start_x_m": "from_x_m",
        "start_y_m": "from_y_m",
        "end_lon": "to_lon",
        "end_lat": "to_lat",
        "end_x_m": "to_x_m",
        "end_y_m": "to_y_m",
    }
    for dst, src in fill_pairs.items():
        out[dst] = pd.to_numeric(out[dst], errors="coerce")
        out[dst] = out[dst].fillna(out[src])

    out["geometry"] = out["geometry"].where(out["geometry"].notna(), out["road_id"].astype(str).map(raw_geometry))
    missing_geometry = out["geometry"].isna()
    can_build = missing_geometry & out[["start_lon", "start_lat", "end_lon", "end_lat"]].notna().all(axis=1)
    out.loc[can_build, "geometry"] = out.loc[can_build].apply(line_from_nodes, axis=1)

    for col in ["roadname", "semantic"]:
        out[col] = out[col].astype("string").fillna("")
    out["location"] = pd.to_numeric(out["location"], errors="coerce")

    return out[EDGE_COLS].reset_index(drop=True)


def parse_speed_datetime(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "timestamp" in out.columns:
        out["dt"] = pd.to_datetime(out["timestamp"], errors="coerce")
    else:
        out["hour_int"] = pd.to_numeric(out["hour"], errors="coerce").round().astype("Int64")
        out["dt"] = pd.to_datetime(out["hour_int"].astype(str), format="%Y%m%d%H%M", errors="coerce")
    out["hour_of_day"] = out["dt"].dt.hour
    out["is_weekday"] = out["dt"].dt.weekday < 5
    out["is_am_peak"] = (out["hour_of_day"] >= 7) & (out["hour_of_day"] < 9)
    out["is_pm_peak"] = (out["hour_of_day"] >= 17) & (out["hour_of_day"] < 19)
    out["is_freeflow_2205"] = (out["hour_of_day"] >= 22) | (out["hour_of_day"] <= 5)
    return out


def harmonic_table(df: pd.DataFrame, mask: pd.Series, speed_col: str) -> pd.DataFrame:
    local = df.loc[mask, ["roadseg_id", "speed"]].copy()
    local["speed"] = pd.to_numeric(local["speed"], errors="coerce")
    local = local[local["roadseg_id"].notna() & local["speed"].gt(0)]
    if local.empty:
        return pd.DataFrame(columns=["roadseg_id", speed_col, f"n_obs_{speed_col}"])
    grp = local.groupby("roadseg_id", as_index=False).agg(
        obs_n=("speed", "size"),
        inv_speed_sum=("speed", lambda s: float(np.sum(1.0 / s.to_numpy(dtype=float)))),
    )
    grp[speed_col] = grp["obs_n"] / grp["inv_speed_sum"]
    grp[f"n_obs_{speed_col}"] = grp["obs_n"].astype(int)
    return grp[["roadseg_id", speed_col, f"n_obs_{speed_col}"]]


def build_speed_tables(processed_dir: Path) -> dict[str, pd.DataFrame]:
    frames = []
    panel_path = processed_dir / "speed_panel_isolated.parquet"
    manual_path = processed_dir / "manual_edge_speeds.csv"

    if panel_path.exists():
        panel = pd.read_parquet(panel_path, columns=["road_id", "timestamp", "speed_kmh"])
        panel = panel.rename(columns={"road_id": "roadseg_id", "speed_kmh": "speed"})
        frames.append(panel)
    if manual_path.exists():
        manual = pd.read_csv(manual_path, usecols=["road_id", "timestamp", "speed_kmh"])
        manual = manual.rename(columns={"road_id": "roadseg_id", "speed_kmh": "speed"})
        frames.append(manual)
    if not frames:
        raise FileNotFoundError("No v2 speed files found.")

    speed = pd.concat(frames, ignore_index=True)
    speed["roadseg_id"] = speed["roadseg_id"].astype("string")
    speed["speed"] = pd.to_numeric(speed["speed"], errors="coerce")
    speed = parse_speed_datetime(speed)
    speed = speed[speed["roadseg_id"].notna() & speed["speed"].gt(0) & speed["dt"].notna()].copy()
    weekday = speed["is_weekday"].fillna(False)

    return {
        "am": harmonic_table(speed, weekday & speed["is_am_peak"].fillna(False), "speed_am_kmh"),
        "pm": harmonic_table(speed, weekday & speed["is_pm_peak"].fillna(False), "speed_pm_kmh"),
        "ff": harmonic_table(speed, speed["is_freeflow_2205"].fillna(False), "speed_ff_kmh"),
        "all": harmonic_table(speed, pd.Series(True, index=speed.index), "speed_all_kmh"),
    }


def write_derived_tables(
    processed_dir: Path, nodes: pd.DataFrame, edges: pd.DataFrame, version_label: str = "v2"
) -> None:
    road_list = edges[["road_id", "roadname", "semantic", "location", "geometry"]].drop_duplicates("road_id")
    road_list.to_excel(processed_dir / "road_list.xlsx", index=False)

    endpoints = edges[
        [
            "road_id",
            "from_node_id",
            "to_node_id",
            "start_lon",
            "start_lat",
            "end_lon",
            "end_lat",
            "start_x_m",
            "start_y_m",
            "end_x_m",
            "end_y_m",
        ]
    ].rename(columns={"from_node_id": "start_node_id", "to_node_id": "end_node_id"})
    endpoints.to_excel(processed_dir / "segment_endpoints_nodes.xlsx", index=False)

    speed_dir = processed_dir / "speed_cache"
    speed_dir.mkdir(parents=True, exist_ok=True)
    speed_tables = build_speed_tables(processed_dir)
    speed_tables["am"].to_csv(speed_dir / "raw_connected_speed_am_by_roadseg.csv", index=False)
    speed_tables["pm"].to_csv(speed_dir / "raw_connected_speed_pm_by_roadseg.csv", index=False)
    speed_tables["ff"].to_csv(speed_dir / "raw_connected_speed_ff_by_roadseg.csv", index=False)
    speed_tables["all"].to_csv(speed_dir / "raw_connected_speed_all_by_roadseg.csv", index=False)

    edge_ids = set(edges["road_id"].dropna().astype(str))
    speed_ids = set(speed_tables["all"]["roadseg_id"].dropna().astype(str))
    pd.DataFrame({"road_id": sorted(edge_ids - speed_ids)}).to_csv(
        processed_dir / "speed_cache" / f"raw_connected_{version_label}_edges_without_speed.csv",
        index=False,
    )


def main() -> None:
    args = parse_args()
    processed_dir = Path(args.processed_dir)
    validate_input_schema(processed_dir)
    backup_dir = backup_inputs(processed_dir, args.backup_dir_name)

    nodes = normalize_nodes(pd.read_excel(processed_dir / "nodes.xlsx"))
    input_edges = pd.read_excel(processed_dir / "directed_edges.xlsx")
    target_road_ids = set(input_edges["road_id"].dropna().astype(str))
    raw_geometry = load_raw_geometry_map(Path(args.raw_speed_path), target_road_ids, args.speed_chunksize)
    edges = normalize_edges(input_edges, nodes, raw_geometry)

    nodes.to_excel(processed_dir / "nodes.xlsx", index=False)
    edges.to_excel(processed_dir / "directed_edges.xlsx", index=False)
    write_derived_tables(processed_dir, nodes, edges, version_label=args.version_label)

    report = {
        "processed_dir": str(processed_dir),
        "version_label": args.version_label,
        "backup_dir": str(backup_dir),
        "nodes_rows": int(len(nodes)),
        "edges_rows": int(len(edges)),
        "edge_geometry_missing": int(edges["geometry"].isna().sum()),
        "raw_geometry_matched_edges": int(edges["road_id"].astype(str).isin(raw_geometry).sum()),
        "node_line_geometry_edges": int((~edges["road_id"].astype(str).isin(raw_geometry)).sum()),
    }
    print(report)


if __name__ == "__main__":
    main()
