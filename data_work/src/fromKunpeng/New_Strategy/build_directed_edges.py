import argparse
from pathlib import Path

import pandas as pd


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--road_list",
        default=str(Path(__file__).resolve().parent / "Processed_Data" / "road_list.xlsx"),
        help="Input road_list.xlsx (default: New_Strategy/Processed_Data/road_list.xlsx)",
    )
    parser.add_argument(
        "--segment_nodes",
        default=str(Path(__file__).resolve().parent / "Processed_Data" / "segment_endpoints_nodes.xlsx"),
        help="Input segment_endpoints_nodes.xlsx (default: New_Strategy/Processed_Data/segment_endpoints_nodes.xlsx)",
    )
    parser.add_argument(
        "--output",
        default=str(Path(__file__).resolve().parent / "Processed_Data" / "directed_edges.xlsx"),
        help="Output directed_edges.xlsx",
    )
    parser.add_argument(
        "--keep_all_edges",
        action="store_true",
        help="Keep multi-edges if multiple road_ids share the same (from_node_id,to_node_id). Default keeps all roads anyway.",
    )
    args = parser.parse_args()

    road_list_path = Path(args.road_list)
    seg_nodes_path = Path(args.segment_nodes)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    road_df = pd.read_excel(road_list_path)
    seg_df = pd.read_excel(seg_nodes_path)

    need_road_cols = ["road_id", "roadname", "semantic", "location", "geometry"]
    need_seg_cols = [
        "road_id",
        "start_node_id",
        "end_node_id",
        "start_lon",
        "start_lat",
        "end_lon",
        "end_lat",
        "start_x_m",
        "start_y_m",
        "end_x_m",
        "end_y_m",
    ]

    for c in need_road_cols:
        if c not in road_df.columns:
            raise KeyError(f"Missing column in road_list.xlsx: {c}")
    for c in need_seg_cols:
        if c not in seg_df.columns:
            raise KeyError(f"Missing column in segment_endpoints_nodes.xlsx: {c}")

    # Ensure join key types match
    road_df["road_id"] = road_df["road_id"].astype(str)
    seg_df["road_id"] = seg_df["road_id"].astype(str)

    merged = seg_df.merge(road_df, on="road_id", how="left", validate="one_to_one")

    # Edge table: one directed edge per road_id
    # Using road_id as edge_id to keep traceability.
    edges = merged.rename(
        columns={
            "start_node_id": "from_node_id",
            "end_node_id": "to_node_id",
        }
    )
    edges.insert(0, "edge_id", edges["road_id"])

    # Select useful columns
    edges_out = edges[
        [
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
    ]

    # Optional: if user wants a simple edge set (dedup by node pair),
    # t can do post-processing later. Here we keep all by default
    #→ road_id usually represents a distinct directed segment.
    if not args.keep_all_edges:
        edges_out = edges_out.sort_values("road_id")

    edges_out.to_excel(out_path, index=False)
    print(f"Directed edges: {len(edges_out)} -> {out_path}")


if __name__ == "__main__":
    main()

