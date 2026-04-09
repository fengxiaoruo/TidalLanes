import argparse
import re
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
from pyproj import Transformer


FLOAT_RE = re.compile(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?")
# Match coordinate tuples in WKT, supporting 2D "x y" or 3D "x y z".
COORD_TUPLE_RE = re.compile(
    r"([-+]?\d*\.?\d+(?:[eE][-+]?\d+)?)\s+([-+]?\d*\.?\d+(?:[eE][-+]?\d+)?)(?:\s+([-+]?\d*\.?\d+(?:[eE][-+]?\d+)?))?"
)


def parse_linestring_endpoints_wkt(linestring_wkt: str) -> tuple[float, float, float, float]:
    """
    Parse WKT LINESTRING and return (start_lon, start_lat, end_lon, end_lat).
    Geometry is expected to be like: LINESTRING (lon lat, lon lat, ...)
    """
    if not isinstance(linestring_wkt, str):
        raise ValueError("geometry must be a WKT string")

    # Extract coordinate tuples and take first/last tuple's (x,y).
    # If geometry contains Z, we ignore it.
    tuples = COORD_TUPLE_RE.findall(linestring_wkt)
    if len(tuples) < 2:
        raise ValueError(f"Unexpected LINESTRING coordinate tuples: {linestring_wkt[:120]}...")

    first = tuples[0]
    last = tuples[-1]

    start_lon = float(first[0])
    start_lat = float(first[1])
    end_lon = float(last[0])
    end_lat = float(last[1])
    return start_lon, start_lat, end_lon, end_lat


@dataclass
class UnionFind:
    parent: list[int]
    size: list[int]

    @classmethod
    def create(cls, n: int) -> "UnionFind":
        return cls(parent=list(range(n)), size=[1] * n)

    def find(self, x: int) -> int:
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a: int, b: int) -> None:
        ra = self.find(a)
        rb = self.find(b)
        if ra == rb:
            return
        if self.size[ra] < self.size[rb]:
            ra, rb = rb, ra
        self.parent[rb] = ra
        self.size[ra] += self.size[rb]


def cluster_endpoints_union_find(points_xy: np.ndarray, eps_m: float) -> np.ndarray:
    """
    Cluster points based on distance threshold using connected components.

    Approach:
      1) Build KD-tree
      2) Find all pairs within eps_m
      3) Union-find connected components
    """
    try:
        from scipy.spatial import cKDTree  # type: ignore

        tree = cKDTree(points_xy)
        pairs = tree.query_pairs(r=eps_m)
        uf = UnionFind.create(points_xy.shape[0])
        for i, j in pairs:
            uf.union(int(i), int(j))

        roots = np.fromiter((uf.find(i) for i in range(points_xy.shape[0])), dtype=int)

    except Exception:
        # Fallback to sklearn DBSCAN (slower / more memory, but robust)
        from sklearn.cluster import DBSCAN  # type: ignore

        clustering = DBSCAN(eps=eps_m, min_samples=1, metric="euclidean")
        roots = clustering.fit_predict(points_xy)

        # Normalize labels to 0..K-1
        uniq = np.unique(roots)
        remap = {old: new for new, old in enumerate(uniq)}
        roots = np.array([remap[int(x)] for x in roots], dtype=int)

        return roots

    # Map arbitrary root ids to 0..K-1
    uniq_roots = np.unique(roots)
    root_to_node = {int(r): int(i) for i, r in enumerate(uniq_roots)}
    node_ids = np.array([root_to_node[int(r)] for r in roots], dtype=int)
    return node_ids


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        default=str(Path(__file__).resolve().parent / "Processed_Data" / "road_list.xlsx"),
        help="Input road_list.xlsx (default: New_Strategy/Processed_Data/road_list.xlsx)",
    )
    parser.add_argument(
        "--output_dir",
        default=str(Path(__file__).resolve().parent / "Processed_Data"),
        help="Output directory (default: New_Strategy/Processed_Data)",
    )
    parser.add_argument(
        "--eps_m",
        type=float,
        default=15.0,
        help="Distance threshold in meters for endpoint clustering (default: 15m)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="For quick testing: only process first N road segments",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_excel(input_path)
    required = ["road_id", "geometry"]
    for c in required:
        if c not in df.columns:
            raise KeyError(f"Missing column: {c} in {input_path}")

    if args.limit is not None:
        df = df.head(args.limit).copy()

    n = len(df)
    if n == 0:
        raise ValueError("road_list.xlsx is empty")

    # Parse endpoints in lon/lat (EPSG:4326)
    start_lon = np.empty(n, dtype=float)
    start_lat = np.empty(n, dtype=float)
    end_lon = np.empty(n, dtype=float)
    end_lat = np.empty(n, dtype=float)

    for i, geom in enumerate(df["geometry"].tolist()):
        slon, slat, elon, elat = parse_linestring_endpoints_wkt(geom)
        start_lon[i] = slon
        start_lat[i] = slat
        end_lon[i] = elon
        end_lat[i] = elat

    # Project endpoints to meters for clustering
    transformer_fwd = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
    start_x, start_y = transformer_fwd.transform(start_lon, start_lat)
    end_x, end_y = transformer_fwd.transform(end_lon, end_lat)

    # Stack all endpoints: [start0, start1, ..., startN-1, end0, ..., endN-1]
    points_xy = np.vstack(
        [
            np.column_stack([start_x, start_y]),
            np.column_stack([end_x, end_y]),
        ]
    )  # shape: (2N, 2)

    # Cluster endpoints
    endpoint_node_ids = cluster_endpoints_union_find(points_xy, eps_m=args.eps_m)
    if endpoint_node_ids.shape[0] != 2 * n:
        raise RuntimeError("Clustering result has unexpected length")

    start_node_ids = endpoint_node_ids[:n]
    end_node_ids = endpoint_node_ids[n:]

    # Build node table (use mean lon/lat/x/y for representative node coordinate)
    transformer_inv = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)
    all_lon = np.concatenate([start_lon, end_lon])
    all_lat = np.concatenate([start_lat, end_lat])
    all_x = points_xy[:, 0]
    all_y = points_xy[:, 1]

    node_count = int(endpoint_node_ids.max()) + 1
    node_rows = []
    for node_id in range(node_count):
        mask = endpoint_node_ids == node_id
        cnt = int(mask.sum())
        lon_mean = float(all_lon[mask].mean())
        lat_mean = float(all_lat[mask].mean())
        x_mean = float(all_x[mask].mean())
        y_mean = float(all_y[mask].mean())

        # Use inverse projection just in case (x/y mean -> lon/lat mean should match closely)
        # Keeping it explicit for clarity.
        lon_rep, lat_rep = transformer_inv.transform(x_mean, y_mean)
        node_rows.append(
            {
                "node_id": node_id,
                "lon": lon_rep,
                "lat": lat_rep,
                "x_m": x_mean,
                "y_m": y_mean,
                "n_endpoints": cnt,
            }
        )

    nodes_df = pd.DataFrame(node_rows).sort_values("node_id")
    nodes_xlsx = output_dir / "nodes.xlsx"
    nodes_df.to_excel(nodes_xlsx, index=False)

    # Segment -> node mapping 
    segment_rows = []
    road_ids = df["road_id"].tolist()
    for i in range(n):
        segment_rows.append(
            {
                "road_id": road_ids[i],
                "start_node_id": int(start_node_ids[i]),
                "end_node_id": int(end_node_ids[i]),
                "start_lon": float(start_lon[i]),
                "start_lat": float(start_lat[i]),
                "end_lon": float(end_lon[i]),
                "end_lat": float(end_lat[i]),
                "start_x_m": float(start_x[i]),
                "start_y_m": float(start_y[i]),
                "end_x_m": float(end_x[i]),
                "end_y_m": float(end_y[i]),
            }
        )

    seg_nodes_df = pd.DataFrame(segment_rows)
    seg_nodes_xlsx = output_dir / "segment_endpoints_nodes.xlsx"
    seg_nodes_df.to_excel(seg_nodes_xlsx, index=False)

    print(f"Nodes: {len(nodes_df)} -> {nodes_xlsx}")
    print(f"Segment endpoints -> nodes: {len(seg_nodes_df)} -> {seg_nodes_xlsx}")


if __name__ == "__main__":
    main()

