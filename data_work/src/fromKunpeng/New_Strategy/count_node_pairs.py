import pandas as pd
import numpy as np
from pathlib import Path


def main():
    base_dir = Path(__file__).resolve().parent
    edges_path = base_dir / "Processed_Data" / "directed_edges.xlsx"

    df = pd.read_excel(edges_path)
    df["from_node_id"] = df["from_node_id"].astype(int)
    df["to_node_id"] = df["to_node_id"].astype(int)

    a = np.minimum(df["from_node_id"].values, df["to_node_id"].values)
    b = np.maximum(df["from_node_id"].values, df["to_node_id"].values)

    from_vals = df["from_node_id"].values
    to_vals = df["to_node_id"].values

    has_ab = (from_vals == a) & (to_vals == b)  # directed edge a -> b
    has_ba = (from_vals == b) & (to_vals == a)  # directed edge b -> a

    temp = pd.DataFrame({"a": a, "b": b, "has_ab": has_ab, "has_ba": has_ba})
    g = temp.groupby(["a", "b"], sort=False).max()

    bid = int((g["has_ab"] & g["has_ba"]).sum())  # both directions exist
    uni = int((g["has_ab"] ^ g["has_ba"]).sum())  # exactly one direction exists
    total = int(g.shape[0])  # unordered pairs that have at least one direction

    print(f"total_unordered_pairs={total}")
    print(f"bidirectional_pairs={bid}")
    print(f"unidirectional_pairs={uni}")


if __name__ == "__main__":
    main()

