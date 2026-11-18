from __future__ import annotations

import json
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.lines import Line2D
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import shortest_path
from shapely.geometry import LineString, Point


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DATA_ROOT = PROJECT_ROOT / "data_work" / "outputs" / "raw_connected_v1_grid_travel_time_edgeproj_v2"
BASELINE_ROOT = PROJECT_ROOT / "data_work" / "outputs" / "manual_centerline_rules_v11"
OUTPUT_ROOT = PROJECT_ROOT / "data_work" / "outputs" / "link_time_construction_example_v1"
SLIDE_FIG_ROOT = PROJECT_ROOT / "Documents" / "PreSlides" / "figs"

HOME_GRID = "sq_22_41"
WORK_GRID = "sq_22_40"


def cut_line(line: LineString, frac_start: float, frac_end: float) -> LineString:
    fa = float(np.clip(frac_start, 0.0, 1.0))
    fb = float(np.clip(frac_end, 0.0, 1.0))
    if fb < fa:
        fa, fb = fb, fa
    start_d = line.length * fa
    end_d = line.length * fb
    coords = [line.interpolate(start_d).coords[0]]
    for coord in line.coords:
        distance = line.project(Point(coord))
        if start_d < distance < end_d:
            coords.append(coord)
    coords.append(line.interpolate(end_d).coords[0])
    return LineString(coords)


def load_inputs() -> tuple[gpd.GeoDataFrame, pd.DataFrame, gpd.GeoDataFrame]:
    grid = gpd.read_parquet(BASELINE_ROOT / "data" / "grid_square_master.parquet")[["grid_id", "geometry"]].copy()
    grid["grid_id"] = grid["grid_id"].astype("string")
    grid = grid.to_crs(3857)

    connectors = pd.read_csv(DATA_ROOT / "data" / "raw_connected_grid_connectors_adjacent_square.csv")
    connectors["grid_id"] = connectors["grid_id"].astype("string")

    roads = pd.read_csv(DATA_ROOT / "data" / "raw_connected_road_edges_AM.csv")
    geometry = gpd.GeoSeries.from_wkt(roads["geometry"], crs="EPSG:3857", on_invalid="ignore")
    valid = geometry.notna() & (~geometry.is_empty)
    roads = gpd.GeoDataFrame(roads.loc[valid].copy(), geometry=geometry.loc[valid], crs=3857)
    for column in ["from_node_id", "to_node_id", "length_m", "speed_kmh", "travel_time_min"]:
        roads[column] = pd.to_numeric(roads[column], errors="coerce")
    return grid, connectors, roads


def reconstruct_actual_path(
    connectors: pd.DataFrame,
    roads: gpd.GeoDataFrame,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, float]:
    road_node_ids = sorted(
        pd.unique(pd.concat([roads["from_node_id"], roads["to_node_id"]], ignore_index=True)).tolist()
    )
    node_map = {int(node_id): idx for idx, node_id in enumerate(road_node_ids)}
    idx_to_node = {idx: node_id for node_id, idx in node_map.items()}
    n_road = len(node_map)
    home_idx = n_road
    work_idx = n_road + 1

    edge_time = roads.set_index("edge_id")["travel_time_min"].to_dict()
    edge_length = roads.set_index("edge_id")["length_m"].to_dict()
    edge_speed = roads.set_index("edge_id")["speed_kmh"].to_dict()
    home = connectors.loc[connectors["grid_id"].eq(HOME_GRID)].copy()
    work = connectors.loc[connectors["grid_id"].eq(WORK_GRID)].copy()
    for frame in [home, work]:
        frame["host_time_min"] = frame["host_edge_id"].map(edge_time)
        frame["host_length_m"] = frame["host_edge_id"].map(edge_length)
        frame["host_speed_kmh_period"] = frame["host_edge_id"].map(edge_speed)
        frame["connector_time_period_min"] = (
            60.0 * (frame["connector_dist_m"] / 1000.0) / frame["host_speed_kmh_period"]
        )
    home["augmented_time_min"] = home["connector_time_period_min"] + home["host_time_min"] * (1.0 - home["access_fraction"])
    work["augmented_time_min"] = work["host_time_min"] * work["access_fraction"] + work["connector_time_period_min"]

    rows = np.concatenate(
        [
            roads["from_node_id"].map(node_map).to_numpy(dtype=int),
            np.full(len(home), home_idx, dtype=int),
            work["from_node_id"].map(node_map).to_numpy(dtype=int),
        ]
    )
    cols = np.concatenate(
        [
            roads["to_node_id"].map(node_map).to_numpy(dtype=int),
            home["to_node_id"].map(node_map).to_numpy(dtype=int),
            np.full(len(work), work_idx, dtype=int),
        ]
    )
    weights = np.concatenate(
        [
            roads["travel_time_min"].to_numpy(dtype=float),
            home["augmented_time_min"].to_numpy(dtype=float),
            work["augmented_time_min"].to_numpy(dtype=float),
        ]
    )
    graph = coo_matrix((weights, (rows, cols)), shape=(n_road + 2, n_road + 2)).tocsr()
    distance, predecessor = shortest_path(
        graph,
        directed=True,
        indices=[home_idx],
        return_predecessors=True,
        unweighted=False,
        method="D",
    )
    total_time = float(distance[0, work_idx])
    if not np.isfinite(total_time):
        raise RuntimeError("Selected grid pair has no finite directed path.")

    path = [work_idx]
    current = work_idx
    while current != home_idx:
        current = int(predecessor[0, current])
        if current < 0:
            raise RuntimeError("Could not reconstruct the selected directed path.")
        path.append(current)
    path.reverse()

    road_lookup = roads.set_index(["from_node_id", "to_node_id"], drop=False)
    segment_rows: list[dict[str, object]] = []
    connector_rows: list[dict[str, object]] = []
    for start, end in zip(path[:-1], path[1:]):
        if start == home_idx:
            matches = home.loc[home["to_node_id"].map(node_map).eq(end)].copy()
            if len(matches) != 1:
                raise RuntimeError("The selected home access arc is not unique.")
            row = matches.iloc[0]
            host = roads.loc[roads["edge_id"].eq(row["host_edge_id"])].iloc[0]
            road_time = float(row["host_time_min"]) * (1.0 - float(row["access_fraction"]))
            road_length = float(row["host_length_m"]) * (1.0 - float(row["access_fraction"]))
            segment_rows.append(
                {
                    "edge_id": row["host_edge_id"],
                    "roadname": host.get("roadname", ""),
                    "length_m": road_length,
                    "speed_kmh": float(host["speed_kmh"]),
                    "time_min": road_time,
                    "geometry": cut_line(host.geometry, float(row["access_fraction"]), 1.0),
                }
            )
            connector_rows.append(
                {
                    "side": "origin",
                    "time_min": float(row["connector_time_period_min"]),
                    "length_m": float(row["connector_dist_m"]),
                    "geometry": LineString(
                        [(float(row["cx"]), float(row["cy"])), (float(row["access_x_m"]), float(row["access_y_m"]))]
                    ),
                }
            )
        elif end == work_idx:
            start_node = int(idx_to_node[start])
            matches = work.loc[work["from_node_id"].eq(start_node)].copy()
            if len(matches) != 1:
                raise RuntimeError("The selected destination access arc is not unique.")
            row = matches.iloc[0]
            host = roads.loc[roads["edge_id"].eq(row["host_edge_id"])].iloc[0]
            road_time = float(row["host_time_min"]) * float(row["access_fraction"])
            road_length = float(row["host_length_m"]) * float(row["access_fraction"])
            segment_rows.append(
                {
                    "edge_id": row["host_edge_id"],
                    "roadname": host.get("roadname", ""),
                    "length_m": road_length,
                    "speed_kmh": float(host["speed_kmh"]),
                    "time_min": road_time,
                    "geometry": cut_line(host.geometry, 0.0, float(row["access_fraction"])),
                }
            )
            connector_rows.append(
                {
                    "side": "destination",
                    "time_min": float(row["connector_time_period_min"]),
                    "length_m": float(row["connector_dist_m"]),
                    "geometry": LineString(
                        [(float(row["access_x_m"]), float(row["access_y_m"])), (float(row["cx"]), float(row["cy"]))]
                    ),
                }
            )
        else:
            start_node = int(idx_to_node[start])
            end_node = int(idx_to_node[end])
            matches = road_lookup.loc[(start_node, end_node)]
            if isinstance(matches, pd.DataFrame):
                matches = matches.loc[np.isclose(matches["travel_time_min"], float(graph[start, end]))]
                if len(matches) != 1:
                    raise RuntimeError("A selected road arc cannot be mapped uniquely to a raw edge.")
                road = matches.iloc[0]
            else:
                road = matches
            segment_rows.append(
                {
                    "edge_id": road["edge_id"],
                    "roadname": road.get("roadname", ""),
                    "length_m": float(road["length_m"]),
                    "speed_kmh": float(road["speed_kmh"]),
                    "time_min": float(road["travel_time_min"]),
                    "geometry": road.geometry,
                }
            )

    segments = gpd.GeoDataFrame(segment_rows, geometry="geometry", crs=3857)
    segments.insert(0, "segment", [f"a{i}" for i in range(1, len(segments) + 1)])
    access = gpd.GeoDataFrame(connector_rows, geometry="geometry", crs=3857)
    reconstructed = float(segments["time_min"].sum() + access["time_min"].sum())
    if not np.isclose(reconstructed, total_time, atol=1e-9):
        raise RuntimeError(f"Segment decomposition {reconstructed} does not match route total {total_time}.")
    return segments, access, total_time


def plot_figure(
    grid: gpd.GeoDataFrame,
    roads: gpd.GeoDataFrame,
    segments: gpd.GeoDataFrame,
    access: gpd.GeoDataFrame,
    total_time: float,
    output_path: Path,
) -> None:
    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "font.size": 9,
            "axes.linewidth": 0.8,
            "figure.facecolor": "white",
            "axes.facecolor": "white",
        }
    )
    fig = plt.figure(figsize=(6.2, 6.6), dpi=300)
    gs = fig.add_gridspec(2, 1, height_ratios=[4.7, 1.55], hspace=0.03)
    ax = fig.add_subplot(gs[0])
    ax_sum = fig.add_subplot(gs[1])

    pair = grid.loc[grid["grid_id"].isin([HOME_GRID, WORK_GRID])].copy()
    window = pair.geometry.union_all().buffer(700)
    local_roads = roads.loc[roads.geometry.intersects(window)].copy()
    local_roads.plot(ax=ax, color="#cfd3d7", linewidth=0.42, alpha=0.92, rasterized=True, zorder=1)

    pair_colors = {HOME_GRID: "#f8e8d8", WORK_GRID: "#dfeef7"}
    for row in pair.itertuples(index=False):
        gpd.GeoSeries([row.geometry], crs=3857).plot(
            ax=ax,
            color=pair_colors[str(row.grid_id)],
            edgecolor="#6f7478",
            linewidth=1.15,
            alpha=0.50,
            zorder=2,
        )

    segment_colors = ["#2f6f9f", "#1f5f8b", "#174f78", "#103f65", "#0a3153"]
    for idx, row in segments.iterrows():
        color = segment_colors[min(idx, len(segment_colors) - 1)]
        gpd.GeoSeries([row.geometry], crs=3857).plot(ax=ax, color="white", linewidth=5.2, zorder=4)
        gpd.GeoSeries([row.geometry], crs=3857).plot(ax=ax, color=color, linewidth=3.15, zorder=5)
        midpoint = row.geometry.interpolate(0.50, normalized=True)
        ax.annotate(
            rf"$a_{{{idx + 1}}}$",
            (midpoint.x, midpoint.y),
            xytext=(0, 10 if idx % 2 == 0 else -13),
            textcoords="offset points",
            ha="center",
            va="center",
            fontsize=8.2,
            color="#17324a",
            bbox={"boxstyle": "round,pad=0.15", "fc": "white", "ec": "none", "alpha": 0.92},
            zorder=8,
        )

    access.plot(ax=ax, color="#d46b36", linewidth=2.0, linestyle=(0, (4, 2)), zorder=6)
    centers = pair.set_index("grid_id").geometry.centroid
    start = centers.loc[HOME_GRID]
    end = centers.loc[WORK_GRID]
    ax.scatter([start.x], [start.y], s=42, color="#c75928", edgecolor="white", linewidth=0.8, zorder=9)
    ax.scatter([end.x], [end.y], s=58, marker="*", color="#c75928", edgecolor="white", linewidth=0.8, zorder=9)
    ax.annotate("origin grid $k$", (start.x, start.y), xytext=(8, -18), textcoords="offset points", fontsize=8.2, color="#3c4145")
    ax.annotate("neighbor grid $l$", (end.x, end.y), xytext=(-8, -18), textcoords="offset points", ha="right", fontsize=8.2, color="#3c4145")

    middle = segments.iloc[len(segments) // 2].geometry
    p0 = middle.interpolate(0.35, normalized=True)
    p1 = middle.interpolate(0.65, normalized=True)
    ax.annotate(
        "",
        xy=(p1.x, p1.y),
        xytext=(p0.x, p0.y),
        arrowprops={"arrowstyle": "-|>", "color": "#0a3153", "lw": 1.8, "mutation_scale": 12},
        zorder=9,
    )

    xmin, ymin, xmax, ymax = pair.total_bounds
    ax.set_xlim(xmin - 480, xmax + 480)
    ax.set_ylim(ymin - 370, ymax + 370)
    ax.set_aspect("equal")
    ax.set_axis_off()
    ax.text(
        0.02,
        0.98,
        "Actual Beijing pair: shortest observed-AM path",
        transform=ax.transAxes,
        ha="left",
        va="top",
        fontsize=9.2,
        color="#20262b",
        weight="bold",
        bbox={"boxstyle": "round,pad=0.25", "fc": "white", "ec": "#d5d8da", "lw": 0.6, "alpha": 0.96},
    )
    ax.legend(
        handles=[
            Line2D([0], [0], color="#174f78", lw=3.0, label="selected raw-road segments"),
            Line2D([0], [0], color="#d46b36", lw=2.0, ls=(0, (4, 2)), label="endpoint access"),
        ],
        loc="lower center",
        bbox_to_anchor=(0.50, -0.015),
        ncol=2,
        frameon=False,
        fontsize=7.5,
        handlelength=2.5,
        columnspacing=1.2,
    )

    ax_sum.set_xlim(0, 1)
    ax_sum.set_ylim(0, 1)
    ax_sum.set_axis_off()
    road_times = segments["time_min"].to_numpy(dtype=float)
    access_times = access["time_min"].to_numpy(dtype=float)
    road_expression = " + ".join(f"{value:.2f}" for value in road_times)
    road_total = float(road_times.sum())
    access_total = float(access_times.sum())
    ax_sum.text(0.02, 0.80, "Raw-road segments", fontsize=8.6, weight="bold", color="#17324a", va="center")
    ax_sum.text(0.02, 0.58, rf"$a_1+\cdots+a_5$:  {road_expression} $\approx$ {road_total:.2f} min", fontsize=8.2, color="#26343f", va="center")
    ax_sum.text(0.02, 0.34, f"Endpoint access:  {access_times[0]:.2f} + {access_times[1]:.2f} = {access_total:.2f} min", fontsize=8.2, color="#8f4523", va="center")
    ax_sum.text(
        0.98,
        0.08,
        rf"$m_{{kl}}^{{obs}}={road_total:.2f}+{access_total:.2f}={total_time:.2f}$ min",
        ha="right",
        va="bottom",
        fontsize=10.2,
        color="white",
        weight="bold",
        bbox={"boxstyle": "round,pad=0.32", "fc": "#174f78", "ec": "none"},
    )
    ax_sum.plot([0.02, 0.98], [0.97, 0.97], color="#d8dbde", lw=0.8)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=300, bbox_inches="tight", pad_inches=0.03)
    fig.savefig(output_path.with_suffix(".pdf"), bbox_inches="tight", pad_inches=0.03)
    plt.close(fig)


def main() -> None:
    grid, connectors, roads = load_inputs()
    segments, access, total_time = reconstruct_actual_path(connectors, roads)
    pair_output = pd.read_csv(DATA_ROOT / "data" / "raw_connected_vs_old_adjacent_adjacent_square_AM.csv")
    pair_row = pair_output.loc[
        pair_output["home_grid"].eq(HOME_GRID) & pair_output["work_grid"].eq(WORK_GRID)
    ].squeeze()
    production_total = float(pair_row["raw_connected_travel_time_min"])
    if not np.isclose(total_time, production_total, atol=1e-9):
        raise RuntimeError(
            f"Reconstructed AM total {total_time} does not match production pair output {production_total}."
        )
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    SLIDE_FIG_ROOT.mkdir(parents=True, exist_ok=True)

    segments.drop(columns="geometry").to_csv(OUTPUT_ROOT / "raw_segment_time_breakdown.csv", index=False)
    access.drop(columns="geometry").to_csv(OUTPUT_ROOT / "endpoint_access_time_breakdown.csv", index=False)
    metadata = {
        "source_output": str(DATA_ROOT.relative_to(PROJECT_ROOT)),
        "grid_source": str((BASELINE_ROOT / "data" / "grid_square_master.parquet").relative_to(PROJECT_ROOT)),
        "period": "AM",
        "home_grid": HOME_GRID,
        "work_grid": WORK_GRID,
        "n_raw_road_segments": int(len(segments)),
        "raw_road_time_min": float(segments["time_min"].sum()),
        "endpoint_access_time_min": float(access["time_min"].sum()),
        "total_physical_time_min": total_time,
        "production_pair_output_min": production_total,
        "connector_time_rule": "connector distance divided by the AM speed of its host road edge; the unsuffixed connector_time_min column is not reused",
        "route_reconstruction": "pair-specific Dijkstra on the directed raw-road graph with endpoint access arcs",
    }
    (OUTPUT_ROOT / "metadata.json").write_text(json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8")

    output_path = SLIDE_FIG_ROOT / "link_time_construction_actual.png"
    plot_figure(grid, roads, segments, access, total_time, output_path)
    print(json.dumps(metadata, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
