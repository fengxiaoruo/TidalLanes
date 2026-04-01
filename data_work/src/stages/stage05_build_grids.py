"""
Stage 05: Build Grids

Purpose:
- Build square, hex, and Voronoi grid systems
- Export stable grid geometry tables

Current source notebook:
- code/03_GridConstruct.ipynb
"""

import argparse
import json
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from scipy.spatial import Voronoi
from shapely.geometry import Point, Polygon


ROOT = Path(__file__).resolve().parents[2]
BOUNDARY_PATH = ROOT / "raw_data" / "gis" / "map" / "北京市边界.shp"


def parse_args():
    parser = argparse.ArgumentParser(description="Stage 05: Build grid systems")
    parser.add_argument("--config", default=None, help="Optional config file path.")
    parser.add_argument("--version-id", required=True, help="Version identifier for outputs.")
    parser.add_argument("--output-dir", default="outputs", help="Base output directory for versioned results.")
    return parser.parse_args()


def save_config_snapshot(version_root: Path, config_path: str | None):
    payload = {
        "stage": "stage05_build_grids",
        "config_path": config_path,
        "boundary_path": str(BOUNDARY_PATH),
        "square_cell_size_m": 3000,
        "hex_resolution": 7,
        "voronoi_min_seed_dist_m": 1500.0,
    }
    (version_root / "config_snapshot.stage05.json").write_text(
        json.dumps(payload, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )


def load_stage01_centerline(version_root: Path):
    path = version_root / "data" / "centerline_dir_master.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Missing stage01 output: {path}")
    cl_dir = gpd.read_parquet(path)
    if cl_dir.crs is None:
        raise ValueError("centerline_dir_master has no CRS.")
    return cl_dir


def build_study_area(cl_dir: gpd.GeoDataFrame):
    target_crs = "EPSG:3857"
    cl_dir_proj = cl_dir.to_crs(target_crs)
    if BOUNDARY_PATH.exists():
        boundary = gpd.read_file(BOUNDARY_PATH)
        if boundary.crs is None:
            boundary = boundary.set_crs("EPSG:4326")
        boundary_proj = boundary.to_crs(target_crs)
        study_area_proj = boundary_proj.geometry.unary_union
    else:
        study_area_proj = cl_dir_proj.geometry.unary_union.buffer(5000)
    return cl_dir_proj, study_area_proj


def create_square_grid(bounds, cell_size_m=3000, crs="EPSG:3857"):
    xmin, ymin, xmax, ymax = bounds
    cols = int(np.ceil((xmax - xmin) / cell_size_m))
    rows = int(np.ceil((ymax - ymin) / cell_size_m))

    grid_cells = []
    grid_ids = []
    for i in range(rows):
        for j in range(cols):
            x1 = xmin + j * cell_size_m
            y1 = ymin + i * cell_size_m
            x2 = x1 + cell_size_m
            y2 = y1 + cell_size_m
            cell = Polygon([(x1, y1), (x2, y1), (x2, y2), (x1, y2), (x1, y1)])
            grid_cells.append(cell)
            grid_ids.append(f"sq_{i}_{j}")

    return gpd.GeoDataFrame({"grid_id": grid_ids}, geometry=grid_cells, crs=crs)


def create_hex_grid_h3(boundary_gdf, resolution=7, target_crs="EPSG:3857"):
    import h3

    boundary_wgs84 = boundary_gdf.to_crs("EPSG:4326")
    boundary_geom = boundary_wgs84.geometry.iloc[0]
    h3_cells = set()

    try:
        _ = h3.latlng_to_cell(39.9, 116.4, resolution)
        use_new_api = True
    except AttributeError:
        use_new_api = False

    minx, miny, maxx, maxy = boundary_geom.bounds
    step = 0.01
    for lon in np.arange(minx, maxx, step):
        for lat in np.arange(miny, maxy, step):
            pt = Point(lon, lat)
            if boundary_geom.contains(pt):
                if use_new_api:
                    h3_index = h3.latlng_to_cell(lat, lon, resolution)
                else:
                    h3_index = h3.geo_to_h3(lat, lon, resolution)
                h3_cells.add(h3_index)

    hex_polys = []
    hex_ids = []
    for h3_index in h3_cells:
        try:
            if use_new_api:
                hex_boundary = h3.cell_to_boundary(h3_index)
            else:
                hex_boundary = h3.h3_to_geo_boundary(h3_index, geo_json=True)
        except Exception:
            try:
                hex_boundary = h3.cell_to_boundary(h3_index)
            except Exception:
                hex_boundary = h3.h3_to_geo_boundary(h3_index, geo_json=True)
        hex_poly = Polygon([(lon, lat) for lat, lon in hex_boundary])
        hex_polys.append(hex_poly)
        hex_ids.append(str(h3_index))

    hex_gdf = gpd.GeoDataFrame({"grid_id": hex_ids}, geometry=hex_polys, crs="EPSG:4326")
    hex_gdf_clipped = gpd.clip(hex_gdf, boundary_wgs84)
    hex_gdf_proj = hex_gdf_clipped.to_crs(target_crs)
    hex_gdf_proj["area_km2"] = hex_gdf_proj.geometry.area / 1e6
    hex_gdf_clipped["area_km2"] = hex_gdf_proj["area_km2"].values
    return hex_gdf_clipped


def build_voronoi_grid(cl_dir_proj, study_area_proj):
    SNAP_TOL = 5.0
    MIN_SEED_DIST = 1500.0
    DEG_THRESHOLD = 2
    BBOX_PAD = 20000.0
    A_MAX_KM2 = 200.0
    MAX_ITERS = 8
    ADD_PER_ITER = 300

    def _line_endpoints(geom):
        coords = list(geom.coords)
        return coords[0], coords[-1]

    endpoints = []
    for geom in cl_dir_proj.geometry.values:
        if geom is None or geom.is_empty:
            continue
        if geom.geom_type == "MultiLineString":
            for g in geom.geoms:
                if g.is_empty:
                    continue
                a, b = _line_endpoints(g)
                endpoints.append(a)
                endpoints.append(b)
        elif geom.geom_type == "LineString":
            a, b = _line_endpoints(geom)
            endpoints.append(a)
            endpoints.append(b)

    endpoints = np.asarray(endpoints)
    key = np.round(endpoints / SNAP_TOL).astype(np.int64)
    df_nodes = pd.DataFrame({"kx": key[:, 0], "ky": key[:, 1]})
    df_nodes["x"] = df_nodes["kx"] * SNAP_TOL
    df_nodes["y"] = df_nodes["ky"] * SNAP_TOL
    df_nodes = df_nodes.drop_duplicates(subset=["kx", "ky"]).reset_index(drop=True)

    nodes_gdf = gpd.GeoDataFrame(
        df_nodes,
        geometry=gpd.points_from_xy(df_nodes["x"], df_nodes["y"]),
        crs=cl_dir_proj.crs,
    )

    df_deg = pd.DataFrame({"kx": key[:, 0], "ky": key[:, 1]})
    deg = df_deg.value_counts().rename("deg").reset_index()
    nodes_gdf = nodes_gdf.merge(deg, on=["kx", "ky"], how="left")
    nodes_gdf["deg"] = nodes_gdf["deg"].fillna(0).astype(int)

    cand = nodes_gdf[nodes_gdf["deg"] >= DEG_THRESHOLD].copy()

    def poisson_greedy(points_xy, min_dist, random_state=0):
        rng = np.random.default_rng(random_state)
        idx = np.arange(points_xy.shape[0])
        rng.shuffle(idx)
        chosen = []
        md2 = min_dist ** 2
        for i in idx:
            p = points_xy[i]
            if chosen:
                C = points_xy[np.array(chosen)]
                d2 = np.sum((C - p) ** 2, axis=1)
                if np.min(d2) < md2:
                    continue
            chosen.append(i)
        mask = np.zeros(points_xy.shape[0], dtype=bool)
        mask[chosen] = True
        return mask

    pts = np.column_stack([cand.geometry.x.values, cand.geometry.y.values])
    mask = poisson_greedy(pts, MIN_SEED_DIST, random_state=42)
    seeds = cand.loc[mask].copy().reset_index(drop=True)

    def voronoi_finite_polygons_2d(vor, radius=None):
        if vor.points.shape[1] != 2:
            raise ValueError("Requires 2D input")
        new_regions = []
        new_vertices = vor.vertices.tolist()
        center = vor.points.mean(axis=0)
        if radius is None:
            radius = np.ptp(vor.points, axis=0).max() * 2
        all_ridges = {}
        for (p1, p2), (v1, v2) in zip(vor.ridge_points, vor.ridge_vertices):
            all_ridges.setdefault(p1, []).append((p2, v1, v2))
            all_ridges.setdefault(p2, []).append((p1, v1, v2))
        for p1, region_idx in enumerate(vor.point_region):
            vertices = vor.regions[region_idx]
            if all(v >= 0 for v in vertices):
                new_regions.append(vertices)
                continue
            ridges = all_ridges[p1]
            new_region = [v for v in vertices if v >= 0]
            for p2, v1, v2 in ridges:
                if v1 < 0 or v2 < 0:
                    v = v1 if v1 >= 0 else v2
                    t = vor.points[p2] - vor.points[p1]
                    t = t / np.linalg.norm(t)
                    n = np.array([-t[1], t[0]])
                    midpoint = (vor.points[p1] + vor.points[p2]) / 2
                    direction = np.sign(np.dot(midpoint - center, n)) * n
                    far_point = vor.vertices[v] + direction * radius
                    new_vertices.append(far_point.tolist())
                    new_region.append(len(new_vertices) - 1)
            vs = np.asarray([new_vertices[v] for v in new_region])
            c = vs.mean(axis=0)
            angles = np.arctan2(vs[:, 1] - c[1], vs[:, 0] - c[0])
            new_region = [v for _, v in sorted(zip(angles, new_region))]
            new_regions.append(new_region)
        return new_regions, np.asarray(new_vertices)

    def build_cells_from_seeds(seeds_gdf, study_area_proj_local, bbox_pad=BBOX_PAD):
        seed_xy = np.column_stack([seeds_gdf.geometry.x.values, seeds_gdf.geometry.y.values])
        minx, miny, maxx, maxy = study_area_proj_local.bounds
        bbox_pts = np.array(
            [
                [minx - bbox_pad, miny - bbox_pad],
                [minx - bbox_pad, maxy + bbox_pad],
                [maxx + bbox_pad, miny - bbox_pad],
                [maxx + bbox_pad, maxy + bbox_pad],
            ]
        )
        vor = Voronoi(np.vstack([seed_xy, bbox_pts]))
        regions, vertices = voronoi_finite_polygons_2d(vor, radius=1e7)

        polys = []
        for i in range(len(seed_xy)):
            region = regions[i]
            polys.append(Polygon(vertices[region]))

        cells = gpd.GeoDataFrame(
            {"cell_id": np.arange(len(polys), dtype=int), "seed_id": np.arange(len(polys), dtype=int)},
            geometry=polys,
            crs=seeds_gdf.crs,
        )
        cells["geometry"] = cells.geometry.buffer(0)
        mask_local = cells.geometry.intersects(study_area_proj_local)
        cells = cells[mask_local].copy()
        cells = gpd.clip(cells, gpd.GeoDataFrame(geometry=[study_area_proj_local], crs=cells.crs))
        cells = cells[~cells.geometry.is_empty].copy()
        cells["area_km2"] = cells.geometry.area / 1e6
        return cells

    def add_seeds_for_large_cells(cells, seeds_gdf, nodes_pool_gdf, a_max_km2, add_cap=200):
        large = cells[cells["area_km2"] > a_max_km2].sort_values("area_km2", ascending=False)
        if large.empty:
            return seeds_gdf, 0
        nodes_pool = nodes_pool_gdf
        sindex = nodes_pool.sindex
        new_pts = []
        added = 0
        existing_xy = set(zip(np.round(seeds_gdf.geometry.x.values, 3), np.round(seeds_gdf.geometry.y.values, 3)))

        for _, row in large.iterrows():
            if added >= add_cap:
                break
            cell_geom = row.geometry
            seed_pt = seeds_gdf.iloc[int(row["seed_id"])].geometry
            cand_idx = list(sindex.intersection(cell_geom.bounds))
            cand = nodes_pool.iloc[cand_idx]
            cand = cand[cand.geometry.within(cell_geom)]
            if len(cand) > 0:
                d = cand.geometry.distance(seed_pt)
                p_new = cand.loc[d.idxmax()].geometry
            else:
                p_new = cell_geom.representative_point()
            xy = (round(p_new.x, 3), round(p_new.y, 3))
            if xy in existing_xy:
                continue
            new_pts.append(p_new)
            existing_xy.add(xy)
            added += 1

        if added == 0:
            return seeds_gdf, 0

        new_seeds = gpd.GeoDataFrame({"deg": [0] * added}, geometry=new_pts, crs=seeds_gdf.crs)
        seeds2 = pd.concat([seeds_gdf[["deg", "geometry"]], new_seeds], ignore_index=True)
        seeds2 = seeds2.drop_duplicates(subset=["geometry"]).reset_index(drop=True)
        return seeds2, added

    nodes_in_area = nodes_gdf[nodes_gdf.geometry.within(study_area_proj)].copy()
    seeds_iter = seeds[["deg", "geometry"]].copy().reset_index(drop=True)
    cells = None

    for _ in range(1, MAX_ITERS + 1):
        cells_it = build_cells_from_seeds(seeds_iter, study_area_proj, bbox_pad=BBOX_PAD)
        if cells_it["area_km2"].max() <= A_MAX_KM2:
            cells = cells_it
            break
        seeds_iter, added = add_seeds_for_large_cells(
            cells_it, seeds_iter, nodes_in_area, a_max_km2=A_MAX_KM2, add_cap=ADD_PER_ITER
        )
        if added == 0:
            cells = cells_it
            break

    if cells is None:
        cells = build_cells_from_seeds(seeds_iter, study_area_proj, bbox_pad=BBOX_PAD)

    grid_voronoi = cells.reset_index(drop=True)
    grid_voronoi["grid_id"] = grid_voronoi["cell_id"].astype(str)
    grid_voronoi = grid_voronoi[["grid_id", "area_km2", "geometry"]].copy()
    return grid_voronoi.to_crs("EPSG:4326")


def compute_segment_stats(centerline_gdf, grid_gdf):
    seg = centerline_gdf[centerline_gdf.geometry.notnull() & (~centerline_gdf.geometry.is_empty)].copy()
    seg["seg_id"] = np.arange(len(seg), dtype=int)
    seg["len_km"] = seg.geometry.length / 1000.0

    grid_for_join = grid_gdf[["grid_id", "geometry"]].copy()
    grid_for_join["geometry"] = grid_for_join.geometry.buffer(0)
    joined = gpd.sjoin(seg[["seg_id", "len_km", "geometry"]], grid_for_join, how="left", predicate="within")
    match_counts = joined.groupby("seg_id")["grid_id"].nunique(dropna=True)
    within_ids = match_counts[match_counts == 1].index

    n_total = len(seg)
    n_within = len(within_ids)
    share_count = n_within / n_total if n_total > 0 else 0

    len_total = seg["len_km"].sum()
    len_within = seg.loc[seg["seg_id"].isin(within_ids), "len_km"].sum()
    share_len = len_within / len_total if len_total > 0 else 0

    return {
        "n_total": n_total,
        "n_within": n_within,
        "share_count": share_count,
        "len_total": len_total,
        "len_within": len_within,
        "share_len": share_len,
    }


def run(config_path: str | None, version_id: str, output_dir: str):
    version_root = Path(output_dir) / version_id
    data_dir = version_root / "data"
    metrics_dir = version_root / "metrics"
    data_dir.mkdir(parents=True, exist_ok=True)
    metrics_dir.mkdir(parents=True, exist_ok=True)

    save_config_snapshot(version_root, config_path)
    cl_dir = load_stage01_centerline(version_root)
    cl_dir_proj, study_area_proj = build_study_area(cl_dir)

    square = create_square_grid(study_area_proj.bounds, cell_size_m=3000, crs="EPSG:3857")
    square = gpd.clip(square, gpd.GeoSeries([study_area_proj], crs="EPSG:3857"))
    square = square[square.geometry.area > 0].copy()
    square["area_km2"] = square.geometry.area / 1e6
    square = square.to_crs("EPSG:4326")

    boundary_gdf_proj = gpd.GeoDataFrame(geometry=[study_area_proj], crs="EPSG:3857")
    hex_grid = create_hex_grid_h3(boundary_gdf_proj, resolution=7, target_crs="EPSG:3857")
    voronoi = build_voronoi_grid(cl_dir_proj, study_area_proj)

    outputs = {
        "grid_square_master.parquet": square,
        "grid_hex_master.parquet": hex_grid,
        "grid_voronoi_master.parquet": voronoi,
    }
    for name, gdf in outputs.items():
        gdf.to_parquet(data_dir / name, index=False)

    stats_rows = []
    for grid_type, gdf in [("square", square), ("hex", hex_grid), ("voronoi", voronoi)]:
        grid_proj = gdf.to_crs(cl_dir_proj.crs)
        stats = compute_segment_stats(cl_dir_proj, grid_proj)
        stats_rows.append(
            {
                "grid_type": grid_type,
                "n_grids": len(gdf),
                "mean_area_km2": float(gdf["area_km2"].mean()),
                "n_segments": stats["n_total"],
                "n_within_one_grid": stats["n_within"],
                "share_within": float(stats["share_count"]),
                "len_total_km": float(stats["len_total"]),
                "len_within_km": float(stats["len_within"]),
                "share_len_within": float(stats["share_len"]),
            }
        )
    pd.DataFrame(stats_rows).to_csv(metrics_dir / "stage05_grid_comparison_stats.csv", index=False)

    print(f"[stage05] saved square grids: {len(square):,}")
    print(f"[stage05] saved hex grids: {len(hex_grid):,}")
    print(f"[stage05] saved voronoi grids: {len(voronoi):,}")


def main():
    args = parse_args()
    run(args.config, args.version_id, args.output_dir)


if __name__ == "__main__":
    main()
