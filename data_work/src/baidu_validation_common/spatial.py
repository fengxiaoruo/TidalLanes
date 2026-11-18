"""Spatial fit metrics for Baidu route geometry vs baseline network geometry."""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pyproj
from shapely.geometry import LineString
from shapely.ops import transform

_A = 6378245.0
_EE = 0.00669342162296594323


def gcj02_to_wgs84(lon, lat):
    if not (72.004 <= lon <= 137.8347 and 0.8293 <= lat <= 55.8271):
        return lon, lat
    dlat = _transform_lat(lon - 105, lat - 35)
    dlon = _transform_lon(lon - 105, lat - 35)
    radlat = lat / 180 * math.pi
    magic = 1 - _EE * math.sin(radlat) ** 2
    sqrtmagic = math.sqrt(magic)
    dlat = (dlat * 180) / ((_A * (1 - _EE)) / (magic * sqrtmagic) * math.pi)
    dlon = (dlon * 180) / (_A / sqrtmagic * math.cos(radlat) * math.pi)
    return lon - dlon, lat - dlat


def _transform_lat(lon, lat):
    ret = -100 + 2 * lon + 3 * lat + 0.2 * lat * lat + 0.1 * lon * lat + 0.2 * math.sqrt(abs(lon))
    ret += (20 * math.sin(6 * lon * math.pi) + 20 * math.sin(2 * lon * math.pi)) * 2 / 3
    ret += (20 * math.sin(lat * math.pi) + 40 * math.sin(lat / 3 * math.pi)) * 2 / 3
    ret += (160 * math.sin(lat / 12 * math.pi) + 320 * math.sin(lat * math.pi / 30)) * 2 / 3
    return ret


def _transform_lon(lon, lat):
    ret = 300 + lon + 2 * lat + 0.1 * lon * lon + 0.1 * lon * lat + 0.1 * math.sqrt(abs(lon))
    ret += (20 * math.sin(6 * lon * math.pi) + 20 * math.sin(2 * lon * math.pi)) * 2 / 3
    ret += (20 * math.sin(lon * math.pi) + 40 * math.sin(lon / 3 * math.pi)) * 2 / 3
    ret += (150 * math.sin(lon / 12 * math.pi) + 300 * math.sin(lon / math.pi / 30)) * 2 / 3
    return ret


def line_gcj_to_wgs(line: LineString) -> LineString:
    if line is None or line.is_empty:
        return line
    return LineString([gcj02_to_wgs84(x, y) for x, y in line.coords])


def make_utm_transformer(crs_from: str = "EPSG:4326", crs_to: str = "EPSG:32650"):
    proj = pyproj.Transformer.from_crs(crs_from, crs_to, always_xy=True)
    return lambda g: transform(proj.transform, g)


def spatial_metrics(
    baseline_wgs: LineString,
    route_gcj: LineString,
    *,
    buffer_m: float,
    to_utm,
) -> dict:
    route_wgs = line_gcj_to_wgs(route_gcj)
    base_u = to_utm(baseline_wgs)
    route_u = to_utm(route_wgs)
    if base_u.length <= 0 or route_u.length <= 0:
        return {
            "route_length_m": float(route_u.length) if route_u.length else np.nan,
            "length_ratio_route_over_geom": np.nan,
            "mean_dist_geom_to_route_m": np.nan,
            "hausdorff_m": np.nan,
            "overlap_geom_in_route_buffer": np.nan,
            "sym_overlap": np.nan,
        }
    base_pts = [base_u.interpolate(i / 20, normalized=True) for i in range(21)]
    mean_dist = float(np.mean([route_u.distance(p) for p in base_pts]))
    buf = float(buffer_m)
    overlap_geom = base_u.intersection(route_u.buffer(buf)).length / base_u.length
    overlap_rt = route_u.intersection(base_u.buffer(buf)).length / route_u.length
    return {
        "route_length_m": float(route_u.length),
        "length_ratio_route_over_geom": float(route_u.length / base_u.length),
        "mean_dist_geom_to_route_m": mean_dist,
        "hausdorff_m": float(base_u.hausdorff_distance(route_u)),
        "overlap_geom_in_route_buffer": float(overlap_geom),
        "sym_overlap": float(min(overlap_geom, overlap_rt)),
    }


def spatial_pass(row: pd.Series) -> bool:
    return bool(
        pd.notna(row.get("mean_dist_geom_to_route_m"))
        and pd.notna(row.get("overlap_geom_in_route_buffer"))
        and row["mean_dist_geom_to_route_m"] <= 80
        and row["overlap_geom_in_route_buffer"] >= 0.5
    )
