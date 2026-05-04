"""Load 县级 polygon centroids from the 2019审图号 map and project to WGS84 for Baidu API inputs."""

from __future__ import annotations

from pathlib import Path
import numpy as np

try:
    import shapefile
except ImportError as exc:  # pragma: no cover
    raise ImportError("This module requires `pyshp` (pip install pyshp).") from exc

try:
    from pyproj import CRS, Transformer
except ImportError as exc:  # pragma: no cover
    raise ImportError("This module requires `pyproj` (pip install pyproj).") from exc


def _ring_mean_xy(shape) -> tuple[float, float]:
    pts = np.asarray(shape.points, dtype=float)
    if pts.size == 0:
        return float("nan"), float("nan")
    return float(pts[:, 0].mean()), float(pts[:, 1].mean())


def load_pac_centroids_wgs84(county_shp_path: Path, prj_path: Path, encoding: str = "gbk") -> dict[int, tuple[float, float]]:
    """Return mapping PAC (6-digit 县级 / 市辖区) -> (lon, lat) WGS84."""
    wkt = prj_path.read_text(encoding="utf-8")
    crs = CRS.from_wkt(wkt)
    transformer = Transformer.from_crs(crs, 4326, always_xy=True)
    reader = shapefile.Reader(str(county_shp_path), encoding=encoding)
    out: dict[int, tuple[float, float]] = {}
    for idx in range(len(reader)):
        shp = reader.shape(idx)
        rec = reader.record(idx)
        pac = int(rec["PAC"])
        mx, my = _ring_mean_xy(shp)
        if not (np.isfinite(mx) and np.isfinite(my)):
            continue
        lon, lat = transformer.transform(mx, my)
        out[pac] = (float(lon), float(lat))
    return out
