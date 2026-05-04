"""乡镇级（统计用区划代码）→ WGS84 质心/代表点，用于替代县级质心。"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


def load_town_centroids_wgs84_csv(
    path: Path | str,
    *,
    town_code_col: str = "town_code",
    lon_col: str = "lon",
    lat_col: str = "lat",
) -> dict[int, tuple[float, float]]:
    """
    从 CSV 读入 乡镇码 → (经度, 纬度)，均为 WGS84。

    CSV 需至少三列：乡镇级行政区代码（与 DTA 中 origin_town_code/dest_town_code 一致，多为 9 位整数）、经度、纬度。
    同一乡镇码多行时保留**最后一行**（并可在调用处自行去重）。
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Town centroids CSV not found: {path}")
    df = pd.read_csv(path)
    for c in (town_code_col, lon_col, lat_col):
        if c not in df.columns:
            raise ValueError(f"Column {c!r} not in {path}; columns={list(df.columns)}")
    df = df[[town_code_col, lon_col, lat_col]].dropna()
    df[town_code_col] = pd.to_numeric(df[town_code_col], errors="coerce")
    df = df[df[town_code_col].notna()].copy()
    df[town_code_col] = df[town_code_col].astype(np.int64)
    dup = int(df[town_code_col].duplicated(keep=False).sum())
    if dup:
        n_dup_codes = int(df[town_code_col].duplicated().sum())
        print(f"[town_centroids] warning: {n_dup_codes} repeated town_code(s) in {path.name}; keeping last")
    df = df.drop_duplicates(subset=[town_code_col], keep="last")
    out: dict[int, tuple[float, float]] = {}
    for _, r in df.iterrows():
        tc = int(r[town_code_col])
        out[tc] = (float(r[lon_col]), float(r[lat_col]))
    return out


def _finite_pair(lon: Any, lat: Any) -> bool:
    try:
        if pd.isna(lon) or pd.isna(lat):
            return False
        x, y = float(lon), float(lat)
    except (TypeError, ValueError):
        return False
    return bool(np.isfinite(x) and np.isfinite(y))


def load_commuting_town_2021_centers_parquet(
    path: Path | str,
) -> tuple[dict[int, tuple[float, float]], dict[int, str], dict[str, int], pd.DataFrame, pd.DataFrame]:
    """
    读取 `commuting_town_2021_centers.parquet`：用 towncode 与 DTA 对齐。

    规则：优先 `office_lon` / `office_lat`；仅当两者不可用或非有限时，使用 `centroid_lon` / `centroid_lat`。
    若两端仍无有效坐标，该乡镇**不进入** lookup 字典（与 DTA 匹配时会丢弃行）。

    返回:
    - code_to_ll: towncode -> (lon, lat)
    - source_by_town: towncode -> "office" | "centroid"
    - summary: 计数
    - df_centroid: 解析为 centroid 的乡镇子表（汇报用，约 21 行量级）
    - df_full_report: 全表 42907 行级 resolved 结果（列含 used_lon, used_lat, resolved_source）
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Parquet not found: {path}")
    df = pd.read_parquet(path)
    tc_col = "towncode" if "towncode" in df.columns else "town_code"
    if tc_col not in df.columns:
        raise ValueError(f"Expected towncode column; have {list(df.columns)}")
    for c in ("office_lon", "office_lat", "centroid_lon", "centroid_lat"):
        if c not in df.columns:
            raise ValueError(f"Parquet missing column {c!r}; columns={list(df.columns)}")

    dup = int(df[tc_col].duplicated(keep=False).sum())
    if dup:
        print(f"[town_centers_parquet] warning: duplicate towncode rows ({dup}); keeping last per code")
    df = df.sort_values(tc_col).drop_duplicates(subset=[tc_col], keep="last")
    n_dedup = int(len(df))

    tc_series = pd.to_numeric(df[tc_col], errors="coerce")
    ol, oa = df["office_lon"], df["office_lat"]
    cl, cta = df["centroid_lon"], df["centroid_lat"]
    ok_off = pd.Series([_finite_pair(a, b) for a, b in zip(ol, oa)], index=df.index)
    ok_cent = pd.Series([_finite_pair(a, b) for a, b in zip(cl, cta)], index=df.index)
    valid_tc = tc_series.notna()
    res_arr = np.where(
        ~valid_tc.to_numpy(),
        "missing",
        np.where(
            ok_off.to_numpy(),
            "office",
            np.where(ok_cent.to_numpy(), "centroid", "missing"),
        ),
    )
    oln = np.asarray(ol, dtype=float)
    oat = np.asarray(oa, dtype=float)
    cln = np.asarray(cl, dtype=float)
    ctn = np.asarray(cta, dtype=float)
    u_lon = np.where(res_arr == "office", oln, np.where(res_arr == "centroid", cln, np.nan))
    u_lat = np.where(res_arr == "office", oat, np.where(res_arr == "centroid", ctn, np.nan))

    rep = pd.DataFrame(
        {
            "towncode": tc_series,
            "used_lon": u_lon,
            "used_lat": u_lat,
            "resolved_source": res_arr,
        }
    )
    for opt in ("center_source", "geocode_status"):
        if opt in df.columns:
            rep[opt] = df[opt].values

    n_office = int((rep["resolved_source"] == "office").sum())
    n_centroid = int((rep["resolved_source"] == "centroid").sum())
    n_drop = int((rep["resolved_source"] == "missing").sum())

    rep_cent = rep[rep["resolved_source"] == "centroid"].copy()
    code_to_ll: dict[int, tuple[float, float]] = {}
    source_by_town: dict[int, str] = {}
    sub = rep[(rep["resolved_source"] != "missing") & rep["towncode"].notna()].copy()
    for tc, ulo, ula, src in zip(
        sub["towncode"].astype(np.int64),
        sub["used_lon"],
        sub["used_lat"],
        sub["resolved_source"],
    ):
        code_to_ll[int(tc)] = (float(ulo), float(ula))
        source_by_town[int(tc)] = str(src)

    summary = {
        "parquet_path": str(path),
        "n_rows_dedup": n_dedup,
        "n_resolved_office": n_office,
        "n_resolved_centroid": n_centroid,
        "n_unresolved": n_drop,
        "n_lookup": len(code_to_ll),
    }
    print(
        f"[town_centers_parquet] {path.name}: office={n_office}, centroid_fallback={n_centroid}, no_coord_dropped={n_drop}"
    )
    return code_to_ll, source_by_town, summary, rep_cent, rep
