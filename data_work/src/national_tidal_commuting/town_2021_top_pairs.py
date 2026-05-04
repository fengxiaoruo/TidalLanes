from __future__ import annotations

import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
from pyproj import Transformer
from shapely.geometry import MultiPoint

from src.national_tidal_commuting.build_top_pairs import (
    aggregate_pairs,
    collapse_unordered_pairs,
    finalize_undirected_top_n,
)
from src.national_tidal_commuting.common import ensure_version_dirs, get_version_root, load_config, resolve_path
from src.national_tidal_commuting.county_centroids_wgs84 import load_pac_centroids_wgs84
from src.national_tidal_commuting.town_centroids_wgs84 import (
    load_commuting_town_2021_centers_parquet,
    load_town_centroids_wgs84_csv,
)


CITY_SUFFIXES = ("市", "地区", "盟")


def normalize_city_name_for_match(value: object) -> str:
    """Match report city names like 北京 to DTA names like 北京市."""
    out = str(value).strip()
    for suffix in CITY_SUFFIXES:
        if out.endswith(suffix):
            return out[: -len(suffix)]
    return out


def rank_top_cities_intra(
    df_intra: pd.DataFrame, top_n_cities: int
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """By sum(commuters_2021) on intra-city town flows, rank 地级市 (prov||city)."""
    df = df_intra.copy()
    if "city_key" not in df.columns:
        df["city_key"] = df["origin_prov_name"].str.strip() + "||" + df["origin_city_name"].str.strip()
    agg = (
        df.groupby("city_key", as_index=False)
        .agg(commute_pop_2021=("commuters_2021", "sum"), n_town_flows=("commuters_2021", "count"))
        .sort_values("commute_pop_2021", ascending=False)
        .head(top_n_cities)
        .reset_index(drop=True)
    )
    agg["national_commute_pop_rank"] = np.arange(1, len(agg) + 1, dtype=int)
    meta_cols = (
        df.groupby("city_key", as_index=False)
        .agg(
            origin_prov_name=("origin_prov_name", "first"),
            origin_city_name=("origin_city_name", "first"),
        )
        .drop_duplicates("city_key")
    )
    ranked = agg.merge(meta_cols, on="city_key", how="left")
    return ranked, agg


def rank_cities_from_congestion_excel(
    df_intra: pd.DataFrame,
    top_n_cities: int,
    city_selection: dict,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    xlsx_path = resolve_path(city_selection["path"])
    sheet_name = city_selection.get("sheet_name", 0)
    city_col = str(city_selection.get("city_column", "城市"))
    free_col = str(city_selection.get("free_speed_column", "畅通速度"))
    speed_col = str(city_selection.get("speed_column", "平均速度"))
    date_col = str(city_selection.get("date_column", "日期"))
    peak_col = str(city_selection.get("peak_type_column", "高峰类型"))
    index_col = str(city_selection.get("congestion_index_column", "source_congestion_index"))

    if xlsx_path.suffix.lower() == ".csv":
        report = pd.read_csv(xlsx_path)
    else:
        report = pd.read_excel(xlsx_path, sheet_name=sheet_name)
    if index_col in report.columns:
        missing = [c for c in [city_col, index_col] if c not in report.columns]
    else:
        missing = [c for c in [city_col, free_col, speed_col] if c not in report.columns]
    if missing:
        raise ValueError(f"{xlsx_path} is missing required congestion columns: {missing}")
    report = report.copy()
    report[city_col] = report[city_col].astype(str).str.strip()
    city_aliases = {str(k).strip(): str(v).strip() for k, v in (city_selection.get("city_aliases") or {}).items()}
    if city_aliases:
        report[city_col] = report[city_col].replace(city_aliases)
    if index_col in report.columns:
        report[index_col] = pd.to_numeric(report[index_col], errors="coerce")
        report = report.dropna(subset=[city_col, index_col]).copy()
        report = report[report[index_col] > 0].copy()
    else:
        report[free_col] = pd.to_numeric(report[free_col], errors="coerce")
        report[speed_col] = pd.to_numeric(report[speed_col], errors="coerce")
        report = report.dropna(subset=[city_col, free_col, speed_col]).copy()
        report = report[(report[free_col] > 0) & (report[speed_col] > 0)].copy()
        report[index_col] = report[free_col] / report[speed_col]
    if report.empty:
        raise ValueError(f"{xlsx_path} has no usable congestion rows after cleaning.")

    agg_spec = {
        "n_congestion_obs": (index_col, "size"),
        "source_congestion_index": (index_col, "mean"),
    }
    if free_col in report.columns:
        agg_spec["source_free_speed_mean"] = (free_col, "mean")
    if speed_col in report.columns:
        agg_spec["source_peak_speed_mean"] = (speed_col, "mean")
    if date_col in report.columns:
        agg_spec["source_date_min"] = (date_col, "min")
        agg_spec["source_date_max"] = (date_col, "max")
    if peak_col in report.columns:
        peak_counts = (
            report.groupby([city_col, peak_col], as_index=False)
            .size()
            .pivot(index=city_col, columns=peak_col, values="size")
            .fillna(0)
            .reset_index()
        )
    else:
        peak_counts = pd.DataFrame({city_col: sorted(report[city_col].unique())})

    congestion = (
        report.groupby(city_col, as_index=False)
        .agg(**agg_spec)
        .sort_values("source_congestion_index", ascending=False)
        .reset_index(drop=True)
    )
    congestion["source_congestion_rank"] = np.arange(1, len(congestion) + 1, dtype=int)
    congestion["source_city_name"] = congestion[city_col]
    congestion["source_city_name_norm"] = congestion[city_col].map(normalize_city_name_for_match)
    congestion = congestion.merge(peak_counts, on=city_col, how="left")

    df = df_intra.copy()
    if "city_key" not in df.columns:
        df["city_key"] = df["origin_prov_name"].str.strip() + "||" + df["origin_city_name"].str.strip()
    meta = (
        df.groupby("city_key", as_index=False)
        .agg(
            commute_pop_2021=("commuters_2021", "sum"),
            n_town_flows=("commuters_2021", "count"),
            origin_prov_name=("origin_prov_name", "first"),
            origin_city_name=("origin_city_name", "first"),
        )
    )
    meta["source_city_name_norm"] = meta["origin_city_name"].map(normalize_city_name_for_match)
    dup = meta.groupby("source_city_name_norm")["city_key"].nunique()
    ambiguous = sorted(dup[dup > 1].index.tolist())
    if ambiguous:
        raise ValueError(f"Ambiguous DTA city-name matches for congestion source: {ambiguous[:20]}")

    ranked = congestion.merge(meta, on="source_city_name_norm", how="left", indicator=True)
    unmatched = ranked[ranked["_merge"] != "both"].copy()
    if not unmatched.empty:
        names = unmatched["source_city_name"].astype(str).tolist()
        raise ValueError(f"Congestion source cities not found in commuting_town_2021.dta: {names}")
    ranked = (
        ranked.drop(columns=["_merge"])
        .sort_values("source_congestion_rank")
        .head(top_n_cities)
        .reset_index(drop=True)
    )
    ranked["national_commute_pop_rank"] = np.arange(1, len(ranked) + 1, dtype=int)
    ranked["city_selection_method"] = "congestion_excel"
    ranked["city_selection_source"] = str(xlsx_path)
    return ranked, congestion


def attach_coords(
    df: pd.DataFrame,
    code_to_ll: dict[int, tuple[float, float]],
    label: str,
    *,
    key_level: str,
) -> pd.DataFrame:
    """
    为行添加 home_x, home_y, work_x, work_y（WGS84）。

    - key_level=\"county\"：用 origin/dest 乡镇码 // 1000 的县级 PAC 查表（与旧版一致，较粗）。
    - key_level=\"town\"：用**完整**乡镇级统计用区划代码查表，需自备乡镇质心/代表点 CSV。
    """
    if key_level not in ("county", "town"):
        raise ValueError("key_level must be 'county' or 'town'")

    o_series = df["origin_town_code"].astype(int)
    d_series = df["dest_town_code"].astype(int)
    if key_level == "county":
        o_key = (o_series // 1000).map(int)
        d_key = (d_series // 1000).map(int)
    else:
        o_key = o_series
        d_key = d_series

    def _in_map(p: int) -> bool:
        try:
            return int(p) in code_to_ll
        except (TypeError, ValueError):
            return False

    miss_o = o_key.map(lambda p: not _in_map(int(p)))
    miss_d = d_key.map(lambda p: not _in_map(int(p)))
    if (miss_o | miss_d).any():
        n_bad = int((miss_o | miss_d).sum())
        kind = "PAC/county" if key_level == "county" else "town code"
        print(f"[town_2021] {label}: dropping {n_bad} rows with {kind} missing in lookup table")
    df = df[~(miss_o | miss_d)].copy()
    o_key = df["origin_town_code"].astype(int) // 1000 if key_level == "county" else df["origin_town_code"].astype(int)
    d_key = df["dest_town_code"].astype(int) // 1000 if key_level == "county" else df["dest_town_code"].astype(int)
    df["home_x"] = o_key.map(lambda p: code_to_ll[int(p)][0])
    df["home_y"] = o_key.map(lambda p: code_to_ll[int(p)][1])
    df["work_x"] = d_key.map(lambda p: code_to_ll[int(p)][0])
    df["work_y"] = d_key.map(lambda p: code_to_ll[int(p)][1])
    df["home_plot_id"] = df["origin_town_code"].astype(int).astype(str)
    df["work_plot_id"] = df["dest_town_code"].astype(int).astype(str)
    df["pop"] = pd.to_numeric(df["commuters_2021"], errors="coerce").fillna(0.0)
    return df[df["pop"] > 0].copy()


def _town_src(ks: str | int, src_map: dict[int, str]) -> str:
    k = int(float(str(ks).strip()))
    return src_map.get(k, "unknown")


def estimate_city_radius_km(df: pd.DataFrame) -> tuple[float, float]:
    pts = pd.concat(
        [
            df[["home_x", "home_y"]].rename(columns={"home_x": "lon", "home_y": "lat"}),
            df[["work_x", "work_y"]].rename(columns={"work_x": "lon", "work_y": "lat"}),
        ],
        ignore_index=True,
    ).dropna().drop_duplicates()
    if pts.empty:
        return float("nan"), float("nan")
    lon0 = float(pts["lon"].mean())
    lat0 = float(pts["lat"].mean())
    proj4 = f"+proj=laea +lat_0={lat0} +lon_0={lon0} +datum=WGS84 +units=m +no_defs"
    transformer = Transformer.from_crs("EPSG:4326", proj4, always_xy=True)
    xy = [transformer.transform(float(lon), float(lat)) for lon, lat in pts[["lon", "lat"]].itertuples(index=False, name=None)]
    if len(xy) == 1:
        return 0.0, 0.0
    hull = MultiPoint(xy).convex_hull
    area_km2 = float(hull.area) / 1_000_000.0 if not hull.is_empty else 0.0
    if area_km2 <= 0:
        xvals = np.array([p[0] for p in xy], dtype=float)
        yvals = np.array([p[1] for p in xy], dtype=float)
        span_m = math.hypot(float(xvals.max() - xvals.min()), float(yvals.max() - yvals.min()))
        return span_m / 2000.0, 0.0
    radius_km = math.sqrt(area_km2 / math.pi)
    return float(radius_km), float(area_km2)


def process_city_town_2021(
    city_key: str,
    city_block: pd.DataFrame,
    config: dict,
    code_to_ll: dict[int, tuple[float, float]],
    city_id: str,
    city_label: str,
    *,
    coord_key_level: str,
    town_source_by_code: dict[int, str] | None = None,
) -> tuple[pd.DataFrame, dict]:
    df0 = city_block.copy()
    same_town_mask = pd.to_numeric(df0["origin_town_code"], errors="coerce").eq(
        pd.to_numeric(df0["dest_town_code"], errors="coerce")
    )
    n_same_town_rows = int(same_town_mask.sum())
    same_town_commuters = float(pd.to_numeric(df0.loc[same_town_mask, "commuters_2021"], errors="coerce").fillna(0.0).sum())
    if n_same_town_rows:
        print(f"[town_2021] {city_label}: dropping {n_same_town_rows} same-township rows before pair construction")
    df0 = df0.loc[~same_town_mask].copy()
    df = attach_coords(df0, code_to_ll, city_label, key_level=coord_key_level)
    for col in ["home_x", "home_y", "work_x", "work_y", "pop"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["home_x", "home_y", "work_x", "work_y", "pop"]).copy()
    city_radius_est_km, city_area_est_km2 = estimate_city_radius_km(df)
    min_dist_radius_share = float(config.get("min_pair_distance_radius_share", 1.0 / 3.0))
    distance_threshold_km = float(city_radius_est_km * min_dist_radius_share) if np.isfinite(city_radius_est_km) else float("nan")

    if df.empty:
        return pd.DataFrame(), {
            "city_key": city_key,
            "city_id": city_id,
            "city_label": city_label,
            "commute_path": "commuting_town_2021.dta (town matrix)",
            "raw_rows": 0,
            "n_directed_edges": 0,
            "n_undirected_pairs": 0,
            "n_pairs_exported": 0,
            "sample_shortfall_flag": True,
            "commuter_undir_mean_sum_exported": 0.0,
            "n_same_town_rows_dropped": n_same_town_rows,
            "same_town_commuters_dropped": same_town_commuters,
            "city_radius_est_km": city_radius_est_km,
            "city_area_est_km2": city_area_est_km2,
            "distance_threshold_km": distance_threshold_km,
        }

    top_n = int(config.get("top_n_pairs", 50))
    supplement_n = int(config.get("supplement_long_distance_top_n", 30))
    directed = aggregate_pairs(df)
    unordered = collapse_unordered_pairs(directed)
    n_zero_dist_pairs_dropped = int((unordered["euclid_dist_km"] <= 0).sum())
    unordered = unordered[unordered["euclid_dist_km"] > 0].copy().reset_index(drop=True)
    selected = finalize_undirected_top_n(
        unordered,
        top_n,
        city_id,
        city_label,
        supplement_n=supplement_n,
        min_dist_km=distance_threshold_km,
    )
    n_und = int(len(unordered))
    if not selected.empty:
        selected["origin_town_code"] = pd.to_numeric(selected["home_key"], errors="coerce").astype("Int64")
        selected["dest_town_code"] = pd.to_numeric(selected["work_key"], errors="coerce").astype("Int64")
        selected["city_radius_est_km"] = city_radius_est_km
        selected["city_area_est_km2"] = city_area_est_km2
        selected["distance_threshold_km"] = distance_threshold_km
        if town_source_by_code and coord_key_level == "town":
            selected = selected.copy()
            selected["home_coord_source"] = selected["home_key"].map(lambda x: _town_src(x, town_source_by_code))
            selected["work_coord_source"] = selected["work_key"].map(lambda x: _town_src(x, town_source_by_code))
            selected["pair_uses_centroid_fallback"] = (
                (selected["home_coord_source"] == "centroid") | (selected["work_coord_source"] == "centroid")
            ).astype(bool)
    n_selected_longdist = int(selected["selected_by_longdist_top30"].sum()) if "selected_by_longdist_top30" in selected.columns else 0

    summary = {
        "city_key": city_key,
        "city_id": city_id,
        "city_label": city_label,
        "commute_path": "commuting_town_2021.dta (town matrix)",
        "raw_rows": int(len(df)),
        "n_directed_edges": int(len(directed)),
        "n_undirected_pairs": n_und,
        "n_pairs_exported": int(len(selected)),
        "sample_shortfall_flag": bool(n_und < top_n) if n_und else True,
        "commuter_undir_mean_sum_exported": float(selected["commuter_undir_mean"].sum()) if len(selected) else 0.0,
        "n_same_town_rows_dropped": n_same_town_rows,
        "same_town_commuters_dropped": same_town_commuters,
        "n_zero_dist_pairs_dropped": n_zero_dist_pairs_dropped,
        "city_radius_est_km": city_radius_est_km,
        "city_area_est_km2": city_area_est_km2,
        "distance_threshold_km": distance_threshold_km,
        "n_selected_longdist_top30_union": n_selected_longdist,
    }
    return selected, summary


def run_town_2021_build(
    config: dict,
    version_id: str | None,
    output_dir: str | None,
) -> None:
    tcfg = config.get("town_2021") or {}
    dta = resolve_path(tcfg.get("commute_dta_path", "raw_data/commuting_town_2021.dta"))
    town_parquet = tcfg.get("town_centers_parquet")
    town_csv = tcfg.get("town_centroids_csv")
    coord_key_level = "town"
    code_to_ll: dict[int, tuple[float, float]] | None = None
    town_source_by_code: dict[int, str] | None = None
    parq_summary: dict | None = None
    rep_centroid_df: pd.DataFrame | None = None
    rep_full_df: pd.DataFrame | None = None
    if town_parquet:
        p_path = resolve_path(town_parquet)
        print(f"[town_2021] loading town centers parquet {p_path} ...")
        code_to_ll, town_source_by_code, parq_summary, rep_centroid_df, rep_full_df = load_commuting_town_2021_centers_parquet(
            p_path
        )
    elif town_csv:
        tc_path = resolve_path(town_csv)
        tcc = str(tcfg.get("town_code_column", "town_code"))
        tlon = str(tcfg.get("lon_column", "lon"))
        tlat = str(tcfg.get("lat_column", "lat"))
        print(f"[town_2021] loading town-level (乡镇) centroids from {tc_path} ...")
        code_to_ll = load_town_centroids_wgs84_csv(tc_path, town_code_col=tcc, lon_col=tlon, lat_col=tlat)
        print(f"[town_2021] {len(code_to_ll)} 乡镇/街道级 codes in lookup (WGS84)")
    else:
        shp = tcfg.get("county_shp_path")
        if not shp:
            raise ValueError(
                "Set town_2021.town_centers_parquet (推荐), 或 town_centroids_csv, 或 county_shp_path (县级回退)."
            )
        shp = Path(resolve_path(shp))
        prj = Path(tcfg.get("county_shp_prj_path") or (shp.parent / (shp.stem + ".prj")))
        encoding = str(tcfg.get("county_shp_encoding", "gbk"))
        coord_key_level = "county"
        print(
            f"[town_2021] using county (县级) polygon centroids from {shp} (coarse; prefer town_centroids_csv for 乡镇). ..."
        )
        code_to_ll = load_pac_centroids_wgs84(shp, prj, encoding=encoding)
        print(f"[town_2021] {len(code_to_ll)} county/district PAC keys")
    top_n_cities = int(config.get("top_n_cities", 50))
    print(f"[town_2021] loading {dta} ...")
    full = pd.read_stata(dta, convert_categoricals=False)
    for c in full.columns:
        if full[c].dtype == object:
            full[c] = full[c].astype(str).str.strip()
    m = (full["origin_prov_name"] == full["dest_prov_name"]) & (full["origin_city_name"] == full["dest_city_name"])
    intra = full[m].copy()
    intra["city_key"] = intra["origin_prov_name"].str.strip() + "||" + intra["origin_city_name"].str.strip()
    print(f"[town_2021] intra-city town flows: {len(intra):,} rows")

    city_selection = config.get("city_selection") or {}
    method = str(city_selection.get("method", "intra_commute_top_n")).strip().lower()
    if method == "congestion_excel":
        ranked, city_selection_source = rank_cities_from_congestion_excel(intra, top_n_cities, city_selection)
    elif method in {"intra_commute_top_n", "commute_top_n", "default"}:
        ranked, city_selection_source = rank_top_cities_intra(intra, top_n_cities)
        ranked["city_selection_method"] = "intra_commute_top_n"
    else:
        raise ValueError(f"Unknown town_2021 city_selection.method: {method!r}")
    version_root = get_version_root(config, version_id, output_dir)
    dirs = ensure_version_dirs(version_root)
    if rep_centroid_df is not None and not rep_centroid_df.empty:
        cpath = dirs["metrics"] / "commuting_town_2021_towns_resolved_to_centroid.csv"
        rep_centroid_df.to_csv(cpath, index=False)
        print(
            f"[town_2021] report: {len(rep_centroid_df)} 个乡镇使用 centroid 坐标（非 office），已写入 {cpath.name}"
        )
    if rep_full_df is not None and not rep_full_df.empty:
        fpath = dirs["metrics"] / "commuting_town_2021_center_resolution_by_towncode.csv"
        rep_full_df.to_csv(fpath, index=False)
        print(f"[town_2021] 全量乡镇解析表: {fpath.name}")
    if parq_summary is not None:
        with (dirs["metrics"] / "commuting_town_2021_centers_load_summary.json").open("w", encoding="utf-8") as fh:
            json.dump(parq_summary, fh, ensure_ascii=False, indent=2)

    national_path = dirs["metrics"] / "national_top_cities_intra_commute_2021.csv"
    ranked.to_csv(national_path, index=False)
    print(f"[town_2021] wrote national city ranking ({len(ranked)} cities) to {national_path}")
    if method == "congestion_excel":
        source_path = dirs["metrics"] / "source_congestion_city_ranking.csv"
        city_selection_source.to_csv(source_path, index=False)
        print(f"[town_2021] wrote source congestion ranking to {source_path}")

    assert code_to_ll is not None
    blocks = list(ranked["city_key"].values)
    selections = []
    summaries = []
    for ridx, city_key in enumerate(blocks, start=1):
        row_meta = ranked[ranked["city_key"] == city_key].iloc[0]
        city_id = f"cn_u{top_n_cities}c_r{ridx:02d}"
        prov = str(row_meta["origin_prov_name"])
        cityn = str(row_meta["origin_city_name"])
        city_label = f"{cityn}（{prov}）" if prov != cityn else prov
        block = intra[intra["city_key"] == city_key]
        print(f"[town_2021] {ridx:02d}/{len(blocks)} {city_key} block rows={len(block):,}")
        sel, summ = process_city_town_2021(
            city_key,
            block,
            config,
            code_to_ll,
            city_id,
            city_label,
            coord_key_level=coord_key_level,
            town_source_by_code=town_source_by_code,
        )
        summ["national_commute_pop_rank"] = int(row_meta["national_commute_pop_rank"])
        summ["intra_commute_pop_2021"] = float(row_meta["commute_pop_2021"])
        if "source_congestion_rank" in row_meta:
            summ["source_congestion_rank"] = int(row_meta["source_congestion_rank"])
            summ["source_congestion_index"] = float(row_meta["source_congestion_index"])
            summ["source_city_name"] = str(row_meta["source_city_name"])
            if "source_free_speed_mean" in row_meta.index:
                summ["source_free_speed_mean"] = float(row_meta["source_free_speed_mean"])
            if "source_peak_speed_mean" in row_meta.index:
                summ["source_peak_speed_mean"] = float(row_meta["source_peak_speed_mean"])
        if not sel.empty:
            sel["national_commute_pop_rank"] = int(row_meta["national_commute_pop_rank"])
            sel["intra_commute_pop_2021"] = float(row_meta["commute_pop_2021"])
            if "source_congestion_rank" in row_meta:
                sel["source_congestion_rank"] = int(row_meta["source_congestion_rank"])
                sel["source_congestion_index"] = float(row_meta["source_congestion_index"])
                sel["source_city_name"] = str(row_meta["source_city_name"])
                if "source_free_speed_mean" in row_meta.index:
                    sel["source_free_speed_mean"] = float(row_meta["source_free_speed_mean"])
                if "source_peak_speed_mean" in row_meta.index:
                    sel["source_peak_speed_mean"] = float(row_meta["source_peak_speed_mean"])
        selections.append(sel)
        summaries.append(summ)

    all_selected = pd.concat(selections, ignore_index=True) if selections else pd.DataFrame()
    city_summary = pd.DataFrame(summaries)

    data_dir = dirs["data"]
    metrics_dir = dirs["metrics"]
    top_pairs_path = data_dir / "top_commuting_pairs.csv"
    summary_path = metrics_dir / "top_commuting_pairs_city_summary.csv"
    all_selected.to_csv(top_pairs_path, index=False)
    city_summary.to_csv(summary_path, index=False)
    if "pair_uses_centroid_fallback" in all_selected.columns and bool(all_selected["pair_uses_centroid_fallback"].any()):
        n_cent = int(all_selected["pair_uses_centroid_fallback"].sum())
        subp = metrics_dir / "top_commuting_pairs_at_least_one_centroid_coord.csv"
        all_selected[all_selected["pair_uses_centroid_fallback"]].to_csv(subp, index=False)
        print(
            f"[town_2021] report: {n_cent} 条 pair 的 home 或 work 至少一端为 centroid 备用坐标，已写入 {subp.name}"
        )
    (version_root / "config_snapshot.top_pairs.json").write_text(
        json.dumps(
            {
                "stage": "national_tidal_commuting.build_top_pairs",
                "source": "town_2021",
                "config_path": config["_config_path"],
                "version_root": str(version_root.resolve()),
                "national_city_ranking_csv": str(national_path.resolve()),
                "selected_pairs_path": str(top_pairs_path.resolve()),
                "city_summary_path": str(summary_path.resolve()),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"[town_2021] wrote {len(all_selected)} selected pairs to {top_pairs_path}")
    print(f"[town_2021] wrote city summary to {summary_path}")


def main_town_2021_cli():
    import argparse

    p = argparse.ArgumentParser(description="Build top pairs from 2021 town commuting matrix + 2019 county map.")
    p.add_argument("--config", required=True)
    p.add_argument("--version-id", default=None)
    p.add_argument("--output-dir", default=None)
    args = p.parse_args()
    config = load_config(args.config)
    run_town_2021_build(config, args.version_id, args.output_dir)
