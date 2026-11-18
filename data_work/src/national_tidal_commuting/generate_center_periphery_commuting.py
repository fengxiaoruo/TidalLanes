from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.diagnostics.mpl_setup import ensure_mpl_cache
from src.national_tidal_commuting.common import ensure_version_dirs, resolve_path


DEFAULT_VERSION_ID = "national_center_periphery_commuting_v1"
DEFAULT_OUTPUT_DIR = "data_work/outputs"
DEFAULT_CORE_SHARE = 0.30
DEFAULT_CHUNKSIZE = 750_000
FLOW_ORDER = ["C_to_C", "C_to_P", "P_to_C", "P_to_P"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build city-level center/periphery 2x2 commuting summaries from the 2021 town OD matrix."
    )
    parser.add_argument("--version-id", default=DEFAULT_VERSION_ID)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--commute-dta-path", default="", help="Optional explicit commuting_town_2021.dta path.")
    parser.add_argument("--town-centers-parquet", default="", help="Optional explicit commuting_town_2021_centers.parquet path.")
    parser.add_argument("--core-workplace-share", type=float, default=DEFAULT_CORE_SHARE)
    parser.add_argument("--chunksize", type=int, default=DEFAULT_CHUNKSIZE)
    parser.add_argument(
        "--max-chunks",
        type=int,
        default=0,
        help="Debug option. If positive, read only this many Stata chunks.",
    )
    return parser.parse_args()


def configure_plots(cache_root: Path) -> None:
    ensure_mpl_cache(str(cache_root))
    plt.rcParams.update(
        {
            "font.sans-serif": [
                "PingFang SC",
                "Noto Sans CJK SC",
                "Arial Unicode MS",
                "Helvetica",
                "Arial",
                "DejaVu Sans",
            ],
            "font.family": "sans-serif",
            "axes.unicode_minus": False,
            "figure.dpi": 180,
            "savefig.dpi": 450,
            "axes.linewidth": 0.8,
            "axes.labelsize": 8,
            "axes.titlesize": 9,
            "xtick.labelsize": 7,
            "ytick.labelsize": 7,
        }
    )


def first_existing_path(explicit: str, candidates: list[str]) -> Path:
    if explicit:
        return resolve_path(explicit)
    for text in candidates:
        path = resolve_path(text)
        if path.exists():
            return path
    return resolve_path(candidates[0])


def read_stata_chunks(path: Path, chunksize: int):
    reader = pd.read_stata(path, convert_categoricals=False, iterator=True)
    while True:
        try:
            chunk = reader.read(nrows=chunksize)
        except StopIteration:
            break
        if chunk is None or chunk.empty:
            break
        yield chunk


def clean_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    use = chunk.copy()
    text_cols = [
        "origin_prov_name",
        "origin_city_name",
        "origin_county_name",
        "origin_town_name",
        "dest_prov_name",
        "dest_city_name",
        "dest_county_name",
        "dest_town_name",
    ]
    for col in text_cols:
        use[col] = use[col].astype(str).str.strip()
    use["commuters_2021"] = pd.to_numeric(use["commuters_2021"], errors="coerce").fillna(0.0).astype(float)
    use["origin_town_code"] = pd.to_numeric(use["origin_town_code"], errors="coerce").astype("Int64")
    use["dest_town_code"] = pd.to_numeric(use["dest_town_code"], errors="coerce").astype("Int64")
    use = use.loc[
        use["commuters_2021"].gt(0)
        & use["origin_town_code"].notna()
        & use["dest_town_code"].notna()
    ].copy()
    use["origin_town_code"] = use["origin_town_code"].astype(np.int64)
    use["dest_town_code"] = use["dest_town_code"].astype(np.int64)
    return use


def keep_intra_city(chunk: pd.DataFrame) -> pd.DataFrame:
    mask = (chunk["origin_prov_name"] == chunk["dest_prov_name"]) & (
        chunk["origin_city_name"] == chunk["dest_city_name"]
    )
    use = chunk.loc[mask].copy()
    use["city_key"] = use["origin_prov_name"] + "||" + use["origin_city_name"]
    return use


def append_town_marginals(intra: pd.DataFrame, origin_parts: list[pd.DataFrame], dest_parts: list[pd.DataFrame]) -> None:
    origin = (
        intra.groupby(
            [
                "city_key",
                "origin_prov_name",
                "origin_city_name",
                "origin_town_code",
                "origin_county_name",
                "origin_town_name",
            ],
            dropna=False,
            as_index=False,
        )["commuters_2021"]
        .sum()
        .rename(
            columns={
                "origin_prov_name": "prov_name",
                "origin_city_name": "city_name",
                "origin_town_code": "town_code",
                "origin_county_name": "county_name",
                "origin_town_name": "town_name",
                "commuters_2021": "resident_worker_mass",
            }
        )
    )
    dest = (
        intra.groupby(
            [
                "city_key",
                "dest_prov_name",
                "dest_city_name",
                "dest_town_code",
                "dest_county_name",
                "dest_town_name",
            ],
            dropna=False,
            as_index=False,
        )["commuters_2021"]
        .sum()
        .rename(
            columns={
                "dest_prov_name": "prov_name",
                "dest_city_name": "city_name",
                "dest_town_code": "town_code",
                "dest_county_name": "county_name",
                "dest_town_name": "town_name",
                "commuters_2021": "workplace_commuter_mass",
            }
        )
    )
    origin_parts.append(origin)
    dest_parts.append(dest)


def combine_town_marginals(parts: list[pd.DataFrame], value_col: str) -> pd.DataFrame:
    if not parts:
        return pd.DataFrame()
    data = pd.concat(parts, ignore_index=True)
    group_cols = ["city_key", "prov_name", "city_name", "town_code"]
    agg = {value_col: "sum", "county_name": "first", "town_name": "first"}
    out = data.groupby(group_cols, as_index=False, dropna=False).agg(agg)
    return out


def build_town_table(origin: pd.DataFrame, dest: pd.DataFrame, core_share: float) -> pd.DataFrame:
    town = origin.merge(
        dest,
        on=["city_key", "prov_name", "city_name", "town_code"],
        how="outer",
        suffixes=("_res", "_work"),
    )
    town["county_name"] = town["county_name_res"].fillna(town["county_name_work"])
    town["town_name"] = town["town_name_res"].fillna(town["town_name_work"])
    town = town.drop(columns=["county_name_res", "county_name_work", "town_name_res", "town_name_work"])
    town["resident_worker_mass"] = town["resident_worker_mass"].fillna(0.0).astype(float)
    town["workplace_commuter_mass"] = town["workplace_commuter_mass"].fillna(0.0).astype(float)
    town["job_residence_ratio"] = town["workplace_commuter_mass"] / town["resident_worker_mass"].replace(0.0, np.nan)
    city_totals = (
        town.groupby("city_key", as_index=False)
        .agg(
            city_workplace_commuter_mass=("workplace_commuter_mass", "sum"),
            city_resident_worker_mass=("resident_worker_mass", "sum"),
            n_towns=("town_code", "nunique"),
        )
    )
    town = town.merge(city_totals, on="city_key", how="left")
    town["workplace_share_city"] = town["workplace_commuter_mass"] / town["city_workplace_commuter_mass"].replace(0.0, np.nan)
    town = town.sort_values(["city_key", "workplace_commuter_mass", "town_code"], ascending=[True, False, True]).copy()
    town["workplace_rank_city"] = town.groupby("city_key").cumcount() + 1
    town["cum_workplace_mass"] = town.groupby("city_key")["workplace_commuter_mass"].cumsum()
    town["cum_workplace_share"] = town["cum_workplace_mass"] / town["city_workplace_commuter_mass"].replace(0.0, np.nan)
    town["prev_cum_workplace_share"] = (
        town.groupby("city_key")["cum_workplace_share"].shift(1).fillna(0.0)
    )
    town["is_center"] = (town["prev_cum_workplace_share"] < core_share) & town["workplace_commuter_mass"].gt(0)

    city_median_ratio = (
        town.replace([np.inf, -np.inf], np.nan)
        .groupby("city_key")["job_residence_ratio"]
        .median()
        .rename("city_median_job_residence_ratio")
        .reset_index()
    )
    town = town.merge(city_median_ratio, on="city_key", how="left")
    town["is_job_intensity_above_city_median"] = town["job_residence_ratio"] >= town["city_median_job_residence_ratio"]
    town["center_definition"] = f"top_workplace_mass_until_{core_share:.2f}_city_share"
    return town


def add_coords(town: pd.DataFrame, centers_path: Path) -> tuple[pd.DataFrame, dict]:
    if not centers_path.exists():
        town["coord_source"] = "missing_file"
        town["lon_wgs84"] = np.nan
        town["lat_wgs84"] = np.nan
        return town, {"town_centers_path": str(centers_path), "exists": False}
    centers = pd.read_parquet(centers_path)
    centers["town_code"] = pd.to_numeric(centers["towncode"], errors="coerce").astype("Int64")
    ok_office = centers["office_lon"].notna() & centers["office_lat"].notna()
    ok_centroid = centers["centroid_lon"].notna() & centers["centroid_lat"].notna()
    centers["lon_wgs84"] = np.where(ok_office, centers["office_lon"], np.where(ok_centroid, centers["centroid_lon"], np.nan))
    centers["lat_wgs84"] = np.where(ok_office, centers["office_lat"], np.where(ok_centroid, centers["centroid_lat"], np.nan))
    centers["coord_source"] = np.where(ok_office, "office", np.where(ok_centroid, "centroid", "missing"))
    keep = centers[["town_code", "lon_wgs84", "lat_wgs84", "coord_source"]].dropna(subset=["town_code"])
    keep["town_code"] = keep["town_code"].astype(np.int64)
    out = town.merge(keep.drop_duplicates("town_code"), on="town_code", how="left")
    summary = {
        "town_centers_path": str(centers_path),
        "exists": True,
        "n_rows": int(len(centers)),
        "n_office": int((centers["coord_source"] == "office").sum()),
        "n_centroid": int((centers["coord_source"] == "centroid").sum()),
        "n_missing": int((centers["coord_source"] == "missing").sum()),
        "n_town_table_missing_coord": int(out["lon_wgs84"].isna().sum()),
    }
    return out, summary


def summarize_city_center_definition(town: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for city_key, grp in town.groupby("city_key", sort=False):
        centers = grp.loc[grp["is_center"]]
        rows.append(
            {
                "city_key": city_key,
                "prov_name": grp["prov_name"].iloc[0],
                "city_name": grp["city_name"].iloc[0],
                "n_towns": int(grp["town_code"].nunique()),
                "n_center_towns": int(centers["town_code"].nunique()),
                "center_workplace_mass": float(centers["workplace_commuter_mass"].sum()),
                "city_workplace_commuter_mass": float(grp["workplace_commuter_mass"].sum()),
                "center_workplace_share": float(
                    centers["workplace_commuter_mass"].sum() / grp["workplace_commuter_mass"].sum()
                )
                if grp["workplace_commuter_mass"].sum() > 0
                else np.nan,
                "center_resident_worker_mass": float(centers["resident_worker_mass"].sum()),
                "center_resident_worker_share": float(
                    centers["resident_worker_mass"].sum() / grp["resident_worker_mass"].sum()
                )
                if grp["resident_worker_mass"].sum() > 0
                else np.nan,
                "top_town_workplace_share": float(grp["workplace_share_city"].max()),
                "median_job_residence_ratio": float(grp["city_median_job_residence_ratio"].iloc[0]),
                "center_mean_job_residence_ratio": float(
                    centers["workplace_commuter_mass"].sum() / centers["resident_worker_mass"].sum()
                )
                if centers["resident_worker_mass"].sum() > 0
                else np.nan,
            }
        )
    return pd.DataFrame(rows).sort_values("city_workplace_commuter_mass", ascending=False)


def classify_flow(origin_center: pd.Series, dest_center: pd.Series) -> pd.Series:
    return np.select(
        [
            origin_center & dest_center,
            origin_center & ~dest_center,
            ~origin_center & dest_center,
            ~origin_center & ~dest_center,
        ],
        FLOW_ORDER,
        default="missing",
    )


def append_flow_summary(
    intra: pd.DataFrame,
    center_lookup: dict[int, bool],
    flow_parts: list[pd.DataFrame],
) -> None:
    use = intra.copy()
    use["origin_center"] = use["origin_town_code"].map(center_lookup)
    use["dest_center"] = use["dest_town_code"].map(center_lookup)
    use = use.loc[use["origin_center"].notna() & use["dest_center"].notna()].copy()
    use["origin_center"] = use["origin_center"].astype(bool)
    use["dest_center"] = use["dest_center"].astype(bool)
    use["flow_type"] = classify_flow(use["origin_center"], use["dest_center"])
    use["same_town"] = use["origin_town_code"].eq(use["dest_town_code"])
    for scope, mask in [
        ("all_intra", pd.Series(True, index=use.index)),
        ("inter_town", ~use["same_town"]),
    ]:
        sub = use.loc[mask]
        if sub.empty:
            continue
        grp = (
            sub.groupby(["city_key", "origin_prov_name", "origin_city_name", "flow_type"], as_index=False)[
                "commuters_2021"
            ]
            .sum()
            .rename(
                columns={
                    "origin_prov_name": "prov_name",
                    "origin_city_name": "city_name",
                    "commuters_2021": "commuters",
                }
            )
        )
        grp["sample_scope"] = scope
        flow_parts.append(grp)


def combine_flow_parts(parts: list[pd.DataFrame]) -> pd.DataFrame:
    if not parts:
        return pd.DataFrame()
    flow = pd.concat(parts, ignore_index=True)
    flow = flow.groupby(["sample_scope", "city_key", "prov_name", "city_name", "flow_type"], as_index=False)[
        "commuters"
    ].sum()
    totals = (
        flow.groupby(["sample_scope", "city_key"], as_index=False)["commuters"]
        .sum()
        .rename(columns={"commuters": "city_scope_commuters"})
    )
    flow = flow.merge(totals, on=["sample_scope", "city_key"], how="left")
    flow["share_city_scope"] = flow["commuters"] / flow["city_scope_commuters"].replace(0.0, np.nan)
    flow["flow_type"] = pd.Categorical(flow["flow_type"], categories=FLOW_ORDER, ordered=True)
    return flow.sort_values(["sample_scope", "city_scope_commuters", "city_key", "flow_type"], ascending=[True, False, True, True])


def build_national_summary(flow: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for scope, grp in flow.groupby("sample_scope", sort=False):
        total = float(grp["commuters"].sum())
        by_type = grp.groupby("flow_type", observed=False)["commuters"].sum().reindex(FLOW_ORDER, fill_value=0.0)
        for flow_type, value in by_type.items():
            rows.append(
                {
                    "sample_scope": scope,
                    "flow_type": flow_type,
                    "commuters": float(value),
                    "share": float(value / total) if total > 0 else np.nan,
                }
            )
        pc = float(by_type.get("P_to_C", 0.0))
        cp = float(by_type.get("C_to_P", 0.0))
        rows.append(
            {
                "sample_scope": scope,
                "flow_type": "P_to_C_over_C_to_P",
                "commuters": np.nan,
                "share": pc / cp if cp > 0 else np.inf,
            }
        )
        rows.append(
            {
                "sample_scope": scope,
                "flow_type": "directional_imbalance_P_to_C_minus_C_to_P_share",
                "commuters": pc - cp,
                "share": (pc - cp) / total if total > 0 else np.nan,
            }
        )
    return pd.DataFrame(rows)


def build_city_wide(flow: pd.DataFrame) -> pd.DataFrame:
    wide = flow.pivot_table(
        index=["sample_scope", "city_key", "prov_name", "city_name", "city_scope_commuters"],
        columns="flow_type",
        values="commuters",
        aggfunc="sum",
        fill_value=0.0,
        observed=False,
    ).reset_index()
    for col in FLOW_ORDER:
        if col not in wide.columns:
            wide[col] = 0.0
        wide[f"{col}_share"] = wide[col] / wide["city_scope_commuters"].replace(0.0, np.nan)
    wide["P_to_C_over_C_to_P"] = wide["P_to_C"] / wide["C_to_P"].replace(0.0, np.nan)
    wide["P_to_C_minus_C_to_P_share"] = (wide["P_to_C"] - wide["C_to_P"]) / wide["city_scope_commuters"].replace(0.0, np.nan)
    return wide.sort_values(["sample_scope", "city_scope_commuters"], ascending=[True, False])


def plot_national_heatmap(summary: pd.DataFrame, out_stem: Path) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(6.2, 3.0), constrained_layout=True)
    for ax, scope in zip(axes, ["all_intra", "inter_town"]):
        sub = summary.loc[(summary["sample_scope"] == scope) & summary["flow_type"].isin(FLOW_ORDER)].copy()
        vals = sub.set_index("flow_type")["share"].reindex(FLOW_ORDER).to_numpy().reshape(2, 2)
        im = ax.imshow(vals * 100.0, cmap="YlOrRd", vmin=0, vmax=max(50, np.nanmax(vals * 100.0)))
        ax.set_xticks([0, 1], ["Destination\ncenter", "Destination\nperiphery"])
        ax.set_yticks([0, 1], ["Origin\ncenter", "Origin\nperiphery"])
        title = "All intra-city" if scope == "all_intra" else "Inter-town only"
        ax.set_title(title)
        for i in range(2):
            for j in range(2):
                ax.text(j, i, f"{vals[i, j] * 100:.1f}%", ha="center", va="center", fontsize=8, color="#222222")
    cbar = fig.colorbar(im, ax=axes, fraction=0.035, pad=0.02)
    cbar.set_label("Share of commuters")
    fig.suptitle("Center-periphery commuting matrix")
    for suffix in [".png", ".pdf", ".svg"]:
        fig.savefig(out_stem.with_suffix(suffix), bbox_inches="tight")
    plt.close(fig)


def plot_city_directional_index(city_wide: pd.DataFrame, out_stem: Path, top_n: int = 40) -> None:
    sub = city_wide.loc[city_wide["sample_scope"] == "inter_town"].head(top_n).copy()
    sub = sub.sort_values("P_to_C_minus_C_to_P_share")
    labels = sub["city_name"].astype(str).to_list()
    values = sub["P_to_C_minus_C_to_P_share"].to_numpy() * 100.0
    fig, ax = plt.subplots(figsize=(5.8, 7.0), constrained_layout=True)
    colors = np.where(values >= 0, "#C44E52", "#4C78A8")
    ax.barh(np.arange(len(sub)), values, color=colors, alpha=0.86)
    ax.axvline(0, color="#333333", linewidth=0.8)
    ax.set_yticks(np.arange(len(sub)), labels)
    ax.set_xlabel("P->C minus C->P, share of inter-town commuters (pp)")
    ax.set_title(f"Directional center-bound imbalance, top {len(sub)} cities by flow")
    for suffix in [".png", ".pdf", ".svg"]:
        fig.savefig(out_stem.with_suffix(suffix), bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    args = parse_args()
    if not (0 < args.core_workplace_share <= 1):
        raise ValueError("--core-workplace-share must be in (0, 1].")

    dta_path = first_existing_path(
        args.commute_dta_path,
        ["raw_data/town/commuting_town_2021.dta", "raw_data/commuting_town_2021.dta"],
    )
    centers_path = first_existing_path(
        args.town_centers_parquet,
        ["raw_data/town/commuting_town_2021_centers.parquet", "raw_data/commuting_town_2021_centers.parquet"],
    )
    version_root = resolve_path(args.output_dir) / args.version_id
    dirs = ensure_version_dirs(version_root)
    configure_plots(version_root.parent)

    origin_parts: list[pd.DataFrame] = []
    dest_parts: list[pd.DataFrame] = []
    n_chunks = 0
    n_rows = 0
    n_intra_rows = 0
    for chunk in read_stata_chunks(dta_path, args.chunksize):
        n_chunks += 1
        clean = clean_chunk(chunk)
        intra = keep_intra_city(clean)
        n_rows += int(len(clean))
        n_intra_rows += int(len(intra))
        append_town_marginals(intra, origin_parts, dest_parts)
        print(f"[pass1] chunk={n_chunks:,} rows={len(clean):,} intra={len(intra):,}")
        if args.max_chunks > 0 and n_chunks >= args.max_chunks:
            break

    origin = combine_town_marginals(origin_parts, "resident_worker_mass")
    dest = combine_town_marginals(dest_parts, "workplace_commuter_mass")
    town = build_town_table(origin, dest, args.core_workplace_share)
    town, coord_summary = add_coords(town, centers_path)
    center_summary = summarize_city_center_definition(town)
    center_lookup = town.set_index("town_code")["is_center"].to_dict()

    flow_parts: list[pd.DataFrame] = []
    n_chunks_pass2 = 0
    for chunk in read_stata_chunks(dta_path, args.chunksize):
        n_chunks_pass2 += 1
        clean = clean_chunk(chunk)
        intra = keep_intra_city(clean)
        append_flow_summary(intra, center_lookup, flow_parts)
        print(f"[pass2] chunk={n_chunks_pass2:,} rows={len(clean):,} intra={len(intra):,}")
        if args.max_chunks > 0 and n_chunks_pass2 >= args.max_chunks:
            break

    flow = combine_flow_parts(flow_parts)
    national = build_national_summary(flow)
    city_wide = build_city_wide(flow)

    town.to_csv(dirs["data"] / "town_center_classification.csv", index=False)
    flow.to_csv(dirs["metrics"] / "city_center_periphery_flows_long.csv", index=False)
    city_wide.to_csv(dirs["metrics"] / "city_center_periphery_flows_wide.csv", index=False)
    national.to_csv(dirs["metrics"] / "national_center_periphery_2x2_summary.csv", index=False)
    center_summary.to_csv(dirs["metrics"] / "city_center_definition_summary.csv", index=False)
    plot_national_heatmap(national, dirs["figures"] / "figure_national_center_periphery_2x2")
    plot_city_directional_index(city_wide, dirs["figures"] / "figure_city_center_bound_imbalance")

    manifest = {
        "version_id": args.version_id,
        "commute_dta_path": str(dta_path),
        "town_centers_parquet": str(centers_path),
        "core_workplace_share": args.core_workplace_share,
        "chunksize": args.chunksize,
        "max_chunks": args.max_chunks,
        "n_chunks_pass1": n_chunks,
        "n_chunks_pass2": n_chunks_pass2,
        "n_positive_rows_read_pass1": n_rows,
        "n_intra_city_rows_pass1": n_intra_rows,
        "coord_summary": coord_summary,
        "outputs": {
            "town_classification": str(dirs["data"] / "town_center_classification.csv"),
            "city_flows_long": str(dirs["metrics"] / "city_center_periphery_flows_long.csv"),
            "city_flows_wide": str(dirs["metrics"] / "city_center_periphery_flows_wide.csv"),
            "national_summary": str(dirs["metrics"] / "national_center_periphery_2x2_summary.csv"),
            "city_center_definition_summary": str(dirs["metrics"] / "city_center_definition_summary.csv"),
        },
        "notes": [
            "Raw data are read in place. The script does not move or rewrite files under raw_data.",
            "Center towns are defined within each prefecture-level city by sorting townships/subdistricts by workplace-side commuter mass and keeping towns until the cumulative workplace share reaches the requested threshold.",
            "The script reports both all intra-city flows and inter-town flows. Inter-town is the preferred descriptive sample for directional commuting pressure.",
            "Resident and workplace masses are row and column sums of the commuting OD matrix, not full census residential population or full employment stock.",
        ],
    }
    (dirs["metrics"] / "run_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[done] wrote {version_root}")


if __name__ == "__main__":
    main()
