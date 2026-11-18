from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
REF = ROOT / "outputs" / "reference_results"


def _require(path: Path) -> Path:
    if not path.exists():
        raise SystemExit(f"METRIC CHECK FAILED: missing {path.relative_to(ROOT)}")
    return path


def _assert_close(label: str, value: float, expected: float, tol: float = 1e-9) -> None:
    if not np.isfinite(value) or abs(value - expected) > tol:
        raise SystemExit(
            f"METRIC CHECK FAILED: {label} expected {expected:.12g}, got {value:.12g}"
        )


def _duncan(jobs: np.ndarray, residents: np.ndarray) -> float:
    jobs = np.asarray(jobs, dtype=float)
    residents = np.asarray(residents, dtype=float)
    if jobs.sum() <= 0 or residents.sum() <= 0:
        raise SystemExit("METRIC CHECK FAILED: Duncan inputs have zero mass")
    return float(0.5 * np.abs(jobs / jobs.sum() - residents / residents.sum()).sum())


def _grid_floor(jobs: np.ndarray, residents: np.ndarray) -> tuple[float, float]:
    rng = np.random.default_rng(20260702)
    probabilities = residents / residents.sum()
    total_jobs = int(round(float(jobs.sum())))
    sims = np.empty(400)
    for idx in range(len(sims)):
        sims[idx] = _duncan(rng.multinomial(total_jobs, probabilities), residents)
    return float(sims.mean()), float(np.quantile(sims, 0.95))


def _weighted_median(values: pd.Series, weights: pd.Series) -> float:
    x = pd.to_numeric(values, errors="coerce").to_numpy(dtype=float)
    w = pd.to_numeric(weights, errors="coerce").to_numpy(dtype=float)
    mask = np.isfinite(x) & np.isfinite(w) & (w > 0)
    if not mask.any():
        raise SystemExit("METRIC CHECK FAILED: empty weighted median input")
    x = x[mask]
    w = w[mask]
    order = np.argsort(x)
    x = x[order]
    w = w[order]
    return float(x[np.searchsorted(np.cumsum(w), 0.5 * w.sum(), side="left")])


def _weighted_share_le(values: pd.Series, weights: pd.Series, threshold: float) -> float:
    x = pd.to_numeric(values, errors="coerce").to_numpy(dtype=float)
    w = pd.to_numeric(weights, errors="coerce").to_numpy(dtype=float)
    mask = np.isfinite(x) & np.isfinite(w) & (w > 0)
    if not mask.any():
        raise SystemExit("METRIC CHECK FAILED: empty weighted share input")
    return float(w[mask][x[mask] <= threshold].sum() / w[mask].sum())


def validate_slide_descriptive_metrics(root: Path = ROOT) -> list[str]:
    ref = root / "outputs" / "reference_results"

    grid = pd.read_csv(_require(ref / "beijing_grid_population_clusters_square.csv"))
    jobs = pd.to_numeric(grid["jobs"], errors="coerce").fillna(0.0).clip(lower=0).to_numpy()
    residents = (
        pd.to_numeric(grid["residents"], errors="coerce")
        .fillna(0.0)
        .clip(lower=0)
        .to_numpy()
    )
    grid_d = _duncan(jobs, residents)
    floor_mean, floor_p95 = _grid_floor(jobs, residents)
    _assert_close("Beijing grid Duncan D", grid_d, 0.25989580900225895)
    _assert_close("Beijing grid Duncan random floor mean", floor_mean, 0.004435129776613738)
    _assert_close("Beijing grid Duncan random floor p95", floor_p95, 0.00461155800754182)

    cross = pd.read_csv(_require(ref / "beijing_duncan_cross_city.csv"))
    beijing = cross[cross["city_key"].astype(str).str.contains("北京")]
    if len(cross) != 100 or len(beijing) != 1:
        raise SystemExit("METRIC CHECK FAILED: cross-city Duncan benchmark shape changed")
    beijing_row = beijing.iloc[0]
    if int(beijing_row["rank"]) != 1:
        raise SystemExit("METRIC CHECK FAILED: Beijing is no longer rank 1 in Duncan table")
    _assert_close("Beijing township Duncan D", float(beijing_row["duncan_D"]), 0.2020698604761652)
    _assert_close("Top-100 township Duncan median", float(cross["duncan_D"].median()), 0.08547255159012901)

    sector = pd.read_csv(_require(ref / "beijing_inbound_sector_summary.csv"))
    inbound = float(sector["inbound_flow"].sum())
    reverse = float(sector["reverse_am_flow"].sum())
    share = sector["inbound_share_of_two_way"]
    pooled_share = inbound / (inbound + reverse)
    pooled_ratio = inbound / reverse
    _assert_close("Pooled inbound share", pooled_share, 0.7449753229221374)
    _assert_close("Pooled inbound reverse ratio", pooled_ratio, 2.9211891627832007)
    if int((share > 0.7).sum()) != 11:
        raise SystemExit("METRIC CHECK FAILED: inbound sectors above 0.70 changed")
    _assert_close("Inbound sector min", float(share.min()), 0.5731051099279818)
    _assert_close("Inbound sector median", float(share.median()), 0.7710122629869732)
    _assert_close("Inbound sector max", float(share.max()), 0.857291233082184)

    with _require(ref / "beijing_jobs_housing_mismatch_metrics.json").open() as fh:
        metrics = json.load(fh)
    _assert_close(
        "JSON grid Duncan D",
        float(metrics["separation"]["duncan_dissimilarity_D"]),
        grid_d,
    )
    _assert_close(
        "JSON inbound share",
        float(metrics["direction"]["pooled_inbound_share_of_two_way"]),
        pooled_share,
    )
    _assert_close(
        "JSON inbound reverse ratio",
        float(metrics["direction"]["pooled_inbound_reverse_ratio"]),
        pooled_ratio,
    )

    national = pd.read_csv(_require(ref / "national_pair_summary_baidu2025.csv"))
    national_use = national[national["balanced_sample_flag"].astype(bool)].copy()
    national_median = _weighted_median(
        national_use["am_minmax_ratio"],
        national_use["pop_total_pair"],
    )
    national_share90 = _weighted_share_le(
        national_use["am_minmax_ratio"],
        national_use["pop_total_pair"],
        0.90,
    )
    _assert_close("National AM flow-weighted median SAR", national_median, 0.8780487804878049)
    _assert_close("National AM flow-weighted share SAR <= 0.90", national_share90, 0.5833776352267191)

    national_city = pd.read_csv(_require(ref / "national_city_summary_baidu2025.csv"))
    if len(national_city) != 99:
        raise SystemExit("METRIC CHECK FAILED: national city summary n changed")
    national_corr = pd.read_csv(
        _require(ref / "national_congestion_asymmetry_correlation_baidu2025.csv")
    )
    corr_row = national_corr[
        national_corr["panel"].eq("weighted_am_minmax_ratio_median")
    ].iloc[0]
    if int(corr_row["n"]) != 99:
        raise SystemExit("METRIC CHECK FAILED: national congestion correlation n changed")
    _assert_close("National congestion-asymmetry Pearson r", float(corr_row["pearson_r"]), -0.4759596461589322)

    shenzhen = pd.read_csv(_require(ref / "shenzhen_flow_weighted_asymmetry_summary.csv"))
    sz_all = shenzhen[shenzhen["weight_scheme"].eq("unweighted_potential_grid_pairs")].iloc[0]
    sz_flow = shenzhen[shenzhen["weight_scheme"].eq("observed_flow")].iloc[0]
    _assert_close("Shenzhen all-pair median SAR", float(sz_all["weighted_median_symmetry_ratio"]), 0.8791884606259548)
    _assert_close("Shenzhen all-pair share SAR <= 0.90", float(sz_all["weighted_share_symmetry_le_0_90"]), 0.5745429189610443)
    _assert_close("Shenzhen observed commuter flow", float(sz_flow["weight_sum"]), 4405107.0)
    _assert_close("Shenzhen observed-flow median SAR", float(sz_flow["weighted_median_symmetry_ratio"]), 0.8191057151001394)
    _assert_close("Shenzhen observed-flow share SAR <= 0.90", float(sz_flow["weighted_share_symmetry_le_0_90"]), 0.715613491340846)
    _assert_close(
        "Shenzhen share actual direction slow >= 10%",
        float(sz_flow["weighted_share_actual_direction_slow_ge_1_10"]),
        0.3800879297597084,
    )

    centerline = pd.read_csv(_require(ref / "beijing_centerline_asymmetry_table.csv"))
    centerline_am = centerline[centerline["peak"].eq("AM") & centerline["ratio"].notna()].copy()
    if len(centerline_am) != 3670:
        raise SystemExit("METRIC CHECK FAILED: Beijing AM centerline count changed")
    _assert_close("Beijing AM centerline median SAR", float(centerline_am["ratio"].median()), 0.8835253008134232)
    _assert_close("Beijing AM centerline share SAR <= 0.90", float((centerline_am["ratio"] <= 0.90).mean()), 0.5471389645776567)

    return [
        (
            "descriptive slide metrics ok: "
            f"grid_D={grid_d:.3f}, random_floor={floor_mean:.3f}, "
            f"Beijing_township_D={float(beijing_row['duncan_D']):.3f}, "
            f"inbound_share={pooled_share * 100:.1f}%, "
            f"national_SAR={national_median:.3f}, "
            f"Shenzhen_flow_SAR={float(sz_flow['weighted_median_symmetry_ratio']):.3f}, "
            f"centerline_SAR={float(centerline_am['ratio'].median()):.3f}"
        )
    ]


if __name__ == "__main__":
    for line in validate_slide_descriptive_metrics():
        print(line)
