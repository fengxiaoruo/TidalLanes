#!/usr/bin/env python3
"""Pool the two source streams and run a simple descriptive time breakpoint."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import statsmodels.api as sm


SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR / "batch_outputs" / "20260710_caitian_identification_final"
ALIGN = ROOT / "stream_alignment_v1"
OUT = ROOT / "pooled_breakpoint_v1"
GROUPS = ["inbound", "outbound"]
DIRS = ["north_to_south", "south_to_north"]
DIR_LABELS = {"north_to_south": "North → South", "south_to_north": "South → North"}
CUTOFFS = {
    "opening": {
        "time": pd.Timestamp("2026-07-10 16:31:00"),
        "pre_regime": "normal",
        "post_regime": "tidal_deployed",
    },
    "recovery": {
        "time": pd.Timestamp("2026-07-10 21:33:25"),
        "pre_regime": "tidal_deployed",
        "post_regime": "normal",
    },
}
BANDWIDTHS = [30, 45, 60]
FUSIONS = ["all_available", "both_streams"]


def load_pooled_bins() -> pd.DataFrame:
    """Average rates across source streams without adding vehicle counts."""
    frame = pd.read_csv(ALIGN / "smoothed_flow_by_bin.csv", parse_dates=["bin_start"])
    frame = frame[
        frame["bin_width_min"].eq(5)
        & frame["regime"].isin(["normal", "tidal_deployed"])
    ].copy()
    frame["bin_center"] = frame["bin_start"] + pd.to_timedelta(2.5, unit="min")
    wide = frame.pivot_table(
        index=["bin_start", "bin_center", "regime"],
        columns=["group", "geo_direction"],
        values="rate_per_min",
        aggfunc="first",
    )
    pooled = wide.reset_index()
    pooled.columns = [
        column[0] if isinstance(column, tuple) and column[1] == "" else column
        for column in pooled.columns
    ]
    for direction in DIRS:
        cols = [(group, direction) for group in GROUPS]
        values = wide.reindex(columns=cols)
        pooled[f"{direction}_rate"] = values.mean(axis=1, skipna=True).to_numpy()
        pooled[f"{direction}_n_streams"] = values.notna().sum(axis=1).to_numpy()

    n2s = pooled["north_to_south_rate"]
    s2n = pooled["south_to_north_rate"]
    pooled["total_flow"] = n2s + s2n
    pooled["direction_difference"] = s2n - n2s
    pooled["direction_balance"] = pooled["direction_difference"] / pooled["total_flow"]
    pooled["south_to_north_share"] = s2n / pooled["total_flow"]
    pooled["both_streams"] = (
        pooled["north_to_south_n_streams"].eq(2)
        & pooled["south_to_north_n_streams"].eq(2)
    )
    pooled["both_directions_observed"] = pooled["total_flow"].notna()
    return pooled.sort_values(["bin_start", "regime"]).reset_index(drop=True)


def make_window_sample(
    pooled: pd.DataFrame,
    cutoff_name: str,
    bandwidth: int,
    fusion: str,
) -> pd.DataFrame:
    spec = CUTOFFS[cutoff_name]
    cutoff = spec["time"]
    pre = pooled[
        pooled["regime"].eq(spec["pre_regime"])
        & pooled["bin_center"].lt(cutoff)
        & pooled["bin_center"].ge(cutoff - pd.to_timedelta(bandwidth, unit="min"))
    ].copy()
    post = pooled[
        pooled["regime"].eq(spec["post_regime"])
        & pooled["bin_center"].ge(cutoff)
        & pooled["bin_center"].le(cutoff + pd.to_timedelta(bandwidth, unit="min"))
    ].copy()
    if fusion == "both_streams":
        pre = pre[pre["both_streams"]].copy()
        post = post[post["both_streams"]].copy()
    pre["post"] = 0
    post["post"] = 1
    sample = pd.concat([pre, post], ignore_index=True)
    sample["running"] = (sample["bin_center"] - cutoff).dt.total_seconds() / 60.0
    return sample.sort_values("bin_center").reset_index(drop=True)


def fit_level_shift(sample: pd.DataFrame, outcome: str) -> dict[str, object]:
    sample = sample.dropna(subset=[outcome]).copy()
    n = len(sample)
    pre = int(sample["post"].eq(0).sum())
    post = int(sample["post"].eq(1).sum())
    result: dict[str, object] = {
        "outcome": outcome,
        "n": n,
        "n_pre": pre,
        "n_post": post,
        "mean_pre": sample.loc[sample["post"].eq(0), outcome].mean(),
        "mean_post": sample.loc[sample["post"].eq(1), outcome].mean(),
        "min_abs_running_pre": sample.loc[sample["post"].eq(0), "running"].abs().min(),
        "min_abs_running_post": sample.loc[sample["post"].eq(1), "running"].abs().min(),
        "gap_between_sides": sample.loc[sample["post"].eq(1), "running"].min()
        - sample.loc[sample["post"].eq(0), "running"].max(),
    }
    if n < 6 or pre < 2 or post < 2:
        result.update({"tau": np.nan, "se_tau": np.nan, "p_tau": np.nan, "pct_change": np.nan})
        return result
    design = sm.add_constant(sample[["post"]])
    maxlags = max(1, min(2, n // 4))
    model = sm.OLS(sample[outcome], design).fit(
        cov_type="HAC", cov_kwds={"maxlags": maxlags, "use_correction": True}
    )
    tau = model.params["post"]
    mean_pre = result["mean_pre"]
    result.update(
        {
            "tau": tau,
            "se_tau": model.bse["post"],
            "p_tau": model.pvalues["post"],
            "pct_change": 100 * tau / abs(mean_pre) if mean_pre else np.nan,
            "hac_lags": maxlags,
        }
    )
    return result


def estimate(pooled: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    outcomes = [
        "direction_balance",
        "south_to_north_share",
        "total_flow",
        "north_to_south_rate",
        "south_to_north_rate",
    ]
    for cutoff_name in CUTOFFS:
        for fusion in FUSIONS:
            for bandwidth in BANDWIDTHS:
                sample = make_window_sample(pooled, cutoff_name, bandwidth, fusion)
                for outcome in outcomes:
                    result = fit_level_shift(sample, outcome)
                    rows.append(
                        {
                            "cutoff": cutoff_name,
                            "cutoff_time": CUTOFFS[cutoff_name]["time"],
                            "fusion": fusion,
                            "bandwidth_min": bandwidth,
                            **result,
                        }
                    )
    return pd.DataFrame(rows)


def plot_balance(pooled: pd.DataFrame) -> None:
    fig, axes = plt.subplots(2, 1, figsize=(13, 7.5), constrained_layout=True, sharey=True)
    colors = {"all_available": "#7c3aed", "both_streams": "#111827"}
    labels = {
        "all_available": "mean of available source streams",
        "both_streams": "mean where both source streams are observed",
    }
    for ax, (cutoff_name, spec) in zip(axes, CUTOFFS.items()):
        cutoff = spec["time"]
        for fusion, color in colors.items():
            pre = pooled[
                pooled["regime"].eq(spec["pre_regime"])
                & pooled["bin_center"].lt(cutoff)
                & pooled["bin_center"].ge(cutoff - pd.Timedelta(minutes=60))
            ].copy()
            post = pooled[
                pooled["regime"].eq(spec["post_regime"])
                & pooled["bin_center"].ge(cutoff)
                & pooled["bin_center"].le(cutoff + pd.Timedelta(minutes=60))
            ].copy()
            if fusion == "both_streams":
                pre = pre[pre["both_streams"]]
                post = post[post["both_streams"]]
            sample = pd.concat([pre, post], ignore_index=True).dropna(subset=["direction_balance"])
            sample["running"] = (sample["bin_center"] - cutoff).dt.total_seconds() / 60
            ax.plot(
                sample["running"],
                sample["direction_balance"],
                marker="o",
                ms=4,
                lw=1.5,
                color=color,
                alpha=0.85 if fusion == "all_available" else 0.65,
                label=labels[fusion],
            )
        ax.axvline(0, color="#dc2626", lw=1.2, linestyle="--")
        ax.axhline(0, color="#6b7280", lw=0.8)
        ax.set_title(f"{cutoff_name}: pooled directional balance", loc="left", fontweight="bold")
        ax.set_ylabel("B = (South → North − North → South) / total")
        ax.set_xlim(-60, 60)
        ax.grid(color="#d1d5db", lw=0.6, alpha=0.7)
        ax.spines[["top", "right"]].set_visible(False)
        ax.legend(frameon=False, fontsize=9, loc="best")
    axes[-1].set_xlabel("Minutes relative to cutoff")
    fig.suptitle("Fused two-camera descriptive breakpoint check", fontsize=15, fontweight="bold")
    fig.savefig(OUT / "01_fused_direction_balance_breakpoint.png", dpi=180, bbox_inches="tight")
    plt.close(fig)


def plot_estimates(results: pd.DataFrame) -> None:
    frame = results[
        (results["outcome"].eq("direction_balance"))
        & (results["fusion"].eq("all_available"))
    ].copy()
    fig, ax = plt.subplots(figsize=(11, 5.5), constrained_layout=True)
    offsets = {"opening": -0.12, "recovery": 0.12}
    markers = {30: "o", 45: "s", 60: "^"}
    colors = {"opening": "#2563eb", "recovery": "#d97706"}
    for cutoff_name in CUTOFFS:
        for bandwidth in BANDWIDTHS:
            row = frame[
                frame["cutoff"].eq(cutoff_name) & frame["bandwidth_min"].eq(bandwidth)
            ].iloc[0]
            x = bandwidth + offsets[cutoff_name]
            ax.errorbar(
                x,
                row["tau"],
                yerr=1.96 * row["se_tau"],
                marker=markers[bandwidth],
                color=colors[cutoff_name],
                capsize=3,
                lw=1.2,
                label=cutoff_name if bandwidth == BANDWIDTHS[0] else "_nolegend_",
            )
    ax.axhline(0, color="#111827", lw=0.9)
    ax.set_xticks(BANDWIDTHS)
    ax.set_xlabel("Window half-width (minutes)")
    ax.set_ylabel("Jump in directional balance B")
    ax.set_title("Simple level-shift estimates after pooling source streams", loc="left", fontweight="bold")
    ax.grid(axis="y", color="#d1d5db", lw=0.6, alpha=0.7)
    ax.spines[["top", "right"]].set_visible(False)
    ax.legend(frameon=False)
    fig.savefig(OUT / "02_fused_breakpoint_estimates.png", dpi=180, bbox_inches="tight")
    plt.close(fig)


def write_notes(results: pd.DataFrame, pooled: pd.DataFrame) -> None:
    main = results[
        (results["fusion"].eq("all_available"))
        & (results["outcome"].eq("direction_balance"))
    ].sort_values(["cutoff", "bandwidth_min"])
    strict = results[
        (results["fusion"].eq("both_streams"))
        & (results["outcome"].eq("direction_balance"))
    ].sort_values(["cutoff", "bandwidth_min"])
    lines = [
        "# 两设备融合后的最简单时间断点检验",
        "",
        "## 融合口径",
        "",
        "- 每个 5 分钟 bin 内，分别计算 inbound 和 outbound 对同一地理方向的车辆/分钟。",
        "- 两个来源流在同一 bin 都有观测时取两个 rate 的平均；只有一个来源流有观测时，主结果保留该观测并记录覆盖数。",
        "- 不把两个机位的车辆数直接相加，因为可能对同一车辆重复计数。",
        "- `both_streams` 结果只保留两个来源流都覆盖、且两个地理方向都可计算的 bin，作为测量稳健性检查。",
        "",
        "## 断点模型",
        "",
        "`Y_t = alpha + tau * 1(t >= cutoff) + error_t`。这里是最简单的 level-shift / interrupted-time-series 检验，使用 5 分钟 bin、HAC 标准误；没有加入局部趋势，因此不是严格的 RDD。",
        "- opening：约 16:31，normal 作为断点前，tidal_deployed 作为断点后。",
        "- recovery：约 21:33:25，tidal_deployed 作为断点前，normal 作为断点后。",
        "- 施工、恢复和机位不稳定阶段不进入状态样本；没有视频覆盖的 bin 不填 0。",
        "",
        "## 方向平衡指标",
        "",
        "`B = (South → North - North → South) / (South → North + North → South)`。B=0 表示两个方向流量相等；B>0 表示南向北占比更高；B<0 表示北向南占比更高。它主要刻画方向结构，不是总流量，也不是单位车道流量。",
        "",
        "## 主结果：all_available 融合样本的 B 跳变",
        "",
    ]
    for _, row in main.iterrows():
        if pd.isna(row["tau"]):
            lines.append(
                f"- {row['cutoff']} / {int(row['bandwidth_min'])} 分钟：样本不足，无法估计。"
            )
        else:
            lines.append(
                f"- {row['cutoff']} / {int(row['bandwidth_min'])} 分钟：B 跳变 {row['tau']:.3f}，"
                f"SE={row['se_tau']:.3f}，p={row['p_tau']:.3f}，"
                f"前后均值 {row['mean_pre']:.3f} → {row['mean_post']:.3f}，"
                f"n={int(row['n'])}（前 {int(row['n_pre'])}，后 {int(row['n_post'])}）。"
            )
    lines.extend(
        [
            "",
            "## 严格共同覆盖样本",
            "",
        ]
    )
    for _, row in strict.iterrows():
        if pd.isna(row["tau"]):
            lines.append(
                f"- {row['cutoff']} / {int(row['bandwidth_min'])} 分钟：共同覆盖样本不足，"
                f"n={int(row['n'])}（前 {int(row['n_pre'])}，后 {int(row['n_post'])}），不估计跳变。"
            )
        else:
            lines.append(
                f"- {row['cutoff']} / {int(row['bandwidth_min'])} 分钟：B 跳变 {row['tau']:.3f}，"
                f"前后均值 {row['mean_pre']:.3f} → {row['mean_post']:.3f}，"
                f"n={int(row['n'])}（前 {int(row['n_pre'])}，后 {int(row['n_post'])}）。"
            )
    lines.extend(
        [
            "",
            "## 解释边界",
            "",
            "- 两设备融合降低了单个机位的测量噪声，但不自动增加独立的时间样本量；两个机位共同受同一时点交通需求冲击。",
            "- 当前断点两侧仍有覆盖缺口，且窗口内只有一天、两个政策转换，因此结果只能作为阶段性方向检查。",
            "- 表中的 HAC p 值只是这个极小样本 level-shift 模型的计算结果，不能作为正式显著性证据；当前应优先看方向、量级和不同窗口是否一致。",
            "- 若 opening 的 B 上升、recovery 的 B 下降，且不同窗口和 only-both-streams 结果方向一致，可以作为“方向重分配与潮汐车道切换相符”的初步证据；仍不能单独证明容量提高或因果效应。",
            "- 下一步应在转换前后连续录制、同步设备，并用重复日期、伪断点、局部趋势和不同 bin 宽度来做正式检验。",
        ]
    )
    (OUT / "pooled_breakpoint_notes.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    pooled = load_pooled_bins()
    results = estimate(pooled)
    pooled.to_csv(OUT / "pooled_5min_series.csv", index=False)
    results.to_csv(OUT / "pooled_breakpoint_results.csv", index=False)
    plot_balance(pooled)
    plot_estimates(results)
    write_notes(results, pooled)
    print(
        results[
            results["outcome"].eq("direction_balance")
            & results["fusion"].eq("all_available")
        ][
            ["cutoff", "bandwidth_min", "tau", "se_tau", "p_tau", "n", "n_pre", "n_post"]
        ].to_string(index=False)
    )
    print(f"output={OUT}")


if __name__ == "__main__":
    main()
