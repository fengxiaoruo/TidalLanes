#!/usr/bin/env python3
"""Exploratory time-RD / interrupted-time-series analysis for tidal-lane changes."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import statsmodels.api as sm


SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR / "batch_outputs" / "20260710_caitian_identification_final"
ALIGN = ROOT / "stream_alignment_v1"
OUT = ROOT / "rdit_v1"
GROUPS = ["inbound", "outbound"]
DIRS = ["north_to_south", "south_to_north"]
DIR_LABELS = {"north_to_south": "North → South", "south_to_north": "South → North"}
COLORS = {"north_to_south": "#2563eb", "south_to_north": "#d97706", "delta": "#7c3aed"}
CUTOFFS = {
    "opening": pd.Timestamp("2026-07-10 16:31:00"),
    "recovery": pd.Timestamp("2026-07-10 21:33:25"),
}
BANDWIDTHS = [10, 15, 20, 30, 45]


def load_bins() -> pd.DataFrame:
    frame = pd.read_csv(ALIGN / "smoothed_flow_by_bin.csv", parse_dates=["bin_start"])
    frame = frame[(frame["regime"].eq("stable_any")) & (frame["bin_width_min"].eq(5))].copy()
    frame["bin_center"] = frame["bin_start"] + pd.to_timedelta(2.5, unit="min")
    return frame


def make_outcomes(frame: pd.DataFrame) -> dict[tuple[str, str], pd.DataFrame]:
    outcomes: dict[tuple[str, str], pd.DataFrame] = {}
    for group in GROUPS:
        sub = frame[frame["group"].eq(group)]
        wide = sub.pivot_table(index="bin_center", columns="geo_direction", values="rate_per_min", aggfunc="first")
        wide = wide.reset_index().sort_values("bin_center")
        wide["delta"] = wide["south_to_north"] - wide["north_to_south"]
        outcomes[(group, "delta")] = wide[["bin_center", "delta"]].rename(columns={"delta": "outcome"})
        for direction in DIRS:
            outcomes[(group, direction)] = wide[["bin_center", direction]].rename(columns={direction: "outcome"})
    return outcomes


def fit_local(sample: pd.DataFrame) -> dict[str, object]:
    sample = sample.dropna(subset=["outcome", "running"]).copy()
    sample["post"] = (sample["running"] >= 0).astype(float)
    sample["post_running"] = sample["post"] * sample["running"]
    n = len(sample)
    pre = int((sample["post"] == 0).sum())
    post = int((sample["post"] == 1).sum())
    out: dict[str, object] = {
        "n": n,
        "n_pre": pre,
        "n_post": post,
        "mean_pre": sample.loc[sample["post"].eq(0), "outcome"].mean(),
        "mean_post": sample.loc[sample["post"].eq(1), "outcome"].mean(),
        "min_abs_running_pre": sample.loc[sample["post"].eq(0), "running"].abs().min(),
        "min_abs_running_post": sample.loc[sample["post"].eq(1), "running"].abs().min(),
        "gap_between_sides": sample.loc[sample["post"].eq(1), "running"].min()
        - sample.loc[sample["post"].eq(0), "running"].max(),
    }
    if n < 6 or pre < 2 or post < 2:
        out.update({"tau": np.nan, "se_tau": np.nan, "p_tau": np.nan, "r2": np.nan, "hac_lags": np.nan})
        return out
    design = sm.add_constant(sample[["post", "running", "post_running"]])
    maxlags = max(1, min(4, n // 4))
    model = sm.OLS(sample["outcome"], design).fit(
        cov_type="HAC", cov_kwds={"maxlags": maxlags, "use_correction": True}
    )
    out.update(
        {
            "tau": model.params["post"],
            "se_tau": model.bse["post"],
            "p_tau": model.pvalues["post"],
            "r2": model.rsquared,
            "hac_lags": maxlags,
        }
    )
    return out


def estimate(outcomes: dict[tuple[str, str], pd.DataFrame]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for cutoff_name, cutoff in CUTOFFS.items():
        for group in GROUPS:
            for outcome_name in ["delta", *DIRS]:
                base = outcomes[(group, outcome_name)].copy()
                base["running"] = (base["bin_center"] - cutoff).dt.total_seconds() / 60.0
                for bandwidth in BANDWIDTHS:
                    sample = base[base["running"].abs().le(bandwidth)].copy()
                    result = fit_local(sample)
                    rows.append(
                        {
                            "cutoff": cutoff_name,
                            "cutoff_time": cutoff,
                            "group": group,
                            "outcome": outcome_name,
                            "bandwidth_min": bandwidth,
                            **result,
                        }
                    )
    return pd.DataFrame(rows)


def make_event_series(outcomes: dict[tuple[str, str], pd.DataFrame]) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    for cutoff_name, cutoff in CUTOFFS.items():
        for group in GROUPS:
            for outcome_name in ["delta", *DIRS]:
                base = outcomes[(group, outcome_name)].copy()
                base["running"] = (base["bin_center"] - cutoff).dt.total_seconds() / 60.0
                base["cutoff"] = cutoff_name
                base["group"] = group
                base["outcome_name"] = outcome_name
                rows.append(base)
    return pd.concat(rows, ignore_index=True)


def plot_event_series(event: pd.DataFrame) -> None:
    fig, axes = plt.subplots(2, 2, figsize=(14, 9), sharex="col", constrained_layout=True)
    for row, cutoff_name in enumerate(CUTOFFS):
        for col, group in enumerate(GROUPS):
            ax = axes[row, col]
            sub = event[(event["cutoff"].eq(cutoff_name)) & (event["group"].eq(group))]
            for outcome_name in ["delta", *DIRS]:
                s = sub[sub["outcome_name"].eq(outcome_name)].sort_values("running")
                for side, side_frame in s.groupby(s["running"].ge(0)):
                    ax.plot(
                        side_frame["running"],
                        side_frame["outcome"],
                        marker="o",
                        ms=3,
                        lw=1.0,
                        color=COLORS[outcome_name],
                        label=("Δ (S→N − N→S)" if outcome_name == "delta" else DIR_LABELS[outcome_name])
                        if not side
                        else "_nolegend_",
                    )
            ax.axvline(0, color="#111827", lw=1)
            ax.set_title(f"{cutoff_name}: {group}", loc="left", fontweight="bold")
            ax.set_ylabel("Vehicles / minute")
            ax.grid(color="#d1d5db", lw=0.6, alpha=0.7)
            ax.spines[["top", "right"]].set_visible(False)
            ax.legend(frameon=False, fontsize=8, loc="best")
            ax.set_xlim(-50, 50)
    for ax in axes[-1, :]:
        ax.set_xlabel("Minutes relative to cutoff")
    fig.suptitle("Exploratory time-RD: 5-minute stable-sample flow", fontsize=15, fontweight="bold")
    fig.savefig(OUT / "01_rdit_event_series.png", dpi=180, bbox_inches="tight")
    plt.close(fig)


def plot_effects(results: pd.DataFrame) -> None:
    frame = results[results["outcome"].eq("delta")].copy()
    fig, axes = plt.subplots(1, 2, figsize=(13, 5.5), sharey=False, constrained_layout=True)
    for ax, cutoff_name in zip(axes, CUTOFFS):
        sub = frame[frame["cutoff"].eq(cutoff_name)]
        for group, marker in zip(GROUPS, ["o", "s"]):
            s = sub[sub["group"].eq(group)].sort_values("bandwidth_min")
            ax.errorbar(
                s["bandwidth_min"],
                s["tau"],
                yerr=1.96 * s["se_tau"],
                marker=marker,
                lw=1.2,
                capsize=3,
                label=group,
            )
        ax.axhline(0, color="#111827", lw=0.9)
        ax.set_title(cutoff_name, loc="left", fontweight="bold")
        ax.set_xlabel("Bandwidth (minutes)")
        ax.set_ylabel("Estimated jump in Δ flow")
        ax.grid(axis="y", color="#d1d5db", lw=0.6, alpha=0.7)
        ax.spines[["top", "right"]].set_visible(False)
        ax.legend(frameon=False)
    fig.suptitle("Exploratory RDiT estimates: directional flow difference", fontsize=15, fontweight="bold")
    fig.savefig(OUT / "02_rdit_delta_effects_by_bandwidth.png", dpi=180, bbox_inches="tight")
    plt.close(fig)


def write_notes(results: pd.DataFrame) -> None:
    primary = results[(results["bandwidth_min"].eq(30)) & (results["outcome"].eq("delta"))]
    lines = [
        "# 探索性时间断点结果",
        "",
        "本结果使用 5 分钟 bin 的稳定样本，估计的是 RDiT / interrupted-time-series 形式，不是严格 sharp RDD。模型允许断点两侧线性趋势不同，标准误使用 HAC。",
        "",
        "## 方向差分主结果（带宽 30 分钟）",
        "",
    ]
    for _, row in primary.iterrows():
        if pd.isna(row["tau"]):
            lines.append(
                f"- {row['cutoff']} / {row['group']}：带宽 30 分钟内前后样本不足，无法估计局部跳变。"
            )
            continue
        lines.append(
            f"- {row['cutoff']} / {row['group']}：Δ流量跳变={row['tau']:.2f}，"
            f"SE={row['se_tau']:.2f}，p={row['p_tau']:.3f}，n={int(row['n'])}，"
            f"断点两侧最近观测距离分别为 {row['min_abs_running_pre']:.1f} 和 {row['min_abs_running_post']:.1f} 分钟，"
            f"两侧观测间隔约 {row['gap_between_sides']:.1f} 分钟。"
        )
    lines.extend(
        [
            "",
            "带宽敏感性：opening / inbound 的 Δ跳变在 30 分钟为 -13.37，但在 45 分钟变为约 1.14；recovery / outbound 在 30 分钟为约 22.94，在 45 分钟为约 10.70。这个变化说明结果主要由过渡空档两侧的外推驱动。",
            "",
            "## 重要限制",
            "",
            "- 开通断点附近有布设空档，恢复断点附近有恢复操作空档；因此估计依赖断点两侧外推，不能当作无缺口的局部 RDD。",
            "- 一天、两个断点、且来源视频覆盖区间不完全相同，当前 p 值只能作为探索性参考。",
            "- 方向差分利用了潮汐车道使两个方向车道数相反变化的结构，但仍不能排除同一时点的交通需求变化和操作影响。",
            "- 正式分析前应加入伪断点、不同 bin、不同带宽、方向混杂过滤和检测质量平衡检查。",
        ]
    )
    (OUT / "rdit_results_notes.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    bins = load_bins()
    outcomes = make_outcomes(bins)
    results = estimate(outcomes)
    event = make_event_series(outcomes)
    results.to_csv(OUT / "rdit_coefficients.csv", index=False)
    event.to_csv(OUT / "rdit_event_series.csv", index=False)
    plot_event_series(event)
    plot_effects(results)
    write_notes(results)
    print(results[(results["outcome"].eq("delta")) & (results["bandwidth_min"].eq(30))][
        ["cutoff", "group", "tau", "se_tau", "p_tau", "n", "n_pre", "n_post", "gap_between_sides"
    ]].to_string(index=False))
    print(f"output={OUT}")


if __name__ == "__main__":
    main()
