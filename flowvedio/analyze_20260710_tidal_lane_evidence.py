#!/usr/bin/env python3
"""Summarize descriptive flow and per-lane evidence around tidal deployment."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR / "batch_outputs" / "20260710_caitian_identification_final"
ANALYSIS = ROOT / "analysis_v1"
OUT = ROOT / "tidal_evidence_v1"

LANES = {
    "normal": {"north_to_south": 4, "south_to_north": 3},
    "tidal_deployed": {"north_to_south": 3, "south_to_north": 4},
}
DIR_LABELS = {"north_to_south": "North → South", "south_to_north": "South → North"}
COLORS = {"north_to_south": "#2563eb", "south_to_north": "#d97706"}


def build_comparison() -> pd.DataFrame:
    stats = pd.read_csv(ANALYSIS / "aggregate_stats_by_stream_state_direction.csv")
    stats = stats[
        stats["counting_status"].eq("count")
        & stats["state"].isin(["normal", "tidal_deployed"])
    ].copy()
    stats["lanes"] = [LANES[state][direction] for state, direction in zip(stats["state"], stats["geo_direction"])]
    stats["rate_per_lane_min"] = stats["rate_per_min"] / stats["lanes"]
    normal = stats[stats["state"].eq("normal")].set_index(["group", "geo_direction"])
    tidal = stats[stats["state"].eq("tidal_deployed")].set_index(["group", "geo_direction"])
    rows = []
    for key in sorted(set(normal.index) & set(tidal.index)):
        n, t = normal.loc[key], tidal.loc[key]
        group, direction = key
        rows.append(
            {
                "group": group,
                "geo_direction": direction,
                "normal_rate_per_min": n["rate_per_min"],
                "tidal_rate_per_min": t["rate_per_min"],
                "flow_change_per_min": t["rate_per_min"] - n["rate_per_min"],
                "flow_pct_change": 100 * (t["rate_per_min"] / n["rate_per_min"] - 1),
                "normal_lanes": n["lanes"],
                "tidal_lanes": t["lanes"],
                "normal_rate_per_lane_min": n["rate_per_lane_min"],
                "tidal_rate_per_lane_min": t["rate_per_lane_min"],
                "unit_lane_change_per_min": t["rate_per_lane_min"] - n["rate_per_lane_min"],
                "unit_lane_pct_change": 100 * (t["rate_per_lane_min"] / n["rate_per_lane_min"] - 1),
                "normal_segments": n["segments"],
                "tidal_segments": t["segments"],
                "normal_exposure_min": n["exposure_min"],
                "tidal_exposure_min": t["exposure_min"],
            }
        )
    return pd.DataFrame(rows)


def add_totals(comparison: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for group in sorted(comparison["group"].unique()):
        g = comparison[comparison["group"].eq(group)]
        for state in ["normal", "tidal_deployed"]:
            if state == "normal":
                rate = g["normal_rate_per_min"].sum()
                lanes = 7
                unit = rate / lanes
            else:
                rate = g["tidal_rate_per_min"].sum()
                lanes = 7
                unit = rate / lanes
            rows.append({"group": group, "state": state, "total_rate_per_min": rate, "total_lanes": lanes, "total_rate_per_lane_min": unit})
    totals = pd.DataFrame(rows)
    return totals


def plot_comparison(comparison: pd.DataFrame) -> None:
    fig, axes = plt.subplots(2, 2, figsize=(12, 8), constrained_layout=True, sharex="col")
    for col, group in enumerate(["inbound", "outbound"]):
        frame = comparison[comparison["group"].eq(group)]
        for direction in ["north_to_south", "south_to_north"]:
            row = frame[frame["geo_direction"].eq(direction)].iloc[0]
            axes[0, col].plot(
                [0, 1],
                [row["normal_rate_per_min"], row["tidal_rate_per_min"]],
                marker="o",
                lw=2,
                color=COLORS[direction],
                label=DIR_LABELS[direction],
            )
            axes[1, col].plot(
                [0, 1],
                [row["normal_rate_per_lane_min"], row["tidal_rate_per_lane_min"]],
                marker="o",
                lw=2,
                color=COLORS[direction],
                label=DIR_LABELS[direction],
            )
        axes[0, col].set_title(f"{group}: total flow", loc="left", fontweight="bold")
        axes[1, col].set_title(f"{group}: flow per lane", loc="left", fontweight="bold")
        for row_ax in axes[:, col]:
            row_ax.set_xticks([0, 1], ["Normal", "Tidal deployed"])
            row_ax.grid(axis="y", color="#d1d5db", lw=0.6, alpha=0.7)
            row_ax.spines[["top", "right"]].set_visible(False)
            row_ax.legend(frameon=False, fontsize=9)
    axes[0, 0].set_ylabel("Vehicles / minute")
    axes[1, 0].set_ylabel("Vehicles / lane / minute")
    fig.suptitle("Descriptive tidal-lane flow comparison", fontsize=15, fontweight="bold")
    fig.savefig(OUT / "01_tidal_flow_and_per_lane_comparison.png", dpi=180, bbox_inches="tight")
    plt.close(fig)


def write_notes(comparison: pd.DataFrame, totals: pd.DataFrame) -> None:
    lines = [
        "# 潮汐车道流量与单位车道流量证据",
        "",
        "- 这里的 normal / tidal_deployed 是跨全天稳定片段的描述性汇总，不是同一时刻的严格前后配对。",
        "- 车道数按锁定口径计算：无潮汐时北向南 4、南向北 3；潮汐时北向南 3、南向北 4。",
        "- 两个来源视频流分别汇总，不直接相加。",
        "",
        "## 结论边界",
        "",
        "outbound 来源流显示了与车道重新分配一致的方向性变化：潮汐后南向北总流量上升、北向南下降。但 inbound 来源流没有重复出同样清晰的模式；单位车道流量也没有在两个来源流中一致上升。因此当前只能说存在描述性证据，尚不能说已经识别出潮汐车道的因果影响或容量提升。",
        "",
        "探索性 RDiT 结果受断点附近施工空档影响，不能作为强因果证据。",
        "",
        "## 关键数值",
        "",
        "- inbound：北向南总流量 43.80 → 32.98（-24.7%），南向北 41.51 → 37.66（-9.3%）；单位车道流量分别变化 +0.4% 和 -32.0%。",
        "- outbound：北向南总流量 41.10 → 26.89（-34.6%），南向北 38.69 → 43.87（+13.4%）；单位车道流量分别变化 -12.8% 和 -15.0%。",
        "- 双向合计：inbound 85.32 → 70.64（-17.2%），outbound 79.79 → 70.76（-11.3%）。两种状态总车道数都为 7，因此双向合计的单位车道变化与总流量变化相同。",
    ]
    (OUT / "tidal_evidence_notes.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    comparison = build_comparison()
    totals = add_totals(comparison)
    comparison.to_csv(OUT / "normal_vs_tidal_per_lane.csv", index=False)
    totals.to_csv(OUT / "normal_vs_tidal_total_per_lane.csv", index=False)
    plot_comparison(comparison)
    write_notes(comparison, totals)
    print(comparison[[
        "group", "geo_direction", "normal_rate_per_min", "tidal_rate_per_min", "flow_pct_change",
        "normal_rate_per_lane_min", "tidal_rate_per_lane_min", "unit_lane_pct_change"
    ]].to_string(index=False))
    print("\nTotals")
    print(totals.to_string(index=False))
    print(f"output={OUT}")


if __name__ == "__main__":
    main()
