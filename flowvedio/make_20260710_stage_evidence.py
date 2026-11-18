#!/usr/bin/env python3
"""Create presentation-oriented interim evidence figures for the Caitian Road pilot."""

from __future__ import annotations

from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.patches import Patch


SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR / "batch_outputs" / "20260710_caitian_identification_final"
EVIDENCE = ROOT / "tidal_evidence_v1"
OUT = ROOT / "stage_evidence_v1"

DIRS = ["north_to_south", "south_to_north"]
DIR_LABELS = {"north_to_south": "North → South", "south_to_north": "South → North"}
DIR_COLORS = {"north_to_south": "#2563eb", "south_to_north": "#d97706"}
STATE_COLORS = {
    "normal": "#16a34a",
    "tidal_deployed": "#2563eb",
    "deployment_in_progress": "#f59e0b",
    "reversal_in_progress": "#dc2626",
    "camera_unstable": "#6b7280",
}


def load_comparison() -> pd.DataFrame:
    frame = pd.read_csv(EVIDENCE / "normal_vs_tidal_per_lane.csv")
    rows = []
    for _, row in frame.iterrows():
        balance_normal = (
            row["normal_rate_per_min"]
            if row["geo_direction"] == "south_to_north"
            else -row["normal_rate_per_min"]
        )
        rows.append(row)
    return frame


def plot_indicators(frame: pd.DataFrame) -> None:
    fig, axes = plt.subplots(2, 2, figsize=(13, 8), constrained_layout=True)
    for col, group in enumerate(["inbound", "outbound"]):
        sub = frame[frame["group"].eq(group)].set_index("geo_direction")
        x = [0, 1]
        for direction in DIRS:
            row = sub.loc[direction]
            axes[0, col].plot(
                x,
                [row["normal_rate_per_min"], row["tidal_rate_per_min"]],
                marker="o",
                lw=2,
                color=DIR_COLORS[direction],
                label=DIR_LABELS[direction],
            )
            axes[1, col].plot(
                x,
                [row["normal_rate_per_lane_min"], row["tidal_rate_per_lane_min"]],
                marker="o",
                lw=2,
                color=DIR_COLORS[direction],
                label=DIR_LABELS[direction],
            )
        normal_ns = sub.loc["north_to_south", "normal_rate_per_min"]
        normal_sn = sub.loc["south_to_north", "normal_rate_per_min"]
        tidal_ns = sub.loc["north_to_south", "tidal_rate_per_min"]
        tidal_sn = sub.loc["south_to_north", "tidal_rate_per_min"]
        balance = [(normal_sn - normal_ns) / (normal_sn + normal_ns), (tidal_sn - tidal_ns) / (tidal_sn + tidal_ns)]
        axes[0, col].set_title(f"{group}: directional flow", loc="left", fontweight="bold")
        axes[1, col].set_title(f"{group}: flow per lane", loc="left", fontweight="bold")
        axes[0, col].set_ylabel("Vehicles / minute")
        axes[1, col].set_ylabel("Vehicles / lane / minute")
        axes[0, col].set_xticks(x, ["Normal", "Tidal deployed"])
        axes[1, col].set_xticks(x, ["Normal", "Tidal deployed"])
        axes[0, col].legend(frameon=False, fontsize=9)
        axes[1, col].legend(frameon=False, fontsize=9)
        axes[0, col].grid(axis="y", color="#d1d5db", lw=0.6, alpha=0.7)
        axes[1, col].grid(axis="y", color="#d1d5db", lw=0.6, alpha=0.7)
        axes[0, col].spines[["top", "right"]].set_visible(False)
        axes[1, col].spines[["top", "right"]].set_visible(False)

        ax_balance = axes[0, col].twinx()
        ax_balance.plot(x, balance, color="#7c3aed", marker="D", ms=5, lw=1.5, ls="--")
        ax_balance.axhline(0, color="#6b7280", lw=0.8)
        ax_balance.set_ylim(-0.35, 0.35)
        ax_balance.set_ylabel("Directional balance", color="#7c3aed")
        ax_balance.tick_params(axis="y", colors="#7c3aed")
        ax_balance.spines["top"].set_visible(False)
    fig.suptitle("Interim evidence: directional reallocation and lane-normalized flow", fontsize=15, fontweight="bold")
    fig.savefig(OUT / "01_stage_directional_evidence.png", dpi=180, bbox_inches="tight")
    plt.close(fig)


def plot_coverage_timeline() -> None:
    segments = pd.read_csv(ROOT / "summary_by_video.csv", parse_dates=["start_local", "end_local"])
    fig, axes = plt.subplots(2, 1, figsize=(15, 5.5), sharex=True, constrained_layout=True)
    for ax, group in zip(axes, ["inbound", "outbound"]):
        sub = segments[segments["group"].eq(group)].sort_values("start_local")
        for _, row in sub.iterrows():
            left = mdates.date2num(row["start_local"])
            width = mdates.date2num(row["end_local"]) - left
            color = STATE_COLORS.get(row["state"], "#9ca3af")
            ax.barh(0, width, left=left, height=0.45, color=color, alpha=0.82)
            if width > 0.015:
                ax.text(left + width / 2, 0, row["state"].replace("_", " "), ha="center", va="center", fontsize=8, color="white")
        ax.axvline(pd.Timestamp("2026-07-10 16:31:00"), color="#111827", lw=1.1, ls="--")
        ax.axvline(pd.Timestamp("2026-07-10 21:33:25"), color="#111827", lw=1.1, ls="--")
        ax.set_title(f"{group} source stream", loc="left", fontweight="bold")
        ax.set_yticks([])
        ax.set_ylim(-0.45, 0.45)
        ax.spines[["top", "right", "left"]].set_visible(False)
    axes[-1].xaxis.set_major_locator(mdates.MinuteLocator(byminute=[0, 30]))
    axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    handles = [Patch(color=color, label=state.replace("_", " ")) for state, color in STATE_COLORS.items()]
    handles.append(plt.Line2D([0], [0], color="#111827", ls="--", label="planned cutoff"))
    fig.legend(handles=handles, loc="lower center", ncol=6, frameon=False, bbox_to_anchor=(0.5, -0.04))
    fig.suptitle("Interim evidence: state coverage and transition gaps", fontsize=15, fontweight="bold")
    fig.savefig(OUT / "02_state_coverage_and_gaps.png", dpi=180, bbox_inches="tight")
    plt.close(fig)


def write_notes(frame: pd.DataFrame) -> None:
    lines = [
        "# 测试阶段：阶段性证据与下一步识别方向",
        "",
        "## 当前可以展示的结果",
        "",
        "- outbound 来源流出现了方向性重分配：潮汐后南向北流量上升、北向南流量下降；这与车道从 3/4 调整为 4/3 的方向一致。",
        "- inbound 来源流没有完全重复这一模式，因此当前图表应表述为“初步方向性证据”，而不是潮汐车道的最终效果。",
        "- 单位车道流量没有稳定提高，说明后续需要同时采集速度、排队长度和实际车道占用，才能讨论容量或效率。",
        "",
        "## 下一步采集",
        "",
        "1. 在开通和恢复前后连续录制至少 20–30 分钟，不能让施工阶段造成两侧断点空档。",
        "2. 两个设备同步时钟，并记录机位坐标、道路方向、机位间距离和实际操作开始/结束时间。",
        "3. 每次保留同一机位、同一计数线，另存原始视频、状态标注和异常片段。",
        "4. 增加多天、多次开通/恢复，形成可重复的时间断点，而不是只依赖一天。",
        "",
        "## 下一步分析与证据链",
        "",
        "- First stage：证明车道数确实按 3/4 发生切换，并精确记录切换时间。",
        "- Reduced form：看两个方向流量、方向差分和速度是否在切换处发生变化。",
        "- Capacity/efficiency：比较单位车道流量、速度和排队长度；不能只看车辆数。",
        "- Identification：使用 5 分钟 bin 的 RDiT，加入伪断点、带宽、日期和方向稳健性；跨机位数据先做时间滞后校验，不直接相加。",
    ]
    (OUT / "stage_results_and_next_collection.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    frame = load_comparison()
    plot_indicators(frame)
    plot_coverage_timeline()
    write_notes(frame)
    print(f"output={OUT}")


if __name__ == "__main__":
    main()
