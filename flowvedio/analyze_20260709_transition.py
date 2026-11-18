#!/usr/bin/env python3
"""Summarize the already-counted 2026-07-09 implementation transition."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR / "batch_outputs" / "20260709_identification_first_hour_overnight_mps"
OUT = SCRIPT_DIR / "batch_outputs" / "20260709_transition_v1"
SUMMARY = ROOT / "summary_plan.csv"


def load_summary() -> pd.DataFrame:
    frame = pd.read_csv(SUMMARY)
    frame["counts"] = frame["counts_by_line"].map(json.loads)
    frame["north_to_south"] = frame["counts"].map(lambda x: float(x.get("north_to_south", 0)))
    frame["south_to_north"] = frame["counts"].map(lambda x: float(x.get("south_to_north", 0)))
    return frame


def summarize(frame: pd.DataFrame, phase: str, normal_lanes: dict[str, int], tidal_lanes: dict[str, int]) -> dict[str, object]:
    duration_min = frame["duration_s"].sum() / 60
    ns = frame["north_to_south"].sum()
    sn = frame["south_to_north"].sum()
    total = ns + sn
    ns_rate = ns / duration_min
    sn_rate = sn / duration_min
    total_rate = total / duration_min
    balance = (sn - ns) / total if total else float("nan")
    lane_map = normal_lanes if phase == "pre" else tidal_lanes
    return {
        "phase": phase,
        "source_video": "; ".join(sorted(frame["source_video"].unique())),
        "duration_min": duration_min,
        "north_to_south_count": ns,
        "south_to_north_count": sn,
        "north_to_south_rate_per_min": ns_rate,
        "south_to_north_rate_per_min": sn_rate,
        "total_rate_per_min": total_rate,
        "direction_balance": balance,
        "north_to_south_lanes": lane_map["north_to_south"],
        "south_to_north_lanes": lane_map["south_to_north"],
        "north_to_south_rate_per_lane_min": ns_rate / lane_map["north_to_south"],
        "south_to_north_rate_per_lane_min": sn_rate / lane_map["south_to_north"],
    }


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    frame = load_summary()
    outbound = frame[frame["source_video"].str.endswith("出城方向1621开始.MOV")].copy()
    pre = outbound[outbound["label"].eq("outbound_pre")]
    immediate = outbound[outbound["segment_id"].eq("002_000330_000830_outbound_post")]
    post_all = outbound[outbound["label"].eq("outbound_post")]
    normal_lanes = {"north_to_south": 4, "south_to_north": 3}
    tidal_lanes = {"north_to_south": 3, "south_to_north": 4}
    rows = [
        summarize(pre, "pre", normal_lanes, tidal_lanes),
        summarize(immediate, "post_immediate", normal_lanes, tidal_lanes),
        summarize(post_all, "post_all", normal_lanes, tidal_lanes),
    ]
    result = pd.DataFrame(rows)
    result.to_csv(OUT / "transition_phase_summary.csv", index=False)

    pre_row = result[result["phase"].eq("pre")].iloc[0]
    immediate_row = result[result["phase"].eq("post_immediate")].iloc[0]
    post_all_row = result[result["phase"].eq("post_all")].iloc[0]
    lines = [
        "# 2026-07-09 彩田路施工切换的阶段性分析",
        "",
        "## 适用范围",
        "",
        "- 使用已经完成的 YOLO 计数结果，不重新识别视频。",
        "- 核心视频是 `出城方向1621开始.MOV`：前 2:45 为未布设，2:45–3:30 为施工/切换，3:30 后为稳定开通。",
        "- `进城方向1623开始.MOV` 和两个 17:27 视频主要是 post，不能单独提供开通前趋势。",
        "",
        "## 出城 1621 视频的即时前后比较",
        "",
        f"- pre（0:00–2:45）：总流量 {pre_row['total_rate_per_min']:.1f} 辆/分钟，北向南 {pre_row['north_to_south_rate_per_min']:.1f}，南向北 {pre_row['south_to_north_rate_per_min']:.1f}，B={pre_row['direction_balance']:.3f}。",
        f"- post immediate（3:30–8:30）：总流量 {immediate_row['total_rate_per_min']:.1f} 辆/分钟，北向南 {immediate_row['north_to_south_rate_per_min']:.1f}，南向北 {immediate_row['south_to_north_rate_per_min']:.1f}，B={immediate_row['direction_balance']:.3f}。",
        f"- 即时 B 变化为 {immediate_row['direction_balance'] - pre_row['direction_balance']:+.3f}；总流量变化为 {immediate_row['total_rate_per_min'] - pre_row['total_rate_per_min']:+.1f} 辆/分钟。",
        f"- 按 4/3 到 3/4 的车道数调整，单位车道流量：北向南 {pre_row['north_to_south_rate_per_lane_min']:.1f} → {immediate_row['north_to_south_rate_per_lane_min']:.1f}，南向北 {pre_row['south_to_north_rate_per_lane_min']:.1f} → {immediate_row['south_to_north_rate_per_lane_min']:.1f} 辆/分钟/车道。",
        "",
        "## 较长 post 时段",
        "",
        f"- 3:30–58:30 的 post 全段平均总流量为 {post_all_row['total_rate_per_min']:.1f} 辆/分钟，B={post_all_row['direction_balance']:.3f}。",
        "- 这段 post 包含较长的时间趋势，不能作为开通前后的局部断点比较；它适合展示开通后交通状态如何继续变化。",
        "",
        "## 判断",
        "",
        "- 这个视频非常适合做第一阶段的“实施过程与测量验证”：它记录了实际施工窗口，并且可以把施工阶段从稳定计数样本中剔除。",
        "- 它不适合单独做可信的 RDD：开通前只有 2:45，无法检查 pre trend；施工窗口还造成断点两侧缺口；进城方向没有对应的 pre。",
        "- 即时前后 B 几乎没有变化，不能把这个视频解读成强的方向重分配效果；更稳妥的用法是证明政策时点、定义 transition exclusion，并作为 0710 多时段结果的过程验证样本。",
        "- 继续使用时，建议把 `pre`、`transition`、`post_immediate` 和 `post_all` 分开，不要把施工段填成 0，也不要把 post 全段和 pre 直接做一个因果比较。",
    ]
    (OUT / "transition_analysis_notes.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(result.to_string(index=False))
    print(f"output={OUT}")


if __name__ == "__main__":
    main()
