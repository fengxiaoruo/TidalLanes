#!/usr/bin/env bash
# 从 data_work 根目录跑 FREE → AM → PM 三档矩阵（默认 national top-50 配置）。
# 使用方式:
#   本地 `src/national_tidal_commuting/ak_local.py` 已写好 AK 时，直接运行
#   ./scripts/run_baidu_travel_panels.sh
# 或显式传入 / 覆盖环境变量:
#   ./scripts/run_baidu_travel_panels.sh 你的AK
#
# 可选环境变量:
#   CONFIG   默认 config/national_tidal_commuting_top50cities_2021_v1.json
#   PANELS   默认 "FREE AM PM"（可改为只跑子集）
#   AK_SOURCE 默认 auto（优先 ak_local.py，再回退环境变量；也可设为 env 或 local）
#   默认不校验本地时间；STRICT=1 会启用 --strict-clock-window（见 query 脚本文案）
#   STRICT_LIVE=1 会启用更严格的真实时段窗；此时建议把 PANELS 设为单个 AM / PM / NIGHT
#   若设了 STRICT=1 仍要在窗外跑，可再设 ALLOW_OFF=1 并追加 --allow-off-window 覆盖

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_WORK_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$DATA_WORK_ROOT"

if [[ -n "${1:-}" ]]; then
  export BAIDU_MAP_AUTH_TOKEN="$1"
fi

CONFIG="${CONFIG:-config/national_tidal_commuting_top50cities_2021_v1.json}"
PANELS="${PANELS:-FREE AM PM}"
AK_SOURCE="${AK_SOURCE:-auto}"
EXTRA=()
if [[ "${STRICT:-0}" == "1" ]]; then
  EXTRA+=(--strict-clock-window)
fi
if [[ "${ALLOW_OFF:-0}" == "1" ]]; then
  EXTRA+=(--allow-off-window)
fi
if [[ "${STRICT_LIVE:-0}" == "1" ]]; then
  EXTRA+=(--strict-live-window)
fi

for w in $PANELS; do
  echo "==== query_window=$w ====" >&2
  # With `set -u`, an empty EXTRA array throws "unbound" on some Bash (e.g. macOS 3.2).
  if ((${#EXTRA[@]})); then
    python src/national_tidal_commuting/query_baidu_routematrix.py --config "$CONFIG" --query-window "$w" --ak-source "$AK_SOURCE" "${EXTRA[@]}"
  else
    python src/national_tidal_commuting/query_baidu_routematrix.py --config "$CONFIG" --query-window "$w" --ak-source "$AK_SOURCE"
  fi
done
echo "完成。每档独立 CSV/JSONL 在 outputs/{version_id}/api/ 下。" >&2
