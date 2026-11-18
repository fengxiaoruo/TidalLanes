#!/usr/bin/env bash
# Full one-round crawl: FREE + AM + PM, entire baseline manifest (11,890 directed / panel).
set -euo pipefail
cd "$(dirname "$0")/.."
CONFIG="config/centerline_baidu_validation_beijing_v1.json"
LABEL="${1:-full_round}"
LOG_DIR="outputs/centerline_baidu_validation_beijing_v1/logs"
mkdir -p "$LOG_DIR"
STAMP=$(date +%Y%m%dT%H%M%S)
LOG="$LOG_DIR/${LABEL}_${STAMP}.log"

echo "[full_round] start $(date -Iseconds) label=$LABEL" | tee -a "$LOG"
for PANEL in FREE AM PM; do
  echo "[full_round] panel=$PANEL begin $(date -Iseconds)" | tee -a "$LOG"
  python -m src.centerline_baidu_validation.query_baidu_direction \
    --config "$CONFIG" \
    --panel "$PANEL" \
    --label "${LABEL}" \
    --sleep-sec 0.25 \
    2>&1 | tee -a "$LOG"
  echo "[full_round] panel=$PANEL done $(date -Iseconds)" | tee -a "$LOG"
done
echo "[full_round] all panels complete $(date -Iseconds)" | tee -a "$LOG"
