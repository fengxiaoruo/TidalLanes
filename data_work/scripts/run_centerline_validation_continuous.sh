#!/usr/bin/env bash
# After the current in-flight crawl finishes, repeat FREE+AM+PM full manifest crawls
# until stopped manually (Ctrl+C / kill the wrapper PID).
set -uo pipefail
cd "$(dirname "$0")/.."

CONFIG="config/centerline_baidu_validation_beijing_v1.json"
PREFIX="${1:-continuous}"
SLEEP_SEC="${SLEEP_SEC:-0.25}"
PAUSE_BETWEEN_ROUNDS_SEC="${PAUSE_BETWEEN_ROUNDS_SEC:-30}"
LOG_DIR="outputs/centerline_baidu_validation_beijing_v1/logs"
mkdir -p "$LOG_DIR"

MASTER_LOG="$LOG_DIR/${PREFIX}_master.log"

log() {
  echo "[continuous] $* $(date -Iseconds)" | tee -a "$MASTER_LOG"
}

wait_for_idle() {
  log "waiting for in-flight centerline_baidu_direction processes..."
  while pgrep -f "src.centerline_baidu_validation.query_baidu_direction" >/dev/null 2>&1; do
    sleep 30
  done
  log "no active crawl processes — ready for next round"
}

run_one_round() {
  local round="$1"
  local label="${PREFIX}_r$(printf '%03d' "${round}")_$(date +%Y%m%dT%H%M%S)"
  local round_log="$LOG_DIR/${label}.log"
  log "round ${round} START label=${label}"
  {
    echo "[round] label=${label} start $(date -Iseconds)"
    for PANEL in FREE AM PM; do
      echo "[round] panel=${PANEL} begin $(date -Iseconds)"
      if python -m src.centerline_baidu_validation.query_baidu_direction \
        --config "$CONFIG" \
        --panel "$PANEL" \
        --label "$label" \
        --sleep-sec "$SLEEP_SEC"; then
        echo "[round] panel=${PANEL} ok $(date -Iseconds)"
      else
        echo "[round] panel=${PANEL} FAILED exit=$? $(date -Iseconds)"
      fi
    done
    echo "[round] label=${label} complete $(date -Iseconds)"
  } 2>&1 | tee -a "$round_log" >> "$MASTER_LOG"
  log "round ${round} DONE label=${label}"
}

log "daemon start prefix=${PREFIX} pid=$$"
log "master log: ${MASTER_LOG}"
log "stop with: kill ${PPID:-$$}  OR  pkill -f run_centerline_validation_continuous"

wait_for_idle

ROUND=1
while true; do
  run_one_round "$ROUND"
  STOP_FILE="$LOG_DIR/${PREFIX}.stop"
  if [[ -f "$STOP_FILE" ]]; then
    log "stop file found (${STOP_FILE}) — exiting after round ${ROUND}"
    exit 0
  fi
  ROUND=$((ROUND + 1))
  log "sleep ${PAUSE_BETWEEN_ROUNDS_SEC}s before next round..."
  sleep "$PAUSE_BETWEEN_ROUNDS_SEC"
done
