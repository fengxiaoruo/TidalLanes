#!/usr/bin/env bash
# Stop continuous crawl after a specific round label completes (e.g. r002).
set -euo pipefail
cd "$(dirname "$0")/.."

PREFIX="${1:-continuous_20260525}"
ROUND_TAG="${2:-r002}"
LOG_DIR="outputs/centerline_baidu_validation_beijing_v1/logs"
STOP_FILE="$LOG_DIR/${PREFIX}.stop"
MASTER_LOG="$LOG_DIR/${PREFIX}_master.log"

log() {
  echo "[stop_watcher] $* $(date -Iseconds)" | tee -a "$MASTER_LOG"
}

log "watching for round ${ROUND_TAG} to finish, then stopping prefix=${PREFIX}"

while true; do
  if pgrep -f "src.centerline_baidu_validation.query_baidu_direction" >/dev/null 2>&1; then
    if pgrep -f "query_baidu_direction.*${PREFIX}_${ROUND_TAG}" >/dev/null 2>&1; then
      sleep 30
      continue
    fi
    # Another round may have started — still wait if r002 PM session incomplete
  fi

  # PM is last panel in a round; any session dir matching r002 with 11890 lines means round done
  done_pm=0
  for d in outputs/centerline_baidu_validation_beijing_v1/api/crawl_sessions/PM_*${PREFIX}_${ROUND_TAG}_*/responses.jsonl; do
    [[ -f "$d" ]] || continue
    n=$(wc -l < "$d" | tr -d ' ')
    if [[ "$n" -ge 11890 ]]; then
      done_pm=1
      log "found complete PM session: $(basename "$(dirname "$d")") (${n} lines)"
      break
    fi
  done

  if [[ "$done_pm" -eq 1 ]]; then
    # Wait for any in-flight query for this round to exit
    while pgrep -f "query_baidu_direction.*${PREFIX}_${ROUND_TAG}" >/dev/null 2>&1; do
      sleep 10
    done
    touch "$STOP_FILE"
    log "created stop file: ${STOP_FILE}"
    pkill -f "run_centerline_validation_continuous.sh ${PREFIX}" || true
    sleep 2
    if pgrep -f "run_centerline_validation_continuous.sh ${PREFIX}" >/dev/null 2>&1; then
      log "warning: continuous wrapper still running"
    else
      log "continuous wrapper stopped"
    fi
    if pgrep -f "query_baidu_direction.*${PREFIX}" >/dev/null 2>&1; then
      log "note: a query_baidu_direction process is still running (should finish current panel only)"
    else
      log "no further crawl processes for ${PREFIX}"
    fi
    log "stop complete — will not start next round"
    exit 0
  fi

  sleep 45
done
