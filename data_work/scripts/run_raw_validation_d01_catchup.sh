#!/usr/bin/env bash
# Catch up d01 panels after a failed/crashed first round (no wall-clock wait).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_WORK="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${DATA_WORK}"

ENV_FILE="${RAW_WEEK_ENV:-${SCRIPT_DIR}/raw_validation_week.env}"
if [[ -f "${ENV_FILE}" ]]; then
  # shellcheck source=/dev/null
  set -a && source "${ENV_FILE}" && set +a
fi

PYTHON="${PYTHON:-python3.11}"
CONFIG="${RAW_CONFIG:-config/raw_baidu_validation_beijing_v1.json}"
SLEEP_SEC="${CRAWL_SLEEP_SEC:-0.35}"
LABEL="${CATCHUP_LABEL:-formal_week_20260528_d01}"

LOG_DIR="${DATA_WORK}/outputs/raw_baidu_validation_beijing_v1/logs"
mkdir -p "${LOG_DIR}"
MASTER_LOG="${LOG_DIR}/${LABEL}_catchup.log"

log() {
  echo "[raw_catchup] $* $(TZ=Asia/Shanghai date -Iseconds)" | tee -a "${MASTER_LOG}"
}

log "start pid=$$ label=${LABEL} config=${CONFIG} sleep=${SLEEP_SEC}s"

for panel in FREE AM PM; do
  log "panel=${panel} begin --steps-info --allow-off-window"
  {
    echo "===== ${LABEL} ${panel} begin $(TZ=Asia/Shanghai date -Iseconds) ====="
    "${PYTHON}" -m src.raw_baidu_validation.query_baidu_direction \
      --config "${CONFIG}" \
      --panel "${panel}" \
      --label "${LABEL}" \
      --sleep-sec "${SLEEP_SEC}" \
      --steps-info \
      --allow-off-window
    echo "===== ${LABEL} ${panel} end $(TZ=Asia/Shanghai date -Iseconds) ====="
  } 2>&1 | tee -a "${LOG_DIR}/${LABEL}_${panel}.log" >>"${MASTER_LOG}"
  log "panel=${panel} complete"
done

log "catchup complete label=${LABEL}"

# If handoff watcher not already running, remind operator (handoff script is separate).
log "next: formal week d02+ should be scheduled via run_raw_validation_handoff_to_formal_week.sh"
