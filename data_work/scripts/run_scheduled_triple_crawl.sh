#!/usr/bin/env bash
# 三轮 national 爬取：仅按「北京时间」开始时刻启动，不设结束时刻；不启用 --strict-live-window。
# 每轮仍写入 api/crawl_sessions/<run_id>/（含 manifest 起止时间、逐条 query_time_local）。
#
# 用法：
#   1) cp scripts/scheduled_triple_crawl.env.example scripts/scheduled_triple_crawl.env
#   2) 编辑 .env 填 REPO_ROOT、三轮 START_*_HHMM、CONFIG
#   3) cd data_work && ./scripts/run_scheduled_triple_crawl.sh
#
# 本脚本会后台启动 3 个子进程（FREE / AM / PM 各一轮），主进程打印 PID 后退出。
# 请确保已配置 ak_local.py 或 BAIDU_MAP_AUTH_TOKEN，且机器在对应时段保持唤醒。

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_WORK="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${DATA_WORK}"

ENV_FILE="${SCHEDULE_ENV:-${SCRIPT_DIR}/scheduled_triple_crawl.env}"
if [[ -f "${ENV_FILE}" ]]; then
  # shellcheck source=/dev/null
  set -a && source "${ENV_FILE}" && set +a
fi

REPO_ROOT="${REPO_ROOT:-$(cd "${DATA_WORK}/.." && pwd)}"
CONFIG="${BAIDU_NATIONAL_CONFIG:-config/national_tidal_commuting_baidu2025_top100_congestion_v1.json}"
START_FREE="${START_FREE_HHMM:?Set START_FREE_HHMM in env}"
START_AM="${START_AM_HHMM:?Set START_AM_HHMM in env}"
START_PM="${START_PM_HHMM:?Set START_PM_HHMM in env}"
SLEEP="${CRAWL_SLEEP_SEC:-0.2}"
LOG_SUB="${CRAWL_LOG_DIR:-logs/scheduled_triple_crawl}"
LOG_DIR="${DATA_WORK}/${LOG_SUB}"
mkdir -p "${LOG_DIR}"

DATE_TAG="$(TZ=Asia/Shanghai date +%Y%m%d)"
BOOT_LOG="${LOG_DIR}/spawn_${DATE_TAG}.log"

if [[ "${CONFIG}" == /* ]]; then
  CFG_PATH="${CONFIG}"
else
  CFG_PATH="${DATA_WORK}/${CONFIG}"
fi
if [[ ! -f "${CFG_PATH}" ]]; then
  echo "Config not found: ${CFG_PATH}" >&2
  exit 1
fi

# 相对 data_work 的 config 路径传给 python（与现有一致）
CONFIG_ARG="${CONFIG}"

_ak_hint() {
  if [[ -f "${DATA_WORK}/src/national_tidal_commuting/ak_local.py" ]]; then
    echo "[schedule] 将使用 ak_local.py（若已填写 BAIDU_MAP_AUTH_TOKEN）" >>"${BOOT_LOG}"
  elif [[ -n "${BAIDU_MAP_AUTH_TOKEN:-}" ]]; then
    echo "[schedule] 将使用环境变量 BAIDU_MAP_AUTH_TOKEN" >>"${BOOT_LOG}"
  else
    echo "[schedule] 警告: 未检测到 ak_local.py 或 BAIDU_MAP_AUTH_TOKEN；请补 AK 后再到点执行。" >>"${BOOT_LOG}"
  fi
}

{
  echo "======== $(TZ=Asia/Shanghai date -Iseconds) ========"
  echo "REPO_ROOT=${REPO_ROOT} DATA_WORK=${DATA_WORK}"
  echo "CONFIG=${CONFIG_ARG}"
  echo "START_FREE=${START_FREE} START_AM=${START_AM} START_PM=${START_PM}"
  _ak_hint
} | tee -a "${BOOT_LOG}"

_spawn() {
  local hhmm="$1"
  local panel="$2"
  local label="$3"
  local logf="${LOG_DIR}/crawl_${panel}_${DATE_TAG}.log"
  (
    cd "${DATA_WORK}"
    echo "[${panel}] child start wait_until=${hhmm} $(TZ=Asia/Shanghai date -Iseconds)" >>"${logf}"
    exec python3.11 scripts/wait_until_shanghai_hhmm_then_exec.py "${hhmm}" -- \
      python3.11 -m src.national_tidal_commuting.run_full_crawl \
      --config "${CONFIG_ARG}" \
      --panels "${panel}" \
      --label "${label}" \
      --sleep-sec "${SLEEP}"
  ) >>"${logf}" 2>&1 &
  echo $!
}

echo "Spawning FREE at ${START_FREE} ..." | tee -a "${BOOT_LOG}"
PID_FREE=$(_spawn "${START_FREE}" FREE "sched_free_${DATE_TAG}")
echo "  PID FREE=${PID_FREE}" | tee -a "${BOOT_LOG}"

echo "Spawning AM at ${START_AM} ..." | tee -a "${BOOT_LOG}"
PID_AM=$(_spawn "${START_AM}" AM "sched_am_${DATE_TAG}")
echo "  PID AM=${PID_AM}" | tee -a "${BOOT_LOG}"

echo "Spawning PM at ${START_PM} ..." | tee -a "${BOOT_LOG}"
PID_PM=$(_spawn "${START_PM}" PM "sched_pm_${DATE_TAG}")
echo "  PID PM=${PID_PM}" | tee -a "${BOOT_LOG}"

echo "Done. Logs under ${LOG_DIR}. 实际请求时段见各轮 api/crawl_sessions/*/session_matrix_flat.csv 中 query_time_local 的 min/max，以及 run_manifest.json 的 crawl_session_start_local / finished_at_local。" | tee -a "${BOOT_LOG}"
