#!/usr/bin/env bash
#
# Incremental cold backup of the irreplaceable parts of TidalLanes.
#
#   ./backup_to_drive.sh              plan only, changes nothing
#   ./backup_to_drive.sh --run        perform the sync
#   ./backup_to_drive.sh --run --set video     sync one set only
#
# Full rationale and the recovery procedure: data_work/docs/BACKUP_WORKFLOW.md

set -euo pipefail

DRIVE="${TIDAL_BACKUP_DRIVE:-冯筱箬}"
DEST_ROOT="/Volumes/${DRIVE}/TidalLanes_archive"
SRC_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# Backup sets: name|source path relative to repo root|destination subdirectory
SETS=(
  "rawdata|data_work/raw_data|raw_data"
  "video|flowvedio|flowvedio"
  "fieldvisit|fieldvisit|fieldvisit"
  "presentation|Documents/PreSlides|PreSlides"
)

# Reproducible or re-downloadable content never reaches the drive. Everything
# here is either regenerable from code plus inputs, or a build artefact.
EXCLUDES=(
  --exclude=".DS_Store"      --exclude="._*"
  --exclude=".Spotlight-V100" --exclude=".Trashes" --exclude=".fseventsd"
  --exclude="__pycache__/"   --exclude="*.pyc"
  --exclude=".venv*/"        --exclude="node_modules/"
  # split_outputs holds re-splittable clips of the source video and is excluded.
  # batch_outputs and traffic_count_outputs are regenerable in principle but
  # cost substantial GPU time to rebuild, so they are retained on the drive.
  --exclude="split_outputs/"
  --exclude=".mplcache/"     --exclude=".cache/"
  --exclude="*.aux" --exclude="*.log" --exclude="*.out" --exclude="*.toc"
  --exclude="*.nav" --exclude="*.snm" --exclude="*.fls" --exclude="*.fdb_latexmk"
  --exclude="*.synctex.gz"
)

DO_RUN=false
ONLY_SET=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --run) DO_RUN=true; shift ;;
    --set) ONLY_SET="${2:?--set needs a name}"; shift 2 ;;
    -h|--help) sed -n '2,10p' "${BASH_SOURCE[0]}"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

if [[ ! -d "/Volumes/${DRIVE}" ]]; then
  echo "Drive '${DRIVE}' is not mounted." >&2
  echo "Connect it, or set TIDAL_BACKUP_DRIVE to the correct volume name." >&2
  exit 1
fi

# exFAT stores timestamps at 2-second granularity and has no POSIX permissions.
# Without --modify-window every file looks modified and each run recopies
# everything; --no-perms --no-owner --no-group stop rsync from fighting the
# filesystem over metadata it cannot represent.
# Option set kept compatible with openrsync (macOS 15 default, protocol 29);
# newer flags such as --info=stats2 are unavailable there.
RSYNC_OPTS=(-rlt --modify-window=2 --no-perms --no-owner --no-group
            --human-readable --partial --stats)
FS_TYPE="$(diskutil info "/Volumes/${DRIVE}" 2>/dev/null \
           | awk -F': *' '/File System Personality/ {print $2}' | xargs || true)"
if [[ "${FS_TYPE}" != *"APFS"* && "${FS_TYPE}" != *"HFS"* && -n "${FS_TYPE}" ]]; then
  echo "note: ${DRIVE} is ${FS_TYPE}. Small files are padded to the cluster size,"
  echo "      so directories of many tiny files inflate substantially on this volume."
  echo
fi

$DO_RUN || RSYNC_OPTS+=(--dry-run)
$DO_RUN || echo "PLAN ONLY. Re-run with --run to copy. Nothing is deleted in either mode."
echo "source:      ${SRC_ROOT}"
echo "destination: ${DEST_ROOT}"
echo

mkdir -p "${DEST_ROOT}"
started="$(date '+%Y-%m-%d %H:%M:%S')"
synced=()

for entry in "${SETS[@]}"; do
  IFS='|' read -r name rel dest <<< "${entry}"
  [[ -n "${ONLY_SET}" && "${ONLY_SET}" != "${name}" ]] && continue

  src="${SRC_ROOT}/${rel}"
  if [[ ! -d "${src}" ]]; then
    echo "skip ${name}: ${rel} not found"
    continue
  fi

  echo "=== ${name}: ${rel} -> ${dest} ==="
  # Trailing slash on the source copies the contents, not the directory itself.
  rsync "${RSYNC_OPTS[@]}" "${EXCLUDES[@]}" "${src}/" "${DEST_ROOT}/${dest}/"
  echo
  synced+=("${name}")
done

if $DO_RUN && [[ ${#synced[@]} -gt 0 ]]; then
  {
    echo "## ${started}"
    echo
    echo "- sets: ${synced[*]}"
    echo "- source: ${SRC_ROOT}"
    echo "- host: $(hostname -s)"
    echo "- git HEAD: $(git -C "${SRC_ROOT}" rev-parse --short HEAD 2>/dev/null || echo 'n/a')"
    echo
  } >> "${DEST_ROOT}/MANIFEST.md"
  echo "Recorded in ${DEST_ROOT}/MANIFEST.md"
fi
