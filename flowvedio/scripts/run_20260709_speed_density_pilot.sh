#!/usr/bin/env bash
set -euo pipefail

cd /Users/fxr/Dropbox/Projects/TidalLanes
source flowvedio/.venv-video/bin/activate

ROOT="flowvedio/batch_outputs/20260709_speed_density_pilot"
VIDEO="flowvedio/20260709彩田路/出城方向1621开始.MOV"

python flowvedio/measure_speed_density_yolo.py "$VIDEO" \
  --outdir "$ROOT/pre_000_165" \
  --phase pre \
  --start-s 0 \
  --end-s 165 \
  --device mps \
  --qa-every-s 30

python flowvedio/measure_speed_density_yolo.py "$VIDEO" \
  --outdir "$ROOT/post_immediate_210_510" \
  --phase post \
  --start-s 210 \
  --end-s 510 \
  --device mps \
  --qa-every-s 60

echo "Pilot extraction complete: $ROOT"
