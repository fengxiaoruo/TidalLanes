#!/usr/bin/env bash
set -euo pipefail

cd /Users/fxr/Dropbox/Projects/TidalLanes
source flowvedio/.venv-video/bin/activate

ROOT="flowvedio/batch_outputs/20260709_speed_density_pilot"
VIDEO="flowvedio/20260709彩田路/出城方向1621开始.MOV"

python flowvedio/measure_speed_density_yolo.py "$VIDEO" \
  --outdir "$ROOT/post_stable_2010_2910" \
  --phase post \
  --start-s 2010 \
  --end-s 2910 \
  --device mps \
  --qa-every-s 120

python flowvedio/analyze_20260709_speed_density_three_windows.py

echo "Stable-window extraction and three-window comparison complete: $ROOT"
