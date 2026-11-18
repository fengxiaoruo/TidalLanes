#!/usr/bin/env bash
# Build baseline centerline Baidu validation manifest.
set -euo pipefail
cd "$(dirname "$0")/.."
CONFIG="${1:-config/centerline_baidu_validation_beijing_v1.example.json}"
python -m src.centerline_baidu_validation.build_crawl_manifest --config "$CONFIG" "${@:2}"
