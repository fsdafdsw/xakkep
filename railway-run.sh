#!/usr/bin/env bash
set -euo pipefail

cd /app/polymarket_edge_bot_realprice

POLL_SECONDS="${POLL_SECONDS:-60}"
VOLUME_ROOT="${RAILWAY_VOLUME_MOUNT_PATH:-/data}"
export PAPER_TRADING_ENABLED="${PAPER_TRADING_ENABLED:-true}"
export PAPER_STATE_DIR="${PAPER_STATE_DIR:-${VOLUME_ROOT}/paper_state}"
export REPORTS_DIR="${REPORTS_DIR:-${VOLUME_ROOT}/reports}"

while true; do
  python main.py
  sleep "${POLL_SECONDS}"
done
