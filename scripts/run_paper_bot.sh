#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

mkdir -p data
PID_FILE="data/paper_bot.pid"
LOG_FILE="data/paper_bot.log"

if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "Paper bot is already running with PID $(cat "$PID_FILE")"
  exit 1
fi

: > "$LOG_FILE"
nohup "$ROOT_DIR/.venv/bin/python" -u "$ROOT_DIR/run_probe.py" --run-bot --run-forever >> "$LOG_FILE" 2>&1 &
echo $! > "$PID_FILE"
echo "Paper bot started with PID $(cat "$PID_FILE")"
echo "Log file: $LOG_FILE"
echo "Watch it with: tail -f $LOG_FILE"
