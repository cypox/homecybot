#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PID_FILE="data/paper_bot.pid"

if [[ ! -f "$PID_FILE" ]]; then
  echo "No PID file found"
  exit 1
fi

PID="$(cat "$PID_FILE")"
if kill -0 "$PID" 2>/dev/null; then
  kill "$PID"
  echo "Stopped paper bot with PID $PID"
else
  echo "Process $PID is not running"
fi

rm -f "$PID_FILE"
