#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

LOG_FILE="data/paper_bot.log"
HEARTBEAT_FILE="data/heartbeat.json"
AUDIT_FILE="data/paper_trade_audit.jsonl"

echo "=== Heartbeat ==="
if [[ -f "$HEARTBEAT_FILE" ]]; then
  cat "$HEARTBEAT_FILE"
else
  echo "No heartbeat file yet"
fi

echo
echo "=== Recent audit events ==="
if [[ -f "$AUDIT_FILE" ]]; then
  tail -n 20 "$AUDIT_FILE"
else
  echo "No audit file yet"
fi

echo
echo "=== Live log tail ==="
touch "$LOG_FILE"
tail -f "$LOG_FILE"
