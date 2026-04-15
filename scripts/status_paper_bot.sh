#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

/home/cypox/workspace/homecybot/.venv/bin/python <<'PY'
import json
from pathlib import Path

root = Path('/home/cypox/workspace/homecybot')
state_path = root / 'data' / 'runtime_state.json'
heartbeat_path = root / 'data' / 'heartbeat.json'
audit_path = root / 'data' / 'paper_trade_audit.jsonl'

state = json.loads(state_path.read_text()) if state_path.exists() else {}
heartbeat = json.loads(heartbeat_path.read_text()) if heartbeat_path.exists() else {}
open_pairs = state.get('open_pairs', [])
trade_journal = state.get('trade_journal', [])
opened = [item for item in trade_journal if item.get('event') == 'open']
closed = [item for item in trade_journal if item.get('event') == 'close']

allocation_per_pair = 10000.0
reference_allocation = allocation_per_pair * max(1, len(open_pairs))
gross_exposure = sum(float(item.get('gross_exposure', 0) or 0) for item in open_pairs)
realized = float(state.get('realized_pnl', 0) or 0)
unrealized = sum(float(item.get('unrealized_pnl', 0) or 0) for item in open_pairs)
net_pnl = realized + unrealized
marked_value = gross_exposure + net_pnl

print('=== Paper Bot Status ===')
print(f"heartbeat: {heartbeat.get('status', 'unknown')} at {heartbeat.get('timestamp', 'n/a')}")
print(f"cycles: {state.get('cycle_count', 0)}")
print()
print('=== Today Summary ===')
print(f"opened trades: {len(opened)}")
print(f"closed trades: {len(closed)}")
print(f"gross exposure: {gross_exposure:.2f} EUR")
print(f"realized pnl: {realized:.2f} EUR")
print(f"unrealized pnl: {unrealized:.2f} EUR")
print(f"net pnl: {net_pnl:.2f} EUR")
print(f"marked value: {marked_value:.2f} EUR")
print(f"reference allocation: {reference_allocation:.2f} EUR")
print()
print('=== Open Pairs ===')
if not open_pairs:
    print('No open pairs')
else:
    for item in open_pairs:
        print(
            f"- {item.get('pair')}: "
            f"{item.get('symbol_a_action')} {item.get('qty_a')} @ {item.get('latest_price_a')} | "
            f"{item.get('symbol_b_action')} {item.get('qty_b')} @ {item.get('latest_price_b')} | "
            f"uPnL={float(item.get('unrealized_pnl', 0) or 0):.2f} EUR"
        )

print()
print('=== Recent Audit ===')
if audit_path.exists():
    lines = audit_path.read_text().strip().splitlines()[-10:]
    for line in lines:
        print(line)
else:
    print('No audit file yet')
PY
