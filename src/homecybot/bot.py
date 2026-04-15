from __future__ import annotations

import json
from datetime import datetime, time as dt_time
from pathlib import Path
import tempfile
import time
from typing import Any, Callable
from zoneinfo import ZoneInfo

from .config import IBConfig
from .ib_client import IBGatewayProbe
from .logger import BotLogger


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _extract_numeric(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip().split()[0]
    try:
        return float(text)
    except ValueError:
        return None


def _parse_hhmm(value: str) -> dt_time:
    hour_text, minute_text = str(value).split(":", maxsplit=1)
    return dt_time(hour=int(hour_text), minute=int(minute_text))


def is_trading_window_open(
    current_dt: datetime | None = None,
    start_time: str = "09:35",
    end_time: str = "15:30",
) -> bool:
    current_dt = current_dt or datetime.now()
    if current_dt.weekday() >= 5:
        return False
    start_clock = _parse_hhmm(start_time)
    end_clock = _parse_hhmm(end_time)
    return start_clock <= current_dt.time() <= end_clock


def should_halt_trading(
    state: dict[str, Any],
    max_daily_loss_eur: float = 250.0,
    max_open_pairs: int = 2,
) -> dict[str, Any]:
    daily_realized = _safe_float(state.get("daily_realized_pnl", state.get("realized_pnl", 0.0)))
    open_pairs = list(state.get("open_pairs", []))

    if max_daily_loss_eur > 0 and daily_realized <= -abs(max_daily_loss_eur):
        return {"halt": True, "reason": "max_daily_loss"}
    if max_open_pairs > 0 and len(open_pairs) > max_open_pairs:
        return {"halt": True, "reason": "max_open_pairs"}
    return {"halt": False, "reason": "ok"}


def select_opportunities(pair_candidates: list[dict[str, Any]], min_score: float = 70.0) -> list[dict[str, Any]]:
    opportunities = [
        pair
        for pair in pair_candidates
        if pair.get("signal") != "flat" and float(pair.get("score", 0.0)) >= min_score
    ]
    opportunities.sort(
        key=lambda item: (abs(float(item.get("zscore", 0.0))), float(item.get("score", 0.0))),
        reverse=True,
    )
    return opportunities


def estimate_order_cost(
    quantity: float,
    bid: float | None = None,
    ask: float | None = None,
    price: float | None = None,
    recent_commission_per_share: float | None = None,
) -> float:
    commission = abs(quantity) * max(_safe_float(recent_commission_per_share), 0.0)
    spread_cost = 0.0
    if bid is not None and ask is not None and ask >= bid:
        spread_cost = abs(quantity) * ((ask - bid) / 2.0)
    elif price is not None:
        spread_cost = 0.0
    return round(commission + spread_cost, 6)


def create_trade_plan(
    pair_candidate: dict[str, Any],
    allocation_eur: float = 10000.0,
) -> dict[str, Any]:
    price_a = max(_safe_float(pair_candidate.get("latest_price_a"), 1.0), 1.0)
    price_b = max(_safe_float(pair_candidate.get("latest_price_b"), 1.0), 1.0)
    hedge_ratio = max(abs(_safe_float(pair_candidate.get("hedge_ratio"), 1.0)), 0.1)
    signal = str(pair_candidate.get("signal", "flat"))
    zscore = _safe_float(pair_candidate.get("zscore"), 0.0)

    if signal == "long_a_short_b" or (signal == "watch" and zscore < 0):
        action_a, action_b = "BUY", "SELL"
    else:
        action_a, action_b = "SELL", "BUY"

    unit_cost = price_a + (hedge_ratio * price_b)
    base_units = max(1, int(allocation_eur / max(unit_cost, 1.0)))
    qty_a = max(1, base_units)
    qty_b = max(1, int(round(base_units * hedge_ratio)))

    gross_exposure = qty_a * price_a + qty_b * price_b
    while gross_exposure > allocation_eur and (qty_a > 1 or qty_b > 1):
        if qty_a > 1 and qty_a * price_a >= qty_b * price_b:
            qty_a -= 1
        elif qty_b > 1:
            qty_b -= 1
        gross_exposure = qty_a * price_a + qty_b * price_b

    recent_commission = _safe_float(pair_candidate.get("recent_commission_per_share"), 0.0)
    estimated_fees = estimate_order_cost(
        qty_a,
        bid=pair_candidate.get("bid_a"),
        ask=pair_candidate.get("ask_a"),
        price=price_a,
        recent_commission_per_share=recent_commission,
    ) + estimate_order_cost(
        qty_b,
        bid=pair_candidate.get("bid_b"),
        ask=pair_candidate.get("ask_b"),
        price=price_b,
        recent_commission_per_share=recent_commission,
    )

    return {
        "pair": f"{pair_candidate['symbol_a']}/{pair_candidate['symbol_b']}",
        "symbol_a": pair_candidate["symbol_a"],
        "symbol_b": pair_candidate["symbol_b"],
        "signal": signal,
        "score": _safe_float(pair_candidate.get("score")),
        "zscore": zscore,
        "symbol_a_action": action_a,
        "symbol_b_action": action_b,
        "qty_a": qty_a,
        "qty_b": qty_b,
        "price_a": round(price_a, 6),
        "price_b": round(price_b, 6),
        "estimated_fees": round(estimated_fees, 2),
        "fee_source": "ib_live",
        "gross_exposure": round(gross_exposure, 2),
        "net_exposure": round(gross_exposure + estimated_fees, 2),
        "allocation_eur": round(allocation_eur, 2),
        "recent_commission_per_share": round(recent_commission, 6),
    }


def assess_exit(
    open_pair: dict[str, Any],
    current_candidate: dict[str, Any],
    exit_zscore: float = 0.5,
    stop_zscore: float = 3.0,
) -> dict[str, Any]:
    current_z = abs(_safe_float(current_candidate.get("zscore"), 0.0))
    if current_z <= exit_zscore:
        return {"action": "close", "reason": "mean_reversion", "zscore": current_z}
    if current_z >= stop_zscore:
        return {"action": "close", "reason": "stop_loss", "zscore": current_z}
    return {"action": "hold", "reason": "signal_active", "zscore": current_z}


def update_open_pair_mark_to_market(
    open_pair: dict[str, Any],
    current_candidate: dict[str, Any],
) -> dict[str, Any]:
    symbol_a_action = str(open_pair.get("symbol_a_action", "BUY"))
    symbol_b_action = str(open_pair.get("symbol_b_action", "SELL"))
    qty_a = _safe_float(open_pair.get("qty_a"), 0.0)
    qty_b = _safe_float(open_pair.get("qty_b"), 0.0)
    entry_price_a = _safe_float(open_pair.get("entry_price_a") or open_pair.get("price_a"), 0.0)
    entry_price_b = _safe_float(open_pair.get("entry_price_b") or open_pair.get("price_b"), 0.0)
    latest_price_a = _safe_float(current_candidate.get("latest_price_a"), entry_price_a)
    latest_price_b = _safe_float(current_candidate.get("latest_price_b"), entry_price_b)

    pnl_a = (latest_price_a - entry_price_a) * qty_a
    pnl_b = (latest_price_b - entry_price_b) * qty_b
    if symbol_a_action == "SELL":
        pnl_a *= -1
    if symbol_b_action == "SELL":
        pnl_b *= -1

    recent_commission = _safe_float(
        current_candidate.get("recent_commission_per_share") or open_pair.get("recent_commission_per_share"),
        0.0,
    )
    fees_paid = _safe_float(open_pair.get("estimated_fees"), 0.0)
    exit_fees = estimate_order_cost(
        qty_a,
        bid=current_candidate.get("bid_a") or open_pair.get("bid_a"),
        ask=current_candidate.get("ask_a") or open_pair.get("ask_a"),
        price=latest_price_a,
        recent_commission_per_share=recent_commission,
    ) + estimate_order_cost(
        qty_b,
        bid=current_candidate.get("bid_b") or open_pair.get("bid_b"),
        ask=current_candidate.get("ask_b") or open_pair.get("ask_b"),
        price=latest_price_b,
        recent_commission_per_share=recent_commission,
    )

    updated = dict(open_pair)
    updated["latest_price_a"] = round(latest_price_a, 6)
    updated["latest_price_b"] = round(latest_price_b, 6)
    updated["current_zscore"] = round(_safe_float(current_candidate.get("zscore"), 0.0), 6)
    updated["gross_unrealized_pnl"] = round(pnl_a + pnl_b, 2)
    updated["estimated_exit_fees"] = round(exit_fees, 2)
    updated["unrealized_pnl"] = round(pnl_a + pnl_b - fees_paid - exit_fees, 2)
    return updated


def summarize_trade_journal(trade_journal: list[dict[str, Any]]) -> dict[str, Any]:
    closed = [
        item
        for item in trade_journal
        if item.get("event") == "close" or item.get("realized_pnl") is not None
    ]
    if not closed:
        return {
            "closed_trades": 0,
            "win_rate": 0.0,
            "avg_realized_pnl": 0.0,
            "cumulative_realized_pnl": 0.0,
        }

    realized_values = [_safe_float(item.get("realized_pnl")) for item in closed]
    wins = sum(1 for value in realized_values if value > 0)
    return {
        "closed_trades": len(closed),
        "win_rate": round(wins / len(closed), 6),
        "avg_realized_pnl": round(sum(realized_values) / len(closed), 6),
        "cumulative_realized_pnl": round(sum(realized_values), 6),
    }


def build_cycle_summary(
    scan_summary: dict[str, Any],
    pair_candidates: list[dict[str, Any]],
    opportunities: list[dict[str, Any]],
    trade_plans: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    trade_plans = trade_plans or []
    top_pair = None
    top_signal = None
    if pair_candidates:
        first = pair_candidates[0]
        top_pair = f"{first['symbol_a']}/{first['symbol_b']}"
        top_signal = first.get("signal")

    pairs_considered = int(scan_summary.get("pairs_considered", 0))
    return {
        "pairs_considered": pairs_considered,
        "pairs_returned": int(scan_summary.get("pairs_returned", len(pair_candidates))),
        "opportunities_found": len(opportunities),
        "trade_plans": len(trade_plans),
        "allocated_notional_eur": round(
            sum(_safe_float(plan.get("gross_exposure")) for plan in trade_plans),
            2,
        ),
        "top_pair": top_pair,
        "top_signal": top_signal,
        "opportunity_ratio": round(len(opportunities) / pairs_considered, 6) if pairs_considered else 0.0,
    }


class PairTradingBot:
    def __init__(self, config: IBConfig, probe: IBGatewayProbe, logger: BotLogger | None = None):
        self.config = config
        self.probe = probe
        self.logger = logger or BotLogger(enabled=True)
        self.state_path = Path(self.config.state_path)
        self.heartbeat_path = Path(self.config.heartbeat_path)
        self.audit_log_path = Path(self.config.audit_log_path)

    def _default_state(self) -> dict[str, Any]:
        return {
            "cycle_count": 0,
            "open_pairs": [],
            "submitted_orders": 0,
            "closed_pairs": [],
            "trade_journal": [],
            "realized_pnl": 0.0,
            "daily_realized_pnl": 0.0,
            "daily_pnl_date": datetime.now(ZoneInfo(self.config.market_timezone)).date().isoformat(),
            "last_opportunities": [],
            "consecutive_errors": 0,
            "halt_new_entries": False,
            "halt_reason": None,
            "last_error": None,
        }

    def _load_state(self) -> dict[str, Any]:
        defaults = self._default_state()
        if not self.state_path.exists():
            return defaults
        try:
            loaded = json.loads(self.state_path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                defaults.update(loaded)
        except (OSError, json.JSONDecodeError):
            return defaults
        return defaults

    def _save_json_atomic(self, path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile("w", dir=path.parent, delete=False, encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
            temp_path = Path(handle.name)
        temp_path.replace(path)

    def _save_state(self, state: dict[str, Any]) -> None:
        self._save_json_atomic(self.state_path, state)

    def _write_heartbeat(self, status: str, **fields: Any) -> None:
        heartbeat = {
            "timestamp": datetime.now(ZoneInfo(self.config.market_timezone)).isoformat(),
            "status": status,
            **fields,
        }
        self._save_json_atomic(self.heartbeat_path, heartbeat)

    def _append_audit_event(self, event: str, **fields: Any) -> None:
        self.audit_log_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "timestamp": datetime.now(ZoneInfo(self.config.market_timezone)).isoformat(),
            "event": event,
            **fields,
        }
        with self.audit_log_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, default=str) + "\n")

    def _reset_daily_state_if_needed(self, state: dict[str, Any]) -> None:
        today = datetime.now(ZoneInfo(self.config.market_timezone)).date().isoformat()
        if state.get("daily_pnl_date") != today:
            state["daily_pnl_date"] = today
            state["daily_realized_pnl"] = 0.0
            if state.get("halt_reason") == "max_daily_loss":
                state["halt_new_entries"] = False
                state["halt_reason"] = None

    def _run_with_retries(self, label: str, action: Callable[[], Any]) -> Any:
        attempts = max(1, self.config.reconnect_attempts)
        last_error: Exception | None = None

        for attempt in range(1, attempts + 1):
            try:
                return action()
            except Exception as exc:
                last_error = exc
                self.logger.warning(
                    "RETRY",
                    f"{label} failed",
                    attempt=f"{attempt}/{attempts}",
                    error=str(exc),
                )
                try:
                    self.probe.disconnect()
                except Exception:
                    pass
                if attempt < attempts:
                    time.sleep(min(float(2 ** (attempt - 1)), self.config.max_retry_delay_seconds))

        raise ConnectionError(f"{label} failed after retries: {last_error}")

    def _reconcile_positions(self, state: dict[str, Any]) -> dict[str, Any]:
        tracked_symbols = {
            symbol
            for pair in state.get("open_pairs", [])
            for symbol in (pair.get("symbol_a"), pair.get("symbol_b"))
            if symbol
        }
        try:
            ib_positions = self.probe.positions()
        except Exception as exc:
            self.logger.warning("RECON", "unable to reconcile positions", error=str(exc))
            return {
                "tracked_symbols": sorted(tracked_symbols),
                "ib_symbols": [],
                "missing_in_ib": sorted(tracked_symbols),
                "untracked_in_ib": [],
                "pruned_pairs": [],
                "error": str(exc),
            }

        ib_symbols = {
            item.get("symbol")
            for item in ib_positions
            if item.get("symbol") and _safe_float(item.get("position")) != 0.0
        }
        missing_in_ib = sorted(tracked_symbols - ib_symbols)
        untracked_in_ib = sorted(ib_symbols - tracked_symbols)

        retained_pairs: list[dict[str, Any]] = []
        pruned_pairs: list[str] = []
        for pair in state.get("open_pairs", []):
            symbols = {pair.get("symbol_a"), pair.get("symbol_b")}
            symbols.discard(None)
            if symbols and symbols.isdisjoint(ib_symbols):
                pruned_pairs.append(str(pair.get("pair")))
            else:
                retained_pairs.append(pair)
        if pruned_pairs:
            state["open_pairs"] = retained_pairs
            for pair_name in pruned_pairs:
                self._append_audit_event("stale_pair_pruned", pair=pair_name)

        if missing_in_ib or untracked_in_ib or pruned_pairs:
            self.logger.warning(
                "RECON",
                "position mismatch detected",
                missing=",".join(missing_in_ib) or "none",
                untracked=",".join(untracked_in_ib) or "none",
                pruned=",".join(pruned_pairs) or "none",
            )
        return {
            "tracked_symbols": sorted(tracked_symbols),
            "ib_symbols": sorted(ib_symbols),
            "missing_in_ib": missing_in_ib,
            "untracked_in_ib": untracked_in_ib,
            "pruned_pairs": pruned_pairs,
        }

    def _allow_new_entries(self, state: dict[str, Any]) -> tuple[bool, str]:
        if not self.config.enable_new_entries:
            return False, "entries_disabled"

        halt_decision = should_halt_trading(
            state,
            max_daily_loss_eur=self.config.max_daily_loss_eur,
            max_open_pairs=self.config.max_open_pairs,
        )
        if halt_decision["halt"]:
            state["halt_new_entries"] = True
            state["halt_reason"] = halt_decision["reason"]
            return False, halt_decision["reason"]

        if state.get("halt_new_entries"):
            return False, str(state.get("halt_reason") or "halted")

        if self.config.only_trade_regular_hours:
            now_market = datetime.now(ZoneInfo(self.config.market_timezone))
            if not is_trading_window_open(
                now_market,
                start_time=self.config.trading_start_time,
                end_time=self.config.trading_stop_new_entries_time,
            ):
                return False, "outside_trading_window"

        return True, "ok"

    def _build_trade_plans(self, opportunities: list[dict[str, Any]], state: dict[str, Any]) -> list[dict[str, Any]]:
        plans: list[dict[str, Any]] = []
        open_pair_names = {item.get("pair") for item in state.get("open_pairs", [])}
        cooldown_cutoff = datetime.now(ZoneInfo(self.config.market_timezone)).timestamp() - (
            self.config.entry_cooldown_minutes * 60
        )
        recently_closed: set[str] = set()
        for item in state.get("trade_journal", []):
            if item.get("event") != "close":
                continue
            timestamp = item.get("timestamp")
            if not timestamp:
                continue
            try:
                closed_at = datetime.fromisoformat(timestamp).timestamp()
            except ValueError:
                continue
            if closed_at >= cooldown_cutoff:
                recently_closed.add(str(item.get("pair")))

        for opportunity in opportunities:
            if opportunity.get("signal") not in {"long_a_short_b", "short_a_long_b"}:
                continue
            pair_name = f"{opportunity['symbol_a']}/{opportunity['symbol_b']}"
            if pair_name in open_pair_names or pair_name in recently_closed:
                continue
            if len(open_pair_names) + len(plans) >= self.config.max_open_pairs:
                break
            if len(plans) >= self.config.max_new_trades_per_cycle:
                break
            plan = create_trade_plan(opportunity, allocation_eur=self.config.allocation_eur)
            plan["entry_price_a"] = plan["price_a"]
            plan["entry_price_b"] = plan["price_b"]
            plan["entry_zscore"] = opportunity.get("zscore")
            plan["planned_at"] = datetime.now(ZoneInfo(self.config.market_timezone)).isoformat()
            plans.append(plan)

        return plans

    def _refresh_open_pairs(
        self,
        state: dict[str, Any],
        pair_candidates: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        candidate_map = {f"{item['symbol_a']}/{item['symbol_b']}": item for item in pair_candidates}
        updated_open_pairs: list[dict[str, Any]] = []
        closed_pairs: list[dict[str, Any]] = []

        for open_pair in state.get("open_pairs", []):
            pair_name = open_pair.get("pair")
            current_candidate = candidate_map.get(pair_name)
            if current_candidate is None:
                updated_open_pairs.append(open_pair)
                continue

            marked = update_open_pair_mark_to_market(open_pair, current_candidate)
            exit_decision = assess_exit(
                marked,
                current_candidate,
                exit_zscore=self.config.exit_zscore,
                stop_zscore=self.config.stop_zscore,
            )
            marked["exit_reason"] = exit_decision["reason"]
            marked["exit_action"] = exit_decision["action"]

            if exit_decision["action"] == "close" and self.config.only_trade_regular_hours:
                now_market = datetime.now(ZoneInfo(self.config.market_timezone))
                if not is_trading_window_open(
                    now_market,
                    start_time=self.config.trading_start_time,
                    end_time="15:55",
                ):
                    marked["exit_reason"] = "waiting_for_market_open"
                    marked["exit_action"] = "hold"

            if marked.get("exit_action") == "close":
                closed_pairs.append(marked)
            else:
                updated_open_pairs.append(marked)

        return updated_open_pairs, closed_pairs

    def _apply_execution_results(
        self,
        state: dict[str, Any],
        trade_plans: list[dict[str, Any]],
        execution_results: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        accepted_plans: list[dict[str, Any]] = []
        result_map = {item.get("pair"): item for item in execution_results}

        for plan in trade_plans:
            execution = result_map.get(plan["pair"], {})
            order_rows = execution.get("orders", [])
            statuses = {str(item.get("status", "")) for item in order_rows}
            if any(status in {"Cancelled", "ApiCancelled", "Inactive"} for status in statuses):
                self.logger.warning("EXEC", "trade rejected", pair=plan["pair"], statuses=sorted(statuses))
                self._append_audit_event("entry_rejected", pair=plan["pair"], statuses=sorted(statuses))
                continue

            fill_map = {
                item.get("symbol"): _safe_float(item.get("avg_fill_price"), 0.0)
                for item in order_rows
                if item.get("avg_fill_price") is not None
            }
            plan = dict(plan)
            if fill_map.get(plan["symbol_a"]):
                plan["entry_price_a"] = round(fill_map[plan["symbol_a"]], 6)
            if fill_map.get(plan["symbol_b"]):
                plan["entry_price_b"] = round(fill_map[plan["symbol_b"]], 6)
            plan["execution_status"] = execution.get("status", "submitted")
            plan["submitted_at"] = datetime.now(ZoneInfo(self.config.market_timezone)).isoformat()
            accepted_plans.append(plan)
            self._append_audit_event(
                "entry_submitted",
                pair=plan["pair"],
                status=plan["execution_status"],
                exposure=plan.get("gross_exposure"),
            )

        state["open_pairs"] = list(state.get("open_pairs", [])) + accepted_plans
        state["submitted_orders"] = int(state.get("submitted_orders", 0)) + len(accepted_plans)
        return accepted_plans

    def _build_performance_metrics(
        self,
        result: dict[str, Any],
        opportunities: list[dict[str, Any]],
        trade_plans: list[dict[str, Any]],
        state: dict[str, Any],
    ) -> dict[str, Any]:
        account_summary = result.get("account_summary", {})
        net_liq = _extract_numeric(account_summary.get("NetLiquidation"))
        available = _extract_numeric(account_summary.get("AvailableFunds"))
        planned_exposure = sum(_safe_float(plan.get("gross_exposure")) for plan in trade_plans)
        estimated_fees = sum(_safe_float(plan.get("estimated_fees")) for plan in trade_plans)
        unrealized = sum(_safe_float(item.get("unrealized_pnl")) for item in state.get("open_pairs", []))
        realized = _safe_float(state.get("realized_pnl"))
        journal_stats = summarize_trade_journal(state.get("trade_journal", []))
        return {
            "allocation_eur": round(self.config.allocation_eur, 2),
            "planned_exposure_eur": round(planned_exposure, 2),
            "estimated_fees_eur": round(estimated_fees, 2),
            "allocation_utilization": round(planned_exposure / max(self.config.allocation_eur, 1.0), 4),
            "net_liquidation_eur": None if net_liq is None else round(net_liq, 2),
            "available_funds_eur": None if available is None else round(available, 2),
            "open_positions_count": len(result.get("positions", [])),
            "tracked_open_pairs": len(state.get("open_pairs", [])),
            "realized_pnl": round(realized, 2),
            "daily_realized_pnl": round(_safe_float(state.get("daily_realized_pnl")), 2),
            "unrealized_pnl": round(unrealized, 2),
            "closed_trades": journal_stats["closed_trades"],
            "win_rate": journal_stats["win_rate"],
            "avg_realized_pnl": journal_stats["avg_realized_pnl"],
            "opportunities_found": len(opportunities),
            "halt_new_entries": bool(state.get("halt_new_entries")),
            "halt_reason": state.get("halt_reason"),
        }

    def run_cycle(
        self,
        symbols: list[str] | None = None,
        top_n: int | None = None,
        min_score: float | None = None,
        log_progress: bool = True,
    ) -> dict[str, Any]:
        self.logger.enabled = log_progress
        symbols = symbols or self.config.symbols
        top_n = top_n or self.config.pair_scan_top_n
        min_score = self.config.opportunity_min_score if min_score is None else min_score
        state = self._load_state()
        self._reset_daily_state_if_needed(state)
        self._write_heartbeat("running", cycle_count=state.get("cycle_count", 0), open_pairs=len(state.get("open_pairs", [])))

        self.logger.info("BOT", "cycle started", symbols=len(symbols), allocation=self.config.allocation_eur, top_n=top_n)

        result = self._run_with_retries("account probe", self.probe.run_probe)
        reconciliation = self._reconcile_positions(state)
        scan_result = self._run_with_retries(
            "pair scan",
            lambda: self.probe.scan_pairs(
                symbols,
                duration=self.config.pair_scan_duration,
                bar_size=self.config.pair_scan_bar_size,
                top_n=top_n,
                min_correlation=self.config.min_correlation,
                entry_threshold=self.config.entry_zscore,
                watch_threshold=self.config.watch_zscore,
                log_progress=log_progress,
                logger=self.logger,
            ),
        )

        pair_candidates = scan_result.get("pairs", [])
        state["open_pairs"], closed_pairs = self._refresh_open_pairs(state, pair_candidates)
        if closed_pairs:
            realized_increment = sum(_safe_float(item.get("unrealized_pnl")) for item in closed_pairs)
            state["realized_pnl"] = round(_safe_float(state.get("realized_pnl")) + realized_increment, 2)
            state["daily_realized_pnl"] = round(_safe_float(state.get("daily_realized_pnl")) + realized_increment, 2)
            state.setdefault("closed_pairs", []).extend(closed_pairs)
            for item in closed_pairs:
                state.setdefault("trade_journal", []).append(
                    {
                        "timestamp": datetime.now(ZoneInfo(self.config.market_timezone)).isoformat(),
                        "event": "close",
                        "pair": item.get("pair"),
                        "reason": item.get("exit_reason"),
                        "realized_pnl": item.get("unrealized_pnl"),
                    }
                )
                self._append_audit_event(
                    "pair_closed",
                    pair=item.get("pair"),
                    reason=item.get("exit_reason"),
                    pnl=item.get("unrealized_pnl"),
                )
                self.logger.info(
                    "EXIT",
                    "pair closed by rule",
                    pair=item.get("pair"),
                    reason=item.get("exit_reason"),
                    pnl=item.get("unrealized_pnl"),
                )

        opportunities = select_opportunities(pair_candidates, min_score=min_score)
        allow_new_entries, gate_reason = self._allow_new_entries(state)
        trade_plans: list[dict[str, Any]] = []
        execution_results: list[dict[str, Any]] = []

        if allow_new_entries:
            trade_plans = self._build_trade_plans(opportunities, state)
            if trade_plans:
                for plan in trade_plans:
                    state.setdefault("trade_journal", []).append(
                        {
                            "timestamp": datetime.now(ZoneInfo(self.config.market_timezone)).isoformat(),
                            "event": "open",
                            "pair": plan["pair"],
                            "signal": plan["signal"],
                            "estimated_fees": plan["estimated_fees"],
                            "gross_exposure": plan["gross_exposure"],
                        }
                    )
                    self.logger.info(
                        "PLAN",
                        "trade prepared",
                        pair=plan["pair"],
                        a=f"{plan['symbol_a_action']} {plan['qty_a']}",
                        b=f"{plan['symbol_b_action']} {plan['qty_b']}",
                        exposure=plan["gross_exposure"],
                        fees=plan["estimated_fees"],
                    )
                if self.config.enable_orders:
                    execution_results = self._run_with_retries(
                        "order execution",
                        lambda: self.probe.execute_trade_plans(trade_plans, logger=self.logger),
                    )
                    trade_plans = self._apply_execution_results(state, trade_plans, execution_results)
                else:
                    execution_results = [{"pair": plan["pair"], "status": "paper-preview"} for plan in trade_plans]
                    state["open_pairs"] = list(state.get("open_pairs", [])) + trade_plans
                    self.logger.info("EXEC", "execution disabled, preview only", plans=len(trade_plans))
            else:
                self.logger.info("PLAN", "no new trade plans", reason="no actionable directional signals")
        else:
            self.logger.info("GATE", "new entries disabled for this cycle", reason=gate_reason)

        bot_cycle = build_cycle_summary(scan_result.get("summary", {}), pair_candidates, opportunities, trade_plans)
        performance = self._build_performance_metrics(result, opportunities, trade_plans, state)

        state["cycle_count"] = int(state.get("cycle_count", 0)) + 1
        state["consecutive_errors"] = 0
        state["last_error"] = None
        state["last_opportunities"] = [f"{item['symbol_a']}/{item['symbol_b']}" for item in opportunities[:5]]
        self._save_state(state)

        result["pair_scan_summary"] = scan_result.get("summary", {})
        result["pair_candidates"] = pair_candidates
        result["opportunities"] = opportunities
        result["trade_plans"] = trade_plans
        result["open_pairs"] = state.get("open_pairs", [])
        result["closed_pairs"] = closed_pairs
        result["execution"] = execution_results
        result["performance"] = performance
        result["trade_journal_summary"] = summarize_trade_journal(state.get("trade_journal", []))
        result["bot_cycle"] = bot_cycle
        result["entry_gate"] = {"allow_new_entries": allow_new_entries, "reason": gate_reason}
        result["reconciliation"] = reconciliation

        self._write_heartbeat(
            "ok",
            cycle_count=state.get("cycle_count"),
            top_pair=bot_cycle.get("top_pair"),
            opportunities=bot_cycle.get("opportunities_found"),
            halt_reason=state.get("halt_reason"),
        )
        self._append_audit_event(
            "cycle_complete",
            top_pair=bot_cycle.get("top_pair"),
            opportunities=bot_cycle.get("opportunities_found"),
            new_entries=allow_new_entries,
            gate_reason=gate_reason,
        )

        self.logger.info(
            "BOT",
            "cycle complete",
            top_pair=bot_cycle.get("top_pair"),
            signal=bot_cycle.get("top_signal"),
            opportunities=bot_cycle.get("opportunities_found"),
            planned=bot_cycle.get("trade_plans"),
        )

        return result

    def run(
        self,
        iterations: int = 1,
        symbols: list[str] | None = None,
        top_n: int | None = None,
        min_score: float | None = None,
        log_progress: bool = True,
    ) -> list[dict[str, Any]]:
        cycles: list[dict[str, Any]] = []
        run_forever = iterations <= 0
        index = 0

        while run_forever or index < iterations:
            self.logger.enabled = log_progress
            step_text = "forever" if run_forever else f"{index + 1}/{iterations}"
            self.logger.info("LOOP", "monitoring cycle", step=step_text)
            try:
                cycles.append(
                    self.run_cycle(
                        symbols=symbols,
                        top_n=top_n,
                        min_score=min_score,
                        log_progress=log_progress,
                    )
                )
            except Exception as exc:
                state = self._load_state()
                state["consecutive_errors"] = int(state.get("consecutive_errors", 0)) + 1
                state["last_error"] = str(exc)
                self._save_state(state)
                self._write_heartbeat(
                    "error",
                    cycle_count=state.get("cycle_count"),
                    consecutive_errors=state["consecutive_errors"],
                    error=str(exc),
                )
                self._append_audit_event(
                    "cycle_error",
                    consecutive_errors=state["consecutive_errors"],
                    error=str(exc),
                )
                self.logger.error(
                    "LOOP",
                    "cycle failed",
                    consecutive_errors=state["consecutive_errors"],
                    error=str(exc),
                )
                if state["consecutive_errors"] >= self.config.max_consecutive_errors:
                    self.logger.error(
                        "LOOP",
                        "stopping after repeated failures",
                        limit=self.config.max_consecutive_errors,
                    )
                    raise

            index += 1
            if run_forever or index < iterations:
                self.logger.info("LOOP", "sleeping before next cycle", seconds=self.config.loop_interval_seconds)
                time.sleep(self.config.loop_interval_seconds)

        return cycles
