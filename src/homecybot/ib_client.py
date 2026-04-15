from __future__ import annotations

import math
import time
from typing import Any

from ib_insync import IB, MarketOrder, Stock

from .config import IBConfig
from .logger import BotLogger
from .pairs import rank_pairs


def _clean_number(value: Any) -> Any:
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


class IBGatewayProbe:
    def __init__(self, config: IBConfig):
        self.config = config
        self.ib = IB()
        self.connected_host: str | None = None

    def connect(self) -> dict[str, Any]:
        try:
            self.ib.disconnect()
            self.ib.connect(
                host=self.config.host,
                port=self.config.port,
                clientId=self.config.client_id,
                readonly=self.config.readonly,
                timeout=8,
            )
            self.ib.reqMarketDataType(self.config.market_data_type)
        except Exception as exc:  # pragma: no cover - network-dependent
            raise ConnectionError(
                f"Unable to connect to IB Gateway at {self.config.host}:{self.config.port} -> {exc}"
            ) from exc

        self.connected_host = self.config.host
        return {
            "host": self.config.host,
            "port": self.config.port,
            "client_id": self.config.client_id,
            "readonly": self.config.readonly,
        }

    def disconnect(self) -> None:
        self.ib.disconnect()

    def server_time(self) -> str:
        return self.ib.reqCurrentTime().isoformat()

    def account_summary(self) -> dict[str, str]:
        tags_of_interest = {
            "AccountType",
            "NetLiquidation",
            "AvailableFunds",
            "BuyingPower",
            "TotalCashValue",
            "ExcessLiquidity",
            "Cushion",
        }
        summary: dict[str, str] = {}
        for row in self.ib.accountSummary(self.config.account or ""):
            if row.tag in tags_of_interest and row.tag not in summary:
                summary[row.tag] = f"{row.value} {row.currency}".strip()
        return summary

    def positions(self) -> list[dict[str, Any]]:
        rows = []
        for position in self.ib.positions(self.config.account or ""):
            rows.append(
                {
                    "symbol": position.contract.symbol,
                    "secType": position.contract.secType,
                    "position": position.position,
                    "avgCost": position.avgCost,
                }
            )
        return rows

    def qualify_symbols(self, symbols: list[str]):
        contracts = [Stock(symbol, "SMART", "USD") for symbol in symbols]
        return self.ib.qualifyContracts(*contracts)

    def market_snapshots(self, symbols: list[str]) -> list[dict[str, Any]]:
        contracts = self.qualify_symbols(symbols)
        tickers = self.ib.reqTickers(*contracts)
        data: list[dict[str, Any]] = []

        for ticker in tickers:
            bid = _clean_number(ticker.bid)
            ask = _clean_number(ticker.ask)
            market_price = _clean_number(ticker.marketPrice())
            spread = None if bid is None or ask is None else round(ask - bid, 6)
            spread_bps = None
            if spread is not None and market_price not in (None, 0):
                spread_bps = round((spread / market_price) * 10000.0, 6)

            data.append(
                {
                    "symbol": ticker.contract.symbol,
                    "bid": bid,
                    "ask": ask,
                    "last": _clean_number(ticker.last),
                    "close": _clean_number(ticker.close),
                    "market_price": market_price,
                    "spread": spread,
                    "spread_bps": spread_bps,
                    "currency": ticker.contract.currency,
                }
            )

        return data

    def historical_sample(self, symbol: str) -> dict[str, Any]:
        contract = self.qualify_symbols([symbol])[0]
        bars = self.ib.reqHistoricalData(
            contract,
            endDateTime="",
            durationStr="2 D",
            barSizeSetting="1 hour",
            whatToShow="TRADES",
            useRTH=True,
            formatDate=1,
        )
        if not bars:
            return {"symbol": symbol, "bars": 0}

        return {
            "symbol": symbol,
            "bars": len(bars),
            "first": str(bars[0].date),
            "last": str(bars[-1].date),
            "latest_close": bars[-1].close,
        }

    def historical_closes(
        self,
        symbols: list[str],
        duration: str = "30 D",
        bar_size: str = "1 day",
        log_progress: bool = False,
    ) -> dict[str, list[float]]:
        contracts = self.qualify_symbols(symbols)
        closes: dict[str, list[float]] = {}
        start_time = time.perf_counter()

        for index, contract in enumerate(contracts, start=1):
            bars = self.ib.reqHistoricalData(
                contract,
                endDateTime="",
                durationStr=duration,
                barSizeSetting=bar_size,
                whatToShow="TRADES",
                useRTH=True,
                formatDate=1,
            )
            series = [float(bar.close) for bar in bars if getattr(bar, "close", None) is not None]
            if len(series) >= 5:
                closes[contract.symbol] = series

            if log_progress:
                elapsed = time.perf_counter() - start_time
                print(
                    f"[scan] history {index}/{len(contracts)} | {contract.symbol} | "
                    f"bars={len(series)} | ready={len(closes)} | elapsed={elapsed:.1f}s"
                )

        return closes

    def historical_market_data(
        self,
        symbols: list[str],
        duration: str = "30 D",
        bar_size: str = "1 day",
    ) -> dict[str, dict[str, Any]]:
        contracts = self.qualify_symbols(symbols)
        data: dict[str, dict[str, Any]] = {}

        for contract in contracts:
            bars = self.ib.reqHistoricalData(
                contract,
                endDateTime="",
                durationStr=duration,
                barSizeSetting=bar_size,
                whatToShow="TRADES",
                useRTH=True,
                formatDate=1,
            )
            closes = [float(bar.close) for bar in bars if getattr(bar, "close", None) is not None]
            volumes = [float(bar.volume) for bar in bars if getattr(bar, "volume", None) is not None]
            if len(closes) >= 5:
                data[contract.symbol] = {"closes": closes, "volumes": volumes}

        return data

    def recent_execution_costs(self, symbols: list[str] | None = None) -> dict[str, dict[str, Any]]:
        wanted = None if symbols is None else set(symbols)
        stats: dict[str, dict[str, Any]] = {}

        try:
            fills = list(self.ib.fills())
        except Exception:
            fills = []

        for fill in fills:
            symbol = getattr(fill.contract, "symbol", None)
            if not symbol or (wanted is not None and symbol not in wanted):
                continue

            commission_report = getattr(fill, "commissionReport", None)
            execution = getattr(fill, "execution", None)
            shares = abs(float(getattr(execution, "shares", 0) or 0))
            commission = float(getattr(commission_report, "commission", 0) or 0)
            if shares <= 0 or commission <= 0:
                continue

            bucket = stats.setdefault(symbol, {"total_commission": 0.0, "total_shares": 0.0, "fills": 0})
            bucket["total_commission"] += commission
            bucket["total_shares"] += shares
            bucket["fills"] += 1

        for symbol, bucket in stats.items():
            bucket["avg_commission_per_share"] = round(bucket["total_commission"] / max(bucket["total_shares"], 1.0), 6)

        return stats

    def scan_pairs(
        self,
        symbols: list[str],
        duration: str = "30 D",
        bar_size: str = "1 day",
        top_n: int = 5,
        min_correlation: float = 0.8,
        entry_threshold: float = 1.5,
        watch_threshold: float = 1.0,
        log_progress: bool = False,
        logger: BotLogger | None = None,
    ) -> dict[str, Any]:
        start_time = time.perf_counter()
        logger = logger or BotLogger(enabled=log_progress)
        if log_progress:
            logger.info(
                "SCAN",
                "starting sweep",
                symbols=len(symbols),
                window=duration,
                bars=bar_size,
                min_corr=min_correlation,
            )

        market_data = self.historical_market_data(symbols, duration=duration, bar_size=bar_size)
        quote_map = {item["symbol"]: item for item in self.market_snapshots(symbols)}
        execution_costs = self.recent_execution_costs(symbols)

        for symbol, payload in market_data.items():
            payload["quote"] = quote_map.get(symbol, {})
            payload["recent_commission_per_share"] = execution_costs.get(symbol, {}).get("avg_commission_per_share")

        if log_progress:
            for index, symbol in enumerate(sorted(market_data), start=1):
                logger.info("HISTORY", "series ready", step=f"{index}/{len(market_data)}", symbol=symbol, bars=len(market_data[symbol]["closes"]))

        last_report_time = start_time

        def progress_update(progress: dict[str, Any]) -> None:
            nonlocal last_report_time
            if not log_progress:
                return
            now = time.perf_counter()
            is_final = progress.get("scanned_pairs") == progress.get("total_pairs")
            if is_final or now - last_report_time >= 0.75:
                rate = float(progress.get("scanned_pairs", 0)) / max(now - start_time, 1e-9)
                logger.info(
                    "PAIRS",
                    "scan progress",
                    progress=f"{progress.get('scanned_pairs')}/{progress.get('total_pairs')}",
                    rate=f"{rate:.2f}/s",
                    top_corr=progress.get("top_correlation"),
                    opps=progress.get("opportunities_found"),
                    last=progress.get("current_pair"),
                )
                last_report_time = now

        ranked = rank_pairs(
            market_data,
            top_n=top_n,
            min_correlation=min_correlation,
            entry_threshold=entry_threshold,
            watch_threshold=watch_threshold,
            progress_callback=progress_update,
        )

        symbol_count = len(market_data)
        pairs_considered = (symbol_count * (symbol_count - 1)) // 2 if symbol_count >= 2 else 0
        elapsed = time.perf_counter() - start_time
        actionable_pairs = sum(1 for item in ranked if item.signal != "flat")
        summary = {
            "symbols_requested": len(symbols),
            "symbols_with_history": symbol_count,
            "pairs_considered": pairs_considered,
            "pairs_returned": len(ranked),
            "actionable_pairs": actionable_pairs,
            "duration": duration,
            "bar_size": bar_size,
            "min_correlation": min_correlation,
            "elapsed_seconds": round(elapsed, 3),
            "pairs_per_second": round(pairs_considered / max(elapsed, 1e-9), 3),
        }

        if log_progress:
            logger.info(
                "SCAN",
                "sweep complete",
                scanned=pairs_considered,
                returned=len(ranked),
                actionable=actionable_pairs,
                elapsed=f"{elapsed:.2f}s",
            )

        pair_rows = []
        for item in ranked:
            row = item.to_dict()
            quote_a = quote_map.get(item.symbol_a, {})
            quote_b = quote_map.get(item.symbol_b, {})
            row["bid_a"] = quote_a.get("bid")
            row["ask_a"] = quote_a.get("ask")
            row["bid_b"] = quote_b.get("bid")
            row["ask_b"] = quote_b.get("ask")
            row["spread_bps_a"] = quote_a.get("spread_bps")
            row["spread_bps_b"] = quote_b.get("spread_bps")
            row["recent_commission_per_share"] = max(
                execution_costs.get(item.symbol_a, {}).get("avg_commission_per_share", 0.0),
                execution_costs.get(item.symbol_b, {}).get("avg_commission_per_share", 0.0),
            )
            pair_rows.append(row)

        return {
            "summary": summary,
            "pairs": pair_rows,
        }

    def execute_trade_plans(self, trade_plans: list[dict[str, Any]], logger: BotLogger | None = None) -> list[dict[str, Any]]:
        logger = logger or BotLogger(enabled=False)
        execution_results: list[dict[str, Any]] = []

        for plan in trade_plans:
            pair = plan.get("pair")
            logger.info("EXEC", "submitting pair order", pair=pair, exposure=plan.get("gross_exposure"))
            contracts = {contract.symbol: contract for contract in self.qualify_symbols([plan["symbol_a"], plan["symbol_b"]])}
            orders = []
            statuses: list[str] = []
            for symbol_key, action_key, qty_key in (
                ("symbol_a", "symbol_a_action", "qty_a"),
                ("symbol_b", "symbol_b_action", "qty_b"),
            ):
                symbol = plan[symbol_key]
                action = plan[action_key]
                quantity = int(plan[qty_key])
                if quantity <= 0:
                    continue
                try:
                    order = MarketOrder(action, quantity)
                    trade = self.ib.placeOrder(contracts[symbol], order)
                    try:
                        self.ib.sleep(1.0)
                    except Exception:
                        pass

                    fills = list(getattr(trade, "fills", []))
                    commission = sum(
                        float(getattr(getattr(fill, "commissionReport", None), "commission", 0) or 0)
                        for fill in fills
                    )
                    fill_prices = [float(getattr(getattr(fill, "execution", None), "price", 0) or 0) for fill in fills]
                    avg_fill_price = None
                    if fill_prices:
                        avg_fill_price = round(sum(fill_prices) / len(fill_prices), 6)

                    status = str(getattr(trade.orderStatus, "status", "Submitted"))
                    statuses.append(status)
                    orders.append(
                        {
                            "symbol": symbol,
                            "action": action,
                            "quantity": quantity,
                            "order_id": getattr(trade.order, "orderId", None),
                            "status": status,
                            "filled": getattr(trade.orderStatus, "filled", 0),
                            "remaining": getattr(trade.orderStatus, "remaining", quantity),
                            "avg_fill_price": avg_fill_price if avg_fill_price is not None else getattr(trade.orderStatus, "avgFillPrice", None),
                            "commission": round(commission, 6),
                        }
                    )
                except Exception as exc:
                    statuses.append("ERROR")
                    orders.append(
                        {
                            "symbol": symbol,
                            "action": action,
                            "quantity": quantity,
                            "status": "ERROR",
                            "error": str(exc),
                        }
                    )
                    logger.error("EXEC", "order submission failed", pair=pair, symbol=symbol, error=str(exc))

            if statuses and all(status == "Filled" for status in statuses):
                pair_status = "Filled"
            elif any(status in {"Cancelled", "ApiCancelled", "Inactive", "ERROR"} for status in statuses):
                pair_status = "Attention"
            elif any(status in {"PreSubmitted", "PendingSubmit", "Submitted"} for status in statuses):
                pair_status = "Working"
            else:
                pair_status = statuses[-1] if statuses else "submitted"

            execution_results.append({"pair": pair, "status": pair_status, "orders": orders})

        return execution_results

    def run_probe(self) -> dict[str, Any]:
        connection = self.connect()
        result: dict[str, Any] = {
            "connection": connection,
            "server_time": self.server_time(),
            "account_summary": {},
            "positions": [],
            "quotes": [],
            "historical_sample": {},
        }

        try:
            result["account_summary"] = self.account_summary()
        except Exception as exc:  # pragma: no cover - network-dependent
            result["account_summary_error"] = str(exc)

        try:
            result["positions"] = self.positions()
        except Exception as exc:  # pragma: no cover - network-dependent
            result["positions_error"] = str(exc)

        try:
            result["quotes"] = self.market_snapshots(self.config.symbols)
        except Exception as exc:  # pragma: no cover - network-dependent
            result["quotes_error"] = str(exc)

        if self.config.symbols:
            try:
                result["historical_sample"] = self.historical_sample(self.config.symbols[0])
            except Exception as exc:  # pragma: no cover - network-dependent
                result["historical_error"] = str(exc)

        return result
