from __future__ import annotations

import argparse
import json
from typing import Any

from .bot import PairTradingBot
from .config import IBConfig
from .ib_client import IBGatewayProbe
from .logger import BotLogger


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Probe IB Gateway connectivity and market data access.")
    parser.add_argument("--config", default="config/settings.json", help="Path to the bot JSON settings file")
    parser.add_argument("--symbols", nargs="*", help="Override the symbol list for this run")
    parser.add_argument("--scan-pairs", action="store_true", help="Run the basic pair scanner")
    parser.add_argument("--run-bot", action="store_true", help="Run the first end-to-end bot cycle")
    parser.add_argument("--top", type=int, help="Override the number of ranked pairs to return")
    parser.add_argument("--iterations", type=int, default=1, help="Number of monitoring cycles when running the bot")
    parser.add_argument("--min-score", type=float, help="Override the minimum opportunity score")
    parser.add_argument("--json", action="store_true", help="Print raw JSON output")
    parser.add_argument("--run-forever", action="store_true", help="Keep monitoring until manually stopped")
    return parser


def format_report(result: dict[str, Any]) -> str:
    lines = []
    conn = result.get("connection", {})
    lines.append("=== IB Gateway Probe ===")
    lines.append(f"Connected host: {conn.get('host', '?')}:{conn.get('port', '?')}")
    lines.append(f"Client ID: {conn.get('client_id', '?')}")
    lines.append(f"Server time: {result.get('server_time', 'n/a')}")
    lines.append("")

    lines.append("Account summary:")
    summary = result.get("account_summary", {})
    if summary:
        for key, value in summary.items():
            lines.append(f"  - {key}: {value}")
    else:
        lines.append("  - No account summary returned")
    if "account_summary_error" in result:
        lines.append(f"  - Error: {result['account_summary_error']}")
    lines.append("")

    lines.append("Positions:")
    positions = result.get("positions", [])
    if positions:
        for item in positions:
            lines.append(
                f"  - {item['symbol']} | {item['secType']} | qty={item['position']} | avgCost={item['avgCost']}"
            )
    else:
        lines.append("  - No open positions")
    if "positions_error" in result:
        lines.append(f"  - Error: {result['positions_error']}")
    lines.append("")

    lines.append("Quotes:")
    quotes = result.get("quotes", [])
    if quotes:
        for quote in quotes:
            lines.append(
                "  - "
                f"{quote['symbol']}: market={quote.get('market_price')} last={quote.get('last')} "
                f"bid={quote.get('bid')} ask={quote.get('ask')} close={quote.get('close')}"
            )
    else:
        lines.append("  - No quote snapshots returned")
    if "quotes_error" in result:
        lines.append(f"  - Error: {result['quotes_error']}")
    lines.append("")

    lines.append("Historical sample:")
    hist = result.get("historical_sample", {})
    if hist:
        lines.append(
            f"  - {hist.get('symbol')}: bars={hist.get('bars')} latest_close={hist.get('latest_close')}"
        )
    else:
        lines.append("  - No historical sample returned")
    if "historical_error" in result:
        lines.append(f"  - Error: {result['historical_error']}")
    lines.append("")

    lines.append("Pair scan summary:")
    scan_summary = result.get("pair_scan_summary", {})
    if scan_summary:
        lines.append(
            f"  - requested={scan_summary.get('symbols_requested')} with_history={scan_summary.get('symbols_with_history')} "
            f"pairs={scan_summary.get('pairs_considered')} returned={scan_summary.get('pairs_returned')} actionable={scan_summary.get('actionable_pairs')}"
        )
        lines.append(
            f"  - window={scan_summary.get('duration')} bars={scan_summary.get('bar_size')} "
            f"min_corr={scan_summary.get('min_correlation')} rate={scan_summary.get('pairs_per_second')} pairs/s"
        )
    else:
        lines.append("  - No pair scan summary available")
    lines.append("")

    lines.append("Candidate pairs:")
    pair_candidates = result.get("pair_candidates", [])
    if pair_candidates:
        for item in pair_candidates:
            lines.append(
                f"  - {item['symbol_a']}/{item['symbol_b']} | signal={item['signal']} | score={item['score']}"
            )
            lines.append(
                f"    corr={item['correlation']} return_corr={item['return_correlation']} hedge={item['hedge_ratio']} half_life={item['half_life']}"
            )
            lines.append(
                f"    z={item['zscore']} ratio_z={item['ratio_zscore']} spread_std={item['spread_std']} crossings={item['mean_crossings']}"
            )
            lines.append(
                f"    latest={item['symbol_a']}:{item['latest_price_a']} | {item['symbol_b']}:{item['latest_price_b']}"
            )
    else:
        lines.append("  - No pair candidates returned")
    if "pair_scan_error" in result:
        lines.append(f"  - Error: {result['pair_scan_error']}")
    lines.append("")

    lines.append("Opportunities:")
    opportunities = result.get("opportunities", [])
    if opportunities:
        for item in opportunities[:5]:
            lines.append(
                f"  - {item['symbol_a']}/{item['symbol_b']} | {item['signal']} | score={item['score']} | z={item['zscore']}"
            )
    else:
        lines.append("  - No active opportunities")
    lines.append("")

    lines.append("Trade plans:")
    trade_plans = result.get("trade_plans", [])
    if trade_plans:
        for plan in trade_plans:
            lines.append(
                f"  - {plan['pair']} | {plan['symbol_a_action']} {plan['qty_a']} / {plan['symbol_b_action']} {plan['qty_b']} | exposure={plan['gross_exposure']}"
            )
    else:
        lines.append("  - No trade plans prepared")
    lines.append("")

    lines.append("Performance:")
    performance = result.get("performance", {})
    if performance:
        lines.append(
            f"  - allocation={performance.get('allocation_eur')} planned={performance.get('planned_exposure_eur')} utilization={performance.get('allocation_utilization')}"
        )
        lines.append(
            f"  - net_liq={performance.get('net_liquidation_eur')} available={performance.get('available_funds_eur')} open_positions={performance.get('open_positions_count')}"
        )
        lines.append(
            f"  - realized={performance.get('realized_pnl')} daily_realized={performance.get('daily_realized_pnl')} unrealized={performance.get('unrealized_pnl')} halt={performance.get('halt_reason')}"
        )
    else:
        lines.append("  - No performance metrics available")

    return "\n".join(lines)


def main() -> int:
    args = build_parser().parse_args()

    try:
        config = IBConfig.from_json(args.config)
        if args.symbols:
            config.symbols = [symbol.upper() for symbol in args.symbols]

        probe = IBGatewayProbe(config)
        logger = BotLogger(enabled=not args.json)
        try:
            if args.run_bot:
                bot = PairTradingBot(config, probe, logger=logger)
                cycles = bot.run(
                    iterations=0 if args.run_forever else args.iterations,
                    symbols=config.symbols,
                    top_n=args.top or config.pair_scan_top_n,
                    min_score=args.min_score,
                    log_progress=not args.json,
                )
                result = cycles[-1]
                result["bot_cycles"] = len(cycles)
            else:
                result = probe.run_probe()
                if args.scan_pairs:
                    try:
                        scan_result = probe.scan_pairs(
                            config.symbols,
                            duration=config.pair_scan_duration,
                            bar_size=config.pair_scan_bar_size,
                            top_n=args.top or config.pair_scan_top_n,
                            min_correlation=config.min_correlation,
                            entry_threshold=config.entry_zscore,
                            watch_threshold=config.watch_zscore,
                            log_progress=not args.json,
                            logger=logger,
                        )
                        result["pair_scan_summary"] = scan_result.get("summary", {})
                        result["pair_candidates"] = scan_result.get("pairs", [])
                    except Exception as exc:
                        result["pair_scan_error"] = str(exc)

            if args.json:
                print(json.dumps(result, indent=2, default=str))
            else:
                print(format_report(result))
            return 0
        finally:
            probe.disconnect()
    except FileNotFoundError:
        print(f"Configuration file not found: {args.config}")
        return 1
    except Exception as exc:
        print("Unable to complete the IB probe.")
        print(str(exc))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
