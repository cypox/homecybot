from __future__ import annotations

from datetime import datetime
from pathlib import Path
import sys
import unittest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from homecybot.bot import (
    assess_exit,
    build_cycle_summary,
    create_trade_plan,
    is_trading_window_open,
    select_opportunities,
    should_halt_trading,
    summarize_trade_journal,
    update_open_pair_mark_to_market,
)


class BotLogicTests(unittest.TestCase):
    def test_select_opportunities_filters_actionable_pairs(self):
        pairs = [
            {"symbol_a": "AAA", "symbol_b": "BBB", "signal": "flat", "score": 50.0, "zscore": 0.2},
            {"symbol_a": "CCC", "symbol_b": "DDD", "signal": "watch", "score": 72.0, "zscore": 1.2},
            {"symbol_a": "EEE", "symbol_b": "FFF", "signal": "short_a_long_b", "score": 80.0, "zscore": 2.1},
        ]

        opportunities = select_opportunities(pairs, min_score=70.0)

        self.assertEqual(len(opportunities), 2)
        self.assertEqual(opportunities[0]["symbol_a"], "EEE")

    def test_create_trade_plan_respects_allocation(self):
        pair = {
            "symbol_a": "NVDA",
            "symbol_b": "QQQ",
            "signal": "short_a_long_b",
            "score": 80.4,
            "zscore": 2.1,
            "hedge_ratio": 0.39,
            "latest_price_a": 196.5,
            "latest_price_b": 628.6,
            "bid_a": 196.45,
            "ask_a": 196.55,
            "bid_b": 628.55,
            "ask_b": 628.65,
            "recent_commission_per_share": 0.0035,
        }

        plan = create_trade_plan(pair, allocation_eur=10000.0)

        self.assertEqual(plan["symbol_a_action"], "SELL")
        self.assertEqual(plan["symbol_b_action"], "BUY")
        self.assertGreater(plan["qty_a"], 0)
        self.assertGreater(plan["qty_b"], 0)
        self.assertLessEqual(plan["gross_exposure"], 10500.0)
        self.assertGreater(plan["estimated_fees"], 0.0)

    def test_assess_exit_flags_mean_reversion_close(self):
        open_pair = {"pair": "NVDA/QQQ", "entry_zscore": 2.2}
        candidate = {"symbol_a": "NVDA", "symbol_b": "QQQ", "zscore": 0.1}

        decision = assess_exit(open_pair, candidate, exit_zscore=0.5, stop_zscore=3.0)

        self.assertEqual(decision["action"], "close")
        self.assertEqual(decision["reason"], "mean_reversion")

    def test_update_open_pair_mark_to_market_tracks_pnl(self):
        open_pair = {
            "pair": "NVDA/QQQ",
            "symbol_a": "NVDA",
            "symbol_b": "QQQ",
            "symbol_a_action": "SELL",
            "symbol_b_action": "BUY",
            "qty_a": 10,
            "qty_b": 4,
            "entry_price_a": 200.0,
            "entry_price_b": 600.0,
        }
        candidate = {
            "symbol_a": "NVDA",
            "symbol_b": "QQQ",
            "latest_price_a": 190.0,
            "latest_price_b": 610.0,
            "zscore": 0.4,
        }

        updated = update_open_pair_mark_to_market(open_pair, candidate)

        self.assertIn("unrealized_pnl", updated)
        self.assertNotEqual(updated["unrealized_pnl"], 0.0)

    def test_summarize_trade_journal(self):
        journal = [
            {"pair": "AAA/BBB", "realized_pnl": 120.0},
            {"pair": "CCC/DDD", "realized_pnl": -20.0},
            {"pair": "EEE/FFF", "realized_pnl": 40.0},
        ]

        stats = summarize_trade_journal(journal)

        self.assertEqual(stats["closed_trades"], 3)
        self.assertAlmostEqual(stats["win_rate"], 2 / 3, places=6)
        self.assertAlmostEqual(stats["avg_realized_pnl"], 46.666667, places=5)

    def test_build_cycle_summary_reports_top_pair_and_counts(self):
        scan_summary = {"pairs_considered": 10, "pairs_returned": 3}
        opportunities = [{"symbol_a": "EEE", "symbol_b": "FFF", "signal": "short_a_long_b", "score": 80.0}]
        candidates = [
            {"symbol_a": "EEE", "symbol_b": "FFF", "signal": "short_a_long_b", "score": 80.0},
            {"symbol_a": "CCC", "symbol_b": "DDD", "signal": "watch", "score": 72.0},
        ]
        trade_plans = [{"pair": "EEE/FFF", "gross_exposure": 9900.0}]

        summary = build_cycle_summary(scan_summary, candidates, opportunities, trade_plans)

        self.assertEqual(summary["pairs_considered"], 10)
        self.assertEqual(summary["opportunities_found"], 1)
        self.assertEqual(summary["trade_plans"], 1)
        self.assertEqual(summary["top_pair"], "EEE/FFF")

    def test_trading_window_open_only_during_weekday_session(self):
        self.assertTrue(is_trading_window_open(datetime(2026, 4, 15, 10, 0), "09:35", "15:30"))
        self.assertFalse(is_trading_window_open(datetime(2026, 4, 18, 10, 0), "09:35", "15:30"))
        self.assertFalse(is_trading_window_open(datetime(2026, 4, 15, 8, 0), "09:35", "15:30"))

    def test_should_halt_trading_on_loss_limit(self):
        decision = should_halt_trading(
            {"realized_pnl": -260.0, "open_pairs": []},
            max_daily_loss_eur=250.0,
            max_open_pairs=2,
        )

        self.assertTrue(decision["halt"])
        self.assertEqual(decision["reason"], "max_daily_loss")


if __name__ == "__main__":
    unittest.main()
