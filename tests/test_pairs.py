from __future__ import annotations

from pathlib import Path
import sys
import unittest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from homecybot.config import IBConfig
from homecybot.ib_client import IBGatewayProbe
from homecybot.pairs import compute_pair_stats, rank_pairs


class PairStatsTests(unittest.TestCase):
    def test_compute_pair_stats_for_related_series(self):
        prices_a = [100, 101, 102, 103, 104, 105]
        prices_b = [50, 50.5, 51, 51.5, 52, 52.5]

        stats = compute_pair_stats("AAA", prices_a, "BBB", prices_b)

        self.assertEqual(stats.symbol_a, "AAA")
        self.assertEqual(stats.symbol_b, "BBB")
        self.assertGreater(stats.correlation, 0.99)
        self.assertGreater(stats.hedge_ratio, 1.5)
        self.assertIn(stats.signal, {"watch", "flat", "long_a_short_b", "short_a_long_b"})
        self.assertGreaterEqual(stats.return_correlation, 0.0)
        self.assertIsNotNone(stats.cointegration_pvalue)
        self.assertGreaterEqual(stats.liquidity_score, 0.0)

    def test_rank_pairs_puts_best_candidate_first(self):
        data = {
            "AAA": [100, 101, 102, 103, 104, 105],
            "BBB": [50, 50.5, 51, 51.5, 52, 52.5],
            "CCC": [30, 35, 28, 40, 20, 45],
        }

        ranked = rank_pairs(data, top_n=3)

        best = ranked[0]
        self.assertEqual({best.symbol_a, best.symbol_b}, {"AAA", "BBB"})


class LiveIBPairTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        config_path = PROJECT_ROOT / "config" / "settings.json"
        if not config_path.exists():
            raise unittest.SkipTest("Live IB settings file not available")

        try:
            cls.config = IBConfig.from_json(config_path)
            cls.probe = IBGatewayProbe(cls.config)
            cls.probe.connect()
        except Exception as exc:
            raise unittest.SkipTest(f"Unable to connect to IB Gateway for integration tests: {exc}")

    @classmethod
    def tearDownClass(cls):
        probe = getattr(cls, "probe", None)
        if probe is not None:
            probe.disconnect()

    def test_ib_returns_multiple_quote_snapshots(self):
        snapshots = self.probe.market_snapshots(self.config.symbols[:2])

        self.assertGreaterEqual(len(snapshots), 2)
        for snapshot in snapshots:
            self.assertIn("symbol", snapshot)
            self.assertTrue(
                any(snapshot.get(field) is not None for field in ["bid", "ask", "last", "close", "market_price"])
            )

    def test_ib_historical_data_supports_pair_ranking(self):
        symbols = self.config.symbols[:4]
        price_map = self.probe.historical_closes(symbols, duration="30 D", bar_size="1 day")

        self.assertGreaterEqual(len(price_map), 2)
        for symbol, closes in price_map.items():
            self.assertGreaterEqual(len(closes), 5, msg=f"Not enough bars for {symbol}")
            self.assertTrue(all(price > 0 for price in closes))

        ranked = rank_pairs(price_map, top_n=3, min_correlation=0.0)
        self.assertGreaterEqual(len(ranked), 1)

        best = ranked[0]
        self.assertIn(best.symbol_a, price_map)
        self.assertIn(best.symbol_b, price_map)
        self.assertGreaterEqual(best.correlation, -1.0)
        self.assertLessEqual(best.correlation, 1.0)
        self.assertIn(best.signal, {"watch", "flat", "long_a_short_b", "short_a_long_b"})
        self.assertIsNotNone(best.ratio_zscore)


if __name__ == "__main__":
    unittest.main()
