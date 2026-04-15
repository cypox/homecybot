from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import unittest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from homecybot.config import IBConfig, parse_bool, parse_symbols


class ConfigTests(unittest.TestCase):
    def test_parse_bool(self):
        self.assertTrue(parse_bool("true"))
        self.assertTrue(parse_bool("Yes"))
        self.assertFalse(parse_bool("no"))

    def test_parse_symbols(self):
        self.assertEqual(parse_symbols("aapl, msft, spy"), ["AAPL", "MSFT", "SPY"])

    def test_config_requires_explicit_connection_values(self):
        cfg = IBConfig(host="gateway-host", port=1234, client_id=101)
        self.assertEqual(cfg.host, "gateway-host")
        self.assertEqual(cfg.port, 1234)
        self.assertGreaterEqual(len(cfg.symbols), 1)

    def test_config_from_json(self):
        payload = {
            "host": "gateway-host",
            "port": 1234,
            "client_id": 101,
            "readonly": True,
            "symbols": ["AAPL", "MSFT"],
            "snapshot_timeout": 4.0,
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "bot_config.json"
            config_path.write_text(json.dumps(payload), encoding="utf-8")
            cfg = IBConfig.from_json(config_path)

        self.assertEqual(cfg.host, "gateway-host")
        self.assertEqual(cfg.symbols, ["AAPL", "MSFT"])


if __name__ == "__main__":
    unittest.main()
