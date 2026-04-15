from __future__ import annotations

from dataclasses import dataclass, field
import json
from pathlib import Path

DEFAULT_SYMBOLS = ["AAPL", "MSFT", "SPY", "QQQ"]


def parse_bool(value: bool | str | None, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def parse_symbols(value: list[str] | str | None) -> list[str]:
    if not value:
        return DEFAULT_SYMBOLS.copy()
    if isinstance(value, list):
        symbols = [str(item).strip().upper() for item in value if str(item).strip()]
    else:
        symbols = [item.strip().upper() for item in value.split(",") if item.strip()]
    return symbols or DEFAULT_SYMBOLS.copy()


@dataclass(slots=True)
class IBConfig:
    host: str
    port: int
    client_id: int
    account: str | None = None
    readonly: bool = True
    symbols: list[str] = field(default_factory=DEFAULT_SYMBOLS.copy)
    snapshot_timeout: float = 4.0
    market_data_type: int = 3
    pair_scan_duration: str = "30 D"
    pair_scan_bar_size: str = "1 day"
    pair_scan_top_n: int = 5
    min_correlation: float = 0.8
    entry_zscore: float = 1.5
    watch_zscore: float = 1.0
    exit_zscore: float = 0.5
    stop_zscore: float = 3.0
    opportunity_min_score: float = 70.0
    loop_interval_seconds: float = 30.0
    allocation_eur: float = 10000.0
    max_open_pairs: int = 2
    max_new_trades_per_cycle: int = 1
    enable_orders: bool = False
    enable_new_entries: bool = True
    only_trade_regular_hours: bool = True
    market_timezone: str = "America/New_York"
    trading_start_time: str = "09:35"
    trading_stop_new_entries_time: str = "15:30"
    max_daily_loss_eur: float = 250.0
    reconnect_attempts: int = 4
    max_retry_delay_seconds: float = 30.0
    max_consecutive_errors: int = 20
    entry_cooldown_minutes: int = 120
    state_path: str = "data/runtime_state.json"
    heartbeat_path: str = "data/heartbeat.json"
    audit_log_path: str = "data/paper_trade_audit.jsonl"

    @classmethod
    def from_json(cls, path: str | Path) -> "IBConfig":
        config_path = Path(path)
        payload = json.loads(config_path.read_text(encoding="utf-8"))

        host = str(payload.get("host", "")).strip()
        port = int(payload.get("port", 0))
        client_id = int(payload.get("client_id", 0))

        market_data_type = int(payload.get("market_data_type", 3))
        pair_scan_top_n = int(payload.get("pair_scan_top_n", 5))
        min_correlation = float(payload.get("min_correlation", 0.8))
        entry_zscore = float(payload.get("entry_zscore", 1.5))
        watch_zscore = float(payload.get("watch_zscore", 1.0))
        exit_zscore = float(payload.get("exit_zscore", 0.5))
        stop_zscore = float(payload.get("stop_zscore", 3.0))
        opportunity_min_score = float(payload.get("opportunity_min_score", 70.0))
        loop_interval_seconds = float(payload.get("loop_interval_seconds", 30.0))
        allocation_eur = float(payload.get("allocation_eur", 10000.0))
        max_open_pairs = int(payload.get("max_open_pairs", 2))
        max_new_trades_per_cycle = int(payload.get("max_new_trades_per_cycle", 1))
        enable_orders = parse_bool(payload.get("enable_orders"), default=False)
        enable_new_entries = parse_bool(payload.get("enable_new_entries"), default=True)
        only_trade_regular_hours = parse_bool(payload.get("only_trade_regular_hours"), default=True)
        market_timezone = str(payload.get("market_timezone", "America/New_York"))
        trading_start_time = str(payload.get("trading_start_time", "09:35"))
        trading_stop_new_entries_time = str(payload.get("trading_stop_new_entries_time", "15:30"))
        max_daily_loss_eur = float(payload.get("max_daily_loss_eur", 250.0))
        reconnect_attempts = int(payload.get("reconnect_attempts", 4))
        max_retry_delay_seconds = float(payload.get("max_retry_delay_seconds", 30.0))
        max_consecutive_errors = int(payload.get("max_consecutive_errors", 20))
        entry_cooldown_minutes = int(payload.get("entry_cooldown_minutes", 120))
        state_path = str(payload.get("state_path", "data/runtime_state.json"))
        heartbeat_path = str(payload.get("heartbeat_path", "data/heartbeat.json"))
        audit_log_path = str(payload.get("audit_log_path", "data/paper_trade_audit.jsonl"))

        if not host:
            raise ValueError("Missing required configuration value: host")
        if port <= 0:
            raise ValueError("Missing or invalid configuration value: port")
        if client_id < 0:
            raise ValueError("Invalid configuration value: client_id")
        if market_data_type not in {1, 2, 3, 4}:
            raise ValueError("Invalid configuration value: market_data_type")
        if pair_scan_top_n <= 0:
            raise ValueError("Invalid configuration value: pair_scan_top_n")
        if not 0 <= min_correlation <= 1:
            raise ValueError("Invalid configuration value: min_correlation")
        if entry_zscore <= 0 or watch_zscore <= 0 or exit_zscore < 0 or stop_zscore <= 0:
            raise ValueError("Invalid configuration value: z-score thresholds")
        if opportunity_min_score < 0:
            raise ValueError("Invalid configuration value: opportunity_min_score")
        if loop_interval_seconds <= 0:
            raise ValueError("Invalid configuration value: loop_interval_seconds")
        if allocation_eur <= 0:
            raise ValueError("Invalid configuration value: allocation_eur")
        if max_open_pairs <= 0 or max_new_trades_per_cycle <= 0:
            raise ValueError("Invalid configuration value: pair limits")
        if max_daily_loss_eur < 0:
            raise ValueError("Invalid configuration value: max_daily_loss_eur")
        if reconnect_attempts <= 0 or max_retry_delay_seconds <= 0 or max_consecutive_errors <= 0:
            raise ValueError("Invalid configuration value: retry controls")
        if entry_cooldown_minutes < 0:
            raise ValueError("Invalid configuration value: entry_cooldown_minutes")
        if not state_path.strip() or not heartbeat_path.strip() or not audit_log_path.strip():
            raise ValueError("Invalid configuration value: runtime file paths")

        return cls(
            host=host,
            port=port,
            client_id=client_id,
            account=payload.get("account") or None,
            readonly=parse_bool(payload.get("readonly"), default=True),
            symbols=parse_symbols(payload.get("symbols")),
            snapshot_timeout=float(payload.get("snapshot_timeout", 4.0)),
            market_data_type=market_data_type,
            pair_scan_duration=str(payload.get("pair_scan_duration", "30 D")),
            pair_scan_bar_size=str(payload.get("pair_scan_bar_size", "1 day")),
            pair_scan_top_n=pair_scan_top_n,
            min_correlation=min_correlation,
            entry_zscore=entry_zscore,
            watch_zscore=watch_zscore,
            exit_zscore=exit_zscore,
            stop_zscore=stop_zscore,
            opportunity_min_score=opportunity_min_score,
            loop_interval_seconds=loop_interval_seconds,
            allocation_eur=allocation_eur,
            max_open_pairs=max_open_pairs,
            max_new_trades_per_cycle=max_new_trades_per_cycle,
            enable_orders=enable_orders,
            enable_new_entries=enable_new_entries,
            only_trade_regular_hours=only_trade_regular_hours,
            market_timezone=market_timezone,
            trading_start_time=trading_start_time,
            trading_stop_new_entries_time=trading_stop_new_entries_time,
            max_daily_loss_eur=max_daily_loss_eur,
            reconnect_attempts=reconnect_attempts,
            max_retry_delay_seconds=max_retry_delay_seconds,
            max_consecutive_errors=max_consecutive_errors,
            entry_cooldown_minutes=entry_cooldown_minutes,
            state_path=state_path,
            heartbeat_path=heartbeat_path,
            audit_log_path=audit_log_path,
        )
