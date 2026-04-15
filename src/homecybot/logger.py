from __future__ import annotations

from datetime import datetime
from typing import Any


class BotLogger:
    def __init__(self, enabled: bool = True):
        self.enabled = enabled

    def emit(self, level: str, scope: str, message: str, **fields: Any) -> None:
        if not self.enabled:
            return

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        extras = " | ".join(f"{key}={value}" for key, value in fields.items())
        suffix = f" | {extras}" if extras else ""
        print(f"{timestamp} | {level.upper():<5} | {scope.upper():<8} | {message}{suffix}", flush=True)

    def info(self, scope: str, message: str, **fields: Any) -> None:
        self.emit("INFO", scope, message, **fields)

    def warning(self, scope: str, message: str, **fields: Any) -> None:
        self.emit("WARN", scope, message, **fields)

    def error(self, scope: str, message: str, **fields: Any) -> None:
        self.emit("ERROR", scope, message, **fields)
