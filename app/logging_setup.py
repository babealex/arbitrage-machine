from __future__ import annotations

import json
import logging
from logging.handlers import RotatingFileHandler
from typing import Any

from app.config import Settings


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "time": self.formatTime(record, self.datefmt),
        }
        if hasattr(record, "event"):
            payload["event"] = record.event
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def setup_logging(settings: Settings) -> logging.Logger:
    settings.ensure_directories()
    logger = logging.getLogger("arbitrage_machine")
    logger.setLevel(getattr(logging, settings.log_level.upper(), logging.INFO))
    logger.handlers.clear()
    file_handler = RotatingFileHandler(settings.log_path, maxBytes=2_000_000, backupCount=5)
    file_handler.setFormatter(JsonFormatter())
    logger.addHandler(file_handler)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(JsonFormatter())
    logger.addHandler(stream_handler)
    return logger
