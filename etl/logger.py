"""
logger.py
Structured JSON logger for the Systemic Zone ETL project.
Every log record is emitted as a single-line JSON object so downstream
tools (Splunk, Loki, jq, pandas) can parse it without regex.
"""

import json
import logging
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


# ── constants ────────────────────────────────────────────────────────────────
LOG_DIR = Path("logs")
LOG_FILE = LOG_DIR / "systemic_zone.jsonl"   # .jsonl = one JSON object per line


# ── JSON formatter ────────────────────────────────────────────────────────────
class JsonFormatter(logging.Formatter):
    """
    Converts every LogRecord into a flat JSON object.

    Fields always present:
        ts          ISO-8601 UTC timestamp
        level       DEBUG / INFO / WARNING / ERROR / CRITICAL
        logger      dotted logger name  (e.g. "etl.extract.reddit")
        msg         the formatted log message
        module      source file stem
        func        calling function name
        line        line number

    Optional fields (only when relevant):
        exc         formatted exception traceback (on exceptions)
        extra.*     any keyword args passed to logger.info(..., extra={...})
    """

    def format(self, record: logging.LogRecord) -> str:
        record_dict: dict[str, Any] = {
            "ts":     datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "level":  record.levelname,
            "logger": record.name,
            "msg":    record.getMessage(),
            "module": record.module,
            "func":   record.funcName,
            "line":   record.lineno,
        }

        # attach exception info when present
        if record.exc_info and record.exc_info[0] is not None:
            record_dict["exc"] = "".join(
                traceback.format_exception(*record.exc_info)
            ).strip()

        # attach any extra fields the caller injected
        _SKIP = {
            "msg", "args", "created", "relativeCreated", "msecs",
            "pathname", "filename", "module", "funcName", "lineno",
            "levelno", "levelname", "name", "thread", "threadName",
            "processName", "process", "exc_info", "exc_text",
            "stack_info", "message",
        }
        for key, value in record.__dict__.items():
            if key not in _SKIP and not key.startswith("_"):
                record_dict[f"extra.{key}"] = value

        return json.dumps(record_dict, default=str, ensure_ascii=False)


# ── factory ───────────────────────────────────────────────────────────────────
def get_logger(name: str, level: int = logging.DEBUG) -> logging.Logger:
    """
    Return (or create) a structured-JSON logger.

    Usage
    -----
    from etl.logger import get_logger
    log = get_logger(__name__)

    log.info("extractor started", extra={"source": "reddit", "subreddit": "MachineLearning"})
    log.error("request failed", extra={"status_code": 429}, exc_info=True)

    Parameters
    ----------
    name  : dotted module name, pass __name__ from calling module
    level : minimum severity to capture (default DEBUG)

    Returns
    -------
    logging.Logger configured with JSON handlers
    """
    logger = logging.getLogger(name)

    # avoid adding duplicate handlers if get_logger is called more than once
    if logger.handlers:
        return logger

    logger.setLevel(level)
    logger.propagate = False   # don't bubble up to root logger

    formatter = JsonFormatter()

    # ── handler 1: stdout (INFO and above) ───────────────────────────────────
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # ── handler 2: rotating file (DEBUG and above) ────────────────────────────
    LOG_DIR.mkdir(exist_ok=True)
    from logging.handlers import RotatingFileHandler
    file_handler = RotatingFileHandler(
        LOG_FILE,
        maxBytes=5 * 1024 * 1024,   # 5 MB per file
        backupCount=5,               # keep last 5 rotated files
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


# ── module-level convenience logger ──────────────────────────────────────────
# Import this directly when you just need a quick log without naming a logger:
#   from etl.logger import log
#   log.info("hello")
log = get_logger("systemic_zone")
