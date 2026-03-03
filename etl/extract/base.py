"""
etl/extract/base.py
Abstract base class that every extractor inherits from.
Enforces a consistent interface and handles shared concerns:
  - config + env loading
  - structured logging
  - output path management
  - saving to JSON and/or Parquet
"""

from __future__ import annotations

import json
import os
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from dotenv import load_dotenv

from etl.logger import get_logger

# load .env once at import time
load_dotenv()

# ── helpers ───────────────────────────────────────────────────────────────────
_CONFIG_PATH = Path("config/config.yaml")


def _load_config() -> dict:
    if not _CONFIG_PATH.exists():
        raise FileNotFoundError(f"Config not found: {_CONFIG_PATH}")
    with open(_CONFIG_PATH, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def _utc_slug() -> str:
    """Return a compact UTC timestamp string safe for filenames."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


# ── base class ────────────────────────────────────────────────────────────────
class BaseExtractor(ABC):
    """
    All extractors extend this class and implement `extract()`.

    Subclass pattern
    ----------------
    class RedditExtractor(BaseExtractor):
        source_name = "reddit"

        def extract(self) -> list[dict]:
            ...
            return records
    """

    source_name: str = "base"   # override in every subclass

    def __init__(self) -> None:
        self.config = _load_config()
        self.log = get_logger(f"etl.extract.{self.source_name}")
        self.source_cfg: dict = self.config.get(self.source_name, {})

        output_dir = Path(self.config.get("paths", {}).get("output_dir", "data/output"))
        self.output_dir = output_dir / self.source_name
        self.output_dir.mkdir(parents=True, exist_ok=True)

        raw_dir = Path(self.config.get("paths", {}).get("raw_dir", "data/raw"))
        self.raw_dir = raw_dir / self.source_name
        self.raw_dir.mkdir(parents=True, exist_ok=True)

        self._output_format: str = (
            self.config.get("output", {}).get("format", "both").lower()
        )
        self._parquet_compression: str = (
            self.config.get("output", {}).get("parquet_compression", "snappy")
        )

    # ── public interface ──────────────────────────────────────────────────────

    @abstractmethod
    def extract(self) -> list[dict[str, Any]]:
        """
        Pull data from the source and return a flat list of record dicts.
        Every record should contain at minimum:
            source      str   which extractor produced it
            extracted_at str  ISO-8601 UTC timestamp
        """

    def run(self) -> Path | None:
        """
        Orchestrate extract → save.  Called by main.py.
        Returns the path of the saved file (Parquet preferred, else JSON).
        """
        self.log.info(
            "extractor started",
            extra={"source": self.source_name},
        )
        try:
            records = self.extract()
        except Exception:
            self.log.error(
                "extraction failed",
                extra={"source": self.source_name},
                exc_info=True,
            )
            return None

        if not records:
            self.log.warning(
                "extractor returned no records",
                extra={"source": self.source_name},
            )
            return None

        self.log.info(
            "extraction complete",
            extra={"source": self.source_name, "record_count": len(records)},
        )
        return self._save(records)

    # ── persistence ───────────────────────────────────────────────────────────

    def _save(self, records: list[dict[str, Any]]) -> Path:
        slug = _utc_slug()
        df = pd.DataFrame(records)
        saved_path: Path | None = None

        if self._output_format in ("json", "both"):
            saved_path = self._save_json(records, slug)

        if self._output_format in ("parquet", "both"):
            saved_path = self._save_parquet(df, slug)

        return saved_path  # type: ignore[return-value]

    def _save_json(self, records: list[dict], slug: str) -> Path:
        path = self.output_dir / f"{self.source_name}_{slug}.json"
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(records, fh, indent=2, default=str, ensure_ascii=False)
        self.log.info(
            "saved JSON",
            extra={"source": self.source_name, "path": str(path), "records": len(records)},
        )
        return path

    def _save_parquet(self, df: pd.DataFrame, slug: str) -> Path:
        path = self.output_dir / f"{self.source_name}_{slug}.parquet"
        df.to_parquet(path, compression=self._parquet_compression, index=False)
        self.log.info(
            "saved Parquet",
            extra={
                "source": self.source_name,
                "path": str(path),
                "rows": len(df),
                "cols": len(df.columns),
                "compression": self._parquet_compression,
            },
        )
        return path

    # ── shared utility ────────────────────────────────────────────────────────

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat(timespec="seconds")

    def _env(self, key: str, required: bool = True) -> str:
        """Read an env var; raise clearly if required and missing."""
        value = os.getenv(key, "")
        if required and not value:
            raise EnvironmentError(
                f"[{self.source_name}] Required env var '{key}' is not set. "
                f"Add it to your .env file."
            )
        return value
