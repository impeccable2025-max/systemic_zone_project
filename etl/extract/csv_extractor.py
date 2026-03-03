"""
etl/extract/csv_extractor.py
Watches configured input directories for CSV and TSV files,
reads them with pandas, adds provenance metadata, and passes
them into the standard output pipeline (JSON + Parquet).

No API key required.

Config keys (config/config.yaml → csv):
    watch_dirs      list of directory paths to scan
    file_patterns   glob patterns  e.g. ["*.csv", "*.tsv"]
    delimiter       auto | "," | "\t" | "|"
    encoding        default utf-8
    infer_dtypes    bool  let pandas infer column types
    skip_rows       int   rows to skip at top of file
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import pandas as pd

from etl.extract.base import BaseExtractor


class CsvExtractor(BaseExtractor):
    source_name = "csv"

    # ── public ────────────────────────────────────────────────────────────────

    def extract(self) -> list[dict[str, Any]]:
        cfg = self.source_cfg
        watch_dirs: list[str] = cfg.get("watch_dirs", ["data/input/csv"])
        file_patterns: list[str] = cfg.get("file_patterns", ["*.csv", "*.tsv"])
        delimiter: str = cfg.get("delimiter", "auto")
        encoding: str = cfg.get("encoding", "utf-8")
        infer_dtypes: bool = bool(cfg.get("infer_dtypes", True))
        skip_rows: int = int(cfg.get("skip_rows", 0))

        # discover files
        files: list[Path] = []
        for dir_str in watch_dirs:
            dir_path = Path(dir_str)
            if not dir_path.exists():
                self.log.debug(
                    "watch_dir does not exist, skipping",
                    extra={"source": self.source_name, "dir": str(dir_path)},
                )
                continue
            for pattern in file_patterns:
                files.extend(sorted(dir_path.glob(pattern)))

        if not files:
            self.log.warning(
                "no CSV/TSV files found in watch_dirs",
                extra={"source": self.source_name, "watch_dirs": watch_dirs},
            )
            return []

        records: list[dict[str, Any]] = []

        for file_path in files:
            self.log.info(
                "reading file",
                extra={"source": self.source_name, "file": str(file_path)},
            )
            try:
                df = self._read_file(
                    file_path,
                    delimiter=delimiter,
                    encoding=encoding,
                    infer_dtypes=infer_dtypes,
                    skip_rows=skip_rows,
                )
                file_records = self._df_to_records(df, file_path)
                records.extend(file_records)
                self.log.info(
                    "file read complete",
                    extra={
                        "source":  self.source_name,
                        "file":    str(file_path),
                        "rows":    len(df),
                        "columns": list(df.columns),
                    },
                )
            except Exception:
                self.log.error(
                    "failed to read file",
                    extra={"source": self.source_name, "file": str(file_path)},
                    exc_info=True,
                )
                continue

        self.log.info(
            "csv extraction complete",
            extra={"source": self.source_name, "total_records": len(records), "files_read": len(files)},
        )
        return records

    # ── private ───────────────────────────────────────────────────────────────

    def _read_file(
        self,
        path: Path,
        delimiter: str,
        encoding: str,
        infer_dtypes: bool,
        skip_rows: int,
    ) -> pd.DataFrame:
        kwargs: dict[str, Any] = {
            "encoding":  encoding,
            "skiprows":  skip_rows,
        }

        # dtype control
        if not infer_dtypes:
            kwargs["dtype"] = str

        # delimiter detection
        if delimiter == "auto":
            sep = self._sniff_delimiter(path, encoding)
        else:
            sep = delimiter

        if path.suffix.lower() in (".tsv",) and delimiter == "auto":
            sep = "\t"

        kwargs["sep"] = sep
        return pd.read_csv(path, **kwargs)

    def _df_to_records(self, df: pd.DataFrame, path: Path) -> list[dict[str, Any]]:
        """Convert a DataFrame to a list of dicts with provenance fields."""
        file_hash = self._file_hash(path)
        provenance = {
            "source":       self.source_name,
            "extracted_at": self._now_iso(),
            "file_name":    path.name,
            "file_path":    str(path.resolve()),
            "file_hash_md5": file_hash,
            "total_rows":   len(df),
        }
        records = []
        for row in df.to_dict(orient="records"):
            record = {**provenance, **row}
            records.append(record)
        return records

    @staticmethod
    def _sniff_delimiter(path: Path, encoding: str) -> str:
        """Peek at the first line and guess the delimiter."""
        import csv
        try:
            with open(path, encoding=encoding, errors="replace") as fh:
                sample = fh.read(4096)
            dialect = csv.Sniffer().sniff(sample, delimiters=",\t|;")
            return dialect.delimiter
        except Exception:
            return ","   # safe fallback

    @staticmethod
    def _file_hash(path: Path) -> str:
        """MD5 hash of file contents — useful for change detection."""
        h = hashlib.md5()
        with open(path, "rb") as fh:
            for chunk in iter(lambda: fh.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()
