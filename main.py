"""
main.py
Entry point for the Systemic Zone ETL pipeline.
Run with:  python main.py  (or specific extractors via flags)

Usage
-----
python main.py                        # run all extractors
python main.py --extractors reddit    # run one
python main.py --extractors reddit arxiv youtube csv   # run several
"""

from __future__ import annotations

import argparse
import sys

from dotenv import load_dotenv

load_dotenv()

from etl.logger import get_logger
from etl.extract.reddit_extractor import RedditExtractor
from etl.extract.arxiv_extractor import ArxivExtractor
from etl.extract.youtube_extractor import YouTubeExtractor
from etl.extract.csv_extractor import CsvExtractor

log = get_logger("systemic_zone.main")

EXTRACTORS = {
    "reddit":  RedditExtractor,
    "arxiv":   ArxivExtractor,
    "youtube": YouTubeExtractor,
    "csv":     CsvExtractor,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Systemic Zone ETL pipeline")
    parser.add_argument(
        "--extractors",
        nargs="*",
        choices=list(EXTRACTORS.keys()),
        default=list(EXTRACTORS.keys()),
        help="Which extractors to run (default: all)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    selected: list[str] = args.extractors

    log.info("pipeline started", extra={"extractors": selected})

    results: dict[str, str] = {}

    for name in selected:
        cls = EXTRACTORS[name]
        log.info("running extractor", extra={"extractor": name})
        try:
            extractor = cls()
            output_path = extractor.run()
            results[name] = str(output_path) if output_path else "no output"
        except Exception:
            log.error("extractor crashed", extra={"extractor": name}, exc_info=True)
            results[name] = "ERROR"

    log.info("pipeline complete", extra={"results": results})

    # print a human-readable summary
    print("\n── Systemic Zone ETL Run Summary ────────────────────")
    for name, path in results.items():
        status = "✓" if path != "ERROR" else "✗"
        print(f"  {status}  {name:<10}  →  {path}")
    print("─────────────────────────────────────────────────────\n")


if __name__ == "__main__":
    main()
