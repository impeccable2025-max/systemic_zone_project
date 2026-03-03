"""
etl/extract/arxiv_extractor.py
Searches arXiv for papers matching configured queries and extracts
structured metadata (title, authors, abstract, categories, PDF link, etc.).

No API key required — the arXiv API is open.

Config keys (config/config.yaml → arxiv):
    queries         list of search strings
    max_results     int   results per query
    sort_by         submittedDate | relevance | lastUpdatedDate
    sort_order      ascending | descending
"""

from __future__ import annotations

from typing import Any

from etl.extract.base import BaseExtractor


class ArxivExtractor(BaseExtractor):
    source_name = "arxiv"

    # ── public ────────────────────────────────────────────────────────────────

    def extract(self) -> list[dict[str, Any]]:
        import arxiv  # imported here for clear missing-package error

        cfg = self.source_cfg
        queries: list[str] = cfg.get("queries", [])
        max_results: int = int(cfg.get("max_results", 50))
        sort_by_str: str = cfg.get("sort_by", "submittedDate")
        sort_order_str: str = cfg.get("sort_order", "descending")

        if not queries:
            self.log.warning("no arxiv queries configured", extra={"source": self.source_name})
            return []

        sort_by = self._parse_sort_by(arxiv, sort_by_str)
        sort_order = self._parse_sort_order(arxiv, sort_order_str)

        records: list[dict[str, Any]] = []
        seen_ids: set[str] = set()   # deduplicate across overlapping queries

        client = arxiv.Client()

        for query in queries:
            self.log.info(
                "searching arxiv",
                extra={
                    "source": self.source_name,
                    "query": query,
                    "max_results": max_results,
                },
            )
            try:
                search = arxiv.Search(
                    query=query,
                    max_results=max_results,
                    sort_by=sort_by,
                    sort_order=sort_order,
                )
                for result in client.results(search):
                    paper_id = result.entry_id
                    if paper_id in seen_ids:
                        continue
                    seen_ids.add(paper_id)
                    records.append(self._result_to_record(result, query))

            except Exception:
                self.log.error(
                    "arxiv query failed",
                    extra={"source": self.source_name, "query": query},
                    exc_info=True,
                )
                continue

        self.log.info(
            "arxiv extraction complete",
            extra={"source": self.source_name, "total_records": len(records)},
        )
        return records

    # ── private ───────────────────────────────────────────────────────────────

    def _result_to_record(self, result, query: str) -> dict[str, Any]:
        return {
            "source":            self.source_name,
            "extracted_at":      self._now_iso(),
            "query":             query,
            "entry_id":          result.entry_id,
            "arxiv_id":          result.get_short_id(),
            "title":             result.title.strip(),
            "authors":           [str(a) for a in result.authors],
            "abstract":          result.summary.strip(),
            "categories":        result.categories,
            "primary_category":  result.primary_category,
            "published":         result.published.isoformat() if result.published else None,
            "updated":           result.updated.isoformat() if result.updated else None,
            "pdf_url":           result.pdf_url,
            "journal_ref":       result.journal_ref,
            "doi":               result.doi,
            "comment":           result.comment,
        }

    @staticmethod
    def _parse_sort_by(arxiv, value: str):
        mapping = {
            "submitteddate": arxiv.SortCriterion.SubmittedDate,
            "relevance":     arxiv.SortCriterion.Relevance,
            "lastupdateddate": arxiv.SortCriterion.LastUpdatedDate,
        }
        return mapping.get(value.lower(), arxiv.SortCriterion.SubmittedDate)

    @staticmethod
    def _parse_sort_order(arxiv, value: str):
        if value.lower() == "ascending":
            return arxiv.SortOrder.Ascending
        return arxiv.SortOrder.Descending
