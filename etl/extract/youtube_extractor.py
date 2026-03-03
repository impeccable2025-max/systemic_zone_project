"""
etl/extract/youtube_extractor.py
Searches YouTube for videos matching configured queries, extracts metadata,
and optionally fetches transcripts (no quota cost) via youtube-transcript-api.

Required env vars (.env):
    YOUTUBE_API_KEY   (YouTube Data API v3 key)

Config keys (config/config.yaml → youtube):
    search_queries      list of search strings
    channel_ids         optional list of channel IDs to restrict results
    max_results         int   results per query (max 50 per API call)
    fetch_transcripts   bool  fetch transcript via youtube-transcript-api
    transcript_languages list  preferred language codes e.g. ["en"]
"""

from __future__ import annotations

from typing import Any

import requests

from etl.extract.base import BaseExtractor

_YT_SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"
_YT_VIDEOS_URL = "https://www.googleapis.com/youtube/v3/videos"


class YouTubeExtractor(BaseExtractor):
    source_name = "youtube"

    # ── public ────────────────────────────────────────────────────────────────

    def extract(self) -> list[dict[str, Any]]:
        api_key = self._env("YOUTUBE_API_KEY")
        cfg = self.source_cfg

        queries: list[str] = cfg.get("search_queries", [])
        channel_ids: list[str] = cfg.get("channel_ids", [])
        max_results: int = min(int(cfg.get("max_results", 25)), 50)
        fetch_transcripts: bool = bool(cfg.get("fetch_transcripts", True))
        transcript_languages: list[str] = cfg.get("transcript_languages", ["en"])

        if not queries and not channel_ids:
            self.log.warning("no queries or channel_ids configured", extra={"source": self.source_name})
            return []

        video_ids: list[str] = []
        seen: set[str] = set()

        for query in queries:
            self.log.info(
                "searching youtube",
                extra={"source": self.source_name, "query": query, "max_results": max_results},
            )
            try:
                ids = self._search_videos(api_key, query, max_results, channel_ids)
                for vid_id in ids:
                    if vid_id not in seen:
                        seen.add(vid_id)
                        video_ids.append(vid_id)
            except Exception:
                self.log.error(
                    "youtube search failed",
                    extra={"source": self.source_name, "query": query},
                    exc_info=True,
                )
                continue

        if not video_ids:
            return []

        # fetch full metadata in batches of 50 (API limit)
        records: list[dict[str, Any]] = []
        for batch_start in range(0, len(video_ids), 50):
            batch = video_ids[batch_start: batch_start + 50]
            try:
                metadata_list = self._fetch_video_metadata(api_key, batch)
                records.extend(metadata_list)
            except Exception:
                self.log.error(
                    "video metadata fetch failed",
                    extra={"source": self.source_name, "batch_start": batch_start},
                    exc_info=True,
                )

        # optionally enrich with transcripts
        if fetch_transcripts:
            records = self._attach_transcripts(records, transcript_languages)

        self.log.info(
            "youtube extraction complete",
            extra={"source": self.source_name, "total_records": len(records)},
        )
        return records

    # ── private ───────────────────────────────────────────────────────────────

    def _search_videos(
        self, api_key: str, query: str, max_results: int, channel_ids: list[str]
    ) -> list[str]:
        params: dict[str, Any] = {
            "part":       "id",
            "q":          query,
            "type":       "video",
            "maxResults": max_results,
            "key":        api_key,
        }
        if channel_ids:
            params["channelId"] = channel_ids[0]   # API accepts one at a time

        resp = requests.get(_YT_SEARCH_URL, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        return [item["id"]["videoId"] for item in data.get("items", []) if "videoId" in item.get("id", {})]

    def _fetch_video_metadata(self, api_key: str, video_ids: list[str]) -> list[dict[str, Any]]:
        params = {
            "part":  "snippet,contentDetails,statistics",
            "id":    ",".join(video_ids),
            "key":   api_key,
        }
        resp = requests.get(_YT_VIDEOS_URL, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        records = []
        for item in data.get("items", []):
            snippet = item.get("snippet", {})
            stats   = item.get("statistics", {})
            details = item.get("contentDetails", {})
            records.append({
                "source":          self.source_name,
                "extracted_at":    self._now_iso(),
                "video_id":        item["id"],
                "url":             f"https://www.youtube.com/watch?v={item['id']}",
                "title":           snippet.get("title"),
                "description":     snippet.get("description"),
                "channel_id":      snippet.get("channelId"),
                "channel_title":   snippet.get("channelTitle"),
                "published_at":    snippet.get("publishedAt"),
                "tags":            snippet.get("tags", []),
                "category_id":     snippet.get("categoryId"),
                "duration":        details.get("duration"),   # ISO 8601 e.g. PT4M13S
                "view_count":      int(stats.get("viewCount", 0) or 0),
                "like_count":      int(stats.get("likeCount", 0) or 0),
                "comment_count":   int(stats.get("commentCount", 0) or 0),
                "transcript":      None,   # filled in below if enabled
            })
        return records

    def _attach_transcripts(
        self, records: list[dict[str, Any]], languages: list[str]
    ) -> list[dict[str, Any]]:
        from youtube_transcript_api import (   # already in conda env
            YouTubeTranscriptApi,
            TranscriptsDisabled,
            NoTranscriptFound,
        )

        for record in records:
            vid_id = record["video_id"]
            try:
                transcript_list = YouTubeTranscriptApi.get_transcript(
                    vid_id, languages=languages
                )
                # join all segments into a single clean string
                record["transcript"] = " ".join(
                    seg["text"].strip() for seg in transcript_list
                )
            except (TranscriptsDisabled, NoTranscriptFound):
                self.log.debug(
                    "no transcript available",
                    extra={"source": self.source_name, "video_id": vid_id},
                )
            except Exception:
                self.log.warning(
                    "transcript fetch error",
                    extra={"source": self.source_name, "video_id": vid_id},
                    exc_info=True,
                )
        return records
