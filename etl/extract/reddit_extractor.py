"""
etl/extract/reddit_extractor.py
Extracts posts (and optionally top-level comments) from configured
subreddits using the PRAW library.

Required env vars (.env):
    REDDIT_CLIENT_ID
    REDDIT_CLIENT_SECRET
    REDDIT_USER_AGENT

Config keys (config/config.yaml → reddit):
    subreddits      list of subreddit names
    post_limit      int   posts per subreddit
    sort            hot | new | top | rising
    time_filter     hour | day | week | month | year | all  (top only)
    include_comments bool
    comment_limit   int   top-level comments per post
"""

from __future__ import annotations

from typing import Any

from etl.extract.base import BaseExtractor


class RedditExtractor(BaseExtractor):
    source_name = "reddit"

    # ── public ────────────────────────────────────────────────────────────────

    def extract(self) -> list[dict[str, Any]]:
        import praw  # imported here so missing package gives a clear error

        reddit = praw.Reddit(
            client_id=self._env("REDDIT_CLIENT_ID"),
            client_secret=self._env("REDDIT_CLIENT_SECRET"),
            user_agent=self._env("REDDIT_USER_AGENT"),
        )

        cfg = self.source_cfg
        subreddits: list[str] = cfg.get("subreddits", [])
        limit: int = int(cfg.get("post_limit", 50))
        sort: str = cfg.get("sort", "hot")
        time_filter: str = cfg.get("time_filter", "week")
        include_comments: bool = bool(cfg.get("include_comments", False))
        comment_limit: int = int(cfg.get("comment_limit", 20))

        if not subreddits:
            self.log.warning("no subreddits configured", extra={"source": self.source_name})
            return []

        records: list[dict[str, Any]] = []

        for sub_name in subreddits:
            self.log.info(
                "fetching subreddit",
                extra={"source": self.source_name, "subreddit": sub_name, "sort": sort, "limit": limit},
            )
            try:
                sub = reddit.subreddit(sub_name)
                posts = self._get_posts(sub, sort, limit, time_filter)

                for post in posts:
                    record = self._post_to_record(post, sub_name)

                    if include_comments:
                        record["comments"] = self._get_comments(post, comment_limit)

                    records.append(record)

            except Exception:
                self.log.error(
                    "failed to fetch subreddit",
                    extra={"source": self.source_name, "subreddit": sub_name},
                    exc_info=True,
                )
                continue   # don't abort the whole run for one bad subreddit

        self.log.info(
            "reddit extraction complete",
            extra={"source": self.source_name, "total_records": len(records)},
        )
        return records

    # ── private ───────────────────────────────────────────────────────────────

    def _get_posts(self, sub, sort: str, limit: int, time_filter: str):
        """Return an iterable of Submission objects according to sort method."""
        if sort == "hot":
            return sub.hot(limit=limit)
        if sort == "new":
            return sub.new(limit=limit)
        if sort == "rising":
            return sub.rising(limit=limit)
        if sort == "top":
            return sub.top(limit=limit, time_filter=time_filter)
        self.log.warning(
            "unknown sort, defaulting to hot",
            extra={"source": self.source_name, "sort": sort},
        )
        return sub.hot(limit=limit)

    def _post_to_record(self, post, subreddit: str) -> dict[str, Any]:
        return {
            "source":          self.source_name,
            "extracted_at":    self._now_iso(),
            "id":              post.id,
            "subreddit":       subreddit,
            "title":           post.title,
            "author":          str(post.author) if post.author else "[deleted]",
            "score":           post.score,
            "upvote_ratio":    post.upvote_ratio,
            "num_comments":    post.num_comments,
            "url":             post.url,
            "permalink":       f"https://reddit.com{post.permalink}",
            "selftext":        post.selftext,
            "is_self":         post.is_self,
            "created_utc":     self._ts(post.created_utc),
            "flair":           post.link_flair_text,
            "over_18":         post.over_18,
        }

    def _get_comments(self, post, limit: int) -> list[dict[str, Any]]:
        """Fetch top-level comments only (no deep tree traversal)."""
        try:
            post.comments.replace_more(limit=0)   # flatten MoreComments objects
            comments = []
            for comment in list(post.comments)[:limit]:
                comments.append({
                    "id":          comment.id,
                    "author":      str(comment.author) if comment.author else "[deleted]",
                    "body":        comment.body,
                    "score":       comment.score,
                    "created_utc": self._ts(comment.created_utc),
                })
            return comments
        except Exception:
            self.log.warning(
                "could not fetch comments",
                extra={"source": self.source_name, "post_id": post.id},
                exc_info=True,
            )
            return []

    @staticmethod
    def _ts(epoch: float) -> str:
        from datetime import datetime, timezone
        return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat(timespec="seconds")
