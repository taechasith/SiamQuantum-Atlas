from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any

import httpx

from siamquantum_atlas.adapters.base import RawMediaRecord, SourceAdapter

logger = logging.getLogger(__name__)

_REDDIT_BASE = "https://www.reddit.com"
_HEADERS = {"User-Agent": "SiamQuantumAtlas/0.1 research-bot (non-commercial)"}

_SUBREDDIT_SEARCHES: list[dict[str, Any]] = [
    {"path": "/r/Thailand/search.json", "q": "quantum", "restrict_sr": "1", "sort": "top", "t": "year"},
    {"path": "/r/QuantumComputing/search.json", "q": "thailand OR thai", "restrict_sr": "1", "sort": "top", "t": "year"},
    {"path": "/r/Physics/search.json", "q": "quantum thailand OR thai", "restrict_sr": "1", "sort": "top", "t": "year"},
    {"path": "/r/science/search.json", "q": "quantum thailand", "restrict_sr": "1", "sort": "top", "t": "year"},
    {"path": "/search.json", "q": "quantum thailand", "sort": "top", "t": "year"},
    {"path": "/search.json", "q": "ควอนตัม OR quantum site:th", "sort": "new", "t": "month"},
    {"path": "/r/askscience/search.json", "q": "quantum thailand", "restrict_sr": "1", "sort": "top", "t": "year"},
]

_THAI_SIGNALS = {
    "thailand", "thai", "ไทย", "กรุงเทพ", "bangkok", "bkk",
    "เชียงใหม่", "chiangmai", "chiang mai", "ภูเก็ต", "phuket",
    "thainess", "สยาม", "สยาม", "mahidol", "chulalongkorn", "kmitl",
}


class RedditLiveAdapter(SourceAdapter):
    name = "reddit_live"

    def fetch(self, max_items: int = 300, **_: Any) -> list[RawMediaRecord]:
        seen_ids: set[str] = set()
        records: list[RawMediaRecord] = []

        for cfg in _SUBREDDIT_SEARCHES:
            if len(records) >= max_items:
                break
            batch = self._search(cfg, seen_ids)
            records.extend(batch)
            time.sleep(1.0)  # Reddit rate limit: 1 req/sec for unauthenticated

        logger.info("RedditLive: fetched %d records", len(records))
        return records[:max_items]

    def _search(self, cfg: dict[str, Any], seen: set[str]) -> list[RawMediaRecord]:
        path = cfg.pop("path", "/search.json")
        params: dict[str, Any] = {"limit": 100, **cfg}
        cfg["path"] = path  # restore for potential reuse

        try:
            resp = httpx.get(
                f"{_REDDIT_BASE}{path}",
                params=params,
                headers=_HEADERS,
                timeout=20,
                follow_redirects=True,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            logger.warning("Reddit search error (path=%r): %s", path, exc)
            return []

        records: list[RawMediaRecord] = []
        for child in data.get("data", {}).get("children", []):
            post = child.get("data", {})
            post_id = post.get("id", "")
            if not post_id or post_id in seen:
                continue

            title = post.get("title", "")
            selftext = post.get("selftext", "")
            combined_text = f"{title} {selftext}".lower()

            # Thailand relevance check
            thai_score = _compute_thai_relevance(title, selftext, post)
            if thai_score < 0.3:
                continue

            seen.add(post_id)

            url = f"https://www.reddit.com{post.get('permalink', '')}"
            canonical = url
            created_utc = post.get("created_utc")
            published_at = datetime.fromtimestamp(created_utc, tz=timezone.utc) if created_utc else None

            score = float(post.get("score", 0) or 0)
            num_comments = float(post.get("num_comments", 0) or 0)
            upvote_ratio = float(post.get("upvote_ratio", 0.5) or 0.5)

            records.append(
                RawMediaRecord(
                    adapter=self.name,
                    platform="reddit",
                    media_type="post",
                    title=title,
                    description=selftext[:500] if selftext else None,
                    full_text=selftext or None,
                    url=url,
                    canonical_url=canonical,
                    published_at=published_at,
                    language_detected=None,
                    domain="reddit.com",
                    raw_payload={
                        "reddit_id": post_id,
                        "subreddit": post.get("subreddit", ""),
                        "score": score,
                        "upvote_ratio": upvote_ratio,
                        "num_comments": num_comments,
                        "author_flair": post.get("author_flair_text"),
                        "link_flair": post.get("link_flair_text"),
                        "thai_relevance_score": thai_score,
                    },
                    likes=score,  # Reddit score ≈ net upvotes
                    comments=num_comments,
                    shares=None,
                    popularity_proxy=score * upvote_ratio,
                )
            )

        return records


def _compute_thai_relevance(title: str, body: str, post: dict) -> float:
    combined = f"{title} {body}".lower()

    # Explicit Thai signals in subreddit
    subreddit = post.get("subreddit", "").lower()
    if subreddit == "thailand":
        return 0.95

    # Author flair contains Thailand indicator
    flair = (post.get("author_flair_text") or "").lower()
    if any(s in flair for s in ["thai", "thailand", "ไทย"]):
        return 0.90

    # Keyword matching
    hits = sum(1 for sig in _THAI_SIGNALS if sig in combined)
    if hits >= 2:
        return 0.80
    if hits == 1:
        return 0.60

    # .th domain in URL
    url = post.get("url", "")
    if ".th/" in url or url.endswith(".th"):
        return 0.75

    return 0.15
