from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any

import httpx

from siamquantum_atlas.adapters.base import RawMediaRecord, SourceAdapter
from siamquantum_atlas.settings import settings

logger = logging.getLogger(__name__)

_SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"
_VIDEOS_URL = "https://www.googleapis.com/youtube/v3/videos"
_QUANTUM_QUERIES = [
    "ควอนตัม",
    "คอมพิวเตอร์ควอนตัม",
    "quantum computing Thailand",
    "quantum physics Thailand",
    "ฟิสิกส์ควอนตัม",
]
_MAX_PER_QUERY = 5  # pages × 50 = 250 results per query; 5 queries × 250 = 1250 cap
_PAGE_SIZE = 50


class YouTubeLiveAdapter(SourceAdapter):
    name = "youtube_live"

    def __init__(self) -> None:
        self.api_key = settings.youtube_api_key

    def fetch(self, max_items: int = 500, **_: Any) -> list[RawMediaRecord]:
        if not self.api_key:
            logger.warning("No YouTube API key — skipping live fetch")
            return []

        video_ids: list[str] = []
        snippet_cache: dict[str, dict] = {}

        for query in _QUANTUM_QUERIES:
            if len(video_ids) >= max_items:
                break
            ids, snippets = self._search_query(query, max_pages=_MAX_PER_QUERY)
            for vid_id in ids:
                if vid_id not in snippet_cache:
                    snippet_cache[vid_id] = snippets[vid_id]
                    video_ids.append(vid_id)
            if len(video_ids) >= max_items:
                break

        video_ids = video_ids[:max_items]
        stats = self._batch_stats(video_ids)
        records: list[RawMediaRecord] = []

        for vid_id in video_ids:
            snippet = snippet_cache.get(vid_id, {})
            stat = stats.get(vid_id, {})
            title = snippet.get("title", "")
            description = snippet.get("description", "")
            channel = snippet.get("channelTitle", "")
            published_raw = snippet.get("publishedAt", "")
            published_at: datetime | None = None
            if published_raw:
                try:
                    published_at = datetime.fromisoformat(published_raw.replace("Z", "+00:00"))
                except ValueError:
                    pass

            views = _safe_float(stat.get("viewCount"))
            likes = _safe_float(stat.get("likeCount"))
            comments = _safe_float(stat.get("commentCount"))
            url = f"https://www.youtube.com/watch?v={vid_id}"

            records.append(
                RawMediaRecord(
                    adapter=self.name,
                    platform="youtube",
                    media_type="video",
                    title=title,
                    description=description,
                    full_text=None,
                    url=url,
                    canonical_url=url,
                    published_at=published_at,
                    language_detected=None,
                    domain="youtube.com",
                    raw_payload={
                        "video_id": vid_id,
                        "channel": channel,
                        "snippet": snippet,
                        "statistics": stat,
                    },
                    views=views,
                    likes=likes,
                    comments=comments,
                    shares=None,
                )
            )

        logger.info("YouTubeLive: fetched %d records", len(records))
        return records

    def _search_query(self, query: str, max_pages: int = 5) -> tuple[list[str], dict[str, dict]]:
        ids: list[str] = []
        snippets: dict[str, dict] = {}
        next_token: str | None = None

        for page in range(max_pages):
            params: dict[str, Any] = {
                "part": "snippet",
                "q": query,
                "type": "video",
                "maxResults": _PAGE_SIZE,
                "regionCode": "TH",
                "relevanceLanguage": "th",
                "key": self.api_key,
            }
            if next_token:
                params["pageToken"] = next_token

            try:
                resp = httpx.get(_SEARCH_URL, params=params, timeout=15)
                if resp.status_code == 403:
                    logger.error("YouTube search 403: %s", _describe_youtube_403(resp))
                    break
                resp.raise_for_status()
                data = resp.json()
            except Exception as exc:
                logger.warning("YouTube search error (query=%r, page=%d): %s", query, page, exc)
                break

            for item in data.get("items", []):
                vid_id = item.get("id", {}).get("videoId")
                if vid_id:
                    ids.append(vid_id)
                    snippets[vid_id] = item.get("snippet", {})

            next_token = data.get("nextPageToken")
            if not next_token:
                break
            time.sleep(0.2)

        return ids, snippets

    def _batch_stats(self, video_ids: list[str]) -> dict[str, dict]:
        stats: dict[str, dict] = {}
        batch_size = 50
        for i in range(0, len(video_ids), batch_size):
            batch = video_ids[i : i + batch_size]
            params = {
                "part": "statistics",
                "id": ",".join(batch),
                "key": self.api_key,
            }
            try:
                resp = httpx.get(_VIDEOS_URL, params=params, timeout=15)
                if resp.status_code == 403:
                    logger.error("YouTube stats 403: %s", _describe_youtube_403(resp))
                    break
                resp.raise_for_status()
                for item in resp.json().get("items", []):
                    stats[item["id"]] = item.get("statistics", {})
            except Exception as exc:
                logger.warning("YouTube stats error (batch %d): %s", i, exc)
            time.sleep(0.1)
        return stats


def _safe_float(val: Any) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _describe_youtube_403(resp: httpx.Response) -> str:
    try:
        payload = resp.json()
    except ValueError:
        return f"HTTP 403 with non-JSON response: {resp.text[:200]}"

    error = payload.get("error") or {}
    errors = error.get("errors") or []
    reason = errors[0].get("reason") if errors else None
    message = error.get("message") or resp.text[:200]

    hints = {
        "accessNotConfigured": (
            "YouTube Data API v3 is not enabled for the Google Cloud project behind this key."
        ),
        "forbidden": (
            "The API key is restricted in a way that blocks this request, or the API is disabled for the key's project."
        ),
        "quotaExceeded": "The project's YouTube Data API quota is exhausted.",
        "dailyLimitExceeded": "The project's daily API limit is exhausted.",
        "keyInvalid": "The API key is invalid.",
        "ipRefererBlocked": "The key's application restrictions are blocking this machine or referrer.",
        "usageLimits.userRateLimitExceeded": "The key hit a per-user rate limit.",
    }
    hint = hints.get(reason, "Inspect the Google Cloud Console error details for this key and project.")
    return f"reason={reason or 'unknown'}; message={message}; hint={hint}"
