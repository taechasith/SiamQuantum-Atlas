from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from siamquantum.config import settings
from siamquantum.models import ServiceResult, SourceRaw

logger = logging.getLogger(__name__)

_SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"
_VIDEOS_URL = "https://www.googleapis.com/youtube/v3/videos"
_QUERY = "quantum"
_PAGE_SIZE = 50  # YouTube max per request


class _QuotaError(Exception):
    pass


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(_QuotaError),
    reraise=True,
)
async def _get(client: httpx.AsyncClient, url: str, params: dict[str, str]) -> dict:
    resp = await client.get(url, params=params, timeout=30.0)
    if resp.status_code == 429:
        raise _QuotaError("YouTube quota exceeded (429)")
    resp.raise_for_status()
    return resp.json()


async def _search_page(
    client: httpx.AsyncClient,
    year: int,
    page_token: str | None,
) -> tuple[list[dict], str | None]:
    """One search.list call. Returns (items, next_page_token)."""
    params: dict[str, str] = {
        "part": "snippet",
        "q": _QUERY,
        "type": "video",
        "regionCode": "TH",
        "relevanceLanguage": "th",
        "maxResults": str(_PAGE_SIZE),
        "publishedAfter": f"{year}-01-01T00:00:00Z",
        "publishedBefore": f"{year}-12-31T23:59:59Z",
        "key": settings.youtube_api_key,
    }
    if page_token:
        params["pageToken"] = page_token

    data = await _get(client, _SEARCH_URL, params)
    items = data.get("items") or []
    next_token: str | None = data.get("nextPageToken")
    return items, next_token


async def _fetch_stats(
    client: httpx.AsyncClient,
    video_ids: list[str],
) -> dict[str, dict]:
    """videos.list for statistics. Returns {video_id: stats_dict}."""
    if not video_ids:
        return {}
    params: dict[str, str] = {
        "part": "statistics,snippet",
        "id": ",".join(video_ids),
        "maxResults": str(len(video_ids)),
        "key": settings.youtube_api_key,
    }
    data = await _get(client, _VIDEOS_URL, params)
    result: dict[str, dict] = {}
    for item in data.get("items") or []:
        vid_id = item.get("id") or ""
        if vid_id:
            result[vid_id] = item.get("statistics") or {}
    return result


def _parse_published_year(published_at: str, fallback: int) -> int:
    try:
        return int(published_at[:4])
    except (ValueError, IndexError):
        return fallback


def _build_source(item: dict, stats: dict, year: int) -> SourceRaw | None:
    vid_id = (item.get("id") or {}).get("videoId") or ""
    if not vid_id:
        return None
    snippet = item.get("snippet") or {}
    title = snippet.get("title") or None
    description = snippet.get("description") or None
    raw_text = f"{title or ''}\n{description or ''}".strip() or None
    published_at = snippet.get("publishedAt") or ""
    pub_year = _parse_published_year(published_at, year)

    def _int(key: str) -> int | None:
        val = stats.get(key)
        return int(val) if val is not None else None

    return SourceRaw(
        platform="youtube",
        url=f"https://www.youtube.com/watch?v={vid_id}",
        title=title,
        raw_text=raw_text,
        published_year=pub_year,
        view_count=_int("viewCount"),
        like_count=_int("likeCount"),
        comment_count=_int("commentCount"),
    )


async def fetch_yearly(year: int) -> ServiceResult:
    """
    Fetch YouTube videos about quantum tech for Thailand for `year`.
    Two-pass: search.list (50+50 paginated) then videos.list for stats.
    Returns ServiceResult with data=list[dict] (SourceRaw schema).
    """
    try:
        async with httpx.AsyncClient() as client:
            # Pass 1: collect up to 100 videos via two search pages
            all_items: list[dict] = []
            page_token: str | None = None

            for page_num in range(2):  # max 2 pages = 100 results
                items, page_token = await _search_page(client, year, page_token)
                all_items.extend(items)
                logger.debug("YouTube search page %d: %d items", page_num + 1, len(items))
                if not page_token:
                    break
                await asyncio.sleep(1.0)  # brief gap between pages

            if not all_items:
                return ServiceResult(ok=True, data=[])

            # Pass 2: fetch statistics for all video IDs
            video_ids = [
                (item.get("id") or {}).get("videoId") or ""
                for item in all_items
            ]
            video_ids = [v for v in video_ids if v]

            # videos.list accepts up to 50 IDs; split into batches
            stats_map: dict[str, dict] = {}
            for i in range(0, len(video_ids), 50):
                batch = video_ids[i : i + 50]
                batch_stats = await _fetch_stats(client, batch)
                stats_map.update(batch_stats)
                if i + 50 < len(video_ids):
                    await asyncio.sleep(1.0)

        # Build SourceRaw records
        records: list[SourceRaw] = []
        seen: set[str] = set()
        for item in all_items:
            vid_id = (item.get("id") or {}).get("videoId") or ""
            per_video_stats = stats_map.get(vid_id, {})
            src = _build_source(item, per_video_stats, year)
            if src and src.url not in seen:
                seen.add(src.url)
                records.append(src)

        logger.info("YouTube fetch_yearly year=%d: %d records", year, len(records))
        return ServiceResult(ok=True, data=[r.model_dump() for r in records])

    except Exception as exc:
        return ServiceResult(ok=False, error=str(exc))
