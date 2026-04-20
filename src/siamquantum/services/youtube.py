from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, cast

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from siamquantum.config import settings
from siamquantum.models import ServiceResult, SourceRaw

logger = logging.getLogger(__name__)

_SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"
_VIDEOS_URL = "https://www.googleapis.com/youtube/v3/videos"
_CHANNELS_URL = "https://www.googleapis.com/youtube/v3/channels"
_PAGE_SIZE = 50  # YouTube max per request

# TI-4.2: Thai-first query strategy (Q2 + Q3 from probe)
_QUERIES = [
    "ควอนตัม",                                          # Q2 — 78% TH channel hit rate
    "คอมพิวเตอร์ควอนตัม OR ฟิสิกส์ควอนตัม",           # Q3 — vocab expansion
]
_TH_COUNTRIES = {"TH"}
_TH_LANGUAGES = {"th", "th-TH"}


class _QuotaError(Exception):
    pass


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(_QuotaError),
    reraise=True,
)
async def _get(client: httpx.AsyncClient, url: str, params: dict[str, str]) -> dict[str, Any]:
    resp = await client.get(url, params=params, timeout=30.0)
    if resp.status_code == 429:
        raise _QuotaError("YouTube quota exceeded (429)")
    resp.raise_for_status()
    return cast(dict[str, Any], resp.json())


async def _search_page(
    client: httpx.AsyncClient,
    query: str,
    year: int,
    page_token: str | None,
) -> tuple[list[dict[str, Any]], str | None]:
    params: dict[str, str] = {
        "part": "snippet",
        "q": query,
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
    return data.get("items") or [], data.get("nextPageToken")


async def _fetch_stats(
    client: httpx.AsyncClient,
    video_ids: list[str],
) -> dict[str, dict[str, Any]]:
    if not video_ids:
        return {}
    params: dict[str, str] = {
        "part": "statistics,snippet",
        "id": ",".join(video_ids),
        "maxResults": str(len(video_ids)),
        "key": settings.youtube_api_key,
    }
    data = await _get(client, _VIDEOS_URL, params)
    result: dict[str, dict[str, Any]] = {}
    for item in data.get("items") or []:
        vid_id = item.get("id") or ""
        if vid_id:
            result[vid_id] = item.get("statistics") or {}
    return result


async def _fetch_channel_info(
    client: httpx.AsyncClient,
    channel_ids: list[str],
) -> dict[str, dict[str, Any]]:
    """channels.list for country + language metadata. Returns {channel_id: info}."""
    if not channel_ids:
        return {}
    result: dict[str, dict[str, Any]] = {}
    for i in range(0, len(channel_ids), 50):
        batch = channel_ids[i : i + 50]
        params: dict[str, str] = {
            "part": "snippet,brandingSettings",
            "id": ",".join(batch),
            "key": settings.youtube_api_key,
        }
        try:
            data = await _get(client, _CHANNELS_URL, params)
        except Exception as exc:
            logger.warning("channels.list error: %s", exc)
            continue
        for item in data.get("items") or []:
            cid = item.get("id") or ""
            if not cid:
                continue
            snippet = item.get("snippet") or {}
            branding = (item.get("brandingSettings") or {}).get("channel") or {}
            result[cid] = {
                "country": snippet.get("country") or branding.get("country"),
                "defaultLanguage": snippet.get("defaultLanguage"),
            }
        if i + 50 < len(channel_ids):
            await asyncio.sleep(1.0)
    return result


def _is_thai_channel(info: dict[str, Any] | None) -> bool:
    """True if channel.country=TH OR channel.defaultLanguage in th/th-TH."""
    if info is None:
        return False
    country = info.get("country") or ""
    lang = info.get("defaultLanguage") or ""
    return country in _TH_COUNTRIES or lang in _TH_LANGUAGES


def _parse_published_year(published_at: str, fallback: int) -> int:
    try:
        return int(published_at[:4])
    except (ValueError, IndexError):
        return fallback


def _build_source(item: dict[str, Any], stats: dict[str, Any], year: int) -> SourceRaw | None:
    vid_id = (item.get("id") or {}).get("videoId") or ""
    if not vid_id:
        return None
    snippet = item.get("snippet") or {}
    title = snippet.get("title") or None
    description = snippet.get("description") or None
    raw_text = f"{title or ''}\n{description or ''}".strip() or None
    pub_year = _parse_published_year(snippet.get("publishedAt") or "", year)

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
    Fetch YouTube videos about Thai quantum tech for `year`.

    Two search passes (Q2 + Q3 Thai queries), deduped by video_id.
    Third pass: channels.list to verify Thai origin.
    Rejects videos where channel.country != TH AND defaultLanguage not in th/th-TH.
    """
    try:
        async with httpx.AsyncClient() as client:
            all_items: list[dict[str, Any]] = []
            seen_ids: set[str] = set()

            for query in _QUERIES:
                page_token: str | None = None
                for page_num in range(2):  # 2 pages × 50 = 100 per query
                    items, page_token = await _search_page(client, query, year, page_token)
                    for item in items:
                        vid_id = (item.get("id") or {}).get("videoId") or ""
                        if vid_id and vid_id not in seen_ids:
                            seen_ids.add(vid_id)
                            all_items.append(item)
                    logger.debug("YouTube q=%r page %d: %d items", query[:20], page_num + 1, len(items))
                    if not page_token:
                        break
                    await asyncio.sleep(1.0)
                await asyncio.sleep(2.0)  # gap between query passes

            if not all_items:
                return ServiceResult(ok=True, data=[])

            # Pass 2: video statistics
            video_ids = [
                (item.get("id") or {}).get("videoId") or ""
                for item in all_items
            ]
            video_ids = [v for v in video_ids if v]

            stats_map: dict[str, dict[str, Any]] = {}
            for i in range(0, len(video_ids), 50):
                batch = video_ids[i : i + 50]
                stats_map.update(await _fetch_stats(client, batch))
                if i + 50 < len(video_ids):
                    await asyncio.sleep(1.0)

            # Pass 3: channel country/language verification
            channel_ids_ordered: list[str] = []
            seen_ch: set[str] = set()
            for item in all_items:
                cid = (item.get("snippet") or {}).get("channelId") or ""
                if cid and cid not in seen_ch:
                    seen_ch.add(cid)
                    channel_ids_ordered.append(cid)

            channel_info = await _fetch_channel_info(client, channel_ids_ordered)

        # Build SourceRaw records — filter non-Thai channels
        records: list[SourceRaw] = []
        rejected = 0
        seen_urls: set[str] = set()

        for item in all_items:
            snippet = item.get("snippet") or {}
            cid = snippet.get("channelId") or ""
            info = channel_info.get(cid)
            if not _is_thai_channel(info):
                rejected += 1
                continue
            vid_id = (item.get("id") or {}).get("videoId") or ""
            src = _build_source(item, stats_map.get(vid_id, {}), year)
            if src and src.url not in seen_urls:
                seen_urls.add(src.url)
                records.append(src)

        logger.info(
            "YouTube fetch_yearly year=%d: %d accepted, %d rejected (non-TH channel)",
            year, len(records), rejected,
        )
        return ServiceResult(
            ok=True,
            data=[r.model_dump() for r in records],
        )

    except Exception as exc:
        return ServiceResult(ok=False, error=str(exc))
