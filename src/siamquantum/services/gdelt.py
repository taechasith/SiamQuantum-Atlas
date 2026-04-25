from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime
from typing import Any, cast

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from siamquantum.config import settings
from siamquantum.models import ServiceResult, SourceRaw

logger = logging.getLogger(__name__)

_QUERY = "quantum sourcecountry:TH"
_MAX_RECORDS = 250
_MIN_INTERVAL = 6.0  # GDELT enforces 1 req/5s; 6s gives headroom


def _year_from_seendate(seendate: str, fallback: int) -> int:
    try:
        return int(seendate[:4])
    except (ValueError, IndexError):
        return fallback


def _parse_response(data: dict[str, Any], year: int) -> list[SourceRaw]:
    articles = data.get("articles") or []
    if len(articles) >= _MAX_RECORDS:
        logger.warning(
            "GDELT returned %d for year=%d — at maxrecords cap. "
            "Implement monthly window splitting before next ingest.",
            len(articles),
            year,
        )
    out: list[SourceRaw] = []
    seen_urls: set[str] = set()
    for art in articles:
        url = (art.get("url") or "").strip()
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)
        title = art.get("title") or None
        seendate = art.get("seendate") or ""
        pub_year = _year_from_seendate(seendate, year)
        out.append(
            SourceRaw(
                platform="gdelt",
                url=url,
                title=title,
                raw_text=title,  # ArtList has no full text; title used as proxy
                published_year=pub_year,
            )
        )
    return out


class _RateLimitError(Exception):
    pass


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=6, max=30),
    retry=retry_if_exception_type(_RateLimitError),
    reraise=True,
)
async def _fetch(client: httpx.AsyncClient, params: dict[str, str]) -> dict[str, Any]:
    await asyncio.sleep(_MIN_INTERVAL)
    resp = await client.get(settings.gdelt_base_url, params=params, timeout=30.0)
    if resp.status_code == 429:
        raise _RateLimitError("GDELT rate limit (429)")
    resp.raise_for_status()
    text = resp.text.strip()
    if not text:
        return {}  # GDELT returns empty body when no results
    return cast(dict[str, Any], resp.json())


async def fetch_yearly(year: int) -> ServiceResult:
    """
    Fetch GDELT articles matching quantum from Thai sources for `year`.
    Returns ServiceResult with data=list[dict] (SourceRaw schema).
    """
    return await fetch_daterange(
        date(year, 1, 1),
        date(year, 12, 31),
    )


async def fetch_daterange(start: date, end: date) -> ServiceResult:
    """
    Fetch GDELT articles for an arbitrary date range [start, end] inclusive.
    Uses GDELT YYYYMMDDHHMMSS format: start at 000000, end at 235959.
    """
    params: dict[str, str] = {
        "query": _QUERY,
        "format": "json",
        "mode": "ArtList",
        "startdatetime": start.strftime("%Y%m%d000000"),
        "enddatetime": end.strftime("%Y%m%d235959"),
        "maxrecords": str(_MAX_RECORDS),
        "sort": "DateDesc",
    }
    try:
        async with httpx.AsyncClient() as client:
            data = await _fetch(client, params)
        records = _parse_response(data, start.year)
        return ServiceResult(ok=True, data=[r.model_dump() for r in records])
    except Exception as exc:
        return ServiceResult(ok=False, error=str(exc))
