from __future__ import annotations

import logging
from datetime import datetime

import feedparser  # type: ignore[import-untyped]

from siamquantum.models import ServiceResult, SourceRaw

logger = logging.getLogger(__name__)

FEEDS: dict[str, str] = {
    "narit": "https://narit.or.th/news/rss",
    "tint": "https://tint.or.th/rss",
    "sciencefocus": "https://sciencefocus.co/rss",
}

_KEYWORDS = ["ควอนตัม", "quantum", "คิวบิต"]


def _matches(text: str) -> bool:
    low = text.lower()
    return any(kw.lower() in low for kw in _KEYWORDS)


def _parse_year(entry: feedparser.FeedParserDict) -> int:
    for field in ("published_parsed", "updated_parsed"):
        val = getattr(entry, field, None)
        if val:
            try:
                return int(val.tm_year)
            except (AttributeError, TypeError):
                pass
    return datetime.utcnow().year


def fetch_rss(feed_name: str) -> ServiceResult:
    """
    Fetch and keyword-filter one RSS feed.
    Returns ServiceResult with data=list[dict] (SourceRaw schema).
    """
    url = FEEDS.get(feed_name)
    if not url:
        return ServiceResult(ok=False, error=f"Unknown feed: {feed_name!r}. Known: {list(FEEDS)}")

    try:
        parsed = feedparser.parse(url)
    except Exception as exc:
        return ServiceResult(ok=False, error=f"feedparser error on {feed_name}: {exc}")

    if parsed.bozo and not parsed.entries:
        return ServiceResult(ok=False, error=f"{feed_name}: feed parse error — {parsed.bozo_exception}")

    records: list[SourceRaw] = []
    seen: set[str] = set()

    for entry in parsed.entries:
        link: str = getattr(entry, "link", "") or ""
        if not link or link in seen:
            continue

        title: str = getattr(entry, "title", "") or ""
        summary: str = getattr(entry, "summary", "") or ""
        raw_text = f"{title}\n{summary}".strip() or None

        if not _matches(title) and not _matches(summary):
            continue

        seen.add(link)
        records.append(
            SourceRaw(
                platform=f"rss_{feed_name}",
                url=link,
                title=title or None,
                raw_text=raw_text,
                published_year=_parse_year(entry),
            )
        )

    logger.info("RSS %s: %d entries → %d quantum-matched", feed_name, len(parsed.entries), len(records))
    return ServiceResult(ok=True, data=[r.model_dump() for r in records])
