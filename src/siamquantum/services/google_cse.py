from __future__ import annotations

import json
import logging
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Literal

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from siamquantum.config import settings
from siamquantum.models import ServiceResult, SourceRaw

logger = logging.getLogger(__name__)

_CSE_URL = "https://www.googleapis.com/customsearch/v1"
_QUOTA_FILE = Path(__file__).parent.parent / "data" / "cse_quota.json"
_DAILY_HARD_LIMIT = 90

Tier = Literal["academic", "media"]

_CX: dict[Tier, str] = {
    "academic": "",  # populated at call time from settings
    "media": "",
}


class QuotaExhaustedError(Exception):
    pass


class _APIError(Exception):
    pass


# ---------------------------------------------------------------------------
# Quota tracker
# ---------------------------------------------------------------------------

def _load_quota() -> dict[str, Any]:
    if _QUOTA_FILE.exists():
        try:
            data: dict[str, Any] = json.loads(_QUOTA_FILE.read_text())
            return data
        except Exception:
            pass
    return {"date": "", "count": 0}


def _save_quota(q: dict[str, Any]) -> None:
    _QUOTA_FILE.parent.mkdir(parents=True, exist_ok=True)
    _QUOTA_FILE.write_text(json.dumps(q))


def _increment_quota() -> int:
    """Increment daily query counter. Raises QuotaExhaustedError if at limit."""
    today = date.today().isoformat()
    q = _load_quota()
    if q.get("date") != today:
        q = {"date": today, "count": 0}
    count: int = int(q.get("count") or 0) + 1
    q["count"] = count
    _save_quota(q)
    if count > _DAILY_HARD_LIMIT:
        raise QuotaExhaustedError(
            f"CSE daily quota exhausted: {count} queries today (limit {_DAILY_HARD_LIMIT})"
        )
    return count


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(_APIError),
    reraise=True,
)
def _get_page(cx: str, query: str, start: int, date_restrict: str) -> list[dict[str, Any]]:
    """One CSE page call (10 results). Increments quota counter."""
    _increment_quota()
    params = {
        "key": settings.google_cse_key,
        "cx": cx,
        "q": query,
        "num": "10",
        "start": str(start),
        "dateRestrict": date_restrict,
    }
    try:
        r = httpx.get(_CSE_URL, params=params, timeout=15)
    except httpx.RequestError as exc:
        raise _APIError(str(exc)) from exc

    if r.status_code in (500, 502, 503, 504, 429):
        raise _APIError(f"CSE HTTP {r.status_code}")
    if r.status_code == 403:
        msg = r.json().get("error", {}).get("message", r.text[:200])
        raise RuntimeError(f"CSE 403 (API not enabled or key invalid): {msg}")
    if r.status_code == 400:
        msg = r.json().get("error", {}).get("message", r.text[:200])
        raise RuntimeError(f"CSE 400 (bad query): {msg}")
    if r.status_code != 200:
        raise _APIError(f"CSE unexpected HTTP {r.status_code}: {r.text[:200]}")

    items: list[dict[str, Any]] = r.json().get("items") or []
    return items


def _cx_for_tier(tier: Tier) -> str:
    mapping: dict[Tier, str] = {
        "academic": settings.google_cse_cx_academic,
        "media": settings.google_cse_cx_media,
    }
    return mapping[tier]


# ---------------------------------------------------------------------------
# OR-query feasibility probe (run once, cached)
# ---------------------------------------------------------------------------

def probe_or_query(tier: Tier = "academic") -> bool:
    """
    Test whether CSE accepts OR-syntax. Returns True if supported.
    Falls back gracefully — does NOT raise on 403/400.
    """
    cx = _cx_for_tier(tier)
    try:
        items = _get_page(cx, "ควอนตัม OR quantum", 1, "y2024")
        supported = len(items) > 0
        logger.info("CSE OR-query probe: %s (%d items)", "supported" if supported else "no results", len(items))
        return supported
    except (RuntimeError, QuotaExhaustedError) as exc:
        logger.warning("CSE OR-query probe failed: %s — falling back to Thai-only", exc)
        return False
    except _APIError as exc:
        logger.warning("CSE OR-query probe API error: %s — falling back to Thai-only", exc)
        return False


# ---------------------------------------------------------------------------
# Main fetch
# ---------------------------------------------------------------------------

def fetch_cse_yearly(year: int, tier: Tier, use_or_query: bool = True) -> ServiceResult:
    """
    Fetch up to 50 CSE results (5 pages × 10) for `year` and `tier`.
    Respects daily quota (hard stop at 90 queries/day).
    """
    cx = _cx_for_tier(tier)
    query = "ควอนตัม OR quantum" if use_or_query else "ควอนตัม"
    date_restrict = f"y{year}"
    records: list[SourceRaw] = []
    seen: set[str] = set()

    for page in range(5):
        start = page * 10 + 1
        try:
            items = _get_page(cx, query, start, date_restrict)
        except QuotaExhaustedError as exc:
            logger.warning("CSE quota: %s — stopping early at page %d", exc, page + 1)
            break
        except RuntimeError as exc:
            return ServiceResult(ok=False, error=str(exc))
        except _APIError as exc:
            logger.warning("CSE page %d error: %s — stopping", page + 1, exc)
            break

        if not items:
            break

        for item in items:
            link: str = str(item.get("link") or "")
            if not link or link in seen:
                continue
            seen.add(link)
            title: str = str(item.get("title") or "")
            snippet: str = str(item.get("snippet") or "")
            raw_text = f"{title}\n{snippet}".strip() or None
            records.append(
                SourceRaw(
                    platform=f"cse_{tier}",
                    url=link,
                    title=title or None,
                    raw_text=raw_text,
                    published_year=year,
                )
            )

        if page < 4 and items:
            time.sleep(6)

    logger.info("CSE %s year=%d: %d records", tier, year, len(records))
    return ServiceResult(ok=True, data=[r.model_dump() for r in records])
