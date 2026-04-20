from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Literal
from zoneinfo import ZoneInfo

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from siamquantum.config import settings
from siamquantum.models import ServiceResult, SourceRaw

logger = logging.getLogger(__name__)

_CSE_URL = "https://www.googleapis.com/customsearch/v1"
_QUOTA_FILE = Path(__file__).parent.parent / "data" / "cse_quota_state.json"
_DAILY_HARD_LIMIT = 90
_PACIFIC = ZoneInfo("America/Los_Angeles")

Tier = Literal["academic", "media"]


class QuotaExhaustedError(Exception):
    pass


class _APIError(Exception):
    pass


# ---------------------------------------------------------------------------
# Persistent Pacific-date quota tracker
# ---------------------------------------------------------------------------

def _pacific_today() -> str:
    return datetime.now(tz=_PACIFIC).strftime("%Y-%m-%d")


def _load_quota() -> dict[str, Any]:
    if _QUOTA_FILE.exists():
        try:
            data: dict[str, Any] = json.loads(_QUOTA_FILE.read_text(encoding="utf-8"))
            return data
        except Exception:
            pass
    return {"last_reset_pacific_date": "", "queries_used_today": 0}


def _save_quota_atomic(q: dict[str, Any]) -> None:
    """Write quota file atomically via temp file + os.replace()."""
    _QUOTA_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = _QUOTA_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(q), encoding="utf-8")
    os.replace(tmp, _QUOTA_FILE)


def _increment_quota() -> int:
    """
    Increment daily query counter (Pacific-date reset).
    Counter incremented BEFORE API call — Google charges quota regardless of response.
    Raises QuotaExhaustedError at >= 90.
    """
    today = _pacific_today()
    q = _load_quota()
    if q.get("last_reset_pacific_date") != today:
        q = {"last_reset_pacific_date": today, "queries_used_today": 0}
    count: int = int(q.get("queries_used_today") or 0) + 1
    q["queries_used_today"] = count
    _save_quota_atomic(q)
    if count > _DAILY_HARD_LIMIT:
        raise QuotaExhaustedError(
            f"CSE quota guard: {count}/100 used today (halting at {_DAILY_HARD_LIMIT} to preserve retry buffer)"
        )
    return count


def get_quota_status() -> dict[str, Any]:
    """Return current quota state for CLI reporting."""
    q = _load_quota()
    today = _pacific_today()
    if q.get("last_reset_pacific_date") != today:
        return {"pacific_date": today, "queries_used_today": 0, "remaining": _DAILY_HARD_LIMIT}
    used: int = int(q.get("queries_used_today") or 0)
    return {"pacific_date": today, "queries_used_today": used, "remaining": max(0, _DAILY_HARD_LIMIT - used)}


# ---------------------------------------------------------------------------
# Probe-once cache (module-level, per session)
# ---------------------------------------------------------------------------

_OR_QUERY_SUPPORTED: bool | None = None


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=5, min=5, max=30),
    retry=retry_if_exception_type(_APIError),
    reraise=True,
)
def _get_page(cx: str, query: str, start: int, date_restrict: str) -> list[dict[str, Any]]:
    """One CSE page call (10 results). Increments quota BEFORE call."""
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

    if r.status_code in (429, 500, 502, 503, 504):
        time.sleep(5)
        raise _APIError(f"CSE HTTP {r.status_code}")
    if r.status_code == 403:
        msg = r.json().get("error", {}).get("message", r.text[:200])
        raise RuntimeError(f"CSE 403 (API not enabled or key restricted): {msg}")
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
# OR-query feasibility probe — probe-once cache
# ---------------------------------------------------------------------------

def probe_or_query(tier: Tier = "academic") -> bool:
    """
    Test whether CSE accepts OR-syntax. Hits API only once per session.
    Subsequent calls return cached result without API call.
    """
    global _OR_QUERY_SUPPORTED
    if _OR_QUERY_SUPPORTED is not None:
        logger.debug("CSE OR-query probe: returning cached result=%s", _OR_QUERY_SUPPORTED)
        return _OR_QUERY_SUPPORTED

    cx = _cx_for_tier(tier)
    try:
        items = _get_page(cx, "ควอนตัม OR quantum", 1, "y2024")
        _OR_QUERY_SUPPORTED = len(items) > 0
        logger.info(
            "CSE OR-query probe (first call): %s (%d items)",
            "supported" if _OR_QUERY_SUPPORTED else "no results",
            len(items),
        )
    except (RuntimeError, QuotaExhaustedError) as exc:
        logger.warning("CSE OR-query probe failed: %s — falling back to Thai-only", exc)
        _OR_QUERY_SUPPORTED = False
    except _APIError as exc:
        logger.warning("CSE OR-query probe API error: %s — falling back to Thai-only", exc)
        _OR_QUERY_SUPPORTED = False

    return _OR_QUERY_SUPPORTED


# ---------------------------------------------------------------------------
# Main fetch
# ---------------------------------------------------------------------------

def fetch_cse_yearly(year: int, tier: Tier, use_or_query: bool = True) -> ServiceResult:
    """
    Fetch up to 50 CSE results (5 pages × 10) for `year` and `tier`.
    Respects daily quota (hard stop at 90 queries/day Pacific time).
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
