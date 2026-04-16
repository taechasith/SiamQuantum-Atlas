from __future__ import annotations

import logging
import os
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

from siamquantum_atlas.adapters.base import RawMediaRecord, SourceAdapter
from siamquantum_atlas.settings import settings

logger = logging.getLogger(__name__)

_DOC_API = "https://api.gdeltproject.org/api/v2/doc/doc"
_RUNTIME_DIR = settings.data_dir / "runtime"
_GDELT_LOCK_PATH = _RUNTIME_DIR / "gdelt_api.lock"
_GDELT_STAMP_PATH = _RUNTIME_DIR / "gdelt_api.last_request"
_LOCK_TIMEOUT_SEC = 120.0
_LOCK_POLL_INTERVAL_SEC = 0.25
_MIN_REQUEST_INTERVAL_SEC = 3.5

# Each query returns up to 250 articles
_QUERIES: list[dict[str, str]] = [
    {"query": "quantum Thailand", "timespan": "72h"},
    {"query": '"quantum computing" Thailand', "timespan": "7d"},
    {"query": '"quantum physics" Thailand', "timespan": "7d"},
    {"query": "ควอนตัม", "timespan": "7d", "sourcelang": "Thai"},
    {"query": "คอมพิวเตอร์ควอนตัม", "timespan": "7d", "sourcelang": "Thai"},
    {"query": "quantum Thailand", "timespan": "30d"},
    {"query": '"quantum" site:.th', "timespan": "30d"},
]

_DOMAIN_AUTHORITY: dict[str, float] = {
    "bbc.co.uk": 0.9, "reuters.com": 0.9, "nature.com": 0.95,
    "science.org": 0.95, "phys.org": 0.85, "techcrunch.com": 0.8,
    "manager.co.th": 0.65, "thairath.co.th": 0.60, "matichon.co.th": 0.65,
    "bangkokpost.com": 0.75, "nationthailand.com": 0.70,
    "thaipbs.or.th": 0.80, "pptv36.com": 0.60, "ch3.com": 0.55,
    "khaosod.co.th": 0.60, "sanook.com": 0.55, "kapook.com": 0.50,
}


class GDELTLiveAdapter(SourceAdapter):
    name = "gdelt_live"

    def fetch(self, max_items: int = 500, **_: Any) -> list[RawMediaRecord]:
        seen_urls: set[str] = set()
        records: list[RawMediaRecord] = []

        for query_cfg in _QUERIES:
            if len(records) >= max_items:
                break
            batch = self._fetch_query(query_cfg, seen_urls)
            records.extend(batch)
            time.sleep(2.0)  # GDELT enforces ~1 req/sec; 2s avoids 429

        logger.info("GDELTLive: fetched %d records", len(records))
        return records[:max_items]

    def _fetch_query(self, cfg: dict[str, str], seen: set[str]) -> list[RawMediaRecord]:
        params: dict[str, Any] = {
            "query": cfg["query"],
            "mode": "artlist",
            "maxrecords": 250,
            "format": "json",
            "timespan": cfg.get("timespan", "7d"),
            "sort": "datedesc",
        }
        if "sourcelang" in cfg:
            params["sourcelang"] = cfg["sourcelang"]

        for attempt in range(4):
            try:
                with _gdelt_request_slot():
                    resp = httpx.get(_DOC_API, params=params, timeout=20)
                if resp.status_code == 429:
                    wait = 5 * (2 ** attempt)
                    logger.warning("GDELT 429 — waiting %ds (attempt %d/4)", wait, attempt + 1)
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                data = resp.json()
                break
            except Exception as exc:
                logger.warning("GDELT fetch error (query=%r): %s", cfg["query"], exc)
                return []
        else:
            logger.warning("GDELT gave up after 4 attempts (query=%r)", cfg["query"])
            return []

        records: list[RawMediaRecord] = []
        for article in data.get("articles", []):
            url = article.get("url", "")
            if not url or url in seen:
                continue
            seen.add(url)

            domain = article.get("domain", "")
            authority = _domain_authority(domain)
            title = article.get("title", "")
            lang = article.get("language", "unknown")
            country = article.get("sourcecountry", "")

            published_at = _parse_gdelt_date(article.get("seendate", ""))
            thai_score = 1.0 if (country == "Thailand" or domain.endswith(".th") or lang == "Thai") else 0.6

            records.append(
                RawMediaRecord(
                    adapter=self.name,
                    platform="gdelt_news",
                    media_type="article",
                    title=title,
                    description=None,
                    full_text=None,
                    url=url,
                    canonical_url=url,
                    published_at=published_at,
                    language_detected=_map_gdelt_lang(lang),
                    domain=domain,
                    raw_payload={
                        "gdelt": article,
                        "query": cfg["query"],
                        "source_country": country,
                        "thai_relevance_score": thai_score,
                    },
                    rank_proxy=authority,
                    popularity_proxy=authority * (0.9 if country == "Thailand" else 0.5),
                )
            )

        return records


def _parse_gdelt_date(raw: str) -> datetime | None:
    # GDELT format: "20240101T000000Z" or "20240101000000"
    if not raw:
        return None
    raw = raw.replace("T", "").replace("Z", "")
    try:
        return datetime.strptime(raw[:14], "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    except ValueError:
        try:
            return datetime.strptime(raw[:8], "%Y%m%d").replace(tzinfo=timezone.utc)
        except ValueError:
            return None


def _map_gdelt_lang(gdelt_lang: str) -> str:
    mapping = {
        "Thai": "th", "English": "en", "Chinese": "zh",
        "Japanese": "ja", "French": "fr", "German": "de",
        "Spanish": "es", "Arabic": "ar",
    }
    return mapping.get(gdelt_lang, gdelt_lang.lower()[:2] if gdelt_lang else "unknown")


def _domain_authority(domain: str) -> float:
    for key, val in _DOMAIN_AUTHORITY.items():
        if domain.endswith(key):
            return val
    if domain.endswith(".th"):
        return 0.55
    if domain.endswith(".edu"):
        return 0.80
    if domain.endswith(".ac.th"):
        return 0.80
    return 0.40


@contextmanager
def _gdelt_request_slot():
    _RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    lock_fd = _acquire_lock(_GDELT_LOCK_PATH, timeout_sec=_LOCK_TIMEOUT_SEC)
    try:
        _wait_for_shared_cooldown(_GDELT_STAMP_PATH, min_interval_sec=_MIN_REQUEST_INTERVAL_SEC)
        yield
        _GDELT_STAMP_PATH.write_text(str(time.time()), encoding="utf-8")
    finally:
        os.close(lock_fd)
        try:
            _GDELT_LOCK_PATH.unlink()
        except FileNotFoundError:
            pass


def _acquire_lock(path: Path, timeout_sec: float) -> int:
    deadline = time.time() + timeout_sec
    while True:
        try:
            return os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_RDWR)
        except FileExistsError:
            try:
                age_sec = time.time() - path.stat().st_mtime
            except FileNotFoundError:
                continue
            if age_sec > timeout_sec:
                path.unlink(missing_ok=True)
                continue
            if time.time() >= deadline:
                raise TimeoutError(f"Timed out waiting for GDELT request lock: {path}")
            time.sleep(_LOCK_POLL_INTERVAL_SEC)


def _wait_for_shared_cooldown(path: Path, min_interval_sec: float) -> None:
    try:
        last_request = float(path.read_text(encoding="utf-8").strip())
    except (FileNotFoundError, ValueError):
        return
    remaining = min_interval_sec - (time.time() - last_request)
    if remaining > 0:
        logger.info("GDELT shared cooldown - sleeping %.1fs", remaining)
        time.sleep(remaining)
