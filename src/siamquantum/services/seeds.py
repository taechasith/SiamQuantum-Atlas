from __future__ import annotations

import logging
from pathlib import Path

from typing import Any

import httpx
import yaml
from bs4 import BeautifulSoup

from siamquantum.models import ServiceResult, SourceRaw

logger = logging.getLogger(__name__)

_SEED_FILE = Path(__file__).parent.parent / "data" / "seed_urls.yaml"
_HEADERS = {"User-Agent": "SiamQuantumAtlas/1.0 (+research)"}


def _extract_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "nav", "footer", "header"]):
        tag.decompose()
    return " ".join(soup.get_text(" ", strip=True).split())[:8000]


def fetch_seeds() -> ServiceResult:
    """
    Fetch all URLs from seed_urls.yaml, extract title + text via httpx + BS4.
    Skips 404/403/timeout with warning. Platform tag: 'manual_seed'.
    """
    if not _SEED_FILE.exists():
        return ServiceResult(ok=False, error=f"Seed file not found: {_SEED_FILE}")

    raw_yaml: dict[str, list[dict[str, Any]]] = yaml.safe_load(_SEED_FILE.read_text(encoding="utf-8"))
    seeds: list[dict[str, Any]] = raw_yaml.get("seeds", [])
    records: list[SourceRaw] = []

    for seed in seeds:
        url: str = seed.get("url", "")
        title_hint: str | None = seed.get("title_hint")
        published_year: int = int(seed.get("published_year", 2020))
        is_direct: bool = bool(seed.get("direct", False))

        if not url:
            continue

        # Direct seeds: insert with known metadata, skip HTTP fetch
        if is_direct:
            records.append(
                SourceRaw(
                    platform="manual_seed",
                    url=url,
                    title=title_hint,
                    raw_text=None,
                    published_year=published_year,
                )
            )
            logger.info("seed direct: %s", url)
            continue

        try:
            r = httpx.get(url, headers=_HEADERS, timeout=10, follow_redirects=True)
            if r.status_code in (403, 404):
                logger.warning("seed skip HTTP %d: %s", r.status_code, url)
                continue
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")
            page_title = soup.title.string.strip() if soup.title and soup.title.string else title_hint
            raw_text = _extract_text(r.text) or None
            records.append(
                SourceRaw(
                    platform="manual_seed",
                    url=url,
                    title=page_title or title_hint,
                    raw_text=raw_text,
                    published_year=published_year,
                )
            )
            logger.info("seed ok: %s", url)
        except httpx.TimeoutException:
            logger.warning("seed timeout: %s", url)
        except Exception as exc:
            logger.warning("seed error %s: %s", url, exc)

    return ServiceResult(ok=True, data=[r.model_dump() for r in records])
