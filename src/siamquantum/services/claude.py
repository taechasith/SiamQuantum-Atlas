from __future__ import annotations

import json
import logging
from typing import Any

import anthropic
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from siamquantum.config import settings
from siamquantum.models import EntityClassification, Triplet

logger = logging.getLogger(__name__)

_TRIPLET_SYSTEM = """\
You extract knowledge triplets from text about quantum technology.
Return ONLY valid JSON with no explanation or markdown:
{"triplets": [{"subject": "string", "relation": "string", "object": "string", "confidence": 0.9}]}
If no triplets can be extracted, return: {"triplets": []}"""

_ENTITY_SYSTEM = """\
Classify this quantum technology content.
Return ONLY valid JSON with no explanation or markdown:
{"content_type": "academic|news|educational|entertainment",
 "production_type": "state_research|university|corporate_media|independent",
 "area": "brief topic area string (e.g. quantum computing, quantum communication)",
 "engagement_level": "low|medium|high"}"""

_DEDUPE_SYSTEM = """\
Determine if these two texts describe the same piece of content (same article or video, possibly republished).
Return ONLY valid JSON: {"is_duplicate": true} or {"is_duplicate": false}"""


class _APIError(Exception):
    pass


def _client() -> anthropic.Anthropic:
    return anthropic.Anthropic(api_key=settings.anthropic_api_key)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(_APIError),
    reraise=True,
)
def _call(system: str, user: str) -> str:
    """Single Claude API call. Returns raw text content. Retries on API errors."""
    try:
        msg = _client().messages.create(
            model=settings.claude_model,
            max_tokens=512,
            temperature=0,
            system=system,
            messages=[{"role": "user", "content": user}],
        )
        return msg.content[0].text  # type: ignore[union-attr]
    except anthropic.RateLimitError as exc:
        raise _APIError(str(exc)) from exc
    except anthropic.APIConnectionError as exc:
        raise _APIError(str(exc)) from exc
    except Exception:
        raise


def _parse_json(text: str) -> Any:
    text = text.strip()
    # Strip markdown fences if Claude wraps response despite instruction
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
    return json.loads(text.strip())


def extract_triplets(text: str) -> list[Triplet]:
    """
    Call Claude to extract knowledge triplets from text.
    1 retry on JSON parse failure. Returns empty list on unrecoverable failure.
    """
    if not text or not text.strip():
        return []

    for attempt in range(2):
        try:
            raw = _call(_TRIPLET_SYSTEM, f"Text:\n{text[:4000]}")
            data = _parse_json(raw)
            return [Triplet(**t) for t in data.get("triplets", [])]
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
            if attempt == 0:
                logger.debug("Triplet parse failed (attempt 1), retrying: %s", exc)
                continue
            logger.warning("extract_triplets: parse failed after 2 attempts: %s", exc)
            return []
        except _APIError as exc:
            logger.warning("extract_triplets: API error: %s", exc)
            return []

    return []


def classify_entity(
    text: str,
    title: str | None = None,
    url: str = "",
) -> EntityClassification | None:
    """
    Call Claude to classify content type, production type, area, engagement_level.
    engagement_level is an initial estimate — Phase 5 overwrites via statistical analysis.
    Returns None on failure.
    """
    snippet = f"Title: {title or ''}\nURL: {url}\n\nText:\n{text[:3000]}"

    for attempt in range(2):
        try:
            raw = _call(_ENTITY_SYSTEM, snippet)
            data = _parse_json(raw)
            return EntityClassification(**data)
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
            if attempt == 0:
                logger.debug("Entity parse failed (attempt 1), retrying: %s", exc)
                continue
            logger.warning("classify_entity: parse failed after 2 attempts: %s", exc)
            return None
        except _APIError as exc:
            logger.warning("classify_entity: API error: %s", exc)
            return None

    return None


def dedupe_check(text_a: str, text_b: str) -> bool:
    """
    Ask Claude if two texts are the same content. Only called in ambiguous cosine zone [0.6, 0.85].
    Returns True if duplicate, False if different or on any failure.
    """
    user = f"Text A:\n{text_a[:2000]}\n\nText B:\n{text_b[:2000]}"
    try:
        raw = _call(_DEDUPE_SYSTEM, user)
        data = _parse_json(raw)
        return bool(data.get("is_duplicate", False))
    except Exception as exc:
        logger.warning("dedupe_check failed: %s", exc)
        return False
