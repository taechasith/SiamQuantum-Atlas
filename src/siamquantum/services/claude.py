from __future__ import annotations

import json
import logging
from typing import Any

import anthropic
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from siamquantum.config import settings
from siamquantum.models import EntityClassification, RelevanceVerdict, TaxonomyClassification, Triplet

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
 "engagement_level": "low|medium|high",
 "media_format": "text_static|audio|video_short|video_long|broadcast_ott|movie|animation",
 "media_format_detail": "short free text description or null",
 "user_intent": "entertainment|information_news|education_self_improvement|lifestyle_inspiration|social_relational",
 "thai_cultural_angle": "short free text if Thai cultural framing present, else null"}"""

_TAXONOMY_SYSTEM = """\
Classify this content along two dimensions.
Return ONLY valid JSON with no explanation or markdown:
{"media_format": "text_static|audio|video_short|video_long|broadcast_ott|movie|animation",
 "media_format_detail": "short free text description or null",
 "user_intent": "entertainment|information_news|education_self_improvement|lifestyle_inspiration|social_relational",
 "thai_cultural_angle": "short free text if Thai cultural framing is present, else null"}
Choose best-fit category. Do not return null for media_format or user_intent."""

_DEDUPE_SYSTEM = """\
Determine if these two texts describe the same piece of content (same article or video, possibly republished).
Return ONLY valid JSON: {"is_duplicate": true} or {"is_duplicate": false}"""

_RELEVANCE_SYSTEM = """\
You are a quantum technology content classifier for a Thai research platform.

REJECT (is_quantum_tech=false) if the content is:
- Pseudoscience using 'quantum' as buzzword: quantum healing, quantum manifestation,
  quantum leap (self-help sense), quantum mysticism, law of attraction
- Music/entertainment with 'Quantum' in name only (band names, song titles, product names)
- Products/vehicles with 'Quantum' as brand/model (VW Santana Quantum, etc.)
- Tangential mentions where quantum is not the topic

ACCEPT (is_quantum_tech=true) only if the content substantively discusses:
- Quantum computing, quantum algorithms, quantum hardware
- Quantum communication/cryptography/networking
- Quantum sensing/metrology
- Quantum materials (superconductors, topological, etc.)
- Quantum physics fundamentals (entanglement, superposition — as physics, not metaphor)
- Quantum technology policy, industry, education

REJECT (is_thailand_related=false) if the content is:
- Entirely about non-Thai events, non-Thai companies, non-Thai researchers,
  with no Thai audience or local angle
- Example: "Willow chip Google" (global), "Brian Cox lecture" (UK academia),
  "VW Santana Brazil" (Brazilian auto)

ACCEPT (is_thailand_related=true) if:
- Thai publisher + Thai audience (even if covering global quantum news, it's framed for Thai readers)
- Thai researcher, company, or institution mentioned
- Thailand policy/education/industry angle
- Thai-language content targeting Thai audience

Be strict. When in doubt, reject with rejection_reason.
Return ONLY valid JSON matching this schema exactly:
{
  "is_quantum_tech": true,
  "is_thailand_related": true,
  "quantum_domain": "quantum_computing",
  "rejection_reason": null,
  "confidence": 0.95
}
quantum_domain must be one of: quantum_computing, quantum_communication, quantum_sensing,
quantum_materials, quantum_fundamentals, quantum_education, quantum_policy_industry, not_applicable.
No preamble."""


class _APIError(Exception):
    pass


# Module-level token accumulator — reset between runs via reset_usage()
_usage_input: int = 0
_usage_output: int = 0


def get_usage() -> tuple[int, int]:
    return _usage_input, _usage_output


def reset_usage() -> None:
    global _usage_input, _usage_output
    _usage_input = 0
    _usage_output = 0


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
    global _usage_input, _usage_output
    try:
        msg = _client().messages.create(
            model=settings.claude_model,
            max_tokens=512,
            temperature=0,
            system=system,
            messages=[{"role": "user", "content": user}],
        )
        _usage_input += msg.usage.input_tokens
        _usage_output += msg.usage.output_tokens
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


def _fallback_area(text: str) -> str:
    lower = text.lower()
    if "quantum" in lower or "ควอนตัม" in text:
        return "quantum technology"
    if "covid" in lower or "โควิด" in text:
        return "covid-19"
    if "cyber" in lower or "ไซเบอร์" in text:
        return "cybersecurity"
    return "general topic"


def _fallback_triplets(text: str) -> list[Triplet]:
    cleaned = " ".join((text or "").split())
    if not cleaned:
        return []
    subject = cleaned[:80]
    return [
        Triplet(
            subject=subject,
            relation="mentions",
            object=_fallback_area(cleaned),
            confidence=0.2,
        )
    ]


def _fallback_entity(text: str, title: str | None = None, url: str = "") -> EntityClassification:
    content = f"{title or ''} {text}".strip()
    lower_url = url.lower()
    content_type = "educational" if "youtube" in lower_url else "news"
    production_type = "independent" if "youtube" in lower_url else "corporate_media"
    return EntityClassification(
        content_type=content_type,
        production_type=production_type,
        area=_fallback_area(content),
        engagement_level="low",
    )


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
            return _fallback_triplets(text)
        except Exception as exc:
            logger.warning("extract_triplets: unexpected error: %s", exc)
            return _fallback_triplets(text)

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
            return _fallback_entity(text, title=title, url=url)
        except Exception as exc:
            logger.warning("classify_entity: unexpected error: %s", exc)
            return _fallback_entity(text, title=title, url=url)

    return None


def classify_taxonomy(
    text: str,
    title: str | None = None,
    url: str = "",
) -> TaxonomyClassification | None:
    """Classify media_format and user_intent for taxonomy backfill. Returns None on failure."""
    snippet = f"Title: {title or ''}\nURL: {url}\n\nText:\n{text[:2000]}"
    for attempt in range(2):
        try:
            raw = _call(_TAXONOMY_SYSTEM, snippet)
            data = _parse_json(raw)
            return TaxonomyClassification(**data)
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
            if attempt == 0:
                continue
            logger.warning("classify_taxonomy: parse failed: %s", exc)
            return None
        except _APIError as exc:
            logger.warning("classify_taxonomy: API error: %s", exc)
            return None
        except Exception as exc:
            logger.warning("classify_taxonomy: unexpected error: %s", exc)
            return None
    return None


def is_relevant_source(
    title: str | None,
    raw_text: str | None,
    platform: str,
) -> RelevanceVerdict | None:
    """
    2-gate LLM classifier: quantum tech relevance + Thailand relatedness.
    temperature=0, 1 parse retry. Returns None on unrecoverable failure.
    """
    snippet = (
        f"Platform: {platform}\n"
        f"Title: {title or '(no title)'}\n\n"
        f"Text:\n{(raw_text or '')[:3000]}"
    )

    for attempt in range(2):
        try:
            raw = _call(_RELEVANCE_SYSTEM, snippet)
            data = _parse_json(raw)
            return RelevanceVerdict(**data)
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
            if attempt == 0:
                logger.debug("Relevance parse failed (attempt 1), retrying: %s", exc)
                continue
            logger.warning("is_relevant_source: parse failed after 2 attempts: %s", exc)
            return None
        except _APIError as exc:
            logger.warning("is_relevant_source: API error: %s", exc)
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
