from __future__ import annotations

from siamquantum_atlas.nlp.thai_preprocess import canonical_summary, detect_language, normalize_unicode


def normalize_record_text(title: str, description: str | None, body: str | None) -> dict[str, str]:
    summary = canonical_summary(title, description, body)
    return {
        "title": normalize_unicode(title),
        "description": normalize_unicode(description or ""),
        "full_text": normalize_unicode(body or ""),
        "summary": summary,
        "language_detected": detect_language(summary),
    }
