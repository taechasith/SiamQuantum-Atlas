from __future__ import annotations

import re
import unicodedata

from langdetect import DetectorFactory, detect

DetectorFactory.seed = 0

try:
    from pythainlp.tokenize import word_tokenize
except Exception:  # pragma: no cover
    word_tokenize = None


def normalize_unicode(text: str) -> str:
    return re.sub(r"\s+", " ", unicodedata.normalize("NFKC", text)).strip()


def detect_language(text: str) -> str:
    if not text.strip():
        return "unknown"
    try:
        return detect(text)
    except Exception:
        return "unknown"


def tokenize_thai_mixed(text: str) -> list[str]:
    normalized = normalize_unicode(text)
    if word_tokenize:
        return [token for token in word_tokenize(normalized, keep_whitespace=False) if token.strip()]
    return re.findall(r"[\u0E00-\u0E7Fa-zA-Z0-9_]+", normalized.lower())


def canonical_summary(title: str, description: str | None, body: str | None) -> str:
    return normalize_unicode(" ".join(filter(None, [title, description, body])))
