from __future__ import annotations

import logging
from typing import Callable

import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

logger = logging.getLogger(__name__)

_LOW_THRESHOLD = 0.6   # below → definitely different
_HIGH_THRESHOLD = 0.85  # above → definitely duplicate


def _thai_tokenizer(text: str) -> list[str]:
    from pythainlp import word_tokenize
    return word_tokenize(text, engine="newmm", keep_whitespace=False)


def _make_vectorizer() -> TfidfVectorizer:
    return TfidfVectorizer(
        tokenizer=_thai_tokenizer,
        token_pattern=None,  # required when providing custom tokenizer
        min_df=1,
        sublinear_tf=True,
    )


def find_duplicates(
    texts: list[str],
    ids: list[int],
    dedupe_check_fn: Callable[[str, str], bool] | None = None,
) -> set[int]:
    """
    Find duplicate source IDs via TF-IDF cosine similarity.

    - cosine < 0.6: different — no further check
    - cosine in [0.6, 0.85]: ambiguous — call dedupe_check_fn if provided
    - cosine > 0.85: duplicate

    Returns set of IDs to discard (lower-id variant of each duplicate pair is kept).
    `dedupe_check_fn` signature: (text_a, text_b) -> bool (True = duplicate).
    """
    if len(texts) < 2:
        return set()

    try:
        vectorizer = _make_vectorizer()
        matrix = vectorizer.fit_transform(texts)
    except Exception as exc:
        logger.warning("TF-IDF vectorization failed: %s", exc)
        return set()

    sims: np.ndarray[tuple[int, int], np.dtype[np.float64]] = cosine_similarity(matrix)
    discard: set[int] = set()

    for i in range(len(ids)):
        if ids[i] in discard:
            continue
        for j in range(i + 1, len(ids)):
            if ids[j] in discard:
                continue
            score = float(sims[i, j])
            if score <= _LOW_THRESHOLD:
                continue
            if score > _HIGH_THRESHOLD:
                is_dup = True
            elif dedupe_check_fn is not None:
                is_dup = dedupe_check_fn(texts[i], texts[j])
                logger.debug(
                    "Claude dedupe check ids=(%d, %d) cosine=%.3f → %s",
                    ids[i], ids[j], score, is_dup,
                )
            else:
                is_dup = False

            if is_dup:
                # Discard the later (higher-index) source; keep the earlier one
                discard.add(ids[j])

    return discard
