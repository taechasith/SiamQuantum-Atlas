from __future__ import annotations

import hashlib
import math
from functools import lru_cache
from typing import Any

from siamquantum_atlas.nlp.thai_preprocess import tokenize_thai_mixed

# ── Model config ───────────────────────────────────────────────────────────────
QWEN_MODEL_ID = "Qwen/Qwen3-Embedding-8B"
EMBEDDING_DIM  = 4096  # Qwen3-Embedding-8B output dimension


@lru_cache(maxsize=1)
def _load_model() -> Any | None:
    """Lazy-load Qwen3-Embedding-8B via sentence-transformers.

    Returns the model, or None if the package / weights are unavailable.
    Loading is cached — subsequent calls return the same model object.
    """
    try:
        from sentence_transformers import SentenceTransformer  # type: ignore

        model = SentenceTransformer(
            QWEN_MODEL_ID,
            trust_remote_code=True,
        )
        return model
    except Exception:
        return None


# ── Fallback: deterministic hash embedding ─────────────────────────────────────
_FALLBACK_DIM = 128


def _hash_embed(text: str) -> list[float]:
    """Lightweight bag-of-tokens embedding used when Qwen3 is unavailable."""
    vector = [0.0] * _FALLBACK_DIM
    tokens = tokenize_thai_mixed(text)
    for token in tokens:
        h = int(hashlib.md5(token.encode()).hexdigest(), 16)
        vector[h % _FALLBACK_DIM] += 1.0
    norm = math.sqrt(sum(v * v for v in vector)) or 1.0
    return [v / norm for v in vector]


# ── Public API ─────────────────────────────────────────────────────────────────

def embed_text(text: str) -> list[float]:
    """Embed a single string.  Uses Qwen3-Embedding-8B when available."""
    model = _load_model()
    if model is not None:
        emb = model.encode(text, normalize_embeddings=True)
        return emb.tolist()
    return _hash_embed(text)


def embed_batch(
    texts: list[str],
    batch_size: int = 16,
    show_progress: bool = False,
) -> list[list[float]]:
    """Embed a list of strings in batches.

    Uses Qwen3-Embedding-8B (with proper last-token pooling) when available,
    falls back to hash-based embeddings otherwise.
    """
    if not texts:
        return []

    model = _load_model()
    if model is not None:
        embs = model.encode(
            texts,
            normalize_embeddings=True,
            batch_size=batch_size,
            show_progress_bar=show_progress,
        )
        return embs.tolist()

    return [_hash_embed(t) for t in texts]
