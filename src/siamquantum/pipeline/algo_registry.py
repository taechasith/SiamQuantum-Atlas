"""
Algorithm Registry
==================
Versioned, self-selecting algorithm store.

Problem solved: every time we improve the relevance classifier or NLP extraction,
we previously overwrote the old code. This makes it impossible to:
  - A/B test new vs old
  - Automatically roll back when a new version regresses
  - Let future devs add their own algorithm versions safely

Usage — registering a new relevance algorithm version:

    from siamquantum.pipeline.algo_registry import algo_registry, AlgoMeta

    @algo_registry.register(
        name="relevance",
        version="v3",
        meta=AlgoMeta(
            description="GPT-4o based classifier with chain-of-thought",
            input_schema={"text": "str", "title": "str"},
            output_schema={"is_qt": "bool", "is_th": "bool", "confidence": "float"},
        ),
    )
    async def relevance_v3(text: str, title: str) -> dict:
        ...

    # Select best automatically:
    fn = algo_registry.best("relevance")

    # Pin a specific version:
    fn = algo_registry.get("relevance", "v2")
"""
from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

logger = logging.getLogger(__name__)

_METRICS_FILE = Path(__file__).parent.parent.parent.parent / "data" / "algo_metrics.jsonl"


@dataclass
class AlgoMeta:
    description: str = ""
    input_schema: dict[str, str] = field(default_factory=dict)
    output_schema: dict[str, str] = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)


@dataclass
class AlgoRun:
    """Record of a single algorithm invocation — appended to algo_metrics.jsonl."""
    name: str
    version: str
    input_hash: str
    duration_ms: float
    ok: bool
    validation_score: float | None = None   # set externally after ground-truth comparison
    error: str | None = None
    ts: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict[str, Any]:
        return self.__dict__.copy()


class _AlgoEntry:
    def __init__(self, name: str, version: str, fn: Callable, meta: AlgoMeta) -> None:
        self.name = name
        self.version = version
        self.fn = fn
        self.meta = meta
        self._scores: list[float] = []   # validation scores collected over time

    def record_score(self, score: float) -> None:
        self._scores.append(score)

    @property
    def mean_score(self) -> float | None:
        return sum(self._scores) / len(self._scores) if self._scores else None


class AlgoRegistry:
    """
    Central registry of all algorithm versions.
    Auto-selects the best version by mean validation score.
    Falls back to the most-recently-registered version when no scores exist.
    """

    def __init__(self) -> None:
        # name → ordered list of entries (oldest first)
        self._entries: dict[str, list[_AlgoEntry]] = {}

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register(
        self,
        name: str,
        version: str,
        meta: AlgoMeta | None = None,
    ) -> Callable[[Callable], Callable]:
        """Decorator. Usage: @algo_registry.register("relevance", "v2")"""
        def decorator(fn: Callable) -> Callable:
            self._add(name, version, fn, meta or AlgoMeta())
            return fn
        return decorator

    def register_fn(
        self,
        name: str,
        version: str,
        fn: Callable,
        meta: AlgoMeta | None = None,
    ) -> None:
        """Imperative registration — for wrapping existing functions."""
        self._add(name, version, fn, meta or AlgoMeta())

    def _add(self, name: str, version: str, fn: Callable, meta: AlgoMeta) -> None:
        if name not in self._entries:
            self._entries[name] = []
        # Allow re-registration (e.g. during testing) — overwrite version
        existing = [e for e in self._entries[name] if e.version == version]
        if existing:
            existing[0].fn = fn
            existing[0].meta = meta
        else:
            self._entries[name].append(_AlgoEntry(name, version, fn, meta))
        logger.debug("AlgoRegistry: registered %s/%s", name, version)

    # ------------------------------------------------------------------
    # Retrieval
    # ------------------------------------------------------------------

    def get(self, name: str, version: str) -> Callable:
        """Get a specific version. Raises KeyError if not found."""
        entry = self._find(name, version)
        return self._timed_wrapper(entry)

    def best(self, name: str) -> Callable:
        """
        Return the best-performing version for `name`.
        Selection rule:
          1. If any versions have validation scores → pick highest mean_score.
          2. If no scores yet → pick the most recently registered (last in list).
        """
        entries = self._entries.get(name, [])
        if not entries:
            raise KeyError(f"No algorithm registered under '{name}'")

        scored = [e for e in entries if e.mean_score is not None]
        if scored:
            best = max(scored, key=lambda e: e.mean_score or 0.0)  # type: ignore[return-value]
            logger.debug(
                "AlgoRegistry: selected %s/%s (mean_score=%.3f)",
                name, best.version, best.mean_score or 0,
            )
        else:
            best = entries[-1]
            logger.debug(
                "AlgoRegistry: selected %s/%s (no scores yet, using latest)",
                name, best.version,
            )

        return self._timed_wrapper(best)

    def record_validation(self, name: str, version: str, score: float) -> None:
        """
        Call this after comparing algorithm output against ground truth.
        Scores accumulate; `best()` will automatically prefer higher-scoring versions.
        """
        entry = self._find(name, version)
        entry.record_score(score)
        logger.info(
            "AlgoRegistry: recorded validation score %.3f for %s/%s",
            score, name, version,
        )

    def versions(self, name: str) -> list[str]:
        return [e.version for e in self._entries.get(name, [])]

    def all_names(self) -> list[str]:
        return list(self._entries.keys())

    def report(self) -> dict[str, Any]:
        """Summary of all registered algorithms and their scores — for /api/algo/status."""
        out: dict[str, Any] = {}
        for name, entries in self._entries.items():
            out[name] = [
                {
                    "version": e.version,
                    "description": e.meta.description,
                    "mean_score": e.mean_score,
                    "n_scores": len(e._scores),
                }
                for e in entries
            ]
        return out

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _find(self, name: str, version: str) -> _AlgoEntry:
        for e in self._entries.get(name, []):
            if e.version == version:
                return e
        raise KeyError(f"Algorithm '{name}' version '{version}' not registered")

    def _timed_wrapper(self, entry: _AlgoEntry) -> Callable:
        """Wrap the fn to record timing + append to metrics log."""
        import functools

        @functools.wraps(entry.fn)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            h = self._hash_args(args, kwargs)
            t0 = time.perf_counter()
            try:
                result = entry.fn(*args, **kwargs)
                dur = (time.perf_counter() - t0) * 1000
                self._log_run(AlgoRun(entry.name, entry.version, h, dur, ok=True))
                return result
            except Exception as exc:
                dur = (time.perf_counter() - t0) * 1000
                self._log_run(AlgoRun(entry.name, entry.version, h, dur, ok=False, error=str(exc)))
                raise

        @functools.wraps(entry.fn)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            import asyncio
            h = self._hash_args(args, kwargs)
            t0 = time.perf_counter()
            try:
                if asyncio.iscoroutinefunction(entry.fn):
                    result = await entry.fn(*args, **kwargs)
                else:
                    result = entry.fn(*args, **kwargs)
                dur = (time.perf_counter() - t0) * 1000
                self._log_run(AlgoRun(entry.name, entry.version, h, dur, ok=True))
                return result
            except Exception as exc:
                dur = (time.perf_counter() - t0) * 1000
                self._log_run(AlgoRun(entry.name, entry.version, h, dur, ok=False, error=str(exc)))
                raise

        import asyncio as _asyncio
        if _asyncio.iscoroutinefunction(entry.fn):
            return async_wrapper
        return sync_wrapper

    @staticmethod
    def _hash_args(args: tuple, kwargs: dict) -> str:
        try:
            raw = json.dumps({"a": args, "k": kwargs}, default=str, sort_keys=True)
            return hashlib.sha1(raw.encode()).hexdigest()[:12]
        except Exception:
            return "unhashable"

    def _log_run(self, run: AlgoRun) -> None:
        try:
            _METRICS_FILE.parent.mkdir(parents=True, exist_ok=True)
            with _METRICS_FILE.open("a", encoding="utf-8") as f:
                f.write(json.dumps(run.to_dict()) + "\n")
        except Exception:
            pass  # never let logging break the algorithm


# Singleton — import this everywhere
algo_registry = AlgoRegistry()


# ---------------------------------------------------------------------------
# Bootstrap: wrap existing algorithm functions into the registry so they are
# tracked without changing their implementation files.
# ---------------------------------------------------------------------------

def _bootstrap_existing_algorithms() -> None:
    """
    Register the current algorithm implementations as versioned entries.
    New versions are added here too — the registry picks the best automatically.
    """
    try:
        from siamquantum.pipeline.filter import recheck_relevance
        algo_registry.register_fn(
            "relevance", "v1", recheck_relevance,
            AlgoMeta(
                description="Claude-3.5-Sonnet single-pass relevance classifier",
                input_schema={"source_id": "int", "db_path": "Path"},
                output_schema={"is_qt": "bool", "is_th": "bool", "confidence": "float"},
                tags=["claude", "classification"],
            ),
        )
    except Exception:
        pass

    try:
        from siamquantum.pipeline.nlp import extract_triplets
        algo_registry.register_fn(
            "triplet_extraction", "v1", extract_triplets,
            AlgoMeta(
                description="Claude-3.5-Sonnet subject-relation-object extraction",
                input_schema={"text": "str"},
                output_schema={"triplets": "list[Triplet]"},
                tags=["claude", "nlp"],
            ),
        )
    except Exception:
        pass

    try:
        from siamquantum.pipeline.analyze import classify_entity
        algo_registry.register_fn(
            "entity_classification", "v1", classify_entity,
            AlgoMeta(
                description="Claude taxonomy classifier (content_type, media_format, user_intent)",
                input_schema={"text": "str", "title": "str"},
                output_schema={"entity": "EntityClassification"},
                tags=["claude", "taxonomy"],
            ),
        )
    except Exception:
        pass


_bootstrap_existing_algorithms()
