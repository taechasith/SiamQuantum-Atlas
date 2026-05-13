"""
Source Adapter System
=====================
Adding a new data source = create one file, implement SourceAdapter, register it.
No changes to pipeline, server, or any other file required.

Example — adding "arxiv" as a new source type:

    # src/siamquantum/adapters/arxiv.py
    from siamquantum.adapters import SourceAdapter, FetchParams, adapter_registry
    from siamquantum.models import SourceRaw

    class ArxivAdapter(SourceAdapter):
        platform_id = "arxiv"
        display_name = "arXiv Preprints"

        async def fetch(self, params: FetchParams) -> list[SourceRaw]:
            ...  # call arXiv API

        def validate(self, raw: SourceRaw) -> "ValidationResult":
            ...  # domain-specific sanity checks

        def extract_meta(self, raw: SourceRaw) -> dict:
            return {"doi": ..., "authors": ..., "categories": ...}

    adapter_registry.register(ArxivAdapter())

Then call: `adapter_registry.fetch("arxiv", params)`
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Protocol, runtime_checkable

from siamquantum.models import SourceRaw


@dataclass
class FetchParams:
    """Unified fetch configuration passed to every adapter."""
    start_date: date | None = None
    end_date: date | None = None
    query: str | None = None           # keyword / search term
    limit: int = 500                   # max records per call
    extra: dict[str, Any] = field(default_factory=dict)  # adapter-specific overrides


@dataclass
class ValidationResult:
    ok: bool
    reasons: list[str] = field(default_factory=list)

    @classmethod
    def passed(cls) -> "ValidationResult":
        return cls(ok=True)

    @classmethod
    def failed(cls, *reasons: str) -> "ValidationResult":
        return cls(ok=False, reasons=list(reasons))


@runtime_checkable
class SourceAdapter(Protocol):
    """
    Contract every data-source adapter must satisfy.
    All methods are sync or async — implement whichever fits.
    """

    platform_id: str       # unique slug stored in sources.platform
    display_name: str      # human label shown in UI

    async def fetch(self, params: FetchParams) -> list[SourceRaw]:
        """Pull raw records from the external source."""
        ...

    def validate(self, raw: SourceRaw) -> ValidationResult:
        """
        Domain-specific quality gate — called before insert.
        Return ValidationResult.failed(...) to skip the record.
        """
        ...

    def extract_meta(self, raw: SourceRaw) -> dict[str, Any]:
        """
        Return arbitrary source-type-specific fields stored in source_meta JSON.
        E.g. arXiv → {"doi": ..., "authors": [...]}
             YouTube → {"thumbnail": ..., "duration_seconds": ...}
        """
        return {}


class _AdapterRegistry:
    """
    Central registry of all source adapters.
    Auto-discovered via register() at import time — no config files needed.
    """

    def __init__(self) -> None:
        self._adapters: dict[str, SourceAdapter] = {}

    def register(self, adapter: SourceAdapter) -> SourceAdapter:
        """Register an adapter. Call at module level in adapter files."""
        if adapter.platform_id in self._adapters:
            raise ValueError(
                f"Adapter '{adapter.platform_id}' already registered. "
                "Use a unique platform_id."
            )
        self._adapters[adapter.platform_id] = adapter
        return adapter

    def get(self, platform_id: str) -> SourceAdapter:
        try:
            return self._adapters[platform_id]
        except KeyError:
            raise KeyError(
                f"No adapter registered for platform '{platform_id}'. "
                f"Available: {list(self._adapters)}"
            )

    def all(self) -> dict[str, SourceAdapter]:
        return dict(self._adapters)

    async def fetch(self, platform_id: str, params: FetchParams) -> list[SourceRaw]:
        """Fetch + validate. Invalid records are dropped with a warning."""
        import logging
        log = logging.getLogger(__name__)
        adapter = self.get(platform_id)
        raws = await adapter.fetch(params)
        valid: list[SourceRaw] = []
        for raw in raws:
            result = adapter.validate(raw)
            if result.ok:
                valid.append(raw)
            else:
                log.debug(
                    "Adapter %s dropped record %s: %s",
                    platform_id, raw.url, "; ".join(result.reasons),
                )
        return valid

    def list_platforms(self) -> list[str]:
        return sorted(self._adapters.keys())


adapter_registry = _AdapterRegistry()
