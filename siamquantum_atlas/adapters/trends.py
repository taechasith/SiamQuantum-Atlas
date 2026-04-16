from __future__ import annotations

from typing import Any

from siamquantum_atlas.adapters.base import RawMediaRecord, SourceAdapter


class TrendsAdapter(SourceAdapter):
    name = "trends"

    def fetch(self, **_: Any) -> list[RawMediaRecord]:
        return []
