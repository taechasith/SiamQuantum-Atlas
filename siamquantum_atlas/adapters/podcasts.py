from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from siamquantum_atlas.adapters.base import RawMediaRecord, SourceAdapter


class PodcastAdapter(SourceAdapter):
    name = "podcast"

    def fetch(self, sample_path: Path | None = None, **_: Any) -> list[RawMediaRecord]:
        if not sample_path:
            return []
        payload = json.loads(sample_path.read_text(encoding="utf-8"))
        return [
            RawMediaRecord(
                adapter=self.name,
                platform=item["platform"],
                media_type=item["media_type"],
                title=item["title"],
                description=item.get("description"),
                full_text=item.get("full_text"),
                url=item["url"],
                canonical_url=item["canonical_url"],
                published_at=datetime.fromisoformat(item["published_at"]) if item.get("published_at") else None,
                language_detected=item.get("language_detected"),
                domain=item.get("domain"),
                raw_payload=item,
                rank_proxy=item.get("rank_proxy"),
                popularity_proxy=item.get("popularity_proxy"),
            )
            for item in payload
            if item["platform"] == "podcast" and item.get("language_detected") == "th"
        ]
