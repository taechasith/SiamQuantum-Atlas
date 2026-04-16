from __future__ import annotations

from collections.abc import Iterable

from siamquantum_atlas.adapters.base import RawMediaRecord
from siamquantum_atlas.utils.hashes import stable_hash


def content_fingerprint(record: RawMediaRecord) -> str:
    return stable_hash("|".join(filter(None, [record.canonical_url, record.title, record.description or "", record.full_text or ""])))


def dedupe_records(records: Iterable[RawMediaRecord]) -> list[RawMediaRecord]:
    seen: set[str] = set()
    deduped: list[RawMediaRecord] = []
    for record in records:
        fingerprint = content_fingerprint(record)
        if fingerprint in seen:
            continue
        seen.add(fingerprint)
        deduped.append(record)
    return deduped
