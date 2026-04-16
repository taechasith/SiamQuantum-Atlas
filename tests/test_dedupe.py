from datetime import datetime

from siamquantum_atlas.adapters.base import RawMediaRecord
from siamquantum_atlas.ingestion.dedupe import dedupe_records


def test_dedupe_records_by_fingerprint() -> None:
    record = RawMediaRecord(
        adapter="gdelt",
        platform="gdelt_news",
        media_type="article",
        title="Quantum",
        description="Desc",
        full_text="Body",
        url="https://example.org/a",
        canonical_url="https://example.org/a",
        published_at=datetime.utcnow(),
        language_detected="th",
        domain="example.org",
    )
    assert len(dedupe_records([record, record])) == 1
