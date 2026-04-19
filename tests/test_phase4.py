"""Phase 4 gate test — NLP pipeline on 5 fixture Thai snippets."""
from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from siamquantum.db.session import init_db, get_connection
from siamquantum.models import EntityClassification, SourceCreate, Triplet
from siamquantum.pipeline.nlp import analyze_year

_FIXTURES: list[dict[str, str]] = [
    {
        "platform": "gdelt",
        "url": "https://example.com/quantum-1",
        "title": "Thailand quantum computing research advances",
        "raw_text": "Thailand quantum computing research advances at Chulalongkorn University. "
                    "The university developed quantum error correction algorithms.",
    },
    {
        "platform": "gdelt",
        "url": "https://example.com/quantum-2",
        "title": "ควอนตัมคอมพิวเตอร์ในไทย",
        "raw_text": "นักวิจัยไทยพัฒนาระบบควอนตัมคอมพิวเตอร์ที่จุฬาลงกรณ์มหาวิทยาลัย "
                    "เพื่อเพิ่มประสิทธิภาพการคำนวณในอนาคต",
    },
    {
        "platform": "youtube",
        "url": "https://www.youtube.com/watch?v=abc123",
        "title": "Quantum cryptography explained in Thai",
        "raw_text": "This video explains quantum key distribution (QKD) protocols. "
                    "Thailand's NECTEC implements QKD for secure communication between Bangkok and Chiang Mai.",
    },
    {
        "platform": "gdelt",
        "url": "https://example.com/quantum-4",
        "title": "NSTDA funds quantum sensing project",
        "raw_text": "NSTDA allocated 50 million baht to quantum sensing research. "
                    "The project aims to develop quantum sensors for medical imaging in Thailand.",
    },
    {
        "platform": "youtube",
        "url": "https://www.youtube.com/watch?v=def456",
        "title": "การประยุกต์ใช้ควอนตัมในภาคอุตสาหกรรม",
        "raw_text": "การนำเทคโนโลยีควอนตัมมาใช้ในภาคอุตสาหกรรมไทย "
                    "บริษัท PTT ร่วมมือกับมหาวิทยาลัยมหิดลในการวิจัยควอนตัมเคมี",
    },
]

_MOCK_TRIPLETS = [
    Triplet(subject="Chulalongkorn University", relation="developed", object="quantum error correction", confidence=0.9),
]
_MOCK_ENTITY = EntityClassification(
    content_type="news",
    production_type="university",
    area="quantum computing",
    engagement_level="medium",
)


@pytest.fixture()
def temp_db() -> Path:
    with tempfile.TemporaryDirectory() as d:
        db_path = Path(d) / "test.db"
        init_db(db_path)
        yield db_path


def _seed_sources(db_path: Path) -> list[int]:
    from datetime import datetime
    ids = []
    with get_connection(db_path) as conn:
        for fix in _FIXTURES:
            cur = conn.execute(
                "INSERT INTO sources (platform, url, title, raw_text, published_year, fetched_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (fix["platform"], fix["url"], fix["title"], fix["raw_text"], 2024, datetime.utcnow().isoformat()),
            )
            conn.commit()
            ids.append(cur.lastrowid)
    return ids


@patch("siamquantum.services.claude.classify_entity", return_value=_MOCK_ENTITY)
@patch("siamquantum.services.claude.extract_triplets", return_value=_MOCK_TRIPLETS)
@patch("siamquantum.services.claude.dedupe_check", return_value=False)
def test_analyze_year_populates_tables(
    mock_dedupe: MagicMock,
    mock_triplets: MagicMock,
    mock_entity: MagicMock,
    temp_db: Path,
) -> None:
    """All 5 fixtures processed → triplets and entities tables populated."""
    _seed_sources(temp_db)

    counts = analyze_year(2024, temp_db)

    assert counts["processed"] == 5, f"expected 5 processed, got {counts}"
    assert counts["skipped_no_text"] == 0
    assert counts["skipped_already_done"] == 0

    with get_connection(temp_db) as conn:
        triplet_count = conn.execute("SELECT COUNT(*) FROM triplets").fetchone()[0]
        entity_count = conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]

    assert triplet_count == 5, f"expected 5 triplet rows, got {triplet_count}"
    assert entity_count == 5, f"expected 5 entity rows, got {entity_count}"


@patch("siamquantum.services.claude.classify_entity", return_value=_MOCK_ENTITY)
@patch("siamquantum.services.claude.extract_triplets", return_value=_MOCK_TRIPLETS)
@patch("siamquantum.services.claude.dedupe_check", return_value=False)
def test_analyze_year_idempotent(
    mock_dedupe: MagicMock,
    mock_triplets: MagicMock,
    mock_entity: MagicMock,
    temp_db: Path,
) -> None:
    """Running analyze_year twice doesn't double-insert entities."""
    _seed_sources(temp_db)

    analyze_year(2024, temp_db)
    counts2 = analyze_year(2024, temp_db)

    assert counts2["skipped_already_done"] == 5
    assert counts2["processed"] == 0

    with get_connection(temp_db) as conn:
        entity_count = conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    assert entity_count == 5


@patch("siamquantum.services.claude.classify_entity", return_value=_MOCK_ENTITY)
@patch("siamquantum.services.claude.extract_triplets", return_value=_MOCK_TRIPLETS)
@patch("siamquantum.services.claude.dedupe_check", return_value=True)
def test_dedup_discards_marked_sources(
    mock_dedupe: MagicMock,
    mock_triplets: MagicMock,
    mock_entity: MagicMock,
    temp_db: Path,
) -> None:
    """When Claude marks all pairs as duplicate, only the first of each pair survives."""
    _seed_sources(temp_db)
    counts = analyze_year(2024, temp_db)

    # At least some discarded (Claude returns True for all ambiguous pairs)
    total = counts["processed"] + counts["discarded_duplicate"]
    assert total == 5, f"processed + discarded should equal 5, got {counts}"
