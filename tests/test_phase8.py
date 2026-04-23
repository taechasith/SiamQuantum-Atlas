from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

from siamquantum.db.repos import GeoRepo
from siamquantum.db.session import get_connection, init_db
from siamquantum.models import EntityClassification, GeoCreate, Triplet
from siamquantum.pipeline.integrity import run_integrity_audit
from siamquantum.pipeline.nlp import analyze_year


def _seed_source(db_path: Path, *, raw_text: str = "Thailand quantum programme builds secure links.") -> int:
    with get_connection(db_path) as conn:
        cur = conn.execute(
            """
            INSERT INTO sources (platform, url, title, raw_text, published_year, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                "gdelt",
                f"https://example.com/{datetime.utcnow().timestamp()}",
                "Quantum Thailand",
                raw_text,
                2024,
                datetime.utcnow().isoformat(),
            ),
        )
        conn.commit()
        return int(cur.lastrowid)


def test_geo_repo_uses_asn_org_when_isp_missing(tmp_path: Path) -> None:
    db_path = tmp_path / "phase8_geo.db"
    init_db(db_path)
    source_id = _seed_source(db_path)

    with get_connection(db_path) as conn:
        GeoRepo(conn).upsert(
            GeoCreate(
                source_id=source_id,
                ip="1.1.1.1",
                lat=13.7,
                lng=100.5,
                isp=None,
                asn_org="Example Transit ASN",
                is_cdn_resolved=False,
            )
        )
        row = conn.execute("SELECT isp, asn_org FROM geo WHERE source_id = ?", (source_id,)).fetchone()

    assert row is not None
    assert row["isp"] == "Example Transit ASN"
    assert row["asn_org"] == "Example Transit ASN"


@patch("siamquantum.services.claude.classify_entity")
@patch("siamquantum.services.claude.extract_triplets")
@patch("siamquantum.services.claude.dedupe_check", return_value=False)
def test_analyze_year_cleans_stale_abstention_before_reprocess(
    mock_dedupe: MagicMock,
    mock_triplets: MagicMock,
    mock_entity: MagicMock,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "phase8_nlp.db"
    init_db(db_path)
    source_id = _seed_source(db_path)

    with get_connection(db_path) as conn:
        conn.execute(
            """
            INSERT INTO nlp_abstentions (source_id, status, reason, updated_at)
            VALUES (?, 'abstained', 'too_short', ?)
            """,
            (source_id, datetime.utcnow().isoformat()),
        )
        conn.commit()

    mock_entity.return_value = EntityClassification(
        content_type="news",
        production_type="university",
        area="quantum communication",
        engagement_level="medium",
    )
    mock_triplets.return_value = [
        Triplet(subject="Thailand", relation="builds", object="quantum network", confidence=0.92)
    ]

    counts = analyze_year(2024, db_path, force_source_ids=[source_id])

    assert counts["processed"] == 1
    with get_connection(db_path) as conn:
        abstention = conn.execute(
            "SELECT COUNT(*) FROM nlp_abstentions WHERE source_id = ?",
            (source_id,),
        ).fetchone()[0]
        triplets = conn.execute(
            "SELECT COUNT(*) FROM triplets WHERE source_id = ?",
            (source_id,),
        ).fetchone()[0]

    assert abstention == 0
    assert triplets == 1


def test_integrity_audit_reports_and_fixes_duplicate_links_and_stale_abstentions(tmp_path: Path) -> None:
    db_path = tmp_path / "phase8_audit.db"
    init_db(db_path)
    source_id = _seed_source(db_path)

    with get_connection(db_path) as conn:
        conn.execute(
            "INSERT INTO geo (source_id, isp, asn_org) VALUES (?, ?, ?)",
            (source_id, None, "Recovered ASN ISP"),
        )
        conn.execute(
            """
            INSERT INTO triplets (source_id, subject, relation, object, confidence)
            VALUES (?, ?, ?, ?, ?), (?, ?, ?, ?, ?)
            """,
            (
                source_id, "Thailand ", "builds", "Quantum Network", 0.9,
                source_id, "thailand", "builds", "quantum network ", 0.7,
            ),
        )
        conn.execute(
            """
            INSERT INTO nlp_abstentions (source_id, status, reason, updated_at)
            VALUES (?, 'abstained', 'duplicate_low_value', ?)
            """,
            (source_id, datetime.utcnow().isoformat()),
        )
        conn.commit()

    report = run_integrity_audit(db_path, fix=True)

    assert report["geo_isp_sync_candidates"] == 1
    assert report["stale_abstentions_with_triplets"] == 1
    assert report["duplicate_graph_links"]["groups"] == 1
    assert report["fixed"]["geo_isp_synced"] == 1
    assert report["fixed"]["stale_abstentions_removed"] == 1
    assert report["fixed"]["duplicate_graph_links_removed"] == 1

    with get_connection(db_path) as conn:
        isp = conn.execute("SELECT isp FROM geo WHERE source_id = ?", (source_id,)).fetchone()[0]
        abstentions = conn.execute("SELECT COUNT(*) FROM nlp_abstentions").fetchone()[0]
        triplets = conn.execute("SELECT COUNT(*) FROM triplets").fetchone()[0]

    assert isp == "Recovered ASN ISP"
    assert abstentions == 0
    assert triplets == 1


def test_integrity_audit_detects_orphans(tmp_path: Path) -> None:
    db_path = tmp_path / "phase8_orphans.db"
    init_db(db_path)

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute(
            """
            INSERT INTO entities (source_id, content_type, production_type, area, engagement_level)
            VALUES (999, 'news', 'independent', 'quantum', 'low')
            """
        )
        conn.execute(
            """
            INSERT INTO triplets (source_id, subject, relation, object, confidence)
            VALUES (999, 'orphan', 'references', 'source', 0.5)
            """
        )
        conn.execute(
            """
            INSERT INTO nlp_abstentions (source_id, status, reason, updated_at)
            VALUES (999, 'abstained', 'missing_source', ?)
            """,
            (datetime.utcnow().isoformat(),),
        )
        conn.commit()
    finally:
        conn.close()

    report = run_integrity_audit(db_path)

    assert report["orphans"] == {"entities": 1, "triplets": 1, "abstentions": 1}
