from __future__ import annotations

import sqlite3
from pathlib import Path

from siamquantum.services import claude
from siamquantum.services.claude import classify_taxonomy
from siamquantum.db.session import get_connection


def run_backfill(db_path: Path) -> dict[str, int | float]:
    """Backfill media_format and user_intent for all entities that lack them."""
    claude.reset_usage()

    conn = sqlite3.connect(db_path)
    rows = conn.execute("""
        SELECT e.source_id, s.title, s.raw_text, s.url
        FROM entities e
        JOIN sources s ON s.id = e.source_id
        WHERE e.media_format IS NULL OR e.user_intent IS NULL
        ORDER BY e.source_id
    """).fetchall()
    conn.close()

    counts = {"rows_seen": len(rows), "rows_processed": 0, "rows_skipped_existing": 0, "rows_failed": 0}

    for source_id, title, raw_text, url in rows:
        text = ((raw_text or "").strip() or (title or "")).strip()
        if not text:
            counts["rows_failed"] += 1
            continue
        try:
            result = classify_taxonomy(text[:2000], title=title, url=url or "")
            if result is None:
                counts["rows_failed"] += 1
                continue
            with get_connection(db_path) as conn:
                conn.execute("""
                    UPDATE entities
                    SET media_format=?, media_format_detail=?, user_intent=?, thai_cultural_angle=?
                    WHERE source_id=?
                """, (
                    result.media_format,
                    result.media_format_detail,
                    result.user_intent,
                    result.thai_cultural_angle,
                    source_id,
                ))
                conn.commit()
            counts["rows_processed"] += 1
        except Exception:
            counts["rows_failed"] += 1

    tok_in, tok_out = claude.get_usage()
    counts["actual_cost_usd"] = round(tok_in * 3.0 / 1_000_000 + tok_out * 15.0 / 1_000_000, 4)
    return counts
