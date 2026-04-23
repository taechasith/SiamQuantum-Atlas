from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Iterable

from siamquantum.db.repos import SourceRepo
from siamquantum.db.session import get_connection
from siamquantum.services import claude
from siamquantum.services.claude import get_usage, reset_usage
from siamquantum.services.dedup import find_duplicates

logger = logging.getLogger(__name__)


def _ensure_nlp_abstentions_table(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS nlp_abstentions (
            source_id INTEGER PRIMARY KEY REFERENCES sources(id) ON DELETE CASCADE,
            status TEXT NOT NULL,
            reason TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )


def _completed_source_ids(conn) -> set[int]:
    _ensure_nlp_abstentions_table(conn)
    rows = conn.execute(
        """
        SELECT DISTINCT t.source_id
        FROM triplets t
        JOIN sources s ON s.id = t.source_id
        UNION
        SELECT a.source_id
        FROM nlp_abstentions a
        JOIN sources s ON s.id = a.source_id
        WHERE a.status = 'abstained'
        """
    ).fetchall()
    return {int(row[0]) for row in rows}


def _mark_abstained(conn, source_id: int, reason: str) -> None:
    _ensure_nlp_abstentions_table(conn)
    conn.execute(
        """
        INSERT INTO nlp_abstentions (source_id, status, reason, updated_at)
        VALUES (?, 'abstained', ?, ?)
        ON CONFLICT(source_id) DO UPDATE SET
          status=excluded.status,
          reason=excluded.reason,
          updated_at=excluded.updated_at
        """,
        (source_id, reason, datetime.utcnow().isoformat()),
    )


def _clear_abstention(conn, source_id: int) -> None:
    _ensure_nlp_abstentions_table(conn)
    conn.execute("DELETE FROM nlp_abstentions WHERE source_id = ?", (source_id,))


def _reset_source_nlp_state(conn, source_id: int) -> None:
    _ensure_nlp_abstentions_table(conn)
    conn.execute("DELETE FROM entities WHERE source_id = ?", (source_id,))
    conn.execute("DELETE FROM triplets WHERE source_id = ?", (source_id,))
    conn.execute("DELETE FROM nlp_abstentions WHERE source_id = ?", (source_id,))


def analyze_year(
    year: int,
    db_path: Path,
    force_source_ids: Iterable[int] | None = None,
) -> dict[str, int]:
    """
    Run NLP pipeline for all sources in `year`.

    Idempotency: skip only if triplets already exist for the source.
    Broken entity-only rows are treated as incomplete and reprocessed cleanly.
    Per-source try/except: single failure increments `failed`, processing continues.

    Returns counts: {processed, skipped_already_done, skipped_no_text,
                     discarded_duplicate, failed, triplets_written, entities_written}.
    """
    reset_usage()
    counts: dict[str, int] = {
        "processed": 0,
        "skipped_already_done": 0,
        "skipped_no_text": 0,
        "discarded_duplicate": 0,
        "failed": 0,
        "abstained": 0,
        "triplets_written": 0,
        "entities_written": 0,
    }
    forced_ids = {int(source_id) for source_id in (force_source_ids or [])}

    with get_connection(db_path) as conn:
        sources = SourceRepo(conn).list_by_year(year)
        if forced_ids:
            sources = [source for source in sources if source.id in forced_ids]
        already_done = _completed_source_ids(conn)

    pending = [s for s in sources if s.id in forced_ids or s.id not in already_done]
    counts["skipped_already_done"] = len([s for s in sources if s.id not in forced_ids and s.id in already_done])

    text_sources: list[tuple[object, str]] = []
    for source in pending:
        raw_text = (source.raw_text or "").strip()
        if len(raw_text) >= 20:
            text_sources.append((source, raw_text))
            continue
        if source.id in forced_ids:
            recovery_text = (source.title or raw_text).strip()
            text_sources.append((source, recovery_text))
            continue
        text_sources.append((source, raw_text))

    min_text_len = 1 if forced_ids else 20
    with_text = [(s, t) for s, t in text_sources if len(t) >= min_text_len]
    without_text = [s for s, t in text_sources if len(t) < min_text_len]
    counts["skipped_no_text"] = len(without_text)

    discard_ids: set[int] = set()
    if len(with_text) > 1:
        texts = [t for _, t in with_text]
        ids = [s.id for s, _ in with_text]
        try:
            discard_ids = find_duplicates(texts, ids, dedupe_check_fn=claude.dedupe_check)
        except Exception as exc:
            logger.warning("dedup failed - skipping dedup pass: %s", exc)
        counts["discarded_duplicate"] = len(discard_ids - forced_ids)

    for source, text in with_text:
        was_dedupe_excluded = source.id in discard_ids
        if was_dedupe_excluded and source.id not in forced_ids:
            continue

        try:
            entity = claude.classify_entity(text, title=source.title, url=source.url)
            triplets = claude.extract_triplets(text)

            with get_connection(db_path) as conn:
                try:
                    conn.execute("BEGIN")
                    # Clean up stale partial rows from prior interrupted runs.
                    _reset_source_nlp_state(conn, source.id)
                    if entity:
                        conn.execute(
                            """
                            INSERT INTO entities
                              (source_id, content_type, production_type, area, engagement_level)
                            VALUES (?, ?, ?, ?, ?)
                            ON CONFLICT(source_id) DO UPDATE SET
                              content_type=excluded.content_type,
                              production_type=excluded.production_type,
                              area=excluded.area,
                              engagement_level=excluded.engagement_level
                            """,
                            (
                                source.id,
                                entity.content_type,
                                entity.production_type,
                                entity.area,
                                entity.engagement_level,
                            ),
                        )
                    if triplets:
                        conn.executemany(
                            """
                            INSERT INTO triplets (source_id, subject, relation, object, confidence)
                            VALUES (?, ?, ?, ?, ?)
                            """,
                            [
                                (source.id, t.subject, t.relation, t.object, t.confidence)
                                for t in triplets
                            ],
                        )
                    else:
                        abstention_reason = "non_quantum_or_low_signal"
                        if len((source.raw_text or "").strip()) < 20:
                            abstention_reason = "too_short"
                        elif was_dedupe_excluded:
                            abstention_reason = "duplicate_low_value"
                        _mark_abstained(conn, source.id, abstention_reason)
                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise

            if entity:
                counts["entities_written"] += 1
            if triplets:
                counts["triplets_written"] += len(triplets)
            else:
                counts["abstained"] += 1
            counts["processed"] += 1
            logger.info(
                "NLP source_id=%d triplets=%d entity=%s",
                source.id,
                len(triplets),
                entity.content_type if entity else "none",
            )

        except Exception as exc:
            logger.error("NLP source_id=%d failed: %s - continuing", source.id, exc)
            counts["failed"] += 1

    tok_in, tok_out = get_usage()
    counts["token_input"] = tok_in
    counts["token_output"] = tok_out
    return counts
