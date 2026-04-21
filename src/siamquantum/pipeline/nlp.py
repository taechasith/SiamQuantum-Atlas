from __future__ import annotations

import logging
from pathlib import Path

from siamquantum.db.repos import SourceRepo
from siamquantum.db.session import get_connection
from siamquantum.services import claude
from siamquantum.services.claude import get_usage, reset_usage
from siamquantum.services.dedup import find_duplicates

logger = logging.getLogger(__name__)


def analyze_year(year: int, db_path: Path) -> dict[str, int]:
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
        "triplets_written": 0,
        "entities_written": 0,
    }

    with get_connection(db_path) as conn:
        sources = SourceRepo(conn).list_by_year(year)
        # A source is complete only once triplets exist.
        already_done: set[int] = {
            int(row[0])
            for row in conn.execute(
                "SELECT DISTINCT source_id FROM triplets"
            ).fetchall()
        }

    pending = [s for s in sources if s.id not in already_done]
    counts["skipped_already_done"] = len(sources) - len(pending)

    text_sources = [(s, (s.raw_text or s.title or "").strip()) for s in pending]
    with_text = [(s, t) for s, t in text_sources if len(t) >= 20]
    without_text = [s for s, t in text_sources if len(t) < 20]
    counts["skipped_no_text"] = len(without_text)

    discard_ids: set[int] = set()
    if len(with_text) > 1:
        texts = [t for _, t in with_text]
        ids = [s.id for s, _ in with_text]
        try:
            discard_ids = find_duplicates(texts, ids, dedupe_check_fn=claude.dedupe_check)
        except Exception as exc:
            logger.warning("dedup failed - skipping dedup pass: %s", exc)
        counts["discarded_duplicate"] = len(discard_ids)

    for source, text in with_text:
        if source.id in discard_ids:
            continue

        try:
            entity = claude.classify_entity(text, title=source.title, url=source.url)
            triplets = claude.extract_triplets(text)

            # Treat entity-only or triplet-less outputs as incomplete: write neither.
            if not entity or not triplets:
                logger.info(
                    "NLP source_id=%d incomplete entity=%s triplets=%d",
                    source.id,
                    "yes" if entity else "no",
                    len(triplets),
                )
                continue

            with get_connection(db_path) as conn:
                try:
                    conn.execute("BEGIN")
                    # Clean up any stale partial rows from prior interrupted runs.
                    conn.execute("DELETE FROM entities WHERE source_id = ?", (source.id,))
                    conn.execute("DELETE FROM triplets WHERE source_id = ?", (source.id,))
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
                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise

            counts["entities_written"] += 1
            counts["triplets_written"] += len(triplets)
            counts["processed"] += 1
            logger.info(
                "NLP source_id=%d triplets=%d entity=%s",
                source.id,
                len(triplets),
                entity.content_type,
            )

        except Exception as exc:
            logger.error("NLP source_id=%d failed: %s - continuing", source.id, exc)
            counts["failed"] += 1

    tok_in, tok_out = get_usage()
    counts["token_input"] = tok_in
    counts["token_output"] = tok_out
    return counts
