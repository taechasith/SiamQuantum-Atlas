from __future__ import annotations

import logging
from pathlib import Path

from siamquantum.db.repos import EntityRepo, SourceRepo, TripletRepo
from siamquantum.db.session import get_connection
from siamquantum.models import EntityCreate, TripletCreate
from siamquantum.services import claude
from siamquantum.services.claude import get_usage, reset_usage
from siamquantum.services.dedup import find_duplicates

logger = logging.getLogger(__name__)


def analyze_year(year: int, db_path: Path) -> dict[str, int]:
    """
    Run NLP pipeline for all sources in `year`.

    Idempotency: skip if source_id exists in EITHER entities OR triplets.
    Writes entity row FIRST (as a started-marker), then triplets.
    Per-source try/except — single failure increments 'failed', processing continues.

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
        # Skip if source_id exists in EITHER table to prevent duplicate triplets
        already_done: set[int] = {
            int(row[0])
            for row in conn.execute(
                "SELECT source_id FROM entities UNION SELECT DISTINCT source_id FROM triplets"
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
            logger.warning("dedup failed — skipping dedup pass: %s", exc)
        counts["discarded_duplicate"] = len(discard_ids)

    for source, text in with_text:
        if source.id in discard_ids:
            continue

        try:
            entity = claude.classify_entity(text, title=source.title, url=source.url)
            triplets = claude.extract_triplets(text)

            with get_connection(db_path) as conn:
                # Entity written first: acts as idempotency marker
                if entity:
                    EntityRepo(conn).upsert(
                        EntityCreate(
                            source_id=source.id,
                            content_type=entity.content_type,
                            production_type=entity.production_type,
                            area=entity.area,
                            engagement_level=entity.engagement_level,
                        )
                    )
                    counts["entities_written"] += 1

                if triplets:
                    TripletRepo(conn).insert_many(
                        [
                            TripletCreate(
                                source_id=source.id,
                                subject=t.subject,
                                relation=t.relation,
                                object=t.object,
                                confidence=t.confidence,
                            )
                            for t in triplets
                        ]
                    )
                    counts["triplets_written"] += len(triplets)

            counts["processed"] += 1
            logger.info(
                "NLP source_id=%d triplets=%d entity=%s",
                source.id,
                len(triplets),
                entity.content_type if entity else "none",
            )

        except Exception as exc:
            logger.error("NLP source_id=%d failed: %s — continuing", source.id, exc)
            counts["failed"] += 1

    tok_in, tok_out = get_usage()
    counts["token_input"] = tok_in
    counts["token_output"] = tok_out
    return counts
