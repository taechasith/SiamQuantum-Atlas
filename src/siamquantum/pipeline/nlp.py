from __future__ import annotations

import logging
from pathlib import Path

from siamquantum.db.repos import EntityRepo, SourceRepo, TripletRepo
from siamquantum.db.session import get_connection
from siamquantum.models import EntityCreate, TripletCreate
from siamquantum.services import claude
from siamquantum.services.dedup import find_duplicates

logger = logging.getLogger(__name__)


def analyze_year(year: int, db_path: Path) -> dict[str, int]:
    """
    Run NLP pipeline for all sources in `year`.

    Per source (skipping already-processed and dedup discards):
      1. extract_triplets → write to triplets table
      2. classify_entity  → write to entities table

    Dedup: TF-IDF cosine on raw_text, Claude in ambiguous zone [0.6, 0.85].
    Returns counts: {processed, skipped_already_done, skipped_no_text, discarded_duplicate}.
    """
    counts: dict[str, int] = {
        "processed": 0,
        "skipped_already_done": 0,
        "skipped_no_text": 0,
        "discarded_duplicate": 0,
    }

    with get_connection(db_path) as conn:
        sources = SourceRepo(conn).list_by_year(year)
        already_done = {
            row["source_id"]
            for row in conn.execute("SELECT source_id FROM entities").fetchall()
        }

    pending = [s for s in sources if s.id not in already_done]
    counts["skipped_already_done"] = len(sources) - len(pending)

    # Separate sources with usable text
    text_sources = [(s, (s.raw_text or s.title or "").strip()) for s in pending]
    with_text = [(s, t) for s, t in text_sources if t]
    without_text = [s for s, t in text_sources if not t]
    counts["skipped_no_text"] = len(without_text)

    # Dedup within the pending batch
    discard_ids: set[int] = set()
    if len(with_text) > 1:
        texts = [t for _, t in with_text]
        ids = [s.id for s, _ in with_text]
        discard_ids = find_duplicates(texts, ids, dedupe_check_fn=claude.dedupe_check)
        counts["discarded_duplicate"] = len(discard_ids)

    for source, text in with_text:
        if source.id in discard_ids:
            continue

        triplets = claude.extract_triplets(text)
        entity = claude.classify_entity(text, title=source.title, url=source.url)

        with get_connection(db_path) as conn:
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

        counts["processed"] += 1
        logger.info(
            "NLP source_id=%d triplets=%d entity=%s",
            source.id,
            len(triplets),
            entity.content_type if entity else "none",
        )

    return counts
