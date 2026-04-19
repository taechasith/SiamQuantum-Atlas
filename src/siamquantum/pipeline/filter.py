from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

from siamquantum.db.session import get_connection
from siamquantum.services.claude import is_relevant_source, get_usage, reset_usage

logger = logging.getLogger(__name__)


def backfill_relevance(db_path: Path) -> dict[str, int]:
    """
    Run relevance classifier on all sources not yet checked.
    Updates is_quantum_tech, is_thailand_related, quantum_domain,
    rejection_reason, relevance_confidence, relevance_checked_at on sources.
    Returns count summary.
    """
    reset_usage()

    with get_connection(db_path) as conn:
        rows = conn.execute("""
            SELECT id, platform, title, raw_text
            FROM sources
            WHERE relevance_checked_at IS NULL
            ORDER BY id
        """).fetchall()

    counts = {
        "checked": 0,
        "accepted": 0,
        "rejected_not_quantum": 0,
        "rejected_not_thai": 0,
        "rejected_both": 0,
        "failed": 0,
    }

    for row in rows:
        source_id: int = row["id"]
        verdict = is_relevant_source(
            title=row["title"],
            raw_text=row["raw_text"],
            platform=row["platform"],
        )

        if verdict is None:
            counts["failed"] += 1
            logger.warning("relevance verdict None for source_id=%d", source_id)
            continue

        now = datetime.now(timezone.utc).isoformat()
        with get_connection(db_path) as conn:
            conn.execute(
                """
                UPDATE sources SET
                    is_quantum_tech = ?,
                    is_thailand_related = ?,
                    quantum_domain = ?,
                    rejection_reason = ?,
                    relevance_confidence = ?,
                    relevance_checked_at = ?
                WHERE id = ?
                """,
                (
                    1 if verdict.is_quantum_tech else 0,
                    1 if verdict.is_thailand_related else 0,
                    verdict.quantum_domain,
                    verdict.rejection_reason,
                    verdict.confidence,
                    now,
                    source_id,
                ),
            )
            conn.commit()

        counts["checked"] += 1
        if verdict.is_quantum_tech and verdict.is_thailand_related:
            counts["accepted"] += 1
        elif not verdict.is_quantum_tech and not verdict.is_thailand_related:
            counts["rejected_both"] += 1
        elif not verdict.is_quantum_tech:
            counts["rejected_not_quantum"] += 1
        else:
            counts["rejected_not_thai"] += 1

    tok_in, tok_out = get_usage()
    counts["token_input"] = tok_in
    counts["token_output"] = tok_out
    # Sonnet pricing: $3/MTok input, $15/MTok output
    counts["cost_usd_cents"] = round(
        (tok_in * 3.0 / 1_000_000 + tok_out * 15.0 / 1_000_000) * 100, 2
    )

    return counts
