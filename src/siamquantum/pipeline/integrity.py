from __future__ import annotations

from pathlib import Path
from typing import Any

from siamquantum.db.session import get_connection


def run_integrity_audit(db_path: Path, *, fix: bool = False) -> dict[str, Any]:
    """
    Run a compact integrity audit and optionally repair deterministic issues.

    Repairs are intentionally narrow:
    - sync geo.isp from geo.asn_org when ISP is missing
    - remove abstentions for sources that already have triplets
    - collapse exact duplicate normalized graph links, keeping one row
    """
    with get_connection(db_path) as conn:
        orphans = {
            "entities": int(conn.execute(
                """
                SELECT COUNT(*) FROM entities e
                LEFT JOIN sources s ON s.id = e.source_id
                WHERE s.id IS NULL
                """
            ).fetchone()[0]),
            "triplets": int(conn.execute(
                """
                SELECT COUNT(*) FROM triplets t
                LEFT JOIN sources s ON s.id = t.source_id
                WHERE s.id IS NULL
                """
            ).fetchone()[0]),
            "abstentions": int(conn.execute(
                """
                SELECT COUNT(*) FROM nlp_abstentions a
                LEFT JOIN sources s ON s.id = a.source_id
                WHERE s.id IS NULL
                """
            ).fetchone()[0]),
        }

        duplicate_groups = conn.execute(
            """
            WITH normalized AS (
                SELECT
                    MIN(id) AS keep_id,
                    COUNT(*) AS row_count
                FROM triplets
                GROUP BY
                    source_id,
                    lower(trim(subject)),
                    lower(trim(relation)),
                    lower(trim(object))
                HAVING COUNT(*) > 1
            )
            SELECT
                COUNT(*) AS groups_count,
                COALESCE(SUM(row_count - 1), 0) AS duplicate_rows
            FROM normalized
            """
        ).fetchone()

        geo_sync_candidates = int(conn.execute(
            "SELECT COUNT(*) FROM geo WHERE isp IS NULL AND asn_org IS NOT NULL"
        ).fetchone()[0])

        stale_abstentions = int(conn.execute(
            """
            SELECT COUNT(*)
            FROM nlp_abstentions a
            WHERE EXISTS (
                SELECT 1 FROM triplets t WHERE t.source_id = a.source_id
            )
            """
        ).fetchone()[0])

        fixed = {
            "geo_isp_synced": 0,
            "stale_abstentions_removed": 0,
            "duplicate_graph_links_removed": 0,
        }

        if fix:
            fixed["geo_isp_synced"] = int(conn.execute(
                """
                UPDATE geo
                SET isp = asn_org
                WHERE isp IS NULL AND asn_org IS NOT NULL
                RETURNING source_id
                """
            ).fetchall().__len__())

            fixed["stale_abstentions_removed"] = int(conn.execute(
                """
                DELETE FROM nlp_abstentions
                WHERE source_id IN (
                    SELECT a.source_id
                    FROM nlp_abstentions a
                    WHERE EXISTS (
                        SELECT 1 FROM triplets t WHERE t.source_id = a.source_id
                    )
                )
                RETURNING source_id
                """
            ).fetchall().__len__())

            fixed["duplicate_graph_links_removed"] = int(conn.execute(
                """
                DELETE FROM triplets
                WHERE id IN (
                    WITH ranked AS (
                        SELECT
                            id,
                            ROW_NUMBER() OVER (
                                PARTITION BY
                                    source_id,
                                    lower(trim(subject)),
                                    lower(trim(relation)),
                                    lower(trim(object))
                                ORDER BY id
                            ) AS rn
                        FROM triplets
                    )
                    SELECT id FROM ranked WHERE rn > 1
                )
                RETURNING id
                """
            ).fetchall().__len__())
            conn.commit()

    return {
        "geo_isp_sync_candidates": geo_sync_candidates,
        "orphans": orphans,
        "stale_abstentions_with_triplets": stale_abstentions,
        "duplicate_graph_links": {
            "groups": int(duplicate_groups["groups_count"]),
            "extra_rows": int(duplicate_groups["duplicate_rows"]),
        },
        "fixed": fixed,
    }
