from __future__ import annotations

import json
import logging
from itertools import combinations
from pathlib import Path

from siamquantum.db.repos import DenStreamStateRepo, EntityRepo, SourceRepo, StatsCacheRepo
from siamquantum.db.session import get_connection
from siamquantum.services.stats import (
    DenStreamClusterer,
    build_feature_vector,
    compute_engagement_levels,
    compute_ttest,
    engagement_score,
)

logger = logging.getLogger(__name__)

_TTEST_CACHE_PREFIX = "ttest:"
_CLUSTER_CACHE_KEY = "macro_clusters"


def run_stats(db_path: Path) -> dict[str, object]:
    """
    Phase 5 stats pipeline:
      1. Load/build DenStream clusterer from snapshot.
      2. Insert feature vectors for all sources with entity rows.
      3. Compute engagement_level per source via tertile within full cohort.
      4. Update entities.engagement_level in DB.
      5. Run all pairwise Welch's t-tests on YouTube engagement; cache results.
      6. Save DenStream snapshot.

    Returns summary dict.
    """
    with get_connection(db_path) as conn:
        # Load all sources that have entity classifications
        rows = conn.execute("""
            SELECT s.id, s.platform, s.published_year, s.view_count, s.like_count,
                   s.comment_count, e.content_type, e.production_type
            FROM sources s
            JOIN entities e ON s.id = e.source_id
        """).fetchall()

        # Load existing DenStream snapshot if any
        snapshot_bytes = DenStreamStateRepo(conn).get_snapshot()

    if snapshot_bytes:
        try:
            clusterer = DenStreamClusterer.from_bytes(snapshot_bytes)
            logger.info("Loaded DenStream snapshot with %d micro-clusters", len(clusterer._clusters))
        except Exception as exc:
            logger.warning("Failed to load DenStream snapshot (%s) — starting fresh", exc)
            clusterer = DenStreamClusterer()
    else:
        clusterer = DenStreamClusterer()

    # Build engagement scores and feature vectors
    source_ids: list[int] = []
    eng_scores: list[float] = []
    years: list[int] = []
    platforms: list[str] = []

    ts = 0.0
    for row in rows:
        eng = engagement_score(row["view_count"], row["like_count"], row["comment_count"])
        source_ids.append(int(row["id"]))
        eng_scores.append(eng)
        years.append(int(row["published_year"]))
        platforms.append(str(row["platform"]))

        vec = build_feature_vector(
            published_year=int(row["published_year"]),
            platform=str(row["platform"]),
            content_type=row["content_type"],
            production_type=row["production_type"],
            engagement_score=eng,
        )
        # Use year as pseudo-timestamp so older data decays relative to newer
        ts = float(row["published_year"]) * 365 * 24 * 3600
        clusterer.insert(vec, ts)

    # Compute engagement levels (tertile within full cohort)
    levels = compute_engagement_levels(eng_scores)

    # Update entities.engagement_level in DB
    updated = 0
    with get_connection(db_path) as conn:
        for sid, level in zip(source_ids, levels):
            conn.execute(
                "UPDATE entities SET engagement_level = ? WHERE source_id = ?",
                (level, sid),
            )
        conn.commit()
        updated = len(source_ids)

    # Macro-cluster snapshot
    macro_clusters = clusterer.get_macro_clusters()
    logger.info("DenStream: %d micro-clusters → %d macro-clusters", len(clusterer._clusters), len(macro_clusters))

    # Save DenStream snapshot and cache macro-clusters
    with get_connection(db_path) as conn:
        DenStreamStateRepo(conn).save_snapshot(clusterer.to_bytes())
        cache = StatsCacheRepo(conn)
        cache.set(_CLUSTER_CACHE_KEY, [mc.model_dump() for mc in macro_clusters])

    # --- Welch's t-test across all pairwise years (YouTube engagement only) ---
    # Group engagement scores by year for YouTube sources
    year_scores: dict[int, list[float]] = {}
    for sid, yr, plat, eng in zip(source_ids, years, platforms, eng_scores):
        if plat == "youtube":
            year_scores.setdefault(yr, []).append(eng)

    all_years = sorted(year_scores.keys())
    ttest_results: list[dict[str, object]] = []
    skipped_pairs: list[tuple[int, int]] = []

    with get_connection(db_path) as conn:
        cache = StatsCacheRepo(conn)
        for ya, yb in combinations(all_years, 2):
            try:
                result = compute_ttest(year_scores[ya], year_scores[yb], ya, yb)
                ttest_results.append(result.model_dump())
                cache.set(f"{_TTEST_CACHE_PREFIX}{ya}_{yb}", result.model_dump())
                logger.info(
                    "t-test %d vs %d: t=%.3f p=%.4f significant=%s",
                    ya, yb, result.t, result.p_value, result.significant,
                )
            except ValueError as exc:
                logger.warning("Skipping t-test %d vs %d: %s", ya, yb, exc)
                skipped_pairs.append((ya, yb))

    return {
        "sources_processed": len(source_ids),
        "engagement_levels_updated": updated,
        "micro_clusters": len(clusterer._clusters),
        "macro_clusters": len(macro_clusters),
        "ttest_pairs_computed": len(ttest_results),
        "ttest_pairs_skipped": len(skipped_pairs),
        "ttest_results": ttest_results,
    }
