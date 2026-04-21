from __future__ import annotations

import logging
from itertools import combinations
from pathlib import Path
from typing import Any

import numpy as np
from numpy.typing import NDArray

from siamquantum.db.repos import DenStreamStateRepo, EntityRepo, SourceRepo, StatsCacheRepo
from siamquantum.db.session import get_connection
from siamquantum.services.stats import (
    DenStreamClusterer,
    build_feature_vector,
    compute_engagement_levels,
    engagement_score,
)
from siamquantum.stats.engagement_bootstrap import (
    bootstrap_geometric_mean,
    bootstrap_pairwise_ratio,
    log_transform_engagement,
    trend_test,
)

logger = logging.getLogger(__name__)

_CLUSTER_CACHE_KEY = "macro_clusters"
_BOOTSTRAP_TREND_KEY = "bootstrap_trend"
_BOOTSTRAP_YEARLY_PREFIX = "bootstrap_yearly:"
_BOOTSTRAP_PAIRWISE_PREFIX = "bootstrap_pairwise:"

_SCOPE = "thai_web_engagement"
_SCOPE_CAVEAT = (
    "Excludes academic publications in English journals and institutional reports "
    "not indexed by GDELT/YouTube. Coverage: 0.4% academic/gov sources (3 of 768)."
)


def _run_bootstrap_stats(db_path: Path) -> dict[str, object]:
    """
    Compute bootstrap geometric-mean CI + pairwise ratios + trend tests on
    YouTube view_count data. Does NOT require entities table. Idempotent.
    """
    with get_connection(db_path) as conn:
        rows = conn.execute(
            """
            SELECT published_year, view_count
            FROM sources
            WHERE platform = 'youtube' AND view_count IS NOT NULL
            ORDER BY published_year
            """
        ).fetchall()

    year_views: dict[int, list[float]] = {}
    for row in rows:
        yr = int(row["published_year"])
        year_views.setdefault(yr, []).append(float(row["view_count"]))

    all_years = sorted(year_views.keys())
    yearly_results: list[dict[str, Any]] = []
    log_views_list: list[NDArray[np.float64]] = []

    for yr in all_years:
        raw = np.array(year_views[yr], dtype=np.float64)
        log_v = log_transform_engagement(raw)
        log_views_list.append(log_v)
        gm: dict[str, Any] = bootstrap_geometric_mean(log_v)
        yearly_results.append({"year": yr, **gm})

    pairwise_results: list[dict[str, Any]] = []
    for ya, yb in combinations(all_years, 2):
        log_a = log_transform_engagement(np.array(year_views[ya], dtype=np.float64))
        log_b = log_transform_engagement(np.array(year_views[yb], dtype=np.float64))
        pair: dict[str, Any] = {"year_a": ya, "year_b": yb, **bootstrap_pairwise_ratio(log_a, log_b)}
        pairwise_results.append(pair)

    trend_result: dict[str, Any] = trend_test(all_years, log_views_list)

    with get_connection(db_path) as conn:
        cache = StatsCacheRepo(conn)
        for entry in yearly_results:
            cache.set(f"{_BOOTSTRAP_YEARLY_PREFIX}{entry['year']}", entry)
        for pair in pairwise_results:
            cache.set(f"{_BOOTSTRAP_PAIRWISE_PREFIX}{pair['year_a']}_{pair['year_b']}", pair)
        cache.set(_BOOTSTRAP_TREND_KEY, trend_result)

    return {
        "yearly_computed": len(yearly_results),
        "pairwise_computed": len(pairwise_results),
        "trend": trend_result,
    }


def run_stats(db_path: Path) -> dict[str, object]:
    """
    Stats pipeline:
      1. Bootstrap geometric-mean CI + trend on YouTube view_count (no entities needed).
      2. Load/build DenStream from entities (if any).
      3. Compute engagement_level per source via tertile.
      4. Update entities.engagement_level in DB.
      5. Save DenStream snapshot + cache macro-clusters.
    """
    # Always run bootstrap stats (independent of entities)
    bootstrap_summary = _run_bootstrap_stats(db_path)

    with get_connection(db_path) as conn:
        rows = conn.execute("""
            SELECT s.id, s.platform, s.published_year, s.view_count, s.like_count,
                   s.comment_count, e.content_type, e.production_type
            FROM sources s
            JOIN entities e ON s.id = e.source_id
        """).fetchall()
        snapshot_bytes = DenStreamStateRepo(conn).get_snapshot()

    if snapshot_bytes:
        try:
            clusterer = DenStreamClusterer.from_bytes(snapshot_bytes)
            logger.info("Loaded DenStream snapshot: %d micro-clusters", len(clusterer._clusters))
        except Exception as exc:
            logger.warning("DenStream snapshot load failed (%s) — starting fresh", exc)
            clusterer = DenStreamClusterer()
    else:
        clusterer = DenStreamClusterer()

    source_ids: list[int] = []
    eng_scores: list[float] = []

    for row in rows:
        eng = engagement_score(row["view_count"], row["like_count"], row["comment_count"])
        source_ids.append(int(row["id"]))
        eng_scores.append(eng)

        vec = build_feature_vector(
            published_year=int(row["published_year"]),
            platform=str(row["platform"]),
            content_type=row["content_type"],
            production_type=row["production_type"],
            engagement_score=eng,
        )
        ts = float(row["published_year"]) * 365 * 24 * 3600
        clusterer.insert(vec, ts)

    levels = compute_engagement_levels(eng_scores)

    updated = 0
    with get_connection(db_path) as conn:
        for sid, level in zip(source_ids, levels):
            conn.execute(
                "UPDATE entities SET engagement_level = ? WHERE source_id = ?",
                (level, sid),
            )
        conn.commit()
        updated = len(source_ids)

    macro_clusters = clusterer.get_macro_clusters()
    logger.info(
        "DenStream: %d micro-clusters -> %d macro-clusters",
        len(clusterer._clusters), len(macro_clusters),
    )

    with get_connection(db_path) as conn:
        DenStreamStateRepo(conn).save_snapshot(clusterer.to_bytes())
        StatsCacheRepo(conn).set(_CLUSTER_CACHE_KEY, [mc.model_dump() for mc in macro_clusters])

    return {
        "sources_processed": len(source_ids),
        "engagement_levels_updated": updated,
        "micro_clusters": len(clusterer._clusters),
        "macro_clusters": len(macro_clusters),
        "ttest_pairs_computed": 0,
        "ttest_pairs_skipped": 0,
        "bootstrap_yearly_computed": bootstrap_summary["yearly_computed"],
        "bootstrap_pairwise_computed": bootstrap_summary["pairwise_computed"],
        "bootstrap_trend": bootstrap_summary["trend"],
    }
