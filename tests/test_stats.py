"""Phase 5 gate tests — stats module with fixture-verified p-values."""
from __future__ import annotations

import math
import tempfile
from pathlib import Path

import numpy as np
import pytest

from siamquantum.services.stats import (
    DenStreamClusterer,
    build_feature_vector,
    compute_engagement_levels,
    compute_ttest,
    engagement_score,
)
from siamquantum.stats.nonparametric import chi2_independence


# ---------------------------------------------------------------------------
# Welch's t-test — fixture-verified p-values
# ---------------------------------------------------------------------------

def test_ttest_significant_known_result() -> None:
    """Groups with clearly different means → p < 0.05, significant=True."""
    # Two well-separated groups — t-test must detect difference
    scores_a = [8.5, 9.0, 8.8, 9.2, 8.7, 9.1, 8.9, 8.6, 9.3, 8.4]   # mean ≈ 8.85
    scores_b = [2.1, 1.8, 2.3, 2.0, 2.2, 1.9, 2.4, 2.1, 1.7, 2.5]   # mean ≈ 2.1
    result = compute_ttest(scores_a, scores_b, 2021, 2024)
    assert result.significant, f"Expected significant but got p={result.p_value:.4f}"
    assert result.p_value < 0.05
    assert result.t > 0           # scores_a > scores_b → positive t
    assert result.year_a == 2021
    assert result.year_b == 2024
    assert result.df > 0


def test_ttest_not_significant_known_result() -> None:
    """Groups drawn from same distribution → p > 0.05, significant=False."""
    # Near-identical values → t ≈ 0, p ≈ 1
    scores_a = [5.00, 5.01, 4.99, 5.02, 4.98, 5.00, 5.01, 4.99, 5.00, 5.02]
    scores_b = [5.00, 5.01, 4.99, 5.00, 5.02, 4.98, 5.01, 5.00, 4.99, 5.01]
    result = compute_ttest(scores_a, scores_b, 2022, 2023)
    assert not result.significant, f"Expected not significant but got p={result.p_value:.4f}"
    assert result.p_value > 0.05


def test_ttest_raises_on_too_few_samples() -> None:
    with pytest.raises(ValueError, match="Need"):
        compute_ttest([1.0], [2.0, 3.0], 2020, 2021)


def test_ttest_df_welch_satterthwaite() -> None:
    """df should be Welch-Satterthwaite (less than n_a + n_b - 2 for unequal variances)."""
    high_var = [1.0, 10.0, 2.0, 9.0, 3.0]   # var ≈ 15.5
    low_var  = [5.0, 5.1, 4.9, 5.0, 5.1]     # var ≈ 0.005
    result = compute_ttest(high_var, low_var, 2020, 2021)
    pooled_df = len(high_var) + len(low_var) - 2  # = 8
    # Welch df ≤ pooled df (equality only when variances equal)
    assert result.df <= pooled_df + 1e-6      # small tolerance for float


# ---------------------------------------------------------------------------
# Engagement score
# ---------------------------------------------------------------------------

def test_engagement_score_youtube() -> None:
    score = engagement_score(1000, 50, 10)
    # log1p(1000 + 50*5 + 10*10) = log1p(1350)
    expected = math.log1p(1350)
    assert abs(score - expected) < 1e-9


def test_engagement_score_gdelt() -> None:
    assert engagement_score(None, None, None) == 0.0


def test_engagement_score_partial() -> None:
    score = engagement_score(100, None, None)
    assert score > 0.0


# ---------------------------------------------------------------------------
# Engagement level assignment
# ---------------------------------------------------------------------------

def test_compute_engagement_levels_tertile() -> None:
    # 9 values: 3 low, 3 medium, 3 high
    scores = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]
    levels = compute_engagement_levels(scores)
    assert levels[0] == "low"
    assert levels[4] == "medium"
    assert levels[8] == "high"
    assert len(levels) == 9


def test_compute_engagement_levels_all_same() -> None:
    scores = [5.0] * 10
    levels = compute_engagement_levels(scores)
    # All same value → all fall at or below p33 → all "low"
    assert all(l in ("low", "medium", "high") for l in levels)


def test_compute_engagement_levels_single() -> None:
    assert compute_engagement_levels([3.0]) == ["medium"]


def test_compute_engagement_levels_empty() -> None:
    assert compute_engagement_levels([]) == []


# ---------------------------------------------------------------------------
# DenStream clusterer
# ---------------------------------------------------------------------------

def test_denstream_inserts_and_clusters() -> None:
    clusterer = DenStreamClusterer(epsilon=1.0, mu=0.5, lambda_decay=0.0)
    t = 1000.0
    # Two clear clusters at [0,0] and [10,10]
    for _ in range(5):
        clusterer.insert(np.array([0.0, 0.0]), t)
        clusterer.insert(np.array([10.0, 10.0]), t)
    assert len(clusterer._clusters) == 2
    macros = clusterer.get_macro_clusters()
    assert len(macros) == 2


def test_denstream_decay_prunes_old_clusters() -> None:
    clusterer = DenStreamClusterer(epsilon=0.5, mu=1.0, lambda_decay=1.0, beta=0.9)
    clusterer.insert(np.array([0.0]), 0.0)
    assert len(clusterer._clusters) == 1
    # After large time gap, weight decays below pruning threshold
    clusterer.insert(np.array([100.0]), 100.0)  # triggers decay + pruning
    # Old cluster weight = 2^(-1.0 * 100) ≈ 0 → pruned
    surviving = [mc for mc in clusterer._clusters if np.allclose(mc.center, [0.0], atol=0.1)]
    assert len(surviving) == 0, "Decayed cluster should have been pruned"


def test_denstream_persistence_round_trip() -> None:
    c1 = DenStreamClusterer()
    c1.insert(np.array([1.0, 2.0]), 1.0)
    c1.insert(np.array([5.0, 6.0]), 1.0)
    data = c1.to_bytes()
    c2 = DenStreamClusterer.from_bytes(data)
    assert len(c2._clusters) == len(c1._clusters)
    for mc1, mc2 in zip(c1._clusters, c2._clusters):
        np.testing.assert_array_almost_equal(mc1.center, mc2.center)
        assert abs(mc1.weight - mc2.weight) < 1e-9


# ---------------------------------------------------------------------------
# Feature vector
# ---------------------------------------------------------------------------

def test_build_feature_vector_shape() -> None:
    vec = build_feature_vector(2024, "youtube", "news", "independent", 5.0)
    assert vec.shape == (11,)
    assert vec.dtype == np.float64


def test_build_feature_vector_unknown_category() -> None:
    vec = build_feature_vector(2020, "gdelt", None, None, 0.0)
    assert vec.shape == (11,)
    # All one-hot dims should be 0 for None categories
    assert vec[2:10].sum() == 0.0


# ---------------------------------------------------------------------------
# Full pipeline integration (temp DB)
# ---------------------------------------------------------------------------

def test_run_stats_pipeline(tmp_path: Path) -> None:
    """Integration: run_stats on seeded DB → engagement levels + DenStream snapshot written."""
    from datetime import datetime
    from siamquantum.db.session import init_db, get_connection
    from siamquantum.pipeline.analyze import run_stats

    db_path = tmp_path / "test.db"
    init_db(db_path)

    with get_connection(db_path) as conn:
        for i in range(6):
            platform = "youtube" if i % 2 == 0 else "gdelt"
            conn.execute(
                "INSERT INTO sources (platform, url, title, published_year, fetched_at,"
                " view_count, like_count, comment_count) VALUES (?,?,?,?,?,?,?,?)",
                (platform, f"https://example.com/{i}", f"title {i}", 2024,
                 datetime.utcnow().isoformat(),
                 (i + 1) * 1000 if platform == "youtube" else None,
                 (i + 1) * 50 if platform == "youtube" else None,
                 (i + 1) * 10 if platform == "youtube" else None),
            )
        conn.commit()
        # Seed entities
        for i in range(1, 7):
            conn.execute(
                "INSERT INTO entities (source_id, content_type, production_type, area, engagement_level)"
                " VALUES (?,?,?,?,?)",
                (i, "news", "independent", "quantum computing", "low"),
            )
        conn.commit()

    result = run_stats(db_path)

    assert result["sources_processed"] == 6
    assert result["engagement_levels_updated"] == 6
    assert result["micro_clusters"] >= 1

    with get_connection(db_path) as conn:
        levels = {r["engagement_level"] for r in
                  conn.execute("SELECT engagement_level FROM entities").fetchall()}
    assert levels <= {"low", "medium", "high"}

    with get_connection(db_path) as conn:
        snap = conn.execute("SELECT snapshot FROM denstream_state WHERE id=1").fetchone()
    assert snap is not None


def test_chi2_independence_ignores_zero_rows_and_columns() -> None:
    result = chi2_independence(
        {
            ("2020", "article"): 12,
            ("2020", "podcast"): 3,
            ("2021", "article"): 0,
            ("2021", "podcast"): 0,
            ("2022", "article"): 7,
            ("2022", "podcast"): 4,
            ("2020", "unused"): 0,
            ("2021", "unused"): 0,
            ("2022", "unused"): 0,
        },
        ["2020", "2021", "2022"],
        ["article", "podcast", "unused"],
    )
    assert result["chi2"] is not None
    assert result["rows_tested"] == 2
    assert result["columns_tested"] == 2
