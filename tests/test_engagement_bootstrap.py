"""Tests for bootstrap geometric-mean engagement inference."""
from __future__ import annotations

import numpy as np
import pytest

from siamquantum.stats.engagement_bootstrap import (
    bootstrap_geometric_mean,
    bootstrap_pairwise_ratio,
    log_transform_engagement,
    trend_test,
)


class TestLogTransform:
    def test_zeros(self) -> None:
        arr = np.array([0.0, 0.0], dtype=np.float64)
        result = log_transform_engagement(arr)
        assert float(result[0]) == pytest.approx(0.0)

    def test_log1p_correctness(self) -> None:
        arr = np.array([0.0, 1.0, 9.0], dtype=np.float64)
        result = log_transform_engagement(arr)
        assert float(result[0]) == pytest.approx(0.0)
        assert float(result[1]) == pytest.approx(np.log(2))
        assert float(result[2]) == pytest.approx(np.log(10))

    def test_large_values_no_overflow(self) -> None:
        arr = np.array([2_304_590.0], dtype=np.float64)  # real max from corpus
        result = log_transform_engagement(arr)
        assert np.isfinite(result[0])


class TestBootstrapGeometricMean:
    def test_constant_array_ci_narrow(self) -> None:
        # log1p(100) then exp = 101 — tests log1p round-trip correctly
        arr = log_transform_engagement(np.full(50, 100.0, dtype=np.float64))
        result = bootstrap_geometric_mean(arr, n_resamples=1000)
        assert result["geo_mean"] == pytest.approx(101.0, rel=1e-3)
        # CI must be very narrow for constant input
        width = float(result["ci_high"]) - float(result["ci_low"])
        assert width < 1.0

    def test_returns_required_keys(self) -> None:
        arr = log_transform_engagement(np.array([10.0, 20.0, 30.0], dtype=np.float64))
        result = bootstrap_geometric_mean(arr, n_resamples=500)
        for key in ("geo_mean", "ci_low", "ci_high", "n", "ci_level"):
            assert key in result

    def test_empty_array(self) -> None:
        arr = np.array([], dtype=np.float64)
        result = bootstrap_geometric_mean(arr)
        assert result["geo_mean"] == 0.0
        assert result["n"] == 0

    def test_geo_mean_within_ci(self) -> None:
        rng = np.random.default_rng(0)
        arr = log_transform_engagement(rng.exponential(scale=10000, size=60).astype(np.float64))
        result = bootstrap_geometric_mean(arr, n_resamples=2000)
        assert float(result["ci_low"]) <= float(result["geo_mean"]) <= float(result["ci_high"])


class TestBootstrapPairwiseRatio:
    def test_symmetric_ratio(self) -> None:
        rng = np.random.default_rng(42)
        a = log_transform_engagement(rng.exponential(5000, size=40).astype(np.float64))
        b = log_transform_engagement(rng.exponential(10000, size=40).astype(np.float64))
        ab = bootstrap_pairwise_ratio(a, b, n_resamples=1000)
        ba = bootstrap_pairwise_ratio(b, a, n_resamples=1000)
        # ratio(b,a) ≈ 1 / ratio(a,b)
        assert float(ab["ratio"]) == pytest.approx(1.0 / float(ba["ratio"]), rel=0.05)

    def test_b_greater_when_b_clearly_larger(self) -> None:
        a = log_transform_engagement(np.full(50, 100.0, dtype=np.float64))
        b = log_transform_engagement(np.full(50, 10000.0, dtype=np.float64))
        result = bootstrap_pairwise_ratio(a, b, n_resamples=1000)
        assert float(result["p_b_greater"]) > 0.99
        assert float(result["ratio"]) > 10.0

    def test_returns_required_keys(self) -> None:
        a = log_transform_engagement(np.array([10.0, 20.0, 30.0], dtype=np.float64))
        b = log_transform_engagement(np.array([15.0, 25.0, 35.0], dtype=np.float64))
        result = bootstrap_pairwise_ratio(a, b, n_resamples=200)
        for key in ("ratio", "ratio_ci_low", "ratio_ci_high", "p_b_greater", "interpretable"):
            assert key in result


class TestTrendTest:
    def test_monotonic_increasing_fixture(self) -> None:
        years = [2020, 2021, 2022, 2023, 2024, 2025]
        # Strictly increasing views: 100, 200, 400, 800, 1600, 3200
        log_views_per_year = [
            log_transform_engagement(np.full(30, float(100 * 2**i), dtype=np.float64))
            for i in range(6)
        ]
        result = trend_test(years, log_views_per_year)
        assert float(result["spearman_rho"]) > 0.9
        assert result["mannkendall_trend"] == "increasing"

    def test_no_trend_flat_fixture(self) -> None:
        years = [2020, 2021, 2022, 2023, 2024, 2025]
        rng = np.random.default_rng(0)
        log_views_per_year = [
            log_transform_engagement(rng.exponential(5000, size=50).astype(np.float64))
            for _ in years
        ]
        result = trend_test(years, log_views_per_year)
        assert "mannkendall_trend" in result
        assert "spearman_rho" in result
        assert "interpretation" in result

    def test_returns_required_keys(self) -> None:
        years = [2020, 2021, 2022]
        log_views = [
            log_transform_engagement(np.array([10.0, 20.0], dtype=np.float64))
            for _ in years
        ]
        result = trend_test(years, log_views)
        for key in ("mannkendall_tau", "mannkendall_p", "mannkendall_trend",
                    "spearman_rho", "spearman_p", "interpretation"):
            assert key in result
