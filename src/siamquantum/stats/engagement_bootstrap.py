from __future__ import annotations

from typing import Any

import numpy as np
from numpy.typing import NDArray
from scipy import stats as scipy_stats  # type: ignore[import-untyped]

try:
    import pymannkendall as mk  # type: ignore[import-untyped]

    _MK_AVAILABLE = True
except ImportError:
    _MK_AVAILABLE = False


def log_transform_engagement(views: NDArray[np.float64]) -> NDArray[np.float64]:
    """Apply log1p transform. Handles zeros: log1p(0) = 0."""
    return np.log1p(views)


def bootstrap_geometric_mean(
    log_views: NDArray[np.float64],
    n_resamples: int = 10_000,
    ci: float = 0.90,
) -> dict[str, Any]:
    """
    Bootstrap CI for geometric mean = exp(mean(log_views)).
    Returns geo_mean, ci_low, ci_high in original (non-log) scale.
    """
    n = int(len(log_views))
    if n == 0:
        return {"geo_mean": 0.0, "ci_low": 0.0, "ci_high": 0.0, "n": 0, "ci_level": ci}
    if n == 1:
        geo = float(np.exp(log_views[0]))
        return {"geo_mean": geo, "ci_low": geo, "ci_high": geo, "n": 1, "ci_level": ci}

    def _mean(x: NDArray[np.float64]) -> np.floating[Any]:
        return np.mean(x)

    result = scipy_stats.bootstrap(
        (log_views,),
        _mean,
        n_resamples=n_resamples,
        confidence_level=ci,
        method="percentile",
        random_state=42,
        vectorized=False,
    )
    geo_mean = float(np.exp(np.mean(log_views)))
    ci_low = float(np.exp(result.confidence_interval.low))
    ci_high = float(np.exp(result.confidence_interval.high))
    return {"geo_mean": geo_mean, "ci_low": ci_low, "ci_high": ci_high, "n": n, "ci_level": ci}


def bootstrap_pairwise_ratio(
    log_views_a: NDArray[np.float64],
    log_views_b: NDArray[np.float64],
    n_resamples: int = 10_000,
    ci: float = 0.90,
) -> dict[str, Any]:
    """
    Bootstrap CI for ratio of geometric means (B / A).
    ratio > 1 means B has higher engagement than A.
    p_b_greater = fraction of bootstrap samples where mean(log_b) > mean(log_a).
    """
    rng = np.random.default_rng(seed=42)

    boot_means_a = np.array(
        [float(np.mean(rng.choice(log_views_a, size=len(log_views_a)))) for _ in range(n_resamples)]
    )
    boot_means_b = np.array(
        [float(np.mean(rng.choice(log_views_b, size=len(log_views_b)))) for _ in range(n_resamples)]
    )

    boot_log_ratios = boot_means_b - boot_means_a
    alpha = 1.0 - ci
    ci_low = float(np.exp(np.percentile(boot_log_ratios, 100 * alpha / 2)))
    ci_high = float(np.exp(np.percentile(boot_log_ratios, 100 * (1 - alpha / 2))))
    ratio = float(np.exp(float(np.mean(log_views_b)) - float(np.mean(log_views_a))))
    p_b_greater = float(np.mean(boot_means_b > boot_means_a))

    return {
        "ratio": ratio,
        "ratio_ci_low": ci_low,
        "ratio_ci_high": ci_high,
        "p_b_greater": p_b_greater,
        "ci_level": ci,
        "interpretable": (
            f"geometric mean ratio {ratio:.2f}x "
            f"(90% CI {ci_low:.2f}\u2013{ci_high:.2f}x); "
            f"{p_b_greater:.0%} of bootstrap samples show B \u003e A"
        ),
    }


def trend_test(
    years: list[int],
    log_views_per_year: list[NDArray[np.float64]],
) -> dict[str, Any]:
    """
    Two complementary trend tests on log-transformed engagement data.

    Mann-Kendall: applied to yearly geometric means (monotonic trend detection).
    Spearman: applied to all (year, log_view) pairs (correlation with time).
    """
    geo_means: list[float] = [
        float(np.exp(float(np.mean(v)))) if len(v) > 0 else 0.0
        for v in log_views_per_year
    ]

    mk_tau = 0.0
    mk_p = 1.0
    mk_trend = "no_trend"
    if _MK_AVAILABLE and len(geo_means) >= 4:
        try:
            mk_result = mk.original_test(geo_means)
            _tau = float(mk_result.Tau)
            _mkp = float(mk_result.p)
            mk_tau = _tau if np.isfinite(_tau) else 0.0
            mk_p = _mkp if np.isfinite(_mkp) else 1.0
            mk_trend = str(mk_result.trend).replace(" ", "_")
        except Exception:
            pass

    all_years_flat: list[float] = []
    all_log_views_flat: list[float] = []
    for yr, views in zip(years, log_views_per_year):
        for v in views:
            all_years_flat.append(float(yr))
            all_log_views_flat.append(float(v))

    sp_rho = 0.0
    sp_p = 1.0
    if len(all_years_flat) >= 3:
        sp_result = scipy_stats.spearmanr(all_years_flat, all_log_views_flat)
        _rho = float(sp_result.statistic)
        _p = float(sp_result.pvalue)
        sp_rho = _rho if np.isfinite(_rho) else 0.0
        sp_p = _p if np.isfinite(_p) else 1.0

    mk_human = (
        f"Mann-Kendall tau={mk_tau:.3f}, p={mk_p:.3f} ({mk_trend.replace('_', ' ')})"
        if _MK_AVAILABLE
        else "Mann-Kendall unavailable"
    )
    sp_human = f"Spearman rho={sp_rho:.3f}, p={sp_p:.4f}"
    interpretation = f"{mk_human}. {sp_human}."

    return {
        "mannkendall_tau": mk_tau,
        "mannkendall_p": mk_p,
        "mannkendall_trend": mk_trend,
        "spearman_rho": sp_rho,
        "spearman_p": sp_p,
        "interpretation": interpretation,
    }
