from __future__ import annotations

from typing import Any

import numpy as np
from numpy.typing import NDArray
from scipy import stats as scipy_stats


def mann_whitney(
    a: NDArray[np.float64],
    b: NDArray[np.float64],
    alternative: str = "two-sided",
) -> dict[str, Any]:
    """Mann-Whitney U test. Returns U, p, effect size r = Z/sqrt(N)."""
    if len(a) < 2 or len(b) < 2:
        return {"u": None, "p": None, "effect_r": None, "note": "insufficient_data"}
    result = scipy_stats.mannwhitneyu(a, b, alternative=alternative)
    n = len(a) + len(b)
    z = scipy_stats.norm.ppf(result.pvalue / 2) if result.pvalue < 1 else 0.0
    effect_r = abs(z) / (n ** 0.5)
    return {
        "u": float(result.statistic),
        "p": float(result.pvalue),
        "effect_r": round(effect_r, 4),
        "significant": bool(result.pvalue < 0.05),
    }


def kruskal_wallis(groups: dict[str, NDArray[np.float64]]) -> dict[str, Any]:
    """Kruskal-Wallis H test across multiple groups."""
    arrays = [v for v in groups.values() if len(v) >= 2]
    if len(arrays) < 2:
        return {"h": None, "p": None, "note": "insufficient_groups"}
    result = scipy_stats.kruskal(*arrays)
    return {
        "h": round(float(result.statistic), 4),
        "p": round(float(result.pvalue), 6),
        "significant": bool(result.pvalue < 0.05),
        "groups": {k: int(len(v)) for k, v in groups.items() if len(v) >= 2},
    }


def chi2_independence(
    contingency: dict[tuple[str, str], int],
    row_cats: list[str],
    col_cats: list[str],
) -> dict[str, Any]:
    """Chi-square test of independence on a contingency table."""
    table = np.array(
        [[contingency.get((r, c), 0) for c in col_cats] for r in row_cats],
        dtype=float,
    )
    if table.sum() == 0:
        return {"chi2": None, "p": None, "note": "empty_table"}
    result = scipy_stats.chi2_contingency(table, correction=False)
    n = table.sum()
    cramers_v = float(np.sqrt(result.statistic / (n * (min(len(row_cats), len(col_cats)) - 1)))) if min(len(row_cats), len(col_cats)) > 1 else 0.0
    return {
        "chi2": round(float(result.statistic), 4),
        "p": round(float(result.pvalue), 6),
        "dof": int(result.dof),
        "cramers_v": round(cramers_v, 4),
        "significant": bool(result.pvalue < 0.05),
    }
