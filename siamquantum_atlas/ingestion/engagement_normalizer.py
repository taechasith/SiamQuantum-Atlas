from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

# Platform-specific engagement score calculation.
# All scores normalized to 0–100 within each platform's distribution.

@dataclass
class EngagementScore:
    raw_score: float | None
    normalized: float | None       # 0–100, within-platform percentile
    percentile: float | None       # 0.0–1.0 position in platform distribution
    method: str                     # how score was computed
    confidence: float               # 0–1, data completeness
    missing_fields: list[str]


def compute_raw_score(platform: str, views: float | None, likes: float | None,
                      comments: float | None, shares: float | None,
                      rank_proxy: float | None, popularity_proxy: float | None,
                      extra: dict[str, Any] | None = None) -> EngagementScore:
    """Compute platform-specific raw engagement score. Never fabricates missing values."""
    extra = extra or {}
    missing: list[str] = []

    if platform == "youtube":
        if views is None:
            missing.append("views")
        if likes is None:
            missing.append("likes")
        if comments is None:
            missing.append("comments")

        if views is not None and views > 0 and (likes is not None or comments is not None):
            # Engagement rate per 1000 views, comments weighted 2×
            rate = ((likes or 0) + (comments or 0) * 2) / views * 1000
            raw = math.log1p(rate) * math.log1p(views) / 10
            method = "youtube_engagement_rate"
            confidence = 0.90 if not missing else 0.65
        elif views is not None:
            raw = math.log1p(views) / 2
            method = "youtube_views_only"
            confidence = 0.50
            missing.extend(["likes", "comments"])
        else:
            return EngagementScore(None, None, None, "no_data", 0.0, missing or ["views", "likes", "comments"])

    elif platform == "reddit":
        score = extra.get("score") or likes or 0
        num_comments = comments or 0
        upvote_ratio = extra.get("upvote_ratio") or 0.5

        if score == 0 and num_comments == 0:
            return EngagementScore(None, None, None, "no_data", 0.0, ["score", "comments"])

        raw = math.log1p(score * upvote_ratio + num_comments * 3)
        method = "reddit_composite"
        confidence = 0.80 if upvote_ratio != 0.5 else 0.65
        if score == 0:
            missing.append("score")

    elif platform == "gdelt_news":
        if rank_proxy is not None and popularity_proxy is not None:
            raw = (rank_proxy * 0.6 + popularity_proxy * 0.4) * 10
            method = "gdelt_authority_proxy"
            confidence = 0.50
        elif rank_proxy is not None:
            raw = rank_proxy * 5
            method = "gdelt_rank_only"
            confidence = 0.35
            missing.append("popularity_proxy")
        else:
            return EngagementScore(None, None, None, "no_proxy_data", 0.20,
                                   ["rank_proxy", "popularity_proxy"])
    else:
        # Generic fallback
        if rank_proxy is not None:
            raw = rank_proxy * 5
            method = "generic_rank_proxy"
            confidence = 0.30
        else:
            return EngagementScore(None, None, None, "no_data", 0.0, ["all_metrics"])

    return EngagementScore(
        raw_score=round(raw, 4),
        normalized=None,  # set later by normalizer
        percentile=None,  # set later by normalizer
        method=method,
        confidence=confidence,
        missing_fields=missing,
    )


def normalize_within_platform(scores_by_platform: dict[str, list[tuple[int, EngagementScore]]]) -> dict[int, EngagementScore]:
    """Set normalized 0–100 and percentile for each item within its platform distribution."""
    result: dict[int, EngagementScore] = {}

    for platform, indexed_scores in scores_by_platform.items():
        valid = [(idx, s) for idx, s in indexed_scores if s.raw_score is not None]
        if not valid:
            for idx, s in indexed_scores:
                result[idx] = s
            continue

        raw_vals = sorted(s.raw_score for _, s in valid)
        min_val = raw_vals[0]
        max_val = raw_vals[-1]
        val_range = max_val - min_val or 1.0
        n = len(raw_vals)

        for idx, score in valid:
            percentile = raw_vals.index(score.raw_score) / max(n - 1, 1)
            normalized = (score.raw_score - min_val) / val_range * 100
            result[idx] = EngagementScore(
                raw_score=score.raw_score,
                normalized=round(normalized, 2),
                percentile=round(percentile, 4),
                method=score.method,
                confidence=score.confidence,
                missing_fields=score.missing_fields,
            )

        # Items with no data get percentile=0
        for idx, score in indexed_scores:
            if idx not in result:
                result[idx] = EngagementScore(
                    raw_score=None, normalized=0.0, percentile=0.0,
                    method=score.method, confidence=0.0, missing_fields=score.missing_fields,
                )

    return result
