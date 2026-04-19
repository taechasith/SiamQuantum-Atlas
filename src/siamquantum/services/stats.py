from __future__ import annotations

import pickle
import time as _time
from dataclasses import dataclass, field
from typing import Any

import numpy as np
from scipy import stats as scipy_stats
from sklearn.cluster import DBSCAN

from siamquantum.models import MacroCluster, TTestResult


# ---------------------------------------------------------------------------
# DenStream micro-cluster
# ---------------------------------------------------------------------------

@dataclass
class _MicroCluster:
    center: np.ndarray  # type: ignore[type-arg]
    weight: float
    timestamp: float  # monotonic seconds of last update


# ---------------------------------------------------------------------------
# DenStreamClusterer
# ---------------------------------------------------------------------------

class DenStreamClusterer:
    """
    Simplified DenStream: online micro-cluster maintenance with exponential
    temporal decay. Macro-clusters are produced on demand via DBSCAN over
    micro-cluster centers.

    Parameters
    ----------
    epsilon      : radius for micro-cluster membership
    mu           : weight threshold — micro-clusters below mu * beta are pruned
    lambda_decay : decay rate λ; weight = 2^(-λ·Δt)
    beta         : pruning factor (fraction of mu below which cluster is pruned)
    """

    def __init__(
        self,
        epsilon: float = 0.5,
        mu: float = 1.0,
        lambda_decay: float = 0.25,
        beta: float = 0.5,
    ) -> None:
        self.epsilon = epsilon
        self.mu = mu
        self.lambda_decay = lambda_decay
        self.beta = beta
        self._clusters: list[_MicroCluster] = []

    def _decay(self, weight: float, elapsed: float) -> float:
        return float(weight * (2.0 ** (-self.lambda_decay * elapsed)))

    def insert(self, point: np.ndarray, timestamp: float | None = None) -> None:  # type: ignore[type-arg]
        """Insert a data point at `timestamp` (default: now)."""
        ts = timestamp if timestamp is not None else _time.monotonic()

        # Decay all existing micro-clusters
        for mc in self._clusters:
            elapsed = ts - mc.timestamp
            if elapsed > 0:
                mc.weight = self._decay(mc.weight, elapsed)
                mc.timestamp = ts

        # Prune below threshold
        prune_threshold = self.beta * self.mu
        self._clusters = [mc for mc in self._clusters if mc.weight >= prune_threshold]

        if not self._clusters:
            self._clusters.append(_MicroCluster(center=point.copy(), weight=1.0, timestamp=ts))
            return

        # Find nearest micro-cluster
        centers = np.stack([mc.center for mc in self._clusters])
        dists: np.ndarray[Any, np.dtype[np.float64]] = np.linalg.norm(centers - point, axis=1)
        nearest_idx = int(np.argmin(dists))

        if dists[nearest_idx] <= self.epsilon:
            mc = self._clusters[nearest_idx]
            # Weighted centroid update
            mc.center = (mc.weight * mc.center + point) / (mc.weight + 1.0)
            mc.weight += 1.0
        else:
            self._clusters.append(_MicroCluster(center=point.copy(), weight=1.0, timestamp=ts))

    def get_macro_clusters(self) -> list[MacroCluster]:
        """Run DBSCAN over micro-cluster centers to produce macro-clusters."""
        if not self._clusters:
            return []

        centers = np.stack([mc.center for mc in self._clusters])
        weights: np.ndarray[Any, np.dtype[np.float64]] = np.array([mc.weight for mc in self._clusters])

        labels: np.ndarray[Any, np.dtype[np.int32]] = DBSCAN(
            eps=self.epsilon * 2.0, min_samples=1
        ).fit_predict(centers)

        macro: dict[int, list[int]] = {}
        for i, label in enumerate(labels.tolist()):
            macro.setdefault(int(label), []).append(i)

        results: list[MacroCluster] = []
        for label, indices in macro.items():
            if label == -1:
                continue
            w = weights[indices]
            c: np.ndarray[Any, np.dtype[np.float64]] = np.average(
                centers[indices], axis=0, weights=w
            )
            results.append(MacroCluster(center=c.tolist(), size=int(w.sum())))

        return results

    def to_bytes(self) -> bytes:
        return pickle.dumps(self)

    @classmethod
    def from_bytes(cls, data: bytes) -> DenStreamClusterer:
        obj = pickle.loads(data)
        if not isinstance(obj, cls):
            raise TypeError(f"Expected {cls.__name__}, got {type(obj).__name__}")
        return obj


# ---------------------------------------------------------------------------
# Feature extraction
# ---------------------------------------------------------------------------

_CONTENT_TYPES = ("academic", "news", "educational", "entertainment")
_PRODUCTION_TYPES = ("state_research", "university", "corporate_media", "independent")


def build_feature_vector(
    published_year: int,
    platform: str,
    content_type: str | None,
    production_type: str | None,
    engagement_score: float,
) -> np.ndarray:  # type: ignore[type-arg]
    """
    Build a fixed-length feature vector for DenStream.
    Dims: [year_norm, is_youtube, ct×4, pt×4, engagement_norm] = 11 dims.
    """
    year_norm = (published_year - 2020) / 10.0
    is_youtube = 1.0 if platform == "youtube" else 0.0
    ct_oh = [1.0 if content_type == ct else 0.0 for ct in _CONTENT_TYPES]
    pt_oh = [1.0 if production_type == pt else 0.0 for pt in _PRODUCTION_TYPES]
    eng_norm = min(engagement_score / 15.0, 1.0)  # log1p(1e6) ≈ 13.8
    return np.array([year_norm, is_youtube, *ct_oh, *pt_oh, eng_norm], dtype=np.float64)


def engagement_score(view_count: int | None, like_count: int | None, comment_count: int | None) -> float:
    """Composite log-scale engagement score for YouTube sources. 0.0 for GDELT."""
    if view_count is None and like_count is None and comment_count is None:
        return 0.0
    v = view_count or 0
    l = like_count or 0
    c = comment_count or 0
    import math
    return math.log1p(v + l * 5 + c * 10)


# ---------------------------------------------------------------------------
# Engagement level assignment
# ---------------------------------------------------------------------------

def compute_engagement_levels(scores: list[float]) -> list[str]:
    """
    Assign low/medium/high by tertile within the provided score list.
    Returns list of labels in same order as input.
    """
    if not scores:
        return []
    if len(scores) == 1:
        return ["medium"]

    arr = np.array(scores)
    p33 = float(np.percentile(arr, 33.3))
    p67 = float(np.percentile(arr, 66.7))

    def _level(s: float) -> str:
        if s <= p33:
            return "low"
        if s <= p67:
            return "medium"
        return "high"

    return [_level(s) for s in scores]


# ---------------------------------------------------------------------------
# Welch's t-test
# ---------------------------------------------------------------------------

def compute_ttest(
    scores_a: list[float],
    scores_b: list[float],
    year_a: int,
    year_b: int,
) -> TTestResult:
    """
    Welch's two-sample t-test (equal_var=False, two-sided).
    Raises ValueError if either group has fewer than 2 samples.
    """
    if len(scores_a) < 2 or len(scores_b) < 2:
        raise ValueError(
            f"Need ≥2 samples per group; got year_a={len(scores_a)} year_b={len(scores_b)}"
        )
    result = scipy_stats.ttest_ind(scores_a, scores_b, equal_var=False)
    t_stat = float(result.statistic)
    p_val = float(result.pvalue)

    # Welch-Satterthwaite degrees of freedom
    na, nb = len(scores_a), len(scores_b)
    sa2, sb2 = float(np.var(scores_a, ddof=1)), float(np.var(scores_b, ddof=1))
    num = (sa2 / na + sb2 / nb) ** 2
    denom = (sa2 / na) ** 2 / (na - 1) + (sb2 / nb) ** 2 / (nb - 1)
    df = num / denom if denom > 0 else float(na + nb - 2)

    return TTestResult(
        year_a=year_a,
        year_b=year_b,
        t=t_stat,
        df=df,
        p_value=p_val,
        significant=p_val < 0.05,
    )
