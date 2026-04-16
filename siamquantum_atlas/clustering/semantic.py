from __future__ import annotations

from dataclasses import dataclass

import numpy as np
from sklearn.cluster import DBSCAN, KMeans
from sklearn.metrics import silhouette_score


# ── Result types ───────────────────────────────────────────────────────────────

@dataclass(slots=True)
class MicroClusterResult:
    """DBSCAN micro-clustering result."""
    labels: list[int]       # -1 = noise point
    n_clusters: int
    n_noise: int


@dataclass(slots=True)
class SemanticClusterResult:
    """K-Means macro-clustering result with Silhouette-optimised k."""
    labels: list[int]
    centers: list[list[float]]
    k: int
    silhouette: float


# ── Micro-clustering: DBSCAN ───────────────────────────────────────────────────

def micro_cluster_dbscan(
    vectors: list[list[float]],
    eps: float = 0.25,
    min_samples: int = 3,
) -> MicroClusterResult:
    """Micro-cluster embeddings with DBSCAN using cosine distance.

    eps is interpreted in cosine-distance space (0 = identical, 1 = orthogonal,
    2 = opposite).  A value of 0.25 ≈ cosine-similarity ≥ 0.75.
    Noise points receive label -1.
    """
    n = len(vectors)
    if n < min_samples:
        return MicroClusterResult(labels=[-1] * n, n_clusters=0, n_noise=n)

    matrix = np.array(vectors, dtype=np.float32)
    db = DBSCAN(eps=eps, min_samples=min_samples, metric="cosine", n_jobs=-1)
    labels: list[int] = db.fit_predict(matrix).tolist()

    n_clusters = len(set(labels) - {-1})
    n_noise    = labels.count(-1)
    return MicroClusterResult(labels=labels, n_clusters=n_clusters, n_noise=n_noise)


# ── Macro-clustering: K-Means + Silhouette ─────────────────────────────────────

def semantic_cluster(
    vectors: list[list[float]],
    k: int | None = None,
    k_min: int = 2,
    k_max: int = 20,
    silhouette_samples: int = 2000,
) -> SemanticClusterResult:
    """Macro-cluster embeddings with K-Means.

    When k is None, the optimal number of clusters is chosen automatically
    by maximising the Silhouette score over [k_min, k_max].
    """
    if not vectors:
        return SemanticClusterResult(labels=[], centers=[], k=0, silhouette=0.0)

    n = len(vectors)
    if n < 3:
        return SemanticClusterResult(
            labels=[0] * n,
            centers=[vectors[0]],
            k=1,
            silhouette=0.0,
        )

    matrix = np.array(vectors, dtype=np.float32)

    if k is not None:
        best_k   = max(1, min(k, n - 1))
        best_sil = 0.0
    else:
        best_k, best_sil = k_min, -1.0
        upper = min(k_max + 1, n)
        for candidate_k in range(k_min, upper):
            km     = KMeans(n_clusters=candidate_k, n_init=10, random_state=42)
            labels = km.fit_predict(matrix)
            try:
                sil = float(silhouette_score(
                    matrix, labels,
                    sample_size=min(silhouette_samples, n),
                    random_state=42,
                ))
            except Exception:
                sil = -1.0
            if sil > best_sil:
                best_sil, best_k = sil, candidate_k

    km     = KMeans(n_clusters=best_k, n_init=10, random_state=42)
    labels = km.fit_predict(matrix).tolist()

    return SemanticClusterResult(
        labels=labels,
        centers=km.cluster_centers_.tolist(),
        k=best_k,
        silhouette=round(best_sil, 4),
    )
