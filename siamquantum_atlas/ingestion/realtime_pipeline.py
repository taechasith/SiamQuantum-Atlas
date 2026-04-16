from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

from siamquantum_atlas.adapters.base import RawMediaRecord
from siamquantum_atlas.adapters.gdelt_live import GDELTLiveAdapter
from siamquantum_atlas.adapters.reddit_live import RedditLiveAdapter
from siamquantum_atlas.adapters.youtube_live import YouTubeLiveAdapter
from siamquantum_atlas.clustering.comm_value import (
    ALL_CLUSTERS,
    CommValueResult,
    classify_comm_value,
    extract_keywords,
)
from siamquantum_atlas.geo.thai_geo import GeoInference, infer_geo
from siamquantum_atlas.ingestion.dedupe import content_fingerprint, dedupe_records
from siamquantum_atlas.ingestion.engagement_normalizer import (
    EngagementScore,
    compute_raw_score,
    normalize_within_platform,
)

logger = logging.getLogger(__name__)

ITEM_CAP = 1300

_QUANTUM_TERMS = {
    "quantum", "ควอนตัม", "qubit", "คิวบิต", "superposition", "entanglement",
    "quantum computing", "คอมพิวเตอร์ควอนตัม", "quantum physics", "ฟิสิกส์ควอนตัม",
    "quantum mechanics", "กลศาสตร์ควอนตัม", "quantum cryptography",
    "quantum supremacy", "quantum advantage", "wave function",
    "schrodinger", "heisenberg", "dirac", "planck",
}

# Phrases that contain "quantum" but are NOT about quantum science
_QUANTUM_FALSE_POSITIVE_PHRASES = {
    "quantum of solace",      # James Bond film
    "quantum leap",           # TV show (unless discussing tech)
    "quantum realm",          # Marvel
    "the quantum",            # band/album name
    "quantum healing",        # pseudoscience handled separately by comm_value
}

_THAILAND_TERMS = {
    "thailand", "thai", "ไทย", "ประเทศไทย", "กรุงเทพ", "bangkok",
    "เชียงใหม่", "ภูเก็ต", "สยาม", "thainess", "th",
    "mahidol", "chulalongkorn", "จุฬา", "มหิดล", "kmitl", "kmutt",
    "suranaree", "มทส", "msu", "kku",
}


@dataclass
class ProcessedItem:
    id: int
    adapter: str
    platform: str
    media_type: str
    title: str
    description: str | None
    url: str
    published_at: str | None
    language: str | None
    domain: str | None
    views: float | None
    likes: float | None
    comments: float | None
    shares: float | None
    rank_proxy: float | None
    popularity_proxy: float | None
    engagement: dict[str, Any]
    geo: dict[str, Any]
    comm_value_cluster: str
    comm_value_confidence: float
    comm_value_signals: list[str]
    is_emerging: bool
    days_old: float | None
    quantum_relevance: float
    thailand_relevance: float
    normalized_engagement: float | None
    keywords: list[str]
    collected_at: str


@dataclass
class RealtimeDataset:
    collected_at: str
    total_items: int
    items: list[ProcessedItem]
    platform_counts: dict[str, int]
    cluster_counts: dict[str, int]
    schema_version: str = "1.0"


class RealtimePipeline:
    def __init__(self) -> None:
        self.adapters = [
            YouTubeLiveAdapter(),
            GDELTLiveAdapter(),
            RedditLiveAdapter(),
        ]

    def run(self, max_items: int = ITEM_CAP) -> RealtimeDataset:
        logger.info("RealtimePipeline: collecting from %d adapters", len(self.adapters))

        # Budget allocation: YouTube 40%, GDELT 40%, Reddit 20%
        budgets = {
            "youtube_live": int(max_items * 0.40),
            "gdelt_live": int(max_items * 0.40),
            "reddit_live": int(max_items * 0.20),
        }

        raw: list[RawMediaRecord] = []
        for adapter in self.adapters:
            budget = budgets.get(adapter.name, 200)
            try:
                batch = adapter.fetch(max_items=budget)
                raw.extend(batch)
                logger.info("%s: %d records", adapter.name, len(batch))
            except Exception as exc:
                logger.error("Adapter %s failed: %s", adapter.name, exc)

        # Deduplicate
        deduped = dedupe_records(raw)
        logger.info("After dedupe: %d records (from %d raw)", len(deduped), len(raw))

        # Relevance filter
        relevant = [r for r in deduped if self._is_relevant(r)]
        logger.info("After relevance filter: %d records", len(relevant))

        # Cap at max_items
        relevant = relevant[:max_items]

        # Compute engagement scores
        scores_by_platform: dict[str, list[tuple[int, EngagementScore]]] = defaultdict(list)
        raw_scores: list[EngagementScore] = []
        for idx, rec in enumerate(relevant):
            extra = rec.raw_payload or {}
            score = compute_raw_score(
                platform=rec.platform,
                views=rec.views,
                likes=rec.likes,
                comments=rec.comments,
                shares=rec.shares,
                rank_proxy=rec.rank_proxy,
                popularity_proxy=rec.popularity_proxy,
                extra=extra,
            )
            scores_by_platform[rec.platform].append((idx, score))
            raw_scores.append(score)

        normalized_map = normalize_within_platform(scores_by_platform)

        # First pass: get engagement percentiles for cluster assignment
        percentiles: dict[int, float | None] = {
            idx: s.percentile for idx, s in normalized_map.items()
        }

        # Build processed items
        processed: list[ProcessedItem] = []
        for idx, rec in enumerate(relevant):
            eng_score = normalized_map.get(idx, raw_scores[idx])
            pct = percentiles.get(idx)

            text = " ".join(filter(None, [rec.title, rec.description, rec.full_text]))
            comm: CommValueResult = classify_comm_value(
                title=rec.title,
                text=text,
                published_at=rec.published_at,
                normalized_engagement=eng_score.normalized,
                engagement_percentile=pct,
            )

            geo: GeoInference = infer_geo(
                text=text,
                domain=rec.domain,
                url=rec.url,
            )

            qt_score = _quantum_relevance(rec.title, text)
            th_score = _thailand_relevance(rec.title, text, rec.domain, rec.raw_payload)

            keywords = extract_keywords(text, top_n=8)

            processed.append(ProcessedItem(
                id=idx,
                adapter=rec.adapter,
                platform=rec.platform,
                media_type=rec.media_type,
                title=rec.title,
                description=rec.description,
                url=rec.url,
                published_at=rec.published_at.isoformat() if rec.published_at else None,
                language=rec.language_detected,
                domain=rec.domain,
                views=rec.views,
                likes=rec.likes,
                comments=rec.comments,
                shares=rec.shares,
                rank_proxy=rec.rank_proxy,
                popularity_proxy=rec.popularity_proxy,
                engagement={
                    "raw_score": eng_score.raw_score,
                    "normalized_0_100": eng_score.normalized,
                    "percentile": eng_score.percentile,
                    "method": eng_score.method,
                    "confidence": eng_score.confidence,
                    "missing_fields": eng_score.missing_fields,
                },
                geo={
                    "province_th": geo.province_th,
                    "province_en": geo.province_en,
                    "region": geo.region,
                    "lat": geo.lat,
                    "lng": geo.lng,
                    "confidence": geo.confidence,
                    "method": geo.method,
                },
                comm_value_cluster=comm.cluster,
                comm_value_confidence=comm.confidence,
                comm_value_signals=comm.signals_matched,
                is_emerging=comm.is_emerging,
                days_old=comm.days_old,
                quantum_relevance=qt_score,
                thailand_relevance=th_score,
                normalized_engagement=eng_score.normalized,
                keywords=keywords,
                collected_at=datetime.now(tz=timezone.utc).isoformat(),
            ))

        platform_counts: dict[str, int] = defaultdict(int)
        cluster_counts: dict[str, int] = defaultdict(int)
        for item in processed:
            platform_counts[item.platform] += 1
            cluster_counts[item.comm_value_cluster] += 1

        logger.info(
            "RealtimePipeline complete: %d items, platforms=%s",
            len(processed), dict(platform_counts),
        )

        return RealtimeDataset(
            collected_at=datetime.now(tz=timezone.utc).isoformat(),
            total_items=len(processed),
            items=processed,
            platform_counts=dict(platform_counts),
            cluster_counts=dict(cluster_counts),
        )

    @staticmethod
    def _is_relevant(rec: RawMediaRecord) -> bool:
        text = " ".join(filter(None, [rec.title, rec.description or "", rec.full_text or ""])).lower()

        # Reject obvious non-physics false positives regardless of platform
        if any(fp in text for fp in _QUANTUM_FALSE_POSITIVE_PHRASES):
            return False

        has_quantum = any(t in text for t in _QUANTUM_TERMS)
        has_thailand = (
            any(t in text for t in _THAILAND_TERMS)
            or (rec.domain and rec.domain.endswith(".th"))
            or rec.raw_payload.get("thai_relevance_score", 0) >= 0.5
            # GDELT already queried with quantum+Thailand keywords — trust adapter-level filter
            or rec.platform == "gdelt_news"
        )
        # For GDELT: queried with "quantum" so all returns are quantum-relevant by construction
        if rec.platform == "gdelt_news":
            return has_thailand
        return has_quantum and has_thailand


def _quantum_relevance(title: str, text: str) -> float:
    combined = f"{title} {text}".lower()
    hits = sum(1 for t in _QUANTUM_TERMS if t in combined)
    return min(1.0, hits / 3)


def _thailand_relevance(title: str, text: str, domain: str | None, payload: dict) -> float:
    combined = f"{title} {text}".lower()
    score = payload.get("thai_relevance_score") or 0
    if score:
        return float(score)
    hits = sum(1 for t in _THAILAND_TERMS if t in combined)
    if domain and domain.endswith(".th"):
        hits += 3
    return min(1.0, hits / 2)
