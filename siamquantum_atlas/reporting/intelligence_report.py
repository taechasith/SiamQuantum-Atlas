from __future__ import annotations

import json
import statistics
from collections import Counter, defaultdict
from dataclasses import asdict
from pathlib import Path
from typing import Any

from siamquantum_atlas.clustering.comm_value import (
    ALL_CLUSTERS,
    CLUSTER_DESCRIPTIONS,
    extract_keywords,
)
from siamquantum_atlas.geo.thai_geo import build_province_hotspot_map
from siamquantum_atlas.ingestion.realtime_pipeline import ProcessedItem, RealtimeDataset

_WHY_WORKS: dict[str, str] = {
    "beginner_education": (
        "Thai general audiences are quantum-curious but lack entry points. "
        "Plain-language explainers get shared widely because the topic feels exclusive — "
        "accessible content lowers the barrier and triggers social sharing."
    ),
    "breakthrough_news": (
        "News frames create a sense of urgency and national pride. "
        "Thai audiences respond strongly to 'first' and 'record' framings, "
        "especially when linked to Thai institutions or regional competitiveness."
    ),
    "student_interest": (
        "Thai university students actively seek academic community signals. "
        "Content that names universities, professors, or research groups builds "
        "credibility and drives shares within academic networks."
    ),
    "career_opportunity": (
        "Scholarships and quantum careers generate high urgency engagement. "
        "Thai students forward these aggressively. Time-limited calls to action "
        "spike comments and shares beyond organic reach."
    ),
    "quantum_computing": (
        "Technical computing content reaches a smaller but highly engaged "
        "niche of engineers and CS students. Comments are substantive. "
        "Engagement rate per viewer is high, even if raw reach is lower."
    ),
    "daily_life_application": (
        "Connecting quantum to medicine, finance, or logistics removes "
        "the 'irrelevant' objection. Thai audiences respond when they can "
        "see economic or health impact on their daily lives."
    ),
    "misconception_confusion": (
        "Pseudoscience and quantum-healing content gets high raw engagement "
        "via confusion and debate, but distorts public understanding. "
        "Debunking content can ride the same traffic while correcting it."
    ),
    "high_engagement_hook": (
        "Top-quartile content shares common patterns: short titles, "
        "emotional stakes, relatable analogies, or celebrity/university "
        "name-drops. These are templates worth studying and adapting."
    ),
    "emerging_topic": (
        "New content in this cluster has not yet peaked. Early coverage "
        "of trending quantum stories gives communicators a first-mover "
        "advantage before the topic is saturated."
    ),
    "low_engagement_topic": (
        "Content in this cluster is not resonating. Common failure modes: "
        "too technical without context, purely academic with no hook, "
        "or duplicating already-saturated angles."
    ),
}

_WHAT_TO_CREATE: dict[str, str] = {
    "beginner_education": (
        "Create a short-form Thai video series: 'ควอนตัมใน 3 นาที' (Quantum in 3 minutes). "
        "Each episode explains one concept with a local analogy. "
        "Target: TikTok, YouTube Shorts, Facebook Reels."
    ),
    "breakthrough_news": (
        "Launch a Thai quantum news digest: weekly roundup of global breakthroughs "
        "with a paragraph on 'what this means for Thailand'. "
        "Partner with Thai university PR offices for local angle."
    ),
    "student_interest": (
        "Create a Thai quantum research map: who is doing what at which university. "
        "Interview format with student researchers. "
        "Distribution: university Facebook groups, LINE OA."
    ),
    "career_opportunity": (
        "Build a curated 'ทุนควอนตัม' (quantum scholarships) newsletter. "
        "Monthly, bilingual, with deadlines and application tips. "
        "High forward rate expected in student communities."
    ),
    "quantum_computing": (
        "Hands-on tutorial series: run a real quantum circuit on IBM Quantum "
        "with Thai commentary. GitHub repo + YouTube video. "
        "Targets the CS/engineering student segment."
    ),
    "daily_life_application": (
        "Create explainer on quantum's role in Thai-specific sectors: "
        "logistics (SCG, Thai Union), healthcare (Bumrungrad), "
        "fintech (Kasikorn). Use infographics with ROI framing."
    ),
    "misconception_confusion": (
        "Produce a Thai quantum myth-busting series. "
        "Format: claim → evidence → verdict. Collaborating with respected "
        "Thai physicists adds credibility and shareability."
    ),
    "high_engagement_hook": (
        "Analyze the top 20 highest-engagement items in this dataset. "
        "Extract the hook patterns (title structure, opening line, visual cue) "
        "and apply them to new quantum content."
    ),
    "emerging_topic": (
        "Fast-respond to the emerging topics in this cluster within 48 hours. "
        "Publish a Thai-language explainer or reaction piece "
        "before the conversation peaks."
    ),
    "low_engagement_topic": (
        "Audit the format, hook, and distribution channel of these items. "
        "Consider repurposing the best underlying ideas with a new frame, "
        "stronger title, or different platform."
    ),
}


def generate_intelligence_report(dataset: RealtimeDataset, output_dir: Path) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    items = dataset.items

    # Build cluster analysis
    cluster_analysis = _build_cluster_analysis(items)

    # Build geo map
    geo_map = _build_geo_map(items)

    # Build top 10 recommendations
    recommendations = _build_recommendations(items, cluster_analysis)

    # Build dataset schema
    schema = _dataset_schema()

    # Build summary
    summary = _build_summary(dataset, cluster_analysis)

    # Limitations
    limitations = _build_limitations(dataset)

    report = {
        "metadata": {
            "collected_at": dataset.collected_at,
            "total_items": dataset.total_items,
            "schema_version": dataset.schema_version,
            "platform_counts": dataset.platform_counts,
        },
        "dataset_schema": schema,
        "summary": summary,
        "cluster_analysis": cluster_analysis,
        "thailand_interest_map": {
            "type": "FeatureCollection",
            "features": geo_map,
            "metadata": {
                "coordinate_system": "WGS84",
                "centroid_source": "province_centroid",
                "gee_compatible": True,
                "note": "Coordinates are province centroids, not individual poster locations. No IP data used.",
            },
        },
        "top_10_recommendations": recommendations,
        "limitations": limitations,
    }

    # Write JSON
    json_path = output_dir / "quantum_intelligence_report.json"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    # Write Markdown
    md_path = output_dir / "quantum_intelligence_report.md"
    md_path.write_text(_render_markdown(report, dataset), encoding="utf-8")

    # Write GEE-ready FeatureCollection
    gee_path = output_dir / "thailand_quantum_geo.geojson"
    gee_path.write_text(
        json.dumps(report["thailand_interest_map"], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    # Write items as JSONL for downstream processing
    jsonl_path = output_dir / "quantum_items.jsonl"
    with jsonl_path.open("w", encoding="utf-8") as f:
        for item in items:
            f.write(json.dumps(asdict(item), ensure_ascii=False) + "\n")

    return {
        "json": json_path,
        "markdown": md_path,
        "geojson": gee_path,
        "jsonl": jsonl_path,
    }


def _build_cluster_analysis(items: list[ProcessedItem]) -> list[dict[str, Any]]:
    by_cluster: dict[str, list[ProcessedItem]] = defaultdict(list)
    for item in items:
        by_cluster[item.comm_value_cluster].append(item)

    analysis = []
    for cluster in ALL_CLUSTERS:
        members = by_cluster.get(cluster, [])
        count = len(members)
        if count == 0:
            analysis.append({
                "cluster": cluster,
                "description": CLUSTER_DESCRIPTIONS.get(cluster, ""),
                "count": 0,
                "avg_engagement": None,
                "median_engagement": None,
                "platform_breakdown": {},
                "top_keywords": [],
                "examples": [],
                "why_it_works_or_fails": _WHY_WORKS.get(cluster, ""),
                "what_to_create_next": _WHAT_TO_CREATE.get(cluster, ""),
            })
            continue

        eng_scores = [i.normalized_engagement for i in members if i.normalized_engagement is not None]
        avg_eng = round(statistics.mean(eng_scores), 2) if eng_scores else None
        median_eng = round(statistics.median(eng_scores), 2) if eng_scores else None

        # Platform breakdown with normalized engagement per platform
        plat_scores: dict[str, list[float]] = defaultdict(list)
        plat_counts: dict[str, int] = defaultdict(int)
        for item in members:
            plat_counts[item.platform] += 1
            if item.normalized_engagement is not None:
                plat_scores[item.platform].append(item.normalized_engagement)

        platform_breakdown = {}
        for plat, cnt in plat_counts.items():
            plat_eng = plat_scores.get(plat, [])
            platform_breakdown[plat] = {
                "count": cnt,
                "avg_normalized_engagement": round(statistics.mean(plat_eng), 2) if plat_eng else None,
                "note": "normalized within platform distribution",
            }

        # Top keywords
        all_text = " ".join(
            " ".join(filter(None, [i.title, i.description or ""])) for i in members
        )
        top_keywords = extract_keywords(all_text, top_n=12)

        # Top examples by engagement
        sorted_members = sorted(
            members,
            key=lambda i: i.normalized_engagement or 0,
            reverse=True,
        )
        examples = [
            {
                "title": item.title[:120],
                "platform": item.platform,
                "url": item.url,
                "published_at": item.published_at,
                "normalized_engagement": item.normalized_engagement,
                "engagement_note": item.engagement.get("method", "unknown"),
                "comm_value_signals": item.comm_value_signals[:5],
            }
            for item in sorted_members[:5]
        ]

        analysis.append({
            "cluster": cluster,
            "description": CLUSTER_DESCRIPTIONS.get(cluster, ""),
            "count": count,
            "avg_engagement": avg_eng,
            "median_engagement": median_eng,
            "engagement_note": (
                f"UNCERTAINTY: {count - len(eng_scores)} of {count} items missing engagement data"
                if len(eng_scores) < count else "complete"
            ),
            "platform_breakdown": platform_breakdown,
            "top_keywords": top_keywords,
            "examples": examples,
            "why_it_works_or_fails": _WHY_WORKS.get(cluster, ""),
            "what_to_create_next": _WHAT_TO_CREATE.get(cluster, ""),
        })

    return analysis


def _build_geo_map(items: list[ProcessedItem]) -> list[dict]:
    item_dicts = [asdict(item) for item in items]
    return build_province_hotspot_map(item_dicts)


def _build_recommendations(
    items: list[ProcessedItem], cluster_analysis: list[dict]
) -> list[dict[str, Any]]:
    recs = []

    # Sort clusters by avg engagement (non-null) descending
    scored_clusters = sorted(
        [c for c in cluster_analysis if c["avg_engagement"] is not None],
        key=lambda c: c["avg_engagement"],
        reverse=True,
    )

    rank = 1
    seen_clusters: set[str] = set()

    # Top 5 from high-performing clusters
    for cluster_data in scored_clusters[:5]:
        cluster = cluster_data["cluster"]
        seen_clusters.add(cluster)
        top_example = cluster_data["examples"][0] if cluster_data["examples"] else None
        recs.append({
            "rank": rank,
            "cluster": cluster,
            "rationale": f"Avg normalized engagement {cluster_data['avg_engagement']}/100 — highest-performing cluster in dataset",
            "action": cluster_data["what_to_create_next"],
            "top_example": top_example,
            "evidence_count": cluster_data["count"],
        })
        rank += 1

    # Rank 6-7: emerging topics
    emerging = [i for i in items if i.is_emerging]
    if emerging:
        top_emerging = sorted(emerging, key=lambda i: i.normalized_engagement or 0, reverse=True)[:2]
        recs.append({
            "rank": rank,
            "cluster": "emerging_topic",
            "rationale": f"{len(emerging)} items published in last 7 days — first-mover opportunity",
            "action": _WHAT_TO_CREATE["emerging_topic"],
            "top_example": {
                "title": top_emerging[0].title[:120],
                "platform": top_emerging[0].platform,
                "url": top_emerging[0].url,
                "published_at": top_emerging[0].published_at,
            } if top_emerging else None,
            "evidence_count": len(emerging),
        })
        rank += 1

    # Rank 7-8: misconception debunk opportunity
    misc_data = next((c for c in cluster_analysis if c["cluster"] == "misconception_confusion"), None)
    if misc_data and misc_data["count"] > 0:
        recs.append({
            "rank": rank,
            "cluster": "misconception_confusion",
            "rationale": (
                f"{misc_data['count']} misconception items found. "
                "Debunking content can capture this audience while improving public understanding."
            ),
            "action": _WHAT_TO_CREATE["misconception_confusion"],
            "top_example": misc_data["examples"][0] if misc_data["examples"] else None,
            "evidence_count": misc_data["count"],
        })
        rank += 1

    # Fill remaining slots up to 10
    remaining_clusters = [
        c for c in cluster_analysis
        if c["cluster"] not in seen_clusters
        and c["cluster"] != "low_engagement_topic"
        and c["count"] > 0
    ]
    for cluster_data in sorted(remaining_clusters, key=lambda c: c["count"], reverse=True):
        if rank > 10:
            break
        recs.append({
            "rank": rank,
            "cluster": cluster_data["cluster"],
            "rationale": f"{cluster_data['count']} items — underserved content niche with engaged audience",
            "action": cluster_data["what_to_create_next"],
            "top_example": cluster_data["examples"][0] if cluster_data["examples"] else None,
            "evidence_count": cluster_data["count"],
        })
        rank += 1

    return recs[:10]


def _build_summary(dataset: RealtimeDataset, cluster_analysis: list[dict]) -> dict[str, Any]:
    items = dataset.items
    eng_vals = [i.normalized_engagement for i in items if i.normalized_engagement is not None]
    langs = Counter(i.language for i in items if i.language)
    top_clusters = sorted(
        [(c["cluster"], c["count"]) for c in cluster_analysis if c["count"] > 0],
        key=lambda x: x[1], reverse=True,
    )

    return {
        "total_items": dataset.total_items,
        "collected_at": dataset.collected_at,
        "platform_breakdown": dataset.platform_counts,
        "language_distribution": dict(langs.most_common(10)),
        "engagement_summary": {
            "items_with_engagement_data": len(eng_vals),
            "items_missing_engagement_data": dataset.total_items - len(eng_vals),
            "avg_normalized_engagement": round(statistics.mean(eng_vals), 2) if eng_vals else None,
            "median_normalized_engagement": round(statistics.median(eng_vals), 2) if eng_vals else None,
            "note": "Scores normalized 0–100 within each platform's distribution. Cross-platform comparison is approximate.",
        },
        "top_clusters": [{"cluster": c, "count": n} for c, n in top_clusters[:5]],
        "geo_coverage": {
            "items_with_province": sum(1 for i in items if i.geo.get("province_en")),
            "items_with_country_only": sum(
                1 for i in items
                if not i.geo.get("province_en") and i.geo.get("confidence", 0) > 0
            ),
            "items_without_geo": sum(1 for i in items if i.geo.get("confidence", 0) == 0),
        },
        "emerging_items": sum(1 for i in items if i.is_emerging),
    }


def _build_limitations(dataset: RealtimeDataset) -> list[str]:
    items = dataset.items
    missing_eng = sum(1 for i in items if i.normalized_engagement is None)
    gdelt_count = dataset.platform_counts.get("gdelt_news", 0)

    lims = [
        f"ENGAGEMENT DATA: {missing_eng}/{dataset.total_items} items have no hard engagement metrics. "
        "GDELT articles have only domain-authority proxies, not real view/like counts. "
        "Treat GDELT engagement scores as relative authority signals, not true audience reach.",

        "GEO INFERENCE: Province assignments inferred from text keywords and domain names only. "
        "No IP addresses used. Confidence scores ≤0.85 for text-inferred locations. "
        "Country-level fallback (conf=0.40) assigned when only 'Thailand' is mentioned.",

        "PLATFORM COVERAGE: Reddit public API returns limited historical depth. "
        "Twitter/X excluded (API requires paid access). LINE and Facebook domestic Thai content "
        "not captured — these platforms hold significant Thai quantum discussion not reflected here.",

        "ENGAGEMENT NORMALIZATION: Scores normalized within each platform's own distribution. "
        "A score of 80/100 on Reddit ≠ 80/100 on YouTube. Cross-platform comparison "
        "should use percentile rank, not absolute normalized scores.",

        "QUANTUM RELEVANCE: Filter uses keyword matching. Items using 'quantum' metaphorically "
        "(e.g. business jargon) may pass the filter. Confidence scores on quantum_relevance "
        "field indicate detection strength.",

        "RECENCY: GDELT timespan queries use 72h–30d windows. YouTube and Reddit default to "
        "top posts from the past year. Dataset reflects near-real-time availability, "
        "not strictly the last 1,300 items by timestamp.",

        "DEDUPLICATION: Near-duplicate detection uses URL + title hashing. "
        "Translated versions of the same article (Thai and English) are kept as separate items "
        "since they represent distinct communications to different audiences.",
    ]

    if gdelt_count > dataset.total_items * 0.5:
        lims.append(
            f"GDELT DOMINANCE: {gdelt_count}/{dataset.total_items} items from GDELT. "
            "News article dominance may skew cluster analysis toward news frames. "
            "YouTube or social engagement patterns may be underrepresented."
        )

    return lims


def _dataset_schema() -> dict[str, Any]:
    return {
        "description": "Schema for each item in quantum_items.jsonl",
        "fields": {
            "id": "int — sequential ID within this collection run",
            "adapter": "str — source adapter name (youtube_live, gdelt_live, reddit_live)",
            "platform": "str — platform identifier (youtube, gdelt_news, reddit)",
            "media_type": "str — video | article | post",
            "title": "str — original title",
            "description": "str | null — description or excerpt",
            "url": "str — source URL",
            "published_at": "ISO 8601 datetime | null",
            "language": "str | null — ISO 639-1 code",
            "domain": "str | null — source domain",
            "views": "float | null — raw view count (YouTube only)",
            "likes": "float | null — raw like/upvote count",
            "comments": "float | null — raw comment count",
            "shares": "float | null — raw share count (null if not available)",
            "rank_proxy": "float | null — domain authority proxy (GDELT only)",
            "popularity_proxy": "float | null — composite authority proxy (GDELT only)",
            "engagement.raw_score": "float | null — platform-specific composite engagement score",
            "engagement.normalized_0_100": "float | null — engagement score normalized 0–100 within platform",
            "engagement.percentile": "float | null — position 0.0–1.0 in platform distribution",
            "engagement.method": "str — how score was computed",
            "engagement.confidence": "float — data completeness confidence 0–1",
            "engagement.missing_fields": "list[str] — fields absent from engagement calculation",
            "geo.province_th": "str | null — Thai province name in Thai script",
            "geo.province_en": "str | null — Thai province name in English",
            "geo.region": "str | null — Central | North | Northeast | South | East",
            "geo.lat": "float | null — province centroid latitude (WGS84)",
            "geo.lng": "float | null — province centroid longitude (WGS84)",
            "geo.confidence": "float — geo inference confidence 0–1",
            "geo.method": "str — explicit_thai | explicit_english | alias | domain | fallback_country | no_signal",
            "comm_value_cluster": "str — communication value cluster label",
            "comm_value_confidence": "float — cluster assignment confidence 0–1",
            "comm_value_signals": "list[str] — keywords/signals that triggered cluster assignment",
            "is_emerging": "bool — published within last 7 days",
            "days_old": "float | null — age in days at collection time",
            "quantum_relevance": "float 0–1 — quantum topic signal strength",
            "thailand_relevance": "float 0–1 — Thailand location/topic signal strength",
            "normalized_engagement": "float | null — alias for engagement.normalized_0_100",
            "keywords": "list[str] — top 8 keywords extracted from title+description",
            "collected_at": "ISO 8601 datetime — when this item was processed",
        },
        "geo_note": "All coordinates are province/country centroids derived from text inference. No IP addresses collected or exposed.",
        "engagement_note": "Normalized scores are within-platform only. Missing metrics are labeled, never imputed.",
    }


def _render_markdown(report: dict, dataset: RealtimeDataset) -> str:
    s = report["summary"]
    lines = [
        "# Thailand Quantum Content Intelligence Report",
        f"\n**Collected:** {s['collected_at']}  ",
        f"**Total items:** {s['total_items']}  ",
        f"**Platforms:** {', '.join(f'{k} ({v})' for k,v in s['platform_breakdown'].items())}",
        "",
        "---",
        "",
        "## Summary",
        "",
        f"- **Items with engagement data:** {s['engagement_summary']['items_with_engagement_data']}",
        f"- **Items missing engagement data:** {s['engagement_summary']['items_missing_engagement_data']}",
        f"- **Avg normalized engagement:** {s['engagement_summary']['avg_normalized_engagement']}",
        f"- **Emerging items (< 7 days):** {s['emerging_items']}",
        f"- **Items with province-level geo:** {s['geo_coverage']['items_with_province']}",
        "",
        "### Language Distribution",
    ]
    for lang, count in s["language_distribution"].items():
        lines.append(f"- `{lang}`: {count}")

    lines += [
        "",
        "---",
        "",
        "## Engagement-First Cluster Analysis",
        "",
    ]

    for cluster_data in report["cluster_analysis"]:
        if cluster_data["count"] == 0:
            continue
        lines += [
            f"### {cluster_data['cluster'].replace('_', ' ').title()}",
            f"*{cluster_data['description']}*",
            "",
            f"- **Count:** {cluster_data['count']}",
            f"- **Avg engagement:** {cluster_data['avg_engagement']} / 100",
            f"- **Median engagement:** {cluster_data['median_engagement']} / 100",
            f"- **Engagement note:** {cluster_data['engagement_note']}",
            "",
            f"**Top keywords:** {', '.join(cluster_data['top_keywords'][:8])}",
            "",
            "**Platform breakdown:**",
        ]
        for plat, pd in cluster_data["platform_breakdown"].items():
            lines.append(f"- {plat}: {pd['count']} items, avg engagement {pd['avg_normalized_engagement']}")

        lines += ["", "**Top examples:**"]
        for ex in cluster_data["examples"][:3]:
            lines.append(f"- [{ex['title'][:80]}]({ex['url']}) — eng: {ex['normalized_engagement']}")

        lines += [
            "",
            f"**Why it works/fails:** {cluster_data['why_it_works_or_fails']}",
            "",
            f"**What to create next:** {cluster_data['what_to_create_next']}",
            "",
            "---",
            "",
        ]

    lines += [
        "## Thailand Interest Map",
        "",
        "*(See `thailand_quantum_geo.geojson` for Google Earth Engine import)*",
        "",
        "### Province Hotspot Ranking",
        "",
    ]
    for feat in report["thailand_interest_map"]["features"][:15]:
        p = feat["properties"]
        lines.append(
            f"{p['hotspot_rank']}. **{p['province_en']}** ({p['region']}) — "
            f"{p['item_count']} items, avg engagement {p['avg_normalized_engagement']}"
        )

    lines += [
        "",
        "---",
        "",
        "## Top 10 Content Recommendations",
        "",
    ]
    for rec in report["top_10_recommendations"]:
        lines += [
            f"### #{rec['rank']} — {rec['cluster'].replace('_', ' ').title()}",
            f"**Rationale:** {rec['rationale']}",
            f"**Action:** {rec['action']}",
            f"**Evidence:** {rec['evidence_count']} items",
            "",
        ]
        if rec.get("top_example"):
            ex = rec["top_example"]
            lines.append(f"**Best example:** [{ex['title'][:80]}]({ex['url']})")
        lines.append("")

    lines += [
        "---",
        "",
        "## Limitations & Confidence Notes",
        "",
    ]
    for lim in report["limitations"]:
        lines.append(f"- {lim}")
        lines.append("")

    return "\n".join(lines)
