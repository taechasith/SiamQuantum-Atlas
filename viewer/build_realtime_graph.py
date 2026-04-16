#!/usr/bin/env python3
"""
Convert realtime pipeline output (ProcessedItems) → viewer graph JSON.

Runs the full realtime collection (1,300 items) and writes
viewer/data/siamquantum_atlas_graph.json so the viewer shows live data.

Usage:
    python viewer/build_realtime_graph.py
    python viewer/build_realtime_graph.py --max-items 1300
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Load .env
env_path = ROOT / ".env"
if env_path.exists():
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        import os; os.environ.setdefault(k.strip(), v.strip())

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
log = logging.getLogger("build_graph")

OUT = Path(__file__).parent / "data" / "siamquantum_atlas_graph.json"
OUT.parent.mkdir(parents=True, exist_ok=True)

LAYER_COLORS = {
    "Articles":  "#2E86AB",
    "Videos":    "#61D095",
    "Posts":     "#4ECDC4",
    "Topics":    "#6A994E",
    "Clusters":  "#7B2CBF",
    "Platforms": "#F18F01",
    "Time":      "#C73E1D",
    "Geo":       "#E76F51",
}

PLATFORM_LAYER = {
    "youtube":    "Videos",
    "gdelt_news": "Articles",
    "reddit":     "Posts",
}

CLUSTER_LABELS = {
    "beginner_education":   "Beginner Education",
    "breakthrough_news":    "Breakthrough News",
    "student_interest":     "Student Interest",
    "career_opportunity":   "Career Opportunity",
    "quantum_computing":    "Quantum Computing",
    "daily_life_application": "Daily Life Application",
    "misconception_confusion": "Misconception / Confusion",
    "high_engagement_hook": "High Engagement Hook",
    "emerging_topic":       "Emerging Topic",
    "low_engagement_topic": "Low Engagement Topic",
}


def _uid(*parts: str) -> str:
    return hashlib.md5(":".join(str(p) for p in parts).encode()).hexdigest()[:14]


def build_graph(items: list[dict]) -> dict:
    nodes: dict[str, dict] = {}
    edges: list[dict] = []

    def add(n: dict) -> None:
        nodes[n["id"]] = n

    for item in items:
        layer = PLATFORM_LAYER.get(item["platform"], "Articles")
        color = LAYER_COLORS[layer]

        # Size: scale by normalized engagement (1–5)
        eng = item.get("normalized_engagement") or 0
        size = round(1.0 + (eng / 100) * 4, 2)

        # Year from published_at
        year = "unknown"
        pub = item.get("published_at")
        if pub:
            try:
                year = pub[:4]
            except Exception:
                pass

        node_id = f"item:{_uid(item['url'])}"
        add({
            "id": node_id,
            "name": (item.get("title") or "Untitled")[:140],
            "layer": layer,
            "platform": item["platform"],
            "url": item.get("url", ""),
            "year": year,
            "description": (item.get("description") or "")[:300],
            "size": size,
            "color": color,
            "engagement": item.get("normalized_engagement"),
            "cluster": item.get("comm_value_cluster"),
            "geo_province": item.get("geo", {}).get("province_en"),
            "geo_region": item.get("geo", {}).get("region"),
            "emerging": item.get("is_emerging", False),
            "thailand_relevance": item.get("thailand_relevance"),
            "quantum_relevance": item.get("quantum_relevance"),
        })

        # ── Platform node ──────────────────────────────────────────────────
        plat_id = f"platform:{_uid(item['platform'])}"
        if plat_id not in nodes:
            add({
                "id": plat_id, "name": item["platform"].replace("_", " ").title(),
                "layer": "Platforms", "size": 2.5,
                "color": LAYER_COLORS["Platforms"], "url": "",
            })
        edges.append({"source": node_id, "target": plat_id, "weight": 0.8})

        # ── Time node ─────────────────────────────────────────────────────
        time_id = f"time:{year}"
        if time_id not in nodes:
            add({
                "id": time_id, "name": year,
                "layer": "Time", "size": 2.0,
                "color": LAYER_COLORS["Time"], "url": "",
            })
        edges.append({"source": node_id, "target": time_id, "weight": 0.6})

        # ── Communication-value cluster node ──────────────────────────────
        cluster = item.get("comm_value_cluster", "")
        if cluster:
            cluster_id = f"cluster:{cluster}"
            if cluster_id not in nodes:
                label = CLUSTER_LABELS.get(cluster, cluster.replace("_", " ").title())
                add({
                    "id": cluster_id, "name": label,
                    "layer": "Clusters", "size": 3.5,
                    "color": LAYER_COLORS["Clusters"], "url": "",
                    "description": f"Communication-value cluster: {label}",
                })
            conf = item.get("comm_value_confidence", 0.5)
            edges.append({"source": node_id, "target": cluster_id, "weight": round(conf * 2, 2)})

        # ── Topic keyword nodes ───────────────────────────────────────────
        for kw in (item.get("keywords") or [])[:5]:
            topic_id = f"topic:{_uid(kw)}"
            if topic_id not in nodes:
                add({
                    "id": topic_id, "name": kw,
                    "layer": "Topics", "size": 1.8,
                    "color": LAYER_COLORS["Topics"], "url": "",
                })
            edges.append({"source": node_id, "target": topic_id, "weight": 1.0})

        # ── Geo region node ───────────────────────────────────────────────
        region = item.get("geo", {}).get("region")
        if region:
            geo_id = f"geo:{_uid(region)}"
            if geo_id not in nodes:
                add({
                    "id": geo_id, "name": f"Region: {region}",
                    "layer": "Geo", "size": 2.2,
                    "color": LAYER_COLORS["Geo"], "url": "",
                })
            edges.append({"source": node_id, "target": geo_id, "weight": 0.7})

    # ── Cluster ↔ cluster edges (shared items ≥ 3) ────────────────────────
    cluster_items: dict[str, set[str]] = {}
    for item in items:
        c = item.get("comm_value_cluster", "")
        if c:
            cluster_items.setdefault(c, set()).add(item["url"])
    cluster_ids = list(cluster_items.keys())
    for i in range(len(cluster_ids)):
        for j in range(i + 1, len(cluster_ids)):
            shared = len(cluster_items[cluster_ids[i]] & cluster_items[cluster_ids[j]])
            if shared >= 2:
                edges.append({
                    "source": f"cluster:{cluster_ids[i]}",
                    "target": f"cluster:{cluster_ids[j]}",
                    "weight": round(shared / 10, 2),
                })

    return {
        "name": "SiamQuantum Atlas — Real-Time Intelligence",
        "layers": list(LAYER_COLORS.keys()),
        "nodes": list(nodes.values()),
        "links": edges,
        "metadata": {
            "generated_by": "build_realtime_graph.py",
            "fetched_at": datetime.now(tz=timezone.utc).isoformat(),
            "item_count": len(items),
            "node_count": len(nodes),
            "edge_count": len(edges),
        },
    }


def fetch_and_build(max_items: int = 1300, log_progress: bool = True) -> dict:
    log.info("Starting realtime collection (cap: %d)…", max_items)
    from siamquantum_atlas.ingestion.realtime_pipeline import RealtimePipeline
    from dataclasses import asdict

    pipeline = RealtimePipeline()
    dataset = pipeline.run(max_items=max_items)

    log.info("Collected %d items. Building graph…", dataset.total_items)
    items = [asdict(item) for item in dataset.items]

    graph = build_graph(items)

    OUT.write_text(json.dumps(graph, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info(
        "Written: %s  (%d nodes, %d edges)",
        OUT, len(graph["nodes"]), len(graph["links"]),
    )
    return graph


def main(max_items: int = 1300) -> None:
    graph = fetch_and_build(max_items=max_items)
    print(f"\nGraph ready: {len(graph['nodes'])} nodes, {len(graph['links'])} edges")
    print(f"  File: {OUT}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-items", type=int, default=1300)
    args = parser.parse_args()
    main(max_items=args.max_items)
