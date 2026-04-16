#!/usr/bin/env python3
"""
SiamQuantum Atlas — live data fetcher.
Pulls from YouTube Data API v3 + GDELT, builds a rich graph JSON with 1000+ nodes.
Writes to viewer/data/siamquantum_atlas_graph.json.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

log = logging.getLogger("atlas.fetcher")

# ── Config ─────────────────────────────────────────────────────────────────────
YOUTUBE_KEY = os.environ.get("SIAMQUANTUM_YOUTUBE_API_KEY", "")
OUT = Path(__file__).parent / "data" / "siamquantum_atlas_graph.json"
OUT.parent.mkdir(parents=True, exist_ok=True)

LAYER_COLORS = {
    "Articles":  "#2E86AB",
    "Videos":    "#61D095",
    "Podcasts":  "#F0B429",
    "Films_TV":  "#F25F5C",
    "Topics":    "#6A994E",
    "Frames":    "#7B2CBF",
    "Platforms": "#F18F01",
    "Time":      "#C73E1D",
    "Clusters":  "#3A0CA3",
}

# ── Search queries ─────────────────────────────────────────────────────────────
YT_QUERIES = [
    "ควอนตัม",
    "ฟิสิกส์ควอนตัม",
    "คอมพิวเตอร์ควอนตัม",
    "ทฤษฎีควอนตัม",
    "ควอนตัมเอนแทงเกิลเมนต์",
    "quantum Thailand",
    "quantum physics Thai",
    "quantum computing Thailand",
    "quantum technology Thailand",
    "quantum education Thailand",
    "quantum science Thailand",
    "quantum Thai language",
    "quantum mechanics Thailand",
    "quantum cryptography Thailand",
    "quantum research Thailand",
    "quantum communication Thailand",
    "Thailand quantum lab",
    "NSTDA quantum Thailand",
    "quantum spirituality Thai",
    "quantum healing Thailand",
]

GDELT_QUERIES = [
    "quantum Thailand",
    "quantum Thai language",
    "quantum physics Thailand",
    "quantum technology Bangkok",
    "quantum computing Thailand",
    "Thailand quantum research",
]

# ── Topic taxonomy ─────────────────────────────────────────────────────────────
TOPIC_MAP = {
    "quantum-computing":     ["computing","computer","qubit","qubits","gate","circuit","algorithm","ibm q","google quantum","คอมพิวเตอร์ควอนตัม","โปรเซสเซอร์ควอนตัม"],
    "quantum-physics":       ["physics","mechanics","wave","particle","superposition","entanglement","uncertainty","schrödinger","ฟิสิกส์ควอนตัม","กลศาสตร์ควอนตัม","ทฤษฎีควอนตัม"],
    "quantum-cryptography":  ["cryptography","encryption","qkd","key distribution","secure","การเข้ารหัส","ความปลอดภัย"],
    "quantum-education":     ["education","learning","student","university","school","lecture","course","textbook","การศึกษา","นักเรียน","มหาวิทยาลัย"],
    "quantum-art":           ["art","music","creative","design","painting","film","ศิลปะ","ดนตรี","ภาพยนตร์"],
    "quantum-biology":       ["biology","life","dna","photosynthesis","bird navigation","enzyme","ชีววิทยา"],
    "quantum-spirituality":  ["spirit","consciousness","soul","healing","meditation","chakra","aura","จิต","วิญญาณ","ไสยศาสตร์","จิตวิญญาณ"],
    "quantum-communication": ["communication","network","teleportation","internet","satellite","การสื่อสาร","โทรคมนาคม"],
    "quantum-sensing":       ["sensing","sensor","measurement","precision","imaging","magnetometry"],
    "quantum-policy":        ["policy","government","investment","national","nstda","stt","ministry","นโยบาย","รัฐบาล","สวทช"],
    "quantum-business":      ["business","startup","company","commercial","market","investment","ธุรกิจ","สตาร์ทอัพ"],
    "quantum-futurism":      ["future","revolution","next generation","breakthrough","อนาคต","ยุคใหม่","การปฏิวัติ"],
    "quantum-hype":          ["hype","pseudoscience","fake","exaggerate","misinformation","mislead","เท็จ","หลอกลวง"],
    "quantum-chemistry":     ["chemistry","molecule","reaction","simulation","เคมี","โมเลกุล"],
    "quantum-materials":     ["material","superconductor","topological","condensed matter","วัสดุ"],
}

def extract_topics(text: str) -> list[str]:
    t = text.lower()
    return [k for k, kws in TOPIC_MAP.items() if any(w in t for w in kws)]

def uid(*parts: str) -> str:
    return hashlib.md5(":".join(str(p) for p in parts).encode()).hexdigest()[:14]

# ── YouTube ────────────────────────────────────────────────────────────────────
def fetch_youtube() -> list[dict]:
    if not YOUTUBE_KEY:
        log.warning("  [youtube] No API key — skipped")
        return []

    items: list[dict] = []
    seen: set[str] = set()

    with httpx.Client(timeout=20) as client:
        for q in YT_QUERIES:
            # Page 1
            page_token = None
            for page in range(2):  # 2 pages × 50 = 100 per query × 20 queries = 2000 max
                params: dict = {
                    "part": "snippet",
                    "q": q,
                    "type": "video",
                    "maxResults": 50,
                    "key": YOUTUBE_KEY,
                    "order": "relevance",
                }
                if page_token:
                    params["pageToken"] = page_token
                try:
                    r = client.get("https://www.googleapis.com/youtube/v3/search", params=params)
                    if r.status_code == 403:
                        log.warning("  [youtube] Quota exceeded")
                        return items
                    if r.status_code != 200:
                        break
                    data = r.json()
                    for it in data.get("items", []):
                        vid = it.get("id", {}).get("videoId", "")
                        if not vid or vid in seen:
                            continue
                        seen.add(vid)
                        s = it.get("snippet", {})
                        year = (s.get("publishedAt") or "")[:4] or "unknown"
                        items.append({
                            "id": f"yt:{vid}",
                            "name": (s.get("title") or "Untitled")[:140],
                            "layer": "Videos",
                            "platform": "YouTube",
                            "url": f"https://youtube.com/watch?v={vid}",
                            "year": year,
                            "description": (s.get("description") or "")[:300],
                            "channel": s.get("channelTitle", ""),
                            "language": "th",
                        })
                    page_token = data.get("nextPageToken")
                    if not page_token:
                        break
                    time.sleep(0.15)
                except Exception as e:
                    log.debug("  [youtube] %s: %s", q, e)
                    break
            log.info("  [youtube] %-45s  total so far: %d", repr(q), len(items))
            time.sleep(0.2)

    return items

# ── GDELT ──────────────────────────────────────────────────────────────────────
def fetch_gdelt() -> list[dict]:
    items: list[dict] = []
    seen: set[str] = set()

    with httpx.Client(timeout=25, follow_redirects=True) as client:
        for q in GDELT_QUERIES:
            try:
                r = client.get(
                    "https://api.gdeltproject.org/api/v2/doc/doc",
                    params={"query": q, "mode": "ArtList", "maxrecords": 250,
                            "format": "json", "timespan": "10y", "sort": "DateDesc"},
                )
                if r.status_code != 200:
                    log.warning("  [gdelt] %s → HTTP %d", q, r.status_code)
                    continue
                arts = r.json().get("articles") or []
                for a in arts:
                    url = a.get("url", "")
                    if not url or url in seen:
                        continue
                    seen.add(url)
                    date = a.get("seendate", "")
                    year = date[:4] if date and len(date) >= 4 else "unknown"
                    items.append({
                        "id": f"art:{uid(url)}",
                        "name": (a.get("title") or "Untitled")[:140],
                        "layer": "Articles",
                        "platform": a.get("domain", "news"),
                        "url": url,
                        "year": year,
                        "description": a.get("title", ""),
                        "language": a.get("language", "en"),
                    })
                log.info("  [gdelt]   %-45s  total so far: %d", repr(q), len(items))
                time.sleep(0.6)
            except Exception as e:
                log.warning("  [gdelt] %s: %s", q, e)

    return items

# ── Embedding helper ───────────────────────────────────────────────────────────
def _embed_items(items: list[dict]) -> list[list[float]]:
    """Return Qwen3-Embedding-8B vectors for each item (hash fallback if unavailable)."""
    texts = [
        f"{it.get('name', '')} {it.get('description', '')}".strip()
        for it in items
    ]
    try:
        import sys as _sys
        _sys.path.insert(0, str(PROJECT_ROOT))
        from siamquantum_atlas.nlp.embeddings import embed_batch
        return embed_batch(texts, show_progress=False)
    except Exception as exc:
        log.warning("  [embed] Qwen3 unavailable (%s) — using hash fallback", exc)
        import hashlib as _hl, math as _m
        def _h(t: str) -> list[float]:
            v = [0.0] * 128
            for w in t.lower().split():
                v[int(_hl.md5(w.encode()).hexdigest(), 16) % 128] += 1.0
            n = _m.sqrt(sum(x * x for x in v)) or 1.0
            return [x / n for x in v]
        return [_h(t) for t in texts]


# ── Barnes-Hut layout ──────────────────────────────────────────────────────────
def _barnes_hut_layout(
    node_ids: list[str],
    edges: list[dict],
) -> dict[str, tuple[float, float]]:
    """Compute 2-D positions with the Barnes-Hut approximated FR layout.

    Uses igraph's Fruchterman-Reingold with grid=True (O(n log n) Barnes-Hut
    approximation).  Falls back to networkx spring_layout if igraph is absent.
    """
    try:
        import igraph as ig  # type: ignore

        id_to_idx = {nid: i for i, nid in enumerate(node_ids)}
        g = ig.Graph(n=len(node_ids), directed=False)
        valid = [
            (id_to_idx[e["source"]], id_to_idx[e["target"]])
            for e in edges
            if e.get("source") in id_to_idx and e.get("target") in id_to_idx
        ]
        if valid:
            g.add_edges(valid)
        layout = g.layout_fruchterman_reingold(grid=True, niter=500)
        coords = layout.coords
        # Normalise to roughly [-500, 500]
        xs = [c[0] for c in coords]
        ys = [c[1] for c in coords]
        x_range = max(abs(max(xs) - min(xs)), 1e-6)
        y_range = max(abs(max(ys) - min(ys)), 1e-6)
        scale = 1000.0 / max(x_range, y_range)
        return {
            node_ids[i]: (
                round((coords[i][0] - (min(xs) + max(xs)) / 2) * scale, 2),
                round((coords[i][1] - (min(ys) + max(ys)) / 2) * scale, 2),
            )
            for i in range(len(node_ids))
        }
    except Exception as exc:
        log.warning("  [layout] igraph unavailable (%s) — using spring fallback", exc)
        import networkx as _nx
        G = _nx.Graph()
        G.add_nodes_from(node_ids)
        for e in edges:
            s, t = e.get("source"), e.get("target")
            if s in G and t in G:
                G.add_edge(s, t, weight=e.get("weight", 1.0))
        pos = _nx.spring_layout(G, seed=42, k=2.0)
        return {
            nid: (round(float(xy[0]) * 500, 2), round(float(xy[1]) * 500, 2))
            for nid, xy in pos.items()
        }


# ── Graph builder ──────────────────────────────────────────────────────────────
def build_graph(raw: list[dict]) -> dict:
    from collections import defaultdict

    nodes: dict[str, dict] = {}
    edges: list[dict] = []

    def add(n: dict) -> None:
        nodes[n["id"]] = n

    item_topics: dict[str, list[str]] = {}  # item_id → [topic_ids]

    for item in raw:
        try:
            age = max(0, datetime.now().year - int(item.get("year", 2015)))
        except Exception:
            age = 5
        size = round(max(0.6, 2.8 - age * 0.1), 2)
        add({**item, "size": size, "color": LAYER_COLORS.get(item["layer"], "#aaa")})

        # Platform node
        plat = item.get("platform") or "unknown"
        pid = f"platform:{uid(plat)}"
        if pid not in nodes:
            add({"id": pid, "name": plat, "layer": "Platforms", "size": 2.0, "color": LAYER_COLORS["Platforms"]})
        edges.append({"source": item["id"], "target": pid, "weight": 1.0})

        # Time node
        yr = item.get("year", "unknown")
        tid = f"time:{yr}"
        if tid not in nodes:
            add({"id": tid, "name": str(yr), "layer": "Time", "size": 2.2, "color": LAYER_COLORS["Time"]})
        edges.append({"source": item["id"], "target": tid, "weight": 0.7})

        # Topic nodes
        text = f"{item.get('name','')} {item.get('description','')}"
        topics = extract_topics(text)
        item_topics[item["id"]] = []
        for t in topics:
            topic_id = f"topic:{t}"
            if topic_id not in nodes:
                add({"id": topic_id, "name": t.replace("-", " "), "layer": "Topics", "size": 2.5, "color": LAYER_COLORS["Topics"]})
            edges.append({"source": item["id"], "target": topic_id, "weight": 1.3})
            item_topics[item["id"]].append(topic_id)

    # Topic co-occurrence edges (topics that share ≥5 items are connected)
    topic_items: dict[str, set] = defaultdict(set)
    for iid, tids in item_topics.items():
        for tid in tids:
            topic_items[tid].add(iid)

    topic_list = list(topic_items.keys())
    for i in range(len(topic_list)):
        for j in range(i + 1, len(topic_list)):
            shared = len(topic_items[topic_list[i]] & topic_items[topic_list[j]])
            if shared >= 5:
                edges.append({"source": topic_list[i], "target": topic_list[j], "weight": round(shared / 10, 2)})

    # Cluster nodes — one per major topic that covers ≥10 items
    for topic_id, covered in topic_items.items():
        if len(covered) >= 10 and topic_id in nodes:
            cid = f"cluster:{topic_id}"
            label = "Cluster: " + nodes[topic_id]["name"]
            if cid not in nodes:
                add({"id": cid, "name": label, "layer": "Clusters", "size": 3.5, "color": LAYER_COLORS["Clusters"]})
            edges.append({"source": topic_id, "target": cid, "weight": 1.0})

    # ── Embedding + clustering pipeline ───────────────────────────────────────
    item_ids   = [it["id"] for it in raw]
    item_nodes = [n for it in raw for n in [nodes.get(it["id"])] if n]

    if raw:
        log.info("  [ml] Embedding %d items (Qwen3-Embedding-8B)...", len(raw))
        vectors = _embed_items(raw)

        # Micro-clustering: DBSCAN
        log.info("  [ml] DBSCAN micro-clustering...")
        try:
            from siamquantum_atlas.clustering.semantic import micro_cluster_dbscan
            micro = micro_cluster_dbscan(vectors)
            log.info("  [ml] DBSCAN → %d micro-clusters, %d noise", micro.n_clusters, micro.n_noise)
            for idx, item in enumerate(raw):
                n = nodes.get(item["id"])
                if n is not None:
                    n["micro_cluster"] = micro.labels[idx]
        except Exception as exc:
            log.warning("  [ml] DBSCAN failed: %s", exc)

        # Macro-clustering: K-Means + Silhouette
        log.info("  [ml] K-Means macro-clustering (Silhouette optimisation)...")
        try:
            from siamquantum_atlas.clustering.semantic import semantic_cluster
            macro = semantic_cluster(vectors)
            log.info("  [ml] K-Means → k=%d, silhouette=%.3f", macro.k, macro.silhouette)
            for idx, item in enumerate(raw):
                n = nodes.get(item["id"])
                if n is not None:
                    n["macro_cluster"] = macro.labels[idx]
            # Add macro-cluster hub nodes
            for cid in range(macro.k):
                hub_id = f"macrocluster:{cid}"
                if hub_id not in nodes:
                    add({
                        "id": hub_id,
                        "name": f"Macro-Cluster {cid}",
                        "layer": "Clusters",
                        "size": 4.0,
                        "color": "#3A0CA3",
                        "macro_cluster": cid,
                    })
                for idx, item in enumerate(raw):
                    if macro.labels[idx] == cid:
                        edges.append({"source": item["id"], "target": hub_id, "weight": 0.5})
        except Exception as exc:
            log.warning("  [ml] K-Means failed: %s", exc)

        # Louvain + Burt's constraint on item similarity graph
        log.info("  [ml] Louvain communities + Burt's constraint...")
        try:
            from siamquantum_atlas.clustering.graph_clusters import build_similarity_graph
            gcr = build_similarity_graph(vectors, threshold=0.45)
            log.info(
                "  [ml] Louvain → %d communities | graph: %d nodes %d edges",
                len(gcr.communities),
                gcr.graph.number_of_nodes(),
                gcr.graph.number_of_edges(),
            )
            for idx, item in enumerate(raw):
                n = nodes.get(item["id"])
                if n is not None:
                    n["community"]      = gcr.louvain_labels.get(idx, -1)
                    n["burt_constraint"] = gcr.constraint.get(idx, 1.0)
        except Exception as exc:
            log.warning("  [ml] Louvain/Burt failed: %s", exc)

    # ── Barnes-Hut layout ─────────────────────────────────────────────────────
    log.info("  [ml] Computing Barnes-Hut layout...")
    try:
        node_id_list = list(nodes.keys())
        positions = _barnes_hut_layout(node_id_list, edges)
        for nid, (x, y) in positions.items():
            if nid in nodes:
                nodes[nid]["x"] = x
                nodes[nid]["y"] = y
        log.info("  [ml] Layout computed for %d nodes", len(positions))
    except Exception as exc:
        log.warning("  [ml] Barnes-Hut layout failed: %s", exc)

    return {
        "nodes": list(nodes.values()),
        "links": edges,
        "layers": list(LAYER_COLORS.keys()),
        "meta": {
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "node_count": len(nodes),
            "edge_count": len(edges),
            "sources": ["YouTube Data API v3", "GDELT DocAPI v2"],
            "algorithms": {
                "embedding":       "Qwen3-Embedding-8B (sentence-transformers)",
                "micro_cluster":   "DBSCAN (cosine, eps=0.25)",
                "macro_cluster":   "K-Means + Silhouette optimisation",
                "connectivity":    "Louvain Modularity (networkx)",
                "brokerage":       "Burt's Structural Constraint",
                "layout":          "Barnes-Hut FR (igraph grid=True)",
            },
        },
    }

# ── Entry point ────────────────────────────────────────────────────────────────
def fetch_and_build(log_progress: bool = True) -> dict:
    t0 = time.time()
    if log_progress:
        log.info("Fetching YouTube...")
    yt = fetch_youtube()

    if log_progress:
        log.info("Fetching GDELT...")
    gd = fetch_gdelt()

    all_items = yt + gd
    if log_progress:
        log.info("Building graph from %d raw items...", len(all_items))

    graph = build_graph(all_items)
    OUT.write_text(json.dumps(graph, ensure_ascii=False, indent=2), encoding="utf-8")

    elapsed = round(time.time() - t0, 1)
    log.info(
        "Done in %.1fs — %d nodes / %d edges → %s",
        elapsed, len(graph["nodes"]), len(graph["links"]), OUT,
    )
    return graph


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    fetch_and_build()
