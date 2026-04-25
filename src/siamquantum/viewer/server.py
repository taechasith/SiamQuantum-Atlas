from __future__ import annotations

import asyncio
import io
import json
import logging
import sqlite3
import time
from collections import Counter
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import BackgroundTasks, FastAPI, HTTPException, Query, Request
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from siamquantum.config import settings
from siamquantum.db.repos import (
    CommunitySubmissionRepo,
    EntityRepo,
    GeoRepo,
    SourceRepo,
    StatsCacheRepo,
    TripletRepo,
)
from siamquantum.db.session import db_path_from_url, get_connection
from siamquantum.models import CommunitySubmissionCreate
from siamquantum.stats.yearly_taxonomy_analytics import build_yearly_taxonomy_analytics

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# In-memory node registry cache (avoids cold DB rebuild on every click)
# ---------------------------------------------------------------------------
_node_registry_mem: dict[str, Any] | None = None
_node_registry_ts: float = 0.0
_NODE_REGISTRY_TTL = 86400.0  # 24h, matches DB cache TTL


def _invalidate_node_registry() -> None:
    global _node_registry_mem, _node_registry_ts
    _node_registry_mem = None
    _node_registry_ts = 0.0


# ---------------------------------------------------------------------------
# Startup: pre-warm registry + daily ingest scheduler
# ---------------------------------------------------------------------------

def _prewarm_registry_sync() -> None:
    try:
        db = db_path_from_url(settings.database_url)
        with get_connection(db) as conn:
            _get_node_registry(conn)
        logger.info("Node registry pre-warmed")
    except Exception:
        logger.exception("Node registry pre-warm failed")


async def _daily_ingest_task() -> None:
    """Run GDELT + YouTube ingest once per day at ~00:05.
    Fetches last 3 days to compensate for GDELT's ~24-48h indexing lag."""
    from datetime import date, datetime, timedelta

    while True:
        now = datetime.now()
        next_run = (now + timedelta(days=1)).replace(hour=0, minute=5, second=0, microsecond=0)
        await asyncio.sleep(max(60.0, (next_run - now).total_seconds()))
        try:
            from siamquantum.pipeline.ingest import ingest_gdelt_daterange, ingest_youtube_daterange
            db = db_path_from_url(settings.database_url)
            today = date.today()
            start = today - timedelta(days=2)  # 3-day window covers GDELT lag
            g_fetched, g_inserted = await ingest_gdelt_daterange(start, today, db)
            logger.info("Daily GDELT: fetched=%d inserted=%d", g_fetched, g_inserted)
            y_fetched, y_inserted = await ingest_youtube_daterange(start, today, db)
            logger.info("Daily YouTube: fetched=%d inserted=%d", y_fetched, y_inserted)
            if g_inserted + y_inserted > 0:
                _invalidate_node_registry()
                if not settings.database_read_only:
                    with get_connection(db) as conn:
                        StatsCacheRepo(conn).invalidate("graph:node_details")
        except Exception:
            logger.exception("Daily ingest failed")


@asynccontextmanager
async def lifespan(app_instance: FastAPI):  # type: ignore[type-arg]
    asyncio.create_task(asyncio.to_thread(_prewarm_registry_sync))
    asyncio.create_task(_daily_ingest_task())
    yield


app = FastAPI(title="SiamQuantum Atlas", version="0.1.0", lifespan=lifespan)
app.add_middleware(GZipMiddleware, minimum_size=1000)

_TEMPLATES_DIR = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))


def _db() -> Path:
    return db_path_from_url(settings.database_url)


def _norm_concept(text: str) -> str:
    return " ".join(text.strip().lower().split())


_HUB_PATTERNS: list[tuple[str, str]] = [
    ("quantum computing", "computing"),
    ("quantum", "quantum"),
    ("thailand", "geography"),
    ("thai", "geography"),
    ("cryptography", "security"),
    ("algorithm", "computing"),
    ("physics", "physics"),
    ("entanglement", "physics"),
    ("technology", "technology"),
    ("research", "research"),
    ("university", "institution"),
    ("government", "institution"),
    ("nstda", "institution"),
    ("nectec", "institution"),
    ("ibm", "industry"),
    ("google", "industry"),
    ("communication", "communication"),
]


def _hub_role(label: str) -> str:
    label_lower = label.lower()
    for pattern, role in _HUB_PATTERNS:
        if pattern in label_lower:
            return role
    return "concept"


def _is_vercel_demo_mode() -> bool:
    return getattr(settings, "database_read_only", False) is True or getattr(settings, "deployment_mode", "local") == "vercel_demo"


def _relevance_metadata(conn: sqlite3.Connection) -> dict[str, Any]:
    checked = int(conn.execute(
        "SELECT COUNT(*) FROM sources WHERE relevance_checked_at IS NOT NULL"
    ).fetchone()[0])
    total = int(conn.execute("SELECT COUNT(*) FROM sources").fetchone()[0])
    return {
        "mode": "operational_default" if checked == 0 else "classifier_backfill",
        "checked_sources": checked,
        "total_sources": total,
        "note": (
            "Current corpus filtering uses operational Thai-quantum defaults. "
            "Rows with is_quantum_tech=1 and is_thailand_related=1 should be read as corpus-scope flags, "
            "not as per-row classifier verification."
        ),
    }


def _graph_metrics_lookup(conn: sqlite3.Connection) -> tuple[dict[str, Any], dict[str, int], dict[str, float]]:
    metrics_obj = StatsCacheRepo(conn).get("graph:metrics")
    metrics = metrics_obj if isinstance(metrics_obj, dict) else {}
    bet_rows = metrics.get("top_betweenness", []) if isinstance(metrics, dict) else []
    bet_rank = {
        str(item.get("id")): index + 1
        for index, item in enumerate(bet_rows)
        if isinstance(item, dict) and item.get("id")
    }
    bet_score = {
        str(item.get("id")): float(item.get("score", 0.0))
        for item in bet_rows
        if isinstance(item, dict) and item.get("id") is not None
    }
    return metrics, bet_rank, bet_score


def _build_graph_node_detail_registry(conn: sqlite3.Connection) -> dict[str, Any]:
    import networkx as nx  # type: ignore[import-untyped]

    rows = conn.execute(
        """
        SELECT
            t.source_id,
            t.subject,
            t.relation,
            t.object,
            s.title,
            s.url,
            s.platform,
            s.published_year,
            s.quantum_domain,
            e.production_type,
            e.media_format,
            e.user_intent
        FROM triplets t
        JOIN sources s ON s.id = t.source_id
        LEFT JOIN entities e ON e.source_id = s.id
        ORDER BY s.published_year DESC, t.id DESC
        LIMIT 10000
        """
    ).fetchall()

    graph: nx.Graph = nx.Graph()
    labels: dict[str, str] = {}
    relation_counts_by_node: dict[str, Counter[str]] = {}
    taxonomy_counts_by_node: dict[str, Counter[str]] = {}
    domain_counts_by_node: dict[str, Counter[str]] = {}
    neighbor_shared_counts_by_node: dict[str, Counter[str]] = {}
    supporting_sources_by_node: dict[str, list[dict[str, Any]]] = {}
    seen_sources_by_node: dict[str, set[int]] = {}

    for row in rows:
        subject = (row["subject"] or "").strip()
        relation = (row["relation"] or "").strip()
        obj = (row["object"] or "").strip()
        subject_id = _norm_concept(subject)
        object_id = _norm_concept(obj)
        if len(subject_id) < 2 or len(object_id) < 2 or subject_id == object_id:
            continue

        labels.setdefault(subject_id, subject)
        labels.setdefault(object_id, obj)
        graph.add_edge(subject_id, object_id)

        taxonomy_parts = [row["media_format"], row["user_intent"], row["production_type"]]
        taxonomy_summary = " · ".join(str(part) for part in taxonomy_parts if part)
        source_payload = {
            "source_id": int(row["source_id"]),
            "title": row["title"] or row["url"],
            "url": row["url"],
            "platform": row["platform"],
            "published_year": row["published_year"],
            "quantum_domain": row["quantum_domain"],
        }

        for node_id, other_label in ((subject_id, obj), (object_id, subject)):
            relation_counts_by_node.setdefault(node_id, Counter())
            taxonomy_counts_by_node.setdefault(node_id, Counter())
            domain_counts_by_node.setdefault(node_id, Counter())
            neighbor_shared_counts_by_node.setdefault(node_id, Counter())
            supporting_sources_by_node.setdefault(node_id, [])
            seen_sources_by_node.setdefault(node_id, set())

            if relation:
                relation_counts_by_node[node_id][relation] += 1
            if taxonomy_summary:
                taxonomy_counts_by_node[node_id][taxonomy_summary] += 1
            if row["quantum_domain"]:
                domain_counts_by_node[node_id][str(row["quantum_domain"])] += 1
            if other_label:
                neighbor_shared_counts_by_node[node_id][other_label] += 1

            source_id = int(row["source_id"])
            if source_id not in seen_sources_by_node[node_id]:
                seen_sources_by_node[node_id].add(source_id)
                supporting_sources_by_node[node_id].append(source_payload)

    _, bet_rank_lookup, bet_score_lookup = _graph_metrics_lookup(conn)
    node_count = graph.number_of_nodes()
    degrees = {node_id: int(graph.degree(node_id)) for node_id in graph.nodes}
    sorted_degrees = sorted(degrees.items(), key=lambda item: (-item[1], labels.get(item[0], item[0])))
    degree_rank_lookup = {node_id: index + 1 for index, (node_id, _degree) in enumerate(sorted_degrees)}

    components = sorted(nx.connected_components(graph), key=len, reverse=True)
    component_lookup: dict[str, tuple[int, int]] = {}
    for index, component in enumerate(components, start=1):
        component_size = len(component)
        for node_id in component:
            component_lookup[node_id] = (index, component_size)

    registry: dict[str, Any] = {}
    for node_id in graph.nodes:
        degree_value = degrees.get(node_id, 0)
        component_rank, component_size = component_lookup.get(node_id, (None, 1))
        neighbor_ids = sorted(
            graph.neighbors(node_id),
            key=lambda item: (-degrees.get(item, 0), labels.get(item, item)),
        )[:8]
        _label = labels.get(node_id, node_id)
        _role = _hub_role(_label)
        _top_rels = [r for r, _ in relation_counts_by_node.get(node_id, Counter()).most_common(3)]
        _top_neighbors = [labels.get(n, n) for n in neighbor_ids[:3]]
        _top_domains = [d for d, _ in domain_counts_by_node.get(node_id, Counter()).most_common(2)]
        _src_count = len(supporting_sources_by_node.get(node_id, []))
        _what = (
            f'"{_label}" is a {_role}-type concept appearing in {degree_value} relations '
            f"across {_src_count} source{'s' if _src_count != 1 else ''} in the Thai quantum media corpus."
        )
        _why = (
            (f"Connected via {', '.join(_top_rels[:2])} relations. " if _top_rels else "")
            + (f"Co-occurs with: {', '.join(_top_neighbors)}. " if _top_neighbors else "")
            + (f"Domains: {', '.join(_top_domains)}." if _top_domains else "")
        ).strip() or "No additional context available."
        registry[node_id] = {
            "id": node_id,
            "label": _label,
            "summary": {
                "what_it_is": _what,
                "why_it_matters": _why,
                "hub_role": _role,
            },
            "metrics": {
                "degree": degree_value,
                "degree_centrality": round((degree_value / max(node_count - 1, 1)), 6),
                "betweenness_centrality": round(bet_score_lookup[node_id], 6) if node_id in bet_score_lookup else None,
                "component_rank": component_rank,
                "component_size": component_size,
                "degree_rank": degree_rank_lookup.get(node_id),
                "betweenness_rank": bet_rank_lookup.get(node_id),
            },
            "neighbors": [
                {
                    "id": neighbor_id,
                    "label": labels.get(neighbor_id, neighbor_id),
                    "degree": degrees.get(neighbor_id, 0),
                    "shared_links": neighbor_shared_counts_by_node.get(node_id, Counter()).get(labels.get(neighbor_id, neighbor_id), 0),
                }
                for neighbor_id in neighbor_ids
            ],
            "top_relations": [
                {"label": label, "count": count}
                for label, count in relation_counts_by_node.get(node_id, Counter()).most_common(6)
            ],
            "supporting_sources_count": len(supporting_sources_by_node.get(node_id, [])),
            "supporting_sources": supporting_sources_by_node.get(node_id, [])[:10],
            "taxonomy_context": [
                {"label": label, "count": count}
                for label, count in taxonomy_counts_by_node.get(node_id, Counter()).most_common(4)
            ],
            "domain_context": [
                {"label": label, "count": count}
                for label, count in domain_counts_by_node.get(node_id, Counter()).most_common(4)
            ],
            "nearby_concepts": [labels.get(neighbor_id, neighbor_id) for neighbor_id in neighbor_ids[:5]],
        }

    return registry


def _get_node_registry(conn: sqlite3.Connection) -> dict[str, Any]:
    global _node_registry_mem, _node_registry_ts
    now = time.monotonic()
    if _node_registry_mem is not None and (now - _node_registry_ts) < _NODE_REGISTRY_TTL:
        return _node_registry_mem

    read_only = bool(settings.database_read_only)

    if read_only:
        # Read-only connection: skip DB cache entirely, use in-memory only
        registry = _build_graph_node_detail_registry(conn)
    else:
        cache = StatsCacheRepo(conn)
        cached = cache.get("graph:node_details")
        registry = cached if isinstance(cached, dict) else _build_graph_node_detail_registry(conn)
        if not isinstance(cached, dict):
            try:
                cache.set("graph:node_details", registry)
            except Exception:
                pass  # Write failed (permissions, disk full) — in-memory cache still works

    _node_registry_mem = registry
    _node_registry_ts = now
    return registry


def _graph_node_detail_payload(conn: sqlite3.Connection, node_id: str) -> dict[str, Any] | None:
    normalized_id = _norm_concept(node_id)
    registry = _get_node_registry(conn)
    payload = registry.get(normalized_id)
    return payload if isinstance(payload, dict) else None


def _process_community_submission(submission_id: int, url: str) -> None:
    """
    Best-effort post-submit processing.
    If the URL is not yet present in `sources` or lacks usable text, keep the
    submission accepted but mark it as limited rather than failing the request.
    """
    from siamquantum.db.repos import StatsCacheRepo, TripletRepo
    from siamquantum.models import TripletCreate
    from siamquantum.pipeline.analyze import run_stats
    from siamquantum.services import claude

    db = _db()
    with get_connection(db) as conn:
        sub_repo = CommunitySubmissionRepo(conn)
        source = SourceRepo(conn).get_by_url(url)
        sub_repo.update_status(submission_id, "queued")

    if source is None:
        with get_connection(db) as conn:
            StatsCacheRepo(conn).invalidate_prefix("ttest:")
            CommunitySubmissionRepo(conn).update_status(submission_id, "queued_no_source")
        return

    text = (source.raw_text or source.title or "").strip()
    if not text:
        with get_connection(db) as conn:
            StatsCacheRepo(conn).invalidate_prefix("ttest:")
            CommunitySubmissionRepo(conn).update_status(submission_id, "queued_no_text")
        return

    try:
        triplets = claude.extract_triplets(text)
        with get_connection(db) as conn:
            if triplets:
                TripletRepo(conn).insert_many(
                    [
                        TripletCreate(
                            source_id=source.id,
                            subject=t.subject,
                            relation=t.relation,
                            object=t.object,
                            confidence=t.confidence,
                        )
                        for t in triplets
                    ]
                )
            StatsCacheRepo(conn).invalidate_prefix("ttest:")

        # Reuse the existing stats pipeline as the minimal DenStream refresh path.
        run_stats(db)

        with get_connection(db) as conn:
            CommunitySubmissionRepo(conn).update_status(submission_id, "processed")
    except Exception:
        with get_connection(db) as conn:
            CommunitySubmissionRepo(conn).update_status(submission_id, "failed")


# ---------------------------------------------------------------------------
# Root redirect
# ---------------------------------------------------------------------------

@app.get("/", include_in_schema=False)
def root(request: Request) -> Any:
    return templates.TemplateResponse(request, "home.html", {"active": "home"})


# ---------------------------------------------------------------------------
# Page routes
# ---------------------------------------------------------------------------

@app.get("/dashboard", include_in_schema=False)
def page_dashboard(request: Request) -> Any:
    return templates.TemplateResponse(request, "dashboard.html")


@app.get("/network", include_in_schema=False)
def page_network(request: Request) -> Any:
    return templates.TemplateResponse(request, "network.html")


@app.get("/analytics", include_in_schema=False)
def page_analytics(request: Request) -> Any:
    return templates.TemplateResponse(request, "analytics.html")


@app.get("/database", include_in_schema=False)
def page_database(request: Request) -> Any:
    return templates.TemplateResponse(request, "database.html")


@app.get("/community", include_in_schema=False)
def page_community(request: Request) -> Any:
    return templates.TemplateResponse(
        request,
        "community.html",
        {"demo_mode": _is_vercel_demo_mode()},
    )


# ---------------------------------------------------------------------------
# API — geo
# ---------------------------------------------------------------------------

@app.get("/api/geo/list")
def api_geo_list(
    cdn: bool = Query(False, description="Include CDN-resolved rows"),
    include_filtered: bool = Query(False, description="Include rows outside the operational corpus-scope filter"),
) -> JSONResponse:
    """
    Returns geo rows joined with source metadata.
    Default (cdn=false): only origin IPs (is_cdn_resolved=0 or NULL).
    Default: only corpus-scope rows (is_quantum_tech=1 AND is_thailand_related=1).
    """
    db = _db()
    relevance_clause = "" if include_filtered else "AND s.is_quantum_tech = 1 AND s.is_thailand_related = 1"
    try:
        with get_connection(db) as conn:
            relevance = _relevance_metadata(conn)
            if cdn:
                rows = conn.execute(f"""
                    SELECT g.source_id, g.lat, g.lng, g.city, g.region,
                           g.isp, g.asn_org, g.is_cdn_resolved,
                           s.platform, s.url, s.title, s.published_year
                    FROM geo g
                    JOIN sources s ON g.source_id = s.id
                    WHERE g.lat IS NOT NULL AND g.lng IS NOT NULL
                    {relevance_clause}
                    ORDER BY s.published_year DESC, g.source_id DESC
                    LIMIT 500
                """).fetchall()
            else:
                rows = conn.execute(f"""
                    SELECT g.source_id, g.lat, g.lng, g.city, g.region,
                           g.isp, g.asn_org, g.is_cdn_resolved,
                           s.platform, s.url, s.title, s.published_year
                    FROM geo g
                    JOIN sources s ON g.source_id = s.id
                    WHERE g.lat IS NOT NULL AND g.lng IS NOT NULL
                      AND (g.is_cdn_resolved = 0 OR g.is_cdn_resolved IS NULL)
                    {relevance_clause}
                    ORDER BY s.published_year DESC, g.source_id DESC
                    LIMIT 500
                """).fetchall()
    except Exception as exc:
        return JSONResponse(
            {
                "ok": False,
                "data": [],
                "count": 0,
                "relevance": None,
                "error": {
                    "code": "geo_list_failed",
                    "message": str(exc),
                },
            },
            status_code=500,
        )

    items = [dict(r) for r in rows]
    return JSONResponse(
        {
            "ok": True,
            "data": items,
            "count": len(items),
            "relevance": relevance,
            "error": None,
        }
    )


# ---------------------------------------------------------------------------
# API — graph (nodes + edges for 3D force graph)
# ---------------------------------------------------------------------------

@app.get("/api/graph")
def api_graph(
    include_filtered: bool = Query(True, description="Include all rows, not just the operational corpus-scope filter"),
) -> JSONResponse:
    """
    Returns concept-level nodes and edges built from triplets.
    Nodes = unique subject/object concept texts. Edges = subject→object per triplet.
    """
    db = _db()
    relevance_clause = "" if include_filtered else "AND s.is_quantum_tech = 1 AND s.is_thailand_related = 1"
    try:
        with get_connection(db) as conn:
            relevance = _relevance_metadata(conn)
            edge_rows = conn.execute(f"""
                SELECT t.subject, t.relation, t.object, t.confidence
                FROM triplets t
                JOIN sources s ON t.source_id = s.id
                WHERE 1=1 {relevance_clause}
                ORDER BY t.id
                LIMIT 10000
            """).fetchall()
    except Exception as exc:
        return JSONResponse(
            {
                "ok": False,
                "data": {"nodes": [], "links": []},
                "relevance": None,
                "error": {"code": "graph_load_failed", "message": str(exc)},
            },
            status_code=500,
        )

    # concept registry: norm_key → display label (first seen)
    concept_label: dict[str, str] = {}
    # degree counter: norm_key → int
    degree: dict[str, int] = {}
    # edge aggregation: (src_key, tgt_key) → {relation, count, confidence_sum}
    EdgeVal = dict[str, Any]
    edge_agg: dict[tuple[str, str], EdgeVal] = {}

    for row in edge_rows:
        subj_raw = (row[0] or "").strip()
        rel_raw = (row[1] or "").strip()
        obj_raw = (row[2] or "").strip()
        conf = float(row[3] or 0.5)

        subj_key = _norm_concept(subj_raw)
        obj_key = _norm_concept(obj_raw)

        if len(subj_key) < 2 or len(obj_key) < 2:
            continue
        if subj_key == obj_key:
            continue

        if subj_key not in concept_label:
            concept_label[subj_key] = subj_raw
        if obj_key not in concept_label:
            concept_label[obj_key] = obj_raw

        degree[subj_key] = degree.get(subj_key, 0) + 1
        degree[obj_key] = degree.get(obj_key, 0) + 1

        edge_key = (subj_key, obj_key)
        if edge_key not in edge_agg:
            edge_agg[edge_key] = {"relation": rel_raw, "count": 0, "conf_sum": 0.0}
        edge_agg[edge_key]["count"] += 1
        edge_agg[edge_key]["conf_sum"] += conf

    nodes = [
        {
            "id": key,
            "label": concept_label[key],
            "val": max(1, degree.get(key, 1)),
        }
        for key in concept_label
    ]

    links = [
        {
            "source": src,
            "target": tgt,
            "label": agg["relation"],
            "value": agg["count"],
        }
        for (src, tgt), agg in edge_agg.items()
    ]

    resp = JSONResponse(
        {
            "ok": True,
            "data": {"nodes": nodes, "links": links},
            "relevance": relevance,
            "error": None,
        }
    )
    resp.headers["Cache-Control"] = "public, max-age=3600"
    return resp


# ---------------------------------------------------------------------------
# API — graph metrics
# ---------------------------------------------------------------------------

def _api_graph_node_detail(node_id: str) -> JSONResponse:
    db = _db()
    try:
        with get_connection(db) as conn:
            relevance = _relevance_metadata(conn)
            payload = _graph_node_detail_payload(conn, node_id)
    except Exception as exc:
        return JSONResponse(
            {
                "ok": False,
                "data": None,
                "relevance": None,
                "error": {"code": "graph_node_detail_failed", "message": str(exc)},
            },
            status_code=500,
        )

    if payload is None:
        return JSONResponse(
            {
                "ok": False,
                "data": None,
                "relevance": relevance,
                "error": {"code": "graph_node_not_found", "message": "Node not found"},
            },
            status_code=404,
        )

    resp = JSONResponse({"ok": True, "data": payload, "relevance": relevance, "error": None})
    resp.headers["Cache-Control"] = "public, max-age=3600"
    return resp


@app.get("/api/graph/node")
def api_graph_node_detail_query(node_id: str = Query(..., min_length=1)) -> JSONResponse:
    """Query-string variant for concept ids that are awkward in path segments."""
    return _api_graph_node_detail(node_id)


@app.get("/api/graph/node/{node_id:path}")
def api_graph_node_detail(node_id: str) -> JSONResponse:
    """Path variant retained for compatibility."""
    return _api_graph_node_detail(node_id)


@app.get("/api/graph/metrics")
def api_graph_metrics() -> JSONResponse:
    """Degree centrality, betweenness centrality, connected components."""
    from siamquantum.pipeline.graph_metrics import compute_metrics
    db = _db()
    try:
        with get_connection(db) as conn:
            cache = StatsCacheRepo(conn)
            metrics = cache.get("graph:metrics")
        if not metrics:
            metrics = compute_metrics(db)
            # compute_metrics internally might try to write to cache, 
            # but if it uses get_connection(db) it will follow the read_only setting.
            # However, if it's already computed and returned, we are good.
    except Exception as exc:
        return JSONResponse(
            {"ok": False, "data": None, "error": {"code": "metrics_failed", "message": str(exc)}},
            status_code=500,
        )
    return JSONResponse({"ok": True, "data": metrics, "error": None})


# ---------------------------------------------------------------------------
# API — taxonomy summary
# ---------------------------------------------------------------------------

@app.get("/api/taxonomy/summary")
def api_taxonomy_summary() -> JSONResponse:
    """media_format and user_intent distributions from entities."""
    db = _db()
    try:
        with get_connection(db) as conn:
            mf_rows = conn.execute(
                "SELECT media_format, COUNT(*) AS n FROM entities WHERE media_format IS NOT NULL GROUP BY media_format ORDER BY n DESC"
            ).fetchall()
            ui_rows = conn.execute(
                "SELECT user_intent, COUNT(*) AS n FROM entities WHERE user_intent IS NOT NULL GROUP BY user_intent ORDER BY n DESC"
            ).fetchall()
            thai_count = conn.execute(
                "SELECT COUNT(*) FROM entities WHERE thai_cultural_angle IS NOT NULL AND thai_cultural_angle != ''"
            ).fetchone()[0]
            qd_rows = conn.execute(
                "SELECT quantum_domain, COUNT(*) AS n FROM sources WHERE quantum_domain IS NOT NULL GROUP BY quantum_domain ORDER BY n DESC"
            ).fetchall()
    except Exception as exc:
        return JSONResponse(
            {"ok": False, "data": None, "error": {"code": "taxonomy_failed", "message": str(exc)}},
            status_code=500,
        )
    return JSONResponse({
        "ok": True,
        "data": {
            "media_format": [{"label": r[0], "count": r[1]} for r in mf_rows],
            "user_intent": [{"label": r[0], "count": r[1]} for r in ui_rows],
            "thai_cultural_angle_count": thai_count,
            "quantum_domain": [{"label": r[0], "count": r[1]} for r in qd_rows],
        },
        "error": None,
    })


# ---------------------------------------------------------------------------
# API — taxonomy stats (cached analysis)
# ---------------------------------------------------------------------------

@app.get("/api/taxonomy/stats")
def api_taxonomy_stats() -> JSONResponse:
    """Return cached taxonomy engagement analysis. Run analyze taxonomy-stats to populate."""
    db = _db()
    keys = [
        "taxonomy:media_format",
        "taxonomy:user_intent",
        "taxonomy:thai_cultural_angle",
        "taxonomy:media_x_intent:chi2",
        "taxonomy:media_x_intent:engagement",
        "taxonomy:insight:strongest_trend",
    ]
    try:
        with get_connection(db) as conn:
            rows = conn.execute(
                "SELECT key, value FROM stats_cache WHERE key IN ({})".format(
                    ",".join("?" for _ in keys)
                ),
                keys,
            ).fetchall()
            data = {
                r["key"].replace("taxonomy:", ""): json.loads(r["value"])
                for r in rows
            }
    except Exception as exc:
        return JSONResponse(
            {"ok": False, "data": None, "error": {"code": "taxonomy_stats_failed", "message": str(exc)}},
            status_code=500,
        )
    return JSONResponse({"ok": True, "data": data, "error": None})


# ---------------------------------------------------------------------------
# API — stats
# ---------------------------------------------------------------------------

@app.get("/api/stats/yearly")
def api_stats_yearly(
    include_filtered: bool = Query(False, description="Include rows outside the operational corpus-scope filter"),
) -> JSONResponse:
    """
    Yearly source counts, bootstrap engagement inference, and trend tests.
    Default: only quantum+thai relevant sources.
    Method: bootstrap geometric mean on log1p(view_count). Scope: Thai web/social engagement only.
    """
    db = _db()
    relevance_clause = "" if include_filtered else "WHERE s.is_quantum_tech = 1 AND s.is_thailand_related = 1"
    relevance_join_clause = "" if include_filtered else "AND s.is_quantum_tech = 1 AND s.is_thailand_related = 1"
    _empty_payload: dict[str, Any] = {
        "scope": "thai_web_engagement",
        "scope_caveat": (
            "Excludes academic publications in English journals and institutional reports "
            "not indexed by GDELT/YouTube. Coverage: 0.4% academic/gov sources (3 of 768)."
        ),
        "method": "bootstrap_geometric_mean",
        "years": [],
        "counts": {},
        "engagement_distribution": {},
        "trendlines": {"total_sources": [], "high_engagement": []},
        "yearly_bootstrap": [],
        "pairwise": [],
        "trend": {},
        "macro_clusters": [],
        "significance": [],
    }
    try:
        with get_connection(db) as conn:
            count_rows = conn.execute(f"""
                SELECT s.published_year, s.platform, COUNT(*) AS n
                FROM sources s
                {relevance_clause}
                GROUP BY s.published_year, s.platform
                ORDER BY s.published_year, s.platform
            """).fetchall()

            eng_rows = conn.execute(f"""
                SELECT s.published_year, e.engagement_level, COUNT(*) AS n
                FROM entities e
                JOIN sources s ON e.source_id = s.id
                WHERE 1=1 {relevance_join_clause}
                GROUP BY s.published_year, e.engagement_level
                ORDER BY s.published_year, e.engagement_level
            """).fetchall()

            clusters_row = conn.execute(
                "SELECT value FROM stats_cache WHERE key = 'macro_clusters'"
            ).fetchone()
            clusters_raw = json.loads(clusters_row["value"]) if clusters_row else None
            clusters = clusters_raw if isinstance(clusters_raw, list) else []

            bootstrap_yearly_rows = conn.execute(
                "SELECT key, value FROM stats_cache WHERE key LIKE 'bootstrap_yearly:%'"
            ).fetchall()
            bootstrap_pairwise_rows = conn.execute(
                "SELECT key, value FROM stats_cache WHERE key LIKE 'bootstrap_pairwise:%'"
            ).fetchall()
            trend_row = conn.execute(
                "SELECT value FROM stats_cache WHERE key = 'bootstrap_trend'"
            ).fetchone()
            trend_raw = json.loads(trend_row["value"]) if trend_row else None
    except Exception as exc:
        return JSONResponse(
            {
                "ok": False,
                "data": _empty_payload,
                "relevance": None,
                "error": {"code": "yearly_stats_failed", "message": str(exc)},
            },
            status_code=500,
        )

    counts: dict[str, dict[str, int]] = {}
    for row in count_rows:
        yr = str(row["published_year"])
        if yr not in counts:
            counts[yr] = {"total": 0}
        counts[yr][row["platform"]] = row["n"]
        counts[yr]["total"] += row["n"]

    eng_dist: dict[str, dict[str, int]] = {}
    for row in eng_rows:
        yr = str(row["published_year"])
        if yr not in eng_dist:
            eng_dist[yr] = {}
        eng_dist[yr][row["engagement_level"]] = row["n"]

    yearly_bootstrap: list[Any] = []
    for row in bootstrap_yearly_rows:
        try:
            yearly_bootstrap.append(json.loads(row["value"]))
        except Exception:
            continue
    yearly_bootstrap.sort(key=lambda x: x.get("year", 0))

    pairwise: list[Any] = []
    for row in bootstrap_pairwise_rows:
        try:
            pairwise.append(json.loads(row["value"]))
        except Exception:
            continue
    pairwise.sort(key=lambda x: (x.get("year_a", 0), x.get("year_b", 0)))

    trend: dict[str, Any] = trend_raw if isinstance(trend_raw, dict) else {}

    year_numbers = sorted(
        {int(y) for y in counts.keys()} | {int(y) for y in eng_dist.keys() if str(y).isdigit()}
    )
    years = [str(y) for y in year_numbers]
    trend_total_sources = [int((counts.get(y) or {}).get("total", 0)) for y in years]
    trend_high_engagement = [int((eng_dist.get(y) or {}).get("high", 0)) for y in years]

    with get_connection(db) as conn:
        relevance = _relevance_metadata(conn)

    return JSONResponse({
        "ok": True,
        "data": {
            "scope": "thai_web_engagement",
            "scope_caveat": (
                "Excludes academic publications in English journals and institutional reports "
                "not indexed by GDELT/YouTube. Coverage: 0.4% academic/gov sources (3 of 768)."
            ),
            "relevance_scope_note": (
                "Corpus scope is currently operational: relevance flags represent the active Thai-quantum corpus boundary, "
                "not per-row classifier verification."
            ),
            "method": "bootstrap_geometric_mean",
            "years": years,
            "counts": counts,
            "engagement_distribution": eng_dist,
            "trendlines": {
                "total_sources": trend_total_sources,
                "high_engagement": trend_high_engagement,
            },
            "yearly_bootstrap": yearly_bootstrap,
            "pairwise": pairwise,
            "trend": trend,
            "macro_clusters": clusters,
            "significance": [],
        },
        "relevance": relevance,
        "error": None,
    })


@app.get("/api/analytics/yearly_taxonomy")
def api_analytics_yearly_taxonomy(
    include_filtered: bool = Query(False, description="Include rows outside the operational corpus-scope filter"),
) -> JSONResponse:
    """Fine-grained yearly topic and production analytics with validation tests and graph payloads."""
    db = _db()
    where = "" if include_filtered else "WHERE s.is_quantum_tech = 1 AND s.is_thailand_related = 1"
    try:
        with get_connection(db) as conn:
            relevance = _relevance_metadata(conn)
            rows = conn.execute(
                f"""
                SELECT s.published_year, s.view_count, s.quantum_domain,
                       e.area, e.content_type, e.production_type,
                       e.media_format, e.media_format_detail, e.user_intent
                FROM sources s
                LEFT JOIN entities e ON s.id = e.source_id
                {where}
                ORDER BY s.published_year ASC, s.id ASC
                """
            ).fetchall()
    except Exception as exc:
        return JSONResponse(
            {
                "ok": False,
                "data": {
                    "topics": {"labels": [], "years": [], "series": [], "tests": {}, "graph": {"nodes": [], "links": [], "community_summaries": []}},
                    "productions": {"labels": [], "years": [], "series": [], "tests": {}, "graph": {"nodes": [], "links": [], "community_summaries": []}},
                    "method_note": "",
                },
                "relevance": None,
                "error": {"code": "yearly_taxonomy_failed", "message": str(exc)},
            },
            status_code=500,
        )

    payload = build_yearly_taxonomy_analytics([dict(row) for row in rows])
    return JSONResponse(
        {
            "ok": True,
            "data": payload,
            "relevance": relevance,
            "error": None,
        }
    )


# ---------------------------------------------------------------------------
# API — database (paginated source list)
# ---------------------------------------------------------------------------

@app.get("/api/sources")
def api_sources(
    year: int | None = Query(None),
    platform: str | None = Query(None),
    content_type: str | None = Query(None),
    media_format: str | None = Query(None),
    user_intent: str | None = Query(None),
    quantum_domain: str | None = Query(None),
    include_filtered: bool = Query(False, description="Include rows outside the operational corpus-scope filter"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
) -> JSONResponse:
    """Paginated source list with optional filters. Default: operational corpus-scope rows only."""
    db = _db()
    conditions = []
    params: list[Any] = []

    if not include_filtered:
        conditions.append("s.is_quantum_tech = 1")
        conditions.append("s.is_thailand_related = 1")
    if year is not None:
        conditions.append("s.published_year = ?")
        params.append(year)
    if platform is not None:
        conditions.append("s.platform = ?")
        params.append(platform)
    if content_type is not None:
        conditions.append("e.content_type = ?")
        params.append(content_type)
    if media_format is not None:
        conditions.append("e.media_format = ?")
        params.append(media_format)
    if user_intent is not None:
        conditions.append("e.user_intent = ?")
        params.append(user_intent)
    if quantum_domain is not None:
        conditions.append("s.quantum_domain = ?")
        params.append(quantum_domain)

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    offset = (page - 1) * page_size

    try:
        with get_connection(db) as conn:
            relevance = _relevance_metadata(conn)
            total = conn.execute(f"""
                SELECT COUNT(*) FROM sources s
                LEFT JOIN entities e ON s.id = e.source_id
                {where}
            """, params).fetchone()[0]

            rows = conn.execute(f"""
                SELECT s.id, s.platform, s.url, s.title, s.published_year,
                       s.view_count, s.like_count, s.comment_count,
                       s.quantum_domain,
                       e.content_type, e.production_type, e.area, e.engagement_level,
                       e.media_format, e.user_intent
                FROM sources s
                LEFT JOIN entities e ON s.id = e.source_id
                {where}
                ORDER BY s.published_year DESC, s.id DESC
                LIMIT ? OFFSET ?
            """, [*params, page_size, offset]).fetchall()
    except Exception as exc:
        return JSONResponse(
            {
                "ok": False,
                "data": {
                    "total": 0,
                    "page": page,
                    "page_size": page_size,
                    "items": [],
                },
                "relevance": None,
                "error": {
                    "code": "sources_query_failed",
                    "message": str(exc),
                },
            },
            status_code=500,
        )

    return JSONResponse(
        {
            "ok": True,
            "data": {
                "total": int(total),
                "page": page,
                "page_size": page_size,
                "items": [dict(r) for r in rows],
            },
            "relevance": relevance,
            "error": None,
        }
    )


# ---------------------------------------------------------------------------
# API — corpus coverage summary
# ---------------------------------------------------------------------------

@app.get("/api/corpus/coverage")
def api_corpus_coverage() -> JSONResponse:
    """Year-by-platform breakdown for the current operational corpus boundary."""
    db = _db()
    try:
        with get_connection(db) as conn:
            relevance = _relevance_metadata(conn)
            rows = conn.execute("""
                SELECT published_year, platform, COUNT(*) AS n
                FROM sources
                WHERE is_quantum_tech = 1 AND is_thailand_related = 1
                GROUP BY published_year, platform
                ORDER BY published_year, platform
            """).fetchall()
            domain_rows = conn.execute("""
                SELECT quantum_domain, COUNT(*) AS n
                FROM sources
                WHERE quantum_domain IS NOT NULL
                  AND is_quantum_tech = 1 AND is_thailand_related = 1
                GROUP BY quantum_domain
                ORDER BY n DESC
            """).fetchall()
            total = conn.execute(
                "SELECT COUNT(*) FROM sources WHERE is_quantum_tech = 1 AND is_thailand_related = 1"
            ).fetchone()[0]
    except Exception as exc:
        return JSONResponse(
            {
                "ok": False,
                "data": None,
                "relevance": None,
                "error": {"code": "coverage_failed", "message": str(exc)},
            },
            status_code=500,
        )

    # Build year → {platform: count} map
    by_year: dict[str, dict[str, int]] = {}
    for r in rows:
        yr = str(r["published_year"])
        if yr not in by_year:
            by_year[yr] = {}
        by_year[yr][r["platform"]] = r["n"]

    return JSONResponse({
        "ok": True,
        "data": {
            "total": int(total),
            "by_year": by_year,
            "by_domain": [{"domain": r["quantum_domain"], "count": r["n"]} for r in domain_rows],
            "years": sorted(by_year.keys()),
        },
        "relevance": relevance,
        "error": None,
    })


# ---------------------------------------------------------------------------
# API — format × intent engagement matrix
# ---------------------------------------------------------------------------

@app.get("/api/analytics/engagement_matrix")
def api_engagement_matrix() -> JSONResponse:
    """Cross-tabulation of media_format × user_intent using bootstrap geometric means on log1p(view_count)."""
    db = _db()
    try:
        with get_connection(db) as conn:
            relevance = _relevance_metadata(conn)
            cached = StatsCacheRepo(conn).get("taxonomy:media_x_intent:engagement") or {}
            formats = conn.execute(
                "SELECT DISTINCT media_format FROM entities WHERE media_format IS NOT NULL ORDER BY media_format"
            ).fetchall()
            intents = conn.execute(
                "SELECT DISTINCT user_intent FROM entities WHERE user_intent IS NOT NULL ORDER BY user_intent"
            ).fetchall()
    except Exception as exc:
        return JSONResponse(
            {
                "ok": False,
                "data": None,
                "relevance": None,
                "error": {"code": "matrix_failed", "message": str(exc)},
            },
            status_code=500,
        )

    cells = cached.get("cells") if isinstance(cached, dict) else None
    cells = cells if isinstance(cells, list) else []
    return JSONResponse({
        "ok": True,
        "data": {
            "cells": cells,
            "strongest_cell": cached.get("strongest_cell") if isinstance(cached, dict) else None,
            "formats": [r[0] for r in formats],
            "intents": [r[0] for r in intents],
        },
        "relevance": relevance,
        "error": None,
    })


# ---------------------------------------------------------------------------
# API — XLSX export
# ---------------------------------------------------------------------------

@app.get("/api/export/xlsx")
def api_export_xlsx(
    year: int | None = Query(None),
    platform: str | None = Query(None),
    content_type: str | None = Query(None),
) -> Any:
    """Stream an XLSX file of sources + entities."""
    import openpyxl
    from openpyxl.styles import Font

    db = _db()
    conditions = []
    params: list[Any] = []
    if year is not None:
        conditions.append("s.published_year = ?")
        params.append(year)
    if platform is not None:
        conditions.append("s.platform = ?")
        params.append(platform)
    if content_type is not None:
        conditions.append("e.content_type = ?")
        params.append(content_type)
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    try:
        with get_connection(db) as conn:
            rows = conn.execute(f"""
                SELECT s.id, s.platform, s.url, s.title, s.published_year,
                       s.view_count, s.like_count, s.comment_count,
                       e.content_type, e.production_type, e.area, e.engagement_level,
                       g.lat, g.lng, g.city, g.is_cdn_resolved
                FROM sources s
                LEFT JOIN entities e ON s.id = e.source_id
                LEFT JOIN geo g ON s.id = g.source_id
                {where}
                ORDER BY s.published_year DESC, s.id DESC
            """, params).fetchall()

        wb = openpyxl.Workbook()
        ws = wb.active
        assert ws is not None
        ws.title = "Sources"

        headers = [
            "ID", "Platform", "URL", "Title", "Year",
            "Views", "Likes", "Comments",
            "Content Type", "Production Type", "Area", "Engagement Level",
            "Lat", "Lng", "City", "CDN Resolved",
        ]
        ws.append(headers)
        for cell in ws[1]:
            cell.font = Font(bold=True)

        for row in rows:
            ws.append(list(dict(row).values()))

        buf = io.BytesIO()
        wb.save(buf)
        buf.seek(0)
    except Exception as exc:
        return JSONResponse(
            {
                "ok": False,
                "data": None,
                "error": {
                    "code": "xlsx_export_failed",
                    "message": str(exc),
                },
            },
            status_code=500,
        )

    filename_parts = ["siamquantum_atlas"]
    if year is not None:
        filename_parts.append(str(year))
    if platform:
        filename_parts.append(platform)
    if content_type:
        filename_parts.append(content_type)
    filename = "_".join(filename_parts) + ".xlsx"
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ---------------------------------------------------------------------------
# API — community submissions queue
# ---------------------------------------------------------------------------

@app.get("/api/community/submissions")
def api_community_submissions(limit: int = Query(8, ge=1, le=25)) -> JSONResponse:
    """Return recent community submissions for the local review queue."""
    try:
        with get_connection(_db()) as conn:
            rows = CommunitySubmissionRepo(conn).list_recent(limit=limit)
    except Exception as exc:
        return JSONResponse(
            {
                "ok": False,
                "data": {"items": []},
                "error": {"code": "community_list_failed", "message": str(exc)},
            },
            status_code=500,
        )

    return JSONResponse(
        {
            "ok": True,
            "data": {"items": [row.model_dump() for row in rows]},
            "error": None,
        }
    )


# ---------------------------------------------------------------------------
# API — community submission
# ---------------------------------------------------------------------------

@app.post("/api/community/submit", status_code=201)
def api_community_submit(
    payload: dict[str, Any],
    background_tasks: BackgroundTasks,
) -> JSONResponse:
    """Accept a community URL submission."""
    if _is_vercel_demo_mode():
        return JSONResponse(
            {
                "ok": False,
                "data": None,
                "error": {
                    "code": "community_disabled_in_demo",
                    "message": (
                        "Community submissions are disabled in Vercel demo mode because the bundled SQLite dataset is served read-only."
                    ),
                },
            },
            status_code=503,
        )
    url = (payload.get("url") or "").strip()
    if not url:
        return JSONResponse(
            {
                "ok": False,
                "data": None,
                "error": {
                    "code": "url_required",
                    "message": "url is required",
                },
            },
            status_code=422,
        )
    handle = (payload.get("handle") or "").strip() or None

    try:
        db = _db()
        with get_connection(db) as conn:
            sub_repo = CommunitySubmissionRepo(conn)
            sub_id = sub_repo.insert(
                CommunitySubmissionCreate(handle=handle, url=url)
            )
            sub_repo.update_status(sub_id, "queued")
    except Exception as exc:
        return JSONResponse(
            {
                "ok": False,
                "data": None,
                "error": {
                    "code": "community_submit_failed",
                    "message": str(exc),
                },
            },
            status_code=500,
        )

    background_tasks.add_task(_process_community_submission, sub_id, url)
    return JSONResponse(
        {
            "ok": True,
            "data": {
                "id": sub_id,
                "status": "queued",
                "message": (
                    "Submission accepted and queued for best-effort processing. "
                    "If source text or external NLP is unavailable, the row stays stored."
                ),
            },
            "error": None,
        },
        status_code=201,
    )


# ---------------------------------------------------------------------------
# API — home page summary
# ---------------------------------------------------------------------------

@app.get("/api/stats/summary")
def api_stats_summary() -> JSONResponse:
    """Key corpus stats for the home page."""
    db = _db()
    try:
        with get_connection(db) as conn:
            total_sources = int(conn.execute(
                "SELECT COUNT(*) FROM sources WHERE is_quantum_tech = 1 AND is_thailand_related = 1"
            ).fetchone()[0])
            total_triplets = int(conn.execute("SELECT COUNT(*) FROM triplets").fetchone()[0])
            year_row = conn.execute(
                "SELECT MIN(published_year), MAX(published_year) FROM sources "
                "WHERE is_quantum_tech = 1 AND is_thailand_related = 1"
            ).fetchone()
            geo_count = int(conn.execute(
                "SELECT COUNT(DISTINCT g.source_id) FROM geo g "
                "JOIN sources s ON g.source_id = s.id "
                "WHERE s.is_quantum_tech = 1 AND s.is_thailand_related = 1"
            ).fetchone()[0])
            platform_rows = conn.execute(
                "SELECT platform, COUNT(*) AS n FROM sources "
                "WHERE is_quantum_tech = 1 AND is_thailand_related = 1 "
                "GROUP BY platform ORDER BY n DESC LIMIT 6"
            ).fetchall()
    except Exception as exc:
        return JSONResponse(
            {"ok": False, "data": None, "error": {"code": "summary_failed", "message": str(exc)}},
            status_code=500,
        )
    return JSONResponse({
        "ok": True,
        "data": {
            "total_sources": total_sources,
            "total_triplets": total_triplets,
            "year_range": [year_row[0], year_row[1]],
            "geo_count": geo_count,
            "platforms": [{"platform": r[0], "count": r[1]} for r in platform_rows],
        },
        "error": None,
    })


@app.post("/api/ingest/today")
async def api_ingest_today() -> JSONResponse:
    """Trigger a manual fetch of today's GDELT + YouTube data."""
    if _is_vercel_demo_mode():
        return JSONResponse(
            {"ok": False, "error": {"code": "demo_mode", "message": "Ingest disabled in demo mode"}},
            status_code=403,
        )
    from datetime import date as _date
    from siamquantum.pipeline.ingest import ingest_gdelt_daterange, ingest_youtube_daterange

    db = _db()
    today = _date.today()
    results: dict[str, Any] = {}
    try:
        g_f, g_i = await ingest_gdelt_daterange(today, today, db)
        results["gdelt"] = {"fetched": g_f, "inserted": g_i}
    except Exception as exc:
        results["gdelt"] = {"error": str(exc)}
    try:
        y_f, y_i = await ingest_youtube_daterange(today, today, db)
        results["youtube"] = {"fetched": y_f, "inserted": y_i}
    except Exception as exc:
        results["youtube"] = {"error": str(exc)}

    total_inserted = sum(
        v.get("inserted", 0) for v in results.values() if isinstance(v, dict)
    )
    if total_inserted > 0:
        _invalidate_node_registry()
        if not settings.database_read_only:
            with get_connection(db) as conn:
                StatsCacheRepo(conn).invalidate("graph:node_details")

    return JSONResponse({"ok": True, "date": today.isoformat(), "results": results, "error": None})


@app.get("/api/pipeline/live")
def api_pipeline_live(limit: int = Query(8, ge=3, le=20)) -> JSONResponse:
    """Real recent intake + analysis readiness for the home page."""
    db = _db()
    try:
        with get_connection(db) as conn:
            recent_rows = conn.execute(
                """
                WITH triplet_counts AS (
                    SELECT source_id, COUNT(*) AS triplet_count
                    FROM triplets
                    GROUP BY source_id
                )
                SELECT
                    s.id,
                    s.platform,
                    s.url,
                    s.title,
                    s.published_year,
                    s.fetched_at,
                    CASE WHEN g.source_id IS NULL THEN 0 ELSE 1 END AS has_geo,
                    CASE WHEN e.source_id IS NULL THEN 0 ELSE 1 END AS has_entity,
                    COALESCE(tc.triplet_count, 0) AS triplet_count,
                    na.status AS nlp_status
                FROM sources s
                LEFT JOIN geo g ON g.source_id = s.id
                LEFT JOIN entities e ON e.source_id = s.id
                LEFT JOIN triplet_counts tc ON tc.source_id = s.id
                LEFT JOIN nlp_abstentions na ON na.source_id = s.id
                WHERE s.is_quantum_tech = 1 AND s.is_thailand_related = 1
                ORDER BY datetime(s.fetched_at) DESC, s.id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

            overview_row = conn.execute(
                """
                WITH triplet_counts AS (
                    SELECT source_id, COUNT(*) AS triplet_count
                    FROM triplets
                    GROUP BY source_id
                )
                SELECT
                    COUNT(*) AS total_sources,
                    SUM(CASE WHEN g.source_id IS NOT NULL THEN 1 ELSE 0 END) AS geocoded_sources,
                    SUM(CASE WHEN COALESCE(tc.triplet_count, 0) > 0 THEN 1 ELSE 0 END) AS triplet_ready_sources,
                    SUM(
                        CASE
                            WHEN e.source_id IS NULL
                              AND COALESCE(tc.triplet_count, 0) = 0
                              AND na.source_id IS NULL
                            THEN 1 ELSE 0
                        END
                    ) AS pulling_sources,
                    SUM(
                        CASE
                            WHEN e.source_id IS NOT NULL
                              AND COALESCE(tc.triplet_count, 0) = 0
                              AND na.source_id IS NULL
                            THEN 1 ELSE 0
                        END
                    ) AS analyzing_sources,
                    SUM(
                        CASE
                            WHEN e.source_id IS NOT NULL
                              OR COALESCE(tc.triplet_count, 0) > 0
                              OR na.source_id IS NOT NULL
                            THEN 1 ELSE 0
                        END
                    ) AS analyzed_sources,
                    SUM(
                        CASE
                            WHEN date(s.fetched_at) = date('now', 'localtime')
                              AND (
                                e.source_id IS NOT NULL
                                OR COALESCE(tc.triplet_count, 0) > 0
                                OR na.source_id IS NOT NULL
                              )
                            THEN 1 ELSE 0
                        END
                    ) AS done_today,
                    MAX(datetime(s.fetched_at)) AS latest_fetch_at
                FROM sources s
                LEFT JOIN geo g ON g.source_id = s.id
                LEFT JOIN entities e ON e.source_id = s.id
                LEFT JOIN triplet_counts tc ON tc.source_id = s.id
                LEFT JOIN nlp_abstentions na ON na.source_id = s.id
                WHERE s.is_quantum_tech = 1 AND s.is_thailand_related = 1
                """
            ).fetchone()

            submission_rows = conn.execute(
                """
                SELECT status, COUNT(*) AS n
                FROM community_submissions
                GROUP BY status
                """
            ).fetchall()

            queue_recent = CommunitySubmissionRepo(conn).list_recent(5)

            stats_cache_row = conn.execute(
                "SELECT MAX(datetime(computed_at)) AS computed_at FROM stats_cache"
            ).fetchone()
            denstream_row = conn.execute(
                "SELECT MAX(datetime(updated_at)) AS updated_at FROM denstream_state"
            ).fetchone()
    except Exception as exc:
        return JSONResponse(
            {"ok": False, "data": None, "error": {"code": "pipeline_live_failed", "message": str(exc)}},
            status_code=500,
        )

    submission_counts = {str(row["status"]): int(row["n"]) for row in submission_rows}

    def _stage_for(row: sqlite3.Row) -> tuple[str, str]:
        if int(row["triplet_count"] or 0) > 0 or row["nlp_status"]:
            return ("analyzed", "Analyzed")
        if int(row["has_entity"] or 0) > 0:
            return ("classified", "Classified")
        if int(row["has_geo"] or 0) > 0:
            return ("geocoded", "Geocoded")
        return ("fetched", "Fetched")

    recent_items = []
    for row in recent_rows:
        stage_key, stage_label = _stage_for(row)
        recent_items.append(
            {
                "id": int(row["id"]),
                "platform": row["platform"],
                "url": row["url"],
                "title": row["title"] or row["url"],
                "published_year": row["published_year"],
                "fetched_at": row["fetched_at"],
                "has_geo": bool(row["has_geo"]),
                "has_entity": bool(row["has_entity"]),
                "triplet_count": int(row["triplet_count"] or 0),
                "nlp_status": row["nlp_status"],
                "stage_key": stage_key,
                "stage_label": stage_label,
            }
        )

    analysis_timestamps = [
        value
        for value in [
            stats_cache_row["computed_at"] if stats_cache_row else None,
            denstream_row["updated_at"] if denstream_row else None,
        ]
        if value
    ]
    latest_analysis_at = max(analysis_timestamps) if analysis_timestamps else None

    overview = {
        "total_sources": int(overview_row["total_sources"] or 0),
        "geocoded_sources": int(overview_row["geocoded_sources"] or 0),
        "triplet_ready_sources": int(overview_row["triplet_ready_sources"] or 0),
        "pulling_sources": int(overview_row["pulling_sources"] or 0),
        "analyzing_sources": int(overview_row["analyzing_sources"] or 0),
        "analyzed_sources": int(overview_row["analyzed_sources"] or 0),
        "done_today": int(overview_row["done_today"] or 0),
        "pending_sources": max(
            int(overview_row["total_sources"] or 0) - int(overview_row["analyzed_sources"] or 0),
            0,
        ),
        "latest_fetch_at": overview_row["latest_fetch_at"] if overview_row else None,
        "latest_analysis_at": latest_analysis_at,
    }

    return JSONResponse(
        {
            "ok": True,
            "data": {
                "overview": overview,
                "recent_sources": recent_items,
                "submissions": {
                    "counts": submission_counts,
                    "recent": [
                        {
                            "id": item.id,
                            "url": item.url,
                            "handle": item.handle,
                            "status": item.status,
                            "submitted_at": item.submitted_at,
                        }
                        for item in queue_recent
                    ],
                },
            },
            "error": None,
        }
    )
