from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any

from fastapi import BackgroundTasks, FastAPI, HTTPException, Query, Request
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

app = FastAPI(title="SiamQuantum Atlas", version="0.1.0")

_TEMPLATES_DIR = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))


def _db() -> Path:
    return db_path_from_url(settings.database_url)


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
def root() -> RedirectResponse:
    return RedirectResponse(url="/dashboard")


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
    return templates.TemplateResponse(request, "community.html")


# ---------------------------------------------------------------------------
# API — geo
# ---------------------------------------------------------------------------

@app.get("/api/geo/list")
def api_geo_list(
    cdn: bool = Query(False, description="Include CDN-resolved rows"),
    include_filtered: bool = Query(False, description="Include non-relevant sources"),
) -> JSONResponse:
    """
    Returns geo rows joined with source metadata.
    Default (cdn=false): only origin IPs (is_cdn_resolved=0 or NULL).
    Default: only quantum+thai relevant sources (is_quantum_tech=1 AND is_thailand_related=1).
    """
    db = _db()
    relevance_clause = "" if include_filtered else "AND (s.is_quantum_tech = 1 AND s.is_thailand_related = 1)"
    try:
        with get_connection(db) as conn:
            if cdn:
                rows = conn.execute(f"""
                    SELECT g.source_id, g.lat, g.lng, g.city, g.region,
                           g.isp, g.asn_org, g.is_cdn_resolved,
                           s.platform, s.url, s.title, s.published_year
                    FROM geo g
                    JOIN sources s ON g.source_id = s.id
                    WHERE g.lat IS NOT NULL AND g.lng IS NOT NULL
                    {relevance_clause}
                    ORDER BY g.source_id
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
                    ORDER BY g.source_id
                """).fetchall()
    except Exception as exc:
        return JSONResponse(
            {
                "ok": False,
                "data": [],
                "count": 0,
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
            "error": None,
        }
    )


# ---------------------------------------------------------------------------
# API — graph (nodes + edges for 3D force graph)
# ---------------------------------------------------------------------------

@app.get("/api/graph")
def api_graph(
    include_filtered: bool = Query(True, description="Include all sources (relevance flags not populated)"),
) -> JSONResponse:
    """
    Returns concept-level nodes and edges built from triplets.
    Nodes = unique subject/object concept texts. Edges = subject→object per triplet.
    """
    import re as _re

    def _norm(text: str) -> str:
        return _re.sub(r"\s+", " ", text.strip().lower())

    db = _db()
    relevance_clause = "" if include_filtered else "AND s.is_quantum_tech = 1 AND s.is_thailand_related = 1"
    try:
        with get_connection(db) as conn:
            edge_rows = conn.execute(f"""
                SELECT t.subject, t.relation, t.object, t.confidence
                FROM triplets t
                JOIN sources s ON t.source_id = s.id
                WHERE 1=1 {relevance_clause}
                ORDER BY t.id
            """).fetchall()
    except Exception as exc:
        return JSONResponse(
            {
                "ok": False,
                "data": {"nodes": [], "links": []},
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

        subj_key = _norm(subj_raw)
        obj_key = _norm(obj_raw)

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

    return JSONResponse(
        {
            "ok": True,
            "data": {"nodes": nodes, "links": links},
            "error": None,
        }
    )


# ---------------------------------------------------------------------------
# API — graph metrics
# ---------------------------------------------------------------------------

@app.get("/api/graph/metrics")
def api_graph_metrics() -> JSONResponse:
    """Degree centrality, betweenness centrality, connected components."""
    from siamquantum.pipeline.graph_metrics import compute_metrics
    db = _db()
    try:
        metrics = compute_metrics(db)
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
    ]
    try:
        with get_connection(db) as conn:
            from siamquantum.db.repos import StatsCacheRepo
            cache = StatsCacheRepo(conn)
            data = {k.replace("taxonomy:", ""): cache.get(k) for k in keys}
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
    include_filtered: bool = Query(False, description="Include non-relevant sources"),
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

            cache = StatsCacheRepo(conn)
            clusters_raw = cache.get("macro_clusters")
            clusters = clusters_raw if isinstance(clusters_raw, list) else []

            bootstrap_yearly_rows = conn.execute(
                "SELECT key, value FROM stats_cache WHERE key LIKE 'bootstrap_yearly:%'"
            ).fetchall()
            bootstrap_pairwise_rows = conn.execute(
                "SELECT key, value FROM stats_cache WHERE key LIKE 'bootstrap_pairwise:%'"
            ).fetchall()
            trend_raw = cache.get("bootstrap_trend")
    except Exception as exc:
        return JSONResponse(
            {"ok": False, "data": _empty_payload, "error": {"code": "yearly_stats_failed", "message": str(exc)}},
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

    return JSONResponse({
        "ok": True,
        "data": {
            "scope": "thai_web_engagement",
            "scope_caveat": (
                "Excludes academic publications in English journals and institutional reports "
                "not indexed by GDELT/YouTube. Coverage: 0.4% academic/gov sources (3 of 768)."
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
        "error": None,
    })


# ---------------------------------------------------------------------------
# API — database (paginated source list)
# ---------------------------------------------------------------------------

@app.get("/api/sources")
def api_sources(
    year: int | None = Query(None),
    platform: str | None = Query(None),
    content_type: str | None = Query(None),
    include_filtered: bool = Query(False, description="Include non-relevant sources"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
) -> JSONResponse:
    """Paginated source list with optional filters. Default: quantum+thai relevant only."""
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

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    offset = (page - 1) * page_size

    try:
        with get_connection(db) as conn:
            total = conn.execute(f"""
                SELECT COUNT(*) FROM sources s
                LEFT JOIN entities e ON s.id = e.source_id
                {where}
            """, params).fetchone()[0]

            rows = conn.execute(f"""
                SELECT s.id, s.platform, s.url, s.title, s.published_year,
                       s.view_count, s.like_count, s.comment_count,
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
            "error": None,
        }
    )


# ---------------------------------------------------------------------------
# API — XLSX export
# ---------------------------------------------------------------------------

@app.get("/api/export/xlsx")
def api_export_xlsx(
    year: int | None = Query(None),
    platform: str | None = Query(None),
    content_type: str | None = Query(None),
):
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
# API — community submission
# ---------------------------------------------------------------------------

@app.post("/api/community/submit", status_code=201)
def api_community_submit(
    payload: dict[str, Any],
    background_tasks: BackgroundTasks,
) -> JSONResponse:
    """Accept a community URL submission."""
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
