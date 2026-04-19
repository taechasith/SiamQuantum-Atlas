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
def api_geo_list(cdn: bool = Query(False, description="Include CDN-resolved rows")) -> JSONResponse:
    """
    Returns geo rows joined with source metadata.
    Default (cdn=false): only origin IPs (is_cdn_resolved=0 or NULL).
    """
    db = _db()
    try:
        with get_connection(db) as conn:
            if cdn:
                rows = conn.execute("""
                    SELECT g.source_id, g.lat, g.lng, g.city, g.region,
                           g.isp, g.asn_org, g.is_cdn_resolved,
                           s.platform, s.url, s.title, s.published_year
                    FROM geo g
                    JOIN sources s ON g.source_id = s.id
                    WHERE g.lat IS NOT NULL AND g.lng IS NOT NULL
                    ORDER BY g.source_id
                """).fetchall()
            else:
                rows = conn.execute("""
                    SELECT g.source_id, g.lat, g.lng, g.city, g.region,
                           g.isp, g.asn_org, g.is_cdn_resolved,
                           s.platform, s.url, s.title, s.published_year
                    FROM geo g
                    JOIN sources s ON g.source_id = s.id
                    WHERE g.lat IS NOT NULL AND g.lng IS NOT NULL
                      AND (g.is_cdn_resolved = 0 OR g.is_cdn_resolved IS NULL)
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
def api_graph() -> JSONResponse:
    """
    Returns nodes (entities joined with sources) and edges (triplets).
    """
    db = _db()
    try:
        with get_connection(db) as conn:
            node_rows = conn.execute("""
                SELECT e.source_id,
                       e.content_type, e.production_type, e.area, e.engagement_level,
                       s.url, s.title, s.published_year AS year, s.platform
                FROM entities e
                JOIN sources s ON e.source_id = s.id
                ORDER BY e.source_id
            """).fetchall()

            edge_rows = conn.execute("""
                SELECT t.id, t.source_id, t.subject, t.relation, t.object, t.confidence
                FROM triplets t
                ORDER BY t.id
            """).fetchall()
    except Exception as exc:
        return JSONResponse(
            {
                "ok": False,
                "data": {"nodes": [], "links": []},
                "error": {
                    "code": "graph_load_failed",
                    "message": str(exc),
                },
            },
            status_code=500,
        )

    nodes = []
    for row in node_rows:
        item = dict(row)
        source_id = item["source_id"]
        nodes.append(
            {
                "id": f"source:{source_id}",
                "source_id": source_id,
                "label": item.get("title") or item.get("url") or f"Source {source_id}",
                "title": item.get("title"),
                "url": item.get("url"),
                "year": item.get("year"),
                "platform": item.get("platform"),
                "content_type": item.get("content_type"),
                "production_type": item.get("production_type"),
                "area": item.get("area"),
                "engagement_level": item.get("engagement_level"),
            }
        )

    links = []
    for row in edge_rows:
        item = dict(row)
        source_id = item["source_id"]
        links.append(
            {
                "id": f"triplet:{item['id']}",
                "source": f"source:{source_id}",
                "target": f"source:{source_id}",
                "source_id": source_id,
                "subject": item.get("subject"),
                "relation": item.get("relation"),
                "object": item.get("object"),
                "confidence": item.get("confidence"),
                "label": " ".join(
                    str(part) for part in [item.get("subject"), item.get("relation"), item.get("object")] if part
                ),
            }
        )

    return JSONResponse(
        {
            "ok": True,
            "data": {
                "nodes": nodes,
                "links": links,
            },
            "error": None,
        }
    )


# ---------------------------------------------------------------------------
# API — stats
# ---------------------------------------------------------------------------

@app.get("/api/stats/yearly")
def api_stats_yearly() -> JSONResponse:
    """
    Yearly source counts, engagement distribution, and cached t-test results.
    """
    db = _db()
    try:
        with get_connection(db) as conn:
            count_rows = conn.execute("""
                SELECT published_year, platform, COUNT(*) AS n
                FROM sources
                GROUP BY published_year, platform
                ORDER BY published_year, platform
            """).fetchall()

            eng_rows = conn.execute("""
                SELECT s.published_year, e.engagement_level, COUNT(*) AS n
                FROM entities e
                JOIN sources s ON e.source_id = s.id
                GROUP BY s.published_year, e.engagement_level
                ORDER BY s.published_year, e.engagement_level
            """).fetchall()

            cache = StatsCacheRepo(conn)
            clusters_raw = cache.get("macro_clusters")
            clusters = clusters_raw if isinstance(clusters_raw, list) else []

            ttest_rows = conn.execute("""
                SELECT key, value FROM stats_cache WHERE key LIKE 'ttest:%'
            """).fetchall()
    except Exception as exc:
        return JSONResponse(
            {
                "ok": False,
                "data": {
                    "years": [],
                    "counts": {},
                    "engagement_distribution": {},
                    "trendlines": {
                        "total_sources": [],
                        "high_engagement": [],
                    },
                    "significance": [],
                    "macro_clusters": [],
                },
                "error": {
                    "code": "yearly_stats_failed",
                    "message": str(exc),
                },
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

    significance = []
    for row in ttest_rows:
        try:
            parsed = json.loads(row["value"])
        except Exception:
            continue
        significance.append(parsed)

    significance.sort(key=lambda item: (item.get("year_a", 0), item.get("year_b", 0)))

    year_numbers = sorted(
        {int(year) for year in counts.keys()} | {int(year) for year in eng_dist.keys() if str(year).isdigit()}
    )
    years = [str(year) for year in year_numbers]

    trend_total_sources = [int((counts.get(year) or {}).get("total", 0)) for year in years]
    trend_high_engagement = [int((eng_dist.get(year) or {}).get("high", 0)) for year in years]

    return JSONResponse(
        {
            "ok": True,
            "data": {
                "years": years,
                "counts": counts,
                "engagement_distribution": eng_dist,
                "trendlines": {
                    "total_sources": trend_total_sources,
                    "high_engagement": trend_high_engagement,
                },
                "significance": significance,
                "macro_clusters": clusters,
            },
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
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
) -> JSONResponse:
    """Paginated source list with optional filters."""
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
                       e.content_type, e.production_type, e.area, e.engagement_level
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
