from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
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

    return JSONResponse([dict(r) for r in rows])


# ---------------------------------------------------------------------------
# API — graph (nodes + edges for 3D force graph)
# ---------------------------------------------------------------------------

@app.get("/api/graph")
def api_graph() -> JSONResponse:
    """
    Returns nodes (entities joined with sources) and edges (triplets).
    """
    db = _db()
    with get_connection(db) as conn:
        node_rows = conn.execute("""
            SELECT e.source_id AS id,
                   e.content_type, e.production_type, e.area, e.engagement_level,
                   s.url, s.title, s.published_year AS year, s.platform
            FROM entities e
            JOIN sources s ON e.source_id = s.id
        """).fetchall()

        edge_rows = conn.execute("""
            SELECT t.id, t.source_id, t.subject, t.relation, t.object, t.confidence
            FROM triplets t
        """).fetchall()

    return JSONResponse({
        "nodes": [dict(r) for r in node_rows],
        "edges": [dict(r) for r in edge_rows],
    })


# ---------------------------------------------------------------------------
# API — stats
# ---------------------------------------------------------------------------

@app.get("/api/stats/yearly")
def api_stats_yearly() -> JSONResponse:
    """
    Yearly source counts, engagement distribution, and cached t-test results.
    """
    db = _db()
    with get_connection(db) as conn:
        # Yearly counts per platform
        count_rows = conn.execute("""
            SELECT published_year, platform, COUNT(*) AS n
            FROM sources
            GROUP BY published_year, platform
            ORDER BY published_year, platform
        """).fetchall()

        # Engagement distribution per year
        eng_rows = conn.execute("""
            SELECT s.published_year, e.engagement_level, COUNT(*) AS n
            FROM entities e
            JOIN sources s ON e.source_id = s.id
            GROUP BY s.published_year, e.engagement_level
            ORDER BY s.published_year, e.engagement_level
        """).fetchall()

        # Macro-clusters from cache
        cache = StatsCacheRepo(conn)
        clusters_raw = cache.get("macro_clusters")
        clusters = clusters_raw if isinstance(clusters_raw, list) else []

        # t-test results from cache (all keys starting with "ttest:")
        ttest_rows = conn.execute("""
            SELECT key, value FROM stats_cache WHERE key LIKE 'ttest:%'
        """).fetchall()

    # Build counts dict: {year: {platform: n, total: n}}
    counts: dict[str, dict[str, int]] = {}
    for row in count_rows:
        yr = str(row["published_year"])
        if yr not in counts:
            counts[yr] = {"total": 0}
        counts[yr][row["platform"]] = row["n"]
        counts[yr]["total"] += row["n"]

    # Build engagement dist: {year: {level: n}}
    eng_dist: dict[str, dict[str, int]] = {}
    for row in eng_rows:
        yr = str(row["published_year"])
        if yr not in eng_dist:
            eng_dist[yr] = {}
        eng_dist[yr][row["engagement_level"]] = row["n"]

    # Parse t-test results
    ttest_results = []
    for row in ttest_rows:
        try:
            ttest_results.append(json.loads(row["value"]))
        except Exception:
            pass

    return JSONResponse({
        "years": sorted(counts.keys()),
        "counts": counts,
        "engagement_distribution": eng_dist,
        "macro_clusters": clusters,
        "ttest_results": ttest_results,
    })


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
            ORDER BY s.published_year DESC, s.id
            LIMIT ? OFFSET ?
        """, [*params, page_size, offset]).fetchall()

    return JSONResponse({
        "total": int(total),
        "page": page,
        "page_size": page_size,
        "items": [dict(r) for r in rows],
    })


# ---------------------------------------------------------------------------
# API — XLSX export
# ---------------------------------------------------------------------------

@app.get("/api/export/xlsx")
def api_export_xlsx(
    year: int | None = Query(None),
    platform: str | None = Query(None),
) -> StreamingResponse:
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
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

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
            ORDER BY s.published_year DESC, s.id
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

    filename = f"siamquantum_atlas{'_' + str(year) if year else ''}.xlsx"
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ---------------------------------------------------------------------------
# API — community submission
# ---------------------------------------------------------------------------

@app.post("/api/community/submit", status_code=201)
def api_community_submit(payload: dict[str, Any]) -> JSONResponse:
    """Accept a community URL submission."""
    url = (payload.get("url") or "").strip()
    if not url:
        raise HTTPException(status_code=422, detail="url is required")
    handle = (payload.get("handle") or "").strip() or None

    db = _db()
    with get_connection(db) as conn:
        sub_id = CommunitySubmissionRepo(conn).insert(
            CommunitySubmissionCreate(handle=handle, url=url)
        )
    return JSONResponse({"id": sub_id, "status": "pending"}, status_code=201)
