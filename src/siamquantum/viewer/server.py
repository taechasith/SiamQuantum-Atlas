from __future__ import annotations

import asyncio
import hashlib
import hmac
import io
import json
import logging
import secrets
import sqlite3
import time
from collections import Counter
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from fastapi import BackgroundTasks, FastAPI, HTTPException, Query, Request
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
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
from siamquantum.pipeline.filter import backfill_relevance, recheck_relevance
from siamquantum.services.supabase import (
    SupabaseError,
    SupabaseUser,
    current_profile,
    ensure_profile_for_user,
    is_admin_profile,
    rest_insert,
    rest_select,
    rest_update,
    slugify,
    supabase_enabled,
    verify_user_access_token,
)
from siamquantum.stats.yearly_taxonomy_analytics import build_yearly_taxonomy_analytics

logger = logging.getLogger(__name__)
_LOCAL_SESSION_COOKIE = "sq_local_session"
_ingest_lock = asyncio.Lock()

_PAGE_FETCH_HEADERS = {
    "User-Agent": "SiamQuantumAtlas/1.0 (+https://siamquantum.org; research metadata fetch)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "th,en;q=0.9",
}


@dataclass
class LocalAuthUser:
    id: str
    email: str | None
    created_at: str | None
    raw: dict[str, Any]

    @property
    def user_metadata(self) -> dict[str, Any]:
        return {}

    @property
    def display_name(self) -> str | None:
        return str(self.raw.get("display_name") or "").strip() or None

    @property
    def avatar_url(self) -> str | None:
        return str(self.raw.get("avatar_url") or "").strip() or None

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


def _stamp_last_ingest(db: Path) -> None:
    """Write current UTC time to pipeline_meta so the UI shows when we last checked."""
    with get_connection(db) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline_meta (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            INSERT INTO pipeline_meta (key, value, updated_at)
            VALUES ('last_ingest_at', datetime('now'), datetime('now'))
            ON CONFLICT(key) DO UPDATE SET
                value      = excluded.value,
                updated_at = excluded.updated_at
            """
        )
        conn.commit()


async def _run_ingest_now() -> dict[str, int]:
    """Fetch last 3 days from GDELT + YouTube, write to DB, refresh caches.
    Returns counts. No-op if another ingest is already running."""
    from datetime import date, timedelta
    from siamquantum.pipeline.ingest import ingest_gdelt_daterange, ingest_youtube_daterange

    if _ingest_lock.locked():
        return {"skipped": 1}
    async with _ingest_lock:
        db = db_path_from_url(settings.database_url)
        today = date.today()
        start = today - timedelta(days=2)
        g_fetched, g_inserted = await ingest_gdelt_daterange(start, today, db)
        logger.info("Ingest GDELT: fetched=%d inserted=%d", g_fetched, g_inserted)
        y_fetched, y_inserted = await ingest_youtube_daterange(start, today, db)
        logger.info("Ingest YouTube: fetched=%d inserted=%d", y_fetched, y_inserted)
        relevance_checked = 0
        if g_inserted + y_inserted > 0:
            new_counts = backfill_relevance(db)
            relevance_checked += int(new_counts.get("checked", 0))
            logger.info("Ingest relevance backfill: %s", new_counts)
        audit_counts = recheck_relevance(
            db,
            stale_after_days=settings.relevance_recheck_days,
            limit=settings.relevance_audit_batch_size,
        )
        relevance_checked += int(audit_counts.get("checked", 0))
        logger.info("Ingest relevance audit: %s", audit_counts)
        if g_inserted + y_inserted > 0 or relevance_checked > 0:
            _invalidate_node_registry()
            if not settings.database_read_only:
                with get_connection(db) as conn:
                    StatsCacheRepo(conn).invalidate("graph:node_details")
        _stamp_last_ingest(db)
        return {
            "gdelt_fetched": g_fetched,
            "gdelt_inserted": g_inserted,
            "youtube_fetched": y_fetched,
            "youtube_inserted": y_inserted,
            "relevance_checked": relevance_checked,
        }


async def _daily_ingest_task() -> None:
    """Run GDELT + YouTube ingest once per day at ~00:05.
    Fetches last 3 days to compensate for GDELT's ~24-48h indexing lag."""
    from datetime import datetime, timedelta

    while True:
        now = datetime.now()
        next_run = (now + timedelta(days=1)).replace(hour=0, minute=5, second=0, microsecond=0)
        await asyncio.sleep(max(60.0, (next_run - now).total_seconds()))
        try:
            await _run_ingest_now()
        except Exception:
            logger.exception("Daily ingest failed")


@asynccontextmanager
async def lifespan(app_instance: FastAPI):  # type: ignore[type-arg]
    if settings.deployment_mode != "vercel":
        _ensure_local_auth_tables()
        asyncio.create_task(asyncio.to_thread(_prewarm_registry_sync))
        asyncio.create_task(_run_ingest_now())  # fetch fresh data on startup
        asyncio.create_task(_daily_ingest_task())
    yield


app = FastAPI(title="SiamQuantum Atlas", version="0.1.0", lifespan=lifespan)
app.add_middleware(GZipMiddleware, minimum_size=1000)

_STATIC_DIR = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")

_TEMPLATES_DIR = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))
templates.env.globals["supabase_url"] = settings.supabase_url
templates.env.globals["supabase_publishable_key"] = settings.supabase_publishable_key
templates.env.globals["supabase_enabled"] = supabase_enabled()


def _db() -> Path:
    return db_path_from_url(settings.database_url)


def _prefer_local_auth() -> bool:
    return not supabase_enabled()


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _password_hash(password: str, *, salt: str) -> str:
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), 120_000)
    return digest.hex()


def _ensure_local_auth_tables() -> None:
    db = _db()
    with get_connection(db) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS local_users (
              id TEXT PRIMARY KEY,
              email TEXT NOT NULL UNIQUE,
              password_salt TEXT NOT NULL,
              password_hash TEXT NOT NULL,
              display_name TEXT,
              avatar_url TEXT,
              bio TEXT,
              website_url TEXT,
              role TEXT NOT NULL DEFAULT 'user',
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS local_sessions (
              token TEXT PRIMARY KEY,
              user_id TEXT NOT NULL REFERENCES local_users(id) ON DELETE CASCADE,
              created_at TEXT NOT NULL,
              expires_at TEXT
            );
            CREATE TABLE IF NOT EXISTS local_categories (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              name TEXT NOT NULL UNIQUE,
              slug TEXT NOT NULL UNIQUE,
              description TEXT,
              created_by TEXT REFERENCES local_users(id) ON DELETE SET NULL,
              created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS local_submitted_data (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              user_id TEXT NOT NULL REFERENCES local_users(id) ON DELETE CASCADE,
              title TEXT NOT NULL,
              description TEXT,
              source_url TEXT,
              category TEXT NOT NULL,
              page_target TEXT,
              status TEXT NOT NULL DEFAULT 'pending',
              analysis_status TEXT NOT NULL DEFAULT 'queued',
              analysis_result TEXT,
              metadata TEXT NOT NULL DEFAULT '{}',
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_local_sessions_user_id ON local_sessions(user_id);
            CREATE INDEX IF NOT EXISTS idx_local_submitted_user_id ON local_submitted_data(user_id);
            CREATE INDEX IF NOT EXISTS idx_local_submitted_public ON local_submitted_data(status, analysis_status, created_at);
            """
        )
        conn.commit()


def _local_row_to_user(row: sqlite3.Row) -> LocalAuthUser:
    raw = dict(row)
    return LocalAuthUser(
        id=str(raw["id"]),
        email=str(raw.get("email") or "").strip() or None,
        created_at=str(raw.get("created_at") or "").strip() or None,
        raw=raw,
    )


def _local_profile_payload(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    raw = dict(row)
    return {
        "id": raw.get("id"),
        "email": raw.get("email"),
        "display_name": raw.get("display_name"),
        "avatar_url": raw.get("avatar_url"),
        "bio": raw.get("bio"),
        "website_url": raw.get("website_url"),
        "role": raw.get("role") or "user",
        "created_at": raw.get("created_at"),
        "updated_at": raw.get("updated_at"),
    }


def _local_submission_payload(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    raw = dict(row)
    analysis_result = raw.get("analysis_result")
    metadata = raw.get("metadata")
    try:
        analysis_result_value = json.loads(analysis_result) if analysis_result else None
    except Exception:
        analysis_result_value = None
    try:
        metadata_value = json.loads(metadata) if metadata else {}
    except Exception:
        metadata_value = {}
    return {
        "id": raw.get("id"),
        "user_id": raw.get("user_id"),
        "title": raw.get("title"),
        "description": raw.get("description"),
        "source_url": raw.get("source_url"),
        "category": raw.get("category"),
        "page_target": raw.get("page_target"),
        "status": raw.get("status"),
        "analysis_status": raw.get("analysis_status"),
        "analysis_result": analysis_result_value,
        "metadata": metadata_value,
        "submitted_by": str(metadata_value.get("submitted_by") or "").strip() or None,
        "created_at": raw.get("created_at"),
        "updated_at": raw.get("updated_at"),
    }


def _local_current_user(request: Request) -> LocalAuthUser | None:
    token = request.cookies.get(_LOCAL_SESSION_COOKIE)
    if not token:
        return None
    db = _db()
    with get_connection(db) as conn:
        row = conn.execute(
            """
            SELECT u.*
            FROM local_sessions s
            JOIN local_users u ON u.id = s.user_id
            WHERE s.token = ?
              AND (s.expires_at IS NULL OR datetime(s.expires_at) > datetime('now'))
            """,
            (token,),
        ).fetchone()
    return _local_row_to_user(row) if row else None


def _require_local_user(request: Request) -> LocalAuthUser | JSONResponse:
    user = _local_current_user(request)
    if not user:
        return JSONResponse(
            {"ok": False, "data": None, "error": {"code": "auth_required", "message": "Login is required for this action."}},
            status_code=401,
        )
    return user


def _require_local_admin(request: Request) -> tuple[LocalAuthUser, dict[str, Any]] | JSONResponse:
    user = _require_local_user(request)
    if isinstance(user, JSONResponse):
        return user
    profile = _local_profile_payload(user.raw)
    if str(profile.get("role") or "").strip().lower() != "admin":
        return JSONResponse(
            {"ok": False, "data": None, "error": {"code": "admin_required", "message": "Admin access is required."}},
            status_code=403,
        )
    return user, profile


def _supabase_not_configured_response() -> JSONResponse:
    return JSONResponse(
        {
            "ok": False,
            "data": None,
            "error": {
                "code": "supabase_not_configured",
                "message": (
                    "Supabase is not configured. Set SUPABASE_URL, "
                    "SUPABASE_PUBLISHABLE_KEY, and SUPABASE_SECRET_KEY in the server environment."
                ),
            },
        },
        status_code=503,
    )


def _bearer_token(request: Request) -> str | None:
    auth_header = request.headers.get("Authorization", "")
    if auth_header.lower().startswith("bearer "):
        token = auth_header[7:].strip()
        return token or None
    return None


def _supabase_error_response(exc: Exception, *, code: str = "supabase_request_failed", status_code: int = 500) -> JSONResponse:
    return JSONResponse(
        {
            "ok": False,
            "data": None,
            "error": {"code": code, "message": str(exc)},
        },
        status_code=status_code,
    )


def _require_auth_user(request: Request) -> tuple[str, SupabaseUser] | JSONResponse:
    if not supabase_enabled():
        return _supabase_not_configured_response()
    access_token = _bearer_token(request)
    if not access_token:
        return JSONResponse(
            {
                "ok": False,
                "data": None,
                "error": {"code": "auth_required", "message": "Login is required for this action."},
            },
            status_code=401,
        )
    try:
        user = verify_user_access_token(access_token)
    except SupabaseError as exc:
        return _supabase_error_response(exc, code="auth_invalid", status_code=401)
    return access_token, user


def _require_admin_user(request: Request) -> tuple[str, SupabaseUser, dict[str, Any]] | JSONResponse:
    auth_result = _require_auth_user(request)
    if isinstance(auth_result, JSONResponse):
        return auth_result
    access_token, user = auth_result
    try:
        profile = current_profile(access_token, user.id)
    except SupabaseError as exc:
        return _supabase_error_response(exc, code="admin_profile_failed", status_code=500)
    if not is_admin_profile(profile):
        return JSONResponse(
            {
                "ok": False,
                "data": None,
                "error": {"code": "admin_required", "message": "Admin access is required."},
            },
            status_code=403,
        )
    return access_token, user, profile or {}


def _submitted_data_payload(row: dict[str, Any]) -> dict[str, Any]:
    created_at = row.get("created_at")
    updated_at = row.get("updated_at")
    metadata = row.get("metadata") or {}
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except Exception:
            metadata = {}
    return {
        "id": row.get("id"),
        "user_id": row.get("user_id"),
        "title": row.get("title"),
        "description": row.get("description"),
        "source_url": row.get("source_url"),
        "category": row.get("category"),
        "page_target": row.get("page_target"),
        "status": row.get("status"),
        "analysis_status": row.get("analysis_status"),
        "analysis_result": row.get("analysis_result"),
        "metadata": metadata,
        "submitted_by": str(metadata.get("submitted_by") or "").strip() or None,
        "created_at": created_at,
        "updated_at": updated_at,
    }


def _submission_categories_from_payload(payload: dict[str, Any]) -> tuple[str, list[str]]:
    raw_categories = payload.get("categories")
    categories: list[str] = []
    if isinstance(raw_categories, list):
        categories = [str(item).strip() for item in raw_categories if str(item or "").strip()]
    elif isinstance(raw_categories, str):
        categories = [raw_categories.strip()] if raw_categories.strip() else []

    raw_meta = payload.get("metadata")
    metadata = raw_meta if isinstance(raw_meta, dict) else {}
    raw_tags = metadata.get("tags")
    if isinstance(raw_tags, list):
        categories.extend(str(item).strip() for item in raw_tags if str(item or "").strip())

    legacy_category = str(payload.get("category") or "").strip()
    if legacy_category:
        categories.insert(0, legacy_category)

    deduped = list(dict.fromkeys(categories))
    return (deduped[0] if deduped else "", deduped)


def _youtube_video_id(url: str) -> str | None:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if host.endswith("youtu.be"):
        return parsed.path.strip("/") or None
    if "youtube.com" in host:
        query = parse_qs(parsed.query)
        if query.get("v"):
            return str(query["v"][0] or "").strip() or None
        if parsed.path.startswith("/shorts/") or parsed.path.startswith("/embed/"):
            parts = [part for part in parsed.path.split("/") if part]
            return parts[1] if len(parts) > 1 else None
    return None


def _clean_page_text(html: str, *, limit: int = 8000) -> tuple[str | None, str | None, str | None]:
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript", "svg", "nav", "footer", "header", "form"]):
        tag.decompose()

    title = None
    if soup.title and soup.title.string:
        title = " ".join(soup.title.string.split()).strip() or None
    meta_description = None
    for selector in (
        {"name": "description"},
        {"property": "og:description"},
        {"name": "twitter:description"},
    ):
        tag = soup.find("meta", attrs=selector)
        content = str(tag.get("content") or "").strip() if tag else ""
        if content:
            meta_description = " ".join(content.split())
            break

    chunks: list[str] = []
    for selector in ("article", "main", "[role='main']"):
        for node in soup.select(selector):
            text = " ".join(node.get_text(" ", strip=True).split())
            if len(text) > 120:
                chunks.append(text)
    if not chunks:
        chunks.append(" ".join(soup.get_text(" ", strip=True).split()))

    text = " ".join(chunks).strip()
    return title, meta_description, text[:limit] if text else None


def _fetch_url_analysis_context(url: str) -> dict[str, Any]:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("URL must start with http:// or https://.")

    context: dict[str, Any] = {
        "url": url,
        "title": None,
        "description": None,
        "text": None,
        "source_access": {"ok": False, "method": None, "status_code": None, "message": None},
    }

    video_id = _youtube_video_id(url)
    if video_id:
        import httpx

        try:
            with httpx.Client(timeout=8, follow_redirects=True) as client:
                response = client.get(
                    "https://www.youtube.com/oembed",
                    params={"url": url, "format": "json"},
                    headers=_PAGE_FETCH_HEADERS,
                )
                context["source_access"].update({"method": "youtube_oembed", "status_code": response.status_code})
                if response.status_code < 400:
                    data = response.json()
                    title = str(data.get("title") or "").strip() or None
                    author = str(data.get("author_name") or "").strip() or None
                    context["title"] = title
                    context["description"] = f"YouTube video by {author}" if author else "YouTube video"
                    context["text"] = "\n".join(part for part in (title, context["description"], f"Video ID: {video_id}") if part)
                    context["source_access"].update({"ok": True, "message": "Read YouTube oEmbed metadata."})
                    return context
        except Exception as exc:
            context["source_access"].update({"method": "youtube_oembed", "message": str(exc)})

    import httpx

    try:
        with httpx.Client(timeout=12, follow_redirects=True) as client:
            response = client.get(url, headers=_PAGE_FETCH_HEADERS)
            context["source_access"].update({"method": "html", "status_code": response.status_code})
            response.raise_for_status()
            title, description, text = _clean_page_text(response.text)
            context.update({"title": title, "description": description, "text": text})
            context["source_access"].update({"ok": bool(text or title or description), "message": "Read page HTML."})
    except Exception as exc:
        context["source_access"].update({"message": str(exc)})

    return context


def _profile_payload(profile: dict[str, Any] | None, user: SupabaseUser) -> dict[str, Any]:
    profile_map = profile or {}
    return {
        "id": user.id,
        "email": profile_map.get("email") or user.email,
        "display_name": profile_map.get("display_name") or user.display_name,
        "avatar_url": profile_map.get("avatar_url") or user.avatar_url,
        "bio": profile_map.get("bio"),
        "website_url": profile_map.get("website_url"),
        "role": profile_map.get("role") or "user",
        "created_at": profile_map.get("created_at") or user.created_at,
        "updated_at": profile_map.get("updated_at"),
    }


def _enqueue_submitted_data_analysis(submission_id: int, source_url: str | None, owner_label: str | None) -> None:
    if not supabase_enabled():
        return
    try:
        rest_update(
            "submitted_data",
            {"analysis_status": "processing"},
            filters={"id": f"eq.{submission_id}"},
            use_service_role=True,
        )

        analysis_result: dict[str, Any] = {
            "source_url": source_url,
            "processed_at": datetime.now(timezone.utc).isoformat(),
            "pipeline": "submitted_data",
            "owner_label": owner_label,
        }

        if source_url:
            db = _db()
            community_submission_id: int | None = None
            with get_connection(db) as conn:
                source = SourceRepo(conn).get_by_url(source_url)
                if source is not None:
                    analysis_result["matched_source"] = {
                        "id": source.id,
                        "platform": source.platform,
                        "title": source.title,
                        "published_year": source.published_year,
                    }
                sub_repo = CommunitySubmissionRepo(conn)
                community_submission_id = sub_repo.insert(
                    CommunitySubmissionCreate(handle=owner_label, url=source_url)
                )
                sub_repo.update_status(community_submission_id, "queued")
            if community_submission_id is not None:
                analysis_result["community_submission_id"] = community_submission_id
                _process_community_submission(community_submission_id, source_url)

        rest_update(
            "submitted_data",
            {
                "analysis_status": "completed",
                "analysis_result": analysis_result,
            },
            filters={"id": f"eq.{submission_id}"},
            use_service_role=True,
        )
    except Exception as exc:
        logger.exception("Submitted data analysis failed")
        try:
            rest_update(
                "submitted_data",
                {
                    "analysis_status": "failed",
                    "analysis_result": {
                        "error": str(exc),
                        "processed_at": datetime.now(timezone.utc).isoformat(),
                    },
                },
                filters={"id": f"eq.{submission_id}"},
                use_service_role=True,
            )
        except Exception:
            logger.exception("Failed to persist submitted_data analysis error")


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
        _label = labels.get(node_id) or str(node_id)
        _role = _hub_role(_label)
        _top_rels = [r for r, _ in relation_counts_by_node.get(node_id, Counter()).most_common(3)]
        _top_neighbors = [labels.get(n) or str(n) for n in neighbor_ids[:3]]
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
                    "label": labels.get(neighbor_id) or str(neighbor_id),
                    "degree": degrees.get(neighbor_id, 0),
                    "shared_links": neighbor_shared_counts_by_node.get(node_id, Counter()).get(labels.get(neighbor_id) or str(neighbor_id), 0),
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
def root() -> RedirectResponse:
    return RedirectResponse(url="/overview", status_code=307)


@app.get("/overview", include_in_schema=False)
def page_overview(request: Request) -> Any:
    return templates.TemplateResponse(request, "home.html", {"active": "overview"})


# ---------------------------------------------------------------------------
# Page routes
# ---------------------------------------------------------------------------

@app.get("/dashboard", include_in_schema=False)
def page_dashboard(request: Request) -> Any:
    return templates.TemplateResponse(request, "dashboard.html", {"active": "dashboard"})


@app.get("/network", include_in_schema=False)
def page_network(request: Request) -> Any:
    return templates.TemplateResponse(request, "network.html", {"active": "network"})


@app.get("/analytics", include_in_schema=False)
def page_analytics(request: Request) -> Any:
    return templates.TemplateResponse(request, "analytics.html", {"active": "analytics"})


@app.get("/database", include_in_schema=False)
def page_database(request: Request) -> Any:
    return templates.TemplateResponse(request, "database.html", {"active": "database"})


@app.get("/submit-data", include_in_schema=False)
def page_submit_data(request: Request) -> Any:
    return templates.TemplateResponse(
        request,
        "community.html",
        {"demo_mode": _is_vercel_demo_mode(), "active": "community"},
    )


@app.get("/community", include_in_schema=False)
def page_community_redirect() -> RedirectResponse:
    return RedirectResponse(url="/submit-data", status_code=307)


@app.get("/profile", include_in_schema=False)
def page_profile(request: Request) -> Any:
    return templates.TemplateResponse(request, "profile.html", {"active": "profile"})


@app.get("/admin/submitted-data", include_in_schema=False)
def page_admin_submitted_data(request: Request) -> Any:
    return templates.TemplateResponse(request, "admin_submitted_data.html", {"active": "profile"})


# ---------------------------------------------------------------------------
# API — geo
# ---------------------------------------------------------------------------

@app.get("/api/geo/list")
def api_geo_list(
    cdn: bool = Query(False, description="Include CDN-resolved rows"),
    include_filtered: bool = Query(True, description="Deprecated — use scope instead."),
    scope: str = Query("strict", description="strict (default, qt AND th) | quantum | thailand | all"),
) -> JSONResponse:
    """
    Returns geo rows joined with source metadata.
    Default scope=strict: only sources where is_quantum_tech=1 AND is_thailand_related=1.
    Broader scopes available via ?scope=quantum|thailand|all for research use.

    Two tiers of geo data are returned:
      - is_approximate=0: real IP-resolved coordinates (authoritative)
      - is_approximate=1: channel_country centroid fallback for CDN/missing-geo sources
        (declared country of origin, not IP — shown differently on the map)
    """
    # Country centroids used when IP resolves to a CDN or geo is missing entirely.
    # Source: channel_country field (declared by the channel owner on YouTube).
    _CENTROIDS: dict[str, tuple[float, float]] = {
        "TH": (13.7563, 100.5018),
        "US": (37.0902, -95.7129),
        "CN": (35.8617, 104.1954),
        "JP": (36.2048, 138.2529),
        "GB": (55.3781, -3.4360),
        "SG": (1.3521, 103.8198),
        "DE": (51.1657, 10.4515),
        "KR": (35.9078, 127.7669),
        "AU": (-25.2744, 133.7751),
        "IN": (20.5937, 78.9629),
        "FR": (46.2276, 2.2137),
        "CA": (56.1304, -106.3468),
        "NL": (52.1326, 5.2913),
        "CH": (46.8182, 8.2275),
        "SE": (60.1282, 18.6435),
        "IL": (31.0461, 34.8516),
    }

    db = _db()
    scope_clauses = {
        "strict": "AND s.is_quantum_tech = 1 AND s.is_thailand_related = 1 AND (s.relevance_confidence IS NULL OR s.relevance_confidence >= 0.65)",
        "quantum": "AND s.is_quantum_tech = 1",
        "thailand": "AND s.is_thailand_related = 1",
        "all": "",
    }
    effective_scope = "strict" if not include_filtered else scope
    relevance_clause = scope_clauses.get(effective_scope, scope_clauses["strict"])
    try:
        with get_connection(db) as conn:
            relevance = _relevance_metadata(conn)

            if cdn:
                # Expert mode: return all geo rows including CDN-resolved (raw data, no approximate tier)
                real_rows = conn.execute(f"""
                    SELECT g.source_id, g.lat, g.lng, g.city, g.region,
                           g.isp, g.asn_org, g.is_cdn_resolved,
                           s.platform, s.url, s.title, s.published_year, s.quantum_domain, s.fetched_at,
                           s.channel_title, s.channel_country,
                           s.is_quantum_tech, s.is_thailand_related,
                           s.relevance_confidence, s.view_count,
                           0 AS is_approximate
                    FROM geo g
                    JOIN sources s ON g.source_id = s.id
                    WHERE g.lat IS NOT NULL AND g.lng IS NOT NULL
                      {relevance_clause}
                    ORDER BY s.published_year DESC, g.source_id DESC
                    LIMIT 2000
                """).fetchall()
                approx_candidates: list = []
            else:
                # Default: Tier 1 = real IP geo, Tier 2 = channel_country centroid fallback
                real_rows = conn.execute(f"""
                    SELECT g.source_id, g.lat, g.lng, g.city, g.region,
                           g.isp, g.asn_org, g.is_cdn_resolved,
                           s.platform, s.url, s.title, s.published_year, s.quantum_domain, s.fetched_at,
                           s.channel_title, s.channel_country,
                           s.is_quantum_tech, s.is_thailand_related,
                           s.relevance_confidence, s.view_count,
                           0 AS is_approximate
                    FROM geo g
                    JOIN sources s ON g.source_id = s.id
                    WHERE g.lat IS NOT NULL AND g.lng IS NOT NULL
                      AND (g.is_cdn_resolved = 0 OR g.is_cdn_resolved IS NULL)
                      {relevance_clause}
                    ORDER BY s.published_year DESC, g.source_id DESC
                    LIMIT 2000
                """).fetchall()

                # Sources with no real geo but have channel_country — synthesise centroid point
                approx_candidates = conn.execute(f"""
                    SELECT s.id AS source_id, s.channel_country,
                           s.platform, s.url, s.title, s.published_year, s.quantum_domain, s.fetched_at,
                           s.channel_title,
                           s.is_quantum_tech, s.is_thailand_related,
                           s.relevance_confidence, s.view_count
                    FROM sources s
                    WHERE s.channel_country IS NOT NULL
                      {relevance_clause}
                      AND s.id NOT IN (
                          SELECT g2.source_id FROM geo g2
                          WHERE g2.is_cdn_resolved = 0 OR g2.is_cdn_resolved IS NULL
                      )
                    ORDER BY s.published_year DESC, s.id DESC
                    LIMIT 2000
                """).fetchall()

    except Exception as exc:
        return JSONResponse(
            {
                "ok": False,
                "data": [],
                "count": 0,
                "relevance": None,
                "error": {"code": "geo_list_failed", "message": str(exc)},
            },
            status_code=500,
        )

    items: list[dict] = [dict(r) for r in real_rows]
    real_ids = {r["source_id"] for r in items}

    for r in approx_candidates:
        d = dict(r)
        country = d.get("channel_country") or ""
        if country not in _CENTROIDS or d["source_id"] in real_ids:
            continue
        lat0, lng0 = _CENTROIDS[country]
        items.append({
            "source_id": d["source_id"],
            "lat": lat0,
            "lng": lng0,
            "city": None,
            "region": None,
            "isp": None,
            "asn_org": None,
            "is_cdn_resolved": None,
            "platform": d["platform"],
            "url": d["url"],
            "title": d["title"],
            "published_year": d["published_year"],
            "quantum_domain": d["quantum_domain"],
            "fetched_at": d["fetched_at"],
            "channel_title": d["channel_title"],
            "channel_country": country,
            "is_quantum_tech": d["is_quantum_tech"],
            "is_thailand_related": d["is_thailand_related"],
            "relevance_confidence": d["relevance_confidence"],
            "view_count": d["view_count"],
            "is_approximate": 1,
        })

    return JSONResponse(
        {
            "ok": True,
            "data": items,
            "count": len(items),
            "real_count": len(real_ids),
            "approximate_count": len(items) - len(real_ids),
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
    edge_agg: dict[tuple[str, str], dict[str, Any]] = {}

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

    try:
        payload = build_yearly_taxonomy_analytics([dict(row) for row in rows])
    except Exception as exc:
        return JSONResponse(
            {
                "ok": False,
                "data": {
                    "topics": {"labels": [], "years": [], "series": [], "tests": {}, "graph": {"nodes": [], "links": [], "community_summaries": []}},
                    "productions": {"labels": [], "years": [], "series": [], "tests": {}, "graph": {"nodes": [], "links": [], "community_summaries": []}},
                    "method_note": "",
                },
                "relevance": relevance,
                "error": {"code": "yearly_taxonomy_failed", "message": str(exc)},
            },
            status_code=500,
        )
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
    search: str | None = Query(None, description="Full-text search on title"),
    source_id: int | None = Query(None, description="Direct lookup by source ID"),
    include_filtered: bool = Query(True, description="Deprecated — use relevance_scope."),
    relevance_scope: str = Query(
        "strict",
        description="strict (default, qt AND th) | relevant (qt OR th) | quantum | thailand | all",
    ),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
) -> JSONResponse:
    """Paginated source list. Default scope='strict' (qt=1 AND th=1 — 154 clean sources)."""
    db = _db()
    conditions: list[str] = []
    params: list[Any] = []

    # ── relevance scope ──────────────────────────────────────────────────────
    effective_scope = relevance_scope if include_filtered else "strict"
    if effective_scope == "strict":
        conditions.append("s.is_quantum_tech = 1")
        conditions.append("s.is_thailand_related = 1")
        # confidence floor: exclude borderline-accepted sources from public view
        conditions.append("(s.relevance_confidence IS NULL OR s.relevance_confidence >= 0.65)")
    elif effective_scope == "quantum":
        conditions.append("s.is_quantum_tech = 1")
    elif effective_scope == "thailand":
        conditions.append("s.is_thailand_related = 1")
    elif effective_scope == "relevant":
        conditions.append("(s.is_quantum_tech = 1 OR s.is_thailand_related = 1)")
    # "all" → no relevance filter (includes fully-rejected)

    # ── field filters ────────────────────────────────────────────────────────
    if source_id is not None:
        conditions.append("s.id = ?")
        params.append(source_id)
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
    if search is not None and search.strip():
        conditions.append("s.title LIKE ?")
        params.append(f"%{search.strip()}%")

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
                       s.quantum_domain, s.fetched_at,
                       s.channel_id, s.channel_title, s.channel_country, s.channel_default_language,
                       s.is_quantum_tech, s.is_thailand_related,
                       s.rejection_reason, s.relevance_confidence,
                       e.content_type, e.production_type, e.area, e.engagement_level,
                       e.media_format, e.user_intent,
                       CASE WHEN EXISTS (
                           SELECT 1 FROM geo g
                           WHERE g.source_id = s.id
                             AND g.lat IS NOT NULL
                             AND (g.is_cdn_resolved = 0 OR g.is_cdn_resolved IS NULL)
                       ) THEN 1 ELSE 0 END AS has_real_geo
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
# API — auth/profile
# ---------------------------------------------------------------------------

@app.get("/api/auth/config")
def api_auth_config() -> JSONResponse:
    return JSONResponse(
        {
            "ok": True,
            "data": {
                "enabled": supabase_enabled(),
                "auth_mode": "local" if _prefer_local_auth() else "supabase",
                "local_mode": _prefer_local_auth(),
                "google_oauth_available": supabase_enabled() and not _prefer_local_auth(),
                "supabase_url": settings.supabase_url,
                "supabase_publishable_key": settings.supabase_publishable_key,
            },
            "error": None,
        }
    )


@app.post("/api/auth/local/register", status_code=201)
def api_local_auth_register(payload: dict[str, Any]) -> JSONResponse:
    _ensure_local_auth_tables()
    email = str(payload.get("email") or "").strip().lower()
    password = str(payload.get("password") or "")
    if not email or "@" not in email:
        return JSONResponse({"ok": False, "data": None, "error": {"code": "email_invalid", "message": "A valid email is required."}}, status_code=422)
    if len(password) < 6:
        return JSONResponse({"ok": False, "data": None, "error": {"code": "password_short", "message": "Password must be at least 6 characters."}}, status_code=422)
    db = _db()
    with get_connection(db) as conn:
        existing = conn.execute("SELECT id FROM local_users WHERE email = ?", (email,)).fetchone()
        if existing:
            return JSONResponse({"ok": False, "data": None, "error": {"code": "email_exists", "message": "An account with this email already exists."}}, status_code=409)
        count_row = conn.execute("SELECT COUNT(*) AS count FROM local_users").fetchone()
        role = "admin" if int(count_row["count"]) == 0 else "user"
        user_id = secrets.token_hex(16)
        salt = secrets.token_hex(16)
        now = _utcnow_iso()
        conn.execute(
            """
            INSERT INTO local_users (id, email, password_salt, password_hash, display_name, role, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (user_id, email, salt, _password_hash(password, salt=salt), email.split("@")[0], role, now, now),
        )
        conn.commit()
        row = conn.execute("SELECT * FROM local_users WHERE id = ?", (user_id,)).fetchone()
    return JSONResponse({"ok": True, "data": {"user": _local_profile_payload(row)}, "error": None}, status_code=201)


@app.post("/api/auth/local/login")
def api_local_auth_login(payload: dict[str, Any]) -> JSONResponse:
    _ensure_local_auth_tables()
    email = str(payload.get("email") or "").strip().lower()
    password = str(payload.get("password") or "")
    db = _db()
    with get_connection(db) as conn:
        row = conn.execute("SELECT * FROM local_users WHERE email = ?", (email,)).fetchone()
        if not row:
            return JSONResponse({"ok": False, "data": None, "error": {"code": "auth_invalid", "message": "Invalid email or password."}}, status_code=401)
        expected = _password_hash(password, salt=row["password_salt"])
        if not hmac.compare_digest(expected, row["password_hash"]):
            return JSONResponse({"ok": False, "data": None, "error": {"code": "auth_invalid", "message": "Invalid email or password."}}, status_code=401)
        token = secrets.token_urlsafe(32)
        expires_at = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()
        conn.execute("INSERT INTO local_sessions (token, user_id, created_at, expires_at) VALUES (?, ?, ?, ?)", (token, row["id"], _utcnow_iso(), expires_at))
        conn.commit()
    response = JSONResponse({"ok": True, "data": {"user": _local_profile_payload(row)}, "error": None})
    response.set_cookie(_LOCAL_SESSION_COOKIE, token, httponly=True, samesite="lax", secure=False, path="/")
    return response


@app.post("/api/auth/local/logout")
def api_local_auth_logout(request: Request) -> JSONResponse:
    token = request.cookies.get(_LOCAL_SESSION_COOKIE)
    if token:
        db = _db()
        with get_connection(db) as conn:
            conn.execute("DELETE FROM local_sessions WHERE token = ?", (token,))
            conn.commit()
    response = JSONResponse({"ok": True, "data": {"logged_out": True}, "error": None})
    response.delete_cookie(_LOCAL_SESSION_COOKIE, path="/")
    return response


@app.get("/api/auth/me")
def api_auth_me(request: Request) -> JSONResponse:
    if _prefer_local_auth():
        user = _require_local_user(request)
        if isinstance(user, JSONResponse):
            return user
        return JSONResponse(
            {
                "ok": True,
                "data": {
                    "user": {
                        "id": user.id,
                        "email": user.email,
                        "created_at": user.created_at,
                        "display_name": user.display_name,
                        "avatar_url": user.avatar_url,
                        "user_metadata": {},
                    },
                    "profile": _local_profile_payload(user.raw),
                },
                "error": None,
            }
        )
    auth_result = _require_auth_user(request)
    if isinstance(auth_result, JSONResponse):
        return auth_result
    access_token, user = auth_result
    try:
        profile = ensure_profile_for_user(access_token, user)
    except SupabaseError as exc:
        return _supabase_error_response(exc, code="profile_sync_failed", status_code=500)
    return JSONResponse(
        {
            "ok": True,
            "data": {
                "user": {
                    "id": user.id,
                    "email": user.email,
                    "created_at": user.created_at,
                    "display_name": user.display_name,
                    "avatar_url": user.avatar_url,
                    "user_metadata": user.user_metadata,
                },
                "profile": _profile_payload(profile, user),
            },
            "error": None,
        }
    )


@app.post("/api/auth/sync-profile")
def api_auth_sync_profile(request: Request) -> JSONResponse:
    if _prefer_local_auth():
        user = _require_local_user(request)
        if isinstance(user, JSONResponse):
            return user
        return JSONResponse({"ok": True, "data": {"profile": _local_profile_payload(user.raw)}, "error": None})
    auth_result = _require_auth_user(request)
    if isinstance(auth_result, JSONResponse):
        return auth_result
    access_token, user = auth_result
    try:
        profile = ensure_profile_for_user(access_token, user)
    except SupabaseError as exc:
        return _supabase_error_response(exc, code="profile_sync_failed", status_code=500)
    return JSONResponse({"ok": True, "data": {"profile": _profile_payload(profile, user)}, "error": None})


@app.get("/api/profile")
def api_profile_get(request: Request) -> JSONResponse:
    if _prefer_local_auth():
        user = _require_local_user(request)
        if isinstance(user, JSONResponse):
            return user
        return JSONResponse({"ok": True, "data": {"profile": _local_profile_payload(user.raw)}, "error": None})
    auth_result = _require_auth_user(request)
    if isinstance(auth_result, JSONResponse):
        return auth_result
    access_token, user = auth_result
    try:
        profile = ensure_profile_for_user(access_token, user)
    except SupabaseError as exc:
        return _supabase_error_response(exc, code="profile_load_failed", status_code=500)
    return JSONResponse({"ok": True, "data": {"profile": _profile_payload(profile, user)}, "error": None})


@app.patch("/api/profile")
def api_profile_update(request: Request, payload: dict[str, Any]) -> JSONResponse:
    if _prefer_local_auth():
        user = _require_local_user(request)
        if isinstance(user, JSONResponse):
            return user
        patch: dict[str, Any] = {}
        for key in ("display_name", "bio", "website_url", "avatar_url"):
            if key in payload:
                value = str(payload.get(key) or "").strip()
                patch[key] = value or None
        if not patch:
            return JSONResponse({"ok": False, "data": None, "error": {"code": "profile_empty_update", "message": "No profile fields were provided."}}, status_code=422)
        patch["updated_at"] = _utcnow_iso()
        assignments = ", ".join(f"{key} = ?" for key in patch)
        values = list(patch.values()) + [user.id]
        db = _db()
        with get_connection(db) as conn:
            conn.execute(f"UPDATE local_users SET {assignments} WHERE id = ?", values)
            conn.commit()
            row = conn.execute("SELECT * FROM local_users WHERE id = ?", (user.id,)).fetchone()
        return JSONResponse({"ok": True, "data": {"profile": _local_profile_payload(row)}, "error": None})
    auth_result = _require_auth_user(request)
    if isinstance(auth_result, JSONResponse):
        return auth_result
    access_token, user = auth_result
    patch: dict[str, Any] = {}
    for key in ("display_name", "bio", "website_url", "avatar_url"):
        value = payload.get(key)
        if value is None:
            continue
        patch[key] = str(value).strip() or None
    if not patch:
        return JSONResponse(
            {
                "ok": False,
                "data": None,
                "error": {"code": "profile_empty_update", "message": "No profile fields were provided."},
            },
            status_code=422,
        )
    try:
        ensure_profile_for_user(access_token, user)
        rows = rest_update(
            "profiles",
            patch,
            filters={"id": f"eq.{user.id}"},
            access_token=access_token,
        )
        profile = rows[0] if rows else current_profile(access_token, user.id)
    except SupabaseError as exc:
        return _supabase_error_response(exc, code="profile_update_failed", status_code=500)
    return JSONResponse({"ok": True, "data": {"profile": _profile_payload(profile, user)}, "error": None})


# ---------------------------------------------------------------------------
# API — categories / submitted data
# ---------------------------------------------------------------------------

@app.get("/api/categories")
def api_categories() -> JSONResponse:
    if _prefer_local_auth():
        db = _db()
        with get_connection(db) as conn:
            rows = conn.execute("SELECT id, name, slug, description, created_by, created_at FROM local_categories ORDER BY name ASC").fetchall()
        return JSONResponse({"ok": True, "data": {"items": [dict(row) for row in rows]}, "error": None})
    if not supabase_enabled():
        return _supabase_not_configured_response()
    try:
        rows = rest_select("categories", order="name.asc")
    except SupabaseError as exc:
        return _supabase_error_response(exc, code="categories_load_failed", status_code=500)
    return JSONResponse({"ok": True, "data": {"items": rows}, "error": None})


@app.post("/api/categories", status_code=201)
def api_categories_create(request: Request, payload: dict[str, Any]) -> JSONResponse:
    if _prefer_local_auth():
        user = _require_local_user(request)
        if isinstance(user, JSONResponse):
            return user
        name = str(payload.get("name") or "").strip()
        description = str(payload.get("description") or "").strip() or None
        if not name:
            return JSONResponse({"ok": False, "data": None, "error": {"code": "category_name_required", "message": "Category name is required."}}, status_code=422)
        slug = slugify(name)
        if not slug:
            return JSONResponse({"ok": False, "data": None, "error": {"code": "category_slug_invalid", "message": "Category name must contain letters or numbers."}}, status_code=422)
        db = _db()
        with get_connection(db) as conn:
            existing = conn.execute("SELECT id FROM local_categories WHERE slug = ?", (slug,)).fetchone()
            if existing:
                return JSONResponse({"ok": False, "data": None, "error": {"code": "category_slug_exists", "message": "A category with this slug already exists."}}, status_code=409)
            now = _utcnow_iso()
            cur = conn.execute(
                "INSERT INTO local_categories (name, slug, description, created_by, created_at) VALUES (?, ?, ?, ?, ?)",
                (name, slug, description, user.id, now),
            )
            conn.commit()
            row = conn.execute("SELECT id, name, slug, description, created_by, created_at FROM local_categories WHERE id = ?", (cur.lastrowid,)).fetchone()
        return JSONResponse({"ok": True, "data": {"category": dict(row)}, "error": None}, status_code=201)
    auth_result = _require_auth_user(request)
    if isinstance(auth_result, JSONResponse):
        return auth_result
    access_token, user = auth_result
    name = str(payload.get("name") or "").strip()
    description = str(payload.get("description") or "").strip() or None
    if not name:
        return JSONResponse(
            {
                "ok": False,
                "data": None,
                "error": {"code": "category_name_required", "message": "Category name is required."},
            },
            status_code=422,
        )
    slug = slugify(name)
    if not slug:
        return JSONResponse(
            {
                "ok": False,
                "data": None,
                "error": {"code": "category_slug_invalid", "message": "Category name must contain letters or numbers."},
            },
            status_code=422,
        )
    try:
        existing = rest_select("categories", filters={"slug": f"eq.{slug}"}, single=True)
        if existing:
            return JSONResponse(
                {
                    "ok": False,
                    "data": None,
                    "error": {"code": "category_slug_exists", "message": "A category with this slug already exists."},
                },
                status_code=409,
            )
        rows = rest_insert(
            "categories",
            {
                "name": name,
                "slug": slug,
                "description": description,
                "created_by": user.id,
            },
            access_token=access_token,
        )
    except SupabaseError as exc:
        return _supabase_error_response(exc, code="category_create_failed", status_code=500)
    return JSONResponse({"ok": True, "data": {"category": rows[0] if rows else None}, "error": None}, status_code=201)


@app.get("/api/submitted-data/mine")
def api_submitted_data_mine(request: Request, limit: int = Query(20, ge=1, le=100)) -> JSONResponse:
    if _prefer_local_auth():
        user = _require_local_user(request)
        if isinstance(user, JSONResponse):
            return user
        db = _db()
        with get_connection(db) as conn:
            rows = conn.execute(
                "SELECT * FROM local_submitted_data WHERE user_id = ? ORDER BY created_at DESC LIMIT ?",
                (user.id, limit),
            ).fetchall()
        return JSONResponse({"ok": True, "data": {"items": [_local_submission_payload(row) for row in rows]}, "error": None})
    auth_result = _require_auth_user(request)
    if isinstance(auth_result, JSONResponse):
        return auth_result
    access_token, user = auth_result
    try:
        _rows = rest_select(
            "submitted_data",
            filters={"user_id": f"eq.{user.id}"},
            access_token=access_token,
            limit=limit,
            order="created_at.desc",
        )
    except SupabaseError as exc:
        return _supabase_error_response(exc, code="submitted_data_load_failed", status_code=500)
    rows: list[dict[str, Any]] = _rows if isinstance(_rows, list) else []
    return JSONResponse(
        {
            "ok": True,
            "data": {"items": [_submitted_data_payload(row) for row in rows]},
            "error": None,
        }
    )


@app.get("/api/submitted-data/public")
def api_submitted_data_public(
    page_target: str | None = Query(None),
    category: str | None = Query(None),
    limit: int = Query(12, ge=1, le=100),
) -> JSONResponse:
    if _prefer_local_auth():
        db = _db()
        query = "SELECT * FROM local_submitted_data WHERE status = 'approved' AND analysis_status = 'completed'"
        params: list[Any] = []
        if page_target:
            query += " AND page_target = ?"
            params.append(page_target.strip())
        if category:
            query += " AND category = ?"
            params.append(category.strip())
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        with get_connection(db) as conn:
            rows = conn.execute(query, params).fetchall()
        return JSONResponse({"ok": True, "data": {"items": [_local_submission_payload(row) for row in rows]}, "error": None})
    if not supabase_enabled():
        return _supabase_not_configured_response()
    filters: dict[str, str] = {
        "status": "eq.approved",
        "analysis_status": "eq.completed",
    }
    if page_target:
        filters["page_target"] = f"eq.{page_target.strip()}"
    if category:
        filters["category"] = f"eq.{category.strip()}"
    try:
        _rows = rest_select("submitted_data", filters=filters, limit=limit, order="created_at.desc")
    except SupabaseError as exc:
        return _supabase_error_response(exc, code="submitted_data_public_failed", status_code=500)
    rows: list[dict[str, Any]] = _rows if isinstance(_rows, list) else []
    return JSONResponse(
        {
            "ok": True,
            "data": {"items": [_submitted_data_payload(row) for row in rows]},
            "error": None,
        }
    )


@app.post("/api/submitted-data/analyze-url")
def api_submitted_data_analyze_url(
    request: Request,
    payload: dict[str, Any],
) -> JSONResponse:
    """Fetch URL page text then call Claude to extract title/description/tags."""
    if _prefer_local_auth():
        user = _require_local_user(request)
        if isinstance(user, JSONResponse):
            return user
    else:
        auth_result = _require_auth_user(request)
        if isinstance(auth_result, JSONResponse):
            return auth_result

    url = str(payload.get("url") or "").strip()
    if not url:
        return JSONResponse(
            {"ok": False, "data": None, "error": {"code": "url_required", "message": "url is required."}},
            status_code=422,
        )

    try:
        context = _fetch_url_analysis_context(url)
    except ValueError as exc:
        return JSONResponse(
            {"ok": False, "data": None, "error": {"code": "url_invalid", "message": str(exc)}},
            status_code=422,
        )
    except Exception as exc:
        logger.warning("analyze_url: page fetch failed for %s: %s", url, exc)
        context = {
            "url": url,
            "title": None,
            "description": None,
            "text": None,
            "source_access": {"ok": False, "method": None, "status_code": None, "message": str(exc)},
        }

    from siamquantum.services import claude
    try:
        page_text = "\n\n".join(
            part
            for part in (
                f"Title: {context.get('title')}" if context.get("title") else None,
                f"Description: {context.get('description')}" if context.get("description") else None,
                str(context.get("text") or "").strip() or None,
            )
            if part
        ) or None
        result = claude.analyze_url(url, page_text)
    except Exception as exc:
        logger.warning("analyze_url: claude call failed: %s", exc)
        return JSONResponse(
            {"ok": False, "data": None, "error": {"code": "analysis_failed", "message": str(exc)}},
            status_code=500,
        )
    if not result.get("title") and context.get("title"):
        result["title"] = context["title"]
    if not result.get("description") and context.get("description"):
        result["description"] = context["description"]
    result["source_access"] = context.get("source_access")

    return JSONResponse({"ok": True, "data": result, "error": None})


@app.post("/api/submitted-data", status_code=201)
def api_submitted_data_create(
    request: Request,
    payload: dict[str, Any],
    background_tasks: BackgroundTasks,
) -> JSONResponse:
    if _prefer_local_auth():
        user = _require_local_user(request)
        if isinstance(user, JSONResponse):
            return user
        title = str(payload.get("title") or "").strip()
        description = str(payload.get("description") or "").strip() or None
        source_url = str(payload.get("source_url") or "").strip() or None
        category, categories = _submission_categories_from_payload(payload)
        page_target = str(payload.get("page_target") or "").strip() or None
        _raw_meta = payload.get("metadata")
        metadata: dict[str, Any] = _raw_meta if isinstance(_raw_meta, dict) else {}
        if not title:
            return JSONResponse({"ok": False, "data": None, "error": {"code": "title_required", "message": "Title is required."}}, status_code=422)
        if not category:
            return JSONResponse({"ok": False, "data": None, "error": {"code": "category_required", "message": "Category is required."}}, status_code=422)
        metadata["tags"] = categories
        metadata["categories"] = categories
        metadata["submitted_by"] = str(user.display_name or user.email or "").strip() or None
        now = _utcnow_iso()
        db = _db()
        with get_connection(db) as conn:
            cur = conn.execute(
                """
                INSERT INTO local_submitted_data
                (user_id, title, description, source_url, category, page_target, status, analysis_status, analysis_result, metadata, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, 'pending', 'queued', NULL, ?, ?, ?)
                """,
                (user.id, title, description, source_url, category, page_target, json.dumps(metadata), now, now),
            )
            conn.commit()
            row = conn.execute("SELECT * FROM local_submitted_data WHERE id = ?", (cur.lastrowid,)).fetchone()
        return JSONResponse({"ok": True, "data": {"item": _local_submission_payload(row)}, "error": None}, status_code=201)
    auth_result = _require_auth_user(request)
    if isinstance(auth_result, JSONResponse):
        return auth_result
    access_token, user = auth_result

    title = str(payload.get("title") or "").strip()
    description = str(payload.get("description") or "").strip() or None
    source_url = str(payload.get("source_url") or "").strip() or None
    category, categories = _submission_categories_from_payload(payload)
    page_target = str(payload.get("page_target") or "").strip() or None
    _raw_meta = payload.get("metadata")
    metadata: dict[str, Any] = _raw_meta if isinstance(_raw_meta, dict) else {}
    if not title:
        return JSONResponse(
            {"ok": False, "data": None, "error": {"code": "title_required", "message": "Title is required."}},
            status_code=422,
        )
    if not category:
        return JSONResponse(
            {"ok": False, "data": None, "error": {"code": "category_required", "message": "Category is required."}},
            status_code=422,
        )
    metadata["tags"] = categories
    metadata["categories"] = categories

    try:
        profile = ensure_profile_for_user(access_token, user)
        owner_label = str(profile.get("display_name") or user.display_name or user.email or "").strip() or None
        metadata["submitted_by"] = owner_label
        rows = rest_insert(
            "submitted_data",
            {
                "user_id": user.id,
                "title": title,
                "description": description,
                "source_url": source_url,
                "category": category,
                "page_target": page_target,
                "status": "pending",
                "analysis_status": "queued",
                "metadata": metadata,
            },
            access_token=access_token,
        )
        created = rows[0] if rows else None
    except SupabaseError as exc:
        return _supabase_error_response(exc, code="submitted_data_create_failed", status_code=500)

    if not created:
        return JSONResponse(
            {
                "ok": False,
                "data": None,
                "error": {"code": "submitted_data_create_failed", "message": "Submission was not created."},
            },
            status_code=500,
        )

    background_tasks.add_task(
        _enqueue_submitted_data_analysis,
        int(created["id"]),
        source_url,
        owner_label,
    )

    return JSONResponse(
        {
            "ok": True,
            "data": {"item": _submitted_data_payload(created)},
            "error": None,
        },
        status_code=201,
    )


@app.get("/api/admin/submitted-data")
def api_admin_submitted_data(
    request: Request,
    status: str | None = Query(None),
    category: str | None = Query(None),
    limit: int = Query(50, ge=1, le=200),
) -> JSONResponse:
    if _prefer_local_auth():
        admin_result = _require_local_admin(request)
        if isinstance(admin_result, JSONResponse):
            return admin_result
        db = _db()
        query = "SELECT * FROM local_submitted_data WHERE 1=1"
        params: list[Any] = []
        if status:
            query += " AND status = ?"
            params.append(status.strip())
        if category:
            query += " AND category = ?"
            params.append(category.strip())
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        with get_connection(db) as conn:
            rows = conn.execute(query, params).fetchall()
        return JSONResponse({"ok": True, "data": {"items": [_local_submission_payload(row) for row in rows]}, "error": None})
    admin_result = _require_admin_user(request)
    if isinstance(admin_result, JSONResponse):
        return admin_result
    filters: dict[str, str] = {}
    if status:
        filters["status"] = f"eq.{status.strip()}"
    if category:
        filters["category"] = f"eq.{category.strip()}"
    try:
        rows = rest_select(
            "submitted_data",
            filters=filters,
            use_service_role=True,
            limit=limit,
            order="created_at.desc",
        )
    except SupabaseError as exc:
        return _supabase_error_response(exc, code="admin_submitted_data_failed", status_code=500)
    return JSONResponse({"ok": True, "data": {"items": rows}, "error": None})


@app.patch("/api/admin/submitted-data/{submission_id}")
def api_admin_submitted_data_update(
    submission_id: int,
    request: Request,
    payload: dict[str, Any],
) -> JSONResponse:
    if _prefer_local_auth():
        admin_result = _require_local_admin(request)
        if isinstance(admin_result, JSONResponse):
            return admin_result
        patch: dict[str, Any] = {}
        for key in ("status", "analysis_status", "analysis_result"):
            if key in payload:
                patch[key] = payload[key]
        if not patch:
            return JSONResponse({"ok": False, "data": None, "error": {"code": "admin_patch_empty", "message": "No admin fields were provided."}}, status_code=422)
        columns: list[str] = []
        values: list[Any] = []
        for key, value in patch.items():
            columns.append(f"{key} = ?")
            if key == "analysis_result":
                values.append(json.dumps(value))
            else:
                values.append(value)
        columns.append("updated_at = ?")
        values.append(_utcnow_iso())
        values.append(submission_id)
        db = _db()
        with get_connection(db) as conn:
            conn.execute(f"UPDATE local_submitted_data SET {', '.join(columns)} WHERE id = ?", values)
            conn.commit()
            row = conn.execute("SELECT * FROM local_submitted_data WHERE id = ?", (submission_id,)).fetchone()
        return JSONResponse({"ok": True, "data": {"item": _local_submission_payload(row) if row else None}, "error": None})
    admin_result = _require_admin_user(request)
    if isinstance(admin_result, JSONResponse):
        return admin_result
    patch: dict[str, Any] = {}
    for key in ("status", "analysis_status", "analysis_result"):
        if key in payload:
            patch[key] = payload[key]
    if not patch:
        return JSONResponse(
            {
                "ok": False,
                "data": None,
                "error": {"code": "admin_patch_empty", "message": "No admin fields were provided."},
            },
            status_code=422,
        )
    try:
        rows = rest_update(
            "submitted_data",
            patch,
            filters={"id": f"eq.{submission_id}"},
            use_service_role=True,
        )
    except SupabaseError as exc:
        return _supabase_error_response(exc, code="admin_submitted_data_update_failed", status_code=500)
    return JSONResponse({"ok": True, "data": {"item": rows[0] if rows else None}, "error": None})


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
    relevance_summary: dict[str, Any] | None = None
    relevance_audit: dict[str, Any] | None = None
    if total_inserted > 0:
        try:
            relevance_summary = backfill_relevance(db)
        except Exception as exc:
            logger.exception("Manual relevance backfill failed")
            results["relevance"] = {"error": str(exc)}
    try:
        relevance_audit = recheck_relevance(
            db,
            stale_after_days=settings.relevance_recheck_days,
            limit=settings.relevance_audit_batch_size,
        )
    except Exception as exc:
        logger.exception("Manual relevance audit failed")
        results["relevance_audit"] = {"error": str(exc)}
    if total_inserted > 0 or (relevance_audit and int(relevance_audit.get("checked", 0)) > 0):
        _invalidate_node_registry()
        if not settings.database_read_only:
            with get_connection(db) as conn:
                StatsCacheRepo(conn).invalidate("graph:node_details")
    return JSONResponse(
        {
            "ok": True,
            "date": today.isoformat(),
            "results": results,
            "relevance": relevance_summary,
            "relevance_audit": relevance_audit,
            "error": None,
        }
    )


@app.post("/api/pipeline/trigger")
async def api_pipeline_trigger() -> JSONResponse:
    """Kick off an on-demand ingest in the background. Returns immediately."""
    if _ingest_lock.locked():
        return JSONResponse({"ok": True, "data": {"status": "already_running"}, "error": None})
    asyncio.create_task(_run_ingest_now())
    return JSONResponse({"ok": True, "data": {"status": "started"}, "error": None})


@app.get("/api/cron/ingest")
async def api_cron_ingest(request: Request) -> JSONResponse:
    """Vercel cron endpoint — runs ingest synchronously and returns results.
    Protected by CRON_SECRET header set automatically by Vercel."""
    auth = request.headers.get("authorization", "")
    expected = f"Bearer {settings.cron_secret}" if settings.cron_secret else None
    if expected and auth != expected:
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401)
    try:
        counts = await _run_ingest_now()
        return JSONResponse({"ok": True, "data": counts, "error": None})
    except Exception as exc:
        logger.exception("Cron ingest failed")
        return JSONResponse(
            {"ok": False, "data": None, "error": str(exc)},
            status_code=500,
        )


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
            try:
                meta_row = conn.execute(
                    "SELECT value FROM pipeline_meta WHERE key='last_ingest_at'"
                ).fetchone()
            except Exception:
                meta_row = None
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
        "latest_fetch_at": (meta_row["value"] if meta_row else None) or (overview_row["latest_fetch_at"] if overview_row else None),
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


# ---------------------------------------------------------------------------
# API — Algorithm registry status  (for /api/system/algos)
# ---------------------------------------------------------------------------

@app.get("/api/system/algos")
def api_system_algos() -> JSONResponse:
    """
    Returns all registered algorithm versions and their performance scores.
    Use this to monitor which algorithm version is currently 'best' and
    to submit validation scores after ground-truth comparisons.
    """
    try:
        from siamquantum.pipeline.algo_registry import algo_registry
        return JSONResponse({"ok": True, "data": algo_registry.report(), "error": None})
    except Exception as exc:
        return JSONResponse({"ok": False, "data": {}, "error": {"code": "algo_report_failed", "message": str(exc)}}, status_code=500)


@app.post("/api/system/algos/{name}/{version}/score")
def api_system_algo_score(
    name: str,
    version: str,
    score: float = Query(..., ge=0.0, le=1.0, description="Validation score 0–1"),
) -> JSONResponse:
    """
    Submit a validation score for an algorithm version.
    The registry uses accumulated scores to auto-select the best version.
    Call this after comparing algorithm output against human-labeled ground truth.
    """
    try:
        from siamquantum.pipeline.algo_registry import algo_registry
        algo_registry.record_validation(name, version, score)
        return JSONResponse({"ok": True, "data": {"name": name, "version": version, "score": score}, "error": None})
    except KeyError as exc:
        return JSONResponse({"ok": False, "data": None, "error": {"code": "algo_not_found", "message": str(exc)}}, status_code=404)


# ---------------------------------------------------------------------------
# API — Adapter registry  (for /api/system/adapters)
# ---------------------------------------------------------------------------

@app.get("/api/system/adapters")
def api_system_adapters() -> JSONResponse:
    """Lists all registered source adapters (platform_id + display_name)."""
    try:
        from siamquantum.adapters import adapter_registry
        adapters = [
            {"platform_id": pid, "display_name": a.display_name}
            for pid, a in adapter_registry.all().items()
        ]
        return JSONResponse({"ok": True, "data": {"adapters": adapters, "count": len(adapters)}, "error": None})
    except Exception as exc:
        return JSONResponse({"ok": False, "data": {"adapters": [], "count": 0}, "error": {"code": "adapters_failed", "message": str(exc)}}, status_code=500)
