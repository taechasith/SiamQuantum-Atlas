from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone

from siamquantum.models import (
    CommunitySubmissionCreate,
    CommunitySubmissionRow,
    DenStreamStateRow,
    EntityCreate,
    EntityRow,
    GeoCreate,
    GeoRow,
    SourceCreate,
    SourceRow,
    StatsCacheRow,
    TripletCreate,
    TripletRow,
)


def _row(model: type, row: sqlite3.Row) -> object:
    return model.model_validate(dict(row))


# ---------------------------------------------------------------------------
# SourceRepo
# ---------------------------------------------------------------------------

class SourceRepo:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._c = conn

    def insert(self, source: SourceCreate) -> int:
        """Insert; returns new id. Skips on duplicate URL (OR IGNORE)."""
        cur = self._c.execute(
            """
            INSERT OR IGNORE INTO sources
              (platform, url, title, raw_text, published_year, fetched_at,
               view_count, like_count, comment_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                source.platform,
                source.url,
                source.title,
                source.raw_text,
                source.published_year,
                source.fetched_at.isoformat(),
                source.view_count,
                source.like_count,
                source.comment_count,
            ),
        )
        self._c.commit()
        return cur.lastrowid or 0

    def get_by_id(self, source_id: int) -> SourceRow | None:
        row = self._c.execute(
            "SELECT * FROM sources WHERE id = ?", (source_id,)
        ).fetchone()
        return SourceRow.model_validate(dict(row)) if row else None

    def get_by_url(self, url: str) -> SourceRow | None:
        row = self._c.execute(
            "SELECT * FROM sources WHERE url = ?", (url,)
        ).fetchone()
        return SourceRow.model_validate(dict(row)) if row else None

    def list_by_year(self, year: int) -> list[SourceRow]:
        rows = self._c.execute(
            "SELECT * FROM sources WHERE published_year = ? ORDER BY id", (year,)
        ).fetchall()
        return [SourceRow.model_validate(dict(r)) for r in rows]

    def list_missing_geo(self) -> list[SourceRow]:
        rows = self._c.execute(
            """
            SELECT s.* FROM sources s
            LEFT JOIN geo g ON s.id = g.source_id
            WHERE g.source_id IS NULL
            ORDER BY s.id
            """
        ).fetchall()
        return [SourceRow.model_validate(dict(r)) for r in rows]

    def count_by_year(self, year: int) -> int:
        return self._c.execute(
            "SELECT COUNT(*) FROM sources WHERE published_year = ?", (year,)
        ).fetchone()[0]


# ---------------------------------------------------------------------------
# GeoRepo
# ---------------------------------------------------------------------------

class GeoRepo:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._c = conn

    def upsert(self, geo: GeoCreate) -> None:
        self._c.execute(
            """
            INSERT INTO geo (source_id, ip, lat, lng, city, region, isp, asn_org, is_cdn_resolved)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_id) DO UPDATE SET
              ip=excluded.ip, lat=excluded.lat, lng=excluded.lng,
              city=excluded.city, region=excluded.region, isp=excluded.isp,
              asn_org=excluded.asn_org, is_cdn_resolved=excluded.is_cdn_resolved
            """,
            (
                geo.source_id, geo.ip, geo.lat, geo.lng,
                geo.city, geo.region, geo.isp,
                geo.asn_org, geo.is_cdn_resolved,
            ),
        )
        self._c.commit()

    def get_by_source_id(self, source_id: int) -> GeoRow | None:
        row = self._c.execute(
            "SELECT * FROM geo WHERE source_id = ?", (source_id,)
        ).fetchone()
        return GeoRow.model_validate(dict(row)) if row else None

    def list_all(self) -> list[GeoRow]:
        rows = self._c.execute("SELECT * FROM geo WHERE lat IS NOT NULL").fetchall()
        return [GeoRow.model_validate(dict(r)) for r in rows]


# ---------------------------------------------------------------------------
# EntityRepo
# ---------------------------------------------------------------------------

class EntityRepo:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._c = conn

    def upsert(self, entity: EntityCreate) -> None:
        self._c.execute(
            """
            INSERT INTO entities
              (source_id, content_type, production_type, area, engagement_level)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(source_id) DO UPDATE SET
              content_type=excluded.content_type,
              production_type=excluded.production_type,
              area=excluded.area,
              engagement_level=excluded.engagement_level
            """,
            (
                entity.source_id,
                entity.content_type,
                entity.production_type,
                entity.area,
                entity.engagement_level,
            ),
        )
        self._c.commit()

    def get_by_source_id(self, source_id: int) -> EntityRow | None:
        row = self._c.execute(
            "SELECT * FROM entities WHERE source_id = ?", (source_id,)
        ).fetchone()
        return EntityRow.model_validate(dict(row)) if row else None


# ---------------------------------------------------------------------------
# TripletRepo
# ---------------------------------------------------------------------------

class TripletRepo:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._c = conn

    def insert_many(self, triplets: list[TripletCreate]) -> None:
        self._c.executemany(
            """
            INSERT INTO triplets (source_id, subject, relation, object, confidence)
            VALUES (?, ?, ?, ?, ?)
            """,
            [(t.source_id, t.subject, t.relation, t.object, t.confidence) for t in triplets],
        )
        self._c.commit()

    def get_by_source_id(self, source_id: int) -> list[TripletRow]:
        rows = self._c.execute(
            "SELECT * FROM triplets WHERE source_id = ?", (source_id,)
        ).fetchall()
        return [TripletRow.model_validate(dict(r)) for r in rows]

    def list_all(self) -> list[TripletRow]:
        rows = self._c.execute("SELECT * FROM triplets").fetchall()
        return [TripletRow.model_validate(dict(r)) for r in rows]


# ---------------------------------------------------------------------------
# StatsCacheRepo
# ---------------------------------------------------------------------------

class StatsCacheRepo:
    _TTL_SECONDS = 86400  # 24h

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._c = conn

    def get(self, key: str) -> object | None:
        row = self._c.execute(
            "SELECT value, computed_at FROM stats_cache WHERE key = ?", (key,)
        ).fetchone()
        if not row:
            return None
        computed_at = datetime.fromisoformat(row["computed_at"])
        age = (datetime.now(timezone.utc) - computed_at.replace(tzinfo=timezone.utc)).total_seconds()
        if age > self._TTL_SECONDS:
            return None
        return json.loads(row["value"])

    def set(self, key: str, value: object) -> None:
        self._c.execute(
            """
            INSERT INTO stats_cache (key, value, computed_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value, computed_at=excluded.computed_at
            """,
            (key, json.dumps(value), datetime.utcnow().isoformat()),
        )
        self._c.commit()

    def invalidate(self, key: str) -> None:
        self._c.execute("DELETE FROM stats_cache WHERE key = ?", (key,))
        self._c.commit()

    def invalidate_prefix(self, prefix: str) -> None:
        self._c.execute("DELETE FROM stats_cache WHERE key LIKE ?", (f"{prefix}%",))
        self._c.commit()


# ---------------------------------------------------------------------------
# CommunitySubmissionRepo
# ---------------------------------------------------------------------------

class CommunitySubmissionRepo:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._c = conn

    def insert(self, sub: CommunitySubmissionCreate) -> int:
        cur = self._c.execute(
            "INSERT INTO community_submissions (handle, url, status, submitted_at) VALUES (?, ?, 'pending', ?)",
            (sub.handle, sub.url, sub.submitted_at.isoformat()),
        )
        self._c.commit()
        return cur.lastrowid or 0

    def list_pending(self) -> list[CommunitySubmissionRow]:
        rows = self._c.execute(
            "SELECT * FROM community_submissions WHERE status = 'pending' ORDER BY submitted_at"
        ).fetchall()
        return [CommunitySubmissionRow.model_validate(dict(r)) for r in rows]

    def update_status(self, sub_id: int, status: str) -> None:
        self._c.execute(
            "UPDATE community_submissions SET status = ? WHERE id = ?", (status, sub_id)
        )
        self._c.commit()


# ---------------------------------------------------------------------------
# DenStreamStateRepo
# ---------------------------------------------------------------------------

class DenStreamStateRepo:
    _ROW_ID = 1

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._c = conn

    def get_snapshot(self) -> bytes | None:
        row = self._c.execute(
            "SELECT snapshot FROM denstream_state WHERE id = ?", (self._ROW_ID,)
        ).fetchone()
        return bytes(row["snapshot"]) if row else None

    def save_snapshot(self, snapshot: bytes) -> None:
        self._c.execute(
            """
            INSERT INTO denstream_state (id, snapshot, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET snapshot=excluded.snapshot, updated_at=excluded.updated_at
            """,
            (self._ROW_ID, snapshot, datetime.utcnow().isoformat()),
        )
        self._c.commit()
