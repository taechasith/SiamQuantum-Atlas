from __future__ import annotations

import re
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator

from siamquantum.config import settings

_SCHEMA_PATH = Path(__file__).parent / "schema.sql"

# ---------------------------------------------------------------------------
# SQL dialect adapter  (SQLite → PostgreSQL)
# ---------------------------------------------------------------------------

_PH_RE = re.compile(r"\?")
_INSERT_OR_IGNORE_RE = re.compile(r"\bINSERT\s+OR\s+IGNORE\s+INTO\b", re.IGNORECASE)
_DT_NOW_RE = re.compile(r"\bdatetime\s*\(\s*'now'(?:\s*,\s*'[^']*')?\s*\)", re.IGNORECASE)
_DATE_NOW_RE = re.compile(r"\bdate\s*\(\s*'now'(?:\s*,\s*'[^']*')?\s*\)", re.IGNORECASE)
_DT_COL_RE = re.compile(r"\bdatetime\s*\((\w+(?:\.\w+)?)\s*\)", re.IGNORECASE)
_DATE_COL_RE = re.compile(r"\bdate\s*\((\w+(?:\.\w+)?)\s*\)", re.IGNORECASE)


def _adapt_for_pg(sql: str) -> str:
    sql = _PH_RE.sub("%s", sql)
    sql = _INSERT_OR_IGNORE_RE.sub("INSERT INTO", sql)
    sql = _DT_NOW_RE.sub("NOW()", sql)
    sql = _DATE_NOW_RE.sub("CURRENT_DATE", sql)
    sql = _DT_COL_RE.sub(r"\1", sql)
    sql = _DATE_COL_RE.sub(r"\1::date", sql)
    return sql


# ---------------------------------------------------------------------------
# Dict-like row that supports both key and integer access (like sqlite3.Row)
# ---------------------------------------------------------------------------

class DictRow:
    __slots__ = ("_d", "_keys")

    def __init__(self, d: dict[str, Any]) -> None:
        self._d = d
        self._keys = list(d.keys())

    def __getitem__(self, key: str | int) -> Any:
        if isinstance(key, int):
            return self._d[self._keys[key]]
        return self._d[key]

    def get(self, key: str, default: Any = None) -> Any:
        return self._d.get(key, default)

    def keys(self) -> Any:
        return self._d.keys()

    def __iter__(self) -> Any:
        return iter(self._d.values())

    def __contains__(self, key: object) -> bool:
        return key in self._d

    def __repr__(self) -> str:  # pragma: no cover
        return repr(self._d)


# ---------------------------------------------------------------------------
# PostgreSQL connection wrapper
# ---------------------------------------------------------------------------

class _PgCursor:
    """Cursor wrapper: makes psycopg2 look like sqlite3.Cursor."""

    def __init__(self, cur: Any) -> None:
        self._cur = cur

    def fetchone(self) -> DictRow | None:
        row = self._cur.fetchone()
        return DictRow(dict(row)) if row else None

    def fetchall(self) -> list[DictRow]:
        return [DictRow(dict(r)) for r in (self._cur.fetchall() or [])]

    @property
    def lastrowid(self) -> int:
        # Only valid after INSERT ... RETURNING id
        try:
            row = self._cur.fetchone()
            if row:
                val = list(dict(row).values())[0]
                return int(val) if val is not None else 0
        except Exception:
            pass
        return 0


class _PgConn:
    """psycopg2 connection wrapper with sqlite3-compatible interface."""

    dialect: str = "pg"

    def __init__(self, pg_conn: Any) -> None:
        self._conn = pg_conn

    def execute(self, sql: str, params: Any = None) -> _PgCursor:
        from psycopg2.extras import RealDictCursor
        cur = self._conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(_adapt_for_pg(sql), params if params is not None else ())
        return _PgCursor(cur)

    def executemany(self, sql: str, params_seq: Any) -> _PgCursor:
        from psycopg2.extras import RealDictCursor
        cur = self._conn.cursor(cursor_factory=RealDictCursor)
        cur.executemany(_adapt_for_pg(sql), params_seq)
        return _PgCursor(cur)

    def commit(self) -> None:
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "_PgConn":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()


# ---------------------------------------------------------------------------
# Unified get_connection — yields SQLite or PG depending on settings
# ---------------------------------------------------------------------------

@contextmanager
def get_connection(db_path: Path) -> Generator[Any, None, None]:
    if settings.pg_database_url:
        import psycopg2
        conn = psycopg2.connect(settings.pg_database_url)
        pg = _PgConn(conn)
        try:
            yield pg
        finally:
            conn.close()
    else:
        if settings.database_read_only:
            db_uri = db_path.resolve().as_uri()
            conn = sqlite3.connect(f"{db_uri}?mode=ro", uri=True)
        else:
            conn = sqlite3.connect(str(db_path))
        try:
            _configure_sqlite(conn, read_only=settings.database_read_only)
            yield conn
        finally:
            conn.close()


def _configure_sqlite(conn: sqlite3.Connection, *, read_only: bool = False) -> None:
    if not read_only:
        try:
            conn.execute("PRAGMA journal_mode=WAL")
        except sqlite3.OperationalError:
            pass
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row


def _run_migrations(conn: sqlite3.Connection) -> None:
    """ALTER TABLE migrations for columns added after initial schema creation."""
    _migrations = [
        "ALTER TABLE geo ADD COLUMN asn_org TEXT",
        "ALTER TABLE geo ADD COLUMN is_cdn_resolved INTEGER",
        "ALTER TABLE sources ADD COLUMN is_quantum_tech INTEGER",
        "ALTER TABLE sources ADD COLUMN is_thailand_related INTEGER",
        "ALTER TABLE sources ADD COLUMN quantum_domain TEXT",
        "ALTER TABLE sources ADD COLUMN rejection_reason TEXT",
        "ALTER TABLE sources ADD COLUMN relevance_confidence REAL",
        "ALTER TABLE sources ADD COLUMN relevance_checked_at TEXT",
        "CREATE INDEX IF NOT EXISTS idx_sources_relevant ON sources(is_quantum_tech, is_thailand_related)",
        "ALTER TABLE sources ADD COLUMN channel_id TEXT",
        "ALTER TABLE sources ADD COLUMN channel_title TEXT",
        "ALTER TABLE sources ADD COLUMN channel_country TEXT",
        "ALTER TABLE sources ADD COLUMN channel_default_language TEXT",
        "CREATE INDEX IF NOT EXISTS idx_sources_channel ON sources(channel_id)",
        "ALTER TABLE entities ADD COLUMN media_format TEXT",
        "ALTER TABLE entities ADD COLUMN media_format_detail TEXT",
        "ALTER TABLE entities ADD COLUMN user_intent TEXT",
        "ALTER TABLE entities ADD COLUMN thai_cultural_angle TEXT",
        "CREATE INDEX IF NOT EXISTS idx_entities_media_format ON entities(media_format)",
        "CREATE INDEX IF NOT EXISTS idx_entities_user_intent ON entities(user_intent)",
        """CREATE TABLE IF NOT EXISTS nlp_abstentions (
            source_id  INTEGER PRIMARY KEY REFERENCES sources(id) ON DELETE CASCADE,
            status     TEXT NOT NULL DEFAULT 'abstained',
            reason     TEXT,
            updated_at TEXT
        )""",
        "CREATE INDEX IF NOT EXISTS idx_triplets_subj_obj ON triplets(subject, object)",
    ]
    for sql in _migrations:
        try:
            conn.execute(sql)
            conn.commit()
        except sqlite3.OperationalError:
            pass


def init_db(db_path: Path) -> None:
    """Create DB file, run schema.sql, then apply column migrations (idempotent). SQLite only."""
    if settings.pg_database_url:
        return  # PG schema managed via pg_schema.sql in Supabase
    db_path.parent.mkdir(parents=True, exist_ok=True)
    schema = _SCHEMA_PATH.read_text(encoding="utf-8")
    with get_connection(db_path) as conn:
        conn.executescript(schema)
        conn.commit()
        _run_migrations(conn)


def db_path_from_url(database_url: str) -> Path:
    """Extract filesystem path from sqlite:/// URL."""
    return Path(database_url.replace("sqlite:///", "", 1))
