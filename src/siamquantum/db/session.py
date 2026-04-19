from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

_SCHEMA_PATH = Path(__file__).parent / "schema.sql"


def _configure(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row


@contextmanager
def get_connection(db_path: Path) -> Generator[sqlite3.Connection, None, None]:
    conn = sqlite3.connect(str(db_path))
    try:
        _configure(conn)
        yield conn
    finally:
        conn.close()


def _run_migrations(conn: sqlite3.Connection) -> None:
    """ALTER TABLE migrations for columns added after initial schema creation."""
    _migrations = [
        "ALTER TABLE geo ADD COLUMN asn_org TEXT",
        "ALTER TABLE geo ADD COLUMN is_cdn_resolved INTEGER",
        # DQ-1: relevance classifier columns on sources
        "ALTER TABLE sources ADD COLUMN is_quantum_tech INTEGER",
        "ALTER TABLE sources ADD COLUMN is_thailand_related INTEGER",
        "ALTER TABLE sources ADD COLUMN quantum_domain TEXT",
        "ALTER TABLE sources ADD COLUMN rejection_reason TEXT",
        "ALTER TABLE sources ADD COLUMN relevance_confidence REAL",
        "ALTER TABLE sources ADD COLUMN relevance_checked_at TEXT",
        "CREATE INDEX IF NOT EXISTS idx_sources_relevant ON sources(is_quantum_tech, is_thailand_related)",
    ]
    for sql in _migrations:
        try:
            conn.execute(sql)
            conn.commit()
        except sqlite3.OperationalError:
            pass  # column already exists


def init_db(db_path: Path) -> None:
    """Create DB file, run schema.sql, then apply column migrations (idempotent)."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    schema = _SCHEMA_PATH.read_text(encoding="utf-8")
    with get_connection(db_path) as conn:
        conn.executescript(schema)
        conn.commit()
        _run_migrations(conn)


def db_path_from_url(database_url: str) -> Path:
    """Extract filesystem path from sqlite:/// URL."""
    return Path(database_url.replace("sqlite:///", "", 1))
