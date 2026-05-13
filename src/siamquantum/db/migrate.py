"""
Versioned Migration Runner
==========================
Replaces the flat _migrations list in session.py with numbered SQL files.

Rules for future devs:
  1. Create a new file: db/migrations/NNNN_description.sql
     (NNNN = zero-padded number, e.g. 0012_add_arxiv_fields.sql)
  2. Write idempotent SQL (CREATE TABLE IF NOT EXISTS, ADD COLUMN IF NOT EXISTS, etc.)
  3. That's it. The runner applies it exactly once and records it.

NEVER edit or delete existing migration files — add a new one instead.
Each migration is a transaction: if it fails, it rolls back cleanly.
"""
from __future__ import annotations

import hashlib
import logging
import sqlite3
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_MIGRATIONS_DIR = Path(__file__).parent / "migrations"

_BOOTSTRAP_SQL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    filename     TEXT NOT NULL UNIQUE,
    sha1         TEXT NOT NULL,
    applied_at   TEXT NOT NULL DEFAULT (datetime('now'))
);
"""


def _sha1(text: str) -> str:
    return hashlib.sha1(text.encode()).hexdigest()


def _applied(conn: Any) -> set[str]:
    try:
        rows = conn.execute(
            "SELECT filename FROM schema_migrations ORDER BY id"
        ).fetchall()
        return {r["filename"] if hasattr(r, "__getitem__") else r[0] for r in rows}
    except Exception:
        return set()


def run_migrations(conn: Any) -> int:
    """
    Apply all pending migrations in order. Returns count of newly applied migrations.
    `conn` must be a live SQLite or PG connection (DictRow-compatible).
    """
    # Ensure migrations table exists
    try:
        conn.execute(_BOOTSTRAP_SQL)
        conn.commit()
    except Exception:
        pass

    applied = _applied(conn)
    is_pg = getattr(conn, "dialect", "sqlite") == "pg"

    migration_files = sorted(_MIGRATIONS_DIR.glob("*.sql"))
    count = 0

    for path in migration_files:
        filename = path.name
        if filename in applied:
            continue

        sql = path.read_text(encoding="utf-8").strip()
        if not sql:
            continue

        sha = _sha1(sql)
        logger.info("Applying migration: %s", filename)

        try:
            # Execute each statement separately (some files have multiple)
            statements = [s.strip() for s in sql.split(";") if s.strip()]
            for stmt in statements:
                if is_pg:
                    conn.execute(stmt)
                else:
                    # SQLite: wrap ALTER TABLE in try/except (idempotent by convention)
                    try:
                        conn.execute(stmt)
                    except sqlite3.OperationalError as exc:
                        if "duplicate column" in str(exc).lower() or "already exists" in str(exc).lower():
                            pass  # idempotent — already applied
                        else:
                            raise

            conn.execute(
                "INSERT INTO schema_migrations (filename, sha1) VALUES (?, ?)",
                (filename, sha),
            )
            conn.commit()
            count += 1
            logger.info("Applied migration: %s", filename)

        except Exception as exc:
            logger.error("Migration %s failed: %s — rolling back", filename, exc)
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            raise RuntimeError(f"Migration {filename} failed: {exc}") from exc

    if count:
        logger.info("Applied %d new migration(s)", count)
    return count
