"""
One-time migration: copy SQLite data → Supabase PostgreSQL.

Usage:
    python -m siamquantum.db.migrate_to_pg

Requires SIAMQUANTUM_PG_DATABASE_URL (or PG_DATABASE_URL) in .env.
Reads from the local SQLite DB configured via SIAMQUANTUM_DATABASE_URL.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import psycopg2
import psycopg2.extras

from siamquantum.config import settings
from siamquantum.db.session import db_path_from_url


def _sqlite_conn(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _pg_conn() -> psycopg2.extensions.connection:
    return psycopg2.connect(settings.pg_database_url)


_TABLES = [
    "sources",
    "geo",
    "entities",
    "triplets",
    "stats_cache",
    "community_submissions",
    "denstream_state",
    "nlp_abstentions",
    "pipeline_meta",
]


def migrate() -> None:
    if not settings.pg_database_url:
        print("ERROR: SIAMQUANTUM_PG_DATABASE_URL not set.", file=sys.stderr)
        sys.exit(1)

    db_path = db_path_from_url(settings.database_url)
    if not db_path.exists():
        print(f"ERROR: SQLite DB not found at {db_path}", file=sys.stderr)
        sys.exit(1)

    sqlite = _sqlite_conn(db_path)
    pg = _pg_conn()
    pg_cur = pg.cursor()

    for table in _TABLES:
        try:
            rows = sqlite.execute(f"SELECT * FROM {table}").fetchall()
        except sqlite3.OperationalError:
            print(f"  skip {table} (not in SQLite)")
            continue

        if not rows:
            print(f"  skip {table} (empty)")
            continue

        cols = rows[0].keys()
        placeholders = ", ".join(["%s"] * len(cols))
        col_names = ", ".join(cols)
        sql = (
            f"INSERT INTO {table} ({col_names}) VALUES ({placeholders})"
            f" ON CONFLICT DO NOTHING"
        )

        data = [tuple(row[c] for c in cols) for row in rows]
        psycopg2.extras.execute_batch(pg_cur, sql, data, page_size=500)
        pg.commit()
        print(f"  migrated {table}: {len(data)} rows")

    # Fix sequences so BIGSERIAL doesn't conflict with existing IDs
    for table, col in [
        ("sources", "id"),
        ("triplets", "id"),
        ("community_submissions", "id"),
    ]:
        pg_cur.execute(
            f"SELECT setval(pg_get_serial_sequence('{table}', '{col}'), "
            f"COALESCE(MAX({col}), 1)) FROM {table}"
        )
    pg.commit()
    print("Sequence reset done.")

    sqlite.close()
    pg.close()
    print("Migration complete.")


if __name__ == "__main__":
    migrate()
