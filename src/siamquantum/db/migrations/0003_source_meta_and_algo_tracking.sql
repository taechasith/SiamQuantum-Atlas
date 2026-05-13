-- Migration 0003: source_meta JSON column + algorithm run tracking
--
-- source_meta: arbitrary JSON per source — adapters store type-specific fields here.
-- Example values by platform:
--   youtube: {"thumbnail": "...", "duration_s": 312, "tags": ["quantum", "thai"]}
--   arxiv:   {"doi": "10.1234/...", "authors": ["A. Smith"], "categories": ["quant-ph"]}
--   gdelt:   {"tone": -2.4, "themes": ["SCIENCE", "QUANTUM"]}
-- Future source types store their fields here — no ALTER TABLE ever needed again.
--
-- schema_migrations: tracks which migration files have been applied (managed by migrate.py).
-- algo_runs: lightweight record of every algorithm invocation for performance tracking.

ALTER TABLE sources ADD COLUMN source_meta TEXT;

CREATE TABLE IF NOT EXISTS schema_migrations (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    filename    TEXT    NOT NULL UNIQUE,
    sha1        TEXT    NOT NULL,
    applied_at  TEXT    NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS algo_runs (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    algo_name        TEXT    NOT NULL,
    algo_version     TEXT    NOT NULL,
    input_hash       TEXT    NOT NULL,
    duration_ms      REAL    NOT NULL,
    ok               INTEGER NOT NULL,
    validation_score REAL,
    error            TEXT,
    ts               TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_algo_runs_name_version ON algo_runs(algo_name, algo_version);
CREATE INDEX IF NOT EXISTS idx_algo_runs_ts           ON algo_runs(ts)
