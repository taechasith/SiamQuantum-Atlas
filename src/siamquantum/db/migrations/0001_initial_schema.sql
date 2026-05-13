-- Migration 0001: initial schema (idempotent — all IF NOT EXISTS)
-- Represents the state of the DB at project inception.

CREATE TABLE IF NOT EXISTS sources (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    platform       TEXT    NOT NULL,
    url            TEXT    UNIQUE NOT NULL,
    title          TEXT,
    raw_text       TEXT,
    published_year INTEGER NOT NULL,
    fetched_at     TEXT    NOT NULL,
    view_count     INTEGER,
    like_count     INTEGER,
    comment_count  INTEGER
);

CREATE TABLE IF NOT EXISTS geo (
    source_id       INTEGER PRIMARY KEY REFERENCES sources(id) ON DELETE CASCADE,
    ip              TEXT,
    lat             REAL,
    lng             REAL,
    city            TEXT,
    region          TEXT,
    isp             TEXT,
    asn_org         TEXT,
    is_cdn_resolved INTEGER
);

CREATE TABLE IF NOT EXISTS entities (
    source_id        INTEGER PRIMARY KEY REFERENCES sources(id) ON DELETE CASCADE,
    content_type     TEXT,
    production_type  TEXT,
    area             TEXT,
    engagement_level TEXT
);

CREATE TABLE IF NOT EXISTS triplets (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id  INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    subject    TEXT    NOT NULL,
    relation   TEXT    NOT NULL,
    object     TEXT    NOT NULL,
    confidence REAL    NOT NULL DEFAULT 1.0
);

CREATE TABLE IF NOT EXISTS stats_cache (
    key         TEXT PRIMARY KEY,
    value       TEXT NOT NULL,
    computed_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS community_submissions (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    handle       TEXT,
    url          TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'pending',
    submitted_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS denstream_state (
    id         INTEGER PRIMARY KEY,
    snapshot   BLOB    NOT NULL,
    updated_at TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sources_year     ON sources(published_year);
CREATE INDEX IF NOT EXISTS idx_sources_platform ON sources(platform);
CREATE INDEX IF NOT EXISTS idx_triplets_source  ON triplets(source_id)
