-- SiamQuantum Atlas — SQLite DDL
-- Idempotent: all CREATE TABLE/INDEX use IF NOT EXISTS

CREATE TABLE IF NOT EXISTS sources (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    platform      TEXT    NOT NULL,                  -- 'gdelt' | 'youtube'
    url           TEXT    UNIQUE NOT NULL,
    title         TEXT,
    raw_text      TEXT,
    published_year INTEGER NOT NULL,
    fetched_at    TEXT    NOT NULL,                  -- ISO-8601
    view_count    INTEGER,                           -- YouTube only
    like_count    INTEGER,                           -- YouTube only
    comment_count INTEGER                            -- YouTube only
);

CREATE TABLE IF NOT EXISTS geo (
    source_id       INTEGER PRIMARY KEY
                    REFERENCES sources(id) ON DELETE CASCADE,
    ip              TEXT,
    lat             REAL,
    lng             REAL,
    city            TEXT,
    region          TEXT,
    isp             TEXT,
    asn_org         TEXT,       -- MaxMind ASN organisation name
    is_cdn_resolved INTEGER     -- 1 = CDN/cloud IP, 0 = likely origin, NULL = unknown
);

CREATE TABLE IF NOT EXISTS entities (
    source_id       INTEGER PRIMARY KEY
                    REFERENCES sources(id) ON DELETE CASCADE,
    content_type    TEXT,    -- 'academic' | 'news' | 'educational' | 'entertainment'
    production_type TEXT,    -- 'state_research' | 'university' | 'corporate_media' | 'independent'
    area            TEXT,
    engagement_level TEXT   -- 'low' | 'medium' | 'high'
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
    value       TEXT NOT NULL,   -- JSON blob
    computed_at TEXT NOT NULL    -- ISO-8601
);

CREATE TABLE IF NOT EXISTS community_submissions (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    handle       TEXT,
    url          TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'pending',  -- 'pending' | 'processed' | 'rejected'
    submitted_at TEXT NOT NULL                     -- ISO-8601
);

CREATE TABLE IF NOT EXISTS denstream_state (
    id         INTEGER PRIMARY KEY,  -- always row 1
    snapshot   BLOB    NOT NULL,     -- pickle bytes
    updated_at TEXT    NOT NULL      -- ISO-8601
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_sources_year        ON sources(published_year);
CREATE INDEX IF NOT EXISTS idx_sources_platform    ON sources(platform);
CREATE INDEX IF NOT EXISTS idx_triplets_source     ON triplets(source_id);
CREATE INDEX IF NOT EXISTS idx_triplets_subj_obj   ON triplets(subject, object);
