-- Migration 0002: relevance flags, channel metadata, taxonomy columns
-- All ALTER TABLE statements are safe to re-run (duplicate column errors ignored by runner)

ALTER TABLE sources ADD COLUMN is_quantum_tech      INTEGER;
ALTER TABLE sources ADD COLUMN is_thailand_related  INTEGER;
ALTER TABLE sources ADD COLUMN quantum_domain        TEXT;
ALTER TABLE sources ADD COLUMN rejection_reason      TEXT;
ALTER TABLE sources ADD COLUMN relevance_confidence  REAL;
ALTER TABLE sources ADD COLUMN relevance_checked_at  TEXT;
ALTER TABLE sources ADD COLUMN channel_id            TEXT;
ALTER TABLE sources ADD COLUMN channel_title         TEXT;
ALTER TABLE sources ADD COLUMN channel_country       TEXT;
ALTER TABLE sources ADD COLUMN channel_default_language TEXT;

ALTER TABLE entities ADD COLUMN media_format        TEXT;
ALTER TABLE entities ADD COLUMN media_format_detail  TEXT;
ALTER TABLE entities ADD COLUMN user_intent          TEXT;
ALTER TABLE entities ADD COLUMN thai_cultural_angle  TEXT;

CREATE INDEX IF NOT EXISTS idx_sources_relevant     ON sources(is_quantum_tech, is_thailand_related);
CREATE INDEX IF NOT EXISTS idx_sources_channel      ON sources(channel_id);
CREATE INDEX IF NOT EXISTS idx_sources_domain       ON sources(quantum_domain);
CREATE INDEX IF NOT EXISTS idx_entities_media_format ON entities(media_format);
CREATE INDEX IF NOT EXISTS idx_entities_user_intent  ON entities(user_intent);
CREATE INDEX IF NOT EXISTS idx_triplets_subj_obj     ON triplets(subject, object);

CREATE TABLE IF NOT EXISTS nlp_abstentions (
    source_id  INTEGER PRIMARY KEY REFERENCES sources(id) ON DELETE CASCADE,
    status     TEXT NOT NULL DEFAULT 'abstained',
    reason     TEXT,
    updated_at TEXT
);

CREATE TABLE IF NOT EXISTS pipeline_meta (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS local_users (
    id            TEXT PRIMARY KEY,
    email         TEXT,
    password_salt TEXT,
    password_hash TEXT,
    display_name  TEXT,
    avatar_url    TEXT,
    bio           TEXT,
    website_url   TEXT,
    role          TEXT NOT NULL DEFAULT 'user',
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS local_sessions (
    token      TEXT PRIMARY KEY,
    user_id    TEXT NOT NULL REFERENCES local_users(id) ON DELETE CASCADE,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS local_categories (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL,
    slug        TEXT UNIQUE NOT NULL,
    description TEXT,
    created_by  TEXT,
    created_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS local_submitted_data (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id         TEXT REFERENCES local_users(id) ON DELETE SET NULL,
    title           TEXT,
    description     TEXT,
    source_url      TEXT,
    category        TEXT,
    page_target     TEXT,
    status          TEXT NOT NULL DEFAULT 'pending',
    analysis_status TEXT,
    analysis_result TEXT,
    metadata        TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
)
