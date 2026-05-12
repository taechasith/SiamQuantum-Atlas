-- SiamQuantum Atlas — PostgreSQL DDL (Supabase)
-- Run once in Supabase SQL editor to set up all tables.
-- Idempotent: all statements use IF NOT EXISTS / ON CONFLICT DO NOTHING.

CREATE TABLE IF NOT EXISTS sources (
    id                       BIGSERIAL PRIMARY KEY,
    platform                 TEXT    NOT NULL,
    url                      TEXT    UNIQUE NOT NULL,
    title                    TEXT,
    raw_text                 TEXT,
    published_year           INTEGER NOT NULL,
    fetched_at               TIMESTAMPTZ NOT NULL,
    view_count               INTEGER,
    like_count               INTEGER,
    comment_count            INTEGER,
    is_quantum_tech          INTEGER,
    is_thailand_related      INTEGER,
    quantum_domain           TEXT,
    rejection_reason         TEXT,
    relevance_confidence     DOUBLE PRECISION,
    relevance_checked_at     TIMESTAMPTZ,
    channel_id               TEXT,
    channel_title            TEXT,
    channel_country          TEXT,
    channel_default_language TEXT
);

CREATE TABLE IF NOT EXISTS geo (
    source_id       BIGINT PRIMARY KEY REFERENCES sources(id) ON DELETE CASCADE,
    ip              TEXT,
    lat             DOUBLE PRECISION,
    lng             DOUBLE PRECISION,
    city            TEXT,
    region          TEXT,
    isp             TEXT,
    asn_org         TEXT,
    is_cdn_resolved INTEGER
);

CREATE TABLE IF NOT EXISTS entities (
    source_id           BIGINT PRIMARY KEY REFERENCES sources(id) ON DELETE CASCADE,
    content_type        TEXT,
    production_type     TEXT,
    area                TEXT,
    engagement_level    TEXT,
    media_format        TEXT,
    media_format_detail TEXT,
    user_intent         TEXT,
    thai_cultural_angle TEXT
);

CREATE TABLE IF NOT EXISTS triplets (
    id         BIGSERIAL PRIMARY KEY,
    source_id  BIGINT NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    subject    TEXT   NOT NULL,
    relation   TEXT   NOT NULL,
    object     TEXT   NOT NULL,
    confidence DOUBLE PRECISION NOT NULL DEFAULT 1.0
);

CREATE TABLE IF NOT EXISTS stats_cache (
    key         TEXT PRIMARY KEY,
    value       TEXT NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS community_submissions (
    id           BIGSERIAL PRIMARY KEY,
    handle       TEXT,
    url          TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'pending',
    submitted_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS denstream_state (
    id         INTEGER PRIMARY KEY,
    snapshot   BYTEA NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS nlp_abstentions (
    source_id  BIGINT PRIMARY KEY REFERENCES sources(id) ON DELETE CASCADE,
    status     TEXT NOT NULL DEFAULT 'abstained',
    reason     TEXT,
    updated_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS pipeline_meta (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

-- User profiles (mirrors Supabase auth.users — one row per user)
CREATE TABLE IF NOT EXISTS profiles (
    id           UUID PRIMARY KEY REFERENCES auth.users(id) ON DELETE CASCADE,
    email        TEXT,
    display_name TEXT,
    avatar_url   TEXT,
    bio          TEXT,
    website_url  TEXT,
    role         TEXT NOT NULL DEFAULT 'user',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- RLS: users can only read/write their own profile; service-role bypasses all
ALTER TABLE profiles ENABLE ROW LEVEL SECURITY;

CREATE POLICY IF NOT EXISTS "profiles_select_own"
    ON profiles FOR SELECT
    USING (auth.uid() = id);

CREATE POLICY IF NOT EXISTS "profiles_insert_own"
    ON profiles FOR INSERT
    WITH CHECK (auth.uid() = id);

CREATE POLICY IF NOT EXISTS "profiles_update_own"
    ON profiles FOR UPDATE
    USING (auth.uid() = id)
    WITH CHECK (auth.uid() = id);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_sources_year        ON sources(published_year);
CREATE INDEX IF NOT EXISTS idx_sources_platform    ON sources(platform);
CREATE INDEX IF NOT EXISTS idx_sources_relevant    ON sources(is_quantum_tech, is_thailand_related);
CREATE INDEX IF NOT EXISTS idx_sources_channel     ON sources(channel_id);
CREATE INDEX IF NOT EXISTS idx_triplets_source     ON triplets(source_id);
CREATE INDEX IF NOT EXISTS idx_triplets_subj_obj   ON triplets(subject, object);
CREATE INDEX IF NOT EXISTS idx_entities_media      ON entities(media_format);
CREATE INDEX IF NOT EXISTS idx_entities_intent     ON entities(user_intent);
