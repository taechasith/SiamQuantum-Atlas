PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS sources (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  platform TEXT NOT NULL,
  url TEXT UNIQUE NOT NULL,
  title TEXT,
  raw_text TEXT,
  published_year INTEGER NOT NULL,
  fetched_at TEXT NOT NULL,
  view_count INTEGER,
  like_count INTEGER,
  comment_count INTEGER,
  is_quantum_tech INTEGER,
  is_thailand_related INTEGER,
  quantum_domain TEXT,
  rejection_reason TEXT,
  relevance_confidence REAL,
  relevance_checked_at TEXT,
  channel_id TEXT,
  channel_title TEXT,
  channel_country TEXT,
  channel_default_language TEXT,
  source_meta TEXT
);

CREATE TABLE IF NOT EXISTS geo (
  source_id INTEGER PRIMARY KEY REFERENCES sources(id) ON DELETE CASCADE,
  ip TEXT,
  lat REAL,
  lng REAL,
  city TEXT,
  region TEXT,
  isp TEXT,
  asn_org TEXT,
  is_cdn_resolved INTEGER
);

CREATE TABLE IF NOT EXISTS entities (
  source_id INTEGER PRIMARY KEY REFERENCES sources(id) ON DELETE CASCADE,
  content_type TEXT,
  production_type TEXT,
  area TEXT,
  engagement_level TEXT,
  media_format TEXT,
  media_format_detail TEXT,
  user_intent TEXT,
  thai_cultural_angle TEXT
);

CREATE TABLE IF NOT EXISTS triplets (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
  subject TEXT NOT NULL,
  relation TEXT NOT NULL,
  object TEXT NOT NULL,
  confidence REAL NOT NULL DEFAULT 1.0
);

CREATE TABLE IF NOT EXISTS stats_cache (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  computed_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS community_submissions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  handle TEXT,
  url TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  submitted_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS denstream_state (
  id INTEGER PRIMARY KEY,
  snapshot BLOB NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  flow_name TEXT NOT NULL,
  task_name TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'running',
  started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  finished_at TEXT,
  duration_s REAL,
  error_text TEXT
);

CREATE TABLE IF NOT EXISTS nlp_abstentions (
  source_id INTEGER PRIMARY KEY REFERENCES sources(id) ON DELETE CASCADE,
  status TEXT NOT NULL,
  reason TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pipeline_meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS local_users (
  id TEXT PRIMARY KEY,
  email TEXT NOT NULL UNIQUE,
  password_salt TEXT NOT NULL,
  password_hash TEXT NOT NULL,
  display_name TEXT,
  avatar_url TEXT,
  bio TEXT,
  website_url TEXT,
  organization TEXT,
  focus TEXT,
  citation TEXT,
  role TEXT NOT NULL DEFAULT 'user',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS local_sessions (
  token TEXT PRIMARY KEY,
  user_id TEXT NOT NULL REFERENCES local_users(id) ON DELETE CASCADE,
  created_at TEXT NOT NULL,
  expires_at TEXT
);

CREATE TABLE IF NOT EXISTS local_categories (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL UNIQUE,
  slug TEXT NOT NULL UNIQUE,
  description TEXT,
  created_by TEXT REFERENCES local_users(id) ON DELETE SET NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS local_submitted_data (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id TEXT NOT NULL REFERENCES local_users(id) ON DELETE CASCADE,
  title TEXT NOT NULL,
  description TEXT,
  source_url TEXT,
  category TEXT NOT NULL,
  page_target TEXT,
  status TEXT NOT NULL DEFAULT 'pending',
  analysis_status TEXT NOT NULL DEFAULT 'queued',
  analysis_result TEXT,
  metadata TEXT NOT NULL DEFAULT '{}',
  province_code TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS admin_audit_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  admin_user_id TEXT NOT NULL,
  admin_email TEXT,
  action TEXT NOT NULL,
  target_type TEXT NOT NULL,
  target_id TEXT,
  details TEXT,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS algo_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  algo_name TEXT NOT NULL,
  algo_version TEXT NOT NULL,
  input_hash TEXT NOT NULL,
  duration_ms REAL NOT NULL,
  ok INTEGER NOT NULL,
  validation_score REAL,
  error TEXT,
  ts TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS schema_migrations (
  filename TEXT PRIMARY KEY,
  applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sources_year ON sources(published_year);
CREATE INDEX IF NOT EXISTS idx_sources_platform ON sources(platform);
CREATE INDEX IF NOT EXISTS idx_sources_relevant ON sources(is_quantum_tech, is_thailand_related);
CREATE INDEX IF NOT EXISTS idx_sources_channel ON sources(channel_id);
CREATE INDEX IF NOT EXISTS idx_sources_domain ON sources(quantum_domain);
CREATE INDEX IF NOT EXISTS idx_geo_lat_lng ON geo(lat, lng);
CREATE INDEX IF NOT EXISTS idx_triplets_source ON triplets(source_id);
CREATE INDEX IF NOT EXISTS idx_triplets_subj_obj ON triplets(subject, object);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_flow ON pipeline_runs(flow_name, started_at);
CREATE INDEX IF NOT EXISTS idx_local_sessions_user_id ON local_sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_local_submitted_user_id ON local_submitted_data(user_id);
CREATE INDEX IF NOT EXISTS idx_local_submitted_public ON local_submitted_data(status, analysis_status, created_at);
CREATE INDEX IF NOT EXISTS idx_admin_audit_created ON admin_audit_log(created_at);
