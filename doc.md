# SiamQuantum Atlas: Architecture Notes

## Overview

SiamQuantum Atlas is a Python-first research platform for collecting, enriching, analysing, and presenting Thai-relevant quantum discourse (2020–present). The stack is FastAPI + Jinja2 + SQLite, with Supabase Auth + PostgREST for user accounts, profiles, categories, and user-owned submitted data. Prefect 3 is available for scheduled orchestration. All code lives in one repo.

## Stack

| Layer | Technology |
|-------|------------|
| Language | Python 3.11+ |
| Web framework | FastAPI + uvicorn (port 8765) |
| Templates | Jinja2 |
| Persistence | SQLite (`data/processed/siamquantum_atlas.db`) |
| Auth / user data | Supabase Auth + PostgREST (optional; falls back to local auth) |
| Schema management | `src/siamquantum/db/schema.sql` + `src/siamquantum/db/migrate.py` + `supabase/migrations/` |
| NLP / AI | Anthropic Claude API (`claude-sonnet-4-6`) |
| Data sources | GDELT API v2, YouTube Data API v3, RSS, curated seeds |
| Geo enrichment | MaxMind GeoLite2 City + ASN (`data/geoip/*.mmdb`) |
| Browser libraries | Leaflet.js, Chart.js, 3d-force-graph (all CDN) |
| CLI | Typer |
| Orchestration | Prefect 3 (optional) |
| Config | pydantic-settings, env prefix `SIAMQUANTUM_` |

## Application Layers

```
src/siamquantum/
├── adapters/         Source Adapter system (protocol + registry for new data sources)
├── db/               Schema DDL, migration runner, session/connection helpers, repos
├── models.py         Pydantic v2 I/O contracts (SourceRaw, SourceRecord, NLPResult, …)
├── config.py         Settings (pydantic-settings, env prefix SIAMQUANTUM_)
├── cli.py            Typer CLI (db, ingest, analyze, filter, serve, orchestration)
├── pipeline/         ingest, NLP analyze, stats, taxonomy, graph metrics, filter, integrity
├── services/         GDELT, YouTube, RSS, seeds, dedup, Claude, GeoIP, Supabase, Google CSE
├── stats/            DenStream clustering, bootstrap, Mann-Kendall, TF-IDF, yearly analytics
├── orchestration/    Prefect 3 flows (refresh + healthcheck) and CLI entry points
└── viewer/           FastAPI server.py — all page routes, JSON API endpoints, Jinja2
```

## Runtime Entry Points

| Entrypoint | Usage |
|------------|-------|
| `python -m siamquantum serve` | Local development — starts uvicorn directly |
| `api/index.py` | Vercel serverless — copies DB to `/tmp`, sets demo env vars, imports FastAPI `app` |
| `app.py` | Convenience shim — adds `src/` to path, re-exports `app` for `uvicorn app:app` |

## SQLite Schema

**Core tables (schema.sql):**

| Table | Purpose |
|-------|---------|
| `sources` | One row per URL. Platform, title, raw_text, year, view/like/comment counts. |
| `geo` | Geo enrichment per source: lat/lng, city, region, ISP, ASN org, CDN flag, approximate flag. |
| `entities` | NLP-derived per source: content_type, production_type, area, engagement_level. |
| `triplets` | Subject–relation–object triplets extracted per source, with confidence score. |
| `stats_cache` | Key/JSON blob cache for computed analytics (invalidated on re-run). |
| `community_submissions` | Legacy simple submission queue (handle + URL). |
| `denstream_state` | Serialised DenStream clustering snapshot (pickle blob). |
| `pipeline_runs` | Prefect task run history (flow_name, task_name, status, timing, error). |

**Migration-added columns (via migrate.py):**

`sources`: `is_quantum_tech`, `is_thailand_related`, `channel_id`, `channel_title`, `channel_country`, `channel_default_language`, `quantum_domain`, `media_format`, `user_intent`, `production_type`, `media_format_detail`, `nlp_confidence`, `is_approximate_geo`

`geo`: `is_approximate` (fallback lat/lng when precise geo unavailable)

**Supabase tables (supabase/migrations/):**

| Table | Purpose |
|-------|---------|
| `profiles` | User profiles (display_name, bio, role, avatar_url). Auto-created on first login. |
| `categories` | Operator-managed topic categories for submitted data. |
| `submitted_data` | User-owned submitted sources with status/analysis workflow. |
| `local_submitted_data` | SQLite mirror of submitted_data for local auth mode. |
| `local_users` / `local_sessions` | SQLite-backed auth tables for local auth mode. |

## Auth Modes

**Supabase mode** (default when env vars present):
- `SUPABASE_URL`, `SUPABASE_PUBLISHABLE_KEY`, `SUPABASE_SECRET_KEY` must be set.
- Browser receives only `SUPABASE_PUBLISHABLE_KEY`.
- Server uses `SUPABASE_SECRET_KEY` for service-role operations.
- Login: email/password + Google OAuth. Session persisted via Supabase cookie.
- Profiles auto-created on first authenticated request.

**Local auth mode** (fallback when Supabase not configured):
- Sessions stored in SQLite (`local_sessions` table).
- Email/password only. No OAuth.
- All submitted_data routes redirect to SQLite-backed equivalents.
- Toggle check: `_prefer_local_auth()` in `viewer/server.py`.

## Pipeline Stages

```
ingest        → sources table (GDELT, YouTube, RSS, seeds)
geo           → geo table (MaxMind GeoLite2; approximate fallback for no-IP sources)
filter        → is_quantum_tech / is_thailand_related flags (Claude classifier)
analyze nlp   → entities + triplets tables (Claude extraction per source)
analyze stats → stats_cache (bootstrap engagement, Mann-Kendall trend, DenStream)
analyze taxonomy-stats → stats_cache (yearly taxonomy breakdowns)
analyze graph-metrics  → stats_cache (betweenness, degree rank, component membership)
```

Each stage is resumable (skips already-processed rows). Run via CLI or Prefect flows.

## Configuration (env vars)

All prefixed `SIAMQUANTUM_` except Supabase vars (intentionally unprefixed):

```text
SIAMQUANTUM_ENV                    # development | production
SIAMQUANTUM_DATABASE_URL           # sqlite:///path/to/siamquantum_atlas.db
SIAMQUANTUM_ANTHROPIC_API_KEY
SIAMQUANTUM_CLAUDE_MODEL           # default: claude-sonnet-4-6
SIAMQUANTUM_YOUTUBE_API_KEY
SIAMQUANTUM_GDELT_BASE_URL         # default: https://api.gdeltproject.org/api/v2/doc/doc
SIAMQUANTUM_VIEWER_PORT            # default: 8765
SIAMQUANTUM_GOOGLE_CSE_KEY         # optional — CSE integration
SIAMQUANTUM_GOOGLE_CSE_CX_ACADEMIC
SIAMQUANTUM_GOOGLE_CSE_CX_MEDIA
SIAMQUANTUM_DEPLOYMENT_MODE        # local | vercel
SIAMQUANTUM_DATABASE_READ_ONLY     # true in Vercel demo
SIAMQUANTUM_RELEVANCE_RECHECK_DAYS        # default: 30
SIAMQUANTUM_RELEVANCE_AUDIT_BATCH_SIZE    # default: 40
CRON_SECRET                        # shared secret for /api/cron/* endpoints
SUPABASE_URL
SUPABASE_PUBLISHABLE_KEY
SUPABASE_SECRET_KEY
```

## Viewer Routes

**Page routes (Jinja2 templates):**

| Route | Template | Notes |
|-------|----------|-------|
| `/` | `index.html` | Landing page |
| `/dashboard` | `dashboard.html` | Map + source overview |
| `/network` | `network.html` | 3D force graph |
| `/analytics` | `analytics.html` | Engagement + taxonomy charts |
| `/database` | `database.html` | Filterable source cards + XLSX export |
| `/submit-data` | `submit_data.html` | Community submission form |
| `/profile` | `profile.html` | Auth + profile management |
| `/admin/submitted-data` | `admin_submitted_data.html` | Admin review queue |
| `/overview` | redirect | → `/dashboard` |
| `/community` | redirect | → `/submit-data` |

**JSON API endpoints (key):**

```
GET  /api/dashboard          source counts, geo breakdown, platform split
GET  /api/network/graph      nodes + links for 3D graph
GET  /api/network/nodes/{id} node detail (metrics, neighbors, relations, sources)
GET  /api/stats              yearly engagement stats
GET  /api/taxonomy           yearly taxonomy breakdown
GET  /api/sources            paginated + filtered source list
GET  /api/sources/export     XLSX download
GET  /api/categories         category list
POST /api/categories         create category (admin)
GET  /api/submitted-data     user's own submissions (auth required)
GET  /api/submitted-data/public  approved + completed submissions
POST /api/submitted-data     submit new source (auth required)
GET  /api/profile            current user profile
PUT  /api/profile            update profile
GET  /api/supabase-config    publishable key for browser
POST /api/cron/ingest        scheduled ingest trigger (CRON_SECRET required)
POST /api/admin/recheck-relevance  trigger low-confidence recheck (admin)
```

## Source Adapter System

`src/siamquantum/adapters/__init__.py` defines a `SourceAdapter` protocol and `_AdapterRegistry`. Adding a new data source requires only creating one file implementing `SourceAdapter`, calling `adapter_registry.register(MyAdapter())`. No changes to pipeline or server code.

## Statistics Methods

| Method | Usage |
|--------|-------|
| Bootstrap geo-mean | Engagement trend estimates (replaces Welch t-test; CV too high) |
| Mann-Kendall | Monotonic trend significance test on yearly time series |
| DenStream | Online micro-cluster algorithm for concept grouping |
| TF-IDF | Term relevance scoring for source text |

## Vercel Deployment

- `vercel.json`: routes all `/*` to `api/index.py`, Python 3.11, cron at `0 1 * * *`.
- `api/index.py`: copies bundled DB to `/tmp` (writable), sets `DEPLOYMENT_MODE=vercel`.
- `api/requirements.txt`: serve-time dependencies only (no pipeline/NLP packages).
- Read-only mode: SQLite opened with `?mode=ro`. No writes accepted in demo.
- Required Vercel env vars: `SIAMQUANTUM_DEPLOYMENT_MODE=vercel_demo`, `SIAMQUANTUM_DATABASE_READ_ONLY=true`.

## External API Constraints

- GDELT: no auth required; rate-limit conservatively (1 req/s max recommended).
- YouTube Data API v3: quota 10,000 units/day. Each `search.list` = 100 units.
- Anthropic Claude: pay-per-token. NLP extraction runs per source — batch carefully.
- MaxMind GeoLite2: free tier requires registration and license key; `.mmdb` files not committed.
- Google CSE: optional, not yet in active use (deferred).
