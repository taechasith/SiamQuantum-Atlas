# SiamQuantum Atlas

SiamQuantum Atlas is the current web-based research tool for Thai quantum discourse mapping. It ingests a mixed corpus of Thai-relevant quantum sources, stores them in SQLite, enriches them with NLP-derived entities and triplets, and serves a working analyst UI through FastAPI + Jinja2.

This repository is not a speculative product shell. It is the live baseline used for corpus review, graph inspection, taxonomy-aware analytics, and operator-led source expansion.

## Current stack

- Python 3.11+
- FastAPI + uvicorn (default port 8765)
- SQLite (`data/processed/siamquantum_atlas.db`)
- GDELT API v2, YouTube Data API v3, Anthropic Claude API
- Jinja2 templates with CDN JavaScript (Leaflet.js, Chart.js, 3d-force-graph)
- Supabase Auth + PostgREST (optional — local auth mode available)
- Prefect 3 (optional — scheduled pipeline orchestration)
- Typer CLI

## What the app does

- Ingests source records from GDELT, YouTube, RSS, and curated seeds.
- Stores source, geo, entity, triplet, cache, and community-submission data in SQLite.
- Runs NLP extraction and classification in a resumable pipeline.
- Builds taxonomy-aware statistics and graph metrics caches.
- Serves the viewer pages:
  - `/`: landing page
  - `/dashboard`: geographic source map with mobile bottom-sheet detail panel
  - `/network`: 3D concept network with click-through node research detail
  - `/analytics`: engagement and taxonomy analysis
  - `/database`: filtered source browser with XLSX export
  - `/submit-data`: authenticated submission and review queue with grounded AI URL analysis
  - `/profile`: login, signup, and editable user profile
  - `/admin/submitted-data`: admin review queue

Compatibility redirects: `/overview` → `/dashboard`, `/community` → `/submit-data`

## Local run

```bash
python -m pip install -e .[dev]
python -m siamquantum serve
```

Default viewer URL: `http://127.0.0.1:8765/dashboard`

Run with auto-reload (development):

```bash
python -m siamquantum serve --reload
```

Run on another port:

```bash
python -m siamquantum serve --port 9000
```

## Supabase auth setup

The app supports Supabase Auth for browser login, profile management, categories, and user-owned submitted data. If Supabase env vars are absent, the app falls back to local auth mode (SQLite-backed sessions).

Required env vars for Supabase mode:

```text
SUPABASE_URL=...
SUPABASE_PUBLISHABLE_KEY=...
SUPABASE_SECRET_KEY=...
```

- `SUPABASE_PUBLISHABLE_KEY` is exposed to browser code by design.
- `SUPABASE_SECRET_KEY` is server-only — never put it in templates or client JS.
- `.env`, `.env.local`, and `.env.*.local` are gitignored.

SQL migration: `supabase/migrations/20260429_auth_profiles_submitted_data.sql`

Manual Supabase dashboard steps:

1. Run the SQL migration in the SQL editor.
2. Go to `Authentication → Providers → Google` and enable Google OAuth.
3. Add Google OAuth client ID and secret.
4. Add your local and production redirect URLs (must land back on `/profile`).

## Operator workflow

Typical local workflow:

```bash
python -m siamquantum ingest seeds
python -m siamquantum ingest rss --feed all
python -m siamquantum ingest gdelt --year 2024
python -m siamquantum ingest youtube --year 2024
python -m siamquantum ingest geo --pending
python -m siamquantum analyze nlp --year 2024
python -m siamquantum analyze stats
python -m siamquantum analyze taxonomy-stats
python -m siamquantum analyze graph-metrics
python -m siamquantum filter relevance
python -m siamquantum serve
```

Not every run needs the full sequence. The system supports incremental operator-led updates.

## Full CLI reference

```text
python -m siamquantum db init
python -m siamquantum db reset --confirm
python -m siamquantum db audit [--fix]

python -m siamquantum ingest today
python -m siamquantum ingest seeds
python -m siamquantum ingest rss --feed all
python -m siamquantum ingest gdelt --year YYYY [--all-years]
python -m siamquantum ingest youtube --year YYYY [--all-years]
python -m siamquantum ingest geo --pending

python -m siamquantum analyze nlp --year YYYY
python -m siamquantum analyze stats
python -m siamquantum analyze taxonomy-stats
python -m siamquantum analyze graph-metrics
python -m siamquantum analyze full

python -m siamquantum filter relevance
python -m siamquantum filter recheck-low-confidence

python -m siamquantum serve [--port 8765] [--reload]

python -m siamquantum orchestration refresh
python -m siamquantum orchestration healthcheck
python -m siamquantum orchestration serve
python -m siamquantum orchestration deploy
python -m siamquantum orchestration worker
```

## Scheduled orchestration (Prefect)

The app includes Prefect 3 flows for scheduled, retryable pipeline runs. See `docs/prefect.md` for setup and CLI reference.

Quick start (no Prefect server required):

```bash
pip install prefect>=3
python -m siamquantum orchestration serve
```

## Current major features

- Operational corpus boundary flags with explicit scope wording in the API/UI.
- Taxonomy-aware engagement summaries using bootstrap and nonparametric methods.
- Concept graph metrics including connected components, hub interpretation, and community summaries.
- Clickable network node detail with neighbor, relation, source, and taxonomy context.
- Geo enrichment with MaxMind GeoLite2 City + ASN; approximate location fallback for sources without precise coordinates.
- Source map mobile UI with a draggable details sheet that stays below the global mobile navigation chrome.
- User-owned submitted data with Supabase-backed auth, profile, review flow, and local-auth fallback.
- Grounded AI URL analysis for submitted data: title, description, category, tags, source evidence, confidence, data quality, and review notes are generated only from fetched page context.
- Low-confidence relevance recheck to improve corpus quality iteratively.
- XLSX export for source/entity review.
- Source Adapter system (`src/siamquantum/adapters/`) for adding new data sources without touching pipeline code.

## Auth and submitted data

- Supabase Auth handles email/password login, signup, Google login, session persistence, and logout.
- The browser only receives `SUPABASE_PUBLISHABLE_KEY`.
- The FastAPI server is the only place that may use `SUPABASE_SECRET_KEY`.
- First authenticated load auto-creates a `profiles` row when needed.
- `/submit-data` writes authenticated user-owned rows into `submitted_data`.
- The Analyze with AI flow fetches source context first, then asks Claude for grounded metadata.
- AI-filled submission metadata stores the full analysis payload under `metadata.ai_analysis`, including `evidence_quotes`, `analysis_notes`, `data_quality`, `confidence`, and `needs_review`.
- Invalid or invented model categories are normalized to the approved category list before reaching the UI.
- Public visibility is limited to rows with `status = 'approved'` and `analysis_status = 'completed'`.
- `/profile` shows the user profile plus their own private submitted data queue.
- `/admin/submitted-data` is for users whose `profiles.role = 'admin'`.

## Known limitations

- Relevance flags are operational corpus defaults unless explicit row-level rechecking has been run; do not read as universal classifier truth.
- NLP extraction is best-effort and depends on available source text plus configured model access.
- Submit-data AI analysis is grounded and normalized, but reviewers should still verify low-confidence or sparse-source submissions.
- SQLite is correct for local/demo operation, not for concurrent multi-writer production use.
- The network view is an interpretation aid, not a causal or ontological truth layer.
- Community automation is intentionally partial — the queue is real, downstream processing remains environment-dependent.

## Vercel demo deployment

The repo ships a read-only Vercel configuration suitable for demo use.

**Entrypoint:** `api/index.py` — resolves the repo root, sets absolute DB path and demo env vars, then imports the FastAPI `app`.

**Routing:** `vercel.json` routes all requests (`/*`) to `api/index.py`.

**Python version:** Specified as `3.11` in `vercel.json` build config.

**Dependencies:** `api/requirements.txt` lists only packages required at serve time (no pipeline/NLP/stats packages), keeping the Vercel function well under the 250 MB limit.

**Required Vercel env vars** (set in Project → Settings → Environment Variables):

```text
SIAMQUANTUM_DEPLOYMENT_MODE=vercel_demo
SIAMQUANTUM_DATABASE_READ_ONLY=true
```

**Limitations in demo mode:**

- No durable writes. Community submissions, ingestion, and NLP runs require a stateful environment.
- XLSX export works but generates the file in-memory per request (no caching).
- Graph node detail cache is rebuilt per cold-start if not pre-populated in the bundled DB.

For durable writes or scheduled ingestion, run locally or on a stateful server.

## Repo intent

This repository tracks the current research-platform baseline. The current version has working analytics and viewer pages; UI is being iterated based on user feedback without changing the underlying stack.
