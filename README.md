# SiamQuantum Atlas

SiamQuantum Atlas is the current web based research-tool version of a Thai quantum discourse mapping system. It ingests a mixed corpus of Thai-relevant quantum sources, stores them in SQLite, enriches them with NLP-derived entities and triplets, and serves a working analyst UI through FastAPI + Jinja2.

This repository is not a speculative product shell. It is the live baseline used for corpus review, graph inspection, taxonomy-aware analytics, and operator-led source expansion. A richer next-version UI is in development and will be refined from user feedback, but this repo represents the honest current state.

## Current stack

- Python 3.10+
- FastAPI
- SQLite
- Jinja2 templates with CDN JavaScript
- 3d-force-graph on the network page
- Local CLI orchestration with Typer

## What the app does now

- Ingests source records from GDELT, YouTube, RSS, curated seeds, and selected source-specific paths.
- Stores source, geo, entity, triplet, cache, and community-submission data in SQLite.
- Runs NLP extraction and classification in a resumable pipeline.
- Builds taxonomy-aware statistics and graph metrics caches.
- Serves the current viewer pages:
  - `/`: landing page
  - `/dashboard`: geographic and source-overview view
  - `/network`: 3D concept network with click-through node research detail
  - `/analytics`: engagement and taxonomy analysis
  - `/database`: filtered source browser
  - `/submit-data`: authenticated submission and review queue entrypoint
  - `/profile`: login, signup, and editable user profile
  - `/admin/submitted-data`: admin review queue

Compatibility redirects:

- `/overview` -> `/dashboard`
- `/community` -> `/submit-data`

## Local run

Install and run:

```bash
python -m pip install -e .[dev]
python -m siamquantum serve
```

Default viewer URL:

```text
http://127.0.0.1:8765/dashboard
```

Run on another port:

```bash
python -m siamquantum serve --port 9000
```

## Supabase auth setup

The app can use Supabase Auth for browser login, profile management, categories, and user-owned submitted data.

Required local environment variables:

```text
SUPABASE_URL=...
SUPABASE_PUBLISHABLE_KEY=...
SUPABASE_SECRET_KEY=...
```

Notes:

- `SUPABASE_PUBLISHABLE_KEY` is exposed to browser code by design.
- `SUPABASE_SECRET_KEY` is server-only and must never be placed in templates or client-side JavaScript.
- `.env`, `.env.local`, and `.env.*.local` are gitignored.

SQL migration:

- `supabase/migrations/20260429_auth_profiles_submitted_data.sql`

Manual Supabase dashboard steps:

1. Open the Supabase dashboard for the project.
2. Run the SQL migration above in the SQL editor or your migration workflow.
3. Go to `Authentication -> Providers -> Google` and enable Google.
4. Add the Google OAuth client ID and client secret there.
5. Add local and production redirect URLs that land back on `/profile`.
6. In local development, make sure the app origin you use in the browser matches the redirect URL registered in Supabase.

## Operator workflow

Typical local workflow:

```bash
python -m siamquantum ingest seeds
python -m siamquantum ingest rss --feed all
python -m siamquantum ingest gdelt --year 2024
python -m siamquantum ingest youtube --year 2024
python -m siamquantum analyze nlp --year 2024
python -m siamquantum analyze stats
python -m siamquantum analyze taxonomy-stats
python -m siamquantum analyze graph-metrics
python -m siamquantum serve
```

Not every run needs the full sequence. The current system supports incremental operator-led updates.

## Current major features

- Operational corpus boundary flags with explicit scope wording in the API/UI.
- Taxonomy-aware engagement summaries using nonparametric or bootstrap-oriented methods.
- Concept graph metrics including connected components, hub interpretation, and community summaries.
- Clickable network node detail with neighbor, relation, source, and taxonomy context.
- User-owned submitted data with Supabase-backed auth, profile, and review flow.
- XLSX export for source/entity review.

## Auth and submitted data

Current auth and user-data behavior:

- Supabase Auth handles email/password login, signup, Google login, session persistence, and logout.
- The browser only receives `SUPABASE_PUBLISHABLE_KEY`.
- The FastAPI server is the only place that may use `SUPABASE_SECRET_KEY`.
- First authenticated load auto-creates a `profiles` row when needed.
- `/submit-data` writes authenticated user-owned rows into `submitted_data`.
- public visibility is limited to rows with:
  - `status = 'approved'`
  - `analysis_status = 'completed'`
- `/profile` shows the user profile plus their own private submitted data queue.
- `/admin/submitted-data` is intended for users whose `profiles.role = 'admin'`.

## Known limitations

- The relevance flags are currently operational corpus defaults unless explicit row-level checking has been run. They should not be read as universal classifier truth.
- NLP extraction is best-effort and depends on available source text plus configured model access.
- SQLite is the correct fit for local/demo operation, not for concurrent multi-writer production use.
- The network view is analytically useful but still an interpretation aid, not a causal or ontological truth layer.
- Community automation is intentionally partial. The queue is real; downstream processing remains environment-dependent.

## Vercel demo deployment

The repo ships a read-only Vercel configuration suitable for demo use.

**Entrypoint:** `api/index.py` — thin shim that resolves the repo root, sets absolute DB path and demo env vars, then imports the FastAPI `app` object.

**Routing:** `vercel.json` routes all requests (`/*`) to `api/index.py`.

**Python version:** `runtime.txt` pins Python 3.11.

**Dependencies:** `api/requirements.txt` lists only the packages required at serve time (no pipeline/NLP/stats packages). This keeps the Vercel function well under the 250 MB limit.

**Demo-safe behavior:**

- All read pages and APIs work normally (`/dashboard`, `/network`, `/analytics`, `/database`, `/submit-data`, all `/api/*` endpoints).
- Submitted data writes require a stateful runtime and Supabase connectivity; read-only demo environments should not expose write-enabled submit flows.
- SQLite is opened in read-only URI mode (`?mode=ro`) so no write lock is acquired.
- The bundled dataset (`data/processed/siamquantum_atlas.db`) is committed to the repo and served from `/var/task`.

**Required Vercel env vars** (set in the Vercel dashboard under Project → Settings → Environment Variables):

```text
SIAMQUANTUM_DEPLOYMENT_MODE=vercel_demo
SIAMQUANTUM_DATABASE_READ_ONLY=true
```

The `DATABASE_URL` is resolved automatically at runtime from the committed DB path. You only need to override it if you point to an external DB.

**Limitations:**

- No durable writes. Community submissions, ingestion, and NLP runs require a stateful environment.
- XLSX export works but generates the file in-memory per request (no caching).
- Graph node detail cache is rebuilt per cold-start if not pre-populated in the bundled DB.

If you need durable writes or scheduled ingestion, run the app locally or on a stateful server instead.

## Repo intent

This repository tracks the current research-platform baseline. The current version already has working analytics and viewer pages, while the UI is being pushed toward the next version through iterative design work. The next stage is gathering user feedback to improve UX/UI further without changing the underlying stack.
