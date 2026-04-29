# SiamQuantum Atlas: Current Architecture Notes

## Overview

SiamQuantum Atlas is a Python-first research platform for collecting, enriching, analyzing, and presenting Thai-relevant quantum discourse. The current app keeps the existing FastAPI + Jinja + SQLite architecture and adds Supabase for user accounts, profile storage, categories, and user-owned submitted data.

Current stack:

- FastAPI for page routes and JSON APIs
- Jinja2 templates for the viewer UI
- SQLite for corpus, analytics cache, and local pipeline state
- Supabase Auth + PostgREST for accounts, profiles, categories, and submitted data
- plain JavaScript with CDN-delivered browser libraries
- Typer for ingestion, analysis, and local serve commands

## Application Layers

We currently structure the repo into four practical layers:

1. ingestion and analysis workflows
2. SQLite persistence and local analytics state
3. viewer/API delivery
4. deployment and auth integrations

This keeps the research pipeline and the user-facing viewer in one codebase without introducing a second frontend stack.

## Runtime Entry Points

Primary viewer runtime:

- `src/siamquantum/viewer/server.py`

That module now owns:

- page routes
- JSON API routes
- Supabase-backed auth/profile/category/submitted-data APIs
- local SQLite-backed analytics and corpus APIs
- template rendering
- demo/read-only gating

CLI entrypoint:

- `src/siamquantum/cli.py`

## Pages

Current page model:

- `/` landing page
- `/dashboard` main in-app entry page
- `/network`
- `/analytics`
- `/database`
- `/submit-data`
- `/profile`
- `/admin/submitted-data`

Compatibility redirects:

- `/overview` -> `/dashboard`
- `/community` -> `/submit-data`

## UI Structure

Templates live in:

- `src/siamquantum/viewer/templates/`

Shared shell:

- `base.html`

The base shell currently owns:

- sidebar and topbar layout
- language toggle
- shared live-data panel
- auth-aware profile link and avatar state
- global ambient visual background

Important page templates:

- `landing.html` for the public landing page
- `dashboard.html` for the primary research dashboard
- `community.html` for the Submit Data workflow
- `profile.html` for login, signup, and editable user profile
- `admin_submitted_data.html` for admin review

## Frontend Behavior

We still avoid a frontend build pipeline. Browser code is plain JavaScript loaded from templates or static JS files.

New shared auth files:

- `src/siamquantum/viewer/static/js/supabase-client.js`
- `src/siamquantum/viewer/static/js/auth.js`

Behavior:

- Supabase publishable key is exposed to browser code
- Supabase secret key is server-only
- browser auth uses Supabase Auth
- protected data operations go through FastAPI endpoints with bearer-token forwarding

## Data Model

### Local SQLite

SQLite remains the local source of truth for:

- corpus rows
- geo rows
- entities
- triplets
- stats cache
- local queue and pipeline state

Key SQLite tables still include:

- `sources`
- `geo`
- `entities`
- `triplets`
- `stats_cache`
- `community_submissions`
- `denstream_state`

These support the corpus analytics and existing local pipeline behavior.

### Supabase

Supabase now owns user-facing account and submission data:

- `profiles`
- `categories`
- `submitted_data`

Migration:

- `supabase/migrations/20260429_auth_profiles_submitted_data.sql`

Security model:

- Row Level Security enabled on all three tables
- users can only read or write their own profile rows
- users can only manage their own pending submitted data
- public reads only see approved and completed submitted data
- admin actions depend on `profiles.role = 'admin'`

## Authentication Model

Supabase env vars are read in:

- `src/siamquantum/config.py`

Required env vars:

- `SUPABASE_URL`
- `SUPABASE_PUBLISHABLE_KEY`
- `SUPABASE_SECRET_KEY`

Important rules:

- publishable key may be used in client code
- secret key must only be used server-side
- `.env`, `.env.local`, and `.env.*.local` are gitignored

Browser auth flow:

- email/password login
- email/password signup
- Google OAuth login
- logout
- persistent Supabase session

Profile bootstrap behavior:

- on first authenticated load, the app syncs the logged-in Supabase user into `profiles`
- initial profile fields use Supabase Auth metadata where available
- Google avatar URL is used as the default profile image when present

## Submitted Data Flow

User submissions now flow through Supabase instead of the old public unauthenticated link form.

Current flow:

1. user signs in
2. user visits `/submit-data`
3. categories load from Supabase
4. user can choose an existing category or create a new one
5. submitted row is written to `submitted_data`
6. new rows start with:
   - `status = 'pending'`
   - `analysis_status = 'queued'`
7. background follow-up can update:
   - `queued`
   - `processing`
   - `completed`
   - `failed`
8. public pages only read rows where:
   - `status = 'approved'`
   - `analysis_status = 'completed'`

The existing local `community_submissions` queue is still present for backward compatibility and local workflow bridging, but the active UI now points at Supabase-backed submitted data.

## Admin Review

Admin review page:

- `/admin/submitted-data`

Admin APIs:

- `GET /api/admin/submitted-data`
- `PATCH /api/admin/submitted-data/{id}`

Current capabilities:

- view all submitted rows
- filter by status
- filter by category
- approve or reject
- mark analysis status
- inspect saved `analysis_result`

## Analytics and Existing Viewer APIs

The main analytics and corpus views remain SQLite-backed.

Important existing APIs still include:

- `/api/stats/summary`
- `/api/pipeline/live`
- `/api/geo/list`
- `/api/graph`
- `/api/graph/metrics`
- `/api/sources`
- `/api/taxonomy/summary`
- `/api/stats/yearly`
- `/api/taxonomy/stats`

The shared live panel in `base.html` remains route-aware and reads the correct API set for each page.

## Deployment Notes

### Local

Standard local run:

```bash
python -m pip install -e .[dev]
python -m siamquantum serve
```

Default viewer URL:

```text
http://127.0.0.1:8765/dashboard
```

### Supabase

Manual external setup is still required:

1. run the SQL migration in the Supabase project
2. enable Google under `Authentication -> Providers`
3. configure OAuth client ID and secret
4. add redirect URLs for local and production use
5. create an admin profile row or promote an existing profile row with `role = 'admin'`

### Vercel Demo

The repo still supports a read-only demo deployment pattern, but write-enabled user flows now require both:

- a stateful runtime for local SQLite behavior where applicable
- Supabase connectivity for auth/profile/submitted-data features

## Current Constraints

We should keep these constraints explicit:

- SQLite is still not a concurrent multi-writer production database
- some NLP and enrichment work remains best-effort
- some analysis outputs are cache-driven rather than real-time
- graph outputs are interpretive, not ontological truth
- demo environments should not expose write flows unless the backend and Supabase integration are actually configured for them

## Current Direction

The current direction is still incremental refinement, not a stack rewrite. We keep:

- FastAPI
- Jinja2 templates
- the local research pipeline
- SQLite-backed analytics behavior

And we layer in:

- Supabase Auth
- editable user profiles
- authenticated submitted data
- admin review and approval workflow
