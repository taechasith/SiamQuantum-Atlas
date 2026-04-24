# SiamQuantum Atlas: Developer Architecture Notes

## Overview

We maintain SiamQuantum Atlas as a Python-first research platform for collecting, enriching, analyzing, and presenting Thai-relevant quantum discourse. We keep the implementation grounded in the existing stack instead of treating the repo like a speculative front-end rewrite target.

At the moment, we use:

- FastAPI for the application and API layer
- Jinja2 templates for the viewer pages
- SQLite as the default local and demo data store
- plain JavaScript plus CDN-delivered libraries for interactive UI behavior
- Typer for the CLI and operator workflow

The current product direction is a more polished research-platform UI, but we are still intentionally building on the same backend, template, and database architecture.

## How We Structure The System

We divide the repo into four practical layers:

1. ingestion and analysis workflows
2. persistence and schema management
3. viewer/API delivery
4. deployment shims and environment-specific behavior

This lets us keep the local research workflow, the public/demo viewer, and the analysis pipeline aligned without introducing a second application stack.

## Runtime Stack

### Backend

We use FastAPI as the runtime web server and API surface. The main viewer app lives in:

- `src/siamquantum/viewer/server.py`

That module handles:

- HTML page routes
- JSON API routes
- response envelopes and error handling
- demo/read-only gating
- template rendering
- viewer-specific aggregation queries

### Templates

We render the UI with Jinja2 templates stored in:

- `src/siamquantum/viewer/templates/`

The current viewer follows a shared shell pattern:

- `base.html` defines the global design system, navigation, live page panel, ambient visual layer, bilingual toggle behavior, and floating cat guide interaction
- page templates extend `base.html` and add page-specific layout, CSS, and JavaScript

This means we do not need React, Vue, or Tailwind to ship the current UI. We keep interaction logic close to the page that owns it and reuse the shared shell where it improves consistency.

### Frontend Libraries

We intentionally keep frontend dependencies narrow and page-specific:

- Leaflet and MarkerCluster on `/dashboard`
- `3d-force-graph` and Three.js on `/network`
- Chart.js on `/analytics`
- plain JavaScript for filtering, polling, drawers, forms, and bilingual UI state

We currently load these libraries from CDNs inside the template files. That keeps the app simple to run in local research environments and avoids adding a separate frontend build step.

### CLI

We use Typer-based CLI commands to run ingestion and analysis workflows. The entrypoints live under:

- `src/siamquantum/cli.py`

We use the CLI for:

- seed ingestion
- RSS / GDELT / YouTube ingestion paths
- NLP analysis
- taxonomy and statistical analysis
- graph metric refresh
- serving the viewer locally

## Data Layer

### Primary Database

We use SQLite as the source of truth for local development, local research use, and read-only demo deployment. The configured database URL is resolved through the project settings layer and typically points at the processed project database.

SQLite is the right tradeoff for the current phase because:

- the platform is primarily operator-driven
- the analysis workflow is batch-oriented
- the demo deployment is read-heavy
- the repo benefits from minimal infrastructure overhead

SQLite is not positioned here as a multi-writer production database.

### Core Tables

We currently rely on these key tables:

- `sources`
- `geo`
- `entities`
- `triplets`
- `stats_cache`
- `community_submissions`
- `nlp_abstentions`
- `denstream_state`

In practice:

- `sources` stores the base corpus rows
- `geo` stores geolocation or hosting-origin rows
- `entities` stores classification and taxonomy-level enrichment
- `triplets` stores extracted relation triples used by the graph view
- `stats_cache` stores expensive or reusable analysis outputs
- `community_submissions` stores the local intake queue
- `nlp_abstentions` tracks skipped or unresolved NLP cases
- `denstream_state` stores streaming or clustering state where applicable

### Schema And DB Utilities

We keep schema and connection utilities in:

- `src/siamquantum/db/`

That area owns:

- initialization
- migrations
- connection helpers
- DB-facing repository logic used by the viewer and pipeline

## Analysis Layer

We treat analysis as a separate concern from viewer delivery. The viewer reads from analysis outputs, but the viewer is not responsible for recomputing the entire research pipeline on every request.

### NLP And Extraction

We use the pipeline modules under:

- `src/siamquantum/pipeline/`

These modules handle:

- source text preparation
- entity extraction
- triplet extraction
- abstention tracking
- downstream enrichment steps

### Statistical Outputs

We use the stats modules under:

- `src/siamquantum/stats/`

The current approach is intentionally conservative. We favor:

- log-oriented handling of heavy-tailed engagement metrics
- bootstrap-based geometric summaries where appropriate
- nonparametric comparison methods
- taxonomy-aware subgroup interpretation

We explicitly avoid overstating raw-view comparisons as if they were automatically trustworthy scientific conclusions.

### Graph Analytics

The network experience depends on:

- extracted triplets from the corpus
- graph assembly for `/api/graph`
- cached graph metrics for `/api/graph/metrics`
- per-node detail interpretation for `/api/graph/node`

We use these outputs to support:

- connected component summaries
- top degree and betweenness rankings
- hub interpretation
- community summaries
- node-level contextual explanation

## Viewer And Route Architecture

### Pages

We currently serve six primary pages:

- `/`
- `/dashboard`
- `/network`
- `/analytics`
- `/database`
- `/community`

### Home

We use `/` as the landing page for the research platform. It combines:

- project framing
- live corpus overview
- real pipeline status
- entry points into each major workflow page

The home page reads real data from:

- `/api/stats/summary`
- `/api/pipeline/live`

### Shared Live Page Panel

We now use a shared live-data panel in `base.html` across the viewer. That panel is route-aware and reads different real APIs depending on the current page.

Examples:

- `/dashboard` reads from `/api/geo/list` and `/api/pipeline/live`
- `/network` reads from `/api/graph` and `/api/graph/metrics`
- `/analytics` reads from `/api/stats/yearly` and `/api/taxonomy/stats`
- `/database` reads from `/api/sources` and `/api/taxonomy/summary`
- `/community` reads from `/api/community/submissions`

We also let the user collapse and reopen this panel. The state is stored in `localStorage`, and the floating cat button can reopen it when hidden.

### Dashboard

The map page remains Leaflet-based and uses real geo rows from `/api/geo/list`. We bias the UI toward origin and provenance interpretation instead of presenting the map like a popularity surface.

### Network

The network page remains based on `3d-force-graph`. We have upgraded the UX without changing the stack:

- lazy graph launch
- clearer graph framing
- detail drawer with richer explanation
- neighbor jump-through interactions
- graph metrics panel
- shared-shell live panel above the page

We describe the page as a 3D concept network, not as a “Three.js-style” demo.

### Analytics

The analytics page continues to render data from:

- `/api/stats/yearly`
- `/api/taxonomy/stats`

We keep the scientific wording cautious and preserve the meaning of the statistical outputs while improving readability and hierarchy.

### Database

The database page remains a filtered browser over `/api/sources`, with taxonomy metadata loaded from `/api/taxonomy/summary`. We currently support:

- year filtering
- platform filtering
- content-type filtering
- taxonomy filters
- XLSX export

### Community

The community page is a real local intake queue, not a mocked form. It writes to `community_submissions` in local mode and exposes queue state through:

- `POST /api/community/submit`
- `GET /api/community/submissions`

In demo mode we clearly disable writes.

## UI System Notes

We are deliberately evolving the viewer into a more polished research product without replacing the architecture.

Current UI characteristics:

- shared premium dark shell
- Thai-friendly unified typography
- ambient lightweight Three.js background particles
- shared live data panel
- floating cat guide interaction
- bilingual Thai/English toggle
- page-specific interaction layers built with plain JavaScript

We currently preserve the server-rendered structure and keep enhancement incremental.

## Settings And Environment Behavior

The app behavior depends on runtime settings, especially for:

- database URL resolution
- deployment mode
- read-only demo mode

In particular, demo-safe behavior is important for Vercel and similar environments where SQLite should not be treated as a writable production store.

## Deployment Notes

### Local

We normally run the app locally with editable install plus the built-in serve command:

```bash
python -m pip install -e .[dev]
python -m siamquantum serve
```

### Vercel Demo

We keep Vercel support constrained to a read-only demo model.

Current deployment pieces:

- `api/index.py` as the Vercel entry shim
- `vercel.json` routing all requests to that entrypoint
- a committed demo database bundle
- read-only or demo-mode guards for write-sensitive features

Important constraints:

- Vercel is not the place for durable SQLite writes
- community submission must be disabled or clearly gated in demo mode
- ingestion and analysis should run in a stateful environment, not in the Vercel request lifecycle

## How We Think About This Repo

We treat this repository as the working research-platform baseline, not as a throwaway prototype.

That means:

- we preserve the current stack
- we improve UX incrementally rather than rewriting the app
- we keep scientific wording accurate
- we keep route and data behavior honest
- we document current reality rather than aspirational features

## Current Limitations

We should keep these constraints explicit:

- SQLite does not give us high-concurrency write behavior
- some NLP and enrichment steps remain best-effort
- relevance semantics are still corpus-operational defaults in some areas
- graph structure is interpretive, not ontological truth
- demo deployment is read-only by design

## Next Development Direction

Our next stage is not a stack rewrite. Our next stage is continued UI refinement based on real usage and user feedback while preserving:

- the FastAPI backend
- the Jinja template structure
- the current route model
- the existing research and analytics pipeline
