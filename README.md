# SiamQuantum Atlas

SiamQuantum Atlas is the current research-tool version of a Thai quantum discourse mapping system. It ingests a mixed corpus of Thai-relevant quantum sources, stores them in SQLite, enriches them with NLP-derived entities and triplets, and serves a working analyst UI through FastAPI + Jinja2.

This repository is not a speculative product shell. It is the live baseline used for corpus review, graph inspection, taxonomy-aware analytics, and operator-led source expansion. A richer next-version UI is in development and will be refined from user feedback, but this repo represents the honest current state.

## Current stack

- Python 3.10+
- FastAPI
- SQLite
- Jinja2 templates with CDN JavaScript
- 3d-force-graph / Three.js on the network page
- Local CLI orchestration with Typer

## What the app does now

- Ingests source records from GDELT, YouTube, RSS, curated seeds, and selected source-specific paths.
- Stores source, geo, entity, triplet, cache, and community-submission data in SQLite.
- Runs NLP extraction and classification in a resumable pipeline.
- Builds taxonomy-aware statistics and graph metrics caches.
- Serves five working pages:
  - `/dashboard`: geographic and source-overview view
  - `/network`: 3D concept network with click-through node research detail
  - `/analytics`: engagement and taxonomy analysis
  - `/database`: filtered source browser
  - `/community`: local submission queue workflow

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
- Community submission persistence for local workflow use.
- XLSX export for source/entity review.

## Known limitations

- The relevance flags are currently operational corpus defaults unless explicit row-level checking has been run. They should not be read as universal classifier truth.
- NLP extraction is best-effort and depends on available source text plus configured model access.
- SQLite is the correct fit for local/demo operation, not for concurrent multi-writer production use.
- The network view is analytically useful but still an interpretation aid, not a causal or ontological truth layer.
- Community automation is intentionally partial. The queue is real; downstream processing remains environment-dependent.

## Deployment reality

The current architecture stays Python + FastAPI + SQLite + Jinja2/CDN JS.

This app can be prepared for Vercel as a read-only demo deployment, but SQLite write workflows are not durable there because the filesystem is ephemeral. In deploy/demo mode:

- read APIs and pages remain available
- the bundled SQLite dataset should be treated as read-only
- write-sensitive features such as community submission should be explicitly disabled or gated

Recommended deploy env for the current Vercel demo shape:

```text
SIAMQUANTUM_DEPLOYMENT_MODE=vercel_demo
SIAMQUANTUM_DATABASE_READ_ONLY=true
SIAMQUANTUM_DATABASE_URL=sqlite:///data/processed/siamquantum_atlas.db
```

If you need durable writes, scheduled ingestion, or automated moderation, keep running the app in a stateful environment rather than assuming Vercel solves that.

## Repo intent

This repository tracks the current research-tool baseline. The next-version UI is being developed separately as a richer interaction layer, and its refinements should be driven by real user feedback from this working system rather than by speculative front-end rewrite churn.
