# SiamQuantum Atlas: current architecture and research-tool spec

## Status

This document describes the current research-tool version of SiamQuantum Atlas. It does not describe a speculative future stack. The live baseline in this repository is a Python + FastAPI + SQLite + Jinja2/CDN JavaScript system with a working analyst UI.

A richer next-version UI is being developed, but it does not replace the current implementation yet. That future UI should be refined through real user feedback from this working research-tool baseline.

## System purpose

SiamQuantum Atlas tracks and interprets Thai-relevant quantum discourse across a mixed source corpus. The system is built to support:

- source collection and expansion
- corpus review
- NLP extraction of entities and triplets
- discourse/network inspection
- taxonomy-aware engagement analysis
- local operator workflows for curation and community intake

## Actual architecture

### Application layer

- FastAPI application in `src/siamquantum/viewer/server.py`
- Jinja2 templates under `src/siamquantum/viewer/templates/`
- CDN JavaScript for maps, charts, and the 3D network scene
- Typer CLI in `src/siamquantum/cli.py`

### Data layer

- SQLite database at the configured `SIAMQUANTUM_DATABASE_URL`
- schema and migrations in `src/siamquantum/db/`
- repo classes for sources, geo rows, entities, triplets, cache entries, and community submissions

### Analysis layer

- NLP pipeline for entity/triplet extraction and abstention handling
- bootstrap/nonparametric engagement analysis
- taxonomy-aware summaries
- graph metrics using NetworkX

## Current corpus and source coverage

The current corpus is assembled from a practical mix of:

- GDELT pulls
- YouTube ingest
- RSS ingest
- curated seed lists
- selected manual or source-specific expansion paths

The corpus boundary is operationally Thai-quantum focused. Current relevance flags should be interpreted as corpus-scope operational defaults unless explicit row-level checking has been run.

## Current database objects

Core persisted tables include:

- `sources`
- `geo`
- `entities`
- `triplets`
- `stats_cache`
- `community_submissions`
- `nlp_abstentions`
- `denstream_state`

The app uses SQLite as the source of truth for the current local/demo workflow.

## Current pages

### `/dashboard`

Dashboard for source geography and coverage framing. It is intended for quick orientation, not deep record review.

### `/network`

Three.js-style 3D concept network using `3d-force-graph`. The page now supports:

- graph-level framing and help text
- leaf-node suppression for readability
- smoother camera focus
- graph metrics panel
- click-through node detail

The node detail panel is lightweight but useful. It exposes:

- concept label
- hub role and centrality context
- component size and rank
- nearby concepts
- dominant relation labels
- supporting source examples
- derived taxonomy/domain context

### `/analytics`

Analytics view for yearly and taxonomy-aware engagement interpretation. The current system favors valid transforms and nonparametric/bootstrap methods over naive raw-view significance claims.

### `/database`

Source browser for filtered inspection across platform, content type, taxonomy fields, and corpus-scope metadata.

### `/community`

Local submission queue workflow. The page persists submissions to SQLite and surfaces recent queue state. It is intentionally honest about automation limits.

## Current statistical approach

The current statistical posture is deliberately conservative:

- log-oriented handling of heavy-tailed engagement values
- bootstrap geometric summaries where appropriate
- nonparametric group comparison methods
- taxonomy-aware subgroup inspection
- cached graph and analytics outputs for viewer performance

The system explicitly avoids presenting raw-view parametric tests as a trustworthy default.

## Current graph and taxonomy analytics

Implemented analytics now include:

- connected component summaries
- largest-component community summaries
- degree-hub and broker-hub interpretation
- top degree and betweenness rankings
- taxonomy summary distributions
- strongest engagement cells across format-intent combinations
- subgroup trend framing where supported by the cached analysis layer

These are interpretive aids over the observed corpus, not claims about the full Thai quantum public sphere.

## Current UI state

The UI is a working research interface, not a full productized front-end platform. It has been upgraded for:

- stronger hierarchy
- cleaner scanability
- better network understandability
- clearer database filtering
- more honest workflow feedback

This is still the current research-tool version. The next-version UI under development is intended to go further on interaction design and polish, then be refined from user feedback rather than imposed as an architecture rewrite.

## Known limitations

- SQLite limits multi-user write scalability.
- Some source enrichment remains best-effort and environment-dependent.
- Relevance semantics are operationally explicit but not universally classifier-verified per row.
- Graph structure reflects extracted discourse relations, not formal knowledge-graph truth.
- Community workflow is real locally, but not appropriate to present as fully automated.
- Vercel is suitable only for a read-only or demo-safe deployment mode unless the storage model changes.

## Deployment reality

The current app can be prepared for Vercel only as a constrained demo deployment:

- FastAPI remains the backend entrypoint.
- SQLite should be treated as read-only in deployment mode.
- Write paths must be disabled or clearly gated.
- Durable ingest, queue processing, and corpus updates should remain in a stateful runtime.

That constraint is architectural reality, not a bug to hide.

## Recommended interpretation

Treat SiamQuantum Atlas as a practical discourse research instrument:

- strong enough for demos, review, and iterative research use
- honest about the limits of the current corpus and infrastructure
- ready for continued UI refinement and operator feedback loops
- not a full production data platform yet
