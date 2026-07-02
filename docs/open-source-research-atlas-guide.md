# Open Source Guide: Build a Research Atlas Platform for Any Topic

This guide explains how to build a platform like SiamQuantum Atlas, but for any database-driven topic: biodiversity records, public policy, local history, scientific literature, media archives, climate events, cultural heritage, patents, companies, legislation, supply chains, or any other corpus that benefits from search, maps, graphs, analytics, user submissions, and repeatable data pipelines.

It is written for two audiences:

- Humans who want a step-by-step implementation plan.
- CLI AI coding agents that need precise instructions, file boundaries, algorithms, and validation checks.

The core idea is simple: collect records from many sources, normalize them into one schema, enrich them with metadata and NLP, compute statistics and graph structure, cache expensive results, and serve analyst-friendly pages through a web app.

## 1. What You Are Building

Build a "Topic Atlas": a web platform that continuously collects topic-relevant sources, classifies them, extracts relationships, and exposes the result through interactive views.

Recommended first version:

- Landing page with a live corpus summary.
- Dashboard map or location view.
- Network graph of concepts, people, institutions, places, or terms.
- Analytics page with trends, category comparisons, and statistical notes.
- Database browser with filters and export.
- Submission page where users can suggest new records.
- Profile page for user-owned submissions.
- Admin review queue.
- CLI pipeline for ingestion, enrichment, analysis, and serving.
- Optional scheduled orchestration.

The platform should be topic-neutral. Do not hard-code "quantum" or any single domain. Use configuration such as:

```text
PROJECT_NAME=OpenAtlas
TOPIC_NAME=Biodiversity
TOPIC_ADJECTIVE=biodiversity-related
PRIMARY_SCOPE=Thailand biodiversity observations
DATABASE_URL=sqlite:///data/processed/openatlas.db
```

## 2. Architecture Overview

Use this shape:

```text
External sources
  -> source adapters
  -> raw normalized records
  -> database insert and dedupe
  -> relevance classifier
  -> metadata/entity classifier
  -> relationship extraction
  -> geo enrichment
  -> statistics and graph metrics
  -> cached JSON payloads
  -> FastAPI/Jinja web UI and JSON API
  -> user submissions and admin review
```

Recommended stack:

- Python 3.11 or newer.
- FastAPI for API and server-rendered pages.
- Jinja2 templates for a simple, deployable UI.
- SQLite for local and demo use.
- PostgreSQL or Supabase for production multi-user writes.
- Typer for CLI commands.
- httpx for external APIs.
- Pydantic for structured records.
- NetworkX for graph metrics.
- NumPy/SciPy/scikit-learn for analytics.
- openpyxl or pandas for XLSX export.
- Prefect, cron, GitHub Actions, or a background worker for scheduled runs.
- Optional LLM provider for grounded classification and extraction.

This stack is deliberately boring. The hard part is data quality, not framework complexity.

## 3. Repository Layout

Create this structure:

```text
your-atlas/
  README.md
  pyproject.toml
  .env.example
  data/
    processed/
  docs/
  scripts/
  src/
    openatlas/
      __init__.py
      __main__.py
      cli.py
      config.py
      models.py
      adapters/
        __init__.py
      db/
        __init__.py
        schema.sql
        migrations/
        migrate.py
        repos.py
        session.py
      pipeline/
        ingest.py
        filter.py
        nlp.py
        graph_metrics.py
        taxonomy_stats.py
        algo_registry.py
      services/
        dedup.py
        geoip.py
        llm.py
        rss.py
        web_fetch.py
      stats/
        engagement_bootstrap.py
        nonparametric.py
      viewer/
        server.py
        templates/
        static/
  tests/
```

For a CLI AI agent: create the skeleton first, then implement one vertical slice end to end before adding more sources.

## 4. Minimum Data Model

Use a relational core. Keep raw source fields separate from derived fields.

```sql
CREATE TABLE IF NOT EXISTS sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    platform TEXT NOT NULL,
    external_id TEXT,
    url TEXT UNIQUE NOT NULL,
    title TEXT,
    raw_text TEXT,
    published_at TEXT,
    published_year INTEGER,
    fetched_at TEXT NOT NULL,
    view_count INTEGER,
    like_count INTEGER,
    comment_count INTEGER,
    source_meta TEXT DEFAULT '{}',

    is_topic_relevant INTEGER,
    is_scope_relevant INTEGER,
    topic_domain TEXT,
    rejection_reason TEXT,
    relevance_confidence REAL,
    relevance_checked_at TEXT
);

CREATE TABLE IF NOT EXISTS geo (
    source_id INTEGER PRIMARY KEY REFERENCES sources(id) ON DELETE CASCADE,
    lat REAL,
    lng REAL,
    city TEXT,
    region TEXT,
    country TEXT,
    confidence REAL,
    method TEXT
);

CREATE TABLE IF NOT EXISTS entities (
    source_id INTEGER PRIMARY KEY REFERENCES sources(id) ON DELETE CASCADE,
    content_type TEXT,
    production_type TEXT,
    topic_area TEXT,
    audience TEXT,
    engagement_level TEXT,
    taxonomy_meta TEXT DEFAULT '{}'
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

CREATE TABLE IF NOT EXISTS submitted_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT,
    title TEXT NOT NULL,
    description TEXT,
    source_url TEXT,
    category TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    analysis_status TEXT NOT NULL DEFAULT 'queued',
    analysis_result TEXT,
    metadata TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
```

Add indexes early:

```sql
CREATE INDEX IF NOT EXISTS idx_sources_platform ON sources(platform);
CREATE INDEX IF NOT EXISTS idx_sources_year ON sources(published_year);
CREATE INDEX IF NOT EXISTS idx_sources_relevance ON sources(is_topic_relevant, is_scope_relevant);
CREATE INDEX IF NOT EXISTS idx_triplets_source ON triplets(source_id);
CREATE INDEX IF NOT EXISTS idx_triplets_pair ON triplets(subject, object);
CREATE INDEX IF NOT EXISTS idx_submitted_status ON submitted_data(status, analysis_status);
```

### Data Modeling Rules

- Store the original text or enough source context to audit extraction.
- Store derived fields with confidence and timestamp.
- Never overwrite user-visible facts without keeping enough provenance to explain when and how they were created.
- Use JSON columns only for flexible metadata, not for primary filter fields.
- For every classifier output, include an abstention path: unknown, low signal, not enough text, duplicate, or out of scope.

## 5. Define Your Domain Configuration

Create `config.py` with project-level settings:

```python
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    project_name: str = "OpenAtlas"
    topic_name: str = "biodiversity"
    scope_name: str = "Thailand"
    database_url: str = "sqlite:///data/processed/openatlas.db"
    database_read_only: bool = False
    llm_api_key: str | None = None
    relevance_recheck_days: int = 30
    relevance_audit_batch_size: int = 50

settings = Settings()
```

Avoid scattering topic words across templates and prompts. A later clone should change the topic by editing configuration, taxonomy files, and source adapters.

## 6. Source Adapter Pattern

Every source should implement the same contract:

```python
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Protocol

@dataclass
class FetchParams:
    start_date: date | None = None
    end_date: date | None = None
    query: str | None = None
    limit: int = 500
    extra: dict[str, Any] = field(default_factory=dict)

@dataclass
class ValidationResult:
    ok: bool
    reasons: list[str] = field(default_factory=list)

class SourceAdapter(Protocol):
    platform_id: str
    display_name: str

    async def fetch(self, params: FetchParams) -> list["SourceRaw"]:
        ...

    def validate(self, raw: "SourceRaw") -> ValidationResult:
        ...

    def extract_meta(self, raw: "SourceRaw") -> dict[str, Any]:
        return {}
```

Examples:

- News/topic monitoring: RSS, GDELT, Google Custom Search, Common Crawl.
- Video/media: YouTube, Vimeo, TikTok metadata, podcast RSS.
- Research: Crossref, OpenAlex, arXiv, PubMed, Semantic Scholar.
- Government: open data portals, legislation APIs, procurement portals.
- Biodiversity: GBIF, iNaturalist, museum APIs.
- Company/product data: SEC filings, Wikidata, product feeds, registries.

Adapter quality gates:

- URL must be valid and canonicalized.
- Title or text must exist.
- Published date should be parsed when available.
- Platform-specific IDs should be stored.
- Records with impossible dates, empty text, or unsupported file types should be skipped with a reason.

## 7. Ingestion Pipeline

Implement ingestion as small idempotent steps:

```text
fetch external records
  -> validate
  -> canonicalize URL
  -> dedupe by URL/external ID
  -> insert sources
  -> mark fetched_at
  -> enqueue enrichment
```

CLI commands:

```text
python -m openatlas db init
python -m openatlas ingest seeds
python -m openatlas ingest rss --feed all
python -m openatlas ingest source --platform openalex --year 2025
python -m openatlas ingest geo --pending
python -m openatlas filter relevance
python -m openatlas analyze nlp --year 2025
python -m openatlas analyze stats
python -m openatlas analyze graph-metrics
python -m openatlas serve
```

Idempotency rules:

- Running the same ingest twice must not duplicate rows.
- Running NLP twice must skip completed rows unless `--force` is passed.
- Failed rows should not stop the whole batch.
- Keep count summaries: fetched, inserted, updated, skipped, failed.
- Commit per source or per small batch, not only at the end of a long run.

## 8. Deduplication Methods

Use multiple dedupe layers. Start cheap, then get smarter.

### Exact URL Dedupe

Canonicalize URLs:

- Lowercase scheme and host.
- Remove tracking parameters such as `utm_*`, `fbclid`, `gclid`.
- Normalize trailing slash.
- Resolve known short links when possible.

Use `UNIQUE(url)`.

### Platform ID Dedupe

For APIs, use `(platform, external_id)` as a second uniqueness rule when possible.

### Text Similarity Dedupe

For near-duplicates:

1. Clean text.
2. Convert to shingles or TF-IDF vectors.
3. Compute cosine similarity.
4. Mark records above a threshold as duplicates.

Useful thresholds:

- `0.95` or higher: almost certainly duplicate.
- `0.85` to `0.95`: likely syndicated or copied.
- Below `0.85`: treat carefully.

Math:

```text
tfidf(t, d) = tf(t, d) * log((N + 1) / (df(t) + 1)) + 1
cosine(a, b) = dot(a, b) / (||a|| * ||b||)
```

Optimization:

- Compare only within the same year or same source family for large corpora.
- Use MinHash or locality-sensitive hashing once pairwise comparison becomes too slow.
- Store a `content_hash` for exact text duplicates.

## 9. Relevance Classifier

Every atlas needs two relevance questions:

1. Is this record about the topic?
2. Is this record inside the platform's chosen scope?

For example:

- Topic: biodiversity.
- Scope: Thailand.
- Accepted only if both are true.

Classifier output:

```json
{
  "is_topic_relevant": true,
  "is_scope_relevant": true,
  "topic_domain": "marine biodiversity",
  "rejection_reason": null,
  "confidence": 0.87
}
```

Approaches:

- Rule-based baseline: keyword lists, regexes, allowlists, blocklists.
- Classical ML: logistic regression or linear SVM over TF-IDF features.
- Embedding classifier: compare source embeddings to labeled topic examples.
- LLM classifier: ask for strict JSON and grounded evidence.
- Hybrid: rules for obvious cases, ML/LLM for uncertain cases.

Recommended scoring model:

```text
score = 0.45 * topic_score
      + 0.25 * scope_score
      + 0.15 * source_quality_score
      + 0.15 * evidence_density_score
```

Accept if:

```text
is_topic_relevant = topic_score >= 0.70
is_scope_relevant = scope_score >= 0.65
confidence = min(score, evidence_density_adjusted_score)
```

Human review buckets:

- `confidence >= 0.85`: auto-accept.
- `0.60 <= confidence < 0.85`: accept but eligible for audit.
- `< 0.60`: queue for review or reject.

Optimization:

- Only classify rows where `relevance_checked_at IS NULL`.
- Recheck stale rows after 30 to 90 days.
- Recheck low-confidence accepted rows first.
- Cache LLM prompts by input hash.
- Keep a validation set with human labels.

## 10. Taxonomy Design

Taxonomy is the backbone of useful analytics.

Create a domain file:

```yaml
topic_domains:
  - marine biodiversity
  - forest ecology
  - invasive species
  - conservation policy

content_types:
  - academic
  - news
  - educational
  - government
  - community

production_types:
  - university
  - government agency
  - civil society
  - independent creator
  - corporate media

media_formats:
  - article
  - report
  - video
  - dataset
  - map
  - academic paper
```

Rules:

- Keep labels stable. Renaming labels breaks trend charts.
- Store unknowns instead of forcing bad categories.
- Do not make too many labels in version one.
- Add a mapping table for aliases.
- Track taxonomy version in metadata.

## 11. NLP Entity and Relationship Extraction

Extract two types of derived data:

1. Entity/taxonomy classification: category labels for each source.
2. Relationship triplets: subject, relation, object.

Triplet examples for different domains:

```text
species -> observed_in -> province
agency -> funds -> project
law -> regulates -> activity
company -> supplies -> product
researcher -> affiliated_with -> institution
concept -> related_to -> concept
```

LLM prompt rules:

- Give the title, URL, and source text excerpt.
- Require strict JSON.
- Require confidence per triplet.
- Require abstention when text is too thin.
- Do not allow invented entities.
- Ask for evidence spans or short supporting notes.

Post-processing:

- Normalize whitespace and casing.
- Drop self-loops where subject equals object.
- Drop very short nodes unless allowlisted.
- Merge aliases with a dictionary.
- Keep relation vocabulary constrained.

Triplet quality score:

```text
triplet_confidence =
    0.40 * model_confidence
  + 0.25 * evidence_overlap
  + 0.20 * entity_specificity
  + 0.15 * relation_allowed_score
```

Drop triplets below `0.50`; queue `0.50` to `0.70` for later review if the graph is sensitive.

## 12. Geographic Enrichment

If your topic has places, create a geocoding layer.

Possible methods:

- Direct lat/lng from source API.
- Place names extracted from text.
- Institution address lookup.
- Domain/IP fallback for web sources.
- Manual curation for known organizations.

Always store `method` and `confidence`.

Example:

```json
{
  "lat": 13.7563,
  "lng": 100.5018,
  "city": "Bangkok",
  "country": "Thailand",
  "confidence": 0.78,
  "method": "place_name_extraction"
}
```

Do not pretend inferred coordinates are exact. In the UI, show approximate points differently from precise observations.

## 13. Graph Model

Build a directed concept graph from triplets:

```text
subject --relation--> object
```

Normalize node IDs:

```python
def norm(text: str) -> str:
    return " ".join(text.strip().lower().split())
```

Edge weight:

```text
weight(subject, object) = count of triplets connecting the normalized pair
```

Recommended metrics:

- Node count and edge count.
- Weakly connected components.
- Degree centrality for prominent concepts.
- Betweenness centrality for bridge concepts.
- Community detection for clusters.
- Top neighbors per node.

Math:

```text
degree_centrality(v) = degree(v) / (n - 1)

betweenness(v) =
  sum over all source-target pairs s,t of
  shortest_paths_through_v(s,t) / all_shortest_paths(s,t)
```

Optimization:

- Compute betweenness only on the largest component for large graphs.
- Cache graph metrics in `stats_cache`.
- Pre-warm node detail registry at server startup.
- Limit frontend graph payloads. Send top N nodes by weighted degree or filter by confidence.
- For huge graphs, use server-side pagination around clicked nodes instead of loading everything.

Community detection:

- Start with greedy modularity communities from NetworkX.
- Later use Leiden or Louvain for larger graphs.
- Store summaries: community size, hub label, top relations, representative sources.

## 14. Analytics and Statistical Methods

Many public datasets are skewed. Views, citations, likes, and counts usually have long tails. Do not use raw arithmetic means as your main story.

### Engagement Transform

Use:

```text
log_views = log(1 + views)
```

This handles zeros and reduces outlier dominance.

### Geometric Mean

For a group:

```text
geometric_mean = exp(mean(log(1 + views))) - 1
```

If your implementation uses `exp(mean(log_views))`, explain whether the output is on the `views + 1` scale or subtract one for original scale.

### Bootstrap Confidence Interval

Algorithm:

```text
input: log-transformed values x[1..n]
repeat B times:
  sample n values from x with replacement
  compute sample mean
CI = percentile interval of bootstrap means
return exp(mean), exp(CI_low), exp(CI_high)
```

Recommended:

- `B = 2,000` for UI refreshes.
- `B = 10,000` for offline reports.
- `90% CI` for exploratory dashboards or `95% CI` for formal reporting.

### Group Differences

Use nonparametric tests:

- Mann-Whitney U for two groups.
- Kruskal-Wallis H for three or more groups.
- Chi-square independence for count tables.

Effect sizes:

```text
Mann-Whitney effect r = |Z| / sqrt(N)
Cramer's V = sqrt(chi2 / (N * (min(rows, cols) - 1)))
```

### Trends

Use:

- Mann-Kendall for monotonic trend in yearly group summaries.
- Spearman correlation for year versus log engagement across observations.

Interpretation:

- Report direction, effect, p-value, and sample size.
- Do not claim causality from trend charts.
- Hide or mark tests with insufficient data.

## 15. Cache Strategy

Cache expensive JSON payloads:

- Graph metrics.
- Node detail registry.
- Yearly analytics.
- Dashboard summaries.
- Export manifests.

Table:

```sql
CREATE TABLE IF NOT EXISTS stats_cache (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    computed_at TEXT NOT NULL
);
```

Cache keys:

```text
graph:metrics:v1
graph:node_details:v1
analytics:yearly_taxonomy:v1
dashboard:summary:v1
```

Invalidation:

- Invalidate graph caches after triplets change.
- Invalidate analytics after sources/entities/view counts change.
- Keep a TTL for server memory caches.
- Include algorithm or taxonomy version in the key.

## 16. Web Application Pages

### Landing Page

Show:

- Corpus size.
- Source types.
- Year range.
- Latest pipeline status.
- Links into dashboard, network, analytics, database, and submissions.

### Dashboard Map

Show:

- Points or regions from `geo`.
- Filters by year, platform, category, confidence, and source type.
- Detail panel for selected source.
- Clear visual distinction between exact and approximate locations.

### Network Page

Show:

- 3D or 2D force graph.
- Node size by weighted degree.
- Edge weight by repeated relation count.
- Click node to load detail: neighbors, relations, sources, categories, confidence.
- Search node by label.
- Filter by relation, category, year, or confidence.

### Analytics Page

Show:

- Topic/category trend charts.
- Production/media type comparisons.
- Bootstrap intervals.
- Nonparametric test summaries.
- A method note explaining log transform, confidence intervals, and limitations.

### Database Page

Show:

- Filtered table.
- Search by title, URL, text, entity, relation.
- Filters by year, platform, category, relevance, confidence.
- XLSX or CSV export.
- Source detail drawer.

### Submission Page

Show:

- URL/title/description/category form.
- AI or rule-based "analyze URL" action.
- Status of user submissions.
- Evidence and confidence for generated metadata.

### Admin Queue

Show:

- Pending submissions.
- Analysis payload.
- Approve/reject/edit actions.
- Reviewer notes.
- Audit history.

## 17. Auth and Roles

Minimum roles:

- Anonymous: can view public data.
- User: can submit data and view own submissions.
- Admin: can review all submitted data and manage categories.

Implementation options:

- Local SQLite auth for demos.
- Supabase Auth for production.
- OAuth provider if your community already uses Google/GitHub/Microsoft.

Rules:

- Browser receives only public keys.
- Secret/service keys stay on the server.
- All admin actions must verify server-side role.
- Public submission visibility requires approved status.

## 18. Grounded URL Analysis for Submissions

When a user submits a URL:

1. Fetch the page.
2. Extract title, meta description, main text, canonical URL.
3. Limit text to a safe context window.
4. Ask classifier to return grounded metadata.
5. Store evidence, confidence, and review notes.
6. Never publish until approved.

Output:

```json
{
  "title": "Source title",
  "description": "Short grounded summary",
  "category": "approved taxonomy label",
  "tags": ["tag one", "tag two"],
  "confidence": 0.74,
  "needs_review": true,
  "evidence_quotes": ["short excerpt from source"],
  "data_quality": "medium",
  "analysis_notes": "Sparse page text; category inferred from title and meta description."
}
```

Normalize generated categories to the approved taxonomy. If the model invents a category, map it to `other` or the closest approved label and keep the original in metadata.

## 19. Algorithm Registry

Use a registry for classifiers and extractors so improvements do not silently replace old behavior.

Store:

- Algorithm name.
- Version.
- Input schema.
- Output schema.
- Description.
- Runtime metrics.
- Validation scores.

Selection rule:

```text
if any versions have validation scores:
    choose version with highest mean validation score
else:
    choose newest registered version
```

Validation score examples:

- Relevance classifier: F1 or balanced accuracy.
- Triplet extractor: precision on reviewed triplets.
- Taxonomy classifier: macro F1.
- Geocoder: distance error bucket accuracy.

Log each run:

```json
{
  "name": "relevance",
  "version": "v2",
  "input_hash": "abc123",
  "duration_ms": 418.2,
  "ok": true,
  "ts": "2026-01-01T00:00:00Z"
}
```

## 20. Optimization Plan

Start with correctness, then optimize the slow parts.

Database:

- Add indexes for every common filter.
- Use pagination for tables.
- Use read-only SQLite connections for demo serving.
- Move to PostgreSQL for concurrent writes.

Pipeline:

- Process only pending rows.
- Batch external API calls.
- Retry transient failures with exponential backoff.
- Save partial progress.
- Use per-source error isolation.

LLM/API cost:

- Use rules before LLMs.
- Skip records with no text.
- Cache by text hash.
- Use smaller models for easy classification.
- Recheck only stale or low-confidence rows.

Graph:

- Limit full graph payload.
- Precompute centralities.
- Use largest component for expensive metrics.
- Store node neighborhood payloads separately.

Frontend:

- Server-render first page.
- Fetch heavy charts asynchronously.
- Compress JSON responses.
- Use filter controls that reduce payload size before rendering.

## 21. Testing Strategy

Write tests around behavior, not only functions.

Minimum tests:

- Database migration creates all tables and indexes.
- Adapter validates and normalizes sample records.
- Ingest command is idempotent.
- Relevance classifier handles accept, reject, and uncertain cases.
- NLP pipeline skips completed rows.
- Graph metrics handle empty, tiny, and normal graphs.
- Analytics methods handle zeros, small samples, and identical values.
- API routes return stable JSON shapes.
- Submission flow requires auth for user-owned actions.
- Admin routes reject non-admin users.

Use fixtures:

```text
tests/fixtures/sample_sources.json
tests/fixtures/sample_triplets.json
tests/fixtures/sample_submissions.json
```

For AI-dependent functions, mock the provider and test JSON normalization separately.

## 22. Deployment Modes

### Local Research Mode

- SQLite database.
- Full pipeline enabled.
- Local auth allowed.
- Run with `python -m openatlas serve`.

### Read-Only Demo Mode

- Bundled SQLite database.
- Writes disabled.
- Ingest disabled.
- Export generated in memory.
- Good for Vercel-style demos.

### Production Mode

- PostgreSQL/Supabase.
- Object storage for large files.
- Background worker for ingestion.
- Cron or Prefect for scheduling.
- Auth provider enabled.
- Backups and monitoring.

## 23. Step-by-Step Build Order

Follow this order. It reduces risk.

1. Create project skeleton and config.
2. Add database schema and migration command.
3. Add `SourceRaw`, `SourceRow`, `Triplet`, and submission models.
4. Implement repositories for sources, entities, triplets, cache, and submissions.
5. Implement one source adapter, preferably RSS or seed YAML.
6. Add ingest CLI and prove idempotency.
7. Build a simple database browser page.
8. Add relevance classifier with rule-based baseline.
9. Add NLP extraction with strict JSON and abstention.
10. Build graph metrics from triplets.
11. Add network page and node detail API.
12. Add analytics page with bootstrap and nonparametric tests.
13. Add geo enrichment and map page if geography matters.
14. Add user submissions.
15. Add admin review.
16. Add cache invalidation.
17. Add scheduled orchestration.
18. Add export.
19. Add tests and CI.
20. Add production deployment mode.

Do not start with a beautiful UI before ingestion and schema are real. The platform is only useful if the data pipeline can be rerun.

## 24. Instructions for CLI AI Agents

When asking a CLI AI agent to build a clone, use a prompt like this:

```text
Build an open-source Topic Atlas platform in this repository.

Topic: <your topic>
Scope: <your geography, language, institution, time range, or corpus boundary>
Primary sources: <APIs, RSS feeds, seed URLs, local CSVs>

Requirements:
- Python FastAPI app with Jinja templates.
- SQLite local database with migration script.
- Typer CLI for db/init, ingest, analyze, filter, serve.
- Source adapter registry.
- Relevance classifier with confidence and rejection reasons.
- NLP entity classification and subject-relation-object triplet extraction.
- Graph metrics using NetworkX.
- Analytics with log1p engagement, bootstrap geometric means, Kruskal-Wallis, chi-square, and trend tests.
- stats_cache table for expensive JSON.
- Pages: landing, dashboard/map, network, analytics, database, submit-data, profile, admin review.
- Tests for schema, ingest idempotency, APIs, graph metrics, and stats edge cases.

Build one vertical slice first:
seed URL ingest -> DB -> relevance -> NLP -> graph metrics -> database page -> network API.
Then expand source adapters and UI.
```

### Paste-Ready Phase-Gated CLI Agent Prompt

Use this longer prompt when you want the agent to work phase by phase and wait for developer approval before moving on.

```text
You are building an open-source Topic Atlas platform in this repository.

Topic: <your topic>
Scope: <your geography, language, institution, time range, or corpus boundary>
Primary sources: <APIs, RSS feeds, seed URLs, local CSVs, public datasets>
Required external services: <LLM provider, Supabase, search API, geocoder, analytics, etc.>
Deployment target: <local only, Vercel demo, VPS, Docker, Supabase/Postgres, etc.>

Core requirements:
- Python FastAPI app with Jinja templates.
- SQLite local database first, with a clean path to PostgreSQL/Supabase later.
- Typer CLI for db/init, ingest, analyze, filter, serve, and export.
- Source adapter registry so new data sources can be added without rewriting pipeline code.
- Relevance classifier with confidence, rejection reason, and stale/low-confidence recheck.
- Entity/taxonomy classification and subject-relation-object triplet extraction.
- Graph metrics using NetworkX.
- Analytics with log1p engagement, bootstrap geometric means, Kruskal-Wallis, chi-square, Mann-Kendall/Spearman trend checks where data permits.
- stats_cache table for expensive JSON payloads.
- Pages: landing, dashboard/map, network, analytics, database, submit-data, profile, admin review.
- Playwright smoke tests for UI pages and browser-console/runtime errors.
- Unit tests for schema, ingest idempotency, APIs, graph metrics, and stats edge cases.

Operating rules:
1. Work in phases. Do not start the next phase until the developer approves the current phase.
2. At the start of each phase, state the exact files you expect to create or edit and the acceptance checks you will run.
3. At the end of each phase, stop and report:
   - completed work
   - files changed
   - commands/tests run
   - Playwright checks run, including pages visited
   - errors found and exact error text
   - missing information or missing credentials
   - tasks the developer must do outside the editor/app
   - whether the phase is ready for approval
4. If an error happens, do not hide it. State:
   - command or action that failed
   - exact error message
   - likely cause
   - whether you can fix it in code
   - whether the developer must provide something outside the editor
5. If information is missing, stop and ask for it only when a safe default is not possible. Examples:
   - missing API key
   - missing OAuth redirect URL
   - missing Supabase project URL/key
   - missing source API terms or dataset URL
   - missing taxonomy labels
   - missing deployment target
6. If a task must be done outside the editor, state it explicitly. Examples:
   - create API key in provider dashboard
   - enable OAuth provider
   - download GeoIP database
   - accept data-source license
   - set production environment variables
   - create Supabase tables using SQL editor
7. Prefer local/demo fallbacks when credentials are missing, but mark the feature as degraded.
8. Do not mark a phase complete until code checks, tests, and Playwright smoke checks pass or until unresolved blockers are clearly reported.
9. Keep implementation topic-neutral. Put topic wording in config, taxonomy files, seed data, prompts, and templates.
10. Do not delete or revert unrelated user changes.

Phase 0: Repository audit and implementation plan
- Inspect the existing project structure.
- Identify framework, package manager, test setup, run commands, and current database files.
- Report a phase plan with proposed files and risks.
- Stop for developer approval.

Acceptance checks:
- No code changes unless needed for discovery.
- Clear list of phases and dependencies.
- Clear list of missing keys/services/datasets.

Phase 1: Project skeleton and configuration
- Add or update package metadata.
- Add config settings for project name, topic, scope, database URL, read-only mode, auth mode, and external API keys.
- Add .env.example with required and optional variables.
- Add basic CLI entrypoint.

Acceptance checks:
- CLI imports successfully.
- Config loads with no real secrets.
- Missing optional keys produce clear warnings, not crashes.
- Stop for developer approval.

Phase 2: Database schema and repositories
- Add schema/migrations for sources, geo, entities, triplets, stats_cache, submitted_data, users/profiles if local auth is needed, and pipeline_runs.
- Add repository helpers for insert, list, update, cache get/set, and submission review.
- Add indexes for common filters.

Acceptance checks:
- `db init` creates the database from scratch.
- Running `db init` twice is safe.
- Unit tests verify required tables and indexes.
- Stop for developer approval.

Phase 3: Source adapter system and first ingest
- Implement SourceAdapter, FetchParams, ValidationResult, and adapter registry.
- Implement one simple adapter first: seed YAML, CSV, RSS, or another source available without paid credentials.
- Add ingest CLI command.
- Add URL canonicalization and exact dedupe.

Acceptance checks:
- Ingest inserts sample records.
- Re-running ingest does not duplicate records.
- Missing source file/API key produces a clear message.
- Stop for developer approval.

Phase 4: Relevance filtering and taxonomy
- Add taxonomy config file.
- Add rule-based relevance classifier baseline.
- Add optional LLM classifier only if API key exists.
- Store topic/scope flags, confidence, domain, rejection reason, and checked timestamp.
- Add stale and low-confidence recheck commands.

Acceptance checks:
- Accepted, rejected, and uncertain fixture records are handled.
- Missing LLM key falls back to rules or reports degraded mode.
- Tests cover classifier output shape.
- Stop for developer approval.

Phase 5: NLP extraction and enrichment
- Add entity/taxonomy classification.
- Add triplet extraction with strict JSON output and abstention records.
- Add dedupe/near-duplicate checks if enough text exists.
- Add geo enrichment only if the topic needs geography.

Acceptance checks:
- NLP skips already completed rows unless forced.
- Thin text creates an abstention instead of crashing.
- Triplets are normalized and confidence-scored.
- Missing LLM/geocoder keys are reported clearly.
- Stop for developer approval.

Phase 6: Graph metrics and cached analytics
- Build directed graph from triplets.
- Compute degree centrality, betweenness on largest component, connected components, community summaries, and node detail payloads.
- Add analytics with log1p engagement, bootstrap geometric means, nonparametric tests, and trend tests where sample sizes permit.
- Cache expensive JSON in stats_cache.

Acceptance checks:
- Empty graph, tiny graph, and normal graph tests pass.
- Analytics handles zeros, identical values, and insufficient groups.
- Cache invalidation works after data changes.
- Stop for developer approval.

Phase 7: Web UI and JSON APIs
- Add FastAPI server, templates, static files, and JSON routes.
- Implement landing, dashboard/map, network, analytics, database, submit-data, profile, and admin review pages.
- Add CSV/XLSX export if dependencies are available.
- Add clear method notes and corpus-boundary wording.

Acceptance checks:
- Server starts locally.
- All key routes return 200 or expected auth redirects.
- API responses have stable JSON shapes.
- Stop before broad visual polish unless the developer approves.

Phase 8: Playwright browser validation
- Install or use existing Playwright setup.
- Start the app locally.
- Visit these pages with Playwright:
  - /
  - /dashboard
  - /network
  - /analytics
  - /database
  - /submit-data
  - /profile
  - /admin/submitted-data
- Capture browser console errors, page errors, failed network requests, missing assets, and layout-blocking runtime exceptions.
- Test at desktop and mobile viewport sizes.
- If maps, charts, or graph canvases exist, verify they are nonblank or have an intentional empty state.

Acceptance checks:
- No unexpected browser console errors.
- No 404s for static assets.
- No unhandled page exceptions.
- Main pages render meaningful content or an explicit empty state.
- Mobile navigation and important text do not overlap.
- Stop for developer approval.

Phase 9: Auth, submissions, and admin review
- Add local auth fallback or Supabase/Auth provider integration.
- Add user-owned submissions.
- Add grounded URL analysis if source fetching and LLM key are available.
- Add admin approve/reject/edit flow.

Acceptance checks:
- Anonymous users cannot modify protected resources.
- Users can see their own submissions.
- Non-admin users cannot access admin actions.
- Approved submissions become public only after review.
- Missing auth provider settings are reported with exact env vars needed.
- Playwright verifies auth pages render without runtime errors.
- Stop for developer approval.

Phase 10: Orchestration, deployment, and documentation
- Add scheduled pipeline option: cron, Prefect, GitHub Actions, or deployment-native scheduler.
- Add deployment instructions for local, read-only demo, and production.
- Add README setup, operator workflow, troubleshooting, data-source license notes, and method notes.

Acceptance checks:
- Fresh setup instructions work.
- Production env vars are documented.
- Read-only demo mode disables writes safely.
- Tests and Playwright smoke checks pass or all blockers are clearly listed.
- Stop for final developer approval.

Final response format after each phase:

Phase: <number and name>
Status: <ready for approval | blocked | needs developer action>
Changed files:
- <file>
Checks run:
- <command or Playwright route>
Errors:
- <none or exact error>
Missing information:
- <none or exact missing item>
Developer must do outside editor:
- <none or exact task>
Next phase after approval:
- <one sentence>
```

### Paste-Ready UI/UX Designer Prompt

Use this prompt when you want a UI/UX designer, product designer, or frontend-focused AI agent to design the atlas experience for any topic. Replace the placeholders with your domain.

```text
Design the UI/UX for a topic-neutral Research Atlas web platform.

Topic: <any topic, for example biodiversity, local history, public policy, scientific literature, media archives, climate events, patents, supply chains, cultural heritage, companies, legislation, health research, education resources, etc.>
Scope: <geography, language, time range, organization, community, dataset boundary, or corpus rule>
Primary users: <researchers, analysts, students, journalists, curators, policymakers, community contributors, admins, etc.>
Primary user job: <what the user needs to accomplish with this atlas>
Data types: <articles, videos, papers, datasets, maps, observations, laws, products, organizations, events, etc.>
Main entities: <people, places, institutions, species, topics, projects, laws, papers, products, events, etc.>
Tone: <serious research, civic dashboard, educational, archival, operational, exploratory, etc.>
Brand constraints: <colors, logo, typography, existing website, accessibility requirements, or "none">
Frontend stack: <FastAPI/Jinja, React, Next.js, Vue, Svelte, static HTML, etc.>

Goal:
Create a usable product interface, not a marketing-only landing page. The first screen should immediately reveal the atlas subject, current corpus state, and the main workflow.

Required product surfaces:
- Landing or home view with live corpus summary and clear entry points.
- Dashboard/map or overview view for location, category, or source distribution.
- Network view for concept/entity relationships.
- Analytics view for trends, statistical comparisons, and method notes.
- Database browser with search, filters, source details, and export affordance.
- Submit-data flow for community/user contributions.
- Profile page for user-owned submissions.
- Admin review queue for approving, rejecting, and editing submissions.
- Empty, loading, error, low-confidence, and read-only/demo states.

Design process:
1. Inspect the existing app before designing. Identify routes, templates/components, CSS system, icon library, chart/map/graph libraries, and responsive patterns.
2. Define the primary workflows:
   - discover corpus overview
   - inspect one source
   - filter/search the database
   - explore a graph node
   - compare trends/categories
   - submit a new record
   - review a pending submission as admin
3. Propose a design system:
   - color roles, not only a color palette
   - typography scale for dense data screens
   - spacing and grid rules
   - table/filter/sidebar patterns
   - chart/map/network states
   - form and review states
   - accessibility and keyboard rules
4. Make the design topic-adaptive. Do not hard-code "quantum" or any other example topic. Use placeholders and configurable labels:
   - <Topic Atlas Name>
   - <Topic>
   - <Scope>
   - <Primary Entity>
   - <Primary Category>
   - <Source Type>
5. Prefer dense, scannable interfaces for research and operational workflows. Avoid generic SaaS hero sections, decorative gradient blobs, nested cards, and oversized marketing copy.
6. Use real controls:
   - filters for year/source/category/confidence
   - tabs for analytics sections
   - toggles for exact vs inferred data
   - icon buttons for search, export, reset, approve, reject, edit, refresh
   - tooltips for unfamiliar icons
   - clear disabled states when data or credentials are missing
7. Write short operational UI copy. Do not fill the app with text explaining how the UI works unless an onboarding or method note genuinely needs it.
8. Design responsive behavior explicitly for desktop, tablet, and mobile.
9. Validate the UI in browser, not only by reading code.

Page-specific UX requirements:

Home:
- Show corpus size, source count by platform, year range, latest pipeline update, and review queue status.
- Provide direct actions into dashboard, network, analytics, database, and submission flow.
- If data is empty, show a useful empty state with the next operator action.

Dashboard/map:
- Show distribution by location, category, source type, or other domain-specific dimension.
- Make precise vs inferred data visually distinct.
- Provide a detail panel that works on mobile without covering navigation.

Network:
- Node size should reflect centrality or source count.
- Edge weight should reflect repeated relationships.
- Clicking a node should show neighbors, relation examples, source links, confidence, and taxonomy context.
- Include search, relation filter, confidence filter, and an intentional empty state.

Analytics:
- Present charts with method notes.
- Show sample size, confidence intervals, and insufficient-data states.
- Avoid implying causality from correlations or trends.

Database:
- Prioritize fast scanning: compact rows, clear filters, stable table layout, and detail drawer.
- Include export control and visible active filters.
- Long titles/URLs must not break layout.

Submit-data:
- Make URL analysis status clear: queued, fetching, analyzed, needs review, failed.
- Show evidence/confidence when AI or automated analysis is used.
- If required API keys are missing, show a graceful degraded state.

Admin:
- Optimize for review speed: side-by-side source metadata, automated analysis, evidence, editable category, approve/reject buttons, reviewer notes.
- Never expose admin actions to non-admin users.

Visual constraints:
- Use the existing design system if one exists.
- Use icons from the existing icon library when available.
- Cards should be used for repeated items, modals, or framed tools, not nested page sections.
- Text must not overlap, clip, or overflow at common viewport widths.
- Do not rely on one dominant hue family for the entire interface.
- Keep contrast accessible.
- Use stable dimensions for maps, graphs, tables, toolbars, and chart panels.
- Motion should clarify state changes, not distract from data.

Playwright/browser validation:
- Start the app locally.
- Visit desktop and mobile viewports for:
  - /
  - /dashboard
  - /network
  - /analytics
  - /database
  - /submit-data
  - /profile
  - /admin/submitted-data
- Capture screenshots.
- Report console errors, page errors, failed network requests, missing assets, blank charts/maps/graphs, layout overlap, horizontal scroll, clipped text, and inaccessible controls.
- Fix all UI issues that can be fixed in code.
- If an issue requires outside-editor work, state it exactly.

Designer output format:

Design summary:
- <short description of the final UX direction>

Changed files or proposed files:
- <file>

Key workflows covered:
- <workflow>

Responsive behavior:
- <desktop/tablet/mobile notes>

Accessibility checks:
- <keyboard, contrast, labels, focus, reduced motion>

Playwright validation:
- Routes visited:
- Viewports tested:
- Console/page errors:
- Screenshots captured:
- Issues fixed:
- Remaining blockers:

Missing information:
- <none or exact missing item>

Developer must do outside editor:
- <none or exact task, such as provide logo, approve brand colors, create API key, enable auth provider, or supply taxonomy labels>
```

Agent guardrails:

- Inspect existing files before editing.
- Keep changes scoped.
- Prefer repository patterns over new architecture.
- Add tests for each behavior.
- Never hard-code the example topic in reusable modules.
- Use configuration and taxonomy files.
- Run formatting, tests, and a local server smoke test before final response.
- Use Playwright to detect browser errors before saying UI work is complete.
- Stop for developer approval between phases when using the phase-gated prompt.

## 25. Common Mistakes

- Building charts before validating data quality.
- Treating LLM labels as truth without confidence, evidence, and review.
- Hard-coding topic-specific strings everywhere.
- Using raw means for skewed engagement data.
- Loading a giant graph into the browser.
- Forgetting idempotency, causing duplicate records.
- Mixing raw and derived fields with no provenance.
- Publishing community submissions before moderation.
- Running full reprocessing every time instead of pending/stale batches.
- Adding too many taxonomy labels before there is enough data.

## 26. Minimal Definition of Done

A serious first release is done when:

- A new developer can run setup from README.
- `db init` creates a working database.
- At least one adapter ingests real records.
- Re-running ingest does not duplicate data.
- Records are classified with confidence and rejection reason.
- NLP writes entities and triplets or records an abstention.
- Graph and analytics pages load from cached API payloads.
- Database page supports search/filter/export.
- Submissions are stored and reviewable.
- Tests cover the core pipeline.
- The method notes clearly explain limitations.

## 27. License and Open-Source Notes

Use a permissive license such as MIT or Apache-2.0 if you want broad reuse. Check the terms for every external data source. Some APIs allow metadata display but restrict redistribution of full text. Store source URLs and metadata when full text redistribution is not allowed.

Recommended policy:

- Keep source attribution visible.
- Store only excerpts when licenses are restrictive.
- Respect robots.txt and API rate limits.
- Document data provenance.
- Allow takedown or correction requests.

## 28. Final Blueprint

The reusable platform pattern is:

```text
Topic configuration
  + source adapters
  + normalized source table
  + relevance boundary
  + taxonomy
  + grounded extraction
  + graph model
  + robust statistics
  + cached APIs
  + analyst UI
  + community submission workflow
  + admin review
  + reproducible CLI
```

Once this pattern exists, changing the subject is mostly a matter of changing source adapters, taxonomy, prompts, and UI wording. The platform can become an atlas for almost any structured knowledge domain.
