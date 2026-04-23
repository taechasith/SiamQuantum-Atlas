# SiamQuantum Atlas

Research platform for tracking Thai public engagement with quantum technology content (2016–2026). Local SQLite tool — not a hosted SaaS.

Stack: Python 3.11, SQLite, GDELT API v2, YouTube Data API v3, Claude API, FastAPI, openpyxl.

## Quickstart

### 1. Environment setup

```bash
python -m venv .venv
. .venv/bin/activate
pip install -e ".[dev]"
```

On Windows PowerShell:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
```

Copy the example environment file and fill in the required keys:

```bash
cp .env.example .env
```

Required settings:

- `SIAMQUANTUM_DATABASE_URL`
- `SIAMQUANTUM_ANTHROPIC_API_KEY`
- `SIAMQUANTUM_YOUTUBE_API_KEY`
- `SIAMQUANTUM_VIEWER_PORT`

### 2. GeoLite2 download

Geo backfill depends on MaxMind GeoLite2 City and ASN databases. Download them before running `ingest geo`.

If the helper script is available in your environment:

```bash
bash scripts/download_geoip.sh
```

Otherwise:

1. Create a MaxMind account and generate a license key.
2. Download the GeoLite2 City and GeoLite2 ASN `.mmdb` files.
3. Place them where the project expects them for Geo/IP enrichment.

### 3. Initialize the database

```bash
python -m siamquantum db init
```

Reset the database only when you want a destructive clean rebuild:

```bash
python -m siamquantum db reset --confirm
```

## Historical ingest flow

Run the ingest commands year by year for the data you want in the database.

```bash
python -m siamquantum ingest gdelt --year 2024
python -m siamquantum ingest youtube --year 2024
python -m siamquantum ingest geo --pending
```

To process a range from 2020 through the selected year:

```bash
python -m siamquantum ingest gdelt --year 2024 --all-years
python -m siamquantum ingest youtube --year 2024 --all-years
```

## NLP and stats flow

Run NLP for a specific year:

```bash
python -m siamquantum analyze nlp --year 2024
```

Run taxonomy backfill (media_format, user_intent, thai_cultural_angle):

```bash
python -m siamquantum analyze taxonomy-backfill
```

Run bootstrap stats pipeline (replaces t-test — uses geometric mean + Mann-Kendall):

```bash
python -m siamquantum analyze stats
```

Run taxonomy engagement analysis (Kruskal-Wallis + Chi-square):

```bash
python -m siamquantum analyze taxonomy-stats
```

Compute graph centrality metrics:

```bash
python -m siamquantum analyze graph-metrics
```

Run the minimal composed flow over years already present in the database:

```bash
python -m siamquantum analyze full
```

## Serve the viewer

```bash
python -m siamquantum serve
```

Default viewer URL:

- `http://localhost:8765`

Development auto-reload:

```bash
python -m siamquantum serve --reload
```

Custom port:

```bash
python -m siamquantum serve --port 9000
```

## CLI reference

```text
python -m siamquantum db init
python -m siamquantum db reset --confirm

python -m siamquantum ingest gdelt --year YYYY [--all-years]
python -m siamquantum ingest youtube --year YYYY [--all-years]
python -m siamquantum ingest geo --pending

python -m siamquantum analyze nlp --year YYYY
python -m siamquantum analyze stats
python -m siamquantum analyze full

python -m siamquantum serve [--port 8765] [--reload]
```

## Viewer pages

- `/dashboard` - Thailand geo dashboard (origin-IP map, Leaflet + marker clustering)
- `/network` - 3D concept graph from triplets (2,001 nodes, hub/leaf toggle, centrality metrics)
- `/analytics` - yearly charts + bootstrap probability bands + taxonomy engagement tables
- `/database` - filtered source cards (year/platform/content_type/media_format/user_intent) + XLSX export
- `/community` - manual URL submission form (local pipeline only, no auto-run)

## Known limitations

- Geo coverage: 350/768 sources resolved, 88 origin points (non-CDN)
- Bootstrap pairwise: covers 2020–2025 (15 pairs). 2026 excluded — only 1 GDELT source with no YouTube view_count
- All 768 sources backfilled to `is_quantum_tech=1`, `is_thailand_related=1` (whole corpus is Thai quantum by design)
- No live GDELT/YouTube pipeline (requires paid API keys — out of scope for local run)

## Make targets

```bash
make install
make db
make ingest-historical
make serve
make test
make lint
```
