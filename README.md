# SiamQuantum Atlas

Research platform tracking Thai public engagement with quantum technology content (2020–present).

**Stack:** Python 3.11 · SQLite · GDELT API v2 · YouTube Data API v3 · Claude API · FastAPI (port 8765)

## Quickstart

```bash
# 1. Install dependencies
pip install -e ".[dev]"

# 2. Configure environment
cp .env.example .env
# Edit .env — set SIAMQUANTUM_ANTHROPIC_API_KEY, SIAMQUANTUM_YOUTUBE_API_KEY, MAXMIND_LICENSE_KEY

# 3. Download GeoLite2 database
bash scripts/download_geoip.sh

# 4. Initialize database
python -m siamquantum db init

# 5. Ingest historical data (2020–2024)
make ingest-historical

# 6. Run NLP + stats
python -m siamquantum analyze full

# 7. Start viewer
python -m siamquantum serve
# → http://localhost:8765
```

## Pages
- `/dashboard` — Thailand map with clustered source markers
- `/network` — 3D force-directed entity/triplet graph
- `/analytics` — yearly engagement charts + t-test significance badges
- `/database` — filterable card grid + XLSX export
- `/community` — manual source submission

## CLI Reference
```
python -m siamquantum db init
python -m siamquantum ingest gdelt --year 2024
python -m siamquantum ingest youtube --year 2024
python -m siamquantum ingest geo --pending
python -m siamquantum analyze nlp --year 2024
python -m siamquantum analyze stats
python -m siamquantum serve [--port 8765] [--reload]
```
