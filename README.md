# SiamQuantum Atlas

Research platform for tracking Thai public engagement with quantum technology content from 2020 onward.

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

Run the stats pipeline:

```bash
python -m siamquantum analyze stats
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

- `/dashboard` - Thailand geo dashboard
- `/network` - 3D entity/triplet graph
- `/analytics` - yearly charts and significance badges
- `/database` - filtered source cards and XLSX export
- `/community` - manual community submission form

## Make targets

```bash
make install
make db
make ingest-historical
make serve
make test
make lint
```
