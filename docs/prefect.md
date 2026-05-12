# Prefect Orchestration

SiamQuantum Atlas uses [Prefect 3](https://docs.prefect.io) to run scheduled, retryable, observable data-refresh pipelines outside the FastAPI/Vercel web runtime.

## Flows

| Flow | Description | Default schedule |
|------|-------------|-----------------|
| `siamquantum-refresh` | Full pipeline: ingest → NLP → stats → taxonomy → graph metrics | Daily at 01:00 UTC |
| `siamquantum-healthcheck` | Probe server + DB freshness | Hourly |

## Quick start — local serve (no Prefect server needed)

```bash
# Install
pip install prefect>=3

# Run once right now
siamquantum orchestration refresh

# Schedule both flows locally (blocking, Ctrl+C to stop)
siamquantum orchestration serve

# Custom schedule
siamquantum orchestration serve --refresh-cron "0 2 * * *" --health-cron "0 */6 * * *"
```

`serve` runs flows in-process on the local machine. All run history, retries, and logs appear in the terminal. No external services required.

## Production — Prefect server + work pool

```bash
# 1. Start the Prefect server (separate terminal)
prefect server start

# 2. Create a work pool
prefect work-pool create --type local-process local-process

# 3. Deploy flows
siamquantum orchestration deploy --pool local-process

# 4. Start a worker (separate terminal, leave running)
siamquantum orchestration worker --pool local-process
```

Open the Prefect UI at <http://127.0.0.1:4200> to view scheduled runs, logs, and retry state.

## CLI reference

```
siamquantum orchestration --help

Commands:
  refresh      Run the siamquantum-refresh flow once (blocking)
  healthcheck  Run the siamquantum-healthcheck flow once (blocking)
  serve        Start local scheduled serve (no server required)
  deploy       Deploy to a running Prefect server
  worker       Start a local-process worker
```

## pipeline_runs table

Every task records its result in the `pipeline_runs` SQLite table:

```sql
SELECT flow_name, task_name, status, finished_at, duration_s
FROM pipeline_runs
ORDER BY finished_at DESC
LIMIT 20;
```

Columns: `flow_name`, `task_name`, `status` (running/success/failure), `started_at`, `finished_at`, `duration_s`, `error_text`.

## Retry policy

| Task | Retries | Delays |
|------|---------|--------|
| `ingest-today` | 3 | 30 s, 120 s, 300 s |
| `analyze-nlp` | 2 | 60 s, 300 s |
| `analyze-stats` | 2 | 30 s, 120 s |
| `analyze-taxonomy-stats` | 2 | 30 s, 120 s |
| `analyze-graph-metrics` | 2 | 30 s, 120 s |
| `check-server` | 3 | 10 s, 30 s, 60 s |

## Environment variables

```env
# Optional — only needed when using `deploy` (Prefect server mode)
PREFECT_API_URL=http://127.0.0.1:4200/api
# PREFECT_API_KEY=...   # Prefect Cloud only
PREFECT_WORK_POOL=local-process
```

`serve` and direct `refresh`/`healthcheck` calls work without any Prefect env vars set.
