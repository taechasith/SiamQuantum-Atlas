from __future__ import annotations

import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from prefect import flow, get_run_logger, task

from siamquantum.config import settings
from siamquantum.db.session import db_path_from_url, get_connection

PYTHON = sys.executable
REPO_ROOT = Path(__file__).parent.parent.parent.parent

_FLOW_REFRESH = "siamquantum-refresh"
_FLOW_HEALTH = "siamquantum-healthcheck"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _record(
    db_path: Path,
    flow_name: str,
    task_name: str,
    status: str,
    started_at: str,
    finished_at: str | None = None,
    duration_s: float | None = None,
    error_text: str | None = None,
) -> None:
    try:
        with get_connection(db_path) as conn:
            conn.execute(
                """INSERT INTO pipeline_runs
                   (flow_name, task_name, status, started_at, finished_at, duration_s, error_text)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (flow_name, task_name, status, started_at, finished_at, duration_s, error_text),
            )
    except Exception:
        pass  # never let DB writes block orchestration


def _run_cli(*args: str) -> tuple[int, str]:
    result = subprocess.run(
        [PYTHON, "-m", "siamquantum", *args],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
    )
    combined = result.stdout
    if result.stderr:
        combined = combined + "\n" + result.stderr
    return result.returncode, combined.strip()


def _cli_task(
    flow_name: str,
    task_name: str,
    cli_args: list[str],
) -> dict[str, Any]:
    logger = get_run_logger()
    db_path = db_path_from_url(settings.database_url)
    started = _now()
    t0 = datetime.now(timezone.utc)

    rc, output = _run_cli(*cli_args)

    duration = (datetime.now(timezone.utc) - t0).total_seconds()
    finished = _now()
    logger.info(output or "(no output)")

    if rc != 0:
        _record(db_path, flow_name, task_name, "failure", started, finished, duration, output[:2000])
        raise RuntimeError(f"{task_name} exited with rc={rc}:\n{output[-500:]}")

    _record(db_path, flow_name, task_name, "success", started, finished, duration)
    return {"rc": rc, "duration_s": round(duration, 2)}


# ---------------------------------------------------------------------------
# Refresh tasks
# ---------------------------------------------------------------------------

@task(retries=3, retry_delay_seconds=[30, 120, 300], name="ingest-today")
def task_ingest_today() -> dict[str, Any]:
    return _cli_task(_FLOW_REFRESH, "ingest-today", ["ingest", "today"])


@task(retries=2, retry_delay_seconds=[60, 300], name="analyze-nlp")
def task_analyze_nlp() -> dict[str, Any]:
    return _cli_task(_FLOW_REFRESH, "analyze-nlp", ["analyze", "nlp", "--all"])


@task(retries=2, retry_delay_seconds=[30, 120], name="analyze-stats")
def task_analyze_stats() -> dict[str, Any]:
    return _cli_task(_FLOW_REFRESH, "analyze-stats", ["analyze", "stats"])


@task(retries=2, retry_delay_seconds=[30, 120], name="analyze-taxonomy-stats")
def task_analyze_taxonomy_stats() -> dict[str, Any]:
    return _cli_task(_FLOW_REFRESH, "analyze-taxonomy-stats", ["analyze", "taxonomy-stats"])


@task(retries=2, retry_delay_seconds=[30, 120], name="analyze-graph-metrics")
def task_analyze_graph_metrics() -> dict[str, Any]:
    return _cli_task(_FLOW_REFRESH, "analyze-graph-metrics", ["analyze", "graph-metrics"])


# ---------------------------------------------------------------------------
# Healthcheck tasks
# ---------------------------------------------------------------------------

@task(retries=3, retry_delay_seconds=[10, 30, 60], name="check-server")
def task_check_server() -> dict[str, Any]:
    import httpx

    logger = get_run_logger()
    url = f"http://localhost:{settings.viewer_port}/api/pipeline/live"
    try:
        resp = httpx.get(url, timeout=10)
        ok = resp.status_code < 400
        logger.info("GET %s → %s", url, resp.status_code)
        return {"ok": ok, "status_code": resp.status_code}
    except Exception as exc:
        logger.warning("Server unreachable at %s: %s", url, exc)
        return {"ok": False, "error": str(exc)}


@task(name="check-db-freshness")
def task_check_db_freshness() -> dict[str, Any]:
    logger = get_run_logger()
    db_path = db_path_from_url(settings.database_url)
    try:
        with get_connection(db_path) as conn:
            row = conn.execute(
                """SELECT task_name, status, finished_at, duration_s
                   FROM pipeline_runs
                   WHERE status = 'success'
                   ORDER BY finished_at DESC LIMIT 1"""
            ).fetchone()
    except Exception as exc:
        logger.warning("DB freshness check failed: %s", exc)
        return {"task_name": None, "status": None, "finished_at": None}

    if row:
        result = {
            "task_name": row[0],
            "status": row[1],
            "finished_at": row[2],
            "duration_s": row[3],
        }
        logger.info("Last successful run: %s", result)
        return result

    logger.warning("No successful pipeline_runs rows — pipeline has never completed.")
    return {"task_name": None, "status": None, "finished_at": None}


# ---------------------------------------------------------------------------
# Flows
# ---------------------------------------------------------------------------

@flow(name=_FLOW_REFRESH, log_prints=True)
def refresh_flow() -> dict[str, Any]:
    """Full daily refresh: ingest → NLP → stats → taxonomy → graph metrics."""
    r_ingest = task_ingest_today()
    r_nlp = task_analyze_nlp()
    r_stats = task_analyze_stats()
    r_taxonomy = task_analyze_taxonomy_stats()
    r_graph = task_analyze_graph_metrics()
    return {
        "ingest": r_ingest,
        "nlp": r_nlp,
        "stats": r_stats,
        "taxonomy": r_taxonomy,
        "graph": r_graph,
    }


@flow(name=_FLOW_HEALTH, log_prints=True)
def healthcheck_flow() -> dict[str, Any]:
    """Probe server availability and DB pipeline freshness."""
    server = task_check_server()
    freshness = task_check_db_freshness()
    return {"server": server, "freshness": freshness}
