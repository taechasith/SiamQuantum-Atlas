"""Tests for Prefect orchestration flows.

These tests run flows in Prefect's ephemeral test mode (no server required).
Tasks are unit-tested by mocking the subprocess call.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_run(returncode: int = 0, stdout: str = "ok", stderr: str = "") -> MagicMock:
    m = MagicMock()
    m.returncode = returncode
    m.stdout = stdout
    m.stderr = stderr
    return m


# ---------------------------------------------------------------------------
# _run_cli helper
# ---------------------------------------------------------------------------

def test_run_cli_success(tmp_path: Path) -> None:
    from siamquantum.orchestration.prefect_flows import _run_cli

    with patch("subprocess.run", return_value=_mock_run(0, "fetched=5")) as mock_sub:
        rc, output = _run_cli("ingest", "today")
    assert rc == 0
    assert "fetched=5" in output
    mock_sub.assert_called_once()


def test_run_cli_failure(tmp_path: Path) -> None:
    from siamquantum.orchestration.prefect_flows import _run_cli

    with patch("subprocess.run", return_value=_mock_run(1, "", "DB error")):
        rc, output = _run_cli("ingest", "today")
    assert rc == 1
    assert "DB error" in output


# ---------------------------------------------------------------------------
# Individual tasks (run outside Prefect context via direct call)
# ---------------------------------------------------------------------------

def test_task_ingest_today_success() -> None:
    from siamquantum.orchestration.prefect_flows import task_ingest_today

    with (
        patch("siamquantum.orchestration.prefect_flows._run_cli", return_value=(0, "fetched=2")),
        patch("siamquantum.orchestration.prefect_flows._record"),
        patch("siamquantum.orchestration.prefect_flows.get_run_logger", return_value=MagicMock()),
    ):
        result = task_ingest_today.fn()
    assert result["rc"] == 0


def test_task_ingest_today_failure() -> None:
    from siamquantum.orchestration.prefect_flows import task_ingest_today

    with (
        patch("siamquantum.orchestration.prefect_flows._run_cli", return_value=(1, "error")),
        patch("siamquantum.orchestration.prefect_flows._record"),
        patch("siamquantum.orchestration.prefect_flows.get_run_logger", return_value=MagicMock()),
    ):
        with pytest.raises(RuntimeError, match="ingest-today exited with rc=1"):
            task_ingest_today.fn()


def test_task_check_server_unreachable() -> None:
    from siamquantum.orchestration.prefect_flows import task_check_server

    with (
        patch("httpx.get", side_effect=Exception("connection refused")),
        patch("siamquantum.orchestration.prefect_flows.get_run_logger", return_value=MagicMock()),
    ):
        result = task_check_server.fn()
    assert result["ok"] is False
    assert "connection refused" in result["error"]


def test_task_check_db_freshness_no_rows(tmp_path: Path) -> None:
    from siamquantum.orchestration.prefect_flows import task_check_db_freshness
    from siamquantum.db.session import init_db

    db_path = tmp_path / "test.db"
    init_db(db_path)

    with (
        patch(
            "siamquantum.orchestration.prefect_flows.db_path_from_url",
            return_value=db_path,
        ),
        patch("siamquantum.orchestration.prefect_flows.get_run_logger", return_value=MagicMock()),
    ):
        result = task_check_db_freshness.fn()
    assert result["task_name"] is None


def test_task_check_db_freshness_with_rows(tmp_path: Path) -> None:
    from siamquantum.orchestration.prefect_flows import task_check_db_freshness
    from siamquantum.db.session import get_connection, init_db

    db_path = tmp_path / "test.db"
    init_db(db_path)
    with get_connection(db_path) as conn:
        conn.execute(
            """INSERT INTO pipeline_runs
               (flow_name, task_name, status, started_at, finished_at, duration_s)
               VALUES (?, ?, ?, ?, ?, ?)""",
            ("siamquantum-refresh", "ingest-today", "success",
             "2026-05-12T01:00:00+00:00", "2026-05-12T01:02:00+00:00", 120.0),
        )
        conn.commit()

    with (
        patch(
            "siamquantum.orchestration.prefect_flows.db_path_from_url",
            return_value=db_path,
        ),
        patch("siamquantum.orchestration.prefect_flows.get_run_logger", return_value=MagicMock()),
    ):
        result = task_check_db_freshness.fn()
    assert result["task_name"] == "ingest-today"
    assert result["status"] == "success"


# ---------------------------------------------------------------------------
# Flow smoke test (ephemeral mode — no Prefect server)
# ---------------------------------------------------------------------------

def test_refresh_flow_smoke() -> None:
    from siamquantum.orchestration.prefect_flows import refresh_flow

    ok_return = (0, "ok")
    with (
        patch("siamquantum.orchestration.prefect_flows._run_cli", return_value=ok_return),
        patch("siamquantum.orchestration.prefect_flows._record"),
        patch("siamquantum.orchestration.prefect_flows.get_run_logger", return_value=MagicMock()),
    ):
        result = refresh_flow()

    assert set(result.keys()) == {"ingest", "nlp", "stats", "taxonomy", "graph"}
    for step in result.values():
        assert step["rc"] == 0


def test_healthcheck_flow_smoke() -> None:
    from siamquantum.orchestration.prefect_flows import healthcheck_flow

    import httpx

    mock_resp = MagicMock()
    mock_resp.status_code = 200

    with (
        patch("httpx.get", return_value=mock_resp),
        patch(
            "siamquantum.orchestration.prefect_flows.db_path_from_url",
            return_value=Path("/nonexistent/test.db"),
        ),
        patch("siamquantum.orchestration.prefect_flows.get_connection", side_effect=Exception("no db")),
        patch("siamquantum.orchestration.prefect_flows.get_run_logger", return_value=MagicMock()),
    ):
        result = healthcheck_flow()

    assert result["server"]["ok"] is True
    assert result["freshness"]["task_name"] is None
