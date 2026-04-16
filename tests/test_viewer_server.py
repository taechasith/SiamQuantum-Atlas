from __future__ import annotations

from unittest.mock import patch

from viewer import server


class DummyError(Exception):
    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


def test_is_transient_overload_error_by_status_code() -> None:
    assert server._is_transient_overload_error(DummyError("busy", status_code=529))


def test_format_api_error_hides_raw_overload_payload() -> None:
    exc = DummyError(
        'API Error: 529 {"type":"error","error":{"type":"overloaded_error","message":"Overloaded"},"request_id":"req_123"}',
        status_code=529,
    )

    result = server._format_api_error(exc)

    assert result["error"] == "The AI provider is overloaded right now. Please retry in a few seconds."
    assert result["request_id"] == "req_123"
    assert result["status_code"] == 529
    assert result["error_type"] == "overloaded_error"


def test_request_analysis_with_retry_retries_transient_overload() -> None:
    calls = {"count": 0}

    def flaky_request(_user_message: str):
        calls["count"] += 1
        if calls["count"] < 3:
            raise DummyError(
                'API Error: 529 {"type":"error","error":{"type":"overloaded_error","message":"Overloaded"},"request_id":"req_retry"}',
                status_code=529,
            )
        return {"ok": True}

    with patch.object(server, "_create_anthropic_analysis", side_effect=flaky_request), patch.object(server.time, "sleep"):
        result = server._request_analysis_with_retry("hello", max_attempts=3)

    assert result == {"ok": True}
    assert calls["count"] == 3
