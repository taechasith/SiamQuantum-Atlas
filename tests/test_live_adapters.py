from __future__ import annotations

from pathlib import Path

import httpx

from siamquantum_atlas.adapters.gdelt_live import _wait_for_shared_cooldown
from siamquantum_atlas.adapters.youtube_live import _describe_youtube_403


def test_describe_youtube_403_exposes_reason_and_hint() -> None:
    response = httpx.Response(
        403,
        json={
            "error": {
                "message": "API access not configured.",
                "errors": [{"reason": "accessNotConfigured"}],
            }
        },
    )

    message = _describe_youtube_403(response)

    assert "reason=accessNotConfigured" in message
    assert "not enabled" in message


def test_wait_for_shared_cooldown_skips_invalid_stamp(tmp_path: Path) -> None:
    stamp = tmp_path / "gdelt_api.last_request"
    stamp.write_text("not-a-timestamp", encoding="utf-8")

    _wait_for_shared_cooldown(stamp, min_interval_sec=10.0)

    assert stamp.read_text(encoding="utf-8") == "not-a-timestamp"
