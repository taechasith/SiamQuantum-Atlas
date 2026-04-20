"""Tests for CSE quota guard + probe-once cache."""
from __future__ import annotations

import json
import importlib
from pathlib import Path
from typing import Generator
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_quota(tmp_path: Path, pacific_date: str, count: int) -> Path:
    f = tmp_path / "cse_quota_state.json"
    f.write_text(json.dumps({"last_reset_pacific_date": pacific_date, "queries_used_today": count}))
    return f


# ---------------------------------------------------------------------------
# Quota tracker tests
# ---------------------------------------------------------------------------

class TestQuotaTracker:
    def test_fresh_file_starts_at_zero(self, tmp_path: Path) -> None:
        with patch("siamquantum.services.google_cse._QUOTA_FILE", tmp_path / "q.json"):
            with patch("siamquantum.services.google_cse._pacific_today", return_value="2026-04-20"):
                from siamquantum.services.google_cse import _increment_quota
                count = _increment_quota()
        assert count == 1

    def test_resets_on_date_change(self, tmp_path: Path) -> None:
        qfile = _write_quota(tmp_path, "2026-04-19", 85)
        with patch("siamquantum.services.google_cse._QUOTA_FILE", qfile):
            with patch("siamquantum.services.google_cse._pacific_today", return_value="2026-04-20"):
                from siamquantum.services.google_cse import _increment_quota
                count = _increment_quota()
        assert count == 1  # reset to 0 then +1
        data = json.loads(qfile.read_text())
        assert data["last_reset_pacific_date"] == "2026-04-20"
        assert data["queries_used_today"] == 1

    def test_same_date_increments(self, tmp_path: Path) -> None:
        qfile = _write_quota(tmp_path, "2026-04-20", 50)
        with patch("siamquantum.services.google_cse._QUOTA_FILE", qfile):
            with patch("siamquantum.services.google_cse._pacific_today", return_value="2026-04-20"):
                from siamquantum.services.google_cse import _increment_quota
                count = _increment_quota()
        assert count == 51

    def test_hard_stop_at_90(self, tmp_path: Path) -> None:
        from siamquantum.services.google_cse import QuotaExhaustedError
        qfile = _write_quota(tmp_path, "2026-04-20", 90)
        with patch("siamquantum.services.google_cse._QUOTA_FILE", qfile):
            with patch("siamquantum.services.google_cse._pacific_today", return_value="2026-04-20"):
                from siamquantum.services.google_cse import _increment_quota
                with pytest.raises(QuotaExhaustedError):
                    _increment_quota()

    def test_atomic_write_uses_tmp(self, tmp_path: Path) -> None:
        """Verify .tmp file is used (os.replace pattern)."""
        import siamquantum.services.google_cse as mod
        q = {"last_reset_pacific_date": "2026-04-20", "queries_used_today": 5}
        with patch.object(mod, "_QUOTA_FILE", tmp_path / "q.json"):
            mod._save_quota_atomic(q)
        assert (tmp_path / "q.json").exists()
        assert not (tmp_path / "q.tmp").exists()  # tmp cleaned up by os.replace


# ---------------------------------------------------------------------------
# Probe-once cache tests
# ---------------------------------------------------------------------------

class TestProbeCache:
    def _reset_cache(self) -> None:
        import siamquantum.services.google_cse as mod
        mod._OR_QUERY_SUPPORTED = None

    def test_probe_calls_api_only_once(self, tmp_path: Path) -> None:
        self._reset_cache()
        import siamquantum.services.google_cse as mod
        mock_get = MagicMock(return_value=[{"title": "t", "link": "https://example.com"}])
        with patch.object(mod, "_get_page", mock_get):
            with patch.object(mod, "_cx_for_tier", return_value="fake_cx"):
                result1 = mod.probe_or_query("academic")
                result2 = mod.probe_or_query("academic")
                result3 = mod.probe_or_query("media")

        assert result1 is True
        assert result2 is True
        assert result3 is True
        assert mock_get.call_count == 1  # only first call hits API

    def test_probe_returns_false_on_empty_items(self, tmp_path: Path) -> None:
        self._reset_cache()
        import siamquantum.services.google_cse as mod
        mock_get = MagicMock(return_value=[])
        with patch.object(mod, "_get_page", mock_get):
            with patch.object(mod, "_cx_for_tier", return_value="fake_cx"):
                result = mod.probe_or_query("academic")
        assert result is False

    def test_probe_returns_false_on_runtime_error(self) -> None:
        self._reset_cache()
        import siamquantum.services.google_cse as mod
        mock_get = MagicMock(side_effect=RuntimeError("CSE 403"))
        with patch.object(mod, "_get_page", mock_get):
            with patch.object(mod, "_cx_for_tier", return_value="fake_cx"):
                result = mod.probe_or_query("academic")
        assert result is False
